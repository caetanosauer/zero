/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

#include "util/pool_alloc.h"
#include "util/sync.h"

#include "k_defines.h"

#include <new>
#include <vector>


#ifdef DEBUG_ALLOC
#include <cstdio>
using namespace std;
#endif

#undef TRACK_LEAKS


#undef TRACE_LEAKS
#undef TRACK_BLOCKS


namespace {
    /* This mess here makes the pointer associated with a
       pthread_key_t act like a normal pointer (except expensive).
     */
    typedef std::vector<pool_alloc*> pool_list;

    extern "C" void delete_pool_list(void* arg) {
	pool_list* pl = (pool_list*) arg;
	if(!pl)
	    return;
	for(uint i=0; i < pl->size(); i++)
	    delete pl->at(i);

	delete pl;
    }
    
    struct s_thread_local_pools {
	pthread_key_t ptkey;
	void init() {
	    // WARNING: not thread safe! (must call it during static initialization)
	    static bool initialized = false;
	    if(initialized)
		return;
	    
	    int err = pthread_key_create(&ptkey, delete_pool_list);
	    assert(!err);
	    initialized = true;
	}
	operator pool_list*() { return get(); }
	pool_list* operator->() { return get(); }
	pool_list* get() {
	    pool_list* ptr = (pool_list*) pthread_getspecific(ptkey);
	    if(!ptr)
		pthread_setspecific(ptkey, ptr = new pool_list);
	    
	    return ptr;
	}
    } thread_local_pools;
}

// the rest of the swatchz counter
static_initialize_pool_alloc::static_initialize_pool_alloc() {
    thread_local_pools.init();
}

// always shift left, 0 indicates full block
typedef unsigned long bitmap;

/* This allocator uses malloc to acquire "blocks" of memory, which are
   broken up into "units" which can be handed out one (or many) at a
   time.

   Each block contains 64 units, and maintains a bitmap which marks used
   and unused units. During normal operation, the allocator
   automatically adjusts its unit size to match the largest request it
   has ever seen; this makes it most useful for allocating objects of
   the same or similar sizes.

   When the application requests memory from the allocator, the latter
   carves out enough units to fit the request and marks them as
   used. It always proceeds in sequence, never checking to see whether
   any previous units have freed up. Once a block fills up, the
   allocator purposefully "leaks" it (see deallocation, below) and
   malloc()s another. 

   Each allocation (containing one or more units) contains enough
   information to notify its owning block when the application frees
   it; the block deletes itself when it gets all its units
   back.

   Allocation and deallocation are thread-safe, even when they occurs
   in different threads at the same time or allocated memory moves
   between threads.
   
 */
// 
#ifdef TRACK_BLOCKS
#include <map>
#include <vector>
#include <utility>
#include <algorithm>

static pthread_mutex_t block_mutex = PTHREAD_MUTEX_INITIALIZER;
struct block_entry {
    pool_alloc::block* _block;
    char const* _name;
    pthread_t _tid;
    int _delta;
    block_entry(pool_alloc::block* b, char const* name, pthread_t tid, int delta)
	: _block(b), _name(name), _tid(tid), _delta(delta) { }
    bool operator<(block_entry const &other) const {
	if(_name < other._name)
	    return true;
	if(_name > other._name)
	    return false;
	if(_tid < other._tid)
	    return true;
	if(_tid > other._tid)
	    return false;
	return _block < other._block;
    }
};
typedef std::map<pool_alloc::block*, int> block_map; // block, delta
typedef std::pair<block_map, pthread_mutex_t> block_map_sync;
typedef std::pair<char const*, pthread_t> thread_alloc_key;
typedef std::pair<block_map_sync*, bool> thread_alloc_value; // map, thread live?
typedef std::map<thread_alloc_key, thread_alloc_value> thread_maps;
typedef std::vector<block_entry> block_list;
static thread_maps* live_blocks;

void print_live_blocks() {
    // not locked because we should only use it from the debugger anyway
    pool_alloc::block_list blist;

    // (thread_alloc_key, thread_alloc_value) pairs
    thread_maps::iterator map = live_blocks->begin();
    while(map != live_blocks->end()) {
	thread_alloc_key const &key = map->first;
	thread_alloc_value &value = map->second;
	block_map* bmap = &value.first->first;

	// (block*, delta) pairs
	block_map::iterator it = bmap->begin();
	for( ; it != bmap->end(); ++it) 
	    blist.push_back(block_entry(it->first, key.first, key.second, it->second));
	
	// is the thread dead and all blocks deallocated?
	if(!value.second && bmap->empty()) 
	    live_blocks->erase(map++);
	else
	    ++map;
    }

    std::sort(blist.begin(), blist.end());
    block_list::iterator it=blist.begin();
    printf("Allocator  (tid): " "address           " " unit bitmap\n");
    for(; it != blist.end(); ++it) {
	printf("%10s (%3d): 0x%016p %4d ", it->_name, it->_tid, it->_block, it->_delta);
	bitmap units = it->_block->_bitmap;
	for(int i=0; i < pool_alloc::BLOCK_UNITS; i++) {
	    putchar('0' + (units&0x1));
	    units >>= 1;
	}
	putchar('\n');    
    }
}
#else
void print_live_blocks() {
    printf("Sorry, block tracking is not enabled. Recompile with -DTRACK_BLOCKS and try again)\n");
}
#endif

pool_alloc::pool_alloc(char const* name, long delta)
    : _name(name), _block(NULL), _next_unit(0), _delta(delta), _offset(0),
      _alloc_count(0), _huge_count(0)
{
    //    printf("t@%d initializing a pool_alloc<%s>\n ", pthread_self(), name);
    memset(_alloc_sizes, 0, sizeof(_alloc_sizes));
    memset(_huge_sizes, 0, sizeof(_huge_sizes));
#ifdef TRACK_BLOCKS
    // register our private block map
    _live_blocks = new block_map_sync;
    _live_blocks->second = thread_mutex_create();
    critical_section_t cs(block_mutex);
    (*live_blocks)[std::make_pair(_name, pthread_self())] = std::make_pair(_live_blocks, true);
#endif

    // add myself to the pool list
    thread_local_pools->push_back(this);
}

pool_alloc::~pool_alloc() {
    //    printf("t@%d destroying a pool_alloc<%s>\n", pthread_self(), _name);
    discard_block();
#ifdef TRACK_BLOCKS
    // mark the block as dead
    critical_section_t cs(block_mutex);
    (*live_blocks)[std::make_pair(_name, pthread_self())].second = false;
#endif
}


pool_alloc::header* pool_alloc::block::get_header(int offset, pool_alloc::bitmap range) {
    header* h = (header*) &_data[offset];
    h->_block = this;
    h->_range = range;
    return h;
}

void pool_alloc::header::release() {

#ifdef TRACE_LEAKS
    fprintf(stderr, "%s unit -1 %p %016llx\n", _block->_name, _block, _range);
#endif

    bitmap result;
    bitmap clear_range = ~_range;
#ifdef __sparcv9
    membar_enter();
    result = atomic_and_64_nv(&_block->_bitmap, clear_range);
    membar_exit();
#else
    result = __sync_and_and_fetch(&_block->_bitmap, clear_range);
#endif
	
    // have all the units come back?
    if(!result) {
#ifdef TRACE_LEAKS
	if(!clear_range)
	    fprintf(stderr, "%s block -1 %p %016llx (oversized)\n", _block->_name, _block, _range);
	else
	    fprintf(stderr, "%s block -1 %p\n", _block->_name, _block);
#endif
#ifdef TRACK_BLOCKS
	{
	    critical_section_t cs(_block->_live_blocks->second);
	    _block->_live_blocks->first.erase(_block);
	}
#endif
	// note: frees the memory 'this' occupies
	::free(_block);
    }
}
    

static int const ADJUST_GOAL = 5; // 1/x
static int const K_MAX = (pool_alloc::BLOCK_UNITS+ADJUST_GOAL-1)/ADJUST_GOAL;

static int kth_biggest(int* array, int array_size) {
    int top_k[K_MAX];
    int k = (array_size+ADJUST_GOAL-1)/ADJUST_GOAL;
    int last=0; // last entry of the top-k list
    top_k[0] = array[0];
    for(int i=1; i < array_size; i++) {
	int size = array[i];

	// find the insertion point
	int index=0;
	while(index <= last && size < top_k[index]) ++index;
	if(index == k)
	    continue; // too small...
	
	// bump everything else over to make room...
	for(int j=k; --j > index; )
	    top_k[j] = top_k[j-1];
	
	// ...and insert
	top_k[index] = size;
	if(index > last)
	    last = index;
    }

    return top_k[last];
}

void pool_alloc::discard_block() {
    // allocate whatever is left of this block and throw it away
    if(_next_unit) {
	_block->get_header(_offset, ~(_next_unit-1))->release();
	_next_unit = 0;
    }
}
    
void pool_alloc::new_block() {
    // adjust the unit size? We want no more than 1/x requests to allocate 2+ units
    

    _huge_count = 0;
    _alloc_count = 0;
    _next_unit = 1;
    int bytes = sizeof(block) + BLOCK_UNITS*_delta;
    void* data = ::malloc(bytes);
    if(!data)
	throw std::bad_alloc();
    
    _block = new(data) block(_name);
    _offset = 0;
#ifdef TRACK_BLOCKS
    {
	_block->_live_blocks = _live_blocks;
	critical_section_t cs(_live_blocks->second);
	_live_blocks->first[_block] = _delta;
    }
#endif
}

pool_alloc::header* pool_alloc::allocate_huge(int size) {
    _huge_sizes[_huge_count++] = size;
    // enough "huge" requests to destroy our ADJUST_GOAL?
    if(_huge_count > K_MAX) {
	discard_block();
	_delta = kth_biggest(_huge_sizes, _huge_count);
	return allocate_normal(size);
    }

    /* Create a block that's just the size we want, and allocate
       the whole thing to this request. 
    */
    void* data = ::malloc(sizeof(block) + size);
    if(!data)
	throw std::bad_alloc();
	
    block* b = new(data) block(_name);
#ifdef TRACK_BLOCKS
    {
	b->_live_blocks = _live_blocks;
	critical_section_t cs(_live_blocks->second);
	_live_blocks->first[b] = size;
    }
#endif
#ifdef TRACE_LEAKS
    fprintf(stderr, "%s block 1 %p+%d (oversized)\n", _name, b, size);
#endif
    return b->get_header(0, ~0ull);
}

pool_alloc::header* pool_alloc::allocate_normal(int size) {

    // is the previous block full?
    if(_next_unit == 0) {
	if(_alloc_count)
	    _delta = kth_biggest(_alloc_sizes, _alloc_count);
	new_block();
#ifdef TRACE_LEAKS
	fprintf(stderr, "%s block 1 %p+%d\n", _name, _block, _delta);
#endif
    }

    // update stats
    _alloc_sizes[_alloc_count++] = size;
    
    // initialize a header 
    header* h = _block->get_header(_offset, 0);
    
    // carve out the units this request needs
    int remaining = size;
    do {
	h->_range |= _next_unit;
	_next_unit <<= 1;
	remaining -= _delta;
	_offset += _delta;
    } while(remaining > 0 && _next_unit);

    // out of space?
    if(remaining > 0) {
	//  free this undersized allocation and try again with a new block
	h->release();
	return allocate_normal(size);
    }

    // success!
    return h;
}

pool_alloc::header* pool_alloc::allocate(int size) {
    // how many units is "huge" ?
    static int const HUGE_THRESHOLD = BLOCK_UNITS/8;

    // huge?
    return (size > HUGE_THRESHOLD*_delta)? allocate_huge(size) : allocate_normal(size);
}

#undef USE_MALLOC

void* pool_alloc::alloc(int size) {
#ifdef USE_MALLOC
    return ::malloc(size);
#endif
    // factor in the header size and round up to the next 8 byte boundary
    size = (size + sizeof(header) + 7) & -8;
    
    header* h = allocate(size);
#ifdef TRACE_LEAKS
    fprintf(stderr, "%s unit 1 %p %016llx\n", _name, h->_block, h->_range);
#endif
    return h->_data;
}

void pool_alloc::free(void* ptr) {
#ifdef USE_MALLOC
    ::free(ptr);
    return;
#endif

    /* C++ "strict aliasing" rules allow the compiler to assume that
       void* and header* never point to the same memory.  If
       optimizations actually take advantage of this, simple casts
       would break.

       NOTE 1: char* automatically aliases everything, but I'm not sure
       if that's enough to make void* -> char* -> header* safe.

       NOTE 2: in practice I've only seen strict aliasing cause
       problems if (a) the memory is actually *accessed* through
       pointers of different types and (b) those pointers both reside
       in the same function after inlining has taken place. But, just
       to be safe...

       Unions are gcc's official way of telling the compiler that two
       incompatible pointers do, in fact, overlap. I suspect it will
       work for CC as well, since a union is rather explicit about
       two things being in the same place.
     */
    union {
	void* vptr;
	char* cptr;
	header* hptr;
    } u = {ptr};
    
    // ptr actually points to header::_data, so back up a bit before calling release()
    u.cptr -= sizeof(header);
    u.hptr->release();
}
