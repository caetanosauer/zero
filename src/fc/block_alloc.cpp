/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
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
/*<std-header orig-src='shore' incl-file-exclusion='BLOCK_ALLOC_CPP'>

 $Id: block_alloc.cpp,v 1.5 2010/09/23 13:52:36 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#include "block_alloc.h"
#include "tls.h"
#include <cassert>

/**\cond skip */

/*
 * dynpool is a thread-safe list into which
 * blocks are released and from which blocks are allocated.
 */

dynpool::dynpool(size_t chip_size, size_t chip_count, size_t log2_block_size, size_t max_bytes)
    : _chip_size(chip_size)
    , _chip_count(chip_count)
    , _log2_block_size(log2_block_size)
    , _arr_end(0)
{
    pthread_mutex_init(&_lock, 0);
    int err = _arr.init(max_bytes, size_t(1) << _log2_block_size);
    if(err) throw std::bad_alloc();
}
    
dynpool::~dynpool() {
    pthread_mutex_destroy(&_lock);
    _arr.fini();
}
    
dynpool::mblock* dynpool::_acquire_block() {
    mblock* rval;
    //tataslock_critical_section cs (&_lock);
    pthread_mutex_lock(&_lock);
    if(_free_list.empty()) {
        size_t block_size = size_t(1) << _log2_block_size;
        size_t new_end = _arr_end+block_size;
        int err = _arr.ensure_capacity(new_end);
        if(err) throw std::bad_alloc();
        rval = new (_arr+_arr_end) mblock(_chip_size, _chip_count, block_size);
        _arr_end = new_end;
    }
    else {
        rval = _free_list.front();
        _free_list.pop_front();
    }
    pthread_mutex_unlock(&_lock);
        
    return rval;
}

void dynpool::_release_block(mblock* b) {
    //tataslock_critical_section cs (&_lock);
    pthread_mutex_lock(&_lock);
    _free_list.push_back(b);
    pthread_mutex_unlock(&_lock);
}

bool dynpool::validate_pointer(void* ptr) {
    // no need for the mutex... dynarray only grows
    union { void* v; char* c; } u={ptr};
    size_t offset = u.c - _arr;
	// An assertion here has been seen when the
	// user did this:
	// w_rc_t func() {}
	// and compiled w/o any warning (-Wall) to
	// tell him about the lack of a return value. Then
	// called the function.  That gets us here.
    if((u.c < _arr) || (offset >= _arr_end)) {
		fprintf(stderr, 
		"Assertion failure in dynpool: invalid pointer. %s\n",
		"(Did you fail to provide a return value for a w_rc_t-valued function?)");
	}
    assert (u.c >= _arr);
    return offset < _arr_end;
}
/**\endcond skip */

#include <map>
struct blob_pool::helper {
    struct BLBlob : tls_tricks::tls_blob<BlockList> {
	long initialized;
	BLBlob() : initialized(false) { }
    };
    typedef std::map<void*, BLBlob> BLMap;
    
    enum { OVERHEAD=sizeof(memory_block::block) };
    enum { MAX_CHIPS=memory_block::block_bits::MAX_CHIPS };
    
    static size_t log2(long n) {
	if(n <= 0)
	    return 0;
	return 1 + log2(n/2);
    }

    static size_t compute_bytes_log2(long chip_size) {
	long min_chips = MAX_CHIPS/2+1;
	long bytes_needed = min_chips*chip_size + OVERHEAD;
	return log2(2*bytes_needed - 1);
    }
    
    static size_t compute_bytes(long chip_size) {
	return 1 << compute_bytes_log2(chip_size);
    }
    
    static size_t compute_count(long chip_size) {
	long bytes = compute_bytes(chip_size);
	long real_count = (bytes - OVERHEAD)/chip_size;
	return std::min<long>(MAX_CHIPS, real_count);
    }

    // these are for the tls manager
    static void init_blmap() { /* no-op */ }
    static void fini_blmap() {
	BLMap* blm = blmap(false);
	if(!blm) 
	    return;
	
	for(BLMap::iterator it=blm->begin(); it != blm->end(); ++it) {
	    assert(it->second.initialized);
	    it->second.get()->~BlockList();
	}
	blm->~BLMap();
    }
    static bool initialize() {
	tls_tricks::tls_manager::register_tls(&init_blmap, &fini_blmap);
	return true;
    }
    static BLMap* blmap(bool force=true) {
	// first time we're called, register with the tls_manager. It
	// doesn't matter if many threads exist already because our
	// init function is empty and any thread which already exited
	// can't have created a blist that needs tearing down.
	static __thread bool blmap_initialized = false;
	static __thread tls_tricks::tls_blob<BLMap> tls_blmap;
	if(!blmap_initialized) {
	    if(!force)
		return NULL;
	    
	    tls_blmap.init();
	    blmap_initialized = true;
	}

	return tls_blmap.get();
    }
    static bool get_blist(BlockList** rbl, void* owner) {
	BLMap &blm = *blmap();
	BLBlob &bl = blm[owner];
	*rbl = bl.get();
	if(bl.initialized)
	    return true;
	bl.initialized = true;
	return false;
    }
};

    // gets old typing this over and over...
#define BLOB_POOL_TEMPLATE_ARGS _chip_size, _chip_count, _block_size

blob_pool::blob_pool(size_t size)
    : _chip_size((size + sizeof(long) - 1) & -sizeof(long))
    , _block_size(helper::compute_bytes(_chip_size))
    , _chip_count(helper::compute_count(_chip_size))
    , _pool(_chip_size, _chip_count,
	    helper::compute_bytes_log2(_chip_size),
	    1024*1024*1024)
{
    assert(size >= sizeof(long));
}

    

/* Acquire one blob from the pool.
 */
void* blob_pool::acquire() {
    BlockList* blist;
    if(! helper::get_blist(&blist, &_pool))
	new (blist) BlockList(&_pool, BLOB_POOL_TEMPLATE_ARGS);
    void* ptr = blist->acquire(BLOB_POOL_TEMPLATE_ARGS);
    assert(_pool.validate_pointer(ptr));
    return ptr;
}
    
/* Verify that we own the object then find its block and report it
   as released. If \e destruct is \e true then call the object's
   desctructor also.
*/
void blob_pool::destroy(void* ptr) {
    assert(_pool.validate_pointer(ptr));
    memory_block::block::release(ptr, BLOB_POOL_TEMPLATE_ARGS);
}
