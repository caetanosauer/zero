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

#ifndef __POOL_ALLOC_H
#define __POOL_ALLOC_H

#include <cassert>
#include <cstdlib>
#include "util/thread_local.h"

class pool_alloc {
public:
    pool_alloc(char const* name, long delta=8);
    ~pool_alloc();
    void* alloc(int size);
    void free(void* ptr);

    // always shift left, 0 indicates full block
    typedef unsigned long bitmap;
    enum constants {BLOCK_UNITS=64};
    
    struct header;

    // WARNING: sizeof(block) must be a multiple of 8 bytes
    struct block_map_sync;
    struct block {
	bitmap _bitmap; // 1 bits mark in-use slots

	// used by TRACE_LEAKS
	const char* _name;

	// used by TRACK_BLOCKS
	block_map_sync* _live_blocks;
	
	char _data[0]; // place-holder
	
	// Whoever creates me owns all the data, and can dole it out as they choose
	block(char const* name)
	    : _bitmap(~0ul), _name(name)
	{
	}
	header* get_header(int index, bitmap range);
	void cut_loose();
    };

    // WARNING: sizeof(header) must be a multiple of 8 bytes
    struct header {
	bitmap _range; // 0 bits mark me
	block* _block;
	char _data[0]; // place-holder for the actual data
	void release();
    };

public:
    // every type-thread combination gets a different pool. Feel free to allocate more pools if desired...
    template<class T>
    static pool_alloc* pool(char const* name) {
	static __thread pool_alloc* _pool = NULL;
	return _pool? _pool : _pool = new pool_alloc(name);
    }

private:
    char const* _name;
    block* _block;
    bitmap _next_unit; // a single bit that marches right to left; 0 indicates the block is full
    int _delta; // how much do I increment the offset for each allocation from the current block?
    int _offset; // current block offset in bytes
    int _alloc_count;
    int _huge_count;
    int _alloc_sizes[BLOCK_UNITS];
    int _huge_sizes[BLOCK_UNITS];
    block_map_sync* _live_blocks;
    
    void new_block();
    header* allocate(int size);
    header* allocate_normal(int size);
    header* allocate_huge(int size);
    void discard_block();
    
};

#define DECLARE_POOL_ALLOC_POOL(cls)					\
    static pool_alloc* pool() { return pool_alloc::pool<cls>(#cls); }	\
    struct dummy_struct
#if 1
#define DECLARE_POOL_ALLOC_NEW_AND_DELETE(cls)				\
    void* operator new(size_t size) { return pool()->alloc(size); }	\
    void operator delete(void* ptr) { pool()->free(ptr); }		\
    DECLARE_POOL_ALLOC_POOL(cls)
#else
#define DECLARE_POOL_ALLOC_NEW_AND_DELETE(cls)    struct dummy_struct
#endif

// swatchz counter to make sure pool_alloc is ready before anything tries to use it
struct static_initialize_pool_alloc { static_initialize_pool_alloc(); };
static static_initialize_pool_alloc pool_alloc_swatchz;


#endif
