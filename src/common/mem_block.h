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
/*<std-header orig-src='shore' incl-file-exclusion='MEM_BLOCK_H'>

 $Id: mem_block.h,v 1.4 2010/12/08 17:37:37 nhall Exp $

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

#ifndef __MEM_BLOCK_H
#define __MEM_BLOCK_H

/**\cond skip */
#include <stddef.h>
#include <cassert>
#include "w_base.h"

#define NORET

/* NOTE: The classes defined here are non-template helpers for the
   template-based block_pool class. Because they are only intended to
   be used from template code, they accept "template" parameters with
   every call instead of storing them internally: register-passing
   from the template code is less expensive than loading the value
   from memory, and also minimizes per-block space overhead.
 */

namespace memory_block {
#if 0 /*keep emacs happy */
} // keep emacs happy...
#endif

/* GCC can't handle zero length arrays, but has an extension to handle
   unsized ones at the end of a struct. Meanwhile, SunStudio can't
   handle unsized arrays, but has an extension to allow zero-length
   arrays.
 */
#ifdef __GNUC__
#define EMPTY_ARRAY_DIM
#else
#define EMPTY_ARRAY_DIM 0
#endif

// forward decl...
struct block_list;

/* A bitmap allocator.

   This class tracks the bookkeeping of chip allocation while leaving
   the corresponding memory management to someone else. The
   implementation requires that chip allocation be single-threaded
   (presumably by some owner thread), but is thread-safe with respect
   to deallocation.

   A given "chip" may be in one of three states:
   
       USABLE: available to be allocated
    
       IN-USE: allocated but not yet freed
    
       ZOMBIE: freed more recently than the last recycling pass

   Allocation is double-buffered in a sense: at the beginning of each
   allocation round, the owning thread unions the current set of
   zombie chips into the usable set; in-use chips are ignored.

   The class has two members to support higher-level functionality:
   
       _owner: an opaque pointer which is used to verify that blocks
           are being released properly
        
       _next: an embedded linked list node for use by the owner and
           otherwise ignored by the implementation
 */
struct block_bits {
    
    typedef uint64_t     bitmap; 

    enum         { MAX_CHIPS=8*sizeof(bitmap) };
    
    NORET        block_bits(size_t chip_count);
    
    size_t       acquire_contiguous(size_t chip_count);

    // @return whether released successfully
    bool         release_contiguous(size_t idx, size_t chip_count);
    
    static
    bitmap       create_mask(size_t bits_set);

    size_t        usable_count() const { return _popc(_usable_chips); }
    size_t        zombie_count() const { return _popc(_zombie_chips); }

    void          recycle();
    
    static
    size_t        _popc(bitmap bm);

    bitmap        _usable_chips; // available as of last recycling (1thr)
    bitmap _zombie_chips; // deallocations since last recycling (racy)
};

/* Control structure for one block of allocatable memory.

   The memory is assumed to be /block_size/ bytes long and must be
   aligned on a /block_size/-sized boundary. The implementation
   exploits the block's alignment to compute the block control for
   pointers being released, meaning there is no per-chip space
   overhead.

   One caveat of the implicit control block pointer approach is that
   the caller is responsible to ensure that any chip passed to
   /release()/ is actually part of a block (presumably by verifying
   that the address range is inside an appropriate memory pool).

   This class manages memory only very loosely: distributing and
   accepting the return of /chip_size/-sized chips. At no time does it
   access any of the memory it manages.
 */
struct block {
    NORET        block(size_t chip_size, size_t chip_count, size_t block_size);
    void*        acquire_chip(size_t chip_size, size_t chip_count, size_t block_size);

    // WARNING: the caller must ensure that ptr is in a valid memory range
    // @return: whether successfully released
    static
    bool         release_chip(void* ptr, size_t chip_size, size_t chip_count, size_t block_size);
    // WARNING: the caller must ensure that ptr is in a valid memory range
    static
    void         release(void* ptr, size_t chip_size, size_t chip_count, size_t block_size);

    void         recycle() { _bits.recycle(); }

    char*        _get(size_t idx, size_t chip_size);
    
    block_bits      _bits;
    block_list*     _owner;
    block*          _next;
    // The memory managed:
    char            _data[EMPTY_ARRAY_DIM];
};


struct block_pool {
    block*        acquire_block(block_list* owner);
    block*        release_block(block* b);

    // true if /ptr/ points to data inside some block managed by this pool
    virtual bool    validate_pointer(void* ptr)=0;
    
    virtual NORET    ~block_pool() { }
    
protected:
    
    virtual block*   _acquire_block()=0;
    // takes back b, then returns b->_next
    virtual void     _release_block(block* b)=0;
};

struct block_list {
    NORET        block_list(block_pool* pool, size_t chip_size,
                   size_t chip_count, size_t block_size);
    
    NORET        ~block_list();
    
    void*         acquire(size_t chip_size, size_t chip_count, size_t block_size);
    block*        acquire_block(size_t block_size);

    void*        _slow_acquire(size_t chip_size, size_t chip_count, size_t block_size);
    void         _change_blocks(size_t chip_size, size_t chip_count, size_t block_size);

    block         _fake_block;
    block*        _tail;
    block_pool*   _pool;
    size_t        _hit_count;
    double        _avg_hit_rate;
    
};


inline
block* block_pool::acquire_block(block_list* owner) {
    block* b = _acquire_block();
    b->_owner = owner;
    b->_next = 0;
    b->_bits.recycle();
    return b;
}

inline
block* block_pool::release_block(block* b) {
    assert(validate_pointer(b));
    block* next = b->_next;
    b->_next = 0;
    b->_owner = 0;
    _release_block(b);
    return next;
}


/* A compile-time predicate.

   Compilation will fail for B=false because only B=true has a definition
*/
template<bool B>
struct fail_unless;

template<>
struct fail_unless<true> {
    static bool valid() { return true; }
};



/* A compile-time bounds checker.

   Fails to compile unless /L <= N <= U/
 */
template<int N, int L, int U>
struct bounds_check : public fail_unless<(L <= N) && (N <= U)> {
    static bool valid() { return true; }
};



/* A template class which, given some positive compile-time constant
   integer /x/, computes the compile-time constant value of
   /floor(log2(x))/
 */

template <int N>
struct meta_log2 : public fail_unless<(N > 2)> {
    enum { VALUE=1+meta_log2<N/2>::VALUE };
};

template<>
struct meta_log2<2> {
    enum { VALUE=1 };
};

template<>
struct meta_log2<1> {
    enum { VALUE=0 };
};

template<int A, int B>
struct meta_min {
    enum { VALUE = (A < B)? A : B };
};

/* A template class which computes the optimal block size. Too large
   a block and the bitmap can't reach all the objects that fit,
   wasting space. Too small and the bitmap is mostly empty, leading to
   high overheads (in both space and time).

   For any given parameters there exists only one value of /BYTES/
   which is a power of two and utilizes 50% < util <= 100% of a
   block_bit's chips. 
 */
template<int ChipSize, int OverheadBytes=sizeof(memory_block::block), int MaxChips=block_bits::MAX_CHIPS>
struct meta_block_size : public fail_unless<(ChipSize > 0 && OverheadBytes >= 0)> {
    enum { CHIP_SIZE    = ChipSize };
    enum { OVERHEAD     = OverheadBytes };
    enum { MAX_CHIPS     = MaxChips };
    enum { MIN_CHIPS     = MAX_CHIPS/2 + 1 };
    enum { BYTES_NEEDED = MIN_CHIPS*ChipSize+OverheadBytes };
    enum { LOG2     = meta_log2<2*BYTES_NEEDED-1>::VALUE };
    
    enum { BYTES     = 1 << LOG2 };
    fail_unless<((BYTES &- BYTES) == BYTES)>     power_of_two;
    
    /* ugly corner case...

       if chips are small compared to overhead then we can end up with
       space for more than MAX_CHIPS. However, cutting the block size
       in half wouldn't leave enough chips behind so we're stuck.

       Note that this wasted space would be small compared with the
       enormous overhead that causes the situation in the first place.
     */    
    enum { REAL_COUNT     = (BYTES-OverheadBytes)/ChipSize };
    fail_unless<((OVERHEAD + MIN_CHIPS*ChipSize) > (int)BYTES/2)> sane_chip_count;
    
    enum { COUNT     = meta_min<MAX_CHIPS, REAL_COUNT>::VALUE };
    bounds_check<COUNT, MIN_CHIPS, MAX_CHIPS>     bounds;

};

} // namespace memory_block

/**\endcond skip */

#endif
