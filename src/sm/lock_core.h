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

// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='LOCK_CORE_H'>

 $Id: lock_core.h,v 1.49 2010/12/08 17:37:42 nhall Exp $

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

#ifndef LOCK_CORE_H
#define LOCK_CORE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#include "lock_lil.h"

class LockCoreFunc {
 public:
    virtual ~LockCoreFunc();

    virtual void operator()(const xct_t* xct) = 0;
};


class bucket_t; // defined in lock_core.cpp

class lock_core_m : public lock_base_t{
    enum { BPB=CHAR_BIT };

public:
    typedef lock_base_t::lmode_t lmode_t;
    typedef lock_base_t::duration_t duration_t;

    NORET        lock_core_m(uint sz);
    NORET        ~lock_core_m();

    int          collect(vtable_t&, bool names_too);

    void        assert_empty() const;
    void        dump();
    void        dump(ostream &o);
    void        _dump(ostream &o);

    
    lil_global_table*   get_lil_global_table() { return _lil_global_table; }

    lock_head_t*    find_lock_head(
                const lockid_t&            n,
                bool                create);
public:
    typedef     w_list_t<lock_head_t,queue_based_lock_t> chain_list_t;
    typedef     w_list_i<lock_head_t,queue_based_lock_t> chain_list_i;

private:
    lock_head_t*    _find_lock_head_in_chain(
                chain_list_t               &l,
                const lockid_t&            n);

public:
    w_rc_t::errcode_t  acquire_lock(
                xct_t*            xd,
                const lockid_t&   name,
                lock_head_t*      lock,
                lock_request_t**  request,
                lmode_t           mode,
                lmode_t&          prev_mode,
                duration_t        duration,
                timeout_in_ms     timeout,
                lmode_t&          ret);

    rc_t        release_lock(
                xct_lock_info_t*  theLockInfo,
                const lockid_t&   name,
                lock_head_t*      lock,
                lock_request_t*   request,
                bool              force);

    void        wakeup_waiters(lock_head_t*& lock);

    rc_t        release_duration(
                xct_lock_info_t*    theLockInfo,
                duration_t        duration,
                bool read_lock_only = false
                );

    lock_head_t*    GetNewLockHeadFromPool(
                const lockid_t&        name,
                lmode_t            mode);
    
    void        FreeLockHeadToPool(lock_head_t* theLockHead);

private:
    uint32_t        _table_bucket(uint32_t id) const { return id % _htabsz; }
    w_rc_t::errcode_t _check_deadlock(
                    xct_t* xd, bool first_time, lock_request_t *myreq);
    
    // internal version that does the actual release
    rc_t    _release_lock(lock_request_t* request, bool force);

#define DEBUG_LOCK_HASH 0
// Turning this on will do lots of very expensive stuff with the
// hash; it's for analyzing the hash function only. Should never be
// used in production.
#if DEBUG_LOCK_HASH
    void               compute_lock_hash_numbers() const;
    void               dump_lock_hash_numbers() const;
#endif
    bucket_t*          _htab;
    uint32_t            _htabsz;
    int                _requests_allocated; // currently-allocated requests.
    // For further study.
    
    /** Global lock table for Light-weight Intent Lock. */
    lil_global_table*  _lil_global_table;
};


#define ACQUIRE_BUCKET_MUTEX(i) MUTEX_ACQUIRE(_htab[i].mutex);
#define RELEASE_BUCKET_MUTEX(i) MUTEX_RELEASE(_htab[i].mutex);
#define BUCKET_MUTEX_IS_MINE(i) w_assert3(MUTEX_IS_MINE(_htab[i].mutex));

#define ACQUIRE_HEAD_MUTEX(l) { \
    w_assert1(l->chain.member_of()!=0); \
    MUTEX_ACQUIRE(l->head_mutex); \
    w_assert1(l->chain.member_of()!=0); \
    }
#define RELEASE_HEAD_MUTEX(l) { \
    w_assert1(l->chain.member_of()!=0); \
    MUTEX_RELEASE(l->head_mutex); \
    }

// MY_LOCK_DEBUG is set in lock_x.h
#if MY_LOCK_DEBUG
#define ASSERT_HEAD_MUTEX_IS_MINE(l) w_assert2(MUTEX_IS_MINE(l->head_mutex));
#define ASSERT_HEAD_MUTEX_NOT_MINE(l) w_assert2(MUTEX_IS_MINE(l->head_mutex)==false);
#else
#define ASSERT_HEAD_MUTEX_IS_MINE(l) 
#define ASSERT_HEAD_MUTEX_NOT_MINE(l) 
#endif


/*<std-footer incl-file-exclusion='LOCK_CORE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
