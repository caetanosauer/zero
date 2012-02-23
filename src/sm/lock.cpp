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
/*<std-header orig-src='shore'>

 $Id: lock.cpp,v 1.157 2010/10/27 17:04:23 nhall Exp $

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

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define LOCK_C

#ifdef __GNUG__
#pragma implementation "lock.h"
#endif

#include "sm_int_1.h"
#include "lock_x.h"
#include "lock_core.h"
#include "lock_lil.h"
#include <new>

#define W_VOID(x) x

#ifdef EXPLICIT_TEMPLATE
template class w_list_i<lock_request_t>;
template class w_list_t<lock_request_t>;
template class w_list_i<lock_head_t>;
template class w_list_t<lock_head_t>;
template class w_list_t<XctWaitsForLockElem>;
template class w_list_i<XctWaitsForLockElem>;
#endif


lock_m::lock_m(int sz)
{
    _core = new lock_core_m(sz);
    w_assert1(_core);
}


void
lock_m::assert_empty() const
{
    _core->assert_empty();
}

lock_m::~lock_m()
{
    assert_empty();
    delete _core;
}


extern "C" void lock_dump_locks();
void lock_dump_locks() { 
    smlevel_0::lm->dump(cerr);
    cerr << flushl;
}

void lock_m::dump(ostream &o)
{
    o << "LOCKS: { " << endl;
    _core->dump(o);
    o << "} " << endl;
}

rc_t lock_m::query(
    const lockid_t&     n,
    lmode_t&            m,
    const tid_t&        tid)
{
    DBGTHRD(<<"lock_m::query for lock " << n);
    xct_t *        xd = xct();
    w_assert9(!implicit || tid != tid_t::null);

    INC_TSTAT(lock_query_cnt);
    m = NL;

    if (tid == tid_t::null) {
        lock_head_t* lock = _core->find_lock_head(n, false);//do not create
        if (lock) {
            // lock head mutex was acquired by find_lock_head
            m = lock->granted_mode;
            RELEASE_HEAD_MUTEX(lock); // acquired in find_lock_head
        }
        return RCOK;
    }
    w_assert2(xd);

    lock_request_t* req = 0;
    lock_head_t* lock = _core->find_lock_head(n, false); // do not create
    if (lock) {
        // lock head mutex was acquired by find_lock_head
        req = lock->find_lock_request(xd->lock_info());
    }
    if (req) {
        m = req->mode();
        RELEASE_HEAD_MUTEX(lock); // acquired in find_lock_head
        return RCOK;
    }

    if (lock)
        RELEASE_HEAD_MUTEX(lock); // acquired in find_lock_head
    return RCOK;
}

lil_global_table* lock_m::get_lil_global_table() {
    return _core->get_lil_global_table();
}


rc_t
lock_m::lock(
    const lockid_t&      n, 
    lmode_t              m,
    duration_t           duration,
    timeout_in_ms        timeout,
    lmode_t*             prev_mode,
    lmode_t*             prev_pgmode,
    lockid_t**           nameInLockHead
    )
{
    lmode_t _prev_mode;
    lmode_t _prev_pgmode;

    rc_t rc = _lock(n, m, _prev_mode, _prev_pgmode, duration, timeout, nameInLockHead);

    if (prev_mode != 0)
        *prev_mode = _prev_mode;
    if (prev_pgmode != 0)
        *prev_pgmode = _prev_pgmode;
    return rc;
}

rc_t
lock_m::_lock(
    const lockid_t&         n,
    lmode_t                 m,
    lmode_t&                prev_mode,
    lmode_t&                prev_pgmode,
    duration_t              duration,
    timeout_in_ms           timeout, 
    lockid_t**              nameInLockHead
    )
{
    FUNC(lock_m::_lock);
    xct_t*                 xd = xct();
    if (xd == NULL) {
        return RCOK;
    }

    w_rc_t                 rc; // == RCOK
    xct_lock_info_t*       theLockInfo = 0;
    lock_request_t*        theLastLockReq = 0;

    INC_TSTAT(lock_request_cnt);

    prev_mode = NL;
    prev_pgmode = NL;

    switch (timeout) {
        case WAIT_SPECIFIED_BY_XCT:
            timeout = xd->timeout_c();
            break;
            // DROP THROUGH to WAIT_SPECIFIED_BY_THREAD ...
            // (whose default is WAIT_FOREVER)

        case WAIT_SPECIFIED_BY_THREAD:
            timeout = me()->lock_timeout();
            break;
    
        default:
            break;
    }

    w_assert9(timeout >= 0 || timeout == WAIT_FOREVER);

    // The lock info is created with the xct constructor.
    theLockInfo = xd->lock_info();

    // This ensures that no two threads can be locking
    // on behalf of the same xct at the same time.
    W_COERCE(theLockInfo->lock_info_mutex.acquire());

    if(theLastLockReq) {
        DBGTHRD(<< "theLastLockReq :" << *theLastLockReq);
    }

    // do the locking protocol.
    lmode_t                ret;
    lock_request_t*        req = 0;

    DBG(<< "need mode " << m);

    w_rc_t::errcode_t rce(eOK);
    do {
        rce = _core->acquire_lock(xd, 
                n, /* lockid_t */
                NULL /* lock */, 
                &req, /* out: request */
                m,  /* needed mode */
                prev_mode,  /* out: previously-held mode */
                duration, 
                timeout, 
                ret /* out: new mode */
                );
    } while (rce && (rce == eRETRY));

#if W_DEBUG_LEVEL >= 3
    if (rce && rce == eDEADLOCK) {
        w_ostrstream s;
        s  << n << " mode " << m;
        fprintf(stderr, 
            "Deadlock detected acquiring %s\n", s.c_str());
    }
#endif
    if (rce) {
        rc = RC(rce);
        goto done;
    }

    if (duration == t_instant) {
#if W_DEBUG_LEVEL > 0
        w_assert1(MUTEX_IS_MINE(theLockInfo->lock_info_mutex));
        if(prev_mode == NL) {
            w_assert1(req->get_count() == 1);
        }
#endif
        W_COERCE( _core->release_lock(theLockInfo, n, 
                    req->get_lock_head(), req, false) );

    }

done:
    w_assert1(theLockInfo != 0);
    W_VOID(theLockInfo->lock_info_mutex.release());

    if (!rc.is_error() && nameInLockHead)  {
        /* XXX This is a problem, it happens! */
        w_assert3(theLastLockReq);
        *nameInLockHead = &theLastLockReq->get_lock_head()->name;
    }
    return rc;
}

lil_lock_modes_t to_lil_mode (lock_base_t::lmode_t m) {
    switch (m) {
        case lock_base_t::IS: return LIL_IS;
        case lock_base_t::IX: return LIL_IX;
        case lock_base_t::SH: return LIL_S;
        case lock_base_t::EX: return LIL_X;
        default:
            w_assert1(false); // shouldn't reach here!
    }
    return LIL_IS;// shouldn't reach here!
}

rc_t lock_m::intent_vol_lock(vid_t vid, lmode_t m)
{
    lil_lock_modes_t mode = to_lil_mode(m);
    xct_t *xd = xct();
    if (xd == NULL) {
        return RCOK;
    }
    
    lil_global_table *global_table = get_lil_global_table();
    lil_private_table* private_table = xd->lil_lock_info();
    lil_private_vol_table *vol_table;
    W_DO(private_table->acquire_vol_table(global_table, vid.vol, mode, vol_table));
    
    return RCOK;
}

rc_t lock_m::intent_store_lock(const stid_t &stid, lmode_t m)
{
    lil_lock_modes_t mode = to_lil_mode(m);
    xct_t *xd = xct();
    if (xd == NULL) {
        return RCOK;
    }
    lil_global_table *global_table = get_lil_global_table();
    lil_private_table* private_table = xd->lil_lock_info();
    // get volume lock table without requesting locks.
    lil_private_vol_table *vol_table = private_table->find_vol_table(stid.vol);
    // only request store lock
    W_DO(vol_table->acquire_store_lock(global_table, stid, mode));
    return RCOK;
}

rc_t lock_m::intent_vol_store_lock(const stid_t &stid, lmode_t m)
{
    lil_lock_modes_t mode = to_lil_mode(m);
    xct_t *xd = xct();
    if (xd == NULL) {
        return RCOK;
    }
    lil_global_table *global_table = get_lil_global_table();
    lil_private_table* private_table = xd->lil_lock_info();
    W_DO(private_table->acquire_vol_store_lock(global_table, stid, mode));
    return RCOK;
}

rc_t
lock_m::unlock(const lockid_t& n)
{
    FUNC(lock_m::unlock);
    DBGTHRD(<< "unlock " << n );

    xct_t*         xd = xct();
    w_rc_t         rc; // == RCOK
    if (xd)  {
        W_COERCE(xd->lock_info()->lock_info_mutex.acquire());
        rc = _core->release_lock(xd->lock_info(), n, 0, 0, false);
        W_VOID(xd->lock_info()->lock_info_mutex.release());
    }

    INC_TSTAT(unlock_request_cnt);
    return rc;
}

/* 
 * Free all locks of a given duration
 *  release not just those whose
 *     duration matches, but all those which shorter duration also
 */
rc_t lock_m::unlock_duration(
    duration_t          duration, bool read_lock_only
	)
{
    FUNC(lock_m::unlock_duration);
    DBGTHRD(<< "lock_m::unlock_duration" 
        << " duration=" << int(duration)
    );

    xct_t*         xd = xct();
    w_rc_t        rc;        // == RCOK
    
    if (xd)  {
        // First, release intent locks on LIL
        lil_global_table *global_table = get_lil_global_table();
        lil_private_table *private_table = xd->lil_lock_info();
        private_table->release_all_locks(global_table, read_lock_only);

        // then, release non-intent locks
        xct_lock_info_t* theLockInfo = xd->lock_info();
        do  {
            W_COERCE(theLockInfo->lock_info_mutex.acquire());

            rc =  _core->release_duration(theLockInfo, duration, read_lock_only);

            W_VOID(theLockInfo->lock_info_mutex.release());
        }  
        while (0);
    }
    return rc;
}
