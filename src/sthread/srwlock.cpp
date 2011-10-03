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

 $Id: srwlock.cpp,v 1.7 2010/12/08 17:37:50 nhall Exp $

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

#include <w.h>
#include <w_debug.h>
#include <sthread.h>

/************************************************************************************
 * mcs_rwlock implementation; cheaper but problematic when we get os preemptions
 */

// CC mangles this as __1cKmcs_rwlockOspin_on_writer6M_v_
// private
int mcs_rwlock::_spin_on_writer() 
{
    int cnt=0;
    while(has_writer()) cnt=1;
    // callers do membar_enter
    return cnt;
}
// CC mangles this as __1cKmcs_rwlockPspin_on_readers6M_v_
// private
void mcs_rwlock::_spin_on_readers() 
{
    while(has_reader());
    // callers do membar_enter
}

// private
void mcs_rwlock::_add_when_writer_leaves(int delta) 
{
    // we always have the parent lock to do this
    int cnt = _spin_on_writer();
    atomic_add_32(&_holders, delta);
    // callers do membar_enter
    if(cnt  && (delta == WRITER)) {
        INC_STH_STATS(rwlock_w_wait);
    }
}

bool mcs_rwlock::attempt_read() 
{
    unsigned int old_value = *&_holders;
    if(old_value & WRITER || 
        old_value != atomic_cas_32(&_holders, old_value, old_value+READER))
        return false;

    membar_enter();
    return true;
}

void mcs_rwlock::acquire_read() 
{
    /* attempt to CAS first. If no writers around, or no intervening
     * add'l readers, we're done
     */
    if(!attempt_read()) {
        INC_STH_STATS(rwlock_r_wait);
        /* There seem to be writers around, or other readers intervened in our
         * attempt_read() above.
         * Join the queue and wait for them to leave 
         */
        {
            CRITICAL_SECTION(cs, (parent_lock*) this);
            _add_when_writer_leaves(READER);
        }
        membar_enter();
    }
}

void mcs_rwlock::release_read() 
{
    w_assert2(has_reader());
    membar_exit(); // flush protected modified data before releasing lock;
    // update and complete any loads by others before I do this write 
    atomic_add_32(&_holders, -READER);
}

bool mcs_rwlock::_attempt_write(unsigned int expected) 
{
    /* succeeds iff we are the only reader (if expected==READER)
     * or if there are no readers or writers (if expected==0)
     *
     * How do we know if the only reader is us?
     * A:  we rely on these facts: 
     * this is called with expected==READER only from attempt_upgrade(), 
     *   which is called from latch only in the case
     *   in which we hold the latch in LATCH_SH mode and 
     *   are requesting it in LATCH_EX mode.
     *
     * If there is a writer waiting we have to get in line 
     * like everyone else.
     * No need for a membar because we already hold the latch
     */

// USE_PTHREAD_MUTEX is determined by configure option and
// thus defined in config/shore-config.h
#if defined(USE_PTHREAD_MUTEX) && USE_PTHREAD_MUTEX==1
    ext_qnode me = QUEUE_EXT_QNODE_INITIALIZER;
#else
    ext_qnode me;
    QUEUE_EXT_QNODE_INITIALIZE(me);
#endif

    if(*&_holders != expected || !attempt(&me))
        return false;
    // at this point, we've called mcs_lock::attempt(&me), and
    // have acquired the parent/mcs lock
    // The following line replaces our reader bit with a writer bit.
    bool result = (expected == atomic_cas_32(&_holders, expected, WRITER));
    release(me); // parent/mcs lock
    membar_enter();
    return result;
}

bool mcs_rwlock::attempt_write() 
{
    if(!_attempt_write(0))
        return false;
    
    // moved to end of _attempt_write() membar_enter();
    return true;
}

void mcs_rwlock::acquire_write() 
{
    /* always join the queue first.
     *
     * 1. We don't want to race with other writers
     *
     * 2. We don't want to make readers deal with the gap between
     * us updating _holders and actually acquiring the MCS lock.
     */
    CRITICAL_SECTION(cs, (parent_lock*) this);
    _add_when_writer_leaves(WRITER);
    w_assert1(has_writer()); // me!

    // now wait for existing readers to clear out
    if(has_reader()) {
        INC_STH_STATS(rwlock_w_wait);
        _spin_on_readers();
    }

    // done!
    membar_enter();
}

void mcs_rwlock::release_write() {
    membar_exit(); // flush protected modified data before releasing lock;
    w_assert1(*&_holders == WRITER);
    *&_holders = 0;
}

bool mcs_rwlock::attempt_upgrade() 
{
    w_assert1(has_reader());
    return _attempt_write(READER);
}

void mcs_rwlock::downgrade() 
{
    membar_exit();  // this is for all intents and purposes, a release
    w_assert1(*&_holders == WRITER);
    *&_holders = READER;
    membar_enter(); // but it's also an acquire
}
