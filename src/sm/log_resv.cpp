/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

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

/*<std-header orig-src='shore'>

 $Id: log_core.cpp,v 1.20 2010/12/08 17:37:42 nhall Exp $

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
#define LOG_RESV_C

#include "chkpt.h"
#include "log_resv.h"
#include "log_storage.h"
#include "log_lsn_tracker.h"

log_resv::log_resv(log_storage* storage)
    :
      _storage(storage),
      _reservations_active(false),
      _space_available(0),
      _space_rsvd_for_chkpt(0),
      _waiting_for_space(false)
{
    DO_PTHREAD(pthread_mutex_init(&_space_lock, 0));
    DO_PTHREAD(pthread_cond_init(&_space_cond, 0));

    _oldest_lsn_tracker = new PoorMansOldestLsnTracker(1 << 20);
    w_assert1(_oldest_lsn_tracker);

    // initial free space estimate... refined once log recovery is complete
    release_space(_storage->recoverable_space(PARTITION_COUNT));

    if (smlevel_0::bf) {
        if(!verify_chkpt_reservation()
                || space_for_chkpt() > _storage->partition_data_size()) {
            cerr<<
                "log partitions too small compared to buffer pool:"<<endl
                <<"    "<<_storage->partition_data_size()
                <<" bytes per partition available"<<endl
                <<"    "<<space_for_chkpt()
                <<" bytes needed for checkpointing dirty pages"<<endl;
            W_FATAL(eOUTOFLOGSPACE);
        }
    }
}

log_resv::~log_resv()
{
    DO_PTHREAD(pthread_mutex_destroy(&_space_lock));
    DO_PTHREAD(pthread_cond_destroy(&_space_cond));

    delete _oldest_lsn_tracker;
    _oldest_lsn_tracker = NULL;
}


rc_t log_resv::wait_for_space(fileoff_t &amt, timeout_in_ms timeout)
{
    DBG(<<"log_resv::wait_for_space " << amt);
    // if they're asking too much don't even bother
    if(amt > _storage->partition_data_size()) {
        return RC(eOUTOFLOGSPACE);
    }

    // wait for a signal or 100ms, whichever is longer...
    w_assert1(amt > 0);
    struct timespec when;
    if(timeout != WAIT_FOREVER)
        sthread_t::timeout_to_timespec(timeout, when);

    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    waiting_xct* wait = new waiting_xct(&amt, &cond);
    DO_PTHREAD(pthread_mutex_lock(&_space_lock));
    _waiting_for_space = true;
    _log_space_waiters.push_back(wait);
    while(amt) {
        /* First time through, someone could have freed up space
           before we acquired this mutex. 2+ times through, maybe our
           previous rounds got us enough that the normal log
           reservation can supply what we still need.
         */
        if(reserve_space(amt)) {
            amt = 0;

            // nullify our entry. Non-racy beause amt > 0 and we hold the mutex
            wait->needed = 0;

            // clean up in case it's pure false alarms
            while(_log_space_waiters.size() && ! _log_space_waiters.back()->needed) {
                delete _log_space_waiters.back();
                _log_space_waiters.pop_back();
            }
            break;
        }
        DBGOUT3(<< "chkpt 3");

        if(smlevel_0::chkpt != NULL) smlevel_0::chkpt->wakeup_and_take();
        if(timeout == WAIT_FOREVER) {
            cerr<<
            "* - * - * tid "<<xct()->tid().get_hi()<<"."<<xct()->tid().get_lo()<<" waiting forever for "<<amt<<" bytes of log" <<endl;
            DO_PTHREAD(pthread_cond_wait(&cond, &_space_lock));
        } else {
            cerr<<
                "* - * - * tid "<<xct()->tid().get_hi()<<"."<<xct()->tid().get_lo()<<" waiting with timeout for "<<amt<<" bytes of log"<<endl;
                int err = pthread_cond_timedwait(&cond, &_space_lock, &when);
                if(err == ETIMEDOUT)
                break;
        }
    }
    cerr<<"* - * - * tid "<<xct()->tid().get_hi()<<"."<<xct()->tid().get_lo()<<" done waiting ("<<amt<<" bytes still needed)" <<endl;

    DO_PTHREAD(pthread_mutex_unlock(&_space_lock));
    return amt? RC(stTIMEOUT) : RCOK;
}

void log_resv::release_space(fileoff_t amt)
{
    DBG(<<"log_resv::release_space " << amt);
    w_assert1(amt >= 0);
    /* NOTE: The use of _waiting_for_space is purposefully racy
       because we don't want to pay the cost of a mutex for every
       space release (which should happen every transaction
       commit...). Instead waiters use a timeout in case they fall
       through the cracks.

       Waiting transactions are served in FIFO order; those which time
       out set their need to -1 leave it for release_space to clean
       it up.
     */
    if(_waiting_for_space) {
        DO_PTHREAD(pthread_mutex_lock(&_space_lock));
        while(amt > 0 && _log_space_waiters.size()) {
            bool finished_one = false;
            waiting_xct* wx = _log_space_waiters.front();
            if( ! wx->needed) {
                finished_one = true;
            }
            else {
                fileoff_t can_give = std::min(amt, *wx->needed);
                *wx->needed -= can_give;
                amt -= can_give;
                if(! *wx->needed) {
                    DO_PTHREAD(pthread_cond_signal(wx->cond));
                    finished_one = true;
                }
            }

            if(finished_one) {
                delete wx;
                _log_space_waiters.pop_front();
            }
        }
        if(_log_space_waiters.empty()) {
            _waiting_for_space = false;
        }

        DO_PTHREAD(pthread_mutex_unlock(&_space_lock));
    }

    lintel::unsafe::atomic_fetch_add<fileoff_t>(&_space_available, amt);
}


/*********************************************************************
 *
 *  log_resv::scavenge(min_rec_lsn, min_xct_lsn)
 *
 *  Scavenge (free, reclaim) unused log files.
 *  We can scavenge all log files with index less
 *  than the minimum of the three lsns:
 *  the two arguments
 *  min_rec_lsn,  : minimum recovery lsn computed by checkpoint
 *  min_xct_lsn,  : first log record written by any uncommitted xct
 *  and
 *  global_min_lsn: the smaller of :
 *     min chkpt rec lsn: min_rec_lsn computed by the last checkpoint
 *     master_lsn: lsn of the last completed checkpoint-begin
 * (so the min chkpt rec lsn is in here twice - that's ok)
 *
 *********************************************************************/
rc_t
log_resv::scavenge(const lsn_t &min_rec_lsn, const lsn_t& min_xct_lsn)
{
    FUNC(log_resv::scavenge);
    _storage->acquire_partition_lock();
    _storage->acquire_scavenge_lock();

#if W_DEBUG_LEVEL > 2
    //_sanity_check();
#endif
    lsn_t lsn = std::min(std::min(_storage->global_min_lsn(), min_rec_lsn), min_xct_lsn);
    int count = _storage->delete_old_partitions(lsn);

    if(count > 0) {
        /* LOG_RESERVATIONS

           reinstate the log space from the reclaimed partitions. We
           can put back the entire partition size because every log
           insert which finishes off a partition will consume whatever
           unused space was left at the end.

           Skim off the top of the released space whatever it takes to
           top up the log checkpoint reservation.
         */
        fileoff_t reclaimed = _storage->recoverable_space(count);
        fileoff_t max_chkpt = max_chkpt_size();
        while(!verify_chkpt_reservation() && reclaimed > 0) {
            long skimmed = std::min(max_chkpt, reclaimed);
            lintel::unsafe::atomic_fetch_add(const_cast<int64_t*>(&_space_rsvd_for_chkpt), skimmed);
            reclaimed -= skimmed;
        }
        release_space(reclaimed);
        _storage->signal_scavenge_cond();
    }
    _storage->release_scavenge_lock();
    _storage->release_partition_lock();

    return RCOK;
}

/* Compute size of the biggest checkpoint we ever risk having to take...
 */
long log_resv::max_chkpt_size() const
{
    /* BUG: the number of transactions which might need to be
       checkpointed is potentially unbounded. However, it's rather
       unlikely we'll ever see more than 5k at any one time, especially
       each active transaction uses an active user thread

       The number of granted locks per transaction is also potentially
       unbounded.  Use a guess average value per active transaction,
       it should be unusual to see maximum active transactions and every
       transaction has the average number of locks
     */
    static long const GUESS_MAX_XCT_COUNT = 5000;
    static long const GUESS_EACH_XCT_LOCK_COUNT = 5;
    static long const FUDGE = sizeof(logrec_t);
    long bf_tab_size = smlevel_0::bf->get_block_cnt()*sizeof(chkpt_bf_tab_t::brec_t);
    long xct_tab_size = GUESS_MAX_XCT_COUNT*sizeof(chkpt_xct_tab_t::xrec_t);
    long xct_lock_size = GUESS_EACH_XCT_LOCK_COUNT*GUESS_MAX_XCT_COUNT*sizeof(chkpt_xct_lock_t::lockrec_t);
    return FUDGE + bf_tab_size + xct_tab_size + xct_lock_size;
}

rc_t
log_resv::file_was_archived(const char * /*file*/)
{
    // TODO: should check that this is the oldest,
    // and that we indeed asked for it to be archived.
    _space_available += _storage->recoverable_space(1);
    return RCOK;
}

void
log_resv::activate_reservations(const lsn_t& curr_lsn)
{
    /* With recovery complete we now activate log reservations.

       In fact, the activation should be as simple as setting the mode to
       t_forward_processing, but we also have to account for any space
       the log already occupies. We don't have to double-count
       anything because nothing will be undone should a crash occur at
       this point.
     */
    w_assert1(smlevel_0::operating_mode == smlevel_0::t_forward_processing);
    // FRJ: not true if any logging occurred during recovery
    // w_assert1(PARTITION_COUNT*_partition_data_size ==
    //       _space_available + _space_rsvd_for_chkpt);
    w_assert1(!_reservations_active);

    // knock off space used by full partitions
    long oldest_pnum = _storage->min_chkpt_rec_lsn().hi();
    long newest_pnum = curr_lsn.hi();
    long full_partitions = newest_pnum - oldest_pnum; // can be zero
    _space_available -= _storage->recoverable_space(full_partitions);

    // and knock off the space used so far in the current partition
    _space_available -= curr_lsn.lo();
    _reservations_active = true;
    // NOTE: _reservations_active does not get checked in the
    // methods that reserve or release space, so reservations *CAN*
    // happen during recovery.

    // not mt-safe
    smlevel_0::errlog->clog << info_prio
        << "Activating reservations: # full partitions "
            << full_partitions
            << ", space available " << space_left()
        << endl
            << ", oldest partition " << oldest_pnum
            << ", newest partition " << newest_pnum
            << ", # partitions " << PARTITION_COUNT
        << endl ;
}

fileoff_t log_resv::take_space(fileoff_t *ptr, int amt)
{
    BOOST_STATIC_ASSERT(sizeof(fileoff_t) == sizeof(int64_t));
    fileoff_t ov = lintel::unsafe::atomic_load(const_cast<int64_t*>(ptr));
    // fileoff_t ov = *ptr;
#if W_DEBUG_LEVEL > 0
    DBGTHRD("take_space " << amt << " old value of ? " << ov);
#endif
    while(1) {
        if (ov < amt) {
            return 0;
        }
	fileoff_t nv = ov - amt;
	if (lintel::unsafe::atomic_compare_exchange_strong(const_cast<int64_t*>(ptr), &ov, nv)) {
	    return amt;
        }
    }
}

fileoff_t log_resv::reserve_space(fileoff_t amt)
{
    return (amt > 0)? take_space(&_space_available, amt) : 0;
}

fileoff_t log_resv::consume_chkpt_reservation(fileoff_t amt)
{
    if(smlevel_0::operating_mode != smlevel_0::t_forward_processing)
       return amt; // not yet active -- pretend it worked

    return (amt > 0)?
        take_space(&_space_rsvd_for_chkpt, amt) : 0;
}

// make sure we have enough log reservation (conservative)
// NOTE: this has to be compared with the size of a partition,
// which _set_size does (it knows the size of a partition)
bool log_resv::verify_chkpt_reservation()
{
    fileoff_t space_needed = max_chkpt_size();
    while(*&_space_rsvd_for_chkpt < 2*space_needed) {
        if(reserve_space(space_needed)) {
            // abuse take_space...
            take_space(&_space_rsvd_for_chkpt, -space_needed);
        } else if(*&_space_rsvd_for_chkpt < space_needed) {
            /* oops...

               can't even guarantee the minimum of one checkpoint
               needed to reclaim log space and solve the problem
             */
            W_FATAL(eOUTOFLOGSPACE);
        } else {
            // must reclaim a log partition
            return false;
        }
    }
    return true;
}
