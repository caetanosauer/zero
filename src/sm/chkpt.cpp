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

 $Id: chkpt.cpp,v 1.81 2010/07/29 21:22:46 nhall Exp $

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
#define CHKPT_C

#include "sm_base.h"
#include "chkpt_serial.h"
#include "chkpt.h"
#include "logdef_gen.cpp"
#include "bf_tree.h"
#include "xct_dependent.h"
#include <new>
#include "sm.h"
#include "lock_raw.h"      // Lock information gathering
#include "w_okvl_inl.h"    // Lock information gathering
struct RawLock;            // Lock information gathering
#include "restart.h"
#include "vol.h"


#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_array_t<lsn_t>;
template class w_auto_delete_array_t<tid_t>;
template class w_auto_delete_array_t<ss_m::xct_state_t>;
#endif


/*********************************************************************
 *
 *  class chkpt_thread_t
 *
 *  Checkpoint thread.
 *
 *********************************************************************/
class chkpt_thread_t : public smthread_t
{
public:
    NORET                chkpt_thread_t();
    NORET                ~chkpt_thread_t();

    virtual void        run();
    void                retire();
    void                awaken();
    bool                is_retired() {return _retire;}

private:
    bool                _retire;
    pthread_mutex_t     _retire_awaken_lock; // paired with _retire_awaken_cond
    pthread_cond_t      _retire_awaken_cond; // paried with _retire_awaken_lock
    bool                _kicked;

    // Simple counter to keep track total pending checkpoint requests
    unsigned int        chkpt_count;

    // disabled
    NORET                chkpt_thread_t(const chkpt_thread_t&);
    chkpt_thread_t&      operator=(const chkpt_thread_t&);
};


/*****************************************************
// Dead code, comment out just in case we need to re-visit it in the future

 // Not waiting on old transactions to finish

struct old_xct_tracker {
    struct dependent : public xct_dependent_t  {
        w_link_t _link;
        old_xct_tracker* _owner;

        dependent(xct_t* xd, old_xct_tracker* owner)
            : xct_dependent_t(xd), _owner(owner)
        {
            register_me();
        }

        virtual void xct_state_changed(ss_m::xct_state_t,
              ss_m::xct_state_t new_state)
        {
            if(new_state == ss_m::xct_ended)
            _owner->report_finished(xd());
        }
    };

    old_xct_tracker() : _list(W_LIST_ARG(dependent, _link), 0) , _count(0)
    {
        pthread_mutex_init(&_lock, 0);
        pthread_cond_init(&_cond, 0);
    }

    ~old_xct_tracker() {
        w_assert2(! _count);
        while(_list.pop());
    }

    void track(xct_t* xd) {
        dependent* d = new dependent(xd, this);
        pthread_mutex_lock(&_lock);
        _count++;
        _list.push(d);
        pthread_mutex_unlock(&_lock);
    }

    void wait_for_all() {
        pthread_mutex_lock(&_lock);
        while(_count)
            pthread_cond_wait(&_cond, &_lock);
        pthread_mutex_unlock(&_lock);
    }

    void report_finished(xct_t*) {
        pthread_mutex_lock(&_lock);
        if(! --_count)
            pthread_cond_signal(&_cond);
        pthread_mutex_unlock(&_lock);
    }

    pthread_mutex_t    _lock;
    pthread_cond_t     _cond;
    w_list_t<dependent, unsafe_list_dummy_lock_t> _list;
    long             _count;

};
*****************************************************/


/*********************************************************************
 *
 *  chkpt_m::chkpt_m()
 *
 *  Constructor for Checkpoint Manager.
 *
 *********************************************************************/
NORET
chkpt_m::chkpt_m()
    : _chkpt_thread(0), _chkpt_count(0)
{
}

/*********************************************************************
 *
 *  chkpt_m::~chkpt_m()
 *
 *  Destructor. If a thread is spawned, tell it to exit.
 *
 *********************************************************************/
NORET
chkpt_m::~chkpt_m()
{
    if (_chkpt_thread)
    {
        retire_chkpt_thread();
    }

    // Destruct the chkpt_m thresd (itself), if it is a simulated crash,
    // the system might be in the middle of taking a checkpoint, and
    // the chkpt_m thread has the chekpt chkpt_serial_m mutex and won't have
    // a chance to release the mutex
    // We cannot blindly release the mutex because it will get into infinite wait
    // in such case, we might see debug assert (core dump) from
    //     ~w_pthread_lock_t() { w_assert1(!_holder); pthread_mutex_destroy(&_mutex);}
}


/*********************************************************************
 *
 *  chkpt_m::spawn_chkpt_thread()
 *
 *  Fork the checkpoint thread.
 *  Caller should spawn the chkpt thread immediatelly after
 *  the chkpt_m has been created.
 *
 *********************************************************************/
void
chkpt_m::spawn_chkpt_thread()
{
    w_assert1(_chkpt_thread == 0);
    if (ss_m::log)
    {
        // Create thread (1) to take checkpoints
        _chkpt_thread = new chkpt_thread_t;
        if (! _chkpt_thread)
            W_FATAL(eOUTOFMEMORY);
        W_COERCE(_chkpt_thread->fork());
    }
}



/*********************************************************************
 *
 *  chkpt_m::retire_chkpt_thread()
 *
 *  Kill the checkpoint thread.
 *
 *  Called from:
 *      chkpt_m destructor
 *      ss_m::_destruct_once() - shutdown storage manager, signal chkpt thread
 *                                            to retire before destroy chkpt_m
 *
 *********************************************************************/
void
chkpt_m::retire_chkpt_thread()
{
    if (ss_m::log)
    {
        w_assert1(_chkpt_thread);

        // Notify the checkpoint thread to retire itself
        _chkpt_thread->retire();
        W_COERCE( _chkpt_thread->join() ); // wait for it to end
        delete _chkpt_thread;
        _chkpt_thread = 0;
    }
}

/*********************************************************************
*
*  chkpt_m::wakeup_and_take()
*
*  Issue an asynch checkpoint request
*
*********************************************************************/
void
chkpt_m::wakeup_and_take()
{
    if(ss_m::log && _chkpt_thread)
    {
        INC_TSTAT(log_chkpt_wake);
        _chkpt_thread->awaken();
    }
}

/*********************************************************************
*
*  chkpt_m::synch_take()
*
*  Issue an synch checkpoint request
*
*********************************************************************/
void chkpt_m::synch_take()
{
    if(ss_m::log)
    {
        // No need to acquire any mutex on checkpoint before calling the checkpoint function
        // The checkpoint function acqures the 'write' mutex internally
        // Sync checkpoint and caller did not ask for lock information
        CmpXctLockTids   lock_cmp;
        XctLockHeap      dummy_heap(lock_cmp);

        take(t_chkpt_sync, dummy_heap);
        w_assert1(0 == dummy_heap.NumElements());
    }
    return;
}


/*********************************************************************
*
*  chkpt_m::synch_take(lock_heap)
*
*  Issue an synch checkpoint request and record the lock information in heap
*
*********************************************************************/
void chkpt_m::synch_take(XctLockHeap& lock_heap)
{
    if(ss_m::log)
    {
        // No need to acquire any mutex on checkpoint before calling the checkpoint function
        // The checkpoint function acqures the 'write' mutex internally
        // Record lock information only from synch checkpoint and if caller asked for it
        take(t_chkpt_sync, lock_heap, true);
    }
    return;
}


/*********************************************************************
 *
 *  chkpt_m::take(chkpt_mode, lock_heap, record_lock)
 *
 *  This is the actual checkpoint function, which can be executed in two different ways:
 *    1. Asynch request: does not block caller thread, i.e. user-requested checkpoint
 *    2. Synch execution: blocks the caller thread
 *
 *  A checkpoint request does not mean a checkpoint will be executed:
 *    1. System is already in the middle of shutting down.  Note that shutdown process
 *        issues a synchronous checkpoint as part of shutdown
 *    2. System started but recovery has not started yet
 *    3. In the middle of recovery, exceptions
 *          - at the end of Log Analysis phase - synchronous checkpoint
 *          - at the end of UNDO phase - synchronous checkpoint
 *
 *  Take a checkpoint. A Checkpoint consists of:
 *    1. Checkpoint Begin Log    (chkpt_begin)
 *    2. Checkpoint Device Table Log(s) (chkpt_dev_tab)
 *        -- all mounted devices
 *    3. Checkpoint Buffer Table Log(s)  (chkpt_bf_tab)
 *        -- dirty page entries in bf and their recovery lsn
 *        -- can have multiple such log records if many dirty pages
 *    4. Checkpoint per transaction lock info log(s) (chkpt_xct_lock)
 *        -- granted locks on an active transactions
 *        -- can have multiple such log records per active transaction
 *             if many granted locks
 *    5. Checkpoint Transaction Table Log(s) (chkpt_xct_tab)
 *        -- active transactions and their first lsn
 *        -- can have multiple such log records if many active transactions
 *    6. Checkpoint End Log (chkpt_end)
 *
 *  Because a checkpoint can be executed either asynch or synch, it cannot return
 *  a return code to caller.
 *  A catastrophic error takes the server down (e.g., not able to make the new master
 *  lsn durable, out-of-log space, etc.)
 *  A minor error (e.g., failed the initial validation) generates a debug output,
 *  and cancel the checkpoint operation silently.
 *********************************************************************/
void chkpt_m::take(chkpt_mode_t chkpt_mode,
                    XctLockHeap& lock_heap,  // In: special heap to hold lock information
                    const bool record_lock)  // In: True if need to record lock information into the heap
{
    FUNC(chkpt_m::take);

    if (t_chkpt_async == chkpt_mode)
    {
        DBGOUT3(<< "Checkpoint request: asynch");
    }
    else
    {
        DBGOUT3(<< "Checkpoint request: synch");
    }

    // Note: on a clean system shutdown ends the log with a checkpoint, but this
    // checkpoint is empty excpet device information: no dirty page and
    // no in-flight transaction

    // It is possible multiple checkpoint requests arrived concurrently.  Despite at most
    // one asynch checkpoint request at any time, but we might have multiple synch
    // checkpoiint requests.
    // This is okay because we will serialize the requests using a 'write' mutex and
    // not lose any of the checkpoint request, although some of the requests might
    // need to wait for a while (blocking)

    if (!ss_m::log)
    {
        // recovery facilities disabled ... do nothing
        return;
    }

    if ((t_chkpt_async == chkpt_mode) && (true == record_lock))
    {
        // Only synch checkpoint (internal) can ask for building lock information in the provided heap
        // this is a coding error so raise error to stop the execution now
        W_FATAL_MSG(fcINTERNAL, << "Only synch (internal) checkpoint can ask for building lock heap");
        return;
    }
    w_assert1(0 == lock_heap.NumElements());

    /*
     * checkpoints are fuzzy
     * but must be serialized wrt each other.
     *
     * Acquire the 'write' mutex immediatelly to serialize concurrent checkpoint requests.
     *
     * NB: EVERYTHING BETWEEN HERE AND RELEASING THE MUTEX
     * MUST BE W_COERCE (not W_DO).
     *
     * The W_COERCE is like W_DO(x), but instead of returning in the error
     * case, it fails catastrophically.
     * It is used in checkpoint function because
     *   Checkpoint has no means to return error information
     */
    chkpt_serial_m::write_acquire();
    DBGOUT1(<<"BEGIN chkpt_m::take");

    // Update statistics
    INC_TSTAT(log_chkpt_cnt);

    // Note: current code in ss_m::_destruct_once() sets the shutting_down flag first, then
    // 1. For clean shutdown, it takes a synchronous checkpoint and then
    //    retires the checkpoint thread
    // 2. For dirty shutdown, it retires the checkpoint thread immediatelly (which might still be
    //    working on a checkpoint triggered by user).
    //
    //Checkpoint can be activated 2 ways:
    // 1. Calling 'wakeup_and_take' - asynch
    // 2. Calling 'chkpt_m::synch_take' - synch

    // Start the initial validation check for make sure the incoming checkpoint request is valid
    bool valid_chkpt = true;

    if (ss_m::log && _chkpt_thread)
    {
        // Log is on and chkpt thread is available, but the chkpt thread has received
        // a 'retire' message (signal to shutdown), return without doing anything
        if (true == _chkpt_thread->is_retired())
        {
            DBGOUT1(<<"END chkpt_m::take - detected retire, skip checkpoint");
            valid_chkpt = false;
        }
    }
    else if ((ss_m::shutting_down) && (t_chkpt_async == chkpt_mode))
    {
        // No asynch checkpoint if we are shutting down
        DBGOUT1(<<"END chkpt_m::take - detected shutdown, skip asynch checkpoint");
        valid_chkpt = false;
    }
    else if ((ss_m::shutting_down) && (t_chkpt_sync == chkpt_mode))
    {
        // Middle of shutdown, allow synch checkpoint request
        DBGOUT1(<<"PROCESS chkpt_m::take - system shutdown, allow synch checkpoint");
    }
    else
    {
        // Not in shutdown
        if (ss_m::before_recovery())
        {
            DBGOUT1(<<"END chkpt_m::take - before system startup/recovery, skip checkpoint");
            valid_chkpt = false;
        }
        else if (ss_m::ss_m::in_recovery() && (t_chkpt_sync != chkpt_mode))
        {
            // Asynch checkpoint
            if (false == ss_m::use_serial_restart())
            {
                // System opened after Log Analysis phase, allow asynch checkpoint
                // after Log Analysis phase
                if (ss_m::ss_m::in_recovery_analysis())
                    valid_chkpt = false;
            }
            else
            {
                // System is not opened during recovery
                DBGOUT1(<<"END chkpt_m::take - system in recovery, skip asynch checkpoint");
                valid_chkpt = false;
            }
        }
        else if (ss_m::in_recovery() && (t_chkpt_sync == chkpt_mode))
        {
            // Synch checkpoint
            if (false == ss_m::use_serial_restart())
            {
                // System opened after Log Analysis phase, accept system checkpoint anytime
                DBGOUT1(<<"PROCESS chkpt_m::take - system in recovery, allow synch checkpoint");
            }
            else
            {
                // System is not opened during recovery
                if (ss_m::in_recovery_analysis() || ss_m::in_recovery_undo())
                {
                    DBGOUT1(<<"PROCESS chkpt_m::take - system in recovery, allow synch checkpoint");
                }
                else
                {
                    DBGOUT1(<<"END chkpt_m::take - system in REDO phase, disallow checkpoint");
                    valid_chkpt = false;
                }
            }
        }
        else
        {
            // We cannot be in recovery if we get here
            if (true == ss_m::in_recovery())
            {
                DBGOUT1(<<"END chkpt_m::take - system should not be in Recovery, exist checkpoint");
                valid_chkpt = false;
            }
            else
            {
                if (t_chkpt_sync == chkpt_mode)
                {
                    DBGOUT1(<<"PROCESS chkpt_m::take - allow synch/internal checkpoint");
                }
                else
                {
                    DBGOUT1(<<"PROCESS chkpt_m::take - allow asynch/user checkpoint");
                }
            }
        }
    }

    // Optimization idea: if the last log record in the recovery log is 'end checkpoint' then
    // no new activities since last completed checkpoint, should we skip this checkpoint request?
    // No, becasue even without new log record, there still might be buffer pool flush after
    // the previous completed checkpoint (note checkpoint is a non-blocking operation),
    // we should take a checkpoint just to be safe

    // Done with the checkpoint validation, should we continue?
    if (false == valid_chkpt)
    {
        // Failed the checkpoint validation, release the 'write' mutex and exist
        chkpt_serial_m::write_release();
        return;
    }

    // We are okay to proceed with the checkpoint process

    // Allocate a buffer for storing log records
    w_auto_delete_t<logrec_t> logrec(new logrec_t);


#define LOG_INSERT(constructor_call, rlsn)            \
    do {                                              \
        new (logrec) constructor_call;                \
        W_COERCE( ss_m::log->insert(*logrec, rlsn) );       \
    } while(0)


/*****************************************************
// Dead code, comment out just in case we need to re-visit it in the future

 // No checking on available log space (Recovery milestone 1)
 // Not waiting on old transactions to finish and no buffle pool flush before checkpoint

 retry:


    // FRJ: We must somehow guarantee that the log always has space to
    // accept checkpoints.  We impose two constraints to this end:

    // 1. We cap the total space checkpoints are allowed to consume in
    //    any one log partition. This is a good idea anyway because
    //    checkpoint size is linear in the number of dirty buffer pool
    //    pages -- ~2MB per GB of dirty data -- and yet the utility of
    //    checkpoints drops off quickly as the dirty page count
    //    increases -- log analysis and recovery must start at the lsn
    //    of the oldest dirty page regardless of how recent the
    //    checkpoint was.

    // 2. No checkpoint may depend on more than /max_openlog-1/ log
    //    partitions. In other words, every checkpoint completion must
    //    leave at least one log partition available.

    // We use these two constraints, together with log reservations,
    // to guarantee the ability to reclaim log space if the log
    // becomes full. The log maintains, on our behalf, a reservation
    // big enough for two maximally-sized checkpoints (ie the dirty
    // page table lists every page in the buffer pool). Every time we
    // reclaim a log segment this reservation is topped up atomically.


    // if current partition is max_openlog then the oldest lsn we can
    // tolerate is 2.0. We must flush all pages dirtied before that
    // time and must wait until all transactions with an earlier
    // start_lsn have ended (at worst they will abort if the log fills
    // up before they can commit).

    //  TODO: use ss_m::log_warn_callback to notify the VAS in
    // case old transactions are't currently active for some reason.

    // Also, remember the current checkpoint count so we can see
    // whether we get raced...

    // #warning "TODO use log_warn_callback in case old transactions aren't logging right now"
    long curr_pnum = ss_m::log->curr_lsn().file();
    long too_old_pnum = std::max(0l, curr_pnum - max_openlog+1);
    if(!ss_m::log->verify_chkpt_reservation()) {
    // Yikes! The log can't guarantee that we'll be able to
    // complete any checkpoint after this one, so we must reclaim
    // space even if the ss_m::log doesn't seem to be full.

        too_old_pnum = ss_m::log->global_min_lsn().file();
        if(too_old_pnum == curr_pnum) {
            // how/why did they reserve so much log space???
            W_FATAL(eOUTOFLOGSPACE);
        }
    }

    // We cannot proceed if any transaction has a too-low start_lsn;
    // wait for them to complete before continuing.

    // WARNING: we have to wake any old transactions which are waiting
    // on locks, or we risk deadlocks where the lock holder waits on a
    // full log while the old transaction waits on the lock.
    lsn_t oldest_valid_lsn = log_m::first_lsn(too_old_pnum+1);
    old_xct_tracker tracker;
    {
    xct_i it(true); // do acquire the xlist_mutex...
    while(xct_t* xd=it.next()) {
        lsn_t const &flsn = xd->first_lsn();
        if(flsn.valid() && flsn < oldest_valid_lsn) {
            // poison the transaction and add it to the list...
            xd->force_nonblocking();
            tracker.track(xd);
        }
    }
    }

    // release the chkpt_serial to do expensive stuff

    // We'll record the current checkpoint count so we can detect
    // whether we get raced during the gap.
    long chkpt_stamp = _chkpt_count;
    chkpt_serial_m::write_release();


    // clear out all too-old pages
    W_COERCE(ss_m::bf->force_until_lsn(oldest_valid_lsn.data()));

    // hopefully the page cleaning took long enough that the old
    // transactions all ended...
    tracker.wait_for_all();

    // raced?
    chkpt_serial_m::write_acquire();
    if(_chkpt_count != chkpt_stamp)
    goto retry;
*****************************************************/

try
{

    // Finally, we're ready to start the actual checkpoint!
    uint16_t total_page_count = 0;
    uint16_t total_txn_count = 0;
    int      total_dev_cnt = 0;
    int      backup_cnt = 0;

    // Write a Checkpoint Begin Log and record its lsn in master
    lsn_t master = lsn_t::null;

    // Put the last mounted LSN in the 'begin checkpoint ' log record, it is used in
    // Recovery Log Analysis for device mounting purpose.
    // Get the master lsn (begin checkpoint LSN) for the rest of the checkpoint
    // operation.
    // The curr_lsn is the lsn of the next-to-be-inserted log record LSN,
    // master LSN must be equal or later than the current lsn (if busy system).
    // The master LSN will be the starting point for Recovery Log Analysis log scan
    const lsn_t curr_lsn = ss_m::log->curr_lsn();
    // CS TODO: is lastMountLSN really necessary? Why?
    // LOG_INSERT(chkpt_begin_log(ss_m::log->GetLastMountLSN()), &master);
    LOG_INSERT(chkpt_begin_log(lsn_t::null), &master);
    w_assert1(curr_lsn.data() <= master.data());

    // The order of logging is important:
    //   1. Device mount
    //   2. Buffer pool dirty pages
    //   3. Active transactions
    // Because during Recovery, we need to mount all device before loading
    // pages into the buffer pool
    // Note that mounting a device would preload the root page, which might be
    // in-doubt page itself, we need to take care of this special case in Recovery

    // Checkpoint backups
    {
        std::vector<string> paths;

        if (smlevel_0::vol) {
            smlevel_0::vol->list_backups(paths);
            if (paths.size() > 0)
            {
                // Write a Checkpoint Device Table Log
                LOG_INSERT(chkpt_backup_tab_log(paths), 0);
            }
            backup_cnt = paths.size();
        }
    }

    // State of restore bitmap (if restore is in progress)
    {
        vol_t* vol = smlevel_0::vol;
        if (vol && vol->is_failed()) {
            LOG_INSERT(chkpt_restore_tab_log(), 0);
        }
    }

    /*
     *  Checkpoint the buffer pool dirty page table, and record
     *  minimum of the recovery lsn of all dirty pages.
     *
     *  We could do this (very slow) operation before grabbing the
     *  checkpoint mutex because all pages can do is get younger by
     *  being flushed; no page can become older than the min_rec_lsn
     *  we record here ... however, we have to serialize checkpoints
     *  because, although they are fuzzy, they cannot intermingle.
     *  One must complete before another starts. Recovery relies
     *  on it.  Either everyone uses wakeup_and_take or they (dismount,
     *  mount, etc) wait on this.
     *
     *  The srv_log does wakeup_and_take() whenever a new partition is
     *  opened, and it might be that a checkpoint is spanning a
     *  partition.
     */

    // Initialize the min_rec_lsn, this is to store the minimum lsn for this checkpoint
    // initialize it to 'master' which is LSN for the 'begin checkpoint'
    lsn_t min_rec_lsn = master;
    {
        // Count of blocks/pages in buffer pool
        bf_idx     bfsz = ss_m::bf->get_block_cnt();

        // One log record per block, max is set to make chkpt_bf_tab_t fit in logrec_t::data_sz
        // chunk = how many dirty page information can fit into one chkpt_bf_tab_log log record
        const     uint32_t chunk = chkpt_bf_tab_t::max;

        // Each log record contains the following array, while each array element has information
        // for one dirty page
        w_auto_delete_array_t<lpid_t> pid(new lpid_t[chunk]);     // page lpid
        w_auto_delete_array_t<snum_t> stores(new snum_t[chunk]);     // page lpid
        w_auto_delete_array_t<lsn_t> rec_lsn(new lsn_t[chunk]);   // initial dirty lsn
        w_auto_delete_array_t<lsn_t> page_lsn(new lsn_t[chunk]);  // last write lsn
        w_assert1(pid && rec_lsn && page_lsn);

        // Start from block 1 because 0 is never used (see Bf_tree.h)
        // Based on the free list implementation in buffer pool, index is same
        // as _buffer and _control_blocks, zero means no link.
        // Index 0 is always the head of the list (points to the first free block
        // or 0 if no free block), therefore index 0 is never used.

        for (bf_idx i = 1; i < bfsz; )
        {
            // Loop over all buffer pages, put as many dirty page information as possible into
            // one log record at a time
            uint32_t count = chunk;

            // Determine the oldest log record (lsn value) that the REDO phase
            // needs to scan during recovery from a crash
            // Have the minimum rec_lsn of the bunch
            // returned iff it's less than the value passed in
            // min_rec_lsn will be recorded in 'end checkpoint' log, it is used to determine the
            // beginning of the REDO phase

            ss_m::bf->get_rec_lsn(i, count, pid.get(), stores.get(),
                            rec_lsn.get(), page_lsn.get(), min_rec_lsn,
                            master, ss_m::log->curr_lsn(),
                            lsn_t::null);
                            // ss_m::log->GetLastMountLSN()); // CS TODO
            if (count)
            {
                total_page_count += count;

                // write all the information into a 'chkpt_bf_tab_log' (chkpt_bf_tab_t) log record
                LOG_INSERT(chkpt_bf_tab_log(count, pid, stores, rec_lsn, page_lsn), 0);
            }
        }
        //fprintf(stderr, "Checkpoint found %d dirty pages\n", total_page_count);
    }

    // Checkpoint is not a blocking operation, do not locking the transaction table
    // Note that transaction table list could change underneath the checkpoint
    //
    // W_COERCE(xct_t::acquire_xlist_mutex());

    // Because it is a descending list, the newest transaction goes to the
    // beginning of the list, new transactions arrived after we started
    // scanning will not be recorded in the checkpoint


    // Checkpoint the transaction table, and record
    // minimum of first_lsn of all transactions.
    // initialize it to 'master' which is LSN for the 'begin checkpoint'
    // min_xct_lsn is used for:
    //    1. Log in 'end checkpoint' log record, in UNDO phase if it is using backward
    //            log scan implementation, this is the stopping LSN for UNDO (not
    //            used in current implementation)
    //    2. Truncate log at the end of checkpoint, currently disabled.
    lsn_t min_xct_lsn = master;
    {
        // For each transaction, record transaction ID,
        // lastLSN (the LSN of the most recent log record for the transaction)
        // 'abort' flags (transaction state), and undo nxt (for rollback)
        // 'undo nxt' is needed because the UNDO is using a special reverse
        // chronological order to roll back loser transactions

        // Max is set to make chkpt_xct_tab_log fit in logrec_t::data_sz
        // chunk = how many txn information can fit into one chkpt_xct_tab_log log record
        const int    chunk = chkpt_xct_tab_t::max;

        // Each t_chkpt_xct_tab log record contains the following array
        // while each array element has information for one active transaction
        w_auto_delete_array_t<tid_t> tid(new tid_t[chunk]);               // transaction ID
        w_auto_delete_array_t<ss_m::xct_state_t> state(new ss_m::xct_state_t[chunk]); // state
        w_auto_delete_array_t<lsn_t> last_lsn(new lsn_t[chunk]);          // most recent log record
        w_auto_delete_array_t<lsn_t> undo_nxt(new lsn_t[chunk]);          // undo next
        w_auto_delete_array_t<lsn_t> first_lsn(new lsn_t[chunk]);         // first lsn of the txn

        // How many locks can fit into one chkpt_xct_lock_log log record
        const int    lock_chunk = chkpt_xct_lock_t::max;

        // Each t_chkpt_xct_lock log record contains the following array
        // while each array element has information for one granted lock
        // in the associated in-flight transaction
        w_auto_delete_array_t<okvl_mode> lock_mode(new okvl_mode[lock_chunk]);  // lock mode
        w_auto_delete_array_t<uint32_t> lock_hash(new uint32_t[lock_chunk]);    // lock hash

        xct_i x(false); // false -> do not acquire the mutex when accessing the transaction table

        // Not 'const' becasue we are acquring a traditional latch on the object
        xct_t* xd = 0;

        do
        {
            int per_chunk_txn_count = 0;
            /*
             * CS TODO -- there is a bug here, which seems to occur when
             * system is being shut down. The xct iterator keeps returning
             * the same object, because the linked list somehow has a circle,
             * i.e., _next->next == _next
             */
            while (per_chunk_txn_count < chunk && (xd = x.next()))
            {
                // Loop over all transactions and record only
                // xcts that dirtied something.
                // Skip those that have ended but not yet
                // been destroyed.
                // Both active and aborted transaction will be
                // recorded in checkpoiont

                // Acquire a traditional read latch to prevent reading transitional data
                // A write latch is issued from the xct_t::change_state function which
                // is used in txn commit and abort operation, it is also issued from
                // ~xct_t when destroying the transaction.
                // We don't want to read transaction data when state is changing or
                // transaction is being destoryed.
                // It is possible the transaction got committed or aborted after we
                // gather log data for the transaction, so the commit or abort log record
                // will be in the recovery log after the checkpoint, it is okay
                //
                // There is a small window (race condition) that transaction is in the process
                // of being destroyed while checkpoint is trying to get information from the
                // same transaction concurrently.  If we catch this window then an exception
                // would be throw, we abort the current checkpoint but not bringing down
                // the system.

                w_rc_t latch_rc = xd->latch().latch_acquire(LATCH_SH, WAIT_FOREVER);
                if (latch_rc.is_error())
                {
                    // Unable to the read acquire latch, cannot continue, log an internal error
                    DBGOUT1 (<< "Error when acquiring LATCH_SH for checkpoint transaction object. xd->tid = "
                             << xd->tid() << ", rc = " << latch_rc);

                    // To be a good citizen, release the 'write' mutex before raise error
                    chkpt_serial_m::write_release();

                    // Abort the current checkpoint silently
                    ss_m::errlog->clog << info_prio
                        << "Failed to latch a txn from checkpoint, abort current checkpoint." << flushl;

                    return;
                }

                if ((xd->state() == xct_t::xct_ended) ||
                    (xd->state() == xct_t::xct_freeing_space) ||
                    (xd->state() == xct_t::xct_stale))
                {
                    // In xct_t::_commit(), the txn state was changed to xct_freeing_space
                    // first, then log the free space log recorsd, followed by the txn complete
                    // log record, followed by lock release and log sync, and then change
                    // the xct state to xct_ended, the gap is long.  Therefore,
                    // if transaction state is in either xct_free_space or xct_ended, the
                    // log record has written and it is safe not to record it

                    // Potential race condition: if error occurred during lock release or log sync,
                    // it would bring down the system and the log not flushed.
                    // If the checkpoint was on at the same time
                    // 1. Checkpoint completed and the target txn was not recorded - the
                    //     recovery starts from this last compeleted checkpoint which does not
                    //     have this transaction, so it won't get rolled back.  Whether we have
                    //     an issue or not is depending on the state of the buffer pool on disk or not,
                    //     but the window for this scenario to occur is so small, is it even possible?
                    // 2. Checkpoint aborted - the transaction would be rollback during recover
                    //     because the 'end txn' log record was not flushed.

                    if (xd->latch().held_by_me())
                        xd->latch().latch_release();
                    continue;
                }

                // If checkpoint comes in during recovery, we might see loser transactions
                // which are yet to be undone.
                // If system starts with checkpoint containing loser txns:
                // 1. For a loser txn in checkpoint, UNDO operation would generate an
                //    'end transaction log record' after UNDO, which cancels out the loser
                //    txn in checkpoint log.
                // 2. For a loser txn in checkpoint, if system crashs before UNDO this
                //    loser txn, then no matching 'end transaction log record' so restart will
                //    take care of this loser txn again.
                // No need to record the _loser_txn flag, because all in-flight txns in recovery
                // will be marked as loser.
                // For on_demand restart, if a loser transaction was in the middle of rolling
                // back (_loser_xct flag is set to loser_undoing) when the checkpoint is being
                // taken, still no need to record the _loser_txn flag, because an 'end transaction'
                // log record would be generated at the end of rollback.

                // Transaction table is implemented in a descend list sorted by tid
                // therefore the newest transaction goes to the beginning of the list
                // When scanning, the newest transaction comes first

                if (xd->first_lsn().valid())
                {
                    // Note that we will pick the rest of txns in transaction table as long as it
                    // has a valid 'first_lsn', including both normal and loser transactions
                    // from restart process

                    // Not all transactions have tid, i.e. system transaction
                    // does not have tid, device mount/dismount does not
                    // have tid
                    tid[per_chunk_txn_count] = xd->tid();

                    // Record the transactions in following states:
                    //     xct_active (both normal and loser), xct_chaining, xct_committing,
                    //     xct_aborting
                    // A transaction state can be xct_t::xct_aborting if a
                    // normal transaction in the middle of aborting
                    // A checkpoint will be taken at the end of Log Analysis phase
                    // therefore record all transaction state as is.
                    //
                    state[per_chunk_txn_count] = xd->state();

                    w_assert1(lsn_t::null!= xd->last_lsn());
                    last_lsn[per_chunk_txn_count] = xd->last_lsn();  // most recent LSN
                    first_lsn[per_chunk_txn_count] = xd->first_lsn();  // first LSN of the txn

                    // 'set_undo_nxt' is initiallized to NULL,
                    // 1. Set in Log_Analysis phase in Recovery for UNDO phase,
                    // 2. Set in xct_t::_flush_logbuf, set to last_lan if undoable,
                    //     set to last_lsn if not a compensation transaction,
                    //     if a compensation record, set to transaction's
                    //     last log record's undo_next
                    // 'undo_nxt' is used in UNDO phase in Recovery,
                    // transaction abort/rollback, also somehow in log truncation

                    // System transaction is not undoable
                    if (xd->is_sys_xct())
                        undo_nxt[per_chunk_txn_count] = lsn_t::null;
                    else
                        undo_nxt[per_chunk_txn_count] = xd->undo_nxt();

                    if (false == xd->is_sys_xct())
                    {
                        // Now we have an active transaction and it is not a system transaction
                        // Process the lock information

                        RawXct* xct = xd->raw_lock_xct();

                        // Record all dirty key locks from this transaction
                        // note that IX is not a dirty lock, only X meets the criteria
                        // also only X lock on key, not on gap
                        // We are holding latch on the transaction so the txn state
                        // cannot change (e.g. commit, abort) while we are gathering
                        // lock information

                        int per_chunk_lock_count = 0;
                        RawLock* lock = xct->private_first;
                        while (NULL != lock)
                        {
                            if (true == lock->mode.contains_dirty_key_lock())
                            {
                                // Only record the locks the transaction has acquired,
                                // not the ones it is waiting on which might time out or
                                // conflict (no log record would be generated in such cases)
                                if (RawLock::ACTIVE == lock->state)
                                {
                                    // Validate the owner of the lock
                                    w_assert1(xct == lock->owner_xct);

                                    // Add the hash value which was constructed using
                                    // the key value and store (index) value, it is the only
                                    // value needed to look into lock manager, 'stid_t' is
                                    // already included in hash value so it is not required here
                                    // and we do not have this information at this point anyway
                                    lock_hash[per_chunk_lock_count] = lock->hash;

                                    // Add granted lock mode of this lock
                                    lock_mode[per_chunk_lock_count] = xct->private_hash_map.get_granted_mode(lock->hash);

                                    ++per_chunk_lock_count;

                                    if (true == record_lock)
                                    {
                                        // Caller asked for lock information into a provided
                                        // heap structure, this is generated only for debug build
#if W_DEBUG_LEVEL>0
                                        // Add the lock information to heap for tracking purpose
                                        comp_lock_info_t* lock_heap_elem =
                                                 new comp_lock_info_t(xct->private_hash_map.get_granted_mode(lock->hash));
                                        if (! lock_heap_elem)
                                        {
                                            W_FATAL(eOUTOFMEMORY);
                                        }
                                         // Fill in the rest of the lock information
                                         lock_heap_elem->tid = xd->tid();
                                         lock_heap_elem->lock_hash = lock->hash;

                                         lock_heap.AddElementDontHeapify(lock_heap_elem);
#endif
                                    }
                                }
                            }

                            if (per_chunk_lock_count == lock_chunk)
                            {
                                // Generate a chkpt_xct_lock_log log record which fits in as many
                                // locks on the current transaction as possible

                                // Generate a log record for the acquired locks on this in-flight transaction
                                // it is safe to do so because we are holding latch on this transaction so the
                                // state cannot change, the t_chkpt_xct_lock log record will be written before the
                                // t_chkpt_xct_tab log record (which consists multiple in-flight transactions)
                                // During Log Analysis backward log scan, it will process t_chkpt_xct_tab
                                // log record before it gets the t_chkpt_xct_lock log record which is the order we want

                                LOG_INSERT(chkpt_xct_lock_log(xd->tid(), per_chunk_lock_count,
                                           lock_mode, lock_hash), 0);

                                per_chunk_lock_count = 0;
                            }
                            else if (per_chunk_lock_count > lock_chunk)
                            {
                                // Error, cannot happen
                                W_FATAL(eOUTOFMEMORY);
                            }

                            // Advance to the next lock
                            lock = lock->xct_next;
                        }
                        w_assert1(NULL == lock);

                        if (0 != per_chunk_lock_count)
                        {
                            // Pick up the last set
                            LOG_INSERT(chkpt_xct_lock_log(xd->tid(), per_chunk_lock_count,
                                       lock_mode, lock_hash), 0);
                        }
                    }

                    // Keep track of the overall minimum xct lsn
                    w_assert1(lsn_t::null!= xd->first_lsn());
                    if (min_xct_lsn > xd->first_lsn())
                        min_xct_lsn = xd->first_lsn();

                    ++per_chunk_txn_count;

                    ++total_txn_count;
                }

                // Release the traditional read latch before go get the next txn
                if (xd->latch().held_by_me())
                    xd->latch().latch_release();

                // Log record is full, need to write it out
                if (per_chunk_txn_count >= chunk)
                    break;
            }

            // It is possible we don't have any transaction when doing the checkpoint,
            // in such case, we will write out 1 log record, this is because we want to
            // record the youndgest xct (with the largest tid)
            {
                // tid is increasing, youngest tid has the largest value
                tid_t  youngest = xct_t::youngest_tid();

                // Filled up one log record, write a Transaction Table Log out
                // before processing more transactions
                LOG_INSERT(chkpt_xct_tab_log(youngest, per_chunk_txn_count,
                                   tid, state, last_lsn, undo_nxt, first_lsn), 0);
            }
        } while (xd);
    }

    // Non-blocking checkpoint, we never acquired a mutex on the list

    /*
     *  Make sure that min_rec_lsn and min_xct_lsn are valid
     *  master: lsn from the 'begin checkpoint' log record
     *  min_xct_lsn: minimum of first_lsn of all recorded transactions, both active and aborting
     *  min_rec_lsn: minimum lsn of all buffer pool dirty or in_doubt pages
     *
     *  If min_*_lsn > master, it could be one of 2 things:
     *    1. Nothing happened, therefore the min_*_lsn are still set to master
     *    2. All activities occurred after 'begin checkpoint'
     *  Recovery would have to start from the master LSN in any case.
     */
    if (min_rec_lsn > master)
        min_rec_lsn = master;
    if (min_xct_lsn > master)
        min_xct_lsn = master;

    // Finish up the checkpoint
    // 1. If log->getLastMountLSN() > master, this is not supposed to happen
    // 2. If we get here (passed the initial checking) and the systme is
    //     in a normal shutting down and this is an asynch/user checkpoint, the
    //     checkpoint must started before the normal system shutdown, therefore
    //     the shutdown process is blocked by this checkpoint, okay to allow the
    //     current checkpoint to finish
    // 3. If system is in a simulated crash shutdown while an asynch/user checkpoint
    //     gets here, the system would be in the process of shutting down and it won't
    //     wait for the checkpoint to finish, don't finish the current checkpoint.

#if 0 // CS TODO -- is this necessary?
    if (ss_m::log->GetLastMountLSN() <= master)
#endif
    {
        if (ss_m::shutting_down && !ss_m::shutdown_clean) // Dirty shutdown (simulated crash)
        {
            DBGOUT1(<<"chkpt_m::take ABORTED due to dirty shutdown, dirty page count = "
                    << total_page_count
                    << ", total txn count = " << total_txn_count
                    << ", total backup count = " << backup_cnt
                    << ", total vol count = " << total_dev_cnt);
        }
        else
        {
            // Write the Checkpoint End Log with:
            //  1. master: LSN from begin checkpoint, thi sis where the checkpoint started
            //  2. min_rec_lsn: minimum lsn of all buffer pool dirty or in_doubt pages
            //  3. min_xct_lsn: minimum lsn of all in-flight (including aborting) transactions

            if (0 == total_page_count)
            {
                // No dirty page, the begin checkpoint lsn and redo_lsn (min_rec_lsn)
                // must be the same
                w_assert1(master == min_rec_lsn);
                LOG_INSERT(chkpt_end_log (master, master, min_xct_lsn), 0);
            }
            else
            {
                // Has dirty page
                LOG_INSERT(chkpt_end_log (master, min_rec_lsn, min_xct_lsn), 0);
            }

// TODO(Restart)... performance
            DBGOUT1(<<"chkpt_m::take completed - total dirty page count = "
                    << total_page_count
                    << ", total txn count = " << total_txn_count
                    << ", total backup count = " << backup_cnt
                    << ", total vol count = " << total_dev_cnt);

            // Sync the log
            // In checkpoint operation, flush the recovery log to harden the log records
            // We either flush the log or flush the buffer pool, but not both

            // We do not flush the buffer pool (bf->force_all()).
            // One scenario: If the dirty page was a new page but never flushed to disk,
            //                 the page do not exist on disk until the buffer pool page gets
            //                 flushed.  When taking a checkpoint, it captures the fact that the
            //                 page is dirty in buffer pool but it does not know the page does
            //                 not exist on disk.  If the transaction ended (log record generated)
            //                 and then system crashed, the page might not exist on disk..
            //                 During system startup, the Recovery starts from the last completed
            //                 checkpoint, it correctly determined the transaction was finished
            //                 and there was an associated in_doubt page, so it tries to REDO the
            //                 page by bring the page in from disk and REDO.  Note the page
            //                 does not exist on disk, so REDO cannot fetch the page from disk.
            // Solution: During the Log Analysis phase, it track the page history to find the
            //                 LSN which made the page dirty initially, in case of virgin page it should
            //                 be the page format log record LSN.  The REDO log scan starts from
            //                 the earliest LSN which might be earlier than the 'begin checkpoint' LSN.
            //                 In this case a page format log record would be REDO for a virgin page
            //                 and we should never have the situation fetching a non-existing page
            //                 from disk.  A virgin page should always start from a page_imp_format
            //                 or t_btree_norec_alloc log record.
            // Note that we cannot force a buffer pool flush after a page format operation, because
            // the transaction was not committed at that point.

            W_COERCE( ss_m::log->flush_all() );

            // Make the new master lsn durable
            // This is the step to record the last completed checkpoint in the known location
            // in each log file (partition), so the Recovery process can find the
            // latest completed checkpoint.
            // It reocrds the begin checkpoint LSN (master) and the
            // minimum LSN (std::min(min_rec_lsn, min_xct_lsn))
            // Error in this step will bring down the server
            w_assert3(min_rec_lsn.hi() > 0);
            w_assert3(min_xct_lsn.hi() > 0);
            ss_m::log->set_master(master, min_rec_lsn, min_xct_lsn);

            // Do not scavenge log space because Single Page Recovery might need
            // old log records.  Once log archieve has been implemented, the log truncation
            // will be driven by the log archieve status, not from checkpoint
            // With the current implementation (no log archieve yet), log->scavenge()
            // is never called
            //
            // W_COERCE( ss_m::log->scavenge(min_rec_lsn, min_xct_lsn) );

            // Finished a completed checkpoint
            DBGOUT1(<< "END chkpt_m::take, begin LSN = " << master
                    << ", min_rec_lsn = " << min_rec_lsn);

        }
    }
#if 0 // CS
    else
    {
        DBGOUT1(<<"chkpt_m::take - GetLastMountLSN > master, invalid situation for checkpoint, "
                << "GetLastMountLSN = " << ss_m::log->getLastMountLSN() << ", master = " << master);
        DBGOUT1(<<"END chkpt_m::take, abort checkpoint" << master);

        ss_m::errlog->clog << error_prio
            << "chkpt_m::take - GetLastMountLSN > master, checkpoint aborted." << flushl;
    }
#endif

    // Release the 'write' mutex so the next checkpoint request can come in
    chkpt_serial_m::write_release();
}
catch (...)
{
    // Catch all exceptions

    // Be a good citizen and release the 'write' mutex first
    chkpt_serial_m::write_acquire();

    // Log a message and abort the current checkpoint, we do not want to
    // bring down the system due to a checkpoint failure
    DBGOUT1(<<"chkpt_m::take, encountered 'catch' block, abort current checkpoint");

    ss_m::errlog->clog << error_prio
        << "Exception caught during a checkpoint process, checkpoint aborted." << flushl;
}

    return;
}


/*********************************************************************
 *
 *  chkpt_thread_t::chkpt_thread_t()
 *
 *  Construct a Checkpoint Thread. Priority level is t_time_critical
 *  so that checkpoints are done as fast as it could be done. Most of
 *  the time, however, the checkpoint thread is blocked waiting for
 *  a go-ahead signal to take a checkpoint.
 *
 *********************************************************************/
chkpt_thread_t::chkpt_thread_t()
    : smthread_t(t_time_critical, "chkpt", WAIT_NOT_USED),
    _retire(false), _kicked(false), chkpt_count(0)
{
    rename("chkpt_thread");            // for debugging
    DO_PTHREAD(pthread_mutex_init(&_retire_awaken_lock, NULL));
    DO_PTHREAD(pthread_cond_init(&_retire_awaken_cond, NULL));
}


/*********************************************************************
 *
 *  chkpt_thread_t::~chkpt_thread_t()
 *
 *  Destroy a checkpoint thread.
 *
 *********************************************************************/
chkpt_thread_t::~chkpt_thread_t()
{
    /* empty */
}



/*********************************************************************
 *
 *  chkpt_thread_t::run()
 *
 *  Body of checkpoint thread. Repeatedly:
 *    1. wait for signal to activate
 *    2. if retire intention registered, then quit
 *    3. write all buffer pages dirtied before the n-1 checkpoint
 *    4. if toggle off then take a checkpoint
 *    5. flip the toggle
 *    6. goto 1
 *
 *  Essentially, the thread will take one checkpoint for every two
 *  wakeups.
 *
 *********************************************************************/
void
chkpt_thread_t::run()
{
    CmpXctLockTids	 lock_cmp;
    XctLockHeap      dummy_heap(lock_cmp);

    while(! _retire)
    {
        {
            // Enter mutex first
            CRITICAL_SECTION(cs, _retire_awaken_lock);
            while(!_kicked  && !_retire)
            {
                // Unlock the mutex and wait on _retire_awaken_cond (checkpoint request)
                DO_PTHREAD(pthread_cond_wait(&_retire_awaken_cond, &_retire_awaken_lock));

                // On return, we own the mutex again
            }
            // On a busy system it is possible (but rare) to have more than one pending
            // checkpoint requests, we will only execute checkpoint once in such case
            if (true == _kicked)
            {
                w_assert1(0 < chkpt_count);
                DBG(<<"Found pending checkpoint request count: " << chkpt_count);
                chkpt_count = 0;
            }

            // Reset the flag before we continue to execute the checkpoint
            _kicked = false;
        }

        w_assert1(ss_m::chkpt);

        // If a retire request arrived, exit immediatelly without checkpoint
        if(_retire)
            break;

        // No need to acquire checkpoint mutex before calling the checkpoint operation
        // Asynch checkpoint should never record lock information
        ss_m::chkpt->take(chkpt_m::t_chkpt_async, dummy_heap);
        w_assert1(0 == dummy_heap.NumElements());
    }
}


/*********************************************************************
 *
 *  chkpt_thread_t::retire()
 *
 *  Register an intention to retire and activate the thread.
 *  The thread will exit when it wakes up and checks the retire
 *  flag.
 *  If the thread is in the middle of executing a checkpoint, it won't check the
 *  retire flag until after the checkpoint finished execution.
 *
 *********************************************************************/
void
chkpt_thread_t::retire()
{
     CRITICAL_SECTION(cs, _retire_awaken_lock);
     _retire = true;
     DO_PTHREAD(pthread_cond_signal(&_retire_awaken_cond));
}


/*********************************************************************
 *
 *  chkpt_thread_t::awaken()
 *
 *  Signal an asynch checkpoint request arrived.
 *
 *********************************************************************/
void
chkpt_thread_t::awaken()
{
    CRITICAL_SECTION(cs, _retire_awaken_lock);
    _kicked = true;
    ++chkpt_count;
    DO_PTHREAD(pthread_cond_signal(&_retire_awaken_cond));
}
