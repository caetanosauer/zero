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
#include "btree_logrec.h"       // Lock re-acquisition
#include "bf_tree.h"
#include "xct_dependent.h"
#include <new>
#include "sm.h"
#include "lock_raw.h"      // Lock information gathering
#include "w_okvl_inl.h"    // Lock information gathering
struct RawLock;            // Lock information gathering
#include "restart.h"
#include "vol.h"
#include <algorithm>

#include "stopwatch.h"

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
chkpt_m::chkpt_m(bool _decoupled)
    : decoupled(_decoupled), _chkpt_thread(0), _chkpt_count(0)
{
    const size_t BLOCK_SIZE = 1024 * 1024;
    _chkpt_last = ss_m::log->master_lsn();
    if(_chkpt_last == lsn_t::null) {
        _chkpt_last = lsn_t(1,0);
    }
    cons = new LogArchiver::LogConsumer(_chkpt_last, BLOCK_SIZE, false);
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

    cons->shutdown();

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

        if(decoupled) {
            dcpld_take(t_chkpt_sync);
        }
        else {
            take(t_chkpt_sync, dummy_heap);
            w_assert1(0 == dummy_heap.NumElements());
        }
        
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

// Scans log forward from master_lsn to begin_lsn
void chkpt_m::forward_scan_log(const lsn_t master_lsn,
                               const lsn_t begin_lsn,
                               chkpt_t& new_chkpt,
                               const bool restart_with_lock)
{
    w_assert0(begin_lsn >= master_lsn);

    new_chkpt.begin_lsn = begin_lsn;
    new_chkpt.min_xct_lsn = begin_lsn;
    new_chkpt.min_rec_lsn = begin_lsn;
    new_chkpt.next_vid = 1;

    if (master_lsn == lsn_t::null) {
        // The only possibility that we have a NULL as master lsn is due to a brand new
        // start (empty) of the engine, in such case, nothing to scan.

        // LL: we have to guarantee that this is the first chkpt ever being taken.
        // In this case, the chkpt begin lsn must be the very first one.
        // w_assert0(begin_lsn == lsn_t(1,0));

        DBGOUT3( << "NULL master_lsn, nothing to scan");
        return;
    }

    // Special map to mange undecided in-flight transactions
    tid_CLR_map          mapCLR;

    bool acquire_lock = true;

    DBGOUT1(<<"forward_scan_log("<<master_lsn<<", "<<begin_lsn<<")");

    cons->open(begin_lsn, false);

    logrec_t*     r;

    int num_chkpt_end_handled = 0;

    while(cons->next(r) && (r->type() == logrec_t::t_tick_sec || r->type() == logrec_t::t_tick_msec)){}

     // Assert first record is Checkpoint Begin Log
    if(r->type() != logrec_t::t_chkpt_begin) {
            DBGOUT1( << setiosflags(ios::right) << r->lsn()
                     << resetiosflags(ios::right) << " R: " << *r);
            W_FATAL_MSG(fcINTERNAL, << "First log record in Log Analysis is not a begin checkpoint log: " << r->type());
    }

    while (cons->next(r) && r->lsn() < begin_lsn)
    {
        acquire_lock = true; // New log record, reset the flag

        DBGOUT3( << setiosflags(ios::right) << r->lsn()
                  << resetiosflags(ios::right) << " A: " << *r);

        if (r->is_single_sys_xct() && !r->null_pid())
        {
            if(_analysis_system_log(*r, new_chkpt) == true)
            {
                continue; // Go to next log record
            }
            else {
                // Failure occured, do not continue
                W_FATAL_MSG(fcINTERNAL, << "Failed to process a system transaction log record during Log Analysis, lsn = " << r->lsn());
            }
        }

        // We already ruled out all SSX logs. So we don't have to worry about
        // multi-page logs in the code below, because multi-page log only exist
        // in system transactions
        w_assert1(!r->is_multi_page());

        // If log is transaction related, insert the transaction
        // into transaction table if it is not already there.
        if (!r->is_single_sys_xct()
            && r->tid() != tid_t::null              // Has a transaction ID
            && !new_chkpt.xct_tab.count(r->tid())   // does not exist in transaction table currently
            && r->type()!=logrec_t::t_comment       // Not a 'comment' log record, comments can be after xct has ended
            && r->type()!=logrec_t::t_skip          // Not a 'skip' log record
            && r->type()!=logrec_t::t_max_logrec)   // Not the special 'max' log record which marks the end
        {
            new_chkpt.xct_tab[r->tid()].state = xct_t::xct_active;
            new_chkpt.xct_tab[r->tid()].last_lsn = r->lsn();
            new_chkpt.xct_tab[r->tid()].undo_nxt = r->xid_prev();
            new_chkpt.xct_tab[r->tid()].first_lsn = r->lsn();

            // Youngest tid is the largest tid, it is used to generate
            // next unique transaction id
            if (r->tid() > new_chkpt.youngest) {
                new_chkpt.youngest = r->tid();
            }
        }
        else if (!r->is_single_sys_xct() && r->tid() != tid_t::null)            // Has a transaction ID
        {
            // Transaction exists in transaction table already
            w_assert0(new_chkpt.xct_tab.count(r->tid()) == 1);
            if(new_chkpt.xct_tab[r->tid()].last_lsn < r->lsn()) {
                new_chkpt.xct_tab[r->tid()].last_lsn = r->lsn();
            }
        }
        else
        {
            // Log record is not related to transaction table
            // Go ahead and process the log record
        }

        switch (r->type())
        {
            case logrec_t::t_chkpt_begin:
                break;

            case logrec_t::t_chkpt_dev_tab:
                if (num_chkpt_end_handled == 0)
                {
                    // Process it only if we have seen a matching 'end checkpoint' log record
                    // meaning we are processing the last completed checkpoint
                    chkpt_dev_tab_t* tab = (chkpt_dev_tab_t*) r->data();
                    std::vector<string> dnames;
                    tab->read_devnames(dnames);
                    w_assert0(tab->count == dnames.size());
                    for (int i = 0; i < tab->count; i++) {
                        if(new_chkpt.dev_tab.count(dnames[i]))  {
                            // volume is already in the list and it is safe to
                            // ignore information about it from the previous chkpt,
                            // since it is guaranteed to be out-dated
                        }
                        else {
                            new_chkpt.dev_tab[dnames[i]].dev_mounted = true;
                            new_chkpt.dev_tab[dnames[i]].dev_lsn = lsn_t::null;
                            // dev_lsn = null, because any further log record
                            // about this device is going to be more recent
                        }
                    }
                    //-1 because next_vid already starts at 1
                    new_chkpt.next_vid += (tab->next_vid - 1);
                }
                break;

            case logrec_t::t_chkpt_backup_tab:
                if (num_chkpt_end_handled == 0)
                {
                    //TODO
                }
                break;

            case logrec_t::t_chkpt_restore_tab:
                if (num_chkpt_end_handled == 0)
                {
                    //TODO
                }
                break;

            case logrec_t::t_chkpt_bf_tab:
                if (num_chkpt_end_handled == 0)
                {
                    _analysis_ckpt_bf_log(*r, new_chkpt);
                }
                break;

            case logrec_t::t_chkpt_xct_lock:
                // Log record for per transaction lock information
                // Due to backward log scan, this log record (might have multiple log
                // records per active transaction) should be retrieved after the corresponding
                // t_chkpt_xct_tab log record, in other words, it was generated prior the
                // corresponding t_chkpt_xct_tab log record
                if ((num_chkpt_end_handled == 0) && (true == restart_with_lock))
                {
                    const chkpt_xct_lock_t* dp = (chkpt_xct_lock_t*) r->data();

                    // Go through all the locks and re-acquire them on the transaction object
                    for (uint i = 0; i < dp->count; i++) {
                        DBGOUT3(<<"_analysis_acquire_ckpt_lock_log - acquire key lock, hash: " << dp->xrec[i].lock_hash
                            << ", key lock mode: " << dp->xrec[i].lock_mode.get_key_mode());
                        lck_tab_entry_t entry;
                        entry.lock_mode = dp->xrec[i].lock_mode;
                        entry.lock_hash = dp->xrec[i].lock_hash;
                        new_chkpt.lck_tab[dp->tid].push_back(entry);
                    }
                }
                else
                {
                    // No matching 'end checkpoint' log record, ignore
                }
                break;

            case logrec_t::t_chkpt_xct_tab:
                if (num_chkpt_end_handled == 0)
                {
                    _analysis_ckpt_xct_log(*r, new_chkpt, mapCLR);
                }
                break;

            case logrec_t::t_chkpt_end:
                if (num_chkpt_end_handled == 0)
                {
                    // We processed the t_chkpt_xct_lock before the t_chkpt_xct_tab.
                    // Now we just make sure that all entries that we saw in
                    // t_chkpt_xct_lock were also present in t_chkpt_xct_tab:
                    for(lck_tab_t::iterator it=new_chkpt.lck_tab.begin();
                         it!=new_chkpt.lck_tab.end(); ++it) {
                        w_assert0(new_chkpt.xct_tab.count(it->first) == 1);
                        //w_assert0(new_chkpt.xct_tab[it->first].state == xct_t::xct_active);
                    }
                }
                num_chkpt_end_handled++;
                break;

            case logrec_t::t_mount_vol:
                {
                    string dev = (const char*)(r->data_ssx());
                    if(new_chkpt.dev_tab.count(dev)) {
                        // volume exists already ...
                        if(new_chkpt.dev_tab[dev].dev_lsn < r->lsn()) {
                            // ... but the log record is more recent
                            new_chkpt.dev_tab[dev].dev_mounted = true;
                            new_chkpt.dev_tab[dev].dev_lsn = r->lsn();
                        }
                    }
                    else {
                        // volume does not exist yet
                        new_chkpt.dev_tab[dev].dev_mounted = true;
                        new_chkpt.dev_tab[dev].dev_lsn = r->lsn();
                    }
                }
                break;

            case logrec_t::t_format_vol:
                new_chkpt.next_vid++;
                break;

            case logrec_t::t_dismount_vol:
                {
                    string dev = (const char*)(r->data_ssx());
                    if(new_chkpt.dev_tab.count(dev)) {
                        // volume exists already ...
                        if(new_chkpt.dev_tab[dev].dev_lsn < r->lsn()) {
                            // ... but the log record is more recent
                            new_chkpt.dev_tab[dev].dev_mounted = false;
                            new_chkpt.dev_tab[dev].dev_lsn = r->lsn();
                        }
                    }
                    else {
                        // volume does not exist yet
                        new_chkpt.dev_tab[dev].dev_mounted = false;
                        new_chkpt.dev_tab[dev].dev_lsn = r->lsn();
                    }
                }
                break;

            case logrec_t::t_add_backup:
                {
                    vid_t vid = *((vid_t*) (r->data_ssx()));
                    const char* dev_name = (const char*)(r->data_ssx() + sizeof(vid_t));

                    new_chkpt.bkp_tab[vid].bkp_path = dev_name;
                }
                break;

            case logrec_t::t_xct_end_group:
                {
                    // Do what we do for t_xct_end for each of the
                    // transactions in the list
                    const xct_list_t* list = (xct_list_t*) r->data();
                    uint listlen = list->count;

                    for(uint i=0; i<listlen; i++) {
                        // If it's not there, it could have been a read-only xct?
                        tid_t tid = list->xrec[i].tid;
                        if(new_chkpt.xct_tab.count(tid) &&
                            new_chkpt.xct_tab[tid].state != xct_t::xct_ended) {
                            // Mark the txn as ended, safe to remove it from transaction table
                            new_chkpt.xct_tab[tid].state = xct_t::xct_ended;
                        }
                    }
                }
                break;

            case logrec_t::t_xct_freeing_space:
            case logrec_t::t_xct_abort:
            case logrec_t::t_xct_end:
                {
                    if (new_chkpt.xct_tab[r->tid()].state != xct_t::xct_ended) {
                        new_chkpt.xct_tab[r->tid()].state = xct_t::xct_ended;
                    }

                    tid_CLR_map::iterator it = mapCLR.find(r->tid().as_int64());
                    if(it != mapCLR.end()) {
                        mapCLR.erase(it);
                    }
                }
                break;

            case logrec_t::t_page_write:
                {
                    lpid_t* pid_begin = (lpid_t*)(r->data());
                    uint32_t* count = (uint32_t*)(r->data() + sizeof(lpid_t));
                    for(uint i=0; i<*count; i++) {
                        lpid_t pid = *pid_begin;
                        pid.page += i;

                        if(new_chkpt.buf_tab.count(pid)) {
                            // Page exists in bf_table, we mark it as !dirty
                            if(new_chkpt.buf_tab[pid].page_lsn < r->lsn()) {
                                new_chkpt.buf_tab[pid].page_lsn = r->lsn();
                                new_chkpt.buf_tab[pid].dirty = false;
                            }
                        }
                        else {
                            // Page does not exist in bf_table, we add it with
                            // dummy values and mark as !dirty
                            new_chkpt.buf_tab[pid].store = 0;
                            new_chkpt.buf_tab[pid].rec_lsn = lsn_t::null;
                            new_chkpt.buf_tab[pid].page_lsn = r->lsn();
                            new_chkpt.buf_tab[pid].dirty = false;
                        }
                    }
                }
                break;

            case logrec_t::t_compensate:
            case logrec_t::t_store_operation:
            case logrec_t::t_page_set_to_be_deleted:
            case logrec_t::t_page_img_format:
            case logrec_t::t_btree_norec_alloc:
            case logrec_t::t_btree_split:
            case logrec_t::t_btree_insert:
            case logrec_t::t_btree_insert_nonghost:
            case logrec_t::t_btree_update:
            case logrec_t::t_btree_overwrite:
            case logrec_t::t_btree_ghost_mark:
            case logrec_t::t_btree_ghost_reclaim:
            case logrec_t::t_btree_ghost_reserve:
            case logrec_t::t_btree_foster_adopt:
            case logrec_t::t_btree_foster_merge:
            case logrec_t::t_btree_foster_rebalance:
            case logrec_t::t_btree_foster_rebalance_norec:
            case logrec_t::t_btree_foster_deadopt:
                // The rest of meanful log records, transaction has been created already
                // we need to take care of both buffer pool and lock acquisition if needed
                {
                    // Take care of common stuff among forward and backward log scan first
                    _analysis_other_log(*r, new_chkpt);

                    if ((acquire_lock == true) && (restart_with_lock == true))
                    {
                        // We need to acquire locks (M3/M4/M5) and this is an
                        // undecided in-flight transaction, process lock for this log record
                        _analysis_process_lock(*r, new_chkpt, mapCLR);
                    }
                    else
                    {
                        // winner transaction, no need to acquire locks
                    }

                    if ((r->tid() != tid_t::null) &&
                        (new_chkpt.xct_tab.count(r->tid())) &&
                        (r->is_page_update()) &&
                        (r->xid_prev() == lsn_t::null) &&
                        (new_chkpt.xct_tab[r->tid()].state == xct_t::xct_ended))
                    {
                        // If this is an update log record, and we have already seen the
                        // transaction end/abort log record for this transaction (backward scan),
                        // and the current log record is the very first log record of this transaction
                        // it is safe to remove this transaction from transaction table now.

                        // Delete the transaction from transaction table because it
                        // reduces the size of llinked-list transaction table and improve
                        // the xct_t::look_up performance of the transaction table
                        new_chkpt.xct_tab.erase(r->tid());
                        new_chkpt.lck_tab.erase(r->tid());
                    }
                }
                break;

            default:
                break;
        }
    }
    // This assertion guarantees that we read all log records we were meant to
    w_assert0(cons->getNextLSN() == begin_lsn);

    // Done with forward log scan, check the compensation list
    _analysis_process_compensation_map(mapCLR, new_chkpt);

    // Remove non-mounted devices
    for(dev_tab_t::iterator it  = new_chkpt.dev_tab.begin();
                            it != new_chkpt.dev_tab.end(); ){
        if(it->second.dev_mounted == false) {
            new_chkpt.dev_tab.erase(it++);
        }
        else {
            ++it;
        }
    }

    // Remove non-dirty pages
    for(buf_tab_t::iterator it  = new_chkpt.buf_tab.begin();
                            it != new_chkpt.buf_tab.end(); ) {
        if(it->second.dirty == false) {
            new_chkpt.buf_tab.erase(it++);
        }
        else {
            ++it;
        }
    }

    // We are done with Log Analysis, at this point each transactions in the transaction
    // table is either loser (active) or winner (ended); non-read-locks have been acquired
    // on all loser transactions. Remove finished transactions.
    for(xct_tab_t::iterator it  = new_chkpt.xct_tab.begin();
                            it != new_chkpt.xct_tab.end(); ) {
        if(it->second.state == xct_t::xct_ended) {
            new_chkpt.lck_tab.erase(it->first); //erase locks
            new_chkpt.xct_tab.erase(it++);      //erase xct
        }
        else {
            ++it;
        }
    }

    // Calculate min_rec_lsn. It is the smallest lsn from all dirty pages.
    for(buf_tab_t::iterator it  = new_chkpt.buf_tab.begin();
                            it != new_chkpt.buf_tab.end(); ++it) {
        if(new_chkpt.min_rec_lsn > it->second.rec_lsn) {
            new_chkpt.min_rec_lsn = it->second.rec_lsn;
        }
    }

    // Calculate min_xct_lsn. It is the smallest lsn from loser transactions.
    for(xct_tab_t::iterator it  = new_chkpt.xct_tab.begin();
                            it != new_chkpt.xct_tab.end(); ++it) {
        if(new_chkpt.min_xct_lsn > it->second.first_lsn) {
            new_chkpt.min_xct_lsn = it->second.first_lsn;
        }
    }

    w_base_t::base_stat_t f = GET_TSTAT(log_fetches);
    w_base_t::base_stat_t i = GET_TSTAT(log_inserts);
    smlevel_0::errlog->clog << info_prio
        << "After analysis_pass: "
        << f << " log_fetches, "
        << i << " log_inserts "
        << " redo_lsn is " << new_chkpt.min_rec_lsn
        << " undo_lsn is " << new_chkpt.min_xct_lsn
        << flushl;

    DBGOUT3 (<< "End of Log Analysis phase.  Master: " << master_lsn
             << ", redo_lsn: " << new_chkpt.min_rec_lsn
             << ", undo lsn: " << new_chkpt.min_xct_lsn);

    // TODO(Restart)... performance
    DBGOUT1( << "Number of in_doubt pages: " << in_doubt_count);

    return;
}

/*********************************************************************
*
*  chkpt_m::backward_scan_log(lock_heap)
*
*  Scans the log backwards, starting from _lsn until the t_chkpt_begin log record
*  corresponding to the latest completed checkpoint.
*
*********************************************************************/
void chkpt_m::backward_scan_log(const lsn_t master_lsn,
                                const lsn_t begin_lsn,
                                chkpt_t& new_chkpt,
                                const bool restart_with_lock)
{
    w_assert0(begin_lsn >= master_lsn);

    new_chkpt.begin_lsn = begin_lsn;
    new_chkpt.min_xct_lsn = begin_lsn;
    new_chkpt.min_rec_lsn = begin_lsn;
    new_chkpt.next_vid = 1;

    if (master_lsn == lsn_t::null) {
        // The only possibility that we have a NULL as master lsn is due to a brand new
        // start (empty) of the engine, in such case, nothing to scan.

        // LL: we have to guarantee that this is the first chkpt ever being taken.
        // In this case, the chkpt begin lsn must be the very first one.
        w_assert0(begin_lsn == lsn_t(1,0));

        DBGOUT1( << "NULL master_lsn, nothing to scan");
        return;
    }

    DBGOUT1(<<"backward_scan_log("<<begin_lsn<<", "<<master_lsn<<")");

    log_i         scan(*log, begin_lsn, false); // false == backward scan
    logrec_t      r;
    lsn_t         lsn;   // LSN of the retrieved log record

    lsn_t         theLastMountLSNBeforeChkpt = lsn_t::null;
    uint          cur_segment = 0;
    int           num_chkpt_end_handled = 0;

    // Special map to mange undecided in-flight transactions
    tid_CLR_map          mapCLR;

    // Boolean to indicate whether we need to acquire non-read lock for the current log record
    // no lock acquisition on a finished (commit or aborted) user transaction
    bool acquire_lock = true;
    bool scan_done = false;
    lsn_t chkpt_begin = master_lsn;

    while (scan.xct_next(lsn, r) && !scan_done)
    {
        acquire_lock = true; // New log record, reset the flag

        DBGOUT3( << setiosflags(ios::right) << lsn
                  << resetiosflags(ios::right) << " A: " << r );

        if (lsn != r.lsn_ck()) // If LSN is not intact, stop now
            W_FATAL_MSG(fcINTERNAL, << "Bad LSN from recovery log scan: " << lsn);

        if(lsn.hi() != cur_segment)
        {
            // Record the current segment log in partition
            cur_segment = lsn.hi();
            smlevel_0::errlog->clog << info_prio
               << "Scanning log segment " << cur_segment << flushl;
        }

        if (r.is_single_sys_xct() && !r.null_pid())
        {
            // We have a system transaction log record
            if (true == _analysis_system_log(r, new_chkpt))
            {
                // Go to next log record
                continue;
            }
            else {
                // Failure occured, do not continue
                W_FATAL_MSG(fcINTERNAL, << "Failed to process a system transaction log record during Log Analysis, lsn = " << lsn);
            }
        }

        // We already ruled out all SSX logs. So we don't have to worry about
        // multi-page logs in the code below, because multi-page log only exist
        // in system transactions
        w_assert1(!r.is_multi_page());

        // If log is transaction related, insert the transaction
        // into transaction table if it is not already there.
        if (!r.is_single_sys_xct()
            && (r.tid() != tid_t::null)             // Has a transaction ID
            && !new_chkpt.xct_tab.count(r.tid())   // does not exist in transaction table currently
            && r.type()!=logrec_t::t_comment        // Not a 'comment' log record, comments can be after xct has ended
            && r.type()!=logrec_t::t_skip           // Not a 'skip' log record
            && r.type()!=logrec_t::t_max_logrec)    // Not the special 'max' log record which marks the end
        {
            new_chkpt.xct_tab[r.tid()].state = xct_t::xct_active;
            new_chkpt.xct_tab[r.tid()].last_lsn = lsn;
            new_chkpt.xct_tab[r.tid()].undo_nxt = r.xid_prev();
            new_chkpt.xct_tab[r.tid()].first_lsn = lsn;

            // Youngest tid is the largest tid, it is used to generate
            // next unique transaction id
            if (r.tid() > new_chkpt.youngest) {
                new_chkpt.youngest = r.tid();
            }
        }
        else if (!r.is_single_sys_xct() && r.tid() != tid_t::null)            // Has a transaction ID
        {
            // Transaction exists in transaction table already

            // Due to backward log scan, if the existing transaction
            // has a state 'xct_ended', which means this transaction was
            // ended either normally or aborted, we can safely ingore
            // lock acquisition on all related log records
            // but we still need to process the log record for 'in-doubt' pages
            if (new_chkpt.xct_tab.count(r.tid())
                && (new_chkpt.xct_tab[r.tid()].state == xct_t::xct_ended))
            {
                // Do not acquire non-read-locks from this log record
                acquire_lock = false;
            }

            // If the existing transaction has a different state (not xct_ended'
            // Two possibilities:
            // In the middle of aborting transaction when system crashed- we have
            //                                 seen some compensation log records in this case,
            //                                 need further processing to determine what to do
            //                                 for this transaction, mainly by analysising all the
            //                                 normal and compensation log records for this transaction
            // In-flight transaction - need to gather all non-read-locks

            // Pass-through and process this log record
        }
        else
        {
            // Log record is not related to transaction table
            // Go ahead and process the log record
        }

        // Process based on the type of the log record
        // Modify transaction table and buffer pool accordingly
        switch (r.type())
        {
            case logrec_t::t_chkpt_begin:
                if (1 == num_chkpt_end_handled)
                {
                    // We have seen a matching 'end checkpoint' log reocrd
                    // now we have reached the 'begin checkpoint' log record

                    // Retrieve the last mount/dismount lsn from the 'begin checkpoint' log record
                    theLastMountLSNBeforeChkpt = *(lsn_t *)r.data();
                    DBGOUT3( << "Last mount LSN from chkpt_begin: " << theLastMountLSNBeforeChkpt);

                    // Signal to stop backward log scan loop now
                    // The current log record lsn might be later (larger) than the master due
                    // to a race condition, but it should not be smaller than master
                    w_assert1(master_lsn <= lsn);
                    scan_done = true;
                }
                else
                {
                    // A 'begin checkpoint' log record without matching 'end checkpoint' log record
                    // This is an incompleted checkpoint, ignore it
                }
                break;

            case logrec_t::t_chkpt_bf_tab:
                // Buffer pool dirty pages from checkpoint
                if (num_chkpt_end_handled == 1)
                {
                    // Process it only if we have seen a matching 'end checkpoint' log record
                    // meaning we are processing the last completed checkpoint
                    _analysis_ckpt_bf_log(r, new_chkpt);
                }
                else
                {
                    // No matching 'end checkpoint' log record, ignore
                }
                break;


            case logrec_t::t_chkpt_xct_lock:
                // Log record for per transaction lock information
                // Due to backward log scan, this log record (might have multiple log
                // records per active transaction) should be retrieved after the corresponding
                // t_chkpt_xct_tab log record, in other words, it was generated prior the
                // corresponding t_chkpt_xct_tab log record
                if ((num_chkpt_end_handled == 1) && (true == restart_with_lock))
                {
                    // Process it only if we have seen a matching 'end checkpoint' log record
                    // meaning we are processing the last completed checkpoint
                    // Also if we need to acquire locks (M3/M4/M5)

                    /*
                    if(new_chkpt.lock_mode.size() < new_chkpt.tid.size()) {
                        new_chkpt.lock_mode.resize(new_chkpt.tid.size());
                    }

                    if(new_chkpt.lock_hash.size() < new_chkpt.tid.size()) {
                        new_chkpt.lock_hash.resize(new_chkpt.tid.size());
                    }
                    */

                    _analysis_ckpt_lock_log(r, new_chkpt);
                }
                else
                {
                    // No matching 'end checkpoint' log record, ignore
                }
                break;

            case logrec_t::t_chkpt_xct_tab:
                // Transaction table entries from checkpoint
                if (num_chkpt_end_handled == 1)
                {
                    // Process it only if we have seen a matching 'end checkpoint' log record
                    // meaning we are processing the last completed checkpoint
                    _analysis_ckpt_xct_log(r, new_chkpt, mapCLR);
                }
                else
                {
                    // No matching 'end checkpoint' log record, , ignore
                }
                break;

            case logrec_t::t_chkpt_backup_tab:
                if (num_chkpt_end_handled == 1)
                {
                    // backups can simply be redone in reverse order because, for
                    // now at least, there is no delete_backup operation. If there
                    // would be one, we would have to replay them in reverse to
                    // take care of a sequence of add and delete, just like in
                    // chkpt_dev_tab and mount/dismounts.
                    //r.redo(0);
                }
                break;

            case logrec_t::t_chkpt_dev_tab:
                if (num_chkpt_end_handled == 1)
                {
                    // Process it only if we have seen a matching 'end checkpoint' log record
                    // meaning we are processing the last completed checkpoint
                    chkpt_dev_tab_t* tab = (chkpt_dev_tab_t*) r.data();
                    std::vector<string> dnames;
                    tab->read_devnames(dnames);
                    w_assert0(tab->count == dnames.size());
                    for (int i = 0; i < tab->count; i++) {
                        if( new_chkpt.dev_tab.count(dnames[i]) ) {
                            // volume is already in the list and it is safe to
                            // ignore information about it from the previous chkpt,
                            // since it is guaranteed to be out-dated
                        }
                        else {
                            new_chkpt.dev_tab[dnames[i]].dev_mounted = true;
                            new_chkpt.dev_tab[dnames[i]].dev_lsn = lsn_t::null;
                            // dev_lsn = null, because any further log record
                            // about this device is going to be more recent
                        }
                    }
                    //-1 because next_vid already starts at 1
                    new_chkpt.next_vid += (tab->next_vid - 1);
                }
                break;

            case logrec_t::t_mount_vol:
                {
                    string dev = (const char*)(r.data_ssx());
                    if(new_chkpt.dev_tab.count(dev)) {
                        // volume exists already ...
                        if(new_chkpt.dev_tab[dev].dev_lsn < lsn) {
                            // ... but the log record is more recent
                            new_chkpt.dev_tab[dev].dev_mounted = true;
                            new_chkpt.dev_tab[dev].dev_lsn = lsn;
                        }
                    }
                    else {
                        // volume does not exist yet
                        new_chkpt.dev_tab[dev].dev_mounted = true;
                        new_chkpt.dev_tab[dev].dev_lsn = lsn;
                    }
                }
                break;

            case logrec_t::t_format_vol:
                new_chkpt.next_vid++;
                break;

            case logrec_t::t_dismount_vol:
                {
                    string dev = (const char*)(r.data_ssx());
                    if(new_chkpt.dev_tab.count(dev)) {
                        // volume exists already ...
                        if(new_chkpt.dev_tab[dev].dev_lsn < lsn) {
                            // ... but the log record is more recent
                            new_chkpt.dev_tab[dev].dev_mounted = false;
                            new_chkpt.dev_tab[dev].dev_lsn = lsn;
                        }
                    }
                    else {
                        // volume does not exist yet
                        new_chkpt.dev_tab[dev].dev_mounted = false;
                        new_chkpt.dev_tab[dev].dev_lsn = lsn;
                    }
                }
                break;

            case logrec_t::t_add_backup:
                {
                    vid_t vid = *((vid_t*) (r.data_ssx()));
                    const char* dev = (const char*)(r.data_ssx() + sizeof(vid_t));

                    new_chkpt.bkp_tab[vid].bkp_path = dev;
                }
            break;

            case logrec_t::t_chkpt_end:
                if (num_chkpt_end_handled == 0)
                {
                    // Found the first 'end checkpoint' which is the last completed checkpoint.
                    // Retrieve information from 'end checkpoint':
                    // 'min_rec_lsn' - the minimum lsn of all buffer pool dirty or in_doubt pages
                    //         log scan REDO phase (if used) starts with the earliest LSN of all in_doubt pages
                    // 'min_txn_lsn' - the minimum txn lsn is the earliest lsn for all in-flight transactions
                    //         backward scan UNDO phase (if used) stops at the minimum txn lsn

                    memcpy(&(chkpt_begin), (lsn_t*) r.data(), sizeof(lsn_t));
                    //memcpy(&(new_chkpt.min_rec_lsn), ((lsn_t*) r.data())+1, sizeof(lsn_t));
                    //memcpy(&(new_chkpt.min_xct_lsn), ((lsn_t*) r.data())+2, sizeof(lsn_t));

                    // The 'begin checkpoint' lsn should be either the same or later (newer)
                    // than the one (due to race condition) specified by caller, but not earlier
                    if (master_lsn > chkpt_begin)
                        W_FATAL_MSG(fcINTERNAL,
                                    << "Master from 'end checkpoint' is earlier than the one specified by caller of Log Analysis");

                    DBGOUT3(<<"t_chkpt_end log record: master=" << chkpt_begin);
                            //<< " min_rec_lsn= " << new_chkpt.min_rec_lsn
                            //<< " min_txn_lsn= " << new_chkpt.min_xct_lsn);
                }

                // Backward log scan. Update 'num_chkpt_end_handled' which stops the scan
                // once we have a matching 'begin checkpoint' log record
                num_chkpt_end_handled++;

                break;

            case logrec_t::t_xct_freeing_space:
                // A t_xct_freeing_space log record is generated when entering
                // txn state 'xct_freeing_space' which is before txn commit or abort.
                // If system crashed before the final txn commit or abort occurred,
                // the recovery log does not know whether the txn should be
                // committed or abort.
                // Due to backward log scan, if we encounter this log record but the
                // transaction was not marked as 'ended' already, we are falling into the
                // scenario that the very last 'transaction end/abort' log record did not
                // get harden before system crash, although all transaction related
                // operations were logged and done.  There is no need to rollback this
                // transaction.  This is a winner transaction (it could be either a commit
                // or abort transaction)

                if (xct_t::xct_ended != new_chkpt.xct_tab[r.tid()].state)
                    new_chkpt.xct_tab[r.tid()].state = xct_t::xct_ended;
                break;

            case logrec_t::t_xct_end_group:
                {
                    // Do what we do for t_xct_end for each of the
                    // transactions in the list
                    const xct_list_t* list = (xct_list_t*) r.data();
                    uint listlen = list->count;

                    for(uint i=0; i<listlen; i++) {
                        tid_t tid = list->xrec[i].tid;
                        // If it's not there, it could have been a read-only xct?
                        if(new_chkpt.xct_tab.count(tid)
                            && new_chkpt.xct_tab[tid].state != xct_t::xct_ended) {
                            // Mark the txn as ended, safe to remove it from transaction table
                            new_chkpt.xct_tab[tid].state = xct_t::xct_ended;
                        }
                    }
                }
                break;

            case logrec_t::t_xct_abort:
                // Transaction aborted before system crash.
                w_assert1(new_chkpt.xct_tab[r.tid()].state != xct_t::xct_ended);

                // fall-through

            case logrec_t::t_xct_end:
                // Log record indicated this txn has ended or aborted
                // It is safe to remove it from transaction table
                // Also no need to gather non-read locks on this transaction
                if (new_chkpt.xct_tab[r.tid()].state != xct_t::xct_ended)
                    new_chkpt.xct_tab[r.tid()].state = xct_t::xct_ended;
                break;

            case logrec_t::t_compensate:
            case logrec_t::t_store_operation:
            case logrec_t::t_page_set_to_be_deleted:
            case logrec_t::t_page_img_format:
            case logrec_t::t_btree_norec_alloc:
            case logrec_t::t_btree_split:
            case logrec_t::t_btree_insert:
            case logrec_t::t_btree_insert_nonghost:
            case logrec_t::t_btree_update:
            case logrec_t::t_btree_overwrite:
            case logrec_t::t_btree_ghost_mark:
            case logrec_t::t_btree_ghost_reclaim:
            case logrec_t::t_btree_ghost_reserve:
            case logrec_t::t_btree_foster_adopt:
            case logrec_t::t_btree_foster_merge:
            case logrec_t::t_btree_foster_rebalance:
            case logrec_t::t_btree_foster_rebalance_norec:
            case logrec_t::t_btree_foster_deadopt:
                // The rest of meanful log records, transaction has been created already
                // we need to take care of both buffer pool and lock acquisition if needed
                {
                    // Take care of common stuff among forward and backward log scan first
                    _analysis_other_log(r, new_chkpt);

                    if ((true == acquire_lock) && (true == restart_with_lock))
                    {
                        // We need to acquire locks (M3/M4/M5) and this is an
                        // undecided in-flight transaction, process lock for this log record
                        _analysis_process_lock(r, new_chkpt, mapCLR);
                    }
                    else
                    {
                        // winner transaction, no need to acquire locks
                    }

                    if (r.tid() != tid_t::null
                        && new_chkpt.xct_tab.count(r.tid())
                        && r.is_page_update()
                        && r.xid_prev() == lsn_t::null
                        && new_chkpt.xct_tab[r.tid()].state == xct_t::xct_ended)
                    {
                        // If this is an update log record, and we have already seen the
                        // transaction end/abort log record for this transaction (backward scan),
                        // and the current log record is the very first log record of this transaction
                        // it is safe to remove this transaction from transaction table now.

                        // Delete the transaction from transaction table because it
                        // reduces the size of llinked-list transaction table and improve
                        // the xct_t::look_up performance of the transaction table
                        new_chkpt.xct_tab.erase(r.tid());
                        new_chkpt.lck_tab.erase(r.tid());
                    }
                }
                break;

            case logrec_t::t_restore_begin:
            case logrec_t::t_restore_end:
            case logrec_t::t_restore_segment:
            case logrec_t::t_chkpt_restore_tab:
                // CS TODO - IMPLEMENT!
                break;

            case logrec_t::t_alloc_a_page:
            case logrec_t::t_dealloc_a_page:
                // do nothing -- page replay will reallocate if necessary
                break;

            case logrec_t::t_page_write:
                {
                    lpid_t* pid_begin = (lpid_t*)(r.data());
                    uint32_t* count = (uint32_t*)(r.data() + sizeof(lpid_t));
                    for(uint i=0; i<*count; i++) {
                        lpid_t pid = *pid_begin;
                        pid.page += i;

                        if(new_chkpt.buf_tab.count(pid)) {
                            // Page exists in bf_table, we mark it as !dirty
                            if(new_chkpt.buf_tab[pid].page_lsn < lsn) {
                                new_chkpt.buf_tab[pid].page_lsn = lsn;
                                new_chkpt.buf_tab[pid].dirty = false;
                            }
                        }
                        else {
                            // Page does not exist in bf_table, we add it with
                            // dummy values and mark as !dirty
                            new_chkpt.buf_tab[pid].store = 0;
                            new_chkpt.buf_tab[pid].rec_lsn = lsn_t::null;
                            new_chkpt.buf_tab[pid].page_lsn = lsn;
                            new_chkpt.buf_tab[pid].dirty = false;
                        }
                    }
                }
                break;

            default:
                // CS: do nothing if we can't recognize logrec
                // if (r.type()!=logrec_t::t_comment &&   // Comments
                //     !r.is_skip() &&                    // Marker for the end of partition
                //     r.type()!=logrec_t::t_max_logrec)  // End of log type
                // {
                //     // Retrieve a log buffer which we don't know how to handle
                //     // Raise erroe
                //     W_FATAL_MSG(fcINTERNAL, << "Unexpected log record type from default: "
                //             << r.type_str());
                // }
                break;
        } //switch
    } //while

    // The assumption is that we must scan a completed checkpoint, stop
    // if the assumption failed.
    if (false == scan_done)
        W_FATAL_MSG(fcINTERNAL, << "Did not scan a completed checkpoint");

    // redo_lsn is where the REDO phase should start for the forward scan (if used),
    // it must be the earliest LSN for all in_doubt pages, which could be earlier
    // than the begin checkpoint LSN
    // undo_lsn is where the UNDO phase should stop for the backward scan (if used),
    // it must be the earliest LSN for all transactions, which could be earlier than
    // the begin checkpoint LSN
    // begin_chkpt is retrieved from 'end checkpoint' log record, it must be set
    // otherwise stop the execution since we must reached an 'end checkpoint' log record
    if (chkpt_begin == lsn_t::null) {
        W_FATAL_MSG(fcINTERNAL, << "Missing begin_chkpt lsn at the end of Log Analysis phase");
    }

    w_assert1(chkpt_begin >= master_lsn);

    // If there were any mounts/dismounts that occured between redo_lsn and
    // begin chkpt, need to redo them
    DBGOUT3( << ((theLastMountLSNBeforeChkpt != lsn_t::null &&
                    theLastMountLSNBeforeChkpt > redo_lsn) \
            ? "redoing mounts/dismounts before chkpt but after redo_lsn"  \
            : "no mounts/dismounts need to be redone"));

    // Done with backward log scan, check the compensation list
    _analysis_process_compensation_map(mapCLR, new_chkpt);

    // Remove non-mounted devices
    for(dev_tab_t::iterator it  = new_chkpt.dev_tab.begin();
                            it != new_chkpt.dev_tab.end(); ){
        if(it->second.dev_mounted == false) {
            new_chkpt.dev_tab.erase(it++);
        }
        else {
            ++it;
        }
    }

    // Remove non-dirty pages
    for(buf_tab_t::iterator it  = new_chkpt.buf_tab.begin();
                            it != new_chkpt.buf_tab.end(); ) {
        if(it->second.dirty == false) {
            new_chkpt.buf_tab.erase(it++);
        }
        else {
            ++it;
        }
    }

    // We are done with Log Analysis, at this point each transactions in the transaction
    // table is either loser (active) or winner (ended); non-read-locks have been acquired
    // on all loser transactions. Remove finished transactions.
    for(xct_tab_t::iterator it  = new_chkpt.xct_tab.begin();
                            it != new_chkpt.xct_tab.end(); ) {
        if(it->second.state == xct_t::xct_ended) {
            new_chkpt.lck_tab.erase(it->first); //erase locks
            new_chkpt.xct_tab.erase(it++);      //erase xct
        }
        else {
            ++it;
        }
    }

    // Calculate min_rec_lsn. It is the smallest lsn from all dirty pages.
    for(buf_tab_t::iterator it  = new_chkpt.buf_tab.begin();
                            it != new_chkpt.buf_tab.end(); ++it) {
        if(new_chkpt.min_rec_lsn > it->second.rec_lsn) {
            new_chkpt.min_rec_lsn = it->second.rec_lsn;
        }
    }

    // Calculate min_xct_lsn. It is the smallest lsn from loser transactions.
    for(xct_tab_t::iterator it  = new_chkpt.xct_tab.begin();
                            it != new_chkpt.xct_tab.end(); ++it) {
        if(new_chkpt.min_xct_lsn > it->second.first_lsn) {
            new_chkpt.min_xct_lsn = it->second.first_lsn;
        }
    }

    w_base_t::base_stat_t f = GET_TSTAT(log_fetches);
    w_base_t::base_stat_t i = GET_TSTAT(log_inserts);
    smlevel_0::errlog->clog << info_prio
        << "After analysis_pass: "
        << f << " log_fetches, "
        << i << " log_inserts "
        << " redo_lsn is " << new_chkpt.min_rec_lsn
        << " undo_lsn is " << new_chkpt.min_xct_lsn
        << flushl;

    DBGOUT3 (<< "End of Log Analysis phase.  Master: " << master_lsn
             << ", redo_lsn: " << new_chkpt.min_rec_lsn
             << ", undo lsn: " << new_chkpt.min_xct_lsn);

    // TODO(Restart)... performance
    DBGOUT1( << "Number of in_doubt pages: " << in_doubt_count);

    return;
}

/*********************************************************************
 *
 *  chkpt_m::_analysis_system_log(r, lsn, in_doubt_count)
 *
 *  Helper function to process one system log record, called by both analysis_pass_forward
 *  and analysis_pass_backward
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
bool chkpt_m::_analysis_system_log(logrec_t& r, chkpt_t& new_chkpt) // In: Log record to process
{
    // Only system transaction log record should come in here
    w_assert1(r.is_single_sys_xct());

    // If the log was a system transaction fused to a single log entry,
    // we should do the equivalent to xct_end, but take care of marking the
    // in_doubt page in buffer pool first

    // Note currently all system transactions are single log entry, we do not have
    // system transaction involving multiple log records

    if (false == r.is_single_sys_xct())
    {
        // Not a system transaction log record
        return false;
    }
    else
    {
        lsn_t lsn = r.get_lsn_ck();

        // Construct a system transaction into transaction table
        new_chkpt.xct_tab[tid_t::Max].state = xct_t::xct_active;
        new_chkpt.xct_tab[tid_t::Max].last_lsn = lsn;
        new_chkpt.xct_tab[tid_t::Max].undo_nxt = lsn_t::null;
        new_chkpt.xct_tab[tid_t::Max].first_lsn = lsn;

        // Get the associated page
        lpid_t page_of_interest = r.construct_pid();
        //DBGOUT3(<<"analysis (single_log system xct): default " <<  r.type()
        //        << " page of interest " << page_of_interest);

        w_assert1(!r.is_undo()); // no UNDO for ssx
        w_assert0(r.is_redo());  // system txn is REDO only

        // Register the page into buffer pool (don't load the actual page)
        // If the log record describe allocation of a page, then
        // Allocation of a page (t_alloc_a_page, t_alloc_consecutive_pages) - clear
        //        the in_doubt bit, because the page might be allocated for a
        //        non-logged operation (e.g., bulk load) which is relying on the page not
        //        being formatted as a regular page.
        //        We clear the in_doubt flag but keep the page in hash table so the page
        //        is considered as used.    A page format log record should come if this is
        //        a regular B-tree page, whcih would mark the in_doubt flag for this page
        // De-allocation of a page (t_dealloc_a_page, t_page_set_to_be_deleted) -
        //        clear the in_doubt bit and remove the page from hash table so the page
        //        slot is available for a different page

        if (r.type() == logrec_t::t_alloc_a_page || r.type() == logrec_t::t_dealloc_a_page)
        {
            // Remove the in_doubt flag in buffer pool of the page if it exists in buffer pool
            if(new_chkpt.buf_tab.count(page_of_interest))
            {
                new_chkpt.buf_tab.erase(page_of_interest);

                // CHECK THIS LATER:
                // Page cb is in buffer pool, clear the 'in_doubt' and 'used' flags
                // If the cb for this page does not exist in buffer pool, no-op
                //if (true == smlevel_0::bf->is_in_doubt(idx))
                //{
                //    if (r.type() == logrec_t::t_alloc_a_page)
                //        smlevel_0::bf->clear_in_doubt(idx, true, key);    // Page is still used
                //    else
                //        smlevel_0::bf->clear_in_doubt(idx, false, key);   // Page is not used
                //    w_assert1(0 < in_doubt_count);
                //    --in_doubt_count;
                //}
            }
        }
        else
        if (false == r.is_skip())    // t_skip marks the end of partition, no-op
        {
            // System transaction does not have txn id, but it must have page number
            // this is true for both single and multi-page system transactions

            if (false == r.null_pid())
            {
                // If the log record has a valid page ID, the operation affects buffer pool
                // Register the page cb in buffer pool (if not exist) and mark the in_doubt flag
                if (0 == page_of_interest.page)
                    W_FATAL_MSG(fcINTERNAL, << "Page # = 0 from a system transaction log record");

                if(new_chkpt.buf_tab.count(page_of_interest)) {   // page is already in bf_table
                    if(new_chkpt.buf_tab[page_of_interest].rec_lsn > lsn) {
                        new_chkpt.buf_tab[page_of_interest].rec_lsn = lsn;
                    }

                    if(new_chkpt.buf_tab[page_of_interest].page_lsn < lsn) {
                        new_chkpt.buf_tab[page_of_interest].page_lsn = lsn;
                        new_chkpt.buf_tab[page_of_interest].dirty = true;
                    }

                    new_chkpt.buf_tab[page_of_interest].store = r.snum();
                }
                else {  // page is not present in buffer table
                    new_chkpt.buf_tab[page_of_interest].store = r.snum();
                    new_chkpt.buf_tab[page_of_interest].rec_lsn = lsn;
                    new_chkpt.buf_tab[page_of_interest].page_lsn = lsn;
                    new_chkpt.buf_tab[page_of_interest].dirty = true;
                }

                // If we get here, we have registed a new page with the 'in_doubt' and 'used' flags
                // set to true in page cb, but not load the actual page

                // If the log touches multi-records, we put that page in buffer pool too.
                // SSX is the only log type that has multi-pages.
                // Note this logic only deal with a log record with 2 pages, no more than 2
                // System transactions with multi-records:
                //      btree_norec_alloc_log - 2nd page is a new page which needs to be allocated
                //      btree_foster_adopt_log
                //      btree_foster_merge_log
                //      btree_foster_rebalance_log
                //      btree_foster_rebalance_norec_log - during a page split, foster parent page would split
                //                                                           does it allocate a new page?
                //      btree_foster_deadopt_log

                if (r.is_multi_page())
                {
                    lpid_t page2_of_interest = r.construct_pid2();
                    //DBGOUT3(<<" multi-page:" <<  page2_of_interest);
                    if (0 == page2_of_interest.page)
                    {
                        if (r.type() == logrec_t::t_btree_norec_alloc)
                        {
                            // 2nd page is a virgin page
                            W_FATAL_MSG(fcINTERNAL,
                                << "Page # = 0 from t_btree_norec_alloca system transaction log record");
                        }
                        else
                        {
                            W_FATAL_MSG(fcINTERNAL,
                                << "Page # = 0 from a multi-record system transaction log record");
                        }
                    }

                    if(new_chkpt.buf_tab.count(page2_of_interest)) {
                        if(new_chkpt.buf_tab[page2_of_interest].rec_lsn > lsn) {
                            new_chkpt.buf_tab[page2_of_interest].rec_lsn = lsn;
                        }

                        if(new_chkpt.buf_tab[page2_of_interest].page_lsn < lsn) {
                            new_chkpt.buf_tab[page2_of_interest].page_lsn = lsn;
                            new_chkpt.buf_tab[page2_of_interest].dirty = true;
                        }

                        new_chkpt.buf_tab[page2_of_interest].store = r.snum();
                    }
                    else {
                        new_chkpt.buf_tab[page2_of_interest].store = r.snum();
                        new_chkpt.buf_tab[page2_of_interest].rec_lsn = lsn;
                        new_chkpt.buf_tab[page2_of_interest].page_lsn = lsn;
                        new_chkpt.buf_tab[page2_of_interest].dirty = true;
                    }
                }
            }
            else
            {
                return false;
            }
        }

        // Because all system transactions are single log record, there is no
        // UNDO for system transaction.
        new_chkpt.xct_tab[tid_t::Max].state = xct_t::xct_ended;

        // The current log record is for a system transaction which has been handled above
        // done with the processing of this system transaction log record
    }
    return true;
}

/*********************************************************************
 *
 *  chkpt_m::_analysis_ckpt_bf_log(r, in_doubt_count)
 *
 *  Helper function to process the chkpt_bf_tab log record, called by both
 *  analysis_pass_forward and analysis_pass_backward
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
void chkpt_m::_analysis_ckpt_bf_log(logrec_t& r,           // In: Log record to process
                                    chkpt_t& new_chkpt)    // In/Out:
{
    const chkpt_bf_tab_t* dp = (chkpt_bf_tab_t*) r.data();

    for (uint i = 0; i < dp->count; i++)
    {
        // For each entry in log,
        // if it is not in buffer pool, register and mark it.
        // If it is already in the buffer pool, update the rec_lsn to the earliest LSN

        if (0 == dp->brec[i].pid.page)
            W_FATAL_MSG(fcINTERNAL, << "Page # = 0 from a page in t_chkpt_bf_tab log record");

        lpid_t pid = dp->brec[i].pid;
        if(new_chkpt.buf_tab.count(pid)) {

            // This page is already in the bf_table but marked as !dirty,
            // meaning there was a more recent t_page_write log record referring
            // to it. We ignore the information from the chkpt_bf_tab
            if(new_chkpt.buf_tab[pid].dirty == false) continue;

            if(new_chkpt.buf_tab[pid].rec_lsn > dp->brec[i].rec_lsn.data()) {
                new_chkpt.buf_tab[pid].rec_lsn = dp->brec[i].rec_lsn.data();
            }

            if(new_chkpt.buf_tab[pid].page_lsn < dp->brec[i].page_lsn.data()) {
                new_chkpt.buf_tab[pid].page_lsn = dp->brec[i].page_lsn.data();
            }
        }
        else {
            new_chkpt.buf_tab[pid].store =  dp->brec[i].store;
            new_chkpt.buf_tab[pid].rec_lsn = dp->brec[i].rec_lsn.data();
            new_chkpt.buf_tab[pid].page_lsn = dp->brec[i].page_lsn.data();
            new_chkpt.buf_tab[pid].dirty = true;
        }
    }
    return;
}


/*********************************************************************
 *
 *  chkpt_m::_analysis_ckpt_xct_log(r, lsn, mapCLR)
 *
 *  Helper function to process the t_chkpt_xct_tab log record, called by
 *  analysis_pass_backward only
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
void chkpt_m::_analysis_ckpt_xct_log(logrec_t& r,          // In: Current log record
                                     chkpt_t& new_chkpt,   // In/Out:
                                     tid_CLR_map& mapCLR)  // In/Out: map to hold counters for in-flight transactions
{
    // Received a t_chkpt_xct_tab log record from backward log scan
    // and there was a matching end checkpoint log record,
    // meaning we are processing the last completed checkpoint
    // go ahead and process this log record

    lsn_t lsn = r.get_lsn_ck();

    w_assert1(lsn.valid());
    const chkpt_xct_tab_t* dp = (chkpt_xct_tab_t*) r.data();

    // Pick up the youngest (largest) tid from checkpoint, we will have this value
    // even if there is no transaction entry in the log record
    if (dp->youngest > new_chkpt.youngest)
        new_chkpt.youngest = dp->youngest;

    // For each entry in the log, it is possible we do not have any transaction entry
    uint iCount = 0;
    uint iTotal = dp->count;
    for (iCount = 0; iCount < iTotal; ++iCount)
    {
        w_assert1(tid_t::null != dp->xrec[iCount].tid);
        tid_t tid = dp->xrec[iCount].tid;

        // We know the transaction was active when the checkpoint was taken,
        // but we do not know whether the transaction was in the middle of
        // normal processing or rollback, or already ended by the time checkpoint
        // finished (because checkpoint is a non-blocking operation).
        // If a transaction object did not exsit in the transaction table at this point,
        // create a transaction for it.  If the transacter did not end during the checkpoint
        // mark it as a loser transaction.  If the transaction ended during checkpoint,
        // mark it as a winner transaction.  No need to update mapCLR in either case.
        // If a transaction object exists in the transaction table at this point,
        // it should be a loser transaction, update to the mapCLR to make sure
        // this is a loser transaction

        if (new_chkpt.xct_tab.count(tid) == 0)
        {
            // Not found in the transaction table

            // The t_chkpt_xct_tab log record was generated by checkpoint, while
            // checkpoint is a non-blocking operation and might take some time
            // to finish the operation.

            // Two cases:
            // 1. Normal case: An in-flight transaction when the checkpoint was taken,
            //      but no activity between checkpoint and system crash
            //      Need to insert this in-flight (loser) transaction into transaction table
            //
            // 2. Corner case: A transaction was active when the checkpoint was
            //      gathering transaction information, it is possible an active transaction ended
            //      before the checkpoint finished its work (it could take some time to gather
            //      transaction information, especially if there are many active transactions),
            //      therefore the 'end transaction' log record for this transaction would be generated
            //      before the checkpoint transaction log record.
            //      If a transaction ended before this checkpoint transaction log record was written,
            //      the 'end transaction' log record can only occur between 'begin checkpoint'
            //      log record and the current checkpoint transaction log record.
            //      For backward log scan (this function), we will see the 'end transaction' log
            //      record after this checkpoint transaction log record and will be able to mark
            //      the transaction status (winner or loser) correctly.  Therefore if a transaction
            //      does not exist in transaction table currently (no activity after the current checkpoint
            //      transaction log record), insert the transaction as a loser transaction into
            //      the transaction table, which is the correct transaction status.

            // Checkpoint thinks this is an in-flight transaction (including transaction
            // in the middle of commiting or aborting) but we have not seen
            // any log record from this in-flight transaction during the backward
            // log scan so far
            w_assert1((xct_t::xct_active == dp->xrec[iCount].state) ||
                      (xct_t::xct_chaining == dp->xrec[iCount].state) ||
                      (xct_t::xct_t::xct_committing == dp->xrec[iCount].state) ||
                      (xct_t::xct_t::xct_aborting == dp->xrec[iCount].state));

            // Since the transaction does not exist in transaction yet
            // create it into the transaction table first
            new_chkpt.xct_tab[tid].state = dp->xrec[iCount].state;
            new_chkpt.xct_tab[tid].last_lsn = dp->xrec[iCount].last_lsn;
            new_chkpt.xct_tab[tid].undo_nxt = dp->xrec[iCount].undo_nxt;
            new_chkpt.xct_tab[tid].first_lsn = dp->xrec[iCount].first_lsn;

            if (dp->xrec[iCount].tid > new_chkpt.youngest)
                new_chkpt.youngest = dp->xrec[iCount].tid;

            // No log record on this transaction after this checkpoint was taken,
            // but we know this transaction was active when the checkpoint
            // started and it did not end after the checkpoint finished.
            // Mark this transaction as a loser transaction regardless whether
            // the transaction was in the middle of rolling back or not

            //DBGOUT3(<<"add xct " << dp->xrec[iCount].tid
            //        << " state " << dp->xrec[iCount].state
            //        << " last lsn " << dp->xrec[iCount].last_lsn
            //        << " undo " << dp->xrec[iCount].undo_nxt
            //        << ", first lsn " << dp->xrec[iCount].first_lsn);
        }
        else
        {
           // Found in the transaction table, it must be marked as:
           // undecided in-flight transaction (active) - active transaction during checkpoint
           //                                                                     and was either active when system crashed or
           //                                                                     in the middle of aborting when system crashed,
           //                                                                     in other words, we did not see end transaction
           //                                                                     log record for this transaction.  We might have
           //                                                                     seen compensation log records from this transaction
           //                                                                     due to transaction abort or savepoint partial rollback.
           //                                                                     The undeicded in-flight transaction requires special
           //                                                                     handling to determine whether it is a loser or winner.
           // winner transaction - transaction ended after the checkpoint but
           //                                                                     before system crash

           w_assert1((xct_t::xct_active == new_chkpt.xct_tab[tid].state) ||
                     (xct_t::xct_ended == new_chkpt.xct_tab[tid].state));
           if (xct_t::xct_active == new_chkpt.xct_tab[tid].state)
           {
               // Undecided in-flight transaction

               tid_CLR_map::iterator search = mapCLR.find(r.tid().as_int64());
               if ((search != mapCLR.end()) && (0 == search->second))
               {
                   // If tid exists in the map, this is an existing undecided in-flight
                   // transaction, also we have seen the same amount of original and
                   // compensation log records from the backward log scan (0 == count)
                   // but the transaction was active when the checkpoint was taken, which
                   // means there were more activities on the transaction before checkpoint
                   // was taken, just to be safe so we don't accidently turn an undecided transaction
                   // into a winner incorrectly, increase the counter by 1 to ensure it is a loser
                   // transaction
                   //
                   // Note that checkpoint is a non-blocking and potentially long lasting
                   // operation which could generate multiple checkpoint log records, there
                   // might be update log records inter-mixed with checkpoint log records

                   mapCLR[r.tid().as_int64()] += 1;
               }
               else
               {
                   // Either the transaction does not exist in the map or the count is not 0
                   // both situations, this is a loser, no op since the loser transaction exists
                   // in the transaction table already
               }
           }
           else
           {
               // Transaction ended (winner), no op
           }
        }
    }

    if (iCount != dp->count)
    {
        // Failed to process all the transactions in log record, error out
        W_FATAL_MSG(fcINTERNAL, << "restart_m::_analysis_ckpt_xct_log: log record has "
                    << dp->count << " transactions but only processed "
                    << iCount << " transactions");
    }
    return;
}

/*********************************************************************
*
*  chkpt_m::_analysis_ckpt_lock_log(r, xd, lock_heap)
*
*  Helper function to process lock re-acquisition for an active transaction in
*  a checkpoint log record
*  called by analysis_pass_backward only
*
*  System is not opened during Log Analysis phase
*
*********************************************************************/
void chkpt_m::_analysis_ckpt_lock_log(logrec_t& r,            // In: log record
                                              chkpt_t& new_chkpt)     // In/Out: checkpoint being generated
{
    // A special function to re-acquire non-read locks for an active transaction
    // in a checkpoint log record and add acquired lock information to the associated
    // transaction object.

    // Called during Log Analysis phase for backward log scan, the
    // buffer pool pages were not loaded into buffer pool and the system
    // was not opened for user transactions, therefore it is safe to access lock
    // manager during Log Analysis phase, no latch would be held when
    // accessing lock manager to re-acqure non-read locks.
    // It should not encounter lock conflicts during lock re-acquisition, because
    // if any conflicts, pre-crash transaction processing would have found them

    const chkpt_xct_lock_t* dp = (chkpt_xct_lock_t*) r.data();


    // If the transaction tid specified in the log record exists in transaction table and
    // it is an in-flight transaction, re-acquire locks on it
    if(new_chkpt.xct_tab.count(dp->tid)) {
        // Transaction exists and in-flight
        if(new_chkpt.xct_tab[dp->tid].state == xct_t::xct_active) {

            // Re-acquire locks:

            if (0 == dp->count) {
                return; // No lock to process
            }
            else {
                // Go through all the locks and re-acquire them on the transaction object
                for (uint i = 0; i < dp->count; i++) {
                    DBGOUT3(<<"_analysis_acquire_ckpt_lock_log - acquire key lock, hash: " << dp->xrec[i].lock_hash
                            << ", key lock mode: " << dp->xrec[i].lock_mode.get_key_mode());
                    lck_tab_entry_t entry;
                    entry.lock_mode = dp->xrec[i].lock_mode;
                    entry.lock_hash = dp->xrec[i].lock_hash;

                    new_chkpt.lck_tab[dp->tid].push_back(entry);
                }

                return;
            }
        }
    }
    else {
        // Due to backward log scan, we should process the
        // associated t_chkpt_xct_tab log record (contain multiple
        // active transactions) before we get to the individual
        // t_chkpt_xct_lock log record (one transaction per log record
        // while a transaction might generate multiple t_chkpt_xct_lock
        // log records), therefore the transaction should exists in the
        // transaction table (either winer or loser) at this point.
        // If we do not find the associated transaction in transaction
        // table, this is unexpected situation, raise an error
        W_FATAL_MSG(fcINTERNAL, << "Log record t_chkpt_xct_lock contains a transaction which does not exist, tid:" << dp->tid);
    }
}

/*********************************************************************
 *
 *  chkpt_m::_analysis_other_log(r, lsn, in_doubt_count, xd)
 *
 *  Helper function to process the rest of meaningful log records, called by both
 *  analysis_pass_forward and analysis_pass_backward
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
void chkpt_m::_analysis_other_log(logrec_t& r,               // In: log record
                                  chkpt_t& new_chkpt)        // In/Out:

{
    lsn_t lsn = r.get_lsn_ck();
    lpid_t page_of_interest = r.construct_pid();
    //DBGOUT3(<<"analysis: default " <<
    //    r.type() << " tid " << r.tid()
    //    << " page of interest " << page_of_interest);
    if (r.is_page_update() && r.type() != logrec_t::t_store_operation)
    {
        // Log record affects buffer pool, and it is not a compensation log record
        //DBGOUT3(<<"is page update " );
        //DBGOUT3( << setiosflags(ios::right) << lsn
        //    << resetiosflags(ios::right) << " A: "
        //    << "is page update " << page_of_interest );
        // redoable, has a pid, and is not compensated.
        if (r.is_undo())
        {
            // r is undoable.
            // If forward log scan, the current txn undo_nxt
            // contains the information from previous log record
            // the incoming lsn should be later than the existing one
            // If backward log scan, undo_lsn should be later than the
            // incoming one
            // We want the transaction undo_lsn to be the latest lsn so
            // it can be used in UNDO for rollback operation

            if (true == use_undo_reverse_restart())
            {
                // If UNDO is using reverse chronological order (use_undo_reverse_restart())
                // this is forward log scan in Log Analysis, set the undo_nxt lsn
                // to the current log record lsn because
                // UNDO is using reverse chronological order
                // and the undo_lsn is used to stop the individual rollback

                if (new_chkpt.xct_tab[r.tid()].undo_nxt < lsn)
                    new_chkpt.xct_tab[r.tid()].undo_nxt = lsn;
            }
            else
            {
                // If UNDO is txn driven, set undo_nxt lsn to the largest (latest) lsn.
                // Abort operation use it to retrieve log record and follow the log
                // record undo_next list
                // This is for both forward and backward log scan in Log Analysis

                if (new_chkpt.xct_tab[r.tid()].undo_nxt < lsn)
                    new_chkpt.xct_tab[r.tid()].undo_nxt = lsn;
            }
        }

        // This type of log record must be redoable
        w_assert0(r.is_redo());

        // These log records are not compensation log and affected buffer pool pages
        // we need to record these in_doubt pages in buffer pool
        // Exceptions:
        // Allocation of a page (t_alloc_a_page, t_alloc_consecutive_pages) - clear
        //                   the in_doubt bit, because the page might be allocated for a
        //                   non-logged operation, we don't want to re-format the page
        // De-allocation of a page (t_dealloc_a_page, t_page_set_to_be_deleted) -
        //                   clear the in_doubt bit, so the page can be evicted if needed.
        if (r.type() == logrec_t::t_alloc_a_page || r.type() == logrec_t::t_dealloc_a_page)
        {
            // Remove the in_doubt flag in buffer pool of the page if it exists in buffer pool
            if(new_chkpt.buf_tab.count(page_of_interest))
            {
                new_chkpt.buf_tab.erase(page_of_interest);

                in_doubt_count--;

                // CHECK THIS LATER:
                // Page cb is in buffer pool, clear the 'in_doubt' and 'used' flags
                // If the cb for this page does not exist in buffer pool, no-op
                //if (true == smlevel_0::bf->is_in_doubt(idx))
                //{
                //    if (r.type() == logrec_t::t_alloc_a_page)
                //        smlevel_0::bf->clear_in_doubt(idx, true, key);    // Page is still used
                //    else
                //        smlevel_0::bf->clear_in_doubt(idx, false, key);   // Page is not used
                //    w_assert1(0 < in_doubt_count);
                //    --in_doubt_count;
                //}
            }
        }
        else
        {
            // Register the page cb in buffer pool (if not exist) and mark the in_doubt flag
            if (0 == page_of_interest.page)
                W_FATAL_MSG(fcINTERNAL, << "Page # = 0 from a page in log record, log type = " << r.type());

            if(new_chkpt.buf_tab.count(page_of_interest)) {
                if(new_chkpt.buf_tab[page_of_interest].rec_lsn > lsn) {
                    new_chkpt.buf_tab[page_of_interest].rec_lsn = lsn;
                }

                if(new_chkpt.buf_tab[page_of_interest].page_lsn < lsn) {
                    new_chkpt.buf_tab[page_of_interest].page_lsn = lsn;
                    new_chkpt.buf_tab[page_of_interest].dirty = true;
                }

                new_chkpt.buf_tab[page_of_interest].store = r.snum();
            }
            else {
                new_chkpt.buf_tab[page_of_interest].store = r.snum();
                new_chkpt.buf_tab[page_of_interest].rec_lsn = lsn;
                new_chkpt.buf_tab[page_of_interest].page_lsn = lsn;
                new_chkpt.buf_tab[page_of_interest].dirty = true;
            }
        }
    }
    else if (r.is_cpsn())
    {
        // If compensation record (t_compensate) should be REDO only,
        // no UNDO and skipped in the UNDO phase.

        // Update undo_nxt lsn of xct
        if(r.is_undo())
        {
            //DBGOUT3(<<"is cpsn, undo " << " undo_nxt<--lsn " << lsn );

            // r is undoable. There is one possible case of
            // this (undoable compensation record)

            // See xct_t::_compensate() for comments regarding
            // undoable compensation record, at one point there was a
            // special case for it, but the usage was eliminated in 1997
            // the author decided to keep the code in case it will be needed again

            W_FATAL_MSG(fcINTERNAL, << "Encounter undoable compensation record in Recovery log");
            new_chkpt.xct_tab[r.tid()].undo_nxt = lsn;
        }
        else
        {
            // Majority of the compensation log should not be undoable.
            // This is a compensation log record in the existing recovery log
            // which came from a user transaction abort operation before
            // system crash.
            // Compensation log record need to be executed in the log scan
            // driven REDO phase, and no-op in transaction UNDO phase.
            // If we encounter a compensation log record, it indicates the
            // current txn has been aborted, set the 'undo_next' to NULL
            // so the txn cannot be rollback in UNDO (should not get there anyway)

            // set undo_nxt to NULL so there is no rollback
            //DBGOUT3(<<"is cpsn, no undo, set undo_next to NULL");
            new_chkpt.xct_tab[r.tid()].undo_nxt = lsn_t::null;
        }

        // Register the page cb in buffer pool (if not exist) and mark the in_doubt flag
        if (r.is_redo())
        {
            if (0 == page_of_interest.page)
                W_FATAL_MSG(fcINTERNAL, << "Page # = 0 from a page in compensation log record");

            if(new_chkpt.buf_tab.count(page_of_interest)) {
                if(new_chkpt.buf_tab[page_of_interest].rec_lsn > lsn) {
                    new_chkpt.buf_tab[page_of_interest].rec_lsn = lsn;
                }

                if(new_chkpt.buf_tab[page_of_interest].page_lsn < lsn) {
                    new_chkpt.buf_tab[page_of_interest].page_lsn = lsn;
                    new_chkpt.buf_tab[page_of_interest].dirty = true;
                }

                new_chkpt.buf_tab[page_of_interest].store = r.snum();
            }
            else {
                new_chkpt.buf_tab[page_of_interest].store = r.snum();
                new_chkpt.buf_tab[page_of_interest].rec_lsn = lsn;
                new_chkpt.buf_tab[page_of_interest].page_lsn = lsn;
                new_chkpt.buf_tab[page_of_interest].dirty = true;
            }
        }
    }
    else if (r.type()!=logrec_t::t_store_operation)   // Store operation (sm)
    {
        // Retrieve a log buffer which we don't know how to handle
        // Raise error
        W_FATAL_MSG(fcINTERNAL, << "Unexpected log record type: " << r.type());
    }
    else  // logrec_t::t_store_operation
    {
        // Store operation, such as create or delete a store, set store parameters, etc.
        // Transaction should not be created for this log because there is no tid
        if(lsn < new_chkpt.min_rec_lsn) {
            new_chkpt.min_rec_lsn = lsn;
        }
    }

    if ((r.tid() != tid_t::null) && (new_chkpt.xct_tab.count(r.tid())))
    {
        // If the log record has an associated txn, update the
        // first (earliest) LSN of the associated txn if the log lsn is
        // smaller than the one recorded in the associated txn
        if (lsn < new_chkpt.xct_tab[r.tid()].first_lsn)
            new_chkpt.xct_tab[r.tid()].first_lsn = lsn;
    }

    return;
}

/*********************************************************************
*
*  chkpt_m::_analysis_process_lock(r, mapCLR, lock_heap, xd)
*
*  Helper function to process lock based on the log record
*  called by analysis_pass_backward only
*
*  System is not opened during Log Analysis phase
*
*********************************************************************/
void chkpt_m::_analysis_process_lock(logrec_t& r,            // In: Current log record
                                     chkpt_t& new_chkpt,
                                     tid_CLR_map& mapCLR)    // In/Out: Map to track undecided in-flight transactions

{
    // This is an undecided in-flight transaction and the log record
    // is a meaningful log record (not checkpoint, transaction end/abort,
    // mount, or system transaction  log records),
    // process lock based on the type of log record

    w_assert1(xct_t::xct_ended != new_chkpt.xct_tab[r.tid()].state);

    if (r.is_page_update())
    {
        // Not compensation log record and it affects buffer pool
        // Is this an undecided transaction?
        // An undecided transaction is an in-flight transaction which
        // contains compensation log records and the transaction did not
        // end when the system crashed
        // Due to backward log scan, if the first log record retrieved for
        // a transaction is not an 'end' or 'abort' log record, then the transaction
        // is either a loser (no compensation) or undecided (with compensation)
        // transaction
        // Express supports save_point (see save_work() and rollback_work()),
        // when we encounter the first log record of an in-flight transaction and if
        // it is not a compensation log record, it still might be an undecided
        // transaction because the transaction might had save_point(s) and also
        // partial roll_back might occurred before the system crash
        //
        // Treat all in-flight transactions as undecided transactions initially by
        // adding all in-flight transactions into compensation map
        // After backward log scan finished, if an entry in the compensation map
        // has more updates than compensations (count > 0), it is a loser transaction.
        // If an entry has same updates and compensations (count == 0), it is a winner.
        // If an entry has more compensations than updates (count < 0), it is possible
        // due to checkpoint, treat it as a loser transaction (although it might be a winner)

        tid_CLR_map::iterator search = mapCLR.find(r.tid().as_int64());
        if(search != mapCLR.end())
        {
            // If tid exists in the map, this is an existing undecided in-flight transaction,
            // in other words, we have seen log records from this in-flight transaction already,
            // need to update the counter
            mapCLR[r.tid().as_int64()] += 1;
        }
        else
        {
            // Does not exist in map, insert the new tid with counter = 1 (first update log record)
            mapCLR.insert(std::make_pair(r.tid().as_int64(), 1));
            // 'emplace' is a better method to use for our purpose, but this function
            // is available for C++11 and after only, while Express is not compiled
            // for C++11 (determined by others early on)
            //     mapCLR.emplace(r.tid().as_int64(), 1);
        }

        // Re-acquire non-read-locks on the log record for all undecided in-flight transactions
        // If the undecided in-flight transaction turns out to be a winner, release all
        // acquired locks at the end of Log Analysis
        // See compensation log record handling for more information on why we are
        // re-acquiring non-read locks for all undecided in-flight transaction log records

        _analysis_acquire_lock_log(r, new_chkpt);

    }

    if (r.is_cpsn())
    {
        // A compensation record from an undecided in-flight transaction,
        // meaning the transaction did not end when system crashed and it
        // was either in the middle of rollback or it had partial rollback due
        // to savepoint operation
        // This transaction requires special handling to determine whether it
        // is a loser (active) or winner (ended) transaction.
        //
        // Ideally we should only re-acquire locks for update log reocrds if it does
        // not have matching compensation log records, the benefits are:
        // 1. Even if the entire transaction rollback operation did not complete,
        //      the associated update operation has been rolled back, therefore REDO
        //      takes care of the on-disk image and no need to lock the associate record
        // 2. Only re-acquire locks when it is needed, in other words, only on update
        //     lock records without matching compensation log records
        // 3. If the undecided transaction turns out to be a winner transaction, we did
        //     not acquired any lock on this transaction and no need to release anything
        //     at the end, simply remove the winner transaction from the transaction table
        //
        // Using the simple counting logic (update = +1, compensation = -1) to determine
        // whether the undecided in-flight transaction is a loser or winner, also whether to
        // re-acquire lock or not:
        //      If count > 0, acquire locks on the update log record
        //      If count <= 0, do not acquire locks because the update log record
        //                           has a matching compensation log record
        //     Example:
        //            Insert A               -- count = 1   -- re-acquire lock
        //            Insert B               -- count = 0
        //            Insert C               -- count = -1
        //            Insert D               -- count = -2
        //            Insert E                -- count = -3
        //            Rollback
        //                 Delete E      -- count = -4
        //                 Delete D     -- count = -3
        //                 Delete C     -- count = -2
        //                 Delete B     -- count = -1
        //            System crash
        // The above logic works well with transaction in the middle of aborting when
        // system crash
        //
        // It does not work correctly when SavePoint and partial rollback are involved,
        // a few examples:
        // 1. Transaction had savepoint rollback, and it was in the middle of rollback
        //       when system crashed, in other words, the first log record retrieved
        //       on this transaction is a compensation log record.
        //       Example:
        //            Insert A               -- count = 2   -- re-acquire lock
        //            SavePoint
        //            Insert B               -- count = 1   -- re-acquire lock but it should not
        //            Insert C              -- count = 0
        //            SavePoint rollback
        //               Delete C           -- count = -1
        //               Delete B           -- count = 0
        //            Insert D               -- count = 1   -- re-acquire lock
        //            Insert E               -- count = 0
        //            Rollback
        //               Delete E           -- count = -1
        //            System crash
        // 2. Transaction had savepoint rollback, and it was in the middle of update
        //      operation when system crashed, in other words, the first log record retrieved
        //      on this transaction is an update log record.
        //      Example:
        //            Insert A                 -- count = 4  -- re-acquire lock
        //            Insert B                  -- count = 3  -- re-acquire lock
        //            SavePoint
        //            Insert C                 -- count = 2  -- re-acquire lock but it should not
        //            Insert D                 -- count = 1  -- re-acquire lock but it should not
        //            SavePoint rollback
        //               Delete D             -- count = 0
        //               Delete C             -- count = 1  -- np re-acquire lock due to compensation
        //            Insert E                  -- count = 2  -- re-acquire lock
        //            SavePoint
        //            Insert F                  -- count = 1  -- re-acquire lock but it should not
        //            Insert G                 -- count = 0
        //            Savepoint rollback
        //               Delete G            -- count = -1
        //               Delete F             -- count = 0
        //            Insert H                 -- count = 1   -- re-acquire lock
        //            System crash
        // These issues could be addressed to achieve the goal of re-acquire lock only if
        // it is necessary, but a more complex logic must be used instead of simple
        // counting logic currently implemented.  For example, we need to distingish
        // between transaction abort rollback and partial rollback, and to identify
        // which update statement to re-acquire locks.
        // Or use a data structure to match the update and compensation pairs.

        // Due to the SavePoint/partial rollback complexity, we are not using the minimum
        // re-acquire lock optimization.  The current implementation re-acquires non-read
        // locks on all update log records from undecided in-flight transactions, if the transaction
        // turns out to be a winner, release the acquired locks and then remove the winner
        // transaction from transaction table, it is not optimal (might have some unnecessary
        // lock acquisitions) but it is simple and correct if SavePoint partial rollback was
        // involved in undecided in-flight transaction
        // Also it is an extreme corner case to encounter the situation that a transaction was
        // at the end of rolling back when the system crashed and the transaction managed
        // to be a winner afterall.

        // TODO(Restart)...
        //      The current counting solution is relying on the assumption that we
        //      must have the same number of original and compensation log records
        //      it does not know how far the rollback went when the system crash.
        //
        //      An alternative implementation for this issue:
        //            1. If a compensation log record, then 'r.undo_nxt()' contains the
        //                    lsn of the original update log record, which allows us to identify
        //                    the pair of original and compensation log records
        //            2. Build a data structure to maintain all the pairs.
        //            3. It is a winner transaction if all the pairs are filled, which indicates
        //                    the rollback has completed.

        tid_CLR_map::iterator search = mapCLR.find(r.tid().as_int64());
        if(search != mapCLR.end())
        {
            // Exist in the map already, update the counter by -1 from the current value
            mapCLR[r.tid().as_int64()] -= 1;
        }
        else
        {
            // Does not exist in map, insert the new tid with counter = -1 (first compensation log record)
            mapCLR.insert(std::make_pair(r.tid().as_int64(), -1));
            // 'emplace' is a better method to use for our purpose, but this function
            // is available for C++11 and after only, while Express is not compiled
            // for C++11 (determined by others early on)
            //     mapCLR.emplace(r.tid().as_int64(), -1);
        }
    }
    return;
}

/*********************************************************************
*
*  chkpt_m::_analysis_acquire_lock_log(r, xd, lock_heap)
*
*  Helper function to process lock re-acquisition based on the log record
*  called by analysis_pass_backward only
*
*  System is not opened during Log Analysis phase
*
*********************************************************************/
void chkpt_m::_analysis_acquire_lock_log(logrec_t& r,            // In: log record
                                         chkpt_t& new_chkpt)
{
    // A special function to re-acquire non-read locks based on a log record,
    // when acquiring lock on key, it sets the intent mode on key also,
    // and add acquired lock information to the associated transaction object.

    // Called during Log Analysis phase for backward log scan, the
    // buffer pool pages were not loaded into buffer pool and the system
    // was not opened for user transactions, therefore it is safe to access lock
    // manager during Log Analysis phase, no latch would be held when
    // accessing lock manager to re-acqure non-read locks.
    // It should not encounter lock conflicts during lock re-acquisition, because
    // if any conflicts, pre-crash transaction processing would have found them

    w_assert1(xct_t::xct_ended != new_chkpt.xct_tab[r.tid()].state);  // In-flight transaction
    w_assert1(false == r.is_single_sys_xct());   // Not a system transaction
    w_assert1(false == r.is_multi_page());       // Not a multi-page log record (system transaction)
    w_assert1(false == r.is_cpsn());             // Not a compensation log record
    w_assert1(false == r.null_pid());            // Has a valid pid, affecting buffer pool
    w_assert1(r.is_page_update());               // It is a log recode affecting record data (not read)

    // There are 3 types of intent locks
    // 1. Intent lock on the given volume (intent_vol_lock) -
    // 2. Intent lock on the given store (intent_store_lock) - store wide operation
    //                where need different lock modes for store and volum, for example,
    //                create or destory an index, _get_du_statistics
    // 3. Intent locks on the given store and its volume in the same mode (intent_vol_store_lock) -
    //                this is used in usual operations like create_assoc (open_store/index) and
    //                cursor lookup upon the first access
    // No re-acquisition on the intent locks since no log records were generated for
    // these operations

    // Qualified log types:
    //    logrec_t::t_btree_insert
    //    logrec_t::t_btree_insert_nonghost
    //    logrec_t::t_btree_update
    //    logrec_t::t_btree_overwrite
    //    logrec_t::t_btree_ghost_mark
    //    logrec_t::t_btree_ghost_reserve

    switch (r.type())
    {
        case logrec_t::t_btree_insert:
            {
                // Insert a record which has an existing ghost record with matching key

                btree_insert_t* dp = (btree_insert_t*) r.data();
                w_assert1(false == dp->sys_txn);

                // Get the key
                w_keystr_t key;
                key.construct_from_keystr(dp->data, dp->klen);
                // Lock re-acquisition
                //DBGOUT3(<<"_analysis_acquire_lock_log - acquire X key lock for INSERT, key: " << key);
                okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(), key.get_length_as_keystr());

                lck_tab_entry_t entry;
                entry.lock_mode = mode;
                entry.lock_hash = lid.hash();

                w_assert0(new_chkpt.xct_tab.count(r.tid()) == 1);

                new_chkpt.lck_tab[r.tid()].push_back(entry);
            }
            break;
        case logrec_t::t_btree_insert_nonghost:
            {
                // Insert a new distinct key, the original operation only need to test whether
                // a key range lock exists, the key range lock is not needed for potential
                // rollback operation therefore it is not held for the remainder of the user transaction.
                // Note that in order to acquire a key range lock, we will need to access data page
                // for the neighboring key, but the buffer pool page is not loaded during Log
                // Analysis phase, luckily we do not need key range lock for this scenario in Restart

                // In Restart, only need to re-acquire key lock, not key range lock

                btree_insert_t* dp = (btree_insert_t*) r.data();
                if (true == dp->sys_txn)
                {
                    // The insertion log record was generated by a page rebalance full logging operation
                    // Do not acquire locks on this log record
                }
                else
                {
                    // Get the key
                    w_keystr_t key;
                    key.construct_from_keystr(dp->data, dp->klen);
                    // Lock re-acquisition
                    //DBGOUT3(<<"_analysis_acquire_lock_log - acquire X key lock for NON_GHOST_INSERT, key: " << key);
                    okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                    lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(), key.get_length_as_keystr());

                    lck_tab_entry_t entry;
                    entry.lock_mode = mode;
                    entry.lock_hash = lid.hash();

                    w_assert0(new_chkpt.xct_tab.count(r.tid()) == 1);

                    new_chkpt.lck_tab[r.tid()].push_back(entry);
                }
            }
            break;
        case logrec_t::t_btree_update:
            {
                btree_update_t* dp = (btree_update_t*) r.data();

                // Get the key
                w_keystr_t key;
                key.construct_from_keystr(dp->_data, dp->_klen);
                // Lock re-acquisition
                //DBGOUT3(<<"_analysis_acquire_lock_log - acquire X key lock for UPDATE, key: " << key);
                okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(), key.get_length_as_keystr());

                lck_tab_entry_t entry;
                entry.lock_mode = mode;
                entry.lock_hash = lid.hash();

                w_assert0(new_chkpt.xct_tab.count(r.tid()) == 1);

                new_chkpt.lck_tab[r.tid()].push_back(entry);
            }
            break;
        case logrec_t::t_btree_overwrite:
            {
                btree_overwrite_t* dp = (btree_overwrite_t*) r.data();

                // Get the key
                w_keystr_t key;
                key.construct_from_keystr(dp->_data, dp->_klen);
                // Lock re-acquisition
                //DBGOUT3(<<"_analysis_acquire_lock_log - acquire X key lock for OVERWRITE, key: " << key);
                okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(), key.get_length_as_keystr());

                lck_tab_entry_t entry;
                entry.lock_mode = mode;
                entry.lock_hash = lid.hash();

                w_assert0(new_chkpt.xct_tab.count(r.tid()) == 1);

                new_chkpt.lck_tab[r.tid()].push_back(entry);
            }
            break;
        case logrec_t::t_btree_ghost_mark:
            {
                // Delete operation only turn the valid record into a ghost record, while the system transaction
                // will clean up the ghost after the user transaction commits and releases its locks, therefore
                // only need a lock on the key value, not any key range

                btree_ghost_t* dp = (btree_ghost_t*) r.data();
                if (1 == dp->sys_txn)
                {
                    // The deletion log record was generated by a page rebalance full logging operation
                    // Do not acquire locks on this log record
                }
                else
                {
                    // Get the key
                    for (size_t i = 0; i < dp->cnt; ++i)
                    {
                        // Get the key
                        w_keystr_t key (dp->get_key(i));
                        // Lock re-acquisition
                        //DBGOUT3(<<"_analysis_acquire_lock_log - acquire X key lock for DELETE, key: " << key);
                        okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                        lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(), key.get_length_as_keystr());

                        lck_tab_entry_t entry;
                        entry.lock_mode = mode;
                        entry.lock_hash = lid.hash();

                        w_assert0(new_chkpt.xct_tab.count(r.tid()) == 1);

                        new_chkpt.lck_tab[r.tid()].push_back(entry);
                    }
                }
            }
            break;
        case logrec_t::t_btree_ghost_reserve:
            {
                // This is to insert a new record where the key did not exist as a ghost
                // Similar to logrec_t::t_btree_insert_nonghost

                // In Restart, only need to re-acquire key lock, not key range lock

                btree_ghost_reserve_t* dp = (btree_ghost_reserve_t*) r.data();

                // Get the key
                w_keystr_t key;
                key.construct_from_keystr(dp->data, dp->klen);
                // Lock re-acquisition
                //DBGOUT3(<<"_analysis_acquire_lock_log - acquire X key lock for GHOST_RESERVE(INSERT), key: " << key);
                okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(), key.get_length_as_keystr());

                lck_tab_entry_t entry;
                entry.lock_mode = mode;
                entry.lock_hash = lid.hash();

                w_assert0(new_chkpt.xct_tab.count(r.tid()) == 1);

                new_chkpt.lck_tab[r.tid()].push_back(entry);
            }
            break;
        default:
            {
                if(r.type() != logrec_t::t_page_img_format && r.type() != logrec_t::t_store_operation)
                    W_FATAL_MSG(fcINTERNAL, << "restart_m::_analysis_acquire_lock_log - Unexpected log record type: " << r.type());
            }
            break;
    }

    return;
}

/*********************************************************************
 *
 *  chkpt_m::_analysis_process_compensation_map(mapCLR)
 *
 *  Helper function to process the compensation list for undecided transactions
 *  called by analysis_pass_backward only
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
void chkpt_m::_analysis_process_compensation_map(tid_CLR_map& mapCLR, chkpt_t& new_chkpt)
// In: map to track log record count for all undecided in-flight transaction
{
    // Done with backward log scan, check the compensation list, these are the undecided
    // in-flight transactions when the system crash occurred, in other words, we did not see
    // the 'abort' or 'end' log reocrd, it might contain compensation log records:
    // 1. All update log records have matching compensation log records -
    //                 Transaction abort finished, but system crashed before the 'abort' log record came out
    //                 Mark the transaction as a winner (ended) transaction
    //                 Release all acquired locks on this transaction
    //                 Need to insert a 'transaction abort' log record into recovery log
    //                 This transaction will be removed from txn table in '_analysis_process_txn_table'
    // 2. Existing compensation log records, but not all update log records have matching
    //     compensation log records -
    //                 Transaction abort did not finish when system crashed
    //                 Mark the transaction as a loser (active) transaction
    //                 Keep all the locks acquired when processing the update log records
    // 3. Only update log records, no compensation log record:
    //                 Typical in-flight Transaction when system crashed
    //                 Mark the transaction as a loser (active) transaction
    //                 Keep all the locks acquired when processing the update log records

    if (true == mapCLR.empty())
        return;

    // Loop through all elements in map
    for (tid_CLR_map::iterator it = mapCLR.begin(); it != mapCLR.end(); it++)
    {
        if (0 == it->second)
        {
            // Change the undecided transaction into a winner transaction,
            // release locks and generate an 'abort' log record
            tid_t t(it->first);
            w_assert1(new_chkpt.xct_tab.count(t) == 1);

            // Free all the acquired locks
            //me()->attach_xct(xd);
            //xd->commit_free_locks();
            new_chkpt.lck_tab.erase(t);

            // Generate a new lock record, because we are in the middle of Log Analysis
            // log generation has been turned off, turn it on temperaury
            //_original_value = smlevel_0::logging_enabled;
            //smlevel_0::logging_enabled = true;
            //log_xct_abort();
            //smlevel_0::logging_enabled = _original_value;

            // Done dealing with the transaction
            //me()->detach_xct(xd);

            // Mark the transaction as a winner in transaction table, all winner transactions
            // will be removed from transaction table later
            if (xct_t::xct_ended != new_chkpt.xct_tab[t].state)
                new_chkpt.xct_tab[t].state = xct_t::xct_ended;
        }
        else
        {
            // Two scenarios, both scenarios make the undecided transaction into
            // a loser transaction, keep all the re-acquired non-read locks:
            // 1. More origianl log records than the compensation log records
            // 2. More compensation log records than update log records.  This can
            //     happen only if when the last checkpoint was taken, the transaction was an
            //     in-flight transaction, and then the transaction started rolling back (or did at
            //     lease one save point partial roll back) after the checkpoint, system crashed
            //     before the transaction finished

            tid_t t(it->first);
            w_assert1(new_chkpt.xct_tab.count(t) == 1);
            w_assert1(xct_t::xct_active == new_chkpt.xct_tab[t].state);
        }
    }
    return;
}

void chkpt_m::dcpld_take(chkpt_mode_t chkpt_mode)
{
    if (t_chkpt_async == chkpt_mode) {
        DBGOUT1(<< "Checkpoint request: asynch");
    }
    else {
        DBGOUT1(<< "Checkpoint request: synch");
    }

    if (!ss_m::log) {
        return; // recovery facilities disabled ... do nothing
    }

    /*
     * Checkpoints are fuzzy but must be serialized wrt each other.
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
    DBGOUT1(<<"BEGIN chkpt_m::dcpld_take");

    INC_TSTAT(log_chkpt_cnt);

    // Start the initial validation check for make sure the incoming checkpoint request is valid
    bool valid_chkpt = true;
    {
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
    }

     // Done with the checkpoint validation, should we continue?
    if (!valid_chkpt) {
        // Failed the checkpoint validation, release the 'write' mutex and exist
        chkpt_serial_m::write_release();
        return;
    }

    // Verifies if there is enough space to insert the maximum lenght of a chkpt
    // in the log.
    if(!ss_m::log->verify_chkpt_reservation()) {
        chkpt_serial_m::write_release();
        W_FATAL(eOUTOFLOGSPACE);
    }

    // Allocate a buffer for storing log records
    w_auto_delete_t<logrec_t> logrec(new logrec_t);

#define LOG_INSERT(constructor_call, rlsn)            \
    do {                                              \
        new (logrec) constructor_call;                \
        W_COERCE( ss_m::log->insert(*logrec, rlsn) );       \
        if(!ss_m::log->consume_chkpt_reservation(logrec->length())) { \
            chkpt_serial_m::write_release();                    \
            W_FATAL(eOUTOFLOGSPACE);                            \
        }                                                       \
    } while(0)

    const lsn_t curr_lsn = ss_m::log->curr_lsn();
    lsn_t begin_lsn = lsn_t::null;

    // Insert chkpt_begin log record.
    LOG_INSERT(chkpt_begin_log(lsn_t::null), &begin_lsn);
    W_COERCE(ss_m::log->flush_all() );

    w_assert1(curr_lsn.data() <= begin_lsn.data());

    // Backward scan from begin_lsn to first completed chkpt (may not be master).
    lsn_t master_lsn = ss_m::log->master_lsn();
    chkpt_t new_chkpt;

    //backward_scan_log(master_lsn, begin_lsn, new_chkpt, true);
    forward_scan_log(master_lsn, begin_lsn, new_chkpt, true);

    //============================ WRITE LOG RECORDS ===========================
    DBGOUT1(<<"==================== CHECKPOINT ====================");
    DBGOUT1(<<"new_chkpt.begin_lsn = " << new_chkpt.begin_lsn);
    DBGOUT1(<<"new_chkpt.min_rec_lsn = " << new_chkpt.min_rec_lsn);
    DBGOUT1(<<"new_chkpt.min_xct_lsn = " << new_chkpt.min_xct_lsn);
    DBGOUT1(<<"new_chkpt.next_vid = " << new_chkpt.next_vid);

    uint chunk;

    // Serialize dev_tab
    chunk = vol_m::MAX_VOLS > (int)chkpt_dev_tab_t::max ? (int)chkpt_dev_tab_t::max : vol_m::MAX_VOLS;
    vector<string> dev_paths;
    for(dev_tab_t::iterator it=new_chkpt.dev_tab.begin(); it!=new_chkpt.dev_tab.end(); ++it) {
        DBGOUT1(<<"dev_paths[]=" << it->first);
        dev_paths.push_back(it->first);
        if(dev_paths.size()==chunk || &*it==&*new_chkpt.dev_tab.rbegin()) {
            // We filled a chunk OR we reached the last element, so we write
            // a log record:
            LOG_INSERT(chkpt_dev_tab_log(dev_paths.size(),
                                        new_chkpt.next_vid,
                                        (const string*)(&dev_paths[0])), 0);
            dev_paths.clear();
        }
    }

    // Serialize bkp_tab
    chunk = vol_m::MAX_VOLS > (int)chkpt_backup_tab_t::max ? (int)chkpt_backup_tab_t::max : vol_m::MAX_VOLS;
    vector<vid_t> backup_vids;
    vector<string> backup_paths;
    for(bkp_tab_t::iterator it=new_chkpt.bkp_tab.begin(); it!=new_chkpt.bkp_tab.end(); ++it) {
        DBGOUT1(<<"backup_vids[]="<<it->first<<" , "<<"backup_paths[]="<<it->second.bkp_path);
        backup_vids.push_back(it->first);
        backup_paths.push_back(it->second.bkp_path);
        if(backup_vids.size()==chunk || &*it==&*new_chkpt.bkp_tab.rbegin()) {
            LOG_INSERT(chkpt_backup_tab_log(backup_vids.size(),
                                        (const vid_t*)(&backup_vids[0]),
                                        (const string*)(&backup_paths[0])), 0);
            backup_vids.clear();
            backup_paths.clear();
        }
    }

    //LOG_INSERT(chkpt_restore_tab_log(vol->vid()), 0);

    // Serialize buf_tab
    chunk = chkpt_bf_tab_t::max;
    vector<lpid_t> pid;
    vector<snum_t> store;
    vector<lsn_t> rec_lsn;
    vector<lsn_t> page_lsn;
    for(buf_tab_t::iterator it=new_chkpt.buf_tab.begin(); it!=new_chkpt.buf_tab.end(); ++it) {
        DBGOUT1(<<"pid[]="<<it->first<< " , " <<
                  "store[]="<<it->second.store<< " , " <<
                  "rec_lsn[]="<<it->second.rec_lsn<< " , " <<
                  "page_lsn[]="<<it->second.page_lsn);
        pid.push_back(it->first);
        store.push_back(it->second.store);
        rec_lsn.push_back(it->second.rec_lsn);
        page_lsn.push_back(it->second.page_lsn);
         if(pid.size()==chunk || &*it==&*new_chkpt.buf_tab.rbegin()) {
            LOG_INSERT(chkpt_bf_tab_log(pid.size(), (const lpid_t*)(&pid[0]),
                                                    (const snum_t*)(&store[0]),
                                                    (const lsn_t*)(&rec_lsn[0]),
                                                    (const lsn_t*)(&page_lsn[0])), 0);
            pid.clear();
            store.clear();
            rec_lsn.clear();
            page_lsn.clear();
         }
    }


    chunk = chkpt_xct_lock_t::max;
    for(lck_tab_t::iterator it=new_chkpt.lck_tab.begin(); it!=new_chkpt.lck_tab.end(); ++it) {
        vector<okvl_mode> lock_mode;
        vector<uint32_t> lock_hash;
        DBGOUT1(<<"tid="<<it->first);
        for(list<lck_tab_entry_t>::iterator jt=it->second.begin(); jt!=it->second.end(); ++jt) {
            DBGOUT1(<<"lock_mode[]="<<jt->lock_mode<<" , lock_hash[]="<<jt->lock_hash);
            lock_mode.push_back(jt->lock_mode);
            lock_hash.push_back(jt->lock_hash);
            if(lock_mode.size()==chunk || &*jt==&*it->second.rbegin()) {
                LOG_INSERT(chkpt_xct_lock_log(it->first,
                                      lock_mode.size(),
                                      (const okvl_mode*)(&lock_mode[0]),
                                      (const uint32_t*)(&lock_hash[0])), 0);
                lock_mode.clear();
                lock_hash.clear();
            }
        }
    }

    chunk = chkpt_xct_tab_t::max;
    vector<tid_t> tid;
    vector<smlevel_0::xct_state_t> state;
    vector<lsn_t> last_lsn;
    vector<lsn_t> undo_nxt;
    vector<lsn_t> first_lsn;
    for(xct_tab_t::iterator it=new_chkpt.xct_tab.begin(); it!=new_chkpt.xct_tab.end(); ++it) {
        DBGOUT1(<<"tid[]="<<it->first<<" , " <<
                  "state[]="<<it->second.state<< " , " <<
                  "last_lsn[]="<<it->second.last_lsn<<" , " <<
                  "undo_nxt[]="<<it->second.undo_nxt<<" , " <<
                  "first_lsn[]="<<it->second.first_lsn);
        tid.push_back(it->first);
        state.push_back(it->second.state);
        last_lsn.push_back(it->second.last_lsn);
        undo_nxt.push_back(it->second.undo_nxt);
        first_lsn.push_back(it->second.first_lsn);
        if(tid.size()==chunk || &*it==&*new_chkpt.xct_tab.rbegin()) {
            LOG_INSERT(chkpt_xct_tab_log(new_chkpt.youngest, tid.size(),
                                        (const tid_t*)(&tid[0]),
                                        (const smlevel_0::xct_state_t*)(&state[0]),
                                        (const lsn_t*)(&last_lsn[0]),
                                        (const lsn_t*)(&undo_nxt[0]),
                                        (const lsn_t*)(&first_lsn[0])), 0);
            tid.clear();
            state.clear();
            last_lsn.clear();
            undo_nxt.clear();
            first_lsn.clear();
        }
    }

    // In case the transaction table was empty, we insert a xct_tab_log anyway,
    // because we want to save the youngest tid.
    if(new_chkpt.xct_tab.size() == 0) {
        LOG_INSERT(chkpt_xct_tab_log(new_chkpt.youngest, tid.size(),
                                        (const tid_t*)(&tid[0]),
                                        (const smlevel_0::xct_state_t*)(&state[0]),
                                        (const lsn_t*)(&last_lsn[0]),
                                        (const lsn_t*)(&undo_nxt[0]),
                                        (const lsn_t*)(&first_lsn[0])), 0);
    }
    //==========================================================================

    if (ss_m::shutting_down && !ss_m::shutdown_clean) // Dirty shutdown (simulated crash)
    {
        DBGOUT1(<<"chkpt_m::take ABORTED due to dirty shutdown, "
                << ", dirty page count = " << pid.size()
                << ", total txn count = " << tid.size()
                << ", total backup count = " << backup_vids.size()
                << ", total vol count = " << dev_paths.size());
    }
    else
    {
        LOG_INSERT(chkpt_end_log (new_chkpt.begin_lsn,
                                  new_chkpt.min_rec_lsn,
                                  new_chkpt.min_xct_lsn), 0);

        W_COERCE(ss_m::log->flush_all() );
        DBGOUT1(<<"Setting master_lsn to " << new_chkpt.begin_lsn);
        ss_m::log->set_master(new_chkpt.begin_lsn,
                              new_chkpt.min_rec_lsn,
                              new_chkpt.min_xct_lsn);
    }

    DBGOUT1(<<"Exiting dcpld_take()");

    // Release the 'write' mutex so the next checkpoint request can come in
    chkpt_serial_m::write_release();
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

    // Verifies if there is enough space to insert the maximum lenght of a chkpt
    // in the log.
    if(!ss_m::log->verify_chkpt_reservation()) {
        chkpt_serial_m::write_release();
        W_FATAL(eOUTOFLOGSPACE);
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
                LOG_INSERT(chkpt_backup_tab_log(paths.size(),
                                                (const string*)(&paths[0])), 0);
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
        w_auto_delete_array_t<PageID> pid(new PageID[chunk]);     // page lpid
        w_auto_delete_array_t<StoreID> stores(new StoreID[chunk]);     // page lpid
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
                    << ", min_rec_lsn = " << min_rec_lsn
                    << ", min_xct_lsn = " << min_xct_lsn);

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
        
        if(ss_m::chkpt->decoupled) {
            ss_m::chkpt->dcpld_take(chkpt_m::t_chkpt_async);
        }
        else {
            ss_m::chkpt->take(chkpt_m::t_chkpt_async, dummy_heap);
            w_assert1(0 == dummy_heap.NumElements());
        }
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
