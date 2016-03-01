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

#define LOG_INSERT(constructor_call, rlsn)            \
    do {                                              \
        new (logrec) constructor_call;                \
        W_COERCE( ss_m::log->insert(*logrec, rlsn) );       \
    } while(0)


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
    NORET                chkpt_thread_t(unsigned interval);
    NORET                ~chkpt_thread_t();

    virtual void        run();
    void                retire();
    void                awaken();
    bool                is_retired() {return _retire;}

private:
    bool                _retire;
    unsigned            _interval;
    pthread_mutex_t     _awaken_lock;
    pthread_cond_t      _awaken_cond;

    // disabled
    NORET                chkpt_thread_t(const chkpt_thread_t&);
    chkpt_thread_t&      operator=(const chkpt_thread_t&);
};

chkpt_m::chkpt_m(const sm_options& options)
    : _chkpt_thread(NULL), _chkpt_count(0)
{
    int interval = options.get_int_option("sm_chkpt_interval", -1);
    if (interval >= 0) {
        _chkpt_thread = new chkpt_thread_t(interval);
        W_COERCE(_chkpt_thread->fork());
    }
}

chkpt_m::~chkpt_m()
{
    if (_chkpt_thread)
    {
        _chkpt_thread->retire();
        _chkpt_thread->awaken();
        W_COERCE(_chkpt_thread->join());
        delete _chkpt_thread;
    }
}

void chkpt_m::wakeup_thread()
{
    if (_chkpt_thread) {
        _chkpt_thread->awaken();
    }
}

/*********************************************************************
*
*  chkpt_m::backward_scan_log(lock_heap)
*
*  Scans the log backwards, starting from _lsn until the t_chkpt_begin log record
*  corresponding to the latest completed checkpoint.
*
*********************************************************************/
void chkpt_t::scan_log()
{
    init();

    lsn_t scan_start = smlevel_0::log->durable_lsn();
    if (scan_start == lsn_t(1,0)) { return; }

    log_i scan(*smlevel_0::log, scan_start, false); // false == backward scan
    logrec_t r;
    lsn_t lsn;   // LSN of the retrieved log record

    bool insideChkpt = false;
    bool scan_done = false;

    while (scan.xct_next(lsn, r) && !scan_done)
    {
        if (r.is_skip() || r.type() == logrec_t::t_comment) {
            continue;
        }

        if (!r.tid().is_null()) {
            if (r.tid() > get_highest_tid()) {
                set_highest_tid(r.tid());
            }

            if (r.is_page_update() || r.is_cpsn()) {
                mark_xct_active(r.tid(), lsn, lsn);

                if (is_xct_active(r.tid())) {
                    if (!r.is_cpsn()) { acquire_lock(r); }
                }
                else if (r.xid_prev().is_null()) {
                    // We won't see this xct again -- delete it
                    delete_xct(r.tid());
                }
            }
        }

        if (r.is_page_update()) {
            w_assert0(r.is_redo());
            mark_page_dirty(r.pid(), lsn, lsn, r.stid());

            if (r.is_multi_page()) {
                w_assert0(r.pid2() != 0);
                mark_page_dirty(r.pid2(), lsn, lsn, r.stid());
            }
        }

        switch (r.type())
        {
            case logrec_t::t_chkpt_begin:
                if (insideChkpt) {
                    // Signal to stop backward log scan loop now
                    begin_lsn = lsn;
                    scan_done = true;
                }
                break;

            case logrec_t::t_chkpt_bf_tab:
                if (insideChkpt) {
                    const chkpt_bf_tab_t* dp = (chkpt_bf_tab_t*) r.data();
                    for (uint i = 0; i < dp->count; i++) {
                        mark_page_dirty(dp->brec[i].pid, dp->brec[i].page_lsn,
                                dp->brec[i].rec_lsn, dp->brec[i].store);
                    }
                }
                break;


            case logrec_t::t_chkpt_xct_lock:
                if (insideChkpt) {
                    const chkpt_xct_lock_t* dp = (chkpt_xct_lock_t*) r.data();
                    if (is_xct_active(dp->tid)) {
                        for (uint i = 0; i < dp->count; i++) {
                            add_lock(dp->tid, dp->xrec[i].lock_mode,
                                    dp->xrec[i].lock_hash);
                        }
                    }
                }
                break;

            case logrec_t::t_chkpt_xct_tab:
                if (insideChkpt) {
                    const chkpt_xct_tab_t* dp = (chkpt_xct_tab_t*) r.data();
                    for (size_t i = 0; i < dp->count; ++i) {
                        tid_t tid = dp->xrec[i].tid;
                        w_assert1(!tid.is_null());
                        mark_xct_active(tid, dp->xrec[i].first_lsn,
                                dp->xrec[i].last_lsn);
                    }
                }
                break;


            case logrec_t::t_chkpt_end:
                // checkpoints should not run concurrently
                w_assert0(!insideChkpt);
                insideChkpt = true;
                break;

            // CS TODO: why do we need this? Isn't it related to 2PC?
            // case logrec_t::t_xct_freeing_space:
            case logrec_t::t_xct_end:
            case logrec_t::t_xct_abort:
                mark_xct_ended(r.tid());
                break;

            case logrec_t::t_xct_end_group:
                {
                    // CS TODO: is this type of group commit still used?
                    w_assert0(false);
                    const xct_list_t* list = (xct_list_t*) r.data();
                    uint listlen = list->count;
                    for(uint i=0; i<listlen; i++) {
                        tid_t tid = list->xrec[i].tid;
                        mark_xct_ended(tid);
                    }
                }
                break;

            case logrec_t::t_page_write:
                {
                    PageID pid = *((PageID*) r.data());
                    uint32_t count = *((uint32_t*) (r.data() + sizeof(PageID)));
                    PageID end = pid + count;

                    while (pid < end) {
                        mark_page_clean(pid, lsn);
                        pid++;
                    }
                }
                break;

            case logrec_t::t_add_backup:
                {
                    const char* dev = (const char*)(r.data_ssx());
                    add_backup(dev);
                }
                break;

            case logrec_t::t_chkpt_backup_tab:
                if (insideChkpt) {
                    // CS TODO
                }
                break;

            case logrec_t::t_restore_begin:
            case logrec_t::t_restore_end:
            case logrec_t::t_restore_segment:
            case logrec_t::t_chkpt_restore_tab:
                // CS TODO - IMPLEMENT!
                break;

            default:
                break;

        } //switch
    } //while

    w_assert0(lsn == lsn_t(1,0) || scan_done);
    w_assert0(lsn == lsn_t(1,0) || !begin_lsn.is_null());

    cleanup();
}

void chkpt_t::init()
{
    begin_lsn = lsn_t::null;
    highest_tid = tid_t::null;
    buf_tab.clear();
    xct_tab.clear();
    bkp_path.clear();
}

void chkpt_t::mark_page_dirty(PageID pid, lsn_t page_lsn, lsn_t rec_lsn,
        StoreID store)
{
    // operator[] adds an empty dirty entry if key is not found
    buf_tab_entry_t& e = buf_tab[pid];
    if (e.resolved) { return; }
    if (page_lsn > e.page_lsn) { e.page_lsn = page_lsn; }
    if (rec_lsn < e.rec_lsn) { e.rec_lsn = rec_lsn; }
    // CS TODO: why do we need the store?
    e.store = store;
}

void chkpt_t::mark_page_clean(PageID pid, lsn_t lsn)
{
    // If pid is already on table, it must remain as dirty.
    // But resolved is set anyway, to stop rec and page lsn from being updated
    // further in mark_page_dirty
    buf_tab_t::iterator it = buf_tab.find(pid);
    if (it != buf_tab.end()) {
        it->second.resolved = true;
    }
    else {
        buf_tab_entry_t e;
        e.dirty = false;
        e.resolved = true;
        buf_tab[pid] = e;
    }
}

bool chkpt_t::is_xct_active(tid_t tid) const
{
    xct_tab_t::const_iterator iter = xct_tab.find(tid);
    if (iter == xct_tab.end()) {
        return false;
    }
    return iter->second.state;
}

void chkpt_t::mark_xct_active(tid_t tid, lsn_t first_lsn, lsn_t last_lsn)
{
    // operator[] adds an empty active entry if key is not found
    xct_tab_entry_t& e = xct_tab[tid];
    if (last_lsn > e.last_lsn) { e.last_lsn = last_lsn; }
    if (first_lsn < e.first_lsn) { e.first_lsn = first_lsn; }
}

void chkpt_t::mark_xct_ended(tid_t tid)
{
    xct_tab[tid].state = xct_t::xct_ended;
}

void chkpt_t::delete_xct(tid_t tid)
{
    xct_tab.erase(tid);
}

void chkpt_t::add_backup(const char* path)
{
    bkp_path = path;
}

void chkpt_t::add_lock(tid_t tid, okvl_mode mode, uint32_t hash)
{
    if (!is_xct_active(tid)) { return; }
    lock_info_t entry;
    entry.lock_mode = mode;
    entry.lock_hash = hash;
    xct_tab[tid].locks.push_back(entry);
}

void chkpt_t::cleanup()
{
    // Remove non-dirty pages
    for(buf_tab_t::iterator it  = buf_tab.begin();
                            it != buf_tab.end(); ) {
        if(it->second.dirty == false) {
            it = buf_tab.erase(it);
        }
        else {
            ++it;
        }
    }

    // Remove finished transactions.
    for(xct_tab_t::iterator it  = xct_tab.begin();
                            it != xct_tab.end(); ) {
        if(it->second.state == xct_t::xct_ended) {
            it = xct_tab.erase(it);
        }
        else {
            ++it;
        }
    }
}

lsn_t chkpt_t::get_min_xct_lsn() const
{
    lsn_t min_xct_lsn = lsn_t::max;
    for(xct_tab_t::const_iterator it = xct_tab.begin();
            it != xct_tab.end(); ++it)
    {
        if(it->second.state != xct_t::xct_ended
                && min_xct_lsn > it->second.first_lsn)
        {
            min_xct_lsn = it->second.first_lsn;
        }
    }
    if (min_xct_lsn == lsn_t::max) { return lsn_t::null; }
    return min_xct_lsn;
}

lsn_t chkpt_t::get_min_rec_lsn() const
{
    lsn_t min_rec_lsn = lsn_t::max;
    for(buf_tab_t::const_iterator it = buf_tab.begin();
            it != buf_tab.end(); ++it)
    {
        if(it->second.dirty && min_rec_lsn > it->second.rec_lsn) {
            min_rec_lsn = it->second.rec_lsn;
        }
    }
    if (min_rec_lsn == lsn_t::max) { return lsn_t::null; }
    return min_rec_lsn;
}

void chkpt_t::serialize()
{
    // Allocate a buffer for storing log records
    logrec_t* logrec = new logrec_t;

    size_t chunk;

    // Serialize bkp_tab -- CS TODO
    if (!bkp_path.empty()) {
        vector<string> backup_paths;
        backup_paths.push_back(bkp_path);
        LOG_INSERT(chkpt_backup_tab_log(backup_paths.size(),
                    (const string*)(&backup_paths[0])), 0);
    }

    //LOG_INSERT(chkpt_restore_tab_log(vol->vid()), 0);

    // Serialize buf_tab
    chunk = chkpt_bf_tab_t::max;
    vector<PageID> pid;
    vector<StoreID> store;
    vector<lsn_t> rec_lsn;
    vector<lsn_t> page_lsn;
    for(buf_tab_t::const_iterator it = buf_tab.begin();
            it != buf_tab.end(); ++it)
    {
        DBGOUT1(<<"pid[]="<<it->first<< " , " <<
                  "store[]="<<it->second.store<< " , " <<
                  "rec_lsn[]="<<it->second.rec_lsn<< " , " <<
                  "page_lsn[]="<<it->second.page_lsn);
        pid.push_back(it->first);
        store.push_back(it->second.store);
        rec_lsn.push_back(it->second.rec_lsn);
        page_lsn.push_back(it->second.page_lsn);
         if(pid.size()==chunk || &*it==&*buf_tab.rbegin()) {
            LOG_INSERT(chkpt_bf_tab_log(pid.size(), (const PageID*)(&pid[0]),
                                                    (const StoreID*)(&store[0]),
                                                    (const lsn_t*)(&rec_lsn[0]),
                                                    (const lsn_t*)(&page_lsn[0])), 0);
            pid.clear();
            store.clear();
            rec_lsn.clear();
            page_lsn.clear();
         }
    }

    chunk = chkpt_xct_tab_t::max;
    vector<tid_t> tid;
    vector<smlevel_0::xct_state_t> state;
    vector<lsn_t> last_lsn;
    vector<lsn_t> first_lsn;
    vector<okvl_mode> lock_mode;
    vector<uint32_t> lock_hash;
    for(xct_tab_t::const_iterator it=xct_tab.begin();
            it != xct_tab.end(); ++it) {
        DBGOUT1(<<"tid[]="<<it->first<<" , " <<
                  "state[]="<<it->second.state<< " , " <<
                  "last_lsn[]="<<it->second.last_lsn<<" , " <<
                  "first_lsn[]="<<it->second.first_lsn);

        tid.push_back(it->first);
        state.push_back(it->second.state);
        last_lsn.push_back(it->second.last_lsn);
        first_lsn.push_back(it->second.first_lsn);
        if(tid.size()==chunk || &*it==&*xct_tab.rbegin()) {
            LOG_INSERT(chkpt_xct_tab_log(get_highest_tid(), tid.size(),
                                        (const tid_t*)(&tid[0]),
                                        (const smlevel_0::xct_state_t*)(&state[0]),
                                        (const lsn_t*)(&last_lsn[0]),
                                        (const lsn_t*)(&first_lsn[0])), 0);
            tid.clear();
            state.clear();
            last_lsn.clear();
            first_lsn.clear();
        }

        // gather lock table
        for(vector<lock_info_t>::const_iterator jt = it->second.locks.begin();
                jt != it->second.locks.end(); ++jt)
        {
            DBGOUT1(<<"    lock_mode[]="<<jt->lock_mode<<" , lock_hash[]="<<jt->lock_hash);
            lock_mode.push_back(jt->lock_mode);
            lock_hash.push_back(jt->lock_hash);
            if(lock_mode.size() == chunk) {
                LOG_INSERT(chkpt_xct_lock_log(it->first,
                                      lock_mode.size(),
                                      (const okvl_mode*)(&lock_mode[0]),
                                      (const uint32_t*)(&lock_hash[0])), 0);
                lock_mode.clear();
                lock_hash.clear();
            }
        }
        if(lock_mode.size() > 0) {
            LOG_INSERT(chkpt_xct_lock_log(it->first,
                        lock_mode.size(),
                        (const okvl_mode*)(&lock_mode[0]),
                        (const uint32_t*)(&lock_hash[0])), 0);
            lock_mode.clear();
            lock_hash.clear();
        }
    }

    // In case the transaction table was empty, we insert a xct_tab_log anyway,
    // because we want to save the highest tid.
    if(xct_tab.size() == 0) {
        LOG_INSERT(chkpt_xct_tab_log(get_highest_tid(), tid.size(),
                                        (const tid_t*)(&tid[0]),
                                        (const smlevel_0::xct_state_t*)(&state[0]),
                                        (const lsn_t*)(&last_lsn[0]),
                                        (const lsn_t*)(&first_lsn[0])), 0);
    }

    delete logrec;
}

void chkpt_t::acquire_lock(logrec_t& r)
{
    w_assert1(is_xct_active(r.tid()));
    w_assert1(!r.is_single_sys_xct());
    w_assert1(!r.is_multi_page());
    w_assert1(!r.is_cpsn());
    w_assert1(r.is_page_update());

    switch (r.type())
    {
        case logrec_t::t_btree_insert:
        case logrec_t::t_btree_insert_nonghost:
            {
                btree_insert_t* dp = (btree_insert_t*) r.data();

                w_keystr_t key;
                key.construct_from_keystr(dp->data, dp->klen);

                okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(),
                        key.get_length_as_keystr());

                add_lock(r.tid(), mode, lid.hash());
            }
            break;
        case logrec_t::t_btree_update:
            {
                btree_update_t* dp = (btree_update_t*) r.data();

                w_keystr_t key;
                key.construct_from_keystr(dp->_data, dp->_klen);

                okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(),
                        key.get_length_as_keystr());

                add_lock(r.tid(), mode, lid.hash());
            }
            break;
        case logrec_t::t_btree_overwrite:
            {
                btree_overwrite_t* dp = (btree_overwrite_t*) r.data();

                w_keystr_t key;
                key.construct_from_keystr(dp->_data, dp->_klen);

                okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(),
                        key.get_length_as_keystr());

                add_lock(r.tid(), mode, lid.hash());
            }
            break;
        case logrec_t::t_btree_ghost_mark:
            {
                btree_ghost_t* dp = (btree_ghost_t*) r.data();
                for (size_t i = 0; i < dp->cnt; ++i) {
                    w_keystr_t key (dp->get_key(i));

                    okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                    lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(),
                            key.get_length_as_keystr());

                    add_lock(r.tid(), mode, lid.hash());
                }
            }
            break;
        case logrec_t::t_btree_ghost_reserve:
            {
                btree_ghost_reserve_t* dp = (btree_ghost_reserve_t*) r.data();

                w_keystr_t key;
                key.construct_from_keystr(dp->data, dp->klen);

                okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(),
                        key.get_length_as_keystr());

                add_lock(r.tid(), mode, lid.hash());
            }
            break;
        default:
            w_assert0(r.type() == logrec_t::t_page_img_format);
            break;
    }

    return;
}

void chkpt_t::dump(ostream& os)
{
    //Re-create transactions
    os << "ACTIVE TRANSACTIONS" << endl;
    for(xct_tab_t::const_iterator it = xct_tab.begin();
                            it != xct_tab.end(); ++it)
    {
        os << it->first << " first_lsn=" << it->second.first_lsn
            << " last_lsn=" << it->second.last_lsn
            << " locks=" << it->second.locks.size()
            << endl;
    }

    // os << "DIRTY PAGES" << endl;
    // for(buf_tab_t::const_iterator it = buf_tab.begin();
    //                         it != buf_tab.end(); ++it)
    // {
    //     os << it->first << "(" << it->second.rec_lsn
    //         << "-" << it->second.page_lsn << ") ";
    // }
    // os << endl;
}

void chkpt_m::take()
{
    chkpt_mutex.acquire_write();
    DBGOUT1(<<"BEGIN chkpt_m::take");

    INC_TSTAT(log_chkpt_cnt);

    // Insert chkpt_begin log record.
    logrec_t* logrec = new logrec_t;
    lsn_t begin_lsn = lsn_t::null;
    LOG_INSERT(chkpt_begin_log(lsn_t::null), &begin_lsn);
    W_COERCE(ss_m::log->flush_all());

    curr_chkpt.scan_log();
    curr_chkpt.serialize();

    // Insert chkpt_end log record
    LOG_INSERT(chkpt_end_log (curr_chkpt.get_begin_lsn(),
                curr_chkpt.get_min_rec_lsn(),
                curr_chkpt.get_min_xct_lsn()), 0);

    // Release the 'write' mutex so the next checkpoint request can come in
    chkpt_mutex.release_write();

    W_COERCE(ss_m::log->flush_all());

    delete logrec;
}

lsn_t chkpt_m::get_curr_rec_lsn()
{
    chkpt_mutex.acquire_read();
    lsn_t ret = curr_chkpt.get_min_rec_lsn();
    chkpt_mutex.release_read();
    return ret;
}

chkpt_thread_t::chkpt_thread_t(unsigned interval)
    : smthread_t(t_time_critical, "chkpt", WAIT_NOT_USED),
    _retire(false), _interval(interval)
{
    DO_PTHREAD(pthread_mutex_init(&_awaken_lock, NULL));
    DO_PTHREAD(pthread_cond_init(&_awaken_cond, NULL));
}

chkpt_thread_t::~chkpt_thread_t()
{
}

void
chkpt_thread_t::run()
{
    // Thread waits for an awake signal or for the interval timeout;
    // whichever comes first
    while(! _retire)
    {
        w_assert1(ss_m::chkpt);
        DO_PTHREAD(pthread_mutex_lock(&_awaken_lock));

        struct timespec timeout;
        sthread_t::timeout_to_timespec(_interval * 1000, timeout); // in ms
        int code = pthread_cond_timedwait(&_awaken_cond, &_awaken_lock, &timeout);
        DO_PTHREAD_TIMED(code);

        lintel::atomic_thread_fence(lintel::memory_order_acquire);
        if(_retire) break;

        ss_m::chkpt->take();

        // CS: see comment on awaken()
        DO_PTHREAD(pthread_mutex_unlock(&_awaken_lock));
    }
}

void
chkpt_thread_t::retire()
{
    _retire = true;
    lintel::atomic_thread_fence(lintel::memory_order_release);
}

void
chkpt_thread_t::awaken()
{
    // Signal may well be lost, which means checkpoint is already running.
    // If an unlucky sequence of events causes the signal to be missed while
    // the checkpoint thread is not running, it should not be a problem since
    // the caller who is waiting on a checkpoint (e.g.,
    // log_storage::get_partition_for_flush) should keep retrying in a loop.
    // Therefore, there's no need for acquiring the mutex here.
    // Since the chkpt thread runs in an interval, there's also no need to
    // use some kind of condition variable like _wakeup_received. We might want
    // to revisit this later and support a chkpt thread that only runs when
    // recieving a signal (e.g., if _interval < 0).
    DO_PTHREAD(pthread_cond_signal(&_awaken_cond));
}
