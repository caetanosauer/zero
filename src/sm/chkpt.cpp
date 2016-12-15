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

#include <fstream>
#include <algorithm>
#include <new>

#define SM_SOURCE
#define CHKPT_C

#include "sm_base.h"
#include "chkpt.h"
#include "btree_logrec.h"       // Lock re-acquisition
#include "bf_tree.h"
#include "sm.h"
#include "lock_raw.h"      // Lock information gathering
#include "w_okvl_inl.h"    // Lock information gathering
struct RawLock;            // Lock information gathering
#include "restart.h"
#include "vol.h"
#include "worker_thread.h"
#include "stopwatch.h"
#include "logrec_support.h"
#include "xct_logger.h"


class chkpt_thread_t : public worker_thread_t
{
public:
    chkpt_thread_t(int interval)
        : worker_thread_t(interval)
    {}

    virtual void do_work()
    {
        if (ss_m::chkpt) {
            ss_m::chkpt->take();
            ss_m::log->get_storage()->wakeup_recycler(true /* chkpt_only */);
        }
    }
};

chkpt_m::chkpt_m(const sm_options& options, lsn_t last_chkpt_lsn)
    : _chkpt_thread(NULL), _chkpt_count(0), _min_rec_lsn(0), _min_xct_lsn(0),
    _last_end_lsn(last_chkpt_lsn)
{
    if (_last_end_lsn.is_null()) { _last_end_lsn = lsn_t(1, 0); }
    int interval = options.get_int_option("sm_chkpt_interval", -1);
    if (interval >= 0) {
        _chkpt_thread = new chkpt_thread_t(interval);
        W_COERCE(_chkpt_thread->fork());
    }

    _no_db_mode = options.get_bool_option("sm_no_db", false);
}

chkpt_m::~chkpt_m()
{
    retire_thread();
}

void chkpt_m::retire_thread()
{
    if (_chkpt_thread) {
        _chkpt_thread->stop();
        delete _chkpt_thread;
        _chkpt_thread = nullptr;
    }
}

void chkpt_m::wakeup_thread()
{
    // No chkpts in no-db mode -- see comments in take() below
    if (_no_db_mode) { return; }

    if (!_chkpt_thread) {
        _chkpt_thread = new chkpt_thread_t(-1);
        W_COERCE(_chkpt_thread->fork());
    }
    _chkpt_thread->wakeup();
}

/*********************************************************************
*
*  chkpt_m::backward_scan_log(lock_heap)
*
*  Scans the log backwards, starting from _lsn until the t_chkpt_begin log record
*  corresponding to the latest completed checkpoint.
*
*********************************************************************/
void chkpt_t::scan_log(lsn_t scan_start, bool no_db_mode)
{
    init();

    if (scan_start.is_null()) {
        scan_start = smlevel_0::log->durable_lsn();
    }
    w_assert1(scan_start <= smlevel_0::log->durable_lsn());
    if (scan_start == lsn_t(1,0)) { return; }

    log_i scan(*smlevel_0::log, scan_start, false); // false == backward scan
    logrec_t r;
    lsn_t lsn = lsn_t::max;   // LSN of the retrieved log record

    // Set when scan finds begin of previous checkpoint
    lsn_t scan_stop = lsn_t(1,0);

    // CS TODO: not needed with file serialization
    // bool insideChkpt = false;
    while (lsn > scan_stop && scan.xct_next(lsn, r))
    {
        if (r.is_skip() || r.type() == logrec_t::t_comment) {
            continue;
        }

        if (!r.tid() == 0) {
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

        analyze_logrec(r, scan_stop, no_db_mode);

        // CS: A CLR is not considered a page update for some reason...
        if (r.is_redo()) {
            mark_page_dirty(r.pid(), lsn, lsn);

            if (r.is_multi_page()) {
                w_assert0(r.pid2() != 0);
                mark_page_dirty(r.pid2(), lsn, lsn);
            }
        }
    }

    w_assert0(no_db_mode || lsn == scan_stop);
    last_scan_start = scan_start;

    cleanup();
}

void chkpt_t::analyze_logrec(logrec_t& r, lsn_t& scan_stop, bool no_db_mode)
{
    auto lsn = r.lsn();

    switch (r.type())
    {
        case logrec_t::t_chkpt_begin:
            // CS TODO: not needed with file serialization
            // if (insideChkpt) {
            //     // Signal to stop backward log scan loop now
            //     scan_stop = lsn;
            // }
            {
                // In no-db mode, ignore chkpt files and always scan whole log
                if (no_db_mode) { break; }

                fs::path fpath = smlevel_0::log->get_storage()->make_chkpt_path(lsn);
                if (fs::exists(fpath)) {
                    ifstream ifs(fpath.string(), ios::binary);
                    deserialize_binary(ifs);
                    ifs.close();
                    scan_stop = lsn;
                }
            }

            break;

        case logrec_t::t_chkpt_bf_tab:
            // CS TODO: not needed with file serialization
            // if (insideChkpt) {
            //     const chkpt_bf_tab_t* dp = (chkpt_bf_tab_t*) r.data();
            //     for (uint i = 0; i < dp->count; i++) {
            //         mark_page_dirty(dp->brec[i].pid, dp->brec[i].page_lsn,
            //                 dp->brec[i].rec_lsn);
            //     }
            // }
            break;


        case logrec_t::t_chkpt_xct_lock:
            // CS TODO: not needed with file serialization
            // if (insideChkpt) {
            //     const chkpt_xct_lock_t* dp = (chkpt_xct_lock_t*) r.data();
            //     if (is_xct_active(dp->tid)) {
            //         for (uint i = 0; i < dp->count; i++) {
            //             add_lock(dp->tid, dp->xrec[i].lock_mode,
            //                     dp->xrec[i].lock_hash);
            //         }
            //     }
            // }
            break;

        case logrec_t::t_chkpt_xct_tab:
            // CS TODO: not needed with file serialization
            // if (insideChkpt) {
            //     const chkpt_xct_tab_t* dp = (chkpt_xct_tab_t*) r.data();
            //     for (size_t i = 0; i < dp->count; ++i) {
            //         tid_t tid = dp->xrec[i].tid;
            //         w_assert1(!tid.is_null());
            //         mark_xct_active(tid, dp->xrec[i].first_lsn,
            //                 dp->xrec[i].last_lsn);
            //     }
            // }
            break;


            // CS TODO: not needed with file serialization
            // case logrec_t::t_chkpt_end:
            // checkpoints should not run concurrently
            // w_assert0(!insideChkpt);
            // insideChkpt = true;
            break;

        case logrec_t::t_xct_end:
        case logrec_t::t_xct_abort:
            mark_xct_ended(r.tid());
            break;

        case logrec_t::t_page_write:
            {
                char* pos = r.data();

                PageID pid = *((PageID*) pos);
                pos += sizeof(PageID);

                lsn_t clean_lsn = *((lsn_t*) pos);
                pos += sizeof(lsn_t);

                uint32_t count = *((uint32_t*) pos);
                PageID end = pid + count;

                while (pid < end) {
                    mark_page_clean(pid, clean_lsn);
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
            // CS TODO
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
}

void chkpt_t::init()
{
    highest_tid = 0;
    last_scan_start = lsn_t::null;
    buf_tab.clear();
    xct_tab.clear();
    bkp_path.clear();
}

void chkpt_t::mark_page_dirty(PageID pid, lsn_t page_lsn, lsn_t rec_lsn)
{
    buf_tab_entry_t& e = buf_tab[pid];
    if (page_lsn > e.page_lsn) { e.page_lsn = page_lsn; }
    if (rec_lsn >= e.clean_lsn && rec_lsn < e.rec_lsn) { e.rec_lsn = rec_lsn; }
}

void chkpt_t::mark_page_clean(PageID pid, lsn_t lsn)
{
    buf_tab_entry_t& e = buf_tab[pid];
    if (lsn > e.clean_lsn) {
        e.clean_lsn = lsn;
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
        if(!it->second.is_dirty()) {
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
        if(it->second.is_dirty() && min_rec_lsn > it->second.rec_lsn) {
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
        Logger::log_sys<chkpt_backup_tab_log>(backup_paths.size(),
                    (const string*)(&backup_paths[0]));
    }

    //LOG_INSERT(chkpt_restore_tab_log(vol->vid()), 0);

    // Serialize buf_tab
    chunk = chkpt_bf_tab_t::max;
    vector<PageID> pid;
    vector<lsn_t> rec_lsn;
    vector<lsn_t> page_lsn;
    for(buf_tab_t::const_iterator it = buf_tab.begin();
            it != buf_tab.end(); ++it)
    {
        DBGOUT1(<<"pid[]="<<it->first<< " , " <<
                  "rec_lsn[]="<<it->second.rec_lsn<< " , " <<
                  "page_lsn[]="<<it->second.page_lsn);
        pid.push_back(it->first);
        rec_lsn.push_back(it->second.rec_lsn);
        page_lsn.push_back(it->second.page_lsn);
         if(pid.size()==chunk || &*it==&*buf_tab.rbegin()) {
             Logger::log_sys<chkpt_bf_tab_log>(pid.size(), (const PageID*)(&pid[0]),
                                                    (const lsn_t*)(&rec_lsn[0]),
                                                    (const lsn_t*)(&page_lsn[0]));
            pid.clear();
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
            Logger::log_sys<chkpt_xct_tab_log>(get_highest_tid(), tid.size(),
                                        (const tid_t*)(&tid[0]),
                                        (const smlevel_0::xct_state_t*)(&state[0]),
                                        (const lsn_t*)(&last_lsn[0]),
                                        (const lsn_t*)(&first_lsn[0]));
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
                Logger::log_sys<chkpt_xct_lock_log>(it->first,
                                      lock_mode.size(),
                                      (const okvl_mode*)(&lock_mode[0]),
                                      (const uint32_t*)(&lock_hash[0]));
                lock_mode.clear();
                lock_hash.clear();
            }
        }
        if(lock_mode.size() > 0) {
            Logger::log_sys<chkpt_xct_lock_log>(it->first,
                        lock_mode.size(),
                        (const okvl_mode*)(&lock_mode[0]),
                        (const uint32_t*)(&lock_hash[0]));
            lock_mode.clear();
            lock_hash.clear();
        }
    }

    // In case the transaction table was empty, we insert a xct_tab_log anyway,
    // because we want to save the highest tid.
    if(xct_tab.size() == 0) {
        Logger::log_sys<chkpt_xct_tab_log>(get_highest_tid(), tid.size(),
                                        (const tid_t*)(&tid[0]),
                                        (const smlevel_0::xct_state_t*)(&state[0]),
                                        (const lsn_t*)(&last_lsn[0]),
                                        (const lsn_t*)(&first_lsn[0]));
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
                btree_ghost_t<btree_page_h*>* dp = (btree_ghost_t<btree_page_h*>*) r.data();
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

    os << "DIRTY PAGES" << endl;
    for(buf_tab_t::const_iterator it = buf_tab.begin();
                            it != buf_tab.end(); ++it)
    {
        os << it->first << "(" << it->second.rec_lsn
            << "-" << it->second.page_lsn << ") " << endl;
    }
    os << endl;
}

void chkpt_m::take()
{
    chkpt_mutex.acquire_write();
    DBGOUT1(<<"BEGIN chkpt_m::take");

    INC_TSTAT(log_chkpt_cnt);

    // Insert chkpt_begin log record.
    logrec_t* logrec = new logrec_t;
    lsn_t begin_lsn;
    begin_lsn = Logger::log_sys<chkpt_begin_log>(lsn_t::null);
    W_COERCE(ss_m::log->flush_all());

    // No chkpts are taken in no-db mode -- log analysis always scans the whole
    // recovery log, which should be kept quite tiny at all times in no-db mode.
    // However, we still need to be able to know the min_xct_lsn, so we get it
    // from the log manager's oldest lsn tracker. To make sure that we keep track
    // of the current LSN, especially after log truncation during a clean shutdown,
    // we just write an empty chekpoint.
    if (!_no_db_mode) {
        // Collect checkpoint information from log
        curr_chkpt.scan_log(begin_lsn, _no_db_mode);
    }

    // Serialize chkpt to file
    fs::path fpath = smlevel_0::log->get_storage()->make_chkpt_path(lsn_t::null);
    fs::path newpath = smlevel_0::log->get_storage()->make_chkpt_path(begin_lsn);
    ofstream ofs(fpath.string(), ios::binary | ios::trunc);
    curr_chkpt.serialize_binary(ofs);
    ofs.close();
    fs::rename(fpath, newpath);
    smlevel_0::log->get_storage()->add_checkpoint(begin_lsn);

    if (_no_db_mode) {
        _min_xct_lsn = ss_m::log->get_oldest_active_lsn();
        _last_end_lsn = begin_lsn;
    }
    else {
        _min_rec_lsn = curr_chkpt.get_min_rec_lsn();
        _min_xct_lsn = curr_chkpt.get_min_xct_lsn();
        _last_end_lsn = curr_chkpt.get_last_scan_start();
    }

    // Release the 'write' mutex so the next checkpoint request can come in
    chkpt_mutex.release_write();

    delete logrec;
}

void chkpt_t::serialize_binary(ofstream& ofs)
{
    ofs.write((char*)&highest_tid, sizeof(tid_t));

    size_t buf_tab_size = buf_tab.size();
    ofs.write((char*)&buf_tab_size, sizeof(size_t));
    for(buf_tab_t::const_iterator it = buf_tab.begin();
            it != buf_tab.end(); ++it)
    {
        ofs.write((char*)&it->first, sizeof(PageID));
        ofs.write((char*)&it->second, sizeof(buf_tab_entry_t));
    }

    size_t xct_tab_size = xct_tab.size();
    ofs.write((char*)&xct_tab_size, sizeof(size_t));
    for(xct_tab_t::const_iterator it=xct_tab.begin();
            it != xct_tab.end(); ++it)
    {
        ofs.write((char*)&it->first, sizeof(tid_t));
        ofs.write((char*)&it->second.state, sizeof(smlevel_0::xct_state_t));
        ofs.write((char*)&it->second.last_lsn, sizeof(lsn_t));
        ofs.write((char*)&it->second.first_lsn, sizeof(lsn_t));

        size_t lock_tab_size = it->second.locks.size();
        ofs.write((char*)&lock_tab_size, sizeof(size_t));
        for(vector<lock_info_t>::const_iterator jt = it->second.locks.begin();
                jt != it->second.locks.end(); ++jt)
        {
            ofs.write((char*)&jt, sizeof(lock_info_t));
        }
    }

    size_t bkp_path_size = bkp_path.size();
    ofs.write((char*)&bkp_path_size, sizeof(size_t));
    if (!bkp_path.empty()) {
        ofs.write((char*)&bkp_path, bkp_path.size());
    }
}

void chkpt_t::deserialize_binary(ifstream& ifs)
{
    if(!ifs.is_open()) {
        cerr << "Could not open input stream for chkpt file" << endl;;
        W_FATAL(fcINTERNAL);
    }

    ifs.read((char*)&highest_tid, sizeof(tid_t));

    size_t buf_tab_size;
    ifs.read((char*)&buf_tab_size, sizeof(size_t));
    for(uint i=0; i<buf_tab_size; i++) {
        PageID pid;
        ifs.read((char*)&pid, sizeof(PageID));

        buf_tab_entry_t entry;
        ifs.read((char*)&entry, sizeof(buf_tab_entry_t));

        DBGOUT1(<<"pid[]="<<pid<< " , " <<
                  "rec_lsn[]="<<entry.rec_lsn<< " , " <<
                  "page_lsn[]="<<entry.page_lsn);

        // buf_tab[pid] = entry;
        mark_page_dirty(pid, entry.page_lsn, entry.rec_lsn);
    }

    size_t xct_tab_size;
    ifs.read((char*)&xct_tab_size, sizeof(size_t));
    for(uint i=0; i<xct_tab_size; i++) {
        tid_t tid;
        ifs.read((char*)&tid, sizeof(tid_t));

        xct_tab_entry_t entry;
        ifs.read((char*)&entry.state, sizeof(smlevel_0::xct_state_t));
        ifs.read((char*)&entry.last_lsn, sizeof(lsn_t));
        ifs.read((char*)&entry.first_lsn, sizeof(lsn_t));

        DBGOUT1(<<"tid[]="<<tid<<" , " <<
                  "state[]="<<entry.state<< " , " <<
                  "last_lsn[]="<<entry.last_lsn<<" , " <<
                  "first_lsn[]="<<entry.first_lsn);

        if (entry.state != smlevel_0::xct_ended) {
            mark_xct_active(tid, entry.first_lsn, entry.last_lsn);

            if (is_xct_active(tid)) {
                size_t lock_tab_size;
                ifs.read((char*)&lock_tab_size, sizeof(size_t));
                for(uint j=0; j<lock_tab_size; j++) {
                    lock_info_t lock_entry;
                    ifs.read((char*)&lock_entry, sizeof(lock_info_t));
                    // entry.locks.push_back(lock_entry);
                    add_lock(tid, lock_entry.lock_mode, lock_entry.lock_hash);

                    DBGOUT1(<< "    lock_mode[]="<<lock_entry.lock_mode
                            << " , lock_hash[]="<<lock_entry.lock_hash);
                }
            }
            // xct_tab[tid] = entry;
        }
    }

    size_t bkp_path_size;
    ifs.read((char*)&bkp_path_size, sizeof(size_t));
    if (!bkp_path.empty()) {
        ifs.read((char*)&bkp_path, bkp_path_size);
    }
}

