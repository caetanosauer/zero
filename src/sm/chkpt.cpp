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

class BackwardLogScanner
{
public:
    BackwardLogScanner(size_t bufferSize, lsn_t start_lsn, lsn_t stop_lsn)
        : bufferSize(bufferSize), bpos(0), stop_lsn(stop_lsn), prev_lsn(lsn_t::null)
    {
        fpos = start_lsn.lo();
        fnumber = start_lsn.hi();

        buffer = new char[bufferSize];
    }

    ~BackwardLogScanner()
    {
        delete[] buffer;
    }

    bool next(logrec_t*& lr)
    {
        if (bpos < sizeof(lsn_t)) {
            fpos += bpos;
            if (!nextBlock()) { return false; }
        }

        lsn_t lsn = *(reinterpret_cast<lsn_t*>(buffer + bpos - sizeof(lsn_t)));
        long offset = lsn.lo() - fpos;
        w_assert1(offset < (long) bufferSize);

        if (offset < 0) {
            fpos += bpos;
            if (!nextBlock()) { return false; }
            return next(lr);
        }

        lr = reinterpret_cast<logrec_t*>(buffer + offset);
        bpos = offset;

        w_assert1(lr->valid_header());
        w_assert1(lsn == lr->lsn());
        w_assert0(prev_lsn.is_null() || lsn.hi() < prev_lsn.hi()
                || lsn + lr->length() == prev_lsn);

        prev_lsn = lsn;
        return lr->lsn() >= stop_lsn;
    }

    bool nextBlock()
    {
        if (!logfile) {
            if (fpos == 0) { fnumber--; }
            if (fnumber < stop_lsn.hi()) { return false; }
            logfile = smlevel_0::log->get_storage()->get_partition(fnumber);
            if (!logfile) { return false; }

            logfile->open_for_read();
            fpos = logfile->get_size();
        }
        w_assert0(logfile);

        bpos = fpos > bufferSize ? bufferSize : fpos;
        auto bytesRead = logfile->read_block(buffer, bpos, fpos - bpos);
        w_assert0(bytesRead == bpos);

        fpos -= bpos;
        if (fpos == 0) {
            logfile = nullptr;
        }

        w_assert1(bpos > 0);
        return true;
    }

private:
    const size_t bufferSize;
    char* buffer;
    shared_ptr<partition_t> logfile;
    size_t bpos;
    size_t fnumber;
    size_t fpos;
    const lsn_t stop_lsn;
    lsn_t prev_lsn;
};

class BackwardFetchLogScanner
{
public:
    BackwardFetchLogScanner(lsn_t start_lsn) : curr_lsn(start_lsn)
    {
    }

    bool next(logrec_t*& lr)
    {
        return smlevel_0::log->fetch_direct(curr_lsn, lr, curr_lsn);
    }

private:
    lsn_t curr_lsn;
};

chkpt_m::chkpt_m(const sm_options& options, chkpt_t* chkpt_info)
    : worker_thread_t(options.get_int_option("sm_chkpt_interval", -1))
{
    _min_rec_lsn = chkpt_info->get_min_rec_lsn();
    _min_xct_lsn = chkpt_info->get_min_xct_lsn();
    _last_end_lsn = chkpt_info->get_last_scan_start();
    if (_last_end_lsn.is_null()) { _last_end_lsn = lsn_t(1, 0); }
    _use_log_archive = options.get_bool_option("sm_chkpt_use_log_archive", false);
    _log_based = options.get_bool_option("sm_chkpt_log_based", false);
    _print_propstats = options.get_bool_option("sm_chkpt_print_propstats", false);

    // _use_log_archive mandatory with nodb mode
    bool no_db_mode = options.get_bool_option("sm_no_db", false);
    bool write_elision = options.get_bool_option("sm_write_elision", false);
    if (no_db_mode || write_elision) {
        _use_log_archive = true;
    }

    if (_print_propstats) {
        _propstats_ofs.open("propstats_chkpt.txt", std::ofstream::out | std::ofstream::trunc);
    }

    fork();
}

void chkpt_m::do_work()
{
    take();
    ss_m::log->get_storage()->wakeup_recycler(true /* chkpt_only */);

    if (_print_propstats) {
        auto redo_length = smlevel_0::log->get_storage()->get_byte_distance(
                _min_rec_lsn, _last_end_lsn);
        _propstats_ofs << _dirty_page_count << '\t' << redo_length << std::endl;
    }
}

chkpt_m::~chkpt_m()
{
    stop();

    if (_print_propstats) {
        _propstats_ofs.close();
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
void chkpt_t::scan_log(lsn_t scan_start, lsn_t archived_lsn)
{
    init();

    if (scan_start.is_null()) {
        scan_start = smlevel_0::log->durable_lsn();
    }
    w_assert1(scan_start <= smlevel_0::log->durable_lsn());
    if (scan_start == lsn_t(1,0)) { return; }

    // Set when scan finds begin of previous checkpoint
    lsn_t scan_stop = lsn_t(1,0);

#ifdef USE_MMAP
    // CS TODO: using BackwardLogScanner for current experiments, but Fetch variant is preferred in the long run
    // BackwardFetchLogScanner scan {scan_start};
    constexpr size_t bufferSize = 8 * 1024 * 1024;
    BackwardLogScanner scan {bufferSize, scan_start, scan_stop};
#else
    constexpr size_t bufferSize = 8 * 1024 * 1024;
    BackwardLogScanner scan {bufferSize, scan_start, scan_stop};
#endif

    logrec_t* lr;
    lsn_t lsn = lsn_t::max;

    while (scan.next(lr) && lsn > scan_stop)
    {
        lsn = lr->lsn();
        if (lr->is_skip() || lr->type() == logrec_t::t_comment) {
            continue;
        }

        // when taking chkpts, scan_start will be a just-generated begin_chkpt -- ignore it
        if (lr->lsn() == scan_start && lr->type() == logrec_t::t_chkpt_begin) {
            continue;
        }

        xct_tab_entry_t* xct = nullptr;

        if (lr->tid() != 0) {
            if (lr->tid() > get_highest_tid()) {
                set_highest_tid(lr->tid());
            }

            auto& s = xct_tab[lr->tid()];
            if (lr->is_page_update() || lr->is_cpsn()) {
                s.update_lsns(lsn, lsn);

                if (s.is_active()) {
                    if (!lr->is_cpsn()) { acquire_lock(s, *lr); }
                }
            }

            xct = &s;
        }

        analyze_logrec(*lr, xct, scan_stop, archived_lsn);

        if (lr->is_redo() && lsn > archived_lsn) {
            mark_page_dirty(lr->pid(), lsn, lsn);

            if (lr->is_multi_page()) {
                w_assert0(lr->pid2() != 0);
                mark_page_dirty(lr->pid2(), lsn, lsn);
            }
        }
    }

    w_assert0(lsn == scan_stop);
    last_scan_start = scan_start;

    cleanup();
}

void chkpt_t::analyze_logrec(logrec_t& r, xct_tab_entry_t* xct, lsn_t& scan_stop,
        lsn_t archived_lsn)
{
    auto lsn = r.lsn();

    switch (r.type())
    {
        case logrec_t::t_chkpt_begin:
            {
                fs::path fpath = smlevel_0::log->get_storage()->make_chkpt_path(lsn);
                if (fs::exists(fpath)) {
                    ifstream ifs(fpath.string(), ios::binary);
                    deserialize_binary(ifs, archived_lsn);
                    ifs.close();
                    scan_stop = lsn;
                }
            }

            break;
        case logrec_t::t_xct_end:
        case logrec_t::t_xct_abort:
            xct->mark_ended();
            break;

        case logrec_t::t_page_write:
            {
                char* pos = r.data();

                PageID pid = *((PageID*) pos);
                pos += sizeof(PageID);

                lsn_t clean_lsn = *((lsn_t*) pos);
                pos += sizeof(lsn_t);

                if (clean_lsn < archived_lsn) { break; }

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
                lsn_t backupLSN = *((lsn_t*) r.data_ssx());
                const char* dev = (const char*)(r.data_ssx() + sizeof(lsn_t));
                add_backup(dev, backupLSN);
            }
            break;
        case logrec_t::t_restore_begin:
            if (!ignore_restore) {
                ongoing_restore = true;
                restore_page_cnt = *((PageID*) r.data_ssx());
                // this might be a failure-upon-failure, in which case we want to
                // ignore the first failure
                ignore_restore = true;
            }
        case logrec_t::t_restore_end:
            {
                // in backward scan, this tells us that there was a restore
                // going on, but it is finished, so we can ignore it
                ignore_restore = true;
            }
            break;
        case logrec_t::t_restore_segment:
            if (!ignore_restore) {
                uint32_t segment = *((uint32_t*) r.data_ssx());
                restore_tab.push_back(segment);
            }
            break;
        default:
            break;

    } //switch
}

void chkpt_t::init()
{
    highest_tid = 0;
    last_scan_start = lsn_t::null;
    ignore_restore = false;
    ongoing_restore = false;
    restore_page_cnt = 0;
    buf_tab.clear();
    xct_tab.clear();
    bkp_path.clear();
    bkp_lsn = lsn_t::null;
    restore_tab.clear();
}

void chkpt_t::mark_page_dirty(PageID pid, lsn_t page_lsn, lsn_t rec_lsn)
{
    buf_tab[pid].mark_dirty(page_lsn, rec_lsn);
}

void chkpt_t::mark_page_clean(PageID pid, lsn_t lsn)
{
    buf_tab[pid].mark_clean(lsn);
}

xct_tab_entry_t& chkpt_t::mark_xct_active(tid_t tid, lsn_t first, lsn_t last)
{
    auto& entry = xct_tab[tid];
    entry.update_lsns(first, last);
    return entry;
}

void chkpt_t::add_backup(const char* path, lsn_t lsn)
{
    bkp_path = path;
    bkp_lsn = lsn;
}

void chkpt_t::cleanup()
{
    // Remove non-dirty pages
    for (auto it  = buf_tab.begin(); it != buf_tab.end(); ) {
        if(!it->second.is_dirty()) {
            it = buf_tab.erase(it);
        }
        else {
            ++it;
        }
    }

    // Remove finished transactions.
    for (auto it  = xct_tab.begin(); it != xct_tab.end(); ) {
        if (!it->second.is_active()) {
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
    for(auto it = xct_tab.cbegin(); it != xct_tab.cend(); ++it)
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
    for(auto it = buf_tab.cbegin(); it != buf_tab.cend(); ++it)
    {
        if(it->second.is_dirty() && min_rec_lsn > it->second.rec_lsn) {
            min_rec_lsn = it->second.rec_lsn;
        }
    }
    if (min_rec_lsn == lsn_t::max) { return lsn_t::null; }
    return min_rec_lsn;
}

void chkpt_t::set_redo_low_water_mark(lsn_t lsn)
{
    // Used by nodb mode (see restart_thread_t::notify_archived_lsn)
    for (auto it  = buf_tab.begin(); it != buf_tab.end(); ) {
        if(it->second.page_lsn < lsn) { it = buf_tab.erase(it); }
        else { ++it; }
    }
}

void chkpt_t::acquire_lock(xct_tab_entry_t& xct, logrec_t& r)
{
    w_assert1(xct.is_active());
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

                xct.add_lock(mode, lid.hash());
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

                xct.add_lock(mode, lid.hash());
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

                xct.add_lock(mode, lid.hash());
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

                    xct.add_lock(mode, lid.hash());
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

                xct.add_lock(mode, lid.hash());
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

void chkpt_m::take(chkpt_t* chkpt)
{
    std::unique_lock<std::mutex> lck {chkpt_mutex};

    DBGOUT1(<<"BEGIN chkpt_m::take");
    INC_TSTAT(log_chkpt_cnt);

    // During log analysis, the chkpt_t object is computed outside this method,
    // but take() is still called to save the checkpoint. In that case, passing
    // a chkpt_t object skips the log scan or fuzzy checkpoint below.
    bool chkpt_given = chkpt;
    if (!chkpt) { chkpt = &curr_chkpt; }

    // Insert chkpt_begin log record.
    lsn_t begin_lsn = Logger::log_sys<chkpt_begin_log>();
    W_COERCE(ss_m::log->flush(begin_lsn));
    w_assert0(!begin_lsn.is_null());

    lsn_t archived_lsn = lsn_t::null;
    if (_use_log_archive) {
        // In no-db mode, updates that have been archived are considered clean
        archived_lsn = smlevel_0::logArchiver->getIndex()->getLastLSN();
    }

    if (!chkpt_given) {
        if (_log_based) {
            // Collect checkpoint information from log
            // CS TODO: interrupt scan_log if stop is requested
            // if (should_exit()) { return; }
            chkpt->scan_log(begin_lsn, archived_lsn);
        }
        else {
            chkpt->init();
            smlevel_0::recovery->checkpoint_dirty_pages(*chkpt);
            smlevel_0::bf->fuzzy_checkpoint(*chkpt);
            xct_t::fuzzy_checkpoint(*chkpt);
            chkpt->set_last_scan_start(begin_lsn);
        }
    }

    // Serialize chkpt to file
    fs::path fpath = smlevel_0::log->get_storage()->make_chkpt_path(lsn_t::null);
    fs::path newpath = smlevel_0::log->get_storage()->make_chkpt_path(begin_lsn);
    ofstream ofs(fpath.string(), ios::binary | ios::trunc);
    chkpt->serialize_binary(ofs);
    ofs.close();
    fs::rename(fpath, newpath);
    smlevel_0::log->get_storage()->add_checkpoint(begin_lsn);

    if (_use_log_archive) {
        // In no-db mode, the min_rec_lsn value is meaningless, since there is
        // no page cleaner. The equivalent in this case, i.e., the point up
        // to which the recovery log can be truncated, is determined by the
        // log archiver.
        _min_rec_lsn = archived_lsn;
    }
    else {
        _min_rec_lsn = chkpt->get_min_rec_lsn();
    }
    _min_xct_lsn = chkpt->get_min_xct_lsn();
    _last_end_lsn = chkpt->get_last_scan_start();
    _dirty_page_count = chkpt->buf_tab.size();
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

    size_t restore_tab_size = restore_tab.size();
    ofs.write((char*) &restore_tab_size, sizeof(size_t));

    if (restore_tab_size > 0) {
        ofs.write((char*)&restore_page_cnt, sizeof(PageID));
    }

    for (auto s : restore_tab) {
        ofs.write((char*) &s, sizeof(uint32_t));
    }

    size_t bkp_path_size = bkp_path.size();
    ofs.write((char*)&bkp_path_size, sizeof(size_t));
    if (!bkp_path.empty()) {
        ofs.write((char*)&bkp_lsn, sizeof(lsn_t));
        ofs.write((char*)&bkp_path, bkp_path.size());
    }
}

void chkpt_t::deserialize_binary(ifstream& ifs, lsn_t archived_lsn)
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

        if (entry.page_lsn > archived_lsn) {
            buf_tab[pid].mark_dirty(entry.page_lsn, entry.rec_lsn);
        }
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

        if (entry.is_active()) {
            auto& new_entry = xct_tab[tid];
            new_entry.update_lsns(entry.first_lsn, entry.last_lsn);

            size_t lock_tab_size;
            ifs.read((char*)&lock_tab_size, sizeof(size_t));
            for(uint j=0; j<lock_tab_size; j++) {
                lock_info_t lock_entry;
                ifs.read((char*)&lock_entry, sizeof(lock_info_t));
                // entry.locks.push_back(lock_entry);
                new_entry.add_lock(lock_entry.lock_mode, lock_entry.lock_hash);

                DBGOUT1(<< "    lock_mode[]="<<lock_entry.lock_mode
                        << " , lock_hash[]="<<lock_entry.lock_hash);
            }
        }
    }

    size_t restore_tab_size;
    ifs.read((char*)&restore_tab_size, sizeof(size_t));

    if (restore_tab_size > 0) {
        ongoing_restore = true;
        ifs.read((char*)&restore_page_cnt, sizeof(PageID));
    }

    uint32_t segment;
    for (size_t i = 0; i < restore_tab_size; i++) {
       ifs.read((char*) &segment, sizeof(uint32_t));
       restore_tab.push_back(segment);
    }

    size_t bkp_path_size;
    ifs.read((char*)&bkp_path_size, sizeof(size_t));
    if (!bkp_path.empty()) {
        ifs.read((char*)&bkp_lsn, sizeof(lsn_t));
        ifs.read((char*)&bkp_path, bkp_path_size);
    }
}

