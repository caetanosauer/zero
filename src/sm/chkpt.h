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

/*<std-header orig-src='shore' incl-file-exclusion='CHKPT_H'>

 $Id: chkpt.h,v 1.23 2010/06/08 22:28:55 nhall Exp $

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

#ifndef CHKPT_H
#define CHKPT_H

#include "w_defines.h"

#include "sm_options.h"
#include "sm_base.h"
#include "w_heap.h"
#include "logarchiver.h"
#include "w_okvl.h"
#include "xct.h"

#include <vector>
#include <list>
#include <map>
#include <algorithm>
#include <limits>

struct buf_tab_entry_t {
    lsn_t rec_lsn;              // initial dirty lsn
    lsn_t page_lsn;             // last write lsn
    lsn_t clean_lsn;            // last time page was cleaned

    buf_tab_entry_t() :
        rec_lsn(lsn_t::max), page_lsn(lsn_t::null), clean_lsn(lsn_t::null)
    {}

    bool is_dirty() const { return page_lsn >= clean_lsn; }
};

struct lock_info_t {
    okvl_mode lock_mode;
    uint32_t lock_hash;
};

struct xct_tab_entry_t {
    smlevel_0::xct_state_t state;
    lsn_t last_lsn;               // most recent log record
    lsn_t first_lsn;              // first lsn of the txn
    vector<lock_info_t> locks;

    xct_tab_entry_t() :
        state(xct_t::xct_active), last_lsn(lsn_t::null), first_lsn(lsn_t::max) {}
};

typedef map<PageID, buf_tab_entry_t>       buf_tab_t;
typedef map<tid_t, xct_tab_entry_t>        xct_tab_t;

class chkpt_t {
    friend class chkpt_m;
private:
    tid_t highest_tid;
    lsn_t last_scan_start;

public: // required for restart for now
    buf_tab_t buf_tab;
    xct_tab_t xct_tab;
    string bkp_path;

public:
    void scan_log(lsn_t scan_start = lsn_t::null);

    void mark_page_dirty(PageID pid, lsn_t page_lsn, lsn_t rec_lsn);
    void mark_page_clean(PageID pid, lsn_t lsn);

    void mark_xct_active(tid_t tid, lsn_t first_lsn, lsn_t last_lsn);
    void mark_xct_ended(tid_t tid);
    bool is_xct_active(tid_t tid) const;
    void delete_xct(tid_t tid);
    void add_lock(tid_t tid, okvl_mode mode, uint32_t hash);

    void add_backup(const char* path);

    lsn_t get_min_rec_lsn() const;
    lsn_t get_min_xct_lsn() const;
    lsn_t get_last_scan_start() const { return last_scan_start; }

    tid_t get_highest_tid() { return highest_tid; }
    void set_highest_tid(tid_t tid) { highest_tid = tid; }

    void dump(ostream& out);

private:
    void init();
    void serialize();
    void serialize_binary(ofstream& ofs);
    void deserialize_binary(ifstream& ofs);
    void cleanup();
    void acquire_lock(logrec_t& r);
};


class chkpt_thread_t;

/*********************************************************************
 *
 *  class chkpt_m
 *
 *  Checkpoint Manager. User calls spawn_chkpt_thread() to fork
 *  a background thread to take checkpoint every now and then.
 *  User calls take() to take a checkpoint immediately.
 *
 *  User calls wakeup_and_take() to wake up the checkpoint
 *  thread to checkpoint soon.
 *
 *********************************************************************/
class chkpt_m : public smlevel_0 {
public:
    /// chkpt_info is obtained via log analysis
    chkpt_m(const sm_options&, chkpt_t* chkpt_info = nullptr);
    virtual ~chkpt_m();

public:
    void take();
    void wakeup_thread();
    void retire_thread();

    lsn_t get_min_rec_lsn() { return _min_rec_lsn; }
    lsn_t get_min_xct_lsn() { return _min_xct_lsn; }

    /*
     * min_active_lsn is the LSN up to which log records can be thrown away,
     * because they will never be needed for undoing an active transaction or
     * redoing a dirty page. If both min_rec_lsn and min_xct_lsn are null, it
     * indicates that the system is "clean", i.e., no active transactions or
     * dirty pages. For this case, we return the LSN of the last checkpoint.
     */
    lsn_t get_min_active_lsn() {
        lsn_t min = _last_end_lsn;
        if (!_min_rec_lsn.is_null() && _min_rec_lsn < min) {
            min = _min_rec_lsn;
        }
        if (!_min_xct_lsn.is_null() && _min_xct_lsn < min) {
            min = _min_xct_lsn;
        }
        return min;
    }

private:
    chkpt_thread_t*  _chkpt_thread;
    long             _chkpt_count;
    chkpt_t          curr_chkpt;
    occ_rwlock       chkpt_mutex;

    void             _acquire_lock(logrec_t& r, chkpt_t& new_chkpt);

    // Values cached from the last checkpoint
    lsn_t _min_rec_lsn;
    lsn_t _min_xct_lsn;
    lsn_t _last_end_lsn;
};

/*<std-footer incl-file-exclusion='CHKPT_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
