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

/*  -- do not edit anything above this line --   </std-header>*/

#include "sm_base.h"
#include "w_heap.h"

//#include "lock.h"               // Lock re-acquisition
//#include "btree_impl.h"         // Lock re-acquisition
//#include "btree_logrec.h"       // Lock re-acquisition

#include <vector>
#include <algorithm>
#include <limits>

// For checkpoint to gather lock information into heap if asked
struct comp_lock_info_t;
class CmpXctLockTids;
typedef class Heap<comp_lock_info_t*, CmpXctLockTids> XctLockHeap;

struct chkpt_t{
  lsn_t begin_lsn;
  lsn_t min_rec_lsn;
  lsn_t min_xct_lsn;

  //Volume Table
  vid_t next_vid;
  vector<string> dev_paths;

  //Backup Table
  vector<vid_t> backup_vids;
  vector<string> backup_paths;

  //Dirty Page Table
  vector<lpid_t> pid;     // page lpid
  vector<snum_t> store;     // page lpid
  vector<lsn_t> rec_lsn;   // initial dirty lsn
  vector<lsn_t> page_lsn;  // last write lsn
  vector<bool> dirty;      //this flag is only used to filter non-dirty pages

  //Lock Table (one vector per transaction)
  vector<vector<okvl_mode> > lock_mode;  // lock mode
  vector<vector<uint32_t> > lock_hash;   // lock hash

  //Transaction Table
  tid_t youngest;
  vector<tid_t> tid;               // transaction ID
  vector<smlevel_0::xct_state_t> state; // state
  vector<lsn_t> last_lsn;          // most recent log record
  vector<lsn_t> undo_nxt;          // undo next
  vector<lsn_t> first_lsn;         // first lsn of the txn
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
    NORET            chkpt_m();
    NORET            ~chkpt_m();

    /*
    * smlevel_0::chkpt_mode is always set to one of the mode
     */
    enum chkpt_mode_t {
        t_chkpt_none,    // no on-going checkpoint
        t_chkpt_sync,    // in the middle of synchronous checkpoint
        t_chkpt_async    // in the middle of asynchronous checkpoint
    };

public:
    void             wakeup_and_take();
    void             spawn_chkpt_thread();
    void             retire_chkpt_thread();
    void             synch_take();
    void             synch_take(XctLockHeap& lock_heap);  // Record lock information in heap
    void             take(chkpt_mode_t chkpt_mode, XctLockHeap& lock_heap, const bool record_lock = false);
    void             dcpld_take(chkpt_mode_t chkpt_mode);
    void             backward_scan_log(const lsn_t master_lsn, const lsn_t begin_lsn, chkpt_t& new_chkpt, const bool restart_with_lock);



private:
    chkpt_thread_t*  _chkpt_thread;
    long             _chkpt_count;
    lsn_t            _chkpt_last;
    LogArchiver::LogConsumer* cons;

    bool             _analysis_system_log(logrec_t& r, chkpt_t& new_chkpt);
    void             _analysis_ckpt_bf_log(logrec_t& r,  chkpt_t& new_chkpt);
    void             _analysis_ckpt_xct_log(logrec_t& r, chkpt_t& new_chkpt, tid_CLR_map& mapCLR);
    void             _analysis_ckpt_lock_log(logrec_t& r, chkpt_t& new_chkpt);
    void             _analysis_other_log(logrec_t& r, chkpt_t& new_chkpt, int xct_idx);
    void             _analysis_process_lock(logrec_t& r, chkpt_t& new_chkpt, tid_CLR_map& mapCLR, int xct_idx);
    void             _analysis_acquire_lock_log(logrec_t& r, chkpt_t& new_chkpt, int xct_idx);
    void             _analysis_process_compensation_map(tid_CLR_map& mapCLR, chkpt_t& new_chkpt);
    void             _analysis_process_txn_table(chkpt_t& new_chkpt);

    template<typename T>
    int indexOf(vector<T> vector, T value) {
      typename std::vector<T>::iterator it = find(vector.begin(), vector.end(), value);
      if(it != vector.end()) {
        size_t r = std::distance(vector.begin(), it);
        w_assert0(r <= INT_MAX);
        return r;
      }
      else {
        return -1;
      }
    }


public:
    // These functions are for the use of chkpt -- to serialize
    // logging of chkpt and prepares
};

/*<std-footer incl-file-exclusion='CHKPT_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
