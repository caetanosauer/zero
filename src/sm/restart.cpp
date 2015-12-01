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

 $Id: restart.cpp,v 1.145 2010/12/08 17:37:43 nhall Exp $

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

#define SM_SOURCE
#define RESTART_C

#include "sm_base.h"
#include "w_heap.h"
#include "chkpt.h"
#include "crash.h"
#include "sm_base.h"
#include "sm_du_stats.h"
#include "sm_base.h"
#include "btree_impl.h"         // Lock re-acquisition
#include "restart.h"
#include "btree_logrec.h"       // Lock re-acquisition
#include "sm.h"                 // Check system shutdown status

#include <fcntl.h>              // Performance reporting
#include <unistd.h>
#include <sstream>

#ifdef EXPLICIT_TEMPLATE
template class Heap<xct_t*, CmpXctUndoLsns>;
#endif

// TODO(Restart)... it was for a space-recovery hack, not needed
// tid_t                restart_m::_redo_tid;


/*****************************************************
//Dead code, comment out just in case we need to re-visit it in the future
// We are using the actual buffer pool to register in_doubt page during Log Analysis
// no longer using the special in-memory dirty page table for this purpose

typedef uint64_t dp_key_t;

inline dp_key_t dp_key(vid_t vid, PageID shpid) {
    return ((dp_key_t) vid << 32) + shpid;
}
inline dp_key_t dp_key(const PageID &pid) {
    return dp_key(pid.vol(), pid);
}
inline vid_t  dp_vid (dp_key_t key) {
    return key >> 32;
}
inline PageID  dp_shpid (dp_key_t key) {
    return key & 0xFFFFFFFF;
}

typedef std::map<dp_key_t, lsndata_t> dp_lsn_map;
typedef std::map<dp_key_t, lsndata_t>::iterator dp_lsn_iterator;
typedef std::map<dp_key_t, lsndata_t>::const_iterator dp_lsn_const_iterator;

// *  In-memory dirty pages table -- a dictionary of of pid and
// *  its recovery lsn.  Used only in recovery, which is to say,
// *  only 1 thread is active here, so the hash table isn't
// *  protected.
class dirty_pages_tab_t {
public:
    dirty_pages_tab_t() : _cachedMinRecLSN(lsndata_null), _validCachedMinRecLSN (false) {}
    ~dirty_pages_tab_t() {}

       // Insert an association (pid, lsn) into the table.
    void                         insert(const PageID& pid, lsndata_t lsn) {
        if (_validCachedMinRecLSN && lsn < _cachedMinRecLSN && lsn != lsndata_null)  {
            _cachedMinRecLSN = lsn;
        }
        w_assert1(_dp_lsns.find(dp_key(pid)) == _dp_lsns.end());
        _dp_lsns.insert(std::pair<dp_key_t, lsndata_t>(dp_key(pid), lsn));
    }

    // Returns if the page already exists in the table.
    bool                         exists(const PageID& pid) const {
        return _dp_lsns.find(dp_key(pid)) != _dp_lsns.end();
    }
    // Returns iterator (pointer) to the page in the table.
    dp_lsn_iterator              find (const PageID& pid) {
        return _dp_lsns.find(dp_key(pid));
    }

    dp_lsn_map&                  dp_lsns() {return _dp_lsns;}
    const dp_lsn_map&            dp_lsns() const {return _dp_lsns;}

    // Compute and return the minimum of the recovery lsn of all entries in the table.
    lsn_t                        min_rec_lsn();

    size_t                       size() const { return _dp_lsns.size(); }

    friend ostream& operator<<(ostream&, const dirty_pages_tab_t& s);

private:
    // rec_lsn of dirty pages.
    dp_lsn_map _dp_lsns;

    // disabled
    dirty_pages_tab_t(const dirty_pages_tab_t&);
    dirty_pages_tab_t&           operator=(const dirty_pages_tab_t&);

    lsndata_t                    _cachedMinRecLSN;
    bool                         _validCachedMinRecLSN;
};

*****************************************************/

/*********************************************************************
 *
 *  restart_m::restart(master, commit_lsn, redo_lsn, last_lsn, in_doubt_count)
 *
 *  Start the restart process. Master is the master lsn (lsn of
 *  the last successful checkpoint record).
 *
 * Restart invokes Log Analysis, REDO and UNDO if system is not opened during
 * the entire recovery process.
 * Restart invokes Log Analysis only if system is opened after Log Analysis.
 *
 *********************************************************************/
void
restart_m::restart(
    lsn_t master,             // In: starting point for log scan
    lsn_t& commit_lsn,        // Out: used if use_concurrent_log_restart()
    lsn_t& redo_lsn,          // Out: used if log driven REDO with use_concurrent_XXX_restart()
    lsn_t& last_lsn,          // Out: used if page driven REDO with use_concurrent_XXX_restart()
    uint32_t& in_doubt_count  // Out: used if log driven REDO with use_concurrent_XXX_restart()
    )
{
    FUNC(restart_m::restart);

    // Make sure the current state is before 'recovery', the Recovery operation can
    // be called only once per system start
    if (!before_recovery())
        W_FATAL_MSG(fcINTERNAL,
        << "Cannot recovery while the system is not in before_recovery state, current state: "
        << smlevel_0::operating_mode);

    redo_lsn = lsn_t::null;        // redo_lsn is the starting point for REDO log forward scan
    commit_lsn = lsn_t::null;      // commit_lsn is the validation point for concurrent mode using log
    in_doubt_count = 0;            // How many in_doubt pages from  Log Analysis phase
    lsn_t undo_lsn = lsn_t::null;  // undo_lsn is the stopping point for UNDO log backward scan (if used)

    // set so mount and dismount redo can tell that they should log stuff.
    DBGOUT1(<<"Recovery starting...");
    smlevel_0::errlog->clog << info_prio << "Restart recovery:" << flushl;
#if W_DEBUG_LEVEL > 2
    {
        DBGOUT5(<<"TX TABLE before analysis:");
        xct_i iter(true); // lock list
        xct_t* xd;
        while ((xd = iter.next()))  {
            w_assert2(  xd->state() == xct_t::xct_active ||
                    xd->state() == xct_t::xct_freeing_space );
            DBGOUT5(<< "transaction " << xd->tid() << " has state " << xd->state());
        }
        DBGOUT5(<<"END TX TABLE before analysis:");
    }
#endif

    // Turn off swizzling because it does not work with REDO and UNDO
    bool org_swizzling_enabled = smlevel_0::bf->is_swizzling_enabled();
    if (org_swizzling_enabled) {
        W_COERCE(smlevel_0::bf->set_swizzling_enabled(false));
    }

    // Phase 1: ANALYSIS.
    // Output : dirty page table, redo lsn, undo lsn and populated heap for undo
    smlevel_0::errlog->clog << info_prio << "Analysis ..." << flushl;

    DBGOUT3(<<"starting analysis at " << master << " redo_lsn = " << redo_lsn);
    if(logtrace)
    {
        // Print some info about the log tracing that will follow.
        // It's so hard to deciper if you're not always looking at this, so
        // we print a little legend.
        fprintf(stderr, "\nLEGEND:\n");
        w_ostrstream s;
        s <<" th.#"
        << " STT"
        << " lsn"
        << " A/R/I/U"
        << "LOGREC(TID, TYPE, FLAGS:F/U PAGE <INFO> (xid_prev)|[xid_prev]";
        fprintf(stderr, "%s\n", s.c_str());
        fprintf(stderr, " #: thread id\n");
        fprintf(stderr, " STT: xct state or ??? if unknown\n");
        fprintf(stderr, " A: read for analysis\n");
        fprintf(stderr, " R: read for redo\n");
        fprintf(stderr, " U: read for rollback\n");
        fprintf(stderr, " I: inserted (undo pass or after recovery)\n");
        fprintf(stderr, " F: inserted by xct in forward processing\n");
        fprintf(stderr, " U: inserted by xct while rolling back\n");
        fprintf(stderr, " [xid_prev-lsn] for non-compensation records\n");
        fprintf(stderr, " (undo-lsn) for compensation records\n");
        fprintf(stderr, "\n\n");
    }

    // Log Analysis phase, the store is not opened for new transaction during this phase
    // Populate transaction table for all in-flight transactions, mark them as 'active'
    // Populate buffer pool for 'in_doubt' pages, register but not loading the pages

    // Populate a special heap with all the loser transactions for UNDO purpose (serial restart mode)
    CmpXctUndoLsns        cmp;
    XctPtrHeap            loser_heap(cmp);

    // Two special heaps to record all the re-acquired locks (on-demand restart mode)
    // First one is populated during Log Analysis phase
    // Second one is populated during checkpoint after the Log Analysis phase
    CmpXctLockTids        lock_cmp;
    XctLockHeap           lock_heap1(lock_cmp);  // For Log Analysis
    XctLockHeap           lock_heap2(lock_cmp);  // For Checkpoint
    bool                  restart_with_lock;     // Whether to acquire lock during Log Analysis

    // Measuer time for Log Analysis phase
    struct timeval tm_before;
    gettimeofday( &tm_before, NULL );

// TODO(Restart)... performance, determine whether to use forward scan or not
//                          based on milestone, this is for performance test only
//                          For normal usage, all milestones are using backward log scan in Log Analysis
/**
    // Using forward scan for M1/M2, backward scan for M3/M4
    if (false == use_concurrent_lock_restart())
    {
        // Forward log scan
        restart_with_lock = false; // Only used in 'analysis_pass_backward()'
        analysis_pass_forward(master, redo_lsn, in_doubt_count, undo_lsn, loser_heap,
                              commit_lsn, last_lsn);
    }
    else
    {
        // Backward log scan
        restart_with_lock = true;
        analysis_pass_backward(master, redo_lsn, in_doubt_count, undo_lsn, loser_heap,
                               commit_lsn, last_lsn, restart_with_lock, lock_heap1);
    }
**/

/* Normal code path, always backward scan during Log Analysis */
    // Using backward scan for all M1 - M5
    if (false == use_concurrent_lock_restart())
    {
        // Not using locks (either serial or log mode) - M1 and M2
        restart_with_lock = false;
    }
    else
    {
        // Using locks - M3 - M5
        restart_with_lock = true;
    }

    //analysis_pass_backward(master, redo_lsn, in_doubt_count, undo_lsn, loser_heap,
    //                       commit_lsn, last_lsn, restart_with_lock, lock_heap1);


    log_analysis(master, restart_with_lock, redo_lsn, undo_lsn, commit_lsn, last_lsn, in_doubt_count, loser_heap, lock_heap1);
    /*
    redo_lsn = cp.min_rec_lsn;
    undo_lsn = cp.min_xct_lsn;
    w_assert0(in_doubt_count == cp.pid.size());
    w_assert0(loser_heap.NumElements() == cp.tid.size());
    w_assert0(redo_lsn == cp.min_rec_lsn);
    w_assert0(undo_lsn == cp.min_xct_lsn);
    */
    // LL: we guess they will always be the same
    //w_assert1(undo_lsn == commit_lsn);

    struct timeval tm_after;
    gettimeofday( &tm_after, NULL );
// TODO(Restart)... Performance
    DBGOUT1(<< "**** Restart Log Analysis, elapsed time (milliseconds): "
            << (((double)tm_after.tv_sec - (double)tm_before.tv_sec) * 1000.0)
            + (double)tm_after.tv_usec/1000.0 - (double)tm_before.tv_usec/1000.0);

    // If nothing from Log Analysis, in other words, if both transaction table and buffer pool
    // are empty, there is nothing to do in REDO and UNDO phases, but we still want to
    // take a 'empty' checkpoint' as the starting point for the next server start.
    // In this case, only one checkpoint will be taken during Recovery, not multiple checkpoints

    int32_t xct_count = xct_t::num_active_xcts();

    // xct_count: the number of loser transactions in transaction table,
    //                 all transactions shoul be marked as 'active', they would be
    //                 removed in the UNDO phase
    // in_doubt_count: the number of in_doubt pages in buffer pool,
    //                 the pages would be loaded and turned into 'dirty' in REDO phase
    if ((0 == xct_count) && (0 == in_doubt_count))
    {
        smlevel_0::errlog->clog << info_prio
            << "Database is clean" << flushl;
    }
    else
    {
        smlevel_0::errlog->clog << info_prio
            << "Log contains " << in_doubt_count
            << " in_doubt pages and " << xct_count
            << " loser transactions" << flushl;
    }

    // Take a synch checkpoint after the Log Analysis phase and before the REDO phase
    w_assert1(smlevel_0::chkpt);
    if ((false == use_concurrent_lock_restart()) && (0 != in_doubt_count))
    {
        // Do not build the heap for lock information
        smlevel_0::chkpt->synch_take();
    }
    else if (0 != in_doubt_count)
    {
        // Checkpoint always gather lock information, but it would build a heap
        // on return only if asked for it (debug only)
        smlevel_0::chkpt->synch_take(lock_heap2);

        // Compare lock information in two heaps, the actual comparision is debug build only
        _compare_lock_entries(lock_heap1, lock_heap2);
    }

    if (false == use_serial_restart())
    {
        // We are done with Log Analysis phase
        // ready to open system before REDO and UNDO phases

        // Turn pointer swizzling on again
        if (org_swizzling_enabled)
        {
////////////////////////////////////////
// TODO(Restart)... with this change, we have disabled swizzling for the entire run
//                          if we open the system after Log Analysis phase
////////////////////////////////////////

            // Do not turn on swizzling
        }

        smlevel_0::errlog->clog << info_prio << "Restart Log Analysis successful." << flushl;
        DBGOUT1(<<"Restart Log Analysis ended");

        if (true == use_aries_restart())
        {
            // If M5 (ARIES), we need to finish REDO before open the system for user transactions
            struct timeval tm_before;
            gettimeofday( &tm_before, NULL );

            // REDO
            // Copy information to global variables first
            smlevel_0::commit_lsn = commit_lsn;
            smlevel_0::redo_lsn = redo_lsn;      // Log driven REDO, starting point for forward log scan
            smlevel_0::last_lsn = last_lsn;      // page driven REDO, last LSN in Recovery log before system crash
            smlevel_0::in_doubt_count = in_doubt_count;

            // Start REDO
            redo_concurrent_pass();

            // Set in_doubt count to 0 so we do not try to REDO again
            smlevel_0::in_doubt_count = 0;

            struct timeval tm_after;
            gettimeofday( &tm_after, NULL );
// TODO(Restart)... Performance
            DBGOUT1(<< "**** ARIES restart REDO, elapsed time (milliseconds): "
                    << (((double)tm_after.tv_sec - (double)tm_before.tv_sec) * 1000.0)
                    + (double)tm_after.tv_usec/1000.0 - (double)tm_before.tv_usec/1000.0);
        }

        // Return to caller (main thread), at this point:
        // M3/M4 - buffer pool contains all 'in_doubt' pages but the actual pages are not loaded.
        // M5 - buffer pool contains loaded dirty pages, in_doubt count = 0
        // Transaction table contains all loser transactions (marked as 'active').
        // This function returns sufficient information to caller, mainly to support 'Log driven REDO'
        // and concurrent txn validation
        // We do not persist the in-memory heap for UNDO, caller is using different
        // logic for UNDO and will not use the heap

        // Note that smlevel_0::operating_mode remains in t_in_analysis, while the caller
        // will change it to t_forward_processing

    }
    else
    {
        // Measure time for M1 REDO/UNDO phases
        struct timeval tm_before;
        gettimeofday( &tm_before, NULL );

        // System is not opened during the entire Recovery process
        // carry on the operations

        // It is valid to have in_doubt_count <> 0 while xct_count == 0 (all transactions ended)
        // because when a transaction commits, it flushs the log but not the buffer pool.

        lsn_t curr_lsn = log->curr_lsn();

        // Change mode to REDO outside of the REDO phase, this is for serialize process only
        smlevel_0::operating_mode = smlevel_0::t_in_redo;

        if (0 != in_doubt_count)
        {
             // Come in here only if we have something to REDO

             //  Phase 2: REDO -- use dirty page table and redo lsn of phase 1
             //                  We save curr_lsn before redo_pass() and assert after
             //                 redo_pass that no log record has been generated.
             //  pass in end_logscan_lsn for debugging

            smlevel_0::errlog->clog << info_prio << "Redo ..." << flushl;

#if W_DEBUG_LEVEL > 2
            {
                DBGOUT5(<<"TX TABLE at end of analysis:");
                xct_i iter(true); // lock list
                xct_t* xd;
                while ((xd = iter.next()))  {
                    w_assert1(  xd->state() == xct_t::xct_active);
                    DBGOUT5(<< "Transaction " << xd->tid() << " has state " << xd->state());
                }
                DBGOUT5(<<"END TX TABLE at end of analysis:");
            }
#endif

            // REDO phase, based on log records (forward scan), load 'in_doubt' pages
            // into buffer pool, REDO the updates, clear the 'in_doubt' flags and mark
            // the 'dirty' flags for modified pages.
            // No change to transaction table or recovery log
            DBGOUT3(<<"starting REDO at " << redo_lsn << " end_logscan_lsn " << curr_lsn);
            redo_log_pass(redo_lsn, curr_lsn, in_doubt_count);

            // no logging during redo
            // CS: commented out -- there may be event log records coming in
            // w_assert1(curr_lsn == log->curr_lsn());

            // We took a checkpoint at the end of Log Analysis phase which caused
            // a log flush, therefore the buffer pool flush at the end of the REDO phase
            // is optional, but we are doing it anyway so if we encounter a system crash
            // after this point, we would have less recovery work to do in the next recovery

            // In order to preserve the invariant that the rec_lsn <= page's lsn (last write lsn on page),
            // we need to make sure that all dirty pages get flushed to disk,
            // since the redo phase does NOT log these page updates, it causes
            // rec_lsns to be at the tail of the log while the page lsns are
            // in the middle of the log somewhere.  It seems worthwhile to
            // do this flush, slow though it might be, because if we have a crash
            // and have to re-recover, we would have less to do at that time.
            //
            // Note this buffer pool flush is only in serial mode, but not in concurrent
            // mode (open database after Log Analysis)

            W_COERCE(bf->wakeup_cleaners());
        }

        struct timeval tm_after;
        gettimeofday( &tm_after, NULL );
// TODO(Restart)... Performance
        DBGOUT1(<< "**** Restart traditional REDO, elapsed time (milliseconds): "
            << (((double)tm_after.tv_sec - (double)tm_before.tv_sec) * 1000.0)
            + (double)tm_after.tv_usec/1000.0 - (double)tm_before.tv_usec/1000.0);

        // Change mode to UNDO outside of the UNDO phase, this is for serialize process only
        smlevel_0::operating_mode = smlevel_0::t_in_undo;

        if (0 != xct_count)
        {
            // Come in here only if we have something to UNDO

            // Phase 3: UNDO -- abort all active transactions
            smlevel_0::errlog->clog  << info_prio<< "Undo ..."
                << " curr_lsn = " << curr_lsn  << " undo_lsn = " << undo_lsn
                << flushl;

            // UNDO phase, based on log records (reverse scan), use compensate operations
            // to UNDO (abort) the in-flight transactions, remove the aborted transactions
            // from the transaction table after rollback (compensation).
            // New log records would be generated due to compensation operations

            // curr_lsn: the current lsn which is at the end of pre-crash recovery log
            // undo_lsn: if doing backward log scan, this is the stopping point of the log scan
            //    in such case the backward log scan should start from from 'curr_lsn' and
            //    stop at 'undo_lsn'
            //    currently the implementation is not using backward log scan therefore
            //    the 'undo_lsn' is not used
            DBGOUT3(<<"starting UNDO phase, current lsn: " << curr_lsn <<
                    ", undo_lsn = " << undo_lsn);
            undo_reverse_pass(loser_heap, last_lsn, undo_lsn);

            smlevel_0::errlog->clog << info_prio << "Oldest active transaction is "
                << xct_t::oldest_tid() << flushl;
            smlevel_0::errlog->clog << info_prio
                << "First new transaction will be greater than "
                << xct_t::youngest_tid() << flushl;

            // Take a synch checkpoint after UNDO phase but before existing the Recovery operation
            smlevel_0::chkpt->synch_take();
        }

        // turn pointer swizzling on again after we are done with the Recovery
        if (org_swizzling_enabled)
        {
            W_COERCE(smlevel_0::bf->set_swizzling_enabled(true));
        }

        smlevel_0::errlog->clog << info_prio << "Restart successful." << flushl;
        DBGOUT1(<<"Recovery ended");

        struct timeval tm_done;
        gettimeofday( &tm_done, NULL );
// TODO(Restart)... Performance
        DBGOUT1(<< "**** Restart traditional UNDO, elapsed time (milliseconds): "
                << (((double)tm_done.tv_sec - (double)tm_after.tv_sec) * 1000.0)
                + (double)tm_done.tv_usec/1000.0 - (double)tm_after.tv_usec/1000.0);

        // Exiting from the Recovery operation, caller of the Recovery operation is
        // responsible of changing the 'operating_mode' to 'smlevel_0::t_forward_processing',
        // because caller is doing some mounting/dismounting devices, we change
        // the 'operating_mode' only after the device mounting operations are done.
    }
}

void restart_m::log_analysis(
    const lsn_t         master,
    bool                restart_with_lock,
    lsn_t&              redo_lsn,
    lsn_t&              undo_lsn,
    lsn_t&              commit_lsn,
    lsn_t&              last_lsn,
    uint32_t&           in_doubt_count,
    XctPtrHeap&         loser_heap,
    XctLockHeap&        lock_heap)
{
    FUNC(restart_m::log_analysis);

    AutoTurnOffLogging turnedOnWhenDestroyed;
    smlevel_0::operating_mode = smlevel_0::t_in_analysis;

    last_lsn = log->curr_lsn();
    chkpt_t v_chkpt;    // virtual checkpoint (not going to be written to log)

    /* Scan the log backward, from the most current lsn (last_lsn) until
     * master lsn or first completed checkpoint (might not be the same).
     * Returns a chkpt_t object with all required information to initialize
     * the other data structures. */
    smlevel_0::chkpt->backward_scan_log(master, last_lsn, v_chkpt, restart_with_lock);

    redo_lsn = v_chkpt.min_rec_lsn;
    undo_lsn = v_chkpt.min_xct_lsn;
    if(v_chkpt.min_xct_lsn == master) {
        commit_lsn = lsn_t::null;
    }
    else{
        commit_lsn = v_chkpt.min_xct_lsn; // or master?
    }

    //Re-load buffer
    for (buf_tab_t::iterator it  = v_chkpt.buf_tab.begin();
                             it != v_chkpt.buf_tab.end(); ++it) {
        bf_idx idx = 0;
        w_rc_t rc = RCOK;

        rc = smlevel_0::bf->register_and_mark(idx, it->first,
                                                it->second.store,
                                                it->second.rec_lsn.data() /*first_lsn*/,
                                                it->second.page_lsn.data() /*last_lsn*/,
                                                in_doubt_count);

        if (rc.is_error()) {
            // Not able to get a free block in buffer pool without evict, cannot continue
            W_FATAL_MSG(fcINTERNAL, << "Failed to record an in_doubt page in t_chkpt_bf_tab during Log Analysis" << rc);
        }
        w_assert1(0 != idx);
    }

    //Re-create transactions
    xct_t::update_youngest_tid(v_chkpt.youngest);
    for(xct_tab_t::iterator it  = v_chkpt.xct_tab.begin();
                            it != v_chkpt.xct_tab.end(); ++it) {
        xct_t* xd = new xct_t(NULL,               // stats
                        WAIT_SPECIFIED_BY_THREAD, // default timeout value
                        false,                    // sys_xct
                        false,                    // single_log_sys_xct
                        it->first,
                        it->second.last_lsn,      // last_LSN
                        it->second.undo_nxt,      // next_undo
                        true);                    // loser_xct, set to true for recovery

        xd->set_first_lsn(it->second.first_lsn); // Set the first LSN of the in-flight transaction
        xd->set_last_lsn(it->second.last_lsn);   // Set the last lsn in the transaction

        // Loser transaction
        if (true == use_serial_restart())
            loser_heap.AddElementDontHeapify(xd);
    }
    if (true == use_serial_restart()) {
        loser_heap.Heapify();
    }

    //Re-acquire locks
    if(restart_with_lock) {
        for(lck_tab_t::iterator it  = v_chkpt.lck_tab.begin();
                                it != v_chkpt.lck_tab.end(); ++it) {
            xct_t* xd = xct_t::look_up(it->first);
            list<lck_tab_entry_t>::iterator jt;
            for(jt = it->second.begin(); jt != it->second.end(); ++jt) {
                _re_acquire_lock(lock_heap, jt->lock_mode, jt->lock_hash, xd);
            }
        }
    }

    //Re-add backups
    // CS TODO only works for one backup
    smlevel_0::vol->sx_add_backup(v_chkpt.bkp_tab.bkp_path, true);
}

/*********************************************************************
 *
 *  restart_m::_re_acquire_lock(lock_heap, mode, hash, xd)
 *
 *  Helper function to add one lock entry information into a lock heap, this is tracking
 *  and debugging purpose, therefore only does the work in debug build
 *  called by analysis_pass_backward
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
void restart_m::_re_acquire_lock(XctLockHeap& lock_heap, // In: heap to record all re-acquired locks
                                 const okvl_mode& mode,     // In: lock mode to acquire
                                 const uint32_t hash,       // In: hash value of the lock to acquire
                                 xct_t* xd)                 // In: associated txn object
{
    w_assert1(0 <= lock_heap.NumElements());

    // Re-acquire the lock, hash value contains both index (store) and key information
    w_rc_t rc = RCOK;
    rc = btree_impl::_ux_lock_key(hash, mode, false /*check_only*/, xd);
    w_assert1(!rc.is_error());

#if W_DEBUG_LEVEL>0

    // Add the lock information to heap for tracking purpose
    comp_lock_info_t* lock_heap_elem = new comp_lock_info_t(mode);
    if (! lock_heap_elem)
    {
        W_FATAL(eOUTOFMEMORY);
    }
    // Fill in the rest of the lock information
    lock_heap_elem->tid = xd->tid();
    lock_heap_elem->lock_hash = hash;

    lock_heap.AddElementDontHeapify(lock_heap_elem);

#endif

    return;
}

/*********************************************************************
 *
 *  restart_m::_compare_lock_entries(lock_heap1, lock_heap2)
 *
 *  Helper function to compare entries in two lock heaps, for tracking/debugging purpose
 *  only used when doing backward log scan from Log Analysis
 * Current usage:
 *    Heap 1: from Log Analysis
 *    Heap 2: from checkpoint after Log Analysis
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
void restart_m::_compare_lock_entries(
                        XctLockHeap& lock_heap1, // In/out: first heap for the comparision, contains lock entries
                        XctLockHeap& lock_heap2) // In/out: second heap for the comparision, contains lock entries
{
    lock_heap1.Heapify();
    lock_heap2.Heapify();

    if (lock_heap1.NumElements() == lock_heap2.NumElements())
    {
        // Same amount of lock entries in both heaps, skip further comparision
        DBGOUT1(<< "_compare_lock_entries: same amount of log entries: " << lock_heap1.NumElements());
        return;
    }
    else
    {
        ERROUT(<< "_compare_lock_entries: different number of lock entries, lock_heap1 (Log Analysis): "
                << lock_heap1.NumElements() << ", lock_heap2 (Checkpoint): " << lock_heap2.NumElements());

        smlevel_0::errlog->clog << info_prio << "_compare_lock_entries: different number of lock entries, lock_heap1: "
                << lock_heap1.NumElements() << ", lock_heap2: " << lock_heap2.NumElements() << flushl;
    }

    // There are different number of entries in two heaps, print out
    // the different entries for debugging purpose
    // This is being done as part of Log Analysis phase while system
    // is not opened for concurrent user transactions, the process potentially
    // could take some time, so it is turned on for debug build only

    // It is possible two heaps have different number of locks, one scenario:
    // During Log Analysis, re-acquired locks on non-deterministic transaction
    // which was in the middle of rolling back (abort), but determined the in-flight
    // transaction was actually completed, therefore deleted all the re-acquired
    // locks of this transaction from lock manager, but the lock information
    // are in the heap already.  In such case, Log Analysis heap (lock_heap1) has
    // more locks than the checkpoint heap (lock_heap2).
    //
    // Note that it would be unexpected if checkpoint heap (lock_heap2) has more
    // locks than Log Analysis heap (lock_heap1).


#if W_DEBUG_LEVEL>0

    /////////////////////////////////////////////////////
    // TODO(Restart)... Not an ideal implementation, consider
    //                          improving the logic to be more efficient later
    /////////////////////////////////////////////////////

    comp_lock_info_t* lock_entry1 = NULL;

    DBGOUT3(<< "***** Print out differences from two heaps: heap1 - Log Analysis, heap2 - checkpoint *****");

    if ((0 != lock_heap1.NumElements()) && (0 == lock_heap2.NumElements()))
    {
        // Print all entries from heap1
        DBGOUT3(<< "Heap1 - ");
        _print_lock_entries(lock_heap1);
    }
    else if ((0 == lock_heap1.NumElements()) && (0 != lock_heap2.NumElements()))
    {
        // Print all entries from heap2
        DBGOUT3(<< "***** Heap2 - ");
        _print_lock_entries(lock_heap2);
    }
    else
    {
        w_assert1(0 != lock_heap1.NumElements());
        w_assert1(0 != lock_heap2.NumElements());

        comp_lock_info_t* lock_entry2 = NULL;

        bool deleted_1 = true;
        bool deleted_2 = true;

        // Process lock entry
        while (lock_heap1.NumElements() > 0)
        {
            // Take a new entry from heap1 if needed
            if (true == deleted_1)
                lock_entry1 = lock_heap1.RemoveFirst();

            // Take a new entry from heap2 if needed
            if (true == deleted_2)
            {
                // Heap 2 is empty, exit the loop
                if (0 == lock_heap2.NumElements())
                    break;
                lock_entry2 = lock_heap2.RemoveFirst();
            }
            w_assert1(NULL != lock_entry1);
            w_assert1(NULL != lock_entry2);

            // Reset
            deleted_1 = false;
            deleted_2 = false;

            // Compare two entries
            if (lock_entry1->tid != lock_entry2->tid)
            {
                // Different tid, these two entries are different
                if (lock_entry1->tid < lock_entry2->tid)
                    deleted_1 = true;
                else
                    deleted_2 = true;
            }
            else
            {
                // Same tid
                if (lock_entry1->lock_hash != lock_entry2->lock_hash)
                {
                    // Different hash, these two entries are different

                    if (lock_entry1->lock_hash < lock_entry2->lock_hash)
                        deleted_1 = true;
                    else
                        deleted_2 = true;
                }
                else
                {
                    // Same hash
                    if (lock_entry1->lock_mode.get_key_mode() != lock_entry2->lock_mode.get_key_mode())
                    {
                        // Different key mode, these two entries are different
                        if (lock_entry1->lock_mode.get_key_mode() < lock_entry2->lock_mode.get_key_mode())
                            deleted_1 = true;
                        else
                            deleted_2 = true;
                    }
                    else
                    {
                        // Same key mode, conclude these two entries are identical
                        // No need to print, move on to the next set of entries
                        deleted_1 = true;
                        deleted_2 = true;
                    }
                }
            }
            w_assert1((true == deleted_1) || (true == deleted_2));

            if (deleted_1 != deleted_2)
            {
                // Print the difference
                if (true == deleted_1)
                {
                    DBGOUT3(<< "Heap1 entry: tid: " << lock_entry1->tid << ", hash: " << lock_entry1->lock_hash
                            << ", key mode: " << lock_entry1->lock_mode.get_key_mode());
                    delete lock_entry1;
                    lock_entry1 = NULL;
                }
                else if (true == deleted_2)
                {
                    DBGOUT3(<< "***** Heap2 entry: tid: " << lock_entry2->tid << ", hash: " << lock_entry2->lock_hash
                            << ", key mode: " << lock_entry2->lock_mode.get_key_mode());
                    delete lock_entry2;
                    lock_entry2 = NULL;
                }
            }
            else
            {
                // Two items are the same, no need to print
                w_assert1(true == deleted_1);
                delete lock_entry1;
                lock_entry1 = NULL;
                delete lock_entry2;
                lock_entry2 = NULL;
            }
        }

        // If anything left in heap 1, print them all
        DBGOUT3(<< "Heap1 - ");
        _print_lock_entries(lock_heap1);

        // If anything left in heap 2, print them all
        DBGOUT3(<< "***** Heap2 - ");
        _print_lock_entries(lock_heap2);
    }

    w_assert1(0 == lock_heap1.NumElements());
    w_assert1(0 == lock_heap2.NumElements());

#endif

    return;
}

/*********************************************************************
 *
 *  restart_m::_print_lock_entries(lock_heap)
 *
 *  Helper function to print and cleanup all entries in a lock heap
 *  only used when doing backward log scan from Log Analysis
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
void restart_m::_print_lock_entries(XctLockHeap& lock_heap) // In: heap object contains lock entries
{
    comp_lock_info_t* lock_entry = NULL;

    if (0 != lock_heap.NumElements())
    {
        // Print all entries from heap1
        while (lock_heap.NumElements() > 0)
        {
            lock_entry = lock_heap.RemoveFirst();
            w_assert1(NULL != lock_entry);

            DBGOUT3(<< "    Heap entry: tid: " << lock_entry->tid << ", hash: " << lock_entry->lock_hash
                    << ", key mode: " << lock_entry->lock_mode.get_key_mode());

            // Free the memory allocated for this entry
            delete lock_entry;
            lock_entry = NULL;
        }
        w_assert1(0 == lock_heap.NumElements());
    }
}

/*********************************************************************
 *
 *  restart_m::redo_log_pass(redo_lsn, end_logscan_lsn, in_doubt_count)
 *
 *  Scan log forward from redo_lsn. Base on entries in buffer pool,
 *  apply redo if durable page is old.
 *
 *  M1 only while system is not opened during the entire Recovery process
 *
 *********************************************************************/
void
restart_m::redo_log_pass(
    const lsn_t        redo_lsn,       // This is where the log scan should start
    const lsn_t&       end_logscan_lsn,// This is the current log LSN, if in serial mode
                                       // REDO should not/generate log and this
                                       // value should not change
                                       // If concurrent mode, this is the stopping
                                       // point for log scan
    const uint32_t     in_doubt_count  // How many in_doubt pages in buffer pool
                                       // for validation purpose
)
{
    // Log driven Redo phase for both serial and concurrent modes

    FUNC(restart_m::redo_pass);

    if (0 == in_doubt_count)
    {
        // No in_doubt page in buffer pool, nothing to do in REDO phase
        return;
    }

    if (false == use_redo_log_restart())
    {
        // If not using log driven REDO
        W_FATAL_MSG(fcINTERNAL, << "REDO phase, restart_m::redo_pass() is valid for log driven REDO operation");
    }
    else
    {
        // The same function can be used in both serial and concurrent (open system early) modes
        // Also both commit_lsn and lock acquision methods

        // If serial mode then REDO phase never writes its own log records or
        // modify anything in  the transaction table
        // Because we are sharing this function for both serial and concurrent modes,
        // comment out this is an extra gurantee to make sure
        // no new log record.
        //
        // AutoTurnOffLogging turnedOnWhenDestroyed;

        // How many pages have been changed from in_doubt to dirty?
        uint32_t dirty_count = 0;

        // Open a forward scan of the recovery log, starting from the redo_lsn which
        // is the earliest lsn determined in the Log Analysis phase
        DBGOUT3(<<"Start redo scanning at redo_lsn = " << redo_lsn);
        log_i scan(*log, redo_lsn);
        lsn_t cur_lsn = log->curr_lsn();
        if(redo_lsn < cur_lsn) {
            DBGOUT3(<< "Redoing log from " << redo_lsn
                    << " to " << cur_lsn);
            smlevel_0::errlog->clog << info_prio
                << "Redoing log from " << redo_lsn
                << " to " << cur_lsn << flushl;
        }
        DBGOUT3( << "LSN " << " A/R/I(pass): " << "LOGREC(TID, TYPE, FLAGS:F/U(fwd/rolling-back) PAGE <INFO>");

        // Allocate a (temporary) log record buffer for reading
        logrec_t r;

        lsn_t lsn;
        lsn_t expected_lsn = redo_lsn;
        bool redone = false;
        bool serial_recovery = use_serial_restart();
        while (scan.xct_next(lsn, r))
        {
            // The difference between serial and concurrent modes with
            // log scan driven REDO:
            //     Concurrent mode needs to know when to stop the log scan
            if ((false == serial_recovery) && (lsn > end_logscan_lsn))
            {
                // If concurrent recovery, user transactions would generate new log records
                // stop forward scanning once we passed the end_logscan_lsn (passed in by caller)
                break;
            }

            DBGOUT3(<<"redo scan returned lsn " << lsn
                    << " expected " << expected_lsn);

            // For each log record ...
            if (!r.valid_header(lsn))
            {
                smlevel_0::errlog->clog << error_prio
                << "Internal error during redo recovery." << flushl;
                smlevel_0::errlog->clog << error_prio
                << "    log record at position: " << lsn
                << " appears invalid." << endl << flushl;
                abort();
            }

            // All these are for debugging and validation purposes
            // redone: whether REDO occurred for this log record
            // expected_lsn: the LSN in the retrieved log is what we expected
            redone = false;
            (void) redone; // Used only for debugging output
            DBGOUT3( << setiosflags(ios::right) << lsn
                          << resetiosflags(ios::right) << " R: " << r);
            w_assert1(lsn == r.lsn_ck());
            w_assert1(lsn == expected_lsn || lsn.hi() == expected_lsn.hi()+1);
            expected_lsn.advance(r.length());

            if ( r.is_redo() )
            {
                // If the log record is marked as REDOable (correct marking is important)
                // Most of the log records are REDOable.
                // These are not REDOable:
                //    txn related log records, e.g., txn begin/commit
                //    checkpoint related log records
                //    skip log records
                // Note compensation log records are 'redo only', the record type are regular
                // log record but marked as 'cpsn'

                // pid in log record is populated when a log record is filled
                // null_pid is checking the page numer (PageID) recorded in the log record
                if (r.null_pid())
                {
                    // Cannot be compensate log record
                    w_assert1(!r.is_cpsn());

                    // The log record does not contain a page number for the buffer pool
                    // there is no 'redo' in the buffer pool but we still need to 'redo' these
                    // transactions

                    // If the transaction is still in the table after log analysis,
                    // it didn't get committed or aborted,
                    // so go ahead and process it.
                    // If it isn't in the table, it was  already committed or aborted.
                    // If it's in the table, its state is prepared or active.
                    // Nothing in the table should now be in aborting state.
                    if (!r.is_single_sys_xct() && r.tid() != tid_t::null)
                    {
                        // Regular transaction with a valid txn id
                        xct_t *xd = xct_t::look_up(r.tid());
                        if (xd)
                        {
                            if (xd->state() == xct_t::xct_active)
                            {
                                DBGOUT3(<<"redo - no page, xct is " << r.tid());
                                r.redo(0);

                                // No page involved, no need to update dirty_count
                                redone = true;
                            }
                            else
                            {
                                // as there is no longer prepared xct, we shouldn't hit here.
                                W_FATAL_MSG(fcINTERNAL,
                                    << "REDO phase, no page transaction not in 'active' state - invalid");
                            }
                        }
                        else
                        {
                            // Transaction is not in the transaction table, it ended already, no-op
                        }
                    }
                    else
                    {
                        // Redo mounts and dismounts, at the start of redo,
                        // all the volumes which were mounted at the redo lsn
                        // should be mounted.
                        // need to do this to take care of the case of creating
                        // a volume which mounts the volume under a temporary
                        // volume id in order to create stores and initialize the
                        // volume.  this temporary volume id can be reused,
                        // which is why this must be done.

                        if (!r.is_single_sys_xct())
                        {
                            // Regular transaction without a valid txn id
                            // It must be a mount or dismount log record

                            w_assert3(
                                      r.type() == logrec_t::t_chkpt_backup_tab ||
                                      r.type() == logrec_t::t_add_backup);

                            //r.redo(0);
                            // CS TODO
                            // log->SetLastMountLSN(lsn);

                            // No page involved, no need to update dirty_count
                            //redone = true;
                        }
                        else
                        {
                            // single-log-sys-xct doesn't have tid (because it's not needed!).

                            // Log Analysis phase took care of buffer pool information for system
                            // transaction.
                            // For system transaction without buffer pool impact, we need to redo
                            // them here.
                            // System transaction should have page number also, the logic here is
                            // on the defensive side in case we have system transactions which
                            // does not affect buffer pool.

                            // Note we cannot look up system transaction in transaction table because
                            // it does not have txn id.

                            // If the system transaction is not for page allocation/deallocation,
                            // creates a new ssx and runs it.
                            // Page allocation - taken care of as part of page format
                            // Page deallocation - no need from a recovery
                            if (r.type() != logrec_t::t_alloc_page
                                    && r.type() != logrec_t::t_dealloc_page
                                    // CS TODO -- restore not supported yet
                                    && r.type() != logrec_t::t_restore_begin
                                    && r.type() != logrec_t::t_restore_segment
                                    && r.type() != logrec_t::t_restore_end
                               )
                            {
                                DBGOUT3(<<"redo - no page, ssx");
                                sys_xct_section_t sxs (true); // single log!
                                w_assert1(!sxs.check_error_on_start().is_error());
                                r.redo(0);
                                // CS TODO
                                // log->SetLastMountLSN(lsn);
                                redone = true;
                                rc_t sxs_rc = sxs.end_sys_xct (RCOK);
                                w_assert1(!sxs_rc.is_error());
                            }
                        }
                    }
                }
                else
                {

                    // The log record contains a page number, ready to load and update the page

                    // It might be a compensate log record for aborted transaction before system crash,
                    // in such case, execute the compensate log reocrd as a normal log record to
                    // achieve the 'transaction abort' effect during REDO phase, no UNDO for
                    // aborted transaction (aborted txn are not kept in transaction table).

                    _redo_log_with_pid(r, lsn, end_logscan_lsn, r.pid(),
                                   redone, dirty_count);
                    if (r.is_multi_page())
                    {
                        w_assert1(r.is_single_sys_xct());
                        // If the log is an SSX log that touches multi-pages, also invoke
                        // REDO on the second page. Whenever the log type moves content
                        // (or, not self-contained), page=dest, page2=src.
                        // So, we try recovering page2 after page.
                        // Note currently only system transaction can affect more than one page, and
                        // in fact it is limited to 2 pages only

                        _redo_log_with_pid(r, lsn, end_logscan_lsn, r.pid2(),
                                           redone, dirty_count);
                    }
                }
            }
            else if ( r.is_cpsn() )
            {
                // Compensate log record in recovery log, they are from aborted/rollback transaction
                // before system crash, these transactions have been rollbacked before the system crash.
                // The actual log records to compensate the actions are marked 'redo' and handled in
                // the above 'if' case.
                // The log records falling into this 'if' are 'compensation' log record which contains the
                // original LSN which being compensated on, no other information.
                // No REDO for these log records.

                // Cannot be a multi-page log record
                w_assert1(false == r.is_multi_page());

                // If this compensation log record is for a previous compensation log record
                // (r.xid_prev() is a cpsn log record), ignore it.
                DBGOUT3(<<"redo - existing compensation log record, r.xid_prev(): " << r.xid_prev());
            }
            DBGOUT3( << setiosflags(ios::right) << lsn
                          << resetiosflags(ios::right) << " R: "
                          << (redone ? " redone" : " skipped") );
        }

        if (in_doubt_count != dirty_count)
        {
            // We did not convert all the in_doubt pages, raise error and do not continue the Recovery
            // CS: ignoring for now due to bug on recovery of page allocations (TODO)
            // (see BitBucket ticket #6)
            //
            //W_FATAL_MSG(fcINTERNAL,
                        //<< "Unexpected dirty page count at the end of REDO phase.  In_doubt count: "
                        //<< in_doubt_count << ", dirty count: " << dirty_count);
        }

        {
            w_base_t::base_stat_t f = GET_TSTAT(log_fetches);
            w_base_t::base_stat_t i = GET_TSTAT(log_inserts);
            smlevel_0::errlog->clog << info_prio
                << "Redo_pass: "
                << f << " log_fetches, "
                << i << " log_inserts " << flushl;
        }
// TODO(Restart)... performance
DBGOUT1(<<"redo - dirty count: " << dirty_count);
    }

    return;
}

/*********************************************************************
*
*  restart_m::_redo_log_with_pid(r, lsn,end_logscan_lsn, page_updated, redone, dirty_count)
*
*  For each log record, load the physical page if it is not in buffer pool yet, set the flags
*  and apply the REDO based on log record if the page is old
*
*  Function returns void, if encounter error condition (any error), raise error and abort
*  the operation, it cannot continue
*
*********************************************************************/
void restart_m::_redo_log_with_pid(
    logrec_t& r,                  // Incoming log record
    lsn_t &lsn,                   // LSN of the incoming log record
    const lsn_t &end_logscan_lsn, // This is the current LSN, if in serial mode,
                                  // REDO should not generate log record
                                  // and this value should not change
                                  // this is passed in for validation purpose
    PageID page_updated,          // Store ID (vol + store number) + page number
                                  // This is mainly because if the log is a multi-page log
                                  // this will be the information for the 2nd page
    bool &redone,                 // Did REDO occurred, for validation purpose
    uint32_t &dirty_count)        // Counter for the number of in_doubt to dirty pages
{
    // Use the log record to get the index in buffer pool
    // Get the cb of the page to make sure the page is indeed 'in_doubt'
    // load the physical page and apply the REDO to the page
    // and then clear the in_doubt flag and set the dirty flag

    // For all the buffer pool access, hold latch on the page
    // becasue we will open the store for new transactions during REDO phase
    // in the future, therefor the latch protection

    w_rc_t rc = RCOK;
    redone = false;             // True if REDO happened

    // 'is_redo()' covers regular transaction but not compensation transaction
    w_assert1(r.is_redo());
    w_assert1(r.pid());
    w_assert1(false == redone);

    // Because we are loading the page into buffer pool directly
    // we cannot have swizzling on
    w_assert1(!smlevel_0::bf->is_swizzling_enabled());

    bf_idx idx = smlevel_0::bf->lookup_in_doubt(page_updated);
    if (0 != idx)
    {
        // Found the page in hashtable of the buffer pool
        // Check the iin_doubt and dirty flag
        // In_doubt flag on: first time hitting this page, the physical page should
        //           not be in memory, load it
        // Dirty flag on: not the first time hitting this page, the physical page
        //          should be in memory already
        // Neither In_doubt or dirty flags are on: this cannot happen, error

        // Acquire write latch because we are going to modify
        bf_tree_cb_t &cb = smlevel_0::bf->get_cb(idx);
        // Acquire write latch for each page because we are going to update
        // Using time out value WAIT_IMMEDIATE:
        //    Serial mode: no conflict because this is the only operation
        //    Concurrent mode (both commit_lsn and lock):
        //                 Page (m2): concurrent txn does not load page, no conflict
        //                 On-demand (m3): only concurrent txn load page, no conflict
        //                 Mixed (m4): potential conflict, the failed one skip the page silently
        //                 ARIES (m5): no conflict because system is not opened
        rc = cb.latch().latch_acquire(LATCH_EX, WAIT_IMMEDIATE);
        if (rc.is_error())
        {
            // Unable to acquire write latch, cannot continue, raise an internal error
            DBGOUT3 (<< "Error when acquiring LATCH_EX for a page in buffer pool. pagw ID: "
                     << page_updated << ", rc = " << rc);
            W_FATAL_MSG(fcINTERNAL, << "REDO (redo_pass()): unable to EX latch a buffer pool page");
            return;
        }

        if ((true == smlevel_0::bf->is_in_doubt(idx)) || (true == smlevel_0::bf->is_dirty(idx)))
        {
            fixable_page_h page;
            bool virgin_page = false;
            bool corrupted_page = false;

            // Comments below (page format) are from the original implementation
            // save this comments so we don't lose the original thought in this area, although
            // the current implementation is different from the original implementation:
            // ***
            // If the log record is for a page format then there are two possible
            // implementations:
            // 1) Trusted LSN on New Pages
            //   If we assume that the LSNs on new pages can always be
            //   trusted then the code reads in the page and
            //   checks the page lsn to see if the log record
            //   needs to be redone.  Note that this requires that
            //   pages on volumes stored on a raw device must be
            //   zero'd when the volume is created.
            //
            // 2) No Trusted LSN on New Pages
            //   If new pages are not in a known (ie. lsn of 0) state
            //   then when a page_init record is encountered, it
            //   must always be redone and therefore all records after
            //   it must be redone.
            //
            // ATTENTION!!!!!! case 2 causes problems with
            //   tmp file pages that can get reformatted as tmp files,
            //   then converted to regular followed by a restart with
            //   no chkpt after the conversion and flushing of pages
            //   to disk, and so it has been disabled. That is to
            //   say:
            //
            //   DO NOT BUILD WITH
            //   DONT_TRUST_PAGE_LSN defined . In any case, I
            //   removed the code for its defined case.
            // ***

            if (r.type() == logrec_t::t_page_img_format
                // btree_norec_alloc is a multi-page log. "page2" (so, !=shpid()) is the new page.
                || (r.type() == logrec_t::t_btree_norec_alloc && page_updated != r.pid())
                // for btree_split, new page is page1 (so, ==shpid())
                || (r.type() == logrec_t::t_btree_split && page_updated == r.pid())
            )
            {
                virgin_page = true;
            }

            if ((true == smlevel_0::bf->is_in_doubt(idx)) && (false == virgin_page))
            {
                // Page is in_doubt and not a virgin page, this is the first time we have seen this page
                // need to load the page from disk into buffer pool first
                // Special case: the page is a root page which exists on disk, it was pre-loaded
                //                     during device mounting (_preload_root_page).
                //                     We will load reload the root page here but not register it to the
                //                     hash table (already registered).  Use the same logic to fix up
                //                     page cb, it does no harm.
                DBGOUT3 (<< "REDO phase, loading page from disk, page = " << page_updated);

                // If past_end is true, the page does not exist on disk and the buffer pool page
                // has been zerod out, we cannot apply REDO in this case
                rc = smlevel_0::bf->load_for_redo(idx, page_updated);

                if (rc.is_error())
                {
                    if (cb.latch().held_by_me())
                        cb.latch().latch_release();
                    if (eBADCHECKSUM == rc.err_num())
                    {
                        // Corrupted page, allow it to continue and we will
                        // use Single-Page-Recovery to recovery the page
                        DBGOUT3 (<< "REDO phase, newly loaded page was corrupted, page = " << page_updated);
                        corrupted_page = true;
                    }
                    else
                    {
                        // All other errors
                        W_FATAL_MSG(fcINTERNAL,
                                    << "Failed to load physical page into buffer pool in REDO phase, page: "
                                    << page_updated << ", RC = " << rc);
                    }
                }

                // Just loaded from disk, set the vol and page in cb
                cb._store_num = r.stid();
                cb._pid_shpid = page_updated;
            }
            else if ((true == smlevel_0::bf->is_in_doubt(idx)) && (true == virgin_page))
            {
                // First time encounter this page and it is a virgin page
                // We have the page cb and hashtable entry for this page already
                // There is nothing to load from disk, set the vol and page in cb

                cb._store_num = r.stid();
                cb._pid_shpid = page_updated;
            }
            else
            {
                // In_doubt flag is off and dirty flag is on, we have seen this page before
                // so the page has been loaded into buffer pool already, on-op
            }

            // Now the physical page is in memory and we have an EX latch on it
            // In this case we are not using fixable_page_h::fix_direct() because
            // we have the idx, need to manage the in_doubt and dirty flags for the page
            // and we have loaded the page already
            // 0. Assocate the page to fixable_page_h, swizzling must be off
            // 1. If a log record pertains does not pertain to one of the pages marked 'in_doubt'
            //   in the buffer pool, no-op (we should not get here in this case)
            // 2. If the page image in the buffer pool is newer than the log record, no-op
            // 3. If the page was corrupted from loading, use Single-Page-Recovery to recover first
            // 4. Apply REDO, modify the pageLSN value in the page image

            // Associate this buffer pool page with fixable_page data structure
            W_COERCE(page.fix_recovery_redo(idx, page_updated));

            // We rely on pid/tag set correctly in individual redo() functions
            // set for all pages, both virgin and non-virgin
            page.get_generic_page()->pid = page_updated;
            page.get_generic_page()->tag = t_btree_p;

            if (virgin_page)
            {
                // Virgin page has no last write
                page.get_generic_page()->lsn = lsn_t::null;
            }
            w_assert1(page.pid() == page_updated);

            if (corrupted_page)
            {
                // Corrupted page, use Single-Page-Recovery to recovery the page before retrieving
                // the last write lsn from page context
                // use the log record lsn for Single-Page-Recovery, which is the the actual emlsn

                // CS: since this method is static, we refer to restart_m indirectly
                // For corrupted page, set the last write to force a complete recovery
                page.get_generic_page()->lsn = lsn_t::null;
                W_COERCE(smlevel_0::recovery->recover_single_page(page, lsn, true));
            }

            /// page.lsn() is the last write to this page
            lsn_t page_lsn = page.lsn();

            DBGOUT3( << setiosflags(ios::right) << lsn
                     << resetiosflags(ios::right) << " R: "
                     << " page_lsn " << page_lsn
                     << " will redo if 1: " << int(page_lsn < lsn));

            if (page_lsn < lsn)
            {
                // The last write to this page was before the log record LSN
                // Need to REDO
                // REDO phase is for buffer pool in_doubt pages, the process is
                // not related to the transactions in transaction table

                // Log record was for a regular transaction
                // logrec_t::redo is invoking redo_gen.cpp (generated file)
                // which calls the appropriate 'redo' methond based
                // on the log type
                // Each log message has to implement its own 'redo'
                // and 'undo' methods, while some of the log records do not
                // support 'redo' and 'undo', for example, checkpoint related
                // log records do not have 'redo' and 'undo' implementation
                // For the generic log records, the 'redo' and 'undo' are in logrec.cpp
                // For the B-tree related log records, they are in btree_logrec.cpp

                // This function is shared by both Recovery and Single-Page-Recovery, it sets the page
                // dirty flag before the function returns, which is redudent for Recovery
                // because we will clear in_doubt flag and set dirty flag later

                DBGOUT3 (<< "redo because page_lsn < lsn");
                w_assert1(page.is_fixed());

                //Both btree_norec_alloc_log and tree_foster_rebalance_log are multi-page
                // system transactions, the 2nd page is the foster child and the page
                // gets initialized as an empty child page during 'redo'
                r.redo(&page);

                // TODO(Restart)... Something to do with space recoverying issue,
                // it does not seem needed with the new code
                //_redo_tid = tid_t::null;

                // Set the 'lsn' of this page (page lsn) to the log record lsn
                // which is the last write to this page
                page.update_initial_and_last_lsn(lsn);
                page.update_clsn(lsn);

                // The _rec_lsn in page cb is the earliest lsn which made the page dirty
                // the _rec_lsn (earliest lns) must be earlier than the page lsn
                // (last write to this page)
                // We need to update the _rec_lsn only if the page in_doubt flag is on
                // or it is a virgin page, meaning this is the first time we have seen
                // this page or it is a brand new page.
                // We do not need to update the _rec_lsn for an already seen page,
                // _rec_lsn should have been set already when we seen it the first time.
                // If we need to set the _rec_lsn, set it using the current log record lsn,
                // both _rec_lsn (initial dirty) and page lsn (last write) are set to the
                // current log record lsn in this case
                if ((true == smlevel_0::bf->is_in_doubt(idx)) ||
                    (true == virgin_page))
                {
                    if (cb._rec_lsn > lsn.data())
                        cb._rec_lsn = lsn.data();
                }

                // Finishe the REDO, set the flag so we will update the dirty page counter later
                redone = true;
            }
            else if (virgin_page)
            {
                // Set the initial dirty LSN to the current log record LSN
                cb._rec_lsn = lsn.data();

                // Virgin page, no need to REDO, set the flag to update dirty page counter
                redone = true;
            }
            else if ((page_lsn >= end_logscan_lsn) && (lsn_t::null != page_lsn))
            {
                // Not a virgin page, end_logscan_lsn is the current recovery log LSN
                // if the page last write LSN > end_logscan_lsn, this cannot happen
                // we have a page corruption
                DBGOUT1( << "WAL violation! page "
                         << page.pid()
                         << " has lsn " << page_lsn
                         << " end of log is record prior to " << end_logscan_lsn);

                if (cb.latch().held_by_me())
                    cb.latch().latch_release();
                W_FATAL_MSG(fcINTERNAL,
                    << "Page LSN > current recovery log LSN, page corruption detected in REDO phase, page: "
                    << page_updated);
            }
            else
            {
                DBGOUT3( << setiosflags(ios::right) << lsn
                         << resetiosflags(ios::right) << " R: "
                         << " page_lsn " << page_lsn
                         << " will skip & increment rec_lsn ");

                // The last write LSN of this page is larger than the current log record LSN
                // No need to apply REDO to the page
                // Bump the recovery lsn (last written) for the page to indicate that
                // the page is younger than the current log record; the earliest
                // record we have to apply is that after the page lsn.

                if (lsn_t::null != page_lsn)  // Virgin page has no last write
                {
                    w_assert1(false == virgin_page); // cannot be a virgin page
                    page.get_generic_page()->lsn = page_lsn.advance(1).data(); // non-const method
                }
            }

            // REDO happened, and this is the first time we seen this page
            if ((true == redone) && (true == smlevel_0::bf->is_in_doubt(idx)))
            {
                // Turn the in_doubt flag into the dirty flag
                smlevel_0::bf->in_doubt_to_dirty(idx);        // In use and dirty

                // For counting purpose, because we have cleared an in_doubt flag,
                // update the dirty_count in all cases
                ++dirty_count;
            }
        }
        else
        {
            // Neither in_doubt or dirty bit was set for the page, but the idx is in hashtable
            // If the log is for page allocation, then the page 'used' flag should be set
            //     later on we would have a log record to format the
            //     page (if it is not non-log operation)
            // If the log is for page deallocation, then the page 'used' flag should not be set
            //     and we should have removed the idx from hashtable, therefore the code
            //     should not get here
            // All other cases are un-expected, raise error
            if (r.type() == logrec_t::t_alloc_page)
            {
                // This is page allocation log record, nothing is in hashtable for this
                // page currently
                // Later on we probably will have a 't_page_img_format' log record
                // (if it is not a non-log operation) to formate this virgin page
                // No-op for the page allocation log record, because the 't_page_img_format'
                // log record has already registered the page in the hashtable

                // No need to change dirty_page count, a future page format log record
                // (t_page_img_format ) will update the dirty_page count

                // The 'used' flag of the page should be set
                // w_assert1(true == smlevel_0::bf->is_used(idx));
            }
            else if (r.type() == logrec_t::t_dealloc_page)
            {
                // The idx should not be in hashtable
                if (cb.latch().held_by_me())
                    cb.latch().latch_release();
                W_FATAL_MSG(fcINTERNAL,
                    << "Deallocated page should not exist in hashtable in REDO phase, page: "
                    << page_updated);
            }
            else
            {
                if (true == smlevel_0::bf->is_used(idx))
                {
                    // If the page 'used' flag is set but none of the other flags are on, and the log record
                    // is not page allocation or deallocation, we should not have this case
                    if (cb.latch().held_by_me())
                        cb.latch().latch_release();
                    W_FATAL_MSG(fcINTERNAL,
                        << "Incorrect in_doubt and dirty flags in REDO phase, page: "
                        << page_updated);
                }
            }
        }

        // Done, release write latch
        if (cb.latch().held_by_me())
            cb.latch().latch_release();
    }
    else
    {
        // The page cb is not in hashtable, 2 possibilities:
        // 1. A deallocation log to remove the page (r.is_page_deallocate())
        // 2. The REDO LSN is before the checkpoint, it is possible the current log record
        //     is somewhere between REDO LSN and checkpoint, it is referring to a page
        //     which was not dirty (not in_doubt) therefore this page does not require
        //     a REDO operation, this might be a rather common scenario
        //
        // NOOP if we get here since the log record does not require REDO

        // CS: An error used to be thrown here, but pages not marked in-doubt
        // during log analysis are actually up-to-date on disk and thus
        // do not require any REDO. Therefore, it is perfectly reasonable and
        // even quite likely that we end up in this "else" block with a regular
        // page update log record. The solution should be to simply ignore it.
        DBGOUT3(<< "Skipped logrec " << r.lsn_ck()
                << " -- page " << r.pid() << " not in doubt");
    }

    return;
}

/*********************************************************************
 *
 *  restart_m::undo_reverse_pass(heap, curr_lsn, undo_lsn)
 *
 *  abort all the active transactions, doing so in a strictly reverse
 *  chronological order.  This is done to get around a boundary condition
 *  in which an xct is aborted (for any reason) when the data volume is
 *  very close to full. Because undoing a btree remove can cause a page
 *  split, we could be unable to allocate a new page for the split, and
 *  this leaves us with a completely unrecoverable volume.  Until we
 *  ran into this case, we were using a pool of threads to do parallel
 *  rollbacks.  If we find an alternative way to deal with the corner case,
 *  such as not allowing volumes to get more than some threshold full,
 *  or having utilties that allow migration from one volume to a larger
 *  volume, we will leave this in place.  *Real* storage managers might
 *  have a variety of ways to cope with this.
 *
 *  But then there will also be the problem of page allocations, which
 *  I think is another reason for undoing in reverse chronological order.
 *
 *  M1 only while system is not opened during the entire Recovery process
 *
 *********************************************************************/
void
restart_m::undo_reverse_pass(
    XctPtrHeap&        heap,      // Heap populated with loser transactions
    const lsn_t        curr_lsn,  // Current lsn, the starting point of backward scan
                                  // Not used currently
    const lsn_t        undo_lsn   // Undo_lsn, the end point of backward scan
                                  // Not used currently
    )
{
    // This function supports both serial and concurrent_log mode
    // For concurrent mode, the same function is used for both concurrent_log
    // and concurrent_lock modes, this is because the code is using the standard
    // transaction rollback and abort functions, which should take care of
    // 'non-read-lock' (if acquired during Log Analysis phase)

    FUNC(restart_m::undo_pass);

    if ((false == use_serial_restart()) && (true == use_undo_reverse_restart()))
    {
        // When running in concurrent mode and using reverse chronological order UNDO
        // caller does not have the special heap, build it base on transaction table

        // Should be empty heap
        w_assert1(0 == heap.NumElements());

        // TODO(Restart)... Not locking the transaction table while
        // looping through it, this logic works while new transactions are
        // coming in, because the current implementation of transaction
        // table is inserting new transactions into the beginning of the
        // transaction table, so they won't affect the on-going loop operation

        xct_i iter(false); // not locking the transaction table list
        xct_t* xd;
        DBGOUT3( << "Building heap...");
        xd = iter.next();
        while (xd)
        {
            DBGOUT3( << "Transaction " << xd->tid()
                     << " has state " << xd->state() );

            if ((true == xd->is_loser_xct()) && (xct_t::xct_active == xd->state()))
            {
                // Found a loser transaction
                heap.AddElementDontHeapify(xd);
            }
            // Advance to the next transaction
            xd = iter.next();
        }
        heap.Heapify();
        DBGOUT3( << "Number of transaction entries in heap: " << heap.NumElements());
    }

    // Now we are ready to start the UNDO operation
    {
        // Executing reverse chronological order UNDO under serial operation (open system
        // after the entire recovery process finished)

        int xct_count = heap.NumElements();
        if (0 == xct_count)
        {
            // No loser transaction in transaction table, nothing to do in UNDO phase
            DBGOUT3(<<"No loser transaction to undo");
            return;
        }

        // curr_lsn and undo_lsn are used only if we are using the backward log scan
        // for the UNDO phase, which is not used currently.
        w_assert1(lsn_t::null != curr_lsn);
        w_assert1(lsn_t::null != undo_lsn);
        w_assert1(curr_lsn.data() != undo_lsn.data());

/*****************************************************
//Dead code, comment out just in case we want to consider this solution in the future
//
// The traditional UNDO is using a backward scan of the recovery log
// and UNDO one log record at a time
// The current log scan implementation is slow and probably could be improved.
// Instead, we decided to use an enhanced version of the original Shore-MT
// implementation which is using heap to record all the loser transactions
// for UNDO purpose.
// The backward scan of the recovery log has been implemented but
// not used.  I am keeping the backward scan code just in case if we need
// to use it for some reasons in the future.

    DBGOUT3(<<"Start undo backward scanning at curr_lsn = " << curr_lsn);
    log_i scan(*log, curr_lsn, false);  // Backward scan

    // Allocate a (temporary) log record buffer for reading
    logrec_t* log_rec_buf=0;

    lsn_t lsn;
    while (scan.xct_next(lsn, log_rec_buf))  // This is backward scan
    {
        if ((lsn.data() < undo_lsn.data()) || (lsn_t::null == lsn.data()))
        {
            // We are done with the backward scan, break out
            break;
        }

        // Process the UNDO for each log record...
    }
*****************************************************/

        // This is an enhanced version of the UNDO phase based on the original
        // implementation of Shore-MT implementation using the heap data structure
        // The main difference is we populating the heap at the end of
        // Log Analysis phase instead of at the beginning of UNDO phase,
        // therefore we don't need to lock down the transaction table during UNDO

        w_ostrstream s;
        s << "restart undo_pass";
        (void) log_comment(s.c_str());

        if(heap.NumElements() > 0)
        {
            DBGOUT3(<<"Undoing  " << heap.NumElements() << " active transactions ");
            smlevel_0::errlog->clog << info_prio
                << "Undoing " << heap.NumElements() << " active transactions "
                << flushl;
        }

        // rollback the xct with the largest lsn, then the 2nd largest lsn,
        // and repeat until all xct's are rolled back completely

        xct_t*  xd;

        if (heap.NumElements() > 1)
        {
            // Only handle transaction which can be UNDOne:
            //     1. System transaction can roll forward instead
            //         currently all system transactions are single log, so
            //         they should not come into UNDO phase at all
            //     2. Compensation operations are REDO only, skipped in UNDO
            //         Log Analysis phase marked the associated transaction
            //         'undo_nxt' to null already, so they would be skipped here

            while (heap.First()->undo_nxt() != lsn_t::null)
            {
                xd = heap.First();

                // We do not have multiple log system transaction currently
                if (true == xd->is_sys_xct())
                {
                    // Nothing to do if single log system transaction
                    w_assert1(true == xd->is_single_log_sys_xct());
                    if (true == xd->is_single_log_sys_xct())
                    {
                        // We should not get here but j.i.c.
                        // Set undo_nxt to NULL so it cannot be rollback
                        xd->set_undo_nxt(lsn_t::null);
                        heap.ReplacedFirst();
                        continue;
                    }
                }

                DBGOUT3( << "Transaction " << xd->tid()
                    << " with undo_nxt lsn " << xd->undo_nxt()
                    << " rolling back to " << heap.Second()->undo_nxt()
                    );

                // Note that this rollback/undo for loser/in-flight transactions
                // which were marked as 'active' in the Log Analysis phase.
                // These transactions are marked 'active' in the transaction table
                // so the standard rooback/abort logic works.
                // We will open the store for new transactions after Log Analysis,
                // new incoming transaction should have different TID and not confused
                // with the loser (marked as active) transactions

                // It behaves as if it were a rollback to a save_point where
                // the save_point is 'undo_nxt' of the next transaction in the heap.
                // This is the same as a normal active transaction rolling back to
                // a specified save point.
                // In a loop it fetches the associated recovery log record using the current
                // transaction's 'undo_nxt' (follow the 'undo_nxt' chain), and then call
                // the 'undo' function of the recovery log record
                // It is being done this way so the roll back is in a strictly reverse
                // chronological order
                // Note that beause this is a 'roll back to save point' logic, locks are not
                // involved here

                // Special case: If there's only one transaction on the heap, there is no save_point
                // from the next transaction in the heap.  The rollbak would be via abort() (below)
                // which rolls back without save_point

                me()->attach_xct(xd);

#if 0 && W_DEBUG_LEVEL > 4
                {
                    lsn_t tmp = heap.Second()->undo_nxt();
                    if(tmp == lsn_t::null)
                    {
                        fprintf(stderr,
                                "WARNING: Rolling back to null lsn_t\n");
                        // Is this a degenerate xct that's still active?
                        // TODO WRITE A RESTART SCRIPT FOR THAT CASE
                    }
                }
#endif
                // Undo until the next-highest undo_nxt for an active
                // xct. If that xct's last inserted log record is a compensation,
                // the compensated-to lsn will be the lsn we find -- just
                // noted that for the purpose of deciphering the log...

                W_COERCE( xd->rollback(heap.Second()->undo_nxt()) );
                me()->detach_xct(xd);

                w_assert9(xd->undo_nxt() < heap.Second()->undo_nxt()
                        || xd->undo_nxt() == lsn_t::null);

                heap.ReplacedFirst();
            }
        }
        // Unless we have only one transaction in the heap, at this point all xct
        // are completely rolled back in a strictly reverse chronological order
        // (no more undo for those transactions)

        while (heap.NumElements() > 0)
        {
            // For all the loser transactions in the heap
            // destroy them from the transaction table

            xd = heap.RemoveFirst();

            // Note that all transaction has been rolled back, excpet a special case
            // where there was only one transaction in the heap, in such case the
            // actual rollback will happen here

            me()->attach_xct(xd);

            w_assert9(xd->undo_nxt() == lsn_t::null || heap.NumElements() == 0);

            DBGOUT3( << "Transaction " << xd->tid()
                    << " is rolled back: aborting it now " );

            // Abort the transaction, this is using the standard transaction abort logic,
            // which release locks (which was not involved in the roll back to save point operation),
            // generate an end transaction log record if any log has been generated by
            // this transaction (i.e. compensation records), and change state accordingly
            // Because we are using the standard abort logic, all the in-flight /loser transactions
            // were marked as 'active' so abort() works correctly
            W_COERCE( xd->abort() );

            delete xd;
        }

        w_assert1(0 == heap.NumElements());
        {
            w_base_t::base_stat_t f = GET_TSTAT(log_fetches);
            w_base_t::base_stat_t i = GET_TSTAT(log_inserts);
            smlevel_0::errlog->clog << info_prio
                << "Undo_pass: "
                << f << " log_fetches, "
                << i << " log_inserts " << flushl;
        }

        // Force a recovery log flush, this would harden the log records
        // generated by compensation operations
        W_COERCE( log->flush_all() );
    }
    return;
}

//*********************************************************************
// restart_m::redo_concurrent_pass()
//
// Function used when system is opened after Log Analysis phase
// while concurrent user transactions are allowed during REDO and UNDO phases
//
// Concurrent can be done through two differe logics:
//     Commit_lsn:   use_concurrent_commit_restart()    <-- Milestone 2
//     Lock:              use_concurrent_lock_restart()        <-- Milestone 3
//
// REDO is performed using one of the following:
//    Log driven:      use_redo_log_restart()                         <-- Milestone 1 default, see redo_pass
//    Page driven:    use_redo_page_restart()                      <-- Milestone 2, minimal logging
//    Page driven:    use_redo_full_logging_restart()             <-- Milestone 2, full logging
//    Demand driven:     use_redo_demand_restart()            <-- Milestone 3
//    Mixed driven:   use_redo_mix_restart()                        <-- Milestone 4
//    ARIES:            same as mixed mode except late open   <-- Milestone 5
//*********************************************************************
void restart_m::redo_concurrent_pass()
{
    if (true == use_serial_restart())
    {
        W_FATAL_MSG(fcINTERNAL, << "REDO phase, restart_m::redo_concurrent_pass() is valid for concurrent operation only");
    }

    FUNC(restart_m::redo_concurrent_pass);

    // REDO has no difference between commit_lsn and lock_acquisition
    // The main difference is from user transaction side to detect conflict
    w_assert1((true == use_concurrent_commit_restart()) || (true == use_concurrent_lock_restart()));

    if (true == use_redo_log_restart())
    {
        // M2, alternative pass using log scan REDO
        // Use the same redo_pass function for log driven REDO phase
        if (0 != smlevel_0::in_doubt_count)
        {
            // Need the REDO operation only if we have in_doubt pages in buffer pool
            // Do not change the smlevel_0::operating_mode, because the system
            // is opened for concurrent txn already

            smlevel_0::errlog->clog << info_prio << "Redo ..." << flushl;

            // Current log lsn is for validation purpose during REDO phase, also the stopping
            // point for forward scan
            lsn_t curr_lsn = log->curr_lsn();
            DBGOUT3(<<"starting REDO at " << smlevel_0::redo_lsn << " end_logscan_lsn " << curr_lsn);
            redo_log_pass(smlevel_0::redo_lsn, curr_lsn, smlevel_0::in_doubt_count);

            // Concurrent txn would generate new log records so the curr_lsn could be different
        }
    }
    else if (true == use_redo_page_restart() || true == use_redo_full_logging_restart())
    {
        // M2, page driven REDO
        _redo_page_pass();
    }
    else if (true == use_redo_demand_restart())
    {
        if ((ss_m::shutting_down) && (ss_m::shutdown_clean))
        {
            // During a clean shutdown, it is okay to call child thread REDO
            _redo_page_pass();
        }
        else
        {
            // M3, On-demand Single-Page-Recovery, should not get here
            W_FATAL_MSG(fcINTERNAL, << "REDO phase, on-demand REDO should not come to Restart thread");
        }
    }
    else if (true == use_redo_mix_restart())
    {
        // M4/M5 mixed mode REDO, start the page driven REDO
        _redo_page_pass();
    }
    else
    {
        W_FATAL_MSG(fcINTERNAL, << "REDO phase, missing execution mode setting for REDO");
    }

    return;
}

//*********************************************************************
// restart_m::undo_concurrent_pass()
//
// Function used when system is opened after Log Analysis phase
// while concurrent user transactions are allowed during REDO and UNDO phases
//
// Concurrent can be done through two differe logics:
//     Commit_lsn:         use_concurrent_commit_restart()   <-- Milestone 2
//     Lock:                    use_concurrent_lock_restart()       <-- Milestone 3
//
// UNDO is performed using one of the following:
//    Reverse driven:      use_undo_reverse_restart()           <-- Milestone 1 default, see undo_pass
//    Transaction driven: use_undo_txn_restart()                  <-- Milestone 2
//    Demand driven:     use_undo_demand_restart()            <-- Milestone 3
//    Mixed driven:   use_undo_mix_restart()                        <-- Milestone 4
//    ARIES:            same as mixed mode except late open   <-- Milestone 5
//*********************************************************************
void restart_m::undo_concurrent_pass()
{
    if (true == use_serial_restart())
    {
        W_FATAL_MSG(fcINTERNAL, << "UNDO phase, restart_m::undo_concurrent_pass() is valid for concurrent operation only");
    }

    FUNC(restart_m::undo_concurrent_pass);

    // UNDO behaves differently between commit_lsn and lock_acquisition
    //     commit_lsn:      no lock operations
    //     lock acquisition: release locks
    // The main difference is from the user transaction side to detect conflicts
    w_assert1((true == use_concurrent_commit_restart()) || (true == use_concurrent_lock_restart()));

    // If use_concurrent_lock_restart(), locks are acquired during Log Analysis phase
    // and release during UNDO phase
    // The implementation of UNDO phases (both txn driven and reverse drive) are using
    // standand transaction abort logic (and transaction rollback logic is reverse driven UNDO)
    // therefore the implementation took care of the lock release already

    if (true == use_undo_reverse_restart())
    {
        // M2, alternative pass using reverse UNDO
        // Use the same undo_pass function for reverse UNDO phase
        // callee must build the heap itself
        CmpXctUndoLsns  cmp;
        XctPtrHeap      heap(cmp);
        undo_reverse_pass(heap, log->curr_lsn().data(), smlevel_0::redo_lsn);  // Input LSNs are not used currently
    }
    else if (true == use_undo_txn_restart())
    {
        // M2, transaction driven
        _undo_txn_pass();
    }
    else if (true == use_undo_demand_restart())
    {
        if ((ss_m::shutting_down) && (ss_m::shutdown_clean))
        {
            // During a clean shutdown, it is okay to call child thread UNDO
            _redo_page_pass();
        }
        else
        {
            // M3, On-demand UNDO, should not get here
            W_FATAL_MSG(fcINTERNAL, << "UNDO phase, on-demand UNDO should not come to Restart thread");
        }
    }
    else if (true == use_undo_mix_restart())
    {
        // M4/M5 mixed mode UNDO, start the transaction driven UNDO
        _undo_txn_pass();
    }
    else
    {
        W_FATAL_MSG(fcINTERNAL, << "UNDO phase, missing execution mode setting for UNDO");
    }

    return;
}

//*********************************************************************
// restart_m::_redo_page_pass()
//
// Function used when system is opened after Log Analysis phase
// while concurrent user transactions are allowed during REDO and UNDO phases
//
// Page driven REDO phase, it handles both commit_lsn and lock acquisition
//*********************************************************************
void restart_m::_redo_page_pass()
{
    // REDO behaves the same between commit_lsn and lock_acquisition
    //     commit_lsn:      no lock operations
    //     lock acquisition: locks acquired during Log Analysis and release in UNDO
    //                             no lock operations during REDO

    w_assert1((true == use_concurrent_commit_restart()) || (true == use_concurrent_lock_restart()));

    // If no in_doubt page in buffer pool, then nothing to process
    if (0 == smlevel_0::in_doubt_count)
    {
        DBGOUT3(<<"No in_doubt page to redo");
        return;
    }
    else
    {
        DBGOUT3( << "restart_m::_redo_page_pass() - Number of in_doubt pages: " << smlevel_0::in_doubt_count);
    }

// TODO(Restart)... performance
DBGOUT1(<<"Start child thread REDO phase");

    w_ostrstream s;
    s << "restart concurrent redo_page_pass";
    (void) log_comment(s.c_str());

    w_rc_t rc = RCOK;
    bf_idx root_idx = 0;

    // Count of blocks/pages in buffer pool
    bf_idx bfsz = bf->get_block_cnt();
    DBGOUT3( << "restart_m::_redo_page_pass() - Number of block count: " << bfsz);

    // Loop through the buffer pool pages and look for in_doubt pages
    // which are dirty and not loaded into buffer pool yet
    // Buffer pool loop starts from block 1 because 0 is never used (see Bf_tree.h)
    // Based on the free list implementation in buffer pool, index is same
    // as _buffer and _control_blocks, zero means no link.
    // Index 0 is always the head of the list (points to the first free block
    // or 0 if no free block), therefore index 0 is never used.

    // Note: Page driven REDO is using Single-Page-Recovery.
    // When Single-Page-Recovery is used for page recovery during normal operation (using parent page),
    // the implementation has assumptions on 'write-order-dependency (WOD)'
    // for the following operations:
    //     btree_foster_merge_log: when recoverying foster parent (dest),
    //                                          assumed foster child (src) is not recovered yet
    //     btree_foster_rebalance_log:  when recoverying foster-child (dest),
    //                                          assured foster parent (scr) is not recovered yet
    // These WODs are not followed during page driven REDO recovery, because
    // the REDO operation is going through all in_doubt pages to recover in_doubt
    // pages one by one, it does not understand nor obey the foster B-tree
    // parent-child relationship.
    // Therefore special logic must be implemented in the 'redo' functions of
    // these log records (Btree_logrec.cpp) when WOD is not being followed

    // In milestone 2, two solutions have been implemented in page driven REDO to handle
    // b-tree rebalance and merge operations:
    // Minimal logging: system transaction log (single log) is used for the entire operation.
    // Full logging: log all record movements, while btree_foster_merge_log and btree_foster_rebalance_log
    //     log records are used to set page fence keys during the REDO operations.
    //
    // For log scan driven REDO operation, we will continue using minimal logging.
    bf_idx current_page;
    for (current_page = 1; current_page < bfsz; ++current_page)
    {
        // Loop through all pages in buffer pool and redo in_doubt pages
        // In_doubt pages could be recovered in multiple situations:
        // 1. REDO phase (this function) to load the page and calling Single-Page-Recovery to recover
        //     page context
        // 2. Single-Page-Recovery operation is using recovery log redo function to recovery
        //     page context, which would trigger a new page loading if the recovery log
        //     has multiple pages (foster merge, foster re-balance).  In such case, the
        //     newly loaded page (via _fix_nonswizzled) would be recovered by nested Single-Page-Recovery,
        //     and it will not be recovered by REDO phase (this function) directly
        // 3. By Single-Page-Recovery triggered by concurrent user transaction - on-demand REDO (M3)

        rc = RCOK;

        bf_tree_cb_t &cb = smlevel_0::bf->get_cb(current_page);
        // Need to acquire traditional EX latch for each page, it is to
        // protect the page from concurrent txn access
        // WAIT_IMMEDIATE to prevent deadlock with concurrent user transaction
        w_rc_t latch_rc = cb.latch().latch_acquire(LATCH_EX, WAIT_IMMEDIATE);
        if (latch_rc.is_error())
        {
            // If failed to acquire latch (e.g., timeout)
            // Page (m2): concurrent txn does not load page, restart should be able
            //                  to acquire latch on a page if it is in_doubt
            // Demand (m3): only concurrent txn can load page, this function
            //                       should not get executed
            // Mixed (m4): potential conflict because both restart and user transaction
            //                    can load and recover a page
            // ARIES (m5): system is not open, restart should be able to acquire latch
            //                    can load and recover a page
            if (true == use_redo_mix_restart())
            {
                // Mixed mode and not able to latch this page
                // Eat the error and skip this page. rely on concurrent transaction
                // to trigger on_demand REDO
                continue;
            }
            else
            {
                // M2, REDO is being done by the restart child thread only
                // Unable to acquire write latch, it should not happen if page was in_doubt
                // but it could happen if page was not in_doubt
                DBGOUT1 (<< "Error when acquiring LATCH_EX for a buffer pool page. cb._pid_shpid = "
                         << cb._pid_shpid << ", rc = " << latch_rc);

                // Only raise error if page was in_doubt
                if ((cb._in_doubt))
                {
                    // Try latch again
                    latch_rc = cb.latch().latch_acquire(LATCH_EX, WAIT_SPECIFIED_BY_THREAD);
                    if (latch_rc.is_error())
                        W_FATAL_MSG(fcINTERNAL, << "REDO (redo_page_pass()): unable to EX latch a buffer pool page ");
                }
                else
                    continue;
            }
        }

        if ((cb._in_doubt))
        {
            // This is an in_doubt page which has not been loaded into buffer pool memory
            // Make sure it is in the hashtable already
            PageID key = cb._pid_shpid;
            bf_idx idx = smlevel_0::bf->lookup_in_doubt(key);
            if (0 == idx)
            {
                if (cb.latch().held_by_me())
                    cb.latch().latch_release();

                // In_doubt page but not in hasktable, this should not happen
                W_FATAL_MSG(fcINTERNAL, << "REDO (redo_page_pass()): in_doubt page not in hash table");
            }
            DBGOUT3( << "restart_m::_redo_page_pass() - in_doubt page idx: " << idx);

            // Okay to load the page from disk into buffer pool memory
            // Load the initial page into buffer pool memory
            // Because we are based on the in_doubt flag in buffer pool, the page could be
            // a virgin page, then nothing to load and just initialize the page.
            // If non-pvirgin page, load the page so we have the page_lsn (last write)
            //
            // Single-Page-Recovery API recover_single_page(p, page_lsn) requires target
            // page pointer in fixable_page_h format and page_lsn (last write to the page).
            // After the page is in buffer pool memory, we can use Single-Page-Recovery to perform
            // the REDO operation

            // After REDO, make sure reset the in_doubt and dirty flags in cb, and make
            // sure hashtable entry is fine
            // Note that we are holding the page latch now, check with Single-Page-Recovery to make sure
            // it is okay to hold the latch

            fixable_page_h page;
            bool virgin_page = false;
            bool corrupted_page = false;

            PageID shpid = cb._pid_shpid;
            StoreID store = cb._store_num;

            // Get the last write lsn on the page, this would be used
            // as emlsn for Single-Page-Recovery if virgin or corrupted page
            // Note that we were overloading cb._dependency_lsn for
            // per page last write lsn in Log Analysis phase until
            // the page context is loaded into buffer pool (REDO), then
            // cb._dependency_lsn will be used for its original purpose
            lsn_t emlsn = cb._dependency_lsn;

            // Try to load the page into buffer pool using information from cb
            // if detects a virgin page, deal with it
            // Special case: the page is a root page which exists on disk, it was pre-loaded
            //                     during device mounting (_preload_root_page).
            //                     We will reload the root page here but not register it to the
            //                     hash table (already registered).  Use the same logic to fix up
            //                     page cb, it does no harm.
            DBGOUT3 (<< "REDO phase, loading page from disk, page = " << shpid);

            // If past_end is true, the page does not exist on disk and the buffer pool page
            // has been zerod out
            rc = smlevel_0::bf->load_for_redo(idx, shpid);

            if (rc.is_error())
            {
                if (eBADCHECKSUM == rc.err_num())
                {
                    // We are using Single-Page-Recovery for REDO, if checksum is
                    // incorrect, make sure we force a Single-Page-Recovery REDO
                    // Do not raise error here
                    DBGOUT3 (<< "REDO phase, corrupted page, page = " << shpid);
                    corrupted_page = true;
                }
                else
                {
                    if (cb.latch().held_by_me())
                        cb.latch().latch_release();

                    // All other errors
                    W_FATAL_MSG(fcINTERNAL,
                                << "Failed to load physical page into buffer pool in REDO phase, page: "
                                << shpid << ", RC = " << rc);
                }
            }

            // Now the physical page is in memory and we have an EX latch on it
            // In this case we are not using fixable_page_h::fix_direct() because
            // we have the idx, need to manage the in_doubt and dirty flags for the page
            // and we have loaded the page already
            // 0. Assocate the page to fixable_page_h, swizzling must be off
            // 1. Use Single-Page-Recovery to carry out REDO operations using the last write lsn,
            //     including regular page, corrupted page and virgin page

            // Associate this buffer pool page with fixable_page data structure
            // PageID: Store ID (volume number + store number) + page number (4+4+4)
            // Re-construct the lpid using several fields in cb
            if (true == use_redo_full_logging_restart())
            {
                // Use full logging, page is buffer pool managed
                W_COERCE(page.fix_recovery_redo(idx, shpid));
            }
            else
            {
                // Use minimal logging, this is for M2/M4/M5
                // page is not buffer pool managed before Single-Page-Recovery
                // only mark the page as buffer pool managed after Single-Page-Recovery
                W_COERCE(page.fix_recovery_redo(idx, shpid, false /* managed*/));
            }

            // CS: this replaces the old past_end flag on load_for_redo
            virgin_page = page.pid() == 0;

            // We rely on pid/tag set correctly in individual redo() functions
            // set for all pages, both virgin and non-virgin
            page.get_generic_page()->pid = shpid;
            page.get_generic_page()->tag = t_btree_p;

            if (true == virgin_page)
            {
                // If virgin page, set the vol, store and page in cb again
                cb._store_num = store;
                cb._pid_shpid = shpid;

                // Need the last write lsn for Single-Page-Recovery, but this is a virgin page and no
                // page context (it does not exist on disk, therefore the page context
                // in memory has been zero'd out), we cannot retrieve the last write lsn
                // from page context.  Set the page lsn to NULL for Single-Page-Recovery and
                // set the emlsn based on information gathered during Log Analysis
                // Single-Page-Recovery will scan log records and collect logs based on page ID,
                // and then redo all associated records

                DBGOUT3(<< "REDO (redo_page_pass()): found a virgin page" <<
                        ", using latest durable lsn for Single-Page-Recovery emlsn and NULL for last write on the page, emlsn = "
                        << emlsn);
                page.set_lsns(lsn_t::null);  // last write lsn
            }
            else if (true == corrupted_page)
            {
                // With a corrupted page, we are not able to verify the correctness of
                // last write lsn on page, so set it to NULL
                // Set the emlsn based on information gathered during Log Analysis
                DBGOUT3(<< "REDO (redo_page_pass()): found a corrupted page" <<
                        ", using latest durable lsn for Single-Page-Recovery emlsn and NULL for last write on the page, emlsn = "
                        << emlsn);
                page.set_lsns(lsn_t::null);  // last write lsn
            }

            // Use Single-Page-Recovery to REDO all in_doubt pages, including virgin and corrupted pages
            w_assert1(page.pid() == shpid);
            w_assert1(page.is_fixed());

            // Both btree_norec_alloc_log and tree_foster_rebalance_log are multi-page
            // system transactions, the 2nd page is the foster child and the page
            // gets initialized as an empty child page during 'redo'.
            // Single-Page-Recovery must take care of these cases

            // page.lsn() is the last write to this page (on disk version)
            // not necessary the actual last write (if the page was not flushed to disk)
            if (emlsn != page.lsn())
            {
                // page.lsn() is different from last write lsn recorded during Log Analysis
                // must be either virgin or corrupted page

                if ((false == virgin_page) && (false == corrupted_page))
                {
                    DBGOUT3(<< "REDO (redo_page_pass()): page lsn != last_write lsn, page lsn: " << page.lsn()
                            << ", last_write_lsn: " << emlsn);
                }
                // Otherwise, we need to recover this page on-page last write to emlsn
                // If we set the page LSN to NULL, it would force a complete recovery
                // from page allocation (if no backup), which is not necessary
                //    page.set_lsns(lsn_t::null);  // set last write lsn to null to force a complete recovery
            }

            // Using Single-Page-Recovery for the REDO operation, which is based on
            // page.pid(), page.vol(), page.pid() and page.lsn()
            // Call Single-Page-Recovery API
            //   page - fixable_page_h, the page to recover
            //   emlsn - last write to the page, if the page
            //   actual_emlsn - we have the last write lsn from log analysis, it is
            //                          okay to verify the emlsn even if this is a
            //                          virgin or corrupted page
            DBGOUT3(<< "REDO (redo_page_pass()): Single-Page-Recovery with emlsn: " << emlsn << ", page idx: " << idx);
            // Signal this page is being accessed by recovery
            // Single-Page-Recovery operation does not hold latch on the page to be recovered, because
            // it assumes the page is private until recovered.  It is not the case during
            // recovery.  It is caller's responsibility to hold latch before accessing Single-Page-Recovery
            page.set_recovery_access();
            W_COERCE(smlevel_0::recovery->recover_single_page(page,    // page to recover
                                                              emlsn,   // emlsn which is the end point of recovery
                                                              true));  // from_lsn because this is not a corrupted page
            page.clear_recovery_access();

            if (false == use_redo_full_logging_restart())
            {
                // Use minimal logging
                // Mark the page as buffer pool managed after Single-Page-Recovery REDO
                W_COERCE(page.fix_recovery_redo_managed());
            }

            // After the page is loaded and recovered (Single-Page-Recovery), the page context should
            // have the last-write lsn information (not in cb).
            // If no page_lsn (last write) in page context, it can only happen if it was a virgin
            // or corrupted page, and Single-Page-Recovery did not find anything in backup and recovery log
            // Is this a valid scenario?  Should this happen, there is nothing we can do because
            // we don't have anything to recover from
            // 'recover_single_page' should debug assert on page.lsn() == emlsn already
            if (lsn_t::null == page.lsn())
            {
                DBGOUT3(<< "REDO (redo_page_pass()): nothing has been recovered by Single-Page-Recovery for page: " << idx);
            }

            // The _rec_lsn in page cb is the earliest lsn which made the page dirty
            // the _rec_lsn (earliest lns) must be earlier than the page_lsn
            // (last write to this page)
            if (cb._rec_lsn > page.lsn().data())
                cb._rec_lsn = page.lsn().data();

            // Done with REDO of this page, turn the in_doubt flag into the dirty flag
            // also clear cb._dependency_lsn which was overloaded for last write lsn
            smlevel_0::bf->in_doubt_to_dirty(idx);        // In use and dirty

            if ((0 == root_idx) && (true == use_redo_delay_restart()))
            {
                // For testing purpose, if we need to sleep during REDO
                // sleep after we recovered the root page (which is needed for tree traversal)

                // Get root-page index if we don't already had it
                root_idx = smlevel_0::bf->get_root_page_idx(shpid);
            }
        }
        else
        {
            // If page in_doubt bit is not set, ignore it
        }

        // Release EX latch before moving to the next page in buffer pool
        if (cb.latch().held_by_me())
            cb.latch().latch_release();

        if ((current_page == root_idx) && (true == use_redo_delay_restart()))
        {
            // Just re-loaded the root page

            // For concurrent testing purpose, delay the REDO
            // operation so user transactions can encounter access conflicts
            // Note the sleep is after REDO processed the in_doubt root page
            DBGOUT3(<< "REDO (redo_page_pass()): sleep after REDO on root page" << root_idx);
            g_me()->sleep(wait_interval); // 1 second, this is a very long time
        }
    }

    // Done with REDO phase

    // Take a synch checkpoint after REDO phase only if there were REDO work
    smlevel_0::chkpt->synch_take();

    return;
}

//*********************************************************************
// restart_m::_undo_txn_pass()
//
// Function used when system is opened after Log Analysis phase
// while concurrent user transactions are allowed during REDO and UNDO phases
// The function could be used for serialized operation with some minor work
//
// Transaction driven UNDO phase, it handles both commit_lsn and lock acquisition
//*********************************************************************
void restart_m::_undo_txn_pass()
{
    // UNDO behaves differently between commit_lsn and lock_acquisition
    //     commit_lsn:      no lock operations
    //     lock acquisition: locks acquired during Log Analysis and release in UNDO

    w_assert1((true == use_concurrent_commit_restart()) || (true == use_concurrent_lock_restart()));

    // If nothing in the transaction table, then nothing to process
    if (0 == xct_t::num_active_xcts())
    {
        DBGOUT3(<<"No loser transaction to undo");
        return;
    }

    if(true == use_undo_delay_restart())
    {
        // For concurrent testing purpose, delay the UNDO
        // operation so the user transactions can hit conflicts
        g_me()->sleep(wait_interval); // 1 second, this is a very long time
    }

    w_ostrstream s;
    s << "restart concurrent undo_txn_pass";
    (void) log_comment(s.c_str());

    // Loop through the transaction table and look for loser txn
    // Do not lock the transaction table when looping through entries
    // in transaction table

    // TODO(Restart)... This logic works while new transactions are
    // coming in, because the current implementation of transaction
    // table is inserting new transactions into the beginning of the
    // transaction table, so they won't affect the on-going loop operation
    // Also because new transaction is always insert into the beginning
    // of the transaction table, so when applying UNDO, we are actually
    // undo the loser transactions in the reverse order, which is the
    // order of execution we need

    xct_i iter(false); // not locking the transaction table list
    xct_t* xd = 0;
    xct_t* curr = 0;
    xd = iter.next();
    while (xd)
    {
        DBGOUT3( << "Transaction " << xd->tid()
                 << " has state " << xd->state() );

        // Acquire latch before checking the loser status
        // latch is not needed for traditional restart (M2) but
        // required for mixed mode due to concurrent transaction
        // on_demand UNDO
        try
        {
            w_rc_t latch_rc = xd->latch().latch_acquire(LATCH_EX, WAIT_FOREVER);
            if (latch_rc.is_error())
            {
                // Not able to acquire latch on this transaction for some reason
                if (true == use_undo_mix_restart())
                {
                    // If mixed mode, it is possible and valid if failed to acquire
                    // latch on a transaction, because a concurrent user transaction
                    // might be checking or triggered a rollback on this transaction
                    // (if it is a loser transaction)
                    // Eat the error and skip this transaction, if thi sis a loser transaction
                    // rely on concurrent transaction to rollback this loser transaction
                    xd = iter.next();
                    continue;
                }
                else
                {
                    // Traditional UNDO, not able to acquire latch
                    // continue the processing of this transaction because
                    // latch is optional in this case
                }
            }
        }
        catch (...)
        {
            // It is possible a race condition occurred, the transaction object is being
            // destroyed, go to the next transaction
            xd = iter.next();
            continue;
        }

        if ((xct_t::xct_active == xd->state()) && (true == xd->is_loser_xct())
             && (false == xd->is_loser_xct_in_undo()))
        {
            // Found a loser txn
            // Mark this loser transaction as in undo first
            // and then release latch
            xd->set_loser_xct_in_undo();

            if (xd->latch().held_by_me())
                xd->latch().latch_release();

            // Prepare to rollback this loser transaction
            curr = iter.curr();
            w_assert1(curr);

            // Advance to the next transaction first
            xd = iter.next();

            // Only handle transaction which can be UNDOne:
            //   1. System transaction can roll forward instead
            //      currently all system transactions are single log, so
            //     they should not come into UNDO phase at all
            //   2. Compensation operations are REDO only, skipped in UNDO
            //     Log Analysis phase marked the associated transaction
            //     'undo_nxt' to null already, so they would be skipped here

            if (lsn_t::null != curr->undo_nxt())  // #2 above
            {
                if (true == curr->is_sys_xct())   // #1 above
                {
                    // We do not have multiple log system transaction currently
                    // Nothing to do if single log system transaction
                    w_assert1(true == curr->is_single_log_sys_xct());

                    // We should not get here but j.i.c.
                    // Set undo_nxt to NULL so it cannot be rollback
                    curr->set_undo_nxt(lsn_t::null);
                }
                else
                {
                    // Normal transaction

                    DBGOUT3( << "Transaction " << curr->tid()
                             << " with undo_nxt lsn " << curr->undo_nxt());

                    // Abort the transaction, this is using the standard transaction abort logic,
                    // which release locks if any were acquired for the loser transaction
                    // generate an end transaction log record if any log has been generated by
                    // this transaction (i.e. compensation records), and change state accordingly
                    // All the in-flight /loser transactions were marked as 'active' so
                    // the standard abort() works correctly
                    //
                    // Note the 'abort' logic takes care of lock release if any, so the same logic
                    // works with both use_concurrent_lock_restart() and use_concurrent_commit_restart()
                    // no special handling (lock release) in this function
                    //     use_concurrent_commit_restart(): no lock acquisition
                    //     use_concurrent_lock_restart(): locks acquired during Log Analysis phase

                    me()->attach_xct(curr);
                    W_COERCE( curr->abort() );

                    // Then destroy the loser transaction
                    delete curr;
                }
            }
            else
            {
                // Loser transaction but no undo_nxt, must be a compensation
                // operation, nothing to undo
            }
        }
        else
        {
            if (xd->latch().held_by_me())
                xd->latch().latch_release();

            // All other transaction, ignore and advance to the next txn
            xd = iter.next();
        }
    }

    // All loser transactions have been taken care of now
    // Force a recovery log flush, this would harden the log records
    // generated by compensation operations

    W_COERCE( log->flush_all() );

    // TODO(Restart)... an optimization idea, while we roll back and
    // delete each loser txn from transaction table, we could adjust
    // commit_lsn accordingly, to open up for more user transactions
    // this optimization is not implemented

    // Set commit_lsn to NULL so all concurrent user txn are allowed
    // also once 'recovery' is completed, user transactions would not
    // validate against commit_lsn anymore.

    smlevel_0::commit_lsn = lsn_t::null;

    // Done with UNDO phase

    // Take a synch checkpoint after UNDO phase but before existing the Recovery operation
    // Checkpoint will be taken only if there were UNDO work
    smlevel_0::chkpt->synch_take();

    return;
}


//*********************************************************************
// Main body of the child thread restart_thread_t for Recovery process
// Only used if system is in concurrent recovery mode, while the system was
// opened after Log Analysis phase to allow concurrent user transactions
//*********************************************************************
void restart_thread_t::run()
{
    // Body of the restart thread to carry out the REDO and UNDO work
    // When this function returns, the child thread will be destroyed

    DBGOUT1(<< "restart_thread_t: Starts REDO and UNDO tasks");

    struct timeval tm_before;
    struct timeval tm_after;
    struct timeval tm_done;

    if (true == restart_m::use_aries_restart())
    {
        // No REDO, only UNDO
        gettimeofday(&tm_after, NULL);
    }
    else
    {
        // Both REDO and UNDO
        gettimeofday(&tm_before, NULL);

        // REDO, call back to restart_m to carry out the concurrent REDO
        working = smlevel_0::t_concurrent_redo;
        smlevel_0::recovery->redo_concurrent_pass();

        gettimeofday(&tm_after, NULL);
// TODO(Restart)... Performance
        DBGOUT1(<< "**** Restart child thread REDO, elapsed time (milliseconds): "
                << (((double)tm_after.tv_sec - (double)tm_before.tv_sec) * 1000.0)
                + (double)tm_after.tv_usec/1000.0 - (double)tm_before.tv_usec/1000.0);
    }

    // UNDO, call back to restart_m to carry out the concurrent UNDO
    working = smlevel_0::t_concurrent_undo;
    smlevel_0::recovery->undo_concurrent_pass();

    // Done
    DBGOUT1(<< "restart_thread_t: Finished REDO and UNDO tasks");
    working = smlevel_0::t_concurrent_done;

    // Set commit_lsn to NULL which allows all concurrent transaction to come in
    // from now on (if using commit_lsn to validate concurrent user transactions)
    smlevel_0::commit_lsn = lsn_t::null;

    gettimeofday( &tm_done, NULL );
// TODO(Restart)... Performance
    DBGOUT1(<< "**** Restart child thread UNDO, elapsed time (milliseconds): "
            << (((double)tm_done.tv_sec - (double)tm_after.tv_sec) * 1000.0)
            + (double)tm_done.tv_usec/1000.0 - (double)tm_after.tv_usec/1000.0);

    return;
};


/*********************************************************************
 *
 *  friend operator<< for dirty page table
 *
 *********************************************************************/

/*****************************************************
//Dead code, comment out just in case we need to re-visit it in the future
// We are using the actual buffer pool to register in_doubt page during Log Analysis
// no longer using the special in-memory dirty page table for this purpose

ostream& operator<<(ostream& o, const dirty_pages_tab_t& s)
{
    o << " Dirty page table: " <<endl;
    for (dp_lsn_const_iterator it = s.dp_lsns().begin(); it != s.dp_lsns().end(); ++it) {
        dp_key_t key = it->first;
        lsndata_t rec_lsn = it->second;
        o << " Vol:" << dp_vid(key) << " Shpid:" << dp_shpid(key) << " lsn " << lsn_t(rec_lsn) << endl;
    }
    return o;
}

lsn_t dirty_pages_tab_t::min_rec_lsn()
{
    if (_validCachedMinRecLSN)  {
        return _cachedMinRecLSN;
    }  else  {
        lsndata_t l = lsndata_max;
        for (std::map<dp_key_t, lsndata_t>::const_iterator it = _dp_lsns.begin(); it != _dp_lsns.end(); ++it) {
            lsndata_t rec_lsn = it->second;
            if (l > rec_lsn && rec_lsn != lsndata_null) {
                l = rec_lsn;
            }
        }
        _cachedMinRecLSN = l;
        _validCachedMinRecLSN = true;
        return l;
    }
}
*****************************************************/

