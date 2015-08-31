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

            W_COERCE(bf->force_volume());
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
    for (uint i=0; i<v_chkpt.pid.size(); i++) {
        bf_idx idx = 0;
        w_rc_t rc = RCOK;

        rc = smlevel_0::bf->register_and_mark(idx, v_chkpt.pid[i],
                                                v_chkpt.store[i],
                                                v_chkpt.rec_lsn[i].data() /*first_lsn*/,
                                                v_chkpt.page_lsn[i].data() /*last_lsn*/,
                                                in_doubt_count);
        
        if (rc.is_error()) {
            // Not able to get a free block in buffer pool without evict, cannot continue
            W_FATAL_MSG(fcINTERNAL, << "Failed to record an in_doubt page in t_chkpt_bf_tab during Log Analysis" << rc);
        }
        w_assert1(0 != idx);
    }

    //Re-create transactions
    xct_t::update_youngest_tid(v_chkpt.youngest);
    for(uint i=0; i<v_chkpt.tid.size(); i++) {
        xct_t* xd = new xct_t(NULL,               // stats
                        WAIT_SPECIFIED_BY_THREAD, // default timeout value
                        false,                    // sys_xct
                        false,                    // single_log_sys_xct
                        v_chkpt.tid[i],
                        v_chkpt.last_lsn[i],      // last_LSN
                        v_chkpt.undo_nxt[i],      // next_undo
                        true);                    // loser_xct, set to true for recovery

        xd->set_first_lsn(v_chkpt.first_lsn[i]); // Set the first LSN of the in-flight transaction
        xd->set_last_lsn(v_chkpt.last_lsn[i]);   // Set the last lsn in the transaction

        // Loser transaction
        if (true == use_serial_restart())
            loser_heap.AddElementDontHeapify(xd);
    }
    if (true == use_serial_restart()) {
        loser_heap.Heapify();
    }

    //Re-acquire locks
    if(restart_with_lock) {
        for(uint i=0; i<v_chkpt.tid.size(); i++) {
            xct_t* xd = xct_t::look_up(v_chkpt.tid[i]);
            for(uint j=0; j<v_chkpt.lock_hash[i].size(); j++) {
                _re_acquire_lock(lock_heap, v_chkpt.lock_mode[i][j], v_chkpt.lock_hash[i][j], xd);
            }
        }
    }

    //Re-mount devices
    smlevel_0::vol->set_next_vid(v_chkpt.next_vid);
    for (uint i=0; i<v_chkpt.dev_paths.size(); i++) {
        smlevel_0::vol->sx_mount(v_chkpt.dev_paths[i].c_str(), false /* log */);
    }

    //Re-add backups
    for (uint i=0; i<v_chkpt.backup_vids.size(); i++) {
        smlevel_0::vol->sx_add_backup(v_chkpt.backup_vids[i],
                                      v_chkpt.backup_paths[i], false /* log */);
    }
}


/*********************************************************************
 *
 *  restart_m::analysis_pass_forward(master, redo_lsn, in_doubt_count, undo_lsn, loser_heap, commit_lsn, last_lsn)
 *
 *  Scan log forward from master_lsn. Insert and update buffer pool,
 *  insert transaction table.
 *  Compute redo_lsn.
 *  No lock acquisition
 *
 *  This function is used in all situations for M1 and M2, because system is not opened during
 *  Log Analysis phase
 *
 *********************************************************************/
void
restart_m::analysis_pass_forward(
    const lsn_t           master,          // In: Starting point for forward log scan
    lsn_t&                redo_lsn,        // Out: Starting point for REDO forward log scan (if used),
                                           //       which could be different from master
    uint32_t&             in_doubt_count,  // Out: Counter for in_doubt page count in buffer pool
    lsn_t&                undo_lsn,        // Out: Stopping point for UNDO backward log scan (if used)
    XctPtrHeap&           loser_heap,      // Out: Heap to record all the loser transactions,
                                           //       used only for reverse chronological order
                                           //       UNDO phase (if used)
    lsn_t&                commit_lsn,      // Out: Commit lsn for concurrent transaction (if used)
    lsn_t&                last_lsn         // Out: Last lsn in the recovery log before system crash
)
{
    FUNC(restart_m::analysis_pass_forward);

    // Actually turn off logging during Log Analysis phase, there is no possibility
    // to add new log records by accident during this phase
    AutoTurnOffLogging turnedOnWhenDestroyed;

    // redo_lsn will be used as the starting point for REDO forward log scan,
    // it should be the earliest LSN for all in_doubt pages, it is very likely this
    // LSN is earlier than the master LSN (begin checkpoint LSN).
    // Because we do not load the physical page during Log Analysis phase, we are
    // not able to retrieve _rec_lsn (initial dirty LSN) from each page, therefore we
    // have to rely on:
    // 1. minimum LSN recorded in the 'end checkpoint' log record
    // 2. If a newly allocated and formated page after the checkpoint, there must
    //     be a page format log record in the recovery log before any usage of the page.

    // Initialize redo_lsn, undo_lsn and last_lsn to 0 which is the smallest lsn
    redo_lsn = lsn_t::null;
    undo_lsn = lsn_t::null;
    last_lsn = lsn_t::null;

    // Change state first regardless whether we have work to do or not
    smlevel_0::operating_mode = smlevel_0::t_in_analysis;

    if (master == lsn_t::null)
    {
        // 'master'is  the LSN from the last completed checkpoint
        // It was identified from log_core::log_core()

        // The only possibility that we have a NULL as master lsn is due to a brand new
        // start (empty) of the engine, in such case, nothing to recover

        DBGOUT3( << "NULL master, nothing to analysis in Log Analysis phase");
        return;
    }

    // We have something to process for Log Analysis
    // initialize commit_lsn to a value larger than the current log lsn
    // this is to ensure we have the largest LSN value to begin with
    lsn_t max_lsn = log->curr_lsn() + 1;
    w_assert1(master < max_lsn);
    commit_lsn = max_lsn;

    // Initialize the in_doubt count
    in_doubt_count = 0;
    lsn_t begin_chkpt = lsn_t::null;

    // Did any device mounting occurred during the Log Analysis phase?
    // mount: for DBGOUT purpose to indicate any device was mounted
    bool mount = false;

    // The UNDO loser_heap must be empty initially
    w_assert1(0 == loser_heap.NumElements());

    // Open a forward scan starting from master (the begin checkpoint LSN from the
    // last completed checkpoint
    log_i         scan(*log, master);
    logrec_t*     log_rec_buf;
    lsn_t         lsn;

    lsn_t         theLastMountLSNBeforeChkpt;

    // Assert first record is Checkpoint Begin Log
    // and get last mount/dismount lsn from it
    {
        if (! scan.xct_next(lsn, log_rec_buf))
        {
            W_COERCE(scan.get_last_rc());
        }
        logrec_t&        r = *log_rec_buf;

        // The first record must be a 'begin checkpoint', otherwise we don't want to continue, error out
        if (r.type() != logrec_t::t_chkpt_begin)
        {
            DBGOUT1( << setiosflags(ios::right) << lsn
                     << resetiosflags(ios::right) << " R: " << r);
            W_FATAL_MSG(fcINTERNAL, << "First log record in Log Analysis is not a begin checkpoint log: " << r.type());
        }

        theLastMountLSNBeforeChkpt = *(lsn_t *)r.data();
        DBGOUT3( << "Last mount LSN from chkpt_begin: " << theLastMountLSNBeforeChkpt);
    }

    unsigned int cur_segment = 0;

    /*
     *  Number of complete chkpts handled.  Only the first
     *  chkpt is actually handled.  There may be a second
     *  complete chkpt due to a race condition between writing
     *  a chkpt_end record and updating the master lsn.  In other words,
     *  a chkpt_end log was hardened, but crash occurred before the master
     *  information was updated, therefore the master is the previous checkpoint,
     *  even if there is a newer completed checkpoint after the checkpoint
     *  recorded in master.
     *
     *  This is a valid scenario and need to be handled.  The log scan is based on
     *  the checkpoint recorded in master, ignoring other completed or incompleted
     *  checkpoints.
     */
    int num_chkpt_end_handled = 0;

    // CS TODO -- dirty hack; see comments below
    char* devtab = NULL;
    char* bkptab = NULL;


    // At the beginning of the Recovery from a system crash, both the transaction table
    // and buffer pool should be initialized with the information from the specified checkpoint,
    // and then modified according to the following log records in the recovery log

    // The buffer 'log_rec_buf' for log record was not allocated in caller, therefore it is
    // using a local buffer in the log buffer.  Do not nest the log scan iternator because it
    // would overwrite the data in log_rec_buf
    // A mutex (partition lock) is being held during fetch of log record, there is
    // no concurrency among fetch requests so it is safe and simple to use a local buffer
    // in the log buffer implementation
    while (scan.xct_next(lsn, log_rec_buf))
    {
        logrec_t& r = *log_rec_buf;

        // Scan next record
        DBGOUT3( << setiosflags(ios::right) << lsn
                  << resetiosflags(ios::right) << " A: " << r );

        // If LSN is not intact, stop now
        if (lsn != r.lsn_ck())
            W_FATAL_MSG(fcINTERNAL, << "Bad LSN from recovery log scan: " << lsn);

        if(lsn.hi() != cur_segment)
        {
            // Record the current segment log in partition
            cur_segment = lsn.hi();
            smlevel_0::errlog->clog << info_prio
               << "Analyzing log segment " << cur_segment << flushl;
        }

        // Forward scan, update last_lsn which is the very last
        // lsn in Recovery log before the system crash
        // we use last_lsn in REDO Single-Page-Recovery if there is a corrupted page
        last_lsn = lsn;

        // CS: volume operations are SSX's without pid
        if (r.is_single_sys_xct() && !r.null_pid())
        {
            // We have a system transaction log record
            if (true == _analysis_system_log(r, lsn, in_doubt_count))
            {
                // Go to next log record
                continue;
            }
            else
            {
                // Failure occured, do not continue
                W_FATAL_MSG(fcINTERNAL, << "Failed to process a system transaction log record during Log Analysis, lsn = " << lsn);
            }
        }

        // We already ruled out all SSX logs. So we don't have to worry about
        // multi-page logs in the code below, because multi-page log only exist
        // in system transactions
        w_assert1(!r.is_multi_page());
        xct_t* xd = 0;

        // If log is transaction related, insert the transaction
        // into transaction table if it is not already there.
        if (!r.is_single_sys_xct() &&
                (r.tid() != tid_t::null) && ! (xd = xct_t::look_up(r.tid()))
                   && r.type()!=logrec_t::t_comment         // comments can be after xct has ended
                   && r.type()!=logrec_t::t_skip            // skip
                   && r.type()!=logrec_t::t_max_logrec)    // mark the end
        {
            DBGOUT3(<<"analysis: inserting tx " << r.tid() << " active ");
            xd = new xct_t(
                                NULL,                     // stats
                                WAIT_SPECIFIED_BY_THREAD, // default timeout value
                                false,                    // sys_xct
                                false,                    // single_log_sys_xct
                                r.tid(),                  // Use the tid from log record
                                lsn,                      // last LSN
                                r.xid_prev(),             // undo_nxt: r.xid_prev() is previous logrec
                                                          //of this xct stored in log record, since
                                                          // this is the first log record for this txn
                                                          // r.xid_prev() should be lsn_t::null
                                true);                    // loser_xct, set to true for recovery
            w_assert1(xd);
            if (r.tid() > xct_t::youngest_tid())
                xct_t::update_youngest_tid(r.tid());

            xd->set_last_lsn(lsn);                  // set the last lsn in the transaction
            w_assert1(lsn < log->curr_lsn());
            xd->set_first_lsn(max_lsn);             // initialize first lsn to a large value
            w_assert1( xd->tid() == r.tid() );
        }
        else
        {
            // No-op to transaction table

            // If Log record is not transaction related, we should not have
            // an entry in the transaction table

            // If the log record is transaction related and the entry already
            // existed in transaction table, 'xd' contains the existing transaction
            // entry at this point

            if (xd)
            {
                // Transaction exists in transaction table
                xd->set_last_lsn(lsn);                  // set the last lsn in the transaction
            }
        }

        // Process based on the type of the log record
        // Modify transaction table and buffer pool accordingly
        switch (r.type())
        {
        case logrec_t::t_chkpt_begin:
            // We already read passed the master checkpoint
            // If we hit another begin checkpoint log, it is either
            // incomplete or a 2nd completed checkpoint, ignore
            // all the logs related to this checkpoint
            // The way to do it is by checking'num_chkpt_end_handled'
            break;

        case logrec_t::t_chkpt_bf_tab:
            // Buffer pool dirty pages from checkpoint
            if (num_chkpt_end_handled == 0)
            {
                // Still processing the master checkpoint record.
                _analysis_ckpt_bf_log(r, in_doubt_count);
            }
            else
            {
                // Not from the master checkpoint, ignore
            }
            break;

        case logrec_t::t_chkpt_xct_lock:
            // Log record for per transaction lock information
            // Skip in forward Log Analysis log scan because forward scan
            // does not re-acquire locks

            break;

        case logrec_t::t_chkpt_xct_tab:
            // Transaction table entries from checkpoint
            if (num_chkpt_end_handled == 0)
            {
                // Still processing the master checkpoint record.
                // For each entry in the log,
                //         If the xct is not in xct tab, insert it.
                const chkpt_xct_tab_t* dp = (chkpt_xct_tab_t*) r.data();
                if (dp->youngest > xct_t::youngest_tid())
                    xct_t::update_youngest_tid(dp->youngest);
                for (uint i = 0; i < dp->count; i++)
                {
                    xd = xct_t::look_up(dp->xrec[i].tid);
                    if (!xd)
                    {
                        // Not found in the transaction table

                        // A potential race condition:
                        // The t_chkpt_xct_tab log record was generated by checkpoint, while
                        // checkpoint is a non-blocking operation and might take some time
                        // to finish the operation.
                        // It is possible that when the checkpoint log record was being gathered,
                        // a transaction was not ended and therefore the information was captured
                        // by the checkpoint.
                        // This transaction ended (generated a end transaction log record)
                        // before the corrresponding checkpoint log record was written out.
                        // In such case during the forward log scan, we would encounter the
                        // end transaction log record first, and then the checkpoint t_chkpt_xct_tab
                        // log record.  We need to make sure we do not mark the ended transaction
                        // as a loser transaction by accident, therefore leave the ended
                        // transaction in the transaction table until we are done with the log scan,
                        // and then cleanup all the ended transaction table at the end.

                        if (dp->xrec[i].state != xct_t::xct_ended)
                        {
                            // skip finished ones
                            xd = new xct_t(
                                        NULL,                     // stats
                                        WAIT_SPECIFIED_BY_THREAD, // default timeout value
                                        false,                    // sys_xct
                                        false,                    // single_log_sys_xct
                                        dp->xrec[i].tid,
                                        dp->xrec[i].last_lsn,     // last_LSN
                                        dp->xrec[i].undo_nxt,     // next_undo
                                        true);                    // loser_xct, set to true for recovery

                            // Set the first LSN of the in-flight transaction
                            xd->set_first_lsn(dp->xrec[i].first_lsn);

                            DBGOUT3(<<"add xct " << dp->xrec[i].tid
                                    << " state " << dp->xrec[i].state
                                    << " last lsn " << dp->xrec[i].last_lsn
                                    << " undo " << dp->xrec[i].undo_nxt
                                    << ", first lsn " << dp->xrec[i].first_lsn);
                            w_assert1(xd);
                        }
                    }
                    else
                    {
                       // Found in the transaction table, it must be marked as:
                       // loser transaction (active) - in-flight transaction during checkpoint
                       // ended transaction - transaction ended before the checkpoint finished
                       w_assert1((xct_t::xct_active == xd->state()) ||
                                 (xct_t::xct_ended == xd->state()));
                    }
                }
            }
            else
            {
                // Not from the master checkpoint, ignore
            }
            break;

        case logrec_t::t_chkpt_backup_tab:
            if (num_chkpt_end_handled == 0)
            {
                // Still processing the master checkpoint record.
                // _analysis_ckpt_dev_log(r, mount);

                // CS: dirty trick to fix mounts and backups during backward
                // log analysis, allowing them to appear in any order. This
                // code is temporary, since a proper fix should also apply to
                // the individual log records (see below).  CS TODO
                // r.redo(0);
                bkptab = new char[logrec_t::max_sz];
                memcpy(bkptab, &r, r.length());
            }
            break;

        case logrec_t::t_add_backup:
            r.redo(0);
            break;

            // CS TODO -- new method
            /* Perform all mounts and dismounts up to the minimum redo lsn,
            * so that the system has the right volumes mounted during
            * the redo phase.  the only time the this should  be redone is
            * when no dirty pages were in the checkpoint and a
            * mount/dismount occurs  before the first page is dirtied after
            * the checkpoint.  the case of the first dirty  page occuring
            * before the checkpoint is handled by undoing mounts/dismounts
            * back to the min dirty page lsn in the analysis_pass
            * after the log has been scanned.
            */

            // w_assert9(num_chkpt_end_handled > 0);
            // mount & dismount shouldn't happen during a check point
            // redo_lsn is initialized to NULL, and only set to the minimum lsn
            // from master 'end checkpoint' when we encounter it during log scan
            // Only redo, no undo for mount & dismount
            // if (lsn < redo_lsn)
            // {
            //     r.redo(0);

            //     if (logrec_t::t_mount_vol == r.type())
            //         mount = true;
            // }
            // break;

        case logrec_t::t_chkpt_end:
            if (num_chkpt_end_handled == 0)
            {
                // Retrieve the master, min_rec_lsn and min_txn_lsn from
                // the first (master) 'end checkpoint',
                // The minimum lsn of all buffer pool dirty or in_doubt pages
                // The REDO phase must start with the earliest LSN of all in_doubt pages
                // The master(begin_chkpt) should be the same as the master from caller
                // The minimum txn lsn is the earliest lsn for all in-flight transactions
                // The UNDO phase backward scan stops at the minimum txn lsn
                unsigned long i = sizeof(lsn_t);

                // GROT: stop gcc from
                // optimizing memcpy into something that
                // chokes on sparc due to misalignment
                // @todo: this is almost certainly obsolete?

                memcpy(&begin_chkpt, (lsn_t*) r.data(), i);
                memcpy(&redo_lsn, ((lsn_t*) r.data())+1, i);
                memcpy(&undo_lsn, ((lsn_t*) r.data())+2, i);

                if (master != begin_chkpt)
                    W_FATAL_MSG(fcINTERNAL,
                                << "Master from 'end checkpoint' is different from caller of Log Analysis");

                DBGOUT3(<<"t_chkpt_end log record: master=" << begin_chkpt
                        << " min_rec_lsn= " << redo_lsn
                        << " min_txn_lsn= " << undo_lsn);

                if(lsn == begin_chkpt)
                {
                    // Only used in mount/unmount related code, comment out (M1)
                    // w_assert9(l2 == dptab.min_rec_lsn());
                }
            }

#if W_DEBUG_LEVEL > 4
            if (num_chkpt_end_handled > 2)
            {
                /*
                 * We hope we do not encounter more than one complete chkpt.
                 * Unfortunately, we *can* crash between the flushing
                 * of a checkpoint-end record and the time we
                 * update the master record (move the pointer to the last
                 * checkpoint)
                 */
                smlevel_0::errlog->clog  << error_prio
                << "Warning: more than 2 complete checkpoints found! "
                <<flushl;
                /*
                 * comment out the following if you are testing
                 * a situation that involves a crash at the
                 * critical point
                 */
                // w_assert9(0);
            }

#endif

            // Done with the master checkpoint log records. Update 'num_chkpt_end_handled'
            // to avoid processing incomplete or extra completed checkpoints
            num_chkpt_end_handled++;
            break;

        case logrec_t::t_xct_freeing_space:

            // A t_xct_freeing_space log record is generated when entering
            // txn state 'xct_freeing_space' which is before txn commit or abort.
            // If system crashed before the final txn commit or abort occurred,
            // the recovery log does not know whether the txn should be
            // committed or abort, do not mark the txn to xct_ended when we
            // see this log record, so if we do not see another log record to indicate
            // txn commit or abort, the txn will be treated as a loser txn and
            // rollback during UNDO phase

            w_assert1(xct_t::xct_ended != xd->state());
            break;

        case logrec_t::t_xct_end_group:
            {
                // Do what we do for t_xct_end for each of the
                // transactions in the list, then drop through
                // and do it for the xct named in "xd" (the attached one)

                const xct_list_t* list = (xct_list_t*) r.data();
                int listlen = list->count;
                for(int i=0; i < listlen; i++)
                {
                    xd = xct_t::look_up(list->xrec[i].tid);
                    // If it's not there, it could have been a read-only xct?
                    w_assert0(xd);
                    // Now do exactly what's done below
                    //  Remove xct from xct tab
                    if ((xd->state() == xct_t::xct_freeing_space) ||
                        (xd->state() == xct_t::xct_aborting))
                    {
                        // was prepared in the master
                        // checkpoint, so the locks
                        // were acquired.  have to free them
                        me()->attach_xct(xd);
                        W_COERCE( lm->unlock_duration() );
                        me()->detach_xct(xd);
                    }

                    // Mark the txn as ended, safe to remove it from transaction table
                    if (xct_t::xct_ended != xd->state())
                        xd->change_state(xct_t::xct_ended);
                }
            }
            break;

        case logrec_t::t_xct_abort:
            // Transaction aborted before system crash.
            // Relying on the REDO phase to execute both the original
            // statements and the compensate statemetns, no UNDO
            // operation for this transaction therefore remove the aborted
            // transaction from transaction table
            w_assert1(xct_t::xct_ended != xd->state());

            // fall-through

        case logrec_t::t_xct_end:
            if ((xd->state() == xct_t::xct_freeing_space) ||
                (xd->state() == xct_t::xct_aborting))
            {
                // was prepared in the master
                // checkpoint, so the locks
                // were acquired.  have to free them
                me()->attach_xct(xd);
                W_COERCE( lm->unlock_duration() );
                me()->detach_xct(xd);
            }

            // Log record indicated this txn has ended or aborted
            // It is safe to remove it from transaction table
            if (xct_t::xct_ended != xd->state())
            {
                xd->change_state(xct_t::xct_ended);

                // Now we have reached the end of this transaction (commit or abort)
                // it is safe to remove this transaction from transaction table now.

                // Delete the transaction from transaction table because it
                // reduces the size of llinked-list transaction table and improve
                // the xct_t::look_up performance of the transaction table
                delete xd;
            }
            break;

        case logrec_t::t_compensate:
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
            // The rest of meanful log records, since we have created transaction already
            // we only care about if the log affect buffer pool here
            // A new txn would be created only if it did not exist already, one txn might
            // contain multiple log records

            {
                // Common stuff among forward and backward log scan
                _analysis_other_log(r, lsn, in_doubt_count, xd);
            }
            break;
        default:
            // We should only see the following log types and they are no-op, and we did not
            // create transaction for them either
            // t_comment
            // t_skip
            // t_max_logrec

            if (r.type()!=logrec_t::t_comment &&   // Comments
                !r.is_skip() &&                    // Marker for the end of partition
                r.type()!=logrec_t::t_max_logrec)  // End of log type
            {
                // Retrieve a log buffer which we don't know how to handle
                // Raise erroe
                W_FATAL_MSG(fcINTERNAL, << "Unexpected log record type from default: " << r.type());
            }
            break;
        }// switch
    }

    // CS TODO -- dirty hack (continuation)
    if (devtab) {
        ((logrec_t*) devtab)->redo(0);
        delete[] devtab;
    }
    if (bkptab) {
        ((logrec_t*) bkptab)->redo(0);
        delete[] bkptab;
    }

    // Read all the recovery logs, we should have a minimum LSN from the master checkpoint
    // at this point, which is where the REDO phase should start for the in_doubt pages
    // Error out if we don't have a valid LSN, same as the UNDO lsn if we are using a backward
    // log scan for UNDO (not used currently)

    // Generate error because the assumption is that we always start the forward log scan
    // from a completed checkpoint, so the redo and undo LSNs must exist.
    // In theory, if we do not have the redo and undo LSNs, we can alwasy start the recovery from
    // the very beginning of the recovery log, but we are not doing so in this implementation
    // therefore raise error
    if (lsn_t::null == redo_lsn)
        W_FATAL_MSG(fcINTERNAL, << "Missing redo_lsn at the end of Log Analysis phase");
    if (lsn_t::null == undo_lsn)
        W_FATAL_MSG(fcINTERNAL, << "Missing undo_lsn at the end of Log Analysis phase");

    // redo_lsn is where the REDO phase should start for the forward scan,
    // it must be the earliest LSN for all in_doubt pages, which could be earlier
    // than the begin checkpoint LSN
    // undo_lsn is where the UNDO phase should stop for the backward scan (if used),
    // it must be the earliest LSN for all transactions, which could be earlier than
    // the begin checkpoint LSN
    // begin_chkpt is retrieved from 'end checkpoint' log record, it must be set
    // otherwise stop the execution since we must reached an 'end checkpoint' log record
    if (lsn_t::null == begin_chkpt)
        W_FATAL_MSG(fcINTERNAL, << "Missing begin_chkpt lsn at the end of Log Analysis phase");

    w_assert1(begin_chkpt == master);
    if (redo_lsn > master)
       redo_lsn = master;
    if (undo_lsn > master)
       undo_lsn = master;

    // Commit_lsn is the validation point for concurrent user transaction if
    // we open the system after Log Analysis phase and use the commit_lsn
    // implementation instead of lock acquisition implementation.
    // If commit_lsn == lsn_t::null, which is the smallest value:
    //    Start from empty database and no recovery: all concurrent user
    //    transactions are allowed
    // If commit_lsn <> lsn_t::null:
    //    Recovery starts from an existing database, it does not mean we
    //        have loser txn or in_doubt page.
    //    If no loser txn or in_doubt page, then commit_lsn == master

    // If there were any mounts/dismounts that occured between redo_lsn and
    // begin chkpt, need to redo them
    DBGOUT3( << ((theLastMountLSNBeforeChkpt != lsn_t::null &&
                    theLastMountLSNBeforeChkpt > redo_lsn) \
            ? "redoing mounts/dismounts before chkpt but after redo_lsn"  \
            : "no mounts/dismounts need to be redone"));

    // At this point, we have mounted devices from t_chkpt_dev_tab log record and
    // also the individual mount/dismount log records
    if ( 0 != in_doubt_count)
    {
        // CS TODO
        // Do we have more to mount?
        // _analysis_process_extra_mount(theLastMountLSNBeforeChkpt, redo_lsn, mount);
    }
    // Now theLastMountLSNBeforeChkpt == redo_lsn

    // Update the last mount LSN, it was originally set from the begin checkpoint log record
    // but it might have been modified to redo_lsn (earlier)
    // CS TODO
    // log::SetLastMountLSN(theLastMountLSNBeforeChkpt);

    // We are done with Log Analysis, at this point each transactions in the transaction
    // table is either loser (active) or winner (ended);

    // Final process of the entries in transaction table
    _analysis_process_txn_table(loser_heap, commit_lsn);

    // Now we should have the final commit_lsn value
    // if it is the same as max_lsn (initial value), set commit_lsn to null
    // because we did not process anything which affects commit_lsn
    // Note the commit_lsn is the minimum txn lsn from all in-flight transactions
    if (commit_lsn == max_lsn)
        commit_lsn = lsn_t::null;

    w_base_t::base_stat_t f = GET_TSTAT(log_fetches);
    w_base_t::base_stat_t i = GET_TSTAT(log_inserts);
    smlevel_0::errlog->clog << info_prio
        << "After analysis_pass: "
        << f << " log_fetches, "
        << i << " log_inserts "
        << " redo_lsn is " << redo_lsn
        << " undo_lsn is " << undo_lsn
        << " commit_lsn is " << commit_lsn
        << flushl;

    DBGOUT3 (<< "End of Log Analysis phase.  Master: "
             << master << ", redo_lsn: " << redo_lsn
             << ", undo lsn: " << undo_lsn << ", commit_lsn: " << commit_lsn);

    DBGOUT1( << "Number of in_doubt pages: " << in_doubt_count);

    if ((false == mount))
    {
        // We did not mount any device during Log Analysis phase
        // All the device mounting should happen before the REDO phase
        // in other words, we will not be able to fetch page from disk since we did not
        // mount any device
        // If we have in_doubt pages, unless all in_doubt pages are virgin pages,
        // otherwise we will run into errors because we won't be able to fetch pages
        // from disk (not mounted)

        DBGOUT1( << "Log Analysis phase: no device mounting occurred.");
    }

    if (true == use_concurrent_lock_restart())
    {
        W_FATAL_MSG(eNOTIMPLEMENTED, << "restart_m::analysis_pass_back - no Lock acquisition");
    }
    return;
}

/*********************************************************************
 *
 *  restart_m::analysis_pass_backward(master, redo_lsn, in_doubt_count,
                                                          undo_lsn, loser_heap, last_lsn, lock_heap)
 *
 *  Scan log backward from end of recovery log until the last completed checkpoint.
 *  Insert and update buffer pool,
 *  insert transaction table.
 *  Compute redo_lsn.
 *  Non-read-lock acquisition
 * compute commit_lsn
 *
 *  This function is used in all situations from M1 - M5, system is not opened during
 *  Log Analysis phase
 *
 * The main difference between analysis_pass_backward and analysis_pass_forward
 * functions are the following:
 *     1. Log scan direction
 *     2. Lock acquisition for M3/M4/M5
 * At the end of Log Analysis, the results in buffer pool and transaction table should be
 * identical except the lock acquisitions if needed.
 *
 * Implementing two different functions (one for forward and one for backward scan)
 * because although some of the code are very similar, there are sufficient differences
 * between these two functions, also for better performance and cleaner code.
 *********************************************************************/
void
restart_m::analysis_pass_backward(
    const lsn_t           master,            // In: End point for backward log scan
                                             //      The backward scan stops at the beginning
                                             //      of the last completed checkpoint, the passed-in
                                             //      'master' is mainly for verification purpose
    lsn_t&                redo_lsn,          // Out: Starting point for REDO forward log scan (if used),
                                             //       which could be different from master
    uint32_t&             in_doubt_count,    // Out: Counter for in_doubt page count in buffer pool
    lsn_t&                undo_lsn,          // Out: Stopping point for UNDO backward log scan (if used)
    XctPtrHeap&           loser_heap,        // Out: Heap to record all the loser transactions,
                                             //       used only for reverse chronological order
                                             //       UNDO phase (if used)
    lsn_t&                commit_lsn,        // Out: Commit lsn for concurrent transaction (if used), M2 only
    lsn_t&                last_lsn,          // Out: Last lsn in the recovery log before system crash
    const bool            restart_with_lock, // In:  true to acquire lock (M3/4), false to use commit_lsn (M2)
                                             //       or no early open (M1)
    XctLockHeap&          lock_heap          // Out: all re-acquired locks (form M3, M4 and M5 only
)
{
    FUNC(restart_m::analysis_pass_backward);

    // Actually turn off logging during Log Analysis phase, there is no possibility
    // to add new log records by accident during this phase
    AutoTurnOffLogging turnedOnWhenDestroyed;

    // redo_lsn will be used as the starting point for REDO forward log scan,
    // it should be the earliest LSN for all in_doubt pages, it is very likely this
    // LSN is earlier than the master LSN (begin checkpoint LSN).
    // Because we do not load the physical page during Log Analysis phase, we are
    // not able to retrieve _rec_lsn (initial dirty LSN) from each page, therefore we
    // have to rely on:
    // 1. minimum LSN recorded in the 'end checkpoint' log record
    // 2. If a newly allocated and formated page after the checkpoint, there must
    //     be a page format log record in the recovery log before any usage of the page.

    // Initialize redo_lsn, undo_lsn and last_lsn to 0 which is the smallest lsn
    redo_lsn = lsn_t::null;
    undo_lsn = lsn_t::null;
    last_lsn = lsn_t::null;

    // Change state first regardless whether we have work to do or not
    smlevel_0::operating_mode = smlevel_0::t_in_analysis;

    if (master == lsn_t::null)
    {
        // 'master' is  the LSN from the last completed checkpoint
        // It was identified from log_core::log_core()

        // The only possibility that we have a NULL as master lsn is due to a brand new
        // start (empty) of the engine, in such case, nothing to recover

        DBGOUT3( << "NULL master, nothing to analysis in Log Analysis phase");
        return;
    }

    // We have something to process for Log Analysis
    // last_lsn is the very last lsn in Recovery log before the system crash
    // record the last_lsn so it can be used in REDO Single-Page-Recovery if there
    // is a corrupted page (failure on failure)
    w_assert1(lsn_t::null != log->curr_lsn());
    last_lsn = log->curr_lsn();

    // Set the max_lsn to a value larger than the last_lsn
    // the max_lsn is used to initialize various lsn values
    lsn_t max_lsn = last_lsn + 1;
    w_assert1(master < max_lsn);

    // Initialize commit_lsn to a value larger than the current log lsn
    // this is to ensure we have the largest LSN value to begin with
    commit_lsn = max_lsn;

    // The UNDO loser_heap must be empty initially
    w_assert1(0 == loser_heap.NumElements());

    // The tracking lock_heap must be empty initially
    w_assert1(0 == lock_heap.NumElements());

    // Initialize the in_doubt count
    in_doubt_count = 0;
    lsn_t begin_chkpt = lsn_t::null;

    // Did any device mounting occurred during the Log Analysis phase?
    // mount: for DBGOUT purpose to indicate any device was mounted
    bool mount = false;

    // Special heap to store mount log records
    CmpMountLsns       cmpMount;
    MountPtrHeap       heapMount(cmpMount);

    // Ready to process the logs from recovery log
    // Open a backward scan starting from the end of recovery log
    // For backward log scan, during initialization of the iterator, the given
    // lsn is the log record beyond the starting point, meaning when the fetch
    // starts, the first log record returned is the one immediately preceding lsn.
    // We are using lsn from curr_lsn() which is the next available lsn, therefore
    // the fetch returns the very last log record in the log file
    log_i         scan(*log, last_lsn, false /*forward scan*/);
    logrec_t*     log_rec_buf;
    lsn_t         lsn;   // LSN of the retrieved log record

    // theLastMountLSNBeforeChkpt is retrieved from the 'begin checkpoint' log record
    // of the last completed checkpoint, which is also the stopping point for the backward scan
    lsn_t         theLastMountLSNBeforeChkpt = lsn_t::null;

    // Number of complete chkpts handled.  Only the first (the latest)
    // chkpt is actually handled.  The last completed checkpoint LSN might
    // be different from the recorded 'master' due to a race condition
    // between writing a chkpt_end record and updating the master lsn.
    // In other words, a chkpt_end log was hardened, but crash occurred before the master
    // information was updated, therefore the master is the previous checkpoint,
    // even if there is a newer completed checkpoint after the checkpoint
    // recorded in master.
    //
    // This is a valid scenario and need to be handled.  The backward log scan
    // is based on last completed checkpoint, not the checkpoint recorded in master.

    int num_chkpt_end_handled = 0;

    // CS TODO -- dirty hack; see comments below
    char* devtab = NULL;
    char* bkptab = NULL;

    // At the beginning of the recovery from a system crash, both the transaction table
    // and buffer pool should be empty

    unsigned int cur_segment = 0;

    // Boolean to stop the backward log scan, set to true once we have a completed checkpoint
    bool scan_done = false;

    // Special map to mange undecided in-flight transactions
    tid_CLR_map          mapCLR;

    // Boolean to indicate whether we need to acquire non-read lock for the current log record
    // no lock acquisition on a finished (commit or aborted) user transaction
    bool acquire_lock = true;

    // The buffer 'log_rec_buf' for log record was not allocated in caller, therefore it is
    // using a local buffer in the log buffer.  Do not nest the log scan iternator because it
    // would overwrite the data in log_rec_buf
    while (scan.xct_next(lsn, log_rec_buf))
    {
        // New log record, reset the flag
        acquire_lock = true;

        logrec_t& r = *log_rec_buf;

        // Scan next record
        DBGOUT3( << setiosflags(ios::right) << lsn
                  << resetiosflags(ios::right) << " A: " << r );

        // If LSN is not intact, stop now
        if (lsn != r.lsn_ck())
            W_FATAL_MSG(fcINTERNAL, << "Bad LSN from recovery log scan: " << lsn);

        if(lsn.hi() != cur_segment)
        {
            // Record the current segment log in partition
            cur_segment = lsn.hi();
            smlevel_0::errlog->clog << info_prio
               << "Analyzing log segment " << cur_segment << flushl;
        }

        if (r.is_single_sys_xct() && !r.null_pid())
        {
            // We have a system transaction log record
            if (true == _analysis_system_log(r, lsn, in_doubt_count))
            {
                // Go to next log record
                continue;
            }
            else
            {
                // Failure occured, do not continue
                W_FATAL_MSG(fcINTERNAL, << "Failed to process a system transaction log record during Log Analysis, lsn = " << lsn);
            }
        }

        // We already ruled out all SSX logs. So we don't have to worry about
        // multi-page logs in the code below, because multi-page log only exist
        // in system transactions
        w_assert1(!r.is_multi_page());
        xct_t* xd = 0;

        // If log is transaction related, insert the transaction
        // into transaction table if it is not already there.
        if (!r.is_single_sys_xct() && (r.tid() != tid_t::null)                     // Has a transaction ID
                   && ! (xd = xct_t::look_up(r.tid()))   // does not exist in transaction table currently
                   && r.type()!=logrec_t::t_comment      // Not a 'comment' log record, comments can be after xct has ended
                   && r.type()!=logrec_t::t_skip         // Not a 'skip' log record
                   && r.type()!=logrec_t::t_max_logrec)  // Not the special 'max' log record which marks the end
        {
            DBGOUT3(<<"analysis: inserting tx " << r.tid() << " active ");
            xd =  new xct_t(
                                NULL,                     // stats
                                WAIT_SPECIFIED_BY_THREAD, // default timeout value
                                false,                    // sys_xct
                                false,                    // single_log_sys_xct
                                r.tid(),                  // Use the tid from log record
                                lsn,                      // last LSN
                                r.xid_prev(),             // undo_nxt: r.xid_prev() is previous logrec
                                                          //of this xct stored in log record, since
                                                          // this is the last log record for this txn
                                                          // it could be either a update log record (loster)
                                                          // or a commit or abort log record (winner)
                                                          // use the lsn from r.xid_prev() which could be
                                                          // the lsn of the second to last log record if
                                                          // loser transaction
                                                          // when process the actual log record, undo_nxt()
                                                          // will get updated again to the latest lsn
                                                          // undo_nxt iss used for rollback/abort operation
                                true);                    // loser_xct, set to true for recovery
            w_assert1(xd);
            // Youngest tid is the largest tid, it is used to generate
            // next unique transaction id
            if (r.tid() > xct_t::youngest_tid())
                xct_t::update_youngest_tid(r.tid());

            xd->set_last_lsn(lsn);                  // set the last lsn in the transaction
                                                    // due to backward scan, this is the
                                                    // last lsn for this transaction, no need to
                                                    // update it when we see other log records
                                                    // associated to this transaction
            w_assert1(lsn < last_lsn);

            xd->set_first_lsn(lsn);             // initialize first lsn to the same value as last_lsn
            w_assert1( xd->tid() == r.tid() );
        }
        else if (!r.is_single_sys_xct() && r.tid() != tid_t::null)            // Has a transaction ID
        {
            // Transaction exists in transaction table already

            // Due to backward log scan, if the existing transaction
            // has a state 'xct_ended', which means this transaction was
            // ended either normally or aborted, we can safely ingore
            // lock acquisition on all related log records
            // but we still need to process the log record for 'in-doubt' pages
            if ((xd) && (xct_t::xct_ended == xd->state()))
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
                w_assert1(master <= lsn);
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
                _analysis_ckpt_bf_log(r, in_doubt_count);
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

                chkpt_xct_lock_t* dp = (chkpt_xct_lock_t*) r.data();
                // If the transaction tid specified in the log record exists in transaction table and
                // it is an in-flight transaction, re-acquire locks on it
                xd = xct_t::look_up(dp->tid);
                if (xd)
                {
                    // Transaction exists and in-flight
                    if (xct_t::xct_active == xd->state())
                    {
                        // Re-acquire locks
                        _analysis_acquire_ckpt_lock_log(r, xd, lock_heap);
                    }
                }
                else
                {
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
                _analysis_ckpt_xct_log(r, lsn, mapCLR);
            }
            else
            {
                // No matching 'end checkpoint' log record, , ignore
            }
            break;

        case logrec_t::t_chkpt_backup_tab:
            if (num_chkpt_end_handled == 1)
            {
                // Still processing the master checkpoint record.
                // _analysis_ckpt_dev_log(r, mount);

                // CS: dirty trick to fix mounts and backups during backward
                // log analysis, allowing them to appear in any order. This
                // code is temporary, since a proper fix should also apply to
                // the individual log records (see below).  CS TODO
                // r.redo(0);
                bkptab = new char[logrec_t::max_sz];
                memcpy(bkptab, &r, r.length());
            }
            break;

        case logrec_t::t_add_backup:
            // see comments in redo of chkpt_backup_tab
            r.redo(0);
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

                unsigned long i = sizeof(lsn_t);

                memcpy(&begin_chkpt, (lsn_t*) r.data(), i);
                memcpy(&redo_lsn, ((lsn_t*) r.data())+1, i);
                memcpy(&undo_lsn, ((lsn_t*) r.data())+2, i);

                // The 'begin checkpoint' lsn should be either the same or later (newer)
                // than the one (due to race condition) specified by caller, but not earlier
                if (master > begin_chkpt)
                    W_FATAL_MSG(fcINTERNAL,
                                << "Master from 'end checkpoint' is earlier than the one specified by caller of Log Analysis");

                DBGOUT3(<<"t_chkpt_end log record: master=" << begin_chkpt
                        << " min_rec_lsn= " << redo_lsn
                        << " min_txn_lsn= " << undo_lsn);
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

            if (xct_t::xct_ended != xd->state())
                xd->change_state(xct_t::xct_ended);
            break;

        case logrec_t::t_xct_end_group:
            {
                // Do what we do for t_xct_end for each of the
                // transactions in the list

                const xct_list_t* list = (xct_list_t*) r.data();
                int listlen = list->count;
                for(int i=0; i < listlen; i++)
                {
                    xd = xct_t::look_up(list->xrec[i].tid);
                    // If it's not there, it could have been a read-only xct?
                    if ((xd) && (xct_t::xct_ended != xd->state()))
                    {
                        // Mark the txn as ended, safe to remove it from transaction table
                        xd->change_state(xct_t::xct_ended);
                    }
                }
            }
            break;

        case logrec_t::t_xct_abort:
            // Transaction aborted before system crash.
            w_assert1(xct_t::xct_ended != xd->state());

            // fall-through

        case logrec_t::t_xct_end:
            // Log record indicated this txn has ended or aborted
            // It is safe to remove it from transaction table
            // Also no need to gather non-read locks on this transaction
            if (xct_t::xct_ended != xd->state())
                xd->change_state(xct_t::xct_ended);
            break;

        case logrec_t::t_compensate:
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
                _analysis_other_log(r, lsn, in_doubt_count, xd);

                if ((true == acquire_lock) && (true == restart_with_lock))
                {
                    // We need to acquire locks (M3/M4/M5) and this is an
                    // undecided in-flight transaction, process lock for this log record
                    _analysis_process_lock(r, mapCLR,lock_heap,xd);
                }
                else
                {
                    // winner transaction, no need to acquire locks
                }

                if ((r.tid() != tid_t::null) && (xd) &&
                    (r.is_page_update()) && (lsn_t::null == r.xid_prev())
                    && (xct_t::xct_ended == xd->state()))
                {
                    // If this is an update log record, and we have already seen the
                    // transaction end/abort log record for this transaction (backward scan),
                    // and the current log record is the very first log record of this transaction
                    // it is safe to remove this transaction from transaction table now.

                    // Delete the transaction from transaction table because it
                    // reduces the size of llinked-list transaction table and improve
                    // the xct_t::look_up performance of the transaction table
                    delete xd;
                }
            }
            break;

        case logrec_t::t_restore_begin:
        case logrec_t::t_restore_end:
        case logrec_t::t_restore_segment:
        case logrec_t::t_chkpt_restore_tab:
            // CS TODO - IMPLEMENT!
            break;

        case logrec_t::t_alloc_page:
        case logrec_t::t_dealloc_page:
            // do nothing -- page replay will reallocate if necessary
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
        }// switch

        if (scan_done) {
            break;
        }
    }

    // CS TODO -- dirty hack (continuation)
    if (devtab) {
        ((logrec_t*) devtab)->redo(0);
        delete[] devtab;
    }
    if (bkptab) {
        ((logrec_t*) bkptab)->redo(0);
        delete[] bkptab;
    }

    // Finished backward log scan of all the recovery logs, we should
    // have a minimum LSN from the last completed checkpoint,
    // which is where the REDO phase should start for the in_doubt
    // pages (if using log scan REDO)

    DBGOUT3(<< "Log Analysis finished backward scan"
            << ", begin LSN = " << begin_chkpt
            << ", master = " << master
            << ", redo_lsn = " << redo_lsn
            << ", undo_lsn = " << undo_lsn);

    // The assumption is that we must scan a completed checkpoint, stop
    // if the assumption failed
    if (false == scan_done)
        W_FATAL_MSG(fcINTERNAL, << "Did not scan a completed checkpoint");

    // If we do not have valid redo_lsn and undo_lsn,
    // generate error because the assumption is that we always stop the backward log scan
    // when reached a completed checkpoint, so the redo and undo LSNs must exist.
    // In theory, if we do not have the redo and undo LSNs, we can alwasy start the recovery from
    // the very beginning of the recovery log, but we are not doing so in this implementation
    // therefore raise error
    if (lsn_t::null == redo_lsn)
        W_FATAL_MSG(fcINTERNAL, << "Missing redo_lsn at the end of Log Analysis phase");
    if (lsn_t::null == undo_lsn)
        W_FATAL_MSG(fcINTERNAL, << "Missing undo_lsn at the end of Log Analysis phase");

    // redo_lsn is where the REDO phase should start for the forward scan (if used),
    // it must be the earliest LSN for all in_doubt pages, which could be earlier
    // than the begin checkpoint LSN
    // undo_lsn is where the UNDO phase should stop for the backward scan (if used),
    // it must be the earliest LSN for all transactions, which could be earlier than
    // the begin checkpoint LSN
    // begin_chkpt is retrieved from 'end checkpoint' log record, it must be set
    // otherwise stop the execution since we must reached an 'end checkpoint' log record
    if (lsn_t::null == begin_chkpt)
        W_FATAL_MSG(fcINTERNAL, << "Missing begin_chkpt lsn at the end of Log Analysis phase");
    w_assert1(begin_chkpt >= master);
    if (redo_lsn > master)
       redo_lsn = master;
    if (undo_lsn > master)
       undo_lsn = master;

    // Commit_lsn is the validation point for concurrent user transaction if
    // we open the system after Log Analysis phase and use the commit_lsn
    // implementation instead of lock acquisition implementation.
    // If commit_lsn == lsn_t::null, which is the smallest value:
    //    Start from empty database and no recovery: all concurrent user
    //    transactions are allowed
    // If commit_lsn <> lsn_t::null:
    //    Recovery starts from an existing database, it does not mean we
    //        have loser txn or in_doubt page.
    //    If no loser txn or in_doubt page, then commit_lsn == master

    // If we had delayed operation from mount or dismount log records, apply it now
    heapMount.Heapify();
    if ( 0 != heapMount.NumElements())
    {
        comp_mount_log_t* mount_entry = NULL;

        // Process mount log record only if it is < redo_lsn
        while (heapMount.NumElements() > 0)
        {
            mount_entry = heapMount.RemoveFirst();
            w_assert1(NULL != mount_entry);

            if (mount_entry->lsn < redo_lsn)
                mount_entry->mount_log_rec_buf->redo(0);

            // Free the memory allocated for this entry
            delete mount_entry->mount_log_rec_buf;
            delete mount_entry;
            mount_entry = NULL;
        }
        w_assert1(0 == heapMount.NumElements());
    }

    // If there were any mounts/dismounts that occured between redo_lsn and
    // begin chkpt, need to redo them
    DBGOUT3( << ((theLastMountLSNBeforeChkpt != lsn_t::null &&
                    theLastMountLSNBeforeChkpt > redo_lsn) \
            ? "redoing mounts/dismounts before chkpt but after redo_lsn"  \
            : "no mounts/dismounts need to be redone"));

    // CS TODO
    // At this point, we have mounted devices from t_chkpt_dev_tab log record and
    // also the individual mount/dismount log records
    // if (0 != in_doubt_count)
    // {
    //     // Do we have more to mount?
    //     _analysis_process_extra_mount(theLastMountLSNBeforeChkpt, redo_lsn, mount);
    // }
    // Now theLastMountLSNBeforeChkpt == redo_lsn

    // Update the last mount LSN, it was originally set from the begin checkpoint log record
    // but it might have been modified to redo_lsn (earlier)
    // CS TODO
    // log->SetLastMountLSN(theLastMountLSNBeforeChkpt);

    // Done with backward log scan, check the compensation list
    _analysis_process_compensation_map(mapCLR);

    // We are done with Log Analysis, at this point each transactions in the transaction
    // table is either loser (active) or winner (ended); non-read-locks have been acquired
    // on all loser transactions.

    // Final process of the entries in transaction table
    _analysis_process_txn_table(loser_heap, commit_lsn);

    // Now we should have the final commit_lsn value
    // if it is the same as max_lsn (initial value), set commit_lsn to null
    // because we did not process anything which affects commit_lsn
    // Note the commit_lsn is the minimum txn lsn from all in-flight transactions
    // If we are doing lock acquisition (M3/M4/M5), set the commit_lsn to null because
    // we do not want to use it for concurrent tranaactions
    if ((commit_lsn == max_lsn) || (true == restart_with_lock))
        commit_lsn = lsn_t::null;

    w_base_t::base_stat_t f = GET_TSTAT(log_fetches);
    w_base_t::base_stat_t i = GET_TSTAT(log_inserts);
    smlevel_0::errlog->clog << info_prio
        << "After analysis_pass: "
        << f << " log_fetches, "
        << i << " log_inserts "
        << " redo_lsn is " << redo_lsn
        << " undo_lsn is " << undo_lsn
        << " commit_lsn is " << commit_lsn
        << flushl;

    DBGOUT3 (<< "End of Log Analysis phase.  Master: "
             << master << ", redo_lsn: " << redo_lsn
             << ", undo lsn: " << undo_lsn << ", commit_lsn: " << commit_lsn);

// TODO(Restart)... performance
    DBGOUT1( << "Number of in_doubt pages: " << in_doubt_count);

    if (false == mount)
    {
        // We did not mount any device during Log Analysis phase
        // All the device mounting should happen before the REDO phase
        // in other words, we will not be able to fetch page from disk since we did not
        // mount any device
        // If we have in_doubt pages, unless all in_doubt pages are virgin pages,
        // otherwise we will run into errors because we won't be able to fetch pages
        // from disk (not mounted)

        DBGOUT1( << "Log Analysis phase: no device mounting occurred.");
    }
    return;
}


/*********************************************************************
 *
 *  restart_m::_analysis_system_log(r, lsn, in_doubt_count)
 *
 *  Helper function to process one system log record, called by both analysis_pass_forward
 *  and analysis_pass_backward
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
bool restart_m::_analysis_system_log(logrec_t& r,             // In: Log record to process
                                     lsn_t lsn,                   // In: LSN of the log record
                                     uint32_t& in_doubt_count)    // In/out: in_doubt count
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
        bf_idx idx = 0;
        w_rc_t rc = RCOK;

        // Construct a system transaction into transaction table
        xct_t* xd = new xct_t(
            NULL,                          // stats
            WAIT_SPECIFIED_BY_THREAD,      // timeout
            true,                          // system xct
            true,                          // single log sys xct
            xct_t::_nxt_tid.atomic_incr(), // let's use a new transaction id
            lsn,                           // last LSN
            lsn_t::null,                   // no next_undo
            true                           // doomed_txn, set to true for recovery
            );

        w_assert1(xd);
        xd->set_last_lsn(lsn);       // set the last lsn in the transaction

        // Get the associated page
        PageID page_of_interest = r.pid();
        DBGOUT3(<<"analysis (single_log system xct): default " <<  r.type()
                << " page of interest " << page_of_interest);

        w_assert1(!r.is_undo()); // no UNDO for ssx
        w_assert0(r.is_redo());  // system txn is REDO only

        // Register the page into buffer pool (don't load the actual page)
        // If the log record describe allocation of a page, then
        // Allocation of a page (t_alloc_page, t_alloc_consecutive_pages) - clear
        //        the in_doubt bit, because the page might be allocated for a
        //        non-logged operation (e.g., bulk load) which is relying on the page not
        //        being formatted as a regular page.
        //        We clear the in_doubt flag but keep the page in hash table so the page
        //        is considered as used.	A page format log record should come if this is
        //        a regular B-tree page, whcih would mark the in_doubt flag for this page
        // De-allocation of a page (t_dealloc_page, t_page_set_to_be_deleted) -
        //        clear the in_doubt bit and remove the page from hash table so the page
        //        slot is available for a different page

        if (r.type() == logrec_t::t_alloc_page || r.type() == logrec_t::t_dealloc_page)
        {
            // Remove the in_doubt flag in buffer pool of the page if it exists in buffer pool
            idx = smlevel_0::bf->lookup_in_doubt(page_of_interest);
            if (0 != idx)
            {
                // Page cb is in buffer pool, clear the 'in_doubt' and 'used' flags
                // If the cb for this page does not exist in buffer pool, no-op
                if (true == smlevel_0::bf->is_in_doubt(idx))
                {
                    if (r.type() == logrec_t::t_alloc_page)
                        smlevel_0::bf->clear_in_doubt(idx, true, page_of_interest);    // Page is still used
                    else
                        smlevel_0::bf->clear_in_doubt(idx, false, page_of_interest);   // Page is not used
                    w_assert1(0 < in_doubt_count);
                    --in_doubt_count;
                }
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
                idx = 0;
                if (0 == page_of_interest)
                    W_FATAL_MSG(fcINTERNAL,
                        << "Page # = 0 from a system transaction log record");
                rc = smlevel_0::bf->register_and_mark(idx, page_of_interest, r.stid(),
                          lsn /*first_lsn*/, lsn /*last_lsn*/, in_doubt_count);

                if (rc.is_error())
                {
                    // Not able to get a free block in buffer pool without evict, cannot continue
                    // This should not happen if the same of buffer pool remains the same before
                    // and after system crash
                    W_FATAL_MSG(fcINTERNAL,
                        << "Failed to record an in_doubt page for system transaction during Log Analysis");
                }
                w_assert1(0 != idx);

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
                    PageID page2_of_interest = r.pid2();
                    DBGOUT3(<<" multi-page:" <<  page2_of_interest);
                    idx = 0;
                    if (0 == page2_of_interest)
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
                    rc = smlevel_0::bf->register_and_mark(idx, page2_of_interest, r.stid(), lsn /*first_lsn*/,
                                                          lsn /*last_lsn*/, in_doubt_count);
                    if (rc.is_error())
                    {
                        // Not able to get a free block in buffer pool without evict, cannot continue
                        W_FATAL_MSG(fcINTERNAL,
                            << "Failed to record a second in_doubt page for system transaction during Log Analysis");
                    }
                    w_assert1(0 != idx);
                }
            }
            else
            {
                return false;
            }
        }

        // Because all system transactions are single log record, there is no
        // UNDO for system transaction.
        xd->change_state(xct_t::xct_ended);

        // The current log record is for a system transaction which has been handled above
        // done with the processing of this system transaction log record
    }

    return true;
}

/*********************************************************************
 *
 *  restart_m::_analysis_ckpt_bf_log(r, in_doubt_count)
 *
 *  Helper function to process the chkpt_bf_tab log record, called by both
 *  analysis_pass_forward and analysis_pass_backward
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
void restart_m::_analysis_ckpt_bf_log(logrec_t& r,              // In: Log record to process
                                         uint32_t& in_doubt_count) // In/out: in_doubt count
{
    bf_idx idx = 0;
    w_rc_t rc = RCOK;

    const chkpt_bf_tab_t* dp = (chkpt_bf_tab_t*) r.data();
    DBGOUT3(<<"t_chkpt_bf_tab, entries: " << dp->count);

    for (uint i = 0; i < dp->count; i++)
    {
        // For each entry in log,
        // if it is not in buffer pool, register and mark it.
        // If it is already in the buffer pool, update the rec_lsn to the earliest LSN

        idx = 0;
        if (0 == dp->brec[i].pid)
            W_FATAL_MSG(fcINTERNAL,
                << "Page # = 0 from a page in t_chkpt_bf_tab log record");
        rc = smlevel_0::bf->register_and_mark(idx, dp->brec[i].pid, dp->brec[i].store,
                    dp->brec[i].rec_lsn.data() /*first_lsn*/,
                    dp->brec[i].page_lsn.data() /*last_lsn*/, in_doubt_count);
        if (rc.is_error())
        {
            // Not able to get a free block in buffer pool without evict, cannot continue
            W_FATAL_MSG(fcINTERNAL,
                << "Failed to record an in_doubt page in t_chkpt_bf_tab during Log Analysis" << rc);
        }
        w_assert1(0 != idx);
        DBGOUT3(<< "Page marked in_doubt in the buffer pool: "
                    << dp->brec[i].pid);
    }
    return;
}


/*********************************************************************
 *
 *  restart_m::_analysis_ckpt_xct_log(r, lsn, mapCLR)
 *
 *  Helper function to process the t_chkpt_xct_tab log record, called by
 *  analysis_pass_backward only
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
void restart_m::_analysis_ckpt_xct_log(logrec_t& r,          // In: Current log record
                                          lsn_t lsn,             // In: LSN of current log record
                                          tid_CLR_map& mapCLR)   // In/Out: map to hold counters for in-flight transactions
{
    // Received a t_chkpt_xct_tab log record from backward log scan
    // and there was a matching end checkpoint log record,
    // meaning we are processing the last completed checkpoint
    // go ahead and process this log record

    w_assert1(lsn.valid());
    xct_t* xd = 0;
    const chkpt_xct_tab_t* dp = (chkpt_xct_tab_t*) r.data();

    // Pick up the youngest (largest) tid from checkpoint, we will have this value
    // even if there is no transaction entry in the log record
    if (dp->youngest > xct_t::youngest_tid())
        xct_t::update_youngest_tid(dp->youngest);

    // For each entry in the log, it is possible we do not have any transaction entry
    uint iCount = 0;
    uint iTotal = dp->count;
    for (iCount = 0; iCount < iTotal; ++iCount)
    {
        w_assert1(tid_t::null != dp->xrec[iCount].tid);
        xd = xct_t::look_up(dp->xrec[iCount].tid);

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

        if (!xd)
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
            xd = new xct_t(
                                NULL,                      // stats
                                WAIT_SPECIFIED_BY_THREAD,  // default timeout value
                                false,                     // sys_xct
                                false,                     // single_log_sys_xct
                                dp->xrec[iCount].tid,
                                dp->xrec[iCount].last_lsn, // last_LSN
                                dp->xrec[iCount].undo_nxt, // next_undo
                                true);                     // loser_xct, set to true for recovery
            w_assert1(xd);

            if (dp->xrec[iCount].tid > xct_t::youngest_tid())
                xct_t::update_youngest_tid(dp->xrec[iCount].tid);

            // No log record on this transaction after this checkpoint was taken,
            // but we know this transaction was active when the checkpoint
            // started and it did not end after the checkpoint finished.
            // Mark this transaction as a loser transaction regardless whether
            // the transaction was in the middle of rolling back or not

            // Set the first LSN of the in-flight transaction
            xd->set_first_lsn(dp->xrec[iCount].first_lsn);

            DBGOUT3(<<"add xct " << dp->xrec[iCount].tid
                    << " state " << dp->xrec[iCount].state
                    << " last lsn " << dp->xrec[iCount].last_lsn
                    << " undo " << dp->xrec[iCount].undo_nxt
                    << ", first lsn " << dp->xrec[iCount].first_lsn);
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

           w_assert1((xct_t::xct_active == xd->state()) ||
                     (xct_t::xct_ended == xd->state()));
           if (xct_t::xct_active == xd->state())
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
 *  restart_m::_analysis_ckpt_dev_log(r, mount)
 *
 *  Helper function to process the chkpt_dev_tab log record, called by both
 *  analysis_pass_forward and analysis_pass_backward
 *
 *  System is not opened during Log Analysis phase
 *
 *  CS TODO : method not used anymore -- just call redo on chkpt_dev_tab
 *
 *********************************************************************/
#if 0
void restart_m::_analysis_ckpt_dev_log(logrec_t& r,  // In: Log record to process
                                           bool& mount)  // Out: whether the mount occurred
{
    // For each entry in the checkpoint related log, mount the device.
    // No dismount because t_chkpt_dev_tab only contain mounted devices

    // In checkpoint generation, the t_chkpt_dev_tab log record must come
    // before the t_chkpt_bf_tab log record, this is for root page handling.
    //
    // Note io_m::mount() calls vol_t::mount(), which calls install_volume()
    // which would preload the root page (_preload_root_page)
    //
    // Scenario 1 (both forward and backward log scans):
    //                               Root page was not an in_doubt page.  The root page gets
    //                               pre-loaded into buffer pool, registered in hash table, page
    //                               is marked as used but not dirty and not in_doubt during
    //                               the 'mount' process.
    //                               No problem in this scenario because REDO phase will not
    //                               encounter the root page.
    // Scenario 2 (forward log scan):
    //                                Root page was an in_doubt page but only identified after
    //                                the 'mount' operation (guranteed by the checkpoint logic).
    //                                It could be either part of t_chkpt_bf_tab or other log records
    //                                which identified the root page as an in_doubt page.
    //                                1. In Log Analysis phase, it marked the root page as 'in_doubt'
    //                                        and update the in_doubt counter.
    //                                2. REDO phase encounters a page fomat log for the root page
    //                                        This can happen only if it is a brand new root page which
    //                                        does not exist on persistent device, therefore the preload
    //                                        root failed.
    //                                        No problem in this scenario because the REDO phase will
    //                                        allocate a virgin root page and register it, also update flags
    //                                        and in_doubt counter accordingly.
    //                                3. REDO phase encounters a regular log record which does
    //                                        operation on the root page.  Because the page is in_doubt
    //                                        so we will try to load the root page, this operation would fail
    //                                        because the root page was loaded already.
    //                                        Need to set the 'In_doubt' and 'dirty' flags correctly and
    //                                        update the in_doubt counter accordingly.
    // Scenario 3 (forward log scan):
    //                               Root page was an in_doubt page but identified before the 'mount'
    //                               operation.  Although the checkpoint operation gurantee the
    //                               't_chkpt_dev_tab' log comes before 't_chkpt_bf_tab', because checkpoint
    //                               is a non-blocking operation, it is possible after the 'begin checkpoint'
    //                               log record, a regular log record comes in before 't_chkpt_dev_tab'
    //                               which mark the root page 'in_doubt' and register the root page
    //                               in hash table.  In this case, we need to make sure the 'in_doubt'
    //                               flag is still on for the root page after pre-loading the root page.
    // Scenario 4 (backward log scan):
    //                               Root page was marked as an in_doubt page when processing log records
    //                               between the last completed checkpoint and system crash.
    //                               This is simular to scenario 3 above.  We need to make sure the 'in_doubt'
    //                               flag is still on for the root page after pre-loading the root page.

    chkpt_dev_tab_t* dv = (chkpt_dev_tab_t*) r.data();
    DBGOUT3(<<"Log Analysis, number of devices in t_chkpt_dev_tab: " << dv->count);
    uint count = dv->count;

    std::vector<string> devnames;
    dv->read_devnames(devnames);
    for (uint i = 0; i < count; ++i)
    {
        smlevel_0::errlog->clog << info_prio
            << "Device " << devnames[i]
             << " will be recovered" << flushl;
        vid_t vid = vid_t(0);
        W_COERCE(io_m::mount(devnames[i].c_str(), vid));

        // Signal the caller device mount occurred
        mount = true;
    }

    // It is a side effect of mount operation to pre-load the root page
    // Do not increase the in_doubt_count for the root page.
    // The in_doubt_count would be increased only if the page is made dirty
    // by other transactions.  If the root page was in_doubt (dirty),
    // REDO will recover the root page, otherwise it does not recover
    // the root page because it was preloaded by the mount operation.

    return;
}
#endif

/*********************************************************************
 *
 *  restart_m::_analysis_other_log(r, lsn, in_doubt_count, xd)
 *
 *  Helper function to process the rest of meaningful log records, called by both
 *  analysis_pass_forward and analysis_pass_backward
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
void restart_m::_analysis_other_log(logrec_t& r,               // In: log record
                                       lsn_t lsn,                 // In: LSN for the log record
                                       uint32_t& in_doubt_count,  // Out: in_doubt_count
                                       xct_t *xd)                 // In: associated txn object

{
    bf_idx idx = 0;
    w_rc_t rc = RCOK;

    PageID page_of_interest = r.pid();
    DBGOUT3(<<"analysis: default " <<
        r.type() << " tid " << r.tid()
        << " page of interest " << page_of_interest);
    if (r.is_page_update())
    {
        // Log record affects buffer pool, and it is not a compensation log record
        DBGOUT3(<<"is page update " );
        DBGOUT3( << setiosflags(ios::right) << lsn
            << resetiosflags(ios::right) << " A: "
            << "is page update " << page_of_interest );
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

                if (xd->undo_nxt() < lsn)
                    xd->set_undo_nxt(lsn);
            }
            else
            {
                // If UNDO is txn driven, set undo_nxt lsn to the largest (latest) lsn.
                // Abort operation use it to retrieve log record and follow the log
                // record undo_next list
                // This is for both forward and backward log scan in Log Analysis

                if (xd->undo_nxt() < lsn)
                    xd->set_undo_nxt(lsn);
            }
        }

        // This type of log record must be redoable
        w_assert0(r.is_redo());

        // These log records are not compensation log and affected buffer pool pages
        // we need to record these in_doubt pages in buffer pool
        // Exceptions:
        // Allocation of a page (t_alloc_page, t_alloc_consecutive_pages) - clear
        //                   the in_doubt bit, because the page might be allocated for a
        //                   non-logged operation, we don't want to re-format the page
        // De-allocation of a page (t_dealloc_page, t_page_set_to_be_deleted) -
        //                   clear the in_doubt bit, so the page can be evicted if needed.
        if (r.type() == logrec_t::t_alloc_page || r.type() == logrec_t::t_dealloc_page)
        {
            // Remove the in_doubt flag in buffer pool of the page if it exists in buffer pool
            idx = smlevel_0::bf->lookup_in_doubt(page_of_interest);
            if (0 != idx)
            {
                // Page cb is in buffer pool, clear the 'in_doubt' and 'used'  flags
                // If the cb for this page does not exist in buffer pool, no-op
                if (true == smlevel_0::bf->is_in_doubt(idx))
                {
                    if (r.type() == logrec_t::t_alloc_page)
                        smlevel_0::bf->clear_in_doubt(idx, true, page_of_interest);   // Page is still used
                    else
                        smlevel_0::bf->clear_in_doubt(idx, false, page_of_interest);  // Page is not used
                    w_assert1(0 < in_doubt_count);
                    --in_doubt_count;
                }
            }
        }
        else
        {
            // Register the page cb in buffer pool (if not exist) and mark the in_doubt flag
            idx = 0;
            if (0 == page_of_interest)
                W_FATAL_MSG(fcINTERNAL,
                    << "Page # = 0 from a page in log record, log type = " << r.type());
            rc = smlevel_0::bf->register_and_mark(idx,
                      page_of_interest, r.stid(),
                      lsn /*first_lsn*/, lsn /*last_lsn*/, in_doubt_count);
            if (rc.is_error())
            {
                // Not able to get a free block in buffer pool without evict, cannot continue in M1
                W_FATAL_MSG(fcINTERNAL,
                    << "Failed to record an in_doubt page for updated page during Log Analysis");
            }
            w_assert1(0 != idx);
        }
    }
    else if (r.is_cpsn())
    {
        // If compensation record (t_compensate) should be REDO only,
        // no UNDO and skipped in the UNDO phase.

        // Update undo_nxt lsn of xct
        if(r.is_undo())
        {
            DBGOUT3(<<"is cpsn, undo " << " undo_nxt<--lsn " << lsn );

            // r is undoable. There is one possible case of
            // this (undoable compensation record)

            // See xct_t::_compensate() for comments regarding
            // undoable compensation record, at one point there was a
            // special case for it, but the usage was eliminated in 1997
            // the author decided to keep the code in case it will be needed again

            W_FATAL_MSG(fcINTERNAL,
                << "Encounter undoable compensation record in Recovery log");

            xd->set_undo_nxt(lsn);
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
            DBGOUT3(<<"is cpsn, no undo, set undo_next to NULL");
            xd->set_undo_nxt(lsn_t::null);
        }

        // Register the page cb in buffer pool (if not exist) and mark the in_doubt flag
        if (r.is_redo())
        {
            idx = 0;
            if (0 == page_of_interest)
                W_FATAL_MSG(fcINTERNAL,
                    << "Page # = 0 from a page in compensation log record");
            rc = smlevel_0::bf->register_and_mark(idx,
                      page_of_interest, r.stid(),
                      lsn /*first_lsn*/, lsn /*last_lsn*/, in_doubt_count);
            if (rc.is_error())
            {
                // Not able to get a free block in buffer pool without evict, cannot continue in M1
                W_FATAL_MSG(fcINTERNAL,
                    << "Failed to record an in_doubt page for compensation record during Log Analysis");
            }
            w_assert1(0 != idx);
        }
    }
    else  // logrec_t::t_store_operation
    {
        // Retrieve a log buffer which we don't know how to handle
        // Raise error
        W_FATAL_MSG(fcINTERNAL, << "Unexpected log record type: " << r.type());
    }

    if ((r.tid() != tid_t::null) && (xd))
    {
        // If the log record has an associated txn, update the
        // first (earliest) LSN of the associated txn if the log lsn is
        // smaller than the one recorded in the associated txn
        if (lsn < xd->first_lsn())
            xd->set_first_lsn(lsn);
    }

    return;
}

/*********************************************************************
*
*  restart_m::_analysis_process_lock(r, mapCLR, lock_heap, xd)
*
*  Helper function to process lock based on the log record
*  called by analysis_pass_backward only
*
*  System is not opened during Log Analysis phase
*
*********************************************************************/
void restart_m::_analysis_process_lock(logrec_t& r,            // In: Current log record
                                           tid_CLR_map& mapCLR,    // In/Out: Map to track undecided in-flight transactions
                                           XctLockHeap& lock_heap, // Out: Heap to gather all re-acquired locks
                                           xct_t *xd)              // In: Associated transaction

{
    // This is an undecided in-flight transaction and the log record
    // is a meaningful log record (not checkpoint, transaction end/abort,
    // mount, or system transaction  log records),
    // process lock based on the type of log record

    w_assert1(xct_t::xct_ended != xd->state());

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

        _analysis_acquire_lock_log(r, xd, lock_heap);

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
        //            Insert B	                -- count = 3  -- re-acquire lock
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
        //            Insert H	               -- count = 1   -- re-acquire lock
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
*  restart_m::_analysis_acquire_lock_log(r, xd, lock_heap)
*
*  Helper function to process lock re-acquisition based on the log record
*  called by analysis_pass_backward only
*
*  System is not opened during Log Analysis phase
*
*********************************************************************/
void restart_m::_analysis_acquire_lock_log(logrec_t& r,            // In: log record
                                               xct_t *xd,              // In: associated txn object
                                               XctLockHeap& lock_heap) // Out: heap to gather lock info
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

    w_assert1(NULL != xd);                        // Valid transaction object
    w_assert1(xct_t::xct_ended != xd->state());  // In-flight transaction
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
                DBGOUT3(<<"_analysis_acquire_lock_log - acquire X key lock for INSERT, key: " << key);
                okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                lockid_t lid (r.stid(),
                        (const unsigned char*) key.buffer_as_keystr(),
                        key.get_length_as_keystr());

                _re_acquire_lock(lock_heap, mode, lid.hash(), xd);
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
                    DBGOUT3(<<"_analysis_acquire_lock_log - acquire X key lock for NON_GHOST_INSERT, key: " << key);
                    okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                    lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(), key.get_length_as_keystr());

                    _re_acquire_lock(lock_heap, mode, lid.hash(), xd);
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
                DBGOUT3(<<"_analysis_acquire_lock_log - acquire X key lock for UPDATE, key: " << key);
                okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(), key.get_length_as_keystr());

                _re_acquire_lock(lock_heap, mode, lid.hash(), xd);
            }
            break;
        case logrec_t::t_btree_overwrite:
            {
                btree_overwrite_t* dp = (btree_overwrite_t*) r.data();

                // Get the key
                w_keystr_t key;
                key.construct_from_keystr(dp->_data, dp->_klen);
                // Lock re-acquisition
                DBGOUT3(<<"_analysis_acquire_lock_log - acquire X key lock for OVERWRITE, key: " << key);
                okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(), key.get_length_as_keystr());

                _re_acquire_lock(lock_heap, mode, lid.hash(), xd);
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
                        DBGOUT3(<<"_analysis_acquire_lock_log - acquire X key lock for DELETE, key: " << key);
                        okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                        lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(), key.get_length_as_keystr());

                        _re_acquire_lock(lock_heap, mode, lid.hash(), xd);
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
                DBGOUT3(<<"_analysis_acquire_lock_log - acquire X key lock for GHOST_RESERVE(INSERT), key: " << key);
                okvl_mode mode = btree_impl::create_part_okvl(okvl_mode::X, key);
                lockid_t lid (r.stid(), (const unsigned char*) key.buffer_as_keystr(), key.get_length_as_keystr());

                _re_acquire_lock(lock_heap, mode, lid.hash(), xd);
            }
            break;
        default:
            {
                W_FATAL_MSG(fcINTERNAL, << "restart_m::_analysis_acquire_lock_log - Unexpected log record type: " << r.type());
            }
            break;
    }

    return;
}

/*********************************************************************
*
*  restart_m::_analysis_acquire_ckpt_lock_log(r, xd, lock_heap)
*
*  Helper function to process lock re-acquisition for an active transaction in
*  a checkpoint log record
*  called by analysis_pass_backward only
*
*  System is not opened during Log Analysis phase
*
*********************************************************************/
void restart_m::_analysis_acquire_ckpt_lock_log(logrec_t& r,            // In: log record
                                                     xct_t *xd,              // In: associated txn object
                                                     XctLockHeap& lock_heap) // Out: heap to gather lock info
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

    w_assert1(NULL != xd);                             // Valid transaction object
    w_assert1(xct_t::xct_ended != xd->state());        // In-flight transaction
    w_assert1(logrec_t::t_chkpt_xct_lock == r.type()); // Correct checkpoint log record

    // Process all active locks  in the checkpoint log record
    const chkpt_xct_lock_t* dp = (chkpt_xct_lock_t*) r.data();
    if (0 == dp->count)
    {
        // No lock to process
        return;
    }

    // Go through all the locks and re-acquire them on the transaction object
    for (uint i = 0; i < dp->count; i++)
    {
        DBGOUT3(<<"_analysis_acquire_ckpt_lock_log - acquire key lock, hash: " << dp->xrec[i].lock_hash
                << ", key lock mode: " << dp->xrec[i].lock_mode.get_key_mode());

        _re_acquire_lock(lock_heap, dp->xrec[i].lock_mode, dp->xrec[i].lock_hash, xd);
    }

    return;
}


/*********************************************************************
 *
 *  restart_m::_analysis_process_extra_mount(theLastMountLSNBeforeChkpt, redo_lsn, mount)
 *
 *  Helper function to process any extra mount operations occurred between
 *  redo_lsn and the last completed checkpoint, called by both
 *  analysis_pass_forward and analysis_pass_backward
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
#if 0
void restart_m::_analysis_process_extra_mount(lsn_t& theLastMountLSNBeforeChkpt,  // In/Out: last LSN
                                          lsn_t& redo_lsn,                              // In: starting point of REDO log scan
                                          bool& mount)                                  // Out: whether mount occurred
{
    logrec_t*  log_rec_buf;
    logrec_t*  __copy__buf = new logrec_t;
    if(! __copy__buf)
    {
        W_FATAL(eOUTOFMEMORY);
    }
    w_auto_delete_t<logrec_t> auto_del(__copy__buf);
    logrec_t&         copy = *__copy__buf;

    // theLastMountLSNBeforeChkpt was from the begin checkpoint log record
    // it was the lsn of the last mount before the begin checkpoint
    while ((theLastMountLSNBeforeChkpt != lsn_t::null)
        && (theLastMountLSNBeforeChkpt > redo_lsn))
    {
        // last mount occurred between redo_lsn and checkpoint
        W_COERCE(log->fetch(theLastMountLSNBeforeChkpt, log_rec_buf,
                    (lsn_t*) NULL, true));

        // HAVE THE LOG_M MUTEX
        // We have to release it in order to do the mount/dismounts
        // so we make a copy of the log record (log_rec_buf points
        // into the log_m's copy, and thus we have the mutex.`

        logrec_t& r = *log_rec_buf;

        // Only copy the valid portion of the log record.
        memcpy(__copy__buf, &r, r.length());

        log->release();

        DBGOUT3( << theLastMountLSNBeforeChkpt << ": " << copy );

        w_assert9(copy.type() == logrec_t::t_dismount_vol ||
                copy.type() == logrec_t::t_mount_vol);

        chkpt_dev_tab_t *dp = (chkpt_dev_tab_t*)copy.data();
        w_assert9(dp->count == 1);

        // CS TODO

        // it is ok if the mount/dismount fails, since this
        // may be caused by the destruction
        // of the volume.  if that was the case then there
        // won't be updates that need to be
        // done/undone to this volume so it doesn't matter.
        if (copy.type() == logrec_t::t_dismount_vol)
        {
            // TODO(Restart)...
            // the existing (previous) buffer pool in_doubt information
            // was from the previous volume, does the 'mounting' of new volume
            // causing issue in buffer pool in_doubt page information?  Most likely yes.
            // This is an un-resolved issue with Instant Restart 'in_doubt' page
            // implementation, because we are relying on in memory in_dobut page
            // information and buffer pool cannot be flushed during Restart time

            // Mount a new volume
            // W_FATAL_MSG(fcINTERNAL, << "Extra mounting at the end of Log Analysis phase");
            // W_IGNORE(io_m::mount(dp->devrec[0].dev_name, dp->devrec[0].vid));
            mount = true;
        }
        else if (copy.type() == logrec_t::t_mount_vol)
        {
            // Dismount the current volume
            // This is happening at the end of Log Analysys phase while
            // we have marked all the in_doubt page information in
            // buffer pool.  If we flush the buffer pool then all the in_doubt
            // page information would be erased, not good
            // W_FATAL_MSG(fcINTERNAL, << "Extra dismounting at the end of Log Analysis phase");
            // W_IGNORE(io_m::dismount(dp->devrec[0].vid, false));  // Do not flush buffer pool
        }

        theLastMountLSNBeforeChkpt = copy.xid_prev();
    }

    // auto-release will free the log rec copy buffer, __copy__buf
    return;
}
#endif

/*********************************************************************
 *
 *  restart_m::_analysis_process_compensation_map(mapCLR)
 *
 *  Helper function to process the compensation list for undecided transactions
 *  called by analysis_pass_backward only
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
void restart_m::_analysis_process_compensation_map(
                                tid_CLR_map& mapCLR) // In: map to track log record count for all undecided in-flight transaction
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

    bool _original_value = false;
    xct_t* xd = 0;

    // Loop through all elements in map
    for (tid_CLR_map::iterator it = mapCLR.begin(); it != mapCLR.end(); it++)
    {
        if (0 == it->second)
        {
            // Change the undecided transaction into a winner transaction,
            // release locks and generate an 'abort' log record
            tid_t t(it->first);
            xd = xct_t::look_up(t);
            w_assert1(NULL != xd);

            // Free all the acquired locks
            me()->attach_xct(xd);
            xd->commit_free_locks();

            // Generate a new lock record, because we are in the middle of Log Analysis
            // log generation has been turned off, turn it on temperaury
            _original_value = smlevel_0::logging_enabled;
            smlevel_0::logging_enabled = true;
            log_xct_abort();
            smlevel_0::logging_enabled = _original_value;

            // Done dealing with the transaction
            me()->detach_xct(xd);

            // Mark the transaction as a winner in transaction table, all winner transactions
            // will be removed from transaction table later
            if (xct_t::xct_ended != xd->state())
                xd->change_state(xct_t::xct_ended);
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
            xd = xct_t::look_up(t);
            w_assert1(NULL != xd);
            w_assert1(xct_t::xct_active == xd->state());
        }
    }

    return;
}


/*********************************************************************
 *
 *  restart_m::_analysis_process_txn_table(heap, commit_lsn)
 *
 *  Helper function to process the transaction table after we finished the log scan
 *  called by both analysis_pass_forward and analysis_pass_backward
 *
 *  System is not opened during Log Analysis phase
 *
 *********************************************************************/
void restart_m::_analysis_process_txn_table(XctPtrHeap& heap,  // Out: heap to store all in-flight transactions
                                                                    //       for serial mode only
                                                 lsn_t& commit_lsn) // In/Out: update commit_lsn value
                                                                    //       ignored if using lock acquisition
{
    // Destroy the ended (winner) transactions
    // if in serial mode, populate the special heap with loser (active) transactions
    // for the UNDO phase.

    // After this step, only loser transactions are left in the transaction table
    // all of them should have state 'active' and marked as is_loser_xct()
    // these loser transactions will be cleaned up in the UNDO phase.

    // We are not locking the transaction table during this process because
    // we are in the Log Analysis phase and the system is not opened for new
    // transaction yet
    // Similarly, no lock is required on the transaction table when deleting
    // ended transaction from transaction table

    xct_i iter(false); // not locking the transaction table list
    xct_t*    xd;
    xct_t*    curr;
    if (true == use_serial_restart())
    {
        DBGOUT3( << "Building loser heap...");
    }
    xd = iter.next();
    while (xd)
    {
        DBGOUT3( << "Transaction " << xd->tid()
                 << " has state " << xd->state() );

        if (xct_t::xct_active == xd->state())
        {
            // The loser_xct flag must be on
            w_assert1(true == xd->is_loser_xct());

            // Determine the value for commit_lsn which is the minimum
            // lsn of all loser transactions
            // For a loser txn, the first lsn is the smallest lsn
            if (commit_lsn > xd->first_lsn())
                commit_lsn = xd->first_lsn();

            // Loser transaction
            if (true == use_serial_restart())
                heap.AddElementDontHeapify(xd);

            // Advance to the next transaction
            xd = iter.next();
        }
        else
        {
            if (xct_t::xct_ended == xd->state())
            {
                // Ended transaction (winner)
                // Due to backward log scan, no need to free non-read locks
                // on 'ended' (winner) transactions because we never
                // acquired locks on these transactions

                curr = iter.curr();
                w_assert1(curr);

                // Advance to the next transaction first
                xd = iter.next();

                // Then destroy the ended transaction
                delete curr;
            }
            else
            {
                // We are not supposed to see transaction with other stats
                W_FATAL_MSG(fcINTERNAL,
                    << "Transaction in the traction table is not loser in Log Analysis phase, xd: "
                    << xd->tid());
            }
        }
    }
    // Done populating the heap, now tell the heap to sort
    if (true == use_serial_restart())
    {
        heap.Heapify();
        DBGOUT3( << "Number of transaction entries in loser heap: " << heap.NumElements());
    }

// TODO(Restart)... performance
    DBGOUT1( << "Number of active transactions in transaction table: " << xct_t::num_active_xcts());

    return;
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
        logrec_t* log_rec_buf=0;

        lsn_t lsn;
        lsn_t expected_lsn = redo_lsn;
        bool redone = false;
        bool serial_recovery = use_serial_restart();
        while (scan.xct_next(lsn, log_rec_buf))
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

            logrec_t& r = *log_rec_buf;

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
            else if (r.type() == logrec_t::t_alloc_page)
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

