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

#include "sm_int_1.h"
#include "w_heap.h"
#include "restart.h"
// include crash.h for definition of LOGTRACE1
#include "crash.h"
#include "bf_tree.h"
#include "sm_int_0.h"
#include "bf_tree_inline.h"
#include "chkpt.h"
#include <map>

#ifdef EXPLICIT_TEMPLATE
template class Heap<xct_t*, CmpXctUndoLsns>;
#endif

tid_t                restart_m::_redo_tid;


/*****************************************************
//Dead code, comment out just in case we need to re-visit it in the future
// We are using the actual buffer pool to register in_doubt page during Log Analysis
// no longer using the special in-memory dirty page table for this purpose

typedef uint64_t dp_key_t;

inline dp_key_t dp_key(volid_t vid, shpid_t shpid) {
    return ((dp_key_t) vid << 32) + shpid;
}
inline dp_key_t dp_key(const lpid_t &pid) {
    return dp_key(pid.vol().vol, pid.page);
}
inline volid_t  dp_vid (dp_key_t key) {
    return key >> 32;
}
inline shpid_t  dp_shpid (dp_key_t key) {
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
    void                         insert(const lpid_t& pid, lsndata_t lsn) {
        if (_validCachedMinRecLSN && lsn < _cachedMinRecLSN && lsn != lsndata_null)  {
            _cachedMinRecLSN = lsn;
        }
        w_assert1(_dp_lsns.find(dp_key(pid)) == _dp_lsns.end());
        _dp_lsns.insert(std::pair<dp_key_t, lsndata_t>(dp_key(pid), lsn));
    }

    // Returns if the page already exists in the table.
    bool                         exists(const lpid_t& pid) const {
        return _dp_lsns.find(dp_key(pid)) != _dp_lsns.end();
    }
    // Returns iterator (pointer) to the page in the table. 
    dp_lsn_iterator              find (const lpid_t& pid) {
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
 *  restart_m::recover(master)
 *
 *  Start the recovery process. Master is the master lsn (lsn of
 *  the last successful checkpoint record).
 *
 *********************************************************************/
void 
restart_m::recover(lsn_t master)
{
    FUNC(restart_m::recover);

    // Make sure the current state is before 'recovery', the Recovery operation can
    // be called only once per system start
    if (!before_recovery())
        W_FATAL_MSG(fcINTERNAL, 
        << "Cannot recovery while the system is not in before_recovery state, current state: " 
        << smlevel_0::operating_mode);

    lsn_t redo_lsn = lsn_t::null;  // redo_lsn is the starting point for REDO log forward scan
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


////////////////////////////////////////
// TODO(Restart)... turning swizzling off in M1
//             we will need to address this issue
//             especially for on-demand recovery
//             turning swizzling on before UNDO raise
//             exception currently
////////////////////////////////////////

    // during REDO, we need to turn off pointer swizzling, it will be turned back on
    // after UNDO phase, after the Recovery is done
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

////////////////////////////////////////
// TODO(Restart)... ignore 'non-read-lock' in M1
////////////////////////////////////////

    // Log Analysis phase, the store is not opened for new transaction during this phase
    // Populate transaction table for all in-flight transactions, mark them as 'active'
    // Populate buffer pool for 'in_doubt' pages, register but not loading the pages
    // Populate the special heap with all the doomed transactions for UNDO purpose
    uint32_t in_doubt_count = 0;
    CmpXctUndoLsns        cmp;
    XctPtrHeap            heap(cmp);
    analysis_pass(master, redo_lsn, in_doubt_count, undo_lsn, heap);

    // If nothing from Log Analysis, in other words, if both transaction table and buffer pool
    // are empty, there is nothing to do in REDO and UNDO phases, but we still want to 
    // take a 'empty' checkpoint' as the starting point for the next server start.
    // In this case, only one checkpoint will be taken during Recovery, not multiple checkpoints

    int32_t xct_count = heap.NumElements();

    // xct_count: the number of doomed transactions in transaction table, 
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
            << " doomed transactions" << flushl;
    }

    // Take a synch checkpoint after the Log Analysis phase and before the REDO phase
    w_assert1(smlevel_1::chkpt);    
    smlevel_1::chkpt->synch_take();

    // It is valid to have in_doubt_count <> 0 while xct_count == 0 (all transactions ended)
    // because when a transaction commits, it flushs the log but not the buffer pool.

    lsn_t curr_lsn = log->curr_lsn(); 

    // Change mode to REDO although we would enter REDO only if we have in_doubt page
    smlevel_0::operating_mode = smlevel_0::t_in_redo;
    if (0 != in_doubt_count)
    {
         // Come in here only if we have something to REDO
         
         //  Phase 2: REDO -- use dirty page table and redo lsn of phase 1
         //                  We save curr_lsn before redo_pass() and assert after
         //                 redo_pass that no log record has been generated.
         //  pass in highest_lsn for debugging

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

////////////////////////////////////////
// TODO(Restart)... does not open the store for new transactions in M1
//                    ignore 'non-read-lock' in M1
////////////////////////////////////////

        // REDO phase, based on log records (forward scan), load 'in_doubt' pages
        // into buffer pool, REDO the updates, clear the 'in_doubt' flags and mark
        // the 'dirty' flags for modified pages.
        // No change to transaction table or recovery log
        DBGOUT3(<<"starting REDO at " << redo_lsn << " highest_lsn " << curr_lsn);
        redo_pass(redo_lsn, curr_lsn, in_doubt_count);

        // no logging during redo
        w_assert1(curr_lsn == log->curr_lsn()); 

        // We took a checkpoint at the end of Log Analysis phase which caused
        // a log flush, therefore the buffer pool flush at the end of the REDO phase
        // is optional, but we are doing it anyway so if we encounter a system crash
        // after this point, we would have less recovery work to do in the next recovery

////////////////////////////////////////
// TODO(Restart)... flush_all in M1
//         once we open REDO phase for new transaction, 
//         then we don't need the flush in buffer pool
////////////////////////////////////////
        
        /* In order to preserve the invariant that the rec_lsn <= page's lsn1,
         * we need to make sure that all dirty pages get flushed to disk,
         * since the redo phase does NOT log these page updates, it causes
         * rec_lsns to be at the tail of the log while the page lsns are
         * in the middle of the log somewhere.  It seems worthwhile to
         * do this flush, slow though it might be, because if we have a crash
         * and have to re-recover, we would have less to do at that time.
         */
        W_COERCE(bf->force_all());
    }

    // Change mode to UNDO although we would enter UNDO only if we have doomed txn
    smlevel_0::operating_mode = smlevel_0::t_in_undo;
    if (0 != xct_count)
    {
        // Come in here only if we have something to UNDO
        
        // Phase 3: UNDO -- abort all active transactions
        smlevel_0::errlog->clog  << info_prio<< "Undo ..." 
            << " curr_lsn = " << curr_lsn  << " undo_lsn = " << undo_lsn
            << flushl;

////////////////////////////////////////
// TODO(Restart)... does not open the store for new transactions in M1
//                    ignore 'non-read-lock' in M1
////////////////////////////////////////

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
        undo_pass(heap, curr_lsn, undo_lsn);

        smlevel_0::errlog->clog << info_prio << "Oldest active transaction is " 
            << xct_t::oldest_tid() << flushl;
        smlevel_0::errlog->clog << info_prio 
            << "First new transaction will be greater than "
            << xct_t::youngest_tid() << flushl;

        // Take a synch checkpoint after UNDO phase but before existing the Recovery operation
        smlevel_1::chkpt->synch_take();
    }

    // turn pointer swizzling on again after we are done with the Recovery
    if (org_swizzling_enabled) 
    {
        W_COERCE(smlevel_0::bf->set_swizzling_enabled(true));
    }

    smlevel_0::errlog->clog << info_prio << "Restart successful." << flushl;
    DBGOUT1(<<"Recovery ended");

    // Exiting from the Recovery operation, caller of the Recovery operation is 
    // responsible of changing the 'operating_mode' to 'smlevel_0::t_forward_processing',
    // because caller is doing some mounting/dismounting devices, we change
    // the 'operating_mode' only after the device mounting operations are done.

}


/*********************************************************************
 *
 *  restart_m::analysis_pass(master, redo_lsn, in_doubt_count, undo_lsn, heap)
 *
 *  Scan log forward from master_lsn. Insert and update buffer pool, 
 *  insert transaction table.
 *  Compute redo_lsn.
 *
 *********************************************************************/
void 
restart_m::analysis_pass(
    const lsn_t           master,
    lsn_t&                redo_lsn,
    uint32_t&             in_doubt_count,  // Counter for in_doubt page count in buffer pool
    lsn_t&                undo_lsn,
    XctPtrHeap&           heap             // Heap to record all the doomed transactions
)
{
    FUNC(restart_m::analysis_pass);

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

    // Initialize both redo_lsn and undo_lsn to 0 which is the smallest lsn
    redo_lsn = lsn_t::null;
    undo_lsn = lsn_t::null;
    in_doubt_count = 0;
    lsn_t begin_chkpt = lsn_t::null;

    // Did any device mounting occurred during the Log Analysis phase?
    // mount: for DBGOUT purpose to indicate any device was mounted
    bool mount = false;

    if (master == lsn_t::null)
    {
        // 'master'is  the LSN from the last completed checkpoint
        // It was identified from log_core::log_core()
        
        // The only possibility that we have a NULL as master lsn is due to a brand new
        // start (empty) of the engine, in such case, nothing to recover

        DBGOUT3( << "NULL master, nothing to analysis in Log Analysis phase");        
        return;
    }

    // The UNDO heap must be empty initially
    w_assert1(0 == heap.NumElements());

    // Change state, we have work to do in Log Analysis phase
    smlevel_0::operating_mode = smlevel_0::t_in_analysis;

    // Open a forward scan starting from master (the begin checkpoint LSN from the 
    // last completed checkpoint
    log_i         scan(*log, master);
    logrec_t*     log_rec_buf;
    lsn_t         lsn;

    lsn_t         theLastMountLSNBeforeChkpt;

    bf_idx idx = 0;
    w_rc_t rc = RCOK;

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
            DBGOUT3( << setiosflags(ios::right) << lsn
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

    // At the beginning of the Recovery from a system crash, both the transaction table
    // and buffer pool should be initialized with the information from the specified checkpoint,
    // and then modified according to the following log records in the recovery log
    
    while (scan.xct_next(lsn, log_rec_buf)) 
    {
        logrec_t& r = *log_rec_buf;

        // Scan next record
        DBGOUT5( << setiosflags(ios::right) << lsn 
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
        
        // If the log was a system transaction fused to a single log entry,
        // we should do the equivalent to xct_end, but take care of marking the
        // in_doubt page in buffer pool first

        // Note currently all system transactions are single log entry, we do not have
        // system transaction involving multiple log records
        
        if (r.is_single_sys_xct()) 
        {
            // Construct a system transaction into transaction table
            xct_t* xd = xct_t::new_xct(
                xct_t::_nxt_tid.atomic_incr(), // let's use a new transaction id
                xct_t::xct_active,             // state
                lsn,                           // last LSN
                lsn_t::null,                   // no next_undo
                WAIT_SPECIFIED_BY_THREAD,      // timeout
                true,                          // system xct
                true                           // single log sys xct
                );
        
            w_assert1(xd);
            xd->set_last_lsn(lsn);         // set the last lsn in the transaction

            // Get the associated page
            lpid_t page_of_interest = r.construct_pid();
            DBGOUT3(<<"analysis (single_log system xct): default " <<  r.type()
                    << " page of interest " << page_of_interest);

            w_assert1(!r.is_undo()); // no UNDO for ssx
            w_assert0(r.is_redo());  // system txn is REDO only

            // Register the page into buffer pool (don't load the actual page)
            // If the log record describe allocation of a page, then
            // Allocation of a page (t_alloc_a_page, t_alloc_consecutive_pages) - clear
            //      the in_doubt bit, because the page might be allocated for a 
            //      non-logged operation (e.g., bulk load) which is relying on the page not 
            //      being formatted as a regular page.
            //      We clear the in_doubt flag but keep the page in hash table so the page
            //      is considered as used.  A page format log record should come if this is
            //      a regular B-tree page, whcih would mark the in_doubt flag for this page
            // De-allocation of a page (t_dealloc_a_page, t_page_set_to_be_deleted) - 
            //      clear the in_doubt bit and remove the page from hash table so the page 
            //      slot is available for a different page

            if ((true == r.is_page_allocate()) ||
                (true == r.is_page_deallocate()))
            {
                // Remove the in_doubt flag in buffer pool of the page if it exists in buffer pool
                uint64_t key = bf_key(page_of_interest.vol().vol, page_of_interest.page);
                bf_idx idx = smlevel_0::bf->lookup_in_doubt(key);
                if (0 != idx)
                {
                    // Page cb is in buffer pool, clear the 'in_doubt' and 'used' flags
                    // If the cb for this page does not exist in buffer pool, no-op 
                    if (true == smlevel_0::bf->is_in_doubt(idx))
                    {
                        if (true == r.is_page_allocate())
                            smlevel_0::bf->clear_in_doubt(idx, true, key);    // Page is still used
                        else
                            smlevel_0::bf->clear_in_doubt(idx, false, key);   // Page is not used
                        w_assert1(0 < in_doubt_count);
                        --in_doubt_count;
                    }
                }
            }
            else if (false == r.is_skip())  // t_skip marks the end of partition, no-op
            {
                // System transaction does not have txn id, but it must have page number
                // this is true for both single and multi-page system transactions
                
                if (false == r.null_pid())
                {
                    // If the log record has a valid page ID, the operation affects buffer pool
                    // Register the page cb in buffer pool (if not exist) and mark the in_doubt flag               
                    idx = 0;
                    if (0 == page_of_interest.page)
                        W_FATAL_MSG(fcINTERNAL, 
                            << "Page # = 0 from a system transaction log record");
                    rc = smlevel_0::bf->register_and_mark(idx, page_of_interest,
                              lsn, in_doubt_count);

                    if (rc.is_error()) 
                    {
                        // Not able to get a free block in buffer pool without evict, cannot continue in M1
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
                    //    btree_norec_alloc_log - 2nd page is a new page which needs to be allocated
                    //    btree_foster_adopt_log
                    //    btree_foster_merge_log
                    //    btree_foster_rebalance_log
                    //    btree_foster_rebalance_norec_log - during a page split, foster parent page would split
                    //                                                         does it allocate a new page?
                    //    btree_foster_deadopt_log
                    if (r.is_multi_page()) 
                    {
                        lpid_t page2_of_interest = r.construct_pid2();
                        DBGOUT3(<<" multi-page:" <<  page2_of_interest);
                        idx = 0;
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
                        rc = smlevel_0::bf->register_and_mark(idx, page2_of_interest, lsn, in_doubt_count);
                        if (rc.is_error()) 
                        {
                            // Not able to get a free block in buffer pool without evict, cannot continue in M1
                            W_FATAL_MSG(fcINTERNAL, 
                                << "Failed to record a second in_doubt page for system transaction during Log Analysis");
                        }
                        w_assert1(0 != idx);
                    }
                }
                else
                {
                    // Log record with system transaction but no page number means 
                    // the system transaction does not affect buffer pool
                    // Can this a valid scenario?  Raise fatal error for now so we can catch it.

                    W_FATAL_MSG(fcINTERNAL, 
                        << "System transaction without a page number, type = " << r.type());

                    DBGOUT3(<<"System transaction without a page number, type =  " << r.type());                
                }
            }
            else
            {
                // If skip log, no-op.
            }

            // Because all system transactions are single log record, there is no
            // UNDO for system transaction.           
            xd->change_state(xct_t::xct_ended);

            // The current log record is for a system transaction which has been handled above
            // go to the next log record
            continue;
        }

        // We already ruled out all SSX logs. So we don't have to worry about
        // multi-page logs in the code below, because multi-page log only exist
        // in system transactions
        w_assert1(!r.is_multi_page());
        xct_t* xd = 0;

        // If log is transaction related, insert the transaction
        // into transaction table if it is not already there.
        if ((r.tid() != tid_t::null) && ! (xd = xct_t::look_up(r.tid()))
                   && r.type()!=logrec_t::t_comment      // comments can be after xct has ended
                   && r.type()!=logrec_t::t_skip         // skip
                   && r.type()!=logrec_t::t_max_logrec)  // mark the end
        {
            DBGOUT3(<<"analysis: inserting tx " << r.tid() << " active ");
            xd = xct_t::new_xct(r.tid(),              // Use the tid from log record
                                xct_t::xct_active,    // state, by default treat as an in-flight
                                                      // transaction and mark it 'active' 
                                                      // the state will be changed to 'end' only
                                                      // if we hit a matching t_xct_end' log
                                lsn,                  // last LSN
                                r.xid_prev());        // next_undo
            w_assert1(xd);
            xct_t::update_youngest_tid(r.tid());

            xd->set_last_lsn(lsn);                  // set the last lsn in the transaction
            w_assert1( xd->tid() == r.tid() );
        }
        else
        {
            // Log record is not transaction related, or already exist in the transaction table
            // no-op to transaction table
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
                const chkpt_bf_tab_t* dp = (chkpt_bf_tab_t*) r.data();
                DBGOUT3(<<"t_chkpt_bf_tab, entries: " << dp->count);
                for (uint i = 0; i < dp->count; i++)  
                {
                    // For each entry in log,
                    // if it is not in buffer pool, register and mark it.
                    // If it is already in the buffer pool, update the rec_lsn to the earliest LSN

                    idx = 0;
                    if (0 == dp->brec[i].pid.page)
                        W_FATAL_MSG(fcINTERNAL, 
                            << "Page # = 0 from a page in t_chkpt_bf_tab log record");
                    rc = smlevel_0::bf->register_and_mark(idx, dp->brec[i].pid,
                            dp->brec[i].rec_lsn.data(), in_doubt_count);
                    if (rc.is_error()) 
                    {
                        // Not able to get a free block in buffer pool without evict, cannot continue in M1
                        W_FATAL_MSG(fcINTERNAL, 
                            << "Failed to record an in_doubt page in t_chkpt_bf_tab during Log Analysis");
                    }
                    w_assert1(0 != idx);
                }
            }
            else
            {
                // Not from the master checkpoint, ignore
            }
            break;
                
        case logrec_t::t_chkpt_xct_tab:
            // Transaction table entries from checkpoint
            if (num_chkpt_end_handled == 0)  
            {
                // Still processing the master checkpoint record.
                // For each entry in the log,
                //         If the xct is not in xct tab, insert it.
                const chkpt_xct_tab_t* dp = (chkpt_xct_tab_t*) r.data();
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
                        // as a doomed transaction by accident, therefore leave the ended 
                        // transaction in the transaction table until we are done with the log scan,
                        // and then cleanup all the ended transaction table at the end.

                        if (dp->xrec[i].state != xct_t::xct_ended)  
                        {
                            // skip finished ones                        
                            xd = xct_t::new_xct(dp->xrec[i].tid,
                                        xct_t::xct_active,   // Instead of using dp->xrec[i].state
                                                             // gathered in checkpoint log,
                                                             // mark transaction active to 
                                                             // indicate this transaction
                                                             // might need UNDO
                                        dp->xrec[i].last_lsn,
                                        dp->xrec[i].undo_nxt);
                            DBGOUT3(<<"add xct " << dp->xrec[i].tid
                                    << " state " << dp->xrec[i].state
                                    << " last lsn " << dp->xrec[i].last_lsn
                                    << " undo " << dp->xrec[i].undo_nxt);
                            w_assert1(xd);
                        }
                    }
                    else
                    {
                       // Found in the transaction table, it must be marked as:
                       // doomed transaction (active) - in-flight transaction during checkpoint
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
            
        case logrec_t::t_chkpt_dev_tab:
            if (num_chkpt_end_handled == 0)  
            {
                // Still processing the master checkpoint record.
                // For each entry in the checkpoint related log, mount the device.
                // No dismount because t_chkpt_dev_tab only contain mounted devices

                // In checkpoint generation, the t_chkpt_dev_tab log record must come
                // before the t_chkpt_bf_tab log record, this is for root page handling.
                //
                // Note io_m::mount() calls vol_t::mount(), which calls install_volume()
                // which would preload the root page (_preload_root_page)
                // Scenario 1: Root page was not an in_doubt page.  The root page gets
                //                  pre-loaded into buffer pool, registered in hash table, page
                //                  is marked as used but not dirty and not in_doubt during 
                //                  the 'mount' process.
                //                  No problem in this scenario because REDO phase will not
                //                  encounter the root page.
                // Scenario 2: Root page was an in_doubt page but only identified after
                //                   the 'mount' operation (guranteed by the checkpoint logic).
                //                   It could be either part of t_chkpt_bf_tab or other log records
                //                   which identified the root page as an in_doubt page.
                //                   1. In Log Analysis phase, it marked the root page as 'in_doubt'
                //                       and update the in_doubt counter.
                //                   2. REDO phase encounters a page fomat log for the root page
                //                       This can happen only if it is a brand new root page which 
                //                       does not exist on disk, therefore the preload root failed.
                //                       No problem in this scenario because the REDO phase will
                //                       allocate a virgin root page and register it, also update flags
                //                       and in_doubt counter accordingly.
                //                   3. In REDO phase encounters a regular log record which does
                //                       operation on the root page.  Because the page is in_doubt
                //                       so we will try to load the root page, this operation would fail
                //                       because the root page was loaded already.
                //                       Need to set the 'In_doubt' and 'dirty' flags correctly and
                //                       update the in_doubt counter accordingly.
                // Scenario 3: Root page was an in_doubt page but identified before the 'mount'
                //                  operation.  Although the checkpoint operation gurantee the 
                //                  't_chkpt_dev_tab' log comes before 't_chkpt_bf_tab', because checkpoint
                //                  is a non-blocking operation, it is possible after the 'begin checkpoint'
                //                  log record, a regular log record comes in before 't_chkpt_dev_tab'
                //                  which mark the root page 'in_doubt' and register the root page 
                //                  in hash table.  In this case, we need to make sure the 'in_doubt'
                //                  flag is still on for the root page.
                
                const chkpt_dev_tab_t* dv = (chkpt_dev_tab_t*) r.data();
                DBGOUT3(<<"Log Analysis, number of devices in t_chkpt_dev_tab: " << dv->count);

                for (uint i = 0; i < dv->count; i++)  
                {
                    smlevel_0::errlog->clog << info_prio 
                        << "Device " << dv->devrec[i].dev_name 
                         << " will be recovered as vid " << dv->devrec[i].vid
                         << flushl;
                    W_COERCE(io_m::mount(dv->devrec[i].dev_name, 
                                       dv->devrec[i].vid));

                    w_assert9(io_m::is_mounted(dv->devrec[i].vid));

                    mount = true;
                }
            }
            break;
        
        case logrec_t::t_dismount_vol:
        case logrec_t::t_mount_vol:

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

            w_assert9(num_chkpt_end_handled > 0);  
            // mount & dismount shouldn't happen during a check point
            // redo_lsn is initialized to NULL, and only set to the minimum lsn
            // from master 'end checkpoint' when we encounter it during log scan
            // Only redo, no undo for mount & dismount
            if (lsn < redo_lsn)  
            {
                r.redo(0);

                if (logrec_t::t_mount_vol == r.type())
                    mount = true;
            }
            break;
                
        case logrec_t::t_chkpt_end:
            if (num_chkpt_end_handled == 0)              
            {
                // Retrieve the master, min_rec_lsn and min_txn_lsn from 
                // the first (master) 'end checkpoint',
                // The minimum lsn of all buffer pool dirty or in_doubt pages
                // The REDO phase must start with the earliest LSN
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

            // Normally if the txn state in 'xct_freeing_space' or 'xct_committing',
            // something went wrong in the commit process, need to abort the txn

            // A t_xct_freeing_space log record is generated when the txn
            // entered 'xct_freeing_space' state
            // Because we are in Recovery, mark the txn to 'ended' state

            if (xct_t::xct_ended != xd->state())
                xd->change_state(xct_t::xct_ended);
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
        case logrec_t::t_xct_end:
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
            // Log record indicated this txn has ended or aborted already
            // It is safe to remove it from transaction table
            if (xct_t::xct_ended != xd->state())
                xd->change_state(xct_t::xct_ended);
            break;

        case logrec_t::t_compensate:
        case logrec_t::t_alloc_a_page:
        case logrec_t::t_alloc_consecutive_pages:
        case logrec_t::t_dealloc_a_page:
        case logrec_t::t_store_operation:
        case logrec_t::t_page_set_to_be_deleted:
        case logrec_t::t_page_img_format:
        case logrec_t::t_btree_norec_alloc:
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
            
            {
                lpid_t page_of_interest = r.construct_pid();
                DBGOUT3(<<"analysis: default " << 
                    r.type() << " tid " << r.tid()
                    << " page of interest " << page_of_interest);
                if (r.is_page_update()) 
                {                    
                    DBGOUT3(<<"is page update " );
                    DBGOUT5( << setiosflags(ios::right) << lsn 
                        << resetiosflags(ios::right) << " A: " 
                        << "is page update " << page_of_interest );
                    // redoable, has a pid, and is not compensated.
                    // Why the compensated predicate?
                    if (r.is_undo()) 
                    {
                        // r is undoable. Update next undo lsn of xct
                        xd->set_undo_nxt(lsn);
                    }

                    // Must be redoable
                    w_assert0(r.is_redo());

                    // These log records are not compensation log and affected buffer pool pages
                    // we need to record these in_doubt pages in buffer pool
                    // Exceptions:
                    // Allocation of a page (t_alloc_a_page, t_alloc_consecutive_pages) - clear
                    //           the in_doubt bit, because the page might be allocated for a 
                    //           non-logged operation, we don't want to re-format the page
                    // De-allocation of a page (t_dealloc_a_page, t_page_set_to_be_deleted) - 
                    //          clear the in_doubt bit, so the page can be evicted if needed.

                    if ((true == r.is_page_allocate()) ||
                        (true == r.is_page_deallocate()))
                    {
                        // Remove the in_doubt flag in buffer pool of the page if it exists in buffer pool
                        uint64_t key = bf_key(page_of_interest.vol().vol, page_of_interest.page);
                        bf_idx idx = smlevel_0::bf->lookup_in_doubt(key);
                        if (0 != idx)
                        {
                            // Page cb is in buffer pool, clear the 'in_doubt' and 'used'  flags
                            // If the cb for this page does not exist in buffer pool, no-op
                            if (true == smlevel_0::bf->is_in_doubt(idx))
                            {
                                if (true == r.is_page_allocate())
                                    smlevel_0::bf->clear_in_doubt(idx, true, key);   // Page is still used
                                else
                                    smlevel_0::bf->clear_in_doubt(idx, false, key);  // Page is not used
                                w_assert1(0 < in_doubt_count);
                                --in_doubt_count;
                            }
                        }
                    }
                    else
                    {
                        // Register the page cb in buffer pool (if not exist) and mark the in_doubt flag
                        idx = 0;
                        if (0 == page_of_interest.page)
                            W_FATAL_MSG(fcINTERNAL, 
                                << "Page # = 0 from a page in log record, log type = " << r.type());
                        rc = smlevel_0::bf->register_and_mark(idx, 
                                  page_of_interest, lsn, in_doubt_count);
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
                        DBGOUT5(<<"is cpsn, undo " << " undo_nxt<--lsn " << lsn );

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
                        // Majority of the compensation log should not be undoable                       
                        DBGOUT3(<<"is cpsn, not undo " << " set undo_next lsn to NULL");
                        xd->set_undo_nxt(lsn_t::null);
                    }

                    // Register the page cb in buffer pool (if not exist) and mark the in_doubt flag
                    if (r.is_redo())
                    {
                        idx = 0;
                        if (0 == page_of_interest.page)
                            W_FATAL_MSG(fcINTERNAL, 
                                << "Page # = 0 from a page in compensation log record");
                        rc = smlevel_0::bf->register_and_mark(idx, 
                                  page_of_interest, lsn, in_doubt_count);
                        if (rc.is_error()) 
                        {
                            // Not able to get a free block in buffer pool without evict, cannot continue in M1
                            W_FATAL_MSG(fcINTERNAL, 
                                << "Failed to record an in_doubt page for compensation record during Log Analysis");
                        }
                        w_assert1(0 != idx);
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
                }
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
    w_assert1(begin_chkpt == master);
    if (redo_lsn > master)
       redo_lsn = master;
    if (undo_lsn > master)
       undo_lsn = master;

    // If there were any mounts/dismounts that occured between redo_lsn and
    // begin chkpt, need to redo them
    DBGOUT3( << ((theLastMountLSNBeforeChkpt != lsn_t::null && 
                    theLastMountLSNBeforeChkpt > redo_lsn) \
            ? "redoing mounts/dismounts before chkpt but after redo_lsn"  \
            : "no mounts/dismounts need to be redone"));

    // At this point, we have mounted devices from t_chkpt_dev_tab log record and
    // also the individual mount/dismount log records
    // Do we have more to mount?
    if ( 0 != in_doubt_count)
    {
        logrec_t* __copy__buf = new logrec_t;
        if(! __copy__buf)
        {
            W_FATAL(eOUTOFMEMORY); 
        }
        w_auto_delete_t<logrec_t> auto_del(__copy__buf);
        logrec_t&         copy = *__copy__buf;

        // theLastMountLSNBeforeChkpt was from the begin checkpoint log record
        // it was the lsn of the last mount before the begin checkpoint
        while (theLastMountLSNBeforeChkpt != lsn_t::null 
            && theLastMountLSNBeforeChkpt > redo_lsn)  
        {

            W_COERCE(log->fetch(theLastMountLSNBeforeChkpt, log_rec_buf, 0));  

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

            // it is ok if the mount/dismount fails, since this 
            // may be caused by the destruction
            // of the volume.  if that was the case then there 
            // won't be updates that need to be
            // done/undone to this volume so it doesn't matter.
            if (copy.type() == logrec_t::t_dismount_vol)  
            {
                W_IGNORE(io_m::mount(dp->devrec[0].dev_name, dp->devrec[0].vid));
                mount = true;
            }
            else
            {
                W_IGNORE(io_m::dismount(dp->devrec[0].vid));
            }

            theLastMountLSNBeforeChkpt = copy.xid_prev();
        }

    // auto-release will free the log rec copy buffer, __copy__buf
    } 
    // Now theLastMountLSNBeforeChkpt == redo_lsn

    // Update the last mount LSN, it was originally set from the begin checkpoint log record
    // but it might have been modified to redo_lsn (earlier)
    io_m::SetLastMountLSN(theLastMountLSNBeforeChkpt);

    // We are done with Log Analysis, at this point each transactions in the transaction
    // table is either doomed (active) or ended; destroy the ended transactions
    // and populate the special heap with doomed (active) transactions for the UNDO phase.
    
    // After this step, only doomed transactions are left in the transaction table   
    // all of them have state 'active' and will be cleaned up in the UNDO phase.
    // We are not locking the transaction table during this process because 
    // we are in the Log Analysis phase and the system is not opened for new 
    // transaction yet
    // Similarly, no lock is required on the transaction table if we want to delete
    // ended transaction from transaction table
    
    {
        xct_i iter(false); // not locking the transaction table list
        xct_t*  xd; 
        xct_t*  curr;
        DBGOUT3( << "Building heap...");
        xd = iter.next();
        while (xd)
        {
            DBGOUT3( << "Transaction " << xd->tid() 
                     << " has state " << xd->state() );

            if (xct_t::xct_active == xd->state())  
            {
                // Doomed transaction
                heap.AddElementDontHeapify(xd);

                // Advance to the next transaction
                xd = iter.next();
            }
            else
            {
                if (xct_t::xct_ended == xd->state())
                {
                    // Ended transaction 
                    curr = iter.curr();
                    w_assert1(curr);

                    // Advance to the next transaction first
                    xd = iter.next();

////////////////////////////////////////
// TODO(Restart)... not handling ignore 'non-read-lock' in M1
//                    me()->attach_xct(curr);
//                    W_DO(curr->commit_free_locks());
//                    me()->detach_xct(curr);                   
////////////////////////////////////////

                    // Then destroy the ended transaction                    
                    xct_t::destroy_xct(curr);
                }
                else
                {
                    // We are not supposed ot see transaction with other stats
                    W_FATAL_MSG(fcINTERNAL, 
                        << "Transaction in the traction table is not doomed in Log Analysis phase, xd: "
                        << xd->tid());                   
                }
            }
        }
        // Done populating the heap, now tell the heap to sort
        heap.Heapify();

        DBGOUT3( << "Number of transaction entries in heap: " << heap.NumElements());
        DBGOUT3( << "Number of active transactions in transaction table: " << xct_t::num_active_xcts());
    }  // destroy iter which also unlock the transaction table

    w_base_t::base_stat_t f = GET_TSTAT(log_fetches);
    w_base_t::base_stat_t i = GET_TSTAT(log_inserts);
    smlevel_0::errlog->clog << info_prio 
        << "After analysis_pass: " 
        << f << " log_fetches, " 
        << i << " log_inserts " 
        << " redo_lsn is " << redo_lsn
        << " undo_lsn is " << undo_lsn
        << flushl;

    DBGOUT3 (<< "End of Log Analysis phase.  Master: " 
             << master << ", redo_lsn: " << redo_lsn
             << ", undo lsn: " << undo_lsn);

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

    return;
}

/*********************************************************************
 * 
 *  restart_m::redo_pass(redo_lsn, highest_lsn, in_doubt_count)
 *
 *  Scan log forward from redo_lsn. Base on entries in buffer pool, 
 *  apply redo if durable page is old.
 *
 *********************************************************************/
void 
restart_m::redo_pass(
    const lsn_t        redo_lsn,       // This is where the log scan should start
    const lsn_t&       highest_lsn,    // This is the current log LSN, REDO should not
                                       //generate log so this value should not change
    const uint32_t     in_doubt_count  // How many in_doubt pages in buffer pool
                                       // for validation purpose
)
{
////////////////////////////////////////
// TODO(Restart)... does not open the store for new transactions during REDO phase in M1
//                    ignore 'non-read-lock' in M1
////////////////////////////////////////

    FUNC(restart_m::redo_pass);

    if (0 == in_doubt_count)
    {
        // No in_doubt page in buffer pool, nothing to do in REDO phase
        return;
    }

    // REDO phase never writes its own log records or modify anything in 
    // the transaction table, this is an extra gurantee to make sure 
    // no new log record.
    
    AutoTurnOffLogging turnedOnWhenDestroyed;

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
    while (scan.xct_next(lsn, log_rec_buf))  
    {
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
            // Note compensation log records are 'redo only'

            // pid in log record is populated when a log record is filled
            // null_pid is checking the page numer (shpid_t) recorded in the log record
            if (r.null_pid()) 
            {
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
                        
                        w_assert9(r.type() == logrec_t::t_dismount_vol || 
                                    r.type() == logrec_t::t_mount_vol);
                        DBGOUT3(<<"redo - no page, no xct, this is a device log record ");

                        r.redo(0);
                        io_m::SetLastMountLSN(lsn);

                        // No page involved, no need to update dirty_count
                        redone = true;
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
                        if ((false == r.is_page_allocate()) &&
                            (false == r.is_page_deallocate()))
                        {                       
                            DBGOUT3(<<"redo - no page, ssx");
                            sys_xct_section_t sxs (true); // single log!
                            w_assert1(!sxs.check_error_on_start().is_error());
                            r.redo(0);
                            io_m::SetLastMountLSN(lsn);
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
                
                _redo_log_with_pid(r, lsn, highest_lsn, r.construct_pid(),
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
                    
                    _redo_log_with_pid(r, lsn, highest_lsn, r.construct_pid2(), 
                                       redone, dirty_count);
                }
            }
        }
        DBGOUT3( << setiosflags(ios::right) << lsn
                      << resetiosflags(ios::right) << " R: " 
                      << (redone ? " redone" : " skipped") );
    }

    if (in_doubt_count != dirty_count)
    {
        // We did not convert all the in_doubt pages, raise error and do not continue the Recovery
        W_FATAL_MSG(fcINTERNAL, 
                    << "Unexpected dirty page count at the end of REDO phase.  In_doubt count: "
                    << in_doubt_count << ", dirty count: " << dirty_count);        
    }

    {
        w_base_t::base_stat_t f = GET_TSTAT(log_fetches);
        w_base_t::base_stat_t i = GET_TSTAT(log_inserts);
        smlevel_0::errlog->clog << info_prio 
            << "Redo_pass: "
            << f << " log_fetches, " 
            << i << " log_inserts " << flushl;
    }

    return;
}

/*********************************************************************
* 
*  restart_m::_redo_log_with_pid(r, lsn,highest_lsn, page_updated, redone, dirty_count)
*
*  For each log record, load the physical page if it is not in buffer pool yet, set the flags
*  and apply the REDO based on log record if the page is old
*
*  Function returns void, if encounter error condition (any error), raise error and abort
*  the operation, it cannot continue
*
*********************************************************************/
void restart_m::_redo_log_with_pid(
    logrec_t& r,              // Incoming log record
    lsn_t &lsn,               // LSN of the incoming log record
    const lsn_t &highest_lsn, // This is the current LSN, REDO should not generate log record
                              // so this value should not change
                              // this is passed in for validation purpose
    lpid_t page_updated,      // Store ID (vol + store number) + page number
                              // This is mainly because if the log is a multi-page log
                              // this will be the information for the 2nd page
    bool &redone,             // Did REDO occurred, for validation purpose
    uint32_t &dirty_count)    // Counter for the number of in_doubt to dirty pages
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
    bool passed_end = false;    // True if we thought the page exists on disk but
                                // it does not exist (it was never flushed 
                                // before the crash

    // 'is_redo()' covers regular transaction, compensation transaction
    // and system transaction (if any)
    w_assert1(r.is_redo());
    w_assert1(r.shpid());

    // Because we are loading the page into buffer pool directly
    // we cannot have swizzling on
    w_assert1(!smlevel_0::bf->is_swizzling_enabled());

    uint64_t key = bf_key(page_updated.vol().vol, page_updated.page);
    bf_idx idx = smlevel_0::bf->lookup_in_doubt(key);
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
        // Acquire write latch for each page because we arw going to update      
        rc = cb.latch().latch_acquire(LATCH_EX, WAIT_FOREVER);
        if (rc.is_error())
        {
            // Unable to acquire write latch, cannot continue, raise an internal error
            DBGOUT3 (<< "Error when acquiring LATCH_EX for a page in buffer pool. pagw ID: "
                     << page_updated.page << ", rc = " << rc);
            W_FATAL_MSG(fcINTERNAL, << "unable to EX latch a buffer pool page");
            return;
        }

        if ((true == smlevel_0::bf->is_in_doubt(idx)) || (true == smlevel_0::bf->is_dirty(idx)))
        {
            fixable_page_h page;
            bool virgin_page = false;

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
                || (r.type() == logrec_t::t_btree_norec_alloc && page_updated.page != r.shpid())) 
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
                DBGOUT3 (<< "REDO phase, loading page from disk, page = " << page_updated.page);

                // If passed_end is true, the page does not exist on disk and the buffer pool page
                // has been zerod out, we cannot apply REDO in this case
                rc = smlevel_0::bf->load_for_redo(idx, page_updated.vol().vol,
                                                  page_updated.page, passed_end);

                if (true == passed_end)
                {
                    // Fetch a page from disk but the page does not exist
                    // This is not a valid situation because if the dirty page was never flushed
                    // to disk before the system crash, the Log Analysis phase trace the page
                    // history to find the original page format record, and the REDO phase
                    // starts its log scan from the earliest LSN, so we should always see the 
                    // page format log record for a dirty page which was not on disk.
                    // Raise error becasue we should not hit this error

                    cb.latch().latch_release();
                    W_FATAL_MSG(fcINTERNAL, 
                                << "REDO phase, expected page does not exist on disk.  Page: "
                                << page_updated.page);
                }
                if (rc.is_error()) 
                {
                    cb.latch().latch_release();                
                    if (eBADCHECKSUM == rc.err_num())
                    {
////////////////////////////////////////
// TODO(Restart)... if the loaded page is corrupted, 
//                    we do not have Single Page Recover
//                    in M1, must raise error and abort
////////////////////////////////////////

                        W_FATAL_MSG(fcINTERNAL, 
                                    << "Bad page checksum in page: " << page_updated.page);
                    }
                    else
                    {
                        // All other errors
                        W_FATAL_MSG(fcINTERNAL, 
                                    << "Failed to load physical page into buffer pool in REDO phase, page: "
                                    << page_updated.page << ", RC = " << rc);
                    }
                }

                // Just loaded from disk, set the vol and page in cb
                cb._pid_vol = page_updated.vol().vol;
                cb._store_num = page_updated.store(); 
                cb._pid_shpid = page_updated.page;
            }
            else if ((true == smlevel_0::bf->is_in_doubt(idx)) && (true == virgin_page))
            {
                // First time encounter this page and it is a virgin page
                // We have the page cb and hashtable entry for this page already
                // There is nothing to load from disk, set the vol and page in cb

                cb._pid_vol = page_updated.vol().vol;
                cb._store_num = page_updated.store(); 
                cb._pid_shpid = page_updated.page;
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
////////////////////////////////////////
// TODO(Restart)... not implemented
            // 3. If the PageLSN value in the page image in the buffer pool differs from the
            //   'prior PageLSN value' in the log record, report an error (no SPR)
////////////////////////////////////////
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

                // This function is shared by both Recovery and SPR, it sets the page
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
            else if ((page_lsn >= highest_lsn) && (lsn_t::null != page_lsn)) 
            {
                // Not a virgin page, highest_lsn is the current recovery log LSN
                // if the page last write LSN > highest_lsn, this cannot happen
                // we have a page corruption
                DBGOUT1( << "WAL violation! page "
                         << page.pid()
                         << " has lsn " << page_lsn
                         << " end of log is record prior to " << highest_lsn);

                cb.latch().latch_release();
                W_FATAL_MSG(fcINTERNAL, 
                    << "Page LSN > current recovery log LSN, page corruption detected in REDO phase, page: "
                    << page_updated.page);
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

            if (true == r.is_page_allocate())
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
                w_assert1(false == smlevel_0::bf->is_used(idx));
            }
            else if (true == r.is_page_deallocate())
            {
                // The idx should not be in hashtable
                cb.latch().latch_release();                
                W_FATAL_MSG(fcINTERNAL, 
                    << "Deallocated page should not exist in hashtable in REDO phase, page: " 
                    << page_updated.page);                
            }
            else
            {
                if (true == smlevel_0::bf->is_used(idx))
                {
                    // If the page 'used' flag is set but none of the other flags are on, and the log record
                    // is not page allocation or deallocation, we should not have this case
                    cb.latch().latch_release();                
                    W_FATAL_MSG(fcINTERNAL, 
                        << "Incorrect in_doubt and dirty flags in REDO phase, page: " 
                        << page_updated.page);
                }
            }
        }

        // Done, release write latch
        if (cb.latch().held_by_me())
            cb.latch().latch_release();
    }
    else
    {
        // The page cb is not in hashtable, the only valid case is if it is 
        // a page deallocation log, in such case the page has been removed from hashtable
        // all other cases are un-expected
        
        // Note that once a page is marked 'in_doubt', it cannot be evicted so
        // the page cb must be in the buffer pool (hashtable)
        if (false == r.is_page_deallocate())
        {
            W_FATAL_MSG(fcINTERNAL, 
                << "Unable to find page in buffer pool hashtable during REDO phase.  Vol: "
                << page_updated.vol().vol << ", page number: "
                << page_updated.page);                
        }
    }

    return;
}

/*********************************************************************
 *
 *  restart_m::undo_pass(heap, curr_lsn, undo_lsn)
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
 *********************************************************************/
void 
restart_m::undo_pass(
    XctPtrHeap&        heap,      // Heap populated with doomed transactions
    const lsn_t        curr_lsn,  // Current lsn, the starting point of backward scan
                                  // Not used currently
    const lsn_t        undo_lsn   // Undo_lsn, the end point of backward scan
                                  // Not used currently
    )
{
////////////////////////////////////////
// TODO(Restart)... does not open the store for new transactions during UNDO phase in M1
//                    ignore 'non-read-lock' in M1
////////////////////////////////////////

    FUNC(restart_m::undo_pass);

    int xct_count = heap.NumElements();
    if (0 == xct_count)
    {
        // No doomed transaction in transaction table, nothing to do in UNDO phase
        DBGOUT3(<<"No doomed transaction to undo");
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
// implementation which is using heap to record all the doom transactions
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
                    xd->set_undo_nxt(lsn_t::null);
                    heap.ReplacedFirst();
                    continue;
                }
            }
           
            DBGOUT3( << "Transaction " << xd->tid() 
                << " with undo_nxt lsn " << xd->undo_nxt()
                << " rolling back to " << heap.Second()->undo_nxt() 
                );

            // Note that this rollback/undo for doomed/in-flight transactions
            // which were marked as 'active' in the Log Analysis phase.
            // These transactions are marked 'active' in the transaction table
            // so the standard rooback/abort logic works.
            // We will open the store for new transactions after Log Analysis,
            // new incoming transaction should have different TID and not confused 
            // with the doomed (marked as active) transactions
            
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
        // For all the doom transactions in the heap
        // destroy them from the transaction table
        
        xd = heap.RemoveFirst();

        // Note that all transaction has been rolled back, excpet a special case
        // where there was only one transaction in the heap, in such case the 
        // actual rollback will happen here
        
        w_assert9(xd->undo_nxt() == lsn_t::null || heap.NumElements() == 0);

        DBGOUT3( << "Transaction " << xd->tid() 
                << " is rolled back: aborting it now " );

        me()->attach_xct(xd);

        // Abort the transaction, this is using the standard transaction abort logic, 
        // which release locks (which was not involved in the roll back to save point operation),
        // generate an end transaction log record if any log has been generated by
        // this transaction (i.e. compensation records), and change state accordingly       
        // Because we are using the standard abort logic, all the in-flight /doomed transactions
        // were marked as 'active' so abort() works correctly
        W_COERCE( xd->abort() );

        xct_t::destroy_xct(xd);
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

    return;
}



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

