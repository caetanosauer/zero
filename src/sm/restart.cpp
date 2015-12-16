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
#include "crash.h"
#include "sm_base.h"
#include "sm_du_stats.h"
#include "sm_base.h"
#include "btree_impl.h"         // Lock re-acquisition
#include "restart.h"
#include "btree_logrec.h"       // Lock re-acquisition
#include "sm.h"                 // Check system shutdown status
#include "stopwatch.h"

#include <fcntl.h>              // Performance reporting
#include <unistd.h>
#include <sstream>

restart_m::restart_m(const sm_options& options)
{
    _restart_thread = NULL;
    instantRestart = options.get_bool_option("sm_restart_instant", true);
}

restart_m::~restart_m()
{
    // If we are still in Log Analysis phase, no child thread yet, go ahead and terminate

    if (_restart_thread)
    {
        if (smlevel_0::shutdown_clean)
        {
            // Clean shutdown, try to let the child thread finish its work
            // This would happen only if user shutdowns the system
            // as soon as the system is open (concurrent recovery just started)
            DBGOUT2(<< "waiting for recovery child thread to finish...");

            // Wait for the child thread to join, we want to give the child thread some time to
            // finish its work but we don't want to wait forever, so we are giving
            // some time to the child thread
            // If the child thread did not have enough time to finish the recovery
            // work (e.g., a lot of recovery work to do) after the wait, terminate the
            // child thread with a message
            // In this case, the normal shutdown becomes a force shutdown, meaning
            // the next server startup will need to recovery again
            // This can happen in concurrent recovery mode because system is opened
            // while the recovery is still going on

            w_rc_t rc = _restart_thread->join(wait_interval);  // Another wait to give child thread more time
            if (rc.is_error())
            {
                DBGOUT1(<< "Normal shutdown - child thread join error: " << rc);
                smlevel_0::errlog->clog << info_prio
                    << "Normal shutdown - child thread join error: " << rc << flushl;
            }
        }
        else
        {
            // Simulated crash, just kill the child thread, no wait
        }

        // Terminate the child thread
        delete _restart_thread;
        _restart_thread = 0;
    }
    w_assert1(!_restart_thread);

    // Okay to destroy the restart_m now
}
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
restart_m::restart()
{
    stopwatch_t timer;

    log_analysis();

    ADD_TSTAT(restart_log_analysis_time, timer.time_us());

    if (!instantRestart)
    {
        // Perform log-based REDO without opening system (ARIES mode)
        lsn_t curr_lsn = smlevel_0::log->curr_lsn();
        redo_log_pass(chkpt.get_min_rec_lsn(), curr_lsn);
        ADD_TSTAT(restart_redo_time, timer.time_us());
    }

    // System can now be opened up for new transactions.
    // REDO and UNDO will be executed concurrently
}

void restart_m::log_analysis()
{
    smlevel_0::operating_mode = smlevel_0::t_in_analysis;

    chkpt.scan_log();

    //Re-create transactions
    xct_t::update_youngest_tid(chkpt.get_highest_tid());
    for(xct_tab_t::const_iterator it = chkpt.xct_tab.begin();
                            it != chkpt.xct_tab.end(); ++it)
    {
        xct_t* xd = new xct_t(NULL,               // stats
                        WAIT_SPECIFIED_BY_THREAD, // default timeout value
                        false,                    // sys_xct
                        false,                    // single_log_sys_xct
                        it->first,
                        it->second.last_lsn,      // last_LSN
                        it->second.last_lsn,      // next_undo == last_lsn
                        true);                    // loser_xct, set to true for recovery

        xd->set_first_lsn(it->second.first_lsn); // Set the first LSN of the in-flight transaction
        xd->set_last_lsn(it->second.last_lsn);   // Set the last lsn in the transaction

        //Re-acquire locks
        for(vector<lock_info_t>::const_iterator jt = it->second.locks.begin();
                jt != it->second.locks.end(); ++it)
        {
            W_COERCE(btree_impl::_ux_lock_key(jt->lock_hash, jt->lock_mode,
                        false /*check_only*/, xd));
        }
    }

    //Re-add backups
    // CS TODO only works for one backup
    smlevel_0::vol->sx_add_backup(chkpt.bkp_path, true);
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
    const lsn_t redo_lsn,
    const lsn_t& end_logscan_lsn
)
{
    // How many pages have been changed from in_doubt to dirty?
    uint32_t dirty_count = 0;

    // Open a forward scan of the recovery log, starting from the redo_lsn which
    // is the earliest lsn determined in the Log Analysis phase
    DBGOUT3(<<"Start redo scanning at redo_lsn = " << redo_lsn);
    log_i scan(*smlevel_0::log, redo_lsn);
    lsn_t cur_lsn = smlevel_0::log->curr_lsn();
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
    while (scan.xct_next(lsn, r))
    {
        // The difference between serial and concurrent modes with
        // log scan driven REDO:
        //     Concurrent mode needs to know when to stop the log scan
        if ((lsn > end_logscan_lsn))
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

    bf_idx idx = smlevel_0::bf->lookup(page_updated);
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

        if (smlevel_0::bf->is_dirty(idx))
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

            if (!virgin_page)
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
            else
            {
                // First time encounter this page and it is a virgin page
                // We have the page cb and hashtable entry for this page already
                // There is nothing to load from disk, set the vol and page in cb

                cb._store_num = r.stid();
                cb._pid_shpid = page_updated;
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
                if (virgin_page)
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
            if (redone)
            {
                // Turn the in_doubt flag into the dirty flag
                smlevel_0::bf->set_dirty(page.get_generic_page());

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

void restart_m::redo_concurrent_pass()
{
    generic_page* page;
    stopwatch_t timer;

    buf_tab_t::const_iterator iter = chkpt.buf_tab.begin();
    while (iter != chkpt.buf_tab.end()) {
        PageID pid = iter->first;
        lsn_t lastLSN = iter->second.page_lsn;

        // simply fixing the page will take care of single-page recovery
        W_COERCE(smlevel_0::bf->fix_nonroot(
                    page, NULL, pid, LATCH_SH, false, false, lastLSN));
        smlevel_0::bf->unfix(page);

        iter++;
    }

    ADD_TSTAT(restart_redo_time, timer.time_us());
    ERROUT(<< "Finished concurrent REDO of " << chkpt.buf_tab.size() << " pages");
}

void restart_m::undo_concurrent_pass()
{
    // If nothing in the transaction table, then nothing to process
    if (0 == xct_t::num_active_xcts())
    {
        DBGOUT3(<<"No loser transaction to undo");
        return;
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
                    // If mixed mode, it is possible and valid if failed to acquire
                    // latch on a transaction, because a concurrent user transaction
                    // might be checking or triggered a rollback on this transaction
                    // (if it is a loser transaction)
                    // Eat the error and skip this transaction, if thi sis a loser transaction
                    // rely on concurrent transaction to rollback this loser transaction
                    xd = iter.next();
                    continue;
            }
        }
        // CS TODO: which exception are we expecting to cacth here???
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

    W_COERCE( smlevel_0::log->flush_all() );
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

    gettimeofday(&tm_before, NULL);

    // REDO, call back to restart_m to carry out the concurrent REDO
    working = true;
    smlevel_0::recovery->redo_concurrent_pass();

    gettimeofday(&tm_after, NULL);
    DBGOUT1(<< "**** Restart child thread REDO, elapsed time (milliseconds): "
            << (((double)tm_after.tv_sec - (double)tm_before.tv_sec) * 1000.0)
            + (double)tm_after.tv_usec/1000.0 - (double)tm_before.tv_usec/1000.0);

    // UNDO, call back to restart_m to carry out the concurrent UNDO
    working = true;
    smlevel_0::recovery->undo_concurrent_pass();

    // Done
    DBGOUT1(<< "restart_thread_t: Finished REDO and UNDO tasks");
    working = false;

    gettimeofday( &tm_done, NULL );
    DBGOUT1(<< "**** Restart child thread UNDO, elapsed time (milliseconds): "
            << (((double)tm_done.tv_sec - (double)tm_after.tv_sec) * 1000.0)
            + (double)tm_done.tv_usec/1000.0 - (double)tm_after.tv_usec/1000.0);
};

