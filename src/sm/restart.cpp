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

void restart_m::log_analysis()
{
    stopwatch_t timer;

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
                jt != it->second.locks.end(); ++jt)
        {
            // cout << "Locking " << jt->lock_hash << " in " << jt->lock_mode <<
            //     " for " << xd->tid() << endl;
            RawLock* entry;
            W_COERCE(smlevel_0::lm->lock(jt->lock_hash, jt->lock_mode,
                        false /*check*/, false /*wait*/, true /*acquire*/,
                        xd, WAIT_SPECIFIED_BY_XCT, &entry));
        }
    }

    //Re-add backups
    // CS TODO only works for one backup
    smlevel_0::vol->sx_add_backup(chkpt.bkp_path, true);

    ADD_TSTAT(restart_log_analysis_time, timer.time_us());

    ERROUT(<< "Log analysis found "
            << chkpt.buf_tab.size() << " dirty pages and "
            << chkpt.xct_tab.size() << " active transactions");
    chkpt.dump(cerr);
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
restart_m::redo_log_pass()
{
    stopwatch_t timer;
    // Perform log-based REDO without opening system (ARIES mode)
    lsn_t end_logscan_lsn = smlevel_0::log->curr_lsn();
    lsn_t redo_lsn = chkpt.get_min_rec_lsn();

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
        if ((lsn > end_logscan_lsn))
        {
            // If concurrent recovery, user transactions would generate new log records
            // stop forward scanning once we passed the end_logscan_lsn (passed in by caller)
            break;
        }

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
            /*
             * CS TODO: in the new extent-based allocation mechanism, 0 is a
             * valid page ID, whereas before it was considered an indication
             * of a log record that does not affect any page. Workaround in
             * the if clause below, which tests if log record is "page-less",
             * by checking if page id is 0 AND if it's not an operation that
             * would apply to the alloc_page, which can have pid 0.
             */
            if (r.pid() == 0 && r.type() != logrec_t::t_alloc_page &&
                    r.type() != logrec_t::t_dealloc_page)
            {
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
                }
                else
                {
                    if (!r.is_single_sys_xct())
                    {
                        // Regular transaction without a valid txn id
                        // It must be a mount or dismount log record

                        w_assert3(
                                r.type() == logrec_t::t_chkpt_backup_tab ||
                                r.type() == logrec_t::t_add_backup);

                        r.redo(0);
                    }
                    else
                    {
                        if ( // CS TODO -- restore not supported yet
                                r.type() != logrec_t::t_restore_begin
                                && r.type() != logrec_t::t_restore_segment
                                && r.type() != logrec_t::t_restore_end
                           )
                        {
                            DBGOUT3(<<"redo - no page, ssx");
                            sys_xct_section_t sxs (true); // single log!
                            w_assert1(!sxs.check_error_on_start().is_error());
                            r.redo(0);
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

                _redo_log_with_pid(r, r.pid(), redone, dirty_count);
                if (r.is_multi_page())
                {
                    w_assert1(r.is_single_sys_xct());
                    _redo_log_with_pid(r, r.pid2(), redone, dirty_count);
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

    ADD_TSTAT(restart_redo_time, timer.time_us());
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
void restart_m::_redo_log_with_pid(logrec_t& r, PageID pid,
        bool &redone, uint32_t &dirty_count)
{
    redone = false;             // True if REDO happened
    w_assert1(r.is_redo());

    bool virgin_page = r.type() == logrec_t::t_page_img_format
            || (r.type() == logrec_t::t_btree_split && pid == r.pid());

    fixable_page_h page;
    W_COERCE(page.fix_direct(pid, LATCH_EX, false, virgin_page));

    if (virgin_page) {
        page.get_generic_page()->pid = pid;
    }

    /// page.lsn() is the last write to this page
    lsn_t page_lsn = page.lsn();

    if (page_lsn < r.lsn())
    {
        w_assert1(page.is_fixed());
        r.redo(&page);
        page.update_initial_and_last_lsn(r.lsn());
        page.update_clsn(r.lsn());
        redone = true;
        ++dirty_count;
    }
}

void restart_m::redo_page_pass()
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

void restart_m::undo_pass()
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
    smlevel_0::recovery->redo_page_pass();

    gettimeofday(&tm_after, NULL);
    DBGOUT1(<< "**** Restart child thread REDO, elapsed time (milliseconds): "
            << (((double)tm_after.tv_sec - (double)tm_before.tv_sec) * 1000.0)
            + (double)tm_after.tv_usec/1000.0 - (double)tm_before.tv_usec/1000.0);

    // UNDO, call back to restart_m to carry out the concurrent UNDO
    working = true;
    smlevel_0::recovery->undo_pass();

    // Done
    DBGOUT1(<< "restart_thread_t: Finished REDO and UNDO tasks");
    working = false;

    gettimeofday( &tm_done, NULL );
    DBGOUT1(<< "**** Restart child thread UNDO, elapsed time (milliseconds): "
            << (((double)tm_done.tv_sec - (double)tm_after.tv_sec) * 1000.0)
            + (double)tm_done.tv_usec/1000.0 - (double)tm_after.tv_usec/1000.0);
};

