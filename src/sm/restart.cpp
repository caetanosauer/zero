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
#include "sm_base.h"
#include "w_heap.h"
#include "sm_base.h"
#include "btree_impl.h"         // Lock re-acquisition
#include "restart.h"
#include "btree_logrec.h"       // Lock re-acquisition
#include "sm.h"                 // Check system shutdown status
#include "stopwatch.h"
#include "xct_logger.h"
#include "bf_tree.h"

#include <fcntl.h>              // Performance reporting
#include <unistd.h>
#include <sstream>
#include <iomanip>

restart_m::restart_m(const sm_options& options)
    : _restart_thread(NULL)
{
    _no_db_mode = options.get_bool_option("sm_no_db", false);
}

restart_m::~restart_m()
{
    if (_restart_thread)
    {
        W_COERCE(_restart_thread->join());
        delete _restart_thread;
    }
}

void restart_m::log_analysis()
{
    stopwatch_t timer;

    chkpt.scan_log(lsn_t::null, _no_db_mode);

    //Re-create transactions
    xct_t::update_youngest_tid(chkpt.get_highest_tid());
    for(xct_tab_t::const_iterator it = chkpt.xct_tab.begin();
                            it != chkpt.xct_tab.end(); ++it)
    {
        xct_t* xd = new xct_t(NULL,               // stats
                        timeout_t::WAIT_FOREVER, // default timeout value
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
                        xd, timeout_t::WAIT_FOREVER, &entry));
        }
    }

    //Re-add backups
    // CS TODO only works for one backup
    // smlevel_0::vol->sx_add_backup(chkpt.bkp_path, true);

    ADD_TSTAT(restart_log_analysis_time, timer.time_us());
    Logger::log_sys<loganalysis_end_log>();

    ERROUT(<< "Log analysis found "
            << chkpt.buf_tab.size() << " dirty pages and "
            << chkpt.xct_tab.size() << " active transactions");
    // chkpt.dump(cerr);
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
        cerr << "Redoing log from " << redo_lsn
            << " to " << cur_lsn << endl;
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
        if (lsn == expected_lsn) {
            expected_lsn.advance(r.length());
        }
        else {
            w_assert1(lsn == lsn_t(expected_lsn.hi() + 1, 0));
            expected_lsn = lsn;
            expected_lsn.advance(r.length());
        }

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
                if (!r.is_single_sys_xct() && r.tid() != 0)
                {
                    // Regular transaction with a valid txn id
                    xct_t *xd = xct_t::look_up(r.tid());
                    if (xd)
                    {
                        if (xd->state() == xct_t::xct_active)
                        {
                            DBGOUT3(<<"redo - no page, xct is " << r.tid());
                            r.redo();

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

                        r.redo();
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
                            r.redo();
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

        // ERROUT(<< "redo_log_pass: " << lsn << " " << r.type_str() << " pid " << r.pid()
        //         << (redone ? " redone" : " skipped") );

    }

    ADD_TSTAT(restart_redo_time, timer.time_us());
    Logger::log_sys<redo_done_log>();
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
        w_assert1(pid == r.pid() || pid == r.pid2());
        w_assert1(pid != r.pid() || (r.page_prev_lsn() == lsn_t::null ||
            r.page_prev_lsn() == page_lsn));

        w_assert1(pid != r.pid2() || (r.page2_prev_lsn() == lsn_t::null ||
            r.page2_prev_lsn() == page_lsn));

        w_assert1(page.is_fixed());
        r.redo(&page);
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
                    page, NULL, pid, LATCH_SH, false, false, false, lastLSN));
        smlevel_0::bf->unfix(page);

        iter++;
    }

    ADD_TSTAT(restart_redo_time, timer.time_us());
    ERROUT(<< "Finished concurrent REDO of " << chkpt.buf_tab.size() << " pages");
    Logger::log_sys<redo_done_log>();
}

void restart_m::undo_pass()
{
    // If nothing in the transaction table, then nothing to process
    if (0 == xct_t::num_active_xcts())
    {
        DBGOUT3(<<"No loser transaction to undo");
        return;
    }

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
            w_rc_t latch_rc = xd->latch().latch_acquire(LATCH_EX, timeout_t::WAIT_FOREVER);
            if (latch_rc.is_error())
            {
                    // If mixed mode, it is possible and valid if failed to acquire
                    // latch on a transaction, because a concurrent user transaction
                    // might be checking or triggered a rollback on this transaction
                    // (if it is a loser transaction)
                    // Eat the error and skip this transaction, if thi sis a loser transaction
                    // rely on concurrent transaction to rollback this loser transaction
                    DBGOUT3(<< "Skipped " << xd->tid() << " due to latch failure");
                    xd = iter.next();
                    continue;
            }
        }
        // CS TODO: which exception are we expecting to cacth here???
        catch (...)
        {
            w_assert0(false);
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
                    // CS TODO: What's with these "just in case" solutions?
                    w_assert0(false);

                    // We should not get here but j.i.c.
                    // Set undo_nxt to NULL so it cannot be rollback
                    curr->set_undo_nxt(lsn_t::null);
                }
                else
                {
                    // Normal transaction

                    DBGOUT3( << "Rolling back txn " << curr->tid()
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

                    smthread_t::attach_xct(curr);
                    W_COERCE(xct_t::abort());

                    // Then destroy the loser transaction
                    delete curr;
                }
            }
            else
            {
                // Loser transaction but no undo_nxt, must be a compensation
                // operation, nothing to undo
                // CS TODO: can this ever happen? Can we just ignore it like that?
                w_assert0(false);
            }
        }
        else
        {
            if (xd->latch().held_by_me())
                xd->latch().latch_release();

            DBGOUT3(<< "Skipped " << xd->tid() << " -- not loser txn or already rolling back");
            // All other transaction, ignore and advance to the next txn
            xd = iter.next();
        }
    }

    // All loser transactions have been taken care of now
    // Force a recovery log flush, this would harden the log records
    // generated by compensation operations

    W_COERCE( smlevel_0::log->flush_all() );
    Logger::log_sys<undo_done_log>();
}

//*********************************************************************
// Main body of the child thread restart_thread_t for Recovery process
// Only used if system is in concurrent recovery mode, while the system was
// opened after Log Analysis phase to allow concurrent user transactions
//*********************************************************************
void restart_thread_t::run()
{
    // CS TODO: add mechanism to interrupt restart thread and terminate
    // before recovery is complete
    smlevel_0::recovery->redo_page_pass();
    smlevel_0::recovery->undo_pass();
    smlevel_0::log->discard_fetch_buffers();
};

/************************************************************************
 * SINGLE_PAGE RECOVERY
 */

void restart_m::dump_page_lsn_chain(std::ostream &o, const PageID &pid, const lsn_t &max_lsn)
{
    lsn_t scan_start = smlevel_0::log->durable_lsn();
    log_i           scan(*smlevel_0::log, scan_start);
    logrec_t        buf;
    lsn_t           lsn;
    // Scan all log entries until EMLSN
    while (scan.xct_next(lsn, buf) && buf.lsn_ck() <= max_lsn) {
        if (buf.type() == logrec_t::t_chkpt_begin) {
            o << "  CHECKPT: " << buf << std::endl;
            continue;
        }

        PageID log_pid = buf.pid();
        PageID log_pid2 = log_pid;
        if (buf.is_multi_page()) {
            log_pid2 = buf.data_ssx_multi()->_page2_pid;
        }

        // Is this page interesting to us?
        if (pid != 0 && pid != log_pid && pid != log_pid2) {
            continue;
        }

        o << "  LOG: " << buf << ", P_PREV=" << buf.page_prev_lsn();
        if (buf.is_multi_page()) {
            o << ", P2_PREV=" << buf.data_ssx_multi()->_page2_prv << std::endl;
        }
        o << std::endl;
    }
}

void grow_buffer(char*& buffer, size_t& buffer_capacity, size_t pos, logrec_t** lr)
{
    DBGOUT1(<< "Doubling SPR buffer capacity");
    buffer_capacity *= 2;
    char* tmp = new char[buffer_capacity];
    memcpy(tmp, buffer, buffer_capacity/2);
    delete[] buffer;
    buffer = tmp;
    if (lr) {
        *lr = (logrec_t*) (buffer + pos);
        w_assert1((*lr)->length() <= buffer_capacity - pos);
    }
}

SprIterator::SprIterator(PageID pid, lsn_t firstLSN, lsn_t lastLSN,
        bool prioritizeArchive)
    :
    buffer_capacity{1 << 18 /* 256KB */},
    last_lsn{lsn_t::null},
    replayed_count{0}
{
    if (!lastLSN.is_null()) {
        // make sure log is durable until the lsn we're trying to fetch
        smlevel_0::log->flush(lastLSN);
    }

    // Allocate initial buffer -- expand later if needed
    // CS: regular allocation is fine since SPR isn't such a critical operation
    buffer = new char[buffer_capacity];
    size_t pos = 0;

    lsn_t archivedLSN = lsn_t::null;
    if (smlevel_0::logArchiver) {
        archivedLSN = smlevel_0::logArchiver->getIndex()->getLastLSN();
    }

    lsn_t nxt = lastLSN;
    bool left_early = false;
    while (firstLSN < nxt && nxt != lsn_t::null) {
        // If nxt has been archived already, fetch it from the archive
        if (nxt < archivedLSN && prioritizeArchive) {
            left_early = true;
            break;
        }

        // STEP 1: Fecth log record and copy it into buffer
        lsn_t lsn = nxt;
        logrec_t* lr = (logrec_t*) (buffer + pos);
        rc_t rc = smlevel_0::log->fetch(lsn, buffer + pos, NULL, true);

        if ((rc.is_error()) && (eEOF == rc.err_num())) {
            // EOF -- scan finished
            left_early = true;
            break;
        }
        else { W_COERCE(rc); }
        w_assert0(lsn == nxt);

        if (sizeof(logrec_t) > buffer_capacity - pos) {
            grow_buffer(buffer, buffer_capacity, pos, &lr);
        }

        lr_offsets.push_front(pos);
        pos += lr->length();

        // STEP 2: Obtain LSN of previous log record on the same page (nxt)

        // follow next pointer. This log might touch multi-pages. So, check both cases.
        if (pid == lr->pid())
        {
            // Target pid matches the first page ID in the log recoredd
            nxt = lr->page_prev_lsn();
        }
        else {
            w_assert0(lr->is_multi_page());
            // Multi-page log record, this is a page rebalance log record (split or merge)
            // while the 2nd page is the source page
            // In this case, the page we are trying to recover was the source page during
            // a page rebalance operation, follow the proper log chain
            w_assert0(lr->data_ssx_multi()->_page2_pid == pid);
            nxt = lr->data_ssx_multi()->_page2_prv;
        }

        // In the cases below, the scan can stop since the page is initialized
        // with this log record
        if (lr->has_page_img(pid)) { break; }
    }

    if ((lastLSN.is_null() || left_early) && ss_m::logArchiver) {
        // Reached EOF when scanning log backwards looking for current_lsn.
        // This means that the log record we're looking for is not in the recovery
        // log anymore and has probably been archived already.
        // Note that we should only get here if write elision is active, because
        // otherwise the log recycler in log_storage should now allow deletion
        // of old log files as long as they might be needed for restart recovery.

        // What we have to do now is fetch the log records between current_lsn and
        // nxt (both exclusive intervals) from the log archive and add them into
        // the buffer as well.
        archive_scan.reset(new ArchiveScanner{ss_m::logArchiver->getIndex()});
        merger.reset(archive_scan->open(pid, pid+1, firstLSN, 0));
    }

    lr_iter = lr_offsets.begin();
}

bool SprIterator::next(logrec_t*& lr)
{
    if (merger) {
        bool ret = merger->next(lr);
        if (ret) {
            last_lsn = lr->lsn();
            replayed_count++;
            return true;
        }

        // archive scan is over -- delete it
        merger.reset();
    }

    if (buffer) {
        lsn_t curr_lsn = lsn_t::null;
        while (curr_lsn <= last_lsn && lr_iter != lr_offsets.end()) {
            lr = reinterpret_cast<logrec_t*>(buffer + *lr_iter);
            lr_iter++;
            curr_lsn = lr->lsn();
        }
        if (curr_lsn > last_lsn) {
            replayed_count++;
            last_lsn = curr_lsn;
            return true;
        }
    }

    return false;
}

SprIterator::~SprIterator()
{
    if (buffer) { delete[] buffer; }
}

void SprIterator::apply(fixable_page_h &p)
{
    lsn_t prev_lsn = lsn_t::null;
    PageID pid = p.pid();
    logrec_t* lr;

    while (next(lr)) {
        w_assert1(lr->valid_header(lsn_t::null));
        w_assert1(replayed_count == 1 || lr->is_multi_page() ||
               (prev_lsn == lr->page_prev_lsn() && p.pid() == lr->pid()));

        if (lr->is_redo() && p.lsn() < lr->lsn()) {
            DBGOUT1(<< "SPR page(" << p.pid()
                    << ") LSN=" << p.lsn() << ", log=" << *lr);

            w_assert1(pid == lr->pid() || pid == lr->pid2());
            w_assert1(lr->has_page_img(pid) || pid != lr->pid()
                    || (lr->page_prev_lsn() == lsn_t::null
                    || lr->page_prev_lsn() == p.lsn()));

            w_assert1(pid != lr->pid2() || (lr->page2_prev_lsn() == lsn_t::null ||
                        lr->page2_prev_lsn() == p.lsn()));

            lr->redo(&p);
        }

        prev_lsn = lr->lsn();
    }
}
