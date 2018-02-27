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

restart_thread_t::restart_thread_t(const sm_options& options)
    : logAnalysisFinished(false)
{
    smthread_t::set_lock_timeout(timeout_t::WAIT_FOREVER);

    instantRestart = options.get_bool_option("sm_restart_instant", true);
    log_based = options.get_bool_option("sm_restart_log_based_redo", true);
    no_db_mode = options.get_bool_option("sm_no_db", false);
    write_elision = options.get_bool_option("sm_write_elision", false);
    take_chkpt = options.get_bool_option("sm_chkpt_after_log_analysis", false);
    // CS TODO: instant restart should also allow log-based redo
    if (instantRestart) { log_based = false; }

    // Thread waits for wakeup anyway with default interval -1, so we can fork
    fork();

    clean_lsn = lsn_t::null;
};

void restart_thread_t::log_analysis()
{
    stopwatch_t timer;

    chkpt.scan_log(lsn_t::null);

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

    ADD_TSTAT(restart_log_analysis_time, timer.time_us());
    Logger::log_sys<loganalysis_end_log>();

    SET_TSTAT(restart_dirty_pages, chkpt.buf_tab.size());
    ERROUT(<< "Log analysis found "
            << chkpt.buf_tab.size() << " dirty pages and "
            << chkpt.xct_tab.size() << " active transactions");

    if (chkpt.ongoing_restore) {
        ERROUT(<< "Log analysis found ongoing restore with "
                << chkpt.restore_tab.size() << " restored segments of a total "
                << chkpt.restore_page_cnt << " pages");
    }

    // chkpt.dump(cerr);
    logAnalysisFinished = true;
}

/*********************************************************************
 *
 *  restart_thread_t::redo_log_pass(redo_lsn, end_logscan_lsn, in_doubt_count)
 *
 *  Scan log forward from redo_lsn. Base on entries in buffer pool,
 *  apply redo if durable page is old.
 *
 *  M1 only while system is not opened during the entire Recovery process
 *
 *********************************************************************/
void
restart_thread_t::redo_log_pass()
{
    stopwatch_t timer;

    lsn_t dur_lsn = smlevel_0::log->durable_lsn();
    lsn_t redo_lsn = chkpt.get_min_rec_lsn();
    if (redo_lsn.is_null()) { return; }

    // Open a forward scan of the recovery log, starting from the redo_lsn which
    // is the earliest lsn determined in the Log Analysis phase
    const size_t blockSize = 1048576;
    LogConsumer iter {redo_lsn, blockSize};
    iter.open(dur_lsn);

    if(redo_lsn < dur_lsn) {
        ERROUT(<< "Redoing log from " << redo_lsn << " to " << dur_lsn);
    }

    logrec_t* lr;
    bool redone = false;
    while (iter.next(lr))
    {
        if (should_exit()) { return; }

        if (lr->is_redo())
        {
            _redo_log_with_pid(*lr, lr->pid(), redone);

            if (lr->is_multi_page()) {
                w_assert1(lr->is_single_sys_xct());
                _redo_log_with_pid(*lr, lr->pid2(), redone);
            }
        }

        DBGOUT5(<< "redo_log_pass: (" << (redone ? " redone" : " skipped") << ") " << *lr);
    }

    ADD_TSTAT(restart_redo_time, timer.time_us());
    Logger::log_sys<redo_done_log>();
}

void restart_thread_t::_redo_log_with_pid(logrec_t& r, PageID pid, bool &redone)
{
    redone = false;
    w_assert1(r.is_redo());

    fixable_page_h page;
    bool virgin_page = r.has_page_img(pid);
    constexpr bool conditional = false, only_if_hit = false, do_recovery = false;
    W_COERCE(page.fix_direct(pid, LATCH_EX, conditional, virgin_page,
                only_if_hit, do_recovery));

    lsn_t page_lsn = page.lsn();
    if (page_lsn < r.lsn())
    {
        w_assert1(pid == r.pid() || pid == r.pid2());
        w_assert1(r.has_page_img(pid) || pid != r.pid()
                    || (r.page_prev_lsn() == lsn_t::null
                    ||  r.page_prev_lsn() == page_lsn));

        w_assert1(pid != r.pid2() || (r.page2_prev_lsn() == lsn_t::null ||
            r.page2_prev_lsn() == page_lsn));

        w_assert1(page.is_fixed());
        r.redo(&page);
        redone = true;
    }
}

void restart_thread_t::redo_page_pass()
{
    stopwatch_t timer;

    auto page_cnt = get_dirty_page_count();
    for (auto e : chkpt.buf_tab) {
        auto pid = e.first;
        // simply fixing the page will take care of single-page recovery
        fixable_page_h p;
        p.fix_direct(pid, LATCH_SH);

        if (should_exit()) { return; }
    }

    ADD_TSTAT(restart_redo_time, timer.time_us());
    ERROUT(<< "Finished REDO of " << page_cnt << " pages");
    Logger::log_sys<redo_done_log>();
}

void restart_thread_t::undo_pass()
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

    // (comment by WG) TODO This logic works while new transactions are
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
                    W_COERCE( curr->abort() );

                    // Then destroy the loser transaction
                    delete curr;
                }
            }
            else
            {
                // Loser transaction but no undo_nxt, must be a compensation
                // operation, nothing to undo
                // CS TODO: can this ever happen? Can we just ignore it like that?
                // w_assert0(false);
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

void restart_thread_t::do_work()
{
    if (take_chkpt) {
        smlevel_0::chkpt->take(&chkpt);
    }

    if (should_exit()) { return; }

    // No redo recovery needed in no-db mode
    // (dirty page table remains)
    if (no_db_mode)
    {
        undo_pass();
        // smlevel_0::log->discard_fetch_buffers();
        quit();
        return;
    }

    // Do undo pass first, since it's usually much shorter than redo.
    if (instantRestart) { undo_pass(); }

    if (should_exit()) { return; }

    if (log_based) { redo_log_pass(); }
    else { redo_page_pass(); }

    // In traditional ARIES, undo must come after redo
    if (!instantRestart) { undo_pass(); }

    // Cannot free fetch buffers here because there is no safe refcount
    // mechanism to avoid accessing buffers after they've been freed.
    // Fetch buffers are currently discarded on partition recycler.
    // smlevel_0::log->discard_fetch_buffers();

    // Now we can wake up cleaner. Otherwise, reads on the DB device will
    // have to compete with the cleaner's writes during recovery, making
    // restart and warm-up time substantially higher.
    // smlevel_0::bf->wakeup_cleaner();

    // Clear dirty page and active transaction tables
    // CS TODO: should not do this with write elision/nodb/etc!
    // clear_chkpt();

    // restart only does one round, so we quit voluntarily here
    quit();
};

/************************************************************************
 * SINGLE_PAGE RECOVERY
 */

void restart_thread_t::dump_page_lsn_chain(std::ostream &o, const PageID &pid, const lsn_t &max_lsn)
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

SprIterator::SprIterator()
    : buffer_capacity{1 << 18 /* 256KB */}
    , archive_scan{smlevel_0::logArchiver ? smlevel_0::logArchiver->getIndex() : nullptr}
{
    // Allocate initial buffer -- expand later if needed
    buffer = new char[buffer_capacity];
}

SprIterator::~SprIterator()
{
    delete[] buffer;
}

void SprIterator::open(PageID pid, lsn_t firstLSN, lsn_t lastLSN, bool prioritizeArchive)
{
    last_lsn = lsn_t::null,
    replayed_count = 0;
    lr_offsets.clear();
    size_t pos = 0;

    if (!lastLSN.is_null()) {
        // make sure log is durable until the lsn we're trying to fetch
        smlevel_0::log->flush(lastLSN);
    }

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
        w_assert1(lsn == nxt);

        if (sizeof(logrec_t) > buffer_capacity - pos) {
            grow_buffer(buffer, buffer_capacity, pos, &lr);
        }

        lr_offsets.push_back(pos);
        pos += lr->length();

        // STEP 2: Obtain LSN of previous log record on the same page (nxt)

        // follow next pointer. This log might touch multi-pages. So, check both cases.
        if (pid == lr->pid())
        {
            // Target pid matches the first page ID in the log recoredd
            nxt = lr->page_prev_lsn();
        }
        else {
            w_assert1(lr->is_multi_page());
            // Multi-page log record, this is a page rebalance log record (split or merge)
            // while the 2nd page is the source page
            // In this case, the page we are trying to recover was the source page during
            // a page rebalance operation, follow the proper log chain
            w_assert1(lr->data_ssx_multi()->_page2_pid == pid);
            nxt = lr->data_ssx_multi()->_page2_prv;
        }

        // If log record contains a page image, the scan can stop since the
        // page is initialized with this log record
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
        archive_scan.open(pid, pid+1, firstLSN);
    }

    lr_iter = lr_offsets.crbegin();
}

bool SprIterator::next(logrec_t*& lr)
{
    if (archive_scan.next(lr)) {
        last_lsn = lr->lsn();
        replayed_count++;
        return true;
    }

    lsn_t curr_lsn = lsn_t::null;
    while (curr_lsn <= last_lsn && lr_iter != lr_offsets.crend()) {
        lr = reinterpret_cast<logrec_t*>(buffer + *lr_iter);
        lr_iter++;
        curr_lsn = lr->lsn();
    }
    if (curr_lsn > last_lsn) {
        replayed_count++;
        last_lsn = curr_lsn;
        return true;
    }

    return false;
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

lsn_t restart_thread_t::get_dirty_page_emlsn(PageID pid) const
{
    spinlock_read_critical_section cs(&chkpt_mutex);

    buf_tab_t::const_iterator it = chkpt.buf_tab.find(pid);
    if (it == chkpt.buf_tab.end()) { return lsn_t::null; }
    return it->second.page_lsn;
}

void restart_thread_t::add_dirty_page(PageID pid, lsn_t lsn)
{
    spinlock_write_critical_section cs(&chkpt_mutex);

    chkpt.mark_page_dirty(pid, lsn, lsn);
}

void restart_thread_t::notify_archived_lsn(lsn_t lsn)
{
    spinlock_write_critical_section cs(&chkpt_mutex);

    /*
     * When a new partition is added to the log archive in nodb mode, we don't
     * need to keep track of page LSNs below the end LSN of the new partition,
     * because log records below this LSN will be found automatically without
     * the per-page log chain traversal done in the SprIterator object.
     */

    if (no_db_mode) {
        chkpt.set_redo_low_water_mark(lsn);
    }
}

void restart_thread_t::notify_cleaned_lsn(lsn_t lsn)
{
    spinlock_write_critical_section cs(&chkpt_mutex);
    // Same as above, but for write elision
    if (write_elision) {
        chkpt.set_redo_low_water_mark(lsn);
        clean_lsn = lsn;
    }
}

PageID restart_thread_t::get_dirty_page_count() const
{
    spinlock_read_critical_section cs(&chkpt_mutex);
    return chkpt.buf_tab.size();
}

void restart_thread_t::checkpoint_dirty_pages(chkpt_t& ex_chkpt) const
{
    spinlock_read_critical_section cs(&chkpt_mutex);
    for (auto e : chkpt.buf_tab) {
        if (e.second.page_lsn > clean_lsn) {
            ex_chkpt.mark_page_dirty(e.first, e.second.page_lsn, e.second.rec_lsn);
        }
    }
}

void restart_thread_t::clear_chkpt()
{
    spinlock_write_critical_section cs(&chkpt_mutex);
    chkpt.init();
}

