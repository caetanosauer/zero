/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#include "sm_base.h"
#include "logdef_gen.cpp"

#include "log.h"
#include "restart.h"
#include "log_spr.h"
#include "logrec.h"

page_evict_log::page_evict_log (const btree_page_h& p,
                                general_recordid_t child_slot, lsn_t child_lsn) {
    new (data_ssx()) page_evict_t(child_lsn, child_slot);
    fill(p, sizeof(page_evict_t));
}

void page_evict_log::redo(fixable_page_h* page) {
    borrowed_btree_page_h bp(page);
    page_evict_t *dp = (page_evict_t*) data_ssx();
    bp.set_emlsn_general(dp->_child_slot, dp->_child_lsn);
}

// CS TODO: why isnt this in restart.cpp??
void restart_m::dump_page_lsn_chain(std::ostream &o, const PageID &pid, const lsn_t &max_lsn) {
    lsn_t master = log->master_lsn();
    o << "Dumping Page LSN Chain for PID=" << pid << ", MAXLSN=" << max_lsn
        << ", MasterLSN=" << master << "..." << std::endl;

    log_i           scan(*log, master);
    logrec_t        buf;
    lsn_t           lsn;
    // Scan all log entries until EMLSN
    while (scan.xct_next(lsn, buf) && buf.lsn_ck() <= max_lsn) {
        if (buf.type() == logrec_t::t_chkpt_begin) {
            o << "  CHECKPT: " << buf << std::endl;
            continue;
        }
        if (buf.null_pid()) {
            continue;
        }

        PageID log_pid = buf->pid();
        PageID log_pid2 = log_pid;
        if (buf->is_multi_page()) {
            log_pid2 = buf->data_ssx_multi()->_page2_pid;
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

    // after the dumping, recovers the original master_lsn because log_i increased it.
    // of course, this is not safe if there are other transactions going.
    // but, this method is for debugging.
    // CS: commented out -- tests seem fine without this
    //_storage->reset_master_lsn(master);
}

rc_t restart_m::recover_single_page(fixable_page_h &p, const lsn_t& emlsn,
                                    const bool from_lsn)  // True if use page lsn as the start point
                                                          // mainly from Restart using Single Pge Recovery
                                                          // in REDO phase where the page is not corrupted
{
    // Single-Page-Recovery operation does not hold latch on the page to be recovered, because
    // it assumes the page is private until recovered.  It is not the case during
    // recovery.  It is caller's responsibility to hold latch before accessing Single-Page-Recovery

    // First, retrieve the backup page we will be based on.
    // If this backup is enough recent, we have to apply only a few logs.
    w_assert1(p.is_fixed());
    PageID pid = p.pid();
    DBGOUT1(<< "Single-Page-Recovery on " << pid << ", EMLSN=" << emlsn);

    // CS TODO: because of cleaner bug, we fetch page from disk itself.  In
    // other words, if we are performing write elision, then we must read from
    // disk instead of backup. We need to distinguish between cases of failure
    // and normal outdated pages. Ideally, the volume manager should handle
    // that transparently, so that a volume read is redirected to a backup if
    // necessary (similar to how restore works currently).

    if (true)
    // if (smlevel_0::bk->page_exists(p.vol(), pid))
    {
        // W_DO(smlevel_0::bk->retrieve_page(*p.get_generic_page(), p.vol(), pid));
        W_DO(smlevel_0::vol->read_page(pid, *p.get_generic_page()));
        w_assert1(pid == p.pid());
        DBGOUT1(<< "Backup page retrieved. Backup-LSN=" << p.lsn());
        if (p.lsn() > emlsn)
        {
            // Last write LSN from backup > given LSN, the page last write LSN
            // from the backup is newer (later) than our recorded emlsn (last write)
            // on the page.
            // First, there are cases of single-page failure in which the backup media
            // are not consulted: the prototypical example here is restart bringing
            // a page up-to-date from an out-of-date page in the database.
            // Second, there might be weird cases (all requiring double failures)
            // in which a database page might be written to persistent storage
            // (e.g., a child page in a self-repairing b-tree index) but the
            // expected LSN value is not up-to-date.

            // Raise error for now, we cannot handle double failures and other special
            // cases currently
////////////////////////////////////////
// TODO(Restart)... NYI
////////////////////////////////////////

            DBGOUT1(<< "Backup page last write LSN > emlsn");
            W_FATAL(eBAD_BACKUPPAGE);
        }
    }
    else
    {
        DBGOUT1(<< "No backup page. Recovering from log only");

        // if the page is not in the backup (possible if the page was created after the
        // backup) or no backup file at all, we need to recover the page purely from the log.
        // If caller specify 'from_lsn', then use the last write LSN on the page as the starting point
        // of the recovery, otherwise set page lsn to NULL to force a complete recovery

        if (false == from_lsn)
        {
            // Complete recovery
            DBGOUT1(<< "Force a complete recovery");
            p.set_lsns(lsn_t::null);
        }
        else
        {
            if (lsn_t::null == p.lsn())
            {
                // Page does not have last write LSN
                DBGOUT1(<< "Recovery from page last write LSN but it is NULL, a complete recovery");
            }
            else
            {
                // Page has last write LSN, use it as the starting point so it is not a complete recovery
                DBGOUT1(<< "Recovery from page last write LSN: " << p.lsn());
            }
        }
    }

    char* buffer = NULL;
    size_t bufsize = 0;
    W_DO(_collect_spr_logs(pid, p.lsn(), emlsn, buffer, bufsize));
    w_assert0(buffer);

    W_DO(_apply_spr_logs(p, buffer, bufsize));
    delete[] buffer;

    w_assert0(p.lsn() == emlsn);
    DBGOUT1(<< "Single-Page-Recovery done for page " << p.pid());
    return RCOK;
}

rc_t restart_m::_collect_spr_logs(
    const PageID& pid,         // In: page ID of the page to work on
    const lsn_t& current_lsn,  // In: known last write to the page, where recovery starts
    const lsn_t& emlsn,        // In: starting point of the log chain
    char*& buffer, size_t& buffer_size)
{
    // When caller from recovery REDO phase on a virgin or corrupted page, we do not have
    // a valid emlsn and page last-write lsn (current_lsn) has been set to lsn_t::null.
    // The pre-crash last log lsn was used instead for emlsn, therefore passed in emlsn
    // is not a valid starting point for log chain, need to find the starting point for the valid
    // page log chain

    // we go back using page log chain like what xct_t::rollback() does on undo log chain.

    DBGOUT1(<< "restart_m::_collect_single_page_recovery_logs: "
            << "current_lsn (end): " << current_lsn
            << ", emlsn (begin): " << emlsn );

    if (emlsn == lsn_t::null)
    {
        // Failure on failure scenario The emlsn is not the actual last write
        // on the page, it is a corrupted page during recovery, we do not have
        // a parent page to retrieve the actual last write lsn, and we cannot
        // trust the last write LSN due to page corruption, using the last LSN
        // before system crash as the emlsn, therefore we need to find the
        // actual emlsn first

        // TODO(Restart)... NYI.  How to find the valid emlsn?  Need backward
        // log scan and slow
        W_FATAL_MSG(fcINTERNAL,
                << "restart_m::_collect_single_page_recovery_logs "
                << "- failure on failure, NYI");
    }

    // Allocate initial buffer -- expand later if needed
    // CS: regular allocation is fine since SPR isn't such a critical operation
    size_t buffer_capacity = 1 << 16; // start with 64KB
    // must be freed by caller
    buffer = new char[buffer_capacity];
    size_t pos = buffer_capacity;

    lsn_t nxt = emlsn;
    while (current_lsn < nxt && nxt != lsn_t::null) {

        // STEP 1: Fecth log record and copy it into buffer
        logrec_t* lr = NULL;
        lsn_t lsn = nxt;
        rc_t rc = log->fetch(lsn, lr, NULL, true);

        if ((rc.is_error()) && (eEOF == rc.err_num())) {
            // EOF -- scan finished
            break;
        }
        else {
            W_DO(rc);
        }
        w_assert0(lsn == nxt);
        DBGOUT1(<< "restart_m::_collect_single_page_recovery_logs, log = " << *lr);

        if (lr->length() > pos) {
            // double capacity of buffer
            DBGOUT1(<< "Doubling SPR buffer capacity");
            buffer_capacity *= 2;
            char* tmp = new char[buffer_capacity];
            memcpy(tmp + buffer_capacity/2, buffer, buffer_capacity/2);
            delete[] buffer;
            buffer = tmp;
            pos += buffer_capacity/2;
            w_assert0(lr->length() <= pos);
        }

        pos -= lr->length();
        memcpy(buffer + pos, lr, lr->length());
        log->release();
        lr = (logrec_t*) (buffer + pos);

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
        // CS: I think the condition below should be == and not != (like split)
        if (lr->type() == logrec_t::t_btree_norec_alloc && pid != lr->pid()) {
            break;
        }
        if (lr->type() == logrec_t::t_btree_split && pid == lr->pid()) {
            break;
        }
        if (lr->type() == logrec_t::t_page_img_format) {
            break;
        }
    }

    // shift log records into beginning of buffer
    buffer_size = buffer_capacity - pos;
    memmove(buffer, buffer + pos, buffer_size);
    return RCOK;
}

rc_t restart_m::_apply_spr_logs(fixable_page_h &p, char* buffer, size_t bufsize)
{
    lsn_t prev_lsn = lsn_t::null;
    size_t pos = 0;
    while (pos < bufsize) {
        logrec_t* lr = (logrec_t*) (buffer + pos);

        w_assert1(lr->valid_header(lsn_t::null));
        w_assert1(pos == 0 || lr->is_multi_page() ||
               (prev_lsn == lr->page_prev_lsn() && p.pid() == lr->pid()));

        if (lr->is_redo() && p.lsn() < lr->lsn_ck()) {
            DBGOUT1(<< "Applying Single-Page-Recovery. current page(" << p.pid()
                    << ") LSN=" << p.lsn() << ", log=" << *lr);
            w_assert1(lr->is_redo());
            lr->redo(&p);
            p.set_lsns(lr->lsn_ck());
            p.update_initial_and_last_lsn(lr->lsn_ck());
            p.update_clsn(lr->lsn_ck());
        }

        pos += lr->length();
        prev_lsn = lr->lsn_ck();
    }

    return RCOK;
}
