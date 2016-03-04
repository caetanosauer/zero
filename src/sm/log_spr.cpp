/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#include "sm_base.h"
#include "logdef_gen.cpp"

#include "log_core.h"
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

rc_t restart_m::recover_single_page(fixable_page_h &p, const lsn_t& emlsn)
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

    // W_DO(smlevel_0::bk->retrieve_page(*p.get_generic_page(), p.vol(), pid.page));
    w_assert1(pid == p.pid());
    w_assert0(p.lsn() <= emlsn);

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
    w_assert0(!emlsn.is_null());

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
        rc_t rc = smlevel_0::log->fetch(lsn, lr, NULL, true);

        if ((rc.is_error()) && (eEOF == rc.err_num())) {
            // EOF -- scan finished
            break;
        }
        else {
            W_DO(rc);
        }
        w_assert0(lsn == nxt);
        // ERROUT(<< "restart_m::_collect_spr_logs, pid = " << pid << ", log = " << *lr);

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
        smlevel_0::log->release();
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
    PageID pid = p.pid();
    size_t pos = 0;
    while (pos < bufsize) {
        logrec_t* lr = (logrec_t*) (buffer + pos);

        w_assert1(lr->valid_header(lsn_t::null));
        w_assert1(pos == 0 || lr->is_multi_page() ||
               (prev_lsn == lr->page_prev_lsn() && p.pid() == lr->pid()));

        if (lr->is_redo() && p.lsn() < lr->lsn_ck()) {
            DBGOUT1(<< "Applying Single-Page-Recovery. current page(" << p.pid()
                    << ") LSN=" << p.lsn() << ", log=" << *lr);

            w_assert1(pid == lr->pid() || pid == lr->pid2());
            w_assert1(pid != lr->pid() || (lr->page_prev_lsn() == lsn_t::null ||
                        lr->page_prev_lsn() == p.lsn()));

            w_assert1(pid != lr->pid2() || (lr->page2_prev_lsn() == lsn_t::null ||
                        lr->page2_prev_lsn() == p.lsn()));

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
