/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#include "sm_int_2.h"
#include "logdef_gen.cpp"

#include "log.h"
#include "log_core.h"
#include "log_spr.h"
#include "logrec.h"
#include "backup.h"

page_evict_log::page_evict_log (const btree_page_h& p,
                                general_recordid_t child_slot, lsn_t child_lsn) {
    new (_data) page_evict_t(child_lsn, child_slot);
    fill(&p.pid(), p.tag(), sizeof(page_evict_t));
}

void page_evict_log::redo(fixable_page_h* page) {
    borrowed_btree_page_h bp(page);
    page_evict_t *dp = (page_evict_t*) _data;
    bp.set_emlsn_general(dp->_child_slot, dp->_child_lsn);
}

void log_m::dump_page_lsn_chain(std::ostream &o, const lpid_t &pid, const lsn_t &max_lsn) {
    lsn_t master = master_lsn();
    o << "Dumping Page LSN Chain for PID=" << pid << ", MAXLSN=" << max_lsn
        << ", MasterLSN=" << master << "..." << std::endl;

    log_i           scan(*this, master);
    logrec_t*       buf;
    lsn_t           lsn;
    // Scan all log entries until EMLSN
    while (scan.xct_next(lsn, buf) && buf->lsn_ck() <= max_lsn) {
        if (buf->type() == logrec_t::t_chkpt_begin) {
            o << "  CHECKPT: " << *buf << std::endl;
            continue;
        }
        if (buf->null_pid()) {
            continue;
        }

        lpid_t log_pid = buf->construct_pid();
        lpid_t log_pid2 = log_pid;
        if (buf->is_multi_page()) {
            log_pid2.page = buf->data_ssx_multi()->_page2_pid;
        }

        // Is this page interesting to us?
        if (pid != lpid_t::null && pid != log_pid && pid != log_pid2) {
            continue;
        }

        o << "  LOG: " << *buf << ", P_PREV=" << buf->page_prev_lsn();
        if (buf->is_multi_page()) {
            o << ", P2_PREV=" << buf->data_ssx_multi()->_page2_prv << std::endl;
        }
        o << std::endl;
    }

    // after the dumping, recovers the original master_lsn because log_i increased it.
    // of course, this is not safe if there are other transactions going.
    // but, this method is for debugging.
    _master_lsn = master;
}

rc_t log_m::recover_single_page(generic_page* p, const lsn_t& emlsn) {
    // First, retrieve the backup page we will be based on.
    // If this backup is enough recent, we have to apply only a few logs.
    lpid_t pid = p->pid;
    DBGOUT1(<< "SPR on " << pid << ", EMLSN=" << emlsn);
    if (smlevel_0::bk->page_exists(p->pid.vol().vol, pid.page)) {
        W_DO(smlevel_0::bk->retrieve_page(*p, p->pid.vol().vol, pid.page));
        w_assert1(pid == p->pid);
        DBGOUT1(<< "Backup page retrieved. Backup-LSN=" << p->lsn);
    } else {
        // if the page is not in the backup (possible if the page was created after the
        // backup), we need to recover the page purely from the log. So, current_lsn=0.
        p->lsn = lsn_t::null;
        DBGOUT1(<< "No backup page. Recovering only from log");
    }

    // Then, collect logs to apply. Depending on the recency of the backup and the
    // previous page-allocation operation on the page, we might have to collect many logs.
    const size_t SPR_LOG_BUFSIZE = 1 << 14;
    char buffer[SPR_LOG_BUFSIZE]; // TODO, we should have an object pool for this.
    std::vector<logrec_t*> ordered_entires;
    W_DO(log_core::THE_LOG->_collect_single_page_recovery_logs(pid, p->lsn, emlsn, buffer,
        SPR_LOG_BUFSIZE, ordered_entires));
    DBGOUT1(<< "Collected log. About to apply " << ordered_entires.size() << " logs");
    W_DO(log_core::THE_LOG->_apply_single_page_recovery_logs(p, ordered_entires));

    // after SPR, the page should be exactly the requested LSN
    w_assert0(p->lsn == emlsn);
    DBGOUT1(<< "SPR done!");
    return RCOK;
}

rc_t log_core::_collect_single_page_recovery_logs(
    const lpid_t& pid, const lsn_t& current_lsn, const lsn_t& emlsn,
    char* log_copy_buffer, size_t buffer_size, std::vector<logrec_t*>& ordered_entries) {
    // we go back using page log chain like what xct_t::rollback() does on undo log chain.
    ordered_entries.clear();
    size_t buffer_capacity = buffer_size;
    for (lsn_t nxt = emlsn; current_lsn < nxt && nxt != lsn_t::null;) {
        logrec_t* record = NULL;
        W_DO(fetch(nxt, record, NULL));
        release(); // release _partition_lock immediately

        if (buffer_capacity < record->length()) {
            // This might happen when we have a really long page log chain,
            // but so far we don't handle this case. crash.
            W_FATAL(eOUTOFMEMORY);
        }

        // follow next pointer. This log might touch multi-pages. So, check both cases.
        if (pid.page == record->shpid()) {
            nxt = record->page_prev_lsn();
        } else if (!record->is_multi_page()
            || pid.page != record->data_ssx_multi()->_page2_pid) {
            W_RETURN_RC_MSG(eWRONG_PAGE_LSNCHAIN, << "PID= " << pid << ", CUR_LSN="
                << current_lsn << ", EMLSN=" << emlsn << ", log=" << record);
        } else {
            w_assert0(record->data_ssx_multi()->_page2_pid == pid.page);
            nxt = record->data_ssx_multi()->_page2_prv;
        }

        if (record->is_redo()) {
            ::memcpy(log_copy_buffer, record, record->length());
            ordered_entries.insert(ordered_entries.begin(),
                                reinterpret_cast<logrec_t*>(log_copy_buffer));
            log_copy_buffer += record->length();
            buffer_capacity -= record->length();
        }
    }
    return RCOK;
}

rc_t log_core::_apply_single_page_recovery_logs(generic_page* p,
    const std::vector<logrec_t*>& ordered_entries) {
    // So far, we assume the SPR target is a fixable page with latches.
    // So far no plan to extend it to non-fixable pages.
    // Note: we can't use dynamic_cast here because we didn't instantiate it as any class.
    // We only reinterpreted a buffer frame (char*).
    fixable_page_h page_h;
    page_h.fix_nonbufferpool_page(p);
    for (std::vector<logrec_t*>::const_iterator it = ordered_entries.begin();
         it != ordered_entries.end(); ++it) {
        logrec_t *record = *it;
        DBGOUT1(<< "Applying SPR. current page LSN=" << page_h.lsn() << ", log=" << *record);
        w_assert1(record->is_redo());
        record->redo(&page_h);
        page_h.set_lsns(record->lsn_ck());
    }
    return RCOK;
}
