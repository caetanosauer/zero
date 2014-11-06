/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#include "sm_int_2.h"
#include "logdef_gen.cpp"

#include "log.h"
#include "restart.h"
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

void restart_m::dump_page_lsn_chain(std::ostream &o, const lpid_t &pid, const lsn_t &max_lsn) {
    lsn_t master = log->master_lsn();
    o << "Dumping Page LSN Chain for PID=" << pid << ", MAXLSN=" << max_lsn
        << ", MasterLSN=" << master << "..." << std::endl;

    log_i           scan(*log, master);
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
    // CS: commented out -- tests seem fine without this
    //_storage->reset_master_lsn(master);
}

rc_t restart_m::recover_single_page(fixable_page_h &p, const lsn_t& emlsn,
                                    const bool actual_emlsn) {
    // Single-Page-Recovery operation does not hold latch on the page to be recovered, because
    // it assumes the page is private until recovered.  It is not the case during
    // recovery.  It is caller's responsibility to hold latch before accessing Single-Page-Recovery

    // First, retrieve the backup page we will be based on.
    // If this backup is enough recent, we have to apply only a few logs.
    w_assert1(p.is_fixed());
    lpid_t pid = p.pid();
    DBGOUT1(<< "Single-Page-Recovery on " << pid << ", EMLSN=" << emlsn);
    if (smlevel_0::bk->page_exists(p.vol(), pid.page)) {
        W_DO(smlevel_0::bk->retrieve_page(*p.get_generic_page(), p.vol(), pid.page));
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
    } else {
        // if the page is not in the backup (possible if the page was created after the
        // backup), we need to recover the page purely from the log. So, current_lsn=0.
        p.set_lsns(lsn_t::null);
        DBGOUT1(<< "No backup page. Recovering from log only");
    }

    // Then, collect logs to apply. Depending on the recency of the backup and the
    // previous page-allocation operation on the page, we might have to collect many logs.
    size_t SPR_LOG_BUFSIZE = 1 << 18; // 1 << 14;    // 1<<18: 256 KB
                                                 // while the max. log record size is 3 pages = 24KB
                                                 // A typical update log record should be much smaller
                                                 // than 1 page (8K)
    if (true == smlevel_0::use_redo_full_logging_restart())
        SPR_LOG_BUFSIZE = 1 << 19;   // 1<<19: 512 KB, page rebalance full logging generates more log records
                                     // use a bigger buffer size in such case although the current
                                     // implementation (hard-coded fixed buffer size) does not prevent OOM

    std::vector<char> buffer(SPR_LOG_BUFSIZE); // TODO, we should have an object pool for this.
    std::vector<logrec_t*> ordered_entires;
    W_DO(_collect_single_page_recovery_logs(pid, p.lsn(), emlsn,
        &buffer[0], SPR_LOG_BUFSIZE, ordered_entires, actual_emlsn));
    DBGOUT1(<< "Collected log. About to apply " << ordered_entires.size() << " logs");
    W_DO(_apply_single_page_recovery_logs(p, ordered_entires));

    // after Single-Page-Recovery, the page should be exactly the requested LSN, perform the validation only if
    // caller is using an actual emlsn
    // An estimated emlsn would be used for a corrupted page during recovery
    if (true == actual_emlsn)
        w_assert0(p.lsn() == emlsn);
    DBGOUT1(<< "Single-Page-Recovery done!");
    return RCOK;
}

rc_t restart_m::_collect_single_page_recovery_logs(
    const lpid_t& pid,
    const lsn_t& current_lsn,
    const lsn_t& emlsn,                        // In: starting point of the log chain
    char* log_copy_buffer, size_t buffer_size,
    std::vector<logrec_t*>& ordered_entries,
    const bool valid_start_emlan)              // In: true if emlsn is the last write of the page,
                                               //     false if an assumed starting point
{
    // When caller from recovery REDO phase on a virgin or corrupted page, we do not have
    // a valid emlsn and page last-write lsn has been set to lsn_t::null.
    // The pre-crash last log lsn was used instead for emlsn, therefore passed in emlsn
    // is not a valid starting point for log chain, need to find the starting point for the valid
    // page log chain

    // we go back using page log chain like what xct_t::rollback() does on undo log chain.
    ordered_entries.clear();
    size_t buffer_capacity = buffer_size;

    DBGOUT1(<< "restart_m::_collect_single_page_recovery_logs: current_lsn (end): " << current_lsn
            << ", emlsn (begin): " << emlsn );

    if (false == valid_start_emlan)
    {
        // Failure on failure scenario
        // The emlsn is not the actual last write on the page, it is a corrupted page
        // during recovery, we do not have a parent page to retrieve the actual last write lsn,
        // and we cannot trust the last write LSN due to page corruption, using the last LSN
        // before system crash as the emlsn, therefore we need to find the actual emlsn first

////////////////////////////////////////
// TODO(Restart)... NYI.  How to find the valid emlsn?  Need backward log scan and slow
////////////////////////////////////////

        DBGOUT1(<< "restart_m::_collect_single_page_recovery_logs: search for the actual emlsn");
        W_FATAL_MSG(fcINTERNAL, << "restart_m::_collect_single_page_recovery_logs - failure on failure, NYI");
    }

    for (lsn_t nxt = emlsn; current_lsn < nxt && nxt != lsn_t::null;) {
        logrec_t* record = NULL;
        lsn_t obtained = nxt;

        rc_t rc = log->fetch(obtained, record, NULL, true);
        log->release(); // release _partition_lock immediately

        if ((rc.is_error()) && (eEOF == rc.err_num()))
        {
            // End of Log, cannot go further
            break;
        }
        if (obtained != nxt) {
            ERROUT(<<"restart_m::fetch() returned a different LSN, old log partition already"
                " wiped?? nxt=" << nxt << ", obtained=" << obtained);
        }

        DBGOUT1(<< "restart_m::_collect_single_page_recovery_logs, log = " << *record);

        if (buffer_capacity < record->length()) {
////////////////////////////////////////
// TODO(Restart)... Single Page Recovery should handle this scenario better
//                          currently buffer size is hard-coded fixed number
////////////////////////////////////////

            // This might happen when we have a really long page log chain,
            // but so far we don't handle this case. crash.

            // Provide information for OOM error
            ERROUT(<< "_collect_single_page_recovery_logs: out-of-memory.  Remaining buffer_capacity: "
                   << buffer_capacity << ", current record length: " << record->length()
                   << ", initial buffer size: " << buffer_size << ", the number of records collected so far: "
                   << ordered_entries.size());
            // FUll logging generates more log records and more likely to cause OOM
            if (true == smlevel_0::use_redo_full_logging_restart())
            {
                ERROUT(<< "Full logging for page rebalance was ON");
            }
            else
            {
                ERROUT(<< "Full logging for page rebalance was OFF");
            }

            W_FATAL(eOUTOFMEMORY);
        }

        // follow next pointer. This log might touch multi-pages. So, check both cases.
        if (pid.page == record->shpid()) {
            nxt = record->page_prev_lsn();
        } else if (!record->is_multi_page()
            || pid.page != record->data_ssx_multi()->_page2_pid) {

            if (!record->is_multi_page())
            {
                // Not multi-page log record, and the page id is different
                // but if the log record is one of the following, not an error
                // simply break from log gathering
                if (record->type() == logrec_t::t_btree_norec_alloc && pid.page != record->shpid())
                    break; // child page allocated. this is the initial log, so no need to go further

                if (record->type() == logrec_t::t_page_img_format)
                    break; // root page allocated. initial log
            }

            W_RETURN_RC_MSG(eWRONG_PAGE_LSNCHAIN, << "PID= " << pid << ", CUR_LSN="
                << current_lsn << ", EMLSN=" << emlsn << ", next_lsn=" << nxt
                << ", obtained_lsn=" << obtained << ", log=" << *record);

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

        if (record->type() == logrec_t::t_btree_norec_alloc && pid.page != record->shpid()) {
            break; // child page allocated. this is the initial log, so no need to go further
        }
        if (record->type() == logrec_t::t_page_img_format) {
            break; // root page allocated. initial log
        }
    }
#ifdef LOG_BUFFER
//hints
//delete tmp;
#endif

    return RCOK;
}

rc_t restart_m::_apply_single_page_recovery_logs(fixable_page_h &p,
    const std::vector<logrec_t*>& ordered_entries) {
    // So far, we assume the Single-Page-Recovery target is a fixable page with latches.
    // So far no plan to extend it to non-fixable pages.
    for (std::vector<logrec_t*>::const_iterator it = ordered_entries.begin();
         it != ordered_entries.end(); ++it) {
        logrec_t *record = *it;
        DBGOUT1(<< "Applying Single-Page-Recovery. current page(" << p.pid() << ") LSN="
            << p.lsn() << ", log=" << *record);
        w_assert1(record->is_redo());
        record->redo(&p);
        p.set_lsns(record->lsn_ck());
    }
    return RCOK;
}
