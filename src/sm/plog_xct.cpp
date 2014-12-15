#include "w_defines.h"

#define SM_SOURCE
#define PLOG_XCT_C

#include "plog_xct.h"
#include "allocator.h"
#include "log_carray.h"
#include "log_core.h"

#include "Lintel/AtomicCounter.hpp" // for CAS

const std::string plog_xct_t::IMPL_NAME = "plog";

DEFINE_SM_ALLOC(plog_xct_t);

plog_xct_t::plog_xct_t(
    sm_stats_info_t*             stats,  // allocated by caller
    timeout_in_ms                timeout,
    bool                         sys_xct,
    bool                         single_log_sys_xct,
    const lsn_t&                 last_lsn,
    const lsn_t&                 undo_nxt,
    bool                         loser_xct
)
    : xct_t(stats, timeout, sys_xct, single_log_sys_xct, tid_t::null,
            last_lsn, undo_nxt, loser_xct)
{
    w_assert1(smlevel_0::clog);
    w_assert1(!loser_xct); // UNDO recovery not supported for plog transactions

    // original _log_buf should not be used (user transactions only for now)
    if (!is_single_log_sys_xct()) {
        delete _log_buf;
        _log_buf = NULL;
    }
}

plog_xct_t::~plog_xct_t()
{
}

rc_t plog_xct_t::get_logbuf(logrec_t*& lr, int nbytes)
{
    // At the current development milestone, system transactions
    // do not use private logs. Instead, they log directly to the
    // centralized log.
    if (!is_piggy_backed_single_log_sys_xct()) {
        char* data = plog.get();

        // In the current milestone (M1), log records are replicated into
        // both the private log and the traditional ARIES log. To achieve that,
        // we simply use the logrec pointer in the current extent as the xct
        // logbuf in the traditional implementation.
        _log_buf = (logrec_t*) data;
    }

    // The replication also means we need to invoke log reservations,
    // which is done in the traditional get_logbuf
    xct_t::get_logbuf(lr, nbytes);

    return RCOK;
}

rc_t plog_xct_t::give_logbuf(logrec_t* lr, const fixable_page_h* p,
                    const fixable_page_h* p2)
{
    // replicate logic on traditional log, i.e., call log->insert and set LSN
    xct_t::give_logbuf(lr, p, p2);
    
    // set page LSN chain once again, since it's different in clog
    if (p != NULL) {
        lr->set_page_prev_lsn(p->clsn());
        if (p2 != NULL) {
            // For multi-page log, also set LSN chain with a branch.
            w_assert1(lr->is_multi_page());
            w_assert1(lr->is_single_sys_xct());
            multi_page_log_t *multi = lr->data_ssx_multi();
            w_assert1(multi->_page2_pid != 0);
            multi->_page2_prv = p2->clsn();
        }
    }

    if (!is_piggy_backed_single_log_sys_xct()) {
        plog.give(lr);
        // avoid xct_t destructor trying to deallocate plog's memory
        _log_buf = NULL;
    }
    else {
        // For system transactions, we need to manually insert each log
        // record in the clog, since otherwise they would go only to the
        // old log_core.
        w_assert1(lr->lsn_ck().hi() > 0);
        smlevel_0::clog->insert(*lr, NULL);
    }

    return RCOK;
}

rc_t plog_xct_t::_abort()
{
    xct_t::_abort();
    plog.set_state(plog_t::ABORTED);
    return RCOK;
}

rc_t plog_xct_t::_commit_nochains(uint32_t flags, lsn_t* plastlsn)
{
    if (!is_piggy_backed_single_log_sys_xct()) {
        // add commit log record (if required)
        bool individual = ! (flags & xct_t::t_group);
        if(individual && !is_single_log_sys_xct()) {
            W_COERCE(log_xct_end());
        }

        plog_t::iter_t* iter = plog.iterate_forwards(); // acquires plog latch
        logrec_t* lr = NULL;

        // join consolidation array to reserve an offset in the log buffer
        CArraySlot* info = NULL;
        long pos = 0;
        size_t size = plog.used_size();
        W_DO(smlevel_0::clog->_join_carray(info, pos, size));
        w_assert1(info);

        // get base LSN where my log records will end up
        lsn_t base_lsn = info->lsn + pos;

        // set final LSN value of each logrec
        while(iter->next(lr)) {
            lr->set_lsn_ck(base_lsn + lr->lsn_ck().data());
            w_assert1(lr->lsn_ck().hi() > 0);
        }
        iter->reset(); // go back to beginning

        // copy whole plog to the assigned position and release
        // TODO: flush directly from plog as buffer
        smlevel_0::clog->_copy_raw(info, pos, plog.get_data(), size);
        W_DO(smlevel_0::clog->_leave_carray(info, size));
        ADD_TSTAT(log_bytes_generated,size);

        // flush log -- transaction now officially committed
        W_DO(smlevel_0::clog->flush(base_lsn + size));

        while(iter->next(lr)) {
            _update_page_cas(lr);
        }

        plog.set_state(plog_t::COMMITTED);

        delete iter; // releases latch on plog
    }

    // lock release and other sutff handled by normal xct commit
    xct_t::_commit(flags, plastlsn);
    return RCOK;
}

rc_t plog_xct_t::_update_page_cas(logrec_t* lr)
{
    // decrement uncommitted counter on page and set PageLSN
    if (!lr->null_pid()) {
        // Later on, we need access to the control block, which the
        // standard fix procedure does not provide. So for now we have 
        // to use this hack, which is basically a "manual" fix.
        // Of course it would be much better if we could use a proper
        // fix method from fixable_page_h (TODO)
        lpid_t pid = lr->construct_pid();
        uint64_t key = bf_key(pid.vol().vol, pid.page);
        uint16_t* uncommitted_cnt;
        generic_page* page;
        latch_t* latch;

        while (true) {
            bf_idx idx = smlevel_0::bf->lookup_in_doubt(key);
            if (idx == 0) {
                // page needs to be fetched
                // Instead of repeating the logic of the fix method once again,
                // we actually invoke a fix just so the page is fetched --
                // only to unfix it and fix it manually once again below.
                // This is again a dirty hack, but at this point this penalty
                // is acceptable because this event (a page being evicted with
                // uncommitted updates) is avoided by the page cleaner, and 
                // even if we do that later, it should be quite rare.
                fixable_page_h fetched_page;
                W_DO(fetched_page.fix_direct(pid.vol().vol, pid.page, LATCH_SH,
                            false, false));
                fetched_page.unfix();
                continue;
            }
            bf_tree_cb_t& cb = smlevel_0::bf->get_cb(idx);
            uncommitted_cnt = &cb._uncommitted_cnt;
            page = smlevel_0::bf->get_page(&cb);

            // Latch the page to prevent it being evicted.
            // Shared mode is fine because only this method increases
            // the PageLSN value, which is done with compare-and-swap
            latch = cb.latchp();
            W_DO(latch->latch_acquire(LATCH_SH));

            // After latching, we have to re-check if the page was not replaced
            // while we performed the lookup and waited for the latch.
            if (cb._pid_shpid == pid.page) {
                break;
            }
        }

        // Keep trying to update PageLSN using compare-and-swap
        // (we must use the native type lsndata_t, aka unsigned long).
        //
        // This process races with the page cleaner, which also latches the
        // page in shared mode while taking a copy to flush. Thus, the copy
        // taken may be either with an old or a new PageLSN. In either case,
        // the data on the page itself is the same (it was updated by the
        // transaction while holding an EX latch). Thus, copying a page
        // before the PageLSN is updated simply causes an unnecessary
        // single-page rollback. This is not harmful, because it just means
        // that the page on disk will have a slightly outdated version.
        lsndata_t new_value = lr->lsn_ck().data();
        while (true) {
            // Currently we use the "clsn" field for the atomic commit
            // protocol. The old "lsn" field is still used by the standard
            // commit protocol. The former flushes log records to clog,
            // while the latter to the standard log.
            lsndata_t old_value = page->clsn.data();
            // Some other transaction may have already incremented the LSN
            // to a larger value, which means we have nothing to do, as the
            // page is already in a newer state.
            if (new_value > old_value)
            {
                if (!lintel::unsafe::atomic_compare_exchange_strong(
                            reinterpret_cast<lsndata_t*>(&page->clsn),
                            &old_value,
                            new_value))
                {
                    continue; // CAS did not succeed -> try again
                }
                DBGOUT3(<< "Updated CLSN of " << pid
                        << " to " << (lsn_t) new_value);
            }
            else {
                DBGOUT3(<< "CLSN of " << pid << " not updated! "
                        << " tried " << (lsn_t) new_value
                        << " was " << (lsn_t) old_value);
            }
            // if we get here, update was either successful or unnecessary
            break;
        }


        // Decrement counter of uncommitted updates. Must also
        // be done atomically since page is latched in shared mode
        lintel::unsafe::atomic_fetch_sub(uncommitted_cnt, 1);

        latch->latch_release();
    }


    return RCOK;
}

rc_t plog_xct_t::_commit_xlatch(uint32_t flags, lsn_t* plastlsn)
{
    // currently not working -- additional synchronization
    // needed so that page CLSN is updated in LSN order
    return RC(eNOTIMPLEMENTED);
}

rc_t plog_xct_t::_update_page_xlatch(logrec_t* lr)
{
    return RC(eNOTIMPLEMENTED);
}
