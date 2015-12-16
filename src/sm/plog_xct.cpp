#include "w_defines.h"

#define SM_SOURCE
#define PLOG_XCT_C

#include "plog_xct.h"
#include "allocator.h"
#include "log_carray.h"
#include "log_core.h"

#include "AtomicCounter.hpp" // for CAS

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
    w_assert1(!loser_xct); // UNDO recovery never happens with plog transactions

    // original _log_buf should not be used (user transactions only for now)
    delete _log_buf;
    _log_buf = NULL;
}

plog_xct_t::~plog_xct_t()
{
}

rc_t plog_xct_t::get_logbuf(logrec_t*& lr, int /* type */)
{
    if (is_piggy_backed_single_log_sys_xct()) {
        lr = _log_buf_for_piggybacked_ssx;
    }
    else {
        lr = (logrec_t*) plog.get();
    }

    return RCOK;
}

rc_t plog_xct_t::give_logbuf(logrec_t* lr, const fixable_page_h* p,
                    const fixable_page_h* p2)
{
    FUNC(plog_xct_t::give_logbuf);

    if (p != NULL) {
        lr->set_page_prev_lsn(lsn_t::null);
        if (p2 != NULL) {
            // For multi-page log, also set LSN chain with a branch.
            w_assert1(lr->is_multi_page());
            w_assert1(lr->is_single_sys_xct());
            multi_page_log_t *multi = lr->data_ssx_multi();
            w_assert1(multi->_page2_pid != 0);
            multi->_page2_prv = lsn_t::null;
        }
    }

    //if (p != NULL) {
        //lr->set_page_prev_lsn(p->lsn());
        //if (p2 != NULL) {
            //// For multi-page log, also set LSN chain with a branch.
            //w_assert1(lr->is_multi_page());
            //w_assert1(lr->is_single_sys_xct());
            //multi_page_log_t *multi = lr->data_ssx_multi();
            //w_assert1(multi->_page2_pid != 0);
            //multi->_page2_prv = p2->lsn();
        //}
    //}

    lsn_t lsn = lsn_t::null;
    if (is_piggy_backed_single_log_sys_xct()) {
        // For single-log system transactions, we need to manually insert each
        // log record in the clog.
        w_assert1(lr->is_single_sys_xct());
        w_assert1(lr == _log_buf_for_piggybacked_ssx);
        smlevel_0::clog->insert(*lr, &lsn);
        w_assert1(lr->lsn_ck().hi() > 0);
    }
    else {
        if (!lr->is_single_sys_xct()) { // single-log sys xct doesn't have xid/xid_prev
            lr->fill_xct_attr(tid(), lsn_t::null);
        }
        plog.give(lr);
    }

    // setting the (old) page lsn field is not strictly necessary,
    // because the clsn field is used instead. However, this method
    // is still responsible for marking the page dirty, so we leave
    // it here for now. In fact, the value of 'lsn' here is only
    // valid for SSX's. It is up to the page cleaner to set
    // PageLSN = CLSN prior to flushing. Only then will recovery
    // work properly.
    _update_page_lsns(p);
    _update_page_lsns(p2);

    return RCOK;
}

// Increment PageLSN by one. In the atomic commit protocol, the PageLSN
// is used simply to detect whether the page changed during a B-tree
// traversal (see bt_cursor_t::_check_page_update).
// UPDATE: Instead of incrementing, just set it to curr_lsn. This is done
// so that the partition containing the page LSN is guaranteed to exist,
// thus allowing the sanity check on log_storage to pass.
// Recall that recovery in the atomic commit protocol uses the clsn field,
// so the value stored in the old page lsn field is irrelevant for restart.
void plog_xct_t::_update_page_lsns(const fixable_page_h *page)
{
    if (page != NULL) {
        lsn_t new_lsn = log->curr_lsn();
        page->update_initial_and_last_lsn(new_lsn.advance(1));
        //lsn_t old_lsn = page->lsn();
        //if (old_lsn == lsn_t::null) {
            //old_lsn = lsn_t(1,0);
        //}
        //if (page->latch_mode() == LATCH_EX) {
            //page->update_initial_and_last_lsn(new_lsn);
        //} else {
            //DBGOUT3(<<"Update LSN without EX latch -- using atomic increment");
            //lsndata_t *addr = reinterpret_cast<lsndata_t*>(&page->get_generic_page()->lsn);
            //lintel::unsafe::atomic_fetch_add(addr, 1);
            //w_assert1(page->lsn() > old_lsn);
        //}
        page->set_dirty();
    }
}

rc_t plog_xct_t::_abort()
{
    W_DO(_pre_abort());

    w_assert0(!_rolling_back);
    _rolling_back = true;

    // rollback simply by iterating plog backwards calling undo()
    plog_t::iter_t* iter = plog.iterate_forwards(); // acquires plog latch
    logrec_t* lr = NULL;

    while(iter->next(lr))
    {
        w_assert1(!lr->is_cpsn()); // CLRs don't exist with plog
        if (lr->is_undo()) {
            w_assert1(!lr->is_single_sys_xct());
            w_assert1(!lr->is_multi_page()); // All multi-page logs are SSX, so no UNDO.

            PageID pid = lr->pid();
            fixable_page_h page;

            if (!lr->is_logical())
            {
                // Operations such as foster adoption, load balance, etc.
                DBGOUT3 (<<"physical UNDO.. which is not quite good");
                // tentatively use fix_direct for this
                // eventually all physical UNDOs should go away
                // CS TODO fix_direct not supported anymore
                w_assert0(false);
                // W_DO(page.fix_direct(pid, LATCH_EX));
                w_assert1(page.pid() == pid);
            }


            lr->undo(page.is_fixed() ? &page : 0);
        }
    }

    // release locks
    // ELR concept does not apply because no flush is required
    // There is also no need to generate abort log records
    W_COERCE( commit_free_locks());

    _rolling_back = false;
    change_state(xct_ended);
    plog.set_state(plog_t::ABORTED);
    delete iter;

    // CS: don't know why this isn't done in change_state (see xct_t::_abort)
    _core->_xct_aborting = false;

    me()->detach_xct(this);        // no transaction for this thread
    INC_TSTAT(abort_xct_cnt);

    return RCOK;
}

// TODO: is there any part of the code that relies on plastlsn being set?
rc_t plog_xct_t::_commit_nochains(uint32_t flags, lsn_t* /* plastlsn */)
{
    if (flags & xct_t::t_chain)  {
        W_FATAL_MSG(fcINTERNAL, <<
                "plog_xct_t does not support chained transactions");
    }

    W_DO(_pre_commit(flags));

    // SSX don't go through the standard commit
    w_assert1(!is_piggy_backed_single_log_sys_xct());

    lsn_t watermark;
    if (plog.used_size() == 0)  { // read-only xct
        // _last_lsn cannot be used because we never set it
        W_DO(_commit_read_only(flags, watermark));
    }
    else {
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

        change_state(xct_ended);
        //if (plastlsn != NULL) *plastlsn = _last_lsn;

        // Free all locks. Do not free locks if chaining.
        if(individual && ! (flags & xct_t::t_chain) && _elr_mode != elr_sx)  {
            W_DO(commit_free_locks());
        }

        plog.set_state(plog_t::COMMITTED);

        delete iter; // releases latch on plog
    }

    INC_TSTAT(commit_xct_cnt);

    me()->detach_xct(this);        // no transaction for this thread

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
        PageID pid = lr->pid();
        uint16_t* uncommitted_cnt;
        generic_page* page;
        latch_t* latch;

        while (true) {
            bf_idx idx = smlevel_0::bf->lookup(pid);
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
                // CS TODO fix_direct not supported anymore
                w_assert0(false);
                // W_DO(fetched_page.fix_direct(pid, LATCH_SH, false, false));
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
            if (cb._pid_shpid == pid) {
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
