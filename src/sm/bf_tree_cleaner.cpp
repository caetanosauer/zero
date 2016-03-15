/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "bf_tree_cleaner.h"
#include "sm_base.h"
#include "bf_tree_cb.h"
#include "bf_tree.h"
#include "generic_page.h"
#include "fixable_page_h.h"
#include "log_core.h"
#include "vol.h"
#include "alloc_cache.h"
#include "eventlog.h"
#include "sm.h"
#include "xct.h"

#include <vector>

const int INITIAL_SORT_BUFFER_SIZE = 64;

bf_tree_cleaner::bf_tree_cleaner(bf_tree_m* bufferpool, const sm_options& _options)
    : page_cleaner_base(bufferpool, _options)
{
}

bf_tree_cleaner::~bf_tree_cleaner()
{
}

void bf_tree_cleaner::do_work()
{
    collect_candidates();
    clean_candidates();
}

void bf_tree_cleaner::clean_candidates()
{
    if (candidates.empty()) {
        return;
    }

    _clean_lsn = smlevel_0::log->curr_lsn();

    PageID prev_pid = 0;
    size_t wpos = 0;
    for (auto iter = candidates.begin(); iter != candidates.end(); iter++) {
        // Latch page and copy image if it's adjacent to previous and workspace
        // still has room
        PageID pid = iter->first;
        bf_idx idx = iter->second;
        if (wpos >= _workspace_size || (wpos != 0 && pid != prev_pid + 1)) {
            // It's time to flush a portion of the workspace
            log_and_flush(wpos);
            wpos = 0;
        }

        if (latch_and_copy(pid, idx, wpos)) {
            wpos++;
            prev_pid = pid;
        }
    }

    if (wpos > 0) { log_and_flush(wpos); }
}

void bf_tree_cleaner::log_and_flush(size_t wpos)
{
    flush_workspace(0, wpos);

    PageID pid = _workspace[0].pid;
    sysevent::log_page_write(pid, _clean_lsn, wpos);

    _clean_lsn = smlevel_0::log->curr_lsn();
}

bool bf_tree_cleaner::latch_and_copy(PageID pid, bf_idx idx, size_t wpos)
{
    const generic_page* const page_buffer = _bufferpool->_buffer;
    bf_tree_cb_t &cb = _bufferpool->get_cb(idx);

    // CS TODO: policy option: wait for latch or just attempt conditionally
    rc_t latch_rc = cb.latch().latch_acquire(LATCH_SH, WAIT_IMMEDIATE);
    if (latch_rc.is_error()) {
        // Could not latch page in EX mode -- just skip it
        return false;
    }

    fixable_page_h page;
    page.fix_nonbufferpool_page(const_cast<generic_page*>(&page_buffer[idx]));
    if (page.pid() != pid) {
        // New page was loaded in the frame -- skip it
        return false;
    }

    // CS TODO: get rid of this buggy and ugly deletion mechanism
    if (page.is_to_be_deleted()) {
        sys_xct_section_t sxs(true);
        W_COERCE (sxs.check_error_on_start());
        W_COERCE (smlevel_0::vol->deallocate_page(page_buffer[idx].pid));
        W_COERCE (sxs.end_sys_xct (RCOK));

        // drop the page from bufferpool too
        _bufferpool->_delete_block(idx);

        cb.latch().latch_release();
        return false;
    }

    // Copy page and update its page_lsn from what's on the cb
    generic_page& pdest = _workspace[wpos];
    ::memcpy(&pdest, page_buffer + idx, sizeof (generic_page));
    pdest.lsn = cb.get_page_lsn();
    // CS TODO: swizzling!
    // if the page contains a swizzled pointer, we need to convert
    // the data back to the original pointer.  we need to do this
    // before releasing SH latch because the pointer might be
    // unswizzled by other threads.
    _bufferpool->_convert_to_disk_page(&pdest);

    cb.latch().latch_release();

    _workspace[wpos].checksum = _workspace[wpos].calculate_checksum();
    _workspace_cb_indexes[wpos] = idx;

    return true;
}

void bf_tree_cleaner::collect_candidates()
{
    candidates.clear();

    bf_idx block_cnt = _bufferpool->_block_cnt;

    for (bf_idx idx = 1; idx < block_cnt; ++idx) {
        bf_tree_cb_t &cb = _bufferpool->get_cb(idx);
        cb.pin();

        // If page is not dirty or not in use, no need to flush
        if (!cb.is_dirty() || !cb._used) {
            cb.unpin();
            continue;
        }

        candidates.emplace_back(cb._pid, idx);
        cb.unpin();
    }

    // CS TODO: one policy could sort each sequence of adjacent pids by cluster size
    // Sort by PageID to exploit large sequential writes
    typedef pair<PageID, bf_idx> pair_t;
    auto lt = [] (const pair_t& a, const pair_t& b) { return a.first < b.first; };
    std::sort(candidates.begin(), candidates.end(), lt);
}
