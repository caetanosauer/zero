/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "bf_tree_cleaner.h"
#include "sm_base.h"
#include "bf_tree.h"
#include "generic_page.h"
#include "fixable_page_h.h"
#include "log_core.h"
#include "alloc_cache.h"
#include "stnode_page.h"
#include "vol.h"
#include "xct_logger.h"
#include "sm.h"
#include "stopwatch.h"
#include "xct.h"
#include <vector>

bf_tree_cleaner::bf_tree_cleaner(const sm_options& options)
    : page_cleaner_base(options)
{
    num_candidates = options.get_int_option("sm_cleaner_num_candidates", 0);
    min_write_size = options.get_int_option("sm_cleaner_min_write_size", 1);
    min_write_ignore_freq = options.get_int_option("sm_cleaner_min_write_ignore_freq", 0);

    string pstr = options.get_string_option("sm_cleaner_policy", "oldest_lsn");
    policy = make_cleaner_policy(pstr);

    if (num_candidates == 0) {
        // Setting num_candidates to zero means that at every round, ALL pages
        // in the buffer pool are cleaned, and the policy therefore doesn't
        // matter at all. Because this kind of behavior doesn't really make
        // sense, we set num_candidates to be the workspace size.
        num_candidates = _workspace_size;
    }

    candidates.reserve(num_candidates);
}

bf_tree_cleaner::~bf_tree_cleaner()
{
}

void bf_tree_cleaner::do_work()
{
    if (ss_m::bf->is_no_db_mode() || !ss_m::vol || !ss_m::vol->caches_ready()) {
        // No volume manager initialized -- no point in starting cleaner
        return;
    }

    if (policy == cleaner_policy::no_policy) {
        clean_no_policy();
    }
    else {
        collect_candidates();
        clean_candidates();
    }
}

/*
 * Without a cleaner policy (i.e., policy == no_policy), one cleaner round
 * consists simply of flushing all dirty pages we can find in one sweep
 * of the whole buffer pool.
 */
void bf_tree_cleaner::clean_no_policy()
{
    size_t w_index = 0;
    for (bf_idx idx = 1; idx < _bufferpool->get_block_cnt(); ++idx) {
        auto& cb = _bufferpool->get_cb(idx);
        if (!cb.pin()) { continue; }

        // If page is not dirty or not in use, no need to flush
        if (!cb.is_dirty() || !cb._used || cb.get_rec_lsn().is_null()) {
            cb.unpin();
            continue;
        }

        if (!latch_and_copy(cb._pid, idx, w_index)) {
            continue;
        }
        w_index++;

        if (w_index == _workspace_size) {
            flush_workspace_no_clusters(w_index);
            w_index = 0;
        }

        cb.unpin();
    }

    flush_workspace_no_clusters(w_index);
}

// Used by no_policy_round
void bf_tree_cleaner::flush_workspace_no_clusters(size_t count)
{
    w_assert1(count <= _workspace_size);

    for (size_t i = 0; i < count; i++) {
        write_pages(i, i+1);
    }

    smlevel_0::vol->sync();

    for (size_t i = 0; i < count; i++) {
        PageID pid = _workspace[i].pid;
        Logger::log_sys<page_write_log>(pid, _clean_lsn, 1);
        mark_pages_clean(i, i+1);
    }
}

void bf_tree_cleaner::clean_candidates()
{
    if (candidates.empty()) {
        return;
    }
    stopwatch_t timer;

    size_t i = 0;
    // bool ignore_min_write = ignore_min_write_now();

    // keeps track of cluster positions in the workspace, so that
    // they can be flush asynchronously below
    std::vector<size_t> clusters;

    while (i < candidates.size()) {
        // Get size of current cluster
        // size_t cluster_size = 1;
        // for (size_t j = i + 1; j < candidates.size(); j++) {
        //     if (candidates[j].pid != candidates[i].pid + (j - i)) {
        //         break;
        //     }
        //     cluster_size++;
        // }

        // // Skip if current cluster is too small
        // if (!ignore_min_write && cluster_size < min_write_size) {
        //     i++;
        //     continue;
        // }

        // ADD_TSTAT(cleaner_time_cpu, timer.time_us());

        _clean_lsn = smlevel_0::log->durable_lsn();

        // index of the current frame in the workspace
        size_t w_index = 0;

        // Copy pages in the cluster to the workspace
        size_t k = 0;
        PageID prev_pid = candidates[i].pid;
        while (w_index < _workspace_size && i+k < candidates.size()) {
            PageID pid = candidates[i+k].pid;
            bf_idx idx = candidates[i+k].idx;

            if (!latch_and_copy(pid, idx, w_index)) {
                k++;
                continue;
            }

            // std::cerr << "cleaning " << candidates[i+k] << endl;

            if (pid > prev_pid + 1 && w_index > 0) {
                // Current cluster ends at index k (k is part of the next cluster)
                clusters.push_back(w_index);
            }

            k++;
            w_index++;
            prev_pid = pid;

            if (should_exit()) { break; }
        }

        clusters.push_back(w_index);

        ADD_TSTAT(cleaner_time_copy, timer.time_us());

        if (should_exit()) { break; }

        flush_clusters(clusters);
        ADD_TSTAT(cleaner_time_io, timer.time_us());

        clusters.clear();
        i += k;
    }
}

void bf_tree_cleaner::flush_clusters(const vector<size_t>& clusters)
{
    size_t i = 0;
    for (auto k : clusters) {
        w_assert1(k > i);
        write_pages(i, k);
        i = k;
    }

    smlevel_0::vol->sync();

    i = 0;
    for (auto k : clusters) {
        w_assert1(k > i);
        PageID pid = _workspace[i].pid;
        Logger::log_sys<page_write_log>(pid, _clean_lsn, k-i);
        mark_pages_clean(i, k);
        i = k;
    }
}

bool bf_tree_cleaner::latch_and_copy(PageID pid, bf_idx idx, size_t wpos)
{
    const generic_page* const page_buffer = _bufferpool->_buffer;
    bf_tree_cb_t &cb = _bufferpool->get_cb(idx);

    auto rc = cb.latch().latch_acquire(LATCH_SH, timeout_t::WAIT_IMMEDIATE);
    if (rc.is_error()) { return false; }

    // No need to pin CB here because eviction must hold EX latch

    fixable_page_h page;
    page.fix_nonbufferpool_page(const_cast<generic_page*>(&page_buffer[idx]));
    if (cb.is_pinned_for_restore() || page.pid() != pid || !cb.is_in_use()) {
        // New page was loaded in the frame -- skip it
        cb.latch().latch_release();
        return false;
    }

    // Pages can be cleaned with the check_recovery flag as long as their
    // expected LSN is registered in the dirty page table by the evictioner

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

    // if the page contains a swizzled pointer, we need to convert
    // the data back to the original pointer.  we need to do this
    // before releasing SH latch because the pointer might be
    // unswizzled by other threads.
    if (pdest.tag == t_btree_p) {
        _bufferpool->_convert_to_disk_page(&pdest);
    }

    // Record the fact that we are taking a copy for flushing in the CB
    cb.mark_persisted_lsn();

    cb.latch().latch_release();

    _workspace[wpos].checksum = _workspace[wpos].calculate_checksum();
    _workspace_cb_indexes[wpos] = idx;

    return true;
}

policy_predicate_t bf_tree_cleaner::get_policy_predicate(cleaner_policy p)
{
    // A less-than function makes pop_heap return the highest value, and a
    // greater-than function the lowest. Because the heap's top element should
    // be the lowest in a "highest" policy and vice-versa, greater-than should be
    // used for "highest" policies and vice-versa. When testing if an element
    // should replace the current top of the heap, the inverse of the
    // comparison function should be used, e.g., in a "highest" policy, an
    // incoming element enters the heap if it is greater than the heap's
    // lowest.
    // IN SUMMARY: greater-than --> highest
    //             lower-than --> lowest
    switch (p) {
        case cleaner_policy::highest_refcount:
            return [this] (const cleaner_cb_info& a, const cleaner_cb_info& b)
            {
                return a.ref_count > b.ref_count;
            };
        case cleaner_policy::lowest_refcount:
            return [this] (const cleaner_cb_info& a, const cleaner_cb_info& b)
            {
                return a.ref_count < b.ref_count;
            };
        case cleaner_policy::lru:
            return [this] (const cleaner_cb_info& a, const cleaner_cb_info& b)
            {
                return a.page_lsn < b.page_lsn;
            };
        case cleaner_policy::highest_density:
            return [this] (const cleaner_cb_info& a, const cleaner_cb_info& b)
            {
                lsndata_t a_window = a.page_lsn.data() - a.rec_lsn.data();
                if (a_window == 0) { a_window = std::numeric_limits<lsndata_t>::max(); }
                auto a_density = (a.ref_count / a_window);

                lsndata_t b_window = b.page_lsn.data() - b.rec_lsn.data();
                if (b_window == 0) { b_window = std::numeric_limits<lsndata_t>::max(); }
                auto b_density = (b.ref_count / b_window);

                return a_density > b_density;
            };
        case cleaner_policy::mixed:
            // mixed policy = LRU half of the time, oldest_lsn the other half
            if (get_rounds_completed() % 2 == 0) {
                return get_policy_predicate(cleaner_policy::lru);
            }
            else {
                return get_policy_predicate(cleaner_policy::oldest_lsn);
            }
        case cleaner_policy::oldest_lsn: default:
            return [this] (const cleaner_cb_info& a, const cleaner_cb_info& b)
            {
                return a.rec_lsn < b.rec_lsn;
            };
    }
}

void bf_tree_cleaner::collect_candidates()
{
    stopwatch_t timer;
    candidates.clear();

    // Comparator to be used by the heap
    auto heap_cmp = get_policy_predicate(policy);

    bf_idx block_cnt = _bufferpool->_block_cnt;

    for (bf_idx idx = 1; idx < block_cnt; ++idx) {
        auto& cb = _bufferpool->get_cb(idx);
        if (!cb.pin()) { continue; }

        // If page is not dirty or not in use, no need to flush
        if (!cb.is_dirty() || !cb._used || cb.get_rec_lsn().is_null() ||
                cb.is_pinned_for_restore())
        {
            cb.unpin();
            continue;
        }

        // add new element to the back of vector
        candidates.emplace_back(idx, cb);

        cb.unpin();

        // manage heap if we are limiting the number of candidates
        if (num_candidates > 0) {
            if (candidates.size() < num_candidates ||
                !heap_cmp(candidates.front(), candidates.back()))
            {
                // if it's among the top-k candidates, push it into the heap
                std::push_heap(candidates.begin(), candidates.end(), heap_cmp);
                while (candidates.size() > num_candidates) {
                    std::pop_heap(candidates.begin(), candidates.end(), heap_cmp);
                    // cerr << "popped "<< candidates.back() << endl;
                    candidates.pop_back();
                }
            }
            // otherwise just remove it
            else { candidates.pop_back(); }
        }
    }

    // CS TODO: one policy could sort each sequence of adjacent pids by cluster size
    // Sort by PageID to exploit large sequential writes
    auto lt = [] (const cleaner_cb_info& a, const cleaner_cb_info& b)
    {
        return a.pid < b.pid;
    };

    std::sort(candidates.begin(), candidates.end(), lt);

    ADD_TSTAT(cleaner_time_cpu, timer.time_us());
}

std::ostream& operator<<(std::ostream& out, const cleaner_cb_info& cb)
{
    out << "pid=" << cb.pid
        << " page=" << cb.page_lsn
        << " rec=" << cb.rec_lsn
        << std::endl;

    return out;
}
