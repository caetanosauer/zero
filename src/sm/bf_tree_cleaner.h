/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BF_TREE_CLEANER_H
#define BF_TREE_CLEANER_H

#include "page_cleaner.h"
#include "bf_tree_cb.h"
#include <functional>

class bf_tree_m;

/**
 * These classes encapsulate a single comparator function to be used
 * as a cleaner policy.
 */
enum class cleaner_policy {
    highest_refcount,
    lowest_refcount,
    oldest_lsn,
    mixed
};

/**
 * Information about each candidate control block considered by whatever
 * cleaner policy is currently active
 */
struct cleaner_cb_info {
    lsn_t clean_lsn;
    lsn_t page_lsn;
    bf_idx idx;
    PageID pid;
    uint16_t ref_count;

    cleaner_cb_info(bf_idx idx, const bf_tree_cb_t& cb) :
        clean_lsn(cb.get_clean_lsn()),
        page_lsn(cb.get_page_lsn()),
        idx(idx),
        pid(cb._pid),
        ref_count(cb._ref_count_ex)
    {}
};

/** Type of predicate functions used by cleaner policies */
using policy_predicate_t =
    std::function<bool(const cleaner_cb_info&, const cleaner_cb_info&)>;

// Forward declaration
class candidate_collector_thread;

class bf_tree_cleaner : public page_cleaner_base {
    friend class candidate_collector_thread;
public:
    /**
     * Constructs this object. This merely allocates arrays and objects.
     * The real start-up is done in start_cleaners() because a constructor can't return error codes.
     * @param cleaner_threads count of worker threads. has to be at least 1. If you want to disable
     * all cleaners at first, use the following parameter.
     * @param initially_wakeup_workers whether to start cleaner threads as soon as possible.
     * Even if this is false, you can start cleaners later by calling wakeup_cleaners().
     */
    bf_tree_cleaner(bf_tree_m* bufferpool, const sm_options& _options);

    /**
     * Destructs this object. This merely de-allocates arrays and objects.
     * Use request_stop_cleaners() or kill_cleaners() to stop cleaner threads.
     */
    ~bf_tree_cleaner();

protected:
    virtual void do_work ();

    /** Return predicate function object that implements given policy */
    policy_predicate_t get_policy_predicate();

    bool ignore_min_write_now() const
    {
        if (min_write_size <= 1) { return true; }
        return min_write_ignore_freq > 0 &&
            (get_rounds_completed() % min_write_ignore_freq == 0);
    }

private:
    void collect_candidates();
    void clean_candidates();
    void log_and_flush(size_t wpos);
    bool latch_and_copy(PageID, bf_idx, size_t wpos);

    /**
     * List of candidate dirty frames to be considered for cleaning.
     * We use two lists -- one is filled in parallel by the candidate collector
     * thread and the other, already collected one, is used to copy and flush.
     */
    unique_ptr<vector<cleaner_cb_info>> next_candidates;
    unique_ptr<vector<cleaner_cb_info>> curr_candidates;

    /// Cleaner policy options
    size_t num_candidates;
    cleaner_policy policy;

    /// Only write out clusters of pages with this minimum size
    size_t min_write_size;

    // Ignore min write size every N rounds (0 for never)
    size_t min_write_ignore_freq;

    /// Do not clean alloc/stnode pages, which are not managed in the buffer pool
    bool ignore_metadata;

    /// Use an asynchronous thread to collect candidates, thus allowing overlap
    /// of CPU utilization and I/O
    bool async_candidate_collection;

    unique_ptr<candidate_collector_thread> collector;
};

inline cleaner_policy make_cleaner_policy(string s)
{
    if (s == "highest_refcount") { return cleaner_policy::highest_refcount; }
    if (s == "lowest_refcount") { return cleaner_policy::lowest_refcount; }
    if (s == "oldest_lsn") { return cleaner_policy::oldest_lsn; }
    if (s == "mixed") { return cleaner_policy::mixed; }
    return cleaner_policy::oldest_lsn;
}

#endif // BF_TREE_CLEANER_H
