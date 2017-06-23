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
    mixed,
    highest_density,
    lru,
    no_policy
};

/**
 * Information about each candidate control block considered by whatever
 * cleaner policy is currently active
 */
struct cleaner_cb_info {
    lsn_t page_lsn;
    lsn_t rec_lsn;
    bf_idx idx;
    PageID pid;
    uint16_t ref_count;

    cleaner_cb_info(bf_idx idx, const bf_tree_cb_t& cb) :
        page_lsn(cb.get_page_lsn()),
        rec_lsn(cb.get_rec_lsn()),
        idx(idx),
        pid(cb._pid),
        ref_count(cb._ref_count_ex)
    {}

    friend std::ostream& operator<<(std::ostream& out, const cleaner_cb_info& cb);
};

/** Type of predicate functions used by cleaner policies */
using policy_predicate_t =
    std::function<bool(const cleaner_cb_info&, const cleaner_cb_info&)>;

class bf_tree_cleaner : public page_cleaner_base
{
public:
    bf_tree_cleaner(const sm_options& _options);
    ~bf_tree_cleaner();

protected:
    virtual void do_work ();

    /** Return predicate function object that implements given policy */
    policy_predicate_t get_policy_predicate(cleaner_policy p);

    bool ignore_min_write_now() const
    {
        if (min_write_size <= 1) { return true; }
        return min_write_ignore_freq > 0 &&
            (get_rounds_completed() % min_write_ignore_freq == 0);
    }

private:
    void collect_candidates();
    void clean_candidates();
    void flush_clusters(const vector<size_t>& clusters);
    bool latch_and_copy(PageID, bf_idx, size_t wpos);

    // Methods used by cleaning without a policy
    void clean_no_policy();
    void flush_workspace_no_clusters(size_t count);

    /**
     * List of candidate dirty frames to be considered for cleaning.
     */
    vector<cleaner_cb_info> candidates;

    /// Cleaner policy options
    size_t num_candidates;
    cleaner_policy policy;

    /// Only write out clusters of pages with this minimum size
    size_t min_write_size;

    // Ignore min write size every N rounds (0 for never)
    size_t min_write_ignore_freq;
};

inline cleaner_policy make_cleaner_policy(string s)
{
    if (s == "highest_refcount") { return cleaner_policy::highest_refcount; }
    if (s == "lowest_refcount") { return cleaner_policy::lowest_refcount; }
    if (s == "oldest_lsn") { return cleaner_policy::oldest_lsn; }
    if (s == "mixed") { return cleaner_policy::mixed; }
    if (s == "highest_density") { return cleaner_policy::highest_density; }
    if (s == "lru") { return cleaner_policy::lru; }
    if (s == "no_policy") { return cleaner_policy::no_policy; }
    w_assert0(false);
}

#endif // BF_TREE_CLEANER_H
