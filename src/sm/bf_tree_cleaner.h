/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BF_TREE_CLEANER_H
#define BF_TREE_CLEANER_H

#include "page_cleaner.h"

class bf_tree_m;

class bf_tree_cleaner : public page_cleaner_base {
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

private:
    virtual void do_work ();
    void collect_candidates();
    void clean_candidates();
    void log_and_flush(size_t wpos);
    bool latch_and_copy(PageID, bf_idx, size_t wpos);

    /** List of candidate dirty frames to be considered for cleaning */
    std::vector<pair<PageID, bf_idx>>         candidates;
};

#endif // BF_TREE_CLEANER_H
