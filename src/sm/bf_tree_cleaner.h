/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BF_TREE_CLEANER_H
#define BF_TREE_CLEANER_H

#include "w_defines.h"
#include "sm_base.h"
#include "smthread.h"
#include "lsn.h"
#include "vol.h"
#include <atomic>
#include <vector>
#include "page_cleaner_base.h"

class bf_tree_m;
class generic_page;

/**
 * \brief The diry page cleaner for the new bufferpool manager.
 * \ingroup SSMBUFPOOL
 * \details
 * This class manages all worker threads which do the
 * actual page cleaning.
 */
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

    void run();

    /**
     * Wakes up the cleaner thread assigned to the given volume.
     */
    void wakeup(bool wait = false);

    /**
     * Gracefully request all cleaners to stop.
     */
    void shutdown ();

    /** Immediately writes out all dirty pages in the given volume.*/
    void force_volume();

private:
    void _do_work ();
    void _clean_volume(const std::vector<bf_idx> &candidates);
    void _flush_write_buffer(size_t from, size_t consecutive);

    /** the buffer pool this cleaner deals with. */
    bf_tree_m*                  _bufferpool;

    uint32_t _write_buffer_pages;
    long _interval_msec;

    std::mutex _wakeup_mutex;
    std::condition_variable _wakeup_condvar;

    std::mutex _done_mutex;
    std::condition_variable _done_condvar;

    /** reused buffer of dirty page's indexes . */
    std::vector<bf_idx>         _candidates_buffer;
    /** reused buffer for sorting dirty page's indexes. */
    uint64_t*                   _sort_buffer;
    /** size of _sort_buffer. */
    size_t                      _sort_buffer_size;

    /** reused buffer to write out the content of dirty pages. */
    generic_page*               _write_buffer;
    bf_idx*                     _write_buffer_indexes;

    /** whether this thread has been requested to stop. */
    bool _stop_requested;
    /** whether this thread has been requested to wakeup. */
    bool _wakeup_requested;
    /** whether cleaner is currently running */
    bool _cleaner_running;

    lsn_t clean_lsn;
};

#endif // BF_TREE_CLEANER_H
