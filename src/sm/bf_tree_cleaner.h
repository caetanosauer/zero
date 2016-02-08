/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BF_TREE_CLEANER_H
#define BF_TREE_CLEANER_H

#include "w_defines.h"
#include "sm_base.h"
#include "smthread.h"
#include "bf_idx.h"
#include "lsn.h"
#include "vol.h"
#include <AtomicCounter.hpp>
#include <vector>

class bf_tree_m;
class generic_page;

/**
 * \brief The diry page cleaner for the new bufferpool manager.
 * \ingroup SSMBUFPOOL
 * \details
 * This class manages all worker threads which do the
 * actual page cleaning.
 */
class bf_tree_cleaner : public smthread_t {
public:
    /**
     * Constructs this object. This merely allocates arrays and objects.
     * The real start-up is done in start_cleaners() because a constructor can't return error codes.
     * @param cleaner_threads count of worker threads. has to be at least 1. If you want to disable
     * all cleaners at first, use the following parameter.
     * @param initially_wakeup_workers whether to start cleaner threads as soon as possible.
     * Even if this is false, you can start cleaners later by calling wakeup_cleaners().
     */
    bf_tree_cleaner(bf_tree_m* bufferpool,
        uint32_t interval_millisec_min,
        uint32_t interval_millisec_max,
        uint32_t write_buffer_pages);

    /**
     * Destructs this object. This merely de-allocates arrays and objects.
     * Use request_stop_cleaners() or kill_cleaners() to stop cleaner threads.
     */
    ~bf_tree_cleaner();

    void run();

    /**
     * Wakes up the cleaner thread assigned to the given volume.
     */
    w_rc_t wakeup_cleaner();

    /**
     * Gracefully request all cleaners to stop.
     */
    w_rc_t request_stop_cleaner ();
    /**
     * Waits until all cleaner threads stop. Should be used after request_stop_cleaners.
     * @param max_wait_millisec maximum milliseconds to wait for join. zero for forever.
     */
    w_rc_t join_cleaner (uint32_t max_wait_millisec = 0);

    /** Immediately writes out all dirty pages in the given volume.*/
    w_rc_t force_volume();

private:
    bool _cond_timedwait (uint64_t timeout_microsec);
    void _take_interval ();
    w_rc_t _do_work ();
    bool _exists_requested_work();
    w_rc_t _clean_volume(const std::vector<bf_idx> &candidates);
    w_rc_t _flush_write_buffer(size_t from, size_t consecutive, unsigned& cleaned_count);

    /** the buffer pool this cleaner deals with. */
    bf_tree_m*                  _bufferpool;

    const uint32_t _interval_millisec_min;
    const uint32_t _interval_millisec_max;
    const uint32_t _write_buffer_pages;

    /**
     * milliseconds to sleep when finding no dirty pages.
     * this value starts from CLEANER_INTERVAL_MILLISEC_MIN and exponentially grows up to CLEANER_INTERVAL_MILLISEC_MAX.
     */
    uint32_t                    _interval_millisec;
    pthread_mutex_t             _interval_mutex;
    pthread_cond_t              _interval_cond;

    /** reused buffer of dirty page's indexes . */
    std::vector<bf_idx>         _candidates_buffer;
    /** reused buffer for sorting dirty page's indexes. */
    uint64_t*                   _sort_buffer;
    /** size of _sort_buffer. */
    size_t                      _sort_buffer_size;

    /** reused buffer to write out the content of dirty pages. */
    generic_page*               _write_buffer;
    bf_idx*                     _write_buffer_indexes;

    /**
     * _volume_requests[vol] indicates whether the volume is requested
     * to be flushed by force_volume() or force_all().
     * When the corresponding worker observes this flag and completes
     * flushing all dirty pages in the volume, the worker turns off the flag.
     */
    bool               _requested_volume;

    /** @todo at some point the flags below should probably become std::atomic_flag's (or lintel
        should be updated to include that type). I also think memory_order_consume would
        be OK for the accesses, instead of the default memory_order_seq_cst, but thats an
        exercise for another day, and a non-x86 architecture. */
    /** whether this thread has been requested to stop. */
    lintel::Atomic<bool> _stop_requested;
    /** whether this thread has been requested to wakeup. */
    lintel::Atomic<bool> _wakeup_requested;

    /** whether any unexpected error happened in some cleaner. */
    bool                        _error_happened;
};

#endif // BF_TREE_CLEANER_H
