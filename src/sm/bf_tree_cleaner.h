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
class bf_tree_cleaner_slave_thread_t;
class generic_page;

/**
 * \brief The diry page cleaner for the new bufferpool manager.
 * \ingroup SSMBUFPOOL
 * \details
 * This class manages all worker threads which do the
 * actual page cleaning.
 */
class bf_tree_cleaner {
    friend class bf_tree_cleaner_slave_thread_t;
public:
    /**
     * Constructs this object. This merely allocates arrays and objects.
     * The real start-up is done in start_cleaners() because a constructor can't return error codes.
     * @param cleaner_threads count of worker threads. has to be at least 1. If you want to disable
     * all cleaners at first, use the following parameter.
     * @param initially_wakeup_workers whether to start cleaner threads as soon as possible.
     * Even if this is false, you can start cleaners later by calling wakeup_cleaners().
     */
    bf_tree_cleaner(bf_tree_m* bufferpool, uint32_t cleaner_threads,
        uint32_t cleaner_interval_millisec_min,
        uint32_t cleaner_interval_millisec_max,
        uint32_t cleaner_write_buffer_pages,
        bool initially_wakeup_workers);

    /**
     * Destructs this object. This merely de-allocates arrays and objects.
     * Use request_stop_cleaners() or kill_cleaners() to stop cleaner threads.
     */
    ~bf_tree_cleaner();

    /**
     * Starts up the cleaners. Sort of initialization of this object.
     */
    w_rc_t start_cleaners();

    /**
     * Wakes up all cleaner threads, starting them if not started yet.
     */
    w_rc_t wakeup_cleaners ();

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

    /** Immediately writes out all dirty pages.*/
    w_rc_t force_all ();

private:
    /** the buffer pool this cleaner deals with. */
    bf_tree_m*                  _bufferpool;

    unsigned get_cleaner();

    /**
     * _volume_requests[vol] indicates whether the volume is requested
     * to be flushed by force_volume() or force_all().
     * When the corresponding worker observes this flag and completes
     * flushing all dirty pages in the volume, the worker turns off the flag.
     */
    bool               _requested_volume;

    /** whether any unexpected error happened in some cleaner. */
    bool                        _error_happened;

    bf_tree_cleaner_slave_thread_t* _slave_thread;

    const uint32_t _cleaner_interval_millisec_min;
    const uint32_t _cleaner_interval_millisec_max;
    const uint32_t _cleaner_write_buffer_pages;

    /**
     * whether to start cleaner threads as soon as possible.
     * Even if this is false, you can start cleaners later by calling wakeup_cleaners().
     */
    const bool                  _initially_wakeup_workers;
};

/**
 * \brief The worker thread to cleans out buffer frames.
 * \ingroup SSMBUFPOOL
 * \details
 * Each worker thread is assigned to an arbitrary number of volumes.
 * On the other hand, every volume is assigned to a single cleaner worker
 * so that it can efficiently write out contiguous dirty pages.
 *
 * No volume (thus no page) is assigned to more than one cleaner to simplify synchronization,
 * which causes no harm because more than one thread for a single physical disk
 * are useless anyway. This also means that it's useless to have more cleaner threads
 * than the count of volumes.
 */
class bf_tree_cleaner_slave_thread_t : public smthread_t {
    friend class bf_tree_cleaner;
public:
    bf_tree_cleaner_slave_thread_t (bf_tree_cleaner* parent);
    ~bf_tree_cleaner_slave_thread_t ();

    void run();

    /** wakes up this thread if it's currently sleeping. */
    void wakeup ();

private:
    bool _cond_timedwait (uint64_t timeout_microsec);
    void _take_interval ();
    w_rc_t _do_work ();
    bool _exists_requested_work();
    w_rc_t _clean_volume(const std::vector<bf_idx> &candidates);
    w_rc_t _flush_write_buffer(size_t from, size_t consecutive,
            unsigned& cleaned_count);

    /** parent object. */
    bf_tree_cleaner*            _parent;

    /** @todo at some point the flags below should probably become std::atomic_flag's (or lintel
        should be updated to include that type). I also think memory_order_consume would
        be OK for the accesses, instead of the default memory_order_seq_cst, but thats an
        exercise for another day, and a non-x86 architecture. */
    /** whether this thread has been requested to start. */
    lintel::Atomic<bool> _start_requested;
    /** whether this thread is currently running. */
    lintel::Atomic<bool> _running;
    /** whether this thread has been requested to stop. */
    lintel::Atomic<bool> _stop_requested;
    /** whether this thread has been requested to wakeup. */
    lintel::Atomic<bool> _wakeup_requested;

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
    generic_page*                     _write_buffer;
    bf_idx*                     _write_buffer_indexes;
};

#endif // BF_TREE_CLEANER_H
