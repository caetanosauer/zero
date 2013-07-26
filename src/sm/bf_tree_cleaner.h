/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef BF_TREE_CLEANER_H
#define BF_TREE_CLEANER_H

#include "w_defines.h"
#include "sm_int_0.h"
#include "smthread.h"
#include "vid_t.h"
#include "bf_idx.h"
#include "lsn.h"
#include <vector>

/** ID of cleaner worker thread. zero means no corresponding worker thread. */
typedef uint16_t bf_cleaner_slave_id_t;

class bf_tree_m;
class bf_tree_cleaner_slave_thread_t;
class page_s;

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
    w_rc_t wakeup_cleaner_for_volume (volid_t vol);

    /**
     * Gracefully request all cleaners to stop.
     */
    w_rc_t request_stop_cleaners ();
    /**
     * Waits until all cleaner threads stop. Should be used after request_stop_cleaners.
     * @param max_wait_millisec maximum milliseconds to wait for join. zero for forever.
     */
    w_rc_t join_cleaners (uint32_t max_wait_millisec = 0);

    /*
    **
    * Forcefully and immediately kills all cleaner threads. This method should be avoided if possible.
    *
    w_rc_t kill_cleaners ();
    */

    /** Immediately writes out all dirty pages in the given volume.*/
    w_rc_t force_volume (volid_t vol);
    
    /** Immediately writes out all dirty pages.*/
    w_rc_t force_all ();

    /** Immediately writes out all dirty pages up to the given LSN. used while checkpointing. */
    w_rc_t force_until_lsn (lsndata_t lsn);

private:
    bool _is_force_until_lsn_done(lsndata_t lsn) const;
    /**
     * Wakes up the specified cleaner thread, starting it if not started yet.
     */
    w_rc_t _wakeup_a_cleaner(bf_cleaner_slave_id_t id);

    /** the buffer pool this cleaner deals with. */
    bf_tree_m*                  _bufferpool;

    /**
     * _volume_assignments[vol] is the ID of assigned cleaner thread
     * for the vol. The assignments are simply balanced regarding
     * the count of volumes per thread.
     */
    bf_cleaner_slave_id_t       _volume_assignments[MAX_VOL_COUNT];

    /**
     * _volume_requests[vol] indicates whether the volume is requested
     * to be flushed by force_volume() or force_all().
     * When the corresponding worker observes this flag and completes
     * flushing all dirty pages in the volume, the worker turns off the flag.
     */
    bool                        _requested_volumes[MAX_VOL_COUNT];

    /** whether any unexpected error happened in some cleaner. */
    bool                        _error_happened;

    /**
     * The LSN up to which all cleaners are requested to flush out all dirty pages.
     * Used while checkpointing.
     * Accesses to this variable are NOT synchronized because, even if an unlucky event
     * happens, the requesting thread will just request again until all slaves
     * have _completed_lsn as large as they need.
     * This simplification assumes the read/write of lsndata_t is atomic or at least regular,
     * which should be true as it's a 64-bits integer.
     */
    volatile lsndata_t          _requested_lsn;

    /**
     * An array of slave threads indexed by the slave-id.
     * Index-zero is always NULL because slave-id=0 means no worker.
     */
    bf_tree_cleaner_slave_thread_t** _slave_threads;

    /**
     * Size of _slave_threads including the dummy slave-id=0 entry.
     */
    const bf_cleaner_slave_id_t _slave_threads_size;
    
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
 * No volume (thus no page) is assigned to more than one cleaners to simplify synchronization,
 * which causes no harm because more than one threads for a single physical disk
 * are useless anyway. This also means that it's useless to have cleaner threads
 * more than the count of volumes.
 */
class bf_tree_cleaner_slave_thread_t : public smthread_t {
    friend class bf_tree_cleaner;
public:
    bf_tree_cleaner_slave_thread_t (bf_tree_cleaner* parent, bf_cleaner_slave_id_t id);
    ~bf_tree_cleaner_slave_thread_t ();
    
    void run();
    
    /** wakes up this thread if it's currently sleeping. */
    void wakeup ();

private:
    bool _cond_timedwait (uint64_t timeout_microsec);
    void _take_interval ();
    w_rc_t _do_work ();
    bool _exists_requested_work();
    w_rc_t _clean_volume(volid_t vol, const std::vector<bf_idx> &candidates, bool requested_volume, lsndata_t requested_lsn);
    w_rc_t _flush_write_buffer(volid_t vol, size_t from, size_t consecutive);

    /** parent object. */
    bf_tree_cleaner*            _parent;

    /** ID of this thread. */
    const bf_cleaner_slave_id_t _id;
    
    /** whether this thread has been requested to start. */
    volatile bool               _start_requested;
    /** whether this thread is currently running. */
    volatile bool               _running;
    /** whether this thread has been requested to stop. */
    volatile bool               _stop_requested;
    /** whether this thread has been requested to wakeup. */
    volatile bool               _wakeup_requested;
    
    /**
     * milliseconds to sleep when finding no dirty pages.
     * this value starts from CLEANER_INTERVAL_MILLISEC_MIN and exponentially grows up to CLEANER_INTERVAL_MILLISEC_MAX. 
     */
    uint32_t                    _interval_millisec;
    pthread_mutex_t             _interval_mutex;
    pthread_cond_t              _interval_cond;
    
    /**
     * The LSN up to which this thread has flushed out all dirty pages.
     * Used while checkpointing.
     * Only one thread (this) is writing and access to lsndata_t (64 bits integer) is atomic, so always safe.
     */
    volatile lsndata_t          _completed_lsn;

    /** reused buffer of dirty page's indexes for each volume. */
    std::vector<std::vector<bf_idx> >         _candidates_buffer;
    /** reused buffer for sorting dirty page's indexes. */
    uint64_t*                   _sort_buffer;
    /** size of _sort_buffer. */
    size_t                      _sort_buffer_size;

    /** reused buffer to write out the content of dirty pages. */
    page_s*                     _write_buffer;
    bf_idx*                     _write_buffer_indexes;
};

#endif // BF_TREE_CLEANER_H
