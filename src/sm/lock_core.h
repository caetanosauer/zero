/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef LOCK_CORE_H
#define LOCK_CORE_H

#include <stdint.h>
#include "lsn.h"

struct RawLock;
struct RawLockQueue;
struct RawXct;
class RawLockBackgroundThread;
template <class T> struct GcPoolForest;
class sm_options;
struct RawLockCleanerFunctor;
class lil_global_table;
class vtable_t;
class okvl_mode;

/**
* \brief Lock table implementation class.
* \ingroup SSMLOCK
* \details
* This is the gut of lock management in Foster B-trees.
* Most of the implementation has been moved to lock_raw.h/cpp.
*/
class lock_core_m {
public:
    NORET        lock_core_m(const sm_options &options);
    NORET        ~lock_core_m();

    int          collect(vtable_t&, bool names_too);
    void        assert_empty() const;
    void        dump(std::ostream &o);


    lil_global_table*   get_lil_global_table() { return _lil_global_table; }

public:
    /** @copydoc RawLockQueue::acquire() */
    w_error_codes  acquire_lock(RawXct* xd, uint32_t hash, const okvl_mode& mode,
                bool check, bool wait, bool acquire, int32_t timeout, RawLock** out);

    /** @copydoc RawLockQueue::retry_acquire() */
    w_error_codes  retry_acquire(RawLock** lock, bool check_only, int32_t timeout);

    void        release_lock(RawLock* lock, lsn_t commit_lsn = lsn_t::null);

    void        release_duration(bool read_lock_only = false, lsn_t commit_lsn = lsn_t::null);

    /**
     * Instantiate shadow transaction object for RAW-style lock manager for the current thread.
     */
    RawXct*     allocate_xct();
    void        deallocate_xct(RawXct* xct);
private:
    uint32_t        _table_bucket(uint32_t id) const { return id % _htabsz; }

    GcPoolForest<RawLock>*      _lock_pool;
    GcPoolForest<RawXct>*       _xct_pool;
    RawLockCleanerFunctor*      _raw_lock_cleaner_functor;
    RawLockBackgroundThread*    _raw_lock_cleaner;

    RawLockQueue*       _htab;
    uint32_t            _htabsz;

    /** Global lock table for Light-weight Intent Lock. */
    lil_global_table*  _lil_global_table;
};

// TODO to remove
// this is for experiments to compare deadlock detection/recovery methods.
#define SWITCH_DEADLOCK_IMPL
#ifdef SWITCH_DEADLOCK_IMPL
/** Whether to use the dreadlock sleep-backoff algorithm? */
extern bool g_deadlock_use_waitmap_obsolete;
/** How long to sleep between each dreadlock spin? */ 
extern int g_deadlock_dreadlock_interval_ms;
class xct_t;
class lock_request_t;
/** function pointer for the implementation of arbitrary _check_deadlock impl. */ 
extern w_error_codes (*g_check_deadlock_impl)(xct_t* xd, lock_request_t *myreq);
#endif // SWITCH_DEADLOCK_IMPL

#endif          /*</std-footer>*/
