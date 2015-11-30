/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef LOCK_H
#define LOCK_H

#include "w_defines.h"

#include "w_okvl.h"
#include "w_okvl_inl.h"
#include "kvl_t.h"
#include "lock_s.h"

class xct_lock_info_t;
class lock_core_m;
class lil_global_table;
struct RawXct;
struct RawLock;
class sm_options;

/**
 * \brief Lock Manager API.
 * \ingroup SSMLOCK
 * See \ref OKVL and \ref LIL.
 */
class lock_m : public smlevel_0 {
public:
    // initialize/takedown functions for thread-local state
    static void on_thread_init();
    static void on_thread_destroy();

    NORET                        lock_m(const sm_options &options);
    NORET                        ~lock_m();

    int                          collect(vtable_t&, bool names_too);

    /**
    * \brief Unsafely check that the lock table is empty for debugging
    *  and assertions at shutdown, when MT-safety shouldn't be an issue.
    */
    void                         assert_empty() const;

    /**
     * \brief Unsafely dump the lock hash table (for debugging).
     * \details Doesn't acquire the mutexes it should for safety, but
     * allows you dump the table while inside the lock manager core.
     */
    void                         dump(ostream &o);

    void                         stats(
                                    u_long & buckets_used,
                                    u_long & max_bucket_len,
                                    u_long & min_bucket_len,
                                    u_long & mode_bucket_len,
                                    float & avg_bucket_len,
                                    float & var_bucket_len,
                                    float & std_bucket_len
                                    ) const;

    lil_global_table*            get_lil_global_table();

    /**
     * \brief Returns the lock granted to this transaction for the given lock ID.
     * @param[in] lock_id identifier of the lock
     * @return the lock mode this transaction has for the lock. ALL_N_GAP_N if not any.
     * \details
     * This method returns very quickly because it only checks transaction-private data.
     * @pre the current thread is the only thread running the current transaction
     */
    okvl_mode                   get_granted_mode(const lockid_t& lock_id);
    okvl_mode                   get_granted_mode(uint32_t hash);

    /**
     * \brief Acquires a lock of the given mode (or stronger)
     * @copydoc RawLockQueue::acquire()
     */
    rc_t                        lock(const lockid_t &n, const okvl_mode &m, bool conditional,
        bool check_only, timeout_in_ms timeout = WAIT_SPECIFIED_BY_XCT, RawLock** out = NULL);
    rc_t                        lock(uint32_t hash, const okvl_mode &m, bool conditional,
        bool check_only, timeout_in_ms timeout = WAIT_SPECIFIED_BY_XCT, RawLock** out = NULL);

    // Special lock function used to re-acquire non-read locks from Restart Log Analysis phase
    // the transaction object is given
    rc_t                        lock(uint32_t hash, const okvl_mode &m, bool check_only, xct_t* xd,
       timeout_in_ms timeout = WAIT_SPECIFIED_BY_XCT);

    /** @copydoc RawLockQueue::retry_acquire() */
    rc_t                        retry_lock(RawLock** lock, bool check_only,
                                           timeout_in_ms timeout = WAIT_SPECIFIED_BY_XCT);

    /**
     * Take an intent lock on the given store.
     */
    rc_t                        intent_store_lock(StoreID stid, okvl_mode::element_lock_mode m);

    void                        unlock(RawLock* lock, lsn_t commit_lsn = lsn_t::null);

    rc_t                        unlock_duration(bool read_lock_only = false, lsn_t commit_lsn = lsn_t::null);

    void                        give_permission_to_violate(lsn_t commit_lsn = lsn_t::null);

    static void                 lock_stats(
        u_long&                      locks,
        u_long&                      acquires,
        u_long&                      cache_hits,
        u_long&                      unlocks,
        bool                         reset);

    RawXct*     allocate_xct();
    void        deallocate_xct(RawXct* xct);

private:
    timeout_in_ms               _convert_timeout(timeout_in_ms timeout);
    timeout_in_ms               _convert_timeout(timeout_in_ms timeout, xct_t* xd);
    lock_core_m*                core() const { return _core; }

    lock_core_m*                _core;
};

#endif // LOCK_H
