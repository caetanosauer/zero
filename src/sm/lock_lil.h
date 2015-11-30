/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef LOCK_LIL_H
#define LOCK_LIL_H

/**
 * \defgroup LIL
 * \brief \b Light-weight \b Intent \b Lock (\b LIL)
 * \ingroup SSMLOCK
 * \details
 * Classes implementing Light-weight Intent Lock (LIL).
 * These are super-fast and scalable lock tables for
 * volume/store.
 * For more details, see jira ticket:94 "Lightweight Intent Lock (LIL)" (originally trac ticket:96).
 */

#include "w_defines.h"

#include <cstring> // for memset
#include "w_rc.h"
#include "srwlock.h"
#include "sthread.h"
#include "stnode_page.h" // only for stnode_page::max
#include "vol.h"

/** max number of volumes overall. */
const uint16_t MAX_VOL_GLOBAL = 1;

/** max number of volumes one transaction can access at a time. */
const uint16_t MAX_VOL_PER_XCT = 4;

/** max number of stores per volume one transaction can access at a time. */
const uint16_t MAX_STORE_PER_VOL_XCT = 16;

enum lil_lock_modes_t {
    LIL_IS = 0,
    LIL_IX = 1,
    LIL_S = 2,
    LIL_X = 3,
    LIL_MODES = 4
};

// All objects here are okay to initialize by memset(0).

/**
 * \brief LIL global lock table to protect Volume/Store from concurrent accesses.
 * \ingroup LIL
 * \details
 * This class provides a super-fast yet non-starving
 * locks for classes that mainly have intent locks (IS/IX).
 * As far as absolute locks (S/X) are rare, this performs
 * way faster than usual lock tables which uses mutex and
 * forms lock-chains to do inter-thread communications.
 * This class only uses spinlocks, counters and sleeps.
 * For more details, see jira ticket:94 "Lightweight Intent Lock (LIL)" (originally trac ticket:96).
 */
class lil_global_table_base {
public:
    uint16_t  _IS_count;  // +2 -> 2
    uint16_t  _IX_count;  // +2 -> 4
    uint16_t  _S_count;   // +2 -> 6
    bool      _X_taken;   // +1 -> 7
    bool      _dummy1;    // +1 -> 8
    uint16_t  _waiting_S; // +2 -> 10
    uint16_t  _waiting_X; // +2 -> 12
    uint32_t            _release_version; // +4 -> 16
    lsn_t               _x_lock_tag; // +8 -> 24. this is for Safe SX-ELR
    pthread_mutex_t     _waiter_mutex;
    pthread_cond_t      _waiter_cond;

    /** all operations in this object are protected by this spin lock. */
    // queue_based_lock_t _spin_lock;
    // srwlock_t _spin_lock;
    tatas_lock _spin_lock;
    // mmm, scalability and overhead is the trade-off here.

    /**
     * Requests the given mode in the lock table.
     * @param[in] mode the lock mode to acquire
     */
    w_rc_t      request_lock(lil_lock_modes_t mode);

    /**
     * Decreases the corresponding counters. This never sees an error or long blocking.
     * @param[in] read_lock_only if true, releases only read locks. default false.
     */
    void        release_locks(bool *lock_taken, bool read_lock_only = false, lsn_t commit_lsn = lsn_t::null);

private:
    w_rc_t      _request_lock_IS(lsn_t &observed_tag);
    w_rc_t      _request_lock_IX(lsn_t &observed_tag);
    w_rc_t      _request_lock_S(lsn_t &observed_tag);
    w_rc_t      _request_lock_X(lsn_t &observed_tag);
    /** @return whether timeout happened .*/
    bool        _cond_timedwait (uint32_t base_version, uint32_t timeout_microsec);
};

/**
 * lock table for store.
 * \ingroup LIL
 */
class lil_global_store_table : public lil_global_table_base {
public:
    lil_global_store_table() {}
    ~lil_global_store_table() {}
};

/**
 * lock table for volume. also contains lock tables for stores in it.
 * \ingroup LIL
 */
class lil_global_vol_table : public lil_global_table_base {
public:
    lil_global_store_table _store_tables[stnode_page::max]; // for all possible stores

    lil_global_vol_table() {
        ::memset (this, 0, sizeof(*this));
        ::pthread_mutex_init(&_waiter_mutex, NULL);
        ::pthread_cond_init(&_waiter_cond, NULL);
        for (size_t i = 0; i < stnode_page::max; ++i) {
            ::pthread_mutex_init(&(_store_tables[i]._waiter_mutex), NULL);
            ::pthread_cond_init(&(_store_tables[i]._waiter_cond), NULL);
        }
    }
    ~lil_global_vol_table(){
        ::pthread_mutex_destroy(&_waiter_mutex);
        ::pthread_cond_destroy(&_waiter_cond);
        for (size_t i = 0; i < stnode_page::max; ++i) {
            ::pthread_mutex_destroy(&(_store_tables[i]._waiter_mutex));
            ::pthread_cond_destroy(&(_store_tables[i]._waiter_cond));
        }
    }
};

/**
 * For all volumes. This object is retained in lock manager.
 * \ingroup LIL
 */
class lil_global_table {
public:
    lil_global_vol_table _vol_tables[MAX_VOL_GLOBAL+1];

    lil_global_table() {
        clear();
    }
    ~lil_global_table(){}
    void clear() {
        ::memset (this, 0, sizeof(*this));
    }
};

/**
 * \brief LIL private lock table to remember Store locks taken by current xct.
 * \ingroup LIL
 */
class lil_private_store_table {
public:
    uint32_t    _store;    // +4 -> 4. zero if this table is not used yet.
    bool        _lock_taken[LIL_MODES]; // +4 -> 8

    lil_private_store_table() {
        clear();
    }
    ~lil_private_store_table(){}
    void clear() {
        ::memset (this, 0, sizeof(*this));
    }
};

/**
 * \brief LIL private lock table to remember Volume locks and Stores in it.
 * \ingroup LIL
 */
class lil_private_vol_table {
public:
    uint16_t    _vid;      // +2 -> 2. zero if this table is not used yet.
    uint16_t    _stores;   // +2 -> 4. number of stores used in _store_tables
    bool        _lock_taken[LIL_MODES]; // +4 -> 8

    lil_private_store_table _store_tables[MAX_STORE_PER_VOL_XCT]; // 8 * MAX_STORE_PER_VOL_XCT

    lil_private_vol_table() {
        clear();
    }
    ~lil_private_vol_table(){}
    void clear() {
        ::memset (this, 0, sizeof(*this));
    }

    /**
     * Get a lock on store.
     * @param[in] global_table accesses this global table to acuiqre lock
     * @param[in] stid ID of the store to access
     * @param[in] mode lock mode
     */
    w_rc_t acquire_store_lock(lil_global_table *global_table, const stid_t &stid,
            lil_lock_modes_t mode);

    /**
     * Release all locks acquired for this volume. This never fails or takes long time.
     * @param[in] read_lock_only if true, releases only read locks. default false.
     */
    void   release_vol_locks(lil_global_table *global_table, bool read_lock_only = false, lsn_t commit_lsn = lsn_t::null);
private:
    lil_private_store_table* _find_store_table(uint32_t store);
};

/**
 * \brief LIL private lock table to remember all locks in xct.
 * \ingroup LIL
 */
class lil_private_table {
public:
    uint16_t    _volumes;   // +2 -> 2. number of volumes used in _vol_tables
    uint16_t    _unused1;   // +2 -> 4
    uint32_t    _unused2;   // +4 -> 8

    lil_private_vol_table _vol_tables[MAX_VOL_PER_XCT];

    lil_private_table() {
        clear();
    }
    ~lil_private_table(){}

    void clear() {
        ::memset (this, 0, sizeof(*this));
    }

    /**
     * Get a lock on volume and return the private lock table for it.
     * @param[in] global_table accesses this global table to acuiqre lock
     * @param[in] vid ID of the volume to access
     * @param[in] mode lock mode
     * @param[out] table private lock table for the volume
     */
    w_rc_t acquire_vol_table(lil_global_table *global_table, uint16_t vid,
            lil_lock_modes_t mode, lil_private_vol_table* &table);

    /**
     * Returns a volume lock table assuming
     */
    w_rc_t get_vol_table_nolock(lil_global_table *global_table, uint16_t vid,
            lil_private_vol_table* &table);

    /**
     * Shortcut method to acquire store and its volume lock in the same mode.
     */
    w_rc_t acquire_vol_store_lock(lil_global_table *global_table, const stid_t &stid,
            lil_lock_modes_t mode);

    /**
     * Release all locks acquired by the current transaction and resets the private table.
     * This never fails or takes long time.
     * @param[in] read_lock_only if true, releases only read locks. default false.
     */
    void   release_all_locks(lil_global_table *global_table, bool read_lock_only = false, lsn_t commit_lsn = lsn_t::null);

    /**
     * Returns a volume lock table for the given volume id.
     */
    lil_private_vol_table* find_vol_table(uint16_t vid);
};

#endif // LOCK_LIL_H
