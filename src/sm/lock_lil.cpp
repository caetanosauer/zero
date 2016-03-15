/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#define SM_SOURCE

#include "sm_base.h"
#include "lock_lil.h"
#include <sys/time.h>

/**
 * maximum time to wait after failed lock acquisition for intent locks.
 * if timeout happens, the xct is rolled back to prevent deadlocks.
 */
const int INTENT_LOCK_TIMEOUT_MICROSEC = 10000;

/**
 * For absolute lock requests.
 * Longer than intent locks to avoid starvation of absolute lock requests.
 */
const int ABSOLUTE_LOCK_TIMEOUT_MICROSEC = 100000;

w_rc_t lil_global_table_base::request_lock(lil_lock_modes_t mode)
{
    lsn_t observed_tag;
    w_rc_t ret;
    switch (mode) {
        case LIL_IS: ret = _request_lock_IS(observed_tag); break;
        case LIL_IX: ret = _request_lock_IX(observed_tag); break;
        case LIL_S: ret = _request_lock_S(observed_tag); break;
        case LIL_X: ret =  _request_lock_X(observed_tag); break;
        default: w_assert1(false); //wtf?
    }
    g_xct()->update_read_watermark(observed_tag);
    return ret;
}

void lil_global_table_base::release_locks(bool *lock_taken, bool read_lock_only, lsn_t commit_lsn)
{
    bool broadcast = false;
    {
        tataslock_critical_section cs (&_spin_lock);
        // CRITICAL_SECTION(cs, _spin_lock);
        ++_release_version; // to let waiting threads that something really happened
        if (lock_taken[LIL_IS]) {
            w_assert1(_IS_count > 0);
            --_IS_count;
            if (_IS_count == 0 && _waiting_X != 0) {
                broadcast = true;
            }
        }
        if (lock_taken[LIL_IX] && !read_lock_only) {
            w_assert1(_IX_count > 0);
            --_IX_count;
            if (_IX_count == 0 && (_waiting_S != 0 || _waiting_X != 0)) {
                broadcast = true;
            }
        }
        if (lock_taken[LIL_S]) {
            w_assert1(_S_count > 0);
            --_S_count;
            broadcast = true;
        }
        if (lock_taken[LIL_X] && !read_lock_only) {
            w_assert1(_X_taken);
            _X_taken = false;
            broadcast = true;
            // only when we release X lock, we update the tag for safe SX-ELR.
            // IX doesn't matter because the lower level will do the job.
            if (commit_lsn.valid() && commit_lsn > _x_lock_tag) {
                DBGOUT1(<<"LIL: tag for Safe SX-ELR updated to " << commit_lsn);
                _x_lock_tag = commit_lsn;
            }
        }
    }
    if (broadcast) {
        int rc_mutex_lock = ::pthread_mutex_lock (&_waiter_mutex);
        w_assert1(rc_mutex_lock == 0);

        int rc_broadcast = ::pthread_cond_broadcast(&_waiter_cond);
        w_assert1(rc_broadcast == 0);

        int rc_mutex_unlock = ::pthread_mutex_unlock (&_waiter_mutex);
        w_assert1(rc_mutex_unlock == 0);
    }
}

const clockid_t CLOCK_FOR_LIL = CLOCK_REALTIME; // CLOCK_MONOTONIC;

bool lil_global_table_base::_cond_timedwait (uint32_t base_version, uint32_t timeout_microsec) {
    int rc_mutex_lock = ::pthread_mutex_lock (&_waiter_mutex);
    w_assert1(rc_mutex_lock == 0);

    timespec   ts;
    ::clock_gettime(CLOCK_FOR_LIL, &ts);
    ts.tv_nsec += (uint64_t) timeout_microsec * 1000;
    ts.tv_sec += ts.tv_nsec / 1000000000;
    ts.tv_nsec = ts.tv_nsec % 1000000000;

#if W_DEBUG_LEVEL>=2
    timespec   ts_init;
    ::clock_gettime(CLOCK_FOR_LIL, &ts_init);
    cout << "LIL: waiting for " << timeout_microsec << " us.." << endl;
#endif // W_DEBUG_LEVEL>=2


    bool timeouted = false;
    while (true) {
        uint32_t version;
        {
            CRITICAL_SECTION(cs, _spin_lock);
            version = _release_version;
        }
        if (version != base_version) {
            break;
        }

        int rc_wait =
            //::pthread_cond_wait(&_waiter_cond, &_waiter_mutex);
            ::pthread_cond_timedwait(&_waiter_cond, &_waiter_mutex, &ts);

#if W_DEBUG_LEVEL>=2
    timespec   ts_end;
    ::clock_gettime(CLOCK_FOR_LIL, &ts_end);
    double ts_diff = (ts_end.tv_sec - ts_init.tv_sec) * 1000000 + (ts_end.tv_nsec - ts_init.tv_nsec) / 1000.0;
    cout << "LIL: waked up after " << ts_diff << " usec. wait-ret="
        << rc_wait << (rc_wait == ETIMEDOUT ? "(ETIMEDOUT)" : "") << endl;
#endif // W_DEBUG_LEVEL>=2

        if (rc_wait == ETIMEDOUT) {
#if W_DEBUG_LEVEL>=2
            timespec   ts_now;
            ::clock_gettime(CLOCK_FOR_LIL, &ts_now);
            if (ts_now.tv_sec > ts.tv_sec || (ts_now.tv_sec == ts.tv_sec && ts_now.tv_nsec >= ts.tv_nsec)) {
            } else {
                cout << "Seems a fake ETIMEDOUT?? Trying again" << endl;
                // ah, CLOCK_MONOTONIC caused this. CLOCK_REALTIME doesn't have this issue.
            }
#endif // W_DEBUG_LEVEL>=2
            timeouted = true;
            break;
        }
    }

    int rc_mutex_unlock = ::pthread_mutex_unlock (&_waiter_mutex);
    w_assert1(rc_mutex_unlock == 0);

    return timeouted;
}

w_rc_t lil_global_table_base::_request_lock_IS(lsn_t &observed_tag)
{
    while (true) {
        uint32_t version;
        {
            // CRITICAL_SECTION(cs, _spin_lock);
            // spinlock_write_critical_section cs (&_spin_lock);
            tataslock_critical_section cs (&_spin_lock);
            if (_IS_count < 65535) { // overflow check. shouldn't happen though
                if (_waiting_X != 0) {
                    // there is waiting X requests. let's give a way to him
                } else {
                    if (!_X_taken) {
                        ++_IS_count;
                        observed_tag = _x_lock_tag;
                        return RCOK;
                    }
                }
            }
            version = _release_version;
        }
        bool timeouted = _cond_timedwait (version, INTENT_LOCK_TIMEOUT_MICROSEC);
        if (timeouted) {
            break;
        }
    }
    return RC(eLOCKTIMEOUT); // give up
}

w_rc_t lil_global_table_base::_request_lock_IX(lsn_t &observed_tag)
{
    while (true) {
        uint32_t version;
        {
            // CRITICAL_SECTION(cs, _spin_lock);
            // spinlock_write_critical_section cs (&_spin_lock);
            tataslock_critical_section cs (&_spin_lock);
            if (_IX_count < 65535) {
                if (_waiting_X != 0 || _waiting_S != 0) {
                    // let's give a way to absolute locks
                } else {
                    if (!_X_taken && _S_count == 0) {
                        ++_IX_count;
                        observed_tag = _x_lock_tag;
                        return RCOK;
                    }
                }
            }
            version = _release_version;
        }
        bool timeouted = _cond_timedwait (version, INTENT_LOCK_TIMEOUT_MICROSEC);
        if (timeouted) {
            break;
        }
    }
    return RC(eLOCKTIMEOUT);
}

w_rc_t lil_global_table_base::_request_lock_S(lsn_t &observed_tag)
{
    bool set_waiting = false;
    while (true) {
        uint32_t version;
        {
            // CRITICAL_SECTION(cs, _spin_lock);
            // spinlock_write_critical_section cs (&_spin_lock);
            tataslock_critical_section cs (&_spin_lock);
            if (!set_waiting) {
                ++_waiting_S;
                set_waiting = true;
            }
            if (_S_count < 65535) {
                if (_waiting_X != 0) {
                    // let's allow X first.
                } else {
                    if (!_X_taken && _IX_count == 0) {
                        ++_S_count;
                        --_waiting_S;
                        observed_tag = _x_lock_tag;
                        return RCOK;
                    }
                }
            }
            version = _release_version;
        }
        bool timeouted = _cond_timedwait (version, ABSOLUTE_LOCK_TIMEOUT_MICROSEC);
        if (timeouted) {
            break;
        }
    }
    return RC(eLOCKTIMEOUT); // give up
}
w_rc_t lil_global_table_base::_request_lock_X(lsn_t &observed_tag)
{
    bool set_waiting = false;
    while (true) {
        uint32_t version;
        {
            // CRITICAL_SECTION(cs, _spin_lock);
            // spinlock_write_critical_section cs (&_spin_lock);
            tataslock_critical_section cs (&_spin_lock);
            if (!set_waiting) {
                ++_waiting_X;
                set_waiting = true;
            }
            if (!_X_taken && _S_count == 0 && _IX_count == 0 && _IS_count == 0) {
                _X_taken = true;
                --_waiting_X;
                observed_tag = _x_lock_tag;
                return RCOK;
            }
            version = _release_version;
        }

        bool timeouted = _cond_timedwait (version, ABSOLUTE_LOCK_TIMEOUT_MICROSEC);
        if (timeouted) {
            break;
        }
    }
    return RC(eLOCKTIMEOUT); // give up
}

/** do we already have a desired lock? */
bool does_already_own (lil_lock_modes_t mode, const bool *lock_taken) {
    switch (mode) {
        case LIL_IS: return (lock_taken[LIL_IS] || lock_taken[LIL_IX] || lock_taken[LIL_S] || lock_taken[LIL_X]);
        case LIL_IX: return (lock_taken[LIL_IX] || lock_taken[LIL_X]);
        case LIL_S: return (lock_taken[LIL_S] || lock_taken[LIL_X]);
        case LIL_X: return (lock_taken[LIL_X]);
        default: w_assert1(false); //wtf?
    }
    return false;
}

/**
 * do we already have some lock?
 * if we already have some intent lock and trying to upgrade it,
 * encountering a blocking means that there is a chance of deadlock.
 * thus, we should immediately give up although this might be too conservative.
 * anways, it's a rare case.
 */
inline bool has_any_lock(const bool *lock_taken, bool read_lock_only = false) {
    if (read_lock_only) {
        return (lock_taken[LIL_IS] || lock_taken[LIL_S]);
    } else {
        return (lock_taken[LIL_IS] || lock_taken[LIL_IX] || lock_taken[LIL_S] || lock_taken[LIL_X]);
    }
}

w_rc_t lil_private_vol_table::acquire_store_lock(lil_global_table *global_table, const StoreID &stid,
        lil_lock_modes_t mode) {
    w_assert1(global_table);
    lil_private_store_table* table = _find_store_table(stid);
    if (table == NULL) {
        return RC(eLIL_TOOMANYST_XCT);
    }

    if (does_already_own(mode, table->_lock_taken)) {
        return RCOK;
    }

    // then, we need to request a lock to global table
    // if it's timeout, it's deadlock
    // CS TODO remove vid from lock manager
    rc_t rc = global_table->_vol_tables[1]._store_tables[stid].request_lock(mode);
    if (rc.is_error()) {
        // this might be a bit too conservative, but doesn't matter for intent locks
        if (rc.err_num() == eLOCKTIMEOUT) {
            return RC (eDEADLOCK);
        } else {
            return rc;
        }
    }
    w_assert1(!table->_lock_taken[mode]);
    table->_lock_taken[mode] = true;
    return RCOK;
}

inline void clear_lock_flags(bool *lock_taken, bool read_lock_only) {
    if (read_lock_only) {
        lock_taken[LIL_IS] = lock_taken[LIL_S] = false;
    } else {
        lock_taken[LIL_IS] = lock_taken[LIL_IX] = lock_taken[LIL_S] = lock_taken[LIL_X] = false;
    }
}

void lil_private_vol_table::release_vol_locks(lil_global_table *global_table, bool read_lock_only, lsn_t commit_lsn)
{
    w_assert1(_vid);
    // release the volume lock
    if (has_any_lock(_lock_taken, read_lock_only)) {
        global_table->_vol_tables[_vid].release_locks(_lock_taken, read_lock_only, commit_lsn);
        clear_lock_flags (_lock_taken, read_lock_only);
    }
    // release store locks under this
    for (uint16_t i = 0; i < _stores; ++i) {
        StoreID store = _store_tables[i]._store;
        w_assert1(store);
        if (has_any_lock(_store_tables[i]._lock_taken, read_lock_only)) {
            global_table->_vol_tables[_vid]._store_tables[store].release_locks(_store_tables[i]._lock_taken, read_lock_only, commit_lsn);
            clear_lock_flags (_store_tables[i]._lock_taken, read_lock_only);
        }
    }
}

lil_private_store_table* lil_private_vol_table::_find_store_table(uint32_t store)
{
    for (uint16_t i = 0; i < _stores; ++i) {
        if (_store_tables[i]._store == store) {
            return &_store_tables[i];
        }
    }

    //newly add the volume.
    if (_stores < MAX_STORE_PER_VOL_XCT) {
        _store_tables[_stores]._store = store;
        lil_private_store_table *ret = &_store_tables[_stores];
        ++_stores;
        return ret;
    } else {
        return NULL; // we have too many
    }
}

w_rc_t lil_private_table::acquire_vol_table(lil_global_table *global_table,
    uint16_t vid, lil_lock_modes_t mode, lil_private_vol_table* &table)
{
    w_assert1(global_table);
    // CS TODO remove vid from lock manager
    table = find_vol_table(1);
    if (table == NULL) {
        return RC(eLIL_TOOMANYVOL_XCT);
    }

    if (does_already_own(mode, table->_lock_taken)) {
        return RCOK;
    }

    // then, we need to request a lock to global table
    w_assert1(vid <= MAX_VOL_GLOBAL);
    // if it's timeout, it's deadlock
    rc_t rc = global_table->_vol_tables[vid].request_lock(mode);
    if (rc.is_error()) {
        if (rc.err_num() == eLOCKTIMEOUT) {
            return RC (eDEADLOCK);
        } else {
            return rc;
        }
    }
    w_assert1(!table->_lock_taken[mode]);
    table->_lock_taken[mode] = true;
    return RCOK;
}
w_rc_t lil_private_table::acquire_vol_store_lock(lil_global_table *global_table, const StoreID &stid,
        lil_lock_modes_t mode)
{
    lil_private_vol_table* vol_table;
    // CS TODO remove vid from lock manager
    W_DO (acquire_vol_table(global_table, 1, mode, vol_table));
    W_DO (vol_table->acquire_store_lock(global_table, stid, mode));
    return RCOK;
}

void lil_private_table::release_all_locks(lil_global_table *global_table, bool read_lock_only, lsn_t commit_lsn)
{
    for (uint16_t i = 0; i < _volumes; ++i) {
        _vol_tables[i].release_vol_locks(global_table, read_lock_only, commit_lsn);
    }
    if (!read_lock_only) {
        clear();
    }
}

lil_private_vol_table* lil_private_table::find_vol_table(uint16_t vid)
{
    for (uint16_t i = 0; i < _volumes; ++i) {
        if (_vol_tables[i]._vid == vid) {
            return &_vol_tables[i];
        }
    }

    //newly add the volume.
    if (_volumes < MAX_VOL_PER_XCT) {
        _vol_tables[_volumes]._vid = vid;
        lil_private_vol_table *ret = &_vol_tables[_volumes];
        ++_volumes;
        return ret;
    } else {
        return NULL; // we have too many
    }
}
