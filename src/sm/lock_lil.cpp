#define SM_SOURCE

#ifdef __GNUG__
#pragma implementation "lock_lil.h"
#endif

#include "sm_int_2.h"
#include "lock_lil.h"


/**
 * millisec to sleep after each failed lock acquisition for intent locks.
 * okay to be large like 100ms because absolute locks are rare.
 */
const int INTENT_LOCK_SLEEP_MS = 10;
const int INTENT_LOCK_MAX_TRIES = 10000;

/**
 * Absolute lock requests are waken up with mutex.
 * However, only one waiter is waken up at each time to simplify lil_global_table_base.
 * So, it still has timeout to periodically re-check the request for unlucky case.
 */
const int ABSOLUTE_LOCK_SLEEP_MS = 10;
const int ABSOLUTE_LOCK_MAX_TRIES = 10000;

w_rc_t lil_global_table_base::request_lock(lil_lock_modes_t mode, bool immediate)
{
    switch (mode) {
        case LIL_IS: return _request_lock_IS(immediate);
        case LIL_IX: return _request_lock_IX(immediate);
        case LIL_S: return _request_lock_S(immediate);
        case LIL_X: return _request_lock_X(immediate);
        default: w_assert1(false); //wtf?
    }
    return RCOK;
}

void lil_global_table_base::release_locks(bool *lock_taken, bool read_lock_only)
{
    CRITICAL_SECTION(cs, _spin_lock);
    // spinlock_write_critical_section cs (&_spin_lock);
    // tataslock_critical_section cs (&_spin_lock);
    if (lock_taken[LIL_IS]) {
        w_assert1(_IS_count > 0);
        --_IS_count;
    }
    if (lock_taken[LIL_IX] && !read_lock_only) {
        w_assert1(_IX_count > 0);
        --_IX_count;
    }
    if (lock_taken[LIL_S]) {
        w_assert1(_S_count > 0);
        --_S_count;
    }
    if (lock_taken[LIL_X] && !read_lock_only) {
        w_assert1(_X_taken);
        _X_taken = false;
    }
    // if absolute lock is requested and can be granted, wake them up
    // this is rare event, so using mutex here doesn't have too much overhead
    if (_IX_count == 0 && !_X_taken && _waiting_S && _S_waiter) {
        _S_waiter->smthread_unblock(smlevel_0::eOK);
    }
    if (_IS_count == 0 && _IX_count == 0 && _S_count == 0 && !_X_taken && _waiting_X && _X_waiter) {
        _X_waiter->smthread_unblock(smlevel_0::eOK);
    }
}

w_rc_t lil_global_table_base::_request_lock_IS(bool immediate)
{
    for (int i = 0; i < INTENT_LOCK_MAX_TRIES; ++i) {
        {
            CRITICAL_SECTION(cs, _spin_lock);
            // spinlock_write_critical_section cs (&_spin_lock);
            // tataslock_critical_section cs (&_spin_lock);
            if (_IS_count < 65535) { // overflow check. shouldn't happen though
                if (_waiting_X) {
                    // there is waiting X requests. let's give a way to him
                } else {
                    if (!_X_taken) {
                        ++_IS_count;
                        return RCOK;
                    }
                }
            }
        }
        // we are here because we failed to get locks immediately
        if (immediate) {
            return RC(smlevel_0::eLOCKTIMEOUT);
        }
        ::usleep (INTENT_LOCK_SLEEP_MS * 1000);
    }
    return RC(smlevel_0::eLOCKTIMEOUT); // give up
}

w_rc_t lil_global_table_base::_request_lock_IX(bool immediate)
{
    for (int i = 0; i < INTENT_LOCK_MAX_TRIES; ++i) {
        {
            CRITICAL_SECTION(cs, _spin_lock);
            // spinlock_write_critical_section cs (&_spin_lock);
            // tataslock_critical_section cs (&_spin_lock);
            if (_IX_count < 65535) {
                if (_waiting_X || _waiting_S) {
                    // let's give a way to absolute locks
                } else {
                    if (!_X_taken && _S_count == 0) {
                        ++_IX_count;
                        return RCOK;
                    }
                }
            }
        }
        if (immediate) {
            return RC(smlevel_0::eLOCKTIMEOUT);
        }
        ::usleep (INTENT_LOCK_SLEEP_MS * 1000);
    }
    return RC(smlevel_0::eLOCKTIMEOUT);
}

w_rc_t lil_global_table_base::_request_lock_S(bool immediate)
{
    for (int i = 0; i < ABSOLUTE_LOCK_MAX_TRIES; ++i) {
        {
            CRITICAL_SECTION(cs, _spin_lock);
            // spinlock_write_critical_section cs (&_spin_lock);
            // tataslock_critical_section cs (&_spin_lock);
            if (_S_count < 255) {
                _waiting_S = true; // this avoids starvation. we set it each time
                _S_waiter = g_me();
                if (_waiting_X) {
                    // let's allow X first.
                } else {
                    if (!_X_taken && _IX_count == 0) {
                        ++_S_count;
                        _waiting_S = false; // okay even if there is another S request.
                        _S_waiter = NULL;
                        return RCOK;
                    }
                }
            }
        }
        if (immediate) {
            return RC(smlevel_0::eLOCKTIMEOUT);
        }
        // even if my S_waiter is overwritten, I will wake up after some time, so it's fine
        g_me()->smthread_block(ABSOLUTE_LOCK_SLEEP_MS, 0);
    }
    return RC(smlevel_0::eLOCKTIMEOUT); // give up
}
w_rc_t lil_global_table_base::_request_lock_X(bool immediate)
{
    for (int i = 0; i < ABSOLUTE_LOCK_MAX_TRIES; ++i) {
        {
            CRITICAL_SECTION(cs, _spin_lock);
            // spinlock_write_critical_section cs (&_spin_lock);
            // tataslock_critical_section cs (&_spin_lock);
            _waiting_X = true;
            _X_waiter = g_me();
            if (!_X_taken && _S_count == 0 && _IX_count == 0 && _IS_count == 0) {
                _X_taken = true;
                _waiting_X = false;
                _X_waiter = NULL;
                return RCOK;
            }
        }
        if (immediate) {
            return RC(smlevel_0::eLOCKTIMEOUT);
        }
        
        // even if my X_waiter is overwritten, I will wake up after some time, so it's fine
        g_me()->smthread_block(ABSOLUTE_LOCK_SLEEP_MS, 0);
    }
    return RC(smlevel_0::eLOCKTIMEOUT); // give up
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

w_rc_t lil_private_vol_table::acquire_store_lock(lil_global_table *global_table, const stid_t &stid,
        lil_lock_modes_t mode) {
    w_assert1(global_table);
    snum_t store = stid.store;
    lil_private_store_table* table = _find_store_table(store);
    if (table == NULL) {
        return RC(smlevel_0::eLIL_TOOMANYST_XCT);
    }
    
    if (does_already_own(mode, table->_lock_taken)) {
        return RCOK;
    }
    
    // then, we need to request a lock to global table
    w_assert1(stid.vol < MAX_VOL_GLOBAL);
    bool immediate = has_any_lock(table->_lock_taken);
    // if it's timeout, it's deadlock
    rc_t rc = global_table->_vol_tables[stid.vol]._store_tables[store].request_lock(mode, immediate);
    if (rc.is_error()) {
        // this might be a bit too conservative, but doesn't matter for intent locks
        if (rc.err_num() == smlevel_0::eLOCKTIMEOUT) {
            return RC (smlevel_0::eDEADLOCK);
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

void lil_private_vol_table::release_vol_locks(lil_global_table *global_table, bool read_lock_only)
{
    w_assert1(_vid);
    // release the volume lock
    if (has_any_lock(_lock_taken, read_lock_only)) {
        global_table->_vol_tables[_vid].release_locks(_lock_taken, read_lock_only);
        clear_lock_flags (_lock_taken, read_lock_only);
    }
    // release store locks under this
    for (uint16_t i = 0; i < _stores; ++i) {
        snum_t store = _store_tables[i]._store;
        w_assert1(store);
        if (has_any_lock(_store_tables[i]._lock_taken, read_lock_only)) {
            global_table->_vol_tables[_vid]._store_tables[store].release_locks(_store_tables[i]._lock_taken, read_lock_only);
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
    if (_stores < MAX_VOL_PER_XCT) {
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
    table = find_vol_table(vid);
    if (table == NULL) {
        return RC(smlevel_0::eLIL_TOOMANYVOL_XCT);
    }
    
    if (does_already_own(mode, table->_lock_taken)) {
        return RCOK;
    }
    
    // then, we need to request a lock to global table
    w_assert1(vid < MAX_VOL_GLOBAL);
    bool immediate = has_any_lock(table->_lock_taken);
    // if it's timeout, it's deadlock
    rc_t rc = global_table->_vol_tables[vid].request_lock(mode, immediate);
    if (rc.is_error()) {
        if (rc.err_num() == smlevel_0::eLOCKTIMEOUT) {
            return RC (smlevel_0::eDEADLOCK);
        } else {
            return rc;
        }
    }
    w_assert1(!table->_lock_taken[mode]);
    table->_lock_taken[mode] = true;
    return RCOK;
}
w_rc_t lil_private_table::acquire_vol_store_lock(lil_global_table *global_table, const stid_t &stid,
        lil_lock_modes_t mode)
{
    lil_private_vol_table* vol_table;
    W_DO (acquire_vol_table(global_table, stid.vol, mode, vol_table));
    W_DO (vol_table->acquire_store_lock(global_table, stid, mode));
    return RCOK;
}

void lil_private_table::release_all_locks(lil_global_table *global_table, bool read_lock_only)
{
    for (uint16_t i = 0; i < _volumes; ++i) {
        _vol_tables[i].release_vol_locks(global_table, read_lock_only);
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
