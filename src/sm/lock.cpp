/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define LOCK_C

#include "sm_base.h"
#include "lock_x.h"
#include "lock_core.h"
#include "lock_lil.h"
#include "xct.h"
#include "lock_s.h"
#include "lock_raw.h"
#include "w_okvl.h"
#include "w_okvl_inl.h"
#include <new>

lock_m::lock_m(const sm_options &options)
{
    _core = new lock_core_m(options);
    w_assert1(_core);
}


void
lock_m::assert_empty() const
{
    _core->assert_empty();
}

lock_m::~lock_m()
{
    assert_empty();
    delete _core;
}


extern "C" void lock_dump_locks();
void lock_dump_locks() {
    smlevel_0::lm->dump(cerr);
    cerr << flushl;
}

void lock_m::dump(ostream &o)
{
    o << "LOCKS: { " << endl;
    _core->dump(o);
    o << "} " << endl;
}

lil_global_table* lock_m::get_lil_global_table() {
    return _core->get_lil_global_table();
}


okvl_mode lock_m::get_granted_mode(const lockid_t& lock_id) {
    return get_granted_mode(lock_id.hash());
}
okvl_mode lock_m::get_granted_mode(uint32_t hash) {
    xct_t* xd = g_xct();
    if (xd == NULL) {
        return ALL_N_GAP_N;
    }
    return xd->raw_lock_xct()->private_hash_map.get_granted_mode(hash);
}

timeout_in_ms lock_m::_convert_timeout(timeout_in_ms timeout) {
    xct_t*                 xd = g_xct();

    return _convert_timeout(timeout, xd);
}

timeout_in_ms lock_m::_convert_timeout(timeout_in_ms timeout, xct_t* xd) {
    w_assert1(NULL != xd);

    switch (timeout) {
        case WAIT_SPECIFIED_BY_XCT:
            timeout = xd->timeout_c();
            break;
            // DROP THROUGH to WAIT_SPECIFIED_BY_THREAD ...
            // (whose default is WAIT_FOREVER)

        case WAIT_SPECIFIED_BY_THREAD:
            timeout = me()->lock_timeout();
            break;

        default:
            break;
    }

    w_assert9(timeout >= 0 || timeout == WAIT_FOREVER);
    return timeout;
}

rc_t lock_m::lock(const lockid_t &n, const okvl_mode &m, bool conditional, bool check_only,
                  timeout_in_ms timeout, RawLock** out) {
    return lock(n.hash(), m, conditional, check_only, timeout, out);
}

rc_t lock_m::lock(uint32_t hash, const okvl_mode &m, bool conditional, bool check_only,
                  timeout_in_ms timeout, RawLock** out) {
    xct_t*                 xd = g_xct();
    if (xd == NULL) {
        return RCOK;
    }

    w_assert1(!conditional || out != NULL);
    RawLock *tmp = NULL;
    if (out == NULL) {
        out = &tmp;
    }

    // First, check the transaction-private hashmap to see if we already have the lock.
    // This is quick because this involves no critical section.
    if (m.is_implied_by(get_granted_mode(hash))) {
        return RCOK;
    }

    timeout = _convert_timeout(timeout);

    w_rc_t                 rc; // == RCOK

    RawXct* xct = xd->raw_lock_xct();
    w_error_codes rce = _core->acquire_lock(xct, hash, m, conditional, check_only, timeout, out);
    if (rce) {
        rc = RC(rce);
    } else {
        // store the lock queue tag we observed. this is for Safe SX-ELR
        xd->update_read_watermark (xct->read_watermark);
    }
    return rc;
}

rc_t lock_m::lock(uint32_t hash, const okvl_mode &m, bool check_only, xct_t* xd,
                  timeout_in_ms timeout)
{
    // Acquire lock on the given transaction object
    // No conditional and no RawLock
    // This helper function is only used by lock re-acquisition from Log Analysis phase in restart

    w_assert1(NULL != xd);

    // First, check the transaction-private hashmap to see if we already have the lock.
    // This is quick because this involves no critical section.
    if (m.is_implied_by(xd->raw_lock_xct()->private_hash_map.get_granted_mode(hash)))
        return RCOK;

    timeout = _convert_timeout(timeout, xd);

    w_rc_t  rc; // == RCOK
    RawLock *tmp = NULL;

    RawXct* xct = xd->raw_lock_xct();
    w_error_codes rce = _core->acquire_lock(xct, hash, m, false /*conditional*/, check_only, timeout, &tmp);
    if (rce)
    {
        rc = RC(rce);
    }
    else
    {
        // store the lock queue tag we observed. this is for Safe SX-ELR
        xd->update_read_watermark (xct->read_watermark);
    }
    return rc;
}

rc_t lock_m::retry_lock(RawLock** lock, bool check_only, timeout_in_ms timeout) {
    w_assert1(lock != NULL && *lock != NULL);
    xct_t*                 xd = g_xct();
    timeout = _convert_timeout(timeout);
    RawXct* xct = xd->raw_lock_xct();
    w_rc_t                 rc; // == RCOK
    w_error_codes rce = _core->retry_acquire(lock, check_only, timeout);
    if (rce) {
        rc = RC(rce);
    } else {
        // store the lock queue tag we observed. this is for Safe SX-ELR
        xd->update_read_watermark (xct->read_watermark);
    }
    return rc;
}

void lock_m::unlock(RawLock* lock, lsn_t commit_lsn) {
    _core->release_lock(lock, commit_lsn);
}


lil_lock_modes_t to_lil_mode (okvl_mode::element_lock_mode m) {
    switch (m) {
        case okvl_mode::IS: return LIL_IS;
        case okvl_mode::IX: return LIL_IX;
        case okvl_mode::S: return LIL_S;
        case okvl_mode::X: return LIL_X;
        default:
            w_assert1(false); // shouldn't reach here!
    }
    return LIL_IS;// shouldn't reach here!
}

rc_t lock_m::intent_store_lock(StoreID stid, okvl_mode::element_lock_mode m)
{
    lil_lock_modes_t mode = to_lil_mode(m);
    xct_t *xd = xct();
    if (xd == NULL) {
        return RCOK;
    }
    lil_global_table *global_table = get_lil_global_table();
    lil_private_table* private_table = xd->lil_lock_info();
    // get volume lock table without requesting locks.
    // CS TODO eliminate volume ids from lock manager
    lil_private_vol_table *vol_table = private_table->find_vol_table(1);
    // only request store lock
    W_DO(vol_table->acquire_store_lock(global_table, stid, mode));
    return RCOK;
}

/*
 * Free all locks of a given duration
 *  release not just those whose
 *     duration matches, but all those which shorter duration also
 */
rc_t lock_m::unlock_duration(
    bool read_lock_only, lsn_t commit_lsn)
{
    xct_t*        xd = xct();
    w_rc_t        rc;        // == RCOK

    if (xd)  {
        // First, release intent locks on LIL
        lil_global_table *global_table = get_lil_global_table();
        lil_private_table *private_table = xd->lil_lock_info();
        private_table->release_all_locks(global_table, read_lock_only, commit_lsn);

        // then, release non-intent locks
        _core->release_duration(read_lock_only, commit_lsn);
    }
    return RCOK;
}

void lock_m::give_permission_to_violate(lsn_t commit_lsn) {
    xct_t* xd = xct();
    if (xd)  {
        xct_lock_info_t* theLockInfo = xd->lock_info();
        spinlock_write_critical_section cs(&theLockInfo->_shared_latch);
        theLockInfo->_permission_to_violate = true;
        theLockInfo->_commit_lsn = commit_lsn;
    }
}

RawXct* lock_m::allocate_xct() {
    return _core->allocate_xct();
}
void lock_m::deallocate_xct(RawXct* xct) {
    _core->deallocate_xct(xct);
}
