/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define LOCK_C

#include "sm_int_1.h"
#include "lock_x.h"
#include "lock_core.h"
#include "lock_lil.h"
#include <new>

lock_m::lock_m(int sz)
{
    _core = new lock_core_m(sz);
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

/*rc_t lock_m::query(
    const lockid_t&     n,
    lmode_t&            m,
    const tid_t&        tid)
{
    DBGTHRD(<<"lock_m::query for lock " << n);
    xct_t *        xd = xct();
    w_assert9(!implicit || tid != tid_t::null);

    INC_TSTAT(lock_query_cnt);
    m = NL;

    if (tid == tid_t::null) {
        lock_head_t* lock = _core->find_lock_head(n, false);//do not create
        if (lock) {
            // lock head mutex was acquired by find_lock_head
            m = lock->granted_mode;
            RELEASE_HEAD_MUTEX(lock); // acquired in find_lock_head
        }
        return RCOK;
    }
    w_assert2(xd);

    lock_request_t* req = 0;
    lock_head_t* lock = _core->find_lock_head(n, false); // do not create
    if (lock) {
        // lock head mutex was acquired by find_lock_head
        req = lock->find_lock_request(xd->lock_info());
    }
    if (req) {
        m = req->mode();
        RELEASE_HEAD_MUTEX(lock); // acquired in find_lock_head
        return RCOK;
    }

    if (lock)
        RELEASE_HEAD_MUTEX(lock); // acquired in find_lock_head
    return RCOK;
}*/

lil_global_table* lock_m::get_lil_global_table() {
    return _core->get_lil_global_table();
}


rc_t
lock_m::lock(
    const lockid_t&      n, 
    const w_okvl&        m,
    bool                 check_only,
    timeout_in_ms        timeout,
    w_okvl*             prev_mode,
    w_okvl*             prev_pgmode
    )
{
    w_okvl _prev_mode;
    w_okvl _prev_pgmode;

    rc_t rc = _lock(n, m, _prev_mode, _prev_pgmode, check_only, timeout);

    if (prev_mode != 0)
        *prev_mode = _prev_mode;
    if (prev_pgmode != 0)
        *prev_pgmode = _prev_pgmode;
    return rc;
}

rc_t
lock_m::_lock(
    const lockid_t&         n,
    const w_okvl&           m,
    w_okvl&                prev_mode,
    w_okvl&                prev_pgmode,
    bool                    check_only,
    timeout_in_ms           timeout
    )
{
    xct_t*                 xd = xct();
    if (xd == NULL) {
        return RCOK;
    }

    w_rc_t                 rc; // == RCOK
    prev_mode.clear();
    prev_pgmode.clear();

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

    w_rc_t::errcode_t rce = _core->acquire_lock(xd, n, m, prev_mode, check_only,  timeout);
    if (rce) {
        rc = RC(rce);
    }
    return rc;
}

lil_lock_modes_t to_lil_mode (w_okvl::singular_lock_mode m) {
    switch (m) {
        case w_okvl::IS: return LIL_IS;
        case w_okvl::IX: return LIL_IX;
        case w_okvl::S: return LIL_S;
        case w_okvl::X: return LIL_X;
        default:
            w_assert1(false); // shouldn't reach here!
    }
    return LIL_IS;// shouldn't reach here!
}

rc_t lock_m::intent_vol_lock(vid_t vid, w_okvl::singular_lock_mode m)
{
    lil_lock_modes_t mode = to_lil_mode(m);
    xct_t *xd = xct();
    if (xd == NULL) {
        return RCOK;
    }
    
    lil_global_table *global_table = get_lil_global_table();
    lil_private_table* private_table = xd->lil_lock_info();
    lil_private_vol_table *vol_table;
    W_DO(private_table->acquire_vol_table(global_table, vid.vol, mode, vol_table));
    
    return RCOK;
}

rc_t lock_m::intent_store_lock(const stid_t &stid, w_okvl::singular_lock_mode m)
{
    lil_lock_modes_t mode = to_lil_mode(m);
    xct_t *xd = xct();
    if (xd == NULL) {
        return RCOK;
    }
    lil_global_table *global_table = get_lil_global_table();
    lil_private_table* private_table = xd->lil_lock_info();
    // get volume lock table without requesting locks.
    lil_private_vol_table *vol_table = private_table->find_vol_table(stid.vol);
    // only request store lock
    W_DO(vol_table->acquire_store_lock(global_table, stid, mode));
    return RCOK;
}

rc_t lock_m::intent_vol_store_lock(const stid_t &stid, w_okvl::singular_lock_mode m)
{
    lil_lock_modes_t mode = to_lil_mode(m);
    xct_t *xd = xct();
    if (xd == NULL) {
        return RCOK;
    }
    lil_global_table *global_table = get_lil_global_table();
    lil_private_table* private_table = xd->lil_lock_info();
    W_DO(private_table->acquire_vol_store_lock(global_table, stid, mode));
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
    FUNC(lock_m::unlock_duration);
    xct_t*        xd = xct();
    w_rc_t        rc;        // == RCOK
    
    if (xd)  {
        // First, release intent locks on LIL
        lil_global_table *global_table = get_lil_global_table();
        lil_private_table *private_table = xd->lil_lock_info();
        private_table->release_all_locks(global_table, read_lock_only, commit_lsn);

        // then, release non-intent locks
        xct_lock_info_t* theLockInfo = xd->lock_info();

        rc =  _core->release_duration(theLockInfo, read_lock_only, commit_lsn);
        w_assert1(read_lock_only || theLockInfo->_head == NULL);
        w_assert1(read_lock_only || theLockInfo->_tail == NULL);
    }
    return rc;
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
