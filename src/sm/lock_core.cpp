/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define LOCK_CORE_C
#define SM_SOURCE

#include "sm_base.h"
#include "lock_s.h"
#include "lock_lil.h"
#include "lock_core.h"
#include "lock_raw.h"
#include "lock_compt.h"
#include "sm_options.h"
#include "xct.h"
#include "w_okvl.h"
#include "w_okvl_inl.h"

// these are not used now
#ifdef SWITCH_DEADLOCK_IMPL
bool g_deadlock_use_waitmap_obsolete = true;
int g_deadlock_dreadlock_interval_ms = 10;
w_error_codes (*g_check_deadlock_impl)(xct_t* xd, lock_request_t *myreq);
#endif // SWITCH_DEADLOCK_IMPL

bool OKVL_EXPERIMENT = false;
uint32_t OKVL_INIT_STR_PREFIX_LEN = 0;
uint32_t OKVL_INIT_STR_UNIQUEFIER_LEN = 0;

struct RawLockCleanerFunctor : public GcWakeupFunctor {
    RawLockCleanerFunctor(RawLockBackgroundThread* cleaner_arg) : cleaner(cleaner_arg) {}
    void wakeup() {
        cleaner->wakeup();
    }
    RawLockBackgroundThread*    cleaner;
};

lock_core_m::lock_core_m(const sm_options &options) : _htab(NULL), _htabsz(0) {
    size_t sz = options.get_int_option("sm_locktablesize", 64000);


    // CS TODO: options below were set in the old Zero tpcc.cpp
            // // very short interval, large segments, for massive accesses.
            // // back-of-envelope-calculation: ignore xct. it's all about RawLock.
            // // sizeof(RawLock)=64 or something. 8 * 256 * 4096 * 64 = 512MB. tolerable.
            // options.set_int_option("sm_rawlock_gc_interval_ms", 3);
            // options.set_int_option("sm_rawlock_lockpool_initseg", 255);
            // options.set_int_option("sm_rawlock_xctpool_initseg", 255);
            // options.set_int_option("sm_rawlock_lockpool_segsize", 1 << 12);
            // options.set_int_option("sm_rawlock_xctpool_segsize", 1 << 8);
            // options.set_int_option("sm_rawlock_gc_generation_count", 5);
            // options.set_int_option("sm_rawlock_gc_init_generation_count", 5);
            // options.set_int_option("sm_rawlock_gc_free_segment_count", 50);
            // options.set_int_option("sm_rawlock_gc_max_segment_count", 255);
            // // meaning: a newly created generation has a lot of (255) segments.
            // // as soon as remaining gets low, we recycle older ones (few generations).

    size_t generation_count = options.get_int_option("sm_rawlock_gc_generation_count", 5);
    size_t init_generations = options.get_int_option("sm_rawlock_gc_init_generation_count", 5);
    if (init_generations > generation_count) {
        generation_count = init_generations;
    }
    size_t lockpool_initseg = options.get_int_option("sm_rawlock_lockpool_initseg", 255);
    size_t xctpool_initseg = options.get_int_option("sm_rawlock_xctpool_initseg", 255);
    size_t lockpool_segsize = options.get_int_option("sm_rawlock_lockpool_segsize", 1 << 12);
    size_t xctpool_segsize = options.get_int_option("sm_rawlock_xctpool_segsize", 1 << 8);
    DBGOUT3(<<"lock_core_m constructor: sm_locktablesize=" << sz
        << ", sm_rawlock_gc_generation_count=" << generation_count
        << ", sm_rawlock_gc_init_generation_count=" << init_generations
        << ", sm_rawlock_lockpool_initseg=" << lockpool_initseg
        << ", sm_rawlock_xctpool_initseg=" << xctpool_initseg
        << ", sm_rawlock_lockpool_segsize=" << lockpool_segsize
        << ", sm_rawlock_xctpool_segsize=" << xctpool_segsize);

    // find _htabsz, a power of 2 greater than sz
    int b=0; // count bits shifted
    for (_htabsz = 1; _htabsz < sz; _htabsz <<= 1) b++;

    w_assert1(!_htab); // just to check size

    w_assert1(_htabsz >= 0x40);
    w_assert1(b >= 6 && b <= 23);
    // if anyone wants a hash table bigger,
    // he's probably in trouble.

    // Now convert to a prime number in that range.
    // get highest prime for that numer:
    b -= 6;

    _htabsz = primes[b];
    _htab = new RawLockQueue[_htabsz];
    w_assert1(_htab);
    ::memset(_htab, 0, _htabsz * sizeof(RawLockQueue));

    _lock_pool = new GcPoolForest<RawLock>("Lock Pool", generation_count,
                                           lockpool_initseg, lockpool_segsize);
    w_assert1(_lock_pool);
    while (_lock_pool->active_generations() < init_generations) {
        _lock_pool->advance_generation(lsn_t::null, lsn_t::null, lockpool_initseg, lockpool_segsize);
    }

    _xct_pool = new GcPoolForest<RawXct>("Xct Pool", generation_count,
                                         xctpool_initseg, xctpool_segsize);
    w_assert1(_xct_pool);
    while (_xct_pool->active_generations() < init_generations) {
        _xct_pool->advance_generation(lsn_t::null, lsn_t::null, xctpool_initseg, xctpool_segsize);
    }

    _raw_lock_cleaner = new RawLockBackgroundThread(options, _lock_pool, _xct_pool);
    w_assert1(_raw_lock_cleaner);
    _raw_lock_cleaner->start();

    _raw_lock_cleaner_functor = new RawLockCleanerFunctor(_raw_lock_cleaner);
    w_assert1(_raw_lock_cleaner_functor);
    _lock_pool->gc_wakeup_functor = _raw_lock_cleaner_functor;
    _xct_pool->gc_wakeup_functor = _raw_lock_cleaner_functor;

    _lil_global_table = new lil_global_table;
    w_assert1(_lil_global_table);
    _lil_global_table->clear();
}

lock_core_m::~lock_core_m()
{
    DBGOUT3( << " lock_core_m::~lock_core_m()" );
    DBGOUT1( << "Checking if all locks were released..." );
    for (uint32_t i = 0; i < _htabsz; ++i) {
        if (!_htab[i].head.next.is_null()) {
            ERROUT( << "There is some lock not released!" );
            dump(std::cerr);
            w_assert0(false);
            break;
        }
    }

    _lock_pool->gc_wakeup_functor = NULL;
    _xct_pool->gc_wakeup_functor = NULL;
    _raw_lock_cleaner->stop_synchronous();
    delete _raw_lock_cleaner;
    delete _raw_lock_cleaner_functor;

    delete _lock_pool;
    delete _xct_pool;

    delete[] _htab;
    _htab = NULL;

    delete _lil_global_table;
    _lil_global_table = NULL;
}


__thread gc_pointer_raw tls_xct_pool_next; // Thread local variable for xct_pool.
__thread gc_pointer_raw tls_lock_pool_next; // Thread local variable for lock_pool.

RawXct* lock_core_m::allocate_xct() {
    RawXct* xct = _xct_pool->allocate(tls_xct_pool_next, ::pthread_self());
    xct->init(static_cast<gc_thread_id>(::pthread_self()), _lock_pool, &tls_lock_pool_next);
    return xct;
}

void lock_core_m::deallocate_xct(RawXct* xct) {
    xct->uninit();
    _xct_pool->deallocate(xct);
}


w_error_codes lock_core_m::acquire_lock(RawXct* xct, uint32_t hash, const okvl_mode& mode,
                bool check, bool wait, bool acquire, int32_t timeout, RawLock** out)
{
    w_assert1(timeout >= 0 || timeout == WAIT_FOREVER);
    uint32_t idx = _table_bucket(hash);
    while (true) {
        w_error_codes er = _htab[idx].acquire(xct, hash, mode, timeout,
                check, wait, acquire, out);
        // Possible return codes:
        //   eDEADLOCK - detected deadlock, released the lock entry,
        //                         automaticlly retry here if caller does not own other locks
        //                         otherwise, return to caller
        //   eCONDLOCKTIMEOUT - there is a lock preventing the grant (compatibility),
        //                         if true == conditional, keep the already inserted lock entry and return control to caller
        //                         caller should retry using retry_acquire
        //   w_error_ok - acquired lock, return to caller

        if (er == eDEADLOCK && !xct->has_locks() && wait && timeout == WAIT_FOREVER) {
            // this was a failed lock aquisition, so it didn't get the new lock.
            // the transaction doesn't have any other lock, and waiting unconditionally.
            // this means we can forever retry without risking anything!
            atomic_synchronize();
            continue;
        }
        return er;
    }
}

w_error_codes lock_core_m::retry_acquire(RawLock** lock, bool acquire, int32_t timeout) {
    w_assert1(timeout >= 0 || timeout == WAIT_FOREVER);
    uint32_t hash = (*lock)->hash;
    uint32_t idx = _table_bucket(hash);
    const okvl_mode& mode = (*lock)->mode;
    RawXct* xct = (*lock)->owner_xct;
    while (true) {
        // Possible return codes:
        //   eDEADLOCK - detected deadlock, released the lock entry,
        //                         automaticlly retry here if caller does not own other locks
        //                         otherwise, return to caller
        //   eCONDLOCKTIMEOUT - there is a lock preventing the grant (compatibility),
        //                         if true == conditional, keep the already inserted lock entry and return control to caller
        //                         caller should retry using retry_acquire
        //   w_error_ok - acquired lock, return to caller
        w_error_codes er = _htab[idx].retry_acquire(lock, true, acquire, timeout);
        if (er == eDEADLOCK && !xct->has_locks() && timeout == WAIT_FOREVER) {
            // same as above, but now the lock was removed. we have to switch to acquire_lock.
            w_assert1(*lock == NULL);
            return acquire_lock(xct, hash, mode, true, true, acquire, WAIT_FOREVER, lock);
        }
        return er;
    }
}

void lock_core_m::release_lock(RawLock* lock, lsn_t commit_lsn) {
    w_assert1(lock);
    uint32_t hash = lock->hash;
    uint32_t idx = _table_bucket(hash);
    _htab[idx].release(lock, commit_lsn);
}


void lock_core_m::release_duration(bool read_lock_only, lsn_t commit_lsn) {
    xct_t* xd = g_xct();
    if (xd == NULL) {
        return;
    }
    RawXct* xct = xd->raw_lock_xct();
    //we always release backwards. otherwise concurrency bug.
    // First, quickly set OBSOLETE to all locks.
    for (RawLock* lock = xct->private_first; lock != NULL; lock = lock->xct_next) {
        if (lock->mode.contains_dirty_lock()) {
            if (!read_lock_only) {
                // also do SX-ELR tag update BEFORE changing the status
                if (commit_lsn != lsn_t::null) {
                    uint32_t hash = lock->hash;
                    uint32_t idx = _table_bucket(hash);
                    _htab[idx].update_xlock_tag(commit_lsn);
                }
                lock->state = RawLock::OBSOLETE;
            }
        } else {
            lock->state = RawLock::OBSOLETE;
        }
    }
    // announce it as soon as possible
    atomic_synchronize();

    // then, do actual deletions. this can be okay to delay.
    if (read_lock_only) {
        // releases only read locks. as now we don't do lock upgrades,
        // lock downgrades are not needed any more.
        for (RawLock* lock = xct->private_first; lock != NULL;) {
            RawLock* next = lock->xct_next;
            if (!lock->mode.contains_dirty_lock()) {
                uint32_t hash = lock->hash;
                uint32_t idx = _table_bucket(hash);
                _htab[idx].release(lock, commit_lsn);
            }
            lock = next;
        }
    } else {
        while (xct->private_first != NULL)  {
            RawLock* lock = xct->private_first;
            uint32_t hash = lock->hash;
            uint32_t idx = _table_bucket(hash);
            _htab[idx].release(lock, commit_lsn);
        }
    }
    DBGOUT4(<<"lock_core_m::release_duration DONE");
}
