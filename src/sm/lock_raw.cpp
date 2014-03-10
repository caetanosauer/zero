/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#include "lock_raw.h"
#include <Lintel/AtomicCounter.hpp>
#include <time.h>
#include "w_okvl_inl.h"
#include "w_debug.h"
#include "critical_section.h"
#include "sthread.h"

/**
 * \brief Invokes "mfence", which is sfence + lfence.
 * \ingroup RAWLOCK
 * \details
 * [JUNG13] uses only one type of membar named "atomic_synchronize", not clarifying which
 * place needs full mfence, which place needs only sfence or lfence.
 * This code might result in too conservative barriers. Should revisit later.
 */
inline void atomic_synchronize() {
    lintel::atomic_thread_fence(lintel::memory_order_seq_cst); // corresponds to full mfence
}


typedef MarkablePointer<RawLock> LockPtr;
typedef MarkablePointer<RawLockQueue> QueuePtr;

RawLock* allocate_lock(RawXct* xct, const okvl_mode& mode) {
    // TODO
    RawLock* lock = new RawLock();
    lock->mode = mode;
    lock->owner_xct = xct;
    return lock;
}

w_rc_t RawLockQueue::acquire(RawXct* xct, const okvl_mode& mode,
                             int32_t timeout_in_ms, bool check_only) {
    if (check_only) {
        // in this case, we don't have to actually make a lock entry. much faster.
        if (!peek_compatiblity(xct, mode)) {
            xct->update_read_watermark(_x_lock_tag);
            return RCOK;
        } else {
            return RC(eLOCKTIMEOUT);
        }
    }

    RawLock* new_lock = allocate_lock(xct, mode);
    new_lock->state = RawLock::ACTIVE;

    atomic_lock_insert(new_lock);
    W_DO(check_compatiblity(new_lock));

    if (new_lock->state == RawLock::WAITING) {
        W_DO(wait_mutex(new_lock, timeout_in_ms));
    }
    xct->update_read_watermark(_x_lock_tag);
    return RCOK;
}


void RawLockQueue::atomic_lock_insert(RawLock* new_lock) {
    LockPtr new_ptr(new_lock);

    // atomic swap to append the new lock.
    LockPtr old_tail(_tail.atomic_swap(new_ptr)); // (1)
    if (!old_tail.is_null()) {
        old_tail->next = new_ptr; // (2)
    } else {
        // observing "old_tail==NULL" with atomic_swap means now this is also the head.
        // as we put new_ptr in _tail, setting it to _head.next is safe.
        DBGOUT3(<<"Yay, I'm the new head " << *new_lock);
        _head.next = new_ptr; // (2)-B
    }
    atomic_synchronize();
    next_pointer_update(); // (3)
    atomic_synchronize();
}

void RawLockQueue::next_pointer_update() {
    while (true) {
        bool retry = false;
        for (LockPtr &lock = wait_for_next(&_head); !lock.is_null();
                lock = wait_for_next(lock.get_pointer())) {
            // Notice lock is LockPtr&, not LockPtr. Thus we can modify it.
            RawLock *pointer = lock.get_pointer();
            if (pointer->state != RawLock::OBSOLETE) {
                continue;
            }
            // mark the stolen bit in the pointer. This makes racing remove safe.
            // See Figure 9.23 and 9.26 of [HERLIHY].
            if (!lock.is_marked() && !lock.atomic_cas(pointer, pointer, false, true)) {
                // someone is also trying to delete it, we are late.
                DBGOUT1(<<"Interesting. race in marking while deletion");
                retry = true;
                break;
            } else {
                // okay, we successfully marked it (or already). now onto actually delink.
                if (lock.atomic_cas(pointer, pointer->next.get_pointer(), false, false)) {
                    DBGOUT3(<<"Successfully delinked");
                } else {
                    // someone has already delinked
                    DBGOUT1(<<"Interesting. race in delinking while deletion");
                    retry = true;
                    break;
                }
            }
        }
        if (!retry) {
            break;
        }
    }
}
LockPtr& RawLockQueue::wait_for_next(RawLock* pointer) {
    w_assert1(pointer != NULL);
    if (pointer == _tail.get_pointer()) {
        // tail has no next pointer anyway
        return const_cast<LockPtr&>(NULL_RAW_LOCK);
    }
    while (pointer->next.is_null()) {
        DBGOUT1(<<"Interesting. concurrent insert waiting for setting old_tail.next");
        atomic_synchronize();
    }
    return pointer->next;
}

w_rc_t RawLockQueue::check_compatiblity(RawLock* new_lock) {
    const okvl_mode &mode = new_lock->mode;
    RawXct *xct = new_lock->owner_xct;
    for (LockPtr &lock = wait_for_next(&_head); !lock.is_null();
            lock = wait_for_next(lock.get_pointer())) {
        RawLock *pointer = lock.get_pointer();
        if (pointer->state != RawLock::ACTIVE) {
            // Only ACTIVE lock entry can prevent this lock.
            continue;
        }

        if (pointer->owner_xct == xct) {
            // Myself
            continue;
        }

        if (!mode.is_compatible_grant(pointer->mode)) {
            // duh, there is a lock that prevents us.
            new_lock->state = RawLock::WAITING;
            // one more chance. Maybe it has been just removed?
            atomic_synchronize();
            if (pointer->state == RawLock::OBSOLETE) {
                // yay!
                new_lock->state = RawLock::ACTIVE;
                atomic_synchronize();
                continue;
            }

            // nope. we have to wait. let's check for deadlock.
            xct->blocker = pointer->owner_xct;
            if (xct->is_deadlocked()) {
                return RC(eDEADLOCK);
            } else {
                xct->state = RawXct::WAITING;
                break;
            }
        }
    }

    return RCOK;
}

bool RawLockQueue::peek_compatiblity(RawXct* xct, const okvl_mode &mode) {
    for (LockPtr &lock = _head.next; !lock.is_null();) {
        RawLock *pointer = lock.get_pointer();

        if (pointer->state != RawLock::ACTIVE || pointer->owner_xct == xct) {
            continue;
        }

        if (!mode.is_compatible_grant(pointer->mode)) {
            return false;
        }
    }
    return true;
}

w_rc_t RawLockQueue::wait_mutex(RawLock* new_lock, int32_t timeout_in_ms) {
    if (timeout_in_ms <= 0) {
        return RC(eLOCKTIMEOUT);
    }
    RawXct *xct = new_lock->owner_xct;
    CRITICAL_SECTION(cs, xct->lock_wait_mutex);
    atomic_synchronize();
    // after membar, we test again if we really have to sleep or not.
    // See Figure 6 of [JUNG13] for why we need to do this.
    W_DO(check_compatiblity(new_lock));
    if (new_lock->state == RawLock::WAITING) {
        xct->state = RawXct::WAITING;
        struct timespec ts;
        sthread_t::timeout_to_timespec(timeout_in_ms, ts);
        int ret = ::pthread_cond_timedwait(&xct->lock_wait_cond, &xct->lock_wait_mutex, &ts);
        if (ret == 0) {
            DBGOUT3(<<"Successfully ended the wait.");
            if (xct->deadlock_detected_while_unlock) {
                DBGOUT1(<<"Deadlock reported by other transaction!");
                return RC(eDEADLOCK);
            }
            DBGOUT3(<<"Now it we should be granted!");
            w_assert1(new_lock->state == RawLock::ACTIVE);
        } else if (ret == ETIMEDOUT) {
            DBGOUT1(<<"Wait timed out");
            return RC(eLOCKTIMEOUT);
        } else {
            // unexpected error
            ERROUT(<<"WTF? " << ret);
            return RC(eINTERNAL);
        }
    } else {
        DBGOUT1(<<"Interesting. Re-check after barrier tells that now we are granted");
        new_lock->state = RawLock::ACTIVE;
        atomic_synchronize();
    }
    return RCOK;
}

void RawLockQueue::release(RawLock* lock, const lsn_t& commit_lsn) {
    RawXct *xct = lock->owner_xct;
    // update the tag for SX-ELR. we don't have mutex, so need to do atomic CAS
    if (lock->mode.contains_dirty_lock() && commit_lsn > _x_lock_tag) {
        DBGOUT3(<<"CAS to update _x_lock_tag... cur=" << _x_lock_tag << " to "
            << commit_lsn);
        while (true) {
            lsndata_t current = _x_lock_tag.data();
            lsndata_t desired = commit_lsn.data();
            if (lintel::unsafe::atomic_compare_exchange_strong<lsndata_t>(
                reinterpret_cast<lsndata_t*>(&_x_lock_tag), &current, desired)) {
                break;
            } else if (lsn_t(current) >= commit_lsn) {
                break;
            }
        }
        DBGOUT3(<<"Done cur=" << _x_lock_tag);
    }

    lock->state = RawLock::OBSOLETE;
    atomic_synchronize();
    // we can ignore locks before us. So, start from lock.
    for (LockPtr &next = wait_for_next(lock); !next.is_null();
            next = wait_for_next(next.get_pointer())) {
        if (next->owner_xct == xct) {
            continue;
        }
        // BEFORE we check the status of the next lock, we have to take mutex.
        // This is required to not miss the case we have to wake them up.
        CRITICAL_SECTION(cs, next->owner_xct->lock_wait_mutex);
        if (next->state == RawLock::WAITING) {
            // Does releasing the lock free him up?
            w_rc_t ret = check_compatiblity(next.get_pointer());
            if (!ret.is_error() && next->state == RawLock::WAITING) {
                // then keep waiting
            } else {
                // the next should wake up for..
                if (ret.err_num() == eDEADLOCK) {
                    // deadlock!!
                    DBGOUT1(<<"Deadlock detected while unlocking.");
                    next->owner_xct->deadlock_detected_while_unlock = true;
                } else {
                    // now granted!!
                    DBGOUT3(<<"Now granted while unlocking.");
                    w_assert1(next->state == RawLock::ACTIVE);
                    next->owner_xct->state = RawXct::ACTIVE;
                }
                atomic_synchronize();
                ::pthread_cond_broadcast(&next->owner_xct->lock_wait_cond);
            }
        }
    }
}


w_rc_t RawLockBucket::acquire(uint32_t hash, RawXct* xct, const okvl_mode& mode,
                              int32_t timeout_in_ms, bool check_only) {

}


void RawXct::init() {
    state = RawXct::UNUSED;
    deadlock_detected_while_unlock = false;
    blocker = NULL;
    read_watermark = lsn_t::null;
    ::pthread_mutex_init(&lock_wait_mutex, NULL);
    ::pthread_cond_init(&lock_wait_cond, NULL);
}

void RawXct::uninit() {
    state = RawXct::UNUSED;
    blocker = NULL;
    ::pthread_mutex_destroy(&lock_wait_mutex);
    ::pthread_cond_destroy(&lock_wait_cond);
}


bool RawXct::is_deadlocked() {
    w_assert1(blocker != NULL);
    for (RawXct* next = blocker; next != NULL; next = next->blocker) {
        if (next->state != RawXct::WAITING) {
            return false;
        }
        if (next->blocker == this) {
            return true;
        }
    }
    return false;
}
