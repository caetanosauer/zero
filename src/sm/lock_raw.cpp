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

w_rc_t RawLockQueue::acquire(RawXct* xct, uint32_t hash, const okvl_mode& mode,
                             int32_t timeout_in_ms, bool check_only) {
    if (check_only) {
        // in this case, we don't have to actually make a lock entry. much faster.
        if (!peek_compatiblity(xct, hash, mode)) {
            xct->update_read_watermark(x_lock_tag);
            return RCOK;
        } else {
            return RC(eLOCKTIMEOUT);
        }
    }

    RawLock* new_lock = xct->allocate_lock(hash, mode, RawLock::ACTIVE);
    atomic_lock_insert(new_lock);
    W_DO(check_compatiblity(new_lock));

    if (new_lock->state == RawLock::WAITING) {
        W_DO(wait_mutex(new_lock, timeout_in_ms));
    }
    xct->update_read_watermark(x_lock_tag);
    return RCOK;
}


void RawLockQueue::atomic_lock_insert(RawLock* new_lock) {
    // atomic CAS to append the new lock.
    // the protocol below is usual lock-free list's algortihm, not the tail swap in [JUNG13]
    MarkablePointer<RawLock> new_ptr(new_lock, false);
    while (true) {
        RawLock *last = tail();
        if (last->next.atomic_cas(last->next, new_ptr)) {
            break;
        }
    }
}

w_rc_t RawLockQueue::check_compatiblity(RawLock* new_lock) const {
    const okvl_mode &mode = new_lock->mode;
    uint32_t hash = new_lock->hash;
    RawXct *xct = new_lock->owner_xct;
    for (MarkablePointer<RawLock> lock = head.next; !lock.is_null(); lock = lock->next) {
        RawLock *pointer = lock.get_pointer();
        if (pointer->state != RawLock::ACTIVE) {
            // Only ACTIVE lock entry can prevent this lock.
            continue;
        }

        if (pointer->owner_xct == xct) {
            // Myself
            continue;
        }
        if (pointer->hash != hash) {
            // Other key. again, queue=bucket but we use precise hash.
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

bool RawLockQueue::peek_compatiblity(RawXct* xct, uint32_t hash, const okvl_mode &mode) const {
    for (MarkablePointer<RawLock> &lock = head.next; !lock.is_null();) {
        RawLock *pointer = lock.get_pointer();
        if (pointer->state != RawLock::ACTIVE || pointer->hash != hash
            || pointer->owner_xct == xct) {
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
    uint32_t hash = lock->hash;
    // update the tag for SX-ELR. we don't have mutex, so need to do atomic CAS
    if (lock->mode.contains_dirty_lock() && commit_lsn > x_lock_tag) {
        DBGOUT3(<<"CAS to update x_lock_tag... cur=" << x_lock_tag.data() << " to "
            << commit_lsn.data());
        while (true) {
            lsndata_t current = x_lock_tag.data();
            lsndata_t desired = commit_lsn.data();
            if (lintel::unsafe::atomic_compare_exchange_strong<lsndata_t>(
                reinterpret_cast<lsndata_t*>(&x_lock_tag), &current, desired)) {
                break;
            } else if (lsn_t(current) >= commit_lsn) {
                break;
            }
        }
        DBGOUT3(<<"Done cur=" << x_lock_tag.data());
    }

    lock->state = RawLock::OBSOLETE;

    atomic_synchronize();
    // we can ignore locks before us. So, start from lock.
    for (MarkablePointer<RawLock> next = lock->next; !next.is_null(); next = next->next) {
        if (next->hash != hash || next->owner_xct == xct) {
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

    // Delink the lock from queue immediately. This is different from [JUNG13]
    while (true) {
        RawLock* predecessor = find(lock);
        // 2-step deletions same as LockFreeList.
        // One simplification is that we don't have to worry about ABA or SEGFAULT.
        // The same lock object (pointer address) never appears again because
        // RawLock objects are maintained by the GC which never recycles.
        RawLock* successor = lock->next.get_pointer();
        if (lock->next.is_marked()
            || lock->next.atomic_cas(successor, successor, false, true, 0, 0)) {
            // CAS succeeded. now really delink it.
            MarkablePointer<RawLock> current_ptr(lock, false);
            MarkablePointer<RawLock> successor_ptr(successor, false);
            delink(predecessor, current_ptr, successor_ptr);
            // done. even if
            break;
        } else {
            // CAS failed. start over.
            DBGOUT1(<<"Interesting. race in delinking while deletion");
           continue;
        }
    }
}

RawLock* RawLockQueue::find(RawLock* lock) const {
    while (true) {
        bool must_retry;
        RawLock* predecessor = find_retry_loop(lock, must_retry);
        if (!must_retry) {
            return predecessor;
        }
    }
}

RawLock* RawLockQueue::find_retry_loop(RawLock* lock, bool& must_retry) const {
    must_retry = false;
    RawLock* predecessor = &head;
    MarkablePointer<RawLock> current = predecessor->next;
    while (!current.is_null()) {
        MarkablePointer<RawLock> successor = current->next;
        while (successor.is_marked()) {
            // current marked for removal. let's delink it
            if (delink(predecessor, current, successor)) {
                current = successor;
                successor = current->next;
            } else {
                // CAS failed. someone might have done something in predecessor, retry.
                must_retry = true;
                return NULL;
            }
        }

        if (current.get_pointer() == lock) {
            return predecessor;
        }
        predecessor = current.get_pointer();
        current = successor;
    }
    // we reached the tail without finding the lock
    w_assert1(false);
    return NULL;
}

RawLock* RawLockQueue::tail() const {
    while (true) {
        bool must_retry;
        RawLock* ret = tail_retry_loop(must_retry);
        if (!must_retry) {
            return ret;
        }
    }
}

RawLock* RawLockQueue::tail_retry_loop(bool& must_retry) const {
    // mostly same as find_retry_loop except.
    must_retry = false;
    RawLock* predecessor = &head;
    MarkablePointer<RawLock> current = predecessor->next;
    while (!current.is_null()) {
        MarkablePointer<RawLock> successor = current->next;
        while (successor.is_marked()) {
            if (delink(predecessor, current, successor)) {
                current = successor;
                successor = current->next;
            } else {
                must_retry = true;
                return NULL;
            }
        }

        predecessor = current.get_pointer();
        current = successor;
    }
    return predecessor;
}

bool RawLockQueue::delink(RawLock* predecessor, const MarkablePointer< RawLock >& target,
                          const MarkablePointer< RawLock >& successor) const {
    MarkablePointer< RawLock > successor_after(successor);
    successor_after.set_mark(false);
    if (predecessor->next.atomic_cas(target, successor_after)) {
        // we just have delinked. the deleted object will be garbage collected
        return true;
    } else {
        // delink failed. someone has done it.
        return false;
    }
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

RawLock* RawXct::allocate_lock(uint32_t hash, const okvl_mode& mode, RawLock::LockState state) {
    RawLock* lock = lock_pool->allocate(*lock_pool_next, thread_id);
    lock->hash = hash;
    lock->mode = mode;
    lock->owner_xct = this;
    lock->next = NULL_RAW_LOCK;
    lock->state = state;
    return lock;
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


std::ostream& operator<<(std::ostream& o, const RawLock& v) {
    o << "RawLock Hash=" << v.hash << " State=";
    switch (v.state) {
        case RawLock::UNUSED : o << "UNUSED"; break;
        case RawLock::OBSOLETE : o << "OBSOLETE"; break;
        case RawLock::ACTIVE : o << "ACTIVE"; break;
        case RawLock::WAITING : o << "WAITING"; break;
        default : o << "Unknown"; break;
    }
    if (v.next.is_marked()) {
        o << "<Marked for death>";
    }
    if (!v.next.is_null()) {
        o << " <Has next>";
    }

    o << "mode=" << v.mode << ", owner_xct=" << v.owner_xct->thread_id;
    return o;
}
