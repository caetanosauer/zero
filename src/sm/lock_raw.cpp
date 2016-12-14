/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#include "lock_raw.h"
#include <time.h>
#include <set>
#include "w_okvl_inl.h"
#include "w_debug.h"
#include "critical_section.h"
#include "sm_options.h"

#include "sm_base.h"
#include "log_core.h"
#include "log_lsn_tracker.h"

// Following includes are to have the ability to handle on_demand UNDO for Restart operation
#include "smthread.h"
#include "lock.h"
#include "xct.h"
#include "restart.h"

////////////////////////////////////////////////////////////////////////////////////////
////
//// RawLockQueue::Iterator and related methods Implementation BEGIN
////
////////////////////////////////////////////////////////////////////////////////////////

// Initialized to a non-zero value to ensure we do not skip the the first RawLockQueue::trigger_UNDO() call
const int HAS_LOSER_COUNT = 999;                     // Has loser transaction in transaction table
const int NO_LOSER_COUNT = 0;                        // No more loser transaction in transaction table
int RawLockQueue::loser_count = HAS_LOSER_COUNT;

RawLockQueue::Iterator::Iterator(const RawLockQueue* enclosure_arg, RawLock* start_from)
    : enclosure(enclosure_arg), predecessor(start_from) {
    w_assert1(predecessor != NULL);
    w_assert1(!predecessor->next.is_marked());
    current = predecessor->next;
}

void RawLockQueue::Iterator::next(bool& must_retry) {
    must_retry = false;
    // the lock-free list iteration with mark-for-death in [MICH93]
    while (!current.is_null() && current->next.is_marked()) {
        // current marked for removal. let's physically remove it to be safe.
        if (enclosure->delink(predecessor, current.get_pointer(), current->next.get_pointer())) {
            current = current->next;
        } else {
            // CAS failed. someone might have done something in predecessor, retry.
            must_retry = true;
            return;
        }
    }
    if (!current.is_null()) {
        predecessor = current.get_pointer();
        current = current->next;
    }
}

RawLock* RawLockQueue::find_predecessor(RawLock* lock) const {
    bool must_retry = false;
    do {
        must_retry = false;
        for (Iterator iterator(this, &head); !must_retry && !iterator.is_null();
                iterator.next(must_retry)) {
            if (iterator.current.get_pointer() == lock) {
                return iterator.predecessor;
            }
        }
    } while (must_retry);
    // we reached the tail without finding the lock
    return NULL;
}

RawLock* RawLockQueue::tail() const {
    do {
        bool must_retry = false;
        Iterator iterator(this, &head);
        for (; !must_retry && !iterator.is_null(); iterator.next(must_retry));
        if (!must_retry) {
            w_assert1(iterator.predecessor != NULL); // at least head exists.
            return iterator.predecessor;
        }
    } while (true);
}

bool RawLockQueue::delink(RawLock* predecessor, RawLock* target, RawLock* successor) const {
    w_assert1(predecessor != NULL);
    w_assert1(target != NULL);
    if (predecessor->next.atomic_cas(target, successor, false, false, 0, 0)) {
        // we just have delinked.
        return true;
    } else {
        // delink failed. someone has done it.
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////////////
////
//// RawLockQueue Implementation BEGIN
////
////////////////////////////////////////////////////////////////////////////////////////

w_error_codes RawLockQueue::acquire(RawXct* xct, uint32_t hash, const okvl_mode& mode,
                int32_t timeout_in_ms, bool check, bool wait, bool acquire,
                RawLock** out)
{
    w_assert1(wait || check || acquire);
    w_assert1(check || !wait);
    w_assert1(out != NULL);
    *out = NULL;
    if (check && !acquire) {
        // peek_compatiblity() provides weaker correctness condition, but check_only is used
        // only when it's still safe (we have EX latch in the page!). so, fine.
        if (peek_compatiblity(xct, hash, mode)) {
            // hope fully this case.
            // in this case, we don't have to actually make a lock entry. much faster.
            xct->update_read_watermark(x_lock_tag);
            return w_error_ok;
        }
        // else, we have to actually make a lock entry and wait. but will remove
        // the lock entry as soon as we are granted.
    }

    // <Line numbers> from [JUNG13] Fig 3.
    RawLock* new_lock = xct->allocate_lock(hash, mode, RawLock::ACTIVE); // A1-A2
    DBGOUT4(<< "RawLockQueue::acquire() before:" << *this << "adding:" << *new_lock);
    atomic_lock_insert(new_lock); // A3 . BTW this implies a barrier in x86
    Compatibility compatibility = check_compatiblity(new_lock); // A4-A5

    if (check && (compatibility.deadlocked || !compatibility.can_be_granted))
    {
        DBGOUT1(<< "RawLockQueue::acquire: not able to acquire lock, check for UNDO");

        // Cannot grant the requested lock
        // Handle on_demand UNDO if we need one
        // note the on_demand UNDO is a blocking operation to the
        // current thread (which has a concurrent user transaction)
        w_assert1(NULL != compatibility.blocker);
        w_assert1(compatibility.blocker != new_lock->owner_xct);
        DBGOUT3(<< "RawLockQueue::acquire(): cannot grant the lock, check for UNDO");
        if (trigger_UNDO(compatibility))
        {
            // The user transaction (this thread) triggered an on_demand UNDO which
            // rolled back a loser transaction, check the compatibility again since
            // the lock manager entries changed due to loser transaction rollback
            DBGOUT3(<< "RawLockQueue::acquire(): current thread triggered UNDO");
            compatibility = check_compatiblity(new_lock); // A4-A5
        }
    }

    if (compatibility.deadlocked) {
        w_assert1(wait);
        release(new_lock, lsn_t::null);
        return eDEADLOCK;
    } else if (!compatibility.can_be_granted && check) {
        // duh, there is a lock that prevents us.
        w_assert1(compatibility.blocker != NULL);
        xct->blocker = compatibility.blocker; // A6'
        new_lock->state = RawLock::WAITING; // A6
        // announce our waiting status to make sure no one is falsely waiting for us.
        atomic_synchronize_if_mutex(); // A7
        if (!wait) {
            // a failed conditional locking! we immediately return, but keep the
            // already inserted lock entry.
            // This return value indicate a safe retry
            // This return value tells the caller to increment the pin count and then
            // release latch on the page, so the page does not get evicted and others
            // can access the page, and then retry the lock acquisition while the
            // retry is a blocking operation for the transaction
            DBGOUT3(<<"RawLockQueue::acquire() failed conditional locking:" << *this);
            *out = new_lock;
            return eCONDLOCKTIMEOUT;
        }
#ifndef PURE_SPIN_RAWLOCK
        compatibility = check_compatiblity(new_lock); // A8
        if (compatibility.deadlocked) {
            release(new_lock, lsn_t::null);
            return eDEADLOCK;
        } else if (compatibility.can_be_granted) {
            xct->blocker = NULL; // A9'
            new_lock->state = RawLock::ACTIVE; // A9
            atomic_synchronize_if_mutex(); // A10
        }
#endif // PURE_SPIN_RAWLOCK
    }

    w_error_codes err_code = complete_acquire(&new_lock, wait, acquire, timeout_in_ms);
    if (err_code == w_error_ok && acquire) {
        w_assert1(new_lock->state == RawLock::ACTIVE);
        *out = new_lock;
    }
    return err_code;
}

w_error_codes RawLockQueue::retry_acquire(RawLock** lock, bool wait, bool acquire,
        int32_t timeout_in_ms)
{
    w_assert1(lock != NULL && (*lock) != NULL);
    RawXct *xct = (*lock)->owner_xct;
    atomic_synchronize();
    Compatibility compatibility = check_compatiblity(*lock);
    if (compatibility.deadlocked) {
        release(*lock, lsn_t::null);
        *lock = NULL;
        return eDEADLOCK;
    } else if (compatibility.can_be_granted) {
        xct->blocker = NULL;
        (*lock)->state = RawLock::ACTIVE;
        atomic_synchronize();
    }

    return complete_acquire(lock, wait, acquire, timeout_in_ms);
}

w_error_codes RawLockQueue::complete_acquire(RawLock** lock, bool wait, bool acquire,
        int32_t timeout_in_ms)
{
    // if we get here, the lock can be granted, but we might need
    // to wait for the lock to be available, it happens in 'wait_for'

    RawXct *xct = (*lock)->owner_xct;
    if (wait && (*lock)->state == RawLock::WAITING) {
        w_error_codes err_code = wait_for(*lock, timeout_in_ms);
        if (err_code != w_error_ok) {
            release(*lock, lsn_t::null);
            *lock = NULL;
            return err_code;
        }
    }

    // okay, we are granted.
    atomic_synchronize();
    w_assert1((*lock)->state == RawLock::ACTIVE);
    w_assert1(xct->blocker == NULL);
    xct->update_read_watermark(x_lock_tag);
    if (!acquire) {
        // immediately release the lock
        release(*lock, lsn_t::null);
        *lock = NULL;
    }
    DBGOUT4(<<"RawLockQueue::acquire() after:" << *this);
    return w_error_ok;
}

void RawLockQueue::atomic_lock_insert(RawLock* new_lock) {
    // atomic CAS to append the new lock.
    // the protocol below is usual lock-free list's algortihm, not the tail swap in [JUNG13]
    MarkablePointer<RawLock> new_ptr(new_lock, false);
    while (true) {
        RawLock *last = tail();
        // in case someone else is now deleting or adding to tail, we must do atomic CAS
        // with special pointer that has mark-for-death and ABA counter.
        // if anything unexpected observed, retry from traversal. same as LockFreeList.
        if (last->next.atomic_cas(NULL_RAW_LOCK, new_ptr)) {
            break;
        }
    }
}

RawLockQueue::Compatibility RawLockQueue::check_compatiblity(RawLock *lock) const {
    if (head.next.get_pointer() == lock) {
        // fast path. If it's the first, because followers respect predecessors, granted.
        // also, remember that no one can newly enter between I and head because
        // new locks are appended at last.
        // Can grant the lock, it is not a deadlock, no blocker
        return Compatibility(true /*can_be_granted*/, false /*deadlocked*/, NULL /*blocker txn*/);
    }
    const okvl_mode &mode = lock->mode;
    uint32_t hash = lock->hash;
    RawXct *xct = lock->owner_xct;
    bool must_retry = false;
    do {
        must_retry = false;
        for (Iterator iterator(this, &head); !must_retry && !iterator.is_null();
                iterator.next(must_retry)) {
            RawLock *pointer = iterator.current.get_pointer();
            if (pointer == lock) {
                // as we atomically insert, every lock respects predecessors.
                // furthermore, we no longer have lock upgrades. they are multiple locks.
                // so, we never have to check locks after myself.
                break;
            }
            if (pointer->state == RawLock::OBSOLETE) {
                continue;
            }

            // oh yeah, we hit the case where the following assertion isn't met, and suprsingly
            // the debugger said pointer->state was OBSOLETE. yes, cacheline-sync happened
            // between the line above and here! it's possible. this assertion has to be disabled.
            // w_assert1(pointer->state == RawLock::ACTIVE || pointer->state == RawLock::WAITING);

            if (pointer->owner_xct == xct) {
                // Myself
                continue;
            }
            if (pointer->hash != hash) {
                // Other key. again, queue=bucket but we use precise hash.
                continue;
            }

            if (!mode.is_compatible_grant(pointer->mode)) {
                // Not able to grant the request lock, it is either deadlock or lock conflict
                // During on_demand restart with lock, lock re-acquisition happens during
                // Log Analysis phase and we should not run into deadlock or lock conflict
                // therefore as part of 'restart' process, the only time we might fall into here
                // is during on_demand UNDO and caller is a user transaction, in this case
                // the user transaction has to trigger on_demand UNDO for the loser
                // transaction (blocker)
                // Rolling back the loser transaction would affect lock manager (lock queue),
                // therefore the operation cannot be performed in this loop, it is caller's
                // responsibility to trigger the UNDO operation based on the return from
                // this function.
                // If deadlock, set blocker to the current owning transaction of the lock, this
                // value would be used only if on_demand UNDO

                if (xct->is_deadlocked(pointer->owner_xct)) {
                    // Cannot grant the lock because this is a deadlock, no blocker txn in this case
                    return Compatibility(false /*can_be_granted*/, true /*deadlocked*/, pointer->owner_xct /*blocker txn*/);
                } else {
                    // Cannot grant the lock but it is not a deadlock (it is a lock conflict),
                    // blocker txn is pointer->owner_xct (current owning transaction of the lock),
                    // caller need to wait for the blocker txn to release its lock
                    return Compatibility(false /*can_be_granted*/, false /*deadlocked*/, pointer->owner_xct /*blocker txn*/);
                }
            }
        }
    } while (must_retry);
    // Can grant the lock, it is not a deadlock, no blocker
    return Compatibility(true /*can_be_granted*/, false /*deadlocked*/, NULL /*blocker txn*/);
}

bool RawLockQueue::peek_compatiblity(RawXct* xct, uint32_t hash, const okvl_mode &mode) const {
    for (MarkablePointer<RawLock> lock = head.next; !lock.is_null();) {
        RawLock *pointer = lock.get_pointer();
        if (pointer->state == RawLock::ACTIVE && pointer->hash == hash
            && pointer->owner_xct != xct) {
            if (!mode.is_compatible_grant(pointer->mode)) {
                return false;
            }
        }
        lock = pointer->next;
    }
    return true;
}

w_error_codes RawLockQueue::wait_for(RawLock* new_lock, int32_t timeout_in_ms) {
    // If we get here, the initial acquire() and retry_acquire() indicates no deadlock
    // and we might need to wait for the lock becomes available.
    // In this function we go into the sleep-wake-up-recheck cycle,
    // it breaks out from the sleep loop if
    // 1) encounter new deadlock, 2) acquired lock, 3) timeout, 4) unexpected error

    w_assert1(timeout_in_ms >= 0 || timeout_in_ms < 0); // to suppress warning
    RawXct *xct = new_lock->owner_xct;
#ifndef PURE_SPIN_RAWLOCK
    CRITICAL_SECTION(cs, xct->lock_wait_mutex); // A18
#endif // PURE_SPIN_RAWLOCK
    atomic_synchronize_if_mutex(); // A19
    // after membar, we test again if we really have to sleep or not.
    // See Figure 6 of [JUNG13] for why we need to do this.
    Compatibility compatibility = check_compatiblity(new_lock); // A20
    if (compatibility.deadlocked) {
        xct->blocker = NULL;
        atomic_synchronize();
        return eDEADLOCK;
    } else if (!compatibility.can_be_granted) {
        xct->state = RawXct::WAITING; // A21

#ifdef PURE_SPIN_RAWLOCK
        uint32_t spin_count = 0;
        while (true) { // pure spin implementation. much more efficient.
            if (((++spin_count) & 0xFFF) == 0) { // not too frequent barriers
                atomic_synchronize();
            }
            compatibility = check_compatiblity(new_lock);
            if (compatibility.can_be_granted) {
                xct->blocker = NULL;
                new_lock->state = RawLock::ACTIVE;
                atomic_synchronize();
                return w_error_ok;
            } else if (compatibility.deadlocked) {
                DBGOUT1(<<"Deadlock found by myself! lock=" << *new_lock << ", queue="
                    << *this << ", xct=" << *xct);
                xct->blocker = NULL;
                atomic_synchronize();
                return eDEADLOCK;
            } else {
                if (xct->blocker != compatibility.blocker) {
                    xct->blocker = compatibility.blocker;
                }
            }
        }
#else // PURE_SPIN_RAWLOCK
        bool forever = timeout_in_ms < 0;
        const int32_t INTERVAL = 1000; // something sensible for debugging. we repeat anyways.
        int max_sleep_count = forever ? 0x7FFFFFFF : (timeout_in_ms / INTERVAL) + 1;
        for (int sleep_count = 0; sleep_count < max_sleep_count; ++sleep_count) {
            DBGOUT3(<<"Going into pthread_cond_timedwait. new_lock=" << *new_lock);
            if (sleep_count > 5) {
                ERROUT(<<"Very long lock wait! Sleep count=" << sleep_count << "/"
                    << max_sleep_count << "new_lock=" << *new_lock);
            }
            struct timespec ts;
            smthread_t::timeout_to_timespec(INTERVAL, ts);
            int ret = ::pthread_cond_timedwait(&xct->lock_wait_cond,
                                                &xct->lock_wait_mutex, &ts); // A22
            atomic_synchronize();
            if (ret != 0 && ret != ETIMEDOUT) {
                // unexpected error
                ERROUT(<<"WTF? " << ret);
                return eINTERNAL;
            }

            DBGOUT3(<<"Woke up.");
            if (xct->deadlock_detected_by_others) {
                DBGOUT1(<<"Deadlock reported by other transaction!");
                xct->blocker = NULL;
                return eDEADLOCK;
            }
            atomic_synchronize();
            if (xct->state == RawXct::WAITING) {
                DBGOUT1(<<"Still waiting. lock=" << *new_lock);
                compatibility = check_compatiblity(new_lock);
                if (compatibility.can_be_granted) {
                    // This shouldn't happen, but as a safety net
                    DBGOUT0(<<"Umm? Now can be granted. No one got us aware of this.");
                    xct->blocker = NULL;
                    xct->state = RawXct::ACTIVE;
                    atomic_synchronize_if_mutex();
                    return w_error_ok;
                } else if (compatibility.deadlocked) {
                    ERROUT(<<"Deadlock found by myself!");
                    xct->blocker = NULL;
                    atomic_synchronize_if_mutex();
                    return eDEADLOCK;
                }
            } else {
                DBGOUT3(<<"Now it's granted!");
                w_assert1(xct->state == RawXct::ACTIVE);
                w_assert1(xct->blocker == NULL);
                return w_error_ok;
            }
        }
        DBGOUT1(<<"Lock timeout!");
        xct->blocker = NULL;
        return eLOCKTIMEOUT;
#endif // PURE_SPIN_RAWLOCK
    } else {
        DBGOUT1(<<"Interesting. Re-check after barrier tells that now we are granted");
        xct->blocker = NULL; // A24'
        xct->state = RawXct::ACTIVE; // A24
        new_lock->state = RawLock::ACTIVE;
        atomic_synchronize_if_mutex(); // A25
        return w_error_ok;
    }
}

void RawLockQueue::update_xlock_tag(const lsn_t& commit_lsn) {
    if (commit_lsn <= x_lock_tag) {
        return;
    }
    DBGOUT4(<<"CAS to update x_lock_tag... cur=" << x_lock_tag.data() << " to "
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
    DBGOUT4(<<"Done cur=" << x_lock_tag.data());
}

void RawLockQueue::release(RawLock* lock, const lsn_t& commit_lsn) {
    w_assert1(!head.next.is_null());
    w_assert1(!head.next.is_marked());
    RawXct *xct = lock->owner_xct;
    xct->blocker = NULL;
    DBGOUT4(<<"Releasing lock=" << *lock << ", xct=" << *xct);
    if (lock->mode.contains_dirty_lock() && commit_lsn > x_lock_tag) {
        update_xlock_tag(commit_lsn);
    }

    lock->state = RawLock::OBSOLETE; // R2
    atomic_synchronize(); // R3

    w_assert1(!head.next.is_null());
    // pure spin implementation doesn't have to do ANYTHING in release. MUCH faster!
#ifndef PURE_SPIN_RAWLOCK
    // we can ignore locks before us. So, start from lock.
    uint32_t hash = lock->hash;
    bool must_retry = false;
    do {
        must_retry = false;
        for (Iterator iterator(this, lock); !must_retry && !iterator.is_null();
                iterator.next(must_retry)) {
            RawLock* next = iterator.current.get_pointer();
            w_assert1(next != lock);
            if (next->hash != hash || next->owner_xct == xct) {
                continue;
            }
            // R4-5
            // Unlike [JUNG13], we don't lock mutex of next->owner_xct here before
            // signaling. We don't need it because next->owner_xct side would check
            // if its sleep is done or not by seeing the state, and sleep again if not yet.
            // Instead, we have to be careful when we change its state to RawXct::ACTIVE
            // without false-acquisition and to RawXct::WAITING without infinite-sleep.
            DBGOUT3(<<"Do I have to wake up this guy? " << *next);
            // Does releasing the lock free him up?
            if (next->state == RawLock::WAITING) { // R6
                Compatibility compatibility = check_compatiblity(next); // R7
                if (!compatibility.can_be_granted && !compatibility.deadlocked) {
                    // then keep waiting
                    DBGOUT3(<<"No, I don't");
                } else {
                    if (compatibility.deadlocked) {
                        // deadlock!!
                        DBGOUT1(<<"Deadlock detected while unlocking.");
                        next->owner_xct->deadlock_detected_by_others = true;
                    } else {
                        // now granted!!
                        DBGOUT3(<<"Now granted while unlocking.");
                        next->state = RawLock::ACTIVE; // R8
                        next->owner_xct->blocker = NULL; // R9'
                        next->owner_xct->state = RawXct::ACTIVE; // R9
                    }
                    atomic_synchronize();
                    ::pthread_cond_broadcast(&next->owner_xct->lock_wait_cond);
                }
            }
        }
    } while (must_retry);
#endif // PURE_SPIN_RAWLOCK

    // Remove the (obsolete) lock from queue immediately. This is different from [JUNG13]
    // [JUNG13] also has to do this even with garbage collector. otherwise they can't safely
    // call "next_pointer_update()" itself. pazzles me.
    w_assert1(!head.next.is_null());
    bool logically_deleted = false;
    int retry_count = 0;
    while (true) {
        w_assert1(logically_deleted || !head.next.is_null());
        w_assert1(!head.next.is_marked());
        RawLock* predecessor = find_predecessor(lock);
        if (predecessor == NULL) {
            if (logically_deleted) {
                // this is totally possible
                DBGOUT2(<<"Interesting. 2nd step of deletion was taken care of other thread");
                break;
            } else {
                // possible with concurrent deletion. try again.
                // but, if it keeps happening even after mfence and many retries, probably bug.
                if (retry_count > 5) {
                    DBGOUT0(<<"The lock we are deleting now isn't found in the queue. phantom"
                        << ", retry_count=" << retry_count
                        << ". lock=" << *lock << ", queue=" << *this << ", me=" << *xct);
                }
                atomic_synchronize();
                ++retry_count;
                continue;
            }
        }
        // 2-step deletions same as LockFreeList.
        // One simplification is that we don't have to worry about ABA or SEGFAULT.
        // The same lock object (pointer address) never appears again because
        // RawLock objects are maintained by the GC which never recycles.
        RawLock* successor = lock->next.get_pointer();
        if (lock->next.is_marked()
            || lock->next.atomic_cas(successor, successor, false, true, 0, 0)) {
            // CAS succeeded. now really delink it.
            logically_deleted = true;
            if (delink(predecessor, lock, successor)) {
                break; // done
            } else {
                // then, we have to retry. unlike the LockFreeList, we can't just leave it
                // and let the following traversals to delete because it might be garbage
                // collected!
                atomic_synchronize();
                continue;
            }
        } else {
            // CAS failed. start over.
            DBGOUT1(<<"Interesting. race in delinking while deletion");
            continue;
        }
    }

    xct->deallocate_lock(lock);
}

bool RawLockQueue::trigger_UNDO(Compatibility& compatibility)
{
    // This helper function is called by RawLockQueue::acquire() which performs
    // initial lock acquire (not retry), it is called after the initial call to check_compatibility(),
    // based on returned 'compatibility', if the request lock was not granted and if we
    // are in on_demand or mixed restart, check whether the blocker txn is a loser txn or not
    // if blocker is a loser transaction and it is not in the middle of rolling back, then
    // perform the on_demand UNDO for this loser transaction
    // Return true if an on_demand UNDO operation was triggered and completed

    if ((compatibility.deadlocked) || (!compatibility.can_be_granted))
    {
        // Cannot grant the requested lock
        // Handle on_demand UNDO if necessary
        w_assert1(NULL != compatibility.blocker);

            // If using lock re-acquisition for restart concurrency control
                DBGOUT3(<< "RawLockQueue::trigger_UNDO(): on_demand or mixed UNDO with loc, check for loser transaction...");

                // If we did not find any loser transaction in the transaction table
                // previously, fast return
                // this is because loser transactions were inserted into transaction
                // during Log Analysis phase only, once the system is opened for
                // user transactions, we will not create more loser transactions
                // into transaction table
                if ( NO_LOSER_COUNT == loser_count)
                {
                    DBGOUT3( << "RawLockQueue::trigger_UNDO: skip due to no loser transaction found in transaction table previously");
                    return false;
                }

                DBGOUT3( << "RawLockQueue::trigger_UNDO: Is the blocking transaction a loser transaction???");

                // Using lock re-acquisition and on_demand or mixed UNDO
                // to rollback loser transactions

                // If the transaction which is holding the lock (compatibility.blocker)
                // is a loser transaction and not in the middle of rolling back currently,
                // then attach to the loser transaction, change the loser transaction state
                // to 'rolling back' state and trigger the rollback of the loser transaction
                // During this operation, the caller transaction is blocked

                // If the transaction which is holding the lock (compatibility.blocker)
                // is a loser transaction but it is in rolling back state already (another
                // user transaction has triggered the UNDO operation), fall through
                // and no-op

                // If the transaction which is holding the lock is not a loser transaction
                // fall through and no-op

                // Note all we have is a RawXct, which is a member of xct_core and
                // we don't have an easy way to identify the owning xct_t object
                // Loop through the transaction table to find the owner of the RawXct first

                xct_i iter(false); // not locking the transaction table list, we only need to
                                   // look at existing transactions for loser transactions,
                                   // no need to look into the new incoming transactions
                xct_t* xd = 0;
                xd = iter.next();
                bool has_loser = false;  // true if we found at least one loser transaction in transaction table
                while (xd)
                {
                    if (true == xd->is_loser_xct())
                        has_loser = true;

                    if ((xct_t::xct_active == xd->state()) &&           // Active txn
                        (compatibility.blocker == xd->raw_lock_xct()))  // it is the blocker txn
                    {
                        // Found the blocker transaction
                        if (true == xd->is_loser_xct())
                        {
                            w_assert1(false == xd->is_sys_xct());
                            DBGOUT3( << "RawLockQueue::trigger_UNDO: blocker transaction is a loser transaction, txn: "
                                     << xd->tid());

                            // Acquire latch before checking the loser status
                            try
                            {
                                w_rc_t latch_rc = xd->latch().latch_acquire(LATCH_EX, timeout_t::WAIT_SPECIFIED_BY_XCT);
                                if (latch_rc.is_error())
                                {
                                    // Failed to acquire latch on the transaction
                                    // this is the loser transaction we are looking for but we
                                    // are not able to latch the txn, to be safe we skip this time
                                    // and wait for next opportunity for the on_demand UNDO

                                    if (stTIMEOUT == latch_rc.err_num())
                                    {
                                        // There is a small possibility that another concurrent
                                        // transaction is checking for loser status on the same
                                        // transaction at this moment
                                        // Eat the error and skip UNDO this time, caller must retry
                                        DBGOUT0( << "RawLockQueue::trigger_UNDO: failed to latch the loser txn object to check rollback status,"
                                                << " this is due to latch time out, skip the on_demand UNDO");
                                        return false;
                                    }
                                    else
                                    {
                                        W_FATAL_MSG(fcINTERNAL,
                                                    << "RawLockQueue::trigger_UNDO: failed to latch the loser txn object due to unknown reason,"
                                                    << " this is un-expected error, error out");
                                    }
                                }
                            }
                            catch (...)
                            {
                                // Race condition while the transaction is being destroyed?
                                // Skip UNDO
                                return false;
                            }

                            if (false == xd->is_loser_xct_in_undo())
                            {
                                // Loser transaction has not been rollback at this point,
                                // we are the first user transaction to trigger the UNDO
                                // set the loser transaction state first to indicate the current
                                // thread is handling the loser transaction UNDO which is a
                                // blocking operation

                                xd->set_loser_xct_in_undo();
                                if (xd->latch().held_by_me())
                                    xd->latch().latch_release();

                                // Only one transaction may be attached to a thread at any time
                                // The current running thread has the user transaction so
                                // it cannot attach to the loser transaction without detach from the
                                // user transaction first
                                xct_t* user_xd = smthread_t::xct();
                                if (user_xd) {
                                    smthread_t::detach_xct(user_xd);
                                }

                                // The blocker txn is a loser transaction and it is not in the middle of rolling back
                                DBGOUT3( << "RawLockQueue::trigger_UNDO: blocker loser transaction needs UNDO,"
                                         << " user txn: " << user_xd->tid() << ", loser txn: " << xd->tid()
                                         << ", detached from user transaction, start UNDO of loser transaction");

                                // Attach to the loser transaction
                                smthread_t::attach_xct(xd);
                                W_COERCE(xct_t::abort());

                                // Done with rollback of loser transaction, destroy it
                                delete xd;

                                DBGOUT3( << "RawLockQueue::trigger_UNDO: blocker loser transaction successfully aborted,"
                                         << "re-attach to the original user transaction");

                                // Re-attach to the original user transaction
                                if (user_xd) {
                                    smthread_t::attach_xct(user_xd);
                                }

                                // Notify caller an on_demand UNDO has been performed
                                return true;
                            }
                            else
                            {
                                if (xd->latch().held_by_me())
                                    xd->latch().latch_release();

                                // The blocker txn is a loser transaction but it is in the middle of rolling back already
                                DBGOUT3( << "RawLockQueue::trigger_UNDO: blocker loser transaction already in the middle of UNDO");
                                return false;
                            }
                        }
                        else
                        {
                            DBGOUT3( << "RawLockQueue::trigger_UNDO: blocker transaction is not a loser transaction");
                            // Do not exist, continue looping through the remaining items in transaction table
                            // because if we do not find any loser transactions in the transaction table
                            // set the static loser count so we can safely skip this function in the future
                        }
                    }
                    else
                    {
                        // No match, continue the search
                    }

                    // Fetch next transaction object
                    xd = iter.next();
                }

                // Done with the loop through transaction table and did not find a match
                // If we did not find any loser transaction in transaction table
                // set the static loser count to 0 so we will skip
                // this expensive lookup loop in the future
                if (false == has_loser)
                {
                    DBGOUT3(<< "RawLockQueue::trigger_UNDO(): no existing loser transaction in transaction table");
                    loser_count = NO_LOSER_COUNT;
                }
                else
                {
                    DBGOUT3(<< "RawLockQueue::trigger_UNDO(): have more loser transactions in transaction table");
                }

    }
    else
    {
        // Lock was granted, no-op
    }

    // On_demand UNDO did not happen
    DBGOUT3(<< "RawLockQueue::trigger_UNDO(): on_demand UNDO did not happen in this function");
    return false;
}


////////////////////////////////////////////////////////////////////////////////////////
////
//// RawXct and its related class Implementation BEGIN
////
////////////////////////////////////////////////////////////////////////////////////////

void RawXct::init(gc_thread_id thread_id_arg,
        GcPoolForest<RawLock>* lock_pool_arg, gc_pointer_raw* lock_pool_next_arg) {
    thread_id = thread_id_arg;
    lock_pool = lock_pool_arg;
    lock_pool_next = lock_pool_next_arg;
    state = RawXct::ACTIVE;
    deadlock_detected_by_others = false;
    blocker = NULL;
    read_watermark = lsn_t::null;
    private_first = NULL;
    private_last = NULL;
#ifndef PURE_SPIN_RAWLOCK
    ::pthread_mutex_init(&lock_wait_mutex, NULL);
    ::pthread_cond_init(&lock_wait_cond, NULL);
#endif // PURE_SPIN_RAWLOCK
}

void RawXct::uninit() {
    state = RawXct::UNUSED;
    blocker = NULL;
#ifndef PURE_SPIN_RAWLOCK
    ::pthread_mutex_destroy(&lock_wait_mutex);
    ::pthread_cond_destroy(&lock_wait_cond);
#endif // PURE_SPIN_RAWLOCK
}

// for assertion only.
// this is quite expensive (insert_100K test takes 2 minutes). thus should be level 4.
bool is_private_list_consistent(RawXct *xct) {
    std::set<RawLock*> dup;
    RawLock* last_observed = NULL;
    for (RawLock* lock = xct->private_first; lock != NULL; lock = lock->xct_next) {
        if (dup.find(lock) != dup.end()) {
            w_assert1(false);
            return false;
        }
        dup.insert(lock);
        last_observed = lock;
    }
    if (xct->private_first == NULL) {
        if (dup.size() != 0 || xct->private_last != NULL) {
            w_assert1(false);
            return false;
        }
    } else {
        if (xct->private_last == NULL) {
            if (dup.size() != 1 || xct->private_first != last_observed) {
                w_assert1(false);
                return false;
            }
        } else if (xct->private_last == xct->private_first) {
            w_assert1(false);
            return false;
        } else if (xct->private_last != last_observed) {
            w_assert1(false);
            return false;
        }
    }
    return true;
}

RawLock* RawXct::allocate_lock(uint32_t hash, const okvl_mode& mode, RawLock::LockState state) {
    RawLock* lock = lock_pool->allocate(*lock_pool_next, thread_id);
    lock->hash = hash;
    lock->mode = mode;
    lock->owner_xct = this;
    lock->next = NULL_RAW_LOCK;
    lock->state = state;

    w_assert4(is_private_list_consistent(this));
    // put it on transaction-private linked list
    if (private_first == NULL) {
        // was empty
        w_assert1(private_last == NULL);
        private_first = lock;
        lock->xct_previous = NULL;
        lock->xct_next = NULL;
    } else if (private_last == NULL) {
        // only one lock
        w_assert1(private_first->xct_next == NULL);
        private_last = lock;
        lock->xct_previous = private_first;
        private_first->xct_next = lock;
        lock->xct_next = NULL;
    } else {
        // else, we put it in last
        w_assert1(private_last->xct_previous != NULL);
        w_assert1(private_last->xct_next == NULL);
        lock->xct_previous = private_last;
        private_last->xct_next = lock;
        private_last = lock;
    }
    w_assert4(is_private_list_consistent(this));

    // and transaction-private hashmap
    private_hash_map.push_front(lock);
    return lock;
}

void RawXct::deallocate_lock(RawLock* lock) {
    // remove from transaction-private linked list
    w_assert1(this == lock->owner_xct);
    w_assert1(private_first != NULL);
    w_assert4(is_private_list_consistent(this));
    if (private_last == NULL) {
        // was the only entry
        w_assert1(private_first->xct_next == NULL);
        w_assert1(lock->xct_previous == NULL);
        w_assert1(lock->xct_next == NULL);
        private_first = NULL;
    } else if (lock == private_first) {
        // was the head
        w_assert1(lock->xct_previous == NULL);
        w_assert1(lock->xct_next != NULL);
        private_first = lock->xct_next;
        private_first->xct_previous = NULL;
    } else if (lock == private_last) {
        // was the tail
        w_assert1(lock->xct_previous != NULL);
        w_assert1(lock->xct_next == NULL);
        lock->xct_previous->xct_next = NULL;
        private_last = lock->xct_previous;
    } else {
        w_assert1(lock->xct_previous != NULL);
        w_assert1(lock->xct_next != NULL);
        lock->xct_previous->xct_next = lock->xct_next;
        lock->xct_next->xct_previous = lock->xct_previous;
    }
    if (private_first == private_last) {
        private_last = NULL;
    }
    w_assert4(is_private_list_consistent(this));

    // and from transaction-private hashmap
    private_hash_map.remove(lock);

    lock_pool->deallocate(lock);
}

bool RawXct::is_deadlocked(RawXct* first_blocker) {
    w_assert1(first_blocker != NULL);
    w_assert1(first_blocker != this);
#ifndef PURE_SPIN_RAWLOCK
    // It's possible that block->block is an infinite loop WITHOUT seeing myself.
    // eg. me -> Xct-A -> Xct-B -> Xct-A -> ....
    // however, std::set for such a thing would be too costly.
    // Let's assume there aren't cycle more than 16 deep. If there is, deadlock-abort.
    const int MAX_DEPTH = 16;
    int depth = 0;
    RawXct* observed[MAX_DEPTH];
#endif // PURE_SPIN_RAWLOCK
    for (RawXct* next = first_blocker; next != NULL; next = next->blocker) {
        if (next->blocker == NULL) {
            return false;
        }
        if (next->blocker == this) {
            return true;
        }
#ifndef PURE_SPIN_RAWLOCK
        for (int i = 0; i < depth; ++i) {
            if (observed[i] == next) {
                DBGOUT1(<<"Not myself as joint point of deadlock, but found someone else's");
                next->deadlock_detected_by_others = true;
                atomic_synchronize();
                ::pthread_cond_broadcast(&next->lock_wait_cond);
            }
        }
        if (depth >= MAX_DEPTH) {
            ERROUT(<<"Very deep wait-for loop. Assume there is a deadlock");
            return true;
        }
        observed[depth] = next;
        ++depth;
#endif // PURE_SPIN_RAWLOCK
    }
    return false;
}

RawXctLockHashMap::RawXctLockHashMap() {
    reset();
}
RawXctLockHashMap::~RawXctLockHashMap() {
}
void RawXctLockHashMap::reset() {
    ::memset(_buckets, 0, sizeof(RawLock*) * RAW_XCT_LOCK_HASHMAP_SIZE);
}

okvl_mode RawXctLockHashMap::get_granted_mode(uint32_t lock_id) const {
    uint32_t bid = _bucket_id(lock_id);
    // we don't take any latch here. See the comment of RawXctLockHashMap
    // for why this is safe.
    okvl_mode ret(ALL_N_GAP_N);
    for (const RawLock *current = _buckets[bid]; current != NULL;
         current = current->xct_hashmap_next) {
        if (current->hash == lock_id && current->state == RawLock::ACTIVE) {
            // we don't upgrade locks any more, so we can have multiple lock entries
            // for the same resource. we take OR of them.
            ret = okvl_mode::combine(ret, current->mode);
        }
    }
    return ret;
}

void RawXctLockHashMap::push_front(RawLock* link) {
    // link becomes a new head of the bucket
    uint32_t bid = _bucket_id(link->hash);
    link->xct_hashmap_previous = NULL;
    link->xct_hashmap_next = _buckets[bid];
    if (_buckets[bid] != NULL) {
        _buckets[bid]->xct_hashmap_previous = link;
    }
    _buckets[bid] = link;
}
void RawXctLockHashMap::remove(RawLock* link) {
    if (link->xct_hashmap_next != NULL) {
        link->xct_hashmap_next->xct_hashmap_previous = link->xct_hashmap_previous;
    }
    if (link->xct_hashmap_previous != NULL) {
        link->xct_hashmap_previous->xct_hashmap_next = link->xct_hashmap_next;
    } else {
        // "link" was the head.
        uint32_t bid = _bucket_id(link->hash);
        w_assert1(_buckets[bid] == link);
        _buckets[bid] = link->xct_hashmap_next;
    }
}

////////////////////////////////////////////////////////////////////////////////////////
////
//// RawLockBackgroundThread Implementation BEGIN
////
////////////////////////////////////////////////////////////////////////////////////////

RawLockBackgroundThread::RawLockBackgroundThread(const sm_options& options,
    GcPoolForest< RawLock >* lock_pool, GcPoolForest< RawXct >* xct_pool) {
    _stop_requested = false;
    _running = false;
    _dummy_lsn_lock = 1000;
    _dummy_lsn_xct = 1000;
    _lock_pool = lock_pool;
    _xct_pool = xct_pool;

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

    _internal_milliseconds = options.get_int_option("sm_rawlock_gc_interval_ms", 3);
    _lockpool_segsize = options.get_int_option("sm_rawlock_lockpool_segsize", 1 << 12);
    _xctpool_segsize = options.get_int_option("sm_rawlock_xctpool_segsize", 1 << 8);
    _generation_count = options.get_int_option("sm_rawlock_gc_generation_count", 5);
    _init_generation_count = options.get_int_option("sm_rawlock_gc_init_generation_count", 5);
    _lockpool_initseg = options.get_int_option("sm_rawlock_lockpool_initseg", 255);
    _xctpool_initseg = options.get_int_option("sm_rawlock_xctpool_initseg", 255);
    _free_segment_count = options.get_int_option("sm_rawlock_gc_free_segment_count", 50);
    _max_segment_count = options.get_int_option("sm_rawlock_gc_max_segment_count", 255);

    DO_PTHREAD(::pthread_mutex_init(&_interval_mutex, NULL));
    DO_PTHREAD(::pthread_cond_init (&_interval_cond, NULL));
    DO_PTHREAD(::pthread_attr_init(&_join_attr));
    DO_PTHREAD(::pthread_attr_setdetachstate(&_join_attr, PTHREAD_CREATE_JOINABLE));
}
RawLockBackgroundThread::~RawLockBackgroundThread() {
    DO_PTHREAD(::pthread_attr_destroy(&_join_attr));
    DO_PTHREAD(::pthread_cond_destroy(&_interval_cond));
    DO_PTHREAD(::pthread_mutex_destroy(&_interval_mutex));
}

void RawLockBackgroundThread::start() {
    DBGOUT1(<<"Starting RawLockBackgroundThread...");
    if (_running) {
        ERROUT(<<"RawLockBackgroundThread already running. ignored.");
        return;
    }
    atomic_synchronize();
    DO_PTHREAD(::pthread_create(&_thread, NULL, RawLockBackgroundThread::pthread_main, this));
}

void* RawLockBackgroundThread::pthread_main(void* t) {
    RawLockBackgroundThread* obj = reinterpret_cast<RawLockBackgroundThread*>(t);
    w_assert1(obj);
    obj->_running = true;
    atomic_synchronize();
    DBGOUT1(<<"RawLockBackgroundThread thread routine entered");

    obj->run_main();

    DBGOUT1(<<"RawLockBackgroundThread thread ended");
    obj->_running = false;
    atomic_synchronize();
    ::pthread_exit(NULL);
    return NULL;
}

/**
 * Main implementation of run_main().
 * Templatized to handle both pools. Can't be a class member without putting these dirty
 * details in header file. C++ sucks, though other languages suck more.
 */
template <class T>
void handle_pool(bool &more_work,
                 bool &stop_requested,
                 GcPoolForest<T>* pool,
                 const char *name,
                 uint32_t generation_count,
                 uint32_t free_segment_count,
                 uint32_t max_segment_count,
                 uint32_t init_segment_count,
                 uint32_t segment_size,
                 int &dummy_lsn) {
    w_assert3(name != NULL);
    DBGOUT1(<< name << "handle_pool start.");
    // Pre allocate segments
    atomic_synchronize();
    if (!stop_requested && pool->curr_generation()->get_free_count() < free_segment_count) {
        DBGOUT1(<< name << "Current generation (" << pool->curr_nowrap << ") has too few free"
            << "  segments. allocated=" << pool->curr_generation()->allocated_segments
            << "/" << pool->curr_generation()->total_segments << ". Preallocating..");
        uint32_t more_segments = free_segment_count;
        if (more_segments + pool->curr_generation()->total_segments > max_segment_count) {
            more_segments = max_segment_count - pool->curr_generation()->total_segments;
        }
        pool->curr_generation()->preallocate_segments(
            free_segment_count, segment_size);
    }

    // Advance generation
    atomic_synchronize();
    if (!stop_requested && pool->curr_generation()->get_free_count() < free_segment_count
            && pool->curr_generation()->total_segments >= max_segment_count) {
        DBGOUT1(<< name << "Current generation (" << pool->curr_nowrap << ")  can't add more"
            << " segments. current allocated=" << pool->curr_generation()->allocated_segments
            << "/" << pool->curr_generation()->total_segments
            << " new gen segments will be:" << init_segment_count);
        lsn_t low_water_mark; // try recycling
        lsn_t now;
        if (smlevel_0::log == NULL) {
            DBGOUT1(<< name << "There is no log manager. Immitating ");
            low_water_mark.set(dummy_lsn - generation_count + 1);
            now.set(++dummy_lsn);
        } else {
            low_water_mark = smlevel_0::log->get_oldest_lsn_tracker()->get_oldest_active_lsn(
                smlevel_0::log->curr_lsn());
            now = smlevel_0::log->curr_lsn();
        }
        bool ret = pool->advance_generation(low_water_mark, now, init_segment_count, segment_size);
        w_assert1(ret);
    }

    // Retire generations
    atomic_synchronize();
    if (!stop_requested && pool->active_generations() > generation_count) {
        DBGOUT1(<< name << "There are too many generations retiring some. head_nowrap="
            << pool->head_nowrap << ", tail_nowrap=" << pool->tail_nowrap
            << ", desired generation_count=" << generation_count);
        lsn_t low_water_mark;
        if (smlevel_0::log == NULL) {
            DBGOUT1(<< name << "There is no log manager. Immitating low-water-mark"
                << ". dummy_lsn=" << dummy_lsn);
            low_water_mark.set(dummy_lsn - generation_count + 1);
        } else {
            DBGOUT1(<< name << "Retrieving low water mark...");
            low_water_mark = smlevel_0::log->get_oldest_lsn_tracker()->get_oldest_active_lsn(
                smlevel_0::log->curr_lsn());
            DBGOUT1(<< name << "Retrieved low water mark=" << low_water_mark.data()
                << "(curr_lsn=" << smlevel_0::log->curr_lsn().data()
                << ", durable_lsn=" << smlevel_0::log->durable_lsn().data() << ")");
        }
        pool->retire_generations(low_water_mark);
    }

    atomic_synchronize();
    if (!stop_requested) {
        if (pool->curr_generation()->get_free_count() < free_segment_count
            || pool->active_generations() > generation_count) {
            more_work = true;
        }
    }
    DBGOUT1(<< name << "handle_pool end. more_work? " << more_work);
}

void RawLockBackgroundThread::run_main() {
    while (!_stop_requested) {
        atomic_synchronize();
        bool more_work = false; // do we have more work without sleep?
        handle_pool<RawLock>(more_work, _stop_requested, _lock_pool, "LockPool:",
            _generation_count, _free_segment_count, _max_segment_count,
            _lockpool_initseg, _lockpool_segsize, _dummy_lsn_lock);
        handle_pool<RawXct>(more_work, _stop_requested, _xct_pool, "XctPool:",
            _generation_count, _free_segment_count, _max_segment_count,
            _xctpool_initseg, _xctpool_segsize, _dummy_lsn_xct);

        // let's sleep.
        atomic_synchronize();
        if (_internal_milliseconds > 0 && !_stop_requested && !more_work) {
            DO_PTHREAD(::pthread_mutex_lock(&_interval_mutex));
            DBGOUT1(<<"RawLockBackgroundThread interval=" << _internal_milliseconds);
            struct timeval now;
            struct timespec timeout;
            ::gettimeofday(&now, NULL);
            timeout.tv_sec = now.tv_sec + _internal_milliseconds / 1000;
            timeout.tv_nsec = now.tv_usec * 1000
                + (_internal_milliseconds % 1000) * 1000000;
            if (timeout.tv_nsec >= 1000000000LL) {
                w_assert1(timeout.tv_nsec < 2000000000LL);
                timeout.tv_nsec -= 1000000000LL;
                ++timeout.tv_sec;
            }
            int ret = ::pthread_cond_timedwait(&_interval_cond, &_interval_mutex, &timeout);
            DBGOUT1(<<"RawLockBackgroundThread after interval! ret=" << ret);
            (void)ret; // suppress compiler warning in release
            DO_PTHREAD(::pthread_mutex_unlock(&_interval_mutex));
        }
    }
}

void RawLockBackgroundThread::stop_synchronous() {
    DBGOUT1(<<"Stopping RawLockBackgroundThread...");
    if (_stop_requested) {
        ERROUT(<<"Already requested.");
        return;
    }
    _stop_requested = true;
    atomic_synchronize();
    if (!_running) {
        DBGOUT1(<<"Already stopped.");
        return;
    }
    DO_PTHREAD(::pthread_mutex_lock(&_interval_mutex));
    int ret_cond = ::pthread_cond_broadcast(&_interval_cond);
    DO_PTHREAD(::pthread_mutex_unlock(&_interval_mutex));
    DBGOUT1(<<"Noticed RawLockBackgroundThread. ret=" << ret_cond << ". joining..");
    void *join_status;
    int ret_join = ::pthread_join(_thread, &join_status);
    DBGOUT1(<<"Joined RawLockBackgroundThread thread. done. ret=" << ret_join);
    w_assert1(!_running);
    (void)ret_join; // suppress compiler warning in release
    (void)ret_cond; // suppress compiler warning in release
}
void RawLockBackgroundThread::wakeup() {
    DBGOUT1(<<"Waking up RawLockBackgroundThread...");
    if (_running) {
        DO_PTHREAD(::pthread_mutex_lock(&_interval_mutex));
        int ret = ::pthread_cond_broadcast(&_interval_cond);
        DO_PTHREAD(::pthread_mutex_unlock(&_interval_mutex));
        DBGOUT1(<<"Woke up RawLockBackgroundThread. ret=" << ret);
        (void)ret; // suppress compiler warning in release
    } else {
        DBGOUT0(<<"The thread is not running.");
    }
}

////////////////////////////////////////////////////////////////////////////////////////
////
//// Dump methods
////
////////////////////////////////////////////////////////////////////////////////////////

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

    o << " mode=" << v.mode << ", owner_xct=" << v.owner_xct->thread_id;
    return o;
}

std::ostream& operator<<(std::ostream& o, const RawLockQueue& v) {
    o << "RawLockQueue x_lock_tag=" << v.x_lock_tag.data() << " locks:" << std::endl;
    for (MarkablePointer<RawLock> lock = v.head.next; !lock.is_null(); lock = lock->next) {
        RawLock *pointer = lock.get_pointer();
        o << *pointer << std::endl;
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, const RawXct &v) {
    o << "Transaction: xct-" << v.thread_id;
    if (v.blocker != NULL) {
        o << " blocked by " << v.blocker->thread_id;
    }
    o << std::endl << "Transaction-private Lock list" << std::endl;
    for (RawLock *lock = v.private_first; lock != NULL; lock = lock->xct_next) {
        o << *lock << std::endl;
    }
    return o;
}

void RawXct::dump_lockinfo(std::ostream& o) const {
    o << *this;
}
