/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#ifndef LOCK_RAW_H
#define LOCK_RAW_H

/**
 * \defgroup RAWLOCK
 * \brief \b RAW-style \b Lock \b Manager
 * \ingroup SSMLOCK
 * \details
 * Classes implementing \e Read-After-Write (RAW) style lock manager proposed by Jung et al
 * [JUNG13]. This lock manager reduces mutex enter/release and the length of critical
 * sections by employing RAW-style algorithm as much as possible.
 *
 * \section TERM Terminology
 * In this code, we avoid using the often misused term "lock-free" because it's confusing
 * especially in our context (lock manager). Instead, we use "RAW-style" to mean that we
 * reduce blocking as much as possible. We say "lock-free" only when it's really lock-free.
 *
 * Remember, the canonical meaning of "lock-free" is "some call always finishes in a finite
 * number of steps" [HERLIHY]. Lock manager is inherently not such an existence.
 *
 * \section HIERARCHY Lock queues and lock entries
 * Shore-MT's lock manager had a three-level hierarchy; bucket with hash modulo,
 * queue with precise hash, and each lock.
 * In [JUNG13], a bucket is a queue. This might cause more hash collisions, but adding
 * another level in RAW-style algorithm is quite tricky (eg, how and when we can safely
 * delete the queue?).
 * So, here we do something in-between.
 *
 * Bucket is a queue. However, each lock also contains the precise hash in the member.
 * When lock-compatibility methods traverse the list, we ignore locks that have different
 * hashes. This gives the same concurrency and yet does not cause 3-level complexity.
 *
 * \section ALGO Algorithm Overview
 * In short, [JUNG13] proposes an RAW-style singly-linked list (based on well known lock-free
 * singly-linked list) as lock queue and a garbage collector for it based on generation
 * versioning. Sounds easy?
 *
 * There are a few complications and simplifications, though.
 *   \li When we have to really wait (conflicting locks), there is no option other than
 * waiting with mutex. So, this is not really a lock-free list.
 *   \li We never \e insert into lock queue, rather we always \e append. This makes many
 * operations easier than the original lock-free linked list.
 *   \li There are several cases we can tolerate false positives (which only results in
 * conservative executions) and even some false negatives (see check_only param of
 * RawLockQueue#acquire()).
 *
 * \section DIFF Differences from [JUNG13]
 * We made a few changes from the original algorithm.
 * The first one is, as described above, we put precise hash in each lock and ignore
 * locks that have different hashes during checks.
 *
 * Another difference is that we don't use "OBSOLETE flag" to disable a lock.
 * We rather immediately delete the lock from the queue. This is still safe because
 * we use delayed garbage collection. No dangling pointer possible.
 * Also, to safely remove a lock in a safe way, we do exactly same as LockFreeList.
 * So, we don't have "tail" as a member in RawLockQueue and traverse the list with
 * checking mark-for-death bit. See Chap 9.8 in [HERLIHY].
 *
 * \section REF References
 *   \li [JUNG13] "A scalable lock manager for multicores"
 *   Hyungsoo Jung, Hyuck Han, Alan D. Fekete, Gernot Heiser, Heon Y. Yeom. SIGMOD'13.
 *   \li Also see \ref MARKPTR, [MICH02], and [HERLIHY].
 */

#include <stdint.h>
#include <ostream>
#include <pthread.h>
#include "w_defines.h"
#include "w_okvl.h"
#include "w_rc.h"
#include "w_gc_pool_forest.h"
#include "w_lockfree_list.h"
#include "w_markable_pointer.h"
#include "lsn.h"

struct RawLockBucket;
struct RawLockQueue;
struct RawLock;
struct RawXct;

/**
 * \brief An RAW-style lock entry in the queue.
 * \ingroup RAWLOCK
 * \details
 * Unlike the original Shore-MT lock manager, this lock entry does not have the notion of
 * "upgrading" state. When we need more permissions in the key, we just create another
 * lock entry. Thus, we might have multiple lock entries from one transaction in one queue.
 * This enables the RAW-style lock queue optimizations.
 */
struct RawLock : public GcPoolEntry {
    enum LockState {
        /** This lock is in the pool and not used in any queue. */
        UNUSED = 0,
        /** This lock exists in a queue, but others can skip over or remove it. */
        OBSOLETE,
        /** This lock is granted and other transactions must respect this lock. */
        ACTIVE,
        /** This lock is not granted and waiting for others to unlock. */
        WAITING,
    };

    RawLock() : hash(0), state(UNUSED), owner_xct(NULL) {}

    /** Precise hash of the protected resource. */
    uint32_t                    hash;

    /** Current status of this lock. */
    LockState                   state;

    /** Constitutes a singly-linked list. */
    MarkablePointer<RawLock>    next;

    /** owning xct. */
    RawXct*                     owner_xct;

    /** Requested lock mode. */
    okvl_mode                   mode;
};
std::ostream& operator<<(std::ostream& o, const RawLock& v);

/** Const object representing NULL RawLock. */
const MarkablePointer<RawLock> NULL_RAW_LOCK;

/**
 * \brief An RAW-style lock queue to hold granted and waiting lock requests (RawLock).
 * \ingroup RAWLOCK
 * \details
 * Queue is a bucket. So, this queue can contain locks for resources with different hashes.
 * However, each lock also contains the precise hash in the member.
 * When lock-compatibility methods traverse the list, we ignore locks that have different
 * hashes. This gives the same concurrency and yet does not cause 3-level complexity.
 */
struct RawLockQueue : public GcPoolEntry {
    /**
     * \brief Adds a new lock in the given mode to this queue, waiting until it is granted.
     * @param[in] xct the transaction to own the new lock
     * @param[in] hash precise hash of the resource to lock.
     * @param[in] mode requested lock mode
     * @param[in] timeout_in_ms maximum length to wait in milliseconds. 0 means
     * conditional locking (immediately timeout).
     * @param[in] check_only if true, this method doesn't actually create a new lock object
     * but just checks if the requested lock mode can be granted or not.
     * \details
     * check_only=true can give a false positive in concurrent unlock case, but
     * give no false negative \b assuming a conflicting lock is not concurrently taken for
     * the key. This assumption holds for our only check_only=true use case, which is the
     * tentative NX lock check before inserting a new key, \b because we then have an EX latch!
     * Thus, this is a safe and efficient check for B-tree insertion.
     */
    w_rc_t  acquire(RawXct *xct, uint32_t hash, const okvl_mode& mode,
                    int32_t timeout_in_ms, bool check_only);

    /**
     * \brief Releases the given lock from this queue, waking up others if necessary.
     * @param[in] lock the lock to release.
     * @param[in] commit_lsn LSN to update X-lock tag during SX-ELR.
     */
    void    release(RawLock *lock, const lsn_t &commit_lsn);

    /**
     * \brief Atomically insert the given lock to this queue. Called from acquire().
     * \details
     * See Figure 4 and Sec 3.1 of [JUNG13].
     */
    void    atomic_lock_insert(RawLock *new_lock);

    /**
     * Checks if the given lock can be granted.
     * Called from acquire() after atomic_lock_insert().
     */
    w_rc_t  check_compatiblity(RawLock *new_lock) const;

    /**
     * \brief Used for check_only=true case. Many things are much simpler and faster.
     * \details
     * This is analogous to the wait-free contains() of lock-free linked list in [HERLIHY].
     * As commented in acquire(), this method is safe for check_only case.
     * EX latch on the page guarantees that no lock for the key is newly coming now.
     */
    bool    peek_compatiblity(RawXct* xct, uint32_t hash, const okvl_mode &mode) const;

    /**
     * Sleeps until the lock is granted using mutex.
     * Called from acquire() after check_compatiblity() if the lock was not immediately granted.
     */
    w_rc_t  wait_mutex(RawLock *new_lock, int32_t timeout_in_ms);

    /**
     * \brief Returns the predecessor of the given lock.
     * This removes marked entries it encounters.
     * @return predecessor of the given lock. Never returns NULL, but might be &head.
     */
    RawLock*    find(RawLock *lock) const;

    /**
     * Main routine of find()
     * @return predecessor of the given lock.
     */
    RawLock*    find_retry_loop(RawLock *lock, bool& must_retry) const;

    /**
     * \brief Returns the last entry of this queue.
     * This removes marked entries it encounters, which is the reason why we can't have
     * "tail" as a variable in the queue and instead we have to traverse each time.
     * @return the last entry. Never returns NULL, but might be &head (meaning empty).
     */
    RawLock*    tail() const;

    /**
     * Main routine of tail()
     * @return the last entry.
     */
    RawLock*    tail_retry_loop(bool& must_retry) const;

    /**
     * \brief Delinks a marked entry from the list and deallocates the entry object.
     * @return whether the delink was really done by this thread
     * \details
     * target should be marked for death (target->next.is_marked()), but this is not
     * a contract because of concurrent delinks. If it isn't (say target is already delinked),
     * then the atomic CAS fails, returning false.
     * \NOTE Although might sound weird, this method \e is const.
     * This method \b physically delinks an already-marked entry, so \b logically it does
     * nothing.
     */
    bool        delink(RawLock* predecessor, const MarkablePointer<RawLock> &target,
                   const MarkablePointer<RawLock> &successor) const;

    /**
     * The always-existing dummy entry as head.
     * _head is never marked for death.
     * Mutable because even find() physically removes something (though logically nothing).
     */
    mutable RawLock             head;

    /**
     * Stores the commit timestamp of the latest transaction that released an X lock on this
     * queue; holds lsn_t::null if no such transaction exists; protected by _requests_latch.
     */
    lsn_t                       x_lock_tag;
};

/**
 * \brief A shadow transaction object for RAW-style lock manager.
 * \ingroup RAWLOCK
 * \details
 * Just like [JUNG13], we have additional (shadow) transaction objects that were used from
 * the real transaction objects. The real transaction objects (xct_t) are immediately
 * recycled after commit/abort while this object lives a bit longer until garbage
 * collection kicks in. This is necessary to make sure the RAW style deadlock-detection
 * does not reference an already revoked object.
 * See Section 4.5 of [JUNG13].
 */
struct RawXct {
    enum XctState {
        /** This object is in the pool and not used in any place. */
        UNUSED = 0,
        /** This transaction is running without being blocked so far. */
        ACTIVE,
        /** This transaction is waiting for some other transaction. */
        WAITING,
    };

    void                        init();
    void                        uninit();

    /**
     * \brief Recursively checks for the case where some blocker is this transaction itself.
     * @return true if there is a cycle, meaning deadlock.
     * \details
     * [JUNG13] does not clarify the algorithm to detect deadlocks.
     * We have originally had Dreadlock, the spinning based detection based on fingerprint, but
     * it would cause too many atomic operations from many threads in this RAW-style queue.
     * I assume a traditional recursive check (who is my blocker's blocker's...) is
     * appropriate with [JUNG13] approach.
     * @pre blocker != NULL
     */
    bool                        is_deadlocked();

    void                        update_read_watermark(const lsn_t &tag) {
        if (read_watermark < tag) {
            read_watermark = tag;
        }
    }

    /** Newly allocate a lock object from the object pool. */
    RawLock*                    allocate_lock(uint32_t hash,
                                              const okvl_mode& mode, RawLock::LockState state);

    /**
     * Identifier of the thread running this transaction, eg pthread_self().
     */
    gc_thread_id                thread_id;

    /** Pointer to object pool for RawLock. */
    GcPoolForest<RawLock>*      lock_pool;

    /** Pointer to thread-local allocation hint for lock_pool. */
    gc_pointer_raw*             lock_pool_next;

    /** Whether this transaction is waiting for another transaction. */
    XctState                    state;

    /** Other transaction while unlocking realized that this transaction is deadlocked. */
    bool                        deadlock_detected_while_unlock;

    /** If exists the transaction that is now blocking this transaction. NULL otherwise.*/
    RawXct*                     blocker;

    /** Used to wait in lock manager, paired with lock_wait_mutex. */
    pthread_cond_t              lock_wait_cond;
    /** Used to wait in lock manager, paired with lock_wait_cond. */
    pthread_mutex_t             lock_wait_mutex;

    /**
     * Whenever a transaction acquires some lock,
     * this value is updated as _read_watermark=max(_read_watermark, lock_bucket.tag)
     * so that we maintain a maximum commit LSN of transactions it depends on.
     * This value is used to commit a read-only transaction with Safe SX-ELR to block
     * until the log manager flushed the log buffer at least to this value.
     * Assuming this protocol, we can do ELR for x-locks.
     * See jira ticket:99 "ELR for X-lock" (originally trac ticket:101).
     */
    lsn_t                       read_watermark;
};

#endif // LOCK_RAW_H
