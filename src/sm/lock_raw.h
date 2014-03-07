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
 * \section HIERARCHY Lock buckets, queues, and lock entries
 * In [JUNG13], a bucket is a queue. However, we can't tolerate hash collisions
 * just because of coarser hash in buckets (e.g., if we have only 1024 buckets, we can have
 * only 1024 lock queues). This restricts concurrency, so we still keep the bucket-queue-lock
 * hierarchy in Shore-MT's lock manager. Queues in buckets are allocated/deallocated just like
 * locks in queues. So, the same RAW techniques and object pooling apply.
 *
 * To recap:
 *   \li \e Bucket : For a range of hash. Say Bucket-1 contains locks with "hash%1024==1".
 * Bucket maintains a singly linked-list of \e Queue.
 *   \li \e Queue : For a precise hash. All lock entries in a queue has exactly same 32 bit
 * hashes, strongly implying they are for the same key (unless nasty hash collisions).
 * Queue maintains a singly linked-list of \e Lock.
 *   \li \e Lock : One lock request from a transaction.
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
 * \section REF References
 *   \li [JUNG13] "A scalable lock manager for multicores"
 *   Hyungsoo Jung, Hyuck Han, Alan D. Fekete, Gernot Heiser, Heon Y. Yeom. SIGMOD'13.
 *
 *   \li [HERLIHY] "The Art of Multiprocessor Programming". Maurice Herlihy, Nir Shavit.
 *   (shame if you don't own a copy yet work on multi-core optimization!)
 *
 *   \li Also see \ref MARKPTR.
 */

#include <stdint.h>
#include <pthread.h>
#include "w_defines.h"
#include "w_okvl.h"
#include "w_rc.h"
#include "w_markable_pointer.h"
#include "lsn.h"

struct RawLockBucket;
class RawLockQueue;
struct RawLock;
struct RawLockQueuePimpl;
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
struct RawLock {
    enum LockState {
        /** This lock is in the pool and not used in any queue. */
        UNUSED = 0,
        /** This lock exists in a queue, but others can skip over or remove it. */
        OBSOLETE,
        /** This lock is granted and other transactions must respect this lock. */
        ACTIVE,
        /** This lock is not granted and waiting for others to unlock. */
        WAITING,
        /** Special entry type only for the always-existing queue head. */
        HEAD,
    };

    /** Current status of this lock. */
    LockState                   state;

    /** Constitutes a singly-linked list. Stashed value=DELETE flag of \b this. */
    MarkablePointer<RawLock>    next;

    /** owning xct. */
    RawXct*                     owner_xct;

    /** Requested lock mode. */
    okvl_mode                   mode;
};

/** Const object representing NULL. */
const MarkablePointer<RawLock> NULL_RAW_LOCK(NULL);

/**
 * \brief An RAW-style lock queue to hold granted and waiting lock requests (RawLock).
 * \ingroup RAWLOCK
 * \details
 * A queue corresponds to one precise 32-bit hash value. Even if we have small number of
 * lock buckets, these queues separate locks with different hashes.
 */
class RawLockQueue {
public:
    enum QueueState {
        /** This queue is in the pool and not used in any bucket. */
        UNUSED = 0,
        /** This queue exists in a bucket, but others can skip over or remove it. */
        OBSOLETE,
        /** This queue is being used. */
        ACTIVE,
        /** Special entry type only for the always-existing bucket head. */
        HEAD,
    };

    /**
     * \brief Adds a new lock in the given mode to this queue, waiting until it is granted.
     * @param[in] xct the transaction to own the new lock
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
    w_rc_t  acquire(RawXct *xct, const okvl_mode& mode, int32_t timeout_in_ms, bool check_only);

    /**
     * \brief Releases the given lock from this queue, waking up others if necessary.
     * @param[in] lock the lock to release.
     * @param[in] commit_lsn LSN to update X-lock tag during SX-ELR.
     */
    void    release(RawLock *lock, const lsn_t &commit_lsn);

    /** Returns an identifier of this queue. Called from the common list class. */
    uint32_t key() const { return _hash; }
    /** Returns if this queue is ready for removal. Called from the common list class. */
    bool     is_obsolete() const { return _state == OBSOLETE; }
    /** Returns next queue in the bucket. Called from the common list class. */
    MarkablePointer<RawLockQueue>&  next() { return _next; }
private:

    /**
     * \brief Atomically insert the given lock to this queue. Called from acquire().
     * \details
     * See Figure 4 and Sec 3.1 of [JUNG13].
     */
    void    atomic_lock_insert(RawLock *new_lock);

    /**
     * \brief Removes OBSOLETE entries using lock-free list technique.
     * \details
     * Called from atomic_lock_insert() before check_compatiblity().
     * [JUNG13] is not clear what chapter of [HERLIHY] it refers to, but I think it's Chap 9.8.
     * The reason why we eliminate OBSOLETE entries now, not later, is described in Sec 4.4 of
     * [JUNG13]. In short, if this transaction started after release of some lock,
     * this transaction must skip it for safe garbage collection.
     */
    void    next_pointer_update();

    /**
     * \brief Wait until the given pointer gets a valid next pointer, then return it.
     * \details
     * while traversing, we don't have to worry about other threads concurrently deleting
     * entries because de-linked entry still points to a valid next, eventually covering
     * all entries we are interested in (as, of course, only OBSOLETE entries are removed).
     * However, in case other threads have just inserted and are now setting next pointer,
     * we have to wait. See Figure 5 of [JUNG13].
     */
    MarkablePointer<RawLock>& wait_for_next(RawLock* pointer);

    /**
     * Checks if the given lock can be granted.
     * Called from acquire() after atomic_lock_insert().
     */
    w_rc_t  check_compatiblity(RawLock *new_lock);

    /**
     * \brief Used for check_only=true case. Many things are much simpler and faster.
     * \details
     * This is analogous to the wait-free contains() of lock-free linked list in [HERLIHY].
     * As commented in acquire(), this method is safe for check_only case.
     * EX latch on the page guarantees that no lock for the key is newly coming now.
     */
    bool    peek_compatiblity(RawXct* xct, const okvl_mode &mode);

    /**
     * Sleeps until the lock is granted using mutex.
     * Called from acquire() after check_compatiblity() if the lock was not immediately granted.
     */
    w_rc_t  wait_mutex(RawLock *new_lock, int32_t timeout_in_ms);

    /**
     * The always-existing dummy entry as queue head with HEAD status.
     */
    RawLock                     _head;

    /** The last entry in this queue or NULL if queue empty. Stashed value=OBSOLETE flag. */
    MarkablePointer<RawLock>    _tail;

    /** Constitutes a singly-linked list in bucket. Stashed value=DELETE flag of \b this. */
    MarkablePointer<RawLockQueue>   _next;

    /** precise hash for this lock queue. */
    uint32_t                    _hash;

    /** For garbage collection of queue object. */
    QueueState                  _state;

    /**
     * Stores the commit timestamp of the latest transaction that released an X lock on this
     * queue; holds lsn_t::null if no such transaction exists; protected by _requests_latch.
     */
    lsn_t                       _x_lock_tag;
};

/**
 * \brief An RAW-style lock bucket for a hash range, holding lock queues (RawLockQueue) in it.
 * \ingroup RAWLOCK
 * \details
 * Each bucket forms a lock-free singly-linked list of queue, ordered by the hash value.
 * Yes, it's really a lock-free linked list exactly as in Chap 9.8 of [HERLIHY].
 * Because the entries (queues) must be unique wrt hash values, we need more strict
 * algorithm, thus using the full lock-free linked list.
 * NOTE This is not a skip-list because we assume entries in each bucket should be at most a
 * few. Otherwise, we should increase the number of buckets.
 */
struct RawLockBucket {
    /**
     * Adds a new lock in this bucket, also acquiring a new queue if necessary.
     * \copydoc RawLockQueue#acquire()
     * @param[in] hash Precise hash of the resource to lock
     */
    w_rc_t  acquire(uint32_t hash, RawXct *xct, const okvl_mode& mode,
                    int32_t timeout_in_ms, bool check_only);
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
