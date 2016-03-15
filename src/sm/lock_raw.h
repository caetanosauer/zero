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
 * \section RAW Read-After-Write
 * In short, RAW is a general protocol to \e Write (Declare) something in a shared variable
 * followed by a \e memory-barrier, then followed by \e Read. Assuming all other threads
 * are also running the same protocol, this can be an efficient lightweight synchronization
 * without too many atomic operations or mutex. Membar is also expensive, but not as much
 * as atomic operations and mutex.
 *
 * \section TERM Terminology
 * In this code, we avoid abusing the often misused term "lock-free" because it's confusing
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
 * waiting with mutex or spinning. So, this is not really a lock-free list.
 *   \li We never \e insert into lock queue, rather we always \e append. This makes many
 * operations easier than the original lock-free linked list.
 *   \li There are several cases we can tolerate false positives (which only results in
 * conservative executions) and even some false negatives (see check_only param of
 * RawLockQueue#acquire()).
 *   \li Garbage collection has to be based on LSN.
 *
 * \section GC Garbage Collection
 * RawXct and RawLock are maintained by LSN-based Garbage Collector.
 * See GcPoolForest for more details.
 *
 * \section DIFF Differences from [JUNG13]
 * We made a few changes from the original algorithm.
 * The first one is, as described above, we put precise hash in each lock and ignore
 * locks that have different hashes during checks.
 *
 * Second difference is that we \e steal a bit from 64 bit pointer to atomically delete
 * an obsolete lock entry. We use MarkablePointer class and the same protocol as the standard
 * LockFreeList with mark-for-death bit. See Chap 9.8 in [HERLIHY].
 *
 * Third difference is that we physically delete the lock from the queue as soon as we
 * end the transaction. This is required to avoid dangling pointer even with garbage collector.
 *
 * Fource, we found that spinning while waiting is more efficient than mutex assuming the
 * system does a right throttling to bound the number of active workers.
 * PURE_SPIN_RAWLOCK ifdef controls it.
 *
 * Finally, we don't have "tail" as a member in RawLockQueue.
 * Again, it's equivalent to the standard Harris-Michael LockFreeList [MICH02].
 *
 * \section REF References
 *   \li [JUNG13] "A scalable lock manager for multicores"
 *   Hyungsoo Jung, Hyuck Han, Alan D. Fekete, Gernot Heiser, Heon Y. Yeom. SIGMOD'13.
 *   \li Also see MarkablePointer, [MICH02], and [HERLIHY].
 */

#include <stdint.h>
#include <ostream>
#include <pthread.h>
#include <AtomicCounter.hpp>
#include "w_defines.h"
#include "w_okvl.h"
#include "w_rc.h"
#include "w_gc_pool_forest.h"
#include "w_markable_pointer.h"
#include "lsn.h"

/**
 * We found that pure spinning is much faster than mutex sleep/wake-up as in [JUNG13].
 * As far as we don't over-subscribe workers, this has no disadvantages.
 * Pure spinning means we don't have to do anything in lock release, we don't have to do
 * keep any of mutexes, so much faster.
 * \ingroup RAWLOCK
 */
#define PURE_SPIN_RAWLOCK

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
#ifdef PURE_SPIN_RAWLOCK
inline void atomic_synchronize_if_mutex() {}
#else // PURE_SPIN_RAWLOCK
inline void atomic_synchronize_if_mutex() { atomic_synchronize(); }
#endif // PURE_SPIN_RAWLOCK

struct RawLockBucket;
struct RawLockQueue;
struct RawLock;
struct RawXct;
class sm_options;

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

    /** Precise hash of the protected resource. */
    uint32_t                    hash;

    /** Current status of this lock. */
    LockState                   state;

    /** Constitutes a singly-linked list in RawLockQueue. */
    MarkablePointer<RawLock>    next;

    /** owning xct. */
    RawXct*                     owner_xct;

    /** Requested lock mode. */
    okvl_mode                   mode;

    /** Doubly-linked list in RawXct. This is a transaction-private information. */
    RawLock*                    xct_previous;
    /** Doubly-linked list in RawXct. This is a transaction-private information. */
    RawLock*                    xct_next;

    // another doubly linked list for RawXctLockHashMap
    RawLock*                    xct_hashmap_previous;
    RawLock*                    xct_hashmap_next;
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
struct RawLockQueue {
    /**
     * \brief Safely iterates through lock entires in this queue from the head.
     * \details
     * You can optionally specify which lock you start from \b if you can guarantee
     * the specified lock is in the queue (e.g., when you check locks that follow your lock).
     * This iterator uses the LockFreeList's mark-for-death technique to be safe.
     * When it finds a marked entry, it does atomic CAS to remove the entry to make sure
     * the returned entries are valid.
     * Instead, the caller must respect the "retry" result unlike usual iterator.
     */
    struct Iterator {
        /**
        * Constructs an iterator to safely iterate over lock entries in this queue.
        * @param[in] start_from the initial entry that would be \e predecessor, not current.
        * @pre start_from != NULL
        * @pre !start_from->next.is_marked()
        */
        Iterator(const RawLockQueue* enclosure, RawLock* start_from);
        /** Says whether the iterator currently points to a null entry (e.g., after tail). */
        bool is_null() const { return current.is_null(); }
        /**
         * Advances the iterator.
         * If is_null(), it does nothing.
         * @param[out] must_retry if this retuns true, the caller must start again.
         */
        void next(bool &must_retry);

        const RawLockQueue*         enclosure;
        /**
         * Previous entry. Never NULL. You can check predecessor->current relation ship with
         * atomic CAS on predecessor->next considering mark-for-death. See [HERLIHY] Chap 9.8.
         */
        RawLock*                    predecessor;
        /**
         * Current entry. Can be is_null() if predecessor is \e probably the tail.
         * Again, you should use atomic CAS to change something on it.
         */
        MarkablePointer<RawLock>    current;
    };

    /**
     * \brief Adds a new lock in the given mode to this queue, waiting until it is granted.
     * @param[in] xct the transaction to own the new lock
     * @param[in] hash precise hash of the resource to lock.
     * @param[in] mode requested lock mode
     * @param[in] timeout_in_ms maximum length to wait in milliseconds.
     * negative number means forever. If conditional, this parameter is ignored.
     * @param[in] conditional If true, this method doesn't wait at all \b and also it leaves
     * the inserted lock entry even if it wasn't granted immediately.
     * @param[in] check_only if true, this method doesn't actually create a new lock object
     * but just checks if the requested lock mode can be granted or not.
     * @param[out] out pointer to the \e successfully acquired lock. it returns NULL if
     * we couldn't get the lock \b except conditional==true case.
     * \details
     * \b check_only=true can give a false positive in concurrent unlock case, but
     * give no false negative \b assuming a conflicting lock is not concurrently taken for
     * the key. This assumption holds for our only check_only=true use case, which is the
     * tentative NX lock check before inserting a new key, \b because we then have an EX latch!
     * Thus, this is a safe and efficient check for B-tree insertion.
     *
     * \b conditional locking is the standard way to take a lock in DBMS without leaving
     * latches long time. B-tree first requests a lock without releasing latch (conditional).
     * If it fails, it releases latch and unconditionally lock, which needs re-check of LSN
     * after lock and re-latch. The purpose of this \e conditional parameter is that we don't
     * want to insert the same lock entry twice when the first conditional locking fails.
     * When conditional==true, we leave the lock entry and return it in \e out even if it
     * wasn't granted. The caller \e MUST be responsible to call retry_acquire() after the
     * failed acquire (which returns eCONDLOCKTIMEOUT if it failed) or release the lock.
     * It is anyway released at commit time, but waiting lock entry should be removed
     * before the transaction does anything else.
     *
     * @pre out != NULL
     */
    w_error_codes   acquire(RawXct *xct, uint32_t hash, const okvl_mode& mode,
                int32_t timeout_in_ms, bool check, bool wait, bool acquire,
                RawLock** out);

    /**
     * Waits for the already-inserted lock entry. Used after a failed conditional locking.
     * @see acquire()
     * \NOTE "lock" is a RawLock**, not RawLock*. It might be cleared after another failure,
     * or become a new lock when it's automatically retried.
     */
    w_error_codes   retry_acquire(RawLock** lock, bool wait, bool acquire,
            int32_t timeout_in_ms);
    /** Subroutine of acquire(), retry_acquire(). */
    w_error_codes   complete_acquire(RawLock** lock, bool wait, bool acquire,
            int32_t timeout_in_ms);

    /**
     * \brief Releases the given lock from this queue, waking up others if necessary.
     * @param[in] lock the lock to release.
     * @param[in] commit_lsn LSN to update X-lock tag during SX-ELR.
     */
    void    release(RawLock *lock, const lsn_t &commit_lsn);

    /** Makes sure x_lock_tag is at least the given LSN. */
    void    update_xlock_tag(const lsn_t& commit_lsn);

    /**
     * \brief Atomically insert the given lock to this queue. Called from acquire().
     * \details
     * See Figure 4 and Sec 3.1 of [JUNG13].
     */
    void    atomic_lock_insert(RawLock *new_lock);

    /** result of check_compatiblity() */
    struct Compatibility {
        Compatibility(bool grant, bool deadlock, RawXct* block)
        : can_be_granted(grant), deadlocked(deadlock), blocker(block) {}
        bool        can_be_granted;
        bool        deadlocked;
        RawXct*     blocker;
    };
    /**
     * Checks if the given lock can be granted.
     * Called from acquire() after atomic_lock_insert() and release().
     */
    Compatibility check_compatiblity(RawLock *lock) const;

    /**
     * \brief Used for check_only=true case. Many things are much simpler and faster.
     * \details
     * This is analogous to the wait-free contains() of lock-free linked list in [HERLIHY].
     * As commented in acquire(), this method is safe for check_only case.
     * EX latch on the page guarantees that no lock for the key is newly coming now.
     * @return whether the mode is permitted
     */
    bool    peek_compatiblity(RawXct* xct, uint32_t hash, const okvl_mode &mode) const;

    /**
     * Sleeps until the lock is granted.
     * Called from acquire() after check_compatiblity() if the lock was not immediately granted.
     */
    w_error_codes wait_for(RawLock *new_lock, int32_t timeout_in_ms);

    /**
     * \brief Returns the predecessor of the given lock.
     * This removes marked entries it encounters.
     * @return predecessor of the given lock. NULL if not found.
     */
    RawLock*    find_predecessor(RawLock *lock) const;

    /**
     * \brief Returns the last entry of this queue.
     * This removes marked entries it encounters, which is the reason why we can't have
     * "tail" as a variable in the queue and instead we have to traverse each time.
     * @return the last entry. Never returns NULL, but might be &head (meaning empty).
     */
    RawLock*    tail() const;

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
    bool        delink(RawLock* predecessor, RawLock* target, RawLock* successor) const;

    // Helper function called by RawLockQueue::acquire
    // Based on the information in Compatibility, if the blocker txn is a loser txn and it is not
    // in the middle of rolling back, trigger the on_demand UNDO for the loser transaction
    // Return true if an on_demand UNDO operation was triggered and completed
    bool        trigger_UNDO(Compatibility& compatibility);  // In: the current compatibility status of the requested lock

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

    // For on_demand and mixed UNDO counting purpose   
    static int                  loser_count;
};
std::ostream& operator<<(std::ostream& o, const RawLockQueue& v);

/**
 * Bucket count in RawXctLockHashMap (private lock entry hashmap).
 * \ingroup RAWLOCK
 * @see RawXctLockHashMap
 */
const int RAW_XCT_LOCK_HASHMAP_SIZE = 1023;

/**
 * \brief A hashmap for lock entries in transaction's \e private memory.
 * \ingroup RAWLOCK
 * \details
 * \section OVERVIEW Overview
 * This auxiliary data structure is to efficiently check what locks the transaction
 * has already acquired. Because this is a transaction-private data structure and
 * there is no multi-threads-per-transaction feature, we don't need any synchronization
 * to access this. It uses no spinlock/mutex and is preferrable especially in multi-socket
 * environment.
 * \section THREADS Multi-thread safety
 * get_granted_mode(), the method used from lock_m::lock(), has a precondition that says
 * it must be called from the transaction's thread.
 * The precondition is important because we don't take latch in get_granted_mode()
 * nor return a copied lock mode (returns reference). As "this" is the only thread
 * that might change granted mode of the lock entry or change the bucket's linked list,
 * this is safe.
 * \section HISTORY History
 * In original Shore-MT, there was only linked list. So, in order to check if a transaction
 * already has some lock, we had to query the public lock table, entering critical sections.
 * Further, Shore-MT had a feature to run a single transaction on multi-threads.
 * There was no truly "private" memory back then for this reason.
 * We found this causes an issue in NUMA environment, and made this private hashmap.
 * \section PERFORMANCE Performance Comparison
 * As of 20140213, with and without this improvement, TPCC on 4-socket machine is as
 * follows: BEFORE=12027 TPS, AFTER=13764 TPS.
 * \NOTE yes, it's a copy-paste from XctLockHashMap, but this would be the only implementation.
 */
class RawXctLockHashMap {
public:
    RawXctLockHashMap();
    ~RawXctLockHashMap();

    /**
     * \brief Returns the lock granted to this transaction for the given lock ID.
     * @param[in] lock_id identifier of the lock entry
     * @return the lock mode this transaction has for the lock. ALL_N_GAP_N if not any.
     * @pre the current thread is the only thread running the transaction of this hashmap
     */
    okvl_mode                   get_granted_mode(uint32_t lock_id) const;

    /** Clears hash buckets. */
    void                        reset();

    /** Add a new entry to this hashmap. */
    void                        push_front(RawLock *link);
    /** Removes the entry from this hashmap. */
    void                        remove(RawLock *link);
private:

    /**
     * Hash buckets. In each bucket, we have a doubly-linked list of xct_lock_entry_t.
     */
    RawLock*                    _buckets[RAW_XCT_LOCK_HASHMAP_SIZE];

    /** Returns the bucket index for the lock. */
    static uint32_t _bucket_id(uint32_t lock_id) { return lock_id % RAW_XCT_LOCK_HASHMAP_SIZE; }
};

/**
 * \brief A shadow transaction object for RAW-style lock manager.
 * \ingroup RAWLOCK
 * \details
 * Just like [JUNG13], we have additional (shadow) transaction objects that are used from
 * the real transaction objects. The real transaction objects (xct_t) are immediately
 * recycled after commit/abort while this object lives a bit longer until garbage
 * collection kicks in. This is necessary to make sure the RAW style deadlock-detection
 * does not reference an already revoked object.
 * See Section 4.5 of [JUNG13].
 */
struct RawXct : GcPoolEntry {
    enum XctState {
        /** This object is in the pool and not used in any place. */
        UNUSED = 0,
        /** This transaction is running without being blocked so far. */
        ACTIVE,
        /** This transaction is waiting for some other transaction. */
        WAITING,
    };

    void                        init(gc_thread_id thread_id,
        GcPoolForest<RawLock>* lock_pool, gc_pointer_raw* lock_pool_next);
    void                        uninit();

    /**
     * \brief Recursively checks for the case where some blocker is this transaction itself.
     * @param[in] first_blocker the transaction that blocks myself.
     * @return true if there is a cycle, meaning deadlock.
     * \details
     * [JUNG13] does not clarify the algorithm to detect deadlocks.
     * We have originally had Dreadlock, the spinning based detection based on fingerprint, but
     * it would cause too many atomic operations from many threads in this RAW-style queue.
     * I assume a traditional recursive check (who is my blocker's blocker's...) is
     * appropriate with [JUNG13] approach.
     * @pre first_blocker != NULL
     */
    bool                        is_deadlocked(RawXct* first_blocker);

    void                        update_read_watermark(const lsn_t &tag) {
        if (read_watermark < tag) {
            read_watermark = tag;
        }
    }

    /**
     * Newly allocate a lock object from the object pool and put it in transaction-private
     * linked-list and hashmap.
     */
    RawLock*                    allocate_lock(uint32_t hash,
                                              const okvl_mode& mode, RawLock::LockState state);
    /**
     * Remove the lock object from transaction-private linked-list and hashmap, then deallocate.
     */
    void                        deallocate_lock(RawLock* lock);

    /** debugout function. */
    void                        dump_lockinfo(std::ostream &out) const;

    /** Returns if this transaction has acquired any lock. */
    bool                        has_locks() const { return private_first != NULL; }

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

    /** Other transaction realized that this transaction is deadlocked. */
    bool                        deadlock_detected_by_others;

    /** If exists the transaction that is now blocking this transaction. NULL otherwise.*/
    RawXct*                     blocker;

#ifndef PURE_SPIN_RAWLOCK
    /** Used to wait in lock manager, paired with lock_wait_mutex. */
    pthread_cond_t              lock_wait_cond;
    /** Used to wait in lock manager, paired with lock_wait_cond. */
    pthread_mutex_t             lock_wait_mutex;
#endif // PURE_SPIN_RAWLOCK

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

    /**
     * A hashmap for lock entries in transaction's \e private memory.
     * Used to quickly check if the transaction already has a required lock.
     */
    RawXctLockHashMap           private_hash_map;

    /**
     * A doubly linked-list for lock entries in transaction's \e private memory.
     * first -> next -> next ... is used for unlock while commit/abort.
     * NULL if zero entries.
     */
    RawLock*                    private_first;
    /**
     * A doubly linked-list for lock entries in transaction's \e private memory.
     * last is used for appending a new lock.
     * NULL if zero or one entries.
     */
    RawLock*                    private_last;
};
std::ostream& operator<<(std::ostream& o, const RawXct& v);

/**
 * \brief The background thread for pre-allocation and garbage collection of object pools
 * used in RAW-style lock manager.
 * \ingroup RAWLOCK
 * \details
 * The background thread takes intervals between pre-allocation and garbage collection,
 * but might be invoked by hurried transactions by calling wakeup() method.
 */
class RawLockBackgroundThread {
public:
    RawLockBackgroundThread(const sm_options &options,
                GcPoolForest<RawLock>* lock_pool, GcPoolForest<RawXct>* xct_pool);
    ~RawLockBackgroundThread();

    /** Start running this thread. */
    void start();
    /** Request this thread to stop and wait until it stops. */
    void stop_synchronous();
    /** Wakeup this thread to do its job. */
    void wakeup();

    /**
     * Gut of this class.
     */
    void run_main();

    /**
     * Handler for pthread_create. Parameter is _this_.
     */
    static void* pthread_main(void *t);
protected:
    /** The background pthread thread. */
    pthread_t       _thread;
    /** To join the thread. */
    pthread_attr_t  _join_attr;
    /** Pthread Mutex for taking internal sleep. */
    pthread_mutex_t _interval_mutex;
    /** Pthread Condition for taking internal sleep. */
    pthread_cond_t  _interval_cond;
    /** Turned on to stop this thread. */
    bool            _stop_requested;
    bool            _running;

    /**
     * When there is no log manager, we still need to do something to invoke
     * retiring. We use this counter to immitate LSN moving forward
     * and retire the last generation. It's unsafe, but so are all no-log executions.
     */
    int             _dummy_lsn_lock;
    int             _dummy_lsn_xct;
    /**
     * We start retiring generations when there are more than this number of generations.
     * \e sm_rawlock_gc_generation_count.
     */
    uint32_t            _generation_count;
    /**
     * When we start up, we pre-allocate this many generations.
     * \e sm_rawlock_gc_init_generation_count.
     */
    uint32_t            _init_generation_count;
    /**
     * We start pre-allocating segments in current generation if we have less than
     * this number of free segments.
     * \e sm_rawlock_gc_free_segment_count.
     */
    uint32_t            _free_segment_count;
    /**
     * We advance generation when there are this number of segments in current generation.
     * \e sm_rawlock_gc_max_segment_count.
     */
    uint32_t            _max_segment_count;

    /**
     * How many milliseconds do we sleep as interval.
     * \e sm_rawlock_gc_interval_ms.
     */
    uint32_t            _internal_milliseconds;
    /**
     * When we create a new lock generation, we initially pre-allocate this number of segments.
     * \e sm_rawlock_lockpool_initseg.
     */
    uint32_t            _lockpool_initseg;
    /**
     * When we create a new xct generation, we initially pre-allocate this number of segments.
     * \e sm_rawlock_xctpool_initseg.
     */
    uint32_t            _xctpool_initseg;
    /**
     * How many objects we create in each segment of _lock_pool.
     * \e sm_rawlock_lockpool_segsize.
     */
    size_t              _lockpool_segsize;
    /**
     * How many objects we create in each segment of _xct_pool.
     * \e sm_rawlock_xctpool_segsize.
     */
    size_t              _xctpool_segsize;

    /** The RawLock pool to take care of. */
    GcPoolForest<RawLock>*     _lock_pool;
    /** The RawXct pool to take care of. */
    GcPoolForest<RawXct>*      _xct_pool;
};

#endif // LOCK_RAW_H
