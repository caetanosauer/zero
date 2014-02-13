/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef LOCK_X_H
#define LOCK_X_H

#include "w_defines.h"

class xct_lock_info_t; // forward
class lock_queue_entry_t;
class lock_queue_t;
class okvl_mode;
class lockid_t;

/**
 * \brief A lock entry in transaction's \e private memory.
 * \ingroup SSMLOCK
 * \details
 * Each transaction maintains a linked-list of this object so that it can release
 * acquired locks when the transaction commits or aborts.
 */
class xct_lock_entry_t {
public:
    xct_lock_entry_t () : prev (NULL), next(NULL), private_hashmap_prev(NULL),
        private_hashmap_next(NULL), queue (NULL), entry (NULL) {}
    // doubly linked list
    xct_lock_entry_t   *prev;
    xct_lock_entry_t   *next;

    // another doubly linked list for XctLockHashMap
    xct_lock_entry_t    *private_hashmap_prev;
    xct_lock_entry_t    *private_hashmap_next;

    // Corresponding object in lock queue.
    lock_queue_t       *queue;
    lock_queue_entry_t *entry;
};

/**
 * Bucket count in XctLockHashMap (private lock entry hashmap).
 * \ingroup SSMLOCK
 * @see XctLockHashMap
 */
const int XCT_LOCK_HASHMAP_SIZE = 1023;

/**
 * \brief A hashmap for lock entries in transaction's \e private memory.
 * \ingroup SSMLOCK
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
 * The precondition is important because we don't take latch in the method
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
 * @see xct_lock_entry_t
 */
struct XctLockHashMap {
    XctLockHashMap();
    ~XctLockHashMap();

    /**
     * \brief Returns the lock granted to this transaction for the given lock ID.
     * @param[in] lock_id identifier of the lock entry
     * @return the lock mode this transaction has for the lock. ALL_N_GAP_N if not any.
     * @pre the current thread is the only thread running the transaction of this hashmap
     */
    const okvl_mode&            get_granted_mode(uint32_t lock_id) const;

    /** Clears hash buckets. */
    void                        reset();

    /**
     * Hash buckets. In each bucket, we have a doubly-linked list of xct_lock_entry_t.
     */
    xct_lock_entry_t*           buckets[XCT_LOCK_HASHMAP_SIZE];

    /** Returns the bucket index for the lock. */
    static uint32_t bucket_id(uint32_t lock_id) { return lock_id % XCT_LOCK_HASHMAP_SIZE; }
};

/**
 * \brief Locking-related status of one transaction.
 * \ingroup SSMLOCK
 * \details
 * Shared between transaction (xct_t) and lock manager.
 * Note that this object is one-per-transaction, not one-per-lock.
 * The per-lock object is xct_lock_entry_t (xct side) and lock_queue_entry_t (queue side).
 */
class xct_lock_info_t : private smlevel_1 {

public:
    NORET            xct_lock_info_t();
    NORET            ~xct_lock_info_t();

    /// Prepare this structure for use by a new transaction.
    /// Used by the TLS agent when recycling a structure after the
    /// xct that used it goes away.
    xct_lock_info_t* reset_for_reuse();

    /// unsafe output operator, for debugging
    friend ostream & operator<<(ostream &o, const xct_lock_info_t &x);

    /// unsafe output operator, for debugging
    ostream &        dump_locks(ostream &out) const;

    /// ID of the transaction that owns this structure.
    tid_t            tid() const { return _tid; }

    /// See above.
    void             set_tid(const tid_t &t) { _tid=t; }

    /// Each thread has a wait_map
    atomic_thread_map_t const &get_wait_map() const { return _wait_map; }
    void              clear_wait_map() {
                            //_wait_map.lock_for_write();
                            _wait_map.clear();
                            //_wait_map.unlock_writer();
                        }
    void              refresh_wait_map(atomic_thread_map_t const &new_map) {
        _wait_map.copy(new_map);
    }
    void              init_wait_map(smthread_t *thr) {
        //_wait_map.lock_for_write();
        _wait_map.copy(thr->get_fingerprint_map());
        //_wait_map.unlock_writer();
        DBGOUT5 (<< "initialized wait map!" << _wait_map);
    }

    xct_lock_entry_t* link_to_new_request (lock_queue_t *queue, lock_queue_entry_t *entry);
    void remove_request (xct_lock_entry_t *entry);

    /** Returns the private hashmap to check already-granted locks. */
    XctLockHashMap&     get_private_hashmap() { return _hashmap; }
public:
    /*
     * List of locks acquired by this xct.
     */
    xct_lock_entry_t *_head;
    xct_lock_entry_t *_tail;

    /**
     * Auxiliary hashmap of the locks acquired by this transaction.
     */
    XctLockHashMap  _hashmap;

    srwlock_t _shared_latch;
    bool      _permission_to_violate;
    lsn_t     _commit_lsn;

private:
    // tid of the most recent transaction using this lock_info; monotonically
    // increasing.
    tid_t           _tid;

    atomic_thread_map_t  _wait_map; // for dreadlocks DLD
};

#endif          /*</std-footer>*/
