/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef LOCK_X_H
#define LOCK_X_H

#include "w_defines.h"

#ifdef __GNUG__
#pragma interface
#endif

class xct_lock_info_t; // forward
class lock_queue_entry_t;
class lock_queue_t;

/**
 * A lock entry in transaction's private memory.
 */
class xct_lock_entry_t {
public:
    xct_lock_entry_t () : prev (NULL), next(NULL), queue (NULL), entry (NULL) {}
    // doubly linked list
    xct_lock_entry_t   *prev;
    xct_lock_entry_t   *next;
    // Corresponding object in lock queue.
    lock_queue_t       *queue;
    lock_queue_entry_t *entry;
};


/**\brief Shared between transaction (xct_t) and lock manager
 * \details
 */
class xct_lock_info_t : private lock_base_t {

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

    /** @see _wait_map_obsolete. */
    bool             is_wait_map_obsolete() const { return _wait_map_obsolete; }
    void             set_wait_map_obsolete(bool val) { _wait_map_obsolete = val; }

    /// Each thread has a wait_map
    atomic_thread_map_t const &get_wait_map() const { return _wait_map; }
    void              clear_wait_map() { 
                            //_wait_map.lock_for_write();
                            _wait_map.clear(); 
                            //_wait_map.unlock_writer();
                        }
    void              refresh_wait_map(atomic_thread_map_t const &new_map) {
        _wait_map.copy(new_map);
        _wait_map_obsolete = false;
    }
    void              init_wait_map(smthread_t *thr) {
        //_wait_map.lock_for_write();
        _wait_map.copy(thr->get_fingerprint_map());
        //_wait_map.unlock_writer();
        _wait_map_obsolete = false;
        DBGOUT5 (<< "initialized wait map!" << _wait_map);
    }

    xct_lock_entry_t* link_to_new_request (lock_queue_t *queue, lock_queue_entry_t *entry);
    void remove_request (xct_lock_entry_t *entry);
public:
    /*
     * List of locks acquired by this xct.
     */
    xct_lock_entry_t *_head;
    xct_lock_entry_t *_tail;

    srwlock_t _shared_latch;
    bool      _permission_to_violate;
    lsn_t     _commit_lsn;

private:
    // tid of the most recent transaction using this lock_info; monotonically 
    // increasing.
    tid_t           _tid;     
                                     
    /**
     * \brief Means if this _wait_map seems old and unreliable.
     * \details
     * This flag was added to avoid too many false deadlocks. 
     * See jira ticket:95 "Modifying Shore-MT: Dreadlock implementation" (originally trac ticket:97).
     * 
     * If this flag is ON, the wait map is suspected to be based on 
     * outdated information as a result of other threads' lock release etc.
     * Seeing this flag ON, other thread will not take OR with this
     * request.
     * 
     * This flag is turn on when another thread releases locks
     * on the same lock chain.
     * This flag is turn off when this thread makes another round of
     * dreadlock propagation, resetting its wait map first.
     * 
     * Note that there will be still both false positives and (tentative) negatives.
     * 
     * False positive (false deadlock): The committing thread only turns ON
     * this flags on the same lock, not recursively, to avoid mutex deadlocks
     * accessing multiple lock heads.
     * 
     * (only tentative) false negative: Seeing the flag ON, the thread resets
     * its wait_map and starts over, so it might need a few rounds of propagation
     * to recursively reconstruct the bitmap to find a real deadlock.
     */
    bool                 _wait_map_obsolete;

    atomic_thread_map_t  _wait_map; // for dreadlocks DLD
};

#endif          /*</std-footer>*/
