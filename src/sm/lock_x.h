/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef LOCK_X_H
#define LOCK_X_H

#include "w_defines.h"

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

    atomic_thread_map_t  _wait_map; // for dreadlocks DLD
};

#endif          /*</std-footer>*/
