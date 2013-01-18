#ifndef LOCK_BUCKET_H
#define LOCK_BUCKET_H

#include <stdint.h>
#include "lsn.h"
#include "sthread.h"
#include "lock_x.h"
#include "lock_s.h"

class xct_t;
class xct_lock_info_t;
class lock_queue_entry_t;
class lock_queue_t;
class xct_lock_entry_t;

class lock_core_m;
class ww_deadlock_wait_die; // just for experiments
class ww_deadlock_wound_wait; // just for experiments
class ww_deadlock_timeout; // just for experiments

/**
 * A lock request entry in lock queue.
 */
class lock_queue_entry_t {
    friend class lock_core_m; // for debug dump
public:
    typedef lock_base_t::lmode_t lmode_t;
    lock_queue_entry_t (xct_t* xct, smthread_t* thr, xct_lock_info_t* li,
                        lock_queue_entry_t* prev, lock_queue_entry_t* next,
                        lmode_t granted_mode, lmode_t requested_mode)
        : _xct(xct), _thr(thr), _li(li), _xct_entry(NULL), _prev(prev), _next(next),
            _granted_mode(granted_mode), _requested_mode(requested_mode) {
    }

    /** owning xct. */
    xct_t*              _xct;
    /** owning thread. */
    smthread_t*         _thr;
    xct_lock_info_t*    _li;
    xct_lock_entry_t*   _xct_entry;
    lock_queue_entry_t* _prev;
    lock_queue_entry_t* _next;
    lmode_t             _granted_mode;
    lmode_t             _requested_mode;
    srwlock_t           _entry_lock;
};
ostream&  operator<<(ostream& o, const lock_queue_entry_t& r);

struct check_grant_result {
    void empty_init () {
        can_be_granted = false;
        deadlock_detected = false;
        deadlock_myself_should_die = false;
        deadlock_other_victim = NULL;
    }
    void init (const atomic_thread_map_t &fingerprint) {
        can_be_granted = true;
        deadlock_detected = false;
        deadlock_myself_should_die = false;
        deadlock_other_victim = NULL;
        refreshed_wait_map.copy(fingerprint);
    }
    bool can_be_granted;
    bool deadlock_detected;
    bool deadlock_myself_should_die;
    smthread_t* deadlock_other_victim;
    atomic_thread_map_t  refreshed_wait_map;
};

/**
 * \brief A lock queue to hold granted and waiting lock requests.
 * NOTE this class's constructor/destructor does nothing. The container
 * of this object (bucket_t) calls allocate_lock_queue/deallocate_lock_queue.
 */
class lock_queue_t {
    friend class lock_core_m; // for debug dump
    friend class ww_deadlock_wait_die; // just for experiments
    friend class ww_deadlock_wound_wait; // just for experiments
    friend class ww_deadlock_timeout; // just for experiments
public:
    typedef lock_base_t::lmode_t lmode_t;
    lock_queue_t(uint32_t hash) : _next (NULL), _hit_counts(0), _hash(hash), _x_lock_tag(lsn_t::null),
        _head (NULL), _tail (NULL) {
    }
    ~lock_queue_t() {}

    inline lock_queue_t* next () { return _next; }
    inline void set_next (lock_queue_t *new_next) { _next = new_next; }

    inline uint32_t hash() const { return _hash;}

    inline void increase_hit_count () {++_hit_counts;}
    inline uint32_t hit_count () const { return _hit_counts;}
    
    inline void update_x_lock_tag (const lsn_t &new_tag) {
        if (!_x_lock_tag.valid() || _x_lock_tag < new_tag) {
            _x_lock_tag = new_tag;
        }
    }
    inline const lsn_t& x_lock_tag () const {return _x_lock_tag;}

    lock_queue_entry_t* find_request (const xct_lock_info_t* li);

    void append_request (lock_queue_entry_t* myreq);
    void detach_request (lock_queue_entry_t* myreq);
    /**
     * try getting a lock.
     * @return if it really succeeded to get the lock.
     */
    bool grant_request (lock_queue_entry_t* myreq);

    /**
     * Checkif my request can be granted, and also check deadlocks.
     */
    void check_can_grant (lock_queue_entry_t* myreq, check_grant_result &result);
    
    /** opportunistically wake up waiters. called when some lock is released. */
    void wakeup_waiters(lmode_t released_granted, lmode_t released_requested);

    /** allocate a new queue object. uses TLS cache in lock_core.cpp. */
    static lock_queue_t* allocate_lock_queue(uint32_t hash);
    /** deallocate a new queue object. uses TLS cache in lock_core.cpp. */
    static void deallocate_lock_queue(lock_queue_t *obj);
    
private:
    /** forms a singly linked list for other queues in the same bucket. */
    lock_queue_t*  _next;
    /** 
     * how popular it is (approximately, so, no protection).
     * for background cleanup to save memory.
     * NOTE this is NOT a pin-count that is precisely incremented/decremented to immediately revoke.
     * the queue is revoked only in background cleanup.
     */
    uint32_t       _hit_counts;
    /** precise hash for this lock queue. */
    const uint32_t _hash;
    /** Stores the commit timestamp of latest transaction that released an X lock on this queue. */
    lsn_t          _x_lock_tag;

    /** the first entry in this queue. */
    lock_queue_entry_t* _head;
    /** the last entry in this queue. */
    lock_queue_entry_t* _tail;

    /**
     * protects accesses to _requests.
     *   read access: find_request, check_can_grant, wakeup_waiters
     *   write access: grant_request, append_request, detach_request
     */
    srwlock_t     _requests_lock;
};

/**
 * Lock table hash table bucket.
 * Lock table's hash table is _htab, a list of bucket_t's,
 * each bucket contains a mutex that protects the list
 * connected through the lock_head_t's "chain" links.
 */
class bucket_t {
public:

    /** pointer to the first lock queue. you need to check the  */ 
    lock_queue_t                  *_queue;
    /**
     * protects accesses to _queue, not the contents of _queue.
     * the critical section is only until returning a lock_queue_t* or adds new lock_queue_t*.
     */
    srwlock_t                      _queue_lock;
    //tatas_lock                     _queue_lock;

    bucket_t() : _queue(NULL) {}
    ~bucket_t() {
        for (lock_queue_t* p = _queue; p != NULL; p = p->next()) {
            lock_queue_t::deallocate_lock_queue(p);
        }
    }

    /** Finds or creates a lock queue for the given hash value. */
    lock_queue_t*                 find_lock_queue(uint32_t hash);

    /**
     * Tries to find a lock queue for the given hash without creating a new lock queue.
     * As this is a readonly access, much faster!
     * @return returns NULL if not found. In that case, call find_lock_queue_create().
     */
    lock_queue_t*                 find_lock_queue_nocreate(uint32_t hash);
    /** This one really creates if not found. */
    lock_queue_t*                 find_lock_queue_create(uint32_t hash);
    //lock_queue_t*                 _find_lock_queue(uint32_t hash);
};

inline lock_queue_t* bucket_t::find_lock_queue(uint32_t hash) {
    //lock_queue_t* p = _find_lock_queue(hash);

    // this comparison is not protected, but fine.
    // because false-positive = checked again by find_lock_queue_nocreate
    // false-negative = checked again by find_lock_queue_create
    lock_queue_t* p = NULL;
    if (_queue != NULL) {
        p = find_lock_queue_nocreate(hash);
    }
    if (p == NULL) {
        // then we have to try the expensive way
        p = find_lock_queue_create(hash);
    }

    w_assert1(p);
    p->increase_hit_count();
    return p;
}
/*
inline lock_queue_t* bucket_t::_find_lock_queue(uint32_t hash) {
    tataslock_critical_section cs (&_queue_lock);
    for (lock_queue_t* p = _queue; p != NULL; p = p->next()) {
        if (p->hash() == hash) {
            return p;
        }
    }
    lock_queue_t* new_p = lock_queue_t::allocate_lock_queue(hash);
    new_p->set_next(_queue);
    _queue = new_p;
    return new_p;
}
*/
inline lock_queue_t* bucket_t::find_lock_queue_nocreate(uint32_t hash) {
    spinlock_read_critical_section cs(&_queue_lock);
    for (lock_queue_t* p = _queue; p != NULL; p = p->next()) {
        if (p->hash() == hash) {
            return p; // most cases should be here...
        }
    }
    return NULL;
}

inline lock_queue_t* bucket_t::find_lock_queue_create(uint32_t hash) {
    spinlock_write_critical_section cs(&_queue_lock);
    // note that we need to scan the linked list again to make sure
    // it wasn't inserted before this call.
    for (lock_queue_t* p = _queue; p != NULL; p = p->next()) {
        if (p->hash() == hash) {
            return p;
        }
    }
    lock_queue_t* new_p = lock_queue_t::allocate_lock_queue(hash);
    new_p->set_next(_queue);
    _queue = new_p;
    return new_p;
}
#endif // LOCK_BUCKET_H
