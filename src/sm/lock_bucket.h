/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef LOCK_BUCKET_H
#define LOCK_BUCKET_H

#include <stdint.h>
#include "w_okvl.h"
#include "w_okvl_inl.h"
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


/**
 * A lock request entry in a lock queue.
 *
 * All fields except _prev and _next, which are protected by L in the
 * usual way, of this class have unusual access protections:
 *
 * For any thread but this->_thr:
 *
 *   read a field:  must have read access to L
 *   write a field: forbidden
 *
 * For the thread this->_thr:
 *
 *   read a field:  always legal
 *   write a field: must have write access to L
 *
 * where L is the _requests_latch latch of the lock_queue_t we belong
 * to if any.  It is believed that this provides complete protection
 * (e.g., no stale reads).
 *
 * The primary reason for these relaxed rules is to allow
 * release_duration to avoid needing to find and take L in read mode
 * just to see if a lock needs to be released.
 */
class lock_queue_entry_t {
public:
    uint32_t get_observed_release_version() const { return _observed_release_version; }
private:
    friend class lock_queue_t;
    friend class lock_core_m;  // TODO: narrow this down later <<<>>>
    friend ostream& operator<<(ostream& o, const lock_queue_entry_t& r);

    lock_queue_entry_t (xct_t& xct, smthread_t& thr, xct_lock_info_t& li,
                        const okvl_mode& granted_mode, const okvl_mode& requested_mode)
        : _xct(xct), _thr(thr), _li(li), _xct_entry(NULL), _prev(NULL), _next(NULL),
            _observed_release_version(0),
            _granted_mode(granted_mode), _requested_mode(requested_mode) {
    }

    xct_t&              _xct;  ///< owning xct.
    smthread_t&         _thr;  ///< owning thread.
    xct_lock_info_t&    _li;
    xct_lock_entry_t*   _xct_entry;

    lock_queue_entry_t* _prev;
    lock_queue_entry_t* _next;

    /**
     * The _release_version of the lock queue when this lock's _wait_map
     * is last updated. When we check deadlocks, we tentatively ignore
     * other lock requests whose observed_release_version is older than
     * _release_version of the lock queue to prevent false detections.
     * 0 if this lock is not waiting.
     * This is NOT protected by latch because this is used only opportunistically.
     */
    uint64_t            _observed_release_version;

    okvl_mode              _granted_mode;
    okvl_mode              _requested_mode;
};
/**
 * Requires holding a read latch for queue._requests_latch where queue
 * is the lock_queue_t that r belongs to.
 */
ostream&  operator<<(ostream& o, const lock_queue_entry_t& r);


/**
 * \brief A lock queue to hold granted and waiting lock requests
 * (lock_queue_entry_t's) for a given lock.
 *
 * NOTE objects of this class are created via the container of this
 * object (bucket_t) calling allocate_lock_queue /
 * deallocate_lock_queue.
 *
 * WARNING: lock_queue_t's are currently only deleted at shutdown.
 * There is no mechanism presently to prevent one thread from deleting
 * a lock_queue_t out from under another thread.  FIXME?
 */
class lock_queue_t {
public:
    lock_queue_t(uint32_t hash) : _hash(hash), _hit_counts(0), _release_version(1), _next (NULL),
        _x_lock_tag(lsn_t::null), _head (NULL), _tail (NULL) {
    }
    ~lock_queue_t() {}

    inline uint32_t hash()       const { return _hash;}
    inline uint32_t hit_count () const { return _hit_counts;}


private:
    friend class bucket_t;
    friend class lock_core_m;  // TODO: narrow this down later <<<>>>

    inline void increase_hit_count () {++_hit_counts;}


    /** Requires read access for the bucket containing us if any's
        _queue_latch. */
    inline lock_queue_t* next () { return _next; }
    /** Requires write access for the bucket containing us if any's
        _queue_latch. */
    inline void set_next (lock_queue_t *new_next) { _next = new_next; }

    /** Allocate a new queue object.  Uses TLS cache in lock_core.cpp. */
    static lock_queue_t* allocate_lock_queue(uint32_t hash);
    /** Deallocate a new queue object.  Uses TLS cache in
        lock_core.cpp.  Shutdown time only. */
    static void deallocate_lock_queue(lock_queue_t *obj);


    /** Requires read access for _request_latch. */
    inline const lsn_t& x_lock_tag () const {return _x_lock_tag;}
    /** Requires write access for _request_latch, new_tag may be
        lsn_t::null. */
    inline void update_x_lock_tag (const lsn_t &new_tag) {
        if (!_x_lock_tag.valid() || _x_lock_tag < new_tag) {
            _x_lock_tag = new_tag;
        }
    }


    lock_queue_entry_t* find_request (const xct_lock_info_t* li);

    void append_request (lock_queue_entry_t* myreq);
    void detach_request (lock_queue_entry_t* myreq);
    /**
     * try getting a lock.
     * @return if it really succeeded to get the lock.
     * Requires current thread is myreq->_thr
     */
    bool grant_request (lock_queue_entry_t* myreq, lsn_t& observed);

    struct check_grant_result {
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
        lsn_t observed;
    };
    /**
     * Checkif my request can be granted, and also check deadlocks.
     */
    void check_can_grant (lock_queue_entry_t* myreq, check_grant_result &result);

    //* Requires read access to _requests_latch of queue other_request belongs to
    bool _check_compatible(const okvl_mode& granted_mode, const okvl_mode& requested_mode,
            lock_queue_entry_t* other_request, bool proceeds_me, lsn_t& observed);

    /** opportunistically wake up waiters.  called when some lock is released. */
    void wakeup_waiters(const okvl_mode& released_requested);


    const uint32_t _hash;  ///< precise hash for this lock queue.

    /**
     * How popular this lock is (approximately, so, no protection).
     * Intended for background cleanup to save memory, but not currently used.
     * Someday, the queue may be revoked in background cleanup.  FIXME
     *
     * NOTE this is NOT a pin-count that is precisely
     * incremented/decremented to control when deletion could occur.
     */
    uint32_t       _hit_counts;

    /**
     * \brief Monotonically increasing counter that is incremented
     * when some xct releases some lock in this queue.
     * \details
     * This is NOT protected by latch because it is used only for quick check
     * to opportunistically prevent false deadlock detection.
     * This counter was previously just a true/false flag (_wait_map_obsolete flag)
     * in waitmap, but turns out that a granular checking prevents
     * false deadlock detections more precisely and efficiently.
     */
    uint64_t       _release_version;

    /** Forms a singly-linked list for other queues in the same bucket
        as us; protected by the bucket containing us's _queue_latch if
        any. */
    lock_queue_t*  _next;


    /** R/W latch to protect remaining fields as well as our
        lock_queue_entry_t's fields. */
    srwlock_t     _requests_latch;

    /** Stores the commit timestamp of the latest transaction that
        released an X lock on this queue; holds lsn_t::null if no such
        transaction exists; protected by _requests_latch. */
    lsn_t          _x_lock_tag;

    /** The first entry in this queue or NULL if queue empty;
        protected by _requests_latch. */
    lock_queue_entry_t* _head;
    /** The last entry in this queue or NULL if queue empty; protected
        by _requests_latch. */
    lock_queue_entry_t* _tail;
};

inline bool lock_queue_t::_check_compatible(const okvl_mode& granted_mode,
    const okvl_mode& requested_mode, lock_queue_entry_t* other_request, bool precedes_me, lsn_t& observed) {
    bool compatible;
    if (precedes_me) {
        // in this case, we _respect_ the predecesor's requested mode, not just granted mode.
        // in case they are trying to upgrade.
        compatible = okvl_mode::is_compatible(other_request->_requested_mode, requested_mode);
        if (!compatible) {
            // Even when my request is not compatible with his request,
            // there is one case where I don't have to respect the predecessor's upgrade desire.
            // If the predecessor is _anyways_ blocked by my _granted_ mode,
            // for example I already have SN and he is trying to upgrade from SS to XS.
            // Now, if I want to upgrade my mode to SS, he will be anyway blocked.
            // Thus, it wouldn't hurt to proceed.
            if (!okvl_mode::is_compatible(other_request->_requested_mode, granted_mode)) {
                // okay, he is anyway blocked. So, let's not respect his upgrade desire.
                // just compare with his granted mode.
                compatible = okvl_mode::is_compatible(other_request->_granted_mode, requested_mode);
            }
        }

    } else {
        compatible = okvl_mode::is_compatible(other_request->_granted_mode, requested_mode);
    }

    if (compatible)
        return true;

    // Can we violate the lock?
    xct_lock_info_t& li = other_request->_li;
    spinlock_read_critical_section cs(&li._shared_latch);
    if (!li._permission_to_violate)
        return false;

    if (!observed.valid() || observed < li._commit_lsn)
        observed = li._commit_lsn;
    return true;
}


/**
 * Lock table hash table bucket.
 *
 * Lock table's hash table is lock_core_m::_htab, which is an array of
 * bucket_t's.  Each bucket contains a linked list of lock_queue_t's
 * and a latch that protects that list's _next pointers.
 */
class bucket_t {
public:
    bucket_t() : _queue(NULL) {}
    ~bucket_t() {
        // assume done only at shutdown so no locks needed:
        lock_queue_t* p = _queue;
        while (p != NULL) {
            lock_queue_t* q = p->next();
            lock_queue_t::deallocate_lock_queue(p);
            p = q;
        }
    }

    /** Finds or creates a lock queue for the given hash value. */
    lock_queue_t* find_lock_queue(uint32_t hash);


private:
    friend void lock_core_m::assert_empty() const;

    /**
     * Protects accesses to _queue and the _next pointers of the
     * lock_queue_t's on that list only.  We only keep this latch
     * until we find or create the right lock_queue_t.
     */
    srwlock_t     _queue_latch;
    //tatas_lock  _queue_latch;

    /** Pointer to the first lock_queue_ of our list; protected by _queue_latch. */
public:    lock_queue_t* _queue;
private:


    /**
     * Tries to find a lock queue for the given hash without creating
     * a new lock queue.  As this is a readonly access, much faster!
     * @return returns NULL if not found.  In that case, call find_lock_queue_create().
     */
    lock_queue_t* find_lock_queue_nocreate(uint32_t hash);
    /**
     * Find a lock queue for the given hash, creating a new lock queue
     * if needed.
     * @return never returns NULL.
     */
    lock_queue_t* find_lock_queue_create(uint32_t hash);
    //lock_queue_t* _find_lock_queue(uint32_t hash);
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
    tataslock_critical_section cs (&_queue_latch);
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
    spinlock_read_critical_section cs(&_queue_latch);
    for (lock_queue_t* p = _queue; p != NULL; p = p->next()) {
        if (p->hash() == hash) {
            return p; // most cases should be here...
        }
    }
    return NULL;
}

inline lock_queue_t* bucket_t::find_lock_queue_create(uint32_t hash) {
    spinlock_write_critical_section cs(&_queue_latch);
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
