#ifndef LOCK_CORE_H
#define LOCK_CORE_H

#include "w_defines.h"

#ifdef __GNUG__
#pragma interface
#endif

#include "lock_lil.h"

class bucket_t;
class lock_queue_t;
class lock_queue_entry_t;

/**
* \brief Lock table implementation class.
* \details
* This is the gut of lock management in Foster B-trees.
* \section Synchronization
* Lock heads, buckets, mutexes, etc.
* Refactored a lot, but still complex!
* We should remove mutexes from main code path like LIL. but long way to go..
* \section Dreadlock Deadlock Detection
* _check_deadlock() implements "Dreadlocks" Deadlock Detector Algorithm,
* based on the algorithm by Eric Koshkinen and Maurice Herlihy.
*
* Each thread chooses three random bits (without
* replacement) from a bitmap as its fingerprint ("hash function").
* 
* Whenever a  thread must wait on a lock, it sets its 
* bitmap to its fingerprint and then to the
* bitwise OR of its own bitmap and the bitmap of its predecessor. 
*
* Its predecessor is some thread waiting on the same lock that the
* thread is trying to acquire but must wait for.
* There is a lot of potential for races in here, threads waking up
* just before the dld tries to unblock them, false positives, etc. 
* For each predecessor, we OR in the wait map for that thread's xct.
* When computing the bitwise OR, if looks for its own fingerprint in
* the predecessor's bitmap: this would indicate a very likely deadlock. 
*
* Upon detecting a deadlock then chooses the younger transaction
* of the two (xcts attached to the two threads) as the victim.
*
* NOTE:
* -- dead lock detection doesn't actually do an abort -- somewhere up
*  the call chain, eDEADLOCK is detected and the caller aborts.
*
* -- the predecessor thread cannot be attached to the same transaction as
*  the thread running the deadlock detection. This is enforced by the
*  the fact that at most one thread of an xct can be in the lock manager
*  at a time (unless blocked waiting), 
*  by locking the xct_lock_info_t mutex (freed while blocking waiting
*  for a lock), and by the lock manager checking waiting_request() to
*  disallow two threads waiting at once.
*
* -- the algorithm only has to traverse the lock_head queue for the
*  lock we are trying to acquire; it does not have to traverse any
*  other queues. At the time it traverses this queue, it holds the
*  lock head's mutex so it should be safe.
*
* -- Herlihy, et al has you spinning on the lock's free status as
*  well as on the wait map changing. We don't do this, as it's not
*  really feasible here.  It is our calling _check_deadlock in a
*  loop that effects the spin on both the wait map. Whenever we
*  stop blocking, we clean our wait map and restart the DLD. This
*  is how updates to other xct wait maps are propagated to us.
*
*  -- We can (and do) have multiple simultaneous victims for the
*  same cycle, by virtue of the fact that any number of xct/threads 
*  in the cycle can be doing simultaneous DLD.  Picking the younger
*  xct of a pair is all we do to reduce the likelihood of this
*  happening. 
*
*  In the hope of minimizing the chance of having too-dense wait maps,
*  we have chosen to associate the fingerprints with the smthread_t,
*  under the assumption that a server will create a pool of smthreads
*  to run the client requests, and so there are likely to be
*  fewer smthreads than total xcts serviced.  An xct can participate
*  in a cycle only if it's got an attached thread, and any
*  xct that has no attached threads has an empty wait map.  
*  This way, if we needed to do so, we could insert code in smthreads
*  to check the uniqueness of a map, and also the density of the
*  union of all smthread wait maps.  It increases the cost of
*  starting an smthread, but that should not be in the critical
*  path of a server.
*/
class lock_core_m : public lock_base_t{
public:
    typedef lock_base_t::lmode_t lmode_t;

    NORET        lock_core_m(uint sz);
    NORET        ~lock_core_m();

    int          collect(vtable_t&, bool names_too);

    /**
    * Unsafely check that the lock table is empty. For debugging -
    *  and assertions at shutdown, when MT-safety shouldn't be an issue.
    */
    void        assert_empty() const;
    void        dump();
    void        dump(ostream &o);
    /**
    *  Unsafely dump the lock hash table (for debugging).
    *  Doesn't acquire the mutexes it should for safety, but allows
    *  you dump the table while inside the lock manager core.
    */
    void        _dump(ostream &o);

    
    lil_global_table*   get_lil_global_table() { return _lil_global_table; }

public:
    w_rc_t::errcode_t  acquire_lock(
                xct_t*            xd,
                const lockid_t&   name,
                lmode_t           mode,
                lmode_t&          prev_mode,
                bool              check_only,
                timeout_in_ms     timeout);

    //* Requires request->_thr is the current thread
    void        release_lock(
                lock_queue_t*      lock,
                lock_queue_entry_t*   request,
                lsn_t             commit_lsn = lsn_t::null);

    rc_t        release_duration(
                xct_lock_info_t*    theLockInfo,
                bool read_lock_only = false,
                lsn_t commit_lsn = lsn_t::null
                );

private:
    uint32_t        _table_bucket(uint32_t id) const { return id % _htabsz; }

    /** returned from following internal functions. */
    enum acquire_lock_ret {
        RET_SUCCESS,
        RET_TIMEOUT,    
        RET_DEADLOCK
    };
    int _acquire_lock(
        xct_t*                 xd,
        lock_queue_t*          lock,
        lmode_t                mode,
        lmode_t&               prev_mode,
        bool                   check_only,
        timeout_in_ms          timeout,
        xct_lock_info_t*       the_xlinfo);
    int _acquire_lock_loop(
        smthread_t*            thr,
        lock_queue_t*          lock,
        lock_queue_entry_t*    req,
        bool                   check_only,
        timeout_in_ms          timeout,
        xct_lock_info_t*       the_xlinfo);
    
    bucket_t*           _htab;
    uint32_t            _htabsz;
    
    /** Global lock table for Light-weight Intent Lock. */
    lil_global_table*  _lil_global_table;
};

// this is for experiments to compare deadlock detection/recovery methods.
#define SWITCH_DEADLOCK_IMPL
#ifdef SWITCH_DEADLOCK_IMPL
/** Whether to use the dreadlock sleep-backoff algorithm? */
extern bool g_deadlock_use_waitmap_obsolete;
/** How long to sleep between each dreadlock spin? */ 
extern int g_deadlock_dreadlock_interval_ms;
/** function pointer for the implementation of arbitrary _check_deadlock impl. */ 
extern w_rc_t::errcode_t (*g_check_deadlock_impl)(xct_t* xd, lock_request_t *myreq);
#endif // SWITCH_DEADLOCK_IMPL

#endif          /*</std-footer>*/
