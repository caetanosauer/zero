/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore' incl-file-exclusion='SMTHREAD_H'>

 $Id: smthread.h,v 1.106 2010/12/08 17:37:43 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef SMTHREAD_H
#define SMTHREAD_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/**\file smthread.h
 * \ingroup MACROS
 */

#include <assert.h>
#ifndef W_H
#include <w.h>
#endif
#ifndef SM_BASE_H
#include <sm_base.h>
#endif
#include <w_bitvector.h>

#include <mutex>
#include <memory>
#include <list>

#include "timeout.h"
#include "latches.h"
#include "logrec.h"
#include "smstats.h"

class xct_t;
class lockid_t;

class smthread_t;


/**\cond skip */
enum { FINGER_BITS=SM_DREADLOCK_FINGERS };
typedef w_bitvector_t<SM_DREADLOCK_BITCOUNT>    sm_thread_map_t;
/**\endcond skip */

/**\brief Fingerprint for this smthread.
 * \details
 * Each smthread_t has a fingerprint. This is used by the
 * deadlock detector.  The fingerprint is a bitmap; each thread's
 * bitmap is unique, the deadlock detector ORs fingerprints together
 * to make a "digest" of the waits-for-map.
 * Rather than have fingerprints associated with transactions, we
 * associate them with threads.
 *
 * This class provides synchronization (protection) for updating the map.
 *
 * \note: If you want to be sure the fingerprints are unique, for the
 * purpose of eliminating false-positives in the lock manager's deadlock
 * detector while debugging something, look at the code
 * in smthread_t::_initialize_fingerprint(), where you can
 * enable some debugging code.
 * (There is no need to make them unique -- if there were,
 * we'd use 1 bit per... -- but if you are debugging something you might
 * want to ensure or detect uniqueness for that purpose.)
 */
typedef sm_thread_map_t atomic_thread_map_t;
/*
    We do NOT need synchronization for Dreadlock bitmap.
    We freshly re-compute bitmaps at each spin (no false-negatives in long run),
    and we tolerate occasional false positives.
    So, atomic_thread_map_t is no longer needed.

class  atomic_thread_map_t : public sm_thread_map_t {
private:
    // mutable srwlock_t   _map_lock;
public:

    bool has_reader() const {
        return _map_lock.has_reader();
    }
    bool has_writer() const {
        return _map_lock.has_writer();
    }
    void lock_for_read() const {
        _map_lock.acquire_read();
    }
    void lock_for_write() {
        _map_lock.acquire_write();
    }
    void unlock_reader() const{
        w_assert2(_map_lock.has_reader());
        _map_lock.release_read();
    }
    void unlock_writer() {
        w_assert2(_map_lock.has_writer());
        _map_lock.release_write();
    }
    atomic_thread_map_t () {
        w_assert1(_map_lock.has_reader() == false);
        w_assert1(_map_lock.has_writer() == false);
    }
    ~atomic_thread_map_t () {
        w_assert1(_map_lock.has_reader() == false);
        w_assert1(_map_lock.has_writer() == false);
    }

    atomic_thread_map_t &operator=(const atomic_thread_map_t &other) {
        // Copy only the bitmap portion; do not touch the
        // _map_lock
#if W_DEBUG_LEVEL > 0
        bool X=_map_lock.has_reader();
        bool Y=_map_lock.has_writer();
#endif
        copy(other);
#if W_DEBUG_LEVEL > 0
        w_assert1(_map_lock.has_reader() == X);
        w_assert1(_map_lock.has_writer() == Y);
#endif
        return *this;
    }
};
*/

/**\brief Storage Manager thread.
 * \ingroup SSMINIT
 * \details
 * \attention
 * All threads that use storage manager functions must be of this type
 * or of type derived from this.
 *
 * Associated with an smthread_t is a POSIX thread (pthread_t).
 * This class is in essence a wrapper around POSIX threads.
 * The maximum number of threads a server can create depends on the
 * availability of resources internal to the pthread implementation,
 * (in addition to system-wide parameters), so it is not possible
 * \e a \e priori to determine whether creation of a new thread will
 * succeed.
 * Failure will result in a fatal error.
 *
 * The storage manager keeps its own thread-local state and provides for
 * a little more control over the starting of threads than does the
 * POSIX interface:  you can do meaningful work between the time the
 * thread is \e created and the time it starts to \e run.
 * The thread constructor creates the underlying pthread_t, which then
 * awaits permission (a pthread condition variable)
 * to continue (signalled by smthread_t::fork).
 */
class smthread_t {
    friend class smthread_init_t;
    struct tcb_t {
        // CS: moved out of xct_t
        logrec_t _logbuf;
        logrec_t _logbuf2; // for "piggy-backed" SSX

        xct_t*   xct;
        int      pin_count;      // number of rsrc_m pins
        int      prev_pin_count; // previous # of rsrc_m pins
        int lock_timeout;    // timeout to use for lock acquisitions
        bool    _in_sm;      // thread is in sm ss_m:: function
        bool    _is_update_thread;// thread is in update function

        int16_t  _depth; // how many "outer" this has
        tcb_t*   _outer; // this forms a singly linked list

        sm_stats_t  _TL_stats; // thread-local stats

        // for lock_head_t::my_lock::get_me
        queue_based_lock_t::ext_qnode _me1;
        queue_based_lock_t::ext_qnode _me2;
        queue_based_lock_t::ext_qnode _me3;
        /**\var static __thread queue_based_lock_t::ext_qnode _xlist_mutex_node;
         * \brief Queue node for holding mutex to serialize
         * access transaction list. Used in xct.cpp
         */
        queue_based_lock_t::ext_qnode _xlist_mutex_node;

        // force this to be 8-byte aligned:

        void    create_TL_stats();
        void    clear_TL_stats();
        void    destroy_TL_stats();
        inline sm_stats_t& TL_stats() { return _TL_stats;}
        inline const sm_stats_t& TL_stats_const() const {
                                                 return _TL_stats; }

        tcb_t(tcb_t* outer) :
            xct(0),
            pin_count(0),
            prev_pin_count(0),
            lock_timeout(timeout_t::WAIT_FOREVER), // default for a thread
            _in_sm(false),
            _is_update_thread(false),
            _depth(outer == NULL ? 1 : outer->_depth + 1),
            _outer(outer)
        {
            QUEUE_EXT_QNODE_INITIALIZE(_me1);
            QUEUE_EXT_QNODE_INITIALIZE(_me2);
            QUEUE_EXT_QNODE_INITIALIZE(_me3);
            QUEUE_EXT_QNODE_INITIALIZE(_xlist_mutex_node);

            create_TL_stats();
        }

        ~tcb_t()
        {
            destroy_TL_stats();
        }
    };

public:

    // bool               _try_initialize_fingerprint(); // true: failure false: ok
    // void               _initialize_fingerprint();
    // void               _uninitialize_fingerprint();
    // short              _fingerprint[FINGER_BITS]; // dreadlocks
    // atomic_thread_map_t  _fingerprint_map; // map containing only fingerprint

public:
    // const atomic_thread_map_t&  get_fingerprint_map() const
    //                         {   return _fingerprint_map; }
    static void timeout_to_timespec(int timeout_ms, struct timespec &when);

public:

    /**\cond skip */
    /* public for debugging */
    static void          init_fingerprint_map();
    /**\endcond skip */


    /**\cond skip
     **\brief Attach this thread to the given transaction.
     * \ingroup SSMXCT
     * @param[in] x Transaction to attach to the thread
     * \details
     * Attach this thread to the transaction \a x or, equivalently,
     * attach \a x to this thread.
     * \note "this" thread need not be the running thread.
     *
     * Only one transaction may be attached to a thread at any time.
     * More than one thread may attach to a transaction concurrently.
     */
    static void             attach_xct(xct_t* x);
    /**\brief Detach this thread from the given transaction.
     * \ingroup SSMXCT
     * @param[in] x Transaction to detach from the thread.
     * \details
     * Detach this thread from the transaction \a x or, equivalently,
     * detach \a x from this thread.
     * \note "this" thread need not be the running thread.
     *
     * If the transaction is not attached, returns error.
     * \endcond skip
     */
    static void             detach_xct(xct_t* x);

    /// get lock_timeout value
    static inline
    int        lock_timeout() {
                    return tcb().lock_timeout;
                }
    /**\brief Set lock_timeout value
     * \details
     * You can give a value WAIT_FOREVER, WAIT_IMMEDIATE, or
     * a positive millisecond value.
     * Every lock request made with WAIT_SPECIFIED_BY_THREAD will
     * use this value.
     *
     * A transaction can be given its own timeout on ss_m::begin_xct.
     * The transaction's lock timeout is used for every lock request
     * made with WAIT_SPECIFIED_BY_XCT.
     * A transaction begun with WAIT_SPECIFIED_BY_THREAD will use
     * the thread's lock_timeout for the transaction timeout.
     *
     * All internal storage manager lock requests use WAIT_SPECIFIED_BY_XCT.
     * Since the transaction can defer to the per-thread timeout, the
     * client has control over which timeout to use by choosing the
     * value given at ss_m::begin_xct.
     */
    static inline
    void             set_lock_timeout(int i) {
                    tcb().lock_timeout = i;
                }

    static logrec_t* get_logbuf()
    {
        return &tcb()._logbuf;
    }

    static logrec_t* get_logbuf2()
    {
        return &tcb()._logbuf2;
    }

    /// return xct this thread is running
    static inline xct_t* xct() { return tcb().xct; }

    /// Return thread-local statistics collected for this thread.
    static inline sm_stats_t& TL_stats() { return tcb().TL_stats(); }

    /// Add thread-local stats into the given structure.
    static void add_from_TL_stats(sm_stats_t &w);

    // NOTE: These macros don't have to be atomic since these thread stats
    // are stored in the smthread and collected when the smthread's tcb is
    // destroyed.

#define GET_TSTAT(x) smthread_t::TL_stats()[enum_to_base(sm_stat_id::x)]
/**\def GET_TSTAT(x)
 *\brief Get per-thread statistic named x
*/

/**\def INC_TSTAT(x)
 *\brief Increment per-thread statistic named x by y
 */
#define INC_TSTAT(x) smthread_t::TL_stats()[enum_to_base(sm_stat_id::x)]++

/**\def ADD_TSTAT(x,y)
 *\brief Increment statistic named x by y
 */
#define ADD_TSTAT(x,y) smthread_t::TL_stats()[enum_to_base(sm_stat_id::x)] += (y)

/**\def SET_TSTAT(x,y)
 *\brief Set per-thread statistic named x to y
 */
#define SET_TSTAT(x,y) smthread_t::TL_stats()[enum_to_base(sm_stat_id::x)] = (y)


    /**\cond skip */
    /*
     *  These functions are used to verify than nothing is
     *  left pinned accidentally.  Call mark_pin_count before an
     *  operation and check_pin_count after it with the expected
     *  number of pins that should not have been realeased.
     */
    static void             mark_pin_count();
    static void             check_pin_count(int change);
    static void             check_actual_pin_count(int actual) ;
    static void             incr_pin_count(int amount) ;
    static int              pin_count() ;

    /*
     *  These functions are used to verify that a thread
     *  is only in one ss_m::, scan::, or pin:: function at a time.
     */
    static inline
    void             in_sm(bool in)    { tcb()._in_sm = in; }
    static inline
    bool             is_in_sm() { return tcb()._in_sm; }

    static inline
    bool             is_update_thread() {
                                        return tcb()._is_update_thread; }
    static inline
    void             set_is_update_thread(bool in) { tcb()._is_update_thread = in; }

    static void             no_xct(xct_t *);

    virtual void     _dump(ostream &) const; // to be over-ridden
    /**\endcond skip */

    /* thread-level block() and unblock aren't public or protected
       accessible.
       These methods are used by the lock manager.
       Otherwise, ordinarly pthreads sychronization variables
       are used.
    */
    // w_error_codes smthread_block(int WAIT_FOREVER,
    //                   const char * const caller = 0,
    //                   const void * id = 0);
    // w_rc_t            smthread_unblock(w_error_codes e);

    // int sampling;
// private:
    // w_error_codes _smthread_block( int WAIT_FOREVER,
    //                           const char * const why =0);
    // w_rc_t           _smthread_unblock(w_error_codes e);

public:
    /**\brief  TLS variables Exported to sm.
     */
    static queue_based_lock_t::ext_qnode& get_me3() { return tcb()._me3; }
    static queue_based_lock_t::ext_qnode& get_me2() { return tcb()._me2; }
    static queue_based_lock_t::ext_qnode& get_me1() { return tcb()._me1; }
    static queue_based_lock_t::ext_qnode& get_xlist_mutex_node() {
                                               return tcb()._xlist_mutex_node;}
private:
    /* sm-specific block / unblock implementation */
    bool            _waiting;

    /** returns the transaction object that is currently active.*/
    /**
     * Stack of transactions this thread conveys.
     * tail is the top of the stack, which is the transaction currently
     * outputting logs or processing REDO/UNDOs.
     * The head is always an empty tcb_t instance. The instance is not actually
     * needed, but it makes easier to work with existing codes
     * (which assume only one tcb_t instance).
     */
    static tcb_t*& tcb_ptr()
    {
        // static local var -- initialized only on first call
        static thread_local tcb_t* tcb = new tcb_t(nullptr);
        return tcb;
    }

    static tcb_t& tcb()
    {
        auto ret = tcb_ptr();
        w_assert3(ret);
        return *ret;
    }

public:
    /** Tells how many transactions are nested. */
    static inline size_t get_tcb_depth() { return tcb()._depth; }

private:

    class GlobalThreadList {
    public:
        void add_me()
        {
            std::unique_lock<std::mutex> lck {_mutex};
            _tcb_list.push_back(tcb_ptr());
        }

        void remove_me()
        {
            std::unique_lock<std::mutex> lck {_mutex};
            auto iter = _tcb_list.begin();
            while (iter != _tcb_list.end()) {
                if (*iter == tcb_ptr()) {
                    iter = _tcb_list.erase(iter);
                }
                else { iter++; }
            }
        }

        template <typename F>
        void for_each_stats(F& f)
        {
            std::unique_lock<std::mutex> lck {_mutex};
            for (auto p : _tcb_list) {
                f(p->TL_stats());
            }
        }

    private:
        std::list<tcb_t*> _tcb_list;
        std::mutex _mutex;
    };

    static std::shared_ptr<GlobalThreadList> thread_list()
    {
        static auto ptr = std::make_shared<GlobalThreadList>();
        return ptr;
    }

public:

    static void add_me_to_thread_list()
    {
        thread_list()->add_me();
    }

    static void remove_me_from_thread_list()
    {
        thread_list()->remove_me();
    }

    template <typename F>
    static void for_each_thread_stats(F& f)
    {
        thread_list()->for_each_stats(f);
    }
};

inline xct_t* xct()
{
    return smthread_t::xct();
}

/**\cond skip */
class smthread_init_t {
public:
    NORET            smthread_init_t();
    NORET            ~smthread_init_t();
private:
    static int       count;
};
/**\endcond  skip */

/**\cond skip */

inline void
smthread_t::mark_pin_count()
{
    tcb().prev_pin_count = tcb().pin_count;
}

inline void
smthread_t::check_pin_count(int W_IFDEBUG4(change))
{
#if W_DEBUG_LEVEL > 3
    int diff = tcb().pin_count - tcb().prev_pin_count;
    if (change >= 0) {
        w_assert4(diff <= change);
    } else {
        w_assert4(diff >= change);
    }
#endif
}

inline void
smthread_t::check_actual_pin_count(int actual)
{
    w_assert3(tcb().pin_count == actual);
}


inline void
smthread_t::incr_pin_count(int amount)
{
    tcb().pin_count += amount;
}

inline int
smthread_t::pin_count()
{
    return tcb().pin_count;
}

void
DumpBlockedThreads(ostream& o);

/*
 * redefine DBGTHRD to use our threads
 */
#ifdef DBGTHRD
#undef DBGTHRD
#endif
#define DBGTHRD(arg) DBG(<< " th." << std::this_thread::get_id() << " " arg)

/**\endcond skip */


/*<std-footer incl-file-exclusion='SMTHREAD_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
