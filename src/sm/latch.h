// -*- mode:c++; c-basic-offset:4 -*-
/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
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

/*<std-header orig-src='shore' incl-file-exclusion='LATCH_H'>

 $Id: latch.h,v 1.35 2010/07/07 20:50:11 nhall Exp $

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

#ifndef LATCH_H
#define LATCH_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include "smthread.h"
#include "latches.h"
#include <list>
#include <thread>

/**
 * \enum latch_mode_t
 *
 * Increasing values indicate increasing degrees of privilege; that is lock mode l, l>k,
 * allows at least as many operations as mode k.  I.e., EX > SH > Q > NL.
 *
 * If you alter this, also change the corresponding interface and definition of
 * latch_mode_str.
 */
enum latch_mode_t { LATCH_NL = 0, LATCH_SH = 2, LATCH_EX = 3 };

/// type of a Q mode ticket; exact type and location of definition TBD
typedef int64_t q_ticket_t;


class latch_t;
extern ostream &operator<<(ostream &, const latch_t &);

/** \brief Indicates a latch is held by this thread.
 *
 *\details Every time we want to grab a latch,
 * we have to create a latch_holder_t.
 * We do that with the holder_search class,
 * which searches a TLS list to make sure  we(this thread)
 * doesn't already hold the latch, and, if not,
 * it creates a new latch_holder_t for the new latch acquisition.
 * It then stuffs the latch_holder_t in the TLS list.
 * If we do already have hold the latch in some capacity,
 * the holder_search returns that existing latch_holder_t.
 * \sa holder_search
 */
class latch_holder_t
{
public:

    static __thread latch_holder_t* thread_local_holders;
    static __thread latch_holder_t* thread_local_freelist;

    latch_t*     _latch;
    latch_mode_t _mode;
    int          _count;
private:
    std::thread::id   _threadid; // REMOVE ME (for debuging)

    // disabled
    latch_holder_t &operator=(latch_holder_t const &other);
public:
    // internal freelist use only!
    latch_holder_t* _prev;
    latch_holder_t* _next;

    latch_holder_t()
    : _latch(NULL), _mode(LATCH_NL), _count(0)
    {
        _threadid = std::this_thread::get_id();
    }

    bool operator==(latch_holder_t const &other) const {
        if(_threadid != other._threadid) return false;
        return _latch == other._latch &&
              _mode == other._mode && _count == other._count;
    }

    void print(ostream &o) const;
};

#include <iosfwd>

/**\brief A short-term hold (exclusive or shared) on a page.
 *
 * \details
 * A latch may be acquire()d multiple times by a single thread.
 * The mode of subsequent acquire()s must be at or above the level
 * of the currently held latch.
 * Each of these individual locks must be released.
 * \sa latch_holder_t
 */
class latch_t /*: public sthread_named_base_t*/ {

public:
    /// Create a latch
    latch_t();
    ~latch_t();

    /// Destroy TLS structures
    static void on_thread_destroy();

    // Dump latch info to the ostream. Not thread-safe.
    ostream&                print(ostream &) const;

    // Return a unique id for the latch.For debugging.
    inline const void *     id() const { return &_lock; }

    /// Change the name of the latch.
    inline void             setname(const char *const desc);

    /// Acquire the latch in given mode. \sa  timeout_t.
    w_rc_t                  latch_acquire(
                                latch_mode_t             m,
                                int timeout = timeout_t::WAIT_FOREVER);
    /**\brief Upgrade from SH to EX if it can be done w/o blocking.
     * \details Returns bool indicating if it would have blocked, in which
     * case the upgrade did not occur. If it didn't have to block, the
     * upgrade did occur.
     * \note Does \b not increment the count.
     */
    w_rc_t                  upgrade_if_not_block(bool& would_block);

    /**\brief Convert atomically an EX latch into an SH latch.
     * \details
     * Does not decrement the latch count.
     */
    void                    downgrade();

    /**\brief release the latch.
     * \details
     * Decrements the latch count and releases only when
	 * it hits 0.
	 * Returns the resulting latch count.
     */
    int                     latch_release();
    /**\brief Unreliable, but helpful for some debugging.
     */
    bool                    is_latched() const;

    /*
     * GNATS 30 fix: changes lock_cnt name to latch_cnt,
     * and adds _total_cnt to the latch structure itself so it can
     * keep track of the total #holders
     * This is an additional cost, but it is a great debugging aid.
     * \todo TODO: get rid of BUG_LATCH_SEMANTICS_FIX: replace with gnats #
     */

    /// Number of acquires.  A thread may hold more than once.
    int                     latch_cnt() const {return _total_count;}

    /// How many threads hold the R/W lock.
    int                     num_holders() const;
    ///  True iff held in EX mode.
    bool                    is_mine() const; // only if ex
    ///  True iff held in EX or SH mode.  Actually, it returns the
	//latch count (# times this thread holds the latch).
    int                     held_by_me() const; // sh or ex
    ///  EX,  SH, or NL (if not held at all).
    latch_mode_t            mode() const;

    /// string names of modes.
    static const char* const    latch_mode_str[4];

private:
    // found, iterator
    w_rc_t                _acquire(latch_mode_t m,
                                 int timeout_in_ms,
                                 latch_holder_t* me);
	// return #times this thread holds the latch after this release
    int                   _release(latch_holder_t* me);
    void                  _downgrade(latch_holder_t* me);

/*
 * Note: the problem with #threads and #cpus and os preemption is real.
 * And it causes things to hang; and it's hard to debug, in the sense that
 * using pthread facilities gives thread-analysis tools and debuggers
 * understood-things with which to work.
 * Consequently, we use w_pthread_rwlock for our lock.
 */
    mutable srwlock_t           _lock;

    // disabled
    NORET                        latch_t(const latch_t&);
    latch_t&                     operator=(const latch_t&);

    uint32_t            _total_count;
};

inline bool
latch_t::is_latched() const
{
    /* NOTE: Benign race -- this function is naturally unreliable, as
     * its return value may become invalid as soon as it is
     * generated. The only way to reliably know if the lock is held at
     * a particular moment is to hold it yourself, which defeats the
     * purpose of asking in the first place...
     * ... except for assertions / debugging... since there are bugs
     * in acquire/release of latches
     */
    return _lock.is_locked();
}

inline int
latch_t::num_holders() const
{
    return _lock.num_holders();
}


inline latch_mode_t
latch_t::mode() const
{
    switch(_lock.mode()) {
    case mcs_rwlock::NONE: return LATCH_NL;
    case mcs_rwlock::WRITER: return LATCH_EX;
    case mcs_rwlock::READER: return LATCH_SH;
    default: w_assert1(0); // shouldn't ever happen
             return LATCH_SH; // keep compiler happy
    }
}

// unsafe: for use in debugger:
extern "C" void print_my_latches();
extern "C" void print_all_latches();
extern "C" void print_latch(const latch_t *l);

/*<std-footer incl-file-exclusion='LATCH_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
