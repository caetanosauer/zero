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

/*<std-header orig-src='shore'>

 $Id: latch.cpp,v 1.49 2010/12/08 17:37:34 nhall Exp $

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

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include "w.h"
#include "latch.h"
#include "w_debug.h"
#include "smthread.h"

#include <cstring>
#include <list>
#include <algorithm>

const char* const  latch_t::latch_mode_str[4] = { "NL", "Q", "SH", "EX" };


latch_t::latch_t() :
    _total_count(0)
{
}

latch_t::~latch_t()
{
#if W_DEBUG_LEVEL > 1
    int t = _total_count;
    // do this just to get the symbol to remain
    if(t) {
        fprintf(stderr, "t=%d\n", t);
    }

    // Should only check if the system is not doing dirty shutdown (ss_m::shutdown_clean)
    // But we don't have a way to check this flag from latch object
////////////////////////////////////////
// TODO(Restart)... comment out the assertion in debug mode for 'instant restart' testing purpose
//                     if we are using simulated crash shutdown, the following assertion might fire if
//                     we are in the middle of taking a checkpoint
//                    need a way to ignore latch count checking if using simulated system crash
//
//                    For now, comment out the assertion, although we might miss other
//                    bugs by comment out the assertion
////////////////////////////////////////

//    w_assert2(t == 0);// BUG_SEMANTICS_FIX

//    w_assert2(mode() == LATCH_NL);
//    w_assert2(num_holders() == 0);

#endif
}


/**\var static __thread latch_holder_t* latch_holder_t::thread_local_holders;
 * \brief Linked list of all latches held by this thread.
 * \ingroup TLS
 *
 * \details
 * Every time we want to grab a latch,
 * we have to create a latch_holder_t; we do that with the
 * holder_search class, which searches the per-thread list below
 * to make sure we `(this thread) don't already hold the latch and
 * if not, it creates a new latch_holder_t for the new latch acquisition,
 * and stuffs the latch_holder_t in this list.
 * If we do already have hold the latch in some capacity, the
 * holder_search returns that existing latch_holder_t.
 * So we can tell if this thread holds a given latch, and we can
 * find all latches held by this thread, but we can't find
 * all the holders of a given latch.
 *
 * \sa latch_holder_t
 */
__thread latch_holder_t* latch_holder_t::thread_local_holders(NULL);

/**\var static __thread latch_holder_t* latch_holder_t::thread_local_freelist;
 * \brief Pool of unused latch_holder_t instances.
 * \ingroup TLS
 *
 * \details
 * Ready for recycling.  These structures are first taken from the global heap
 * but put on this list for reuse rather than ::free-ed.
 * When the thread is destroyed, the items on this list are returned
 * to the global heap.
 *
 * \sa latch_holder_t
 */
__thread latch_holder_t* latch_holder_t::thread_local_freelist(NULL);

/**\brief The list-handling class for latch_holder_t instances.
 *
 * \details
 * Really, all this does is provide an iterator and a means to
 * insert (push_front)  and remove (unlink) these things.
 *
 * The list contents are always instances of latch_holder_t, which
 * have an internal link for creating the list.
 */
class holder_list
{
    latch_holder_t* &_first;
public:
    holder_list(latch_holder_t* &first) : _first(first) { }

    /**\brief Iterator over a list of latch_holder_t structures */
    struct iterator {
        latch_holder_t* _cur;
        public:

        /// Construct an iterator starting with the given latch_holder_t.
        explicit iterator(latch_holder_t* cur) : _cur(cur) { }

        /// Get current.
        operator latch_holder_t*() const { return _cur; }

        /// Get current.
        latch_holder_t* operator->() const { return *this; }

        ///  Make iterator point to next.
        iterator &operator++() { _cur = _cur->_next; return *this; }

        ///  Make iterator point to next.
        iterator operator++(int) { return ++iterator(*this); }
    };

    /// Dereferencing this iterator brings us to the first item in the list.
    iterator begin() { return iterator(_first); }

    /// Dereferencing this iterator brings us past the last item in any list.
    iterator end() { return iterator(NULL); }

    /// Insert h at the front of this list.
    void push_front(latch_holder_t* h) {
        h->_next = _first;
        if(_first) _first->_prev = h;
        h->_prev = NULL;
        _first = h;
    }

    /// Remove whatever is the current item for the given iterator.
    latch_holder_t* unlink(iterator const &it) {
        if(it->_next)
            it->_next->_prev = it->_prev;

        if(it->_prev)
            it->_prev->_next = it->_next;
        else
            _first = it->_next;

        // now it's orphaned...
        return it;
    }
};

/**\class holders_print
 * \brief For debugging only.
 *
 * \details
 *
 * Constructor looks through all the holders in the
 * implied list starting with the latch_holder_t passed in as the sole
 * constructor argument.
 *
 * It prints info about each latch_holder_t in the list.
 *
 * \sa latch_holder_t.
 */
class  holders_print
{
private:
    holder_list _holders;
    void print(holder_list holders)
    {
        holder_list::iterator it=holders.begin();
        for(; it!=holders.end() && it->_latch;  ++it)
        {
            it->print(cerr);
        }
    }
public:
    holders_print(latch_holder_t *list)
    : _holders(list)
    {
        print(_holders);
    }
};

/**\class holder_search
 * \brief Finds all latches held by this thread.
 *
 * \details
 * Searches a thread-local list for a latch_holder_t that is a
 * reference to the given latch_t.
 *
 * \sa latch_holder_t.
 */
class holder_search
{
public:
    /// find holder of given latch in given list
    static holder_list::iterator find(holder_list holders, latch_t const* l)
    {
        holder_list::iterator it=holders.begin();
        for(; it!=holders.end() && it->_latch != l; ++it) ;
        return it;
    }

    /// count # times we find a given latch in the list. For debugging, asserts.
    static int count(holder_list holders, latch_t const* l)
    {
        holder_list::iterator it=holders.begin();
        int c=0;
        for(; it!=holders.end(); ++it) if(it->_latch == l) c++;
        return c;
    }

private:
    holder_list _holders;
    latch_holder_t* &_freelist;
    holder_list::iterator _end;
    holder_list::iterator _it;

public:
    /// Insert latch_holder_t for given latch if not already there.
    holder_search(latch_t const* l)
        : _holders(latch_holder_t::thread_local_holders),
          _freelist(latch_holder_t::thread_local_freelist),
          _end(_holders.end()),
          _it(find(_holders, l))
    {
        // if we didn't find the latch in the list,
        // create a new latch_holder_t (with mode LATCH_NL)
        // to return, just so that the value() method always
        // returns a non-null ptr.  It might be used, might not.
        if(_it == _end) {
            latch_holder_t* h = _freelist;
            if(h) _freelist = h->_next;
            // need to clear out the latch either way
            if(h)
                // h->latch_holder_t(); // reinit
                h = new(h) latch_holder_t();
            else
                h = new latch_holder_t;
            _holders.push_front(h);
            _it = _holders.begin();
        }
        w_assert2(count(_holders, l) <= 1);
    }

    ~holder_search()
    {
        if(_it == _end || _it->_mode != LATCH_NL)
            return;

        // don't hang onto it in the holders list  if it's not latched.
        latch_holder_t* h = _holders.unlink(_it);
        h->_next = _freelist;
        _freelist = h;
    }

    latch_holder_t* operator->() { return this->value(); }

    latch_holder_t* value() { return (_it == _end)?
        (latch_holder_t *)(NULL) : &(*_it); }
}; // holder_search

w_rc_t
latch_t::latch_acquire(latch_mode_t mode, int timeout_in_ms)
{
    w_assert1(mode != LATCH_NL);
    holder_search me(this);
    return _acquire(mode, timeout_in_ms, me.value());
}

w_rc_t
latch_t::upgrade_if_not_block(bool& would_block)
{
    DBGTHRD(<< " want to upgrade " << *this );
    holder_search me(this);

    // should already hold the latch
    w_assert3(me.value() != NULL);

    // already hold EX? DON'T INCREMENT THE COUNT!
    if(me->_mode == LATCH_EX) {
        would_block = false;
        return RCOK;
    }

    w_rc_t rc = _acquire(LATCH_EX, timeout_t::WAIT_IMMEDIATE, me.value());
    if(rc.is_error()) {
        // it never should have tried to block
        w_assert3(rc.err_num() != stTIMEOUT);
        if(rc.err_num() != stINUSE)
            return RC_AUGMENT(rc);

        would_block = true;
    }
    else {
        // upgrade should not increase the lock count
        lintel::unsafe::atomic_fetch_sub(&_total_count, 1);
        me->_count--;
        would_block = false;
    }
    return RCOK;
}

int latch_t::latch_release()
{
    holder_search me(this);
    // we should already hold the latch!
    w_assert2(me.value() != NULL);
    return _release(me.value());
}

w_rc_t latch_t::_acquire(latch_mode_t new_mode,
    int timeout,
    latch_holder_t* me)
{
    DBGTHRD( << "want to acquire in mode "
            << W_ENUM(new_mode) << " " << *this
            );
    w_assert2(new_mode != LATCH_NL);
    w_assert2(me);

    bool is_upgrade = false;
    if(me->_latch == this)
    {
        // we already hold the latch
        w_assert2(me->_mode != LATCH_NL);
        w_assert2(mode() == me->_mode);
        // note: _mode can't change while we hold the latch!
        if(mode() == LATCH_EX) {
            w_assert2(num_holders() == 1);
            // once we hold it in EX all later acquires default to EX as well
            new_mode = LATCH_EX;
        } else {
            w_assert2(num_holders() >= 1);
        }
        if(me->_mode == new_mode) {
            DBGTHRD(<< "we already held latch in desired mode " << *this);
            lintel::unsafe::atomic_fetch_add(&_total_count, 1);// BUG_SEMANTICS_FIX
            me->_count++; // thread-local
            // fprintf(stderr, "acquire latch %p %dx in mode %s\n",
            //        this, me->_count, latch_mode_str[new_mode]);
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
            // These are counted in bf statistics.
            // but if we don't count them here, we will get
            // a misleading impression of the wait counts
            // is_upgrade is figured w/o consideraton whether request is
            // conditional/unconditional, but we consider it
            // uncondl because the unconditional case is
            // the one we're trying to understand in the callers
            // (bf find, bf scan, btree latch
            INC_TSTAT(latch_uncondl_nowait);
#endif
            return RCOK;
        } else if(new_mode == LATCH_EX && me->_mode == LATCH_SH) {
            is_upgrade = true;
        }
    } else {
        // init 'me' (latch holder) for the critical section
        me->_latch = this;
        me->_mode = LATCH_NL;
        me->_count = 0;
    }

    // have to acquire for real

    if(is_upgrade) {
        // to avoid deadlock,
        // never block on upgrade
        if(!_lock.attempt_upgrade())
            return RC(stINUSE);

        w_assert2(me->_count > 0);
        w_assert2(new_mode == LATCH_EX);
        me->_mode = new_mode;
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
        // These are counted in bf statistics.
        // but if we don't count them here, we will get
        // a misleading impression of the wait counts
        // is_upgrade is figured w/o consideraton whether request is
        // conditional/unconditional, but we consider it
        // uncondl because the unconditional case is
        // the one we're trying to understand in the callers
        // (bf find, bf scan, btree latch
        INC_TSTAT(latch_uncondl_nowait);
#endif
    } else {
        if(timeout == timeout_t::WAIT_IMMEDIATE) {
            INC_TSTAT(needs_latch_condl);
            bool success = (new_mode == LATCH_SH)?
                _lock.attempt_read() : _lock.attempt_write();
            if(!success)
                return RC(stTIMEOUT);
            INC_TSTAT(latch_condl_nowait);
        }
        else {
            // forever timeout
            INC_TSTAT(needs_latch_uncondl);
            if(new_mode == LATCH_SH) {
// NOTE: These stats are questionable in their
// heiseneffect as well as in the fact that we might
// not wait in the _lock.acquire_{read,write} call
// after the attempt- call. Nevertheless, they might
// help us in some instances to understand where the
// contention is, and are under compiler control for
// this reason.
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
                if(_lock.attempt_read()) {
                    INC_TSTAT(latch_uncondl_nowait);
                } else
#endif
                _lock.acquire_read();
            }
            else {
                w_assert2(new_mode == LATCH_EX);
                w_assert2(me->_count == 0);
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
                if(_lock.attempt_write()) {
                    INC_TSTAT(latch_uncondl_nowait);
                } else
#endif
                _lock.acquire_write();
            }
        }
        w_assert2(me->_count == 0);
        me->_mode = new_mode;
    }
    lintel::unsafe::atomic_fetch_add(&_total_count, 1);// BUG_SEMANTICS_FIX
    me->_count++;// BUG_SEMANTICS_FIX
    DBGTHRD(<< "acquired " << *this );
    return RCOK;
}


int
latch_t::_release(latch_holder_t* me)
{
    DBGTHRD(<< "want to release " << *this );

    w_assert2(me->_latch == this);

    w_assert2(me->_mode != LATCH_NL);
    w_assert2(me->_count > 0);

    lintel::unsafe::atomic_fetch_sub(&_total_count, 1);
    if(--me->_count) {
        DBGTHRD(<< "was held multiple times -- still " << me->_count << " " << *this );
        return me->_count;
    }

    if(me->_mode == LATCH_SH) {
        w_assert2(_lock.has_reader());
        if (_lock.has_reader())
            _lock.release_read();
    }
    else {
        w_assert2(_lock.has_writer());
        if (_lock.has_writer())
            _lock.release_write();
    }
    me->_mode = LATCH_NL;
    return 0;
}

void latch_t::downgrade() {
    holder_search me(this);
    // we should already hold the latch!
    w_assert3(me.value() != NULL);
    _downgrade(me.value());
}

void
latch_t::_downgrade(latch_holder_t* me)
{
    DBGTHRD(<< "want to downgrade " << *this );

    w_assert3(me->_latch == this);
    w_assert3(me->_mode == LATCH_EX);
    w_assert3(me->_count > 0);

    _lock.downgrade();
    me->_mode = LATCH_SH;

}

void latch_holder_t::print(ostream &o) const
{
    o << "Holder " << latch_t::latch_mode_str[int(_mode)]
        << " cnt=" << _count
    << " latch:";
    if(_latch) {
        o  << *_latch << endl;
    } else {
        o  << "NULL" << endl;
    }
}

void latch_t::on_thread_destroy()
{
    // w_assert3(!latch_holder_t::thread_local_holders);
    // latch_holder_t* freelist = latch_holder_t::thread_local_freelist;
    // while(freelist) {
    //     latch_holder_t* node = freelist;
    //     freelist = node->_next;
    //     delete node;
    // }
    // latch_holder_t::thread_local_freelist = NULL;
}

// return the number of times the latch is held by this thread
// or 0 if I do not hold the latch
// There should never be more than one holder structure for a single
// latch.
int
latch_t::held_by_me() const
{
    holder_search me(this);
    return me.value()? me->_count : 0;
}

bool
latch_t::is_mine() const {
    holder_search me(this);
    return me.value()? (me->_mode == LATCH_EX) : false;
}

// NOTE: this is not safe, but it can be used by unit tests
// and for debugging
std::ostream &latch_t::print(std::ostream &out) const
{
    out <<    "latch(" << this << ") ";
    out << " held in " << latch_mode_str[int(mode())] << " mode ";
    out << "by " << num_holders() << " threads " ;
    out << "total " << latch_cnt() << " times " ;
    out << endl;
    return out;
}


ostream& operator<<(ostream& out, const latch_t& l)
{
    return l.print(out);
}

// For use in debugger:
void print_latch(const latch_t *l)
{
    if(l != NULL) l->print(cerr);
}

// For use in debugger:
void print_my_latches()
{
    holders_print all(latch_holder_t::thread_local_holders);
}

