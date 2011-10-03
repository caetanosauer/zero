#ifndef MCS_LOCK_H
#define MCS_LOCK_H
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

// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='MCS_LOCK_H'>

 $Id: mcs_lock.h,v 1.7 2010/11/08 15:07:23 nhall Exp $

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

/*  -- do not edit anything above this line --   </std-header>*/

/**\cond skip */

/**\brief An MCS queuing spinlock. 
 *
 * Useful for short, contended critical sections. 
 * If contention is expected to be rare, use a
 * tatas_lock; 
 * if critical sections are long, use pthread_mutex_t so 
 * the thread can block instead of spinning.
 *
 * Tradeoffs are:
   - test-and-test-and-set locks: low-overhead but not scalable
   - queue-based locks: higher overhead but scalable
   - pthread mutexes : high overhead and blocks, but frees up cpu for other threads
*/
struct mcs_lock {
    struct qnode;
    typedef qnode volatile* qnode_ptr;
    struct qnode {
        qnode_ptr _next;
        bool _waiting;
        //      qnode() : _next(NULL), _waiting(false) { } // non-POD, alas
    };
    struct ext_qnode {
        qnode _node;
        mcs_lock* _held;
        operator qnode*() { return &_node; }
    };
#define MCS_EXT_QNODE_INITIALIZER {{NULL,false},NULL}
#define MCS_EXT_QNODE_INITIALIZE(x) \
{ (x)._node._next = NULL; (x)._node._waiting = false; (x)._held = NULL; }
    qnode_ptr volatile _tail;
    mcs_lock() : _tail(NULL) { }

    /* This spinning occurs whenever there are critical sections ahead
       of us.

       CC mangles this as __1cImcs_lockPspin_on_waiting6Mpon0AFqnode__v_
    */
    void spin_on_waiting(qnode_ptr me) {
        while(me->_waiting);
    }
    /* Only acquire the lock if it is free...
     */
    bool attempt(ext_qnode* me) {
        if(attempt((qnode_ptr) me)) {
            me->_held = this;
            return true;
        }
        return false;
    }
    bool attempt(qnode_ptr me) {
        me->_next = NULL;
        me->_waiting = true;
        membar_producer();
        qnode_ptr pred = (qnode_ptr) atomic_cas_ptr(&_tail, 0, (void*) me);
        // lock held?
        if(pred)
            return false;
        membar_enter();
        return true;
    }
    // return true if the lock was free
    void* acquire(ext_qnode* me) {
        me->_held = this;
        return acquire((qnode*) me);
    }
    void* acquire(qnode_ptr me) {
        me->_next = NULL;
        me->_waiting = true;
        membar_producer();
        qnode_ptr pred = (qnode_ptr) atomic_swap_ptr(&_tail, (void*) me);
        if(pred) {
            pred->_next = me;
            spin_on_waiting(me);
        }
        membar_enter();
        return (void*) pred;
    }

    /* This spinning only occurs when we are at _tail and catch a
       thread trying to enqueue itself.

       CC mangles this as __1cImcs_lockMspin_on_next6Mpon0AFqnode__3_
    */
    qnode_ptr spin_on_next(qnode_ptr me) {
        qnode_ptr next;
        while(!(next=me->_next));
        return next;
    }
    void release(ext_qnode *me) { 
        w_assert1(is_mine(me));
        me->_held = 0; release((qnode*) me); 
    }
    void release(ext_qnode &me) { w_assert1(is_mine(&me)); release(&me); }
    void release(qnode &me) { release(&me); }
    void release(qnode_ptr me) {
        membar_exit();

        qnode_ptr next;
        if(!(next=me->_next)) {
            if(me == _tail && me == (qnode_ptr) 
                    atomic_cas_ptr(&_tail, (void*) me, NULL))
            return;
            next = spin_on_next(me);
        }
        next->_waiting = false;
    }
    // bool is_mine(qnode_ptr me) { return me->_held == this; }
    bool is_mine(ext_qnode* me) { return me->_held == this; }
};
/**\endcond skip */
#endif

