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
#include <AtomicCounter.hpp>
#include "w_defines.h"

/**
 * Used to access qnode's _waiting and _delegated together
 * \b regardless \b of \b endianness.
 */
union qnode_status {
    struct {
        int32_t _waiting;
        int32_t _delegated;
    } individual;
    int64_t _combined;
};
const qnode_status QNODE_IDLE = {{0, 0}};
const qnode_status QNODE_WAITING = {{1, 0}};
const qnode_status QNODE_DELEGATED = {{1, 1}};

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
    struct qnode {
        qnode*  _next;
        qnode_status _status;
        // int32_t _waiting;
        // int32_t _delegated;
        qnode volatile* vthis() { return this; }
    };
    struct ext_qnode {
        qnode _node;
        mcs_lock* _held;
        operator qnode*() { return &_node; }
    };
#define MCS_EXT_QNODE_INITIALIZER {{NULL,false},NULL}
#define MCS_EXT_QNODE_INITIALIZE(x) \
{ (x)._node._next = NULL; (x)._node._waiting = 0; (x)._node._delegated = 0; (x)._held = NULL; }
    qnode* _tail;
    mcs_lock() : _tail(NULL) { }

    /* This spinning occurs whenever there are critical sections ahead
       of us.
    */
    void spin_on_waiting(qnode* me) {
        while(me->vthis()->_status.individual._waiting);
    }
    /* Only acquire the lock if it is free...
     */
    bool attempt(ext_qnode* me) {
        if(attempt((qnode*) me)) {
            me->_held = this;
            return true;
        }
        return false;
    }
    bool attempt(qnode* me) {
        me->_next = NULL;
        me->_status.individual._waiting = 1;
        // lock held?
        qnode* null_cas_tmp = NULL;
        if(!lintel::unsafe::atomic_compare_exchange_strong<qnode*>(
            &_tail, &null_cas_tmp, (qnode*) me))
            return false;
        lintel::atomic_thread_fence(lintel::memory_order_acquire);
        return true;
    }
    // return true if the lock was free
    void* acquire(ext_qnode* me) {
        me->_held = this;
        return acquire((qnode*) me);
    }
    void* acquire(qnode* me) {
        return __unsafe_end_acquire(me, __unsafe_begin_acquire(me));
    }

    qnode* __unsafe_begin_acquire(qnode* me) {
        me->_next = NULL;
        me->_status.individual._waiting = 1;
        qnode* pred = lintel::unsafe::atomic_exchange<qnode*>(&_tail, me);
        if(pred) {
            pred->_next = me;
        }
        return pred;
    }
    void* __unsafe_end_acquire(qnode* me, qnode* pred) {
        if(pred) {
            spin_on_waiting(me);
        }
        lintel::atomic_thread_fence(lintel::memory_order_acquire);
        return (void*) pred;
    }

    /* This spinning only occurs when we are at _tail and catch a
       thread trying to enqueue itself.

       CC mangles this as __1cImcs_lockMspin_on_next6Mpon0AFqnode__3_
    */
    qnode* spin_on_next(qnode* me) {
        qnode* next;
        while(!(next=me->vthis()->_next));
        return next;
    }
    void release(ext_qnode *me) { 
        w_assert1(is_mine(me));
        me->_held = 0; release((qnode*) me); 
    }
    void release(ext_qnode &me) { w_assert1(is_mine(&me)); release(&me); }
    void release(qnode &me) { release(&me); }
    void release(qnode* me) {
        lintel::atomic_thread_fence(lintel::memory_order_release);

        qnode* next;
        if(!(next=me->_next)) {
            qnode* me_cas_tmp = me;
            if(me == _tail &&
                lintel::unsafe::atomic_compare_exchange_strong<qnode*>(&_tail, &me_cas_tmp, (qnode*) NULL)) {
                return;
            }
            next = spin_on_next(me);
        }
        next->_status.individual._waiting = 0;
    }
    // bool is_mine(qnode* me) { return me->_held == this; }
    bool is_mine(ext_qnode* me) { return me->_held == this; }
};

/** Used to keep mcs_lock in its own cacheline. */
const size_t CACHELINE_MCS_PADDING = CACHELINE_SIZE - sizeof(mcs_lock);
#endif

