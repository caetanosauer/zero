/* -*- mode:C++; c-basic-offset:4 -*-
   Shore-kits -- Benchmark implementations for Shore-MT
   
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

/** @file resource_pool.cpp
 *
 *  @brief Implements methods described in resource_pool.h.
 *
 *  @author Naju Mancheril (ngm)
 *
 *  Since a thread is not doing anything while waiting to reserve
 *  resources, we can stack allocate the nodes we use to maintain a
 *  list of waiting threads.
 *
 *  If a thread tries to reserve resources and does not have to wait,
 *  it is responsible for updating the state of the resource_pool_s
 *  structure. If the thread waits, then it can assume that the pool's
 *  state has been properly updated when it wakes.
 *
 *  @bug None known.
 */

#include <pthread.h>              /* need to include this first */
#include "util/resource_pool.h"   /* for prototypes */
#include "util/static_list.h"     /* for static_list_t functions */

#include <stdlib.h>        /* for NULL */
#include <assert.h>        /* for assert() */
#include "util/trace.h"
#include "util/tassert.h"



/* internal constants */

#define TRACE_RESOURCE_POOL 0


/* internal structures */

/**
 * @brief Each waiting thread stack-allocates an instance of this
 * structure. It contains the linked list node structure used to add
 * this thread to the waiter queue.
 */
struct waiter_node_s
{
    int req_reserve_count;
    pthread_cond_t request_granted;
    struct static_list_node_s list_node;
};



/* method definitions */

/**
 *  @brief Reserve 'n' copies of the resource.
 *
 *  @param rp The resource pool.
 *
 *  @param n Wait for this many resources to appear unreserved. If n
 *  is larger than the pool capacity, the behavior is undefined.
 *
 *  THE CALLER MUST BE HOLDING THE INTERNAL MUTEX ('mutexp' used to
 *  initialize 'rp') WHEN CALLING THIS FUNCTION. THE CALLER WILL HOLD
 *  THIS MUTEX WHEN THE FUNCTION RETURNS.
 *
 *  @return void
 */

void resource_pool_t::reserve(int n)
{
  
    /* Error checking. If 'n' is larger than the pool capacity, we will
       never be able to satisfy the request. */
    TASSERT(n <= _capacity);
  
    TRACE(TRACE_RESOURCE_POOL & TRACE_ALWAYS, "%s was %d:%d:%d\n",
          _name.data(),
          _capacity,
          _reserved,
          _non_idle);
  
    /* Checks:
     
       - If there are other threads waiting, add ourselves to the queue
       of waiters so we can maintain FIFO ordering.

       - If there are no waiting threads, but the number of unreserved
       threads is too small, add ourselves to the queue of waiters. */
    int num_unreserved = _capacity - _reserved;
    if (!static_list_is_empty(&_waiters) || (num_unreserved < n)) {

        wait_for_turn(n);
    
        /* If we are here, we have been granted the resources. The thread
           which gave them to us has already updated the pool's state. */
        TRACE(TRACE_RESOURCE_POOL & TRACE_ALWAYS, "%s after_woken %d:%d:%d\n",
              _name.data(),
              _capacity,
              _reserved,
              _non_idle);

        return;
    }


    /* If we are here, we did not wait. We are responsible for updating
       the state of the rpaphore before we exit. */
    _reserved += n;
  
    TRACE(TRACE_RESOURCE_POOL & TRACE_ALWAYS, "%s didnt_sleep %d:%d:%d\n",
          _name.data(),
          _capacity,
          _reserved,
          _non_idle);
}



/** 
 *  @brief Unreserve the specified number of resources.
 *
 *  @param rp The resource pool.
 *
 *  THE CALLER MUST BE HOLDING THE INTERNAL MUTEX ('mutexp' used to
 *  initialize 'rp') WHEN CALLING THIS FUNCTION. THE CALLER WILL HOLD
 *  THIS MUTEX WHEN THE FUNCTION RETURNS.
 *
 *  @return void
 */

void resource_pool_t::unreserve(int n)
{
    /* error checking */
    TASSERT(_reserved >= n);
    TRACE(TRACE_RESOURCE_POOL & TRACE_ALWAYS, "%s was %d:%d:%d\n",
          _name.data(),
          _capacity,
          _reserved,
          _non_idle);

    /* update the 'reserved' count */
    _reserved -= n;

    TRACE(TRACE_RESOURCE_POOL & TRACE_ALWAYS, "%s now %d:%d:%d\n",
          _name.data(),
          _capacity,
          _reserved,
          _non_idle);

    waiter_wake();
  
    TRACE(TRACE_RESOURCE_POOL & TRACE_ALWAYS, "%s after_waking %d:%d:%d\n",
          _name.data(),
          _capacity,
          _reserved,
          _non_idle);
}



/** 
 *  @brief Increase the capacity, releasing any threads whose requests
 *  can now be satisfied.
 *
 *  @param rp The resource pool.
 *
 *  @param diff The increase in capacity.
 *
 *  THE CALLER MUST BE HOLDING THE INTERNAL MUTEX ('mutexp' used to
 *  initialize 'rp') WHEN CALLING THIS FUNCTION. THE CALLER WILL HOLD
 *  THIS MUTEX WHEN THE FUNCTION RETURNS.
 *
 *  @return void
 */

void resource_pool_t::notify_capacity_increase(int diff)
{
    TASSERT(diff > 0);
    TRACE(TRACE_RESOURCE_POOL & TRACE_ALWAYS, "%s was %d:%d:%d\n",
          _name.data(),
          _capacity,
          _reserved,
          _non_idle);

    _capacity += diff;
    waiter_wake();

    TRACE(TRACE_RESOURCE_POOL & TRACE_ALWAYS, "%s now %d:%d:%d\n",
          _name.data(),
          _capacity,
          _reserved,
          _non_idle);
}



/** 
 *  @brief Increase the capacity, releasing any threads whose requests
 *  can now be satisfied.
 *
 *  @param rp The resource pool.
 *
 *  @param diff The increase in capacity.
 *
 *  THE CALLER MUST BE HOLDING THE INTERNAL MUTEX ('mutexp' used to
 *  initialize 'rp') WHEN CALLING THIS FUNCTION. THE CALLER WILL HOLD
 *  THIS MUTEX WHEN THE FUNCTION RETURNS.
 *
 *  @return void
 */

void resource_pool_t::notify_idle()
{
    TASSERT(_non_idle > 0);
    _non_idle--;
    TRACE(TRACE_RESOURCE_POOL & TRACE_ALWAYS, "%s IDLE %d:%d:%d\n",
          _name.data(),
          _capacity,
          _reserved,
          _non_idle);
}


void resource_pool_t::notify_non_idle()
{
    TASSERT(_non_idle < _reserved);
    _non_idle++;
    TRACE(TRACE_RESOURCE_POOL & TRACE_ALWAYS, "%s NON_IDLE %d:%d:%d\n",
          _name.data(),
          _capacity,
          _reserved,
          _non_idle);
}


int resource_pool_t::get_capacity()
{
    return _capacity;
}


int resource_pool_t::get_reserved()
{
    return _reserved;
}


/**
 * @brief The set of idle resources can be divided into idle and
 * non-idle resources. This method returns the number of idle
 * resources.
 */

int resource_pool_t::get_non_idle()
{
    return _non_idle;
}




/* definitions of internal helper functions */

/**
 * @brief Wait fot the requested number of resources to appear.
 *
 * @param rp The rpaphore.
 *
 * @param req_reserve_count The number of resources the caller wishes
 * to acquire.
 *
 * @pre Calling thread holds resource pool mutex on entry.
 */

void resource_pool_t::wait_for_turn(int req_reserve_count)
{
 
    struct waiter_node_s node;
    node.req_reserve_count = req_reserve_count;
    pthread_cond_init(&node.request_granted, NULL);
    static_list_append(&_waiters, &node, &node.list_node);
  
    pthread_cond_wait(&node.request_granted, _mutexp);
  
    /* If we are here, we have been granted access! The state of the
       lock has been updated. We just need to return to the caller. */
}



/**
 * @brief Wake as many threads as we can using the new 'reserved'
 * count.
 *
 * @pre Calling thread holds resource pool mutex on entry.
 */

void resource_pool_t::waiter_wake()
{
    while ( !static_list_is_empty(&_waiters) ) {
        int num_unreserved = _capacity - _reserved;
    
        void* waiter_node;
        int get_ret =
            static_list_get_head(&_waiters, &waiter_node);
        TASSERT(get_ret == 0);
        struct waiter_node_s* wn = (struct waiter_node_s*)waiter_node;
    
        if (num_unreserved < wn->req_reserve_count)
            /* Not enough resources to let this thread through... */
            return;
    
        /* Hit another thread that can be allowed to pass. */
        /* Remove thread from queue. Wake it. Update rpaphore count. */
        int remove_ret =
            static_list_remove_head(&_waiters, &waiter_node, NULL);
        TASSERT(remove_ret == 0);
        wn = (struct waiter_node_s*)waiter_node;
 
        _reserved += wn->req_reserve_count;
        pthread_cond_signal(&wn->request_granted);
    }
}
