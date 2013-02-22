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

/** @file resource_pool.h
 *
 *  @brief A resource pool is used to track the acquisition and use of
 *  a certain resource in the system. The pool's capacity represents
 *  the total number of instances of this resource in the system. The
 *  purpose of this pool is to provide a way for threads to reserve
 *  resources. They will wait until the requested number of resources
 *  becomes available and then proceed. In addition, we provide a way
 *  for threads to record when they actually use these resources. The
 *  caller may use the number of unused (idle) resources to identify
 *  when to create more (and increase the pool capacity).
 *
 *  This implementation uses Pthreads synchronization functions.
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @bug None known.
 */

#ifndef __UTIL_RESOURCE_POOL_H
#define __UTIL_RESOURCE_POOL_H

#include "util/thread.h"
#include "util/c_str.h"
#include "util/static_list.h"
#include "util/static_list_struct.h" /* for static_list_s structure */



/******************************************************************
 * 
 * @class: resource_pool_t
 *
 ******************************************************************/

class resource_pool_t 
{
private:

    pthread_mutex_t* _mutexp;

    int _capacity;
    int _reserved;
    int _non_idle;
  
    c_str _name;

    struct static_list_s _waiters;
  
public:

    resource_pool_t(pthread_mutex_t* mutexp, int capacity, const c_str& name)
        : _mutexp(mutexp)
        , _capacity(capacity)
        , _reserved(0)
        , _non_idle(0)
        , _name(name)
    {
        static_list_init(&_waiters);
    }

    void reserve(int n);
    void unreserve(int n);
    void notify_capacity_increase(int diff);
    void notify_idle();
    void notify_non_idle();
    int  get_capacity();
    int  get_reserved();
    int  get_non_idle();

private:
 
    void wait_for_turn(int req_reserve_count);
    void waiter_wake();
};

#endif
