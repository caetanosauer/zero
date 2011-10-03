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

 $Id: sthread_core_pthread.cpp,v 1.10 2010/12/09 15:20:17 nhall Exp $

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


/*
 *   NewThreads is Copyright 1992, 1993, 1994, 1995, 1996, 1997, 1998 by:
 *
 *    Josef Burger    <bolo@cs.wisc.edu>
 *    Dylan McNamee    <dylan@cse.ogi.edu>
 *      Ed Felten       <felten@cs.princeton.edu>
 *
 *   All Rights Reserved.
 *
 *   NewThreads may be freely used as long as credit is given
 *   to the above authors and the above copyright is maintained.
 */

#include <w.h>
#include "sthread.h"
#include <w_stream.h>
#include <w_pthread.h>
#include "stcore_pthread.h"

#ifndef HAVE_SEMAPHORE_H

static    int    sem_init(sthread_core_t::sem_t *sem, int, int count)
{
    /* XXX could bitch if shared was true, but it is just for
       local compatability */

    sem->count = count;
    DO_PTHREAD(pthread_mutex_init(&sem->lock, NULL));
    DO_PTHREAD(pthread_cond_init(&sem->wake, NULL));

    return 0;
}

static    void    sem_destroy(sthread_core_t::sem_t *sem)
{
    DO_PTHREAD(pthread_mutex_destroy(&sem->lock));
    DO_PTHREAD(pthread_cond_destroy(&sem->wake));
}

static    inline    void    sem_post(sthread_core_t::sem_t *sem)
{
    DO_PTHREAD(pthread_mutex_lock(&sem->lock));
    sem->count++;
    if (sem->count > 0)
        DO_PTHREAD(pthread_cond_signal(&sem->wake));
    DO_PTHREAD(pthread_mutex_unlock(&sem->lock));
}

static    inline    void    sem_wait(sthread_core_t::sem_t *sem)
{
    DO_PTHREAD(pthread_mutex_lock(&sem->lock));
    while (sem->count <= 0)
        DO_PTHREAD(pthread_cond_wait(&sem->wake, &sem->lock));
    sem->count--;
    DO_PTHREAD(pthread_mutex_unlock(&sem->lock));
}
#endif


// starting function called by every pthread created; core* is the
// argument. Through the core* we get the "real function and arg.
extern "C" void *pthread_core_start(void *_arg);
void *pthread_core_start(void *_arg)
{
    sthread_core_t    *me = (sthread_core_t *) _arg;

    // core is_virgin says the "real" function hasn't started yet
    // Unfortunately, we have multiple phases of startup here
    me->is_virgin = 0;
    (me->start_proc)(me->start_arg);
    return 0;
}


int sthread_core_init(sthread_core_t *core,
              void (*proc)(void *), void *arg,
              unsigned stack_size)
{
    int    n;

    /* Get a life; XXX magic number */
    if (stack_size > 0 && stack_size < 1024)
        return -1;

    core->is_virgin = 1;
    core->start_proc = proc;
    core->start_arg = arg;
    core->stack_size = stack_size;

    if (stack_size > 0) {
        /* A real thread :thread id, default attributes, start func, arg */
        n = pthread_create(&core->pthread, NULL, pthread_core_start, core);
        if (n == -1) {
            w_rc_t e= RC(fcOS);
            // EAGAIN: insufficient resources
            // Really, there's no way to tell when the system will
            // say it's hit the maximum # threads because that depends
            // on a variety of resources, and in any case, we don't
            // know how much memory will be required for another thread.
            cerr << "pthread_create():" << endl << e << endl;
            return -1;
        }
        core->creator = pthread_self();
    }
    else {
        /* This is the main thread.  It runs in the "system"
           pthread; no pthread_create is needed.
         */

        /* A more elegant solution would be to make a
           "fake" stack using the kernel stack origin
           and stacksize limit.   This could also allow
           the main() stack to have a thread-package size limit,
           to catch memory hogs in the main thread. */

        /* The system stack is never virgin */
        core->is_virgin = 0;
        core->pthread = pthread_self();
        core->creator = core->pthread; // main thread
    }
    return 0;
}

/* clean up : called on destruction.
 * All we do now is join the thread
 */
void sthread_core_exit(sthread_core_t* core, bool &joined)
{
    void    *join_value=NULL;
    if(joined) {
        return;
    }

    /* must wait for the thread and then harvest its thread */

    if (core->stack_size > 0) {
        int res = pthread_join(core->pthread, &join_value);
        if(res) {
            const char *msg="";
            switch(res) {
                case EINVAL:
                    msg = "Not a joinable thread: EINVAL";
                    break;
                case ESRCH:
                    msg = "No such thread: ESRCH";
                    break;
                case EDEADLK:
                    msg = "Joining with self: EDEADLK";
                    break;
                default:
                    break;
            }
            if(res) {
               w_ostrstream o;
               o << "sthread_core_exit:"
                   << " Unexpected result from pthread_join: "
                   << msg << " core is : ";

               o << *core << endl;

               W_FATAL_MSG(fcINTERNAL,  << o.c_str() << endl);
            }
        }
        /* And the thread is gone */
    }
    joined = true;
}

ostream &operator<<(ostream &o, const sthread_core_t &core)
{
    o << "core: ";
    if (core.stack_size == 0)
        W_FORM(o)("[ system thread %#lx creator %#lx ]", 
                (long) core.pthread, 
                (long) core.creator
                );
    else
        W_FORM(o)("[ thread %#lx creator %#lx ] size=%d",  
            (long) core.pthread, 
            (long) core.creator, 
            core.stack_size);
    if (core.is_virgin)
        o << ", virgin-core";
    return o;
}
