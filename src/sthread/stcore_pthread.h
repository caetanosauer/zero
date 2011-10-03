/*<std-header orig-src='shore' incl-file-exclusion='STCORE_PTHREAD_H'>

 $Id: stcore_pthread.h,v 1.10 2010/12/09 15:20:17 nhall Exp $

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

#ifndef STCORE_PTHREAD_H
#define STCORE_PTHREAD_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
 *   NewThreads is Copyright 1992, 1993, 1994, 1995, 1996, 1997, 1998 by:
 *
 *        Josef Burger        <bolo@cs.wisc.edu>
 *      Dylan McNamee   <dylan@cse.ogi.edu>
 *      Ed Felten       <felten@cs.princeton.edu>
 *
 *   All Rights Reserved.
 *
 *   NewThreads may be freely used as long as credit is given
 *   to the above authors and the above copyright is maintained.
 */

#include <w_pthread.h>
#ifdef HAVE_SEMAPHORE_H
#include <semaphore.h>
#endif

/**\cond skip */
typedef struct sthread_core_t {
    int          is_virgin;        /* TRUE if just started */
    void        (*start_proc)(void *);  /* thread start function */
    void        *start_arg;        /* argument for start_proc */
    int         stack_size;        /* stack size */
    void        *sthread;        /* sthread which uses this core */
    pthread_t   pthread;
    pthread_t   creator;         /* thread that created this pthread, for
                                    debugging only */
} sthread_core_t;
/**\endcond skip */

ostream &operator<<(ostream &o, const sthread_core_t &core);

extern int  sthread_core_init(sthread_core_t* t,
                              void (*proc)(void *), void *arg,
                              unsigned stack_size);

extern void sthread_core_exit(sthread_core_t *t, bool &joined);

/*<std-footer incl-file-exclusion='STCORE_PTHREAD_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
