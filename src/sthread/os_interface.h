/*<std-header orig-src='shore' incl-file-exclusion='OS_INTERFACE_H'>

 $Id: os_interface.h,v 1.16 2010/05/26 01:21:27 nhall Exp $

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

#ifndef OS_INTERFACE_H
#define OS_INTERFACE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/


/*
 *   NewThreads I/O is Copyright 1995, 1996, 1997, 1998 by:
 *
 *    Josef Burger    <bolo@cs.wisc.edu>
 *
 *   All Rights Reserved.
 *
 *   NewThreads I/O may be freely used as long as credit is given
 *   to the above author(s) and the above copyright is maintained.
 */

/*
 * Yes.  This is a giant mess.  It is a set of macros and
 * typedefs to provide compatability with serveral operating
 * systems in "Posix" mode.  It is used ONLY in places that
 * the details of those operating systems need to be available,
 * such as in the guts of the thread package.
 */

/* This preliminary section provides exceptions to the default
   values that are needed in each environment. */



/* The following are all the default values for the os_ stuff */
#ifndef os_open
#define    os_open(p,f,m)    open(p,f,m)
#endif

#ifndef os_close
#define    os_close(d)    close(d)
#endif

#define    os_read(d,b,c)    read(d,b,c)
#define    os_write(d,b,c)    write(d,b,c)
#define    os_readv(d,v,c)    readv(d,v,c)
#define    os_writev(d,v,c) writev(d,v,c)
#define    os_pread(d,b,c,p)    pread(d,b,c,p)
#define    os_pwrite(d,b,c,p)    pwrite(d,b,c,p)

#ifndef os_lseek
#define    os_lseek(d,o,w)    lseek(d,o,w)
#endif

#ifndef os_fstat
#define    os_fstat(d,s)    fstat(d,s)
#endif

#ifndef os_stat
#define    os_stat(f,s)    stat(f,s)
#endif

#ifndef os_ftruncate
#define    os_ftruncate(d,s)    ftruncate(d,s)
#endif

#ifndef os_truncate
#define    os_truncate(f,s)    truncate(f,s)
#endif
#ifndef os_fsync
#define    os_fsync(d)    fsync(d)
#endif
#ifndef os_lockf
#define    os_lockf(d,f,s)    lockf(d,f,s)
#endif

#ifndef HAVE_OS_STAT_T
typedef struct stat    os_stat_t;
#else
#undef    HAVE_OS_STAT_T
#endif


#define    os_opendir(f)    opendir(f)
#define    os_closedir(d)    closedir(d)

// Use readdir(3).  We should implement an sthread method
// readdir(), which uses readdir_r.  However, for the time being,
// since it is used by the sm only to open the log (1 thread),
// this is not high priority (yet).  No vas should be doing any readdir at
// the same time it's constructing a storage manger. 
// TODO: NANCy document this (above)
#ifndef os_readdir
#define    os_readdir(d)    readdir(d)
#endif

#ifndef HAVE_OS_DIRENT_T
typedef    struct dirent    os_dirent_t;
#else
#undef    HAVE_OS_DIRENT_T
#endif

#if HAVE_DIRENT_H
    /* XXX something wacked with gcc 3.2 and solaris headers */
#include <dirent.h>        /* XXX unconventional dirent */
#else
struct DIR;
#endif

typedef    DIR    *os_dir_t;


#define    os_getrlimit(l,r)    getrlimit(l,r)
#define    os_setrlimit(l,r)    setrlimit(l,r)

typedef struct rlimit os_rlimit_t;

/*<std-footer incl-file-exclusion='OS_INTERFACE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
