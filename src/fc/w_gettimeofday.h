/*<std-header orig-src='shore' incl-file-exclusion='W_GETTIMEOFDAY_H'>

 $Id: w_gettimeofday.h,v 1.12 2010/05/26 01:20:25 nhall Exp $

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

#ifndef W_GETTIMEOFDAY_H
#define W_GETTIMEOFDAY_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <sys/time.h>

// Solaris 10 doesn't have timersub
#ifndef timersub
#define timersub(a, b, res)                           \
    do {                                              \
        (res)->tv_sec = (a)->tv_sec - (b)->tv_sec;    \
        (res)->tv_usec = (a)->tv_usec - (b)->tv_usec; \
        if ((res)->tv_usec < 0) {                     \
            (res)->tv_sec--;                          \
            (res)->tv_usec += 1000000;                \
        }                                             \
    } while (0)
#endif //timersub

/*<std-footer incl-file-exclusion='W_GETTIMEOFDAY_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
