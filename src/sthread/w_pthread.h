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
/*<std-header orig-src='shore' incl-file-exclusion='STHREAD_H'>

 $Id: w_pthread.h,v 1.1 2010/12/09 15:20:17 nhall Exp $

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

#ifndef W_PTHREAD_H
#define W_PTHREAD_H

#include <pthread.h>
#include <w_strstream.h>
#include <string.h>

#define DO_PTHREAD_BARRIER(x) \
{   int res = x; \
    if(res && res != PTHREAD_BARRIER_SERIAL_THREAD) { \
       w_ostrstream S; \
       S << "Unexpected result from " << #x << " " << res << " "; \
       char buf[100]; \
       (void) strerror_r(res, &buf[0], sizeof(buf)); \
       S << buf << ends; \
       W_FATAL_MSG(fcINTERNAL, << S.c_str()); \
    }  \
}
#define DO_PTHREAD(x) \
{   int res = x; \
    if(res) { \
       w_ostrstream S; \
       S << "Unexpected result from " << #x << " " << res << " "; \
       char buf[100]; \
       (void) strerror_r(res, &buf[0], sizeof(buf)); \
       S << buf << ends; \
       W_FATAL_MSG(fcINTERNAL, << S.c_str()); \
    }  \
}
#define DO_PTHREAD_TIMED(x) \
{   int res = x; \
    if(res && res != ETIMEDOUT) { \
        W_FATAL_MSG(fcINTERNAL, \
                <<"Unexpected result from " << #x << " " << res); \
    } \
}

#endif          /*</std-footer>*/
