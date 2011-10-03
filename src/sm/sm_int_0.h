/*<std-header orig-src='shore' incl-file-exclusion='SM_INT_0_H'>

 $Id: sm_int_0.h,v 1.16 2010/05/26 01:20:43 nhall Exp $

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

#ifndef SM_INT_0_H
#define SM_INT_0_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#if defined(SM_SOURCE) && !defined(SM_LEVEL)
#    define SM_LEVEL 0
#endif

#include <w_debug.h>
#include <sysdefs.h>
#include <basics.h>
#include <sthread.h>
#include <vec_t.h>
#include <latch.h>
#include <lid_t.h>
#if defined(SM_SOURCE)
/* Do not force this on VASs */
#include <sm_s.h>
#endif /* SM_SOURCE */
#include <smthread.h>
#include <tid_t.h>
#include "smstats.h"

#if defined(SM_SOURCE) && (SM_LEVEL >= 0) 
#    include <bf.h>
#    include <page.h>
#    include <pmap.h>
#    include <sm_io.h>
#    include <log.h>

#endif


/*<std-footer incl-file-exclusion='SM_INT_0_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
