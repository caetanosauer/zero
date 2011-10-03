/*<std-header orig-src='shore' incl-file-exclusion='VTABLE_ENUM_H'>

 $Id: sthread_vtable_enum.h,v 1.2 2010/05/26 01:21:34 nhall Exp $

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

#ifndef STHREAD_VTABLE_ENUM_H
#define STHREAD_VTABLE_ENUM_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <vtable.h>
/*
 * Enum definition used for converting per-thread info
 * to a virtual table.  The enum assigns a distinct number to each
 * "attribute" of a thread.
 * This acts as an index into a vtable_row_t;
 *
 * The attribute names are defined
 */

enum sthread_vtable_attr_index {
    /* for sthreads */
    sthread_id_attr,
    sthread_name_attr,
    sthread_status_attr,

#include "sthread_stats_collect_enum_gen.h"

    /* last number! */
    sthread_last 
};

extern const char *sthread_vtable_attr_names[];  // in vtable_sthread.cpp

/*<std-footer incl-file-exclusion='VTABLE_ENUM_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
