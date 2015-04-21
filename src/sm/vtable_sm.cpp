/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

/*<std-header orig-src='shore'>

 $Id: vtable_sm.cpp,v 1.3 2010/06/08 22:28:57 nhall Exp $

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

#define SM_SOURCE
#define VTABLE_SM_C

#ifdef __GNUG__
class prologue_rc_t;
#endif

#include "w.h"
#include "sm_int_3.h"
#include "chkpt.h"
#include "sm.h"
#include "sm_vtable_enum.h"
#include "prologue.h"
#include "vol.h"
#include "crash.h"
#include "restart.h"


/*--------------------------------------------------------------*
 *  ss_m::lock_collect()                            *
 *  ss_m::thread_collect()                            *
 *  ss_m::xct_collect()                                    *
 *  ss_m::stats_collect()                                *
 *  wrappers for hidden things                                  *
 *--------------------------------------------------------------*/
/**\brief Collect a virtual table describing transactions.
 *
 *\details
 * \todo thread_collect
 */
rc_t
ss_m::xct_collect( vtable_t & res, bool names_too) 
{
    if(xct_t::collect(res, names_too) == 0) return RCOK;
    return RC(eOUTOFMEMORY);
}
rc_t
ss_m::lock_collect( vtable_t& res, bool names_too) 
{
    if(lm->collect(res, names_too)==0) return RCOK;
    return RC(eOUTOFMEMORY);
}

/**\brief Collect a virtual table of thread information.
 *
 * \details
 * \todo thread_collect
 *
 * See also:
 * - sthread_vtable_attr_index (index into attributes of a
 * vtable_row_t for an sthread.
 */
rc_t
ss_m::thread_collect( vtable_t & res, bool names_too) 
{
    // this calls sthread_t::collect,
    // which goes through all threads, calling
    // each thread's virtual collect function

    if(smthread_t::collect(res, names_too)==0) return RCOK;
    return RC(eOUTOFMEMORY);
}
