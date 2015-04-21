/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

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

 $Id: smfile.cpp,v 1.69 2010/12/17 19:36:26 nhall Exp $

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
#define SMFILE_C

#include "w.h"
#include "sm_int_0.h"
#include "btcursor.h"
#include "sm.h"
#include "suppress_unused.h"
#include "vol.h"

#if W_DEBUG_LEVEL > 2
#define  FILE_LOG_COMMENT_ON 1
#else
#define  FILE_LOG_COMMENT_ON 0
#endif

/*--------------------------------------------------------------*
 *  ss_m::set_store_property()                                  *
 *--------------------------------------------------------------*/
rc_t
ss_m::set_store_property(stid_t stid, store_property_t property)
{
    SM_PROLOGUE_RC(ss_m::set_store_property, in_xct, read_write, 0);
    W_DO( _set_store_property( stid, property) );
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::get_store_property()                                  *
 *--------------------------------------------------------------*/
rc_t
ss_m::get_store_property(stid_t stid, store_property_t& property)
{
    SM_PROLOGUE_RC(ss_m::get_store_property, in_xct,read_only, 0);
    W_DO( _get_store_property( stid, property) );
    return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::_set_store_property()                                 *
 *--------------------------------------------------------------*/
rc_t
ss_m::_set_store_property(
    stid_t              stid,
    store_property_t        property)
{

    /*
     * Can't change to a load file. (You can create it as
     * a load file.)
     */
    if (property & st_load_file)  {
        return RC(eBADSTOREFLAGS);
    }

    /*
     * can't change to a t_temporary file. You can change
     * to an insert file, which combines with st_tmp (in that
     * page fix for a page of st_insert_file get the st_tmp
     * OR-ed in to the page's store flags).
     */
    store_flag_t newflags = _make_store_flag(property);
    if (newflags == st_tmp)  {
        return RC(eBADSTOREFLAGS);
    }

    /*
     * find out the current property
     */
    store_flag_t oldflags = st_unallocated;

    W_DO(vol->get(stid.vol)->get_store_flags(stid.store, oldflags) );

    if (oldflags == newflags)  {
        return RCOK;
    }


    W_DO(vol->get(stid.vol)->set_store_flags(stid.store, newflags) );

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_get_store_property()                                 *
 *--------------------------------------------------------------*/
rc_t
ss_m::_get_store_property(
    stid_t              stid,
    store_property_t&   property)
{
    store_flag_t flags = st_unallocated;
    W_DO(vol->get(stid.vol)->get_store_flags(stid.store, flags) );

    if (flags & st_regular) {
        w_assert2((flags & (st_tmp|st_load_file|st_insert_file)) == 0);
        property = t_regular;
        return RCOK;
    }
    if (flags & st_load_file) {
        // io_m::create_store stuffs the st_tmp flags in with
        // the st_load_file flags when the store is created.
        // It gets converted on commit to st_regular.
        w_assert2((flags & (st_insert_file|st_regular)) == 0);
        w_assert2((flags & st_tmp) == st_tmp);
        property = t_load_file;
        return RCOK;
    }

    if (flags & st_insert_file) {
        // Files can't be created as st_insert_file, but they
        // can get changed to this.
        // They get converted on commit to st_regular.
        // The page fix causes the st_tmp flag to be OR-ed in
        // for the page's store flags.
        // Why these are handled differently, I don't know.
        w_assert2((flags & (st_load_file|st_regular)) == 0);
        w_assert2((flags & st_tmp) == 0);
        property = t_insert_file;
        return RCOK;
    }

    if (flags & st_tmp)  {
        property = t_temporary;
    } else {
        W_FATAL(eINTERNAL);
    }

    return RCOK;
}
