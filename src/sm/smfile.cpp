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
#include "option.h"
#include "sm_int_4.h"
#include "btcursor.h"
#include "device.h"
#include "sm.h"


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
 *  ss_m::create_file()                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::create_file(
    vid_t                          vid, 
    stid_t&                        fid, 
    store_property_t               property,
    shpid_t                        cluster_hint // = 0
)
{
    SM_PROLOGUE_RC(ss_m::create_file, in_xct, read_write, 0);
#if FILE_LOG_COMMENT_ON
    W_DO(log_comment("create_file"));
#endif
    DBGTHRD(<<"create_file " <<vid << " " << property );
    W_DO(_create_file(vid, fid, property, cluster_hint));
    DBGTHRD(<<"create_file returns " << fid);
    return RCOK;
}




rc_t
ss_m::lvid_to_vid(const lvid_t& lvid, vid_t& vid)
{
    SM_PROLOGUE_RC(ss_m::lvid_to_vid, can_be_in_xct,read_only, 0);
    vid = io->get_vid(lvid);
    if (vid == vid_t::null) return RC(eBADVOL);
    return RCOK;
}

rc_t
ss_m::vid_to_lvid(vid_t vid, lvid_t& lvid)
{
    SM_PROLOGUE_RC(ss_m::lvid_to_vid, can_be_in_xct,read_only, 0);
    lvid = io->get_lvid(vid);
    if (lvid == lvid_t::null) return RC(eBADVOL);
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
    store_flag_t oldflags = st_bad;

    W_DO( io->get_store_flags(stid, oldflags) );

    if (oldflags == newflags)  {
        return RCOK;
    }


    W_DO( io->set_store_flags(stid, newflags) );

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
    store_flag_t flags = st_bad;
    W_DO( io->get_store_flags(stid, flags) );

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

/*--------------------------------------------------------------*
 *  ss_m::_create_file()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::_create_file(vid_t vid, stid_t& fid,
                   store_property_t property,
                   shpid_t        cluster_hint // = 0
                   )
{
    //TODO: SHORE-KITS-API
    assert(0);
}

/*--------------------------------------------------------------*
 *  ss_m::_destroy_file()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::_destroy_file(const stid_t& fid)
{
    //TODO: SHORE-KITS-API
    assert(0);
}


/*--------------------------------------------------------------*
 *  ss_m::create_mrbt_file()                                    *
 *--------------------------------------------------------------*/
rc_t
ss_m::create_mrbt_file(
    vid_t                          vid, 
    stid_t&                        fid, 
    store_property_t               property,
    shpid_t                        cluster_hint // = 0
)
{
    //TODO: SHORE-KITS-API
    assert(0);
}

/*--------------------------------------------------------------*
 *  ss_m::destroy_mrbt_file()                                   *
 *--------------------------------------------------------------*/
rc_t
ss_m::destroy_mrbt_file(const stid_t& fid)
{
    //TODO: SHORE-KITS-API
    assert(0);
}

/*--------------------------------------------------------------*
 *  ss_m::create_rec()                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::create_rec(const stid_t& fid, const vec_t& hdr,
                 smsize_t len_hint, const vec_t& data, rid_t& new_rid,
#ifdef SM_DORA
                 const bool bIgnoreLocks,
#endif
                 uint4_t  policy
                 )
{
    //TODO: SHORE-KITS-API
    assert(0);
}

/*--------------------------------------------------------------*
 *  ss_m::destroy_rec()                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::destroy_rec(const rid_t& rid
#ifdef SM_DORA
                  , const bool bIgnoreLocks
#endif
                  )
{
     //TODO: SHORE-KITS-API
    assert(0);
}


/*--------------------------------------------------------------*
 *  ss_m::create_mrbt_rec_in_page()                             *
 *--------------------------------------------------------------*/
rc_t
ss_m::find_page_and_create_mrbt_rec(const stid_t& fid, const lpid_t& leaf, const vec_t& hdr,
				    smsize_t len_hint, const vec_t& data, rid_t& new_rid,
				    const bool bIgnoreLocks, const bool bIgnoreLatches)
{
    //TODO: SHORE-KITS-API
    assert(0);
}

/*--------------------------------------------------------------*
 *  ss_m::destroy_mrbt_rec()                                    *
 *--------------------------------------------------------------*/
rc_t
ss_m::destroy_mrbt_rec(const rid_t& rid
		       , const bool bIgnoreLocks, const bool bIgnoreLatches
                  )
{
    //TODO: SHORE-KITS-API
    assert(0);
}
