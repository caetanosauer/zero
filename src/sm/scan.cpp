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

 $Id: scan.cpp,v 1.163 2010/12/09 15:20:14 nhall Exp $

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
#define SCAN_C
#ifdef __GNUG__
#   pragma implementation
#endif
#include <sm_int_4.h>
#include <sm.h>
#include <pin.h>
#include <scan.h>
#include <bf_prefetch.h>
#include <btcursor.h>

#if W_DEBUG_LEVEL > 1
inline void         pin_i::_set_lsn_for_scan() {
    //TODO: SHORE-KITS-API
    assert(0);
}
#endif

/* NOTE (frj): this whole _error_occurred/PROLOGUE thing is kind of
   broken... I think the idea is that if one function call errors out
   the scan_i will remember and just return the error instead of doing
   more damage. However, with the new strict single owner semantics of
   w_rc_t, you only get to return the error once. Nobody should be
   retrying after an error anyway, but I found at least one spot where
   _error_occurred was not set and the next PROLOGUE blew up because
   of it.
   Update(neh): Yes, that's the idea. If it's not used properly,
   it's not the prologue that's broken; it's a sign of a bug
   elsewhere, so I'll keep this in. To get around the strict
   owner semantics issue, we'll generate a new error code from the
   error number.
   
 */
#define SCAN_METHOD_PROLOGUE1                           \
    do {                                                \
        if(_error_occurred.is_error())                  \
            return RC(_error_occurred.err_num());       \
    } while(0)



/*********************************************************************
 *
 *  scan_index_i::scan_index_i(stid, c1, bound1, c2, bound2, cc, prefetch)
 *
 *  Create a scan on index "stid" between "bound1" and "bound2".
 *  c1 could be >, >= or ==. c2 could be <, <= or ==.
 *  cc is the concurrency control method to use on the index.
 *
 *********************************************************************/
scan_index_i::scan_index_i(
    const stid_t&         stid_, 
    cmp_t                 c1, 
    const cvec_t&         bound1_, 
    cmp_t                 c2, 
    const cvec_t&         bound2_, 
    bool                  include_nulls,
    concurrency_t         cc,
    lock_mode_t           mode,
    const bool            bIgnoreLatches
    ) 
: xct_dependent_t(xct()),
  _stid(stid_),
  ntype(ss_m::t_bad_ndx_t),
  _eof(false),
  _error_occurred(),
  _btcursor(0),
  _skip_nulls( ! include_nulls ),
  _cc(cc)
{
    //TODO: SHORE-KITS-API
    assert(0);
}


scan_index_i::~scan_index_i()
{
    //TODO: SHORE-KITS-API
    assert(0);
}


/*********************************************************************
 *
 *  scan_index_i::_init(cond, b1, c2, b2)
 *
 *  Initialize a scan. Called by all constructors.
 *
 *  Of which there is only 1, and it uses mode=SH
 *
 *********************************************************************/
void 
scan_index_i::_init(
    cmp_t                 cond, 
    const cvec_t&         bound,
    cmp_t                 c2, 
    const cvec_t&         b2,
    lock_mode_t           mode)
{
    //TODO: SHORE-KITS-API
    assert(0);
}



/*********************************************************************
 * 
 *  scan_index_i::xct_state_changed(old_state, new_state)
 *
 *  Called by xct_t when transaction changes state. Terminate the
 *  the scan if transaction is aborting or committing.
 *  Note: this only makes sense in forward processing, since in
 *  recovery there is no such thing as an instantiated scan_index_i. 
 *
 *********************************************************************/
void 
scan_index_i::xct_state_changed(
    xct_state_t                /*old_state*/,
    xct_state_t                new_state)
{
    //TODO: SHORE-KITS-API
    assert(0);
}


/*********************************************************************
 *
 *  scan_index_i::finish()
 *
 *  Terminate the scan.
 *
 *********************************************************************/
void 
scan_index_i::finish()
{
    //TODO: SHORE-KITS-API
    assert(0);
}


/*********************************************************************
 *
 *  scan_index_i::_fetch(key, klen, el, elen, skip)
 *
 *  Fetch current entry into "key" and "el". If "skip" is true,
 *  advance the scan to the next qualifying entry.
 *
 *********************************************************************/
rc_t
scan_index_i::_fetch(
    vec_t*         key, 
    smsize_t*         klen, 
    vec_t*         el, 
    smsize_t*         elen,
    bool         skip)
{
    //TODO: SHORE-KITS-API
    assert(0);
}
    

scan_file_i::scan_file_i(
        const stid_t& stid_, const rid_t& start,
         concurrency_t cc, bool pre, 
         lock_mode_t /*mode TODO: remove.  is documented as ignored*/,
        const bool  bIgnoreLatches) 
: xct_dependent_t(xct()),
  stid(stid_),
  curr_rid(start),
  _eof(false),
  _cc(cc), 
  _do_prefetch(pre),
  _prefetch(0)
{
    //TODO: SHORE-KITS-API
    assert(0);
}

scan_file_i::scan_file_i(const stid_t& stid_, concurrency_t cc, 
   bool pre, lock_mode_t /*mode TODO: remove. this documented as ignored*/,
   const bool  bIgnoreLatches)
: xct_dependent_t(xct()),
  stid(stid_),
  _eof(false),
  _cc(cc),
  _do_prefetch(pre),
  _prefetch(0)
{
    //TODO: SHORE-KITS-API
    assert(0);
}

scan_file_i::~scan_file_i()
{
    //TODO: SHORE-KITS-API
    assert(0);
}




rc_t scan_file_i::_init(bool for_append) 
{
    //TODO: SHORE-KITS-API
    assert(0);
}

rc_t
scan_file_i::next(pin_i*& pin_ptr, smsize_t start, bool& eof)
{
    //TODO: SHORE-KITS-API
    assert(0);
}

rc_t
scan_file_i::_next(pin_i*& pin_ptr, smsize_t start, bool& eof)
{
    //TODO: SHORE-KITS-API
    assert(0);
}

rc_t
scan_file_i::next_page(pin_i*& pin_ptr, smsize_t start, bool& eof)
{
    //TODO: SHORE-KITS-API
    assert(0);
}

void scan_file_i::finish()
{
    //TODO: SHORE-KITS-API
    assert(0);
}

/*********************************************************************
 * 
 *  scan_file_i::xct_state_changed(old_state, new_state)
 *
 *  Called by xct_t when transaction changes state. Terminate the
 *  the scan if transaction is aborting or committing.
 *  Note: this only makes sense in forward processing, since in
 *  recovery there is no such thing as an instantiated scan_file_i. 
 *
 *********************************************************************/
void 
scan_file_i::xct_state_changed(
    xct_state_t                /*old_state*/,
    xct_state_t                new_state)
{
    //TODO: SHORE-KITS-API
    assert(0);
}
