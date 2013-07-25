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

 $Id: pin.cpp,v 1.146 2010/11/08 15:06:55 nhall Exp $

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
#define PIN_C

#ifdef __GNUG__
#pragma implementation "pin.h"
#endif

#include <pin.h>
#include "suppress_unused.h"

const char* pin_i::body()
{
    //TODO: SHORE-KITS-API
    assert(0);
    SUPPRESS_NON_RETURN(char*);
}

rc_t pin_i::pin(const rid_t& rid, smsize_t start, lock_mode_t lmode,
                const bool bIgnoreLatches)
{
    SUPPRESS_UNUSED_4(rid, start, lmode, bIgnoreLatches);
    //TODO: SHORE-KITS-API
    assert(0);
    SUPPRESS_NON_RETURN(rc_t);
}

rc_t pin_i::pin(const rid_t& rid, smsize_t start, 
        lock_mode_t lmode, latch_mode_t latch_mode)
{
    SUPPRESS_UNUSED_4(rid, start, lmode, latch_mode);
    //TODO: SHORE-KITS-API
    assert(0);
    SUPPRESS_NON_RETURN(rc_t);
}

rc_t pin_i::repin(lock_mode_t lmode)
{
    SUPPRESS_UNUSED(lmode);
    //TODO: SHORE-KITS-API
    assert(0);
    SUPPRESS_NON_RETURN(rc_t);
}

void pin_i::unpin()
{
    //TODO: SHORE-KITS-API
    assert(0);
}

rc_t pin_i::update_rec(smsize_t start, const vec_t& data,
                       int* old_value /* temporary: for degugging only */
                       , const bool bIgnoreLocks
                       )
{
    SUPPRESS_UNUSED_4(start, data, old_value, bIgnoreLocks);
    //TODO: SHORE-KITS-API
    assert(0);
    SUPPRESS_NON_RETURN(rc_t);
}

rc_t pin_i::update_mrbt_rec(smsize_t start, const vec_t& data,
			    int* old_value, /* temporary: for degugging only */
			    const bool bIgnoreLocks,
			    const bool bIgnoreLatches)
{
    SUPPRESS_UNUSED_5(start, data, old_value, bIgnoreLocks, bIgnoreLatches);
    //TODO: SHORE-KITS-API
    assert(0);
    SUPPRESS_NON_RETURN(rc_t);
}

rc_t pin_i::append_rec(const vec_t& data)
{
    SUPPRESS_UNUSED(data);
    //TODO: SHORE-KITS-API
    assert(0);
    SUPPRESS_NON_RETURN(rc_t);
}

rc_t pin_i::append_mrbt_rec(const vec_t& data,
			    const bool bIgnoreLocks,
			    const bool bIgnoreLatches)
{
    SUPPRESS_UNUSED_3(data, bIgnoreLocks, bIgnoreLatches);
    //TODO: SHORE-KITS-API
    assert(0);
    SUPPRESS_NON_RETURN(rc_t);
}

rc_t pin_i::truncate_rec(smsize_t amount)
{
    SUPPRESS_UNUSED(amount);
    //TODO: SHORE-KITS-API
    assert(0);
    SUPPRESS_NON_RETURN(rc_t);
}
