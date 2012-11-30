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
/*<std-header orig-src='shore'>

 $Id: atomic_templates.h,v 1.5 2010/12/17 19:36:26 nhall Exp $

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

/*
* The vast majority of the atomic add instructions are for
* incrementing or decrementing by one. For those, we use
* atomic_inc and atomic_dec.
*
* For the few others, we need to be careful about the
* delta argument lest we lose bits in it by silent conversion
* from a 64-bit int to a 32-bit int.
* For that reason, we've made templates for each delta type
* and what with their unwieldy names, we might be forced to
* be careful about which we use.
*
*/

#ifndef ATOMIC_TEMPLATES_H
#define ATOMIC_TEMPLATES_H

#include "atomic_ops.h"

/* The volatile in the template might appear to be
 * redundant, but it allows us to use T that isn't itself volatile
 *
 * Don't be surprised if you have to expand this list of 
 * specializations.
 */

/*
template<class T> void atomic_inc(T volatile &val)
{ lintel::unsafe::atomic_fetch_add<T>(const_cast<T*>(&val), 1); }
template<class T> void atomic_dec(T volatile &val)
{ lintel::unsafe::atomic_fetch_sub<T>(const_cast<T*>(&val), 1); }
template<class T> T atomic_inc_nv(T volatile &val)
{ return lintel::unsafe::atomic_fetch_add<T>(const_cast<T*>(&val), T(1))+1; }
template<class T> T atomic_dec_nv(T volatile &val)
{ return lintel::unsafe::atomic_fetch_sub<T>(const_cast<T*>(&val), T(1))-1; }
template<class T> void atomic_add_int_delta(T volatile &val, int delta)
{ lintel::unsafe::atomic_fetch_add<T>(const_cast<T*>(&val), delta); }
template<class T> void atomic_add_long_delta(T volatile &val, long delta)
{ lintel::unsafe::atomic_fetch_add<T>(const_cast<T*>(&val), delta); }
*/

#endif          /*</std-footer>*/
