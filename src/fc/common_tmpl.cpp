/*<std-header orig-src='shore'>

 $Id: common_tmpl.cpp,v 1.9 2010/05/26 01:20:21 nhall Exp $

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

/*
 * Instantiations of commonly used fc templates
 */
#ifdef EXPLICIT_TEMPLATE
#include "w.h"
#include "w_minmax.h"

template class w_auto_delete_array_t<char>;

#ifndef GNUG_BUG_14
template int max<int>(int, int);
template  u_long max<u_long>(u_long,  u_long);
template  int    max<int>( int,  int);
template  u_int  max<u_int>( u_int,  u_int);
template  u_short max<u_short>( u_short,  u_short);
#endif /* ! GNUG_BUG_14 */
#endif

