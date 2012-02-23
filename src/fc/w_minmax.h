/*<std-header orig-src='shore' incl-file-exclusion='W_MINMAX_H'>

 $Id: w_minmax.h,v 1.21 2010/12/08 17:37:37 nhall Exp $

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

#ifndef W_MINMAX_H
#define W_MINMAX_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#if !defined(GNUG_BUG_14) 

// WARNING: due to a gcc 2.6.* bug, do not used these
//          since there is no way to explicitly instantiate
//        function templates.

template <class T>
inline const T 
max(const T x, const T y)
{
    return x > y ? x : y;
}

template <class T>
inline const T 
min(const T x, const T y)
{
    return x < y ? x : y;
}
#endif /* !__GNUC__ */

#if HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#ifndef MAX
#define MAX(x, y)       ((x) > (y) ? (x) : (y))
#endif /*MAX*/

#ifndef MIN
#define MIN(x, y)       ((x) < (y) ? (x) : (y))
#endif /*MIN*/

/*<std-footer incl-file-exclusion='W_MINMAX_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
