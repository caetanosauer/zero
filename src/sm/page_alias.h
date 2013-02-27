/*<std-header orig-src='shore' incl-file-exclusion='PAGE_ALIAS_H'>

 $Id: page_alias.h,v 1.3 2010/05/26 01:20:40 nhall Exp $

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

#ifndef PAGE_ALIAS_H
#define PAGE_ALIAS_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/* This is a hack.  This information should really be generated at
   compile time from the size of the actual data structures.   Or
   the code which depends upon this should just 'new' the appropriate
   data structures.   However, this puts all the dependencies in one
   place for now without doing major work. */

/* All the aliases are the same size for now, could change in future */

#if defined(ARCH_LP64)
#define    PAGE_ALIAS    40
#else
#define    PAGE_ALIAS    24
#endif

#define    PAGE_ALIAS_FILE        (PAGE_ALIAS)
#define    PAGE_ALIAS_LGDATA    (PAGE_ALIAS)


/*<std-footer incl-file-exclusion='PAGE_ALIAS_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
