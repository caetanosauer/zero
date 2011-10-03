/*<std-header orig-src='shore' incl-file-exclusion='HOSTNAME_H'>

 $Id: hostname.h,v 1.12 2010/05/26 01:20:12 nhall Exp $

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

#ifndef HOSTNAME_H
#define HOSTNAME_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#if defined(Sparc) 
extern "C" {
    extern int gethostname(char *, int);
}
#endif

/*<std-footer incl-file-exclusion='HOSTNAME_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
