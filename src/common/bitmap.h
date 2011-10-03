/*<std-header orig-src='shore' incl-file-exclusion='BITMAP_H'>

 $Id: bitmap.h,v 1.23 2010/05/26 01:20:11 nhall Exp $

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

#ifndef BITMAP_H
#define BITMAP_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

extern "C" int bm_first_set(const u_char* bm, int size, int start);
extern "C" int bm_first_clr(const u_char* bm, int size, int start);

extern "C" int bm_last_set(const u_char *bm, int size, int start);
extern "C" int bm_last_clr(const u_char *bm, int size, int start);

extern "C" int bm_num_set(const u_char* bm, int size);
extern "C" int bm_num_clr(const u_char* bm, int size);

extern "C" bool bm_is_set(const u_char* bm, int offset);
extern "C" bool bm_is_clr(const u_char* bm, int offset);

extern "C" void bm_zero(u_char* bm, int size);
extern "C" void bm_fill(u_char* bm, int size);

extern "C" void bm_set(u_char* bm, int offset);
extern "C" void bm_clr(u_char* bm, int offset);

inline bool bm_is_clr(const u_char* bm, int offset)
{
    return !bm_is_set(bm, offset);
}

inline int bm_num_clr(const u_char* bm, int size)
{
    return size - bm_num_set(bm, size);
}

/*<std-footer incl-file-exclusion='BITMAP_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
