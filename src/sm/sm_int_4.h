/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

/*<std-header orig-src='shore' incl-file-exclusion='SM_INT_4_H'>

 $Id: sm_int_4.h,v 1.10 2010/05/26 01:20:43 nhall Exp $

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

#ifndef SM_INT_4_H
#define SM_INT_4_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#if defined(SM_SOURCE) && !defined(SM_LEVEL)
#    define SM_LEVEL 4
#endif

#ifndef SM_INT_3_H
#include "sm_int_3.h"
#endif

class ss_m;
class lid_m;

class smlevel_4 : public smlevel_3 {
public:
    static ss_m*    SSM;    // we will change to lower case later
    static lid_m*    lid;
};
typedef smlevel_4 smlevel_top;

#if (SM_LEVEL >= 4)
#    include <btcursor.h>
#    include <lid.h>
#    include <xct_dependent.h>
#    include <prologue.h>
#endif

/*<std-footer incl-file-exclusion='SM_INT_4_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
