/*<std-header orig-src='shore' incl-file-exclusion='SM_INT_1_H'>

 $Id: sm_int_1.h,v 1.14 2010/10/27 17:04:23 nhall Exp $

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

#ifndef SM_INT_1_H
#define SM_INT_1_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#if defined(SM_SOURCE) && !defined(SM_LEVEL)
#    define SM_LEVEL 1
#endif

#ifndef SM_INT_0_H
#include "sm_int_0.h"
#endif


class chkpt_m;

/* xct_freeing_space implies that the xct is completed, but not yet freed stores and
   extents.  xct_ended implies completed and freeing space completed */
class smlevel_1 : public smlevel_0 {
public:

    /**\todo xct_state_t */
    // The numeric equivalents of state are not significant; they are
    // given here only for convenience in debugging/grepping
	// Well, their ORDER is significant, so that you can only
	// change state to a larger state with change_state().
    enum xct_state_t {  xct_stale = 0x0,  
                        xct_active = 0x1,  // active or rolling back in
                        // recovery/undo, or doing rollback_work
                        xct_prepared = 0x2, 
                        xct_chaining = 0x3, 
                        xct_committing = 0x4, 
                        xct_aborting = 0x5, 
                        xct_freeing_space = 0x6, 
                        xct_ended = 0x7
    };
    static chkpt_m*    chkpt;
};

#if (SM_LEVEL >= 1)
#    include <lock.h>
#    include <logrec.h>
#    include <xct.h>

#endif
class xct_log_warn_check_t : public smlevel_0 {
public:
    static w_rc_t check(xct_t*&);
};
#if defined(__GNUC__) && __GNUC_MINOR__ > 6
ostream& operator<<(ostream& o, const smlevel_1::xct_state_t& xct_state);
#endif

/*<std-footer incl-file-exclusion='SM_INT_1_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
