/*<std-header orig-src='shore' incl-file-exclusion='XCT_DEPENDENT_H'>

 $Id: xct_dependent.h,v 1.9 2010/06/21 20:39:39 nhall Exp $

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

#ifndef XCT_DEPENDENT_H
#define XCT_DEPENDENT_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

class xct_t;

class xct_dependent_t {
public:
    virtual NORET        ~xct_dependent_t();

    virtual void        xct_state_changed(
    smlevel_1::xct_state_t        old_state,
    smlevel_1::xct_state_t        new_state) = 0;

    xct_t*                xd() const { return _xd; }
protected:
    NORET               xct_dependent_t(xct_t* xd);

    /* Our parent class won't be initialized yet at the time our
       constructor is called, so it's a Very Bad Thing to go adding
       ourselves to the list. Instead, we require the parent class to
       call this function after it finishes constructing.

       If they forget to register our destructor will assert.
     */
    void        register_me(); // called because our 
private:
    friend class xct_t;
    // Must protect the list when detaching; the
    // only way to do that is through the xct_t, so
    // we need a ptr to it here:
    xct_t *     _xd;
    w_link_t    _link;
    bool        _registered;
};

/*<std-footer incl-file-exclusion='XCT_DEPENDENT_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
