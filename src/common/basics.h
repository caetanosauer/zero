/*<std-header orig-src='shore' incl-file-exclusion='BASICS_H'>

 $Id: basics.h,v 1.73 2010/07/26 23:37:06 nhall Exp $

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

#ifndef BASICS_H
#define BASICS_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#ifndef W_BASE_H
#include <w_base.h>
#endif

/* sizes-in-bytes for all persistent data in the SM. */
typedef uint32_t               smsize_t;

/* For types of store, volumes, see stid_t.h and vid_t.h */

typedef uint32_t    shpid_t; 

#ifndef SM_SOURCE
// This is for servers. SM_SOURCE is defined in the SM sources.
namespace locking {
    typedef w_base_t::lock_mode_t  lock_mode_t; 
    static const lock_mode_t NL = w_base_t::NL;
    static const lock_mode_t IS = w_base_t::IS;
    static const lock_mode_t IX = w_base_t::IX;
    static const lock_mode_t SH = w_base_t::SH;
    static const lock_mode_t SIX = w_base_t::SIX;
    static const lock_mode_t UD = w_base_t::UD;
    static const lock_mode_t EX = w_base_t::EX;
    // key range locks
    static const lock_mode_t NS = w_base_t::NS;
    static const lock_mode_t NU = w_base_t::NU;
    static const lock_mode_t NX = w_base_t::NX;
    static const lock_mode_t SN = w_base_t::SN;
    static const lock_mode_t SU = w_base_t::SU;
    static const lock_mode_t SX = w_base_t::SX;
    static const lock_mode_t UN = w_base_t::UN;
    static const lock_mode_t US = w_base_t::US;
    static const lock_mode_t UX = w_base_t::UX;
    static const lock_mode_t XN = w_base_t::XN;
    static const lock_mode_t XS = w_base_t::XS;
    static const lock_mode_t XU = w_base_t::XU;
    static const lock_mode_t NN = w_base_t::NN;
    static const lock_mode_t SS = w_base_t::SS;
    static const lock_mode_t UU = w_base_t::UU;
    static const lock_mode_t XX = w_base_t::XX;
}
using namespace locking;

/**\brief Types for API used for 2PC */
namespace two_phase_commit {
	typedef w_base_t::vote_t  vote_t; 
}
using namespace two_phase_commit;
#endif


/* Type of a record# on a page  in SM (sans page,store,volume info) */
typedef int16_t slotid_t;  

/* XXX duplicates w_base types. */
const int32_t    max_int4 = 0x7fffffff;         /*  (1 << 31) - 1;  */
const int32_t    max_int4_minus1 = max_int4 -1;
const int32_t    min_int4 = 0x80000000;         /* -(1 << 31);        */

const uint16_t    max_uint2 = 0xffff;
const uint16_t    min_uint2 = 0;
const uint32_t    max_uint4 = 0xffffffff;
const uint32_t    min_uint4 = 0;



/*
 * Safe Integer conversion (ie. casting) function
 */
inline int u4i(uint32_t x) {w_assert1(x<=(unsigned)max_int4); return int(x); }

// inline unsigned int  uToi(int32_t x) {assert(x>=0); return (uint) x; }



inline bool is_aligned(smsize_t sz)
{
    return w_base_t::is_aligned(sz);
}

inline bool is_aligned(const void* p)
{
    return w_base_t::is_aligned(p);
}

/*<std-footer incl-file-exclusion='BASICS_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
