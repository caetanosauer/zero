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

#include <w_base.h>

/* sizes-in-bytes for all persistent data in the SM. */
typedef uint32_t               smsize_t;

/* For types of store, volumes, see stid_t.h and vid_t.h */

typedef uint32_t    PageID;
typedef uint32_t    StoreID;

// Used in log archive
typedef int32_t run_number_t;

/* Type of a record# on a page  in SM (sans page,store,volume info) */
typedef int16_t slotid_t;

/**
* \brief An integer to point to any record in B-tree pages.
* \details
* -1 if foster-child, 0 if pid0, 1 or larger if real child.
* Same as slotid_t, but used to avoid confusion.
*/
typedef int16_t general_recordid_t;
/**
 * \brief Defines constant values/methods for general_recordid_t.
 */
struct GeneralRecordIds {
    enum ConstantValues {
        /** "Record not found" etc. */
        INVALID = -2,
        /** Represents a foster child record. */
        FOSTER_CHILD = -1,
        /** Represents a PID0 record. */
        PID0 = 0,
        /** Represents the first real child. */
        REAL_CHILD_BEGIN = 1,
    };

    static slotid_t from_general_to_slot(general_recordid_t general) {
        return general - 1;
    }
    static general_recordid_t from_slot_to_general(slotid_t slot) {
        return slot + 1;
    }
};

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
