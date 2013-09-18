/*<std-header orig-src='shore' incl-file-exclusion='LID_T_H'>

 $Id: lid_t.h,v 1.38 2010/12/08 17:37:34 nhall Exp $

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

#ifndef LID_T_H
#define LID_T_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifndef BASICS_H
#include "basics.h"
#endif

/*********************************************************************
 * Logical IDs
 *
 ********************************************************************/

/**\addtogroup IDS 
 * The following persistent storage structures have identifiers, not
 * all of which(ids) are persistent:
 * - Indexes: stid_t (store id)
 * - Pages:   pid_t (short page id), lpid_t (long page id)
 * - Volumes: vid_t (short volume id), lvid_t (long volume id)
 * - Device
 */

/*
    Physical volume IDs (vid_t) are currently used to make unique
    long volume IDs.  This is a temporary hack which we support with
    this typedef:
*/
typedef uint16_t VID_T;

/**\brief long volume ID.  See \ref IDS. 
 *\ingroup IDS
 *
 * \details A long, almost-unique identifier for a volume, generated from
 * a clock and a network address. Written to a volume's header.
 * This is the only persistent identifier for a volume, and it is
 * used to be sure that one doesn't doubly-mount a volume via different
 * paths. 
 * Thus, in certain sm methods, the long volume ID is used to identify
 * a volume, e.g., in destroy_vol, get_volume_quota.
*/
struct lvid_t {
    /* usually generated from net addr of creating server */
    uint32_t high;
    /* usually generated from timeofday when created */
    uint32_t low;

    /* do not want constructors for things embeded in objects. */
    lvid_t() : high(0), low(0) {}
    lvid_t(uint32_t hi, uint32_t lo) : high(hi), low(lo) {}
        
    bool operator==(const lvid_t& s) const
                        {return (low == s.low) && (high == s.high);}
    bool operator!=(const lvid_t& s) const
                        {return (low != s.low) || (high != s.high);}

    // in lid_t.cpp:
    friend ostream& operator<<(ostream&, const lvid_t&);
    friend istream& operator>>(istream&, lvid_t&);

    // defined in lid_t.cpp
    static const lvid_t null;
};


inline uint32_t w_hash(const lvid_t& lv)
{
    return lv.high + lv.low;
}
#endif
