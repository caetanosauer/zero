/*<std-header orig-src='shore' incl-file-exclusion='STID_T_H'>

 $Id: stid_t.h,v 1.15 2010/12/08 17:37:34 nhall Exp $

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

#ifndef STID_T_H
#define STID_T_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/**\brief Store Number
 *\ingroup IDS
 * \details
 * This type represents a store number, 
 * used when the volume id is implied somehow.
 *
 * See \ref IDS.
 */
typedef uint32_t    snum_t;

#ifndef VID_T_H
#include <vid_t.h>
#endif
#ifndef DEVID_T_H
#include <devid_t.h>
#endif

#include <sthread.h>

/**\brief A class that performs comparisons of snum_t for use with std::map */ 
struct compare_snum_t 
{
    bool operator() (snum_t const &a, snum_t const &b) const
    {
        return a < b;
    }
};

/**\brief Store ID.  See \ref IDS.
 *\ingroup IDS
 * \details
 * This class represents a store identifier. 
 * A store id is part of record identifiers, and by itself, it
 * identifies files and indexes.
 * It contains a volume identifier, vid_t.
 * 
 *
 * See \ref IDS.
 */
struct stid_t {
    vid_t    vol;
    fill2    filler; // vol is 2 bytes, store is now 4
    snum_t    store;
    
    stid_t();
    stid_t(const stid_t& s);
    stid_t(vid_t vid, snum_t snum);
    stid_t(volid_t vid, snum_t snum);

    bool operator==(const stid_t& s) const;
    bool operator!=(const stid_t& s) const;

    friend ostream& operator<<(ostream&, const stid_t& s);
    friend istream& operator>>(istream&, stid_t& s);

    static const stid_t null;
    operator const void*() const;
};

inline stid_t::stid_t(const stid_t& s) : vol(s.vol), store(s.store)
{}

inline stid_t::stid_t() : vol(0), store(0)
{}

inline stid_t::stid_t(vid_t v, snum_t s) : vol(v), store(s)
{}
inline stid_t::stid_t(volid_t v, snum_t s) : vol(vid_t(v)), store(s)
{}

inline stid_t::operator const void*() const
{
    return vol ? (void*) 1 : 0;
}


inline bool stid_t::operator==(const stid_t& s) const
{
    return (vol == s.vol) && (store == s.store);
}

inline bool stid_t::operator!=(const stid_t& s) const
{
    return ! (*this == s);
}

/** maximum number of stores that can exist in one volume. 1 to 65536. */
#define MAX_STORE_COUNT 65536

/*<std-footer incl-file-exclusion='STID_T_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
