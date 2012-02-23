/*<std-header orig-src='shore' incl-file-exclusion='VID_T_H'>

 $Id: vid_t.h,v 1.28 2010/06/23 23:43:29 nhall Exp $

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

#ifndef VID_T_H
#define VID_T_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
// implementation is in lid_t.cpp
#pragma interface
#endif

/**\brief Volume ID. See \ref IDS.
 *\ingroup IDS
 * \details
 * This class represents a volume identifier, the id that is persistent
 * in the database. It is usually a short integer. 
 * Its size is two bytes.
 *
 * A volume id is part of record identifiers and store identifiers,
 * as well as part of "long" page identifiers.
 *
 * See \ref IDS.
 */
struct vid_t {

    enum {
          first_local = 1
         };

                    vid_t() : vol(0) {}
                    vid_t(uint16_t v) : vol(v) {}
    void        init_local()        {vol = first_local;}

    void        incr_local()        {
                                    vol++;
                                }

    // This function casts a vid_t to a uint16_t.  It is needed
    // in lid_t.h where there is a hack to use vid_t to
    // create a long volume ID.
                    operator uint16_t () const {return vol;}

    // Data Members
    uint16_t        vol;

    static const vid_t null;
    friend inline ostream& operator<<(ostream&, const vid_t& v);
    friend inline istream& operator>>(istream&, vid_t& v);
    friend bool operator==(const vid_t& v1, const vid_t& v2)  {
        return v1.vol == v2.vol;
    }
    friend bool operator!=(const vid_t& v1, const vid_t& v2)  {
        return v1.vol != v2.vol;
    }
};

inline ostream& operator<<(ostream& o, const vid_t& v)
{
    return o << v.vol;
}
 
inline istream& operator>>(istream& i, vid_t& v)
{
    return i >> v.vol;
}

/*<std-footer incl-file-exclusion='VID_T_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
