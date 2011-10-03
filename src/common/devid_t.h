/*<std-header orig-src='shore' incl-file-exclusion='DEVID_T_H'>

 $Id: devid_t.h,v 1.22 2010/07/26 23:37:06 nhall Exp $

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

#ifndef DEVID_T_H
#define DEVID_T_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

/**\brief Internal Device ID
 *
 * \ingroup IDS
 *
 * \details
 * This identifier is not persistent; it is assigned when
 * a device is mounted (by the filesystem's file name (a string))
 */
struct devid_t {
    uint64_t    id;
    uint32_t    dev;
#ifdef ZERO_INIT
    fill4    dummy;
#endif

    devid_t() : id(0), dev(0) {};
    devid_t(const char* pathname);

    bool operator==(const devid_t& d) const {
        return id == d.id && dev == d.dev;
    }

    bool operator!=(const devid_t& d) const {return !(*this==d);}
    friend ostream& operator<<(ostream&, const devid_t& d);

    static const devid_t null;
};

/*<std-footer incl-file-exclusion='DEVID_T_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
