/*<std-header orig-src='shore' incl-file-exclusion='PMAP_H'>

 $Id: pmap.h,v 1.12 2010/06/08 22:28:55 nhall Exp $

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

#ifndef PMAP_H
#define PMAP_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#include <bitmap.h>

struct Pmap 
{
    /* number of bits */
    enum    { _count = smlevel_0::ext_sz };
    /* number of bytes */
    enum    { _size = smlevel_0::ext_map_sz_in_bytes };

    u_char  bits[_size];

    inline    Pmap() {
        clear_all();
    }

    inline    void    set(int bit) { bm_set(bits, bit); }
    inline    void    clear(int bit) { bm_clr(bits, bit); }

    inline    bool    is_set(int bit) const { return bm_is_set(bits, bit); }
    inline    bool    is_clear(int bit) const { return bm_is_clr(bits, bit);}

    inline    int    num_set() const { return bm_num_set(bits, _count); }
    inline    int    num_clear() const { return bm_num_clr(bits, _count); }

    inline    int    first_set(int start) const {
        return bm_first_set(bits, _count, start);
    }
    inline    int    first_clear(int start) const {
        return bm_first_clr(bits, _count, start);
    }
    inline    int    last_set(int start) const {
        return bm_last_set(bits, _count, start);
    }
    inline    int    last_clear(int start) const {
        return bm_last_clr(bits, _count, start);
    }

    inline    int    size() const { return _size; }
    inline    int    count() const { return _count; }

    /* bm_num_set is too expensive for this use.
     XXX doesn't work if #bits != #bytes * 8 */
    inline    bool    is_empty() const {
        unsigned    i;
        for (i = 0; i < _size; i++)
            if (bits[i])
                break;
        return (i == _size);
    }
    inline    void    clear_all() { bm_zero(bits, _count); }
    inline    void    set_all() { bm_fill(bits, _count); }

    ostream    &print(ostream &s) const;
};

extern    ostream &operator<<(ostream &, const Pmap &pmap);

/* Aligned Pmaps, aka page map. Bit map showing which pages
 * are allocated (bit set) or just reserved (bit not set).
 *
 * Depending upon the pmap size it automagically
 * provides a filler in the pmap to align it to a 4 byte boundary.
 * This aligned version is used in various structures to guarantee
 * size and alignment of other members 
*/

#define SM_EXTENTSIZE_IN_BYTES ((SM_EXTENTSIZE+7)/8)
#if ((SM_EXTENTSIZE_IN_BYTES/4)*4)==(SM_EXTENTSIZE_IN_BYTES)
// #warning Pmap_Align4: Pmap
typedef    Pmap    Pmap_Align4;
#else
class Pmap_Align4 : public Pmap {
public:
    inline    Pmap_Align4    &operator=(const Pmap &from) {
        *(Pmap *)this = from;    // don't copy the filler
        return *this;
    }
private:
#if ((SM_EXTENTSIZE_IN_BYTES & 0x3)==0x3)
// #warning Pmap_Align4: 1 byte needed
    fill1    filler;    // keep purify happy
#elif ((SM_EXTENTSIZE_IN_BYTES & 0x2)==0x2)
// #warning Pmap_Align4: 2 bytes needed
    fill2    filler;    // keep purify happy
#elif ((SM_EXTENTSIZE_IN_BYTES & 0x1)==0x1)
// #warning Pmap_Align4: 3 bytes needed
    fill1    filler1;    // keep purify happy
    fill2    filler2;    // keep purify happy
#else
#error Programmer failure: SM_EXTENTSIZE_IN_BYTES  SM_EXTENTSIZE
#endif
};
#endif

/*<std-footer incl-file-exclusion='PMAP_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
