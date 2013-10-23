/*<std-header orig-src='shore' incl-file-exclusion='W_BITMAP_H'>

 $Id: w_bitmap.h,v 1.13 2010/05/26 01:20:23 nhall Exp $

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

#ifndef W_BITMAP_H
#define W_BITMAP_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <w_base.h>

/** \brief Bitmaps of arbitrary sizes (in bits).  NOT USED by the storage manager.
 *\ingroup UNUSED 
 */
class w_bitmap_t : public w_base_t {
public:
    /// construct for \e size bits
    NORET            w_bitmap_t(uint32_t size);
    /// construct for \e size bits, using given pointer to storage 
    NORET            w_bitmap_t(uint8_t* p, uint32_t size);

    NORET            ~w_bitmap_t();

    /// clear all bits
    void            zero();
    /// set all bits
    void            fill();

    void            resetPtr(uint8_t* p, uint32_t size);
    /// set bit at offset
    void            set(uint32_t offset);
    /// return first set bit after bit at start
    int32_t            first_set(uint32_t start) const;
    /// return # bits set
    uint32_t            num_set() const;
    /// return true iff bit at offset is set
    bool            is_set(uint32_t offset) const;

    /// clear bit at offset
    void            clr(uint32_t offset);
    /// return first clear bit after bit at start
    int32_t            first_clr(uint32_t start) const;
    /// return # bits clear
    uint32_t            num_clr() const;
    /// return true iff bit at offset is clear
    bool            is_clr(uint32_t offset) const;

    /// return size in bits
    uint32_t            size() const;        // # bits

    /// return size in bytes needed for numBits
    static int        bytesForBits(uint32_t numBits);

    /// return pointer to the storage area
    uint8_t*            addr();
    /// return const pointer to the storage area
    const uint8_t*        addr() const;

    friend ostream&        operator<<(ostream&, const w_bitmap_t&);
private:
    uint8_t*             ptr;
    uint32_t            sz; // # bits
    bool            mem_alloc;

};

inline NORET
w_bitmap_t::w_bitmap_t(uint8_t* p, uint32_t size)
    : ptr(p), sz(size), mem_alloc(false)
{
}

inline NORET
w_bitmap_t::~w_bitmap_t()
{
   if (mem_alloc) delete [] ptr ; 
}

inline void 
w_bitmap_t::resetPtr(uint8_t* p, uint32_t size)
{
   w_assert9(!mem_alloc);
   sz = size;
   ptr = p;
}

inline bool
w_bitmap_t::is_clr(uint32_t offset) const
{
    return !is_set(offset);
}

inline uint32_t
w_bitmap_t::size() const
{
    return sz ;
}

inline uint32_t
w_bitmap_t::num_clr() const
{
    return sz - num_set();
}

inline uint8_t*
w_bitmap_t::addr() 
{
    return ptr;
}

inline const uint8_t*
w_bitmap_t::addr() const
{
    return ptr;
}


/*<std-footer incl-file-exclusion='W_BITMAP_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
