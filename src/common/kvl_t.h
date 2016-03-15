/*<std-header orig-src='shore' incl-file-exclusion='KVL_T_H'>

 $Id: kvl_t.h,v 1.10 2010/05/26 01:20:12 nhall Exp $

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

#ifndef KVL_T_H
#define KVL_T_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/* NB: you must already have defined the type size_t,
 * (which is defined include "basics.h") before you include this.
 */

#include "vec_t.h"

/**\brief Key-Value Lock identifier.
 *
 * \details
 * Used by the lock manager.
 */
struct kvl_t {
    StoreID            stid;
    uint32_t        h;
    uint32_t        g;

    static const cvec_t eof;
    static const cvec_t bof;

    // Empty key-value
    NORET            kvl_t();
    // Empty key-value lock id for a store ID.
    NORET            kvl_t(StoreID id, const cvec_t& v);
    // Empty key-value lock id for a store ID.
    NORET            kvl_t(
        StoreID                _stid,
        const cvec_t&         v1,
        const cvec_t&         v2);

    NORET            ~kvl_t();

    NORET            kvl_t(const kvl_t& k);

    kvl_t&             operator=(const kvl_t& k);

    kvl_t&             set(StoreID s, const cvec_t& v);
    kvl_t&             set(StoreID s,
        const cvec_t&         v1,
        const cvec_t&         v2);
    bool operator==(const kvl_t& k) const;
    bool operator!=(const kvl_t& k) const;
    friend ostream& operator<<(ostream&, const kvl_t& k);
    friend istream& operator>>(istream&, kvl_t& k);
};

inline NORET
kvl_t::kvl_t()
    : stid(0), h(0), g(0)
{
}

inline NORET
kvl_t::kvl_t(StoreID id, const cvec_t& v)
    : stid(id)
{
    v.calc_kvl(h), g = 0;
}

inline NORET
kvl_t::kvl_t(StoreID id, const cvec_t& v1, const cvec_t& v2)
    : stid(id)
{
    v1.calc_kvl(h); v2.calc_kvl(g);
}

inline NORET
kvl_t::~kvl_t()
{
}

inline NORET
kvl_t::kvl_t(const kvl_t& k)
    : stid(k.stid), h(k.h), g(k.g)
{
}

inline kvl_t&
kvl_t::operator=(const kvl_t& k)
{
    stid = k.stid;
    h = k.h, g = k.g;
    return *this;
}


inline kvl_t&
kvl_t::set(StoreID s, const cvec_t& v)
{
    stid = s, v.calc_kvl(h), g = 0;
    return *this;
}

inline kvl_t&
kvl_t::set(StoreID s, const cvec_t& v1, const cvec_t& v2)
{
    stid = s, v1.calc_kvl(h), v2.calc_kvl(g);
    return *this;
}

inline bool
kvl_t::operator==(const kvl_t& k) const
{
    return h == k.h && g == k.g;
}

inline bool
kvl_t::operator!=(const kvl_t& k) const
{
    return ! (*this == k);
}

/*<std-footer incl-file-exclusion='KVL_T_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
