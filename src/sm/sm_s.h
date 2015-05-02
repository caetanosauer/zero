/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore' incl-file-exclusion='SM_S_H'>

 $Id: sm_s.h,v 1.94 2010/12/08 17:37:43 nhall Exp $

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

#ifndef SM_S_H
#define SM_S_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifndef STHREAD_H
#include <sthread.h>
#endif

#ifndef STID_T_H
#include <stid_t.h>
#endif

#include "basics.h"

/**\brief Long page ID.
 * \details
 * Store ID volume number + page number.
 */
class lpid_t {
public:
    vid_t          _vol;
    shpid_t        page;
    
    lpid_t();
    lpid_t(const stid_t& s, shpid_t p);
    lpid_t(vid_t v, shpid_t p);
    bool valid() const;

    vid_t         vol()   const { return _vol;}

    // necessary and sufficient conditions for
    // is_null() are determined by default constructor, q.v.
    bool        is_null() const { return page == 0; }

    bool operator==(const lpid_t& p) const;
    bool operator!=(const lpid_t& p) const;
    bool operator<(const lpid_t& p) const;
    bool operator<=(const lpid_t& p) const;
    bool operator>(const lpid_t& p) const;
    bool operator>=(const lpid_t& p) const;
    friend ostream& operator<<(ostream&, const lpid_t& p);
    friend istream& operator>>(istream&, lpid_t& p);


    static const lpid_t bof;
    static const lpid_t eof;
    static const lpid_t null;
};



class rid_t;

/**\brief Short Record ID
 *\ingroup IDS
 * \details
 * This class represents a short record identifier, which is
 * used when the volume id is implied somehow.
 *
 * A short record id contains a slot, a (short) page id, and a store number.
 * A short page id is just a page number (in basics.h).
 *
 * See \ref IDS.
 */
class shrid_t {
public:
    shpid_t        page;
    snum_t        store;
    slotid_t        slot;
    fill2        filler; // because page, snum_t are 4 bytes, slotid_t is 2

    shrid_t();
    shrid_t(const rid_t& r);
    shrid_t(shpid_t p, snum_t st, slotid_t sl) : page(p), store(st), slot(sl) {}
    friend ostream& operator<<(ostream&, const shrid_t& s);
    friend istream& operator>>(istream&, shrid_t& s);
};


#define RID_T

/**\brief Record ID
 *\ingroup IDS
 * \details
 * This class represents a long record identifier, used in the
 * Storage Manager API, but not stored persistently.
 *
 * A record id contains a slot and a (long) page id.
 * A long page id contains a store id and a volume id.
 *
 * See \ref IDS.
 */
class rid_t {
public:
    lpid_t        pid;
    slotid_t        slot;
    fill2        filler;  // for initialization of last 2 unused bytes

    rid_t();
    rid_t(vid_t vid, const shrid_t& shrid);
    rid_t(const lpid_t& p, slotid_t s) : pid(p), slot(s) {};

    stid_t stid() const;

    bool operator==(const rid_t& r) const;
    bool operator!=(const rid_t& r) const;
    bool operator<(const rid_t& r) const;
    
    friend ostream& operator<<(ostream&, const rid_t& s);
    friend istream& operator>>(istream&, rid_t& s);

    static const rid_t null;
};


#include <lsn.h>

inline ostream& operator<<(ostream& o, const lsn_t& l)
{
    return o << l.file() << '.' << l.rba();
}

inline istream& operator>>(istream& i, lsn_t& l)
{
    sm_diskaddr_t d;
    char c;
    uint64_t f;
    i >> f >> c >> d;
    l = lsn_t(f, d);
    return i;
}

inline lpid_t::lpid_t() : page(0) {}

inline lpid_t::lpid_t(const stid_t& s, shpid_t p) : _vol(s.vol), page(p)
{}

inline lpid_t::lpid_t(vid_t v, shpid_t p) :
        _vol(v), page(p)
{}

inline rid_t::rid_t() : slot(0)
{}

inline rid_t::rid_t(vid_t vid, const shrid_t& shrid) :
        pid(vid, shrid.page), slot(shrid.slot)
{}

inline bool lpid_t::operator==(const lpid_t& p) const
{
    return (page == p.page) && (_vol == p._vol);
}

inline bool lpid_t::operator!=(const lpid_t& p) const
{
    return !(*this == p);
}

inline bool lpid_t::operator<(const lpid_t& p) const
{
    if (_vol != p._vol) {
        return _vol < p._vol;
    }
    return page < p.page;
}

inline bool lpid_t::operator<=(const lpid_t& p) const
{
    return (*this == p) || (*this < p);
}

inline bool lpid_t::operator>(const lpid_t& p) const
{
    if (_vol != p._vol) {
        return _vol > p._vol;
    }
    return page > p.page;
}

inline bool lpid_t::operator>=(const lpid_t& p) const
{
    return (*this == p) || (*this > p);
}

inline bool rid_t::operator==(const rid_t& r) const
{
    return (pid == r.pid && slot == r.slot);
}

inline bool rid_t::operator!=(const rid_t& r) const
{
    return !(*this == r);
}

/*<std-footer incl-file-exclusion='SM_S_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
