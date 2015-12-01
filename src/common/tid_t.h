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

// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='TID_T_H'>

 $Id: tid_t.h,v 1.68 2010/06/15 17:28:29 nhall Exp $

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

#ifndef TID_T_H
#define TID_T_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include "AtomicCounter.hpp"

/**\brief Transaction ID
 *
 * \ingroup IDS
 * \details
 * Transaction IDs are 64-bit quantities.
 * They can be constructed of and used as a pair
 * of two 32-bit value, the high and low parts,
 * or they can be constructed from a single 64-bit value.
 *
 * \note The two-part nature comes from the days before 64-bit architectures,
 * and it's retained for the purpose of printing transaction ids.
 * They are output in the form "hi.low", which is more readable than
 * printing as a 64-bit value.
 * In no other way need we maintain high and low parts.
 */
class tid_t {
public:
    typedef uint64_t datum_t;
    enum { hwm = max_uint4 };

    tid_t() : _data(0) { }
    tid_t(uint32_t l, uint32_t h) : _data( (((datum_t) h) << 32) | l ) { }
    tid_t(datum_t x) : _data(x) { }

    uint32_t get_hi() const { return (uint32_t) (_data >> 32); }
    uint32_t get_lo() const { return (uint32_t) _data; }
    datum_t  as_int64() const { return _data; }

    tid_t& operator=(const tid_t& t)    {
        _data = t._data;
        return *this;
    }

    bool is_null() const { return _data == 0; }

    datum_t atomic_incr() {
        return lintel::unsafe::atomic_fetch_add(&_data, 1)+1;
    }
    tid_t &atomic_assign_max(const tid_t &tid) {
	const datum_t new_value = tid._data;
        datum_t old_value = _data;
	do {
	    if(new_value > old_value) {
		// do nothing
	    } else {
		break;
	    }
	} while (!lintel::unsafe::atomic_compare_exchange_strong(static_cast<uint64_t*>(&_data), &old_value, new_value));

        return *this;
    }
    tid_t &atomic_assign_min(const tid_t &tid) {
	const datum_t new_value = tid._data;
        datum_t old_value = _data;
	do {
	    if(new_value < old_value) {
		// do nothing
	    } else {
		break;
	    }
	} while (!lintel::unsafe::atomic_compare_exchange_strong(static_cast<uint64_t*>(&_data), &old_value, new_value));
	return *this;
    }

    inline bool operator==(const tid_t& tid) const  {
        return _data == tid._data;
    }
    inline bool operator!=(const tid_t& tid) const  {
        return !(*this == tid);
    }
    inline bool operator<(const tid_t& tid) const  {
        return _data < tid._data;
    }
    inline bool operator<=(const tid_t& tid) const  {
        return !(tid < *this);
    }
    inline bool operator>(const tid_t& tid) const  {
        return (tid < *this);
    }
    inline bool operator>=(const tid_t& tid) const  {
        return !(*this < tid);
    }

    static const tid_t Max;
    static const tid_t null;

private:

    datum_t        _data;
};


/* XXX yes, this is disgusting, but at least it allows it to
   be a shore.def option.  In reality, this specification should
   be revisited.    These fixed length objects have caused a
   fair amount of problems, and it might be time to rethink the
   issue a bit. */
#ifdef COMMON_GTID_LENGTH
#define max_gtid_len        COMMON_GTID_LENGTH
#else
#define max_gtid_len  96
#endif

#ifdef COMMON_SERVER_HANDLE_LENGTH
#define max_server_handle_len  COMMON_SERVER_HANDLE_LENGTH
#else
#define max_server_handle_len  96
#endif


#include <w_stream.h>

inline ostream& operator<<(ostream& o, const tid_t& t)
{
    return o << t.get_hi() << '.' << t.get_lo();
}

inline istream& operator>>(istream& i, tid_t& t)
{
    char ch;
    uint32_t h, l;
    i >> h >> ch >> l;
    t = tid_t(l,h);
    return i;
}


#include "w_opaque.h"

/**\typedef opaque_quantity<max_gtid_len> gtid_t
 * \brief Global transaction Identifier used for Two-Phase Commit
 */
typedef opaque_quantity<max_gtid_len> gtid_t;
/**\typedef opaque_quantity<max_server_handle_len> server_handle_t;
 * \brief Coordinator Handle used for Two-Phase Commit
 * */
typedef opaque_quantity<max_server_handle_len> server_handle_t;

/*<std-footer incl-file-exclusion='TID_T_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
