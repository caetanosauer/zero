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

/*<std-header orig-src='shore' incl-file-exclusion='W_RC_H'>

 $Id: w_hashing.h,v 1.2 2010/05/26 01:20:25 nhall Exp $

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

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifndef W_CUCKOOHASHFUNCS_H
#define  W_CUCKOOHASHFUNCS_H

/** \brief A namespace to contain types related to hashing.
 */
namespace w_hashing {

/** \brief A "universal hash" class based on a random-number generator.  
 * \details
 * Helper class for other hashing classes.
 */
class uhash 
{
public:
    /// Initializes with given number.
    /// Do not use rand() for seed this for repeatability and easier debugging.
    uhash() {}
    ~uhash() {}
    /// Returns a hash of the argument.
    static uint64_t hash64(uint32_t seed, uint64_t x);
    static uint32_t hash32(uint32_t seed, uint64_t x);
    
    /// Returns a hash for arbitrary length of string.
    static uint64_t hash64(uint32_t seed, const unsigned char *str, int16_t len);
    static uint32_t hash32(uint32_t seed, const unsigned char *str, int16_t len);
    
    static uint32_t convert64_32 (uint64_t num) {
        return ((uint32_t) (num >> 32)) ^ ((uint32_t) (num & 0xFFFFFFFF));
    }
};

inline uint64_t uhash::hash64(uint32_t seed, uint64_t x) {
    return seed * (x >> 32) + (x & 0xFFFFFFFF);
}
inline uint32_t uhash::hash32(uint32_t seed, uint64_t x) {
    return convert64_32(hash64(seed, x));
}

inline uint64_t uhash::hash64(uint32_t seed, const unsigned char *str, int16_t len)
{
    // simple iterative multiply-sum method on byte-by-byte (safe on every architecture)
    w_assert1(len >= 0);
    uint64_t ret = 0;
    for (int16_t i = 0; i < len; ++i) {
        ret = seed * ret + str[i];
    }
    return ret;
}
inline uint32_t uhash::hash32(uint32_t seed, const unsigned char *str, int16_t len) {
    return convert64_32(hash64(seed, str, len));
}

} /* namespace w_hashing */

#endif
