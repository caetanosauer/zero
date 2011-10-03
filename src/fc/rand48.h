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
/*<std-header orig-src='shore'>

 $Id: rand48.h,v 1.3 2010/07/07 20:50:12 nhall Exp $

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

/* 
 * Thread-local pseudo-random-number generator
 */

#include "w_base.h"
#define RAND48_INITIAL_SEED 0x330eabcd1234ull
#define RAND48_INITIALIZER { RAND48_INITIAL_SEED }

typedef int64_t signed48_t;
typedef uint64_t unsigned48_t;

// used by testers (in tests/ and smsh).  Not operators because that would conflict
// with the std:: operators for unsigned ints, alas.
#include <fstream>
void in(ifstream& i, unsigned48_t& what);
void out(ofstream& o, const unsigned48_t& what);

/**\brief 48-bit pseudo-random-number generator.
 */
class rand48 {
public:
    /// Set the seed
    void     seed(unsigned48_t seed) { _state = _mask(seed); }
    /// Return 48-bit pseudo-random number
    signed48_t  rand()          { return signed48_t(_update()); }
    /// Return 64-bit pseudo-random number
    double      drand();

    /// Return 48-bit pseudo-random number modulo given maximum
    signed48_t  randn(signed48_t max)  { return signed48_t(max*drand()); }

// private: Must be public for test programs.
    unsigned48_t _update();
    unsigned48_t _mask(unsigned48_t x) const;

public:
/*! Making _state private * makes this a non-pod type, 
 * and we use it for thread-private data.
 */

/* No constructor: must seed it.  If you get a purify UMR here,
 * it's because the struct wasn't seeded, which is a programmer error
 */

    unsigned48_t _state;
};

