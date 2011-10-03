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

 $Id: rand48.cpp,v 1.2 2010/05/26 01:20:21 nhall Exp $

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

#include "rand48.h"

unsigned48_t rand48::_mask(unsigned48_t x)  const
{ 
    return x & 0xffffffffffffull; 
}

unsigned48_t rand48::_update() 
{
    _state = _mask(_state*0x5deece66dull + 0xb);
    return _state;
}

double rand48::drand() 
{
    /* In order to avoid the cost of multiple floating point ops we
       conjure up a double directly based on its bitwise
       representation:

       |S|EEEEEEEEEEE|FFFFFF....F|
           (11 bits)   (52 bits)
       
       where V = (-1)**S * 2**(E-1023) * 1.F (ie F is the fractional
       part of an implied 53-bit fixed-point number which can take
       values ranging from 1.000... to 1.111...). 

       The idea is to left-justify our random bits in the mantissa
       field (hence the << 4). Setting S=0 and E=0x3ff=1023 gives
       (-1)**0 * 2**(0x3ff-1023) * 1.F = 1.F, which is always
       normalized. We then subtract 1.0 to get the answer we actually
       want -- 0.F -- and make the hardware normalize it for us.
     */
    union { unsigned48_t n; double d; } u = {
    (0x3ffull << 52) | (_update() << 4)
    };
    return u.d-1.0;
}

/**\brief Used by testers.  
 *
 * Not operators because that would conflict
 * with the std:: operators for unsigned ints, alas.
 */
void out(ofstream& o, const unsigned48_t& what)
{
    /*
     * expect "........,........,........"
     * no spaces
     */

    union {
       unsigned48_t   seed;
       unsigned short dummy[sizeof(unsigned48_t)/sizeof(unsigned short)];
    } PUN;
    PUN.seed = what;

    o << 
        PUN.dummy[0] << "," << 
        PUN.dummy[1] << "," << 
        PUN.dummy[2] << "," << 
        PUN.dummy[3] << endl;
}

/**\brief Used by testers.  
 *
 * Not operators because that would conflict
 * with the std:: operators for unsigned ints, alas.
 */
void in(ifstream& i, unsigned48_t& res)
{
    /*
     * print "0x........,0x........,0x........"
     */
    union {
       unsigned48_t   seed;
       unsigned short dummy[sizeof(unsigned48_t)/sizeof(unsigned short)];
    } PUN ;

    char             comma = ',';
    unsigned         j=0;

    while( (comma == ',') && 
        (j < sizeof(PUN.dummy)/sizeof(unsigned short)) &&
        (i >>  PUN.dummy[j])
        ) {

            if(i.peek() == ',') i >> comma;
            j++;
    }
    if(j < sizeof(PUN.dummy)/sizeof(unsigned short) ) {
        // This actually sets the badbit:
        i.clear(ios::badbit|i.rdstate());
    }
    res = PUN.seed;
}
