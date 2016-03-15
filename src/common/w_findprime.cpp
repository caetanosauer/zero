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
/*<std-header orig-src='shore' incl-file-exclusion='W_FINDPRIME_CPP'>

 $Id: w_findprime.cpp,v 1.3 2010/06/23 23:42:57 nhall Exp $

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

#include "w_findprime.h"
#include <vector>

// lots of help from Wikipedia here!
int64_t w_findprime(int64_t min) 
{
    // the first 25 primes
    static char const prime_start[] = {
    // skip 2,3,5 because our mod60 test takes care of them for us
    /*2, 3, 5,*/ 7, 11, 13, 17, 19, 23, 29, 31, 37, 41,
    43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97
    };
    // x%60 isn't on this list, x is divisible by 2, 3 or 5. If it
    // *is* on the list it still might not be prime
    static char const sieve_start[] = {
    // same as the start list, but adds 1,49 and removes 3,5
    1, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 49, 53, 59
    };

    // use the starts to populate our data structures
    std::vector<int64_t> primes(prime_start, prime_start+sizeof(prime_start));
    char sieve[60];
    memset(sieve, 0, sizeof(sieve));

    for(uint64_t i=0; i < sizeof(sieve_start); i++)
    sieve[int64_t(sieve_start[i])] = 1;

    /* We aren't interested in 4000 digit numbers here, so a Sieve of
       Erastothenes will work just fine, especially since we're
       seeding it with the first 25 primes and avoiding the (many)
       numbers that divide by 2,3 or 5.
     */
    for(int64_t x=primes.back()+1; primes.back() < min; x++) {
    if(!sieve[x%60])
        continue; // divides by 2, 3 or 5

    bool prime = true;
    for(int64_t i=0; prime && primes[i]*primes[i] <= x; i++) 
        prime = (x%primes[i]) > 0;

    if(prime) 
        primes.push_back(x);
    }

    return primes.back();
}
