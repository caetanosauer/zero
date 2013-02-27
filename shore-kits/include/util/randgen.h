/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
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

#ifndef __RANDGEN_H
#define __RANDGEN_H

#ifdef __GCC
#include <cstdlib>
#else
#include <stdlib.h> /* On Sun's CC <stdlib.h> defines rand_r,
                       <cstdlib> doesn't */
#endif


#undef USE_STHREAD_RAND
//#define USE_STHREAD_RAND

#ifdef USE_STHREAD_RAND
#include "sm_vas.h"
#endif

class randgen_t 
{
    unsigned int _seed;
    
public:

    randgen_t() {
        reset(0);
    }
    
    randgen_t(unsigned int seed) {
        reset(seed);
    }

    randgen_t(void* addr) {
        // losses the top 4 bytes
        reset((unsigned int)((long)addr));
    }

    void reset(unsigned int seed) {
        _seed = seed;
    }
    
    int rand() {
        return rand_r(&_seed);
    }
    
    /**
     * Returns a pseudorandom, uniformly distributed int value between
     * 0 (inclusive) and the specified value (exclusive).
     *
     * Source http://java.sun.com/j2se/1.5.0/docs/api/java/util/Random.html#nextInt(int)
     */
    int rand(int n) {
        assert(n > 0);

#ifdef USE_STHREAD_RAND       
        int k = abs(sthread_t::me()->rand());
#endif

        if ((n & -n) == n) {  
            // i.e., n is a power of 2
#ifdef USE_STHREAD_RAND
            return (int)((n * (uint64_t)k) / RAND_MAX);
#else
            return (int)((n * (uint64_t)rand()) / RAND_MAX);
#endif
        }

        int bits, val;
        do {
#ifdef USE_STHREAD_RAND
            bits = k;
#else
            if (RAND_MAX < n) {
		int bits_lower = rand();
		int bits_upper = rand();
		int bits_last = rand() % 2;
		bits_upper = bits_upper << 15;
		bits_last = bits_last << 30;
		bits = bits_lower + bits_upper + bits_last;
	    } else {
		bits = rand();
	    }
#endif
            val = bits % n;
        } while(bits - val + (n-1) < 0);
        
        return val;
    }
    
};


#endif
