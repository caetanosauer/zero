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

#ifndef __UTIL_ZIPFIAN_H
#define __UTIL_ZIPFIAN_H

//#include "rand48.h"
#include <cmath>

/* Return a zipfian-distributed random number.

   The computation is based on a x**-(1/s) curve, which has the property that
   - it's a straight line on log-log scale
   - increasing s makes the slope steeper (= more skewed)
   - the value approaches infinity as x approaches zero

   Therefore, we need to compute three things:

   cutoff=pow(n, -k): because 1/x can produce infinitely large values,
   and we only want to allow values up to 'n', we have to generate
   inputs ~U(cutoff, 1) rather than ~U(0, 1). The larger the skew and
   the range, the closer to zero we can tolerate.

   output=pow(u, -1./k): this comes straight from the observation we
   started with; output(1) = 1, and output(cutoff) = n; increasing k
   makes the output approach n more slowly, as desired.

   k=1.1*skew-1: for some reason, there's a very precise relationship
   between the k we want the final distribution to have and the k we
   should supply. I have no idea why, but empirically it holds very
   precisely for every combination of n and k which I've tested.
*/

struct zipfian 
{
//    rand48 _rng;
    double _k;
    double _mk_inv;
    double _cutoff;
    double _1mcutoff;

//    zipfian(int n, double s, long seed_val=rand48::DEFAULT_SEED)
    zipfian(int n, double s)
// _rng(seed_val), _k(1.1*s-1), _mk_inv(-1./_k)
	: _k(1.1*s-1), _mk_inv(-1./_k)
	, _cutoff(pow(n, -_k)), _1mcutoff(1-_cutoff)
    {
    }

    int operator()(double u) { return next(u); }
    
    int next(double uniform_input) {
        //double u = _rng.drand();
        uniform_input *= _1mcutoff;
        uniform_input += _cutoff;
        return (int) pow(uniform_input, _mk_inv);
    }
};

#endif // __UTIL_ZIPFIAN_H

