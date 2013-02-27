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

/** @file workload/tpch/common/tpch_random.cpp
 *  
 *  @brief Functions used for the generation of the inputs for 
 *         all the tpch transaction.
 *
 *  @version Based on TPC-H Standard Specification Revision 5.4 (Apr 2005)
 */


#include "workload/tpch/tpch_random.h"
#include "workload/tpch/tpch_const.h"


ENTER_NAMESPACE(tpch);


/** Terminology
 *  
 *  [x .. y]: Represents a closed range of values starting with x and ending 
 *            with y
 *
 *  random(x,y): uniformly distributed value between x and y.
 *
 *  NURand(A, x, y): (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
 *                   non-uniform random, where
 *  exp-1 | exp-2: bitwise logical OR
 *  exp-1 % exp-2: exp-1 modulo exp-2
 *  C: random(0, A)
 */                  



/** @func random(int, int, randgen_t*)
 *
 *  @brief Generates a uniform random number between low and high. 
 *  Not seen by public.
 */

int random(int low, int high, randgen_t* rp) {

  return (low + rp->rand(high - low + 1));
}


/** @func URand(int, int)
 *
 *  @brief Generates a uniform random number between (low) and (high)
 */

int URand(int low, int high) {

  thread_t* self = thread_get_self();
  assert (self);
  randgen_t* randgenp = self->randgen();
  assert (randgenp);

  int d = high - low + 1;

  /*
  if (((d & -d) == d) && (high > 1)) {
      // we avoid to pass a power of 2 to rand()
      return( low + ((randgenp->rand(high - low + 2) + randgenp->rand(high - low))/2) );
  }
  */

  //return (low + sthread_t::me()->rand());
  return (low + randgenp->rand(d));
}


/** @func NURand(int, int, int)
 *
 *  @brief Generates a non-uniform random number
 */

int NURand(int A, int low, int high) {

  thread_t* self = thread_get_self();
  assert (self);
  randgen_t* randgenp = self->randgen();
  assert (randgenp);

  return ( (((random(0, A, randgenp) | random(low, high, randgenp)) 
             + random(0, A, randgenp)) 
            % (high - low + 1)) + low );
}

EXIT_NAMESPACE(tpch);
