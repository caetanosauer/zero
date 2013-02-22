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

/** @file workload/tpcc/common/tpcc_random.h
 *  
 *  @brief Functions used for the generation of the inputs for
 *         all the TPC-C transactions
 *
 *  @version Based on TPC-C Standard Specification Revision 5.4 (Apr 2005)
 */

#ifndef __TPCC_COMMON_H
#define __TPCC_COMMON_H

#include "util.h"


ENTER_NAMESPACE(tpcc);


/** Terminology
 *  
 *  [x .. y]:        Represents a closed range of values starting with x 
 *                   and ending with y
 *  NURand(A, x, y): Non-uniform distributed value between x and y
 *  URand(x, y):     Uniformly distributed value between x and y
 */                  


/** Exported Functions */

int NURand(int A, int low, int high);

int generate_cust_last(int select, char* dest);

EXIT_NAMESPACE(tpcc);

#endif

