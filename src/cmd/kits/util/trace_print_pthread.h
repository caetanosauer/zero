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

/** @file trace_print_pthread.h
 *
 *  @brief Exports trace_print_pthread().
 *
 *  @bug See trace_print_pthread.cpp.
 */
#ifndef _TRACE_PRINT_PTHREAD_H
#define _TRACE_PRINT_PTHREAD_H

#include <pthread.h> /* for pthread_t */
#include <cstdio>   /* for FILE* */



/* exported functions */

void trace_print_pthread(FILE* out_stream, pthread_t thread);



#endif
