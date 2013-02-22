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

/** @file trace_print_pthread.cpp
 *
 *  @brief Implements trace_print_pthread(). Unfortunately, POSIX does
 *  not specify the representation of a pthread_t. This file provides
 *  a portable way to print a pthread_t instance.
 *
 *  @bug None known.
 */
#include "trace/trace_print_pthread.h" /* for prototypes */



/* definitions of exported functions */


/**
 *  @brief Print the specified pthread_t to the specified stream. This
 *  function is reentrant. It does not flush the specified stream when
 *  complete.
 *
 *  @param out_stream The stream to print the pthread_t to.
 *
 *  @param thread The pthread_t to print.
 *
 *  @return void
 */

void trace_print_pthread(FILE* out_stream, pthread_t thread)
{

  /* operating system specific */

  /* GNU Linux */
#if defined(linux) || defined(__linux)
#ifndef __USE_GNU
#define __USE_GNU
#endif

  /* detected GNU Linux */
  /* A pthread_t is an unsigned long int. */
  fprintf(out_stream, "%lu", thread);
  return;

#endif


  /* Sun Solaris */
#if defined(sun) || defined(__sun)
#if defined(__SVR4) || defined(__svr4__)

  /* detected Sun Solaris */
  /* A pthread_t is an unsigned int. */
  fprintf(out_stream, "%u", thread);
  return;

#endif
#endif

}
