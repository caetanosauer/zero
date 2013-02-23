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

/** @file trace_force.cpp
 *
 *  @brief trace_force_() is no longer used since we started using
 *  synchronization for our output streams. It should be removed from
 *  the trace module soon.
 *
 *  @brief Exports trace_force() function. trace_force() should only
 *  be invoked by code within the tracing module. All code outside the
 *  trace module should invoke TRACE().
 */

#include "trace/trace_force.h" /* for prototypes */

#include "k_defines.h"

/* internal constants */

#define FORCE_BUFFER_SIZE 256



/* definitions of exported functions */


void trace_force_(const char* filename, int line_num, const char* function_name,
		  char* format, ...)
{

  int function_name_len = strlen(function_name);
  int size = FORCE_BUFFER_SIZE + function_name_len;
  char buf[size];

  
  snprintf(buf, size, "\nFORCE: %s:%d %s: ", filename, line_num, function_name);
  int   msg_offset = strlen(buf);
  int   msg_size   = size - msg_offset;
  char* msg        = &buf[ msg_offset ];
  

  va_list ap;
  va_start(ap, format);
  vsnprintf(msg, msg_size, format, ap);
  fprintf(stderr, buf);
  va_end(ap);
}
