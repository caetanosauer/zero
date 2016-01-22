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

/** @file trace_stream.h
 *
 *  @brief Exports trace_stream().
 *
 *  @bug See trace_stream.cpp.
 */
#ifndef _TRACE_STREAM_H
#define _TRACE_STREAM_H

#include <cstdio>   /* for FILE* */
#include <cstdarg>  /* for va_list datatype */



/* exported functions */

void trace_stream(FILE* out_stream,
		  const char* filename, int line_num, const char* function_name,
		  char* format, va_list ap);



#endif
