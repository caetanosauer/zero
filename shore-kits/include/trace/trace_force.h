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

/** @file trace_force.h
 *
 *  @brief Exports TRACE_FORCE(). TRACE_FORCE() should only be invoked
 *  by code within the tracing module. All code outside the trace
 *  module should invoke TRACE().
 *
 *  @bug See trace_force.cpp.
 */
#ifndef _TRACE_FORCE_H
#define _TRACE_FORCE_H

#include <cstdarg> /* for varargs */



/* exported functions */

void trace_force_(const char* filename, int line_num, const char* function_name,
		  char* format, ...) __attribute__((format(printf, 4, 5)));;



/* exported macros */


/**
 *  @def TRACE_FORCE
 *
 * @brief Used by TRACE() to report an error.
 *
 * @param format The format string for the printing. Format follows
 * that of printf(3).
 *
 * @param rest Optional arguments that can printed (see printf(3)
 * definition for more details).
 *
 * @return void
 */

#define TRACE_FORCE(format, rest...) trace_force_(__FILE__, __LINE__, __FUNCTION__, format, ##rest)



#endif // _TRACE_FORCE_H
