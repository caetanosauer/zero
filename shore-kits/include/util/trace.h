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

/** @file trace.h
 *
 *  @brief Tracing module.
 *
 *  @note See trace.cpp.
 */

#ifndef __UTIL_TRACE_H
#define __UTIL_TRACE_H

#include <cstdarg>             /* for varargs */
#include <stdint.h>            /* for uint32_t */

#include "util/compat.h"
#include "trace/trace_types.h"


/* exported functions */

struct tracer {
    char const* _file;
    int _line;
    char const* _function;
    tracer(char const* file, int line, char const* function)
	: _file(file), _line(line), _function(function)
    {
    }
    void operator()(unsigned int type, char const* format, ...) ATTRIBUTE(format(printf, 3, 4));
};

void trace_(unsigned int trace_type,
	    const char* filename, int line_num, const char* function_name,
	    char const* format, ...) ;
void trace_set(unsigned int trace_type_mask);
unsigned int trace_get();



/* exported macros */


/**
 *  @def TRACE
 *
 * @brief Other modules in our program use this macro for
 * reporting. We can use preprocessor macros like __FILE__ and
 * __LINE__ to provide more information in the output messages. We can
 * also remove all messages at compile time by changing the
 * definition.
 *
 * @param type The type of the message. This is really a bit vector of
 * all times when this message should be printed. If the current debug
 * setting contains any one of these bits, we print the message.
 *
 * @param format The format string for the printing. Format follows
 * that of printf(3).
 *
 * @param rest Optional arguments that can printed (see printf(3)
 * definition for more details).
 *
 * @return void
 */
#define TRACE tracer(__FILE__, __LINE__, __FUNCTION__)



/**
 *  @def TRACE_SET
 *
 * @brief Macro wrapper for trace_set()
 * 
 * @param types Passed through as trace_type_mask parameter of
 * trace_set().
 *
 * @return void
 */
#define TRACE_SET(types) trace_set(types)



/**
 *  @def TRACE_GET
 *
 * @brief Macro wrapper for trace_get()
 * 
 * @param types Passed through as trace_type_mask parameter of
 * trace_get().
 *
 * @return void
 */
#define TRACE_GET() trace_get()



#endif // __UTIL_TRACE_H
