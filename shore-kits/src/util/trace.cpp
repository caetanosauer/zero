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

/** @file trace.cpp
 *
 *  @brief Tracing module. To add a new tracing type, create a
 *  constant for the type in trace_types.h. Then register it in the
 *  switch() statement below. You may also want to add a macro for
 *  that tracing type in trace.h.
 *
 *  @author Naju Mancheril (ngm)
 */

#include "util/trace.h"              /* for prototypes */
#include "util/sync.h"

#include "k_defines.h"

/* internal data structures */
#if 0
static void trace_force_(const char* filename, int line_num, const char* function_name,
		  char* format, ...) __attribute__((format(printf, 4, 5)));

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
#endif


static void trace_print_pthread(FILE* out_stream, pthread_t thread);


static void trace_stream(FILE* out_stream,
		  const char* filename, int line_num, const char* function_name,
		  char const* format, va_list ap);


/**
 *  @brief Bit vector representing the current set of messages to be
 *  printed. Should be set at runtime to turn different message types
 *  on and off. We initialize it here to enable all messages. That
 *  way, any messages we print during client startup will be printed.
 */
static unsigned int trace_current_setting = ~0u;



/* definitions of exported functions */


/**
 *  @brief Convert the specified message into a single string and
 *  process it.
 *
 *  @param trace_type The message type. This determines how we will
 *  process it. For example, TRACE_TYPE_DEBUG could mean that we print
 *  the message to the console.
 *
 *  @param filename The name of the file where the message is coming
 *  from.
 *
 *  @param line_num The line number in the file where the message is
 *  coming from.
 *
 *  @param function_name The name of the function where the message is
 *  coming from.
 *
 *  @param format The format string for this message. printf()
 *  syntax.
 *
 *  @param ... Optional arguments referenced by the format string.
 */
void tracer::operator()(unsigned int trace_type, char const* format, ...)
{

    /* Print if any trace_type bits match bits in the current trace
       setting. */
    unsigned int do_trace = trace_current_setting & trace_type;
    if ( do_trace == 0 )
        return;

    
    va_list ap;
    va_start(ap, format);
  
    /* currently, we only support printing to streams */
    trace_stream(stdout, _file, _line, _function, format, ap);

    va_end(ap);
    return;
}



/**
 *  @brief Specify the set of trace types that are currently enabled.
 *
 *  @param trace_type_mask Bitmask used to specify the current set of
 *  trace types.
 */
void trace_set(unsigned int trace_type_mask) {

    /* avoid unnecessary synchronization here */
    /* should really only be called once at the beginning of the
       program */
    trace_current_setting = trace_type_mask;
}



/**
 *  @brief Get the set of trace types that are currently enabled.
 */
unsigned int trace_get() {
    return trace_current_setting;
}



/* internal constants */

static const int FORCE_BUFFER_SIZE = 256;



/* definitions of exported functions */

#if 0
static void trace_force_(const char* filename, int line_num, const char* function_name,
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
#endif

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

static void trace_print_pthread(FILE* out_stream, pthread_t thread)
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




/* internal data structures */

static pthread_mutex_t stream_mutex = thread_mutex_create();



/* definitions of exported functions */


/**
 *  @brief Prints the entire trace message to the specified stream.
 *
 *  @param out_stream The stream to print to. We flush after the
 *  print. We also make one call to fprintf(), so the message should
 *  be printed atomically.
 *
 *  @param filename The filename that the message is coming from. Used
 *  in message prefix.
 *
 *  @param line_num The line number that the message is coming
 *  from. Used in message prefix.
 *
 *  @param function_name The name of the function where the message is
 *  coming from. Used in message prefix.
 *
 *  @param format The format of the output message (not counting the
 *  prefix).
 *
 *  @param ap The parameters for format.
 *
 *  @return void
 */

static void trace_stream(FILE* out_stream,
		  const char* filename, int line_num, const char* function_name,
		  char const* format, va_list ap)
{

    /* Any message we print should be prefixed by:
       .
       "<thread ID>: <filename>:<line num>:<function name>: "

       Even though individual fprintf() calls can be expected to execute
       atomically, there is no such guarantee across multiple calls. We
       use a mutex to synchronize access to the console.
    */
    thread_t* this_thread = thread_get_self();
    critical_section_t cs(stream_mutex);
  

    /* Try to print a meaningful (string) thread ID. If no ID is
       registered, just use pthread_t returned by pthread_self(). */
    if ( this_thread != NULL )
        fprintf(out_stream, "%s", this_thread->thread_name().data());
    else
        trace_print_pthread(out_stream, pthread_self());


    fprintf(out_stream, ": %s:%d:%s: ", filename, line_num, function_name);
    vfprintf(out_stream, format, ap);

    /* No need to flush in a critical section. Worst-case, someone else
       prints between out print and our flush. Since fflush() is atomic
       with respect to fprintf(), we end up simply flushing someone
       else's data along with our own. */
    fflush(out_stream);
}
