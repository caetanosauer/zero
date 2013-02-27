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

/** @file trace_types.h
 *
 *  @brief Lists all TRACE types. Each of these should be a bit
 *  specified in a bit vector. We current support up to 32 tracing
 *  types.
 */

#ifndef __TRACE_TYPES_H
#define __TRACE_TYPES_H



/* exported constants */

#define TRACE_COMPONENT_MASK_ALL       (~0)
#define TRACE_COMPONENT_MASK_NONE        0

#define TRACE_ALWAYS               (unsigned int)(1 << 0)
#define TRACE_TUPLE_FLOW           (unsigned int)(1 << 1)
#define TRACE_PACKET_FLOW          (unsigned int)(1 << 2)
#define TRACE_SYNC_COND            (unsigned int)(1 << 3)
#define TRACE_SYNC_LOCK            (unsigned int)(1 << 4)
#define TRACE_THREAD_LIFE_CYCLE    (unsigned int)(1 << 5)
#define TRACE_TEMP_FILE            (unsigned int)(1 << 6)
#define TRACE_CPU_BINDING          (unsigned int)(1 << 7)
#define TRACE_QUERY_RESULTS        (unsigned int)(1 << 8)
#define TRACE_QUERY_PROGRESS       (unsigned int)(1 << 9)
#define TRACE_STATISTICS           (unsigned int)(1 << 10)
#define TRACE_NETWORK              (unsigned int)(1 << 11)
#define TRACE_RESPONSE_TIME        (unsigned int)(1 << 12)
#define TRACE_WORK_SHARING         (unsigned int)(1 << 13)
#define TRACE_TRX_FLOW             (unsigned int)(1 << 14)
#define TRACE_KEY_COMP             (unsigned int)(1 << 15)
#define TRACE_RECORD_FLOW          (unsigned int)(1 << 16)
#define TRACE_DEBUG                (unsigned int)(1 << 31)



#endif
