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

/** @file alignedops.cpp
 *
 *  @brief Implements aligned memory operations.
 *
 *  @author Naju Mancheril (ngm)
 */
#include "util/alignedops.h" /* for prototypes */
#include "util/guard.h"        /* for guard*/
#include "util/trace.h"        /* for TRACE */
#include <cstring>
#include <cassert>



/* debugging */

static const int debug_trace_type = TRACE_DEBUG;
#define DEBUG_TRACE(format, arg) TRACE(debug_trace_type, format, arg)



/* definitions of exported functions */

/**
 *  @brief Returned an aligned copy of the data.
 *
 *  @return NULL on error.
 */
void* aligned_alloc(size_t min_buf_size, size_t align_size,
                    void** aligned_base) {
    
    assert(min_buf_size > 0);
    assert(align_size > 0);

    /* allocate space */
    size_t alloc_size = min_buf_size + align_size - 1;
    guard<char> big_buf = new char[alloc_size];
    if (big_buf == NULL)
        return NULL;

    /* locate aligned location */
    size_t aligned_base_addr;
    for (aligned_base_addr = (size_t)(void*)big_buf;
         (aligned_base_addr % align_size) != 0; aligned_base_addr++);
    
    void* dst = (void*)aligned_base_addr;
    assert(aligned_base_addr < ((size_t)(void*)big_buf + alloc_size));
    *aligned_base = dst;
    
    return big_buf.release();
}
