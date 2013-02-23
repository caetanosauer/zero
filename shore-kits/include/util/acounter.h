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

/** @file acounter.h
 *
 *  @brief An acounter_t is a counter with an atomic fetch and
 *  increment operation. This implementation is thread safe.
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @bug None known.
 */
#ifndef _ACOUNTER_H
#define _ACOUNTER_H

#include "util/thread.h"
#include "util/sync.h"



/* exported datatypes */

class acounter_t {

 private:

    pthread_mutex_t _mutex;
    int _value;
  
 public:

    acounter_t(int value=0)
        : _mutex(thread_mutex_create()),
          _value(value)
    {
    }

    ~acounter_t() {
        thread_mutex_destroy(_mutex);
    }

    int fetch_and_inc() {
        critical_section_t cs(_mutex);
        return _value++;
    }
        
};



#endif
