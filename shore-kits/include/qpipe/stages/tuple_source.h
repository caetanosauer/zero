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

#ifndef __QPIPE_TUPLE_SOURCE_H
#define __QPIPE_TUPLE_SOURCE_H

#include "qpipe/core.h"
#include <cstdlib>


using namespace qpipe;

class tuple_source_t {

public:
  
    /**
     *  @brief Produce another packet that we can dispatch to receive
     *  another stream of tuples. Since we only consider left-deep query
     *  plans, it is useful to specify the right/inner relation of join
     *  as a tuple_source_t.
     */
    virtual packet_t* reset()=0;

    virtual size_t tuple_size()=0;

    virtual ~tuple_source_t() { }
};



#endif
