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

/** @file:   process_tuple.h
 *
 *  @brief:  Interface for the final step of each query
 *
 *  @author: Ippokratis Pandis
 */


#ifndef __QPIPE_PROCESS_TUPLE_H
#define __QPIPE_PROCESS_TUPLE_H

#include "qpipe/core/tuple.h"
#include "qpipe/core/packet.h"


ENTER_NAMESPACE(qpipe);


class process_tuple_t {
public:
    virtual void begin() { }
    virtual void process(const tuple_t&)=0;
    virtual void end() { }
    virtual ~process_tuple_t() { }
};


EXIT_NAMESPACE(qpipe);

#endif
