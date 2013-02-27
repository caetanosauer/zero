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

#ifndef __QPIPE_BNL_JOIN_H
#define __QPIPE_BNL_JOIN_H

#include <cstdio>
#include <string>

#include "qpipe/core.h"
#include "qpipe/stages/tuple_source.h"



using namespace std;
using namespace qpipe;



/* exported datatypes */


/**
 *  @brief
 */
struct bnl_join_packet_t : public packet_t {
  
    static const c_str PACKET_TYPE;

    guard<packet_t>       _left;
    guard<tuple_fifo>     _left_buffer;
    guard<tuple_source_t> _right_source;
    guard<tuple_join_t>   _join;
    
    
    /**
     *  @brief bnl_join_packet_t constructor.
     *
     *  @param packet_id The ID of this packet. This should point to a
     *  block of bytes allocated with malloc(). This packet will take
     *  ownership of this block and invoke free() when it is
     *  destroyed.
     *
     *  @param output_buffer The buffer where this packet should send
     *  its data. A packet DOES NOT own its output buffer (we will not
     *  invoke delete or free() on this field in our packet
     *  destructor).
     *
     *  @param output_filter The filter that will be applied to any
     *  tuple sent to output_buffer. The packet OWNS this filter. It
     *  will be deleted in the packet destructor.
     */
    bnl_join_packet_t(const c_str    &packet_id,
                      tuple_fifo* output_buffer,
                      tuple_filter_t* output_filter,
                      packet_t* left,
                      tuple_source_t* right_source,
                      tuple_join_t* join)
        : packet_t(packet_id, PACKET_TYPE, output_buffer, output_filter, NULL,
                   false, /* merging not allowed */
                   true   /* unreserve worker on completion */
                   ),
          _left(left),
          _left_buffer(left->output_buffer()),
          _right_source(right_source),
          _join(join)
    {
    }
    
    virtual void declare_worker_needs(resource_declare_t*) {
        TRACE(TRACE_ALWAYS, "UNIMPLEMENTED! Why are you not using hash join?\n");
        assert(0);
    }
};



/**
 *  @brief Block-nested loop join stage.
 */
class bnl_join_stage_t : public stage_t {

public:

    static const c_str DEFAULT_STAGE_NAME;
    typedef bnl_join_packet_t stage_packet_t;
    
    bnl_join_stage_t() { }
    
    virtual ~bnl_join_stage_t() { }
    
protected:

    virtual void process_packet();
};



#endif
