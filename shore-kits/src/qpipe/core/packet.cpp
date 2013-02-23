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

#include "qpipe/core/packet.h"
#include "qpipe/core/stage_container.h"
#include "util.h"
#include "util/thread.h"


ENTER_NAMESPACE(qpipe);

// change this variable to set the style of sharing we use...

osp_policy_t osp_global_policy = OSP_FULL;



/**
 *  @brief packet_t constructor.
 *
 *  @param packet_id The ID of this packet. This should be a block of
 *  bytes allocated with malloc(). This packet will take ownership of
 *  this block and invoke free() when it is destroyed.
 *
 *  @param packet_type The type of this packet. A packet DOES NOT own
 *  its packet_type string (we will not invoke delete or free() on
 *  this field in our packet destructor).
 *
 *  @param output_buffer The buffer where this packet should send its
 *  data. A packet DOES NOT own its output buffer (we will not invoke
 *  delete or free() on this field in our packet destructor).
 *
 *  @param output_filter The filter that will be applied to any tuple
 *  sent to _output_buffer. The packet OWNS this filter. It will be
 *  deleted in the packet destructor.
 *
 *  @param merge_enabled Whether this packet can be merged with other
 *  packets. This parameter is passed by value, so there is no
 *  question of ownership.
 */

packet_t::packet_t(const c_str    &packet_id,
		   const c_str    &packet_type,
		   tuple_fifo*     output_buffer,
		   tuple_filter_t* output_filter,
                   query_plan*     plan,
                   bool            merge_enabled,
                   bool            unreserve_on_completion)
    : _plan(plan),
      
      /* Allow packets to be created unbound to any query. */
      _qstate(NULL),

      _merge_enabled(merge_enabled),
      _unreserve_on_completion(unreserve_on_completion),
      _packet_id("%s_%s", thread_get_self()->thread_name().data(), packet_id.data()),
      _packet_type(packet_type),
      _output_buffer(output_buffer),
      _output_filter(output_filter),
      _next_tuple_on_merge(stage_container_t::NEXT_TUPLE_UNINITIALIZED),
      _next_tuple_needed  (stage_container_t::NEXT_TUPLE_INITIAL_VALUE)
{
    // error checking
    assert(output_buffer != NULL);
    assert(output_filter != NULL);
    assert(!merge_enabled || plan != NULL);

    TRACE(TRACE_PACKET_FLOW, "Created %s packet with ID %s\n",
	  _packet_type.data(),
	  _packet_id.data());
}



/**
 *  @brief packet_t destructor.
 */

packet_t::~packet_t(void) {
    
    TRACE(TRACE_PACKET_FLOW, "Destroying %s packet with ID %s\n",
	  _packet_type.data(),
	  _packet_id.data());
}



EXIT_NAMESPACE(qpipe);
