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

#include "qpipe/stages/func_call.h"



const c_str func_call_packet_t::PACKET_TYPE = "FUNC_CALL";



const c_str func_call_stage_t::DEFAULT_STAGE_NAME = "FUNC_CALL_STAGE";



/**
 *  @brief Invoke a packet's specified function with the packet's
 *  tuple buffer.
 */
void func_call_stage_t::process_packet() {
    adaptor_t* adaptor = _adaptor;
    func_call_packet_t* packet = (func_call_packet_t*)adaptor->get_packet();
    packet->_func(adaptor, packet->_func_arg);
}
