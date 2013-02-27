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

#include "qpipe/stages/echo.h"



const c_str echo_packet_t::PACKET_TYPE = "ECHO";



const c_str echo_stage_t::DEFAULT_STAGE_NAME = "ECHO_STAGE";



void echo_stage_t::process_packet() {


    adaptor_t* adaptor = _adaptor;
    echo_packet_t* packet = (echo_packet_t*)adaptor->get_packet();
    
    
    tuple_fifo* input_buffer = packet->_input_buffer;
    dispatcher_t::dispatch_packet(packet->_input);

    
    guard<qpipe::page> next_page = qpipe::page::alloc(input_buffer->tuple_size());
    while (1) {
        if (!input_buffer->copy_page(next_page))
            break;
        adaptor->output(next_page);
    }
}
