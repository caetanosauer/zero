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

#include "qpipe/stages/delay_writer.h"
#include "util.h"
#include <unistd.h>


ENTER_NAMESPACE(qpipe);

const c_str delay_writer_packet_t::PACKET_TYPE = "DELAY_WRITER";

const c_str delay_writer_stage_t::DEFAULT_STAGE_NAME = "DELAY_WRITER_STAGE";

/**
 *  @brief Write a fixed number of tuples at a specified rate.
 */
void delay_writer_stage_t::process_packet() {

    adaptor_t* adaptor = _adaptor;
    delay_writer_packet_t* packet = (delay_writer_packet_t*)adaptor->get_packet();
    
    /* create blank tuple */
    size_t tuple_size = packet->_output_tuple_size;
    array_guard_t<char> tuple_data = new char[tuple_size];
    memset(tuple_data, 0, sizeof(tuple_data));
    tuple_t tuple(tuple_data, tuple_size);
    

    int delay_us = packet->_delay_us;
    int num_tuples = packet->_num_tuples;
    for (int i = 0; i < num_tuples; i++) {
        busy_delay_us(delay_us);
        adaptor->output(tuple);
    }
}


EXIT_NAMESPACE(qpipe);
