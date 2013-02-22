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

#include "qpipe/stages/aggregate.h"

ENTER_NAMESPACE(qpipe);


const c_str aggregate_packet_t::PACKET_TYPE = "AGGREGATE";

const c_str aggregate_stage_t::DEFAULT_STAGE_NAME = "AGGREGATE_STAGE";


void aggregate_stage_t::process_packet() 
{
    adaptor_t* adaptor = _adaptor;
    aggregate_packet_t* packet = (aggregate_packet_t*)adaptor->get_packet();

    
    tuple_aggregate_t* aggregate = packet->_aggregator;
    key_extractor_t* extract = packet->_extract;
    tuple_fifo* input_buffer = packet->_input_buffer;
    dispatcher_t::dispatch_packet(packet->_input);


    // "I" own dest, so allocate space for it on the stack
    size_t dest_size = packet->output_buffer()->tuple_size();
    array_guard_t<char> dest_data = new char[dest_size];
    tuple_t dest(dest_data, dest_size);


    size_t agg_size = aggregate->tuple_size();
    array_guard_t<char> agg_data = new char[agg_size];
    tuple_t agg(agg_data, agg_size);
    
    size_t key_size = extract->key_size();
    char* last_key = aggregate->key_extractor()->extract_key(agg_data);

    int i = 0;
    bool first = true;
    while (1) {

        // No more tuples?
        tuple_t src;
        if(!input_buffer->get_tuple(src))
            // Exit from loop, but can't return quite yet since we may
            // still have one more aggregation to perform.
            break;
            
        // got another tuple
        const char* key = extract->extract_key(src);

        // break group?
        if(first || /* allow init() call if first tuple */
           (key_size && memcmp(last_key, key, key_size))) {

            if(!first) {
                aggregate->finish(dest, agg.data);
                TRACE(0&TRACE_ALWAYS, "key_size = %d\n", key_size);
                adaptor->output(dest);
            }
 
            aggregate->init(agg.data);
            memcpy(last_key, key, key_size);
            first = false;
        }
        
        aggregate->aggregate(agg.data, src);
        i++;
    }

    // output the last group, if any
    if(!first) {
        aggregate->finish(dest, agg.data);
        adaptor->output(dest);
    }
}


EXIT_NAMESPACE(qpipe);

