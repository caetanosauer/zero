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

#include "qpipe/stages/bnl_join.h"

#include <cstring>



const c_str bnl_join_packet_t::PACKET_TYPE = "BNL_JOIN";



const c_str bnl_join_stage_t::DEFAULT_STAGE_NAME = "BNL_JOIN_STAGE";



/**
 *  @brief Join the left and right relations using a nested
 *  loop. Instead of reading the inner relation for every outer
 *  relation tuple, we do page-sized reads.
 */

void bnl_join_stage_t::process_packet() {

    adaptor_t* adaptor = _adaptor;
    bnl_join_packet_t* packet = (bnl_join_packet_t*)adaptor->get_packet();
    
    tuple_fifo* left_buffer  = packet->_left_buffer;
    tuple_source_t* right_source = packet->_right_source;
    tuple_join_t*   join = packet->_join;
    
    
    // dispatch outer relation so we can stream it
    TRACE(TRACE_ALWAYS,
          "Dispatching _left... Is declare_worker_needs() implemented?\n");
    dispatcher_t::dispatch_packet(packet->_left);


    // get ready...
    size_t key_size = join->key_size();
    array_guard_t<char> output_data = new char[join->output_tuple_size()];
    tuple_t output_tuple(output_data, sizeof(output_data));
    
    
    guard<qpipe::page> outer_tuple_page = qpipe::page::alloc(left_buffer->tuple_size());
    guard<qpipe::page> inner_tuple_page = qpipe::page::alloc(right_source->tuple_size());

    while (1) {
        
        
        // get another page of tuples from the outer relation
        if (!left_buffer->copy_page(outer_tuple_page))
            // done with outer relation... done with join
            return;
    

        // re-read the inner relation
        packet_t* right_packet = right_source->reset();
        tuple_fifo* right_buffer = right_packet->output_buffer();
        TRACE(TRACE_ALWAYS,
              "Dispatching _right_packet... Is declare_worker_needs() implemented?\n");
        dispatcher_t::dispatch_packet(right_packet);
        
        
        while (1) {
            
            
            // get another page of tuples from the inner relation
            if (!right_buffer->copy_page(inner_tuple_page))
                // done with inner relation... continue to next page
                // of outer relation
                break;
            
            
            // join each tuple on the outer relation page with each
            // tuple on the inner relation page
            qpipe::page::iterator o_it = outer_tuple_page->begin();
            while(o_it != outer_tuple_page->end()) {

                tuple_t outer_tuple = o_it.advance();
                const char* outer_key = join->left_key(outer_tuple);


                qpipe::page::iterator i_it = inner_tuple_page->begin();
                while(i_it != inner_tuple_page->end()) {


                    // check for equality by extracting keys and using
                    // memcmp() to check for byte-by-byte equality
                    tuple_t inner_tuple = i_it.advance();
                    const char* inner_key = join->right_key(inner_tuple);
                    if ( memcmp( outer_key, inner_key, key_size ) == 0 ) {
                        // keys match!
                        join->join(output_tuple, outer_tuple, inner_tuple);
                        _adaptor->output(output_tuple);
                    }

                    
                } // endof loop over inner page
            } // endof loop over outer page
            
            
        } // endof loop over inner relation
    } // endof loop over outer relation
    
}
