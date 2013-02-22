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

#include "qpipe/stages/fdump.h"


const c_str fdump_packet_t::PACKET_TYPE = "FDUMP";

const c_str fdump_stage_t::DEFAULT_STAGE_NAME = "FDUMP_STAGE";

/**
 *  @brief Write the file specified by fdump_packet_t p.
 *
 *  @return 0 on success. Non-zero on unrecoverable error. The stage
 *  should terminate all queries it is processing.
 */

void fdump_stage_t::process_packet() {

    adaptor_t* adaptor = _adaptor;
    fdump_packet_t* packet = (fdump_packet_t*) adaptor->get_packet();

    if (packet->_child) {
        dispatcher_t::dispatch_packet(packet->_child);
    }
    
    const c_str &filename = packet->_filename;
    // make sure the file gets closed when we're done
    guard<FILE> file = fopen(filename.data(), "w+");
    if (file == NULL)
        THROW3(FileException,
            "Caught %s opening '%s'",
            errno_to_str().data(), filename.data());


    tuple_fifo* input_buffer = packet->_input_buffer;

    if (!(packet->_tuple_to_c_str)) {
        // Usual function, write pages

        guard<qpipe::page> next_page = qpipe::page::alloc(input_buffer->tuple_size());
        while (1) {

            if (!input_buffer->copy_page(next_page)) {
                TRACE(TRACE_DEBUG, "Finished dump to file %s\n", filename.data());
                break;
            }

            TRACE(TRACE_ALWAYS, "Wrote page\n");
            adaptor->output(next_page); // small change, to make it work like in a chain of packets (herc)
            next_page->fwrite_full_page(file);
        }
    } else {
        // For each tuple, write the c_str returned by
        // the function pointer.
        if (!input_buffer->ensure_read_ready()) {
            // No tuples
            return;
        }

        tuple_t tup(NULL, input_buffer->tuple_size());
        
        while (1) {
            
            // Are there tuples?
            if (!input_buffer->get_tuple(tup))
                break;
            
            const c_str* str = packet->_tuple_to_c_str(&tup);
            fwrite(str->data(), sizeof(char), strlen(str->data()), file);
            delete str;
            
            adaptor->output(tup);
            
        }
    }
}
