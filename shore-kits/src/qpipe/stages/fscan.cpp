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

#include "qpipe/stages/fscan.h"



const c_str fscan_packet_t::PACKET_TYPE = "FSCAN";



const c_str fscan_stage_t::DEFAULT_STAGE_NAME = "FSCAN_STAGE";

/**
 *  @brief Read the file specified by fscan_packet_t p.
 *
 *  @return void
 *
 *  @throw May throw exceptions on error.
 */

void fscan_stage_t::process_packet() {

    adaptor_t* adaptor = _adaptor;
    fscan_packet_t* packet = (fscan_packet_t*)adaptor->get_packet();


    const c_str &filename = packet->_filename;
    guard<FILE> file = fopen(filename.data(), "r");
    if (file == NULL)
        THROW3(FileException,
                        "Caught %s opening '%s'",
                        errno_to_str().data(), filename.data());

        
    guard<qpipe::page> tuple_page =
        qpipe::page::alloc(packet->output_buffer()->tuple_size());
    

    // If we have FSCAN working sharing enabled, we could be accepting
    // packets now. We would like to avoid doing so when we start
    // sending tuples to output().
    bool accepting_packets = true;

    while (1)
    {
	// read the next page of tuples
	if(!tuple_page->fread_full_page(file))
            return;
        
	// We must stop accepting packets as soon as we output() any
	// tuples. Any packet accepted after this point will miss some
	// of the data we are reading.
	if (accepting_packets) {
	    adaptor->stop_accepting_packets();
	    accepting_packets = false;
	}

	// output() each tuple in the page
        qpipe::page::iterator it = tuple_page->begin();
        while(it != tuple_page->end())
	    adaptor->output(*it++);

	// continue to next page
    }
}
