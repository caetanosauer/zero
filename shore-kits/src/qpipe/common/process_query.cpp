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

#include "qpipe/core/tuple_fifo.h"
#include "qpipe/core/dispatcher.h"
#include "qpipe/common/process_query.h"
#include "util.h"

ENTER_NAMESPACE(qpipe);


void process_query(packet_t* root, process_tuple_t& pt)
{
    guard<tuple_fifo> out = root->output_buffer();
    
    dispatcher_t::worker_reserver_t* wr = dispatcher_t::reserver_acquire();

    /* reserve worker threads and dispatch... */
    root->declare_worker_needs(wr);
    wr->acquire_resources();
    dispatcher_t::dispatch_packet(root);
    
    /* process query results */
    tuple_t output;
    pt.begin();
    while(out->get_tuple(output))
        pt.process(output);
    pt.end();

    dispatcher_t::reserver_release(wr);
}


EXIT_NAMESPACE(qpipe);
