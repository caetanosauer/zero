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

#ifndef __QPIPE_SIEVE_H
#define __QPIPE_SIEVE_H

#include <cstdio>
#include "util.h"
#include "qpipe/core.h"

using namespace qpipe;



struct sieve_packet_t : public packet_t {

    static const c_str PACKET_TYPE;
    
    guard<tuple_sieve_t> _sieve;
    guard<packet_t>      _input;
    guard<tuple_fifo>    _input_buffer;
    
    sieve_packet_t(const c_str       &packet_id,
                   tuple_fifo*        output_buffer,
                   tuple_filter_t*    output_filter,
                   tuple_sieve_t*     sieve,
                   packet_t*          input)
	: packet_t(packet_id, PACKET_TYPE, output_buffer, output_filter,
                   create_plan(output_filter, sieve, input->plan()),
                   true, /* merging allowed */
                   true  /* unreserve worker on completion */
                   ),
	  _sieve(sieve),
          _input(input),
          _input_buffer(input->output_buffer())
    {
        assert(_input != NULL);
        assert(_input_buffer != NULL);
    }

    static query_plan* create_plan(tuple_filter_t* filter, tuple_sieve_t* sieve,
                                   query_plan const* child)
    {
        c_str action("%s:%s", PACKET_TYPE.data(), sieve->to_string().data());
        query_plan const** children = new query_plan const*[1];
        children[0] = child;
        return new query_plan(action, filter->to_string(), children, 1);
    }

    virtual void declare_worker_needs(resource_declare_t* declare) {
        declare->declare(_packet_type, 1);
        _input->declare_worker_needs(declare);
    }
};



class sieve_stage_t : public stage_t {

public:

    static const c_str DEFAULT_STAGE_NAME;
    typedef sieve_packet_t stage_packet_t;
    
    sieve_stage_t() { }
    ~sieve_stage_t() { }

 protected:
    
    virtual void process_packet();
};



#endif
