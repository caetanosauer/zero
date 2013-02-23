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

#ifndef __QPIPE_DELAY_WRITER_H
#define __QPIPE_DELAY_WRITER_H

#include "qpipe/core.h"


ENTER_NAMESPACE(qpipe);


/**
 *  @brief Packet definition for the DELAY_WRITER stage.
 */

struct delay_writer_packet_t : public packet_t {

public:

    static const c_str PACKET_TYPE;


    const size_t _output_tuple_size;
    const int _delay_us;
    const int _num_tuples;

   
    /**
     *  @brief delay_writer_packet_t constructor.
     */
    delay_writer_packet_t(const c_str&    packet_id,
                          tuple_fifo*     output_buffer,
                          tuple_filter_t* output_filter,
                          int             delay_us,
                          int             num_tuples)
	: packet_t(packet_id, PACKET_TYPE, output_buffer, output_filter,
                   NULL,
                   false, /* merging not allowed */
                   true   /* unreserve worker on completion */
                   ),
	  _output_tuple_size(output_buffer->tuple_size()),
          _delay_us(delay_us),
          _num_tuples(num_tuples)
    {
    }

    virtual void declare_worker_needs(resource_declare_t* declare) {
        declare->declare(_packet_type, 1);
        /* no inputs */
    }
};



/**
 *  @brief Table scan stage that reads tuples from the storage manager.
 */

class delay_writer_stage_t : public stage_t {

public:

    typedef delay_writer_packet_t stage_packet_t;
    static const c_str DEFAULT_STAGE_NAME;

    static const size_t DELAY_WRITER_BULK_READ_BUFFER_SIZE;

    delay_writer_stage_t() { }

    virtual ~delay_writer_stage_t() { }

protected:

    virtual void process_packet();
};


EXIT_NAMESPACE(qpipe);


#endif
