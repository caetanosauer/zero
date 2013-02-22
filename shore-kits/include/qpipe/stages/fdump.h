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

#ifndef __QPIPE_FDUMP_H
#define __QPIPE_FDUMP_H

#include <cstdio>

#include "qpipe/core.h"


using namespace qpipe;



/* exported functions */


/**
 *  @brief
 */
struct fdump_packet_t : public packet_t {

public:
    
    static const c_str PACKET_TYPE;

    guard<tuple_fifo> _input_buffer;
    
    c_str _filename;

    notify_t* _notifier;

    const c_str* (*_tuple_to_c_str)(tuple_t*);
    
    guard<packet_t> _child;

    /**
     * 
     *  @brief fdump_packet_t constructor.
     * 
     *  The FDUMP packet's purpose is to monitor an input tuple_fifo buffer
     *  and output its pages to a specified file. It also echoes the input
     *  pages to its output buffer, so that it can be used as an intermediate
     *  packet in a plan.
     * 
     *  @param packet_id The ID of this packet. This should point to a
     *  block of bytes allocated with malloc(). This packet will take
     *  ownership of this block and invoke free() when it is
     *  destroyed.
     *
     *  @param output_buffer The buffer where this packet should send
     *  its data. A packet DOES NOT own its output buffer (we will not
     *  invoke delete or free() on this field in our packet
     *  destructor).
     *
     *  @param output_filter The filter that will be applied to any
     *  tuple sent to output_buffer. The packet OWNS this filter. It
     *  will be deleted in the packet destructor.
     *
     *  FDUMP doesn't really need a tuple filter. All filtering for
     *  the query can be moved to the stage feeding the FDUMP. The
     *  caller should be able to get away with creating an instance of
     *  the tuple_filter_t base class.
     * 
     *  @param input_buffer The input buffer from where FDUMP will read
     *  pages to write to a file. If a child packet is specified, you
     *  can specify this as NULL, since
     *  this will automatically be directed to the child packet's
     *  output buffer.
     *
     *  @param filename The name of the file to scan. This should be a
     *  block of bytes allocated with malloc(). This packet will take
     *  ownership of this block and invoke free() when it is
     *  destroyed.
     *
     *  @param notifier An optional notify_t instance that the worker
     *  thread will invoke notify() on when the FDUMP operation is
     *  complete. Pass NULL to avoid this behavior. Even if a non-NULL
     *  value is passed, this packet WILL NOT take ownership of this
     *  object. We will however, invoke notify() on this parameter in
     *  our destructor.
     * 
     *  @param child The child packet of this FDUMP, which serves as
     *  the input. The packet's output buffer is appointed as the input
     *  buffer of FDUMP.
     * 
     *  @param tuple_to_c_str If you do not specify a function pointer that
     *  converts an input tuple to a string, then the usual page's
     *  fwrite_full_page() is used.
     * 
     */    
    fdump_packet_t(const c_str    &packet_id,
		   tuple_fifo*     output_buffer,
                   tuple_filter_t* output_filter,
		   tuple_fifo*     input_buffer,
		   const c_str    &filename,
                   notify_t*       notifier=NULL,
                   packet_t*       child=NULL,
                   const c_str* (*tuple_to_c_str)(tuple_t*)=NULL)
	: packet_t(packet_id, PACKET_TYPE, output_buffer, output_filter,
                   create_plan(output_filter, child),
                   false, /* merging not allowed */
                   false  /* keep worker on completion */
                   ),
	  _input_buffer(input_buffer),
          _filename(filename),
          _notifier(notifier),
          _child(child),
          _tuple_to_c_str(tuple_to_c_str)
    {
        if (_child) {
            _input_buffer = _child.get()->output_buffer();
        }
    }

    static query_plan * create_plan(tuple_filter_t* filter, packet_t* child) {
        c_str action("%s:%s", PACKET_TYPE.data(), child->_packet_type.data());
        if (child) {
            query_plan const** children = new query_plan const*[1];
            children[0] = child->plan();
            return new query_plan(action, filter->to_string(), children, 1);
        } else {
            return NULL;
        }
    }

    virtual void declare_worker_needs(resource_declare_t * declare) {
        if (_child) {
            declare->declare(_packet_type, 1);
            _child->declare_worker_needs(declare);
        } else {
            /* Do nothing. The stage the that creates us is responsible
            for deciding how many FDUMP workers it needs. */
            ;
        }
    }

};

/**
 *  @brief FDUMP stage. Creates a (hopefully) temporary file on the
 *  local file system.
 */
class fdump_stage_t : public stage_t {

public:

    static const c_str DEFAULT_STAGE_NAME;
    typedef fdump_packet_t stage_packet_t;

    fdump_stage_t() { }

    virtual ~fdump_stage_t() { }

protected:

    virtual void process_packet();
};



#endif
