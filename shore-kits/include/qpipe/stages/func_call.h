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

#ifndef __QPIPE_FUNC_CALL_H
#define __QPIPE_FUNC_CALL_H

#include <cstdio>

#include "qpipe/core.h"



using namespace qpipe;



/* exported datatypes */


class func_call_packet_t : public packet_t {
  
public:

    static const c_str PACKET_TYPE;
    
    
    void (*_func) (void*, void*);
    
    
    void* _func_arg;
    
    
    void (*_destructor) (void*);

    
    /**
     *  @brief func_call_packet_t constructor.
     *
     *  @param packet_id The ID of this packet. This should point to a
     *  block of bytes allocated with malloc(). This packet will take
     *  ownership of this block and invoke free() when it is
     *  destroyed.
     *
     *  @param output_buffer This packet's output buffer. This buffer
     *  is not used directly. The container will send_eof() and close
     *  this buffer when process_packet() returns. However, this will
     *  not be done until func is called with func_arg. So a test
     *  program may pass in a tuple_fifo for both output_buffer
     *  and func_arg. A packet DOES NOT own its output buffer (we will
     *  not invoke delete or free() on this field in our packet
     *  destructor).
     *
     *  @param output_filter This filter is not used, but it cannot be
     *  NULL. Pass in a tuple_filter_t instance. The packet OWNS this
     *  filter. It will be deleted in the packet destructor.
     *
     *  @param func The function that should be called with func_arg.
     *
     *  @param func_arg func will be called with this value. This
     *  packet does not own this value.
     */
    func_call_packet_t(const c_str    &packet_id,
                       tuple_fifo* output_buffer,
                       tuple_filter_t* output_filter,
                       void (*func) (void*, void*),
                       void* func_arg,
                       void (*destructor) (void*) = NULL,
                       bool _merge=false,
                       bool _unreserve=true)
        : packet_t(packet_id, PACKET_TYPE, output_buffer, output_filter,
                   _merge ? create_plan(output_filter, func) : NULL,
                   _merge,    /* whether to merge */
                   _unreserve /* whether to unreserve worker on completion */
                   ),
          _func(func),
          _func_arg(func_arg),
          _destructor(destructor)
    {
        // error checking
        assert(func != NULL);
        if (_merge && !_unreserve) {
            TRACE(TRACE_ALWAYS,
                  "WARNING: Won't release worker on completion, but work sharing possible\n"
                  "         What do you want the container to do when it merges?\n");
        }
    }

    
    virtual ~func_call_packet_t() {
        // if a destructor was specified, apply it to _func_arg field
        if ( _destructor != NULL )
            _destructor(_func_arg);
    }

    static query_plan* create_plan(tuple_filter_t* filter, void (*func) (void*, void*)) {
        c_str action("%p", func);
        return new query_plan(action, filter->to_string(), NULL, 0);
    }
    
    virtual void declare_worker_needs(resource_declare_t*) {
        /* Do nothing. The stage the that creates us is responsible
           for deciding how many FUNC_CALL workers it needs. */
    }
};



/**
 *  @brief FUNC_CALL stage. Simply invokes a packet's specified
 *  function with the packet's tuple buffer. Useful for unit testing.
 */
class func_call_stage_t : public stage_t {

public:
  
    static const c_str DEFAULT_STAGE_NAME;
    typedef func_call_packet_t stage_packet_t;
    
    func_call_stage_t() { }
    
    virtual ~func_call_stage_t() { }
    
protected:

    virtual void process_packet();
};



#endif
