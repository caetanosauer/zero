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

#ifndef __QPIPE_STAGE_H
#define __QPIPE_STAGE_H

#include "qpipe/core/tuple.h"
#include "qpipe/core/packet.h"
#include "util.h"


ENTER_NAMESPACE(qpipe);

/* exported datatypes */


/**
 *  @brief A QPIPE stage is a queue of packets (work that must be
 *  completed) and a process_next_packet() function that worker
 *  threads can call to process the packets.
 */
class stage_t {


public:

    /**
     *  @brief The purpose of a stage::adaptor_t is to provide a
     *  stage's process packet method with exactly the functionality
     *  it needs to read from the primary packet and send results out
     *  to every packet in the stage.
     */
    struct adaptor_t {

    private:

        guard<page> _page;

    public:
        
	virtual const c_str &get_container_name()=0;
        virtual packet_t* get_packet()=0;
        virtual void output(page* p)=0;
	virtual void stop_accepting_packets()=0;	
        virtual bool check_for_cancellation()=0;
        
        /**
         *  @brief Write a tuple to each waiting output buffer in a
         *  chain of packets.
         *
         *  @throw an exception if we should stop processing the query
         *  early for any reason. This may occur due to a non-error
         *  condition (such as a partially-shared packet completing)
         *  but is always an "exceptional" (ie unexpected) condition
         *  for a stage implementation and therefore treated the same
         *  way.
         */
        void output(const tuple_t &tuple) {
            assert(!_page->full());
            _page->append_tuple(tuple);
            if(_page->full()) {
                output(_page);
                _page->clear();
            }
        }
        
        adaptor_t(page* p)
            : _page(p)
        {
            assert(_page);
        }
        
        virtual ~adaptor_t() { }
        
    protected:
        /**
         * @brief outputs the last partial page, if any. The adaptor's
         * implementation should call this function after normal
         * processing has completed (stage implementations need not
         * concern themselves with this).
         */
        void flush() {
            if(!_page->empty())
                output(_page);
        }
        
    };


protected:

    adaptor_t* _adaptor;

    virtual void process_packet()=0;

public:

    stage_t()
	: _adaptor(NULL)
	
    {
    }

    virtual ~stage_t() { }
    

    void init(adaptor_t* adaptor) {
	_adaptor = adaptor;
    }    


    /**
     *  @brief Process this packet. The stage container must invoke
     *  init_stage() with an adaptor that we can use.
     *
     *  @return 0 on success. Non-zero value on unrecoverable
     *  error. If this function returns error, the stage will
     *  terminate all queries that it is currently involved in
     *  computing.
     */
    void process() {
	assert(_adaptor != NULL);
        
        // process rebinding instructions here since we have access to
        // the primary packet
        packet_t* packet = _adaptor->get_packet();
        query_state_t* qstate = packet->get_query_state();
        if (qstate != NULL)
            /* do CPU binding */
            qstate->rebind_self(packet);
        
	process_packet();
    }
};



EXIT_NAMESPACE(qpipe);

#endif
