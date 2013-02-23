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

/** @file:   tscan.cpp
 *
 *  @brief:  Implementation of the QPipe Shore-MT table scan stage
 *
 *  @author: Ippokratis Pandis
 *  @date:   Apr 2010
 */

#include "qpipe/stages/tscan.h"
#include <unistd.h>

#include "sm_vas.h"

using namespace shore;

ENTER_NAMESPACE(qpipe);


/******************************************************************
 * 
 * @class: Packet for the table scans
 *
 ******************************************************************/

const c_str tscan_packet_t::PACKET_TYPE = "TSCAN";

const c_str tscan_stage_t::DEFAULT_STAGE_NAME = "TSCAN_STAGE";

#define KB 1024
#define MB (1024*KB)

tscan_packet_t::tscan_packet_t(const c_str&    packet_id,
                               tuple_fifo*     output_buffer,
                               tuple_filter_t* output_filter,
                               ss_m*           db,
                               table_desc_t*   table,
                               xct_t*          pxct,
                               lock_mode_t     lm)
    : packet_t(packet_id, PACKET_TYPE, output_buffer, output_filter,
               create_plan(output_filter, table),
               true, /* merging allowed */
               true  /* unreserve worker on completion */
               ),
      _db(db), _table(table), _xct(pxct), _lm(lm)
{
    assert(_db);
    assert(_table);
    assert(_xct);
}


query_plan* tscan_packet_t::create_plan(tuple_filter_t* filter, 
                                        table_desc_t* table) 
{
    c_str action("%s:%s", PACKET_TYPE.data(), table->name());
    return new query_plan(action, filter->to_string(), NULL, 0);
}
    
void tscan_packet_t::declare_worker_needs(resource_declare_t* declare) 
{
    declare->declare(_packet_type, 1);
    /* no inputs */
}


// Ideally, we would like to allocate a large blob and do bulk reading. 
// The blob must be aligned for int accesses and a multiple of 1024 bytes long.

const size_t tscan_stage_t::TSCAN_BULK_READ_BUFFER_SIZE=256*KB;



/******************************************************************
 * 
 * @class: Stage for table scans
 *
 ******************************************************************/



/******************************************************************
 * 
 * @fn:     Stage for table scans
 *
 * @brief:  Read the specified table.
 *
 * @return: 0 on success. Non-zero on unrecoverable error. The stage
 *          should terminate all queries it is processing.
 *
 ******************************************************************/

void tscan_stage_t::process_packet() 
{
    adaptor_t* adaptor = _adaptor;
    tscan_packet_t* packet = (tscan_packet_t*)adaptor->get_packet();
    smthread_t::me()->attach_xct(packet->_xct);
    
    // Create and open scan
    simple_table_iter_t tscanner(packet->_db, packet->_table, packet->_lm);
    bool eof(false);
    pin_i* handle(NULL);
    uint pcnt=0;
    uint  tsz(packet->_table->maxsize());
    //char* tbd=0;

    w_rc_t e = tscanner.next(eof,handle);
    while (!e.is_error() && !eof) {
        //assert (tsz == handle.body_size());
        //TRACE( TRACE_ALWAYS, "(%d) (%d)\n", tsz, handle->body_size());

        // Copy the record out of the SM
        //tbd = new char[tsz];        
        //memcpy(tbd,handle->body(),tsz);
        tuple_t at((char*)handle->body(),tsz);
#warning MA:Check that this does not break anything.
        adaptor->output(at);

        e = tscanner.next(eof,handle);
    }

    // page_list* table = packet->_db;
    // for(page_list::iterator it=table->begin(); it != table->end(); ++it) {
    //     adaptor->output(*it);
    // }

    smthread_t::me()->detach_xct(packet->_xct);
}


EXIT_NAMESPACE(qpipe);
