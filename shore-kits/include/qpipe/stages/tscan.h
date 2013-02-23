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

/** @file:   tscan.h
 *
 *  @brief:  The QPipe Shore-MT table scan stage
 *
 *  @author: Ippokratis Pandis
 *  @date:   Apr 2010
 */

#ifndef __QPIPE_TSCAN_H
#define __QPIPE_TSCAN_H

#include "sm/shore/shore_table_man.h"

#include "qpipe/core/tuple_fifo.h"
#include "qpipe/core.h"

using namespace shore;

ENTER_NAMESPACE(qpipe);


/******************************************************************
 * 
 * @class: Packet for the table scans
 *
 ******************************************************************/

struct tscan_packet_t : public packet_t 
{
    ss_m*         _db;
    table_desc_t* _table;
    xct_t*        _xct;
    lock_mode_t   _lm;

    static const c_str PACKET_TYPE;
   
    /**
     *  @param: packet_id 
     *  The ID of this packet. This should point to a block of bytes allocated 
     *  with malloc(). This packet will take ownership of this block and invoke 
     *  free() when it is destroyed.
     *
     *  @param: output_buffer 
     *  The buffer where this packet should send its data. A packet DOES NOT 
     *  own its output buffer (we will not invoke delete or free() on this 
     *  field in our packet destructor).
     *
     *  @param: output_filter 
     *  The filter that will be applied to any tuple sent to output_buffer. 
     *  The packet OWNS this filter. It will be deleted in the packet destructor.
     *
     *  @param: table_desc_t 
     *  A pointer to the descriptor of the table we are going to scan. We will
     *  not take ownership of this object.
     *
     *  @param: xct_t
     *  The transaction inside which this query is executed
     *
     *  @param: lock_mode_t
     *  The concurrency level which will be used by the scan.
     */

    tscan_packet_t(const c_str&    packet_id,
		   tuple_fifo*     output_buffer,
		   tuple_filter_t* output_filter,
		   ss_m*           db,
                   table_desc_t*   table,
                   xct_t*          pxct,
                   lock_mode_t     lm=SH);

    static query_plan* create_plan(tuple_filter_t* filter, table_desc_t* file);
    void declare_worker_needs(resource_declare_t* declare);

}; // EOF: tscan_packet_t



/******************************************************************
 * 
 * @class: Table scan stage
 *
 ******************************************************************/

class tscan_stage_t : public stage_t 
{
public:

    typedef tscan_packet_t stage_packet_t;
    static const c_str  DEFAULT_STAGE_NAME;

    static const size_t TSCAN_BULK_READ_BUFFER_SIZE;

    tscan_stage_t() { }
    ~tscan_stage_t() { }

protected:
    
    virtual void process_packet();

}; // EOF: tscan_stage_t


EXIT_NAMESPACE(qpipe);

#endif
