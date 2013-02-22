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

/** @file:   shore_reqs.cpp
 *
 *  @brief:  Structures that represent user requests
 *
 *  @author: Ippokratis Pandis, Feb 2010
 */


#include "sm/shore/shore_trx_worker.h"


ENTER_NAMESPACE(shore);


/****************************************************************** 
 *
 * @fn:    notify_client()
 *
 * @brief: If it is time, notifies the client (signals client's cond var) 
 *
 ******************************************************************/

void base_request_t::notify_client() 
{
    // signal cond var
    condex* pcondex = _result.get_notify();
    if (pcondex) {
        TRACE( TRACE_TRX_FLOW, "Xct (%d) notifying client (%x)\n", 
               _tid.get_lo(), pcondex);
	pcondex->signal();
        _result.set_notify(NULL);
    }
    else {
        TRACE( TRACE_TRX_FLOW, "Xct (%d) not notifying client\n", 
               _tid.get_lo());
    }
}


EXIT_NAMESPACE(shore);
