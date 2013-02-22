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


#ifndef __SCHEDULER_POLICY_H
#define __SCHEDULER_POLICY_H

#include "qpipe/core/packet.h"
#include "qpipe/core/query_state.h"
#include "qpipe/scheduler/cpu.h"



ENTER_NAMESPACE(qpipe);


using qpipe::packet_t;
using qpipe::query_state_t;


class policy_t {

public:

    virtual query_state_t* query_state_create()=0;
    virtual void query_state_destroy(query_state_t* qstate)=0;
    virtual ~policy_t() { }
};



EXIT_NAMESPACE(qpipe);



#endif
