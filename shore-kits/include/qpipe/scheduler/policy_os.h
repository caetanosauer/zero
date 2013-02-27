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

#ifndef __QPIPE_POLICY_OS_H
#define __QPIPE_POLICY_OS_H

#include "qpipe/scheduler/policy.h"
#include <cstdlib>


ENTER_NAMESPACE(qpipe);



/* exported datatypes */

class policy_os_t : public policy_t {

protected:

    class os_query_state_t : public qpipe::query_state_t {
    public:
        os_query_state_t() { }
        virtual ~os_query_state_t() { }

        virtual void rebind_self(packet_t*) {
            // do no rebinding
        }
    };


public:

    policy_os_t() { }
    virtual ~policy_os_t() {}

    
    virtual query_state_t* query_state_create() {
        return new os_query_state_t();
    }


    virtual void query_state_destroy(query_state_t* qs) {
        os_query_state_t* qstate = dynamic_cast<os_query_state_t*>(qs);
        delete qstate;
    }

};

EXIT_NAMESPACE(qpipe);

#endif
