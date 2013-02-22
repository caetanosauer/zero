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

#ifndef __QPIPE_POLICY_QUERY_CPU_H
#define __QPIPE_POLICY_QUERY_CPU_H

#include "util.h"
#include "qpipe/scheduler/policy.h"
#include "qpipe/scheduler/cpu_set_struct.h"
#include "qpipe/scheduler/cpu_set.h"



ENTER_NAMESPACE(qpipe);



/* exported datatypes */

class policy_query_cpu_t : public policy_t {

protected:

    pthread_mutex_t _cpu_next_mutex;
    struct cpu_set_s _cpu_set;
    int _cpu_next;
    int _cpu_num;
 

    class query_cpu_query_state_t : public qpipe::query_state_t {

    public:

        // Just track the CPU we picked for this query
        cpu_t _qcpu;

        query_cpu_query_state_t(cpu_t qcpu)
            : _qcpu(qcpu)
        {
        }

        virtual ~query_cpu_query_state_t() { }

        virtual void rebind_self(packet_t*) {
             /* Rebind calling thread to query's CPU. */
            cpu_bind_self(_qcpu);
        }
    };

  
public:
   
    policy_query_cpu_t()
        : _cpu_next_mutex(thread_mutex_create())
    {
        cpu_set_init(&_cpu_set);
        _cpu_next = 0;
        _cpu_num  = cpu_set_get_num_cpus(&_cpu_set);
    }


    virtual ~policy_query_cpu_t() {
        cpu_set_finish(&_cpu_set);
        thread_mutex_destroy(_cpu_next_mutex);
    }


    virtual query_state_t* query_state_create() {

        int next_cpu;
    
        critical_section_t cs(_cpu_next_mutex);
    
        // RR-CPU dispatching policy requires that every call to
        // assign_packet_to_cpu() results in an increment of the next cpu
        // index.
        next_cpu = _cpu_next;
        _cpu_next = (_cpu_next + 1) % _cpu_num;
    
        cs.exit();
    
        return new query_cpu_query_state_t( cpu_set_get_cpu(&_cpu_set, next_cpu) );
    }
  

    virtual void query_state_destroy(query_state_t* qs) {
        // Dynamic cast acts like an assert(), verifying the type.
        query_cpu_query_state_t* qstate = dynamic_cast<query_cpu_query_state_t*>(qs);
        delete qstate;
    }

   
};



EXIT_NAMESPACE(qpipe);



#endif
