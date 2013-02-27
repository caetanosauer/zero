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

#ifndef __CPU_BIND_H
#define __CPU_BIND_H

#include "util.h"

ENTER_NAMESPACE(qpipe);

class packet_t;

struct cpu_bind {
    struct nop;
public:
    // instructs the scheduler to choose which CPU (if any) to bind
    // the current thread to, based on the packet supplied
    virtual void bind(packet_t* packet)=0;
    virtual ~cpu_bind() { }
};

struct cpu_bind::nop : cpu_bind {
    virtual void bind(packet_t*) {
        // do nothing!
    }
};

EXIT_NAMESPACE(qpipe);

#endif 
