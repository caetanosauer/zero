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

#ifndef __QUERY_STATE_H
#define __QUERY_STATE_H

#include "util.h"


ENTER_NAMESPACE(qpipe);


class packet_t;
struct query_state_t {

    /* We expect this class to be subclassed. */

protected:
    query_state_t() { }
    virtual ~query_state_t() { }
    
public:

    /**
     * We could create a default implementation that does not
     * rebinding, but this is safer.
     */
    virtual void rebind_self(packet_t*)=0;

};


EXIT_NAMESPACE(qpipe);


#endif 
