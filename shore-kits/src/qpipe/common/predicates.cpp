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

#include "qpipe/common/predicates.h"


ENTER_NAMESPACE(qpipe);



static bool use_deterministic_predicates(void) {
    return true;
}



predicate_randgen_t predicate_randgen_t::acquire(const char* caller_tag) {
    if (use_deterministic_predicates())
        /* deterministic */
        return predicate_randgen_t(caller_tag);
    else
        /* non-deterministic */
        return predicate_randgen_t(thread_get_self()->randgen());
}



EXIT_NAMESPACE(qpipe);
