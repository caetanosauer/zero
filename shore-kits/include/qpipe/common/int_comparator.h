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

#ifndef __QPIPE_INT_COMPARATOR_H
#define __QPIPE_INT_COMPARATOR_H

#include "qpipe/core.h"

ENTER_NAMESPACE(qpipe);

struct int_key_compare_t : public key_compare_t {
    virtual int operator()(const void*, const void*) const {
	unreachable();
    }
    virtual key_compare_t* clone() const {
        return new int_key_compare_t(*this);
    }
};

typedef default_key_extractor_t int_key_extractor_t;

EXIT_NAMESPACE(qpipe);

#endif
