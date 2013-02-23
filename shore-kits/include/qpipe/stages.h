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

#ifndef __QPIPE_STAGES_H
#define __QPIPE_STAGES_H

#include "qpipe/stages/aggregate.h"
#include "qpipe/stages/bnl_in.h"
#include "qpipe/stages/bnl_join.h"
#include "qpipe/stages/fdump.h"
#include "qpipe/stages/fscan.h"
#include "qpipe/stages/func_call.h"
#include "qpipe/stages/hash_join.h"
#include "qpipe/stages/sort_merge_join.h"
#include "qpipe/stages/pipe_hash_join.h"
#include "qpipe/stages/merge.h"
#include "qpipe/stages/partial_aggregate.h"
#include "qpipe/stages/hash_aggregate.h"
#include "qpipe/stages/sort.h"
#include "qpipe/stages/sorted_in.h"
#include "qpipe/stages/tscan.h"
#include "qpipe/stages/echo.h"
#include "qpipe/stages/sieve.h"
#include "qpipe/stages/delay_writer.h"
#include "qpipe/stages/tuple_source.h"

#include "qpipe/stages/register_stage_containers.h"

#endif

