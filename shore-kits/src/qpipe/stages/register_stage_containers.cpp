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

/** @file:   register_stage_containers.cpp
 *
 *  @brief:  Registers (initiates) containers of the various stages used
 *
 */

#include "qpipe/stages/register_stage_containers.h"
#include "qpipe/stages.h"

ENTER_NAMESPACE(qpipe);

//#define MAX_NUM_CLIENTS 16
#define MAX_NUM_CLIENTS 128

#define MAX_NUM_TSCAN_THREADS             MAX_NUM_CLIENTS * 2 // Q4, Q16 have two scans
#define MAX_NUM_AGGREGATE_THREADS         MAX_NUM_CLIENTS
#define MAX_NUM_PARTIAL_AGGREGATE_THREADS MAX_NUM_CLIENTS * 2 // Q1 uses two partial aggregates
#define MAX_NUM_HASH_JOIN_THREADS         MAX_NUM_CLIENTS
#define MAX_NUM_SORT_MERGE_JOIN_THREADS   MAX_NUM_CLIENTS
#define MAX_NUM_FUNC_CALL_THREADS         MAX_NUM_CLIENTS
#define MAX_NUM_SORT_THREADS              MAX_NUM_CLIENTS * 2 // Q16 uses two sorts
#define MAX_NUM_SORTED_IN_STAGE_THREADS   MAX_NUM_CLIENTS


void register_stage_containers() 
{
    TRACE( TRACE_ALWAYS, "Registering stage containers\n");

    register_stage<tscan_stage_t>(MAX_NUM_TSCAN_THREADS, true);
    register_stage<aggregate_stage_t>(MAX_NUM_AGGREGATE_THREADS, true);
    register_stage<partial_aggregate_stage_t>(MAX_NUM_PARTIAL_AGGREGATE_THREADS, true);
    register_stage<hash_aggregate_stage_t>(MAX_NUM_AGGREGATE_THREADS, true);
    register_stage<hash_join_stage_t>(MAX_NUM_HASH_JOIN_THREADS, true);
    register_stage<sort_merge_join_stage_t>(MAX_NUM_SORT_MERGE_JOIN_THREADS, true);
    register_stage<pipe_hash_join_stage_t>(MAX_NUM_CLIENTS, true);
    register_stage<func_call_stage_t>(MAX_NUM_FUNC_CALL_THREADS, true);
    register_stage<sort_stage_t>(MAX_NUM_SORT_THREADS, true);
    register_stage<fdump_stage_t> (MAX_NUM_CLIENTS, true);
    register_stage<sorted_in_stage_t>(MAX_NUM_SORTED_IN_STAGE_THREADS, true);
    register_stage<echo_stage_t>(MAX_NUM_CLIENTS, true);
    register_stage<sieve_stage_t>(MAX_NUM_CLIENTS, true);
}

EXIT_NAMESPACE(qpipe);
