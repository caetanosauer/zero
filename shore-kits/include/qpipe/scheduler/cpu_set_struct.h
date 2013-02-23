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


/** @file cpu_set_struct.h
 *
 *  @brief Exports cpu_set_s structure. Do not include this
 *  file unless a module needs to statically create these
 *  structures. All manipulations should be done on a
 *  cpu_set_p instance using one of the exported macros.
 *
 *  @bug See cpu.cpp.
 */

#ifndef __SCHEDULER_CPU_SET_STRUCT_H
#define __SCHEDULER_CPU_SET_STRUCT_H

#include "util/namespace.h"
#include "qpipe/scheduler/os_support.h"
#include "qpipe/scheduler/cpu_struct.h"

ENTER_NAMESPACE(qpipe);

/* exported structures */


/**
 *  @brief A set of CPUs.
 */
struct cpu_set_s
{
  /** The number of CPUs in this set. */
  int cpuset_num_cpus;

  /** The CPUs in this set. */
  struct cpu_s* cpuset_cpus;
};


EXIT_NAMESPACE(qpipe);

#endif
