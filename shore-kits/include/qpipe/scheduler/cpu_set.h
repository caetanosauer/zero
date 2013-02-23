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

/** @file cpu_set.h
 *
 *  @brief Exports cpu_set_p datatype.
 *
 *  @bug See cpu_set.cpp.
 */

#ifndef __QPIPE_CPU_SET_H
#define __QPIPE_CPU_SET_H

#include "util/namespace.h"
#include "qpipe/scheduler/os_support.h"
#include "qpipe/scheduler/cpu.h"

ENTER_NAMESPACE(qpipe);

/* exported datatypes */

typedef struct cpu_set_s* cpu_set_p;

/* exported functions */

void cpu_set_init(cpu_set_p cpu_set);
int cpu_set_get_num_cpus(cpu_set_p cpu_set);
cpu_t cpu_set_get_cpu(cpu_set_p cpu_set, int index);
void cpu_set_finish(cpu_set_p cpu_set);

EXIT_NAMESPACE(qpipe);

#endif
