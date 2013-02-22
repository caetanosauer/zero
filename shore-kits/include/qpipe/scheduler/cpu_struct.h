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

/** @file cpu_struct.h
 *
 *  @brief Exports cpu_s structure. Do not include this
 *  file unless a module needs to statically create these
 *  structures. All manipulations should be done on a cpu_t
 *  instance using one of the exported macros.
 *
 *  @bug See cpu.cpp.
 */

#ifndef __QPIPE_CPU_STRUCT_H
#define __QPIPE_CPU_STRUCT_H

#include "util/namespace.h"
#include "qpipe/scheduler/os_support.h"



/* operating system specific includes */


ENTER_NAMESPACE(qpipe);

/* exported structures */

/**
 *  @brief Architecture-specific representation of a CPU.
 */
struct cpu_s
{

  /* regardless of architecture, we would like to provide a unique
     integer id for each CPU so we can print them */
  int cpu_unique_id;

#ifdef FOUND_LINUX
  /* GNU Linux */

  /* detected GNU Linux */
  /** One of the operations we would like to provide on a CPU is the
      ability of a thread to bind to it. We implement processor
      binding in Linux using the hard affinity library functions
      provided by glibc. This cpu_set_t should be initialized to
      contain a single CPU (this one). */
    os_cpu_set_t cpu_set;
 
#else
//#ifdef FOUND_SOLARIS
  /* Sun Solaris */
  
  /* detected Sun Solaris */
  /** One of the operations we would like to provide on a CPU is the
      ability of a thread to bind to it. We implement processor
      binding in Solaris using the processor_bind() system call. This
      field is initialized to this CPU's ID. */
  processorid_t    cpu_id;

  /** We use the processor_info() system call to ensure that a CPU of
      a specified ID is available for binding. Rather than throw the
      returned information away, we store it. */
  processor_info_t cpu_proc_info;
#endif  

};


EXIT_NAMESPACE(qpipe);

#endif
