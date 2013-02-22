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

/** @file cpu.cpp
 *
 *  @brief Implements cpu_t functions.
 *
 *  @bug None known.
 */

#include "util.h"
#include "qpipe/scheduler/cpu.h"
#include "qpipe/scheduler/cpu_struct.h"
#include "qpipe/scheduler/os_support.h"


ENTER_NAMESPACE(qpipe);

/* definitions of exported functions */


/**
 *  @brief Bind the calling thread to the specified CPU. We cannot run
 *  QPipe if any of the system calls that this function relies on
 *  fails, so we deal with errors by invoking QPIPE_PANIC().
 *
 *  @param cpu_info The cpu. Should be initialized by
 *  init_info().
 *
 *  @return void
 */
void cpu_bind_self(cpu_t cpu)
{    


  /* GNU Linux */
#ifdef FOUND_LINUX

  /* detected GNU Linux */
  /* sched_setaffinity() sets the CPU affinity mask of the process
     denoted by pid.  If pid is zero, then the current process is
     used. We really want to bind the current THREAD, but in Linux, a
     THREAD is a processor with its own pid_t. */
  
  if ( sched_setaffinity(0, sizeof(os_cpu_set_t), &cpu->cpu_set) )
    throw EXCEPTION2(QPipeException,
                     "Caught %s in call to sched_setaffinity()",
                     errno_to_str().data());

  return;

#else
//#ifdef FOUND_SOLARIS
  /* Sun Solaris */
  
  /* detected Sun Solaris */
  /* The processor_bind() function binds the LWP (lightweight process)
     or set of LWPs specified by idtype and id to the processor
     specified by processorid. If obind is not NULL, this function
     also sets the processorid_t variable pointed to by obind to the
     previous binding of one of the specified LWPs, or to PBIND_NONE
     if the selected LWP was not bound.

     If id is P_MYID, the specified LWP, process, or task is the
     current one. */

  if ( processor_bind(P_LWPID, P_MYID, cpu->cpu_id, NULL) )
      THROW2(QPipeException,
                      "Caught %s while binding processor",
                      errno_to_str().data());
  return;

#endif

      THROW1(QPipeException, 
                  "Unsupported operating system\n");

}


int  cpu_get_unique_id(cpu_t cpu) {
  return cpu->cpu_unique_id;
}

EXIT_NAMESPACE(qpipe);
