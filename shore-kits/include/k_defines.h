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

/** @file:   k_defines.h
 *
 *  @brief:  Defines the correct includes, atomic primitives, etc..
 *           Used so that a single shore-kits codebase to compile with
 *           both Shore-MT and Shore-SM-6.X.X (the x86_64 or Nancy's port)
 *
 *  @author: Ippokratis Pandis, May 2010
 *
 */

#ifndef __KITS_DEFINES_H
#define __KITS_DEFINES_H

// If we use the x86-ported branch (aka Nancy's branch) then w_defines.h should
// be included instead of atomic.h

#ifdef CFG_SHORE_6

#include <w_defines.h>
#include <atomic_templates.h>

#include "sm_vas.h"
typedef queue_based_lock_t mcs_lock;

#include "block_alloc.h"

#else // CFG_SHORE_MT

#include <atomic.h>

#endif


#ifdef __SUNPRO_CC
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#else
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cstdarg>
#endif


#ifndef __sparcv9

typedef int processorid_t;
const int PBIND_NONE = -1;
// const int P_LWPID = 0;
// const int P_MYID = 0;

// // IP: on non-SunOS/sparcv9 we disable processor_bind
// #define processorid_bind(A,B,C,D) NULL

#else

// for binding LWP to cores
#include <sys/types.h>
#include <sys/processor.h>
#include <sys/procset.h>

#endif

#endif /** __KITS_DEFINES_H */
