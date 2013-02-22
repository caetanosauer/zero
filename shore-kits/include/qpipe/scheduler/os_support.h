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

#ifndef _OS_SUPPORT_H
#define _OS_SUPPORT_H

#ifdef  UNSUPPORTED_OS
#define UNSUPPORTED_OS 1
#endif


/* GNU Linux */
#if defined(linux) || defined(__linux)
#define FOUND_LINUX
/* detected GNU Linux */
#undef  UNSUPPORTED_OS
#define UNSUPPORTED_OS 0

#ifndef __USE_GNU
#define __USE_GNU
#endif

#include <sched.h>

typedef cpu_set_t os_cpu_set_t;

#else

/* Sun Solaris */
#if defined(sun) || defined(__sun)
#if defined(__SVR4) || defined(__svr4__)

#undef  UNSUPPORTED_OS
#define UNSUPPORTED_OS 0

/* detected Sun Solaris */
#include <sys/types.h>
#include <sys/processor.h>

#endif
#endif

#endif




#if UNSUPPORTED_OS
#error "Unsupported operating system\n"
#endif



#endif
