/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

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

/*<std-header orig-src='shore' incl-file-exclusion='W_DEFINES_H' no-defines='true'>

 $Id: w_defines.h,v 1.8 2010/12/08 17:37:37 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef W_DEFINES_H
#define W_DEFINES_H

/*  -- do not edit anything above this line --   </std-header>*/

/* shore-config.h does not have duplicate-include protection, but
   this file does, and we don't include shore-config.h anywhere else

   Before including it, erase the existing autoconf #defines (if any)
   to avoid warnings about redefining macros.
*/
#ifdef PACKAGE
#undef PACKAGE
#endif
#ifdef PACKAGE_NAME
#undef PACKAGE_NAME
#endif
#ifdef PACKAGE_STRING
#undef PACKAGE_STRING
#endif
#ifdef PACKAGE_BUGREPORT
#undef PACKAGE_BUGREPORT
#endif
#ifdef PACKAGE_TARNAME
#undef PACKAGE_TARNAME
#endif
#ifdef PACKAGE_VERSION
#undef PACKAGE_VERSION
#endif
#ifdef VERSION
#undef VERSION
#endif
#include "shore-config.h"
#include "shore.def"

// #ifdef HAVE_VALGRIND_H
// #define USING_VALGRIND 1
// #include <valgrind.h>
// #elif defined(HAVE_VALGRIND_VALGRIND_H)
// #define USING_VALGRIND 1
// #include <valgrind/valgrind.h>
// #endif

// #ifdef USING_VALGRIND
// #include "valgrind_help.h"
// #endif

// now ZERO_INIT is set according to W_DEBUG_LEVEL.
#if W_DEBUG_LEVEL>0
#define ZERO_INIT 1 /* for valgrind/purify */
#endif

#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <climits>

#include <stdint.h>

#include <unistd.h>

/* the following cannot be "verbatim" included in shore-config.def,
 * unfortunately. The  #undef ARCH_LP64 gets mangled
 * by autoconf.
 */
#ifdef ARCH_LP64
/* enabled LP64 - let's make sure the environment can handle it */
#if defined(_SC_V6_LP64_OFF64) || _XBS5_LP64_OFF64 || _SC_V6_LPBIG_OFFBIG || _XBS5_LPBIG_OFFBIG
#else
#warning Turning off ARCH_LP64
#undef ARCH_LP64
#endif
/* ARCH_LP64 was defined (might no longer be) */
#endif

/* Issue warning if we don't have large file offsets with ILP32 */
#ifndef ARCH_LP64

#if _SC_V6_ILP32_OFFBIG || _XBS5_ILP32_OFFBIG
#else
#warning large file off_t support seems to be missing accoring to sysconf !
#endif

/* ARCH_LP64 not defined */
#endif


#if SM_PAGESIZE > 32768 * 8
#error SM does not support pages this large.
#endif


#include <sys/types.h>
using namespace std;

// avoid nasty bus errors...
template<class T>
static inline T* aligned_cast(char const* ptr)
{
  // bump the pointer up to the next proper alignment (always a power of 2)
  size_t val = (size_t) ptr;
  val += __alignof__(T) - 1;
  val &= -__alignof__(T);
  return (T*) val;
}


/** \brief constructs a blob of N bytes.
*
*  When instantiated on the stack, allocates enough
*  bytes on the stack to hold an object of size N, aligned.
*  Specialize this to handle arbitrary sizes (sizeof(a)+sizeof(b), etc).
*/
template<int N>
class allocaN {
  char _buf[N+__alignof__(double)];
public:
  operator void*() { return aligned_cast<double>(_buf); }
  // no destructor because we don't know what's going on...
};

/**
* \brief CPU Cache line size in bytes.
* \details
* Most modern CPU has 64 bytes cacheline.
* Some less popular CPU like Spark uses 128 bytes.
* This value is used for padding to keep lock objects in different cachelines.
* TODO: CMake script to automatically detect this and cmakedefine for it (JIRA ZERO-179).
*/
const size_t CACHELINE_SIZE = 64;

/*<std-footer incl-file-exclusion='W_DEFINES_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
