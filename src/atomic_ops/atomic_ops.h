#ifndef SHORE_ATOMICS_H
#define SHORE_ATOMICS_H
#include "shore-config.h"

#ifndef HAVE_UCHAR_T
typedef unsigned char uchar_t;
#endif
#ifndef HAVE_USHORT_T
typedef unsigned short ushort_t;
#endif
#ifndef HAVE_UINT_T
typedef unsigned int   uint_t;
#endif
#ifndef HAVE_ULONG_T
typedef unsigned long   ulong_t;
#endif

#ifndef HAVE_CHAR_T
typedef char char_t;
#endif
#ifndef HAVE_SHORT_T
typedef short short_t;
#endif
#ifndef HAVE_INT_T
typedef int   int_t;
#endif
#ifndef HAVE_LONG_T
typedef long   long_t;
#endif


#if defined(HAVE_ATOMIC_H) && !defined(HAVE_MEMBAR_ENTER)
#error  atomic_ops does not include membar_ops
#endif
#if defined(HAVE_MEMBAR_ENTER) && !defined(HAVE_ATOMIC_H) 
#error  membar_ops defined but atomic_ops missing
#endif

#ifdef HAVE_ATOMIC_H
#include <atomic.h>
#else

#if defined(__GLIBC_HAVE_LONG_LONG) && __GLIBC_HAVE_LONG_LONG!=0

#ifndef _INT64_TYPE
#define _INT64_TYPE
#define _INT64_TYPE_DEFINED
#endif

#endif

#include "atomic_ops_impl.h"

// Clean up after defining these
#ifdef _INT64_TYPE_DEFINED
#undef _INT64_TYPE
#undef _INT64_TYPE_DEFINED
#endif

#endif
#endif
