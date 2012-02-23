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

// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore'>

 $Id: atomic_templates.h,v 1.5 2010/12/17 19:36:26 nhall Exp $

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

/*
* The vast majority of the atomic add instructions are for
* incrementing or decrementing by one. For those, we use
* atomic_inc and atomic_dec.
*
* For the few others, we need to be careful about the
* delta argument lest we lose bits in it by silent conversion
* from a 64-bit int to a 32-bit int.
* For that reason, we've made templates for each delta type
* and what with their unwieldy names, we might be forced to
* be careful about which we use.
*
*/

#ifndef ATOMIC_TEMPLATES_H
#define ATOMIC_TEMPLATES_H

#include "atomic_ops.h"

/* The volatile in the template might appear to be
 * redundant, but it allows us to use T that isn't itself volatile
 *
 * Don't be surprised if you have to expand this list of 
 * specializations.
 */

template<class T>
void atomic_inc(T volatile &val);
template<class T>
void atomic_dec(T volatile &val);

template<>
inline void atomic_inc(int volatile &val)
{ atomic_add_int( (unsigned int volatile *)&val, 1); }

template<>
inline void atomic_inc(unsigned int volatile &val)
{ atomic_add_int(&val, 1); }

template<>
inline void atomic_inc(unsigned long volatile &val)
{ atomic_add_long(&val, 1); }

#if defined(ARCH_LP64)
// long and long long are same size
#elif defined(__GNUG__)
template<>
inline void atomic_inc(uint64_t volatile &val)
{ atomic_add_64((volatile uint64_t *)&val, 1); }
#else
#error int64_t Not supported for this compiler.
#endif

template<>
inline void atomic_dec(int volatile &val)
{ atomic_add_int( (unsigned int volatile *)&val, -1); }


template<>
inline void atomic_dec(unsigned int volatile &val)
{ atomic_add_int(&val, -1); }

template<>
inline void atomic_dec(unsigned long volatile &val)
{ atomic_add_long(&val, -1); }

#if defined(ARCH_LP64)
// long and long long are same size
#elif defined(__GNUG__)
template<>
inline void atomic_dec(uint64_t volatile &val)
{ atomic_add_64((volatile uint64_t *)&val, -1); }
#else
#error int64_t Not supported for this compiler.
#endif

template<class T>
T atomic_inc_nv(T volatile &val);
template<class T>
T atomic_dec_nv(T volatile &val);

template<>
inline int atomic_inc_nv(int volatile &val)
{ return atomic_add_int_nv((unsigned int volatile*)&val, 1); }
template<>
inline unsigned int atomic_inc_nv(unsigned int volatile &val)
{ return atomic_add_int_nv(&val, 1); }
template<>
inline unsigned long atomic_inc_nv(unsigned long volatile &val)
{ return atomic_add_long_nv(&val, 1); }
#if defined(ARCH_LP64)
// long and long long are same size
#elif defined(__GNUG__)
inline uint64_t atomic_inc_nv(uint64_t volatile &val)
{ return atomic_add_64_nv(&val, 1); }
#else
#error int64_t Not supported for this compiler.
#endif

template<>
inline int atomic_dec_nv(int volatile &val)
{ return atomic_add_int_nv((unsigned int volatile*)&val, -1); }
template<>
inline unsigned int atomic_dec_nv(unsigned int volatile &val)
{ return atomic_add_int_nv(&val, -1); }
template<>
inline unsigned long atomic_dec_nv(unsigned long volatile &val)
{ return atomic_add_long_nv(&val, -1); }

#if defined(ARCH_LP64)
// long and long long are same size
#elif defined(__GNUG__)
inline uint64_t atomic_dec_nv(uint64_t volatile &val)
{ return atomic_add_64_nv(&val, -1); }
#else
#error int64_t Not supported for this compiler.
#endif


/* The following templates take an int delta */

template<class T>
void atomic_add_int_delta(T volatile &val, int delta);

template<>
inline void atomic_add_int_delta(unsigned int volatile &val, int delta) 
{ atomic_add_int(&val, delta); }

template<>
inline void atomic_add_int_delta(int volatile &val, int delta) 
{ atomic_add_int((unsigned int volatile*)&val, delta); }

template<>
inline void atomic_add_int_delta(unsigned long volatile &val, int delta) 
{ atomic_add_long(&val, delta); }

template<>
inline void atomic_add_int_delta(long volatile &val, int delta) 
{ atomic_add_long((unsigned long volatile*)&val, delta); }

#if defined(ARCH_LP64)
// long and long long are same size
#elif defined(__GNUG__)
template<>
inline void atomic_add_int_delta(uint64_t volatile &val, int delta) 
{ 
    int64_t  deltalg=delta;
    atomic_add_64(&val, deltalg); 
}

template<>
inline void atomic_add_int_delta(long long volatile &val, int delta) 
{ 
    int64_t  deltalg=delta;
    atomic_add_64((uint64_t volatile *)&val, deltalg); 
}
#else
#error int64_t Not supported for this compiler.
#endif

/* atomic_add_nv variants */

template<class T>
T atomic_add_nv_int_delta(T volatile &val, int delta);

template<>
inline unsigned int atomic_add_nv_int_delta(unsigned int volatile &val, int delta) 
{ return atomic_add_int_nv(&val, delta); }

template<>
inline int atomic_add_nv_int_delta(int volatile &val, int delta) 
{ return atomic_add_int_nv((unsigned int*) &val, delta); }

template<>
inline unsigned long atomic_add_nv_int_delta(unsigned long volatile &val, int delta) 
{ return atomic_add_long_nv(&val, delta); }

template<>
inline long atomic_add_nv_int_delta(long volatile &val, int delta) 
{ return atomic_add_long_nv((unsigned long*) &val, delta); }

#if defined(ARCH_LP64)
// long and long long are same size
#elif defined(__GNUG__)
template<>
inline uint64_t atomic_add_nv_int_delta(uint64_t volatile &val, int delta) 
{ return atomic_add_64_nv(&val, delta); }

template<>
inline long long atomic_add_nv_int_delta(long long volatile &val, int delta) 
{ return atomic_add_64_nv((uint64_t *)&val, delta); }
#else
#error int64_t Not supported for this compiler.
#endif

/* The following templates take a long delta */
template<class T>
void atomic_add_long_delta(T volatile &val, long delta);

template<>
inline void atomic_add_long_delta(unsigned int volatile &val, long delta) 
{ atomic_add_int(&val, delta); }

template<>
inline void atomic_add_long_delta(int volatile &val, long delta) 
{ atomic_add_int((unsigned int volatile*)&val, delta); }

template<>
inline void atomic_add_long_delta(unsigned long volatile &val, long delta) 
{ atomic_add_long(&val, delta); }

template<>
inline void atomic_add_long_delta(long volatile &val, long delta) 
{ atomic_add_long((unsigned long volatile*)&val, delta); }

#if defined(ARCH_LP64)
// long and long long are same size
#elif defined(__GNUG__)
// Needed when building for 32 bits:
template<>
inline void atomic_add_long_delta(uint64_t volatile &val, long delta) 
{ 
    int64_t  deltalg=delta;
    atomic_add_64(&val, deltalg); 
}

template<>
inline void atomic_add_long_delta(long long volatile &val, long delta) 
{ 
    int64_t  deltalg=delta;
    atomic_add_64((uint64_t volatile *)&val, deltalg); 
}
#else
#error int64_t Not supported for this compiler.
#endif

/* atomic_add_nv variants */

template<class T>
T atomic_add_nv_long_delta(T volatile &val, long delta);

template<>
inline unsigned int atomic_add_nv_long_delta(unsigned int volatile &val, long delta) 
{ return atomic_add_int_nv(&val, delta); }

template<>
inline int atomic_add_nv_long_delta(int volatile &val, long delta) 
{ return atomic_add_int_nv((unsigned int*) &val, delta); }

template<>
inline unsigned long atomic_add_nv_long_delta(unsigned long volatile &val, long delta) 
{ return atomic_add_long_nv(&val, delta); }

template<>
inline long atomic_add_nv_long_delta(long volatile &val, long delta) 
{ return atomic_add_long_nv((unsigned long*) &val, delta); }

#if defined(ARCH_LP64)
// long and long long are same size
#elif defined(__GNUG__)
template<>
inline uint64_t atomic_add_nv_long_delta(uint64_t volatile &val, long delta) 
{ return atomic_add_64_nv(&val, delta); }

template<>
inline long long atomic_add_nv_long_delta(long long volatile &val, long delta) 
{ return atomic_add_64_nv((uint64_t *)&val, delta); }
#else
#error int64_t Not supported for this compiler.
#endif

#endif          /*</std-footer>*/
