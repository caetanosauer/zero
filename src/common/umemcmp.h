/*<std-header orig-src='shore' incl-file-exclusion='UMEMCMP_H'>

 $Id: umemcmp.h,v 1.22 2010/12/08 17:37:34 nhall Exp $

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

#ifndef UMEMCMP_H
#define UMEMCMP_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
 * This file provides an version of memcmp() called umemcmp that
 * compared unsigned characters instead of signed.
 * For correct operation of the btree code umemcmp() must be used.
 * In fact we recommend you using memcmp only to test for 
 * == or != as it can give different results for > and < depending
 * on the compiler.
 */

#include <assert.h>

#ifndef W_WORKAROUND_H
#include <w_workaround.h>
#endif

// Simple byte-by-byte comparisions
inline int __umemcmp(const unsigned char* p, const unsigned char* q, int n)
{
    int i;
    for (i = 0; (i < n) && (*p == *q); i++, p++, q++) ;
    return (i < n) ? *p - *q : 0;
}

/*
 * XXX this is a dangerous assumption; correct operation for umemcpy()
 * should be verified!
 *
 * So far only sparcs (sunos) have been found to need a special umemcmp.
 */
#if defined(Sparc)

inline uint int_alignment_check(const void *i) 
{
    uint tmp = (ptrdiff_t) i & (sizeof(int)-1);
    w_assert9(tmp == (ptrdiff_t) i % sizeof(int));
    return tmp;
}
inline bool is_int_aligned(const void *i)
{
    return int_alignment_check(i) == 0;
}

// Smarter way if things are aligned.  Basically this does the
// comparison an int at a time.
inline int umemcmp_smart(const void* p_, const void* q_, int n)
{
    const unsigned char* p = (const unsigned char*)p_;
    const unsigned char* q = (const unsigned char*)q_;

    // If short, just use simple method
    if (n < (int)(2*sizeof(int)))
        return __umemcmp(p, q, n);

    // See if both are aligned to the same value
    if (int_alignment_check(p) == int_alignment_check(q)) {
        if (!is_int_aligned(p)) {
            // can't handle misaliged, use simple method
            return __umemcmp(p, q, n);
        }

        // Compare an int at a time
        uint i;
        for (i = 0; i < n/sizeof(int); i++) {
            if (((unsigned*)p)[i] != ((unsigned*)q)[i]) {
                return (((unsigned*)p)[i] > ((unsigned*)q)[i]) ? 1 : -1;
            }
        }
        // take care of the leftover bytes
        int j = i*sizeof(int);
        if (j) return __umemcmp(p+j, q+j, n-j);
    } else {
        // misaligned with respect to eachother
        return __umemcmp(p, q, n);
    }
    return 0; // must be equal
}

inline int umemcmp_old(const void* p, const void* q, int n)
{
    return __umemcmp((unsigned char*)p, (unsigned char*)q, n);
}

inline int umemcmp(const void* p, const void* q, int n)
{
#if W_DEBUG_LEVEL > 2
    // check for any bugs in umemcmp_smart
    int t1 = umemcmp_smart(p, q, n);
    int t2 = __umemcmp((unsigned char*)p, (unsigned char*)q, n);
    assert(t1 == t2 || (t1 < 0 && t2 < 0) || (t1 > 0 && t2 > 0));
    return t1;
#else
    return umemcmp_smart(p, q, n);
#endif 
}

#else  /* defined(Sparc) */

inline int umemcmp(const void* p, const void* q, int n)
{
#if W_DEBUG_LEVEL > 2
    // verify that memcmp is equivalent to umemcmp
    int t1 = memcmp(p, q, n);
    int t2 = __umemcmp((unsigned char*)p, (unsigned char*)q, n);
    w_assert3(t1 == t2 || (t1 < 0 && t2 < 0) || (t1 > 0 && t2 > 0));
    return t1;
#else
    return memcmp(p, q, n);
#endif 
}

#endif /* defined(Sparc)  */

/*<std-footer incl-file-exclusion='UMEMCMP_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
