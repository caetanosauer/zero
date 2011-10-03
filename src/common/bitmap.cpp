/*<std-header orig-src='shore'>

 $Id: bitmap.cpp,v 1.29 2010/12/08 17:37:34 nhall Exp $

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

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define BITMAP_C

#ifdef __GNUC__
#pragma implementation "bitmap.h"
#endif

#include <cstdlib>
#include <w_stream.h>
#include "basics.h" 
#include "bitmap.h" 
#include <w_debug.h>

inline int div8(int x)         { return x >> 3; }
inline int mod8(int x)         { return x & 7; }
inline int div32(int x)        { return x >> 5; }
inline int mod32(int x)        { return x & 31; }

const int OVERFLOW_MASK = 0x100;
    

void bm_zero(u_char* bm, int size)
{
    int n = div8(size - 1) + 1;
    for (int i = 0; i < n; i++, bm++)
        *bm = 0;
}

void bm_fill(u_char* bm, int size)
{
    int n = div8(size - 1) + 1;
    for (int i = 0; i < n; i++, bm++)
        *bm = ~0;
}

bool bm_is_set(const u_char* bm, int offset)
{
    return (bm[div8(offset)] & (1 << mod8(offset))) != 0;
}

void bm_set(u_char* bm, int offset)
{
    bm[div8(offset)] |= (1 << mod8(offset));
}

void bm_clr(u_char* bm, int offset)
{
    bm[div8(offset)] &= ~(1 << mod8(offset));
}

int bm_first_set(const u_char* bm, int size, int start)
{
#if W_DEBUG_LEVEL > 2
    const u_char *bm0 = bm;
#endif
    register int mask;
    
    w_assert3(start >= 0 && start <= size);
    
    bm += div8(start);
    mask = 1 << mod8(start);
    
    for (size -= start; size; start++, size--)  {
        if (*bm & mask)  {
            w_assert3(bm_is_set(bm0, start));
            return start;
        }
        if ((mask <<= 1) == OVERFLOW_MASK)  {
            mask = 1;
            bm++;
        }
    }
    
    return -1;
}

int bm_first_clr(const u_char* bm, int size, int start)
{
    w_assert3(start >= 0 && start <= size);
    register int mask;
#if W_DEBUG_LEVEL > 2
    const u_char *bm0 = bm;
#endif
    
    bm += div8(start);
    mask = 1 << mod8(start);
    
    for (size -= start; size; start++, size--) {
        if ((*bm & mask) == 0)    {
            w_assert3(bm_is_clr(bm0, start));
            return start;
        }
        if ((mask <<= 1) == OVERFLOW_MASK)  {
            mask = 1;
            bm++;
        }
    }
    
    return -1;
}


int bm_last_set(const u_char* bm, int size, int start)
{
    register unsigned mask;
#if W_DEBUG_LEVEL > 2
    const    u_char *bm0 = bm;
#endif
    
    w_assert3(start >= 0 && start < size);
    
    bm += div8(start);
    mask = 1 << mod8(start);
    
    for (size = start+1; size; start--, size--)  {
        if (*bm & mask)  {
            w_assert3(bm_is_set(bm0, start));
            return start;
        }
        if ((mask >>= 1) == 0)  {
            mask = 0x80;
            bm--;
        }
    }
    
    return -1;
}


int bm_last_clr(const u_char* bm, int size, int start)
{
    register unsigned mask;
#if W_DEBUG_LEVEL > 2
    const u_char *bm0 = bm;
#endif
    
    w_assert3(start >= 0 && start < size);
    
    bm += div8(start);
    mask = 1 << mod8(start);
    
    for (size = start+1; size; start--, size--)  {
        if ((*bm & mask) == 0)  {
            w_assert3(bm_is_clr(bm0, start));
            return start;
        }
        if ((mask >>= 1) == 0)  {
            mask = 0x80;
            bm--;
        }
    }
    
    return -1;
}


int bm_num_set(const u_char* bm, int size)
{
    int count;
    int mask;
    for (count = 0, mask = 1; size; size--)  {
        if (*bm & mask)
            count++;
        if ((mask <<= 1) == OVERFLOW_MASK)  {
            mask = 1;
            bm++;
        }
    }
    return count;
}

void bm_print(u_char* bm, int size)
{
    for (int i = 0; i < size; i++)  {
        cout << (bm_is_set(bm, i) != 0);
    }
}

