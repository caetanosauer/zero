/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef LOCK_COMPT_H
#define LOCK_COMPT_H
/**
 * this file defines the lock compatibility table and other constant
 * values for lock tables. Only included from lock_core.cpp
 */

// use this to compute highest prime # 
// less that requested hash table size. 
// Actually, it uses the highest prime less
// than the next power of 2 larger than the
// number requested.  Lowest allowable
// hash table option size is 64.

static const uint32_t primes[] = {
        /* 0x40, 64, 2**6 */ 61,
        /* 0x80, 128, 2**7  */ 127,
        /* 0x100, 256, 2**8 */ 251,
        /* 0x200, 512, 2**9 */ 509,
        /* 0x400, 1024, 2**10 */ 1021,
        /* 0x800, 2048, 2**11 */ 2039,
        /* 0x1000, 4096, 2**12 */ 4093,
        /* 0x2000, 8192, 2**13 */ 8191,
        /* 0x4000, 16384, 2**14 */ 16381,
        /* 0x8000, 32768, 2**15 */ 32749,
        /* 0x10000, 65536, 2**16 */ 65521,
        /* 0x20000, 131072, 2**17 */ 131071,
        /* 0x40000, 262144, 2**18 */ 262139,
        /* 0x80000, 524288, 2**19 */ 524287,
        /* 0x100000, 1048576, 2**20 */ 1048573,
        /* 0x200000, 2097152, 2**21 */ 2097143,
        /* 0x400000, 4194304, 2**22 */ 4194301,
        /* 0x800000, 8388608, 2**23   */ 8388593
};

#endif // LOCK_COMPT_H