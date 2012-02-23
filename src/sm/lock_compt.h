#ifndef LOCK_COMPT_H
#define LOCK_COMPT_H
/**
 * this file defines the lock compatibility table and other constant
 * values for lock tables. Only included from lock_core.cpp
 */


/*********************************************************************
 *
 *  parent_mode[i] is the lock mode of parent of i
 *        e.g. parent_mode[EX] is IX.
 *        e.g. parent_mode[UD] is IX.
 *        e.g. parent_mode[SIX] is IX.
 *        e.g. parent_mode[SH] is IS.
 *        e.g. parent_mode[IX] is IX.
 *        e.g. parent_mode[IS] is IS.
 *        e.g. parent_mode[NL] is NL.
 *
 *********************************************************************/
const lock_base_t::lmode_t lock_m::parent_mode[NUM_MODES] = {
//  NL, IS, IX, SH, SIX,UD, EX
    NL, IS, IX, IS, IX, IX, IX,
//  NS, NU, NX, SN, SU, SX, UN, US, UX, XN, XS, XU
    IS, IX, IX, IS, IX, IX, IX, IX, IX, IX, IX, IX
};


/*********************************************************************
 *   mode_str[i]:        string describing lock mode i
 *********************************************************************/
const char* const lock_base_t::mode_str[NUM_MODES] = {
    "NL", "IS", "IX", "SH", "SIX", "UD", "EX",
    "NS", "NU", "NX", "SN", "SU", "SX", "UN", "US", "UX", "XN", "XS", "XU"
};


/*********************************************************************
 *
 *  Compatibility Table (diff xact)
 *        Page 408, Table 7.11, "Transaction Processing" by Gray & Reuter
 *
 *  compat[r][g] returns bool value if a requested mode r is compatible
 *        with a granted mode g.
 *
 *********************************************************************/
const bool T = true; // to make following easier to read
const bool F = false;

// NOTE Intent-lock vs. key range lock such as "IS - NX" are all T because it never happens
const bool lock_base_t::compat
[NUM_MODES] /* request mode */
[NUM_MODES] /* granted mode */
= {
/*req*/  //NL  IS  IX  SH SIX  UD  EX    NS  NU  NX  SN  SU  SX  UN  US  UX  XN  XS  XU (granted)
/*NL*/    { T,  T,  T,  T,  T,  T,  T,    T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T }, 
/*IS*/    { T,  T,  T,  T,  T,  F,  F,    T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T },
/*IX*/    { T,  T,  T,  F,  F,  F,  F,    T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T },
/*SH*/    { T,  T,  F,  T,  F,  F,  F,    T,  F,  F,  T,  F,  F,  F,  F,  F,  F,  F,  F },
/*SIX*/   { T,  T,  F,  F,  F,  F,  F,    T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T },
/*UD*/    { T,  F,  F,  T,  F,  F,  F,    T,  F,  F,  T,  F,  F,  F,  F,  F,  F,  F,  F },
/*EX*/    { T,  F,  F,  F,  F,  F,  F,    F,  F,  F,  F,  F,  F,  F,  F,  F,  F,  F,  F },

/*NS*/    { T,  T,  T,  T,  T,  F,  F,    T,  F,  F,  T,  F,  F,  T,  T,  F,  T,  T,  F },
/*NU*/    { T,  T,  T,  T,  T,  F,  F,    T,  F,  F,  T,  F,  F,  T,  T,  F,  T,  F,  F },
/*NX*/    { T,  T,  T,  F,  T,  F,  F,    F,  F,  F,  T,  F,  F,  T,  F,  F,  T,  F,  F },
/*SN*/    { T,  T,  T,  T,  T,  F,  F,    T,  T,  T,  T,  T,  T,  F,  F,  F,  F,  F,  F },
/*SU*/    { T,  T,  T,  T,  T,  F,  F,    T,  F,  F,  T,  F,  F,  F,  F,  F,  F,  F,  F },
/*SX*/    { T,  T,  T,  F,  T,  F,  F,    F,  F,  F,  T,  F,  F,  F,  F,  F,  F,  F,  F },
/*UN*/    { T,  T,  T,  T,  T,  F,  F,    T,  T,  T,  T,  T,  T,  F,  F,  F,  F,  F,  F },
/*US*/    { T,  T,  T,  T,  T,  F,  F,    T,  F,  F,  T,  F,  F,  F,  F,  F,  F,  F,  F },
/*UX*/    { T,  T,  T,  F,  T,  F,  F,    F,  F,  F,  T,  F,  F,  F,  F,  F,  F,  F,  F },
/*XN*/    { T,  T,  T,  F,  T,  F,  F,    T,  T,  T,  F,  F,  F,  F,  F,  F,  F,  F,  F },
/*XS*/    { T,  T,  T,  F,  T,  F,  F,    T,  F,  F,  F,  F,  F,  F,  F,  F,  F,  F,  F },
/*XU*/    { T,  T,  T,  F,  T,  F,  F,    T,  F,  F,  F,  F,  F,  F,  F,  F,  F,  F,  F }
};

/*********************************************************************
 *
 *  Supremum Table (Page 467, Figure 8.6)
 *  (called Lock Conversion Matrix there)
 *
 *        supr[i][j] returns the supremum of two lock modes i, j.
 *
 *********************************************************************/
// NOTE Intent-lock + key range lock such as "IS + NX" are all "LL" (invalid) because it never happens
const lock_base_t::lmode_t lock_base_t::supr[NUM_MODES][NUM_MODES] = {
        //NL    IS    IX    SH    SIX   UD    EX     NS  NU  NX  SN  SU  SX  UN  US  UX  XN  XS  XU
/*NL*/  { NL,   IS,   IX,   SH,   SIX,  UD,   EX,    NS, NU, NX, SN, SU, SX, UN, US, UX, XN, XS, XU },
/*IS*/  { IS,   IS,   IX,   SH,   SIX,  UD,   EX,    LL, LL, LL, LL, LL, LL, LL, LL, LL, LL, LL, LL },
/*IX*/  { IX,   IX,   IX,   SIX,  SIX,  EX,   EX,    LL, LL, LL, LL, LL, LL, LL, LL, LL, LL, LL, LL },
/*SH*/  { SH,   SH,   SIX,  SH,   SIX,  UD,   EX,    SH, SU, SX, SH, SU, SX, US, US, UX, XS, XS, XU },
/*SIX*/ { SIX,  SIX,  SIX,  SIX,  SIX,  SIX,  EX,    LL, LL, LL, LL, LL, LL, LL, LL, LL, LL, LL, LL },
/*UD*/  { UD,   UD,   EX,   UD,   SIX,  UD,   EX,    UD, UD, UX, UD, UD, UX, UD, UD, UX, XU, XU, XU },
/*EX*/  { EX,   EX,   EX,   EX,   EX,   EX,   EX,    EX, EX, EX, EX, EX, EX, EX, EX, EX, EX, EX, EX },

/*NS*/  { NS,   LL,   LL,   SH,   LL,   US,   EX,    NS, NU, NX, SH, SU, SX, US, US, UX, XS, XS, XU },
/*NU*/  { NU,   LL,   LL,   SU,   LL,   UD,   EX,    NU, NU, NX, SU, SU, SX, UD, UD, UX, XU, XU, XU },
/*NX*/  { NX,   LL,   LL,   SX,   LL,   UX,   EX,    NX, NX, NX, SX, SX, SX, UX, UX, UX, EX, EX, EX },
/*SN*/  { SN,   LL,   LL,   SH,   LL,   UD,   EX,    SH, SU, SX, SN, SU, SX, UN, US, UX, XN, XS, XU },
/*SU*/  { SU,   LL,   LL,   SU,   LL,   UD,   EX,    SU, SU, SX, SU, SU, SX, UD, UD, UX, XU, XU, XU },
/*SX*/  { SX,   LL,   LL,   SX,   LL,   UX,   EX,    SX, SX, SX, SX, SX, SX, UX, UX, UX, EX, EX, EX },
/*UN*/  { UN,   LL,   LL,   US,   LL,   UD,   EX,    US, UD, UX, UN, UD, UX, UN, US, UX, XN, XS, XU },
/*US*/  { US,   LL,   LL,   US,   LL,   UD,   EX,    US, UD, UX, US, UD, UX, US, US, UX, XS, XS, XU },
/*UX*/  { UX,   LL,   LL,   UX,   LL,   UX,   EX,    UX, UX, UX, UX, UX, UX, UX, UX, UX, EX, EX, EX },
/*XN*/  { XN,   LL,   LL,   XS,   LL,   XU,   EX,    XS, XU, EX, XN, XU, EX, XN, XS, EX, XN, XS, XU },
/*XS*/  { XS,   LL,   LL,   XS,   LL,   XU,   EX,    XS, XU, EX, XS, XU, EX, XS, XS, EX, XS, XS, XU },
/*XU*/  { XU,   LL,   LL,   XU,   LL,   XU,   EX,    XU, XU, EX, XU, XU, EX, XU, XU, EX, XU, XU, XU },
};


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