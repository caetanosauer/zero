/* 
 *  Sccsid:     @(#)bcd2.h	2.1.8.1
 */

#ifndef SSB_DBGEN_BCD2_H
#define SSB_DBGEN_BCD2_H

#include "util/namespace.h"

ENTER_NAMESPACE(dbgenssb);

int bin_bcd2(long binary, long *low_res, long *high_res);
int bcd2_bin(long *dest, long bcd);
int bcd2_add(long *bcd_low, long *bcd_high, long addend);
int bcd2_sub(long *bcd_low, long *bcd_high, long subend);
int bcd2_mul(long *bcd_low, long *bcd_high, long multiplier);
int bcd2_div(long *bcd_low, long *bcd_high, long divisor);
long bcd2_mod(long *bcd_low, long *bcd_high, long modulo);
long bcd2_cmp(long *bcd_low, long *bcd_high, long compare);

EXIT_NAMESPACE(dbgenssb);

#endif
