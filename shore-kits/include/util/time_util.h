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

/** @file time_util.h
 * 
 *  @brief Miscellaneous time-related utilities
 * 
 *  @author Ippokratis Pandis (ipandis)
 */

#ifndef __TIME_UTIL_H
#define __TIME_UTIL_H

#include <time.h>

#include "k_defines.h"

#include <ctype.h>


// Time conversion functions

int    datepart(char const* str, const time_t *pt);
time_t str_to_timet(char const* str);
void   timet_to_str(char* dst, time_t time);



/** time_t manipulation functions
 *
 *  @note These function use the Unix timezone functions
 *        like mktime() and localtime_r()
 */

// Add or subtract a number of days, weeks or months
time_t time_add_day(time_t time, int days);
time_t time_add_week(time_t time, int weeks);
time_t time_add_month(time_t time, int months);
time_t time_add_year(time_t time, int years);

// Return the beginning or end of the day
time_t time_day_begin(time_t t);
time_t time_day_end(time_t t);



#endif // __TIME_UTIL_H

