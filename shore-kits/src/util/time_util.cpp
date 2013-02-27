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

/** @file time_util.cpp
 * 
 *  @brief Miscellaneous time-related utilities
 * 
 *  @author Ippokratis Pandis (ipandis)
 */

#include "util/time_util.h"

#include <cassert>

#include "k_defines.h"


/******************************************************************** 
 *
 *  @fn:     datepart
 *
 *  @brief:  Returns the date part of the time parameter
 *
 *  @return: A time_t value containing @time plus the weeks added.
 *
 ********************************************************************/

int datepart(char const* str, const time_t *pt)
{
    struct tm tm_struct;

    localtime_r(pt, &tm_struct);

    if(strcmp(str, "yy") == 0) {
        return (tm_struct.tm_year + 1900);
    }
  
    return 0;
}




/******************************************************************** 
 *
 *  @fn:     str_to_timet
 *
 *  @brief:  Converts a string in format YYYY-MM-DD to corresponding time_t object
 *
 *  @return: A time_t value containing @time plus the weeks added.
 *
 ********************************************************************/

time_t str_to_timet(char const* str) 
{
    // String in YYYY-MM-DD format
    tm time_str;
    int count = sscanf(str, "%d-%d-%d", 
                       &time_str.tm_year, &time_str.tm_mon, &time_str.tm_mday);
    assert(count == 3);
    time_str.tm_year -= 1900;
    time_str.tm_mon--;
    time_str.tm_hour = 0;
    time_str.tm_min = 0;
    time_str.tm_sec = 0;
    time_str.tm_isdst = -1;

    return mktime(&time_str);
}


/******************************************************************** 
 *
 *  @fn:     timet_to_str
 *
 *  @brief:  Converts a time_t object to a string with format YYYY-MM-DD
 *
 ********************************************************************/

void timet_to_str(char* dst, time_t time) 
{
    struct tm atm;
    localtime_r(&time, &atm);
    sprintf(dst, "%04d-%02d-%02d\n", 
            atm.tm_year+1900, atm.tm_mon+1, atm.tm_mday);
}





/******************************************************************** 
 *
 *  time_t manipulation functions.
 *
 *  @note The functions below use the Unix timezone functions like mktime()
 *         and localtime_r()
 *
 ********************************************************************/


// First day of the reformation, counted from 1 Jan 1
#define REFORMATION_DAY 639787

// They corrected out 11 days
#define MISSING_DAYS 11

// First day of reformation
#define THURSDAY 4

// Offset value; 1 Jan 1 was a Saturday
#define SATURDAY 6


/******************************************************************** 
 *
 *  @fn:     days_in_month
 *
 *  @brief:  Number of days in a month, using 0 (Jan) to 11 (Dec). 
 *           For leap years, add 1 to February (month 1). 
 *
 *  @return: A time_t value containing @time plus the weeks added.
 *
 ********************************************************************/

static const int days_in_month[12] = {
	31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };


/******************************************************************** 
 *
 *  @fn:     time_add_day
 *
 *  @brief:  Adds the given number of weeks to a time value.
 *
 *  @note:   Adds a day onto the time, using local time.
 *           Note that if clocks go forward due to daylight savings time, there
 *           are some non-existent local times, so the hour may be changed to 
 *           make it a valid time. This also means that it may not be wise to 
 *           keep calling time_add_day() to step through a certain period - if 
 *           the hour gets changed to make it valid time, any further calls to 
 *           time_add_day() will also return this hour, which may not be what 
 *           you want.
 *
 *  @return: A time_t value containing @time plus the days added.
 *
 ********************************************************************/

time_t 
time_add_day (time_t time, int days) 
{
    struct tm tm;

    localtime_r(&time, &tm);
    tm.tm_mday += days;
    tm.tm_isdst = -1;    
    time_t calendar_time = mktime (&tm);

    return calendar_time;
}


/******************************************************************** 
 *
 *  @fn:     time_add_week
 *
 *  @brief:  Adds the given number of weeks to a time value.
 *
 *  @return: A time_t value containing @time plus the weeks added.
 *
 ********************************************************************/

time_t 
time_add_week (time_t time, int weeks) 
{
    return time_add_day (time, weeks * 7);
}


/******************************************************************** 
 *
 *  @fn:     time_add_month
 *
 *  @brief:  Adds the given number of months to a time value.
 *
 *  @return: A time_t value containing @time plus the months added.
 *
 ********************************************************************/

time_t 
time_add_month (time_t time, int months) 
{
    struct tm tm;

    localtime_r(&time, &tm);
    tm.tm_mon += months;
    tm.tm_isdst = -1;

    return mktime(&tm);
}


/******************************************************************** 
 *
 *  @fn:     time_add_year
 *
 *  @brief:  Adds the given number of years to a time value.
 *
 *  @return: A time_t value containing @time plus the years added.
 *
 ********************************************************************/

time_t 
time_add_year (time_t time, int years) 
{
    struct tm tm;

    localtime_r(&time, &tm);
    tm.tm_year += years;
    tm.tm_isdst = -1;

    return mktime(&tm);
}


/******************************************************************** 
 *
 *  @fn:     time_day_begin
 *
 *  @brief:  Returns the start of the day, according to the local time.
 *
 *  @return: The time corresponding to the beginning of the day.
 *
 ********************************************************************/

time_t 
time_day_begin (time_t t) 
{
  struct tm tm;
  
  localtime_r(&t, &tm);
  tm.tm_hour = tm.tm_min = tm.tm_sec = 0;
  tm.tm_isdst = -1;
  
  return mktime (&tm);
}


/******************************************************************** 
 *
 *  @fn:     time_day_end
 *
 *  @brief:  Returns the end of the day, according to the local time.
 *
 *  @return: The time corresponding to the end of the day.
 *
 ********************************************************************/

time_t 
time_day_end (time_t t)
{
    struct tm tm;
    
  localtime_r(&t, &tm);
  tm.tm_hour = tm.tm_min = tm.tm_sec = 0;
  tm.tm_mday++;
  tm.tm_isdst = -1;
  
  return mktime (&tm);
}
