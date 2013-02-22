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

/** @file:   prcinfo.h
 * 
 *  @brief:  Processor usage information for SunOS
 * 
 *  @author: Ippokratis Pandis, Nov 2008
 */


/*
 * Prints all field resource, usage and microstat accounting fields
 */

#ifndef __UTIL_PRCINFO_H
#define __UTIL_PRCINFO_H

#include <sys/types.h>
#include <sys/time.h>

#include <procfs.h>

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <math.h>

#include "util/stopwatch.h"
#include "util/procstat.h"

#include "k_defines.h"

#include <iostream>

using namespace std;

struct processinfo_t 
{
    int _fd;

    prusage_t   _old_prusage;
    prusage_t   _prusage;
    stopwatch_t _timer;

    bool _print_at_exit;
    int _is_ok;

    uint _ncpus;

    processinfo_t(const bool printatexit = false);
    ~processinfo_t();

    // prints information and resets
    int reset();
    int print();
    ulong_t iochars();

    uint get_num_of_cpus();

    cpu_load_values_t get_load();

    static double trans(timestruc_t ats);

    static void hr_min_sec(char*, long);
    static void prtime(string label, timestruc_t* ts);
    static void prtime(string label, long long& delay);

    static void tsadd(timestruc_t* result, timestruc_t* a, timestruc_t* b);
    static void tssub(timestruc_t* result, timestruc_t* a, timestruc_t* b);

}; // EOF: processinfo_t

#endif /** __UTIL_PRCINFO_H */
