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

/** @file:   linux_procstat.h
 *
 *  @brief:  Class that provides information about CPU usage in Linux
 *
 *  @author: Ippokratis Pandis (ipandis)
 */

#ifndef __UTIL_LINUX_PROCSTAT_H
#define __UTIL_LINUX_PROCSTAT_H

#include "util/procstat.h"
#include "util/topinfo.h"

#include "k_defines.h"


// Linux CPU usage monitoring thread 
class linux_procmonitor_t : public procmonitor_t
{ 
private: 
    topinfo_t _topinfo;

protected:
    void _setup(const double interval_sec);
        
public:

    linux_procmonitor_t(shore::ShoreEnv* env, 
                        const double interval_sec = 1);
    ~linux_procmonitor_t();

    // procmonitor interface

    void     stat_reset();

    double   get_avg_usage(bool bUpdateReading=true);
    void     print_avg_usage();
    void     print_ext_stats();
    ulong_t  iochars();
    uint     get_num_of_cpus();

    cpu_load_values_t get_load();


}; // EOF: linux_procmonitor_t

#endif /** __UTIL_LINUX_PROCSTAT_H */
