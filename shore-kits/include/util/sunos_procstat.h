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

/** @file:   sunos_procstat.h
 *
 *  @brief:  Class that provides information about CPU usage in SunOS
 *
 *  @note:   Needs to compile with -lkstat -lrt
 *
 *  @author: Ippokratis Pandis (ipandis)
 *  @author: Ryan Johnson (ryanjohn)
 */

#ifndef __UTIL_SUNOS_PROCSTAT_H
#define __UTIL_SUNOS_PROCSTAT_H

#include "util/procstat.h"
#include "util/prcinfo.h"

#include <kstat.h>

struct kstat_entry 
{
    kstat_t*	ksp;

    long	    offset;
    cpu_measurement measured[2];
};


// SunOS process monitoring information 
class sunos_procmonitor_t : public procmonitor_t
{ 
private: 

    processinfo_t _prcinfo;

    kstat_ctl_t* _pkc;

    std::vector<kstat_entry> _entries;
    double _entries_sz;
    bool _first_time;
    long _last_measurement;
    long _new_measurement;
    cpu_measurement _totals;
    cpu_measurement _m;
    double _inuse;
    kstat_entry* _kse;
    int _kid;
    kstat_named_t* _kn;


protected:
    void _setup(const double interval_sec);
        
public:

    sunos_procmonitor_t(shore::ShoreEnv* env,
                        const double interval_sec = 1);
    ~sunos_procmonitor_t();

    // Hooks for the while loop
    w_rc_t case_setup();
    w_rc_t case_tick();
    w_rc_t case_reset();

    // PROCSTAT interface
    void print_stats();

    // Statistics -- INTERFACE

    void     stat_reset();

    double   get_avg_usage(bool bUpdateReading=true);
    void     print_avg_usage();
    void     print_ext_stats();
    ulong_t  iochars();
    uint     get_num_of_cpus();

    // for Measured and Offered CPU load
    cpu_load_values_t get_load();

}; // EOF: sunos_procmonitor_t

#endif /** __UTIL_SUNOS_PROCSTAT_H */
