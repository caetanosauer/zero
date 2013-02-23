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

/** @file:   linux_procstat.cpp
 *
 *  @brief:  Implementation of a class that provides information about
 *           the CPU usage during runs at Linux
 *
 *  @author: Ippokratis Pandis (ipandis)
 */

#include "util/linux_procstat.h"


// The Linux procmonitor is mostly just a wrapper for topinfo

linux_procmonitor_t::linux_procmonitor_t(shore::ShoreEnv* env,
                                         const double interval_sec)
    : procmonitor_t("linux-mon",env,interval_sec)
{ 
    _setup(interval_sec);
}


linux_procmonitor_t::~linux_procmonitor_t() 
{ 
}


void linux_procmonitor_t::_setup(const double interval_sec) 
{
    procmonitor_t::_setup(interval_sec);
}


// procmonitor interface

double linux_procmonitor_t::get_avg_usage(bool bUpdateReading)
{
    // Just return current reading, do not reset
    return (_topinfo.get_avg_usage(bUpdateReading));
}

void linux_procmonitor_t::print_avg_usage() 
{ 
    _topinfo.print_avg_usage();
}

void linux_procmonitor_t::print_ext_stats()
{
    _topinfo.print_stats();
}

ulong_t linux_procmonitor_t::iochars()
{
    return (_topinfo.iochars());
}

void linux_procmonitor_t::stat_reset()
{
    _topinfo.reset();
}

uint linux_procmonitor_t::get_num_of_cpus()
{
    return (_topinfo.get_num_of_cpus());
}

cpu_load_values_t linux_procmonitor_t::get_load()
{
    return (_topinfo.get_load());
}
