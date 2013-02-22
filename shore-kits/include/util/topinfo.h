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

/** @file:   topinfo.h
 * 
 *  @brief:  Processor usage information for Linux environments.
 *
 *  @note:   It needs the glibtop2-dev library installed!
 *
 *  @note:   There are two versions of the processor usage information.
 *           This one is specific for linux and needs the glibtop library.
 * 
 *  @author: Ippokratis Pandis
 *  @date:   July 2008
 */


#ifndef __UTIL_TOPINFO_H
#define __UTIL_TOPINFO_H


#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <errno.h>
#include <math.h>

#include "util/stopwatch.h"
#include "util/procstat.h"

#include <iostream>
#include <vector>

/*
 * @note: Taken from 
 * http://www.linuxforums.org/forum/linux-programming-scripting/23128-c-program-get-cpu-usage-free-total-ratio.html
 * 
 */

// This program is looking for CPU,Memory,Procs.
// Also at glibtop header there are a lot of useful functionality.

#include <glibtop.h>
#include <glibtop/cpu.h>
#include <glibtop/mem.h>
#include <glibtop/proclist.h>
#include <glibtop/fsusage.h>
#include <glibtop/mountlist.h>

using namespace std;

// Implemented as a Singleton class
struct topinfo_t 
{
private:

    glibtop*         _conf;

    glibtop_cpu      _cpu;
    glibtop_mem      _mem;
    glibtop_proclist _proclist;

    glibtop_cpu      _old_cpu;
    glibtop_mem      _old_mem;
    glibtop_proclist _old_proclist;

    glibtop_mountlist   _mount_list;
    glibtop_mountentry* _mount_entries;


    uint _reg_fs;
    vector<string> _v_reg_mount;
    vector<glibtop_fsusage> _v_fs;
    vector<glibtop_fsusage> _v_old_fs;

    typedef vector<string>::iterator mount_it;
    typedef vector<glibtop_fsusage>::iterator fsusage_it;

public:

    topinfo_t();
    ~topinfo_t();

    void print_stats();

    // CPU
    int    update_cpu();
    double get_avg_usage(bool bUpdateReading=true);
    void   print_avg_usage();
    uint   get_num_of_cpus();

    cpu_load_values_t get_load();


    // MEMORY
    int  update_mem();
    void print_mem();

    // PROCLIST
    int  update_proclist();
    void print_proclist();    

    // DISK
    int  register_fs(const string& mountpath);
    int  update_fs();
    void print_fs(const double delay);

    ulong_t iochars();

    
    void reset();

}; // EOF: topinfo_t


#endif // __UTIL_TOPINFO_H
