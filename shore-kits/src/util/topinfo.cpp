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

/** @file:   topinfo.cpp
 * 
 *  @brief:  Implementation of a process information class.
 *           Implementation specific to Linux
 * 
 *  @author: Ippokratis Pandis (ipandis)
 *  @date:   July 2009
 */


#include "util/topinfo.h"
#include "util/trace.h"
#include <cassert>

topinfo_t::topinfo_t()
    : _reg_fs(0)
{
    _conf = glibtop_init();
    assert (_conf);
    TRACE( TRACE_DEBUG, "NCPU=(%d), REALCPU=(%d)\n",
           _conf->ncpu, _conf->real_ncpu);


    _mount_entries = glibtop_get_mountlist(&_mount_list,1);
    for (uint i=0; i< _mount_list.number; ++i) {
        TRACE( TRACE_DEBUG, "%d: %d %s %s %s\n",
               i, _mount_entries[i].dev, _mount_entries[i].devname,
               _mount_entries[i].mountdir, _mount_entries[i].type);
    }
}

topinfo_t::~topinfo_t()
{
}


// reset - stores the current info to the old_* variables
void topinfo_t::reset()
{
    update_cpu();
    _old_cpu = _cpu;

    update_mem();
    _old_mem = _mem;

    update_proclist();
    _old_proclist = _proclist;
}


// Prints all the stats it can get
void topinfo_t::print_stats()
{
    // print_avg_usage();
    // print_mem();
    // print_proclist();
}



/////////////////////////////////////////////////////////////////////////////
//                                                                         //
//  CPU - information about the CPU utilization                            //
//                                                                         //
/////////////////////////////////////////////////////////////////////////////

int topinfo_t::update_cpu()
{
    glibtop_get_cpu(&_cpu);
    return (0);
}


void topinfo_t::print_avg_usage()
{
    update_cpu();

    double cpus = _conf->ncpu+1;
    double total = (double) (_cpu.total-_old_cpu.total);
    double user  = (double) (_cpu.user-_old_cpu.user);
    double sys   = (double) (_cpu.sys-_old_cpu.sys);
    double idle  = (double) (_cpu.idle-_old_cpu.idle);

    //TRACE( TRACE_STATISTICS, "CPU TYPE INFO:\n" );
    TRACE( TRACE_STATISTICS, 
           "\nAvgCPUs: (%.1f) (%.1f%%)\n"                       \
           "AvgUser: (%.1f) (%.1f%%)\n"                         \
           "AvgSys : (%.1f) (%.1f%%)\n" ,
           (total-idle)*cpus/total, 100*(total-idle)/total,
           user*cpus/total, 100*user/total,
           sys*cpus/total, 100*sys/total);

    // store the read cpu info for the next iteration
    _old_cpu = _cpu;
}


double topinfo_t::get_avg_usage(bool bUpdateReading)
{
    update_cpu();

    double cpus = _conf->ncpu+1;
    double total = (double) (_cpu.total-_old_cpu.total);
    double idle  = (double) (_cpu.idle-_old_cpu.idle);

    // store the read cpu info for the next iteration
    if (bUpdateReading) {
        _old_cpu = _cpu;
    }

    return( (total-idle)*cpus/total );
}

uint topinfo_t::get_num_of_cpus()
{
    return(_conf->ncpu+1);
}

cpu_load_values_t topinfo_t::get_load()
{
    cpu_load_values_t load;
    load.run_tm = (double)  (_cpu.total-_old_cpu.total);
    load.wait_tm = 0.;
    return (load);
}


/////////////////////////////////////////////////////////////////////////////
//                                                                         //
//  MEMORY - information about the memory usage                            //
//                                                                         //
/////////////////////////////////////////////////////////////////////////////

int topinfo_t::update_mem()
{
    glibtop_get_mem(&_mem);
    return (0);
}


const int MBYTE = 1024*1024;

void topinfo_t::print_mem()
{
    TRACE( TRACE_STATISTICS, "MEMORY INFO:\n:" );
    TRACE( TRACE_STATISTICS, "Memory Total   : %ld MB\n" , 
           (ulong)_mem.total/MBYTE);
    TRACE( TRACE_STATISTICS, "Memory Used    : %ld MB\n" , 
           (ulong)_mem.used/MBYTE);
    TRACE( TRACE_STATISTICS, "Memory Free    : %ld MB\n" , 
           (ulong)_mem.free/MBYTE);
    TRACE( TRACE_STATISTICS, "Memory Buffered: %ld MB\n" , 
           (ulong)_mem.buffer/MBYTE);
    TRACE( TRACE_STATISTICS, "Memory Cached  : %ld MB\n" , 
           (ulong)_mem.cached/MBYTE);
    TRACE( TRACE_STATISTICS, "Memory User    : %ld MB\n" , 
           (ulong)_mem.user/MBYTE);
    TRACE( TRACE_STATISTICS, "Memory Locked  : %ld MB\n" , 
           (ulong)_mem.locked/MBYTE);
}



/////////////////////////////////////////////////////////////////////////////
//                                                                         //
//  PROCLIST - information about the processes in the system               //
//                                                                         //
/////////////////////////////////////////////////////////////////////////////


int topinfo_t::update_proclist()
{
    int which = 0;
    int arg = 0;
    glibtop_get_proclist(&_proclist,which,arg);
    return (0);
}


void topinfo_t::print_proclist()
{
    TRACE( TRACE_STATISTICS, "PROCLIST INFO:\n:" );
    TRACE( TRACE_STATISTICS, "Number: %ld\n" , (ulong)_proclist.number);
    TRACE( TRACE_STATISTICS, "Total : %ld\n" , (ulong)_proclist.total);
    TRACE( TRACE_STATISTICS, "Size  : %ld\n" , (ulong)_proclist.size);
}


/////////////////////////////////////////////////////////////////////////////
//                                                                         //
//  FS - file system information                                           //
//                                                                         //
/////////////////////////////////////////////////////////////////////////////

int topinfo_t::register_fs(const string& mountpath)
{
    // 1. First, check if already registered mount path
    for (mount_it mit = _v_reg_mount.begin(); 
         mit != _v_reg_mount.end(); ++mit) 
        {
            if (*mit == mountpath) {
                TRACE( TRACE_ALWAYS, 
                       "Try to register duplicate entry (%s) (%s)\n",
                       (*mit).c_str(), mountpath.c_str());
                return (-1);
            }
        }

    // 2. Second, add corresponding entry to the three vectors
    TRACE( TRACE_ALWAYS, "Registering (%s) for monitoring\n",
           mountpath.c_str());
    _v_reg_mount.push_back(mountpath);
    glibtop_fsusage afsusage;
    glibtop_get_fsusage(&afsusage,mountpath.c_str());
    _v_fs.push_back(afsusage);
    _v_old_fs.push_back(afsusage);
    
    ++_reg_fs;

    return (0);
}

int topinfo_t::update_fs()
{
    glibtop_fsusage afsusage;
    string mount;
    int i = 0;

    // Iterate over all registered mount paths
    for (mount_it mit = _v_reg_mount.begin();
         mit != _v_reg_mount.end(); ++mit)
        {
            mount = *mit;
            glibtop_get_fsusage(&afsusage,mount.c_str());
            _v_fs[i++] = afsusage;
        }

    return (0);
}


void topinfo_t::print_fs(const double delay)
{
    // 1. Make sure the three vectors have equal size
    assert ( _v_fs.size() == _v_old_fs.size());
    assert ( _reg_fs == _v_fs.size());

    update_fs();

    glibtop_fsusage afsusage, oldfsusage;
    guint32 bsz;
    guint64 bread, bwrite;

    // 2. Print info
    for (uint i=0; i<_reg_fs; ++i) {
        
        afsusage = _v_fs[i];
        oldfsusage = _v_old_fs[i];
        bsz = afsusage.block_size;
        bread = afsusage.read - oldfsusage.read;
        bwrite = afsusage.write - oldfsusage.write;
     
        TRACE( TRACE_STATISTICS, "Mount  : %s\tBlockSZ: %d\n", 
               _v_reg_mount[i].c_str(), bsz);
        TRACE( TRACE_STATISTICS, "Read   : %d (%.2fMB/sec)\n" , 
               bread, (double)(bsz*bread)/(double)(delay*1048576));
        TRACE( TRACE_STATISTICS, "Write  : %d (%.2fMB/sec)\n" , 
               bwrite, (double)(bsz*bwrite)/(double)(delay*1048576));
        
    }

    
    // Copies over the updated FSs
    assert (_v_fs.size() == _v_old_fs.size());
    int i = 0;
    int sz = _v_fs.size();

    for (i=0;i<sz;++i) {
        _v_old_fs[i] = _v_fs[i];
    }
}


ulong_t topinfo_t::iochars()
{
    // IP: TODO
    return (0);
}
