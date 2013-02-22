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

/** @file:   sunos_procstat.cpp
 *
 *  @brief:  Implementation of a class that provides information about
 *           the CPU usage during runs at SunOS 
 *
 *  @author: Ippokratis Pandis (ipandis)
 *  @author: Ryan Johnson (ryanjohn)
 */

#include "util/sunos_procstat.h"
#include "util.h"

sunos_procmonitor_t::sunos_procmonitor_t(shore::ShoreEnv* env,
                                         const double interval_sec)
    : procmonitor_t("sunos-mon",env,interval_sec), 
      _pkc(NULL), _entries_sz(0),
      _first_time(true), _last_measurement(1), _new_measurement(0),
      _inuse(0), _kse(NULL), _kid(0), _kn(NULL)
{ 
    _setup(interval_sec);
}


sunos_procmonitor_t::~sunos_procmonitor_t() 
{ 
    if (*&_state != CPS_NOTSET) {            
        kstat_close(_pkc);
    }
}


//// Interface

void sunos_procmonitor_t::stat_reset()
{
    _prcinfo.reset();
}

double sunos_procmonitor_t::get_avg_usage(bool bUpdateReading) 
{ 
    return (*&_avg_usage); 
}

void sunos_procmonitor_t::print_avg_usage() 
{ 
    double au = *&_avg_usage;
    double entriessz = _entries.size();
    TRACE( TRACE_STATISTICS, 
           "\nAvgCPU:       (%.1f) (%.1f%%)\n",
           au, 100*(au/entriessz));
}

void sunos_procmonitor_t::print_ext_stats()
{
    _prcinfo.print();
}

ulong_t sunos_procmonitor_t::iochars()
{
    return (_prcinfo.iochars());
}

uint sunos_procmonitor_t::get_num_of_cpus()
{
    return (_prcinfo.get_num_of_cpus());
}

cpu_load_values_t sunos_procmonitor_t::get_load()
{
    return (_prcinfo.get_load());
}


//// Private functions 

void sunos_procmonitor_t::_setup(const double interval_sec) 
{
    procmonitor_t::_setup(interval_sec);

    _entries.clear();
    _entries.reserve(64); 
    
    // get set up
    // - open the kstat
    // - find and stash all the cpu::sys kstats
    // - find and stash the offset of the cpu::sys:cpu_nsec_idle entry
    // - take the first measurement

    _pkc = kstat_open();
    for(kstat_t* ksp=_pkc->kc_chain; ksp; ksp=ksp->ks_next) {
	if(strcmp(ksp->ks_module, "cpu") == 0 && strcmp(ksp->ks_name, "sys") == 0) {
	    int kid = kstat_read(_pkc, ksp, 0);
	    kstat_named_t* kn = (kstat_named_t*) ksp->ks_data;
	    for(long i=0; i < ksp->ks_ndata; i++) {
		if(strcmp(kn[i].name, "cpu_nsec_idle") == 0) {
		    kstat_entry entry = {ksp, i, {{ksp->ks_snaptime, kn->value.ui64}, {0, 0}}};
		    _entries.push_back(entry);
		    break;
		}
	    }
	}
    }
}



w_rc_t sunos_procmonitor_t::case_setup()
{
    _first_time = true;
    _last_measurement = 1;    
    _new_measurement = 0;
    _totals.set(0,0);
    _m.set(0,0);
    _inuse = 0;        
    _kse = NULL;
    _kid = 0;
    _kn = NULL;

    _entries_sz = _entries.size();
    assert (_pkc);

    return (RCOK);
}

w_rc_t sunos_procmonitor_t::case_tick()
{            
    _new_measurement = 1 - _last_measurement;

    // get the new measurement
    _totals.clear();

    for(int i=0; i<_entries_sz; i++) {
        _kse = &_entries[i];
        _kid = kstat_read(_pkc, _kse->ksp, 0);
        _kn = ((kstat_named_t*) _kse->ksp->ks_data) + _kse->offset;
        _m.set(_kse->ksp->ks_snaptime, _kn->value.ui64);
        _kse->measured[_new_measurement] = _m;
        _m -= _kse->measured[_last_measurement];
        _totals += _m;
    }

    // record usage if not paused
    if ((!_first_time) && (*&_state==CPS_RUNNING)) {
        _inuse = _entries_sz - _entries_sz*_totals.cpu_nsec_idle/_totals.timestamp;

        // update total usage and calculate average
        ++_num_usage_readings;
        _total_usage += _inuse;
        _avg_usage = _total_usage/_num_usage_readings; 
    }

    _first_time = false;
    _last_measurement = _new_measurement;

    return (RCOK);
}


w_rc_t sunos_procmonitor_t::case_reset()
{
    _first_time=true;
    _last_measurement = 1;
    return (RCOK);    
}
