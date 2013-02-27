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

/** @file:   procstat.cpp
 *
 *  @brief:  Abstract class that provides information about
 *           the CPU usage during runs
 *
 *  @author: Ippokratis Pandis (ipandis)
 */

#include "util/procstat.h"

#include "util.h"

#include "sm/shore/shore_env.h"


/*********************************************************************
 *
 *  @class: cpu_measurement
 *
 *********************************************************************/

cpu_measurement& cpu_measurement::operator+=(cpu_measurement const &other) 
{
    timestamp += other.timestamp;
    cpu_nsec_idle += other.cpu_nsec_idle;
    return (*this);
}

cpu_measurement& cpu_measurement::operator-=(cpu_measurement const &other) 
{
    timestamp -= other.timestamp;
    cpu_nsec_idle -= other.cpu_nsec_idle;
    return (*this);
}

void cpu_measurement::clear() {
    timestamp = 0;
    cpu_nsec_idle = 0;
}

void cpu_measurement::set(uint64_t snaptime, uint64_t nsec_idle) 
{
    timestamp = snaptime;
    cpu_nsec_idle = nsec_idle;
}



/*********************************************************************
 *
 *  @class: procmonitor_t
 *
 *********************************************************************/

procmonitor_t::procmonitor_t(const char* name, 
                             shore::ShoreEnv* env,
                             const double interval_sec)
    : thread_t(name), _interval_usec(0), _interval_sec(interval_sec),
      _total_usage(0), _num_usage_readings(0), _avg_usage(0),
      _state(CPS_NOTSET), _env(env), _last_reading(0),
      _print_verbose(false)
{
}


procmonitor_t::~procmonitor_t()
{
    if (*&_state != CPS_NOTSET) {            
        pthread_mutex_destroy(&_mutex);
        pthread_cond_destroy(&_cond);
    }
}



/*********************************************************************
 *
 *  @fn:    setup
 *
 *  @brief: Sets the interval 
 *
 *********************************************************************/

void procmonitor_t::_setup(const double interval_sec) 
{
    // set interval
    assert (interval_sec>0);
    _interval_usec = int(interval_sec*1e6);

    if (interval_sec<1) {
        TRACE( TRACE_DEBUG, "CPU usage updated every (%0.3f) msec\n", 
               _interval_usec/1000.);
    }
    else {
	TRACE( TRACE_DEBUG, "CPU usage(updated every (%0.6f) sec\n", 
               _interval_usec/1000./1000);
    }

    // setup cond
    pthread_mutex_init(&_mutex, NULL);
    pthread_cond_init(&_cond, NULL);  

    _state = CPS_PAUSE;
}



/*********************************************************************
 *
 *  @fn:    print_load
 *
 *  @brief: Prints the average load
 *
 *********************************************************************/

void procmonitor_t::work()
{

    if (*&_state==CPS_NOTSET) _setup(_interval_sec);
    assert (*&_state!=CPS_NOTSET);

    // Hook
    case_setup();
    
    eCPS astate = *&_state;
    int error = 0;
    struct timespec start;

    pthread_mutex_lock(&_mutex);
    clock_gettime(CLOCK_REALTIME, &start);    

    struct timespec ts = start;    
    static long const BILLION = 1000*1000*1000;

    while (true) {

        astate = *&_state;
            
        switch (astate) {
        case (CPS_RUNNING):
        case (CPS_PAUSE):    // PAUSE behaves like RUNNING, but without recording data

            // Hook
            case_tick();
	if(_print_verbose)
	    print_verbose();
	else print_interval();

            // Update secs/usecs
            ts = start;
            ts.tv_nsec += _interval_usec*1000;
            if(ts.tv_nsec > BILLION) {
                ts.tv_nsec -= BILLION;
                ts.tv_sec++;
            }            

            // sleep periodically until next measurement
            while(true) {

                error = pthread_cond_timedwait(&_cond, &_mutex, &ts);
                clock_gettime(CLOCK_REALTIME, &start);
                if(start.tv_sec > ts.tv_sec || 
                   (start.tv_sec == ts.tv_sec && start.tv_nsec > ts.tv_nsec))
                    break;
            }
            start = ts;
            break;


        case (CPS_RESET):

            // Hook
            case_reset();

            // clear
            _total_usage = 0;
            _num_usage_readings = 0;
            _avg_usage = 0;
            _state = CPS_RUNNING;
            break;

        case (CPS_STOP):

            // Hook
            case_stop();
            return;

        case (CPS_NOTSET): 
        default:
            assert(0); // invalid value 
            break;
        }
    }
}



/*********************************************************************
 *
 *  @fn:    print_load
 *
 *  @brief: Prints the average load
 *
 *********************************************************************/

void procmonitor_t::print_load(const double delay)
{
    const cpu_load_values_t load = get_load();

    // Print only if got meaningful numbers
    if ((load.run_tm>0.) && (load.wait_tm>0.)) {
        TRACE( TRACE_ALWAYS, "\nCpuLoad = (%.2f)\nAbsLoad = (%.2f)\n", 
               (load.run_tm+load.wait_tm)/load.run_tm, load.run_tm/delay);
    }
}


/*********************************************************************
 *
 *  @fn:    print_interval_throughput
 *
 *  @brief: Prints the throughput during the last interval
 *
 *********************************************************************/

void procmonitor_t::print_interval()
{
    assert(_env);
    uint_t att = _env->get_trx_att();    
    if (_env->get_measure() == shore::MST_MEASURE) {
        TRACE( TRACE_STATISTICS, "(%.1f) (%.1f)\n", 
               get_avg_usage(false), 
               (double)(att-_last_reading)/_interval_sec);
    }
    _last_reading=att;
}



/*********************************************************************
 *
 *  Control
 *
 *  - Updates to the state variable
 *  - Signal handlers
 *
 *********************************************************************/

void procmonitor_t::cntr_reset()  
{ 
    stat_reset();
    _state = CPS_RESET; 
}

void procmonitor_t::cntr_pause()  
{ 
    _state = CPS_PAUSE; 
}

void procmonitor_t::cntr_resume() 
{ 
    _state = CPS_RUNNING; 
}

void procmonitor_t::cntr_stop()   
{ 
    _state = CPS_STOP; 
}


w_rc_t procmonitor_t::case_setup() 
{ 
    return (RCOK); 
}

w_rc_t procmonitor_t::case_reset() 
{ 
    return (RCOK); 
}

w_rc_t procmonitor_t::case_stop() 
{ 
    return (RCOK); 
}

w_rc_t procmonitor_t::case_tick() 
{ 
    return (RCOK); 
}


void procmonitor_t::setEnv(shore::ShoreEnv* env)
{
    assert (env);
    _env = env;
}


void procmonitor_t::set_print_verbose(bool print_verbose)
{
    _print_verbose = print_verbose;
}

void procmonitor_t::print_verbose()
{
    assert(_env);
    uint_t att = _env->get_trx_att();    
    if (_env->get_measure() == shore::MST_MEASURE) {
        TRACE( TRACE_STATISTICS, "(%.1f) (%.1f)\n", 
               get_avg_usage(false), 
               (double)(att-_last_reading)/_interval_sec);
	print_load(_interval_sec);
	print_ext_stats();
    }
    _last_reading=att;
}
