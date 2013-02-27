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

/** @file:   shore_worker.cpp
 *
 *  @brief:  Declaration of the worker threads in Shore
 *
 *  @author: Ippokratis Pandis (ipandis)
 */


#include "sm/shore/shore_worker.h"
#include "sm/shore/shore_env.h"

ENTER_NAMESPACE(shore);


#ifdef WORKER_VERBOSE_STATS
#warning Verbose worker statistics enabled
#endif

#ifdef WORKER_VERY_VERBOSE_STATS
#warning Very verbose worker statistics enabled (avg time waiting window)
#endif



/****************************************************************** 
 *
 * @fn:     print_stats(), reset()
 *
 * @brief:  Helper functions for the worker stats
 * 
 ******************************************************************/

void worker_stats_t::print_stats() const
{
    uint executed = _processed - _early_aborts;

    // How many requests were "touched" by this worker
    TRACE( TRACE_STATISTICS, "Processed       (%d)\n", _processed);

    // How many requests were actually processed (touched - early aborts)
    TRACE( TRACE_STATISTICS, "Executed        (%d) \t%.1f%%\n", 
           executed, (double)(100*executed)/(double)_processed);

    // Total (problems/touched) and real failure (problems/executed) rate 
    TRACE( TRACE_STATISTICS, "Failure rate    (%d) \t%.1f%% \t%.1f%%\n", 
           _problems, (double)(100*_problems)/(double)_processed, 
           (double)(100*_problems)/(double)executed);

    // How many packets were executed right from the input queue
    // If that value is ~100% it means that this worker is the bottleneck
    TRACE( TRACE_STATISTICS, "Input served    (%d) \t%.1f%%\n", 
           _served_input, (double)(100*_served_input)/(double)_processed);

    // How many packets had to wait on a lock before being executed 
    // If that value is ~100% it means that this worker is not the bottleneck
    // or/and that is has been assigned to few objects (e.g. partition small)
    TRACE( TRACE_STATISTICS, "Wait served     (%d) \t%.1f%%\n", 
           _served_waiting, (double)(100*_served_waiting)/(double)_processed);
    
    // How many requests were already aborted when they were checked by this worker
    TRACE( TRACE_STATISTICS, "Early aborts    (%d) \t%.1f%%\n", 
           _early_aborts, (double)(100*_early_aborts)/(double)_processed);

    // How many requests were aborted (by another worker) while this worker
    // was executing them
    TRACE( TRACE_STATISTICS, "Midway aborts   (%d) \t%.1f%% \t%.1f%%\n", 
           _mid_aborts, (double)(100*_mid_aborts)/(double)_processed,
           (double)(100*_mid_aborts)/(double)executed);

    // How many times this worker thread used the cond var to sleep
    // If that value is ~0% it means that this worker is the bottleneck. 
    // On the other hand, the larger the value it means the more sys time is added
    // to the system by this worker. One solution could be to assign more work to it
    TRACE( TRACE_STATISTICS, "Sleeped         (%d) \t%.1f%%\n", 
           _condex_sleep, (double)(100*_condex_sleep)/(double)_processed);

    // How many times this worker thread had decided to sleep but it didn't, 
    // because a new request arrived. Non-negligible value to this statistic means
    // that this worker is close to start waking-sleeping. Potentially, can be
    // assigned more work
    TRACE( TRACE_STATISTICS, "Failed sleep   (%d) \t%.1f%%\n", 
           _failed_sleep, (double)(100*_failed_sleep)/(double)_processed);


#ifdef WORKER_VERBOSE_STATS

    // Average time to execute an request
    TRACE( TRACE_STATISTICS, "Avg Action     (%.3fms) \t(%.3fms)\n", 
           _serving_total/(double)_processed, _serving_total/(double)executed);

    // Average time to execute an RVP per RVPs executed when this worker was
    // the last caller.
    TRACE( TRACE_STATISTICS, "Avg RVP exec   (%.3fms)\n", 
           _rvp_exec_time/(double)_rvp_exec);

    // Average time to notify after an RVP per RVPs executed when this worker was
    // the last caller
    TRACE( TRACE_STATISTICS, "Avg RVP notify (%.3fms)\n", 
           _rvp_notify_time/(double)_rvp_exec);
    
    // Average waiting time in queue per request
    TRACE( TRACE_STATISTICS, "Avg in queue   (%.3f)\n", _waiting_total/(double)_processed);

#ifdef WORKER_VERY_VERBOSE_STATS
    // Prints out the average waiting time window 
    for (int i=1; i<WAITING_WINDOW; i++) {
        TRACE( TRACE_STATISTICS, "(%d -> %d). Wait (%.2f)\n",
               i, i+1, _ww[(_ww_idx+i)%WAITING_WINDOW]);        
    }
#endif
#endif
}

worker_stats_t& 
worker_stats_t::operator+= (worker_stats_t const& rhs)
{
    _processed += rhs._processed;
    _problems  += rhs._problems;

    _served_input  += rhs._served_input;
    _served_waiting += rhs._served_waiting;

    _condex_sleep += rhs._condex_sleep;
    _failed_sleep += rhs._failed_sleep;

    _early_aborts += rhs._early_aborts;
    _mid_aborts += rhs._mid_aborts;

#ifdef WORKER_VERBOSE_STATS
    _waiting_total += rhs._waiting_total;
    _serving_total += rhs._serving_total;

    _rvp_exec        += rhs._rvp_exec;
    _rvp_exec_time   += rhs._rvp_exec_time;
    _rvp_notify_time += rhs._rvp_notify_time;
    
#ifdef WORKER_VERY_VERBOSE_STATS
    for (int i=0; i<WAITING_WINDOW; i++) _ww[i] += rhs._ww[i];
    _ww_idx += rhs._ww_idx;
    _last_change += rhs._last_change;
#endif
#endif

    return (*this);
}


void worker_stats_t::reset()
{
    _processed = 0;
    _problems  = 0;

    _served_input  = 0;
    _served_waiting  = 0;

    _condex_sleep = 0;
    _failed_sleep = 0;

    _early_aborts = 0;
    _mid_aborts = 0;

#ifdef WORKER_VERBOSE_STATS
    _waiting_total = 0;
    _serving_total = 0;

    _rvp_exec = 0;
    _rvp_exec_time = 0;
    _rvp_notify_time = 0;
    
#ifdef WORKER_VERY_VERBOSE_STATS
    for (int i=0; i<WAITING_WINDOW; i++) _ww[i] = 0;
    _ww_idx = 0;
    _last_change = 0;
#endif
#endif
}


#ifdef WORKER_VERBOSE_STATS

void worker_stats_t::update_waited(const double queue_time)
{
    _waiting_total += queue_time;
#ifdef WORKER_VERY_VERBOSE_STATS
    _last_change += _for_last_change.time(); 
    if (_last_change>1) {
        // if more than 1 sec passed since the last ww_idx change, move it
        _ww_idx = (_ww_idx + 1) % WAITING_WINDOW;
        _ww[_ww_idx] = 0;
        _last_change = 0;
    }
    // update waiting window (ww)
    _ww[_ww_idx] += queue_time;
#endif
}

void worker_stats_t::update_served(const double serve_time_ms)
{
    _serving_total += serve_time_ms;
}

void worker_stats_t::update_rvp_exec_time(const double rvp_exec_time_ms)
{
    _rvp_exec_time += rvp_exec_time_ms;
    ++_rvp_exec;
}

void worker_stats_t::update_rvp_notify_time(const double rvp_notify_time_ms)
{
    _rvp_notify_time += rvp_notify_time_ms;
}

#endif // WORKER_VERBOSE_STATS



/****************************************************************** 
 *
 * base_worker_t
 * 
 ******************************************************************/

/****************************************************************** 
 *
 * @fn:     work()
 *
 * @brief:  Worker thread entrance. Implements a simple state machine
 * 
 ******************************************************************/

void base_worker_t::work() 
{   
    ss_m::set_sli_enabled(_use_sli);
    int rval = 0;

    // state machine
    while (true) {
        switch (get_control()) {

        //     usleep(1000); // Sleep for a millisecond
        //     break;

        case (WC_PAUSED):
            if (work_PAUSED())
                return;
            break;

        case (WC_ACTIVE):

            _env->env_thread_init();

            // does the real work
            rval = work_ACTIVE();

            _env->env_thread_fini();

            if (rval)
                return;
            break;

        case (WC_STOPPED): // exits
            work_STOPPED();
            return;
            break;

        default:
            assert(0); // should not be in any other state
        }
    }
}






/****************************************************************** 
 *
 * @fn:     _work_PAUSED_impl()
 *
 * @brief:  Implementation of the PAUSED state
 *
 * @return: 0 on success
 * 
 ******************************************************************/

int base_worker_t::_work_PAUSED_impl()
{
    //    TRACE( TRACE_DEBUG, "Pausing...\n");

    // state (WC_PAUSED)

    // while paused loop and sleep
    while (get_control() == WC_PAUSED) {
        sleep(1);
    }
    return (0);
}


/****************************************************************** 
 *
 * @fn:     _work_STOPPED_impl()
 *
 * @brief:  Implementation of the STOPPED state. 
 *          (1) It stops, joins and deletes the next thread(s) in the list
 *          (2) It clears any internal state
 *
 * @return: 0 on success
 * 
 ******************************************************************/

int base_worker_t::_work_STOPPED_impl()
{
    //    TRACE( TRACE_DEBUG, "Stopping...\n");

    // state (WC_STOPPED)

    // 1. Abort any trx attached
    xct_t* ax = smthread_t::xct();
    if (ax) {
        abort_one_trx(ax);
        TRACE( TRACE_ALWAYS, "Aborted one\n");
    }


    // 2. Clears queue
    // Goes over all requests in the queue and aborts trxs
    _pre_STOP_impl();


    // 3. Propagate signal to next thread in the list, if any
    CRITICAL_SECTION(next_cs, _next_lock);
    if (_next) { 
        // if someone is linked behind stop it and join it 
        _next->stop();
        _next->join();
        TRACE( TRACE_DEBUG, "Next joined...\n");
        delete (_next);
    }        
    _next = NULL; // join() ?

    // any cleanup code should be here

    // 4. Print statistics
    // stats();

    return (0);
}



/****************************************************************** 
 *
 * @fn:     abort_one_trx()
 *
 * @brief:  Aborts the specific transaction
 *
 * @return: (true) on success
 * 
 ******************************************************************/

bool base_worker_t::abort_one_trx(xct_t* axct) 
{    
    assert (_env);
    assert (axct);
    smthread_t::me()->attach_xct(axct);
    w_rc_t e = ss_m::abort_xct();
    if (e.is_error()) {
        TRACE( TRACE_ALWAYS, "Xct abort failed [0x%x]\n", e.err_num());
        return (false);
    }    
    return (true);
}



/****************************************************************** 
 *
 * @fn:     stats()
 *
 * @brief:  Prints stats if at least 10 requests have been served
 * 
 ******************************************************************/

void base_worker_t::stats() 
{ 
    if (_stats._processed < MINIMUM_PROCESSED) {
        // don't print partitions which have served too few actions
        TRACE( TRACE_DEBUG, "(%s) few data\n", thread_name().data());
    }
    else {
        TRACE( TRACE_STATISTICS, "(%s)\n", thread_name().data());
        _stats.print_stats(); 
    }

    // clears after printing results
    _stats.reset();
}



worker_stats_t base_worker_t::get_stats() 
{ 
#if 0
    if (_stats._processed < MINIMUM_PROCESSED) {
        // don't print partitions which have served too few actions
        TRACE( TRACE_STATISTICS, "(%s) few data\n", thread_name().data());
    }
    else {
        TRACE( TRACE_STATISTICS, "(%s)\n", thread_name().data());
        _stats.print_stats(); 
    }       
#endif
    return (*&_stats); 
}





/****************************************************************** 
 *
 * @fn:     ACCESS_RECORD_TRACE
 *
 * @brief:  Tracing of record accesses
 * 
 ******************************************************************/

#ifdef ACCESS_RECORD_TRACE
#warning Tracing record accesses enabled
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
void base_worker_t::create_trace_dir(string dir)
{
    mkdir(dir.c_str(),0777);
}
void base_worker_t::open_trace_file()
{
    string dir = envVar::instance()->getVar("dir-trace","RAT");
    create_trace_dir(dir);
    time_t second = time(NULL);
    stringstream st;
    st << second;
    string fname = dir + string("/rat-") + st.str() + string("-") + this->thread_name() + string(".csv");
    TRACE( TRACE_ALWAYS, "Opening file (%s)\n", fname.c_str());
    _trace_file.open(fname.c_str());
}
void base_worker_t::close_trace_file()
{
    for (vector<string>::iterator it = _events.begin(); it != _events.end(); it++) {
        _trace_file << this->thread_name() << "," << *it << endl;
    }
    _trace_file.close();
    TRACE( TRACE_ALWAYS, "Closing file. (%d) events dumped\n", _events.size());
}
#endif // ACCESS_RECORD_TRACE


EXIT_NAMESPACE(shore);
