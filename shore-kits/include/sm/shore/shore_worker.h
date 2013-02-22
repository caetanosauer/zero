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

/** @file:   shore_worker.h
 *
 *  @brief:  Wrapper for Shore worker threads
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */


#ifndef __SHORE_WORKER_H
#define __SHORE_WORKER_H

#include <cstdio>


#include "k_defines.h"

#include "util.h"
#include "sm/shore/common.h"

ENTER_NAMESPACE(shore);


// Use this to enable verbode stats for worker threads
#undef WORKER_VERBOSE_STATS
//#define WORKER_VERBOSE_STATS

// ditto
#undef WORKER_VERY_VERBOSE_STATS
//#define WORKER_VERY_VERBOSE_STATS


// Define this flag to dump traces of record accesses
#undef ACCESS_RECORD_TRACE
//#define ACCESS_RECORD_TRACE

const int WAITING_WINDOW = 5; // keep track the last 5 seconds

// A worker needs to have processed at least 10 packets to print its own stats
const uint MINIMUM_PROCESSED = 10; 


/******************************************************************** 
 *
 * @struct: worker_stats_t
 *
 * @brief:  Worker statistics
 * 
 * @note:   No lock-protected. Assumes that only the assigned worker thread
 *          will modify it.
 *
 ********************************************************************/

struct worker_stats_t
{
    uint _processed;
    uint _problems;

    uint _served_input;
    uint _served_waiting;
    
    uint _condex_sleep;
    uint _failed_sleep;

    uint _early_aborts;
    uint _mid_aborts;

#ifdef WORKER_VERBOSE_STATS
    void update_served(const double serve_time_ms);
    double _serving_total;   // in msecs

    void update_rvp_exec_time(const double rvp_exec_time);
    void update_rvp_notify_time(const double rvp_notify_time);
    uint   _rvp_exec;
    double _rvp_exec_time;
    double _rvp_notify_time;

    void update_waited(const double queue_time);
    double _waiting_total; // not only the last WAITING_WINDOW secs

#ifdef WORKER_VERY_VERBOSE_STATS
    double _ww[WAITING_WINDOW];
    uint _ww_idx; // index on the ww (waiting window) ring
    double _last_change;
    stopwatch_t _for_last_change;
#endif 
#endif

    worker_stats_t() 
        : _processed(0), _problems(0),
          _served_input(0), _served_waiting(0),
          _condex_sleep(0), _failed_sleep(0),
          _early_aborts(0), _mid_aborts(0)
#ifdef WORKER_VERBOSE_STATS
        , _serving_total(0), 
          _rvp_exec(0), _rvp_exec_time(0), _rvp_notify_time(0), 
          _waiting_total(0)
#ifdef WORKER_VERY_VERBOSE_STATS
        , _ww_idx(0), _last_change(0)
#endif
#endif
    { }

    ~worker_stats_t() { }
      
    void print_stats() const;

    void reset();

    void print_and_reset() { print_stats(); reset(); }

    worker_stats_t& operator+=(worker_stats_t const& rhs);

}; // EOF: worker_stats_t



/******************************************************************** 
 *
 * @class: base_worker_t
 *
 * @brief: An smthread-based non-template abstract base class for the 
 *         Shore worker threads
 *
 * @note:  By default the worker thread is not bound to any processor.
 *         The creator of the worker thread needs to
 *         decide where and if it will bind it somewhere.
 * @note:  Implemented as a state machine.
 * 
 ********************************************************************/

class ShoreEnv;

class base_worker_t : public thread_t 
{
protected:

    // status variables
    volatile uint_t  _control;
    volatile eDataOwnerState _data_owner;
    uint_t volatile   _ws;

    // cond var for sleeping instead of looping after a while
    condex                   _notify;

    // data
    ShoreEnv*                _env;    

    // needed for linked-list of workers
    base_worker_t*           _next;
    tatas_lock               _next_lock;

    // statistics
    worker_stats_t _stats;

    // processor binding
    bool                     _is_bound;
    processorid_t            _prs_id;

    // sli
    int                      _use_sli;

    // states
    virtual int _work_PAUSED_impl();
    virtual int _work_ACTIVE_impl()=0;
    virtual int _work_STOPPED_impl();
    virtual int _pre_STOP_impl()=0;


    void _print_stats_impl() const;

public:

    base_worker_t(ShoreEnv* env, c_str tname, processorid_t aprsid, const int use_sli) 
        : thread_t(tname), 
          _control(WC_PAUSED), _data_owner(DOS_UNDEF), _ws(WS_UNDEF),
          _env(env),
          _next(NULL), _is_bound(false), _prs_id(aprsid), _use_sli(use_sli)
    {
    }

    virtual ~base_worker_t() { }    

    // access methods //

    // for the linked list
    void set_next(base_worker_t* apworker) {
        assert (apworker);
        CRITICAL_SECTION(next_cs, _next_lock);
        _next = apworker;
    }

    base_worker_t* get_next() { return (_next); }

    // data owner state
    void set_data_owner_state(const eDataOwnerState ados) {
        assert ((ados==DOS_ALONE)||(ados==DOS_MULTIPLE));
        atomic_swap(&_data_owner, ados);
    }

    bool is_alone_owner() { return (*&_data_owner==DOS_ALONE); }

    // @brief: Set working state
    // @note:  This function can be called also by other threads
    //         (other than the worker)
    inline uint_t set_ws(const uint_t new_ws) {
        uint_t old_ws = *&_ws;
        while (old_ws!=new_ws) {

            // Do not change WS, if it is already set to commit
            if ((old_ws==WS_COMMIT_Q)&&(new_ws!=WS_LOOP)) { return (old_ws); }
            
            // Update WS
            uint_t cur_ws = atomic_cas(&_ws,old_ws,new_ws);           
            if (cur_ws == old_ws) {
                // If cas successful, then wake up worker if sleeping
                if ((old_ws==WS_SLEEP)&&(new_ws!=WS_SLEEP)) { condex_wakeup(); }
                return (old_ws);
            }

            // Keep on trying
            // Update old_ws because in the meantime someone may have
            // changed the WS (and for example woke up the sleeping worker)
            old_ws = cur_ws;
        }
        return (old_ws);
    }

    inline uint_t get_ws() { return (*&_ws); }


    inline bool can_continue(const uint_t my_ws) {
        return ((*&_ws==my_ws)||(*&_ws==WS_LOOP));
    }

    inline bool is_sleeping(void) {
        return (*&_ws==WS_SLEEP);
    }
   


    // Condex functions in order to sleep/wakeup worker //

    // This function is called when the worker decides it is time to sleep
    inline int condex_sleep() { 
        // can sleep only if in WS_LOOP
        // (if on WS_COMMIT_Q or WS_INPUT_Q it means that a
        //  COMMIT or INPUT action was enqueued during this
        //  LOOP so there is no need to sleep).
        uint_t old_ws = *&_ws;
        while (old_ws==WS_LOOP) {
            uint_t cur_ws = atomic_cas_uint(&_ws,old_ws,(uint_t)WS_SLEEP);
            if (cur_ws == old_ws) {
                // If cas successful, then sleep
                _notify.wait();
                ++_stats._condex_sleep;
                return (1);
            }

            // Keep on trying
            old_ws = cur_ws;
        }
        ++_stats._failed_sleep;
        return (0);
    }


    // @note: The caller thread should have already changed the WS 
    //        before calling this function
    inline void condex_wakeup() {         
        //assert (*&_ws!=WS_SLEEP); 
        _notify.signal(); 
    }



    // working states //


    // thread control
    inline uint_t get_control() { return (*&_control); }

    inline bool set_control(const uint_t awc) {
        //
        // Allowed transition matrix:
        //
        // |------------------------------------------------|
        // |(new)    | PAUSED | ACTIVE | STOPPED | RECOVERY |
        // |(old)    |        |        |         |          |
        // |-------------------------------------|----------|
        // |PAUSED   |        |    Y   |    Y    |    Y     |
        // |ACTIVE   |   Y    |        |    Y    |    Y     |
        // |STOPPED  |        |        |    Y    |    Y     |
        // |RECOVERY |   Y    |    Y   |    Y    |    Y     |
        // |-------------------------------------|----------|
        //
        {
            if ((*&_control == WC_PAUSED) && 
                ((awc == WC_ACTIVE) || (awc == WC_STOPPED))) {
                atomic_swap(&_control, awc);
                return (true);
            }

            if ((*&_control == WC_ACTIVE) && 
                ((awc == WC_PAUSED) || (awc == WC_STOPPED))) {
                atomic_swap(&_control, awc);
                return (true);
            }

            // Can go to recovery at any point
            if ((*&_control == WC_RECOVERY) || (awc == WC_RECOVERY)) {
                atomic_swap(&_control, awc);
                return (true);
            }
        }
        TRACE( TRACE_DEBUG, "Not allowed transition (%d)-->(%d)\n",
               _control, awc);
        return (false);
    }
    
    // commands
    void stop() {    
        set_control(WC_STOPPED);
        if (is_sleeping()) _notify.signal();
    }

    void start() {
        set_control(WC_ACTIVE);
        if (is_sleeping()) _notify.signal();
    }

    void pause() {
        set_control(WC_PAUSED);
        if (is_sleeping()) _notify.signal();
    }        

    // state implementation
    inline int work_PAUSED()  { return (_work_PAUSED_impl());  }        
    inline int work_ACTIVE()  { return (_work_ACTIVE_impl());  }
    inline int work_STOPPED() { return (_work_STOPPED_impl()); }

    // thread entrance
    void work();

    // helper //    

    bool abort_one_trx(xct_t* axct);

    void stats(); 

    worker_stats_t get_stats();
    
    void reset_stats() { _stats.reset(); }


#ifdef ACCESS_RECORD_TRACE
    ofstream _trace_file;
    vector<string> _events;
    void create_trace_dir(string dir);
    void open_trace_file();
    void close_trace_file();
#endif

private:

    // copying not allowed
    base_worker_t(base_worker_t const &);
    void operator=(base_worker_t const &);
    
}; // EOF: base_worker_t

EXIT_NAMESPACE(shore);

#endif /** __SHORE_WORKER_H */

