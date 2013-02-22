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

/** @file:   flusher.h
 *
 *  @brief:  The dora-flusher.
 *
 *  @author: Ippokratis Pandis, Feb 2010
 */

#include "dora/dflusher.h"
#include "xct.h"

ENTER_NAMESPACE(dora);


/******************************************************************** 
 *
 * @struct: dora_flusher_t
 * 
 ********************************************************************/

dora_flusher_t::dora_flusher_t(ShoreEnv* penv, 
                               c_str tname,
                               processorid_t aprsid, 
                               const int use_sli) 
    : flusher_t(penv, tname, aprsid, use_sli)
{ 
    _dora_toflush = new DoraQueue(_pxct_toflush_pool.get());
    assert (_dora_toflush.get());
    _dora_toflush->setqueue(WS_COMMIT_Q,this,2000,0);  // wake-up immediately, spin 2000

    _dora_flushing = new DoraQueue(_pxct_flushing_pool.get());
    assert (_dora_flushing.get());
    _dora_flushing->setqueue(WS_COMMIT_Q,this,0,0);  // wake-up immediately

    // Create and start the notifier
    fprintf(stdout, "Starting dora-notifier...\n");
    _notifier = new dora_notifier_t(_env, c_str("DNotifier"));
    assert(_notifier.get());
    _notifier->fork();
    _notifier->start();
}

dora_flusher_t::~dora_flusher_t() 
{ 
    // -- clear queues --
    // they better be empty by now
    assert (_dora_toflush->is_empty());
    _dora_toflush.done();

    assert (_dora_flushing->is_empty());
    _dora_flushing.done();
}



/****************************************************************** 
 *
 * @fn:     _check_waiting()
 *
 * @brief:  Checks the list of xcts waiting at the "to flush" queue 
 *
 * @return: 0 on success
 * 
 ******************************************************************/

int dora_flusher_t::_check_waiting(bool& bSleepNext, 
                                   const lsn_t& durablelsn, 
                                   lsn_t& maxlsn,
                                   uint& waiting)
{
    terminal_rvp_t* prvp = NULL;
    lsn_t xctlsn;

    // Check if there are xcts waiting at the "to flush" queue 
    while ((!_dora_toflush->is_empty()) || (bSleepNext)) {

        // Pop and read the xct info 
        prvp = _dora_toflush->pop();

        // The only way for pop() to return NULL is when signalled to stop
        if (prvp) {
            xctlsn = prvp->my_last_lsn();

            TRACE( TRACE_TRX_FLOW, 
                   "Xct (%d) lastLSN (%d) durableLSN (%d)\n",
                   prvp->tid().get_lo(), xctlsn.lo(), maxlsn.lo());

            // If the xct is already durable (had been flushed) then
            // send it directly back to the executor of its final-rvp
            if (durablelsn > xctlsn) {
                //_send_to_last_executor(prvp);
                _notifier->enqueue_tonotify(prvp);
                _stats.alreadyFlushed++;
            }
            else {
                // Otherwise, add the rvp to the syncing (in-flight) list,
                // and update statistics
                maxlsn = std::max(maxlsn,xctlsn);
                _dora_flushing->push(prvp,false);
                waiting++;
            }
        }

        bSleepNext = false;
        _stats.served++;
    }

    return (0);
}


/****************************************************************** 
 *
 * @fn:     _move_from_flushing()
 *
 * @brief:  Called when we know that everyone on the "flushing" queue
 *          is durable. In the baseline case, it notifies clients.
 *
 * @return: 0 on success
 * 
 ******************************************************************/

int dora_flusher_t::_move_from_flushing(const lsn_t& durablelsn)
{
    terminal_rvp_t* prvp = NULL;
    lsn_t xctlsn;

    while (!_dora_flushing->is_empty()) {
        prvp = _dora_flushing->pop();
        xctlsn = prvp->my_last_lsn();
        assert (xctlsn < durablelsn);
        //_send_to_last_executor(prvp);
        _notifier->enqueue_tonotify(prvp);
    }

    return (0); 
}



/****************************************************************** 
 *
 * @fn:     _pre_STOP_impl()
 *
 * @brief:  Operations done before the thread stops 
 *
 * @return: 0 on success
 * 
 ******************************************************************/

int dora_flusher_t::_pre_STOP_impl() 
{ 
    uint afterStop = 0;
    terminal_rvp_t* prvp = NULL;

    // Stop notifier
    _notifier->stop();
    _notifier->join();

    // Notify the clients and clean up queues
    // We don't need to notify the partitions about the actions because the
    // flusher is closing and the partition threads/objects may have already
    // been destructed. The correct would be to have some specific order in
    // the destruction of objects.
    while (!_dora_flushing->is_empty()) {
        ++afterStop;
        prvp = _dora_flushing->pop();
        prvp->notify_client();
        prvp->giveback();
    }

    while (!_dora_toflush->is_empty()) {
        ++afterStop;
        prvp = _dora_toflush->pop();
        prvp->notify_client();
        prvp->giveback();
    }

    if (afterStop>0) {
        TRACE( TRACE_ALWAYS, 
               "Xcts flushed at stop (%d)\n",
               afterStop);
    }

    return(0); 
}



/****************************************************************** 
 *
 * @fn:     Contruction/destruction of DNotifier
 *
 ******************************************************************/

dora_notifier_t::dora_notifier_t(ShoreEnv* env, 
                                 c_str tname,
                                 processorid_t aprsid, 
                                 const int use_sli) 
    : base_worker_t(env, tname, aprsid, use_sli)
{ 
    _pxct_tonotify_pool = new Pool(sizeof(xct_t*),FLUSHER_BUFFER_EXPECTED_SZ);
    _tonotify = new DoraQueue(_pxct_tonotify_pool.get());
    assert (_tonotify.get());
    _tonotify->setqueue(WS_COMMIT_Q,this,0,0);  // wake-up immediately
}

dora_notifier_t::~dora_notifier_t() 
{ 
    // -- clear queues --
    // they better be empty by now
    assert (_tonotify->is_empty());
    _tonotify.done();
    _pxct_tonotify_pool.done();
}


/****************************************************************** 
 *
 * @fn:     _work_ACTIVE_impl()
 *
 * @brief:  Implementation of the ACTIVE state for the notifier in
 *          DORA GroupCommit.
 *
 ******************************************************************/

int dora_notifier_t::_work_ACTIVE_impl()
{    
    envVar* ev = envVar::instance();
    int binding = ev->getVarInt("dora-cpu-binding",0);
    if (binding==0) _prs_id = PBIND_NONE;
    TRY_TO_BIND(_prs_id,_is_bound);

    terminal_rvp_t* prvp = NULL;

    // Check if signalled to stop
    while (get_control() == WC_ACTIVE) {        

        // Reset the flags for the new loop
        set_ws(WS_LOOP);
        
        // It will block if empty
        prvp = _tonotify->pop(); 

        if (prvp) {
            prvp->upd_committed_stats();
            prvp->notify_partitions();
            prvp->notify_client();
            prvp->giveback();
            prvp = NULL;
        }
    }
    return (0);
}



/****************************************************************** 
 *
 * @fn:     _pre_STOP_impl()
 *
 * @brief:  Operations done before the thread stops 
 *
 * @return: 0 on success
 * 
 ******************************************************************/

int dora_notifier_t::_pre_STOP_impl() 
{ 
    uint afterStop = 0;
    terminal_rvp_t* prvp = NULL;
    while (!_tonotify->is_empty()) {
        ++afterStop;

        prvp = _tonotify->pop();
        prvp->upd_committed_stats();
        // see comment at flusher.cpp:324 (at flusher::_pre_STOP_impl)
        prvp->notify_client();
        prvp->giveback();
        prvp = NULL;
    }

    if (afterStop>0) 
        TRACE( TRACE_ALWAYS, 
               "Xcts notified at stop (%d)\n",
               afterStop);
    return(0); 
}


EXIT_NAMESPACE(dora);
