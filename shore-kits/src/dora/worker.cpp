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

/** @file:   worker.cpp
 *
 *  @brief:  The DORA worker threads.
 *
 *  @author: Ippokratis Pandis, Sept 2008
 */


#include "dora/worker.h"
#include "dora/action.h"
#include "dora/partition.h"
#include "dora/rvp.h"


ENTER_NAMESPACE(dora);



/******************************************************************** 
 *
 * @class: dora_worker_t
 *
 * @brief: The DORA worker thread
 * 
 ********************************************************************/

dora_worker_t::dora_worker_t(ShoreEnv* env, base_partition_t* apart, c_str tname,
                             processorid_t aprsid, const int use_sli) 
    : base_worker_t(env, tname, aprsid, use_sli),
      _partition(apart)
{ 
}

dora_worker_t::~dora_worker_t() 
{ 
}


// partition related
void dora_worker_t::set_partition(base_partition_t* apart) 
{
    assert (apart);
    _partition = apart;
}


base_partition_t* dora_worker_t::get_partition() 
{
    return (_partition);
}

int dora_worker_t::doRecovery() 
{ 
    return (_work_ACTIVE_impl()); 
}


int dora_worker_t::_pre_STOP_impl() 
{ 
    assert(_partition); 
    return (_partition->abort_all_enqueued()); 
}



/****************************************************************** 
 *
 * @fn:     _work_ACTIVE_impl()
 *
 * @brief:  Implementation of the ACTIVE state
 *
 * @return: 0 on success
 * 
 ******************************************************************/

int dora_worker_t::_work_ACTIVE_impl()
{    
    int binding = envVar::instance()->getVarInt("dora-cpu-binding",0);
    if (binding==0) _prs_id = PBIND_NONE;
    TRY_TO_BIND(_prs_id,_is_bound);

    // state (WC_ACTIVE)

    // Start serving actions from the partition
    base_action_t* apa = NULL;
    BaseActionPtrList actionReadyList;
    BaseActionPtrList actionPromotedList;
    actionReadyList.clear();
    actionPromotedList.clear();

    bool inRecovery = false;

    // Initiate the sdesc cache
    me()->alloc_sdesc_cache();

    // Check if signalled to stop
    while ((get_control() & WC_ACTIVE) || (inRecovery=(get_control() == WC_RECOVERY))) {
        
        // reset the flags for the new loop
        apa = NULL;
        set_ws(WS_LOOP);
        
        // committed actions

        // 2. first release any committed actions

        while (_partition->has_committed()) {           

            // 2a. get the first committed
            apa = _partition->dequeue_commit();
            assert (apa);
            TRACE( TRACE_TRX_FLOW, "Received committed (%d)\n", apa->tid().get_lo());

            
            // 2b. release the locks acquired for this action
            apa->trx_rel_locks(actionReadyList,actionPromotedList);
            TRACE( TRACE_TRX_FLOW, "Received (%d) ready\n", actionReadyList.size());

            // 2c. the action has done its cycle, and can be deleted
            apa->giveback();
            apa = NULL;

            // 2d. serve any ready to execute actions 
            //     (those actions became ready due to apa's lock releases)
            for (BaseActionPtrIt it=actionReadyList.begin(); it!=actionReadyList.end(); ++it) {
                _serve_action(*it);
                ++_stats._served_waiting;
            }

            // clear the two lists
            actionReadyList.clear();
            actionPromotedList.clear();
        }            

        if (inRecovery) {
            if (!_partition->has_input()) { goto loopexit; }
        }

        // new (input) actions

        // 3. dequeue an action from the (main) input queue

        // @note: it will spin inside the queue or (after a while) wait on a cond var

        apa = _partition->dequeue();

        // 4. check if it can execute the particular action
        if (apa) {
            TRACE( TRACE_TRX_FLOW, "Input trx (%d)\n", apa->tid().get_lo());
            if (apa->trx_acq_locks()) {
                // 4b. if it can acquire all the locks, 
                //     go ahead and serve this action
                _serve_action(apa);
                ++_stats._served_input;
            }
        }
    }

 loopexit:
    // Release sdesc cache before exiting
    me()->free_sdesc_cache();
    return (0);
}



/****************************************************************** 
 *
 * @fn:     _serve_action()
 *
 * @brief:  Executes an action, once this action is cleared to execute.
 *          That is, it assumes that the action has already acquired 
 *          all the required locks from its partition.
 * 
 ******************************************************************/

int dora_worker_t::_serve_action(base_action_t* paction)
{
    // 0. make sure that the action has all the keys it needs
    assert (paction);
    assert (paction->is_ready());

    bool is_error = false;
    w_rc_t e = RCOK;
    int r_code = 0;

    // 1. get pointer to rvp
    rvp_t* aprvp = paction->rvp();
    assert (aprvp);
    

#ifdef WORKER_VERBOSE_STATS
    // 1a. update verbose statistics
    _stats.update_waited(paction->waited());
#endif

    // 2. before attaching check if this trx is still active
    if (!aprvp->isAborted()) {

        // 3. attach to xct
        attach_xct(paction->xct());
        TRACE( TRACE_TRX_FLOW, "Attached to (%d)\n", paction->tid().get_lo());

#ifdef WORKER_VERBOSE_STATS
        stopwatch_t serving_time;
#endif
            
        // 4. serve action
        e = paction->trx_exec();

#ifdef WORKER_VERBOSE_STATS
        _stats.update_served(serving_time.time_ms());
#endif

        if (e.is_error()) {

            if (e.err_num() == de_MIDWAY_ABORT) {
                r_code = de_MIDWAY_ABORT;
                TRACE( TRACE_TRX_FLOW, "Midway abort (%d)\n", paction->tid().get_lo());
                ++_stats._mid_aborts;
            }
            else {

                TRACE( TRACE_TRX_FLOW, "Problem running xct (%d) [0x%x]\n",
                       paction->tid().get_lo(), e.err_num());
                
                is_error = true;
                r_code = de_WORKER_RUN_XCT;
                
                ++_stats._problems;
            }
        }          

        // 5. detach from trx
        TRACE( TRACE_TRX_FLOW, "Detaching from (%d)\n", paction->tid().get_lo());
        detach_xct(paction->xct());

    }
    else {
        r_code = de_EARLY_ABORT;
        TRACE( TRACE_TRX_FLOW, "Early abort (%d)\n", paction->tid().get_lo());
        ++_stats._early_aborts;
    }


#ifdef WORKER_VERBOSE_STATS
    stopwatch_t rvp_time;
#endif

    // 6. Finalize processing        
    if (aprvp->post(is_error)) {
        // Last caller
        // Execute the code of this rendez-vous point
        e = aprvp->run();            

#ifdef WORKER_VERBOSE_STATS
        _stats.update_rvp_exec_time(rvp_time.time_ms());
#endif

        if (e.is_error()) {
            TRACE( TRACE_ALWAYS, "Problem running rvp for xct (%d) [0x%x]\n",
                   paction->tid().get_lo(), e.err_num());
            r_code = de_WORKER_RUN_RVP;
        }

        // The final-rvp giveback and the client notification 
        // is done in the RVP->run()
        // If the xct is dirty and the DFlusher is enabled then
        // those will be done by the DFlusher.
        // Otherwise, they will be done inside the RVP. 
        aprvp = NULL;
    }

#ifdef WORKER_VERBOSE_STATS
        _stats.update_rvp_notify_time(rvp_time.time_ms());
#endif

    // 7. update worker stats
    ++_stats._processed;    

    return (r_code);
}



EXIT_NAMESPACE(dora);

