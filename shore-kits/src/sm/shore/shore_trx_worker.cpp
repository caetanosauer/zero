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

/** @file:   shore_trx_worker.cpp
 *
 *  @brief:  Declaration of the worker threads in Baseline
 *           (specialization of the Shore workers)
 *
 *  @author: Ippokratis Pandis, Feb 2010
 */


#include "sm/shore/shore_trx_worker.h"
#include "sm/shore/shore_env.h"

ENTER_NAMESPACE(shore);


/****************************************************************** 
 *
 * @class: trx_worker_t
 *
 * @brief: Wrapper for the Baseline worker threads
 *
 ******************************************************************/

trx_worker_t::trx_worker_t(ShoreEnv* env, c_str tname, 
                           processorid_t aprsid,
                           const int use_sli) 
    : base_worker_t(env, tname, aprsid, use_sli)
{ 
    assert (env);
    _actionpool = new Pool(sizeof(Request*),REQUESTS_PER_WORKER_POOL_SZ);
    _pqueue = new Queue( _actionpool.get() );
}

trx_worker_t::~trx_worker_t() 
{ 
    _pqueue = NULL;
    _actionpool = NULL;
}


void trx_worker_t::init(const int lc) 
{
    _pqueue->setqueue(WS_INPUT_Q,this,lc,0);
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

int trx_worker_t::_work_ACTIVE_impl()
{    
    // bind to the specified processor
    _prs_id = PBIND_NONE;
    TRY_TO_BIND(_prs_id,_is_bound);

    w_rc_t e;
    Request* ar = NULL;

    // Check if signalled to stop
    while (get_control() == WC_ACTIVE) {
        
        // Reset the flags for the new loop
        ar = NULL;
        set_ws(WS_LOOP);

        // Dequeue a request from the (main) input queue
        // It will spin inside the queue or (after a while) wait on a cond var
        ar = _pqueue->pop();

        // Execute the particular request and deallocate it
        if (ar) {
            _serve_action(ar);
            ++_stats._served_input;

#ifndef CFG_FLUSHER
            _env->_request_pool.destroy(ar);
#endif
        }
    }
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

int trx_worker_t::_serve_action(Request* prequest)
{
    // Begin xct 
    // *** note: It used to attach but the clients no longer begin
    //           the xct in order the SLI to work 
    assert (prequest);
    //smthread_t::me()->attach_xct(prequest->_xct);
    tid_t atid;
    {
    w_rc_t e = _env->db()->begin_xct(atid);
    if (e.is_error()) {
        TRACE( TRACE_TRX_FLOW, "Problem beginning xct [0x%x]\n",
               e.err_num());
        ++_stats._problems;
        return (1);
    }
    }

    xct_t* pxct = smthread_t::me()->xct();
    assert (pxct);
    TRACE( TRACE_TRX_FLOW, "Begin (%d)\n", atid.get_lo());
    prequest->_xct = pxct;
    prequest->_tid = atid;
            
    // Serve request
    {
    w_rc_t e = _env->run_one_xct(prequest);
    if (e.is_error()) {
        TRACE( TRACE_TRX_FLOW, "Problem running xct (%d) (%d) [0x%x]\n",
               prequest->_tid.get_lo(), prequest->_xct_id, e.err_num());
        ++_stats._problems;
        return (1);
    }
    }

    // Update worker stats
    ++_stats._processed;    
    return (0);
}



/****************************************************************** 
 *
 * @fn:     _pre_STOP_impl()
 *
 * @brief:  Goes over all the requests in the two queues and aborts 
 *          any unprocessed request
 * 
 ******************************************************************/

int trx_worker_t::_pre_STOP_impl()
{
    Request* pr;
    int reqs_read  = 0;
    int reqs_write = 0;
    int reqs_abt   = 0;

    assert (_pqueue);

    // Go over the readers list
    for (; _pqueue->_read_pos != _pqueue->_for_readers->end(); _pqueue->_read_pos++) {
        pr = *(_pqueue->_read_pos);
        ++reqs_read;
        if (abort_one_trx(pr->_xct)) ++reqs_abt;
    }

    // Go over the writers list
    {
        CRITICAL_SECTION(q_cs, _pqueue->_lock);
        for (_pqueue->_read_pos = _pqueue->_for_writers->begin();
             _pqueue->_read_pos != _pqueue->_for_writers->end();
             _pqueue->_read_pos++) 
            {
                pr = *(_pqueue->_read_pos);
                ++reqs_write;
                if (abort_one_trx(pr->_xct)) ++reqs_abt;
            }
    }

    if ((reqs_read + reqs_write) > 0) {
        TRACE( TRACE_ALWAYS, "(%d) aborted before stopping. (%d) (%d)\n", 
               reqs_abt, reqs_read, reqs_write);
    }
    return (reqs_abt);
}


EXIT_NAMESPACE(shore);
