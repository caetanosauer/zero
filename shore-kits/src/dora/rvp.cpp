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

/** @file:   rvp.cpp
 *
 *  @brief:  Implementation of Rendezvous points for DORA
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */

#include "dora/rvp.h"
#include "dora/dora_env.h"

ENTER_NAMESPACE(dora);


/****************************************************************** 
 *
 * @class: rvp_t base rendez-vous class
 *
 ******************************************************************/

rvp_t::rvp_t() 
    : base_request_t() 
{ }

rvp_t::rvp_t(xct_t* axct, const tid_t& atid, const int axctid,
             const trx_result_tuple_t& presult, 
             const uint intra_trx_cnt, const uint total_actions) 
{ 
    _set(axct, atid, axctid, 
         presult, intra_trx_cnt, total_actions);
}


/****************************************************************** 
 *
 * @fn:    copying allowed
 *
 ******************************************************************/

rvp_t::rvp_t(const rvp_t& rhs)
    : base_request_t()
{
    _set(rhs._xct, rhs._tid, rhs._xct_id, rhs._result,
         rhs._countdown.remaining(), rhs._actions.size());
    copy_actions(rhs._actions);
}

rvp_t& rvp_t::operator=(const rvp_t& rhs)
{
    if (this != &rhs) {
        _set(rhs._xct,rhs._tid,rhs._xct_id,rhs._result,
             rhs._countdown.remaining(),rhs._actions.size());
        copy_actions(rhs._actions);
    }
    return (*this);
}

/****************************************************************** 
 *
 * @fn:    copy_actions()
 *
 * @brief: Initiates the list of actions of a rvp
 *
 ******************************************************************/

int rvp_t::copy_actions(const baseActionsList& actionList)
{
    _actions.reserve(actionList.size());
    _actions.assign(actionList.begin(),actionList.end()); // copy list content
    return (0);
}


/****************************************************************** 
 *
 * @fn:    append_actions()
 *
 * @brief: Appends a list of actions to the list of actions of a rvp
 *
 * @note:  Thread-safe
 *
 ******************************************************************/

int rvp_t::append_actions(const baseActionsList& actionList)
{
    CRITICAL_SECTION(action_cs, _actions_lock);
    // append new actionList to the end of the list
    _actions.insert(_actions.end(), actionList.begin(),actionList.end()); 
    return (0);
}


/****************************************************************** 
 *
 * @fn:    add_action()
 *
 * @brief: Adds an action to the list of actions of a rvp
 *
 ******************************************************************/

int rvp_t::add_action(base_action_t* paction) 
{
    assert (paction);
    assert (this==paction->rvp());
    _actions.push_back(paction);
    return (0);
}



/****************************************************************** 
 *
 * @fn:    run()
 *
 * @brief: Call the _run() and then gives back the object
 *
 ******************************************************************/

w_rc_t rvp_t::run()
{
    w_rc_t e = _run();
    giveback();
    return (e);
}


/******************************************************************** 
 *
 * @class: terminal_rvp_t
 *
 ********************************************************************/

terminal_rvp_t::terminal_rvp_t() 
    : rvp_t(), _db(NULL), _denv(NULL)
{ 
}

// terminal_rvp_t::terminal_rvp_t(ss_m* db, 
//                                DoraEnv* denv,
//                                xct_t* axct, 
//                                const tid_t& atid, 
//                                const int axctid, 
//                                trx_result_tuple_t &presult, 
//                                const int intra_trx_cnt, 
//                                const int total_actions) 
//     : rvp_t(axct, atid, axctid, presult, intra_trx_cnt, total_actions),
//       _db(db),_denv(denv)
// { 
//     assert(db);
// }

terminal_rvp_t::terminal_rvp_t(const terminal_rvp_t& rhs)
    : rvp_t(rhs)
{ 
    _db = rhs._db;
    _denv = rhs._denv;
}

terminal_rvp_t& terminal_rvp_t::operator=(const terminal_rvp_t& rhs)
{
    rvp_t::operator=(rhs);
    _db = rhs._db;
    _denv = rhs._denv;
    return (*this);
}

terminal_rvp_t::~terminal_rvp_t() 
{ 
}


/****************************************************************** 
 *
 * @fn:    notify_partitions()
 *
 * @brief: Notifies for any committed actions
 *
 ******************************************************************/

int terminal_rvp_t::notify_partitions()
{
#warning (IP) Perf. optimization --> Do not enqueue_committed your own action!

    for (baseActionsIt it=_actions.begin(); it!=_actions.end(); ++it) {
        (*it)->notify_own_partition();
    }
    return (_actions.size());
}

/****************************************************************** 
 *
 * @fn:    run()
 *
 * @brief: Wrapper for the _run() call
 *
 ******************************************************************/

w_rc_t terminal_rvp_t::run()
{
    return(_run());
}


/****************************************************************** 
 *
 * @fn:    _run()
 *
 * @brief: Code executed at terminal rendez-vous point
 *
 * @note:  The default action is to commit. However, it may be overlapped
 * @note:  There are hooks for updating the correct stats
 *
 ******************************************************************/

w_rc_t terminal_rvp_t::_run()
{
    assert (_db);
    assert (_denv);

    // attach to this xct
    assert (_xct);
    smthread_t::me()->attach_xct(_xct);

    // try to commit    
    w_rc_t rcdec;
    if (_decision == AD_ABORT) {

        // We cannot abort lazily because log rollback works on 
        // disk-resident records
        rcdec = _db->abort_xct();

        if (rcdec.is_error()) {
            TRACE( TRACE_ALWAYS, "Xct (%d) abort failed [0x%x]\n",
                   _tid.get_lo(), rcdec.err_num());
        }
        else {
            TRACE( TRACE_TRX_FLOW, "Xct (%d) aborted\n", _tid.get_lo());
            upd_aborted_stats();
        }

#ifdef CFG_FLUSHER
        notify_on_abort();
#endif
    }
    else {

#ifdef CFG_FLUSHER
        // DF1. Commit lazily
        lsn_t xctLastLsn;
        rcdec = _db->commit_xct(true,&xctLastLsn);
        set_last_lsn(xctLastLsn);
#else        
        rcdec = _db->commit_xct();    
#endif

        if (rcdec.is_error()) {
            TRACE( TRACE_ALWAYS, "Xct (%d) commit failed [0x%x]\n",
                   _tid.get_lo(), rcdec.err_num());
            upd_aborted_stats();
            w_rc_t eabort = _db->abort_xct();

            if (eabort.is_error()) {
                TRACE( TRACE_ALWAYS, "Xct (%d) abort failed [0x%x]\n",
                       _tid.get_lo(), eabort.err_num());
            }

#ifdef CFG_FLUSHER
            notify_on_abort();
#endif
        }
        else {
#ifdef CFG_FLUSHER
            // DF2. Enqueue to the "to flush" queue of DFlusher             
            _denv->enqueue_toflush(this);
#else
            (void)_denv;
            TRACE( TRACE_TRX_FLOW, "Xct (%d) committed\n", _tid.get_lo());
            upd_committed_stats();
#endif
        }
    }    

#ifndef CFG_FLUSHER
    // If DFlusher is disabled, then the last thing to do is to notify the
    // client. Otherwise, we will not notify the client here, but only after we 
    // know that the xct had been flushed
    notify_client();

    // In addition, if DFlusher is enabled, the notification of the partitions and the
    // giveback of the RVP will happen by the dflusher
    notify_partitions();
    giveback();
#endif

    return (rcdec);
}




/****************************************************************** 
 *
 * @fn:    notify_on_abort()
 *
 * @brief: Only if DFlusher is enabled. Notifies partitions (for actions) 
 *         and client about aborted xcts
 *
 * @CAREFUL: It calls giveback() so it should be the LAST
 *           time we touch this rvp
 *
 ******************************************************************/

void terminal_rvp_t::notify_on_abort()
{
    // If DFlusher is enabled
    // Do the notification of the committed actions and 
    // release the local locks here.

    // After we aborted the xct we need to notify the client and the partitions.
    // The notification of the commmitted xcts will be done by the DNotifier thread.

    notify_partitions();
    notify_client();

    TRACE( TRACE_TRX_FLOW, "Giving back aborted (%d)\n", _tid.get_lo());

    giveback();
}

EXIT_NAMESPACE(dora);
