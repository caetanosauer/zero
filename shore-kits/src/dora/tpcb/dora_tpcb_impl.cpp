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

/** @file:   dora_tpcb_impl.cpp
 *
 *  @brief:  DORA TPCB TRXs
 *
 *  @note:   Implementation of RVPs and Actions that synthesize (according to DORA)
 *           the TPCB trxs 
 *
 *  @author: Ippokratis Pandis (ipandis)
 *  @date:   July 2009
 */

#include "dora/tpcb/dora_tpcb_impl.h"
#include "dora/tpcb/dora_tpcb.h"

using namespace shore;
using namespace tpcb;


ENTER_NAMESPACE(dora);


/******************************************************************** 
 *
 * DORA TPCB ACCT_UPDATE
 *
 ********************************************************************/

DEFINE_DORA_FINAL_RVP_CLASS(final_au_rvp,acct_update);


/******************************************************************** 
 *
 * DORA TPCB ACCT_UPDATE ACTIONS
 *
 * (1) UPD-BR
 * (2) UPD-TE
 * (3) UPD-AC
 * (4) INS-HI 
 *
 ********************************************************************/


void upd_br_action::calc_keys()
{
    _down.push_back(_in.b_id);
}


w_rc_t upd_br_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Branch
    tuple_guard<branch_man_impl> prb(_penv->branch_man());
    rep_row_t areprow(_penv->branch_man()->ts());
    areprow.set(_penv->branch_desc()->maxsize()); 
    prb->_rep = &areprow;


    /* UPDATE Branches
     * SET    b_balance = b_balance + <delta>
     * WHERE  b_id = <b_id rnd>;
     *
     * plan: index probe on "B_IDX"
     */
    
    // 1. Probe Branches
    TRACE( TRACE_TRX_FLOW, "App: %d UA:b-idx-nl (%d)\n", _tid.get_lo(), _in.b_id);
    W_DO(_penv->branch_man()->b_idx_nl(_penv->db(), prb, _in.b_id));

    double total;
    prb->get_value(1, total);
    prb->set_value(1, total + _in.delta);
    
    // 2. Update tuple
    W_DO(_penv->branch_man()->update_tuple(_penv->db(), prb, NL));

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prb->print_tuple();
#endif

    return RCOK;
}



void upd_te_action::calc_keys()
{
    _down.push_back(_in.t_id);
}



w_rc_t upd_te_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Teller
    tuple_guard<teller_man_impl> prt(_penv->teller_man());
    rep_row_t areprow(_penv->teller_man()->ts());
    areprow.set(_penv->teller_desc()->maxsize()); 
    prt->_rep = &areprow;

    /* UPDATE Tellers
     * SET    t_balance = t_balance + <delta>
     * WHERE  t_id = <t_id rnd>;
     *
     * plan: index probe on "T_IDX"
     */
    
    // 1. Probe Tellers
    TRACE( TRACE_TRX_FLOW, "App: %d UA:t-idx-nl (%d)\n", _tid.get_lo(), _in.t_id);
    W_DO(_penv->teller_man()->t_idx_nl(_penv->db(), prt, _in.t_id));

    double total;
    prt->get_value(2, total);
    prt->set_value(2, total + _in.delta);
    
    // 2. Update tuple
    W_DO(_penv->teller_man()->update_tuple(_penv->db(), prt, NL));

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prt->print_tuple();
#endif

    return RCOK;
}



void upd_ac_action::calc_keys()
{
    _down.push_back(_in.a_id);
}



w_rc_t upd_ac_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Account
    tuple_guard<account_man_impl> pra(_penv->account_man());
    rep_row_t areprow(_penv->account_man()->ts());
    areprow.set(_penv->account_desc()->maxsize()); 
    pra->_rep = &areprow;

    /* UPDATE Accounts
     * SET    a_balance = a_balance + <delta>
     * WHERE  a_id = <a_id rnd>;
     *
     * plan: index probe on "A_IDX"
     */

    // 1. Probe Accounts
    TRACE( TRACE_TRX_FLOW, "App: %d UA:a-idx-nl (%d)\n", _tid.get_lo(), _in.a_id);
    W_DO(_penv->account_man()->a_idx_nl(_penv->db(), pra, _in.a_id));

    double total;
    pra->get_value(2, total);
    pra->set_value(2, total + _in.delta);

    // 2. Update tuple
    W_DO(_penv->account_man()->update_tuple(_penv->db(), pra, NL));

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    pra->print_tuple();
#endif

    return RCOK;
}



void ins_hi_action::calc_keys()
{
    _down.push_back(_in.a_id);
}



w_rc_t ins_hi_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // History
    tuple_guard<history_man_impl> prh(_penv->history_man());
    rep_row_t areprow(_penv->account_man()->ts());
    areprow.set(_penv->account_desc()->maxsize()); 
    prh->_rep = &areprow;

    /* INSERT INTO History
     * VALUES (<t_id>, <b_id>, <a_id>, <delta>, <timestamp>)
     */
    
    // 1. Insert tuple
    prh->set_value(0, _in.b_id);
    prh->set_value(1, _in.t_id);
    prh->set_value(2, _in.a_id);
    prh->set_value(3, _in.delta);
    prh->set_value(4, time(NULL));
#ifdef CFG_HACK
    prh->set_value(5, "padding"); // PADDING
#endif

    TRACE( TRACE_TRX_FLOW, "App: %d UA:ins-hi\n", _tid.get_lo());
    W_DO(_penv->history_man()->add_tuple(_penv->db(), prh, NL));

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prh->print_tuple();
#endif

    return RCOK;
}




EXIT_NAMESPACE(dora);
