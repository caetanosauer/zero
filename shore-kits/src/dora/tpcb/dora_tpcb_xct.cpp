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

/** @file:   dora_tpcb_xct.cpp
 *
 *  @brief:  Declaration of the DORA TPCB transactions
 *
 *  @author: Ippokratis Pandis (ipandis)
 *  @date:   July 2009
 */

#include "dora/tpcb/dora_tpcb_impl.h"
#include "dora/tpcb/dora_tpcb.h"

using namespace shore;
using namespace tpcb;


ENTER_NAMESPACE(dora);


typedef partition_t<int>   irpImpl; 


/******** Exported functions  ********/


/********
 ******** Caution: The functions below should be invoked inside
 ********          the context of a smthread
 ********/


/******************************************************************** 
 *
 * TPCB DORA TRXS
 *
 * (1) The dora_XXX functions are wrappers to the real transactions
 * (2) The xct_dora_XXX functions are the implementation of the transactions
 *
 ********************************************************************/


/******************************************************************** 
 *
 * TPCB DORA TRXs Wrappers
 *
 * @brief: They are wrappers to the functions that execute the transaction
 *         body. Their responsibility is to:
 *
 *         1. Prepare the corresponding input
 *         2. Check the return of the trx function and abort the trx,
 *            if something went wrong
 *         3. Update the tpcb db environment statistics
 *
 ********************************************************************/


// --- without input specified --- //

DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTPCBEnv,acct_update);



// --- with input specified --- //

/******************************************************************** 
 *
 * DORA TPCB ACCT_UPDATE
 *
 ********************************************************************/

w_rc_t DoraTPCBEnv::dora_acct_update(const int xct_id, 
                                     trx_result_tuple_t& atrt, 
                                     acct_update_input_t& in,
                                     const bool bWake)
{
    if(_start_imbalance > 0 && !_bAlarmSet) {
	CRITICAL_SECTION(alarm_cs, _alarm_lock);
	if(!_bAlarmSet) {
	    alarm(_start_imbalance);
	    _bAlarmSet = true;
	}
    }
	
    // 1. Initiate transaction
    tid_t atid;   

    W_DO(_pssm->begin_xct(atid));
    TRACE( TRACE_TRX_FLOW, "Begin (%d)\n", atid.get_lo());

    xct_t* pxct = smthread_t::me()->xct();

    // 2. Detatch self from xct
    assert (pxct);
    smthread_t::me()->detach_xct(pxct);
    TRACE( TRACE_TRX_FLOW, "Detached from (%d)\n", atid.get_lo());

    // 3. Setup the final RVP
    final_au_rvp* frvp = new_final_au_rvp(pxct,atid,xct_id,atrt);

    // 4. Generate the actions
    upd_br_action* upd_br = new_upd_br_action(pxct,atid,frvp,in);
    upd_te_action* upd_te = new_upd_te_action(pxct,atid,frvp,in);
    upd_ac_action* upd_ac = new_upd_ac_action(pxct,atid,frvp,in);
    ins_hi_action* ins_hi = new_ins_hi_action(pxct,atid,frvp,in);

    // 5a. Decide about partition
    // 5b. Enqueue
    {
        // *** Reminder: The TPC-B records start their numbering from 0 ***
        irpImpl* my_br_part = decide_part(br(),in.b_id);
        assert (my_br_part);
        //        TRACE( TRACE_STATISTICS,"BR (%d) -> (%d)\n", in.b_id, my_br_part->part_id());

        irpImpl* my_te_part = decide_part(te(),in.t_id);
        assert (my_te_part);
        //        TRACE( TRACE_STATISTICS,"TE (%d) -> (%d)\n", in.t_id, my_te_part->part_id());

        irpImpl* my_ac_part = decide_part(ac(),in.a_id);
        assert (my_ac_part);
        //        TRACE( TRACE_STATISTICS,"AC (%d) -> (%d)\n", in.a_id, my_ac_part->part_id());

        irpImpl* my_hi_part = decide_part(hi(),in.t_id);
        assert (my_hi_part);
        //        TRACE( TRACE_STATISTICS,"HI (%d) -> (%d)\n", in.t_id, my_hi_part->part_id());
        

        // BR_PART_CS
        CRITICAL_SECTION(br_part_cs, my_br_part->_enqueue_lock);
        if (my_br_part->enqueue(upd_br,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_BR\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }

        // TE_PART_CS
        CRITICAL_SECTION(te_part_cs, my_te_part->_enqueue_lock);
        br_part_cs.exit();
        if (my_te_part->enqueue(upd_te,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_TE\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }

        // AC_PART_CS
        CRITICAL_SECTION(ac_part_cs, my_ac_part->_enqueue_lock);
        te_part_cs.exit();
        if (my_ac_part->enqueue(upd_ac,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_AC\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }

        // HI_PART_CS
        CRITICAL_SECTION(hi_part_cs, my_hi_part->_enqueue_lock);
        ac_part_cs.exit();
        if (my_hi_part->enqueue(ins_hi,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing INS_HI\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK); 
}


EXIT_NAMESPACE(dora);
