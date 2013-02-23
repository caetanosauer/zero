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

/** @file:   dora_tpcc_xct.cpp
 *
 *  @brief:  Declaration of the Shore DORA transactions
 *
 *  @author: Ippokratis Pandis, Sept 2008
 */

#include "dora/tpcc/dora_mbench.h"
#include "dora/tpcc/dora_payment.h"
#include "dora/tpcc/dora_order_status.h"
#include "dora/tpcc/dora_stock_level.h"
#include "dora/tpcc/dora_delivery.h"
#include "dora/tpcc/dora_new_order.h"

#include "dora/tpcc/dora_tpcc.h"

using namespace shore;
using namespace tpcc;


ENTER_NAMESPACE(dora);


typedef partition_t<int>   irpImpl; 


/******** Exported functions  ********/


/********
 ******** Caution: The functions below should be invoked inside
 ********          the context of a smthread
 ********/


/******************************************************************** 
 *
 * TPC-C DORA TRXS
 *
 * (1) The dora_XXX functions are wrappers to the real transactions
 * (2) The xct_dora_XXX functions are the implementation of the transactions
 *
 ********************************************************************/


/******************************************************************** 
 *
 * TPC-C DORA TRXs Wrappers
 *
 * @brief: They are wrappers to the functions that execute the transaction
 *         body. Their responsibility is to:
 *
 *         1. Prepare the corresponding input
 *         2. Check the return of the trx function and abort the trx,
 *            if something went wrong
 *         3. Update the tpcc db environment statistics
 *
 ********************************************************************/



// --- without input specified --- //

DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTPCCEnv,new_order);
DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTPCCEnv,payment);
DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTPCCEnv,order_status);
DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTPCCEnv,delivery);
DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTPCCEnv,stock_level);
DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTPCCEnv,mbench_wh);
DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTPCCEnv,mbench_cust);




// --- with input specified --- //

/******************************************************************** 
 *
 * DORA TPC-C NEW_ORDER
 *
 ********************************************************************/


w_rc_t DoraTPCCEnv::dora_new_order(const int xct_id,
                                   trx_result_tuple_t& atrt, 
                                   new_order_input_t& anoin,
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
    me()->detach_xct(pxct);
    TRACE( TRACE_TRX_FLOW, "Detached from (%d)\n", atid.get_lo());

    
    // IP: for now, cannot handle remote WHs
    //assert (anoin._all_local==1);

    // 3. Calculate intratrx and total
    int whid     = anoin._wh_id;
    
    // 4. Setup the midway RVP
    mid1_nord_rvp* midrvp = new_mid1_nord_rvp(pxct,atid,xct_id,atrt,anoin,bWake);

    // 5. Enqueue all the actions

    {
        // 5a. Generate the inputs
        no_item_nord_input_t anoitin;
        anoin.get_no_item_input(anoitin);
        
        // 5b. Generate the actions
        r_wh_nord_action* r_wh_nord = new_r_wh_nord_action(pxct,atid,midrvp,anoitin);
        irpImpl* my_wh_part = decide_part(whs(),whid);

        r_cust_nord_action* r_cust_nord = new_r_cust_nord_action(pxct,atid,midrvp,anoitin);
        irpImpl* my_cust_part = decide_part(cus(),whid);

        upd_dist_nord_action* upd_dist_nord = new_upd_dist_nord_action(pxct,atid,midrvp,anoitin);
        irpImpl* my_dist_part = decide_part(dis(),whid);

        r_item_nord_action* r_item_nord = new_r_item_nord_action(pxct,atid,midrvp,anoin);
        irpImpl* my_item_part = decide_part(ite(),whid);

        // WH_PART_CS
        CRITICAL_SECTION(wh_part_cs, my_wh_part->_enqueue_lock);

        if (my_wh_part->enqueue(r_wh_nord,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_WH_NORD\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }

        // DIST_PART_CS
        CRITICAL_SECTION(dist_part_cs, my_dist_part->_enqueue_lock);
        wh_part_cs.exit();

        if (my_dist_part->enqueue(upd_dist_nord,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_DIST_NORD\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }

        // CUST_PART_CS
        CRITICAL_SECTION(cust_part_cs, my_cust_part->_enqueue_lock);
        dist_part_cs.exit();

        if (my_cust_part->enqueue(r_cust_nord,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_CUST_NORD\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
                
        // ITEM_PART_CS
        CRITICAL_SECTION(item_part_cs, my_item_part->_enqueue_lock);
        cust_part_cs.exit();

        if (my_item_part->enqueue(r_item_nord,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_ITEM_NORD\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }
    return (RCOK); 
}



/******************************************************************** 
 *
 * DORA TPC-C PAYMENT
 *
 ********************************************************************/

w_rc_t DoraTPCCEnv::dora_payment(const int xct_id,
                                 trx_result_tuple_t& atrt, 
                                 payment_input_t& apin,
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
    me()->detach_xct(pxct);
    TRACE( TRACE_TRX_FLOW, "Detached from (%d)\n", atid.get_lo());

    // 3. Setup the next RVP
    // PH1 consists of 3 packets
    midway_pay_rvp* rvp = new_midway_pay_rvp(pxct,atid,xct_id,atrt,apin,bWake);
    
    // 3. Generate the actions    
    upd_wh_pay_action* pay_upd_wh     = new_upd_wh_pay_action(pxct,atid,rvp,apin);
    upd_dist_pay_action* pay_upd_dist = new_upd_dist_pay_action(pxct,atid,rvp,apin);
    upd_cust_pay_action* pay_upd_cust = new_upd_cust_pay_action(pxct,atid,rvp,apin);


    // For each action
    // 4a. Decide about partition
    // 4b. Enqueue
    //
    // All the enqueues should appear atomic
    // That is, there should be a total order across trxs 
    // (it terms of the sequence actions are enqueued)

    {
        int wh = apin._home_wh_id;

        // first, figure out to which partitions to enqueue
        irpImpl* my_wh_part   = decide_part(whs(),wh);
        irpImpl* my_dist_part = decide_part(dis(),wh);
        irpImpl* my_cust_part = decide_part(cus(),wh);

        // then, start enqueueing

        // WH_PART_CS
        CRITICAL_SECTION(wh_part_cs, my_wh_part->_enqueue_lock);
            
        if (my_wh_part->enqueue(pay_upd_wh,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing PAY_UPD_WH\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }

        // DIS_PART_CS
        CRITICAL_SECTION(dis_part_cs, my_dist_part->_enqueue_lock);
        wh_part_cs.exit();

        if (my_dist_part->enqueue(pay_upd_dist,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing PAY_UPD_DIST\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }

        // CUS_PART_CS
        CRITICAL_SECTION(cus_part_cs, my_cust_part->_enqueue_lock);
        dis_part_cs.exit();

        if (my_cust_part->enqueue(pay_upd_cust,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing PAY_UPD_CUST\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK); 
}



/******************************************************************** 
 *
 * DORA TPC-C ORDER_STATUS
 *
 ********************************************************************/

w_rc_t DoraTPCCEnv::dora_order_status(const int xct_id,
                                      trx_result_tuple_t& atrt, 
                                      order_status_input_t& aordstin,
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
    me()->detach_xct(pxct);
    TRACE( TRACE_TRX_FLOW, "Detached from (%d)\n", atid.get_lo());

    // 2. Setup the next RVP
    // PH1 consists of 1 packet
    mid1_ordst_rvp* mid1_rvp = new_mid1_ordst_rvp(pxct,atid,xct_id,atrt,aordstin,bWake);
    
    // 3. Generate the action
    r_cust_ordst_action* r_cust = new_r_cust_ordst_action(pxct,atid,mid1_rvp,aordstin);

    // For each action
    // 4a. Decide about partition
    // 4b. Enqueue

    {
        int wh = aordstin._wh_id;

        // first, figure out to which partitions to enqueue
        irpImpl* my_cust_part = decide_part(cus(),wh);

        // CUST_PART_CS
        CRITICAL_SECTION(cust_part_cs, my_cust_part->_enqueue_lock);
            
        if (my_cust_part->enqueue(r_cust,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing ORDST_R_CUST\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }
    return (RCOK); 
}



/******************************************************************** 
 *
 * DORA TPC-C DELIVERY
 *
 ********************************************************************/

w_rc_t DoraTPCCEnv::dora_delivery(const int xct_id,
                                  trx_result_tuple_t& atrt, 
                                  delivery_input_t& adelin,
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


    assert (0); // TODO!
#warning TODO:  DEL-CUST key should be 2 (WH|DI) and DEL-ORD  key should be 2 (WH|DI)


    // 2. Detatch self from xct
    assert (pxct);
    me()->detach_xct(pxct);
    TRACE( TRACE_TRX_FLOW, "Detached from (%d)\n", atid.get_lo());

    // 3. Setup the final RVP
    final_del_rvp* frvp = new_final_del_rvp(pxct,atid,xct_id,atrt);

    // 4. Setup the next RVP and actions
    // PH1 consists of DISTRICTS_PER_WAREHOUSE actions
    for (int i=0;i<DISTRICTS_PER_WAREHOUSE;i++) {
        // 4a. Generate an RVP
        //mid1_del_rvp* rvp = new_mid1_del_rvp(pxct,atid,xct_id,frvp->result(),adelin,bWake);
        mid1_del_rvp* rvp = NULL;
        assert (0); // IP: Not implemented
        rvp->postset(frvp,i);
    
        // 4b. Generate an action
        del_nord_del_action* del_del_nord = new_del_nord_del_action(pxct,atid,rvp,adelin);
        del_del_nord->postset(i);


        // For each action
        // 5a. Decide about partition
        // 5b. Enqueue
        //
        // All the enqueues should appear atomic
        // That is, there should be a total order across trxs 
        // (it terms of the sequence actions are enqueued)

        {
            int wh = adelin._wh_id;

            // first, figure out to which partitions to enqueue
            irpImpl* my_nord_part = decide_part(nor(),wh);

            // then, start enqueueing

            // NORD_PART_CS
            CRITICAL_SECTION(nord_part_cs, my_nord_part->_enqueue_lock);

            if (my_nord_part->enqueue(del_del_nord,bWake)) {
                TRACE( TRACE_DEBUG, "Problem in enqueueing DEL_DEL_NORD-%d\n", i);
                assert (0); 
                return (RC(de_PROBLEM_ENQUEUE));
            }
        }
    }

    return (RCOK); 
}



/******************************************************************** 
 *
 * DORA TPC-C STOCK LEVEL
 *
 ********************************************************************/

w_rc_t DoraTPCCEnv::dora_stock_level(const int xct_id,
                                     trx_result_tuple_t& atrt, 
                                     stock_level_input_t& astoin,
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
    me()->detach_xct(pxct);
    TRACE( TRACE_TRX_FLOW, "Detached from (%d)\n", atid.get_lo());

    // 3. Setup the next RVP
    // PH1 consists of 1 packet
    mid1_stock_rvp* rvp = new_mid1_stock_rvp(pxct,atid,xct_id,atrt,astoin,bWake);
    
    // 4. Generate the action
    r_dist_stock_action* stock_r_dist = new_r_dist_stock_action(pxct,atid,rvp,astoin);

    // For each action
    // 5a. Decide about partition
    // 5b. Enqueue
    //
    // All the enqueues should appear atomic
    // That is, there should be a total order across trxs 
    // (it terms of the sequence actions are enqueued)

    {
        int wh = astoin._wh_id;
        
        // first, figure out to which partitions to enqueue
        irpImpl* my_dist_part = decide_part(dis(),wh);

        // then, start enqueueing

        // DIS_PART_CS
        CRITICAL_SECTION(dis_part_cs, my_dist_part->_enqueue_lock);

        if (my_dist_part->enqueue(stock_r_dist,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing STOCK_R_DIST\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK); 
}




/******************************************************************** 
 *
 * DORA MBENCHES
 *
 ********************************************************************/

w_rc_t DoraTPCCEnv::dora_mbench_wh(const int xct_id, 
                                   trx_result_tuple_t& atrt, 
                                   mbench_wh_input_t& in,
                                   const bool bWake)
{
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
    final_mb_rvp* frvp = new_final_mb_rvp(pxct,atid,xct_id,atrt);

    // 4. Generate the actions
    upd_wh_mb_action* upd_wh = new_upd_wh_mb_action(pxct,atid,frvp,in);

    // For each action
    // 5a. Decide about partition
    // 5b. Enqueue
    //
    // All the enqueues should appear atomic
    // That is, there should be a total order across trxs 
    // (it terms of the sequence actions are enqueued)

    {        
        irpImpl* mypartition = decide_part(whs(),in._wh_id);

        // WH_PART_CS
        CRITICAL_SECTION(wh_part_cs, mypartition->_enqueue_lock);
        if (mypartition->enqueue(upd_wh,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_WH_MB\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK); 
}

w_rc_t DoraTPCCEnv::dora_mbench_cust(const int xct_id, 
                                     trx_result_tuple_t& atrt, 
                                     mbench_cust_input_t& in,
                                     const bool bWake)
{
    // 1. Initiate transaction
    tid_t atid;   

    W_DO(_pssm->begin_xct(atid));
    TRACE( TRACE_TRX_FLOW, "Begin (%d)\n", atid.get_lo());

    xct_t* pxct = smthread_t::me()->xct();

    // 2. Detatch self from xct
    assert (pxct);
    me()->detach_xct(pxct);
    TRACE( TRACE_TRX_FLOW, "Detached from (%d)\n", atid.get_lo());

    // 3. Setup the final RVP
    final_mb_rvp* frvp = new_final_mb_rvp(pxct,atid,xct_id,atrt);
    
    // 4. Generate the actions
    upd_cust_mb_action* upd_cust = new_upd_cust_mb_action(pxct,atid,frvp,in);

    // For each action
    // 5a. Decide about partition
    // 5b. Enqueue
    //
    // All the enqueues should appear atomic
    // That is, there should be a total order across trxs 
    // (it terms of the sequence actions are enqueued)

    {
        irpImpl* mypartition = decide_part(cus(),in._wh_id);
        
        // CUS_PART_CS
        CRITICAL_SECTION(cus_part_cs, mypartition->_enqueue_lock);
        if (mypartition->enqueue(upd_cust,bWake)) { 
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_CUST\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK); 
}


EXIT_NAMESPACE(dora);
