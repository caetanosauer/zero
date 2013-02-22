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

/** @file:   dora_tm1_xct.cpp
 *
 *  @brief:  Declaration of the DORA TM1 transactions
 *
 *  @author: Ippokratis Pandis (ipandis)
 *  @date:   Feb 2009
 */

#include "dora/tm1/dora_tm1_impl.h"
#include "dora/tm1/dora_tm1.h"

#include <algorithm>
#include <list>
#include <utility>

using namespace shore;
using namespace tm1;

using namespace std;

ENTER_NAMESPACE(dora);

typedef partition_t<int>   irpImpl; 


/********
 ******** Caution: The functions below should be invoked inside
 ********          the context of a smthread
 ********/


/******************************************************************** 
 *
 * TM1 DORA TRXS
 *
 * (1) The dora_XXX functions are wrappers to the real transactions
 * (2) The xct_dora_XXX functions are the implementation of the transactions
 *
 ********************************************************************/


/******************************************************************** 
 *
 * TM1 DORA TRXs Wrappers
 *
 * @brief: They are wrappers to the functions that execute the transaction
 *         body. Their responsibility is to:
 *
 *         1. Prepare the corresponding input
 *         2. Check the return of the trx function and abort the trx,
 *            if something went wrong
 *         3. Update the tm1 db environment statistics
 *
 ********************************************************************/


// --- without input specified --- //

DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTM1Env,get_sub_data);
DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTM1Env,get_new_dest);
DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTM1Env,get_acc_data);
DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTM1Env,upd_sub_data);
DEFINE_ALTER_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTM1Env,upd_sub_data,upd_sub_data_mix);
DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTM1Env,upd_loc);
DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTM1Env,ins_call_fwd);
DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTM1Env,del_call_fwd);

DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTM1Env,get_sub_nbr);

DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(DoraTM1Env,ins_call_fwd_bench);


// --- with input specified --- //

/******************************************************************** 
 *
 * DORA TM1 GET_SUB_DATA
 *
 ********************************************************************/

w_rc_t DoraTM1Env::dora_get_sub_data(const int xct_id, 
                                     trx_result_tuple_t& atrt, 
                                     get_sub_data_input_t& in,
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
    final_gsd_rvp* frvp = new_final_gsd_rvp(pxct,atid,xct_id,atrt);    

    // 4. Generate the actions
    r_sub_gsd_action* r_sub = new_r_sub_gsd_action(pxct,atid,frvp,in);

    // 5a. Decide about partition
    // 5b. Enqueue

    {        
        irpImpl* my_sub_part = decide_part(sub(),in._s_id);
        assert (my_sub_part);

        // SUB_PART_CS
        CRITICAL_SECTION(sub_part_cs, my_sub_part->_enqueue_lock);
        if (my_sub_part->enqueue(r_sub,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_SUB_GSD\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK); 
}



/******************************************************************** 
 *
 * DORA TM1 GET_NEW_DEST
 *
 ********************************************************************/
#ifdef TM1GND2

w_rc_t DoraTM1Env::dora_get_new_dest(const int xct_id, 
                                     trx_result_tuple_t& atrt, 
                                     get_new_dest_input_t& in,
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

    // 3. Setup the next RVP
    // PH1 consists of 1 action
    mid_gnd_rvp* rvp = new_mid_gnd_rvp(pxct,atid,xct_id,atrt,in,bWake);    

    // 4. Generate the action
    r_sf_gnd_action* r_sf = new_r_sf_gnd_action(pxct,atid,rvp,in);

    // 5a. Decide about partition
    // 5b. Enqueue
    {        
        irpImpl* my_sf_part = decide_part(sf(),in._s_id);

        // SF_PART_CS
        CRITICAL_SECTION(sf_part_cs, my_sf_part->_enqueue_lock);
        if (my_sf_part->enqueue(r_sf,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_SF\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }
    return (RCOK); 
}

#else

w_rc_t DoraTM1Env::dora_get_new_dest(const int xct_id, 
                                     trx_result_tuple_t& atrt, 
                                     get_new_dest_input_t& in,
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
    TRACE( TRACE_TRX_FLOW, "Begin (%d)\n", atid);

    xct_t* pxct = smthread_t::me()->xct();

    // 2. Detatch self from xct
    assert (pxct);
    smthread_t::me()->detach_xct(pxct);
    TRACE( TRACE_TRX_FLOW, "Detached from (%d)\n", atid);

    // 3. Setup the final RVP
    final_gnd_rvp* frvp = new_final_gnd_rvp(pxct,atid,xct_id,atrt);    

    // 4. Generate the actions
    r_sf_gnd_action* r_sf = new_r_sf_gnd_action(pxct,atid,frvp,in);
    r_cf_gnd_action* r_cf = new_r_cf_gnd_action(pxct,atid,frvp,in);

    // 5a. Decide about partition
    // 5b. Enqueue

    {        
        irpImpl* my_sf_part = decide_part(sf(),in._s_id);
        irpImpl* my_cf_part = decide_part(cf(),in._s_id);

        // SF_PART_CS
        CRITICAL_SECTION(sf_part_cs, my_sf_part->_enqueue_lock);
        if (my_sf_part->enqueue(r_sf,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_SF\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }

        // CF_PART_CS
        CRITICAL_SECTION(cf_part_cs, my_cf_part->_enqueue_lock);
        sf_part_cs.exit();
        if (my_cf_part->enqueue(r_cf,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_CF\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK); 
}
#endif



/******************************************************************** 
 *
 * DORA TM1 GET_ACC_DATA
 *
 ********************************************************************/

w_rc_t DoraTM1Env::dora_get_acc_data(const int xct_id, 
                                     trx_result_tuple_t& atrt, 
                                     get_acc_data_input_t& in,
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
    final_gad_rvp* frvp = new_final_gad_rvp(pxct,atid,xct_id,atrt);    

    // 4. Generate the actions
    r_ai_gad_action* r_ai = new_r_ai_gad_action(pxct,atid,frvp,in);

    // 5a. Decide about partition
    // 5b. Enqueue

    {        
        irpImpl* my_ai_part = decide_part(ai(),in._s_id);

        // AI_PART_CS
        CRITICAL_SECTION(ai_part_cs, my_ai_part->_enqueue_lock);
        if (my_ai_part->enqueue(r_ai,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_AI_GAD\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK); 
}



/******************************************************************** 
 *
 * DORA TM1 UPD_SUB_DATA
 *
 ********************************************************************/
#ifdef TM1USD2

w_rc_t DoraTM1Env::dora_upd_sub_data(const int xct_id, 
                                     trx_result_tuple_t& atrt, 
                                     upd_sub_data_input_t& in,
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

    // 3. Setup the next RVP
    // PH1 consists of 1 action
    mid_usd_rvp* rvp = new_mid_usd_rvp(pxct,atid,xct_id,atrt,in,bWake);    

    // 4. Generate the action
    upd_sf_usd_action* upd_sf = new_upd_sf_usd_action(pxct,atid,rvp,in);

    // 5a. Decide about partition
    // 5b. Enqueue
    {        
        irpImpl* my_sf_part = decide_part(sf(),in._s_id);

        // SF_PART_CS
        CRITICAL_SECTION(sf_part_cs, my_sf_part->_enqueue_lock);
        if (my_sf_part->enqueue(upd_sf,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_SF\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }
    return (RCOK); 
}

#else


w_rc_t DoraTM1Env::dora_upd_sub_data(const int xct_id, 
                                     trx_result_tuple_t& atrt, 
                                     upd_sub_data_input_t& in,
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
    TRACE( TRACE_TRX_FLOW, "Begin (%d)\n", atid);

    xct_t* pxct = smthread_t::me()->xct();

    // 2. Detatch self from xct
    assert (pxct);
    smthread_t::me()->detach_xct(pxct);
    TRACE( TRACE_TRX_FLOW, "Detached from (%d)\n", atid);

    // 3. Setup the final RVP
    final_usd_rvp* frvp = new_final_usd_rvp(pxct,atid,xct_id,atrt);    

    // 4. Generate the actions
    upd_sub_usd_action* upd_sub = new_upd_sub_usd_action(pxct,atid,frvp,in);
    upd_sf_usd_action* upd_sf = new_upd_sf_usd_action(pxct,atid,frvp,in);

    // 5a. Decide about partition
    // 5b. Enqueue

    {        
        irpImpl* my_sub_part = decide_part(sub(),in._s_id);
        irpImpl* my_sf_part = decide_part(sf(),in._s_id);

        // SUB_PART_CS
        CRITICAL_SECTION(sub_part_cs, my_sub_part->_enqueue_lock);
        if (my_sub_part->enqueue(upd_sub,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_SUB\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }

        // SF_PART_CS
        CRITICAL_SECTION(sf_part_cs, my_sf_part->_enqueue_lock);
        sub_part_cs.exit();
        if (my_sf_part->enqueue(upd_sf,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_SF\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK); 
}

#endif


/******************************************************************** 
 *
 * DORA TM1 UPD_SUB_DATA MIX
 *
 ********************************************************************/

w_rc_t DoraTM1Env::dora_upd_sub_data_mix(const int xct_id, 
					 trx_result_tuple_t& atrt, 
					 upd_sub_data_input_t& in,
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

    // 3. Setup the next RVP
    // PH1 consists of 1 action
    mid_usdmix_rvp* rvp = new_mid_usdmix_rvp(pxct,atid,xct_id,atrt,in,bWake);    

    // 4. Generate the action
    upd_sub_usdmix_action* upd_sub = new_upd_sub_usdmix_action(pxct,atid,rvp,in);

    // 5a. Decide about partition
    // 5b. Enqueue
    {        
        irpImpl* my_sub_part = decide_part(sub(),in._s_id);

        // SUB_PART_CS
        CRITICAL_SECTION(sub_part_cs, my_sub_part->_enqueue_lock);
        if (my_sub_part->enqueue(upd_sub,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_SUB\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }
    return (RCOK); 
}


/******************************************************************** 
 *
 * DORA TM1 UPD_LOC
 *
 ********************************************************************/

w_rc_t DoraTM1Env::dora_upd_loc(const int xct_id, 
                                trx_result_tuple_t& atrt, 
                                upd_loc_input_t& in,
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
    final_ul_rvp* frvp = new_final_ul_rvp(pxct,atid,xct_id,atrt);    

    // 4. Generate the actions
    upd_sub_ul_action* upd_sub = new_upd_sub_ul_action(pxct,atid,frvp,in);

    // 5a. Decide about partition
    // 5b. Enqueue

    {        
        irpImpl* my_sub_part = decide_part(sub(),in._s_id);

        // SUB_PART_CS
        CRITICAL_SECTION(sub_part_cs, my_sub_part->_enqueue_lock);
        if (my_sub_part->enqueue(upd_sub,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_SUB_UL\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK); 
}



/******************************************************************** 
 *
 * DORA TM1 INS_CALL_FWD
 *
 ********************************************************************/

w_rc_t DoraTM1Env::dora_ins_call_fwd(const int xct_id, 
                                     trx_result_tuple_t& atrt, 
                                     ins_call_fwd_input_t& in,
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
    
    // 3. Setup the next RVP
    // PH1 consists of 1 action
#ifdef TM1ICF2
    mid1_icf_rvp* rvp = new_mid1_icf_rvp(pxct,atid,xct_id,atrt,in,bWake);    
#else
    mid_icf_rvp* rvp = new_mid_icf_rvp(pxct,atid,xct_id,atrt,in,bWake);
#endif

    // 4. Generate the action
    r_sub_icf_action* r_sub = new_r_sub_icf_action(pxct,atid,rvp,in);

    // 5a. Decide about partition
    // 5b. Enqueue
    {        
        irpImpl* my_sub_part = decide_part(sub(),in._s_id);

        // SUB_PART_CS
        CRITICAL_SECTION(sub_part_cs, my_sub_part->_enqueue_lock);
        if (my_sub_part->enqueue(r_sub,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_SUB_ICF\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }
    return (RCOK); 
}


/******************************************************************** 
 *
 * DORA TM1 DEL_CALL_FWD
 *
 ********************************************************************/

w_rc_t DoraTM1Env::dora_del_call_fwd(const int xct_id, 
                                     trx_result_tuple_t& atrt, 
                                     del_call_fwd_input_t& in,
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
    
    // 3. Setup the next RVP
    // PH1 consists of 1 action
    mid_dcf_rvp* rvp = new_mid_dcf_rvp(pxct,atid,xct_id,atrt,in,bWake);    

    // 4. Generate the action
    r_sub_dcf_action* r_sub = new_r_sub_dcf_action(pxct,atid,rvp,in);

    // 6a. Decide about partition
    // 6b. Enqueue
    {        
        irpImpl* my_sub_part = decide_part(sub(),in._s_id);

        // SUB_PART_CS
        CRITICAL_SECTION(sub_part_cs, my_sub_part->_enqueue_lock);
        if (my_sub_part->enqueue(r_sub,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_SUB_DCF\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK); 
}



/******************************************************************** 
 *
 * DORA TM1 INS_CALL_FWD_BENCH
 *
 ********************************************************************/

w_rc_t DoraTM1Env::dora_ins_call_fwd_bench(const int xct_id, 
                                           trx_result_tuple_t& atrt, 
                                           ins_call_fwd_bench_input_t& in,
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
    
    // 3. Setup the next RVP
    // PH1 consists of 1 action
    final_icfb_rvp* rvp = new_final_icfb_rvp(pxct,atid,xct_id,atrt);

    // 4. Generate the actions
    r_sub_icfb_action* r_sub = new_r_sub_icfb_action(pxct,atid,rvp,in);
    i_cf_icfb_action* i_cf = new_i_cf_icfb_action(pxct,atid,rvp,in);

    // 5a. Decide about partition
    // 5b. Enqueue
    {        
        irpImpl* my_sub_part = decide_part(sub(),in._s_id);
        irpImpl* my_cf_part = decide_part(cf(),in._s_id);

        // SUB_PART_CS
        CRITICAL_SECTION(sub_part_cs, my_sub_part->_enqueue_lock);
        if (my_sub_part->enqueue(r_sub,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_SUB_ICFB\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }

        // CF_PART_CS
        CRITICAL_SECTION(cf_part_cs, my_cf_part->_enqueue_lock);
        sub_part_cs.exit();
        if (my_cf_part->enqueue(i_cf,bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing I_CF_ICFB\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }

    }
    return (RCOK); 
}



/******************************************************************** 
 *
 * DORA TM1 GET_SUB_NBR
 *
 * There are two version of the dora_get_sub_nbr transaction. 
 *
 * The first assumes that the system knows that there is 1-to-1 correspondence
 * between SUB_NBR and S_ID. Thus, it calculates the ranges of each probe and 
 * sends an action only to the partitions that should be involved. 
 *
 * The second treats the access to SUB_NBR_IDX as a secondary action.
 * Thus, the dispatcher makes only 1 probe on this secondary index and it gathers 
 * the S_IDs along with the RIDs. Then, it sorts the <S_ID,RID> pairs on S_ID.
 * Finally, it creates the corresponding actions and sends them to the appropriate
 * partitions. The executors will not probe the index again, instead they will
 * acquire the action locks and access the record directly using the RID.
 *
 ********************************************************************/

#ifndef USE_DORA_EXT_IDX

// Case 1: Assume that there is knowledge about the 1-to-1 correspondence between
//         SUB_NBR and S_ID
w_rc_t DoraTM1Env::dora_get_sub_nbr(const int xct_id, 
                                    trx_result_tuple_t& atrt, 
                                    get_sub_nbr_input_t& in,
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


    // Find out how many actions need to be produced
    uint intratrx = 1; 
    int inRange = get_rec_to_access(); // the range of the index scan
    int left = TM1_SUBS_PER_SF - (in._s_id % TM1_SUBS_PER_SF); // how many left in the first partition
    vector<uint> rangeVec;

    rangeVec.push_back(std::min(left,inRange));
    inRange -= left;

    uint start = in._s_id + left;
    uint step = 0;    

    while ((inRange > 0) && (start<TM1_SUBS_PER_SF*_scaling_factor)) {
        step = std::min(inRange,TM1_SUBS_PER_SF);
        rangeVec.push_back(step);
        intratrx++;
        inRange -= TM1_SUBS_PER_SF;
        start += step;
    }

    // Setup the final RVP with the correct intratrx
    final_gsn_rvp* frvp = new_final_gsn_rvp(pxct,atid,xct_id,atrt,intratrx);    
    
    // Generate and enqueue the actions
    for (vector<uint>::iterator vit = rangeVec.begin(); vit < rangeVec.end(); vit++) {
        in._range = *vit; // pass the info about the resized action
        r_sub_gsn_action* r_sub = new_r_sub_gsn_action(pxct,atid,frvp,in);
        
        // Find partition and enqueue
        {        
            irpImpl* my_sub_part = decide_part(sub(),in._s_id);

            // SUB_PART_CS
            CRITICAL_SECTION(sub_part_cs, my_sub_part->_enqueue_lock);
            if (my_sub_part->enqueue(r_sub,bWake)) {
                TRACE( TRACE_DEBUG, "Problem in enqueueing R_SUB_GSN\n");
                assert (0); 
                return (RC(de_PROBLEM_ENQUEUE));
            }
        }

        // Move to the start of the next partition, if that's "out of bounds" (larger than the 
        // largest s_id in the database, then stop
        in._s_id += *vit;

        // if (in._s_id >= (TM1_SUBS_PER_SF*_scaling_factor)) {
        //     TRACE( TRACE_ALWAYS, "Out of bounds (%d) (%d)\n", atid.get_lo(), in._s_id);
        // }        
    }

    return (RCOK); 
}

#else

bool compare_pairs( pair<int,rid_t> first, pair<int,rid_t> second )
{
    return (first.first < second.first);
}

void print_pair(const pair<int,rid_t>& aPair) 
{
    cout << aPair.first << " - " << aPair.second << endl;
}

// Case 2: Treat the access to the SUB_NBR_IDX as a secondary action. In that case, the index
//         contains also the S_ID, in order to know where to send the next action, which is
//         a direct access to the record via its RID.
w_rc_t DoraTM1Env::dora_get_sub_nbr(const int xct_id, 
                                    trx_result_tuple_t& atrt, 
                                    get_sub_nbr_input_t& in,
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
    w_rc_t e = RCOK;
    tid_t atid;   
    W_DO(_pssm->begin_xct(atid));
    TRACE( TRACE_TRX_FLOW, "Begin (%d)\n", atid.get_lo());
    xct_t* pxct = smthread_t::me()->xct();

    // 2. Do the SUB_NBR_IDX index probe

    in._range = get_rec_to_access(); // the range of the index scan
    list< pair<int,rid_t> > subsFound;
    int sid;
    rid_t rid;

    {
        // get Subscriber table tuple from the cache
        table_row_t* prsub = _psub_man->get_tuple();
        assert (prsub);

        rep_row_t areprow(_psub_man->ts());
        areprow.set(_psub_desc->maxsize()); 
        prsub->_rep = &areprow;

        rep_row_t lowrep(_psub_man->ts());
        rep_row_t highrep(_psub_man->ts());
        lowrep.set(_psub_desc->maxsize()); 
        highrep.set(_psub_desc->maxsize()); 

        bool eof;

        /* SELECT s_id, msc_location
         * FROM   Subscriber
         * WHERE  sub_nbr >= <sub_nbr rndstr as s1>
         * AND    sub_nbr <  (s1 + <range>);
         *
         * plan: index probe on "SUB_NBR_IDX"
         */

        { // make gotos safe

            // 1. Secondary index access to Subscriber using sub_nbr and range.
            //    This can be done by any thread, thus, it is done in SH-ared mode. 
            guard<index_scan_iter_impl<subscriber_t> > sub_iter;
            {
                index_scan_iter_impl<subscriber_t>* tmp_sub_iter;
                TRACE( TRACE_TRX_FLOW, 
                       "App: %d GSN:sub-nbr-idx-iter (%d) (%d)\n", 
                       atid.get_lo(), in._s_id, in._range);

                // Note: the access is done in SH (not NL)
                e = _psub_man->sub_get_idx_iter(_pssm, tmp_sub_iter, prsub, 
                                                lowrep,highrep,
                                                in._s_id, in._range,
                                                SH, false);
                if (e.is_error()) { goto done; }                   
                sub_iter = tmp_sub_iter;
            }

            // 2. Read all the returned records
            e = sub_iter->next(_pssm, eof, *prsub);
            if (e.is_error()) { goto done; }
                    
            while (!eof) {
                // Read the sid, which now is part of the index entry Key, in order 
                // to know where to route the particular action next. Also, read the 
                // rid, so that the following action to be a direct access.
                prsub->get_value(0, sid);
                rid = prsub->rid();
                subsFound.push_back( make_pair( sid, rid ) );
                
                TRACE( TRACE_TRX_FLOW, "App: %d GSN: read (%d)\n", 
                       atid.get_lo(), sid);

                e = sub_iter->next(_pssm, eof, *prsub);
                if (e.is_error()) { goto done; }                    
            }
        } // goto

    done:
        // give back the tuple
        _psub_man->give_tuple(prsub);
        if (e.is_error()) { return (e); }
    }    


    // 3. Done accessing the database from the dispatcher. Detatch self from xct
    assert (pxct);
    smthread_t::me()->detach_xct(pxct);
    TRACE( TRACE_TRX_FLOW, "Detached from (%d)\n", atid.get_lo());

    // 4. Sort the list of S_ID, RID pairs and iterate the list creating the 
    // corresponding actions

    if (subsFound.empty()) {
        TRACE( TRACE_DEBUG, "Index probe didn't return any record\n");
        return (RC(se_NO_CURRENT_TUPLE));
    }

    //    for_each(subsFound.begin(),subsFound.end(),print_pair);

    subsFound.sort(compare_pairs);

    //    for_each(subsFound.begin(),subsFound.end(),print_pair);

    // Find out how many actions need to be produced
    int start = subsFound.front().first;
    uint intratrx = 1; 
    int nextBound = start + (TM1_SUBS_PER_SF - (start % TM1_SUBS_PER_SF));
    in._pairs.clear();

    final_gsn_rvp* frvp = new_final_gsn_rvp(pxct,atid,xct_id,atrt,intratrx); 
    vector<r_sub_gsn_acc_action*> vecActions;    

    typedef list< pair<int,rid_t> >::iterator pair_iterator;
    for (pair_iterator it=subsFound.begin(); it!=subsFound.end(); it++) {
        sid = (*it).first;
        rid = (*it).second;

        // Check if crossed the boundary. If yes, then it creates the action,
        // and starts adding pairs to the vector of the new action.
        if ( sid >= nextBound ) {
            nextBound += TM1_SUBS_PER_SF;            
            vecActions.push_back( new_r_sub_gsn_acc_action(pxct,atid,frvp,in) );
            in._pairs.clear();
            ++intratrx;
        }

        in._pairs.push_back( make_pair(sid, rid) );
    }

    // Package the last action
    if (!in._pairs.empty()) {
        vecActions.push_back( new_r_sub_gsn_acc_action(pxct,atid,frvp,in) );        
    }

    // Set the correct intratrx to the final RVP
    frvp->resize(intratrx,intratrx);
    
    // Enqueue the actions
    typedef vector<r_sub_gsn_acc_action*>::iterator action_iterator;
    r_sub_gsn_acc_action* pa = NULL;
    for (action_iterator ait=vecActions.begin(); ait!=vecActions.end(); ait++) {
        pa = *ait;
        sid = pa->_in._pairs.back().first;
        
        // Find partition and enqueue
        {        
            irpImpl* my_sub_part = decide_part(sub(),sid);

            // SUB_PART_CS
            CRITICAL_SECTION(sub_part_cs, my_sub_part->_enqueue_lock);
            if (my_sub_part->enqueue(pa,bWake)) {
                TRACE( TRACE_DEBUG, "Problem in enqueueing R_SUB_GSN\n");
                assert (0); 
                return (RC(de_PROBLEM_ENQUEUE));
            }
        }
    }

    return (e); 
}
#endif

EXIT_NAMESPACE(dora);
