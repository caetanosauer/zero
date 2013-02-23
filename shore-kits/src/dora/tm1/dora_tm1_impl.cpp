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

/** @file:   dora_tm1_impl.cpp
 *
 *  @brief:  DORA TM1 TRXs
 *
 *  @note:   Implementation of RVPs and Actions that synthesize (according to DORA)
 *           the TM1 trxs 
 *
 *  @author: Ippokratis Pandis, Feb 2009
 */

#include "dora/tm1/dora_tm1_impl.h"
#include "dora/tm1/dora_tm1.h"

using namespace shore;
using namespace tm1;


ENTER_NAMESPACE(dora);

typedef partition_t<int>   irpImpl; 


/******************************************************************** 
 *
 * DORA TM1 GET_SUB_DATA
 *
 ********************************************************************/

DEFINE_DORA_FINAL_RVP_CLASS(final_gsd_rvp,get_sub_data);


/******************************************************************** 
 *
 * DORA TM1 GET_SUB_DATA ACTIONS
 *
 * (1) R-SUB
 *
 ********************************************************************/


void r_sub_gsd_action::calc_keys()
{
    set_read_only();
    _down.push_back(_in._s_id);
}



w_rc_t r_sub_gsd_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Subscriber
    tuple_guard<sub_man_impl> prsub(_penv->sub_man());
    rep_row_t areprow(_penv->sub_man()->ts());
    areprow.set(_penv->sub_desc()->maxsize()); 
    prsub->_rep = &areprow;
    
    /* SELECT s_id, sub_nbr, 
     *        bit_XX, hex_XX, byte2_XX,
     *        msc_location, vlr_location
     * FROM   Subscriber
     * WHERE  s_id = <s_id>
     *
     * plan: index probe on "S_IDX"
     */

    // 1. retrieve Subscriber (read-only)
    TRACE( TRACE_TRX_FLOW, "App: %d GSD:sub-idx-nl (%d)\n",
	   _tid.get_lo(), _in._s_id);
    W_DO(_penv->sub_man()->sub_idx_nl(_penv->db(), prsub, _in._s_id));

    tm1_sub_t asub;

    // READ SUBSCRIBER
    
    prsub->get_value(0,  asub.S_ID);
    prsub->get_value(1,  asub.SUB_NBR, 17);
    
    // BIT_XX
    prsub->get_value(2,  asub.BIT_XX[0]);
    prsub->get_value(3,  asub.BIT_XX[1]);
    prsub->get_value(4,  asub.BIT_XX[2]);
    prsub->get_value(5,  asub.BIT_XX[3]);
    prsub->get_value(6,  asub.BIT_XX[4]);
    prsub->get_value(7,  asub.BIT_XX[5]);
    prsub->get_value(8,  asub.BIT_XX[6]);
    prsub->get_value(9,  asub.BIT_XX[7]);
    prsub->get_value(10, asub.BIT_XX[8]);
    prsub->get_value(11, asub.BIT_XX[9]);
    
    // HEX_XX
    prsub->get_value(12, asub.HEX_XX[0]);
    prsub->get_value(13, asub.HEX_XX[1]);
    prsub->get_value(14, asub.HEX_XX[2]);
    prsub->get_value(15, asub.HEX_XX[3]);
    prsub->get_value(16, asub.HEX_XX[4]);
    prsub->get_value(17, asub.HEX_XX[5]);
    prsub->get_value(18, asub.HEX_XX[6]);
    prsub->get_value(19, asub.HEX_XX[7]);
    prsub->get_value(20, asub.HEX_XX[8]);
    prsub->get_value(21, asub.HEX_XX[9]);
    
    // BYTE2_XX
    prsub->get_value(22, asub.BYTE2_XX[0]);
    prsub->get_value(23, asub.BYTE2_XX[1]);
    prsub->get_value(24, asub.BYTE2_XX[2]);
    prsub->get_value(25, asub.BYTE2_XX[3]);
    prsub->get_value(26, asub.BYTE2_XX[4]);
    prsub->get_value(27, asub.BYTE2_XX[5]);
    prsub->get_value(28, asub.BYTE2_XX[6]);
    prsub->get_value(29, asub.BYTE2_XX[7]);
    prsub->get_value(30, asub.BYTE2_XX[8]);
    prsub->get_value(31, asub.BYTE2_XX[9]);
    
    prsub->get_value(32, asub.MSC_LOCATION);
    prsub->get_value(33, asub.VLR_LOCATION);

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prsub->print_tuple();
#endif

    return RCOK;
}



/******************************************************************** 
 *
 * DORA TM1 GET_NEW_DEST
 *
 ********************************************************************/

#ifdef TM1GND2

w_rc_t mid_gnd_rvp::_run() 
{
    // 1. Setup the final RVP
    final_gnd_rvp* frvp = _penv->new_final_gnd_rvp(_xct,_tid,_xct_id,_result,_actions);

    // 2. Check if aborted during previous phase
    CHECK_MIDWAY_RVP_ABORTED(frvp);

    // 3. Generate the action
    r_cf_gnd_action* r_cf = _penv->new_r_cf_gnd_action(_xct,_tid,frvp,_in);

    TRACE( TRACE_TRX_FLOW, "Next phase (%d)\n", _tid.get_lo());    


    // 4a. Decide about partition
    // 4b. Enqueue
    {        
        irpImpl* my_cf_part = _penv->decide_part(_penv->cf(),_in._s_id);

        // CF_PART_CS
        CRITICAL_SECTION(cf_part_cs, my_cf_part->_enqueue_lock);
        if (my_cf_part->enqueue(r_cf,_bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_CF_GND\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }
    return (RCOK);
}

#endif

DEFINE_DORA_FINAL_RVP_CLASS(final_gnd_rvp,get_new_dest);



/******************************************************************** 
 *
 * DORA TM1 GET_NEW_DEST ACTIONS
 *
 * (1) R-SF
 * (2) R-CF
 *
 ********************************************************************/


void r_sf_gnd_action::calc_keys()
{
    set_read_only();
    int sftype = _in._sf_type;
    _down.push_back(_in._s_id);
    _down.push_back(sftype);
}



w_rc_t r_sf_gnd_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // SpecialFacility
    tuple_guard<sf_man_impl> prsf(_penv->sf_man());
    rep_row_t areprow(_penv->sf_man()->ts());
    areprow.set(_penv->sf_desc()->maxsize()); 
    prsf->_rep = &areprow;

    tm1_sf_t asf;

    // 1. retrieve SpecialFacility (read-only)
    TRACE( TRACE_TRX_FLOW, "App: %d GND:sf-idx-nl (%d) (%d)\n", 
	   _tid.get_lo(), _in._s_id, _in._sf_type);
#ifndef TM1GND2
    if (_prvp->isAborted()) { W_DO(RC(de_MIDWAY_ABORT)); }
#endif
    W_DO(_penv->sf_man()->sf_idx_nl(_penv->db(), prsf, _in._s_id, _in._sf_type));
    prsf->get_value(2, asf.IS_ACTIVE);
    if (!asf.IS_ACTIVE) {
	// Not active special facility
	W_DO(RC(se_NO_CURRENT_TUPLE));
    }

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prsf->print_tuple();
#endif

    return RCOK;
}




void r_cf_gnd_action::calc_keys()
{
    set_read_only();
    int sftype = _in._sf_type;
    int stime = _in._s_time;
    _down.push_back(_in._s_id);
    _down.push_back(sftype);
    _down.push_back(stime);
}



w_rc_t r_cf_gnd_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // CallForwarding
    tuple_guard<cf_man_impl> prcf(_penv->cf_man());
    rep_row_t areprow(_penv->cf_man()->ts());
    areprow.set(_penv->cf_desc()->maxsize()); 
    prcf->_rep = &areprow;

    rep_row_t lowrep(_penv->cf_man()->ts());
    rep_row_t highrep(_penv->cf_man()->ts());
    lowrep.set(_penv->cf_desc()->maxsize()); 
    highrep.set(_penv->cf_desc()->maxsize()); 

    tm1_cf_t acf;
    bool eof;
    bool bFound = false;

#ifndef TM1GND2
    if (_prvp->isAborted()) { W_DO(RC(de_MIDWAY_ABORT)); }
#endif
    
    // 1. Retrieve the call forwarding destination            
    guard<index_scan_iter_impl<call_forwarding_t> > cf_iter;
    {
	index_scan_iter_impl<call_forwarding_t>* tmp_cf_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d GND:cf-idx-iter-nl\n", _tid.get_lo());
	W_DO(_penv->cf_man()->cf_get_idx_iter_nl(_penv->db(), tmp_cf_iter, prcf,
						 lowrep, highrep, _in._s_id,
						 _in._sf_type, _in._s_time));
	cf_iter = tmp_cf_iter;
    }
    W_DO(cf_iter->next(_penv->db(), eof, *prcf));

    while (!eof) {
#ifndef TM1GND2
	if (_prvp->isAborted()) { W_DO(RC(de_MIDWAY_ABORT)); }
#endif
	// check the retrieved CF e_time                
	prcf->get_value(3, acf.END_TIME);
	if (acf.END_TIME > _in._e_time) {
	    prcf->get_value(4, acf.NUMBERX, 17);
	    TRACE( TRACE_TRX_FLOW, "App: %d GND: found (%d) (%d) (%s)\n", 
		   _tid.get_lo(), _in._e_time, acf.END_TIME, acf.NUMBERX);
	    bFound = true;
	}
	TRACE( TRACE_TRX_FLOW, "App: %d GND:cf-idx-iter-next\n", _tid.get_lo());
	W_DO(cf_iter->next(_penv->db(), eof, *prcf));
    }
    
    if (!bFound) { 
	W_DO(RC(se_NO_CURRENT_TUPLE)); 
    }

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prcf->print_tuple();
#endif

    return RCOK;
}




/******************************************************************** 
 *
 * DORA TM1 GET_ACC_DATA
 *
 ********************************************************************/

DEFINE_DORA_FINAL_RVP_CLASS(final_gad_rvp,get_acc_data);


/******************************************************************** 
 *
 * DORA TM1 GET_ACC_DATA ACTIONS
 *
 * (1) R-AI
 *
 ********************************************************************/


void r_ai_gad_action::calc_keys()
{
    set_read_only();
    int aitype = _in._ai_type;
    _down.push_back(_in._s_id);
    _down.push_back(aitype);
}



w_rc_t r_ai_gad_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // AccessInfo
    tuple_guard<ai_man_impl> prai(_penv->ai_man());
    rep_row_t areprow(_penv->ai_man()->ts());
    areprow.set(_penv->ai_desc()->maxsize()); 
    prai->_rep = &areprow;

    /* SELECT data1, data2, data3, data4
     * FROM   Access_Info
     * WHERE  s_id = <s_id rnd>
     * AND    ai_type = <ai_type rnd>
     *
     * plan: index probe on "AI_IDX"
     */

    // 1. retrieve AccessInfo (read-only)
    TRACE( TRACE_TRX_FLOW, "App: %d GAD:ai-idx-nl (%d) (%d)\n", 
	   _tid.get_lo(), _in._s_id, _in._ai_type);
    W_DO(_penv->ai_man()->ai_idx_nl(_penv->db(), prai, _in._s_id, _in._ai_type));
    
    // READ ACCESS-INFO
    tm1_ai_t aai;
    prai->get_value(2,  aai.DATA1);
    prai->get_value(3,  aai.DATA2);
    prai->get_value(4,  aai.DATA3, 5);
    prai->get_value(5,  aai.DATA4, 9);

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prai->print_tuple();
#endif

    return RCOK;
}




/******************************************************************** 
 *
 * DORA TM1 UPD_SUB_DATA
 *
 ********************************************************************/

#ifdef TM1USD2

w_rc_t mid_usd_rvp::_run() 
{
    // 1. Setup the final RVP
    final_usd_rvp* frvp = _penv->new_final_usd_rvp(_xct,_tid,_xct_id,_result,_actions);

    // 2. Check if aborted during previous phase
    CHECK_MIDWAY_RVP_ABORTED(frvp);

    // 3. Generate the action
    upd_sub_usd_action* upd_sub = _penv->new_upd_sub_usd_action(_xct,_tid,frvp,_in);

    TRACE( TRACE_TRX_FLOW, "Next phase (%d)\n", _tid.get_lo());    

    // 4a. Decide about partition
    // 4b. Enqueue
    {        
        irpImpl* my_sub_part = _penv->decide_part(_penv->sub(),_in._s_id);

        // SUB_PART_CS
        CRITICAL_SECTION(sub_part_cs, my_sub_part->_enqueue_lock);
        if (my_sub_part->enqueue(upd_sub,_bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_SUB_USD\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }
    return (RCOK);
}

#endif

DEFINE_DORA_FINAL_RVP_CLASS(final_usd_rvp,upd_sub_data);


/******************************************************************** 
 *
 * DORA TM1 UPD_SUB_DATA ACTIONS
 *
 * (1) UPD-SUB
 * (2) UPD-SF
 *
 ********************************************************************/


void upd_sub_usd_action::calc_keys()
{
    _down.push_back(_in._s_id);
}


w_rc_t upd_sub_usd_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Subscriber
    tuple_guard<sub_man_impl> prsub(_penv->sub_man());
    rep_row_t areprow(_penv->sub_man()->ts());
    areprow.set(_penv->sub_desc()->maxsize()); 
    prsub->_rep = &areprow;

    /* UPDATE Subscriber
     * SET bit_1 = <bit_rnd>
     * WHERE s_id = <s_id rnd subid>;
     *
     * plan: index probe on "S_IDX"
     */
    
    // 1. Update Subscriber
    TRACE( TRACE_TRX_FLOW,
	   "App: %d USD:sub-idx-nl (%d)\n", _tid.get_lo(), _in._s_id);    
#ifndef TM1USD2
    if (_prvp->isAborted()) { W_DO(RC(de_MIDWAY_ABORT)); }
#endif
    W_DO(_penv->sub_man()->sub_idx_nl(_penv->db(), prsub, _in._s_id));
    prsub->set_value(2, _in._a_bit);
#ifndef TM1USD2
    if (_prvp->isAborted()) { W_DO(RC(de_MIDWAY_ABORT)); }
#endif
    W_DO(_penv->sub_man()->update_tuple(_penv->db(), prsub, NL));

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prsub->print_tuple();
#endif

    return RCOK;
}



void upd_sf_usd_action::calc_keys()
{
    int sftype = _in._sf_type;
    _down.push_back(_in._s_id);
    _down.push_back(sftype);
}


w_rc_t upd_sf_usd_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Subscriber
    tuple_guard<sf_man_impl> prsf(_penv->sf_man());
    rep_row_t areprow(_penv->sf_man()->ts());
    areprow.set(_penv->sf_desc()->maxsize()); 
    prsf->_rep = &areprow;

    /* UPDATE Special_Facility
     * SET data_a = <data_a rnd>
     * WHERE s_id = <s_id value subid>
     * AND sf_type = <sf_type rnd>;
     *
     * plan: index probe on "SF_IDX"
     */
    
    // 2. Update SpecialFacility
    TRACE( TRACE_TRX_FLOW, "App: %d USD:sf-idx-nl (%d) (%d)\n", 
	   _tid.get_lo(), _in._s_id, _in._sf_type);
    W_DO(_penv->sf_man()->sf_idx_nl(_penv->db(), prsf, _in._s_id, _in._sf_type));
    prsf->set_value(4, _in._a_data);
    W_DO(_penv->sf_man()->update_tuple(_penv->db(), prsf, NL));

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prsf->print_tuple();
#endif

    return RCOK;
}


/******************************************************************** 
 *
 * DORA TM1 UPD_SUB_DATA MIX
 *
 ********************************************************************/

w_rc_t mid_usdmix_rvp::_run() 
{
    // 1. Setup the final RVP
    final_usdmix_rvp* frvp = _penv->new_final_usdmix_rvp(_xct,_tid,_xct_id,_result,_actions);

    // 2. Check if aborted during previous phase
    CHECK_MIDWAY_RVP_ABORTED(frvp);

    // 3. Generate the action
    upd_sf_usdmix_action* upd_sf = _penv->new_upd_sf_usdmix_action(_xct,_tid,frvp,_in);

    TRACE( TRACE_TRX_FLOW, "Next phase (%d)\n", _tid.get_lo());    

    // 4a. Decide about partition
    // 4b. Enqueue
    {        
        irpImpl* my_sf_part = _penv->decide_part(_penv->sf(),_in._s_id);

        // SF_PART_CS
        CRITICAL_SECTION(sf_part_cs, my_sf_part->_enqueue_lock);
        if (my_sf_part->enqueue(upd_sf,_bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing UPD_SF_USD\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }
    return (RCOK);
}

DEFINE_DORA_FINAL_RVP_CLASS(final_usdmix_rvp,upd_sub_data);


/******************************************************************** 
 *
 * DORA TM1 UPD_SUB_DATA MIX ACTIONS
 *
 * (1) UPD-SUB
 * (2) UPD-SF
 *
 ********************************************************************/


void upd_sub_usdmix_action::calc_keys()
{
    _down.push_back(_in._s_id);
}


w_rc_t upd_sub_usdmix_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Subscriber
    tuple_guard<sub_man_impl> prsub(_penv->sub_man());
    rep_row_t areprow(_penv->sub_man()->ts());
    areprow.set(_penv->sub_desc()->maxsize()); 
    prsub->_rep = &areprow;

    /* UPDATE Subscriber
     * SET bit_1 = <bit_rnd>
     * WHERE s_id = <s_id rnd subid>;
     *
     * plan: index probe on "S_IDX"
     */
    
    // 1. Update Subscriber
    TRACE( TRACE_TRX_FLOW,
	   "App: %d USD:sub-idx-nl (%d)\n", _tid.get_lo(), _in._s_id);
    W_DO(_penv->sub_man()->sub_idx_nl(_penv->db(), prsub, _in._s_id));
    prsub->set_value(2, _in._a_bit);
    W_DO(_penv->sub_man()->update_tuple(_penv->db(), prsub, NL));

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prsub->print_tuple();
#endif

    return RCOK;
}



void upd_sf_usdmix_action::calc_keys()
{
    int sftype = _in._sf_type;
    _down.push_back(_in._s_id);
    _down.push_back(sftype);
}


w_rc_t upd_sf_usdmix_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // SpecialFacility
    tuple_guard<sf_man_impl> prsf(_penv->sf_man());
    rep_row_t areprow(_penv->sf_man()->ts());
    areprow.set(_penv->sf_desc()->maxsize()); 
    prsf->_rep = &areprow;

    /* UPDATE Special_Facility
     * SET data_a = <data_a rnd>
     * WHERE s_id = <s_id value subid>
     * AND sf_type = <sf_type rnd>;
     *
     * plan: index probe on "SF_IDX"
     */
    
    // 2. Update SpecialFacility
    TRACE( TRACE_TRX_FLOW, "App: %d USD:sf-idx-nl (%d) (%d)\n", 
	   _tid.get_lo(), _in._s_id, _in._sf_type);
    W_DO(_penv->sf_man()->sf_idx_nl(_penv->db(), prsf, _in._s_id, _in._sf_type));
    prsf->set_value(4, _in._a_data);
    W_DO(_penv->sf_man()->update_tuple(_penv->db(), prsf, NL));

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prsf->print_tuple();
#endif

    return RCOK;
}


/******************************************************************** 
 *
 * DORA TM1 UPD_LOC
 *
 ********************************************************************/

DEFINE_DORA_FINAL_RVP_CLASS(final_ul_rvp,upd_loc);


/******************************************************************** 
 *
 * DORA TM1 UPD_LOC ACTIONS
 *
 * (1) UPD-SUB
 *
 ********************************************************************/


void upd_sub_ul_action::calc_keys()
{
    _down.push_back(_in._s_id);
}


w_rc_t upd_sub_ul_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Subscriber
    tuple_guard<sub_man_impl> prsub(_penv->sub_man());
    rep_row_t areprow(_penv->sub_man()->ts());
    areprow.set(_penv->sub_desc()->maxsize()); 
    prsub->_rep = &areprow;

    /* UPDATE Subscriber
     * SET    vlr_location = <vlr_location rnd>
     * WHERE  sub_id = <sub_id rnd>;
     *
     * plan: index probe on "SUB_IDX"
     */
    
    // 1. Probe Subscriber through sec index
    TRACE( TRACE_TRX_FLOW, 
	   "App: %d UL:sub-nbr-idx-nl (%d)\n", _tid.get_lo(), _in._s_id);
    W_DO(_penv->sub_man()->sub_nbr_idx_nl(_penv->db(), prsub, _in._sub_nbr));
    int probed_sid;
    prsub->get_value(0, probed_sid);
    if (probed_sid != _in._s_id) {
	TRACE( TRACE_ALWAYS, "Probed sid not matching sid (%d) (%d) ***\n",
	       probed_sid, _in._s_id);
	W_DO(RC(de_WRONG_IDX_DATA));
    }
    prsub->set_value(33, _in._vlr_loc);
    
    // 2. Update tuple
    W_DO(_penv->sub_man()->update_tuple(_penv->db(), prsub, NL));

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prsub->print_tuple();
#endif

    return RCOK;
}





/******************************************************************** 
 *
 * DORA TM1 INS_CALL_FWD
 *
 * (1) mid_icf_rvp
 * (2) final_icf_rvp
 *
 ********************************************************************/

#ifdef TM1ICF2

w_rc_t mid1_icf_rvp::_run() 
{
    // 1. Setup the next RVP
    // PH2 consists of 1 action
    mid2_icf_rvp* rvp = _penv->new_mid2_icf_rvp(_xct,_tid,_xct_id,_result,_in,_actions,_bWake);

    // 2. Check if aborted during previous phase
    CHECK_MIDWAY_RVP_ABORTED(rvp);

    // 2. Generate the action
    r_sf_icf_action* r_sf = _penv->new_r_sf_icf_action(_xct,_tid,rvp,_in);

    TRACE( TRACE_TRX_FLOW, "Next phase (%d)\n", _tid.get_lo());    

    // 3a. Decide about partition
    // 3b. Enqueue
    {        
        irpImpl* my_sf_part = _penv->decide_part(_penv->sf(),_in._s_id);

        // SF_PART_CS
        CRITICAL_SECTION(sf_part_cs, my_sf_part->_enqueue_lock);
        if (my_sf_part->enqueue(r_sf,_bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_SF_ICF\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }
    return (RCOK);
}


w_rc_t mid2_icf_rvp::_run() 
{
    // 1. Setup the final RVP
    // PH3 consists of 1 action
    final_icf_rvp* frvp = _penv->new_final_icf_rvp(_xct,_tid,_xct_id,_result,_actions);

    // 2. Check if aborted during previous phase
    CHECK_MIDWAY_RVP_ABORTED(frvp);

    // 3. Generate the action
    ins_cf_icf_action* ins_cf = _penv->new_ins_cf_icf_action(_xct,_tid,frvp,_in);

    TRACE( TRACE_TRX_FLOW, "Next phase (%d)\n", _tid.get_lo());    

    // 4a. Decide about partition
    // 4b. Enqueue
    {        
        irpImpl* my_cf_part = _penv->decide_part(_penv->cf(),_in._s_id);

        // CF_PART_CS
        CRITICAL_SECTION(cf_part_cs, my_cf_part->_enqueue_lock);
        if (my_cf_part->enqueue(ins_cf,_bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing INS_CF_ICF\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }
    return (RCOK);
}

#else

w_rc_t mid_icf_rvp::_run() 
{
    // 1. Setup the final RVP
    final_icf_rvp* frvp = _penv->new_final_icf_rvp(_xct,_tid,_xct_id,_result,_actions);    

    // 2. Check if aborted during previous phase
    CHECK_MIDWAY_RVP_ABORTED(frvp);

    // 2. Generate the actions
    r_sf_icf_action* r_sf = _penv->new_r_sf_icf_action(_xct,_tid,frvp,_in);
    ins_cf_icf_action* ins_cf = _penv->new_ins_cf_icf_action(_xct,_tid,frvp,_in);

    TRACE( TRACE_TRX_FLOW, "Next phase (%d)\n", _tid.get_lo());    

    // 3a. Decide about partition
    // 3b. Enqueue
    {        
        irpImpl* my_sf_part = _penv->decide_part(_penv->sf(),_in._s_id);
        irpImpl* my_cf_part = _penv->decide_part(_penv->cf(),_in._s_id);

        // SF_PART_CS
        CRITICAL_SECTION(sf_part_cs, my_sf_part->_enqueue_lock);
        if (my_sf_part->enqueue(r_sf,_bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing R_SF_ICF\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }

        // CF_PART_CS
        CRITICAL_SECTION(cf_part_cs, my_cf_part->_enqueue_lock);
        sf_part_cs.exit();
        if (my_cf_part->enqueue(ins_cf,_bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing INS_CF_ICF\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK);
}

#endif

DEFINE_DORA_FINAL_RVP_CLASS(final_icf_rvp,ins_call_fwd);


/******************************************************************** 
 *
 * DORA TM1 INS_CALL_FWD ACTIONS
 *
 * (1) R-SUB
 * (2) R-SF
 * (3) INS-CF
 *
 ********************************************************************/


void r_sub_icf_action::calc_keys()
{
    set_read_only();
    _down.push_back(_in._s_id);
}


w_rc_t r_sub_icf_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Subscriber
    tuple_guard<sub_man_impl> prsub(_penv->sub_man());
    rep_row_t areprow(_penv->sub_man()->ts());
    areprow.set(_penv->sub_desc()->maxsize()); 
    prsub->_rep = &areprow;

    // 1. Probe Subscriber sec index
    TRACE( TRACE_TRX_FLOW, 
	   "App: %d ICF:sub-nbr-idx-nl (%d)\n", _tid.get_lo(), _in._s_id);
    W_DO(_penv->sub_man()->sub_nbr_idx_nl(_penv->db(), prsub, _in._sub_nbr));
    int probed_sid;
    prsub->get_value(0, probed_sid);
    if (probed_sid != _in._s_id) {
	TRACE( TRACE_ALWAYS, "Probed sid not matching sid (%d) (%d) ***\n",
	       probed_sid, _in._s_id);
	W_DO(RC(de_WRONG_IDX_DATA));
    }

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prsub->print_tuple();
#endif

    return RCOK;
}



void r_sf_icf_action::calc_keys()
{
    set_read_only();
    int sftype = _in._sf_type;
    _down.push_back(_in._s_id);
    _down.push_back(sftype);
}


w_rc_t r_sf_icf_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // SpecialFacility
    tuple_guard<sf_man_impl> prsf(_penv->sf_man());
    rep_row_t areprow(_penv->sf_man()->ts());
    areprow.set(_penv->sf_desc()->maxsize()); 
    prsf->_rep = &areprow;

    rep_row_t lowrep(_penv->sf_man()->ts());
    rep_row_t highrep(_penv->sf_man()->ts());
    lowrep.set(_penv->sf_desc()->maxsize()); 
    highrep.set(_penv->sf_desc()->maxsize()); 

    tm1_sf_t asf;
    bool bFound = false;
    bool eof;

    /* SELECT sf_type bind sfid sf_type>
     * FROM   Special_Facility
     * WHERE  s_id = <s_id value subid>
     *
     * plan: iter index on "SF_IDX"
     */
#ifndef TM1ICF2
    if (_prvp->isAborted()) { W_DO(RC(de_MIDWAY_ABORT)); }
#endif        

    // 1. Retrieve SpecialFacility (Read-only)
    guard<index_scan_iter_impl<special_facility_t> > sf_iter;
    {
	index_scan_iter_impl<special_facility_t>* tmp_sf_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d ICF:sf-idx-iter-nl\n", _tid.get_lo());
	W_DO(_penv->sf_man()->sf_get_idx_iter_nl(_penv->db(), tmp_sf_iter, prsf,
						 lowrep, highrep, _in._s_id));
	sf_iter = tmp_sf_iter;
    }
    W_DO(sf_iter->next(_penv->db(), eof, *prsf));

    while (!eof) {
#ifndef TM1ICF2
        if (_prvp->isAborted()) { W_DO(RC(de_MIDWAY_ABORT)); }
#endif
	// check the retrieved SF sf_type
	prsf->get_value(1, asf.SF_TYPE);

	if (asf.SF_TYPE == _in._sf_type) {
	    TRACE( TRACE_TRX_FLOW, "App: %d ICF: found (%d) (%d)\n", 
		   _tid.get_lo(), _in._s_id, asf.SF_TYPE);
	    bFound = true;
	    break;
	}
        
	TRACE( TRACE_TRX_FLOW, "App: %d ICF:sf-idx-iter-next\n", _tid.get_lo());
	W_DO(sf_iter->next(_penv->db(), eof, *prsf));
    }            
    
    if (!bFound) { 
	W_DO(RC(se_NO_CURRENT_TUPLE)); 
    } 

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prsf->print_tuple();
#endif

    return RCOK;
}



void ins_cf_icf_action::calc_keys()
{
    int sftype = _in._sf_type;
    int stime = _in._s_time;
    _down.push_back(_in._s_id);
    _down.push_back(sftype);
    _down.push_back(stime);
}


w_rc_t ins_cf_icf_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // CallForwarding
    tuple_guard<cf_man_impl> prcf(_penv->cf_man());
    rep_row_t areprow(_penv->cf_man()->ts());
    areprow.set(_penv->cf_desc()->maxsize()); 
    prcf->_rep = &areprow;
    rep_row_t areprow_key(_penv->cf_man()->ts());
    areprow_key.set(_penv->cf_desc()->maxsize()); 
    prcf->_rep_key = &areprow_key;

    // 3. Check if it can successfully insert CallForwarding entry
    w_rc_t e;
    TRACE( TRACE_TRX_FLOW, "App: %d ICF:cf-idx-nl (%d) (%d) (%d)\n", 
	   _tid.get_lo(), _in._s_id, _in._sf_type, _in._s_time);
#ifndef TM1ICF2
    if (_prvp->isAborted()) { W_DO(RC(de_MIDWAY_ABORT)); }
#endif
    e = _penv->cf_man()->cf_idx_nl(_penv->db(), prcf, _in._s_id,
				   _in._sf_type, _in._s_time);
    
    // idx probes return se_TUPLE_NOT_FOUND
    if (e.is_error() && e.err_num() == se_TUPLE_NOT_FOUND) { 
	// 4. Insert
	prcf->set_value(0, _in._s_id);
	prcf->set_value(1, _in._sf_type);
	prcf->set_value(2, _in._s_time);
	prcf->set_value(3, _in._e_time);
	prcf->set_value(4, _in._numberx);                
#ifdef CFG_HACK
	prcf->set_value(5, "padding"); // PADDING
#endif
	TRACE (TRACE_TRX_FLOW, "App: %d ICF:ins-cf\n", _tid.get_lo());
#ifndef TM1ICF2
        if (_prvp->isAborted()) { W_DO(RC(de_MIDWAY_ABORT)); }
#endif
	W_DO(_penv->cf_man()->add_tuple(_penv->db(), prcf, NL));
    } else {
	// in any other case it should fail
	W_DO(RC(se_CANNOT_INSERT_TUPLE));
    }

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prcf->print_tuple();
#endif

    return RCOK;
}



/******************************************************************** 
 *
 * DORA TM1 DEL_CALL_FWD
 *
 * (1) mid_dcf_rvp
 * (2) final_dcf_rvp
 *
 ********************************************************************/


w_rc_t mid_dcf_rvp::_run() 
{
    // 1. Setup the final RVP
    final_dcf_rvp* frvp = _penv->new_final_dcf_rvp(_xct,_tid,_xct_id,_result,_actions);    

    // 2. Check if aborted during previous phase
    CHECK_MIDWAY_RVP_ABORTED(frvp);

    // 2. Generate the actions
    del_cf_dcf_action* del_cf = _penv->new_del_cf_dcf_action(_xct,_tid,frvp,_in);

    TRACE( TRACE_TRX_FLOW, "Next phase (%d)\n", _tid.get_lo());    

    // 3a. Decide about partition
    // 3b. Enqueue
    {        
        irpImpl* my_cf_part = _penv->decide_part(_penv->cf(),_in._s_id);

        // CF_PART_CS
        CRITICAL_SECTION(cf_part_cs, my_cf_part->_enqueue_lock);
        if (my_cf_part->enqueue(del_cf,_bWake)) {
            TRACE( TRACE_DEBUG, "Problem in enqueueing DEL_CF_ICF\n");
            assert (0); 
            return (RC(de_PROBLEM_ENQUEUE));
        }
    }

    return (RCOK);
}


DEFINE_DORA_FINAL_RVP_CLASS(final_dcf_rvp,del_call_fwd);


/******************************************************************** 
 *
 * DORA TM1 DEL_CALL_FWD ACTIONS
 *
 * (1) R-SUB
 * (2) DEL-CF
 *
 ********************************************************************/


void r_sub_dcf_action::calc_keys()
{
    set_read_only();
    _down.push_back(_in._s_id);
}


w_rc_t r_sub_dcf_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Subscriber
    tuple_guard<sub_man_impl> prsub(_penv->sub_man());
    rep_row_t areprow(_penv->sub_man()->ts());
    areprow.set(_penv->sub_desc()->maxsize()); 
    prsub->_rep = &areprow;

    // 1. Probe Subscriber sec index
    TRACE( TRACE_TRX_FLOW, 
	   "App: %d DCF:sub-nbr-idx-nl (%d)\n", _tid.get_lo(), _in._s_id);
    W_DO(_penv->sub_man()->sub_nbr_idx_nl(_penv->db(), prsub, _in._sub_nbr));
    int probed_sid;
    prsub->get_value(0, probed_sid);
    if (probed_sid != _in._s_id) {
	TRACE( TRACE_ALWAYS, "Probed sid not matching sid (%d) (%d) ***\n",
	       probed_sid, _in._s_id);
	W_DO(RC(de_WRONG_IDX_DATA));
    }

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prsub->print_tuple();
#endif

    return RCOK;
}



void del_cf_dcf_action::calc_keys()
{
    int sftype = _in._sf_type;
    int stime = _in._s_time;
    _down.push_back(_in._s_id);
    _down.push_back(sftype);
    _down.push_back(stime);
}



w_rc_t del_cf_dcf_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // CallForwarding
    tuple_guard<cf_man_impl> prcf(_penv->cf_man());
    rep_row_t areprow(_penv->cf_man()->ts());
    areprow.set(_penv->cf_desc()->maxsize()); 
    prcf->_rep = &areprow;

    /* DELETE FROM Call_Forwarding
     * WHERE s_id = <s_id value subid>
     * AND sf_type = <sf_type rnd>
     * AND start_time = <start_time rnd>;
     *
     * plan: index probe on "CF_IDX"     
     */
    
    // 2. Delete CallForwarding record
    TRACE( TRACE_TRX_FLOW, "App: %d DCF:cf-idx-upd (%d) (%d) (%d)\n", 
	   _tid.get_lo(), _in._s_id, _in._sf_type, _in._s_time);
    W_DO(_penv->cf_man()->cf_idx_nl(_penv->db(), prcf, 
				    _in._s_id, _in._sf_type, _in._s_time));
    TRACE (TRACE_TRX_FLOW, "App: %d DCF:del-cf\n", _tid.get_lo()); 
    W_DO(_penv->cf_man()->delete_tuple(_penv->db(), prcf, NL));

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prcf->print_tuple();
#endif

    return RCOK;
}



/******************************************************************** 
 *
 * DORA TM1 INS_CALL_FWD_BENCH
 *
 * (1) final_icfb_rvp
 *
 ********************************************************************/


DEFINE_DORA_FINAL_RVP_CLASS(final_icfb_rvp,ins_call_fwd_bench);


/******************************************************************** 
 *
 * DORA TM1 INS_CALL_FWD_BENCH ACTIONS
 *
 * (1) R-SUB
 * (2) I-CF
 *
 ********************************************************************/


void r_sub_icfb_action::calc_keys()
{
    set_read_only();
    _down.push_back(_in._s_id);
}


w_rc_t r_sub_icfb_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Subscriber
    tuple_guard<sub_man_impl> prsub(_penv->sub_man());
    rep_row_t areprow(_penv->sub_man()->ts());
    areprow.set(_penv->sub_desc()->maxsize()); 
    prsub->_rep = &areprow;

    // 1. Probe Subscriber sec index
    TRACE( TRACE_TRX_FLOW, 
	   "App: %d ICF:sub-nbr-idx-nl (%d)\n", _tid.get_lo(), _in._s_id);
    W_DO(_penv->sub_man()->sub_nbr_idx_nl(_penv->db(), prsub, _in._sub_nbr));
    prsub->get_value(0, _in._s_id);

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prsub->print_tuple();
#endif

    return RCOK;
}



void i_cf_icfb_action::calc_keys()
{
    int sftype = _in._sf_type;
    int stime = _in._s_time;
    _down.push_back(_in._s_id);
    _down.push_back(sftype);
    _down.push_back(stime);
}


w_rc_t i_cf_icfb_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // CallForwarding
    tuple_guard<cf_man_impl> prcf(_penv->cf_man());
    rep_row_t areprow(_penv->cf_man()->ts());
    areprow.set(_penv->cf_desc()->maxsize()); 
    prcf->_rep = &areprow;
    rep_row_t areprow_key(_penv->cf_man()->ts());
    areprow_key.set(_penv->cf_desc()->maxsize()); 
    prcf->_rep_key = &areprow_key;

    // 3. Check if it can successfully insert CallForwarding entry
    w_rc_t e;
    TRACE( TRACE_TRX_FLOW, "App: %d ICFB:cf-idx-nl (%d) (%d) (%d)\n", 
	   _tid.get_lo(), _in._s_id, _in._sf_type, _in._s_time);
    e = _penv->cf_man()->cf_idx_nl(_penv->db(), prcf, 
				   _in._s_id, _in._sf_type, _in._s_time);
    // idx probes return se_TUPLE_NOT_FOUND
    if (e.is_error() && e.err_num() == se_TUPLE_NOT_FOUND) { 
	// 4. Insert
	prcf->set_value(0, _in._s_id);
	prcf->set_value(1, _in._sf_type);
	prcf->set_value(2, _in._s_time);
	prcf->set_value(3, _in._e_time);
	prcf->set_value(4, _in._numberx);                
#ifdef CFG_HACK
	prcf->set_value(5, "padding"); // PADDING
#endif
	TRACE (TRACE_TRX_FLOW, "App: %d ICFB:ins-cf\n", _tid.get_lo());
	W_DO(_penv->cf_man()->add_tuple(_penv->db(), prcf, NL));
    } else { // 3. Delete Call Forwarding record if tuple found
	if(e.is_error()) { W_DO(e); }
	TRACE (TRACE_TRX_FLOW, "App: %d ICFB:del-cf\n", _tid.get_lo());
	W_DO(_penv->cf_man()->delete_tuple(_penv->db(), prcf, NL));
    }

#ifdef PRINT_TRX_RESULTS
    // dumps the status of all the table rows used
    prcf->print_tuple();
#endif

    return RCOK;
}







/******************************************************************** 
 *
 * DORA TM1 GET_SUB_NBR
 *
 ********************************************************************/

DEFINE_DORA_FINAL_RVP_CLASS(final_gsn_rvp,get_sub_nbr);


/******************************************************************** 
 *
 * DORA TM1 GET_SUB_NBR ACTIONS
 *
 * The are two implementations of the GET_SUB_NBR transaction.
 *
 * Case 1: R-SUB - does the index probe and record retrieval here 
 *
 * Case 2: R-SUB-ACC - the index probe has been made by the dispatcher
 *                     here it does only the record retrieval.
 *
 ********************************************************************/

#ifndef USE_DORA_EXT_IDX
void r_sub_gsn_action::calc_keys()
{
    // This is a read-only action
    set_read_only();

    // This action is not accessing a single key
    set_is_range(); 

    // Iterate the range of values and put an entry for each value  
    _key_list.reserve(_in._range);
    int sid;
    for (uint i=0; i<_in._range; i++) {
        range_action_impl<int>::Key aKey;
        sid = _in._s_id + i;
        aKey.push_back(sid);
        range_action_impl<int>::_key_list.push_back(aKey);
    }

    // !!! BUG BUG BUG !!!
    // It does not handle correctly the case when a key in the range is
    // is already locked
    // !!! BUG BUG BUG !!!
#warning Look at lock_man_t::acquire_all() !!!
}

w_rc_t r_sub_gsn_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Subscriber
    tuple_guard<sub_man_impl> prsub(_penv->sub_man());
    rep_row_t areprow(_penv->sub_man()->ts());
    areprow.set(_penv->sub_desc()->maxsize()); 
    prsub->_rep = &areprow;
    rep_row_t lowrep(_penv->sub_man()->ts());
    rep_row_t highrep(_penv->sub_man()->ts());
    lowrep.set(_penv->sub_desc()->maxsize()); 
    highrep.set(_penv->sub_desc()->maxsize()); 

    bool eof;
    int sid, vlrloc;

    /* SELECT s_id, msc_location
     * FROM   Subscriber
     * WHERE  sub_nbr >= <sub_nbr rndstr as s1>
     * AND    sub_nbr <  (s1 + <range>);
     *
     * plan: index probe on "SUB_NBR_IDX"
     */

    // 1. Secondary index access to Subscriber using sub_nbr and range
    guard<index_scan_iter_impl<subscriber_t> > sub_iter;
    {
	index_scan_iter_impl<subscriber_t>* tmp_sub_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d GSN:sub-nbr-idx-iter (%d) (%d)\n", 
	       _tid.get_lo(), _in._s_id, _in._range);
	W_DO(_penv->sub_man()->sub_get_idx_iter(_penv->db(), tmp_sub_iter, prsub, 
						lowrep,highrep, _in._s_id,
						_in._range, NL, true));
	sub_iter = tmp_sub_iter;
    }
    
    // 2. Read all the returned records
    W_DO(sub_iter->next(_penv->db(), eof, *prsub));
    while (!eof) {
	prsub->get_value(0, sid);
	prsub->get_value(33, vlrloc);
	TRACE( TRACE_TRX_FLOW, "App: %d GSN: read (%d) (%d)\n",
	       _tid.get_lo(), sid, vlrloc);
	W_DO(sub_iter->next(_penv->db(), eof, *prsub));
    }

    return RCOK;
}

#else

void r_sub_gsn_acc_action::calc_keys()
{
    set_read_only();
    set_secondary();

    // This action is not accessing a single key
    // Iterate the range of values and put an entry for each value  
    set_is_range(); 
    _key_list.reserve(_in._pairs.size());
    typedef vector< pair<int,rid_t> >::iterator pair_iterator;
    for (pair_iterator it=_in._pairs.begin(); it!=_in._pairs.end(); it++) {
        range_action_impl<int>::Key aKey;
        aKey.push_back((*it).first);
        range_action_impl<int>::_key_list.push_back(aKey);
    }
}


w_rc_t r_sub_gsn_acc_action::trx_exec() 
{
    assert (_penv);

    // get table tuple from the cache
    // Subscriber
    tuple_guard<sub_man_impl> prsub(_penv->sub_man());
    rep_row_t areprow(_penv->sub_man()->ts());
    areprow.set(_penv->sub_desc()->maxsize()); 
    prsub->_rep = &areprow;
    int vlrloc;

    typedef vector< pair<int,rid_t> >::iterator pair_iterator;
    rid_t arid;
    for (pair_iterator it=_in._pairs.begin(); it!=_in._pairs.end(); it++) {
	arid = (*it).second;
	
	// Access directly the record
	prsub->set_rid(arid);
        W_DO(_penv->sub_man()->read_tuple(prsub,NL));
	prsub->get_value(33, vlrloc);
	
	TRACE( TRACE_TRX_FLOW, "App: %d GSN: read (%d) (%d)\n", 
	       _tid.get_lo(), (*it).first, vlrloc);
    }

    return RCOK;
}

#endif


EXIT_NAMESPACE(dora);
