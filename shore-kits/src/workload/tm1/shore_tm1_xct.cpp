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

/** @file:   shore_tm1_xct.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TM1 transactions
 *
 *  @author: Ippokratis Pandis, Feb 2009
 */

#include "workload/tm1/shore_tm1_env.h"
#include "workload/tm1/tm1_input.h"

#include <vector>
#include <numeric>
#include <algorithm>

using namespace shore;


ENTER_NAMESPACE(tm1);



/******************************************************************** 
 *
 * Thread-local TM1 TRXS Stats
 *
 ********************************************************************/


static __thread ShoreTM1TrxStats my_stats;

void ShoreTM1Env::env_thread_init()
{
    CRITICAL_SECTION(stat_mutex_cs, _statmap_mutex);
    _statmap[pthread_self()] = &my_stats;
}

void ShoreTM1Env::env_thread_fini()
{
    CRITICAL_SECTION(stat_mutex_cs, _statmap_mutex);
    _statmap.erase(pthread_self());
}



/******************************************************************** 
 *
 *  @fn:    _get_stats
 *
 *  @brief: Returns a structure with the currently stats
 *
 ********************************************************************/

ShoreTM1TrxStats ShoreTM1Env::_get_stats()
{
    CRITICAL_SECTION(cs, _statmap_mutex);
    ShoreTM1TrxStats rval;
    rval -= rval; // dirty hack to set all zeros
    for (statmap_t::iterator it=_statmap.begin(); it != _statmap.end(); ++it) 
	rval += *it->second;
    return (rval);
}


/******************************************************************** 
 *
 *  @fn:    reset_stats
 *
 *  @brief: Updates the last gathered statistics
 *
 ********************************************************************/

void ShoreTM1Env::reset_stats()
{
    CRITICAL_SECTION(last_stats_cs, _last_stats_mutex);
    _last_stats = _get_stats();
}


/******************************************************************** 
 *
 *  @fn:    print_throughput
 *
 *  @brief: Prints the throughput given measurement delay
 *
 ********************************************************************/

void ShoreTM1Env::print_throughput(const double iQueriedSF, 
                                   const int iSpread, 
                                   const int iNumOfThreads, 
                                   const double delay,
                                   const ulong_t mioch,
                                   const double avgcpuusage)
{
    CRITICAL_SECTION(last_stats_cs, _last_stats_mutex);

    // get the current statistics
    ShoreTM1TrxStats current_stats = _get_stats();
    
    // now calculate the diff
    current_stats -= _last_stats;
        
    uint trxs_att  = current_stats.attempted.total();
    uint trxs_abt  = current_stats.failed.total();
    uint trxs_dld  = current_stats.deadlocked.total();

    TRACE( TRACE_ALWAYS, "*******\n"                \
           "SF:           (%.1f)\n"                 \
           "Spread:       (%s)\n"                   \
           "Threads:      (%d)\n"                   \
           "Trxs Att:     (%d)\n"                   \
           "Trxs Abt:     (%d)\n"                   \
           "Trxs Dld:     (%d)\n"                   \
           "Success Rate: (%.1f%%)\n"               \
           "Secs:         (%.2f)\n"                 \
           "IOChars:      (%.2fM/s)\n"              \
           "AvgCPUs:      (%.1f) (%.1f%%)\n"        \
           "MQTh/s:       (%.2f)\n",
           iQueriedSF, 
           (iSpread ? "Yes" : "No"),
           iNumOfThreads, trxs_att, trxs_abt, trxs_dld,
           ((double)100*(trxs_att-trxs_abt-trxs_dld))/(double)trxs_att,
           delay, mioch/delay, avgcpuusage, 
           100*avgcpuusage/get_max_cpu_count(),
           (trxs_att-trxs_abt-trxs_dld)/delay);
}




/******************************************************************** 
 *
 * TM1 TRXS
 *
 * (1) The run_XXX functions are wrappers to the real transactions
 * (2) The xct_XXX functions are the implementation of the transactions
 *
 ********************************************************************/


/********************************************************************* 
 *
 *  @fn:    run_one_xct
 *
 *  @brief: Initiates the execution of one TM1 xct
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure internal has expired.
 *
 *********************************************************************/

w_rc_t ShoreTM1Env::run_one_xct(Request* prequest)
{
    assert (prequest);

    if(_start_imbalance > 0 && !_bAlarmSet) {
	CRITICAL_SECTION(alarm_cs, _alarm_lock);
	if(!_bAlarmSet) {
	    alarm(_start_imbalance);
	    _bAlarmSet = true;
	}
    }

    // if BASELINE TM1 MIX
    if (prequest->type() == XCT_TM1_MIX) {        
        prequest->set_type(random_tm1_xct_type(abs(smthread_t::me()->rand()%100)));
    }
    
    switch (prequest->type()) {
        
        // TM1 BASELINE
    case XCT_TM1_GET_SUB_DATA:
        return (run_get_sub_data(prequest));
    case XCT_TM1_GET_NEW_DEST:
        return (run_get_new_dest(prequest));
    case XCT_TM1_GET_ACC_DATA:
        return (run_get_acc_data(prequest));
    case XCT_TM1_UPD_SUB_DATA:
        return (run_upd_sub_data(prequest));
    case XCT_TM1_UPD_LOCATION:
        return (run_upd_loc(prequest));
    case XCT_TM1_INS_CALL_FWD:
        return (run_ins_call_fwd(prequest));
    case XCT_TM1_DEL_CALL_FWD:
        return (run_del_call_fwd(prequest));
    case XCT_TM1_CALL_FWD_MIX:
        // evenly pick one of the {Ins/Del}CallFwd
        if (URand(1,100)>50)
            return (run_ins_call_fwd(prequest));
        else
            return (run_del_call_fwd(prequest));

    case XCT_TM1_GET_SUB_NBR:
        return (run_get_sub_nbr(prequest));

    case XCT_TM1_INS_CALL_FWD_BENCH:
        return (run_ins_call_fwd_bench(prequest));
    case XCT_TM1_DEL_CALL_FWD_BENCH:
        return (run_del_call_fwd_bench(prequest));
    case XCT_TM1_CALL_FWD_MIX_BENCH:
        // evenly pick one of the {Ins/Del}CallFwdBench
        if (URand(1,100)>50)
            return (run_ins_call_fwd_bench(prequest));
        else
            return (run_del_call_fwd_bench(prequest));
	
    default:
        assert (0); // UNKNOWN TRX-ID
    }
    return (RCOK);
}



/******************************************************************** 
 *
 * TPC-C TRXs Wrappers
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


DEFINE_TRX(ShoreTM1Env,get_sub_data);
DEFINE_TRX(ShoreTM1Env,get_new_dest);
DEFINE_TRX(ShoreTM1Env,get_acc_data);
DEFINE_TRX(ShoreTM1Env,upd_sub_data);
DEFINE_TRX(ShoreTM1Env,upd_loc);
DEFINE_TRX(ShoreTM1Env,ins_call_fwd);
DEFINE_TRX(ShoreTM1Env,del_call_fwd);

DEFINE_TRX(ShoreTM1Env,get_sub_nbr);

DEFINE_TRX(ShoreTM1Env,ins_call_fwd_bench);
DEFINE_TRX(ShoreTM1Env,del_call_fwd_bench);

// uncomment the line below if want to dump (part of) the trx results
//#define PRINT_TRX_RESULTS


/******************************************************************** 
 *
 * TM1 POPULATE_ONE
 *
 * @brief: Inserts a Subscriber with the specific SUB_ID and all the
 *         corresponding AI, SF, and CF entries (according to the 
 *         benchmark specification.
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::xct_populate_one(const int sub_id)
{
    assert (sub_id>=0);

    // get table tuples from the caches
    tuple_guard<sub_man_impl> prsub(_psub_man);
    tuple_guard<ai_man_impl> prai(_pai_man);
    tuple_guard<sf_man_impl> prsf(_psf_man);
    tuple_guard<cf_man_impl> prcf(_pcf_man);

    rep_row_t areprow(_psub_man->ts());
    rep_row_t areprow_key(_psub_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_psub_desc->maxsize());
    areprow_key.set(_psub_desc->maxsize()); 

    prsub->_rep = &areprow;
    prai->_rep = &areprow;
    prsf->_rep = &areprow;
    prcf->_rep = &areprow;
    prsub->_rep_key = &areprow_key;
    prai->_rep_key = &areprow_key;
    prsf->_rep_key = &areprow_key;
    prcf->_rep_key = &areprow_key;

    short i,j=0;

    // determine how many AI records to have    
    short num_ai = URand(TM1_MIN_AI_PER_SUBSCR,
                         TM1_MAX_AI_PER_SUBSCR);

    // determine how many SF records to have    
    short num_sf = URand(TM1_MIN_SF_PER_SUBSCR,
                         TM1_MAX_SF_PER_SUBSCR);

    short num_cf = 0;

    // POPULATE SUBSCRIBER

    prsub->set_value(0, sub_id);
    char asubnbr[STRSIZE(TM1_SUB_NBR_SZ)];
    memset(asubnbr,0,STRSIZE(TM1_SUB_NBR_SZ));
    sprintf(asubnbr,"%015d",sub_id);
    prsub->set_value(1, asubnbr);
    
    // BIT_XX
    prsub->set_value(2,  URandBool());
    prsub->set_value(3,  URandBool());
    prsub->set_value(4,  URandBool());
    prsub->set_value(5,  URandBool());
    prsub->set_value(6,  URandBool());
    prsub->set_value(7,  URandBool());
    prsub->set_value(8,  URandBool());
    prsub->set_value(9,  URandBool());
    prsub->set_value(10, URandBool());
    prsub->set_value(11, URandBool());
    
    // HEX_XX
    prsub->set_value(12, URandShort(0,15));
    prsub->set_value(13, URandShort(0,15));
    prsub->set_value(14, URandShort(0,15));
    prsub->set_value(15, URandShort(0,15));
    prsub->set_value(16, URandShort(0,15));
    prsub->set_value(17, URandShort(0,15));
    prsub->set_value(18, URandShort(0,15));
    prsub->set_value(19, URandShort(0,15));
    prsub->set_value(20, URandShort(0,15));
    prsub->set_value(21, URandShort(0,15));
    
    // BYTE2_XX
    prsub->set_value(22, URandShort(0,255));
    prsub->set_value(23, URandShort(0,255));
    prsub->set_value(24, URandShort(0,255));
    prsub->set_value(25, URandShort(0,255));
    prsub->set_value(26, URandShort(0,255));
    prsub->set_value(27, URandShort(0,255));
    prsub->set_value(28, URandShort(0,255));
    prsub->set_value(29, URandShort(0,255));
    prsub->set_value(30, URandShort(0,255));
    prsub->set_value(31, URandShort(0,255));
    
    prsub->set_value(32, URand(0,(2<<16)-1));
    prsub->set_value(33, URand(0,(2<<16)-1));
    
#ifdef CFG_HACK
    prsub->set_value(34, "padding");         // PADDING
#endif
    
    W_DO(_psub_man->add_tuple(_pssm, prsub));
    
    TRACE( TRACE_TRX_FLOW, "Added SUB - (%d)\n", sub_id);
    
    short type;
    
    // POPULATE ACCESS_INFO
    
    for (i=0; i<num_ai; ++i) {
	
	prai->set_value(0, sub_id);
	
	// AI_TYPE
	type = i+1;
	prai->set_value(1, type);
	
	// DATA 1,2
	prai->set_value(2, URandShort(0,255));
	prai->set_value(3, URandShort(0,255));
	
	// DATA 3,4
	char data3[TM1_AI_DATA3_SZ];
	URandFillStrCaps(data3,TM1_AI_DATA3_SZ);
	prai->set_value(4, data3);
	
	char data4[TM1_AI_DATA4_SZ];
	URandFillStrCaps(data4,TM1_AI_DATA4_SZ);
	prai->set_value(5, data4);      
		
#ifdef CFG_HACK
	prai->set_value(6, "padding");            // PADDING
#endif
	
	W_DO(_pai_man->add_tuple(_pssm, prai));
	
	TRACE( TRACE_TRX_FLOW, "Added AI-%d - (%d|%d|%s|%s)\n",
	       i,sub_id,i+1,data3,data4);
    }
    
    // POPULATE SPECIAL_FACILITY
    
    for (i=0; i<num_sf; ++i) {
	
	prsf->set_value(0, sub_id);
        
	// SF_TYPE 
	type = i+1;
	prsf->set_value(1, type);	

	prsf->set_value(2, (URand(1,100)<85? true : false));
	prsf->set_value(3, URandShort(0,255));
	prsf->set_value(4, URandShort(0,255));
	
	// DATA_B
	char datab[TM1_SF_DATA_B_SZ];
	URandFillStrCaps(datab,TM1_SF_DATA_B_SZ);
	prsf->set_value(5, datab);
	
#ifdef CFG_HACK
	prsf->set_value(6, "padding");            // PADDING
#endif
	
	W_DO(_psf_man->add_tuple(_pssm, prsf));
	
	TRACE( TRACE_TRX_FLOW, "Added SF-%d - (%d|%d|%s)\n",
	       i,sub_id,i+1,datab);

	// POPULATE CALL_FORWARDING
	
	// decide how many CF to have for this SF
	num_cf = URand(TM1_MIN_CF_PER_SF,
		       TM1_MAX_CF_PER_SF);
	
	short atime;
        
	for (j=0; j<num_cf; ++j) {
	    
	    prcf->set_value(0, sub_id);
	    
	    type = i+1;
	    prcf->set_value(1, type);
	    
	    atime = j*8;
	    prcf->set_value(2, atime);
	    
	    atime = j*8 + URandShort(1,8);
	    prcf->set_value(3, atime);
            
	    char numbx[TM1_CF_NUMBERX_SZ];
	    URandFillStrNumbx(numbx,TM1_CF_NUMBERX_SZ);
	    prcf->set_value(4, numbx);                	    
#ifdef CFG_HACK
	    prcf->set_value(5, "padding");                // PADDING
#endif
	    
	    W_DO(_pcf_man->add_tuple(_pssm, prcf));          
	    
	    TRACE( TRACE_TRX_FLOW, "Added CF-%d - (%d|%d|%d)\n",
		   i,sub_id,i+1,j*8);
	}
    }
    
    return RCOK;
}




/******************************************************************** 
 *
 * TM1 GET_SUB_DATA
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::xct_get_sub_data(const int xct_id, 
                                     get_sub_data_input_t& gsdin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    
    // Touches 1 table:
    // Subscriber
    tuple_guard<sub_man_impl> prsub(_psub_man);
    
    rep_row_t areprow(_psub_man->ts());

    // allocate space for the table representations
    areprow.set(_psub_desc->maxsize()); 

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
    TRACE( TRACE_TRX_FLOW, "App: %d GSD:sub-idx-probe (%d)\n", 
	   xct_id, gsdin._s_id);
    W_DO(_psub_man->sub_idx_probe(_pssm, prsub, gsdin._s_id));
    
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
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prsub->print_tuple();
#endif

    return RCOK;

} // EOF: GET_SUB_DATA




/******************************************************************** 
 *
 * TM1 GET_NEW_DEST
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::xct_get_new_dest(const int xct_id, 
                                     get_new_dest_input_t& gndin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // Touches 2 tables:
    // SpecialFacility and CallForwarding
    tuple_guard<sf_man_impl> prsf(_psf_man);
    tuple_guard<cf_man_impl> prcf(_pcf_man);

    // allocate space for the larger of the 2 table representations
    rep_row_t areprow(_pcf_man->ts());

    areprow.set(_pcf_desc->maxsize()); 

    prsf->_rep = &areprow;
    prcf->_rep = &areprow;

    rep_row_t lowrep(_pcf_man->ts());
    rep_row_t highrep(_pcf_man->ts());
    lowrep.set(_pcf_desc->maxsize()); 
    highrep.set(_pcf_desc->maxsize()); 

    tm1_sf_t asf;
    tm1_cf_t acf;
    bool eof;
    bool bFound = false;
    

    /* SELECT cf.numberx
     * FROM   Special_Facility AS sf, Call_Forwarding AS cf
     * WHERE
     *           (sf.s_id = <s_id rnd>
     *    AND    sf.sf_type = <sf_type rnd>
     *    AND    sf.is_active = 1)
     * AND    
     *           (cf.s_id = sf.s_id
     *    AND cf.sf_type = sf.sf_type)
     *
     * AND       (cf.start_time \<= <start_time rnd>
     *    AND <end_time rnd> \< cf.end_time);
     *
     * plan: index probe on "SF_IDX"
     *       iter on index  "CF_IDX"
     */

    // 1. Retrieve SpecialFacility (read-only)
    TRACE( TRACE_TRX_FLOW, "App: %d GND:sf-idx-probe (%d) (%d)\n", 
	   xct_id, gndin._s_id, gndin._sf_type);
    W_DO(_psf_man->sf_idx_probe(_pssm, prsf, 
				gndin._s_id, gndin._sf_type));    
    prsf->get_value(2, asf.IS_ACTIVE);

    // If it is and active special facility
    // 2. Retrieve the call forwarding destination (read-only)
    if (asf.IS_ACTIVE) {
	guard<index_scan_iter_impl<call_forwarding_t> > cf_iter;
	{
	    index_scan_iter_impl<call_forwarding_t>* tmp_cf_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d GND:cf-idx-iter\n", xct_id);
	    W_DO(_pcf_man->cf_get_idx_iter(_pssm, tmp_cf_iter, prcf,
					   lowrep, highrep,
					   gndin._s_id, gndin._sf_type, 
					   gndin._s_time));
	    cf_iter = tmp_cf_iter;
	}	
	W_DO(cf_iter->next(_pssm, eof, *prcf));
	while (!eof) {	    
	    // check the retrieved CF e_time                
	    prcf->get_value(3, acf.END_TIME);
	    if (acf.END_TIME > gndin._e_time) {
		prcf->get_value(4, acf.NUMBERX, 17);
		TRACE( TRACE_TRX_FLOW, "App: %d GND: found (%d) (%d) (%s)\n", 
		       xct_id, gndin._e_time, acf.END_TIME, acf.NUMBERX);
		bFound = true;
	    }	    
	    TRACE( TRACE_TRX_FLOW, "App: %d GND:cf-idx-iter-next\n", xct_id);
	    W_DO(cf_iter->next(_pssm, eof, *prcf));
	}
    }
    if (!bFound) { 
	return RC(se_NO_CURRENT_TUPLE); 
    }

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prsf->print_tuple();
    prcf->print_tuple();
#endif

    return RCOK;
    
} // EOF: GET_NEW_DEST




/******************************************************************** 
 *
 * TM1 GET_ACC_DATA
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::xct_get_acc_data(const int xct_id, 
                                     get_acc_data_input_t& gadin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // Touches 1 table:
    // AccessInfo
    tuple_guard<ai_man_impl> prai(_pai_man);

    rep_row_t areprow(_pai_man->ts());

    // allocate space for the table representations
    areprow.set(_pai_desc->maxsize()); 

    prai->_rep = &areprow;


    /* SELECT data1, data2, data3, data4
     * FROM   Access_Info
     * WHERE  s_id = <s_id rnd>
     * AND    ai_type = <ai_type rnd>
     *
     * plan: index probe on "AI_IDX"
     */

    // 1. retrieve AccessInfo (read-only)
    TRACE( TRACE_TRX_FLOW, "App: %d GAD:ai-idx-probe (%d) (%d)\n", 
	   xct_id, gadin._s_id, gadin._ai_type);
    W_DO(_pai_man->ai_idx_probe(_pssm, prai, gadin._s_id, gadin._ai_type));
    tm1_ai_t aai;
    prai->get_value(2,  aai.DATA1);
    prai->get_value(3,  aai.DATA2);
    prai->get_value(4,  aai.DATA3, 5);
    prai->get_value(5,  aai.DATA4, 9);

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prai->print_tuple();
#endif
    
    return RCOK;

} // EOF: GET_ACC_DATA




/******************************************************************** 
 *
 * TM1 UPD_SUB_DATA
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::xct_upd_sub_data(const int xct_id, 
                                     upd_sub_data_input_t& usdin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // Touches 2 tables:
    // Subscriber, SpecialFacility
    tuple_guard<sub_man_impl> prsub(_psub_man);
    tuple_guard<sf_man_impl> prsf(_psf_man);

    rep_row_t areprow(_psub_man->ts());

    // allocate space for the larger table representation
    areprow.set(_psub_desc->maxsize()); 

    prsub->_rep = &areprow;
    prsf->_rep = &areprow;


    /* UPDATE Subscriber
     * SET bit_1 = <bit_rnd>
     * WHERE s_id = <s_id rnd subid>;
     *
     * plan: index probe on "S_IDX"
     * 
     * UPDATE Special_Facility
     * SET data_a = <data_a rnd>
     * WHERE s_id = <s_id value subid>
     * AND sf_type = <sf_type rnd>;
     *
     * plan: index probe on "SF_IDX"
     */

    // IP: Moving the Upd(SF) first because it is the only 
    //     operation on this trx that may fail
    //#warning baseline::upd_sub_data does first the Upd(SF) and then Upd(Sub)
    
    // 1. Update SpecialFacility
    TRACE( TRACE_TRX_FLOW, "App: %d USD:sf-idx-upd (%d) (%d)\n", 
	   xct_id, usdin._s_id, usdin._sf_type);
    W_DO(_psf_man->sf_idx_upd(_pssm, prsf, usdin._s_id, usdin._sf_type));    
    prsf->set_value(4, usdin._a_data);        
    W_DO(_psf_man->update_tuple(_pssm, prsf));

    // 2. Update Subscriber
    TRACE( TRACE_TRX_FLOW, "App: %d USD:sub-idx-upd (%d)\n", 
	   xct_id, usdin._s_id);
    W_DO(_psub_man->sub_idx_upd(_pssm, prsub, usdin._s_id));
    prsub->set_value(2, usdin._a_bit);
    W_DO(_psub_man->update_tuple(_pssm, prsub));

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prsub->print_tuple();
    prsf->print_tuple();
#endif

    return RCOK;

} // EOF: UPD_SUB_DATA




/******************************************************************** 
 *
 * TM1 UPD_LOC
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::xct_upd_loc(const int xct_id, 
                                upd_loc_input_t& ulin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // Touches 1 table:
    // Subscriber
    tuple_guard<sub_man_impl> prsub(_psub_man);

    rep_row_t areprow(_psub_man->ts());

    // allocate space for the larger table representation
    areprow.set(_psub_desc->maxsize()); 

    prsub->_rep = &areprow;


    /* UPDATE Subscriber
     * SET    vlr_location = <vlr_location rnd>
     * WHERE  sub_nbr = <sub_nbr rndstr>;
     *
     * plan: index probe on "SUB_NBR_IDX"
     */

    // 1. Update Subscriber
    TRACE( TRACE_TRX_FLOW, "App: %d UL:sub-nbr-idx-upd (%d)\n", 
	   xct_id, ulin._s_id);
    W_DO(_psub_man->sub_nbr_idx_upd(_pssm, prsub, ulin._sub_nbr));
    prsub->set_value(33, ulin._vlr_loc);
    W_DO(_psub_man->update_tuple(_pssm, prsub));

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prsub->print_tuple();
#endif
    
    return RCOK;

} // EOF: UPD_LOC




/******************************************************************** 
 *
 * TM1 INS_CALL_FWD
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::xct_ins_call_fwd(const int xct_id, 
                                     ins_call_fwd_input_t& icfin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // Touches 3 tables:
    // Subscriber, SpecialFacility, CallForwarding
    tuple_guard<sub_man_impl> prsub(_psub_man);
    tuple_guard<sf_man_impl> prsf(_psf_man);
    tuple_guard<cf_man_impl> prcf(_pcf_man);

    rep_row_t areprow(_psub_man->ts());

    // allocate space for the larger table representation
    areprow.set(_psub_desc->maxsize()); 

    prsub->_rep = &areprow;
    prsf->_rep = &areprow;
    prcf->_rep = &areprow;

    rep_row_t lowrep(_psf_man->ts());
    rep_row_t highrep(_psf_man->ts());
    lowrep.set(_psf_desc->maxsize()); 
    highrep.set(_psf_desc->maxsize()); 

    tm1_sf_t  asf;
    bool bFound = false;
    bool eof;


    /* SELECT <s_id bind subid s_id>
     * FROM   Subscriber
     * WHERE  sub_nbr = <sub_nbr rndstr>;
     *
     * plan: index probe on "SUB_NBR_IDX"
     *
     * SELECT sf_type bind sfid sf_type>
     * FROM   Special_Facility
     * WHERE  s_id = <s_id value subid>
     *
     * plan: iter index on "SF_IDX"
     *
     * INSERT INTO Call_Forwarding
     * VALUES (<s_id value subid>, <sf_type rnd sf_type>,
     *         <start_time rnd>, <end_time rnd>, 
     *         <numberx rndstr>);
     */

    // 1. Retrieve Subscriber (Read-only)
    TRACE( TRACE_TRX_FLOW, "App: %d ICF:sub-nbr-idx (%d)\n", 
	   xct_id, icfin._s_id);
    W_DO(_psub_man->sub_nbr_idx_probe(_pssm, prsub, icfin._sub_nbr));
    prsub->get_value(0, icfin._s_id);
        
    // 2. Retrieve SpecialFacility (Read-only)
    guard<index_scan_iter_impl<special_facility_t> > sf_iter;
    {
	index_scan_iter_impl<special_facility_t>* tmp_sf_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d ICF:sf-idx-iter\n", xct_id);
	W_DO(_psf_man->sf_get_idx_iter(_pssm, tmp_sf_iter, prsf,
				       lowrep, highrep, icfin._s_id));
	sf_iter = tmp_sf_iter;
    }
    W_DO(sf_iter->next(_pssm, eof, *prsf));
    while (!eof) {
	// check the retrieved SF sf_type
	prsf->get_value(1, asf.SF_TYPE);
	if (asf.SF_TYPE == icfin._sf_type) {
	    TRACE( TRACE_TRX_FLOW, "App: %d ICF: found (%d) (%d)\n", 
		   xct_id, icfin._s_id, asf.SF_TYPE);
	    bFound = true;
	    break;
	}            
	TRACE( TRACE_TRX_FLOW, "App: %d ICF:sf-idx-iter-next\n", xct_id);
	W_DO(sf_iter->next(_pssm, eof, *prsf));
    }            
    if (bFound == false) 
	return RC(se_NO_CURRENT_TUPLE);     

    // 3. Check if it can successfully insert
    TRACE( TRACE_TRX_FLOW, "App: %d ICF:cf-idx-probe (%d) (%d) (%d)\n", 
	   xct_id, icfin._s_id, icfin._sf_type, icfin._s_time);
    w_rc_t e = _pcf_man->cf_idx_probe(_pssm, prcf, icfin._s_id,
				      icfin._sf_type, icfin._s_time);
    
    // idx probes return se_TUPLE_NOT_FOUND
    if (e.err_num() == se_TUPLE_NOT_FOUND) { 	
	// 4. Insert Call Forwarding record
	prcf->set_value(0, icfin._s_id);
	prcf->set_value(1, icfin._sf_type);
	prcf->set_value(2, icfin._s_time);
	prcf->set_value(3, icfin._e_time);
	prcf->set_value(4, icfin._numberx);                	
#ifdef CFG_HACK
	prcf->set_value(5, "padding"); // PADDING
#endif
	TRACE (TRACE_TRX_FLOW, "App: %d ICF:ins-cf\n", xct_id);
	W_DO(_pcf_man->add_tuple(_pssm, prcf));
    } else { // in any other case it should fail	
	return RC(se_CANNOT_INSERT_TUPLE);
    }        

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prsub->print_tuple();
    prsf->print_tuple();
    prcf->print_tuple();
#endif

    return RCOK;
    
} // EOF: INS_CALL_FWD




/******************************************************************** 
 *
 * TM1 DEL_CALL_FWD
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::xct_del_call_fwd(const int xct_id, 
                                     del_call_fwd_input_t& dcfin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // Touches 2 tables:
    // Subscriber, CallForwarding
    tuple_guard<sub_man_impl> prsub(_psub_man);
    tuple_guard<cf_man_impl> prcf(_pcf_man);

    rep_row_t areprow(_psub_man->ts());

    // allocate space for the larger table representation
    areprow.set(_psub_desc->maxsize()); 

    prsub->_rep = &areprow;
    prcf->_rep = &areprow;


    /* SELECT <s_id bind subid s_id>
     * FROM Subscriber
     * WHERE sub_nbr = <sub_nbr rndstr>;
     *
     * plan: index probe on "SUB_NBR_IDX"
     *
     * DELETE FROM Call_Forwarding
     * WHERE s_id = <s_id value subid>
     * AND sf_type = <sf_type rnd>
     * AND start_time = <start_time rnd>;
     *
     * plan: index probe on "CF_IDX"     
     */

    // 1. Retrieve Subscriber (Read-only)
    TRACE( TRACE_TRX_FLOW, "App: %d DCF:sub-nbr-idx (%d)\n", 
	   xct_id, dcfin._s_id);
    W_DO(_psub_man->sub_nbr_idx_probe(_pssm, prsub, dcfin._sub_nbr));
    prsub->get_value(0, dcfin._s_id);
    
    // 2. Delete CallForwarding record
    TRACE( TRACE_TRX_FLOW, "App: %d DCF:cf-idx-upd (%d) (%d) (%d)\n", 
	   xct_id, dcfin._s_id, dcfin._sf_type, dcfin._s_time);
    W_DO(_pcf_man->cf_idx_upd(_pssm, prcf, 
			      dcfin._s_id, dcfin._sf_type, dcfin._s_time));
    TRACE( TRACE_TRX_FLOW, "App: %d DCF:del-cf\n", xct_id);        
    W_DO(_pcf_man->delete_tuple(_pssm, prcf));

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prsub->print_tuple();
    prcf->print_tuple();
#endif

    return RCOK;

} // EOF: DEL_CALL_FWD




/******************************************************************** 
 *
 * TM1 GET_SUB_NBR
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::xct_get_sub_nbr(const int xct_id, 
                                    get_sub_nbr_input_t& gsnin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // Touches 1 table:
    // Subscriber
    tuple_guard<sub_man_impl> prsub(_psub_man);

    rep_row_t areprow(_psub_man->ts());

    // allocate space for the larger table representation
    areprow.set(_psub_desc->maxsize()); 

    prsub->_rep = &areprow;

    rep_row_t lowrep(_psub_man->ts());
    rep_row_t highrep(_psub_man->ts());
    lowrep.set(_psub_desc->maxsize()); 
    highrep.set(_psub_desc->maxsize()); 

    bool eof;
    uint range = get_rec_to_access();

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
	       xct_id, gsnin._s_id, range);
	W_DO(_psub_man->sub_get_idx_iter(_pssm, tmp_sub_iter, prsub, 
					 lowrep,highrep,
					 gsnin._s_id,range,
					 SH,     // read-only access
					 true));  // retrieve record
	sub_iter = tmp_sub_iter;
    }

    // 2. Read all the returned records
    W_DO(sub_iter->next(_pssm, eof, *prsub));
    while (!eof) {
	prsub->get_value(0, sid);
	prsub->get_value(33, vlrloc);
	TRACE( TRACE_TRX_FLOW, "App: %d GSN: read (%d) (%d)\n", 
	       xct_id, sid, vlrloc);
	W_DO(sub_iter->next(_pssm, eof, *prsub));
    }
    
    return RCOK;
    
} // EOF: GET_SUB_NBR




/********************************************************************
 *
 * TM1 INS_CALL_FWD_BENCH
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::xct_ins_call_fwd_bench(const int xct_id,
					   ins_call_fwd_bench_input_t& icfbin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // Touches 3 tables:
    // Subscriber, SpecialFacility, CallForwarding
    tuple_guard<sub_man_impl> prsub(_psub_man);
    tuple_guard<cf_man_impl> prcf(_pcf_man);

    rep_row_t areprow(_psub_man->ts());

    // allocate space for the larger table representation
    areprow.set(_psub_desc->maxsize());

    prsub->_rep = &areprow;
    prcf->_rep = &areprow;

    // try to insert to call_forwarding_t if the record to insert
    // is not already there, otherwise delete the found record

    // 1. Retrieve Subscriber (Read-only)
    TRACE( TRACE_TRX_FLOW, "App: %d ICFB:sub-nbr-idx (%d)\n",
	   xct_id, icfbin._s_id);
    W_DO(_psub_man->sub_nbr_idx_probe(_pssm, prsub, icfbin._sub_nbr));
    prsub->get_value(0, icfbin._s_id);

    // 2. Check if it can successfully insert
    TRACE( TRACE_TRX_FLOW, "App: %d ICFB:cf-idx-probe (%d) (%d) (%d)\n",
	   xct_id, icfbin._s_id, icfbin._sf_type, icfbin._s_time);
    w_rc_t e = _pcf_man->cf_idx_upd(_pssm, prcf, icfbin._s_id,
				    icfbin._sf_type, icfbin._s_time);
    
    // idx probes return se_TUPLE_NOT_FOUND
    if (e.is_error()) {
	if (e.err_num() != se_TUPLE_NOT_FOUND) {
	    W_DO(e);	    
	}
	// 3. Insert Call Forwarding record
	prcf->set_value(0, icfbin._s_id);
	prcf->set_value(1, icfbin._sf_type);
	prcf->set_value(2, icfbin._s_time);
	prcf->set_value(3, icfbin._e_time);
	prcf->set_value(4, icfbin._numberx);	    
#ifdef CFG_HACK
	prcf->set_value(5, "padding"); // PADDING
#endif	    
	TRACE( TRACE_TRX_FLOW, "App: %d ICF:ins-cf\n", xct_id);	    
	W_DO(_pcf_man->add_tuple(_pssm, prcf));
    } else { // 3. Delete Call Forwarding record if tuple found
	TRACE( TRACE_TRX_FLOW, "App: %d DCF:del-cf\n", xct_id);
	W_DO(_pcf_man->delete_tuple(_pssm, prcf));
    }

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction
    // dumps the status of all the table rows used
    prsub->print_tuple();
    prcf->print_tuple();
#endif
    
    return RCOK;
    
} // EOF: INS_CALL_FWD_BENCH




/********************************************************************
 *
 * TM1 DEL_CALL_FWD_BENCH
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::xct_del_call_fwd_bench(const int xct_id,
					   del_call_fwd_bench_input_t& dcfbin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // Touches 2 tables:
    // Subscriber, CallForwarding
    tuple_guard<sub_man_impl> prsub(_psub_man);
    tuple_guard<cf_man_impl> prcf(_pcf_man);

    rep_row_t areprow(_psub_man->ts());

    // allocate space for the larger table representation
    areprow.set(_psub_desc->maxsize());

    prsub->_rep = &areprow;
    prcf->_rep = &areprow;


    /* SELECT <s_id bind subid s_id>
     * FROM Subscriber
     * WHERE sub_nbr = <sub_nbr rndstr>;
     *
     * plan: index probe on "SUB_NBR_IDX"
     *
     * DELETE FROM Call_Forwarding
     * WHERE s_id = <s_id value subid>
     * AND sf_type = <sf_type rnd>
     * AND start_time = <start_time rnd>;
     *
     * plan: index probe on "CF_IDX"
     */

    // 1. Retrieve Subscriber (Read-only)
    TRACE( TRACE_TRX_FLOW, "App: %d DCFB:sub-nbr-idx (%d)\n",
	   xct_id, dcfbin._s_id);	
    W_DO(_psub_man->sub_nbr_idx_probe(_pssm, prsub, dcfbin._sub_nbr));
    prsub->get_value(0, dcfbin._s_id);
	
    // 2. Delete CallForwarding record
    TRACE( TRACE_TRX_FLOW, "App: %d DCFB:cf-idx-upd (%d) (%d) (%d)\n",
	   xct_id, dcfbin._s_id, dcfbin._sf_type, dcfbin._s_time);    
    w_rc_t e = _pcf_man->cf_idx_upd(_pssm, prcf, dcfbin._s_id,
				    dcfbin._sf_type, dcfbin._s_time);

    // If record not found
    if (e.is_error()) { 
	if (e.err_num() != se_TUPLE_NOT_FOUND) {
	    W_DO(e);
	}
	// 3. Insert Call Forwarding record
	prcf->set_value(0, dcfbin._s_id);
	prcf->set_value(1, dcfbin._sf_type);
	prcf->set_value(2, dcfbin._s_time);
	short atime = URand(1,24);
	prcf->set_value(3, atime);
	char numbx[TM1_CF_NUMBERX_SZ];
	URandFillStrNumbx(numbx,TM1_CF_NUMBERX_SZ);
	prcf->set_value(4, numbx);
#ifdef CFG_HACK
	prcf->set_value(5, "padding"); // PADDING
#endif
	TRACE( TRACE_TRX_FLOW, "App: %d DCFB:ins-cf\n", xct_id);
	W_DO(_pcf_man->add_tuple(_pssm, prcf));
    } else {
	TRACE( TRACE_TRX_FLOW, "App: %d DCF:del-cf\n", xct_id);
	W_DO(_pcf_man->delete_tuple(_pssm, prcf));
    }

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction
    // dumps the status of all the table rows used
    prsub->print_tuple();
    prcf->print_tuple();
#endif

    return RCOK;
    
} // EOF: DEL_CALL_FWD_BENCH


EXIT_NAMESPACE(tm1);
