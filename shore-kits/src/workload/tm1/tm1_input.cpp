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

/** @file:  tm1_input.cpp
 *
 *  @brief: Implementation of the (common) inputs for the TM1 trxs
 */

#include "k_defines.h"

#include "workload/tm1/tm1_input.h"

#warning TM1: Using UZRand for generating skewed Zipfian inputs. Skewness can be controlled by the 'zipf' shell command.

ENTER_NAMESPACE(tm1);	

// related to dynamic skew for load imbalance
skewer_t s_skewer;
bool _change_load = false;

/* -------------------- */
/* --- GET_SUB_DATA --- */
/* -------------------- */


get_sub_data_input_t& get_sub_data_input_t::operator= (const get_sub_data_input_t& rhs) 
{
    _s_id = rhs._s_id;
    return (*this);
}


get_sub_data_input_t create_get_sub_data_input(int sf, 
                                               int specificSub)
{
    assert (sf>0);

    get_sub_data_input_t gsdin;

    if(_change_load) {
	gsdin._s_id = s_skewer.get_input();
    }
    else {
	if (specificSub>0)
	    gsdin._s_id = specificSub;
	else
	    gsdin._s_id = UZRand(1,sf*TM1_SUBS_PER_SF);
    }
    
    return (gsdin);
}




/* -------------------- */
/* --- GET_NEW_DEST --- */
/* -------------------- */


get_new_dest_input_t& get_new_dest_input_t::operator= (const get_new_dest_input_t& rhs) 
{
    _s_id = rhs._s_id;
    _sf_type = rhs._sf_type;
    _s_time = rhs._s_time;
    _e_time = rhs._e_time;
    return (*this);
}


get_new_dest_input_t create_get_new_dest_input(int sf, 
                                               int specificSub)
{
    assert (sf>0);

    get_new_dest_input_t gndin;

    if(_change_load) {
	gndin._s_id = s_skewer.get_input();
    } else {
	if (specificSub>0)
	    gndin._s_id = specificSub;
	else
	    gndin._s_id = UZRand(1,sf*TM1_SUBS_PER_SF);
    }
    
    gndin._sf_type = URand(1,4);
    gndin._s_time = URand(0,2) * 8;
    gndin._e_time = URand(1,24);
    return (gndin);
}



/* -------------------- */
/* --- GET_ACC_DATA --- */
/* -------------------- */

get_acc_data_input_t& get_acc_data_input_t::operator= (const get_acc_data_input_t& rhs) 
{
    _s_id = rhs._s_id;
    _ai_type = rhs._ai_type;
    return (*this);
}


get_acc_data_input_t create_get_acc_data_input(int sf, 
                                               int specificSub)
{
    assert (sf>0);

    get_acc_data_input_t gadin;

    if(_change_load) {
	gadin._s_id = s_skewer.get_input();
    } else {
	if (specificSub>0)
	    gadin._s_id = specificSub;
	else
	    gadin._s_id = UZRand(1,sf*TM1_SUBS_PER_SF);
    }
    
    gadin._ai_type = URand(1,4);
    return (gadin);
}


/* -------------------- */
/* --- UPD_SUB_DATA --- */
/* -------------------- */

upd_sub_data_input_t& upd_sub_data_input_t::operator= (const upd_sub_data_input_t& rhs) 
{
    _s_id = rhs._s_id;
    _sf_type = rhs._sf_type;
    _a_bit = rhs._a_bit;
    _a_data = rhs._a_data;
    return (*this);
}


upd_sub_data_input_t create_upd_sub_data_input(int sf, 
                                               int specificSub)
{
    assert (sf>0);

    upd_sub_data_input_t usdin;

    if(_change_load) {
	usdin._s_id = s_skewer.get_input();
    } else {
	if (specificSub>0)
	    usdin._s_id = specificSub;
	else
	    usdin._s_id = UZRand(1,sf*TM1_SUBS_PER_SF);
    }
    
    usdin._sf_type = URand(1,4);
    usdin._a_bit = URandBool();
    usdin._a_data = URand(0,255);
    return (usdin);
}


/* --------------- */
/* --- UPD_LOC --- */
/* --------------- */


upd_loc_input_t& upd_loc_input_t::operator= (const upd_loc_input_t& rhs) 
{
    _s_id = rhs._s_id;
    store_string(_sub_nbr, rhs._sub_nbr);
    _vlr_loc = rhs._vlr_loc;
    return (*this);
}


upd_loc_input_t create_upd_loc_input(int sf, 
                                     int specificSub)
{
    assert (sf>0);

    upd_loc_input_t ulin;

    if(_change_load) {
	ulin._s_id = s_skewer.get_input();
    } else {
	if (specificSub>0)
	    ulin._s_id = specificSub;
	else
	    ulin._s_id = UZRand(1,sf*TM1_SUBS_PER_SF);
    }
    
    sprintf(ulin._sub_nbr,"%015.15d",ulin._s_id);
    ulin._vlr_loc = URand(0,(2<<16)-1);
    return (ulin);
}


/* -------------------- */
/* --- INS_CALL_FWD --- */
/* -------------------- */


ins_call_fwd_input_t& ins_call_fwd_input_t::operator= (const ins_call_fwd_input_t& rhs) 
{
    _s_id = rhs._s_id;
    store_string(_sub_nbr, rhs._sub_nbr);
    _sf_type = rhs._sf_type;
    _s_time = rhs._s_time;
    _e_time = rhs._e_time;
    store_string(_numberx, rhs._numberx);
    return (*this);
}


ins_call_fwd_input_t create_ins_call_fwd_input(int sf, 
                                               int specificSub)
{
    assert (sf>0);

    ins_call_fwd_input_t icfin;

    if(_change_load) {
	icfin._s_id = s_skewer.get_input();
    } else {
	if (specificSub>0)
	    icfin._s_id = specificSub;
	else
	    icfin._s_id = UZRand(1,sf*TM1_SUBS_PER_SF);
    }
    
    sprintf(icfin._sub_nbr,"%015.15d",icfin._s_id);
    icfin._sf_type = URand(1,4);
    icfin._s_time = URand(0,2) * 8;
    icfin._e_time = URand(1,24);
    sprintf(icfin._numberx,"%015.15d",URand(1,sf*TM1_SUBS_PER_SF));
    return (icfin);
}


/* -------------------- */
/* --- DEL_CALL_FWD --- */
/* -------------------- */

del_call_fwd_input_t& del_call_fwd_input_t::operator= (const del_call_fwd_input_t& rhs) 
{
    _s_id = rhs._s_id;
    store_string(_sub_nbr, rhs._sub_nbr);
    _sf_type = rhs._sf_type;
    _s_time = rhs._s_time;
    return (*this);
}


del_call_fwd_input_t create_del_call_fwd_input(int sf, 
                                               int specificSub)
{
    assert (sf>0);

    del_call_fwd_input_t dcfin;

    if(_change_load) {
	dcfin._s_id = s_skewer.get_input();	
    } else {
	if (specificSub>0)
	    dcfin._s_id = specificSub;
	else
	    dcfin._s_id = UZRand(1,sf*TM1_SUBS_PER_SF);
    }
    
    sprintf(dcfin._sub_nbr,"%015.15d",dcfin._s_id);
    dcfin._sf_type = URand(1,4);
    dcfin._s_time = URand(0,2) * 8;
    return (dcfin);
}



/* ------------------- */
/* --- GET_SUB_NBR --- */
/* ------------------- */


get_sub_nbr_input_t& get_sub_nbr_input_t::operator= (const get_sub_nbr_input_t& rhs) 
{
    _s_id = rhs._s_id;
    _range = rhs._range;

#ifdef USE_DORA_EXT_IDX
    _pairs = rhs._pairs;
#endif    

    return (*this);
}


get_sub_nbr_input_t create_get_sub_nbr_input(int sf, 
                                             int specificSub)
{
    assert (sf>0);

    get_sub_nbr_input_t gsnin;

    if(_change_load) {
	gsnin._s_id = s_skewer.get_input();
    } else {
	if (specificSub>0)
	    gsnin._s_id = specificSub;
	else
	    gsnin._s_id = UZRand(1,sf*TM1_SUBS_PER_SF);
    }
    
    return (gsnin);
}



/* -------------------------- */
/* --- INS_CALL_FWD_BENCH --- */
/* ------------------------- */


ins_call_fwd_bench_input_t& ins_call_fwd_bench_input_t::operator= (const ins_call_fwd_bench_input_t& rhs) 
{
    _s_id = rhs._s_id;
    store_string(_sub_nbr, rhs._sub_nbr);
    _sf_type = rhs._sf_type;
    _s_time = rhs._s_time;
    _e_time = rhs._e_time;
    store_string(_numberx, rhs._numberx);
    return (*this);
}


ins_call_fwd_bench_input_t create_ins_call_fwd_bench_input(int sf, 
							   int specificSub)
{
    assert (sf>0);

    ins_call_fwd_bench_input_t icfbin;

    if (specificSub>0)
        icfbin._s_id = specificSub;
    else
        icfbin._s_id = UZRand(1,sf*TM1_SUBS_PER_SF);

    sprintf(icfbin._sub_nbr,"%015.15d",icfbin._s_id);
    icfbin._sf_type = URand(1,4);
    icfbin._s_time = URand(0,2) * 8;
    icfbin._e_time = URand(1,24);
    sprintf(icfbin._numberx,"%015.15d",URand(1,sf*TM1_SUBS_PER_SF));
    return (icfbin);
}


/* -------------------------- */
/* --- DEL_CALL_FWD_BENCH --- */
/* -------------------------- */

del_call_fwd_bench_input_t& del_call_fwd_bench_input_t::operator= (const del_call_fwd_bench_input_t& rhs) 
{
    _s_id = rhs._s_id;
    store_string(_sub_nbr, rhs._sub_nbr);
    _sf_type = rhs._sf_type;
    _s_time = rhs._s_time;
    return (*this);
}


del_call_fwd_bench_input_t create_del_call_fwd_bench_input(int sf, 
							   int specificSub)
{
    assert (sf>0);

    del_call_fwd_bench_input_t dcfbin;

    if (specificSub>0)
        dcfbin._s_id = specificSub;
    else
        dcfbin._s_id = UZRand(1,sf*TM1_SUBS_PER_SF);

    sprintf(dcfbin._sub_nbr,"%015.15d",dcfbin._s_id);
    dcfbin._sf_type = URand(1,4);
    dcfbin._s_time = URand(0,2) * 8;
    return (dcfbin);
}







/******************************************************************************* 
 * 
 *  @func:  random_tm1_xct_type
 *  
 *  @brief: Translates or picks a random xct type given the benchmark 
 *          specification
 *
 *******************************************************************************/

int random_tm1_xct_type(const int selected)
{
    int random_type = selected;
    if (random_type < 0)
        random_type = rand()%100;
    assert (random_type >= 0);

    int sum = 0;
    sum+=PROB_TM1_GET_SUB_DATA;
    if (random_type < sum)
	return XCT_TM1_GET_SUB_DATA;

    sum+=PROB_TM1_GET_NEW_DEST;
    if (random_type < sum)
	return XCT_TM1_GET_NEW_DEST;


    sum+=PROB_TM1_GET_ACC_DATA;
    if (random_type < sum)
	return XCT_TM1_GET_ACC_DATA;

    sum+=PROB_TM1_UPD_SUB_DATA;
    if (random_type < sum)
	return XCT_TM1_UPD_SUB_DATA;

    sum+=PROB_TM1_UPD_LOCATION;
    if (random_type < sum)
	return XCT_TM1_UPD_LOCATION;

    sum+=PROB_TM1_INS_CALL_FWD;
    if (random_type < sum)
	return XCT_TM1_INS_CALL_FWD;

    sum+=PROB_TM1_DEL_CALL_FWD;
    if (random_type < sum)
	return XCT_TM1_DEL_CALL_FWD;

    // should not reach this point
    assert (0);
    return (0);
}



EXIT_NAMESPACE(tm1);
