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

/** @file tpcc_input.cpp
 *
 *  @brief Implementation of the (common) inputs for the TPC-C trxs
 *  @brief Generate inputs for the TPCC TRXs
 *
 *  @author Ippokratis Pandis (ipandis)
 */


#include "util.h"
#include "workload/tpcc/tpcc_random.h" 
#include "workload/tpcc/tpcc_input.h"



ENTER_NAMESPACE(tpcc);



// uncomment line below to use produce the same input
//#define USE_SAME_INPUT

// uncomment line below to produce "safe" (tested code paths) inputs
#undef USE_SAFE_PATHS
//#define USE_SAFE_PATHS 

// uncomment line below to generate NewOrder inputs for rollback
#undef USE_NO_NORD_INPUTS_FOR_ROLLBACK
//#define USE_NO_NORD_INPUTS_FOR_ROLLBACK

// uncomment line below to query only local WHs
#undef USE_ONLY_LOCAL_WHS
#define USE_ONLY_LOCAL_WHS


// prints out warnings about the configuration
#ifdef USE_SAME_INPUT
#warning TPCC - Uses xcts with same input
#endif

#ifdef USE_SAFE_PATHS
#warning TPCC - Does not generate xcts that probe C_NAME_IDX
#endif

#ifdef USE_NO_NORD_INPUTS_FOR_ROLLBACK
#warning TPCC - Does not generate aborting NewOrder xcts
#endif

#ifdef USE_ONLY_LOCAL_WHS
#warning TPCC - Uses only local Warehouses 
#endif

// related to dynamic skew for load imbalance
skewer_t w_skewer;
bool _change_load = false;

/* ----------------------- */
/* --- NEW_ORDER_INPUT --- */
/* ----------------------- */

ol_item_info& 
ol_item_info::operator= (const ol_item_info& rhs)
{
    _ol_i_id = rhs._ol_i_id;
    _ol_supply_wh_select = rhs._ol_supply_wh_select;
    _ol_supply_wh_id = rhs._ol_supply_wh_id;
    _ol_quantity = rhs._ol_quantity;

    _item_amount = rhs._item_amount;

    _astock = rhs._astock;
    _aitem = rhs._aitem;

    return (*this);
}

new_order_input_t& 
new_order_input_t::operator= (const new_order_input_t& rhs) 
{
    // copy input
    _wh_id = rhs._wh_id;
    _d_id = rhs._d_id;
    _c_id = rhs._c_id;
    _ol_cnt = rhs._ol_cnt;
    _rbk = rhs._rbk;

    _tstamp = rhs._tstamp;
    _all_local = rhs._all_local;
    _d_next_o_id = rhs._d_next_o_id;

    _awh   = rhs._awh;
    _acust = rhs._acust;
    _adist = rhs._adist;

    for (int i=0; i<rhs._ol_cnt; i++) {
        items[i] = rhs.items[i];
    }

    return (*this);
}


#ifdef CFG_DORA
void 
new_order_input_t::get_no_item_input(no_item_nord_input_t& anoin)
{
    anoin._wh_id = _wh_id;
    anoin._d_id = _d_id;
    anoin._c_id = _c_id;
    anoin._ol_cnt = _ol_cnt;
    anoin._rbk = _rbk;

    anoin._tstamp = _tstamp;
    anoin._all_local = _all_local;
    anoin._d_next_o_id = _d_next_o_id;    
}

void 
new_order_input_t::get_with_item_input(with_item_nord_input_t& awin, 
                                       const int idx)
{
    awin._wh_id = _wh_id;
    awin._d_id = _d_id;
    awin._c_id = _c_id;
    awin._ol_cnt = _ol_cnt;
    awin._rbk = _rbk;

    awin._tstamp = _tstamp;
    awin._all_local = _all_local;
    awin._d_next_o_id = _d_next_o_id;    

    awin._ol_idx = idx;

    awin.item = items[idx];
}

no_item_nord_input_t& 
no_item_nord_input_t::operator= (const no_item_nord_input_t& rhs) 
{
    // copy input
    _wh_id = rhs._wh_id;
    _d_id = rhs._d_id;
    _c_id = rhs._c_id;
    _ol_cnt = rhs._ol_cnt;
    _rbk = rhs._rbk;

    _tstamp = rhs._tstamp;
    _all_local = rhs._all_local;
    _d_next_o_id = rhs._d_next_o_id;

    return (*this);
}

with_item_nord_input_t& 
with_item_nord_input_t::operator= (const with_item_nord_input_t& rhs) 
{
    // copy input
    _wh_id = rhs._wh_id;
    _d_id = rhs._d_id;
    _c_id = rhs._c_id;
    _ol_cnt = rhs._ol_cnt;
    _rbk = rhs._rbk;

    _tstamp = rhs._tstamp;
    _all_local = rhs._all_local;
    _d_next_o_id = rhs._d_next_o_id;

    _ol_idx = rhs._ol_idx;

    item = rhs.item;

    return (*this);
}
#endif


/********************************************************************* 
 *
 *  @fn:    create_new_order_input
 *
 *  @brief: Creates the input for a new NEW_ORDER request, 
 *          given the scaling factor (sf) of the database
 *
 *********************************************************************/ 

new_order_input_t create_new_order_input(int sf, int specificWH) 
{
    // check scaling factor
    assert (sf>0);

    // produce NEW_ORDER params according to tpcc spec v.5.9
    new_order_input_t noin;

#ifndef USE_SAME_INPUT    

    if(_change_load) {
	noin._wh_id = w_skewer.get_input();
    } else {
	if (specificWH>0)
	    noin._wh_id = specificWH;
	else
	    noin._wh_id = URand(1, sf);
    }
    
    noin._d_id   = URand(1, 10);
    noin._c_id   = NURand(1023, 1, 3000);
    noin._ol_cnt = URand(5, 15);
    noin._rbk    = URand(1, 100); // if rbk == 1 - ROLLBACK

    noin._tstamp = time(NULL);

    // generate the items order
    for (int i=0; i<noin._ol_cnt; i++) {
        noin.items[i]._ol_i_id = NURand(8191, 1, 100000);
        noin.items[i]._ol_supply_wh_select = URand(1, 100); // 1 - 99
        noin.items[i]._ol_quantity = URand(1, 10);

#ifndef USE_ONLY_LOCAL_WHS
        if (noin.items[i]._ol_supply_wh_select == 1) {
            // remote new_order
            noin.items[i]._ol_supply_wh_id = URand(1, sf);
            if (noin.items[i]._ol_supply_wh_id != noin._wh_id)
                noin._all_local = 0; // if indeed remote WH, then update all_local flag
        }
        else {
            // home new_order
            noin.items[i]._ol_supply_wh_id = noin._wh_id;
        }
#else
        noin.items[i]._ol_supply_wh_id = noin._wh_id;
#endif        
    }

#ifndef USE_NO_NORD_INPUTS_FOR_ROLLBACK
    if (noin._rbk == 1) {   
        // generate an input that it will cause a rollback
        noin.items[noin._ol_cnt-1]._ol_i_id = -1;
        TRACE( TRACE_TRX_FLOW, "Bad input...\n");
    }
#endif


#else
    // same input
    noin._wh_id  = 1;
    noin._d_id   = 2;
    noin._c_id   = 3;
    noin._ol_cnt = 10;
    noin._rbk    = 50;

    // generate the items order
    for (int i=0; i<noin._ol_cnt; i++) {
        noin.items[i]._ol_i_id = 1;
        noin.items[i]._ol_supply_wh_select = 50;      
        noin.items[i]._ol_supply_wh_id = noin._wh_id; /* home new_order */ 
        noin.items[i]._ol_quantity = 5;
    }
#endif        

    return (noin);

}; // EOF: create_new_order




/* --------------------- */
/* --- PAYMENT_INPUT --- */
/* --------------------- */

payment_input_t& 
payment_input_t::operator= (const payment_input_t& rhs) 
{
    // copy input
    _home_wh_id = rhs._home_wh_id;
    _home_d_id = rhs._home_d_id;
    _v_cust_wh_selection = rhs._v_cust_wh_selection;
    _remote_wh_id = rhs._remote_wh_id;
    _remote_d_id = rhs._remote_d_id;
    _v_cust_ident_selection = rhs._v_cust_ident_selection;
    _c_id = rhs._c_id;
    _h_amount = rhs._h_amount;

    if (rhs._c_last) {
        store_string(_c_last, rhs._c_last);
    }        

    return (*this);
}



/********************************************************************* 
 *
 *  @fn:    create_payment_input
 *
 *  @brief: Creates the input for a new PAYMENT request, 
 *          given the scaling factor (sf) of the database
 *
 *********************************************************************/ 

payment_input_t create_payment_input(int sf, int specificWH) 
{
    // check scaling factor
    assert (sf>0);

    // produce PAYMENT params according to tpcc spec v.5.9
    payment_input_t pin;

#ifndef USE_SAME_INPUT

    if(_change_load) {
	pin._home_wh_id = w_skewer.get_input();	
    } else {
	if (specificWH>0)
	    pin._home_wh_id = specificWH;
	else
	    pin._home_wh_id = URand(1, sf);
    }
    
    pin._home_d_id = URand(1, 10);    
    pin._h_amount = (long)URand(100, 500000)/(long)100.00;
    pin._h_date = time(NULL);

#ifndef USE_ONLY_LOCAL_WHS
    pin._v_cust_wh_selection = URand(1, 100); // 85 - 15        
    if (pin._v_cust_wh_selection <= 85) {
        // all local payment
        pin._remote_wh_id = pin._home_wh_id;
        pin._remote_d_id = pin._home_d_id;
    }
    else {
        // remote warehouse
        if (sf == 1) {
            pin._remote_wh_id = 1;
        }
        else {
            // pick a remote wh (different from the home_wh)
            do {
                pin._remote_wh_id = URand(1, sf);
            } while (pin._home_wh_id == pin._remote_wh_id);
        }
        pin._remote_d_id = URand(1, 10);
    }
#else
    pin._v_cust_wh_selection = 50;
    pin._remote_wh_id = pin._home_wh_id;
    pin._remote_d_id = pin._home_d_id;
#endif


#ifdef USE_SAFE_PATHS
    pin._v_cust_ident_selection = URand(61, 100); // 60 - 40
#else
    pin._v_cust_ident_selection = URand(1, 100); // 60 - 40
#endif

    if (pin._v_cust_ident_selection <= 60) {
        // Calls the function that returns the correct cust_last
        generate_cust_last(NURand(255,0,999), pin._c_last);    
    }
    else {
        pin._c_id = NURand(1023, 1, 3000);
    }

#else
    // Same input
    pin._home_wh_id = 1;
    pin._home_d_id =  2;
    pin._v_cust_wh_selection = 80;
    pin._remote_wh_id = 1;
    pin._remote_d_id =  3;
    pin._v_cust_ident_selection = 50;
    pin._c_id =  1500;        
    //pin._c_last = NULL;
    pin._h_amount = 1000.00;
    pin._h_date = time(NULL);
#endif        

    return (pin);

}; // EOF: create_payment



/* -------------------------- */
/* --- ORDER_STATUS_INPUT --- */
/* -------------------------- */

order_status_input_t& 
order_status_input_t::operator= (const order_status_input_t& rhs) 
{
    _wh_id    = rhs._wh_id;
    _d_id     = rhs._d_id;
    _c_select = rhs._c_select;
    _c_id     = rhs._c_id;
        
    _o_id = rhs._o_id;
    _o_ol_cnt = rhs._o_ol_cnt;
    
    if (rhs._c_last) {
        store_string(_c_last, rhs._c_last);
    }

    return (*this);
}



/********************************************************************* 
 *
 *  @fn:    create_order_status_input
 *
 *  @brief: Creates the input for a new ORDER_STATUS request, 
 *          given the scaling factor (sf) of the database
 *
 *********************************************************************/ 

order_status_input_t create_order_status_input(int sf, int specificWH)
{
    // check scaling factor
    assert (sf > 0);

    // produce PAYMENT params according to tpcc spec v.5.4
    order_status_input_t osin;

#ifndef USE_SAME_INPUT    

    if(_change_load) {
	osin._wh_id = w_skewer.get_input();
    } else {
	if (specificWH>0)
	    osin._wh_id = specificWH;
	else
	    osin._wh_id    = URand(1, sf);
    }
    
    osin._d_id     = URand(1, 10);

#ifdef USE_SAFE_PATHS
    osin._c_select = URand(61, 100);
#else
    osin._c_select = URand(1, 100); /* 60% - 40% */
#endif

    if (osin._c_select <= 60) {
        // Calls the function that returns the correct cust_last
        generate_cust_last(NURand(255,0,999), osin._c_last);    
    }
    else {
        osin._c_id = NURand(1023, 1, 3000);
    }

#else
    // same input
    osin._wh_id    = 1;
    osin._d_id     = 2;
    osin._c_select = 80;
    osin._c_id     = 3;
    //osin._c_last   = NULL;
#endif

    return (osin);

}; // EOF: create_order_status



/* ---------------------- */
/* --- DELIVERY_INPUT --- */
/* ---------------------- */

delivery_input_t& 
delivery_input_t::operator= (const delivery_input_t& rhs) 
{
    _wh_id      = rhs._wh_id;
    _carrier_id = rhs._carrier_id;

    return (*this);
}



/********************************************************************* 
 *
 *  @fn:    create_delivery_input
 *
 *  @brief: Creates the input for a new DELIVERY request, 
 *          given the scaling factor (sf) of the database
 *
 *********************************************************************/ 

delivery_input_t create_delivery_input(int sf, int specificWH)
{
    // check scaling factor
    assert (sf > 0);

    // produce PAYMENT params according to tpcc spec v.5.9
    delivery_input_t din;

#ifndef USE_SAME_INPUT    

    if(_change_load) {
	din._wh_id = w_skewer.get_input();
    } else {
	if (specificWH>0)
	    din._wh_id = specificWH;
	else
	    din._wh_id = URand(1, sf);
    }
    
    din._carrier_id = URand(1, 10);

#else
    // same input
    din._wh_id      = 1;
    din._carrier_id = 1;
#endif        

    return (din);

}; // EOF: create_delivery



/* ------------------------- */
/* --- STOCK_LEVEL_INPUT --- */
/* ------------------------- */

stock_level_input_t& 
stock_level_input_t::operator= (const stock_level_input_t& rhs) 
{
    _wh_id     = rhs._wh_id;
    _d_id      = rhs._d_id;
    _threshold = rhs._threshold;

    _next_o_id = rhs._next_o_id;
    _o_ol_cnt = rhs._o_ol_cnt;
    _pvwi = rhs._pvwi;

    return (*this);
}


/********************************************************************* 
 *
 *  @fn:    create_stock_level_input
 *
 *  @brief: Creates the input for a new STOCK_LEVEL request, 
 *          given the scaling factor (sf) of the database
 *
 *********************************************************************/ 

stock_level_input_t create_stock_level_input(int sf, int specificWH)
{
    // check scaling factor
    assert (sf > 0);

    // produce PAYMENT params according to tpcc spec v.5.4
    stock_level_input_t slin;

#ifndef USE_SAME_INPUT    

    if(_change_load) {
	slin._wh_id = w_skewer.get_input();
    } else {
	if (specificWH>0)
	    slin._wh_id = specificWH;
	else
	    slin._wh_id = URand(1, sf);
    }
    
    slin._d_id      = URand(1, 10);
    slin._threshold = URand(10, 20);

#else
    // same input
    slin._wh_id     = 1;
    slin._d_id      = 2;
    slin._threshold = 15;
#endif        

    return (slin);

}; // EOF: create_stock_level



/* ------------------------- */
/* --- MBENCH_WH_INPUT --- */
/* ------------------------- */

mbench_wh_input_t& 
mbench_wh_input_t::operator= (const mbench_wh_input_t& rhs) 
{
    _wh_id  = rhs._wh_id;
    _amount = rhs._amount;
    return (*this);
}



/********************************************************************* 
 *
 *  @fn:    create_mbench_wh_input
 *
 *  @brief: Creates the input for a new MBENCH_WH request, 
 *          given the scaling factor (sf) of the database
 *
 *********************************************************************/ 

mbench_wh_input_t create_mbench_wh_input(int sf, int specificWH)
{
    // check scaling factor
    assert (sf > 0);

    mbench_wh_input_t mwin;

#ifndef USE_SAME_INPUT    

    if (specificWH>0)
        mwin._wh_id = specificWH;
    else
        mwin._wh_id = URand(1, sf);

    mwin._amount = (double)URand(1,1000);

#else
    // same input
    mwin._wh_id  = 1;
    mwin._amount = 150;
#endif        

    return (mwin);

}; // EOF: mbench_wh



/* ------------------------- */
/* --- MBENCH_CUST_INPUT --- */
/* ------------------------- */

mbench_cust_input_t& 
mbench_cust_input_t::operator= (const mbench_cust_input_t& rhs) 
{
    _wh_id  = rhs._wh_id;
    _d_id   = rhs._d_id;
    _c_id   = rhs._c_id;
    _amount = rhs._amount;
    return (*this);
}





/********************************************************************* 
 *
 *  @fn:    create_mbench_cust_input
 *
 *  @brief: Creates the input for a new MBENCH_CUST request, 
 *          given the scaling factor (sf) of the database
 *
 *********************************************************************/ 

mbench_cust_input_t create_mbench_cust_input(int sf, int specificWH)
{
    // check scaling factor
    assert (sf > 0);

    mbench_cust_input_t mcin;

#ifndef USE_SAME_INPUT    

    if (specificWH>0)
        mcin._wh_id = specificWH;
    else
        mcin._wh_id = URand(1, sf);

    mcin._d_id = URand(1,10);
    mcin._c_id = NURand(1023,1,3000);
    mcin._amount = (double)URand(1,1000);

#else
    // same input
    mcin._wh_id  = 1;
    mcin._d_id   = 2;
    mcin._c_id   = 3;
    mcin._amount = 15;
#endif        

    return (mcin);

}; // EOF: mbench_cust



EXIT_NAMESPACE(tpcc);
