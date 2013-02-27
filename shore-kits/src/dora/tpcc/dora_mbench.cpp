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

/** @file:   dora_mbench.cpp
 *
 *  @brief:  DORA MBENCHES
 *
 *  @note:   Implementation of RVPs and Actions that synthesize 
 *           the mbenches according to DORA
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */

#include "dora/tpcc/dora_mbench.h"
#include "dora/tpcc/dora_tpcc.h"

using namespace shore;
using namespace tpcc;


ENTER_NAMESPACE(dora);


/******************************************************************** 
 *
 * MBench TRXs
 *
 ********************************************************************/

DEFINE_DORA_FINAL_RVP_CLASS(final_mb_rvp,mbench_wh);



/******************************************************************** 
 *
 * MBench WH
 *
 * (1) UPDATE-WAREHOUSE
 *
 ********************************************************************/

void upd_wh_mb_action::calc_keys() 
{
    _down.push_back(_in._wh_id);
}


w_rc_t upd_wh_mb_action::trx_exec() 
{
    assert (_penv);

    // mbench trx touches 1 table: 
    // warehouse

    // get table tuples from the caches
    tuple_guard<warehouse_man_impl> prwh(_penv->warehouse_man());
    rep_row_t areprow(_penv->warehouse_man()->ts());
    areprow.set(_penv->warehouse_desc()->maxsize()); 
    prwh->_rep = &areprow;

    // 1. retrieve warehouse for update
    TRACE( TRACE_TRX_FLOW,
	   "App: %d PAY:wh-idx-nl (%d)\n", _tid.get_lo(), _in._wh_id);
    W_DO(_penv->warehouse_man()->wh_index_probe_nl(_penv->db(), prwh,
						   _in._wh_id));
    
    /* UPDATE warehouse SET w_ytd = wytd + :h_amount
     * WHERE w_id = :w_id
     *
     * SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip
     * FROM warehouse
     * WHERE w_id = :w_id
     *
     * plan: index probe on "W_IDX"
     */    
    TRACE( TRACE_TRX_FLOW, "App: %d PAY:wh-update-ytd-nl (%d)\n", 
	   _tid.get_lo(), _in._wh_id);
    W_DO(_penv->warehouse_man()->wh_update_ytd_nl(_penv->db(), prwh,
						  _in._amount));
    tpcc_warehouse_tuple awh;
    prwh->get_value(1, awh.W_NAME, 11);
    prwh->get_value(2, awh.W_STREET_1, 21);
    prwh->get_value(3, awh.W_STREET_2, 21);
    prwh->get_value(4, awh.W_CITY, 21);
    prwh->get_value(5, awh.W_STATE, 3);
    prwh->get_value(6, awh.W_ZIP, 10);

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prwh->print_tuple();
#endif

    return RCOK;
}


/******************************************************************** 
 *
 * MBench CUST
 *
 * (1) UPDATE-CUSTOMER
 *
 ********************************************************************/

void upd_cust_mb_action::calc_keys() 
{
    _down.push_back(_in._wh_id);
    _down.push_back(_in._d_id);
    _down.push_back(_in._c_id);
}


w_rc_t upd_cust_mb_action::trx_exec() 
{
    assert (_penv);

    // mbench trx touches 1 table: customer

    // get table tuple from the cache
    tuple_guard<customer_man_impl> prcust(_penv->customer_man());
    rep_row_t areprow(_penv->customer_man()->ts());
    areprow.set(_penv->customer_desc()->maxsize()); 
    prcust->_rep = &areprow;

    // 1. retrieve customer for update
    
    /* SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, 
     * c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, 
     * c_discount, c_balance, c_ytd_payment, c_payment_cnt 
     * FROM customer 
     * WHERE c_id = :c_id AND c_w_id = :c_w_id AND c_d_id = :c_d_id 
     * FOR UPDATE OF c_balance, c_ytd_payment, c_payment_cnt
     *
     * plan: index probe on "C_IDX"
     */
    TRACE( TRACE_TRX_FLOW,
	   "App: %d PAY:cust-idx-probe-forupdate-nl (%d) (%d) (%d)\n", 
	   _tid.get_lo(), _in._wh_id, _in._d_id, _in._c_id);
    W_DO(_penv->customer_man()->cust_index_probe_nl(_penv->db(), prcust,
						    _in._wh_id, _in._d_id,
						    _in._c_id));
    
    // retrieve customer
    tpcc_customer_tuple acust;
    prcust->get_value(3,  acust.C_FIRST, 17);
    prcust->get_value(4,  acust.C_MIDDLE, 3);
    prcust->get_value(5,  acust.C_LAST, 17);
    prcust->get_value(6,  acust.C_STREET_1, 21);
    prcust->get_value(7,  acust.C_STREET_2, 21);
    prcust->get_value(8,  acust.C_CITY, 21);
    prcust->get_value(9,  acust.C_STATE, 3);
    prcust->get_value(10, acust.C_ZIP, 10);
    prcust->get_value(11, acust.C_PHONE, 17);
    prcust->get_value(12, acust.C_SINCE);
    prcust->get_value(13, acust.C_CREDIT, 3);
    prcust->get_value(14, acust.C_CREDIT_LIM);
    prcust->get_value(15, acust.C_DISCOUNT);
    prcust->get_value(16, acust.C_BALANCE);
    prcust->get_value(17, acust.C_YTD_PAYMENT);
    prcust->get_value(18, acust.C_LAST_PAYMENT);
    prcust->get_value(19, acust.C_PAYMENT_CNT);
    prcust->get_value(20, acust.C_DATA_1, 251);
    prcust->get_value(21, acust.C_DATA_2, 251);

    // update customer fields
    acust.C_BALANCE -= _in._amount;
    acust.C_YTD_PAYMENT += _in._amount;
    acust.C_PAYMENT_CNT++;
    
    // if bad customer
    if ((acust.C_CREDIT[0] == 'B') && (acust.C_CREDIT[1] == 'C')) { 
	// 10% of customers

	/* SELECT c_data
	 * FROM customer 
	 * WHERE c_id = :c_id AND c_w_id = :c_w_id AND c_d_id = :c_d_id
	 * FOR UPDATE OF c_balance, c_ytd_payment, c_payment_cnt, c_data
	 *
	 * plan: index probe on "C_IDX"
	 */

	// update the data
	char c_new_data_1[251];
	char c_new_data_2[251];
	sprintf(c_new_data_1, "%d,%d,%d,%d,%d,%1.2f", _in._c_id, _in._d_id,
		_in._wh_id, _in._d_id, _in._wh_id, _in._amount);
	int len = strlen(c_new_data_1);
	strncat(c_new_data_1, acust.C_DATA_1, 250-len);
	strncpy(c_new_data_2, &acust.C_DATA_1[250-len], len);
	strncpy(c_new_data_2, acust.C_DATA_2, 250-len);
	
	TRACE( TRACE_TRX_FLOW, "App: %d PAY:bad-cust-update-tuple-nl\n", 
	       _tid.get_lo());
	W_DO(_penv->customer_man()->cust_update_tuple_nl(_penv->db(), prcust,
							 acust, c_new_data_1, 
							 c_new_data_2));
    } else { /* good customer */
	TRACE( TRACE_TRX_FLOW, "App: %d PAY:good-cust-update-tuple-nl\n", 
	       _tid.get_lo());
	W_DO(_penv->customer_man()->cust_update_tuple_nl(_penv->db(), prcust,
							 acust, NULL, NULL));
    }

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prcust->print_tuple();
#endif

    return RCOK;
}


EXIT_NAMESPACE(dora);
