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

/** @file:   shore_tpce_xct_trade_result.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-E TRADE RESULT transaction
 *
 *  @author: Cansu Kaynak
 *  @author: Djordje Jevdjic
 */

#include "workload/tpce/shore_tpce_env.h"
#include "workload/tpce/tpce_const.h"
#include "workload/tpce/tpce_input.h"

#include <vector>
#include <numeric>
#include <algorithm>
#include <stdio.h>
#include <stdlib.h>
#include "workload/tpce/egen/CE.h"
#include "workload/tpce/egen/TxnHarnessStructs.h"
#include "workload/tpce/shore_tpce_egen.h"

using namespace shore;
using namespace TPCE;

//#define TRACE_TRX_FLOW TRACE_ALWAYS

ENTER_NAMESPACE(tpce);

/******************************************************************** 
 *
 * TPC-E TRADE_RESULT
 *
 ********************************************************************/

w_rc_t ShoreTPCEEnv::xct_trade_result(const int xct_id,
				      trade_result_input_t& ptrin)
{
    // check whether the input is null or not
    if(ptrin._trade_price == -1) {
	atomic_inc_uint_nv(&_num_invalid_input);
	return (RCOK);
    }	
    
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    tuple_guard<trade_man_impl> prtrade(_ptrade_man);
    tuple_guard<trade_type_man_impl> prtradetype(_ptrade_type_man);
    tuple_guard<holding_summary_man_impl> prholdingsummary(_pholding_summary_man);
    tuple_guard<customer_account_man_impl> prcustaccount(_pcustomer_account_man);
    tuple_guard<holding_man_impl> prholding(_pholding_man);
    tuple_guard<holding_history_man_impl> prholdinghistory(_pholding_history_man);
    tuple_guard<security_man_impl> prsecurity(_psecurity_man);
    tuple_guard<broker_man_impl> prbroker(_pbroker_man);
    tuple_guard<settlement_man_impl> prsettlement(_psettlement_man);
    tuple_guard<cash_transaction_man_impl> prcashtrans(_pcash_transaction_man);
    tuple_guard<commission_rate_man_impl> prcommissionrate(_pcommission_rate_man);
    tuple_guard<customer_taxrate_man_impl> prcusttaxrate(_pcustomer_taxrate_man);
    tuple_guard<taxrate_man_impl> prtaxrate(_ptaxrate_man);
    tuple_guard<customer_man_impl> prcustomer(_pcustomer_man);
    tuple_guard<trade_history_man_impl> prtradehist(_ptrade_history_man);

    rep_row_t areprow(_pcustomer_man->ts());

    areprow.set(_pcustomer_desc->maxsize());

    prtrade->_rep = &areprow;
    prtradetype->_rep = &areprow;
    prholdingsummary->_rep = &areprow;
    prcustaccount->_rep = &areprow;
    prholding->_rep = &areprow;
    prholdinghistory->_rep = &areprow;
    prsecurity->_rep = &areprow;
    prbroker->_rep = &areprow;
    prsettlement->_rep = &areprow;
    prcashtrans->_rep = &areprow;
    prcommissionrate->_rep = &areprow;
    prcusttaxrate->_rep = &areprow;
    prtaxrate->_rep = &areprow;
    prcustomer->_rep = &areprow;
    prtradehist->_rep = &areprow;

    rep_row_t lowrep(_pcustomer_man->ts());
    rep_row_t highrep(_pcustomer_man->ts());

    lowrep.set(_pcustomer_desc->maxsize());
    highrep.set(_pcustomer_desc->maxsize());

    int trade_qty;
    TIdent acct_id = -1;
    bool type_is_sell;
    int hs_qty = -1;
    char symbol[16]; //15
    bool is_lifo;
    char type_id[4]; //3
    bool trade_is_cash;
    char type_name[13];	//12
    double charge;
    
    //BEGIN FRAME1

    /**
     * 	SELECT 	acct_id = T_CA_ID, type_id = T_TT_ID,
     *          symbol = T_S_SYMB, trade_qty = T_QTY,
     *		charge = T_CHRG, is_lifo = T_LIFO, trade_is_cash = T_IS_CASH
     *	FROM	TRADE
     *	WHERE	T_ID = trade_id
     */
    TRACE( TRACE_TRX_FLOW, "App: %d TR:t-idx-probe (%ld) \n",
	   xct_id, ptrin._trade_id);
    w_rc_t e = _ptrade_man->t_index_probe(_pssm, prtrade, ptrin._trade_id);
    if(e.is_error()) {	
	assert(e.err_num() != se_TUPLE_NOT_FOUND); //Harness control
	W_DO(e);
    }
    prtrade->get_value(3, type_id, 4); 	//3
    prtrade->get_value(4, trade_is_cash);
    prtrade->get_value(5, symbol, 16);	//15
    prtrade->get_value(6, trade_qty);
    prtrade->get_value(8, acct_id);
    prtrade->get_value(11, charge);
    prtrade->get_value(14, is_lifo);
    
    /**
     * 	SELECT 	type_name = TT_NAME, type_is_sell = TT_IS_SELL,
     *          type_is_market = TT_IS_MRKT
     *	FROM	TRADE_TYPE
     *   	WHERE	TT_ID = type_id
     */
    TRACE( TRACE_TRX_FLOW, "App: %d TR:tt-idx-probe (%s) \n", xct_id, type_id);
    W_DO(_ptrade_type_man->tt_index_probe(_pssm, prtradetype, type_id));

    prtradetype->get_value(1, type_name, 13); //12
    prtradetype->get_value(2, type_is_sell);
    bool type_is_market;
    prtradetype->get_value(3, type_is_market);

    /**
     * 	SELECT 	hs_qty = HS_QTY
     *	FROM	HOLDING_SUMMARY
     *  	WHERE	HS_CA_ID = acct_id and HS_S_SYMB = symbol
     */
    TRACE( TRACE_TRX_FLOW, "App: %d TR:hs-idx-probe (%ld) (%s) \n",
	   xct_id, acct_id, symbol);
    e = _pholding_summary_man->hs_index_probe(_pssm, prholdingsummary,
					      acct_id, symbol);
    if (e.is_error()) {
	hs_qty=0;
    } else  {
	prholdingsummary->get_value(2, hs_qty);
    }
    if(hs_qty == -1){ //-1 = NULL, no prior holdings exist
	hs_qty = 0;
    }
    //END FRAME1

    TIdent cust_id;
    double buy_value = 0;
    double sell_value = 0;
    myTime trade_dts = time(NULL);
    TIdent broker_id;
    short tax_status;
    
    //BEGIN FRAME2
    TIdent hold_id;
    double hold_price;
    int hold_qty;
    int needed_qty = trade_qty;
    uint num_deleted = 0;
    
    /**
     *
     * 	SELECT 	broker_id = CA_B_ID, cust_id = CA_C_ID, tax_status = CA_TAX_ST
     *	FROM	CUSTOMER_ACCOUNT
     *	WHERE	CA_ID = acct_id
     */
    TRACE( TRACE_TRX_FLOW, "App: %d TR:ca-idx-probe (%ld) \n", xct_id, acct_id);
    W_DO(_pcustomer_account_man->ca_index_probe(_pssm, prcustaccount, acct_id));
    
    prcustaccount->get_value(1, broker_id);
    prcustaccount->get_value(2, cust_id);
    prcustaccount->get_value(4, tax_status);
    
    if(type_is_sell){
	if(hs_qty == 0){
	    /**
	     *	INSERT INTO HOLDING_SUMMARY (HS_CA_ID, HS_S_SYMB, HS_QTY)
	     *	VALUES 			    (acct_id, symbol, -trade_qty)
	     */
	    prholdingsummary->set_value(0, acct_id);
	    prholdingsummary->set_value(1, symbol);
	    prholdingsummary->set_value(2, -1*trade_qty);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TR:hs-add-tuple (%ld) \n",
		   xct_id, acct_id);
	    W_DO(_pholding_summary_man->add_tuple(_pssm, prholdingsummary));
	} else if(hs_qty != trade_qty) {
	    /**
	     *	UPDATE	HOLDING_SUMMARY
	     *	SET	HS_QTY = hs_qty - trade_qty
	     *	WHERE	HS_CA_ID = acct_id and HS_S_SYMB = symbol
	     */
	    TRACE( TRACE_TRX_FLOW, "App: %d TR:hs-update (%ld) (%s) (%d) \n",
		   xct_id, acct_id, symbol, (hs_qty - trade_qty));
	    W_DO(_pholding_summary_man->hs_update_qty(_pssm, prholdingsummary,
						      acct_id, symbol,
						      (hs_qty - trade_qty)));
	}
	
	//// TPCE-SPEC-1.12.0
	// Sell Trade:
	// First look for existing holdings, H_QTY > 0		
	if (hs_qty > 0) {
	    guard<index_scan_iter_impl<holding_t> > h_iter;
	    asc_sort_scan_t* h_list_sort_iter;
	    
	    if (is_lifo) {
		/**
		 *
		 * 	SELECT	H_T_ID, H_QTY, H_PRICE
		 *  	FROM	HOLDING
		 *	WHERE	H_CA_ID = acct_id and H_S_SYMB = symbol
		 *	ORDER BY H_DTS DESC
		 */
		{
		    index_scan_iter_impl<holding_t>* tmp_h_iter;
		    TRACE( TRACE_TRX_FLOW,
			   "App: %d TR:h-get-iter-by-idx2-backwards (%ld) (%s)\n",
			   xct_id, acct_id, symbol);
		    W_DO(_pholding_man->h_get_iter_by_index2(_pssm, tmp_h_iter,
							     prholding, lowrep,
							     highrep, acct_id,
							     symbol, true));
		    h_iter = tmp_h_iter;
		}
	    } else {
		/**
		 * 	SELECT	H_T_ID, H_QTY, H_PRICE
		 * 	FROM	HOLDING
		 *	WHERE	H_CA_ID = acct_id and H_S_SYMB = symbol
		 *	ORDER BY H_DTS ASC
		 */
		{
		    index_scan_iter_impl<holding_t>* tmp_h_iter;
		    TRACE( TRACE_TRX_FLOW,
			   "App: %d TR:h-get-iter-by-idx2 (%ld) (%s)\n",
			   xct_id, acct_id, symbol);
		    W_DO(_pholding_man->h_get_iter_by_index2(_pssm, tmp_h_iter,
							     prholding, lowrep,
							     highrep, acct_id,
							     symbol));
		    h_iter = tmp_h_iter;
		}
	    }
	    
	    //// TPCE-SPEC-1.12.0
	    // Liquidate existing holdings. Note that more than
	    // 1 HOLDING record can be deleted here since customer
	    // may have the same security with differing prices.

	    bool eof;
	    W_DO(h_iter->next(_pssm, eof, *prholding));
	    while (needed_qty !=0 && !eof) {
		TIdent hold_id;
		int hold_qty;
		double hold_price;
		
		prholding->get_value(0, hold_id);
		prholding->get_value(4, hold_price);
		prholding->get_value(5, hold_qty);
		
		/* PIN: I made some changes regarding the query plan here:
		 * (1) Since "if(hold_qty > needed_qty)" branch cause lots of code
		 *     repetition, instead this condition is put only on the places
		 *     where it actually makes a difference.
		 *     There are only three such places;
		 *     (a) HH_AFTER_QTY column value for the tuple to be inserted
		 *         into the HOLDING_HISTORY table
		 *     (b) Whether to update the current HOLDING tuple or delete it
		 *     (c) How to set the new values of buy_value, sell_value, and
		 *         needed_qty
		 *     Maybe this won't make that big of a difference in terms of
		 *     performance but the code is much cleaner now.
		 * (2) The operation on the HOLDING table is moved before the
		 *     operation on the HOLDING_HISTORY table since in shore-kits
		 *     we keep the same scratch space for all of the tuple values
		 *     and it makes sense to operate on the HOLDING tuple value
		 *     while we still have it in our scratch space. Otherwise, we
		 *     have to perform an index probe on the primary index of the
		 *     HOLDING table, which increases the execution time.
		 * (3) Since deleting a tuple from a table while performing an
		 *     index scan on the same table efficiently really messes
		 *     things up in Shore-MT (crashes happen due to race conditions
		 *     on updating page metadata), the rids of the HOLDING tuples
		 *     that should be deleted are kept in a small array and the
		 *     deletions are performed after the scan.
		 * A similar query plan change is done below for the BUY Trade
		 * case (see PIN* below)
		 */
		
		/*
		 * if(hold_qty > needed_qty) {
		 * 	INSERT INTO
		 *		HOLDING_HISTORY (
		 *		HH_H_T_ID,
		 *		HH_T_ID,
		 *		HH_BEFORE_QTY,
		 *		HH_AFTER_QTY
		 *	)
		 *	VALUES(
		 *		hold_id, // H_T_ID of original trade
		 *		trade_id, // T_ID current trade
		 *		hold_qty, // H_QTY now
		 *		hold_qty - needed_qty // H_QTY after update
		 *	)
		 *
		 * 	UPDATE 	HOLDING
		 *	SET	H_QTY = hold_qty - needed_qty
		 *	WHERE	current of hold_list
		 *
		 *      buy_value += needed_qty * hold_price
		 *      sell_value += needed_qty * trade_price
		 *      needed_qty = 0
		 * } else {
		 * 	INSERT INTO
		 *		HOLDING_HISTORY (
		 *		HH_H_T_ID,
		 *		HH_T_ID,
		 *		HH_BEFORE_QTY,
		 *		HH_AFTER_QTY
		 *	)
		 *	VALUES(
		 *		hold_id, // H_T_ID of original trade
		 *		trade_id, // T_ID current trade
		 *		hold_qty, // H_QTY now
		 *		0 // H_QTY after update
		 *	)
		 *
		 *      DELETE 	FROM	HOLDING
		 *	WHERE	current of hold_list
		 *
		 *      buy_value += hold_qty * hold_price
		 *      sell_value += hold_qty * trade_price
		 *      needed_qty = needed_qty - hold_qty
		 * }
		 */

		if(hold_qty > needed_qty){
		    TRACE( TRACE_TRX_FLOW,
			   "App: %d TR:hold-update (%ld) (%s) (%d) \n",
			   xct_id, acct_id, symbol, (hs_qty - trade_qty));
		    W_DO(_pholding_man->h_update_qty(_pssm, prholding,
						     (hold_qty - needed_qty)));
		} else {
		    ptrin._holding_rid[num_deleted] = prholding->rid();
		    num_deleted++;
		}
		
		prholdinghistory->set_value(0, hold_id);
		prholdinghistory->set_value(1, ptrin._trade_id);
		prholdinghistory->set_value(2, hold_qty);
		prholdinghistory->set_value(3,
					    (hold_qty > needed_qty) ? (hold_qty - needed_qty) : 0); 
		
		TRACE( TRACE_TRX_FLOW,
		       "App: %d TR:hh-add-tuple (%ld) (%ld) (%d) (%d)\n",
		       xct_id, hold_id, ptrin._trade_id, hold_price,
		       ((hold_qty > needed_qty) ? (hold_qty - needed_qty) : 0));
		W_DO(_pholding_history_man->add_tuple(_pssm, prholdinghistory));

		if(hold_qty > needed_qty) {
		    buy_value += needed_qty * hold_price;
		    sell_value += needed_qty * ptrin._trade_price;
		    needed_qty = 0;
		} else {
		    buy_value += hold_qty * hold_price;
		    sell_value += hold_qty * ptrin._trade_price;
		    needed_qty = needed_qty - hold_qty;
		}
		
		TRACE( TRACE_TRX_FLOW, "App: %d TR:hold-list (%ld)\n",
		       xct_id, hold_id);
		W_DO(h_iter->next(_pssm, eof, *prholding));
	    }
	    
	    for(uint i=0; i<num_deleted; i++) {
		TRACE(TRACE_TRX_FLOW,
		      "%d - TR:h-delete-tuple - (%d).(%d).(%d).(%d)\n", xct_id,
		      (ptrin._holding_rid[i]).pid.vol().vol,
		      (ptrin._holding_rid[i]).pid.store(),
		      (ptrin._holding_rid[i]).pid.page,
		      (ptrin._holding_rid[i]).slot);
		W_DO(_pholding_man->h_delete_tuple(_pssm, prholding,
						   ptrin._holding_rid[i]));
	    }
	}
	
	//// TPCE-SPEC-1.12.0
	// Sell Short:
	// If needed_qty > 0 then customer has sold all existing
	// holdings and customer is selling short. A new HOLDING
	// record will be created with H_QTY set to the negative
	// number of needed shares.
	if(needed_qty > 0) {
	    /**
	     * 	INSERT INTO
	     *		HOLDING_HISTORY (
	     *		HH_H_T_ID,
	     *		HH_T_ID,
	     *		HH_BEFORE_QTY,
	     *		HH_AFTER_QTY
	     *	)
	     *	VALUES(
	     *		trade_id, // T_ID current is original trade
	     *		trade_id, // T_ID current trade
	     *		0, // H_QTY before
	     *		(-1) * needed_qty // H_QTY after insert
	     *	)
	     */	    
	    prholdinghistory->set_value(0, ptrin._trade_id);
	    prholdinghistory->set_value(1, ptrin._trade_id);
	    prholdinghistory->set_value(2, 0);
	    prholdinghistory->set_value(3, (-1) * needed_qty);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TR:hh-add-tuple (%ld)(%ld)(%d)(%d)\n",
		   xct_id, ptrin._trade_id, ptrin._trade_id, 0, (-1) * needed_qty);
	    W_DO(_pholding_history_man->add_tuple(_pssm, prholdinghistory));
		    
	    /**
	     * 	INSERT INTO
	     *		HOLDING (
	     *				H_T_ID,
	     *				H_CA_ID,
	     *				H_S_SYMB,
	     *				H_DTS,
	     *				H_PRICE,
	     *				H_QTY
	     *			)
	     *	VALUES 		(
	     *				trade_id, // H_T_ID
	     *				acct_id, // H_CA_ID
	     *				symbol, // H_S_SYMB
	     *				trade_dts, // H_DTS
	     *				trade_price, // H_PRICE
	     *				(-1) * needed_qty //* H_QTY
	     *			)
	     */
	    prholding->set_value(0, ptrin._trade_id);
	    prholding->set_value(1, acct_id);
	    prholding->set_value(2, symbol);
	    prholding->set_value(3, trade_dts);
	    prholding->set_value(4, ptrin._trade_price);
	    prholding->set_value(5, (-1) * needed_qty);
	    
	    TRACE( TRACE_TRX_FLOW,
		   "App: %d TR:h-add-tuple (%ld) (%ld) (%s) (%ld) (%d) (%d) \n",
		   xct_id, ptrin._trade_id, acct_id, symbol, trade_dts,
		   ptrin._trade_price, (-1) * needed_qty);
	    W_DO(_pholding_man->add_tuple(_pssm, prholding));
	} else if (hs_qty == trade_qty) {
	    /**
	     * DELETE FROM	HOLDING_SUMMARY
	     * WHERE HS_CA_ID = acct_id and HS_S_SYMB = symbol
	     */
	    TRACE( TRACE_TRX_FLOW, "App: %d TR:hs-delete-tuple (%ld) (%s) \n",
		   xct_id, acct_id, symbol);
	    W_DO(_pholding_summary_man->delete_tuple(_pssm, prholdingsummary));
	}
    } else { //// TPCE-SPEC-1.12.0 - The trade is a BUY
	if (hs_qty == 0) {
	    /**
	     *	INSERT INTO HOLDING_SUMMARY (
	     *					HS_CA_ID,
	     *					HS_S_SYMB,
	     *					HS_QTY
	     *				   )
	     *	VALUES 			(
	     *					acct_id,
	     *					symbol,
	     *					trade_qty
	     *				)
	     **/
	    prholdingsummary->set_value(0, acct_id);
	    prholdingsummary->set_value(1, symbol);
	    prholdingsummary->set_value(2, trade_qty);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TR:hs-add-tuple (%ld) \n",
		   xct_id, acct_id);
	    W_DO(_pholding_summary_man->add_tuple(_pssm, prholdingsummary));
	} else if (-hs_qty != trade_qty) {
	    /**
	     *	UPDATE	HOLDING_SUMMARY
	     *	SET	HS_QTY = hs_qty + trade_qty
	     *	WHERE	HS_CA_ID = acct_id and HS_S_SYMB = symbol
	     */
	    TRACE( TRACE_TRX_FLOW, "App: %d TR:holdsumm-update (%ld) (%s) (%d) \n",
		   xct_id, acct_id, symbol, (hs_qty + trade_qty));
	    W_DO(_pholding_summary_man->hs_update_qty(_pssm, prholdingsummary,
						      acct_id, symbol,
						      (hs_qty + trade_qty)));
	}
	
	//// TPCE-SPEC-1.12.0
	// Short Cover:
	// First look for existing negative holdings, H_QTY < 0,
	// which indicates a previous short sell. The buy trade
	// will cover the short sell.
	if(hs_qty < 0) {
	    guard<index_scan_iter_impl<holding_t> > h_iter;
	    asc_sort_scan_t* h_list_sort_iter;
	    
	    if(is_lifo) {
		/**
		 * 	SELECT	H_T_ID, H_QTY, H_PRICE
		 * 	FROM	HOLDING
		 *	WHERE	H_CA_ID = acct_id and H_S_SYMB = symbol
		 *	ORDER BY H_DTS DESC
		 */
		{
		    index_scan_iter_impl<holding_t>* tmp_h_iter;
		    TRACE( TRACE_TRX_FLOW,
			   "App: %d TR:h-get-iter-by-idx2-backward (%ld) (%s)\n",
			   xct_id, acct_id, symbol);
		    W_DO(_pholding_man->h_get_iter_by_index2(_pssm, tmp_h_iter,
							     prholding, lowrep,
							     highrep, acct_id,
							     symbol, true));
		    h_iter = tmp_h_iter;
		}
	    } else {
		/**
		 * 	SELECT	H_T_ID, H_QTY, H_PRICE
		 *      FROM	HOLDING
		 *	WHERE	H_CA_ID = acct_id and H_S_SYMB = symbol
		 *	ORDER  BY H_DTS ASC
		 */
		{
		    index_scan_iter_impl<holding_t>* tmp_h_iter;
		    TRACE( TRACE_TRX_FLOW,
			   "App: %d TR:h-get-iter-by-idx2 (%ld) (%s)\n",
			   xct_id, acct_id, symbol);
		    W_DO(_pholding_man->h_get_iter_by_index2(_pssm, tmp_h_iter,
							     prholding, lowrep,
							     highrep, acct_id,
							     symbol));
		    h_iter = tmp_h_iter;
		}
	    }
	    
	    bool eof;
	    W_DO(h_iter->next(_pssm, eof, *prholding));
	    while (needed_qty!=0 && !eof) {
		TIdent hold_id;
		int hold_qty;
		double hold_price;
		
		prholding->get_value(0, hold_id);
		prholding->get_value(4, hold_price);
		prholding->get_value(5, hold_qty);
		
		/*
		 * if(hold_qty + needed_qty < 0) {
		 * 	INSERT INTO
		 *		HOLDING_HISTORY (
		 *		HH_H_T_ID,
		 *		HH_T_ID,
		 *		HH_BEFORE_QTY,
		 *		HH_AFTER_QTY
		 *	)
		 *	VALUES(
		 *		hold_id, // H_T_ID of original trade
		 *		trade_id, // T_ID current trade
		 *		hold_qty, // H_QTY now
		 *		hold_qty + needed_qty // H_QTY after update
		 *	)
		 *
		 * 	UPDATE 	HOLDING
		 *	SET	H_QTY = hold_qty + needed_qty
		 *	WHERE	current of hold_list
		 *
		 *      sell_value += needed_qty * hold_price;
		 *      buy_value += needed_qty * ptrin._trade_price;
		 *      needed_qty = 0;
		 * } else {
		 * 	INSERT INTO
		 *		HOLDING_HISTORY (
		 *		HH_H_T_ID,
		 *		HH_T_ID,
		 *		HH_BEFORE_QTY,
		 *		HH_AFTER_QTY
		 *	)
		 *	VALUES(
		 *		hold_id, // H_T_ID of original trade
		 *		trade_id, // T_ID current trade
		 *		hold_qty, // H_QTY now
		 *		0 // H_QTY after update
		 *	)
		 *
		 *      DELETE FROM	HOLDING
		 *      WHERE current of hold_list
		 *
		 *      hold_qty = -hold_qty;
		 *      sell_value += hold_qty * hold_price;
		 *      buy_value += hold_qty * ptrin._trade_price;
		 *      needed_qty = needed_qty - hold_qty;
		 * }
		 */
		
		if(hold_qty + needed_qty < 0) {
		    TRACE( TRACE_TRX_FLOW, "App: %d TR:h-update (%ld) (%s) (%d)\n",
			   xct_id, acct_id, symbol, (hs_qty + trade_qty));
		    W_DO(_pholding_man->h_update_qty(_pssm, prholding,
						     (hold_qty + needed_qty)));
		} else {
		    ptrin._holding_rid[num_deleted] = prholding->rid();
		    num_deleted++;
		}
		
		prholdinghistory->set_value(0, hold_id);
		prholdinghistory->set_value(1, ptrin._trade_id);
		prholdinghistory->set_value(2, hold_qty);
		prholdinghistory->set_value(3,
					    (hold_qty + needed_qty < 0) ? (hold_qty + needed_qty) : 0);
		
		TRACE( TRACE_TRX_FLOW,
		       "App: %d TR:hh-add-tuple (%ld) (%ld) (%d) (%d)\n",
		       xct_id, hold_id, ptrin._trade_id, hold_price,
		       (hold_qty + needed_qty));
		W_DO(_pholding_history_man->add_tuple(_pssm, prholdinghistory));
			
		if(hold_qty + needed_qty < 0) {
		    sell_value += needed_qty * hold_price;
		    buy_value += needed_qty * ptrin._trade_price;
		    needed_qty = 0;
		} else {
		    hold_qty = -hold_qty;
		    sell_value += hold_qty * hold_price;
		    buy_value += hold_qty * ptrin._trade_price;
		    needed_qty = needed_qty - hold_qty;
		}
		
		TRACE( TRACE_TRX_FLOW, "App: %d TR:hold-list (%ld) \n",
		       xct_id, hold_id);
		W_DO(h_iter->next(_pssm, eof, *prholding));
	    }
	    
	    for(uint i=0; i<num_deleted; i++) {
		TRACE(TRACE_TRX_FLOW,
		      "%d - TR:h-delete-tuple - (%d).(%d).(%d).(%d)\n", xct_id,
		      (ptrin._holding_rid[i]).pid.vol().vol,
		      (ptrin._holding_rid[i]).pid.store(),
		      (ptrin._holding_rid[i]).pid.page,
		      (ptrin._holding_rid[i]).slot);
		W_DO(_pholding_man->h_delete_tuple(_pssm, prholding,
						   ptrin._holding_rid[i]));
	    }
	}
	
	//// TPCE-SPEC-1.12.0
	// Buy Trade:
	// If needed_qty > 0, then the customer has covered all
	// previous Short Sells and the customer is buying new
	// holdings. A new HOLDING record will be created with
	// H_QTY set to the number of needed shares.
	if (needed_qty > 0){
	    /**
	     * 	INSERT INTO
	     *		HOLDING_HISTORY (
	     *		HH_H_T_ID,
	     *		HH_T_ID,
	     *		HH_BEFORE_QTY,
	     *		HH_AFTER_QTY
	     *	)
	     *	VALUES(
	     *		trade_id, // T_ID current is original trade
	     *		trade_id, // T_ID current trade
	     *		0, // H_QTY before
	     *		needed_qty // H_QTY after insert
	     *	)
	     */
	    prholdinghistory->set_value(0, ptrin._trade_id);
	    prholdinghistory->set_value(1, ptrin._trade_id);
	    prholdinghistory->set_value(2, 0);
	    prholdinghistory->set_value(3, needed_qty);
	    
	    TRACE( TRACE_TRX_FLOW,
		   "App: %d TR:hh-add-tuple (%ld) (%ld) (%d) (%d)\n",
		   xct_id, ptrin._trade_id, ptrin._trade_id, 0, needed_qty);
	    W_DO(_pholding_history_man->add_tuple(_pssm, prholdinghistory));
			
	    /**
	     *
	     * 	INSERT INTO
	     *		HOLDING (
	     *				H_T_ID,
	     *				H_CA_ID,
	     *				H_S_SYMB,
	     *				H_DTS,
	     *				H_PRICE,
	     *				H_QTY
	     *			)
	     *	VALUES 		(
	     *				trade_id, // H_T_ID
	     *				acct_id, // H_CA_ID
	     *				symbol, // H_S_SYMB
	     *				trade_dts, // H_DTS
	     *				trade_price, // H_PRICE
	     *				needed_qty // H_QTY
	     *			)
	     */
	    prholding->set_value(0, ptrin._trade_id);
	    prholding->set_value(1, acct_id);
	    prholding->set_value(2, symbol);
	    prholding->set_value(3, trade_dts);
	    prholding->set_value(4, ptrin._trade_price);
	    prholding->set_value(5, needed_qty);
	    
	    TRACE( TRACE_TRX_FLOW,
		   "App: %d TR:h-add-tuple (%ld) (%ld) (%s) (%d) (%d) (%d) \n",
		   xct_id, ptrin._trade_id, acct_id, symbol, trade_dts,
		   ptrin._trade_price, needed_qty);
	    W_DO(_pholding_man->add_tuple(_pssm, prholding));
	} else if ((-hs_qty) == trade_qty) {
	    /**
	     * DELETE FROM	HOLDING_SUMMARY
	     * WHERE HS_CA_ID = acct_id and HS_S_SYMB = symbol
	     */
	    TRACE( TRACE_TRX_FLOW, "App: %d TR:hs-delete-tuple (%ld) (%s) \n",
		   xct_id, acct_id, symbol);
	    W_DO(_pholding_summary_man->delete_tuple(_pssm, prholdingsummary));
	}
    }
    //END FRAME2
    
    //BEGIN FRAME3
    double tax_amount = 0;
    if((tax_status == 1 || tax_status == 2) && (sell_value > buy_value)) {
	double tax_rates = 0;
	/**
	 *	SELECT 	tax_rates = sum(TX_RATE)
	 *	FROM	TAXRATE
	 *	WHERE  	TX_ID in ( 	SELECT	CX_TX_ID
	 *				FROM	CUSTOMER_TAXRATE
	 *				WHERE	CX_C_ID = cust_id)
	 */
	guard< index_scan_iter_impl<customer_taxrate_t> > cx_iter;
	{
	    index_scan_iter_impl<customer_taxrate_t>* tmp_cx_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d TR:ct-iter-by-idx (%ld) \n",
		   xct_id, cust_id);
	    W_DO(_pcustomer_taxrate_man->cx_get_iter_by_index(_pssm, tmp_cx_iter,
							      prcusttaxrate,
							      lowrep, highrep,
							      cust_id));
	    cx_iter = tmp_cx_iter;
	}
	bool eof;
	W_DO(cx_iter->next(_pssm, eof, *prcusttaxrate));
	while (!eof) {
	    char tax_id[5]; //4
	    prcusttaxrate->get_value(0, tax_id, 5); //4
	    
	    TRACE(TRACE_TRX_FLOW,"App: %d TR:tx-idx-probe (%s)\n", xct_id, tax_id);
	    W_DO(_ptaxrate_man->tx_index_probe(_pssm, prtaxrate, tax_id));
		
	    double rate;
	    prtaxrate->get_value(2, rate);
	    
	    tax_rates += rate;
	    
	    W_DO(cx_iter->next(_pssm, eof, *prcusttaxrate));
	}
	tax_amount = (sell_value - buy_value) * tax_rates;
	
	/**
	 *	UPDATE	TRADE
	 *	SET	T_TAX = tax_amount
	 *	WHERE	T_ID = trade_id
	 */
	TRACE( TRACE_TRX_FLOW, "App: %d TR:t-upd-tax-by-ind (%ld) (%d)\n",
	       xct_id, ptrin._trade_id, tax_amount);
	W_DO(_ptrade_man->t_update_tax_by_index(_pssm, prtrade, ptrin._trade_id,
						tax_amount));
	assert(tax_amount > 0); //Harness control
    } //END FRAME3

    double comm_rate = 0;
    char s_name[51]; //50

    //BEGIN FRAME4

    /**
     *	SELECT	s_ex_id = S_EX_ID, s_name = S_NAME
     *	FROM	SECURITY
     *	WHERE	S_SYMB = symbol
     */
    TRACE( TRACE_TRX_FLOW, "App: %d TR:security-idx-probe (%s)\n", xct_id, symbol);
    W_DO(_psecurity_man->s_index_probe(_pssm, prsecurity, symbol));
    
    char s_ex_id[7]; //6
    prsecurity->get_value(3, s_name, 51); //50
    prsecurity->get_value(4, s_ex_id, 7); //7

    /**
     *	SELECT	c_tier = C_TIER
     *	FROM	CUSTOMER
     *	WHERE	C_ID = cust_id
     */
    guard< index_scan_iter_impl<customer_t> > c_iter;
    {
	index_scan_iter_impl<customer_t>* tmp_c_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d TR:c-get-iter-by-idx3 (%ld) \n",
	       xct_id, cust_id);
	W_DO(_pcustomer_man->c_get_iter_by_index3(_pssm, tmp_c_iter, prcustomer,
						  lowrep, highrep, cust_id));
	c_iter = tmp_c_iter;
    }
    bool eof;
    TRACE( TRACE_TRX_FLOW, "App: %d TR:c-iter-next \n", xct_id);
    W_DO(c_iter->next(_pssm, eof, *prcustomer));

    short c_tier;
    prcustomer->get_value(7, c_tier);
    
    /**
     *	SELECT	1 row comm_rate = CR_RATE
     *	FROM	COMMISSION_RATE
     *	WHERE	CR_C_TIER = c_tier and CR_TT_ID = type_id and CR_EX_ID = s_ex_id
     *	        and CR_FROM_QTY <= trade_qty and CR_TO_QTY >= trade_qty
     */
    guard< index_scan_iter_impl<commission_rate_t> > cr_iter;
    {
	index_scan_iter_impl<commission_rate_t>* tmp_cr_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d TR:cr-iter-by-idx (%d) (%s) (%ld) (%d) \n",
	       xct_id, c_tier, type_id, s_ex_id, trade_qty);
	W_DO(_pcommission_rate_man->cr_get_iter_by_index(_pssm, tmp_cr_iter,
							 prcommissionrate, lowrep,
							 highrep, c_tier, type_id,
							 s_ex_id, trade_qty));
	cr_iter = tmp_cr_iter;
    }
    TRACE( TRACE_TRX_FLOW, "App: %d TR:cr-iter-next \n", xct_id);
    W_DO(cr_iter->next(_pssm, eof, *prcommissionrate));
    while (!eof) {			  
	int to_qty;
	prcommissionrate->get_value(4, to_qty);
	if(to_qty >= trade_qty){				
	    prcommissionrate->get_value(5, comm_rate);
	    break;
	}
	TRACE( TRACE_TRX_FLOW, "App: %d TR:cr-iter-next \n", xct_id);
	W_DO(cr_iter->next(_pssm, eof, *prcommissionrate));
    }
    //END FRAME4
    assert(comm_rate > 0.00);  //Harness control

    double comm_amount = (comm_rate / 100) * (trade_qty * ptrin._trade_price);
    char st_completed_id[5] = "CMPT"; //4

    //BEGIN FRAME5

    /**
     *	UPDATE	TRADE
     * 	SET	T_COMM = comm_amount, T_DTS = trade_dts, T_ST_ID = st_completed_id,
     *		T_TRADE_PRICE = trade_price
     *	WHERE	T_ID = trade_id
     */
    TRACE( TRACE_TRX_FLOW,
	   "App: %d TR:t-upd-ca_td_sci_tp-by-ind (%ld) (%lf) (%ld) (%s) (%lf) \n",
	   xct_id, ptrin._trade_id, comm_amount, trade_dts, st_completed_id,
	   ptrin._trade_price);
    W_DO(_ptrade_man->t_update_ca_td_sci_tp_by_index(_pssm, prtrade,
						     ptrin._trade_id, comm_amount,
						     trade_dts, st_completed_id,
						     ptrin._trade_price));
    
    /**
     *	INSERT INTO TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID)
     *	VALUES 			  (trade_id, // TH_T_ID
     *				   now_dts, // TH_DTS
     *			           st_completed_id // TH_ST_ID
     *				  )
     */
    myTime now_dts = time(NULL);
    
    prtradehist->set_value(0, ptrin._trade_id);
    prtradehist->set_value(1, now_dts);
    prtradehist->set_value(2, st_completed_id);
    
    TRACE( TRACE_TRX_FLOW, "App: %d TR:th-add-tuple (%ld) \n",
	   xct_id, ptrin._trade_id);
    W_DO(_ptrade_history_man->add_tuple(_pssm, prtradehist));

    /**
     *	UPDATE BROKER
     *	SET B_COMM_TOTAL = B_COMM_TOTAL+comm_amount, B_NUM_TRADES=B_NUM_TRADES+1
     *	WHERE	B_ID = broker_id
     */
    TRACE( TRACE_TRX_FLOW, "App: %d TR:broker-upd-ca_td_sci_tp-by-ind (%ld)(%d)\n",
	   xct_id, broker_id, comm_amount);
    W_DO(_pbroker_man->broker_update_ca_nt_by_index(_pssm, prbroker, broker_id,
						    comm_amount));
    //END FRAME5
    
    myTime due_date = trade_dts /*time(NULL)*/ + 48*60*60; //add 2 days
    double se_amount;
    if(type_is_sell) {
	se_amount = (trade_qty * ptrin._trade_price) - charge - comm_amount;
    } else {
	se_amount = -((trade_qty * ptrin._trade_price) + charge + comm_amount);
    }
    if (tax_status == 1){
	se_amount = se_amount - tax_amount;
    }
    
    //BEGIN FRAME6
    char cash_type[41] = "\0"; //40
    if(trade_is_cash) {
	strcpy (cash_type, "Cash Account");
    } else {
	strcpy(cash_type,"Margin");
    }
    
    /**
     *	INSERT INTO SETTLEMENT (
     *					SE_T_ID,
     *					SE_CASH_TYPE,
     *					SE_CASH_DUE_DATE,
     *					SE_AMT
     *				)
     *	VALUES 			(
     * 					trade_id,
     *					cash_type,
     *					due_date,
     *					se_amount
     *				)
     */
    prsettlement->set_value(0, ptrin._trade_id);
    prsettlement->set_value(1, cash_type);
    prsettlement->set_value(2, due_date);
    prsettlement->set_value(3, se_amount);
    
    TRACE( TRACE_TRX_FLOW, "App: %d TR:se-add-tuple (%ld)\n",
	   xct_id, ptrin._trade_id);
    W_DO(_psettlement_man->add_tuple(_pssm, prsettlement));

    if(trade_is_cash) {
	/**
	 *	update 	CUSTOMER_ACCOUNT
	 *	set	CA_BAL = CA_BAL + se_amount
	 *	where	CA_ID = acct_id
	 */	
	TRACE( TRACE_TRX_FLOW, "App: %d TR:ca-upd-tuple (%ld)\n", xct_id, acct_id);
	W_DO(_pcustomer_account_man->ca_update_bal(_pssm, prcustaccount,
						   acct_id, se_amount));
	// PIN: look at the below PIN
	double acct_bal;
	prcustaccount->get_value(5, acct_bal);
	
	/**
	 * insert into CASH_TRANSACTION (CT_DTS, CT_T_ID, CT_AMT, CT_NAME)
	 * values (trade_dts, trade_id, se_amount,
	 *	    type_name + " " + trade_qty + " shares of " + s_name)
	 */
	prcashtrans->set_value(0, ptrin._trade_id);
	prcashtrans->set_value(1, trade_dts);
	prcashtrans->set_value(2, se_amount);	
	stringstream ss;
	ss << type_name << " " << trade_qty << " shares of " << s_name;
	prcashtrans->set_value(3, ss.str().c_str());
	
	TRACE( TRACE_TRX_FLOW, "App: %d TR:ct-add-tuple (%ld) (%ld) (%lf) (%s)\n",
	       xct_id, trade_dts, ptrin._trade_id, se_amount, ss.str().c_str());
	W_DO(_pcash_transaction_man->add_tuple(_pssm, prcashtrans));
    } else {
	/**
	 *	select	acct_bal = CA_BAL
	 *	from	CUSTOMER_ACCOUNT
	 *	where	CA_ID = acct_id
	 */

	/*
	 * @note: PIN: this is a little hack
	 *             if prcustaccount is already filled due to above update
	 *             there is no need to do an index probe here
	 */
	TRACE(TRACE_TRX_FLOW,"App: %d TR:ca-index-probe (%ld)\n", xct_id, acct_id);
	W_DO(_pcustomer_account_man->ca_index_probe(_pssm,prcustaccount,acct_id));
	double acct_bal;
	prcustaccount->get_value(5, acct_bal);
    } //END FRAME6

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction
    // dumps the status of all the table rows used
    rtrade.print_tuple();
    rtradetype.print_tuple();
    rholdingsummary.print_tuple();
    rcustaccount.print_tuple();
    rholding.print_tuple();
    rsecurity.print_tuple();
    rbroker.print_tuple();
    rsettlement.print_tuple();
    rcashtrans.print_tuple();
    rcommussionrate.print_tuple();
    rcusttaxrate.print_tuple();
    rtaxrate.print_tuple();
    rcustomer.print_tuple();
    rtradehist.print_tuple();
#endif

    return RCOK;

}


EXIT_NAMESPACE(tpce);    
