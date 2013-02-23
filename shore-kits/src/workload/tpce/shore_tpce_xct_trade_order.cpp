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

/** @file:   shore_tpce_xct_trade_order.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-E TRADE ORDER transaction
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

ENTER_NAMESPACE(tpce);

extern unsigned long lastTradeId;

/******************************************************************** 
 *
 * TPC-E TRADE_ORDER
 *
 ********************************************************************/

w_rc_t ShoreTPCEEnv::xct_trade_order(const int xct_id, trade_order_input_t& ptoin)
{    
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    tuple_guard<customer_account_man_impl> prcustacct(_pcustomer_account_man);
    tuple_guard<customer_man_impl> prcust(_pcustomer_man);
    tuple_guard<broker_man_impl> prbroker(_pbroker_man);
    tuple_guard<account_permission_man_impl> pracctperm(_paccount_permission_man);
    tuple_guard<company_man_impl> prcompany(_pcompany_man);
    tuple_guard<security_man_impl> prsecurity(_psecurity_man);
    tuple_guard<last_trade_man_impl> prlasttrade(_plast_trade_man);
    tuple_guard<holding_summary_man_impl> prholdingsummary(_pholding_summary_man);
    tuple_guard<holding_man_impl> prholding(_pholding_man);
    tuple_guard<charge_man_impl> prcharge(_pcharge_man);
    tuple_guard<trade_man_impl> prtrade(_ptrade_man);
    tuple_guard<trade_type_man_impl> prtradetype(_ptrade_type_man);
    tuple_guard<trade_history_man_impl> prtradehist(_ptrade_history_man);
    tuple_guard<trade_request_man_impl> prtradereq(_ptrade_request_man);
    tuple_guard<customer_taxrate_man_impl> prcusttaxrate(_pcustomer_taxrate_man);
    tuple_guard<taxrate_man_impl> prtaxrate(_ptaxrate_man);
    tuple_guard<commission_rate_man_impl> prcommrate(_pcommission_rate_man);

    rep_row_t areprow(_pcompany_man->ts());

    areprow.set(_pcompany_desc->maxsize());

    prcustacct->_rep = &areprow;
    prcust->_rep = &areprow;
    prbroker->_rep = &areprow;
    pracctperm->_rep = &areprow;
    prtrade->_rep = &areprow;
    prtradereq->_rep = &areprow;
    prtradehist->_rep = &areprow;
    prcompany->_rep = &areprow;
    prsecurity->_rep = &areprow;
    prlasttrade->_rep = &areprow;
    prtradetype->_rep = &areprow;
    prholdingsummary->_rep = &areprow;
    prholding->_rep = &areprow;
    prcharge->_rep = &areprow;
    prcusttaxrate->_rep = &areprow;
    prtaxrate->_rep = &areprow;
    prcommrate->_rep = &areprow;

    rep_row_t lowrep(_pcompany_man->ts());
    rep_row_t highrep(_pcompany_man->ts());

    lowrep.set(_pcompany_desc->maxsize());
    highrep.set(_pcompany_desc->maxsize());

    double requested_price = ptoin._requested_price;
    
    //BEGIN FRAME1
    char cust_f_name[21];  //20
    char cust_l_name[26]; //21
    double acct_bal;
    char tax_id[21]; //20
    short tax_status;
    TIdent cust_id;
    short cust_tier;
    TIdent broker_id;
    
    /**
     * SELECT 	acct_name = CA_NAME, broker_id = CA_B_ID,
     *          cust_id = CA_C_ID, tax_status = CA_TAX_ST
     * FROM 	CUSTOMER_ACCOUNT
     * WHERE	CA_ID = acct_id
     */    
    TRACE( TRACE_TRX_FLOW, "App: %d TO:ca-idx-probe (%ld) \n",
	   xct_id, ptoin._acct_id);
    W_DO(_pcustomer_account_man->ca_index_probe(_pssm,prcustacct,ptoin._acct_id));

    char acct_name[51] = "\0"; //50
    prcustacct->get_value(1, broker_id);
    prcustacct->get_value(2, cust_id);
    prcustacct->get_value(3, acct_name, 51);
    prcustacct->get_value(4, tax_status);
    // PIN: might be needed in the future;
    //      getting it now since already done the probe
    prcustacct->get_value(5, acct_bal);
    
    assert(acct_name[0] != 0); //Harness control
    
    /**
     * 	SELECT	cust_f_name = C_F_NAME,	cust_l_name = C_L_NAME,
     *          cust_tier = C_TIER, tax_id = C_TAX_ID
     *	FROM	CUSTOMER
     *	WHERE 	C_ID = cust_id
     */
    TRACE( TRACE_TRX_FLOW, "App: %d TO:c-idx-probe (%ld) \n", xct_id, cust_id);
    W_DO(_pcustomer_man->c_index_probe(_pssm, prcust, cust_id));

    prcust->get_value(1, tax_id, 21);
    prcust->get_value(3, cust_l_name, 26);
    prcust->get_value(4, cust_f_name, 21);
    prcust->get_value(7, cust_tier);
    
    /**
     * 	SELECT	broker_name = B_NAME
     *	FROM	BROKER
     *	WHERE	B_ID = broker_id
     */
    guard< index_scan_iter_impl<broker_t> > br_iter;
    {
	index_scan_iter_impl<broker_t>* tmp_br_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d BV:b-get-iter-by-idx3 (%s) \n",
	       xct_id, broker_id);
	W_DO(_pbroker_man->b_get_iter_by_index2(_pssm, tmp_br_iter, prbroker,
						lowrep, highrep, broker_id));
	br_iter = tmp_br_iter;
    }
    bool eof;
    TRACE( TRACE_TRX_FLOW, "App: %d TO:br-iter-next \n", xct_id);
    W_DO(br_iter->next(_pssm, eof, *prbroker));
    if(eof) { W_DO(RC(se_NOT_FOUND)); }

    char broker_name[50];
    prbroker->get_value(2, broker_name, 50);
    //END FRAME1
		
    /* @note: PIN spec says the following for Trade-Order in 3.3.7:
     * "Next, the Transaction conditionally validates that the person executing
     * the trade is authorized to perform such actions on the specified account.
     * If the executor is not authorized, then the Transaction rolls back.
     * However, during the benchmark execution, the CE will always generate
     * authorized executors." So, 
     */
    
    //BEGIN FRAME2
    
    /**
     * 	select
     *		ap_acl = AP_ACL
     *	from
     *		ACCOUNT_PERMISSION
     *	where
     *		AP_CA_ID = acct_id and
     *		AP_F_NAME = exec_f_name and
     *		AP_L_NAME = exec_l_name and
     *		AP_TAX_ID = exec_tax_id
     */    
    if(strcmp(ptoin._exec_l_name, cust_l_name) != 0 ||
       strcmp(ptoin._exec_f_name, cust_f_name) != 0 ||
       strcmp(ptoin._exec_tax_id, tax_id) != 0 ) {
	
	TRACE( TRACE_TRX_FLOW, "App: %d TO:ap-idx-probe (%ld) (%s) \n",
	       xct_id,  ptoin._acct_id, ptoin._exec_tax_id);
	W_DO(_paccount_permission_man->ap_index_probe(_pssm, pracctperm,
						      ptoin._acct_id,
						      ptoin._exec_tax_id));
	
	char f_name[21], l_name[26];
	pracctperm->get_value(3, l_name, 26);
	pracctperm->get_value(4, f_name, 21);
	
	char ap_acl[5] = ""; //4
	if(strcmp(ptoin._exec_l_name, l_name) == 0 &&
	   strcmp(ptoin._exec_f_name, f_name) == 0) {
	    pracctperm->get_value(1, ap_acl, 5);
	} else {
	    W_DO(RC(se_NOT_FOUND));
	}
	
	assert(strcmp(ap_acl, "") != 0); // Harness Control
    }
    //END FRAME2

    //BEGIN FRAME3
    double comm_rate;
    char status_id[5]; //4
    double charge_amount;
    bool type_is_market;    
    double buy_value;
    double sell_value;
    double tax_amount;
    char symbol[16]; //15
    strcpy(symbol, ptoin._symbol);
    TIdent co_id;
    char co_name[61]; //60
    char exch_id[7]; //6    
    double cust_assets;
    double market_price;
    char s_name[71]; //70
    bool 	type_is_sell;

    if(symbol[0] == '\0'){
	/**
	 * 	select
	 *		co_id = CO_ID
	 *	from
	 *		COMPANY
	 *	where
	 *		CO_NAME = co_name
	 */
	guard< index_scan_iter_impl<company_t> > co_iter;
	{
	    index_scan_iter_impl<company_t>* tmp_co_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d TO:co-get-iter-by-idx2 (%s) \n",
		   xct_id, ptoin._co_name);
	    W_DO(_pcompany_man->co_get_iter_by_index2(_pssm, tmp_co_iter,
						      prcompany, lowrep, highrep,
						      ptoin._co_name));
	    co_iter = tmp_co_iter;
	}
	bool eof;
	TRACE( TRACE_TRX_FLOW, "App: %d TO:co-iter-next \n", xct_id);
	W_DO(co_iter->next(_pssm, eof, *prcompany));
	prcompany->get_value(0, co_id);
		
	/**
	 * 	select
	 *		exch_id = S_EX_ID,
	 *		s_name = S_NAME,
	 *		symbol = S_SYMB
	 *	from
	 *		SECURITY
	 *	where
	 *		S_CO_ID = co_id and
	 *		S_ISSUE = issue
	 *
	 */
	guard< index_scan_iter_impl<security_t> > s_iter;
	{
	    index_scan_iter_impl<security_t>* tmp_s_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d TO:s-get-iter-by-idx2 (%ld) (%s) \n",
		   xct_id, co_id, ptoin._issue);
	    W_DO(_psecurity_man->s_get_iter_by_index4(_pssm, tmp_s_iter,
						      prsecurity, lowrep, highrep,
						      co_id, ptoin._issue));
	    s_iter = tmp_s_iter;
	}	
	TRACE( TRACE_TRX_FLOW, "App: %d TO:s-iter-next \n", xct_id);
	W_DO(s_iter->next(_pssm, eof, *prsecurity));
	while(!eof){
	    prsecurity->get_value(0, symbol, 16);
	    prsecurity->get_value(3, s_name, 71);				
	    prsecurity->get_value(4, exch_id, 7);	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TO:s-iter-next \n", xct_id);
	    W_DO(s_iter->next(_pssm, eof, *prsecurity));
	}
    } else {
	/**
	 * 	select
	 *		co_id = S_CO_ID,
	 *		exch_id = S_EX_ID,
	 *		s_name = S_NAME
	 *	from
	 *		SECURITY
	 *	where
	 *		S_SYMB = symbol
	 */	
	TRACE( TRACE_TRX_FLOW, "App: %d TO:s-idx-probe (%s) \n", xct_id, symbol);
	W_DO(_psecurity_man->s_index_probe(_pssm, prsecurity, symbol));
	prsecurity->get_value(3, s_name, 71);
	prsecurity->get_value(4, exch_id, 7);
	prsecurity->get_value(5, co_id);

	/**
	 * 	select
	 *		co_name = CO_NAME
	 *	from
	 *		COMPANY
	 *	where
	 *		CO_ID = co_id
	 */	
	TRACE( TRACE_TRX_FLOW, "App: %d TO:co-idx-probe (%ld) \n", xct_id, co_id);
	W_DO(_pcompany_man->co_index_probe(_pssm, prcompany, co_id));
	prcompany->get_value(2, co_name, 61);
    }
    
    /**
     * 	select
     *		market_price = LT_PRICE
     *	from
     *		LAST_TRADE
     *	where
     *		LT_S_SYMB = symbol
     */
    TRACE( TRACE_TRX_FLOW, "App: %d TO:lt-idx-probe (%s) \n", xct_id, symbol);
    W_DO(_plast_trade_man->lt_index_probe(_pssm, prlasttrade, symbol));
    prlasttrade->get_value(2, market_price);
    
    /**
     * 	select
     *		type_is_market = TT_IS_MRKT,
     *		type_is_sell = TT_IS_SELL
     *	from
     *		TRADE_TYPE
     *	where
     *		TT_ID = trade_type_id
     *
     */
    TRACE( TRACE_TRX_FLOW, "App: %d TO:tt-idx-probe (%s) \n",
	   xct_id, ptoin._trade_type_id);
    W_DO(_ptrade_type_man->tt_index_probe(_pssm, prtradetype,
					  ptoin._trade_type_id));
    prtradetype->get_value(2, type_is_sell);
    prtradetype->get_value(3, type_is_market);
    
    if(type_is_market){
	requested_price = market_price;
    }
    
    double hold_price;
    int hold_qty;
    int needed_qty = ptoin._trade_qty;
    int hs_qty = -1;
    
    buy_value = 0;
    sell_value = 0;
    
    /**
     * 	select
     *		hs_qty = HS_QTY
     *	from
     *		HOLDING_SUMMARY
     *	where
     *		HS_CA_ID = acct_id and
     *		HS_S_SYMB = symbol
     */
    TRACE( TRACE_TRX_FLOW, "App: %d TO:hs-idx-probe (%ld) (%s) \n",
	   xct_id, ptoin._acct_id, symbol);
    if((_pholding_summary_man->hs_index_probe(_pssm,
					      prholdingsummary,
					      ptoin._acct_id,
					      symbol)).is_error()) {
	hs_qty = 0;			
    } else {
	prholdingsummary->get_value(2, hs_qty);
    }
    
    /* PIN: I made some changes regarding the query plan here:
     * (1) Since "if(type_is_sell)" branch cause some code repetition,
     *     instead this condition is put only on the places where it actually
     *     makes a difference. There are only two such places;
     *     (a) The condition that decides on whether to do the index scan or not
     *     (b)        eDoraLockMode req_lm = DL_CC_EXCL;
     =>      if (base_action_t::is_read_only()) req_lm = DL_CC_SHARED;
     ) How to set the new values of buy_value, sell_value, and needed_qty
     * Maybe this won't make that big of a difference in terms of performance but
     * the code is much cleaner now.
     */
    if((type_is_sell && hs_qty > 0) || (!type_is_sell && hs_qty < 0)) {
	guard<index_scan_iter_impl<holding_t> > h_iter;
	if(ptoin._is_lifo){
	    /**
	     * SELECT	H_QTY, H_PRICE
	     * FROM		HOLDING
	     * WHERE	H_CA_ID = acct_id and H_S_SYMB = symbol
	     * ORDER BY 	H_DTS DESC
	     */
	    {  
		index_scan_iter_impl<holding_t>* tmp_h_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d TO:h-iter-by-idx2 (%ld) (%s) \n",
		       xct_id, ptoin._acct_id, symbol);
		W_DO(_pholding_man->h_get_iter_by_index2(_pssm, tmp_h_iter,
							 prholding, lowrep,
							 highrep, ptoin._acct_id,
							 symbol, true));
		h_iter = tmp_h_iter;
	    }
	} else {
	    /**
	     * 	SELECT	H_QTY, H_PRICE
	     *   	FROM	HOLDING
	     *	WHERE	H_CA_ID = acct_id and H_S_SYMB = symbol
	     *	ORDER BY H_DTS ASC
	     */
	    {
		index_scan_iter_impl<holding_t>* tmp_h_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d TO:h-iter-by-idx2 \n", xct_id);
		W_DO(_pholding_man->h_get_iter_by_index2(_pssm, tmp_h_iter,
							 prholding, lowrep,
							 highrep, ptoin._acct_id,
							 symbol));
		h_iter = tmp_h_iter;
	    }
	}
	
	bool eof;
	TRACE( TRACE_TRX_FLOW, "App: %d TO:h-iter-next \n", xct_id);
	W_DO(h_iter->next(_pssm, eof, *prholding));
	while (needed_qty != 0 && !eof) {
	    int hold_qty;
	    double hold_price;
	    
	    prholding->get_value(4, hold_price);
	    prholding->get_value(5, hold_qty);
	    
	    if(type_is_sell) {
		if(hold_qty > needed_qty) {
		    buy_value += needed_qty * hold_price;
		    sell_value += needed_qty * requested_price;
		    needed_qty = 0;
		} else {
		    buy_value += hold_qty * hold_price;
		    sell_value += hold_qty * requested_price;
		    needed_qty = needed_qty - hold_qty;
		}
	    } else {
		if(hold_qty + needed_qty < 0){
		    sell_value += needed_qty * hold_price;
		    buy_value += needed_qty * requested_price;
		    needed_qty = 0;
		} else {
		    hold_qty = -hold_qty;
		    sell_value += hold_qty * hold_price;
		    buy_value += hold_qty * requested_price;
		    needed_qty = needed_qty - hold_qty;
		}
	    }	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TO:h-iter-next \n", xct_id);
	    W_DO(h_iter->next(_pssm, eof, *prholding));
	}
    }
    
    if((sell_value > buy_value) && ((tax_status == 1 || tax_status == 2))) {
	double tax_rates;
	/**
	 *	select
	 *		tax_rates = sum(TX_RATE)
	 *	from
	 *		TAXRATE
	 *	where
	 *		TX_ID in (
	 *				select
	 *					CX_TX_ID
	 *				from
	 *					CUSTOMER_TAXRATE
	 *				where
	 *					CX_C_ID = cust_id
	 *			)
	 */
	guard< index_scan_iter_impl<customer_taxrate_t> > cx_iter;
	{
	    index_scan_iter_impl<customer_taxrate_t>* tmp_cx_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d TO:cx-get-iter-by-idx (%ld) \n",
		   xct_id, cust_id);
	    W_DO(_pcustomer_taxrate_man->cx_get_iter_by_index(_pssm, tmp_cx_iter,
							      prcusttaxrate,
							      lowrep, highrep,
							      cust_id));
	    cx_iter = tmp_cx_iter;
	}
	
	bool eof;
	TRACE( TRACE_TRX_FLOW, "App: %d TO:cx-iter-next \n", xct_id);
	W_DO(cx_iter->next(_pssm, eof, *prcusttaxrate));
	while (!eof) {
	    char tax_id[5]; //4
	    prcusttaxrate->get_value(0, tax_id, 5);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TO:tx-idx-probe (%s) \n",
		   xct_id, tax_id);
	    W_DO(_ptaxrate_man->tx_index_probe(_pssm, prtaxrate, tax_id));

	    double rate;
	    prtaxrate->get_value(2, rate);
	    tax_rates += rate;
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TO:cx-iter-next \n", xct_id);
	    W_DO(cx_iter->next(_pssm, eof, *prcusttaxrate));
	}
	tax_amount = (sell_value - buy_value) * tax_rates;
    }
    
    /**
     *
     *	select
     *		comm_rate = CR_RATE
     *	from
     *		COMMISSION_RATE
     *	where
     *		CR_C_TIER = cust_tier and
     *		CR_TT_ID = trade_type_id and
     *		CR_EX_ID = exch_id and
     *		CR_FROM_QTY <= trade_qty and
     *		CR_TO_QTY >= trade_qty
     */
    guard< index_scan_iter_impl<commission_rate_t> > cr_iter;
    {
	index_scan_iter_impl<commission_rate_t>* tmp_cr_iter;
	TRACE(TRACE_TRX_FLOW, "App: %d TR:cr-iter-by-idx (%d) (%s) (%s) (%d)\n",
	      xct_id, cust_tier, ptoin._trade_type_id, exch_id, ptoin._trade_qty);
	W_DO(_pcommission_rate_man->cr_get_iter_by_index(_pssm, tmp_cr_iter,
							 prcommrate, lowrep,
							 highrep, cust_tier,
							 ptoin._trade_type_id,
							 exch_id,
							 ptoin._trade_qty));
	cr_iter = tmp_cr_iter;
    }

    TRACE( TRACE_TRX_FLOW, "App: %d TO:cr-iter-next \n", xct_id);
    W_DO(cr_iter->next(_pssm, eof, *prcommrate));
    while (!eof) {
	int to_qty;
	prcommrate->get_value(4, to_qty);
	
	if(to_qty >= ptoin._trade_qty){
	    prcommrate->get_value(5, comm_rate);
	    break;
	}
	
	TRACE( TRACE_TRX_FLOW, "App: %d TO:cr-iter-next \n", xct_id);
	W_DO(cr_iter->next(_pssm, eof, *prcommrate));	
    }
    
    /**
     *
     *	select
     *		charge_amount = CH_CHRG
     *	from
     *		CHARGE
     *	where
     *		CH_C_TIER = cust_tier and
     *		CH_TT_ID = trade_type_id
     */
    TRACE( TRACE_TRX_FLOW, "App: %d TO:ch-idx-probe (%d) (%s) \n",
	   xct_id, cust_tier, ptoin._trade_type_id);
    W_DO(_pcharge_man->ch_index_probe(_pssm, prcharge, cust_tier,
				      ptoin._trade_type_id));
    prcharge->get_value(2, charge_amount);
    
    double hold_assets = 0;
    cust_assets = 0;
    
    if(ptoin._type_is_margin) {
	/**
	 *	select
	 *		acct_bal = CA_BAL
	 *	from
	 *		CUSTOMER_ACCOUNT
	 *	where
	 *		CA_ID = acct_id
	 */

	/*
	 * @note: PIN: since prcustacct is known due to the index probe from above
	 * we are going to get the acct_bal there regardless of this branch
	 */
	//TRACE( TRACE_TRX_FLOW, "App: %d TO:ca-idx-probe (%ld) \n",
	//       xct_id, ptoin._acct_id);
	//W_DO(_pcustomer_account_man->ca_index_probe(_pssm, prcustacct,
	//                                            ptoin._acct_id));
	//prcustacct->get_value(5, acct_bal);

	/**
	 *
	 *	select
	 *		hold_assets = sum(HS_QTY * LT_PRICE)
	 *	from
	 *		HOLDING_SUMMARY,
	 *		LAST_TRADE
	 *	where
	 *		HS_CA_ID = acct_id and
	 *		LT_S_SYMB = HS_S_SYMB
	 */
	guard< index_scan_iter_impl<holding_summary_t> > hs_iter;
	{
	    index_scan_iter_impl<holding_summary_t>* tmp_hs_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d TO:hs-iter-by-idx (%ld) \n",
		   xct_id, ptoin._acct_id);
	    W_DO(_pholding_summary_man->hs_get_iter_by_index(_pssm, tmp_hs_iter,
							     prholdingsummary,
							     lowrep, highrep,
							     ptoin._acct_id));
	    hs_iter = tmp_hs_iter;
	}
	
	TRACE( TRACE_TRX_FLOW, "App: %d TO:hs-iter-next \n", xct_id);
	W_DO(hs_iter->next(_pssm, eof, *prholdingsummary));
	while (!eof) {
	    char symb[16]; //15
	    prholdingsummary->get_value(1, symb, 16);
	    int hs_qty;
	    prholdingsummary->get_value(2, hs_qty);
	    
	    TRACE( TRACE_TRX_FLOW, "App:%d TO:lt-idx-probe (%s)\n", xct_id, symb);
	    W_DO(_plast_trade_man->lt_index_probe(_pssm, prlasttrade, symb));

	    double lt_price;
	    prlasttrade->get_value(3, lt_price);
	    hold_assets += (lt_price * hs_qty);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TO:hs-iter-next \n", xct_id);
	    W_DO(hs_iter->next(_pssm, eof, *prholdingsummary));
	}
	cust_assets = hold_assets + acct_bal;
    }
    
    // Set the status for this trade
    if (type_is_market) {
	strcpy(status_id, ptoin._st_submitted_id);
    } else {
	strcpy(status_id, ptoin._st_pending_id);
    }
    //END FRAME3
    
    if((sell_value > buy_value) &&
       ((tax_status == 1) || (tax_status == 2)) && (tax_amount == 0)) {
	assert(false); //Harness control
    } else if(comm_rate == 0.0000) {
	assert(false); //Harness control
    } else if(charge_amount == 0) {
	assert(false); //Harness control
    }
    
    double comm_amount = (comm_rate/100) * ptoin._trade_qty * requested_price;
    char exec_name[50]; //49
    strcpy(exec_name, ptoin._exec_f_name);
    strcat(exec_name, " ");
    strcat(exec_name, ptoin._exec_l_name);
    bool is_cash = !ptoin._type_is_margin;
    TIdent trade_id;

    //BEGIN FRAME4
    myTime now_dts = time(NULL);
    
    trade_id = (long long) atomic_inc_64_nv(&lastTradeId);			  
	    
    /**
     *	INSERT INTO TRADE (T_ID, T_DTS, T_ST_ID, T_TT_ID, T_IS_CASH,
     *			   T_S_SYMB, T_QTY, T_BID_PRICE, T_CA_ID, T_EXEC_NAME,
     *		           T_TRADE_PRICE, T_CHRG, T_COMM, T_TAX, T_LIFO)
     *	VALUES 		  (trade_id, // T_ID
     *		           now_dts, // T_DTS
     *			   status_id, // T_ST_ID
     *			   trade_type_id, // T_TT_ID
     *			   is_cash, // T_IS_CASH
     *			   symbol, // T_S_SYMB
     *			   trade_qty, // T_QTY
     *			   requested_price, // T_BID_PRICE
     *			   acct_id, // T_CA_ID
     *			   exec_name, // T_EXEC_NAME
     *			   NULL, // T_TRADE_PRICE
     *			   charge_amount, // T_CHRG
     *			   comm_amount // T_COMM
     *			   0, // T_TAX
     *			   is_lifo // T_LIFO
     *			  )
     */
    prtrade->set_value(0, trade_id);
    prtrade->set_value(1, now_dts);
    prtrade->set_value(2, status_id);
    prtrade->set_value(3, ptoin._trade_type_id);
    prtrade->set_value(4, is_cash);
    prtrade->set_value(5, symbol);
    prtrade->set_value(6, ptoin._trade_qty);
    prtrade->set_value(7, requested_price);
    prtrade->set_value(8, ptoin._acct_id);
    prtrade->set_value(9, exec_name);
    prtrade->set_value(10, (double)-1);
    prtrade->set_value(11, charge_amount);
    prtrade->set_value(12, comm_amount);
    prtrade->set_value(13, (double)0);
    prtrade->set_value(14, ptoin._is_lifo);
    
    TRACE( TRACE_TRX_FLOW, "App: %d TO:t-add-tuple (%ld) \n", xct_id, trade_id);
    W_DO(_ptrade_man->add_tuple(_pssm, prtrade));

    if(!type_is_market) {
	/**
	 * INSERT INTO TRADE_REQUEST 	(TR_T_ID, TR_TT_ID, TR_S_SYMB,
	 *				 TR_QTY, TR_BID_PRICE, TR_B_ID)
	 * VALUES 			(trade_id, // TR_T-ID
	 *				 trade_type_id, // TR_TT_ID
	 *				 symbol, // TR_S_SYMB
	 *				 trade_qty, // TR_QTY
	 *				 requested_price, // TR_BID_PRICE
	 *				 broker_id // TR_B_ID
	 *				)
	 */
	prtradereq->set_value(0, trade_id);
	prtradereq->set_value(1, ptoin._trade_type_id);
	prtradereq->set_value(2, symbol);
	prtradereq->set_value(3, ptoin._trade_qty);
	prtradereq->set_value(4, requested_price);
	prtradereq->set_value(5, broker_id);
	
	TRACE( TRACE_TRX_FLOW, "App: %d TO:tr-add-tuple (%ld) \n",
	       xct_id, trade_id);
	W_DO(_ptrade_request_man->add_tuple(_pssm, prtradereq));
    }
    
    /**
     *	INSERT INTO TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID)
     *	VALUES 			  (trade_id, // TH_T_ID
     *				   now_dts, // TH_DTS
     *				   status_id // TH_ST_ID
     *				  )
     **/
    prtradehist->set_value(0, trade_id);
    prtradehist->set_value(1, now_dts);
    prtradehist->set_value(2, status_id);

    TRACE( TRACE_TRX_FLOW, "App: %d TO:th-add-tuple (%ld) \n", xct_id, trade_id);
    W_DO(_ptrade_history_man->add_tuple(_pssm, prtradehist));
    //END FRAME4
    
    //BEGIN FRAME5
    if(ptoin._roll_it_back) {
	TRACE( TRACE_TRX_FLOW, "App: %d TO:ROLLBACK \n", xct_id);
	W_DO(RC(se_NOT_FOUND));
    }
    //END FRAME5
    
    //BEGIN FRAME6
    //send TradeRequest to Market
    PTradeRequest req = new TTradeRequest();
    req->trade_id = trade_id;
    req->trade_qty = ptoin._trade_qty;
    strcpy(req->symbol, symbol);
    strcpy(req->trade_type_id, ptoin._trade_type_id);
    req->price_quote = requested_price;
    if(type_is_market) {
	req->eAction=eMEEProcessOrder;
    } else {
	req->eAction=eMEESetLimitOrderTrigger;
    }
    mee->SubmitTradeRequest(req);
    delete req;
    // END FRAME6
    
#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction
    // dumps the status of all the table rows used
    rcustacct.print_tuple();
    rcust.print_tuple();
    rbrok.print_tuple();
    racctperm.print_tuple();
    rtrade.print_tuple();
    rtradereq.print_tuple();
    rtradehist.print_tuple();
    rcompany.print_tuple();
    rsecurity.print_tuple();
    rlasttrade.print_tuple();
    rtradetype.print_tuple();
    rholdingsummary.print_tuple();
    rholding.print_tuple();
    rcharge.print_tuple();
    rcusttaxrate.print_tuple();
    rtaxrate.print_tuple();
    rcommrate.print_tuple();
#endif

    return RCOK;

}


EXIT_NAMESPACE(tpce);    
