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

/** @file:   shore_tpce_xct_trade_lookup.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-E TRADE LOOKUP transaction
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

/******************************************************************** 
 *
 * TPC-E TRADE LOOKUP
 *
 ********************************************************************/

w_rc_t ShoreTPCEEnv::xct_trade_lookup(const int xct_id,
				      trade_lookup_input_t& ptlin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    tuple_guard<trade_man_impl> prtrade(_ptrade_man);
    tuple_guard<trade_type_man_impl> prtradetype(_ptrade_type_man);
    tuple_guard<settlement_man_impl> prsettlement(_psettlement_man);
    tuple_guard<cash_transaction_man_impl> prcashtrans(_pcash_transaction_man);
    tuple_guard<trade_history_man_impl> prtradehist(_ptrade_history_man);
    tuple_guard<holding_history_man_impl> prholdinghistory(_pholding_history_man);

    rep_row_t areprow(_ptrade_man->ts());

    areprow.set(_ptrade_desc->maxsize());

    prtrade->_rep = &areprow;
    prtradetype->_rep = &areprow;
    prsettlement->_rep = &areprow;
    prcashtrans->_rep = &areprow;
    prtradehist->_rep = &areprow;
    prholdinghistory->_rep = &areprow;

    rep_row_t lowrep(_ptrade_man->ts());
    rep_row_t highrep(_ptrade_man->ts());

    // allocate space for the biggest of the table representations
    lowrep.set(_ptrade_desc->maxsize());
    highrep.set(_ptrade_desc->maxsize());

    int max_trades = ptlin._max_trades;

    array_guard_t<double> bid_price = new double[max_trades];
    array_guard_t< char[50] > exec_name = new char[max_trades][50];
    array_guard_t<bool> is_cash = new bool[max_trades];
    array_guard_t<bool> is_market = new bool[max_trades];
    array_guard_t<double> trade_price = new double[max_trades];    
    array_guard_t<TIdent> trade_list = new TIdent[max_trades];
    array_guard_t<double> settlement_amount = new double[max_trades];
    array_guard_t<myTime> settlement_cash_due_date = new myTime[max_trades];
    array_guard_t< char[41] > settlement_cash_type = new char[max_trades][41];
    array_guard_t<double> cash_transaction_amount = new double[max_trades];
    array_guard_t<myTime> cash_transaction_dts = new myTime[max_trades];
    array_guard_t< char[101] > cash_transaction_name = new char[max_trades][101];
    array_guard_t< myTime[3] > trade_history_dts = new myTime[max_trades][3];
    array_guard_t< char[3][5] > trade_history_status_id = new char[max_trades][3][5];
    array_guard_t<TIdent> acct_id = new TIdent[max_trades];
    array_guard_t<int> quantity = new int[max_trades];
    array_guard_t< char[4] > trade_type = new char[max_trades][4];
    array_guard_t<myTime> trade_dts = new myTime[max_trades];

    //BEGIN FRAME1
    int num_found = 0;
    if(ptlin._frame_to_execute == 1) {	
	for (num_found = 0; num_found < max_trades; num_found++){
	    /**
	     *	select
	     *		bid_price[i] = T_BID_PRICE,
	     *		exec_name[i] = T_EXEC_NAME,
	     *		is_cash[i] = T_IS_CASH,
	     *		is_market[i] = TT_IS_MRKT,
	     *		trade_price[i] = T_TRADE_PRICE
	     *	from
	     *		TRADE,
	     *		TRADE_TYPE
	     *	where
	     *		T_ID = trade_id[i] and
	     *		T_TT_ID = TT_ID
	     */		
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:t-idx-probe (%ld) \n",
		   xct_id, ptlin._trade_id[num_found]);
	    W_DO(_ptrade_man->t_index_probe(_pssm, prtrade,
					    ptlin._trade_id[num_found]));

	    prtrade->get_value(4, is_cash[num_found]);
	    prtrade->get_value(7, bid_price[num_found]);
	    prtrade->get_value(9, exec_name[num_found], 50); //49
	    prtrade->get_value(10, trade_price[num_found]);	    
	    char tt_id[4]; //3
	    prtrade->get_value(3, tt_id, 4);
	    
	    TRACE( TRACE_TRX_FLOW,"App:%d TL:tt-idx-probe (%s)\n", xct_id, tt_id);
	    W_DO(_ptrade_type_man->tt_index_probe(_pssm, prtradetype, tt_id));

	    prtradetype->get_value(3, is_market[num_found]);

	    /**
	     *	select
	     *		settlement_amount[i] = SE_AMT,
	     *		settlement_cash_due_date[i] = SE_CASH_DUE_DATE,
	     *		settlement_cash_type[i] = SE_CASH_TYPE
	     *	from
	     *		SETTLEMENT
	     *	where
	     *		SE_T_ID = trade_id[i]
	     */	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:se-idx-probe (%ld) \n",
		   xct_id, ptlin._trade_id[num_found]);
	    W_DO(_psettlement_man->se_index_probe(_pssm, prsettlement,
						  ptlin._trade_id[num_found]));

	    prsettlement->get_value(3, settlement_amount[num_found]);
	    prsettlement->get_value(2, settlement_cash_due_date[num_found]);
	    prsettlement->get_value(1, settlement_cash_type[num_found], 41); //40

	    // get cash information if this is a cash transaction, should
	    // only return one row for each trade that was a cash transaction
	    if(is_cash[num_found]) {
		/**
		 *	select
		 *		cash_transaction_amount[i] = CT_AMT,
		 *		cash_transaction_dts[i] = CT_DTS,
		 *		cash_transaction_name[i] = CT_NAME
		 *	from
		 *		CASH_TRANSACTION
		 *	where
		 *		CT_T_ID = trade_id[i]
		 *
		 */
		
		TRACE( TRACE_TRX_FLOW, "App: %d TL:ct-idx-probe (%ld) \n",
		       xct_id, ptlin._trade_id[num_found]);
		W_DO(_pcash_transaction_man->ct_index_probe(_pssm, prcashtrans,
							    ptlin._trade_id[num_found]));

		prcashtrans->get_value(1, cash_transaction_dts[num_found]);
		prcashtrans->get_value(2, cash_transaction_amount[num_found]);
		prcashtrans->get_value(3, cash_transaction_name[num_found], 101);
	    }
	    
	    /**
	     *	select first 3 rows
	     *		trade_history_dts[i][] = TH_DTS,
	     *		trade_history_status_id[i][] = TH_ST_ID
	     *	from
	     *		TRADE_HISTORY
	     *	where
	     *		TH_T_ID = trade_id[i]
	     *	order by
	     *		TH_DTS
	     */		
	    guard<index_scan_iter_impl<trade_history_t> > th_iter;
	    {
		index_scan_iter_impl<trade_history_t>* tmp_th_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d TL:th-iter-by-trade-idx (%ld) \n",
		       xct_id, ptlin._trade_id[num_found]);
		W_DO(_ptrade_history_man->th_get_iter_by_index(_pssm, tmp_th_iter,
							       prtradehist,
							       lowrep, highrep,
							       ptlin._trade_id[num_found]));
		th_iter = tmp_th_iter;
	    }
	    
	    // ascending order due to index
	    int j=0;
	    bool eof;
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:th-iter-next \n", xct_id);
	    W_DO(th_iter->next(_pssm, eof, *prtradehist));
	    while (!eof && j<3) {
		prtradehist->get_value(1, trade_history_dts[num_found][j]);
		prtradehist->get_value(2, trade_history_status_id[num_found][j],
				       5);
		j++;	    
		TRACE( TRACE_TRX_FLOW, "App: %d TL:th-iter-next \n", xct_id);
		W_DO(th_iter->next(_pssm, eof, *prtradehist));
	    }
	}
	assert(num_found == max_trades); //Harness control
    } //END FRAME1
    //BEGIN FRAME2
    else if(ptlin._frame_to_execute == 2) {
	// Get trade information
	// Should return between 0 and max_trades rows
	
	/**
	 *	select first max_trades rows
	 *		bid_price[] = T_BID_PRICE,
	 *		exec_name[] = T_EXEC_NAME,
	 *		is_cash[] = T_IS_CASH,
	 *		trade_list[] = T_ID,
	 *		trade_price[] = T_TRADE_PRICE
	 *	from
	 *		TRADE
	 *	where
	 *		T_CA_ID = acct_id and
	 *		T_DTS >= start_trade_dts and
	 *		T_DTS <= end_trade_dts
	 *	order by
	 *		T_DTS asc
	 */	
	guard<index_scan_iter_impl<trade_t> > t_iter;
	{
	    index_scan_iter_impl<trade_t>* tmp_t_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:t-iter-by-idx2 %ld %ld %ld \n",
		   xct_id, ptlin._acct_id, ptlin._start_trade_dts,
		   ptlin._end_trade_dts);
	    W_DO(_ptrade_man->t_get_iter_by_index2(_pssm, tmp_t_iter, prtrade,
						   lowrep, highrep,
						   ptlin._acct_id,
						   ptlin._start_trade_dts,
						   ptlin._end_trade_dts));
	    t_iter = tmp_t_iter;
	}

	//already sorted in ascending order because of index
	bool eof;
	TRACE( TRACE_TRX_FLOW, "App: %d TL:t-iter-next \n", xct_id);
	W_DO(t_iter->next(_pssm, eof, *prtrade));
	for(num_found = 0 ; num_found < max_trades && !eof ; num_found++) {
	    prtrade->get_value(0, trade_list[num_found]);
	    prtrade->get_value(4, is_cash[num_found]);
	    prtrade->get_value(7, bid_price[num_found]);
	    prtrade->get_value(9, exec_name[num_found], 50); //49
	    prtrade->get_value(10, trade_price[num_found]);	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:t-iter-next \n", xct_id);
	    W_DO(t_iter->next(_pssm, eof, *prtrade));
	}
	
	for(int i = 0; i < num_found; i++) {
	    /**
	     *	select
	     *		settlement_amount[i] = SE_AMT,
	     *		settlement_cash_due_date[i] = SE_CASH_DUE_DATE,
	     *		settlement_cash_type[i] = SE_CASH_TYPE
	     *	from
	     *		SETTLEMENT
	     *	where
	     *		SE_T_ID = trade_list[i]
	     */
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:se-idx-probe (%ld) \n",
		   xct_id, trade_list[i]);
	    W_DO(_psettlement_man->se_index_probe(_pssm, prsettlement,
						  trade_list[i]));

	    prsettlement->get_value(3, settlement_amount[i]);
	    prsettlement->get_value(2, settlement_cash_due_date[i]);
	    prsettlement->get_value(1, settlement_cash_type[i], 41); //40
	    
	    if(is_cash[i]) {
		/**
		 *	select
		 *		cash_transaction_amount[i] = CT_AMT,
		 *		cash_transaction_dts[i] = CT_DTS,
		 *		cash_transaction_name[i] = CT_NAME
		 *	from
		 *		CASH_TRANSACTION
		 *	where
		 *		CT_T_ID = trade_list[i]
		 *
		 */
		TRACE( TRACE_TRX_FLOW, "App: %d TL:ct-idx-probe (%ld) \n",
		       xct_id, trade_list[i]);
		W_DO(_pcash_transaction_man->ct_index_probe(_pssm, prcashtrans,
							    trade_list[i]));

		prcashtrans->get_value(1, cash_transaction_dts[i]);
		prcashtrans->get_value(2, cash_transaction_amount[i]);
		prcashtrans->get_value(3, cash_transaction_name[i], 101); //100
	    }
	    
	    /**
	     *
	     *	select first 3 rows
	     *		trade_history_dts[i][] = TH_DTS,
	     *		trade_history_status_id[i][] = TH_ST_ID
	     *	from
	     *		TRADE_HISTORY
	     *	where
	     *		TH_T_ID = trade_list[i]
	     *	order by
	     *		TH_DTS
	     */
	    guard<index_scan_iter_impl<trade_history_t> > th_iter;
	    {
		index_scan_iter_impl<trade_history_t>* tmp_th_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d TL:th-get-iter-by-idx %ld \n",
		       xct_id, trade_list[i]);
		W_DO(_ptrade_history_man->th_get_iter_by_index(_pssm, tmp_th_iter,
							       prtradehist,
							       lowrep, highrep,
							       trade_list[i]));
		th_iter = tmp_th_iter;
	    }
	    
	    // ascending order due to index
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:th-iter-next \n", xct_id);
	    W_DO(th_iter->next(_pssm, eof, *prtradehist));
	    int j = 0;
	    while (!eof && j<3) {
		prtradehist->get_value(1, trade_history_dts[i][j]);
		prtradehist->get_value(2, trade_history_status_id[i][j], 5);
		j++;
		TRACE( TRACE_TRX_FLOW, "App: %d TL:th-iter-next \n", xct_id);
		W_DO(th_iter->next(_pssm, eof, *prtradehist));
	    }
	}
	assert(num_found >= 0 && num_found <= max_trades); //Harness control
    } //END FRAME2
    //BEGIN FRAME3
    else if(ptlin._frame_to_execute == 3) {  
	// Should return between 0 and max_trades rows.
	/**
	 *
	 *	select first max_trades rows
	 *		acct_id[] = T_CA_ID,
	 *		exec_name[] = T_EXEC_NAME,
	 *		is_cash[] = T_IS_CASH,
	 *		price[] = T_TRADE_PRICE,
	 *		quantity[] = T_QTY,
	 *		trade_dts[] = T_DTS,
	 *		trade_list[] = T_ID,
	 *		trade_type[] = T_TT_ID
	 *	from
	 *		TRADE
	 *	where
	 *		T_S_SYMB = symbol and
	 *		T_DTS >= start_trade_dts and
	 *		T_DTS <= end_trade_dts
	 *	order by
	 *		T_DTS asc
	 *
	 */
	guard<index_scan_iter_impl<trade_t> > t_iter;
	{
	    index_scan_iter_impl<trade_t>* tmp_t_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:t-get-iter-by-idx3 %s %ld %ld \n",
		   xct_id, ptlin._symbol, ptlin._start_trade_dts,
		   ptlin._end_trade_dts);
	    W_DO(_ptrade_man->t_get_iter_by_index3(_pssm, tmp_t_iter, prtrade,
						   lowrep, highrep, ptlin._symbol,
						   ptlin._start_trade_dts,
						   ptlin._end_trade_dts));
	    t_iter = tmp_t_iter;
	}
	
	//already sorted in ascending order because of index
	bool eof;
	TRACE( TRACE_TRX_FLOW, "App: %d TL:t-iter-next \n", xct_id);
	W_DO(t_iter->next(_pssm, eof, *prtrade));
	for(num_found = 0; num_found < max_trades && !eof; num_found++) {
	    prtrade->get_value(0, trade_list[num_found]);
	    prtrade->get_value(1, trade_dts[num_found]);
	    prtrade->get_value(3, trade_type[num_found], 4);
	    prtrade->get_value(4, is_cash[num_found]);
	    prtrade->get_value(6, quantity[num_found]);				
	    prtrade->get_value(8, acct_id[num_found]);
	    prtrade->get_value(9, exec_name[num_found], 50); //49
	    prtrade->get_value(10, trade_price[num_found]);
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:t-iter-next \n", xct_id);
	    W_DO(t_iter->next(_pssm, eof, *prtrade));
	}
	
	for(int i = 0; i < num_found; i++){
	    /**
	     *	select
	     *		settlement_amount[i] = SE_AMT,
	     *		settlement_cash_due_date[i] = SE_CASH_DUE_DATE,
	     *		settlement_cash_type[i] = SE_CASH_TYPE
	     *	from
	     *		SETTLEMENT
	     *	where
	     *		SE_T_ID = trade_list[i]
	     */
	    TRACE( TRACE_TRX_FLOW, "TL: %d SE:se-idx-probe (%ld) \n",
		   xct_id, trade_list[i]);
	    W_DO(_psettlement_man->se_index_probe(_pssm, prsettlement,
						  trade_list[i]));
		
	    prsettlement->get_value(1, settlement_cash_type[i], 41); //40
	    prsettlement->get_value(3, settlement_amount[i]);
	    prsettlement->get_value(2, settlement_cash_due_date[i]);
	    
	    if(is_cash[i]){
		/**
		 *	select
		 *		cash_transaction_amount[i] = CT_AMT,
		 *		cash_transaction_dts[i] = CT_DTS,
		 *		cash_transaction_name[i] = CT_NAME
		 *	from
		 *		CASH_TRANSACTION
		 *	where
		 *		CT_T_ID = trade_list[i]
		 */		    
		TRACE( TRACE_TRX_FLOW, "App: %d TL:ct-idx-probe (%ld) \n",
		       xct_id, trade_list[i]);
		W_DO(_pcash_transaction_man->ct_index_probe(_pssm, prcashtrans,
							    trade_list[i]));
		    
		prcashtrans->get_value(1, cash_transaction_dts[i]);
		prcashtrans->get_value(2, cash_transaction_amount[i]);
		prcashtrans->get_value(3, cash_transaction_name[i], 101); //100
	    }
	    
	    /**
	     *	select first 3 rows
	     *		trade_history_dts[i][] = TH_DTS,
	     *		trade_history_status_id[i][] = TH_ST_ID
	     *	from
	     *		TRADE_HISTORY
	     *	where
	     *		TH_T_ID = trade_list[i]
	     *	order by
	     *		TH_DTS
	     */
	    guard<index_scan_iter_impl<trade_history_t> > th_iter;
	    {
		index_scan_iter_impl<trade_history_t>* tmp_th_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d TL:th-get-iter-by-idx %ld \n",
		       xct_id, trade_list[i]);
		W_DO(_ptrade_history_man->th_get_iter_by_index(_pssm, tmp_th_iter,
							       prtradehist,
							       lowrep, highrep,
							       trade_list[i]));
		th_iter = tmp_th_iter;
	    }
	    
	    //ascending order due to index
	    int j=0;
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:th-iter-next \n", xct_id);
	    W_DO(th_iter->next(_pssm, eof, *prtradehist));
	    while (!eof && j<3) {
		prtradehist->get_value(1, trade_history_dts[i][j]);
		prtradehist->get_value(2, trade_history_status_id[i][j], 5);
		j++;
		TRACE( TRACE_TRX_FLOW, "App: %d TL:th-iter-next \n", xct_id);
		W_DO(th_iter->next(_pssm, eof, *prtradehist));
	    }
	}
	assert(num_found >= 0 && num_found <= max_trades); //Harness control
    } //END FRAME3
    //BEGIN FRAME4
    else if(ptlin._frame_to_execute == 4) {
	
	TIdent  holding_history_id[20];
	TIdent  holding_history_trade_id[20];
	int	quantity_before[20];
	int	quantity_after[20];
	    
	/**
	 *	select first 1 row
	 *		trade_id = T_ID
	 *	from
	 *		TRADE
	 *	where
	 *		T_CA_ID = acct_id and
	 *		T_DTS >= start_trade_dts
	 *	order by
	 *		T_DTS asc
	 */	    
	guard<index_scan_iter_impl<trade_t> > t_iter;
	{
	    index_scan_iter_impl<trade_t>* tmp_t_iter;
	    TRACE( TRACE_TRX_FLOW,
		   "App: %d TL:t-get-iter-by-idx2 (%ld) (%ld) (%ld) \n",
		   xct_id, ptlin._acct_id, ptlin._start_trade_dts, MAX_DTS);
	    W_DO(_ptrade_man->t_get_iter_by_index2(_pssm, tmp_t_iter, prtrade,
						   lowrep, highrep,
						   ptlin._acct_id,
						   ptlin._start_trade_dts,
						   MAX_DTS, false, false));
	    t_iter = tmp_t_iter;
	}

	//already sorted in ascending order because of index	    
	bool eof;
	W_DO(t_iter->next(_pssm, eof, *prtrade));
	    	    
	TIdent trade_id;
	prtrade->get_value(0, trade_id);
	    
	/**
	 *	select first 20 rows
	 *		holding_history_id[] = HH_H_T_ID,
	 *		holding_history_trade_id[] = HH_T_ID,
	 *		quantity_before[] = HH_BEFORE_QTY,
	 *		quantity_after[] = HH_AFTER_QTY
	 *	from
	 *		HOLDING_HISTORY
	 *	where
	 *		HH_H_T_ID in
	 *				(select
	 *					HH_H_T_ID
	 *				from
	 *					HOLDING_HISTORY
	 *				where
	 *					HH_T_ID = trade_id
	 * 				)
	 */
	guard< index_scan_iter_impl<holding_history_t> > hh_iter;
	{
	    index_scan_iter_impl<holding_history_t>* tmp_hh_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:hh-iter-by-idx (%ld)\n",
		   xct_id, trade_id);
	    W_DO(_pholding_history_man->hh_get_iter_by_index2(_pssm, tmp_hh_iter,
							      prholdinghistory,
							      lowrep, highrep,
							      trade_id));
	    hh_iter = tmp_hh_iter;
	}
	
	TRACE( TRACE_TRX_FLOW, "App: %d TL:hh-iter-next \n", xct_id);
	W_DO(hh_iter->next(_pssm, eof, *prholdinghistory));
	int num_found;
	for(num_found = 0; num_found < 20 && !eof; num_found++) {
	    prholdinghistory->get_value(0, holding_history_id[num_found]);
	    prholdinghistory->get_value(1, holding_history_trade_id[num_found]);
	    prholdinghistory->get_value(2, quantity_before[num_found]);
	    prholdinghistory->get_value(3, quantity_after[num_found]);
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:hh-iter-next \n", xct_id);
	    W_DO(hh_iter->next(_pssm, eof, *prholdinghistory));
	}
	assert(num_found >= 0 && num_found <= 20); //Harness control
    } //END FRAME4
    
#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction
    // dumps the status of all the table rows used
    rtrade.print_tuple();
    rtradetype.print_tuple();
    rsettlement.print_tuple();
    rcashtrans.print_tuple();
    rtradehist.print_tuple();
    rholdinghistory.print_tuple();
#endif

    return RCOK;

}


EXIT_NAMESPACE(tpce);    
