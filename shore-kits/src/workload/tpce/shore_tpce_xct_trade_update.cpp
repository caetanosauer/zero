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

/** @file:   shore_tpce_xct_trade_update.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-E TRADE UPDATE transaction
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
 * TPC-E TRADE_UPDATE
 *
 ********************************************************************/

w_rc_t ShoreTPCEEnv::xct_trade_update(const int xct_id, trade_update_input_t& ptuin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    tuple_guard<cash_transaction_man_impl> prcashtrans(_pcash_transaction_man);
    tuple_guard<security_man_impl> prsecurity(_psecurity_man);
    tuple_guard<settlement_man_impl> prsettlement(_psettlement_man);
    tuple_guard<trade_man_impl> prtrade(_ptrade_man);
    tuple_guard<trade_type_man_impl> prtradetype(_ptrade_type_man);
    tuple_guard<trade_history_man_impl> prtradehist(_ptrade_history_man);

    rep_row_t areprow(_psecurity_man->ts());

    areprow.set(_psecurity_desc->maxsize());

    prcashtrans->_rep = &areprow;
    prsecurity->_rep = &areprow;
    prsettlement->_rep = &areprow;
    prtrade->_rep = &areprow;
    prtradetype->_rep = &areprow;
    prtradehist->_rep = &areprow;

    rep_row_t lowrep( _psecurity_man->ts());
    rep_row_t highrep( _psecurity_man->ts());

    // allocate space for the biggest of the table representations
    lowrep.set(_psecurity_desc->maxsize());
    highrep.set(_psecurity_desc->maxsize());

    unsigned int max_trades = ptuin._max_trades;
    int num_found = 0;
    int num_updated = 0;
    
    if(ptuin._frame_to_execute == 1) {
	int i;

	array_guard_t<double> bid_price = new double[max_trades];
	array_guard_t< char[50] > exec_name = new char[max_trades][50]; //49
	array_guard_t<bool> is_cash = new bool[max_trades];
	array_guard_t<bool>	is_market = new bool[max_trades];
	array_guard_t<double> trade_price = new double[max_trades];
	array_guard_t<double> settlement_amount = new double[max_trades];
	array_guard_t<myTime> settlement_cash_due_date = new myTime[max_trades];
	array_guard_t< char[41] > settlement_cash_type = new char[max_trades][41];
	array_guard_t<double> cash_transaction_amount = new double[max_trades];
	array_guard_t<myTime> cash_transaction_dts = new myTime[max_trades];
	array_guard_t< char[101] > cash_transaction_name=new char[max_trades][101];
	array_guard_t< myTime[3] > trade_history_dts = new myTime[max_trades][3];
	array_guard_t< char[3][5] > trade_history_status_id = new char[max_trades][3][5];
	
	for(i = 0; i < max_trades; i++){
	    if(num_updated < ptuin._max_updates){
		/**
		   select
		   ex_name = T_EXEC_NAME
		   from
		   TRADE
		   where
		   T_ID = trade_id[i]
		   num_found = num_found + row_count
		*/
		TRACE( TRACE_TRX_FLOW, "App: %d TU:t-idx-probe-for-update (%ld)\n",
		       xct_id,  ptuin._trade_id[i]);
		W_DO(_ptrade_man->t_index_probe_forupdate(_pssm, prtrade,
							  ptuin._trade_id[i]));

		char ex_name[50]; //49
		prtrade->get_value(9, ex_name, 50);
		
		num_found++;
		
		string temp_exec_name(ex_name);
		size_t index = temp_exec_name.find(" X ");
		if(index != string::npos){
		    temp_exec_name.replace(index, 3, " ");
		} else {
		    index = temp_exec_name.find(" ");
		    temp_exec_name.replace(index, 1, " X ");
		}
		strcpy(ex_name, temp_exec_name.c_str());
		
		/**
		   update
		   TRADE
		   set
		   T_EXEC_NAME = ex_name
		   where
		   T_ID = trade_id[i]
		*/
		TRACE( TRACE_TRX_FLOW, "App: %d TU:t-update-exec-name (%s) \n",
		       xct_id, ex_name);
		W_DO(_ptrade_man->t_update_name(_pssm, prtrade, ex_name));

		num_updated++;
	    } else {
		/**
		   select
		   bid_price[i] = T_BID_PRICE,
		   exec_name[i] = T_EXEC_NAME,
		   is_cash[i]   = T_IS_CASH,
		   is_market[i] = TT_IS_MRKT,
		   trade_price[i] = T_TRADE_PRICE
		   from
		   TRADE,
		   TRADE_TYPE
		   where
		   T_ID = trade_id[i] and
		   T_TT_ID = TT_ID
		*/
		
		/*
		 * @note: PIN: this is a little hack
		 *             if prtrade is already filled due to above update
		 *             there is no need to do an index probe here
		 */		    
		TRACE( TRACE_TRX_FLOW, "TL: %d TU:t-idx-probe (%ld) \n",
		       xct_id, ptuin._trade_id[i]);
		W_DO(_ptrade_man->t_index_probe(_pssm,prtrade,ptuin._trade_id[i]));
	    }
	    
	    prtrade->get_value(4, is_cash[i]);
	    prtrade->get_value(7, bid_price[i]);
	    prtrade->get_value(9, exec_name[i], 50);
	    prtrade->get_value(10, trade_price[i]);
	    char tt_id[4]; //3
	    prtrade->get_value(3, tt_id, 4);
	    
	    TRACE(TRACE_TRX_FLOW, "App: %d TU:tt-idx-probe (%s)\n", xct_id, tt_id);
	    W_DO(_ptrade_type_man->tt_index_probe(_pssm, prtradetype, tt_id));
	    prtradetype->get_value(3, is_market[i]);
	    
	    /**
	       select
	       settlement_amount[i]        = SE_AMT,
	       settlement_cash_due_date[i] = SE_CASH_DUE_DATE,
	       settlement_cash_type[i]     = SE_CASH_TYPE
	       from
	       SETTLEMENT
	       where
	       SE_T_ID = trade_id[i]
	    */
	    TRACE( TRACE_TRX_FLOW, "App: %d TU:settlement-idx-probe (%ld) \n",
		   xct_id,  ptuin._trade_id[i]);
	    W_DO(_psettlement_man->se_index_probe(_pssm, prsettlement,
						  ptuin._trade_id[i]));
	    prsettlement->get_value(3, settlement_amount[i]);
	    prsettlement->get_value(2, settlement_cash_due_date[i]);
	    prsettlement->get_value(1, settlement_cash_type[i], 41);

	    if(is_cash[i]){
		/**
		   select
		   cash_transaction_amount[i] = CT_AMT,
		   cash_transaction_dts[i]    = CT_DTS,
		   cash_transaction_name[i]   = CT_NAME
		   from
		   CASH_TRANSACTION
		   where
		   CT_T_ID = trade_id[i]
		*/
		TRACE( TRACE_TRX_FLOW,
		       "App: %d TU:cash-transaction-idx-probe (%ld) \n",
		       xct_id, ptuin._trade_id[i]);
		W_DO(_pcash_transaction_man->ct_index_probe(_pssm, prcashtrans,
							    ptuin._trade_id[i]));
		prcashtrans->get_value(1, cash_transaction_dts[i]);
		prcashtrans->get_value(2, cash_transaction_amount[i]);
		prcashtrans->get_value(3, cash_transaction_name[i], 101);
	    }

	    /**
	       select first 3 rows
	       trade_history_dts[i][]       = TH_DTS,
	       trade_history_status_id[i][] = TH_ST_ID
	       from
	       TRADE_HISTORY
	       where
	       TH_T_ID = trade_id[i]
	       order by
	       TH_DTS
	    */
	    guard<index_scan_iter_impl<trade_history_t> > th_iter;
	    {
		index_scan_iter_impl<trade_history_t>* tmp_th_iter;
		TRACE(TRACE_TRX_FLOW, "App: %d TU:th-iter-by-trade-idx\n", xct_id);
		W_DO(_ptrade_history_man->th_get_iter_by_index(_pssm, tmp_th_iter,
							       prtradehist,
							       lowrep, highrep,
							       ptuin._trade_id[i]));
		th_iter = tmp_th_iter;
	    }
	    
	    //ascending order due to index
	    bool eof;
	    TRACE( TRACE_TRX_FLOW, "App: %d TU:th-iter-next \n", xct_id);
	    W_DO(th_iter->next(_pssm, eof, *prtradehist));
	    int j=0;
	    while (!eof && j<3) {
		prtradehist->get_value(1, trade_history_dts[i][j]);
		prtradehist->get_value(2, trade_history_status_id[i][j], 5);
		j++;
		TRACE( TRACE_TRX_FLOW, "App: %d TU:th-iter-next \n", xct_id);
		W_DO(th_iter->next(_pssm, eof, *prtradehist));
	    }
	}
	//Harness control
	assert(num_found == max_trades && num_updated == ptuin._max_updates);
    } else if(ptuin._frame_to_execute == 2){
	int i;
	
	array_guard_t<double> bid_price = new double[max_trades];
	array_guard_t< char[50] > exec_name = new char[max_trades][50]; //49
	array_guard_t<bool> is_cash = new bool[max_trades];
	array_guard_t<TIdent> trade_list = new TIdent[max_trades];
	array_guard_t<double> trade_price = new double[max_trades];
	array_guard_t<double> settlement_amount = new double[max_trades];
	array_guard_t<myTime> settlement_cash_due_date = new myTime[max_trades];
	array_guard_t< char[41] > settlement_cash_type = new char[max_trades][41];
	array_guard_t<double> cash_transaction_amount = new double[max_trades];
	array_guard_t<myTime> cash_transaction_dts = new myTime[max_trades];
	array_guard_t< char[101] > cash_transaction_name=new char[max_trades][101];
	array_guard_t< myTime[3] > trade_history_dts = new myTime[max_trades][3];
	array_guard_t< char[3][5] > trade_history_status_id = new char[max_trades][3][5]; //4
	
	/**
	   select first max_trades rows
	   bid_price[]   = T_BID_PRICE,
	   exec_name[]   = T_EXEC_NAME,
	   is_cash[]     = T_IS_CASH,
	   trade_list[]  = T_ID,
	   trade_price[] = T_TRADE_PRICE
	   from
	   TRADE
	   where
	   T_CA_ID = acct_id and
	   T_DTS >= start_trade_dts and
	   T_DTS <= end_trade_dts
	   order by
	   T_DTS asc
	*/
	guard<index_scan_iter_impl<trade_t> > t_iter;
	{
	    index_scan_iter_impl<trade_t>* tmp_t_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:t-iter-by-idx2 %ld %ld %ld \n",
		   xct_id, ptuin._acct_id, ptuin._start_trade_dts,
		   ptuin._end_trade_dts);
	    W_DO(_ptrade_man->t_get_iter_by_index2(_pssm, tmp_t_iter, prtrade,
						   lowrep, highrep, ptuin._acct_id,
						   ptuin._start_trade_dts,
						   ptuin._end_trade_dts));
	    t_iter = tmp_t_iter;
	}
	
	bool eof;
	TRACE( TRACE_TRX_FLOW, "App: %d TL:t-iter-next \n", xct_id);
	W_DO(t_iter->next(_pssm, eof, *prtrade));
	for(int j = 0; j < max_trades && !eof; j++){
	    prtrade->get_value(7, bid_price[j]);
	    prtrade->get_value(9, exec_name[j], 50); //49
	    prtrade->get_value(4, is_cash[j]);
	    prtrade->get_value(0, trade_list[j]);
	    prtrade->get_value(10, trade_price[j]);
	    
	    num_found++;
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TL:t-iter-next \n", xct_id);
	    W_DO(t_iter->next(_pssm, eof, *prtrade));
	}
	for(i = 0; i < num_found; i++) {
	    if(num_updated < ptuin._max_updates){
		/**
		   select
		   cash_type = SE_CASH_TYPE
		   from
		   SETTLEMENT
		   where
		   SE_T_ID = trade_list[i]
		*/
		TRACE(TRACE_TRX_FLOW, "App: %d TU:se-idx-probe-for-update (%ld)\n",
		      xct_id, trade_list[i]);
		W_DO(_psettlement_man->se_index_probe_forupdate(_pssm,
								prsettlement,
								trade_list[i]));
		char se_cash_type[41]; //40
		prsettlement->get_value(1, se_cash_type, 41);
		
		string temp_cash_type;
		if(is_cash[i]){
		    if(strcmp(se_cash_type, "Cash Account") == 0){
			temp_cash_type = "Cash";
		    } else {
			temp_cash_type = "Cash Account";
		    }
		} else {
		    if(strcmp(se_cash_type, "Margin Account") == 0){
			temp_cash_type = "Margin";
		    } else {
			temp_cash_type = "Margin Account";
		    }
		}
		
		/**
		   update
		   SETTLEMENT
		   set
		   SE_CASH_TYPE = cash_type
		   where
		   SE_T_ID = trade_list[i]
		*/
		TRACE( TRACE_TRX_FLOW, "App: %d TU:se-update-exec-name (%s) \n",
		       xct_id, temp_cash_type.c_str());
		W_DO(_psettlement_man->se_update_name(_pssm, prsettlement,
						      temp_cash_type.c_str()));

		num_updated++;
	    } else {
		/**
		   select
		   settlement_amount[i]        = SE_AMT,
		   settlement_cash_due_date[i] = SE_CASH_DUE_DATE,
		   settlement_cash_type[i]     = SE_CASH_TYPE
		   from
		   SETTLEMENT
		   where
		   SE_T_ID = trade_list[i]
		*/
		
		/*
		 * @note: PIN: this is a little hack
		 *            if prsettlement is already filled due to above update
		 *            there is no need to do an index probe here
		 */
		TRACE( TRACE_TRX_FLOW, "App: %d TU:se-idx-probe (%ld) \n",
		       xct_id,  trade_list[i]);
		W_DO(_psettlement_man->se_index_probe(_pssm, prsettlement,
						      trade_list[i]));
	    }	    
	    prsettlement->get_value(1, settlement_cash_type[i], 41);
	    prsettlement->get_value(2, settlement_cash_due_date[i]);
	    prsettlement->get_value(3, settlement_amount[i]);
	    
	    if(is_cash[i]){
		/**
		   select
		   cash_transaction_amount[i] = CT_AMT,
		   cash_transaction_dts[i]    = CT_DTS
		   cash_transaction_name[i]   = CT_NAME
		   from
		   CASH_TRANSACTION
		   where
		   CT_T_ID = trade_list[i]
		*/
		TRACE( TRACE_TRX_FLOW,
		       "App: %d TU:cash-transaction-idx-probe (%ld) \n",
		       xct_id, trade_list[i]);
		W_DO(_pcash_transaction_man->ct_index_probe(_pssm, prcashtrans,
							    trade_list[i]));
		prcashtrans->get_value(1, cash_transaction_dts[i]);
		prcashtrans->get_value(2, cash_transaction_amount[i]);
		prcashtrans->get_value(3, cash_transaction_name[i], 101);
	    }
	    
	    /**
	       select first 3 rows
	       trade_history_dts[i][]       = TH_DTS,
	       trade_history_status_id[i][] = TH_ST_ID
	       from
	       TRADE_HISTORY
	       where
	       TH_T_ID = trade_list[i]
	       order by
	       TH_DTS
	    */
	    guard<index_scan_iter_impl<trade_history_t> > th_iter;
	    {
		index_scan_iter_impl<trade_history_t>* tmp_th_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d TU:th-iter-by-trade-idx\n",xct_id);
		W_DO(_ptrade_history_man->th_get_iter_by_index(_pssm, tmp_th_iter,
							       prtradehist,
							       lowrep, highrep,
							       trade_list[i]));
		th_iter = tmp_th_iter;
	    }
	    
	    //ascending order due to index
	    bool eof;
	    TRACE( TRACE_TRX_FLOW, "App: %d TU:th-iter-next \n", xct_id);
	    W_DO(th_iter->next(_pssm, eof, *prtradehist));
	    int j=0;
	    while (!eof && j<3) {
		prtradehist->get_value(1, trade_history_dts[i][j]);
		prtradehist->get_value(2, trade_history_status_id[i][j], 5);
		j++;		    
		TRACE( TRACE_TRX_FLOW, "App: %d TU:th-iter-next \n", xct_id);
		W_DO(th_iter->next(_pssm, eof, *prtradehist));
	    }
	}
	assert(num_updated == num_found && num_updated >= 0 &&
	       num_found <= max_trades);  //Harness control
    } else if(ptuin._frame_to_execute == 3) {
	int i;
	    
	array_guard_t<TIdent> acct_id = new TIdent[max_trades];
	array_guard_t< char[50] > exec_name = new char[max_trades][50]; //49
	array_guard_t<bool> is_cash = new bool[max_trades];
	array_guard_t<double> price = new double[max_trades];
	array_guard_t<int> quantity = new int[max_trades];
	array_guard_t< char[71] > s_name = new char[max_trades][71]; //70
	array_guard_t<myTime> trade_dts = new myTime[max_trades];
	array_guard_t<TIdent> trade_list = new TIdent[max_trades];
	array_guard_t< char[4] > trade_type = new char[max_trades][4]; //3
	array_guard_t< char[13] > type_name = new char[max_trades][13]; //12
	array_guard_t<double> settlement_amount = new double[max_trades];
	array_guard_t<myTime> settlement_cash_due_date = new myTime[max_trades];
	array_guard_t< char[41] > settlement_cash_type = new char[max_trades][41];
	array_guard_t<double> cash_transaction_amount = new double[max_trades];
	array_guard_t<myTime> cash_transaction_dts = new myTime[max_trades];
	array_guard_t< char[101] > cash_transaction_name=new char[max_trades][101];
	array_guard_t< myTime[3] > trade_history_dts = new myTime[max_trades][3];
	array_guard_t< char[3][5] > trade_history_status_id = new char[max_trades][3][5]; //4
	
	/**
	   select first max_trades rows
	   acct_id[]    = T_CA_ID,
	   exec_name[]  = T_EXEC_NAME,
	   is_cash[]    = T_IS_CASH,
	   price[]      = T_TRADE_PRICE,
	   quantity[]   = T_QTY,
	   s_name[]     = S_NAME,
	   trade_dts[]  = T_DTS,
	   trade_list[] = T_ID,
	   trade_type[] = T_TT_ID,
	   type_name[]  = TT_NAME
	   from
	   TRADE,
	   TRADE_TYPE,
	   SECURITY
	   where
	   T_S_SYMB = symbol and
	   T_DTS >= start_trade_dts and
	   T_DTS <= end_trade_dts and
	   TT_ID = T_TT_ID and
	   S_SYMB = T_S_SYMB
	   order by
	   T_DTS asc	    
	*/
	guard<index_scan_iter_impl<trade_t> > t_iter;
	{
	    index_scan_iter_impl<trade_t>* tmp_t_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d TU:t-iter-by-idx3 %s %ld %ld \n",
		   xct_id, ptuin._symbol, ptuin._start_trade_dts,
		   ptuin._end_trade_dts);
	    W_DO(_ptrade_man->t_get_iter_by_index3(_pssm, tmp_t_iter, prtrade,
						   lowrep, highrep, ptuin._symbol,
						   ptuin._start_trade_dts,
						   ptuin._end_trade_dts)); 
	    t_iter = tmp_t_iter;
	}
	
	bool eof;
	TRACE( TRACE_TRX_FLOW, "App: %d TU:t-iter-next \n", xct_id);
	W_DO(t_iter->next(_pssm, eof, *prtrade));
	for(int j = 0; j < max_trades && !eof; j++){
	    prtrade->get_value(0, trade_list[j]);
	    prtrade->get_value(1, trade_dts[j]);
	    prtrade->get_value(3, trade_type[j], 4);
	    prtrade->get_value(4, is_cash[j]);
	    prtrade->get_value(6, quantity[j]);
	    prtrade->get_value(8, acct_id[j]);
	    prtrade->get_value(9, exec_name[j], 50);
	    prtrade->get_value(10, price[j]);
	    char t_s_symb[16]; //15
	    prtrade->get_value(5, t_s_symb, 16);

	    TRACE( TRACE_TRX_FLOW, "App: %d TU:s-idx-probe (%s) \n",
		   xct_id, t_s_symb);
	    W_DO(_psecurity_man->s_index_probe(_pssm, prsecurity, t_s_symb));
	    prsecurity->get_value(3, s_name[j], 71);

	    TRACE( TRACE_TRX_FLOW, "App: %d TU:tt-idx-probe (%s) \n",
		   xct_id, trade_type[j]);
	    W_DO(_ptrade_type_man->tt_index_probe(_pssm, prtradetype,
						  trade_type[j]));
	    prtradetype->get_value(1, type_name[j], 13);
	    
	    num_found++;
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TU:t-iter-next \n", xct_id);
	    W_DO(t_iter->next(_pssm, eof, *prtrade));
	}
	
	for(i = 0; i < num_found; i++) {
	    /**
	       select
	       settlement_amount[i]        = SE_AMT,
	       settlement_cash_due_date[i] = SE_CASH_DUE_DATE,
	       settlement_cash_type[i]     = SE_CASH_TYPE
	       from
	       SETTLEMENT
	       where
	       SE_T_ID = trade_list[i]
	    */
	    TRACE( TRACE_TRX_FLOW, "App: %d TU:se-idx-probe (%ld) \n",
		   xct_id, trade_list[i]);
	    W_DO(_psettlement_man->se_index_probe(_pssm, prsettlement,
						  trade_list[i]));
	    prsettlement->get_value(1, settlement_cash_type[i], 41);
	    prsettlement->get_value(2, settlement_cash_due_date[i]);
	    prsettlement->get_value(3, settlement_amount[i]);
	    
	    if(is_cash[i]){
		if(num_updated < ptuin._max_updates){
		    /**
		       select
		       ct_name = CT_NAME
		       from
		       CASH_TRANSACTION
		       where
		       CT_T_ID = trade_list[i]
		    */
		    TRACE(TRACE_TRX_FLOW,
			  "App:%dTU:cash-transaction-index-probe-forupdate(%ld)\n",
			  xct_id, trade_list[i]);
		    W_DO(_pcash_transaction_man->
			 ct_index_probe_forupdate(_pssm, prcashtrans,
						  trade_list[i]));
		    char ct_name[101]; //100
		    prcashtrans->get_value(3, ct_name, 101);
		    
		    string temp_ct_name(ct_name);
		    size_t index = temp_ct_name.find(" shares of ");
		    if(index != string::npos){
			stringstream ss;
			ss << type_name[i] <<  " " << quantity[i]
			   << " Shares of " << s_name[i];
			temp_ct_name = ss.str();
		    } else {
			stringstream ss;
			ss << type_name[i] <<  " " << quantity[i]
			   << " shares of " << s_name[i];
			temp_ct_name = ss.str();
		    }
		    strcpy(ct_name, temp_ct_name.c_str());
		    
		    /**
		       update
		       CASH_TRANSACTION
		       set
		       CT_NAME = ct_name
		       where
		       CT_T_ID = trade_list[i]		      
		    */
		    TRACE( TRACE_TRX_FLOW, "App: %d TU:ct-update-ct-name (%s) \n",
			   xct_id, ct_name);
		    W_DO(_pcash_transaction_man->ct_update_name(_pssm, prcashtrans,
								ct_name));
		    
		    num_updated++;
		} else {
		    /**
		       select
		       cash_transaction_amount[i] = CT_AMT,
		       cash_transaction_dts[i]    = CT_DTS
		       cash_transaction_name[i]   = CT_NAME
		       from
		       CASH_TRANSACTION
		       where
		       CT_T_ID = trade_list[i]	    
		    */
		    
		    /*
		     * @note: PIN: this is a little hack
		     *        if prsettlement is already filled due to above update
		     *        there is no need to do an index probe here
		     */
		    TRACE( TRACE_TRX_FLOW,
			   "App: %d TU:cash-transaction-idx-probe (%ld) \n",
			   xct_id, trade_list[i]);
		    W_DO(_pcash_transaction_man->ct_index_probe(_pssm, prcashtrans,
								trade_list[i]));
		}
		prcashtrans->get_value(1, cash_transaction_dts[i]);
		prcashtrans->get_value(2, cash_transaction_amount[i]);
		prcashtrans->get_value(3, cash_transaction_name[i], 101);
	    }
	    
	    /**
	       select first 3 rows
	       trade_history_dts[i][]       = TH_DTS,
	       trade_history_status_id[i][] = TH_ST_ID
	       from
	       TRADE_HISTORY
	       where
	       TH_T_ID = trade_list[i]
	       order by
	       TH_DTS asc		  
	    */
	    guard<index_scan_iter_impl<trade_history_t> > th_iter;
	    {
		index_scan_iter_impl<trade_history_t>* tmp_th_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d TU:th-iter-by-trade-idx %ld \n",
		       xct_id, trade_list[i]);
		W_DO(_ptrade_history_man->th_get_iter_by_index(_pssm, tmp_th_iter,
							       prtradehist,
							       lowrep, highrep,
							       trade_list[i]));
		th_iter = tmp_th_iter;
	    }
	    
	    //ascending order due to index
	    bool eof;
	    TRACE( TRACE_TRX_FLOW, "App: %d TU:th-iter-next \n", xct_id);
	    W_DO(th_iter->next(_pssm, eof, *prtradehist));
	    int j=0;
	    while (!eof && j<3) {
		prtradehist->get_value(1, trade_history_dts[i][j]);
		prtradehist->get_value(2, trade_history_status_id[i][j], 5);
		j++;
		TRACE( TRACE_TRX_FLOW, "App: %d TU:th-iter-next \n", xct_id);
		W_DO(th_iter->next(_pssm, eof, *prtradehist));
	    }
	}
    }		
    
#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction
    // dumps the status of all the table rows used
    rcashtrans.print_tuple();
    rsecurity.print_tuple();
    rsettlement.print_tuple();
    rtrade.print_tuple();
    rtradetype.print_tuple();
    rtradehist.print_tuple();
#endif

    return RCOK;

}


EXIT_NAMESPACE(tpce);    
