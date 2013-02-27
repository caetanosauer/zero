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

/** @file:   shore_tpce_xct_market_feed.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-E MARKET FEED transction
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
 * TPC-E MARKET FEED
 *
 ********************************************************************/


w_rc_t ShoreTPCEEnv::xct_market_feed(const int xct_id,
				     market_feed_input_t& pmfin)
{
    // check whether it has input
    if(pmfin._type_limit_buy[0] == '\0') {
	atomic_inc_uint_nv(&_num_invalid_input);
	return RCOK;
    }	

    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    tuple_guard<last_trade_man_impl> prlasttrade(_plast_trade_man);
    tuple_guard<trade_request_man_impl> prtradereq(_ptrade_request_man);
    tuple_guard<trade_man_impl> prtrade(_ptrade_man);
    tuple_guard<trade_history_man_impl> prtradehist(_ptrade_history_man);

    rep_row_t areprow(_ptrade_man->ts());

    areprow.set(_ptrade_desc->maxsize());

    prlasttrade->_rep = &areprow;
    prtradereq->_rep = &areprow;
    prtrade->_rep = &areprow;
    prtradehist->_rep = &areprow;

    rep_row_t lowrep( _ptrade_man->ts());
    rep_row_t highrep( _ptrade_man->ts());

    // allocate space for the biggest of the table representations
    lowrep.set(_ptrade_desc->maxsize());
    highrep.set(_ptrade_desc->maxsize());

    myTime	now_dts;
    double	req_price_quote;
    TIdent	req_trade_id;
    int		req_trade_qty;
    char	req_trade_type[4]; //3
    int		rows_updated;
    uint        num_deleted;

    now_dts = time(NULL);
    
    //the transaction should formally start here!
    for(rows_updated = 0; rows_updated < max_feed_len; rows_updated++) {
	num_deleted = 0;
	
	/**
	   update
	   LAST_TRADE
	   set
	   LT_PRICE = price_quote[i],
	   LT_VOL = LT_VOL + trade_qty[i],
	   LT_DTS = now_dts
	   where
	   LT_S_SYMB = symbol[i]
	*/
	TRACE( TRACE_TRX_FLOW, "App: %d MF:lt-update (%s) (%d) (%d) (%ld) \n",
	       xct_id, pmfin._symbol[rows_updated],
	       pmfin._price_quote[rows_updated],
	       pmfin._trade_qty[rows_updated], now_dts);
	W_DO(_plast_trade_man->
	     lt_update_by_index(_pssm, prlasttrade, pmfin._symbol[rows_updated],
				pmfin._price_quote[rows_updated],
				pmfin._trade_qty[rows_updated], now_dts));

	/**
	   select
	   TR_T_ID,
	   TR_BID_PRICE,
	   TR_TT_ID,
	   TR_QTY
	   from
	   TRADE_REQUEST
	   where
	   TR_S_SYMB = symbol[i] and (
	   (TR_TT_ID = type_stop_loss and
	   TR_BID_PRICE >= price_quote[i]) or
	   (TR_TT_ID = type_limit_sell and
	   TR_BID_PRICE <= price_quote[i]) or
	   (TR_TT_ID = type_limit_buy and
	   TR_BID_PRICE >= price_quote[i])
	   )
	*/
	guard< index_scan_iter_impl<trade_request_t> > tr_iter;
	{
	    index_scan_iter_impl<trade_request_t>* tmp_tr_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d MF:tr-get-iter-by-idx4 (%s) \n",
		   xct_id, pmfin._symbol[rows_updated]);
	    W_DO(_ptrade_request_man->
		 tr_get_iter_by_index4(_pssm, tmp_tr_iter, prtradereq, lowrep,
				       highrep, pmfin._symbol[rows_updated]));
	    tr_iter = tmp_tr_iter;
	}
	
	bool eof;
	TRACE( TRACE_TRX_FLOW, "App: %d MF:tr-iter-next \n", xct_id);
	w_rc_t e = tr_iter->next(_pssm, eof, *prtradereq);
	if (e.is_error()) {
	    if(e.err_num() == smlevel_0::eBADSLOTNUMBER) {
		eof = true;
	    } else {
		W_DO(e);
	    }
	}
	while(!eof) {
	    prtradereq->get_value(1, req_trade_type, 4);
	    prtradereq->get_value(4, req_price_quote);
	    
	    if((strcmp(req_trade_type, pmfin._type_stop_loss) == 0 &&
		(req_price_quote >= pmfin._price_quote[rows_updated])) ||
	       (strcmp(req_trade_type, pmfin._type_limit_sell) == 0 &&
		(req_price_quote <= pmfin._price_quote[rows_updated])) ||
	       (strcmp(req_trade_type, pmfin._type_limit_buy)== 0 &&
		(req_price_quote >= pmfin._price_quote[rows_updated]))
	       ) {
		prtradereq->get_value(0, req_trade_id);
		prtradereq->get_value(3, req_trade_qty);
				
		/**
		   delete
		   TRADE_REQUEST
		   where
		   current of request_list
		*/
		// PIN: get the RID and perform the delete after the scan
		//      to prevent crashes.
		/*
		  TRACE( TRACE_TRX_FLOW, "App: %d MF:tr-delete \n", xct_id);
		  W_DO(_ptrade_request_man->delete_tuple(_pssm, prtradereq));
		*/
		pmfin._trade_rid[num_deleted] = prtradereq->rid();
		num_deleted++;
		    
		/**
		   update
		   TRADE
		   set
		   T_DTS   = now_dts,
		   T_ST_ID = status_submitted
		   where
		   T_ID = req_trade_id
		*/
		TRACE( TRACE_TRX_FLOW, "App: %d MF:t-update (%ld) (%ld) (%s) \n",
		       xct_id, req_trade_id, now_dts, pmfin._status_submitted);
		W_DO(_ptrade_man->
		     t_update_dts_stdid_by_index(_pssm, prtrade,
						 req_trade_id, now_dts,
						 pmfin._status_submitted));
		    
		/**
		   insert into
		   TRADE_HISTORY
		   values (
		   TH_T_ID = req_trade_id,
		   TH_DTS = now_dts,
		   TH_ST_ID = status_submitted)
		*/
		prtradehist->set_value(0, req_trade_id);
		prtradehist->set_value(1, now_dts);
		prtradehist->set_value(2, pmfin._status_submitted);
		
		TRACE(TRACE_TRX_FLOW,"App: %d MF:th-add-tuple (%ld) (%ld) (%s)\n",
		      xct_id, req_trade_id, now_dts, pmfin._status_submitted);
		W_DO(_ptrade_history_man->add_tuple(_pssm, prtradehist));
	    } 
	    TRACE( TRACE_TRX_FLOW, "App: %d MF:tr-iter-next \n", xct_id);
	    e = tr_iter->next(_pssm, eof, *prtradereq);
	    if (e.is_error()) {
		if(e.err_num() == smlevel_0::eBADSLOTNUMBER) {
		    eof = true;
		} else {
		    W_DO(e);
		}
	    }
	    for(uint i=0; i<num_deleted; i++) {
		TRACE( TRACE_TRX_FLOW,
		       "%d - MF:tr-delete-tuple - (%d).(%d).(%d).(%d)\n", xct_id,
		       (pmfin._trade_rid[i]).pid.vol().vol,
		       (pmfin._trade_rid[i]).pid.store(),
		       (pmfin._trade_rid[i]).pid.page,
		       (pmfin._trade_rid[i]).slot);
		e = _ptrade_request_man->tr_delete_tuple(_pssm, prtradereq,
							 pmfin._trade_rid[i]);
		if (e.is_error() && e.err_num() != smlevel_0::eBADSLOTNUMBER) {
		    W_DO(e);
		}
	    }
	}
    }
    assert(rows_updated == max_feed_len); //Harness Control
    
    /* @note: PIN: the below is not executed because it ends up at an abstract
       virtual function
    // send triggered trades to the Market Exchange Emulator
    // via the SendToMarket interface.
    // This should be done after the related database changes have committed
    For (j=0; j<rows_sent; j++)
    {
    SendToMarketFromFrame(TradeRequestBuffer[i].symbol,
    TradeRequestBuffer[i].trade_id,
    TradeRequestBuffer[i].price_quote,
    TradeRequestBuffer[i].trade_qty,
    TradeRequestBuffer[i].trade_type);
    }
    */
    
#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction
    // dumps the status of all the table rows used
    rlasttrade.print_tuple();
    rtradereq.print_tuple();
    rtrade.print_tuple();
    rtradehist.print_tuple();
#endif

    return RCOK;
    
}


EXIT_NAMESPACE(tpce);    
