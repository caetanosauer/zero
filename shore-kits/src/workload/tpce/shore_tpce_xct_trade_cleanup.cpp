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

/** @file:   shore_tpce_xct_trade_cleanup.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-E TRADE CLEANUP transaction
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
 * TPC-E TRADE CLEANUP
 *
 ********************************************************************/

w_rc_t ShoreTPCEEnv::xct_trade_cleanup(const int xct_id,
				       trade_cleanup_input_t& ptcin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    tuple_guard<trade_man_impl> prtrade(_ptrade_man);
    tuple_guard<trade_history_man_impl> prtradehist(_ptrade_history_man);
    tuple_guard<trade_request_man_impl> prtradereq(_ptrade_request_man);

    rep_row_t areprow(_ptrade_man->ts());
    areprow.set(_ptrade_desc->maxsize());

    prtradereq->_rep = &areprow;
    prtrade->_rep = &areprow;
    prtradehist->_rep = &areprow;

    rep_row_t lowrep( _ptrade_man->ts());
    rep_row_t highrep( _ptrade_man->ts());

    // allocate space for the biggest of the table representations
    lowrep.set(_ptrade_desc->maxsize());
    highrep.set(_ptrade_desc->maxsize());

    TIdent	t_id;
    TIdent 	tr_t_id;
    myTime 	now_dts;
    
    /**
       select
       TR_T_ID
       from
       TRADE_REQUEST
       order by
       TR_T_ID
    */
    
    /* PIN: To get rid of the primary index TRADE_REQUEST,
       since TRADE_CLEANUP is not performance critical and
       since we have to touch all the TRADE_REQUEST tuples anyway here,
       I think this won't hurt
       Also I'm going to do the deleting of TRADE_REQUEST tuples here as well
    */
    
    guard<table_scan_iter_impl<trade_request_t> > tr_iter;
    {
	table_scan_iter_impl<trade_request_t>* tmp_tr_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d TC:tr-get-table-iter \n", xct_id);
	W_DO(_ptrade_request_man->get_iter_for_file_scan(_pssm, tmp_tr_iter));
	tr_iter = tmp_tr_iter;
    }
    
    //ascending order
    rep_row_t sortrep(_ptrade_man->ts());
    sortrep.set(_ptrade_desc->maxsize());
    
    asc_sort_buffer_t tr_list(1);
    
    tr_list.setup(0, SQL_LONG);
    
    table_row_t rsb(&tr_list);
    asc_sort_man_impl tr_sorter(&tr_list, &sortrep);
	
    bool eof;
    W_DO(tr_iter->next(_pssm, eof, *prtradereq));
    while(!eof){
	prtradereq->get_value(0, tr_t_id);
	
	rsb.set_value(0, tr_t_id);
	tr_sorter.add_tuple(rsb);
	
	TRACE( TRACE_TRX_FLOW, "App: %d TC:tr-delete- \n", xct_id);
	W_DO(_ptrade_request_man->delete_tuple(_pssm, prtradereq));
	    	    
	W_DO(tr_iter->next(_pssm, eof, *prtradereq));
    }
    
    asc_sort_iter_impl tr_list_sort_iter(_pssm, &tr_list, &tr_sorter);
    
    TRACE( TRACE_TRX_FLOW, "App: %d TC:tr-sort-iter-next \n", xct_id);
    W_DO(tr_list_sort_iter.next(_pssm, eof, rsb));
    while(!eof){
	rsb.get_value(0, tr_t_id);
	
	now_dts = time(NULL);
	
	/**
	   insert into
	   TRADE_HISTORY (
	   TH_T_ID, TH_DTS, TH_ST_ID
	   )
	   values (
	   tr_t_id,         // TH_T_ID
	   now_dts,         // TH_DTS
	   st_submitted_id  // TH_ST_ID
	   )
	*/
	prtradehist->set_value(0, tr_t_id);
	prtradehist->set_value(1, now_dts);
	prtradehist->set_value(2, ptcin._st_submitted_id);
	
	TRACE( TRACE_TRX_FLOW, "App: %d TC:th-add-tuple (%ld) (%ld) (%s) \n",
	       xct_id, tr_t_id, now_dts, ptcin._st_submitted_id);
	W_DO(_ptrade_history_man->add_tuple(_pssm, prtradehist));

	/**
	   update
	   TRADE
	   set
	   T_ST_ID = st_canceled_id,
	   T_DTS = now_dts
	   where
	   T_ID = tr_t_id
	*/	
	TRACE( TRACE_TRX_FLOW, "App: %d TC:t-update (%ld) (%ld) (%s) \n",
	       xct_id, tr_t_id, now_dts, ptcin._st_canceled_id);
	W_DO(_ptrade_man->t_update_dts_stdid_by_index(_pssm, prtrade, tr_t_id,
						      now_dts,
						      ptcin._st_canceled_id));
	
	/**
	   insert into
	   TRADE_HISTORY (
	   TH_T_ID, TH_DTS, TH_ST_ID
	   )
	   values (
	   tr_t_id,        // TH_T_ID
	   now_dts,        // TH_DTS
	   st_canceled_id  // TH_ST_ID
	   )
	*/
	prtradehist->set_value(0, tr_t_id);
	prtradehist->set_value(1, now_dts);
	prtradehist->set_value(2, ptcin._st_canceled_id);
	
	TRACE( TRACE_TRX_FLOW, "App: %d TC:th-add-tuple (%ld) (%ld) (%s) \n",
	       xct_id, tr_t_id, now_dts, ptcin._st_canceled_id);
	W_DO(_ptrade_history_man->add_tuple(_pssm, prtradehist));

	TRACE( TRACE_TRX_FLOW, "App: %d TC:tr-sort-iter-next \n", xct_id);
	W_DO(tr_list_sort_iter.next(_pssm, eof, rsb));
    }
    
    /** PIN: Read the above PIN
       delete
       from
       TRADE_REQUEST
    */
    
    /**
       select
       T_ID
       from
       TRADE
       where
       T_ID >= trade_id and
       T_ST_ID = st_submitted_id
    */
    guard<index_scan_iter_impl<trade_t> > t_iter;
    {
	index_scan_iter_impl<trade_t>* tmp_t_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d TC:t-iter-by-idx \n", xct_id);
	W_DO(_ptrade_man->t_get_iter_by_index(_pssm, tmp_t_iter, prtrade, lowrep,
					      highrep, ptcin._trade_id));
	t_iter = tmp_t_iter;
    }
    
    TRACE( TRACE_TRX_FLOW, "App: %d TC:t-iter-next \n", xct_id);
    W_DO(t_iter->next(_pssm, eof, *prtrade));
    while(!eof){
	char t_st_id[5]; //4
	prtrade->get_value(2, t_st_id, 5);
	
	if(strcmp(t_st_id, ptcin._st_submitted_id) == 0){
	    now_dts = time(NULL);
	    /** 
		update
		TRADE
		set
		T_ST_ID = st_canceled_id
		T_DTS = now_dts
		where
		T_ID = t_id
	    */
	    TRACE( TRACE_TRX_FLOW, "App: %d TC:t-update (%ld) (%ld) (%s) \n",
		   xct_id, t_id, now_dts, ptcin._st_canceled_id);
	    W_DO(_ptrade_man->t_update_dts_stdid_by_index(_pssm, prtrade, t_id,
							  now_dts,
							  ptcin._st_canceled_id));

	    /**
	       insert into
	       TRADE_HISTORY (
	       TH_T_ID, TH_DTS, TH_ST_ID
	       )
	       values (
	       t_id,           // TH_T_ID
	       now_dts,        // TH_DTS
	       st_canceled_id  // TH_ST_ID
	       )
	    */	    
	    prtradehist->set_value(0, t_id);
	    prtradehist->set_value(1, now_dts);
	    prtradehist->set_value(2, ptcin._st_canceled_id);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TC:th-add-tuple (%ld) (%ld) (%s) \n",
		   xct_id, t_id, now_dts, ptcin._st_canceled_id);
	    W_DO(_ptrade_history_man->add_tuple(_pssm, prtradehist));
	}
	TRACE( TRACE_TRX_FLOW, "App: %d TC:t-iter-next \n", xct_id);
	W_DO(t_iter->next(_pssm, eof, *prtrade));
    }

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction
    // dumps the status of all the table rows used
    rtradereq.print_tuple();
    rtrade.print_tuple();
    rtradehist.print_tuple();
#endif

    return RCOK;

}


EXIT_NAMESPACE(tpce);    
