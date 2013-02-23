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

/** @file:   shore_tpce_xct_broker_volume.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-E BROKER VOLUME transaction
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
 * TPC-E BROKER VOLUME
 *
 ********************************************************************/

w_rc_t ShoreTPCEEnv::xct_broker_volume(const int xct_id,
				       broker_volume_input_t& pbvin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    tuple_guard<broker_man_impl> prbroker(_pbroker_man);
    tuple_guard<company_man_impl> prcompany(_pcompany_man);
    tuple_guard<industry_man_impl> prindustry(_pindustry_man);
    tuple_guard<sector_man_impl> prsector(_psector_man);
    tuple_guard<security_man_impl> prsecurity(_psecurity_man);
    tuple_guard<trade_request_man_impl> prtradereq(_ptrade_request_man);

    rep_row_t areprow(_pcompany_man->ts());

    areprow.set(_pcompany_desc->maxsize());

    prbroker->_rep = &areprow;
    prcompany->_rep = &areprow;
    prindustry->_rep = &areprow;
    prsector->_rep = &areprow;
    prsecurity->_rep = &areprow;
    prtradereq->_rep = &areprow;

    rep_row_t lowrep( _pcompany_man->ts());
    rep_row_t highrep( _pcompany_man->ts());

    // allocate space for the biggest of the table representations
    lowrep.set(_pcompany_desc->maxsize());
    highrep.set(_pcompany_desc->maxsize());

    /**
       select
       broker_name[] = B_NAME,
       volume[]      = sum(TR_QTY * TR_BID_PRICE)
       from
       TRADE_REQUEST,
       SECTOR,
       INDUSTRY
       COMPANY,
       BROKER,
       SECURITY
       where
       TR_B_ID = B_ID and
       TR_S_SYMB = S_SYMB and
       S_CO_ID = CO_ID and
       CO_IN_ID = IN_ID and
       SC_ID = IN_SC_ID and
       B_NAME in (broker_list) and
       SC_NAME = sector_name
       group by
       B_NAME
       order by
       2 DESC
    */
    
    //descending order
    rep_row_t sortrep(_pcompany_man->ts());
    sortrep.set(_pcompany_desc->maxsize());
    
    desc_sort_buffer_t tr_list(2);
    tr_list.setup(0, SQL_FLOAT);
    tr_list.setup(1, SQL_FIXCHAR, 50);
    
    table_row_t rsb(&tr_list);
    desc_sort_man_impl tr_sorter(&tr_list, &sortrep);
    
    guard< index_scan_iter_impl<sector_t> > sc_iter;
    {
	index_scan_iter_impl<sector_t>* tmp_sc_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d BV:sc-get-iter-by-idx2 (%s) \n",
	       xct_id, pbvin._sector_name);
	W_DO(_psector_man->sc_get_iter_by_index2(_pssm, tmp_sc_iter, prsector,
						 lowrep, highrep,
						 pbvin._sector_name));
	sc_iter = tmp_sc_iter;
    }
    
    bool eof;
    TRACE( TRACE_TRX_FLOW, "App: %d BV:sc-iter-next \n", xct_id);
    W_DO(sc_iter->next(_pssm, eof, *prsector));

    char sc_id[3];
    prsector->get_value(0, sc_id, 3);
    
    for(uint i = 0;
	i < max_broker_list_len && strcmp(pbvin._broker_list[i], "\0") != 0;
	i++){
	guard< index_scan_iter_impl<broker_t> > br_iter;
	{
	    index_scan_iter_impl<broker_t>* tmp_br_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d BV:b-get-iter-by-idx3 (%s) \n",
		   xct_id, pbvin._broker_list[i]);
	    W_DO(_pbroker_man->b_get_iter_by_index3(_pssm, tmp_br_iter, prbroker,
						    lowrep, highrep,
						    pbvin._broker_list[i]));
	    br_iter = tmp_br_iter;
	}
	TRACE( TRACE_TRX_FLOW, "App: %d BV:br-iter-next \n", xct_id);
	W_DO(br_iter->next(_pssm, eof, *prbroker));

	TIdent b_id;
	prbroker->get_value(0, b_id);
	
	double volume = 0;
	
	guard< index_scan_iter_impl<industry_t> > in_iter;
	{
	    index_scan_iter_impl<industry_t>* tmp_in_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d BV:in-get-iter-by-idx3 (%s) \n",
		   xct_id, sc_id);
	    W_DO(_pindustry_man->in_get_iter_by_index3(_pssm, tmp_in_iter,
						       prindustry,
						       lowrep, highrep, sc_id));
	    in_iter = tmp_in_iter;
	}

	TRACE( TRACE_TRX_FLOW, "App: %d BV:in-iter-next \n", xct_id);
	W_DO(in_iter->next(_pssm, eof, *prindustry));

	while(!eof){
	    char in_id[2];
	    prindustry->get_value(0, in_id, 2);
	    
	    guard< index_scan_iter_impl<company_t> > co_iter;
	    {
		index_scan_iter_impl<company_t>* tmp_co_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d BV:co-get-iter-by-idx3 (%s) \n",
		       xct_id, in_id);
		W_DO(_pcompany_man->co_get_iter_by_index3(_pssm, tmp_co_iter,
							  prcompany, lowrep,
							  highrep, in_id));
		co_iter = tmp_co_iter;
	    }
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d BV:co-iter-next \n", xct_id);
	    W_DO(co_iter->next(_pssm, eof, *prcompany));
		
	    TIdent co_id;
	    while(!eof){
		prcompany->get_value(0, co_id);
		
		guard< index_scan_iter_impl<security_t> > s_iter;
		{
		    index_scan_iter_impl<security_t>* tmp_s_iter;
		    TRACE( TRACE_TRX_FLOW,
			   "App: %d BV:s-get-iter-by-idx4 (%ld)\n",
			   xct_id, co_id);
		    W_DO(_psecurity_man->s_get_iter_by_index4(_pssm, tmp_s_iter,
							      prsecurity, lowrep,
							      highrep, co_id));
		    s_iter = tmp_s_iter;
		}
		
		TRACE( TRACE_TRX_FLOW, "App: %d BV:s-iter-next \n", xct_id);
		W_DO(s_iter->next(_pssm, eof, *prsecurity));

		while(!eof){				 
		    char s_symb[16]; //15
		    prsecurity->get_value(0, s_symb, 16);
		    
		    guard< index_scan_iter_impl<trade_request_t> > tr_iter;
		    {
			index_scan_iter_impl<trade_request_t>* tmp_tr_iter;
			TRACE( TRACE_TRX_FLOW,
			       "App: %d BV:tr-get-iter-by-idx4 (%s) (%ld)\n",
			       xct_id,  s_symb, b_id);
			W_DO(_ptrade_request_man->
			     tr_get_iter_by_index4(_pssm, tmp_tr_iter, prtradereq,
						   lowrep, highrep,
						   s_symb, b_id));
			tr_iter = tmp_tr_iter;
		    }
		    
		    TRACE( TRACE_TRX_FLOW, "App: %d BV:tr-iter-next \n", xct_id);
		    W_DO(tr_iter->next(_pssm, eof, *prtradereq));
		    
		    while(!eof){
			int tr_qty;
			double tr_bid_price;
			prtradereq->get_value(3, tr_qty);
			prtradereq->get_value(4, tr_bid_price);			
			volume += (tr_qty * tr_bid_price);			
			TRACE( TRACE_TRX_FLOW, "App:%d BV:tr-iter-next\n",xct_id);
			W_DO(tr_iter->next(_pssm, eof, *prtradereq));
		    }
		    TRACE( TRACE_TRX_FLOW, "App: %d TO:s-iter-next \n", xct_id);
		    W_DO(s_iter->next(_pssm, eof, *prsecurity));
		}
		TRACE( TRACE_TRX_FLOW, "App: %d BV:co-iter-next \n", xct_id);
		W_DO(co_iter->next(_pssm, eof, *prcompany));
	    }
	    TRACE( TRACE_TRX_FLOW, "App: %d BV:in-iter-next \n", xct_id);
	    W_DO(in_iter->next(_pssm, eof, *prindustry));
	}
	if(volume != 0){
	    rsb.set_value(0, volume);
	    rsb.set_value(1, pbvin._broker_list[i]);
	    tr_sorter.add_tuple(rsb);
	}
    }
    
    /* @note: PIN: 
     * "// row_count will frequently be zero near the start of a Test Run when
     *  // TRADE_REQUEST table is mostly empty."
     * The below quote us taken from TPC-E spec and it's very true.
     * This means we cannot run this harness control as it is at the
     * beginning no matter what.
     * We should either have a variable indicating turn_on_harness_control or
     * just run the the slightly changed version of this control (like below).
     */
    unsigned int list_len = tr_sorter.count();
    // assert(( list_len > 0) && (list_len <= max_broker_list_len)); // original
    assert (list_len <= max_broker_list_len); // modified harness control
    
    if(list_len > 0) {
	array_guard_t< char[50] > broker_name = new char[list_len][50]; //49
	array_guard_t<double> volume = new double[list_len];
	
	desc_sort_iter_impl tr_list_sort_iter(_pssm, &tr_list, &tr_sorter);
	bool eof;
	W_DO(tr_list_sort_iter.next(_pssm, eof, rsb));
	for(int j = 0; j < max_broker_list_len && !eof; j++){
	    rsb.get_value(0, volume[j]);
	    rsb.get_value(1, broker_name[j], 50);
	    W_DO(tr_list_sort_iter.next(_pssm, eof, rsb));
	}
    }
    
#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction
    // dumps the status of all the table rows used
    rbroker.print_tuple();
    rcompany.print_tuple();
    rindustry.print_tuple();
    rsector.print_tuple();
    rsecurity.print_tuple();
    rtradereq.print_tuple();
#endif

    return RCOK;

}


EXIT_NAMESPACE(tpce);    
