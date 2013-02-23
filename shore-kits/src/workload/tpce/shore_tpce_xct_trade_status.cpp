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

/** @file:   shore_tpce_xct_trade_status.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-E TRADE STATUS transaction
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
 * TPC-E TRADE_STATUS
 *
 ********************************************************************/

w_rc_t ShoreTPCEEnv::xct_trade_status(const int xct_id,
				      trade_status_input_t& ptsin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    tuple_guard<broker_man_impl> prbroker(_pbroker_man);
    tuple_guard<customer_man_impl> prcustomer(_pcustomer_man);
    tuple_guard<customer_account_man_impl> prcustacct(_pcustomer_account_man);
    tuple_guard<exchange_man_impl> prexchange(_pexchange_man);
    tuple_guard<security_man_impl> prsecurity(_psecurity_man);
    tuple_guard<status_type_man_impl> prstatustype(_pstatus_type_man);
    tuple_guard<trade_man_impl> prtrade(_ptrade_man);
    tuple_guard<trade_type_man_impl> prtradetype(_ptrade_type_man);

    rep_row_t areprow(_ptrade_man->ts());

    areprow.set(_ptrade_desc->maxsize());

    prbroker->_rep = &areprow;
    prcustomer->_rep = &areprow;
    prcustacct->_rep = &areprow;
    prexchange->_rep = &areprow;
    prsecurity->_rep = &areprow;
    prstatustype->_rep = &areprow;
    prtrade->_rep = &areprow;
    prtradetype->_rep = &areprow;

    rep_row_t lowrep( _pexchange_man->ts());
    rep_row_t highrep( _pexchange_man->ts());

    // allocate space for the biggest of the table representations
    lowrep.set(_pexchange_desc->maxsize());
    highrep.set(_pexchange_desc->maxsize());

    TIdent trade_id[50];
    myTime trade_dts[50];
    char status_name[50][11]; //10
    char type_name[50][13]; //12
    char symbol[50][16]; //15
    int trade_qty[50];
    char exec_name[50][50]; //49
    double charge[50];
    char s_name[50][71]; //70
    char ex_name[50][101]; //100
    
    /**
       select first 50 row
       trade_id[]    = T_ID,
       trade_dts[]   = T_DTS,
       status_name[] = ST_NAME,
       type_name[]   = TT_NAME,
       symbol[]      = T_S_SYMB,
       trade_qty[]   = T_QTY,
       exec_name[]   = T_EXEC_NAME,
       charge[]      = T_CHRG,
       s_name[]      = S_NAME,
       ex_name[]     = EX_NAME
       from
       TRADE,
       STATUS_TYPE,
       TRADE_TYPE,
       SECURITY,
       EXCHANGE
       where
       T_CA_ID = acct_id and
       ST_ID = T_ST_ID and
       TT_ID = T_TT_ID and
       S_SYMB = T_S_SYMB and
       EX_ID = S_EX_ID
       order by
       T_DTS desc
    */
    guard< index_scan_iter_impl<trade_t> > t_iter;
    int i = 0;
    bool eof;
    while(i !=max_trade_status_len) {
	i = 0;
	{
	    index_scan_iter_impl<trade_t>* tmp_t_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d TS:t-iter-by-idx2 (%ld) \n",
		   xct_id, ptsin._acct_id);
	    W_DO(_ptrade_man->t_get_iter_by_index2(_pssm, tmp_t_iter, prtrade,
						   lowrep, highrep,
						   ptsin._acct_id, 0, MAX_DTS,
						   true));
	    t_iter = tmp_t_iter;
	}
	TRACE( TRACE_TRX_FLOW, "App: %d TS:t-iter-next \n", xct_id);
	W_DO(t_iter->next(_pssm, eof, *prtrade));
	while(!eof && i < max_trade_status_len) {
	    prtrade->get_value(0, trade_id[i]);
	    prtrade->get_value(1, trade_dts[i]);
	    prtrade->get_value(5, symbol[i], 16);
	    prtrade->get_value(6, trade_qty[i]);
	    prtrade->get_value(9, exec_name[i], 50);
	    prtrade->get_value(11, charge[i]);
	    char t_st_id[5], t_tt_id[4]; //4, 3, 15	    
	    prtrade->get_value(2, t_st_id, 5);
	    prtrade->get_value(3, t_tt_id, 4);

	    TRACE( TRACE_TRX_FLOW, "App: %d TS:st-idx-probe (%s) \n",
		   xct_id, t_st_id);
	    W_DO(_pstatus_type_man->st_index_probe(_pssm, prstatustype, t_st_id));
	    prstatustype->get_value(1, status_name[i], 11);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TS:tt-idx-probe (%s) \n",
		   xct_id, t_tt_id);
	    W_DO(_ptrade_type_man->tt_index_probe(_pssm, prtradetype, t_tt_id));
	    prtradetype->get_value(1, type_name[i], 13);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TS:s-idx-probe (%s) \n",
		   xct_id, symbol[i]);
	    W_DO(_psecurity_man->s_index_probe(_pssm, prsecurity, symbol[i]));
	    prsecurity->get_value(3, s_name[i], 71);
	    char s_ex_id[7]; //6
	    prsecurity->get_value(4, s_ex_id, 7);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TS:ex-idx-probe (%s) \n",
		   xct_id, s_ex_id);
	    W_DO(_pexchange_man->ex_index_probe(_pssm, prexchange, s_ex_id));
	    prexchange->get_value(1, ex_name[i], 101);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d TS:t-iter-next \n", xct_id);
	    W_DO(t_iter->next(_pssm, eof, *prtrade));
	    i++;
	}
    }
    TRACE( TRACE_TRX_FLOW, "App: %d TS:count-after-sort %d\n", xct_id, i);
    assert(i == max_trade_status_len); // Harness control		
    
    /**
       select
       cust_l_name = C_L_NAME,
       cust_f_name = C_F_NAME,
       broker_name = B_NAME
       from
       CUSTOMER_ACCOUNT,
       CUSTOMER,
       BROKER
       where
       CA_ID = acct_id and
       C_ID = CA_C_ID and
       B_ID = CA_B_ID
    */    
    TRACE( TRACE_TRX_FLOW, "App: %d TS:ca-idx-probe (%ld) \n",
	   xct_id, ptsin._acct_id);
    W_DO(_pcustomer_account_man->ca_index_probe(_pssm,prcustacct,ptsin._acct_id));
    TIdent ca_c_id, ca_b_id;
    prcustacct->get_value(1, ca_b_id);
    prcustacct->get_value(2, ca_c_id);
    
    guard< index_scan_iter_impl<broker_t> > br_iter;
    {
	index_scan_iter_impl<broker_t>* tmp_br_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d BV:b-get-iter-by-idx3 (%s) \n",
	       xct_id, ca_b_id);
	W_DO(_pbroker_man->b_get_iter_by_index2(_pssm, tmp_br_iter, prbroker,
						lowrep, highrep, ca_b_id));
	br_iter = tmp_br_iter;
    }

    TRACE( TRACE_TRX_FLOW, "App: %d TO:br-iter-next \n", xct_id);
    W_DO(br_iter->next(_pssm, eof, *prbroker));
    if(eof) { W_DO(RC(se_NOT_FOUND)); }	
    char broker_name[50]; //49
    prbroker->get_value(2, broker_name, 50);
	
    TRACE( TRACE_TRX_FLOW, "App: %d TS:c-idx-probe (%ld) \n", xct_id, ca_c_id);
    W_DO(_pcustomer_man->c_index_probe(_pssm, prcustomer, ca_c_id));
    char cust_l_name[26]; //25
    char cust_f_name[21]; //20
    prcustomer->get_value(3, cust_l_name, 26);
    prcustomer->get_value(4, cust_f_name, 21);

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction
    // dumps the status of all the table rows used
    rbroker.print_tuple();
    rcustacct.print_tuple();
    rcustomer.print_tuple();
    rexchange.print_tuple();
    rsecurity.print_tuple();
    rstatustype.print_tuple();
    rtrade.print_tuple();
    rtradetype.print_tuple();
#endif

    return RCOK;

}


EXIT_NAMESPACE(tpce);    
