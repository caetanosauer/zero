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

/** @file:   shore_tpce_xct_data_maintenance.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-E DATA MAINTENANCE transaction
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
 * TPC-E DATA MAINTENANCE
 *
 ********************************************************************/

w_rc_t ShoreTPCEEnv::xct_data_maintenance(const int xct_id,
					  data_maintenance_input_t& pdmin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    tuple_guard<account_permission_man_impl> pracctperm(_paccount_permission_man);
    tuple_guard<address_man_impl> praddress(_paddress_man);
    tuple_guard<company_man_impl> prcompany(_pcompany_man);
    tuple_guard<customer_man_impl> prcust(_pcustomer_man);
    tuple_guard<customer_taxrate_man_impl> prcusttaxrate(_pcustomer_taxrate_man);
    tuple_guard<daily_market_man_impl> prdailymarket(_pdaily_market_man);
    tuple_guard<exchange_man_impl> prexchange(_pexchange_man);
    tuple_guard<financial_man_impl> prfinancial(_pfinancial_man);
    tuple_guard<security_man_impl> prsecurity(_psecurity_man);
    tuple_guard<news_item_man_impl> prnewsitem(_pnews_item_man);
    tuple_guard<news_xref_man_impl> prnewsxref(_pnews_xref_man);
    tuple_guard<taxrate_man_impl> prtaxrate(_ptaxrate_man);
    tuple_guard<watch_item_man_impl> prwatchitem(_pwatch_item_man);
    tuple_guard<watch_list_man_impl> prwatchlist(_pwatch_list_man);

    rep_row_t areprow(_pnews_item_man->ts());

    areprow.set(_pnews_item_desc->maxsize());

    pracctperm->_rep = &areprow;
    praddress->_rep = &areprow;
    prcompany->_rep = &areprow;
    prcust->_rep = &areprow;
    prcusttaxrate->_rep = &areprow;
    prdailymarket->_rep = &areprow;
    prexchange->_rep = &areprow;
    prfinancial->_rep = &areprow;
    prsecurity->_rep = &areprow;
    prnewsitem->_rep = &areprow;
    prnewsxref->_rep = &areprow;
    prtaxrate->_rep = &areprow;
    prwatchitem->_rep = &areprow;
    prwatchlist->_rep = &areprow;

    rep_row_t lowrep( _pnews_item_man->ts());
    rep_row_t highrep( _pnews_item_man->ts());

    // allocate space for the biggest of the table representations
    lowrep.set(_pnews_item_desc->maxsize());
    highrep.set(_pnews_item_desc->maxsize());

    if(strcmp(pdmin._table_name, "ACCOUNT_PERMISSION") == 0){
	/**
	   select first 1 row
	   acl = AP_ACL
	   from
	   ACCOUNT_PERMISSION
	   where
	   AP_CA_ID = acct_id
	   order by
	   AP_ACL DESC
	*/
	guard<index_scan_iter_impl<account_permission_t> > ap_iter;
	{
	    index_scan_iter_impl<account_permission_t>* tmp_ap_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:ap-get-iter-by-idx %ld \n",
		   xct_id, pdmin._acct_id);
	    W_DO(_paccount_permission_man->ap_get_iter_by_index(_pssm,
								tmp_ap_iter,
								pracctperm,
								lowrep, highrep,
								pdmin._acct_id));
	    ap_iter = tmp_ap_iter;
	}
	//descending order
	rep_row_t sortrep(_pnews_item_man->ts());
	sortrep.set(_pnews_item_desc->maxsize());
	
	desc_sort_buffer_t ap_list(1);
	
	ap_list.setup(0, SQL_FIXCHAR, 4);
	
	table_row_t rsb(&ap_list);
	desc_sort_man_impl ap_sorter(&ap_list, &sortrep);
	
	bool eof;
	TRACE( TRACE_TRX_FLOW, "App: %d DM:ap-iter-next \n", xct_id);
	W_DO(ap_iter->next(_pssm, eof, *pracctperm));

	if(eof) {
	    TRACE( TRACE_TRX_FLOW,
		   "App: %d DM: no account permission tuple with acct_id (%d) \n",
		   xct_id, pdmin._acct_id);
	    W_DO(RC(se_NOT_FOUND));
	}
	
	while(!eof){
	    char acl[5]; //4
	    pracctperm->get_value(1, acl, 5);

	    rsb.set_value(0, acl);
	    ap_sorter.add_tuple(rsb);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:ap-iter-next \n", xct_id);
	    W_DO(ap_iter->next(_pssm, eof, *pracctperm));
	}
	
	desc_sort_iter_impl ap_list_sort_iter(_pssm, &ap_list, &ap_sorter);
	
	TRACE( TRACE_TRX_FLOW, "App: %d DM:ap-sort-iter-next \n", xct_id);
	W_DO(ap_list_sort_iter.next(_pssm, eof, rsb));

	char acl[5]; //4
	rsb.get_value(0, acl, 5);
	
	char new_acl[5]; //4
	if(strcmp(acl, "1111") != 0){
	    /**
	       update
	       ACCOUNT_PERMISSION
	       set
	       AP_ACL="0011"
	       where
	       AP_CA_ID = acct_id and
	       AP_ACL = acl
	    */
	    strcmp(new_acl, "1111");
	} else {
	    /**
	       update
	       ACCOUNT_PERMISSION
	       set
	       AP_ACL = ”0011”
	       where
	       AP_CA_ID = acct_id and
	       AP_ACL = acl
	    */
	    strcmp(new_acl, "0011");
	}
	{
	    index_scan_iter_impl<account_permission_t>* tmp_ap_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:ap-get-iter-by-idx %ld \n",
		   xct_id, pdmin._acct_id);
	    W_DO(_paccount_permission_man->ap_get_iter_by_index(_pssm,
								tmp_ap_iter,
								pracctperm,
								lowrep, highrep,
								pdmin._acct_id));
	    ap_iter = tmp_ap_iter;
	}
	
	TRACE( TRACE_TRX_FLOW, "App: %d DM:ap-iter-next \n", xct_id);
	W_DO(ap_iter->next(_pssm, eof, *pracctperm));
	while(!eof){
	    char ap_acl[5]; //4
	    pracctperm->get_value(1, ap_acl, 5);
	    
	    if(strcmp(ap_acl, acl) == 0){
		TRACE( TRACE_TRX_FLOW, "App: %d DM:ap-update (%s) \n",
		       xct_id, new_acl);
		W_DO(_paccount_permission_man->ap_update_acl(_pssm, pracctperm,
							     new_acl));
	    }
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:ap-iter-next \n", xct_id);
	    W_DO(ap_iter->next(_pssm, eof, *pracctperm));
	}	

    } else if(strcmp(pdmin._table_name, "ADDRESS") == 0) {
	char line2[81] = "\0"; //80
	TIdent ad_id = 0;
	
	if(pdmin._c_id != 0){
	    /**
	       select
	       line2 = AD_LINE2,
	       ad_id = AD_ID
	       from
	       ADDRESS, CUSTOMER
	       where
	       AD_ID = C_AD_ID and
	       C_ID = c_id
	    */	    
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:c-idx-probe (%ld) \n",
		   xct_id,  pdmin._c_id);
	    W_DO(_pcustomer_man->c_index_probe(_pssm, prcust, pdmin._c_id));
	    prcust->get_value(9, ad_id);	    
	} else {
	    /**
	       select
	       line2 = AD_LINE2,
	       ad_id = AD_ID
	       from
	       ADDRESS, COMPANY
	       where
	       AD_ID = CO_AD_ID and
	       CO_ID = co_id
	    */
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:co-idx-probe (%ld) \n",
		   xct_id,  pdmin._co_id);
	    W_DO(_pcompany_man->co_index_probe(_pssm, prcompany, pdmin._co_id));
	    prcompany->get_value(6, ad_id);
	}
	TRACE( TRACE_TRX_FLOW, "App: %d DM:ad-idx-probe-for-update (%ld) \n",
	       xct_id,  ad_id);
	W_DO(_paddress_man->ad_index_probe_forupdate(_pssm, praddress, ad_id));
	praddress->get_value(2, line2, 81);

	char new_line2[81];
	if(strcmp(line2, "Apt. 10C") != 0) {
	    /**
	       update
	       ADDRESS
	       set
	       AD_LINE2 = “Apt. 10C”
	       where
	       AD_ID = ad_id
	    */
	    strcpy(new_line2, "Apt. 10C");
	} else {
	    /**
	       update
	       ADDRESS
	       set
	       AD_LINE2 = “Apt. 22”
	       where
	       AD_ID = ad_id
	    */
	    strcpy(new_line2, "Apt. 22");
	}
	TRACE( TRACE_TRX_FLOW, "App: %d DM:ad-update (%ld) (%s) \n",
	       xct_id, ad_id, new_line2);
	W_DO(_paddress_man->ad_update_line2(_pssm, praddress, new_line2));

    } else if(strcmp(pdmin._table_name, "COMPANY") == 0) {
	char sprate[5] = "\0"; //4
	
	TRACE( TRACE_TRX_FLOW, "App: %d DM:co-idx-probe (%ld) \n",
	       xct_id,  pdmin._co_id);
	W_DO(_pcompany_man->co_index_probe_forupdate(_pssm, prcompany,
						     pdmin._co_id));
	prcompany->get_value(4, sprate, 5);
	
	char new_sprate[5];
	if(strcmp(sprate, "ABA") != 0){
	    /**
	       update
	       COMPANY
	       set
	       CO_SP_RATE = “ABA”
	       where
	       CO_ID = co_id
	    */
	    strcpy(new_sprate, "ABA");
	} else {
	    /**
	       update
	       COMPANY
	       set
	       CO_SP_RATE = “AAA”
	       where
	       CO_ID = co_id
	    */
	    strcpy(new_sprate, "AAA");
	}
	TRACE( TRACE_TRX_FLOW, "App: %d DM:co-update (%s \n", xct_id, new_sprate);
	W_DO(_pcompany_man->co_update_sprate(_pssm, prcompany, new_sprate));

    } else if(strcmp(pdmin._table_name, "CUSTOMER") == 0){
	char email2[51] = "\0"; //50
	int len = 0;
	int lenMindspring = strlen("@mindspring.com");
	
	/**
	   select
	   email2 = C_EMAIL_2
	   from
	   CUSTOMER
	   where
	   C_ID = c_id
	*/	
	TRACE( TRACE_TRX_FLOW, "App: %d DM:c-idx-probe (%d) \n",
	       xct_id,  pdmin._c_id);
	W_DO(_pcustomer_man->c_index_probe_forupdate(_pssm, prcust, pdmin._c_id));
	prcust->get_value(23, email2, 51);
	
	len = strlen(email2);
	string temp_email2(email2);
	char new_email2[51];
	if(((len - lenMindspring) > 0 &&
	    (temp_email2.substr(len-lenMindspring,
				lenMindspring).compare("@mindspring.com")==0))) {
	    /**
	       update
	       CUSTOMER
	       set
	       C_EMAIL_2 = substring(C_EMAIL_2, 1,
	       charindex(“@”,C_EMAIL_2) ) + „earthlink.com‟
	       where
	       C_ID = c_id
	    */
	    string temp_new_email2 = temp_email2.substr(1, temp_email2.find_first_of('@')) + "earthlink.com";
	    strcpy(new_email2, temp_new_email2.c_str());
	} else {
	    /**
	       update
	       CUSTOMER
	       set
	       C_EMAIL_2 = substring(C_EMAIL_2, 1,
	       charindex(“@”,C_EMAIL_2) ) + „mindspring.com‟
	       where
	       C_ID = c_id
	    */
	    string temp_new_email2 = temp_email2.substr(1, temp_email2.find_first_of('@')) + "mindspring.com";
	    strcpy(new_email2, temp_new_email2.c_str());
	}
	TRACE( TRACE_TRX_FLOW, "App: %d DM:c-update (%s) \n", xct_id, new_email2);
	W_DO(_pcustomer_man->c_update_email2(_pssm, prcust, new_email2));

    } else if(strcmp(pdmin._table_name, "CUSTOMER_TAXRATE") == 0){
	char old_tax_rate[5];//4
	char new_tax_rate[5];//4
	int tax_num;
	
	/**
	   select
	   old_tax_rate = CX_TX_ID
	   from
	   CUSTOMER_TAXRATE
	   where
	   CX_C_ID = c_id and (CX_TX_ID like “US%” or CX_TX_ID like “CN%”)
	*/
	guard< index_scan_iter_impl<customer_taxrate_t> > cx_iter;
	{
	    index_scan_iter_impl<customer_taxrate_t>* tmp_cx_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:cx-get-iter-by-idx (%ld) \n",
		   xct_id, pdmin._c_id);
	    W_DO(_pcustomer_taxrate_man->cx_get_iter_by_index(_pssm, tmp_cx_iter,
							      prcusttaxrate,
							      lowrep, highrep,
							      pdmin._c_id));
	    cx_iter = tmp_cx_iter;
	}
	
	bool eof;
	TRACE( TRACE_TRX_FLOW, "App: %d DM:cx-iter-next \n", xct_id);
	W_DO(cx_iter->next(_pssm, eof, *prcusttaxrate));
	while(!eof){
	    prcusttaxrate->get_value(0, old_tax_rate, 5);
	    
	    string temp_old_tax_rate(old_tax_rate);
	    if(temp_old_tax_rate.substr(0,2).compare("US") == 0 ||
	       temp_old_tax_rate.substr(0,2).compare("CN") == 0 ) {
		
		if(temp_old_tax_rate.substr(0,2).compare("US") == 0){
		    if(temp_old_tax_rate.compare("US5") == 0){
			strcpy(new_tax_rate, "US1");
		    } else {
			tax_num = atoi(temp_old_tax_rate.substr(2,1).c_str()) + 1;
			stringstream temp_new_tax_rate;
			temp_new_tax_rate << "US" << tax_num;
			strcpy(new_tax_rate, temp_new_tax_rate.str().c_str());
		    }
		} else {
		    if(temp_old_tax_rate.compare("CN4") == 0){
			strcpy(new_tax_rate, "CN1");
		    } else {
			tax_num = atoi(temp_old_tax_rate.substr(2,1).c_str()) + 1;
			stringstream temp_new_tax_rate;
			temp_new_tax_rate << "CN" << tax_num;
			strcpy(new_tax_rate, temp_new_tax_rate.str().c_str());
		    }
		}
		
		/**
		   update
		   CUSTOMER_TAXRATE
		   set
		   CX_TX_ID = new_tax_rate
		   where
		   CX_C_ID = c_id and
		   CX_TX_ID = old_tax_rate
		*/		
		TRACE( TRACE_TRX_FLOW, "App: %d DM:cx-update (%s) \n",
		       xct_id, new_tax_rate);
		W_DO(_pcustomer_taxrate_man->cx_update_txid(_pssm, prcusttaxrate,
							    new_tax_rate));
	    }
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:cx-iter-next \n", xct_id);
	    W_DO(cx_iter->next(_pssm, eof, *prcusttaxrate));
	}
	
    } else if(strcmp(pdmin._table_name, "DAILY_MARKET") == 0){
	/**
	   update
	   DAILY_MARKET
	   set
	   DM_VOL = DM_VOL + vol_incr
	   where
	   DM_S_SYMB = symbol
	   and substring ((convert(char(8),DM_DATE,3),1,2) = day_of_month
	*/
	guard< index_scan_iter_impl<daily_market_t> > dm_iter;
	{
	    index_scan_iter_impl<daily_market_t>* tmp_dm_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:dm-get-iter-by-idx4 (%s) \n",
		   xct_id, pdmin._symbol);
	    W_DO(_pdaily_market_man->dm_get_iter_by_index(_pssm, tmp_dm_iter,
							  prdailymarket, lowrep,
							  highrep, pdmin._symbol,
							  0, SH, false));
	    dm_iter = tmp_dm_iter;
	}
	
	bool eof;
	W_DO(dm_iter->next(_pssm, eof, *prdailymarket));
	while(!eof){
	    myTime dm_date;
	    prdailymarket->get_value(0, dm_date);
	    
	    if(dayOfMonth(dm_date) == pdmin._day_of_month){
		TRACE( TRACE_TRX_FLOW, "App: %d MD:dm-update (%d) \n",
		       xct_id, pdmin._vol_incr);
		W_DO(_pdaily_market_man->dm_update_vol(_pssm, prdailymarket,
						       pdmin._vol_incr));
	    }	    
	    W_DO(dm_iter->next(_pssm, eof, *prdailymarket));
	}
	    
    } else if(strcmp(pdmin._table_name, "EXCHANGE") == 0){
	int rowcount = 0;
	/**
	   select
	   rowcount = count(*)
	   from
	   EXCHANGE
	   where
	   EX_DESC like “%LAST UPDATED%”
	*/
	guard<table_scan_iter_impl<exchange_t> > ex_iter;
	{
	    table_scan_iter_impl<exchange_t>* tmp_ex_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:ex-get-table-iter \n", xct_id);
	    W_DO(_pexchange_man->get_iter_for_file_scan(_pssm, tmp_ex_iter));
	    ex_iter = tmp_ex_iter;
	}
	
	bool eof;
	W_DO(ex_iter->next(_pssm, eof, *prexchange));
	while(!eof){
	    char ex_desc[151]; //150
	    prexchange->get_value(5, ex_desc, 151);
	    
	    string temp_ex_desc(ex_desc);
	    if(temp_ex_desc.find("LAST UPDATED") != -1){
		rowcount++;
	    }
	    
	    W_DO(ex_iter->next(_pssm, eof, *prexchange));
	}
	
	if(rowcount == 0) {
	    /**
	       update
	       EXCHANGE
	       set
	       EX_DESC = EX_DESC + “ LAST UPDATED “ + getdatetime()
	    */
	    {
		table_scan_iter_impl<exchange_t>* tmp_ex_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d DM:ex-get-table-iter \n", xct_id);
		W_DO(_pexchange_man->get_iter_for_file_scan(_pssm, tmp_ex_iter));
		ex_iter = tmp_ex_iter;
	    }
	    W_DO(ex_iter->next(_pssm, eof, *prexchange));
	    while(!eof){
		char ex_desc[151]; //150
		prexchange->get_value(5, ex_desc, 151);
		
		string new_desc(ex_desc);
		stringstream ss;
		ss << "" << new_desc << " LAST UPDATED " << time(NULL) << "";
		
		TRACE( TRACE_TRX_FLOW, "App: %d MD:ex-update (%s) \n",
		       xct_id, ss.str().c_str());
		W_DO(_pexchange_man->ex_update_desc(_pssm, prexchange,
						    ss.str().c_str()));

		W_DO(ex_iter->next(_pssm, eof, *prexchange));
	    }	    
	} else {
	    /**
	       update
	       EXCHANGE
	       set
	       EX_DESC = substring(EX_DESC,1,
	       len(EX_DESC)-len(getdatetime())) + getdatetime()
	    */
	    table_scan_iter_impl<exchange_t>* tmp_ex_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:ex-get-table-iter \n", xct_id);
	    W_DO(_pexchange_man->get_iter_for_file_scan(_pssm, tmp_ex_iter));
	    ex_iter = tmp_ex_iter;
	    
	    W_DO(ex_iter->next(_pssm, eof, *prexchange));
	    while(!eof){
		ex_iter = tmp_ex_iter;
		char ex_desc[151]; //150
		prexchange->get_value(5, ex_desc, 151);
		
		string temp(ex_desc), new_desc;
		new_desc = temp.substr(0, temp.find_last_of(" ") + 1);
		stringstream ss;
		ss << "" << new_desc << time(NULL);
		
		TRACE( TRACE_TRX_FLOW, "App: %d MD:ex-update (%s) \n",
		       xct_id, ss.str().c_str());
		W_DO(_pexchange_man->ex_update_desc(_pssm, prexchange,
						    ss.str().c_str()));
		
		W_DO(ex_iter->next(_pssm, eof, *prexchange));
	    }
	}
	
    } else if(strcmp(pdmin._table_name, "FINANCIAL") == 0){
	int rowcount = 0;
	
	/**
	   select
	   rowcount = count(*)
	   from
	   FINANCIAL
	   where
	   FI_CO_ID = co_id and
	   substring(convert(char(8),
	   FI_QTR_START_DATE,2),7,2) = “01”
	*/
	guard< index_scan_iter_impl<financial_t> > fi_iter;
	{
	    index_scan_iter_impl<financial_t>* tmp_fi_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:fi-get-iter-by-idx (%ld) \n",
		   xct_id,  pdmin._co_id);
	    W_DO(_pfinancial_man->fi_get_iter_by_index(_pssm, tmp_fi_iter,
						       prfinancial, lowrep,
						       highrep, pdmin._co_id));
	    fi_iter = tmp_fi_iter;
	}
	
	bool eof;
	TRACE( TRACE_TRX_FLOW, "App: %d DM:fi-iter-next \n", xct_id);
	W_DO(fi_iter->next(_pssm, eof, *prfinancial));
	while(!eof){
	    myTime fi_qtr_start_date;
	    prfinancial->get_value(3, fi_qtr_start_date);
	    
	    if(dayOfMonth(fi_qtr_start_date) == 1){
		rowcount++;
	    }
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:fi-iter-next \n", xct_id);
	    W_DO(fi_iter->next(_pssm, eof, *prfinancial));
	}
	
	if(rowcount > 0){
	    /**
	       update
	       FINANCIAL
	       set
	       FI_QTR_START_DATE = FI_QTR_START_DATE + 1 day
	       where
	       FI_CO_ID = co_id
	    */
	    {
		index_scan_iter_impl<financial_t>* tmp_fi_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d DM:fi-get-iter-by-idx (%ld) \n",
		       xct_id, pdmin._co_id);
		W_DO(_pfinancial_man->fi_get_iter_by_index(_pssm, tmp_fi_iter,
							   prfinancial,
							   lowrep, highrep,
							   pdmin._co_id));
		fi_iter = tmp_fi_iter;
	    }
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:fi-iter-next \n", xct_id);
	    W_DO(fi_iter->next(_pssm, eof, *prfinancial));
	    while(!eof){
		myTime fi_qtr_start_date;
		prfinancial->get_value(3, fi_qtr_start_date);
		
		fi_qtr_start_date += (60*60*24); // add 1 day
		
		TRACE( TRACE_TRX_FLOW, "App: %d MD:fi-update (%ld) \n",
		       xct_id, fi_qtr_start_date);
		W_DO(_pfinancial_man->fi_update_desc(_pssm, prfinancial,
						     fi_qtr_start_date));

		TRACE( TRACE_TRX_FLOW, "App: %d DM:fi-iter-next \n", xct_id);
		W_DO(fi_iter->next(_pssm, eof, *prfinancial));
	    }
	} else {
	    /**
	       update
	       FINANCIAL
	       set
	       FI_QTR_START_DATE = FI_QTR_START_DATE – 1 day
	       where
	       FI_CO_ID = co_id
	    */
	    {
		index_scan_iter_impl<financial_t>* tmp_fi_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d DM:fi-get-iter-by-idx (%ld) \n",
		       xct_id,  pdmin._co_id);
		W_DO(_pfinancial_man->fi_get_iter_by_index(_pssm, tmp_fi_iter,
							   prfinancial,
							   lowrep, highrep,
							   pdmin._co_id));
		fi_iter = tmp_fi_iter;
	    }
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:fi-iter-next \n", xct_id);
	    W_DO(fi_iter->next(_pssm, eof, *prfinancial));
	    while(!eof){
		myTime fi_qtr_start_date;
		prfinancial->get_value(3, fi_qtr_start_date);
		
		fi_qtr_start_date -= (60*60*24);
		
		TRACE( TRACE_TRX_FLOW, "App: %d MD:fi-update-desc (%ld) \n",
		       xct_id, fi_qtr_start_date);
		W_DO(_pfinancial_man->fi_update_desc(_pssm, prfinancial,
						     fi_qtr_start_date));
		
		TRACE( TRACE_TRX_FLOW, "App: %d DM:fi-iter-next \n", xct_id);
		W_DO(fi_iter->next(_pssm, eof, *prfinancial));
	    }
	}
	
    } else if(strcmp(pdmin._table_name, "NEWS_ITEM") == 0){
	/**
	   update
	   NEWS_ITEM
	   set
	   NI_DTS = NI_DTS + 1day
	   where
	   NI_ID = (
	   select
	   NX_NI_ID
	   from
	   NEWS_XREF
	   where
	   NX_CO_ID = @co_id)
	*/
	guard< index_scan_iter_impl<news_xref_t> > nx_iter;
	{
	    index_scan_iter_impl<news_xref_t>* tmp_nx_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:nx-get-iter-by-idx (%ld) \n",
		   xct_id, pdmin._co_id);
	    W_DO(_pnews_xref_man->nx_get_iter_by_index(_pssm, tmp_nx_iter,
						       prnewsxref, lowrep,
						       highrep, pdmin._co_id));
	    nx_iter = tmp_nx_iter;
	}

	bool eof;
	W_DO(nx_iter->next(_pssm, eof, *prnewsxref));
	while(!eof){
	    TIdent nx_ni_id;
	    prnewsxref->get_value(0, nx_ni_id);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:nx-idx-probe (%d) \n",
		   xct_id, nx_ni_id);
	    W_DO(_pnews_item_man->ni_index_probe_forupdate(_pssm, prnewsitem,
							   nx_ni_id));
	    
	    myTime ni_dts;
	    prnewsitem->get_value(4, ni_dts);
	    
	    ni_dts += (60 * 60 * 24);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:nx-update-nidts (%d) \n",
		   xct_id, ni_dts);
	    W_DO(_pnews_item_man->ni_update_dts_by_index(_pssm, prnewsitem,
							 ni_dts));
	    
	    W_DO(nx_iter->next(_pssm, eof, *prnewsxref));
	}
	
    } else if(strcmp(pdmin._table_name, "SECURITY") == 0){
	/**
	   update
	   SECURITY
	   set
	   S_EXCH_DATE = S_EXCH_DATE + 1day
	   where
	   S_SYMB = symbol
	*/
	
	TRACE( TRACE_TRX_FLOW, "App: %d DM:s-idx-probe (%s) \n",
	       xct_id, pdmin._symbol);
	W_DO(_psecurity_man->s_index_probe_forupdate(_pssm, prsecurity,
						     pdmin._symbol));

	myTime s_exch_date;
	prsecurity->get_value(8, s_exch_date);
	
	s_exch_date += (60 * 60 * 24);
	
	TRACE( TRACE_TRX_FLOW, "App: %d DM:s-update-ed (%d) \n",
	       xct_id, s_exch_date);
	W_DO(_psecurity_man->s_update_ed(_pssm, prsecurity, s_exch_date));

    } else if(strcmp(pdmin._table_name, "TAXRATE") == 0){
	/**
	   select
	   tx_name = TX_NAME
	   from
	   TAXRATE
	   where
	   TX_ID = tx_id	
	*/
	
	char tx_name[51]; //50
	
	TRACE( TRACE_TRX_FLOW, "App: %d DM:tx-idx-probe (%d) \n",
	       xct_id, pdmin._tx_id);
	W_DO(_ptaxrate_man->tx_index_probe_forupdate(_pssm, prtaxrate,
						     pdmin._tx_id));

	prtaxrate->get_value(1, tx_name, 51);
	
	string temp(tx_name);
	size_t index = temp.find("Tax");
	if(index != string::npos){
	    temp.replace(index, 3, "tax");
	} else {
	    index = temp.find("tax");
	    temp.replace(index, 3, "Tax");
	}
	
	/**
	   update
	   TAXRATE
	   set
	   TX_NAME = tx_name
	   where
	   TX_ID = tx_id
	*/	
	TRACE( TRACE_TRX_FLOW, "App: %d DM:tx-update-name (%s) \n",
	       xct_id, tx_name);
	W_DO(_ptaxrate_man->tx_update_name(_pssm, prtaxrate, temp.c_str()));
	    
    } else if(strcmp(pdmin._table_name, "WATCH_ITEM") == 0){
	// PIN: TODO: this part can be optimized,
	// the same stuff is scanned bizillion times
	
	/**
	   select
	   cnt = count(*)     // number of rows is [50..150]
	   from
	   WATCH_ITEM,
	   WATCH_LIST
	   where
	   WL_C_ID = c_id and
	   WI_WL_ID = WL_ID
	*/	
	int cnt = 0;
	char old_symbol[16], new_symbol[16] = "\0"; //15, 15

	guard< index_scan_iter_impl<watch_list_t> > wl_iter;
	{
	    index_scan_iter_impl<watch_list_t>* tmp_wl_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:wl-get-iter-by-idx2 (%ld) \n",
		   xct_id, pdmin._c_id);
	    W_DO(_pwatch_list_man->wl_get_iter_by_index2(_pssm, tmp_wl_iter,
							 prwatchlist, lowrep,
							 highrep, pdmin._c_id));
	    wl_iter = tmp_wl_iter;
	}
	
	bool eof;
	W_DO(wl_iter->next(_pssm, eof, *prwatchlist));
	while(!eof){
	    TIdent wl_id;
	    prwatchlist->get_value(0, wl_id);
	    
	    guard< index_scan_iter_impl<watch_item_t> > wi_iter;
	    {
		index_scan_iter_impl<watch_item_t>* tmp_wi_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d DM:wi-get-iter-by-idx (%ld) \n",
		       xct_id, wl_id);
		W_DO(_pwatch_item_man->wi_get_iter_by_index(_pssm, tmp_wi_iter,
							    prwatchitem, lowrep,
							    highrep, wl_id));
		wi_iter = tmp_wi_iter;
	    }	    
	    W_DO(wi_iter->next(_pssm, eof, *prwatchitem));
	    while(!eof){
		cnt++;
		char wi_s_symbol[16]; //15
		prwatchitem->get_value(1, wi_s_symbol, 16);		
		W_DO(wi_iter->next(_pssm, eof, *prwatchitem));
	    }	    
	    W_DO(wl_iter->next(_pssm, eof, *prwatchlist));
	}	
	cnt = (cnt+1)/2;

	/**
	   select
	   old_symbol = WI_S_SYMB
	   from
	   ( select
	   ROWNUM,
	   WI_S_SYMB
	   from
	   WATCH_ITEM,
	   WATCH_LIST
	   where
	   WL_C_ID = c_id and
	   WI_WL_ID = WL_ID and
	   order by
	   WI_S_SYMB asc)
	   where
	   rownum = cnt
	*/
	//already sorted in ascending order because of its index

	index_scan_iter_impl<watch_list_t>* tmp_wl_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d DM:wl-get-iter-by-idx2 (%ld) \n",
	       xct_id, pdmin._c_id);
	W_DO(_pwatch_list_man->wl_get_iter_by_index2(_pssm, tmp_wl_iter,
						     prwatchlist, lowrep,
						     highrep, pdmin._c_id));
	wl_iter = tmp_wl_iter;
	
	W_DO(wl_iter->next(_pssm, eof, *prwatchlist));
	while(!eof && cnt > 0){
	    TIdent wl_id;
	    prwatchlist->get_value(0, wl_id);
	    
	    guard< index_scan_iter_impl<watch_item_t> > wi_iter;
	    {
		index_scan_iter_impl<watch_item_t>* tmp_wi_iter;
		TRACE( TRACE_TRX_FLOW, "App: %d DM:wi-get-iter-by-idx (%ld) \n",
		       xct_id, wl_id);
		W_DO(_pwatch_item_man->wi_get_iter_by_index(_pssm, tmp_wi_iter,
							    prwatchitem, lowrep,
							    highrep, wl_id));
		wi_iter = tmp_wi_iter;
	    }	    
	    W_DO(wi_iter->next(_pssm, eof, *prwatchitem));
	    while(!eof){
		cnt--;
		if(cnt == 0){
		    prwatchitem->get_value(1, old_symbol, 16);
		    break;
		}		
		W_DO(wi_iter->next(_pssm, eof, *prwatchitem));
	    }
	    W_DO(wl_iter->next(_pssm, eof, *prwatchlist));
	}
	
	/**
	   select first 1
	   new_symbol = S_SYMB
	   from
	   SECURITY
	   where
	   S_SYMB > old_symbol and
	   S_SYMB not in (
	   select
	   WI_S_SYMB
	   from
	   WATCH_ITEM,
	   WATCH_LIST
	   where
	   WL_C_ID = c_id and
	   WI_WL_ID = WL_ID)
	   order by
	   S_SYMB asc
	*/
	//already sorted in ascending order because of its index
	
	guard< index_scan_iter_impl<security_t> > s_iter;
	{
	    index_scan_iter_impl<security_t>* tmp_s_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:s-get-iter-by-idx (%s) \n",
		   xct_id, old_symbol);
	    W_DO(_psecurity_man->s_get_iter_by_index(_pssm, tmp_s_iter,
						     prsecurity, lowrep, highrep,
						     old_symbol));
	    s_iter = tmp_s_iter;
	}
	
	W_DO(s_iter->next(_pssm, eof, *prsecurity));
	while(!eof){
	    char s_symb[16]; //15
	    prsecurity->get_value(0, s_symb, 16);
	    
	    index_scan_iter_impl<watch_list_t>* tmp_wl_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d DM:wl-get-iter-by-idx2 (%ld) \n",
		   xct_id, pdmin._c_id);
	    W_DO(_pwatch_list_man->wl_get_iter_by_index2(_pssm, tmp_wl_iter,
							 prwatchlist, lowrep,
							 highrep, pdmin._c_id));
	    wl_iter = tmp_wl_iter;	    
	    W_DO(wl_iter->next(_pssm, eof, *prwatchlist));
	    while(!eof){
		TIdent wl_id;
		prwatchlist->get_value(0, wl_id);
		
		guard< index_scan_iter_impl<watch_item_t> > wi_iter;
		{
		    index_scan_iter_impl<watch_item_t>* tmp_wi_iter;
		    TRACE(TRACE_TRX_FLOW,
			  "App: %d DM:wi-get-iter-by-idx (%ld)\n", xct_id, wl_id);
		    W_DO(_pwatch_item_man->wi_get_iter_by_index(_pssm,
								tmp_wi_iter,
								prwatchitem,
								lowrep, highrep,
								wl_id));
		    wi_iter = tmp_wi_iter;
		}		
		W_DO(wi_iter->next(_pssm, eof, *prwatchitem));
		while(!eof){
		    char wi_s_symb[16]; //15
		    prwatchitem->get_value(1, wi_s_symb, 16);		    
		    if(strcmp(s_symb, wi_s_symb) != 0){
			strcpy(new_symbol, s_symb);
			break;
		    }		    
		    W_DO(wi_iter->next(_pssm, eof, *prwatchitem));
		}
		if(strcmp(new_symbol, "\0") != 0) {
		    break;
		}
		W_DO(wl_iter->next(_pssm, eof, *prwatchlist));
	    }
	    if(strcmp(new_symbol, "\0") != 0) {
		break;
	    }
	    W_DO(s_iter->next(_pssm, eof, *prsecurity));
	}
	
	/**
	   update
	   WATCH_ITEM
	   set
	   WI_S_SYMB = new_symbol
	   from
	   WATCH_LIST
	   where
	   WL_C_ID = c_id and
	   WI_WL_ID = WL_ID and
	   WI_S_SYMB = old_symbol
	*/
	TRACE( TRACE_TRX_FLOW, "App: %d DM:wl-get-iter-by-idx2 (%ld) \n",
	       xct_id, pdmin._c_id);
	W_DO(_pwatch_list_man->wl_get_iter_by_index2(_pssm, tmp_wl_iter,
						     prwatchlist, lowrep,
						     highrep, pdmin._c_id));
	wl_iter = tmp_wl_iter;
	
	W_DO(wl_iter->next(_pssm, eof, *prwatchlist));
	while(!eof && cnt > 0){
	    TIdent wl_id;
	    prwatchlist->get_value(0, wl_id);

	    TRACE( TRACE_TRX_FLOW, "App: %d DM:wi-update-symb (%ld) (%s) (%s) \n",
		   xct_id, wl_id, old_symbol, new_symbol);
	    W_DO(_pwatch_item_man->wi_update_symb(_pssm, prwatchitem, wl_id,
						  old_symbol, new_symbol));
	    W_DO(wl_iter->next(_pssm, eof, *prwatchlist));
	}
    }

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction
    // dumps the status of all the table rows used
    racctperm.print_tuple();
    raddress.print_tuple();
    rcompany.print_tuple();
    rcustomer.print_tuple();
    rcustomertaxrate.print_tuple();
    rdailymarket.print_tuple();
    rexchange.print_tuple();
    rfinancial.print_tuple();
    rsecurity.print_tuple();
    rnewsitem.print_tuple();
    rnewsxref.print_tuple();
    rtaxrate.print_tuple();
    rwatchitem.print_tuple();
    rwatchlist.print_tuple();
#endif

    return RCOK;

}


EXIT_NAMESPACE(tpce);    
