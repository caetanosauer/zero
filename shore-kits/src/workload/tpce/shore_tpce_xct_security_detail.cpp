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

/** @file:  shore_tpce_xct_security_detail.cpp
 *
 *  @brief: Implementation of the Baseline Shore TPC-E SECURITY DETAIL transaction
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
 * TPC-E SECURITY DETAIL
 *
 ********************************************************************/

w_rc_t ShoreTPCEEnv::xct_security_detail(const int xct_id,
					 security_detail_input_t& psdin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    tuple_guard<address_man_impl> praddress(_paddress_man);
    tuple_guard<company_man_impl> prcompany(_pcompany_man);
    tuple_guard<company_competitor_man_impl> prcompanycomp(_pcompany_competitor_man);
    tuple_guard<daily_market_man_impl> prdailymarket(_pdaily_market_man);
    tuple_guard<exchange_man_impl> prexchange(_pexchange_man);
    tuple_guard<financial_man_impl> prfinancial(_pfinancial_man);
    tuple_guard<industry_man_impl> prindustry(_pindustry_man);
    tuple_guard<last_trade_man_impl> prlasttrade(_plast_trade_man);
    tuple_guard<news_item_man_impl> prnewsitem(_pnews_item_man);
    tuple_guard<news_xref_man_impl> prnewsxref(_pnews_xref_man);
    tuple_guard<security_man_impl> prsecurity(_psecurity_man);
    tuple_guard<zip_code_man_impl> przipcode(_pzip_code_man);

    rep_row_t areprow(_pnews_item_man->ts());
    areprow.set(_pnews_item_desc->maxsize());

    praddress->_rep = &areprow;
    prcompany->_rep = &areprow;
    prcompanycomp->_rep = &areprow;
    prdailymarket->_rep = &areprow;
    prexchange->_rep = &areprow;
    prfinancial->_rep = &areprow;
    prindustry->_rep = &areprow;
    prlasttrade->_rep = &areprow;
    prnewsitem->_rep = &areprow;
    prnewsxref->_rep = &areprow;
    prsecurity->_rep = &areprow;
    przipcode->_rep = &areprow;

    rep_row_t lowrep( _pnews_item_man->ts());
    rep_row_t highrep( _pnews_item_man->ts());

    // allocate space for the biggest of the table representations
    lowrep.set(_pnews_item_desc->maxsize());
    highrep.set(_pnews_item_desc->maxsize());

    /**
       select
       s_name          = S_NAME,
       co_id           = CO_ID,
       co_name         = CO_NAME,
       sp_rate         = CO_SP_RATE
       ceo_name        = CO_CEO,
       co_desc         = CO_DESC,
       open_date       = CO_OPEN_DATE,
       co_st_id        = CO_ST_ID,
       co_ad_line1     = CA.AD_LINE1,
       co_ad_line2     = CA.AD_LINE2,
       co_ad_town      = ZCA.ZC_TOWN,
       co_ad_div       = ZCA.ZC_DIV,
       co_ad_zip       = CA.AD_ZC_CODE,
       co_ad_ctry      = CA.AD_CTRY,
       num_out         = S_NUM_OUT,
       start_date      = S_START_DATE,
       exch_date       = S_EXCH_DATE,
       pe_ratio        = S_PE,
       52_wk_high      = S_52WK_HIGH,
       52_wk_high_date = S_52WK_HIGH_DATE,
       52_wk_low       = S_52WK_LOW,
       52_wk_low_date  = S_52WK_LOW_DATE,
       divid           = S_DIVIDEND,
       yield           = S_YIELD,
       ex_ad_div       = ZEA.ZC_DIV,
       ex_ad_ctry      = EA.AD_CTRY
       ex_ad_line1     = EA.AD_LINE1,
       ex_ad_line2     = EA.AD_LINE2,
       ex_ad_town      = ZEA.ZC_TOWN,
       ex_ad_zip       = EA.AD_ZC_CODE,
       ex_close        = EX_CLOSE,
       ex_desc         = EX_DESC,
       ex_name         = EX_NAME,
       ex_num_symb     = EX_NUM_SYMB,
       ex_open         = EX_OPEN
       from
       SECURITY,
       COMPANY,
       ADDRESS CA,
       ADDRESS EA,
       ZIP_CODE ZCA,
       ZIP_CODE ZEA,
       EXCHANGE
       where
       S_SYMB = symbol and
       CO_ID = S_CO_ID and
       CA.AD_ID = CO_AD_ID and
       EA.AD_ID = EX_AD_ID and
       EX_ID = S_EX_ID and
       ca.ad_zc_code = zca.zc_code and
       ea.ad_zc_code =zea.zc_code
    */

    char s_name[71]; //70
    TIdent co_id;
    char co_name[61]; //60
    char sp_rate[5];  //4
    char ceo_name[47]; //46
    char co_desc[151]; //150
    myTime open_date;
    char co_st_id[5]; //4
    char co_ad_line1[81]; //80
    char co_ad_line2[81]; //80
    char co_ad_town[81]; //80
    char co_ad_div[81]; //80
    char co_ad_zip[13]; //12
    char co_ad_ctry[81]; //80
    double num_out;
    myTime start_date;
    myTime exch_date;
    double pe_ratio;
    double S52_wk_high;
    myTime S52_wk_high_date;
    double S52_wk_low;
    myTime S52_wk_low_date;
    double divid;
    double yield;
    char ex_ad_div[81]; //80
    char ex_ad_ctry[81]; //80
    char ex_ad_line1[81]; //80
    char ex_ad_line2[81]; //80
    char ex_ad_town[81]; //80
    char ex_ad_zip[13]; //12
    int ex_close;
    char ex_desc[151]; //151
    char ex_name[101]; //100
    int ex_num_symb;
    int ex_open;	
    TIdent s_co_id;
    char s_ex_id[7]; //6

    TRACE( TRACE_TRX_FLOW, "App:%d SD:s-idx-probe (%s)\n", xct_id, psdin._symbol);
    W_DO(_psecurity_man->s_index_probe(_pssm, prsecurity, psdin._symbol));

    prsecurity->get_value(3, s_name, 71);
    prsecurity->get_value(4, s_ex_id, 7);
    prsecurity->get_value(5, s_co_id);
    prsecurity->get_value(6, num_out);
    prsecurity->get_value(7, start_date);
    prsecurity->get_value(8, exch_date);
    prsecurity->get_value(9, pe_ratio);
    prsecurity->get_value(10, S52_wk_high);
    prsecurity->get_value(11, S52_wk_high_date);
    prsecurity->get_value(12, S52_wk_low);
    prsecurity->get_value(13, S52_wk_low_date);
    prsecurity->get_value(14, divid);
    prsecurity->get_value(15, yield);
    
    TRACE( TRACE_TRX_FLOW, "App: %d SD:co-idx-probe (%ld) \n", xct_id,  s_co_id);
    W_DO(_pcompany_man->co_index_probe(_pssm, prcompany, s_co_id));

    prcompany->get_value(0, co_id);
    prcompany->get_value(1, co_st_id, 5);
    prcompany->get_value(2, co_name, 61);
    prcompany->get_value(4, sp_rate, 5);
    prcompany->get_value(5, ceo_name, 47);
    TIdent co_ad_id;
    prcompany->get_value(6, co_ad_id);
    prcompany->get_value(7, co_desc, 151);
    prcompany->get_value(8, open_date);
	
    //get company address and zip code    
    TRACE( TRACE_TRX_FLOW, "App: %d SD:ad-idx-probe (%ld) \n", xct_id, co_ad_id);
    W_DO(_paddress_man->ad_index_probe(_pssm, praddress, co_ad_id));

    praddress->get_value(1, co_ad_line1, 81);
    praddress->get_value(2, co_ad_line2, 81);    
    char ca_ad_zc_code[13]; //12
    praddress->get_value(3, ca_ad_zc_code, 13);

    TRACE(TRACE_TRX_FLOW, "App:%d SD:zc-idx-probe (%s)\n", xct_id, ca_ad_zc_code);
    W_DO(_pzip_code_man->zc_index_probe(_pssm, przipcode, ca_ad_zc_code));

    przipcode->get_value(1, co_ad_town, 81);
    przipcode->get_value(2, co_ad_div, 81);
    
    TRACE( TRACE_TRX_FLOW, "App: %d SD:ex-idx-probe (%s) \n", xct_id, s_ex_id);
    W_DO(_pexchange_man->ex_index_probe(_pssm, prexchange, s_ex_id));
	
    prexchange->get_value(1, ex_name, 101);
    prexchange->get_value(2, ex_num_symb);
    prexchange->get_value(3, ex_open);
    prexchange->get_value(4, ex_close);
    prexchange->get_value(5, ex_desc, 151);    
    TIdent ex_ad_id;
    prexchange->get_value(6, ex_ad_id);
	
    //get exchange address and zip code
    TRACE( TRACE_TRX_FLOW, "App: %d SD:ad-idx-probe (%ld) \n", xct_id, ex_ad_id);
    W_DO(_paddress_man->ad_index_probe(_pssm, praddress, ex_ad_id));

    praddress->get_value(1, ex_ad_line1, 81);
    praddress->get_value(2, ex_ad_line2, 81);
    praddress->get_value(3, ex_ad_zip, 13);
    praddress->get_value(4, ex_ad_ctry, 81);
    
    TRACE( TRACE_TRX_FLOW, "App: %d SD:zc-idx-probe (%s) \n", xct_id, ex_ad_zip);
    W_DO(_pzip_code_man->zc_index_probe(_pssm, przipcode, ex_ad_zip));

    przipcode->get_value(1, ex_ad_town, 81);
    przipcode->get_value(2, ex_ad_div, 81);
    
    /**
       select first max_comp_len rows
       cp_co_name[] = CO_NAME,
       cp_in_name[] = IN_NAME
       from
       COMPANY_COMPETITOR, COMPANY, INDUSTRY
       where
       CP_CO_ID = co_id and
       CO_ID = CP_COMP_CO_ID and
       IN_ID = CP_IN_ID
    */
    char cp_co_name[3][61]; //60
    char in_name[3][51]; //50

    guard< index_scan_iter_impl<company_competitor_t> > cc_iter;
    {
	index_scan_iter_impl<company_competitor_t>* tmp_cc_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d SD:cc-get-iter-by-index (%ld) \n",
	       xct_id,  co_id);
	W_DO(_pcompany_competitor_man->cc_get_iter_by_index(_pssm, tmp_cc_iter,
							    prcompanycomp, lowrep,
							    highrep, co_id));
	cc_iter = tmp_cc_iter;
    }
    bool eof;
    TRACE( TRACE_TRX_FLOW, "App: %d SD:cc-iter-next \n", xct_id);
    W_DO(cc_iter->next(_pssm, eof, *prcompanycomp));
    for(int i = 0; i < 3 && !eof; i++){
	TIdent cp_comp_co_id;
	char cp_in_id[3]; //2
	
	prcompanycomp->get_value(1, cp_comp_co_id);
	prcompanycomp->get_value(2, cp_in_id, 3);
	
	TRACE( TRACE_TRX_FLOW, "App:%d SD:in-idx-probe (%s)\n", xct_id, cp_in_id);
	W_DO(_pindustry_man->in_index_probe(_pssm, prindustry, cp_in_id));

	prindustry->get_value(1, in_name[i], 51);
	
	TRACE( TRACE_TRX_FLOW, "App: %d SD:co-idx-probe (%ld) \n",
	       xct_id, cp_comp_co_id);
	W_DO(_pcompany_man->co_index_probe(_pssm, prcompany, cp_comp_co_id));

	prcompany->get_value(2, cp_co_name[i], 61);

	TRACE( TRACE_TRX_FLOW, "App: %d SD:cc-iter-next \n", xct_id);
	W_DO(cc_iter->next(_pssm, eof, *prcompanycomp));
    }
    
    /**
       select first max_fin_len rows
       fin[].year        = FI_YEAR,
       fin[].qtr         = FI_QTR,
       fin[].strart_date = FI_QTR_START_DATE,
       fin[].rev         = FI_REVENUE,
       fin[].net_earn    = FI_NET_EARN,
       fin[].basic_eps   = FI_BASIC_EPS,
       fin[].dilut_eps   = FI_DILUT_EPS,
       fin[].margin      = FI_MARGIN,
       fin[].invent      = FI_INVENTORY,
       fin[].assets      = FI_ASSETS,
       fin[].liab        = FI_LIABILITY,
       fin[].out_basic   = FI_OUT_BASIC,
       fin[].out_dilut   = FI_OUT_DILUT
       from
       FINANCIAL
       where
       FI_CO_ID = co_id
       order by
       FI_YEAR asc,
       FI_QTR
    */
    int fin_year[20];
    int fin_qtr[20];
    myTime fin_start_date[20];
    double fin_rev[20];
    double fin_net_earn[20];
    double fin_basic_eps[20];
    double fin_dilut_eps[20];
    double fin_margin[20];
    double fin_invent[20];
    double fin_assets[20];
    double fin_liab[20];
    double fin_out_basic[20];
    double fin_out_dilut[20];
	
    guard< index_scan_iter_impl<financial_t> > fi_iter;
    {
	index_scan_iter_impl<financial_t>* tmp_fi_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d SD:fi-get-iter-by-idx (%ld) \n",
	       xct_id, co_id);
	W_DO(_pfinancial_man->fi_get_iter_by_index(_pssm, tmp_fi_iter,
						   prfinancial, lowrep, highrep,
						   co_id));
	fi_iter = tmp_fi_iter;
    }

    //already sorted because of the index
    TRACE( TRACE_TRX_FLOW, "App: %d SD:fi-iter-next \n", xct_id);
    W_DO(fi_iter->next(_pssm, eof, *prfinancial));
    int fin_len;
    for(fin_len = 0; fin_len < 20 && !eof; fin_len++){
	int fi_year;
	prfinancial->get_value(1, fi_year);
	fin_year[fin_len] = fi_year;
	
	short fi_qtr;
	prfinancial->get_value(2, fi_qtr);
	fin_qtr[fin_len] = fi_qtr;
	
	myTime fi_qtr_start_date;
	prfinancial->get_value(3, fi_qtr_start_date);
	fin_start_date[fin_len] = fi_qtr_start_date;
	
	double fi_revenue;
	prfinancial->get_value(4, fi_revenue);
	fin_rev[fin_len] = fi_revenue;
	
	double fi_net_earn;
	prfinancial->get_value(5, fi_net_earn);
	fin_net_earn[fin_len] = fi_net_earn;
	
	double fi_basic_eps;
	prfinancial->get_value(6, fi_basic_eps);
	fin_basic_eps[fin_len] = fi_basic_eps;
	
	double fi_dilut_eps;
	prfinancial->get_value(7, fi_dilut_eps);
	fin_dilut_eps[fin_len] = fi_dilut_eps;
	    
	double fi_margin;
	prfinancial->get_value(8, fi_margin);
	fin_margin[fin_len] = fi_margin;
	
	double fi_inventory;
	prfinancial->get_value(9, fi_inventory);
	fin_invent[fin_len] = fi_inventory;
	
	double fi_assets;
	prfinancial->get_value(10, fi_assets);
	fin_assets[fin_len] = fi_assets;
	
	double fi_liability;
	prfinancial->get_value(11, fi_liability);
	fin_liab[fin_len] = fi_liability;
	
	double fi_out_basics;
	prfinancial->get_value(12, fi_out_basics);
	fin_out_basic[fin_len] = fi_out_basics;
	
	double fi_out_dilut;
	prfinancial->get_value(13, fi_out_dilut);
	fin_out_dilut[fin_len] = fi_out_dilut;
	
	TRACE( TRACE_TRX_FLOW, "App: %d SD:fi-iter-next \n", xct_id);
	W_DO(fi_iter->next(_pssm, eof, *prfinancial));
    }
    assert(fin_len == max_fin_len); //Harness control
    
    /**
       select first max_rows_to_return rows
       day[].date  = DM_DATE,
       day[].close = DM_CLOSE,
       day[].high  = DM_HIGH,
       day[].low   = DM_LOW,
       day[].vol   = DM_VOL
       from
       DAILY_MARKET
       where
       DM_S_SYMB = symbol and
       DM_DATE >= start_day
       order by
       DM_DATE asc
       day_len = row_count
    */
    array_guard_t<myTime> day_date = new myTime[psdin._max_rows_to_return];
    array_guard_t<double> day_close = new double[psdin._max_rows_to_return];
    array_guard_t<double> day_high = new double[psdin._max_rows_to_return];
    array_guard_t<double> day_low = new double[psdin._max_rows_to_return];
    array_guard_t<int> day_vol = new int[psdin._max_rows_to_return];
    
    guard< index_scan_iter_impl<daily_market_t> > dm_iter;
    {
	index_scan_iter_impl<daily_market_t>* tmp_dm_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d SD:dm-get-iter-by-idx (%s) (%ld) \n",
	       xct_id, psdin._symbol, psdin._start_day);
	W_DO(_pdaily_market_man->dm_get_iter_by_index(_pssm, tmp_dm_iter,
						      prdailymarket, lowrep,
						      highrep, psdin._symbol,
						      psdin._start_day));
	dm_iter = tmp_dm_iter;
    }

    //already sorted due to index
    TRACE( TRACE_TRX_FLOW, "App: %d SD:dm-iter-next \n", xct_id);
    W_DO(dm_iter->next(_pssm, eof, *prdailymarket));
    int day_len;
    for(day_len = 0; day_len < psdin._max_rows_to_return && !eof; day_len++){
	prdailymarket->get_value(0, day_date[day_len]);
	prdailymarket->get_value(2, day_close[day_len]);
	prdailymarket->get_value(3, day_high[day_len]);
	prdailymarket->get_value(4, day_low[day_len]);
	prdailymarket->get_value(5, day_vol[day_len]);
	
	TRACE( TRACE_TRX_FLOW, "App: %d SD:dm-iter-next %ld \n",
	       xct_id, day_date[day_len]);
	W_DO(dm_iter->next(_pssm, eof, *prdailymarket));
    }
    assert(day_len >= min_day_len && day_len <= max_day_len); //Harness control
    
    /**
       select
       last_price = LT_PRICE,
       last_open  = LT_OPEN_PRICE,
       last_vol   = LT_VOL
       from
       LAST_TRADE
       where
       LT_S_SYMB = symbol
    */
    TRACE(TRACE_TRX_FLOW, "App:%d SD:lt-idx-probe (%s)\n", xct_id, psdin._symbol);
    W_DO(_plast_trade_man->lt_index_probe(_pssm, prlasttrade, psdin._symbol));

    double last_price, last_open, last_vol;
    prlasttrade->get_value(2, last_price);
    prlasttrade->get_value(3, last_open);
    prlasttrade->get_value(4, last_vol);
	
    char news_item[2][max_news_item_size+1]; //10000
    myTime news_dts[2];
    char news_src[2][31]; //30
    char news_auth[2][31]; //30
    char news_headline[2][81]; //80
    char news_summary[2][256]; //255
    
    int news_len;
    if(psdin._access_lob_flag) {
	/**
	   select first max_news_len rows
	   news[].item     = NI_ITEM,
	   news[].dts      = NI_DTS,
	   news[].src      = NI_SOURCE,
	   news[].auth     = NI_AUTHOR,
	   news[].headline = "",
	   news[].summary  = ""
	   from
	   NEWS_XREF,
	   NEWS_ITEM
	   where
	   NI_ID = NX_NI_ID and
	   NX_CO_ID = co_id
	*/	
	guard< index_scan_iter_impl<news_xref_t> > nx_iter;
	{
	    index_scan_iter_impl<news_xref_t>* tmp_nx_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d SD:nx-get-iter-by-idx (%ld) \n",
		   xct_id, co_id);
	    W_DO(_pnews_xref_man->nx_get_iter_by_index(_pssm, tmp_nx_iter,
						       prnewsxref, lowrep,
						       highrep, co_id));
	    nx_iter = tmp_nx_iter;
	}
	
	TRACE( TRACE_TRX_FLOW, "App: %d SD:nx-iter-next \n", xct_id);
	W_DO(nx_iter->next(_pssm, eof, *prnewsxref));
	for(news_len = 0; news_len < 2 && !eof; news_len++){
	    TIdent nx_ni_id;
	    prnewsxref->get_value(0, nx_ni_id);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d SD:nx-idx-probe (%ld) \n",
		   xct_id, nx_ni_id);
	    W_DO(_pnews_item_man->ni_index_probe(_pssm, prnewsitem, nx_ni_id));

	    prnewsitem->get_value(3, news_item[news_len], max_news_item_size+1);
	    prnewsitem->get_value(4, news_dts[news_len]);
	    prnewsitem->get_value(5, news_src[news_len], 31);
	    prnewsitem->get_value(6, news_auth[news_len], 31);
	    strcpy(news_headline[news_len], "");
	    strcpy(news_summary[news_len], "");
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d SD:nx-iter-next %ld \n",
		   xct_id, nx_ni_id);
	    W_DO(nx_iter->next(_pssm, eof, *prnewsxref));
	}
    } else {
	/**
	   select first max_news_len rows
	   news[].item     = "",
	   news[].dts      = NI_DTS,
	   news[].src      = NI_SOURCE,
	   news[].auth     = NI_AUTHOR,
	   news[].headline = NI_HEADLINE,
	   news[].summary  = NI_SUMMARY
	   from
	   NEWS_XREF,
	   NEWS_ITEM
	   where
	   NI_ID = NX_NI_ID and
	   NX_CO_ID = co_id
	*/
	guard< index_scan_iter_impl<news_xref_t> > nx_iter;
	{
	    index_scan_iter_impl<news_xref_t>* tmp_nx_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d SD:nx-get-iter-by-idx (%ld) \n",
		   xct_id, co_id);
	    W_DO(_pnews_xref_man->nx_get_iter_by_index(_pssm, tmp_nx_iter,
						       prnewsxref,
						       lowrep, highrep, co_id));
	    nx_iter = tmp_nx_iter;
	}
	
	TRACE( TRACE_TRX_FLOW, "App: %d SD:nx-iter-next \n", xct_id);
	W_DO(nx_iter->next(_pssm, eof, *prnewsxref));
	for(news_len = 0; news_len < 2 && !eof; news_len++){
	    TIdent nx_ni_id;
	    prnewsxref->get_value(0, nx_ni_id);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d SD:nx-idx-probe (%ld) \n",
		   xct_id, nx_ni_id);
	    W_DO(_pnews_item_man->ni_index_probe(_pssm, prnewsitem, nx_ni_id));

	    strcpy(news_item[news_len], "");
	    prnewsitem->get_value(1, news_headline[news_len], 81);
	    prnewsitem->get_value(2, news_summary[news_len], 256);
	    prnewsitem->get_value(4, news_dts[news_len]);
	    prnewsitem->get_value(5, news_src[news_len], 31);
	    prnewsitem->get_value(6, news_auth[news_len], 31);
	    
	    TRACE( TRACE_TRX_FLOW, "App: %d SD:nx-iter-next %ld \n",
		   xct_id, nx_ni_id);
	    W_DO(nx_iter->next(_pssm, eof, *prnewsxref));
	}
    }
    assert(news_len == max_news_len); //Harness control

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction
    // dumps the status of all the table rows used
    raddress.print_tuple();
    rcompany.print_tuple();
    rcompanycomp.print_tuple();
    rdailymarket.print_tuple();
    rexchange.print_tuple();
    rfinancial.print_tuple();
    rindustry.print_tuple();
    rlasttrade.print_tuple();
    rnewsitem.print_tuple();
    rnewsxref.print_tuple();
    rsecurity.print_tuple();
    rzipcode.print_tuple();
#endif

    return RCOK;

}


EXIT_NAMESPACE(tpce);    
