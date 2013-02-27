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

/** @file:   shore_tpce_schema_man.h
 *
 *  @brief:  Declaration of the TPC-E table managers
 *
 *  @author: Ippokratis Pandis, Apr 2010
 *  @author: Cansu Kaynak, april 2010
 *  @author: Djordje Jevdjic, april 2010
 *
 */


#ifndef __SHORE_TPCE_SCHEMA_MANAGER_H
#define __SHORE_TPCE_SCHEMA_MANAGER_H

#include "workload/tpce/shore_tpce_schema.h"
#include "workload/tpce/egen/TxnHarnessStructs.h"

using namespace TPCE;
using namespace shore;


ENTER_NAMESPACE(tpce);

/* ------------------------------------------------------------------ */
/* --- The managers of all the tables used in the TPC-E benchmark --- */
/* ------------------------------------------------------------------ */


/* ----------------------------- */
/* ---- ACCOUNT_PERMISSION ----- */
/* ----------------------------- */

class account_permission_man_impl : public table_man_impl<account_permission_t>
{
    typedef table_row_t account_permission_tuple;
    typedef index_scan_iter_impl <account_permission_t>  account_permission_index_iter;

public:

    account_permission_man_impl(account_permission_t* aAccount_PermissionDesc)
        : table_man_impl<account_permission_t>(aAccount_PermissionDesc)   { }

    ~account_permission_man_impl() { }

    w_rc_t ap_index_probe(ss_m* db, account_permission_tuple* ptuple, 
			  const TIdent acct_id,
			  const char* exec_tax_id);
							 
    w_rc_t ap_get_iter_by_index(ss_m* db,
				account_permission_index_iter* &iter,
				account_permission_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent acct_id,
				lock_mode_t alm = SH,
				bool need_tuple = true);
    
    w_rc_t ap_update_acl(ss_m* db, account_permission_tuple* ptuple, const char* acl, lock_mode_t lm = EX);
}; 


/* ----------------- */
/* ---- BROKER ----- */
/* ----------------- */

class broker_man_impl : public table_man_impl<broker_t>
{
    typedef table_row_t broker_tuple;
    typedef index_scan_iter_impl <broker_t>  broker_index_iter;

public:

    broker_man_impl(broker_t* aBrokerDesc)
        : table_man_impl<broker_t>(aBrokerDesc)
    { }

    ~broker_man_impl() { }

    w_rc_t broker_update_ca_nt_by_index(ss_m* db, broker_tuple* ptuple, const TIdent broker_id,
					const double comm_amount, lock_mode_t lm = EX);    
    
    w_rc_t b_get_iter_by_index2(ss_m* db,
				broker_index_iter* &iter,
				broker_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent b_id,
				lock_mode_t alm = SH,
				bool need_tuple = false);

    w_rc_t b_get_iter_by_index3(ss_m* db,
				broker_index_iter* &iter,
				broker_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const char* b_name,
				lock_mode_t alm = SH,
				bool need_tuple = false);
}; 


/* ----------------------------- */
/* ---- CASH_TRANSACTION ------- */
/* ----------------------------- */

class cash_transaction_man_impl : public table_man_impl<cash_transaction_t>
{
    typedef table_row_t cash_transaction_tuple;

public:

    cash_transaction_man_impl(cash_transaction_t* aCash_TransactionDesc)
        : table_man_impl<cash_transaction_t>(aCash_TransactionDesc) { }

    ~cash_transaction_man_impl() { }

    w_rc_t ct_index_probe(ss_m* db, cash_transaction_tuple* ptuple, const TIdent tr_id);

    w_rc_t ct_index_probe_forupdate(ss_m* db, cash_transaction_tuple* ptuple, const TIdent t_id);
    
    w_rc_t ct_update_name(ss_m* db, cash_transaction_tuple* ptuple, const char* ct_name, lock_mode_t lm = EX);
}; 


/* ----------------- */
/* ---- CHARGE ----- */
/* ----------------- */

class charge_man_impl : public table_man_impl<charge_t>
{
    typedef table_row_t charge_tuple;
    typedef index_scan_iter_impl <charge_t>  charge_index_iter;

public:

    charge_man_impl(charge_t* aChargeDesc)
        : table_man_impl<charge_t>(aChargeDesc)  { }

    ~charge_man_impl() { }

    w_rc_t ch_index_probe(ss_m* db, charge_tuple* ptuple, const short cust_tier, const char* trade_type_id);

}; 


/* ------------------------ */
/* --- COMMISSION_RATE ---- */
/* ------------------------ */

class commission_rate_man_impl : public table_man_impl<commission_rate_t>
{
    typedef table_row_t commission_rate_tuple;
    typedef index_scan_iter_impl <commission_rate_t>  commission_rate_index_iter;

public:

    commission_rate_man_impl(commission_rate_t* aCommission_RateDesc)
        : table_man_impl<commission_rate_t>(aCommission_RateDesc)    { }

    ~commission_rate_man_impl() { }

    w_rc_t cr_get_iter_by_index(ss_m* db,
				commission_rate_index_iter* &iter,
				commission_rate_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const short c_tier, 
				const char* type_id, 
				const char* s_ex_id,
				const int trade_qty,
				lock_mode_t alm = SH,
                                bool need_tuple = true);

}; 


/* ----------------- */
/* --- COMPANY ----- */
/* ----------------- */

class company_man_impl : public table_man_impl<company_t>
{
    typedef table_row_t company_tuple;
    typedef index_scan_iter_impl <company_t>  company_index_iter;

public:

    company_man_impl(company_t* aCompanyDesc)
        : table_man_impl<company_t>(aCompanyDesc)  { }

    ~company_man_impl() { }

    w_rc_t co_get_iter_by_index2(ss_m* db, company_index_iter* &iter, company_tuple* ptuple, rep_row_t &replow, rep_row_t &rephigh,
				 const char* co_name, lock_mode_t alm = SH, bool need_tuple = false);
				
    w_rc_t co_index_probe(ss_m* db, company_tuple* ptuple, const TIdent co_id);
   
    w_rc_t co_index_probe_forupdate(ss_m* db, company_tuple* ptuple, const TIdent co_id);
   
    w_rc_t co_update_sprate(ss_m* db, company_tuple* ptuple, const char* sprate, lock_mode_t lm = EX);
   
    w_rc_t co_get_iter_by_index3(ss_m* db, company_index_iter* &iter, company_tuple* ptuple, rep_row_t &replow, rep_row_t &rephigh, 
				 const char* in_id, lock_mode_t alm = SH, bool need_tuple = false);

}; 


/* ----------------- */
/* --- CUSTOMER ---- */
/* ----------------- */

class customer_man_impl : public table_man_impl<customer_t>
{
    typedef table_row_t customer_tuple;
    typedef index_scan_iter_impl <customer_t> customer_index_iter;
public:

    customer_man_impl(customer_t* aCustomerDesc)
        : table_man_impl<customer_t>(aCustomerDesc)  { }

    ~customer_man_impl() { }

    w_rc_t c_index_probe(ss_m* db, customer_tuple* ptuple, const TIdent cust_id);
    
    w_rc_t c_index_probe_forupdate(ss_m* db, customer_tuple* ptuple, const TIdent cust_id); 
				
    w_rc_t c_update_email2(ss_m* db, customer_tuple* ptuple, const char* email2, lock_mode_t alm = EX);

    w_rc_t c_get_iter_by_index2(ss_m* db,
				customer_index_iter* &iter,
				customer_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const char* tax_id,
				lock_mode_t alm = SH,
				bool need_tuple = true);

    w_rc_t c_get_iter_by_index3(ss_m* db,
				customer_index_iter* &iter,
				customer_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent cust_id,
				lock_mode_t alm = SH,
				bool need_tuple = false);

}; 


/* ------------------------ */
/* --- CUSTOMER_ACCOUNT --- */
/* ------------------------ */

class customer_account_man_impl : public table_man_impl<customer_account_t>
{
    typedef table_row_t customer_account_tuple;
    typedef index_scan_iter_impl <customer_account_t> customer_account_index_iter;

public:

    customer_account_man_impl(customer_account_t* aCustomer_AccountDesc)
        : table_man_impl<customer_account_t>(aCustomer_AccountDesc)   { }

    ~customer_account_man_impl() { }

    w_rc_t ca_index_probe(ss_m* db, customer_account_tuple* ptuple, const TIdent acct_id);

    w_rc_t ca_update_bal(ss_m* db, customer_account_tuple* ptuple,
			 const TIdent acct_id, const double se_amount,
			 lock_mode_t alm = EX);
    
    w_rc_t ca_get_iter_by_index2(ss_m* db,
				 customer_account_index_iter* &iter,
				 customer_account_tuple* ptuple,
				 rep_row_t &replow,
				 rep_row_t &rephigh,
				 const TIdent cust_id,
				 lock_mode_t alm = SH,
				 bool need_tuple = true);
    
}; 


/* ------------------------ */
/* --- CUSTOMER_TAXRATE --- */
/* ------------------------ */

class customer_taxrate_man_impl : public table_man_impl<customer_taxrate_t>
{
    typedef table_row_t customer_taxrate_tuple;
    typedef index_scan_iter_impl <customer_taxrate_t> customer_taxrate_index_iter;

public:

    customer_taxrate_man_impl(customer_taxrate_t* aCustomer_TaxrateDesc)
        : table_man_impl<customer_taxrate_t>(aCustomer_TaxrateDesc)  { }

    ~customer_taxrate_man_impl() { }

    w_rc_t cx_get_iter_by_index(ss_m* db,
				customer_taxrate_index_iter* &iter,
				customer_taxrate_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent cust_id,
				lock_mode_t alm = SH,
                                bool need_tuple = false);
				
    w_rc_t cx_update_txid(ss_m* db, customer_taxrate_tuple* ptuple, const char* new_tax_rate, lock_mode_t lm = EX);
    
};


/* -----------------*/
/* --- HOLDING ---- */
/* ---------------- */

class holding_man_impl : public table_man_impl<holding_t>
{
    typedef table_row_t holding_tuple; 
    typedef index_scan_iter_impl <holding_t> holding_index_iter;
public:
    
    holding_man_impl(holding_t* aHoldingDesc)
        : table_man_impl<holding_t>(aHoldingDesc)
    { }
    
    ~holding_man_impl() { }
    
    w_rc_t h_get_iter_by_index2(ss_m* db,
				holding_index_iter* &iter,
				holding_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent acct_id,
				const char* symbol,
				bool is_backward = false,
				lock_mode_t alm = SH,
				bool need_tuple = true);

    w_rc_t h_update_qty(ss_m* db, holding_tuple* ptuple, const int qty, lock_mode_t lm = EX);
    
    w_rc_t h_delete_tuple(ss_m* db, holding_tuple* ptuple, rid_t rid);
    
}; 


/* ------------------------ */
/* --- HOLDING_HISTORY ---- */
/* ------------------------ */

class holding_history_man_impl : public table_man_impl<holding_history_t>
{
    typedef table_row_t holding_history_tuple;
    typedef index_scan_iter_impl <holding_history_t> holding_history_index_iter;

public:

    holding_history_man_impl(holding_history_t* aHolding_HistoryDesc)
        : table_man_impl<holding_history_t>(aHolding_HistoryDesc)
    { }

    ~holding_history_man_impl() { }
			 
    w_rc_t hh_get_iter_by_index2(ss_m* db,
				 holding_history_index_iter* &iter,
				 holding_history_tuple* ptuple,
				 rep_row_t &replow,
				 rep_row_t &rephigh,
				 TIdent trade_id,
				 lock_mode_t alm = SH,
                                 bool need_tuple = true);
    
};


/* ------------------------ */
/* --- HOLDING_SUMMARY ---- */
/* ------------------------ */

class holding_summary_man_impl : public table_man_impl<holding_summary_t>
{
    typedef table_row_t holding_summary_tuple;
    typedef index_scan_iter_impl <holding_summary_t> holding_summary_index_iter;

public:

    holding_summary_man_impl(holding_summary_t* aHolding_SummaryDesc)
        : table_man_impl<holding_summary_t>(aHolding_SummaryDesc)    { }

    ~holding_summary_man_impl() { }

    w_rc_t hs_index_probe(ss_m* db,
			  holding_summary_tuple* ptuple,
			  const TIdent acct_id,
			  const char* symbol);

    w_rc_t hs_update_qty(ss_m* db,
		         holding_summary_tuple* ptuple,
			 const TIdent acct_id,
			 const char* symbol,
		         const int qty,
		         lock_mode_t lm = EX);
			     
    w_rc_t hs_get_iter_by_index(ss_m* db,
				holding_summary_index_iter* &iter,
				holding_summary_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent acct_id,
				bool need_tuple = true,
				lock_mode_t alm = SH);
			       
}; 


/* --------------------- */
/* ----- LAST_TRADE ---- */
/* --------------------- */

class last_trade_man_impl : public table_man_impl<last_trade_t>
{
    typedef table_row_t last_trade_tuple;

public:

    last_trade_man_impl(last_trade_t* aLast_TradeDesc)
        : table_man_impl<last_trade_t>(aLast_TradeDesc) { }

    ~last_trade_man_impl() { }
    
    w_rc_t lt_index_probe(ss_m* db, last_trade_tuple* ptuple, const char* symbol);
    
    w_rc_t lt_update_by_index(ss_m* db, last_trade_tuple* ptuple, 
			      const char* symbol,
			      const double price_quote,
			      const int trade_qty,
			      const myTime now_dts,
			      lock_mode_t lm = EX);
    
}; 


/* ----------------- */
/* --- SECURITY ---- */
/* ----------------- */

class security_man_impl : public table_man_impl<security_t>
{
    typedef table_row_t security_tuple;
    typedef index_scan_iter_impl <security_t> security_index_iter;

public:

    security_man_impl(security_t* aSecurityDesc)
        : table_man_impl<security_t>(aSecurityDesc) { }

    ~security_man_impl() { }

    w_rc_t s_index_probe(ss_m* db, security_tuple* ptuple, const char* symbol);

    w_rc_t s_index_probe_forupdate(ss_m* db, security_tuple* ptuple, const char* symbol);
				    
    w_rc_t s_update_ed(ss_m* db, security_tuple* ptuple, const myTime exch_date, lock_mode_t lm = EX);
    
    w_rc_t s_get_iter_by_index(ss_m* db,
			       security_index_iter* &iter,
			       security_tuple* ptuple,
			       rep_row_t &replow,
			       rep_row_t &rephigh,
			       const char* symbol,						
			       lock_mode_t alm = SH,
			       bool need_tuple = false);

    w_rc_t s_get_iter_by_index4(ss_m* db,
				security_index_iter* &iter,
				security_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent co_id,
				lock_mode_t alm = SH,
				bool need_tuple = false);

    w_rc_t s_get_iter_by_index4(ss_m* db,
				security_index_iter* &iter,
				security_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent co_id,
				const char* issue,
				lock_mode_t alm = SH,
				bool need_tuple = true);

}; 


/* ------------------- */
/* --- SETTLEMENT ---- */
/* ------------------- */

class settlement_man_impl : public table_man_impl<settlement_t>
{
    typedef table_row_t settlement_tuple;

public:

    settlement_man_impl(settlement_t* aSettlementDesc)
        : table_man_impl<settlement_t>(aSettlementDesc)  { }

    ~settlement_man_impl() { }

    w_rc_t se_index_probe(ss_m* db, settlement_tuple* ptuple, const TIdent tr_id);
    
    w_rc_t se_index_probe_forupdate(ss_m* db, settlement_tuple* ptuple, const TIdent tr_id);
    
    w_rc_t se_update_name(ss_m* db, settlement_tuple* ptuple, const char* cash_type, lock_mode_t lm = EX);
    
}; 


/* -------------------- */
/* ---- TAXRATE ------- */
/* -------------------- */

class taxrate_man_impl : public table_man_impl<taxrate_t>
{
    typedef table_row_t taxrate_tuple;

public:

    taxrate_man_impl(taxrate_t* aTaxrateDesc)
        : table_man_impl<taxrate_t>(aTaxrateDesc)
    { }

    ~taxrate_man_impl() { }

    w_rc_t tx_index_probe(ss_m* db, taxrate_tuple* ptuple, const char* tx_id);

    w_rc_t tx_index_probe_forupdate(ss_m* db, taxrate_tuple* ptuple, const char* tx_id);
    
    w_rc_t tx_update_name(ss_m* db, taxrate_tuple* ptuple, const char* tx_name, lock_mode_t lm = EX);
    
}; 


/* --------------- */
/* ---- TRADE ---- */
/* --------------- */

class trade_man_impl : public table_man_impl<trade_t>
{
    typedef table_row_t trade_tuple;
    typedef index_scan_iter_impl <trade_t> trade_index_iter;
public:

    trade_man_impl(trade_t* aTradeDesc)
        : table_man_impl<trade_t>(aTradeDesc)   { }

    ~trade_man_impl() { }

    w_rc_t t_index_probe(ss_m* db, trade_tuple* ptuple, const TIdent trade_id);

    w_rc_t t_update_tax_by_index(ss_m* db, trade_tuple* ptuple, const TIdent t_id,
				 const double tax_amount, lock_mode_t lm = EX);
    
    w_rc_t t_update_ca_td_sci_tp_by_index(ss_m* db,
					  trade_tuple* ptuple,
					  const TIdent trade_id,
					  const double comm_amount,
					  const myTime trade_dts,
					  const char* st_completed_id,
					  const double trade_price,
					  lock_mode_t lm = EX);
					 
    w_rc_t t_get_iter_by_index(ss_m* db,
			       trade_index_iter* &iter,
			       trade_tuple* ptuple,
			       rep_row_t &replow,
			       rep_row_t &rephigh,
			       const TIdent t_id,
			       lock_mode_t alm = SH,
			       bool need_tuple = true);
				
    w_rc_t t_get_iter_by_index2(ss_m* db,
				trade_index_iter* &iter,
				trade_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent acct_id,
				const myTime start_dts,
				const myTime end_dts,
				bool is_backward = false,
                                bool need_tuple = true,
				lock_mode_t alm = SH);
				
    w_rc_t t_get_iter_by_index3(ss_m* db,
				trade_index_iter* &iter,
				trade_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const char* symbol,
				const myTime start_dts,
				const myTime end_dts,
				lock_mode_t alm = SH,
                                bool need_tuple = true);
				
    w_rc_t t_update_dts_stdid_by_index(ss_m* db,
				       trade_tuple* ptuple,
				       const TIdent req_trade_id,
				       const myTime now_dts,
				       const char* status_submitted,
				       lock_mode_t lm = EX);				      
				      
    w_rc_t t_index_probe_forupdate(ss_m* db, trade_tuple* ptuple, const TIdent t_id);
    
    w_rc_t t_update_name(ss_m* db, trade_tuple* ptuple, const char* exec_name, lock_mode_t lm = EX);
			  
}; 


/* ----------------------- */
/* ---- TRADE_HISTORY ---- */
/* ----------------------- */

class trade_history_man_impl : public table_man_impl<trade_history_t>
{
    typedef table_row_t trade_history_tuple;
    typedef index_scan_iter_impl <trade_history_t> trade_history_index_iter;
public:

    trade_history_man_impl(trade_history_t* aTrade_HistoryDesc)
        : table_man_impl<trade_history_t>(aTrade_HistoryDesc)
    { }

    ~trade_history_man_impl() { }
    
    w_rc_t th_get_iter_by_index(ss_m* db,
				trade_history_index_iter* &iter,
				trade_history_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent t_id,
				lock_mode_t alm = SH,
                                bool need_tuple = true);

}; 


/* -------------------- */
/* ---- TRADE_TYPE ---- */
/* -------------------- */

class trade_type_man_impl : public table_man_impl<trade_type_t>
{
    typedef table_row_t trade_type_tuple;

public:

    trade_type_man_impl(trade_type_t* aTrade_TypeDesc)
        : table_man_impl<trade_type_t>(aTrade_TypeDesc)
    { }

    ~trade_type_man_impl() { }

    w_rc_t tt_index_probe(ss_m* db, trade_type_tuple* ptuple, const char* trade_type_id);

};


/* ------------------- */
/* --- STATUS_TYPE --- */
/* ------------------- */

class status_type_man_impl : public table_man_impl<status_type_t>
{
    typedef table_row_t status_type_tuple;

public:

    status_type_man_impl(status_type_t* aStatus_TypeDesc)
        : table_man_impl<status_type_t>(aStatus_TypeDesc)
    { }

    ~status_type_man_impl() { }

    w_rc_t st_index_probe(ss_m* db, status_type_tuple* ptuple, const char* st_id);
    
}; 


/* ------------------- */
/* --- SECTOR -------- */
/* ------------------- */

class sector_man_impl : public table_man_impl<sector_t>
{
    typedef table_row_t sector_tuple;
    typedef index_scan_iter_impl <sector_t> sector_index_iter;

public:

    sector_man_impl(sector_t* aSectorDesc)
        : table_man_impl<sector_t>(aSectorDesc)
    { }

    ~sector_man_impl() { }

    w_rc_t sc_get_iter_by_index2(ss_m* db,
				 sector_index_iter* &iter,
				 sector_tuple* ptuple,
				 rep_row_t &replow,
				 rep_row_t &rephigh,
				 const char* sc_name,
				 lock_mode_t alm = SH,
				 bool need_tuple = false);
    
};


/* ------------------- */
/* --- EXCHANGE ------ */
/* ------------------- */

class exchange_man_impl : public table_man_impl<exchange_t>
{
    typedef table_row_t exchange_tuple;
    typedef index_scan_iter_impl <exchange_t> exchange_index_iter;

public:

    exchange_man_impl(exchange_t* aExchangeDesc)
        : table_man_impl<exchange_t>(aExchangeDesc)
    { }

    ~exchange_man_impl() { }

    w_rc_t ex_update_desc(ss_m* db, exchange_tuple* ptuple, const char* new_desc, lock_mode_t alm = EX);
			    
    w_rc_t ex_index_probe(ss_m* db, exchange_tuple* ptuple, const char* id);
    
};


/* ------------------- */
/* --- INDUSTRY ------ */
/* ------------------- */

class industry_man_impl : public table_man_impl<industry_t>
{
    typedef table_row_t industry_tuple;
    typedef index_scan_iter_impl <industry_t> industry_index_iter;

public:

    industry_man_impl(industry_t* aIndustryDesc)
        : table_man_impl<industry_t>(aIndustryDesc)
    { }

    ~industry_man_impl() { }

    w_rc_t in_index_probe(ss_m* db, industry_tuple* ptuple, const char* id);

    w_rc_t in_get_iter_by_index2(ss_m* db,
				 industry_index_iter* &iter,
				 industry_tuple* ptuple,
				 rep_row_t &replow,
				 rep_row_t &rephigh,
				 const char* in_name,
				 lock_mode_t alm = SH,
				 bool need_tuple = false);
    
    w_rc_t in_get_iter_by_index3(ss_m* db,
				 industry_index_iter* &iter,
				 industry_tuple* ptuple,
				 rep_row_t &replow,
				 rep_row_t &rephigh,
				 const char* sc_id,
				 lock_mode_t alm = SH,
				 bool need_tuple = false);

};


/* ------------------- */
/* --- ZIP CODE ------ */
/* ------------------- */

class zip_code_man_impl : public table_man_impl<zip_code_t>
{
    typedef table_row_t zip_code_tuple;
    typedef index_scan_iter_impl <zip_code_t>  zip_code_index_iter;
public:

    zip_code_man_impl(zip_code_t* aZip_CodeDesc)
        : table_man_impl<zip_code_t>(aZip_CodeDesc)  { }

    ~zip_code_man_impl() { }

    w_rc_t zc_index_probe(ss_m* db, zip_code_tuple* ptuple, const char* zc_code);

}; 


/* ------------------- */
/* -- TRADE_REQUEST -- */
/* ------------------- */

class trade_request_man_impl : public table_man_impl<trade_request_t>
{
    typedef table_row_t trade_request_tuple;
    typedef index_scan_iter_impl <trade_request_t>  trade_request_index_iter;
public:

    trade_request_man_impl(trade_request_t* aTrade_RequestDesc)
        : table_man_impl<trade_request_t>(aTrade_RequestDesc)
    { }

    ~trade_request_man_impl() { }

    w_rc_t tr_delete_tuple(ss_m* db, trade_request_tuple* ptuple, rid_t rid);
    
    w_rc_t tr_get_iter_by_index4(ss_m* db,
				 trade_request_index_iter* &iter,
				 trade_request_tuple* ptuple,
				 rep_row_t &replow,
				 rep_row_t &rephigh,
				 const char* tr_s_symb,
				 const TIdent tr_b_id,
				 lock_mode_t alm = SH,
				 bool need_tuple = true);

    w_rc_t tr_get_iter_by_index4(ss_m* db,
				 trade_request_index_iter* &iter,
				 trade_request_tuple* ptuple,
				 rep_row_t &replow,
				 rep_row_t &rephigh,
				 const char* tr_s_symb,
				 lock_mode_t alm = SH,
				 bool need_tuple = true);

};


/* ------------------------- */
/* --- COMPANY_COMPETITOR -- */
/* --------------------------*/

class company_competitor_man_impl : public table_man_impl<company_competitor_t>
{
    typedef table_row_t company_competitor_tuple;
    typedef index_scan_iter_impl <company_competitor_t>  company_competitor_index_iter;

public:

    company_competitor_man_impl(company_competitor_t* aCompany_CompetitorDesc)
        : table_man_impl<company_competitor_t>(aCompany_CompetitorDesc)
    { }

    ~company_competitor_man_impl() { }

    w_rc_t cc_get_iter_by_index(ss_m* db,
				company_competitor_index_iter* &iter,
				company_competitor_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent co_id,
				bool need_tuple = false,
				lock_mode_t alm = SH);

};


/* ------------------------- */
/* --- DAILY_MARKET -------- */
/* --------------------------*/

class daily_market_man_impl : public table_man_impl<daily_market_t>
{
    typedef table_row_t daily_market_tuple;
    typedef index_scan_iter_impl <daily_market_t>  daily_market_index_iter;
public:

    daily_market_man_impl(daily_market_t* aDaily_MarketDesc)
        : table_man_impl<daily_market_t>(aDaily_MarketDesc)
    { }

    ~daily_market_man_impl() { }

    w_rc_t dm_index_probe(ss_m* db, daily_market_tuple* ptuple, const char* symbol, const myTime start_date);
				
    w_rc_t dm_update_vol(ss_m* db, daily_market_tuple* ptuple, const int vol_incr, lock_mode_t lm = EX);
			  
    w_rc_t dm_get_iter_by_index(ss_m* db,
				daily_market_index_iter* &iter,
				daily_market_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const char* symbol,
				const myTime start_day = 0,
				lock_mode_t alm = SH,
				bool need_tuple = true);
				
};


/* ---------------------- */
/* --- FINANCIAL -------- */
/* -----------------------*/

class financial_man_impl : public table_man_impl<financial_t>
{
    typedef table_row_t financial_tuple;
    typedef index_scan_iter_impl <financial_t>  financial_index_iter;
public:

    financial_man_impl(financial_t* aFinancialDesc)
        : table_man_impl<financial_t>(aFinancialDesc)
    { }

    ~financial_man_impl() { }

    w_rc_t fi_update_desc(ss_m* db, financial_tuple* ptuple, const myTime new_qtr_start_date, lock_mode_t alm = EX);
			    
    w_rc_t fi_get_iter_by_index(ss_m* db,
				financial_index_iter* &iter,
				financial_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent co_id,
				lock_mode_t alm = SH,
				bool need_tuple = true);
    
};


/* ---------------------- */
/* --- ADDRESS ---------- */
/* -----------------------*/

class address_man_impl : public table_man_impl<address_t>
{
    typedef table_row_t address_tuple;

public:

    address_man_impl(address_t* aAddressDesc)
        : table_man_impl<address_t>(aAddressDesc)
    { }

    ~address_man_impl() { }

    w_rc_t ad_get_table_iter(ss_m* db, table_iter* &iter);						  

    w_rc_t ad_index_probe(ss_m* db, address_tuple* ptuple, const TIdent ad_id);
    
    w_rc_t ad_index_probe_forupdate(ss_m* db, address_tuple* ptuple, const TIdent ad_id);
    
    w_rc_t ad_update_line2(ss_m* db, address_tuple* ptuple, const char* line2, lock_mode_t = EX);   
    
};


/* ---------------------- */
/* --- WATCH_ITEM ------- */
/* -----------------------*/

class watch_item_man_impl : public table_man_impl<watch_item_t>
{
    typedef table_row_t watch_item_tuple;
    typedef index_scan_iter_impl <watch_item_t>  watch_item_index_iter;
public:

    watch_item_man_impl(watch_item_t* aWatch_ItemDesc)
        : table_man_impl<watch_item_t>(aWatch_ItemDesc)
    { }

    ~watch_item_man_impl() { }

    w_rc_t wi_update_symb(ss_m* db, watch_item_tuple* ptuple, const TIdent wl_id,
			  const char* old_symbol, const char* new_symbol, lock_mode_t lm = EX);
     
    w_rc_t wi_get_iter_by_index(ss_m* db,
				watch_item_index_iter* &iter,
				watch_item_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent wl_id,
				lock_mode_t alm = SH,
				bool need_tuple = false);

};


/* ---------------------- */
/* --- WATCH_LIST ------- */
/* -----------------------*/

class watch_list_man_impl : public table_man_impl<watch_list_t>
{
    typedef table_row_t watch_list_tuple;
    typedef index_scan_iter_impl <watch_list_t>  watch_list_index_iter;

public:

    watch_list_man_impl(watch_list_t* aWatch_ListDesc)
        : table_man_impl<watch_list_t>(aWatch_ListDesc)
    { }

    ~watch_list_man_impl() { }

    w_rc_t wl_get_iter_by_index2(ss_m* db,
				 watch_list_index_iter* &iter,
				 watch_list_tuple* ptuple,
				 rep_row_t &replow,
				 rep_row_t &rephigh,
				 const TIdent c_id,
				 lock_mode_t alm = SH,
				 bool need_tuple = false);
    
};


/* ---------------------- */
/* --- NEWS_ITEM -------- */
/* -----------------------*/

class news_item_man_impl : public table_man_impl<news_item_t>
{
    typedef table_row_t news_item_tuple;

public:

    news_item_man_impl(news_item_t* aNews_ItemDesc)
        : table_man_impl<news_item_t>(aNews_ItemDesc)
    { }

    ~news_item_man_impl() { }

    w_rc_t ni_index_probe(ss_m* db, news_item_tuple* ptuple, const TIdent ni_id);
    
    w_rc_t ni_index_probe_forupdate(ss_m* db, news_item_tuple* ptuple, const TIdent ni_id);
			      
    w_rc_t ni_update_dts_by_index(ss_m* db, news_item_tuple* ptuple, const myTime ni_dts, lock_mode_t alm = EX);
    
};


/* ---------------------- */
/* --- NEWS_XREF -------- */
/* -----------------------*/

class news_xref_man_impl : public table_man_impl<news_xref_t>
{
    typedef table_row_t news_xref_tuple;
    typedef index_scan_iter_impl <news_xref_t>  news_xref_index_iter;
    
public:

    news_xref_man_impl(news_xref_t* aNews_XrefDesc)
        : table_man_impl<news_xref_t>(aNews_XrefDesc)  { }

    ~news_xref_man_impl() {}

    w_rc_t nx_get_table_iter(ss_m* db, table_iter* &iter);
    
    w_rc_t nx_get_iter_by_index(ss_m* db,
				news_xref_index_iter* &iter,
				news_xref_tuple* ptuple,
				rep_row_t &replow,
				rep_row_t &rephigh,
				const TIdent co_id,
				lock_mode_t alm = SH,
				bool need_tuple = false);
    
};


EXIT_NAMESPACE(tpce);

#endif /* __SHORE_TPCE_SCHEMA_MANAGER_H */
