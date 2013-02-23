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

/** @file:   shore_tpce_schema_man.cpp
 *
 *  @brief:  Implementation of the workload-specific access methods 
 *           on TPC-E tables
 *
 *  @author: Cansu Kaynak, Apr 2010
 *  @author: Djordje Jevdjic, Apr 2010
 *
 */

#include "workload/tpce/shore_tpce_schema_man.h"


using namespace shore;

ENTER_NAMESPACE(tpce);


/* ----------------------------- */
/* ---- ACCOUNT_PERMISSION ----- */
/* ----------------------------- */

w_rc_t account_permission_man_impl::ap_index_probe(ss_m* db, account_permission_tuple* ptuple, 
                                                   const TIdent acct_id,
                                                   const char* exec_tax_id)
{
    assert (ptuple);    
    ptuple->set_value(0, acct_id);
    ptuple->set_value(2, exec_tax_id);
    return (index_probe_by_name(db, "AP_INDEX", ptuple));
}

w_rc_t account_permission_man_impl::ap_get_iter_by_index(ss_m* db,
							 account_permission_index_iter* &iter,
							 account_permission_tuple* ptuple,
							 rep_row_t &replow,
							 rep_row_t &rephigh,
							 const TIdent acct_id,
							 lock_mode_t alm,
							 bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("AP_INDEX");
    assert (pindex);

    // AP_INDEX: { 0, 2 }

    // prepare the key to be probed
    ptuple->set_value(0, acct_id);
    ptuple->set_value(2, "");

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(2, temp);
    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t account_permission_man_impl::ap_update_acl(ss_m* db,
                                                  account_permission_tuple* ptuple,
                                                  const char* acl,
                                                  lock_mode_t lm)
{
    assert (ptuple); 
    ptuple->set_value(1, acl);
    return (update_tuple(db, ptuple, lm));
}


/* ----------------- */
/* --- ADDRESS ----- */
/* ----------------- */

w_rc_t address_man_impl::ad_get_table_iter(ss_m* db, table_iter* &iter)						  
{
    /* get the tuple iterator (table scan) */
    W_DO(get_iter_for_file_scan(db, iter));
    return (RCOK);
}

w_rc_t address_man_impl::ad_index_probe(ss_m* db, address_tuple* ptuple, const TIdent ad_id)
{
    assert (ptuple);    
    ptuple->set_value(0, ad_id);
    return (index_probe_by_name(db, "AD_INDEX", ptuple));
}

w_rc_t address_man_impl::ad_index_probe_forupdate(ss_m* db, address_tuple* ptuple, const TIdent ad_id)
{
    assert (ptuple);    
    ptuple->set_value(0, ad_id);
    return (index_probe_forupdate_by_name(db, "AD_INDEX", ptuple));
}

w_rc_t address_man_impl::ad_update_line2(ss_m* db,
                                         address_tuple* ptuple,
                                         const char* line2,
                                         lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(2, line2);
    return (update_tuple(db, ptuple, lm));
}


/* ----------------- */
/* ---- BROKER ----- */
/* ----------------- */

w_rc_t broker_man_impl::broker_update_ca_nt_by_index(ss_m* db, broker_tuple* ptuple, const TIdent broker_id,
						     const double comm_amount, lock_mode_t lm)
{
    assert (ptuple);

    // 1. idx probe for update the broker
    // 2. update table
    ptuple->set_value(0, broker_id);
    W_DO(index_probe_forupdate_by_name(db, "B_INDEX", ptuple));
    
    double b_comm_total;
    ptuple->get_value(4, b_comm_total);
    int  b_num_trades;
    ptuple->get_value(3, b_num_trades);
	
    ptuple->set_value(4, b_comm_total + comm_amount);
    ptuple->set_value(3, b_num_trades + 1);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t broker_man_impl::b_get_iter_by_index2(ss_m* db,
					     broker_index_iter* &iter,
					     broker_tuple* ptuple,
					     rep_row_t &replow,
					     rep_row_t &rephigh,
					     const TIdent b_id, 
					     lock_mode_t alm,
					     bool need_tuple)
{
    assert (ptuple);
    assert (_ptable);
    
    index_desc_t* pindex = _ptable->find_index("B_INDEX_2");
    assert (pindex);
    
    // B_INDEX: { 0, 2 }
    
    // prepare the key to be probed
    ptuple->set_value(0, b_id);
    ptuple->set_value(2, "");
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);
    
    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(2, temp);
    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    
    
    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t broker_man_impl::b_get_iter_by_index3(ss_m* db,
					     broker_index_iter* &iter,
					     broker_tuple* ptuple,
					     rep_row_t &replow,
					     rep_row_t &rephigh,
					     const char* b_name,
					     lock_mode_t alm,
					     bool need_tuple)
{
    assert (ptuple);
    assert (_ptable);
    
    index_desc_t* pindex = _ptable->find_index("B_INDEX_3");
    assert (pindex);
    
    // CR_INDEX: { 2, 0 }
    
    // prepare the key to be probed
    ptuple->set_value(2, b_name);
    ptuple->set_value(0, (long long) 0);
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    ptuple->set_value(0, MAX_ID);
    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    
    
    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* ----------------------------- */
/* ---- CASH_TRANSACTION ------- */
/* ----------------------------- */

w_rc_t cash_transaction_man_impl::ct_index_probe(ss_m* db, cash_transaction_tuple* ptuple, const TIdent tr_id)
{
    assert (ptuple);    
    ptuple->set_value(0, tr_id);
    return (index_probe_by_name(db, "CT_INDEX", ptuple));
}

w_rc_t cash_transaction_man_impl::ct_index_probe_forupdate(ss_m* db, cash_transaction_tuple* ptuple, const TIdent t_id)
{
    assert (ptuple);    
    ptuple->set_value(0, t_id);
    return (index_probe_forupdate_by_name(db, "CT_INDEX", ptuple));
}

w_rc_t cash_transaction_man_impl::ct_update_name(ss_m* db, cash_transaction_tuple* ptuple, const char* ct_name, lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(3, ct_name);
    return (update_tuple(db, ptuple, lm));
}


/* ----------------- */
/* ---- CHARGE ----- */
/* ----------------- */

w_rc_t charge_man_impl::ch_index_probe(ss_m* db, charge_tuple* ptuple, const short cust_tier, const char* trade_type_id)
{
      assert (ptuple);    
      ptuple->set_value(0, trade_type_id);
      ptuple->set_value(1, cust_tier);
      return (index_probe_by_name(db, "CH_INDEX", ptuple));
}


/* ------------------------ */
/* --- COMMISSION_RATE ---- */
/* ------------------------ */

w_rc_t commission_rate_man_impl::cr_get_iter_by_index(ss_m* db,
                                                      commission_rate_index_iter* &iter,
                                                      commission_rate_tuple* ptuple,
                                                      rep_row_t &replow,
                                                      rep_row_t &rephigh,
                                                      const short c_tier, 
                                                      const char* type_id, 
                                                      const char* s_ex_id,
                                                      const int trade_qty,
                                                      lock_mode_t alm,
                                                      bool need_tuple)
{
    assert (ptuple);
    assert (_ptable);

    index_desc_t* pindex = _ptable->find_index("CR_INDEX");
    assert (pindex);
    
    // CR_INDEX: { 0, 1, 2, 3 }
    
    // prepare the key to be probed
    ptuple->set_value(0, c_tier);
    ptuple->set_value(1, type_id);
    ptuple->set_value(2, s_ex_id);
    ptuple->set_value(3, (int)0);
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);
    
    ptuple->set_value(3, trade_qty);
    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    
    
    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::le, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* ----------------- */
/* --- COMPANY ----- */
/* ----------------- */

w_rc_t company_man_impl::co_get_iter_by_index2(ss_m* db,
                                               company_index_iter* &iter,
                                               company_tuple* ptuple,
                                               rep_row_t &replow,
                                               rep_row_t &rephigh,
                                               const char* co_name,
                                               lock_mode_t alm,
                                               bool need_tuple)
{
    assert (ptuple);

    //find index
    assert (_ptable);    
    index_desc_t* pindex = _ptable->find_index("CO_INDEX_2");
    assert (pindex);

    // get the lowest key value 
    ptuple->set_value(2, co_name);
    ptuple->set_value(0, (long long)0);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    // get the highest key value 
    ptuple->set_value(0, MAX_ID);

    int highsz  = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);
    

    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


w_rc_t company_man_impl::co_index_probe(ss_m* db, company_tuple* ptuple, const TIdent co_id)
{
    assert (ptuple);    
    ptuple->set_value(0, co_id);
    return (index_probe_by_name(db, "CO_INDEX", ptuple));
}

w_rc_t company_man_impl::co_index_probe_forupdate(ss_m* db, company_tuple* ptuple, const TIdent co_id)
{
    assert (ptuple);    
    ptuple->set_value(0, co_id);
    return (index_probe_forupdate_by_name(db, "CO_INDEX", ptuple));
}

w_rc_t company_man_impl::co_update_sprate(ss_m* db, company_tuple* ptuple, const char* sprate, lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(4, sprate);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t company_man_impl::co_get_iter_by_index3(ss_m* db,
                                               company_index_iter* &iter,
                                               company_tuple* ptuple,
                                               rep_row_t &replow,
                                               rep_row_t &rephigh,
                                               const char* in_id,
                                               lock_mode_t alm,
                                               bool need_tuple)
{
    assert (ptuple);

    /* find index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("CO_INDEX_3");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(3, in_id);
    ptuple->set_value(0, (long long)0);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(0, MAX_ID);

    int highsz  = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::le, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* --------------------------- */
/* --- COMPANY_COMPETITOR ---- */
/* --------------------------- */

w_rc_t company_competitor_man_impl::cc_get_iter_by_index(ss_m* db,
                                                          company_competitor_index_iter* &iter,
                                                          company_competitor_tuple* ptuple,
                                                          rep_row_t &replow,
                                                          rep_row_t &rephigh,
                                                          const TIdent co_id,
                                                          bool need_tuple,
                                                          lock_mode_t alm)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("CP_INDEX");
    assert (pindex);

    // CP_INDEX: { 0, 1, 2 }

    // prepare the key to be probed
    ptuple->set_value(0, co_id);
    ptuple->set_value(1, (long long)0);
    ptuple->set_value(2, "");

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    ptuple->set_value(0, co_id + 1);
    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* ----------------- */
/* --- CUSTOMER ---- */
/* ----------------- */

w_rc_t customer_man_impl::c_index_probe(ss_m* db, customer_tuple* ptuple, const TIdent cust_id)
{
    assert (ptuple);    
    ptuple->set_value(0, cust_id);
    return (index_probe_by_name(db, "C_INDEX", ptuple));
}

w_rc_t customer_man_impl::c_index_probe_forupdate(ss_m* db, customer_tuple* ptuple, const TIdent cust_id)
{
    assert (ptuple);    
    ptuple->set_value(0, cust_id);
    return (index_probe_forupdate_by_name(db, "C_INDEX", ptuple));
}

w_rc_t customer_man_impl::c_update_email2(ss_m* db, customer_tuple* ptuple, const char* email2, lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(23, email2);    
    return (update_tuple(db, ptuple, lm));
}

w_rc_t customer_man_impl::c_get_iter_by_index2(ss_m* db,
					       customer_index_iter* &iter,
					       customer_tuple* ptuple,
					       rep_row_t &replow,
					       rep_row_t &rephigh,
					       const char* tax_id,
					       lock_mode_t alm,
					       bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("C_INDEX_2");
    assert (pindex);

    // C_INDEX_2: { 1 , 0 }

    // prepare the key to be probed
    ptuple->set_value(1, tax_id);
    ptuple->set_value(0, (long long) 0);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    ptuple->set_value(1, tax_id);
    ptuple->set_value(0, MAX_ID);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t customer_man_impl::c_get_iter_by_index3(ss_m* db,
					       customer_index_iter* &iter,
					       customer_tuple* ptuple,
					       rep_row_t &replow,
					       rep_row_t &rephigh,
					       const TIdent cust_id,
					       lock_mode_t alm,
					       bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("C_INDEX_3");
    assert (pindex);

    // C_INDEX_2: { 0 , 7 }

    // prepare the key to be probed
    ptuple->set_value(0, cust_id);
    ptuple->set_value(7, (short) 0);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    ptuple->set_value(0, cust_id);
    ptuple->set_value(7, (short) MAX_SHORT);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* ------------------------ */
/* --- CUSTOMER_ACCOUNT --- */
/* ------------------------ */

w_rc_t customer_account_man_impl::ca_index_probe(ss_m* db, customer_account_tuple* ptuple, const TIdent acct_id)
{
    assert (ptuple);    
    ptuple->set_value(0, acct_id);
    return (index_probe_by_name(db, "CA_INDEX", ptuple));
}

w_rc_t customer_account_man_impl::ca_update_bal(ss_m* db, customer_account_tuple* ptuple,
						const TIdent acct_id, const double se_amount,
						lock_mode_t lm)
{
    assert (ptuple);

    ptuple->set_value(0, acct_id);
    W_DO(index_probe_forupdate_by_name(db, "CA_INDEX", ptuple));
    
    double ca_bal;
    ptuple->get_value(5, ca_bal);
    ptuple->set_value(5, ca_bal + se_amount);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t customer_account_man_impl::ca_get_iter_by_index2(ss_m* db,
							customer_account_index_iter* &iter,
							customer_account_tuple* ptuple,
							rep_row_t &replow,
							rep_row_t &rephigh,
							const TIdent cust_id,
							lock_mode_t alm,
							bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("CA_INDEX_2");
    assert (pindex);

    // CA_INDEX_2: { 2 }

    // prepare the key to be probed
    ptuple->set_value(2, cust_id);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    ptuple->set_value(2, cust_id + 1);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* ------------------------ */
/* --- CUSTOMER_TAXRATE --- */
/* ------------------------ */

w_rc_t customer_taxrate_man_impl::cx_get_iter_by_index(ss_m* db,
                                                       customer_taxrate_index_iter* &iter,
                                                       customer_taxrate_tuple* ptuple,
                                                       rep_row_t &replow,
                                                       rep_row_t &rephigh,
                                                       const TIdent cust_id,
                                                       lock_mode_t alm,
                                                       bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("CX_INDEX");
    assert (pindex);

    // CX_INDEX: { 1 , 0 }

    // prepare the key to be probed
    ptuple->set_value(1, cust_id);
    ptuple->set_value(0, "");

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(0, temp);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t customer_taxrate_man_impl::cx_update_txid(ss_m* db,
                                                 customer_taxrate_tuple* ptuple,
                                                 const char* new_tax_rate,
                                                 lock_mode_t lm)
{
    assert (ptuple);
	
    // 1. fill the ptuple
    W_DO(read_tuple(ptuple));

    // 2. delete the old entry from the index
    W_DO(delete_index_entry(db, "CX_INDEX", ptuple));

    // 3. update the tuple
    ptuple->set_value(0, new_tax_rate);
    W_DO(update_tuple(db, ptuple, lm));

    // 4. add the updated entry to the index
    W_DO(add_index_entry(db, "CX_INDEX", ptuple));

    return (RCOK);
}


/* -----------------------*/
/* --- DAILY MARKET ----- */
/* ---------------------- */

w_rc_t daily_market_man_impl::dm_index_probe(ss_m* db, daily_market_tuple* ptuple,
					     const char* symbol, const myTime start_date)
{
    assert (ptuple);    
    ptuple->set_value(0, start_date);
    ptuple->set_value(1, symbol);
    return (index_probe_by_name(db, "DM_INDEX_2", ptuple));
}

w_rc_t daily_market_man_impl::dm_update_vol(ss_m* db,
                                            daily_market_tuple* ptuple,
                                            const int vol_incr,
                                            lock_mode_t lm)
{
    assert (ptuple);
    
    // 1. fill the ptuple
    W_DO(read_tuple(ptuple));

    // 2. update the tuple
    int ex_vol;
    ptuple->get_value(5, ex_vol);
    ptuple->set_value(5, (ex_vol + vol_incr));
    return (update_tuple(db, ptuple, lm));
}

w_rc_t daily_market_man_impl::dm_get_iter_by_index(ss_m* db,
                                                   daily_market_index_iter* &iter,
                                                   daily_market_tuple* ptuple,
                                                   rep_row_t &replow,
                                                   rep_row_t &rephigh,
                                                   const char* symbol,
                                                   const myTime start_day,
                                                   lock_mode_t alm,
                                                   bool need_tuple)
{
    assert (ptuple);

    //DM_INDEX { 1, 0 }
	
    /* find the index */
    
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("DM_INDEX");    
    assert (pindex);
    
    // get the lowest key value 
    ptuple->set_value(1, symbol);
    ptuple->set_value(0, start_day);
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    // get the highest key value 
    ptuple->set_value(0, MAX_DTS);
    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);
    
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
                                 scan_index_i::ge, vec_t(replow._dest, lowsz),
                                 scan_index_i::le, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* -----------------------*/
/* ------- EXCHANGE ----- */
/* ---------------------- */

w_rc_t exchange_man_impl::ex_update_desc(ss_m* db,
                                         exchange_tuple* ptuple,
                                         const char* new_desc,
                                         lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(5, new_desc);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t exchange_man_impl::ex_index_probe(ss_m* db,
                                         exchange_tuple* ptuple,
                                         const char* id)
{
    assert (ptuple);    
    ptuple->set_value(0, id);
    return (index_probe_by_name(db, "EX_INDEX", ptuple));
}


/* -----------------------*/
/* ------- FINANCIAL ---- */
/* ---------------------- */

w_rc_t financial_man_impl::fi_update_desc(ss_m* db, financial_tuple* ptuple,
					  const myTime new_qtr_start_date, lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(3, new_qtr_start_date);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t financial_man_impl::fi_get_iter_by_index(ss_m* db,
						financial_index_iter* &iter,
						financial_tuple* ptuple,
						rep_row_t &replow,
						rep_row_t &rephigh,
						const TIdent co_id,
						lock_mode_t alm,
						bool need_tuple)
{
    assert (ptuple);

    //FI_INDEX { 0, 1, 2 }
	
    /* find the index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("FI_INDEX");    
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(0, co_id);
    ptuple->set_value(1, (int)0);
    ptuple->set_value(2, (short)0);
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(0, co_id + 1);
    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    /* get the tuple iterator (index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
                                 scan_index_i::ge, vec_t(replow._dest, lowsz),
                                 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* -----------------*/
/* --- HOLDING ---- */
/* ---------------- */

w_rc_t holding_man_impl::h_get_iter_by_index2(ss_m* db,
                                              holding_index_iter* &iter,
                                              holding_tuple* ptuple,
                                              rep_row_t &replow,
                                              rep_row_t &rephigh,
                                              const TIdent acct_id,
                                              const char* symbol,
					      bool is_backward,
                                              lock_mode_t alm,
                                              bool need_tuple)
{
    assert (ptuple);

    //H_INDEX_2 { 1, 2, 3 }
	
    /* find the index */
    
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("H_INDEX_2");    
    assert (pindex);
    
    // get the lowest key value 
    ptuple->set_value(1, acct_id);
    ptuple->set_value(2, symbol);
    ptuple->set_value(3, (myTime)0);
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);
    
    // get the highest key value 
    ptuple->set_value(3, MAX_DTS);
    
    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    if(is_backward) {
	W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				     scan_index_i::le, vec_t(rephigh._dest, highsz),
				     scan_index_i::ge, vec_t(replow._dest, lowsz)));
    } else {
	W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				     scan_index_i::ge, vec_t(replow._dest, lowsz),
				     scan_index_i::le, vec_t(rephigh._dest, highsz)));
    }
    
    return (RCOK);
}

w_rc_t holding_man_impl::h_update_qty(ss_m* db,
                                      holding_tuple* ptuple,
                                      const int qty,
                                      lock_mode_t lm)
{
    // tuple is already retrieved
    assert (ptuple);
    ptuple->set_value(5, qty);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t holding_man_impl::h_delete_tuple(ss_m* db, holding_tuple* ptuple, rid_t rid)
{
    assert (ptuple);

    // 1. read tuple
    ptuple->set_rid(rid);
    W_DO(read_tuple(ptuple, EX));

    // 2. delete tuple
    W_DO(delete_tuple(db, ptuple));

    return (RCOK);
}


/* ------------------------ */
/* --- HOLDING_HISTORY ---- */
/* ------------------------ */

w_rc_t holding_history_man_impl::hh_get_iter_by_index2(ss_m* db,
						       holding_history_index_iter* &iter,
						       holding_history_tuple* ptuple,
						       rep_row_t &replow,
						       rep_row_t &rephigh,
						       TIdent trade_id,
						       lock_mode_t alm,
						       bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("HH_INDEX_2");
    assert (pindex);

    // HH_INDEX_2: { 1 }

    // prepare the key to be probed
    ptuple->set_value(1, trade_id);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    ptuple->set_value(1, trade_id+1);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* ------------------------ */
/* --- HOLDING_SUMMARY ---- */
/* ------------------------ */

w_rc_t holding_summary_man_impl::hs_index_probe(ss_m* db,
						holding_summary_tuple* ptuple,
						const TIdent acct_id,
						const char* symbol)
{
    assert (ptuple);    
    ptuple->set_value(0, acct_id);
    ptuple->set_value(1, symbol);
    return (index_probe_by_name(db, "HS_INDEX", ptuple));
}

w_rc_t holding_summary_man_impl::hs_update_qty(ss_m* db,
                                               holding_summary_tuple* ptuple,
					       const TIdent acct_id,
					       const char* symbol,
                                               const int qty,
                                               lock_mode_t lm)
{
    assert (ptuple);

    ptuple->set_value(0, acct_id);
    ptuple->set_value(1, symbol);
    W_DO(index_probe_forupdate_by_name(db, "HS_INDEX", ptuple));

    ptuple->set_value(2, qty);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t holding_summary_man_impl::hs_get_iter_by_index(ss_m* db,
                                                      holding_summary_index_iter* &iter,
                                                      holding_summary_tuple* ptuple,
                                                      rep_row_t &replow,
                                                      rep_row_t &rephigh,
                                                      const TIdent acct_id,
                                                      bool need_tuple,
                                                      lock_mode_t alm)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("HS_INDEX");
    assert (pindex);

    // HS_INDEX: { 0, 1 }

    // prepare the key to be probed
    ptuple->set_value(0, acct_id);
    ptuple->set_value(1, "");

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(1, temp);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* -------------------- */
/* ---- INDUSTRY ------ */
/* -------------------- */

w_rc_t industry_man_impl::in_index_probe(ss_m* db, industry_tuple* ptuple, const char* id)
{
    assert (ptuple);    
    ptuple->set_value(0, id);
    return (index_probe_by_name(db, "IN_INDEX", ptuple));
}

w_rc_t industry_man_impl::in_get_iter_by_index2(ss_m* db,
                                                industry_index_iter* &iter,
                                                industry_tuple* ptuple,
                                                rep_row_t &replow,
                                                rep_row_t &rephigh,
                                                const char* in_name,
                                                lock_mode_t alm,
                                                bool need_tuple)
{
    assert (ptuple);

    /* find the index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("IN_INDEX_2");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(1, in_name);
    ptuple->set_value(0, "");
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(0, temp);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    /* get the tuple iterator (index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t industry_man_impl::in_get_iter_by_index3(ss_m* db,
                                                industry_index_iter* &iter,
                                                industry_tuple* ptuple,
                                                rep_row_t &replow,
                                                rep_row_t &rephigh,
                                                const char* sc_id,
                                                lock_mode_t alm,
                                                bool need_tuple)
{
    assert (ptuple);

    /* find the index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("IN_INDEX_3");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(2, sc_id);
    ptuple->set_value(0, "");
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(0, temp);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    /* get the tuple iterator (index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* --------------------- */
/* ----- LAST_TRADE ---- */
/* --------------------- */

w_rc_t last_trade_man_impl::lt_index_probe(ss_m* db, last_trade_tuple* ptuple, const char* symbol)
{
    assert (ptuple);    
    ptuple->set_value(0, symbol);
    return (index_probe_by_name(db, "LT_INDEX", ptuple));
}

w_rc_t last_trade_man_impl::lt_update_by_index(ss_m* db, last_trade_tuple* ptuple, 
                                               const char* symbol,
                                               const double price_quote,
                                               const int trade_qty,
                                               const myTime now_dts,
                                               lock_mode_t lm)
{
    assert (ptuple);

    // 1. idx probe for update the last_trade
    // 2. update table

    ptuple->set_value(0, symbol);
    W_DO(index_probe_forupdate_by_name(db, "LT_INDEX", ptuple));    
    
    double lt_vol;
    ptuple->get_value(4, lt_vol);    
    
    ptuple->set_value(4, lt_vol + (double)trade_qty);    
    ptuple->set_value(2, price_quote);
    ptuple->set_value(1, now_dts);
    return (update_tuple(db, ptuple, lm));
}


/* ----------------- */
/* --- SECURITY ---- */
/* ----------------- */

w_rc_t security_man_impl::s_index_probe(ss_m* db, security_tuple* ptuple, const char* symbol)
{
    assert (ptuple);    
    ptuple->set_value(0, symbol);
    return (index_probe_by_name(db, "S_INDEX", ptuple));
}

w_rc_t security_man_impl::s_get_iter_by_index4(ss_m* db,
                                               security_index_iter* &iter,
                                               security_tuple* ptuple,
                                               rep_row_t &replow,
                                               rep_row_t &rephigh,
                                               const TIdent co_id,
					       lock_mode_t alm,
					       bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    
    index_desc_t* pindex = _ptable->find_index("S_INDEX_4");
    assert (pindex);
    
    // S_INDEX_4: { 5, 1, 0 }

    // prepare the key to be probed
    ptuple->set_value(5, co_id);
    ptuple->set_value(1, "");
    ptuple->set_value(0, "");
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(1, temp);
    ptuple->set_value(0, temp);
    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest); 
    
    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t security_man_impl::s_get_iter_by_index4(ss_m* db,
                                               security_index_iter* &iter,
                                               security_tuple* ptuple,
                                               rep_row_t &replow,
                                               rep_row_t &rephigh,
                                               const TIdent co_id,
					       const char* issue,
					       lock_mode_t alm,
					       bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    
    index_desc_t* pindex = _ptable->find_index("S_INDEX_4");
    assert (pindex);
    
    // S_INDEX_4: { 5, 1, 0 }

    // prepare the key to be probed
    ptuple->set_value(5, co_id);
    ptuple->set_value(1, issue);
    ptuple->set_value(0, "");
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(0, temp);
    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest); 
    
    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t security_man_impl::s_get_iter_by_index(ss_m* db,
                                              security_index_iter* &iter,
                                              security_tuple* ptuple,
                                              rep_row_t &replow,
                                              rep_row_t &rephigh,
                                              const char* symbol,
                                              lock_mode_t alm,
                                              bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("S_INDEX");
    assert (pindex);

    // S_INDEX: { 0 }

    // prepare the key to be probed
    ptuple->set_value(0, symbol); 

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(0, temp);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t security_man_impl::s_index_probe_forupdate(ss_m* db, 
                                                  security_tuple* ptuple,
                                                  const char* symbol)
{
    assert (ptuple);    
    ptuple->set_value(0, symbol);
    return (index_probe_forupdate_by_name(db, "S_INDEX", ptuple));
}

w_rc_t security_man_impl::s_update_ed(ss_m* db, security_tuple* ptuple, const myTime exch_date, lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(8, exch_date);
    return (update_tuple(db, ptuple, lm));
}


/* ------------------- */
/* --- NEWS_ITEM ----- */
/* ------------------- */

w_rc_t news_item_man_impl::ni_index_probe(ss_m* db, news_item_tuple* ptuple, const TIdent ni_id)
{
    assert (ptuple);    
    ptuple->set_value(0, ni_id);
    return (index_probe_by_name(db, "NI_INDEX", ptuple));
}

w_rc_t news_item_man_impl::ni_index_probe_forupdate(ss_m* db, 
                                                    news_item_tuple* ptuple,
                                                    const TIdent ni_id)
{
    assert (ptuple);    
    ptuple->set_value(0, ni_id);
    return (index_probe_forupdate_by_name(db, "NI_INDEX", ptuple));
}

w_rc_t news_item_man_impl::ni_update_dts_by_index(ss_m* db,
                                                  news_item_tuple* ptuple,
                                                  const myTime ni_dts,
                                                  lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(4, ni_dts);
    return (update_tuple(db, ptuple, lm));
}


/* ------------------- */
/* --- NEWS_XREF ----- */
/* ------------------- */

w_rc_t news_xref_man_impl::nx_get_table_iter(ss_m* db, table_iter* &iter)						  
{
    /* get the tuple iterator (table scan) */
    W_DO(get_iter_for_file_scan(db, iter));
    return (RCOK);
}

w_rc_t news_xref_man_impl::nx_get_iter_by_index(ss_m* db,
                                                news_xref_index_iter* &iter,
                                                news_xref_tuple* ptuple,
                                                rep_row_t &replow,
                                                rep_row_t &rephigh,
                                                const TIdent co_id,
						lock_mode_t alm,
						bool need_tuple)
{
    assert (ptuple);

    /* find the index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("NX_INDEX");    
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(1, co_id);
    ptuple->set_value(0, (long long)0);
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(0, MAX_ID);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    /* get the tuple iterator (index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
                                 scan_index_i::ge, vec_t(replow._dest, lowsz),
                                 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* ------------------- */
/* --- STATUS_TYPE --- */
/* ------------------- */

w_rc_t status_type_man_impl::st_index_probe(ss_m* db, status_type_tuple* ptuple, const char* st_id)
{
    assert (ptuple);    
    ptuple->set_value(0, st_id);
    return (index_probe_by_name(db, "ST_INDEX", ptuple));
}


/* --------------- */
/* --- SECTOR ---- */
/* --------------- */

w_rc_t sector_man_impl::sc_get_iter_by_index2(ss_m* db,
					      sector_index_iter* &iter,
					      sector_tuple* ptuple,
					      rep_row_t &replow,
					      rep_row_t &rephigh,
					      const char* sc_name,
					      lock_mode_t alm,
					      bool need_tuple)
{
    assert (ptuple);

    /* find the index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("SC_INDEX_2");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(1, sc_name);
    ptuple->set_value(0, "");
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(0, temp);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    /* get the tuple iterator (index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* ------------------- */
/* --- SETTLEMENT ---- */
/* ------------------- */

w_rc_t settlement_man_impl::se_index_probe(ss_m* db, settlement_tuple* ptuple,const TIdent tr_id)
{
    assert (ptuple);    
    ptuple->set_value(0, tr_id);
    return (index_probe_by_name(db, "SE_INDEX", ptuple));
}

w_rc_t settlement_man_impl::se_index_probe_forupdate(ss_m* db, settlement_tuple* ptuple, const TIdent tr_id)
{
    assert (ptuple);    
    ptuple->set_value(0, tr_id);
    return (index_probe_forupdate_by_name(db, "SE_INDEX", ptuple));
}

w_rc_t settlement_man_impl::se_update_name(ss_m* db, settlement_tuple* ptuple, const char* cash_type, lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(1, cash_type);
    return (update_tuple(db, ptuple, lm));
}


/* -------------------- */
/* ---- TAXRATE ------- */
/* -------------------- */

w_rc_t taxrate_man_impl::tx_index_probe(ss_m* db, taxrate_tuple* ptuple, const char* tx_id)
{
    assert (ptuple);    
    ptuple->set_value(0, tx_id);
    return (index_probe_by_name(db, "TX_INDEX", ptuple));
}

w_rc_t taxrate_man_impl::tx_index_probe_forupdate(ss_m* db, taxrate_tuple* ptuple, const char* tx_id)
{
    assert (ptuple);    
    ptuple->set_value(0, tx_id);
    return (index_probe_forupdate_by_name(db, "TX_INDEX", ptuple));
}

w_rc_t taxrate_man_impl::tx_update_name(ss_m* db, taxrate_tuple* ptuple, const char* tx_name, lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(1, tx_name);
    return (update_tuple(db, ptuple, lm));
}


/* --------------- */
/* ---- TRADE ---- */
/* --------------- */

w_rc_t trade_man_impl::t_index_probe(ss_m* db, trade_tuple* ptuple, const TIdent trade_id)
{
    assert (ptuple);    
    ptuple->set_value(0, trade_id);
    return (index_probe_by_name(db, "T_INDEX", ptuple));
}

w_rc_t trade_man_impl::t_update_tax_by_index(ss_m* db,
                                             trade_tuple* ptuple,
                                             const TIdent t_id,
                                             const double tax_amount,
                                             lock_mode_t lm)
{
    assert (ptuple);

    // 1. idx probe for update the trade
    // 2. update taxamount and update table
    ptuple->set_value(0, t_id);
    W_DO(index_probe_forupdate_by_name(db, "T_INDEX", ptuple));

    ptuple->set_value(13, tax_amount);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t trade_man_impl::t_update_dts_stdid_by_index(ss_m* db,
                                                   trade_tuple* ptuple,
                                                   const TIdent req_trade_id,
                                                   const myTime now_dts,
                                                   const char* status_submitted,
                                                   lock_mode_t lm)
{
    assert (ptuple);

    /* @note: PIN: since one of the updated columns (dts) is a part of
     * the secondary indexes for this table,
     * we cannot simply go and update this column,
     * we need to update the secondary indexes as well,
     * the simplest way to do this is to first delete this tuple's entry from
     * the secondary indexes and insert it again with the updated columns
     */
    
    // 1. idx probe for update the trade
    ptuple->set_value(0, req_trade_id);
    W_DO(index_probe_forupdate_by_name(db, "T_INDEX", ptuple));
        
    // 2. delete the old entry from the secondary indexes
    W_DO(delete_index_entry(db, "T_INDEX_2", ptuple));
    W_DO(delete_index_entry(db, "T_INDEX_3", ptuple));
    
    // 3. update the tuple
    ptuple->set_value(1, now_dts);
    ptuple->set_value(2, status_submitted);
    W_DO(update_tuple(db, ptuple, lm));
    
    // 4. add the updated entry to the secondary indexes
    W_DO(add_index_entry(db, "T_INDEX_2", ptuple));
    W_DO(add_index_entry(db, "T_INDEX_3", ptuple));
    
    return (RCOK);
}

w_rc_t trade_man_impl::t_update_ca_td_sci_tp_by_index(ss_m* db,
                                                      trade_tuple* ptuple,
                                                      const TIdent trade_id,
                                                      const double comm_amount,
                                                      const myTime trade_dts,
                                                      const char* st_completed_id,
                                                      const double trade_price,
                                                      lock_mode_t lm)
{
    assert (ptuple);

    // 1. idx probe for update the trade
    ptuple->set_value(0, trade_id);
    W_DO(index_probe_forupdate_by_name(db, "T_INDEX", ptuple));

    // 2. delete the old entry from the secondary indexes
    W_DO(delete_index_entry(db, "T_INDEX_2", ptuple));
    W_DO(delete_index_entry(db, "T_INDEX_3", ptuple));

    // 3. update the tuple
    ptuple->set_value(1, trade_dts);
    ptuple->set_value(2, st_completed_id);
    ptuple->set_value(10, trade_price);
    ptuple->set_value(12, comm_amount);
    W_DO(update_tuple(db, ptuple, lm));
    
    // 4. add the updated entry to the secondary indexes
    W_DO(add_index_entry(db, "T_INDEX_2", ptuple));
    W_DO(add_index_entry(db, "T_INDEX_3", ptuple));

    return (RCOK);
}

w_rc_t trade_man_impl::t_get_iter_by_index2(ss_m* db,
                                            trade_index_iter* &iter,
                                            trade_tuple* ptuple,
                                            rep_row_t &replow,
                                            rep_row_t &rephigh,
                                            const TIdent acct_id,
                                            const myTime start_dts,
                                            const myTime end_dts,
					    bool is_backward,
                                            bool need_tuple,
                                            lock_mode_t alm)
{
    assert (ptuple);

    // T_INDEX_2 { 8, 1, 0 }

    /* pointer to the index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("T_INDEX_2");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(8, acct_id);
    ptuple->set_value(1, start_dts);
    ptuple->set_value(0, (long long) 0);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    // get the highest key value
    //ptuple->set_value(8, acct_id+1);
    ptuple->set_value(1, MAX_DTS);
    ptuple->set_value(0, MAX_ID);
    
    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);
    
    /* get the tuple iterator (not index only scan) */
    if(is_backward) {
	W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				     scan_index_i::le, vec_t(rephigh._dest, highsz),
				     scan_index_i::ge, vec_t(replow._dest, lowsz)));
    } else {
	W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				     scan_index_i::ge, vec_t(replow._dest, lowsz),
				     scan_index_i::le, vec_t(rephigh._dest, highsz)));
    }
    
    return (RCOK);
}

w_rc_t trade_man_impl::t_get_iter_by_index3(ss_m* db,
                                            trade_index_iter* &iter,
                                            trade_tuple* ptuple,
                                            rep_row_t &replow,
                                            rep_row_t &rephigh,
                                            const char* symbol,
                                            const myTime start_dts,
                                            const myTime end_dts,
                                            lock_mode_t alm,
                                            bool need_tuple)
{
    assert (ptuple);

    //alignment problem
    
    // T_INDEX_3 { 5, 1 }

    // pointer to the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("T_INDEX_3");
    assert (pindex);

    //get the lowest key value
    ptuple->set_value(5, symbol);
    ptuple->set_value(1, start_dts);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    // get the highest key value
    ptuple->set_value(5, symbol);
    ptuple->set_value(1, end_dts);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);
    

    /* get the tuple iterator (not index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::le, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t trade_man_impl::t_index_probe_forupdate(ss_m* db, trade_tuple* ptuple, const TIdent t_id)
{
    assert (ptuple);    
    ptuple->set_value(0, t_id);
    return (index_probe_forupdate_by_name(db, "T_INDEX", ptuple));
}

w_rc_t trade_man_impl::t_update_name(ss_m* db, trade_tuple* ptuple, const char* exec_name, lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(9, exec_name);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t trade_man_impl::t_get_iter_by_index(ss_m* db,
                                           trade_index_iter* &iter,
                                           trade_tuple* ptuple,
                                           rep_row_t &replow,
                                           rep_row_t &rephigh,
                                           const TIdent t_id,
                                           lock_mode_t alm,
                                           bool need_tuple)
{
    assert (ptuple);

    // T_INDEX { 0 }

    /* pointer to the index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("T_INDEX");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(0, t_id);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(0, MAX_ID);
    
    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);
    
    /* get the tuple iterator (not index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::le, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* ----------------------- */
/* ---- TRADE_HISTORY ---- */
/* ----------------------- */

w_rc_t trade_history_man_impl::th_get_iter_by_index(ss_m* db,
                                                    trade_history_index_iter* &iter,
                                                    trade_history_tuple* ptuple,
                                                    rep_row_t &replow,
                                                    rep_row_t &rephigh,
                                                    const TIdent t_id,
                                                    lock_mode_t alm,
                                                    bool need_tuple)
{
    assert (ptuple);

    /* find the index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("TH_INDEX_3");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(0, t_id);
    ptuple->set_value(1, (long long) 0);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(0, t_id+1);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    /* get the tuple iterator (index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* ----------------------- */
/* ---- TRADE_REQUEST ---- */
/* ----------------------- */

w_rc_t trade_request_man_impl::tr_delete_tuple(ss_m* db, trade_request_tuple* ptuple, rid_t rid)
{
    assert (ptuple);

    // 1. read tuple
    ptuple->set_rid(rid);
    W_DO(read_tuple(ptuple, EX));

    // 2. delete tuple
    W_DO(delete_tuple(db, ptuple));

    return (RCOK);    
}

w_rc_t trade_request_man_impl::tr_get_iter_by_index4(ss_m* db,
                                                     trade_request_index_iter* &iter,
                                                     trade_request_tuple* ptuple,
                                                     rep_row_t &replow,
                                                     rep_row_t &rephigh,
                                                     const char* tr_s_symb, 
                                                     const TIdent tr_b_id,
                                                     lock_mode_t alm,
                                                     bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("TR_INDEX_4");
    assert (pindex);

    // TR_INDEX_2: { 2, 5 }

    // prepare the key to be probed
    ptuple->set_value(2, tr_s_symb);
    ptuple->set_value(5, tr_b_id);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    ptuple->set_value(5, tr_b_id+1);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    
    
    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t trade_request_man_impl::tr_get_iter_by_index4(ss_m* db,
                                                     trade_request_index_iter* &iter,
                                                     trade_request_tuple* ptuple,
                                                     rep_row_t &replow,
                                                     rep_row_t &rephigh,
                                                     const char* tr_s_symb, 
                                                     lock_mode_t alm,
                                                     bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("TR_INDEX_4");
    assert (pindex);

    // TR_INDEX_2: { 2, 5 }

    // prepare the key to be probed
    ptuple->set_value(2, tr_s_symb);
    ptuple->set_value(5, (long long) 0);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    ptuple->set_value(5, MAX_ID);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    
    
    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter, alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* -------------------- */
/* ---- TRADE_TYPE ---- */
/* -------------------- */

w_rc_t trade_type_man_impl::tt_index_probe(ss_m* db,
                                           trade_type_tuple* ptuple,
                                           const char* trade_type_id)
{
    assert (ptuple);    
    ptuple->set_value(0, trade_type_id);
    return (index_probe_by_name(db, "TT_INDEX", ptuple));
}   


/* -------------------- */
/* ---- WATCH_ITEM ---- */
/* -------------------- */

w_rc_t watch_item_man_impl::wi_update_symb(ss_m* db, watch_item_tuple* ptuple, const TIdent wl_id,
					   const char* old_symbol, const char* new_symbol, lock_mode_t lm)
{
    // 1. find the tuple from the index
    assert (ptuple);    
    ptuple->set_value(0, wl_id);
    ptuple->set_value(1, old_symbol);
    W_DO(index_probe_forupdate_by_name(db, "WI_INDEX", ptuple));

    // 2. delete the old entry from the index
    W_DO(delete_index_entry(db, "WI_INDEX", ptuple));

    // 3. update the tuple
    ptuple->set_value(1, new_symbol);
    W_DO(update_tuple(db, ptuple, lm));

    // 4. add the updated entry to the index
    W_DO(add_index_entry(db, "WI_INDEX", ptuple));
}

w_rc_t watch_item_man_impl::wi_get_iter_by_index(ss_m* db,
                                                 watch_item_index_iter* &iter,
                                                 watch_item_tuple* ptuple,
                                                 rep_row_t &replow,
                                                 rep_row_t &rephigh,
                                                 const TIdent wl_id,
                                                 lock_mode_t alm,
                                                 bool need_tuple)
{
    assert (ptuple);

    /* find the index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("WI_INDEX");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(0, wl_id);
    ptuple->set_value(1, "");
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(1, temp);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    /* get the tuple iterator (index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* -------------------- */
/* ---- WATCH_LIST ---- */
/* -------------------- */

w_rc_t watch_list_man_impl::wl_get_iter_by_index2(ss_m* db,
                                                  watch_list_index_iter* &iter,
                                                  watch_list_tuple* ptuple,
                                                  rep_row_t &replow,
                                                  rep_row_t &rephigh,
                                                  const TIdent c_id,
                                                  lock_mode_t alm,
                                                  bool need_tuple)
{
    assert (ptuple);

    /* find the index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("WL_INDEX_2");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(1, c_id);
    ptuple->set_value(0, (long long)0);
    
    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(0, MAX_ID);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    /* get the tuple iterator (index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* -------------------- */
/* ---- ZIP_CODE ---- */
/* -------------------- */

w_rc_t zip_code_man_impl::zc_index_probe(ss_m* db,
                                         zip_code_tuple* ptuple,
                                         const char* zc_code)
{
    assert (ptuple);    
    ptuple->set_value(0, zc_code);
    return (index_probe_by_name(db, "ZC_INDEX", ptuple));
}   


EXIT_NAMESPACE(tpce);
