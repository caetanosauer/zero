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

/** @file:   shore_tpcc_schema.h
 *
 *  @brief:  Implementation of the workload-specific access methods 
 *           on TPC-C tables
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#include "workload/tpcc/shore_tpcc_schema_man.h"

using namespace shore;


ENTER_NAMESPACE(tpcc);



/*********************************************************************
 *
 * Workload-specific access methods on tables
 *
 *********************************************************************/


/* ----------------- */
/* --- WAREHOUSE --- */
/* ----------------- */


w_rc_t 
warehouse_man_impl::wh_index_probe(ss_m* db,
                                   warehouse_tuple* ptuple,
                                   const int w_id)
{
    assert (ptuple);    
    ptuple->set_value(0, w_id);
    return (index_probe_by_name(db, "W_IDX", ptuple));
}


w_rc_t 
warehouse_man_impl::wh_index_probe_forupdate(ss_m* db,
                                             warehouse_tuple* ptuple,
                                             const int w_id)
{
    assert (ptuple);    
    ptuple->set_value(0, w_id);
    return (index_probe_forupdate_by_name(db, "W_IDX", ptuple));
}


w_rc_t 
warehouse_man_impl::wh_index_probe_nl(ss_m* db,
                                      warehouse_tuple* ptuple,
                                      const int w_id)
{
    assert (ptuple);    
    ptuple->set_value(0, w_id);
    return (index_probe_nl_by_name(db, "W_IDX", ptuple));
}


w_rc_t 
warehouse_man_impl::wh_update_ytd(ss_m* db,
                                  warehouse_tuple* ptuple,
                                  const double amount,
                                  lock_mode_t lm)
{ 
    // 1. increases the ytd of the warehouse and updates the table

    assert (ptuple);
    assert (ptuple->is_rid_valid());

    double ytd;
    ptuple->get_value(8, ytd);
    ytd += amount;
    ptuple->set_value(8, ytd);
    W_DO(update_tuple(db, ptuple, lm));
    return (RCOK);
}


w_rc_t 
warehouse_man_impl::wh_update_ytd_nl(ss_m* db,
                                     warehouse_tuple* ptuple,
                                     const double amount)
{ 
    return (wh_update_ytd(db,ptuple,amount,NL));
}



/* ---------------- */
/* --- DISTRICT --- */
/* ---------------- */


w_rc_t district_man_impl::dist_index_probe(ss_m* db,
                                           district_tuple* ptuple,
                                           const int w_id,
                                           const int d_id)
{
    assert (ptuple);
    ptuple->set_value(0, d_id);
    ptuple->set_value(1, w_id);
    return (index_probe_by_name(db, "D_IDX", ptuple));
}

w_rc_t district_man_impl::dist_index_probe_forupdate(ss_m* db,
                                                     district_tuple* ptuple,
                                                     const int w_id,
                                                     const int d_id)
{
    assert (ptuple);
    ptuple->set_value(0, d_id);
    ptuple->set_value(1, w_id);
    return (index_probe_forupdate_by_name(db, "D_IDX", ptuple));
}

w_rc_t district_man_impl::dist_index_probe_nl(ss_m* db,
                                              district_tuple* ptuple,
                                              const int w_id,
                                              const int d_id)
{
    assert (ptuple);
    ptuple->set_value(0, d_id);
    ptuple->set_value(1, w_id);
    return (index_probe_nl_by_name(db, "D_IDX", ptuple));
}

w_rc_t district_man_impl::dist_update_ytd(ss_m* db,
                                          district_tuple* ptuple,
                                          const double amount,
                                          lock_mode_t lm)
{
    // 1. increases the ytd of the district and updates the table

    assert (ptuple);
    assert (ptuple->is_rid_valid());

    double d_ytd;
    ptuple->get_value(9, d_ytd);
    d_ytd += amount;
    ptuple->set_value(9, d_ytd);
    W_DO(update_tuple(db, ptuple, lm));
    return (RCOK);
}

w_rc_t district_man_impl::dist_update_ytd_nl(ss_m* db,
                                             district_tuple* ptuple,
                                             const double amount)
{
    return (dist_update_ytd(db,ptuple,amount,NL));
}

w_rc_t district_man_impl::dist_update_next_o_id(ss_m* db,
                                                district_tuple* ptuple,
                                                const int next_o_id,
                                                lock_mode_t lm)
{
    // 1. updates the next order id of the district

    assert (ptuple);
    assert (ptuple->is_rid_valid());

    ptuple->set_value(10, next_o_id);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t district_man_impl::dist_update_next_o_id_nl(ss_m* db,
                                                   district_tuple* ptuple,
                                                   const int next_o_id)
{
    return (dist_update_next_o_id(db,ptuple,next_o_id,NL));
}



/* ---------------- */
/* --- CUSTOMER --- */
/* ---------------- */


w_rc_t customer_man_impl::cust_get_iter_by_index(ss_m* db,
                                                 customer_index_iter* &iter,
                                                 customer_tuple* ptuple,
                                                 rep_row_t &replow,
                                                 rep_row_t &rephigh,
                                                 const int w_id,
                                                 const int d_id,
                                                 const char* c_last,
                                                 lock_mode_t alm,
                                                 bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("C_NAME_IDX");
    assert (pindex);

    // C_NAME_IDX: {2 - 1 - 5 - 3 - 0}

    // prepare the key to be probed
    ptuple->set_value(0, 0);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    ptuple->set_value(3, "");
    ptuple->set_value(5, c_last);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(3, temp);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


w_rc_t customer_man_impl::cust_get_iter_by_index_nl(ss_m* db,
                                                    customer_index_iter* &iter,
                                                    customer_tuple* ptuple,
                                                    rep_row_t &replow,
                                                    rep_row_t &rephigh,
                                                    const int w_id,
                                                    const int d_id,
                                                    const char* c_last,
                                                    bool need_tuple)
{
    return (cust_get_iter_by_index(db,iter,ptuple,replow,rephigh,w_id,d_id,c_last,NL,need_tuple));
}


w_rc_t customer_man_impl::cust_index_probe(ss_m* db,
                                           customer_tuple* ptuple,
                                           const int w_id,
                                           const int d_id,
                                           const int c_id)
{
    assert (ptuple);
    return (cust_index_probe_by_name(db, "C_IDX", ptuple, w_id, d_id, c_id));
}

w_rc_t customer_man_impl::cust_index_probe_by_name(ss_m* db,
                                                   const char* idx_name,
                                                   customer_tuple* ptuple,
                                                   const int w_id,
                                                   const int d_id,
                                                   const int c_id)
{
    assert (idx_name);
    ptuple->set_value(0, c_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    return (index_probe_by_name(db, idx_name, ptuple));
}

w_rc_t customer_man_impl::cust_index_probe_forupdate(ss_m * db,
                                                     customer_tuple* ptuple,
                                                     const int w_id,
                                                     const int d_id,
                                                     const int c_id)
{
    assert (ptuple);
    ptuple->set_value(0, c_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    return (index_probe_forupdate_by_name(db, "C_IDX", ptuple));
}

w_rc_t customer_man_impl::cust_index_probe_nl(ss_m * db,
                                              customer_tuple* ptuple,
                                              const int w_id,
                                              const int d_id,
                                              const int c_id)
{
    assert (ptuple);
    ptuple->set_value(0, c_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    return (index_probe_nl_by_name(db, "C_IDX", ptuple));
}

w_rc_t customer_man_impl::cust_update_tuple(ss_m* db,
                                            customer_tuple* ptuple,
                                            const tpcc_customer_tuple& acustomer,
                                            const char* adata1,
                                            const char* adata2,
                                            lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(16, acustomer.C_BALANCE);
    ptuple->set_value(17, acustomer.C_YTD_PAYMENT);
    ptuple->set_value(19, acustomer.C_PAYMENT_CNT);

    if (adata1)
	ptuple->set_value(20, adata1);

    if (adata2)
	ptuple->set_value(21, adata2);

    return (update_tuple(db, ptuple, lm));
}


w_rc_t customer_man_impl::cust_update_tuple_nl(ss_m* db,
                                               customer_tuple* ptuple,
                                               const tpcc_customer_tuple& acustomer,
                                               const char* adata1,
                                               const char* adata2)
{
    return (cust_update_tuple(db,ptuple,acustomer,adata1,adata2,NL));
}


w_rc_t customer_man_impl::cust_update_discount_balance(ss_m* db,
                                                       customer_tuple* ptuple,
                                                       const decimal discount,
                                                       const decimal balance,
                                                       lock_mode_t lm)
{
    assert (ptuple);
    assert (discount>=0);
    ptuple->set_value(15, discount);
    ptuple->set_value(16, balance);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t customer_man_impl::cust_update_discount_balance_nl(ss_m* db,
                                                          customer_tuple* ptuple,
                                                          const decimal discount,
                                                          const decimal balance)
{
    return (cust_update_discount_balance(db,ptuple,discount,balance,NL));
}



/* ----------------- */
/* --- NEW_ORDER --- */
/* ----------------- */

				    
w_rc_t new_order_man_impl::no_get_iter_by_index(ss_m* db,
                                                new_order_index_iter* &iter,
                                                new_order_tuple* ptuple,
                                                rep_row_t &replow,
                                                rep_row_t &rephigh,
                                                const int w_id,
                                                const int d_id,
                                                lock_mode_t alm,
                                                bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("NO_IDX");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(0, 0);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(1, d_id+1);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    /* get the tuple iterator (index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


w_rc_t new_order_man_impl::no_get_iter_by_index_nl(ss_m* db,
                                                   new_order_index_iter* &iter,
                                                   new_order_tuple* ptuple,
                                                   rep_row_t &replow,
                                                   rep_row_t &rephigh,
                                                   const int w_id,
                                                   const int d_id,
                                                   bool need_tuple)
{
    return (no_get_iter_by_index(db,iter,ptuple,replow,rephigh,w_id,d_id,NL,need_tuple));
}


w_rc_t new_order_man_impl::no_delete_by_index(ss_m* db,
                                              new_order_tuple* ptuple,
                                              const int w_id,
                                              const int d_id,
                                              const int o_id)
{
    assert (ptuple);

    // 1. idx probe new_order
    // 2. deletes the retrieved new_order

    ptuple->set_value(0, o_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
//     W_DO(index_probe_forupdate_by_name(db, "NO_IDX", ptuple));
    W_DO(delete_tuple(db, ptuple));

    return (RCOK);
}

w_rc_t new_order_man_impl::no_delete_by_index_nl(ss_m* db,
                                                 new_order_tuple* ptuple,
                                                 const int w_id,
                                                 const int d_id,
                                                 const int o_id)
{
    // !!! NO-LOCK version !!!

    assert (ptuple);

    // 1. idx probe new_order
    // 2. deletes the retrieved new_order

    ptuple->set_value(0, o_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    //W_DO(index_probe_nl_by_name(db, "NO_IDX", ptuple));
    W_DO(delete_tuple(db, ptuple, NL));

    return (RCOK);
}



/* ------------- */
/* --- ORDER --- */
/* ------------- */


w_rc_t order_man_impl::ord_get_iter_by_index(ss_m* db,
                                             order_index_iter* &iter,
                                             order_tuple* ptuple,
                                             rep_row_t &replow,
                                             rep_row_t &rephigh,
                                             const int w_id,
                                             const int d_id,
                                             const int c_id,
                                             lock_mode_t alm,
                                             bool need_tuple)
{
    assert (ptuple);

    // find index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("O_CUST_IDX");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(0, 0);
    ptuple->set_value(1, c_id);
    ptuple->set_value(2, d_id);
    ptuple->set_value(3, w_id);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(1, c_id+1);

    int highsz  = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


w_rc_t order_man_impl::ord_get_iter_by_index_nl(ss_m* db,
                                                order_index_iter* &iter,
                                                order_tuple* ptuple,
                                                rep_row_t &replow,
                                                rep_row_t &rephigh,
                                                const int w_id,
                                                const int d_id,
                                                const int c_id,
                                                bool need_tuple)
{
    return (ord_get_iter_by_index(db,iter,ptuple,replow,rephigh,w_id,d_id,c_id,NL,need_tuple));
}


w_rc_t order_man_impl::ord_update_carrier_by_index(ss_m* db,
                                                   order_tuple* ptuple,
                                                   const int carrier_id)
{
    assert (ptuple);

    // 1. idx probe for update the order
    // 2. update carrier_id and update table

    W_DO(index_probe_forupdate_by_name(db, "O_IDX", ptuple));

    ptuple->set_value(5, carrier_id);
    W_DO(update_tuple(db, ptuple));

    return (RCOK);
}

w_rc_t order_man_impl::ord_update_carrier_by_index_nl(ss_m* db,
                                                      order_tuple* ptuple,
                                                      const int carrier_id)
{
    // !!! NO-LOCK version !!!

    assert (ptuple);

    // 1. idx probe the order
    // 2. update carrier_id and update table

    W_DO(index_probe_nl_by_name(db, "O_IDX", ptuple));

    ptuple->set_value(5, carrier_id);
    W_DO(update_tuple(db, ptuple, NL));

    return (RCOK);
}



/* ----------------- */
/* --- ORDERLINE --- */
/* ----------------- */


w_rc_t order_line_man_impl::ol_get_range_iter_by_index(ss_m* db,
                                                       order_line_index_iter* &iter,
                                                       order_line_tuple* ptuple,
                                                       rep_row_t &replow,
                                                       rep_row_t &rephigh,
                                                       const int w_id,
                                                       const int d_id,
                                                       const int low_o_id,
                                                       const int high_o_id,
                                                       lock_mode_t alm,
                                                       bool need_tuple)
{
    assert (ptuple);

    // OL_IDX - { 2, 1, 0, 3 } = { OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER }

    // pointer to the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("OL_IDX");
    assert (pindex);

    // get the lowest key value
    ptuple->set_value(0, low_o_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    ptuple->set_value(3, (int)0);  /* assuming that ol_number starts from 1 */

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    // get the highest key value
    ptuple->set_value(0, high_o_id+1);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);
    
    // get the tuple iterator (not index only scan)
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


w_rc_t order_line_man_impl::ol_get_range_iter_by_index_nl(ss_m* db,
                                                          order_line_index_iter* &iter,
                                                          order_line_tuple* ptuple,
                                                          rep_row_t &replow,
                                                          rep_row_t &rephigh,
                                                          const int w_id,
                                                          const int d_id,
                                                          const int low_o_id,
                                                          const int high_o_id,
                                                          bool need_tuple)
{
    return (ol_get_range_iter_by_index(db,iter,ptuple,replow,rephigh,
                                       w_id,d_id,low_o_id,high_o_id,NL,need_tuple));
}



w_rc_t order_line_man_impl::ol_get_probe_iter_by_index(ss_m* db,
                                                       order_line_index_iter* &iter,
                                                       order_line_tuple* ptuple,
                                                       rep_row_t &replow,
                                                       rep_row_t &rephigh,
                                                       const int w_id,
                                                       const int d_id,
                                                       const int o_id,
                                                       lock_mode_t alm,
                                                       bool need_tuple)
{
    assert (ptuple);

    // OL_IDX - { 2, 1, 0, 3 } = { OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER }

    // find index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("OL_IDX");
    assert (pindex);

    ptuple->set_value(0, o_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    ptuple->set_value(3, (int)0);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(0, o_id+1);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t order_line_man_impl::ol_get_probe_iter_by_index_nl(ss_m* db,
                                                          order_line_index_iter* &iter,
                                                          order_line_tuple* ptuple,
                                                          rep_row_t &replow,
                                                          rep_row_t &rephigh,
                                                          const int w_id,
                                                          const int d_id,
                                                          const int o_id,
                                                          bool need_tuple)
{
    return (ol_get_probe_iter_by_index(db,iter,ptuple,replow,rephigh,
                                       w_id,d_id,o_id,NL,need_tuple));
}



/* ------------ */
/* --- ITEM --- */
/* ------------ */


w_rc_t item_man_impl::it_index_probe(ss_m* db, 
                                     item_tuple* ptuple,
                                     const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    return (index_probe_by_name(db, "I_IDX", ptuple));
}

w_rc_t item_man_impl::it_index_probe_forupdate(ss_m* db, 
                                               item_tuple* ptuple,
                                               const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    return (index_probe_forupdate_by_name(db, "I_IDX", ptuple));
}

w_rc_t item_man_impl::it_index_probe_nl(ss_m* db, 
                                        item_tuple* ptuple,
                                        const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    return (index_probe_nl_by_name(db, "I_IDX", ptuple));
}



/* ------------- */
/* --- STOCK --- */
/* ------------- */


w_rc_t stock_man_impl::st_index_probe(ss_m* db,
                                      stock_tuple* ptuple,
                                      const int w_id,
                                      const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    ptuple->set_value(1, w_id);
    return (index_probe_by_name(db, "S_IDX", ptuple));
}

w_rc_t stock_man_impl::st_index_probe_forupdate(ss_m* db,
                                                stock_tuple* ptuple,
                                                const int w_id,
                                                const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    ptuple->set_value(1, w_id);
    return (index_probe_forupdate_by_name(db, "S_IDX", ptuple));
}

w_rc_t stock_man_impl::st_index_probe_nl(ss_m* db,
                                         stock_tuple* ptuple,
                                         const int w_id,
                                         const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    ptuple->set_value(1, w_id);
    return (index_probe_nl_by_name(db, "S_IDX", ptuple));
}

w_rc_t  stock_man_impl::st_update_tuple(ss_m* db,
                                        stock_tuple* ptuple,
                                        const tpcc_stock_tuple* pstock,
                                        lock_mode_t lm)
{
    // 1. updates the specified tuple

    assert (ptuple);
    assert (ptuple->is_rid_valid());

    ptuple->set_value(2, pstock->S_REMOTE_CNT);
    ptuple->set_value(3, pstock->S_QUANTITY);
    ptuple->set_value(4, pstock->S_ORDER_CNT);
    ptuple->set_value(5, pstock->S_YTD);
    //    return (table_man_impl<stock_t>::update_tuple(db, ptuple));
    return (update_tuple(db, ptuple, lm));
}

w_rc_t  stock_man_impl::st_update_tuple_nl(ss_m* db,
                                           stock_tuple* ptuple,
                                           const tpcc_stock_tuple* pstock)
{
    return (st_update_tuple(db,ptuple,pstock,NL));
}


EXIT_NAMESPACE(tpcc);
