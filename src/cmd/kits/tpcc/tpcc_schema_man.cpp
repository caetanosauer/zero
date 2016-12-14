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

#include "tpcc_schema_man.h"

namespace tpcc {

/*********************************************************************
 *
 * Workload-specific access methods on tables
 *
 *********************************************************************/


/* ----------------- */
/* --- WAREHOUSE --- */
/* ----------------- */


w_rc_t
warehouse_man_impl::wh_index_probe(
                                   warehouse_tuple* ptuple,
                                   const int w_id)
{
    assert (ptuple);
    ptuple->set_value(0, w_id);
    return (index_probe(_ptable->primary_idx(), ptuple));
}


w_rc_t
warehouse_man_impl::wh_index_probe_forupdate(
                                             warehouse_tuple* ptuple,
                                             const int w_id)
{
    assert (ptuple);
    ptuple->set_value(0, w_id);
    return (index_probe_forupdate(_ptable->primary_idx(), ptuple));
}

w_rc_t
warehouse_man_impl::wh_update_ytd(
                                  warehouse_tuple* ptuple,
                                  const double amount)
{
    // 1. increases the ytd of the warehouse and updates the table

    assert (ptuple);

    double ytd;
    ptuple->get_value(8, ytd);
    ytd += amount;
    ptuple->set_value(8, ytd);
    W_DO(update_tuple(ptuple));
    return (RCOK);
}



/* ---------------- */
/* --- DISTRICT --- */
/* ---------------- */


w_rc_t district_man_impl::dist_index_probe(
                                           district_tuple* ptuple,
                                           const int w_id,
                                           const int d_id)
{
    assert (ptuple);
    ptuple->set_value(0, d_id);
    ptuple->set_value(1, w_id);
    return (index_probe(_ptable->primary_idx(), ptuple));
}

w_rc_t district_man_impl::dist_index_probe_forupdate(
                                                     district_tuple* ptuple,
                                                     const int w_id,
                                                     const int d_id)
{
    assert (ptuple);
    ptuple->set_value(0, d_id);
    ptuple->set_value(1, w_id);
    return (index_probe_forupdate(_ptable->primary_idx(), ptuple));
}

w_rc_t district_man_impl::dist_update_ytd(
                                          district_tuple* ptuple,
                                          const double amount)
{
    // 1. increases the ytd of the district and updates the table

    assert (ptuple);

    double d_ytd;
    ptuple->get_value(9, d_ytd);
    d_ytd += amount;
    ptuple->set_value(9, d_ytd);
    W_DO(update_tuple(ptuple));
    return (RCOK);
}

w_rc_t district_man_impl::dist_update_next_o_id(
                                                district_tuple* ptuple,
                                                const int next_o_id)
{
    // 1. updates the next order id of the district

    assert (ptuple);

    ptuple->set_value(10, next_o_id);
    return (update_tuple(ptuple));
}


/* ---------------- */
/* --- CUSTOMER --- */
/* ---------------- */


w_rc_t customer_man_impl::cust_get_iter_by_index(
                                                 customer_index_iter* &iter,
                                                 customer_tuple* ptuple,
                                                 rep_row_t &replow,
                                                 rep_row_t &rephigh,
                                                 const int w_id,
                                                 const int d_id,
                                                 const char* c_last,
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

    size_t lowsz = replow._bufsz;
    ptuple->store_key(replow._dest, lowsz, pindex);

    // CS TODO -- use open-end btcursor
    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(3, temp);

    size_t highsz = replow._bufsz;
    ptuple->store_key(rephigh._dest, highsz, pindex);

    /* index only access */
    iter = new customer_index_iter(pindex, this, need_tuple);
    W_DO(iter->open_scan(replow._dest, lowsz, true,
                rephigh._dest, highsz, false));
    return (RCOK);
}

w_rc_t customer_man_impl::cust_index_probe(
                                           customer_tuple* ptuple,
                                           const int w_id,
                                           const int d_id,
                                           const int c_id)
{
    assert (ptuple);
    ptuple->set_value(0, c_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    return (index_probe(_ptable->primary_idx(), ptuple));
}

w_rc_t customer_man_impl::cust_index_probe_by_name(
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
    return (index_probe_by_name(idx_name, ptuple));
}

w_rc_t customer_man_impl::cust_index_probe_forupdate(
                                                     customer_tuple* ptuple,
                                                     const int w_id,
                                                     const int d_id,
                                                     const int c_id)
{
    assert (ptuple);
    ptuple->set_value(0, c_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    return (index_probe_forupdate(_ptable->primary_idx(), ptuple));
}

w_rc_t customer_man_impl::cust_update_tuple(
                                            customer_tuple* ptuple,
                                            const tpcc_customer_tuple& acustomer,
                                            const char* adata1,
                                            const char* adata2)
{
    assert (ptuple);
    ptuple->set_value(16, acustomer.C_BALANCE);
    ptuple->set_value(17, acustomer.C_YTD_PAYMENT);
    ptuple->set_value(19, acustomer.C_PAYMENT_CNT);

    if (adata1)
	ptuple->set_value(20, adata1);

    if (adata2)
	ptuple->set_value(21, adata2);

    return (update_tuple(ptuple));
}



w_rc_t customer_man_impl::cust_update_discount_balance(
                                                       customer_tuple* ptuple,
                                                       const decimal discount,
                                                       const decimal balance)
{
    assert (ptuple);
    assert (discount>=0);
    ptuple->set_value(15, discount);
    ptuple->set_value(16, balance);
    return (update_tuple(ptuple));
}


/* ----------------- */
/* --- NEW_ORDER --- */
/* ----------------- */


w_rc_t new_order_man_impl::no_get_iter_by_index(
                                                new_order_table_iter* &iter,
                                                new_order_tuple* ptuple,
                                                rep_row_t &replow,
                                                rep_row_t &rephigh,
                                                const int w_id,
                                                const int d_id,
                                                bool /*need_tuple*/)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->primary_idx();
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(0, 0);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);

    size_t lowsz = replow._bufsz;
    ptuple->store_key(replow._dest, lowsz, pindex);

    /* get the highest key value */
    ptuple->set_value(1, d_id+1);

    size_t highsz = rephigh._bufsz;
    ptuple->store_key(rephigh._dest, highsz, pindex);

    iter = new new_order_table_iter(this);
    W_DO(iter->open_scan(replow._dest, lowsz, true,
                rephigh._dest, highsz, false));
    return (RCOK);
}


w_rc_t new_order_man_impl::no_delete_by_index(
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
//     W_DO(index_probe_forupdate_by_name("NO_IDX", ptuple));
    W_DO(delete_tuple(ptuple));

    return (RCOK);
}


/* ------------- */
/* --- ORDER --- */
/* ------------- */


w_rc_t order_man_impl::ord_get_iter_by_index(
                                             order_index_iter* &iter,
                                             order_tuple* ptuple,
                                             rep_row_t &replow,
                                             rep_row_t &rephigh,
                                             const int w_id,
                                             const int d_id,
                                             const int c_id,
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

    size_t lowsz = replow._bufsz;
    ptuple->store_key(replow._dest, lowsz, pindex);

    /* get the highest key value */
    ptuple->set_value(1, c_id+1);

    size_t highsz = rephigh._bufsz;
    ptuple->store_key(rephigh._dest, highsz, pindex);

    iter = new order_index_iter(pindex, this, need_tuple);
    W_DO(iter->open_scan(replow._dest, lowsz, true,
                rephigh._dest, highsz, false));
    return (RCOK);
}


w_rc_t order_man_impl::ord_update_carrier_by_index(
                                                   order_tuple* ptuple,
                                                   const int carrier_id)
{
    assert (ptuple);

    // 1. idx probe for update the order
    // 2. update carrier_id and update table

    W_DO(index_probe_forupdate(_ptable->primary_idx(), ptuple));

    ptuple->set_value(5, carrier_id);
    W_DO(update_tuple(ptuple));

    return (RCOK);
}


/* ----------------- */
/* --- ORDERLINE --- */
/* ----------------- */


w_rc_t order_line_man_impl::ol_get_range_iter_by_index(
                                                       order_line_table_iter* &iter,
                                                       order_line_tuple* ptuple,
                                                       rep_row_t &replow,
                                                       rep_row_t &rephigh,
                                                       const int w_id,
                                                       const int d_id,
                                                       const int low_o_id,
                                                       const int high_o_id,
                                                       bool need_tuple)
{
    assert (ptuple);

    // OL_IDX - { 2, 1, 0, 3 } = { OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER }

    // pointer to the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->primary_idx();
    assert (pindex);

    // get the lowest key value
    ptuple->set_value(0, low_o_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    ptuple->set_value(3, (int)0);  /* assuming that ol_number starts from 1 */

    size_t lowsz = replow._bufsz;
    ptuple->store_key(replow._dest, lowsz, pindex);

    // get the highest key value
    ptuple->set_value(0, high_o_id+1);

    size_t highsz = replow._bufsz;
    ptuple->store_key(rephigh._dest, highsz, pindex);

    // get the tuple iterator (not index only scan)
    (void) need_tuple;
    iter = new order_line_table_iter(this);
    W_DO(iter->open_scan(replow._dest, lowsz, true,
                rephigh._dest, highsz, false));
    return (RCOK);
}


w_rc_t order_line_man_impl::ol_get_probe_iter_by_index(
                                                       order_line_table_iter* &iter,
                                                       order_line_tuple* ptuple,
                                                       rep_row_t &replow,
                                                       rep_row_t &rephigh,
                                                       const int w_id,
                                                       const int d_id,
                                                       const int o_id,
                                                       bool /* need_tuple */)
{
    assert (ptuple);

    // OL_IDX - { 2, 1, 0, 3 } = { OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER }

    // find index
    assert (_ptable);
    index_desc_t* pindex = _ptable->primary_idx();
    assert (pindex);

    ptuple->set_value(0, o_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    ptuple->set_value(3, (int)0);

    size_t lowsz = replow._bufsz;
    ptuple->store_key(replow._dest, lowsz, pindex);

    /* get the highest key value */
    ptuple->set_value(0, o_id+1);

    size_t highsz = rephigh._bufsz;
    ptuple->store_key(rephigh._dest, highsz, pindex);

    iter = new order_line_table_iter(this);
    W_DO(iter->open_scan(replow._dest, lowsz, true,
                rephigh._dest, highsz, false));
    return (RCOK);
}

/* ------------ */
/* --- ITEM --- */
/* ------------ */


w_rc_t item_man_impl::it_index_probe(
                                     item_tuple* ptuple,
                                     const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    return (index_probe(_ptable->primary_idx(), ptuple));
}

w_rc_t item_man_impl::it_index_probe_forupdate(
                                               item_tuple* ptuple,
                                               const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    return (index_probe_forupdate(_ptable->primary_idx(), ptuple));
}


/* ------------- */
/* --- STOCK --- */
/* ------------- */


w_rc_t stock_man_impl::st_index_probe(
                                      stock_tuple* ptuple,
                                      const int w_id,
                                      const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    ptuple->set_value(1, w_id);
    return (index_probe(_ptable->primary_idx(), ptuple));
}

w_rc_t stock_man_impl::st_index_probe_forupdate(
                                                stock_tuple* ptuple,
                                                const int w_id,
                                                const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    ptuple->set_value(1, w_id);
    return (index_probe_forupdate(_ptable->primary_idx(), ptuple));
}

w_rc_t  stock_man_impl::st_update_tuple(
                                        stock_tuple* ptuple,
                                        const tpcc_stock_tuple* pstock)
{
    // 1. updates the specified tuple

    assert (ptuple);

    ptuple->set_value(2, pstock->S_REMOTE_CNT);
    ptuple->set_value(3, pstock->S_QUANTITY);
    ptuple->set_value(4, pstock->S_ORDER_CNT);
    ptuple->set_value(5, pstock->S_YTD);
    //    return (table_man_impl<stock_t>::update_tuple(ptuple));
    return (update_tuple(ptuple));
}

};
