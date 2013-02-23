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

/** @file:   shore_tpch_schema_man.h
 *
 *  @brief:  Implementation of the workload-specific access methods 
 *           on TPC-H tables
 *
 *  @author: Nastaran Nikparto, Summer 2011
 *  @author: Ippokratis Pandis, Jun 2009
 *
 */

#include "workload/tpch/shore_tpch_schema_man.h"
#include "util.h"

using namespace shore;


ENTER_NAMESPACE(tpch);


/*********************************************************************
 *
 * Workload-specific access methods on tables
 *
 *********************************************************************/


/* -------------- */
/* --- NATION --- */
/* -------------- */


w_rc_t nation_man_impl::n_index_probe(ss_m* db,
                                          nation_tuple* ptuple,
                                          const int n_nationkey)
{
    assert (ptuple);    
    ptuple->set_value(0, n_nationkey);
    return (index_probe_by_name(db, "N_IDX", ptuple));
}


/* -------------- */
/* --- REGION --- */
/* -------------- */


w_rc_t region_man_impl::r_index_probe(ss_m* db,
                                           region_tuple* ptuple,
                                           const int r_regionkey)
{
    assert (ptuple);
    ptuple->set_value(0, r_regionkey);
    return (index_probe_by_name(db, "R_IDX", ptuple));
}

/* ------------ */
/* --- PART --- */
/* ------------ */

w_rc_t part_man_impl::p_index_probe(ss_m* db,
                                    part_tuple* ptuple,
                                    const int p_partkey)
{
	assert(ptuple);

	ptuple->set_value(0, p_partkey);
	return (index_probe_by_name(db, "P_IDX", ptuple));
}


/* ---------------- */
/* --- SUPPLIER --- */
/* ---------------- */


w_rc_t supplier_man_impl::s_index_probe(ss_m* db, 
                                     supplier_tuple* ptuple,
                                     const int s_suppkey)
{
    assert (ptuple);
    ptuple->set_value(0, s_suppkey);
    return (index_probe_by_name(db, "S_IDX", ptuple));
}


/* ---------------- */
/* --- PARTSUPP --- */
/* ---------------- */


w_rc_t partsupp_man_impl::ps_index_probe(ss_m* db,
                                      partsupp_tuple* ptuple,
                                      const int ps_partkey,
                                      const int ps_suppkey)
{
    assert (ptuple);
    ptuple->set_value(0, ps_partkey);
    ptuple->set_value(1, ps_suppkey);
    return (index_probe_by_name(db, "PS_IDX", ptuple));
}

/* ---------------- */
/* --- CUSTOMER --- */
/* ---------------- */


//w_rc_t customer_man_impl::c_get_iter_by_index(ss_m* db,
//                                                 customer_index_iter* &iter,
//                                                 customer_tuple* ptuple,
//                                                 rep_row_t &replow,
//                                                 rep_row_t &rephigh,
//                                                 const int c_custkey,
//                                                 lock_mode_t alm,
//                                                 bool need_tuple)
//{
//    assert (ptuple);
//
//    // find the index
//    assert (_ptable);
//    index_desc_t* pindex = _ptable->find_index("C_NAME_IDX");
//    assert (pindex);
//
//    // C_NAME_IDX: {2 - 1 - 5 - 3 - 0}
//
//    // prepare the key to be probed
//    ptuple->set_value(0, 0);
//    ptuple->set_value(1, d_id);
//    ptuple->set_value(2, w_id);
//    ptuple->set_value(3, "");
//    ptuple->set_value(5, c_last);
//
//    int lowsz = format_key(pindex, ptuple, replow);
//    assert (replow._dest);
//
//    char   temp[2];
//    temp[0] = MAX('z', 'Z')+1;
//    temp[1] = '\0';
//    ptuple->set_value(3, temp);
//
//    int highsz = format_key(pindex, ptuple, rephigh);
//    assert (rephigh._dest);    
//
//    /* index only access */
//    W_DO(get_iter_for_index_scan(db, pindex, iter,
//                                 alm, need_tuple,
//				 scan_index_i::ge, vec_t(replow._dest, lowsz),
//				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
//    return (RCOK);
//}
//


w_rc_t customer_man_impl::c_index_probe(ss_m* db,
                                           customer_tuple* ptuple,
                                           const int c_custkey)
{
    assert (ptuple);
    return (c_index_probe_by_name(db, "C_IDX", ptuple, c_custkey));
}

w_rc_t customer_man_impl::c_index_probe_by_name(ss_m* db,
                                                   const char* idx_name,
                                                   customer_tuple* ptuple,
                                                   const int c_custkey)
{
    assert (idx_name);
    ptuple->set_value(0, c_custkey);
    return (index_probe_by_name(db, idx_name, ptuple));
}


/* -------------- */
/* --- ORDERS --- */
/* -------------- */


w_rc_t orders_man_impl::o_get_iter_by_index(ss_m* db,
                                             orders_index_iter* &iter,
                                             orders_tuple* ptuple,
                                             rep_row_t &replow,
                                             rep_row_t &rephigh,
                                             const int c_orderkey,
                                             lock_mode_t alm,
                                             bool need_tuple)
{
    assert (ptuple);

    /* find index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("O_IDX");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(0, c_orderkey);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(0, c_orderkey + 1);

    int highsz  = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t orders_man_impl::o_get_iter_by_findex(ss_m* db,
                                             orders_index_iter* &iter,
                                             orders_tuple* ptuple,
                                             rep_row_t &replow,
                                             rep_row_t &rephigh,
                                             const int o_custkey,
                                             lock_mode_t alm,
                                             bool need_tuple)
{
    assert (ptuple);

    /* find index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("O_FK_CUSTKEY");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(1, o_custkey);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(1, o_custkey + 1);

    int highsz  = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t orders_man_impl::o_get_range_iter_by_index(ss_m* db,
				     orders_index_iter* &iter,
				     orders_tuple* ptuple,
				     rep_row_t &replow,
				     rep_row_t &rephigh,
				     const time_t low_o_orderdate,
				     const time_t high_o_orderdate,
				     lock_mode_t alm,
				     bool need_tuple)
{
	assert(ptuple);

	/*pointer to the index*/
	assert (_ptable);
	index_desc_t* pindex = _ptable->find_index("O_IDX_ORDERDATE");
	assert (pindex);

	/* get the lowest key value */
	char low_date[15];
	timet_to_str(low_date, low_o_orderdate);
	ptuple->set_value(4, low_date);

	int lowsz = format_key(pindex, ptuple, replow);
	assert (replow._dest);

	/* get the highest key value */
	char high_date[15];
	timet_to_str(high_date, high_o_orderdate+1);
	ptuple->set_value(4, high_date);

	int highsz = format_key(pindex, ptuple, rephigh);
	assert (rephigh._dest);
    
	/* get the tuple iterator (not index only scan) */
	W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
	return (RCOK);
}


/* ---------------- */
/* --- LINEITEM --- */
/* ---------------- */

w_rc_t lineitem_man_impl::l_get_range_iter_by_receiptdate_index(ss_m* db,
                                                       lineitem_index_iter* &iter,
                                                       lineitem_tuple* ptuple,
                                                       rep_row_t &replow,
                                                       rep_row_t &rephigh,
                                                       const time_t low_l_receiptdate,
                                                       const time_t high_l_receiptdate,
                                                       lock_mode_t alm,
                                                       bool need_tuple)
{
    assert (ptuple);

    /* pointer to the index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("L_IDX_RECEIPTDATE");
    assert (pindex);

    /* get the lowest key value */
    char lowdate[15];
    timet_to_str(lowdate, low_l_receiptdate);
    ptuple->set_value(12, lowdate);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    char highdate[15];
    timet_to_str(highdate, high_l_receiptdate+1);
    ptuple->set_value(12,highdate );

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);
    
    /* get the tuple iterator (not index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t lineitem_man_impl::l_get_range_iter_by_index(ss_m* db,
                                                       lineitem_index_iter* &iter,
                                                       lineitem_tuple* ptuple,
                                                       rep_row_t &replow,
                                                       rep_row_t &rephigh,
                                                       const time_t low_l_shipdate,
                                                       const time_t high_l_shipdate,
                                                       lock_mode_t alm,
                                                       bool need_tuple)
{
    assert (ptuple);

    /* pointer to the index */
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("L_IDX_SHIPDATE");
    assert (pindex);
 
    /* get the lowest key value */
    char lowdate[15];
    timet_to_str(lowdate, low_l_shipdate);
    ptuple->set_value(10, lowdate);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    char highdate[15];
    timet_to_str(highdate, high_l_shipdate+1);
    ptuple->set_value(10, highdate);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);
    
    /* get the tuple iterator (not index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


EXIT_NAMESPACE(tpch);
