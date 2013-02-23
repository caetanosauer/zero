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

/* -*- mode:C++; c-basic-offset:4 -*- */

/** @file:   shore_ssb_schema.h
 *
 *  @brief:  Implementation of the workload-specific access methods 
 *           on SSB tables
 *
 *  @author: Manos Athanassoulis, Jun 2010
 *
 */

#include "workload/ssb/shore_ssb_schema_man.h"


using namespace shore;


ENTER_NAMESPACE(ssb);


/*********************************************************************
 *
 * Workload-specific access methods on tables
 *
 *********************************************************************/



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
//    index_desc_t* pindex = _ptable->find_index("C_NAME_INDEX");
//    assert (pindex);
//
//    // C_NAME_INDEX: {2 - 1 - 5 - 3 - 0}
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
    ptuple->set_value(0, c_custkey);
    return (index_probe_by_name(db, "C_IDX", ptuple));
}

/*w_rc_t customer_man_impl::c_index_probe_by_name(ss_m* db,
                                                   const char* idx_name,
                                                   customer_tuple* ptuple,
                                                   const int c_custkey)
{
    assert (idx_name);
    ptuple->set_value(0, c_custkey);
    return (index_probe_by_name(db, idx_name, ptuple));
}*/


/* ------------ */
/* --- DATE --- */
/* ------------ */


w_rc_t date_man_impl::d_index_probe(ss_m* db,
                                           date_tuple* ptuple,
                                           const int d_datekey)
{
    assert (ptuple);
    ptuple->set_value(0, d_datekey);
    return (index_probe_by_name(db, "D_IDX", ptuple));
}



/* ----------------- */
/* --- LINEORDER --- */
/* ----------------- */

w_rc_t lineorder_man_impl::lo_index_probe(ss_m* db,
                                          lineorder_tuple* ptuple,
                                          const int lo_orderkey,
                                          const int lo_linenumber)
{
    assert (ptuple);
    ptuple->set_value(0, lo_orderkey);
    ptuple->set_value(1, lo_linenumber);
    return (index_probe_by_name(db, "LO_IDX", ptuple));
}


/*w_rc_t lineorder_man_impl::lo_get_range_iter_by_index(ss_m* db,
                                                       lineorder_tuple* ptuple,
                                                      const int lo_orderkey,
                                                      const int lo_linenumber)
{
    assert (ptuple);
    ptuple->set_value(0, lo_orderkey);
    ptuple->set_value(1, lo_linenumber);
    return (index_probe_by_name(db, "LO_IDX", ptuple));
}


w_rc_t lineorder_man_impl::lo_get_range_iter_by_index(ss_m* db,
                                                       lineorder_tuple* ptuple,
                                                      const int lo_orderkey)
{
    assert (ptuple);
    return (index_probe_by_name(db, "LO_IDX_ORDERKEY", ptuple, lo_orderkey));
}
*/

/*
w_rc_t lineorder_man_impl::lo_get_range_iter_by_index(ss_m* db,
                                                       lineorder_index_iter* &iter,
                                                       lineorder_tuple* ptuple,
                                                       rep_row_t &replow,
                                                       rep_row_t &rephigh,
                                                       const time_t low_l_shipdate,
                                                       const time_t high_l_shipdate,
                                                       lock_mode_t alm,
                                                       bool need_tuple)
{
    assert (ptuple);

// pointer to the index 
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("L_IDX_SHIPDATE");
    assert (pindex);

// get the lowest key value 
    ptuple->set_value(10, low_l_shipdate);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

// get the highest key value 
    ptuple->set_value(10, high_l_shipdate+1);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);
    
// get the tuple iterator (not index only scan) 
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}
*/

EXIT_NAMESPACE(ssb);
