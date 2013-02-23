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

/** @file:   shore_tpcb_schema.h
 *
 *  @brief:  Implementation of the workload-specific access methods 
 *           on TPC-B tables
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#include "workload/tpcb/shore_tpcb_schema_man.h"

using namespace shore;


ENTER_NAMESPACE(tpcb);


/*********************************************************************
 *
 * Workload-specific access methods on tables
 *
 *********************************************************************/


/* ----------------- */
/* --- BRANCH --- */
/* ----------------- */

w_rc_t
branch_man_impl::b_index_probe(ss_m* db,
			       branch_tuple* ptuple,
			       const int b_id)
{
    assert (ptuple);
    ptuple->set_value(0, b_id);
    return (index_probe_by_name(db, "B_INDEX", ptuple));
}

w_rc_t branch_man_impl::b_index_probe_forupdate(ss_m* db,
                                                branch_tuple* ptuple,
                                                const int b_id)
{
    assert (ptuple);    
    ptuple->set_value(0, b_id);
    return (index_probe_forupdate_by_name(db, "B_IDX", ptuple));
}

w_rc_t branch_man_impl::b_idx_nl(ss_m* db,
                                 branch_tuple* ptuple,
                                 const int b_id)
{
    assert (ptuple);
    ptuple->set_value(0, b_id);
    return (index_probe_nl_by_name(db, "B_IDX", ptuple));
}


/* ---------------- */
/* --- TELLER --- */
/* ---------------- */


w_rc_t teller_man_impl::t_index_probe_forupdate(ss_m* db,
                                                teller_tuple* ptuple,
                                                const int t_id)
{
    assert (ptuple);    
    ptuple->set_value(0, t_id);
    return (index_probe_forupdate_by_name(db, "T_IDX", ptuple));
}

w_rc_t teller_man_impl::t_idx_nl(ss_m* db,
                                 teller_tuple* ptuple,
                                 const int t_id)
{
    assert (ptuple);    
    ptuple->set_value(0, t_id);
    return (index_probe_nl_by_name(db, "T_IDX", ptuple));
}



/* ---------------- */
/* --- ACCOUNT --- */
/* ---------------- */

w_rc_t account_man_impl::a_index_probe(ss_m* db,
                                       account_tuple* ptuple,
                                       const int a_id,
                                       const int b_id,
                                       const double balance)
{
    assert (ptuple);
    ptuple->set_value(0, a_id);
#ifdef PLP_MBENCH
    ptuple->set_value(1, b_id);
    ptuple->set_value(2, balance);
#else
    (void)b_id;
    (void)balance;
#endif
    return (index_probe_by_name(db, "A_IDX", ptuple));
}

w_rc_t account_man_impl::a_delete_by_index(ss_m* db,
                                           account_tuple* ptuple,
                                           const int a_id,
                                           const int b_id,
                                           const double balance)
{
    assert (ptuple);
    ptuple->set_value(0, a_id);
#ifdef PLP_MBENCH
    ptuple->set_value(1, b_id);
    ptuple->set_value(2, balance);
#else
    (void)b_id;
    (void)balance;
#endif
    W_DO(index_probe_forupdate_by_name(db, "A_IDX", ptuple));
    return (delete_tuple(db, ptuple));
}

w_rc_t account_man_impl::a_index_probe_forupdate(ss_m* db,
                                                 account_tuple* ptuple,
                                                 const int a_id,
                                                 const int b_id,
                                                 const double balance)
{
    assert (ptuple);    
    ptuple->set_value(0, a_id);
#ifdef PLP_MBENCH
    ptuple->set_value(1, b_id);
    ptuple->set_value(2, balance);
#else
    (void)b_id;
    (void)balance;
#endif
    return (index_probe_forupdate_by_name(db, "A_IDX", ptuple));
}

w_rc_t account_man_impl::a_idx_nl(ss_m* db,
                                  account_tuple* ptuple,
                                  const int a_id,
                                  const int b_id,
                                  const double balance)
{
    assert (ptuple);    
    ptuple->set_value(0, a_id);
#ifdef PLP_MBENCH
    ptuple->set_value(1, b_id);
    ptuple->set_value(2, balance);
#else
    (void)b_id;
    (void)balance;
#endif
    return (index_probe_nl_by_name(db, "A_IDX", ptuple));
}


EXIT_NAMESPACE(tpcb);
