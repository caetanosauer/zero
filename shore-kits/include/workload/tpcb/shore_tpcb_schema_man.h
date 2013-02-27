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

/** @file:   shore_tpcb_schema_man.h
 *
 *  @brief:  Declaration of the TPC-B table managers
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#ifndef __SHORE_TPCB_SCHEMA_MANAGER_H
#define __SHORE_TPCB_SCHEMA_MANAGER_H


#include "workload/tpcb/shore_tpcb_schema.h"

using namespace shore;


ENTER_NAMESPACE(tpcb);



/* ------------------------------------------------------------------ */
/* --- The managers of all the tables used in the TPC-B benchmark --- */
/* ------------------------------------------------------------------ */


class branch_man_impl : public table_man_impl<branch_t>
{
    typedef table_row_t branch_tuple;

public:

    branch_man_impl(branch_t* aBranchDesc)
        : table_man_impl<branch_t>(aBranchDesc)
    { }

    ~branch_man_impl() { }

    // --- access specific tuples  ---
    w_rc_t b_index_probe(ss_m* db,
			 branch_tuple* ptuple,
			 const int b_id);

    w_rc_t b_index_probe_forupdate(ss_m* db, 
				   branch_tuple* ptuple, 
				   const int b_id);

    w_rc_t b_idx_nl(ss_m* db, 
                    branch_tuple* ptuple, 
                    const int b_id);    

}; // EOF: branch_man_impl



class teller_man_impl : public table_man_impl<teller_t>
{
    typedef table_row_t teller_tuple;

public:

    teller_man_impl(teller_t* aTellerDesc)
        : table_man_impl<teller_t>(aTellerDesc)
    { }

    ~teller_man_impl() { }

    // --- access specific tuples  ---
    w_rc_t t_index_probe_forupdate(ss_m* db, 
				   teller_tuple* ptuple, 
				   const int t_id);

    w_rc_t t_idx_nl(ss_m* db, 
                    teller_tuple* ptuple, 
                    const int t_id);

}; // EOF: teller_man_impl



class account_man_impl : public table_man_impl<account_t>
{
    typedef table_row_t account_tuple;
    typedef table_scan_iter_impl<account_t> account_table_iter;
    typedef index_scan_iter_impl<account_t> account_index_iter;

public:

    account_man_impl(account_t* aAccountDesc)
        : table_man_impl<account_t>(aAccountDesc)
    { }

    ~account_man_impl() { }

    // --- access specific tuples  ---
    w_rc_t a_index_probe_forupdate(ss_m* db, 
				   account_tuple* ptuple, 
				   const int a_id,
                                   const int b_id = 0, // PLP_MBENCH
                                   const double balance = 0);

    w_rc_t a_delete_by_index(ss_m* db,
			     account_tuple* ptuple,
			     const int a_id,
			     const int b_id = 0, // PLP_MBENCH
			     const double balance = 0);

    w_rc_t a_index_probe(ss_m* db,
			 account_tuple* ptuple,
			 const int a_id,
			 const int b_id = 0, // PLP_MBENCH
			 const double balance = 0);

    w_rc_t a_idx_nl(ss_m* db, 
                    account_tuple* ptuple, 
                    const int a_id,
                    const int b_id = 0, // PLP_MBENCH
                    const double balance = 0);

}; // EOF: account_man_impl




class history_man_impl : public table_man_impl<history_t>
{
    typedef table_row_t history_tuple;

public:

    history_man_impl(history_t* aHistoryDesc)
        : table_man_impl<history_t>(aHistoryDesc)
    { }

    ~history_man_impl() { }

}; // EOF: history_man_impl



EXIT_NAMESPACE(tpcb);

#endif /* __SHORE_TPCB_SCHEMA_MANAGER_H */
