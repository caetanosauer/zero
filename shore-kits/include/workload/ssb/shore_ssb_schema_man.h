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

/** @file:   shore_ssb_schema_man.h
 *
 *  @brief:  Declaration of the SSB table managers
 *
 *  @author: Manos Athanassoulis, June 2010
 *
 */

#ifndef __SHORE_SSB_SCHEMA_MANAGER_H
#define __SHORE_SSB_SCHEMA_MANAGER_H

#include "workload/ssb/ssb_struct.h"
#include "workload/ssb/shore_ssb_schema.h"


using namespace shore;


ENTER_NAMESPACE(ssb);



/* ------------------------------------------------------------------ */
/* --- The managers of all the tables used in the SSB benchmark --- */
/* ------------------------------------------------------------------ */




class part_man_impl : public table_man_impl<part_t>
{
    typedef table_row_t part_tuple;

public:

    part_man_impl(part_t* aPartDesc)
        : table_man_impl<part_t>(aPartDesc)
    { }

    ~part_man_impl() { }

    w_rc_t p_index_probe(ss_m* db,
                         part_tuple* ptuple,
                         const int p_partkey);

}; // EOF: part_man_impl


class date_man_impl : public table_man_impl<date_t>
{
    typedef table_row_t date_tuple;

public:

    date_man_impl(date_t* aPartDesc)
        : table_man_impl<date_t>(aPartDesc)
    { }

    ~date_man_impl() { }

    w_rc_t d_index_probe(ss_m* db,
                         date_tuple* ptuple,
                         const int d_datekey);


}; // EOF: date_man_impl


class supplier_man_impl : public table_man_impl<supplier_t>
{
    typedef table_row_t supplier_tuple;

public:

    supplier_man_impl(supplier_t* aSupplierDesc)
        : table_man_impl<supplier_t>(aSupplierDesc)
    {
    }

    ~supplier_man_impl()
    {
    }

    /* --- access the table --- */
    w_rc_t s_index_probe(ss_m* db,
                         supplier_tuple* ptuple,
                         const int s_suppkey);


}; // EOF: supplier_man_impl



class customer_man_impl : public table_man_impl<customer_t>
{
    typedef table_row_t customer_tuple;

public:

    customer_man_impl(customer_t* aCustomerDesc)
        : table_man_impl<customer_t>(aCustomerDesc)
    { }

    ~customer_man_impl() { }

    /* --- access specific tuples --- */
    // w_rc_t c_get_iter_by_index(ss_m* db,
    //                               customer_index_iter* &iter,
    //                               customer_tuple* ptuple,
    //                               rep_row_t &replow,
    //                               rep_row_t &rephigh,
    //                               const int c_custkey,
    //                               lock_mode_t alm = SH,
    //                               bool need_tuple = false);


    w_rc_t c_index_probe(ss_m* db,
                         customer_tuple* ptuple,
                         const int c_custkey);
    
    /*    w_rc_t c_index_probe_by_name(ss_m* db,
                                 const char* idx_name,
                                 customer_tuple* ptuple,
                                 const int c_custkey);
    */
}; // EOF: customer_man_impl







class lineorder_man_impl : public table_man_impl<lineorder_t>
{
    typedef table_row_t lineorder_tuple;

public:

    lineorder_man_impl(lineorder_t* aLineorderDesc)
        : table_man_impl<lineorder_t>(aLineorderDesc)
    {
    }

    ~lineorder_man_impl()
    {
    }

    /* --- access the table --- */
    w_rc_t lo_index_probe(ss_m* db,
                          lineorder_tuple* ptuple,
                          const int lo_orderkey,
                          const int lo_linenumber);

    /*w_rc_t lineorder_man_impl::lo_get_range_iter_by_index(ss_m* db,
                                                       lineorder_tuple* ptuple,
                                                      const int lo_orderkey);

    */

    //w_rc_t lo_get_range_iter_by_index(ss_m* db,
    //                                 lineorder_index_iter* &iter,
    //                                 lineorder_tuple* ptuple,
    //                                 rep_row_t &replow,
    //                                 rep_row_t &rephigh,
    //                                 const time_t low_l_shipdate,
    //                                 const time_t high_l_shipdate,
    //                                 lock_mode_t alm = SH,
    //                                 bool need_tuple = true);

    
    //    w_rc_t l_get_probe_iter_by_index(ss_m* db,
    //                                      lineitem_index_iter* &iter,
    //                                      lineitem_tuple* ptuple,
    //                                      rep_row_t &replow,
    //                                      rep_row_t &rephigh,
    //                                      const int l_orderkey,
    //                                      const int l_linenumber,
    //                                      lock_mode_t alm = SH,
    //                                      bool need_tuple = true);

}; // EOF: lineitem_man_impl

EXIT_NAMESPACE(ssb);

#endif /* __SHORE_SSB_SCHEMA_MANAGER_H */
