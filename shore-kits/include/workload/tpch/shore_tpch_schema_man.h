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
 *  @brief:  Declaration of the TPC-H table managers
 *
 *  @author: Nastaran Nikparto, Summer 2011
 *  @author: Ippokratis Pandis, June 2009
 *
 */

#ifndef __SHORE_TPCH_SCHEMA_MANAGER_H
#define __SHORE_TPCH_SCHEMA_MANAGER_H

#include "workload/tpch/tpch_struct.h"
#include "workload/tpch/shore_tpch_schema.h"


using namespace shore;


ENTER_NAMESPACE(tpch);



/* ------------------------------------------------------------------ */
/* --- The managers of all the tables used in the TPC-H benchmark --- */
/* ------------------------------------------------------------------ */


class nation_man_impl : public table_man_impl<nation_t>
{
    typedef table_row_t nation_tuple;

public:

    nation_man_impl(nation_t* aNationDesc)
        : table_man_impl<nation_t>(aNationDesc)
    { }

    ~nation_man_impl() { }

    /* --- access specific tuples  --- */
    w_rc_t n_index_probe(ss_m* db, 
                         nation_tuple* ptuple, 
                         const int n_nationkey);
    
}; // EOF: nation_man_impl


class region_man_impl : public table_man_impl<region_t>
{
    typedef table_row_t region_tuple;

public:

    region_man_impl(region_t* aRegionDesc)
        : table_man_impl<region_t>(aRegionDesc)
    { }

    ~region_man_impl() { }

    /* --- access specific tuples --- */
    w_rc_t r_index_probe(ss_m* db,
                         region_tuple* ptuple,
                         const int r_regionkey);

}; // EOF: region_man_impl

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


class supplier_man_impl : public table_man_impl<supplier_t>
{
    typedef table_row_t supplier_tuple;
    typedef table_scan_iter_impl<supplier_t> supplier_table_iter;
    typedef index_scan_iter_impl<supplier_t> supplier_index_iter;

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

class partsupp_man_impl : public table_man_impl<partsupp_t>
{
    typedef table_row_t partsupp_tuple;
    typedef table_scan_iter_impl<partsupp_t> partsupp_table_iter;
    typedef index_scan_iter_impl<partsupp_t> partsupp_index_iter;

public:

    partsupp_man_impl(partsupp_t* aPartsuppDesc)
        : table_man_impl<partsupp_t>(aPartsuppDesc)
    {
    }

    ~partsupp_man_impl()
    {
    }
		
    w_rc_t ps_index_probe(ss_m* db,
                          partsupp_tuple* ptuple,
                          const int ps_partkey,
                          const int ps_suppkey);

}; // EOF: partsupp_man_impl


class customer_man_impl : public table_man_impl<customer_t>
{
    typedef table_row_t customer_tuple;
    typedef table_scan_iter_impl<customer_t> customer_table_iter;
    typedef index_scan_iter_impl<customer_t> customer_index_iter;

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
    
    w_rc_t c_index_probe_by_name(ss_m* db,
                                 const char* idx_name,
                                 customer_tuple* ptuple,
                                 const int c_custkey);

}; // EOF: customer_man_impl


class orders_man_impl : public table_man_impl<orders_t>
{
    typedef table_row_t orders_tuple;
    typedef table_scan_iter_impl<orders_t> orders_table_iter;
    typedef index_scan_iter_impl<orders_t> orders_index_iter;

public:

    orders_man_impl(orders_t* aOrdersDesc)
        : table_man_impl<orders_t>(aOrdersDesc)
    {
    }

    ~orders_man_impl()
    {
    }

    /* --- access specific tuples --- */         
    w_rc_t o_get_iter_by_index(ss_m* db,
                               orders_index_iter* &iter,
                               orders_tuple* ptuple,
                               rep_row_t &replow,
                               rep_row_t &rephigh,
                               const int c_orderkey,
                               lock_mode_t alm = SH,
                               bool need_tuple = true);
    
    w_rc_t o_get_iter_by_findex(ss_m* db,
				orders_index_iter* &iter,
                                orders_tuple* ptuple,
                                rep_row_t &replow,
                                rep_row_t &rephigh,
                                const int o_custkey,
                                lock_mode_t alm = SH,
                                bool need_tuple = true);
    
    w_rc_t o_get_range_iter_by_index(ss_m* db,
				     orders_index_iter* &iter,
				     orders_tuple* ptuple,
				     rep_row_t &replow,
				     rep_row_t &rephigh,
				     const time_t low_o_orderdate,
				     const time_t high_o_orderdate,
				     lock_mode_t alm = SH,
				     bool need_tuple = true);
	

}; // EOF: orders_man_impl


class lineitem_man_impl : public table_man_impl<lineitem_t>
{
    typedef table_row_t lineitem_tuple;
    typedef table_scan_iter_impl<lineitem_t> lineitem_table_iter;
    typedef index_scan_iter_impl<lineitem_t> lineitem_index_iter;

public:

    lineitem_man_impl(lineitem_t* aLineitemDesc)
        : table_man_impl<lineitem_t>(aLineitemDesc)
    {
    }

    ~lineitem_man_impl()
    {
    }

    /* --- access the table --- */
    w_rc_t l_get_range_iter_by_index(ss_m* db,
                                     lineitem_index_iter* &iter,
                                     lineitem_tuple* ptuple,
                                     rep_row_t &replow,
                                     rep_row_t &rephigh,
                                     const time_t low_l_shipdate,
                                     const time_t high_l_shipdate,
                                     lock_mode_t alm = SH,
                                     bool need_tuple = true);

    
    //    w_rc_t l_get_probe_iter_by_index(ss_m* db,
    //                                      lineitem_index_iter* &iter,
    //                                      lineitem_tuple* ptuple,
    //                                      rep_row_t &replow,
    //                                      rep_row_t &rephigh,
    //                                      const int l_orderkey,
    //                                      const int l_linenumber,
    //                                      lock_mode_t alm = SH,
    //                                      bool need_tuple = true);

    w_rc_t l_get_range_iter_by_receiptdate_index(ss_m* db,
                                                       lineitem_index_iter* &iter,
                                                       lineitem_tuple* ptuple,
                                                       rep_row_t &replow,
                                                       rep_row_t &rephigh,
                                                       const time_t low_l_receiptdate,
                                                       const time_t high_l_receiptdate,
                                                       lock_mode_t alm = SH,
						 bool need_tuple = true);
}; // EOF: lineitem_man_impl

EXIT_NAMESPACE(tpch);

#endif /* __SHORE_TPCH_SCHEMA_MANAGER_H */
