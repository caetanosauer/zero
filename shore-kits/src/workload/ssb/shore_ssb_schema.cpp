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

/** @file:   shore_ssb_schema.h
 *
 *  @brief:  Declaration of the SSB tables
 *
 *  @author: Manos Athanassoulis, June 2010
 *
 */


/*********************************************************************
 *
 * SSB SCHEMA
 * 
 * This file contains the classes for tables in the SSB benchmark.
 * A class derived from ssb_table_t (which inherits from table_desc_t) 
 * is created for each table in the databases.
 *
 *********************************************************************/


/*
 * indices created on the tables are:
 *
 * 1. PART (p_)
 * a. primary (unique) index on part(p_partkey)
 *
 * 2. SUPPLIER (s_)
 * a. primary (unique) index on supplier(s_suppkey)
 *
 * 3. CUSTOMER (c_)
 * a. primary (unique) index on customer(c_custkey)
 *
 * 4. DATE (d_)
 * a. primary (unique) index on orders(o_orderkey)
 *
 * 5. LINEORDER (lo_)
 * a. primary (unique) index on lineitem(lo_orderkey, lo_linenumber)
 * b. secondary index on lineitem(l_orderkey)
 */


#include "workload/ssb/shore_ssb_schema.h"

using namespace shore;

ENTER_NAMESPACE(ssb);

supplier_t::supplier_t(const uint4_t& pd) : 
    table_desc_t("SUPPLIER", SSB_SUPPLIER_FCOUNT, pd) 
{
    // table schema
    _desc[0].setup(SQL_INT,     "S_SUPPKEY");
    _desc[1].setup(SQL_FIXCHAR, "S_NAME",    25);
    _desc[2].setup(SQL_FIXCHAR, "S_ADDRESS", 25);
    _desc[3].setup(SQL_FIXCHAR, "S_CITY",    10);
    _desc[4].setup(SQL_FIXCHAR, "S_NATION", 15);
    _desc[5].setup(SQL_FIXCHAR, "S_REGION", 12);
    _desc[6].setup(SQL_FIXCHAR, "S_PHONE", 15);

    uint keys[1] = {0}; // IDX { S_SUPPKEY}
				
    // baseline - Regular
    if ((pd & PD_NORMAL) && !(pd & PD_NOLOCK)) {
        TRACE( TRACE_DEBUG, "Regular idxs for (%s)\n", _name);
 
        // create unique index s_index on (s_suppkey)
        create_primary_idx_desc("S_IDX", 0, keys, 1);
    } 
}


part_t::part_t(const uint4_t& pd) : 
    table_desc_t("PART", SSB_PART_FCOUNT, pd) 
{
    // table schema
    _desc[0].setup(SQL_INT,     "P_PARTKEY");
    _desc[1].setup(SQL_FIXCHAR, "P_NAME",      22);  
    _desc[2].setup(SQL_FIXCHAR, "P_MFGR",       6); 
    _desc[3].setup(SQL_FIXCHAR, "P_CATEGORY",   7);   
    _desc[4].setup(SQL_FIXCHAR, "P_BRAND",      9);    
    _desc[5].setup(SQL_FIXCHAR, "P_COLOR",     11);    
    _desc[6].setup(SQL_FIXCHAR, "P_TYPE",      25);  
    _desc[7].setup(SQL_INT,     "P_SIZE"); 
    _desc[8].setup(SQL_FIXCHAR, "P_CONTAINER", 10);
                                
    uint keys[1] = { 0 }; // IDX { P_PARTKEY }
        
    if ((pd & PD_NORMAL) && !(pd & PD_NOLOCK)) {
        TRACE(TRACE_DEBUG, "Regular idxs for (%s)\n", _name);
            
        create_primary_idx_desc("P_IDX", 0, keys, 1);
    }
}


customer_t::customer_t(const uint4_t& pd) : 
    table_desc_t("CUSTOMER", SSB_CUSTOMER_FCOUNT, pd) 
{
    // table schema
    _desc[0].setup(SQL_INT,     "C_CUSTKEY");
    _desc[1].setup(SQL_FIXCHAR, "C_NAME",       25);       
    _desc[2].setup(SQL_FIXCHAR, "C_ADDRESS",    25);       
    _desc[3].setup(SQL_FIXCHAR, "C_CITY",       10);
        _desc[4].setup(SQL_FIXCHAR, "C_NATION",     15);   
        _desc[5].setup(SQL_FIXCHAR, "C_REGION",     12);
        _desc[6].setup(SQL_FIXCHAR, "C_PHONE",      15);
        _desc[7].setup(SQL_FIXCHAR, "C_MKTSEGMENT", 10);

    uint keys[1] = { 0 }; // IDX { C_CUSTKEY }
        
    //    uint fkeys[1] = { 3 }; // IDX { C_NATIONKEY }

    // baseline - regular indexes
    if ((pd & PD_NORMAL) && !(pd & PD_NOLOCK)) {
        TRACE( TRACE_DEBUG, "Regular idxs for (%s)\n", _name);
 
        // create unique index c_index on (c_custkey)
        create_primary_idx_desc("C_IDX", 0, keys, 1);
    }
}

date_t::date_t(const uint4_t& pd) : 
    table_desc_t("DATE", SSB_DATE_FCOUNT, pd) 
{
    // table schema
     _desc[0].setup(SQL_INT,     "D_DATEKEY");
    _desc[1].setup(SQL_FIXCHAR, "D_DATE", 18);       
    _desc[2].setup(SQL_FIXCHAR, "D_DAYOFWEEK", 10);       
    _desc[3].setup(SQL_FIXCHAR, "D_MONTH", 9);  
    _desc[4].setup(SQL_INT,     "D_YEAR");  
    _desc[5].setup(SQL_INT,    "D_YEARMONTHNUM");   
    _desc[6].setup(SQL_FIXCHAR, "D_YEARMONTH", 7);
    _desc[7].setup(SQL_INT, "D_DAYNUMINWEEK");
    _desc[8].setup(SQL_INT, "D_DATNUMINMONTH");
    _desc[9].setup(SQL_INT, "D_DATNUMINYEAR");
    _desc[10].setup(SQL_INT, "D_MONTHNUMINYEAR");
    _desc[11].setup(SQL_INT, "D_WEEKNUMINYEAR");
    _desc[12].setup(SQL_FIXCHAR, "D_SELLINGSEASON", 12);
    _desc[13].setup(SQL_FIXCHAR, "D_LASTDAYINWEEKFL",2);
    _desc[14].setup(SQL_FIXCHAR, "D_LASTDAYINMONTHFL",2);
    _desc[15].setup(SQL_FIXCHAR, "D_HOLIDAYFL",2);
    _desc[16].setup(SQL_FIXCHAR, "D_WEEKDAYFL",2);

    uint keys[1] = { 0 }; // IDX { C_CUSTKEY }
        
    // baseline - regular indexes
    if ((pd & PD_NORMAL) && !(pd & PD_NOLOCK)) {
        TRACE( TRACE_DEBUG, "Regular idxs for (%s)\n", _name);
 
        // create unique index c_index on (c_custkey)
        create_primary_idx_desc("D_IDX", 0, keys, 1);
    }
}






lineorder_t::lineorder_t(const uint4_t& pd) : 
    table_desc_t("LINEORDER", SSB_LINEORDER_FCOUNT, pd) 
{
    // table schema
    _desc[0].setup(SQL_INT,     "LO_ORDERKEY");
    _desc[1].setup(SQL_INT,     "LO_LINENUMBER");
    _desc[2].setup(SQL_INT,     "LO_CUSTKEY");
    _desc[3].setup(SQL_INT,     "LO_PARTKEY");
    _desc[4].setup(SQL_INT,     "LO_SUPPKEY");
    _desc[5].setup(SQL_INT,     "LO_ORDERDATE");
    _desc[6].setup(SQL_FIXCHAR, "LO_ORDERPRIORITY", 15);
    _desc[7].setup(SQL_INT,     "LO_SHIPPRIORITY");
    _desc[8].setup(SQL_INT,     "LO_QUANTITY");
    _desc[9].setup(SQL_INT,     "LO_EXTENDEDPRICE");
    _desc[10].setup(SQL_INT,    "LO_ORDTOTALPRICE");
    _desc[11].setup(SQL_INT,    "LO_DISCOUNT");
    _desc[12].setup(SQL_INT,    "LO_REVENUE");
    _desc[13].setup(SQL_INT,    "LO_SUPPLYCOST");
    _desc[14].setup(SQL_INT,    "LO_TAX");
    _desc[15].setup(SQL_INT,    "LO_COMMITDATE");
    _desc[16].setup(SQL_FIXCHAR,"LO_SHIPMODE", 10);

    uint keys[2] = {0, 1}; // IDX { L_ORDERKEY, L_LINENUMBER }

    uint fkeys1[1] = { 0 }; // IDX { L_ORDERKEY }

    // baseline - regular indexes
    if ((pd & PD_NORMAL) && !(pd & PD_NOLOCK)) {
        TRACE( TRACE_DEBUG, "Regular idxs for (%s)\n", _name);

        // create unique index l_index on ()
        create_primary_idx_desc("LO_IDX", 0, keys, 2);

        create_index_desc("LO_IDX_ORDERKEY", 0, fkeys1, 1, false);
    }
}


EXIT_NAMESPACE(ssb);
