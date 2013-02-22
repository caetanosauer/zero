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
 *  @brief:  Declaration of the TPC-B tables
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#include "workload/tpcb/shore_tpcb_schema.h"

using namespace shore;

ENTER_NAMESPACE(tpcb);



/*********************************************************************
 *
 * TPC-B SCHEMA
 * 
 * This file contains the classes for tables in tpcb benchmark.
 * A class derived from tpcb_table_t (which inherits from table_desc_t) 
 * is created for each table in the databases.
 *
 *********************************************************************/


/*
 * A primary index is created on each table except HISTORY
 *
 * 1. BRANCH
 * a. primary (unique) index on branch(b_id)
 * 
 * 2. TELLER
 * a. primary (unique) index on teller(t_id)
 *
 * 3. ACCOUNT
 * a. primary (unique) index on account(a_id)
 *
 */


branch_t::branch_t(const uint4_t& pd)
#ifdef CFG_HACK
    : table_desc_t("BRANCH", 3, pd) 
#else
      : table_desc_t("BRANCH", 2, pd) 
#endif
{
    // Schema
    _desc[0].setup(SQL_INT,   "B_ID");
    _desc[1].setup(SQL_FLOAT, "B_BALANCE");

#ifdef CFG_HACK
#warning Adding _PADDING fields in some of the TM1 and TPCB tables
    _desc[2].setup(SQL_FIXCHAR,  "B_PADDING", 100-sizeof(int)-sizeof(double));
#endif
    
    // create unique index b_idx on (b_id)
    uint  keys1[1] = { 0 }; // IDX { B_ID }
    create_primary_idx_desc("B_IDX", 0, keys1, 1, pd);
}



teller_t::teller_t(const uint4_t& pd)
#ifdef CFG_HACK
    : table_desc_t("TELLER", 4, pd) 
#else
      : table_desc_t("TELLER", 3, pd) 
#endif
{
    // Schema
    _desc[0].setup(SQL_INT,   "T_ID");     
    _desc[1].setup(SQL_INT,   "T_B_ID");   
    _desc[2].setup(SQL_FLOAT, "T_BALANCE");

#ifdef CFG_HACK
    _desc[3].setup(SQL_FIXCHAR,  "T_PADDING", 100-2*sizeof(int) - sizeof(double));
#endif

    // create unique index t_idx on (t_id)    
    uint keys1[1] = { 0 }; // IDX { T_ID }
    create_primary_idx_desc("T_IDX", 0, keys1, 1, pd);
}



account_t::account_t(const uint4_t& pd)
#ifdef CFG_HACK
    : table_desc_t("ACCOUNT", 4, pd) 
#else
      : table_desc_t("ACCOUNT", 3, pd) 
#endif
{
    // Schema
    _desc[0].setup(SQL_INT,    "A_ID");
    _desc[1].setup(SQL_INT,    "A_B_ID");       
    _desc[2].setup(SQL_FLOAT,  "A_BALANCE");  

#ifdef CFG_HACK
    _desc[3].setup(SQL_FIXCHAR,   "A_PADDING", 100-2*sizeof(int)-sizeof(double));  
#endif
    
#ifdef PLP_MBENCH
#warning PLP MBench !!!!
    uint keys1[3] = { 0, 1, 2};
    uint nkeys = 3;
#else
    uint keys1[1] = {0 }; // IDX { A_ID }
    uint nkeys = 1;
#endif

    // create unique index a_idx on (a_id)    
    create_primary_idx_desc("A_IDX", 0, keys1, nkeys, pd);
}


history_t::history_t(const uint4_t& pd)
#ifdef CFG_HACK
    : table_desc_t("HISTORY", 6, pd) 
#else
      : table_desc_t("HISTORY", 5, pd) 
#endif
{
    // Schema
    _desc[0].setup(SQL_INT,   "H_B_ID");
    _desc[1].setup(SQL_INT,   "H_T_ID");  
    _desc[2].setup(SQL_INT,   "H_A_ID"); 
    _desc[3].setup(SQL_FLOAT, "H_DELTA");   /* old: INT */
    _desc[4].setup(SQL_FLOAT, "H_TIME");    /* old: TIME */

#ifdef CFG_HACK
    _desc[5].setup(SQL_FIXCHAR,  "H_PADDING", 50-3*sizeof(int)-2*sizeof(double)); 
#endif    

    // NO INDEXES
}


EXIT_NAMESPACE(tpcb);
