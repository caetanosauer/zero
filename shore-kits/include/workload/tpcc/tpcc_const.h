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

/** @file:   tpcc_const.h
 *
 *  @brief:  Constants needed by the TPC-C kit
 *
 *  @author: Mengzhi Wang, April 2001
 *  @author: Ippokratis Pandis, January 2008
 */

#ifndef __TPCC_CONST_H
#define __TPCC_CONST_H


#include "util/namespace.h"


ENTER_NAMESPACE(tpcc);


const int TPCC_C_LAST_SZ  = 16;
const int TPCC_C_FIRST_SZ = 16;



// --- standard scale -- //

const int DISTRICTS_PER_WAREHOUSE = 10;
const int CUSTOMERS_PER_DISTRICT  = 3000;
const int ITEMS                   = 100000;
const int STOCK_PER_WAREHOUSE     = ITEMS;
const int MIN_OL_PER_ORDER        = 5;
const int MAX_OL_PER_ORDER        = 15;
const int NU_ORDERS_PER_DISTRICT  = 900;

const int MAX_TABLENAM_LENGTH     = 20;
const int MAX_RECORD_LENGTH       = 512;


// --- number of fields per table --- //

const int TPCC_WAREHOUSE_FCOUNT  = 9;
const int TPCC_DISTRICT_FCOUNT   = 11;
const int TPCC_CUSTOMER_FCOUNT   = 22;
const int TPCC_HISTORY_FCOUNT    = 8;
const int TPCC_NEW_ORDER_FCOUNT  = 3;
const int TPCC_ORDER_FCOUNT      = 8;
const int TPCC_ORDER_LINE_FCOUNT = 10;
const int TPCC_ITEM_FCOUNT       = 5;
const int TPCC_STOCK_FCOUNT      = 17;

// -- number of tables -- //

const int SHORE_TPCC_TABLES = 9;



/* ----------------------------- */
/* --- TPC-C TRANSACTION MIX --- */
/* ----------------------------- */

const int XCT_MIX           = 0;
const int XCT_NEW_ORDER     = 1;
const int XCT_PAYMENT       = 2;
const int XCT_ORDER_STATUS  = 3;
const int XCT_DELIVERY      = 4;
const int XCT_STOCK_LEVEL   = 5;

const int XCT_LITTLE_MIX    = 9;


const int XCT_MBENCH_WH   = 11;
const int XCT_MBENCH_CUST = 12;


// --- probabilities for the TPC-C MIX --- //

const int PROB_NEWORDER     = 45;
const int PROB_PAYMENT      = 43;
const int PROB_ORDER_STATUS = 4;
const int PROB_DELIVERY     = 4;
const int PROB_STOCK_LEVEL  = 4;


// --- Helper functions --- //


// Translates or picks a random xct type given the benchmark specification
int random_xct_type(int selected);


EXIT_NAMESPACE(tpcc);

#endif /* __TPCC_CONST_H */
