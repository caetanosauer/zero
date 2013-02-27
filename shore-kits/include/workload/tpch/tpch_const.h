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

/** @file:   tpch_const.h
 *
 *  @brief:  Constants needed by the TPC-H kit
 *
 *  @author: Ippokratis Pandis, June 2009
 */

#ifndef __TPCH_CONST_H
#define __TPCH_CONST_H


#include "util/namespace.h"


ENTER_NAMESPACE(tpch);


// --- base cardinality-- //

const int NATIONS   = 25;
const int REGIONS   = 5;
const int PARTS     = 200000;
const int SUPPLIERS = 10000;
const int PARTSUPPS = 800000;
const int CUSTOMERS = 150000;
const int ORDERS    = 1500000;

const int LINEITEMS_1   = 6001215;
const int LINEITEMS_10  = 59986052;
const int LINEITEMS_30  = 179998372;
const int LINEITEMS_100 = 600037902;

const int MAX_TABLENAM_LENGTH   = 8;
const int MAX_RECORD_LENGTH     = 234;


// --- number of fields per table --- //

const int TPCH_NATION_FCOUNT    = 4;
const int TPCH_REGION_FCOUNT    = 3;
const int TPCH_PART_FCOUNT      = 9;
const int TPCH_SUPPLIER_FCOUNT  = 7;
const int TPCH_PARTSUPP_FCOUNT  = 5;
const int TPCH_CUSTOMER_FCOUNT  = 8;
const int TPCH_ORDERS_FCOUNT    = 9;
const int TPCH_LINEITEM_FCOUNT  = 16;

// -- number of tables -- //

const int SHORE_TPCH_TABLES = 8;

/* ----------------------------- */
/* --- TPC-H TRANSACTION MIX --- */
/* ----------------------------- */

const int XCT_TPCH_MIX      = 40;
const int XCT_TPCH_Q1       = 41;
const int XCT_TPCH_Q2       = 42;
const int XCT_TPCH_Q3       = 43;
const int XCT_TPCH_Q4       = 44;
const int XCT_TPCH_Q5       = 45;
const int XCT_TPCH_Q6       = 46;
const int XCT_TPCH_Q7       = 47;
const int XCT_TPCH_Q8       = 48;
const int XCT_TPCH_Q9       = 49;
const int XCT_TPCH_Q10      = 50;
const int XCT_TPCH_Q11      = 51;
const int XCT_TPCH_Q12      = 52;
const int XCT_TPCH_Q13      = 53;
const int XCT_TPCH_Q14      = 54;
const int XCT_TPCH_Q15      = 55;
const int XCT_TPCH_Q16      = 56;
const int XCT_TPCH_Q17      = 57;
const int XCT_TPCH_Q18      = 58;
const int XCT_TPCH_Q19      = 59;
const int XCT_TPCH_Q20      = 60;
const int XCT_TPCH_Q21      = 61;
const int XCT_TPCH_Q22      = 62;

const int XCT_TPCH_QLINEITEM = 70;
const int XCT_TPCH_QORDERS   = 71;
const int XCT_TPCH_QREGION   = 72;
const int XCT_TPCH_QNATION   = 73;
const int XCT_TPCH_QSUPPLIER = 74;
const int XCT_TPCH_QPART     = 75;
const int XCT_TPCH_QPARTSUPP = 76;
const int XCT_TPCH_QCUSTOMER = 77;

const int XCT_QPIPE_TPCH_MIX      = 1040;


EXIT_NAMESPACE(tpch);

#endif /* __TPCH_CONST_H */
