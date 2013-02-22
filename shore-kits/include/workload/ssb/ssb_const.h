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

/** @file:   ssb_const.h
 *
 *  @brief:  Constants needed by the SSB kit
 *
 *  @author: Manos Athanassoulis, June 2010
 */

#ifndef __SSB_CONST_H
#define __SSB_CONST_H


#include "util/namespace.h"


ENTER_NAMESPACE(ssb);


// --- base cardinality-- //

//const int PARTS      = 200000;//logarithmic growth
//const int SUPPLIERS  = 2000;
//const int DATES      = 7*366;//TODO does not scale -> should make it smaller though check what fiscal date is supposed to mean
//const int CUSTOMERS  = 30000;
//const int LINEORDERS = 6000000;

//const int MAX_TABLENAM_LENGTH   = 8;
//const int MAX_RECORD_LENGTH     = 150;//TODO verify it


// --- number of fields per table --- //


const int SSB_PART_FCOUNT      = 9;
const int SSB_SUPPLIER_FCOUNT  = 7;
const int SSB_DATE_FCOUNT  = 17;
const int SSB_CUSTOMER_FCOUNT  = 8;
const int SSB_LINEORDER_FCOUNT  = 17;

// -- number of tables -- //

const int SHORE_SSB_TABLES = 5;

/* ----------------------------- */
/* --- TPC-H TRANSACTION MIX --- */
/* ----------------------------- */

const int XCT_SSB_MIX        = 80;
const int XCT_SSB_Q1_1       = 81;
const int XCT_SSB_Q1_2       = 82;
const int XCT_SSB_Q1_3       = 83;
const int XCT_SSB_Q2_1       = 84;
const int XCT_SSB_Q2_2       = 85;
const int XCT_SSB_Q2_3       = 86;
const int XCT_SSB_Q3_1       = 87;
const int XCT_SSB_Q3_2       = 88;
const int XCT_SSB_Q3_3       = 89;
const int XCT_SSB_Q3_4       = 90;
const int XCT_SSB_Q4_1       = 91;
const int XCT_SSB_Q4_2       = 92;
const int XCT_SSB_Q4_3       = 93;
const int XCT_SSB_QPART      = 94;
const int XCT_SSB_QDATE      = 95;
const int XCT_SSB_QSUPPLIER  = 96;
const int XCT_SSB_QCUSTOMER  = 97;
const int XCT_SSB_QLINEORDER = 98;
const int XCT_SSB_QTEST = 99;


const int XCT_QPIPE_SSB_MIX      = 1080;


EXIT_NAMESPACE(ssb);

#endif /* __SSB_CONST_H */
