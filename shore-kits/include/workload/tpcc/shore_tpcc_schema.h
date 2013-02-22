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

/** @file:   shore_tpcc_schema.h
 *
 *  @brief:  Declaration of the TPC-C tables
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#ifndef __SHORE_TPCC_SCHEMA_H
#define __SHORE_TPCC_SCHEMA_H


#include <math.h>

#include "sm_vas.h"
#include "util.h"

#include "workload/tpcc/tpcc_const.h"
#include "sm/shore/shore_table_man.h"

using namespace shore;


ENTER_NAMESPACE(tpcc);



/* -------------------------------------------------- */
/* --- All the tables used in the TPC-C benchmark --- */
/* ---                                            --- */
/* --- Schema details at:                         --- */
/* --- src/workload/tpcc/shore_tpcc_schema.cpp    --- */
/* -------------------------------------------------- */


DECLARE_TABLE_SCHEMA_PD(warehouse_t);
DECLARE_TABLE_SCHEMA_PD(district_t);
DECLARE_TABLE_SCHEMA_PD(stock_t);
DECLARE_TABLE_SCHEMA_PD(order_line_t);
DECLARE_TABLE_SCHEMA_PD(customer_t);
DECLARE_TABLE_SCHEMA_PD(history_t);
DECLARE_TABLE_SCHEMA_PD(order_t);
DECLARE_TABLE_SCHEMA_PD(new_order_t);
DECLARE_TABLE_SCHEMA_PD(item_t);


EXIT_NAMESPACE(tpcc);

#endif // __SHORE_TPCC_SCHEMA_H
