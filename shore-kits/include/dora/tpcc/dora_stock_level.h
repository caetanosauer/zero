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

/** @file:   dora_order_status.h
 *
 *  @brief:  DORA TPC-C STOCK LEVEL
 *
 *  @note:   Definition of RVPs and Actions that synthesize 
 *           the TPC-C StockLevel trx according to DORA
 *
 *  @author: Ippokratis Pandis, Jan 2009
 */


#ifndef __DORA_TPCC_STOCK_LEVEL_H
#define __DORA_TPCC_STOCK_LEVEL_H


#include "dora.h"
#include "workload/tpcc/shore_tpcc_env.h"
#include "dora/tpcc/dora_tpcc.h"

using namespace shore;
using namespace tpcc;


ENTER_NAMESPACE(dora);



//
// RVPS
//
// (1) mid1_stock_rvp
// (2) mid2_stock_rvp
// (3) final_stock_rvp
//

//@brief: Submits the IT(OL) action
DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid1_stock_rvp,DoraTPCCEnv,stock_level_input_t,1,1);

//@brief: Submits the R(ST) --join-- action
DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid2_stock_rvp,DoraTPCCEnv,stock_level_input_t,1,2);

DECLARE_DORA_FINAL_RVP_CLASS(final_stock_rvp,DoraTPCCEnv,1,3);


//
// ACTIONS
//
// (1) r_dist_stock_action
// (2) r_ol_stock_action
// (3) r_st_stock_action
// 

DECLARE_DORA_ACTION_WITH_RVP_CLASS(r_dist_stock_action,int,DoraTPCCEnv,mid1_stock_rvp,stock_level_input_t,2);

// !!! 2 fields only (WH,DI) determine the ORDERLINE table accesses, not 3 !!!
DECLARE_DORA_ACTION_WITH_RVP_CLASS(r_ol_stock_action,int,DoraTPCCEnv,mid2_stock_rvp,stock_level_input_t,2);

// !!! 1 field only (WH) determines the STOCK table accesses, not 2!!!
DECLARE_DORA_ACTION_NO_RVP_CLASS(r_st_stock_action,int,DoraTPCCEnv,stock_level_input_t,2);


EXIT_NAMESPACE(dora);

#endif /** __DORA_TPCC_STOCK_LEVEL_H */

