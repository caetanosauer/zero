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

/** @file:   dora_new_order.h
 *
 *  @brief:  DORA TPC-C NEW_ORDER
 *
 *  @note:   Definition of RVPs and Actions that synthesize 
 *           the TPC-C NewOrder trx according to DORA
 *
 *  @author: Ippokratis Pandis, Nov 2008
 */


#ifndef __DORA_TPCC_NEW_ORDER_H
#define __DORA_TPCC_NEW_ORDER_H


#include "dora/tpcc/dora_tpcc.h"
#include "workload/tpcc/shore_tpcc_env.h"

using namespace shore;
using namespace tpcc;


ENTER_NAMESPACE(dora);




//
// RVPS
//
// (1) mid1_nord_rvp
// (2) mid2_nord_rvp
// (3) mid3_nord_rvp
// (4) final_nord_rvp
//


DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid1_nord_rvp,DoraTPCCEnv,new_order_input_t,4,4);
DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid2_nord_rvp,DoraTPCCEnv,new_order_input_t,2,6);
DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid3_nord_rvp,DoraTPCCEnv,new_order_input_t,1,7);
DECLARE_DORA_FINAL_RVP_CLASS(final_nord_rvp,DoraTPCCEnv,1,8);




//
// ACTIONS
//

//
// Start -> Midway 1
//
// (1) r_wh_nord_action
// (2) r_cust_nord_action
// (3) upd_dist_nord_action
// (4) r_item_nord_action
// 

DECLARE_DORA_ACTION_WITH_RVP_CLASS(r_wh_nord_action,int,DoraTPCCEnv,mid1_nord_rvp,no_item_nord_input_t,1);

DECLARE_DORA_ACTION_WITH_RVP_CLASS(upd_dist_nord_action,int,DoraTPCCEnv,mid1_nord_rvp,no_item_nord_input_t,2);

// !!! 2 fields only (WH,DI) determine the CUSTOMER table accesses, not 3 !!!
DECLARE_DORA_ACTION_WITH_RVP_CLASS(r_cust_nord_action,int,DoraTPCCEnv,mid1_nord_rvp,no_item_nord_input_t,2);

// !!! The items reading is performed as a single action (instead of OLCNT)
// !!! 1 field only (WH) determines the ITEM table accesses, not 2 and not ITEM !!!
DECLARE_DORA_ACTION_WITH_RVP_CLASS(r_item_nord_action,int,DoraTPCCEnv,mid1_nord_rvp,new_order_input_t,1);


//
// Midway 1 -> Midway 2
//
// (5) ins_ord_nord_action
// (6) ins_nord_nord_action
//

// !!! 2 fields only (WH,DI) determine the ORDER table accesses, not 3 !!!
DECLARE_DORA_ACTION_WITH_RVP_CLASS(ins_ord_nord_action,int,DoraTPCCEnv,mid2_nord_rvp,no_item_nord_input_t,2);

// !!! 2 fields only (WH,DI) determine the NEW-ORDER table accesses, not 3 !!!
DECLARE_DORA_ACTION_WITH_RVP_CLASS(ins_nord_nord_action,int,DoraTPCCEnv,mid2_nord_rvp,no_item_nord_input_t,2);


//
// Midway 2 -> Midway 3
//
// (7) ins_ol_nord_action
//
// !!! 2 fields only (WH,DI) determine the ORDERLINE table accesses, not 3 !!!
DECLARE_DORA_ACTION_WITH_RVP_CLASS(ins_ol_nord_action,int,DoraTPCCEnv,mid3_nord_rvp,new_order_input_t,2);


//
// Midway 3 -> Final
//
// (8) upd_sto_nord_action
//

// !!! The stocks updating is performed as a single action (instead of OLCNT)
// !!! 1 field only (WH) determines the STOCK table accesses, not 2 !!!
DECLARE_DORA_ACTION_NO_RVP_CLASS(upd_sto_nord_action,int,DoraTPCCEnv,new_order_input_t,1);



EXIT_NAMESPACE(dora);

#endif /** __DORA_TPCC_NEW_ORDER_H */

