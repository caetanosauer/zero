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
 *  @brief:  DORA TPC-C ORDER STATUS
 *
 *  @note:   Definition of RVPs and Actions that synthesize 
 *           the TPC-C OrderStatus trx according to DORA
 *
 *  @author: Ippokratis Pandis, Jan 2009
 */


#ifndef __DORA_TPCC_ORDER_STATUS_H
#define __DORA_TPCC_ORDER_STATUS_H


#include "dora.h"
#include "workload/tpcc/shore_tpcc_env.h"
#include "dora/tpcc/dora_tpcc.h"

using namespace shore;
using namespace tpcc;


ENTER_NAMESPACE(dora);



//
// RVPS
//
// (1) mid1_ordst_rvp
// (2) mid2_ordst_rvp
// (3) final_ordst_rvp
//


DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid1_ordst_rvp,DoraTPCCEnv,order_status_input_t,1,1);
DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid2_ordst_rvp,DoraTPCCEnv,order_status_input_t,1,2);
DECLARE_DORA_FINAL_RVP_CLASS(final_ordst_rvp,DoraTPCCEnv,1,3);



//
// ACTIONS
//
// (1) r_cust_ordst_action
// (2) r_ord_ordst_action
// (2) r_ol_ordst_action
// 

// !!! 2 fields only (WH,DI) determine the CUSTOMER table accesses, not 3 !!! 
DECLARE_DORA_ACTION_WITH_RVP_CLASS(r_cust_ordst_action,int,DoraTPCCEnv,mid1_ordst_rvp,order_status_input_t,2);

// !!! 2 fields only (WH,DI) determine the ORDER table accesses, not 3 !!!
DECLARE_DORA_ACTION_WITH_RVP_CLASS(r_ord_ordst_action,int,DoraTPCCEnv,mid2_ordst_rvp,order_status_input_t,2);

// !!! 2 fields only (WH,DI) determine the ORDERLINE table accesses, not 3 !!!
DECLARE_DORA_ACTION_NO_RVP_CLASS(r_ol_ordst_action,int,DoraTPCCEnv,order_status_input_t,2);



EXIT_NAMESPACE(dora);

#endif /** __DORA_TPCC_ORDER_STATUS_H */

