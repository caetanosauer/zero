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

/** @file:   dora_tpcc_client.cpp
 *
 *  @brief:  Implementation of the DORA client for the TPCC benchmark
 *
 *  @author: Ippokratis Pandis, May 2009
 */

#include "dora/tpcc/dora_tpcc_client.h"


ENTER_NAMESPACE(dora);

// Look also at include/workload/tpcc/tpcc_const.h
const int XCT_DORA_MIX           = 100;
const int XCT_DORA_NEW_ORDER     = 101;
const int XCT_DORA_PAYMENT       = 102;
const int XCT_DORA_ORDER_STATUS  = 103;
const int XCT_DORA_DELIVERY      = 104;
const int XCT_DORA_STOCK_LEVEL   = 105;

const int XCT_DORA_LITTLE_MIX    = 109;

const int XCT_DORA_MBENCH_WH   = 111;
const int XCT_DORA_MBENCH_CUST = 112;


/********************************************************************* 
 *
 *  dora_tpcc_client_t
 *
  *********************************************************************/

int dora_tpcc_client_t::load_sup_xct(mapSupTrxs& stmap)
{
    // clears the supported trx map and loads its own
    stmap.clear();

    // Baseline TPC-C trxs
    stmap[XCT_DORA_MIX]          = "DORA-TPCC-Mix";
    stmap[XCT_DORA_NEW_ORDER]    = "DORA-TPCC-NewOrder";
    stmap[XCT_DORA_PAYMENT]      = "DORA-TPCC-Payment";
    stmap[XCT_DORA_ORDER_STATUS] = "DORA-TPCC-OrderStatus";
    stmap[XCT_DORA_DELIVERY]     = "DORA-TPCC-Delivery";
    stmap[XCT_DORA_STOCK_LEVEL]  = "DORA-TPCC-StockLevel";

    // A Mix of 50%-50% NewOrder & Payment
    stmap[XCT_DORA_LITTLE_MIX]   = "DORA-TPCC-LittleMix";

    // Microbenchmarks
    stmap[XCT_DORA_MBENCH_WH]    = "DORA-TPCC-MBench-WHs";
    stmap[XCT_DORA_MBENCH_CUST]  = "DORA-TPCC-MBench-CUSTs";

    return (stmap.size());
}

/********************************************************************* 
 *
 *  @fn:    submit_one
 *
 *  @brief: Entry point for running one DORA TPC-C xct 
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure internal has expired.
 *
 *********************************************************************/
 
w_rc_t dora_tpcc_client_t::submit_one(int xct_type, int xctid) 
{
    // if DORA TPC-C MIX
    bool bWake = false;
    if (xct_type == XCT_DORA_MIX) {        
        xct_type = XCT_DORA_MIX + random_xct_type(rand(100));
        bWake = true;
    }

    // pick a valid wh id
    int whid = _wh;
    if (_wh==0) {
        whid = URand(1,_qf); 
        bWake = true;
    }

    trx_result_tuple_t atrt;
    if (condex* c = _cp->take_one()) {
        atrt.set_notify(c);
        bWake = true;
    }
    
    switch (xct_type) {

        // TPC-C DORA
    case XCT_DORA_NEW_ORDER:
        return (_tpccdb->dora_new_order(xctid,atrt,whid,bWake));
    case XCT_DORA_PAYMENT:
        return (_tpccdb->dora_payment(xctid,atrt,whid,bWake));
    case XCT_DORA_ORDER_STATUS:
        return (_tpccdb->dora_order_status(xctid,atrt,whid,bWake));
    case XCT_DORA_DELIVERY:
        return (_tpccdb->dora_delivery(xctid,atrt,whid,bWake));
    case XCT_DORA_STOCK_LEVEL:
        return (_tpccdb->dora_stock_level(xctid,atrt,whid,bWake));

        // Little Mix (NewOrder/Payment 50%-50%)
    case XCT_DORA_LITTLE_MIX:
        if (URand(1,100)>50)
            return (_tpccdb->dora_new_order(xctid,atrt,whid,true));
        else
            return (_tpccdb->dora_payment(xctid,atrt,whid,true));

        // MBENCH DORA
    case XCT_DORA_MBENCH_WH:
        return (_tpccdb->dora_mbench_wh(xctid,atrt,whid,bWake));
    case XCT_DORA_MBENCH_CUST:
        return (_tpccdb->dora_mbench_cust(xctid,atrt,whid,bWake));

    default:
        assert (0); // UNKNOWN TRX-ID
    }
    return (RCOK);
}

EXIT_NAMESPACE(dora);


