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

/** @file:   shore_tpcc_client.cpp
 *
 *  @brief:  Implementation of the client for the TPCC benchmark
 *
 *  @author: Ippokratis Pandis, July 2008
 */

#include "workload/tpcc/shore_tpcc_client.h"

ENTER_NAMESPACE(tpcc);


/********************************************************************* 
 *
 *  baseline_tpcc_client_t
 *
 *********************************************************************/


baseline_tpcc_client_t::baseline_tpcc_client_t(c_str tname, const int id, 
                                               ShoreTPCCEnv* env, 
                                               const MeasurementType aType, 
                                               const int trxid, 
                                               const int numOfTrxs, 
                                               processorid_t aprsid, 
                                               const int sWH, const double qf) 
    : base_client_t(tname,id,env,aType,trxid,numOfTrxs,aprsid),
      _wh(sWH), _qf(qf)
{
    assert (env);
    assert (_wh>=0 && _qf>0);
    
    // pick worker thread
    _worker = _env->worker(_id);
    assert (_worker);
}


int baseline_tpcc_client_t::load_sup_xct(mapSupTrxs& stmap)
{
    // clears the supported trx map and loads its own
    stmap.clear();

    // Baseline TPC-C trxs
    stmap[XCT_MIX]          = "TPCC-Mix";
    stmap[XCT_NEW_ORDER]    = "TPCC-NewOrder";
    stmap[XCT_PAYMENT]      = "TPCC-Payment";
    stmap[XCT_ORDER_STATUS] = "TPCC-OrderStatus";
    stmap[XCT_DELIVERY]     = "TPCC-Delivery";
    stmap[XCT_STOCK_LEVEL]  = "TPCC-StockLevel";

    stmap[XCT_LITTLE_MIX]   = "TPCC-LittleMix";

    // Microbenchmarks
    stmap[XCT_MBENCH_WH]    = "TPCC-MBench-WHs";
    stmap[XCT_MBENCH_CUST]  = "TPCC-MBench-CUSTs";

    return (stmap.size());
}


/********************************************************************* 
 *
 *  @fn:    submit_one
 *
 *  @brief: Entry point for running one TPC-C xct 
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure internal has expired.
 *
 *********************************************************************/
 
w_rc_t baseline_tpcc_client_t::submit_one(int xct_type, int xctid) 
{    
    // Set input
    trx_result_tuple_t atrt;
    bool bWake = false;
    if (condex* c = _cp->take_one()) {
        atrt.set_notify(c);
        TRACE( TRACE_TRX_FLOW, "Sleeping\n");
        bWake = true;
    }

    // Pick a valid WH
    int whid = _wh;
    if (_wh==0) 
        whid = URand(1,_qf); 

    // Get one action from the trash stack
    trx_request_t* arequest = new (_env->_request_pool) trx_request_t;
    tid_t atid;
    arequest->set(NULL,atid,xctid,atrt,xct_type,whid);    

    // Enqueue to worker thread
    assert (_worker);
    _worker->enqueue(arequest,bWake);
    return (RCOK);
}



EXIT_NAMESPACE(tpcc);
