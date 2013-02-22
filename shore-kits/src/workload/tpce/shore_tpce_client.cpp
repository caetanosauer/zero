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

/** @file:   shore_tpce_client.cpp
 *
 *  @brief:  Implementation of the test client for the TPCE benchmark
 *
 *  @author: Ippokratis Pandis, Apr 2010
 */

#include "workload/tpce/shore_tpce_client.h"


ENTER_NAMESPACE(tpce);



/********************************************************************* 
 *
 *  baseline_tpce_client_t
 *
 *********************************************************************/


baseline_tpce_client_t::baseline_tpce_client_t(c_str tname, const int id, 
                                               ShoreTPCEEnv* env, 
                                               const MeasurementType aType, 
                                               const int trxid, 
                                               const int numOfTrxs, 
                                               processorid_t aprsid, 
                                               const int selID, const double qf) 
    : base_client_t(tname,id,env,aType,trxid,numOfTrxs,aprsid),
      _selid(selID), _qf(qf)
{
    assert (env);
    assert (_id>=0 && _qf>0);
    
    // pick worker thread
    _worker = _env->worker(_id);
    assert (_worker);
}


const int baseline_tpce_client_t::load_sup_xct(mapSupTrxs& stmap)
{
    // clears the supported trx map and loads its own
    stmap.clear();

    stmap[XCT_TPCE_MIX]              = "TPC-E MIX";
    stmap[XCT_TPCE_BROKER_VOLUME]    = "TPC-E BROKER_VOLUME";
    stmap[XCT_TPCE_CUSTOMER_POSITION]= "TPC-E CUSTOMER_POSITION";
    stmap[XCT_TPCE_MARKET_FEED]      = "TPC-E MARKET_FEED";
    stmap[XCT_TPCE_MARKET_WATCH]     = "TPC-E MARKET_WATCH";
    stmap[XCT_TPCE_SECURITY_DETAIL]  = "TPC-E SECURITY_DETAIL";
    stmap[XCT_TPCE_TRADE_LOOKUP]     = "TPC-E TRADE_LOOKUP";
    stmap[XCT_TPCE_TRADE_ORDER]      = "TPC-E TRADE_ORDER";
    stmap[XCT_TPCE_TRADE_RESULT]     = "TPC-E TRADE_RESULT";
    stmap[XCT_TPCE_TRADE_STATUS]     = "TPC-E TRADE_STATUS";
    stmap[XCT_TPCE_TRADE_UPDATE]     = "TPC-E TRADE_UPDATE";
    stmap[XCT_TPCE_DATA_MAINTENANCE] = "TPC-E DATA_MAINTENANCE";
    stmap[XCT_TPCE_TRADE_CLEANUP]    = "TPC-E TRADE_CLEANUP";

    return (stmap.size());
}


/********************************************************************* 
 *
 *  @fn:    submit_one
 *
 *  @brief: Baseline client - Entry point for running one trx 
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure internal has expired.
 *
 *********************************************************************/
 
w_rc_t baseline_tpce_client_t::submit_one(int xct_type, int xctid) 
{
    // Set input
    trx_result_tuple_t atrt;
    bool bWake = false;
    if (condex* c = _cp->take_one()) {
        atrt.set_notify(c);
        bWake = true;
    }

    // Pick a valid ID
    int selid = _selid;
    // if (_selid==0) 
    //     selid = URand(1,_qf); 

    // Get one action from the trash stack
    trx_request_t* arequest = new (_env->_request_pool) trx_request_t;
    tid_t atid;
    arequest->set(NULL,atid,xctid,atrt,xct_type,selid);    

    // Enqueue to worker thread
    assert (_worker);
    _worker->enqueue(arequest,bWake);
    return (RCOK);
}


EXIT_NAMESPACE(tpce);


