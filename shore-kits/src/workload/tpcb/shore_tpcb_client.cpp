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

/** @file:   shore_tpcb_client.cpp
 *
 *  @brief:  Implementation of the test client for the TPCB benchmark
 *
 *  @author: Ippokratis Pandis, Feb 2009
 */

#include "workload/tpcb/shore_tpcb_client.h"

ENTER_NAMESPACE(tpcb);



/********************************************************************* 
 *
 *  baseline_tpcb_client_t
 *
 *********************************************************************/

baseline_tpcb_client_t::baseline_tpcb_client_t(c_str tname, const int id, 
                                               ShoreTPCBEnv* env, 
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


int baseline_tpcb_client_t::load_sup_xct(mapSupTrxs& stmap)
{
    // clears the supported trx map and loads its own
    stmap.clear();

    // Baseline TPC-B trxs
    stmap[XCT_TPCB_ACCT_UPDATE]          = "TPCB-AccountUpdate";

    stmap[XCT_TPCB_MBENCH_INSERT_ONLY]   = "TPCB-MbenchInsertOnly";
    stmap[XCT_TPCB_MBENCH_DELETE_ONLY]   = "TPCB-MbenchDeleteOnly";
    stmap[XCT_TPCB_MBENCH_PROBE_ONLY]    = "TPCB-MbenchProbeOnly";
    stmap[XCT_TPCB_MBENCH_INSERT_DELETE] = "TPCB-MbenchInsertDelete";
    stmap[XCT_TPCB_MBENCH_INSERT_PROBE]  = "TPCB-MbenchInsertProbe";
    stmap[XCT_TPCB_MBENCH_DELETE_PROBE]  = "TPCB-MbenchDeleteProbe";
    stmap[XCT_TPCB_MBENCH_MIX]           = "TPCB-MbenchMix";

    return (stmap.size());
}


/********************************************************************* 
 *
 *  @fn:    submit_one
 *
 *  @brief: Entry point for running one TPC-B xct 
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure interval has expired.
 *
 *********************************************************************/
 
w_rc_t baseline_tpcb_client_t::submit_one(int xct_type, int xctid) 
{
    // Set input
    trx_result_tuple_t atrt;
    bool bWake = false;
    if (condex* c = _cp->take_one()) {
        atrt.set_notify(c);
        TRACE( TRACE_TRX_FLOW, "Sleeping\n");
        bWake = true;
    }

    // Pick a valid ID
    int selid = _selid;
//     if (_selid==0) 
//         selid = URand(1,_qf); 

    // Get one action from the trash stack
    trx_request_t* arequest = new (_env->_request_pool) trx_request_t;
    tid_t atid;
    arequest->set(NULL,atid,xctid,atrt,xct_type,selid);

    // Enqueue to worker thread
    assert (_worker);
    _worker->enqueue(arequest,bWake);
    return (RCOK);
}


EXIT_NAMESPACE(tpcb);


