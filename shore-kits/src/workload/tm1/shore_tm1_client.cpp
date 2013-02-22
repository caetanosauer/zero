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

/** @file:   shore_tm1_client.cpp
 *
 *  @brief:  Implementation of the client for the TM1 benchmark
 *
 *  @author: Ippokratis Pandis, Feb 2009
 */

#include "workload/tm1/shore_tm1_client.h"

ENTER_NAMESPACE(tm1);


/********************************************************************* 
 *
 *  baseline_tm1_client_t
 *
 *********************************************************************/

baseline_tm1_client_t::baseline_tm1_client_t(c_str tname, const int id, 
                                             ShoreTM1Env* env, 
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


int baseline_tm1_client_t::load_sup_xct(mapSupTrxs& stmap)
{
    // clears the supported trx map and loads its own
    stmap.clear();

    // Baseline TM1 trxs
    stmap[XCT_TM1_MIX]             = "TM1-Mix";
    stmap[XCT_TM1_GET_SUB_DATA]    = "TM1-GetSubData";
    stmap[XCT_TM1_GET_NEW_DEST]    = "TM1-GetNewDest";
    stmap[XCT_TM1_GET_ACC_DATA]    = "TM1-GetAccData";
    stmap[XCT_TM1_UPD_SUB_DATA]    = "TM1-UpdSubData";
    stmap[XCT_TM1_UPD_LOCATION]    = "TM1-UpdLocation";
    stmap[XCT_TM1_CALL_FWD_MIX]    = "TM1-CallFwd-Mix";
    stmap[XCT_TM1_INS_CALL_FWD]    = "TM1-InsCallFwd";
    stmap[XCT_TM1_DEL_CALL_FWD]    = "TM1-DelCallFwd";

    stmap[XCT_TM1_GET_SUB_NBR]    = "TM1-GetSubNbr";

    stmap[XCT_TM1_CALL_FWD_MIX_BENCH]    = "TM1-CallFwd-Mix-Bench";
    stmap[XCT_TM1_INS_CALL_FWD_BENCH]    = "TM1-InsCallFwd-Bench";
    stmap[XCT_TM1_DEL_CALL_FWD_BENCH]    = "TM1-DelCallFwd-Bench";

    return (stmap.size());
}


/********************************************************************* 
 *
 *  @fn:    submit_one
 *
 *  @brief: Entry point for running one TM1 xct 
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure interval has expired.
 *
 *********************************************************************/
 
w_rc_t baseline_tm1_client_t::submit_one(int xct_type, int xctid) 
{
    // Set input    
    trx_result_tuple_t atrt;
    bool bWake = false;
    if (condex* c = _cp->take_one()) {
        atrt.set_notify(c);
        bWake = true;
    }

    // Pick a valid sf
    int selsf = _selid;
    if (_selid==0) {
        selsf = URand(1,_qf);
    }

    // Decide which ID inside that SF to use
    int selid = (selsf-1)*TM1_SUBS_PER_SF + URand(1,TM1_SUBS_PER_SF);

    // Get one action from the trash stack
    trx_request_t* arequest = new (_env->_request_pool) trx_request_t;
    tid_t atid;
    arequest->set(NULL,atid,xctid,atrt,xct_type,selid);

    // Enqueue to the worker thread
    assert (_worker);
    _worker->enqueue(arequest,bWake);
    return (RCOK);
}



EXIT_NAMESPACE(tm1);


