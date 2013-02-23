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

/** @file:   shore_ssb_client.cpp
 *
 *  @brief:  Implementation of the client for the SSB benchmark
 *
 *  @author: Manos Athanassoulis, June 2010
 */

#include "workload/ssb/shore_ssb_client.h"

ENTER_NAMESPACE(ssb);


/********************************************************************* 
 *
 *  baseline_ssb_client_t
 *
 *********************************************************************/

baseline_ssb_client_t::baseline_ssb_client_t(c_str tname, const int id, 
                                               ShoreSSBEnv* env, 
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

int baseline_ssb_client_t::load_sup_xct(mapSupTrxs& stmap)
{
    // clears the supported trx map and loads its own
    stmap.clear();

    // Baseline SSB trxs
        stmap[XCT_SSB_MIX]          = "SSB-Mix";
    //     stmap[XCT_SSB_Q1_1]           = "SSB-Q1_1";
//     stmap[XCT_SSB_Q1_2]           = "SSB-Q1_2";
//     stmap[XCT_SSB_Q1_3]           = "SSB-Q1_3";
//     stmap[XCT_SSB_Q2_1]           = "SSB-Q2_1";
//     stmap[XCT_SSB_Q2_2]           = "SSB-Q2_2";
//     stmap[XCT_SSB_Q2_3]           = "SSB-Q2_3";
//     stmap[XCT_SSB_Q3_1]           = "SSB-Q3_1";
//     stmap[XCT_SSB_Q3_2]           = "SSB-Q3_2";
//     stmap[XCT_SSB_Q3_3]           = "SSB-Q3_3";
//     stmap[XCT_SSB_Q3_4]           = "SSB-Q3_4";
//     stmap[XCT_SSB_Q4_1]           = "SSB-Q4_1";
//     stmap[XCT_SSB_Q4_2]           = "SSB-Q4_2";
//     stmap[XCT_SSB_Q4_3]           = "SSB-Q4_3";



#ifdef CFG_QPIPE

    stmap[XCT_QPIPE_SSB_MIX]               = "QPIPE-SSB-MIX";

    uint qpipebase = XCT_QPIPE_SSB_MIX - XCT_SSB_MIX;

     stmap[qpipebase + XCT_SSB_Q1_1]           = "QPIPE-SSB-Q1_1";
     stmap[qpipebase + XCT_SSB_Q1_2]           = "QPIPE-SSB-Q1_2";
     stmap[qpipebase + XCT_SSB_Q1_3]           = "QPIPE-SSB-Q1_3";
     stmap[qpipebase + XCT_SSB_Q2_1]           = "QPIPE-SSB-Q2_1";
     stmap[qpipebase + XCT_SSB_Q2_2]           = "QPIPE-SSB-Q2_2";
     stmap[qpipebase + XCT_SSB_Q2_3]           = "QPIPE-SSB-Q2_3";
     stmap[qpipebase + XCT_SSB_Q3_1]           = "QPIPE-SSB-Q3_1";
     stmap[qpipebase + XCT_SSB_Q3_2]           = "QPIPE-SSB-Q3_2";
     stmap[qpipebase + XCT_SSB_Q3_3]           = "QPIPE-SSB-Q3_3";
     stmap[qpipebase + XCT_SSB_Q3_4]           = "QPIPE-SSB-Q3_4";
     stmap[qpipebase + XCT_SSB_Q4_1]           = "QPIPE-SSB-Q4_1";
     stmap[qpipebase + XCT_SSB_Q4_2]           = "QPIPE-SSB-Q4_2";
     stmap[qpipebase + XCT_SSB_Q4_3]           = "QPIPE-SSB-Q4_3";
     stmap[qpipebase + XCT_SSB_QPART]            = "QPIPE-SSB-QPART";
     stmap[qpipebase + XCT_SSB_QDATE]            = "QPIPE-SSB-QDATE";
     stmap[qpipebase + XCT_SSB_QSUPPLIER]        = "QPIPE-SSB-QSUPPLIER";
     stmap[qpipebase + XCT_SSB_QCUSTOMER]        = "QPIPE-SSB-QCUSTOMER";
     stmap[qpipebase + XCT_SSB_QLINEORDER]       = "QPIPE-SSB-QLINEORDER";


#endif

    
    return (stmap.size());
}


/********************************************************************* 
 *
 *  @fn:    submit_one
 *
 *  @brief: Entry point for running one SSB query (trx) 
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure internal has expired.
 *
 *********************************************************************/
 
w_rc_t baseline_ssb_client_t::submit_one(int xct_type, int xctid) 
{
    // Set input
    trx_result_tuple_t atrt;
    bool bWake = false;
    if (condex* c = _cp->take_one()) {
        TRACE( TRACE_TRX_FLOW, "Sleeping\n");
        atrt.set_notify(c);
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


EXIT_NAMESPACE(ssb);
