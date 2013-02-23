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

/** @file:   shore_tpch_client.cpp
 *
 *  @brief:  Implementation of the client for the TPCH benchmark
 *
 *  @author: Manos Athanassoulis, February 2012
 *  @author: Nastaran Nikparto, Summer 2011
 *  @author: Ippokratis Pandis, July 2008
 */

#include "workload/tpch/shore_tpch_client.h"

ENTER_NAMESPACE(tpch);


/*********************************************************************
 *
 *  baseline_tpch_client_t
 *
 *********************************************************************/

baseline_tpch_client_t::baseline_tpch_client_t(c_str tname, const int id,
                                               ShoreTPCHEnv* env,
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

int baseline_tpch_client_t::load_sup_xct(mapSupTrxs& stmap)
{
    // clears the supported trx map and loads its own
    stmap.clear();

    // Baseline TPC-H trxs
    stmap[XCT_TPCH_MIX]          = "TPCH-Mix";
    stmap[XCT_TPCH_Q1]           = "TPCH-Q1";
    stmap[XCT_TPCH_Q2]           = "TPCH-Q2";
    stmap[XCT_TPCH_Q3]           = "TPCH-Q3";
    stmap[XCT_TPCH_Q4]           = "TPCH-Q4";
    stmap[XCT_TPCH_Q5]           = "TPCH-Q5";
    stmap[XCT_TPCH_Q6]           = "TPCH-Q6";
    stmap[XCT_TPCH_Q7]           = "TPCH-Q7";
    stmap[XCT_TPCH_Q8]           = "TPCH-Q8";
    stmap[XCT_TPCH_Q9]           = "TPCH-Q9";
    stmap[XCT_TPCH_Q10]          = "TPCH-Q10";
    stmap[XCT_TPCH_Q11]          = "TPCH-Q11";
    stmap[XCT_TPCH_Q12]          = "TPCH-Q12";
    stmap[XCT_TPCH_Q13]          = "TPCH-Q13";
    stmap[XCT_TPCH_Q14]          = "TPCH-Q14";
    stmap[XCT_TPCH_Q15]          = "TPCH-Q15";
    stmap[XCT_TPCH_Q16]          = "TPCH-Q16";
    stmap[XCT_TPCH_Q17]          = "TPCH-Q17";
    stmap[XCT_TPCH_Q18]          = "TPCH-Q18";
    stmap[XCT_TPCH_Q19]          = "TPCH-Q19";
    stmap[XCT_TPCH_Q20]          = "TPCH-Q20";
    stmap[XCT_TPCH_Q21]          = "TPCH-Q21";
    stmap[XCT_TPCH_QLINEITEM]      = "TPCH-QLINEITEM";
    stmap[XCT_TPCH_QORDERS]        = "TPCH-QORDERS";
    stmap[XCT_TPCH_QREGION]        = "TPCH-QREGION";
    stmap[XCT_TPCH_QNATION]        = "TPCH-QNATION";
    stmap[XCT_TPCH_QCUSTOMER]      = "TPCH-QCUSTOMER";
    stmap[XCT_TPCH_QSUPPLIER]      = "TPCH-QSUPPLIER";
    stmap[XCT_TPCH_QPART]          = "TPCH-QPART";
    stmap[XCT_TPCH_QPARTSUPP]      = "TPCH-QPARTSUPP";



#ifdef CFG_QPIPE

    stmap[XCT_QPIPE_TPCH_MIX]               = "QPIPE-TPCH-MIX";

    uint qpipebase = XCT_QPIPE_TPCH_MIX - XCT_TPCH_MIX;
    stmap[qpipebase + XCT_TPCH_Q1]          = "QPIPE-TPCH-Q1";
    stmap[qpipebase + XCT_TPCH_Q2]          = "QPIPE-TPCH-Q2";
    stmap[qpipebase + XCT_TPCH_Q3]          = "QPIPE-TPCH-Q3";
    stmap[qpipebase + XCT_TPCH_Q4]          = "QPIPE-TPCH-Q4";
    stmap[qpipebase + XCT_TPCH_Q5]          = "QPIPE-TPCH-Q5";
    stmap[qpipebase + XCT_TPCH_Q6]          = "QPIPE-TPCH-Q6";
    stmap[qpipebase + XCT_TPCH_Q7]          = "QPIPE-TPCH-Q7";
    stmap[qpipebase + XCT_TPCH_Q8]          = "QPIPE-TPCH-Q8";
    stmap[qpipebase + XCT_TPCH_Q9]          = "QPIPE-TPCH-Q9";
    stmap[qpipebase + XCT_TPCH_Q10]         = "QPIPE-TPCH-Q10";
    stmap[qpipebase + XCT_TPCH_Q11]         = "QPIPE-TPCH-Q11";
    stmap[qpipebase + XCT_TPCH_Q12]         = "QPIPE-TPCH-Q12";
    stmap[qpipebase + XCT_TPCH_Q13]         = "QPIPE-TPCH-Q13";
    stmap[qpipebase + XCT_TPCH_Q14]         = "QPIPE-TPCH-Q14";
    stmap[qpipebase + XCT_TPCH_Q15]         = "QPIPE-TPCH-Q15";
    stmap[qpipebase + XCT_TPCH_Q16]         = "QPIPE-TPCH-Q16";
    stmap[qpipebase + XCT_TPCH_Q17]         = "QPIPE-TPCH-Q17";
    stmap[qpipebase + XCT_TPCH_Q18]         = "QPIPE-TPCH-Q18";
    stmap[qpipebase + XCT_TPCH_Q19]         = "QPIPE-TPCH-Q19";
    stmap[qpipebase + XCT_TPCH_Q20]         = "QPIPE-TPCH-Q20";
    stmap[qpipebase + XCT_TPCH_Q21]         = "QPIPE-TPCH-Q21";
    stmap[qpipebase + XCT_TPCH_Q22]         = "QPIPE-TPCH-Q22";
    stmap[qpipebase + XCT_TPCH_QLINEITEM]   = "QPIPE-TPCH-QLINEITEM";
    stmap[qpipebase + XCT_TPCH_QORDERS]     = "QPIPE-TPCH-QORDERS";
    stmap[qpipebase + XCT_TPCH_QREGION]     = "QPIPE-TPCH-QREGION";
    stmap[qpipebase + XCT_TPCH_QNATION]     = "QPIPE-TPCH-QNATION";
    stmap[qpipebase + XCT_TPCH_QCUSTOMER]   = "QPIPE-TPCH-QCUSTOMER";
    stmap[qpipebase + XCT_TPCH_QSUPPLIER]   = "QPIPE-TPCH-QSUPPLIER";
    stmap[qpipebase + XCT_TPCH_QPART]       = "QPIPE-TPCH-QPART";
    stmap[qpipebase + XCT_TPCH_QPARTSUPP]   = "QPIPE-TPCH-QPARTSUPP";

#endif


    return (stmap.size());
}


/*********************************************************************
 *
 *  @fn:    submit_one
 *
 *  @brief: Entry point for running one TPC-H query (trx)
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure internal has expired.
 *
 *********************************************************************/

w_rc_t baseline_tpch_client_t::submit_one(int xct_type, int xctid)
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


EXIT_NAMESPACE(tpch);
