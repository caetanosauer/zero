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

/** @file:   qpipe_ssb_xct.cpp
 *
 *  @brief:  Declaration of the QPIPE SSB transactions
 *
 *  @author: Manos Athanassoulis
 *  @date:   June 2010
 */

#include "workload/ssb/shore_ssb_env.h"
#include "qpipe/stages.h"
#include "qpipe/qcommon.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(ssb);


/********
 ******** Caution: The functions below should be invoked inside
 ********          the context of a smthread
 ********/


/******************************************************************** 
 *
 * SSB QPIPE TRXS
 *
 * (1) The qpipe_XXX functions are wrappers to the real transactions
 * (2) The xct_qpipe_XXX functions are the implementation of the transactions
 *
 ********************************************************************/


/******************************************************************** 
 *
 * SSB QPIPE TRXs Wrappers
 *
 * @brief: They are wrappers to the functions that execute the transaction
 *         body. Their responsibility is to:
 *
 *         1. Prepare the corresponding input
 *         2. Check the return of the trx function and abort the trx,
 *            if something went wrong
 *         3. Update the ssb db environment statistics
 *
 ********************************************************************/


// --- without input specified --- //
DEFINE_QPIPE_TRX(ShoreSSBEnv,qpart);
DEFINE_QPIPE_TRX(ShoreSSBEnv,qdate);
DEFINE_QPIPE_TRX(ShoreSSBEnv,qsupplier);
DEFINE_QPIPE_TRX(ShoreSSBEnv,qcustomer);
DEFINE_QPIPE_TRX(ShoreSSBEnv,qlineorder);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q1_1);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q1_2);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q1_3);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q2_1);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q2_2);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q2_3);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q3_1);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q3_2);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q3_3);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q3_4);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q4_1);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q4_2);
DEFINE_QPIPE_TRX(ShoreSSBEnv,q4_3);


/********************************************************************* 
 *
 *  @fn:    run_one_qpipe_xct
 *
 *  @note:  Call the execution of a QPIPE SSB query
 *
 *********************************************************************/

w_rc_t ShoreSSBEnv::run_one_qpipe_xct(Request* prequest)
{
    // if QPIPE SSB MIX

    int type = prequest->type();
    assert (type >= XCT_QPIPE_SSB_MIX);
    if (type == XCT_QPIPE_SSB_MIX) {
        type = abs(smthread_t::me()->rand()%22);
        //prequest->set_type(XCT_SSB_MIX + smthread_t::me()->rand()%22);
    }
    else {
        type -= (XCT_QPIPE_SSB_MIX - XCT_SSB_MIX);
    }

    switch (type) {

        // SSB QPIPE
    case XCT_SSB_QDATE:
        return (run_qpipe_qdate(prequest));

    case XCT_SSB_QPART:
        return (run_qpipe_qpart(prequest));

    case XCT_SSB_QSUPPLIER:
        return (run_qpipe_qsupplier(prequest));

    case XCT_SSB_QCUSTOMER:
        return (run_qpipe_qcustomer(prequest));

    case XCT_SSB_QLINEORDER:
        return (run_qpipe_qlineorder(prequest));

    case XCT_SSB_Q1_1:
        return (run_qpipe_q1_1(prequest));

    case XCT_SSB_Q1_2: 
        return (run_qpipe_q1_2(prequest));

    case XCT_SSB_Q1_3: 
        return (run_qpipe_q1_3(prequest));

    case XCT_SSB_Q2_1:
        return (run_qpipe_q2_1(prequest));

    case XCT_SSB_Q2_2: 
        return (run_qpipe_q2_2(prequest));

    case XCT_SSB_Q2_3:
        return (run_qpipe_q2_3(prequest));

    case XCT_SSB_Q3_1: 
        return (run_qpipe_q3_1(prequest));

    case XCT_SSB_Q3_2: 
        return (run_qpipe_q3_2(prequest));

    case XCT_SSB_Q3_3: 
        return (run_qpipe_q3_3(prequest));

    case XCT_SSB_Q3_4:
        return (run_qpipe_q3_4(prequest));

    case XCT_SSB_Q4_1:
        return (run_qpipe_q4_1(prequest));

    case XCT_SSB_Q4_2:
        return (run_qpipe_q4_2(prequest));

    case XCT_SSB_Q4_3:
        return (run_qpipe_q4_3(prequest));

    default:
        //assert (0); // UNKNOWN TRX-ID
        TRACE( TRACE_ALWAYS, "Unknown transaction\n");
    }
    return (RCOK);
}


// // --- with input specified --- //
// Moved to a corresponding (separate) qpipe/qpipe_qX.cpp file 


EXIT_NAMESPACE(qpipe);
