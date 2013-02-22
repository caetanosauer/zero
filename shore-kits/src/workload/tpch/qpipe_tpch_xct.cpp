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

/** @file:   qpipe_tpch_xct.cpp
 *
 *  @brief:  Declaration of the QPIPE TPCH transactions
 *
 *  @author: Ippokratis Pandis, Manos Athanassoulis
 *  @date:   Apr 2010
 */

#include "workload/tpch/shore_tpch_env.h"
#include "qpipe/stages.h"
#include "qpipe/qcommon.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(tpch);


/********
 ******** Caution: The functions below should be invoked inside
 ********          the context of a smthread
 ********/


/********************************************************************
 *
 * TPCH QPIPE TRXS
 *
 * (1) The qpipe_XXX functions are wrappers to the real transactions
 * (2) The xct_qpipe_XXX functions are the implementation of the transactions
 *
 ********************************************************************/


/********************************************************************
 *
 * TPCH QPIPE TRXs Wrappers
 *
 * @brief: They are wrappers to the functions that execute the transaction
 *         body. Their responsibility is to:
 *
 *         1. Prepare the corresponding input
 *         2. Check the return of the trx function and abort the trx,
 *            if something went wrong
 *         3. Update the tpch db environment statistics
 *
 ********************************************************************/


// --- without input specified --- //
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q1);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q2);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q3);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q4);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q5);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q6);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q7);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q8);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q9);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q10);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q11);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q12);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q13);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q14);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q15);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q16);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q17);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q18);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q19);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q20);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q21);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,q22);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,qlineitem);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,qorders);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,qpart);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,qpartsupp);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,qsupplier);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,qnation);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,qregion);
DEFINE_QPIPE_TRX(ShoreTPCHEnv,qcustomer);



/*********************************************************************
 *
 *  @fn:    run_one_qpipe_xct
 *
 *  @note:  Call the execution of a QPIPE TPC-H query
 *
 *********************************************************************/

w_rc_t ShoreTPCHEnv::run_one_qpipe_xct(Request* prequest)
{
    // if QPIPE TPC-H MIX

    int type = prequest->type();
    assert (type >= XCT_QPIPE_TPCH_MIX);
    if (type == XCT_QPIPE_TPCH_MIX) {
        type = abs(smthread_t::me()->rand()%22);
        //prequest->set_type(XCT_TPCH_MIX + smthread_t::me()->rand()%22);
    }
    else {
        type -= (XCT_QPIPE_TPCH_MIX - XCT_TPCH_MIX);
    }

    switch (type) {

        // TPCH QPIPE
    case XCT_TPCH_Q1:
        return (run_qpipe_q1(prequest));

    case XCT_TPCH_Q2:
        return (run_qpipe_q2(prequest));

    case XCT_TPCH_Q3:
        return (run_qpipe_q3(prequest));

    case XCT_TPCH_Q4:
        return (run_qpipe_q4(prequest));

    case XCT_TPCH_Q5:
        return (run_qpipe_q5(prequest));

    case XCT_TPCH_Q6:
        return (run_qpipe_q6(prequest));

    case XCT_TPCH_Q7:
        return (run_qpipe_q7(prequest));

    case XCT_TPCH_Q8:
        return (run_qpipe_q8(prequest));

    case XCT_TPCH_Q9:
        return (run_qpipe_q9(prequest));

    case XCT_TPCH_Q10:
        return (run_qpipe_q10(prequest));

    case XCT_TPCH_Q11:
        return (run_qpipe_q11(prequest));

    case XCT_TPCH_Q12:
        return (run_qpipe_q12(prequest));

    case XCT_TPCH_Q13:
        return (run_qpipe_q13(prequest));

    case XCT_TPCH_Q14:
        return (run_qpipe_q14(prequest));

    case XCT_TPCH_Q15:
        return (run_qpipe_q15(prequest));

    case XCT_TPCH_Q16:
        return (run_qpipe_q16(prequest));

    case XCT_TPCH_Q17:
        return (run_qpipe_q17(prequest));

    case XCT_TPCH_Q18:
        return (run_qpipe_q18(prequest));

    case XCT_TPCH_Q19:
        return (run_qpipe_q19(prequest));

    case XCT_TPCH_Q20:
        return (run_qpipe_q20(prequest));

    case XCT_TPCH_Q21:
        return (run_qpipe_q21(prequest));

    case XCT_TPCH_Q22:
        return (run_qpipe_q22(prequest));

    case XCT_TPCH_QLINEITEM:
	return (run_qpipe_qlineitem(prequest));

    case XCT_TPCH_QORDERS:
	return (run_qpipe_qorders(prequest));

    case XCT_TPCH_QREGION:
	return (run_qpipe_qregion(prequest));

    case XCT_TPCH_QNATION:
	return (run_qpipe_qnation(prequest));

    case XCT_TPCH_QCUSTOMER:
	return (run_qpipe_qcustomer(prequest));

    case XCT_TPCH_QSUPPLIER:
	return (run_qpipe_qsupplier(prequest));

    case XCT_TPCH_QPART:
	return (run_qpipe_qpart(prequest));

    case XCT_TPCH_QPARTSUPP:
	return (run_qpipe_qpartsupp(prequest));

    default:
        //assert (0); // UNKNOWN TRX-ID
        TRACE( TRACE_ALWAYS, "Unknown transaction\n");
    }
    return (RCOK);
}


// --- with input specified --- //
// Moved to a corresponding (separate) qpipe/qpipe_qX.cpp file

EXIT_NAMESPACE(qpipe);
