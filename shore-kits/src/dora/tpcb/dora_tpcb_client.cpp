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

/** @file:   dora_tpcb_client.cpp
 *
 *  @brief:  Implementation of the DORA client for the TPCB benchmark
 *
 *  @author: Ippokratis Pandis, May 2009
 */

#include "dora/tpcb/dora_tpcb_client.h"


ENTER_NAMESPACE(dora);


// Look also at include/workload/tpcb/tpcb_const.h
const int XCT_TPCB_DORA_ACCT_UPDATE   = 331;



/********************************************************************* 
 *
 *  dora_tpcb_client_t
 *
  *********************************************************************/

int dora_tpcb_client_t::load_sup_xct(mapSupTrxs& stmap)
{
    // clears the supported trx map and loads its own
    stmap.clear();

    // Baseline TPCB trxs
    stmap[XCT_TPCB_DORA_ACCT_UPDATE]    = "DORA-TPCB-AcctUpd";
    return (stmap.size());
}


/********************************************************************* 
 *
 *  @fn:    submit_one
 *
 *  @brief: Entry point for running one DORA TPC-B xct 
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure internal has expired.
 *
 *********************************************************************/
 
w_rc_t dora_tpcb_client_t::submit_one(int xct_type, int xctid) 
{
    // if DORA TPCB MIX
    bool bWake = false;

    // Pick a valid sf
    int selid = _selid;

//     if (_selid==0)
//         selid = URand(1,_qf);

    trx_result_tuple_t atrt;
    if (condex* c = _cp->take_one()) {
        atrt.set_notify(c);
        bWake = true;
    }
    
    switch (xct_type) {

        // TPCB DORA
    case XCT_TPCB_DORA_ACCT_UPDATE:
        return (_tpcbdb->dora_acct_update(xctid,atrt,selid,bWake));

    default:
        assert (0); // UNKNOWN TRX-ID
    }
    return (RCOK);
}



EXIT_NAMESPACE(dora);


