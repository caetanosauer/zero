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

/** @file:   dora_tm1_client.cpp
 *
 *  @brief:  Implementation of the DORA client for the TM1 benchmark
 *
 *  @author: Ippokratis Pandis, May 2009
 */

#include "dora/tm1/dora_tm1_client.h"


ENTER_NAMESPACE(dora);


// Look also at include/workload/tm1/tm1_const.h
// @note: The DORA_XXX should be (DORA_MIX + REGULAR_TRX_ID)
const int XCT_TM1_DORA_MIX           = 200;
const int XCT_TM1_DORA_GET_SUB_DATA  = 221;
const int XCT_TM1_DORA_GET_NEW_DEST  = 222;
const int XCT_TM1_DORA_GET_ACC_DATA  = 223;
const int XCT_TM1_DORA_UPD_SUB_DATA  = 224;
const int XCT_TM1_DORA_UPD_LOCATION  = 225;
const int XCT_TM1_DORA_CALL_FWD_MIX  = 226;

const int XCT_TM1_DORA_INS_CALL_FWD  = 227;
const int XCT_TM1_DORA_DEL_CALL_FWD  = 228;

const int XCT_TM1_DORA_GET_SUB_NBR   = 229;

const int XCT_TM1_DORA_CALL_FWD_MIX_BENCH  = 230;
const int XCT_TM1_DORA_INS_CALL_FWD_BENCH  = 231;
const int XCT_TM1_DORA_DEL_CALL_FWD_BENCH  = 232;

const int XCT_TM1_DORA_UPD_SUB_DATA_MIX  = 234;

/********************************************************************* 
 *
 *  dora_tm1_client_t
 *
  *********************************************************************/

int dora_tm1_client_t::load_sup_xct(mapSupTrxs& stmap)
{
    // clears the supported trx map and loads its own
    stmap.clear();

    // Baseline TM1 trxs
    stmap[XCT_TM1_DORA_MIX]             = "DORA-TM1-Mix";
    stmap[XCT_TM1_DORA_GET_SUB_DATA]    = "DORA-TM1-GetSubData";
    stmap[XCT_TM1_DORA_GET_NEW_DEST]    = "DORA-TM1-GetNewDest";
    stmap[XCT_TM1_DORA_GET_ACC_DATA]    = "DORA-TM1-GetAccData";
    stmap[XCT_TM1_DORA_UPD_SUB_DATA]    = "DORA-TM1-UpdSubData";
    stmap[XCT_TM1_DORA_UPD_LOCATION]    = "DORA-TM1-UpdLocation";
    stmap[XCT_TM1_DORA_CALL_FWD_MIX]    = "DORA-TM1-CallFwd-Mix";
    stmap[XCT_TM1_DORA_INS_CALL_FWD]    = "DORA-TM1-InsCallFwd";
    stmap[XCT_TM1_DORA_DEL_CALL_FWD]    = "DORA-TM1-DelCallFwd";

#ifndef USE_DORA_EXT_IDX
    stmap[XCT_TM1_DORA_GET_SUB_NBR]     = "DORA-TM1-GetSubNbr";
#else
    stmap[XCT_TM1_DORA_GET_SUB_NBR]     = "DORA-TM1-GetSubNbr-Ext";
#endif

    stmap[XCT_TM1_DORA_CALL_FWD_MIX_BENCH]    = "DORA-TM1-CallFwdMixBench";
    stmap[XCT_TM1_DORA_INS_CALL_FWD_BENCH]    = "DORA-TM1-InsCallFwdBench";
    stmap[XCT_TM1_DORA_DEL_CALL_FWD_BENCH]    = "DORA-TM1-DelCallFwdBench";

    return (stmap.size());
}

/********************************************************************* 
 *
 *  @fn:    submit_one
 *
 *  @brief: Entry point for running one DORA TM1 xct 
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure internal has expired.
 *
 *********************************************************************/
 
w_rc_t dora_tm1_client_t::submit_one(int xct_type, int xctid) 
{
    // if DORA TM1 MIX
    bool bWake = false;
    if (xct_type == XCT_TM1_DORA_MIX) {        
        xct_type = XCT_TM1_DORA_MIX + random_tm1_xct_type(rand(100));
	if(xct_type == XCT_TM1_DORA_UPD_SUB_DATA) {
	    xct_type = XCT_TM1_DORA_UPD_SUB_DATA_MIX;
	}
        bWake = true;
    }

    // pick a valid sf
    int selsf = _selid;

    // decide which SF to use
    if (_selid==0) {
        selsf = URand(1,_qf); 
        bWake = true;
    }

    // decide which ID inside that SF to use
    int selid = (selsf-1)*TM1_SUBS_PER_SF + URand(1,TM1_SUBS_PER_SF);

    trx_result_tuple_t atrt;
    if (condex* c = _cp->take_one()) {
        atrt.set_notify(c);
        bWake = true;
    }
    
    switch (xct_type) {

        // TM1 DORA
    case XCT_TM1_DORA_GET_SUB_DATA:
        return (_tm1db->dora_get_sub_data(xctid,atrt,selid,bWake));
    case XCT_TM1_DORA_GET_NEW_DEST:
        return (_tm1db->dora_get_new_dest(xctid,atrt,selid,bWake));
    case XCT_TM1_DORA_GET_ACC_DATA:
        return (_tm1db->dora_get_acc_data(xctid,atrt,selid,bWake));
    case XCT_TM1_DORA_UPD_SUB_DATA:
        return (_tm1db->dora_upd_sub_data(xctid,atrt,selid,bWake));
    case XCT_TM1_DORA_UPD_LOCATION:
        return (_tm1db->dora_upd_loc(xctid,atrt,selid,bWake));
    case XCT_TM1_DORA_INS_CALL_FWD:
        return (_tm1db->dora_ins_call_fwd(xctid,atrt,selid,bWake));
    case XCT_TM1_DORA_DEL_CALL_FWD:
        return (_tm1db->dora_del_call_fwd(xctid,atrt,selid,bWake));

        // Mix
    case XCT_TM1_DORA_CALL_FWD_MIX:
        // evenly pick one of the {Ins/Del}CallFwd
        if (URand(1,100)>50)
            return (_tm1db->dora_ins_call_fwd(xctid,atrt,selid,true)); // always wake in the mix
        else
            return (_tm1db->dora_del_call_fwd(xctid,atrt,selid,true));

    case XCT_TM1_DORA_GET_SUB_NBR:
        return (_tm1db->dora_get_sub_nbr(xctid,atrt,selid,bWake));

    case XCT_TM1_DORA_INS_CALL_FWD_BENCH:
        return (_tm1db->dora_ins_call_fwd_bench(xctid,atrt,selid,true));

    case XCT_TM1_DORA_UPD_SUB_DATA_MIX:
	return (_tm1db->dora_upd_sub_data_mix(xctid,atrt,selid,bWake));

    default:
        assert (0); // UNKNOWN TRX-ID
    }
    return (RCOK);
}


EXIT_NAMESPACE(dora);


