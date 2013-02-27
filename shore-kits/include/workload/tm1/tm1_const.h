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

/** @file:   tm1_const.h
 *
 *  @brief:  Constants needed by the TM1 kit
 *
 *  @author: Ippokratis Pandis, Feb 2009
 */

#ifndef __TM1_CONST_H
#define __TM1_CONST_H


#include "util/namespace.h"


ENTER_NAMESPACE(tm1);

// Define the USE_DORA_EXT_IDX in order to create the "correct"
// SUB_NBR_IDX (with the  
#undef  USE_DORA_EXT_IDX
//#define USE_DORA_EXT_IDX


// the scaling factor unit (arbitrarily chosen)
// SF = 1   --> 10K Subscribers
// SF = 100 --> 1M  Subscribers
const int TM1_SUBS_PER_SF = 10000;

const int TM1_DEF_SF = 10;
const int TM1_DEF_QF = 10;


// commit every 1000 new Subscribers
const int TM1_LOADING_COMMIT_INTERVAL = 1000;
const int TM1_LOADING_TRACE_INTERVAL  = 10000;


// loading-related defaults
//const int TM1_LOADERS_TO_USE     = 40;
const int TM1_LOADERS_TO_USE     = 1;
const int TM1_MAX_NUM_OF_LOADERS = 128;
const int TM1_SUBS_TO_PRELOAD = 2000; 


/* --- standard scale -- */

const int TM1_MIN_AI_PER_SUBSCR = 1;
const int TM1_MAX_AI_PER_SUBSCR = 4;

const int TM1_MIN_SF_PER_SUBSCR = 1;
const int TM1_MAX_SF_PER_SUBSCR = 4;

const int TM1_MIN_CF_PER_SF = 0;
const int TM1_MAX_CF_PER_SF = 3;

const int TM1_PROB_ACTIVE_SF_YES = 85;
const int TM1_PROB_ACTIVE_SF_NO  = 15;


/* --- number of fields per table --- */

// @note It is the actual number of fields
// @note When padding is used it should be +1

const int TM1_SUB_FCOUNT  = 34;
const int TM1_AI_FCOUNT   = 6; 
const int TM1_SF_FCOUNT   = 6; 
const int TM1_CF_FCOUNT   = 5; 


// some field sizes
const int TM1_SUB_NBR_SZ    = 16;
const int TM1_AI_DATA3_SZ   = 3;
const int TM1_AI_DATA4_SZ   = 5;
const int TM1_SF_DATA_B_SZ  = 5;
const int TM1_CF_NUMBERX_SZ = 15;




/* --------------------------- */
/* --- TM1 TRANSACTION MIX --- */
/* --------------------------- */

const int XCT_TM1_MIX           = 20;
const int XCT_TM1_GET_SUB_DATA  = 21;
const int XCT_TM1_GET_NEW_DEST  = 22;
const int XCT_TM1_GET_ACC_DATA  = 23;
const int XCT_TM1_UPD_SUB_DATA  = 24;
const int XCT_TM1_UPD_LOCATION  = 25;
const int XCT_TM1_CALL_FWD_MIX  = 26;

const int XCT_TM1_INS_CALL_FWD  = 27;
const int XCT_TM1_DEL_CALL_FWD  = 28;

const int XCT_TM1_GET_SUB_NBR   = 29;

const int XCT_TM1_CALL_FWD_MIX_BENCH  = 30;
const int XCT_TM1_INS_CALL_FWD_BENCH  = 31;
const int XCT_TM1_DEL_CALL_FWD_BENCH  = 32;




/* --- probabilities for the TM1 MIX --- */

// READ-ONLY (80%)
const int PROB_TM1_GET_SUB_DATA  = 35;
const int PROB_TM1_GET_NEW_DEST  = 10;
const int PROB_TM1_GET_ACC_DATA  = 35;

// UPDATE (20%)
const int PROB_TM1_UPD_SUB_DATA  = 2;
const int PROB_TM1_UPD_LOCATION  = 14;
const int PROB_TM1_INS_CALL_FWD  = 2;
const int PROB_TM1_DEL_CALL_FWD  = 2;
//const int PROB_TM1_INS_CALL_FWD_BENCH  = 2;
//const int PROB_TM1_DEL_CALL_FWD_BENCH  = 2;

EXIT_NAMESPACE(tm1);

#endif /* __TM1_CONST_H */
