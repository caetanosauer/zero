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

/** @file:   dora_tm1_impl.h
 *
 *  @brief:  DORA TM1 TRXs
 *
 *  @note:   Definition of RVPs and Actions that synthesize (according to DORA)
 *           the TM1 trx
 *
 *  @author: Ippokratis Pandis, Feb 2009
 */


#ifndef __DORA_TM1_IMPL_H
#define __DORA_TM1_IMPL_H


#include "dora.h"
#include "workload/tm1/shore_tm1_env.h"
#include "dora/tm1/dora_tm1.h"

using namespace shore;
using namespace tm1;


ENTER_NAMESPACE(dora);



/******************************************************************** 
 *
 * DORA TM1 GET_SUB_DATA
 *
 ********************************************************************/

DECLARE_DORA_FINAL_RVP_CLASS(final_gsd_rvp,DoraTM1Env,1,1);

DECLARE_DORA_ACTION_NO_RVP_CLASS(r_sub_gsd_action,int,DoraTM1Env,get_sub_data_input_t,1);



/******************************************************************** 
 *
 * DORA TM1 GET_NEW_DEST
 *
 ********************************************************************/
#ifdef TM1GND2
DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid_gnd_rvp,DoraTM1Env,get_new_dest_input_t,1,1);
DECLARE_DORA_FINAL_RVP_CLASS(final_gnd_rvp,DoraTM1Env,1,2);

DECLARE_DORA_ACTION_WITH_RVP_CLASS(r_sf_gnd_action,int,DoraTM1Env,mid_gnd_rvp,get_new_dest_input_t,2);

DECLARE_DORA_ACTION_NO_RVP_CLASS(r_cf_gnd_action,int,DoraTM1Env,get_new_dest_input_t,3);
#else
DECLARE_DORA_FINAL_RVP_CLASS(final_gnd_rvp,DoraTM1Env,2,2);

DECLARE_DORA_ACTION_NO_RVP_CLASS(r_sf_gnd_action,int,DoraTM1Env,get_new_dest_input_t,2);
DECLARE_DORA_ACTION_NO_RVP_CLASS(r_cf_gnd_action,int,DoraTM1Env,get_new_dest_input_t,3);
#endif


/******************************************************************** 
 *
 * DORA TM1 GET_ACC_DATA
 *
 ********************************************************************/

DECLARE_DORA_FINAL_RVP_CLASS(final_gad_rvp,DoraTM1Env,1,1);

DECLARE_DORA_ACTION_NO_RVP_CLASS(r_ai_gad_action,int,DoraTM1Env,get_acc_data_input_t,2);



/******************************************************************** 
 *
 * DORA TM1 UPD_SUB_DATA
 *
 ********************************************************************/
#ifdef TM1USD2
DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid_usd_rvp,DoraTM1Env,upd_sub_data_input_t,1,1);
DECLARE_DORA_FINAL_RVP_CLASS(final_usd_rvp,DoraTM1Env,1,2);

DECLARE_DORA_ACTION_WITH_RVP_CLASS(upd_sf_usd_action,int,DoraTM1Env,mid_usd_rvp,upd_sub_data_input_t,2);

DECLARE_DORA_ACTION_NO_RVP_CLASS(upd_sub_usd_action,int,DoraTM1Env,upd_sub_data_input_t,1);
#else
DECLARE_DORA_FINAL_RVP_CLASS(final_usd_rvp,DoraTM1Env,2,2);

DECLARE_DORA_ACTION_NO_RVP_CLASS(upd_sub_usd_action,int,DoraTM1Env,upd_sub_data_input_t,1);
DECLARE_DORA_ACTION_NO_RVP_CLASS(upd_sf_usd_action,int,DoraTM1Env,upd_sub_data_input_t,2);
#endif


/******************************************************************** 
 *
 * DORA TM1 UPD_SUB_DATA MIX
 *
 ********************************************************************/
DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid_usdmix_rvp,DoraTM1Env,upd_sub_data_input_t,1,1);
DECLARE_DORA_FINAL_RVP_CLASS(final_usdmix_rvp,DoraTM1Env,1,2);

DECLARE_DORA_ACTION_WITH_RVP_CLASS(upd_sub_usdmix_action,int,DoraTM1Env,mid_usdmix_rvp,upd_sub_data_input_t,1);

DECLARE_DORA_ACTION_NO_RVP_CLASS(upd_sf_usdmix_action,int,DoraTM1Env,upd_sub_data_input_t,2);


/******************************************************************** 
 *
 * DORA TM1 UPD_LOC
 *
 ********************************************************************/

DECLARE_DORA_FINAL_RVP_CLASS(final_ul_rvp,DoraTM1Env,1,1);

DECLARE_DORA_ACTION_NO_RVP_CLASS(upd_sub_ul_action,int,DoraTM1Env,upd_loc_input_t,1);



/******************************************************************** 
 *
 * DORA TM1 INS_CALL_FWD
 *
 ********************************************************************/
#ifdef TM1ICF2
DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid1_icf_rvp,DoraTM1Env,ins_call_fwd_input_t,1,1);
DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid2_icf_rvp,DoraTM1Env,ins_call_fwd_input_t,1,2);
DECLARE_DORA_FINAL_RVP_CLASS(final_icf_rvp,DoraTM1Env,1,3);

DECLARE_DORA_ACTION_WITH_RVP_CLASS(r_sub_icf_action,int,DoraTM1Env,mid1_icf_rvp,ins_call_fwd_input_t,1);
DECLARE_DORA_ACTION_WITH_RVP_CLASS(r_sf_icf_action,int,DoraTM1Env,mid2_icf_rvp,ins_call_fwd_input_t,2);
DECLARE_DORA_ACTION_NO_RVP_CLASS(ins_cf_icf_action,int,DoraTM1Env,ins_call_fwd_input_t,3);
#else
DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid_icf_rvp,DoraTM1Env,ins_call_fwd_input_t,1,1);
DECLARE_DORA_FINAL_RVP_CLASS(final_icf_rvp,DoraTM1Env,2,3);

DECLARE_DORA_ACTION_WITH_RVP_CLASS(r_sub_icf_action,int,DoraTM1Env,mid_icf_rvp,ins_call_fwd_input_t,1);
DECLARE_DORA_ACTION_NO_RVP_CLASS(r_sf_icf_action,int,DoraTM1Env,ins_call_fwd_input_t,2);
DECLARE_DORA_ACTION_NO_RVP_CLASS(ins_cf_icf_action,int,DoraTM1Env,ins_call_fwd_input_t,3);
#endif



/******************************************************************** 
 *
 * DORA TM1 INS_CALL_FWD_BENCH
 *
 ********************************************************************/

DECLARE_DORA_FINAL_RVP_CLASS(final_icfb_rvp,DoraTM1Env,2,2);

DECLARE_DORA_ACTION_NO_RVP_CLASS(r_sub_icfb_action,int,DoraTM1Env,ins_call_fwd_bench_input_t,1);
DECLARE_DORA_ACTION_NO_RVP_CLASS(i_cf_icfb_action,int,DoraTM1Env,ins_call_fwd_bench_input_t,3);


/******************************************************************** 
 *
 * DORA TM1 DEL_CALL_FWD
 *
 ********************************************************************/

DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(mid_dcf_rvp,DoraTM1Env,del_call_fwd_input_t,1,1);
DECLARE_DORA_FINAL_RVP_CLASS(final_dcf_rvp,DoraTM1Env,1,2);

DECLARE_DORA_ACTION_WITH_RVP_CLASS(r_sub_dcf_action,int,DoraTM1Env,mid_dcf_rvp,del_call_fwd_input_t,1);
DECLARE_DORA_ACTION_NO_RVP_CLASS(del_cf_dcf_action,int,DoraTM1Env,del_call_fwd_input_t,3);



/******************************************************************** 
 *
 * DORA TM1 GET_SUB_NBR
 *
 ********************************************************************/

DECLARE_DORA_FINAL_DYNAMIC_RVP_CLASS(final_gsn_rvp,DoraTM1Env);
#ifndef USE_DORA_EXT_IDX
DECLARE_DORA_ACTION_NO_RVP_CLASS(r_sub_gsn_action,int,DoraTM1Env,get_sub_nbr_input_t,1);
#else
DECLARE_DORA_ACTION_NO_RVP_CLASS(r_sub_gsn_acc_action,int,DoraTM1Env,get_sub_nbr_input_t,1);
#endif


EXIT_NAMESPACE(dora);

#endif /** __DORA_TM1_IMPL_H */

