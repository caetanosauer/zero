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

/** @file:   dora_tm1.h
 *
 *  @brief:  The DORA TM1 class
 *
 *  @author: Ippokratis Pandis, Feb 2009
 */


#ifndef __DORA_TM1_H
#define __DORA_TM1_H


#include <cstdio>

#include "tls.h"

#include "util.h"
#include "workload/tm1/shore_tm1_env.h"
#include "dora/dora_env.h"
#include "dora.h"

using namespace shore;
using namespace tm1;


ENTER_NAMESPACE(dora);



// Forward declarations

// TM1 GetSubData
class final_gsd_rvp;
class r_sub_gsd_action;

// TM1 GetNewDest
//#undef TM1GND2
#define TM1GND2
#ifdef TM1GND2
class final_gnd_rvp;
class mid_gnd_rvp;
class r_sf_gnd_action;
class r_cf_gnd_action;
#else
class final_gnd_rvp;
class r_sf_gnd_action;
class r_cf_gnd_action;
#endif

// TM1 GetAccData
class final_gad_rvp;
class r_ai_gad_action;

// TM1 UpdSubData
//#undef TM1USD2
#define TM1USD2
#ifdef TM1USD2
class final_usd_rvp;
class mid_usd_rvp;
class upd_sub_usd_action;
class upd_sf_usd_action;
#else
class final_usd_rvp;
class upd_sub_usd_action;
class upd_sf_usd_action;
#endif

// TM1 UpdSubData Mix
class final_usdmix_rvp;
class mid_usdmix_rvp;
class upd_sub_usdmix_action;
class upd_sf_usdmix_action;

// TM1 UpdLocation
class final_ul_rvp;
class upd_sub_ul_action;

// TM1 InsCallFwd
//#undef TM1ICF2
#define TM1ICF2
#ifdef TM1ICF2
class final_icf_rvp;
class mid1_icf_rvp;
class mid2_icf_rvp;
class r_sub_icf_action;
class r_sf_icf_action;
class ins_cf_icf_action;
#else
class final_icf_rvp;
class mid_icf_rvp;
class r_sub_icf_action;
class r_sf_icf_action;
class ins_cf_icf_action;
#endif

// TM1 InsCallFwdBench (mbench)
class final_icfb_rvp;
class r_sub_icfb_action;
class i_cf_icfb_action;



// TM1 DelCallFwd
class final_dcf_rvp;
class mid_dcf_rvp;
class r_sub_dcf_action;
class del_cf_dcf_action;

// TM1 GetSubNbr
class final_gsn_rvp;
#ifndef USE_DORA_EXT_IDX
class r_sub_gsn_action;
#else
class r_sub_gsn_acc_action;
#endif


/******************************************************************** 
 *
 * @class: dora_tm1
 *
 * @brief: Container class for all the data partitions for the TM1 database
 *
 ********************************************************************/

class DoraTM1Env : public ShoreTM1Env, public DoraEnv
{
public:
    
    DoraTM1Env();
    virtual ~DoraTM1Env();


    //// Control Database

    // {Start/Stop/Resume/Pause} the system 
    int start();
    int stop();
    int resume();
    int pause();
    w_rc_t newrun();
    int set(envVarMap* /* vars */) { return(0); /* do nothing */ };
    int dump();
    int info() const; 
    int statistics();    
    int conf();


    //// Partition-related
    w_rc_t update_partitioning();


    //// DORA TM1 - PARTITIONED TABLES

    DECLARE_DORA_PARTS(sub);
    DECLARE_DORA_PARTS(ai);
    DECLARE_DORA_PARTS(sf);
    DECLARE_DORA_PARTS(cf);



    //// DORA TM1 - TRXs   


    ////////////////
    // GetSubData //
    ////////////////

    DECLARE_DORA_TRX(get_sub_data);

    DECLARE_DORA_FINAL_RVP_GEN_FUNC(final_gsd_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(r_sub_gsd_action,rvp_t,get_sub_data_input_t);



    ////////////////
    // GetNewDest //
    ////////////////

    DECLARE_DORA_TRX(get_new_dest);

#ifdef TM1GND2
    DECLARE_DORA_MIDWAY_RVP_GEN_FUNC(mid_gnd_rvp,get_new_dest_input_t);
    DECLARE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_gnd_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(r_sf_gnd_action,mid_gnd_rvp,get_new_dest_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(r_cf_gnd_action,rvp_t,get_new_dest_input_t);
#else
    DECLARE_DORA_FINAL_RVP_GEN_FUNC(final_gnd_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(r_sf_gnd_action,rvp_t,get_new_dest_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(r_cf_gnd_action,rvp_t,get_new_dest_input_t);
#endif


    ////////////////
    // GetAccData //
    ////////////////

    DECLARE_DORA_TRX(get_acc_data);

    DECLARE_DORA_FINAL_RVP_GEN_FUNC(final_gad_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(r_ai_gad_action,rvp_t,get_acc_data_input_t);



    ////////////////
    // UpdSubData //
    ////////////////

    DECLARE_DORA_TRX(upd_sub_data);

#ifdef TM1USD2
    DECLARE_DORA_MIDWAY_RVP_GEN_FUNC(mid_usd_rvp,upd_sub_data_input_t);
    DECLARE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_usd_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(upd_sf_usd_action,mid_usd_rvp,upd_sub_data_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(upd_sub_usd_action,rvp_t,upd_sub_data_input_t);
#else
    DECLARE_DORA_FINAL_RVP_GEN_FUNC(final_usd_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(upd_sub_usd_action,rvp_t,upd_sub_data_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(upd_sf_usd_action,rvp_t,upd_sub_data_input_t);
#endif


    ////////////////////
    // UpdSubData Mix //
    ////////////////////

    // trxlid:  upd_sub_data
    // trximpl: upd_sub_data_mix

    DECLARE_ALTER_DORA_TRX(upd_sub_data,upd_sub_data_mix);
    
    DECLARE_DORA_MIDWAY_RVP_GEN_FUNC(mid_usdmix_rvp,upd_sub_data_input_t);
    DECLARE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_usdmix_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(upd_sub_usdmix_action,mid_usdmix_rvp,upd_sub_data_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(upd_sf_usdmix_action,rvp_t,upd_sub_data_input_t);

    
    ////////////
    // UpdLoc //
    ////////////

    DECLARE_DORA_TRX(upd_loc);

    DECLARE_DORA_FINAL_RVP_GEN_FUNC(final_ul_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(upd_sub_ul_action,rvp_t,upd_loc_input_t);



    ////////////////
    // InsCallFwd //
    ////////////////

    DECLARE_DORA_TRX(ins_call_fwd);

#ifdef TM1ICF2
    DECLARE_DORA_MIDWAY_RVP_GEN_FUNC(mid1_icf_rvp,ins_call_fwd_input_t);
    DECLARE_DORA_MIDWAY_RVP_WITH_PREV_GEN_FUNC(mid2_icf_rvp,ins_call_fwd_input_t);
    DECLARE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_icf_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(r_sub_icf_action,mid1_icf_rvp,ins_call_fwd_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(r_sf_icf_action,mid2_icf_rvp,ins_call_fwd_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(ins_cf_icf_action,rvp_t,ins_call_fwd_input_t);
#else
    DECLARE_DORA_MIDWAY_RVP_GEN_FUNC(mid_icf_rvp,ins_call_fwd_input_t);
    DECLARE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_icf_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(r_sub_icf_action,mid_icf_rvp,ins_call_fwd_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(r_sf_icf_action,rvp_t,ins_call_fwd_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(ins_cf_icf_action,rvp_t,ins_call_fwd_input_t);
#endif



    //////////////////////
    // InsCallFwd-Bench //
    //////////////////////

    DECLARE_DORA_TRX(ins_call_fwd_bench);

    DECLARE_DORA_FINAL_RVP_GEN_FUNC(final_icfb_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(r_sub_icfb_action,rvp_t,ins_call_fwd_bench_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(i_cf_icfb_action,rvp_t,ins_call_fwd_bench_input_t);




    ////////////////
    // DelCallFwd //
    ////////////////

    DECLARE_DORA_TRX(del_call_fwd);

    DECLARE_DORA_MIDWAY_RVP_GEN_FUNC(mid_dcf_rvp,del_call_fwd_input_t);
    DECLARE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_dcf_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(r_sub_dcf_action,mid_dcf_rvp,del_call_fwd_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(del_cf_dcf_action,rvp_t,del_call_fwd_input_t);



    ////////////////
    // GetSubNbr  //
    ////////////////

    DECLARE_DORA_TRX(get_sub_nbr);

    DECLARE_DORA_FINAL_DYNAMIC_RVP_GEN_FUNC(final_gsn_rvp);
#ifndef USE_DORA_EXT_IDX
    DECLARE_DORA_ACTION_GEN_FUNC(r_sub_gsn_action,rvp_t,get_sub_nbr_input_t);
#else
    DECLARE_DORA_ACTION_GEN_FUNC(r_sub_gsn_acc_action,rvp_t,get_sub_nbr_input_t);    
#endif

        
}; // EOF: DoraTM1Env


EXIT_NAMESPACE(dora);

#endif /** __DORA_TM1_H */

