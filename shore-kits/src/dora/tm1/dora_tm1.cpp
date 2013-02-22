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

/** @file:   dora_tm1.cpp
 *
 *  @brief:  Implementation of the DORA TM1 class
 *
 *  @author: Ippokratis Pandis, Feb 2009
 */

#include "tls.h"

#include "dora/tm1/dora_tm1.h"
#include "dora/tm1/dora_tm1_impl.h"

using namespace shore;
using namespace tm1;


ENTER_NAMESPACE(dora);



// max field counts for (int) keys of tm1 tables
const uint sub_IRP_KEY = 1;
const uint ai_IRP_KEY  = 2;
const uint sf_IRP_KEY  = 2;
const uint cf_IRP_KEY  = 3;

// key estimations for each partition of the tm1 tables
const uint sub_KEY_EST = 1000;
const uint ai_KEY_EST  = 2500;
const uint sf_KEY_EST  = 2500;
const uint cf_KEY_EST  = 3750;



/****************************************************************** 
 *
 *  The DORA TM1 environment
 *
 ******************************************************************/

DoraTM1Env::DoraTM1Env()
    : ShoreTM1Env(), DoraEnv()
{ 
    update_pd(this);
}

DoraTM1Env::~DoraTM1Env()
{ 
    stop();
}


/****************************************************************** 
 *
 * @fn:    start()
 *
 * @brief: Starts the DORA TM1
 *
 * @note:  Creates a corresponding number of partitions per table.
 *         The decision about the number of partitions per table may 
 *         be based among others on:
 *         - _env->_sf : the database scaling factor
 *         - _env->_{max,active}_cpu_count: {hard,soft} cpu counts
 *
 ******************************************************************/


int DoraTM1Env::start()
{
    // 1. Read configuration
    // 2. Create partitioned tables
    // 3. Add them to the vector
    // 4. Reset each table
    // 5. Start logger

    conf(); // re-configure
    processorid_t icpu(_starting_cpu);

    // SUBSCRIBER
    GENERATE_DORA_PARTS(sub,sub);

    // ACCESS INFO
    GENERATE_DORA_PARTS(ai,ai);

    // SPECIAL FACILITY
    GENERATE_DORA_PARTS(sf,sf);

    // CALL FORWARDING
    GENERATE_DORA_PARTS(cf,cf);

    // Call the post-start procedure of the dora environment
    DoraEnv::_post_start(this);
    return (0);
}



/******************************************************************** 
 *
 *  @fn:    update_partitioning()
 *
 *  @brief: Applies the baseline partitioning to the TPC-B tables
 *
 ********************************************************************/

w_rc_t DoraTM1Env::update_partitioning() 
{
    // *** Reminder: The numbering in TM1 starts from 1. 

    // First configure
    conf();

    // The number of partitions in the indexes should match the DORA 
    // partitioning
    int minKeyVal = 1;
    int maxKeyVal = (get_sf()*TM1_SUBS_PER_SF)+1;

    // vec_t minKey((char*)(&minKeyVal),sizeof(int));
    // vec_t maxKey((char*)(&maxKeyVal),sizeof(int));

    char* minKey = (char*)malloc(sizeof(int));
    memset(minKey,0,sizeof(int));
    memcpy(minKey,&minKeyVal,sizeof(int));

    char* maxKey = (char*)malloc(sizeof(int));
    memset(maxKey,0,sizeof(int));
    memcpy(maxKey,&maxKeyVal,sizeof(int));

    // All the TM1 tables use the SUB_ID as the first column
    _psub_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_sub);
    _pai_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_ai);
    _psf_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_sf);
    _pcf_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_cf);

    free (minKey);
    free (maxKey);

    return (RCOK);
}



/****************************************************************** 
 *
 * @fn:    stop()
 *
 * @brief: Stops the DORA TM1
 *
 ******************************************************************/

int DoraTM1Env::stop()
{
    // Call the post-stop procedure of the dora environment
    return (DoraEnv::_post_stop(this));
}


/****************************************************************** 
 *
 * @fn:    resume()
 *
 * @brief: Resumes the DORA TM1
 *
 ******************************************************************/

int DoraTM1Env::resume()
{
    assert (0); // TODO (ip)
    set_dbc(DBC_ACTIVE);
    return (0);
}


/****************************************************************** 
 *
 * @fn:    pause()
 *
 * @brief: Pauses the DORA TM1
 *
 ******************************************************************/

int DoraTM1Env::pause()
{
    assert (0); // TODO (ip)
    set_dbc(DBC_PAUSED);
    return (0);
}


/****************************************************************** 
 *
 * @fn:    conf()
 *
 * @brief: Re-reads configuration
 *
 ******************************************************************/

int DoraTM1Env::conf()
{
    ShoreTM1Env::conf();

    _check_type();

    envVar* ev = envVar::instance();

    // Get CPU and binding configuration
    _cpu_range = get_active_cpu_count();
    _starting_cpu = ev->getVarInt("dora-cpu-starting",DF_CPU_STEP_PARTITIONS);
    _cpu_table_step = ev->getVarInt("dora-cpu-table-step",DF_CPU_STEP_TABLES);

    // For each table calculate the number of partition to create. 
    // This decision depends on: 
    // (a) The number of CPUs available
    // (b) The ratio of partitions per CPU in the configuration (shore.conf)
    // (c) The number of distinct values/records for the routing field, which 
    //     depending on the table, the workload, and the scaling factor

    // Estimation of number of records. Actually, this is the number of
    // distinct values the routing field can get. In TM1's case we are 
    // using the SubscriberID.
    uint recordEstimation = get_sf()*TM1_SUBS_PER_SF;
    // User requested number of partitions
    double sub_PerCPU = ev->getVarDouble("dora-ratio-tm1-sub",0);
    _parts_sub = ( sub_PerCPU>0 ? ceil(_cpu_range * sub_PerCPU) : 1);
    _parts_sub = std::min(recordEstimation,_parts_sub);

    // AccessInfo
    double ai_PerCPU = ev->getVarDouble("dora-ratio-tm1-ai",0);
    _parts_ai = ( ai_PerCPU>0 ? (_cpu_range * ai_PerCPU) : 1);
    _parts_ai = std::min(recordEstimation,_parts_ai);

    // SpecialFacility
    double sf_PerCPU = ev->getVarDouble("dora-ratio-tm1-sf",0);
    _parts_sf = ( sf_PerCPU>0 ? (_cpu_range * sf_PerCPU) : 1);
    _parts_sf = std::min(recordEstimation,_parts_sf);

    // CallForwarding
    double cf_PerCPU = ev->getVarDouble("dora-ratio-tm1-cf",0);
    _parts_cf = ( cf_PerCPU>0 ? (_cpu_range * cf_PerCPU) : 1);
    _parts_cf = std::min(recordEstimation,_parts_cf);

    TRACE( TRACE_STATISTICS,"Total number of partitions (%d)\n",
           (_parts_sub+_parts_ai+_parts_sf+_parts_cf));

    return (0);
}


/****************************************************************** 
 *
 * @fn:    newrun()
 *
 * @brief: Prepares the DORA TM1 DB for a new run
 *
 ******************************************************************/

w_rc_t DoraTM1Env::newrun()
{
    return (DoraEnv::_newrun(this));
}


/****************************************************************** 
 *
 * @fn:    dump()
 *
 * @brief: Dumps information about all the tables and partitions
 *
 ******************************************************************/

int DoraTM1Env::dump()
{
    return (DoraEnv::_dump(this));
}


/****************************************************************** 
 *
 * @fn:    info()
 *
 * @brief: Information about the current state of DORA
 *
 ******************************************************************/

int DoraTM1Env::info() const
{
    return (DoraEnv::_info(this));
}


/******************************************************************** 
 *
 *  @fn:    statistics
 *
 *  @brief: Prints statistics for DORA-TM1
 *
 ********************************************************************/

int DoraTM1Env::statistics() 
{
    DoraEnv::_statistics(this);
    return (0);

    // TM1 STATS
    // disabled
    TRACE( TRACE_STATISTICS, "----- TM1  -----\n");
    ShoreTM1Env::statistics();
    return (0);
}



/******************************************************************** 
 *
 *  Thread-local action and rvp object caches
 *
 ********************************************************************/



////////////////
// GetSubData //
////////////////

DEFINE_DORA_FINAL_RVP_GEN_FUNC(final_gsd_rvp,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(r_sub_gsd_action,rvp_t,get_sub_data_input_t,int,DoraTM1Env);



////////////////
// GetNewDest //
////////////////
#ifdef TM1GND2
DEFINE_DORA_MIDWAY_RVP_GEN_FUNC(mid_gnd_rvp,get_new_dest_input_t,DoraTM1Env);
DEFINE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_gnd_rvp,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(r_sf_gnd_action,mid_gnd_rvp,get_new_dest_input_t,int,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(r_cf_gnd_action,rvp_t,get_new_dest_input_t,int,DoraTM1Env);
#else

#warning OLD-GetNewDest !!
DEFINE_DORA_FINAL_RVP_GEN_FUNC(final_gnd_rvp,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(r_sf_gnd_action,rvp_t,get_new_dest_input_t,int,DoraTM1Env);
DEFINE_DORA_ACTION_GEN_FUNC(r_cf_gnd_action,rvp_t,get_new_dest_input_t,int,DoraTM1Env);
#endif


////////////////
// GetAccData //
////////////////

DEFINE_DORA_FINAL_RVP_GEN_FUNC(final_gad_rvp,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(r_ai_gad_action,rvp_t,get_acc_data_input_t,int,DoraTM1Env);



////////////////
// UpdSubData //
////////////////
#ifdef TM1USD2
DEFINE_DORA_MIDWAY_RVP_GEN_FUNC(mid_usd_rvp,upd_sub_data_input_t,DoraTM1Env);
DEFINE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_usd_rvp,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(upd_sf_usd_action,mid_usd_rvp,upd_sub_data_input_t,int,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(upd_sub_usd_action,rvp_t,upd_sub_data_input_t,int,DoraTM1Env);
#else

#warning OLD-UpdSubData !!
DEFINE_DORA_FINAL_RVP_GEN_FUNC(final_usd_rvp,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(upd_sub_usd_action,rvp_t,upd_sub_data_input_t,int,DoraTM1Env);
DEFINE_DORA_ACTION_GEN_FUNC(upd_sf_usd_action,rvp_t,upd_sub_data_input_t,int,DoraTM1Env);
#endif


////////////////////
// UpdSubData Mix //
////////////////////
DEFINE_DORA_MIDWAY_RVP_GEN_FUNC(mid_usdmix_rvp,upd_sub_data_input_t,DoraTM1Env);
DEFINE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_usdmix_rvp,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(upd_sub_usdmix_action,mid_usdmix_rvp,upd_sub_data_input_t,int,DoraTM1Env);
DEFINE_DORA_ACTION_GEN_FUNC(upd_sf_usdmix_action,rvp_t,upd_sub_data_input_t,int,DoraTM1Env);


////////////
// UpdLoc //
////////////

DEFINE_DORA_FINAL_RVP_GEN_FUNC(final_ul_rvp,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(upd_sub_ul_action,rvp_t,upd_loc_input_t,int,DoraTM1Env);



////////////////
// InsCallFwd //
////////////////
#ifdef TM1ICF2
DEFINE_DORA_MIDWAY_RVP_GEN_FUNC(mid1_icf_rvp,ins_call_fwd_input_t,DoraTM1Env);
DEFINE_DORA_MIDWAY_RVP_WITH_PREV_GEN_FUNC(mid2_icf_rvp,ins_call_fwd_input_t,DoraTM1Env);
DEFINE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_icf_rvp,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(r_sub_icf_action,mid1_icf_rvp,ins_call_fwd_input_t,int,DoraTM1Env);
DEFINE_DORA_ACTION_GEN_FUNC(r_sf_icf_action,mid2_icf_rvp,ins_call_fwd_input_t,int,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(ins_cf_icf_action,rvp_t,ins_call_fwd_input_t,int,DoraTM1Env);

#else

#warning OLD-InsCallFwd !!
DEFINE_DORA_MIDWAY_RVP_GEN_FUNC(mid_icf_rvp,ins_call_fwd_input_t,DoraTM1Env);
DEFINE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_icf_rvp,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(r_sub_icf_action,mid_icf_rvp,ins_call_fwd_input_t,int,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(r_sf_icf_action,rvp_t,ins_call_fwd_input_t,int,DoraTM1Env);
DEFINE_DORA_ACTION_GEN_FUNC(ins_cf_icf_action,rvp_t,ins_call_fwd_input_t,int,DoraTM1Env);
#endif



////////////////
// InsCallFwd-Bench //
////////////////

DEFINE_DORA_FINAL_RVP_GEN_FUNC(final_icfb_rvp,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(r_sub_icfb_action,rvp_t,ins_call_fwd_bench_input_t,int,DoraTM1Env);
DEFINE_DORA_ACTION_GEN_FUNC(i_cf_icfb_action,rvp_t,ins_call_fwd_bench_input_t,int,DoraTM1Env);



////////////////
// DelCallFwd //
////////////////

DEFINE_DORA_MIDWAY_RVP_GEN_FUNC(mid_dcf_rvp,del_call_fwd_input_t,DoraTM1Env);
DEFINE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_dcf_rvp,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(r_sub_dcf_action,mid_dcf_rvp,del_call_fwd_input_t,int,DoraTM1Env);

DEFINE_DORA_ACTION_GEN_FUNC(del_cf_dcf_action,rvp_t,del_call_fwd_input_t,int,DoraTM1Env);



///////////////
// GetSubNbr //
///////////////

DEFINE_DORA_FINAL_DYNAMIC_RVP_GEN_FUNC(final_gsn_rvp,DoraTM1Env);
#ifndef USE_DORA_EXT_IDX
DEFINE_DORA_ACTION_GEN_FUNC(r_sub_gsn_action,rvp_t,get_sub_nbr_input_t,int,DoraTM1Env);
#else
DEFINE_DORA_ACTION_GEN_FUNC(r_sub_gsn_acc_action,rvp_t,get_sub_nbr_input_t,int,DoraTM1Env);
#endif



EXIT_NAMESPACE(dora);
