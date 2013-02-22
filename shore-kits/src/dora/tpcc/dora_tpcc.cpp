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

/** @file:   dora_tpcc.cpp
 *
 *  @brief:  Implementation of the DORA TPC-C class
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */

#include "tls.h"

#include "dora/tpcc/dora_tpcc.h"

#include "dora/tpcc/dora_mbench.h"
#include "dora/tpcc/dora_payment.h"
#include "dora/tpcc/dora_order_status.h"
#include "dora/tpcc/dora_stock_level.h"
#include "dora/tpcc/dora_delivery.h"
#include "dora/tpcc/dora_new_order.h"

using namespace shore;
using namespace tpcc;


ENTER_NAMESPACE(dora);



// max field counts for (int) keys of tpc-c tables
const uint whs_IRP_KEY = 1;
const uint dis_IRP_KEY = 2;
const uint cus_IRP_KEY = 3;
const uint his_IRP_KEY = 6;
const uint nor_IRP_KEY = 3;
const uint ord_IRP_KEY = 4;
const uint ite_IRP_KEY = 1;
const uint oli_IRP_KEY = 4;
const uint sto_IRP_KEY = 2;

// key estimations for each partition of the tpc-c tables
const uint whs_KEY_EST = 100;
const uint dis_KEY_EST = 1000;
const uint cus_KEY_EST = 1000;
const uint his_KEY_EST = 1000;
const uint nor_KEY_EST = 1000;
const uint ord_KEY_EST = 1000;
const uint ite_KEY_EST = 1000;
const uint oli_KEY_EST = 1000;
const uint sto_KEY_EST = 1000;



/****************************************************************** 
 *
 * @fn:    construction/destruction
 *
 * @brief: If configured, it creates and starts the flusher 
 *
 ******************************************************************/
    
DoraTPCCEnv::DoraTPCCEnv()
    : ShoreTPCCEnv()
{ 
    update_pd(this);
}

DoraTPCCEnv::~DoraTPCCEnv() 
{ 
    stop();
}


/****************************************************************** 
 *
 * @fn:    start()
 *
 * @brief: Starts the DORA TPC-C
 *
 * @note:  Creates a corresponding number of partitions per table.
 *         The decision about the number of partitions per table may 
 *         be based among others on:
 *         - _env->_sf : the database scaling factor
 *         - _env->_{max,active}_cpu_count: {hard,soft} cpu counts
 *
 ******************************************************************/

int DoraTPCCEnv::start()
{
    // 1. Creates partitioned tables
    // 2. Adds them to the vector
    // 3. Resets each table

    conf(); // re-configure
    processorid_t icpu(_starting_cpu);

    // WAREHOUSE
    GENERATE_DORA_PARTS(whs,warehouse);

    // DISTRICT
    GENERATE_DORA_PARTS(dis,district);

    // HISTORY
    GENERATE_DORA_PARTS(his,history);

    // CUSTOMER
    GENERATE_DORA_PARTS(cus,customer);
   
    // NEW-ORDER
    GENERATE_DORA_PARTS(nor,new_order);

    // ORDER
    GENERATE_DORA_PARTS(ord,order);

    // ITEM
    GENERATE_DORA_PARTS(ite,item);

    // ORDER-LINE
    GENERATE_DORA_PARTS(oli,order_line);

    // STOCK
    GENERATE_DORA_PARTS(sto,stock);

    // Call the pre-start procedure of the dora environment
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

w_rc_t DoraTPCCEnv::update_partitioning() 
{
    // *** Reminder: the numbering in TPC-C starts from 1

    // First configure
    conf();

    // Pulling this partitioning out of the thin air
    int minKeyVal = 1;
    int maxKeyVal = get_sf()+1;

    char* minKey = (char*)malloc(sizeof(int));
    memset(minKey,0,sizeof(int));
    memcpy(minKey,&minKeyVal,sizeof(int));

    char* maxKey = (char*)malloc(sizeof(int));
    memset(maxKey,0,sizeof(int));
    memcpy(maxKey,&maxKeyVal,sizeof(int));


#warning !!! IP: To have real PLP, the partitioning info should be decoupled by the order in the prefix tree !!!
    // IP: 
    // Right now we are limited to the range of value on the possible values
    // the leading column of the index may take. In the case of TPC-C that is 
    // the number of Warehouses (the primary keys of almost all the TPC-C tables 
    // start with W_ID). That makes partitioning evenly difficult (e.g., how to
    // partition a 10WH TPC-C database on a machine with 16 cores?) and it is not
    // flexible. For example, we may have some semantic knowledge about the 
    // distribution of values of another field, not necesserily the one in the
    // leading column the index.
    // In the DORA paper we say that the routing fields of a table may be chosen
    // arbitrarily. However, in the current implementation we do not have that.
    // Ideally, the DORA/PLP layer should be providing a value (integer) and 
    // the KeyRangeMap would return a partition id. This value may *not* be the
    // key. 
    // The index scan performance will be affected. Because now if the partitioning
    // is not according to the leading column, then we have to start multiple
    // index scans.

    // [ 1 .. #WH+1 )
    // Warehouses,Districts,Customers,NewOrders,Orders,OrderLine,History,Stock
    _pwarehouse_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_whs);
    _pdistrict_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_dis);
    _pcustomer_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_cus);
    _pnew_order_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_nor);
    _porder_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_ord);
    _porder_line_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_oli);    
    _phistory_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_his);
    _pstock_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_sto);

    // Items: [ 1 .. 100K+1 )
    maxKeyVal = ITEMS + 1;
    memset(maxKey,0,sizeof(int));
    memcpy(maxKey,&maxKeyVal,sizeof(int));
    _pitem_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),_parts_ite);

    free (minKey);
    free (maxKey);

    return (RCOK);
}



/****************************************************************** 
 *
 * @fn:    stop()
 *
 * @brief: Stops the DORA TPC-C
 *
 ******************************************************************/

int DoraTPCCEnv::stop()
{
    return (DoraEnv::_post_stop(this));
}


/****************************************************************** 
 *
 * @fn:    resume()
 *
 * @brief: Resumes the DORA TPC-C
 *
 ******************************************************************/

int DoraTPCCEnv::resume()
{
    assert (0); // TODO (ip)
    set_dbc(DBC_ACTIVE);
    return (0);
}


/****************************************************************** 
 *
 * @fn:    pause()
 *
 * @brief: Pauses the DORA TPC-C
 *
 ******************************************************************/

int DoraTPCCEnv::pause()
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

int DoraTPCCEnv::conf()
{
    ShoreTPCCEnv::conf();
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

    // WARNING: pin: other than items table all the other tables are partitioned
    //               according to whs id now so their record estimation should be
    //               taken as whs table's record estimation otherwise the partitioning
    //               is screwed when whs table record estimation is less than the
    //               cpu_range (check tpcc-10 at dias2 for this bug). so now i omitted
    //               the record estimation calculation for other tables

    // In TPC-C, each table has different routing fields. Hence, there is 
    // different recordEstimation per table.
    uint recordEstimation = get_sf(); // == # of Warehouses
    double whs_PerCPU = ev->getVarDouble("dora-ratio-tpcc-whs",0);
    _parts_whs = ( whs_PerCPU>0 ? (_cpu_range * whs_PerCPU) : 1);
    _parts_whs = std::min(recordEstimation,_parts_whs);

    // Districts 
    //recordEstimation = get_sf()*DISTRICTS_PER_WAREHOUSE;
    double dis_PerCPU = ev->getVarDouble("dora-ratio-tpcc-dis",0);
    _parts_dis = ( dis_PerCPU>0 ? (_cpu_range * dis_PerCPU) : 1);
    _parts_dis = std::min(recordEstimation,_parts_dis);

    // Customers
    //recordEstimation = get_sf()*DISTRICTS_PER_WAREHOUSE*CUSTOMERS_PER_DISTRICT;
    double cus_PerCPU = ev->getVarDouble("dora-ratio-tpcc-cus",0);
    _parts_cus = ( cus_PerCPU>0 ? (_cpu_range * cus_PerCPU) : 1);
    _parts_cus = std::min(recordEstimation,_parts_cus);

    // NewOrders - Growing table. We use the number of Districts instead, because 
    //             its primary key prefix is W_ID,D_ID,...
    //recordEstimation = get_sf()*DISTRICTS_PER_WAREHOUSE*NU_ORDERS_PER_DISTRICT;
    double nor_PerCPU = ev->getVarDouble("dora-ratio-tpcc-nor",0);
    _parts_nor = ( nor_PerCPU>0 ? (_cpu_range * nor_PerCPU) : 1);
    _parts_nor = std::min(recordEstimation,_parts_nor);

    // Orders - Growing table, treating is similarly with NewOrders
    // recordEstimation = get_sf()*DISTRICTS_PER_WAREHOUSE;
    double ord_PerCPU = ev->getVarDouble("dora-ratio-tpcc-ord",0);
    _parts_ord = ( ord_PerCPU>0 ? (_cpu_range * ord_PerCPU) : 1);
    _parts_ord = std::min(recordEstimation,_parts_ord);

    // OrderLines - Growing table, treating is similarly with NewOrders
    //recordEstimation = get_sf()*DISTRICTS_PER_WAREHOUSE;
    double oli_PerCPU = ev->getVarDouble("dora-ratio-tpcc-oli",0);
    _parts_oli = ( oli_PerCPU>0 ? (_cpu_range * oli_PerCPU) : 1);
    _parts_oli = std::min(recordEstimation,_parts_oli);

    // History - No primary key, we use the Districts because it is a 
    //           quite flexible number (10 x #Warehouses)
    //recordEstimation = get_sf()*DISTRICTS_PER_WAREHOUSE;
    double his_PerCPU = ev->getVarDouble("dora-ratio-tpcc-his",0);
    _parts_his = ( his_PerCPU>0 ? (_cpu_range * his_PerCPU) : 1);
    _parts_his = std::min(recordEstimation,_parts_his);

    // Stocks
    //recordEstimation = get_sf()*STOCK_PER_WAREHOUSE;
    double sto_PerCPU = ev->getVarDouble("dora-ratio-tpcc-sto",0);
    _parts_sto = ( whs_PerCPU>0 ? (_cpu_range * sto_PerCPU) : 1);
    _parts_sto = std::min(recordEstimation,_parts_sto);

    // Item - Constant == 100K
    recordEstimation = ITEMS;
    double ite_PerCPU = ev->getVarDouble("dora-ratio-tpcc-ite",0);
    _parts_ite = ( ite_PerCPU>0 ? (_cpu_range * ite_PerCPU) : 1);
    _parts_ite = std::min(recordEstimation,_parts_ite);


    TRACE( TRACE_STATISTICS,"Total number of partitions (%d)\n",
           (_parts_whs+_parts_dis+_parts_cus+_parts_nor+
            _parts_ord+_parts_oli+_parts_his+_parts_ite+
            _parts_sto));
    
    return (0);
}


/****************************************************************** 
 *
 * @fn:    newrun()
 *
 * @brief: Prepares the DORA TPC-C DB for a new run
 *
 ******************************************************************/

w_rc_t DoraTPCCEnv::newrun()
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

int DoraTPCCEnv::dump()
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

int DoraTPCCEnv::info() const
{
    return (DoraEnv::_info(this));
}


/******************************************************************** 
 *
 *  @fn:    statistics
 *
 *  @brief: Prints statistics for DORA 
 *
 ********************************************************************/

int DoraTPCCEnv::statistics() 
{
    DoraEnv::_statistics(this);

    // TPCC STATS
    TRACE( TRACE_STATISTICS, "----- TPCC  -----\n");
    ShoreTPCCEnv::statistics();
    return (0);
}



/******************************************************************** 
 *
 *  Thread-local action and rvp object caches
 *
 ********************************************************************/


// MBenches RVP //

DEFINE_DORA_FINAL_RVP_GEN_FUNC(final_mb_rvp,DoraTPCCEnv);

DEFINE_DORA_ACTION_GEN_FUNC(upd_wh_mb_action,rvp_t,mbench_wh_input_t,int,DoraTPCCEnv);
DEFINE_DORA_ACTION_GEN_FUNC(upd_cust_mb_action,rvp_t,mbench_cust_input_t,int,DoraTPCCEnv);


///////////////////////////////////////////////////////////////////////////////////////

// TPC-C PAYMENT


DEFINE_DORA_MIDWAY_RVP_GEN_FUNC(midway_pay_rvp,payment_input_t,DoraTPCCEnv);
DEFINE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_pay_rvp,DoraTPCCEnv);


DEFINE_DORA_ACTION_GEN_FUNC(upd_wh_pay_action,midway_pay_rvp,payment_input_t,int,DoraTPCCEnv);
DEFINE_DORA_ACTION_GEN_FUNC(upd_dist_pay_action,midway_pay_rvp,payment_input_t,int,DoraTPCCEnv);
DEFINE_DORA_ACTION_GEN_FUNC(upd_cust_pay_action,midway_pay_rvp,payment_input_t,int,DoraTPCCEnv);

DEFINE_DORA_ACTION_GEN_FUNC(ins_hist_pay_action,rvp_t,payment_input_t,int,DoraTPCCEnv);
//const tpcc_warehouse_tuple& awh
//const tpcc_district_tuple& adist


///////////////////////////////////////////////////////////////////////////////////////



///////////////////////////////////////////////////////////////////////////////////////

// TPC-C ORDER STATUS

DEFINE_DORA_MIDWAY_RVP_GEN_FUNC(mid1_ordst_rvp,order_status_input_t,DoraTPCCEnv);

DEFINE_DORA_MIDWAY_RVP_WITH_PREV_GEN_FUNC(mid2_ordst_rvp,order_status_input_t,DoraTPCCEnv);

DEFINE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_ordst_rvp,DoraTPCCEnv);

DEFINE_DORA_ACTION_GEN_FUNC(r_cust_ordst_action,mid1_ordst_rvp,order_status_input_t,int,DoraTPCCEnv);
DEFINE_DORA_ACTION_GEN_FUNC(r_ord_ordst_action,mid2_ordst_rvp,order_status_input_t,int,DoraTPCCEnv);

DEFINE_DORA_ACTION_GEN_FUNC(r_ol_ordst_action,rvp_t,order_status_input_t,int,DoraTPCCEnv);



///////////////////////////////////////////////////////////////////////////////////////



///////////////////////////////////////////////////////////////////////////////////////

// TPC-C STOCK LEVEL

DEFINE_DORA_MIDWAY_RVP_GEN_FUNC(mid1_stock_rvp,stock_level_input_t,DoraTPCCEnv);
DEFINE_DORA_MIDWAY_RVP_WITH_PREV_GEN_FUNC(mid2_stock_rvp,stock_level_input_t,DoraTPCCEnv);

DEFINE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_stock_rvp,DoraTPCCEnv);


DEFINE_DORA_ACTION_GEN_FUNC(r_dist_stock_action,mid1_stock_rvp,stock_level_input_t,int,DoraTPCCEnv);
DEFINE_DORA_ACTION_GEN_FUNC(r_ol_stock_action,mid2_stock_rvp,stock_level_input_t,int,DoraTPCCEnv);

DEFINE_DORA_ACTION_GEN_FUNC(r_st_stock_action,rvp_t,stock_level_input_t,int,DoraTPCCEnv);



///////////////////////////////////////////////////////////////////////////////////////



///////////////////////////////////////////////////////////////////////////////////////

// TPC-C DELIVERY

DEFINE_DORA_MIDWAY_RVP_GEN_FUNC(mid1_del_rvp,delivery_input_t,DoraTPCCEnv);
//final_del_rvp* frvp
//const int d_id

DEFINE_DORA_MIDWAY_RVP_WITH_PREV_GEN_FUNC(mid2_del_rvp,delivery_input_t,DoraTPCCEnv);
//final_del_rvp* frvp
//const int d_id

DEFINE_DORA_FINAL_RVP_GEN_FUNC(final_del_rvp,DoraTPCCEnv);


DEFINE_DORA_ACTION_GEN_FUNC(del_nord_del_action,mid1_del_rvp,delivery_input_t,int,DoraTPCCEnv);
//const int d_id

DEFINE_DORA_ACTION_GEN_FUNC(upd_ord_del_action,mid2_del_rvp,delivery_input_t,int,DoraTPCCEnv);
//const int d_id
//const int o_id

DEFINE_DORA_ACTION_GEN_FUNC(upd_oline_del_action,mid2_del_rvp,delivery_input_t,int,DoraTPCCEnv);
//const int d_id
//const int o_id

DEFINE_DORA_ACTION_GEN_FUNC(upd_cust_del_action,rvp_t,delivery_input_t,int,DoraTPCCEnv);
//const int d_id
//const int o_id
//const int amount


///////////////////////////////////////////////////////////////////////////////////////




///////////////////////////////////////////////////////////////////////////////////////

// TPC-C NEWORDER

// RVP
DEFINE_DORA_MIDWAY_RVP_GEN_FUNC(mid1_nord_rvp,new_order_input_t,DoraTPCCEnv);

DEFINE_DORA_MIDWAY_RVP_WITH_PREV_GEN_FUNC(mid2_nord_rvp,new_order_input_t,DoraTPCCEnv);

DEFINE_DORA_MIDWAY_RVP_WITH_PREV_GEN_FUNC(mid3_nord_rvp,new_order_input_t,DoraTPCCEnv);

DEFINE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(final_nord_rvp,DoraTPCCEnv);


// Start -> Midway 1
DEFINE_DORA_ACTION_GEN_FUNC(r_wh_nord_action,mid1_nord_rvp,no_item_nord_input_t,int,DoraTPCCEnv);

DEFINE_DORA_ACTION_GEN_FUNC(upd_dist_nord_action,mid1_nord_rvp,no_item_nord_input_t,int,DoraTPCCEnv);

DEFINE_DORA_ACTION_GEN_FUNC(r_cust_nord_action,mid1_nord_rvp,no_item_nord_input_t,int,DoraTPCCEnv);

DEFINE_DORA_ACTION_GEN_FUNC(r_item_nord_action,mid1_nord_rvp,new_order_input_t,int,DoraTPCCEnv);


// Midway 1 -> Midway 2
DEFINE_DORA_ACTION_GEN_FUNC(ins_ord_nord_action,mid2_nord_rvp,no_item_nord_input_t,int,DoraTPCCEnv);

DEFINE_DORA_ACTION_GEN_FUNC(ins_nord_nord_action,mid2_nord_rvp,no_item_nord_input_t,int,DoraTPCCEnv);


// Midway 2 -> Midway 3
DEFINE_DORA_ACTION_GEN_FUNC(ins_ol_nord_action,mid3_nord_rvp,new_order_input_t,int,DoraTPCCEnv);


// Midway 3 -> Final
DEFINE_DORA_ACTION_GEN_FUNC(upd_sto_nord_action,rvp_t,new_order_input_t,int,DoraTPCCEnv);


///////////////////////////////////////////////////////////////////////////////////////




EXIT_NAMESPACE(dora);

