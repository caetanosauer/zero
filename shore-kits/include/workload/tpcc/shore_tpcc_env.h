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

/** @file:   shore_tpcc_env.h
 *
 *  @brief:  Definition of the Shore TPC-C environment
 *
 *  @author: Ippokratis Pandis (ipandis)
 */

#ifndef __SHORE_TPCC_ENV_H
#define __SHORE_TPCC_ENV_H


#include "sm_vas.h"
#include "util.h"

#include "sm/shore/shore_env.h"
#include "sm/shore/shore_asc_sort_buf.h"
#include "sm/shore/shore_trx_worker.h"

#include "workload/tpcc/tpcc_const.h"

#include "workload/tpcc/shore_tpcc_schema_man.h"
#include "workload/tpcc/tpcc_input.h"

#include <map>

using std::map;
using namespace shore;


ENTER_NAMESPACE(tpcc);


#define TPCC_SCALING_FACTOR             100
#define QUERIED_TPCC_SCALING_FACTOR     100



/****************************************************************** 
 *
 *  @struct: ShoreTPCCEnv Stats
 *
 *  @brief:  TPCC Environment statistics
 *
 ******************************************************************/

struct ShoreTPCCTrxCount 
{
    uint new_order;
    uint payment;
    uint order_status;
    uint delivery;
    uint stock_level;

    uint mbench_wh;
    uint mbench_cust;

    ShoreTPCCTrxCount& operator+=(ShoreTPCCTrxCount const& rhs) {
        new_order += rhs.new_order; 
        payment += rhs.payment; 
        order_status += rhs.order_status;
        delivery += rhs.delivery;
        stock_level += rhs.stock_level; 
        mbench_wh += rhs.mbench_wh;
        mbench_cust += rhs.mbench_cust;
	return (*this);
    }

    ShoreTPCCTrxCount& operator-=(ShoreTPCCTrxCount const& rhs) {
        new_order -= rhs.new_order; 
        payment -= rhs.payment; 
        order_status -= rhs.order_status;
        delivery -= rhs.delivery;
        stock_level -= rhs.stock_level; 
        mbench_wh -= rhs.mbench_wh;
        mbench_cust -= rhs.mbench_cust;
	return (*this);
    }

    uint total() const {
        return (new_order+payment+order_status+delivery+stock_level+
                mbench_wh+mbench_cust);
    }
    
}; // EOF: ShoreTPCCTrxCount



struct ShoreTPCCTrxStats
{
    ShoreTPCCTrxCount attempted;
    ShoreTPCCTrxCount failed;
    ShoreTPCCTrxCount deadlocked;

    ShoreTPCCTrxStats& operator+=(ShoreTPCCTrxStats const& other) {
        attempted  += other.attempted;
        failed     += other.failed;
        deadlocked += other.deadlocked;
        return (*this);
    }

    ShoreTPCCTrxStats& operator-=(ShoreTPCCTrxStats const& other) {
        attempted  -= other.attempted;
        failed     -= other.failed;
        deadlocked -= other.deadlocked;
        return (*this);
    }

}; // EOF: ShoreTPCCTrxStats



/******************************************************************** 
 * 
 *  ShoreTPCCEnv
 *  
 *  Shore TPC-C Database.
 *
 ********************************************************************/

// For P-Loader 
static int const NORD_PER_UNIT = 9;
static int const CUST_PER_UNIT = 30;
static int const HIST_PER_UNIT = 30;
static int const ORDERS_PER_UNIT = 30;
static int const STOCK_PER_UNIT = 100;
static int const UNIT_PER_WH = 1000;
static int const UNIT_PER_DIST = 100;
static int const ORDERS_PER_DIST = 3000;


class ShoreTPCCEnv : public ShoreEnv
{
public:

    typedef std::map<pthread_t, ShoreTPCCTrxStats*> statmap_t;

    class table_builder_t;
    class table_creator_t;
    class checkpointer_t;

private:
    w_rc_t _post_init_impl();
    
public:    

    ShoreTPCCEnv();
    virtual ~ShoreTPCCEnv();


    // DB INTERFACE

    virtual int set(envVarMap* /* vars */) { return(0); /* do nothing */ };
    virtual int open() { return(0); /* do nothing */ };
    virtual int pause() { return(0); /* do nothing */ };
    virtual int resume() { return(0); /* do nothing */ };    
    virtual w_rc_t newrun() { return(RCOK); /* do nothing */ };

    virtual int post_init();
    virtual w_rc_t load_schema();

    virtual int conf();
    virtual int start();
    virtual int stop();
    virtual int info() const;
    virtual int statistics();    
    
    int dump();

    virtual void print_throughput(const double iQueriedSF, 
                                  const int iSpread, 
                                  const int iNumOfThreads,
                                  const double delay,
                                  const ulong_t mioch,
                                  const double avgcpuusage);



    // Public methods //    

    // --- operations over tables --- //
    w_rc_t loaddata();  
    w_rc_t warmup();
    w_rc_t check_consistency();

    
    // TPCC Tables
    DECLARE_TABLE(warehouse_t,warehouse_man_impl,warehouse);
    DECLARE_TABLE(district_t,district_man_impl,district);
    DECLARE_TABLE(customer_t,customer_man_impl,customer);
    DECLARE_TABLE(history_t,history_man_impl,history);
    DECLARE_TABLE(new_order_t,new_order_man_impl,new_order);
    DECLARE_TABLE(order_t,order_man_impl,order);
    DECLARE_TABLE(order_line_t,order_line_man_impl,order_line);
    DECLARE_TABLE(item_t,item_man_impl,item);
    DECLARE_TABLE(stock_t,stock_man_impl,stock);
    

    // --- kit trxs --- //

    w_rc_t run_one_xct(Request* prequest);

    DECLARE_TRX(new_order);
    DECLARE_TRX(payment);
    DECLARE_TRX(order_status);
    DECLARE_TRX(delivery);
    DECLARE_TRX(stock_level);

    DECLARE_TRX(mbench_wh);
    DECLARE_TRX(mbench_cust);

    // P-Loader
    DECLARE_TRX(populate_baseline);
    DECLARE_TRX(populate_one_unit);    

    // Helper xcts
    w_rc_t _xct_delivery_helper(const int xct_id, delivery_input_t& pdin,
				std::vector<int>& dlist, int& d_id,
				const bool SPLIT_TRX);
    
    // Update the partitioning info, if any needed
    virtual w_rc_t update_partitioning();

    // for thread-local stats
    virtual void env_thread_init();
    virtual void env_thread_fini();   

    // stat map
    statmap_t _statmap;

    // snapshot taken at the beginning of each experiment    
    ShoreTPCCTrxStats _last_stats;
    virtual void reset_stats();
    ShoreTPCCTrxStats _get_stats();
    
    // set load imbalance and time to apply it
    void set_skew(int area, int load, int start_imbalance);
    void start_load_imbalance();
    void reset_skew();
    
    //print the current tables into files
    w_rc_t db_print(int lines);

    //fetch the pages of the current tables and their indexes into the buffer pool
    w_rc_t db_fetch();
    
}; // EOF ShoreTPCCEnv
   


EXIT_NAMESPACE(tpcc);


#endif /* __SHORE_TPCC_ENV_H */
