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

/** @file:   shore_ssb_env.h
 *
 *  @brief:  Definition of the Shore SSB environment
 *
 *  @author: Manos Athanassoulis 
 */

#ifndef __SHORE_SSB_ENV_H
#define __SHORE_SSB_ENV_H

#include "sm_vas.h"
#include "util.h"

#include "sm/shore/shore_env.h"
#include "sm/shore/shore_asc_sort_buf.h"
#include "sm/shore/shore_trx_worker.h"

#include "workload/ssb/ssb_const.h"

#include "workload/ssb/shore_ssb_schema_man.h"
#include "workload/ssb/ssb_input.h"

#ifdef CFG_QPIPE
#include "qpipe.h"
#endif

#include <map>

using namespace shore;


ENTER_NAMESPACE(ssb);


using std::map;



// Sets the scaling factor of the SSB database
// @note Some data structures base their size on this value

const double SSB_SCALING_FACTOR = 1;


// For the population
//const long CUST_POP_UNIT = 10;
//const long PART_POP_UNIT = 10;
const long LO_POP_UNIT = 10;

/****************************************************************** 
 *
 *  @struct: ShoreSSBEnv Stats
 *
 *  @brief:  SSB Environment statistics
 *
 ******************************************************************/

struct ShoreSSBTrxCount 
{
    uint qdate;
    uint qpart;
    uint qsupplier;
    uint qcustomer;
    uint qlineorder;
    uint qtest;
    uint q1_1;
    uint q1_2;
    uint q1_3;
    uint q2_1;
    uint q2_2;
    uint q2_3;
    uint q3_1;
    uint q3_2;
    uint q3_3;
    uint q3_4;
    uint q4_1;
    uint q4_2;
    uint q4_3;

    uint qNP;


    ShoreSSBTrxCount& operator+=(ShoreSSBTrxCount const& rhs) {
        qdate += rhs.qdate;
        qpart += rhs.qpart;
        qsupplier += rhs.qsupplier;
        qcustomer += rhs.qcustomer;
        qlineorder += rhs.qlineorder;
        qtest += rhs.qtest;
        q1_1 += rhs.q1_1;
        q1_2 += rhs.q1_2;
        q1_3 += rhs.q1_3;
        q2_1 += rhs.q2_1;
        q2_2 += rhs.q2_2;
        q2_3 += rhs.q2_3;
        q3_1 += rhs.q3_1;
        q3_2 += rhs.q3_2;
        q3_3 += rhs.q3_3;
        q3_4 += rhs.q3_4;
        q4_1 += rhs.q4_1;
        q4_2 += rhs.q4_2;
        q4_3 += rhs.q4_3;
        
        qNP += rhs.qNP;

        return (*this);
    }

    ShoreSSBTrxCount& operator-=(ShoreSSBTrxCount const& rhs) {
        qdate -= rhs.qdate;
        qpart -= rhs.qpart;
        qsupplier -= rhs.qsupplier;
        qcustomer -= rhs.qcustomer;
        qlineorder -= rhs.qlineorder;
        qtest -= rhs.qtest;
        q1_1 -= rhs.q1_1;
        q1_2 -= rhs.q1_2;
        q1_3 -= rhs.q1_3;
        q2_1 -= rhs.q2_1;
        q2_2 -= rhs.q2_2;
        q2_3 -= rhs.q2_3;
        q3_1 -= rhs.q3_1;
        q3_2 -= rhs.q3_2;
        q3_3 -= rhs.q3_3;
        q3_4 -= rhs.q3_4;
        q4_1 -= rhs.q4_1;
        q4_2 -= rhs.q4_2;
        q4_3 -= rhs.q4_3;
        
        qNP -= rhs.qNP;

        return (*this);
    }



    uint total() const {
        return 
            (qlineorder+qcustomer+qsupplier+qpart+qdate+qtest+
             q1_1+q1_2+q1_3+q2_1+q2_2+q2_3+q3_1+q3_2+q3_3+q3_4+q4_1+q4_2+q4_3+qNP);
    }

}; // EOF: ShoreSSBTrxCount


struct ShoreSSBTrxStats
{
    ShoreSSBTrxCount attempted;
    ShoreSSBTrxCount failed;
    ShoreSSBTrxCount deadlocked;

    ShoreSSBTrxStats& operator+=(ShoreSSBTrxStats const& other) {
        attempted  += other.attempted;
        failed     += other.failed;
        deadlocked += other.deadlocked;
        return (*this);
    }

    ShoreSSBTrxStats& operator-=(ShoreSSBTrxStats const& other) {
        attempted  -= other.attempted;
        failed     -= other.failed;
        deadlocked -= other.deadlocked;
        return (*this);
    }

}; // EOF: ShoreSSBTrxStats


/******************************************************************** 
 * 
 *  ShoreSSBEnv
 *  
 *  Shore TPC-H Database.
 *
 ********************************************************************/

class ShoreSSBEnv : public ShoreEnv
{
public:
    typedef std::map<pthread_t, ShoreSSBTrxStats*> statmap_t;

    class table_builder_t;
    class table_creator_t;
    struct checkpointer_t;

private:

    w_rc_t _post_init_impl();

    // Helper functions for the loading
    //w_rc_t _gen_one_nation(const int id, rep_row_t& areprow);
    //w_rc_t _gen_one_region(const int id, rep_row_t& areprow);
    w_rc_t _gen_one_supplier(const int id, rep_row_t& areprow);
    w_rc_t _gen_one_date(const int id, rep_row_t& areprow);
    w_rc_t _gen_one_part(const int id, rep_row_t& areprow);
    w_rc_t _gen_one_customer(const int id, rep_row_t& areprow);
    w_rc_t _gen_one_lineorder(const int id, rep_row_t& areprow);

    //    w_rc_t _gen_one_part_based(const int id, rep_row_t& areprow);
    //w_rc_t _gen_one_cust_based(const int id, rep_row_t& areprow);
    
public:    

    ShoreSSBEnv();
    virtual ~ShoreSSBEnv();

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

    w_rc_t warmup();
    w_rc_t check_consistency();

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
    
    // SSB Tables
    DECLARE_TABLE(part_t,part_man_impl,part);
    DECLARE_TABLE(supplier_t,supplier_man_impl,supplier);
    DECLARE_TABLE(date_t,date_man_impl,date);
    DECLARE_TABLE(customer_t,customer_man_impl,customer);
    DECLARE_TABLE(lineorder_t,lineorder_man_impl,lineorder);
    

    // --- kit baseline trxs --- //

    w_rc_t run_one_xct(Request* prequest);

    // QUERIES (Transactions)
    DECLARE_TRX(qpart);
    DECLARE_TRX(qdate);
    DECLARE_TRX(qsupplier);
    DECLARE_TRX(qcustomer);
    DECLARE_TRX(qlineorder);
    DECLARE_TRX(qtest);
    DECLARE_TRX(q1_1);
    DECLARE_TRX(q1_2);
    DECLARE_TRX(q1_3);
    DECLARE_TRX(q2_1);
    DECLARE_TRX(q2_2);
    DECLARE_TRX(q2_3);
    DECLARE_TRX(q3_1);
    DECLARE_TRX(q3_2);
    DECLARE_TRX(q3_3);
    DECLARE_TRX(q3_4);
    DECLARE_TRX(q4_1);
    DECLARE_TRX(q4_2);
    DECLARE_TRX(q4_3);

    // QUERIES for the non-partition aligned benchmark
    DECLARE_TRX(qNP);

    // Database population
    DECLARE_TRX(populate_baseline);
    DECLARE_TRX(populate_some_lineorders);
    //    DECLARE_TRX(populate_some_parts);
    //    DECLARE_TRX(populate_some_custs);


#ifdef CFG_QPIPE
private:
    guard<policy_t> _sched_policy;

public:
    policy_t* get_sched_policy();
    policy_t* set_sched_policy(const char* spolicy);
    w_rc_t run_one_qpipe_xct(Request* prequest);

    // QPipe QUERIES (Transactions)
    DECLARE_QPIPE_TRX(qpart);
    DECLARE_QPIPE_TRX(qdate);
    DECLARE_QPIPE_TRX(qsupplier);
    DECLARE_QPIPE_TRX(qcustomer);
    DECLARE_QPIPE_TRX(qlineorder);
    DECLARE_QPIPE_TRX(qtest);
    DECLARE_QPIPE_TRX(q1_1);
    DECLARE_QPIPE_TRX(q1_2);
    DECLARE_QPIPE_TRX(q1_3);
    DECLARE_QPIPE_TRX(q2_1);
    DECLARE_QPIPE_TRX(q2_2);
    DECLARE_QPIPE_TRX(q2_3);
    DECLARE_QPIPE_TRX(q3_1);
    DECLARE_QPIPE_TRX(q3_2);
    DECLARE_QPIPE_TRX(q3_3);
    DECLARE_QPIPE_TRX(q3_4);
    DECLARE_QPIPE_TRX(q4_1);
    DECLARE_QPIPE_TRX(q4_2);
    DECLARE_QPIPE_TRX(q4_3);
#endif


    // for thread-local stats
    virtual void env_thread_init();
    virtual void env_thread_fini();   

    // stat map
    statmap_t _statmap;

    // snapshot taken at the beginning of each experiment    
    ShoreSSBTrxStats _last_stats;
    virtual void reset_stats();
    ShoreSSBTrxStats _get_stats();
    
}; // EOF ShoreSSBEnv
 

EXIT_NAMESPACE(ssb);


#endif /* __SHORE_SSB_ENV_H */
