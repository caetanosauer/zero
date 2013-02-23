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

/** @file:   shore_tm1_env.h
 *
 *  @brief:  Definition of the Shore TM1 environment
 *
 *  @author: Ippokratis Pandis, Feb 2009
 */

#ifndef __SHORE_TM1_ENV_H
#define __SHORE_TM1_ENV_H


#include "sm_vas.h"
#include "util.h"

#include "workload/tm1/tm1_const.h"
#include "workload/tm1/tm1_input.h"

#include "sm/shore/shore_env.h"
#include "sm/shore/shore_trx_worker.h"

#include "workload/tm1/shore_tm1_schema_man.h"

#include <map>

using std::map;
using namespace shore;


ENTER_NAMESPACE(tm1);



/******************************************************************** 
 * 
 *  ShoreTM1Env Stats
 *  
 *  Shore TM1 Database transaction statistics
 *
 ********************************************************************/

struct ShoreTM1TrxCount
{
    uint get_sub_data;
    uint get_new_dest;
    uint get_acc_data;
    uint upd_sub_data;
    uint upd_loc;
    uint ins_call_fwd;
    uint del_call_fwd;

    uint get_sub_nbr;

    uint ins_call_fwd_bench;
    uint del_call_fwd_bench;
    
    ShoreTM1TrxCount& operator+=(ShoreTM1TrxCount const& rhs) {
        get_sub_data += rhs.get_sub_data;
        get_new_dest += rhs.get_new_dest;
        get_acc_data += rhs.get_acc_data;
        upd_sub_data += rhs.upd_sub_data;
        upd_loc += rhs.upd_loc;
        ins_call_fwd += rhs.ins_call_fwd;
        del_call_fwd += rhs.del_call_fwd;

        get_sub_nbr += rhs.get_sub_nbr;

	ins_call_fwd_bench += rhs.ins_call_fwd_bench;
        del_call_fwd_bench += rhs.del_call_fwd_bench;

	return (*this);
    }

    ShoreTM1TrxCount& operator-=(ShoreTM1TrxCount const& rhs) {
        get_sub_data -= rhs.get_sub_data;
        get_new_dest -= rhs.get_new_dest;
        get_acc_data -= rhs.get_acc_data;
        upd_sub_data -= rhs.upd_sub_data;
        upd_loc -= rhs.upd_loc;
        ins_call_fwd -= rhs.ins_call_fwd;
        del_call_fwd -= rhs.del_call_fwd;

        get_sub_nbr -= rhs.get_sub_nbr;

	ins_call_fwd_bench -= rhs.ins_call_fwd_bench;
        del_call_fwd_bench -= rhs.del_call_fwd_bench;

	return (*this);
    }

    uint total() const {
        return (get_sub_data+get_new_dest+get_acc_data+
                upd_sub_data+upd_loc+ins_call_fwd+del_call_fwd+
                get_sub_nbr+ins_call_fwd_bench+del_call_fwd_bench);
    }
    
}; // EOF: ShoreTM1TrxCount


struct ShoreTM1TrxStats
{
    ShoreTM1TrxCount attempted;
    ShoreTM1TrxCount failed;
    ShoreTM1TrxCount deadlocked;

    ShoreTM1TrxStats& operator+=(ShoreTM1TrxStats const& other) {
        attempted  += other.attempted;
        failed     += other.failed;
        deadlocked += other.deadlocked;
        return (*this);
    }

    ShoreTM1TrxStats& operator-=(ShoreTM1TrxStats const& other) {
        attempted  -= other.attempted;
        failed     -= other.failed;
        deadlocked -= other.deadlocked;
        return (*this);
    }

}; // EOF: ShoreTM1TrxStats




/******************************************************************** 
 * 
 *  ShoreTM1Env
 *  
 *  Shore TM1 Database
 *
 ********************************************************************/

class ShoreTM1Env : public ShoreEnv
{
public:

    typedef std::map<pthread_t, ShoreTM1TrxStats*> statmap_t;

    class table_builder_t;
    class table_creator_t;

private:
    w_rc_t _post_init_impl();
    
public:    

    ShoreTM1Env();
    virtual ~ShoreTM1Env();

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

    virtual w_rc_t warmup() { return(RCOK); /* do nothing */ };
    virtual w_rc_t check_consistency() { return(RCOK); /* do nothing */ };

    virtual void print_throughput(const double iQueriedSF, 
                                  const int iSpread, 
                                  const int iNumOfThreads,
                                  const double delay,
                                  const ulong_t mioch,
                                  const double avgcpuusage);

    // Public methods //    

    // --- operations over tables --- //
    virtual w_rc_t loaddata();  
    w_rc_t xct_populate_one(const int sub_id);

    // TM1 tables
    DECLARE_TABLE(subscriber_t,sub_man_impl,sub);
    DECLARE_TABLE(access_info_t,ai_man_impl,ai);
    DECLARE_TABLE(special_facility_t,sf_man_impl,sf);
    DECLARE_TABLE(call_forwarding_t,cf_man_impl,cf);


    // --- kit trxs --- //

    w_rc_t run_one_xct(Request* prequest);

    DECLARE_TRX(get_sub_data);
    DECLARE_TRX(get_new_dest);
    DECLARE_TRX(get_acc_data);
    DECLARE_TRX(upd_sub_data);
    DECLARE_TRX(upd_loc);
    DECLARE_TRX(ins_call_fwd);
    DECLARE_TRX(del_call_fwd);

    DECLARE_TRX(get_sub_nbr);

    DECLARE_TRX(ins_call_fwd_bench);
    DECLARE_TRX(del_call_fwd_bench);
    
    // Update the partitioning info, if any needed
    virtual w_rc_t update_partitioning();

    // for thread-local stats
    virtual void env_thread_init();
    virtual void env_thread_fini();   

    // stat map
    statmap_t _statmap;

    // snapshot taken at the beginning of each experiment    
    ShoreTM1TrxStats _last_stats;
    virtual void reset_stats();
    ShoreTM1TrxStats _get_stats();

    // set load imbalance and time to apply it
    void set_skew(int area, int load, int start_imbalance);
    void start_load_imbalance();
    void reset_skew();
    
    //print the current tables into files
    w_rc_t db_print(int lines);

    //fetch the pages of the current tables and their indexes into the buffer pool
    w_rc_t db_fetch();
    
}; // EOF ShoreTM1Env
   


EXIT_NAMESPACE(tm1);


#endif /* __SHORE_TM1_ENV_H */

