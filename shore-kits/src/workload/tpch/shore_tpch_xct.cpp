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

/** @file:   shore_tpch_xct.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-H transactions
 *
 *  @author: Nastaran Nikparto, Summer 2011
 *  @author: Ippokratis Pandis (ipandis)
 */

#include "workload/tpch/shore_tpch_env.h"
#include "workload/tpch/tpch_random.h"
#include "workload/tpch/tpch_struct.h"
#include "workload/tpch/tpch_util.h"

#include <vector>
#include <map>
#include <set>
#include <numeric>
#include <algorithm>
#include <stdio.h>
#include <fstream>
#include "workload/tpch/dbgen/dss.h"
#include "workload/tpch/dbgen/dsstypes.h"

// #include "qp/shore/qp_util.h"
// #include "qp/shore/ahhjoin.h"

#include "sm_base.h"

using namespace std;
using namespace shore;
using namespace dbgentpch;
//using namespace qp;


ENTER_NAMESPACE(tpch);



/******************************************************************** 
 *
 * Thread-local TPC-H TRXS Stats
 *
 ********************************************************************/


static __thread ShoreTPCHTrxStats my_stats;

void ShoreTPCHEnv::env_thread_init()
{
    CRITICAL_SECTION(stat_mutex_cs, _statmap_mutex);
    _statmap[pthread_self()] = &my_stats;
}

void ShoreTPCHEnv::env_thread_fini()
{
    CRITICAL_SECTION(stat_mutex_cs, _statmap_mutex);
    _statmap.erase(pthread_self());
}


/******************************************************************** 
 *
 *  @fn:    _get_stats
 *
 *  @brief: Returns a structure with the currently stats
 *
 ********************************************************************/

ShoreTPCHTrxStats ShoreTPCHEnv::_get_stats()
{
    CRITICAL_SECTION(cs, _statmap_mutex);
    ShoreTPCHTrxStats rval;
    rval -= rval; // dirty hack to set all zeros
    for (statmap_t::iterator it=_statmap.begin(); it != _statmap.end(); ++it) 
        rval += *it->second;
    return (rval);
}


/******************************************************************** 
 *
 *  @fn:    reset_stats
 *
 *  @brief: Updates the last gathered statistics
 *
 ********************************************************************/

void ShoreTPCHEnv::reset_stats()
{
    CRITICAL_SECTION(last_stats_cs, _last_stats_mutex);
    _last_stats = _get_stats();
}


/******************************************************************** 
 *
 *  @fn:    print_throughput
 *
 *  @brief: Prints the throughput given a measurement delay
 *
 ********************************************************************/

void ShoreTPCHEnv::print_throughput(const double iQueriedSF, 
                                    const int iSpread, 
                                    const int iNumOfThreads, 
                                    const double delay,
                                    const ulong_t mioch,
                                    const double avgcpuusage)
{
    CRITICAL_SECTION(last_stats_cs, _last_stats_mutex);
    
    // get the current statistics
    ShoreTPCHTrxStats current_stats = _get_stats();
	   
    // now calculate the diff
    current_stats -= _last_stats;
	       
    uint trxs_att  = current_stats.attempted.total();
    uint trxs_abt  = current_stats.failed.total();
    uint trxs_dld  = current_stats.deadlocked.total();    

    TRACE( TRACE_ALWAYS, "*******\n"             \
           "QueriedSF: (%.1f)\n"                 \
           "Spread:    (%s)\n"                   \
           "Threads:   (%d)\n"                   \
           "Trxs Att:  (%d)\n"                   \
           "Trxs Abt:  (%d)\n"                   \
           "Trxs Dld:  (%d)\n"                   \
           "Secs:      (%.2f)\n"                 \
           "IOChars:   (%.2fM/s)\n"              \
           "AvgCPUs:   (%.1f) (%.1f%%)\n"        \
           "TPS:       (%.2f)\n",
           iQueriedSF, 
           (iSpread ? "Yes" : "No"),
           iNumOfThreads, trxs_att, trxs_abt, trxs_dld,
           delay, mioch/delay, avgcpuusage, 
           100*avgcpuusage/get_max_cpu_count(),
           (trxs_att-trxs_abt-trxs_dld)/delay);
}




/******************************************************************** 
 *
 * TPC-H Parallel Loading
 *
 ********************************************************************/

/*
  DATABASE POPULATION TRANSACTIONS

  The TPC-H database has 8 tables. Out of those:
  3 are based on the Customers (CUSTOMER,ORDER,LINEITEM)
  2 are based on the Parts (PART,PARTSUPP)
  2 are static (NATION,REGION)
  1 depends on the SF but not Customers or Parts (SUPPLIER)

  Regular cardinalities:

  Supplier :                10K*SF     (2MB)
  Nation   :                25         (<1MB)
  Region   :                5          (<1MB)

  Part     :                0.2M*SF    (30MB)
  PartSupp : 4*Part     =   0.8M*SF    (110MB)

  Customer :                0.15M*SF   (25MB)
  Order    : 10*Cust    =   1.5M*SF    (150MB)
  Lineitem : [1,7]*Cust =   6M*SF      (650MB)

  
  The table creator:
  1) Creates all the tables 
  2) Loads completely the first 3 tables (Supplier,Nation,Region)
  3) Loads #ParLoaders*DIVISOR Part units (Part,PartSupp)
  4) Loads #ParLoaders*DIVISOR Customer units (Customer,Order,Lineitem)


  The sizes of the records:
  NATION:   192
  REGION:   56
  SUPPLIER: 208
  PART:     176
  PARTSUPP: 224
  CUSTOMER: 240
  ORDERS:   152
  LINEITEM: 152

*/


/******************************************************************** 
 *
 * Those functions populate records for the TPC-H database. They do not
 * commit thought. So, they need to be invoked inside a transaction
 * and the caller needs to commit at the end. 
 *
 ********************************************************************/

#undef  DO_PRINT_TPCH_RECS
//#define DO_PRINT_TPCH_RECS

// Populates one nation
w_rc_t ShoreTPCHEnv::_gen_one_nation(const int id, rep_row_t& areprow)
{    
    tuple_guard<nation_man_impl> prna(_pnation_man);
    prna->_rep = &areprow;

    code_t ac;
    mk_nation(id, &ac);
    
#ifdef DO_PRINT_TPCH_RECS
    TRACE( TRACE_ALWAYS, "%ld,%s,%ld,%s,%d\n",
	   ac.code, ac.text, ac.join, ac.comment, ac.clen);
#endif
    
    prna->set_value(0, (int)ac.code);
    prna->set_value(1, ac.text);
    prna->set_value(2, (int)ac.join);
    prna->set_value(3, ac.comment);    
    W_DO(_pnation_man->add_tuple(_pssm, prna));

    return RCOK;
}

// Populates one region
w_rc_t ShoreTPCHEnv::_gen_one_region(const int id, rep_row_t& areprow)
{    
    tuple_guard<region_man_impl> prre(_pregion_man);
    prre->_rep = &areprow;

    code_t ac;
    mk_region(id, &ac);

#ifdef DO_PRINT_TPCH_RECS
    TRACE( TRACE_ALWAYS, "%ld,%s,%s,%d\n", 
           ac.code, ac.text, ac.comment, ac.clen);
#endif

    prre->set_value(0, (int)ac.code);
    prre->set_value(1, ac.text);
    prre->set_value(2, ac.comment);
    W_DO(_pregion_man->add_tuple(_pssm, prre));

    return RCOK;
}

// Populates one supplier
w_rc_t ShoreTPCHEnv::_gen_one_supplier(const int id, rep_row_t& areprow)
{    
    tuple_guard<supplier_man_impl> prsu(_psupplier_man);
    prsu->_rep = &areprow;

    dbgentpch::supplier_t as;
    mk_supp(id, &as);
    
#ifdef DO_PRINT_TPCH_RECS
    if (id%100==0) {
        TRACE( TRACE_ALWAYS, "%ld,%s,%s,%d,%ld,%s,%ld,%s,%d\n", 
               as.suppkey,as.name,as.address,as.alen,as.nation_code,
               as.phone,as.acctbal,as.comment,as.clen); 
    }
#endif
    
    prsu->set_value(0, (int)as.suppkey);
    prsu->set_value(1, as.name);
    prsu->set_value(2, as.address);
    prsu->set_value(3, (int)as.nation_code);
    prsu->set_value(4, as.phone);
    prsu->set_value(5, (double)as.acctbal);
    prsu->set_value(6, as.comment);
    W_DO(_psupplier_man->add_tuple(_pssm, prsu));

    return RCOK;
}

// Populates one part and the corresponding partsupp
w_rc_t ShoreTPCHEnv::_gen_one_part_based(const int id, rep_row_t& areprow)
{    
    tuple_guard<part_man_impl> prpa(_ppart_man);
    tuple_guard<partsupp_man_impl> prps(_ppartsupp_man);

    prpa->_rep = &areprow;
    prps->_rep = &areprow;

    // 1. Part
    dbgentpch::part_t ap;
    mk_part(id, &ap);
    
#ifdef DO_PRINT_TPCH_RECS
    if (id%100==0) {
	TRACE( TRACE_ALWAYS, "%ld,%s,%s,%s,%s,%ld,%s,%ld,%s\n",
	       ap.partkey,ap.name,ap.mfgr,ap.brand,ap.type,
	       ap.size,ap.container,ap.retailprice,ap.comment); 
    }
#endif

    prpa->set_value(0, (int)ap.partkey);
    prpa->set_value(1, ap.name);
    prpa->set_value(2, ap.mfgr);
    prpa->set_value(3, ap.brand);
    prpa->set_value(4, ap.type);
    prpa->set_value(5, (int)ap.size);
    prpa->set_value(6, ap.container);
    prpa->set_value(7, (double)ap.retailprice);
    prpa->set_value(8, ap.comment);
    W_DO(_ppart_man->add_tuple(_pssm, prpa));
    
    for (int i=0; i< SUPP_PER_PART; ++i) {
	
#ifdef DO_PRINT_TPCH_RECS
	if (id%100==0) {
	    TRACE( TRACE_ALWAYS, "%ld,%ld,%ld,%ld,%s\n",
		   ap.s[i].partkey,ap.s[i].suppkey,
		   ap.s[i].qty,ap.s[i].scost,
		   ap.s[i].comment); 
	}
#endif

	// 2. PartSupp
	prps->set_value(0, (int)ap.s[i].partkey);
	prps->set_value(1, (int)ap.s[i].suppkey);
	prps->set_value(2, (int)ap.s[i].qty);
	prps->set_value(3, (double)ap.s[i].scost);
	prps->set_value(4, ap.s[i].comment);
        W_DO(_ppartsupp_man->add_tuple(_pssm, prps));
    }

    return RCOK;
}

// Populates one customer and the corresponding orders and lineitems
w_rc_t ShoreTPCHEnv::_gen_one_cust_based(const int id, rep_row_t& areprow)
{
    tuple_guard<customer_man_impl> prcu(_pcustomer_man);
    tuple_guard<orders_man_impl> pror(_porders_man);
    tuple_guard<lineitem_man_impl> prli(_plineitem_man);

    prcu->_rep = &areprow;
    pror->_rep = &areprow;
    prli->_rep = &areprow;

    // 1. Customer
    dbgentpch::customer_t ac;
    mk_cust(id, &ac);
    
#ifdef DO_PRINT_TPCH_RECS        
    if (id%100==0) {
	TRACE( TRACE_ALWAYS, "%ld,%s,%s,%ld,%s,%ld,%s,%s\n",
	       ac.custkey,ac.name,ac.address,ac.nation_code,
	       ac.phone,ac.acctbal,ac.mktsegment,ac.comment); 
    }
#endif
    
    prcu->set_value(0, (int)ac.custkey);
    prcu->set_value(1, ac.name);
    prcu->set_value(2, ac.address);
    prcu->set_value(3, (int)ac.nation_code);
    prcu->set_value(4, ac.phone);
    prcu->set_value(5, (double)ac.acctbal);
    prcu->set_value(6, ac.mktsegment);
    prcu->set_value(7, ac.comment);
    W_DO( _pcustomer_man->add_tuple(_pssm, prcu));

    for (int i=0; i<ORDERS_PER_CUSTOMER; ++i) {
	// 2. Orders            
	dbgentpch::order_t ao;
	mk_order(id*10+i, &ao, 0);
	
#ifdef DO_PRINT_TPCH_RECS
	if (id%100==0) {
	    TRACE( TRACE_ALWAYS, "%ld,%ld,%s,%ld,%s,%s,%s,%ld,%s\n",
		   ao.okey,ao.custkey,ao.orderstatus,ao.totalprice,
		   ao.odate,ao.opriority,ao.clerk,ao.spriority,ao.comment); 
	}
#endif
	
	pror->set_value(0, (int)ao.okey);            
	pror->set_value(1, (int)ao.custkey);
	pror->set_value(2, ao.orderstatus);
	pror->set_value(3, (double)ao.totalprice);
	pror->set_value(4, ao.odate);
	pror->set_value(5, ao.opriority);
	pror->set_value(6, ao.clerk);
	pror->set_value(7, (int)ao.spriority);
	pror->set_value(8, ao.comment);
	W_DO(_porders_man->add_tuple(_pssm, pror));

	for (int j=0; j<ao.lines; ++j) {
	    // 3. LineItem	    
#ifdef DO_PRINT_TPCH_RECS
	    if (id%100==0) {
		TRACE(TRACE_ALWAYS,"%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%s,%s,%s,%s,%s,%s,%s,%s\n",
		      ao.l[j].okey,ao.l[j].partkey,ao.l[j].suppkey,
		      ao.l[j].lcnt,ao.l[j].quantity,ao.l[j].eprice,
		      ao.l[j].discount,ao.l[j].tax,
		      ao.l[j].rflag,ao.l[j].lstatus,
		      ao.l[j].cdate,ao.l[j].sdate,ao.l[j].rdate,
		      ao.l[j].shipinstruct,ao.l[j].shipmode,ao.l[j].comment); 
	    }
#endif
	    
	    prli->set_value(0, (int)ao.l[j].okey);
	    prli->set_value(1, (int)ao.l[j].partkey);
	    prli->set_value(2, (int)ao.l[j].suppkey);
	    prli->set_value(3, (int)ao.l[j].lcnt);
	    prli->set_value(4, (double)ao.l[j].quantity);
	    prli->set_value(5, (double)ao.l[j].eprice);
	    prli->set_value(6, (double)ao.l[j].discount);
	    prli->set_value(7, (double)ao.l[j].tax);
	    prli->set_value(8, ao.l[j].rflag);
	    prli->set_value(9, ao.l[j].lstatus);
	    prli->set_value(10, ao.l[j].cdate);
	    prli->set_value(11, ao.l[j].sdate);
	    prli->set_value(12, ao.l[j].rdate);
	    prli->set_value(13, ao.l[j].shipinstruct);
	    prli->set_value(14, ao.l[j].shipmode);
	    prli->set_value(15, ao.l[j].comment);
	    W_DO(_plineitem_man->add_tuple(_pssm, prli));
	}
    }

    return RCOK;
}


/******************************************************************** 
 *
 * TPC-H Loading: Population transactions
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_populate_baseline(const int /* xct_id */, 
                                           populate_baseline_input_t& in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);

    // The Customer is the biggest (240) of all the tables
    rep_row_t areprow(_pcustomer_man->ts());
    areprow.set(_pcustomer_desc->maxsize());

    // 2. Build the small tables
    TRACE( TRACE_ALWAYS, "Building NATION !!!\n");    
    for (int i=0; i<NO_NATIONS; ++i) {
        W_DO(_gen_one_nation(i, areprow));
    }

    TRACE( TRACE_ALWAYS, "Building REGION !!!\n");
    for (int i=0; i<NO_REGIONS; ++i) {
        W_DO(_gen_one_region(i, areprow));
    }

    TRACE( TRACE_ALWAYS, "Building SUPPLIER !!!\n");
    for (int i=0; i<in._sf*SUPPLIER_PER_SF; ++i) {
        W_DO(_gen_one_supplier(i, areprow));
    }

    W_COERCE(_pssm->commit_xct());
    W_COERCE(_pssm->begin_xct());

    // 3. Insert first rows in the Part-based tables
    if (in._loader_count == 1) {
        int sf=in._sf;
        int ppsf=PART_PER_SF;
        TRACE( TRACE_ALWAYS, "Building PARTS and PARTSUPP (%d)!!!\n",sf*ppsf);
        //MA: A simplified version of single threaded build for correctness.
        int step=in._sf*PART_PER_SF/10;
        for (int i=0; i<in._sf*PART_PER_SF; ++i) {
            W_DO(_gen_one_part_based(i, areprow));
            if (i>0 && i%step==0) {
                TRACE( TRACE_ALWAYS, "PARTS-PARTSUPP(%d\%)!!!\n", 10*i/step);
                W_COERCE(_pssm->commit_xct());
                W_COERCE(_pssm->begin_xct());
            }
        }
    } else {
        TRACE( TRACE_ALWAYS, "Starting PARTS !!!\n");
        for (int i = 0; i < in._loader_count; ++i) {
            long start = i * in._parts_per_thread;
            long end = start + in._divisor - 1;
            TRACE(TRACE_ALWAYS, "Parts %d .. %d\n", start, end);
	    for (int j = start; j < end; ++j) {
                W_DO(_gen_one_part_based(j, areprow));
            }
        }
    }
    
    // 4. Insert first rows in the Customer-based tables
    if (in._loader_count == 1) {
        int sf=in._sf;
        int cpsf=CUSTOMER_PER_SF;
        TRACE( TRACE_ALWAYS,"Building CUSTS, ORDERS, LINEITEM (%d)!!!\n",sf*cpsf);
        //MA: A simplified version of single threaded build for correctness.
        int step=in._sf*CUSTOMER_PER_SF/10;
        int step100=in._sf*CUSTOMER_PER_SF/100;
        int current=0;
        for (int i=0; i<in._sf*CUSTOMER_PER_SF; ++i) {
            W_DO(_gen_one_cust_based(i,areprow));
            if (i>0 && i%step==0) {
                TRACE(TRACE_ALWAYS,"\nCUSTS-ORDERS-LINEITEM(%d\%)!\n",100*i/step);
            }
            if (i>0 && i%step100==0) {
                W_COERCE(_pssm->commit_xct());
                W_COERCE(_pssm->begin_xct());
            }
            if (current==1000) {
                printf(".");
                current=0;
            } else {
                current++;
	    }
        }
    } else {
        TRACE( TRACE_ALWAYS, "Starting CUSTS !!!\n");
        for (int i=0; i < in._loader_count; ++i) {
            long start = i*in._custs_per_thread;
            long end = start + in._divisor - 1;
            TRACE( TRACE_ALWAYS, "[%d/%d] Custs %d .. %d\n",
		   i, in._loader_count, start, end);
            for (int j=start; j<end; ++j) {
                W_DO(_gen_one_cust_based(j,areprow));
            }
        }
    }
    
    return (_pssm->commit_xct());
}

// We pass on the parameter (xct_id) the number of parts to be populated
// and on the parameter (in) the starting id
w_rc_t ShoreTPCHEnv::xct_populate_some_parts(const int xct_id, 
                                             populate_some_parts_input_t& in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);

    // The Partsupp is the biggest (224) of all the 2 tables
    rep_row_t areprow(_ppartsupp_man->ts());
    areprow.set(_ppartsupp_desc->maxsize());

    int id = in._partid;

    // Generate (xct_id) parts
    for (id=in._partid; id<in._partid+xct_id; id++) {
        W_DO(_gen_one_part_based(id, areprow));
    }

    return (_pssm->commit_xct());
}

// We pass on the parameter (xct_id) the number of customers to be populated
// and on the parameter (in) the starting id
w_rc_t ShoreTPCHEnv::xct_populate_some_custs(const int xct_id, 
                                             populate_some_custs_input_t& in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);

    // The Customer is the biggest (240) of the 3 tables
    rep_row_t areprow(_pcustomer_man->ts());
    areprow.set(_pcustomer_desc->maxsize()); 

    int id = in._custid;

    // Generate (xct_id) customers
    for (id=in._custid; id<in._custid+xct_id; id++) {
        W_DO(_gen_one_cust_based(id, areprow));
    }

    return (_pssm->commit_xct());
} 



/******************************************************************** 
 *
 * TPC-H QUERIES (TRXS)
 *
 * (1) The run_XXX functions are wrappers to the real transactions
 * (2) The xct_XXX functions are the implementation of the transactions
 *
 ********************************************************************/


/********************************************************************* 
 *
 *  @fn:    run_one_xct
 *
 *  @brief: Baseline client - Entry point for running one trx 
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure internal has expired.
 *
 *********************************************************************/

w_rc_t ShoreTPCHEnv::run_one_xct(Request* prequest)
{
#ifdef CFG_QPIPE
    if (prequest->type() >= XCT_QPIPE_TPCH_MIX) {
        return (run_one_qpipe_xct(prequest));
    }
#endif

    // if BASELINE TPC-H MIX
    if (prequest->type() == XCT_TPCH_MIX) {
        prequest->set_type(XCT_TPCH_MIX + abs(smthread_t::me()->rand()%22));
    }

    switch (prequest->type()) {

        // TPC-H BASELINE
    case XCT_TPCH_Q1:
        return (run_q1(prequest));

    case XCT_TPCH_Q2:
        return (run_q2(prequest));

    case XCT_TPCH_Q3:
        return (run_q3(prequest));

    case XCT_TPCH_Q4:
        return (run_q4(prequest));

    case XCT_TPCH_Q5:
        return (run_q5(prequest));

    case XCT_TPCH_Q6:
        return (run_q6(prequest));;

    case XCT_TPCH_Q7:
        return (run_q7(prequest));

    case XCT_TPCH_Q8:
        return (run_q8(prequest));

    case XCT_TPCH_Q9:
        return (run_q9(prequest));

    case XCT_TPCH_Q10:
        return (run_q10(prequest));

    case XCT_TPCH_Q11:
        return (run_q11(prequest));

    case XCT_TPCH_Q12:
        return (run_q12(prequest));;

    case XCT_TPCH_Q13:
        return (run_q13(prequest));

    case XCT_TPCH_Q14:
        return (run_q14(prequest));;

    case XCT_TPCH_Q15:
        return (run_q15(prequest));

    case XCT_TPCH_Q16:
        return (run_q16(prequest));

    case XCT_TPCH_Q17:
        return (run_q17(prequest));

    case XCT_TPCH_Q18:
        return (run_q18(prequest));

    case XCT_TPCH_Q19:
        return (run_q19(prequest));

    case XCT_TPCH_Q20:
        return (run_q20(prequest));

    case XCT_TPCH_Q21:
        return (run_q21(prequest));

    case XCT_TPCH_Q22:
        return (run_q22(prequest));

    case XCT_TPCH_QLINEITEM:
	return (run_qlineitem(prequest));

    case XCT_TPCH_QORDERS:
	return (run_qorders(prequest));

    case XCT_TPCH_QREGION:
	return (run_qregion(prequest));

    case XCT_TPCH_QNATION:
	return (run_qnation(prequest));

    case XCT_TPCH_QCUSTOMER:
	return (run_qcustomer(prequest));

    case XCT_TPCH_QSUPPLIER:
	return (run_qsupplier(prequest));

    case XCT_TPCH_QPART:
	return (run_qpart(prequest));

    case XCT_TPCH_QPARTSUPP:
	return (run_qpartsupp(prequest));

    default:
        //assert (0); // UNKNOWN TRX-ID
        TRACE( TRACE_ALWAYS, "Unknown transaction\n");
        assert(0);
    }
    return (RCOK);
}


/******************************************************************** 
 *
 * TPC-H TRXs Wrappers
 *
 * @brief: They are wrappers to the functions that execute the transaction
 *         body. Their responsibility is to:
 *
 *         1. Prepare the corresponding input
 *         2. Check the return of the trx function and abort the trx,
 *            if something went wrong
 *         3. Update the tpch db environment statistics
 *
 ********************************************************************/


// --- without input specified --- //

DEFINE_TRX(ShoreTPCHEnv,q1);
DEFINE_TRX(ShoreTPCHEnv,q2);
DEFINE_TRX(ShoreTPCHEnv,q3);
DEFINE_TRX(ShoreTPCHEnv,q4);
DEFINE_TRX(ShoreTPCHEnv,q5);
DEFINE_TRX(ShoreTPCHEnv,q6);
DEFINE_TRX(ShoreTPCHEnv,q7);
DEFINE_TRX(ShoreTPCHEnv,q8);
DEFINE_TRX(ShoreTPCHEnv,q9);
DEFINE_TRX(ShoreTPCHEnv,q10);
DEFINE_TRX(ShoreTPCHEnv,q11);
DEFINE_TRX(ShoreTPCHEnv,q12);
DEFINE_TRX(ShoreTPCHEnv,q13);
DEFINE_TRX(ShoreTPCHEnv,q14);
DEFINE_TRX(ShoreTPCHEnv,q15);
DEFINE_TRX(ShoreTPCHEnv,q16);
DEFINE_TRX(ShoreTPCHEnv,q17);
DEFINE_TRX(ShoreTPCHEnv,q18);
DEFINE_TRX(ShoreTPCHEnv,q19);
DEFINE_TRX(ShoreTPCHEnv,q20);
DEFINE_TRX(ShoreTPCHEnv,q21);
DEFINE_TRX(ShoreTPCHEnv,q22);
DEFINE_TRX(ShoreTPCHEnv,qlineitem);
DEFINE_TRX(ShoreTPCHEnv,qorders);
DEFINE_TRX(ShoreTPCHEnv,qpart);
DEFINE_TRX(ShoreTPCHEnv,qpartsupp);
DEFINE_TRX(ShoreTPCHEnv,qsupplier);
DEFINE_TRX(ShoreTPCHEnv,qnation);
DEFINE_TRX(ShoreTPCHEnv,qregion);
DEFINE_TRX(ShoreTPCHEnv,qcustomer);


// uncomment the line below if want to dump (part of) the trx results
//#define PRINT_TRX_RESULTS

//TODO: MA: remove them when implemented
w_rc_t ShoreTPCHEnv::xct_qlineitem  (const int /* xct_id */, qlineitem_input_t&)
{
    TRACE( TRACE_ALWAYS, "********** SCAN *********\n");
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}

w_rc_t ShoreTPCHEnv::xct_qorders  (const int /* xct_id */, qorders_input_t&)
{
    TRACE( TRACE_ALWAYS, "********** SCAN *********\n");
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}

w_rc_t ShoreTPCHEnv::xct_qnation  (const int /* xct_id */, qnation_input_t&)
{
    TRACE( TRACE_ALWAYS, "********** SCAN *********\n");
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}

w_rc_t ShoreTPCHEnv::xct_qregion  (const int /* xct_id */, qregion_input_t&)
{
    TRACE( TRACE_ALWAYS, "********** SCAN *********\n");
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}

w_rc_t ShoreTPCHEnv::xct_qcustomer(const int /* xct_id */, qcustomer_input_t&)
{
    TRACE( TRACE_ALWAYS, "********** SCAN *********\n");
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}

w_rc_t ShoreTPCHEnv::xct_qsupplier(const int /* xct_id */, qsupplier_input_t&)
{
    TRACE( TRACE_ALWAYS, "********** SCAN *********\n");
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}

w_rc_t ShoreTPCHEnv::xct_qpart    (const int /* xct_id */, qpart_input_t&)
{
    TRACE( TRACE_ALWAYS, "********** SCAN *********\n");
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}

w_rc_t ShoreTPCHEnv::xct_qpartsupp(const int /* xct_id */, qpartsupp_input_t&)
{
    TRACE( TRACE_ALWAYS, "********** SCAN *********\n");
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}



/******************************************************************** 
 *
 * TPC-H Q1
 *
 ********************************************************************/

struct q1_group_by_key_t 
{
    char return_flag;
    char linestatus;

    q1_group_by_key_t(char flag, char status)
    {
        return_flag = flag;
        linestatus = status;
        //memcpy(return_flag, flag, 2);
        //memcpy(linestatus, status, 2);
    };

    q1_group_by_key_t(const q1_group_by_key_t& rhs)
    {
        return_flag = rhs.return_flag;
        linestatus = rhs.linestatus;
        //memcpy(return_flag, rhs.return_flag, 2);
        //memcpy(linestatus, rhs.linestatus, 2);
    };

    q1_group_by_key_t& operator=(const q1_group_by_key_t& rhs)
    {
        return_flag = rhs.return_flag;
        linestatus = rhs.linestatus;
        //memcpy(return_flag, rhs.return_flag, 2);
        //memcpy(linestatus, rhs.linestatus, 2);
        return (*this);
    };
};


class q1_group_by_comp {
public:
    bool operator() (
                     const q1_group_by_key_t& lhs, 
                     const q1_group_by_key_t& rhs) const
    {
        return ((lhs.return_flag < rhs.return_flag) ||
                ((lhs.return_flag == rhs.return_flag) &&
                 (lhs.linestatus < rhs.linestatus))
		);
    }
};


class q1_group_by_value_t {
public:
    int sum_qty;
    int sum_base_price;
    decimal sum_disc_price;
    decimal sum_charge;
    decimal sum_discount;
    int count;

    q1_group_by_value_t() {
        sum_qty = 0;
        sum_base_price = 0;
        sum_disc_price = 0;
        sum_charge = 0;
        sum_discount = 0;
        count = 0;
    }

    q1_group_by_value_t(const q1_group_by_value_t& rhs) {
        sum_qty = rhs.sum_qty;
        sum_base_price = rhs.sum_base_price;
        sum_disc_price = rhs.sum_disc_price;;
        sum_charge = rhs.sum_charge;
        sum_discount = rhs.sum_discount;
        count = rhs.count;
    }

    q1_group_by_value_t& operator+=(const q1_group_by_value_t& rhs)
    {
        sum_qty += rhs.sum_qty;
        sum_base_price += rhs.sum_base_price;
        sum_disc_price += rhs.sum_disc_price;;
        sum_charge += rhs.sum_charge;
        sum_discount += rhs.sum_discount;
        count += rhs.count;

        return(*this);
    }
};


struct q1_output_ele_t 
{
    char l_returnflag;
    char l_linestatus;
    int sum_qty;
    int sum_base_price;
    decimal sum_disc_price;
    decimal sum_charge;
    decimal avg_qty;
    decimal avg_price;
    decimal avg_disc;
    int count_order;
};


w_rc_t ShoreTPCHEnv::xct_q1(const int /* xct_id */, q1_input_t& pq1in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // q1 trx touches 1 tables:
    // lineitem
    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    // allocate space for lineitem representation
    rep_row_t areprow(_plineitem_man->ts());
    areprow.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &areprow;

    /*
      select
      l_returnflag,
      l_linestatus,
      sum(l_quantity) as sum_qty,
      sum(l_extendedprice) as sum_base_price,
      sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
      sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
      avg(l_quantity) as avg_qty,
      avg(l_extendedprice) as avg_price,
      avg(l_discount) as avg_disc,
      count(*) as count_order
      from
      lineitem
      where
      l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
      group by
      l_returnflag,
      l_linestatus
      order by
      l_returnflag,
      l_linestatus;
    */

    /* table scan lineitem */
    guard< table_scan_iter_impl<lineitem_t> > l_iter;
    {
	table_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->get_iter_for_file_scan(_pssm, tmp_l_iter));
	l_iter = tmp_l_iter;
    }
    
    bool eof;
    tpch_lineitem_tuple aline;
    map<q1_group_by_key_t, q1_group_by_value_t, q1_group_by_comp> q1_result;
    map<q1_group_by_key_t, q1_group_by_value_t>::iterator it;
    vector<q1_output_ele_t> q1_output;
    
    /*
      l_returnflag = 8 l_linestatus = 9 l_quantity = 4
      l_extendedprice = 5 l_discount = 6 l_tax = 7
      l_shipdate = 10
    */

    /*
      int sum_qty;
      int sum_base_price;
      decimal sum_disc_price;
      decimal sum_charge;
      decimal sum_discount;
      int count;
    */    
    W_DO(l_iter->next(_pssm, eof, *prlineitem));
    q1_group_by_value_t value;
    while (!eof) {
	prlineitem->get_value(4, aline.L_QUANTITY);
	prlineitem->get_value(5, aline.L_EXTENDEDPRICE);
	prlineitem->get_value(6, aline.L_DISCOUNT);
	prlineitem->get_value(7, aline.L_TAX);
	prlineitem->get_value(8, aline.L_RETURNFLAG);
	prlineitem->get_value(9, aline.L_LINESTATUS);
	prlineitem->get_value(10, aline.L_SHIPDATE, 15);
	
	time_t the_shipdate = str_to_timet(aline.L_SHIPDATE);
	
	if (the_shipdate <= pq1in.l_shipdate) {
	    q1_group_by_key_t key(aline.L_RETURNFLAG, aline.L_LINESTATUS);
	    
	    value.sum_qty = aline.L_QUANTITY;
	    value.sum_base_price = aline.L_EXTENDEDPRICE;
	    value.sum_disc_price = (aline.L_EXTENDEDPRICE * (1-aline.L_DISCOUNT));
	    value.sum_charge = (aline.L_EXTENDEDPRICE * (1-aline.L_DISCOUNT) * 
				(1+aline.L_TAX));
	    value.sum_discount = (aline.L_DISCOUNT);
	    value.count = 1;
	    
	    it = q1_result.find(key);
	    if (it != q1_result.end()) {
		// exists, update 
		(*it).second += value;
	    } else {
		q1_result.insert(pair<q1_group_by_key_t,
				 q1_group_by_value_t>(key, value));
	    }
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }
    
    q1_output_ele_t q1_output_ele;
    for (it = q1_result.begin(); it != q1_result.end(); it ++) {
	q1_output_ele.l_returnflag = (*it).first.return_flag;
	q1_output_ele.l_linestatus = (*it).first.linestatus;
	q1_output_ele.sum_qty = (*it).second.sum_qty;
	q1_output_ele.sum_base_price = (*it).second.sum_base_price;
	q1_output_ele.sum_disc_price = (*it).second.sum_disc_price;
	q1_output_ele.sum_charge = (*it).second.sum_charge;
	q1_output_ele.avg_qty = (*it).second.sum_qty / (*it).second.count;
	q1_output_ele.avg_price = (*it).second.sum_base_price/(*it).second.count;
	q1_output_ele.avg_disc = (*it).second.sum_discount / (*it).second.count;
	q1_output_ele.count_order = (*it).second.count;
	q1_output.push_back(q1_output_ele);
	
	TRACE( TRACE_QUERY_RESULTS, "%d|%d|%d|%d|%.2f|%.2f|%.2f|%.2f|%.2f|%d\n",
	       q1_output_ele.l_returnflag,
	       q1_output_ele.l_linestatus,
	       q1_output_ele.sum_qty,
	       q1_output_ele.sum_base_price,
	       q1_output_ele.sum_disc_price.to_double(),
	       q1_output_ele.sum_charge.to_double(),
	       q1_output_ele.avg_qty.to_double(),
	       q1_output_ele.avg_price.to_double(),
	       q1_output_ele.avg_disc.to_double(),
	       q1_output_ele.count_order);	
    }
    
    return RCOK;
    
}; // EOF: Q1 



/******************************************************************** 
 *
 * TPC-H Q2
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_q2(const int /* xct_id */, q2_input_t& q2in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //table scan part    
    map<int, pair< decimal, vector<int>* > > minlist;

    tuple_guard<part_man_impl> prpart(_ppart_man);
    
    rep_row_t preprow(_ppart_man->ts());
    preprow.set(_ppart_desc->maxsize());

    prpart->_rep = &preprow;

    guard<table_scan_iter_impl<part_t> > p_iter;
    {
	table_scan_iter_impl<part_t>* tmp_p_iter;
	W_DO(_ppart_man->get_iter_for_file_scan(_pssm, tmp_p_iter));
	p_iter = tmp_p_iter;
    }
	    
    bool eof;
    tpch_part_tuple apart;
    
    W_DO(p_iter->next(_pssm, eof, *prpart));

    while(!eof){
	int size;
	prpart->get_value(0, apart.P_PARTKEY);
	prpart->get_value(4, apart.P_TYPE, 25);
	prpart->get_value(5, size);	

	char types3[10];
	types3_to_str(types3, q2in.p_types3);
	    
	if( size == q2in.p_size && strstr(apart.P_TYPE, types3) != NULL){
	    vector<int>* v = new vector<int>();
	    minlist.insert(pair<int,
			   pair< decimal,vector<int>* > >(apart.P_PARTKEY,
							  pair<decimal,
							  vector<int>* >(-1, v)));
	}
	W_DO(p_iter->next(_pssm, eof, *prpart));
    }

    // table scan nation
    map<int,bool> nationK;

    tuple_guard<nation_man_impl> prnation(_pnation_man);

    rep_row_t areprow(_pnation_man->ts());
    areprow.set(_pnation_desc->maxsize()); 
    
    prnation->_rep = &areprow;

    guard< table_scan_iter_impl<nation_t> > n_iter;
    {
	table_scan_iter_impl<nation_t>* tmp_n_iter;
	W_DO(_pnation_man->get_iter_for_file_scan(_pssm, tmp_n_iter));
	n_iter = tmp_n_iter;
    }

    tpch_nation_tuple anation;

    W_DO(n_iter->next(_pssm, eof, *prnation));

    while (!eof) {
	prnation->get_value(0, anation.N_NATIONKEY);
	prnation->get_value(2, anation.N_REGIONKEY);
	if( anation.N_REGIONKEY == q2in.r_name ) {
	    nationK.insert(pair<int,bool> (anation.N_NATIONKEY,0));
	}
	W_DO(n_iter->next(_pssm, eof, *prnation));
    }
    
    //table scan supplier
    map<int, bool> suppK;

    tuple_guard<supplier_man_impl> prsupp(_psupplier_man);

    rep_row_t sreprow(_psupplier_man->ts());
    sreprow.set(_psupplier_desc->maxsize());

    prsupp->_rep = &sreprow;

    guard<table_scan_iter_impl<supplier_t> > s_iter;
    {
	table_scan_iter_impl<supplier_t>* tmp_s_iter;
	W_DO(_psupplier_man->get_iter_for_file_scan(_pssm, tmp_s_iter));
	s_iter = tmp_s_iter;
    }
    
    tpch_supplier_tuple asupplier;
    
    W_DO(s_iter->next(_pssm, eof, *prsupp));

    while(!eof){
	prsupp->get_value(0, asupplier.S_SUPPKEY);
	prsupp->get_value(3, asupplier.S_NATIONKEY);	
	if( nationK.find(asupplier.S_NATIONKEY) != nationK.end()){
	    suppK.insert(pair<int,bool>(asupplier.S_SUPPKEY, true));
	}
	W_DO(s_iter->next(_pssm, eof, *prsupp));
    }
        
    //table scan partsupp
    tuple_guard<partsupp_man_impl> prpartsupp(_ppartsupp_man);

    rep_row_t psreprow(_ppartsupp_man->ts());
    psreprow.set(_ppartsupp_desc->maxsize());

    prpartsupp->_rep = &psreprow;

    guard<table_scan_iter_impl<partsupp_t> > ps_iter;
    {
	table_scan_iter_impl<partsupp_t>* tmp_ps_iter;
	W_DO(_ppartsupp_man->get_iter_for_file_scan(_pssm, tmp_ps_iter));
	ps_iter = tmp_ps_iter;
    }
    
    tpch_partsupp_tuple apartsupp;
    
    W_DO(ps_iter->next(_pssm, eof, *prpartsupp));

    while(!eof){
	prpartsupp->get_value(0, apartsupp.PS_PARTKEY);
	prpartsupp->get_value(1, apartsupp.PS_SUPPKEY);
	prpartsupp->get_value(3, apartsupp.PS_SUPPLYCOST);
	
	map<int, pair< decimal, vector<int>* > >::iterator pit =  minlist.find
	    (apartsupp.PS_PARTKEY);	
	map<int,bool>::iterator sit = suppK.find(apartsupp.PS_SUPPKEY);
	
	if( pit != minlist.end() && sit != suppK.end() ){		
	    if( pit->second.first > apartsupp.PS_SUPPLYCOST ||
		pit->second.first == -1){
		minlist.insert(pair<int, pair< decimal, vector<int>* > >
			       (apartsupp.PS_PARTKEY,pair< decimal, vector<int>* >
				(apartsupp.PS_SUPPLYCOST, new vector<int>()) ));
	    }
	    //problem with decimal datatype
	    if( pit->second.first == apartsupp.PS_SUPPLYCOST){ 		    
		pit->second.second->push_back(apartsupp.PS_SUPPKEY);
	    }
	}
	W_DO(ps_iter->next(_pssm, eof, *prpartsupp));
    }
        
    return RCOK;
}



/******************************************************************** 
 *
 * TPC-H Q3
 *
 ********************************************************************/

struct q3_group_by_key_t 
{
    int l_orderkey;
    time_t o_orderdate;
    int o_shippriority;
  
    q3_group_by_key_t(int okey, time_t date, int shpprrty)
    {
	l_orderkey = okey;
	o_orderdate = date;
	o_shippriority = shpprrty;

    };

    q3_group_by_key_t(const q3_group_by_key_t& rhs)
    {
	l_orderkey = rhs.l_orderkey;
	o_orderdate = rhs.o_orderdate;
	o_shippriority = rhs.o_shippriority;

    };

    q3_group_by_key_t& operator=(const q3_group_by_key_t& rhs)
    {
	l_orderkey = rhs.l_orderkey;
	o_orderdate = rhs.o_orderdate;
	o_shippriority = rhs.o_shippriority;
        return (*this);
    };
};


class q3_group_by_comp {
public:
    bool operator() (
                     const q3_group_by_key_t& lhs, 
                     const q3_group_by_key_t& rhs) const
    {
        return ((lhs.l_orderkey < rhs.l_orderkey) ||
                ((lhs.l_orderkey == rhs.l_orderkey) &&
                 (lhs.o_orderdate < rhs.o_orderdate))||
		(lhs.l_orderkey == rhs.l_orderkey) &&
                 (lhs.o_orderdate == rhs.o_orderdate) &&
		(lhs.o_shippriority < rhs.o_shippriority)
		);
    }
};

class q3_order_needed_data{
public:
    time_t o_orderdate;
    int o_shippriority;

    q3_order_needed_data(time_t date, int shpprrty)
    {
	o_orderdate = date;
	o_shippriority = shpprrty;

    };

};

w_rc_t ShoreTPCHEnv::xct_q3(const int /* xct_id */, q3_input_t&  q3in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //table scan customer
    map<int,bool> custkeys;

    tuple_guard<customer_man_impl> prcustomer(_pcustomer_man);

    rep_row_t areprow(_pcustomer_man->ts());
    areprow.set(_pcustomer_desc->maxsize());

    prcustomer->_rep = &areprow;

    guard< table_scan_iter_impl<customer_t> > c_iter;
    {
	table_scan_iter_impl<customer_t>* tmp_c_iter;
	W_DO(_pcustomer_man->get_iter_for_file_scan(_pssm, tmp_c_iter));
	c_iter = tmp_c_iter;
    }
    
    bool eof;
    tpch_customer_tuple acust;
    
    W_DO(c_iter->next(_pssm, eof, *prcustomer));

    while(!eof) {
	prcustomer->get_value(0, acust.C_CUSTKEY);
	prcustomer->get_value(6, acust.C_MKTSEGMENT, 10);
	int seg = str_to_segment(acust.C_MKTSEGMENT);
	if( seg == q3in.c_segment) {
	    custkeys.insert(pair<int,bool> (acust.C_CUSTKEY, true));
	}
	W_DO(c_iter->next(_pssm, eof, *prcustomer));
    }

    //table scan orders
    int c =0 ;
    map<int, q3_order_needed_data> ordersdt;

    tuple_guard<orders_man_impl> prorder(_porders_man);
    
    rep_row_t o_areprow(_porders_man->ts());
    o_areprow.set(_porders_desc->maxsize());
    
    prorder->_rep = &o_areprow;

    guard< table_scan_iter_impl<orders_t> > o_iter;
    {
	table_scan_iter_impl<orders_t>* tmp_o_iter;
	W_DO(_porders_man->get_iter_for_file_scan(_pssm, tmp_o_iter));
	o_iter = tmp_o_iter;
    }

    tpch_orders_tuple anorder;
    
    W_DO(o_iter->next(_pssm, eof, *prorder));
	
    while(!eof){
	c++;
	prorder->get_value(0, anorder.O_ORDERKEY);
	prorder->get_value(1, anorder.O_CUSTKEY);
	prorder->get_value(4, anorder.O_ORDERDATE, 15);
	prorder->get_value(7, anorder.O_SHIPPRIORITY);	
	time_t the_date = str_to_timet(anorder.O_ORDERDATE);
	if(custkeys.find(anorder.O_CUSTKEY) != custkeys.end()
	    && the_date < q3in.current_date) {		
	    ordersdt.insert(pair<int,q3_order_needed_data>
			    (anorder.O_ORDERKEY, q3_order_needed_data
			     (the_date, anorder.O_SHIPPRIORITY)));
	}
	W_DO(o_iter->next(_pssm, eof, *prorder));
    }
    
    //table scan lineitem
    map<q3_group_by_key_t, double, q3_group_by_comp> shippingQ;

    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t alreprow(_plineitem_man->ts());
    alreprow.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &alreprow;
    
    guard< table_scan_iter_impl<lineitem_t> > l_iter;
    {
	table_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->get_iter_for_file_scan(_pssm, tmp_l_iter));
	l_iter = tmp_l_iter;
    }
    
    tpch_lineitem_tuple aline;

    W_DO(l_iter->next(_pssm, eof, *prlineitem));
    
    while (!eof) {
	prlineitem->get_value(0, aline.L_ORDERKEY);
	prlineitem->get_value(10, aline.L_SHIPDATE, 15);
	prlineitem->get_value(5, aline.L_EXTENDEDPRICE);
	prlineitem->get_value(6, aline.L_DISCOUNT);	
	time_t the_shipdate = str_to_timet(aline.L_SHIPDATE);	
	map<int, q3_order_needed_data>::iterator tmp =
	    ordersdt.find(aline.L_ORDERKEY);
	if(tmp != ordersdt.end() && the_shipdate > q3in.current_date ){
	    map<q3_group_by_key_t, double, q3_group_by_comp>::iterator tmp2 = 
		shippingQ.find(q3_group_by_key_t(aline.L_ORDERKEY,
						 tmp->second.o_orderdate,
						 tmp->second.o_shippriority));
	    double  sum = aline.L_EXTENDEDPRICE * (1-aline.L_DISCOUNT);
	    if(tmp2 != shippingQ.end()){
		sum += tmp2->second; 
	    }
	    shippingQ.insert(pair<q3_group_by_key_t, double >
			     (q3_group_by_key_t(aline.L_ORDERKEY,
						tmp->second.o_orderdate,
						tmp->second.o_shippriority),sum));
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }
    
    return RCOK;
}



/******************************************************************** 
 *
 * TPC-H Q4
 *
 ********************************************************************/

struct ltstr {
    bool operator()(const char* s1, const char* s2) const {
	return strcmp(s1, s2) < 0; }
};


w_rc_t ShoreTPCHEnv::xct_q4(const int /* xct_id */, q4_input_t&  in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //phase one: Index Seek Orders(OrderDate)
    map<int, int> forder_prio; //filtered order + priority
    
    //we need to touch table orders
    tuple_guard<orders_man_impl> prorders(_porders_man);

    //allocate space for its tuple representation
    rep_row_t areprow(_porders_man->ts());
    areprow.set(_porders_desc->maxsize());

    prorders->_rep = & areprow;

    rep_row_t lowrep(_porders_man->ts());
    rep_row_t highrep(_porders_man->ts());

    lowrep.set(_porders_desc->maxsize());
    highrep.set(_porders_desc->maxsize());

    struct tm date = (*localtime(&in.o_orderdate));
    date.tm_mon += 2;
    if(date.tm_mon > 11){
	date.tm_mon -= 12;
	date.tm_year ++;
    }
    time_t last_orderdate = mktime(&date);
	
    tpch_orders_tuple anorder;
    
    guard<index_scan_iter_impl<orders_t> > o_iter;
    {
	index_scan_iter_impl<orders_t>* tmp_o_iter;
	W_DO(_porders_man->o_get_range_iter_by_index(_pssm, tmp_o_iter, prorders,
						     lowrep, highrep,
						     in.o_orderdate,
						     last_orderdate));
	o_iter = tmp_o_iter;
    }
    
    bool eof;
    W_DO(o_iter->next(_pssm, eof, *prorders));
    while(!eof){
	prorders->get_value(0, anorder.O_ORDERKEY);
	prorders->get_value(5, anorder.O_ORDERPRIORITY, 16);
	int p = str_to_priority(anorder.O_ORDERPRIORITY);
	forder_prio.insert(pair<int, int>( anorder.O_ORDERKEY, p));
	W_DO(o_iter->next(_pssm, eof, *prorders));
    }
    
    //phase two: file Scan Lineitem
    map<int,int> priority_count;
    for( int i = 0; i < 5; i++) {
	priority_count.insert(pair<int,int>(i,0));
    }

    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    // allocate space for lineitem representation
    rep_row_t areprow2(_plineitem_man->ts());
    areprow2.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &areprow2;
    
    guard< table_scan_iter_impl<lineitem_t> > l_iter;
    {
	table_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->get_iter_for_file_scan(_pssm, tmp_l_iter));
	l_iter = tmp_l_iter;
    }
            
    tpch_lineitem_tuple aline;

    W_DO(l_iter->next(_pssm, eof, *prlineitem));
    
    while (!eof) {
	prlineitem->get_value(0, aline.L_ORDERKEY);
	prlineitem->get_value(11, aline.L_COMMITDATE, 15);
	prlineitem->get_value(12, aline.L_RECEIPTDATE, 15);	
	time_t the_commitdate = str_to_timet(aline.L_COMMITDATE);
	time_t the_receiptdate = str_to_timet(aline.L_RECEIPTDATE);	
	map<int,int>::iterator tmp;	
	if((tmp = forder_prio.find(aline.L_ORDERKEY)) != forder_prio.end() &&
	   the_commitdate < the_receiptdate){
	    int c =priority_count.find(tmp->second)->second;
	    c++;
	    priority_count[tmp->second] = c;
	    forder_prio.erase( tmp);
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }
           
    return RCOK;
};// EOF: Q4



/******************************************************************** 
 *
 * TPC-H Q5
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_q5(const int /* xct_id */, q5_input_t& q5in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    map<int, double> nation_rev;

    tuple_guard<nation_man_impl> prnation(_pnation_man);

    rep_row_t areprow(_pnation_man->ts());
    areprow.set(_pnation_desc->maxsize()); 

    prnation->_rep = &areprow;
    
    guard< table_scan_iter_impl<nation_t> > n_iter;
    {
	table_scan_iter_impl<nation_t>* tmp_n_iter;
	W_DO(_pnation_man->get_iter_for_file_scan(_pssm, tmp_n_iter));
	n_iter = tmp_n_iter;
    }
        
    tpch_nation_tuple anation;
    bool eof;

    W_DO(n_iter->next(_pssm, eof, *prnation));

    while (!eof) {
	prnation->get_value(0, anation.N_NATIONKEY);
	prnation->get_value(2, anation.N_REGIONKEY);
	if( anation.N_REGIONKEY == q5in.r_name ) {
	    nation_rev.insert(pair<int,double> (anation.N_NATIONKEY,0.0));
	}
	W_DO(n_iter->next(_pssm, eof, *prnation));
    }

    //table scan customer : c_nationkey in nation_rev    
    map<int, int> customer_nation;

    tuple_guard<customer_man_impl> prcustomer(_pcustomer_man);

    rep_row_t acreprow(_pcustomer_man->ts());
    acreprow.set(_pcustomer_desc->maxsize());

    prcustomer->_rep = &acreprow;

    guard< table_scan_iter_impl<customer_t> > c_iter;
    {
	table_scan_iter_impl<customer_t>* tmp_c_iter;
	W_DO(_pcustomer_man->get_iter_for_file_scan(_pssm, tmp_c_iter));
	c_iter = tmp_c_iter;
    }

    tpch_customer_tuple acust;
    
    W_DO(c_iter->next(_pssm, eof, *prcustomer));

    while(!eof){
	prcustomer->get_value(0, acust.C_CUSTKEY);
	prcustomer->get_value(3, acust.C_NATIONKEY);
	if( nation_rev.find(acust.C_NATIONKEY) != nation_rev.end() ){
	    customer_nation.insert(pair<int,int>(acust.C_CUSTKEY,
						 acust.C_NATIONKEY));
	}
	W_DO(c_iter->next(_pssm, eof, *prcustomer));
    }

    //index scan on orderdate
    map<int, int> ordersK_cust;

    tuple_guard<orders_man_impl> prorders(_porders_man);

    rep_row_t aoreprow(_porders_man->ts());
    aoreprow.set(_porders_desc->maxsize());

    prorders->_rep = & aoreprow;

    rep_row_t lowrep(_porders_man->ts());
    rep_row_t highrep(_porders_man->ts());

    lowrep.set(_porders_desc->maxsize());
    highrep.set(_porders_desc->maxsize());

    struct tm date = (*localtime(&q5in.o_orderdate));
    
    date.tm_year += 1;
	
    time_t last_orderdate = mktime(&date);
    
    tpch_orders_tuple anorder;
    
    guard< table_scan_iter_impl<orders_t> > o_iter;
    {
	table_scan_iter_impl<orders_t>* tmp_o_iter;
	W_DO(_porders_man->get_iter_for_file_scan(_pssm, tmp_o_iter));
	o_iter = tmp_o_iter;
    }

    W_DO(o_iter->next(_pssm, eof, *prorders));
    while(!eof){
	prorders->get_value(0, anorder.O_ORDERKEY);
	prorders->get_value(1, anorder.O_CUSTKEY);
	prorders->get_value(4, anorder.O_ORDERDATE, 15);
	time_t orderT = str_to_timet(anorder.O_ORDERDATE);
	if( customer_nation.find(anorder.O_CUSTKEY) != customer_nation.end() &&
	    (orderT >= q5in.o_orderdate && orderT < last_orderdate)) {
	    ordersK_cust.insert(pair<int,int> (anorder.O_ORDERKEY,
					       anorder.O_CUSTKEY));
	}
	W_DO(o_iter->next(_pssm, eof, *prorders));
    }
    
    //supplier
    map<int,int> supp_nation;

    tuple_guard<supplier_man_impl> prsupp(_psupplier_man);

    rep_row_t sreprow(_psupplier_man->ts());
    sreprow.set(_psupplier_desc->maxsize());

    prsupp->_rep = &sreprow;

    guard<table_scan_iter_impl<supplier_t> > s_iter;
    {
	table_scan_iter_impl<supplier_t>* tmp_s_iter;
	W_DO(_psupplier_man->get_iter_for_file_scan(_pssm, tmp_s_iter));
	s_iter = tmp_s_iter;
    }

    tpch_supplier_tuple asupplier;
    
    W_DO(s_iter->next(_pssm, eof, *prsupp));

    while(!eof){
	prsupp->get_value(0, asupplier.S_SUPPKEY);
	prsupp->get_value(3, asupplier.S_NATIONKEY);
	if(nation_rev.find(asupplier.S_NATIONKEY) != nation_rev.end()){
	    supp_nation.insert(pair<int,int>
			       (asupplier.S_SUPPKEY, asupplier.S_NATIONKEY));
	}
	W_DO(s_iter->next(_pssm, eof, *prsupp));
    }

    // table scan lineitem 
    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t alreprow(_plineitem_man->ts());
    alreprow.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &alreprow;

    guard< table_scan_iter_impl<lineitem_t> > l_iter;
    {
	table_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->get_iter_for_file_scan(_pssm, tmp_l_iter));
	l_iter = tmp_l_iter;
    }
            
    tpch_lineitem_tuple aline;
    
    W_DO(l_iter->next(_pssm, eof, *prlineitem));
	
    while (!eof) {
	prlineitem->get_value(0, aline.L_ORDERKEY);
	prlineitem->get_value(2, aline.L_SUPPKEY);
	prlineitem->get_value(5, aline.L_EXTENDEDPRICE);
	prlineitem->get_value(6, aline.L_DISCOUNT);
	double price = ( 1 - aline.L_DISCOUNT)*aline.L_EXTENDEDPRICE;
	map<int,int>::iterator suppiter = supp_nation.find(aline.L_SUPPKEY);
	map<int,int>::iterator orderiter = ordersK_cust.find(aline.L_ORDERKEY);
	if(orderiter != ordersK_cust.end() && suppiter != supp_nation.end()){
	    map<int,int>::iterator custiter =
		customer_nation.find(orderiter->second);
	    if(custiter != customer_nation.end()
	       && custiter->second == suppiter->second){
		int nationKey = custiter->second;
		double c =nation_rev.find(nationKey)->second;
		c += price;
		nation_rev[nationKey] = c;
	    }
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }

    return RCOK;
}



/******************************************************************** 
 *
 * TPC-H Q6
 *
 ********************************************************************/

// l_extendedprice l_discount l_shipdate l_quantity

w_rc_t ShoreTPCHEnv::xct_q6(const int /* xct_id */, q6_input_t& pq6in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // q6 trx touches 1 tables: lineitem
    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t areprow(_plineitem_man->ts());
    areprow.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &areprow;
    
    //       select
    //       sum(l_extendedprice*l_discount) as revenue
    //       from
    //       lineitem
    //       where
    //       l_shipdate >= date '[DATE]'
    //       and l_shipdate < date '[DATE]' + interval '1' year
    //       and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
    //       and l_quantity < [QUANTITY]
	
    // index scan on shipdate
    rep_row_t lowrep(_plineitem_man->ts());
    rep_row_t highrep(_plineitem_man->ts());

    lowrep.set(_plineitem_desc->maxsize());
    highrep.set(_plineitem_desc->maxsize());

    struct tm date;    
    if (gmtime_r(&(pq6in.l_shipdate), &date) == NULL) {
	return RCOK;
    }
    date.tm_year ++;
    time_t last_shipdate = mktime(&date);
    
    guard< index_scan_iter_impl<lineitem_t> > l_iter;
    {
	index_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->l_get_range_iter_by_index(_pssm, tmp_l_iter,
						       prlineitem, lowrep,
						       highrep, pq6in.l_shipdate,
						       last_shipdate));
	l_iter = tmp_l_iter;
    }

    bool eof;
    tpch_lineitem_tuple aline;
    double q6_result = 0;

    W_DO(l_iter->next(_pssm, eof, *prlineitem));

    while (!eof) {
	prlineitem->get_value(4, aline.L_QUANTITY);
	prlineitem->get_value(5, aline.L_EXTENDEDPRICE);
	prlineitem->get_value(6, aline.L_DISCOUNT);
	prlineitem->get_value(10, aline.L_SHIPDATE, 15);	
	if ((aline.L_DISCOUNT > pq6in.l_discount - 0.01) &&
	    (aline.L_DISCOUNT < pq6in.l_discount + 0.01) &&
	    (aline.L_QUANTITY < pq6in.l_quantity)) {
	    q6_result += (aline.L_EXTENDEDPRICE * aline.L_DISCOUNT);
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }

    return RCOK;

}; // EOF: Q6 



/******************************************************************** 
 *
 * TPC-H Q7
 *
 ********************************************************************/

struct q7_group_by_key_t{

    int supp_nation;
    int cust_nation;
    int l_year;

    q7_group_by_key_t(int s_n, int c_n, int y)
    {
	supp_nation = s_n;
	cust_nation = c_n;
	l_year = y;

    };

    q7_group_by_key_t(const q7_group_by_key_t& rhs)
    {
	supp_nation = rhs.supp_nation;
	cust_nation = rhs.cust_nation;
	l_year = rhs.l_year;
    };

    q7_group_by_key_t& operator=(const q7_group_by_key_t& rhs)
    {
	supp_nation = rhs.supp_nation;
	cust_nation = rhs.cust_nation;
	l_year = rhs.l_year;

        return (*this);
    };
};


class q7_group_by_comp {
public:
    bool operator() (
                     const q7_group_by_key_t& lhs, 
                     const q7_group_by_key_t& rhs) const
    {
        return (lhs.supp_nation < rhs.supp_nation ||
		(lhs.supp_nation == rhs.supp_nation &&
		 lhs.cust_nation < rhs.cust_nation ) ||
		(lhs.supp_nation == rhs.supp_nation &&
		 lhs.cust_nation == rhs.cust_nation &&
		 rhs.l_year < lhs.l_year) );
    }
};

w_rc_t ShoreTPCHEnv::xct_q7(const int /* xct_id */, q7_input_t&  q7in )
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // table scan customer c_nationkey = n_name1, n_name2
    map<int,int> cust_nationK;

    tuple_guard<customer_man_impl> prcustomer(_pcustomer_man);

    rep_row_t areprow(_pcustomer_man->ts());
    areprow.set(_pcustomer_desc->maxsize());

    prcustomer->_rep = &areprow;

    guard< table_scan_iter_impl<customer_t> > c_iter;
    {
	table_scan_iter_impl<customer_t>* tmp_c_iter;
	W_DO(_pcustomer_man->get_iter_for_file_scan(_pssm, tmp_c_iter));
	c_iter = tmp_c_iter;
    }
    
    bool eof;
    tpch_customer_tuple acust;

    W_DO(c_iter->next(_pssm, eof, *prcustomer));

    while(!eof){
	prcustomer->get_value(0, acust.C_CUSTKEY);
	prcustomer->get_value(3, acust.C_NATIONKEY);
	if(acust.C_NATIONKEY == q7in.n_name1||acust.C_NATIONKEY == q7in.n_name2) {
	    cust_nationK.insert(pair<int,int>(acust.C_CUSTKEY,acust.C_NATIONKEY));
	}
	W_DO(c_iter->next(_pssm, eof, *prcustomer));
    }

    //table scan order o_customerkey in customer_nationK
    map<int,int> orderk_custk;

    tuple_guard<orders_man_impl> prorder(_porders_man);
    
    rep_row_t o_areprow(_porders_man->ts());
    o_areprow.set(_porders_desc->maxsize());

    prorder->_rep = &o_areprow;

    guard< table_scan_iter_impl<orders_t> > o_iter;
    {
	table_scan_iter_impl<orders_t>* tmp_o_iter;
	W_DO(_porders_man->get_iter_for_file_scan(_pssm, tmp_o_iter));
	o_iter = tmp_o_iter;
    }

    tpch_orders_tuple anorder;
    
    W_DO(o_iter->next(_pssm, eof, *prorder));

    while(!eof){
	prorder->get_value(0, anorder.O_ORDERKEY);
	prorder->get_value(1, anorder.O_CUSTKEY);
	if( cust_nationK.find(anorder.O_CUSTKEY) != cust_nationK.end()) {
	    orderk_custk.insert(pair<int,int>(anorder.O_ORDERKEY,
					      anorder.O_CUSTKEY));
	}
	W_DO(o_iter->next(_pssm, eof, *prorder));
    }

    //table scan supplier : s_nationkey = n_name1 s_nationkey = n_name2
    map<int,int> supp_nationk;

    tuple_guard<supplier_man_impl> prsupp(_psupplier_man);

    rep_row_t sreprow(_psupplier_man->ts());
    sreprow.set(_psupplier_desc->maxsize());

    prsupp->_rep = &sreprow;

    guard<table_scan_iter_impl<supplier_t> > s_iter;
    {
	table_scan_iter_impl<supplier_t>* tmp_s_iter;
	W_DO(_psupplier_man->get_iter_for_file_scan(_pssm, tmp_s_iter));
	s_iter = tmp_s_iter;
    }
    
    tpch_supplier_tuple asupplier;
    
    W_DO(s_iter->next(_pssm, eof, *prsupp));

    while(!eof){
	prsupp->get_value(0, asupplier.S_SUPPKEY);
	prsupp->get_value(3, asupplier.S_NATIONKEY);
	if( asupplier.S_NATIONKEY == q7in.n_name1 ||
	    asupplier.S_NATIONKEY == q7in.n_name2) {
	    supp_nationk.insert(pair<int,int>
				(asupplier.S_SUPPKEY, asupplier.S_NATIONKEY));
	}
	W_DO(s_iter->next(_pssm, eof, *prsupp));
    }

    //table scan lineitem
    map<q7_group_by_key_t, double, q7_group_by_comp> vol_shipping;

    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t alreprow(_plineitem_man->ts());
    alreprow.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &alreprow;

    struct tm start_shipdate;
    start_shipdate.tm_year = 95;
    start_shipdate.tm_mon = 0;
    start_shipdate.tm_mday = 1;
    time_t s_ship = mktime(&start_shipdate);
    
    struct tm end_shipdate;
    end_shipdate.tm_year = 96;
    end_shipdate.tm_mon = 11;
    end_shipdate.tm_mday =31;
    time_t e_ship = mktime(&end_shipdate);
    
    guard< table_scan_iter_impl<lineitem_t> > l_iter;
    {
	table_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->get_iter_for_file_scan(_pssm, tmp_l_iter));
	l_iter = tmp_l_iter;
    }

    tpch_lineitem_tuple aline;

    W_DO(l_iter->next(_pssm, eof, *prlineitem));

    while (!eof) {
	prlineitem->get_value(0, aline.L_ORDERKEY);
	prlineitem->get_value(2, aline.L_SUPPKEY);
	prlineitem->get_value(5, aline.L_EXTENDEDPRICE);
	prlineitem->get_value(6, aline.L_DISCOUNT);
	prlineitem->get_value(10, aline.L_SHIPDATE, 15);
	    
	time_t shipdate = str_to_timet(aline.L_SHIPDATE);
	struct tm date = (*localtime(&shipdate));
	double price = aline.L_EXTENDEDPRICE*(1- aline.L_DISCOUNT);

	map<int,int>:: iterator order = orderk_custk.find(aline.L_ORDERKEY);
	map<int,int>:: iterator supp = supp_nationk.find(aline.L_SUPPKEY);
	map<int,int>:: iterator cust;
	
	if(order != orderk_custk.end() && supp != supp_nationk.end() &&
	   date.tm_year <= 96 && date.tm_year >= 95){
	    
	    cust = cust_nationK.find(order->second);

	    if(cust != cust_nationK.end()){
		if(cust->second != supp->second){
		    map<q7_group_by_key_t, double, q7_group_by_comp>::iterator it=
			vol_shipping.find(q7_group_by_key_t(supp->second,
							    cust->second,
							    date.tm_year));
		    if( it != vol_shipping.end()){
			double c = it->second;
			c +=  price;
			vol_shipping.insert(pair<q7_group_by_key_t,
					    double>
					    (q7_group_by_key_t(supp->second,
							       cust->second,
							       date.tm_year), c));
		    } else {
			vol_shipping.insert(pair<q7_group_by_key_t, double>
					    (q7_group_by_key_t(supp->second,
							       cust->second,
							       date.tm_year),
					     price));
		    }
		} 
	    }
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }
    
    return RCOK;
}



/******************************************************************** 
 *
 * TPC-H Q8
 *
 ********************************************************************/

struct q8_inner_table{

    int  s_key;
    int o_year;
    double price;

    q8_inner_table(int s, int o, double p){
	s_key = s;
	o_year = o;
	price = p;
    }
    
};

struct q8_groupe_by_value_t{
    double a; // a/b
    double b;
    
    q8_groupe_by_value_t(double aa, double bb){

	a = aa;
	b = bb;
    }
    
};

w_rc_t ShoreTPCHEnv::xct_q8(const int /* xct_id */, q8_input_t&  q8in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //retrive r_key of q8in.n_name
    int r_key;
    map<int, bool> nation_k;

    tuple_guard<nation_man_impl> prnation(_pnation_man);
   
    rep_row_t areprow(_pnation_man->ts());
    areprow.set(_pnation_desc->maxsize()); 

    prnation->_rep = &areprow;

    W_DO(_pnation_man->n_index_probe(_pssm, prnation, q8in.n_name));

    prnation->get_value(2, r_key);

    // retrive nation_keys which are in r_key
    guard< table_scan_iter_impl<nation_t> > n_iter;
    {
	table_scan_iter_impl<nation_t>* tmp_n_iter;
	W_DO(_pnation_man->get_iter_for_file_scan(_pssm, tmp_n_iter));
	n_iter = tmp_n_iter;
    }
    
    bool eof;
    tpch_nation_tuple anation;
    
    W_DO(n_iter->next(_pssm, eof, *prnation));

    while (!eof) {
	prnation->get_value(2, anation.N_REGIONKEY);
	prnation->get_value(0, anation.N_NATIONKEY);
	if(anation.N_REGIONKEY == r_key) {
	    nation_k.insert(pair<int,bool>(anation.N_NATIONKEY, true));
	}
	W_DO(n_iter->next(_pssm, eof, *prnation));
    }

    //file scan part
    map<int, bool> p_keys;
    char p_type[26];    
    type_to_str(q8in.p_type, p_type);

    tuple_guard<part_man_impl> prpart(_ppart_man);

    rep_row_t preprow(_ppart_man->ts());
    preprow.set(_ppart_desc->maxsize());

    prpart->_rep = &preprow;

    guard<table_scan_iter_impl<part_t> > p_iter;
    {
	table_scan_iter_impl<part_t>* tmp_p_iter;
	W_DO(_ppart_man->get_iter_for_file_scan(_pssm, tmp_p_iter));
	p_iter = tmp_p_iter;
    }
	    
    tpch_part_tuple apart;

    W_DO(p_iter->next(_pssm, eof, *prpart));

    while(!eof){
	prpart->get_value(4, apart.P_TYPE, 25);
	prpart->get_value(0, apart.P_PARTKEY);
	if( strcmp(apart.P_TYPE, p_type ) == 0  ){
	    p_keys.insert(pair<int,bool> (apart.P_PARTKEY, true));
	}
	W_DO(p_iter->next(_pssm, eof, *prpart));
    }

    //table scan customer
    map<int, bool> cust_k;

    tuple_guard<customer_man_impl> prcustomer(_pcustomer_man);

    rep_row_t acreprow(_pcustomer_man->ts());
    acreprow.set(_pcustomer_desc->maxsize());

    prcustomer->_rep = &acreprow;

    guard< table_scan_iter_impl<customer_t> > c_iter;
    {
	table_scan_iter_impl<customer_t>* tmp_c_iter;
	W_DO(_pcustomer_man->get_iter_for_file_scan(_pssm, tmp_c_iter));
	c_iter = tmp_c_iter;
    }

    tpch_customer_tuple acust;
    
    W_DO(c_iter->next(_pssm, eof, *prcustomer));

    while(!eof){
	prcustomer->get_value(0, acust.C_CUSTKEY);
	prcustomer->get_value(3, acust.C_NATIONKEY);
	if( nation_k.find(acust.C_NATIONKEY) != nation_k.end()){
	    cust_k.insert(pair<int,bool>(acust.C_CUSTKEY,true));
	}
	W_DO(c_iter->next(_pssm, eof, *prcustomer));
    }

    //index scan order
    //we need to touch table orders
    map<int,int> orders_ky; //orders key and date

    tuple_guard<orders_man_impl> prorders(_porders_man);

    rep_row_t aoreprow(_porders_man->ts());
    aoreprow.set(_porders_desc->maxsize());

    prorders->_rep = & aoreprow;

    rep_row_t lowrep(_porders_man->ts());
    rep_row_t highrep(_porders_man->ts());

    lowrep.set(_porders_desc->maxsize());
    highrep.set(_porders_desc->maxsize());

    struct tm date1; date1.tm_year = 95; date1.tm_mon = 0; date1.tm_mday = 1;
    struct tm date2; date2.tm_year = 96; date2.tm_mon = 11; date2.tm_mday = 31;
    time_t t1 = mktime(&date1);
    time_t t2 = mktime(&date2);
    
    guard<index_scan_iter_impl<orders_t> > o_iter;
    {
	index_scan_iter_impl<orders_t>* tmp_o_iter;
	W_DO(_porders_man->o_get_range_iter_by_index(_pssm, tmp_o_iter,	prorders,
						     lowrep, highrep, t1, t2));
	o_iter = tmp_o_iter;
    }
    
    tpch_orders_tuple anorder;
    	
    W_DO(o_iter->next(_pssm, eof, *prorders));

    while(!eof){
	prorders->get_value(0, anorder.O_ORDERKEY);
	prorders->get_value(1, anorder.O_CUSTKEY);
	prorders->get_value(4, anorder.O_ORDERDATE, 15);
	time_t t = str_to_timet(anorder.O_ORDERDATE);
	struct tm date = *(localtime(&t));
	if( cust_k.find(anorder.O_CUSTKEY) != cust_k.end()) {
	    orders_ky.insert(pair<int,int>(anorder.O_ORDERKEY, date.tm_year));
	}
	W_DO(o_iter->next(_pssm, eof, *prorders));
    }

    //supplier
    map<int,int> supp_nation;

    tuple_guard<supplier_man_impl> prsupp(_psupplier_man);

    rep_row_t sreprow(_psupplier_man->ts());
    sreprow.set(_psupplier_desc->maxsize());

    prsupp->_rep = &sreprow;

    guard<table_scan_iter_impl<supplier_t> > s_iter;
    {
	table_scan_iter_impl<supplier_t>* tmp_s_iter;
	W_DO(_psupplier_man->get_iter_for_file_scan(_pssm, tmp_s_iter));
	s_iter = tmp_s_iter;
    }

    tpch_supplier_tuple asupplier;
    
    W_DO(s_iter->next(_pssm, eof, *prsupp));

    while(!eof){
	prsupp->get_value(0, asupplier.S_SUPPKEY);
	prsupp->get_value(3, asupplier.S_NATIONKEY);
	supp_nation.insert(pair<int,int>(asupplier.S_SUPPKEY,
					 asupplier.S_NATIONKEY));
	W_DO(s_iter->next(_pssm, eof, *prsupp));
    }

    // table scan lineitem
    vector <q8_inner_table> all_nation;

    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t alreprow(_plineitem_man->ts());
    alreprow.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &alreprow;

    guard< table_scan_iter_impl<lineitem_t> > l_iter;
    {
	table_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->get_iter_for_file_scan(_pssm, tmp_l_iter));
	l_iter = tmp_l_iter;
    }

    tpch_lineitem_tuple aline;
    
    W_DO(l_iter->next(_pssm, eof, *prlineitem));

    while (!eof) {
	prlineitem->get_value(0, aline.L_ORDERKEY);
	prlineitem->get_value(1, aline.L_PARTKEY);
	prlineitem->get_value(2, aline.L_SUPPKEY);
	prlineitem->get_value(5, aline.L_EXTENDEDPRICE);
	prlineitem->get_value(6, aline.L_DISCOUNT);
	double price = (1-aline.L_DISCOUNT)*aline.L_EXTENDEDPRICE;
	map<int,bool>:: iterator pit = p_keys.find(aline.L_PARTKEY);
	map<int,int>::iterator oit = orders_ky.find(aline.L_ORDERKEY);
	if(pit != p_keys.end() && oit != orders_ky.end()) {
	    all_nation.push_back(q8_inner_table(aline.L_SUPPKEY,
						oit->second, price));
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }

    //compute the final result
    map<int,q8_groupe_by_value_t> mkt_share;
    map<int,q8_groupe_by_value_t>::iterator it;
    
    for(int i = 0; i < all_nation.size(); i++){
	it = mkt_share.find(all_nation[i].o_year);
	double a,b;
	if (it != mkt_share.end()){
	    a = it->second.a;
	    b = it->second.b;
	}else{
	    a = 0;
	    b =0;
	}
	if( supp_nation.find(all_nation[i].s_key)->second == q8in.n_name){
	    a+=all_nation[i].price;
	}
	b+=all_nation[i].price;
	mkt_share.insert(pair<int,q8_groupe_by_value_t>(all_nation[i].o_year,
							q8_groupe_by_value_t(a,
									     b)));
    }
    
    return RCOK;
}



/******************************************************************** 
 *
 * TPC-H Q9
 *
 ********************************************************************/

struct q9_group_by_key_t{

    int nation_k;
    int year;

    q9_group_by_key_t(int n, int y){

	nation_k = n;
	year = y;
    }
};

class q9_group_by_comp {
public:
    bool operator() (
                     const q9_group_by_key_t& lhs, 
                     const q9_group_by_key_t& rhs) const
    {
        return (lhs.nation_k < rhs.nation_k) ||
	    (lhs.nation_k == rhs.nation_k && lhs.year < rhs.year);
    }
};


w_rc_t ShoreTPCHEnv::xct_q9(const int /* xct_id */, q9_input_t& q9in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //table scan part_t : p_name contains '%color%'
    map<int, bool> p_keys;

    tuple_guard<part_man_impl> prpart(_ppart_man);

    rep_row_t preprow(_ppart_man->ts());
    preprow.set(_ppart_desc->maxsize());

    prpart->_rep = &preprow;

    guard<table_scan_iter_impl<part_t> > p_iter;
    {
	table_scan_iter_impl<part_t>* tmp_p_iter;
	W_DO(_ppart_man->get_iter_for_file_scan(_pssm, tmp_p_iter));
	p_iter = tmp_p_iter;
    }
	    
    bool eof;
    tpch_part_tuple apart;
    
    W_DO(p_iter->next(_pssm, eof, *prpart));

    while(!eof){
	    prpart->get_value(0, apart.P_PARTKEY);
	    prpart->get_value(1, apart.P_NAME, 55);
	    char part_name[55];
	    pname_to_str(q9in.p_name, part_name);
	    if(strstr(apart.P_NAME,part_name) != NULL){
		p_keys.insert(pair<int,bool> ( apart.P_PARTKEY, 0));
	    } 
	    W_DO(p_iter->next(_pssm, eof, *prpart));
    }

    //table scan supplier
    map<int,int> suppK_nK;

    tuple_guard<supplier_man_impl> prsupp(_psupplier_man);

    rep_row_t sreprow(_psupplier_man->ts());
    sreprow.set(_psupplier_desc->maxsize());

    prsupp->_rep = &sreprow;

    guard<table_scan_iter_impl<supplier_t> > s_iter;
    {
	table_scan_iter_impl<supplier_t>* tmp_s_iter;
	W_DO(_psupplier_man->get_iter_for_file_scan(_pssm, tmp_s_iter));
	s_iter = tmp_s_iter;
    }

    tpch_supplier_tuple asupplier;
    
    W_DO(s_iter->next(_pssm, eof, *prsupp));

    while(!eof){
	prsupp->get_value(0, asupplier.S_SUPPKEY);
	prsupp->get_value(3, asupplier.S_NATIONKEY);
	suppK_nK.insert(pair<int,int>(asupplier.S_SUPPKEY,asupplier.S_NATIONKEY));
	W_DO(s_iter->next(_pssm, eof, *prsupp));
    }

    //table scan order
    map<int,int> orderK_y;

    tuple_guard<orders_man_impl> prorder(_porders_man);

    rep_row_t o_areprow(_porders_man->ts());
    o_areprow.set(_porders_desc->maxsize());

    prorder->_rep = &o_areprow;

    guard< table_scan_iter_impl<orders_t> > o_iter;
    {
	table_scan_iter_impl<orders_t>* tmp_o_iter;
	W_DO(_porders_man->get_iter_for_file_scan(_pssm, tmp_o_iter));
	o_iter = tmp_o_iter;
    }

    tpch_orders_tuple anorder;
    
    W_DO(o_iter->next(_pssm, eof, *prorder));

    while(!eof){
	prorder->get_value(1, anorder.O_ORDERKEY);
	prorder->get_value(4, anorder.O_ORDERDATE,15);
	time_t t = str_to_timet(anorder.O_ORDERDATE);
	struct tm date = *localtime(&t);
	orderK_y.insert( pair<int,int> (anorder.O_ORDERKEY, date.tm_year));
	W_DO(o_iter->next(_pssm, eof, *prorder));
    }
        
    //table scane lineitem
    map <q9_group_by_key_t, decimal, q9_group_by_comp> profit_m;

    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t areprow(_plineitem_man->ts());
    areprow.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &areprow;

    tuple_guard<partsupp_man_impl> prpartsupp(_ppartsupp_man);

    rep_row_t apsreprow(_ppartsupp_man->ts());
    apsreprow.set(_ppartsupp_desc->maxsize()); 

    prpartsupp->_rep = &apsreprow; 

    guard< table_scan_iter_impl<lineitem_t> > l_iter;
    {
	table_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->get_iter_for_file_scan(_pssm, tmp_l_iter));
	l_iter = tmp_l_iter;
    }
    
    tpch_lineitem_tuple aline;
    tpch_partsupp_tuple apartsupp;
    
    W_DO(l_iter->next(_pssm, eof, *prlineitem));
	
    while (!eof) {
	prlineitem->get_value(0, aline.L_ORDERKEY);
	prlineitem->get_value(1, aline.L_PARTKEY);
	prlineitem->get_value(2, aline.L_SUPPKEY);
	prlineitem->get_value(4, aline.L_QUANTITY);
	prlineitem->get_value(5, aline.L_EXTENDEDPRICE);
	prlineitem->get_value(6, aline.L_DISCOUNT);
	if((_ppartsupp_man->ps_index_probe(_pssm, prpartsupp, aline.L_PARTKEY,
					   aline.L_SUPPKEY)).is_error()) {
	    W_DO(l_iter->next(_pssm, eof, *prlineitem));
	    continue;
	}
	prpartsupp->get_value(3, apartsupp.PS_SUPPLYCOST);
	
	decimal price = aline.L_EXTENDEDPRICE * (1 - aline.L_DISCOUNT) -
	    apartsupp.PS_SUPPLYCOST* aline.L_QUANTITY;
	
	if( p_keys.find(aline.L_PARTKEY) != p_keys.end() ){		
	    map<int,int>::iterator oit = orderK_y.find( aline.L_ORDERKEY);
	    map<int,int>::iterator sit = suppK_nK.find( aline.L_SUPPKEY);
	    
	    if(oit == orderK_y.end() || sit == suppK_nK.end()){
		W_DO(l_iter->next(_pssm, eof, *prlineitem));
		continue;
	    }
	    
	    int y = oit->second; 
	    int nation_k = sit->second; 	    
	    map <q9_group_by_key_t, decimal, q9_group_by_comp>::iterator it =
		profit_m.find(q9_group_by_key_t(nation_k, y));
	    decimal c = price;
	    if( it != profit_m.end()) {
		c += it->second;
	    }
	    profit_m.insert(pair<q9_group_by_key_t, decimal>
			    (q9_group_by_key_t(nation_k, y) ,c) );
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }
       
    return RCOK;
}



/******************************************************************** 
 *
 * TPC-H Q10
 *
 ********************************************************************/

class q10_group_by_key_t{

public:
    
    int c_custkey;
    char c_name[25];
    decimal c_acctbal;
    char phone[15];
    int n_key;
    char c_address[40];
    char c_comment[117];

    q10_group_by_key_t
    (int k, char* name, decimal bal, char* phone, int n, char* add, char* cmt){
	c_custkey = k;
	strcpy(c_name, name);
	c_acctbal = bal;
	strcpy(phone, phone);
	n_key = n;
	strcpy(c_address, add);
	strcpy(c_comment, cmt);
	
    }
};

class q10_group_by_key_comp{

public:
    bool operator() (
                     const q10_group_by_key_t& lhs, 
                     const q10_group_by_key_t& rhs) const
    {
        return lhs.c_custkey < rhs.c_custkey;
    }
};



w_rc_t ShoreTPCHEnv::xct_q10(const int /* xct_id */, q10_input_t&  q10in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    struct tm date2 = *gmtime(&q10in.o_orderdate);
    date2.tm_mon += 3;
    if( date2.tm_mon > 11){	
	date2.tm_mon -= 12;
	date2.tm_year++;
    }

    time_t t2 = mktime(&date2);
    time_t t1 = q10in.o_orderdate;
    
    // table scan order
    map<int, vector<int>* > cust_ordersK;
    map<int, double> orders_price;

    tuple_guard<orders_man_impl> prorder(_porders_man);
    
    rep_row_t o_areprow(_porders_man->ts());
    o_areprow.set(_porders_desc->maxsize());
    
    prorder->_rep = &o_areprow;

    guard< table_scan_iter_impl<orders_t> > o_iter;
    {
	table_scan_iter_impl<orders_t>* tmp_o_iter;
	W_DO(_porders_man->get_iter_for_file_scan(_pssm, tmp_o_iter));
	o_iter = tmp_o_iter;
    }

    bool eof;
    tpch_orders_tuple anorder;

    W_DO(o_iter->next(_pssm, eof, *prorder));

    while(!eof){
	prorder->get_value(0, anorder.O_ORDERKEY);
	prorder->get_value(1, anorder.O_CUSTKEY);
	prorder->get_value(4, anorder.O_ORDERDATE, 15);
	time_t orderdate = str_to_timet(anorder.O_ORDERDATE);
	if(orderdate >= t1 && orderdate < t2){
	    orders_price.insert(pair<int,double>(anorder.O_ORDERKEY, 0.0));
	    map<int, vector<int>* >::iterator it =
		cust_ordersK.find(anorder.O_CUSTKEY);
	    if( it != cust_ordersK.end() ){
		it->second->push_back(anorder.O_ORDERKEY);
	    } else {
		vector<int> * v = new vector<int>();
		v->push_back(anorder.O_ORDERKEY);
		cust_ordersK.insert(pair<int, vector<int>*>(anorder.O_CUSTKEY,v));
	    }
	}
	W_DO(o_iter->next(_pssm, eof, *prorder));
    }

    //table scan lineitem: flag ='R' & orderkey in orders_price
    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t areprow(_plineitem_man->ts());
    areprow.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &areprow;

    guard< table_scan_iter_impl<lineitem_t> > l_iter;
    {
	table_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->get_iter_for_file_scan(_pssm, tmp_l_iter));
	l_iter = tmp_l_iter;
    }

    tpch_lineitem_tuple aline;

    W_DO(l_iter->next(_pssm, eof, *prlineitem));

    while (!eof) {
	prlineitem->get_value(0, aline.L_ORDERKEY);
	prlineitem->get_value(8, aline.L_RETURNFLAG);
	prlineitem->get_value(5, aline.L_EXTENDEDPRICE);
	prlineitem->get_value(6, aline.L_DISCOUNT);
	double price =  (1 - aline.L_DISCOUNT) * aline.L_EXTENDEDPRICE;
	map<int,double>::iterator it = orders_price.find(aline.L_ORDERKEY);
	if(aline.L_RETURNFLAG == 'R' && it != orders_price.end() ){
	    double c = it->second;
	    c += price;
	    orders_price[ aline.L_ORDERKEY ] = c;
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }

    //retrive customer info and calc the scalar
    tuple_guard<customer_man_impl> prcustomer(_pcustomer_man);

    rep_row_t acreprow(_pcustomer_man->ts());
    acreprow.set(_pcustomer_desc->maxsize());

    prcustomer->_rep = &acreprow;

    tpch_customer_tuple acust;
    map<q10_group_by_key_t, double, q10_group_by_key_comp> customer_rev;
    map<int, vector<int>* > :: iterator cit = cust_ordersK.begin();

    while( cit != cust_ordersK.end()){
	vector<int>::iterator oit = cit->second->begin();
	double rev = 0;
	while(oit != cit->second->end()){
	    rev += orders_price.find(*oit)->second;
	    oit++;
	}
	if((_pcustomer_man->c_index_probe(_pssm, prcustomer,
					  cit->first)).is_error()) {
	    cit++;
	    continue;
	}
	prcustomer->get_value(1, acust.C_NAME, 25);
	prcustomer->get_value(2, acust.C_ADDRESS, 40);
	prcustomer->get_value(3, acust.C_NATIONKEY);
	prcustomer->get_value(4, acust.C_PHONE, 15);
	prcustomer->get_value(5, acust.C_ACCTBAL);
	prcustomer->get_value(2, acust.C_COMMENT, 117);
	customer_rev[q10_group_by_key_t(cit->first, acust.C_NAME, acust.C_ACCTBAL,
					acust.C_PHONE, acust.C_NATIONKEY,
					acust.C_ADDRESS, acust.C_COMMENT)] = rev;
	cit++;
    }

    return RCOK;
}



/******************************************************************** 
 *
 * TPC-H Q11
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_q11(const int /* xct_id */, q11_input_t& q11in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //table scan supplier
    map<int,bool> suppK;

    tuple_guard<supplier_man_impl> prsupp(_psupplier_man);

    rep_row_t sreprow(_psupplier_man->ts());
    sreprow.set(_psupplier_desc->maxsize());

    prsupp->_rep = &sreprow;

    guard<table_scan_iter_impl<supplier_t> > s_iter;
    {
	table_scan_iter_impl<supplier_t>* tmp_s_iter;
	W_DO(_psupplier_man->get_iter_for_file_scan(_pssm, tmp_s_iter));
	s_iter = tmp_s_iter;
    }
    
    bool eof;
    tpch_supplier_tuple asupplier;
    
    W_DO(s_iter->next(_pssm, eof, *prsupp));

    while(!eof){
	prsupp->get_value(0, asupplier.S_SUPPKEY);
	prsupp->get_value(3, asupplier.S_NATIONKEY);
	if( asupplier.S_NATIONKEY == q11in.n_name ) {
	    suppK.insert(pair<int,bool>(asupplier.S_SUPPKEY,true));
	}
	W_DO(s_iter->next(_pssm, eof, *prsupp));
    }
    
    //table scan partsupp
    map<int, decimal> partK_val;
    decimal totalval = 0;

    tuple_guard<partsupp_man_impl> prpartsupp(_ppartsupp_man);

    rep_row_t psreprow(_ppartsupp_man->ts());
    psreprow.set(_ppartsupp_desc->maxsize());

    prpartsupp->_rep = &psreprow;

    guard<table_scan_iter_impl<partsupp_t> > ps_iter;
    {
	table_scan_iter_impl<partsupp_t>* tmp_ps_iter;
	W_DO(_ppartsupp_man->get_iter_for_file_scan(_pssm, tmp_ps_iter));
	ps_iter = tmp_ps_iter;
    }

    tpch_partsupp_tuple apartsupp;

    W_DO(ps_iter->next(_pssm, eof, *prpartsupp));

    while(!eof){
	prpartsupp->get_value(0, apartsupp.PS_PARTKEY);
	prpartsupp->get_value(1, apartsupp.PS_SUPPKEY);
	prpartsupp->get_value(2, apartsupp.PS_AVAILQTY);
	prpartsupp->get_value(3, apartsupp.PS_SUPPLYCOST);
	if( suppK.find(apartsupp.PS_SUPPKEY) != suppK.end() ){
	    map<int,decimal>::iterator pit =partK_val.find(apartsupp.PS_PARTKEY);
	    decimal c = apartsupp.PS_AVAILQTY * apartsupp.PS_SUPPLYCOST;
	    totalval += c;
	    if( pit != partK_val.end()){
		c += pit->second;
	    }
	    partK_val[apartsupp.PS_PARTKEY] = c; 
	}
	W_DO(ps_iter->next(_pssm, eof, *prpartsupp));
    }

    return RCOK;
}



/******************************************************************** 
 *
 * TPC-H Q12
 *
 ********************************************************************/

//  * Query 12
//  * 
//  * select l_shipmode, sum(case
//  * 	when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH'
//  * 	then 1
//  * 	else 0 end) as high_line_count, 
//  *    sum(case
//  *      when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH'
//  *      then 1
//  *      else 0 end) as low_line_count
//  * from tpcd.orders, tpcd.lineitem
//  * where o_orderkey = l_orderkey and
//  *       (l_shipmode = "SHIP" or l_shipmode = "RAIL") and
//  *       l_commitdate < l_receiptdate and
//  *       l_shipdate < l_commitdate and
//  *       l_receiptdate >= "1994-01-01" and
//  *       l_receiptdate < "1995-01-01"
//  * group by l_shipmode
//  * order by l_shipmode




w_rc_t ShoreTPCHEnv::xct_q12(const int /* xct_id */, q12_input_t& q12in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //phase one: indexscan lineitem
   vector<pair<int, int> > orderK_shipmode;
   map<int, pair<int,int> > shpmd_HLC_LLC; 
   shpmd_HLC_LLC.insert(pair<int, pair<int,int> >
			(q12in.l_shipmode1, pair<int,int>(0,0)));
   shpmd_HLC_LLC.insert(pair<int, pair<int,int> >
			(q12in.l_shipmode2, pair<int,int>(0,0)));

   tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

   rep_row_t areprow(_plineitem_man->ts());
   areprow.set(_plineitem_desc->maxsize());
   
   prlineitem->_rep = &areprow;
   
   rep_row_t lowrep(_plineitem_man->ts());
   rep_row_t highrep(_plineitem_man->ts());
   
   lowrep.set(_plineitem_desc->maxsize());
   highrep.set(_plineitem_desc->maxsize());
    
   struct tm date;
   if(gmtime_r(&(q12in.l_receiptdate), &date) == NULL){
       return RCOK;
   }
   date.tm_year++;
   time_t last_receiptdate = mktime(&date);
   
   guard <index_scan_iter_impl<lineitem_t> >l_iter;
   {
       index_scan_iter_impl<lineitem_t>* tmp_l_iter;
       W_DO(_plineitem_man->
	    l_get_range_iter_by_receiptdate_index(_pssm, tmp_l_iter, prlineitem,
						  lowrep, highrep,
						  q12in.l_receiptdate,
						  last_receiptdate));
       l_iter = tmp_l_iter;
   }

   bool eof;
   tpch_lineitem_tuple aline;
   
   W_DO(l_iter->next(_pssm, eof, *prlineitem));

   while(!eof){
       prlineitem->get_value(0, aline.L_ORDERKEY);
       prlineitem->get_value(10, aline.L_SHIPDATE, 15);
       prlineitem->get_value(11, aline.L_COMMITDATE, 15);
       prlineitem->get_value(12, aline.L_RECEIPTDATE, 15);
       prlineitem->get_value(14, aline.L_SHIPMODE, 10);
       time_t shipdate = str_to_timet(aline.L_SHIPDATE);
       time_t commitdate = str_to_timet(aline.L_COMMITDATE);	
       time_t receiptdate = str_to_timet(aline.L_RECEIPTDATE);
       int shipmode = str_to_shipmode(aline.L_SHIPMODE);
       if(shipmode == q12in.l_shipmode1 || shipmode == q12in.l_shipmode2) {
	   if(commitdate < receiptdate && shipdate < commitdate) {
	       orderK_shipmode.push_back(pair<int,int>(aline.L_ORDERKEY,
						       shipmode));
	   }
       }
       W_DO(l_iter->next(_pssm, eof, *prlineitem));
   }

   //phase2 joining order-lineitem
   tuple_guard<orders_man_impl> prorder(_porders_man);
   
   rep_row_t aoreprow(_porders_man->ts());
   aoreprow.set(_plineitem_desc->maxsize());
   
   prorder->_rep = &aoreprow;

   rep_row_t low_rep(_porders_man->ts());
   rep_row_t high_rep(_porders_man->ts());
   
   low_rep.set(_porders_desc->maxsize());
   high_rep.set(_porders_desc->maxsize());
   for(vector<pair<int,int> >::iterator iter = orderK_shipmode.begin();
       iter != orderK_shipmode.end();
       iter++){
	//index_scan_order
       int orderKey = iter->first;
       guard<index_scan_iter_impl<orders_t> > o_iter;
       {
	   index_scan_iter_impl<orders_t>* tmp_o_iter;
	   W_DO(_porders_man->o_get_iter_by_index(_pssm, tmp_o_iter, prorder,
						  low_rep, high_rep, orderKey));
	   o_iter = tmp_o_iter;
       }

       bool eof;

       W_DO(o_iter->next(_pssm, eof, *prorder));

       char priority[15];
       prorder->get_value(5, priority, 15);
       
       int hlc = shpmd_HLC_LLC[iter->second].first;
       int llc = shpmd_HLC_LLC[iter->second].second;
       
       if(strcmp(priority,"2-HIGH") == 0 or strcmp(priority,"1-URGENT") ==0){
	   shpmd_HLC_LLC[iter->second] = pair<int,int>( hlc+1, llc);
       } else {
	   shpmd_HLC_LLC[iter->second] = pair<int,int>( hlc, llc+1);
       }
   }

   return RCOK;
}// EOF: Q12



/******************************************************************** 
 *
 * TPC-H Q13
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_q13(const int /* xct_id */, q13_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    /*  select
	c_count, count(*) as custdist
	from (
	select
	      c_custkey,
	      count(o_orderkey)
	      from
	      customer left outer join orders on
	      c_custkey = o_custkey
	      and o_comment not like .%[WORD1]%[WORD2]%.
	      group by
	      c_custkey
	      )as c_orders (c_custkey, c_count)
	group by
	c_count
	order by
	custdist desc,
	c_count desc;*/
    
    //phase 1: list of c_custkey
    map<int,int> c_orders;

    tuple_guard<customer_man_impl> prcustomer(_pcustomer_man);

    rep_row_t areprow(_pcustomer_man->ts());
    areprow.set(_pcustomer_desc->maxsize());

    prcustomer->_rep = &areprow;

    //table scan customer
    guard< table_scan_iter_impl<customer_t> > c_iter;
    {
	table_scan_iter_impl<customer_t>* tmp_c_iter;
	W_DO(_pcustomer_man->get_iter_for_file_scan(_pssm, tmp_c_iter));
	c_iter = tmp_c_iter;
    }

    bool eof;
    tpch_customer_tuple acust;

    W_DO(c_iter->next(_pssm, eof, *prcustomer));

    while(!eof){
	prcustomer->get_value(0, acust.C_CUSTKEY);
	c_orders[ acust.C_CUSTKEY ] = 0;
	W_DO(c_iter->next(_pssm, eof, *prcustomer));
    }
    
    //phase 2: count the number of order each customer have made
    tuple_guard<orders_man_impl> prorder(_porders_man);
    
    rep_row_t o_areprow(_porders_man->ts());
    o_areprow.set(_porders_desc->maxsize());

    prorder->_rep = &o_areprow;

    guard< table_scan_iter_impl<orders_t> > o_iter;
    {
	table_scan_iter_impl<orders_t>* tmp_o_iter;
	W_DO(_porders_man->get_iter_for_file_scan(_pssm, tmp_o_iter));
	o_iter = tmp_o_iter;
    }

    tpch_orders_tuple aorder;

    W_DO(o_iter->next(_pssm, eof, *prorder));

    while(!eof){
	prorder->get_value(1, aorder.O_CUSTKEY);
	int n = c_orders[ aorder.O_CUSTKEY ];
	n++;
	c_orders[ aorder.O_CUSTKEY ] = n;
	W_DO(o_iter->next(_pssm, eof, *prorder));
    }
    
    //phase 3: compute scalar
    map<int, int> o_count_cust;

    for(map<int,int>::iterator iter = c_orders.begin();
	iter != c_orders.end();
	iter++){
	map<int,int>::iterator tmp = o_count_cust.find(iter->second);
	if(tmp != o_count_cust.end()){
	    int c = tmp->second;
	    c++;
	    o_count_cust[iter->second] = c;
	} else {
	    o_count_cust[iter->second] = 1;
	}
    }

    return RCOK;
}// EOF: Q13



/******************************************************************** 
 *
 * TPC-H Q14
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_q14(const int /* xct_id */, q14_input_t& q14in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    map<int, vector<float>* > pKey_prices;
    double totalrevenue = 0;    

    //phase 1: index seek: lineitem
    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t areprow(_plineitem_man->ts());
    areprow.set(_plineitem_desc->maxsize());

    prlineitem->_rep = &areprow;

    rep_row_t lowrep(_plineitem_man->ts());
    rep_row_t highrep(_plineitem_man->ts());

    lowrep.set(_plineitem_desc->maxsize());
    highrep.set(_plineitem_desc->maxsize());
    
    struct tm date;
    if(gmtime_r(&(q14in.l_shipdate), &date) == NULL){
	return RCOK;
    }
    date.tm_mon+=1;
    if( date.tm_mon > 11){
	date.tm_mon -= 12;
	date.tm_year ++;
    }	
    time_t last_shipdate = mktime(&date);

    guard<index_scan_iter_impl<lineitem_t> > l_iter;
    {
	index_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->l_get_range_iter_by_index(_pssm, tmp_l_iter,
						       prlineitem, lowrep,
						       highrep, q14in.l_shipdate,
						       last_shipdate));
	l_iter = tmp_l_iter;
    }

    bool eof;
    tpch_lineitem_tuple aline;
    
    W_DO(l_iter->next(_pssm, eof, *prlineitem));

    while(!eof){
	prlineitem->get_value(1, aline.L_PARTKEY);
	prlineitem->get_value(5, aline.L_EXTENDEDPRICE);
	prlineitem->get_value(6, aline.L_DISCOUNT);
	float theprice = aline.L_EXTENDEDPRICE *(1 - aline.L_DISCOUNT);
	map<int, vector<float>*>::iterator pvector =
	    pKey_prices.find(aline.L_PARTKEY);
	if( pvector != pKey_prices.end() ){
	    pvector->second->push_back( theprice );
	} else {
	    vector<float>* v = new vector<float>();
	    v->push_back( theprice );
	    pKey_prices.insert(pair<int, vector<float>* > (aline.L_PARTKEY, v));
	}
	totalrevenue += theprice;
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }
	
    //phase 2 :joining part and lineitem tables
    double promorevenue = 0;

    //table scan Part
    tuple_guard<part_man_impl> prpart(_ppart_man);

    rep_row_t preprow(_ppart_man->ts());
    preprow.set(_ppart_desc->maxsize());
    
    prpart->_rep = &preprow;

    guard<table_scan_iter_impl<part_t> > p_iter;
    {
	table_scan_iter_impl<part_t>* tmp_p_iter;
	W_DO(_ppart_man->get_iter_for_file_scan(_pssm, tmp_p_iter));
	p_iter = tmp_p_iter;
    }
    
    tpch_part_tuple apart;
    
    W_DO(p_iter->next(_pssm, eof, *prpart));

    while(!eof){
	prpart->get_value(4, apart.P_TYPE, 25);
	prpart->get_value(0, apart.P_PARTKEY);
	map<int,vector<float>* >::iterator temp=pKey_prices.find(apart.P_PARTKEY);
	if( strstr(apart.P_TYPE, "PROMO") != NULL && temp != pKey_prices.end() ){
	    for(int i = 0; i < temp->second->size(); i++ ){
		promorevenue += ( *( temp->second ) )[i];
	    }
	}
	W_DO(p_iter->next(_pssm, eof, *prpart));
    }
    
    return RCOK;
}// EOF: Q14



/******************************************************************** 
 *
 * TPC-H Q15
 *
 ********************************************************************/

struct suppliercmp {
    bool operator()( const tpch_supplier_tuple& s1, const tpch_supplier_tuple& s2 ) const {
	return s1.S_SUPPKEY < s2.S_SUPPKEY;
    }
};


w_rc_t ShoreTPCHEnv::xct_q15(const int /* xct_id */, q15_input_t& q15in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    map<int,float> stream_id;

    //phase 1 :create the view
    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t areprow(_plineitem_man->ts());
    areprow.set(_plineitem_desc->maxsize());

    prlineitem->_rep = &areprow;

    rep_row_t lowrep(_plineitem_man->ts());
    rep_row_t highrep(_plineitem_man->ts());

    lowrep.set(_plineitem_desc->maxsize());
    highrep.set(_plineitem_desc->maxsize());
    
    struct tm date;
    if(gmtime_r(&(q15in.l_shipdate), &date) == NULL){
	return RCOK;
    }

    date.tm_mon+=3;
    if( date.tm_mon > 11){
	date.tm_mon -= 12;
	date.tm_year ++;
    }
    time_t last_shipdate = mktime(&date);
    
    guard<index_scan_iter_impl<lineitem_t> > l_iter;
    {
	index_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->l_get_range_iter_by_index(_pssm, tmp_l_iter,
						       prlineitem, lowrep,
						       highrep, q15in.l_shipdate,
						       last_shipdate));
	l_iter = tmp_l_iter;
    }

    bool eof;
    tpch_lineitem_tuple aline;
    
    W_DO(l_iter->next(_pssm, eof, *prlineitem));

    while(!eof){
	prlineitem->get_value(2, aline.L_SUPPKEY);
	prlineitem->get_value(5, aline.L_EXTENDEDPRICE);
	prlineitem->get_value(6, aline.L_DISCOUNT);
	float theprice = aline.L_EXTENDEDPRICE *(1 - aline.L_DISCOUNT);
	map<int, float>::iterator tmp =  stream_id.find(aline.L_SUPPKEY);
	if(tmp != stream_id.end()){
	    float s = tmp->second;
	    s += theprice;
	    stream_id[ aline.L_SUPPKEY ] = s;
	}else{	   
	    stream_id[aline.L_SUPPKEY] = theprice;
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }

    float maxrev = stream_id.begin()->second;
    for(map<int,float>::iterator iter = stream_id.begin();
	iter != stream_id.end();
	iter++) {
	if( maxrev < iter->second ) {
	    maxrev = iter->second;
	}
    }
    for(map<int,float>::iterator iter = stream_id.begin();
	iter != stream_id.end();
	iter++) {
	if(iter->second != maxrev) {
	   stream_id.erase(iter);
	}
    }
    
    //phase 2 joining with supply table: indexscan
    map<tpch_supplier_tuple, int, suppliercmp> supprev;

    tuple_guard<supplier_man_impl> prsupp(_psupplier_man);
   
    rep_row_t sreprow(_psupplier_man->ts());
    sreprow.set(_psupplier_desc->maxsize());
    
    prsupp->_rep = &sreprow;
    
    for(map<int,float>::iterator iter = stream_id.begin();
	iter != stream_id.end();
	iter++){
	_psupplier_man->s_index_probe(_pssm, prsupp, iter->first);
	tpch_supplier_tuple asupp;
	prsupp->get_value(1, asupp.S_NAME, 25);
	prsupp->get_value(2, asupp.S_ADDRESS, 40);
	prsupp->get_value(4, asupp.S_PHONE, 15);
	supprev[asupp] = iter->second;
    }
    
    return RCOK;    
}// EOF: Q15



/******************************************************************** 
 *
 * TPC-H Q16
 *
 ********************************************************************/
struct required_type{

public:
    
    int brand;
    int type;
    int size;

    required_type(int b, int t, int s){

	brand = b;
	type = t;
	size = s;
    }

    required_type(){}
};

struct required_type_cmp {
    bool operator()( const required_type& r1, const required_type& r2 ) const {
	return ( (r1.brand < r2.brand) ||
		 (r1.brand == r2.brand && r1.type < r2.type)||
		 (r1.brand == r2.brand && r1.type == r2.type && r1.size < r2.size) );
    }
};


w_rc_t ShoreTPCHEnv::xct_q16(const int /* xct_id */, q16_input_t& q16in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //phase#1 table_scan part
    map<int, required_type> pKeys_type;

    tuple_guard<part_man_impl> prpart(_ppart_man);

    rep_row_t preprow(_ppart_man->ts());
    preprow.set(_ppart_desc->maxsize());

    prpart->_rep = &preprow;

    guard<table_scan_iter_impl<part_t> > p_iter;
    {
	table_scan_iter_impl<part_t>* tmp_p_iter;
	W_DO(_ppart_man->get_iter_for_file_scan(_pssm, tmp_p_iter));
	p_iter = tmp_p_iter;
    }
	    
    bool eof;
    tpch_part_tuple apart;
    
    W_DO(p_iter->next(_pssm, eof, *prpart));

    while(!eof){
	int psize;
	prpart->get_value(0, apart.P_PARTKEY);
	prpart->get_value(3, apart.P_BRAND, 10);
	prpart->get_value(4, apart.P_TYPE, 25);
	prpart->get_value(5, psize);
	int brand = str_to_Brand(apart.P_BRAND);
	char *sylb;
	int type;
	sylb = strtok( apart.P_TYPE," ");
	type = str_to_types1(sylb)*10 + str_to_types2(strtok(NULL," "));
	if( brand != q16in.p_brand &&  type != q16in.p_type){
	    for(int i = 0; i < 8; i++) {
		if( q16in.p_size[i] == psize){
		    pKeys_type[apart.P_PARTKEY]=required_type(brand, psize, type);
		    break;
		}
	    }
	}
	W_DO(p_iter->next(_pssm, eof, *prpart));
    }

    //phase#2 table scan supplier
    map<int,bool> suppkeybl; //supplier's key black list

    tuple_guard<supplier_man_impl> prsupp(_psupplier_man);

    rep_row_t sreprow(_psupplier_man->ts());
    sreprow.set(_psupplier_desc->maxsize());

    prsupp->_rep = &sreprow;

    guard<table_scan_iter_impl<supplier_t> > s_iter;
    {
	table_scan_iter_impl<supplier_t>* tmp_s_iter;
	W_DO(_psupplier_man->get_iter_for_file_scan(_pssm, tmp_s_iter));
	s_iter = tmp_s_iter;
    }
    
    tpch_supplier_tuple asupplier;
    
    W_DO(s_iter->next(_pssm, eof, *prsupp));

    while(!eof){
	prsupp->get_value(0, asupplier.S_SUPPKEY);
	prsupp->get_value(6, asupplier.S_COMMENT, 101);
	char* p1 = strstr(asupplier.S_COMMENT, "Customer");
	char* p2 = strstr(asupplier.S_COMMENT, "Complaints");
	if( p1!= NULL && p2 != NULL){
	    if( p2 - p1 > 0 ) {
		suppkeybl[asupplier.S_SUPPKEY] = true;
	    }
	}
	W_DO(s_iter->next(_pssm, eof, *prsupp));
    }

    //phase#3 table scan ps
    map<required_type, int, required_type_cmp> suppcount;

    tuple_guard<partsupp_man_impl> prpartsupp(_ppartsupp_man);

    rep_row_t psreprow(_ppartsupp_man->ts());
    sreprow.set(_ppartsupp_desc->maxsize());

    prpartsupp->_rep = &psreprow;

    guard<table_scan_iter_impl<partsupp_t> > ps_iter;
    {
	table_scan_iter_impl<partsupp_t>* tmp_ps_iter;
	W_DO(_ppartsupp_man->get_iter_for_file_scan(_pssm, tmp_ps_iter));
	ps_iter = tmp_ps_iter;
    }
    
    tpch_partsupp_tuple apartsupp;
    
    W_DO(ps_iter->next(_pssm, eof, *prpartsupp));

    while(!eof){
	prpartsupp->get_value(0, apartsupp.PS_PARTKEY);
	prpartsupp->get_value(1, apartsupp.PS_SUPPKEY);
	map<int, required_type>::iterator tmpiter;
	if((tmpiter = pKeys_type.find(apartsupp.PS_PARTKEY))!=pKeys_type.end() &&
	   suppkeybl.find(apartsupp.PS_SUPPKEY) == suppkeybl.end()){
	    map<required_type, int, required_type_cmp>::iterator tmpiter2;
	    if((tmpiter2 = suppcount.find(tmpiter->second))!= suppcount.end()){
		int c = tmpiter2->second;
		c++;
		suppcount[tmpiter2->first] = c;
	    } else {
		suppcount[tmpiter2->first] = 1;
	    }		
	}
	W_DO(ps_iter->next(_pssm, eof, *prpartsupp));
    }
    
    return RCOK;
}// EOF: Q16



/******************************************************************** 
 *
 * TPC-H Q17
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_q17(const int /* xct_id */, q17_input_t& q17in )
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //phase#1 table scan part
    map<int, vector<pair<int,int> >* > pKey_lineitems;

    tuple_guard<part_man_impl> prpart(_ppart_man);

    rep_row_t preprow(_ppart_man->ts());
    preprow.set(_ppart_desc->maxsize());

    prpart->_rep = &preprow;

    guard<table_scan_iter_impl<part_t> > p_iter;
    {
	table_scan_iter_impl<part_t>* tmp_p_iter;
	W_DO(_ppart_man->get_iter_for_file_scan(_pssm, tmp_p_iter));
	p_iter = tmp_p_iter;
    }
	    
    bool eof;
    tpch_part_tuple apart;
    
    W_DO(p_iter->next(_pssm, eof, *prpart));

    while(!eof){
	prpart->get_value(0, apart.P_PARTKEY);
	prpart->get_value(3, apart.P_BRAND, 10);
	prpart->get_value(6, apart.P_CONTAINER, 10);
	int brand = str_to_Brand(apart.P_BRAND);
	char *s1 = strtok(apart.P_CONTAINER, " ");
	int container = str_to_containers1(s1)*10+
	    str_to_containers2(strtok(NULL," "));
	if(brand == q17in.p_brand && container == q17in.p_container){
	    vector<pair<int,int> > * v = new vector<pair<int,int> > ();
	    pKey_lineitems[apart.P_PARTKEY] = v;
	}
	W_DO(p_iter->next(_pssm, eof, *prpart));
    }

    //phase#2 table scan lineitem
    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t areprow(_plineitem_man->ts());
    areprow.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &areprow;

    guard< table_scan_iter_impl<lineitem_t> > l_iter;
    {
	table_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->get_iter_for_file_scan(_pssm, tmp_l_iter));
	l_iter = tmp_l_iter;
    }
    
    tpch_lineitem_tuple aline;

    W_DO(l_iter->next(_pssm, eof, *prlineitem));

    while (!eof) {
	prlineitem->get_value(1, aline.L_PARTKEY);
	prlineitem->get_value(5, aline.L_EXTENDEDPRICE);
	prlineitem->get_value(4, aline.L_QUANTITY);
	map<int, vector<pair<int,int> >* >::iterator tmpiter;
	tmpiter = pKey_lineitems.find(aline.L_PARTKEY);
	if(tmpiter != pKey_lineitems.end()){
	    tmpiter->second->push_back(pair<int,int>
				       (aline.L_EXTENDEDPRICE, aline.L_QUANTITY));
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }

    //phase#3 compute the scalar
    double sum = 0;
    for(map<int, vector<pair<int,int> >* >::iterator iter=pKey_lineitems.begin();
	iter != pKey_lineitems.end();
	iter++){
	if(iter->second->size() == 0){
	    delete (iter->second);
	    continue;
	}
	double avg = 0;
	for(int i=0; i< iter->second->size(); i++){
	    avg+= (*(iter->second))[i].second;
	}
	avg /= iter->second->size();
	avg *= .2;
	for(int i=0; i< iter->second->size(); i++){
	    if((*(iter->second))[i].second < avg){
		sum += (*(iter->second))[i].first;
	    }
	}
	delete (iter->second);
    }

    return RCOK;
}// EOF: Q17



/******************************************************************** 
 *
 * TPC-H Q18
 *
 ********************************************************************/
struct Q18_row{
    char c_name [25];
    int c_key;
    time_t o_orderdate;
    decimal o_totalprice;

    Q18_row(){}
    
    Q18_row(char* name, int key, time_t date, decimal price){

	strcpy(c_name, name);
	c_key = key;
	o_orderdate = date;
	o_totalprice = price;
    }
};

w_rc_t ShoreTPCHEnv::xct_q18(const int /* xct_id */, q18_input_t& q18in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // table scan lineitem : largeorders
    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t lreprow(_plineitem_man->ts());
    lreprow.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &lreprow;
    
    map<int, int> order_Squant;
    map<int, Q18_row> result;

    guard< table_scan_iter_impl<lineitem_t> > l_iter;
    {
	table_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->get_iter_for_file_scan(_pssm, tmp_l_iter));
	l_iter = tmp_l_iter;
    }
            
    tpch_lineitem_tuple aline;
    bool eof;

    W_DO(l_iter->next(_pssm, eof, *prlineitem));

    while (!eof) {
	prlineitem->get_value(0, aline.L_ORDERKEY);
	prlineitem->get_value(4, aline.L_QUANTITY);
	map<int,int>::iterator iter = order_Squant.find( aline.L_ORDERKEY);
	if( iter != order_Squant.end()){
	    int c = iter->second;
	    c+= aline.L_QUANTITY;
	    order_Squant[aline.L_ORDERKEY] = c;
	} else {
	    order_Squant[aline.L_ORDERKEY] = aline.L_QUANTITY;
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }

    //
    tuple_guard<orders_man_impl> prorders(_porders_man);
    tuple_guard<customer_man_impl> prcustomer(_pcustomer_man);

    rep_row_t oreprow(_pcustomer_man->ts());
    oreprow.set(_pcustomer_desc->maxsize());
    
    prorders->_rep = &oreprow;
    prcustomer->_rep = &oreprow;

    rep_row_t lowrep(_pcustomer_man->ts());
    rep_row_t highrep(_pcustomer_man->ts());
    
    lowrep.set(_pcustomer_desc->maxsize());
    highrep.set(_pcustomer_desc->maxsize());

    for(map<int,int>::iterator it = order_Squant.begin();
	it != order_Squant.end();
	it++){
	// index scan order
	tpch_orders_tuple anorder;
	if( it->second > q18in.l_quantity){
	    guard<index_scan_iter_impl<orders_t> > o_iter;
	    {
		index_scan_iter_impl<orders_t>* tmp_o_iter;
		W_DO(_porders_man->o_get_iter_by_index(_pssm, tmp_o_iter,
						       prorders, lowrep, highrep,
						       it->first));
		o_iter = tmp_o_iter;
	    }
	    
	    bool eof;
	    
	    W_DO(o_iter->next(_pssm, eof, *prorders));
	    prorders->get_value(1, anorder.O_CUSTKEY);
	    prorders->get_value(3, anorder.O_TOTALPRICE);
	    prorders->get_value(4, anorder.O_ORDERDATE, 15);
	}
	
	//index proble customer
	time_t the_orderdate = str_to_timet(anorder.O_ORDERDATE);     
	tpch_customer_tuple acustomer;
	_pcustomer_man->c_index_probe(_pssm, prcustomer, anorder.O_CUSTKEY);
	prcustomer->get_value(1, acustomer.C_NAME, 25);
	_pcustomer_man->give_tuple(prcustomer);
	result.insert(pair<int,Q18_row>(it->first,Q18_row(acustomer.C_NAME,
							  anorder.O_CUSTKEY,
							  the_orderdate,
							  anorder.O_TOTALPRICE)));
    }

    return RCOK;
}// EOF: Q18



/******************************************************************** 
 *
 * TPC-H Q19
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_q19(const int /* xct_id */, q19_input_t& q19in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //phase#1: table scan Part
    map<int, int> partkey_brand;
    double revenue = 0;

    tuple_guard<part_man_impl> prpart(_ppart_man);

    rep_row_t preprow(_ppart_man->ts());
    preprow.set(_ppart_desc->maxsize());

    prpart->_rep = &preprow;

    guard<table_scan_iter_impl<part_t> > p_iter;
    {
	table_scan_iter_impl<part_t>* tmp_p_iter;
	W_DO(_ppart_man->get_iter_for_file_scan(_pssm, tmp_p_iter));
	p_iter = tmp_p_iter;
    }
    
    bool eof;
    tpch_part_tuple apart;
    
    W_DO(p_iter->next(_pssm, eof, *prpart));

    while(!eof){
	int size;
	prpart->get_value(0, apart.P_PARTKEY);
	prpart->get_value(3, apart.P_BRAND, 10);
	prpart->get_value(5, size/*apart.P_SIZE*/);
	prpart->get_value(6, apart.P_CONTAINER, 10);
	int brand = str_to_Brand( apart.P_BRAND );
	if( brand == q19in.p_brand[0] &&
	    ( strcmp( apart.P_CONTAINER, "SM CASE") == 0 ||
	      strcmp( apart.P_CONTAINER, "SM BOX") == 0 ||
	      strcmp( apart.P_CONTAINER, "SM PACK") == 0 ||
	      strcmp( apart.P_CONTAINER, "SM PKG") == 0 ) &&
	    size > 1 && size < 5 ){
	    partkey_brand.insert( pair<int,int>(apart.P_PARTKEY, 1));
	} else if( brand == q19in.p_brand[1] &&
		   ( strcmp( apart.P_CONTAINER, "MED BAG") ||
		     strcmp( apart.P_CONTAINER, "MED BOX") ||
		     strcmp( apart.P_CONTAINER, "MED PKG") ||
		     strcmp( apart.P_CONTAINER, "MED PACK") ) &&
		   size > 1 && size < 10){
	    partkey_brand.insert( pair<int,int> (apart.P_PARTKEY, 2));
	} else if( brand == q19in.p_brand[2] &&
		   ( strcmp( apart.P_CONTAINER, "MED BAG") ||
		     strcmp( apart.P_CONTAINER, "MED BAG") ||
		     strcmp( apart.P_CONTAINER, "MED BAG") ||
		     strcmp( apart.P_CONTAINER, "MED BAG") ) &&
		   size > 1 && size < 15 ){
	    partkey_brand.insert( pair<int,int> (apart.P_PARTKEY, 3));
	} 
	W_DO(p_iter->next(_pssm, eof, *prpart));
    }
    
    //phase#2 tablescan lineitem
    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t lreprow(_plineitem_man->ts());
    lreprow.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &lreprow;

    guard< table_scan_iter_impl<lineitem_t> > l_iter;
    {
	table_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->get_iter_for_file_scan(_pssm, tmp_l_iter));
	l_iter = tmp_l_iter;
    }
        
    tpch_lineitem_tuple aline;

    W_DO(l_iter->next(_pssm, eof, *prlineitem));
    
    while (!eof) {
	prlineitem->get_value(1, aline.L_PARTKEY);
	prlineitem->get_value(4, aline.L_QUANTITY);
	prlineitem->get_value(5, aline.L_EXTENDEDPRICE);
	prlineitem->get_value(6, aline.L_DISCOUNT);
	prlineitem->get_value(13, aline.L_SHIPINSTRUCT, 25);
	prlineitem->get_value(14, aline.L_SHIPMODE, 10);
	map<int,int>::iterator iter = partkey_brand.find( aline.L_PARTKEY );
	if(iter != partkey_brand.end() &&
	   ( strcmp( aline.L_SHIPMODE, "AIR") == 0 ||
	     strcmp( aline.L_SHIPMODE, "AIR REG") == 0 ) &&
	   strcmp( aline.L_SHIPINSTRUCT, "DELIVER IN PERSON") == 0 &&
	   aline.L_QUANTITY  >= q19in.l_quantity[iter->second-1] &&
	   aline.L_QUANTITY <= q19in.l_quantity[iter->second-1] + 10 ){
	    revenue += aline.L_EXTENDEDPRICE * ( 1 - aline.L_DISCOUNT );
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }

    return RCOK;
}// EOF: Q19



/******************************************************************** 
 *
 * TPC-H Q20
 *
 ********************************************************************/

struct int_pair_cmp {
    bool operator()( const pair<int,int>& r1, const pair<int,int>& r2 ) const {
	return (r1.first < r2.first || ( r1.first == r2.first && r1.second < r2.second)) ;
    }
};


w_rc_t ShoreTPCHEnv::xct_q20(const int /* xct_id */, q20_input_t& q20in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    char p_name[55];
    pname_to_str(q20in.p_color, p_name);

    //phase#1 :table scan part
    map<int,bool> partkey;

    tuple_guard<part_man_impl> prpart(_ppart_man);

    rep_row_t preprow(_ppart_man->ts());
    preprow.set(_ppart_desc->maxsize());

    prpart->_rep = &preprow;

    guard<table_scan_iter_impl<part_t> > p_iter;
    {
	table_scan_iter_impl<part_t>* tmp_p_iter;
	W_DO(_ppart_man->get_iter_for_file_scan(_pssm, tmp_p_iter));
	p_iter = tmp_p_iter;
    }
	    
    bool eof;
    tpch_part_tuple apart;
    
    W_DO(p_iter->next(_pssm, eof, *prpart));

    while(!eof){
	prpart->get_value(0, apart.P_PARTKEY);
	prpart->get_value(1, apart.P_NAME, 55);
	char* s1 = strtok(apart.P_NAME, " ");
	if( strcmp( s1, p_name) == 0 ) {
	    partkey.insert(pair<int, bool> (apart.P_PARTKEY, 1));
	}
	W_DO(p_iter->next(_pssm, eof, *prpart));
    }
  
    //phase#2 table scan ps
    map<pair<int,int>, pair<int,int>, int_pair_cmp> supppart_avquant_sumquant;

    tuple_guard<partsupp_man_impl> prpartsupp(_ppartsupp_man);

    rep_row_t psreprow(_ppartsupp_man->ts());
    psreprow.set(_ppartsupp_desc->maxsize());

    prpartsupp->_rep = &psreprow;

    guard<table_scan_iter_impl<partsupp_t> > ps_iter;
    {
	table_scan_iter_impl<partsupp_t>* tmp_ps_iter;
	W_DO(_ppartsupp_man->get_iter_for_file_scan(_pssm, tmp_ps_iter));
	ps_iter = tmp_ps_iter;
    }
    
    tpch_partsupp_tuple apartsupp;

    W_DO(ps_iter->next(_pssm, eof, *prpartsupp));

    while(!eof){
	prpartsupp->get_value(0, apartsupp.PS_PARTKEY);
	prpartsupp->get_value(1, apartsupp.PS_SUPPKEY);
	prpartsupp->get_value(2, apartsupp.PS_AVAILQTY);
	if( partkey.find(apartsupp.PS_PARTKEY) != partkey.end() ){
	    pair< pair <int, int>,pair<int,int> > tmp
		(pair<int,int>( apartsupp.PS_SUPPKEY, apartsupp.PS_SUPPKEY),
		 pair<int,int>( apartsupp.PS_AVAILQTY, 0) );
	    supppart_avquant_sumquant.insert(tmp);
	}
	W_DO(ps_iter->next(_pssm, eof, *prpartsupp));
    }
 
    //phase#3 indexscan lineitem: l_shipdate
    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t lreprow(_plineitem_man->ts());
    lreprow.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &lreprow;
    
    rep_row_t lowrep(_plineitem_man->ts());
    rep_row_t highrep(_plineitem_man->ts());

    lowrep.set(_plineitem_desc->maxsize());
    highrep.set(_plineitem_desc->maxsize());

    struct tm date;
    if (gmtime_r(&(q20in.l_shipdate), &date) == NULL) {
	return RCOK;
    }
    date.tm_year ++;
    time_t last_shipdate = mktime(&date);
    
    guard< index_scan_iter_impl<lineitem_t> > l_iter;
    {
	index_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->l_get_range_iter_by_index(_pssm, tmp_l_iter,
						       prlineitem, lowrep,
						       highrep, q20in.l_shipdate,
						       last_shipdate));
	l_iter = tmp_l_iter;
    }

    tpch_lineitem_tuple aline;
    double q6_result = 0;
    
    W_DO(l_iter->next(_pssm, eof, *prlineitem));
	
    while (!eof) {
	prlineitem->get_value(1, aline.L_PARTKEY);
	prlineitem->get_value(2, aline.L_SUPPKEY);
	prlineitem->get_value(4, aline.L_QUANTITY);
	map< pair<int,int>, pair<int,int>, int_pair_cmp >::iterator it =
	    supppart_avquant_sumquant.find(pair<int,int>
					   (aline.L_PARTKEY, aline.L_SUPPKEY));
	if( it != supppart_avquant_sumquant.end()){
	    int c = it->second.second;
	    c+= aline.L_QUANTITY;
	    supppart_avquant_sumquant[ pair<int,int>(aline.L_PARTKEY,
						     aline.L_SUPPKEY)]=
		pair<int,int>( it->second.first, c);	       
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }
    
    //phase#4 : compare the available_quantity with sumof_quantity
    map<int, bool> suppkey;
    map< pair<int,int>, pair<int, int> > :: iterator it =
	supppart_avquant_sumquant.begin();
    while( it != supppart_avquant_sumquant.end()){
	if( it->second.first > .5 * it->second.second ){
	    suppkey.insert( pair<int,bool>(it->first.first, true));
	}
	it++;
    }

    //phase#5 : look for the nationkey of [NATION]
    //.. no need for it

    //phase#6 : retrive s_name and s_address of the suppkeys, index scan
    tuple_guard<supplier_man_impl> prsupp(_psupplier_man);
   
    rep_row_t sreprow(_psupplier_man->ts());
    sreprow.set(_psupplier_desc->maxsize());

    prsupp->_rep = &sreprow;

    for(map<int,bool>::iterator iter = suppkey.begin();
	iter != suppkey.end();
	iter++){
       _psupplier_man->s_index_probe(_pssm, prsupp, iter->first);
       tpch_supplier_tuple asupp;
       prsupp->get_value(1, asupp.S_NAME, 25);
       prsupp->get_value(2, asupp.S_ADDRESS, 40);
       prsupp->get_value(3, asupp.S_NATIONKEY);
       if(asupp.S_NATIONKEY == q20in.n_name){
	   //result
       }
    }
    
  return RCOK;
}



/******************************************************************** 
 *
 * TPC-H Q21
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_q21(const int /* xct_id */, q21_input_t& q21in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //phase#1 scan supplier_t for supps which s_nationkey == q21.in
    map<int, string> suppkey;

    tuple_guard<supplier_man_impl> prsupp(_psupplier_man);

    rep_row_t sreprow(_psupplier_man->ts());
    sreprow.set(_psupplier_desc->maxsize());

    prsupp->_rep = &sreprow;

    guard<table_scan_iter_impl<supplier_t> > s_iter;
    {
	table_scan_iter_impl<supplier_t>* tmp_s_iter;
	W_DO(_psupplier_man->get_iter_for_file_scan(_pssm, tmp_s_iter));
	s_iter = tmp_s_iter;
    }
    
    bool eof;
    tpch_supplier_tuple asupplier;

    W_DO(s_iter->next(_pssm, eof, *prsupp));

    while(!eof){
	prsupp->get_value(0, asupplier.S_SUPPKEY);
	prsupp->get_value(1, asupplier.S_NAME,25);
	prsupp->get_value(3, asupplier.S_NATIONKEY);
	if( asupplier.S_NATIONKEY == q21in.n_name){
	    suppkey.insert(pair<int,string>
			   ( asupplier.S_SUPPKEY, string(asupplier.S_NAME)));
	}
	W_DO(s_iter->next(_pssm, eof, *prsupp));
    }

    //phase#2 : scan order for tuples which stat = 'F'
    map<int,bool> orderkey;

    tuple_guard<orders_man_impl> prorder(_porders_man);
    
    rep_row_t o_areprow(_porders_man->ts());
    o_areprow.set(_porders_desc->maxsize());

    prorder->_rep = &o_areprow;

    guard< table_scan_iter_impl<orders_t> > o_iter;
    {
	table_scan_iter_impl<orders_t>* tmp_o_iter;
	W_DO(_porders_man->get_iter_for_file_scan(_pssm, tmp_o_iter));
	o_iter = tmp_o_iter;
    }

    tpch_orders_tuple anorder;
    
    W_DO(o_iter->next(_pssm, eof, *prorder));

    while(!eof){
	prorder->get_value(0, anorder.O_ORDERKEY);
	prorder->get_value(2, anorder.O_ORDERSTATUS);
	if( anorder.O_ORDERSTATUS == 'F' ) {
	    orderkey.insert( pair <int,bool> (anorder.O_ORDERKEY, true) );
	}
	W_DO(o_iter->next(_pssm, eof, *prorder));
    }

    //table scan lineitem for delayed tuples
    vector<pair<int,int> > suppkey_orderkey;
    map<int,int> multiOrders;
    map<int,int> delay2order; //orders with more than 2 supp delay for them

    tuple_guard<lineitem_man_impl> prlineitem(_plineitem_man);

    rep_row_t areprow2(_plineitem_man->ts());
    areprow2.set(_plineitem_desc->maxsize()); 

    prlineitem->_rep = &areprow2;
    
    guard< table_scan_iter_impl<lineitem_t> > l_iter;
    {
	table_scan_iter_impl<lineitem_t>* tmp_l_iter;
	W_DO(_plineitem_man->get_iter_for_file_scan(_pssm, tmp_l_iter));
	l_iter = tmp_l_iter;
    }

    tpch_lineitem_tuple aline;

    W_DO(l_iter->next(_pssm, eof, *prlineitem));
    
    while (!eof) {
	prlineitem->get_value(0, aline.L_ORDERKEY);
	prlineitem->get_value(2, aline.L_SUPPKEY);	    
	prlineitem->get_value(11, aline.L_COMMITDATE, 15);
	prlineitem->get_value(12, aline.L_RECEIPTDATE, 15);
	time_t the_commitdate = str_to_timet(aline.L_COMMITDATE);
	time_t the_receiptdate = str_to_timet(aline.L_RECEIPTDATE);
	if( orderkey.find(aline.L_ORDERKEY) != orderkey.end()){
	    if (the_commitdate < the_receiptdate &&
		suppkey.find(aline.L_SUPPKEY) != suppkey.end()){
		suppkey_orderkey.push_back(pair<int,int>
					   (aline.L_SUPPKEY, aline.L_ORDERKEY));
		map<int,int>::iterator it = delay2order.find(aline.L_ORDERKEY);
		if (it != delay2order.end() && it->second != aline.L_SUPPKEY){
		    delay2order[it->first] = -1;
		} else {
		    delay2order[it->first] = aline.L_SUPPKEY;
		}
	    }
	    map<int,int>::iterator it = multiOrders.find(aline.L_ORDERKEY);
	    if (it != multiOrders.end() && it->second != aline.L_SUPPKEY){
		multiOrders[it->first] = -1;
	    } else {
		multiOrders[it->first] = aline.L_SUPPKEY;
	    }
	}
	W_DO(l_iter->next(_pssm, eof, *prlineitem));
    }
    
    //final phase: computing the scalar
    map<int,int> supK_numwait;    
    for(int i = 0; i < suppkey_orderkey.size(); i++){
	if( multiOrders[suppkey_orderkey[i].second] == -1 &&
	    delay2order[suppkey_orderkey[i].second] != -1) {
	    if(supK_numwait.find(suppkey_orderkey[i].first)!=supK_numwait.end()){
		int n = supK_numwait.find(suppkey_orderkey[i].first)->second;
		n++;
		supK_numwait[suppkey_orderkey[i].first] = n;
	    } else {
		supK_numwait[suppkey_orderkey[i].first] = 1;
	    }
	}
    }
   
    return RCOK;
}



/******************************************************************** 
 *
 * TPC-H Q22
 *
 ********************************************************************/

w_rc_t ShoreTPCHEnv::xct_q22(const int /* xct_id */, q22_input_t& q22in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //phase#1 tablescan customer: <ckey,acctbal,code> and AVG(acctbal)
    map<int, pair<decimal,int> > ckey_acbalCcode;
    decimal bal_avg = 0;

    tuple_guard<customer_man_impl> prcustomer(_pcustomer_man);

    rep_row_t areprow(_pcustomer_man->ts());
    areprow.set(_pcustomer_desc->maxsize());

    prcustomer->_rep = &areprow;

    guard< table_scan_iter_impl<customer_t> > c_iter;
    {
	table_scan_iter_impl<customer_t>* tmp_c_iter;
	W_DO(_pcustomer_man->get_iter_for_file_scan(_pssm, tmp_c_iter));
	c_iter = tmp_c_iter;
    }
    
    bool eof;
    tpch_customer_tuple acust;

    W_DO(c_iter->next(_pssm, eof, *prcustomer));

    while(!eof){
	prcustomer->get_value(0, acust.C_CUSTKEY);
	prcustomer->get_value(4, acust.C_PHONE, 15);
	prcustomer->get_value(5, acust.C_ACCTBAL);
	char* c_cntrycode_str =  strtok ( acust.C_PHONE, "-" );
	int c_cntrycode =  atoi( c_cntrycode_str);
	for(int i = 0; i < 7; i++ ){
	    if( q22in.cntrycode[i] == c_cntrycode){
		ckey_acbalCcode.insert( pair<int,  pair<decimal,int> >
					(acust.C_CUSTKEY, pair<decimal,int>
					 (acust.C_ACCTBAL, c_cntrycode)));
		bal_avg += acust.C_ACCTBAL;
		break;
	    }
	}
	W_DO(c_iter->next(_pssm, eof, *prcustomer));
    }

    bal_avg /= (int)ckey_acbalCcode.size();
    
    //phase#2 index scan order
    //scalar: numof customer, total balance
    map<int, pair<int, decimal> > cntrycode_scalars; 

    tuple_guard<orders_man_impl> prorders(_porders_man);

    rep_row_t aoreprow(_porders_man->ts());
    aoreprow.set(_porders_desc->maxsize());

    prorders->_rep = & aoreprow;

    rep_row_t lowrep(_porders_man->ts());
    rep_row_t highrep(_porders_man->ts());

    lowrep.set(_porders_desc->maxsize());
    highrep.set(_porders_desc->maxsize());

    map<int, pair<decimal,int> >::iterator it =  ckey_acbalCcode.begin();
    while(it != ckey_acbalCcode.end()){
	guard<index_scan_iter_impl<orders_t> > o_iter;
	{
	    index_scan_iter_impl<orders_t>* tmp_o_iter;
	    W_DO(_porders_man->o_get_iter_by_findex(_pssm, tmp_o_iter, prorders,
						    lowrep, highrep, it->first));
	    o_iter = tmp_o_iter;
	}
	
	bool eof;
	W_DO(o_iter->next(_pssm, eof, *prorders));

	if(!eof && it->second.first > bal_avg){
	    map<int, pair<int,decimal> >::iterator tmp =
		cntrycode_scalars.find( it->second.second);
	    if(tmp != cntrycode_scalars.end()){
		int c = tmp->second.first;
		decimal t = tmp->second.second;
		c++;
		t+= it->second.first;
		cntrycode_scalars[it->second.second] = pair<int, decimal> (c, t);
	    } else {
		cntrycode_scalars[it->second.second] = pair<int, decimal>
		    (1, it->second.first);
	    }
	}       
	it++;
    }

    return RCOK;
}


EXIT_NAMESPACE(tpch);
