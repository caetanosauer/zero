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

/** @file:   shore_ssb_xct.cpp
 *
 *  @brief:  Implementation of the Baseline Shore SSB transactions
 *
 *  @author: Manos Athanassoulis, June 2010
 */

#include "workload/ssb/shore_ssb_env.h"
#include "workload/ssb/ssb_random.h"

#include <vector>
#include <map>
#include <numeric>
#include <algorithm>

#include "workload/ssb/dbgen/dss.h"
#include "workload/ssb/dbgen/dsstypes.h"

// #include "qp/shore/qp_util.h"
// #include "qp/shore/ahhjoin.h"

#include "sm_base.h"


using namespace shore;
using namespace dbgenssb;
//using namespace qp;


ENTER_NAMESPACE(ssb);


/******************************************************************** 
 *
 * Thread-local SSB TRXS Stats
 *
 ********************************************************************/


static __thread ShoreSSBTrxStats my_stats;

void ShoreSSBEnv::env_thread_init()
{
    CRITICAL_SECTION(stat_mutex_cs, _statmap_mutex);
    _statmap[pthread_self()] = &my_stats;
}

void ShoreSSBEnv::env_thread_fini()
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

ShoreSSBTrxStats ShoreSSBEnv::_get_stats()
{
    CRITICAL_SECTION(cs, _statmap_mutex);
    ShoreSSBTrxStats rval;
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

void ShoreSSBEnv::reset_stats()
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

void ShoreSSBEnv::print_throughput(const double iQueriedSF, 
                                    const int iSpread, 
                                    const int iNumOfThreads, 
                                    const double delay,
                                    const ulong_t mioch,
                                    const double avgcpuusage)
{
    CRITICAL_SECTION(last_stats_cs, _last_stats_mutex);
    
    // get the current statistics
    ShoreSSBTrxStats current_stats = _get_stats();
	   
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
 * SSB Parallel Loading
 *
 ********************************************************************/

/*
  DATABASE POPULATION TRANSACTIONS

  The SSB database has 5 tables. Out of those:
  1 is static (DATE)
  3 depend on SF (CUSTOMER, PART, SUPPLIER)
  1 depends on all of the above tables (LINEORDER)

  Regular cardinalities:

  Supplier :            =   2K*SF   

  Part     :            =   0.2M*floor(1+log2(SF ))
  Date     :            =   7*365 = 2556

  Customer :                30K*SF 
  Lineorder: [1,7]*Cust =   6M*SF  

  
  The table creator:
  1) Creates all the tables 
  2) Loads completely DATE, SUPPLIER, CUSTOMER
  3) Loads #ParLoaders*DIVISOR PART units (PART,LINEORDER)

  The sizes of the records:
  SUPPLIER:  ??
  PART:      ??
  CUSTOMER:  ??
  DATE:      ??
  LINEORDER: ??


  TPCH
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
 * Those functions populate records for the SSB database. They do not
 * commit thought. So, they need to be invoked inside a transaction
 * and the caller needs to commit at the end. 
 *
 ********************************************************************/

#undef  DO_PRINT_SSB_RECS
//#define DO_PRINT_SSB_RECS

//Populates one date
w_rc_t ShoreSSBEnv::_gen_one_date(const int id, 
				  rep_row_t& areprow)
{    
    tuple_guard<date_man_impl> prda(_pdate_man);
    
    prda->_rep = &areprow;
    
    dbgenssb::date_t ad;
    mk_date(id, &ad);
    
#ifdef DO_PRINT_SSB_RECS
    TRACE( TRACE_ALWAYS,
	   "%ld,%s,%s,%s,%d,%d,%s,%d,%d,%d,%d,%d,%s[len:%d],%s,%s,%s,%s\n",
	   ad.datekey, ad.date, ad.dayofweek, ad.month, ad.year,
	   ad.yearmonthnum, ad.yearmonth, ad.daynuminweek, ad.daynuminmonth,
	   ad.daynuminyear, ad.monthnuminyear, ad.weeknuminyear,
	   ad.sellingseason, ad.slen, ad.lastdayinweekfl, ad.lastdayinmonthfl,
	   ad.holidayfl, ad.weekdayfl);
#endif
    
    prda->set_value(0, (int)ad.datekey);
    prda->set_value(1, ad.date);
    prda->set_value(2, ad.dayofweek);
    prda->set_value(3, ad.month);
    prda->set_value(4, (int)ad.year);
    prda->set_value(5, (int)ad.yearmonthnum);
    prda->set_value(6, ad.yearmonth);
    prda->set_value(7, (int)ad.daynuminweek);
    prda->set_value(8, (int)ad.daynuminmonth);
    prda->set_value(9, (int)ad.daynuminyear);
    prda->set_value(10, (int)ad.monthnuminyear);
    prda->set_value(11, (int)ad.weeknuminyear);
    prda->set_value(12, ad.sellingseason);
    prda->set_value(13, ad.lastdayinweekfl);
    prda->set_value(14, ad.lastdayinmonthfl);
    prda->set_value(15, ad.holidayfl);
    prda->set_value(16, ad.weekdayfl);
    
    W_DO(_pdate_man->add_tuple(_pssm, prda));
    
    return RCOK;
}


// Populates one supplier
w_rc_t ShoreSSBEnv::_gen_one_supplier(const int id,
				      rep_row_t& areprow)
{    
    tuple_guard<supplier_man_impl> prsu(_psupplier_man);
    
    prsu->_rep = &areprow;

    dbgenssb::supplier_t as;
    mk_supp(id, &as);

#ifdef DO_PRINT_SSB_RECS
    if (id%100==0) {
        TRACE(TRACE_ALWAYS, "%ld,%s,%s[len:%d],%s,%s,[%d:]%s,%s\n",
	      as.suppkey, as.name, as.address, as.alen, as.city,
	      as.nation_name, as.region_key, as.region_name, as.phone); 
    }
#endif
    
    prsu->set_value(0, (int)as.suppkey);
    prsu->set_value(1, as.name);
    prsu->set_value(2, as.address);
    prsu->set_value(3, as.city);
    prsu->set_value(4, as.nation_name);
    prsu->set_value(5, as.region_name);
    prsu->set_value(6, as.phone);

    W_DO(_psupplier_man->add_tuple(_pssm, prsu));

    return RCOK;
}


// Populates one customer
w_rc_t ShoreSSBEnv::_gen_one_customer(const int id,
				      rep_row_t& areprow)
{    
    tuple_guard<customer_man_impl> prcu(_pcustomer_man);

    prcu->_rep = &areprow;

    dbgenssb::customer_t ac;
    mk_cust(id, &ac);
    
#ifdef DO_PRINT_SSB_RECS
    if (id%100==0) {
        TRACE(TRACE_ALWAYS,
	      "%ld,%s[len:%d],%s[len:%d],%s[len:%d],%s(%d),[%d:]%s(%d),%s,%s\n",
	      ac.custkey, ac.name, strlen(ac.name), ac.address,
	      strlen(ac.address), ac.city, strlen(ac.city), ac.nation_name,
	      strlen(ac.nation_name), ac.region_key, ac.region_name,
	      strlen(ac.region_name), ac.phone, ac.mktsegment); 
    }
#endif
    
    prcu->set_value(0, (int)ac.custkey);
    prcu->set_value(1, ac.name);
    prcu->set_value(2, ac.address);
    prcu->set_value(3, ac.city);
    prcu->set_value(4, ac.nation_name);
    prcu->set_value(5, ac.region_name);
    prcu->set_value(6, ac.phone);
    prcu->set_value(7, ac.mktsegment);
    
    W_DO(_pcustomer_man->add_tuple(_pssm, prcu));

    return RCOK;
}


// Populates one part 
w_rc_t ShoreSSBEnv::_gen_one_part(const int id, 
				  rep_row_t& areprow)
{    
    tuple_guard<part_man_impl> prpa(_ppart_man);

    prpa->_rep = &areprow;
    
    dbgenssb::part_t ap;
    mk_part(id, &ap);

#ifdef DO_PRINT_SSB_RECS
    if (id%100==0) {
        TRACE( TRACE_ALWAYS,
	       "%ld,%s[len:%d],%s,%s,%s,%s[len:%d],%s[len:%d],%ld,%s\n",
               ap.partkey,ap.name,ap.nlen,ap.mfgr,ap.category,ap.brand,ap.color,
               ap.clen,ap.type,ap.tlen,ap.size,ap.container); 
    }
#endif
    
    prpa->set_value(0, (int)ap.partkey);
    prpa->set_value(1, ap.name);
    prpa->set_value(2, ap.mfgr);
    prpa->set_value(3, ap.category);
    prpa->set_value(4, ap.brand);
    prpa->set_value(5, ap.color);
    prpa->set_value(6, ap.type);
    prpa->set_value(7, (int)ap.size);
    prpa->set_value(8, ap.container);

    W_DO(_ppart_man->add_tuple(_pssm, prpa));

    return RCOK;
}


// Populates one lineorder
w_rc_t ShoreSSBEnv::_gen_one_lineorder(const int id, 
				       rep_row_t& areprow)
{    
    dbgenssb::order_t o;
    
    INIT_HUGE(o.okey);
    for (int i=0; i < O_LCNT_MAX; i++) {
	INIT_HUGE(o.lineorders[i].okey);	
    }
    mk_order(id, &o, 0);
    
#ifdef DO_PRINT_SSB_RECS
    if (id%100==0) {
	for(int i=0;i<o.lines;i++) {
	    TRACE( TRACE_ALWAYS,
		   "%d,%d,%d,%d,%d,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s\n",
		   (int)o.okey[0], (int)o.lineorders[i].linenumber,
		   (int)o.custkey, (int)o.lineorders[i].partkey,
		   (int)o.lineorders[i].suppkey,
		   (int)atoi(o.lineorders[i].orderdate),
		   o.lineorders[i].opriority,
		   (int)o.lineorders[i].ship_priority,
		   (int)o.lineorders[i].quantity,
		   (int)o.lineorders[i].extended_price,
		   (int)o.lineorders[i].order_totalprice,
		   (int)o.lineorders[i].discount,
		   (int)o.lineorders[i].revenue, (int)o.lineorders[i].supp_cost,
		   (int)o.lineorders[i].tax,
		   (int)atoi(o.lineorders[i].commit_date),
		   o.lineorders[i].shipmode);
	}
    }
#endif
    
    for (int i=0;i<o.lines;i++) {

	tuple_guard<lineorder_man_impl> prlo(_plineorder_man);

        prlo->_rep = &areprow;
        
	prlo->set_value(0, (int)o.okey[0]);
	prlo->set_value(1, (int)o.lineorders[i].linenumber);
	prlo->set_value(2, (int)o.custkey);
	prlo->set_value(3, (int)o.lineorders[i].partkey);
	prlo->set_value(4, (int)o.lineorders[i].suppkey);
	prlo->set_value(5, (int)atoi(o.lineorders[i].orderdate));
	prlo->set_value(6, o.lineorders[i].opriority);
	prlo->set_value(7, (int)o.lineorders[i].ship_priority);
	prlo->set_value(8, (int)o.lineorders[i].quantity);
	prlo->set_value(9, (int)o.lineorders[i].extended_price);
	prlo->set_value(10, (int)o.lineorders[i].order_totalprice);
	prlo->set_value(11, (int)o.lineorders[i].discount);
	prlo->set_value(12, (int)o.lineorders[i].revenue);
	prlo->set_value(13, (int)o.lineorders[i].supp_cost);
	prlo->set_value(14, (int)o.lineorders[i].tax);
	prlo->set_value(15, (int)atoi(o.lineorders[i].commit_date));
	prlo->set_value(16, o.lineorders[i].shipmode);
            
        W_DO(_plineorder_man->add_tuple(_pssm, prlo));
    }

    return RCOK;
}




/******************************************************************** 
 *
 * SSB Loading: Population transactions
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_populate_baseline(const int /* xct_id */, 
                                           populate_baseline_input_t& in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);

    // The LINEORDER is the biggest (147) of all the tables
    rep_row_t areprow(_pcustomer_man->ts());
    areprow.set(_pcustomer_desc->maxsize());
    //rep_row_t areprow(_plineorder_man->ts());
    //areprow.set(_plineorder_desc->maxsize());
    TRACE( TRACE_ALWAYS, "LO MAX SIZE:%d \n",_plineorder_desc->maxsize());
    TRACE( TRACE_ALWAYS, "PA MAX SIZE:%d \n",_ppart_desc->maxsize());
    TRACE( TRACE_ALWAYS, "CU MAX SIZE:%d \n",_pcustomer_desc->maxsize());
    TRACE( TRACE_ALWAYS, "SU MAX SIZE:%d \n",_psupplier_desc->maxsize());
    TRACE( TRACE_ALWAYS, "DA MAX SIZE:%d \n",_pdate_desc->maxsize());

    // 2. Build the small tables
    TRACE( TRACE_ALWAYS, "Building DATE !!!\n");
    for(int i=1; i<=NO_DATE; ++i) {
        W_DO(_gen_one_date(i, areprow));
    }

    TRACE( TRACE_ALWAYS, "Building SUPPLIER SF=%d*%d=%d!!!\n",
	   (int)in._sf, (int)SUPPLIER_PER_SF, (int)(in._sf*SUPPLIER_PER_SF));
    for (int i=1; i<=in._sf*SUPPLIER_PER_SF; ++i) {
        W_DO(_gen_one_supplier(i, areprow));
    }

    TRACE( TRACE_ALWAYS, "Starting CUSTOMER SF=%d*%d=%d!!!\n",
	   (int)in._sf, (int)CUSTOMER_PER_SF, (int)(in._sf*CUSTOMER_PER_SF));
    for (int i=1; i<=in._sf*CUSTOMER_PER_SF; ++i) {
        W_DO(_gen_one_customer(i, areprow));
    }

    TRACE( TRACE_ALWAYS, "Starting PART 1+log2(SF)=1+log2(%d)=%d*%d=%d!!!\n",
	   (int)in._sf, (int)(1+log2(in._sf)), (int)PART_PER_SF,
	   (int)((1+log2(in._sf))*PART_PER_SF));
    for (int i=1; i<=(1+log2(in._sf))*PART_PER_SF; ++i) {
        W_DO( _gen_one_part(i, areprow));
    }

    // 3. Insert first rows of lineorder
    TRACE( TRACE_ALWAYS, "Starting LINEORDERS SF=%d*%d=%d!!!\n",
	   (int)in._sf, (int)LINEORDER_PER_SF, (int)(in._sf*LINEORDER_PER_SF));
    
    for (int i=0; i < in._loader_count; ++i) {
        long start = i * in._lineorder_per_thread + 1;
        long end = start + in._divisor - 1;
        TRACE( TRACE_ALWAYS, "Lineorder %d .. %d\n", start, end);
        
        for (int j=start; j<end; ++j) {
            W_DO(e = _gen_one_lineorder(j,areprow));
        }    
    }

    W_DO(_pssm->commit_xct());
    return RCOK;
}


// We pass on the parameter (xct_id) the number of parts to be populated
// and on the parameter (in) the starting id
w_rc_t ShoreSSBEnv::xct_populate_some_lineorders(const int xct_id, 
						 populate_some_lineorders_input_t& in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_plineorder_man->ts());
    areprow.set(_plineorder_desc->maxsize());

    int id = in._orderid;

    // Generate (xct_id) parts
    for (id=in._orderid; id<in._orderid+xct_id; id++) {
        W_DO(_gen_one_lineorder(id, areprow));
    }

    W_DO(_pssm->commit_xct());
    return RCOK;
}


/*
// We pass on the parameter (xct_id) the number of parts to be populated
// and on the parameter (in) the starting id
w_rc_t ShoreSSBEnv::xct_populate_some_parts(const int xct_id, 
                                             populate_some_parts_input_t& in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);

    // The Partsupp is the biggest (224) of all the 2 tables
    rep_row_t areprow(_ppartsupp_man->ts());
    areprow.set(_ppartsupp_desc->maxsize());

    w_rc_t e = RCOK;
    int id = in._partid;

    // Generate (xct_id) parts
    for (id=in._partid; id<in._partid+xct_id; id++) {
        e = _gen_one_part_based(id, areprow);
        if(e.is_error()) { return (e); }
    }

    e = _pssm->commit_xct();
    return (e);
}


// We pass on the parameter (xct_id) the number of customers to be populated
// and on the parameter (in) the starting id
w_rc_t ShoreSSBEnv::xct_populate_some_custs(const int xct_id, 
                                             populate_some_custs_input_t& in)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);

    // The Customer is the biggest (240) of the 3 tables
    rep_row_t areprow(_pcustomer_man->ts());
    areprow.set(_pcustomer_desc->maxsize()); 

    w_rc_t e = RCOK;
    int id = in._custid;

    // Generate (xct_id) customers
    for (id=in._custid; id<in._custid+xct_id; id++) {
        e = _gen_one_cust_based(id, areprow);
        if (e.is_error()) { return (e); }
    }

    e = _pssm->commit_xct();
    return (e);
} 
*/


/******************************************************************** 
 *
 * SSB QUERIES (TRXS)
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

w_rc_t ShoreSSBEnv::run_one_xct(Request* prequest)
{
#ifdef CFG_QPIPE
    if (prequest->type() >= XCT_QPIPE_SSB_MIX) {
        return (run_one_qpipe_xct(prequest));
    }
#endif

    // if BASELINE SSB MIX
    if (prequest->type() == XCT_SSB_MIX) {
        prequest->set_type(XCT_SSB_MIX + abs(smthread_t::me()->rand()%22));
    }

    switch (prequest->type()) {

        // SSB BASELINE
    case XCT_SSB_QDATE:
        return (run_qdate(prequest));

    case XCT_SSB_QPART:
        return (run_qpart(prequest));

    case XCT_SSB_QSUPPLIER:
        return (run_qsupplier(prequest));

    case XCT_SSB_QCUSTOMER:
        return (run_qcustomer(prequest));

    case XCT_SSB_QLINEORDER:
        return (run_qlineorder(prequest));

    case XCT_SSB_QTEST:
        return (run_qtest(prequest));
        
    case XCT_SSB_Q1_1:
        return (run_q1_1(prequest));

    case XCT_SSB_Q1_2:
        return (run_q1_2(prequest));

    case XCT_SSB_Q1_3:
        return (run_q1_3(prequest));

    case XCT_SSB_Q2_1:
        return (run_q2_1(prequest));

    case XCT_SSB_Q2_2:
        return (run_q2_2(prequest));

    case XCT_SSB_Q2_3:
        return (run_q2_3(prequest));;

    case XCT_SSB_Q3_1:
        return (run_q3_1(prequest));

    case XCT_SSB_Q3_2:
        return (run_q3_2(prequest));

    case XCT_SSB_Q3_3:
        return (run_q3_3(prequest));

    case XCT_SSB_Q3_4:
        return (run_q3_4(prequest));

    case XCT_SSB_Q4_1:
        return (run_q4_1(prequest));

    case XCT_SSB_Q4_2:
        return (run_q4_2(prequest));;

    case XCT_SSB_Q4_3:
        return (run_q4_3(prequest));

    default:
        //assert (0); // UNKNOWN TRX-ID
        TRACE( TRACE_ALWAYS, "Unknown transaction\n");
        assert(0);
    }
    return (RCOK);
}


/******************************************************************** 
 *
 * SSB TRXs Wrappers
 *
 * @brief: They are wrappers to the functions that execute the transaction
 *         body. Their responsibility is to:
 *
 *         1. Prepare the corresponding input
 *         2. Check the return of the trx function and abort the trx,
 *            if something went wrong
 *         3. Update the ssb db environment statistics
 *
 ********************************************************************/


// --- without input specified --- //

DEFINE_TRX(ShoreSSBEnv,q1_1);
DEFINE_TRX(ShoreSSBEnv,q1_2);
DEFINE_TRX(ShoreSSBEnv,q1_3);
DEFINE_TRX(ShoreSSBEnv,q2_1);
DEFINE_TRX(ShoreSSBEnv,q2_2);
DEFINE_TRX(ShoreSSBEnv,q2_3);
DEFINE_TRX(ShoreSSBEnv,q3_1);
DEFINE_TRX(ShoreSSBEnv,q3_2);
DEFINE_TRX(ShoreSSBEnv,q3_3);
DEFINE_TRX(ShoreSSBEnv,q3_4);
DEFINE_TRX(ShoreSSBEnv,q4_1);
DEFINE_TRX(ShoreSSBEnv,q4_2);
DEFINE_TRX(ShoreSSBEnv,q4_3);
DEFINE_TRX(ShoreSSBEnv,qpart);
DEFINE_TRX(ShoreSSBEnv,qdate);
DEFINE_TRX(ShoreSSBEnv,qcustomer);
DEFINE_TRX(ShoreSSBEnv,qsupplier);
DEFINE_TRX(ShoreSSBEnv,qlineorder);
DEFINE_TRX(ShoreSSBEnv,qtest);


// uncomment the line below if want to dump (part of) the trx results
//#define PRINT_TRX_RESULTS


/******************************************************************** 
 *
 * SSB QDATE
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qdate(const int /* xct_id */, 
                            qdate_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB QPART
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qpart(const int /* xct_id */, 
                            qpart_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB QCUSTOMER
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qcustomer(const int /* xct_id */, 
                            qcustomer_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB QSUPPLIER
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qsupplier(const int /* xct_id */, 
                            qsupplier_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB QLINEORDER
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qlineorder(const int /* xct_id */, 
                            qlineorder_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}

/******************************************************************** 
 *
 * SSB QTEST
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qtest(const int /* xct_id */, 
                            qtest_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB Q1_1
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q1_1(const int /* xct_id */, 
                            q1_1_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB Q1_2
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q1_2(const int /* xct_id */, 
                            q1_2_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB Q1_3
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q1_3(const int /* xct_id */, 
                            q1_3_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}



/******************************************************************** 
 *
 * SSB Q2_1
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q2_1(const int /* xct_id */, 
                            q2_1_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB Q2_2
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q2_2(const int /* xct_id */, 
                            q2_2_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB Q2_3
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q2_3(const int /* xct_id */, 
                            q2_3_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}



/******************************************************************** 
 *
 * SSB Q3_1
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q3_1(const int /* xct_id */, 
                            q3_1_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB Q3_2
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q3_2(const int /* xct_id */, 
                            q3_2_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB Q3_3
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q3_3(const int /* xct_id */, 
                            q3_3_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}

/******************************************************************** 
 *
 * SSB Q3_4
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q3_4(const int /* xct_id */, 
                            q3_4_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}



/******************************************************************** 
 *
 * SSB Q4_1
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q4_1(const int /* xct_id */, 
                            q4_1_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB Q4_2
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q4_2(const int /* xct_id */, 
                            q4_2_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}


/******************************************************************** 
 *
 * SSB Q4_3
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_q4_3(const int /* xct_id */, 
                            q4_3_input_t& /* in */)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    return (RC(smlevel_0::eNOTIMPLEMENTED));
}




EXIT_NAMESPACE(ssb);
