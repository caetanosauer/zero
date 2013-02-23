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

/** @file:   shore_tpch_env.h
 *
 *  @brief:  Definition of the Shore TPC-H environment
 *
 *  @author: Nastaran Nikparto, Summer 2011
 *  @author: Ippokratis Pandis (ipandis)
 */

#ifndef __SHORE_TPCH_ENV_H
#define __SHORE_TPCH_ENV_H

#include "sm_vas.h"
#include "util.h"

#include "sm/shore/shore_env.h"
#include "sm/shore/shore_asc_sort_buf.h"
#include "sm/shore/shore_trx_worker.h"

#include "workload/tpch/tpch_const.h"

#include "workload/tpch/shore_tpch_schema_man.h"
#include "workload/tpch/tpch_input.h"

#ifdef CFG_QPIPE
#include "qpipe.h"
#endif

#include <map>

using namespace shore;


ENTER_NAMESPACE(tpch);


using std::map;



// Sets the scaling factor of the TPC-H database
// @note Some data structures base their size on this value

const double TPCH_SCALING_FACTOR = 1;


// For the population
const long CUST_POP_UNIT = 10;
const long PART_POP_UNIT = 10;

/* ---------------------------------------------------------------
 *
 * @struct : row_data_t 
 *
 * @brief : this struct is for keep intermediate result of the join operation
 *
 * --------------------------------------------------------------- */

class row_data_t
{
public:
    vector<table_row_t *> rows;
};

/****************************************************************** 
 *
 *  @struct: ShoreTPCHEnv Stats
 *
 *  @brief:  TPCH Environment statistics
 *
 ******************************************************************/

struct ShoreTPCHTrxCount 
{
    uint q1;
    uint q2;
    uint q3;
    uint q4;
    uint q5;
    uint q6;
    uint q7;
    uint q8;
    uint q9;
    uint q10;
    uint q11;
    uint q12;
    uint q13;
    uint q14;
    uint q15;
    uint q16;
    uint q17;
    uint q18;
    uint q19;
    uint q20;
    uint q21;
    uint q22;
    uint qlineitem;
    uint qnation;
    uint qregion;
    uint qorders;
    uint qpart;
    uint qpartsupp;
    uint qsupplier;
    uint qcustomer;

    uint qNP;


    ShoreTPCHTrxCount& operator+=(ShoreTPCHTrxCount const& rhs) {
        q1 += rhs.q1; 
        q2 += rhs.q2; 
        q3 += rhs.q3; 
        q4 += rhs.q4; 
        q5 += rhs.q5; 
        q6 += rhs.q6; 
        q7 += rhs.q7; 
        q8 += rhs.q8; 
        q9 += rhs.q9; 
        q10 += rhs.q10; 
        q11 += rhs.q11; 
        q12 += rhs.q12; 
        q13 += rhs.q13; 
        q14 += rhs.q14; 
        q15 += rhs.q15; 
        q16 += rhs.q16; 
        q17 += rhs.q17; 
        q18 += rhs.q18; 
        q19 += rhs.q19; 
        q20 += rhs.q20; 
        q21 += rhs.q21; 
        q22 += rhs.q22;
        qlineitem += rhs.qlineitem;
        qnation += rhs.qnation;
        qregion += rhs.qregion;
        qorders += rhs.qorders;
        qpart += rhs.qpart;
        qpartsupp += rhs.qpartsupp;
        qsupplier += rhs.qsupplier;
        qcustomer += rhs.qcustomer;

        qNP += rhs.qNP;

        return (*this);
    }

    ShoreTPCHTrxCount& operator-=(ShoreTPCHTrxCount const& rhs) {
        q1 -= rhs.q1; 
        q2 -= rhs.q2; 
        q3 -= rhs.q3; 
        q4 -= rhs.q4; 
        q5 -= rhs.q5; 
        q6 -= rhs.q6; 
        q7 -= rhs.q7; 
        q8 -= rhs.q8; 
        q9 -= rhs.q9; 
        q10 -= rhs.q10; 
        q11 -= rhs.q11; 
        q12 -= rhs.q12; 
        q13 -= rhs.q13; 
        q14 -= rhs.q14; 
        q15 -= rhs.q15; 
        q16 -= rhs.q16; 
        q17 -= rhs.q17; 
        q18 -= rhs.q18; 
        q19 -= rhs.q19; 
        q20 -= rhs.q20; 
        q21 -= rhs.q21; 
        q22 -= rhs.q22; 
        qlineitem -= rhs.qlineitem;
        qnation -= rhs.qnation;
        qregion -= rhs.qregion;
        qorders -= rhs.qorders;
        qpart -= rhs.qpart;
        qpartsupp -= rhs.qpartsupp;
        qsupplier -= rhs.qsupplier;
        qcustomer -= rhs.qcustomer;
        
        qNP -= rhs.qNP;
        
        return (*this);
    }

    uint total() const {
        return (q1+q2+q3+q4+q5+q6+q7+q8+q9+q10+
                q11+q12+q13+q14+q15+q16+q17+q18+q19+q20+
                q21+q22+qlineitem+qnation+qregion+qorders+qpart+qpartsupp+qsupplier+qcustomer+
                qNP);
    }

}; // EOF: ShoreTPCHTrxCount


struct ShoreTPCHTrxStats
{
    ShoreTPCHTrxCount attempted;
    ShoreTPCHTrxCount failed;
    ShoreTPCHTrxCount deadlocked;

    ShoreTPCHTrxStats& operator+=(ShoreTPCHTrxStats const& other) {
        attempted  += other.attempted;
        failed     += other.failed;
        deadlocked += other.deadlocked;
        return (*this);
    }

    ShoreTPCHTrxStats& operator-=(ShoreTPCHTrxStats const& other) {
        attempted  -= other.attempted;
        failed     -= other.failed;
        deadlocked -= other.deadlocked;
        return (*this);
    }

}; // EOF: ShoreTPCHTrxStats


/******************************************************************** 
 * 
 *  ShoreTPCHEnv
 *  
 *  Shore TPC-H Database.
 *
 ********************************************************************/

class ShoreTPCHEnv : public ShoreEnv
{
public:
    typedef std::map<pthread_t, ShoreTPCHTrxStats*> statmap_t;

    class table_builder_t;
    class table_creator_t;
    struct checkpointer_t;

private:

    w_rc_t _post_init_impl();

    // Helper functions for the loading
    w_rc_t _gen_one_nation(const int id, rep_row_t& areprow);
    w_rc_t _gen_one_region(const int id, rep_row_t& areprow);
    w_rc_t _gen_one_supplier(const int id, rep_row_t& areprow);
    w_rc_t _gen_one_part_based(const int id, rep_row_t& areprow);
    w_rc_t _gen_one_cust_based(const int id, rep_row_t& areprow);
    
public:    
    ShoreTPCHEnv();
    virtual ~ShoreTPCHEnv();

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
    
    // TPCH Tables
    DECLARE_TABLE(nation_t,nation_man_impl,nation);
    DECLARE_TABLE(region_t,region_man_impl,region);
    DECLARE_TABLE(part_t,part_man_impl,part);
    DECLARE_TABLE(supplier_t,supplier_man_impl,supplier);
    DECLARE_TABLE(partsupp_t,partsupp_man_impl,partsupp);
    DECLARE_TABLE(customer_t,customer_man_impl,customer);
    DECLARE_TABLE(orders_t,orders_man_impl,orders);
    DECLARE_TABLE(lineitem_t,lineitem_man_impl,lineitem);
    

    // --- kit baseline trxs --- //

    w_rc_t run_one_xct(Request* prequest);

    // QUERIES (Transactions)
    DECLARE_TRX(q1);
    DECLARE_TRX(q2);
    DECLARE_TRX(q3);
    DECLARE_TRX(q4);
    DECLARE_TRX(q5);
    DECLARE_TRX(q6);
    DECLARE_TRX(q7);
    DECLARE_TRX(q8);
    DECLARE_TRX(q9);
    DECLARE_TRX(q10);
    DECLARE_TRX(q11);
    DECLARE_TRX(q12);
    DECLARE_TRX(q13);
    DECLARE_TRX(q14);
    DECLARE_TRX(q15);
    DECLARE_TRX(q16);
    DECLARE_TRX(q17);
    DECLARE_TRX(q18);
    DECLARE_TRX(q19);
    DECLARE_TRX(q20);
    DECLARE_TRX(q21);
    DECLARE_TRX(q22);
    DECLARE_TRX(qlineitem);
    DECLARE_TRX(qorders);
    DECLARE_TRX(qnation);
    DECLARE_TRX(qregion);
    DECLARE_TRX(qpart);
    DECLARE_TRX(qsupplier);
    DECLARE_TRX(qpartsupp);
    DECLARE_TRX(qcustomer);
    
    // QUERIES for the non-partition aligned benchmark
    DECLARE_TRX(qNP);

    // Database population
    DECLARE_TRX(populate_baseline);
    DECLARE_TRX(populate_some_parts);
    DECLARE_TRX(populate_some_custs);

    //nasi

    template<class T1_man, class T2_man, class T1_desc, class T2_desc>
    w_rc_t
    join_1_table(guard<T1_man> , guard<T2_man> ,
		 guard<T1_desc>, guard<T2_desc>,
		 vector<row_data_t*>& );


    template<class T_man, class T_desc>
    w_rc_t
    join_2_table(guard<T_man> , guard<T_desc> ,
		 vector<row_data_t *> ,
		 vector<row_data_t*> );

    template<class T> void f() {}
    //

#ifdef CFG_QPIPE
private:
    guard<policy_t> _sched_policy;

public:
    policy_t* get_sched_policy();
    policy_t* set_sched_policy(const char* spolicy);
    w_rc_t run_one_qpipe_xct(Request* prequest);

    // QPipe QUERIES (Transactions)
    DECLARE_QPIPE_TRX(q1);
    DECLARE_QPIPE_TRX(q2);
    DECLARE_QPIPE_TRX(q3);
    DECLARE_QPIPE_TRX(q4);
    DECLARE_QPIPE_TRX(q5);
    DECLARE_QPIPE_TRX(q6);
    DECLARE_QPIPE_TRX(q7);
    DECLARE_QPIPE_TRX(q8);
    DECLARE_QPIPE_TRX(q9);
    DECLARE_QPIPE_TRX(q10);
    DECLARE_QPIPE_TRX(q11);
    DECLARE_QPIPE_TRX(q12);
    DECLARE_QPIPE_TRX(q13);
    DECLARE_QPIPE_TRX(q14);
    DECLARE_QPIPE_TRX(q15);
    DECLARE_QPIPE_TRX(q16);
    DECLARE_QPIPE_TRX(q17);
    DECLARE_QPIPE_TRX(q18);
    DECLARE_QPIPE_TRX(q19);
    DECLARE_QPIPE_TRX(q20);
    DECLARE_QPIPE_TRX(q21);
    DECLARE_QPIPE_TRX(q22);
    DECLARE_QPIPE_TRX(qlineitem);
    DECLARE_QPIPE_TRX(qorders);
    DECLARE_QPIPE_TRX(qnation);
    DECLARE_QPIPE_TRX(qregion);
    DECLARE_QPIPE_TRX(qpart);
    DECLARE_QPIPE_TRX(qsupplier);
    DECLARE_QPIPE_TRX(qpartsupp);
    DECLARE_QPIPE_TRX(qcustomer);
#endif


    // for thread-local stats
    virtual void env_thread_init();
    virtual void env_thread_fini();   

    // stat map
    statmap_t _statmap;

    // snapshot taken at the beginning of each experiment    
    ShoreTPCHTrxStats _last_stats;
    virtual void reset_stats();
    ShoreTPCHTrxStats _get_stats();
    
}; // EOF ShoreTPCHEnv


template<class T_man, class T_desc>
w_rc_t
ShoreTPCHEnv::join_2_table(guard<T_man> _t_m, guard<T_desc> _t_d, vector<row_data_t *> setOfRow, vector<row_data_t *> result)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //we need to touch t1
   
    table_row_t* t_r = _t_m->get_tuple();
    assert(t_r);
    
    w_rc_t e =RCOK;
    
    //allocate space for t1 represenration
    rep_row_t areprow(_t_m->ts());
    areprow.set(_t_d->maxsize());

    t_r->_rep = &areprow;
    guard< table_scan_iter_impl<T_desc> > l_iter;
    { 
	table_scan_iter_impl<T_desc>* t;
        e = _t_m->get_iter_for_file_scan(_pssm, t);
        l_iter = t;
        if (e.is_error()) { goto done; }
    }
    bool eof;
   
    e = l_iter->next(_pssm, eof, *t_r);

    while(!eof){
       for(int r = 0; r < setOfRow.size(); r++){
	//join setOfRow[r] with t
	   
	   row_data_t * joined_row = new  row_data_t(*setOfRow[r]);
	   joined_row->rows.push_back(t_r);
	   result.push_back(joined_row);

       }
    e = l_iter->next(_pssm, eof, *t_r);
    if (e.is_error()) {goto done;}
    
    }
    
    return (RCOK);
}

template<class T1_man, class T2_man, class T1_desc, class T2_desc>
 w_rc_t
ShoreTPCHEnv::join_1_table(guard<T1_man> _t1_m, guard<T2_man> _t2_m, guard<T1_desc>_t1_d, guard<T2_desc>_t2_d, vector<row_data_t*>& result)
{

    
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    //we need to touch t1
    table_row_t* t1_r = _t1_m->get_tuple();
    assert(t1_r);
    
    w_rc_t e1 =RCOK;
    
    //allocate space for t1 represenration
    rep_row_t areprow1(_t1_m->ts());
    areprow1.set(_t1_d->maxsize());

    t1_r->_rep = &areprow1;

    //now we need to touch t2
    table_row_t* t2_r = _t2_m->get_tuple();
    assert(t2_r);
    w_rc_t e2 =RCOK;
        
    //allocate space for t2 represenration
    rep_row_t areprow2(_t2_m->ts());
    areprow2.set(_t2_d->maxsize());

    t2_r->_rep = &areprow2;
    int c =0;
    guard< table_scan_iter_impl<T1_desc> > l_iter1;
    { 
	table_scan_iter_impl<T1_desc>* t1;
        e1 = _t1_m->get_iter_for_file_scan(_pssm, t1);
        l_iter1 = t1;
        if (e1.is_error()) { goto done; }
    }
    bool eof1;
   
   e1 = l_iter1->next(_pssm, eof1, *t1_r);
   
   while(!eof1){

       guard< table_scan_iter_impl<T2_desc> > l_iter2;
	{ 
	   table_scan_iter_impl<T2_desc>* t2;
           e2 = _t2_m->get_iter_for_file_scan(_pssm, t2);
           l_iter2 = t2;
           if (e2.is_error()) { goto done; }
	}
	bool eof2;
   
	e2 = l_iter2->next(_pssm, eof2, *t2_r);

	while(!eof2){
		// now we have t1_r and t2_row, so lets make instance of row_data
	    
	    c++;
	    table_row_t* r1 = new table_row_t(*t1_r);
	    table_row_t* r2 = new table_row_t(*t2_r);

	    row_data_t* j = new row_data_t;
	    j->rows.push_back(r1);
	    j->rows.push_back(r2);

	    result.push_back(j);
	    
	    e2 = l_iter2->next(_pssm, eof2, *t2_r);
	    if (e2.is_error()) {goto done;}
	}

	e1 = l_iter1->next(_pssm, eof1, *t1_r);
	if (e1.is_error()) {goto done;}
   }
   
 done:
   cout<<"*******num of tuple : "<<c<<endl;
   _t1_m->give_tuple(t1_r);
   _t2_m->give_tuple(t2_r);
   return (RCOK); //pin: maybe return an error here if things aren't ok  

}


EXIT_NAMESPACE(tpch);


#endif /* __SHORE_TPCH_ENV_H */
