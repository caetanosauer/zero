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

/** @file:   shore_tpcb_env.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-B transactions
 *
 *  @author: Ryan Johnson      (ryanjohn)
 *  @author: Ippokratis Pandis (ipandis)
 *  @date:   Feb 2009
 */

#include "workload/tpcb/shore_tpcb_env.h"

#include <vector>
#include <numeric>
#include <algorithm>

using namespace shore;


ENTER_NAMESPACE(tpcb);



/******************************************************************** 
 *
 * Thread-local TPC-B TRXS Stats
 *
 ********************************************************************/

static __thread ShoreTPCBTrxStats my_stats;

void ShoreTPCBEnv::env_thread_init()
{
    CRITICAL_SECTION(stat_mutex_cs, _statmap_mutex);
    _statmap[pthread_self()] = &my_stats;
}

void ShoreTPCBEnv::env_thread_fini()
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

ShoreTPCBTrxStats ShoreTPCBEnv::_get_stats()
{
    CRITICAL_SECTION(cs, _statmap_mutex);
    ShoreTPCBTrxStats rval;
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

void ShoreTPCBEnv::reset_stats()
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

void ShoreTPCBEnv::print_throughput(const double iQueriedSF, 
                                    const int iSpread, 
                                    const int iNumOfThreads, 
                                    const double delay,
                                    const ulong_t mioch,
                                    const double avgcpuusage)
{
    CRITICAL_SECTION(last_stats_cs, _last_stats_mutex);

    // get the current statistics
    ShoreTPCBTrxStats current_stats = _get_stats();
    
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
 * TPC-B TRXS
 *
 * (1) The run_XXX functions are wrappers to the real transactions
 * (2) The xct_XXX functions are the implementation of the transactions
 *
 ********************************************************************/


/********************************************************************* 
 *
 *  @fn:    run_one_xct
 *
 *  @brief: Initiates the execution of one TPC-B xct
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure internal has expired.
 *
 *********************************************************************/

w_rc_t ShoreTPCBEnv::run_one_xct(Request* prequest)
{
    assert (prequest);

    if(_start_imbalance > 0 && !_bAlarmSet) {
	CRITICAL_SECTION(alarm_cs, _alarm_lock);
	if(!_bAlarmSet) {
	    alarm(_start_imbalance);
	    _bAlarmSet = true;
	}
    }

    int rand;

    switch (prequest->type()) {

	// TPC-B BASELINE
     case XCT_TPCB_ACCT_UPDATE:
	 return (run_acct_update(prequest));

	 // MBENCH BASELINE
     case XCT_TPCB_MBENCH_INSERT_ONLY:
	 return (run_mbench_insert_only(prequest));
     case XCT_TPCB_MBENCH_DELETE_ONLY:
	 return (run_mbench_delete_only(prequest));
     case XCT_TPCB_MBENCH_PROBE_ONLY:
	 return (run_mbench_probe_only(prequest));
     case XCT_TPCB_MBENCH_INSERT_DELETE:
        if (URand(1,100)>_delete_freq)
            return (run_mbench_insert_only(prequest));
        else
            return (run_mbench_delete_only(prequest));
	//return (run_mbench_insert_delete(prequest));
     case XCT_TPCB_MBENCH_INSERT_PROBE:
        if (URand(1,100)>_probe_freq)
            return (run_mbench_insert_only(prequest));
        else
            return (run_mbench_probe_only(prequest));
	// return (run_mbench_insert_probe(prequest));
     case XCT_TPCB_MBENCH_DELETE_PROBE:
        if (URand(1,100)>_delete_freq)
            return (run_mbench_probe_only(prequest));
        else
            return (run_mbench_delete_only(prequest));
	// return (run_mbench_delete_probe(prequest));
     case XCT_TPCB_MBENCH_MIX:
	 rand = URand(1,100); 
        if (rand<=_insert_freq)
            return (run_mbench_insert_only(prequest));
        else if(rand<=_insert_freq+_probe_freq)
            return (run_mbench_probe_only(prequest));	 
	else 
	    return (run_mbench_delete_only(prequest));
	// return (run_mbench_mix(prequest));

     default:
	 assert (0); // UNKNOWN TRX-ID
     }
    return (RCOK);
}



/******************************************************************** 
 *
 * TPC-B TRXs Wrappers
 *
 * @brief: They are wrappers to the functions that execute the transaction
 *         body. Their responsibility is to:
 *
 *         1. Prepare the corresponding input
 *         2. Check the return of the trx function and abort the trx,
 *            if something went wrong
 *         3. Update the tpcb db environment statistics
 *
 ********************************************************************/


DEFINE_TRX(ShoreTPCBEnv,acct_update);
DEFINE_TRX(ShoreTPCBEnv,populate_db);

// microbenchmarks
DEFINE_TRX(ShoreTPCBEnv,mbench_insert_only);
DEFINE_TRX(ShoreTPCBEnv,mbench_delete_only);
DEFINE_TRX(ShoreTPCBEnv,mbench_probe_only);
DEFINE_TRX(ShoreTPCBEnv,mbench_insert_delete);
DEFINE_TRX(ShoreTPCBEnv,mbench_insert_probe);
DEFINE_TRX(ShoreTPCBEnv,mbench_delete_probe);
DEFINE_TRX(ShoreTPCBEnv,mbench_mix);


// uncomment the line below if want to dump (part of) the trx results
//#define PRINT_TRX_RESULTS


/******************************************************************** 
 *
 * TPC-B Acct Update
 *
 ********************************************************************/

w_rc_t ShoreTPCBEnv::xct_acct_update(const int /* xct_id */, 
                                     acct_update_input_t& ppin)
{
    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    
    // account update trx touches 4 tables:
    // branch, teller, account, and history

    // get table tuples from the caches
    tuple_guard<branch_man_impl> prb(_pbranch_man);
    tuple_guard<teller_man_impl> prt(_pteller_man);
    tuple_guard<account_man_impl> pracct(_paccount_man);
    tuple_guard<history_man_impl> prhist(_phistory_man);

    rep_row_t areprow(_paccount_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_paccount_desc->maxsize()); 

    prb->_rep = &areprow;
    prt->_rep = &areprow;
    pracct->_rep = &areprow;
    prhist->_rep = &areprow;

    double total;
    
    // 1. Update account
    W_DO(_paccount_man->a_index_probe_forupdate(_pssm, pracct, ppin.a_id));
    pracct->get_value(2, total);
    pracct->set_value(2, total + ppin.delta);
    W_DO(_paccount_man->update_tuple(_pssm, pracct));
    
    // 2. Write to History
    prhist->set_value(0, ppin.b_id);
    prhist->set_value(1, ppin.t_id);
    prhist->set_value(2, ppin.a_id);
    prhist->set_value(3, ppin.delta);
    prhist->set_value(4, time(NULL));
#ifdef CFG_HACK
    prhist->set_value(5, "padding"); // PADDING
#endif
    W_DO(_phistory_man->add_tuple(_pssm, prhist));
    
    // 3. Update teller
    W_DO(_pteller_man->t_index_probe_forupdate(_pssm, prt, ppin.t_id));
    prt->get_value(2, total);
    prt->set_value(2, total + ppin.delta);
    W_DO(_pteller_man->update_tuple(_pssm, prt));
    
    // 4. Update branch
    W_DO(_pbranch_man->b_index_probe_forupdate(_pssm, prb, ppin.b_id));
    prb->get_value(1, total);
    prb->set_value(1, total + ppin.delta);
    W_DO(_pbranch_man->update_tuple(_pssm, prb));
    
#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prb->print_tuple();
    prt->print_tuple();
    pracct->print_tuple();
    prhist->print_tuple();
#endif
    
    return RCOK;

} // EOF: ACCT UPDATE




/******************************************************************** 
 *
 * TPCB POPULATE_DB
 *
 * @brief: When called for the first time,
 *         populates the BRANCH and TELLER tables.
 *         Then populates ACCOUNTS.
 *
 ********************************************************************/

w_rc_t ShoreTPCBEnv::xct_populate_db(const int /* xct_id */, 
                                     populate_db_input_t& ppin)
{
    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);

    // account update trx touches 4 tables:
    // branch, teller, account, and history
    
    // get table tuples from the caches
    tuple_guard<branch_man_impl> prb(_pbranch_man);
    tuple_guard<teller_man_impl> prt(_pteller_man);
    tuple_guard<account_man_impl> pracct(_paccount_man);
    tuple_guard<history_man_impl> prhist(_phistory_man);

    rep_row_t areprow(_paccount_man->ts());
    rep_row_t areprow_key(_paccount_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_paccount_desc->maxsize());
    areprow_key.set(_paccount_desc->maxsize());

    prb->_rep = &areprow;
    prt->_rep = &areprow;
    pracct->_rep = &areprow;
    prhist->_rep = &areprow;
    prb->_rep_key = &areprow_key;
    prt->_rep_key = &areprow_key;
    pracct->_rep_key = &areprow_key;
    prhist->_rep_key = &areprow_key;

    if(ppin._first_a_id < 0) {
	// Populate the branches and tellers
	for(int i=0; i < ppin._sf; i++) {
	    prb->set_value(0, i);
	    prb->set_value(1, 0.0);
#ifdef CFG_HACK
	    prb->set_value(2, "padding"); // PADDING
#endif
	    W_DO(_pbranch_man->add_tuple(_pssm, prb));
	}
	TRACE( TRACE_STATISTICS, "Loaded %d branches\n", ppin._sf);
	
	for(int i=0; i < ppin._sf*TPCB_TELLERS_PER_BRANCH; i++) {
	    prt->set_value(0, i);
	    prt->set_value(1, i/TPCB_TELLERS_PER_BRANCH);
	    prt->set_value(2, 0.0);
#ifdef CFG_HACK
	    prt->set_value(3, "padding"); // PADDING
#endif
	    W_DO(_pteller_man->add_tuple(_pssm, prt));
	}
	TRACE( TRACE_STATISTICS, "Loaded %d tellers\n",
	       ppin._sf*TPCB_TELLERS_PER_BRANCH);
    } else {
	// Populate 10k accounts
	for(int i=0; i < TPCB_ACCOUNTS_CREATED_PER_POP_XCT; i++) {
	    int a_id = ppin._first_a_id +
		(TPCB_ACCOUNTS_CREATED_PER_POP_XCT-i-1);
	    pracct->set_value(0, a_id);
	    pracct->set_value(1, a_id/TPCB_ACCOUNTS_PER_BRANCH);
	    pracct->set_value(2, 0.0);	    
#ifdef CFG_HACK
	    pracct->set_value(3, "padding"); // PADDING
#endif
	    W_DO(_paccount_man->add_tuple(_pssm, pracct));
	}
    }
    // The database loader which calls this xct does not use the xct wrapper,
    // so it should do the commit here
    W_DO(_pssm->commit_xct());
    
#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prb->print_tuple();
    prt->print_tuple();
    pracct->print_tuple();
    prhist->print_tuple();
#endif

    return RCOK;

} // EOF: POPULATE




/******************************************************************** 
 *
 * TPC-B MBench Insert Only
 *
 ********************************************************************/

w_rc_t ShoreTPCBEnv::xct_mbench_insert_only(const int /* xct_id */, 
					    mbench_insert_only_input_t& mioin)
{
    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    
    // mbench insert only trx touches 1 table:
    // accounts

    // get table tuples from the caches
    tuple_guard<account_man_impl> pracct(_paccount_man);

    rep_row_t areprow(_paccount_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_paccount_desc->maxsize()); 

    pracct->_rep = &areprow;

    // 1. insert a tupple to accounts
    pracct->set_value(0, mioin.a_id);
    pracct->set_value(1, mioin.b_id);
    pracct->set_value(2, mioin.balance);
#ifdef CFG_HACK
    pracct->set_value(3, "padding");
#endif
    W_DO(_paccount_man->add_tuple(_pssm, pracct));

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    pracct->print_tuple();
#endif

    return RCOK;

} // EOF: MBENCH INSERT ONLY




/******************************************************************** 
 * 
 * TPC-B MBench Delete Only
 *
 ********************************************************************/

w_rc_t ShoreTPCBEnv::xct_mbench_delete_only(const int /* xct_id */, 
					    mbench_delete_only_input_t& mdoin)
{
    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // mbench insert only trx touches 1 table:
    // accounts

    // get table tuples from the caches
    tuple_guard<account_man_impl> pracct(_paccount_man);

    rep_row_t areprow(_paccount_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_paccount_desc->maxsize()); 

    pracct->_rep = &areprow;

    // 1. delete a tupple from accounts
    W_DO(_paccount_man->a_delete_by_index(_pssm, pracct, mdoin.a_id,
					  mdoin.b_id, mdoin.balance));

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    pracct->print_tuple();
#endif

    return RCOK;

} // EOF: MBENCH DELETE ONLY




/******************************************************************** 
 *
 * TPC-B MBench Probe Only
 *
 ********************************************************************/

w_rc_t ShoreTPCBEnv::xct_mbench_probe_only(const int /* xct_id */, 
					   mbench_probe_only_input_t& mpoin)
{
    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // mbench insert only trx touches 1 table:
    // accounts

    // get table tuples from the caches
    tuple_guard<account_man_impl> pracct(_paccount_man);

    rep_row_t areprow(_paccount_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_paccount_desc->maxsize()); 

    pracct->_rep = &areprow;

    // 1. retrieve a tupple from accounts
    w_rc_t e = _paccount_man->a_index_probe(_pssm, pracct, mpoin.a_id,
					    mpoin.b_id, mpoin.balance);
    if (e.is_error() && (e.err_num() != se_TUPLE_NOT_FOUND)) { 
	W_DO(e); 
    }
    
    return RCOK;

} // EOF: MBENCH PROBE ONLY



// TODO: has to be updated if decided to be used
/******************************************************************** 
 *
 * TPC-B MBench Insert Delete
 *
 ********************************************************************/
w_rc_t ShoreTPCBEnv::xct_mbench_insert_delete(const int /* xct_id */, 
					      mbench_insert_delete_input_t& midin)
{
    w_rc_t e = RCOK;

    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // mbench insert delete trx touches 1 table:
    // branch

    // get table tuples from the caches
    table_row_t* prb = _pbranch_man->get_tuple();
    assert (prb);

    rep_row_t areprow(_paccount_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_paccount_desc->maxsize()); 
    prb->_rep = &areprow;

    { // make gotos safe

	// 1. insert a tupple to branches
	prb->set_value(0, midin.b_id);
	prb->set_value(1, 0.0);
	prb->set_value(2, "padding");
	e = _pbranch_man->add_tuple(_pssm, prb);
	if (e.is_error()) { goto done; }

	// 2. delete a tupple from branches
	e = _pbranch_man->delete_tuple(_pssm, prb);
	if (e.is_error()) { goto done; }

    } // goto

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prb->print_tuple();
#endif

done:
    // return the tuples to the cache
    _pbranch_man->give_tuple(prb);
    return (e);

} // EOF: MBENCH INSERT DELETE




/******************************************************************** 
 *
 * TPC-B MBench Insert Probe
 *
 ********************************************************************/
w_rc_t ShoreTPCBEnv::xct_mbench_insert_probe(const int /* xct_id */, 
					     mbench_insert_probe_input_t& mipin)
{
    w_rc_t e = RCOK;

    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // mbench insert probe trx touches 1 table:
    // branch

    // get table tuples from the caches
    table_row_t* prb = _pbranch_man->get_tuple();
    assert (prb);

    rep_row_t areprow(_paccount_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_paccount_desc->maxsize()); 
    prb->_rep = &areprow;

    { // make gotos safe

	// 1. insert a tupple to branches
	prb->set_value(0, mipin.b_id);
	prb->set_value(1, 0.0);
	prb->set_value(2, "padding");
	e = _pbranch_man->add_tuple(_pssm, prb);
	if (e.is_error()) { goto done; }

	// 2. retrive the inserted tupple from branches
	e = _pbranch_man->b_index_probe(_pssm, prb, mipin.b_id);
	if (e.is_error()) { goto done; }

    } // goto

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prb->print_tuple();
#endif

done:
    // return the tuples to the cache
    _pbranch_man->give_tuple(prb);
    return (e);

} // EOF: MBENCH INSERT PROBE




/******************************************************************** 
 *
 * TPC-B MBench Delete Probe
 *
 ********************************************************************/
w_rc_t ShoreTPCBEnv::xct_mbench_delete_probe(const int /* xct_id */, 
					     mbench_delete_probe_input_t& mdpin)
{
    w_rc_t e = RCOK;

    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // mbench delete probe trx touches 1 table:
    // branch

    // get table tuples from the caches
    table_row_t* prb = _pbranch_man->get_tuple();
    assert (prb);

    rep_row_t areprow(_paccount_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_paccount_desc->maxsize()); 
    prb->_rep = &areprow;

    { // make gotos safe

	// 1. retrive a tupple from branches
	e = _pbranch_man->b_index_probe(_pssm, prb, mdpin.b_id);
	if (e.is_error()) { goto done; }

	// 2. delete a tupple from branches
	e = _pbranch_man->delete_tuple(_pssm, prb);
	if (e.is_error()) { goto done; }
	
    } // goto

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prb->print_tuple();
#endif

done:
    // return the tuples to the cache
    _pbranch_man->give_tuple(prb);
    return (e);

} // EOF: MBENCH DELETE PROBE




/******************************************************************** 
 *
 * TPC-B MBench Mix
 *
 ********************************************************************/
w_rc_t ShoreTPCBEnv::xct_mbench_mix(const int /* xct_id */, 
				    mbench_mix_input_t& mmin)
{
    w_rc_t e = RCOK;

    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // mbench mix trx touches 1 table:
    // branch

    // get table tuples from the caches
    table_row_t* prb = _pbranch_man->get_tuple();
    assert (prb);

    rep_row_t areprow(_paccount_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_paccount_desc->maxsize()); 
    prb->_rep = &areprow;

    { // make gotos safe

	// 1. insert a tupple to branches
	prb->set_value(0, mmin.b_id);
	prb->set_value(1, 0.0);
	prb->set_value(2, "padding");
	e = _pbranch_man->add_tuple(_pssm, prb);
	if (e.is_error()) { goto done; }

	// 2. retrive the inserted tupple from branches
	e = _pbranch_man->b_index_probe(_pssm, prb, mmin.b_id);
	if (e.is_error()) { goto done; }

	// 3. delete a tupple from branches
	e = _pbranch_man->delete_tuple(_pssm, prb);
	if (e.is_error()) { goto done; }

    } // goto

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prb->print_tuple();
#endif

done:
    // return the tuples to the cache
    _pbranch_man->give_tuple(prb);
    return (e);

} // EOF: MBENCH MIX




EXIT_NAMESPACE(tpcb);
