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

/** @file:   shore_client_env.cpp
 *
 *  @brief:  Implementation base class for Shore clients
 *
 *  @author: Ippokratis Pandis, July 2008
 */

#include "sm/shore/shore_client.h"

ENTER_NAMESPACE(shore);


/********************************************************************* 
 *
 *  @fn:    abort/resume_test
 *
 *  @brief: Used for SIGINT functionality
 *
 *********************************************************************/

static bool _abort_test = false;
void base_client_t::abort_test() 
{
    _abort_test = true;
}
void base_client_t::resume_test() 
{
    _abort_test = false;
}
bool base_client_t::is_test_aborted() 
{
    return (_abort_test);
}


/********************************************************************* 
 *
 *  @fn:    submit_batch
 *
 *  @brief: Submits a batch of trxs and always uses the last trx to
 *          wait on its cond var
 *
 *********************************************************************/

w_rc_t base_client_t::submit_batch(int xct_type, int& trx_cnt, const int batch_sz) 
{       
    assert (batch_sz);
    assert (_cp);
    for(int j=1; j <= batch_sz; j++) {

        // adding think time
        //usleep(int(_think_time*(sthread_t::drand())));

        if (j == batch_sz) 
	    _cp->please_take_one();
        W_COERCE(submit_one(xct_type, trx_cnt++));
    }
    return (RCOK);
}

static pthread_mutex_t client_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t client_cond = PTHREAD_COND_INITIALIZER;
static int client_needed_count;
extern void shell_expect_clients(int count) {
    client_needed_count = count;
}
extern void shell_await_clients() {
    CRITICAL_SECTION(cs, client_mutex);
    while(client_needed_count)
	pthread_cond_wait(&client_cond, &client_mutex);
}
void client_ready() {
    CRITICAL_SECTION(cs, client_mutex);
    if(! --client_needed_count)
	pthread_cond_signal(&client_cond);
}

/********************************************************************* 
 *
 *  @fn:    run_xcts
 *
 *  @brief: Entry point for running the trxs 
 *
 *********************************************************************/

w_rc_t base_client_t::run_xcts(int xct_type, int num_xct)
{
    int i=0;
    int batchsz=1;

    client_ready();
    
    // retrieve the default batch size and think time
    envVar* ev = envVar::instance();
    batchsz = ev->getVarInt("db-cl-batchsz",BATCH_SIZE);
    _think_time = ev->getVarInt("db-cl-thinktime",THINK_TIME);

    if ((_think_time>0) && (batchsz>1)) {
        TRACE( TRACE_ALWAYS, "error: Batchsz=%d && ThinkTime=%d\n", 
               batchsz, _think_time);
        assert(0);
    }


    // If in DORA (or at least not in Baseline) allocate an empty sdesc cache
    // so that the xct does not allocate one. The DORA workers will do that.
    string sysname = ev->getSysName();
    if (sysname.compare("baseline")!=0) {
        me()->alloc_sdesc_cache();
    }
    

    switch (_measure_type) {

        // case of number-of-trxs-based measurement
    case (MT_NUM_OF_TRXS):

        // submit a batch of num_xct trxs
        W_COERCE(submit_batch(xct_type, i, num_xct));
        // wait for the batch to complete
        _cp->wait();
        break;

        // case of duration-based measurement
    case (MT_TIME_DUR):
	
	// submit the first two batches...
	W_COERCE(submit_batch(xct_type, i, batchsz));
	W_COERCE(submit_batch(xct_type, i, batchsz));

	// main loop
        while (true) {
	    // wait for the first to complete
	    _cp->wait();

	    // check for exit...
	    if(_abort_test || _env->get_measure() == MST_DONE)
		break;

	    // submit a replacement batch...
	    W_COERCE(submit_batch(xct_type, i, batchsz));
        }
	
	// wait for the last batch to complete...
	_cp->wait();	
        break;

    default:
        assert (0); // UNSUPPORTED MEASUREMENT TYPE
        break;
    }
    TRACE( TRACE_TRX_FLOW, "Exiting...\n");
    if (sysname.compare("baseline")!=0) {
        me()->free_sdesc_cache();
    }

    return (RCOK);
}


EXIT_NAMESPACE(shore);





