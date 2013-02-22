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

/** @file:   shore_tpcc_xct.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-C transactions
 *
 *  @author: Ippokratis Pandis (ipandis)
 */

#include "workload/tpcc/shore_tpcc_env.h"
#include "workload/tpcc/tpcc_random.h"

#include <vector>
#include <numeric>
#include <algorithm>

using namespace shore;


ENTER_NAMESPACE(tpcc);



/******************************************************************** 
 *
 * Thread-local TPC-C TRXS Stats
 *
 ********************************************************************/


static __thread ShoreTPCCTrxStats my_stats;

void ShoreTPCCEnv::env_thread_init()
{
    CRITICAL_SECTION(stat_mutex_cs, _statmap_mutex);
    _statmap[pthread_self()] = &my_stats;
}

void ShoreTPCCEnv::env_thread_fini()
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

ShoreTPCCTrxStats ShoreTPCCEnv::_get_stats()
{
    CRITICAL_SECTION(cs, _statmap_mutex);
    ShoreTPCCTrxStats rval;
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

void ShoreTPCCEnv::reset_stats()
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

void ShoreTPCCEnv::print_throughput(const double iQueriedSF, 
                                    const int iSpread, 
                                    const int iNumOfThreads, 
                                    const double delay,
                                    const ulong_t mioch,
                                    const double avgcpuusage)
{
    CRITICAL_SECTION(last_stats_cs, _last_stats_mutex);

    // get the current statistics
    ShoreTPCCTrxStats current_stats = _get_stats();
    
    // now calculate the diff
    current_stats -= _last_stats;
        
    uint trxs_att  = current_stats.attempted.total();
    uint trxs_abt  = current_stats.failed.total();
    uint trxs_dld  = current_stats.deadlocked.total();    
    int nords_com = current_stats.attempted.new_order - current_stats.failed.new_order - current_stats.deadlocked.new_order;

    TRACE( TRACE_ALWAYS, "*******\n"                \
           "QueriedSF: (%.1f)\n"                    \
           "Spread:    (%s)\n"                      \
           "Threads:   (%d)\n"                      \
           "Trxs Att:  (%d)\n"                      \
           "Trxs Abt:  (%d)\n"                      \
           "Trxs Dld:  (%d)\n"                      \
           "NOrd Com:  (%d)\n"                      \
           "Secs:      (%.2f)\n"                    \
           "IOChars:   (%.2fM/s)\n"                 \
           "AvgCPUs:   (%.1f) (%.1f%%)\n"           \
           "TPS:       (%.2f)\n"                    \
           "tpm-C:     (%.2f)\n",
           iQueriedSF, 
           (iSpread ? "Yes" : "No"),
           iNumOfThreads, trxs_att, trxs_abt, trxs_dld, nords_com, 
           delay, mioch/delay, avgcpuusage, 
           100*avgcpuusage/get_max_cpu_count(),
           (trxs_att-trxs_abt-trxs_dld)/delay,
           60*nords_com/delay);
}




/******************************************************************** 
 *
 * TPC-C Parallel Loading
 *
 ********************************************************************/

/*
  DATABASE POPULATION TRANSACTIONS

  All tables other than ITEM (100k entries) scale with the number of warehouses:

  District	10
  New Order	9k
  Customer	30k
  History	30k
  Order		30k
  Stock		100k
  Order Line	300k
  
  Other than District we can easily divide the dataset into 1000ths of
  a warehouse, leading to the following unit of work:

  9 NO, 30 {CUST, HIST, ORDER}, 100 STOCK, 300 OLINE
*/
#define A_C_ID          1023
#define C_C_ID          127
#define C_C_LAST        173
#define C_OL_I_ID       1723
#define A_C_LAST        255
#define A_OL_I_ID       8191

static char alnum[] =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

static const char *last_name_parts[] =
    {
	"BAR",
	"OUGHT",
	"ABLE",
	"PRI",
	"PRES",
	"ESE",
	"ANTI",
	"CALLY",
	"ATION",
	"EING"
    };

static int rand_integer(int lo, int hi) {
    return (URand(lo,hi));
    //return sthread_t::randn(hi-lo+1) + lo;
}

/** @fn: create_random_a_string
 *
 *  @brief create a random alphanumeric string, of random length between lo and
 *  hi and place them in designated buffer. Routine returns the actual
 *  length.
 *
 *  @param lo - end of acceptable length range
 *  @param hi - end of acceptable length range
 *  
 *  @output actual length
 *  @output random alphanumeric string
 */

static int create_random_a_string( char *out_buffer, int length_lo, int length_hi ) {
    int i, actual_length ;

    actual_length = rand_integer( length_lo, length_hi ) ;

    for (i = 0; i < actual_length; i++ ) {
	out_buffer[i] = alnum[rand_integer( 0, 61 )] ;
    }
    out_buffer[actual_length] = '\0' ;

    return (actual_length);
}


/** @fn: create_random_n_string
 *
 *  @brief:  create a random numeric string, of random length between lo and
 *  hi and place them in designated buffer. Routine returns the actual
 *  length.
 *
 *  @param lo - end of acceptable length range
 *  @param hi - end of acceptable length range
 *
 *  @output actual length
 *  @output random numeric string
 */
static int create_random_n_string( char *out_buffer, int length_lo, int length_hi ) {
    int i, actual_length ;

    actual_length = rand_integer( length_lo, length_hi ) ;

    for (i = 0; i < actual_length; i++ )
	{
	    out_buffer[i] = (char)rand_integer( 48,57 ) ;
	}
    out_buffer[actual_length] = '\0' ;

    return (actual_length);
}


// @fn: NUrand_val
int NUrand_val ( int A, int x, int y, int C ) {
    return((((rand_integer(0,A)|rand_integer(x,y))+C)%(y-x+1))+x);
}


/** @fn: create_a_string_with_original
 * 
 *  @brief: create a random alphanumeric string, of random length between lo and
 *  hi and place them in designated buffer. Routine returns the actual
 *  length. The word "ORIGINAL" is placed at a random location in the buffer at
 *  random, for a given percent of the records. 
 *
 *  @note: Cannot use on strings of length less than 8. Lower limit must be > 8
 */

int create_a_string_with_original( char *out_buffer, int length_lo,
                                   int length_hi, int percent_to_set )
{
    int actual_length, start_pos ;

    actual_length = create_random_a_string( out_buffer, length_lo, length_hi ) ;

    if ( rand_integer( 1, 100 ) <= percent_to_set )
	{
	    start_pos = rand_integer( 0, actual_length-8 ) ;
	    strncpy(out_buffer+start_pos,"ORIGINAL",8) ;
	}

    return (actual_length);
}


/** @fn: create_random_last_name
 *
 *  @brief: create_random_last_name generates a random number from 0 to 999
 *    inclusive. a random name is generated by associating a random string
 *    with each digit of the generated number. the three strings are
 *    concatenated to generate the name
 */

int create_random_last_name(char *out_buffer, int cust_num) 
{
    int random_num;

    if (cust_num == 0)
	random_num = NUrand_val( A_C_LAST, 0, 999, C_C_LAST );    //@d261133mte
    else
	random_num = cust_num - 1;

    strcpy(out_buffer, last_name_parts[random_num / 100]);
    random_num %= 100;
    strcat(out_buffer, last_name_parts[random_num / 10]);
    random_num %= 10;
    strcat(out_buffer, last_name_parts[random_num]);

    return(strlen(out_buffer));
}


w_rc_t ShoreTPCCEnv::xct_populate_baseline(const int /* xct_id */, 
                                           populate_baseline_input_t& pbin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);

    tuple_guard<warehouse_man_impl> prwh(_pwarehouse_man);
    tuple_guard<district_man_impl> prdist(_pdistrict_man);

    // not sure which of the Warehouse and District is bigger, 
    // but customer is bigger than both
    rep_row_t areprow(_pcustomer_man->ts());
    rep_row_t areprow_key(_pcustomer_man->ts());

    areprow.set(_pcustomer_desc->maxsize());
    areprow_key.set(_pcustomer_desc->maxsize());

    prwh->_rep = &areprow;
    prdist->_rep = &areprow;
    prwh->_rep_key = &areprow_key;
    prdist->_rep_key = &areprow_key;

    // generate warehouses, districts and ITEM tables
    int wh = pbin._wh;
    for(int i=1; i <= wh; i++) {
	for(int j=0; j <= DISTRICTS_PER_WAREHOUSE; j++) {
	    char name[11];
	    char street_1[21];
	    char street_2[21];
	    char city[21];
	    char state[3];
	    char zip[10];
	    double tax = rand_integer(0, 2000)/10000.0;
	    
	    create_random_a_string( name,      6,10) ; // create name
	    create_random_a_string( street_1, 10,20) ; // create street 1
	    create_random_a_string( street_2, 10,20) ; // create street 2
	    create_random_a_string( city,     10,20) ; // create city
	    create_random_a_string( state,     2,2) ;  // create state
	    create_random_n_string( zip,       4,4) ;  // create zip
	    strcat(zip, "11111");

	    if(j == 0) {
		// warehouse
		prwh->set_value(0, i);
		prwh->set_value(1, name);
		prwh->set_value(2, street_1);
		prwh->set_value(3, street_2);
		prwh->set_value(4, city);
		prwh->set_value(5, state);
		prwh->set_value(6, zip);
		prwh->set_value(7, tax);
		prwh->set_value(8, 300000.0);
		W_DO(_pwarehouse_man->add_tuple(_pssm, prwh));
	    } else {
		// dist
		prdist->set_value(0, j);
		prdist->set_value(1, i);
		prdist->set_value(2, name);
		prdist->set_value(3, street_1);
		prdist->set_value(4, street_2);
		prdist->set_value(5, city);
		prdist->set_value(6, state);
		prdist->set_value(7, zip);
		prdist->set_value(8, tax);
		prdist->set_value(9, 300000.0);
		prdist->set_value(10, CUSTOMERS_PER_DISTRICT+1);
		W_DO(_pdistrict_man->add_tuple(_pssm, prdist));
	    }
	}
    }
    
    // Should do the commit here, called by the loader
    W_DO(_pssm->commit_xct());
    
    return RCOK;
}


w_rc_t ShoreTPCCEnv::xct_populate_one_unit(const int /* xct_id */, 
                                           populate_one_unit_input_t& pbuin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);

    tuple_guard<customer_man_impl> prcust(_pcustomer_man);
    tuple_guard<new_order_man_impl> prno(_pnew_order_man);
    tuple_guard<order_man_impl> prord(_porder_man);
    tuple_guard<item_man_impl> pritem(_pitem_man);
    tuple_guard<history_man_impl> prhist(_phistory_man);
    tuple_guard<stock_man_impl> prst(_pstock_man);
    tuple_guard<order_line_man_impl> prol(_porder_line_man);
    
    rep_row_t areprow(_pcustomer_man->ts());
    rep_row_t areprow_key(_pcustomer_man->ts());

    // allocate space for the biggest of the 8 table representations
    areprow.set(_pcustomer_desc->maxsize());
    areprow_key.set(_pcustomer_desc->maxsize()); 

    prcust->_rep = &areprow;
    prno->_rep = &areprow;
    prord->_rep = &areprow;
    prhist->_rep = &areprow;
    prst->_rep = &areprow;
    prol->_rep = &areprow;
    pritem->_rep = &areprow;
    prcust->_rep_key = &areprow_key;
    prno->_rep_key = &areprow_key;
    prord->_rep_key = &areprow_key;
    prhist->_rep_key = &areprow_key;
    prst->_rep_key = &areprow_key;
    prol->_rep_key = &areprow_key;
    pritem->_rep_key = &areprow_key;

    int unit = pbuin._unit;

    // ORDER, NORD, and OLINE (must be done together)
    double currtmstmp = time(0);
    int wid = unit/UNIT_PER_WH + 1;
    int did = ((unit/UNIT_PER_DIST) % DISTRICTS_PER_WAREHOUSE) + 1;
    int base_oid = ((unit*ORDERS_PER_UNIT) % ORDERS_PER_DIST) + 1;
    for(int i=0; i < ORDERS_PER_UNIT; i++) {
	int oid = base_oid + i;
	int o_cid = pbuin._cids[oid-1];
	// this one goes in the nord table, needs a non-null carrier
	bool is_new = (oid > 2100);
	int carrier = is_new? 0 : rand_integer(1, 10);
	int olines = rand_integer(MIN_OL_PER_ORDER, MAX_OL_PER_ORDER);
	int all_local = true;	
	// olines
	for(int j=1; j <= olines; j++) {
	    char dist_info[25];
	    int item_num = rand_integer(1, ITEMS);
	    create_random_a_string(dist_info, 24, 24);
	    int amount = rand_integer(0, 999999);
	    // insert oline
	    prol->set_value(0, oid);
	    prol->set_value(1, did);
	    prol->set_value(2, wid);
	    prol->set_value(3, j);
	    prol->set_value(4, item_num);
	    prol->set_value(5, wid);
	    prol->set_value(6, is_new? 0.0 : currtmstmp);
	    prol->set_value(7, 5);
	    prol->set_value(8, amount);
	    prol->set_value(9, dist_info);
	    W_DO(_porder_line_man->add_tuple(_pssm, prol));
	}
	// insert order
	prord->set_value(0, oid);
	prord->set_value(1, o_cid);
	prord->set_value(2, did);
	prord->set_value(3, wid);
	prord->set_value(4, currtmstmp);
	prord->set_value(5, carrier);
	prord->set_value(6, olines);
	prord->set_value(7, all_local);
	W_DO(_porder_man->add_tuple(_pssm, prord));
	// insert new order
	if(is_new) {
	    prno->set_value(0, oid);
	    prno->set_value(1, did);
	    prno->set_value(2, wid);
	    W_DO(_pnew_order_man->add_tuple(_pssm, prno));
	}
    }
    
    // STOCK
    int stock_base = ((unit*STOCK_PER_UNIT) % STOCK_PER_WAREHOUSE) + 1;
    for(int i=0; i < STOCK_PER_UNIT; i++) {
	char stock_dist[10][25];
	char stock_data[51];
	int stock_num = stock_base + i;
	int qty = rand_integer(10, 100);
	create_a_string_with_original(stock_data, 26, 50, 10);
	for(uint_t j=0; j < sizeof(stock_dist)/sizeof(stock_dist[0]); j++) {
	    create_random_a_string(stock_dist[j], 24, 24);
	}
	// insert stock
	prst->set_value(0, stock_num);
	prst->set_value(1, wid);
	prst->set_value(2, (int)0);
	prst->set_value(3, qty);
	prst->set_value(4, (int)0);
	prst->set_value(5, (int)0);
	for(int k=0; k < 10; k++) {
	    prst->set_value(6+k, stock_dist[k]);
	}
	prst->set_value(16, stock_data);
	W_DO(_pstock_man->add_tuple(_pssm, prst));
	
	// ITEM
	if(wid == 1) {
	    char item_name[25];
	    char item_data[51];
	    int im_id = rand_integer(1, 10000);
	    int item_price = rand_integer(100, 10000);
	    create_random_a_string( item_name, 14, 24);
	    create_a_string_with_original( item_data, 26, 50, 10) ;    
	    // insert item
	    pritem->set_value(0, stock_num);
	    pritem->set_value(1, im_id);
	    pritem->set_value(2, item_name);
	    pritem->set_value(3, item_price);
	    pritem->set_value(4, item_data);	    
	    W_DO(_pitem_man->add_tuple(_pssm, pritem));
	}
    }
    
    // HIST
    int cid_base = ((unit*CUST_PER_UNIT) % CUSTOMERS_PER_DISTRICT) + 1;
    for(int i=0; i < CUST_PER_UNIT; i++) {
	char hist_data[25] ;
	int cid = cid_base+i;
	double amount = 1000; // $10.00
	create_random_a_string(hist_data, 12, 24);
	// insert hist
	prhist->set_value(0, cid);
	prhist->set_value(1, did);
	prhist->set_value(2, wid);
	prhist->set_value(3, did);
	prhist->set_value(4, did);
	prhist->set_value(5, currtmstmp);
	prhist->set_value(6, amount);
	prhist->set_value(7, hist_data);
	W_DO(_phistory_man->add_tuple(_pssm, prhist));
    }

    // CUSTOMER
    for(int i=0; i < CUST_PER_UNIT; i++) {
	char cust_last[17];
	//char cust_middle[3];
	char cust_first[17];
	char cust_street_1[21];
	char cust_street_2[21];
	char cust_city[21];
	char cust_state[3];
	char cust_zip[10];
	char cust_phone[17];
	char cust_credit[3];
	char cust_data1[251];
	char cust_data2[251];
	double cust_discount = rand_integer(0, 5000)/10000.0;
	int cid = cid_base + i;
	
	create_random_last_name(cust_last, (cid <= 1000)? cid : 0);
	create_random_a_string( cust_first,     8,16) ; // create first name
	create_random_a_string( cust_street_1, 10,20) ; // create street 1
	create_random_a_string( cust_street_2, 10,20) ; // create street 2
	create_random_a_string( cust_city,     10,20) ; // create city
	create_random_a_string( cust_state,     2,2) ;  // create state
	create_random_n_string( cust_zip,       4,4) ;  // create zip
	strcat(cust_zip, "11111");
	create_random_n_string( cust_phone, 16,16) ;
	strcpy( cust_credit, (rand_integer( 1, 100 ) <= 10)? "BC" : "GC");
	create_random_a_string( cust_data1, 250, 250);
	create_random_a_string( cust_data2, 50,  250);	
	// insert cust
	prcust->set_value(0, cid);
	prcust->set_value(1, did);
	prcust->set_value(2, wid);
	prcust->set_value(3, cust_first);
	prcust->set_value(4, "OE");
	prcust->set_value(5, cust_last);
	prcust->set_value(6, cust_street_1);
	prcust->set_value(7, cust_street_2);
	prcust->set_value(8, cust_city);
	prcust->set_value(9, cust_state);
	prcust->set_value(10, cust_zip);
	prcust->set_value(11, cust_phone);
	prcust->set_value(12, currtmstmp);
	prcust->set_value(13, cust_credit);
	prcust->set_value(14, 50000.0);
	prcust->set_value(15, cust_discount);
	prcust->set_value(16, 0.0);
	prcust->set_value(17, -10.0);
	prcust->set_value(18, 10.0);
	prcust->set_value(19, 1);
	prcust->set_value(20, cust_data1);
	prcust->set_value(21, cust_data2);	
	W_DO(_pcustomer_man->add_tuple(_pssm, prcust));
    }
    
    // Should do the commit here, called by the loaded
    W_DO(_pssm->commit_xct());

    return RCOK;
} 



/******************************************************************** 
 *
 * TPC-C TRXS
 *
 * (1) The run_XXX functions are wrappers to the real transactions
 * (2) The xct_XXX functions are the implementation of the transactions
 *
 ********************************************************************/


/********************************************************************* 
 *
 *  @fn:    run_one_xct
 *
 *  @brief: Initiates the execution of one TPC-C xct
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure internal has expired.
 *
 *********************************************************************/

w_rc_t ShoreTPCCEnv::run_one_xct(Request* prequest)
{
    if(_start_imbalance > 0 && !_bAlarmSet) {
	CRITICAL_SECTION(alarm_cs, _alarm_lock);
	if(!_bAlarmSet) {
	    alarm(_start_imbalance);
	    _bAlarmSet = true;
	}
    }

    // if BASELINE TPC-C MIX
    if (prequest->type() == XCT_MIX) {
        prequest->set_type(random_xct_type(abs(smthread_t::me()->rand()%100)));
    }
    
    switch (prequest->type()) {
        
        // TPC-C BASELINE
    case XCT_NEW_ORDER:
        return (run_new_order(prequest));
    case XCT_PAYMENT:
        return (run_payment(prequest));;
    case XCT_ORDER_STATUS:
        return (run_order_status(prequest));
    case XCT_DELIVERY:
        return (run_delivery(prequest));
    case XCT_STOCK_LEVEL:
        return (run_stock_level(prequest));

        // Little Mix (NewOrder/Payment 50%-50%)
    case XCT_LITTLE_MIX:
        if (URand(1,100)>50)
            return (run_new_order(prequest));
        else
            return (run_payment(prequest));


        // MBENCH BASELINE
    case XCT_MBENCH_WH:
        return (run_mbench_wh(prequest));
    case XCT_MBENCH_CUST:
        return (run_mbench_cust(prequest)); 
    default:
        assert (0); // UNKNOWN TRX-ID
    }
    return (RCOK);
}




/******************************************************************** 
 *
 * TPC-C TRXs Wrappers
 *
 * @brief: They are wrappers to the functions that execute the transaction
 *         body. Their responsibility is to:
 *
 *         1. Prepare the corresponding input
 *         2. Check the return of the trx function and abort the trx,
 *            if something went wrong
 *         3. Update the tpcc db environment statistics
 *
 ********************************************************************/


// --- without input specified --- //

DEFINE_TRX(ShoreTPCCEnv,new_order);
DEFINE_TRX(ShoreTPCCEnv,payment);
DEFINE_TRX(ShoreTPCCEnv,order_status);
DEFINE_TRX(ShoreTPCCEnv,delivery);
DEFINE_TRX(ShoreTPCCEnv,stock_level);
DEFINE_TRX(ShoreTPCCEnv,mbench_wh);
DEFINE_TRX(ShoreTPCCEnv,mbench_cust);


// uncomment the line below if want to dump (part of) the trx results
//#define PRINT_TRX_RESULTS


/******************************************************************** 
 *
 * TPC-C NEW_ORDER
 *
 ********************************************************************/

w_rc_t ShoreTPCCEnv::xct_new_order(const int xct_id, 
				   new_order_input_t& pnoin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    
    // new_order trx touches 8 tables:
    // warehouse, district, customer, neworder, order, item, stock, orderline
    tuple_guard<warehouse_man_impl> prwh(_pwarehouse_man);
    tuple_guard<district_man_impl> prdist(_pdistrict_man);
    tuple_guard<customer_man_impl> prcust(_pcustomer_man);
    tuple_guard<new_order_man_impl> prno(_pnew_order_man);
    tuple_guard<order_man_impl> prord(_porder_man);
    tuple_guard<item_man_impl> pritem(_pitem_man);
    tuple_guard<stock_man_impl> prst(_pstock_man);
    tuple_guard<order_line_man_impl> prol(_porder_line_man);

    rep_row_t areprow(_pcustomer_man->ts());

    // allocate space for the biggest of the 8 table representations
    areprow.set(_pcustomer_desc->maxsize()); 

    prwh->_rep = &areprow;
    prdist->_rep = &areprow;
    prcust->_rep = &areprow;
    prno->_rep = &areprow;
    prord->_rep = &areprow;
    pritem->_rep = &areprow;
    prst->_rep = &areprow;
    prol->_rep = &areprow;


    /* SELECT c_discount, c_last, c_credit, w_tax
     * FROM customer, warehouse
     * WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id
     *
     * plan: index probe on "W_IDX", index probe on "C_IDX"
     */

    // 1. retrieve warehouse (read-only)
    TRACE( TRACE_TRX_FLOW, "App: %d NO:wh-idx-probe (%d)\n", 
	   xct_id, pnoin._wh_id);
    W_DO(_pwarehouse_man->wh_index_probe(_pssm, prwh, pnoin._wh_id));

    tpcc_warehouse_tuple awh;
    prwh->get_value(7, awh.W_TAX);


    /* SELECT d_tax, d_next_o_id
     * FROM district
     * WHERE d_id = :d_id AND d_w_id = :w_id
     *
     * plan: index probe on "D_IDX"
     */
    
    // 2. retrieve district for update
    TRACE( TRACE_TRX_FLOW, "App: %d NO:dist-idx-upd (%d) (%d)\n", 
	   xct_id, pnoin._wh_id, pnoin._d_id);
    W_DO(_pdistrict_man->dist_index_probe_forupdate(_pssm, prdist,
						    pnoin._wh_id, pnoin._d_id));
    
    tpcc_district_tuple adist;
    prdist->get_value(8, adist.D_TAX);
    prdist->get_value(10, adist.D_NEXT_O_ID);
    adist.D_NEXT_O_ID++;

    // 3. retrieve customer
    TRACE( TRACE_TRX_FLOW, "App: %d NO:cust-idx-probe (%d) (%d) (%d)\n", 
	   xct_id, pnoin._wh_id, pnoin._d_id, pnoin._c_id);
    W_DO(_pcustomer_man->cust_index_probe(_pssm, prcust, pnoin._wh_id, 
					  pnoin._d_id, pnoin._c_id));
    
    tpcc_customer_tuple  acust;
    prcust->get_value(15, acust.C_DISCOUNT);
    prcust->get_value(13, acust.C_CREDIT, 3);
    prcust->get_value(5, acust.C_LAST, 17);
    
    
    /* UPDATE district
     * SET d_next_o_id = :next_o_id+1
     * WHERE CURRENT OF dist_cur
     */
    
    TRACE( TRACE_TRX_FLOW, "App: %d NO:dist-upd-next-o-id (%d)\n", 
	   xct_id, adist.D_NEXT_O_ID);
    W_DO(_pdistrict_man->dist_update_next_o_id(_pssm, prdist,
					       adist.D_NEXT_O_ID));

    double total_amount = 0;

    for (int item_cnt=0; item_cnt<pnoin._ol_cnt; item_cnt++) {

	// 4. for all items read item, and update stock, and order line
	int ol_i_id = pnoin.items[item_cnt]._ol_i_id;
	int ol_supply_w_id = pnoin.items[item_cnt]._ol_supply_wh_id;
	
	
	/* SELECT i_price, i_name, i_data
	 * FROM item
	 * WHERE i_id = :ol_i_id
	 *
	 * plan: index probe on "I_IDX"
	 */
	
	tpcc_item_tuple aitem;
	TRACE( TRACE_TRX_FLOW, "App: %d NO:item-idx-probe (%d)\n", 
	       xct_id, ol_i_id);
	W_DO(_pitem_man->it_index_probe(_pssm, pritem, ol_i_id));
	
	pritem->get_value(4, aitem.I_DATA, 51);
	pritem->get_value(3, aitem.I_PRICE);
	pritem->get_value(2, aitem.I_NAME, 25);
	
	int item_amount = aitem.I_PRICE * pnoin.items[item_cnt]._ol_quantity; 
	total_amount += item_amount;
	//info->items[item_cnt].ol_amount = amount;
	
	
	/* SELECT s_quantity, s_remote_cnt, s_data,
	 *        s_dist0, s_dist1, s_dist2, ...
	 * FROM stock
	 * WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id
	 *
	 * plan: index probe on "S_IDX"
	 */
	
	tpcc_stock_tuple astock;
	TRACE( TRACE_TRX_FLOW, "App: %d NO:stock-idx-upd (%d) (%d)\n", 
	       xct_id, ol_supply_w_id, ol_i_id);
	W_DO(_pstock_man->st_index_probe_forupdate(_pssm, prst,
						   ol_supply_w_id, ol_i_id));
	prst->get_value(0, astock.S_I_ID);
	prst->get_value(1, astock.S_W_ID);
	prst->get_value(5, astock.S_YTD);
	astock.S_YTD += pnoin.items[item_cnt]._ol_quantity;
	prst->get_value(2, astock.S_REMOTE_CNT);        
	prst->get_value(3, astock.S_QUANTITY);
	astock.S_QUANTITY -= pnoin.items[item_cnt]._ol_quantity;
	if (astock.S_QUANTITY < 10) astock.S_QUANTITY += 91;
	//prst->get_value(6+pnoin._d_id, astock.S_DIST[6+pnoin._d_id], 25);
	prst->get_value(6+pnoin._d_id, astock.S_DIST[pnoin._d_id], 25);
	prst->get_value(16, astock.S_DATA, 51);

	char c_s_brand_generic;
	if (strstr(aitem.I_DATA, "ORIGINAL") != NULL && 
	    strstr(astock.S_DATA, "ORIGINAL") != NULL)
	    c_s_brand_generic = 'B';
	else c_s_brand_generic = 'G';
	
	prst->get_value(4, astock.S_ORDER_CNT);
	astock.S_ORDER_CNT++;
	
	if (pnoin._wh_id != ol_supply_w_id) {
	    astock.S_REMOTE_CNT++;
	}
	
	
	/* UPDATE stock
	 * SET s_quantity = :s_quantity, s_order_cnt = :s_order_cnt
	 * WHERE s_w_id = :w_id AND s_i_id = :ol_i_id;
	 */
	
	TRACE( TRACE_TRX_FLOW, "App: %d NO:stock-upd-tuple (%d) (%d)\n", 
	       xct_id, astock.S_W_ID, astock.S_I_ID);
	W_DO(_pstock_man->st_update_tuple(_pssm, prst, &astock));
	
	
	/* INSERT INTO order_line
	 * VALUES (o_id, d_id, w_id, ol_ln, ol_i_id, supply_w_id,
	 *        '0001-01-01-00.00.01.000000', ol_quantity, iol_amount, dist)
	 */
	
	prol->set_value(0, adist.D_NEXT_O_ID);
	prol->set_value(1, pnoin._d_id);
	prol->set_value(2, pnoin._wh_id);
	prol->set_value(3, item_cnt+1);
	prol->set_value(4, ol_i_id);
	prol->set_value(5, ol_supply_w_id);
	prol->set_value(6, pnoin._tstamp);
	prol->set_value(7, pnoin.items[item_cnt]._ol_quantity);
	prol->set_value(8, item_amount);	
	//prol->set_value(9, astock.S_DIST[6+pnoin._d_id]);
	prol->set_value(9, astock.S_DIST[pnoin._d_id]);
	TRACE( TRACE_TRX_FLOW, "App: %d NO:ol-add-tuple (%d) (%d) (%d) (%d)\n", 
	       xct_id, pnoin._wh_id, pnoin._d_id,
	       adist.D_NEXT_O_ID, item_cnt+1);
	W_DO(_porder_line_man->add_tuple(_pssm, prol));
    } // end for loop
        
    
    /* INSERT INTO orders
     * VALUES (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
     */

    // 5. insert row to orders and new_order
    prord->set_value(0, adist.D_NEXT_O_ID);
    prord->set_value(1, pnoin._c_id);
    prord->set_value(2, pnoin._d_id);
    prord->set_value(3, pnoin._wh_id);
    prord->set_value(4, pnoin._tstamp);
    prord->set_value(5, 0);
    prord->set_value(6, pnoin._ol_cnt);
    prord->set_value(7, pnoin._all_local);
    TRACE( TRACE_TRX_FLOW, "App: %d NO:ord-add-tuple (%d)\n", 
	   xct_id, adist.D_NEXT_O_ID);
    W_DO(_porder_man->add_tuple(_pssm, prord));

    
    /* INSERT INTO new_order VALUES (o_id, d_id, w_id) */
    
    prno->set_value(0, adist.D_NEXT_O_ID);
    prno->set_value(1, pnoin._d_id);
    prno->set_value(2, pnoin._wh_id);
    TRACE( TRACE_TRX_FLOW, "App: %d NO:nord-add-tuple (%d) (%d) (%d)\n", 
	   xct_id, pnoin._wh_id, pnoin._d_id, adist.D_NEXT_O_ID);
    W_DO(_pnew_order_man->add_tuple(_pssm, prno));

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prwh->print_tuple();
    prdist->print_tuple();
    prcust->print_tuple();
    prno->print_tuple();
    prord->print_tuple();
    pritem->print_tuple();
    prst->print_tuple();
    prol->print_tuple();
#endif

    return RCOK;

} // EOF: NEW_ORDER




/******************************************************************** 
 *
 * TPC-C PAYMENT
 *
 ********************************************************************/

w_rc_t ShoreTPCCEnv::xct_payment(const int xct_id, 
                                 payment_input_t& ppin)
{
    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    
    // payment trx touches 4 tables: 
    // warehouse, district, customer, and history
    
    // get table tuples from the caches
    tuple_guard<warehouse_man_impl> prwh(_pwarehouse_man);
    tuple_guard<district_man_impl> prdist(_pdistrict_man);
    tuple_guard<customer_man_impl> prcust(_pcustomer_man);
    tuple_guard<history_man_impl> prhist(_phistory_man);

    rep_row_t areprow(_pcustomer_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_pcustomer_desc->maxsize()); 

    prwh->_rep = &areprow;
    prdist->_rep = &areprow;
    prcust->_rep = &areprow;
    prhist->_rep = &areprow;

    // 1. retrieve warehouse for update
    TRACE( TRACE_TRX_FLOW, "App: %d PAY:wh-idx-upd (%d)\n", 
	   xct_id, ppin._home_wh_id);
    W_DO(_pwarehouse_man->wh_index_probe_forupdate(_pssm, prwh, 
						   ppin._home_wh_id));
    
    // 2. retrieve district for update
    TRACE( TRACE_TRX_FLOW, "App: %d PAY:dist-idx-upd (%d) (%d)\n", 
	   xct_id, ppin._home_wh_id, ppin._home_d_id);
    W_DO(_pdistrict_man->dist_index_probe_forupdate(_pssm, prdist,
						    ppin._home_wh_id,
						    ppin._home_d_id));
    
    // 3. retrieve customer for update

    // find the customer wh and d
    int c_w = (ppin._v_cust_wh_selection > 85 ? ppin._home_wh_id : ppin._remote_wh_id);
    int c_d = (ppin._v_cust_wh_selection > 85 ? ppin._home_d_id : ppin._remote_d_id);
    
    if (ppin._v_cust_ident_selection <= 60) {

	// 3a. if no customer selected already use the
	//     index on the customer name

	
	/* SELECT  c_id, c_first
	 * FROM customer
	 * WHERE c_last = :c_last AND c_w_id = :c_w_id AND c_d_id = :c_d_id
	 * ORDER BY c_first
	 *
	 * plan: index only scan on "C_NAME_IDX"
	 */

	rep_row_t lowrep(_pcustomer_man->ts());
	rep_row_t highrep(_pcustomer_man->ts());
	
	guard< index_scan_iter_impl<customer_t> > c_iter;
	{
	    index_scan_iter_impl<customer_t>* tmp_c_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d PAY:cust-iter-by-name-idx (%s)\n", 
		   xct_id, ppin._c_last);
	    W_DO(_pcustomer_man->cust_get_iter_by_index(_pssm, tmp_c_iter,
							prcust,	lowrep, highrep,
							c_w, c_d,
							ppin._c_last));
	    c_iter = tmp_c_iter;
	}

	vector<int> v_c_id;
	int a_c_id = 0;
	int count = 0;
	bool eof;
	
	W_DO(c_iter->next(_pssm, eof, *prcust));
	while (!eof) {
	    // push the retrieved customer id to the vector
	    ++count;
	    prcust->get_value(0, a_c_id);
	    v_c_id.push_back(a_c_id);
	    TRACE( TRACE_TRX_FLOW, "App: %d PAY:cust-iter-next (%d)\n", 
		   xct_id, a_c_id);
	    W_DO(c_iter->next(_pssm, eof, *prcust));
	}
	assert (count);	

	// 3b. find the customer id in the middle of the list
	ppin._c_id = v_c_id[(count+1)/2-1];
    }
    assert (ppin._c_id>0);


    /* SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, 
     * c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, 
     * c_discount, c_balance, c_ytd_payment, c_payment_cnt 
     * FROM customer 
     * WHERE c_id = :c_id AND c_w_id = :c_w_id AND c_d_id = :c_d_id 
     * FOR UPDATE OF c_balance, c_ytd_payment, c_payment_cnt
     *
     * plan: index probe on "C_IDX"
     */
    
    TRACE( TRACE_TRX_FLOW, "App: %d PAY:cust-idx-upd (%d) (%d) (%d)\n", 
	   xct_id, c_w, c_d, ppin._c_id);
    W_DO(_pcustomer_man->cust_index_probe_forupdate(_pssm, prcust, 
						    c_w, c_d, ppin._c_id));

    //double c_balance, c_ytd_payment;
    //int    c_payment_cnt;
    tpcc_customer_tuple acust;
    
    // retrieve customer
    prcust->get_value(3,  acust.C_FIRST, 17);
    prcust->get_value(4,  acust.C_MIDDLE, 3);
    prcust->get_value(5,  acust.C_LAST, 17);
    prcust->get_value(6,  acust.C_STREET_1, 21);
    prcust->get_value(7,  acust.C_STREET_2, 21);
    prcust->get_value(8,  acust.C_CITY, 21);
    prcust->get_value(9,  acust.C_STATE, 3);
    prcust->get_value(10, acust.C_ZIP, 10);
    prcust->get_value(11, acust.C_PHONE, 17);
    prcust->get_value(12, acust.C_SINCE);
    prcust->get_value(13, acust.C_CREDIT, 3);
    prcust->get_value(14, acust.C_CREDIT_LIM);
    prcust->get_value(15, acust.C_DISCOUNT);
    prcust->get_value(16, acust.C_BALANCE);
    prcust->get_value(17, acust.C_YTD_PAYMENT);
    prcust->get_value(18, acust.C_LAST_PAYMENT);
    prcust->get_value(19, acust.C_PAYMENT_CNT);
    prcust->get_value(20, acust.C_DATA_1, 251);
    prcust->get_value(21, acust.C_DATA_2, 251);
    
    // update customer fields
    acust.C_BALANCE -= ppin._h_amount;
    acust.C_YTD_PAYMENT += ppin._h_amount;
    acust.C_PAYMENT_CNT++;
    
    // if bad customer
    if (acust.C_CREDIT[0] == 'B' && acust.C_CREDIT[1] == 'C') { 
	// 10% of customers
	
	/* SELECT c_data
	 * FROM customer 
	 * WHERE c_id = :c_id AND c_w_id = :c_w_id AND c_d_id = :c_d_id
	 * FOR UPDATE OF c_balance, c_ytd_payment, c_payment_cnt, c_data
	 *
	 * plan: index probe on "C_IDX"
	 */
	
	// update the data
	char c_new_data_1[251];
	char c_new_data_2[251];
	sprintf(c_new_data_1, "%d,%d,%d,%d,%d,%1.2f",
		ppin._c_id, c_d, c_w, ppin._home_d_id, 
		ppin._home_wh_id, ppin._h_amount);
	
	int len = strlen(c_new_data_1);
	strncat(c_new_data_1, acust.C_DATA_1, 250-len);
	strncpy(c_new_data_2, &acust.C_DATA_1[250-len], len);
	strncpy(c_new_data_2, acust.C_DATA_2, 250-len);
	
	TRACE( TRACE_TRX_FLOW, "App: %d PAY:cust-upd-tuple\n", xct_id);
	W_DO(_pcustomer_man->cust_update_tuple(_pssm, prcust, acust, 
					       c_new_data_1, c_new_data_2));
    } else { // good customer
	TRACE( TRACE_TRX_FLOW, "App: %d PAY:cust-upd-tuple\n", xct_id);
	W_DO(_pcustomer_man->cust_update_tuple(_pssm, prcust, acust,
					       NULL, NULL));
    }

    
    /* UPDATE district SET d_ytd = d_ytd + :h_amount
     * WHERE d_id = :d_id AND d_w_id = :w_id
     *
     * SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
     * FROM district
     * WHERE d_id = :d_id AND d_w_id = :w_id
     *
     * plan: index probe on "D_IDX"
     */
    
    TRACE( TRACE_TRX_FLOW, "App: %d PAY:dist-upd-ytd (%d) (%d)\n", 
	   xct_id, ppin._home_wh_id, ppin._home_d_id);
    W_DO(_pdistrict_man->dist_update_ytd(_pssm, prdist, ppin._h_amount));

    tpcc_district_tuple adistr;
    prdist->get_value(2, adistr.D_NAME, 11);
    prdist->get_value(3, adistr.D_STREET_1, 21);
    prdist->get_value(4, adistr.D_STREET_2, 21);
    prdist->get_value(5, adistr.D_CITY, 21);
    prdist->get_value(6, adistr.D_STATE, 3);
    prdist->get_value(7, adistr.D_ZIP, 10);
    
    
    /* UPDATE warehouse SET w_ytd = wytd + :h_amount
     * WHERE w_id = :w_id
     *
     * SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip
     * FROM warehouse
     * WHERE w_id = :w_id
     *
     * plan: index probe on "W_IDX"
     */
    
    TRACE( TRACE_TRX_FLOW, "App: %d PAY:wh-update-ytd (%d)\n", 
	   xct_id, ppin._home_wh_id);
    W_DO(_pwarehouse_man->wh_update_ytd(_pssm, prwh, ppin._h_amount));

    tpcc_warehouse_tuple awh;
    prwh->get_value(1, awh.W_NAME, 11);
    prwh->get_value(2, awh.W_STREET_1, 21);
    prwh->get_value(3, awh.W_STREET_2, 21);
    prwh->get_value(4, awh.W_CITY, 21);
    prwh->get_value(5, awh.W_STATE, 3);
    prwh->get_value(6, awh.W_ZIP, 10);
    
    
    /* INSERT INTO history
     * VALUES (:c_id, :c_d_id, :c_w_id, :d_id, :w_id, 
     *         :curr_tmstmp, :ih_amount, :h_data)
     */
    
    tpcc_history_tuple ahist;
    sprintf(ahist.H_DATA, "%s   %s", awh.W_NAME, adistr.D_NAME);
    ahist.H_DATE = time(NULL);
    
    prhist->set_value(0, ppin._c_id);
    prhist->set_value(1, c_d);
    prhist->set_value(2, c_w);
    prhist->set_value(3, ppin._home_d_id);
    prhist->set_value(4, ppin._home_wh_id);
    prhist->set_value(5, ahist.H_DATE);
    prhist->set_value(6, ppin._h_amount * 100.0);
    prhist->set_value(7, ahist.H_DATA);
    
    TRACE( TRACE_TRX_FLOW, "App: %d PAY:hist-add-tuple\n", xct_id);
    W_DO(_phistory_man->add_tuple(_pssm, prhist));

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prwh->print_tuple();
    prdist->print_tuple();
    prcust->print_tuple();
    prhist->print_tuple();
#endif

    return RCOK;

} // EOF: PAYMENT




/******************************************************************** 
 *
 * TPC-C ORDER STATUS
 *
 * Input: w_id, d_id, c_id (use c_last if set to null), c_last
 *
 * @note: Read-Only trx
 *
 ********************************************************************/

w_rc_t ShoreTPCCEnv::xct_order_status(const int xct_id, 
                                      order_status_input_t& pstin)
{
    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    int w_id = pstin._wh_id;
    int d_id = pstin._d_id;

    // order_status trx touches 3 tables: 
    // customer, order and orderline

    tuple_guard<customer_man_impl> prcust(_pcustomer_man);
    tuple_guard<order_man_impl> prord(_porder_man);
    tuple_guard<order_line_man_impl> prol(_porder_line_man);

    rep_row_t areprow(_pcustomer_man->ts());

    // allocate space for the biggest of the 3 table representations
    areprow.set(_pcustomer_desc->maxsize()); 

    prcust->_rep = &areprow;
    prord->_rep = &areprow;
    prol->_rep = &areprow;

    rep_row_t lowrep(_pcustomer_man->ts());
    rep_row_t highrep(_pcustomer_man->ts());

    tpcc_orderline_tuple* porderlines = NULL;

    // allocate space for the biggest of the (customer) and (order)
    // table representations
    lowrep.set(_pcustomer_desc->maxsize()); 
    highrep.set(_pcustomer_desc->maxsize()); 

    // 1a. select customer based on name //
    if (pstin._c_id == 0) {

	/* SELECT  c_id, c_first
	 * FROM customer
	 * WHERE c_last = :c_last AND c_w_id = :w_id AND c_d_id = :d_id
	 * ORDER BY c_first
	 *
	 * plan: index only scan on "C_NAME_IDX"
	 */
	
	assert (pstin._c_select <= 60);
	assert (pstin._c_last);
	
	guard<index_scan_iter_impl<customer_t> > c_iter;
	{
	    index_scan_iter_impl<customer_t>* tmp_c_iter;
	    TRACE( TRACE_TRX_FLOW, "App: %d ORDST:cust-iter-by-name-idx\n",
		   xct_id);
	    W_DO(_pcustomer_man->cust_get_iter_by_index(_pssm, tmp_c_iter,
							prcust, lowrep, highrep,
							w_id, d_id,
							pstin._c_last));
	    c_iter = tmp_c_iter;
	}
	
	vector<int>  c_id_list;
	int  id = 0;
	int  count = 0;
	bool eof;
	
	W_DO(c_iter->next(_pssm, eof, *prcust));
	while (!eof) {
	    // push the retrieved customer id to the vector
	    ++count;
	    prcust->get_value(0, id);            
	    c_id_list.push_back(id);
	    TRACE( TRACE_TRX_FLOW, "App: %d ORDST:cust-iter-next\n", xct_id);
	    W_DO(c_iter->next(_pssm, eof, *prcust));
	}
	assert (count);
	
	// find the customer id in the middle of the list            
	pstin._c_id = c_id_list[(count+1)/2-1];
    }
    assert (pstin._c_id>0);

    // 1. probe the customer //

    /* SELECT c_first, c_middle, c_last, c_balance
     * FROM customer
     * WHERE c_id = :c_id AND c_w_id = :w_id AND c_d_id = :d_id
     *
     * plan: index probe on "C_IDX"
     */
    
    TRACE( TRACE_TRX_FLOW, "App: %d ORDST:cust-idx-probe (%d) (%d) (%d)\n", 
	   xct_id, w_id, d_id, pstin._c_id);
    W_DO(_pcustomer_man->cust_index_probe(_pssm, prcust, 
					  w_id, d_id, pstin._c_id));
    
    tpcc_customer_tuple acust;
    prcust->get_value(3,  acust.C_FIRST, 17);
    prcust->get_value(4,  acust.C_MIDDLE, 3);
    prcust->get_value(5,  acust.C_LAST, 17);
    prcust->get_value(16, acust.C_BALANCE);
    
    // 2. retrieve the last order of this customer
    
    /* SELECT o_id, o_entry_d, o_carrier_id
     * FROM orders
     * WHERE o_w_id = :w_id AND o_d_id = :d_id AND o_c_id = :o_c_id
     * ORDER BY o_id DESC
     *
     * plan: index scan on "O_CUST_IDX"
     */
    
    guard<index_scan_iter_impl<order_t> > o_iter;
    {
	index_scan_iter_impl<order_t>* tmp_o_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d ORDST:ord-iter-by-idx\n", xct_id);
	W_DO(_porder_man->ord_get_iter_by_index(_pssm, tmp_o_iter, prord,
						lowrep, highrep,
						w_id, d_id, pstin._c_id));
	o_iter = tmp_o_iter;
    }
    
    tpcc_order_tuple aorder;
    bool eof;
    
    W_DO(o_iter->next(_pssm, eof, *prord));
    while (!eof) {
	prord->get_value(0, aorder.O_ID);
	prord->get_value(4, aorder.O_ENTRY_D);
	prord->get_value(5, aorder.O_CARRIER_ID);
	prord->get_value(6, aorder.O_OL_CNT);
	W_DO(o_iter->next(_pssm, eof, *prord));
    }
    
    // we should have retrieved a valid id and ol_cnt for the order
    assert (aorder.O_ID);
    assert (aorder.O_OL_CNT);
    
    // 3. retrieve all the orderlines that correspond to the last order
    
    /* SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d 
     * FROM order_line 
     * WHERE ol_w_id = :H00003 AND ol_d_id = :H00004 AND ol_o_id = :H00016 
     *
     * plan: index scan on "OL_IDX"
     */
    
    guard<index_scan_iter_impl<order_line_t> > ol_iter;
    {
	index_scan_iter_impl<order_line_t>* tmp_ol_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d ORDST:ol-iter-by-idx\n", xct_id);
	W_DO(_porder_line_man->ol_get_probe_iter_by_index(_pssm, tmp_ol_iter,
							  prol, lowrep, highrep,
							  w_id, d_id,
							  aorder.O_ID));
	ol_iter = tmp_ol_iter;
    }
        
    porderlines = new tpcc_orderline_tuple[aorder.O_OL_CNT];
    int i=0;
    
    W_DO(ol_iter->next(_pssm, eof, *prol));
    while (!eof) {
	prol->get_value(4, porderlines[i].OL_I_ID);
	prol->get_value(5, porderlines[i].OL_SUPPLY_W_ID);
	prol->get_value(6, porderlines[i].OL_DELIVERY_D);
	prol->get_value(7, porderlines[i].OL_QUANTITY);
	prol->get_value(8, porderlines[i].OL_AMOUNT);
	i++;
	W_DO(ol_iter->next(_pssm, eof, *prol));
    }
    
#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    rcust.print_tuple();
    rord.print_tuple();
    rordline.print_tuple();
#endif

    if (porderlines) delete [] porderlines;

    return RCOK;
    
}; // EOF: ORDER-STATUS




/******************************************************************** 
 *
 * TPC-C DELIVERY
 *
 * Input data: w_id, carrier_id
 *
 * @note: Delivers one new_order (undelivered order) from each district
 *
 ********************************************************************/
unsigned int delivery_abort_ctr = 0;

w_rc_t ShoreTPCCEnv::xct_delivery(const int xct_id, 
                                  delivery_input_t& pdin)
{
    static bool const SPLIT_TRX = false;
    std::vector<int> dlist(DISTRICTS_PER_WAREHOUSE);
    int d_id;
    w_rc_t e = _xct_delivery_helper(xct_id, pdin, dlist, d_id, SPLIT_TRX);
    while(SPLIT_TRX && e.is_error() && e.err_num() == smlevel_0::eDEADLOCK) {
	W_COERCE(_pssm->abort_xct());
	W_DO(_pssm->begin_xct());
	atomic_inc_32(&delivery_abort_ctr);
	dlist.push_back(d_id); // retry the failed trx
	e = _xct_delivery_helper(xct_id, pdin, dlist, d_id, SPLIT_TRX);
    }
    return e;
}; // EOF: DELIVERY

w_rc_t ShoreTPCCEnv::_xct_delivery_helper(const int xct_id, 
					  delivery_input_t& pdin,
					  std::vector<int>& dlist,
					  int& d_id,
					  const bool SPLIT_TRX) {
    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    int w_id       = pdin._wh_id;
    int carrier_id = pdin._carrier_id; 
    time_t ts_start = time(NULL);

    // delivery trx touches 4 tables: 
    // new_order, order, orderline, and customer

    tuple_guard<new_order_man_impl> prno(_pnew_order_man);
    tuple_guard<order_man_impl> prord(_porder_man);
    tuple_guard<order_line_man_impl> prol(_porder_line_man);
    tuple_guard<customer_man_impl> prcust(_pcustomer_man);

    rep_row_t areprow(_pcustomer_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_pcustomer_desc->maxsize()); 

    prno->_rep = &areprow;
    prord->_rep = &areprow;
    prol->_rep = &areprow;
    prcust->_rep = &areprow;

    rep_row_t lowrep(_porder_line_man->ts());
    rep_row_t highrep(_porder_line_man->ts());

    // allocate space for the biggest of the (new_order) and (orderline)
    // table representations
    lowrep.set(_porder_line_desc->maxsize()); 
    highrep.set(_porder_line_desc->maxsize()); 

    for(uint_t i=0; i < dlist.size(); i++) {
        dlist[i] = i+1;
    }
    if(SPLIT_TRX) {
	std::random_shuffle(dlist.begin(), dlist.end());
    }

    // process each district separately
    while(dlist.size()) {
	d_id = dlist.back();
	dlist.pop_back();	    
	
	// 1. Get the new_order of the district, with the min value
	
	/* SELECT MIN(no_o_id) INTO :no_o_id:no_o_id_i
	 * FROM new_order
	 * WHERE no_d_id = :d_id AND no_w_id = :w_id
	 *
	 * plan: index scan on "NO_IDX"
	 */

	TRACE( TRACE_TRX_FLOW, "App: %d DEL:nord-iter-by-idx (%d) (%d)\n", 
	       xct_id, w_id, d_id);	
	guard<index_scan_iter_impl<new_order_t> > no_iter;
	{
	    index_scan_iter_impl<new_order_t>* tmp_no_iter;
	    W_DO(_pnew_order_man->no_get_iter_by_index(_pssm, tmp_no_iter,
						       prno, lowrep, highrep,
						       w_id, d_id,
						       EX, false));
	    no_iter = tmp_no_iter;
	}
	
	bool eof;
	
	// iterate over all new_orders and load their no_o_ids to sort buffer
	W_DO(no_iter->next(_pssm, eof, *prno));
	
	if (eof) continue; // skip this district
	
	int no_o_id;
	prno->get_value(0, no_o_id);
	assert (no_o_id);
	
	// 2. Delete the retrieved new order
	
	/* DELETE FROM new_order
	 * WHERE no_w_id = :w_id AND no_d_id = :d_id AND no_o_id = :no_o_id
	 *
	 * plan: index scan on "NO_IDX"
	 */
	
	TRACE( TRACE_TRX_FLOW,
	       "App: %d DEL:nord-delete-by-index (%d) (%d) (%d)\n", 
	       xct_id, w_id, d_id, no_o_id);	
	W_DO(_pnew_order_man->no_delete_by_index(_pssm, prno, 
						 w_id, d_id, no_o_id));
	
	// 3a. Update the carrier for the delivered order (in the orders table)
	// 3b. Get the customer id of the updated order
	
	/* UPDATE orders SET o_carrier_id = :o_carrier_id
	 * SELECT o_c_id INTO :o_c_id FROM orders
	 * WHERE o_id = :no_o_id AND o_w_id = :w_id AND o_d_id = :d_id;
	 *
	 * plan: index probe on "O_IDX"
	 */
	
	TRACE( TRACE_TRX_FLOW,
	       "App: %d DEL:ord-idx-probe-upd (%d) (%d) (%d)\n", 
	       xct_id, w_id, d_id, no_o_id);	
	prord->set_value(0, no_o_id);
	prord->set_value(2, d_id);
	prord->set_value(3, w_id);
	W_DO(_porder_man->ord_update_carrier_by_index(_pssm, prord,
						      carrier_id));

	int  c_id;
	prord->get_value(1, c_id);
	
	// 4a. Calculate the total amount of the orders from orderlines
	// 4b. Update all the orderlines with the current timestamp
        
	/* SELECT SUM(ol_amount) INTO :total_amount FROM order_line
	 * UPDATE ORDER_LINE SET ol_delivery_d = :curr_tmstmp
	 * WHERE ol_w_id = :w_id AND ol_d_id = :no_d_id AND ol_o_id = :no_o_id;
	 *
	 * plan: index scan on "OL_IDX"
	 */
	
	TRACE( TRACE_TRX_FLOW, 
	       "App: %d DEL:ol-iter-probe-by-idx (%d) (%d) (%d)\n", 
	       xct_id, w_id, d_id, no_o_id);
	
	int total_amount = 0;
	guard<index_scan_iter_impl<order_line_t> > ol_iter;
	{
	    index_scan_iter_impl<order_line_t>* tmp_ol_iter;
	    W_DO(_porder_line_man->ol_get_probe_iter_by_index(_pssm,
							      tmp_ol_iter,
							      prol, 
							      lowrep, highrep,
							      w_id, d_id,
							      no_o_id));
	    ol_iter = tmp_ol_iter;
	}
	
	// iterate over all the orderlines for the particular order
	W_DO(ol_iter->next(_pssm, eof, *prol));
	while (!eof) {
	    // update the total amount
	    int current_amount;
	    prol->get_value(8, current_amount);
	    total_amount += current_amount;
	    // update orderline
	    prol->set_value(6, ts_start);
	    W_DO(_porder_line_man->update_tuple(_pssm, prol));
	    // go to the next orderline
	    W_DO(ol_iter->next(_pssm, eof, *prol));
	}
	
	// 5. Update balance of the customer of the order
	
	/* UPDATE customer
	 * SET c_balance=c_balance+:total_amount,c_delivery_cnt=c_delivery_cnt+1
	 * WHERE c_id = :c_id AND c_w_id = :w_id AND c_d_id = :no_d_id;
	 *
	 * plan: index probe on "C_IDX"
	 */
	
	TRACE( TRACE_TRX_FLOW,
	       "App: %d DEL:cust-idx-probe-upd (%d) (%d) (%d)\n", 
	       xct_id, w_id, d_id, c_id);
	W_DO(_pcustomer_man->cust_index_probe_forupdate(_pssm, prcust, 
							w_id, d_id, c_id));
	
	double   balance;
	prcust->get_value(16, balance);
	prcust->set_value(16, balance+total_amount);
	W_DO(_pcustomer_man->update_tuple(_pssm, prcust));
	
	if(SPLIT_TRX && dlist.size()) {
#ifdef CFG_FLUSHER
#warning TPCC-Delivery does not do the intermediate commits lazily
#endif
	    W_DO(_pssm->commit_xct());
	    W_DO(_pssm->begin_xct());
	}
    }	

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prno->print_tuple();
    prord->print_tuple();
    prordline->print_tuple();
    prcust->print_tuple();
#endif

}



/******************************************************************** 
 *
 * TPC-C STOCK LEVEL
 *
 * Input data: w_id, d_id, threshold
 *
 * @note: Read-only transaction
 *
 ********************************************************************/

w_rc_t ShoreTPCCEnv::xct_stock_level(const int xct_id, 
                                     stock_level_input_t& pslin)
{
    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    
    // stock level trx touches 3 tables: 
    // district, orderline, and stock

    tuple_guard<district_man_impl> prdist(_pdistrict_man);
    tuple_guard<order_line_man_impl> prol(_porder_line_man);
    tuple_guard<stock_man_impl> prst(_pstock_man);

    rep_row_t areprow(_pdistrict_man->ts());

    // allocate space for the biggest of the 3 table representations
    areprow.set(_pdistrict_desc->maxsize()); 

    prdist->_rep = &areprow;
    prol->_rep = &areprow;
    prst->_rep = &areprow;

    // 1. get next_o_id from the district
    
    /* SELECT d_next_o_id INTO :o_id
     * FROM district
     * WHERE d_w_id = :w_id AND d_id = :d_id
     *
     * (index scan on D_IDX)
     */
    
    TRACE( TRACE_TRX_FLOW, "App: %d STO:dist-idx-probe (%d) (%d)\n", 
	   xct_id, pslin._wh_id, pslin._d_id);
    W_DO(_pdistrict_man->dist_index_probe(_pssm, prdist, 
					  pslin._wh_id, pslin._d_id));

    int next_o_id = 0;
    prdist->get_value(10, next_o_id);

    
    /*
     *   SELECT COUNT(DISTRICT(s_i_id)) INTO :stock_count
     *   FROM order_line, stock
     *   WHERE ol_w_id = :w_id AND ol_d_id = :d_id
     *       AND ol_o_id < :o_id AND ol_o_id >= :o_id-20
     *       AND s_w_id = :w_id AND s_i_id = ol_i_id
     *       AND s_quantity < :threshold;
     *
     *   Plan: 1. index scan on OL_IDX 
     *         2. sort ol tuples in the order of i_id from 1
     *         3. index scan on S_IDX
     *         4. fetch stock with sargable on quantity from 3
     *         5. nljoin on 2 and 4
     *         6. unique on 5
     *         7. group by on 6
     */
    
    // 2a. Index scan on order_line table
    
    TRACE( TRACE_TRX_FLOW, "App: %d STO:ol-iter-by-idx (%d) (%d) (%d) (%d)\n", 
	   xct_id, pslin._wh_id, pslin._d_id, next_o_id-20, next_o_id);
    
    rep_row_t lowrep(_porder_line_man->ts());
    rep_row_t highrep(_porder_line_man->ts());
    rep_row_t sortrep(_porder_line_man->ts());
    // allocate space for the biggest of the (new_order) and (orderline)
    // table representations
    lowrep.set(_porder_line_desc->maxsize()); 
    highrep.set(_porder_line_desc->maxsize()); 
    sortrep.set(_porder_line_desc->maxsize()); 
    
    guard<index_scan_iter_impl<order_line_t> > ol_iter;
    {
	index_scan_iter_impl<order_line_t>* tmp_ol_iter;
	W_DO(_porder_line_man->ol_get_range_iter_by_index(_pssm, tmp_ol_iter,
							  prol, lowrep, highrep,
							  pslin._wh_id,
							  pslin._d_id,
							  next_o_id-20,
							  next_o_id));
	ol_iter = tmp_ol_iter;
    }
    
    asc_sort_buffer_t ol_list(4);
    ol_list.setup(0, SQL_INT);  // OL_I_ID
    ol_list.setup(1, SQL_INT);  // OL_W_ID
    ol_list.setup(2, SQL_INT);  // OL_D_ID
    ol_list.setup(3, SQL_INT);  // OL_O_ID
    asc_sort_man_impl ol_sorter(&ol_list, &sortrep);
    table_row_t rsb(&ol_list);

    // iterate over all selected orderlines and add them to the sorted buffer
    bool eof;
    W_DO(ol_iter->next(_pssm, eof, *prol));
    while (!eof) {
	int temp_oid, temp_iid;
	int temp_wid, temp_did;        
	prol->get_value(4, temp_iid);
	prol->get_value(0, temp_oid);
	prol->get_value(2, temp_wid);
	prol->get_value(1, temp_did);
	rsb.set_value(0, temp_iid);
	rsb.set_value(1, temp_wid);
	rsb.set_value(2, temp_did);
	rsb.set_value(3, temp_oid);	
	ol_sorter.add_tuple(rsb);	
	W_DO(ol_iter->next(_pssm, eof, *prol));
    }
    assert (ol_sorter.count());

    // 2b. Sort orderline tuples on i_id
    asc_sort_iter_impl ol_list_sort_iter(_pssm, &ol_list, &ol_sorter);
    int last_i_id = -1;
    int count = 0;

    // 2c. Nested loop join order_line with stock
    W_DO(ol_list_sort_iter.next(_pssm, eof, rsb));
    while (!eof) {
	// use the index to find the corresponding stock tuple
	int i_id;
	int w_id;
	rsb.get_value(0, i_id);
	rsb.get_value(1, w_id);

	// 2d. Index probe the Stock
	W_DO(_pstock_man->st_index_probe(_pssm, prst, w_id, i_id));

	// check if stock quantity below threshold 
	int quantity;
	prst->get_value(3, quantity);
	if (quantity < pslin._threshold) {
	    // Do join on the two tuples	    
	    /* the work is to count the number of unique item id. We keep
	     * two pieces of information here: the last item id and the
	     * current count.  This is enough because the item id is in
	     * increasing order.
	     */
	    if (last_i_id != i_id) {
		last_i_id = i_id;
		count++;
	    }            
	    TRACE( TRACE_TRX_FLOW, "App: %d STO:found-one (%d) (%d) (%d)\n", 
		   xct_id, count, i_id, quantity);
	    
	}
	W_DO(ol_list_sort_iter.next(_pssm, eof, rsb));
    }
    
#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    rdist.print_tuple();
    rordline.print_tuple();
    rstock.print_tuple();
#endif
    
    return RCOK;
}




/******************************************************************** 
 *
 * MBENCHES
 *
 ********************************************************************/


/******************************************************************** 
 *
 * MBENCH-WH
 *
 ********************************************************************/

w_rc_t ShoreTPCCEnv::xct_mbench_wh(const int xct_id, 
                                   mbench_wh_input_t& mbin)
{
    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);
    
    // mbench trx touches 1 table: 
    // warehouse
    
    // get table tuples from the caches
    tuple_guard<warehouse_man_impl> prwh(_pwarehouse_man);

    rep_row_t areprow(_pwarehouse_man->ts());

    // allocate space for the biggest of the 4 table representations
    areprow.set(_pwarehouse_desc->maxsize()); 

    prwh->_rep = &areprow;

    // 1. retrieve warehouse for update
    TRACE( TRACE_TRX_FLOW, "App: %d MBWH:wh-idx-upd (%d)\n",
	   xct_id, mbin._wh_id);
    W_DO(_pwarehouse_man->wh_index_probe_forupdate(_pssm, prwh, mbin._wh_id));

    
    /* UPDATE warehouse SET w_ytd = wytd + :h_amount
     * WHERE w_id = :w_id
     *
     * SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip
     * FROM warehouse
     * WHERE w_id = :w_id
     *
     * plan: index probe on "W_IDX"
     */
    
    TRACE( TRACE_TRX_FLOW, "App: %d MBWH:wh-update-ytd (%d)\n", 
	   xct_id, mbin._wh_id);
    W_DO(_pwarehouse_man->wh_update_ytd(_pssm, prwh, mbin._amount));

    tpcc_warehouse_tuple awh;
    prwh->get_value(1, awh.W_NAME, 11);
    prwh->get_value(2, awh.W_STREET_1, 21);
    prwh->get_value(3, awh.W_STREET_2, 21);
    prwh->get_value(4, awh.W_CITY, 21);
    prwh->get_value(5, awh.W_STATE, 3);
    prwh->get_value(6, awh.W_ZIP, 10);

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prwh->print_tuple();
#endif

    return RCOK;

} // EOF: MBENCH-WH




/******************************************************************** 
 *
 * MBENCH-CUST
 *
 ********************************************************************/

w_rc_t ShoreTPCCEnv::xct_mbench_cust(const int xct_id, 
                                     mbench_cust_input_t& mcin)
{
    // ensure a valid environment    
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // mbench trx touches 1 table: 
    // customer

    tuple_guard<customer_man_impl> prcust(_pcustomer_man);

    rep_row_t areprow(_pcustomer_man->ts());

    // allocate space for the table representation
    areprow.set(_pcustomer_desc->maxsize()); 

    prcust->_rep = &areprow;
    
    // 1. retrieve customer for update
    
    /* SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, 
     * c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, 
     * c_discount, c_balance, c_ytd_payment, c_payment_cnt 
     * FROM customer 
     * WHERE c_id = :c_id AND c_w_id = :c_w_id AND c_d_id = :c_d_id 
     * FOR UPDATE OF c_balance, c_ytd_payment, c_payment_cnt
     *
     * plan: index probe on "C_IDX"
     */
    
    TRACE( TRACE_TRX_FLOW, "App: %d MBC:cust-idx-upd (%d) (%d) (%d)\n", 
	   xct_id, mcin._wh_id, mcin._d_id, mcin._c_id);
    W_DO(_pcustomer_man->cust_index_probe_forupdate(_pssm, prcust, 
						    mcin._wh_id, 
						    mcin._d_id, 
						    mcin._c_id));
    
    tpcc_customer_tuple acust;
    
    // retrieve customer
    prcust->get_value(3,  acust.C_FIRST, 17);
    prcust->get_value(4,  acust.C_MIDDLE, 3);
    prcust->get_value(5,  acust.C_LAST, 17);
    prcust->get_value(6,  acust.C_STREET_1, 21);
    prcust->get_value(7,  acust.C_STREET_2, 21);
    prcust->get_value(8,  acust.C_CITY, 21);
    prcust->get_value(9,  acust.C_STATE, 3);
    prcust->get_value(10, acust.C_ZIP, 10);
    prcust->get_value(11, acust.C_PHONE, 17);
    prcust->get_value(12, acust.C_SINCE);
    prcust->get_value(13, acust.C_CREDIT, 3);
    prcust->get_value(14, acust.C_CREDIT_LIM);
    prcust->get_value(15, acust.C_DISCOUNT);
    prcust->get_value(16, acust.C_BALANCE);
    prcust->get_value(17, acust.C_YTD_PAYMENT);
    prcust->get_value(18, acust.C_LAST_PAYMENT);
    prcust->get_value(19, acust.C_PAYMENT_CNT);
    prcust->get_value(20, acust.C_DATA_1, 251);
    prcust->get_value(21, acust.C_DATA_2, 251);
    
    // update customer fields
    acust.C_BALANCE -= mcin._amount;
    acust.C_YTD_PAYMENT += mcin._amount;
    acust.C_PAYMENT_CNT++;
    
    // if bad customer
    if (acust.C_CREDIT[0] == 'B' && acust.C_CREDIT[1] == 'C') { 
	// 10% of customers
	
	/* SELECT c_data
	 * FROM customer 
	 * WHERE c_id = :c_id AND c_w_id = :c_w_id AND c_d_id = :c_d_id
	 * FOR UPDATE OF c_balance, c_ytd_payment, c_payment_cnt, c_data
	 *
	 * plan: index probe on "C_IDX"
	 */
	
	// update the data
	char c_new_data_1[251];
	char c_new_data_2[251];
	sprintf(c_new_data_1, "%d,%d,%d,%d,%d,%1.2f",
		mcin._c_id, mcin._d_id, mcin._wh_id, 
		mcin._d_id, mcin._wh_id, mcin._amount);
	
	int len = strlen(c_new_data_1);
	strncat(c_new_data_1, acust.C_DATA_1, 250-len);
	strncpy(c_new_data_2, &acust.C_DATA_1[250-len], len);
	strncpy(c_new_data_2, acust.C_DATA_2, 250-len);
	
	TRACE( TRACE_TRX_FLOW, "App: %d PAY:bad-cust-upd-tuple\n", xct_id);
	W_DO(_pcustomer_man->cust_update_tuple(_pssm, prcust, acust, 
					       c_new_data_1, c_new_data_2));
    } else { // good customer
	TRACE( TRACE_TRX_FLOW, "App: %d PAY:good-cust-upd-tuple\n", xct_id);
	W_DO(_pcustomer_man->cust_update_tuple(_pssm, prcust, acust, 
					       NULL, NULL));
    }

#ifdef PRINT_TRX_RESULTS
    // at the end of the transaction 
    // dumps the status of all the table rows used
    prcust->print_tuple();
#endif

    return RCOK;

} // EOF: MBENCH-CUST


EXIT_NAMESPACE(tpcc);
