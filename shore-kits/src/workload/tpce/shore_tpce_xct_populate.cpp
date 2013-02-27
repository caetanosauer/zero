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

/** @file:   shore_tpce_xct_populate.cpp
 *
 *  @brief:  Implementation of the Baseline Shore TPC-E Database Population
 *           and Transaction Declerations
 *
 *  @author: Cansu Kaynak
 *  @author: Djordje Jevdjic
 */

#include "workload/tpce/shore_tpce_env.h"
#include "workload/tpce/tpce_const.h"
#include "workload/tpce/tpce_input.h"

#include <vector>
#include <numeric>
#include <algorithm>
#include <stdio.h>
#include <stdlib.h>
#include "workload/tpce/egen/CE.h"
#include "workload/tpce/egen/TxnHarnessStructs.h"
#include "workload/tpce/shore_tpce_egen.h"

using namespace shore;
using namespace TPCE;

//#define TRACE_TRX_FLOW TRACE_ALWAYS
//#define TRACE_TRX_RESULT TRACE_ALWAYS

ENTER_NAMESPACE(tpce);

#ifdef COMPILE_FLAT_FILE_LOAD 
extern FILE * fssec;
extern FILE* fshs;
#endif

const int loadUnit = 10000;

int testCnt = 0;

#ifdef TESTING_TPCE

int trxs_cnt_executed[10];
int trxs_cnt_failed[10];

#endif

int TradeOrderCnt;

unsigned long lastTradeId = 0;

//buffers for Egen data
AccountPermissionBuffer accountPermissionBuffer (3015);
CustomerBuffer customerBuffer (1005);
CustomerAccountBuffer customerAccountBuffer (1005);
CustomerTaxrateBuffer  customerTaxrateBuffer (2010);
HoldingBuffer holdingBuffer(10000);
HoldingHistoryBuffer holdingHistoryBuffer(2*loadUnit);
HoldingSummaryBuffer holdingSummaryBuffer(6000);
WatchItemBuffer watchItemBuffer (iMaxItemsInWL*1020+5000);
WatchListBuffer watchListBuffer (1020);

BrokerBuffer brokerBuffer(100);
CashTransactionBuffer cashTransactionBuffer(loadUnit);
ChargeBuffer chargeBuffer(20);
CommissionRateBuffer commissionRateBuffer (245);
SettlementBuffer settlementBuffer(loadUnit);
TradeBuffer tradeBuffer(loadUnit);
TradeHistoryBuffer tradeHistoryBuffer(3*loadUnit);
TradeTypeBuffer tradeTypeBuffer (10);


CompanyBuffer companyBuffer (1000);
CompanyCompetitorBuffer companyCompetitorBuffer(3000);
DailyMarketBuffer dailyMarketBuffer(3000);
ExchangeBuffer exchangeBuffer(9);
FinancialBuffer financialBuffer (1500);
IndustryBuffer industryBuffer(107);
LastTradeBuffer lastTradeBuffer (1005);
NewsItemBuffer newsItemBuffer(200); 
NewsXRefBuffer newsXRefBuffer(200);//big
SectorBuffer sectorBuffer(17);
SecurityBuffer securityBuffer(1005);


AddressBuffer addressBuffer(1005);
StatusTypeBuffer statusTypeBuffer (10);
TaxrateBuffer taxrateBuffer (325);
ZipCodeBuffer zipCodeBuffer (14850);

/******************************************************************** 
 *
 * Thread-local TPC-E TRXS Stats
 *
 ********************************************************************/

static __thread ShoreTPCETrxStats my_stats;

void ShoreTPCEEnv::env_thread_init()
{
    CRITICAL_SECTION(stat_mutex_cs, _statmap_mutex);
    _statmap[pthread_self()] = &my_stats;
}

void ShoreTPCEEnv::env_thread_fini()
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

ShoreTPCETrxStats ShoreTPCEEnv::_get_stats()
{
    CRITICAL_SECTION(cs, _statmap_mutex);
    ShoreTPCETrxStats rval;
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

void ShoreTPCEEnv::reset_stats()
{
    CRITICAL_SECTION(last_stats_cs, _last_stats_mutex);
    _last_stats = _get_stats();
    _num_invalid_input = 0;
}


/******************************************************************** 
 *
 *  @fn:    print_throughput
 *
 *  @brief: Prints the throughput given a measurement delay
 *
 ********************************************************************/

void ShoreTPCEEnv::print_throughput(const double iQueriedSF, 
				    const int iSpread,
				    const int iNumOfThreads,
				    const double delay,
				    const ulong_t mioch,
				    const double avgcpuusage)
{
    CRITICAL_SECTION(last_stats_cs, _last_stats_mutex);

    // get the current statistics
    ShoreTPCETrxStats current_stats = _get_stats();

    // now calculate the diff
    current_stats -= _last_stats;

    int trxs_att  = current_stats.attempted.total();
    int trxs_abt  = current_stats.failed.total();
    int trxs_dld  = current_stats.deadlocked.total();

    TRACE( TRACE_ALWAYS, "*******\n"                \
	   "Spread:    (%s)\n"                      \
	   "Threads:   (%d)\n"                      \
	   "Trxs Att:  (%d)\n"                      \
	   "Trxs Abt:  (%d)\n"                      \
	   "Trxs Dld:  (%d)\n"                      \
	   "Secs:      (%.2f)\n"                    \
	   "IOChars:   (%.2fM/s)\n"                 \
	   "AvgCPUs:   (%.1f) (%.1f%%)\n"           \
	   "TPS:       (%.2f)\n"                    \
	   "Invalid Input:  (%d)\n",
	   (iSpread ? "Yes" : "No"),
	   iNumOfThreads, trxs_att, trxs_abt, trxs_dld,
	   delay, mioch/delay, avgcpuusage, 100*avgcpuusage/64,
	   (trxs_att-trxs_abt-trxs_dld)/delay,
	   _num_invalid_input);
}

/******************************************************************** 
 *
 * TPC-E TRXS
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

w_rc_t ShoreTPCEEnv::run_one_xct(Request* prequest)
{
    // check if there is ready transaction initiated by market 
    if(prequest->type()==XCT_TPCE_MIX) {
	double rand =  (1.0*(smthread_t::me()->rand()%10000))/100.0;
	if (rand<0) rand*=-1.0;
	prequest->set_type(random_xct_type(rand));
    }
 
    switch (prequest->type()) {

    case XCT_TPCE_BROKER_VOLUME:
	return run_broker_volume(prequest);

    case XCT_TPCE_CUSTOMER_POSITION:
	return run_customer_position(prequest);

    case XCT_TPCE_MARKET_FEED:
	return run_market_feed(prequest);

    case XCT_TPCE_MARKET_WATCH:
	return run_market_watch(prequest);

    case XCT_TPCE_SECURITY_DETAIL:
	return run_security_detail(prequest);

    case XCT_TPCE_TRADE_LOOKUP:
	return run_trade_lookup(prequest);

    case XCT_TPCE_TRADE_ORDER:
	return run_trade_order(prequest);

    case XCT_TPCE_TRADE_RESULT:
	return run_trade_result(prequest);

    case XCT_TPCE_TRADE_STATUS:
	return run_trade_status(prequest);

    case XCT_TPCE_TRADE_UPDATE:
	return run_trade_update(prequest);

    case XCT_TPCE_DATA_MAINTENANCE:
	return run_data_maintenance(prequest);

    case XCT_TPCE_TRADE_CLEANUP:
	return run_trade_cleanup(prequest);

    default:
	printf("************type %d**********\n ", prequest->type());
	assert (0); // UNKNOWN TRX-ID
    }
    return (RCOK);
}



/******************************************************************** 
 *
 * TPC-E Database Loading
 *
 ********************************************************************/

/*
  DATABASE POPULATION TRANSACTIONS

  The TPC-E database has 33 tables. Out of those:
  9 are fixed with following cardinalities:
  CHARGE		 		15  
  COMMISSION_RATE		  	240
  EXCHANGE				4 
  INDUSTRY				102 
  SECTOR				12 
  STATUS_TYPE				5 
  TAXRATE				320 
  TRADE_TYPE				5
  ZIP_CODE				14741 

  16 are scaling (size is proportional to the number of customers):
  CUSTOMER				1*customer_count
  CUSTOMER_TAXRATE      2*customer_count
  CUSTOMER_ACCOUNT      5*customer_count
  ACCOUNT_PERMISSION   ~7.1* customer_count 
  ADDRESS	        1.5*customer_count + 4  
  BROKER		0.01*customer_count
  COMPANY		0.5 * customer_count
  COMPANY_COMPETITOR    1.5* customer_count
  DAILY_MARKET		4,469,625 securities * 1,305 
  FINANCIAL		10*customer_count 
  LAST_TRADE		0.685*customer_count 
  NEWS_ITEM		1*customer_count
  NEWS_XREF		1*customer_count 
  SECURITY              0.685*customer_count
  WATCH_LIST		1*customer_count 
  WATCH_ITEM           ~100*customer_count

  8 growing tables, proportional to the following expression: (customer_count*initial_trading_days)/scaling_factor ratio!
  CASH_TRANSACTION		
  HOLDING 
  HOLDING_HISTORY 
  HOLDING_SUMMARY 
  SETTLEMENT 
  TRADE 
  TRADE_HISTORY 
  TRADE_REQUEST

*/



/******************************************************************** 
 *
 * These functions populate records for the TPC-E database. They do not
 * commit though. So, they need to be invoked inside a transaction
 * and the caller needs to commit at the end. 
 *
 ********************************************************************/

// Populates one SECTOR
w_rc_t ShoreTPCEEnv::_load_one_sector(rep_row_t& areprow, PSECTOR_ROW record)
{    
    tuple_guard<sector_man_impl> pr(_psector_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->SC_ID);
    pr->set_value(1, record->SC_NAME);
    W_DO(_psector_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_sector()
{
    bool isLast = pGenerateAndLoad->isLastSector();
    while(!isLast) {
	PSECTOR_ROW record = pGenerateAndLoad->getSectorRow();
	sectorBuffer.append(record);
	isLast= pGenerateAndLoad->isLastSector();
    }
    sectorBuffer.setMoreToRead(false);
}

// Populates one CHARGE
w_rc_t ShoreTPCEEnv::_load_one_charge(rep_row_t& areprow, PCHARGE_ROW record)
{    
    tuple_guard<charge_man_impl> pr(_pcharge_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->CH_TT_ID);
    pr->set_value(1, (short)record->CH_C_TIER);
    pr->set_value(2, record->CH_CHRG);
    W_DO(_pcharge_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_charge()
{
    bool isLast = pGenerateAndLoad->isLastCharge();
    while(!isLast) {
	PCHARGE_ROW record = pGenerateAndLoad->getChargeRow();
	chargeBuffer.append(record);
	isLast= pGenerateAndLoad->isLastCharge();
    }
    chargeBuffer.setMoreToRead(false);
}

// Populates one COMMISSION_RATE
w_rc_t ShoreTPCEEnv::_load_one_commission_rate(rep_row_t& areprow,
					       PCOMMISSION_RATE_ROW record)
{    
    tuple_guard<commission_rate_man_impl> pr(_pcommission_rate_man);
    pr->_rep = &areprow;

    pr->set_value(0, (short)(record->CR_C_TIER)); //int in EGEN
    pr->set_value(1, record->CR_TT_ID);
    pr->set_value(2, record->CR_EX_ID);
    pr->set_value(3, (int)(record->CR_FROM_QTY)); //double in egen
    pr->set_value(4, (int)(record->CR_TO_QTY)); //double in egen
    pr->set_value(5, record->CR_RATE);
    W_DO(_pcommission_rate_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_commission_rate()
{
    bool isLast = pGenerateAndLoad->isLastCommissionRate();
    while(!isLast) {
	PCOMMISSION_RATE_ROW record = pGenerateAndLoad->getCommissionRateRow();
	commissionRateBuffer.append(record);
	isLast= pGenerateAndLoad->isLastCommissionRate();
    }
    commissionRateBuffer.setMoreToRead(false);
}

// Populates one EXCHANGE
w_rc_t ShoreTPCEEnv::_load_one_exchange(rep_row_t& areprow, PEXCHANGE_ROW record)
{    
    tuple_guard<exchange_man_impl> pr(_pexchange_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->EX_ID);
    pr->set_value(1, record->EX_NAME);
    pr->set_value(2, record->EX_NUM_SYMB);
    pr->set_value(3, record->EX_OPEN);
    pr->set_value(4, record->EX_CLOSE);
    pr->set_value(5, record->EX_DESC);
    pr->set_value(6, record->EX_AD_ID );
    W_DO(_pexchange_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_exchange()
{
    bool isLast = pGenerateAndLoad->isLastExchange();
    while(!isLast) {
	assert(testCnt<10);
	PEXCHANGE_ROW record = pGenerateAndLoad->getExchangeRow();
	exchangeBuffer.append(record);    
	isLast= pGenerateAndLoad->isLastExchange();
    }
    exchangeBuffer.setMoreToRead(false);
}

// Populates one INDUSTRY
w_rc_t ShoreTPCEEnv::_load_one_industry(rep_row_t& areprow, PINDUSTRY_ROW record)
{    
    tuple_guard<industry_man_impl> pr(_pindustry_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->IN_ID);
    pr->set_value(1, record->IN_NAME);
    pr->set_value(2, record->IN_SC_ID);
    W_DO(_pindustry_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_industry()
{
    bool isLast = pGenerateAndLoad->isLastIndustry();
    while(!isLast) {
	PINDUSTRY_ROW record = pGenerateAndLoad->getIndustryRow();
	industryBuffer.append(record);
	isLast= pGenerateAndLoad->isLastIndustry();
    }
    industryBuffer.setMoreToRead(false);
}

// Populates one STATUS_TYPE
w_rc_t ShoreTPCEEnv::_load_one_status_type(rep_row_t& areprow,
					   PSTATUS_TYPE_ROW record)
{    
    tuple_guard<status_type_man_impl> pr(_pstatus_type_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->ST_ID);
    pr->set_value(1, record->ST_NAME);
    W_DO(_pstatus_type_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_status_type()
{
    bool isLast = pGenerateAndLoad->isLastStatusType();
    while(!isLast){
	PSTATUS_TYPE_ROW record = pGenerateAndLoad->getStatusTypeRow();
	statusTypeBuffer.append(record);
	isLast= pGenerateAndLoad->isLastStatusType();
    }
    statusTypeBuffer.setMoreToRead(false);
}

// Populates one TAXRATE
w_rc_t ShoreTPCEEnv::_load_one_taxrate(rep_row_t& areprow, PTAXRATE_ROW record)
{    
    tuple_guard<taxrate_man_impl> pr(_ptaxrate_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->TX_ID);
    pr->set_value(1, record->TX_NAME);
    pr->set_value(2, (record->TX_RATE));
    W_DO(_ptaxrate_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_taxrate()
{
    bool hasNext;
    do{
	PTAXRATE_ROW record = pGenerateAndLoad->getTaxrateRow();
	taxrateBuffer.append(record);
	hasNext= pGenerateAndLoad->hasNextTaxrate();
    } while(hasNext);
    taxrateBuffer.setMoreToRead(false);
}

// Populates one TRADE_TYPE
w_rc_t ShoreTPCEEnv::_load_one_trade_type(rep_row_t& areprow,
					  PTRADE_TYPE_ROW record)
{    
    tuple_guard<trade_type_man_impl> pr(_ptrade_type_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->TT_ID);
    pr->set_value(1, record->TT_NAME);
    pr->set_value(2, (bool)record->TT_IS_SELL);
    pr->set_value(3, (bool)record->TT_IS_MRKT);
    W_DO(_ptrade_type_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_trade_type()
{
    bool isLast = pGenerateAndLoad->isLastTradeType();
    while(!isLast) {
	PTRADE_TYPE_ROW record = pGenerateAndLoad->getTradeTypeRow();
	tradeTypeBuffer.append(record);
	isLast= pGenerateAndLoad->isLastTradeType();
    }
    tradeTypeBuffer.setMoreToRead(false);
}

// Populates one ZIP_CODE
w_rc_t ShoreTPCEEnv::_load_one_zip_code(rep_row_t& areprow, PZIP_CODE_ROW record)
{    
    tuple_guard<zip_code_man_impl> pr(_pzip_code_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->ZC_CODE);
    pr->set_value(1, record->ZC_TOWN);
    pr->set_value(2, record->ZC_DIV);
    W_DO(_pzip_code_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_zip_code()
{
    bool hasNext = pGenerateAndLoad->hasNextZipCode();
    while(hasNext) {
	PZIP_CODE_ROW record = pGenerateAndLoad->getZipCodeRow();
	zipCodeBuffer.append(record);
	hasNext= pGenerateAndLoad->hasNextZipCode();
    }
    zipCodeBuffer.setMoreToRead(false);
}

// Populates one CUSTOMER
w_rc_t ShoreTPCEEnv::_load_one_customer(rep_row_t& areprow, PCUSTOMER_ROW record)
{    
    tuple_guard<customer_man_impl> pr(_pcustomer_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->C_ID);
    pr->set_value(1, record->C_TAX_ID);
    pr->set_value(2, record->C_ST_ID);
    pr->set_value(3, record->C_L_NAME);
    pr->set_value(4, record->C_F_NAME);
    pr->set_value(5, record->C_M_NAME);
    char xxz[2];
    xxz[0]=record->C_GNDR;
    xxz[1]='\0';
    pr->set_value(6, xxz);
    pr->set_value(7, (short)record->C_TIER);
    pr->set_value(8, (long long) EgenTimeToTimeT(record->C_DOB));
    pr->set_value(9, record->C_AD_ID);
    pr->set_value(10, record->C_CTRY_1);
    pr->set_value(11, record->C_AREA_1);
    pr->set_value(12, record->C_LOCAL_1);
    pr->set_value(13, record->C_EXT_1);
    pr->set_value(14, record->C_CTRY_2);
    pr->set_value(15, record->C_AREA_2);
    pr->set_value(16, record->C_LOCAL_2);
    pr->set_value(17, record->C_EXT_2);
    pr->set_value(18, record->C_CTRY_3);
    pr->set_value(19, record->C_AREA_3);
    pr->set_value(20, record->C_LOCAL_3);
    pr->set_value(21, record->C_EXT_3);
    pr->set_value(22, record->C_EMAIL_1);
    pr->set_value(23, record->C_EMAIL_2);
    W_DO(_pcustomer_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_customer()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextCustomer();
	PCUSTOMER_ROW record = pGenerateAndLoad->getCustomerRow();
	customerBuffer.append(record);
    } while((hasNext && customerBuffer.hasSpace()));
    customerBuffer.setMoreToRead(hasNext);
}

// Populates one CUSTOMER_TAXRATE
w_rc_t ShoreTPCEEnv::_load_one_customer_taxrate(rep_row_t& areprow,
						PCUSTOMER_TAXRATE_ROW record)
{
    tuple_guard<customer_taxrate_man_impl> pr(_pcustomer_taxrate_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->CX_TX_ID);
    pr->set_value(1, record->CX_C_ID);
    W_DO(_pcustomer_taxrate_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_customer_taxrate()
{
    bool hasNext;
    int taxrates=pGenerateAndLoad->getTaxratesCount();
    do {
	hasNext= pGenerateAndLoad->hasNextCustomerTaxrate();
	for(int i=0; i<taxrates; i++) {
	    PCUSTOMER_TAXRATE_ROW record =
		pGenerateAndLoad->getCustomerTaxrateRow(i);
	    customerTaxrateBuffer.append(record);
	}
    } while((hasNext && customerTaxrateBuffer.hasSpace()));    
    customerTaxrateBuffer.setMoreToRead(hasNext);
}

// Populates one CUSTOMER_ACCOUNT
w_rc_t ShoreTPCEEnv::_load_one_customer_account(rep_row_t& areprow,
						PCUSTOMER_ACCOUNT_ROW record)
{    
    tuple_guard<customer_account_man_impl> pr(_pcustomer_account_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->CA_ID);
    pr->set_value(1, record->CA_B_ID);
    pr->set_value(2, record->CA_C_ID);
    pr->set_value(3, record->CA_NAME);
    pr->set_value(4, (short)(record->CA_TAX_ST));
    pr->set_value(5, record->CA_BAL);
    W_DO(_pcustomer_account_man->add_tuple(_pssm, pr));

    return RCOK;
}

// Populates one ACCOUNT_PERMISSION
w_rc_t ShoreTPCEEnv::_load_one_account_permission(rep_row_t& areprow,
						  PACCOUNT_PERMISSION_ROW record)
{    
    tuple_guard<account_permission_man_impl> pr(_paccount_permission_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->AP_CA_ID);
    pr->set_value(1, record->AP_ACL);
    pr->set_value(2, record->AP_TAX_ID);
    pr->set_value(3, record->AP_L_NAME);
    pr->set_value(4, record->AP_F_NAME);
    W_DO(_paccount_permission_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_ca_and_ap()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextCustomerAccount();
	PCUSTOMER_ACCOUNT_ROW record = pGenerateAndLoad->getCustomerAccountRow();
	customerAccountBuffer.append(record);
	int perms = pGenerateAndLoad->PermissionsPerCustomer();
	for(int i=0; i<perms; i++) {
	    PACCOUNT_PERMISSION_ROW row =
		pGenerateAndLoad->getAccountPermissionRow(i);
	    accountPermissionBuffer.append(row);
	}
    } while((hasNext && customerAccountBuffer.hasSpace()));
    customerAccountBuffer.setMoreToRead(hasNext);
}

// Populates one ADDRESS
w_rc_t ShoreTPCEEnv::_load_one_address(rep_row_t& areprow, PADDRESS_ROW record)
{    
    tuple_guard<address_man_impl> pr(_paddress_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->AD_ID);
    pr->set_value(1, record->AD_LINE1);
    pr->set_value(2, record->AD_LINE2);
    pr->set_value(3, record->AD_ZC_CODE);
    pr->set_value(4, record->AD_CTRY);
    W_DO(_paddress_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_address()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextAddress();
	PADDRESS_ROW record = pGenerateAndLoad->getAddressRow();
	addressBuffer.append(record);
    } while((hasNext && addressBuffer.hasSpace()));
    addressBuffer.setMoreToRead(hasNext);
}

// Populates one WATCH_LIST
w_rc_t ShoreTPCEEnv::_load_one_watch_list(rep_row_t& areprow,
					  PWATCH_LIST_ROW record)
{    
    tuple_guard<watch_list_man_impl> pr(_pwatch_list_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->WL_ID);
    pr->set_value(1, record->WL_C_ID);
    W_DO(_pwatch_list_man->add_tuple(_pssm, pr));

    return RCOK;
}

// Populates one WATCH_ITEM
w_rc_t ShoreTPCEEnv::_load_one_watch_item(rep_row_t& areprow,
					  PWATCH_ITEM_ROW record)
{    
    tuple_guard<watch_item_man_impl> pr(_pwatch_item_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->WI_WL_ID);
    pr->set_value(1, record->WI_S_SYMB);
    W_DO(_pwatch_item_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_wl_and_wi()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextWatchList();
	PWATCH_LIST_ROW record = pGenerateAndLoad->getWatchListRow();
	watchListBuffer.append(record);
	int items = pGenerateAndLoad->ItemsPerWatchList();
	for(int i=0; i<items; ++i) {
	    PWATCH_ITEM_ROW row = pGenerateAndLoad->getWatchItemRow(i);
	    watchItemBuffer.append(row);
	}
    } while(hasNext && watchListBuffer.hasSpace());
    watchListBuffer.setMoreToRead(hasNext);
}

// Populates one COMPANY
w_rc_t ShoreTPCEEnv::_load_one_company(rep_row_t& areprow, PCOMPANY_ROW record)
{    
    tuple_guard<company_man_impl> pr(_pcompany_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->CO_ID);
    pr->set_value(1, record->CO_ST_ID);
    pr->set_value(2, record->CO_NAME);
    pr->set_value(3, record->CO_IN_ID);
    pr->set_value(4, record->CO_SP_RATE);
    pr->set_value(5, record->CO_CEO);
    pr->set_value(6, record->CO_AD_ID);
    pr->set_value(7, record->CO_DESC);
    pr->set_value(8, EgenTimeToTimeT(record->CO_OPEN_DATE));
    W_DO(_pcompany_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_company()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextCompany();
	PCOMPANY_ROW record = pGenerateAndLoad->getCompanyRow();
	companyBuffer.append(record);
    } while((hasNext && companyBuffer.hasSpace()));
    companyBuffer.setMoreToRead(hasNext);
}

// Populates one COMPANY_COMPETITOR
w_rc_t ShoreTPCEEnv::_load_one_company_competitor(rep_row_t& areprow,
						  PCOMPANY_COMPETITOR_ROW record)
{    
    tuple_guard<company_competitor_man_impl> pr(_pcompany_competitor_man);
    pr->_rep = &areprow;

    pr->set_value(0, (record->CP_CO_ID));
    pr->set_value(1, record->CP_COMP_CO_ID);
    pr->set_value(2, record->CP_IN_ID);
    W_DO(_pcompany_competitor_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_company_competitor()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextCompanyCompetitor();
	PCOMPANY_COMPETITOR_ROW record =
	    pGenerateAndLoad->getCompanyCompetitorRow();
	companyCompetitorBuffer.append(record);
    } while((hasNext && companyCompetitorBuffer.hasSpace()));
    companyCompetitorBuffer.setMoreToRead(hasNext);
}

// Populates one DAILY_MARKET
w_rc_t ShoreTPCEEnv::_load_one_daily_market(rep_row_t& areprow,
					    PDAILY_MARKET_ROW record)
{    
    tuple_guard<daily_market_man_impl> pr(_pdaily_market_man);
    pr->_rep = &areprow;

    pr->set_value(0, EgenTimeToTimeT(record->DM_DATE));
    pr->set_value(1, record->DM_S_SYMB);
    pr->set_value(2, record->DM_CLOSE);
    pr->set_value(3, record->DM_HIGH);
    pr->set_value(4, record->DM_LOW);
    pr->set_value(5, (int)record->DM_VOL); //double in EGEN
    W_DO(_pdaily_market_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_daily_market()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextDailyMarket();
	PDAILY_MARKET_ROW record = pGenerateAndLoad->getDailyMarketRow();
	dailyMarketBuffer.append(record);
    } while((hasNext && dailyMarketBuffer.hasSpace()));
    dailyMarketBuffer.setMoreToRead(hasNext);
}

// Populates one FINANCIAL
w_rc_t ShoreTPCEEnv::_load_one_financial(rep_row_t& areprow, PFINANCIAL_ROW record)
{    
    tuple_guard<financial_man_impl> pr(_pfinancial_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->FI_CO_ID);
    pr->set_value(1, record->FI_YEAR);
    pr->set_value(2, (short)record->FI_QTR); //int in Egen
    pr->set_value(3,  EgenTimeToTimeT(record->FI_QTR_START_DATE));
    pr->set_value(4,  record->FI_REVENUE);
    pr->set_value(5,  record->FI_NET_EARN);
    pr->set_value(6,  record->FI_BASIC_EPS);
    pr->set_value(7,  record->FI_DILUT_EPS);
    pr->set_value(8,  record->FI_MARGIN);
    pr->set_value(9,  record->FI_INVENTORY);
    pr->set_value(10, record->FI_ASSETS);
    pr->set_value(11, record->FI_LIABILITY);
    pr->set_value(12, record->FI_OUT_BASIC);
    pr->set_value(13, record->FI_OUT_DILUT);
    W_DO(_pfinancial_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_financial()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextFinancial();
	PFINANCIAL_ROW record = pGenerateAndLoad->getFinancialRow();
	financialBuffer.append(record);
    } while((hasNext && financialBuffer.hasSpace()));
    financialBuffer.setMoreToRead(hasNext);
}

// Populates one LAST_TRADE
w_rc_t ShoreTPCEEnv::_load_one_last_trade(rep_row_t& areprow,
					  PLAST_TRADE_ROW record)
{    
    tuple_guard<last_trade_man_impl> pr(_plast_trade_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->LT_S_SYMB);
    pr->set_value(1, EgenTimeToTimeT(record->LT_DTS));
    pr->set_value(2, record->LT_PRICE);
    pr->set_value(3, record->LT_OPEN_PRICE);
    pr->set_value(4, (double) record->LT_VOL); //int in Egen, INT64 in new Egen
    W_DO(_plast_trade_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_last_trade()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextLastTrade();
	PLAST_TRADE_ROW record = pGenerateAndLoad->getLastTradeRow();
	lastTradeBuffer.append(record);
    } while((hasNext && lastTradeBuffer.hasSpace()));
    lastTradeBuffer.setMoreToRead(hasNext);
}

// Populates one NEWS_ITEM
w_rc_t ShoreTPCEEnv::_load_one_news_item(rep_row_t& areprow, PNEWS_ITEM_ROW record)
{    
    tuple_guard<news_item_man_impl> pr(_pnews_item_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->NI_ID);
    pr->set_value(1, record->NI_HEADLINE);
    pr->set_value(2, record->NI_SUMMARY);
    char ni[max_news_item_size+1]; ni[max_news_item_size] = '\0';
    memcpy(ni,record->NI_ITEM,max_news_item_size);
    pr->set_value(3, ni);
    pr->set_value(4, EgenTimeToTimeT(record->NI_DTS));
    pr->set_value(5, record->NI_SOURCE);
    pr->set_value(6, record->NI_AUTHOR);
    W_DO(_pnews_item_man->add_tuple(_pssm, pr));

    return RCOK;
}

// Populates one NEWS_XREF
w_rc_t ShoreTPCEEnv::_load_one_news_xref(rep_row_t& areprow, PNEWS_XREF_ROW record)
{    
    tuple_guard<news_xref_man_impl> pr(_pnews_xref_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->NX_NI_ID);
    pr->set_value(1, record->NX_CO_ID);
    W_DO(_pnews_xref_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_ni_and_nx()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextNewsItemAndNewsXRef();
	PNEWS_ITEM_ROW record1 = pGenerateAndLoad->getNewsItemRow();
	PNEWS_XREF_ROW record2 = pGenerateAndLoad->getNewsXRefRow();
	newsItemBuffer.append(record1);
	newsXRefBuffer.append(record2);
    } while((hasNext && newsItemBuffer.hasSpace()));
    newsItemBuffer.setMoreToRead(hasNext);
    newsXRefBuffer.setMoreToRead(hasNext);
}

// Populates one SECURITY
w_rc_t ShoreTPCEEnv::_load_one_security(rep_row_t& areprow, PSECURITY_ROW record)
{    
    tuple_guard<security_man_impl> pr(_psecurity_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->S_SYMB);
    pr->set_value(1, record->S_ISSUE);
    pr->set_value(2, record->S_ST_ID);
    pr->set_value(3, record->S_NAME);
    pr->set_value(4, record->S_EX_ID);
    pr->set_value(5, record->S_CO_ID);
    pr->set_value(6, record->S_NUM_OUT);
    pr->set_value(7, EgenTimeToTimeT(record->S_START_DATE));
    pr->set_value(8, EgenTimeToTimeT(record->S_EXCH_DATE));
    pr->set_value(9, record->S_PE);
    pr->set_value(10, (record->S_52WK_HIGH));
    pr->set_value(11, EgenTimeToTimeT(record->S_52WK_HIGH_DATE));
    pr->set_value(12, (record->S_52WK_LOW));
    pr->set_value(13, EgenTimeToTimeT(record->S_52WK_LOW_DATE));
    pr->set_value(14, (record->S_DIVIDEND));
    pr->set_value(15, (record->S_YIELD));
#ifdef COMPILE_FLAT_FILE_LOAD 
    fprintf(fssec, "%s|%s|%s|%s\n",
	    record->S_SYMB, record->S_ISSUE, record->S_ST_ID, record->S_NAME); 
#endif
    W_DO(_psecurity_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_security()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextSecurity();
	PSECURITY_ROW record = pGenerateAndLoad->getSecurityRow();
	securityBuffer.append(record);
    } while((hasNext && securityBuffer.hasSpace()));
    securityBuffer.setMoreToRead(hasNext);
}

// Populates one TRADE
w_rc_t ShoreTPCEEnv::_load_one_trade(rep_row_t& areprow, PTRADE_ROW record)
{    
    tuple_guard<trade_man_impl> pr(_ptrade_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->T_ID);
    pr->set_value(1, EgenTimeToTimeT(record->T_DTS));
    pr->set_value(2, record->T_ST_ID);
    pr->set_value(3, record->T_TT_ID);
    pr->set_value(4, (bool)record->T_IS_CASH);
    pr->set_value(5, record->T_S_SYMB);
    pr->set_value(6, record->T_QTY);
    pr->set_value(7, (record->T_BID_PRICE));
    pr->set_value(8, record->T_CA_ID);
    pr->set_value(9, record->T_EXEC_NAME);
    pr->set_value(10, (record->T_TRADE_PRICE));
    pr->set_value(11, (record->T_CHRG));
    pr->set_value(12, (record->T_COMM));
    pr->set_value(13, (record->T_TAX));
    pr->set_value(14, (bool)record->T_LIFO);
    W_DO(_ptrade_man->add_tuple(_pssm, pr));

    return RCOK;
}

// Populates one TRADE_HISTORY
w_rc_t ShoreTPCEEnv::_load_one_trade_history(rep_row_t& areprow,
					     PTRADE_HISTORY_ROW record)
{    
    tuple_guard<trade_history_man_impl> pr(_ptrade_history_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->TH_T_ID);
    pr->set_value(1, EgenTimeToTimeT(record->TH_DTS));
    pr->set_value(2, record->TH_ST_ID);
    W_DO(_ptrade_history_man->add_tuple(_pssm, pr));

    return RCOK;
}

// Populates one SETTLEMENT
w_rc_t ShoreTPCEEnv::_load_one_settlement(rep_row_t& areprow,
					  PSETTLEMENT_ROW record)
{    
    tuple_guard<settlement_man_impl> pr(_psettlement_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->SE_T_ID);
    pr->set_value(1, record->SE_CASH_TYPE);
    pr->set_value(2, EgenTimeToTimeT(record->SE_CASH_DUE_DATE));
    pr->set_value(3, record->SE_AMT);
    W_DO(_psettlement_man->add_tuple(_pssm, pr));

    return RCOK;
}

// Populates one CASH_TRANSACTION
w_rc_t ShoreTPCEEnv::_load_one_cash_transaction(rep_row_t& areprow,
						PCASH_TRANSACTION_ROW record)
{    
    tuple_guard<cash_transaction_man_impl> pr(_pcash_transaction_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->CT_T_ID);
    pr->set_value(1, EgenTimeToTimeT(record->CT_DTS));
    pr->set_value(2, record->CT_AMT);
    pr->set_value(3, record->CT_NAME);
    W_DO(_pcash_transaction_man->add_tuple(_pssm, pr));

    return RCOK;
}

// Populates one HOLDING_HISTORY
w_rc_t ShoreTPCEEnv::_load_one_holding_history(rep_row_t& areprow,
					       PHOLDING_HISTORY_ROW record)
{    
    tuple_guard<holding_history_man_impl> pr(_pholding_history_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->HH_H_T_ID);
    pr->set_value(1, record->HH_T_ID);
    pr->set_value(2, record->HH_BEFORE_QTY);
    pr->set_value(3, record->HH_AFTER_QTY);
    W_DO(_pholding_history_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_trade_unit()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextTrade();
	PTRADE_ROW row = pGenerateAndLoad->getTradeRow();
	tradeBuffer.append(row);
	int hist = pGenerateAndLoad->getTradeHistoryRowCount();
	for(int i=0; i<hist; i++) {
	    PTRADE_HISTORY_ROW record = pGenerateAndLoad->getTradeHistoryRow(i);
	    tradeHistoryBuffer.append(record);
	}
	if(pGenerateAndLoad->shouldProcessSettlementRow()) {
	    PSETTLEMENT_ROW record = pGenerateAndLoad->getSettlementRow();
	    settlementBuffer.append(record);
	}
	if(pGenerateAndLoad->shouldProcessCashTransactionRow()) {
	    PCASH_TRANSACTION_ROW record=pGenerateAndLoad->getCashTransactionRow();
	    cashTransactionBuffer.append(record);
	}
	hist = pGenerateAndLoad->getHoldingHistoryRowCount();
	for(int i=0; i<hist; i++) {
	    PHOLDING_HISTORY_ROW record=pGenerateAndLoad->getHoldingHistoryRow(i);
	    holdingHistoryBuffer.append(record);
	}
    } while((hasNext && tradeBuffer.hasSpace()));
    tradeBuffer.setMoreToRead(hasNext);
}

// Populates one BROKER
w_rc_t ShoreTPCEEnv::_load_one_broker(rep_row_t& areprow, PBROKER_ROW record)
{    
    tuple_guard<broker_man_impl> pr(_pbroker_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->B_ID);
    pr->set_value(1, record->B_ST_ID);
    pr->set_value(2, record->B_NAME);
    pr->set_value(3, record->B_NUM_TRADES);
    pr->set_value(4, (record->B_COMM_TOTAL));
    W_DO(_pbroker_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_broker()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextBroker();
	PBROKER_ROW record = pGenerateAndLoad->getBrokerRow();
	brokerBuffer.append(record);
    } while((hasNext && brokerBuffer.hasSpace()));
    brokerBuffer.setMoreToRead(hasNext);
}

// Populates one HOLDING
w_rc_t ShoreTPCEEnv::_load_one_holding(rep_row_t& areprow, PHOLDING_ROW record)
{    
    tuple_guard<holding_man_impl> pr(_pholding_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->H_T_ID);
    pr->set_value(1, record->H_CA_ID);
    pr->set_value(2, record->H_S_SYMB);
    pr->set_value(3, EgenTimeToTimeT(record->H_DTS));
    pr->set_value(4, record->H_PRICE);
    pr->set_value(5, record->H_QTY);
    W_DO(_pholding_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_holding()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextHolding();
	PHOLDING_ROW record = pGenerateAndLoad->getHoldingRow();
	holdingBuffer.append(record);
    } while((hasNext && holdingBuffer.hasSpace()));
    holdingBuffer.setMoreToRead(hasNext);
}

// Populates one HOLDING_SUMMARY
w_rc_t ShoreTPCEEnv::_load_one_holding_summary(rep_row_t& areprow,
					       PHOLDING_SUMMARY_ROW record)
{    
    tuple_guard<holding_summary_man_impl> pr(_pholding_summary_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->HS_CA_ID);
    pr->set_value(1, record->HS_S_SYMB);
    pr->set_value(2, record->HS_QTY);
#ifdef COMPILE_FLAT_FILE_LOAD 
    fprintf(fshs, "%lld|%s|%d\n",
	    record->HS_CA_ID, record->HS_S_SYMB, record->HS_QTY); 
#endif
    W_DO(_pholding_summary_man->add_tuple(_pssm, pr));

    return RCOK;
}

void ShoreTPCEEnv::_read_holding_summary()
{
    bool hasNext;
    do {
	hasNext= pGenerateAndLoad->hasNextHoldingSummary();
	PHOLDING_SUMMARY_ROW record = pGenerateAndLoad->getHoldingSummaryRow();
	holdingSummaryBuffer.append(record);
    } while((hasNext && holdingSummaryBuffer.hasSpace()));
    holdingSummaryBuffer.setMoreToRead(hasNext);
}

// Populates one TRADE_REQUEST
w_rc_t ShoreTPCEEnv::_load_one_trade_request(rep_row_t& areprow,
					     PTRADE_REQUEST_ROW record)
{
    tuple_guard<trade_request_man_impl> pr(_ptrade_request_man);
    pr->_rep = &areprow;

    pr->set_value(0, record->TR_T_ID);
    pr->set_value(1, record->TR_TT_ID);
    pr->set_value(2, record->TR_S_SYMB);
    pr->set_value(3, record->TR_QTY);
    pr->set_value(4, (record->TR_BID_PRICE));
    pr->set_value(5, record->TR_B_ID); //named incorrectly in egen
    W_DO(_ptrade_request_man->add_tuple(_pssm, pr));

    return RCOK;
}

//populating small tables
w_rc_t ShoreTPCEEnv::xct_populate_small(const int xct_id,
					populate_small_input_t& ptoin)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);

    // The exchange has the biggest row all the fixed tables
    rep_row_t areprow(_pexchange_man->ts());
    areprow.set(_pexchange_desc->maxsize());

    // 2. Build the small tables
    TRACE( TRACE_ALWAYS, "Building CHARGE !!!\n");
    int rows=chargeBuffer.getSize();
    for(int i=0; i<rows; i++){
	PCHARGE_ROW record = chargeBuffer.get(i);
	W_DO(_load_one_charge(areprow, record));
    }
    pGenerateAndLoad->ReleaseCharge();
    TRACE( TRACE_ALWAYS, "Building COMMISSION_RATE !!!\n");
    rows=commissionRateBuffer.getSize();
    for(int i=0; i<rows; i++){
	PCOMMISSION_RATE_ROW record = commissionRateBuffer.get(i);
	W_DO(_load_one_commission_rate(areprow, record));
    }
    pGenerateAndLoad->ReleaseCommissionRate();
    TRACE( TRACE_ALWAYS, "Building EXCHANGE !!!\n");
    rows=exchangeBuffer.getSize();
    for(int i=0; i<rows; i++){
	PEXCHANGE_ROW record = exchangeBuffer.get(i);
	W_DO(_load_one_exchange(areprow, record));
    }
    pGenerateAndLoad->ReleaseExchange();
    TRACE( TRACE_ALWAYS, "Building INDUSTRY !!!\n");
    rows=industryBuffer.getSize();
    for(int i=0; i<rows; i++){
	PINDUSTRY_ROW record = industryBuffer.get(i);
	W_DO(_load_one_industry(areprow, record));
    }
    pGenerateAndLoad->ReleaseIndustry();
    TRACE( TRACE_ALWAYS, "Building SECTOR !!!\n");
    rows=sectorBuffer.getSize();
    for(int i=0; i<rows; i++){
	PSECTOR_ROW record = sectorBuffer.get(i);
	W_DO(_load_one_sector(areprow, record));
    }
    pGenerateAndLoad->ReleaseSector();
    TRACE( TRACE_ALWAYS, "Building STATUS_TYPE !!!\n");
    rows=statusTypeBuffer.getSize();
    for(int i=0; i<rows; i++){
	PSTATUS_TYPE_ROW record = statusTypeBuffer.get(i);
	W_DO(_load_one_status_type(areprow, record));
    }
    pGenerateAndLoad->ReleaseStatusType();
    TRACE( TRACE_ALWAYS, "Building TAXRATE !!!\n");
    rows=taxrateBuffer.getSize();
    for(int i=0; i<rows; i++){
	PTAXRATE_ROW record = taxrateBuffer.get(i);
	W_DO(_load_one_taxrate(areprow, record));
    }
    pGenerateAndLoad->ReleaseTaxrate();
    TRACE( TRACE_ALWAYS, "Building TRADE_TYPE !!!\n");
    rows=tradeTypeBuffer.getSize();
    for(int i=0; i<rows; i++){
	PTRADE_TYPE_ROW record = tradeTypeBuffer.get(i);
	W_DO(_load_one_trade_type(areprow, record));
    }
    pGenerateAndLoad->ReleaseTradeType();
    TRACE( TRACE_ALWAYS, "Building ZIP CODE !!!\n");
    rows=zipCodeBuffer.getSize();
    for(int i=0; i<rows; i++){
	PZIP_CODE_ROW record = zipCodeBuffer.get(i);
	W_DO(_load_one_zip_code(areprow, record));
    }
    pGenerateAndLoad->ReleaseZipCode();

    return (_pssm->commit_xct());
}

//customer
w_rc_t ShoreTPCEEnv::xct_populate_customer(const int xct_id,
					   populate_customer_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pcustomer_man->ts());
    areprow.set(_pcustomer_desc->maxsize());

    int rows=customerBuffer.getSize();
    for(int i=0; i<rows; i++) {
	PCUSTOMER_ROW record = customerBuffer.get(i);
	W_DO(_load_one_customer(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_customer()
{	
    pGenerateAndLoad->InitCustomer();
    TRACE( TRACE_ALWAYS, "Building CUSTOMER !!!\n");
    while(customerBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	customerBuffer.reset();
	_read_customer();
	populate_customer_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_customer(1, in),
			 log_space_needed, retry, this);
    }
    pGenerateAndLoad->ReleaseCustomer();
    customerBuffer.release();
}

//address
w_rc_t ShoreTPCEEnv::xct_populate_address(const int xct_id,
					  populate_address_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_paddress_man->ts());
    areprow.set(_paddress_desc->maxsize());

    int rows=addressBuffer.getSize();
    for(int i=0; i<rows; i++){
	PADDRESS_ROW record = addressBuffer.get(i);
	W_DO(_load_one_address(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_address()
{	
    pGenerateAndLoad->InitAddress();
    TRACE( TRACE_ALWAYS, "Building ADDRESS !!!\n");
    while(addressBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	addressBuffer.reset();
	_read_address();
	populate_address_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_address(1, in),
			 log_space_needed, retry, this);
    }
    pGenerateAndLoad->ReleaseAddress();
    addressBuffer.release();
}

//CustomerAccount and AccountPermission
w_rc_t ShoreTPCEEnv::xct_populate_ca_and_ap(const int xct_id,
					    populate_ca_and_ap_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pcustomer_account_man->ts());
    areprow.set(_pcustomer_account_desc->maxsize());

    int rows=customerAccountBuffer.getSize();
    for(int i=0; i<rows; i++){
	PCUSTOMER_ACCOUNT_ROW record = customerAccountBuffer.get(i);
	W_DO(_load_one_customer_account(areprow, record));
    }
    rows=accountPermissionBuffer.getSize();
    for(int i=0; i<rows; i++){
	PACCOUNT_PERMISSION_ROW record = accountPermissionBuffer.get(i);
	W_DO(_load_one_account_permission(areprow, record));
    }
    
    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_ca_and_ap()
{
    pGenerateAndLoad->InitCustomerAccountAndAccountPermission();
    TRACE( TRACE_ALWAYS, "Building CustomerAccount and AccountPermission !!!\n");
    while(customerAccountBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	customerAccountBuffer.reset();
	accountPermissionBuffer.reset();
	_read_ca_and_ap();
	populate_ca_and_ap_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_ca_and_ap(1, in),
			 log_space_needed, retry, this);
    }
    pGenerateAndLoad->ReleaseCustomerAccountAndAccountPermission();
    customerAccountBuffer.release();
    accountPermissionBuffer.release();
}

//Watch List and Watch Item
w_rc_t ShoreTPCEEnv::xct_populate_wl_and_wi(const int xct_id,
					    populate_wl_and_wi_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pwatch_item_man->ts());
    areprow.set(_pwatch_item_desc->maxsize());

    int rows=watchListBuffer.getSize();
    for(int i=0; i<rows; i++){
	PWATCH_LIST_ROW record = watchListBuffer.get(i);
	W_DO(_load_one_watch_list(areprow, record));
    }
    rows=watchItemBuffer.getSize();
    for(int i=0; i<rows; i++){
	PWATCH_ITEM_ROW record = watchItemBuffer.get(i);
	W_DO(_load_one_watch_item(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_wl_and_wi()
{	
    pGenerateAndLoad->InitWatchListAndWatchItem();
    TRACE( TRACE_ALWAYS, "Building WATCH_LIST table and WATCH_ITEM !!!\n");
    while(watchListBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	watchItemBuffer.reset();
	watchListBuffer.reset();
	_read_wl_and_wi();
	populate_wl_and_wi_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_wl_and_wi(1, in),
			 log_space_needed, retry, this);
    }
    pGenerateAndLoad->ReleaseWatchListAndWatchItem();
    watchItemBuffer.release();
    watchListBuffer.release();
}

//CUSTOMER_TAXRATE
w_rc_t ShoreTPCEEnv::xct_populate_customer_taxrate(const int xct_id,
						   populate_customer_taxrate_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pcustomer_taxrate_man->ts());
    areprow.set(_pcustomer_taxrate_desc->maxsize());

    int rows=customerTaxrateBuffer.getSize();
    for(int i=0; i<rows; i++){
	PCUSTOMER_TAXRATE_ROW record = customerTaxrateBuffer.get(i);
	W_DO(_load_one_customer_taxrate(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_customer_taxrate()
{	
    pGenerateAndLoad->InitCustomerTaxrate();
    TRACE( TRACE_ALWAYS, "Building CUSTOMER_TAXRATE !!!\n");
    while(customerTaxrateBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	customerTaxrateBuffer.reset();
	_read_customer_taxrate();
	populate_customer_taxrate_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_customer_taxrate(1, in),
			 log_space_needed, retry, this);
    }
    pGenerateAndLoad->ReleaseCustomerTaxrate();
    customerTaxrateBuffer.release();
}

//COMPANY
w_rc_t ShoreTPCEEnv::xct_populate_company(const int xct_id,
					  populate_company_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pcompany_man->ts());
    areprow.set(_pcompany_desc->maxsize());

    int rows=companyBuffer.getSize();
    for(int i=0; i<rows; i++){
	PCOMPANY_ROW record = companyBuffer.get(i);
	W_DO(_load_one_company(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_company()
{	
    pGenerateAndLoad->InitCompany();
    TRACE( TRACE_ALWAYS, "Building COMPANY  !!!\n");
    while(companyBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	companyBuffer.reset();
	_read_company();
	populate_company_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_company(1, in),
			 log_space_needed, retry, this);
    }
    pGenerateAndLoad->ReleaseCompany();
    companyBuffer.release();
}

//COMPANY COMPETITOR
w_rc_t ShoreTPCEEnv::xct_populate_company_competitor(const int xct_id,
						     populate_company_competitor_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pcompany_competitor_man->ts());
    areprow.set(_pcompany_competitor_desc->maxsize());

    int rows=companyCompetitorBuffer.getSize();
    for(int i=0; i<rows; i++){
	PCOMPANY_COMPETITOR_ROW record = companyCompetitorBuffer.get(i);
	W_DO(_load_one_company_competitor(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_company_competitor()
{	
    pGenerateAndLoad->InitCompanyCompetitor();
    TRACE( TRACE_ALWAYS, "Building COMPANY COMPETITOR !!!\n");
    while(companyCompetitorBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	companyCompetitorBuffer.reset();
	_read_company_competitor();
	populate_company_competitor_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_company_competitor(1, in),
			 log_space_needed, retry, this);
    }
    pGenerateAndLoad->ReleaseCompanyCompetitor();
    companyCompetitorBuffer.release();
}

//COMPANY
w_rc_t ShoreTPCEEnv::xct_populate_daily_market(const int xct_id,
					       populate_daily_market_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pdaily_market_man->ts());
    areprow.set(_pdaily_market_desc->maxsize());

    int rows=dailyMarketBuffer.getSize();
    for(int i=0; i<rows; i++){
	PDAILY_MARKET_ROW record = dailyMarketBuffer.get(i);
	W_DO(_load_one_daily_market(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_daily_market()
{	
    pGenerateAndLoad->InitDailyMarket();
    TRACE( TRACE_ALWAYS, "DAILY_MARKET   !!!\n");
    while(dailyMarketBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	dailyMarketBuffer.reset();
	_read_daily_market();
	populate_daily_market_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_daily_market(1, in),
			 log_space_needed, retry, this);
    }
    pGenerateAndLoad->ReleaseDailyMarket();
    dailyMarketBuffer.release();
}

//FINANCIAL
w_rc_t ShoreTPCEEnv::xct_populate_financial(const int xct_id,
					    populate_financial_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pfinancial_man->ts());
    areprow.set(_pfinancial_desc->maxsize());

    int rows=financialBuffer.getSize();
    for(int i=0; i<rows; i++){
	PFINANCIAL_ROW record = financialBuffer.get(i);
	W_DO(_load_one_financial(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_financial()
{	
    pGenerateAndLoad->InitFinancial();
    TRACE( TRACE_ALWAYS, "Building FINANCIAL  !!!\n");
    while(financialBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	financialBuffer.reset();
	_read_financial();
	populate_financial_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_financial(1, in),
			 log_space_needed, retry, this);
    }
    pGenerateAndLoad->ReleaseFinancial();
    financialBuffer.release();
}

//SECURITY
w_rc_t ShoreTPCEEnv::xct_populate_security(const int xct_id,
					   populate_security_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_psecurity_man->ts());
    areprow.set(_psecurity_desc->maxsize());

    int rows=securityBuffer.getSize();
    for(int i=0; i<rows; i++){
	PSECURITY_ROW record = securityBuffer.get(i);
	W_DO(_load_one_security(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_security()
{	
    pGenerateAndLoad->InitSecurity();
    TRACE( TRACE_ALWAYS, "Building SECURITY  !!!\n");
    while(securityBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	securityBuffer.reset();
	_read_security();
	populate_security_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());	
	CHECK_XCT_RETURN(this->xct_populate_security(1, in),
			 log_space_needed, retry, this);
    }
    pGenerateAndLoad->ReleaseSecurity();
    securityBuffer.release();
}

//LAST_TRADE
w_rc_t ShoreTPCEEnv::xct_populate_last_trade(const int xct_id,
					     populate_last_trade_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_plast_trade_man->ts());
    areprow.set(_plast_trade_desc->maxsize());

    int rows=lastTradeBuffer.getSize();
    for(int i=0; i<rows; i++){
	PLAST_TRADE_ROW record = lastTradeBuffer.get(i);
	W_DO(_load_one_last_trade(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_last_trade()
{	
    pGenerateAndLoad->InitLastTrade();
    TRACE( TRACE_ALWAYS, "Building LAST_TRADE  !!!\n");
    while(lastTradeBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	lastTradeBuffer.reset();
	_read_last_trade();
	populate_last_trade_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_last_trade(1, in),
			 log_space_needed, retry, this);
    }
    pGenerateAndLoad->ReleaseLastTrade();
    lastTradeBuffer.release();
}

//Watch List and Watch Item
w_rc_t ShoreTPCEEnv::xct_populate_ni_and_nx(const int xct_id,
					    populate_ni_and_nx_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pnews_item_man->ts());
    areprow.set(_pnews_item_desc->maxsize());

    int rows=newsXRefBuffer.getSize();
    for(int i=0; i<rows; i++){
	PNEWS_XREF_ROW record = newsXRefBuffer.get(i);
	W_DO(_load_one_news_xref(areprow, record));
    }
    rows=newsItemBuffer.getSize();
    for(int i=0; i<rows; i++){
	PNEWS_ITEM_ROW record = newsItemBuffer.get(i);
	W_DO(_load_one_news_item(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_ni_and_nx()
{	
    pGenerateAndLoad->InitNewsItemAndNewsXRef();
    TRACE( TRACE_ALWAYS, "Building NEWS_ITEM and NEWS_XREF !!!\n");
    while(newsItemBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	newsItemBuffer.reset();
	newsXRefBuffer.reset();
	_read_ni_and_nx();
	populate_ni_and_nx_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_ni_and_nx(1, in),
			 log_space_needed, retry, this);
    }
    pGenerateAndLoad->ReleaseNewsItemAndNewsXRef();
    newsItemBuffer.release();
    newsXRefBuffer.release();
}

//populating growing tables
w_rc_t ShoreTPCEEnv::xct_populate_unit_trade(const int xct_id,
					     populate_unit_trade_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pnews_item_man->ts());
    areprow.set(_pnews_item_desc->maxsize());

    int rows=tradeBuffer.getSize();
    for(int i=0; i<rows; i++){
	PTRADE_ROW record = tradeBuffer.get(i);
	W_DO(_load_one_trade(areprow, record));
    }
    rows=tradeHistoryBuffer.getSize();
    for(int i=0; i<rows; i++){
	PTRADE_HISTORY_ROW record = tradeHistoryBuffer.get(i);
	W_DO(_load_one_trade_history(areprow, record));
    }
    rows=settlementBuffer.getSize();
    for(int i=0; i<rows; i++){
	PSETTLEMENT_ROW record = settlementBuffer.get(i);
	W_DO(_load_one_settlement(areprow, record));
    }
    rows=cashTransactionBuffer.getSize();
    for(int i=0; i<rows; i++){
	PCASH_TRANSACTION_ROW record = cashTransactionBuffer.get(i);
	W_DO(_load_one_cash_transaction(areprow, record));
    }
    rows=holdingHistoryBuffer.getSize();
    for(int i=0; i<rows; i++){
	PHOLDING_HISTORY_ROW record = holdingHistoryBuffer.get(i);
	W_DO(_load_one_holding_history(areprow, record));
    }

    return (_pssm->commit_xct());
}

//BROKER
w_rc_t ShoreTPCEEnv::xct_populate_broker(const int xct_id,
					 populate_broker_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pbroker_man->ts());
    areprow.set(_pbroker_desc->maxsize());

    int rows=brokerBuffer.getSize();
    for(int i=0; i<rows; i++){
	PBROKER_ROW record = brokerBuffer.get(i);
	W_DO(_load_one_broker(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_broker()
{	
    while(brokerBuffer.hasMoreToRead()) {
	long log_space_needed = 0;
	brokerBuffer.reset();
	_read_broker();
	populate_broker_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_broker(1, in),
			 log_space_needed, retry, this);
    }
}

//HOLDING_SUMMARY
w_rc_t ShoreTPCEEnv::xct_populate_holding_summary(const int xct_id,
						  populate_holding_summary_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pholding_summary_man->ts());
    areprow.set(_pholding_summary_desc->maxsize());

    int rows=holdingSummaryBuffer.getSize();
    for(int i=0; i<rows; i++){
	PHOLDING_SUMMARY_ROW record = holdingSummaryBuffer.get(i);
	W_DO(_load_one_holding_summary(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_holding_summary()
{	
    while(holdingSummaryBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	holdingSummaryBuffer.reset();
	_read_holding_summary();
	populate_holding_summary_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_holding_summary(1, in),
			 log_space_needed, retry, this);
    }
}

//HOLDING
w_rc_t ShoreTPCEEnv::xct_populate_holding(const int xct_id,
					  populate_holding_input_t& ptoin)
{
    assert (_pssm);
    assert (_initialized);

    rep_row_t areprow(_pholding_man->ts());
    areprow.set(_pholding_desc->maxsize());

    int rows=holdingBuffer.getSize();
    for(int i=0; i<rows; i++){
	PHOLDING_ROW record = holdingBuffer.get(i);
	W_DO(_load_one_holding(areprow, record));
    }

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::populate_holding()
{	
    while(holdingBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	holdingBuffer.reset();
	_read_holding();
	populate_holding_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_holding(1, in),
			 log_space_needed, retry, this);
    }
}

void ShoreTPCEEnv::populate_unit_trade()
{
     while(tradeBuffer.hasMoreToRead()){
	long log_space_needed = 0;
	tradeBuffer.reset();
	tradeHistoryBuffer.reset();
	settlementBuffer.reset();
	cashTransactionBuffer.reset();
	holdingHistoryBuffer.reset();
	_read_trade_unit();
	printf("\n\n Populating trade unit\n\n" );
	populate_unit_trade_input_t in;
    retry:
	W_COERCE(this->db()->begin_xct());
	CHECK_XCT_RETURN(this->xct_populate_unit_trade(1, in),
			 log_space_needed, retry, this);
    }
}

void ShoreTPCEEnv::populate_growing()
{	
    pGenerateAndLoad->InitHoldingAndTrade();
    TRACE( TRACE_ALWAYS, "Building growing tables  !!!\n");
    int cnt =0;
    do {
	populate_unit_trade();
	populate_broker();
	populate_holding_summary();
	populate_holding();
	printf("\nload unit %d\n",++cnt);
	tradeBuffer.newLoadUnit();
	tradeHistoryBuffer.newLoadUnit();
	settlementBuffer.newLoadUnit();
	cashTransactionBuffer.newLoadUnit();
	holdingHistoryBuffer.newLoadUnit();
	brokerBuffer.newLoadUnit();
	holdingSummaryBuffer.newLoadUnit();
	holdingBuffer.newLoadUnit();	
    } while(pGenerateAndLoad->hasNextLoadUnit());
    pGenerateAndLoad->ReleaseHoldingAndTrade();
    tradeBuffer.release();
    tradeHistoryBuffer.release();
    settlementBuffer.release();
    cashTransactionBuffer.release();
    holdingHistoryBuffer.release();
    brokerBuffer.release();
    holdingSummaryBuffer.release();
    holdingBuffer.release();
}

w_rc_t ShoreTPCEEnv::xct_find_maxtrade_id(const int xct_id,
					  find_maxtrade_id_input_t& ptoin)
{
    assert (_pssm);

    tuple_guard<trade_man_impl> prtrade(_ptrade_man);

    rep_row_t lowrep(_ptrade_man->ts());
    rep_row_t highrep(_ptrade_man->ts());

    lowrep.set(_ptrade_desc->maxsize());
    highrep.set(_ptrade_desc->maxsize());	

    rep_row_t areprow(_ptrade_man->ts());
    areprow.set(_ptrade_desc->maxsize());

    prtrade->_rep = &areprow;

    TIdent trade_id;
    guard<index_scan_iter_impl<trade_t> > t_iter;
    {
	index_scan_iter_impl<trade_t>* tmp_t_iter;
	TRACE( TRACE_TRX_FLOW, "App: %d TO:t-iter-by-caid-idx \n", xct_id);
	W_DO(_ptrade_man->t_get_iter_by_index(_pssm, tmp_t_iter, prtrade, lowrep,
					      highrep, 0));
	t_iter = tmp_t_iter;	  
    }
    bool eof;
    TRACE( TRACE_TRX_FLOW, "App: %d TO:t-iter-next \n", xct_id);
    W_DO(t_iter->next(_pssm, eof, *prtrade));
    while(!eof){	
	prtrade->get_value(0, trade_id);
	W_DO(t_iter->next(_pssm, eof, *prtrade));
    }
    lastTradeId = ++trade_id;		  

    return (_pssm->commit_xct());
}

void ShoreTPCEEnv::find_maxtrade_id()
{
    find_maxtrade_id_input_t in;
    long log_space_needed = 0;
 retry:
    W_COERCE(this->db()->begin_xct());
    CHECK_XCT_RETURN(this->xct_find_maxtrade_id(1, in),
		     log_space_needed, retry, this);
    printf("last trade id: %lld\n", lastTradeId);
}




/******************************************************************** 
 *
 * TPC-E TRXs Wrappers
 *
 * @brief: They are wrappers to the functions that execute the transaction
 *         body. Their responsibility is to:
 *
 *         1. Prepare the corresponding input
 *         2. Check the return of the trx function and abort the trx,
 *            if something went wrong
 *         3. Update the tpce db environment statistics
 *
 ********************************************************************/

DEFINE_TRX(ShoreTPCEEnv,broker_volume);
DEFINE_TRX(ShoreTPCEEnv,customer_position);
DEFINE_TRX(ShoreTPCEEnv,market_feed);
DEFINE_TRX(ShoreTPCEEnv,market_watch);
DEFINE_TRX(ShoreTPCEEnv,security_detail);
DEFINE_TRX(ShoreTPCEEnv,trade_lookup);
DEFINE_TRX(ShoreTPCEEnv,trade_order);
DEFINE_TRX(ShoreTPCEEnv,trade_result);
DEFINE_TRX(ShoreTPCEEnv,trade_status);
DEFINE_TRX(ShoreTPCEEnv,trade_update);
DEFINE_TRX(ShoreTPCEEnv,data_maintenance);
DEFINE_TRX(ShoreTPCEEnv,trade_cleanup);

EXIT_NAMESPACE(tpce);    
