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

/** @file:   shore_tpce_env.cpp
 *
 *  @brief:  Declaration of the Shore TPC-E environment (database)
 *
 *  @author: Ippokratis Pandis, Apr 2010
 *  @author: Djordje Jevdjic, Apr 2010
 */

#include "workload/tpce/shore_tpce_env.h"
#include "sm/shore/shore_helper_loader.h"

#include <w_defines.h>
#include "workload/tpce/egen/CE.h"
#include <stdio.h>

using namespace shore;
using namespace TPCE;



// Thread-local row caches

ENTER_NAMESPACE(shore);
DEFINE_ROW_CACHE_TLS(tpce, sector);
DEFINE_ROW_CACHE_TLS(tpce, charge);
DEFINE_ROW_CACHE_TLS(tpce, commission_rate);
DEFINE_ROW_CACHE_TLS(tpce, exchange);
DEFINE_ROW_CACHE_TLS(tpce, industry);
DEFINE_ROW_CACHE_TLS(tpce, status_type);
DEFINE_ROW_CACHE_TLS(tpce, taxrate);
DEFINE_ROW_CACHE_TLS(tpce, trade_type);
DEFINE_ROW_CACHE_TLS(tpce, zip_code);
DEFINE_ROW_CACHE_TLS(tpce, cash_transaction);
DEFINE_ROW_CACHE_TLS(tpce, settlement);
DEFINE_ROW_CACHE_TLS(tpce, trade);
DEFINE_ROW_CACHE_TLS(tpce, trade_history);
DEFINE_ROW_CACHE_TLS(tpce, trade_request);
DEFINE_ROW_CACHE_TLS(tpce, account_permission);
DEFINE_ROW_CACHE_TLS(tpce, broker);
DEFINE_ROW_CACHE_TLS(tpce, company);
DEFINE_ROW_CACHE_TLS(tpce, customer);
DEFINE_ROW_CACHE_TLS(tpce, company_competitor);
DEFINE_ROW_CACHE_TLS(tpce, security);
DEFINE_ROW_CACHE_TLS(tpce, customer_account);
DEFINE_ROW_CACHE_TLS(tpce, daily_market);
DEFINE_ROW_CACHE_TLS(tpce, customer_taxrate);
DEFINE_ROW_CACHE_TLS(tpce, holding);
DEFINE_ROW_CACHE_TLS(tpce, financial);
DEFINE_ROW_CACHE_TLS(tpce, holding_history);
DEFINE_ROW_CACHE_TLS(tpce, address);
DEFINE_ROW_CACHE_TLS(tpce, holding_summary);
DEFINE_ROW_CACHE_TLS(tpce, last_trade);
DEFINE_ROW_CACHE_TLS(tpce, watch_item);
DEFINE_ROW_CACHE_TLS(tpce, news_item);
DEFINE_ROW_CACHE_TLS(tpce, watch_list);
DEFINE_ROW_CACHE_TLS(tpce, news_xref);
EXIT_NAMESPACE(shore);



ENTER_NAMESPACE(tpce);

CCETxnInputGenerator*	m_TxnInputGenerator;
CDM*			m_CDM;
CMEESUT*		meesut;
CMEE* 			mee; 
MFBuffer* MarketFeedInputBuffer;
TRBuffer* TradeResultInputBuffer;

#ifdef COMPILE_FLAT_FILE_LOAD 
FILE *fssec, *fshs;
#endif

/** Exported functions */


unsigned int AutoRand()
{
	struct timeval tv;
	struct tm ltr;
	gettimeofday(&tv, NULL);
	struct tm* lt = localtime_r(&tv.tv_sec, &ltr);
	return (((lt->tm_hour * MinutesPerHour + lt->tm_min) * SecondsPerMinute +
			lt->tm_sec) * MsPerSecond + tv.tv_usec / 1000);
}

void setRNGSeeds(CCETxnInputGenerator* gen, unsigned int UniqueId )
{
    CDateTime   Now;
    INT32       BaseYear;
    INT32       Tmp1, Tmp2;

    Now.GetYMD( &BaseYear, &Tmp1, &Tmp2 );

    // Set the base year to be the most recent year that was a multiple of 5.
    BaseYear -= ( BaseYear % 5 );
    CDateTime   Base( BaseYear, 1, 1 ); // January 1st in the BaseYear

    // Initialize the seed with the current time of day measured in 1/10's of a second.
    // This will use up to 20 bits.
    RNGSEED Seed;
    Seed = Now.MSec() / 100;

    // Now add in the number of days since the base time.
    // The number of days in the 5 year period requires 11 bits.
    // So shift up by that much to make room in the "lower" bits.
    Seed <<= 11;
    Seed += (RNGSEED)((INT64)Now.DayNo() - (INT64)Base.DayNo());

    // So far, we've used up 31 bits.
    // Save the "last" bit of the "upper" 32 for the RNG id.
    // In addition, make room for the caller's 32-bit unique id.
    // So shift a total of 33 bits.
    Seed <<= 33;

    // Now the "upper" 32-bits have been set with a value for RNG 0.
    // Add in the sponsor's unique id for the "lower" 32-bits.
   // Seed += UniqueId;
    Seed += UniqueId;

    // Set the TxnMixGenerator RNG to the unique seed.
    gen->SetRNGSeed( Seed );
//    m_DriverCESettings.cur.TxnMixRNGSeed = Seed;

    // Set the RNG Id to 1 for the TxnInputGenerator.
    Seed |= UINT64_CONST(0x0000000100000000);
    gen->SetRNGSeed( Seed );
//    m_DriverCESettings.cur.TxnInputRNGSeed = Seed;
}


//check the cardinality of all tables to see if it is correctly generated
void printCardinality()
{
   printf("accountPermissionBuffer.getCnt()  %d\n",accountPermissionBuffer.getCnt() );
   printf("customerAccountBuffer.getCnt()  %d\n",customerAccountBuffer.getCnt() );
   printf("customerTaxrateBuffer.getCnt()  %d\n",customerTaxrateBuffer.getCnt() );
   printf("customerBuffer.getCnt()  %d\n",customerBuffer.getCnt() );
   printf("holdingBuffer.getCnt()  %d\n",holdingBuffer.getCnt() );
   printf("watchItemBuffer.getCnt()  %d\n",watchItemBuffer.getCnt() );
   printf("watchListBuffer.getCnt()  %d\n",watchListBuffer.getCnt() );
   printf("brokerBuffer.getCnt()  %d\n",brokerBuffer.getCnt() );
   printf("cashTransactionBuffer.getCnt()  %d\n",cashTransactionBuffer.getCnt() );
   printf("chargeBuffer.getCnt()  %d\n",chargeBuffer.getCnt() );
   printf("holdingHistoryBuffer.getCnt()  %d\n",holdingHistoryBuffer.getCnt() );
   printf("holdingSummaryBuffer.getCnt()  %d\n",holdingSummaryBuffer.getCnt() );
   printf("commissionRateBuffer.getCnt()  %d\n",commissionRateBuffer.getCnt() );
   printf("companyBuffer.getCnt()  %d\n",companyBuffer.getCnt() );
   printf("companyCompetitorBuffer.getCnt()  %d\n",companyCompetitorBuffer.getCnt() );
   printf("dailyMarketBuffer.getCnt()  %d\n",dailyMarketBuffer.getCnt() );
  

   printf("settlementBuffer.getCnt()  %d\n",settlementBuffer.getCnt() );
   printf("tradeBuffer.getCnt()  %d\n",tradeBuffer.getCnt() );
   printf("tradeHistoryBuffer.getCnt()  %d\n",tradeHistoryBuffer.getCnt() );
   printf("tradeTypeBuffer.getCnt()  %d\n",tradeTypeBuffer.getCnt() );


   printf("exchangeBuffer.getCnt()  %d\n",exchangeBuffer.getCnt() );
   printf("financialBuffer.getCnt()  %d\n",financialBuffer.getCnt() );
   printf("industryBuffer.getCnt()  %d\n",industryBuffer.getCnt() );
   printf("newsItemBuffer.getCnt()  %d\n",newsItemBuffer.getCnt() );
   printf("lastTradeBuffer.getCnt()  %d\n",lastTradeBuffer.getCnt() );
   printf("newsXRefBuffer.getCnt()  %d\n",newsXRefBuffer.getCnt() );
   printf("sectorBuffer.getCnt()  %d\n",sectorBuffer.getCnt() );
   printf("securityBuffer.getCnt()  %d\n",securityBuffer.getCnt() );
   printf("addressBuffer.getCnt()  %d\n",addressBuffer.getCnt() );
   printf("statusTypeBuffer.getCnt()  %d\n",statusTypeBuffer.getCnt() );
   printf("taxrateBuffer.getCnt()  %d\n",taxrateBuffer.getCnt() );
   printf("zipCodeBuffer.getCnt()  %d\n",zipCodeBuffer.getCnt() );
}


void testInputs()
{
    for(int i=0; i<5; i++) {
        trade_lookup_input_t in4 = create_trade_lookup_input(0,0);
        in4.print();	
    }
    for(int i=0; i<2; i++) {  trade_lookup_input_t in41 = create_trade_lookup_input(0,0);
        in41.print();
    }
    for(int i=0; i<2; i++) {  market_watch_input_t in5 = create_market_watch_input(0,0);
        in5.print();
    }
    for(int i=0; i<2; i++) {  security_detail_input_t in6 = create_security_detail_input(0,0);
        in6.print();	
    }
    trade_status_input_t in7 = create_trade_status_input(0,0);
    in7.print();
    for(int i=0; i<2; i++) {  trade_update_input_t in8 = create_trade_update_input(0,0);
        in8.print();	
    }
    for(int i=0; i<2; i++) {
        data_maintenance_input_t in9 = create_data_maintenance_input(0,0);
        in9.print();
    }
    for(int i=0; i<2; i++) {
        trade_cleanup_input_t  in10 =   create_trade_cleanup_input(0,0);
        in10.print();
    }
 
    for(int i=0; i<2; i++) {
        trade_result_input_t in10 =   create_trade_result_input(0,0);
        in10.print();
    }
 
    for(int i=0; i<2; i++) {
        market_feed_input_t  in10 =   create_market_feed_input(0,0);
        in10.print();
    } 
}

/******************************************************************** 
 *
 * ShoreTPCEEnv functions
 *
 ********************************************************************/ 



/** Construction  */
ShoreTPCEEnv::ShoreTPCEEnv():
    ShoreEnv(), _num_invalid_input(0)
{
    // read the scaling factor from the configuration file
    
    

    //INITIALIZE EGEN
    _customers = upd_sf() * TPCE_CUSTS_PER_LU;
    _working_days = envVar::instance()->getSysVarInt("wd");
    _scaling_factor_tpce = envVar::instance()->getSysVarInt("sfe");

    char sfe_str[8], wd_str[8], cust_str[8];
    memset(sfe_str,0,8);
    memset(wd_str,0,8);
    memset(cust_str,0,8);
    sprintf(sfe_str, "%d",_scaling_factor_tpce);
    sprintf(wd_str, "%d",_working_days);
    sprintf(cust_str, "%d",_customers);


 
#ifdef COMPILE_FLAT_FILE_LOAD 
     fssec = fopen("shoresecurity.txt","wt");
     fshs = fopen("shorehsummary.txt","wt");
     const char * params[] = {"to_skip", "-i", "./src/workload/tpce/egen/flat/egen_flat_in/","-o", "./src/workload/tpce/egen/flat/egen_flat_out/", "-l", "FLAT", "-f", sfe_str, "-w", wd_str, "-c", cust_str, "-t", cust_str  }; 
     egen_init(15,  (char **)params);  
#else
     const char * params[] = {"to_skip", "-i", "./src/workload/tpce/egen/flat/egen_flat_in/", "-l", "NULL", "-f", sfe_str, "-w", wd_str, "-c", cust_str, "-t", cust_str }; 
     egen_init(13,  (char **)params);      
#endif

     //Initialize Client Transaction Input generator
     m_TxnInputGenerator = transactions_input_init(_customers, _scaling_factor_tpce , _working_days);

     unsigned int seed = AutoRand();
     setRNGSeeds(m_TxnInputGenerator, seed);
  
     m_CDM = data_maintenance_init(_customers, _scaling_factor_tpce, _working_days);
	
     //Initialize Market side
     MarketFeedInputBuffer = new MFBuffer();
     TradeResultInputBuffer = new TRBuffer();
    
     meesut = new CMEESUT();
     meesut->setMFQueue(MarketFeedInputBuffer);
     meesut->setTRQueue(TradeResultInputBuffer);
     mee = market_init( _working_days*8, meesut, AutoRand()); 		

#ifdef TESTING_TPCE
    for(int i=0; i<10; i++) trxs_cnt_executed[i]= trxs_cnt_failed[i]=0;
#endif
        TradeOrderCnt = 0;

}

ShoreTPCEEnv::~ShoreTPCEEnv() 
{
    egen_release();
    if (m_TxnInputGenerator) delete m_TxnInputGenerator;
    if (MarketFeedInputBuffer) delete MarketFeedInputBuffer;
    if (TradeResultInputBuffer) delete TradeResultInputBuffer;
    if (meesut) delete meesut;
}



w_rc_t ShoreTPCEEnv::load_schema()
{
    // create the schema
    _paccount_permission_desc   = new account_permission_t(get_pd());
    _pcustomer_desc   = new customer_t(get_pd());
    _pcustomer_account_desc  = new customer_account_t(get_pd());
    _pcustomer_taxrate_desc  = new customer_taxrate_t(get_pd());
    _pholding_desc  = new holding_t(get_pd());
    _pholding_history_desc  = new holding_history_t(get_pd());
    _pholding_summary_desc  = new holding_summary_t(get_pd());
    _pwatch_item_desc  = new watch_item_t(get_pd());
    _pwatch_list_desc  = new watch_list_t(get_pd());
    _pbroker_desc  = new broker_t(get_pd());
    _pcash_transaction_desc  = new cash_transaction_t(get_pd());
    _pcharge_desc  = new charge_t(get_pd());
    _pcommission_rate_desc  = new commission_rate_t(get_pd());
    _psettlement_desc  = new settlement_t(get_pd());
    _ptrade_desc  = new trade_t(get_pd());
    _ptrade_history_desc  = new trade_history_t(get_pd());
    _ptrade_request_desc  = new trade_request_t(get_pd());
    _ptrade_type_desc  = new trade_type_t(get_pd());
    _pcompany_desc  = new company_t(get_pd());
    _pcompany_competitor_desc  = new company_competitor_t(get_pd());
    _pdaily_market_desc  = new daily_market_t(get_pd());
    _pexchange_desc  = new exchange_t(get_pd());
    _pfinancial_desc  = new financial_t(get_pd());
    _pindustry_desc  = new industry_t(get_pd());
    _plast_trade_desc  = new last_trade_t(get_pd());
    _pnews_item_desc  = new news_item_t(get_pd());
    _pnews_xref_desc  = new news_xref_t(get_pd());
    _psector_desc  = new sector_t(get_pd());
    _psecurity_desc  = new security_t(get_pd());
    _paddress_desc  = new address_t(get_pd());
    _pstatus_type_desc  = new status_type_t(get_pd());
    _ptaxrate_desc  = new taxrate_t(get_pd());
    _pzip_code_desc  = new zip_code_t(get_pd());


    //     initiate the table managers
    _paccount_permission_man   = new account_permission_man_impl(_paccount_permission_desc.get());
    _pcustomer_man   = new customer_man_impl(_pcustomer_desc.get());
    _pcustomer_account_man  = new customer_account_man_impl(_pcustomer_account_desc.get());
    _pcustomer_taxrate_man  = new customer_taxrate_man_impl(_pcustomer_taxrate_desc.get());
    _pholding_man   = new holding_man_impl(_pholding_desc.get());
    _pholding_history_man   = new holding_history_man_impl(_pholding_history_desc.get());
    _pholding_summary_man  = new holding_summary_man_impl(_pholding_summary_desc.get());
    _pwatch_item_man  = new watch_item_man_impl(_pwatch_item_desc.get());
    _pwatch_list_man  = new watch_list_man_impl(_pwatch_list_desc.get());
    _pbroker_man   = new broker_man_impl(_pbroker_desc.get());
    _pcash_transaction_man   = new cash_transaction_man_impl(_pcash_transaction_desc.get());
    _pcharge_man  = new charge_man_impl(_pcharge_desc.get());
    _pcommission_rate_man  = new commission_rate_man_impl(_pcommission_rate_desc.get());
    _psettlement_man   = new settlement_man_impl(_psettlement_desc.get());
    _ptrade_man   = new trade_man_impl(_ptrade_desc.get());
    _ptrade_history_man  = new trade_history_man_impl(_ptrade_history_desc.get());
    _ptrade_request_man  = new trade_request_man_impl(_ptrade_request_desc.get());
    _ptrade_type_man  = new trade_type_man_impl(_ptrade_type_desc.get());
    _pcompany_man   = new company_man_impl(_pcompany_desc.get());
    _pcompany_competitor_man  = new company_competitor_man_impl(_pcompany_competitor_desc.get());
    _pdaily_market_man  = new daily_market_man_impl(_pdaily_market_desc.get());
    _pexchange_man   = new exchange_man_impl(_pexchange_desc.get());
    _pfinancial_man  = new financial_man_impl(_pfinancial_desc.get());
    _pindustry_man  = new industry_man_impl(_pindustry_desc.get());
    _plast_trade_man  = new last_trade_man_impl(_plast_trade_desc.get());
#warning we must set this to 10000 insted of 100.000
    _pnews_item_man   = new news_item_man_impl(_pnews_item_desc.get());
    _pnews_xref_man  = new news_xref_man_impl(_pnews_xref_desc.get()); 
    _psector_man  = new sector_man_impl(_psector_desc.get());
    _psecurity_man  = new security_man_impl(_psecurity_desc.get());
    _paddress_man   = new address_man_impl(_paddress_desc.get());
    _pstatus_type_man  = new status_type_man_impl(_pstatus_type_desc.get());
    _ptaxrate_man  = new taxrate_man_impl(_ptaxrate_desc.get());
    _pzip_code_man  = new zip_code_man_impl(_pzip_code_desc.get());

    return (RCOK);
}


/******************************************************************** 
 *
 *  @fn:    info()
 *
 *  @brief: Prints information about the current db instance status
 *
 ********************************************************************/

int ShoreTPCEEnv::info() const
{
    TRACE( TRACE_ALWAYS, "SF      = (%d)\n", _scaling_factor);
    return (0);
}


/******************************************************************** 
 *
 *  @fn:    statistics
 *
 *  @brief: Prints statistics for TPCE 
 *
 ********************************************************************/

int ShoreTPCEEnv::statistics() 
{
    // read the current trx statistics
    CRITICAL_SECTION(cs, _statmap_mutex);
    ShoreTPCETrxStats rval;
    rval -= rval; // dirty hack to set all zeros
    for (statmap_t::iterator it=_statmap.begin(); it != _statmap.end(); ++it) 
	rval += *it->second;

    TRACE( TRACE_STATISTICS, "BrokerVolume. Att (%d). Abt (%d). Dld (%d)\n",
	   rval.attempted.broker_volume,
	   rval.failed.broker_volume,
	   rval.deadlocked.broker_volume);
     
    TRACE( TRACE_STATISTICS, "CustomerPosition. Att (%d). Abt (%d). Dld (%d)\n",
	   rval.attempted.customer_position,
	   rval.failed.customer_position,
	   rval.deadlocked.customer_position);
    
    TRACE( TRACE_STATISTICS, "MarketFeed. Att (%d). Abt (%d). Dld (%d)\n",
	   rval.attempted.market_feed,
	   rval.failed.market_feed,
	   rval.deadlocked.market_feed);
    
    TRACE( TRACE_STATISTICS, "MarketWatch. Att (%d). Abt (%d). Dld (%d)\n",
	   rval.attempted.market_watch,
	   rval.failed.market_watch,
	   rval.deadlocked.market_watch);
    
   TRACE( TRACE_STATISTICS, "SecurityDetail. Att (%d). Abt (%d). Dld (%d)\n",
	  rval.attempted.security_detail,
	  rval.failed.security_detail,
	  rval.deadlocked.security_detail);
   
   TRACE( TRACE_STATISTICS, "TradeLookup. Att (%d). Abt (%d). Dld (%d)\n",
	  rval.attempted.trade_lookup,
	  rval.failed.trade_lookup,
	  rval.deadlocked.trade_lookup);
   
   TRACE( TRACE_STATISTICS, "TradeOrder. Att (%d). Abt (%d). Dld (%d)\n",
	  rval.attempted.trade_order,
	  rval.failed.trade_order,
	  rval.deadlocked.trade_order);
   
   TRACE( TRACE_STATISTICS, "TradeResult. Att (%d). Abt (%d). Dld (%d)\n",
	  rval.attempted.trade_result,
	  rval.failed.trade_result,
	  rval.deadlocked.trade_result);
   
   TRACE( TRACE_STATISTICS, "TradeStatus. Att (%d). Abt (%d). Dld (%d)\n",
	  rval.attempted.trade_status,
	  rval.failed.trade_status,
	  rval.deadlocked.trade_status);

   TRACE( TRACE_STATISTICS, "TradeUpdate. Att (%d). Abt (%d). Dld (%d)\n",
	  rval.attempted.trade_update,
	  rval.failed.trade_update,
	  rval.deadlocked.trade_update);
   
   TRACE( TRACE_STATISTICS, "DataMaintenance. Att (%d). Abt (%d). Dld (%d)\n",
	  rval.attempted.data_maintenance,
	  rval.failed.data_maintenance,
	  rval.deadlocked.data_maintenance);

   TRACE( TRACE_STATISTICS, "TradeCleanup. Att (%d). Abt (%d). Dld (%d)\n",
	  rval.attempted.trade_cleanup,
	  rval.failed.trade_cleanup,
	  rval.deadlocked.trade_cleanup);
   
   ShoreEnv::statistics();
   
   return (0);
}


/******************************************************************** 
 *
 *  @fn:    start()
 *
 *  @brief: Starts the tpce env
 *
 ********************************************************************/

int ShoreTPCEEnv::start()
{
    return (ShoreEnv::start());
}

int ShoreTPCEEnv::stop()
{
    return (ShoreEnv::stop());
}


/******************************************************************** 
 *
 *  @fn:    set_sf/qf
 *
 *  @brief: Set the scaling and queried factors
 *
 ********************************************************************/



void ShoreTPCEEnv::set_cust(const int aSF)
{
    if ((aSF >  0) && ((aSF % 1000) == 0)) {
        TRACE( TRACE_ALWAYS, "New #customers: %d\n", aSF);
        _customers = aSF;
    }
    else {
        TRACE( TRACE_ALWAYS, "Invalid #customers input: %d\n", aSF);
    }
}

void ShoreTPCEEnv::set_wd(const int aSF)
{
    if (aSF > 0) {
        TRACE( TRACE_ALWAYS, "New working_days factor: %d\n", aSF);
        _working_days = aSF;
    }
    else {
        TRACE( TRACE_ALWAYS, "Invalid working_days input: %d\n", aSF);
    }
}

void ShoreTPCEEnv::set_sfe(const int aSF)
{
    if (aSF > 0) {
        TRACE( TRACE_ALWAYS, "New scaling_factor_tpce: %d\n", aSF);
        _scaling_factor_tpce = aSF;
    }
    else {
        TRACE( TRACE_ALWAYS, "Invalid scaling_factor_tpce input: %d\n", aSF);
    }
}


// 
struct ShoreTPCEEnv::checkpointer_t : public thread_t {
    ShoreTPCEEnv* _env;
    checkpointer_t(ShoreTPCEEnv* env) : thread_t("TPC-E Load Checkpointer"), _env(env) { }
    virtual void work();
};




/****************************************************************** 
 *
 * @struct: table_creator_t
 *
 * @brief:  Helper class for creating the environment tables and
 *          loading a number of records in a single-threaded fashion
 *
 ******************************************************************/

class ShoreTPCEEnv::table_builder_t : public thread_t {
    ShoreTPCEEnv* _env;
    int my_load_unit;
public:
    table_builder_t(ShoreTPCEEnv* env)
	: thread_t("TPC-E loader"), _env(env) { }
    virtual void work();
};

void ShoreTPCEEnv::table_builder_t::work() 
{
    w_rc_t e = RCOK;

    //populating fixed
    populate_small_input_t in;
    long log_space_needed = 0;
    _env->read_small();
 retry:
    W_COERCE(_env->db()->begin_xct());

    if(log_space_needed > 0) {
        W_COERCE(_env->db()->xct_reserve_log_space(log_space_needed));
    }

    e = _env->xct_populate_small(1, in);    
    CHECK_XCT_RETURN(e,log_space_needed,retry,_env);
    _env->release_small();

    //populating scaling tables
    _env->populate_address(); 
    _env->populate_customer();
    _env->populate_ca_and_ap();
    _env->populate_customer_taxrate();
    _env->populate_wl_and_wi(); 

    _env->populate_company(); 
    _env->populate_company_competitor();
    _env->populate_daily_market();
    _env->populate_financial();
    _env->populate_last_trade();
    _env-> populate_ni_and_nx();
    _env->populate_security();

    //populate growing tables
    _env-> populate_growing();
    printCardinality();
#ifdef COMPILE_FLAT_FILE_LOAD 
    fclose(fshs);
    fclose(fssec);
#endif
    //  testInputs();
    _env->find_maxtrade_id();

}



/****************************************************************** 
 *
 * @class: table_builder_t
 *
 * @brief:  Helper class for loading the environment tables
 *
 ******************************************************************/


struct ShoreTPCEEnv::table_creator_t : public thread_t {
    ShoreTPCEEnv* _env;
     table_creator_t(ShoreTPCEEnv* env)
	: thread_t("TPC-E Table Creator"), _env(env) { }
    virtual void work();
};

void ShoreTPCEEnv::checkpointer_t::work() 
{
    bool volatile* loaded = &_env->_loaded;
    while(!*loaded) {
	_env->set_measure(MST_MEASURE);
	for(int i=0; i < 60 && !*loaded; i++) 
	    ::sleep(1);
		
        TRACE( TRACE_ALWAYS, "db checkpoint - start\n");
        _env->checkpoint();
        TRACE( TRACE_ALWAYS, "db checkpoint - end\n");
    }
    _env->set_measure(MST_PAUSE);
}



void  ShoreTPCEEnv::table_creator_t::work() 
{
    /* create the tables */
     W_COERCE(_env->db()->begin_xct());
     W_COERCE(_env->_paccount_permission_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pcustomer_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pcustomer_account_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pcustomer_taxrate_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pholding_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pholding_history_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pholding_summary_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pwatch_list_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pwatch_item_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pbroker_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pcash_transaction_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pcharge_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pcommission_rate_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_psettlement_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_ptrade_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_ptrade_history_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_ptrade_request_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_ptrade_type_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pcompany_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pcompany_competitor_desc->create_physical_table(_env->db()));    
     W_COERCE(_env->_pdaily_market_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pexchange_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pfinancial_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pindustry_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_plast_trade_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pnews_item_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pnews_xref_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_psector_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_psecurity_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_paddress_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pstatus_type_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_ptaxrate_desc->create_physical_table(_env->db()));
     W_COERCE(_env->_pzip_code_desc->create_physical_table(_env->db()));
     W_COERCE(_env->db()->commit_xct());
 
//     /*
//       create 10k accounts in each partition to buffer workers from each other
//      */
//     for(long i=-1; i < _pcount; i++) {
// 	long a_id = i*_psize;
// 	populate_db_input_t in(_sf, a_id);
// 	trx_result_tuple_t out;
// 	fprintf(stderr, "Populating %d a_ids starting with %ld\n", TPCE_ACCOUNTS_CREATED_PER_POP_XCT, a_id);
// 	W_COERCE(_env->db()->begin_xct());
// 	W_COERCE(_env->xct_populate_db(a_id, out, in));
//     }

//     W_COERCE(_env->db()->begin_xct());
//     W_COERCE(_env->_post_init_impl());
//     W_COERCE(_env->db()->commit_xct());
}


/********
 ******** Caution: The functions below should be invoked inside
 ********          the context of a smthread
 ********/


/****************************************************************** 
 *
 * @fn:    loaddata()
 *
 * @brief: Loads the data for all the TPCE tables, given the current
 *         scaling factor value. During the loading the SF cannot be
 *         changed.
 *
 ******************************************************************/

w_rc_t ShoreTPCEEnv::loaddata() 
{
  w_rc_t e;
   // 0. lock the loading status and the scaling factor
    CRITICAL_SECTION(load_cs, _load_mutex);
    if (_loaded) {
        TRACE( TRACE_TRX_FLOW, 
               "Env already loaded. Doing nothing...\n");
        return (RCOK);
    }        
    CRITICAL_SECTION(scale_cs, _scaling_mutex);
    time_t tstart = time(NULL); 

    // 2. Fire up the table creator and baseline loader
    {
	guard<table_creator_t> tc;
	tc = new table_creator_t(this);
	tc->fork();
	tc->join();
    }
    // 3. Fire up a checkpointer 
    guard<checkpointer_t> chk(new checkpointer_t(this));
    chk->fork();
 

    // 4. Fire up the loaders
    TRACE( TRACE_ALWAYS, "Firing up %d loaders ..\n", 1);
    {
      guard<table_builder_t>loader = new table_builder_t(this);
      // array_guard_t< guard<table_builder_t> > loaders(new guard<table_builder_t>[loaders_to_use]);
      loader->fork();
      loader->join();
    }  

    // 5. Print stats
    time_t tstop = time(NULL);
    TRACE( TRACE_ALWAYS, "Loading finished in (%d) secs...\n", (tstop - tstart));

    // 6. Notify that the env is loaded
    _loaded = true;
    chk->join();
   return (RCOK);
}




/****************************************************************** 
 *
 * @fn:    check_consistency()
 *
 * @brief: Iterates over all tables and checks consistency between
 *         the values stored in the base table (file) and the 
 *         corresponding indexes.
 *
 ******************************************************************/

w_rc_t ShoreTPCEEnv::check_consistency()
{
    // not loaded from files, so no inconsistency possible
    return RCOK;
}


/****************************************************************** 
 *
 * @fn:    warmup()
 *
 * @brief: Touches the entire database - For memory-fitting databases
 *         this is enough to bring it to load it to memory
 *
 ******************************************************************/

w_rc_t ShoreTPCEEnv::warmup()
{
    return (check_consistency());
}


/******************************************************************** 
 *
 *  @fn:    dump
 *
 *  @brief: Print information for all the tables in the environment
 *
 ********************************************************************/

int ShoreTPCEEnv::dump()
{
    assert (0); // IP: not implemented yet
    return (0);
}


int ShoreTPCEEnv::conf()
{
    // reread the params
    ShoreEnv::conf();
    upd_sf();
    upd_worker_cnt();
    return (0);
}


int ShoreTPCEEnv::post_init() 
{
    conf();
    if (get_pd() & PD_PADDED) {

        W_COERCE(db()->begin_xct());
        w_rc_t rc = _post_init_impl();
        if(rc.is_error()) {
            db()->abort_xct();
            return (rc.err_num());
        }
        else {
            TRACE( TRACE_ALWAYS, "-> Done\n");
            db()->commit_xct();
        }    
    }

    this->find_maxtrade_id();
    return (0);
}


/********************************************************************* 
 *
 *  @fn:    _post_init_impl
 *
 *  @brief: Makes Post initialization operations
 *
 *********************************************************************/ 

w_rc_t ShoreTPCEEnv::_post_init_impl() 
{
    // Doing nothing in TPC-E
    return (RCOK);
}
  

/********************************************************************* 
 *
 *  @fn:   db_print
 *
 *  @brief: Prints the current tpce tables to files
 *
 *********************************************************************/ 

w_rc_t ShoreTPCEEnv::db_print(int lines) 
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // print tables
    W_DO(_paccount_permission_man->print_table(_pssm, lines));    
    W_DO(_pcustomer_man->print_table(_pssm, lines));
    W_DO(_pcustomer_account_man->print_table(_pssm, lines));
    W_DO(_pcustomer_taxrate_man->print_table(_pssm, lines));
    W_DO(_pholding_man->print_table(_pssm, lines));
    W_DO(_pholding_history_man->print_table(_pssm, lines));
    W_DO(_pholding_summary_man->print_table(_pssm, lines));
    W_DO(_pwatch_item_man->print_table(_pssm, lines));
    W_DO(_pwatch_list_man->print_table(_pssm, lines));
    W_DO(_pbroker_man->print_table(_pssm, lines));
    W_DO(_pcash_transaction_man->print_table(_pssm, lines));
    W_DO(_pcharge_man->print_table(_pssm, lines));
    W_DO(_pcommission_rate_man->print_table(_pssm, lines));
    W_DO(_psettlement_man->print_table(_pssm, lines));
    W_DO(_ptrade_man->print_table(_pssm, lines));
    W_DO(_ptrade_history_man->print_table(_pssm, lines));
    W_DO(_ptrade_request_man->print_table(_pssm, lines));
    W_DO(_ptrade_type_man->print_table(_pssm, lines));
    W_DO(_pcompany_man->print_table(_pssm, lines));
    W_DO(_pcompany_competitor_man->print_table(_pssm, lines));
    W_DO(_pdaily_market_man->print_table(_pssm, lines));
    W_DO(_pexchange_man->print_table(_pssm, lines));
    W_DO(_pfinancial_man->print_table(_pssm, lines));
    W_DO(_pindustry_man->print_table(_pssm, lines));
    W_DO(_plast_trade_man->print_table(_pssm, lines));
    W_DO(_pnews_item_man->print_table(_pssm, lines));
    W_DO(_pnews_xref_man->print_table(_pssm, lines));
    W_DO(_psector_man->print_table(_pssm, lines));
    W_DO(_psecurity_man->print_table(_pssm, lines));
    W_DO(_paddress_man->print_table(_pssm, lines));
    W_DO(_pstatus_type_man->print_table(_pssm, lines));
    W_DO(_ptaxrate_man->print_table(_pssm, lines));
    W_DO(_pzip_code_man->print_table(_pssm, lines));

    return (RCOK);
}


/********************************************************************* 
 *
 *  @fn:   db_fetch
 *
 *  @brief: Fetches the current tpce tables to buffer pool
 *
 *********************************************************************/ 

w_rc_t ShoreTPCEEnv::db_fetch() 
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // fetch tables
    W_DO(_paccount_permission_man->fetch_table(_pssm));    
    W_DO(_pcustomer_man->fetch_table(_pssm));
    W_DO(_pcustomer_account_man->fetch_table(_pssm));
    W_DO(_pcustomer_taxrate_man->fetch_table(_pssm));
    W_DO(_pholding_man->fetch_table(_pssm));
    W_DO(_pholding_history_man->fetch_table(_pssm));
    W_DO(_pholding_summary_man->fetch_table(_pssm));
    W_DO(_pwatch_item_man->fetch_table(_pssm));
    W_DO(_pwatch_list_man->fetch_table(_pssm));
    W_DO(_pbroker_man->fetch_table(_pssm));
    W_DO(_pcash_transaction_man->fetch_table(_pssm));
    W_DO(_pcharge_man->fetch_table(_pssm));
    W_DO(_pcommission_rate_man->fetch_table(_pssm));
    W_DO(_psettlement_man->fetch_table(_pssm));
    W_DO(_ptrade_man->fetch_table(_pssm));
    W_DO(_ptrade_history_man->fetch_table(_pssm));
    W_DO(_ptrade_request_man->fetch_table(_pssm));
    W_DO(_ptrade_type_man->fetch_table(_pssm));
    W_DO(_pcompany_man->fetch_table(_pssm));
    W_DO(_pcompany_competitor_man->fetch_table(_pssm));
    W_DO(_pdaily_market_man->fetch_table(_pssm));
    W_DO(_pexchange_man->fetch_table(_pssm));
    W_DO(_pfinancial_man->fetch_table(_pssm));
    W_DO(_pindustry_man->fetch_table(_pssm));
    W_DO(_plast_trade_man->fetch_table(_pssm));
    W_DO(_pnews_item_man->fetch_table(_pssm));
    W_DO(_pnews_xref_man->fetch_table(_pssm));
    W_DO(_psector_man->fetch_table(_pssm));
    W_DO(_psecurity_man->fetch_table(_pssm));
    W_DO(_paddress_man->fetch_table(_pssm));
    W_DO(_pstatus_type_man->fetch_table(_pssm));
    W_DO(_ptaxrate_man->fetch_table(_pssm));
    W_DO(_pzip_code_man->fetch_table(_pssm));

    return (RCOK);
}


EXIT_NAMESPACE(tpce);
