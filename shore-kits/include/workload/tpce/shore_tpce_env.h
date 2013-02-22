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

/** @file:   shore_tpce_env.h
 *
 *  @brief:  Definition of the Shore TPC-E environment
 *
 *  @author: Ippokratis Pandis, Apr 2010
 *  @author: Djordje Jevdjic, Apr 2010
 */

#ifndef __SHORE_TPCE_ENV_H
#define __SHORE_TPCE_ENV_H


#include "sm_vas.h"
#include "util.h"

#include "workload/tpce/tpce_input.h"

#include "sm/shore/shore_env.h"
#include "sm/shore/shore_asc_sort_buf.h"
#include "sm/shore/shore_desc_sort_buf.h"
#include "sm/shore/shore_trx_worker.h"

#include "workload/tpce/shore_tpce_schema_man.h"
#include "workload/tpce/egen/EGenLoader_stdafx.h"
#include "workload/tpce/egen/EGenStandardTypes.h"
#include "workload/tpce/egen/EGenTables_stdafx.h"
#include "workload/tpce/egen/Table_Defs.h"
#include "workload/tpce/shore_tpce_egen.h"
#include "workload/tpce/tpce_input.h"
#include "workload/tpce/egen/DM.h"

#include <map>

using namespace shore;
using namespace TPCE;


//#define TESTING_TPCE

int egen_init(int argc, char* argv[]);
void egen_release();
extern CGenerateAndLoad*  pGenerateAndLoad;

ENTER_NAMESPACE(tpce);

using std::map;

extern AccountPermissionBuffer accountPermissionBuffer;
extern CustomerBuffer customerBuffer ;
extern CustomerAccountBuffer customerAccountBuffer ;
extern CustomerTaxrateBuffer  customerTaxrateBuffer ;
extern HoldingBuffer holdingBuffer;
extern HoldingHistoryBuffer holdingHistoryBuffer;
extern HoldingSummaryBuffer holdingSummaryBuffer;
extern WatchItemBuffer watchItemBuffer ;
extern WatchListBuffer watchListBuffer;

extern BrokerBuffer brokerBuffer;
extern CashTransactionBuffer cashTransactionBuffer;
extern ChargeBuffer chargeBuffer;
extern CommissionRateBuffer commissionRateBuffer;
extern SettlementBuffer settlementBuffer;
extern TradeBuffer tradeBuffer;
extern TradeHistoryBuffer tradeHistoryBuffer;
extern TradeTypeBuffer tradeTypeBuffer;


extern CompanyBuffer companyBuffer ;
extern CompanyCompetitorBuffer companyCompetitorBuffer;
extern DailyMarketBuffer dailyMarketBuffer;
extern ExchangeBuffer exchangeBuffer;
extern FinancialBuffer financialBuffer;
extern IndustryBuffer industryBuffer;
extern LastTradeBuffer lastTradeBuffer;
extern NewsItemBuffer newsItemBuffer;
extern NewsXRefBuffer newsXRefBuffer;//big
extern SectorBuffer sectorBuffer;
extern SecurityBuffer securityBuffer;


extern AddressBuffer addressBuffer;
extern StatusTypeBuffer statusTypeBuffer;
extern TaxrateBuffer taxrateBuffer ;
extern ZipCodeBuffer zipCodeBuffer ;


#ifdef TESTING_TPCE
extern int trxs_cnt_executed[];
extern int trxs_cnt_failed[];
#endif

extern int TradeOrderCnt;


/******************************************************************** 
 * 
 *  ShoreTPCEEnv Stats
 *  
 *  Shore TPC-E Database transaction statistics
 *
 ********************************************************************/

struct ShoreTPCETrxCount
{
    uint broker_volume;
    uint customer_position;
    uint market_feed;
    uint market_watch;
    uint security_detail;
    uint trade_lookup;
    uint trade_order;
    uint trade_result;
    uint trade_status;
    uint trade_update;
    uint data_maintenance;
    uint trade_cleanup;

    ShoreTPCETrxCount& operator+=(ShoreTPCETrxCount const& rhs) {
        broker_volume += rhs.broker_volume;
        customer_position += rhs.customer_position;
        market_feed += rhs.market_feed;
        market_watch += rhs.market_watch;
        security_detail += rhs.security_detail;
        trade_lookup += rhs.trade_lookup;
        trade_order += rhs.trade_order;
        trade_result += rhs.trade_result;
        trade_status += rhs.trade_status;
        trade_update += rhs.trade_update;
        data_maintenance += rhs.data_maintenance;
        trade_cleanup += rhs.trade_cleanup;
        
	return (*this);
    }

    ShoreTPCETrxCount& operator-=(ShoreTPCETrxCount const& rhs) {
        broker_volume -= rhs.broker_volume;
        customer_position -= rhs.customer_position;
        market_feed -= rhs.market_feed;
        market_watch -= rhs.market_watch;
        security_detail -= rhs.security_detail;
        trade_lookup -= rhs.trade_lookup;
        trade_order -= rhs.trade_order;
        trade_result -= rhs.trade_result;
        trade_status -= rhs.trade_status;
        trade_update -= rhs.trade_update;
        data_maintenance -= rhs.data_maintenance;
        trade_cleanup -= rhs.trade_cleanup;

	return (*this);
    }

    const int total() const {
        return (broker_volume + customer_position + market_feed +
                market_watch + security_detail + trade_lookup +
                trade_order + trade_result + trade_status +
                trade_update + data_maintenance + trade_cleanup);
    }
    
}; // EOF: ShoreTPCETrxCount


struct ShoreTPCETrxStats
{
    ShoreTPCETrxCount attempted;
    ShoreTPCETrxCount failed;
    ShoreTPCETrxCount deadlocked;

    ShoreTPCETrxStats& operator+=(ShoreTPCETrxStats const& other) {
        attempted  += other.attempted;
        failed     += other.failed;
        deadlocked += other.deadlocked;
        return (*this);
    }

    ShoreTPCETrxStats& operator-=(ShoreTPCETrxStats const& other) {
        attempted  -= other.attempted;
        failed     -= other.failed;
        deadlocked -= other.deadlocked;
        return (*this);
    }

}; // EOF: ShoreTPCETrxStats



/******************************************************************** 
 * 
 *  ShoreTPCEEnv
 *  
 *  Shore TPC-E Database.
 *
 ********************************************************************/

class ShoreTPCEEnv : public ShoreEnv
{
public:
    typedef std::map<pthread_t, ShoreTPCETrxStats*> statmap_t;

    class table_builder_t;
    class table_creator_t;
    struct checkpointer_t;

protected:       
    /*
     * note: PIN: the definition of the scaling factor in tpce is different from
     *            the one we have been using so far.
     *            scaling factor means the ratio of the cardinality of the customer
     *            table to throughput (measured as tpsE) according to TPCE-E spec
     *            the "load unit" term from the TPC-E spec is the same as the
     *            "scaling factor" term we have been using in the kits.
     */
    int             _customers;
    int             _working_days; 
    int             _scaling_factor_tpce; 

private:
    w_rc_t _post_init_impl();

    //helper functions for loading
    w_rc_t _load_one_sector(rep_row_t& areprow, PSECTOR_ROW record);
    w_rc_t _load_one_charge( rep_row_t& areprow, PCHARGE_ROW record);   
    w_rc_t _load_one_commission_rate(rep_row_t& areprow, PCOMMISSION_RATE_ROW record);
    w_rc_t _load_one_exchange(rep_row_t& areprow, PEXCHANGE_ROW record);   
    w_rc_t _load_one_industry(rep_row_t& areprow, PINDUSTRY_ROW record);
    w_rc_t _load_one_status_type(rep_row_t& areprow, PSTATUS_TYPE_ROW record);   
    w_rc_t _load_one_taxrate(rep_row_t& areprow, PTAXRATE_ROW record);
    w_rc_t _load_one_trade_type(rep_row_t& areprow, PTRADE_TYPE_ROW record);   
    w_rc_t _load_one_zip_code(rep_row_t& areprow, PZIP_CODE_ROW record);

    w_rc_t _load_one_cash_transaction(rep_row_t& areprow, PCASH_TRANSACTION_ROW record);   
    w_rc_t _load_one_settlement(rep_row_t& areprow, PSETTLEMENT_ROW record);
    w_rc_t _load_one_trade(rep_row_t& areprow, PTRADE_ROW record);   
    w_rc_t _load_one_trade_history(rep_row_t& areprow, PTRADE_HISTORY_ROW record);
    w_rc_t _load_one_trade_request(rep_row_t& areprow, PTRADE_REQUEST_ROW record);   

    w_rc_t _load_one_account_permission(rep_row_t& areprow, PACCOUNT_PERMISSION_ROW record);
    w_rc_t _load_one_broker(rep_row_t& areprow, PBROKER_ROW record);   
    w_rc_t _load_one_company(rep_row_t& areprow, PCOMPANY_ROW record);
    w_rc_t _load_one_customer(rep_row_t& areprow, PCUSTOMER_ROW record);   
    w_rc_t _load_one_company_competitor(rep_row_t& areprow, PCOMPANY_COMPETITOR_ROW record);
    w_rc_t _load_one_security(rep_row_t& areprow, PSECURITY_ROW record);   
    w_rc_t _load_one_customer_account(rep_row_t& areprow, PCUSTOMER_ACCOUNT_ROW record);
    w_rc_t _load_one_daily_market(rep_row_t& areprow, PDAILY_MARKET_ROW record);   
    w_rc_t _load_one_customer_taxrate(rep_row_t& areprow, PCUSTOMER_TAXRATE_ROW record);
    w_rc_t _load_one_holding(rep_row_t& areprow, PHOLDING_ROW record);   
    w_rc_t _load_one_financial(rep_row_t& areprow, PFINANCIAL_ROW record);
    w_rc_t _load_one_holding_history(rep_row_t& areprow, PHOLDING_HISTORY_ROW record);   
    w_rc_t _load_one_address(rep_row_t& areprow, PADDRESS_ROW record);
    w_rc_t _load_one_holding_summary(rep_row_t& areprow, PHOLDING_SUMMARY_ROW record);   
    w_rc_t _load_one_last_trade(rep_row_t& areprow, PLAST_TRADE_ROW record);
    w_rc_t _load_one_watch_item(rep_row_t& areprow, PWATCH_ITEM_ROW record);   
    w_rc_t _load_one_news_item(rep_row_t& areprow, PNEWS_ITEM_ROW record);
    w_rc_t _load_one_watch_list(rep_row_t& areprow, PWATCH_LIST_ROW record);   
    w_rc_t _load_one_news_xref(rep_row_t& areprow, PNEWS_XREF_ROW record);


    void _read_sector();
    void _read_charge();   
    void _read_commission_rate();
    void _read_exchange();   
    void _read_industry();
    void _read_status_type();   
    void _read_taxrate();
    void _read_trade_type();   
    void _read_zip_code();

    void _read_cash_transaction();   
    void _read_settlement();
    void _read_trade();   
    void _read_trade_history();

    void _read_ca_and_ap();
    void _read_broker();   
    void _read_company();
    void _read_customer();   
    void _read_company_competitor();
    void _read_security();   
    void _read_daily_market();   
    void _read_customer_taxrate();
    void _read_holding();   
    void _read_financial();
    void _read_holding_history();   
    void _read_address();
    void _read_holding_summary();   
    void _read_last_trade();
    void _read_wl_and_wi();   
    void _read_ni_and_nx();
    void _read_trade_unit();

    
public:    

    /** Construction  */
    ShoreTPCEEnv();

    virtual ~ShoreTPCEEnv();

    // DB INTERFACE

    virtual int set(envVarMap* /*vars*/) { return(0); /* do nothing */ };
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
  
    int dump();

    // --- scaling and querying factor --- //  
    void set_cust(const int aC);
    inline int get_cust() { return (_customers); }  

    void set_wd(const int awd);
    inline int get_wd() { return (_working_days); }

    void set_sfe(const int asf);
    inline int get_sfe() { return (_scaling_factor_tpce); }

    
    // PIN: to count the aborts due to invalid input in TRADE_RESULT and MARKET_FEED
    //      will be reported in the output
    uint _num_invalid_input; 
    virtual void print_throughput(const double iQueriedSF, 
                                  const int iSpread, 
                                  const int iNumOfThreads,
                                  const double delay,
                                  const ulong_t mioch,
                                  const double avgcpuusage);

    void read_small(){
        pGenerateAndLoad->InitCharge();
        _read_charge();
        pGenerateAndLoad->InitCommissionRate();
        _read_commission_rate();
        pGenerateAndLoad->InitExchange();
        _read_exchange();
        pGenerateAndLoad->InitIndustry();
        _read_industry();
        pGenerateAndLoad->InitSector();
        _read_sector();
        pGenerateAndLoad->InitStatusType();  
        _read_status_type();
        pGenerateAndLoad->InitTaxrate();
        _read_taxrate();
        pGenerateAndLoad->InitTradeType();
        _read_trade_type();
        pGenerateAndLoad->InitZipCode();
        _read_zip_code();
    }

    void release_small(){
        chargeBuffer.release();
        commissionRateBuffer.release();
        exchangeBuffer.release();
        industryBuffer.release();
        sectorBuffer.release();
        statusTypeBuffer.release();
        taxrateBuffer.release();
        tradeTypeBuffer.release();
        zipCodeBuffer.release();
    }
    
    void populate_customer();
    void populate_address();
    void populate_ca_and_ap();
    void populate_wl_and_wi();
    void populate_ni_and_nx();
    void populate_last_trade();
    void populate_company();
    void populate_company_competitor();
    void populate_daily_market();
    void populate_financial();
    void populate_security();
    void populate_customer_taxrate();
    void populate_broker();
    void populate_holding();
    void populate_holding_summary();
    void populate_unit_trade();
    void populate_growing();
    void find_maxtrade_id();
    // Public methods //    

    // --- operations over tables --- //
    w_rc_t loaddata();  
    w_rc_t warmup();
    w_rc_t check_consistency();


    // TPCE Tables

    // Fixed tables
    DECLARE_TABLE(sector_t,sector_man_impl,sector);
    DECLARE_TABLE(charge_t,charge_man_impl,charge);
    DECLARE_TABLE(commission_rate_t,commission_rate_man_impl,commission_rate);
    DECLARE_TABLE(exchange_t,exchange_man_impl,exchange);
    DECLARE_TABLE(industry_t,industry_man_impl,industry);
    DECLARE_TABLE(status_type_t,status_type_man_impl,status_type);
    DECLARE_TABLE(taxrate_t,taxrate_man_impl,taxrate);
    DECLARE_TABLE(trade_type_t,trade_type_man_impl,trade_type);
    DECLARE_TABLE(zip_code_t,zip_code_man_impl,zip_code);

    // Growing tables
    DECLARE_TABLE(cash_transaction_t,cash_transaction_man_impl,cash_transaction);
    DECLARE_TABLE(settlement_t,settlement_man_impl,settlement);
    DECLARE_TABLE(trade_t,trade_man_impl,trade);
    DECLARE_TABLE(trade_history_t,trade_history_man_impl,trade_history);
    DECLARE_TABLE(trade_request_t,trade_request_man_impl,trade_request);

    // Scaling tables
    DECLARE_TABLE(account_permission_t,account_permission_man_impl,account_permission);
    DECLARE_TABLE(broker_t,broker_man_impl,broker);
    DECLARE_TABLE(company_t,company_man_impl,company);
    DECLARE_TABLE(customer_t,customer_man_impl,customer);
    DECLARE_TABLE(company_competitor_t,company_competitor_man_impl,company_competitor);
    DECLARE_TABLE(security_t,security_man_impl,security);
    DECLARE_TABLE(customer_account_t,customer_account_man_impl,customer_account);
    DECLARE_TABLE(daily_market_t,daily_market_man_impl,daily_market);
    DECLARE_TABLE(customer_taxrate_t,customer_taxrate_man_impl,customer_taxrate);
    DECLARE_TABLE(holding_t,holding_man_impl,holding);
    DECLARE_TABLE(financial_t,financial_man_impl,financial);
    DECLARE_TABLE(holding_history_t,holding_history_man_impl,holding_history);
    DECLARE_TABLE(address_t,address_man_impl,address);
    DECLARE_TABLE(holding_summary_t,holding_summary_man_impl,holding_summary);
    DECLARE_TABLE(last_trade_t,last_trade_man_impl,last_trade);
    DECLARE_TABLE(watch_list_t,watch_list_man_impl,watch_list);
    DECLARE_TABLE(watch_item_t,watch_item_man_impl,watch_item);
    DECLARE_TABLE(news_item_t,news_item_man_impl,news_item);
    DECLARE_TABLE(news_xref_t,news_xref_man_impl,news_xref);


    // --- kit baseline trxs --- //

    w_rc_t run_one_xct(Request* prequest);

    // TPCE Transactions
    DECLARE_TRX(broker_volume);
    DECLARE_TRX(customer_position);
    DECLARE_TRX(market_feed);
    DECLARE_TRX(market_watch);
    DECLARE_TRX(security_detail);
    DECLARE_TRX(trade_lookup);
    DECLARE_TRX(trade_order);
    DECLARE_TRX(trade_result);
    DECLARE_TRX(trade_status);
    DECLARE_TRX(trade_update);
    DECLARE_TRX(data_maintenance);
    DECLARE_TRX(trade_cleanup);


    //LOADING TRANSACTIONS
    DECLARE_TRX(populate_small);
    DECLARE_TRX(populate_customer);
    DECLARE_TRX(populate_address);
    DECLARE_TRX(populate_ca_and_ap);
    DECLARE_TRX(populate_wl_and_wi);
    DECLARE_TRX(populate_company);
    DECLARE_TRX(populate_company_competitor);
    DECLARE_TRX(populate_daily_market);
    DECLARE_TRX(populate_financial);
    DECLARE_TRX(populate_last_trade);
    DECLARE_TRX(populate_ni_and_nx);
    DECLARE_TRX(populate_security);
    DECLARE_TRX(populate_customer_taxrate);
    DECLARE_TRX(populate_broker);   
    DECLARE_TRX(populate_holding);   
    DECLARE_TRX(populate_holding_summary);   
    DECLARE_TRX(populate_unit_trade);   

    //helper transaction
    DECLARE_TRX(find_maxtrade_id);
    
    // for thread-local stats
    virtual void env_thread_init();
    virtual void env_thread_fini();   

    // stat map
    statmap_t _statmap;

    // snapshot taken at the beginning of each experiment    
    ShoreTPCETrxStats _last_stats;
    virtual void reset_stats();
    ShoreTPCETrxStats _get_stats();

    //print the current tables into files
    w_rc_t db_print(int lines);

    //fetch the pages of the current tables and their indexes into the buffer pool
    w_rc_t db_fetch();
    
}; // EOF ShoreTPCEEnv
   


EXIT_NAMESPACE(tpce);



#endif /* __SHORE_TPCE_ENV_H */

