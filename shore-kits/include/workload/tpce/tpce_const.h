/** @file:   tpce_const.h
 *
 *  @brief:  Constants needed by the TPC-E kit
 *
 *  @author: Djordje Jevdjic
 *  @author: Cansu Kaynak
 */

#ifndef __TPCE_CONST_H
#define __TPCE_CONST_H


#include "util/namespace.h"
#include "workload/tpce/egen/CE.h"
#include "workload/tpce/egen/TxnHarnessStructs.h"

typedef long long myTime;

ENTER_NAMESPACE(tpce);

// the load unit
// in kits when SF = 1 --> 1000 customers (1 load unit for tpce spec)
const int TPCE_CUSTS_PER_LU = 1000;

// --- useful constants --- //

const TIdent MAX_ID    = UINT64(-1)/2;
const myTime MAX_DTS   = UINT64(-1)/2;
const int    MAX_VAL   = UINT32(-1)/2;
const short  MAX_SHORT = UINT16(-1)/2;

const int min_broker_list_len	= 20;
const int max_broker_list_len	= 40;

const int max_acct_len		= 10;
const int max_hist_len		= 30;

const int max_feed_len		= 20;

const int min_day_len		= 5;
const int max_day_len		= 20;
const int max_fin_len		= 20;
const int max_news_len		= 2;
const int max_comp_len		= 3;


const int max_news_item_size		= 10000;

const int TradeLookupMaxRows	= 20;
const int TradeLookupFrame1MaxRows	= 20;
const int TradeLookupFrame2MaxRows	= 20;
const int TradeLookupFrame3MaxRows	= 20;
const int TradeLookupMaxTradeHistoryRowsReturned = 20;

const int max_trade_status_len	= 50;

const int TradeUpdateMaxRows	= 20;
const int TradeUpdateFrame1MaxRows	= 20;
const int TradeUpdateFrame2MaxRows	= 20;
const int TradeUpdateFrame3MaxRows	= 20;
const int TradeUpdateMaxTradeHistoryRowsReturned = 3;

// --- number of fields per table --- //

const int TPCE_ACCOUNT_PERMISSION_FCOUNT   = 5;
const int TPCE_CUSTOMER_FCOUNT             = 24;
const int TPCE_CUSTOMER_ACCOUNT_FCOUNT     = 6;
const int TPCE_CUSTOMER_TAXRATE_FCOUNT     = 2;
const int TPCE_HOLDING_FCOUNT              = 6;
const int TPCE_HOLDING_HISTORY_FCOUNT      = 4;
const int TPCE_HOLDING_SUMMARY_FCOUNT      = 3;
const int TPCE_WATCH_ITEM_FCOUNT           = 2;
const int TPCE_WATCH_LIST_FCOUNT           = 2;

const int TPCE_BROKER_FCOUNT			= 5; 
const int TPCE_CASH_TRANSACTION_FCOUNT	   	= 4;
const int TPCE_CHARGE_FCOUNT			= 3;
const int TPCE_COMMISSION_RATE_FCOUNT	   	= 6;
const int TPCE_SETTLEMENT_FCOUNT	       	= 4;
const int TPCE_TRADE_FCOUNT			= 15;
const int TPCE_TRADE_HISTORY_FCOUNT		   = 3;
const int TPCE_TRADE_REQUEST_FCOUNT		   = 6;
const int TPCE_TRADE_TYPE_FCOUNT		   = 4;

const int TPCE_COMPANY_FCOUNT			   = 9;
const int TPCE_COMPANY_COMPETITOR_FCOUNT	   = 3;
const int TPCE_DAILY_MARKET_FCOUNT		   = 6;
const int TPCE_EXCHANGE_FCOUNT			   = 7;
const int TPCE_FINANCIAL_FCOUNT			   = 14;
const int TPCE_INDUSTRY_FCOUNT			   = 3;
const int TPCE_LAST_TRADE_FCOUNT		   = 5;
const int TPCE_NEWS_ITEM_FCOUNT			   = 7;
const int TPCE_NEWS_XREF_FCOUNT			   = 2;
const int TPCE_SECTOR_FCOUNT			   = 2;
const int TPCE_SECURITY_FCOUNT			   = 16;

const int TPCE_ADDRESS_FCOUNT			   = 5;
const int TPCE_STATUS_TYPE_FCOUNT		   = 2;
const int TPCE_TAXRATE_FCOUNT			   = 3;
const int TPCE_ZIP_CODE_FCOUNT			   = 3;


// -- number of tables -- //

const int SHORE_TPCE_TABLES = 33;




// TPC-E Transaction IDs

const int XCT_TPCE_MIX               = 70;
const int XCT_TPCE_BROKER_VOLUME     = 71;
const int XCT_TPCE_CUSTOMER_POSITION = 72;
const int XCT_TPCE_MARKET_FEED       = 73;
const int XCT_TPCE_MARKET_WATCH      = 74;
const int XCT_TPCE_SECURITY_DETAIL   = 75;
const int XCT_TPCE_TRADE_LOOKUP      = 76;
const int XCT_TPCE_TRADE_ORDER       = 77;
const int XCT_TPCE_TRADE_RESULT      = 78;
const int XCT_TPCE_TRADE_STATUS      = 79;
const int XCT_TPCE_TRADE_UPDATE      = 80;
const int XCT_TPCE_DATA_MAINTENANCE  = 81;
const int XCT_TPCE_TRADE_CLEANUP     = 82;


// TPC-E Transaction Probabilities

const double PROB_TPCE_DATA_MAINTENANCE  = 0;  
const double PROB_TPCE_TRADE_CLEANUP     = 0;
const double PROB_TPCE_MARKET_FEED       = 1.0;
const double PROB_TPCE_TRADE_UPDATE      = 2.0;
const double PROB_TPCE_BROKER_VOLUME     = 4.9;
const double PROB_TPCE_TRADE_LOOKUP      = 8.0;
const double PROB_TPCE_TRADE_RESULT      = 10.0;
const double PROB_TPCE_TRADE_ORDER       = 10.1;
const double PROB_TPCE_CUSTOMER_POSITION = 13.0;
const double PROB_TPCE_SECURITY_DETAIL   = 14.0;
const double PROB_TPCE_MARKET_WATCH      = 18.0;
const double PROB_TPCE_TRADE_STATUS      = 19.0;


// --- Helper functions --- //


// Translates or picks a random xct type given the benchmark specification
int random_xct_type(const double idx);

EXIT_NAMESPACE(tpce);

#endif /* __TPCE_CONST_H */
