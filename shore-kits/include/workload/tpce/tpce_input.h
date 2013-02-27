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

/** @file tpce_input.h
 *
 *  @brief Declaration of the (common) inputs for the TPC-E trxs
 *  @brief Declaration of functions that generate the inputs for the TPCE TRXs
 *
 *  @author Cansu Kaynak
 *  @author Djordje Jevdjic
 */

#ifndef __TPCE_INPUT_H
#define __TPCE_INPUT_H

#include "workload/tpce/tpce_const.h"
#include "workload/tpce/tpce_struct.h"
#include "util.h"
#include "workload/tpce/egen/CE.h"
#include "workload/tpce/egen/TxnHarnessStructs.h"
#include "workload/tpce/egen/DM.h"
#include "workload/tpce/egen/MEE.h"
#include "workload/tpce/MEESUT.h"

using namespace TPCE;

CCETxnInputGenerator*  transactions_input_init(int customers, int sf, int wdays);
CDM*  data_maintenance_init(int customers, int sf, int wdays);
CMEE* market_init( INT32 TradingTimeSoFar, CMEESUTInterface *pSUT, UINT32 UniqueId);

ENTER_NAMESPACE(tpce);

extern CCETxnInputGenerator*	m_TxnInputGenerator;
extern CDM*	   		m_CDM;
extern CMEESUT*			meesut;
extern CMEE* 	   		mee; 

//Converts EGEN TIME representation to time_t structure
myTime EgenTimeToTimeT(CDateTime &cdt);


//Converts EGEN TIMESTAMP representation to time_t structure
myTime EgenTimeStampToTimeT(TIMESTAMP_STRUCT &tss);

//gets day of the month
int dayOfMonth(myTime& t);

/*********************************************************************
 * 
 * broker_volume_input_t
 *
 * Input for any BROKER_VOLUME transaction
 *
 *********************************************************************/
 
struct broker_volume_input_t
{
    char _broker_list[40][STRSIZE(49)]; 
    char _sector_name[STRSIZE(30)]; 
	
    // Construction/Destructions
    broker_volume_input_t()
    {
	memset(_sector_name, '\0', STRSIZE(30));
    }; 
	
    ~broker_volume_input_t() {  };
    void print ();
    // Assignment operator
    broker_volume_input_t& operator= (const broker_volume_input_t& rhs);
};



/*********************************************************************
 * 
 * customer_position_input_t
 *
 * Input for any CUSTOMER_POSITION transaction
 *
 *********************************************************************/
 
struct customer_position_input_t
{
    TIdent	_acct_id_idx;
    TIdent 	_cust_id;
    bool	_get_history;
    char 	_tax_id[STRSIZE(20)];

    // Construction/Destructions
    customer_position_input_t()
	:_acct_id_idx(0), _cust_id(0),
	 _get_history(false)		
    {
	memset(_tax_id, '\0', 21);
    }; 

    void print();

    ~customer_position_input_t() {  }; 
	
    // Assignment operator
    customer_position_input_t& operator= (const customer_position_input_t& rhs);
};




/*********************************************************************
 * 
 * trade_order_input_t
 *
 * Input for any TRADE_ORDER transaction
 *
 *********************************************************************/
 
struct trade_order_input_t
{
    TIdent 	_acct_id;
    char	_co_name[61];
    char 	_exec_f_name[21];
    char 	_exec_l_name[26];
    char 	_exec_tax_id[21];
    bool	_is_lifo;   ////they use INT
    char 	_issue[7];
    double	_requested_price;
    bool	_roll_it_back; ////they use INT
    char	_st_pending_id[5];
    char	_st_submitted_id[5];
    char 	_symbol[16];
    int 	_trade_qty;   //INT, not double
    char	_trade_type_id[4];
    bool	_type_is_margin; ////they use INT
	
    void print();
		
    // Construction/Destructions
    trade_order_input_t()
	:_acct_id(0), _is_lifo(false), _requested_price(0), 
	 _roll_it_back(false), _trade_qty(0), _type_is_margin(false)
    {
	memset(_co_name, '\0', 61);
	memset(_exec_f_name, '\0', 21);
	memset(_exec_l_name, '\0', 26);
	memset(_exec_tax_id, '\0', 21);
	memset(_issue, '\0', 7);
	memset(_st_pending_id, '\0', 5);
	memset(_st_submitted_id, '\0', 5);
	memset(_symbol, '\0', 16);
	memset(_trade_type_id, '\0', 4);
    }; 
	
    ~trade_order_input_t() {  };
	
    // Assignment operator
    trade_order_input_t& operator= (const trade_order_input_t& rhs);
};



/*********************************************************************
 * 
 * trade_lookup_input_t
 *
 * Input for any TRADE_LOOKUP transaction
 *
 *********************************************************************/
 
struct trade_lookup_input_t
{
    TIdent 	_acct_id;
    int	        _frame_to_execute;
    TIdent 	_max_acct_id; 
    int 	_max_trades; 	
    myTime	_start_trade_dts; 
    myTime	_end_trade_dts; 	
    char	_symbol[16];		
    TIdent	_trade_id[20];		
	
    void print();
    // Construction/Destructions
    trade_lookup_input_t()
	:_acct_id(0), _end_trade_dts(0), _frame_to_execute(0), 
	 _max_acct_id(0), _max_trades(0), _start_trade_dts(0)
    {	
	memset(_symbol, '\0', 16);
    }; 
	
    ~trade_lookup_input_t() {  };
	
    // Assignment operator
    trade_lookup_input_t& operator= (const trade_lookup_input_t& rhs);
};


/*********************************************************************
 * 
 * trade_result_input_t
 *
 * Input for any TRADE_RESULT transaction
 *
 *********************************************************************/
 
struct trade_result_input_t
{
    // temp array to keep the holding tuples
    // to be deleted during the index scan
    rid_t    _holding_rid[10];
    
    TIdent 	_trade_id;
    double	_trade_price;	
	
    // Construction/Destructions
    trade_result_input_t()
	:_trade_id(0), _trade_price(0)
    {}; 
    void print();	
    ~trade_result_input_t() {  };
	
    // Assignment operator
    trade_result_input_t& operator= (const trade_result_input_t& rhs);
};

/*********************************************************************
 * 
 * market_watch_input_t
 *
 * Input for any MARKET_WATCH transaction
 *
 *********************************************************************/
struct market_watch_input_t  
{
    TIdent	_acct_id;
    TIdent 	_cust_id;
    TIdent	_starting_co_id;
    TIdent 	_ending_co_id;
    char	_industry_name[51];
    myTime	_start_date;

    void print();

    // Construction/Destructions
    market_watch_input_t(): 
	_acct_id(0), _cust_id(0), _ending_co_id(0),
	_start_date(0), _starting_co_id(0)
    {
	memset(_industry_name, '\0', 51);
    }; 
	
    ~market_watch_input_t() {  };
	
    // Assignment operator
    market_watch_input_t& operator= (const market_watch_input_t& rhs);
  
};

/*********************************************************************
 * 
 * security_detail_input_t
 *
 * Input for any SECURITY_DETAIL transaction
 *
 *********************************************************************/
struct security_detail_input_t
{
    bool	_access_lob_flag;
    char 	_symbol[16];
    myTime      _start_day;
    int 	_max_rows_to_return;


    // Construction/Destructions
    security_detail_input_t(): 
	_access_lob_flag(false), _max_rows_to_return(0),
	_start_day(0)
    {
	memset(_symbol, '\0', 16);
    }; 
    void print();	
    ~security_detail_input_t() {  };
	
    // Assignment operator
    security_detail_input_t& operator= (const security_detail_input_t& rhs);
};

/*********************************************************************
 * 
 * trade_status_input_t  
 *
 * Input for any TRADE_STATUS transaction
 *
 *********************************************************************/
struct trade_status_input_t 
{
    TIdent 	_acct_id;
    // Construction/Destructions
    trade_status_input_t()
	:_acct_id(0)
    {}; 
    void print();	
    ~trade_status_input_t() {  };
	
    // Assignment operator
    trade_status_input_t& operator= (const trade_status_input_t& rhs);
};

/*********************************************************************
 * 
 * trade_update_input_t 
 *
 * Input for any TRADE_UPDATE transaction
 *
 *********************************************************************/
struct trade_update_input_t 
{
    TIdent	_acct_id;
    int		_frame_to_execute;
    int		_max_trades;
    int		_max_updates;
    char	_symbol[cSYMBOL_len+1];
    myTime	_start_trade_dts;
    myTime	_end_trade_dts;
    TIdent	_trade_id[TradeUpdateFrame1MaxRows];
    TIdent	_max_acct_id;


    void print();
    trade_update_input_t():
	_acct_id(0), _frame_to_execute(0), _max_trades(0), _max_updates(0),   _max_acct_id(0)
    {
	memset(_symbol, '\0', 16);
    }
    ~trade_update_input_t(){}
};

/*********************************************************************
 * 
 * data_maintenance_input_t
 *
 * Input for any DATA_MAINTENANCE transaction
 *
 *********************************************************************/
struct data_maintenance_input_t 
{
    TIdent 	_acct_id;
    TIdent 	_c_id;
    TIdent 	_co_id;
    int 	_day_of_month;
    char 	_symbol[16];
    char  	_table_name[31];
    char	_tx_id[21];
    int		_vol_incr;
  
    void print();
    // Construction/Destructions
    data_maintenance_input_t ():
	_acct_id(0), _c_id(0), _co_id(0),
	_vol_incr(0), _day_of_month(0)
  
    {
	memset(_symbol, '\0', 16);
	memset(_table_name, '\0', 31);
	memset(_tx_id, '\0', 21);
    }; 
	
    ~data_maintenance_input_t() {  };
	
    // Assignment operator
    data_maintenance_input_t& operator= (const data_maintenance_input_t& rhs);  
};


/*********************************************************************
 * 
 * market_feed_input_t
 *
 * Input for any MARKET_FEED transaction
 *
 *********************************************************************/
struct market_feed_input_t 
{
    // temp array to keep the holding tuples
    // to be deleted during the index scan
    rid_t       _trade_rid[10];
    
    double 	_price_quote[max_feed_len];
    char 	_status_submitted[5];
    char	_symbol[max_feed_len][16];
    int 	_trade_qty[max_feed_len];
    char	_type_limit_buy[4];
    char	_type_limit_sell[4];
    char 	_type_stop_loss[4];
  
    void print();  
    // Construction/Destructions
    market_feed_input_t()
    {
	memset(_status_submitted, '\0', 5);
	memset(_type_limit_buy, '\0', 4); 
	memset(_type_limit_sell, '\0', 4); 
	memset(_type_stop_loss, '\0', 4); 
    }; 
	
    ~market_feed_input_t() {  };
	
    // Assignment operator
    market_feed_input_t& operator= (const market_feed_input_t& rhs);
};

/*********************************************************************
 * 
 * trade_cleanup_input_t 
 *
 * Input for any TRADE_CLEANUP transaction
 *
 *************************fixed********************************************/
struct trade_cleanup_input_t 
{
    char 	_st_canceled_id[5];
    char 	_st_pending_id[5];
    char 	_st_submitted_id[5];
    TIdent	_trade_id;
  
    // Construction/Destructions
    trade_cleanup_input_t():_trade_id(0)
    {
	memset(_st_canceled_id, '\0', 5);
	memset(_st_pending_id, '\0', 5);
	memset(_st_submitted_id, '\0', 5);
    
    }; 
    void print();	
    ~trade_cleanup_input_t() {  };
	
    // Assignment operator
    trade_cleanup_input_t& operator= (const trade_cleanup_input_t& rhs);
};


broker_volume_input_t     create_broker_volume_input(int sf, int specificIdx);
customer_position_input_t create_customer_position_input(int sf, int specificIdx);
market_feed_input_t       create_market_feed_input(int sf, int specificIdx);
market_watch_input_t      create_market_watch_input(int sf, int specificIdx);
security_detail_input_t   create_security_detail_input(int sf, int specificIdx);
trade_lookup_input_t      create_trade_lookup_input(int sf, int specificIdx);
trade_order_input_t       create_trade_order_input(int sf, int specificIdx);
trade_result_input_t      create_trade_result_input(int sf, int specificIdx);
trade_status_input_t      create_trade_status_input(int sf, int specificIdx);
trade_update_input_t      create_trade_update_input(int sf, int specificIdx);
data_maintenance_input_t  create_data_maintenance_input(int sf, int specificIdx);
trade_cleanup_input_t     create_trade_cleanup_input(int sf, int specificIdx);


struct populate_small_input_t{};
struct populate_customer_input_t{};
struct populate_address_input_t{};
struct populate_ca_and_ap_input_t{};
struct populate_wl_and_wi_input_t{};
struct populate_company_input_t{};
struct populate_company_competitor_input_t{};
struct populate_daily_market_input_t{};
struct populate_financial_input_t{};
struct populate_last_trade_input_t{};
struct populate_ni_and_nx_input_t{};
struct populate_security_input_t{};
struct populate_customer_taxrate_input_t{};
struct populate_broker_input_t{};
struct populate_holding_input_t{};
struct populate_holding_summary_input_t{};
struct populate_unit_trade_input_t{};

struct find_maxtrade_id_input_t{};



EXIT_NAMESPACE(tpce);
#endif
