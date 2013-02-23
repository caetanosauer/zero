/** @file tpce_struct.h
 *
 *  @brief Data structures for the TPC-E database
 *
 *  @author Djordje Jevdjic
 *  @author Cansu Kaynak
 */

#ifndef __TPCE_STRUCT_H
#define __TPCE_STRUCT_H

#include <cstdlib>
#include <unistd.h>
#include <sys/time.h>
#include "util.h"
#include "workload/tpce/egen/TxnHarnessStructs.h"

using namespace TPCE;

ENTER_NAMESPACE(tpce);


/* use this for allocation of NULL-terminated strings */
#define STRSIZE(x)(x+1)



/* exported structures */
/* ------------------------------------------------------------- */
/* --- CUSTOMER tables used in the TPC-E benchmark --- */
/* ------------------------------------------------------------- */

//ACCOUNT_PERMISSION
struct tpce_account_permission_tuple{
	TIdent 	AP_CA_ID;
	char    AP_ACL			[STRSIZE(4)];
	char    AP_TAX_ID    		[STRSIZE(20)];
	char    AP_L_NAME   		[STRSIZE(25)];
	char    AP_F_NAME 		[STRSIZE(20)];
};


struct tpce_account_permission_key{
	TIdent 	AP_CA_ID;
	char    AP_ACL			[STRSIZE(4)];
};

//CUSTOMER
struct tpce_customer_tuple{
	TIdent 	C_ID;
	char    C_TAX_ID		[STRSIZE(20)];
	char    C_ST_ID			[STRSIZE(4)];
	char    C_L_NAME   		[STRSIZE(25)];
	char    C_F_NAME 		[STRSIZE(20)];
	char    C_M_NAME;
	char    C_GNDR;
	short   C_TIER;
	time_t 	C_DOB;  //DATE
	TIdent	C_AD_ID;
	char    C_CTRY_1		[STRSIZE(3)];
	char    C_AREA_1    		[STRSIZE(3)];
	char    C_LOCAL_1   		[STRSIZE(10)];
	char    C_EXT_1 		[STRSIZE(5)];
	char    C_CTRY_2		[STRSIZE(3)];
	char    C_AREA_2    		[STRSIZE(3)];
	char    C_LOCAL_2   		[STRSIZE(10)];
	char    C_EXT_2 		[STRSIZE(5)];
	char    C_CTRY_3		[STRSIZE(3)];
	char    C_AREA_3	   	[STRSIZE(3)];
	char    C_LOCAL_3   		[STRSIZE(10)];
	char    C_EXT_3 		[STRSIZE(5)];
	char    C_EMAIL_1   		[STRSIZE(50)];
	char    C_EMAIL_2 		[STRSIZE(50)];
};

struct tpce_customer_key{
  //int 	TIdent;
  long long 	TIdent;
};


//CUSTOMER_ACCOUNT
struct tpce_customer_account_tuple{
	TIdent		CA_ID;
	TIdent		CA_B_ID;
	TIdent		CA_C_ID;
	char		CA_NAME		[STRSIZE(50)];
	short		CA_TAX_ST;
	double 	CA_BAL;
};
		

struct tpce_customer_account_key{
	TIdent 	CA_ID;
};


//CUSTOMER_TAXRATE
struct tpce_customer_taxrate_tuple
{
	char	CX_TX_ID	[STRSIZE(4)];
	TIdent	CX_C_ID;
};	

struct tpce_customer_taxrate_key{
	TIdent 	CA_ID;
	char	CX_TX_ID	[STRSIZE(4)];
};


//HOLDING
struct tpce_holding_tuple{
	TIdent		H_T_ID;
	TIdent		H_CA_ID;
	char		H_S_SYMB	[STRSIZE(4)];
	time_t		H_DTS; //DATETIME
	double 	H_PRICE;
	int		H_QTY;
};

struct tpce_holding_key{
	TIdent		H_T_ID;
};

//HODLING_HISTORY
struct tpce_holding_history_tuple{
	TIdent		HH_H_T_ID;
	TIdent		HH_T_ID;
	int		HH_BEFORE_QTY;
	int		HH_AFTER_QTY;
};

struct tpce_holding_history_key{
	TIdent		HH_H_T_ID;
	TIdent		HH_T_ID;
};


//HOLDING_SUMMARY
struct tpce_holding_summary_tuple{
	TIdent		HS_CA_ID;
	char		HS_S_SYMB  [STRSIZE(15)];
	int		HS_QTY;
};

struct tpce_holding_summary_key{
	TIdent		HS_CA_ID;
	char		HS_S_SYMB  [STRSIZE(15)];
};

//WATCH_ITEM
struct tpce_watch_item_tuple{
	TIdent	WI_WL_ID;
	char	WI_S_SYMB 	   [STRSIZE(15)];
};

struct tpce_watch_item_key{
	TIdent	WI_WL_ID;
	char	WI_S_SYMB 	[STRSIZE(15)];
};



//WATCH_LIST
struct tpce_watch_list_tuple{
	TIdent		WL_ID;
	TIdent		WL_C_ID;
};

struct tpce_watch_list_key{
	TIdent		WL_ID;
};


/* ------------------------------------------------------------- */
/* --- BROKER tables used in the TPC-E benchmark --- */
/* ------------------------------------------------------------- */

//BROKER
struct tpce_broker_tuple{
	TIdent  B_ID;
	char    B_ST_ID		[STRSIZE(4)];
	char    B_NAME		[STRSIZE(49)];
	char    B_NUM_TRADES 	[STRSIZE(9)];
	double  B_COMM_TOTAL;
};

struct tpce_broker_key{
	TIdent B_ID;
};


//CASH_TRANSACTION
struct tpce_cash_transaction_tuple{
    	TIdent	CT_T_ID;
	time_t  CT_DTS;   //DATETIME
	double  CT_AMT;
	char	CT_NAME		[STRSIZE(100)];
};

struct tpce_cash_transaction_key{
	TIdent			CT_T_ID;
};


//CHARGE
struct tpce_charge_tuple{	
	char	CH_TT_ID	[STRSIZE(3)] ;
	short	CH_C_TIER;
	double	CH_CHRG;
};

struct tpce_charge_key{
	char	CH_TT_ID	[STRSIZE(3)] ;
	short	CH_C_TIER;

};


//COMMISSION_RATE
struct tpce_commission_rate_tuple{
	short   CR_C_TIER;
	char    CR_TT_ID	[STRSIZE(3)];
	char    CR_EX_ID	[STRSIZE(6)];
	int	CR_FROM_QTY;
	int	CR_TO_QTY;
	double  CR_RATE;

};

struct tpce_commission_rate_key{
	short   CR_C_TIER;
	char    CR_TT_ID	[STRSIZE(3)];
	char    CR_EX_ID	[STRSIZE(6)];
	int     CR_FROM_QTY;
};


//SETTLEMENT 
struct tpce_settlement_tuple{
	TIdent		SE_T_ID;
	char		SE_CASH_TYPE	[STRSIZE(40)];
	time_t		SE_CASH_DUE_DATE; //DATE
	double		SE_AMT; 
};

struct tpce_settlement_key{
	int		SE_T_ID;

};


//TRADE
struct tpce_trade_tuple{
	TIdent	T_ID;
	time_t	T_DTS;  //DATETIME
	char	T_ST_ID		[STRSIZE(4)];
	char	T_TT_ID		[STRSIZE(3)];
	bool	T_IS_CASH;
	char	T_S_SYMB	[STRSIZE(15)];
	int	T_QTY;
	double	T_BID_PRICE;
	TIdent	T_CA_ID;
	char	T_EXEC_NAME	[STRSIZE(49)];
	double	T_TRADE_PRICE;
	double	T_CHRG;
	double	T_COMM;
	double	T_TAX;
	bool	T_LIFO;
};

struct tpce_trade_key{
	TIdent		T_ID;
};

//TRADE_HISTORY
struct tpce_trade_history_tuple{
	TIdent	TH_T_ID;
	time_t	TH_DTS; //DATETIME
	char	TH_ST_ID	[STRSIZE(4)];
};

struct tpce_trade_history_key{
	TIdent	TH_T_ID;
	char	TH_ST_ID	[STRSIZE(4)];
};


//TRADE_REQUEST
struct tpce_trade_request_tuple{
	TIdent	TR_T_ID;
	char	TR_TT_ID	[STRSIZE(3)];
	char	TR_S_SYMB	[STRSIZE(15)];
	int	TR_QTY;
	double	TR_BID_PRICE;
	TIdent	TR_B_ID;

};

struct tpce_trade_request_key{
	TIdent		TR_T_ID;	
};


//TRADE_TYPE
struct tpce_trade_type_tuple{
	char	TT_ID	[STRSIZE(3)];
	char	TT_NAME	[STRSIZE(12)];
	bool	TT_IS_SELL;
	bool	TT_IS_MRKT;
};

struct tpce_trade_type_key{
	char	TT_ID	[STRSIZE(3)];
};


/* ------------------------------------------------------------- */
/* --- MARKET tables used in the TPC-E benchmark --- */
/* ------------------------------------------------------------- */

//COMPANY
struct tpce_company_tuple{
	TIdent 	CO_ID;
	char    CO_ST_ID	[STRSIZE(4)];
	char    CO_NAME    	[STRSIZE(60)];
	char    CO_IN_ID   	[STRSIZE(2)];
	char    CO_SP_RATE 	[STRSIZE(4)];
	char    CO_CEO	 	[STRSIZE(46)];
	TIdent  CO_AD_ID;
	char    CO_DESC	 	[STRSIZE(150)];
	time_t  CO_OPENDATE;
	
};

struct tpce_company_tuple_key{
	TIdent  CO_ID;
};

//COMPANY_COMPETITOR
struct tpce_company_competitor_tuple{
	TIdent	CP_CO_ID;
	TIdent 	CP_COMP_CO_ID;
	char    CP_IN_ID	[STRSIZE(2)];
	
};

struct tpce_company_competitor_tuple_key{
	TIdent	CP_CO_ID;
	TIdent 	CP_COMP_CO_ID;
	char    CP_IN_ID	[STRSIZE(2)];
};


//DAILY_MARKET
struct tpce_daily_market_tuple{
	time_t 		DM_DATE;
	char    	DM_S_SYMB	[STRSIZE(15)];
	double    	DM_CLOSE;
	double 		DM_HIGH;
	double 		DM_LOW;
	TIdent 		DM_VOL;	
};


struct tpce_daily_market_tuple_key{
	time_t 		DM_DATE;
	char    	DM_S_SYMB	[STRSIZE(15)];
};

//EXCHANGE
struct tpce_exchange_tuple{
	char    	EX_ID		[STRSIZE(6)];
	char    	EX_NAME		[STRSIZE(100)];
	int 		EX_NUM_SYMB;
	int 		EX_OPEN;
	int 		EX_CLOSE;
	char    	EX_DESC		[STRSIZE(150)];
	TIdent		EX_AD_ID;

};


struct tpce_exchange_tuple_key{
	char    	EX_ID		[STRSIZE(6)];
};

//FINANCIAL
struct tpce_financial_tuple{
	TIdent 		FI_CO_ID;
	int		FI_YEAR;
	short		FI_QTR;
	time_t		FI_QTR_START_DATE;
	double		FI_QTR_REVENUE;
	double		FI_NET_EARN;
	double		FI_BASIC_EPS;
	double    	FI_DILUT_EPS	[STRSIZE(150)];
	double    	FI_MARGIN;
	double    	FI_INVENTORY;
	double    	FI_ASSETS;
	double    	FI_LIABILITY;
	TIdent    	FI_OUT_BASIC;
	TIdent    	FI_OUT_DILUT;
};


struct tpce_financial_tuple_key{
	TIdent 		FI_CO_ID;
	int		FI_YEAR;
	short		FI_QTR;
};	
	

//INDUSTRY
struct tpce_industry_tuple{
	char    	IN_ID		[STRSIZE(2)];
	char    	IN_NAME		[STRSIZE(50)];
	char    	IN_SC_ID	[STRSIZE(2)];
};

struct tpce_industry_tuple_key{
	char    	IN_ID		[STRSIZE(2)];
};

//LAST_TRADE
struct tpce_last_trade_tuple{
	char    	LT_S_SYMB	[STRSIZE(15)];
	time_t		LT_DTS;
	double		LT_PRICE;
	double		LT_OPEN_PRICE;
	TIdent		LT_VOL;
};

struct tpce_last_trade_tuple_key{
	char    	LT_S_SYMB	[STRSIZE(15)];
};
	
//NEWS_ITEM
struct tpce_news_item_tuple{	
	TIdent 	NI_ID;
	char    NI_HEADLINE		[STRSIZE(80)];
	char    NI_SUMMARY		[STRSIZE(255)];
	char    NI_ITEM			[STRSIZE(100000)];
	time_t	NI_DTS;
	char    NI_SOURCE		[STRSIZE(30)];
	char    NI_AUTHOR		[STRSIZE(30)];
};

struct tpce_news_item_tuple_key{	
	TIdent 	NI_ID;
};

//NEWS_XREF
struct tpce_news_xref_tuple{
	TIdent NX_NI_ID;
	TIdent NX_CO_ID;
};

struct tpce_news_xref_tuple_key{
	TIdent NX_NI_ID;
	TIdent NX_CO_ID;
};

//SECTOR
struct tpce_sector_tuple{
	char    	SC_ID	[STRSIZE(2)];
	char    	SC_NAME	[STRSIZE(30)];
};

struct tpce_sector_tuple_key{
	char    	SC_ID	[STRSIZE(2)];
};

//SECURITY
struct tpce_security_tuple{
	char    S_SYMB	[STRSIZE(15)];
	char    S_ISSUE	[STRSIZE(6)];
	char    S_ST_ID	[STRSIZE(4)];
	char    S_NAME	[STRSIZE(70)];
	char    S_EX_ID	[STRSIZE(6)];
	TIdent	S_CO_ID;
	TIdent S_NUM_OUT;
	time_t 	S_START_DATE;
	time_t 	S_EXCH_DATE;
	double S_PE;
	double S_52WK_HIGH;
	time_t	S_52WK_HIGH_DATE;
	double S_52WK_LOW;
	time_t  S_52WK_LOW_DATE;
	double S_DIVIDEND;
	double S_YIELD;
};

struct tpce_security_tuple_key{
	char    S_SYMB	[STRSIZE(15)];
};


/* ------------------------------------------------------------- */
/* --- DIMENSION tables used in the TPC-E benchmark --- */
/* ------------------------------------------------------------- */

//ADDRESS
struct tpce_address_tuple{
	TIdent	AD_ID;
	char    AD_LINE1	[STRSIZE(80)];
	char    AD_LINE2	[STRSIZE(80)];
	char    AD_ZC_CODE	[STRSIZE(12)];
	char    AD_CTRY		[STRSIZE(80)];
};

struct tpce_address_tuple_key{
	TIdent		AD_ID;
};

//STATUS_TYPE
struct tpce_status_type_tuple{
	char    ST_ID	[STRSIZE(4)];
	char    ST_NAME	[STRSIZE(10)];
};

struct tpce_status_type_tuple_key{
	char    ST_ID	[STRSIZE(4)];
};

//TAX_RATE
struct tpce_tax_rate_tuple{
	char    TX_ID	[STRSIZE(4)];
	char    TX_NAME	[STRSIZE(50)];
	double TX_RATE;
};

struct tpce_tax_rate_tuple_key{
	char    TX_ID	[STRSIZE(4)];
};

//ZIP_CODE
struct tpce_zip_code_tuple{
	char    ZC_CODE	[STRSIZE(12)];
	char    ZC_TOWN	[STRSIZE(80)];
	char    ZC_DIV	[STRSIZE(80)];
};

struct tpce_zip_code_tuple_key{
	char    ZC_CODE	[STRSIZE(12)];
};

EXIT_NAMESPACE(tpce);

#endif /* __TPCE_STRUCT_H */
