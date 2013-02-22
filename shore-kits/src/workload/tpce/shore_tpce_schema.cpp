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

/** @file:   shore_tpce_schema.cpp
 *
 *  @brief:  Declaration of the TPC-E tables
 *
 *  @author: Djordje Jevdjic
 *  @author: Cansu Kaynak
 */

#include "workload/tpce/shore_tpce_schema.h"

using namespace shore;

ENTER_NAMESPACE(tpce);


/*********************************************************************
 *
 * TPC-E SCHEMA
 * 
 * This file contains the classes for tables in the TPC-E benchmark.
 *
 *********************************************************************/

/* @note: PIN: Here we try to adapt the indexes Microsoft SQL Server uses for TPC-E
 *             as much as possible. The indexes that SQL Server uses and we do not use
 *              are marked with "// MSSQL - not used here" comments.
 */


/* ------------------------------------------------------------- */
/* --- CUSTOMER tables used in the TPC-E benchmark --- */
/* ------------------------------------------------------------- */


account_permission_t::account_permission_t(const uint4_t& pd) 
    : table_desc_t("ACCOUNT_PERMISSION", TPCE_ACCOUNT_PERMISSION_FCOUNT, pd) //81B
{
    _desc[0].setup(SQL_LONG,    "AP_CA_ID"); //FK (CA_)		//8
    _desc[1].setup(SQL_FIXCHAR, "AP_ACL",4);			//5
    _desc[2].setup(SQL_FIXCHAR, "AP_TAX_ID",20);		//21	
    _desc[3].setup(SQL_FIXCHAR, "AP_L_NAME",25);		//26
    _desc[4].setup(SQL_FIXCHAR, "AP_F_NAME",20);		//21
		
    uint  keys[2] = { 0, 2 };

    create_primary_idx_desc("AP_INDEX", 0, keys, 2, pd);
}


customer_t::customer_t(const uint4_t& pd)
    : table_desc_t("CUSTOMER", TPCE_CUSTOMER_FCOUNT, pd) //280B
{
    _desc[0].setup(SQL_LONG,     "C_ID");			//8	
    _desc[1].setup(SQL_FIXCHAR,  "C_TAX_ID",20);		//21
    _desc[2].setup(SQL_FIXCHAR,  "C_ST_ID",4); //FK (ST_)	//5
    _desc[3].setup(SQL_FIXCHAR,  "C_L_NAME",25);		//26
    _desc[4].setup(SQL_FIXCHAR,  "C_F_NAME",20);		//21
    _desc[5].setup(SQL_FIXCHAR,  "C_M_NAME",1);			//2
    _desc[6].setup(SQL_FIXCHAR,  "C_GNDR", 1); 			//2
    _desc[7].setup(SQL_SMALLINT, "C_TIER");//char in EGEN	//2
    _desc[8].setup(SQL_LONG,     "C_DOB"); //DATE		//8
    _desc[9].setup(SQL_LONG,     "C_AD_ID");//FK (AD_)		//8
    _desc[10].setup(SQL_FIXCHAR, "C_CTRY_1", 3);		//4
    _desc[11].setup(SQL_FIXCHAR, "C_AREA_1", 3);		//4
    _desc[12].setup(SQL_FIXCHAR, "C_LOCAL_1", 10);		//11
    _desc[13].setup(SQL_FIXCHAR, "C_EXT_1", 5);			//6
    _desc[14].setup(SQL_FIXCHAR, "C_CTRY_2", 3);		//4
    _desc[15].setup(SQL_FIXCHAR, "C_AREA_2", 3);		//4	
    _desc[16].setup(SQL_FIXCHAR, "C_LOCAL_2", 10);		//11
    _desc[17].setup(SQL_FIXCHAR, "C_EXT_2", 5);			//6
    _desc[18].setup(SQL_FIXCHAR, "C_CTRY_3", 3);		//4
    _desc[19].setup(SQL_FIXCHAR, "C_AREA_3", 3);		//4
    _desc[20].setup(SQL_FIXCHAR, "C_LOCAL_3", 10);		//11
    _desc[21].setup(SQL_FIXCHAR, "C_EXT_3", 5);			//6
    _desc[22].setup(SQL_FIXCHAR, "C_EMAIL_1", 50);		//51
    _desc[23].setup(SQL_FIXCHAR, "C_EMAIL_2", 50);		//51	
			
    uint  keys[1] = { 0 };
    uint  keys2[2] = { 1, 0 };
    uint  keys3[2] = { 0, 7 };

    create_primary_idx_desc("C_INDEX", 0, keys, 1, pd); //unique
    create_index_desc("C_INDEX_2", 0, keys2, 2); //unique
    create_index_desc("C_INDEX_3", 0, keys3, 2); //unique
}


customer_account_t::customer_account_t(const uint4_t& pd) 
    : table_desc_t("CUSTOMER_ACCOUNT", TPCE_CUSTOMER_ACCOUNT_FCOUNT, pd) //85B
{
    _desc[0].setup(SQL_LONG,   	 "CA_ID");				//8
    _desc[1].setup(SQL_LONG,   	 "CA_B_ID"); //FK (B_)			//8
    _desc[2].setup(SQL_LONG,   	 "CA_C_ID"); //FK (C_)			//8
    _desc[3].setup(SQL_FIXCHAR,  "CA_NAME", 50);			//51
    _desc[4].setup(SQL_SMALLINT, "CA_TAX_ST");  /////CHAR IN EGEN	//2
    _desc[5].setup(SQL_FLOAT,  	 "CA_BAL");				//8
		
    uint  keys[1] = { 0 };

    /*
     * @note: PIN: the place where this index is used requires reaching the
     *             heap page anyway to get CA_BAL so i am not going to put
     *             CA_ID here to the index and waste space.
     */
    //uint  keys2[2] = { 2, 0 }; // MSSQL - not used here
    uint  keys2[2] = { 2 };

    // @note: PIN: i cannot see any access that this index can be useful for
    //uint  keys3[3] = { 1, 0, 2 };  // MSSQL - not used here

    /*
     * @note: PIN: the place where this index is used uses CA_ID to reach
     *             the others. however, it almost reads all the other columns.
     *             putting all of them to the index seems like an ugly trick to me.
     */
    //uint  keys4[5] = { 0, 1, 2, 4, 3 }; // MSSQL - not used here

    create_primary_idx_desc("CA_INDEX", 0, keys, 1, pd); //unique
    create_index_desc("CA_INDEX_2", 0, keys2, 1, false, false, pd); //non-unique
    //create_index_desc("CA_INDEX_3", 0, keys3, 3, true, false, pd);//unique
    //create_index_desc("CA_INDEX_4", 0, keys4, 5, true, false, pd);//unique
}


customer_taxrate_t::customer_taxrate_t(const uint4_t& pd)
    : table_desc_t("CUSTOMER_TAXRATE", TPCE_CUSTOMER_TAXRATE_FCOUNT, pd) //13B
{
    _desc[0].setup(SQL_FIXCHAR, "CX_TX_ID", 4);//FK (TX_)		//5
    _desc[1].setup(SQL_LONG,    "CX_C_ID"); //FK (C_)			//8
		
    uint  keys[2] = { 1 , 0 };
	
    create_primary_idx_desc("CX_INDEX", 0, keys, 2, pd); //unique
}


holding_t::holding_t(const uint4_t& pd) 
    : table_desc_t("HOLDING", TPCE_HOLDING_FCOUNT, pd) //52B
{
    _desc[0].setup(SQL_LONG,    "H_T_ID");//FK (T_)		//8
    _desc[1].setup(SQL_LONG,    "H_CA_ID"); //FK (HS_)		//8
    _desc[2].setup(SQL_FIXCHAR, "H_S_SYMB", 16);//FK (HS_)	//16, padded
    _desc[3].setup(SQL_LONG,    "H_DTS"); //DATETIME		//8
    _desc[4].setup(SQL_FLOAT,   "H_PRICE");			//8
    _desc[5].setup(SQL_INT,     "H_QTY");			//4

    // @note: PIN: not needed
    //uint  keys[1] = { 0 }; // MSSQL - not used here
    /*
     * @note: PIN: the place where this index is used uses H_CA_ID and H_S_SYMB
     *             to reach H_PRICE, H_QTY, and H_T_ID. since, it needs the actual
     *             tuple anyway for H_PRICE and H_QTY, i removed H_T_ID from the index
     *             to not to waste space. i kept H_DTS since it's used for retrieving
     *             ordered tuples which is required by the transactions
     */
    //uint  keys1[4] = { 1, 2, 3, 0 };  // MSSQL - not used here
    uint  keys2[3] = { 1, 2, 3 };
    
    //create_primary_idx_desc("H_INDEX", 0, keys, 1, pd); //unique
    //create_index_desc("H_INDEX_1", 0, keys2, 4, true, false, pd); //unique
    create_index_desc("H_INDEX_2", 0, keys2, 3, false, false, pd); //non-unique
 }


holding_history_t::holding_history_t(const uint4_t& pd)
    : table_desc_t("HOLDING_HISTORY", TPCE_HOLDING_HISTORY_FCOUNT, pd) //24B
{
    _desc[0].setup(SQL_LONG, "HH_H_T_ID");//FK (T_)		//8
    _desc[1].setup(SQL_LONG, "HH_T_ID"); //FK (T_)		//8
    _desc[2].setup(SQL_INT,  "HH_BEFORE_QTY");		//4
    _desc[3].setup(SQL_INT,  "HH_AFTER_QTY");		//4

    /*
     * @note: PIN: MSSQL picks this one as primary key index and the
     *             other one as the secondary index but here we are
     *             going to use a secondary index built on HH_T_ID
     *             because it's enough
     */
    //uint  keys[2] = { 0, 1 }; // MSSQL - not used here
    //uint  keys1[2] = { 1, 0 };  // MSSQL - not used here
    uint  keys2[1] = { 1 };
    
    //create_primary_idx_desc("HH_INDEX", 0, keys, 2); //unique
    //create_index_desc("HH_INDEX_1", 0, keys1, 2, true, false, pd); //unique
    create_index_desc("HH_INDEX_2", 0, keys2, 1, false, false, pd); //non-unique
}


holding_summary_t::holding_summary_t(const uint4_t& pd)
    : table_desc_t("HOLDING_SUMMARY", TPCE_HOLDING_SUMMARY_FCOUNT, pd) //28B
{
    _desc[0].setup(SQL_LONG,    "HS_CA_ID");//FK (CA_)	
    _desc[1].setup(SQL_FIXCHAR, "HS_S_SYMB", 16); //FK (S_), //was 15, padded
    _desc[2].setup(SQL_INT,     "HS_QTY");

    uint  keys[2] = { 0, 1 };

    create_primary_idx_desc("HS_INDEX", 0, keys, 2, pd); //unique
}


watch_item_t::watch_item_t(const uint4_t& pd)
    : table_desc_t("WATCH_ITEM", TPCE_WATCH_ITEM_FCOUNT, pd) //24B
{
    _desc[0].setup(SQL_LONG,    "WI_WL_ID");//FK (WL_)
    _desc[1].setup(SQL_FIXCHAR, "WI_S_SYMB", 15); //FK (S_)

    uint  keys[2] = { 0, 1 };
	
    create_primary_idx_desc("WI_INDEX", 0, keys, 2, pd); //unique
}


watch_list_t::watch_list_t(const uint4_t& pd)
    : table_desc_t("WATCH_LIST", TPCE_WATCH_LIST_FCOUNT, pd) //16B
{
    _desc[0].setup(SQL_LONG, "WL_ID");
    _desc[1].setup(SQL_LONG, "WL_C_ID"); //FK (C_)

    // @note: PIN: no need this, search is always done with c_id
    //uint  keys[1] = { 0 }; // MSSQL - not used
    uint  keys2[2] = { 1, 0 };

    //create_primary_idx_desc("WL_INDEX", 0, keys, 1); //unique
    create_primary_idx_desc("WL_INDEX_2", 0, keys2, 2, pd); //unique

}



/* ------------------------------------------------------------- */
/* --- BROKER tables used in the TPC-E benchmark --- */
/* ------------------------------------------------------------- */


broker_t::broker_t(const uint4_t& pd)
    : table_desc_t("BROKER", TPCE_BROKER_FCOUNT, pd) //79B
{
    _desc[0].setup(SQL_LONG,    "B_ID");			//8
    _desc[1].setup(SQL_FIXCHAR, "B_ST_ID", 4); //FK (ST_)	//5
    _desc[2].setup(SQL_FIXCHAR, "B_NAME", 52);			//50; padded
    _desc[3].setup(SQL_INT,     "B_NUM_TRADES");	    	//8
    _desc[4].setup(SQL_FLOAT,   "B_COMM_TOTAL");   		//8

    uint  keys[1] = { 0 };
    uint  keys2[2] = { 0, 2 };
    uint  keys3[2] = { 2, 0 };
    
    create_primary_idx_desc("B_INDEX", 0, keys, 1, pd); //unique
    create_index_desc("B_INDEX_2", 0, keys2, 2, true, false, pd); //unique
    create_index_desc("B_INDEX_3", 0, keys3, 2, true, false, pd); //unique	
}


cash_transaction_t::cash_transaction_t(const uint4_t& pd)
    : table_desc_t("CASH_TRANSACTION", TPCE_CASH_TRANSACTION_FCOUNT, pd) //125B
{
    _desc[0].setup(SQL_LONG,    "CT_T_ID");//FK (T_)
    _desc[1].setup(SQL_LONG,    "CT_DTS"); //DATETIME
    _desc[2].setup(SQL_FLOAT,   "CT_AMT");
    _desc[3].setup(SQL_FIXCHAR, "CT_NAME", 100);	    
	 	
    uint  keys[1] = { 0 };

    create_primary_idx_desc("CT_INDEX", 0, keys, 1, pd); //unique
}


charge_t::charge_t(const uint4_t& pd)
    : table_desc_t("CHARGE", TPCE_CHARGE_FCOUNT, pd) //15B
{
    _desc[0].setup(SQL_FIXCHAR,  "CH_TT_ID", 4);//FK (TT_)//padded
    _desc[1].setup(SQL_SMALLINT, "CH_C_TIER"); 
    _desc[2].setup(SQL_FLOAT,    "CH_CHRG");
	 	
    uint  keys[2] = { 0 , 1 }; 

    create_primary_idx_desc("CH_INDEX", 0, keys, 2, pd); //unique
}


commission_rate_t::commission_rate_t(const uint4_t& pd)
    : table_desc_t("COMMISSION_RATE", TPCE_COMMISSION_RATE_FCOUNT, pd) //34B
{
    _desc[0].setup(SQL_SMALLINT, "CR_C_TIER");			
    _desc[1].setup(SQL_FIXCHAR,  "CR_TT_ID", 6); //FK (TT_), padded
    _desc[2].setup(SQL_FIXCHAR,  "CR_EX_ID", 8); //FK (EX_), padded
    _desc[3].setup(SQL_INT,      "CR_FROM_QTY");
    _desc[4].setup(SQL_INT,      "CR_TO_QTY");
    _desc[5].setup(SQL_FLOAT,    "CR_RATE");
		
    uint  keys[4] = { 0, 1, 2, 3 };

    create_primary_idx_desc("CR_INDEX", 0, keys, 4, pd); //unique
}


settlement_t::settlement_t(const uint4_t& pd)
    : table_desc_t("SETTLEMENT", TPCE_SETTLEMENT_FCOUNT, pd) //65B
{
    _desc[0].setup(SQL_LONG,     "SE_T_ID"); //FK(T_)
    _desc[1].setup(SQL_FIXCHAR,  "SE_CASH_TYPE", 40); 
    _desc[2].setup(SQL_LONG,     "SE_CASH_DUE_DATE"); //DATE
    _desc[3].setup(SQL_FLOAT,    "SE_AMT");

    uint  keys[1] = { 0 };

    create_primary_idx_desc("SE_INDEX", 0, keys, 1, pd); //unique
}


trade_t::trade_t(const uint4_t& pd)
    : table_desc_t("TRADE", TPCE_TRADE_FCOUNT, pd) //145B
{
    _desc[0].setup(SQL_LONG,    "T_ID"); //8
    _desc[1].setup(SQL_LONG,    "T_DTS"); //DATETIME		//8
    _desc[2].setup(SQL_FIXCHAR, "T_ST_ID",4); //FK (ST_)	//5
    _desc[3].setup(SQL_FIXCHAR, "T_TT_ID",3);//FK (TT_)	//4
    _desc[4].setup(SQL_BIT,     "T_IS_CASH");  //INT IN EGEN	//1
    _desc[5].setup(SQL_FIXCHAR, "T_S_SYMB",16);//FK (S_)	//16 //padded
    _desc[6].setup(SQL_INT,     "T_QTY"); //4
    _desc[7].setup(SQL_FLOAT,   "T_BID_PRICE"); //8
    _desc[8].setup(SQL_LONG,    "T_CA_ID");//FK (CA_) //8
    _desc[9].setup(SQL_FIXCHAR, "T_EXEC_NAME",49); //50
    _desc[10].setup(SQL_FLOAT,  "T_TRADE_PRICE");  //8
    _desc[11].setup(SQL_FLOAT,  "T_CHRG"); //8
    _desc[12].setup(SQL_FLOAT,  "T_COMM"); //8
    _desc[13].setup(SQL_FLOAT,  "T_TAX");  //8
    _desc[14].setup(SQL_BIT,    "T_LIFO"); //INT IN EGEN

    uint  keys[1] = { 0 };
    uint  keys2[3] = { 8, 1, 0 };
    // @note: PIN: for this secondary indexes, last column MSSQL chooses seems unnecassary
    //uint  keys3[2] = { 5, 1, 0 }; // MSSQL - not used
    uint  keys3[2] = { 5, 1 };	

    create_primary_idx_desc("T_INDEX", 0, keys, 1, pd); //unique
    create_index_desc("T_INDEX_2", 0, keys2, 3, true, false, pd); //non-unique
    create_index_desc("T_INDEX_3", 0, keys3, 2, false, false, pd); //non-unique
}


trade_history_t::trade_history_t(const uint4_t& pd)
    : table_desc_t("TRADE_HISTORY", TPCE_TRADE_HISTORY_FCOUNT, pd) //21B
{
    _desc[0].setup(SQL_LONG,    "TH_T_ID");//FK (T_)
    _desc[1].setup(SQL_LONG,    "TH_DTS"); //DATETIME
    _desc[2].setup(SQL_FIXCHAR, "TH_ST_ID", 4); //FK (ST_)

    /*
     * @note: PIN: how this is used is TH_T_ID is given and the other two
     *             columns are retrieved so having TH_T_ID in the index is
     *             enough for now, and since you have to go to the actual
     *             record anyway there is no use to keep TH_ST_ID on the index
     */
    //uint  keys[2] = { 0, 2 };	// MSSQL - not used
    //uint keys2[1] = { 0 };
    uint keys3[2] = { 0, 1 };

    //create_primary_idx_desc("TH_INDEX", 0, keys, 2, pd); //unique
    //create_index_desc("TH_INDEX_2", 0, keys2, 1, false, false, pd); //non-unique
    create_index_desc("TH_INDEX_3", 0, keys3, 2, false, false, pd); //non-unique
}


trade_request_t::trade_request_t(const uint4_t& pd)
    : table_desc_t("TRADE_REQUEST", TPCE_TRADE_REQUEST_FCOUNT, pd) //49B
{
    _desc[0].setup(SQL_LONG,	"TR_T_ID");
    _desc[1].setup(SQL_FIXCHAR,	"TR_TT_ID", 3); //FK (TT_)
    _desc[2].setup(SQL_FIXCHAR, "TR_S_SYMB", 16);//FK (EX_), was 15, fixed for padding
    _desc[3].setup(SQL_INT,     "TR_QTY");
    _desc[4].setup(SQL_FLOAT,   "TR_BID_PRICE");
    _desc[5].setup(SQL_LONG,    "TR_B_ID"); //called TR_CA_ID in EGEN?, (FK B_)

    //@note: PIN: no one is using it
    //uint  keys[1] = { 0 };  // MSSQL - not used
    //@note: PIN: have to go to the tuple regardless of using key2 and key3 so key4 makes more sense
    //uint  keys2[3] = { 5, 2, 0 }; // MSSQL - not used
    //uint  keys3[5] = { 2, 0, 1, 4, 3 };  // MSSQL - not used
    uint  keys4[2] = { 2, 5 };
    
    //create_primary_idx_desc("TR_INDEX", 0, keys, 1, pd); //unique
    //create_index_desc("TR_INDEX_2", 0, keys2, 3); //unique
    //create_index_desc("TR_INDEX_3", 0, keys3, 5); //unique   
    create_index_desc("TR_INDEX_4", 0, keys4, 2, false, false, pd); //non-unique
}


trade_type_t::trade_type_t(const uint4_t& pd) 
    : table_desc_t("TRADE_TYPE", TPCE_TRADE_TYPE_FCOUNT, pd) //20B
{
    _desc[0].setup(SQL_FIXCHAR,	"TT_ID", 4);//was 3, padded
    _desc[1].setup(SQL_FIXCHAR,	"TT_NAME", 12); 
    _desc[2].setup(SQL_BIT,  	"TT_IS_SELL"); //INT IN EGEN
    _desc[3].setup(SQL_BIT,   	"TT_IS_MRKT"); //INT IN EGEN

    //@note: PIN: MSSQL is smoking something here, it has a ridiculous number of secondary indexes
    
    uint  keys[1] = { 0 };

    create_primary_idx_desc("TT_INDEX", 0, keys, 1, pd); //unique

}


/* ------------------------------------------------- */
/* --- MARKET tables used in the TPC-E benchmark --- */
/* ------------------------------------------------- */

company_t::company_t(const uint4_t& pd)
    : table_desc_t("COMPANY", TPCE_COMPANY_FCOUNT, pd) //298B
{
    _desc[0].setup(SQL_LONG,    "CO_ID");	 //8
    _desc[1].setup(SQL_FIXCHAR, "CO_ST_ID", 4);	 //5
    _desc[2].setup(SQL_FIXCHAR, "CO_NAME", 60);	 //61
    _desc[3].setup(SQL_FIXCHAR,	"CO_IN_ID", 4);	 //5 //should be 2, padding for index 2
    _desc[4].setup(SQL_FIXCHAR,	"CO_SP_RATE", 4); //5
    _desc[5].setup(SQL_FIXCHAR,	"CO_CEO", 46);    //47
    _desc[6].setup(SQL_LONG,    "CO_AD_ID");     //8
    _desc[7].setup(SQL_FIXCHAR, "CO_DESC", 150); //151
    _desc[8].setup(SQL_LONG,    "CO_OPEN_DATE"); //8
		
    uint  keys1[1] = { 0 };
    uint  keys2[2] = { 2, 0 };
    uint  keys3[2] = { 3, 0 };
    
    create_primary_idx_desc("CO_INDEX", 0, keys1, 1, pd); //unique
    create_index_desc("CO_INDEX_2", 0, keys2, 2, true, false, pd); //unique
    create_index_desc("CO_INDEX_3", 0, keys3, 2, true, false, pd); //unique
}

company_competitor_t::company_competitor_t(const uint4_t& pd)
    : table_desc_t("COMPANY_COMPETITOR", TPCE_COMPANY_COMPETITOR_FCOUNT, pd) //21B
{
    _desc[0].setup(SQL_LONG,    "CP_CO_ID");
    _desc[1].setup(SQL_LONG,    "CP_COMP_CO_ID");
    _desc[2].setup(SQL_FIXCHAR, "CP_IN_ID", 4); //was 2, padded
		
    uint  keys1[3] = { 0, 1, 2 };
    // @note: PIN: since keys1 is enough, i decided to not to use this one
    //uint  keys2[0] = { 0 }; // MSSQL - not used here
	
    create_primary_idx_desc("CP_INDEX", 0, keys1, 3, pd); //unique
    //create_index_desc("CP_INDEX_2", 0, keys2, 1, false, false, pd); //non-unique
}


daily_market_t::daily_market_t(const uint4_t& pd) 
    : table_desc_t("DAILY_MARKET", TPCE_DAILY_MARKET_FCOUNT, pd) //52B
{
    _desc[0].setup(SQL_LONG,    "DM_DATE");
    _desc[1].setup(SQL_FIXCHAR, "DM_S_SYMB", 16); //was 15, padded
    _desc[2].setup(SQL_FLOAT,   "DM_CLOSE"); 	
    _desc[3].setup(SQL_FLOAT,   "DM_HIGH");
    _desc[4].setup(SQL_FLOAT,   "DM_LOW");
    _desc[5].setup(SQL_INT,     "DM_VOL");
		
    uint  keys1[2] = { 1, 0 };
    uint  keys2[2] = { 0, 1 };
    
    create_primary_idx_desc("DM_INDEX", 0, keys1, 2, pd); //unique
    create_index_desc("DM_INDEX_2", 0, keys2, 2, true, false, pd); //unique
}


exchange_t::exchange_t(const uint4_t& pd) 
    : table_desc_t("EXCHANGE", TPCE_EXCHANGE_FCOUNT, pd) //279B
{
    _desc[0].setup(SQL_FIXCHAR, "EX_ID", 6); 		//7
    _desc[1].setup(SQL_FIXCHAR, "EX_NAME", 100);	//101
    _desc[2].setup(SQL_INT,     "EX_NUM_SYMB");		//8
    _desc[3].setup(SQL_INT,     "EX_OPEN");		//8
    _desc[4].setup(SQL_INT,     "EX_CLOSE");		//8
    _desc[5].setup(SQL_FIXCHAR, "EX_DESC", 150);	//151
    _desc[6].setup(SQL_LONG,    "EX_AD_ID");		//8
		
    uint  keys[1] = { 0 };
	
    create_primary_idx_desc("EX_INDEX", 0, keys, 1, pd); //unique
}


financial_t::financial_t(const uint4_t& pd)
    : table_desc_t("FINANCIAL", TPCE_FINANCIAL_FCOUNT, pd) //102B
{
    _desc[0].setup(SQL_LONG,     "FI_CO_ID");
    _desc[1].setup(SQL_INT,      "FI_YEAR");
    _desc[2].setup(SQL_SMALLINT, "FI_QTR"); //int in EGEN
    _desc[3].setup(SQL_LONG,     "FI_QTR_START_DATE");
    _desc[4].setup(SQL_FLOAT,    "FI_REVENUE");
    _desc[5].setup(SQL_FLOAT,    "FI_NET_EARN");
    _desc[6].setup(SQL_FLOAT,    "FI_BASIC_EPS");
    _desc[7].setup(SQL_FLOAT,    "FI_DILUT_EPS");
    _desc[8].setup(SQL_FLOAT,    "FI_MARGIN");
    _desc[9].setup(SQL_FLOAT,    "FI_INVENTORY");
    _desc[10].setup(SQL_FLOAT,   "FI_ASSETS");
    _desc[11].setup(SQL_FLOAT,   "FI_LIABILITY");
    _desc[12].setup(SQL_FLOAT,   "FI_OUT_BASIC");
    _desc[13].setup(SQL_FLOAT,   "FI_OUT_DILUT");
		
    uint  keys[3] = { 0, 1, 2 };
	
    create_primary_idx_desc("FI_INDEX", 0, keys, 3, pd); 
}


industry_t::industry_t(const uint4_t& pd) 
    : table_desc_t("INDUSTRY", TPCE_INDUSTRY_FCOUNT, pd) //57B
{
    _desc[0].setup(SQL_FIXCHAR, "IN_ID", 2);    //3
    _desc[1].setup(SQL_FIXCHAR, "IN_NAME", 50); //51
    _desc[2].setup(SQL_FIXCHAR, "IN_SC_ID", 2); //3
		
    uint  keys1[1] = { 0 };
    uint  keys2[2] = { 1, 0 };
    uint  keys3[2] = { 2, 0 };
	
    create_primary_idx_desc("IN_INDEX", 0, keys1, 1, pd); //unique
    create_index_desc("IN_INDEX_2", 0, keys2, 2, true, false, pd); //unique
    create_index_desc("IN_INDEX_3", 0, keys3, 2, true, false, pd); //unique
}


last_trade_t::last_trade_t(const uint4_t& pd)
    : table_desc_t("LAST_TRADE", TPCE_LAST_TRADE_FCOUNT, pd) //44B
{
    _desc[0].setup(SQL_FIXCHAR, "LT_S_SYMB", 16);//padded
    _desc[1].setup(SQL_LONG,  	"LT_DTS");
    _desc[2].setup(SQL_FLOAT,  	"LT_PRICE");
    _desc[3].setup(SQL_FLOAT,  	"LT_OPEN_PRICE");
    _desc[4].setup(SQL_FLOAT,  	"LT_VOL"); //int in Egen, INT64 in new Egen
		
    uint  keys[1] = { 0 };
	
    create_primary_idx_desc("LT_INDEX", 0, keys, 1, pd); //unique
}

        
news_item_t::news_item_t(const uint4_t& pd)
    : table_desc_t("NEWS_ITEM", TPCE_NEWS_ITEM_FCOUNT, pd) //biggest Byte
{
    _desc[0].setup(SQL_LONG,    "NI_ID");		//8
    _desc[1].setup(SQL_FIXCHAR, "NI_HEADLINE", 80);	//81
    _desc[2].setup(SQL_FIXCHAR, "NI_SUMMARY", 255);	//256

#warning should be 100000, but it fails in     _pnews_item_man   = new news_item_man_impl(_pnews_item_desc.get());

    _desc[3].setup(SQL_FIXCHAR, "NI_ITEM", max_news_item_size);	//BLOB, Ask Ippo
    _desc[4].setup(SQL_LONG,  	"NI_DTS");		//8
    _desc[5].setup(SQL_FIXCHAR, "NI_SOURCE", 30);	//31
    _desc[6].setup(SQL_FIXCHAR, "NI_AUTHOR", 30);	//31
		
    uint  keys[1] = { 0 };
	
    create_primary_idx_desc("NI_INDEX", 0, keys, 1, pd);
}


news_xref_t::news_xref_t(const uint4_t& pd)
    : table_desc_t("NEWS_XREF", TPCE_NEWS_XREF_FCOUNT, pd) //16B
{
    _desc[0].setup(SQL_LONG,   	"NX_NI_ID");
    _desc[1].setup(SQL_LONG,   	"NX_CO_ID");
		
    uint  keys[2] = { 1, 0 };
	
    create_primary_idx_desc("NX_INDEX", 0, keys, 2, pd); //unique
}


sector_t::sector_t(const uint4_t& pd)
    : table_desc_t("SECTOR", TPCE_SECTOR_FCOUNT, pd) //38B
{
    _desc[0].setup(SQL_FIXCHAR, "SC_ID", 4); //padded
    _desc[1].setup(SQL_FIXCHAR, "SC_NAME", 32); //padded

    // @note: PIN: primary index is not needed
    //uint  keys[1] = { 0 }; // MSSQL - not used
    uint  keys2[2] = { 1, 0 };
	
    // create_primary_idx_desc("SC_INDEX", 0, keys, 1, pd); //unique
    create_index_desc("SC_INDEX_2", 0, keys2, 2, true, false, pd); //unique
}


security_t::security_t(const uint4_t& pd)
    : table_desc_t("SECURITY", TPCE_SECURITY_FCOUNT, pd) //197B
{
    _desc[0].setup(SQL_FIXCHAR, "S_SYMB", 16);	//17, was 15, changed for padding
    _desc[1].setup(SQL_FIXCHAR, "S_ISSUE", 8);	//9, was 6, changed for padding 
    _desc[2].setup(SQL_FIXCHAR, "S_ST_ID", 4);	//5
    _desc[3].setup(SQL_FIXCHAR, "S_NAME", 70);	//71
    _desc[4].setup(SQL_FIXCHAR, "S_EX_ID", 6);	//7
    _desc[5].setup(SQL_LONG,   	"S_CO_ID");
    _desc[6].setup(SQL_FLOAT,   "S_NUM_OUT");
    _desc[7].setup(SQL_LONG,  	"S_START_DATE");
    _desc[8].setup(SQL_LONG,   	"S_EXCH_DATE");
    _desc[9].setup(SQL_FLOAT,	"S_PE");
    _desc[10].setup(SQL_FLOAT, 	"S_52WK_HIGH");
    _desc[11].setup(SQL_LONG,	"S_52WK_HIGH_DATE");
    _desc[12].setup(SQL_FLOAT,	"S_52WK_LOW");
    _desc[13].setup(SQL_LONG,	"S_52WK_LOW_DATE");
    _desc[14].setup(SQL_FLOAT,  "S_DIVIDEND");
    _desc[15].setup(SQL_FLOAT,  "S_YIELD");
		
    uint  keys1[1] = { 0 };
    /*
     * @note: PIN: Even if this index is used to retrieve some of the fields,
     *             you still need the actual tuple so I am omiting this index.
     *             Instead creating an index with key4 is better.
     */
    //uint  keys2[4] = { 5, 1, 4, 0 }; // MSSQL - not used
    // @note: PIN: I see no use for this index
    //uint  keys3[3] = { 5, 6, 0 }; // MSSQL - not used
    uint  keys4[3] = { 5, 1, 0 };
		
    create_primary_idx_desc("S_INDEX", 0, keys1, 1, pd); //unique
    //create_index_desc("S_INDEX_2", 0, keys2, 4); //unique
    //create_index_desc("S_INDEX_3", 0, keys3, 3); //unique
    create_index_desc("S_INDEX_4", 0, keys4, 3, true, false, pd); //unique
}



/* ------------------------------------------------------------- */
/* --- DIMENSION tables used in the TPC-E benchmark --- */
/* ------------------------------------------------------------- */


address_t::address_t(const uint4_t& pd)
    : table_desc_t("ADDRESS", TPCE_ADDRESS_FCOUNT, pd) //264B
{
    _desc[0].setup(SQL_LONG,    "AD_ID");
    _desc[1].setup(SQL_FIXCHAR, "AD_LINE1", 80);
    _desc[2].setup(SQL_FIXCHAR, "AD_LINE2", 80);
    _desc[3].setup(SQL_FIXCHAR, "AD_ZC_CODE", 12);
    _desc[4].setup(SQL_FIXCHAR, "AD_CTRY", 80);
		
    uint  keys[1] = { 0 };
	
    create_primary_idx_desc("AD_INDEX", 0, keys, 1, pd);
}


status_type_t::status_type_t(const uint4_t& pd)
    : table_desc_t("STATUS_TYPE", TPCE_STATUS_TYPE_FCOUNT, pd) //16B
{
    _desc[0].setup(SQL_FIXCHAR, "ST_ID", 4);
    _desc[1].setup(SQL_FIXCHAR, "ST_NAME", 10);
		
    uint  keys[1] = { 0 };
	
    create_primary_idx_desc("ST_INDEX", 0, keys, 1, pd);
}


taxrate_t::taxrate_t(const uint4_t& pd)
    : table_desc_t("TAXRATE", TPCE_TAXRATE_FCOUNT, pd) //64B
{
    _desc[0].setup(SQL_FIXCHAR, "TX_ID", 4);
    _desc[1].setup(SQL_FIXCHAR, "TX_NAME", 50);
    _desc[2].setup(SQL_FLOAT,  	"TX_RATE");
		
    uint  keys[1] = { 0 };
	
    create_primary_idx_desc("TX_INDEX", 0, keys, 1, pd);
}


zip_code_t::zip_code_t(const uint4_t& pd)
    : table_desc_t("ZIP_CODE", TPCE_ZIP_CODE_FCOUNT, pd) //179B
{
    _desc[0].setup(SQL_FIXCHAR, "ZC_CODE", 12);
    _desc[1].setup(SQL_FIXCHAR, "ZC_TOWN", 80);
    _desc[2].setup(SQL_FIXCHAR, "ZC_DIV", 80);
		
    uint  keys[1] = { 0 };
	
    create_primary_idx_desc("ZC_INDEX", 0, keys, 1, pd);
}



/* ------------------------------------------------------------- */
/* --- END of TPC-E tables ------------------------------------- */
/* ------------------------------------------------------------- */


EXIT_NAMESPACE(tpce);

