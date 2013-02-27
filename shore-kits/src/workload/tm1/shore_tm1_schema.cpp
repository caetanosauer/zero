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

/** @file:   shore_tm1_schema.h
 * 
 *  @brief:  Declaration of the Telecom One (TM1) benchmark tables
 *
 *  @author: Ippokratis Pandis, Feb 2009
 */

#include "workload/tm1/shore_tm1_schema.h"

using namespace shore;

ENTER_NAMESPACE(tm1);



/*********************************************************************
 *
 * TM1 SCHEMA
 * 
 * This file contains the classes for tables in the TM1 benchmark.
 *
 *********************************************************************/


/*
 * Indices created on the tables are:
 *
 * 1. SUBSCRIBER
 * a. primary (unique) index on subscriber(s_id)
 * b. secondary (unique) index on subscriber(s_sub_nbr)
 *
 * 2. ACCESS_INFO
 * a. primary (unique) index on access_info(s_id,ai_type)
 *
 * 3. SPECIAL_FACILITY
 * a. primary (unique) index on special_facility(s_id,sf_type)
 *
 * 4. CALL_FORWARDING
 * a. primary (unique) index on call_forwarding(s_id,sf_type,start_time)
 *
 */


subscriber_t::subscriber_t(const uint4_t& pd)
#ifdef CFG_HACK
    : table_desc_t("SUBSCRIBER", TM1_SUB_FCOUNT+1, pd) 
#else
    : table_desc_t("SUBSCRIBER", TM1_SUB_FCOUNT, pd) 
#endif
{
    // Schema
    _desc[0].setup(SQL_INT,         "S_ID");         // UNIQUE [1..SF]

    _desc[1].setup(SQL_FIXCHAR,     "SUB_NBR", TM1_SUB_NBR_SZ);  

    _desc[2].setup(SQL_BIT,         "BIT_1");        // BIT (0,1)
    _desc[3].setup(SQL_BIT,         "BIT_2");
    _desc[4].setup(SQL_BIT,         "BIT_3");
    _desc[5].setup(SQL_BIT,         "BIT_4");
    _desc[6].setup(SQL_BIT,         "BIT_5");
    _desc[7].setup(SQL_BIT,         "BIT_6");
    _desc[8].setup(SQL_BIT,         "BIT_7");
    _desc[9].setup(SQL_BIT,         "BIT_8");
    _desc[10].setup(SQL_BIT,        "BIT_9");
    _desc[11].setup(SQL_BIT,        "BIT_10");

    _desc[12].setup(SQL_SMALLINT,   "HEX_1");        // SMALLINT (0,15)
    _desc[13].setup(SQL_SMALLINT,   "HEX_2");
    _desc[14].setup(SQL_SMALLINT,   "HEX_3");
    _desc[15].setup(SQL_SMALLINT,   "HEX_4");
    _desc[16].setup(SQL_SMALLINT,   "HEX_5");
    _desc[17].setup(SQL_SMALLINT,   "HEX_6");
    _desc[18].setup(SQL_SMALLINT,   "HEX_7");
    _desc[19].setup(SQL_SMALLINT,   "HEX_8");
    _desc[20].setup(SQL_SMALLINT,   "HEX_9");
    _desc[21].setup(SQL_SMALLINT,   "HEX_10");

    _desc[22].setup(SQL_SMALLINT,   "BYTE2_1");      // SMALLINT (0,255)
    _desc[23].setup(SQL_SMALLINT,   "BYTE2_2");
    _desc[24].setup(SQL_SMALLINT,   "BYTE2_3");
    _desc[25].setup(SQL_SMALLINT,   "BYTE2_4");
    _desc[26].setup(SQL_SMALLINT,   "BYTE2_5");
    _desc[27].setup(SQL_SMALLINT,   "BYTE2_6");
    _desc[28].setup(SQL_SMALLINT,   "BYTE2_7");
    _desc[29].setup(SQL_SMALLINT,   "BYTE2_8");
    _desc[30].setup(SQL_SMALLINT,   "BYTE2_9");
    _desc[31].setup(SQL_SMALLINT,   "BYTE2_10");

    _desc[32].setup(SQL_INT,        "MSC_LOCATION"); // INT (0,2^32-1)
    _desc[33].setup(SQL_INT,        "VLR_LOCATION");

#ifdef CFG_HACK
    int padding_sz = 100-10*sizeof(bool)-20*sizeof(short)-3*sizeof(int)
        -TM1_SUB_NBR_SZ*sizeof(char);
    _desc[34].setup(SQL_FIXCHAR,       "S_PADDING", padding_sz);
#endif        

    // create unique index s_index on (s_id)
    uint keys1[1] = { 0 }; // IDX { S_ID }
    create_primary_idx_desc("S_IDX", 0, keys1, 1, pd);

    // create unique secondary index sub_nbr_index on (sub_nbr)
    uint keys2[1] = { 1 }; // IDX { SUB_NBR }

#ifdef USE_DORA_EXT_IDX
    if (pd & PD_NOLOCK) {
            // Create the index on sub_nbr extended with the key information.
            // This index will be accessed arbitrarily by multiple threads. 
            // Therefore, normal concurrency control will be used.
            uint keys2_ext[2] = { 1, 0 }; // IDX { SUB_NBR, S_ID }
            create_index_desc("SUB_NBR_IDX", 0, keys2_ext, 2, true, false, (pd ^ PD_NOLOCK));
    }
    else
#endif
        create_index_desc("SUB_NBR_IDX", 0, keys2, 1, true, false, pd);
}


access_info_t::access_info_t(const uint4_t& pd)
#ifdef CFG_HACK
    : table_desc_t("ACCESS_INFO", TM1_AI_FCOUNT+1, pd) 
#else
    : table_desc_t("ACCESS_INFO", TM1_AI_FCOUNT, pd) 
#endif
{
    // Schema
    _desc[0].setup(SQL_INT,        "S_ID");       // REF S.S_ID
    _desc[1].setup(SQL_SMALLINT,   "AI_TYPE");    // SMALLINT (1,4)   - (AI.S_ID,AI.AI_TYPE) is PRIMARY KEY
    _desc[2].setup(SQL_SMALLINT,   "DATA1");      // SMALLINT (0,255)
    _desc[3].setup(SQL_SMALLINT,   "DATA2");     
    _desc[4].setup(SQL_FIXCHAR,       "DATA3", TM1_AI_DATA3_SZ);   // CHAR (3). [A-Z]
    _desc[5].setup(SQL_FIXCHAR,       "DATA4", TM1_AI_DATA4_SZ);   // CHAR (5). [A-Z]


#ifdef CFG_HACK
    int padding_sz = 50-3*sizeof(short)-1*sizeof(int)
        -(TM1_AI_DATA3_SZ+TM1_AI_DATA4_SZ)*sizeof(char);
    _desc[6].setup(SQL_FIXCHAR,       "AI_PADDING", padding_sz);
#endif

    // There are between 1 and 4 Acess_Info records per Subscriber.
    // 25% Subscribers with one record
    // 25% Subscribers with two records
    // etc...

    // create unique index ai_index on (s_id, ai_type)
    uint keys[2] = { 0, 1 }; // IDX { S_ID, AI_TYPE }
    create_primary_idx_desc("AI_IDX", 0, keys, 2, pd);
}



special_facility_t::special_facility_t(const uint4_t& pd)
#ifdef CFG_HACK
    : table_desc_t("SPECIAL_FACILITY", TM1_SF_FCOUNT+1, pd) 
#else
    : table_desc_t("SPECIAL_FACILITY", TM1_SF_FCOUNT, pd) 
#endif
{
    // Schema
    _desc[0].setup(SQL_INT,        "S_ID");         // REF S.S_ID
    _desc[1].setup(SQL_SMALLINT,   "SF_TYPE");      // SMALLINT (1,4). (SF.S_ID,SF.SF_TYPE) PRIMARY KEY
    _desc[2].setup(SQL_BIT,        "IS_ACTIVE");    // BIT (0,1). 85% is 1 - 15% is 0
    _desc[3].setup(SQL_SMALLINT,   "ERROR_CNTRL");  // SMALLINT (0,255)
    _desc[4].setup(SQL_SMALLINT,   "DATA_A");       
    _desc[5].setup(SQL_FIXCHAR,       "DATA_B", TM1_SF_DATA_B_SZ);  // CHAR (5) [A-Z] 

#ifdef CFG_HACK
    int padding_sz = 50-1*sizeof(bool)-3*sizeof(short)-1*sizeof(int)
        -TM1_SF_DATA_B_SZ*sizeof(char);
    _desc[6].setup(SQL_FIXCHAR,       "SF_PADDING", padding_sz);
#endif

    // There are between 1 and 4 Special_Facility records per Subscriber.
    // 25% Subscribers with one sf
    // 25% Subscribers with two sf
    // etc...

    // create unique index sf_idx on (s_id, sf_type)
    uint keys[2] = { 0, 1 }; // IDX { S_ID, SF_TYPE }
    create_primary_idx_desc("SF_IDX", 0, keys, 2, pd);
}



call_forwarding_t::call_forwarding_t(const uint4_t& pd)
#ifdef CFG_HACK
    : table_desc_t("CALL_FORWARDING", TM1_CF_FCOUNT+1, pd) 
#else
    : table_desc_t("CALL_FORWARDING", TM1_CF_FCOUNT, pd)
#endif
{
    // Schema
    _desc[0].setup(SQL_INT,        "S_ID");        // REF SF.S_ID
    _desc[1].setup(SQL_SMALLINT,   "SF_TYPE");     // REF SF.SF_TYPE
    _desc[2].setup(SQL_SMALLINT,   "START_TIME");  // SMALLINT {0,8,16}
    _desc[3].setup(SQL_SMALLINT,   "END_TIME");    // SMALLINT START_TIME + URAND(1,8)
    _desc[4].setup(SQL_FIXCHAR,    "NUMBERX", TM1_CF_NUMBERX_SZ); // CHAR (15) [0-9]

#ifdef CFG_HACK
    int padding_sz = 50-3*sizeof(short)-1*sizeof(int)-TM1_CF_NUMBERX_SZ*sizeof(char);
    _desc[5].setup(SQL_FIXCHAR,       "CF_PADDING", padding_sz);
#endif

    // create unique index cf_idx on (s_id, sf_type, start_time)
    uint keys[3] = { 0, 1, 2 }; // IDX { S_ID, SF_TYPE, START_TIME }
    create_primary_idx_desc("CF_IDX", 0, keys, 3, pd);
}


EXIT_NAMESPACE(tm1);
