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

/** @file tpcc_struct.h
 *
 *  @brief Data structures for the TPC-C database
 *
 *  @author Ippokratis Pandis (ipandis)
 *
 */

#ifndef __TPCC_STRUCT_H
#define __TPCC_STRUCT_H

#include <cstdlib>
#include <unistd.h>
#include <sys/time.h>
#include "util.h"


ENTER_NAMESPACE(tpcc);


/* use this for allocation of NULL-terminated strings */
#define STRSIZE(x)(x+1)



/* exported structures */


// CUSTOMER

struct tpcc_customer_tuple {
    int     C_C_ID;
    int     C_D_ID;
    int     C_W_ID;
    char    C_FIRST    [STRSIZE(16)];
    char    C_MIDDLE   [STRSIZE(2)];
    char    C_LAST     [STRSIZE(16)];
    char    C_STREET_1 [STRSIZE(20)];
    char    C_STREET_2 [STRSIZE(20)];
    char    C_CITY     [STRSIZE(20)];
    char    C_STATE    [STRSIZE(2)];
    char    C_ZIP      [STRSIZE(9)];
    char    C_PHONE    [STRSIZE(16)];
    time_t  C_SINCE;
    char    C_CREDIT   [STRSIZE(2)];
    decimal C_CREDIT_LIM;
    decimal C_DISCOUNT;
    decimal C_BALANCE;
    decimal C_YTD_PAYMENT;
    decimal C_LAST_PAYMENT;
    int     C_PAYMENT_CNT;
    char    C_DATA_1   [STRSIZE(250)];
    char    C_DATA_2   [STRSIZE(250)];    

    c_str tuple_to_str() {
        return(c_str("CUST = %d|%d|%d|%s|%s|%s|%s|%s|%s|%s|%s|%s|%.2f|%s -",
                     C_C_ID, C_D_ID, C_W_ID, C_FIRST, C_MIDDLE, C_LAST,
                     C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
                     C_PHONE, C_SINCE, C_CREDIT));
    }
    

};


struct tpcc_customer_tuple_key {
    int C_C_ID;
    int C_D_ID;
    int C_W_ID;

    bool operator==(const tpcc_customer_tuple_key& rhs) const {
        return ((C_C_ID == rhs.C_C_ID) && 
                (C_D_ID == rhs.C_D_ID) &&
                (C_W_ID == rhs.C_W_ID));
    }
    
    bool operator<(const tpcc_customer_tuple_key& rhs) const {
        if ((C_C_ID < rhs.C_C_ID) ||
            ((C_C_ID == rhs.C_C_ID) && (C_D_ID < rhs.C_D_ID)) ||
            ((C_C_ID == rhs.C_C_ID) && (C_D_ID == rhs.C_D_ID) && (C_W_ID < rhs.C_W_ID)))
            return (true);
        else
            return (false);
    }
};


struct tpcc_customer_tuple_body {
    char    C_FIRST    [STRSIZE(16)];
    char    C_MIDDLE   [STRSIZE(2)];
    char    C_LAST     [STRSIZE(16)];
    char    C_STREET_1 [STRSIZE(20)];
    char    C_STREET_2 [STRSIZE(20)];
    char    C_CITY     [STRSIZE(20)];
    char    C_STATE    [STRSIZE(2)];
    char    C_ZIP      [STRSIZE(9)];
    char    C_PHONE    [STRSIZE(16)];
    time_t  C_SINCE;
    char    C_CREDIT   [STRSIZE(2)];
    decimal C_CREDIT_LIM;
    decimal C_DISCOUNT;
    decimal C_BALANCE;
    decimal C_YTD_PAYMENT;
    decimal C_LAST_PAYMENT;
    int     C_PAYMENT_CNT;
    char    C_DATA_1   [STRSIZE(250)];
    char    C_DATA_2   [STRSIZE(250)];    
};


// DISTRICT

struct tpcc_district_tuple {
    int D_ID;
    int D_W_ID;
    char D_NAME     [STRSIZE(10)];
    char D_STREET_1 [STRSIZE(20)];
    char D_STREET_2 [STRSIZE(20)];
    char D_CITY     [STRSIZE(20)];
    char D_STATE    [STRSIZE(2)];
    char D_ZIP      [STRSIZE(9)];
    decimal D_TAX;
    decimal D_YTD;
    int D_NEXT_O_ID;


    c_str tuple_to_str() {
        return(c_str("DISTR = %d|%d|%s|%s|%s|%s|%s|%s|%.2f|%.2f|%d",
                     D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2,
                     D_CITY, D_STATE, D_ZIP, 
                     D_TAX.to_double(), D_YTD.to_double(), D_NEXT_O_ID));
    }

};


struct tpcc_district_tuple_key {
    int D_ID;
    int D_W_ID;
};



// HISTORY

struct tpcc_history_tuple {
    int     H_C_ID;
    int     H_C_D_ID;
    int     H_C_W_ID;
    int     H_D_ID;
    int     H_W_ID;
    time_t  H_DATE;
    decimal H_AMOUNT;
    char    H_DATA [STRSIZE(25)];

    c_str tuple_to_str() {
        return(c_str("HIST = %d|%d|%d|%d|%d|%.2f|%.2f|%s",
                     H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID,
                     H_W_ID, H_DATE, H_AMOUNT.to_double(), 
                     H_DATA));
    }
};



struct tpcc_history_tuple_key {
    int    H_C_ID;
    int    H_C_D_ID;
    int    H_C_W_ID;
    int    H_D_ID;
    int    H_W_ID;
    time_t H_DATE;

    bool operator==(const tpcc_history_tuple_key& rhs) const {
        return ((H_C_ID == rhs.H_C_ID) && 
                (H_C_D_ID == rhs.H_C_D_ID) && 
                (H_C_W_ID == rhs.H_C_W_ID) && 
                (H_D_ID == rhs.H_D_ID) && 
                (H_W_ID == rhs.H_W_ID) && 
                (H_DATE == rhs.H_DATE));
    }
    
    bool operator<(const tpcc_history_tuple_key& rhs) const {
        if ((H_C_ID < rhs.H_C_ID) ||
            ((H_C_ID == rhs.H_C_ID) && (H_C_D_ID < rhs.H_C_D_ID)) ||
            ((H_C_ID == rhs.H_C_ID) && (H_C_D_ID == rhs.H_C_D_ID) && 
             (H_C_W_ID < rhs.H_C_W_ID)) ||
            ((H_C_ID == rhs.H_C_ID) && (H_C_D_ID == rhs.H_C_D_ID) && 
             (H_C_W_ID == rhs.H_C_W_ID) && (H_D_ID < rhs.H_D_ID)) ||
            ((H_C_ID == rhs.H_C_ID) && (H_C_D_ID == rhs.H_C_D_ID) && 
             (H_C_W_ID == rhs.H_C_W_ID) && (H_D_ID == rhs.H_D_ID) &&
             (H_W_ID < rhs.H_W_ID)) ||
            ((H_C_ID == rhs.H_C_ID) && (H_C_D_ID == rhs.H_C_D_ID) && 
             (H_C_W_ID == rhs.H_C_W_ID) && (H_D_ID == rhs.H_D_ID) &&
             (H_W_ID == rhs.H_W_ID) && (H_DATE == rhs.H_DATE))
            )
            return (true);
        else
            return (false);
    }        
};


struct tpcc_history_tuple_body {
    decimal H_AMOUNT;
    char    H_DATA [STRSIZE(25)];
};


// ITEM

struct tpcc_item_tuple {
    int  I_ID;
    int  I_IM_ID;
    char I_NAME [STRSIZE(24)];
    int  I_PRICE;
    char I_DATA [STRSIZE(50)];
};



struct tpcc_item_tuple_key {
    int I_ID;
};



// NEW_ORDER

struct tpcc_new_order_tuple { // The whole record is the key
    int NO_O_ID;
    int NO_D_ID;
    int NO_W_ID;
};


// ORDER

struct tpcc_order_tuple {
    int    O_ID;
    int    O_C_ID;
    int    O_D_ID;
    int    O_W_ID;
    time_t O_ENTRY_D;
    int    O_CARRIER_ID;
    int    O_OL_CNT;
    int    O_ALL_LOCAL;
};


struct tpcc_order_tuple_key {
    int O_ID;
    int O_C_ID;
    int O_D_ID;
    int O_W_ID;
};


struct tpcc_order_tuple_body {
    time_t O_ENTRY_D;
    int    O_CARRIER_ID;
    int    O_OL_CNT;
    int    O_ALL_LOCAL;
};




// ORDERLINE

struct tpcc_orderline_tuple {
    int    OL_O_ID;
    int    OL_D_ID;
    int    OL_W_ID;
    int    OL_NUMBER;
    int    OL_I_ID;
    int    OL_SUPPLY_W_ID;
    time_t OL_DELIVERY_D;
    int    OL_QUANTITY;
    int    OL_AMOUNT;
    char   OL_DIST_INFO [STRSIZE(25)];
};


struct tpcc_orderline_tuple_key {
    int OL_O_ID;
    int OL_D_ID;
    int OL_W_ID;
    int OL_NUMBER;
};


struct tpcc_orderline_tuple_body {
    int    OL_I_ID;
    int    OL_SUPPLY_W_ID;
    time_t OL_DELIVERY_D;
    int    OL_QUANTITY;
    int    OL_AMOUNT;
    char   OL_DIST_INFO [STRSIZE(25)];
};



// STOCK

struct tpcc_stock_tuple {
    int S_I_ID;
    int S_W_ID;
    int S_REMOTE_CNT;
    int S_QUANTITY;
    int S_ORDER_CNT;
    int S_YTD;

    char S_DIST[10][STRSIZE(24)];

#if 0
    char S_DIST_01 [STRSIZE(24)];
    char S_DIST_02 [STRSIZE(24)];
    char S_DIST_03 [STRSIZE(24)];
    char S_DIST_04 [STRSIZE(24)];
    char S_DIST_05 [STRSIZE(24)];
    char S_DIST_06 [STRSIZE(24)];
    char S_DIST_07 [STRSIZE(24)];
    char S_DIST_08 [STRSIZE(24)];
    char S_DIST_09 [STRSIZE(24)];
    char S_DIST_10 [STRSIZE(24)];
#endif

    char S_DATA    [STRSIZE(50)];
};


struct tpcc_stock_tuple_key {
    int S_I_ID;
    int S_W_ID;
};


// WAREHOUSE

struct tpcc_warehouse_tuple {
    int W_ID;
    char W_NAME     [STRSIZE(10)];
    char W_STREET_1 [STRSIZE(20)];
    char W_STREET_2 [STRSIZE(20)];
    char W_CITY     [STRSIZE(20)];
    char W_STATE    [STRSIZE(2)];
    char W_ZIP      [STRSIZE(9)];
    decimal W_TAX;
    decimal W_YTD;

    c_str tuple_to_str() {
        return(c_str("WH= %d|%s|%s|%s|%s|%s|%s|%.2f|%.2f",
                     W_ID, W_NAME, W_STREET_1, W_STREET_2, 
                     W_CITY, W_STATE, W_ZIP, 
                     W_TAX.to_double(), W_YTD.to_double()));
    }
};

struct tpcc_warehouse_tuple_key {
    int W_ID;
};

EXIT_NAMESPACE(tpcc);

#endif
