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

/* -*- mode:C++; c-basic-offset:4 -*- */

/** @file ssb_struct.h
 *
 *  @brief Data structures for the SSB database
 *
 *  @author Manos Athanassoulis
 *
 */

#ifndef __SSB_STRUCT_H
#define __SSB_STRUCT_H

#include <cstdlib>
#include <unistd.h>
#include <sys/time.h>
#include "util.h"


ENTER_NAMESPACE(ssb);


// use this for allocation of NULL-terminated strings
#define STRSIZE(x) (x+1)


//Exported structures

enum ssb_l_shipmode {
    REG_AIR,
    AIR,
    RAIL,
    TRUCK,
    MAIL,
    FOB,
    SHIP,
    END_SHIPMODE
};

enum ssb_nation{
    ALGERIA,
    ARGENTINA,
    BRAZIL,
    CANADA,
    CHINA,
    EGYPT,
    ETHIOPIA,
    FRANCE,
    GERMANY,
    INDIA,
    INDONESIA,
    IRAN,
    IRAQ,
    JAPAN,
    JORDAN,
    KENYA,
    MOROCCO,
    MOZAMBIQUE,
    PERU,
    ROMANIA,
    RUSSIA,
    SAUDI_ARABIA,
    UNITED_KINGDOM,
    UNITED_STATES,
    VIETNAM
};


// SSB constants

const int SUPPLIER_PER_SF  =    2000;
const int CUSTOMER_PER_SF  =   30000;
const int PART_PER_SF      =  200000; //actually 200K*(1+log2(SF))
const int LINEORDER_PER_SF = 1500000; // results in ~6000000 tuples for SF=1
const int NO_DATE          =    2556;


// exported structures



// PART

struct ssb_part_tuple 
{
    int      P_PARTKEY;
    char     P_NAME         [STRSIZE(22)]; 
    char     P_MFGR         [STRSIZE(6)];
    char     P_CATEGORY     [STRSIZE(7)]; 
    char     P_BRAND        [STRSIZE(9)]; 
    char     P_COLOR        [STRSIZE(11)];
    char     P_TYPE         [STRSIZE(25)];
    int      P_SIZE;
    char     P_CONTAINER    [STRSIZE(10)]; 
};



struct ssb_part_tuple_key {
    int P_PARTKEY;

    bool operator==(const ssb_part_tuple_key& rhs) const {
        return P_PARTKEY == rhs.P_PARTKEY;
    }

    bool operator<(const ssb_part_tuple_key& rhs) const {
        return P_PARTKEY < rhs.P_PARTKEY;
    }
};

struct ssb_part_tuple_body 
{
    int       S_SUPPKEY;
    char      S_NAME      [STRSIZE(25)];
    char      S_ADDRESS   [STRSIZE(40)];
    char      S_PHONE     [STRSIZE(15)];
    decimal   S_ACCTBAL;
    char      S_COMMENT   [STRSIZE(101)];
};




// SUPPLIER 

struct ssb_supplier_tuple 
{
    int       S_SUPPKEY;
    char      S_NAME      [STRSIZE(25)];
    char      S_ADDRESS   [STRSIZE(25)]; 
    char      S_CITY      [STRSIZE(10)];
    char      S_NATION    [STRSIZE(15)];
    char      S_REGION    [STRSIZE(12)];
    char      S_PHONE     [STRSIZE(15)];
};

struct ssb_supplier_tuple_key 
{
    int S_SUPPKEY;

    bool operator==(const ssb_supplier_tuple_key& rhs) const {
        return S_SUPPKEY == rhs.S_SUPPKEY;
    }

    bool operator<(const ssb_supplier_tuple_key& rhs) const {
        return S_SUPPKEY < rhs.S_SUPPKEY;
    }
};


struct ssb_supplier_tuple_body 
{
    int       S_SUPPKEY;
    char      S_NAME      [STRSIZE(25)];
    char      S_ADDRESS   [STRSIZE(25)]; 
    char      S_CITY      [STRSIZE(10)];
    char      S_NATION    [STRSIZE(15)];
    char      S_REGION    [STRSIZE(12)];
    char      S_PHONE     [STRSIZE(15)];
};


// DATE

struct ssb_date_tuple 
{
    int     D_DATEKEY;
    char    D_DATE      [STRSIZE(18)];
    char    D_DAYOFWEEK [STRSIZE(9)];
    char    D_MONTH     [STRSIZE(9)];
    int     D_YEAR;
    int     D_YEARMONTHNUM;
    char    D_YEARMONTH [STRSIZE(7)];
    int     D_DAYNUMINWEEK;
    int     D_DAYNUMINMONTH;
    int     D_DAYNUMINYEAR;
    int     D_MONTHNUMINYEAR;
    int     D_WEEKNUMINYEAR;
    char    D_SELLINGSEASON [STRSIZE(12)];
    char    D_LASTDAYINWEEKFL[2];
    char    D_LASTDAYINMONTHFL[2];
    char    D_HOLIDAYFL[2];
    char    D_WEEKDAYFL[2];
};


struct ssb_date_tuple_key 
{
    int     D_DATEKEY;

    bool operator==(const ssb_date_tuple_key& rhs) const {
        return (D_DATEKEY == rhs.D_DATEKEY);

    }

    bool operator<(const ssb_date_tuple_key& rhs) const {
        return (D_DATEKEY < rhs.D_DATEKEY);
    }


};






// CUSTOMER

struct ssb_customer_tuple 
{
    int     C_CUSTKEY;
    char    C_NAME       [STRSIZE(25)];
    char    C_ADDRESS    [STRSIZE(25)];
    char    C_CITY       [STRSIZE(10)];
    char    C_NATION     [STRSIZE(15)];
    char    C_REGION [STRSIZE(12)];
    char    C_PHONE [STRSIZE(15)];
    char    C_MKTSEGMENT [STRSIZE(10)];


    c_str tuple_to_str() {
        return (c_str("CUST = %d|%s|%s|%s|%s|%s|%s -",
                C_CUSTKEY, C_NAME, C_ADDRESS, 
                C_NATION, C_REGION, C_PHONE, C_MKTSEGMENT));
    }
    

};


struct ssb_customer_tuple_key {
    int C_CUSTKEY;

    bool operator==(const ssb_customer_tuple_key& rhs) const {
        return ((C_CUSTKEY == rhs.C_CUSTKEY));
    }
    
    bool operator<(const ssb_customer_tuple_key& rhs) const {
        if ((C_CUSTKEY < rhs.C_CUSTKEY))
            return (true);
        else
            return (false);
    }
};


struct ssb_customer_tuple_body 
{
    char    C_NAME       [STRSIZE(25)];
    char    C_ADDRESS    [STRSIZE(40)];
    char    C_PHONE      [STRSIZE(15)];
    decimal C_ACCTBAL;
    char    C_MKTSEGMENT [STRSIZE(10)];
    char    C_COMMENT    [STRSIZE(117)];
};





// LINEORDER

struct ssb_lineorder_tuple 
{
    int     LO_ORDERKEY; 
    int     LO_LINENUMBER;
    int     LO_CUSTKEY;
    int     LO_PARTKEY;
    int     LO_SUPPKEY;
    int     LO_ORDERDATE;
    char    LO_ORDERPRIORITY [STRSIZE(15)];
    int    LO_SHIPPRIORITY;
    int     LO_QUANTITY;
    int     LO_EXTENDEDPRICE;
    int     LO_ORDTOTALPRICE;
    int     LO_DISCOUNT;
    int   LO_REVENUE;
    int     LO_SUPPLYCOST;
    int     LO_TAX;
    int     LO_COMMIDATE;
    char    LO_SHIPMODE [STRSIZE(10)];

    c_str tuple_to_str() {
        return(c_str("LO = %d|%d|%d|%d|%d|%d|%s -",
                     LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY,
                     LO_SUPPKEY, LO_ORDERDATE, LO_ORDERPRIORITY));
    }

    c_str key_to_str() {
        return(c_str("LO = %d|%d -",
                     LO_ORDERKEY, LO_LINENUMBER));
    }

};


struct ssb_lineitem_tuple_key 
{
    int LO_ORDERKEY;
    int LO_LINENUMBER;

    bool operator==(const ssb_lineitem_tuple_key& rhs) const {
        return (LO_ORDERKEY == rhs.LO_ORDERKEY) &&
            (LO_LINENUMBER == rhs.LO_LINENUMBER);
    }

    bool operator<(const ssb_lineitem_tuple_key& rhs) const {
        return (LO_ORDERKEY < rhs.LO_ORDERKEY) || 
            ((LO_ORDERKEY == rhs.LO_ORDERKEY) && 
             (LO_LINENUMBER == rhs.LO_LINENUMBER));
    }
};


struct ssb_lineitem_tuple_body 
{
    int     LO_ORDERKEY; 
    int     LO_LINENUMBER;
    int     LO_CUSTKEY;
    int     LO_PARTKEY;
    int     LO_SUPPKEY;
    int     LO_ORDERDATE;
    char    LO_ORDERPRIORITY [STRSIZE(15)];
    char    LO_SHIPPRIORITY;
    int     LO_QUANTITY;
    int     LO_EXTENDEDPRICE;
    int     LO_ORDTOTALPRICE;
    int     LO_DISCOUNT;
    double   LO_REVENUE;
    int     LO_SUPPLYCOST;
    int     LO_TAX;
    int     LO_COMMIDATE;
    char    LO_SHIPMODE [STRSIZE(10)];
};

EXIT_NAMESPACE(ssb);

#endif
