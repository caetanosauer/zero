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

/** @file tpch_struct.h
 *
 *  @brief Data structures for the TPC-H database
 *
 *  @author: Nastaran Nikparto, Summer 2011
 *  @author Ippokratis Pandis (ipandis)
 *
 */

#ifndef __TPCH_STRUCT_H
#define __TPCH_STRUCT_H

#include <cstdlib>
#include <unistd.h>
#include <sys/time.h>
#include "util.h"


ENTER_NAMESPACE(tpch);


// use this for allocation of NULL-terminated strings
#define STRSIZE(x) (x+1)


//Exported structures

enum tpch_l_shipmode {
    REG_AIR,
    AIR,
    RAIL,
    TRUCK,
    MAIL,
    FOB,
    SHIP,
    END_SHIPMODE
};

enum tpch_r_name{
    AFRICA,
    AMERICA,
    ASIA,
    EUROPE,
    MIDDLE_EAST,
    END_R_NAME
};

enum tpch_n_name{
    ALGERIA,
     ARGENTINA,
     BRAZIL,
     CANADA,
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
    CHINA,
    ROMANIA,
    SAUDI_ARABIA,
    VIETNAM,
    RUSSIA,
    UNITED_KINGDOM,
    UNITED_STATES,
    END_N_NAME
};

enum tpch_p_type_s1{
    STANDARD,
    SMALL,
    MEDIUM,
    LARGE,
    ECONOMY,
    PROMO,
    END_TYPE_S1
};

enum tpch_p_type_s2{
    ANODIZED,
    BURNISHED,
    PLATED,
    POLISHED,
    BRUSHED,
    END_TYPE_S2
};

enum tpch_p_type_s3{
    TIN,
    NICKEL,
    BRASS,
    STEEL,
    COPPER,
    END_TYPE_S3
};

enum tpch_p_container_s1{
    SM,
    LG,
    MED,
    JUMBO,
    WRAP,
    END_P_CONTAINER_S1
};

enum tpch_p_container_s2{

    CASE,
    BOX,
    BAG,
    JAR,
    PKG,
    PACK,
    CAN,
    DRUM,
    END_P_contAINER_S2
};

enum tpch_o_priority{

    O_URGENT,
    O_HIGH,
    O_MEDIUM,
    O_NOT_SPECIFIED,
    O_LOW,
    END_O_PRIORITY
};

enum tpch_c_segment{
    AUTOMOBILE,
    BUILDING,
    FURNITURE,
    MACHINERY,
    HOUSEHOLD,
    END_SEGMENT
};

struct tpch_p_type{
    int s1;
    int s2;
    int s3;
};

enum tpch_pname{
    almond, antique, aquamarine, azure, beige, bisque, black, blanched, blue, blush, brown, burlywood, burnished, chartreuse, chiffon, chocolate, coral, cornflower, cornsilk, cream, cyan, dark, deep, dim, dodger, drab, firebrick, floral, forest, frosted, gainsboro, ghost, goldenrod, green, grey, honeydew, hot, indian, ivory, khaki, lace, lavender, lawn, lemon, light, lime, linen, magenta, maroon, medium, metallic, midnight, mint, misty, moccasin, navajo, navy, olive, orange, orchid, pale, papaya, peach, peru, pink, plum, powder, puff, purple, red, rose, rosy, royal, saddle, salmon, sandy, seashell, sienna, sky, slate, smoke, snow, spring, steel, tan, thistle, tomato, turquoise, violet, wheat, white, yellow, END_P_NAME};


// TPC-H constants

const int NO_NATIONS = 25;
const int NO_REGIONS = 5;
const int SUPPLIER_PER_SF = 10000;
const int PART_PER_SF = 200000;
const int CUSTOMER_PER_SF = 150000;
const int ORDERS_PER_CUSTOMER = 10;
const int LINEITEM_PER_ORDER = 7;


// exported structures


// NATION 

struct tpch_nation_tuple 
{
    int  N_NATIONKEY;
    char N_NAME     [STRSIZE(25)];
    int  N_REGIONKEY;
    char N_COMMENT  [STRSIZE(152)];

    c_str tuple_to_str() {
        return(c_str("NATI = %d|%s|%d|%s",
                     N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT));
    }

};


struct tpch_nation_tuple_key 
{
    int  N_NATIONKEY;

    bool operator==(const tpch_nation_tuple_key& rhs) const {
        return (N_NATIONKEY == rhs.N_NATIONKEY);
    }

    bool operator<(const tpch_nation_tuple_key& rhs) const {
        return (N_NATIONKEY < rhs.N_NATIONKEY);
    }
};

struct tpch_nation_tuple_body 
{
    char N_NAME     [STRSIZE(25)];
    int  N_REGIONKEY;
    char N_COMMENT  [STRSIZE(152)];
};

// REGION

struct tpch_region_tuple 
{
    int  R_REGIONKEY;
    char R_NAME    [STRSIZE(25)];
    char R_COMMENT [STRSIZE(25)];

    c_str tuple_to_str() {
        return(c_str("REGN = %d|%s|%s",
                     R_REGIONKEY, R_NAME, R_COMMENT));
    }
};



struct tpch_region_tuple_key 
{
    int    R_REGIONKEY;

    bool operator==(const tpch_region_tuple_key& rhs) const {
        return ((R_REGIONKEY == rhs.R_REGIONKEY));
    }
    
    bool operator<(const tpch_region_tuple_key& rhs) const {
        if ((R_REGIONKEY < rhs.R_REGIONKEY))
            return (true);
        else
            return (false);
    }        
};


struct tpch_region_tuple_body 
{
    char R_NAME    [STRSIZE(25)];
    char R_COMMENT [STRSIZE(25)];
};


// PART

struct tpch_part_tuple 
{
    int      P_PARTKEY;
    char     P_NAME         [STRSIZE(55)]; 
    char     P_MFGR         [STRSIZE(25)];
    char     P_BRAND        [STRSIZE(10)]; 
    char     P_TYPE         [STRSIZE(25)]; 
    int      P_SIZE;
    char     P_CONTAINER    [STRSIZE(10)];
    decimal  P_RETAILPRICE;
    char     P_COMMENT      [STRSIZE(23)]; 
};



struct tpch_part_tuple_key {
    int P_PARTKEY;

    bool operator==(const tpch_part_tuple_key& rhs) const {
        return P_PARTKEY == rhs.P_PARTKEY;
    }

    bool operator<(const tpch_part_tuple_key& rhs) const {
        return P_PARTKEY < rhs.P_PARTKEY;
    }
};

struct tpch_part_tuple_body 
{
    char     P_NAME         [STRSIZE(55)]; 
    char     P_MFGR         [STRSIZE(25)];
    char     P_BRAND        [STRSIZE(10)]; 
    char     P_TYPE         [STRSIZE(25)]; 
    int      P_SIZE;
    char     P_CONTAINER    [STRSIZE(10)];
    decimal  P_RETAILPRICE;
    char     P_COMMENT      [STRSIZE(23)]; 
};


// SUPPLIER 

struct tpch_supplier_tuple 
{ // The whole record is the key
    int       S_SUPPKEY;
    char      S_NAME      [STRSIZE(25)];
    char      S_ADDRESS   [STRSIZE(40)]; 
    int       S_NATIONKEY;
    char      S_PHONE     [STRSIZE(15)];
    decimal   S_ACCTBAL;
    char      S_COMMENT   [STRSIZE(101)];
};

struct tpch_supplier_tuple_key 
{
    int S_SUPPKEY;

    bool operator==(const tpch_supplier_tuple_key& rhs) const {
        return S_SUPPKEY == rhs.S_SUPPKEY;
    }

    bool operator<(const tpch_supplier_tuple_key& rhs) const {
        return S_SUPPKEY < rhs.S_SUPPKEY;
    }
};


struct tpch_supplier_tuple_body 
{
    char      S_NAME      [STRSIZE(25)];
    char      S_ADDRESS   [STRSIZE(40)]; 
    int       S_NATIONKEY;
    char      S_PHONE     [STRSIZE(15)];
    decimal   S_ACCTBAL;
    char      S_COMMENT   [STRSIZE(101)];
};


// PARTSUPP

struct tpch_partsupp_tuple 
{
    int     PS_PARTKEY;
    int     PS_SUPPKEY;
    int     PS_AVAILQTY;
    decimal PS_SUPPLYCOST;
    char    PS_COMMENT [STRSIZE(199)];
};


struct tpch_partsupp_tuple_key 
{
    int     PS_PARTKEY;
    int     PS_SUPPKEY;

    bool operator==(const tpch_partsupp_tuple_key& rhs) const {
        return (PS_PARTKEY == rhs.PS_PARTKEY) &&
            (PS_SUPPKEY == rhs.PS_SUPPKEY);
    }

    bool operator<(const tpch_partsupp_tuple_key& rhs) const {
        return (PS_PARTKEY < rhs.PS_PARTKEY) ||
            ((PS_PARTKEY == rhs.PS_PARTKEY) && 
             (PS_SUPPKEY < rhs.PS_SUPPKEY)) ||
            ((PS_PARTKEY < rhs.PS_PARTKEY) &&
             (PS_SUPPKEY == rhs.PS_SUPPKEY));
    }
};


struct tpch_partsupp_tuple_body 
{
    int     PS_AVAILQTY;
    decimal PS_SUPPLYCOST;
    char    PS_COMMENT [STRSIZE(199)];
};

// CUSTOMER

struct tpch_customer_tuple 
{
    int     C_CUSTKEY;
    char    C_NAME       [STRSIZE(25)];
    char    C_ADDRESS    [STRSIZE(40)];
    int	    C_NATIONKEY;
    char    C_PHONE      [STRSIZE(15)];
    decimal C_ACCTBAL;
    char    C_MKTSEGMENT [STRSIZE(10)];
    char    C_COMMENT    [STRSIZE(117)];

    c_str tuple_to_str() {
        return(c_str("CUST = %d|%s|%s|%d|%s|%.2f|%s|%s -",
                     C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY,
                     C_PHONE, C_ACCTBAL.to_double(), C_MKTSEGMENT, C_COMMENT));
    }
    

};


struct tpch_customer_tuple_key {
    int C_CUSTKEY;

    bool operator==(const tpch_customer_tuple_key& rhs) const {
        return ((C_CUSTKEY == rhs.C_CUSTKEY));
    }
    
    bool operator<(const tpch_customer_tuple_key& rhs) const {
        if ((C_CUSTKEY < rhs.C_CUSTKEY))
            return (true);
        else
            return (false);
    }
};


struct tpch_customer_tuple_body 
{
    char    C_NAME       [STRSIZE(25)];
    char    C_ADDRESS    [STRSIZE(40)];
    int	    C_NATIONKEY;
    char    C_PHONE      [STRSIZE(15)];
    decimal C_ACCTBAL;
    char    C_MKTSEGMENT [STRSIZE(10)];
    char    C_COMMENT    [STRSIZE(117)];
};


// ORDERS

struct tpch_orders_tuple 
{
    int      O_ORDERKEY;
    int      O_CUSTKEY;
    char     O_ORDERSTATUS;
    decimal  O_TOTALPRICE;

//     time_t   O_ORDERDATE;
    char     O_ORDERDATE     [STRSIZE(15)];

    char     O_ORDERPRIORITY [STRSIZE(15)];
    char     O_CLERK         [STRSIZE(15)];
    int      O_SHIPPRIORITY;
    char     O_COMMENT       [STRSIZE(79)];
};


struct tpch_orders_tuple_key 
{
    int      O_ORDERKEY;
};


struct tpch_orders_tuple_body 
{
    int      O_CUSTKEY;
    char     O_ORDERSTATUS;
    decimal  O_TOTALPRICE;

//     time_t   O_ORDERDATE;
    char     O_ORDERDATE     [STRSIZE(15)];

    char     O_ORDERPRIORITY [STRSIZE(15)];
    char     O_CLERK         [STRSIZE(15)];
    int      O_SHIPPRIORITY;
    char     O_COMMENT       [STRSIZE(79)];
};



// LINEITEM

struct tpch_lineitem_tuple 
{
    int     L_ORDERKEY; 
    int     L_PARTKEY;
    int     L_SUPPKEY;
    int     L_LINENUMBER;
    double  L_QUANTITY;    
    double  L_EXTENDEDPRICE;
    double  L_DISCOUNT;
    double  L_TAX;
    char    L_RETURNFLAG;
    char    L_LINESTATUS;

//     time_t  L_SHIPDATE;
//     time_t  L_COMMITDATE;
//     time_t  L_RECEIPTDATE;
    char  L_SHIPDATE    [STRSIZE(15)];
    char  L_COMMITDATE  [STRSIZE(15)];
    char  L_RECEIPTDATE [STRSIZE(15)];

    char    L_SHIPINSTRUCT  [STRSIZE(25)];
    char    L_SHIPMODE      [STRSIZE(10)];
    char    L_COMMENT       [STRSIZE(44)];
};


struct tpch_lineitem_tuple_key 
{
    int L_ORDERKEY;
    int L_LINENUMBER;

    bool operator==(const tpch_lineitem_tuple_key& rhs) const {
        return (L_ORDERKEY == rhs.L_ORDERKEY) &&
            (L_LINENUMBER == rhs.L_LINENUMBER);
    }

    bool operator<(const tpch_lineitem_tuple_key& rhs) const {
        return (L_ORDERKEY < rhs.L_ORDERKEY) || 
            ((L_ORDERKEY == rhs.L_ORDERKEY) && 
             (L_LINENUMBER == rhs.L_LINENUMBER));
    }
};


struct tpch_lineitem_tuple_body 
{
    int     L_PARTKEY;
    int     L_SUPPKEY;
    int	    L_QUANTITY;    
    int	    L_EXTENDEDPRICE;
    int	    L_DISCOUNT;
    int	    L_TAX;
    char    L_RETURNFLAG;
    char    L_LINESTATUS;

//     time_t  L_SHIPDATE;
//     time_t  L_COMMITDATE;
//     time_t  L_RECEIPTDATE;
    char  L_SHIPDATE    [STRSIZE(15)];
    char  L_COMMITDATE  [STRSIZE(15)];
    char  L_RECEIPTDATE [STRSIZE(15)];

    char    L_SHIPINSTRUCT  [STRSIZE(25)];
    char    L_SHIPMODE      [STRSIZE(10)];
    char    L_COMMENT       [STRSIZE(44)];
};

EXIT_NAMESPACE(tpch);

#endif
