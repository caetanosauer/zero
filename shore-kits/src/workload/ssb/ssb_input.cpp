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

/** @file ssb_input.cpp
 *
 *  @brief Implementation of the (common) inputs for the SSB trxs
 *  @brief Generate inputs for the SSB TRXs
 *
 *  @author Manos Athanassoulis
 */


#include "util.h"
#include "workload/ssb/ssb_random.h" 
#include "workload/ssb/ssb_util.h" 
#include "workload/ssb/ssb_input.h"

#include "workload/ssb/dbgen/dss.h"
#include "workload/ssb/dbgen/dsstypes.h"

using namespace dbgenssb; // for random MODES etc..

ENTER_NAMESPACE(ssb);

qdate_input_t       create_qdate_input(const double /*sf*/, const int /*specificWH*/) 
{qdate_input_t a;return a;}
qpart_input_t       create_qpart_input(const double /*sf*/, const int /*specificWH*/) 
{qpart_input_t a;return a;}
qsupplier_input_t       create_qsupplier_input(const double /*sf*/, const int /*specificWH*/) 
{qsupplier_input_t a;return a;}
qcustomer_input_t       create_qcustomer_input(const double /*sf*/, const int /*specificWH*/) 
{qcustomer_input_t a;return a;}
qlineorder_input_t create_qlineorder_input(const double /*sf*/, const int /*specificWH*/) 
{qlineorder_input_t a;return a;}
qtest_input_t create_qtest_input(const double /*sf*/, const int /*specificWH*/)
{qtest_input_t a;return a;}
//q1_2_input_t create_q1_2_input(const double /*sf*/, const int /*specificWH*/) {q1_2_input_t a;return a;}
//q1_3_input_t create_q1_3_input(const double /*sf*/, const int /*specificWH*/) {q1_3_input_t a;return a;}
//q2_1_input_t create_q2_1_input(const double /*sf*/, const int /*specificWH*/) {q2_1_input_t a;return a;}
//q2_2_input_t create_q2_2_input(const double /*sf*/, const int /*specificWH*/) {q2_2_input_t a;return a;}
//q2_3_input_t create_q2_3_input(const double /*sf*/, const int /*specificWH*/) {q2_3_input_t a;return a;}
//q3_1_input_t create_q3_1_input(const double /*sf*/, const int /*specificWH*/) {q3_1_input_t a;return a;}
//q3_3_input_t create_q3_3_input(const double /*sf*/, const int /*specificWH*/) {q3_3_input_t a;return a;}
//q3_4_input_t create_q3_4_input(const double /*sf*/, const int /*specificWH*/) {q3_4_input_t a;return a;}
//q4_1_input_t create_q4_1_input(const double /*sf*/, const int /*specificWH*/) {q4_1_input_t a;return a;}
//q4_2_input_t create_q4_2_input(const double /*sf*/, const int /*specificWH*/) {q4_2_input_t a;return a;}
//q4_3_input_t create_q4_3_input(const double /*sf*/, const int /*specificWH*/) {q4_3_input_t a;return a;}


/******************************************************************** 
 *
 *  Q1_1
 *
 ********************************************************************/

q1_1_input_t& q1_1_input_t::operator=(const q1_1_input_t& rhs)
{
    d_year = rhs.d_year;
    lo_discount_lo = rhs.lo_discount_lo;
    lo_discount_hi = rhs.lo_discount_hi;
    lo_quantity = rhs.lo_quantity;

    return (*this);
}

q1_1_input_t create_q1_1_input(const double /*sf*/, const int /*specificWH*/)

{       
    q1_1_input_t q1_1_in;
    
    q1_1_in.d_year = 1993;
    q1_1_in.lo_discount_lo = 1;
    q1_1_in.lo_discount_hi = 3;
    q1_1_in.lo_quantity = 25;
    
    printf("q1_1_in: %d %d %d %d \n",q1_1_in.d_year,q1_1_in.lo_discount_lo,q1_1_in.lo_discount_hi,q1_1_in.lo_quantity);
    
    return q1_1_in;
}

/******************************************************************** 
 *
 *  Q1_2
 *
 ********************************************************************/
q1_2_input_t& q1_2_input_t::operator=(const q1_2_input_t& rhs)
{
    d_yearmonthnum = rhs.d_yearmonthnum;
    lo_discount_lo = rhs.lo_discount_lo;
    lo_discount_hi = rhs.lo_discount_hi;
    lo_quantity_lo = rhs.lo_quantity_lo;
    lo_quantity_hi = rhs.lo_quantity_hi;

    return (*this);
}

q1_2_input_t create_q1_2_input(const double /*sf*/, const int /*specificWH*/)

{       
    q1_2_input_t q1_2_in;
    
    q1_2_in.d_yearmonthnum = 199401;
    q1_2_in.lo_discount_lo = 4;
    q1_2_in.lo_discount_hi = 6;
    q1_2_in.lo_quantity_lo = 26;
    q1_2_in.lo_quantity_hi = 35;
    
    printf("q1_2_in: %d %d %d %d %d\n",q1_2_in.d_yearmonthnum,q1_2_in.lo_discount_lo,q1_2_in.lo_discount_hi,q1_2_in.lo_quantity_lo,q1_2_in.lo_quantity_hi);
    
    return q1_2_in;
}

/******************************************************************** 
 *
 *  Q1_3
 *
 ********************************************************************/

q1_3_input_t& q1_3_input_t::operator=(const q1_3_input_t& rhs)
{
    d_weeknuminyear = rhs.d_weeknuminyear;
    d_year = rhs.d_year;
    lo_discount_lo = rhs.lo_discount_lo;
    lo_discount_hi = rhs.lo_discount_hi;
    lo_quantity_lo = rhs.lo_quantity_lo;
    lo_quantity_hi = rhs.lo_quantity_hi;

    return (*this);
}

q1_3_input_t create_q1_3_input(const double /*sf*/, const int /*specificWH*/)

{       
    q1_3_input_t q1_3_in;
    
    q1_3_in.d_weeknuminyear = 6;
    q1_3_in.d_year = 1994;
    q1_3_in.lo_discount_lo = 5;
    q1_3_in.lo_discount_hi = 7;
    q1_3_in.lo_quantity_lo = 26;
    q1_3_in.lo_quantity_hi = 35;
    
    printf("q1_3_in: %d %d %d %d %d %d\n",q1_3_in.d_weeknuminyear,q1_3_in.d_year,q1_3_in.lo_discount_lo,q1_3_in.lo_discount_hi,q1_3_in.lo_quantity_lo,q1_3_in.lo_quantity_hi);
    
    return q1_3_in;
}

/******************************************************************** 
 *
 *  Q2_1
 *
 ********************************************************************/

q2_1_input_t& q2_1_input_t::operator=(const q2_1_input_t& rhs)
{
    strcpy(p_category,rhs.p_category);
    strcpy(s_region,rhs.s_region);

    return (*this);
}

q2_1_input_t create_q2_1_input(const double /*sf*/, const int /*specificWH*/)

{       
    q2_1_input_t q2_1_in;
    
    strcpy(q2_1_in.p_category,"MFGR#12");
    strcpy(q2_1_in.s_region,"AMERICA");
    
    printf("q2_1_in: %s %s\n",q2_1_in.p_category,q2_1_in.s_region);
    
    return q2_1_in;
}

/******************************************************************** 
 *
 *  Q2_2
 *
 ********************************************************************/

q2_2_input_t& q2_2_input_t::operator=(const q2_2_input_t& rhs)
{
    strcpy(p_brand_1,rhs.p_brand_1);
    strcpy(p_brand_2,rhs.p_brand_2);
    strcpy(s_region,rhs.s_region);

    return (*this);
}

q2_2_input_t create_q2_2_input(const double /*sf*/, const int /*specificWH*/)

{       
    q2_2_input_t q2_2_in;
    
    strcpy(q2_2_in.p_brand_1,"MFGR#2221");
    strcpy(q2_2_in.p_brand_2,"MFGR#2228");
    strcpy(q2_2_in.s_region ,"ASIA");
    
    printf("q2_2_in: %s %s %s\n",q2_2_in.p_brand_1,q2_2_in.p_brand_2,q2_2_in.s_region);
    
    return q2_2_in;
}


/******************************************************************** 
 *
 *  Q2_3
 *
 ********************************************************************/

q2_3_input_t& q2_3_input_t::operator=(const q2_3_input_t& rhs)
{
    strcpy(p_brand,rhs.p_brand);
    strcpy(s_region,rhs.s_region);

    return (*this);
}

q2_3_input_t create_q2_3_input(const double /*sf*/, const int /*specificWH*/)

{       
    q2_3_input_t q2_3_in;
    
    strcpy(q2_3_in.p_brand ,"MFGR#2239");
    strcpy(q2_3_in.s_region ,"EUROPE");
    
    printf("q2_3_in: %s %s\n",q2_3_in.p_brand,q2_3_in.s_region);
    
    return q2_3_in;
}

/******************************************************************** 
 *
 *  Q3_1
 *
 ********************************************************************/

q3_1_input_t& q3_1_input_t::operator=(const q3_1_input_t& rhs)
{
    _year_lo = rhs._year_lo;
    _year_hi = rhs._year_hi;
    
    strcpy(c_region,rhs.c_region);
    strcpy(s_region,rhs.s_region);
    
    return (*this);
}


q3_1_input_t create_q3_1_input(const double /*sf*/, const int /*specificWH*/)
{
    q3_1_input_t q3_1_in;
    
    q3_1_in._year_lo = 1992;
    q3_1_in._year_hi = 1997;
    strcpy(q3_1_in.c_region ,"ASIA");
    strcpy(q3_1_in.s_region ,"ASIA");
    
    printf("q3_1_in: %d %d %s %s\n",q3_1_in._year_lo,q3_1_in._year_hi,q3_1_in.c_region,q3_1_in.s_region);
    
    return q3_1_in;
}

/******************************************************************** 
 *
 *  Q3_2
 *
 ********************************************************************/


q3_2_input_t& q3_2_input_t::operator=(const q3_2_input_t& rhs)
{
    _year_lo = rhs._year_lo;
    _year_hi = rhs._year_hi;
    
    strcpy(_nation,rhs._nation);
    
    return (*this);
}


q3_2_input_t create_q3_2_input(const double /*sf*/, const int /*specificWH*/)
{
    q3_2_input_t q3_2_in;
    int _id_nation;
    
    q3_2_in._year_lo=URand(1992,1998);
    q3_2_in._year_hi=URand(q3_2_in._year_lo,1998);
    _id_nation=URand(0,24);
    get_nation(q3_2_in._nation,(ssb_nation)_id_nation);
    
    printf("q3_2_in: %d %d %s\n",q3_2_in._year_lo,q3_2_in._year_hi,q3_2_in._nation);
    //printf("ENUM: %d %d",ALGERIA,VIETNAM);
    //strcpy(q3_2_in._nation,"UNITED STATES");
    
    return q3_2_in;
}

/******************************************************************** 
 *
 *  Q3_3
 *
 ********************************************************************/


q3_3_input_t& q3_3_input_t::operator=(const q3_3_input_t& rhs)
{
    _year_lo = rhs._year_lo;
    _year_hi = rhs._year_hi;
    
    strcpy(c_city_1,rhs.c_city_1);
    strcpy(c_city_2,rhs.c_city_2);
    strcpy(s_city_1,rhs.s_city_1);
    strcpy(s_city_2,rhs.s_city_2);
    
    return (*this);
}


q3_3_input_t create_q3_3_input(const double /*sf*/, const int /*specificWH*/)
{
    q3_3_input_t q3_3_in;
    
    q3_3_in._year_lo=1992;
    q3_3_in._year_hi=1997;
    strcpy(q3_3_in.c_city_1,"UNITED KI1");
    strcpy(q3_3_in.c_city_2,"UNITED KI5");
    strcpy(q3_3_in.s_city_1,"UNITED KI1");
    strcpy(q3_3_in.s_city_2,"UNITED KI5");
    
    
    printf("q3_3_in: %d %d %s %s %s %s\n",q3_3_in._year_lo,q3_3_in._year_hi,q3_3_in.c_city_1,q3_3_in.c_city_2,q3_3_in.s_city_1,q3_3_in.s_city_2);
    
    return q3_3_in;
}


/******************************************************************** 
 *
 *  Q3_4
 *
 ********************************************************************/

q3_4_input_t& q3_4_input_t::operator=(const q3_4_input_t& rhs)
{   
    strcpy(c_city_1,rhs.c_city_1);
    strcpy(c_city_2,rhs.c_city_2);
    strcpy(s_city_1,rhs.s_city_1);
    strcpy(s_city_2,rhs.s_city_2);
    strcpy(d_yearmonth,rhs.d_yearmonth);
    
    return (*this);
}


q3_4_input_t create_q3_4_input(const double /*sf*/, const int /*specificWH*/)
{
    q3_4_input_t q3_4_in;
    
    strcpy(q3_4_in.c_city_1 , "UNITED KI1");
    strcpy(q3_4_in.c_city_2 , "UNITED KI5");
    strcpy(q3_4_in.s_city_1 , "UNITED KI1");
    strcpy(q3_4_in.s_city_2 , "UNITED KI5");
    strcpy(q3_4_in.d_yearmonth , "Dec1997");
    
    printf("q3_4_in: %s %s %s %s %s\n",q3_4_in.c_city_1,q3_4_in.c_city_2,q3_4_in.s_city_1,q3_4_in.s_city_2,q3_4_in.d_yearmonth);
    
    return q3_4_in;
}


/******************************************************************** 
 *
 *  Q4_1
 *
 ********************************************************************/

q4_1_input_t& q4_1_input_t::operator=(const q4_1_input_t& rhs)
{   
    strcpy(c_region,rhs.c_region);
    strcpy(s_region,rhs.s_region);
    strcpy(p_mfgr_1,rhs.p_mfgr_1);
    strcpy(p_mfgr_2,rhs.p_mfgr_2);
    
    return (*this);
}


q4_1_input_t create_q4_1_input(const double /*sf*/, const int /*specificWH*/)
{
    q4_1_input_t q4_1_in;
    
    strcpy(q4_1_in.c_region , "AMERICA");
    strcpy(q4_1_in.s_region , "AMERICA");
    strcpy(q4_1_in.p_mfgr_1 , "MFGR#1");
    strcpy(q4_1_in.p_mfgr_2 , "MFGR#2");
    
    printf("q4_1_in: %s %s %s %s\n",q4_1_in.c_region,q4_1_in.s_region,q4_1_in.p_mfgr_1,q4_1_in.p_mfgr_2);
    
    return q4_1_in;
}


/******************************************************************** 
 *
 *  Q4_2
 *
 ********************************************************************/

q4_2_input_t& q4_2_input_t::operator=(const q4_2_input_t& rhs)
{   
    d_year_1 = rhs.d_year_1;
    d_year_2 = rhs.d_year_2;
    strcpy(c_region,rhs.c_region);
    strcpy(s_region,rhs.s_region);
    strcpy(p_mfgr_1,rhs.p_mfgr_1);
    strcpy(p_mfgr_2,rhs.p_mfgr_2);
    
    return (*this);
}


q4_2_input_t create_q4_2_input(const double /*sf*/, const int /*specificWH*/)
{
    q4_2_input_t q4_2_in;
    
    q4_2_in.d_year_1 = 1997;
    q4_2_in.d_year_2 = 1998;
    strcpy(q4_2_in.c_region , "AMERICA");
    strcpy(q4_2_in.s_region , "AMERICA");
    strcpy(q4_2_in.p_mfgr_1 , "MFGR#1");
    strcpy(q4_2_in.p_mfgr_2 , "MFGR#2");
    
    printf("q4_2_in: %d %d %s %s %s %s\n",q4_2_in.d_year_1,q4_2_in.d_year_2,q4_2_in.c_region,q4_2_in.s_region,q4_2_in.p_mfgr_1,q4_2_in.p_mfgr_2);
    
    return q4_2_in;
}


/******************************************************************** 
 *
 *  Q4_3
 *
 ********************************************************************/

q4_3_input_t& q4_3_input_t::operator=(const q4_3_input_t& rhs)
{   
    d_year_1 = rhs.d_year_1;
    d_year_2 = rhs.d_year_2;
    strcpy(s_nation,rhs.s_nation);
    strcpy(p_category,rhs.p_category);
    
    return (*this);
}


q4_3_input_t create_q4_3_input(const double /*sf*/, const int /*specificWH*/)
{
    q4_3_input_t q4_3_in;
    
    q4_3_in.d_year_1 = 1997;
    q4_3_in.d_year_2 = 1998;
    strcpy(q4_3_in.s_nation , "UNITED STATES");
    strcpy(q4_3_in.p_category , "MFGR#14");
    
    printf("q4_3_in: %d %d %s %s\n",q4_3_in.d_year_1,q4_3_in.d_year_2,q4_3_in.s_nation,q4_3_in.p_category);
    
    return q4_3_in;
}

EXIT_NAMESPACE(ssb);
