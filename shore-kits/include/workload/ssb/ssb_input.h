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

/** @file ssb_input.h
 *
 *  @brief Declaration of the (common) inputs for the SSB trxs
 *  @brief Declaration of functions that generate the inputs for the SSB TRXs
 *
 *  @author Manos Athanassoulis
 */

#ifndef __SSB_INPUT_H
#define __SSB_INPUT_H

#include "workload/ssb/ssb_const.h"
#include "workload/ssb/ssb_struct.h"


ENTER_NAMESPACE(ssb);

// Inputs for the Queries

struct qcustomer_input_t { };
struct qsupplier_input_t { };
struct qpart_input_t { };
struct qdate_input_t { };
struct qlineorder_input_t { };
struct qtest_input_t { };
//struct q1_1_input_t { };
//struct q1_2_input_t { };
//struct q1_3_input_t { };
//struct q2_1_input_t { };
//struct q2_2_input_t { };
//struct q2_3_input_t { };
//struct q3_1_input_t { };
//struct q3_2_input_t { };
//struct q3_3_input_t { };
//struct q3_4_input_t { };
//struct q4_1_input_t { };
//struct q4_2_input_t { };
//struct q4_3_input_t { };

qcustomer_input_t  create_qcustomer_input (const double sf, const int specificWH);
qsupplier_input_t  create_qsupplier_input (const double sf, const int specificWH);
qdate_input_t      create_qdate_input     (const double sf, const int specificWH);
qpart_input_t      create_qpart_input     (const double sf, const int specificWH);
qlineorder_input_t create_qlineorder_input(const double sf, const int specificWH);
qtest_input_t create_qtest_input(const double sf, const int specificWH);
//q1_1_input_t create_q1_1_input(const double sf, const int specificWH);
//q1_2_input_t create_q1_2_input(const double sf, const int specificWH);
//q1_3_input_t create_q1_3_input(const double sf, const int specificWH);
//q2_1_input_t create_q2_1_input(const double sf, const int specificWH);
//q2_2_input_t create_q2_2_input(const double sf, const int specificWH);
//q2_3_input_t create_q2_3_input(const double sf, const int specificWH);
//q3_1_input_t create_q3_1_input(const double sf, const int specificWH);
//q3_2_input_t create_q3_2_input(const double sf, const int specificWH);
//q3_3_input_t create_q3_3_input(const double sf, const int specificWH);
//q3_4_input_t create_q3_4_input(const double sf, const int specificWH);
//q4_1_input_t create_q4_1_input(const double sf, const int specificWH);
//q4_2_input_t create_q4_2_input(const double sf, const int specificWH);
//q4_3_input_t create_q4_3_input(const double sf, const int specificWH); 



/******************************************************************** 
 *
 *  Q1-TPCH example
 *
 ********************************************************************/


/*
TPCH example
struct q1_input_t 
{
    time_t l_shipdate;
    q1_input_t& operator=(const q1_input_t& rhs);
};

q1_input_t    create_q1_input(const double sf, 
                              const int specificWH = 0);

*/

/******************************************************************** 
 *
 *  Q1_1-SSB example
 *
 ********************************************************************/

struct q1_1_input_t 
{
    int d_year;
    int lo_discount_lo;
    int lo_discount_hi;
    int lo_quantity;

    q1_1_input_t& operator=(const q1_1_input_t& rhs);
};

q1_1_input_t    create_q1_1_input(const double sf,
                                const int specificWH = 0);

/******************************************************************** 
 *
 *  Q1_2-SSB example
 *
 ********************************************************************/

struct q1_2_input_t 
{
    int d_yearmonthnum;
    int lo_discount_lo;
    int lo_discount_hi;
    int lo_quantity_lo;
    int lo_quantity_hi;

    q1_2_input_t& operator=(const q1_2_input_t& rhs);
};

q1_2_input_t    create_q1_2_input(const double sf,
                                const int specificWH = 0);

/******************************************************************** 
 *
 *  Q1_3-SSB example
 *
 ********************************************************************/

struct q1_3_input_t 
{
    int d_weeknuminyear;
    int d_year;
    int lo_discount_lo;
    int lo_discount_hi;
    int lo_quantity_lo;
    int lo_quantity_hi;

    q1_3_input_t& operator=(const q1_3_input_t& rhs);
};

q1_3_input_t    create_q1_3_input(const double sf,
                                const int specificWH = 0);

/******************************************************************** 
 *
 *  Q2_1-SSB example
 *
 ********************************************************************/

struct q2_1_input_t 
{
    char p_category[8];
    char s_region[13];

    q2_1_input_t& operator=(const q2_1_input_t& rhs);
};

q2_1_input_t    create_q2_1_input(const double sf,
                                const int specificWH = 0);

/******************************************************************** 
 *
 *  Q2_2-SSB example
 *
 ********************************************************************/

struct q2_2_input_t 
{
    char p_brand_1[10];
    char p_brand_2[10];
    char s_region[13];

    q2_2_input_t& operator=(const q2_2_input_t& rhs);
};

q2_2_input_t    create_q2_2_input(const double sf,
                                const int specificWH = 0);

/******************************************************************** 
 *
 *  Q2_3-SSB example
 *
 ********************************************************************/

struct q2_3_input_t 
{
    char p_brand[10];
    char s_region[13];

    q2_3_input_t& operator=(const q2_3_input_t& rhs);
};

q2_3_input_t    create_q2_3_input(const double sf,
                                const int specificWH = 0);

/******************************************************************** 
 *
 *  Q3_1-SSB example
 *
 ********************************************************************/

struct q3_1_input_t 
{
    int _year_lo;
    int _year_hi;
    char c_region[13];
    char s_region[13];

    q3_1_input_t& operator=(const q3_1_input_t& rhs);
};

q3_1_input_t    create_q3_1_input(const double sf, 
                              const int specificWH = 0);

/******************************************************************** 
 *
 *  Q3_2-SSB example
 *
 ********************************************************************/

struct q3_2_input_t 
{
    int _year_lo;
    int _year_hi;
    char _nation[16];

    q3_2_input_t& operator=(const q3_2_input_t& rhs);
};

q3_2_input_t    create_q3_2_input(const double sf, 
                              const int specificWH = 0);


/******************************************************************** 
 *
 *  Q3_3-SSB example
 *
 ********************************************************************/

struct q3_3_input_t 
{
    int _year_lo;
    int _year_hi;
    char c_city_1[11];
    char c_city_2[11];
    char s_city_1[11];
    char s_city_2[11];


    q3_3_input_t& operator=(const q3_3_input_t& rhs);
};

q3_3_input_t    create_q3_3_input(const double sf, 
                              const int specificWH = 0);

/******************************************************************** 
 *
 *  Q3_4-SSB example
 *
 ********************************************************************/

struct q3_4_input_t 
{
    char c_city_1[11];
    char c_city_2[11];
    char s_city_1[11];
    char s_city_2[11];
    char d_yearmonth[8];


    q3_4_input_t& operator=(const q3_4_input_t& rhs);
};

q3_4_input_t    create_q3_4_input(const double sf, 
                              const int specificWH = 0);

/******************************************************************** 
 *
 *  Q4_1-SSB example
 *
 ********************************************************************/

struct q4_1_input_t 
{
    char c_region[13];
    char s_region[13];
    char p_mfgr_1[7];
    char p_mfgr_2[7];
    

    q4_1_input_t& operator=(const q4_1_input_t& rhs);
};

q4_1_input_t    create_q4_1_input(const double sf, 
                              const int specificWH = 0);

/******************************************************************** 
 *
 *  Q4_2-SSB example
 *
 ********************************************************************/

struct q4_2_input_t 
{
    int d_year_1;
    int d_year_2;
    char c_region[13];
    char s_region[13];
    char p_mfgr_1[7];
    char p_mfgr_2[7];
    

    q4_2_input_t& operator=(const q4_2_input_t& rhs);
};

q4_2_input_t    create_q4_2_input(const double sf, 
                              const int specificWH = 0);

/******************************************************************** 
 *
 *  Q4_3-SSB example
 *
 ********************************************************************/

struct q4_3_input_t 
{
    int d_year_1;
    int d_year_2;
    char s_nation[16];
    char p_category[8];
    

    q4_3_input_t& operator=(const q4_3_input_t& rhs);
};

q4_3_input_t    create_q4_3_input(const double sf, 
                              const int specificWH = 0);


/******************************************************************** 
 *
 *  Q-NP
 *
 ********************************************************************/

struct qNP_input_t 
{
    int _custid;

    qNP_input_t& operator=(const qNP_input_t& rhs);
};

qNP_input_t    create_qNP_input(const double sf, 
                                const int specificWH = 0);




/******************************************************************** 
 *
 *  Inputs for the SSB Database Population
 *
 ********************************************************************/

struct populate_baseline_input_t 
{
    double _sf;
    int _loader_count;
    int _divisor;
    int _lineorder_per_thread;
    //    int _part_per_thread;
};


struct populate_some_lineorders_input_t
{
    long _orderid;
};

EXIT_NAMESPACE(ssb);

#endif

