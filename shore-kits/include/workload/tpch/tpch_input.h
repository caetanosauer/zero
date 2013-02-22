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

/** @file tpch_input.h
 *
 *  @brief Declaration of the (common) inputs for the TPC-H trxs
 *  @brief Declaration of functions that generate the inputs for the TPCH TRXs
 *
 *  @author: Nastaran Nikparto, Summer 2011
 *  @author Ippokratis Pandis (ipandis)
 */

#ifndef __TPCH_INPUT_H
#define __TPCH_INPUT_H

#include "workload/tpch/tpch_const.h"
#include "workload/tpch/tpch_struct.h"


ENTER_NAMESPACE(tpch);

// Inputs for the Queries

//Dummy input for scans
struct qlineitem_input_t
{
    int dummy;
    qlineitem_input_t& operator=(const qlineitem_input_t& rhs);
};

qlineitem_input_t    create_qlineitem_input(const double sf,
                              const int specificWH = 0);

struct qorders_input_t
{
    int dummy;
    qorders_input_t& operator=(const qorders_input_t& rhs);
};

qorders_input_t    create_qorders_input(const double sf,
                              const int specificWH = 0);

struct qnation_input_t
{
    int dummy;
    qnation_input_t& operator=(const qnation_input_t& rhs);
};

qnation_input_t    create_qnation_input(const double sf,
                              const int specificWH = 0);

struct qregion_input_t
{
    int dummy;
    qregion_input_t& operator=(const qregion_input_t& rhs);
};

qregion_input_t    create_qregion_input(const double sf,
                              const int specificWH = 0);

struct qsupplier_input_t
{
    int dummy;
    qsupplier_input_t& operator=(const qsupplier_input_t& rhs);
};

qsupplier_input_t    create_qsupplier_input(const double sf,
                              const int specificWH = 0);

struct qpart_input_t
{
    int dummy;
    qpart_input_t& operator=(const qpart_input_t& rhs);
};

qpart_input_t    create_qpart_input(const double sf,
                              const int specificWH = 0);

struct qpartsupp_input_t
{
    int dummy;
    qpartsupp_input_t& operator=(const qpartsupp_input_t& rhs);
};

qpartsupp_input_t    create_qpartsupp_input(const double sf,
                              const int specificWH = 0);

struct qcustomer_input_t
{
    int dummy;
    qcustomer_input_t& operator=(const qcustomer_input_t& rhs);
};

qcustomer_input_t    create_qcustomer_input(const double sf,
                              const int specificWH = 0);

/********************************************************************
 *
 *  Q1
 *
 ********************************************************************/

struct q1_input_t 
{
    time_t l_shipdate;
    q1_input_t& operator=(const q1_input_t& rhs);
};

q1_input_t    create_q1_input(const double sf, 
                              const int specificWH = 0);

/******************************************************************** 
 *
 *  Q2
 *
 ********************************************************************/

struct q2_input_t {
    int p_size;
    int p_types3;
    int r_name;
    
    q2_input_t& operator=(const q2_input_t& rhs);
};
q2_input_t create_q2_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q3
 *
 ********************************************************************/

struct q3_input_t {

    time_t current_date;
    int c_segment;
     q3_input_t& operator=(const q3_input_t& rhs);
};
q3_input_t create_q3_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q4
 *
 ********************************************************************/

struct q4_input_t 
{    
    time_t o_orderdate;
    q4_input_t& operator=(const q4_input_t& rhs);
};

q4_input_t    create_q4_input(const double sf, 
                              const int specificWH = 0);
/******************************************************************** 
 *
 *  Q5
 *
 ********************************************************************/

struct q5_input_t {
    int r_name;
    time_t o_orderdate;
    q5_input_t& operator=(const q5_input_t& rhs);
};
q5_input_t create_q5_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q6
 *
 ********************************************************************/

struct q6_input_t 
{
    time_t l_shipdate;//small value
    double l_quantity;
    double l_discount;

    q6_input_t& operator=(const q6_input_t& rhs);
};

q6_input_t    create_q6_input(const double sf, 
                              const int specificWH = 0);


/******************************************************************** 
 *
 *  Q7
 *
 ********************************************************************/

struct q7_input_t {
    int n_name1;
    int n_name2;
    q7_input_t& operator=(const q7_input_t& rhs);
};
q7_input_t create_q7_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q8
 *
 ********************************************************************/
struct q8_input_t {
    int n_name;
    int r_name;

    tpch_p_type p_type;
    
    q8_input_t& operator=(const q8_input_t& rhs);
};
q8_input_t create_q8_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q9
 *
 ********************************************************************/

struct q9_input_t {
    int p_name;

    q9_input_t& operator=(const q9_input_t& rhs);
};
q9_input_t create_q9_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q10
 *
 ********************************************************************/

struct q10_input_t {
    time_t o_orderdate;
    q10_input_t& operator=(const q10_input_t& rhs);
};
q10_input_t create_q10_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q11
 *
 ********************************************************************/

struct q11_input_t {
    int n_name;
    double fraction;
    
    q11_input_t& operator=(const q11_input_t& rhs);
};
q11_input_t create_q11_input(const double sf, const int specificWH);



/******************************************************************** 
 *
 *  Q12
 *
 ********************************************************************/

struct q12_input_t 
{
    int l_shipmode1; //should be tpch_l_shipmode?
    int l_shipmode2; //should be tpch_l_shipmode?
    time_t l_receiptdate; 

    q12_input_t& operator=(const q12_input_t& rhs);
};

q12_input_t    create_q12_input(const double sf, 
                                const int specificWH = 0);


/******************************************************************** 
 *
 *  Q13
 *
 ********************************************************************/

struct q13_input_t 
{
    char WORD1[15];
    char WORD2[15];

    q13_input_t& operator=(const q13_input_t& rhs);
};

q13_input_t    create_q13_input(const double sf, 
                                const int specificWH = 0);




/******************************************************************** 
 *
 *  Q14
 *
 ********************************************************************/

struct q14_input_t 
{
    time_t l_shipdate;

    q14_input_t& operator=(const q14_input_t& rhs);
};

q14_input_t    create_q14_input(const double sf, 
                                const int specificWH = 0);

/******************************************************************** 
 *
 *  Q15
 *
 ********************************************************************/
struct q15_input_t {

    time_t l_shipdate;
    q15_input_t& operator=(const q15_input_t& rhs);
};
q15_input_t create_q15_input(const double sf, const int specificWH);


/******************************************************************** 
 *
 *  Q16
 *
 ********************************************************************/
struct q16_input_t {
    int p_brand;
    int p_type;

    int p_size[8];
    q16_input_t& operator=(const q16_input_t& rhs);
};
q16_input_t create_q16_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q17
 *
 ********************************************************************/
struct q17_input_t {
    int p_brand;
    int p_container;
    q17_input_t& operator=(const q17_input_t& rhs);
};
q17_input_t create_q17_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q18
 *
 ********************************************************************/
struct q18_input_t {
    int l_quantity;
    q18_input_t& operator=(const q18_input_t& rhs);
};
q18_input_t create_q18_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q19
 *
 ********************************************************************/
struct q19_input_t {

    int l_quantity[3];
    int p_brand[3];
    
    q19_input_t& operator=(const q19_input_t& rhs);
};
q19_input_t create_q19_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q20
 *
 ********************************************************************/
struct q20_input_t {
    int n_name;
    int p_color;
    time_t l_shipdate;
    
    q20_input_t& operator=(const q20_input_t& rhs);
};
q20_input_t create_q20_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q21
 *
 ********************************************************************/
struct q21_input_t {
    int n_name;
    q21_input_t& operator=(const q21_input_t& rhs);
};
q21_input_t create_q21_input(const double sf, const int specificWH);

/******************************************************************** 
 *
 *  Q22
 *
 ********************************************************************/
struct q22_input_t {
    int cntrycode[7];

    q22_input_t& operator=(const q22_input_t& rhs);
};
q22_input_t create_q22_input(const double sf, const int specificWH);

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
 *  Inputs for the TPC-H Database Population
 *
 ********************************************************************/

struct populate_baseline_input_t 
{
    double _sf;
    int _loader_count;
    int _divisor;
    int _parts_per_thread;
    int _custs_per_thread;
};


struct populate_some_parts_input_t 
{
    int _partid;
};


struct populate_some_custs_input_t 
{
    int _custid;
};


EXIT_NAMESPACE(tpch);

#endif

