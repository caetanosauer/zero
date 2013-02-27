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

/** @file tpch_input.cpp
 *
 *  @brief Implementation of the (common) inputs for the TPC-H trxs
 *  @brief Generate inputs for the TPCH TRXs
 *
 *  @author: Nastaran Nikparto, Summer 2011
 *  @author Ippokratis Pandis (ipandis)
 */


#include "util.h"
#include "workload/tpch/tpch_random.h" 
#include "workload/tpch/tpch_input.h"

#include "workload/tpch/dbgen/dss.h"
#include "workload/tpch/dbgen/dsstypes.h"
#include "workload/tpch/tpch_util.h"

using namespace dbgentpch; // for random MODES etc..

ENTER_NAMESPACE(tpch);

//Dummy input for scans
qlineitem_input_t& qlineitem_input_t::operator=(const qlineitem_input_t& rhs){dummy=rhs.dummy; return (*this);}
qlineitem_input_t    create_qlineitem_input(const double sf,
                              const int specificWH){qlineitem_input_t r;r.dummy=0;return r;}

qorders_input_t& qorders_input_t::operator=(const qorders_input_t& rhs){dummy=rhs.dummy; return (*this);}
qorders_input_t    create_qorders_input(const double sf,
                              const int specificWH){qorders_input_t r;r.dummy=0;return r;}

qnation_input_t& qnation_input_t::operator=(const qnation_input_t& rhs){dummy=rhs.dummy; return (*this);}
qnation_input_t    create_qnation_input(const double sf,
                              const int specificWH){qnation_input_t r;r.dummy=0;return r;}

qregion_input_t& qregion_input_t::operator=(const qregion_input_t& rhs){dummy=rhs.dummy; return (*this);}
qregion_input_t    create_qregion_input(const double sf,
                              const int specificWH ){qregion_input_t r;r.dummy=0;return r;}

qsupplier_input_t& qsupplier_input_t::operator=(const qsupplier_input_t& rhs){dummy=rhs.dummy; return (*this);}
qsupplier_input_t    create_qsupplier_input(const double sf,
                              const int specificWH){qsupplier_input_t r;r.dummy=0;return r;}

qpart_input_t& qpart_input_t::operator=(const qpart_input_t& rhs){dummy=rhs.dummy; return (*this);}
qpart_input_t    create_qpart_input(const double sf,
                              const int specificWH){qpart_input_t r;r.dummy=0;return r;}

qpartsupp_input_t& qpartsupp_input_t::operator=(const qpartsupp_input_t& rhs){dummy=rhs.dummy; return (*this);}
qpartsupp_input_t    create_qpartsupp_input(const double sf,
                              const int specificWH){qpartsupp_input_t r;r.dummy=0;return r;}

qcustomer_input_t& qcustomer_input_t::operator=(const qcustomer_input_t& rhs){dummy=rhs.dummy; return (*this);}
qcustomer_input_t    create_qcustomer_input(const double sf,
                              const int specificWH){qcustomer_input_t r;r.dummy=0;return r;}


/******************************************************************** 
 *
 *  Q1
 *
 ********************************************************************/


q1_input_t& q1_input_t::operator=(const q1_input_t& rhs)
{
    l_shipdate = rhs.l_shipdate;
    return (*this);
};


q1_input_t  create_q1_input(const double /* sf */, 
                            const int /* specificWH */)
{
    q1_input_t q1_input;

    struct tm shipdate;

    shipdate.tm_year = 98;//since 1900
    shipdate.tm_mon = 11;//month starts from 0
    shipdate.tm_mday = 1-URand(60, 120);//day starts from 1
    shipdate.tm_hour = 0;
    shipdate.tm_min = 0;
    shipdate.tm_sec = 0;

    // The format: YYYY-MM-DD
    
    q1_input.l_shipdate = mktime(&shipdate);

    return (q1_input);
};

/******************************************************************** 
 *
 *  Q2
 *
 ********************************************************************/
q2_input_t& q2_input_t::operator=(const q2_input_t& rhs){

    p_size = rhs.p_size;
    p_types3 = rhs.p_types3;
    r_name = rhs.r_name;

    return (*this);
};

q2_input_t create_q2_input(const double /* sf */, 
                            const int /* specificWH */){
    q2_input_t q2_input;

    q2_input.p_size = URand(1,50);
    q2_input.r_name = URand(0,END_R_NAME-1);
    q2_input.p_types3 = URand(0,END_TYPE_S3-1);
    
    /*    q2_input.p_size = 15;
    q2_input.r_name = 3;
    q2_input.p_types3 = 2; //BRASS*/
    
    return (q2_input);
};

/******************************************************************** 
 *
 *  Q3
 *
 ********************************************************************/

    
q3_input_t& q3_input_t::operator=(const q3_input_t& rhs){

    current_date = rhs.current_date;
    c_segment = rhs.c_segment;

    return (*this);
};    


q3_input_t create_q3_input(const double sf, const int specificWH){

    q3_input_t q3in;

    q3in.c_segment = URand(0, END_SEGMENT-1);

    struct tm date;
    date.tm_year = 95;
    date.tm_mon = 2;
    date.tm_mday = URand(1,31);

    date.tm_sec = 0;
    date.tm_min = 0;
    date.tm_hour = 0;
   
    q3in.current_date =  mktime(&date);

    
   /*     q3in.c_segment = HOUSEHOLD;

    struct tm date;
    date.tm_year = 95;
    date.tm_mon = 2;
    date.tm_mday = 4;

    date.tm_sec = 0;
    date.tm_min = 0;
    date.tm_hour = 0;
   
    q3in.current_date =  mktime(&date);*/

    
    return q3in;

}


/******************************************************************** 
 *
 *  Q4
 *
 ********************************************************************/


q4_input_t& q4_input_t::operator=(const q4_input_t& rhs)
{
    o_orderdate = rhs.o_orderdate;
    return (*this);
};


q4_input_t  create_q4_input(const double /* sf */, 
                            const int /* specificWH */)
{
    q4_input_t q4_input;

    struct tm orderdate;

    int month=URand(0,57);
    int year=month/12;
    month=month%12;

    orderdate.tm_year = 93+year;
    orderdate.tm_mon = month;
    orderdate.tm_mday = 1;
    orderdate.tm_hour = 0;
    orderdate.tm_min = 0;
    orderdate.tm_sec = 0;

    // cout<<orderdate.tm_year<<":"<<orderdate.tm_mon<<endl;
    

    /*orderdate.tm_year = 93;
    orderdate.tm_mon = 6;
    orderdate.tm_mday = 1;
    orderdate.tm_hour = 4; //problem with mktime and gmtime
    orderdate.tm_min = 0;
    orderdate.tm_sec = 0;*/

    // The format: YYYY-MM-DD
  
    q4_input.o_orderdate = mktime(&orderdate);

    return (q4_input);
};

/******************************************************************** 
 *
 *  Q5
 *
 ********************************************************************/

q5_input_t& q5_input_t::operator=(const q5_input_t& rhs){

    r_name = rhs.r_name;
    o_orderdate = rhs.o_orderdate;
    
    return (*this);
};


q5_input_t create_q5_input(const double /* sf */, const int /* specificWH */) {

    q5_input_t q5in;

    q5in.r_name = URand(0, END_R_NAME-1);

    struct tm orderdate;

    orderdate.tm_year = URand(93, 97);//year starts from 1900
    orderdate.tm_mon = 0;//month starts from 0
    orderdate.tm_mday = 1;
    orderdate.tm_hour = 0;
    orderdate.tm_min = 0;
    orderdate.tm_sec = 0;
    
    q5in.o_orderdate = mktime(&orderdate);

    /*q5in.r_name = 2;

     struct tm orderdate;

    orderdate.tm_year = 94;//year starts from 1900
    orderdate.tm_mon = 0;//month starts from 0
    orderdate.tm_mday = 1;
    orderdate.tm_hour = 0;
    orderdate.tm_min = 0;
    orderdate.tm_sec = 0;*/
    
    q5in.o_orderdate = mktime(&orderdate);

    	
    return (q5in);
};

/******************************************************************** 
 *
 *  Q6
 *
 ********************************************************************/

q6_input_t& q6_input_t::operator=(const q6_input_t& rhs){

    l_shipdate = rhs.l_shipdate;//small value
    l_quantity = rhs.l_quantity;
    l_discount = rhs.l_discount;
    
    return (*this);
};


q6_input_t create_q6_input(const double /* sf */, const int /* specificWH */) {

    q6_input_t q6in;

    struct tm shipdate;

    shipdate.tm_year = URand(93, 97);//year starts from 1900
    shipdate.tm_mon = 0;//month starts from 0
    shipdate.tm_mday = 1;
    shipdate.tm_hour = 0;
    shipdate.tm_min = 0;
    shipdate.tm_sec = 0;
    
    q6in.l_shipdate = mktime(&shipdate);
    
//randomly selected within [0.02 .. 0.09];
    q6in.l_discount = (rand() / RAND_MAX) * 0.07 + 0.02;

    q6in.l_quantity = URand(24,25);
    
    
    return (q6in);
};

/******************************************************************** 
 *
 *  Q7
 *
 ********************************************************************/

q7_input_t& q7_input_t::operator=(const q7_input_t& rhs){

    n_name1 = rhs.n_name1;
    n_name2 = rhs.n_name2;
    
    return (*this);
};


q7_input_t create_q7_input(const double /* sf */, const int /* specificWH */) {

    q7_input_t q7in;

    q7in.n_name1 = URand(0, END_N_NAME-1);
    q7in.n_name2 = URand(0, END_N_NAME-1);
    
    while(q7in.n_name1 == q7in.n_name2)
    q7in.n_name2 = URand(0, END_N_NAME-1);

    /*q7in.n_name1 = 6;
      q7in.n_name2 = 7;*/
    
    return q7in;
}

/********************************************************************* 
 *
 *  Q8
 *
 ********************************************************************/

q8_input_t& q8_input_t::operator=(const q8_input_t& rhs){

    n_name = rhs.n_name;
    r_name = rhs.n_name;

    p_type = rhs.p_type;

    return (*this);
}

q8_input_t create_q8_input(const double /* sf */, const int /* specificWH */) {

    q8_input_t q8in;



    q8in.r_name = URand(0, END_R_NAME-1);
    bool nation_ok = false;
    while(!nation_ok) {
    	q8in.n_name = URand(0, END_N_NAME-1);
    	switch(q8in.r_name) {
    	case AFRICA:
    		nation_ok = (q8in.n_name == ALGERIA || q8in.n_name == ETHIOPIA || q8in.n_name == KENYA || q8in.n_name == MOROCCO || q8in.n_name == MOZAMBIQUE);
    		break;
    	case AMERICA:
    		nation_ok = (q8in.n_name == ARGENTINA || q8in.n_name == BRAZIL || q8in.n_name == CANADA || q8in.n_name == PERU || q8in.n_name == UNITED_STATES);
    		break;
    	case ASIA:
    		nation_ok = (q8in.n_name == INDIA || q8in.n_name == INDONESIA || q8in.n_name == JAPAN || q8in.n_name == CHINA || q8in.n_name == VIETNAM);
    		break;
    	case EUROPE:
    		nation_ok = (q8in.n_name == FRANCE || q8in.n_name == GERMANY || q8in.n_name == ROMANIA || q8in.n_name == RUSSIA || q8in.n_name == UNITED_KINGDOM);
    		break;
    	case MIDDLE_EAST:
    		nation_ok = (q8in.n_name == EGYPT || q8in.n_name == IRAN || q8in.n_name == IRAQ || q8in.n_name == JORDAN || q8in.n_name == SAUDI_ARABIA);
    		break;
    	}
    }

    q8in.p_type.s1 = URand(0, END_TYPE_S1-1);
    q8in.p_type.s2 = URand(0, END_TYPE_S2-1);
    q8in.p_type.s3 = URand(0, END_TYPE_S3-1);

    /*q8in.n_name = 2;

    q8in.p_type.s1 = 4;
    q8in.p_type.s2 = 0;
    q8in.p_type.s3 = 3;*/
    
    
    return q8in;
}


/********************************************************************* 
 *
 *  Q9
 *
 ********************************************************************/

q9_input_t& q9_input_t::operator=(const q9_input_t& rhs){

    p_name = rhs.p_name;
    return (*this);
}

q9_input_t create_q9_input(const double /* sf */, const int /* specificWH */) {

    q9_input_t q9in;

    q9in.p_name = URand(0, END_P_NAME-1);
    // q9in.p_name = 33;
    
    return q9in; }

/********************************************************************* 
 *
 *  Q10
 *
 ********************************************************************/

q10_input_t& q10_input_t::operator=(const q10_input_t& rhs){

    o_orderdate = rhs.o_orderdate;

    return (*this);
}

q10_input_t create_q10_input(const double /* sf */, const int /* specificWH */) {

    q10_input_t q10in;

    struct tm date1;
    date1.tm_year = 93; date1.tm_mon = 1; date1.tm_mday = 1;
    date1.tm_sec = 0; date1.tm_min = 0; date1.tm_hour = 0;
    
    struct tm date2;
    date2.tm_year = 95; date2.tm_mon = 0; date2.tm_mday = 31;
    date2.tm_sec = 59; date2.tm_min = 59; date2.tm_hour = 23;

    time_t t1 = mktime(&date1);
    time_t t2 = mktime(&date2);

    q10in.o_orderdate =  URand(t1,t2);

    /*struct tm date;
    date.tm_year = 93; date.tm_mon = 9; date.tm_mday = 1;
    date.tm_sec = 59; date.tm_min = 59; date.tm_hour = 23;

    q10in.o_orderdate = mktime(&date);*/
    
    return q10in;
}

/********************************************************************* 
 *
 *  Q11
 *
 ********************************************************************/
q11_input_t& q11_input_t::operator=(const q11_input_t& rhs){

    n_name = rhs.n_name;
    fraction = rhs.fraction;
    
    return (*this);
}

q11_input_t create_q11_input(const double /* sf */, const int /* specificWH */) {
    q11_input_t q11in;

      q11in.n_name = URand(0, END_N_NAME-1);
      q11in.fraction = 0.0001; // how to retrive SF???

    /*q11in.n_name = 7;
      q11in.fraction = 0.0001;*/
    
    return q11in;
}

/******************************************************************** 
 *
 *  Q12
 *
 *  l_shipmode1:   Random within the list of MODES defined in 
 *                 Clause 5.2.2.13 (tpc-h-2.8.0 pp.91)
 *  l_shipmode2:   Random within the list of MODES defined in
 *                 Clause 5.2.2.13 (tpc-h-2.8.0 pp.91)
 *                 and different from l_shipmode1
 *  l_receiptdate: First of January of a random year within [1993 .. 1997]
 *
 ********************************************************************/

q12_input_t& q12_input_t::operator=(const q12_input_t& rhs)
{    
    l_shipmode1=rhs.l_shipmode1;
    l_shipmode2=rhs.l_shipmode2;
    l_receiptdate = rhs.l_receiptdate;
    return (*this);
};


q12_input_t    create_q12_input(const double /* sf */, 
                                const int /* specificWH */)
{
    q12_input_t q12_input;

    //pick_str(&l_smode_set, L_SMODE_SD, q12_input.l_shipmode1);
    //pick_str(&l_smode_set, L_SMODE_SD, q12_input.l_shipmode2);
    q12_input.l_shipmode1=URand(0,END_SHIPMODE-1);
    q12_input.l_shipmode2=(q12_input.l_shipmode1+URand(0,END_SHIPMODE-2)+1)%END_SHIPMODE;

    struct tm receiptdate;

    // Random year [1993 .. 1997]
    receiptdate.tm_year = URand(93, 97);

    // First of January
    receiptdate.tm_mon = 0;
    receiptdate.tm_mday = 1;
    receiptdate.tm_hour = 0;
    receiptdate.tm_min = 0;
    receiptdate.tm_sec = 0;

    q12_input.l_receiptdate = mktime(&receiptdate);
	
    return (q12_input);
};



/******************************************************************** 
 *
 *  Q13
 *
 *  o_comment: WORD1 randomly selected from: special, pending, unusual, express
 *             WORD2 randomly selected from: packages, requests, accounts, deposits
 *
 ********************************************************************/


q13_input_t& q13_input_t::operator=(const q13_input_t& rhs)
{
    strcpy(WORD1,rhs.WORD1);
    strcpy(WORD2,rhs.WORD2);
    return (*this);
};


q13_input_t    create_q13_input(const double /* sf */, 
                                const int /* specificWH */)
{
    q13_input_t q13_input;

    int num_first_entries, num_second_entries;
    //WORD1 randomly selected from: special, pending, unusual, express
    //WORD2 randomly selected from: packages, requests, accounts, deposits
    static char const* FIRST[] = {"special", "pending", "unusual", "express"};
    static char const* SECOND[] = {"packages", "requests", "accounts", "deposits"};

    num_first_entries  = sizeof(FIRST)/sizeof(FIRST[0]);
    num_second_entries = sizeof(SECOND)/sizeof(SECOND[0]);

    strcpy(q13_input.WORD1,FIRST[URand(1,num_first_entries)-1]);
    strcpy(q13_input.WORD2,SECOND[URand(1,num_second_entries)-1]);

    return (q13_input);
};


/******************************************************************** 
 *
 *  Q14
 *
 *  l_shipdate: The first day of a month randomly selected from a
 *              random year within [1993 .. 1997]
 *
 ********************************************************************/


q14_input_t& q14_input_t::operator=(const q14_input_t& rhs)
{
    l_shipdate = rhs.l_shipdate;
    return (*this);
};


q14_input_t    create_q14_input(const double /* sf */, 
                                const int /* specificWH */)
{
    q14_input_t q14_input;

    struct tm shipdate;

    // Random year within [1993 .. 1997]
    shipdate.tm_year = URand(93, 97);

    // Random month ([1 .. 12])
    shipdate.tm_mon = URand(0,11);

    // First day
    shipdate.tm_mday = 1;
    shipdate.tm_hour = 0;
    shipdate.tm_min = 0;
    shipdate.tm_sec = 0;

    q14_input.l_shipdate = mktime(&shipdate);
	
    return (q14_input);
};

/******************************************************************** 
 *
 *  Q15
 *
 *  starting_date:The first day of a month randomly selected from a
 *              random year within [1993 .. 1997]
 *
 ********************************************************************/


q15_input_t& q15_input_t::operator=(const q15_input_t& rhs)
{
    l_shipdate = rhs.l_shipdate;
    return (*this);
};


q15_input_t    create_q15_input(const double /* sf */, 
                                const int /* specificWH */)
{
    q15_input_t q15_input;

    struct tm shipdate;

    // Random year within [1993 .. 1997]
    shipdate.tm_year = URand(93, 97);

    // Random month ([1 .. 12])
    shipdate.tm_mon = URand(0,11);

    // First day
    shipdate.tm_mday = 1;
    shipdate.tm_hour = 0;
    shipdate.tm_min = 0;
    shipdate.tm_sec = 0;

    q15_input.l_shipdate = mktime(&shipdate);
	
    return (q15_input);
};

/******************************************************************** 
 *
 *  Q16
 *
 *  
 *
 ********************************************************************/

q16_input_t& q16_input_t::operator=(const q16_input_t& rhs){

    p_brand = rhs.p_brand;
    p_type = rhs.p_type;

    for(int i=0; i<8; i++)
	p_size[i] = rhs.p_size[i];

    return (*this);
};

q16_input_t create_q16_input(const double /* sf */, const int /* specificWH */) {

    q16_input_t q16_input;

    q16_input.p_brand = URand(1,5)*10 + URand(1,5);
    q16_input.p_type = URand(0,5)*10 + URand(0,4);

	bool contains;
	int j;

    for(int i=0; i<8; i++) {
	do {
	q16_input.p_size[i] = URand(1,50);
	contains = false;
	for(j = 0; j < i; j++) {
		if(q16_input.p_size[i] == q16_input.p_size[j]) contains = true;
	}
	} while(contains);
	}

    return q16_input;

};

/******************************************************************** 
 *
 *  Q17
 *
 *  
 *
 ********************************************************************/

q17_input_t& q17_input_t::operator=(const q17_input_t& rhs){

    p_brand = rhs.p_brand;
    p_container = rhs.p_container;

    return (*this);
};

q17_input_t create_q17_input(const double /* sf */, const int /* specificWH */) {

    q17_input_t q17_input;
    
      q17_input.p_brand = URand(1,5)*10 + URand(1,5);
      q17_input.p_container = URand(0,4)*10 +URand(0,7);

    //q17_input.p_brand = 23;
    //q17_input.p_container = 21;
    
    return q17_input;
}

/******************************************************************** 
 *
 *  Q18
 *
 *  
 *
 ********************************************************************/

q18_input_t& q18_input_t::operator=(const q18_input_t& rhs){
    l_quantity = rhs.l_quantity;
    return (*this);
}

q18_input_t create_q18_input(const double /* sf */, const int /* specificWH */) {

    q18_input_t q18input;

    q18input.l_quantity = URand(312,315);

    return q18input;
}

/******************************************************************** 
 *
 *  Q19
 *
 *  
 *
 ********************************************************************/

q19_input_t& q19_input_t::operator=(const q19_input_t& rhs){

    for(int i=0; i<3; i++){
	
	l_quantity[i] = rhs.l_quantity[i];
	p_brand[i] = rhs.p_brand[i];
    }

    return (*this);
}

q19_input_t create_q19_input(const double /* sf */, const int /* specificWH */) {

    q19_input_t q19in;

    q19in.l_quantity[0] = URand(1,10);
    q19in.l_quantity[1] = URand(10,20);
    q19in.l_quantity[2] = URand(20,30);

    for(int i = 0; i < 3; i++){
	q19in.p_brand[i] = URand(0,4)*10+ URand(0,4);
    }
    
    return q19in;
}

/******************************************************************** 
 *
 *  Q20
 *
 *  
 *
 ********************************************************************/

q20_input_t& q20_input_t::operator=(const q20_input_t& rhs){

    n_name = rhs.n_name; 
    p_color = rhs. p_color;
    l_shipdate = rhs.l_shipdate;

    return (*this);
}

q20_input_t create_q20_input(const double /* sf */, const int /* specificWH */) {

    q20_input_t q20in;

    q20in.n_name = URand(0, END_N_NAME);
    q20in.p_color = URand(0, END_P_NAME);

    struct tm t;
    t.tm_sec = 0;   
    t.tm_min = 0;   
    t.tm_hour = 0;  
    t.tm_mday = 1;  
    t.tm_mon = 0;   
    t.tm_year = URand(93,97);  
    t.tm_wday = 0;  
    t.tm_yday = 0;  

    q20in.l_shipdate = mktime(&t);
    
    return q20in;
}

/******************************************************************** 
 *
 *  Q21
 *  n_name : a randomly selected int within [0,END_N_NAME]
 *
 *  
 *
 ********************************************************************/
q21_input_t& q21_input_t::operator=(const q21_input_t& rhs){

    n_name = rhs.n_name;

    return (*this);
}

q21_input_t create_q21_input(const double sf, const int specificWH){

    q21_input_t q21in;

    q21in.n_name = URand(0, END_N_NAME-1);
    
    return q21in;

}

/******************************************************************** 
 *
 *  Q22
 *
 *  cnrtycode[7] = randomly selected without repetition from the possible values for Country  *   code
 *
 *
 ********************************************************************/
q22_input_t create_q22_input(const double /* sf */, const int /* specificWH */) {

    q22_input_t q22in;

	bool contains;
	int j;

    for(int i=0; i<7; i++) {
	do {
	q22in.cntrycode[i] = URand(10, 34);
	contains = false;
	for(j = 0; j < i; j++) {
		if(q22in.cntrycode[i] == q22in.cntrycode[j]) contains = true;
	}
	} while(contains);
	}


    return q22in;
}

q22_input_t& q22_input_t::operator=(const q22_input_t& rhs){

    for(int i = 0; i < 7; i++)
	cntrycode[i] = rhs.cntrycode[i];

    return (*this);
}

EXIT_NAMESPACE(tpch);
