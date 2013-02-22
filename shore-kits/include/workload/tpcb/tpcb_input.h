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

/** @file tpcb_input.h
 *
 *  @brief Declaration of the (common) inputs for the TPC-C trxs
 *
 *  @author: Ryan Johnson, Feb 2009
 *  @author: Ippokratis Pandis, Feb 2009
 */

#ifndef __TPCB_INPUT_H
#define __TPCB_INPUT_H

#include "util.h"


ENTER_NAMESPACE(tpcb);



const int XCT_TPCB_ACCT_UPDATE = 31;
const int XCT_TPCB_POPULATE_DB = 39;

// microbenchmarks
const int XCT_TPCB_MBENCH_INSERT_ONLY = 41;
const int XCT_TPCB_MBENCH_DELETE_ONLY = 42;
const int XCT_TPCB_MBENCH_PROBE_ONLY = 43;
const int XCT_TPCB_MBENCH_INSERT_DELETE = 44;
const int XCT_TPCB_MBENCH_INSERT_PROBE = 45;
const int XCT_TPCB_MBENCH_DELETE_PROBE = 46;
const int XCT_TPCB_MBENCH_MIX = 47;


enum { TPCB_TELLERS_PER_BRANCH=10 };
enum { TPCB_ACCOUNTS_PER_BRANCH=100000 };
enum { TPCB_ACCOUNTS_CREATED_PER_POP_XCT=10000 }; // must evenly divide ACCOUNTS_PER_BRANCH


/** Exported variables */
// related to dynamic skew 
extern skewer_t b_skewer;
extern skewer_t t_skewer;
extern skewer_t a_skewer;
extern bool _change_load;


/** Exported data structures */


/*********************************************************************
 * 
 * @class abstract trx_input_t
 *
 * @brief Base class for the Input of any transaction
 *
 *********************************************************************/

struct acct_update_input_t 
{
    int b_id;
    int t_id;
    int a_id;
    double delta;

    acct_update_input_t() { }
};


struct populate_db_input_t 
{
    int _sf;
    int _first_a_id;

    populate_db_input_t(int sf, int a_id) : _sf(sf), _first_a_id(a_id) { }
};

// microbenchmarks
struct mbench_insert_only_input_t 
{
    int b_id;
    int a_id;
    double balance;

    mbench_insert_only_input_t() { }

    void print();
};

struct mbench_delete_only_input_t 
{
    int b_id;
    int a_id;
    double balance;

    mbench_delete_only_input_t() { }

    void print();
};

struct mbench_probe_only_input_t 
{
    int b_id;
    int a_id;
    double balance;

    mbench_probe_only_input_t() { }

    void print();
};

struct mbench_insert_delete_input_t 
{
    int b_id;
    int a_id;
    double balance;

    mbench_insert_delete_input_t() { }
};

struct mbench_insert_probe_input_t 
{
    int b_id;
    int a_id;
    double balance;

    mbench_insert_probe_input_t() { }
};

struct mbench_delete_probe_input_t 
{
    int b_id;
    int a_id;
    double balance;

    mbench_delete_probe_input_t() { }
};

struct mbench_mix_input_t 
{
    int b_id;
    int a_id;
    double balance;

    mbench_mix_input_t() { }
};


/////////////////////////////////////////////////////////////
//
// @brief: Declaration of functions that generate the inputs 
//         for the TPCB TRXs
//
/////////////////////////////////////////////////////////////


acct_update_input_t create_acct_update_input(int SF, 
                                             int specificBr = 0);


populate_db_input_t create_populate_db_input(int SF, 
                                             int specificBr = 0);


mbench_insert_only_input_t create_mbench_insert_only_input(int SF, 
							   int specificBr = 0);


mbench_delete_only_input_t create_mbench_delete_only_input(int SF, 
							   int specificBr = 0);


mbench_probe_only_input_t create_mbench_probe_only_input(int SF, 
							  int specificBr = 0);


mbench_insert_delete_input_t create_mbench_insert_delete_input(int SF, 
							       int specificBr = 0);


mbench_insert_probe_input_t create_mbench_insert_probe_input(int SF, 
							     int specificBr = 0);


mbench_delete_probe_input_t create_mbench_delete_probe_input(int SF, 
							     int specificBr = 0);


mbench_mix_input_t create_mbench_mix_input(int SF, 
					   int specificBr = 0);


EXIT_NAMESPACE(tpcb);


#endif

