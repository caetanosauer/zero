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

/** @file:   shore_tpcb_schema.h
 *
 *  @brief:  Declaration of the TPC-B tables
 *
 *  @author: Ryan Johnson      (ryanjohn)
 *  @author: Ippokratis Pandis (ipandis)
 *  @date:   Feb 2009
 */

#ifndef __SHORE_TPCB_SCHEMA_H
#define __SHORE_TPCB_SCHEMA_H


#include <math.h>

#include "sm_vas.h"
#include "util.h"

#include "sm/shore/shore_table_man.h"

using namespace shore;


ENTER_NAMESPACE(tpcb);


/* --------------------------------------------------- */
/* --- All the tables used in the TPC-B benchmark  --- */
/* ---                                             --- */
/* --- Schema details at:                          --- */
/* --- src/workload/tpcb/shore_tpcb_schema.cpp     --- */
/* --------------------------------------------------- */


DECLARE_TABLE_SCHEMA_PD(branch_t);
DECLARE_TABLE_SCHEMA_PD(teller_t);
DECLARE_TABLE_SCHEMA_PD(account_t);
DECLARE_TABLE_SCHEMA_PD(history_t);


EXIT_NAMESPACE(tpcb);

#endif /* __SHORE_TPCB_SCHEMA_H */
