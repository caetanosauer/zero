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

#ifndef __SHORE_TM1_SCHEMA_H
#define __SHORE_TM1_SCHEMA_H


#include <math.h>

#include "sm_vas.h"
#include "util.h"

#include "workload/tm1/tm1_const.h"

#include "sm/shore/shore_table_man.h"

using namespace shore;


ENTER_NAMESPACE(tm1);


/* ------------------------------------------------- */
/* --- All the tables used in the TM1 benchmark  --- */
/* ---                                           --- */
/* --- Schema details at:                        --- */
/* --- src/workload/tm1/shore_tm1_schema.cpp     --- */
/* ------------------------------------------------- */


DECLARE_TABLE_SCHEMA_PD(subscriber_t);
DECLARE_TABLE_SCHEMA_PD(access_info_t);
DECLARE_TABLE_SCHEMA_PD(special_facility_t);
DECLARE_TABLE_SCHEMA_PD(call_forwarding_t);


EXIT_NAMESPACE(tm1);


#endif /* __SHORE_TM1_SCHEMA_H */
