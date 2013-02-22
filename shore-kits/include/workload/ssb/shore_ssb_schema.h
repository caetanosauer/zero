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

/** @file:   shore_ssb_schema.h
 *
 *  @brief:  Declaration of the SSB tables
 *
 *  @author: Manos Athanassoulis, June 2010
 *
 */

#ifndef __SHORE_SSB_SCHEMA_H
#define __SHORE_SSB_SCHEMA_H

#include <math.h>

#include "sm_vas.h"
#include "util.h"

#include "workload/ssb/ssb_const.h"
#include "sm/shore/shore_table_man.h"

using namespace shore;


ENTER_NAMESPACE(ssb);


/* -------------------------------------------------- */
/* --- All the tables used in the SSB benchmark --- */
/* ---                                            --- */
/* --- Schema details at:                         --- */
/* --- src/workload/ssb/shore_ssb_schema.cpp    --- */
/* -------------------------------------------------- */


DECLARE_TABLE_SCHEMA_PD(supplier_t);
DECLARE_TABLE_SCHEMA_PD(part_t);
DECLARE_TABLE_SCHEMA_PD(date_t);
DECLARE_TABLE_SCHEMA_PD(customer_t);
DECLARE_TABLE_SCHEMA_PD(lineorder_t);


EXIT_NAMESPACE(ssb);

#endif /* __SHORE_SSB_SCHEMA_H */
