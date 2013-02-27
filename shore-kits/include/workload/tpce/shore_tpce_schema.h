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

/** @file:   shore_tpce_schema.h
 *
 *  @brief:  Declaration of the TPC-E tables
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#ifndef __SHORE_TPCE_SCHEMA_H
#define __SHORE_TPCE_SCHEMA_H

#include <math.h>

#include "sm_vas.h"
#include "util.h"

#include "sm/shore/shore_table_man.h"
#include "workload/tpce/tpce_const.h"

using namespace shore;


ENTER_NAMESPACE(tpce);



/*********************************************************************
 *
 * TPC-E SCHEMA
 *
 *********************************************************************/

/* -------------------------------------------------- */
/* --- All the tables used in the TPC-E benchmark --- */
/* ---                                            --- */
/* --- Schema details at:                         --- */
/* --- src/workload/tpce/shore_tpce_schema.cpp    --- */
/* -------------------------------------------------- */

// Fixed tables
DECLARE_TABLE_SCHEMA_PD(sector_t);

DECLARE_TABLE_SCHEMA_PD(charge_t);
DECLARE_TABLE_SCHEMA_PD(commission_rate_t);
DECLARE_TABLE_SCHEMA_PD(exchange_t);
DECLARE_TABLE_SCHEMA_PD(industry_t);
DECLARE_TABLE_SCHEMA_PD(status_type_t);
DECLARE_TABLE_SCHEMA_PD(taxrate_t);
DECLARE_TABLE_SCHEMA_PD(trade_type_t);
DECLARE_TABLE_SCHEMA_PD(zip_code_t);

// Growing tables
DECLARE_TABLE_SCHEMA_PD(cash_transaction_t);
DECLARE_TABLE_SCHEMA_PD(settlement_t);
DECLARE_TABLE_SCHEMA_PD(trade_t);
DECLARE_TABLE_SCHEMA_PD(trade_history_t);
DECLARE_TABLE_SCHEMA_PD(trade_request_t);

// Scaling tables
DECLARE_TABLE_SCHEMA_PD(account_permission_t);
DECLARE_TABLE_SCHEMA_PD(broker_t);
DECLARE_TABLE_SCHEMA_PD(company_t);
DECLARE_TABLE_SCHEMA_PD(customer_t);
DECLARE_TABLE_SCHEMA_PD(company_competitor_t);
DECLARE_TABLE_SCHEMA_PD(security_t);
DECLARE_TABLE_SCHEMA_PD(customer_account_t);
DECLARE_TABLE_SCHEMA_PD(daily_market_t);
DECLARE_TABLE_SCHEMA_PD(customer_taxrate_t);
DECLARE_TABLE_SCHEMA_PD(holding_t);
DECLARE_TABLE_SCHEMA_PD(financial_t);
DECLARE_TABLE_SCHEMA_PD(holding_history_t);
DECLARE_TABLE_SCHEMA_PD(address_t);
DECLARE_TABLE_SCHEMA_PD(holding_summary_t);
DECLARE_TABLE_SCHEMA_PD(last_trade_t);
DECLARE_TABLE_SCHEMA_PD(watch_item_t);
DECLARE_TABLE_SCHEMA_PD(news_item_t);
DECLARE_TABLE_SCHEMA_PD(watch_list_t);
DECLARE_TABLE_SCHEMA_PD(news_xref_t);

// Unknown
DECLARE_TABLE_SCHEMA_PD(dimension_t);

EXIT_NAMESPACE(tpce);

#endif /* __SHORE_TPCE_SCHEMA_H */
