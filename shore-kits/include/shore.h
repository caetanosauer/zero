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

/** @file:   shore.h
 *
 *  @brief:  Includes all the Shore-related files
 *
 *  @author: Ippokratis Pandis
 *
 */

#ifndef __SHORE_H
#define __SHORE_H

#include "sm/shore/common.h"
#include "sm/shore/shore_reqs.h"
#include "sm/shore/shore_worker.h"
#include "sm/shore/srmwqueue.h"

#include "sm/shore/shore_error.h"
#include "sm/shore/shore_tools.h"
#include "sm/shore/shore_file_desc.h"
#include "sm/shore/shore_field.h"
#include "sm/shore/shore_index.h"
#include "sm/shore/shore_iter.h"
#include "sm/shore/shore_msg.h"
#include "sm/shore/shore_row.h"
#include "sm/shore/shore_row_cache.h"
#include "sm/shore/shore_table.h"
#include "sm/shore/shore_asc_sort_buf.h"
#include "sm/shore/shore_desc_sort_buf.h"
#include "sm/shore/shore_helper_loader.h"
#include "sm/shore/shore_table_man.h"
#include "sm/shore/shore_env.h"

#include "sm/shore/shore_client.h"
#include "sm/shore/shore_trx_worker.h"
#include "sm/shore/shore_worker.h"
#include "sm/shore/shore_shell.h"

#endif /** __SHORE_H */
