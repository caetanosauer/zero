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

/** @file:   qcommon.h
 *
 *  @brief:  Common structures and macros used by QPipe workloads 
 *
 *  @author: Ippokratis Pandis, Apr 2010
 */

#ifndef __QPIPE_COMMON_H
#define __QPIPE_COMMON_H

#include "sm/shore/shore_env.h"

#include "qpipe/common/process_tuple.h"
#include "qpipe/common/process_query.h"
#include "qpipe/common/int_comparator.h"



#define DECLARE_QPIPE_TRX(trxlid)                                       \
    w_rc_t run_qpipe_##trxlid(Request* prequest, trxlid##_input_t& in); \
    w_rc_t run_qpipe_##trxlid(Request* prequest);                       \
    w_rc_t xct_qpipe_##trxlid(const int xct_id, trxlid##_input_t& in)


#define DEFINE_QPIPE_TRX(cname,trxlid)                                  \
    DEFINE_RUN_WITHOUT_INPUT_TRX_WRAPPER(cname,trxlid,qpipe_##trxlid);  \
    DEFINE_RUN_WITH_INPUT_TRX_WRAPPER(cname,trxlid,qpipe_##trxlid)

#endif
