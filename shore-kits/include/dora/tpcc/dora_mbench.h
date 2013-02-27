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

/** @file:   dora_mbench.h
 *
 *  @brief:  DORA MBENCHES
 *
 *  @note:   Definition of RVPs and Actions that synthesize 
 *           the mbenches according to DORA
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */


#ifndef __DORA_MBENCHES_H
#define __DORA_MBENCHES_H


#include "dora.h"
#include "workload/tpcc/shore_tpcc_env.h"
#include "dora/tpcc/dora_tpcc.h"

using namespace shore;
using namespace tpcc;


ENTER_NAMESPACE(dora);




/******************************************************************** 
 *
 * DORA TPCC MBENCHES
 *
 ********************************************************************/

DECLARE_DORA_FINAL_RVP_CLASS(final_mb_rvp,DoraTPCCEnv,1,1);

DECLARE_DORA_ACTION_NO_RVP_CLASS(upd_wh_mb_action,int,DoraTPCCEnv,mbench_wh_input_t,1);

DECLARE_DORA_ACTION_NO_RVP_CLASS(upd_cust_mb_action,int,DoraTPCCEnv,mbench_cust_input_t,1);


EXIT_NAMESPACE(dora);

#endif /** __DORA_MBENCHES_H */

