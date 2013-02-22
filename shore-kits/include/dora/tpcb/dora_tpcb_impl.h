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

/** @file:   dora_tpcb_impl.h
 *
 *  @brief:  DORA TPCB TRXs
 *
 *  @note:   Definition of RVPs and Actions that synthesize (according to DORA)
 *           the TPCB trx
 *
 *  @author: Ippokratis Pandis
 *  @date:   July 2009
 */


#ifndef __DORA_TPCB_IMPL_H
#define __DORA_TPCB_IMPL_H


#include "dora.h"
#include "workload/tpcb/shore_tpcb_env.h"
#include "dora/tpcb/dora_tpcb.h"

using namespace shore;
using namespace tpcb;


ENTER_NAMESPACE(dora);



/******************************************************************** 
 *
 * DORA TPCB ACCT_UPDATE
 *
 ********************************************************************/

DECLARE_DORA_FINAL_RVP_CLASS(final_au_rvp,DoraTPCBEnv,4,4);

DECLARE_DORA_ACTION_NO_RVP_CLASS(upd_br_action,int,DoraTPCBEnv,acct_update_input_t,1);
DECLARE_DORA_ACTION_NO_RVP_CLASS(upd_te_action,int,DoraTPCBEnv,acct_update_input_t,1);
DECLARE_DORA_ACTION_NO_RVP_CLASS(upd_ac_action,int,DoraTPCBEnv,acct_update_input_t,1);
DECLARE_DORA_ACTION_NO_RVP_CLASS(ins_hi_action,int,DoraTPCBEnv,acct_update_input_t,1);

EXIT_NAMESPACE(dora);

#endif /** __DORA_TPCB_IMPL_H */

