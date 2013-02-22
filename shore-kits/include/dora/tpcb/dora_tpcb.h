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

/** @file:   dora_tpcb.h
 *
 *  @brief:  The DORA TPCB class
 *
 *  @author: Ippokratis Pandis, July 2009
 */


#ifndef __DORA_TPCB_H
#define __DORA_TPCB_H


#include <cstdio>

#include "tls.h"

#include "util.h"
#include "workload/tpcb/shore_tpcb_env.h"
#include "dora/dora_env.h"
#include "dora.h"

using namespace shore;
using namespace tpcb;


ENTER_NAMESPACE(dora);



// Forward declarations

// TPCB AcctUpod
class final_au_rvp;
class upd_br_action;
class upd_ac_action;
class upd_te_action;
class ins_hi_action;


/******************************************************************** 
 *
 * @class: dora_tpcb
 *
 * @brief: Container class for all the data partitions for the TPCB database
 *
 ********************************************************************/

class DoraTPCBEnv : public ShoreTPCBEnv, public DoraEnv
{
public:
    
    DoraTPCBEnv();
    virtual ~DoraTPCBEnv();

    //// Control Database

    // {Start/Stop/Resume/Pause} the system 
    int start();
    int stop();
    int resume();
    int pause();
    w_rc_t newrun();
    int set(envVarMap* /* vars */) { return(0); /* do nothing */ };
    int dump();
    int info() const;    
    int statistics();    
    int conf();


    //// Partition-related
    w_rc_t update_partitioning();


    //// DORA TPCB - PARTITIONED TABLES

    DECLARE_DORA_PARTS(br);  // Branch
    DECLARE_DORA_PARTS(te);  // Account
    DECLARE_DORA_PARTS(ac);  // Teller
    DECLARE_DORA_PARTS(hi);  // History


    //// DORA TPCB - TRXs   


    //////////////
    // AccntUpd //
    //////////////

    DECLARE_DORA_TRX(acct_update);

    DECLARE_DORA_FINAL_RVP_GEN_FUNC(final_au_rvp);

    DECLARE_DORA_ACTION_GEN_FUNC(upd_br_action,rvp_t,acct_update_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(upd_te_action,rvp_t,acct_update_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(upd_ac_action,rvp_t,acct_update_input_t);
    DECLARE_DORA_ACTION_GEN_FUNC(ins_hi_action,rvp_t,acct_update_input_t);
        
}; // EOF: DoraTPCBEnv


EXIT_NAMESPACE(dora);

#endif // __DORA_TPCB_H

