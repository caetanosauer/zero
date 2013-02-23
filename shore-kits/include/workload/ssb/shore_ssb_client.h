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

/** @file:   shore_ssb_client.h
 *
 *  @brief:  Defines the client for the SSB benchmark
 *
 *  @author: Manos Athanassoulis, June 2010
 */

#ifndef __SHORE_SSB_CLIENT_H
#define __SHORE_SSB_CLIENT_H

#include "sm/shore/shore_client.h"

#include "workload/ssb/ssb_const.h"
#include "workload/ssb/shore_ssb_env.h"


ENTER_NAMESPACE(ssb);

using namespace shore;


/******************************************************************** 
 *
 * @enum:  baseline_ssb_client_t
 *
 * @brief: The Baseline SSB kit smthread-based test client class
 *
 ********************************************************************/

class baseline_ssb_client_t : public base_client_t 
{
private:
    int _selid;
    trx_worker_t* _worker;
    double _qf;
    
public:

    baseline_ssb_client_t() { }     

    baseline_ssb_client_t(c_str tname, const int id, ShoreSSBEnv* env, 
                           const MeasurementType aType, const int trxid, 
                           const int numOfTrxs, 
                           processorid_t aprsid, const int selID, const double qf);

    ~baseline_ssb_client_t() { }

    // every client class should implement this function
    static int load_sup_xct(mapSupTrxs& map);

    // INTERFACE 

    w_rc_t submit_one(int xct_type, int xctid);    

}; // EOF: baseline_ssb_client_t


EXIT_NAMESPACE(ssb);

#endif /** __SHORE_SSB_CLIENT_H */
