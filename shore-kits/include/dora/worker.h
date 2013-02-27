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

/** @file:  worker.h
 *
 *  @brief: The DORA worker threads (specialization of the Base workers)
 *
 *  @author Ippokratis Pandis, Sept 2008
 */


#ifndef __DORA_WORKER_H
#define __DORA_WORKER_H

#include "sm/shore/shore_worker.h"

#include "dora/base_partition.h"
#include "dora/base_action.h"

using namespace shore;


ENTER_NAMESPACE(dora);



/******************************************************************** 
 *
 * @class: dora_worker_t
 *
 * @brief: The DORA worker thread
 * 
 ********************************************************************/

class dora_worker_t : public base_worker_t
{
private:
    
    base_partition_t*     _partition;

    // states
    int _work_ACTIVE_impl(); 

    int _pre_STOP_impl();

    // serves one action
    int _serve_action(base_action_t* paction);

public:

    dora_worker_t(ShoreEnv* env, base_partition_t* apart, c_str tname,
                  processorid_t aprsid = PBIND_NONE, const int use_sli = 0);

    ~dora_worker_t();


    // access methods

    // partition related
    void set_partition(base_partition_t* apart);
    base_partition_t* get_partition();
    int doRecovery();

}; // EOF: dora_worker_t


EXIT_NAMESPACE(dora);

#endif /** __DORA_WORKER_H */

