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

/** @file:  shore_trx_worker.h
 *
 *  @brief: Wrapper for the worker threads in Baseline 
 *          (specialization of the Shore workers)
 *
 *  @author Ippokratis Pandis, Nov 2008
 */


#ifndef __SHORE_TRX_WORKER_H
#define __SHORE_TRX_WORKER_H


#include "sm/shore/srmwqueue.h"
#include "sm/shore/shore_reqs.h"
#include "sm/shore/shore_worker.h"


ENTER_NAMESPACE(shore);


/******************************************************************** 
 *
 * @class: trx_worker_t
 *
 * @brief: The baseline system worker threads
 *
 ********************************************************************/

const int REQUESTS_PER_WORKER_POOL_SZ = 60;

class trx_worker_t : public base_worker_t
{
public:
    typedef trx_request_t      Request;
    typedef srmwqueue<Request> Queue;

private:

    guard<Queue>         _pqueue;
    guard<Pool>          _actionpool;

    // states
    int _work_ACTIVE_impl(); 

    int _pre_STOP_impl();

    // serves one action
    int _serve_action(Request* prequest);

public:

    trx_worker_t(ShoreEnv* env, c_str tname, 
                 processorid_t aprsid = PBIND_NONE,
                 const int use_sli = 0);
    ~trx_worker_t();

    // Enqueues a request to the queue of the worker thread
    inline void enqueue(Request* arequest, const bool bWake=true) {
        _pqueue->push(arequest,bWake);
    }
        
    void init(const int lc);        

}; // EOF: trx_worker_t

EXIT_NAMESPACE(shore);

#endif /** __SHORE_TRX_WORKER_H */

