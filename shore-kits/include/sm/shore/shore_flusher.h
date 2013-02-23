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

/** @file:   shore_flusher.h
 *
 *  @brief:  Specialization of a worker thread that acts as the mainstream-flusher.
 *
 *  @author: Ippokratis Pandis, Feb 2010
 */


/**
   Log flushing is a major source of context switches, as well as, of an 
   unexpected but definitely non-scalable problem with the notification of the 
   sleepers on cond vars, which makes almost impossible to saturate workloads 
   like TM1-UpdLocation with very high commit rate that put pressure on the log.

   In order to alleviate the problem with the high rate of context switches and 
   the overuse of condition variables, we implement a staged group
   commit mechanism. The thread that executes the final-rvp of the transaction 
   calls a lazy commit and does not context switch until the corresponding flush 
   completes. Instead it transfers the control to another specialized worker 
   thread, called the flusher. 

   The flusher, monitors the statistics (how many transactions are in the 
   group unflushed, what is the size of the unflushed log, how much time it has 
   passed since the last flush) and makes a decision on when to flush. Typically, 
   if up to T msecs or N bytes or K xcts are unflushed a call to flush is 
   triggered.  

   worker:
   {  ...
      commit_xct(lazy);
      flusher->enqueue_toflush(pxct); } 

   
   flusher:
   while (true) {
      if (time_to_flush()) {
         flush_all();
         while (pxct = toflush_queue->get_one()) {
            pxct->notify_final_rvp(); 
         }
      }
   }

   In order to enable this mechanism Shore-kits needs to be configured with:
   --enable-dflusher
*/

#ifndef __SHORE_FLUSHER_H
#define __SHORE_FLUSHER_H


#include "sm/shore/shore_trx_worker.h"


ENTER_NAMESPACE(shore);


/******************************************************************** 
 *
 * @struct: flusher_stats_t
 *
 * @brief:  Various statistics for the flusher
 * 
 ********************************************************************/

struct flusher_stats_t 
{
    uint   served;
    uint   flushes;

    long long logsize;
    uint   alreadyFlushed;
    uint   waiting;

    uint trigByXcts;
    uint trigBySize;
    uint trigByTimeout;
    
    flusher_stats_t();
    ~flusher_stats_t();

    void print() const;
    void reset();


    // Helper functions used by both the mainstream and the DORA flusher
    // They are here in order to be accessed by both 

    // Taken from shore/sm/log.h
    static long floor(long offset, long block_size) { return (offset/block_size)*block_size; }
    static long ceil(long offset, long block_size) { return floor(offset + block_size - 1, block_size); }

    long _logpart_sz;
    long _log_diff(const lsn_t& head, const lsn_t& tail);


}; // EOF: flusher_stats_t



/******************************************************************** 
 *
 * @class: flusher_t
 *
 * @brief: A class for the Flusher that implements group commit (and
 *         batch flushing) in Shore
 * 
 ********************************************************************/


const int FLUSHER_BUFFER_EXPECTED_SZ    = 3000;   // pulling this out of the thin air
const int FLUSHER_GROUP_SIZE_THRESHOLD  = 100;    // Flush every 100 xcts
const int FLUSHER_LOG_SIZE_THRESHOLD    = 200000; // Flush every 200K
const int FLUSHER_TIME_THRESHOLD        = 1000;   // Flush every 1000usec (msec)


class flusher_t : public base_worker_t
{   
public:
    typedef srmwqueue<trx_request_t>    BaseQueue;

private:

    // It has two queues, one with the un-flushed xcts and one with those 
    // underway
    guard<BaseQueue> _base_toflush;
    guard<BaseQueue> _base_flushing;

protected:

    guard<Pool> _pxct_toflush_pool;
    guard<Pool> _pxct_flushing_pool;

    flusher_stats_t _stats;
    
    virtual int _pre_STOP_impl();
    int _work_ACTIVE_impl(); 
    virtual int _check_waiting(bool& bSleepNext, 
                               const lsn_t& durablelsn, 
                               lsn_t& maxlsn,
                               uint& waiting);
    virtual int _move_from_flushing(const lsn_t& durablelsn);

public:

    flusher_t(ShoreEnv* env, c_str tname,
              processorid_t aprsid = PBIND_NONE, 
              const int use_sli = 0);
    virtual ~flusher_t();

    inline void enqueue_toflush(trx_request_t* areq) { _base_toflush->push(areq,true); }

    int statistics();  

}; // EOF: flusher_t



EXIT_NAMESPACE(shore);

#endif /** __SHORE_FLUSHER_H */

