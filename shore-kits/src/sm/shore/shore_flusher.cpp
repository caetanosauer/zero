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
 *  @brief:  The mainstreama flusher.
 *
 *  @author: Ippokratis Pandis, Feb 2010
 */

#include "sm/shore/shore_flusher.h"
#include "sm/shore/shore_env.h"
#include "xct.h"

ENTER_NAMESPACE(shore);


/****************************************************************** 
 *
 * @struct: flusher_stats_t
 * 
 ******************************************************************/

// copied from sm/log.h
const uint smBLOCK_SIZE=8192;
const uint smSEGMENT_SIZE=128*smBLOCK_SIZE;

flusher_stats_t::flusher_stats_t()
    : served(0), flushes(0), logsize(0), alreadyFlushed(0), waiting(0),
      trigByXcts(0), trigBySize(0), trigByTimeout(0)
{

    // Calculates the partition size
    _logpart_sz = (envVar::instance()->getSysVarInt("logbufsize"))/8;
    //fprintf(stdout, "LogPartitionSize: %d\n", _logpart_sz);
    _logpart_sz = floor(_logpart_sz + smSEGMENT_SIZE - 1, smSEGMENT_SIZE);
    //fprintf(stdout, "LogPartitionSize: %d\n", _logpart_sz);
}


flusher_stats_t::~flusher_stats_t() 
{
}


void flusher_stats_t::print() const 
{
    static const long MEGABYTE = 1024*1024;

    TRACE( TRACE_STATISTICS, "Flushes:     (%d)\n", flushes);
    TRACE( TRACE_STATISTICS, "Xcts:        (%d)\t(%.2f)\n", 
           served, (double)served/(double)flushes);
    TRACE( TRACE_STATISTICS, "Logsize MBs: (%ld)\t(%.2f)\n", 
           logsize/MEGABYTE, (long long)logsize/(long long)(flushes*MEGABYTE));
    TRACE( TRACE_STATISTICS, "Already:     (%d)\t(%.2f%%)\n", 
           alreadyFlushed, (double)(100*alreadyFlushed)/(double)served);
    TRACE( TRACE_STATISTICS, "Waiting:     (%d)\t(%.2f)\n", 
           waiting, (double)waiting/(double)flushes);

    TRACE( TRACE_STATISTICS, "By Xcts:     (%d)\t(%.2f%%)\n", 
           trigByXcts,(double)(100*trigByXcts)/(double)flushes);
    TRACE( TRACE_STATISTICS, "By Size:     (%d)\t(%.2f%%)\n", 
           trigBySize,(double)(100*trigBySize)/(double)flushes);
    TRACE( TRACE_STATISTICS, "By Timeout:  (%d)\t(%.2f%%)\n", 
           trigByTimeout,(double)(100*trigByTimeout)/(double)flushes);
}

void flusher_stats_t::reset()
{
    served = 0;
    flushes = 0;

    logsize = 0;
    alreadyFlushed = 0;
    waiting = 0;

    trigByXcts = 0;
    trigBySize = 0;
    trigByTimeout = 0;
}



/****************************************************************** 
 *
 * @fn:     _log_diff(const lsn_t&, const lsn_t&)
 *
 * @brief:  Returns a rough estimation of the log size between
 *          two lsn_t
 * 
 ******************************************************************/

long flusher_stats_t::_log_diff(const lsn_t& head, const lsn_t& tail)
{
    if (head.hi()==tail.hi()) return (head.lo()-tail.lo());
    assert(head.hi()>tail.hi());
    long diff = (head.hi() - tail.hi()) * _logpart_sz;
    if (head.lo()>tail.lo()) return (diff + (head.lo()-tail.lo()));
    return (diff - (tail.lo()-head.lo()));
}




/******************************************************************** 
 *
 * @struct: flusher_t
 * 
 ********************************************************************/

flusher_t::flusher_t(ShoreEnv* env, 
                     c_str tname,
                     processorid_t aprsid, 
                     const int use_sli) 
    : base_worker_t(env, tname, aprsid, use_sli)
{ 
    _pxct_toflush_pool = new Pool(sizeof(xct_t*),FLUSHER_BUFFER_EXPECTED_SZ);
    _base_toflush = new BaseQueue(_pxct_toflush_pool.get());
    assert (_base_toflush.get());
    _base_toflush->setqueue(WS_COMMIT_Q,this,2000,0);  // wake-up immediately, spin 2000

    _pxct_flushing_pool = new Pool(sizeof(xct_t*),FLUSHER_BUFFER_EXPECTED_SZ);
    _base_flushing = new BaseQueue(_pxct_flushing_pool.get());
    assert (_base_flushing.get());
    _base_flushing->setqueue(WS_COMMIT_Q,this,0,0);  // wake-up immediately
}

flusher_t::~flusher_t() 
{ 
    // -- clear queues --
    // they better be empty by now
    assert (_base_toflush->is_empty());
    _base_toflush.done();
    _pxct_toflush_pool.done();

    assert (_base_flushing->is_empty());
    _base_flushing.done();
    _pxct_flushing_pool.done();
}

int flusher_t::statistics()
{
    _stats.print();
    _stats.reset();
    return (0);
}


/****************************************************************** 
 *
 * @fn:     _work_ACTIVE_impl()
 *
 * @brief:  Implementation of the ACTIVE state (StagedGroupCommit)
 *          The flusher monitors the toflush queue and decides when
 *          it is good time to issue a flush
 *
 * @uses:   A couple of threshold values to decide whether to flush
 *
 * @return: 0 on success
 * 
 ******************************************************************/

int flusher_t::_work_ACTIVE_impl()
{    
    envVar* ev = envVar::instance();
    int binding = ev->getVarInt("flusher-binding",0);
    if (binding==0) _prs_id = PBIND_NONE;
    TRY_TO_BIND(_prs_id,_is_bound);

    // read configuration 
    uint maxGroupSize = ev->getVarInt("flusher-group-size",FLUSHER_GROUP_SIZE_THRESHOLD);
    uint maxLogSize = ev->getVarInt("flusher-log-size",FLUSHER_LOG_SIZE_THRESHOLD);
    uint maxTimeIntervalusec = ev->getVarInt("flusher-timeout",FLUSHER_TIME_THRESHOLD);

    uint waiting = 0;
    lsn_t durablelsn, maxlsn;
    bool bShouldFlush = false;
    long logWaiting = 0;
    struct timespec start, ts;
    static long const BILLION = 1000*1000*1000;
    bool bSleepNext = false;

    clock_gettime(CLOCK_REALTIME, &start);

    // set timeout
    ts = start;
    ts.tv_nsec += maxTimeIntervalusec * 1000;
    if (ts.tv_nsec > BILLION) {
        ts.tv_nsec -= BILLION;
        ts.tv_sec++;
    }

    // Check if signalled to stop
    while (get_control() == WC_ACTIVE) {        

        // Reset the flags for the new loop
        set_ws(WS_LOOP);
        bShouldFlush = false;

        // Read the durable lsn
        _env->db()->get_durable_lsn(durablelsn);
        maxlsn = durablelsn;

        // Check the list of waiting to flush xcts
        _check_waiting(bSleepNext,durablelsn,maxlsn,waiting);

        // Decide whether to flush or not
        if (waiting >= maxGroupSize) {
            // Do we have already too many waiting?
            bShouldFlush = true;
            _stats.trigByXcts++;
        }
        else {
            logWaiting = _stats._log_diff(maxlsn,durablelsn);
            if (logWaiting >= maxLogSize) {
                // Is the log to be flushed already big?
                bShouldFlush = true;
                _stats.trigBySize++;
            }
            else {
                // Not enough requests or log to flush the group 

                // When was the last time we flushed?
                clock_gettime(CLOCK_REALTIME, &start);
                if ((start.tv_sec > ts.tv_sec) ||
                    (start.tv_sec == ts.tv_sec && start.tv_nsec > ts.tv_nsec)) {
                    bShouldFlush = true;
                    _stats.trigByTimeout++;
                    
                    // set next timeout
                    ts = start;
                    ts.tv_nsec += maxTimeIntervalusec * 1000;
                    if (ts.tv_nsec > BILLION) {
                        ts.tv_nsec -= BILLION;
                        ts.tv_sec++;
                    }
                }
                else {
                    // Set a flag which will put it to sleep in the next loop,
                    // unless a new request arrives. But, before sleeping call
                    // for a lazy flush 
                    if (waiting) _env->db()->sync_log();                    
                    bSleepNext = true;
                }
            }
        }

        if (get_control() != WC_ACTIVE) return(0);

        // If yes, flush 
        //
        // IP: Another option is to flush asynchronously at this point and 
        // block on the first "flushing" request that has not been durable 
        // already
        if (bShouldFlush) {
            _stats.flushes++;
            _stats.waiting += waiting;
            _stats.logsize += logWaiting;
            _env->db()->sync_log(); // it will block
            
            waiting = 0;
            logWaiting = 0;
        }

        // At this point we know that everyone on the "flushing" queue is durable
        // Notify all the clients
        
        // Re-read the durable lsn, just for sanity checking
        _env->db()->get_durable_lsn(durablelsn);
        _move_from_flushing(durablelsn);
    }
    return (0);
}


/****************************************************************** 
 *
 * @fn:     _check_waiting()
 *
 * @brief:  Checks the list of xcts waiting at the "to flush" queue 
 *
 * @return: 0 on success
 * 
 ******************************************************************/

int flusher_t::_check_waiting(bool& bSleepNext, 
                              const lsn_t& durablelsn, 
                              lsn_t& maxlsn,
                              uint& waiting)
{
    trx_request_t* preq = NULL;   
    lsn_t xctlsn;

    // Check if there are xcts waiting at the "to flush" queue 
    while ((!_base_toflush->is_empty()) || (bSleepNext)) {

        // Pop and read the xct info 
        preq = _base_toflush->pop();

        // The only way for pop() to return NULL is when signalled to stop
        if (preq) {
            xctlsn = preq->my_last_lsn();

            TRACE( TRACE_TRX_FLOW, 
                   "Xct (%d) lastLSN (%d) durableLSN (%d)\n",
                   preq->tid().get_lo(), xctlsn.lo(), maxlsn.lo());

            // If the xct is already durable (had been flushed) then
            // notify client
            if (durablelsn > xctlsn) {
                _stats.alreadyFlushed++;
                preq->notify_client();
                _env->inc_trx_com();
                _env->_request_pool.destroy(preq);
            }
            else {
                // Otherwise, add the rvp to the syncing (in-flight) list,
                // and update statistics
                maxlsn = std::max(maxlsn,xctlsn);
                _base_flushing->push(preq,false);
                waiting++;
            }
        }

        bSleepNext = false;
        _stats.served++;
    }

    return (0);
}


/****************************************************************** 
 *
 * @fn:     _move_from_flushing()
 *
 * @brief:  Called when we know that everyone on the "flushing" queue
 *          is durable. In the baseline case, it notifies clients.
 *
 * @return: 0 on success
 * 
 ******************************************************************/

int flusher_t::_move_from_flushing(const lsn_t& durablelsn)
{
    trx_request_t* preq = NULL;   
    lsn_t xctlsn;
    
    while (!_base_flushing->is_empty()) {
        preq = _base_flushing->pop();
        xctlsn = preq->my_last_lsn();
        assert (xctlsn < durablelsn);
        preq->notify_client();
        _env->inc_trx_com();
        _env->_request_pool.destroy(preq);
    }

    return (0); 
}



/****************************************************************** 
 *
 * @fn:     _pre_STOP_impl()
 *
 * @brief:  Operations done before the thread stops 
 *
 * @return: 0 on success
 * 
 ******************************************************************/

int flusher_t::_pre_STOP_impl() 
{ 
    uint afterStop = 0;
    trx_request_t* preq = NULL;

    // Notify the clients and clean up queues
    while (!_base_flushing->is_empty()) {
        ++afterStop;
        preq = _base_flushing->pop();
        preq->notify_client();
        _env->_request_pool.destroy(preq);
    }

    while (!_base_toflush->is_empty()) {
        ++afterStop;
        preq = _base_toflush->pop();
        preq->notify_client();
        _env->_request_pool.destroy(preq);
    }

    if (afterStop>0) 
        TRACE( TRACE_ALWAYS, 
               "Xcts flushed at stop (%d)\n",
               afterStop);

    return(0); 
}


EXIT_NAMESPACE(shore);
