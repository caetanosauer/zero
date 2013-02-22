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

/** @file:   partition.h
 *
 *  @brief:  Template-based class for each logical partition in DORA
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */

#ifndef __DORA_PARTITION_H
#define __DORA_PARTITION_H


#include "dora/base_partition.h"

#include "sm/shore/srmwqueue.h"

#include "dora/lockman.h"
#include "dora/worker.h"

#include "sm/shore/shore_helper_loader.h"


using namespace shore;


ENTER_NAMESPACE(dora);

const int ACTIONS_PER_INPUT_QUEUE_POOL_SZ = 60;
const int ACTIONS_PER_COMMIT_QUEUE_POOL_SZ = 60;


/******************************************************************** 
 *
 * @class: partition_t
 *
 * @brief: Abstract class for the data partitions
 *
 ********************************************************************/

template <class DataType>
class partition_t : public base_partition_t
{
public:

    typedef action_t<DataType>         Action;
    typedef dora_worker_t              Worker;
    typedef srmwqueue<Action>          Queue;
    typedef key_wrapper_t<DataType>    Key;
    typedef lock_man_t<DataType>       LockManager;

    typedef KALReq_t<DataType>      KALReq;
    typedef std::vector<KALReq>     KALReqVec;
    //typedef typename PooledVec<KALReq>::Type     KALReqVec;

protected:

    // pointers to primary owner and pool of standby worker threads
    Worker*       _owner;        // primary owner
    Worker*       _standby;      // standby pool

    // lock manager for the partition
    guard<LockManager>  _plm;


    // Each partition has two lists of Actions
    //
    // 1. (_committed_list)
    //    The list of actions that were committed.
    //    If an action is committed the partition's lock manager needs to remove
    //    all the locks occupied by this particular action.
    //
    // 2. (_input_queue)
    //    The queue were multiple writers input new requests (Actions).
    //    This is the main entrance point for new requests for the partition. 
    //
    //
    // The active worker thread does the following cycle:
    //
    // - Checks if there is any action to be committed, and releases the 
    //   corresponding locks.
    // - The release of committed actions, may make waiting actions
    //   to grab the locks and be ready to be executed. The worker
    //   executes all those.
    // - If no actions were committe, it goes to the queue and waits for new inputs.
    //

    // queue of new Actions
    // single reader - multiple writers
    guard<Queue>    _input_queue; 

    // queue of committed Actions
    // single reader - multiple writers
    guard<Queue>    _committed_queue;

    // pools for actions for the srmwqueues
    guard<Pool>     _actionptr_input_pool;
    guard<Pool>     _actionptr_commit_pool;

public:

    partition_t(ShoreEnv* env, table_desc_t* ptable, 
                const uint apartid, 
                const processorid_t aprsid,
                const uint keyEstimation);

    virtual ~partition_t() 
    { 
        _input_queue.done();
        _actionptr_input_pool.done();
        
        _committed_queue.done();
        _actionptr_commit_pool.done();
    }    

    // get lock manager
    inline LockManager* plm() { return (_plm); }

    // get owner thread
    Worker* owner() { return (_owner); }


    //// Action-related methods ////

    // release a trx
    inline int release(Action* action,
                       BaseActionPtrList& readyList, 
                       BaseActionPtrList& promotedList) 
    { 
        return (_plm->release_all(action,readyList,promotedList)); 
    }

    inline bool acquire(KALReqVec& akalvec) 
    {
        return (_plm->acquire_all(akalvec));
    }



    //// Partition Interface ////

    // returns true if action can be enqueued in this partition
    virtual bool verify(Action& /* action */) { assert(0); /* TODO */ return (true); }

    // input for normal actions
    // enqueues action, 0 on success
    int enqueue(Action* pAction, const bool bWake);
    virtual base_action_t* dequeue();
    inline int has_input(void) const { 
        return (!_input_queue->is_empty()); 
    }    
    bool is_input_owner(base_worker_t* aworker) {
        return (_input_queue->is_control(aworker));
    }

    // deque of actions to be committed
    int enqueue_commit(Action* apa, const bool bWake=true);
    virtual base_action_t* dequeue_commit();
    inline int has_committed(void) const { 
        return (!_committed_queue->is_empty()); 
    }
    bool is_committed_owner(base_worker_t* aworker) {
        return (_committed_queue->is_control(aworker));
    }


    // resets/initializes the partition, possibly to a new processor
    virtual int reset(const processorid_t aprsid = PBIND_NONE,
                      const uint standby_sz = DF_NUM_OF_STANDBY_THRS);

    // Goes over all the actions and aborts them
    virtual int abort_all_enqueued();

    // stops the partition
    virtual void stop();

    // prepares the partition for a new run
    virtual w_rc_t prepareNewRun();

    // stats
    virtual void statistics(worker_stats_t& gather);

    virtual void dump();

    void stlsize(uint& gather);

private:                

    // thread control
    int _start_owner();
    int _stop_threads();
    int _generate_primary();
    int _generate_standby_pool(const uint sz, uint& pool_sz,
                               const processorid_t aprsid = PBIND_NONE);
    Worker* _generate_worker(const processorid_t aprsid, c_str wname, const int use_sli);    

protected:    

    int isFree(Key akey, eDoraLockMode lmode);

}; // EOF: partition_t



/****************************************************************** 
 *
 * Construction
 *
 * @note: Setups the queues and their corresponding pools
 *
 ******************************************************************/

template <class DataType>
partition_t<DataType>::partition_t(ShoreEnv* env, table_desc_t* ptable, 
                                   const uint apartid, 
                                   const processorid_t aprsid,
                                   const uint keyEstimation) 
    : base_partition_t(env,ptable,apartid,aprsid),
      _owner(NULL), _standby(NULL)
{
    _plm = new LockManager(keyEstimation);

    _actionptr_input_pool = new Pool(sizeof(Action*),ACTIONS_PER_INPUT_QUEUE_POOL_SZ);
    _input_queue = new Queue(_actionptr_input_pool.get());

    _actionptr_commit_pool = new Pool(sizeof(Action*),ACTIONS_PER_COMMIT_QUEUE_POOL_SZ);
    _committed_queue = new Queue(_actionptr_commit_pool.get());
}



/****************************************************************** 
 *
 * @fn:     enqueue()
 *
 * @brief:  Enqueues action at the input queue
 *
 * @return: 0 on success, see dora_error.h for error codes
 *
 ******************************************************************/

template <class DataType>
int partition_t<DataType>::enqueue(Action* pAction, const bool bWake)
{
    //assert(verify(*pAction)); // TODO: Enable this
//     if (!verify(*pAction)) {
//         TRACE( TRACE_DEBUG, "Try to enqueue to the wrong partition...\n");
//         return (de_WRONG_PARTITION);
//     }

#ifdef WORKER_VERBOSE_STATS
    pAction->mark_enqueue();
#endif

    pAction->set_partition(this);
    _input_queue->push(pAction,bWake);
    return (0);
}



/****************************************************************** 
 *
 * @fn:    dequeue()
 *
 * @brief: Returns the action at the head of the input queue
 *
 ******************************************************************/

template <class DataType>
inline base_action_t* partition_t<DataType>::dequeue()
{
    return (_input_queue->pop());
}




/****************************************************************** 
 *
 * @fn:     enqueue_commit()
 *
 * @brief:  Pushes an action to the partition's committed actions list
 *
 ******************************************************************/

template <class DataType>
int partition_t<DataType>::enqueue_commit(Action* pAction, const bool bWake)
{
    assert (pAction->get_partition()==this);
    TRACE( TRACE_TRX_FLOW, "Enq committed (%d) to (%s-%d)\n", 
           pAction->tid().get_lo(), _table->name(), _part_id);
    _committed_queue->push(pAction,bWake);
    return (0);
}



/****************************************************************** 
 *
 * @fn:     dequeue_commit()
 *
 * @brief:  Returns the action at the head of the committed queue
 *
 ******************************************************************/

template <class DataType>
inline base_action_t* partition_t<DataType>::dequeue_commit()
{
    //assert (has_committed());
    return (_committed_queue->pop());
}



/****************************************************************** 
 *
 * @fn:     stop()
 *
 * @brief:  Stops the partition
 *          - Stops and deletes all workers
 *          - Clears data structures
 *
 ******************************************************************/

template <class DataType>
void partition_t<DataType>::stop()
{        
    // Stop the worker & standby threads
    _stop_threads();

    // Clear queues
    _input_queue->clear();
    _committed_queue->clear();
    
    // Reset lock-manager
    _plm->reset();
}



/****************************************************************** 
 *
 * @fn:     reset()
 *
 * @brief:  Resets the partition
 *
 * @param:  (optional) The size of the standby pool
 *
 * @return: Returns 0 on success
 *
 * @note:   Check dora_error.h for error codes
 *
 ******************************************************************/

template <class DataType>
int partition_t<DataType>::reset(const processorid_t aprsid,
                                 const uint poolsz)
{        
    TRACE( TRACE_DEBUG, "part (%s-%d) - pool (%d) - cpu (%d)\n", 
           _table->name(), _part_id, poolsz, aprsid);

    // Stop the worker & standby threads
    _stop_threads();

    // Clear queues
    _input_queue->clear();
    _committed_queue->clear();
    
    // Reset lock-manager
    _plm->reset();


    // Lock the primary and standby pool, and generate workers
    CRITICAL_SECTION(owner_cs, _owner_lock);
    CRITICAL_SECTION(standby_cs, _standby_lock);
    CRITICAL_SECTION(pat_cs, _pat_count_lock);    

    _prs_id = aprsid;

    // Generate primary
    if (_generate_primary()) {
        TRACE (TRACE_ALWAYS, "part (%s-%d) Failed to generate primary thread\n",
               _table->name(), _part_id);
        assert (0); // should not happen
        return (de_GEN_PRIMARY_WORKER);
    }

    // Set a single active thread
    _pat_count = 1;
    _pat_state = PATS_SINGLE;

    // Generate standby pool   
    if (_generate_standby_pool(poolsz, _standby_cnt, aprsid)) {
        TRACE (TRACE_ALWAYS, "part (%s-%d) Failed to generate pool of (%d) threads\n",
               _table->name(), _part_id, poolsz);
        TRACE (TRACE_ALWAYS, "part (%s-%d) Pool of only (%d) threads generated\n", 
               _table->name(), _part_id, _standby_cnt);
        return (de_GEN_STANDBY_POOL);
    }

    // Kick-off primary
    _start_owner();
    return (0);
}    


/****************************************************************** 
 *
 * @fn:     prepareNewRun()
 *
 * @brief:  Prepares the partition for a new run
 *          Clears the lm and the queues (if they have anything)
 *
 * @note:   This function should be called only before new runs.
 *
 ******************************************************************/

template <class DataType>
w_rc_t partition_t<DataType>::prepareNewRun() 
{
    // Needs to spin until worker is done "cleaning" left-overs 
    // (un-executed actions and logical locks)
    assert (_owner);
    while (!_owner->is_sleeping()) {
        // spin //
        TRACE( TRACE_ALWAYS, "Waiting for (%s-%d) to sleep\n", 
               _table->name(), _part_id);
        static uint HALF_MILLION = 500000;
        usleep(HALF_MILLION); // sleep for a half a sec
    }

    // --------------------------------------
    // Enter recovery mode for that partition
    // eWorkerControl old_wc = _owner->get_control();
    // _owner->set_control(WC_RECOVERY);

    // Clear queues but not remove owner
    while (!_committed_queue->is_really_empty()) {
        TRACE( TRACE_ALWAYS, "CommittedQueue of (%s-%d) not empty\n");
        _owner->doRecovery();
    }
    _committed_queue->clear(false);

    while (!_input_queue->is_really_empty()) {
        TRACE( TRACE_ALWAYS, "InputQueue of (%s-%d) not empty\n");
        _owner->doRecovery();
    }
    _input_queue->clear(false); 


    // Make sure that no key is left locked
    //
    // If the owner is sleeping it means that either it has finished
    // working and all the logical locks are clean, or there is a lock
    // waiting for actions executed by other partitions to finish.
    // This function is called before every new run. Hence, the latter
    // case should not be happening
    vector<xct_t*> toabort;
    _plm->is_clean(toabort);
    if (!toabort.empty()) {
        // Fire up an smthread to abort the list
        abort_smt_t* asmt = new abort_smt_t(c_str(_table->name()),_env,toabort);
        assert(asmt);
        asmt->fork();
        asmt->join();
        uint aborted = asmt->_aborted;
        TRACE( TRACE_ALWAYS, "(%d) xcts aborted\n", aborted);
        delete(asmt);
        asmt = NULL;
    }

    // Reset lock manager map by removing all the entries.
    // This should happen only if the size of the map exceeds
    // a certain value
    if (_plm->keystouched() > D_MIN_KEYS_TOUCHED) {
        TRACE( TRACE_DEBUG, "Cleaning LockMap (%s-%d) with (%d) keys\n",
               _table->name(), _part_id, _plm->keystouched());
        _plm->reset();
    }

    //_owner->set_control(old_wc);
    // Exit recovery mode
    // --------------------------------------
    return (RCOK);
}


/****************************************************************** 
 *
 * @fn:     _start_owner()
 *
 * @brief:  Kick starts the owner
 *
 * @note:   Assumes that the owner_cs is already locked
 *
 ******************************************************************/

template <class DataType>
int partition_t<DataType>::_start_owner()
{
    assert (_owner);
    _owner->start();
    return (0);
}


/****************************************************************** 
 *
 * @fn:     _stop_threads()
 *
 * @brief:  Sends a stop message to all the workers
 *
 ******************************************************************/

template <class DataType>
int partition_t<DataType>::_stop_threads()
{
    int i = 0;

    // owner
    CRITICAL_SECTION(owner_cs, _owner_lock);
    if (_owner) {
        _owner->stop();
        _owner->join();
        delete (_owner);
        ++i;
    }    
    _owner = NULL; // join()?

    // reset queues' worker control pointers
    _input_queue->setqueue(WS_UNDEF,NULL,0,0); 
    _committed_queue->setqueue(WS_UNDEF,NULL,0,0); 


    // standy
    CRITICAL_SECTION(standby_cs, _standby_lock);
    if (_standby) {
        _standby->stop();
        _standby->join();
        delete (_standby);
        ++i;
    }
    _standby = NULL; // join()?
    _standby_cnt = 0;

    // thread stats
    CRITICAL_SECTION(pat_cs, _pat_count_lock);
    _pat_count = 0;
    _pat_state = PATS_UNDEF;

    return (0);
}


/****************************************************************** 
 *
 * @fn:     _generate_standby_pool()
 *
 * @brief:  Generates (sz) standby worker threads, linked together
 *
 * @return: 0 on sucess
 *
 * @note:   Assumes lock on pool head pointer is already acquired
 * @note:   The second param will be set equal with the number of
 *          threads actually generated (_standby_cnt)
 *
 ******************************************************************/

template <class DataType>
int partition_t<DataType>::_generate_standby_pool(const uint sz, uint& pool_sz,
                                                  const processorid_t /* aprsid */)
{
    assert (_standby==NULL); // prevent losing thread pointer 

    Worker* pworker = NULL;
    Worker* pprev_worker = NULL;
    pool_sz=0;

    if (sz>0) {

        // Generate the head of standby pool
        int use_sli = envVar::instance()->getVarInt("db-worker-sli",0);

        // IP: We can play with the binding of the standby threads
        pworker = _generate_worker(_prs_id, 
                                   c_str("%s-P-%d-STBY-%d", _table->name(), _part_id, pool_sz),
                                   use_sli);
        if (!pworker) {
            TRACE( TRACE_ALWAYS, "Problem generating worker thread (%d)\n", pool_sz);
            return (de_GEN_WORKER);
        }

        ++pool_sz;
        _standby = pworker;      // set head of pool
        pprev_worker = pworker;

        _standby->fork();
                                   
        TRACE( TRACE_DEBUG, "Head standby worker thread forked\n");  

        // Generate the rest of the pool
        for (pool_sz=1; pool_sz<sz; pool_sz++) {
            // Generate a worker
            pworker = _generate_worker(_prs_id,
                                       c_str("%s-P-%d-STBY-%d", _table->name(), _part_id, pool_sz),
                                       use_sli);
            if (!pworker) {
                TRACE( TRACE_ALWAYS, "Problem generating worker thread (%d)\n", pool_sz);
                return (de_GEN_WORKER);
            }

            // Add to linked list
            pprev_worker->set_next(pworker);
            pprev_worker = pworker;

            _standby->fork();

            TRACE( TRACE_DEBUG, "Standby worker (%d) thread forked\n", pool_sz);
        }
    }    
    return (0);
}


/****************************************************************** 
 *
 * @fn:     _generate_primary()
 *
 * @brief:  Generates a worker thread, promotes it to primary owner, 
 *          sets the queue to point to primary owner's controls,
 *          and forks it
 *
 * @return: Retuns 0 on sucess
 *
 * @note:   Assumes lock on owner pointer is already acquired
 *
 ******************************************************************/

template <class DataType>
int partition_t<DataType>::_generate_primary() 
{
    assert (_owner==NULL); // prevent losing thread pointer 

    envVar* pe = envVar::instance();

    int use_sli = pe->getVarInt("db-worker-sli",0);

    Worker* pworker = _generate_worker(_prs_id, 
                                       c_str("%s-P-%d-PRI",_table->name(), _part_id),
                                       use_sli);
    if (!pworker) {
        TRACE( TRACE_ALWAYS, "Problem generating worker thread\n");
        return (de_GEN_WORKER);
    }

    _owner = pworker;
    _owner->set_data_owner_state(DOS_ALONE);

    int lc = pe->getVarInt("db-worker-queueloops",0);
    int thres_inp_q = pe->getVarInt("dora-worker-inp-q-sz",0);
    int thres_com_q = pe->getVarInt("dora-worker-com-q-sz",0);

    // it is safer the thresholds not to be larger than the client batch size
    int batch_sz = pe->getVarInt("db-cl-batchsz",0);
    if (batch_sz < thres_inp_q) thres_inp_q = batch_sz;
    if (batch_sz < thres_com_q) thres_com_q = batch_sz;

    // pass worker thread controls to the two queues
    _input_queue->setqueue(WS_INPUT_Q,_owner,lc,thres_inp_q);  
    _committed_queue->setqueue(WS_COMMIT_Q,_owner,lc,thres_com_q);  

    _owner->fork();
    return (0);
}


/****************************************************************** 
 *
 * @fn:     _generate_worker()
 *
 * @brief:  Generates a worker thread and assigns it to the partition
 *
 * @return: Retuns 0 on sucess
 *
 ******************************************************************/

template <class DataType>
dora_worker_t* partition_t<DataType>::_generate_worker(const processorid_t prsid,
                                                       c_str strname,
                                                       const int use_sli) 
{
    // 1. create worker thread
    // 2. set self as worker's partition
    Worker* pworker = new Worker(_env, this, strname, prsid, use_sli); 
    return (pworker);
}


/****************************************************************** 
 *
 * @fn:     abort_all_enqueued()
 *
 * @brief:  Goes over the queue and aborts any unprocessed request
 * 
 ******************************************************************/

template <class DataType>
int partition_t<DataType>::abort_all_enqueued()
{
    // 1. go over all requests
    Action* pa;
    int reqs_read  = 0;
    int reqs_write = 0;
    int reqs_abt   = 0;

    assert (_owner);

    // go over the readers list
    while (_input_queue->_read_pos != _input_queue->_for_readers->end()) {
        pa = *(_input_queue->_read_pos++);
        ++reqs_read;
        if (_owner->abort_one_trx(pa->xct())) 
            ++reqs_abt;        
    }

    // go over the writers list
    {
        CRITICAL_SECTION(q_cs, _input_queue->_lock);
        for (_input_queue->_read_pos = _input_queue->_for_writers->begin();
             _input_queue->_read_pos != _input_queue->_for_writers->end();
             _input_queue->_read_pos++) 
            {
                pa = *(_input_queue->_read_pos);
                ++reqs_write;
                if (_owner->abort_one_trx(pa->xct())) 
                    ++reqs_abt;
            }
    }

    if ((reqs_read + reqs_write) > 0) {
        TRACE( TRACE_ALWAYS, "(%d) aborted before stopping. (%d) (%d)\n", 
               reqs_abt, reqs_read, reqs_write);
    }
    return (reqs_abt);
}



/****************************************************************** 
 *
 * @fn:     statistics()
 *
 ******************************************************************/

template <class DataType>
void partition_t<DataType>::statistics(worker_stats_t& gather) 
{
    if (_owner) {
        gather += _owner->get_stats();
        _owner->reset_stats();
    }
}


/****************************************************************** 
 *
 * @fn:     stlsize()
 *
 * @brief:  The size of the map of the lock manager (aka local lock table)
 *
 ******************************************************************/

template <class DataType>
void partition_t<DataType>::stlsize(uint& gather) 
{
    assert (_plm); 
    gather += _plm->keystouched();
}



/****************************************************************** 
 *
 * @fn:     dump()
 *
 ******************************************************************/

template <class DataType>
void partition_t<DataType>::dump() 
{
    base_partition_t::dump();
    _plm->dump();
}


EXIT_NAMESPACE(dora);

#endif /** __DORA_PARTITION_H */

