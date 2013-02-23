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

/** @file:  srmwqueue.h
 *
 *  @brief: A single-reader, multiple-writer queue.
 *
 *  Queue size is unbounded and the (shore_worker) reader initially spins while 
 *  waiting for new elements to arrive and then sleeps on a condex.
 *
 *  @author: Ippokratis Pandis (ipandis)
 *  @author: Ryan Johnson (ryanjohn)
 */

#ifndef __SHORE_SRMW_QUEUE_H
#define __SHORE_SRMW_QUEUE_H

#include <sthread.h>
#include <vector>

#include "util.h"
#include "sm/shore/common.h"
#include "sm/shore/shore_worker.h"


ENTER_NAMESPACE(shore);


template<class Action>
struct srmwqueue 
{
    typedef typename PooledVec<Action*>::Type ActionVec;
    typedef typename ActionVec::iterator ActionVecIt;
    
    // owner thread
    base_worker_t* _owner;

    guard<ActionVec> _for_writers;
    guard<ActionVec> _for_readers;
    ActionVecIt _read_pos;
    mcs_lock      _lock;
    int volatile  _empty;

    eWorkingState _my_ws;

    int _loops; // how many loops (spins) it will do before going to sleep (1=sleep immediately)
    int _thres; // threshold value before waking up

    srmwqueue(Pool* actionPtrPool) 
        : _owner(NULL), _empty(true), _my_ws(WS_UNDEF), 
          _loops(0), _thres(0)
    { 
        assert (actionPtrPool);
        _for_writers = new ActionVec(actionPtrPool);
        _for_readers = new ActionVec(actionPtrPool);
        _read_pos = _for_readers->begin();
    }
    ~srmwqueue() { }


    // sets the pointer of the queue to the controls of a specific worker thread
    void setqueue(eWorkingState aws, base_worker_t* owner, const int& loops, const int& thres) 
    {
        CRITICAL_SECTION(q_cs, _lock);
        _my_ws = aws;
        _owner = owner;
        _loops = loops;
        _thres = thres;
    }

    // returns true if the passed control is the same
    bool is_control(base_worker_t* athread) const { return (_owner==athread); }  

    // !!! @note: should be called only by the reader !!!
    inline int is_empty(void) const {
        return ((_read_pos == _for_readers->end()) && (*&_empty));
    }

    // The expensive version which first locks, and then checks if empty
    bool is_really_empty(void) 
    {
        CRITICAL_SECTION(cs, _lock);
        bool isEmpty = ((_read_pos == _for_readers->end()) && (*&_empty));
        if (isEmpty) { assert (_for_writers->empty()); }
        return (isEmpty);
    }

    // spins until new input is set
    bool wait_for_input() 
    {
        assert (_owner);
        int loopcnt = 0;
        uint_t wc = WC_ACTIVE;

        // 1. start spinning
	while (*&_empty) {

            wc = _owner->get_control(); 

            // 2. if thread was signalled to stop
	    //if ((wc != WC_ACTIVE) && (wc != WC_RECOVERY)) {
	    if (wc != WC_ACTIVE) {
                _owner->set_ws(WS_FINISHED);
		return (false);
            }

            // 3. if thread was signalled to go to other queue
            if (!_owner->can_continue(_my_ws)) return (false);
            
            // 4. if spinned too much, start waiting on the condex
            if (++loopcnt > _loops) {
                loopcnt = 0;
    
                //TRACE( TRACE_TRX_FLOW, "Condex sleeping (%d)...\n", _my_ws);
                //assert (_my_ws==WS_INPUT_Q); // can sleep only on input queue
                loopcnt = _owner->condex_sleep();
                //TRACE( TRACE_TRX_FLOW, "Condex woke (%d) (%d)...\n", _my_ws, loopcnt);

                // after it wakes up, should do the loop again.
                // if something has been pushed then _empty will be false
                // and it will proceed normally.
                // if signalled because it should stop, it will do a loop 
                // and return false.
                // if signalled because it should go to other queue, it will
                // do a loop and return false.
            }
	}
    
	{
	    CRITICAL_SECTION(cs, _lock);
	    _for_readers->erase(_for_readers->begin(),_for_readers->end());
	    _for_writers->swap(*_for_readers);
	    _empty = true;
	}
    
	_read_pos = _for_readers->begin();
	return (true);
    }
    
    inline Action* pop() {
        // pops an action from the input vector, or waits for one to show up
	if ((_read_pos == _for_readers->end()) && (!wait_for_input()))
	    return (NULL);
	return (*(_read_pos++));
    }

    inline void push(Action* a, const bool bWake) {
        //assert (a);
        int queue_sz;

        // push action
        {
            CRITICAL_SECTION(cs, _lock);
            _for_writers->push_back(a);
            _empty = false;
            queue_sz = _for_writers->size();
        }

        // don't try to wake on every call. let for some requests to batch up
        if ((queue_sz >= _thres) || bWake) {        
            // wake up if assigned worker thread sleeping
            _owner->set_ws(_my_ws);
        }
    }

    // resets queue
    void clear(const bool removeOwner=true) {
        CRITICAL_SECTION(q_cs, _lock);

        // clear owner
        if (removeOwner) _owner = NULL;

        // clear lists
        _for_writers->erase(_for_writers->begin(),_for_writers->end());
        _for_readers->erase(_for_readers->begin(),_for_readers->end());

        // set the reading position to the beginning
        _read_pos = _for_readers->begin();

        // the queue is empty again
        _empty = true;
    }    
  
}; // EOF: struct srmwqueue


EXIT_NAMESPACE(shore);

#endif /** __SHORE_SRMW_QUEUE_H */

