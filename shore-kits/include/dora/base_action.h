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

/** @file:   base_action.h
 *
 *  @brief:  Base class for DORA actions
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */


#ifndef __DORA_BASE_ACTION_H
#define __DORA_BASE_ACTION_H

#include <cstdio>

#include "util.h"

#include "dora/key.h"

//using namespace shore;

ENTER_NAMESPACE(dora);


struct rvp_t; 

/******************************************************************** 
 *
 * @class: base_action_t
 *
 * @brief: (Non template-based) abstract base class for the actions
 *
 ********************************************************************/

class base_action_t
{
public:

    typedef std::vector<base_action_t*>     BaseActionPtrList;
    typedef BaseActionPtrList::iterator     BaseActionPtrIt;

protected:

    // rendez-vous point
    rvp_t*         _prvp;

    // trx-specific
    xct_t*         _xct;
    tid_t          _tid;

    int            _keys_needed;

    // flag indicating whether this action is read-only or not
    bool           _read_only;

    // flag showing that this action has set its keys
    bool           _keys_set;

    // flag set if action is secondary
    bool           _secondary;


#ifdef WORKER_VERBOSE_STATS
    stopwatch_t    _since_enqueue;
#endif

    // base action init
    inline void _base_set(xct_t* axct, const tid_t& atid, rvp_t* prvp, 
                          const int numkeys, const bool ro)
    {
        _tid = atid;
        _xct = axct;
        _prvp = prvp;
        _keys_needed = numkeys;
        _read_only = ro;
        _keys_set = false;
        _secondary =false;
    }

public:

    base_action_t() :
        _prvp(NULL), _xct(NULL), _keys_needed(0), 
        _read_only(false), _keys_set(0), _secondary(false)
    { }

    virtual ~base_action_t() { }


    // access methods
    inline rvp_t* rvp() { return (_prvp); }
    inline xct_t* xct() { return (_xct); }    
    inline tid_t  tid() { return (_tid); }
    inline tid_t tid() const { return (_tid); }

    // read only
    inline bool is_read_only() { return (_read_only); }
    inline void set_read_only() { 
        assert (!_keys_set); // this can happen only if keys are not set yet
        _read_only = true;
    }


    // secondary action - There is a list of RIDs that can be accessed directly,
    //                    once the locks are acquired.
    inline bool is_secondary() { return (_secondary); }
    inline void set_secondary() { _secondary = true; }

    // needed keys operations
    //inline const int needed() { return (_keys_needed); }

    inline bool is_ready() { 
        // if it does not need any other keys, 
        // it is ready to proceed
        return (_keys_needed==0);
    }

    inline int setkeys(const int numLocks = 1) {
        assert (numLocks);
        _keys_needed = numLocks;
        return (_keys_needed);
    } 

    inline bool gotkeys(const int numLocks = 1) {
        // called when acquired a number of locks
        // decreases the needed keys counter 
        // and returns if it is ready to proceed        

        // should need at least numLocks locks
        assert ((_keys_needed-numLocks)>=0); 
        _keys_needed -= numLocks;
        return (_keys_needed==0);
    }

    inline bool are_keys_set() { return (_keys_set); }
    inline void keys_set(const bool are_set = true) { _keys_set = are_set; }


    // copying allowed
    base_action_t(base_action_t const& rhs)
        : _prvp(rhs._prvp), _xct(rhs._xct), 
          _tid(rhs._tid), _keys_needed(rhs._keys_needed),
          _read_only(rhs._read_only),
          _keys_set(rhs._keys_set),
          _secondary(rhs._secondary)
    { }

    base_action_t& operator=(base_action_t const& rhs);


    // INTERFACE

    // executes action body
    virtual w_rc_t trx_exec()=0;          

    // acquires the required locks in order to proceed
    virtual bool trx_acq_locks()=0;

    // releases acquired locks
    virtual int trx_rel_locks(BaseActionPtrList& readyList, 
                              BaseActionPtrList& promotedList)=0;

    // hook to update the keys for this action
    virtual int trx_upd_keys()=0; 

    // enqueues self on the list of committed actions
    virtual void notify_own_partition()=0; 

    // should give memory back to the atomic trash stack
    virtual void giveback()=0;

#ifdef WORKER_VERBOSE_STATS
    void mark_enqueue();
    double waited();
#endif

}; // EOF: base_action_t


EXIT_NAMESPACE(dora);

#endif /** __DORA_BASE_ACTION_H */

