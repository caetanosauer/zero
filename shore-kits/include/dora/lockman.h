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

/** @file:   lockman.h
 *
 *  @brief:  Lock manager for DORA partitions
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */

#ifndef __DORA_LOCK_MAN_H
#define __DORA_LOCK_MAN_H

#include <cstdio>
#include <map>
#include <vector>
#include <deque>

#include "util/stl_pooled_alloc.h"

#include "dora/dora_error.h"
#include "dora/logical_lock.h"
#include "dora/action.h"


using std::vector;

ENTER_NAMESPACE(dora);

typedef base_action_t*                  BaseActionPtr;
typedef std::vector<base_action_t*>     BaseActionPtrList;
typedef BaseActionPtrList::iterator     BaseActionPtrIt;

/******************************************************************** 
 *
 * @class: lock_man_t
 *
 * @brief: Lock manager for the locks of a partition
 *
 * @note:  The lock manager consists of a
 *         - A map for the status of logical locks (llst_map_t)
 *         - A bi-map for associating trxs with Keys
 *
 *
 ********************************************************************/


template<class DataType>
struct lock_man_t
{
public:

    typedef action_t<DataType>       Action;

    typedef key_wrapper_t<DataType>  Key;

    typedef KeyLockMap<DataType>     KeyLLMap;
    typedef std::pair<Key,LogicalLock>        KeyLLPair;

    typedef std::_Rb_tree_node<KeyLLPair>     KeyLLMapNode;

    typedef KALReq_t<DataType>      KALReq;
    //typedef typename PooledVec<KALReq>::Type KALReqVec;
    typedef std::vector<KALReq>               KALReqVec;
    typedef typename KALReqVec::iterator      KALReqIt;

private:

    // data
    guard<KeyLLMap>     _key_ll_m;   // map of keys to logical locks 

public:    

    lock_man_t(const int keyEstimation) 
    { 
        _key_ll_m = new KeyLLMap(keyEstimation);
    }   
    
    ~lock_man_t() { }


    // @fn:     acquire_all()
    // @brief:  Tries to acquire all the locks from a list of keys on behalf of a trx.
    // @return: (true) on success
    inline bool acquire_all(KALReqVec& akalrvec) 
    {
        bool bResult = true;
        int i=0;
        for (KALReqIt it=akalrvec.begin(); it!=akalrvec.end(); ++it) {
            // request to acquire all the locks for this partition
            ++i;
            if (!_key_ll_m->acquire(*it)) {

                // !!! WARNING WARNING WARNING !!!
                //
                // It works correctly only when it wants to acquire a single Lock.
                // Not correct when it wants to acquire a list of Locks.
                //
                //
                // It should ignore that it failed to acquire one, keep on
                // trying to acquire the rest of the list, and then come back on
                // the not acquired one. Also, the resume algorithm needs to
                // be modified. That is, when an action is promoted, because a
                // lock it was waiting for got released, it should make sure that
                // all the keys are now acquired.
                //
                // !!! WARNING WARNING WARNING !!!

                // if a key cannot be acquired, return false
                TRACE( TRACE_TRX_FLOW, "Cannot acquire for (%d)\n", 
                       ((*it).tid()->get_lo()));
                bResult = false;
            }
        }
        assert(i); // makes sure that the action tries to acquire at least one key
        return (bResult);
    }


    // @fn:     release_all(Action*,BaseActionPtrList&,BaseActionPtrList&)
    // @brief:  Releases all the LLs help by a particular trx
    // @return: Returns a list of actions that are ready to run
    inline int release_all(Action* paction, 
                           BaseActionPtrList& readyList, 
                           BaseActionPtrList& promotedList) 
    {
        assert (paction);
        assert (readyList.empty() && promotedList.empty());
        TRACE( TRACE_TRX_FLOW, "Releasing (%d)\n", paction->tid().get_lo());

        // 1. Release and drop all the keys of this trx
        for (KALReqIt it=paction->requests()->begin(); it!=paction->requests()->end(); ++it) {
            Key* pkey = (*it).key();
            _key_ll_m->release(*pkey,paction,promotedList);
        }

        // 2. Iterate over all the promoted actions
        BaseActionPtr ap = NULL;
        for (uint i=0; i<promotedList.size(); ++i) {

            // if they are ready to execute return them as ready to the worker 
            ap = promotedList[i];
            ap->gotkeys(1);
            if (ap->is_ready()) {
                TRACE( TRACE_TRX_FLOW, "(%d) ready (%d)\n", 
                       paction->tid().get_lo(), ap->tid().get_lo());
                readyList.push_back(ap);
            }
        }                
        return (0);
    }



    //// Debugging ////

    uint keystouched() const { return (_key_ll_m->keystouched()); }

    void reset() { _key_ll_m->reset(); }
    void dump() { _key_ll_m->dump(); }

    bool is_clean(vector<xct_t*>& toabort) { 
        return (_key_ll_m->is_clean(toabort)); 
    }

}; // EOF: struct lock_man_t


EXIT_NAMESPACE(dora);

#endif /* __DORA_LOCK_MAN_H */
