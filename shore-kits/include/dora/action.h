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

/** @file:   action.h
 *
 *  @brief:  DORA action class
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */


#ifndef __DORA_ACTION_H
#define __DORA_ACTION_H

#include <cstdio>

#include "util.h"

#include "dora/base_action.h"
#include "dora/logical_lock.h"


using namespace shore;

ENTER_NAMESPACE(dora);

template<typename DataType> class partition_t;


/******************************************************************** 
 *
 * @class: action_t
 *
 * @brief: (Template-based) abstract class for the actions
 *
 * @note:  Actions are similar with packets in staged dbs
 *
 ********************************************************************/


template <typename DataType>
class action_t : public base_action_t
{
public:
    
    typedef key_wrapper_t<DataType>            Key;
    //typedef typename PooledVec<Key*>::Type    KeyPtrVec;
    typedef std::vector<Key*>                  KeyPtrVec;

    typedef partition_t<DataType>              Partition;

    typedef KALReq_t<DataType>                 KALReq;
    //typedef typename PooledVec<KALReq>::Type  KALReqVec;
    typedef std::vector<KALReq_t<DataType> >   KALReqVec;

protected:

    // a vector of pointers to keys
    KeyPtrVec  _keys;

    // a vector of requests for keys
    KALReqVec  _requests;

    // pointer to the partition
    Partition*  _partition;

    inline void _act_set(xct_t* axct, const tid_t& atid, rvp_t* prvp, 
                         const int numkeys, 
                         const bool ro=false)
    {
        _base_set(axct,atid,prvp,numkeys,ro);

        assert (numkeys);
        _keys.reserve(numkeys);
        _requests.reserve(1);
    }


public:

    action_t() : 
        base_action_t(), _partition(NULL)
    { }

    virtual ~action_t() { }


    // copying allowed
    action_t(action_t const& rhs) 
        : base_action_t(rhs), 
          _keys(rhs._keys), _requests(rhs._requests), _partition(rhs._partition)
    { assert (0); }

    action_t& operator=(action_t const& rhs) {
        if (this != &rhs) {
            assert (0);
            base_action_t::operator=(rhs);
            _keys = rhs._keys;
            _requests = rhs._requests;
            _partition = rhs._partition;
        }
        return (*this);
    }

    
    // access methods

    KeyPtrVec* keys() { return (&_keys); }    
    KALReqVec* requests() { return (&_requests); }

    inline Partition* get_partition() const { return (_partition); }
    inline void set_partition(Partition* ap) {
        assert (ap); _partition = ap;
    }
 

    // INTERFACE

    virtual w_rc_t trx_exec()=0;

    bool trx_acq_locks() 
    {
        assert (_partition);
        trx_upd_keys();
        return (_partition->acquire(_requests));
    }

    int trx_rel_locks(BaseActionPtrList& readyList, 
                      BaseActionPtrList& promotedList)
    {
        assert (_partition);
        assert (_keys_set); // at this point the keys should already be set (at trx_acq_locks)
        //trx_upd_keys();
        return (_partition->release(this,readyList,promotedList));
    }


    virtual int trx_upd_keys()=0;

    void notify_own_partition() 
    {
        assert (_partition);
        _partition->enqueue_commit(this);
    }

    virtual void giveback()=0;                            


    virtual void setup() 
    {
    }

    virtual void reset() 
    {
        // clear contents
        _keys.erase(_keys.begin(),_keys.end());
        _requests.erase(_requests.begin(),_requests.end());
    }
    
}; // EOF: action_t



EXIT_NAMESPACE(dora);

#endif /** __DORA_ACTION_H */

