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

/** @file:   range_action.h
 *
 *  @brief:  Encapsulation of each range-based actions
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */


#ifndef __DORA_RANGE_ACTION_H
#define __DORA_RANGE_ACTION_H


#include "dora/action.h"

using namespace shore;

ENTER_NAMESPACE(dora);



/******************************************************************** 
 *
 * @class: range_action_t
 *
 * @brief: Abstract template-based class for range-based actions
 *
 * @note:  The vector has only two members: {down, up}
 *         Vector.Front = down, Vector.Back = up
 *
 ********************************************************************/

template <class DataType>
class range_action_impl : public action_t<DataType>
{
public:
    typedef key_wrapper_t<DataType>  Key;
    typedef action_t<DataType>       Action;
    typedef KALReq_t<DataType>       KALReq;

protected:

    // the range of keys
    bool _isRange;
    void set_is_range() { _isRange = true; }

    Key _down;
    vector<Key> _key_list;

    inline void _range_act_set(xct_t* axct, const tid_t& atid, rvp_t* prvp, 
                               const int keylen) 
    {
        // the range action has two keys: 
        // the low and hi value that determine the range
        Action::_act_set(axct,atid,prvp,2); 
        assert (keylen);
        _down.reserve(keylen);
    }

public:

    // By default, the action is supposed to access a single Key (point not range access)

    range_action_impl()
        : Action(), _isRange(false)
    { }

    range_action_impl(const range_action_impl& rhs)
        : Action(rhs), _isRange(rhs._isRange), 
          _down(rhs._down), _key_list(rhs._key_list)
    { assert (0); }

    range_action_impl operator=(const range_action_impl& rhs)
    {
        if (this != &rhs) {
            assert (0); // IP: TODO check
            Action::operator=(rhs);
            _isRange = rhs._isRange;
            _down = rhs._down;
            _key_list = rhs._key_list;
        }
        return (*this);
    }

    virtual ~range_action_impl() { }    

    // INTERFACE 

    virtual void calc_keys()=0; 

    virtual w_rc_t trx_exec()=0;


    // There are two types of actions that inherit from this class:
    // 
    // Point accesses - Need to lock only one Key
    //
    // Range accesses - need to lock multiple Keys from this partition
    // In that case the population of the _requests is done by the real 
    // action, at the calc_keys() 
    virtual int trx_upd_keys()
    {
        if (base_action_t::are_keys_set()) return (1); // if already set do not recalculate

        // The hook for the real action to populate the keys needed
        calc_keys();

        eDoraLockMode req_lm = DL_CC_EXCL;
        if (base_action_t::is_read_only()) req_lm = DL_CC_SHARED;

        if (_isRange) {
            // Range access - need to lock a list of Keys

            // !!! WARNING WARNING WARNING !!!
            //
            // No element should be added to the _key_list *after* this point.
            // We are using the address of the element at the vector. If
            // we add any entry then the address reference may be invalidated
            // and Bad Things WILL Happen (tm).
            //
            // !!! WARNING WARNING WARNING !!!

            uint range = _key_list.size();
            base_action_t::setkeys(range);            
            for (uint i=0; i<range; i++) {
                KALReq akr(this,req_lm,&_key_list[i]);
                Action::_requests.push_back(akr);
            }
            base_action_t::keys_set(range);
        }
        else {
            // Point access - need to lock only one Key
            Action::_keys.push_back(&_down);
            base_action_t::setkeys(1); // It needs only 1 key
            KALReq akr(this,req_lm,&_down);
            Action::_requests.push_back(akr);
            base_action_t::keys_set(true); // The 1 key it needed is set
        }
        return (0);
    }

    virtual void giveback()=0;

    virtual void init() { }

    virtual void reset()
    {
        Action::reset();

        // clear contents
        _down.reset();
        _key_list.clear();
    }

}; // EOF: range_action_impl



EXIT_NAMESPACE(dora);

#endif /** __DORA_RANGE_ACTION_H */

