/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
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

/*<std-header orig-src='shore' incl-file-exclusion='PROLOGUE_H'>

 $Id: prologue.h,v 1.56 2010/12/08 17:37:43 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef PROLOGUE_H
#define PROLOGUE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

// arguments:
// name of function in which this is used
//
// expected state: 
//  not_in_xct 
//  in_xct 
//  can_be_in_xct
//  commitable_xct
//  abortable_xct
//
// pin_cnt_change : when the prologue_rc_t is destructed, there
//  can be up to this many extra pins lingering.
#define SM_PROLOGUE_RC(func_name, is_in_xct, constrnt, pin_cnt_change)         \
    FUNC(func_name);                                                           \
    prologue_rc_t prologue(prologue_rc_t::is_in_xct,                           \
                           prologue_rc_t::constrnt, (pin_cnt_change));         \
    if (prologue.error_occurred()) return prologue.rc();

// NOTE : these next two make sense only for scan iterators; the 
// _error_occurred are local to the scan_i.
// INIT_SCAN_PROLOGUE_RC is for the constructors
// NOTE: 2nd arg readwrite has to be fully specified by caller
// so that it can be conditional  (see scan.cpp)
#define INIT_SCAN_PROLOGUE_RC(func_name, readwrite, pin_cnt_change)            \
    FUNC(func_name);                                                           \
    prologue_rc_t prologue(prologue_rc_t::in_xct,                              \
            readwrite, (pin_cnt_change));                                      \
    if (prologue.error_occurred()) {                                           \
        _error_occurred = prologue.rc();                                       \
    }

// SCAN_METHOD_PROLOGUE_RC is for the methods, which return an rc 
#define SCAN_METHOD_PROLOGUE(func_name, constraint, pin_cnt_change)            \
    FUNC(func_name);                                                           \
    prologue_rc_t prologue(prologue_rc_t::in_xct,                              \
            prologue_rc_t::constraint,   (pin_cnt_change));                    \
    if (prologue.error_occurred()) {                                           \
        _error_occurred = prologue.rc();                                       \
         return w_rc_t(_error_occurred);                                       \
    }                                                                          \
    if(_error_occurred.is_error()) {                                           \
         return w_rc_t(_error_occurred);                                       \
    }


class prologue_rc_t {
public:
    enum xct_state_t {
        in_xct,          // must be active and not prepared
        commitable_xct, // must be prepared if external, 
                        // else must be active or prepared
        not_in_xct,     // may not have tx, regardless of state
        can_be_in_xct,  // in or not -- no test for active or prepared
        abortable_xct   // active or prepared 
    };
    enum xct_constraint_t {
        read_only,      // this method/function is read-only and so can have
                        // multiple threads attached to this xct
        read_write      // this method/function may perform updates 
                        // and so if in xct, must be a single-threaded xct
    };
 
    prologue_rc_t(xct_state_t is_in_xct, 
                 xct_constraint_t, 
                 int pin_cnt_change);
    ~prologue_rc_t();
    void no_longer_in_xct();
    bool error_occurred() const {return _rc.is_error();}
    rc_t   rc() {return _rc;}

private:
    xct_state_t       _xct_state;
    xct_t *           _the_xct; // attached xct when we constructed prologue
    xct_constraint_t  _constraint;
    int               _pin_cnt_change;
    rc_t              _rc;
    xct_log_switch_t* _toggle;
    xct_t*            _victim;
    // FRJ: malloc is too expensive so we cheat. Allocate space locally
    // and use placement-new. 32 bytes should be way plenty.
    enum        { SIZEOF_TOGGLE=32 };
    long        __toggle_data[SIZEOF_TOGGLE/sizeof(long)];
};

/*
 * Install the implementation code in sm.cpp
 */
#if defined(SM_C) 

prologue_rc_t::prologue_rc_t(
        xct_state_t is_in_xct,  // expected state
        xct_constraint_t constraint,  // what this method does
        int pin_cnt_change) :
            _xct_state(is_in_xct), 
            _constraint(constraint), 
            _pin_cnt_change(pin_cnt_change),
            _toggle(0), _victim(0)
{
    w_assert1(!me()->is_in_sm()); // avoid nesting prologues
    _the_xct = xct();

    bool        check_log = true;
    bool        check_1thread = false;

    switch (_xct_state) 
    {
    case in_xct:
        if ( (!_the_xct) 
            || (_the_xct->state() != smlevel_1::xct_active)) {

            _rc = rc_t(__FILE__, __LINE__, 
                    (_the_xct && _the_xct->state() == smlevel_1::xct_prepared)?
                    smlevel_0::eISPREPARED :
                    smlevel_0::eNOTRANS
                );
            check_log = false;
        } 
        break;

    case commitable_xct: {
        // called from commit and chain
        // If this tx is participating in an external 2pc,
        // it MUST be prepared before commit.  
        //
        // Furthermore, we cannot have more than one thread attached.
        int        error = 0;
        if ( ! _the_xct  ) {
            error = smlevel_0::eNOTRANS;
        } else if (_the_xct->is_extern2pc() && 
                (_the_xct->state() != smlevel_1::xct_prepared) ) 
        {
            error = smlevel_0::eNOTPREPARED;
        } else if( (_the_xct->state() != smlevel_1::xct_active) 
                && (_the_xct->state() != smlevel_1::xct_prepared) 
                ) {
            error = smlevel_0::eNOTRANS;
        }

        if(error) {
            _rc = rc_t(__FILE__, __LINE__, error);
            check_log = false;
            no_longer_in_xct(); // to avoid choking in destructor.
        }
        check_1thread = true;
        break;
    }

    case abortable_xct:
        // do not special-case external2pc transactions -- they
        // can abort any time, since this is presumed-abort protocol
        //
        // But we must be sure there's only one thread attached.
        if (! _the_xct || (_the_xct->state() != smlevel_1::xct_active && 
                _the_xct->state() != smlevel_1::xct_prepared)) {
            _rc = rc_t(__FILE__, __LINE__, smlevel_0::eNOTRANS);
            check_log = false;
        }
        check_1thread = true;
        break;

    case not_in_xct:
        if (_the_xct) {
            _rc = rc_t(__FILE__, __LINE__, smlevel_0::eINTRANS);
        }
        check_log = false;
        break;

    case can_be_in_xct:
        // do nothing
        break;

    default:
        W_FATAL(smlevel_0::eINTERNAL);
        break;
    }

#if W_DEBUG_LEVEL > 2
    me()->mark_pin_count();
    me()->in_sm(true);
#endif 

    if(_xct_state != not_in_xct) {
        // FRJ: use placement new to avoid malloc
        w_assert1(sizeof(__toggle_data) >= sizeof(xct_log_switch_t));
        _toggle = new(__toggle_data) xct_log_switch_t(smlevel_0::ON);
    }

    // let the first error found prevail
    if(_rc.is_error())  {
        return;
    }

    // Make sure we don't have multiple update threads attached.
    // Also, Make note that this is an update thread.
    if(_the_xct && (_constraint == read_write))  {
        int num = _the_xct->attach_update_thread(); 
        if(num > 1) {
            _rc =  RC(smlevel_0::eTWOUTHREAD);
            return; //return this error
        }
        check_1thread = false;
    }
    else check_log = false;

    if(check_1thread) 
    {
        _rc = _the_xct->check_one_thread_attached();
        // let the first error found prevail
        if(_rc.is_error())  {
            return;
        }
    }


    if(check_log && !smlevel_0::in_recovery() ) 
    {
        _rc = xct_log_warn_check_t::check(_victim);
        // if(_rc.is_error())  {
            // Caller will check this and return the error.
        // }
    }
}


prologue_rc_t::~prologue_rc_t()
{
    if(_the_xct && (_constraint == read_write))  {
        _the_xct->detach_update_thread();
    }

#if W_DEBUG_LEVEL > 2
    me()->check_pin_count(_pin_cnt_change);
    me()->in_sm(false);
#endif 
    // FRJ: destruct manually becuase we used placement new
    if(_toggle) { _toggle->~xct_log_switch_t(); }

    if(_victim) {
        sm_stats_info_t * stats = _victim->is_instrumented() ? 
                _victim->steal_stats() : 0;
        // should always be able to abort.
        W_COERCE(_victim->abort());
        INC_TSTAT(log_warn_abort_cnt);
        // delete _victim;
        xct_t::destroy_xct(_victim);
        delete stats; 
        _victim = 0;
    }
}

inline void
prologue_rc_t::no_longer_in_xct()
{
    _xct_state = not_in_xct;
    _the_xct = NULL;
}

#endif /* SM_C */

/*<std-footer incl-file-exclusion='PROLOGUE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
