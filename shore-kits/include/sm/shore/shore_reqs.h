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

/** @file:  shore_reqs.h
 *
 *  @brief: Structures that represent user requests 
 *
 *  @author Ippokratis Pandis, Feb 2010
 */


#ifndef __SHORE_REQS_H
#define __SHORE_REQS_H


#include "sm_vas.h"
#include "util.h"


ENTER_NAMESPACE(shore);


const int NO_VALID_TRX_ID = -1;

/******************************************************************** 
 *
 * @enum  TrxState
 *
 * @brief Possible states of a transaction
 *
 ********************************************************************/

enum TrxState { UNDEF       = 0x0, 
                UNSUBMITTED = 0x1, 
                SUBMITTED   = 0x2, 
                POISSONED   = 0x4, 
		COMMITTED   = 0x8, 
                ROLLBACKED  = 0x10 
};


/******************************************************************** 
 *
 * @class: trx_result_tuple_t
 *
 * @brief: Class used to represent the result of a transaction
 *
 ********************************************************************/

class trx_result_tuple_t 
{
private:

    TrxState R_STATE;
    int R_ID;
    condex* _notify;
   
public:

    trx_result_tuple_t() { reset(UNDEF, -1, NULL); }

    trx_result_tuple_t(TrxState aTrxState, int anID, condex* apcx = NULL) { 
        reset(aTrxState, anID, apcx);
    }

    ~trx_result_tuple_t() { }

    // @fn copy constructor
    trx_result_tuple_t(const trx_result_tuple_t& t) {
	reset(t.R_STATE, t.R_ID, t._notify);
    }      

    // @fn copy assingment
    trx_result_tuple_t& operator=(const trx_result_tuple_t& t) {        
        reset(t.R_STATE, t.R_ID, t._notify);        
        return (*this);
    }
    
    // @fn equality operator
    friend bool operator==(const trx_result_tuple_t& t, 
                           const trx_result_tuple_t& s) 
    {       
        return ((t.R_STATE == s.R_STATE) && (t.R_ID == s.R_ID));
    }


    // Access methods
    condex* get_notify() const { return (_notify); }
    void set_notify(condex* notify) { _notify = notify; }
    
    int get_id() const { return (R_ID); }
    void set_id(const int aID) { R_ID = aID; }

    TrxState get_state() { return (R_STATE); }
    void set_state(TrxState aState) { 
       assert ((aState >= UNDEF) && (aState <= ROLLBACKED));
       R_STATE = aState;
    }

    void reset(TrxState aTrxState, int anID, condex* notify) {
        // check for validity of inputs
        assert ((aTrxState >= UNDEF) && (aTrxState <= ROLLBACKED));
        assert (anID >= NO_VALID_TRX_ID);

        R_STATE = aTrxState;
        R_ID = anID;
	_notify = notify;
    }
        
}; // EOF: trx_result_tuple_t



/******************************************************************** 
 *
 * @struct: base_request_t
 *
 * @brief:  Base class for the requests
 * 
 ********************************************************************/

struct base_request_t
{
    // trx-specific
    xct_t*              _xct; // Not the owner
    tid_t               _tid;
    int                 _xct_id;
    trx_result_tuple_t  _result;

    base_request_t() 
        : _xct(NULL),_xct_id(-1)
    { }

    base_request_t(xct_t* pxct, const tid_t& atid, const int axctid,
                   const trx_result_tuple_t& aresult)
        : _xct(pxct),_tid(atid),_xct_id(axctid),_result(aresult)
    {
        assert (pxct);
    }

    ~base_request_t() 
    { 
        _xct = NULL;
    }


    inline void set(xct_t* pxct, const tid_t& atid, const int axctid,
                    const trx_result_tuple_t& aresult)
    {
        _xct = pxct;
        _tid = atid;
        _xct_id = axctid;
        _result = aresult;
    }

    inline xct_t* xct() { return (_xct); }
    inline tid_t tid() const { return (_tid); }
    inline int xct_id() const { return (_xct_id); }

    void notify_client();

    lsn_t        _my_last_lsn;
    inline void  set_last_lsn(const lsn_t& alsn) { _my_last_lsn = alsn; }
    inline lsn_t my_last_lsn() { return (_my_last_lsn); }

}; // EOF: base_request_t



/******************************************************************** 
 *
 * @struct: trx_request_t
 *
 * @brief:  Represents the requests in the Baseline system
 * 
 ********************************************************************/

struct trx_request_t : public base_request_t
{
    int                 _xct_type;
    int                 _spec_id; 

    trx_request_t() 
        : base_request_t(), _xct_type(-1),_spec_id(0)
    { }

    trx_request_t(xct_t* pxct, const tid_t& atid, const int axctid,
                  const trx_result_tuple_t& aresult, 
                  const int axcttype, const int aspecid)
        : base_request_t(pxct,atid,axctid,aresult),
          _xct_type(axcttype), _spec_id(aspecid)
    {
    }

    ~trx_request_t() { }  

    void set(xct_t* pxct, const tid_t& atid, const int axctid,
             const trx_result_tuple_t& aresult, 
             const int axcttype, const int aspecid)
    {
        base_request_t::set(pxct,atid,axctid,aresult);
        _xct_type = axcttype;
        _spec_id = aspecid;
    }

    inline int type() const { return (_xct_type); }
    inline void set_type(const int atype) { _xct_type = atype; }
    inline int selectedID() { return (_spec_id); }

}; // EOF: trx_request_t



EXIT_NAMESPACE(shore);

#endif /** __SHORE_REQS_H */

