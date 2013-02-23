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

/** @file:   dora_payment.h
 *
 *  @brief:  DORA TPC-C PAYMENT
 *
 *  @note:   Definition of RVPs and Actions that synthesize 
 *           the TPC-C Payment trx according to DORA
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */


#ifndef __DORA_TPCC_PAYMENT_H
#define __DORA_TPCC_PAYMENT_H


#include "dora.h"
#include "workload/tpcc/shore_tpcc_env.h"
#include "dora/tpcc/dora_tpcc.h"

using namespace shore;
using namespace tpcc;


ENTER_NAMESPACE(dora);



//
// RVPS
//
// (1) midway_pay_rvp
// (2) final_pay_rvp
//


/******************************************************************** 
 *
 * @class: midway_pay_rvp
 *
 * @brief: Submits the history packet, 
 *         Passes the warehouse and district tuples to the next phase
 *
 ********************************************************************/

class midway_pay_rvp : public rvp_t
{
private:
    typedef object_cache_t<midway_pay_rvp> rvp_cache;
    rvp_cache* _cache;
    DoraTPCCEnv* _ptpccenv;
    bool _bWake;
    // data needed for the next phase
    tpcc_warehouse_tuple _awh;
    tpcc_district_tuple  _adist;
public:
    midway_pay_rvp() : rvp_t(), _cache(NULL), _ptpccenv(NULL) { }
    ~midway_pay_rvp() { _cache=NULL; _ptpccenv=NULL; }

    // access methods
    inline void set(xct_t* axct, const tid_t& atid, const int& axctid,
                    trx_result_tuple_t& presult, 
                    const payment_input_t& ppin, const bool bWake,
                    DoraTPCCEnv* penv, rvp_cache* pc) 
    { 
        _pin = ppin;
        _bWake = bWake;
        assert (penv);
        _ptpccenv = penv;
        assert (pc);
        _cache = pc;
        _set(axct,atid,axctid,presult,3,3);
    }
    inline void giveback() { _cache->giveback(this); }    

    payment_input_t      _pin;
    tpcc_warehouse_tuple* wh() { return (&_awh); }
    tpcc_district_tuple* dist() { return (&_adist); }    

    // the interface
    w_rc_t _run();

}; // EOF: midway_pay_rvp



/******************************************************************** 
 *
 * @class: final_pay_rvp
 *
 * @brief: Terminal Phase of Payment
 *
 ********************************************************************/

class final_pay_rvp : public terminal_rvp_t
{
private:
    typedef object_cache_t<final_pay_rvp> rvp_cache;
    DoraTPCCEnv* _ptpccenv;
    rvp_cache* _cache;
public:
    final_pay_rvp() : terminal_rvp_t(), _ptpccenv(NULL), _cache(NULL) { }
    ~final_pay_rvp() { _cache=NULL; _ptpccenv=NULL; }

    // access methods
    inline void set(xct_t* pxct, const tid_t& atid, const int axctid,
                    trx_result_tuple_t& presult, 
                    DoraTPCCEnv* penv, rvp_cache* pc) 
    { 
        assert (penv);
        _ptpccenv = penv;
        assert (pc);
        _cache = pc;
        _set(penv->db(),penv,pxct,atid,axctid,presult,1,4);
    }
    inline void giveback() { _cache->giveback(this); }    

    // interface
    w_rc_t run();
    void upd_committed_stats(); // update the committed trx stats
    void upd_aborted_stats(); // update the committed trx stats

}; // EOF: final_pay_rvp


//
// ACTIONS
//
// (0) pay_action - generic that holds a payment input
//
// (1) upd_wh_pay_action
// (2) upd_dist_pay_action
// (3) upd_cust_pay_action
// (4) ins_hist_pay_action
// 


/******************************************************************** 
 *
 * @abstract class: pay_action
 *
 * @brief:          Holds a payment input and a pointer to ShoreTPCCEnv
 *
 ********************************************************************/

class pay_action : public range_action_impl<int>
{
protected:
    DoraTPCCEnv*   _ptpccenv;
    payment_input_t _pin;

    inline void _pay_act_set(xct_t* axct, const tid_t& atid, rvp_t* prvp, 
                             const int keylen, const payment_input_t& pin, 
                             DoraTPCCEnv* penv) 
    {
        _range_act_set(axct,atid,prvp,keylen); 
        _pin = pin;
        assert (penv);
        _ptpccenv = penv;
    }

public:    
    pay_action() : range_action_impl<int>(), _ptpccenv(NULL) { }
    virtual ~pay_action() { }

    virtual w_rc_t trx_exec()=0;    
    virtual void calc_keys()=0; 
    
}; // EOF: pay_action


// UPD_WH_PAY_ACTION
class upd_wh_pay_action : public pay_action
{
private:
    typedef object_cache_t<upd_wh_pay_action> act_cache;
    act_cache*       _cache;
public:    
    upd_wh_pay_action() : pay_action() { }
    ~upd_wh_pay_action() { }
    w_rc_t trx_exec();    
    midway_pay_rvp* _m_rvp;
    void calc_keys();
    inline void set(xct_t* axct, const tid_t& atid,
                    midway_pay_rvp* prvp, 
                    const payment_input_t& pin,
                    DoraTPCCEnv* penv, act_cache* pc) 
    {
        assert (prvp);
        _m_rvp = prvp;
        assert (pc);
        _cache = pc;
        _pay_act_set(axct,atid,prvp,1,pin,penv);  // key is (WH)
    }
    inline void giveback() { _cache->giveback(this); }    
   
}; // EOF: upd_wh_pay_action


// UPD_DIST_PAY_ACTION
class upd_dist_pay_action : public pay_action
{
private:
    typedef object_cache_t<upd_dist_pay_action> act_cache;
    act_cache*       _cache;
public:   
    upd_dist_pay_action() : pay_action() { }
    ~upd_dist_pay_action() { }
    w_rc_t trx_exec();    
    midway_pay_rvp* _m_rvp;
    void calc_keys();
    inline void set(xct_t* axct, const tid_t& atid, midway_pay_rvp* amrvp, 
                    const payment_input_t& pin,
                    DoraTPCCEnv* penv, act_cache* pc) 
    {
        assert (amrvp);
        _m_rvp = amrvp;
        assert (pc);
        _cache = pc;
        _pay_act_set(axct,atid,amrvp,2,pin,penv);  // key is (WH|DIST)
    }
    inline void giveback() { _cache->giveback(this); }    

}; // EOF: upd_wh_pay_action


// UPD_CUST_PAY_ACTION
class upd_cust_pay_action : public pay_action
{
private:
    typedef object_cache_t<upd_cust_pay_action> act_cache;
    act_cache*       _cache;
public:    
    upd_cust_pay_action() : pay_action() { }
    ~upd_cust_pay_action() { }
    w_rc_t trx_exec();   
    midway_pay_rvp* _m_rvp;
    void calc_keys();
    inline void set(xct_t* axct, const tid_t& atid, 
                    midway_pay_rvp* prvp, 
                    const payment_input_t& pin,
                    DoraTPCCEnv* penv, act_cache* pc) 
    {
        assert (prvp);
        _m_rvp = prvp;
        assert (pc);
        _cache = pc;
        _pay_act_set(axct,atid,prvp,2,pin,penv);  // key is (WH|DIST)
    }
    inline void giveback() { _cache->giveback(this); }    

}; // EOF: upd_cust_pay_action


// INS_HIST_PAY_ACTION
class ins_hist_pay_action : public pay_action
{
private:
    typedef object_cache_t<ins_hist_pay_action> act_cache;
    act_cache*       _cache;
public:    
    ins_hist_pay_action() : pay_action() { }
    ~ins_hist_pay_action() { }
    w_rc_t trx_exec();    
    tpcc_warehouse_tuple _awh;
    tpcc_district_tuple _adist;    
    void calc_keys();
    inline void set(xct_t* axct, const tid_t& atid, 
                    rvp_t* prvp, 
                    const payment_input_t& pin,
                    DoraTPCCEnv* penv, act_cache* pc) 
    {
        assert (pc);
        _cache = pc;
        _pay_act_set(axct,atid,prvp,1,pin,penv);  // key is (HIST)
    }
    inline void postset(const tpcc_warehouse_tuple& awh,
                        const tpcc_district_tuple& adist)
    {
        _awh = awh;
        _adist = adist;
    }
    inline void giveback() { _cache->giveback(this); }    

}; // EOF: ins_hist_pay_action



EXIT_NAMESPACE(dora);

#endif /** __DORA_TPCC_PAYMENT_H */

