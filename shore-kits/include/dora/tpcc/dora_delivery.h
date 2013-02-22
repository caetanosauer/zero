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

/** @file:   dora_delivery.h
 *
 *  @brief:  DORA TPC-C DELIVERY
 *
 *  @note:   Definition of RVPs and Actions that synthesize 
 *           the TPC-C Delivery trx according to DORA
 *
 *  @author: Ippokratis Pandis, Jan 2009
 */


#ifndef __DORA_TPCC_DELIVERY_H
#define __DORA_TPCC_DELIVERY_H


#include "dora.h"
#include "workload/tpcc/shore_tpcc_env.h"
#include "dora/tpcc/dora_tpcc.h"

using namespace shore;
using namespace tpcc;


ENTER_NAMESPACE(dora);



//
// RVPS
//
// (1) mid1_del_rvp
// (2) mid2_del_rvp
// (3) final_del_rvp
//



/******************************************************************** 
 *
 * @class: final_del_rvp
 *
 * @brief: Terminal Phase of Delivery
 *
 ********************************************************************/

class final_del_rvp : public terminal_rvp_t
{
private:
    typedef object_cache_t<final_del_rvp> rvp_cache;
    DoraTPCCEnv* _ptpccenv;
    rvp_cache* _cache;
public:
    final_del_rvp() : terminal_rvp_t(), _ptpccenv(NULL), _cache(NULL) { }
    ~final_del_rvp() { _cache=NULL; _ptpccenv=NULL; }

    // access methods
    inline void set(xct_t* axct, const tid_t& atid, const int axctid,
                    trx_result_tuple_t& presult, 
                    DoraTPCCEnv* penv, rvp_cache* pc) 
    { 
        assert (penv);
        _ptpccenv = penv;
        assert (pc);
        _cache = pc;
        _set(penv->db(),penv,axct,atid,axctid,presult,
             DISTRICTS_PER_WAREHOUSE,4*DISTRICTS_PER_WAREHOUSE);
    }
    inline void giveback() { _cache->giveback(this); }    

    // interface
    w_rc_t run();
    void upd_committed_stats(); // update the committed trx stats
    void upd_aborted_stats(); // update the committed trx stats

}; // EOF: final_del_rvp


/******************************************************************** 
 *
 * @class: mid1_del_rvp
 *
 * @brief: Submits the upd_order and upd_orderlines packets, 
 *         Passes the w_id, d_id, and o_id, as well as, the pointer to 
 *         the final_rvp to the next phase
 *
 ********************************************************************/

class mid1_del_rvp : public rvp_t
{
private:
    typedef object_cache_t<mid1_del_rvp> rvp_cache;
    rvp_cache* _cache;
    DoraTPCCEnv* _ptpccenv;
    bool _bWake;    
    // data needed for the next phase
    delivery_input_t _din;
    int _d_id;
    int _o_id;
    final_del_rvp* _final_rvp;
public:
    mid1_del_rvp() : rvp_t(), _cache(NULL), _ptpccenv(NULL), 
                     _o_id(-1) { }
    ~mid1_del_rvp() { _cache=NULL; _ptpccenv=NULL; 
        _o_id = -1; }

    // access methods
    inline void set(xct_t* axct, const tid_t& atid, const int& axctid,
                    trx_result_tuple_t& presult, 
                    const delivery_input_t& din, const bool bWake,
                    DoraTPCCEnv* penv, rvp_cache* pc) 
    { 
        _din = din;
        _bWake = bWake;
        assert (penv);
        _ptpccenv = penv;
        assert (pc);
        _cache = pc;
        _set(axct,atid,axctid,presult,1,1); // 1 intratrx - 1 total actions
    }
    inline void postset(final_del_rvp* prvp, const int d_id)
    {
        _d_id = d_id;

        assert (prvp);
        _final_rvp = prvp;
    }        
                        
    inline void giveback() { _cache->giveback(this); }    

    void set_o_id(const int aoid) { _o_id = aoid; } 

    // the interface
    w_rc_t _run();

}; // EOF: mid1_del_rvp



/******************************************************************** 
 *
 * @class: mid2_del_rvp
 *
 * @brief: Submits the upd_customer packet, 
 *         Passes the w_id, d_id, c_id, and amount, as well as, the pointer to 
 *         the final_rvp to the next phase
 *
 ********************************************************************/

class mid2_del_rvp : public rvp_t
{
private:
    typedef object_cache_t<mid2_del_rvp> rvp_cache;
    rvp_cache* _cache;
    DoraTPCCEnv* _ptpccenv;
    bool _bWake;
    // data needed for the next phase
    delivery_input_t _din;
    int _d_id;
    int _c_id;
    int _amount;
    tatas_lock _amount_lock;
    final_del_rvp* _final_rvp;

public:
    mid2_del_rvp() : rvp_t(), _cache(NULL), _ptpccenv(NULL), 
                     _c_id(-1), _amount(-1) { }
    ~mid2_del_rvp() { _cache=NULL; _ptpccenv=NULL; 
        _c_id = -1; _amount = -1; }

    // access methods
    inline void set(xct_t* axct, const tid_t& atid, const int& axctid,
                    trx_result_tuple_t& presult, 
                    const delivery_input_t& din, const bool bWake,
                    DoraTPCCEnv* penv, rvp_cache* pc) 
    { 
        _din = din;
        _bWake = bWake;
        _ptpccenv = penv;
        assert (pc);
        _cache = pc;
        _set(axct,atid,axctid,presult,2,3); // 2 intratrx - 3 total actions
    }
    inline void postset(const int d_id,final_del_rvp* prvp)
    {
        _d_id = d_id;
        _final_rvp = prvp;
    }
    inline void giveback() { _cache->giveback(this); }    

    void add_amount(const int anamount) { 
        CRITICAL_SECTION(amount_cs, _amount_lock);
        _amount += anamount; 
    } 

    void set_c_id(const int c_id) { _c_id = c_id; }

    // the interface
    w_rc_t _run();

}; // EOF: mid2_del_rvp




//
// ACTIONS
//
// (0) del_action - generic that holds a delivery input
//
// (1) del_nord_del_action
// (2) upd_ord_del_action
// (3) upd_oline_del_action
// (4) upd_cust_del_action
// 


/******************************************************************** 
 *
 * @abstract class: del_action
 *
 * @brief:          Holds a delivery input and a pointer to ShoreTPCCEnv
 *
 ********************************************************************/

class del_action : public range_action_impl<int>
{
protected:
    DoraTPCCEnv*   _ptpccenv;
    delivery_input_t _din;
    int _d_id;

    inline void _del_act_set(xct_t* axct, const tid_t& atid, rvp_t* prvp, 
                             const int keylen, const delivery_input_t& din, 
                             const int did,
                             DoraTPCCEnv* penv) 
    {
        _range_act_set(axct,atid,prvp,keylen); 
        _din = din;
        _d_id = did;
        assert (penv);
        _ptpccenv = penv;
    }

public:    
    del_action() : range_action_impl<int>(), _ptpccenv(NULL) { }
    virtual ~del_action() { }

    virtual w_rc_t trx_exec()=0;    
    virtual void calc_keys()=0; 
    
}; // EOF: del_action


// DEL_NORD_DEL_ACTION
class del_nord_del_action : public del_action
{
private:
    typedef object_cache_t<del_nord_del_action> act_cache;
    act_cache*       _cache;
    mid1_del_rvp* _pmid1_rvp;
public:    
    del_nord_del_action() : del_action() { }
    ~del_nord_del_action() { }
    w_rc_t trx_exec();    
    void calc_keys() {
        _down.push_back(_din._wh_id);
        _down.push_back(_d_id);
    }
    inline void set(xct_t* axct, const tid_t& atid, 
                    mid1_del_rvp* pmid1_rvp, 
                    const delivery_input_t& din,
                    DoraTPCCEnv* penv, act_cache* pc) 
    {
        assert (pmid1_rvp);
        _pmid1_rvp = pmid1_rvp;
        assert (pc);
        _cache = pc;
        _del_act_set(axct,atid,pmid1_rvp,2,din,0,penv);  // key is (WH|D)
    }
    inline void postset(const int d_id)
    {
        _d_id = d_id;
    }
    inline void giveback() { _cache->giveback(this); }    
   
}; // EOF: del_nord_del_action



// UPD_ORD_DEL_ACTION
class upd_ord_del_action : public del_action
{
private:
    typedef object_cache_t<upd_ord_del_action> act_cache;
    act_cache*       _cache;
    mid2_del_rvp* _pmid2_rvp;
    int _o_id;
public:    
    upd_ord_del_action() : del_action() { }
    ~upd_ord_del_action() { }
    w_rc_t trx_exec();    
    void calc_keys() {
        _down.push_back(_din._wh_id);
        _down.push_back(_d_id);
        _down.push_back(_o_id);
        _down.push_back(_o_id);
    }
    inline void set(xct_t* axct, const tid_t& atid, 
                    mid2_del_rvp* pmid2_rvp, 
                    const delivery_input_t& din,
                    DoraTPCCEnv* penv, act_cache* pc) 
    {
        assert (pmid2_rvp);
        _pmid2_rvp = pmid2_rvp;
        assert (pc);
        _cache = pc;
        _del_act_set(axct,atid,pmid2_rvp,3,din,0,penv);  // key is (WH|D|ORD)
    }
    inline void postset(const int d_id, const int o_id)
    {
        _d_id = d_id;
        _o_id = o_id;
    }
    inline void giveback() { _cache->giveback(this); }    
   
}; // EOF: upd_ord_del_action



// UPD_OLINE_DEL_ACTION
class upd_oline_del_action : public del_action
{
private:
    typedef object_cache_t<upd_oline_del_action> act_cache;
    act_cache*       _cache;
    mid2_del_rvp* _pmid2_rvp;
    int _o_id;
public:    
    upd_oline_del_action() : del_action() { }
    ~upd_oline_del_action() { }
    w_rc_t trx_exec();    
    void calc_keys() {
        _down.push_back(_din._wh_id);
        _down.push_back(_d_id);
        _down.push_back(_o_id);
    }
    inline void set(xct_t* axct, const tid_t& atid, 
                    mid2_del_rvp* pmid2_rvp, 
                    const delivery_input_t& din,
                    DoraTPCCEnv* penv, act_cache* pc) 
    {
        assert (pmid2_rvp);
        _pmid2_rvp = pmid2_rvp;
        assert (pc);
        _cache = pc;
        _del_act_set(axct,atid,pmid2_rvp,3,din,0,penv);  // key is (WH|D|ORD)
    }
    inline void postset(const int d_id, const int o_id)
    {
        _d_id = d_id;
        _o_id = o_id;
    }
    inline void giveback() { _cache->giveback(this); }    
   
}; // EOF: upd_oline_del_action



// UPD_CUST_DEL_ACTION
class upd_cust_del_action : public del_action
{
private:
    typedef object_cache_t<upd_cust_del_action> act_cache;
    act_cache*       _cache;
    int _c_id;
    int _amount;
public:    
    upd_cust_del_action() : del_action(), _c_id(-1), _amount(-1) { }
    ~upd_cust_del_action() { _c_id=-1; _amount=-1; }
    w_rc_t trx_exec();    
    void calc_keys() {
        _down.push_back(_din._wh_id);
        _down.push_back(_d_id);
        _down.push_back(_c_id);
    }
    inline void set(xct_t* axct, const tid_t& atid, 
                    rvp_t* prvp,
                    const delivery_input_t& din,
                    DoraTPCCEnv* penv, act_cache* pc) 
    {
        assert (pc);
        _cache = pc;
        _del_act_set(axct,atid,prvp,3,din,0,penv);  // key is (WH|D|C)
    }
    inline void postset(const int d_id, const int c_id, const int amount)
    {
        _d_id = d_id;
        _c_id = c_id;
        _amount = amount;
    }

    inline void giveback() { _cache->giveback(this); }    
   
}; // EOF: upd_cust_del_action



EXIT_NAMESPACE(dora);

#endif /** __DORA_TPCC_DELIVERY_H */

