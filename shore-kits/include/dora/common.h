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

/** @file:   common.h
 *
 *  @brief:  Common constants, defines and enums
 *
 *  @author: Ippokratis Pandis, Oct 2008
 *
 */

#ifndef __DORA_COMMON_H
#define __DORA_COMMON_H

#include "util.h"

#include "tls.h"

#include <vector>



ENTER_NAMESPACE(dora);


using std::vector;
using std::pair;



// CONSTANTS

// The minimum number of keys the need to be touched in order the corresponding
// lock manager to clear the entries in the map before each new run
const uint D_MIN_KEYS_TOUCHED     = 10000;     


const uint DF_CPU_RANGE           = 8;         // cpu range for each table
const uint DF_CPU_STARTING        = 2;         // starting cpu
const uint DF_CPU_STEP_TABLES     = 16;        // next-cpu among different tables
const uint DF_CPU_STEP_PARTITIONS = 2;         // next-cpu among partitions of the same table

const uint DF_NUM_OF_PARTITIONS_PER_TABLE = 1; // number of partitions per table
const uint DF_NUM_OF_STANDBY_THRS         = 0; // IP: assuming main-memory 



// ENUMS


/******************************************************************** 
 *
 * @enum:  eDoraLockMode
 *
 * @brief: Possible lock types in DORA
 *
 *         - DL_CC_NOLOCK : unlocked
 *         - DL_CC_SHARED : shared lock
 *         - DL_CC_EXCL   : exclusive lock
 *
 ********************************************************************/

enum eDoraLockMode {
    DL_CC_NOLOCK    = 0,
    DL_CC_SHARED    = 1,
    DL_CC_EXCL      = 2,

    DL_CC_MODES     = 3 // @careful: Not an actual lock mode
};

// static eDoraLockMode DoraLockModeArray[DL_CC_MODES] =
//     { DL_CC_NOLOCK, DL_CC_SHARED, DL_CC_EXCL };



/******************************************************************** 
 *
 * Lock compatibility Matrix
 *
 ********************************************************************/

extern int DoraLockModeMatrix[DL_CC_MODES][DL_CC_MODES];

// static int DoraLockModeMatrix[DL_CC_MODES][DL_CC_MODES] = { {1, 1, 1},
//                                                             {1, 1, 0},
//                                                             {1, 0, 0} };





/******************************************************************** 
 *
 * @enum:  eActionDecision
 *
 * @brief: Possible decision of an action
 *
 * @note:  Abort - if something goes wrong with own action
 *         Die - if some other action (of the same trx) decides to abort
 *         
 *         Propagate - The xct has completed, but the client and the 
 *                     other workers need to be notified
 *
 ********************************************************************/

enum eActionDecision { AD_UNDECIDED = 0x1, 
                       AD_ABORT =     0x2, 
                       AD_DEADLOCK =  0x4, 
                       AD_COMMIT =    0x8, 
                       AD_DIE =       0x10,
                       AD_PROPAGATE = 0x20}
;



/******************************************************************** 
 *
 * @enum:  dora_types_t
 *
 * @brief: Ther eare different types of the DORA system:
 *         DT_PLAIN       - normal DORA
 *         DT_PLP         - for all PLPs
 *
 ********************************************************************/

enum dora_types_t { DT_PLAIN      = 0x1,
                    DT_PLP        = 0x2
};




/******************************************************************** 
 *
 * @enum:  ePartitionPolicy
 *
 * @brief: Possible types of a data partition
 *
 *         - PP_RANGE  : range partitioning
 *         - PP_HASH   : hash-based partitioning
 *         - PP_PREFIX : prefix-based partitioning (predicate)
 *
 ********************************************************************/

enum ePartitionPolicy { PP_UNDEF  = 0x1, 
                        PP_RANGE  = 0x2, 
                        PP_HASH   = 0x4, 
                        PP_PREFIX = 0x8
};




const int KEYPTR_PER_ACTION_POOL_SZ = 60;
const int KALREQ_PER_ACTION_POOL_SZ = 30;
const int DT_PER_ACTION_POOL_SZ = 360;


typedef Pool* PoolPtr;

const int ACTIONS_PER_RVP_POOL_SZ = 30; // should be comparable with batch_sz



// RVP cache

#define DECLARE_RVP_CACHE(Type)                         \
    struct Type##_cache   {                             \
        guard< object_cache_t<Type> > _cache;           \
        Type##_cache() {                                \
            _cache = new object_cache_t<Type>(); }      \
        ~Type##_cache() { _cache.done(); } };


#define DECLARE_TLS_RVP_CACHE(Type)              \
    DECLARE_RVP_CACHE(Type);                     \
    DECLARE_TLS(Type##_cache,my_##Type##_cache);


// ACTION cache

#define DECLARE_ACTION_CACHE(Type,Datatype)                             \
    struct Type##_cache  {                                              \
        guard<object_cache_t<Type> > _cache;                            \
        guard<Pool> _keyPtrPool;                                        \
        guard<Pool> _kalReqPool;                                        \
        guard<Pool> _dtPool;                                            \
        Type##_cache() {                                                \
            _cache = new object_cache_t<Type>(); }                      \
        ~Type##_cache() { _cache.done(); } };



#define DECLARE_TLS_ACTION_CACHE(Type,Datatype)      \
    DECLARE_ACTION_CACHE(Type,Datatype);             \
    DECLARE_TLS(Type##_cache,my_##Type##_cache);




// Table Partitions

#define DECLARE_DORA_PARTS(abbrv)                                       \
    guard<irpTableImpl> _##abbrv##_irpt;                                \
    uint _parts_##abbrv;                                                \
    inline irpTableImpl* abbrv() { return (_##abbrv##_irpt.get()); }


#define GENERATE_DORA_PARTS(abbrv,tablename)                            \
    { _##abbrv##_irpt = new irpTableImpl(this, tablename##_desc(), dtype(), icpu, _cpu_range, abbrv##_KEY_EST); \
    if (!_##abbrv##_irpt) {                                             \
        TRACE( TRACE_ALWAYS, "Problem in creating irp-table\n");        \
        assert (0); return (de_GEN_TABLE); }                            \
    _irptp_vec.push_back(_##abbrv##_irpt.get());                        \
    icpu = _next_cpu(icpu, _##abbrv##_irpt, _cpu_table_step);}



////////////////////////////////////////////////////////////////////////////////
//
// DORA TRANSACTIONS
// 
////////////////////////////////////////////////////////////////////////////////


#define DECLARE_DORA_TRX(trx)                                           \
    w_rc_t dora_##trx(const int xct_id, trx_result_tuple_t& atrt,       \
                      trx##_input_t& in, const bool bWake);             \
    w_rc_t dora_##trx(const int xct_id, trx_result_tuple_t& atrt,       \
                      const int specificID, const bool bWake)


#define DEFINE_DORA_WITHOUT_INPUT_TRX_WRAPPER(cname,trx)                \
    w_rc_t cname::dora_##trx(const int xct_id, trx_result_tuple_t& atrt, \
                             const int specificID, const bool bWake) {  \
        trx##_input_t in = create_##trx##_input(_scaling_factor, specificID); \
        return (dora_##trx(xct_id, atrt, in, bWake)); }




#define DECLARE_ALTER_DORA_TRX(trxlid,trximpl)				\
    w_rc_t dora_##trximpl(const int xct_id, trx_result_tuple_t& atrt,	\
			  trxlid##_input_t& in, const bool bWake);	\
    w_rc_t dora_##trximpl(const int xct_id, trx_result_tuple_t& atrt,	\
			  const int specificID, const bool bWake)

#define DEFINE_ALTER_DORA_WITHOUT_INPUT_TRX_WRAPPER(cname,trxlid,trximpl) \
    w_rc_t cname::dora_##trximpl(const int xct_id, trx_result_tuple_t& atrt, \
				 const int specificID, const bool bWake) { \
        trxlid##_input_t in = create_##trxlid##_input(_scaling_factor, specificID); \
        return (dora_##trximpl(xct_id, atrt, in, bWake)); }





////////////////////////////////////////////////////////////////////////////////
//
// RVP & ACTION GENERATORS (used at Dora*Env)
// 
////////////////////////////////////////////////////////////////////////////////





// Midway RVP without previous actions transfer

#define DECLARE_DORA_MIDWAY_RVP_GEN_FUNC(rvpname,inputname)             \
    rvpname* new_##rvpname(xct_t* axct, const tid_t& atid, const int axctid, \
                           trx_result_tuple_t& presult, const inputname& in, const bool bWake)


#define DEFINE_DORA_MIDWAY_RVP_GEN_FUNC(rvpname,inputname,classname)    \
    DECLARE_TLS_RVP_CACHE(rvpname);                                     \
    rvpname* classname::new_##rvpname(xct_t* axct, const tid_t& atid, const int axctid, \
                                      trx_result_tuple_t& presult, const inputname& in, \
                                      const bool bWake) {               \
        rvpname* myrvp = my_##rvpname##_cache->_cache->borrow();        \
        assert (myrvp);                                                 \
        myrvp->set(axct,atid,axctid,presult,in,bWake,this,my_##rvpname##_cache->_cache.get()); \
        return (myrvp); }



// Dynamic Midway RVP without previous actions transfer

#define DECLARE_DORA_MIDWAY_DYNAMIC_RVP_GEN_FUNC(rvpname,inputname)     \
    rvpname* new_##rvpname(xct_t* axct, const tid_t& atid, const int axctid, \
                           trx_result_tuple_t& presult, const inputname& in, \
                           const int intratrx, const int total, const bool bWake)


#define DEFINE_DORA_MIDWAY_DYNAMIC_RVP_GEN_FUNC(rvpname,inputname,classname) \
    DECLARE_TLS_RVP_CACHE(rvpname);                                     \
    rvpname* classname::new_##rvpname(xct_t* axct, const tid_t& atid, const int axctid, \
                                      trx_result_tuple_t& presult, const inputname& in, \
                                      const int intratrx, const int total, \
                                      const bool bWake) {               \
        rvpname* myrvp = my_##rvpname##_cache->_cache->borrow();        \
        assert (myrvp);                                                 \
        myrvp->set(axct,atid,axctid,presult,in,bWake,this,              \
                   my_##rvpname##_cache->_cache.get(),                  \
                   intratrx,total);                                     \
        return (myrvp); }



// Midway RVP with previous actions transfer

#define DECLARE_DORA_MIDWAY_RVP_WITH_PREV_GEN_FUNC(rvpname,inputname)   \
    rvpname* new_##rvpname(xct_t* axct, const tid_t& atid, const int axctid, \
                           trx_result_tuple_t& presult, const inputname& in, \
                           baseActionsList& actions, const bool bWake)

#define DEFINE_DORA_MIDWAY_RVP_WITH_PREV_GEN_FUNC(rvpname,inputname,classname)    \
    DECLARE_TLS_RVP_CACHE(rvpname);                                     \
    rvpname* classname::new_##rvpname(xct_t* axct, const tid_t& atid, const int axctid, \
                                      trx_result_tuple_t& presult, const inputname& in, \
                                      baseActionsList& actions, const bool bWake) { \
        rvpname* myrvp = my_##rvpname##_cache->_cache->borrow();        \
        assert (myrvp);                                                 \
        myrvp->set(axct,atid,axctid,presult,in,bWake,this,my_##rvpname##_cache->_cache.get()); \
        myrvp->copy_actions(actions);                                   \
        return (myrvp); }




// Final RVP without previous actions transfer
// @note: the final rvps do not need an input

#define DECLARE_DORA_FINAL_RVP_GEN_FUNC(rvpname)                        \
    rvpname* new_##rvpname(xct_t* axct,                                 \
                           const tid_t& atid, const int axctid,         \
                           trx_result_tuple_t& presult)


#define DEFINE_DORA_FINAL_RVP_GEN_FUNC(rvpname,classname)               \
    DECLARE_TLS_RVP_CACHE(rvpname);                                     \
    rvpname* classname::new_##rvpname(xct_t* axct, const tid_t& atid, const int axctid, \
                                      trx_result_tuple_t& presult) {    \
        rvpname* myrvp = my_##rvpname##_cache->_cache->borrow();        \
        assert (myrvp);                                                 \
        myrvp->set(axct,atid,axctid,presult,this,my_##rvpname##_cache->_cache.get()); \
        return (myrvp); }



// Final RVP with previous actions transfer

#define DECLARE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(rvpname)              \
    rvpname* new_##rvpname(xct_t* axct,                                 \
                           const tid_t& atid, const int axctid,         \
                           trx_result_tuple_t& presult, baseActionsList& actions)


#define DEFINE_DORA_FINAL_RVP_WITH_PREV_GEN_FUNC(rvpname,classname)     \
    DECLARE_TLS_RVP_CACHE(rvpname);                                     \
    rvpname* classname::new_##rvpname(xct_t* axct, const tid_t& atid, const int axctid, \
                                      trx_result_tuple_t& presult, baseActionsList& actions) { \
        rvpname* myrvp = my_##rvpname##_cache->_cache->borrow();        \
        assert (myrvp);                                                 \
        myrvp->set(axct,atid,axctid,presult,this,my_##rvpname##_cache->_cache.get()); \
        myrvp->copy_actions(actions);                                   \
        return (myrvp); }



// Dynamic Final RVP w/o previous actions

#define DECLARE_DORA_FINAL_DYNAMIC_RVP_GEN_FUNC(rvpname)                \
    rvpname* new_##rvpname(xct_t* axct,                                 \
                           const tid_t& atid, const int axctid,         \
                           trx_result_tuple_t& presult,                 \
                           const int intratrx)


#define DEFINE_DORA_FINAL_DYNAMIC_RVP_GEN_FUNC(rvpname,classname)       \
    DECLARE_TLS_RVP_CACHE(rvpname);                                     \
    rvpname* classname::new_##rvpname(xct_t* axct, const tid_t& atid, const int axctid, \
                                      trx_result_tuple_t& presult,      \
                                      const int intratrx) {             \
        rvpname* myrvp = my_##rvpname##_cache->_cache->borrow();        \
        assert (myrvp);                                                 \
        myrvp->set(axct,atid,axctid,presult,this,                       \
                   my_##rvpname##_cache->_cache.get(),                  \
                   intratrx,intratrx);                                  \
        return (myrvp); }



// Dynamic Final RVP with previous actions transfer

#define DECLARE_DORA_FINAL_DYNAMIC_RVP_WITH_PREV_GEN_FUNC(rvpname)      \
    rvpname* new_##rvpname(xct_t* axct, const tid_t& atid, const int axctid, \
                           trx_result_tuple_t& presult,                 \
                           const int intratrx, const int total,         \
                           baseActionsList& actions)


#define DEFINE_DORA_FINAL_DYNAMIC_RVP_WITH_PREV_GEN_FUNC(rvpname,classname) \
    DECLARE_TLS_RVP_CACHE(rvpname);                                     \
    rvpname* classname::new_##rvpname(xct_t* axct, const tid_t& atid, const int axctid, \
                                      trx_result_tuple_t& presult,      \
                                      const int intratrx, const int total, \
                                      baseActionsList& actions) {       \
        rvpname* myrvp = my_##rvpname##_cache->_cache->borrow();        \
        assert (myrvp);                                                 \
        myrvp->set(axct,atid,axctid,presult,this,                       \
                   my_##rvpname##_cache->_cache.get(),                  \
                   intratrx,total);                                     \
        myrvp->copy_actions(actions);                                   \
        return (myrvp); }




// Actions

#define DECLARE_DORA_ACTION_GEN_FUNC(actioname,rvpname,inputname)       \
    actioname* new_##actioname(xct_t* axct, const tid_t& atid, rvpname* prvp, const inputname& in)


#define DEFINE_DORA_ACTION_GEN_FUNC(actioname,rvpname,inputname,actiontype,classname) \
    DECLARE_TLS_ACTION_CACHE(actioname,actiontype);                     \
    actioname* classname::new_##actioname(xct_t* axct, const tid_t& atid, rvpname* prvp, const inputname& in) { \
        actioname* myaction = my_##actioname##_cache->_cache->borrow(); \
        assert (myaction);                                              \
        myaction->set(axct,atid,prvp,in,this,my_##actioname##_cache->_cache.get()); \
        prvp->add_action(myaction);                                     \
        return (myaction); }





////////////////////////////////////////////////////////////////////////////////
//
// RVP & ACTION CLASSES
// 
////////////////////////////////////////////////////////////////////////////////


///// RVP CLASS 


#define DECLARE_DORA_FINAL_RVP_CLASS(cname,envname,intratrx,total)      \
    class cname : public terminal_rvp_t {                               \
    private:                                                            \
            typedef object_cache_t<cname> rvp_cache;                    \
            envname* _penv;                                             \
            rvp_cache* _cache;                                          \
    public:                                                             \
            cname() : terminal_rvp_t(), _penv(NULL), _cache(NULL) { }   \
            ~cname() { _cache=NULL; _penv=NULL; }                       \
            inline void set(xct_t* axct, const tid_t& atid,             \
                            const int axctid,                           \
                            trx_result_tuple_t& presult, envname* penv, rvp_cache* pc) { \
                assert (penv); _penv = penv;                            \
                assert (pc); _cache = pc;                               \
                _set(penv->db(),penv,axct,atid,axctid,presult,intratrx,total); } \
            inline void giveback() { _cache->giveback(this); }          \
            void upd_committed_stats();                                 \
            void upd_aborted_stats(); }


// @note: When intratrx or total are not known a-priori (e.g. NewOrder)
#define DECLARE_DORA_FINAL_DYNAMIC_RVP_CLASS(cname,envname)             \
    class cname : public terminal_rvp_t {                               \
    private:                                                            \
            typedef object_cache_t<cname> rvp_cache;                    \
            envname* _penv;                                             \
            rvp_cache* _cache;                                          \
    public:                                                             \
            cname() : terminal_rvp_t(), _penv(NULL), _cache(NULL) { }   \
            ~cname() { _cache=NULL; _penv=NULL; }                       \
            inline void set(xct_t* axct, const tid_t& atid,             \
                            const int axctid,                           \
                            trx_result_tuple_t& presult, envname* penv, rvp_cache* pc, \
                            const int intratrx, const int total) {      \
                assert (penv); _penv = penv;                            \
                assert (pc); _cache = pc;                               \
                _set(penv->db(),penv,axct,atid,axctid,presult,intratrx,total); } \
            inline void giveback() { _cache->giveback(this); }          \
            void upd_committed_stats();                                 \
            void upd_aborted_stats(); }


#define DEFINE_DORA_FINAL_RVP_CLASS(cname,trx)                          \
    void cname::upd_committed_stats() {                                 \
        _penv->_inc_##trx##_att(); _penv->inc_trx_com(); }              \
    void cname::upd_aborted_stats() {                                   \
        _penv->_inc_##trx##_att(); _penv->_inc_##trx##_failed();        \
        _penv->inc_trx_att(); }



#define DECLARE_DORA_EMPTY_MIDWAY_RVP_CLASS(cname,envname,inputname,intratrx,total) \
    class cname : public rvp_t {                                        \
    private:                                                            \
            typedef object_cache_t<cname> rvp_cache;                    \
            envname* _penv;                                             \
            rvp_cache* _cache;                                          \
            bool _bWake;                                                \
    public:                                                             \
            cname() : rvp_t(), _penv(NULL), _cache(NULL) { }            \
            ~cname() { _cache=NULL; _penv=NULL; }                       \
            inputname _in;                                              \
            inline void set(xct_t* axct, const tid_t& atid, const int axctid, \
                            trx_result_tuple_t& presult,                \
                            const inputname& in, const bool bWake,      \
                            envname* penv, rvp_cache* pc) {             \
                _in = in;                                               \
                _bWake = bWake;                                         \
                assert (penv); _penv = penv;                            \
                assert (pc); _cache = pc;                               \
                _set(axct,atid,axctid,presult,intratrx,total); }        \
            inline void giveback() { _cache->giveback(this); }          \
            w_rc_t _run(); }



#define DECLARE_DORA_EMPTY_MIDWAY_DYNAMIC_RVP_CLASS(cname,envname,inputname) \
    class cname : public rvp_t {                                        \
    private:                                                            \
            typedef object_cache_t<cname> rvp_cache;                    \
            envname* _penv;                                             \
            rvp_cache* _cache;                                          \
            bool _bWake;                                                \
    public:                                                             \
            cname() : rvp_t(), _penv(NULL), _cache(NULL) { }            \
            ~cname() { _cache=NULL; _penv=NULL; }                       \
            inputname _in;                                              \
            inline void set(xct_t* axct, const tid_t& atid, const int axctid, \
                            trx_result_tuple_t& presult,                \
                            const inputname& in, const bool bWake,      \
                            envname* penv, rvp_cache* pc,               \
                            const int intratrx, const int total) {      \
                _in = in;                                               \
                _bWake = bWake;                                         \
                assert (penv); _penv = penv;                            \
                assert (pc); _cache = pc;                               \
                _set(axct,atid,axctid,presult,intratrx,total); }        \
            inline void giveback() { _cache->giveback(this); }          \
            w_rc_t _run(); }



///// ACTION CLASS

#define DECLARE_DORA_ACTION_NO_RVP_CLASS(aname,datatype,envname,inputname,keylen) \
    class aname : public range_action_impl<datatype> {                  \
    private:                                                            \
            typedef object_cache_t<aname> act_cache;                    \
            envname* _penv;                                             \
            act_cache* _cache;                                          \
    public:                                                             \
            aname() : range_action_impl<datatype>(), _penv(NULL) { }    \
            ~aname() { }                                                \
            inputname _in;                                              \
            inline void giveback() { _cache->giveback(this); }          \
            inline void set(xct_t* axct, const tid_t& atid, rvp_t* prvp, \
                            const inputname& in, envname* penv, act_cache* pc) { \
                assert (pc); _cache = pc;                               \
                assert (penv); _penv = penv;                            \
                _in = in;                                               \
                _range_act_set(axct,atid,prvp,keylen); }                \
            w_rc_t trx_exec();                                          \
            void calc_keys(); }


#define DECLARE_DORA_ACTION_WITH_RVP_CLASS(aname,datatype,envname,rvpname,inputname,keylen) \
    class aname : public range_action_impl<datatype> {                  \
    private:                                                            \
            typedef object_cache_t<aname> act_cache;                    \
            envname* _penv;                                             \
            rvpname* _prvp;                                             \
            act_cache* _cache;                                          \
    public:                                                             \
            aname() : range_action_impl<datatype>(), _penv(NULL) { }    \
            ~aname() { }                                                \
            inputname _in;                                              \
            inline void giveback() { _cache->giveback(this); }          \
            inline void set(xct_t* axct, const tid_t& atid, rvpname* prvp, \
                            const inputname& in, envname* penv, act_cache* pc) { \
                assert (pc); _cache = pc;                               \
                assert (prvp); _prvp = prvp;                            \
                assert (penv); _penv = penv;                            \
                _in = in;                                               \
                _range_act_set(axct,atid,prvp,keylen); }                \
            w_rc_t trx_exec();                                          \
            void calc_keys(); }



// Check if a Midway RVP is Aborted
//
// If xct has already aborted then it calls immediately the next rvp
// to abort and execute its code. 
// In the majority of cases it is the final-rvp code

// The RVP notification and giveback take place at the rvp::run 

#define CHECK_MIDWAY_RVP_ABORTED(nextrvp)                               \
    if (isAborted()) {                                                  \
        nextrvp->abort();                                               \
        w_rc_t e = nextrvp->run();                                      \
        if (e.is_error()) {                                             \
            TRACE( TRACE_ALWAYS,                                        \
                   "Problem running rvp for xct (%d) [0x%x]\n",         \
                   _tid.get_lo(), e.err_num()); }                       \
        nextrvp = NULL;                                                 \
        return (e); }


const int DF_ACTION_CACHE_SZ = 100;


EXIT_NAMESPACE(dora);

#endif // __DORA_COMMON_H
