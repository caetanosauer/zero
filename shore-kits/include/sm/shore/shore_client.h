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

/** @file:   shore_client.h
 *
 *  @brief:  Wrapper for Shore client threads
 *
 *  @author: Ippokratis Pandis, July 2008
 *
 */

#ifndef __SHORE_CLIENT_H
#define __SHORE_CLIENT_H

#include "k_defines.h"

#include "sm/shore/shore_env.h"
#include "sm/shore/shore_helper_loader.h"


ENTER_NAMESPACE(shore);


// enumuration of different binding types
enum eBindingType { BT_NONE=0, BT_NEXT=1, BT_SPREAD=2 };


//// Default values for the environment ////


// Default values for the power-runs //


// default value to spread threads
const int DF_SPREAD_THREADS        = 1;

// default number of threads
const int DF_NUM_OF_THR            = 5;

// maximum number of threads
const int MAX_NUM_OF_THR           = 1000;

// default number of transactions executed per thread
const int DF_TRX_PER_THR           = 100;

// default duration for time-based measurements (in secs)
const int DF_DURATION              = 20;

// default number of iterations
const int DF_NUM_OF_ITERS          = 5;

// default processor binding
const eBindingType DF_BINDING_TYPE = BT_NONE;


// Default values for the warmups //

// default number of transactions executed per thread during warmup
const int DF_WARMUP_TRX_PER_THR = 1000;

// default duration of warmup (in secs)
const int DF_WARMUP_DURATION    = 20;

// default number of iterations during warmup
const int DF_WARMUP_ITERS       = 3;



// default batch size
const int BATCH_SIZE = 10;

// default think time
const int THINK_TIME = 0;


// Instanciate and close the Shore environment
int inst_test_env(int argc, char* argv[]);
int close_test_env();


/******************************************************************** 
 *
 * @enum  MeasurementType
 *
 * @brief Possible measurement types for tester thread
 *
 ********************************************************************/

const int DF_WARMUP_INTERVAL = 2; // 2 secs

enum MeasurementType { MT_UNDEF, MT_NUM_OF_TRXS, MT_TIME_DUR };


/******************************************************************** 
 *
 * @enum:  base_client_t
 *
 * @brief: An smthread-based base class for the clients
 *
 ********************************************************************/

class base_client_t : public thread_t 
{
public:

    // supported trxs
    typedef map<int,string>            mapSupTrxs;
    typedef mapSupTrxs::iterator       mapSupTrxsIt;
    typedef mapSupTrxs::const_iterator mapSupTrxsConstIt;

protected:

    // the environment
    ShoreEnv* _env;    

    // workload parameters
    MeasurementType _measure_type;
    int _trxid;
    int _notrxs;

    int _think_time; // in microseconds

    // used for submitting batches
    guard<condex_pair> _cp;

    // for processor binding
    bool          _is_bound;
    processorid_t _prs_id;

    int _id; // thread id
    int _rv;

public:

    base_client_t() 
        : thread_t("none"), _env(NULL), _measure_type(MT_UNDEF), 
          _trxid(-1), _notrxs(-1), _think_time(0),
          _is_bound(false), _prs_id(PBIND_NONE),
          _rv(1)
    { }
    
    base_client_t(c_str tname, const int id, ShoreEnv* env, 
                  const MeasurementType aType, const int trxid, 
                  const int numOfTrxs,
                  processorid_t aprsid = PBIND_NONE) 
	: thread_t(tname), _env(env), _measure_type(aType), 
          _trxid(trxid), _notrxs(numOfTrxs), _think_time(0),
          _is_bound(false), _prs_id(aprsid), _id(id), _rv(0)
    {
        assert (_env);
        assert (_measure_type != MT_UNDEF);
        assert (_notrxs || (_measure_type == MT_TIME_DUR));
        _cp = new condex_pair();
    }

    virtual ~base_client_t() { }


    // thread entrance
    void work() {

        TRY_TO_BIND(_prs_id,_is_bound);

        // 2. init env in not initialized
        if (!_env->is_initialized()) {
            if (_env->init()) {
                // Couldn't initialize the Shore environment
                // cannot proceed
                TRACE( TRACE_ALWAYS, "Couldn't initialize Shore...\n");
                _rv = 1;
                return;
            }
        }

        // 4. run workload
        powerrun();
    }
    
    // access methods
    int id() { return (_id); }
    bool is_bound() const { return (_is_bound); }
    inline int rv() { return (_rv); }
    
    // methods
    w_rc_t run_xcts(int xct_type, int num_xct);
    int powerrun() {
        w_rc_t rc = _env->loaddata();
        if(rc.is_error()) return rc.err_num();
        return (run_xcts(_trxid, _notrxs).err_num());
    }
       
    w_rc_t submit_batch(int xct_type, int& trx_cnt, const int batch_size);

    static void abort_test();
    static void resume_test();
    static bool is_test_aborted();

    // every client class should implement this functions
    static int load_sup_xct(mapSupTrxs& map) {
        map.clear(); return (map.size());
    }

    // INTERFACE    

    virtual w_rc_t submit_one(int xct_type, int num_xct)=0;


    // debugging 

    void print_tables() { assert (_env); _env->dump(); }


private:

    // copying not allowed
    base_client_t(base_client_t const &);
    void operator=(base_client_t const &);


}; // EOF: base_client_t


EXIT_NAMESPACE(shore);


#endif /** __SHORE_CLIENT_H */
