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

/** @file shore_helper_loader.cpp
 *
 *  @brief Declaration of helper loader thread classes
 *
 *  @author Ippokratis Pandis (ipandis)
 */

#include "sm/shore/shore_helper_loader.h"
#include "sm/shore/shore_env.h"


using namespace shore;


/****************************************************************** 
 *
 *  @class: db_init_smt_t
 *
 *  @brief: An smthread inherited class that it is used for initiating
 *          the Shore environment
 *
 ******************************************************************/
    
db_init_smt_t::db_init_smt_t(c_str tname, ShoreEnv* db) 
    : thread_t(tname), _env(db)
{
    assert (_env);
}


db_init_smt_t::~db_init_smt_t() 
{ 
}

void db_init_smt_t::work()
{
    if (!_env->is_initialized()) {
        _rv = _env->init();
        if (_rv) {
            // Couldn't initialize the Shore environment
            // cannot proceed
            TRACE( TRACE_ALWAYS, "Couldn't initialize Shore...\n");
            return;
        }
    }

    // if reached this point everything went ok
    TRACE( TRACE_DEBUG, "Shore initialized...\n");
    _rv = 0;
}



/****************************************************************** 
 *
 *  @class: db_log_smt_t
 *
 *  @brief: An smthread inherited class that it is used for flushing 
 *          the log
 *
 ******************************************************************/

int db_init_smt_t::rv() 
{ 
    return (_rv); 
}


void db_log_smt_t::work() 
{
    assert (_env);
#ifndef CFG_SHORE_6
    _env->db()->flushlog();
#endif
    _rv = 0;
}


void db_load_smt_t::work() 
{
    _rc = _env->loaddata();
    _rv = 0;
}



void close_smt_t::work() 
{
    assert (_env);
    TRACE( TRACE_ALWAYS, "Closing Shore...\n");
    if (_env) {
        _env->close();
        delete (_env);
        _env = NULL;
    }        
}




void dump_smt_t::work() 
{
    assert (_env);
    TRACE( TRACE_DEBUG, "Dumping...\n");
    _env->dump();
    _rv = 0;
}



/****************************************************************** 
 *
 *  @class: abort_smt_t
 *
 *  @brief: An smthread inherited class that it is used just for
 *          aborting a list of transactions
 *
 ******************************************************************/
    
abort_smt_t::abort_smt_t(c_str tname, 
                         ShoreEnv* env, 
                         vector<xct_t*>& toabort)
    : thread_t(tname), _env(env)
{
    assert(env);
    _toabort = &toabort;
}
 
abort_smt_t::~abort_smt_t()
{
}

void abort_smt_t::work()
{
    w_rc_t r = RCOK;
    xct_t* victim = NULL;
    me()->alloc_sdesc_cache();
    for (vector<xct_t*>::iterator it = _toabort->begin();
         it != _toabort->end(); ++it) {

        victim = *it;
        assert (victim);
        _env->db()->attach_xct(victim);
        r = _env->db()->abort_xct();
        if (r.is_error()) {
            TRACE( TRACE_ALWAYS, "Problem aborting (%x)\n", *it);
        }
        else {
            _aborted++;
        }
    }
    me()->free_sdesc_cache();
}



