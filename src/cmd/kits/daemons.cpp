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

#include "daemons.h"
#include "shore_env.h"


/******************************************************************
 *
 *  @class: db_init_smt_t
 *
 *  @brief: An smthread inherited class that it is used for initiating
 *          the Shore environment
 *
 ******************************************************************/

db_init_smt_t::db_init_smt_t(std::string tname, ShoreEnv* db)
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



int db_init_smt_t::rv()
{
    return (_rv);
}


#if 0 // CS TODO

void close_smt_t::work()
{
    TRACE( TRACE_ALWAYS, "Closing Shore...\n");
    if (_env != NULL)
    {
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

#endif



/******************************************************************
 *
 *  @class: abort_smt_t
 *
 *  @brief: An smthread inherited class that it is used just for
 *          aborting a list of transactions
 *
 ******************************************************************/

abort_smt_t::abort_smt_t(std::string tname,
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
    // me()->alloc_sdesc_cache();
    for (vector<xct_t*>::iterator it = _toabort->begin();
         it != _toabort->end(); ++it) {

        victim = *it;
        assert (victim);
        smthread_t::attach_xct(victim);
        r = xct_t::abort();
        if (r.is_error()) {
            TRACE( TRACE_ALWAYS, "Problem aborting (%x)\n", *it);
        }
        else {
            _aborted++;
        }
    }
    // me()->free_sdesc_cache();
}

void checkpointer_t::work()
{
    // overflows in 64 years, so it's fine
    int ticker = 0;
    int act_delay = _env->get_activation_delay();
    while(_active) {
    	/**
    	 * CS: sleep one second F times rather than sleeping F seconds.
    	 * This allows to thread to finish after one second at most
    	 * once it is deactivated.
    	 */
        for (int i = 0; i < _env->get_chkpt_freq(); i++) {
            ::sleep(1);
            ticker++;
            /*
             * Also send signals to wake up archiver and merger daemons.
             * Wait is set to false, because this is just a mechanism to
             * make sure that the daemons are always running (up to a 1 sec
             * window). (TODO) In the future, those daemons should be controlled
             * by system activity.
             */
            if (ticker >= act_delay) {
                _env->activate_archiver();
            }
            if (!_active) return;
        }
       	TRACE( TRACE_ALWAYS, "db checkpoint - start\n");
       	_env->checkpoint();
       	TRACE( TRACE_ALWAYS, "db checkpoint - end\n");
    }
    TRACE( TRACE_ALWAYS, "Checkpointer thread deactivated\n");
}

void crasher_t::work()
{
    ::sleep(_timeout);
    abort();
}

