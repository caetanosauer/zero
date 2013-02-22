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

/** @file shore_tools.h
 *
 *  @brief Shore common tools
 *
 *  @author Ippokratis Pandis (ipandis)
 */

#ifndef __SHORE_TOOLS_H
#define __SHORE_TOOLS_H


#include "sm_vas.h"
#include "util/namespace.h"

#include "sm/shore/shore_file_desc.h"

#include "sm/shore/shore_env.h"


ENTER_NAMESPACE(shore);



/******** Exported functions ********/

/** @class trx_smthread_t
 *
 *  @brief An smthread-based class for running trxs
 */

template<class InputClass>
class trx_smthread_t : public smthread_t {
private:
    typedef trx_result_tuple_t (*trxfn)(InputClass*, const int, ShoreEnv*);

private:
    trxfn _fn;           // pointer to trx function 
    InputClass* _input;
    ShoreEnv* _env;
    int _id;
    c_str _tname;

public:
    trx_result_tuple_t _rv;

    trx_smthread_t(trxfn fn, InputClass* fninput, ShoreEnv* env, 
                   const int id, c_str tname)
	: smthread_t(t_regular, tname.data()), _fn(fn), _input(fninput),
          _env(env), _id(id), _tname(tname), _rv(trx_result_tuple_t(UNDEF, id))
    {
    }
    
    ~trx_smthread_t() { }

    // thread entrance
    void run()
    {
        // trx executing function, as well as, environment and input
        // should not be NULL
        assert (_fn!=NULL);
        assert (_input!=NULL);
        assert (_env!=NULL);
        _rv = (*_fn)(_input, _id, _env);
    }

    /** @note Those two functions should be implemented by every
     *        smthread-inherited class that runs using run_smthread()
     *  @note The return type of retval() can be anything
     */
    inline trx_result_tuple_t retval() { return (_rv); }
    inline c_str tname() { return (_tname); }

}; // EOF: trx_smthread_t



/****************************************************************** 
 *
 *  @fn     run_smthread
 *
 *  @brief  Creates an smthread inherited class and runs it. The
 *          second argument is the return of the run() function of the 
 *          thread it is allocated in here. It is responsibility of 
 *          the caller to deallocate.
 *
 *  @return non-zero on error or failed thread operation
 *
 ******************************************************************/

template<class SMThread, class SMTReturn>
int run_smthread(SMThread* t, SMTReturn* &r)
{
    if (!t)
	W_FATAL(fcOUTOFMEMORY);

    w_rc_t e = t->fork();
    if(e.is_error()) {
        TRACE( TRACE_ALWAYS, "Error forking thread (%s)...\n", 
               t->name());
	return (1);
    }

    e = t->join();
    if (e.is_error()) {
        TRACE( TRACE_ALWAYS, "Error joining thread (%s)...\n", 
               t->name());
	return (2);
    }

    r = new SMTReturn(t->retval());
    assert (r);

    // if we reached this point everything went ok
    return (0);
}


/** @fn run_xct
 *  
 *  @brief  Runs a transaction, checking for deadlock and retrying
 *  automatically as need be. It essentially wraps the operator() of the
 *  Transaction class with a begin_xct(), abort_xct() and commit_xct() calls.
 *
 *  @note Transaction's operator() should take as argument the db handle and 
 *  return a w_rc_t.
 */

template<class Transaction>
w_rc_t run_xct(ss_m* ssm, Transaction &t) {
    w_rc_t e;
    do {
	e = ssm->begin_xct();
	if(e.is_error())
	    break;
	e = t(ssm);
	if(e.is_error())
	    e = ssm->abort_xct();
	else
	    e = ssm->commit_xct();
    } while(e.is_error() && e.err_num() == smlevel_0::eDEADLOCK);
    return e;
}



/** @struct create_volume_xct
 *  
 *  @brief  Creates a volume in the context of a transaction.
 */

template<class Parser>
struct create_volume_xct 
{
    //    vid_t &_vid;
    //    pthread_mutex_t* _vol_mutex;
    ShoreEnv* _penv;

    char const* _table_name;
    file_info_t &_info;
    size_t _bytes;

    create_volume_xct(char const* tname, file_info_t &info, 
                      size_t bytes, ShoreEnv* env
                      )
	: _table_name(tname), _info(info), _bytes(bytes), _penv(env)
    {
    }

    w_rc_t operator()(ss_m* ssm) {

	CRITICAL_SECTION(cs, *(_penv->get_vol_mutex()));

	stid_t root_iid;
	vec_t table_name(_table_name, strlen(_table_name));
	unsigned size = sizeof(_info);
	vec_t table_info(&_info, size);
	bool found;

	W_DO(ss_m::vol_root_index(*(_penv->vid()), root_iid));
	W_DO(ss_m::find_assoc(root_iid, table_name, &_info, size, found));

	if(found) {
	    cout << "Removing previous instance of " << _table_name << endl;
	    W_DO(ss_m::destroy_file(_info.fid()));
	    W_DO(ss_m::destroy_assoc(root_iid, table_name, table_info));
	}

	// create the file and register it with the root index
	cout << "Creating table ``" << _table_name
	     << "'' with " << _bytes << " bytes per record" << endl;
	W_DO(ssm->create_file(*(_penv->vid()), _info._fid, smlevel_3::t_regular));
	W_DO(ss_m::vol_root_index(*(_penv->vid()), root_iid));
	W_DO(ss_m::create_assoc(root_iid, table_name, table_info));

	return (RCOK);
    }

}; // EOF: create_volume_xct 




EXIT_NAMESPACE(shore);

#endif
