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

/** @file:   shore_shell.cpp
 *
 *  @brief:  Implementation of shell class for Shore environments
 *
 *  @author: Ippokratis Pandis, Sept 2008
 */

#include "sm/shore/shore_shell.h"


#ifdef CFG_BT
#ifndef CFG_SHORE_6
#include "backtrace.h"
#endif
#endif


ENTER_NAMESPACE(shore);

// Globals 

extern "C" void alarm_handler(int sig) 
{
    if(sig == SIGALRM) {
	if(_g_shore_env->get_measure() != MST_DONE) {
	    TRACE( TRACE_ALWAYS, "Start Load Imbalance\n");
	    _g_shore_env->start_load_imbalance();
	}
    } else {
	_g_shore_env->set_measure(MST_DONE);
    }
}

bool volatile _g_canceled = false;

double _theSF = DF_SF;


//// shore_shell_t interface ////


shore_shell_t::shore_shell_t(const char* prompt, 
                             const bool netmode,
                             const int netport,                  
                             const bool inputfilemode, 
                             const string inputfile,
                             processorid_t acpustart) 
    : shell_t(prompt,true,netmode,netport,inputfilemode,inputfile), 
      _env(NULL), 
      _start_prs_id(acpustart), _current_prs_id(acpustart)
{
    // install signal handler
    struct sigaction sa;
    struct sigaction sa_old;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = &alarm_handler;
    if(sigaction(SIGALRM, &sa, &sa_old) < 0) {
        exit(1);        
    }
}

shore_shell_t::~shore_shell_t() 
{ 
    if (_env) {
        _env->stop();
        close_smt_t* clt = new close_smt_t(_env, c_str("clt"));
        assert (clt);
        clt->fork(); // pointer is deleted by clt thread
        clt->join();
        int rv = clt->_rv;
        if (rv) {
            fprintf( stderr, "Error in closing thread...\n");
        }
        delete (clt);
        clt = NULL;
    }
}



/******************************************************************** 
 *
 *  @fn:    {trxs,bp}_map
 *
 *  @brief: The supported trxs and binding policies maps
 *
 ********************************************************************/


// TRXS - Supported Transactions //

void shore_shell_t::print_sup_trxs(void) const 
{
    TRACE( TRACE_ALWAYS, "Supported TRXs\n");
    for (mapSupTrxsConstIt cit= _sup_trxs.begin();
         cit != _sup_trxs.end(); cit++)
            TRACE( TRACE_ALWAYS, "%d -> %s\n", cit->first, cit->second.c_str());
}

const char* shore_shell_t::translate_trx(const int iSelectedTrx) const
{
    mapSupTrxsConstIt cit = _sup_trxs.find(iSelectedTrx);
    if (cit != _sup_trxs.end())
        return (cit->second.c_str());
    return ("Unsupported TRX");
}


// BP - Binding Policies //

void shore_shell_t::print_sup_bp(void) 
{
    TRACE( TRACE_ALWAYS, "Supported Binding Policies\n");
    for (mapBindPolsIt cit= _sup_bps.begin();
         cit != _sup_bps.end(); cit++)
            TRACE( TRACE_ALWAYS, "%d -> %s\n", cit->first, cit->second.c_str());
}

const char* shore_shell_t::translate_bp(const eBindingType abt)
{
    mapBindPolsIt it = _sup_bps.find(abt);
    if (it != _sup_bps.end())
        return (it->second.c_str());
    return ("Unsupported Binding Policy");
}


/****************************************************************** 
 *
 * @fn:    next_cpu()
 *
 * @brief: Decides what is the next cpu for the forking clients 
 *
 * @note:  This decision is based on:
 *         - abt                     - the selected binding type
 *         - aprd                    - the current cpu
 *         - _env->_max_cpu_count    - the maximum cpu count (hard-limit)
 *         - _env->_active_cpu_count - the active cpu count (soft-limit)
 *         - this->_start_prs_id     - the first assigned cpu for the table
 *
 ******************************************************************/

processorid_t shore_shell_t::next_cpu(const eBindingType abt, 
                                      const processorid_t aprd) 
{
    processorid_t nextprs;
    switch (abt) {
    case (BT_NONE):
        return (PBIND_NONE);
    case (BT_NEXT):
        nextprs = ((aprd+1) % _env->get_active_cpu_count());
        return (nextprs);
    case (BT_SPREAD):
        static const uint NIAGARA_II_STEP = 8;
        nextprs = ((aprd+NIAGARA_II_STEP) % _env->get_active_cpu_count());
        return (nextprs);
    }
    assert (0); // Should not reach this point
    return (nextprs);
}


/******************************************************************** 
 *
 *  @fn:    print_CMD_info
 *
 *  @brief: Prints command-specific info
 *
 ********************************************************************/

void shore_shell_t::print_MEASURE_info(const double iQueriedSF, const int iSpread, 
                                       const int iNumOfThreads, const int iDuration,
                                       const int iSelectedTrx, const int iIterations,
                                       const eBindingType abt)
{
    // Print out configuration
    TRACE( TRACE_ALWAYS, "\n" \
           "QueriedSF:     (%.1f)\n" \
           "SpreadThreads: (%s)\n" \
           "Binding:       (%s)\n" \
           "NumOfThreads:  (%d)\n" \
           "Duration:      (%d)\n" \
           "Trx:           (%s)\n" \
           "Iterations:    (%d)\n",
           iQueriedSF, (iSpread ? "Yes" : "No"), 
           translate_bp(abt),
           iNumOfThreads, iDuration, translate_trx(iSelectedTrx), 
           iIterations);
}


void shore_shell_t::print_TEST_info(const double iQueriedSF, const int iSpread, 
                                    const int iNumOfThreads, const int iNumOfTrxs,
                                    const int iSelectedTrx, const int iIterations,
                                    const eBindingType abt)
{
    // Print out configuration
    TRACE( TRACE_ALWAYS, "\n"
           "QueriedSF:      (%.1f)\n" \
           "Spread Threads: (%s)\n" \
           "Binding:        (%s)\n" \
           "NumOfThreads:   (%d)\n" \
           "NumOfTrxs:      (%d)\n" \
           "Trx:            (%s)\n" \
           "Iterations:     (%d)\n",
           iQueriedSF, (iSpread ? "Yes" : "No"), 
           translate_bp(abt),
           iNumOfThreads, iNumOfTrxs, translate_trx(iSelectedTrx),
           iIterations);
}


/******************************************************************** 
 *
 *  @fn:    print_usage
 *
 *  @brief: Prints the normal usage for {TEST/MEASURE/WARMUP} cmds
 *
 ********************************************************************/

int shore_shell_t::print_usage(const char* command) 
{
    assert (command);

    TRACE( TRACE_ALWAYS, "\n\nSupported commands: TRXS/LOAD/WARMUP/TEST/MEASURE\n\n" );

    TRACE( TRACE_ALWAYS, "WARMUP Usage:\n\n" \
           "*** warmup [<NUM_QUERIED> <NUM_TRXS> <DURATION> <ITERATIONS>]\n" \
           "\nParameters:\n" \
           "<NUM_QUERIED> : The SF queried (Default=10) (optional)\n" \
           "<NUM_TRXS>    : Number of transactions per thread (Default=1000) (optional)\n" \
           "<DURATION>    : Duration of experiment in secs (Default=20) (optional)\n" \
           "<ITERATIONS>  : Number of iterations (Default=3) (optional)\n\n");

    TRACE( TRACE_ALWAYS, "TEST Usage:\n\n" \
           "*** test <NUM_QUERIED> [<SPREAD> <NUM_THRS> <NUM_TRXS> <TRX_ID> <ITERATIONS> <BINDING>]\n" \
           "\nParameters:\n" \
           "<NUM_QUERIED> : The SF queried (queried factor)\n" \
           "<SPREAD>      : Whether to spread threads (0=No, Other=Yes, Default=No) (optional)\n" \
           "<NUM_THRS>    : Number of threads used (optional)\n" \
           "<NUM_TRXS>    : Number of transactions per thread (optional)\n" \
           "<TRX_ID>      : Transaction ID to be executed (0=mix) (optional)\n" \
           "<ITERATIONS>  : Number of iterations (Default=5) (optional)\n" \
           "<BINDING>     : Binding Type (Default=0-No binding) (optional)\n\n");

    TRACE( TRACE_ALWAYS, "MEASURE Usage:\n\n" \
           "*** measure <NUM_QUERIED> [<SPREAD> <NUM_THRS> <DURATION> <TRX_ID> <ITERATIONS> <BINDING>]\n" \
           "\nParameters:\n" \
           "<NUM_QUERIED> : The SF queried (queried factor)\n" \
           "<SPREAD>      : Whether to spread threads (0=No, Other=Yes, Default=No) (optional)\n" \
           "<NUM_THRS>    : Number of threads used (optional)\n" \
           "<DURATION>    : Duration of experiment in secs (Default=20) (optional)\n" \
           "<TRX_ID>      : Transaction ID to be executed (0=mix) (optional)\n" \
           "<ITERATIONS>  : Number of iterations (Default=5) (optional)\n" \
           "<BINDING>     : Binding Type (Default=0-No binding) (optional)\n");
    
    TRACE( TRACE_ALWAYS, "\n\nCurrently Scaling factor = (%d)\n", _theSF);

    print_sup_trxs();
    print_sup_bp();

    return (SHELL_NEXT_CONTINUE);
}




/******************************************************************** 
 *
 *  @fn:    pre_process_cmd
 *
 *  @brief: Does some cleanup before executing any cmd
 *
 ********************************************************************/

void shore_shell_t::pre_process_cmd()
{
    _g_canceled = false;

    _current_prs_id = _start_prs_id;

    // make sure any previous abort is cleared
    base_client_t::resume_test();

    _g_mon->stat_reset();
}



/******************************************************************** 
 *
 *  @fn:    process_cmd_LOAD
 *
 *  @brief: Parses the LOAD cmd and calls the virtual impl function
 *
 ********************************************************************/

int shore_shell_t::process_cmd_LOAD(const char* /* command */)
{
    assert (_env);
    assert (_env->is_initialized());

    if (_env->is_loaded()) {
        TRACE( TRACE_ALWAYS, "Environment already loaded\n");
        return (SHELL_NEXT_CONTINUE);
    }

    // call the virtual function that implements the test    
    return (_cmd_LOAD_impl());
}


/******************************************************************** 
 *
 *  @fn:    process_cmd_WARMUP
 *
 *  @brief: Parses the WARMUP cmd and calls the virtual impl function
 *
 ********************************************************************/

int shore_shell_t::process_cmd_WARMUP(const char* command)
{
    assert (_env);
    assert (_env->is_initialized());

    // first check if env initialized and loaded
    // try to load and abort on error
    w_rc_t rcl = _env->loaddata();
    if (rcl.is_error()) {
        return (SHELL_NEXT_QUIT);
    }

    // 0. Parse Parameters
    envVar* ev = envVar::instance();
    double numOfQueriedSF      = ev->getVarDouble("test-num-queried",DF_NUM_OF_QUERIED_SF);
    double tmp_numOfQueriedSF  = numOfQueriedSF;
    int numOfTrxs              = ev->getVarInt("test-num-trxs",DF_WARMUP_TRX_PER_THR);
    int tmp_numOfTrxs          = numOfTrxs;
    int duration               = ev->getVarInt("measure-duration",DF_WARMUP_DURATION);
    int tmp_duration           = duration;
    int iterations             = ev->getVarInt("test-iterations",DF_WARMUP_ITERS);
    int tmp_iterations         = iterations;

 
    // Parses new test run data
    char command_tag[SERVER_COMMAND_BUFFER_SIZE];
    if ( sscanf(command, "%s %lf %d %d %d",
                command_tag,
                &tmp_numOfQueriedSF,
                &tmp_numOfTrxs,
                &tmp_duration,
                &tmp_iterations) < 1 ) 
    {
        TRACE( TRACE_ALWAYS, "Wrong input. Type (help warmup)\n"); 
        return (SHELL_NEXT_CONTINUE);
    }


    // OPTIONAL Parameters

    // 1- number of queried scaling factor - numOfQueriedSF
    if ((tmp_numOfQueriedSF>0) && (tmp_numOfQueriedSF<=_theSF)) {
        numOfQueriedSF = tmp_numOfQueriedSF;
    }
    else {
        numOfQueriedSF = _theSF;
    }
    assert (numOfQueriedSF <= _theSF);
    
    // 2- number of trxs - numOfTrxs
    if (tmp_numOfTrxs>0)
        numOfTrxs = tmp_numOfTrxs;
    
    // 3- duration of measurement - duration
    if (tmp_duration>0)
        duration = tmp_duration;

    // 4- number of iterations - iterations
    if (tmp_iterations>0)
        iterations = tmp_iterations;


    // Print out configuration
    TRACE( TRACE_ALWAYS, "\n" \
           "Queried SF   : %.1f\n" \
           "Num of Trxs  : %d\n" \
           "Duration     : %d\n" \
           "Iterations   : %d\n", 
           numOfQueriedSF, numOfTrxs, duration, iterations);

    // call the virtual function that implements the test    
    return (_cmd_WARMUP_impl(numOfQueriedSF, numOfTrxs, duration, iterations));
}




/******************************************************************** 
 *
 *  @fn:    process_cmd_TEST
 *
 *  @brief: Parses the TEST cmd and calls the virtual impl function
 *
 ********************************************************************/

int shore_shell_t::process_cmd_TEST(const char* command)
{
    assert (_env);
    assert (_env->is_initialized());

    // first check if env initialized and loaded
    // try to load and abort on error
    w_rc_t rcl = _env->loaddata();
    if (rcl.is_error()) {
        return (SHELL_NEXT_QUIT);
    }

    // 0. Parse Parameters
    envVar* ev = envVar::instance();
    double numOfQueriedSF      = ev->getVarDouble("test-num-queried",DF_NUM_OF_QUERIED_SF);
    double tmp_numOfQueriedSF  = numOfQueriedSF;
    int spreadThreads          = ev->getVarInt("test-spread",DF_SPREAD_THREADS);
    int tmp_spreadThreads      = spreadThreads;
    int numOfThreads           = ev->getVarInt("test-num-threads",DF_NUM_OF_THR);
    int tmp_numOfThreads       = numOfThreads;
    int numOfTrxs              = ev->getVarInt("test-num-trxs",DF_TRX_PER_THR);
    int tmp_numOfTrxs          = numOfTrxs;
    int selectedTrxID          = ev->getVarInt("test-trx-id",DF_TRX_ID);
    int tmp_selectedTrxID      = selectedTrxID;
    int iterations             = ev->getVarInt("test-iterations",DF_NUM_OF_ITERS);
    int tmp_iterations         = iterations;
    int binding       = DF_BINDING_TYPE;//ev->getVarInt("test-cl-binding",DF_BINDING_TYPE);
    int tmp_binding   = binding;


    // update the SF
    double tmp_sf = ev->getSysVarDouble("sf");
    if (tmp_sf>0) {
        TRACE( TRACE_STATISTICS, "Updated SF (%.1f)\n", tmp_sf);
        _theSF = tmp_sf;
    }

    
    // Parses new test run data
    char command_tag[SERVER_COMMAND_BUFFER_SIZE];
    if ( sscanf(command, "%s %lf %d %d %d %d %d %d",
                command_tag,
                &tmp_numOfQueriedSF,
                &tmp_spreadThreads,
                &tmp_numOfThreads,
                &tmp_numOfTrxs,
                &tmp_selectedTrxID,
                &tmp_iterations,
                &tmp_binding) < 2 ) 
    {
        TRACE( TRACE_ALWAYS, "Wrong input. Type (help test)\n"); 
        return (SHELL_NEXT_CONTINUE);
    }


    // REQUIRED Parameters

    // 1- number of queried Scaling factor - numOfQueriedSF
    if ((tmp_numOfQueriedSF>0) && (tmp_numOfQueriedSF<=_theSF)) {
        numOfQueriedSF = tmp_numOfQueriedSF;
    }
    else {
        numOfQueriedSF = _theSF;
    }
    assert (numOfQueriedSF <= _theSF);


    // OPTIONAL Parameters

    // 2- spread trxs
    spreadThreads = tmp_spreadThreads;

    // 3- number of threads - numOfThreads
    if ((tmp_numOfThreads>0) && (tmp_numOfThreads<=MAX_NUM_OF_THR)) {
        numOfThreads = tmp_numOfThreads;
        //if (spreadThreads && (numOfThreads > numOfQueriedSF))
        //numOfThreads = numOfQueriedSF;
        if (spreadThreads && ((numOfThreads % (int)numOfQueriedSF)!=0)) {
            TRACE( TRACE_ALWAYS, 
                   "\n!!! Warning QueriedSF=(%.1f) and Threads=(%d) - not spread uniformly!!!\n",
                   numOfQueriedSF, numOfThreads);
        }
    }
    else {
        numOfThreads = numOfQueriedSF;
    }
    
    // 4- number of trxs - numOfTrxs
    if (tmp_numOfTrxs>0)
        numOfTrxs = tmp_numOfTrxs;

    // 5- selected trx
    mapSupTrxsConstIt stip = _sup_trxs.find(tmp_selectedTrxID);
    if (stip!=_sup_trxs.end()) {
        selectedTrxID = tmp_selectedTrxID;
    }
    else {
        TRACE( TRACE_ALWAYS, "Unsupported TRX\n");
        return (SHELL_NEXT_CONTINUE);
    }

    // 6- number of iterations - iterations
    if (tmp_iterations>0)
        iterations = tmp_iterations;

    // 8- binding type   
    mapBindPolsIt cit = _sup_bps.find(eBindingType(tmp_binding));
    if (cit!= _sup_bps.end()) {
        binding = tmp_binding;
    }
    else {
        TRACE( TRACE_ALWAYS, "Unsupported Binding\n");
        return (SHELL_NEXT_CONTINUE);
    }


    // call the virtual function that implements the test    
    return (_cmd_TEST_impl(numOfQueriedSF, spreadThreads, numOfThreads,
                           numOfTrxs, selectedTrxID, iterations, 
                           eBindingType(binding)));
}



/******************************************************************** 
 *
 *  @fn:    process_cmd_MEASURE
 *
 *  @brief: Parses the MEASURE cmd and calls the virtual impl function
 *
 ********************************************************************/

int shore_shell_t::process_cmd_MEASURE(const char* command)
{
    assert (_env);
    assert (_env->is_initialized());

    // first check if env initialized and loaded
    // try to load and abort on error
    w_rc_t rcl = _env->loaddata();
    if (rcl.is_error()) {
        return (SHELL_NEXT_QUIT);
    }

    // 0. Parse Parameters
    envVar* ev = envVar::instance();
    double numOfQueriedSF      = ev->getVarDouble("measure-num-queried",DF_NUM_OF_QUERIED_SF);
    double tmp_numOfQueriedSF  = numOfQueriedSF;
    int spreadThreads          = ev->getVarInt("measure-spread",DF_SPREAD_THREADS);
    int tmp_spreadThreads      = spreadThreads;
    int numOfThreads           = ev->getVarInt("measure-num-threads",DF_NUM_OF_THR);
    int tmp_numOfThreads       = numOfThreads;
    int duration               = ev->getVarInt("measure-duration",DF_DURATION);
    int tmp_duration           = duration;
    int selectedTrxID          = ev->getVarInt("measure-trx-id",DF_TRX_ID);
    int tmp_selectedTrxID      = selectedTrxID;
    int iterations             = ev->getVarInt("measure-iterations",DF_NUM_OF_ITERS);
    int tmp_iterations         = iterations;
    int binding       = DF_BINDING_TYPE;//ev->getVarInt("measure-cl-binding",DF_BINDING_TYPE);
    int tmp_binding   = binding;
    
    // Parses new test run data
    char command_tag[SERVER_COMMAND_BUFFER_SIZE];
    if ( sscanf(command, "%s %lf %d %d %d %d %d %d",
                command_tag,
                &tmp_numOfQueriedSF,
                &tmp_spreadThreads,
                &tmp_numOfThreads,
                &tmp_duration,
                &tmp_selectedTrxID,
                &tmp_iterations,
                &tmp_binding) < 2 ) 
    {
        TRACE( TRACE_ALWAYS, "Wrong input. Type (help measure)\n"); 
        return (SHELL_NEXT_CONTINUE);
    }

    // update the SF
    double tmp_sf = ev->getSysVarDouble("sf");
    if (tmp_sf>0) {
        TRACE( TRACE_DEBUG, "Updated SF (%.1f)\n", tmp_sf);
        _theSF = tmp_sf;
    }


    // REQUIRED Parameters

    // 1- number of queried warehouses - numOfQueriedSF
    if ((tmp_numOfQueriedSF>0) && (tmp_numOfQueriedSF<=_theSF)) {
        numOfQueriedSF = tmp_numOfQueriedSF;
    }
    else {
        numOfQueriedSF = _theSF;
    }
    assert (numOfQueriedSF <= _theSF);


    // OPTIONAL Parameters

    // 2- spread trxs
    spreadThreads = tmp_spreadThreads;

    // 3- number of threads - numOfThreads
    if ((tmp_numOfThreads>0) && (tmp_numOfThreads<=MAX_NUM_OF_THR)) {
        numOfThreads = tmp_numOfThreads;
        //if (spreadThreads && (numOfThreads > numOfQueriedSF))
        //numOfThreads = numOfQueriedSF;
        if (spreadThreads && ((numOfThreads % (int)numOfQueriedSF)!=0)) {
            TRACE( TRACE_ALWAYS, 
                   "\n!!! Warning QueriedSF=(%.1f) and Threads=(%d) - not spread uniformly!!!\n",
                   numOfQueriedSF, numOfThreads);
        }
    }
    else {
        numOfThreads = numOfQueriedSF;
    }
    
    // 4- duration of measurement - duration
    if (tmp_duration>0)
        duration = tmp_duration;

    // 5- selected trx
    mapSupTrxsConstIt stip = _sup_trxs.find(tmp_selectedTrxID);
    if (stip!=_sup_trxs.end()) {
        selectedTrxID = tmp_selectedTrxID;
    }
    else {
        TRACE( TRACE_ALWAYS, "Unsupported TRX\n");
        return (SHELL_NEXT_CONTINUE);
    }

    // 6- number of iterations - iterations
    if (tmp_iterations>0)
        iterations = tmp_iterations;

    // 8- binding type   
    mapBindPolsIt cit = _sup_bps.find(eBindingType(tmp_binding));
    if (cit!= _sup_bps.end()) {
        binding = tmp_binding;
    }
    else {
        TRACE( TRACE_ALWAYS, "Unsupported Binding\n");
        return (SHELL_NEXT_CONTINUE);
    }

    // call the virtual function that implements the measurement    
    return (_cmd_MEASURE_impl(numOfQueriedSF, spreadThreads, numOfThreads,
                              duration, selectedTrxID, iterations,
                              eBindingType(binding)));
}


/******************************************************************** 
 *
 *  @fn:    SIGINT_handler
 *
 *  @brief: Aborts test/measurement
 *
 ********************************************************************/

int shore_shell_t::SIGINT_handler() 
{
    if(_processing_command && !_g_canceled) {
	_g_canceled = true;
	base_client_t::abort_test();
	return 0;
    }

    // fallback...
    return (shell_t::SIGINT_handler());
}


/******************************************************************** 
 *
 *  @fn:    _cmd_WARMUP_impl
 *
 *  @brief: Implementation of the WARMUP cmd
 *
 ********************************************************************/

int shore_shell_t::_cmd_WARMUP_impl(const double /* iQueriedSF */, 
                                    const int /* iTrxs */, 
                                    const int /* iDuration */, 
                                    const int /* iIterations */)
{
    TRACE( TRACE_ALWAYS, "warming up...\n");

    assert (_env);
    assert (_env->is_initialized());
    assert (_env->is_loaded());

    // if warmup fails abort
    w_rc_t rcw = _env->warmup();            
    if (rcw.is_error()) {
        assert (0); // should not fail
        return (SHELL_NEXT_QUIT);
    }
    return (SHELL_NEXT_CONTINUE);            
}


/******************************************************************** 
 *
 *  @fn:    _cmd_LOAD_impl
 *
 *  @brief: Implementation of the LOAD cmd
 *
 ********************************************************************/

int shore_shell_t::_cmd_LOAD_impl()
{
    TRACE( TRACE_ALWAYS, "loading...\n");

    assert (_env);
    assert (_env->is_initialized());

    w_rc_t rcl = _env->loaddata();
    if (rcl.is_error()) {
        TRACE( TRACE_ALWAYS, "Problem loading data\n");
        return (SHELL_NEXT_QUIT);
    }
    assert (_env->is_loaded());
    return (SHELL_NEXT_CONTINUE);            
}



//// EOF: shore_shell_t commands ////


int shore_shell_t::register_commands() 
{
    REGISTER_CMD_PARAM(restart_cmd_t,_restarter,_env);
    REGISTER_CMD_PARAM(info_cmd_t,_informer,_env);
    REGISTER_CMD_PARAM(stats_cmd_t,_stater,_env);
    REGISTER_CMD_PARAM(smstats_cmd_t,_smstater,_env);
    REGISTER_CMD_PARAM(dump_cmd_t,_dumper,_env);
    REGISTER_CMD_PARAM(fake_iodelay_cmd_t,_fakeioer,_env);
    REGISTER_CMD_PARAM(freq_cmd_t,_freqer,_env);
    REGISTER_CMD_PARAM(skew_cmd_t,_skewer,_env);
    REGISTER_CMD_PARAM(stats_verbose_cmd_t,_stats_verboser,_env);
    REGISTER_CMD_PARAM(db_print_cmd_t,_db_printer,_env);
    REGISTER_CMD_PARAM(db_fetch_cmd_t,_db_fetch,_env);

    REGISTER_CMD_PARAM(log_cmd_t,_logger,_env);
    REGISTER_CMD_PARAM(asynch_cmd_t,_asyncher,_env);

#ifndef CFG_SHORE_6
    REGISTER_CMD_PARAM(fake_logdelay_cmd_t,_fakelogdelayer,_env);
#endif
	
    REGISTER_CMD_PARAM(sli_cmd_t,_slier,_env);
    REGISTER_CMD_PARAM(elr_cmd_t,_elrer,_env);

#ifdef CFG_BT
    REGISTER_CMD_PARAM(bt_cmd_t,_bter,_env);
#endif

    REGISTER_CMD_PARAM(measure_cmd_t,_measurer,this);
    REGISTER_CMD_PARAM(test_cmd_t,_tester,this);
    REGISTER_CMD_PARAM(warmup_cmd_t,_warmuper,this);
    REGISTER_CMD_PARAM(load_cmd_t,_loader,this);
    REGISTER_CMD_PARAM(trxs_cmd_t,_trxser,this);

    return (0);
}






/*********************************************************************
 *
 *  "restart" command
 *
 *********************************************************************/

void restart_cmd_t::setaliases() 
{ 
    _name = string("restart"); 
    _aliases.push_back("restart"); 
}

int restart_cmd_t::handle(const char* /* cmd */) 
{ 
    assert (_env); 
    _env->stop(); 
    _env->start(); 
    return (SHELL_NEXT_CONTINUE);
}

void restart_cmd_t::usage() 
{ 
    TRACE( TRACE_ALWAYS, "usage: info\n"); 
}
    
string restart_cmd_t::desc() const 
{ 
    return (string("Restart")); 
}  




/*********************************************************************
 *
 *  "info" command
 *
 *********************************************************************/

void info_cmd_t::setaliases() 
{ 
    _name = string("info"); 
    _aliases.push_back("info"); 
    _aliases.push_back("i"); 
}

int info_cmd_t::handle(const char* /* cmd */) 
{ 
    assert (_env); 
    _env->info(); 
    return (SHELL_NEXT_CONTINUE); 
}

void info_cmd_t::usage() 
{ 
    TRACE( TRACE_ALWAYS, "usage: info\n"); 
}

string info_cmd_t::desc() const 
{ 
    return (string("Prints info about the state of db instance")); 
}




/*********************************************************************
 *
 *  "stats" command
 *
 *********************************************************************/

void stats_cmd_t::setaliases() 
{ 
    _name = string("stats"); 
    _aliases.push_back("stats"); 
    _aliases.push_back("st"); 
}

int stats_cmd_t::handle(const char* /* cmd */) 
{ 
    assert (_env); 
    _env->statistics(); 
    return (SHELL_NEXT_CONTINUE);
}

void stats_cmd_t::usage() 
{ 
    TRACE( TRACE_ALWAYS, "usage: stats\n"); 
}
    
string stats_cmd_t::desc() const 
{
    return (string("Prints gathered statistics")); 
}



/*********************************************************************
 *
 *  "smstats" command
 *
 *********************************************************************/

void smstats_cmd_t::setaliases() 
{ 
    _name = string("smstats"); 
    _aliases.push_back("smst"); 
}

int smstats_cmd_t::handle(const char* /* cmd */) 
{ 
    assert (_env); 
    _env->gatherstats_sm(); 
    return (SHELL_NEXT_CONTINUE);
}

void smstats_cmd_t::usage() 
{ 
    TRACE( TRACE_ALWAYS, "usage: smstats\n"); 
}
    
string smstats_cmd_t::desc() const 
{
    return (string("Prints gathered statistics from the SM")); 
}




/*********************************************************************
 *
 *  "dump" command
 *
 *********************************************************************/

void dump_cmd_t::setaliases() 
{ 
    _name = string("dump"); 
    _aliases.push_back("dump"); 
    _aliases.push_back("d"); 
}

int dump_cmd_t::handle(const char* /* cmd */) 
{ 
    assert (_env); 
    _env->dump(); 
    return (SHELL_NEXT_CONTINUE); 
}

void dump_cmd_t::usage() 
{ 
    TRACE( TRACE_ALWAYS, "usage: dump\n"); 
}

string dump_cmd_t::desc() const 
{ 
    return (string("Dumps db instance data")); 
}
    



/*********************************************************************
 *
 *  "iodelay" command
 *
 *********************************************************************/

void fake_iodelay_cmd_t::setaliases() 
{ 
    _name = string("iodelay"); 
    _aliases.push_back("iodelay"); 
    _aliases.push_back("io"); 
}

int fake_iodelay_cmd_t::handle(const char* cmd)
{
    char cmd_tag[SERVER_COMMAND_BUFFER_SIZE];
    char iodelay_tag[SERVER_COMMAND_BUFFER_SIZE];    
    if ( sscanf(cmd, "%s %s", cmd_tag, iodelay_tag) < 2) {
        // prints all the env
        usage();
        return (SHELL_NEXT_CONTINUE);
    }
    assert (_env);
    int delay = atoi(iodelay_tag);
    if (!delay>0) {
        _env->disable_fake_disk_latency();
    }
    else {
        _env->enable_fake_disk_latency(delay);
    }
    return (SHELL_NEXT_CONTINUE);
}


void fake_iodelay_cmd_t::usage(void)
{
    TRACE( TRACE_ALWAYS, "IODELAY Usage:\n\n"                           \
           "*** iodelay <DELAY>\n"                                      \
           "\nParameters:\n"                                            \
           "<DELAY> - the enforced fake io delay, if 0 disables fake io delay\n\n");
}

string fake_iodelay_cmd_t::desc() const 
{ 
    return (string("Sets the fake I/O disk delay")); 
}



/*********************************************************************
 *
 *  "freq" command
 *
 *  Sets the frequencies in the INS/DEL/PROBE mix workloads
 *
 *********************************************************************/

void freq_cmd_t::setaliases() 
{ 
    _name = string("freq"); 
    _aliases.push_back("freq"); 
}

int freq_cmd_t::handle(const char* cmd)
{
    char cmd_tag[SERVER_COMMAND_BUFFER_SIZE];
    char sInsertFreq[SERVER_COMMAND_BUFFER_SIZE];    
    char sDeleteFreq[SERVER_COMMAND_BUFFER_SIZE];    

    if ( sscanf(cmd, "%s %s %s", cmd_tag, sInsertFreq, sDeleteFreq) < 3) {
        // prints all the env
        usage();
        return (SHELL_NEXT_CONTINUE);
    }
    assert (_env);

    int insert_freq = atoi(sInsertFreq);
    int delete_freq = atoi(sDeleteFreq);

    // Insert freq is [0,100]
    insert_freq= (insert_freq<0) ? 0 : insert_freq;
    insert_freq= (insert_freq>100) ? 100 : insert_freq;
    
    // Delete freq is [0,100-insert_freq]                
    delete_freq= (delete_freq<0) ? 0 : delete_freq;
    delete_freq= (insert_freq+delete_freq>100) ? (100-insert_freq)  : delete_freq;

    // Probe is the rest, [0,100-insert_freq-delete_freq]               
    int probe_freq = 100-insert_freq-delete_freq;

    TRACE( TRACE_ALWAYS, "Setting frequencies I=%d%% D=%d%% P=%d%%\n",
           insert_freq, delete_freq, probe_freq);
    _env->set_freqs( insert_freq, delete_freq, probe_freq);
    return (SHELL_NEXT_CONTINUE);
}


void freq_cmd_t::usage(void)
{
    TRACE( TRACE_ALWAYS, "FREQ Usage:\n\n"                              \
           "*** freq <INSERT_FREQ> <DELETE_FREQ>\n"                     \
           "\nParameters:\n"                                            \
           "<INSERT_FREQ> - The frequency of the insertions\n"          \
           "<DELETE_FREQ> - The frequency of the deletions\n\n"         \
           "The PROB_FREQ wil be 100 - INSERT_FREQ - DELETE_FREQ\n\n");
}

string freq_cmd_t::desc() const 
{ 
    return (string("Sets the frequency of INS/DEL/PROBES, used by some workloads")); 
}


/*********************************************************************
 *
 *  "skew" command
 *
 *  Sets the load imbalance related values
 *
 *********************************************************************/

void skew_cmd_t::setaliases() 
{ 
    _name = string("skew"); 
    _aliases.push_back("skew"); 
}

int skew_cmd_t::handle(const char* cmd)
{
    char cmd_tag[SERVER_COMMAND_BUFFER_SIZE];
    char sArea[SERVER_COMMAND_BUFFER_SIZE];    
    char sLoad[SERVER_COMMAND_BUFFER_SIZE];
    char sTime[SERVER_COMMAND_BUFFER_SIZE];

    if ( sscanf(cmd, "%s %s %s %s", cmd_tag, sArea, sLoad, sTime) < 4) {
        // prints all the env
        usage();
	_env->reset_skew();
        return (SHELL_NEXT_CONTINUE);
    }
    assert (_env);

    int area = atoi(sArea);
    int load = atoi(sLoad);
    int time = atoi(sTime);

    // Area percentage is [0,100]
    area = (area<0) ? 0 : area;
    area = (area>100) ? 100 : area;

    // Load percentage is [0,100]
    load = (load<0) ? 0 : load;
    load = (load>100) ? 100 : load;

    TRACE( TRACE_ALWAYS, "Setting load imbalance Area=%d%% Load=%d%% Start=%dsec\n", area, load, time);
    _env->set_skew(area, load, time);
    return (SHELL_NEXT_CONTINUE);
}


void skew_cmd_t::usage(void)
{
    TRACE( TRACE_ALWAYS, "SKEW Usage:\n\n"                              \
           "*** skew <AREA> <LOAD> <START_TIME>\n"                     \
           "\nParameters:\n"                                            \
           "<AREA> - Percentage of the area that will be affected by the given load\n"          \
           "<LOAD> - Work load of the given area\n"         \
           "<START_TIME> - After this many seconds the above the load change will be applied\n\n" \
	   "DISABLED\n\n");
}

string skew_cmd_t::desc() const 
{ 
    return (string("Sets the load of a particular percentage of records in the database, used by some workloads")); 
}


/*********************************************************************
 *
 *  "stats_verbose" command
 *
 *  More statistics printed during the measurement
 *
 *********************************************************************/

void stats_verbose_cmd_t::setaliases() 
{ 
    _name = string("stats_verbose"); 
    _aliases.push_back("stats_verbose"); 
}

int stats_verbose_cmd_t::handle(const char* cmd)
{
    char cmd_tag[SERVER_COMMAND_BUFFER_SIZE];
    char s[SERVER_COMMAND_BUFFER_SIZE];    

    if ( sscanf(cmd, "%s %s", cmd_tag, s) < 2) {
        // prints all the env
        usage();
        return (SHELL_NEXT_CONTINUE);
    }
    assert (_env);

    if(0 == strcasecmp("on", s)) {
	_g_mon->set_print_verbose(true);
	TRACE( TRACE_ALWAYS, "Verbose statistics will be printed every second during a measurement!\n" );
    } else if(0 == strcasecmp("off", s)) {
	_g_mon->set_print_verbose(false);
	TRACE( TRACE_ALWAYS, "Verbose statistics off!\n" );
    } else {
	usage();
    }

    return (SHELL_NEXT_CONTINUE);
}


void stats_verbose_cmd_t::usage(void)
{
    TRACE( TRACE_ALWAYS, "STATS_VERBOSE Usage:\n\n"                              \
           "*** stats_verbose <MODE>\n"                     \
           "\nParameters:\n"                                            \
           "<MODE> - \"ON\" starts to print verbose statistics every second during a measurement. " \
	   "\"OFF\" disables it.\n\n");
}

string stats_verbose_cmd_t::desc() const 
{ 
    return (string("To start-stop printing more statistics during a measurement.")); 
}


/*********************************************************************
 *
 *  "db_print" command
 *
 *  Prints the contents of the current db tables into files 
 *
 *********************************************************************/

void db_print_cmd_t::setaliases() 
{ 
    _name = string("db_print"); 
    _aliases.push_back("db_print"); 
}

int db_print_cmd_t::handle(const char* cmd)
{
    char cmd_tag[SERVER_COMMAND_BUFFER_SIZE];
    char lines_c[SERVER_COMMAND_BUFFER_SIZE];

    if ( sscanf(cmd, "%s %s", cmd_tag, lines_c) < 1) {
        // prints all the env
        usage();
        return (SHELL_NEXT_CONTINUE);
    }
    assert (_env);

    int lines_i = atoi(lines_c);
    _env->db_print_init(lines_i);
	
    return (SHELL_NEXT_CONTINUE);
}


void db_print_cmd_t::usage(void)
{
    TRACE( TRACE_ALWAYS, "DB_PRINT Usage:\n\n"				\
	   "*** db_print <NUM_LINES>\n"					\
	   "\nParamaters:\n"						\
	   "<NUM_LINES> - Number of lines to be written to a table's file before passing to another file\n\n");
}

string db_print_cmd_t::desc() const 
{ 
    return (string("To print the contents of the current db tables into files.")); 
}


/*********************************************************************
 *
 *  "db_fetch" command
 *
 *  Fetches the pages of the current db tables and their indexes into the buffer pool
 *
 *********************************************************************/

void db_fetch_cmd_t::setaliases() 
{ 
    _name = string("db_fetch"); 
    _aliases.push_back("db_fetch"); 
}

int db_fetch_cmd_t::handle(const char* cmd)
{
    assert (_env);
    _env->db_fetch_init();
    return (SHELL_NEXT_CONTINUE);
}


void db_fetch_cmd_t::usage(void)
{
    TRACE( TRACE_ALWAYS, "DB_FETCH Usage:\n\n*** db_fetch\n\n");
}

string db_fetch_cmd_t::desc() const 
{ 
    return (string("To fetch  the pages of the current db tables and their indexes into the buffer pool.")); 
}


#ifndef CFG_SHORE_6
/*********************************************************************
 *
 *  "fake_logdelay" command
 *
 *********************************************************************/

void fake_logdelay_cmd_t::setaliases() 
{ 
    _name = string("logdelay"); 
    _aliases.push_back("logdelay"); 
}

int fake_logdelay_cmd_t::handle(const char* cmd)
{
#ifdef CFG_SHORE_6
    TRACE( TRACE_ALWAYS, "In Shore-SM doing nothing\n"); 
    return (SHELL_NEXT_CONTINUE);
#endif

    char logdelay_tag[SERVER_COMMAND_BUFFER_SIZE];    
    if ( sscanf(cmd, "%*s %s", logdelay_tag) < 1) {
        // prints all the pssm
        usage();
        return (SHELL_NEXT_CONTINUE);
    }
    
    if(0 == strcasecmp("on", logdelay_tag)) {
	W_COERCE(ss_m::enable_fake_log_latency());
    }
    else if(0 == strcasecmp("off", logdelay_tag)) {
	W_COERCE(ss_m::disable_fake_log_latency());
    }
    else if(0 == strcasecmp("get", logdelay_tag)) {
	// do nothing...
    }
    else {
	int delay = atoi(logdelay_tag);
	if(delay < 0 || delay > 1000*1000) {
	    usage();
	}
	else {
	    W_COERCE(ss_m::set_fake_log_latency(delay));
	    if (!delay>0) {
		W_COERCE(ss_m::disable_fake_log_latency());
		W_COERCE(ss_m::set_fake_log_latency(0));
	    }
	    else {
		W_COERCE(ss_m::set_fake_log_latency(delay));
		W_COERCE(ss_m::enable_fake_log_latency());
	    }
	}
    }

    int tmp=0;
    bool enabled = false;
    W_COERCE(ss_m::get_fake_log_latency(enabled, tmp));
    TRACE( TRACE_ALWAYS, "LOGDELAY=%d (%s)\n", tmp, enabled? "enabled" : "disabled");
    return (SHELL_NEXT_CONTINUE);
}

void fake_logdelay_cmd_t::usage()
{
    TRACE( TRACE_ALWAYS, "LOGDELAY Usage:\n\n"                           \
           "*** logdelay <DELAY>\n"                                      \
           "\nParameters:\n"                                            \
           "<DELAY> - the enforced fake io delay, if 0 disables fake io delay\n\n");
}

string fake_logdelay_cmd_t::desc() const
{
    return string("Sets the fake log disk delay");
}

#endif



/*********************************************************************
 *
 *  "log" command
 *
 *********************************************************************/

void log_cmd_t::setaliases() 
{ 
    _name = string("log"); 
    _aliases.push_back("log");
    _aliases.push_back("l");
}

int log_cmd_t::handle(const char* cmd)
{
    char cmd_tag[SERVER_COMMAND_BUFFER_SIZE];
    char level_tag[SERVER_COMMAND_BUFFER_SIZE];

    if ( sscanf(cmd, "%s %s", cmd_tag, level_tag) < 2) {
        usage();
    }
    else {
	if(strcasecmp("get", level_tag)) {
	    rc_t err = _env->db()->set_log_features(level_tag);
	    if(err.is_error()) {
		TRACE( TRACE_ALWAYS, "*** Invalid feature set: %s ***", level_tag);
	    }
	}
	char const* level = _env->db()->get_log_features();
        TRACE( TRACE_ALWAYS, "Enabled log features: %s\n", level);
	delete[] level;
    }

    return (SHELL_NEXT_CONTINUE);
}

void log_cmd_t::usage(void)
{
    TRACE( TRACE_ALWAYS, "LOG Usage:\n\n"
           "*** log <feature-list>\n"
	   "\n"
           "<feature-list> is a case- and order-insensitive string containing zero or more\n"
	   "of the following logging features (note dependencies):\n"
	   "	C: C-Array\n"
	   "	    F: Fastpath\n"
	   "	    L: Print LSN groups (not fully implemented yet)\n"
	   "	D: Decoupled memcpy\n"
	   "	    M: MCS exposer\n"
	   "	        E: Exposure groups\n"
	   "	            X: Print Exposure groups (not fully implemented yet)\n"
	   "\n"
	   );
}

string log_cmd_t::desc() const
{ 
    return (string("Sets the logging mechanism")); 
}


/*********************************************************************
 *
 *  "asynch" command
 *
 *********************************************************************/

asynch_cmd_t::asynch_cmd_t(ShoreEnv* env) 
    : _env(env), _enabled(false) 
{ }

void asynch_cmd_t::setaliases() 
{ 
    _name = string("asynch"); 
    _aliases.push_back("asynch"); 
}

int asynch_cmd_t::handle(const char* /* cmd */)
{
    _enabled = !_enabled;    
    _env->setAsynchCommit(_enabled);
    TRACE( TRACE_ALWAYS, "AsynchCommit=%d\n", _enabled);
    return (SHELL_NEXT_CONTINUE);
}

void asynch_cmd_t::usage()
{
    TRACE( TRACE_ALWAYS, "Asynch Usage:\n\n"       \
           "*** asynch\n\n");
}


string asynch_cmd_t::desc() const
{
    return (string("Sets the fake log disk delay"));
}



/*********************************************************************
 *
 *  "sli" command
 *
 *********************************************************************/

sli_cmd_t::sli_cmd_t(ShoreEnv* env) 
    : _env(env), _enabled(false) 
{ }

void sli_cmd_t::setaliases() 
{ 
    _name = string("sli"); 
    _aliases.push_back("sli"); 
}

int sli_cmd_t::handle(const char* /* cmd */)
{
    _enabled = !_enabled;    
    _env->db()->set_sli_enabled(_enabled);
    _env->setSLIEnabled(_enabled);
    TRACE( TRACE_ALWAYS, "SLI=%d\n", _enabled);
    return (SHELL_NEXT_CONTINUE);
}

void sli_cmd_t::usage()
{
    TRACE( TRACE_ALWAYS, "SLI Usage:\n\n"       \
           "*** sli\n\n");
}

string sli_cmd_t::desc() const 
{ 
    return (string("Enables/disables SLI")); 
}




/*********************************************************************
 *
 *  "elr" command
 *
 *********************************************************************/

elr_cmd_t::elr_cmd_t(ShoreEnv* env) 
    : _env(env), _enabled(false) 
{ }

void elr_cmd_t::setaliases() 
{ 
    _name = string("elr"); 
    _aliases.push_back("elr"); 
}

int elr_cmd_t::handle(const char* /* cmd */)
{
    _enabled = !_enabled;    
    _env->db()->set_elr_enabled(_enabled);
    _env->setELREnabled(_enabled);
    TRACE( TRACE_ALWAYS, "ELR=%d\n", _enabled);
    return (SHELL_NEXT_CONTINUE);
}

void elr_cmd_t::usage(void)
{
    TRACE( TRACE_ALWAYS, "ELR Usage:\n\n"       \
           "*** elr\n\n");
}

string elr_cmd_t::desc() const 
{ 
    return (string("Enables/disables ELR")); 
}




#ifdef CFG_BT
/*********************************************************************
 *
 *  "bt" command
 *
 *********************************************************************/

bt_cmd_t::bt_cmd_t(ShoreEnv* env) 
    : _env(env), _enabled(false) 
{ }

void bt_cmd_t::setaliases() 
{ 
    _name = string("bt"); 
    _aliases.push_back("bt"); 
}

#ifndef CFG_SHORE_6
int bt_cmd_t::handle(const char* cmd)
{
#define CMDLEN 10
#define FNLEN 100
    char subcmd[CMDLEN+1];
    char fname[FNLEN+1];
    int count = sscanf(cmd, "%*s %10s %100s", subcmd, fname);
    switch(count) {
    case 0:
	backtrace_set_enabled(!backtrace_get_enabled());
	break;
    case 1:
	if(0 == strcmp("on", subcmd))
	    backtrace_set_enabled(true);
	else if(0 == strcmp("off", subcmd))
	    backtrace_set_enabled(false);
	else {
	    TRACE(TRACE_ALWAYS, "Invalid argument: %s\n", cmd);
	}
	break;
    case 2:
	if(0 == strcmp("print", subcmd)) {
	    if(0 < strlen(fname)) {
		FILE* f = fopen(fname, "w");
		backtrace_print_all(f);
		fclose(f);
	    }
	    break;
	}
	// else fall through
    default:
	TRACE(TRACE_ALWAYS, "Invalid argument: %s\n", cmd);
	break;
    }
    TRACE( TRACE_ALWAYS, "BT=%d\n", backtrace_get_enabled());
    return (SHELL_NEXT_CONTINUE);
}

void bt_cmd_t::usage()
{
    TRACE( TRACE_ALWAYS, "BT Usage:\n\n"        \
           "*** bt [on|off|print fname]\n\n");
}

string bt_cmd_t::desc() const 
{
    return (string("Enables/disables/prints collection of backtrace information"));
}

#else

int bt_cmd_t::handle(const char* cmd)
{
#define CMDLEN 10
    uint plevel=0;
    int count = sscanf(cmd, "%*s %ud", &plevel);
    switch(count) {
    case 0:
	usage();
	break;
    case 1:
        ss_m::set_plp_tracing(plevel);
        TRACE( TRACE_ALWAYS, "PLEVEL=%d\n", plevel);
        break;
    default:
	TRACE(TRACE_ALWAYS, "Invalid argument: %s\n", cmd);
	break;
    }
    return (SHELL_NEXT_CONTINUE);
}

void bt_cmd_t::usage()
{
    TRACE( TRACE_ALWAYS, "BT Usage:\n\n"        \
           "*** bt <tracing-level (int)>\n\n");
}

string bt_cmd_t::desc() const 
{
    return (string("Enables/disables PLP tracing at shore-sm-6"));
}

#endif // CFG_SHORE_6
#endif // CFG_BT




/*********************************************************************
 *
 *  "measure" command
 *
 *********************************************************************/

void measure_cmd_t::setaliases() 
{
    _name = string("measure"); 
    _aliases.push_back("measure"); 
    _aliases.push_back("m"); 
}

int measure_cmd_t::handle(const char* cmd) 
{
    _kit->pre_process_cmd();
    return (_kit->process_cmd_MEASURE(cmd)); 
}

void measure_cmd_t::usage() 
{ 
    TRACE( TRACE_ALWAYS, "MEASURE Usage:\n\n"                           \
           "*** measure <NUM_QUERIED> [<SPREAD> <NUM_THRS> <DURATION> <TRX_ID> <ITERATIONS> <BINDING>]\n" \
           "\nParameters:\n"                                            \
           "<NUM_QUERIED> : The SF queried (queried factor)\n"          \
           "<SPREAD>      : Whether to spread threads (0=No, Other=Yes, Default=No) (optional)\n" \
           "<NUM_THRS>    : Number of threads used (optional)\n"        \
           "<DURATION>    : Duration of experiment in secs (Default=20) (optional)\n" \
           "<TRX_ID>      : Transaction ID to be executed (0=mix) (optional)\n" \
           "<ITERATIONS>  : Number of iterations (Default=5) (optional)\n" \
           "<BINDING>     : Binding Type (Default=0-No binding) (optional)\n\n");
}

string measure_cmd_t::desc() const 
{
    return string("Duration-based Measurement (powerrun)");
}




/*********************************************************************
 *
 *  "test" command
 *
 *********************************************************************/

void test_cmd_t::setaliases() 
{ 
    _name = string("test"); 
    _aliases.push_back("test"); 
}

int test_cmd_t::handle(const char* cmd) 
{
    _kit->pre_process_cmd();
    return (_kit->process_cmd_TEST(cmd));
}

void test_cmd_t::usage()
{
    TRACE( TRACE_ALWAYS, "TEST Usage:\n\n" \
           "*** test <NUM_QUERIED> [<SPREAD> <NUM_THRS> <NUM_TRXS> <TRX_ID> <ITERATIONS> <BINDING>]\n" \
           "\nParameters:\n" \
           "<NUM_QUERIED> : The SF queried (queried factor)\n" \
           "<SPREAD>      : Whether to spread threads (0=No, Other=Yes, Default=No) (optional)\n" \
           "<NUM_THRS>    : Number of threads used (optional)\n" \
           "<NUM_TRXS>    : Number of transactions per thread (optional)\n" \
           "<TRX_ID>      : Transaction ID to be executed (0=mix) (optional)\n" \
           "<ITERATIONS>  : Number of iterations (Default=5) (optional)\n" \
           "<BINDING>     : Binding Type (Default=0-No binding) (optional)\n\n");
}

string test_cmd_t::desc() const 
{
    return string("NumOfXcts-based Measument (powerrun)");
}




/*********************************************************************
 *
 *  "warmup" command
 *
 *********************************************************************/

void warmup_cmd_t::setaliases()
{
    _name = string("warmup"); 
    _aliases.push_back("warmup");
}

int warmup_cmd_t::handle(const char* cmd) 
{ 
    _kit->pre_process_cmd();
    return (_kit->process_cmd_WARMUP(cmd)); 
}

void warmup_cmd_t::usage() 
{ 
    TRACE( TRACE_ALWAYS, "WARMUP Usage:\n\n" \
           "*** warmup [<NUM_QUERIED> <NUM_TRXS> <DURATION> <ITERATIONS>]\n" \
           "\nParameters:\n" \
           "<NUM_QUERIED> : The SF queried (Default=10) (optional)\n" \
           "<NUM_TRXS>    : Number of transactions per thread (Default=1000) (optional)\n" \
           "<DURATION>    : Duration of experiment in secs (Default=20) (optional)\n" \
           "<ITERATIONS>  : Number of iterations (Default=3) (optional)\n\n");
}

string warmup_cmd_t::desc() const 
{
    return string("Does a preselected warmup run");
}




/*********************************************************************
 *
 *  "load" command
 *
 *********************************************************************/

void load_cmd_t::setaliases()
{
    _name = string("load"); 
    _aliases.push_back("load");
}

int load_cmd_t::handle(const char* cmd) 
{ 
    _kit->pre_process_cmd();
    return (_kit->process_cmd_LOAD(cmd)); 
}

void load_cmd_t::usage() 
{ 
    TRACE( TRACE_ALWAYS, "LOAD Usage:\n\n" \
           "*** load\n");
}

string load_cmd_t::desc() const 
{
    return string("Loads a database for the benchmark");
}




/*********************************************************************
 *
 *  "trxs" command
 *
 *********************************************************************/

void trxs_cmd_t::setaliases()
{
    _name = string("trxs"); 
    _aliases.push_back("trxs");
}

int trxs_cmd_t::handle(const char* /* cmd */) 
{ 
    _kit->print_sup_trxs();
    return (SHELL_NEXT_CONTINUE); 
}

void trxs_cmd_t::usage() 
{ 
    TRACE( TRACE_ALWAYS, "usage: trxs\n"); 
}

string trxs_cmd_t::desc() const 
{
    return string("Lists the available transactions in the benchmark");
}



EXIT_NAMESPACE(shore);

