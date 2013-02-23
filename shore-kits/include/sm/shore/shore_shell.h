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

/** @file:   shore_shell.h
 *
 *  @brief:  Abstract shell class for Shore environments 
 *
 *  @author: Ippokratis Pandis, Sept 2008
 */

#ifndef __SHORE_SHELL_H
#define __SHORE_SHELL_H

#include "k_defines.h"

#include "util/shell.h"

#include "sm/shore/shore_env.h"
#include "sm/shore/shore_helper_loader.h"
#include "sm/shore/shore_client.h"


ENTER_NAMESPACE(shore);


// Globals

using std::map;

extern "C" void alarm_handler(int sig);

extern bool volatile _g_canceled;

class shore_shell_t;


// default database size (scaling factor)
const int DF_SF            = 10;
extern double    _theSF;

// Default values for the power-runs //

// default queried factor
const int DF_NUM_OF_QUERIED_SF    = 10;

// default transaction id to be executed
const int DF_TRX_ID                = -1;



// Declares commands that need only a pointer to the Enviroment object.
// Typically, those commands call the functions of the DB interface or
// interface the SM API.
#define DECLARE_ENV_CMD(name) \
    struct name##_cmd_t : public command_handler_t {    \
        ShoreEnv* _env;                                 \
        name##_cmd_t(ShoreEnv* aEnv) : _env(aEnv) { };  \
        ~name##_cmd_t() { }                             \
        void setaliases();                              \
        int handle(const char* cmd);                    \
        void usage();                                   \
        string desc() const; }


DECLARE_ENV_CMD(restart);
DECLARE_ENV_CMD(info);
DECLARE_ENV_CMD(stats);
DECLARE_ENV_CMD(smstats);
DECLARE_ENV_CMD(dump);
DECLARE_ENV_CMD(fake_iodelay);
DECLARE_ENV_CMD(freq);
DECLARE_ENV_CMD(skew);
DECLARE_ENV_CMD(db_print);
DECLARE_ENV_CMD(db_fetch);
DECLARE_ENV_CMD(stats_verbose);
DECLARE_ENV_CMD(fake_logdelay);
DECLARE_ENV_CMD(log);



// Declares commands that need a pointer to the Enviroment object and
// a boolean that keeps the state whether something is turned on or off.
// Typically, those commands interface the SM API, such as SLI, ELR which 
// can be turned on/off. The constructor is allowed to be set at the 
// definition so that we can choose the initial state of the boolean.
#define DECLARE_ENV_ONOFF_CMD(name) \
    struct name##_cmd_t : public command_handler_t {    \
        ShoreEnv* _env;                                 \
        bool _enabled;                                  \
        name##_cmd_t(ShoreEnv* aEnv);                   \
        ~name##_cmd_t() { }                             \
        void setaliases();                              \
        int handle(const char* cmd);                    \
        void usage();                                   \
        string desc() const; }


DECLARE_ENV_ONOFF_CMD(asynch);

DECLARE_ENV_ONOFF_CMD(sli);
DECLARE_ENV_ONOFF_CMD(elr);

#ifdef CFG_BT
DECLARE_ENV_ONOFF_CMD(bt);
#endif // CFG_BT



// Declares commands that need a pointer to the Enviroment object and
// an member variable that keeps the state.
#define DECLARE_ENV_VAR_CMD(name,vartype)                       \
    struct name##_cmd_t : public command_handler_t {            \
        ShoreEnv* _env;                                         \
        vartype _state;                                         \
        name##_cmd_t(ShoreEnv* aEnv, const vartype aState);     \
        ~name##_cmd_t() { }                                     \
        void setaliases();                                      \
        int handle(const char* cmd);                            \
        void usage();                                           \
        string desc() const; }

// Currently no command in that category


// Declares commands that need only a pointer to a ShoreShell object.
#define DECLARE_KIT_CMD(name) \
    struct name##_cmd_t : public command_handler_t {            \
        shore_shell_t* _kit;                                    \
        name##_cmd_t(shore_shell_t* aKit) : _kit(aKit) { };     \
        ~name##_cmd_t() { }                                     \
        void setaliases();                                      \
        int handle(const char* cmd);                            \
        void usage();                                           \
        string desc() const; }


DECLARE_KIT_CMD(measure);
DECLARE_KIT_CMD(test);
DECLARE_KIT_CMD(warmup);
DECLARE_KIT_CMD(load);
DECLARE_KIT_CMD(trxs);




/*********************************************************************
 *
 *  @abstract class: shore_shell_t
 *
 *  @brief: Base class for shells for Shore environment
 *
 *  @usage: - Inherit from this class
 *          - Implement the process_{TEST/MEASURE/WARMUP} fuctions
 *          - Call the start() function
 *
 *
 *  @note:  Supported commands - { TEST/MEASURE/WARMUP/LOAD }
 *  @note:  To add new command function process_command() should be overridden 
 *  @note:  SIGINT handling
 *
 *********************************************************************/

class shore_shell_t : public shell_t 
{
protected:

    ShoreEnv* _env;

    processorid_t _start_prs_id;
    processorid_t _current_prs_id;    

    // supported trxs
    typedef map<int,string>            mapSupTrxs;
    typedef mapSupTrxs::iterator       mapSupTrxsIt;
    typedef mapSupTrxs::const_iterator mapSupTrxsConstIt;

    mapSupTrxs _sup_trxs;

    // supported binding policies
    typedef map<eBindingType,string>   mapBindPols;
    typedef mapBindPols::iterator      mapBindPolsIt;    
    mapBindPols _sup_bps;


    // supported cmds
    guard<restart_cmd_t>        _restarter;
    guard<info_cmd_t>           _informer;
    guard<stats_cmd_t>          _stater;
    guard<smstats_cmd_t>        _smstater;
    guard<dump_cmd_t>           _dumper;
    guard<fake_iodelay_cmd_t>   _fakeioer;   
    guard<freq_cmd_t>           _freqer;
    guard<skew_cmd_t>           _skewer;
    guard<stats_verbose_cmd_t>  _stats_verboser;
    guard<db_print_cmd_t>       _db_printer;
    guard<db_fetch_cmd_t>       _db_fetch;
    
    guard<log_cmd_t>            _logger;
    guard<asynch_cmd_t>         _asyncher;

#ifndef CFG_SHORE_6
    guard<fake_logdelay_cmd_t>  _fakelogdelayer;   
#endif

    guard<sli_cmd_t>            _slier;
    guard<elr_cmd_t>            _elrer;

#ifdef CFG_BT
    guard<bt_cmd_t>             _bter;
#endif

    guard<measure_cmd_t>        _measurer;
    guard<test_cmd_t>           _tester;
    guard<warmup_cmd_t>         _warmuper;
    guard<load_cmd_t>           _loader;
    guard<trxs_cmd_t>           _trxser;

public:

    shore_shell_t(const char* prompt, 
                  const bool netmode,
                  const int netport,                  
                  const bool inputfilemode, 
                  const string inputfile,
                  processorid_t acpustart = PBIND_NONE);
    virtual ~shore_shell_t();

    // access methods
    inline ShoreEnv* db() { return(_env); }

    // shell interface
    virtual void pre_process_cmd();
    virtual int print_usage(const char* command);
    virtual int SIGINT_handler();

    virtual int register_commands();


    // Instanciate and close the Shore environment
    virtual int inst_test_env(int argc, char* argv[])=0;

    // supported trxs and binding policies
    virtual int load_trxs_map(void)=0;
    virtual int load_bp_map(void)=0;
    void print_sup_trxs(void) const;
    void print_sup_bp(void);
    const char* translate_trx(const int iSelectedTrx) const;
    const char* translate_bp(const eBindingType abt);


    // supported commands and their usage
    virtual int process_cmd_MEASURE(const char* command);
    virtual int process_cmd_TEST(const char* command);
    virtual int process_cmd_WARMUP(const char* command);    
    virtual int process_cmd_LOAD(const char* command);        


    // virtual implementation of the {WARMUP/TEST/MEASURE} 
    // WARMUP/LOAD are virtual
    // TEST/MEASURE are pure virtual
    virtual int _cmd_WARMUP_impl(const double iQueriedSF, const int iTrxs, 
                                 const int iDuration, const int iIterations);
    virtual int _cmd_LOAD_impl(void);

    virtual int _cmd_TEST_impl(const double iQueriedSF, const int iSpread,
                               const int iNumOfThreads, const int iNumOfTrxs,
                               const int iSelectedTrx, const int iIterations,
                               const eBindingType abt)=0;
    virtual int _cmd_MEASURE_impl(const double iQueriedSF, const int iSpread,
                                  const int iNumOfThreads, const int iDuration,
                                  const int iSelectedTrx, const int iIterations,
                                  const eBindingType abt)=0;    

    virtual w_rc_t prepareNewRun() { return (RCOK); }

    // for the client processor binding policy
    virtual processorid_t next_cpu(const eBindingType abt,
                                   const processorid_t aprd);


protected:
    
    void print_MEASURE_info(const double iQueriedSF, const int iSpread, 
                            const int iNumOfThreads, const int iDuration,
                            const int iSelectedTrx, const int iIterations,
                            const eBindingType abt);

    void print_TEST_info(const double iQueriedSF, const int iSpread, 
                         const int iNumOfThreads, const int iNumOfTrxs,
                         const int iSelectedTrx, const int iIterations,
                         const eBindingType abt);

}; // EOF: shore_shell_t


EXIT_NAMESPACE(shore);

#endif /* __TESTER_SHORE_SHELL_H */

