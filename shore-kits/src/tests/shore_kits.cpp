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

/** @file:   shore_kits.cpp
 *
 *  @brief:  Shore kits
 *
 *  @author: Ippokratis Pandis, Sept 2008
 *
 */

#include "sm/shore/shore_shell.h"

#include "workload/tpcc/shore_tpcc_env.h"
#include "workload/tpcc/shore_tpcc_client.h"

#include "workload/tm1/shore_tm1_env.h"
#include "workload/tm1/shore_tm1_client.h"

#include "workload/tpcb/shore_tpcb_env.h"
#include "workload/tpcb/shore_tpcb_client.h"

#include "workload/tpch/shore_tpch_env.h"
#include "workload/tpch/shore_tpch_client.h"

#include "workload/ssb/shore_ssb_env.h"
#include "workload/ssb/shore_ssb_client.h"

#include "workload/tpce/shore_tpce_env.h"
#include "workload/tpce/shore_tpce_client.h"

#ifdef CFG_SIMICS
#include "util/simics-magic-instruction.h"
#endif

#ifdef CFG_DORA
#include "dora.h"
#include "dora/tpcc/dora_tpcc.h"
#include "dora/tpcc/dora_tpcc_client.h"
#include "dora/tm1/dora_tm1.h"
#include "dora/tm1/dora_tm1_client.h"
#include "dora/tpcb/dora_tpcb.h"
#include "dora/tpcb/dora_tpcb_client.h"
#endif

#ifdef CFG_VTUNE
#include <ittnotify.h> // VTune API definitions
#endif

#include <dlfcn.h>

using namespace shore;

using namespace tpcc;
using namespace tm1;
using namespace tpcb;
using namespace tpch;
using namespace ssb;
using namespace tpce;


//////////////////////////////
// Prints out configuration info
#ifdef CFG_DORA
#warning Configuration: DORA enabled
using namespace dora;
#endif

#ifdef CFG_FLUSHER
#warning Configuration: FLUSHER/GROUPCOMMIT enabled
#endif

#warning Configuration: SLI enabled
#warning Configuration: ELR enabled

#ifdef CFG_SIMICS
#warning Configuration: With SIMICS MAGIC INSTRUCTIONS
#endif

#ifdef CFG_CACHES
#warning Configuration: With Object CACHES
#endif

#ifdef CFG_BT
#warning Configuration: Backtracing enabled
#endif

#ifdef CFG_QPIPE
#warning Configuration: QPipe enabled
#endif

#ifdef CFG_VTUNE
#warning Configuration: VTune controls enabled
#endif

//////////////////////////////

// Value-definitions of the different Sysnames
enum SysnameValue { snBaseline = 0x1,
                    snDORA     = 0x2,
                    snPLP      = 0x4
};

// Map to associate string with then enum values

static map<string,SysnameValue> mSysnameValue;

void initsysnamemap() 
{
    mSysnameValue["baseline"] = snBaseline;
    mSysnameValue["dora"]     = snDORA;
    mSysnameValue["plp"]      = snPLP;
}

//////////////////////////////


// Value-definitions of the different Benchmarks
enum BenchmarkValue { bmTPCC, bmTM1, bmTPCB, bmTPCH , bmSSB, bmTPCE };

// Map to associate string with then enum values

static map<string,BenchmarkValue> mBenchmarkValue;

void initbenchmarkmap() 
{
    mBenchmarkValue["tpcc"]  = bmTPCC;
    mBenchmarkValue["tm1"]   = bmTM1;
    mBenchmarkValue["tpcb"]  = bmTPCB;
    mBenchmarkValue["tpch"]  = bmTPCH;
    mBenchmarkValue["ssb"]   = bmSSB;
    mBenchmarkValue["tpce"]  = bmTPCE;
}



//////////////////////////////

template<class Client,class DB>
class kit_t : public shore_shell_t
{ 
private:
    DB* _dbinst;

public:

    kit_t(const char* prompt, 
          const bool netmode, const int port,
          const bool inputfilemode, const string inputfile) 
        : shore_shell_t(prompt,netmode,port,inputfilemode,inputfile)
    {
        // load supported trxs and binding policies maps
        load_trxs_map();
        load_bp_map();
    }
    ~kit_t() { }


    virtual int inst_test_env(int argc, char* argv[]);
    virtual int load_trxs_map();
    virtual int load_bp_map();


    // impl of supported commands
    virtual int _cmd_TEST_impl(const double iQueriedSF, const int iSpread,
                               const int iNumOfThreads, const int iNumOfTrxs,
                               const int iSelectedTrx, const int iIterations,
                               const eBindingType abt);
    virtual int _cmd_MEASURE_impl(const double iQueriedSF, const int iSpread,
                                  const int iNumOfThreads, const int iDuration,
                                  const int iSelectedTrx, const int iIterations,
                                  const eBindingType abt);

    virtual w_rc_t prepareNewRun() { assert(_dbinst); return(_dbinst->newrun()); }

}; // EOF: kit_t



/********************************************************************* 
 *
 *  @fn:      load_trxs_map
 *
 *  @brief:   Instanciates a client and calls the function that loads
 *            the map of supported trxs.
 *
 *  @returns: The number of supported trxs
 *
 *********************************************************************/

template<class Client,class DB>
int kit_t<Client,DB>::load_trxs_map(void)
{
    CRITICAL_SECTION(shell_cs,_lock);
    // gets the supported trxs from the client
    return (Client::load_sup_xct(_sup_trxs));
}

template<class Client,class DB>
int kit_t<Client,DB>::load_bp_map(void)
{
    CRITICAL_SECTION(shell_cs,_lock);
    // Basic binding policies
    _sup_bps[BT_NONE]          = "NoBinding";
    _sup_bps[BT_NEXT]          = "Adjacent";
    _sup_bps[BT_SPREAD]        = "SpreadToCores";
    return (_sup_bps.size());
}


/********************************************************************* 
 *
 *  @fn:      inst_test_env
 *
 *  @brief:   Instanciates the Shore environment, 
 *            Opens the database and sets the appropriate SFs
 *  
 *  @returns: 1 on error
 *
 *********************************************************************/

template<class Client,class DB>
int kit_t<Client,DB>::inst_test_env(int argc, char* argv[]) 
{    
    // 1. Instanciate the Shore Environment
    _env = _dbinst = new DB();
    assert (_g_mon);
    _env->set_max_cpu_count(_g_mon->get_num_of_cpus());

    // 2. Initialize the Shore Environment
    // the initialization must be executed in a shore context
    db_init_smt_t* initializer = new db_init_smt_t(c_str("init"), _env);
    assert (initializer);
    initializer->fork();
    initializer->join(); 
    int rv = initializer->rv();
    delete (initializer);
    initializer = NULL;

    if (rv) {
        TRACE( TRACE_ALWAYS, "Exiting...\n");
        return (rv);
    }

    // 3. set SF - if param valid
    assert (_dbinst);
    if (argc>2) {
        int queriedSF = atoi(argv[2]);
        _dbinst->set_qf(queriedSF);
    }
    _theSF = _dbinst->get_sf();


    // 4. Load supported trxs and bps
    if (!load_trxs_map()) {
        TRACE( TRACE_ALWAYS, "No supported trxs...\nExiting...");
        return (1);
    }
    if (!load_bp_map()) {
        TRACE( TRACE_ALWAYS, "No supported binding policies...\nExiting...");
        return (1);
    }

    // 5. Now that everything is set, register any additional commands
    shore_shell_t::register_commands();

    // 6. Start the VAS
    return (_dbinst->start());
}



namespace shore {
    extern void shell_expect_clients(int count);
    extern void shell_await_clients();
}



static const int MILLION = 1000000;

/********************************************************************* 
 *
 * COMMANDS
 *
 *********************************************************************/

// cmd: TEST

template<class Client,class DB>
int kit_t<Client,DB>::_cmd_TEST_impl(const double iQueriedSF, 
                                     const int iSpread,
                                     const int iNumOfThreads, 
                                     const int iNumOfTrxs,
                                     const int iSelectedTrx, 
                                     const int iIterations,
                                     const eBindingType abt)
{
    // print test information
    print_TEST_info(iQueriedSF, iSpread, iNumOfThreads, 
                    iNumOfTrxs, iSelectedTrx, iIterations, abt);

    _dbinst->upd_sf();
    _dbinst->set_qf(iQueriedSF);

    Client* testers[MAX_NUM_OF_THR];
    for (int j=0; j<iIterations && !base_client_t::is_test_aborted(); j++) {

        TRACE( TRACE_ALWAYS, "Iteration [%d of %d]\n",
               (j+1), iIterations);

        // reset starting cpu and wh id
        _current_prs_id = _start_prs_id;
        int wh_id = 0;

        _env->reset_stats();

        // reset monitor stats
        _g_mon->cntr_reset();

        // set measurement state to measure - start counting everything
	TRACE(TRACE_ALWAYS, "begin measurement\n");
        _env->set_measure(MST_MEASURE);
	stopwatch_t timer;

        // 1. create and fork client clients
        for (int i=0; i<iNumOfThreads; i++) {
            // create & fork testing threads
            if (iSpread) {
                wh_id = (i%(int)iQueriedSF)+1;
            }

            testers[i] = new Client(c_str("CL-%d",i), i, _dbinst, 
                                    MT_NUM_OF_TRXS, iSelectedTrx, iNumOfTrxs,
                                    _current_prs_id, wh_id, iQueriedSF);
            assert (testers[i]);
            testers[i]->fork();
            _current_prs_id = next_cpu(abt, _current_prs_id);
        }

        // 2. join the tester threads
        for (int i=0; i<iNumOfThreads; i++) {
            testers[i]->join();
            if (testers[i]->rv()) {
                TRACE( TRACE_ALWAYS, "Error in testing...\n");
                TRACE( TRACE_ALWAYS, "Exiting...\n");
                assert (false);
            }    
            delete (testers[i]);
        }

	double delay = timer.time();
        //xct_stats stats = shell_get_xct_stats();
        _g_mon->cntr_pause();
        ulong_t miochs = _g_mon->iochars()/MILLION;
	TRACE(TRACE_ALWAYS, "end measurement\n");
        _env->print_throughput(iQueriedSF,iSpread,iNumOfThreads,delay,
                               miochs, _g_mon->get_avg_usage(true));

        _g_mon->print_load(delay);
        _g_mon->print_ext_stats();
        
        // flush the log before the next iteration
	_env->set_measure(MST_PAUSE);
        TRACE( TRACE_DEBUG, "db checkpoint - start\n");
        _env->checkpoint();
        TRACE( TRACE_ALWAYS, "Checkpoint\n");
    }

    // set measurement state
    _env->set_measure(MST_DONE);

    // Prepare for the next round
    w_rc_t e = prepareNewRun();
    if (e.is_error()) {
        TRACE( TRACE_ALWAYS, "!!! Problem preparing for the next run\n");
    }

    return (SHELL_NEXT_CONTINUE);
}



// cmd: MEASURE

template<class Client,class DB>
int kit_t<Client,DB>::_cmd_MEASURE_impl(const double iQueriedSF, 
                                        const int iSpread,
                                        const int iNumOfThreads, 
                                        const int iDuration,
                                        const int iSelectedTrx, 
                                        const int iIterations,
                                        const eBindingType abt)
{
#ifdef CFG_VTUNE
    // to resume sampling
    __itt_resume();
#endif
    
    // print measurement info
    print_MEASURE_info(iQueriedSF, iSpread, iNumOfThreads, iDuration, 
                       iSelectedTrx, iIterations, abt);

    _dbinst->upd_sf();
    _dbinst->set_qf(iQueriedSF);

    Client* testers[MAX_NUM_OF_THR];
    _current_prs_id = _start_prs_id;     // reset starting cpu and wh id
    int wh_id = 0;
    
    // 1. prepare for measurement
    _env->set_measure(MST_WARMUP);
    shell_expect_clients(iNumOfThreads);

    // 2. create and fork client threads
    for (int i=0; i<iNumOfThreads; i++) {
        // create & fork testing threads
        if (iSpread) {
            wh_id = (i%(int)iQueriedSF)+1;
        }

        testers[i] = new Client(c_str("CL-%d",i), i, _dbinst, 
                                MT_TIME_DUR, iSelectedTrx, 0,
                                _current_prs_id, wh_id, iQueriedSF);
        assert (testers[i]);
        testers[i]->fork();
        _current_prs_id = next_cpu(abt, _current_prs_id);
    }        
    
    // give them some time (2secs) to start-up
    shell_await_clients();    
    
    // 2. run iterations
    int remaining = 0;
    double delay = 0;
    for (int j=0; j<iIterations && !base_client_t::is_test_aborted(); j++) {
	if(remaining == 0) {
	    sleep(1);
	    TRACE( TRACE_ALWAYS, "Iteration [%d of %d]\n",
		   (j+1), iIterations);

	    // reset cpu monitor
	    _g_mon->cntr_reset();

	    // set measurement state
	    TRACE(TRACE_ALWAYS, "begin measurement\n");
	    _env->set_measure(MST_MEASURE);

	    _env->reset_stats();
	    delay = 0;
	    remaining = iDuration;
	}
	stopwatch_t timer;
	remaining = sleep(remaining);
	if(remaining != 0) {
	    delay += timer.time();
	    j--;
	    continue;
	}
	delay += timer.time();
        _g_mon->cntr_pause();
	TRACE(TRACE_ALWAYS, "end measurement\n");
        ulong_t miochs = _g_mon->iochars()/MILLION;
        _env->print_throughput(iQueriedSF,iSpread,iNumOfThreads,delay,
                               miochs, _g_mon->get_avg_usage(true));

        _g_mon->print_load(delay);
        _g_mon->print_ext_stats();
	       
        // flush the log before the next iteration
	_env->set_measure(MST_PAUSE);
        TRACE( TRACE_DEBUG, "db checkpoint - start\n");
        _env->checkpoint();
        TRACE( TRACE_ALWAYS, "Checkpoint\n");
    }

    // 3. join the tester threads
    _env->set_measure(MST_DONE);
    for (int i=0; i<iNumOfThreads; i++) {
        testers[i]->join();
        //fprintf(stderr,".");
        if (testers[i]->rv()) {
            TRACE( TRACE_ALWAYS, "Error in testing...\n");
            assert (false);
        }    
        assert (testers[i]);
        delete (testers[i]);
    }

    // set measurement state
    _env->set_measure(MST_DONE);

    TRACE( TRACE_ALWAYS, "Preparing for the next run\n");

    // Prepare for the next round
    w_rc_t e = prepareNewRun();
    if (e.is_error()) {
        TRACE( TRACE_ALWAYS, "!!! Problem preparing for the next run\n");
    }

#ifdef CFG_VTUNE
    // to pause sampling
    __itt_pause();
#endif 
    
    return (SHELL_NEXT_CONTINUE);
}


///////////////////////////////
// Declare the possible kits //

// Baseline
typedef kit_t<baseline_tpcc_client_t,ShoreTPCCEnv> baselineTPCCKit;
typedef kit_t<baseline_tm1_client_t,ShoreTM1Env> baselineTM1Kit;
typedef kit_t<baseline_tpcb_client_t,ShoreTPCBEnv> baselineTPCBKit;
typedef kit_t<baseline_tpch_client_t,ShoreTPCHEnv> baselineTPCHKit;

#ifdef CFG_QPIPE
typedef kit_t<baseline_ssb_client_t,ShoreSSBEnv> baselineSSBKit;
#endif

typedef kit_t<baseline_tpce_client_t,ShoreTPCEEnv> baselineTPCEKit;


#ifdef CFG_DORA
typedef kit_t<dora_tpcc_client_t,DoraTPCCEnv> doraTPCCKit;
typedef kit_t<dora_tm1_client_t,DoraTM1Env> doraTM1Kit;
typedef kit_t<dora_tpcb_client_t,DoraTPCBEnv> doraTPCBKit;
#endif

////////////////////////////////

bool validatePhysical(string physical)
{   
    if (physical.compare("normal")==0) {
        return (true);
    }
    if (physical.compare("mrbtnorm")==0) {
        return (true);
    }
    if (physical.compare("mrbtpart")==0) {
        return (true);
    }
    if (physical.compare("mrbtleaf")==0) {
        return (true);
    }
    return (false);
}

void usage() 
{
    TRACE( TRACE_ALWAYS, "\nAccepted parameters:\n"                       \
           "-r            : Clobbber existing database\n"               \
           "-n            : Start in network mode (listens to port)\n"  \
           "-p <PORT>     : Listen to specific port\n"                  \
           "-c <CONFIG>   : Use specific configuration\n"               \
           "-s <SYSTEM>   : Start specific system (baseline,dora,plp,plppart,plpleaf). Default: baseline\n" \
           "-d <PHYSICAL> : Use specific physical design (normal,mrbtnorm,...). Default: normal\n" \
           "-x            : Enable physical design hacks\n"   \
           "-g <RANGE>    : Use specific range (in some workloads)\n"   \
           "-f <FILE>     : Use specific configuration file (ie. instead of shore.conf)\n" \
           "-i <IN_FILE>  : Run the commands in the specific input file\n" \
           "-h            : Print this message and exit\n");
}



int main(int argc, char* argv[]) 
{
    dlopen("foo", 0); // for gdb's sake only...
    
#ifdef CFG_VTUNE
    // to pause sampling
    __itt_pause();
#endif 
    
    thread_init();

#ifdef CFG_SIMICS
    MAGIC_BREAKPOINT;
#endif

    TRACE_SET( TRACE_ALWAYS | TRACE_STATISTICS 
               //               | TRACE_NETWORK 
               //| TRACE_CPU_BINDING
               //              | TRACE_QUERY_RESULTS
               //              | TRACE_PACKET_FLOW
               //               | TRACE_RECORD_FLOW
               //               | TRACE_TRX_FLOW
               //| TRACE_DEBUG
               );

    _g_shore_env = NULL;
    _g_mon = NULL;

    // Get options
    bool netmode = false;
    bool inputfilemode = false;
    string inputfile;
    int netport = 0;
    string system,physical;
    int iRange = 0;    
    int c = 0;
    string config = DEFAULTCONFIG;
    string confFile = ENVCONFFILE;

    envVar* ev = envVar::instance();

    // Check if there is any particular configuration selected
    // We first need to make sure we use the correct config file

    while ((c = getopt(argc,argv,"f:rnp:c:s:d:xg:i:h")) != -1) {
        switch (c) {
        case 'f':
            TRACE( TRACE_ALWAYS, "CONFIGFILE (%s)\n", optarg);
            confFile = (string)optarg;            
            ev->setConfFile(confFile);
            TRACE( TRACE_ALWAYS, "CONFIG (%s)\n", config.c_str());
            ev->setConfiguration(config);
            break;
        case 'r':
            TRACE( TRACE_ALWAYS, "CLOBBERING DB\n");
            ev->setVarInt("db-clobberdev",1);
            break;
        case 'n':
            TRACE( TRACE_ALWAYS, "NETMODE\n");
            netmode = true;
            break;
        case 'p':
            netport = atoi(optarg);
            TRACE( TRACE_ALWAYS, "PORT (%d)\n", netport);
            break;
        case 'c':
            TRACE( TRACE_ALWAYS, "CONFIG (%s)\n", optarg);
            config = (string)optarg;
            ev->setConfiguration(config);
            break;
        case 's':
            TRACE( TRACE_ALWAYS, "SYSTEM (%s)\n", optarg);
            system = (string)optarg;
            ev->setSysName(system);
            break;
        case 'd':
            TRACE( TRACE_ALWAYS, "PHYSICAL DESIGN (%s)\n", optarg);
            physical = (string)optarg;
            if (!validatePhysical(physical)) {
                TRACE( TRACE_ALWAYS, "Unknown physical design (%s)\n",
                       physical.c_str());
                return(2);
            }
            ev->setSysDesign(physical);
            break;
        case 'x':
            TRACE( TRACE_ALWAYS, "Enabling hacks\n");
            ev->setVarInt("physical-hacks-enable",1);
            break;
        case 'g':
            iRange = atoi(optarg);
            TRACE( TRACE_ALWAYS, "RANGE (%d)\n", iRange);
            ev->setVarInt("records-to-access",iRange);
            break;
        case 'i':
            inputfilemode = true;
            TRACE( TRACE_ALWAYS, "INPUTFILE (%s)\n", optarg);
            inputfile = (string)optarg;            
            break;
        case 'h':
            usage();
            return (2);
            break;                 
        default:
            TRACE( TRACE_ALWAYS, "Wrong parameter. Accepted: rnp:c:xg:s:d:h\n");
            return (2);
        }
    }

    // make sure that no netmode and inputfilemode are selected at the same time
    assert( ((inputfilemode == true) && (netmode == true)) == false);

    // Get env vars
    initsysnamemap();
    initbenchmarkmap();

    string sysname = ev->getSysName();
    string benchmarkname = ev->getSysVar("benchmark");

    // Decide the physical design
    physical = ev->getSysDesign();    

    bool nameset=false;
    string dbname;

    // 3. Initialize shell
    guard<shore_shell_t> kit = NULL;

    TRACE( TRACE_ALWAYS, "Starting (%s-%s-%s) kit\n", 
           benchmarkname.c_str(),
           physical.c_str(),
           sysname.c_str());


    // TPC-C
    if (benchmarkname.compare("tpcc")==0) {
        dbname = "(tpcc-" + physical + "-";
        switch (mSysnameValue[sysname]) {
        case snBaseline:
            dbname += "base) ";
            kit = new baselineTPCCKit(dbname.c_str(),netmode,netport,inputfilemode,inputfile);
            break;
#ifdef CFG_DORA
        case snDORA:
            dbname += "dora) "; nameset=true;
        case snPLP:
            if (!nameset) dbname += "plp) ";
            kit = new doraTPCCKit(dbname.c_str(),netmode,netport,inputfilemode,inputfile);
            break;
#endif
        default:
            TRACE( TRACE_ALWAYS, "Not supported configuration. Exiting...\n");
            return (3);
        }
    }

    // TM1
    if (benchmarkname.compare("tm1")==0) {
        dbname = "(tatp-" + physical + "-";
        switch (mSysnameValue[sysname]) {
        case snBaseline:
            dbname += "base) ";
            kit = new baselineTM1Kit(dbname.c_str(),netmode,netport,inputfilemode,inputfile);
            break;
#ifdef CFG_DORA
        case snDORA:
            dbname += "dora) "; nameset=true;
        case snPLP:
            if (!nameset) dbname += "plp) ";
            kit = new doraTM1Kit(dbname.c_str(),netmode,netport,inputfilemode,inputfile);
            break;
#endif
        default:
            TRACE( TRACE_ALWAYS, "Not supported configuration. Exiting...\n");
            return (4);
        }
    }

    // TPC-B
    if (benchmarkname.compare("tpcb")==0) {
        dbname = "(tpcb-" + physical + "-";
        switch (mSysnameValue[sysname]) {
        case snBaseline:
            dbname += "base) ";
            kit = new baselineTPCBKit(dbname.c_str(),netmode,netport,inputfilemode,inputfile);
            break;
#ifdef CFG_DORA
        case snDORA:
            dbname += "dora) "; nameset=true;
        case snPLP:
            if (!nameset) dbname += "plp) ";
            kit = new doraTPCBKit(dbname.c_str(),netmode,netport,inputfilemode,inputfile);
            break;
#endif
        default:
            TRACE( TRACE_ALWAYS, "Not supported configuration. Exiting...\n");
            return (5);
        }
    }

    // TPC-H
    if (benchmarkname.compare("tpch")==0) {
        switch (mSysnameValue[sysname]) {
        case snBaseline:
            kit = new baselineTPCHKit("(tpch-base) ",netmode,netport,inputfilemode,inputfile);
            break;
#ifdef CFG_DORA
        case snDORA:
            dbname += "dora) "; nameset=true;
            assert (0); // TODO
            //kit = new doraTPCHKit("(tpch-dora) ",netmode,netport,inputfilemode,inputfile);
            break;
#endif
        default:
            TRACE( TRACE_ALWAYS, "Not supported configuration. Exiting...\n");
            return (5);
        }
    }

#ifdef CFG_QPIPE
    // SSB
    if (benchmarkname.compare("ssb")==0) {
        switch (mSysnameValue[sysname]) {
        case snBaseline:
            kit = new baselineSSBKit("(ssb-base) ",netmode,netport,inputfilemode,inputfile);
            break;
#ifdef CFG_DORA
        case snDORA:
            dbname += "dora) "; nameset=true;
            assert (0); // TODO
            //kit = new doraSSBKit("(ssb-dora) ",netmode,netport,inputfilemode,inputfile);
            break;
#endif
        default:
            TRACE( TRACE_ALWAYS, "Not supported configuration. Exiting...\n");
            return (5);
        }
    }
#endif


    // TPC-E
    if (benchmarkname.compare("tpce")==0) {
        switch (mSysnameValue[sysname]) {
        case snBaseline:
            kit = new baselineTPCEKit("(tpce-base) ",netmode,netport,inputfilemode,inputfile);
            break;
#ifdef CFG_DORA
        case snDORA:
            dbname += "dora) "; nameset=true;
            assert (0); // TODO
            //kit = new doraTPCEKit("(tpce-dora) ",netmode,netport,inputfilemode,inputfile);
            break;
#endif
        default:
            TRACE( TRACE_ALWAYS, "Not supported configurations. Exiting...\n");
            return (5);
        }
    }


    assert (kit.get());

    // Create and fork the process monitor
#ifdef __sparcv9
    _g_mon = new sunos_procmonitor_t(NULL);
#else
    _g_mon = new linux_procmonitor_t(NULL); 
#endif
    assert (_g_mon);

    // Instanciate and start the Shore environment
    TRACE ( TRACE_ALWAYS, "Starting Shore Environment\n" );
    if (kit->inst_test_env(argc, argv)) {
        return (6);
    }

    // Make sure data is loaded
    w_rc_t rcl = kit->db()->loaddata();
    if (rcl.is_error()) {
        return (7);
    }

    // Set the global variable to the kit's db - for alarm() to work
    _g_shore_env = kit->db();

    // Prepare for the next round
    TRACE( TRACE_ALWAYS, "Preparing for the next run\n");
    w_rc_t e = kit->prepareNewRun();
    if (e.is_error()) {
        TRACE( TRACE_ALWAYS, "!!! Problem preparing for the next run\n");
        return (9);
    }

    // Start the system monitoring thread
    _g_mon->setEnv(kit->db());
    _g_mon->fork();

    // Start processing commands
    TRACE ( TRACE_ALWAYS, "Starting processing commands\n" );

    int start = kit->start();
    if (start < 0) 
    {
        TRACE( TRACE_ALWAYS, "Error in starting shell\n");
        return (8);
    }

    // Dump the statistics before exiting
    kit->db()->statistics();

    // Stop, join, and delete the cpu monitor
    _g_mon->cntr_stop();
    _g_mon->join();
    delete (_g_mon);

    // The Shore environment will close at the destructor of the kit
    return (0);
}
