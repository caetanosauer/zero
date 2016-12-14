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

/** @file shore_env.cpp
 *
 *  @brief Implementation of a Shore environment (database)
 *
 *  @author Ippokratis Pandis (ipandis)
 */


// #include "util/confparser.h"
#include "shore_env.h"
#include "btree.h"
#include "xct.h"
#include "trx_worker.h"
#include "daemons.h"
#include "util/random_input.h"

// Get SM options spec from command class
#include "command.h"

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
// #include "sm/shore/shore_flusher.h"
// #include "sm/shore/shore_helper_loader.h"

using namespace boost;

// #warning IP: TODO pass arbitrary -sm_* options from shore.conf to shore

// Exported variables //

ShoreEnv* _g_shore_env = NULL;

// procmonitor_t* _g_mon = NULL;



// Exported functions //



/********************************************************************
 *
 *  @fn:    print_env_stats
 *
 *  @brief: Prints trx statistics
 *
 ********************************************************************/

void env_stats_t::print_env_stats() const
{
    TRACE( TRACE_STATISTICS, "===============================\n");
    TRACE( TRACE_STATISTICS, "Database transaction statistics\n");
    TRACE( TRACE_STATISTICS, "Attempted: %d\n", _ntrx_att);
    TRACE( TRACE_STATISTICS, "Committed: %d\n", _ntrx_com);
    TRACE( TRACE_STATISTICS, "Aborted  : %d\n", (_ntrx_att-_ntrx_com));
    TRACE( TRACE_STATISTICS, "===============================\n");
}




/********************************************************************
 *
 *  @class: ShoreEnv
 *
 *  @brief: The base class for all the environments (== databases)
 *          in Shore-MT
 *
 ********************************************************************/

ShoreEnv::ShoreEnv(po::variables_map& vm)
    : db_iface(),
      _pssm(NULL),
      _initialized(false), _init_mutex(thread_mutex_create()),
      _loaded(false), _load_mutex(thread_mutex_create()),
      _statmap_mutex(thread_mutex_create()),
      _last_stats_mutex(thread_mutex_create()),
      _vol_mutex(thread_mutex_create()),
      _max_cpu_count(0),
      _active_cpu_count(0),
      _worker_cnt(0),
      _measure(MST_UNDEF),
      _pd(PD_NORMAL),
      _insert_freq(0),_delete_freq(0),_probe_freq(100),
      _chkpt_freq(0),
      _enable_archiver(false), _enable_merger(false),
      _activation_delay(0),
      _crash_delay(0),
      _bAlarmSet(false),
      _start_imbalance(0),
      _skew_type(SKEW_NONE),
      _request_pool(sizeof(trx_request_t)),
      _bUseSLI(false),
      _bUseELR(false),
      _bUseFlusher(false)
      // _logger(NULL)
{
    optionValues = vm;

    pthread_mutex_init(&_scaling_mutex, NULL);
    pthread_mutex_init(&_queried_mutex, NULL);


    string physical = optionValues["db-config-design"].as<string>();
    if(physical.compare("normal")==0) {
        set_pd(PD_NORMAL);
    }
    if(optionValues["physical-hacks-enable"].as<int>()) {
        add_pd(PD_PADDED);
    }
    setSLIEnabled(optionValues["db-worker-sli"].as<bool>());
    set_rec_to_access(optionValues["records-to-access"].as<uint>());
}


ShoreEnv::~ShoreEnv()
{
    if (dbc()!=DBC_STOPPED) stop();

    pthread_mutex_destroy(&_init_mutex);
    pthread_mutex_destroy(&_statmap_mutex);
    pthread_mutex_destroy(&_last_stats_mutex);
    pthread_mutex_destroy(&_load_mutex);
    pthread_mutex_destroy(&_vol_mutex);

    pthread_mutex_destroy(&_scaling_mutex);
    pthread_mutex_destroy(&_queried_mutex);
}


bool ShoreEnv::is_initialized()
{
    CRITICAL_SECTION(cs, _init_mutex);
    return (_initialized);
}

bool ShoreEnv::is_loaded()
{
    CRITICAL_SECTION(cs, _load_mutex);
    return (_loaded);
}

w_rc_t ShoreEnv::load()
{

    // 1. lock the loading status and the scaling factor
    CRITICAL_SECTION(load_cs, _load_mutex);
    if (_loaded) {
        // TRACE( TRACE_TRX_FLOW,
        //       "Env already loaded. Doing nothing...\n");
        return (RCOK);
    }
    CRITICAL_SECTION(scale_cs, _scaling_mutex);
    time_t tstart = time(NULL);

    _loaders_to_use = optionValues["threads"].as<int>();

    // 2. Invoke benchmark-specific table creator
    W_DO(create_tables());

    // 3. Kick-off checkpoint thread
    // guard<checkpointer_t> chk(new checkpointer_t(this));
    // if (_chkpt_freq > 0) {
    //     chk->fork();
    // }

    // CS: loaders only start working once this is set (was in old checkpointer code)
    set_measure(MST_MEASURE);

    // 4. Invoke benchmark-specific table loaders
    W_DO(load_data());

    set_measure(MST_PAUSE);

    // 5. Print stats, join checkpointer, and return
    time_t tstop = time(NULL);
    TRACE( TRACE_ALWAYS, "Loading finished in (%d) secs...\n", (tstop - tstart));

    // if (_chkpt_freq > 0) {
    //     chk->set_active(false);
    //     chk->join();
    // }

    _loaded = true;
    return RCOK;
}

void ShoreEnv::set_max_cpu_count(const uint cpucnt)
{
    _max_cpu_count = ( cpucnt>0 ? cpucnt : _max_cpu_count);
    if (_active_cpu_count>_max_cpu_count) {
        _active_cpu_count = _max_cpu_count;
    }
}

uint ShoreEnv::get_max_cpu_count() const
{
    return (_max_cpu_count);
}

uint ShoreEnv::get_active_cpu_count() const
{
    return (_active_cpu_count);
}


/********************************************************************
 *
 *  @fn:    Related to Scaling and querying factor
 *
 *  @brief: Scaling factor (sf) - The size of the entire database
 *          Queried factor (qf) - The part of the database accessed at a run
 *
 ********************************************************************/

void ShoreEnv::set_qf(const double aQF)
{
    if ((aQF>0) && (aQF<=_scaling_factor)) {
        TRACE( TRACE_ALWAYS, "New Queried Factor: %.1f\n", aQF);
        _queried_factor = aQF;
    }
    else {
        TRACE( TRACE_ALWAYS, "Invalid queried factor input: %.1f\n", aQF);
    }
}

double ShoreEnv::get_qf() const
{
    return (_queried_factor);
}


void ShoreEnv::set_sf(const double aSF)
{
    if (aSF > 0.0) {
        TRACE( TRACE_ALWAYS, "New Scaling factor: %.1f\n", aSF);
        _scaling_factor = aSF;
    }
    else {
        TRACE( TRACE_ALWAYS, "Invalid scaling factor input: %.1f\n", aSF);
    }
}

double ShoreEnv::get_sf() const
{
    return (_scaling_factor);
}

void ShoreEnv::print_sf() const
{
    TRACE( TRACE_ALWAYS, "Scaling Factor = (%.1f)\n", get_sf());
    TRACE( TRACE_ALWAYS, "Queried Factor = (%.1f)\n", get_qf());
}

// void ShoreEnv::log_insert(kits_logger_t::logrec_kind_t kind)
// {
//     rc_t rc = _logger->insert(kind);
//     if (rc.is_error()) {
//         TRACE( TRACE_ALWAYS, "!! Error inserting kits log record: %s\n",
//                 w_error_t::error_string(rc.err_num()));
//     }
// }


/********************************************************************
 *
 *  @fn:    Related to physical design

 ********************************************************************/

uint32_t ShoreEnv::get_pd() const
{
    return (_pd);
}

uint32_t ShoreEnv::set_pd(const physical_design_t& apd)
{
    _pd = apd;
    TRACE( TRACE_ALWAYS, "DB set to (%x)\n", _pd);
    return (_pd);
}

uint32_t ShoreEnv::add_pd(const physical_design_t& apd)
{
    _pd |= apd;
    TRACE( TRACE_ALWAYS, "DB set to (%x)\n", _pd);
    return (_pd);
}

bool ShoreEnv::check_hacks_enabled()
{
    // enable hachs by default
    int he = optionValues["physical-hacks-enable"].as<int>();
    _enable_hacks = (he == 1 ? true : false);
    return (_enable_hacks);
}

bool ShoreEnv::is_hacks_enabled() const
{
    return (_enable_hacks);
}


/********************************************************************
 *
 *  @fn:    Related to microbenchmarks
 *  @brief: Set the insert/delete/probe frequencies
 *
 ********************************************************************/
void ShoreEnv::set_freqs(int insert_freq, int delete_freq, int probe_freq)
{
    assert ((insert_freq>=0) && (insert_freq<=100));
    assert ((delete_freq>=0) && (delete_freq<=100));
    assert ((probe_freq>=0) && (probe_freq<=100));
    _insert_freq = insert_freq;
    _delete_freq = delete_freq;
    _probe_freq = probe_freq;
}

void ShoreEnv::set_chkpt_freq(int chkpt_freq)
{
    _chkpt_freq = chkpt_freq;
}

void ShoreEnv::set_archiver_opts(bool enable_archiver, bool enable_merger)
{
    _enable_archiver = enable_archiver;
    _enable_merger = enable_merger;
}

void ShoreEnv::set_crash_delay(int crash_delay)
{
    _crash_delay = crash_delay;
}

/********************************************************************
 *
 *  @fn:    Related to load balancing work
 *  @brief: Set the load imbalance and the time to start it
 *
 ********************************************************************/
void ShoreEnv::set_skew(int area, int load, int start_imbalance, int skew_type)
{
    (void) area;
    (void) load;
    assert ((load>=0) && (load<=100));
    assert (start_imbalance>0);
    assert((area>0) && (area<100));

    _start_imbalance = start_imbalance;

    if (skew_type <= 0)
        skew_type = URand(1,10);
    if(skew_type < 6) {
    // 1. Keep the initial skew (no changes)
    _skew_type = SKEW_NORMAL;
    TRACE( TRACE_ALWAYS, "SKEW_NORMAL\n");
    } else if(skew_type < 9) {
    // 2. Change the area of the initial skew after some random duration
    _skew_type = SKEW_DYNAMIC;
    TRACE( TRACE_ALWAYS, "SKEW_DYNAMIC\n");
    } else if(skew_type < 11) {
    // 3. Change the initial skew randomly after some random duration
    // (a) The new skew can be like the old one but in another spot
    // (b) The skew can be omitted for sometime and
    // (c) The percentages might be changed
    _skew_type = SKEW_CHAOTIC;
    TRACE( TRACE_ALWAYS, "SKEW_CHAOTIC\n");
    } else {
    assert(0); // More cases can be added as wanted
    }
}


/********************************************************************
 *
 *  @fn:    Related to load balancing work
 *  @brief: reset the load imbalance and the time to start it if necessary
 *
 ********************************************************************/
void ShoreEnv::start_load_imbalance()
{
    // @note: pin: can change these boundaries depending on preference
    if(_skew_type == SKEW_DYNAMIC || _skew_type == SKEW_CHAOTIC) {
    _start_imbalance = URand(10,30);
    _bAlarmSet = false;
    }
}


/********************************************************************
 *
 *  @fn:    Related to load balancing work
 *  @brief: Set the flags to stop the load imbalance
 *
 ********************************************************************/
void ShoreEnv::reset_skew()
{
    _start_imbalance = 0;
    _bAlarmSet = false;
}


/********************************************************************
 *
 *  @fn:    Related to environment workers
 *
 *  @brief: Each environment has a set of worker threads
 *
 ********************************************************************/

uint ShoreEnv::upd_worker_cnt()
{
    // update worker thread cnt

    _worker_cnt = optionValues["threads"].as<int>();
    return (_worker_cnt);
}


trx_worker_t* ShoreEnv::worker(const uint idx)
{
    return (_workers[idx%_worker_cnt]);
}




/********
 ******** Caution: The functions below should be invoked inside
 ********          the context of a smthread
 ********/


/*********************************************************************
 *
 *  @fn     init
 *
 *  @brief  Initialize the shore environment. Reads configuration
 *          opens database and device.
 *
 *  @return 0 on success, non-zero otherwise
 *
 *********************************************************************/

int ShoreEnv::init()
{
    CRITICAL_SECTION(cs,_init_mutex);
    if (_initialized) {
        TRACE( TRACE_ALWAYS, "Already initialized\n");
        return (0);
    }

    // Set sys params
    if (_set_sys_params()) {
        TRACE( TRACE_ALWAYS, "Problem in setting system parameters\n");
        return (1);
    }


    // Apply configuration to the storage manager
    if (configure_sm()) {
        TRACE( TRACE_ALWAYS, "Error configuring Shore\n");
        return (2);
    }

    // Load the database schema
    if (load_schema().is_error()) {
        TRACE( TRACE_ALWAYS, "Error loading the database schema\n");
        return (3);
    }

    // Update partitioning information
    if (update_partitioning().is_error()) {
        TRACE( TRACE_ALWAYS, "Error updating the partitioning info\n");
        return (4);
    }


    return (0);
}


/*********************************************************************
 *
 *  @fn:     start
 *
 *  @brief:  Starts the Shore environment
 *           Updates the scaling/queried factors and starts the workers
 *
 *  @return: 0 on success, non-zero otherwise
 *
 *********************************************************************/

int ShoreEnv::start()
{
    // Start the storage manager
    if (start_sm()) {
        TRACE( TRACE_ALWAYS, "Error starting Shore database\n");
        return (5);
    }

    if (!_clobber) {
    // Cache fids at the kits side
        xct_t::begin();
        W_COERCE(load_and_register_fids());
        W_COERCE(xct_t::commit());
        // Call the (virtual) post-initialization function
        if (int rval = post_init()) {
            TRACE( TRACE_ALWAYS, "Error in Shore post-init\n");
            return (rval);
        }
    }

    // if we reached this point the environment is properly initialized
    _initialized = true;
    TRACE( TRACE_DEBUG, "ShoreEnv initialized\n");

    upd_worker_cnt();

    assert (_workers.empty());

    TRACE( TRACE_ALWAYS, "Starting (%s)\n", _sysname.c_str());
    info();

    // read from env params the loopcnt
    int lc = optionValues["db-worker-queueloops"].as<int>();

#ifdef CFG_FLUSHER
    _start_flusher();
#endif

    WorkerPtr aworker;
    for (uint i=0; i<_worker_cnt; i++) {

        aworker = new Worker(this,std::string("work-%d", i), -1,_bUseSLI);
        _workers.push_back(aworker);

        aworker->init(lc);
        aworker->start();
        aworker->fork();
    }
    return (0);
}



/*********************************************************************
 *
 *  @fn:     stop
 *
 *  @brief:  Stops the Shore environment
 *
 *  @return: 0 on success, non-zero otherwise
 *
 *********************************************************************/

int ShoreEnv::stop()
{
    // Check if initialized
    CRITICAL_SECTION(cs, _init_mutex);

    if (dbc() == DBC_STOPPED)
    {
        // Already stopped
        TRACE( TRACE_ALWAYS, "(%s) already stopped\n",
               _sysname.c_str());
        return (0);
    }

    TRACE( TRACE_ALWAYS, "Stopping (%s)\n", _sysname.c_str());
    info();

    if (!_initialized) {
        cerr << "Environment not initialized..." << endl;
        return (1);
    }

    // Stop workers
    int i=0;
    for (WorkerIt it = _workers.begin(); it != _workers.end(); ++it) {
        i++;
        TRACE( TRACE_DEBUG, "Stopping worker (%d)\n", i);
        if (*it) {
            (*it)->stop();
            (*it)->join();
            delete (*it);
        }
    }
    _workers.clear();

#ifdef CFG_FLUSHER
    _stop_flusher();
#endif

    // Set the stoped flag
    set_dbc(DBC_STOPPED);

    // If reached this point the Shore environment is stopped
    return (0);
}



/*********************************************************************
 *
 *  @fn:     close
 *
 *  @brief:  Closes the Shore environment
 *
 *  @return: 0 on success, non-zero otherwise
 *
 *********************************************************************/

int ShoreEnv::close()
{
    TRACE( TRACE_ALWAYS, "Closing (%s)\n", _sysname.c_str());

    // First stop the environment
    int r = stop();
    if (r != 0)
    {
        // If it returned != 0 then error occured
        return (r);
    }

    // Then, close Shore-MT
    CRITICAL_SECTION(cs, _init_mutex);
    close_sm();
    _initialized = false;

    // If reached this point the Shore environment is closed
    return (0);
}


/********************************************************************
 *
 *  @fn:    statistics
 *
 *  @brief: Prints statistics for the SM
 *
 ********************************************************************/

int ShoreEnv::statistics()
{
    CRITICAL_SECTION(cs, _init_mutex);
    if (!_initialized) {
        cerr << "Environment not initialized..." << endl;
        return (1);
    }

#ifdef CFG_FLUSHER
    if (_base_flusher) _base_flusher->statistics();
#endif

    // If reached this point the Shore environment is closed
    //gatherstats_sm();
    return (0);
}



/** Storage manager functions */


/********************************************************************
 *
 *  @fn:     close_sm
 *
 *  @brief:  Closes the storage manager
 *
 *  @return: 0 on sucess, non-zero otherwise
 *
 ********************************************************************/

int ShoreEnv::close_sm()
{
    TRACE( TRACE_ALWAYS, "Closing Shore storage manager...\n");

    if (!_pssm) {
        TRACE( TRACE_ALWAYS, "sm already closed...\n");
        return (1);
    }

    // Final stats
    gatherstats_sm();

    /*
     *  destroying the ss_m instance causes the SSM to shutdown
     */
    delete (_pssm);

    // If we reached this point the sm is closed
    return (0);
}


/********************************************************************
 *
 *  @fn:     gatherstats_sm
 *
 *  @brief:  Collects and prints statistics from the sm
 *
 ********************************************************************/

static sm_stats_info_t oldstats;

void ShoreEnv::gatherstats_sm()
{
    // sm_du_stats_t stats;
    // memset(&stats, 0, sizeof(stats));

    sm_stats_info_t stats;
    ss_m::gather_stats(stats);

    sm_stats_info_t diff = stats;
    diff -= _last_sm_stats;

    // Print the diff and save the latest reading
    cout << diff << endl;
    _last_sm_stats = stats;
}



/********************************************************************
 *
 *  @fn:     configure_sm
 *
 *  @brief:  Configure Shore environment
 *
 *  @return: 0 on sucess, non-zero otherwise
 *
 ********************************************************************/

int ShoreEnv::configure_sm()
{
    TRACE( TRACE_DEBUG, "Configuring Shore...\n");

    upd_worker_cnt();
    Command::setSMOptions(_popts, optionValues);

    // If we reached this point the sm is configured correctly
    return (0);
}



int ShoreEnv::start_sm()
{
    TRACE( TRACE_DEBUG, "Starting Shore...\n");

    if (_initialized == false) {
        _pssm = new ss_m(_popts);
        // _logger = new kits_logger_t(_pssm);
    }
    else {
        TRACE( TRACE_DEBUG, "Shore already started...\n");
        return (1);
    }

    // format and mount the database...

    assert (_pssm);

    if (_clobber) {
        // if didn't clobber then the db is already loaded
        CRITICAL_SECTION(cs, _load_mutex);

        // create catalog index (must be on stid 1)
        StoreID cat_stid;
        xct_t::begin();
        W_COERCE(btree_m::create(cat_stid));
        w_assert0(cat_stid == 1);
        W_COERCE(xct_t::commit());

        // set that the database is not loaded
        _loaded = false;
    }
    else {
        // if didn't clobber then the db is already loaded
        CRITICAL_SECTION(cs, _load_mutex);

        // CS: No need to mount since mounted devices are restored during
        // log analysis, i.e., list of mounted devices is kept in the persistent
        // system state. Mount here is only necessary if we explicitly dismount
        // after loading, which is not the case.

        // Make sure that catalog index (stid 1) exists
        vol_t* vol = ss_m::vol;
        w_assert0(vol);
        w_assert0(vol->is_alloc_store(1));

        // "speculate" that the database is loaded
        _loaded = true;
    }


    // If we reached this point the sm has started correctly
    return (0);
}

/******************************************************************
 *
 *  @fn:    get_trx_{att,com}()
 *
 ******************************************************************/

unsigned ShoreEnv::get_trx_att() const
{
    return (*&_env_stats._ntrx_att);
}

unsigned ShoreEnv::get_trx_com() const
{
    return (*&_env_stats._ntrx_com);
}



/******************************************************************
 *
 *  @fn:    set_{max/active}_cpu_count()
 *
 *  @brief: Setting new cpu counts
 *
 *  @note:  Setting max cpu count is disabled. This value can be set
 *          only at the config file.
 *
 ******************************************************************/

void ShoreEnv::set_active_cpu_count(const unsigned actcpucnt)
{
    _active_cpu_count = ( actcpucnt>0 ? actcpucnt : _active_cpu_count );
}



/** Helper functions */


/******************************************************************
 *
 *  @fn:     _set_sys_params()
 *
 *  @brief:  Sets system params
 *
 *  @return: Returns 0 on success
 *
 ******************************************************************/

int ShoreEnv::_set_sys_params()
{
    // procmonitor returns 0 if it cannot find the number of processors

    _max_cpu_count = optionValues["sys-maxcpucount"].as<uint>();

    // Set active CPU info
    uint tmp_active_cpu_count = optionValues["sys-activecpucount"].as<uint>();
    if (tmp_active_cpu_count>_max_cpu_count) {
        _active_cpu_count = _max_cpu_count;
    }
    else {
        _active_cpu_count = tmp_active_cpu_count;
    }
    print_cpus();

    _activation_delay = optionValues["activation_delay"].as<uint>();
    return (0);
}


void ShoreEnv::print_cpus() const
{
    TRACE( TRACE_ALWAYS, "MaxCPU=(%d) - ActiveCPU=(%d)\n",
           _max_cpu_count, _active_cpu_count);
}





/******************************************************************
 *
 * @fn:    restart()
 *
 * @brief: Re-starts the environment
 *
 * @note:  - Stops everything
 *         - Rereads configuration
 *         - Starts everything
 *
 ******************************************************************/

int ShoreEnv::restart()
{
    TRACE( TRACE_DEBUG, "Restarting (%s)...\n", _sysname.c_str());
    stop();
    conf();
    _set_sys_params();
    start();
    return(0);
}


/******************************************************************
 *
 *  @fn:    conf
 *
 *  @brief: Prints configuration
 *
 ******************************************************************/

int ShoreEnv::conf()
{
    TRACE( TRACE_DEBUG, "ShoreEnv configuration\n");
    // Print storage manager options
    BOOST_FOREACH(const po::variables_map::value_type& pair, optionValues)
    {
        const std::string& key = pair.first;
        try {
            cout<< "[" << key << "] = "<< optionValues[key].as<int>()<<endl;
        }
        catch(boost::bad_any_cast const& e) {
            try {
                cout<< "[" << key << "] = "<< optionValues[key].as<bool>()<<endl;
            }
            catch (boost::bad_any_cast const& e) {
                try {
                    cout<< "[" << key << "] = "<< optionValues[key].as<string>()<<endl;
                }
                catch (boost::bad_any_cast const& e) {
                    try {
                        cout<< "[" << key << "] = "<< optionValues[key].as<unsigned>()<<endl;
                    }
                    catch (boost::bad_any_cast const& e) {
                        try {
                            cout<< "[" << key << "] = "<< optionValues[key].as<uint>()<<endl;
                        }
                        catch (boost::bad_any_cast const& e) {
                            continue;
                        }
                    }
                }
            }
        }
    };
    return (0);
}

int ShoreEnv::dump()
{
    TRACE( TRACE_DEBUG, "~~~~~~~~~~~~~~~~~~~~~\n");
    TRACE( TRACE_DEBUG, "Dumping Shore Data\n");

    TRACE( TRACE_ALWAYS, "Not implemented...\n");

    TRACE( TRACE_DEBUG, "~~~~~~~~~~~~~~~~~~~~~\n");
    return (0);
}



/******************************************************************
 *
 *  @fn:    setAsynchCommit()
 *
 *  @brief: Sets whether asynchronous commit will be used or not
 *
 ******************************************************************/

void ShoreEnv::setAsynchCommit(const bool bAsynch)
{
    _asynch_commit = bAsynch;
}


#if 0

/******************************************************************
 *
 *  @fn:    start_flusher()
 *
 *  @brief: Starts the baseline flusher
 *
 ******************************************************************/

int ShoreEnv::_start_flusher()
{
    _base_flusher = new flusher_t(this,std::string("base-flusher"));
    assert (_base_flusher);
    _base_flusher->fork();
    _base_flusher->start();
    return (0);
}


/******************************************************************
 *
 *  @fn:    stop_flusher()
 *
 *  @brief: Stops the baseline flusher
 *
 ******************************************************************/

int ShoreEnv::_stop_flusher()
{
    _base_flusher->stop();
    _base_flusher->join();
    return (0);
}


/******************************************************************
 *
 *  @fn:    to_base_flusher()
 *
 *  @brief: Enqueues a request to the base flusher
 *
 ******************************************************************/

void ShoreEnv::to_base_flusher(Request* ar)
{
// TODO: IP: Add multiple flusher to the baseline as well
    _base_flusher->enqueue_toflush(ar);
}


/******************************************************************
 *
 *  @fn:    db_print_init
 *
 *  @brief: Starts the db printer thread and then deletes it
 *
 ******************************************************************/

void ShoreEnv::db_print_init(int num_lines)
{
    table_printer_t* db_printer = new table_printer_t(this, num_lines);
    db_printer->fork();
    db_printer->join();
    delete (db_printer);
}


/******************************************************************
 *
 *  @fn:    db_fetch_init
 *
 *  @brief: Starts the db fetcher thread and then deletes it
 *
 ******************************************************************/

void ShoreEnv::db_fetch_init()
{
    table_fetcher_t* db_fetcher = new table_fetcher_t(this);
    db_fetcher->fork();
    db_fetcher->join();
    delete (db_fetcher);
}
#endif
