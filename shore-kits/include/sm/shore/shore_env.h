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

/** @file:   shore_env.h
 *
 *  @brief:  Definition of a Shore environment (database)
 *
 *  @author: Ippokratis Pandis (ipandis)
 */

#ifndef __SHORE_ENV_H
#define __SHORE_ENV_H

#include "k_defines.h"
#include "sm_vas.h"

#include <map>

#include "util.h"

#include "shore_reqs.h"
#include "shore_file_desc.h"


ENTER_NAMESPACE(shore);


using std::map;


/******** Constants ********/

const int SHORE_NUM_OF_RETRIES       = 3;

#define SHORE_TABLE_DATA_DIR  "databases"
#define SHORE_CONF_FILE       "shore.conf"

const string CONFIG_PARAM       = "db-config";
const string CONFIG_PARAM_VALUE = "invalid";


// !!!
// shore.conf should have values with the corresponding configuration suffix
// for all the database-instance-dependent parameters
// !!!


// SHORE_SYS_OPTIONS:
// Database-independent options 
const string SHORE_SYS_OPTIONS[][2] = {
    { "db-clobberdev", "0" },
    { "sys-maxcpucount", "0" },
    { "sys-activecpucount", "0" },
    { "shore-fakeiodelay", "0" },
    { "shore-fakeiodelay-enable", "0" },
};

const int    SHORE_NUM_SYS_OPTIONS  = 5;


// SHORE_SYS_SM_OPTIONS: 
// Those options are appended as parameters when starting Shore
// Those are the database-independent
const string SHORE_SYS_SM_OPTIONS[][3]  = {
    { "-sm_logging", "shore-logging", "yes" },
    { "-sm_logisraw", "shore-logisraw", "no" },
    { "-sm_diskrw", "shore-diskrw", "diskrw" },
    { "-sm_errlog", "shore-errlog", "info" },
    { "-sm_num_page_writers", "shore-pagecleaners", "16" },
};

const int    SHORE_NUM_SYS_SM_OPTIONS   = 5;


// SHORE_DB_SM_OPTIONS: 
// Those options are appended as parameters when starting Shore
// Thore are the database-instance-specific
const string SHORE_DB_SM_OPTIONS[][3]  = {
    { "-sm_bufpoolsize", "bufpoolsize", "0" },
    { "-sm_logdir", "logdir", "log" },
    { "-sm_logsize", "logsize", "0" },
    { "-sm_logbufsize", "logbufsize", "0" },
    { "-sm_logcount", "logcount", "10" },      /*** IP: Remove -sm_logcount no dlog anymore... ****/
};

const int    SHORE_NUM_DB_SM_OPTIONS   = 5;


// SHORE_DB_OPTIONS
// Database-instance-specific options
const string SHORE_DB_OPTIONS[][2] = {
    { "device", "databases/shore" },
    { "devicequota", "0" },
    { "loadatadir", SHORE_TABLE_DATA_DIR },
    { "sf", "0" },
    { "system", "invalid" },
};

const int    SHORE_NUM_DB_OPTIONS  = 5;




/****************************************************************** 
 *
 * MACROS used in _env, _schema, _xct files
 *
 ******************************************************************/

#define DECLARE_TRX(trxlid) \
    w_rc_t run_##trxlid(Request* prequest, trxlid##_input_t& in);       \
    w_rc_t run_##trxlid(Request* prequest);                             \
    w_rc_t xct_##trxlid(const int xct_id, trxlid##_input_t& in);        \
    void   _inc_##trxlid##_att();                                       \
    void   _inc_##trxlid##_failed();                                    \
    void   _inc_##trxlid##_dld()


#define DECLARE_TABLE(table,manimpl,abbrv)                              \
    guard<manimpl>   _p##abbrv##_man;                                   \
    inline manimpl*  abbrv##_man() { return (_p##abbrv##_man); }        \
    guard<table>     _p##abbrv##_desc;                                  \
    inline table*    abbrv##_desc() { return (_p##abbrv##_desc.get()); }



// There may be multiple implementations of the same transaction on the
// same enviroment. For example, we have the conventional TPC-H queries 
// and the QPipe ones. On the other hand, to run a transaction we need
// to know the name of the implementation the user selected to run, and
// the name of the transaction whose input will be generated and statistics 
// will be updated.  
//
// In order to achieve this discrimination, the "trxlid" identifies
// logically that transaction (for example TPC-H Q1), and the "trximpl" 
// identifies the implementation which is going to be used. 

#ifdef CFG_FLUSHER // ***** Mainstream FLUSHER ***** //
// Commits lazily

#define DEFINE_RUN_WITH_INPUT_TRX_WRAPPER(cname,trxlid,trximpl)         \
    w_rc_t cname::run_##trximpl(Request* prequest, trxlid##_input_t& in) { \
        int xct_id = prequest->xct_id();                                \
        TRACE( TRACE_TRX_FLOW, "%d. %s ...\n", xct_id, #trximpl);       \
        _inc_##trxlid##_att();                                          \
        w_rc_t e = xct_##trximpl(xct_id, in);                           \
        if (!e.is_error()) {                                            \
            lsn_t xctLastLsn;                                           \
            e = _pssm->commit_xct(true,&xctLastLsn);                    \
            prequest->set_last_lsn(xctLastLsn); }                       \
        if (e.is_error()) {                                             \
            if (e.err_num() != smlevel_0::eDEADLOCK)                    \
                _inc_##trxlid##_failed();                               \
            else _inc_##trxlid##_dld();                                 \
            TRACE( TRACE_TRX_FLOW, "Xct (%d) aborted [0x%x]\n", xct_id, e.err_num()); \
            w_rc_t e2 = _pssm->abort_xct();                             \
            if(e2.is_error()) TRACE( TRACE_ALWAYS, "Xct (%d) abort failed [0x%x]\n", xct_id, e2.err_num()); \
            prequest->notify_client();                                  \
            _request_pool.destroy(prequest);				\
            if ((*&_measure)!=MST_MEASURE) return (e);                  \
            _env_stats.inc_trx_att();                                   \
            return (e); }                                               \
        TRACE( TRACE_TRX_FLOW, "Xct (%d) (%d) to flush\n", xct_id, prequest->tid().get_lo()); \
        to_base_flusher(prequest);                                      \
        return (RCOK); }

#else // ***** NO FLUSHER ***** //

#define DEFINE_RUN_WITH_INPUT_TRX_WRAPPER(cname,trxlid,trximpl)         \
    w_rc_t cname::run_##trximpl(Request* prequest, trxlid##_input_t& in) { \
        int xct_id = prequest->xct_id();                                \
        TRACE( TRACE_TRX_FLOW, "%d. %s ...\n", xct_id, #trximpl);       \
        _inc_##trxlid##_att();                                          \
        w_rc_t e = xct_##trximpl(xct_id, in);                           \
        if (!e.is_error()) {                                            \
            if (isAsynchCommit()) e = _pssm->commit_xct(true);          \
            else e = _pssm->commit_xct(); }                             \
        if (e.is_error()) {                                             \
            if (e.err_num() != smlevel_0::eDEADLOCK)                    \
                _inc_##trxlid##_failed();                               \
            else _inc_##trxlid##_dld();                                 \
            TRACE( TRACE_TRX_FLOW, "Xct (%d) aborted [0x%x]\n", xct_id, e.err_num()); \
            w_rc_t e2 = _pssm->abort_xct();                             \
            if(e2.is_error()) TRACE( TRACE_ALWAYS, "Xct (%d) abort failed [0x%x]\n", xct_id, e2.err_num()); \
            prequest->notify_client();                                  \
            if ((*&_measure)!=MST_MEASURE) return (e);                  \
            _env_stats.inc_trx_att();                                   \
            return (e); }                                               \
        TRACE( TRACE_TRX_FLOW, "Xct (%d) completed\n", xct_id);         \
        prequest->notify_client();                                      \
        if ((*&_measure)!=MST_MEASURE) return (RCOK);                   \
        _env_stats.inc_trx_com();                                       \
        return (RCOK); }

#endif // ***** EOF: CFG_FLUSHER ***** //


#define DEFINE_RUN_WITHOUT_INPUT_TRX_WRAPPER(cname,trxlid,trximpl)      \
    w_rc_t cname::run_##trximpl(Request* prequest) {                    \
        trxlid##_input_t in = create_##trxlid##_input(_queried_factor, prequest->selectedID()); \
        return (run_##trximpl(prequest, in)); }


#define DEFINE_TRX_STATS(cname,trxlid)                                  \
    void cname::_inc_##trxlid##_att()    { ++my_stats.attempted.trxlid; } \
    void cname::_inc_##trxlid##_failed() { ++my_stats.failed.trxlid; }  \
    void cname::_inc_##trxlid##_dld() { ++my_stats.deadlocked.trxlid; }


// In the common case, there is a single implementation per transaction
#define DEFINE_TRX(cname,trx)                                   \
    DEFINE_RUN_WITHOUT_INPUT_TRX_WRAPPER(cname,trx,trx);        \
    DEFINE_RUN_WITH_INPUT_TRX_WRAPPER(cname,trx,trx);           \
    DEFINE_TRX_STATS(cname,trx)


#ifdef USE_SHORE_6

#define CHECK_XCT_RETURN(rc,needed_next_time,retry,ENV)			\
    if (rc.is_error()) {						\
	TRACE( TRACE_ALWAYS, "Error %x\n", rc.err_num());		\
	long used = ENV->db()->xct_log_space_needed();			\
	W_COERCE(ENV->db()->abort_xct());				\
	switch(rc.err_num()) {						\
	case smlevel_0::eDEADLOCK:					\
	    goto retry;							\
	case smlevel_0::eOUTOFLOGSPACE:					\
	    needed_next_time = used;					\
	    goto retry;							\
	default:							\
	    stringstream os; os << rc << ends;				\
	    string str = os.str();					\
	    TRACE( TRACE_ALWAYS,					\
		   "Eek! Unable to populate db due to: \n%s\n",		\
		   str.c_str());					\
	    W_FATAL(rc.err_num());					\
	}								\
    }

#else

#define CHECK_XCT_RETURN(rc,needed_next_time,retry,ENV)			\
    if (rc.is_error()) {						\
	TRACE( TRACE_ALWAYS, "Error %x\n", rc.err_num());		\
	long used = 0;                                                  \
	W_COERCE(ENV->db()->abort_xct());				\
	switch(rc.err_num()) {						\
	case smlevel_0::eDEADLOCK:					\
	    goto retry;							\
	case smlevel_0::eOUTOFLOGSPACE:					\
	    needed_next_time = used;					\
	    goto retry;							\
	default:							\
	    stringstream os; os << rc << ends;				\
	    string str = os.str();					\
	    TRACE( TRACE_ALWAYS,					\
		   "Eek! Unable to populate db due to: \n%s\n",		\
		   str.c_str());					\
	    W_FATAL(rc.err_num());					\
	}								\
    }

#endif



/****************************************************************** 
 *
 *  @struct: env_stats_t
 *
 *  @brief:  Environment statistics - total trxs attempted/committed
 *
 ******************************************************************/

struct env_stats_t 
{
    volatile uint_t _ntrx_att;
    volatile uint_t _ntrx_com;

    env_stats_t() 
        : _ntrx_att(0), _ntrx_com(0)
    { }

    ~env_stats_t() { }

    void print_env_stats() const;

    inline uint_t inc_trx_att() { return (atomic_inc_uint_nv(&_ntrx_att)); }
    inline uint_t inc_trx_com() {
        atomic_inc_uint(&_ntrx_att);
        return (atomic_inc_uint_nv(&_ntrx_com)); 
    }

}; // EOF env_stats_t



/******************************************************************** 
 *
 * @enum:  eDBControl
 *
 * @brief: States of a controlled DB
 *
 ********************************************************************/

enum eDBControl { DBC_UNDEF =   0x1, 
                  DBC_PAUSED =  0x2, 
                  DBC_ACTIVE =  0x4, 
                  DBC_STOPPED = 0x8 
};


/*********************************************************************
 *
 *  @abstract class: db_iface
 *
 *  @brief:          Interface of basic shell commands for dbs
 *
 *  @usage:          - Inherit from this class
 *                   - Implement the process_{START/STOP/PAUSE/RESUME} fuctions
 *
 *********************************************************************/

class db_iface
{
public:
    typedef map<string,string>        envVarMap;
    typedef envVarMap::iterator       envVarIt;
    typedef envVarMap::const_iterator envVarConstIt;

private:
    volatile unsigned int _dbc;

public:

    db_iface() 
        : _dbc(DBC_UNDEF)
    { }
    virtual ~db_iface() { }

    // Access methods

    eDBControl dbc() { return (eDBControl(*&_dbc)); }
    
    void set_dbc(const eDBControl adbc) {
        assert (adbc!=DBC_UNDEF);
        unsigned int tmp = adbc;
        atomic_swap_uint(&_dbc, tmp);
    }
    

    // DB INTERFACE     

    virtual int conf()=0;    
    virtual int set(envVarMap* vars)=0;    
    virtual w_rc_t load_schema()=0;
    virtual int init()=0;
    virtual int open()=0;
    virtual int close()=0;
    virtual int start()=0;
    virtual int stop()=0;
    virtual int restart()=0;
    virtual int pause()=0;
    virtual int resume()=0;    
    virtual w_rc_t newrun()=0;    
    virtual int statistics()=0;    
    virtual int dump()=0;    
    virtual int info() const=0;
    
}; // EOF: db_iface



// Forward decl
class base_worker_t;
class trx_worker_t;
class flusher_t;
class ShoreEnv;


/******** Exported variables ********/

extern ShoreEnv* _g_shore_env;

extern procmonitor_t* _g_mon;

extern int ssm_max_small_rec;


/******************************************************************** 
 *
 * @enum  MeasurementState
 *
 * @brief Possible states of a measurement in the Shore Enviroment
 *
 ********************************************************************/

enum MeasurementState { MST_UNDEF   = 0x0, 
                        MST_WARMUP  = 0x1, 
                        MST_MEASURE = 0x2, 
                        MST_DONE    = 0x4, 
                        MST_PAUSE   = 0x8 
};


/******************************************************************** 
 * 
 *  ShoreEnv
 *  
 *  Shore database abstraction. Among others it configures, starts 
 *  and closes the Shore database 
 *
 ********************************************************************/

class ShoreEnv : public db_iface
{
public:
    typedef std::map<string,string> ParamMap;

    typedef trx_request_t Request;
#ifdef CFG_SHORE_6
    typedef blob_pool RequestStack;
#else
    typedef atomic_class_stack<trx_request_t> RequestStack;
#endif

    typedef trx_worker_t                Worker;
    typedef trx_worker_t*               WorkerPtr;
    typedef std::vector<WorkerPtr>           WorkerPool;
    typedef std::vector<WorkerPtr>::iterator WorkerIt;

protected:       

    ss_m*           _pssm;               // database handle

    // Status variables
    bool            _initialized; 
    pthread_mutex_t _init_mutex;
    bool            _loaded;
    pthread_mutex_t _load_mutex;

    pthread_mutex_t _statmap_mutex;
    pthread_mutex_t _last_stats_mutex;

    // Device and volume. There is a single volume per device. 
    // The whole environment resides in a single volume.
    devid_t            _devid;     // device id
    guard<vid_t>       _pvid;      // volume id
    stid_t             _root_iid;  // root id of the volume
    pthread_mutex_t    _vol_mutex; // volume mutex
    lvid_t             _lvid;      // logical volume id (unnecessary, using physical ids)
    unsigned int       _vol_cnt;   // volume count (unnecessary, always 1)

    // Configuration variables
    guard<option_group_t>   _popts;     // config options

    ParamMap      _sys_opts;  // db-instance-independent options  
    ParamMap      _sm_opts;   // db-instance-specific options that are passed to Shore
    ParamMap      _dev_opts;  // db-instance-specific options    

    // Processor info
    uint _max_cpu_count;    // hard limit
    uint _active_cpu_count; // soft limit


    // List of worker threads
    WorkerPool      _workers;    
    uint            _worker_cnt;         

    // Scaling factors
    //
    // @note: The scaling factors of any environment is an integer value 
    //        So we are putting them on shore_env
    // 
    // In various environments: 
    //
    // TM1:  SF=1 --> 15B   (10K Subscribers)
    // TPCB: SF=1 --> 20MB  (1 Branch)
    // TPCC: SF=1 --> 130MB (1 Warehouse)
    // TPCE: SF=1 --> 6.5GB   (1K Customers)
    // TPCH: SF=1 --> 250MB (1 Warehouse)
    //       
    double             _scaling_factor; 
    pthread_mutex_t _scaling_mutex;
    double             _queried_factor;
    pthread_mutex_t _queried_mutex;


    // Stats
    env_stats_t        _env_stats; 
    sm_stats_info_t    _last_sm_stats;

    // Measurement state
    volatile uint _measure;

    // system name
    string          _sysname;

    // physical design characteristics
    uint4_t _pd;
    bool _enable_hacks;
    
    
    // Helper functions
    void usage(option_group_t& options);
    void readconfig();

    // Used for some benchmarks - number of records to access
    volatile uint _rec_to_acc;

    // The insert/delete/probe frequencies for microbenchmarks
    int _insert_freq;
    int _delete_freq;
    int _probe_freq;

    // Storage manager access functions
    int  configure_sm();
    int  start_sm();
    int  close_sm();

    // load balancing settings
    volatile bool _bAlarmSet;
    tatas_lock _alarm_lock;
    int _start_imbalance;
    skew_type_t _skew_type;
    
public:

    ShoreEnv();
    virtual ~ShoreEnv();


    // DB INTERFACE

    virtual int conf();
    virtual int set(envVarMap* /* vars */) { return(0); /* do nothing */ };
    virtual int init();
    virtual int post_init() { return (0); /* do nothing */ }; // Should return >0 on error
    virtual int open() { return(0); /* do nothing */ };
    virtual int close();
    virtual int start();
    virtual int stop();
    virtual int restart();
    virtual int pause() { return(0); /* do nothing */ };
    virtual int resume() { return(0); /* do nothing */ };    
    virtual w_rc_t newrun()=0;
    virtual int statistics();
    virtual int dump();
    virtual int info() const=0;


    // Loads the database schema after the config file is read, and before the storage
    // manager is started.
    // Should return >0 on error
    virtual w_rc_t load_schema()=0; 

    virtual w_rc_t warmup()=0;
    virtual w_rc_t loaddata()=0;
    virtual w_rc_t check_consistency()=0;
       
    // inline access methods
    inline ss_m* db() { return(_pssm); }
    inline vid_t* vid() { return(_pvid); }

    bool is_initialized();
    bool is_loaded();
    
    void set_measure(const MeasurementState aMeasurementState) {
        //assert (aMeasurementState != MST_UNDEF);
        unsigned int tmp = aMeasurementState;
        atomic_swap_uint(&_measure, tmp);
    }
    inline MeasurementState get_measure() { return (MeasurementState(*&_measure)); }


    pthread_mutex_t* get_init_mutex() { return (&_init_mutex); }
    pthread_mutex_t* get_vol_mutex() { return (&_vol_mutex); }
    pthread_mutex_t* get_load_mutex() { return (&_load_mutex); }
    bool get_init_no_cs() { return (_initialized); }
    bool get_loaded_no_cs() { return (_loaded); }
    void set_init_no_cs(const bool b_is_init) { _initialized = b_is_init; }
    void set_loaded_no_cs(const bool b_is_loaded) { _loaded = b_is_loaded; }

    // CPU count functions
    void print_cpus() const;
    uint get_max_cpu_count() const;
    void set_max_cpu_count(const uint cpucnt);
    uint get_active_cpu_count() const;
    void set_active_cpu_count(const uint actcpucnt);
    // disabled - max_count can be set only on conf
    //    void set_max_cpu_count(const int maxcpucnt); 

    // --- scaling and querying factor --- //
    void set_qf(const double aQF);
    double get_qf() const;
    void set_sf(const double aSF);
    double get_sf() const;
    double upd_sf();
    void print_sf() const;

    // Set physical design characteristics
    uint4_t get_pd() const;
    uint4_t set_pd(const physical_design_t& apd);
    uint4_t add_pd(const physical_design_t& apd);
    bool check_hacks_enabled();
    bool is_hacks_enabled() const;
    virtual w_rc_t update_partitioning() { return (RCOK); }

    // -- insert/delete/probe frequencies for microbenchmarks -- //
    void set_freqs(int insert_freq = 0, int delete_freq = 0, int probe_freq = 0);

    // load imbalance related
    virtual void set_skew(int area, int load, int start_imbalance);
    virtual void reset_skew();
    virtual void start_load_imbalance();

    // print the current db to files
    virtual void db_print_init(int num_lines);
    virtual w_rc_t db_print(int num_lines) { return(RCOK); }

    // fetch the current db to buffer pool
    virtual void db_fetch_init();
    virtual w_rc_t db_fetch() { return(RCOK); }
    
    // Environment workers
    uint upd_worker_cnt();
    trx_worker_t* worker(const uint idx);        

    // Request atomic trash stack
    RequestStack _request_pool;

    // For thread-local stats
    virtual void env_thread_init()=0;
    virtual void env_thread_fini()=0;   

    // Fake io delay interface
    int disable_fake_disk_latency();
    int enable_fake_disk_latency(const int adelay);

    // Collects and print statistics from the SM
    void gatherstats_sm();

    // Takes a checkpoint (forces dirty pages)
    int checkpoint();

    string sysname() { return (_sysname); }
    env_stats_t* get_env_stats() { return (&_env_stats); }

    // For temp throughput calculation
    uint_t get_trx_att() const;
    uint_t get_trx_com() const;

    inline void inc_trx_att() { _env_stats.inc_trx_att(); }
    inline void inc_trx_com() { _env_stats.inc_trx_com(); }

    // Throughput printing
    virtual void print_throughput(const double iQueriedSF, 
                                  const int iSpread, 
                                  const int iNumOfThreads,
                                  const double delay,
                                  const ulong_t mioch,
                                  const double avgcpuusage)=0;

    virtual void reset_stats()=0;

    inline uint get_rec_to_access() { return *&_rec_to_acc; }


    // Run one transaction
    virtual w_rc_t run_one_xct(Request* prequest)=0;



    // Control whether asynchronous commit will be used
    inline bool isAsynchCommit() const { return (_asynch_commit); }
    void setAsynchCommit(const bool bAsynch);


    // SLI
public:
    bool isSLIEnabled() const { return (_bUseSLI); }
    void setSLIEnabled(const bool bUseSLI) { _bUseSLI = bUseSLI; }
protected:
    bool _bUseSLI;    


    // ELR
public:
    bool isELREnabled() const { return (_bUseELR); }
    void setELREnabled(const bool bUseELR) { _bUseELR = bUseELR; }
protected:
    bool _bUseELR;


    // FLUSHER
public: 
    bool isFlusherEnabled() const { return (_bUseFlusher); }
    void setFlusherEnabled(const bool bUseFlusher) { _bUseFlusher = bUseFlusher; }

protected:
    bool               _bUseFlusher;
    guard<flusher_t>   _base_flusher;
    virtual int        _start_flusher();
    virtual int        _stop_flusher();
    void               to_base_flusher(Request* ar);


protected:
   
    // returns 0 on success
    int _set_sys_params();
    bool _asynch_commit;    

}; // EOF ShoreEnv



EXIT_NAMESPACE(shore);

#endif /* __SHORE_ENV_H */

