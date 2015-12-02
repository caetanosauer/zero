#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

#include <fcntl.h>
#include <unistd.h>
#include <sstream>


#include "w_stream.h"
#include "w.h"
#include "w_strstream.h"
#include "sm_vas.h"
#include "sm_base.h"
#include "generic_page.h"
#include "smthread.h"
#include "btree.h"
#include "btcursor.h"
#include "btree_impl.h"
#include "btree_page_h.h"
#include "btree_test_env.h"
#include "sm_options.h"
#include "xct.h"
#include "sm_base.h"
#include "sm_external.h"
#include "srwlock.h"

// log buffer
#include "logbuf_common.h"

#include "../nullbuf.h"
#if W_DEBUG_LEVEL <= 3
nullbuf null_obj;
std::ostream vout (&null_obj);
#endif // W_DEBUG_LEVEL
namespace {
    char device_name[MAXPATHLEN] = "./volumes/dev_test";
    char global_log_dir[MAXPATHLEN] = "./log";
    char global_clog_dir[MAXPATHLEN] = "./clog";
    char global_archive_dir[MAXPATHLEN] = "./archive";
    char global_backup_dir[MAXPATHLEN] = "./backups";
}

// For multiple thread access to btree_populate_records
typedef srwlock_t sm_test_rwlock_t;
static sm_test_rwlock_t    _test_begin_xct_mutex;

sm_options btree_test_env::make_sm_options(
            int32_t locktable_size,
            int bufferpool_size_in_pages,
            uint32_t cleaner_threads,
            uint32_t cleaner_interval_millisec_min,
            uint32_t cleaner_interval_millisec_max,
            uint32_t cleaner_write_buffer_pages,
            bool initially_enable_cleaners,
            bool enable_swizzling,
            const std::vector<std::pair<const char*, int64_t> > &additional_int_params,
            const std::vector<std::pair<const char*, bool> > &additional_bool_params,
            const std::vector<std::pair<const char*, const char*> > &additional_string_params) {
    sm_options options;
    options.set_int_option("sm_bufpoolsize", SM_PAGESIZE / 1024 * bufferpool_size_in_pages);
    options.set_int_option("sm_locktablesize", locktable_size);
    // Most testcases make little locks. to speed them up, use small number here.
    // In a few testcases that need many locks, specify larger number in sm_options.
    options.set_int_option("sm_rawlock_lockpool_initseg", 20);
    options.set_int_option("sm_rawlock_xctpool_initseg", 20);
    options.set_int_option("sm_rawlock_lockpool_segsize", 1 << 10);
    options.set_int_option("sm_rawlock_xctpool_segsize", 1 << 8);
    // most of experiments don't bother creating transactions, so we can't garbage collect.
    // to not throw away active generations, just increase threashold.
    options.set_int_option("sm_rawlock_gc_generation_count", 100);
    options.set_string_option("sm_logdir", global_log_dir);
    options.set_string_option("sm_archdir", global_archive_dir);
    options.set_int_option("sm_num_page_writers", cleaner_threads);
    options.set_int_option("sm_cleaner_interval_millisec_min", cleaner_interval_millisec_min);
    options.set_int_option("sm_cleaner_interval_millisec_max", cleaner_interval_millisec_max);
    options.set_int_option("sm_cleaner_write_buffer_pages", cleaner_write_buffer_pages);
    options.set_bool_option("sm_backgroundflush", initially_enable_cleaners);
    options.set_bool_option("sm_bufferpool_swizzle", enable_swizzling);

    for (std::vector<std::pair<const char*, int64_t> >::const_iterator iter = additional_int_params.begin();
            iter != additional_int_params.end(); ++iter) {
        std::cout << "additional int parameter: " << iter->first << "=" << iter->second << std::endl;
        options.set_int_option(iter->first, iter->second);
    }
    for (std::vector<std::pair<const char*, bool> >::const_iterator iter = additional_bool_params.begin();
            iter != additional_bool_params.end(); ++iter) {
        std::cout << "additional bool parameter: " << iter->first << "=" << iter->second << std::endl;
        options.set_bool_option(iter->first, iter->second);
    }
    for (std::vector<std::pair<const char*, const char*> >::const_iterator iter = additional_string_params.begin();
            iter != additional_string_params.end(); ++iter) {
        std::cout << "additional string parameter: " << iter->first << "=" << iter->second << std::endl;
        options.set_string_option(iter->first, iter->second);
    }
    return options;
}
sm_options btree_test_env::make_sm_options(
            int32_t locktable_size,
            int bufferpool_size_in_pages,
            uint32_t cleaner_threads,
            uint32_t cleaner_interval_millisec_min,
            uint32_t cleaner_interval_millisec_max,
            uint32_t cleaner_write_buffer_pages,
            bool initially_enable_cleaners,
            bool enable_swizzling) {
    std::vector<std::pair<const char*, int64_t> > dummy_int;
    std::vector<std::pair<const char*, bool> > dummy_bool;
    std::vector<std::pair<const char*, const char*> > dummy_string;
    return make_sm_options(locktable_size,
            bufferpool_size_in_pages,
            cleaner_threads,
            cleaner_interval_millisec_min,
            cleaner_interval_millisec_max,
            cleaner_write_buffer_pages,
            initially_enable_cleaners,
            enable_swizzling,
            dummy_int, dummy_bool, dummy_string);
}

/** thread object to host Btree test functors. */
class testdriver_thread_t : public smthread_t {
public:

        testdriver_thread_t(test_functor *functor,
            btree_test_env *env,
            const sm_options &options)
                : smthread_t(t_regular, "testdriver_thread_t"),
                _env(env),
                _options(options),
                _retval(0),
                _functor(functor)
        {
            // Initialize using serial traditional restart mode
            // constructor used by test_crash.cpp which does not specify restart mode
            do_construct(m1_default_restart);
        }

        testdriver_thread_t(test_functor *functor,
            btree_test_env *env,
            const sm_options &options,
            int32_t restart_mode)
                : smthread_t(t_regular, "testdriver_thread_t"),
                _env(env),
                _options(options),
                _retval(0),
                _functor(functor)
        {
            // Initialize using caller specified restart mode
            do_construct(restart_mode);
        }

        ~testdriver_thread_t()  {}

        void run();
        int  return_value() const { return _retval; }


private:
        void   do_construct(int32_t restart_mode);
        w_rc_t do_init();

        btree_test_env *_env;
        sm_options      _options; // run-time options
        int             _retval; // return value from run()
        test_functor*   _functor;// test functor object
};

void
testdriver_thread_t::do_construct(int32_t restart_mode)
{
    // Private function called by testdriver_thread_t constructors to
    // complement required options if not set
    // Also set up the restart mode per caller's request

    std::string not_set("not_set");
    int not_set_int = -1;
    _options.set_string_option("sm_dbfile", device_name);
    if (_options.get_string_option("sm_logdir", not_set) == not_set) {
        _options.set_string_option("sm_logdir", global_log_dir);
    }
    if (_options.get_string_option("sm_archdir", not_set) == not_set) {
        _options.set_string_option("sm_archdir", global_archive_dir);
    }
    if (_options.get_string_option("sm_backup_dir", not_set) == not_set) {
        _options.set_string_option("sm_backup_dir", global_backup_dir);
    }
    if (_options.get_int_option("sm_bufpoolsize", not_set_int) == not_set_int) {
        _options.set_int_option("sm_bufpoolsize",
                 SM_PAGESIZE / 1024 * default_bufferpool_size_in_pages);
    }

    if(_options.get_bool_option("sm_testenv_init_vol", true)) {
        _options.set_bool_option("sm_truncate", true);
    }

    // Control which internal restart mode/setting to use.  This is the only place from test suites
    // to set the value for 'sm_restart'.
    // If not set, the internal default value is determined in sm.cpp (hard coded).
    //
    // This function is called by all constructors, while mode 1 (serial traditional restart)
    // is for non-restart related test suites and serial traditional restart test suite
    // (test_crash)
    // Other restart modes are for target restart testing (e.g. test_restart,
    // test_concurrent_restart), not used by non-restart related test suites.
    //
    // Valid modes are specified in sm.cpp, these are internal setting, see sm.cpp for
    // detail information on each mode
    // If an invalid restart_mode was specified, the system defaults to the internal
    // default setting in sm.cpp
    if (_options.get_int_option("sm_restart", not_set_int) == not_set_int) {
        _options.set_int_option("sm_restart", restart_mode);
    }
}

rc_t
testdriver_thread_t::do_init()
{
    if(_options.get_bool_option("sm_testenv_init_vol", true)) {
        if (_functor->_need_init) {
            _functor->_test_volume._device_name = device_name;
        }
    }

    return RCOK;
}

void testdriver_thread_t::run()
{
    // Now start a storage manager.
    vout << "Starting SSM and performing recovery ..." << endl;
    {
        ss_m ssm (_options);
        _env->_ssm = &ssm;

        sm_config_info_t config_info;
        rc_t rc = ss_m::config_info(config_info);
        if(rc.is_error()) {
            cerr << "Could not get storage manager configuration info: " << rc << endl;
            _retval = 1;
            return;
        }
        rc = do_init();
        if(rc.is_error()) {
            cerr << "Init failed: " << rc << endl;
            _retval = 1;
            return;
        }

        rc = _functor->run_test (&ssm);
        if(rc.is_error()) {
            cerr << "Failure: " << rc << endl;
            _retval = 1;
            return;
        }

        if (!_functor->_clean_shutdown) {
            ssm.set_shutdown_flag(false);
        }

        // end of ssm scope
        // Clean up and shut down
        vout << "\nShutting down SSM " << (_functor->_clean_shutdown ? "cleanly" : "simulating a crash") << "..." << endl;
        _env->_ssm = NULL;
    }
    vout << "Finished!" << endl;
}


btree_test_env::btree_test_env()
{
    _ssm = NULL;
    _use_locks = false;
}

btree_test_env::~btree_test_env()
{
}


void btree_test_env::assure_empty_dir(const char *folder_name)
{
// TODO(Restart)... performance, for AFTER testing
    assure_dir(folder_name);
    empty_dir(folder_name);
}
void btree_test_env::assure_dir(const char *folder_name)
{
    vout << "creating folder '" << folder_name << "' if not exists..." << endl;
    if (mkdir(folder_name, S_IRWXU) == 0) {
        vout << "created." << endl;
    } else {
        if (errno == EEXIST) {
            vout << "already exists." << endl;
        } else {
            cerr << "couldn't create folder. error:" << errno << "." << endl;
            ASSERT_TRUE(false);
        }
    }
}

void btree_test_env::empty_dir(const char *folder_name)
{
// TODO(Restart)... performance, for AFTER testing
/**/
    // want to use boost::filesystem... but let's keep this project boost-free.
    vout << "removing existing files..." << endl;
    DIR *d = opendir(folder_name);
    ASSERT_TRUE(d != NULL);
    for (struct dirent *ent = readdir(d); ent != NULL; ent = readdir(d)) {
        // Skip "." and ".."
        if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) {
            continue;
        }
        std::stringstream buf;
        buf << folder_name << "/" << ent->d_name;
        std::remove (buf.str().c_str());
    }
    ASSERT_EQ(closedir (d), 0);
/**/
}

void btree_test_env::SetUp()
{
#ifdef LOG_DIRECT_IO
    char tests_dir[MAXPATHLEN] = "/var/tmp/";
#else
    //char tests_dir[MAXPATHLEN] = "/dev/shm/";
    char tests_dir[MAXPATHLEN] = "/var/tmp/";
#endif
    strcat(tests_dir, getenv("USER"));
    assure_dir(tests_dir);
    strcat(tests_dir, "/btree_test_env");
    assure_dir(tests_dir);
    strcpy(log_dir, tests_dir);
    strcpy(clog_dir, tests_dir);
    strcpy(archive_dir, tests_dir);
    strcpy(vol_dir, tests_dir);
    strcat(log_dir, "/log");
    strcat(clog_dir, "/clog");
    strcat(archive_dir, "/archive");
    strcat(vol_dir, "/volumes");
    assure_empty_dir(log_dir);
    assure_empty_dir(clog_dir);
    assure_empty_dir(archive_dir);
    assure_empty_dir(vol_dir);
    strcpy(device_name, vol_dir);
    strcat(device_name, "/dev_test");
    strcpy(global_log_dir, log_dir);
    strcpy(global_clog_dir, clog_dir);
    strcpy(global_archive_dir, archive_dir);
    _use_locks = false;
}
void btree_test_env::empty_logdata_dir()
{
// TODO(Restart)... performance, for AFTER testing
    empty_dir(log_dir);
    empty_dir(clog_dir);
    empty_dir(archive_dir);
    empty_dir(vol_dir);
}

void btree_test_env::TearDown()
{

}

int btree_test_env::runBtreeTest (w_rc_t (*func)(ss_m*, test_volume_t*),
                    bool use_locks, int32_t lock_table_size,
                    int bufferpool_size_in_pages,
                    uint32_t cleaner_threads,
                    uint32_t cleaner_interval_millisec_min,
                    uint32_t cleaner_interval_millisec_max,
                    uint32_t cleaner_write_buffer_pages,
                    bool initially_enable_cleaners,
                    bool enable_swizzling
                                 )
{
    return runBtreeTest(func, use_locks,
                make_sm_options(lock_table_size,
                    bufferpool_size_in_pages,
                    cleaner_threads,
                    cleaner_interval_millisec_min,
                    cleaner_interval_millisec_max,
                    cleaner_write_buffer_pages,
                    initially_enable_cleaners,
                    enable_swizzling));
}

int btree_test_env::runBtreeTest (w_rc_t (*func)(ss_m*, test_volume_t*),
        bool use_locks, int32_t lock_table_size,
        int bufferpool_size_in_pages,
        uint32_t cleaner_threads,
        uint32_t cleaner_interval_millisec_min,
        uint32_t cleaner_interval_millisec_max,
        uint32_t cleaner_write_buffer_pages,
        bool initially_enable_cleaners,
        bool enable_swizzling,
        const std::vector<std::pair<const char*, int64_t> > &additional_int_params,
        const std::vector<std::pair<const char*, bool> > &additional_bool_params,
        const std::vector<std::pair<const char*, const char*> > &additional_string_params
) {
    return runBtreeTest(func, use_locks,
                make_sm_options(lock_table_size,
                    bufferpool_size_in_pages,
                    cleaner_threads,
                    cleaner_interval_millisec_min,
                    cleaner_interval_millisec_max,
                    cleaner_write_buffer_pages,
                    initially_enable_cleaners,
                    enable_swizzling,
                    additional_int_params, additional_bool_params, additional_string_params));
}

int btree_test_env::runBtreeTest (w_rc_t (*func)(ss_m*, test_volume_t*),
    bool use_locks, const sm_options &options) {
    _use_locks = use_locks;
    int rv;
    {
        default_test_functor functor(func);
        testdriver_thread_t smtu(&functor, this, options);

        /* cause the thread's run() method to start */
        w_rc_t e = smtu.fork();
        if(e.is_error()) {
            cerr << "Error forking thread: " << e <<endl;
            return 1;
        }

        /* wait for the thread's run() method to end */
        e = smtu.join();
        if(e.is_error()) {
            cerr << "Error joining thread: " << e <<endl;
            return 1;
        }

        rv = smtu.return_value();
    }
    return rv;
}

// Begin... for test_restart.cpp, test_concurrent_restart.cpp
int btree_test_env::runRestartTest (restart_test_base *context,
    restart_test_options *restart_options,
    bool use_locks, int32_t lock_table_size,
    int bufferpool_size_in_pages,
    uint32_t cleaner_threads,
    uint32_t cleaner_interval_millisec_min,
    uint32_t cleaner_interval_millisec_max,
    uint32_t cleaner_write_buffer_pages,
    bool initially_enable_cleaners,
    bool enable_swizzling) {
    return runRestartTest(context, restart_options, use_locks,
            make_sm_options(lock_table_size,
                    bufferpool_size_in_pages,
                    cleaner_threads,
                    cleaner_interval_millisec_min,
                    cleaner_interval_millisec_max,
                    cleaner_write_buffer_pages,
                    initially_enable_cleaners,
                    enable_swizzling));
}

int btree_test_env::runRestartTest (restart_test_base *context,
    restart_test_options *restart_options,
    bool use_locks, int32_t lock_table_size,
    int bufferpool_size_in_pages,
    uint32_t cleaner_threads,
    uint32_t cleaner_interval_millisec_min,
    uint32_t cleaner_interval_millisec_max,
    uint32_t cleaner_write_buffer_pages,
    bool initially_enable_cleaners,
    bool enable_swizzling,
    const std::vector<std::pair<const char*, int64_t> > &additional_int_params,
    const std::vector<std::pair<const char*, bool> > &additional_bool_params,
    const std::vector<std::pair<const char*, const char*> > &additional_string_params) {
    return runRestartTest(context, restart_options, use_locks,
        make_sm_options(lock_table_size,
                    bufferpool_size_in_pages,
                    cleaner_threads,
                    cleaner_interval_millisec_min,
                    cleaner_interval_millisec_max,
                    cleaner_write_buffer_pages,
                    initially_enable_cleaners,
                    enable_swizzling,
                    additional_int_params, additional_bool_params, additional_string_params));
}
int btree_test_env::runRestartTest (restart_test_base *context, restart_test_options *restart_options,
                                      bool use_locks, const sm_options &options) {
    _use_locks = use_locks;
    _restart_options = restart_options;
    // This function is called by restart test cases, while caller specify
    // the restart mode via 'restart_mode'
    // e.g., serial traditional, various concurrent combinations

    DBGOUT2 ( << "Going to call pre_shutdown()...");
    int rv;
    w_rc_t e;
        {
        if (restart_options->shutdown_mode == simulated_crash)
            {
            // Simulated crash
            restart_dirty_test_pre_functor functor(context);
            testdriver_thread_t smtu(&functor, this, options, restart_options->restart_mode);  // User specified restart mode
            e = smtu.fork();
            if(e.is_error())
                {
                cerr << "Error forking thread while pre_shutdown: " << e <<endl;
                return 1;
                }
            e = smtu.join();
            if(e.is_error())
                {
                cerr << "Error joining thread while pre_shutdown: " << e <<endl;
                return 1;
                }

            rv = smtu.return_value();
            if (rv != 0)
                {
                cerr << "Error while pre_shutdown rv= " << rv <<endl;
                return rv;
                }
            }
        else
            {
            // Clean shutdown
            restart_clean_test_pre_functor functor(context);
            testdriver_thread_t smtu(&functor, this, options, restart_options->restart_mode);  // User specified restart mode
            e = smtu.fork();
            if(e.is_error())
                {
                cerr << "Error forking thread while pre_shutdown: " << e <<endl;
                return 1;
                }
            e = smtu.join();
            if(e.is_error())
                {
                cerr << "Error joining thread while pre_shutdown: " << e <<endl;
                return 1;
                }

            rv = smtu.return_value();
            if (rv != 0)
                {
                cerr << "Error while pre_shutdown rv= " << rv <<endl;
                return rv;
                }
            }
        }

    if (restart_options->restart_mode == smlevel_0::t_restart_disable) {
        return rv;
    }

    DBGOUT2 ( << "Going to call post_shutdown()...");
        {
        restart_test_post_functor functor(context);
        testdriver_thread_t smtu(&functor, this, options, restart_options->restart_mode);   // User specified restart mode

        w_rc_t e = smtu.fork();
        if(e.is_error())
            {
            cerr << "Error forking thread while post_shutdown: " << e <<endl;
            return 1;
            }

        e = smtu.join();
        if(e.is_error())
            {
            cerr << "Error joining thread while post_shutdown: " << e <<endl;
            return 1;
            }

        rv = smtu.return_value();
        }

    return rv;

}
// End... for test_restart.cpp, test_concurrent_restart.cpp

int btree_test_env::runCrashTest (crash_test_base *context,
    bool use_locks, int32_t lock_table_size,
    int bufferpool_size_in_pages,
    uint32_t cleaner_threads,
    uint32_t cleaner_interval_millisec_min,
    uint32_t cleaner_interval_millisec_max,
    uint32_t cleaner_write_buffer_pages,
    bool initially_enable_cleaners,
    bool enable_swizzling) {
    return runCrashTest(context, use_locks,
                make_sm_options(lock_table_size,
                    bufferpool_size_in_pages,
                    cleaner_threads,
                    cleaner_interval_millisec_min,
                    cleaner_interval_millisec_max,
                    cleaner_write_buffer_pages,
                    initially_enable_cleaners,
                    enable_swizzling));
}

int btree_test_env::runCrashTest (crash_test_base *context,
    bool use_locks, int32_t lock_table_size,
    int bufferpool_size_in_pages,
    uint32_t cleaner_threads,
    uint32_t cleaner_interval_millisec_min,
    uint32_t cleaner_interval_millisec_max,
    uint32_t cleaner_write_buffer_pages,
    bool initially_enable_cleaners,
    bool enable_swizzling,
    const std::vector<std::pair<const char*, int64_t> > &additional_int_params,
    const std::vector<std::pair<const char*, bool> > &additional_bool_params,
    const std::vector<std::pair<const char*, const char*> > &additional_string_params) {
    return runCrashTest(context, use_locks,
                make_sm_options(lock_table_size,
                    bufferpool_size_in_pages,
                    cleaner_threads,
                    cleaner_interval_millisec_min,
                    cleaner_interval_millisec_max,
                    cleaner_write_buffer_pages,
                    initially_enable_cleaners,
                    enable_swizzling,
                    additional_int_params, additional_bool_params, additional_string_params));
}
int btree_test_env::runCrashTest (crash_test_base *context, bool use_locks, const sm_options &options) {
    _use_locks = use_locks;

    DBGOUT2 ( << "Going to call pre_crash()...");
    int rv;
    {
        crash_test_pre_functor functor(context);
        testdriver_thread_t smtu(&functor, this,  options);  // Use serial restart mode

        w_rc_t e = smtu.fork();
        if(e.is_error()) {
            cerr << "Error forking thread while pre_crash: " << e <<endl;
            return 1;
        }

        e = smtu.join();
        if(e.is_error()) {
            cerr << "Error joining thread while pre_crash: " << e <<endl;
            return 1;
        }

        rv = smtu.return_value();
        if (rv != 0) {
            cerr << "Error while pre_crash rv= " << rv <<endl;
            return rv;
        }
    }
    DBGOUT2 ( << "Crash simulated! going to call post_crash()...");
    {
        crash_test_post_functor functor(context);
        testdriver_thread_t smtu(&functor, this, options);  // Use serial restart mode

        w_rc_t e = smtu.fork();
        if(e.is_error()) {
            cerr << "Error forking thread while post_crash: " << e <<endl;
            return 1;
        }

        e = smtu.join();
        if(e.is_error()) {
            cerr << "Error joining thread while post_crash: " << e <<endl;
            return 1;
        }

        rv = smtu.return_value();
    }

    return rv;

}

// Begin... for Instant Restart performance tests
// Main API for caller to start the test
int btree_test_env::runRestartPerfTest (
    restart_performance_test_base *context,             // Required, specify the basic setup
    restart_test_options *restart_options,              // Required, specify restart options (e.g. milestone)
    bool use_locks,                                     // Required, true (locking), false (no locking)
    int32_t lock_table_size,                            // Optional, default: default_locktable_size
    int bufferpool_size_in_pages,                       // Optional, default: default_bufferpool_size_in_pages
    uint32_t cleaner_threads,                           // Optional, default: 1
    uint32_t cleaner_interval_millisec_min,             // Optional, default: 1000
    uint32_t cleaner_interval_millisec_max,             // Optional, default: 256000
    uint32_t cleaner_write_buffer_pages,                // Optional, default: 64
    bool initially_enable_cleaners,                     // Optional, default: true
    bool enable_swizzling)                              // Optional, default: default_enable_swizzling
{
    return runRestartPerfTest(context, restart_options, use_locks,
                              make_sm_options(lock_table_size,
                                              bufferpool_size_in_pages,
                                              cleaner_threads,
                                              cleaner_interval_millisec_min,
                                              cleaner_interval_millisec_max,
                                              cleaner_write_buffer_pages,
                                              initially_enable_cleaners,
                                              enable_swizzling));
}

// Optional API, not used for prestart performance test currently
int btree_test_env::runRestartPerfTest (
    restart_performance_test_base *context,                // Required, specify the basic setup
    restart_test_options *restart_options,                 // Required, specify restart options (e.g. milestone)
    bool use_locks,                                        // Required, true (locking), false (no locking)
    int32_t lock_table_size,                               // Required
    int bufferpool_size_in_pages, // Required
    uint32_t cleaner_threads,                              // Required
    uint32_t cleaner_interval_millisec_min,                // Required
    uint32_t cleaner_interval_millisec_max,                // Required
    uint32_t cleaner_write_buffer_pages,                   // Required
    bool initially_enable_cleaners,                        // Required
    bool enable_swizzling,                                 // Required
    const std::vector<std::pair<const char*, int64_t> > &additional_int_params,         // Required
    const std::vector<std::pair<const char*, bool> > &additional_bool_params,           // Required
    const std::vector<std::pair<const char*, const char*> > &additional_string_params)  // Required
{
    return runRestartPerfTest(context, restart_options, use_locks,
                              make_sm_options(lock_table_size,
                                              bufferpool_size_in_pages,
                                              cleaner_threads,
                                              cleaner_interval_millisec_min,
                                              cleaner_interval_millisec_max,
                                              cleaner_write_buffer_pages,
                                              initially_enable_cleaners,
                                              enable_swizzling,
                                              additional_int_params,
                                              additional_bool_params,
                                              additional_string_params));
}

// Internal API to start the restart performance test
int btree_test_env::runRestartPerfTest (
    restart_performance_test_base *context,  // In: specify the basic setup, e.g. start from scratch or not
    restart_test_options *restart_options,   // In: specify restart options, e.g. milestone
    bool use_locks,                          // In: ture if enable locking (M3, M4), false if disable locking (M1, M2)
    const sm_options &options)               // In: other settings
{
    _use_locks = use_locks;
    _restart_options = restart_options;

    // This function is called by restart performance test cases, while caller specify
    // the restart mode via 'restart_option'
    // e.g., serial traditional, various concurrent combinations

    // Performance test has 3 phases:
    //   Initial_shutdow - populate database and then clean shutdown
    //                            same operation for all milestones
    //   pre_shutdown - update the existing database from phase 1 (initial_shutdown)
    //                           caller specifies either clean or crash (in-flight transactions) shutdown
    //   post_shutdown - using the existing database from phase 2 (pre_shutdown)
    //                            performance measurement on 1000 successful user transactions, clean shutdown

    int rv;
    w_rc_t e;
    DBGOUT2 ( << "Going to call initial_shutdown()...");
    {
        // Start from a new database and populate it
        // Only clean shutdown from this phase
        restart_performance_initial_functor functor(context);
        testdriver_thread_t smtu(&functor, this, options, restart_options->restart_mode);  // User specified restart mode
        e = smtu.fork();
        if (e.is_error())
        {
            std::cerr << "Error forking thread while initial_shutdown: " << e << std::endl;
            return 1;
        }
        e = smtu.join();
        if (e.is_error())
        {
            std::cerr << "Error joining thread while initial_shutdown: " << e << std::endl;
            return 1;
        }

        rv = smtu.return_value();
        if (rv != 0)
        {
            std::cerr << "Error while initial_shutdown rv= " << rv << std::endl;
            return rv;
        }
    }

    DBGOUT2 ( << "Going to call pre_shutdown()...");
    {
        if (restart_options->shutdown_mode == simulated_crash)
        {
            // Start from an existing database created in phase 1
            // Simulated crash
            restart_performance_dirty_pre_functor functor(context);
            testdriver_thread_t smtu(&functor, this, options, restart_options->restart_mode);  // User specified restart mode
            e = smtu.fork();
            if (e.is_error())
            {
                std::cerr << "Error forking thread while pre_shutdown: " << e << std::endl;
                return 1;
            }
            e = smtu.join();
            if (e.is_error())
            {
                std::cerr << "Error joining thread while pre_shutdown: " << e << std::endl;
                return 1;
            }

            rv = smtu.return_value();
            if (rv != 0)
            {
                std::cerr << "Error while pre_shutdown rv= " << rv << std::endl;
                return rv;
            }
        }
        else
        {
            // Start from an existing database created in phase 1
            // Clean shutdown
            restart_performance_clean_pre_functor functor(context);
            testdriver_thread_t smtu(&functor, this, options, restart_options->restart_mode);  // User specified restart mode
            e = smtu.fork();
            if (e.is_error())
            {
                std::cerr << "Error forking thread while pre_shutdown: " << e << std::endl;
                return 1;
            }
            e = smtu.join();
            if (e.is_error())
            {
                std::cerr << "Error joining thread while pre_shutdown: " << e << std::endl;
                return 1;
            }

            rv = smtu.return_value();
            if (rv != 0)
            {
                std::cerr << "Error while pre_shutdown rv= " << rv << std::endl;
                return rv;
            }
        }
    }
    DBGOUT2 ( << "Going to call post_shutdown()...");
    {
        // Start from an existing database from phase 2

        // Flush system cache first
        // Force a flush of system cache before shutdown, the goal is to
        // make sure after the simulated system crash, the recovery process
        // has to get data from disk, not file system cache
/**/
        int i;
        i = system("free -m");  // before free cache
        i = system("/bin/sync");  // flush but not free the page cache
        i = system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'");  // free page cache and inode cache
        i = system("free -m");  // after free cache
        // Force read/write a big file
        i = system("dd if=/home/weyg/test.core of=/home/weyg/test2.core bs=100k count=10000 conv=fdatasync"); // Read and write 1G of data using cache
        i = system("rm /home/weyg/test2.core");

        std::cout << "Return value from system command: " << i << std::endl;
/**/
        struct timeval tm_before;
        gettimeofday( &tm_before, NULL );

        std::cout << "More read... " << std::endl;
        int pfd;
//        pfd = ::open("/home/weyg/more.core", O_RDONLY);      // 6.4 GB
//        long source_size = 6385741824;
        pfd = ::open("/home/weyg/dev_test", O_RDONLY);   // 549.9 MB
//        long source_size = 549928960;
//        pfd = ::open("/home/weyg/large.core", O_RDONLY); 	 // 25.5 GB
//        long source_size = 25542967296;

        if (-1 == pfd)
        {
            // Not able to open the specified file, continue...
            std::cout << "Failed to open file for read: " << pfd << std::endl;
        }
        else
        {
            // Read a lot to fill the system and/or device cache
            const int per_read_size = 8192;
            char buffer[per_read_size];
            int offset = per_read_size * 10;  // Starting offset
            for (int i = 0; i < 30000; ++i)
            {
/* Relative seek  *
                offset = ::rand()%1000+99;
                // Move to a new location
                __off_t seeked = ::lseek(pfd, offset, SEEK_CUR);
                w_assert0(-1 != seeked);
                // Read some bytes
                int read_bytes = ::read(pfd, buffer, sizeof(buffer));
                w_assert0(-1 != read_bytes);
**/

/* Re-seek from the beginning each time *
                offset += per_read_size + ::rand()%1000+99;
                // Move to a new location
                __off_t seeked = ::lseek(pfd, offset, SEEK_SET);
                w_assert0(-1 != seeked);
               // Read some bytes
               int read_bytes = ::read(pfd, buffer, sizeof(buffer));
               w_assert0(-1 != read_bytes);
**/

/* One call with re-seek from the beginning each time */
                offset += per_read_size + ::rand()%1000+99;
                int read_bytes = ::pread(pfd, buffer, sizeof(buffer), offset);
                w_assert0(-1 != read_bytes);
/**/

/* Relative seek with offset on multiple of 8192 *
                offset = (::rand()%10+1) * per_read_size;
                // Move to a new location
                __off_t seeked = ::lseek(pfd, offset, SEEK_CUR);
                w_assert0(-1 != seeked);
                // Read some bytes
                int read_bytes = ::read(pfd, buffer, sizeof(buffer));
                w_assert0(-1 != read_bytes);
**/

/* One call with re-seek. offset on multiple of 8192 *
                offset += (::rand()%4+1) * per_read_size;
                int read_bytes = ::pread(pfd, buffer, sizeof(buffer), offset);
                w_assert0(-1 != read_bytes);
**/

/* Re-seek with offset on 8192, backward *
                offset += per_read_size;
                int real_offset = 0 - offset;  // make negative offset to go backward
                // Move to a new location
                __off_t seeked = ::lseek(pfd, real_offset, SEEK_END);
                w_assert0(-1 != seeked);
                // Read some bytes
                int read_bytes = ::read(pfd, buffer, sizeof(buffer));
                w_assert0(-1 != read_bytes);
**/

/* One call with re-seek. backward read, !!!! only work with 6GB source  !!!!*
                offset += per_read_size + (::rand()%10+1) * per_read_size;
                __off_t new_offset = (__off_t)(source_size - offset);
                int read_bytes = ::pread(pfd, buffer, sizeof(buffer), new_offset);
                w_assert0(-1 != read_bytes);
**/

/* One call with re-seek. backward read, !!!! only work with 25.5GB source  !!!!*
                offset += per_read_size + (::rand()%20+1) * per_read_size;
                __off_t new_offset = (__off_t)(source_size - offset);
                int read_bytes = ::pread(pfd, buffer, sizeof(buffer), new_offset);
                w_assert0(-1 != read_bytes);
**/


            }
            // Done, close the file
            ::close(pfd);
            std::cout << "Done with extra read" << std::endl;

            struct timeval tm_after;
            gettimeofday( &tm_after, NULL );
            DBGOUT0(<< "**** Random reads, elapsed time (milliseconds): "
                    << (((double)tm_after.tv_sec - (double)tm_before.tv_sec) * 1000.0)
                    + (double)tm_after.tv_usec/1000.0 - (double)tm_before.tv_usec/1000.0);
        }
/**/

        // Setup timer first
        // CUP cycles
        unsigned int hi, lo;
        __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
        context->_start = ((unsigned long long)lo)|( ((unsigned long long)hi)<<32);

        // Elapsed time in millisecs seconds
        struct timeval tm;
        gettimeofday( &tm, NULL );

        const double   MICROSECONDS_IN_SECOND  = 1000000.0; // microseconds in a second
        const double   MILLISECS_IN_SECOND     = 1000.0;    // millisecs in a second

        context->_start_time = ((double)tm.tv_sec*MILLISECS_IN_SECOND) +
                              ((double)tm.tv_usec/(MICROSECONDS_IN_SECOND/MILLISECS_IN_SECOND));

        // Specify clean shutdown at the end of this phase
        restart_performance_post_functor functor(context);
        testdriver_thread_t smtu(&functor, this, options, restart_options->restart_mode);   // User specified restart mode

        w_rc_t e = smtu.fork();
        if (e.is_error())
        {
            std::cerr << "Error forking thread while post_shutdown: " << e << std::endl;
            return 1;
        }

        e = smtu.join();
        if (e.is_error())
        {
            std::cerr << "Error joining thread while post_shutdown: " << e << std::endl;
            return 1;
        }

        rv = smtu.return_value();
    }

    return rv;
}
// End... for Instant Restart performance tests

// Begin... for Instant Restart performance before tests
// Main API for caller to start the test
int btree_test_env::runRestartPerfTestBefore (
    restart_performance_test_base *context,             // Required, specify the basic setup
    restart_test_options *restart_options,              // Required, specify restart options (e.g. milestone)
    bool use_locks,                                     // Required, true (locking), false (no locking)
    int32_t lock_table_size,                            // Optional, default: default_locktable_size
    int bufferpool_size_in_pages,                       // Optional, default: default_bufferpool_size_in_pages
    uint32_t cleaner_threads,                           // Optional, default: 1
    uint32_t cleaner_interval_millisec_min,             // Optional, default: 1000
    uint32_t cleaner_interval_millisec_max,             // Optional, default: 256000
    uint32_t cleaner_write_buffer_pages,                // Optional, default: 64
    bool initially_enable_cleaners,                     // Optional, default: true
    bool enable_swizzling)                              // Optional, default: default_enable_swizzling
{
    return runRestartPerfTestBefore(context, restart_options, use_locks,
                              make_sm_options(lock_table_size,
                                              bufferpool_size_in_pages,
                                              cleaner_threads,
                                              cleaner_interval_millisec_min,
                                              cleaner_interval_millisec_max,
                                              cleaner_write_buffer_pages,
                                              initially_enable_cleaners,
                                              enable_swizzling));
}

// Optional API, not used for prestart performance test currently
int btree_test_env::runRestartPerfTestBefore (
    restart_performance_test_base *context,                // Required, specify the basic setup
    restart_test_options *restart_options,                 // Required, specify restart options (e.g. milestone)
    bool use_locks,                                        // Required, true (locking), false (no locking)
    int32_t lock_table_size,                               // Required
    int bufferpool_size_in_pages, // Required
    uint32_t cleaner_threads,                              // Required
    uint32_t cleaner_interval_millisec_min,                // Required
    uint32_t cleaner_interval_millisec_max,                // Required
    uint32_t cleaner_write_buffer_pages,                   // Required
    bool initially_enable_cleaners,                        // Required
    bool enable_swizzling,                                 // Required
    const std::vector<std::pair<const char*, int64_t> > &additional_int_params,         // Required
    const std::vector<std::pair<const char*, bool> > &additional_bool_params,           // Required
    const std::vector<std::pair<const char*, const char*> > &additional_string_params)  // Required
{
    return runRestartPerfTestBefore(context, restart_options, use_locks,
                              make_sm_options(lock_table_size,
                                              bufferpool_size_in_pages,
                                              cleaner_threads,
                                              cleaner_interval_millisec_min,
                                              cleaner_interval_millisec_max,
                                              cleaner_write_buffer_pages,
                                              initially_enable_cleaners,
                                              enable_swizzling,
                                              additional_int_params,
                                              additional_bool_params,
                                              additional_string_params));
}

// Internal API to start the restart performance test
int btree_test_env::runRestartPerfTestBefore (
    restart_performance_test_base *context,  // In: specify the basic setup, e.g. start from scratch or not
    restart_test_options *restart_options,   // In: specify restart options, e.g. milestone
    bool use_locks,                          // In: ture if enable locking (M3, M4), false if disable locking (M1, M2)
    const sm_options &options)               // In: other settings
{
    _use_locks = use_locks;
    _restart_options = restart_options;

    // This function is called by restart performance 'before' test cases, while caller specify
    // the restart mode via 'restart_option'
    // e.g., serial traditional, various concurrent combinations

    // Performance test has 2 phases:
    //   Initial_shutdow - populate database and then clean shutdown
    //                            same operation for all milestones
    //   pre_shutdown - update the existing database from phase 1 (initial_shutdown)
    //                           caller specifies either clean or crash (in-flight transactions) shutdown

    int rv;
    w_rc_t e;
    DBGOUT2 ( << "Going to call initial_shutdown()...");
    {
        // Start from a new database and populate it
        // Only clean shutdown from this phase
        restart_performance_initial_functor functor(context);
        testdriver_thread_t smtu(&functor, this, options, restart_options->restart_mode);  // User specified restart mode
        e = smtu.fork();
        if (e.is_error())
        {
            std::cerr << "Error forking thread while initial_shutdown: " << e << std::endl;
            return 1;
        }
        e = smtu.join();
        if (e.is_error())
        {
            std::cerr << "Error joining thread while initial_shutdown: " << e << std::endl;
            return 1;
        }

        rv = smtu.return_value();
        if (rv != 0)
        {
            std::cerr << "Error while initial_shutdown rv= " << rv << std::endl;
            return rv;
        }
    }

    DBGOUT2 ( << "Going to call pre_shutdown()...");
    {
        if (restart_options->shutdown_mode == simulated_crash)
        {
            // Start from an existing database created in phase 1
            // Simulated crash
            restart_performance_dirty_pre_functor functor(context);
            testdriver_thread_t smtu(&functor, this, options, restart_options->restart_mode);  // User specified restart mode
            e = smtu.fork();
            if (e.is_error())
            {
                std::cerr << "Error forking thread while pre_shutdown: " << e << std::endl;
                return 1;
            }
            e = smtu.join();
            if (e.is_error())
            {
                std::cerr << "Error joining thread while pre_shutdown: " << e << std::endl;
                return 1;
            }

            rv = smtu.return_value();
            if (rv != 0)
            {
                std::cerr << "Error while pre_shutdown rv= " << rv << std::endl;
                return rv;
            }
        }
        else
        {
            // Start from an existing database created in phase 1
            // Clean shutdown
            restart_performance_clean_pre_functor functor(context);
            testdriver_thread_t smtu(&functor, this, options, restart_options->restart_mode);  // User specified restart mode
            e = smtu.fork();
            if (e.is_error())
            {
                std::cerr << "Error forking thread while pre_shutdown: " << e << std::endl;
                return 1;
            }
            e = smtu.join();
            if (e.is_error())
            {
                std::cerr << "Error joining thread while pre_shutdown: " << e << std::endl;
                return 1;
            }

            rv = smtu.return_value();
            if (rv != 0)
            {
                std::cerr << "Error while pre_shutdown rv= " << rv << std::endl;
                return rv;
            }
        }
    }

    return rv;
}
// End... for Instant Restart performance 'before' tests

// Begin... for Instant Restart performance 'after' tests
// Main API for caller to start the test
int btree_test_env::runRestartPerfTestAfter (
    restart_performance_test_base *context,             // Required, specify the basic setup
    restart_test_options *restart_options,              // Required, specify restart options (e.g. milestone)
    bool use_locks,                                     // Required, true (locking), false (no locking)
    int32_t lock_table_size,                            // Optional, default: default_locktable_size
    int bufferpool_size_in_pages,                       // Optional, default: default_bufferpool_size_in_pages
    uint32_t cleaner_threads,                           // Optional, default: 1
    uint32_t cleaner_interval_millisec_min,             // Optional, default: 1000
    uint32_t cleaner_interval_millisec_max,             // Optional, default: 256000
    uint32_t cleaner_write_buffer_pages,                // Optional, default: 64
    bool initially_enable_cleaners,                     // Optional, default: true
    bool enable_swizzling)                              // Optional, default: default_enable_swizzling
{
    return runRestartPerfTestAfter(context, restart_options, use_locks,
                              make_sm_options(lock_table_size,
                                              bufferpool_size_in_pages,
                                              cleaner_threads,
                                              cleaner_interval_millisec_min,
                                              cleaner_interval_millisec_max,
                                              cleaner_write_buffer_pages,
                                              initially_enable_cleaners,
                                              enable_swizzling));
}

// Optional API, not used for prestart performance test currently
int btree_test_env::runRestartPerfTestAfter (
    restart_performance_test_base *context,                // Required, specify the basic setup
    restart_test_options *restart_options,                 // Required, specify restart options (e.g. milestone)
    bool use_locks,                                        // Required, true (locking), false (no locking)
    int32_t lock_table_size,                               // Required
    int bufferpool_size_in_pages, // Required
    uint32_t cleaner_threads,                              // Required
    uint32_t cleaner_interval_millisec_min,                // Required
    uint32_t cleaner_interval_millisec_max,                // Required
    uint32_t cleaner_write_buffer_pages,                   // Required
    bool initially_enable_cleaners,                        // Required
    bool enable_swizzling,                                 // Required
    const std::vector<std::pair<const char*, int64_t> > &additional_int_params,         // Required
    const std::vector<std::pair<const char*, bool> > &additional_bool_params,           // Required
    const std::vector<std::pair<const char*, const char*> > &additional_string_params)  // Required
{
    return runRestartPerfTestAfter(context, restart_options, use_locks,
                              make_sm_options(lock_table_size,
                                              bufferpool_size_in_pages,
                                              cleaner_threads,
                                              cleaner_interval_millisec_min,
                                              cleaner_interval_millisec_max,
                                              cleaner_write_buffer_pages,
                                              initially_enable_cleaners,
                                              enable_swizzling,
                                              additional_int_params,
                                              additional_bool_params,
                                              additional_string_params));
}

// Internal API to start the restart performance test
int btree_test_env::runRestartPerfTestAfter (
    restart_performance_test_base *context,  // In: specify the basic setup, e.g. start from scratch or not
    restart_test_options *restart_options,   // In: specify restart options, e.g. milestone
    bool use_locks,                          // In: ture if enable locking (M3, M4), false if disable locking (M1, M2)
    const sm_options &options)               // In: other settings
{
    _use_locks = use_locks;
    _restart_options = restart_options;

    // This function is called by restart performance test cases, while caller specify
    // the restart mode via 'restart_option'
    // e.g., serial traditional, various concurrent combinations

    // Performance test has 1 phase:
    //   post_shutdown - using the existing database from phase 2 (pre_shutdown)
    //                            performance measurement on 1000 successful user transactions, clean shutdown

    int rv;
    w_rc_t e;

    DBGOUT2 ( << "Going to call post_shutdown()...");
    {
        // Start from an existing database from phase 2 with existing log and data files
        // Existing data file:
        // /dev/shm/weyg/btree_test_env/volumes/dev_test

        // Caller should do a manual system shutdown before calling this test case
        // to ensure all caches are clean

        // Setup timer first
        // CUP cycles
        unsigned int hi, lo;
        __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
        context->_start = ((unsigned long long)lo)|( ((unsigned long long)hi)<<32);

        // Elapsed time in millisecs seconds
        struct timeval tm;
        gettimeofday( &tm, NULL );

        const double   MICROSECONDS_IN_SECOND  = 1000000.0; // microseconds in a second
        const double   MILLISECS_IN_SECOND     = 1000.0;    // millisecs in a second

        context->_start_time = ((double)tm.tv_sec*MILLISECS_IN_SECOND) +
                              ((double)tm.tv_usec/(MICROSECONDS_IN_SECOND/MILLISECS_IN_SECOND));


        // Initialize and also specify starting from existing data file and clean shutdown at the end
        restart_performance_post_functor functor(context);
        testdriver_thread_t smtu(&functor, this, options, restart_options->restart_mode);   // User specified restart mode

        // Start the recovery
        w_rc_t e = smtu.fork();
        if (e.is_error())
        {
            std::cerr << "Error forking thread while post_shutdown: " << e << std::endl;
            return 1;
        }

        e = smtu.join();
        if (e.is_error())
        {
            std::cerr << "Error joining thread while post_shutdown: " << e << std::endl;
            return 1;
        }

        rv = smtu.return_value();
    }

    return rv;
}
// End... for Instant Restart performance 'after' tests


void btree_test_env::set_xct_query_lock() {
    if (_use_locks) {
        xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);
    } else {
        xct()->set_query_concurrency(smlevel_0::t_cc_none);
    }
}

w_rc_t x_btree_create_index(ss_m* ssm, test_volume_t *test_volume, StoreID &stid, PageID &root_pid)
{
    W_DO(ssm->begin_xct());
    W_DO(ssm->create_index(stid));
    W_DO(ssm->open_store(stid, root_pid));
    W_DO(ssm->commit_xct());
    return RCOK;
}

w_rc_t x_begin_xct(ss_m* ssm, bool use_locks)
{
    W_DO(ssm->begin_xct());
    if (use_locks) {
        xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);
    }
    return RCOK;
}
w_rc_t x_commit_xct(ss_m* ssm)
{
    W_DO(ssm->commit_xct());
    return RCOK;
}

w_rc_t x_abort_xct(ss_m* ssm)
{
    W_DO(ssm->abort_xct());
    return RCOK;
}

w_rc_t x_btree_get_root_pid(ss_m* ssm, const StoreID &stid, PageID &root_pid)
{
    W_DO(ssm->open_store_nolock(stid, root_pid));
    return RCOK;
}
w_rc_t x_btree_adopt_foster_all(ss_m* ssm, const StoreID &stid)
{
    PageID root_pid;
    W_DO (x_btree_get_root_pid (ssm, stid, root_pid));
    W_DO(ssm->begin_xct());
    {
        btree_page_h root_p;
        W_DO(root_p.fix_root(stid, LATCH_EX));
        W_DO(btree_impl::_sx_adopt_foster_all(root_p, true));
    }
    W_DO(ssm->commit_xct());
    return RCOK;
}
w_rc_t x_btree_verify(ss_m* ssm, const StoreID &stid) {
    W_DO(ssm->begin_xct());
    bool consistent;
    W_DO(ssm->verify_index(stid, 19, consistent));
    EXPECT_TRUE (consistent) << "BTree verification of index " << stid << " failed";
    W_DO(ssm->commit_xct());
    if (!consistent) {
        return RC(eBADARGUMENT); // or some other thing
    }
    return RCOK;
}

// helper function for insert. keystr/datastr have to be NULL terminated
w_rc_t x_btree_lookup_and_commit(ss_m* ssm, const StoreID &stid, const char *keystr, std::string &data, bool use_locks) {
    W_DO(ssm->begin_xct());
    if (use_locks) {
        xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);
    }
    rc_t rc = x_btree_lookup (ssm, stid, keystr, data);
    if (rc.is_error()) {
        W_DO (ssm->abort_xct());
    } else {
        W_DO(ssm->commit_xct());
    }
    return rc;
}
w_rc_t x_btree_lookup(ss_m* ssm, const StoreID &stid, const char *keystr, std::string &data) {
    w_keystr_t key;
    key.construct_regularkey(keystr, strlen(keystr));
    char buf[SM_PAGESIZE];
    smsize_t elen = SM_PAGESIZE;
    bool found;
    W_DO(ssm->find_assoc(stid, key, buf, elen, found));
    if (found) {
        data.assign(buf, elen);
    } else {
        data.clear();
    }
    return RCOK;
}

w_rc_t x_btree_insert(ss_m* ssm, const StoreID &stid, const char *keystr, const char *datastr) {
    w_keystr_t key;
    key.construct_regularkey(keystr, strlen(keystr));
    vec_t data;
    data.set(datastr, strlen(datastr));
    W_DO(ssm->create_assoc(stid, key, data));
    return RCOK;
}
w_rc_t x_btree_insert_and_commit(ss_m* ssm, const StoreID &stid, const char *keystr, const char *datastr, bool use_locks) {
    W_DO(ssm->begin_xct());
    if (use_locks) {
        xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);
    }
    rc_t rc = x_btree_insert (ssm, stid, keystr, datastr);
    if (rc.is_error()) {
        W_DO (ssm->abort_xct());
    } else {
        W_DO(ssm->commit_xct());
    }
    return rc;
}

// helper function for remove
w_rc_t x_btree_remove(ss_m* ssm, const StoreID &stid, const char *keystr) {
    w_keystr_t key;
    key.construct_regularkey(keystr, strlen(keystr));
    W_DO(ssm->destroy_assoc(stid, key));
    return RCOK;
}
w_rc_t x_btree_remove_and_commit(ss_m* ssm, const StoreID &stid, const char *keystr, bool use_locks) {
    W_DO(ssm->begin_xct());
    if (use_locks) {
        xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);
    }
    rc_t rc = x_btree_remove(ssm, stid, keystr);
    if (rc.is_error()) {
        W_DO (ssm->abort_xct());
    } else {
        W_DO(ssm->commit_xct());
    }
    return rc;
}

w_rc_t x_btree_update_and_commit(ss_m* ssm, const StoreID &stid, const char *keystr, const char *datastr, bool use_locks)
{
    W_DO(ssm->begin_xct());
    if (use_locks) {
        xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);
    }
    rc_t rc = x_btree_update(ssm, stid, keystr, datastr);
    if (rc.is_error()) {
        W_DO (ssm->abort_xct());
    } else {
        W_DO(ssm->commit_xct());
    }
    return rc;
}
w_rc_t x_btree_update(ss_m* ssm, const StoreID &stid, const char *keystr, const char *datastr)
{
    w_keystr_t key;
    key.construct_regularkey(keystr, strlen(keystr));
    vec_t data;
    data.set(datastr, strlen(datastr));
    W_DO(ssm->update_assoc(stid, key, data));
    return RCOK;
}

w_rc_t x_btree_overwrite_and_commit(ss_m* ssm, const StoreID &stid, const char *keystr, const char *datastr, smsize_t offset, bool use_locks)
{
    W_DO(ssm->begin_xct());
    if (use_locks) {
        xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);
    }
    rc_t rc = x_btree_overwrite(ssm, stid, keystr, datastr, offset);
    if (rc.is_error()) {
        W_DO (ssm->abort_xct());
    } else {
        W_DO(ssm->commit_xct());
    }
    return rc;
}
w_rc_t x_btree_overwrite(ss_m* ssm, const StoreID &stid, const char *keystr, const char *datastr, smsize_t offset)
{
    w_keystr_t key;
    key.construct_regularkey(keystr, strlen(keystr));
    smsize_t elen = strlen(datastr);
    W_DO(ssm->overwrite_assoc(stid, key, datastr, offset, elen));
    return RCOK;
}

// helper function to briefly check the stored contents in BTree
w_rc_t x_btree_scan(ss_m* ssm, const StoreID &stid, x_btree_scan_result &result, bool use_locks) {
    W_DO(ssm->begin_xct());
    if (use_locks) {
        xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);
    }
    // fully scan the BTree
    bt_cursor_t cursor (stid, true);

    result.rownum = 0;
    bool first_key = true;
    do {
        W_DO(cursor.next());
        if (cursor.eof()) {
            break;
        }
        if (first_key) {
            first_key = false;
            result.minkey.assign((const char*)cursor.key().serialize_as_nonkeystr().data(), cursor.key().get_length_as_nonkeystr());
        }

// TODO(Restart)...
//std::cout << "%%%%% Key: " << cursor.key().serialize_as_nonkeystr().data() << std::endl;

        result.maxkey.assign((const char*)cursor.key().serialize_as_nonkeystr().data(), cursor.key().get_length_as_nonkeystr());
        ++result.rownum;
    } while (true);
    W_DO(ssm->commit_xct());
    return RCOK;
}

bool x_in_restart(ss_m* ssm)
{
    // System is still in the process of 'restart'
    return ssm->in_restart();
}


/* Thread-API to be used by tests to simulate multiple threads as sources of transactions.
 * Usage: 1) Construct thread object providing a pointer to the function to be executed when the thread is run.
 *       This function has to be a static function with an argument of type StoreID as the only one.
 *    2) Call fork() on the thread object to run the thread
 *   [3) Call join() on the thread to wait for it to finish or use _finished to see if it has finished yet]
 */
transact_thread_t::transact_thread_t(StoreID* stid_list, void (*runfunc)(StoreID*)) : smthread_t(t_regular, "transact_thread_t"), _stid_list(stid_list), _finished(false) {
    _runnerfunc = runfunc;
    _thid = next_thid++;
}
transact_thread_t::~transact_thread_t() {}

int transact_thread_t::next_thid = 10;

void transact_thread_t::run() {
    std::cout << ":T" << _thid << " starting..";
    _runnerfunc(_stid_list);
    _finished = true;
    std::cout << ":T" << _thid << " finished." << std::endl;
}

w_rc_t btree_test_env::btree_populate_records(StoreID &stid,
                                                  bool fCheckPoint,           // Issue checkpointt in the middle of insertion
                                                  test_txn_state_t txnState,  // What to do with the transaction
                                                  bool splitIntoSmallTrans,   // Default: false
                                                                              // Ture if one insertion per transaction
                                                  char keyPrefix)             // Default: '\0'
                                                                              // Use as key prefix is not '\0'
{
    // If split into multiple transactions, caller cannot ask for in-flight transaction state
    if (t_test_txn_in_flight == txnState)
        w_assert1(false == splitIntoSmallTrans);

    // Set the data size is the max_entry_size minus key size
    // because the total size must be smaller than or equal to
    // btree_m::max_entry_size()
    bool isMulti = keyPrefix != '\0';
    const int key_size = isMulti ? 6 : 5;
    const int data_size = btree_m::max_entry_size() - key_size - 1;
    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
    int num;

    vec_t data;
    char data_str[data_size+1];
    data_str[data_size] = '\0';
    memset(data_str, 'D', data_size);
    w_keystr_t key;
    char key_str[key_size];
    key_str[0] = 'k';
    key_str[1] = 'e';
    key_str[2] = 'y';
    if (isMulti)
        key_str[3] = keyPrefix;

    // Insert enough records to ensure page split
    // Multiple transactions with one insertion per transaction

    if (!splitIntoSmallTrans)
    {
        // One big transaction

        // Using mutex to make sure the entire transaction got executed together
// TODO(Restart)... potentially there is an issue if multiple threads are calling this function
//     with one big transaction, some are in-flight, some are commit.
//     Seeing core dump from 'begin_xct', we might have issues in the transaction
//     manager implementation with concurrent and longer lasting transactions from
//     multiple threads, or maybe the usage of 'detach_xct' is not correct?
//    Using mutex seems to work around the issue, need more investigation....

        spinlock_write_critical_section cs(&_test_begin_xct_mutex);
        W_DO(begin_xct());

        for (int i = 0; i < recordCount; ++i)
        {
            num = recordCount - 1 - i;

            key_str[key_size-2] = ('0' + ((num / 10) % 10));
            key_str[key_size-1] = ('0' + (num % 10));

            if (true == fCheckPoint)
            {
                // Take one checkpoint half way through insertions
                if (num == recordCount/2)
                    W_DO(ss_m::checkpoint());
            }

            // Insert
            W_DO(btree_insert(stid, key_str, data_str));
        }

        if(t_test_txn_commit == txnState)
            W_DO(commit_xct());
        else if (t_test_txn_abort == txnState)
            W_DO(abort_xct());
        else
            ss_m::detach_xct();
    }
    else
    {
        // Multiple small transactions, one insertion per transaction
        for (int i = 0; i < recordCount; ++i)
        {
            num = recordCount - 1 - i;

            key_str[key_size-2] = ('0' + ((num / 10) % 10));
            key_str[key_size-1] = ('0' + (num % 10));

            if (true == fCheckPoint)
            {
                // Take one checkpoint half way through insertions
                if (num == recordCount/2)
                    W_DO(ss_m::checkpoint());
            }

            W_DO(begin_xct());
            W_DO(btree_insert(stid, key_str, data_str));
            if(t_test_txn_commit == txnState)
                W_DO(commit_xct());
            else if (t_test_txn_abort == txnState)
                W_DO(abort_xct());
            else
                w_assert1(false);
        }
    }

    return RCOK;
}

w_rc_t btree_test_env::delete_records(StoreID &stid,
                                        bool fCheckPoint,          // Issue checkpointt in the middle of deletion
                                        test_txn_state_t txnState, // What to do with the transaction
                                        char keyPrefix)            // Default: '\0'
                                                                   // Use as key prefix is not '\0'
{
    // One big transaction with multiple deletions
    // Currently this function does not support multiple small deletion transactions

    const bool isMulti = (keyPrefix!='\0');
    const int key_size = isMulti ? 6 : 5; // When this is used in multi-threaded and/or multi-index tests,
                                          // each thread needs to pass a different keyPrefix
                                          // to prevent duplicate records
    char key_str[key_size];
    key_str[0] = 'k';
    key_str[1] = 'e';
    key_str[2] = 'y';

    // Delete every second record, will lead to page merge

    // Use spinlock to ensure all statements in the transaction got executed together
    spinlock_write_critical_section cs(&_test_begin_xct_mutex);

    W_DO(begin_xct());
    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
    for (int i=0; i < recordCount; i+=2) {

        key_str[3] = ('0' + ((i / 10) % 10));
        key_str[4] = ('0' + (i % 10));
        if(isMulti) {
            key_str[3] = keyPrefix;
            key_str[4] = ('0' + ((i / 10) % 10));
            key_str[5] = ('0' + (i % 10));
        }
        else {
            key_str[3] = ('0' + ((i / 10) % 10));
            key_str[4] = ('0' + (i % 10));
        }
        if (true == fCheckPoint && i == recordCount/2) {
            // Take one checkpoint halfway through deletions
            W_DO(ss_m::checkpoint());
        }
        W_DO(btree_remove(stid, key_str));
    }
    if (t_test_txn_commit == txnState)
        W_DO(commit_xct());
    else if (t_test_txn_abort == txnState)
        W_DO(abort_xct());
    else
        ss_m::detach_xct();
    return RCOK;
}

