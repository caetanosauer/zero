#ifndef TESTS_BTREE_TEST_ENV_H
#define TESTS_BTREE_TEST_ENV_H

#include <vector>
#include <utility>
#include <string>
#include <sys/param.h>

#include "w_defines.h"
#include "w_base.h"
#include "sm_vas.h"
#include "gtest/gtest.h"

#if W_DEBUG_LEVEL > 3
#define vout std::cout
#else // W_DEBUG_LEVEL
extern std::ostream vout;
#endif // W_DEBUG_LEVEL

class ss_m;

/**
 * Details of the test data volume.
 */
struct test_volume_t {
    /** persistent volume id to give the volume. */ 
    lvid_t      _lvid;   
    /** short (integer) volume id. */
    vid_t       _vid;
    /** path of the devise. */
    const char* _device_name;
};

const int default_quota_in_pages = 64;
const int default_bufferpool_size_in_pages = 64;
const int default_locktable_size = 1 << 6;

#ifdef DEFAULT_SWIZZLING_OFF
const bool default_enable_swizzling = false;
#else // DEFAULT_SWIZZLING_OFF
const bool default_enable_swizzling = true;
#endif //DEFAULT_SWIZZLING_OFF

// a few convenient functions for testcases
w_rc_t x_begin_xct(ss_m* ssm, bool use_locks);
w_rc_t x_commit_xct(ss_m* ssm);
w_rc_t x_abort_xct(ss_m* ssm);
w_rc_t x_btree_create_index(ss_m* ssm, test_volume_t *test_volume, stid_t &stid, lpid_t &root_pid);
w_rc_t x_btree_get_root_pid(ss_m* ssm, const stid_t &stid, lpid_t &root_pid);
w_rc_t x_btree_adopt_foster_all(ss_m* ssm, const stid_t &stid);
w_rc_t x_btree_verify(ss_m* ssm, const stid_t &stid);
w_rc_t x_btree_lookup_and_commit(ss_m* ssm, const stid_t &stid, const char *keystr, std::string &data, bool use_locks = false);
w_rc_t x_btree_lookup(ss_m* ssm, const stid_t &stid, const char *keystr, std::string &data);
w_rc_t x_btree_insert_and_commit(ss_m* ssm, const stid_t &stid, const char *keystr, const char *datastr, bool use_locks = false);
w_rc_t x_btree_insert(ss_m* ssm, const stid_t &stid, const char *keystr, const char *datastr);
w_rc_t x_btree_remove_and_commit(ss_m* ssm, const stid_t &stid, const char *keystr, bool use_locks = false);
w_rc_t x_btree_remove(ss_m* ssm, const stid_t &stid, const char *keystr);
w_rc_t x_btree_update_and_commit(ss_m* ssm, const stid_t &stid, const char *keystr, const char *datastr, bool use_locks = false);
w_rc_t x_btree_update(ss_m* ssm, const stid_t &stid, const char *keystr, const char *datastr);
w_rc_t x_btree_overwrite_and_commit(ss_m* ssm, const stid_t &stid, const char *keystr, const char *datastr, smsize_t offset, bool use_locks = false);
w_rc_t x_btree_overwrite(ss_m* ssm, const stid_t &stid, const char *keystr, const char *datastr, smsize_t offset);
bool   x_in_recovery(ss_m* ssm);


/** Delete backup if exists. */
void x_delete_backup(ss_m* ssm, test_volume_t *test_volume);
/** Take a backup of the test volume. */
w_rc_t x_take_backup(ss_m* ssm, test_volume_t *test_volume);

struct x_btree_scan_result {
    int rownum;
    // don't use code point >127 for this test!
    std::string minkey;
    std::string maxkey;
};
w_rc_t x_btree_scan(ss_m* ssm, const stid_t &stid, x_btree_scan_result &result, bool use_locks = false);

class test_functor {
public:
    test_functor() {}
    virtual ~test_functor() {}
    virtual w_rc_t run_test(ss_m *ssm) = 0;

    bool _need_init;
    bool _clean_shutdown; // false only in pre_crash
    test_volume_t _test_volume; // if _need_init, this will be overwritten. otherwise reused.
};

/**
 * For most testcases.
 */
class default_test_functor : public test_functor {
public:
    default_test_functor(w_rc_t (*functor)(ss_m*, test_volume_t*)) {
        _functor = functor;
        _need_init = true;
        _clean_shutdown = true;
    }
    w_rc_t run_test(ss_m *ssm) {
        return _functor (ssm, &_test_volume);
    }
    rc_t (*_functor)(ss_m*, test_volume_t*);
};

// Begin... for test_restart.cpp and test_concurrent_restart.cpp
// The base class for all restart test cases.
// Derived classes must implement two functions; pre_shutdown() and post_shutdown().
// These are called before and after a normal or (simulated) crash shutdown
// @See btree_test_env::runRestartTest()
class restart_test_base {
public:
    restart_test_base() {}
    virtual ~restart_test_base() {}

    virtual w_rc_t pre_shutdown(ss_m *ssm) = 0;

    virtual w_rc_t post_shutdown(ss_m *ssm) = 0;

    test_volume_t _volume;

    stid_t _stid;
    lpid_t _root_pid;
};

class restart_dirty_test_pre_functor : public test_functor {
public:
    restart_dirty_test_pre_functor(restart_test_base *context) {
        _context = context;
        _need_init = true;
        _clean_shutdown = false; // to simulate a crash
    }
    w_rc_t run_test(ss_m *ssm) {
        _context->_volume = _test_volume; // remember the volume for use in post_shutdown
        return _context->pre_shutdown (ssm);  // crash
    }
    restart_test_base *_context;
};
class restart_clean_test_pre_functor : public test_functor {
public:
    restart_clean_test_pre_functor(restart_test_base *context) {
        _context = context;
        _need_init = true;
        _clean_shutdown = true;
    }
    w_rc_t run_test(ss_m *ssm) {
        _context->_volume = _test_volume; // remember the volume for use in post_shutdown
        return _context->pre_shutdown (ssm);   // no crash
    }
    restart_test_base *_context;
};
class restart_test_post_functor : public test_functor {
public:
    restart_test_post_functor(restart_test_base *context) {
        _context = context;
        _need_init = false; // we inherit the data we made in pre_shutdown()
        _clean_shutdown = true;
        _test_volume = context->_volume; //pre-set the volume we already made
    }
    w_rc_t run_test(ss_m *ssm) {
        return _context->post_shutdown (ssm);
    }
    restart_test_base *_context;
};
// End... for test_restart.cpp and and test_concurrent_restart.cpp

/**
 * The base class for all crash test cases.
 * Derived classes must implement two functions; pre_crash() and post_crash().
 * These are called before and after a (simulated) crash.
 * @See btree_test_env::runCrashTest()
 */
class crash_test_base {
public:
    crash_test_base() {}
    virtual ~crash_test_base() {}

    /**
     * This function is called before the simulated crash.
     * This function is supposed to setup the situation we want to test.
     * For many crash tests, we want to disable automatic background
     * write-outs in bufferpool, so we disable it before we call this function.
     */
    virtual w_rc_t pre_crash(ss_m *ssm) = 0;

    /**
     * This function is called after the simulated crash.
     * This function is supposed to check if the recovery process did
     * a correct job.
     */
    virtual w_rc_t post_crash(ss_m *ssm) = 0;

// objects that can be used in pre_crash() and post_crash()
    test_volume_t _volume;
    
    // these two are used in many crash testcases, so defined here although some test might not use them.
    stid_t _stid;
    lpid_t _root_pid;
};

/** for crash test cases. */
class crash_test_pre_functor : public test_functor {
public:
    crash_test_pre_functor(crash_test_base *context) {
        _context = context;
        _need_init = true;
        _clean_shutdown = false; // to simulate a crash
    }
    w_rc_t run_test(ss_m *ssm) {
        _context->_volume = _test_volume; // remember the volume for use in post_crash
        return _context->pre_crash (ssm);
    }
    crash_test_base *_context;
};
class crash_test_post_functor : public test_functor {
public:
    crash_test_post_functor(crash_test_base *context) {
        _context = context;
        _need_init = false; // we inherit the data we made in pre_crash()
        _clean_shutdown = true;
        _test_volume = context->_volume; //pre-set the volume we already made
    }
    w_rc_t run_test(ss_m *ssm) {
        return _context->post_crash (ssm);
    }
    crash_test_base *_context;
};

/**
 * Sets up log and volume for BTree testcases.
 * Register this environment to gtest in your main() function.
 * @See http://code.google.com/p/googletest/wiki/V1_6_AdvancedGuide#Global_Set-Up_and_Tear-Down
 */
class btree_test_env : public ::testing::Environment {
public:
    btree_test_env();
    ~btree_test_env();
    void SetUp();
    void TearDown();

    static sm_options make_sm_options(
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
        const std::vector<std::pair<const char*, const char*> > &additional_string_params);

    static sm_options make_sm_options(
        int32_t locktable_size,
        int bufferpool_size_in_pages,
        uint32_t cleaner_threads,
        uint32_t cleaner_interval_millisec_min,
        uint32_t cleaner_interval_millisec_max,
        uint32_t cleaner_write_buffer_pages,
        bool initially_enable_cleaners,
        bool enable_swizzling);

    /**
     * Call this method to run your test on a storage-manager-thread (smthread).
     * This method creates a volume and log directory ss_m for each test.
     * @param functor pointer to the test function.
     * @return exit code of the thread. 0 if succeeded.
     */
    int runBtreeTest (w_rc_t (*functor)(ss_m*, test_volume_t*),
                      bool use_locks = false,
                      int32_t lock_table_size = default_locktable_size,
                      int disk_quota_in_pages = default_quota_in_pages,
                      int bufferpool_size_in_pages = default_bufferpool_size_in_pages,
                      uint32_t cleaner_threads = 1,
                      uint32_t cleaner_interval_millisec_min       =   1000,
                      uint32_t cleaner_interval_millisec_max       = 256000,
                      uint32_t cleaner_write_buffer_pages          =     64,
                      bool initially_enable_cleaners = true,
                      bool enable_swizzling = default_enable_swizzling
                     );

    /** This is most concise. New code should use this one. */
    int runBtreeTest (w_rc_t (*functor)(ss_m*, test_volume_t*), bool use_locks, int disk_quota_in_pages, const sm_options &options);

    /** Overload for convenience. */
    int runBtreeTest (w_rc_t (*functor)(ss_m*, test_volume_t*), const sm_options &options) {
        return runBtreeTest(functor, false, default_quota_in_pages, options);
    }

    /**
     * Overload to set additional parameters.
     * @param use_locks whether to use locks
     * @param lock_table_size from 2^6 to 2^23. default 2^6 in testcases.
     * @param disk_quota_in_pages size of volume. set larger values for testcases that insert many.
     * @param bufferpool_size_in_pages size of bufferpool.
     * @param additional_params (optional) can set additional parameters for ss_m.
     * See sm.h for list of configurable parameters.
     * @see runBtreeTest(w_rc_t (*functor)(ss_m*, test_volume_t*))
     */
    int runBtreeTest (w_rc_t (*functor)(ss_m*, test_volume_t*),
                      bool use_locks, int32_t lock_table_size,
                      int disk_quota_in_pages, int bufferpool_size_in_pages,
                      uint32_t cleaner_threads,
                      uint32_t cleaner_interval_millisec_min,
                      uint32_t cleaner_interval_millisec_max,
                      uint32_t cleaner_write_buffer_pages,
                      bool initially_enable_cleaners,
                      bool enable_swizzling,
                      const std::vector<std::pair<const char*, int64_t> > &additional_int_params,
                      const std::vector<std::pair<const char*, bool> > &additional_bool_params,
                      const std::vector<std::pair<const char*, const char*> > &additional_string_params);

    /**
    * Runs a restart testcase in various recovery modes
    * Caller specify the recovery mode through input parameter 'recovery_mode'
    * @param context the object to implement pre_shutdiwn(), post_shutdown().
    * @see restart_test_base
    */
    int runRestartTest (restart_test_base *context,
                      bool fCrash,
                      int32_t recovery_mode,                      
                      bool use_locks = false,
                      int32_t lock_table_size = default_locktable_size,
                      int disk_quota_in_pages = default_quota_in_pages,
                      int bufferpool_size_in_pages = default_bufferpool_size_in_pages,
                      uint32_t cleaner_threads = 1,
                      uint32_t cleaner_interval_millisec_min	   = 1000,
                      uint32_t cleaner_interval_millisec_max	   = 256000,
                      uint32_t cleaner_write_buffer_pages          = 64,
                      bool initially_enable_cleaners = true,
                      bool enable_swizzling = default_enable_swizzling
                      );

    /** This is most concise. New code should use this one. */
    int runRestartTest (restart_test_base *context, bool fCrash, int32_t recovery_mode,
                          bool use_locks, int disk_quota_in_pages, const sm_options &options);


    int runRestartTest (restart_test_base *context,
                      bool fCrash,
                      int32_t recovery_mode,                      
                      bool use_locks, int32_t lock_table_size,
                      int disk_quota_in_pages, int bufferpool_size_in_pages,
                      uint32_t cleaner_threads,
                      uint32_t cleaner_interval_millisec_min,
                      uint32_t cleaner_interval_millisec_max,
                      uint32_t cleaner_write_buffer_pages,
                      bool initially_enable_cleaners,
                      bool enable_swizzling,
                      const std::vector<std::pair<const char*, int64_t> > &additional_int_params,
                      const std::vector<std::pair<const char*, bool> > &additional_bool_params,
                      const std::vector<std::pair<const char*, const char*> > &additional_string_params);

    /**
     * Runs a crash testcase in serial traditional recovery mode
     * @param context the object to implement pre_crash(), post_crash().
     * @see crash_test_base
     */
    int runCrashTest (crash_test_base *context,
                      bool use_locks = false,
                      int32_t lock_table_size = default_locktable_size,
                      int disk_quota_in_pages = default_quota_in_pages,
                      int bufferpool_size_in_pages = default_bufferpool_size_in_pages,
                      uint32_t cleaner_threads = 1,
                      uint32_t cleaner_interval_millisec_min       =   1000,
                      uint32_t cleaner_interval_millisec_max       = 256000,
                      uint32_t cleaner_write_buffer_pages          =     64,
                      bool initially_enable_cleaners = true,
                      bool enable_swizzling = default_enable_swizzling
                     );
    
    /**
     * Overload for additional params.
     */
    int runCrashTest (crash_test_base *context,
                      bool use_locks, int32_t lock_table_size,
                      int disk_quota_in_pages, int bufferpool_size_in_pages,
                      uint32_t cleaner_threads,
                      uint32_t cleaner_interval_millisec_min,
                      uint32_t cleaner_interval_millisec_max,
                      uint32_t cleaner_write_buffer_pages,
                      bool initially_enable_cleaners,
                      bool enable_swizzling,
                      const std::vector<std::pair<const char*, int64_t> > &additional_int_params,
                      const std::vector<std::pair<const char*, bool> > &additional_bool_params,
                      const std::vector<std::pair<const char*, const char*> > &additional_string_params);

    /** This is most concise. New code should use this one. */
    int runCrashTest (crash_test_base *context, bool use_locks, int disk_quota_in_pages, const sm_options &options);

    void empty_logdata_dir();
    
    bool get_use_locks () const { return _use_locks;}
    void set_use_locks (bool value) { _use_locks = value;}
    
    // set query concurrency according to _use_locks
    void set_xct_query_lock();

public:
    // some helper functions
    w_rc_t begin_xct() {
        return x_begin_xct(_ssm, _use_locks);
    }
    w_rc_t commit_xct() {
        return x_commit_xct(_ssm);
    }
    w_rc_t abort_xct() {
        return x_abort_xct(_ssm);
    }
    w_rc_t btree_lookup_and_commit(const stid_t &stid, const char *keystr, std::string &data) {
        return x_btree_lookup_and_commit(_ssm, stid, keystr, data, _use_locks);
    }
    w_rc_t btree_lookup(const stid_t &stid, const char *keystr, std::string &data) {
        return x_btree_lookup(_ssm, stid, keystr, data);
    }
    w_rc_t btree_insert_and_commit(const stid_t &stid, const char *keystr, const char *datastr) {
        return x_btree_insert_and_commit(_ssm, stid, keystr, datastr, _use_locks);
    }
    w_rc_t btree_insert(const stid_t &stid, const char *keystr, const char *datastr) {
        return x_btree_insert(_ssm, stid, keystr, datastr);
    }
    w_rc_t btree_remove_and_commit(const stid_t &stid, const char *keystr) {
        return x_btree_remove_and_commit(_ssm, stid, keystr, _use_locks);
    }
    w_rc_t btree_remove(const stid_t &stid, const char *keystr) {
        return x_btree_remove(_ssm, stid, keystr);
    }
    w_rc_t btree_update_and_commit(const stid_t &stid, const char *keystr, const char *datastr) {
        return x_btree_update_and_commit(_ssm, stid, keystr, datastr, _use_locks);
    }
    w_rc_t btree_update(const stid_t &stid, const char *keystr, const char *datastr) {
        return x_btree_update(_ssm, stid, keystr, datastr);
    }
    w_rc_t btree_overwrite_and_commit(const stid_t &stid, const char *keystr, const char *datastr, smsize_t offset) {
        return x_btree_overwrite_and_commit(_ssm, stid, keystr, datastr, offset, _use_locks);
    }
    w_rc_t btree_overwrite(const stid_t &stid, const char *keystr, const char *datastr, smsize_t offset) {
        return x_btree_overwrite(_ssm, stid, keystr, datastr, offset);
    }
    w_rc_t btree_scan(const stid_t &stid, x_btree_scan_result &result) {
        return x_btree_scan(_ssm, stid, result, _use_locks);
    }
    bool in_recovery(){
        return x_in_recovery(_ssm);
    }
    
    ss_m* _ssm;
    bool _use_locks;
    char log_dir[MAXPATHLEN];
    char vol_dir[MAXPATHLEN];
private:
    void assure_dir(const char *folder_name);
    void assure_empty_dir(const char *folder_name);
    void empty_dir(const char *folder_name);

};

w_rc_t populate_records(stid_t &stid, bool fCheckPoint, bool fInflight);

class transact_thread_t : public smthread_t {
public:
    transact_thread_t(stid_t stid, void (*runfunct)(stid_t));
    ~transact_thread_t();

    virtual void run();
    static int next_thid; // Adopted from test_deadlock, assuming there is a reason
    stid_t _stid;
    int _thid;
    void (*_runnerfunc)(stid_t);
    bool _finished;
};

#endif // TESTS_BTREE_TEST_ENV_H
