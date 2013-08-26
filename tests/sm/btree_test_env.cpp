#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

#include "w_stream.h"
#include "w.h"
#include "option.h"
#include "w_strstream.h"
#include "sm_vas.h"
#include "sm_base.h"
#include "generic_page.h"
#include "bf.h"
#include "smthread.h"
#include "page_bf_inline.h"
#include "btree.h"
#include "btcursor.h"
#include "btree_impl.h"
#include "btree_page.h"
#include "btree_test_env.h"
#include "xct.h"

#include "../nullbuf.h"
#if W_DEBUG_LEVEL <= 3
nullbuf null_obj;
std::ostream vout (&null_obj);
#endif // W_DEBUG_LEVEL
namespace {
    char device_name[MAXPATHLEN] = "./volumes/dev_test";
    char global_log_dir[MAXPATHLEN] = "./log";

}
/** thread object to host Btree test functors. */
class testdriver_thread_t : public smthread_t {
public:

        testdriver_thread_t(test_functor *functor,
            btree_test_env *env,
            int32_t locktable_size,
            int disk_quota_in_pages,
            int bufferpool_size_in_pages,
            uint32_t cleaner_threads,
            uint32_t cleaner_interval_millisec_min,
            uint32_t cleaner_interval_millisec_max,
            uint32_t cleaner_write_buffer_pages,
            bool initially_enable_cleaners,
            bool enable_swizzling,
            const std::vector<std::pair<const char*, const char*> > &additional_params
        ) 
                : smthread_t(t_regular, "testdriver_thread_t"),
                _env(env),
                _additional_params(additional_params),
                _options(NULL),
                _locktable_size (locktable_size),
                _disk_quota_in_pages(disk_quota_in_pages),
                _bufferpool_size_in_pages(bufferpool_size_in_pages),
                _cleaner_threads(cleaner_threads),
                _cleaner_interval_millisec_min(cleaner_interval_millisec_min),
                _cleaner_interval_millisec_max(cleaner_interval_millisec_max),
                _cleaner_write_buffer_pages(cleaner_write_buffer_pages),
                _initially_enable_cleaners(initially_enable_cleaners),
                _enable_swizzling(enable_swizzling),
                _retval(0),
                _functor(functor){}

        ~testdriver_thread_t()  { if(_options) delete _options; }

        void run();
        int  return_value() const { return _retval; }


private:
        w_rc_t handle_options(); // run-time options
        w_rc_t do_init(ss_m &ssm);
    
        btree_test_env *_env;
        const std::vector<std::pair<const char*, const char*> > &_additional_params;
        option_group_t* _options; // run-time options
        int32_t         _locktable_size;
        int             _disk_quota_in_pages;
        int             _bufferpool_size_in_pages;
        uint32_t        _cleaner_threads;
        uint32_t        _cleaner_interval_millisec_min;
        uint32_t        _cleaner_interval_millisec_max;
        uint32_t        _cleaner_write_buffer_pages;
        bool            _initially_enable_cleaners;
        bool            _enable_swizzling;
        int             _retval; // return value from run()
        test_functor*   _functor;// test functor object
};

w_rc_t 
testdriver_thread_t::handle_options()
{
    const int option_level_cnt = 3; 

    _options = new option_group_t (option_level_cnt);
    if(!_options) {
        cerr << "Out of memory: could not allocate from heap." <<
            endl;
        _retval = 1;
        return RC(fcINTERNAL);
    }
    option_group_t &options(*_options);
    W_DO(ss_m::setup_options(&options));


    // set the values. we don't use config files for ease of testing. simply set the values.
    w_ostrstream      err_stream;
    {
        std::stringstream str;
        int bufpool_size_in_kb = SM_PAGESIZE / 1024 * _bufferpool_size_in_pages;
        str << bufpool_size_in_kb;
        W_DO(options.set_value("sm_bufpoolsize", true, str.str().c_str(), true, &err_stream));
    }
    {
        std::stringstream str;
        str << _locktable_size;
        W_DO(options.set_value("sm_locktablesize", true, str.str().c_str(), true, &err_stream));
    }
    W_DO(options.set_value("sm_logdir", true, global_log_dir, true, &err_stream));

    {
        std::stringstream str;
        str << _cleaner_threads;
        W_DO(options.set_value("sm_num_page_writers", true, str.str().c_str(), true, &err_stream));
    }
    {
        std::stringstream str;
        str << _cleaner_interval_millisec_min;
        W_DO(options.set_value("sm_cleaner_interval_millisec_min", true, str.str().c_str(), true, &err_stream));
    }
    {
        std::stringstream str;
        str << _cleaner_interval_millisec_max;
        W_DO(options.set_value("sm_cleaner_interval_millisec_max", true, str.str().c_str(), true, &err_stream));
    }
    // TODO cleaner_write_buffer_pages isn't an option in option_t
    
    W_DO(options.set_value("sm_backgroundflush", true, _initially_enable_cleaners ? "yes" : "no", true, &err_stream));
    W_DO(options.set_value("sm_bufferpool_swizzle", true, _enable_swizzling ? "yes" : "no", true, &err_stream));

    // set additional values given by each test case
    for (std::vector<std::pair<const char*, const char*> >::const_iterator iter = _additional_params.begin();
            iter != _additional_params.end(); ++iter) {
        cout << "additional parameter: " << iter->first << "=" << iter->second << endl;
        W_DO(options.set_value(iter->first, true, iter->second, true, &err_stream));
    }

    // check required options for values
    w_rc_t rc = options.check_required(&err_stream);
    if (rc.is_error()) {
        cerr << "These required options are not set:" << endl;
        cerr << err_stream.c_str() << endl;
        return rc;
    }

    return RCOK;
}

rc_t
testdriver_thread_t::do_init(ss_m &ssm)
{
    const int quota_in_kb = SM_PAGESIZE / 1024 * _disk_quota_in_pages;
    if (_functor->_need_init) {
        _functor->_test_volume._device_name = device_name;
        _functor->_test_volume._vid = 1;

        vout << "Formatting device: " << _functor->_test_volume._device_name 
                << " with a " << quota_in_kb << "KB quota ..." << endl;
        W_DO(ssm.format_dev(_functor->_test_volume._device_name, quota_in_kb, true));
    } else {
        w_assert0(_functor->_test_volume._device_name);
    }    

    devid_t        devid;
    vout << "Mounting device: " << _functor->_test_volume._device_name  << endl;
    // mount the new device
    u_int        vol_cnt;
    W_DO(ssm.mount_dev(_functor->_test_volume._device_name, vol_cnt, devid));

    vout << "Mounted device: " << _functor->_test_volume._device_name  
            << " volume count " << vol_cnt
            << " device " << devid
            << endl;

    if (_functor->_need_init) {
        // generate a volume ID for the new volume we are about to
        // create on the device
        vout << "Generating new lvid: " << endl;
        W_DO(ssm.generate_new_lvid(_functor->_test_volume._lvid));
        vout << "Generated lvid " << _functor->_test_volume._lvid <<  endl;

        // create the new volume 
        vout << "Creating a new volume on the device" << endl;
        vout << "    with a " << quota_in_kb << "KB quota ..." << endl;

        W_DO(ssm.create_vol(_functor->_test_volume._device_name, _functor->_test_volume._lvid,
                        quota_in_kb, false, _functor->_test_volume._vid));
        vout << "    with local handle(phys volid) " << _functor->_test_volume._vid << endl;
    } else {
        w_assert0(_functor->_test_volume._vid != vid_t::null);
        w_assert0(_functor->_test_volume._lvid != lvid_t::null);
    }

    return RCOK;
}

void testdriver_thread_t::run()
{
    /* deal with run-time options */
    w_rc_t rc = handle_options();
    if(rc.is_error()) {
        _retval = 1;
        return;
    }

    // Now start a storage manager.
    vout << "Starting SSM and performing recovery ..." << endl;
    {
        ss_m ssm;
        _env->_ssm = &ssm;

        sm_config_info_t config_info;
        rc = ss_m::config_info(config_info);
        if(rc.is_error()) {
            cerr << "Could not get storage manager configuration info: " << rc << endl; 
            _retval = 1;
            return;
        }
        rc = do_init(ssm);
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
}
void btree_test_env::SetUp()
{
    char tests_dir[MAXPATHLEN] = "/dev/shm/";
    strcat(tests_dir, getenv("USER"));
    assure_dir(tests_dir);
    strcat(tests_dir, "/btree_test_env");
    assure_dir(tests_dir);
    strcpy(log_dir, tests_dir);
    strcpy(vol_dir, tests_dir);
    strcat(log_dir, "/log");
    strcat(vol_dir, "/volumes");
    assure_empty_dir(log_dir);
    assure_empty_dir(vol_dir);
    strcpy(device_name, vol_dir);
    strcat(device_name, "/dev_test");
    strcpy(global_log_dir, log_dir);
    _use_locks = false;
}
void btree_test_env::empty_logdata_dir()
{
    empty_dir(log_dir);
    empty_dir(vol_dir);
}

void btree_test_env::TearDown()
{
    
}

int btree_test_env::runBtreeTest (w_rc_t (*functor)(ss_m*, test_volume_t*),
                    bool use_locks, int32_t lock_table_size,
                    int disk_quota_in_pages,
                    int bufferpool_size_in_pages,
                    uint32_t cleaner_threads,
                    uint32_t cleaner_interval_millisec_min,
                    uint32_t cleaner_interval_millisec_max,
                    uint32_t cleaner_write_buffer_pages,
                    bool initially_enable_cleaners,
                    bool enable_swizzling
                                 )
{
    std::vector<std::pair<const char*, const char*> > dummy;
    return runBtreeTest(functor, use_locks, lock_table_size,
            disk_quota_in_pages, bufferpool_size_in_pages,
            cleaner_threads,
            cleaner_interval_millisec_min,
            cleaner_interval_millisec_max,
            cleaner_write_buffer_pages,
            initially_enable_cleaners,
            enable_swizzling,
            dummy);
}

int btree_test_env::runBtreeTest (w_rc_t (*func)(ss_m*, test_volume_t*),
        bool use_locks, int32_t lock_table_size,
        int disk_quota_in_pages,
        int bufferpool_size_in_pages,
        uint32_t cleaner_threads,
        uint32_t cleaner_interval_millisec_min,
        uint32_t cleaner_interval_millisec_max,
        uint32_t cleaner_write_buffer_pages,
        bool initially_enable_cleaners,
        bool enable_swizzling,
        const std::vector<std::pair<const char*, const char*> > &additional_params
)
{
    _use_locks = use_locks;
    int rv;
    {
        default_test_functor functor(func);
        testdriver_thread_t smtu(
            &functor, this, lock_table_size,
            disk_quota_in_pages, bufferpool_size_in_pages,
            cleaner_threads,
            cleaner_interval_millisec_min,
            cleaner_interval_millisec_max,
            cleaner_write_buffer_pages,
            initially_enable_cleaners,
            enable_swizzling,
            additional_params);

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

int btree_test_env::runCrashTest (crash_test_base *context,
    bool use_locks, int32_t lock_table_size,
    int disk_quota_in_pages, int bufferpool_size_in_pages,
    uint32_t cleaner_threads,
    uint32_t cleaner_interval_millisec_min,
    uint32_t cleaner_interval_millisec_max,
    uint32_t cleaner_write_buffer_pages,
    bool initially_enable_cleaners,
    bool enable_swizzling) {
    std::vector<std::pair<const char*, const char*> > dummy;
    return runCrashTest(context, use_locks, lock_table_size,
            disk_quota_in_pages, bufferpool_size_in_pages,
            cleaner_threads,
            cleaner_interval_millisec_min,
            cleaner_interval_millisec_max,
            cleaner_write_buffer_pages,
            initially_enable_cleaners,
            enable_swizzling,
            dummy);
}

int btree_test_env::runCrashTest (crash_test_base *context,
    bool use_locks, int32_t lock_table_size,
    int disk_quota_in_pages, int bufferpool_size_in_pages,
    uint32_t cleaner_threads,
    uint32_t cleaner_interval_millisec_min,
    uint32_t cleaner_interval_millisec_max,
    uint32_t cleaner_write_buffer_pages,
    bool initially_enable_cleaners,
    bool enable_swizzling,
    const std::vector<std::pair<const char*, const char*> > &additional_params) {
        
    _use_locks = use_locks;

    DBGOUT2 ( << "Going to call pre_crash()...");
    int rv;
    {
        crash_test_pre_functor functor(context);
        testdriver_thread_t smtu(
            &functor, this, lock_table_size,
            disk_quota_in_pages, bufferpool_size_in_pages,
            cleaner_threads,
            cleaner_interval_millisec_min,
            cleaner_interval_millisec_max,
            cleaner_write_buffer_pages,
            initially_enable_cleaners,
            enable_swizzling,
            additional_params);

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
        testdriver_thread_t smtu(
            &functor, this, lock_table_size,
            disk_quota_in_pages, bufferpool_size_in_pages,
            cleaner_threads,
            cleaner_interval_millisec_min,
            cleaner_interval_millisec_max,
            cleaner_write_buffer_pages,
            initially_enable_cleaners,
            enable_swizzling,
            additional_params);

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


void btree_test_env::set_xct_query_lock() {
    if (_use_locks) {
        xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);
    } else {
        xct()->set_query_concurrency(smlevel_0::t_cc_none);
    }
}

w_rc_t x_btree_create_index(ss_m* ssm, test_volume_t *test_volume, stid_t &stid, lpid_t &root_pid)
{
    W_DO(ssm->begin_xct());
    W_DO(ssm->create_index(test_volume->_vid, stid));
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

w_rc_t x_btree_get_root_pid(ss_m* ssm, const stid_t &stid, lpid_t &root_pid)
{
    W_DO(ssm->open_store_nolock(stid, root_pid));
    return RCOK;
}
w_rc_t x_btree_adopt_foster_all(ss_m* ssm, const stid_t &stid)
{
    lpid_t root_pid;
    W_DO (x_btree_get_root_pid (ssm, stid, root_pid));
    W_DO(ssm->begin_xct());
    {
        btree_page_h root_p;
        W_DO(root_p.fix_root(stid.vol.vol, stid.store, LATCH_EX));
        W_DO(btree_impl::_sx_adopt_foster_all(root_p, true));
    }
    W_DO(ssm->commit_xct());
    return RCOK;
}
w_rc_t x_btree_verify(ss_m* ssm, const stid_t &stid) {
    W_DO(ssm->begin_xct());
    bool consistent;
    W_DO(ssm->verify_index(stid, 19, consistent));
    EXPECT_TRUE (consistent) << "BTree verification of index " << stid << " failed";
    W_DO(ssm->commit_xct());
    if (!consistent) {
        return RC(smlevel_0::eBADARGUMENT); // or some other thing
    }
    return RCOK;
}

// helper function for insert. keystr/datastr have to be NULL terminated
w_rc_t x_btree_lookup_and_commit(ss_m* ssm, const stid_t &stid, const char *keystr, std::string &data, bool use_locks) {
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
w_rc_t x_btree_lookup(ss_m* ssm, const stid_t &stid, const char *keystr, std::string &data) {
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

w_rc_t x_btree_insert(ss_m* ssm, const stid_t &stid, const char *keystr, const char *datastr) {
    w_keystr_t key;
    key.construct_regularkey(keystr, strlen(keystr));
    vec_t data;
    data.set(datastr, strlen(datastr));
    W_DO(ssm->create_assoc(stid, key, data));    
    return RCOK;
}
w_rc_t x_btree_insert_and_commit(ss_m* ssm, const stid_t &stid, const char *keystr, const char *datastr, bool use_locks) {
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
w_rc_t x_btree_remove(ss_m* ssm, const stid_t &stid, const char *keystr) {
    w_keystr_t key;
    key.construct_regularkey(keystr, strlen(keystr));
    W_DO(ssm->destroy_assoc(stid, key));
    return RCOK;
}
w_rc_t x_btree_remove_and_commit(ss_m* ssm, const stid_t &stid, const char *keystr, bool use_locks) {
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

w_rc_t x_btree_update_and_commit(ss_m* ssm, const stid_t &stid, const char *keystr, const char *datastr, bool use_locks)
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
w_rc_t x_btree_update(ss_m* ssm, const stid_t &stid, const char *keystr, const char *datastr)
{
    w_keystr_t key;
    key.construct_regularkey(keystr, strlen(keystr));
    vec_t data;
    data.set(datastr, strlen(datastr));
    W_DO(ssm->update_assoc(stid, key, data));    
    return RCOK;
}

w_rc_t x_btree_overwrite_and_commit(ss_m* ssm, const stid_t &stid, const char *keystr, const char *datastr, smsize_t offset, bool use_locks)
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
w_rc_t x_btree_overwrite(ss_m* ssm, const stid_t &stid, const char *keystr, const char *datastr, smsize_t offset)
{
    w_keystr_t key;
    key.construct_regularkey(keystr, strlen(keystr));
    smsize_t elen = strlen(datastr);
    W_DO(ssm->overwrite_assoc(stid, key, datastr, offset, elen));
    return RCOK;
}

// helper function to briefly check the stored contents in BTree
w_rc_t x_btree_scan(ss_m* ssm, const stid_t &stid, x_btree_scan_result &result, bool use_locks) {
    W_DO(ssm->begin_xct());
    if (use_locks) {
        xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);
    }
    // fully scan the BTree
    bt_cursor_t cursor (stid.vol.vol, stid.store, true);
    
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
        result.maxkey.assign((const char*)cursor.key().serialize_as_nonkeystr().data(), cursor.key().get_length_as_nonkeystr());
        ++result.rownum;
    } while (true);
    W_DO(ssm->commit_xct());
    return RCOK;
}
