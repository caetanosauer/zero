/** Example to create a new volume and an index in it. */
#include "sm_vas.h"
#include "sm.h"
#include <sstream>

const int QUOTA_IN_KB = (1 << 16); // 64 MB
const int OUR_VOLUME_ID = 1;

#ifndef ORIGINAL_SHOREMT
const uint FIRST_STORE_ID = 1;
#else// ORIGINAL_SHOREMT
const uint FIRST_STORE_ID = 3;
#endif// ORIGINAL_SHOREMT

class example_thread_t : public smthread_t {
public:
    example_thread_t(const char *log_folder_arg, const char *data_device_arg, bool nuke_it_arg)
        : log_folder(log_folder_arg), data_device(data_device_arg), options (3), vid(OUR_VOLUME_ID), nuke_it(nuke_it_arg) {}
    ~example_thread_t() {}
    void run();
    int  return_value() const { return retval; }
    
    int fire_example();

protected:
    rc_t set_options();
    rc_t do_init();
    void empty_dir(const char *folder_name);
    rc_t run_example();

    const char *log_folder;
    const char *data_device;
    option_group_t options;
    lvid_t lvid;
    vid_t vid;
    stid_t stid;
    int retval;
    
    bool nuke_it;
};

rc_t example_thread_t::set_options()
{
    W_DO(ss_m::setup_options(&options));
    w_ostrstream      err_stream;
    W_DO(options.set_value("sm_bufpoolsize", true, "16384", true, &err_stream)); // 16 MB
    W_DO(options.set_value("sm_logbufsize", true, "4096", true, &err_stream)); // 4 MB
    W_DO(options.set_value("sm_logsize", true, "65536", true, &err_stream)); // 64 MB
    W_DO(options.set_value("sm_logdir", true, log_folder, true, &err_stream));

    // check required options for values
    w_rc_t rc = options.check_required(&err_stream);
    if (rc.is_error()) {
        std::cerr << "These required options are not set:" << std::endl;
        std::cerr << err_stream.c_str() << std::endl;
        return rc;
    }
    return RCOK;
}
void example_thread_t::empty_dir(const char *folder_name)
{
    // want to use boost::filesystem... but let's keep this project boost-free.
    DIR *d = ::opendir(folder_name);
    w_assert0(d != NULL);
    for (struct dirent *ent = ::readdir(d); ent != NULL; ent = ::readdir(d)) {
        // Skip "." and ".."
        if (!::strcmp(ent->d_name, ".") || !::strcmp(ent->d_name, "..")) {
            continue;
        }
        std::stringstream buf;
        buf << folder_name << "/" << ent->d_name;
        std::remove (buf.str().c_str());
    }
    ::closedir (d);
}
rc_t example_thread_t::do_init()
{
    sm_config_info_t config_info;
    W_DO(ss_m::config_info(config_info));
    
    devid_t        devid;
    u_int        vol_cnt;
    if (nuke_it) {
        W_DO(ss_m::format_dev(data_device, QUOTA_IN_KB, true));
        W_DO(ss_m::mount_dev(data_device, vol_cnt, devid));
        W_DO(ss_m::generate_new_lvid(lvid));
        W_DO(ss_m::create_vol(data_device, lvid, QUOTA_IN_KB, false, vid));
    } else {
        W_DO(ss_m::mount_dev(data_device, vol_cnt, devid, vid));
    }

    return RCOK;
}

void example_thread_t::run()
{
    if (nuke_it) {
        std::cout << "deleting all existing files.. " << std::endl; 
        std::remove (data_device);
        empty_dir (log_folder);
    }

    w_rc_t rc_opt = set_options();
    if (rc_opt.is_error()) {
        std::cerr << "set_options failed: " << rc_opt << std::endl; 
        retval = 1;
        return;
    }
    {
        ss_m ssm; // do this AFTER options are set. this creates all static information
        
        w_rc_t rc = do_init();
        if (rc.is_error()) {
            std::cerr << "Init failed: " << rc << std::endl; 
            retval = 1;
            return;
        }
        
        rc = run_example();
        if (rc.is_error()) {
            std::cerr << "run_derived() failed: " << rc << std::endl; 
            retval = 1;
            return;
        }
        std::cout << "shutting down ssm..." << std::endl;
    }
    retval = 0;
}

int example_thread_t::fire_example() {
    w_rc_t e = fork();
    if(e.is_error()) {
        std::cerr << "Error forking thread: " << e <<std::endl;
        return 1;
    }

    e = join();
    if(e.is_error()) {
        std::cerr << "Error joining thread: " << e <<std::endl;
        return 1;
    }
    std::cout << "quiting" << std::endl;
    int rv = return_value();

    return rv;
}

rc_t example_thread_t::run_example() {
    std::cout << "Hello World" << std::endl;

    W_DO(ss_m::begin_xct());
    W_DO (ss_m::create_index(vid, stid));
    W_DO(ss_m::commit_xct());
    std::cout << "Created a new index. StoreID=" << stid << std::endl;

    W_DO(ss_m::begin_xct());
    w_keystr_t key;
    key.construct_regularkey("keystr1", 7);
    vec_t data ("data", 4);
    W_DO(ss_m::create_assoc(stid, key, data));
    W_DO(ss_m::commit_xct());

    std::cout << "Inserted something. now execute example2!" << std::endl;
    return RCOK;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        std::cout << "parameters: <log_folder> <data_device>" << std::endl;
        std::cout << "example param: /media/SSDVolume /home/hkimura/data_file" << std::endl;
        return 1;
    }
    example_thread_t t(argv[1], argv[2], true);
    return t.fire_example();
}

