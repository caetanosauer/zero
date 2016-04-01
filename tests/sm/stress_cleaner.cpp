#include <sstream>
#include "sm.h"
#include "stopwatch.h"
#include "sm_options.h"
#include "log_core.h"
#include "vol.h"
#include "alloc_cache.h"
#include "stnode_page.h"
#include "bf_tree.h"
#include "bf_tree_cleaner.h"
#include <chrono>
#include <thread>

#include <boost/program_options.hpp>
namespace po = boost::program_options;

using namespace std;

po::options_description options_desc;
po::variables_map options;
sm_options sm_opt;

vol_t* vol;
bf_tree_m* bf;

size_t bufsize;
string dbfile;
string logdir;
size_t dirty_rate;
size_t duration;
PageID last_pid;

lsn_t global_lsn = lsn_t(1, 0);

void setup_options()
{
    options_desc.add_options()
    ("bufsize,b", po::value<size_t>(&bufsize)->default_value(100),
        "Size of buffer pool in MB")
    ("dbfile,f", po::value<string>(&dbfile)->default_value("db"),
        "Path to database file")
    ("duration,d", po::value<size_t>(&duration)->default_value(60),
        "Duration for which to run experiment (in seconds)")
    ("dirty-rate,r", po::value<size_t>(&dirty_rate)->default_value(1000),
        "How many times a second a page is updated (marked dirty)")
    ("logdir,l", po::value<string>(&logdir)->default_value("/dev/shm/log"),
        "Log directory")
    ;
}

class page_dirtier_thread : public smthread_t
{
public:
    page_dirtier_thread() :
        smthread_t(t_regular, "page_dirtier")
    {}

    virtual ~page_dirtier_thread() {}

    virtual void run()
    {
        std::default_random_engine generator;
        std::uniform_int_distribution<PageID> distr(0, last_pid);

        stopwatch_t watch;
        long long begin_ts = watch.now();
        long usec_duration = duration * 1000000; // microseconds

        w_assert0(dirty_rate <= 1000000);
        size_t sleep_time_us = 1000000 / dirty_rate;
        chrono::duration<size_t, micro> sleep_duration {sleep_time_us};

        generic_page* page;
        size_t i = 0;
        while (true) {
            PageID pid = distr(generator);

            if (alloc_cache_t::is_alloc_pid(pid) || pid == stnode_page::stpid) {
                continue;
            }

            W_COERCE(bf->fix_nonroot(page, NULL, pid, LATCH_EX, false, false));
            bf->set_page_lsn(page, global_lsn);
            global_lsn.advance(1);
            bf->unfix(page);

            this_thread::sleep_for(sleep_duration);

            // Only check for expiration approx. every second
            if (++i % dirty_rate == 0) {
                if (watch.now() - begin_ts > usec_duration) { break; }
            }
        }

    }
};

void init_bf()
{
    // Allocate and fix-init all pages
    generic_page* page;
    last_pid = bf->get_block_cnt();
    while (vol->num_used_pages() < last_pid) {
        PageID pid;
        W_COERCE(vol->alloc_a_page(pid));
        W_COERCE(bf->fix_nonroot(page, NULL, pid, LATCH_EX, false, true));
        page->lsn = lsn_t::null;
        page->pid = pid;
        page->store = 0;
        bf->unfix(page);
    }
}

class main_thread_t : public smthread_t
{
public:
    main_thread_t() :
        smthread_t(t_regular, "bf_stresser")
    {}

    virtual ~main_thread_t() {}

    virtual void run ()
    {
        sm_opt.set_string_option("sm_logdir", logdir);
        sm_opt.set_string_option("sm_dbfile", dbfile);
        sm_opt.set_int_option("sm_bufpoolsize", bufsize);
        sm_opt.set_bool_option("sm_format", true);
        sm_opt.set_int_option("sm_cleaner_interval", 0);

        smlevel_0::log = new log_core(sm_opt);
        smlevel_0::log->init();

        vol = new vol_t(sm_opt, nullptr);
        bf = new bf_tree_m(sm_opt);

        smlevel_0::bf = bf;
        smlevel_0::vol = vol;

        vol->build_caches(true /*format*/);
        bf->get_cleaner();
        init_bf();

        page_dirtier_thread dirtier;
        dirtier.fork();
        dirtier.join();

        bf->shutdown();
        vol->shutdown(false);
        smlevel_0::log->shutdown();

        delete bf;
        delete vol;
        delete smlevel_0::log;
    }
};

int main(int argc, char** argv)
{
    setup_options();
    po::store(po::parse_command_line(argc, argv, options_desc), options);
    po::notify(options);

    sthread_t::initialize_sthreads_package();
    smthread_t::init_fingerprint_map();

    main_thread_t t;
    t.fork();
    t.join();

    sm_stats_info_t stats;
    ss_m::gather_stats(stats);
    cout << stats << endl;
}
