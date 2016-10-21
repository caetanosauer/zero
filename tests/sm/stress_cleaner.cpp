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
#include "base/command.h"
#include "btree_logrec.h"
#include <chrono>
#include <random>
#include <thread>
#include <functional>

#include <boost/program_options.hpp>
namespace po = boost::program_options;

using namespace std;

po::options_description options_desc;
po::variables_map options;
sm_options sm_opt;

vol_t* vol;
bf_tree_m* bf;

size_t dirty_rate;
size_t duration;
PageID last_pid;
string trace_file;

vector<PageID> pid_trace;
w_keystr_t fake_key;
bool scan_done;

void setup_options()
{
    Command::setupSMOptions(options_desc);
    options_desc.add_options()
    ("duration,d", po::value<size_t>(&duration)->default_value(0),
        "Duration for which to run experiment (in seconds)")
    ("dirty-rate,r", po::value<size_t>(&dirty_rate)->default_value(1000),
        "How many times a second a page is updated (marked dirty)")
    ("trace-file", po::value<string>(&trace_file)->default_value(""),
        "File containing a trace of PIDs (one per line) to mark dirty")
    ;
}

lsn_t log_update(const btree_page_h& page)
{
    static logrec_t* lr = new logrec_t;
    static lsn_t ret;
    new (lr) btree_overwrite_log(page, fake_key, "old", "new", 100, 3);
    W_COERCE(smlevel_0::log->insert(*lr, &ret));
    return ret;
}

PageID get_next_pid_trace()
{
    static size_t i = 0;
    if (i >= pid_trace.size()) {
        if (duration == 0) {
            scan_done = true;
            return 0;
        }
        i = 0;
    }
    // if (i % 100000 == 0) { cout << i << endl; }
    return pid_trace[i++];
}

PageID get_next_pid_random()
{
    static std::default_random_engine generator;
    static std::uniform_int_distribution<PageID> distr(0, last_pid);
    return distr(generator);
}

class page_dirtier_thread : public smthread_t
{
public:
    page_dirtier_thread() :
        smthread_t(t_regular, "page_dirtier")
    {}

    virtual ~page_dirtier_thread() {}

    template<bool UseTrace> PageID get_next_pid();

    virtual void run()
    {
        std::function<PageID()> get_next_pid;
        if (pid_trace.size() > 0) {
            get_next_pid = get_next_pid_trace;
            duration = 0;
        }
        else {
            get_next_pid = get_next_pid_random;
            if (duration == 0) { duration = 60; }
        }

        stopwatch_t watch;
        long long begin_ts = watch.now();
        long usec_duration = duration * 1000000; // microseconds

        w_assert0(dirty_rate <= 1000000);
        size_t sleep_time_us = 1000000 / dirty_rate;
        chrono::duration<size_t, micro> sleep_duration {sleep_time_us};

        size_t count = 0;
        scan_done = false;
        while (!scan_done) {
            PageID pid = get_next_pid();

            if (alloc_cache_t::is_alloc_pid(pid) || pid == stnode_page::stpid) {
                continue;
            }

            btree_page_h p;
            W_COERCE(p.fix_direct(pid, LATCH_EX));
            lsn_t lsn = log_update(p);
            p.update_page_lsn(lsn);

            // this_thread::sleep_for(sleep_duration);

            // Only check for expiration every 10k iterations
            if (duration > 0 && ++count % 10000 == 0) {
                if (watch.now() - begin_ts > usec_duration) {
                    scan_done = true;
                }
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

/**
 * Load PIDs from trace file into array.
 * Whole file is read -- assuming for now there's enough memory
 */
void load_trace()
{
    if (trace_file.empty()) { return; }

    constexpr size_t ssize = 64;
    std::ifstream in(trace_file);
    char str[ssize];
    while (true) {
        in.getline(str, ssize);
        if (!in.good()) {
            return;
        }
        pid_trace.push_back(atoi(str));
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
        sm_opt.set_bool_option("sm_format", true);
        sm_opt.set_int_option("sm_cleaner_interval", 0);
        // Ignoring metadata pages is crucial, because no log info is available
        // for stnode cache
        sm_opt.set_bool_option("sm_cleaner_ignore_metadata", true);
        Command::setSMOptions(sm_opt, options);

        // CS TODO
        ss_m::_options = sm_opt;

        smlevel_0::log = new log_core(sm_opt);
        smlevel_0::log->init();

        bool archiving = sm_opt.get_bool_option("sm_archiving", false);
        if (archiving) {
            smlevel_0::logArchiver = new LogArchiver(sm_opt);
            smlevel_0::logArchiver->fork();
        }

        vol = new vol_t(sm_opt, nullptr);
        bf = new bf_tree_m(sm_opt);

        smlevel_0::bf = bf;
        smlevel_0::vol = vol;

        vol->build_caches(true /*format*/);
        init_bf();
        load_trace();

        cout << "stress_cleaner initialization done!" << endl;

        bf->get_cleaner();
        page_dirtier_thread dirtier;
        dirtier.fork();
        dirtier.join();

        bf->shutdown();
        vol->shutdown(false);
        if (archiving) {
            smlevel_0::logArchiver->shutdown();
            delete smlevel_0::logArchiver;
        }
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

    fake_key.construct_regularkey("k0", 2);

    main_thread_t t;
    t.fork();
    t.join();

    sm_stats_info_t stats;
    ss_m::gather_stats(stats);
    cout << stats << endl;
}
