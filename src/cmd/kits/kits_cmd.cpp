#include "kits_cmd.h"

#include <stdexcept>
#include <string>
#include <thread>
#include <chrono>

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

#include "thread_wrapper.h"
#include "fixable_page_h.h"
#include "shore_env.h"
#include "tpcb/tpcb_env.h"
#include "tpcb/tpcb_client.h"
#include "tpcc/tpcc_env.h"
#include "tpcc/tpcc_client.h"
#include "tpcc/tpcc_input.h"

#include "util/stopwatch.h"

int MAX_THREADS = 1000;

template <class Env>
class CrashThread : public thread_wrapper_t
{
public:
    CrashThread(Env* env, unsigned delay, bool wait_for_warmup)
        : env(env), delay(delay), wait_for_warmup(wait_for_warmup)
    {
    }

    virtual void run()
    {
        if (wait_for_warmup) {
            env->wait_for_warmup();
        }

        std::this_thread::sleep_for(std::chrono::seconds(delay));

        cerr << "Crash thread will now shutdown SM" << endl;
        // env->set_sm_shudown_filthy(true);
        env->set_stop_benchmark(true);
    }

private:
    Env* env;
    unsigned delay;
    bool wait_for_warmup;
};

class FailureThread : public thread_wrapper_t
{
public:
    FailureThread(unsigned delay, bool* flag)
        : delay(delay), flag(flag)
    {
    }

    virtual ~FailureThread() {}

    virtual void run()
    {
        ::sleep(delay);

        smlevel_0::bf->set_media_failure();

        // disable eager archiving
        // smlevel_0::logArchiver->setEager(false);

        *flag = true;
        lintel::atomic_thread_fence(lintel::memory_order_release);
    }

private:
    unsigned delay;
    bool* flag;
};

class SkewShiftingThread : public worker_thread_t
{
public:
    SkewShiftingThread(unsigned delay)
        : worker_thread_t(0), delay(delay)
    {
    }

    virtual void do_work()
    {
        // CS TODO: onlt works for TPC-C!
        ::sleep(delay);
        tpcc::w_skewer.increment_intervals();
    }

private:
    unsigned delay;
};

void KitsCommand::set_stop_benchmark(bool stop)
{
    shoreEnv->set_stop_benchmark(stop);
}

void KitsCommand::crashFilthy()
{
    shoreEnv->set_sm_shudown_filthy(true);
    set_stop_benchmark(true);
}


void KitsCommand::setupOptions()
{
    setupSMOptions(options);
    boost::program_options::options_description kits("Kits Options");
    kits.add_options()
        ("benchmark,b", po::value<string>(&opt_benchmark)->required(),
            "Benchmark to execute. Possible values: tpcb, tpcc")
        ("load", po::value<bool>(&opt_load)->default_value(false)
            ->implicit_value(true),
            "If set, log and archive folders are emptied, database files \
            and backups are deleted, and dataset is loaded from scratch")
        ("trxs", po::value<int>(&opt_num_trxs)->default_value(0),
            "Number of transactions to execute")
        ("logVolume", po::value<unsigned>(&opt_log_volume)->default_value(0),
            "Run benchmark until the given amount of log volume (in MB) is reached \
            (overrides the trxs option)")
        ("duration", po::value<unsigned>(&opt_duration)->default_value(0),
            "Run benchmark for the given number of seconds (overrides the \
            logVolume option)")
        ("no_stop", po::value<bool>(&opt_no_stop)->default_value(false)
            ->implicit_value(true),
            "Run benchmark continuously until a crash occurs (overrides the \
            duration option)")
        ("threads,t", po::value<int>(&opt_num_threads)->default_value(4),
            "Number of threads to execute benchmark with")
        ("select_trx,s", po::value<int>(&opt_select_trx)->default_value(0),
            "Transaction code or mix identifier (0 = all trxs)")
        ("queried_sf,q", po::value<int>(&opt_queried_sf)->default_value(1),
            "Scale factor to which to restrict queries")
        ("asyncCommit", po::value<bool>(&opt_asyncCommit)->default_value(true)
            ->implicit_value(true),
            "Whether to use asynchronous commit (non-durable) for Kits transactions")
        ("spread", po::value<bool>(&opt_spread)->default_value(true)
            ->implicit_value(true),
            "Attach each worker thread to a fixed core for improved concurrency")
        ("eager", po::value<bool>(&opt_eager)->default_value(true)
            ->implicit_value(true),
            "Run log archiving in eager mode")
        ("skew", po::value<bool>(&opt_skew)->default_value(false)
            ->implicit_value(true),
            "Activate skew on transaction inputs (currently only 80:20 skew \
            is supported, i.e., 80% of access to 20% of data")
        ("warmup", po::value<bool>(&opt_warmup)->default_value(false),
            "Warmup buffer before crashing or counting duration")
        ("crashDelay", po::value<int>(&opt_crashDelay)->default_value(-1),
            "Time (sec) to wait before aborting execution to simulate system \
            failure (negative disables)")
        ("crashDelayAfterInit", po::value<bool>(&opt_crashDelayAfterInit)->default_value(true),
            "Start counting crash delay only after SM is initialized (and recovered if in ARIES)")
        ("failDelay", po::value<int>(&opt_failDelay)->default_value(-1),
            "Time to wait before marking the volume as failed (simulates media failure)")
        ("skewShiftDelay", po::value<int>(&opt_skewShiftDelay)->default_value(0),
            "Shift skewed are every N seconds")
    ;
    options.add(kits);
}

ShoreEnv* KitsCommand::getShoreEnv()
{
    return shoreEnv;
}

KitsCommand::KitsCommand()
    : mtype(MT_UNDEF), clientsForked(false), failure_thread(nullptr)
{}

void KitsCommand::run()
{
    init();

    if (opt_load) {
        shoreEnv->load();
        cout << "Loading finished!" << endl;
    }

    // Spawn failure thread if requested
    if (opt_failDelay >= 0) {
        mediaFailure(opt_failDelay);
    }

    if (opt_skew && opt_skewShiftDelay > 0) {
        skew_shifter = std::make_shared<SkewShiftingThread>(opt_skewShiftDelay);
    }

    if (runBenchAfterLoad()) {
        runBenchmark();
    }

    finish();

    if (failure_thread) {
        failure_thread->join();
        delete failure_thread;
    }

    if (skew_shifter) {
        skew_shifter->stop();
    }

    if (pre_init_crash_thread) {
        pre_init_crash_thread->join();
        pre_init_crash_thread = nullptr;
    }
    // crash_thread aborts the program, so we don't worry about joining it
}

void KitsCommand::mediaFailure(unsigned delay)
{
    //FailureThread* failure_thread = nullptr;
    hasFailed = false;
    failure_thread = new FailureThread(delay, &hasFailed);
    failure_thread->fork();
}

void KitsCommand::randomRootPageFailure()
{
    std::vector<StoreID> stores;
    smlevel_0::vol->get_stnode_cache()->get_used_stores(stores);
    int randomStore = random()%(stores.size() -1);
    // StoreID stid = stores.at(randomStore);
    // PageID root_pid = smlevel_0::vol->get_stnode_cache()->get_root_pid(stid);
    fixable_page_h page;

    page.fix_root(stores.at(randomStore), LATCH_EX);
    generic_page* raw_page = page.get_generic_page();
    raw_page->lsn = lsn_t(1, 0);
    page.unfix(true);
}

void KitsCommand::init()
{
    // just TPC-B and TPC-C for now
    if (opt_benchmark == "tpcb") {
        initShoreEnv<tpcb::ShoreTPCBEnv>();
    }
    else if (opt_benchmark == "tpcc") {
        initShoreEnv<tpcc::ShoreTPCCEnv>();
    }
    else {
        throw runtime_error("Unknown benchmark string");
    }
}

void KitsCommand::runBenchmark()
{
    if (opt_benchmark == "tpcb") {
        runBenchmarkSpec<tpcb::baseline_tpcb_client_t, tpcb::ShoreTPCBEnv>();
    }
    else if (opt_benchmark == "tpcc") {
        runBenchmarkSpec<tpcc::baseline_tpcc_client_t, tpcc::ShoreTPCCEnv>();
    }
    else {
        throw runtime_error("Unknown benchmark string");
    }
}

template<class Client, class Environment>
void KitsCommand::runBenchmarkSpec()
{
    shoreEnv->reset_stats();

    // reset monitor stats
#ifdef HAVE_CPUMON
    _g_mon->cntr_reset();
#endif

    if (opt_queried_sf <= 0) {
        opt_queried_sf = shoreEnv->get_sf();
    }

    stopwatch_t timer;

    if (runBenchAfterLoad()) {
        TRACE(TRACE_ALWAYS, "begin measurement\n");
        createClients<Client, Environment>();

        // Spawn crash thread if requested
        if (opt_crashDelay >= 0 && opt_crashDelayAfterInit) {
            forkClients();

            auto crash_thread =
                std::make_shared<CrashThread<ShoreEnv>>(shoreEnv, opt_crashDelay, opt_warmup);
            crash_thread->fork();
            crash_thread->join();

            // Reinitialize shore env
            joinClients();
            shoreEnv->close();
            auto res = shoreEnv->start();
            w_assert0(res == 0);
            TRACE(TRACE_ALWAYS, "begin measurement\n");
            createClients<Client, Environment>();
        }
    }

    doWork();

    if (runBenchAfterLoad()) {
        joinClients();
    }

    double delay = timer.time();
    //xct_stats stats = shell_get_xct_stats();
#ifdef HAVE_CPUMON
    _g_mon->cntr_pause();
    unsigned long miochs = _g_mon->iochars()/MILLION;
    double usage = _g_mon->get_avg_usage(true);
#else
    unsigned long miochs = 0;
    double usage = 0;
#endif
    TRACE(TRACE_ALWAYS, "end measurement\n");
    shoreEnv->print_throughput(opt_queried_sf, opt_spread, opt_num_threads, delay,
            miochs, usage);
}

template<class Client, class Environment>
void KitsCommand::createClients()
{
    // reset starting cpu and wh id
    int current_prs_id = -1;
    int wh_id = 0;

    mtype = MT_UNDEF;
    int trxsPerThread = 0;
    if (opt_no_stop == true) {
        mtype = MT_NO_STOP;
    }
    else if (opt_duration > 0) {
        mtype = MT_TIME_DUR;
    }
    else if (opt_num_trxs > 0) {
        mtype = MT_NUM_OF_TRXS;
        trxsPerThread = opt_num_trxs / opt_num_threads;
    }
    else if (opt_log_volume > 0) {
        mtype = MT_LOG_VOL;
    }


    for (int i = 0; i < opt_num_threads; i++) {
        // create & fork testing threads
        if (opt_spread) {
            wh_id = (i%(int)opt_queried_sf)+1;
        }

        // CS: 1st arg is binding type, which I don't know what it is for It
        // seems like it is a way to specify what the next CPU id is.  If
        // BT_NONE is given, it simply returns -1 TODO: this is required to
        // implement opt_spread -- take a look!  current_prs_id =
        // next_cpu(BT_NONE, current_prs_id);
        Client* client = new Client(
                "client-" + std::to_string(i), i,
                (Environment*) shoreEnv,
                mtype, opt_select_trx,
                trxsPerThread,
                current_prs_id /* cpu id -- see below */,
                wh_id, opt_queried_sf,
                opt_num_threads);
        w_assert0(client);
        clients.push_back(client);
    }
}

void KitsCommand::forkClients()
{
    for (size_t i = 0; i < clients.size(); i++) {
        clients[i]->fork();
    }
    clientsForked = true;
    shoreEnv->set_measure(MST_MEASURE);
}

void KitsCommand::joinClients()
{
    shoreEnv->set_measure(MST_DONE);

    if (clientsForked) {
        for (size_t i = 0; i < clients.size(); i++) {
            clients[i]->join();
            if (clients[i]->rv()) {
                throw runtime_error("Client thread reported error");
            }
            delete (clients[i]);
        }
        clientsForked = false;
        clients.clear();
    }
}

void KitsCommand::doWork()
{
    lsn_t last_log_tail = smlevel_0::log->durable_lsn();

    forkClients();

    if (opt_failDelay > 0) {
        // Start benchmark and wait for failure thread to mark device failed
        sleep(opt_failDelay);
        while (!hasFailed) {
            sleep(1);
            lintel::atomic_thread_fence(lintel::memory_order_consume);
        }

        // Now wait for device to be restored -- check every 1 second
        while (smlevel_0::bf->is_media_failure()) {
            sleep(1);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    // If running for a time duration, wait specified number of seconds
    if (mtype == MT_TIME_DUR) {
        std::this_thread::sleep_for(std::chrono::seconds(opt_duration));
    }
    else if (mtype == MT_LOG_VOL) {
        // check every second
        size_t part_size = smlevel_0::log->get_storage()->get_partition_size();
        unsigned long generated_log_vol = 0;
        while (generated_log_vol / 1048576 < opt_log_volume) {
            ::sleep(1);
            lsn_t log_tail = smlevel_0::log->durable_lsn();
            unsigned partitions = log_tail.hi() - last_log_tail.hi();
            if (partitions == 0) {
                generated_log_vol += log_tail.lo() - last_log_tail.lo();
            }
            else {
                generated_log_vol += part_size - last_log_tail.lo();
                generated_log_vol += part_size * (partitions-1);
                generated_log_vol += log_tail.lo();
            }
            last_log_tail = log_tail;
        }
    }
    else if (mtype == MT_NO_STOP) {
        // keep running until variable is externally set to true
        while(!shoreEnv->should_stop_benchmark()){
            ::usleep(100000); // 100ms
        }
    }
}

template<class Environment>
void KitsCommand::initShoreEnv()
{
    shoreEnv = new Environment(optionValues);

    shoreEnv->set_sf(opt_queried_sf);
    shoreEnv->set_qf(opt_queried_sf);
    shoreEnv->set_loaders(opt_num_threads);
    shoreEnv->setAsynchCommit(opt_asyncCommit);

    auto res = shoreEnv->init();
    w_assert0(res == 0);
    shoreEnv->set_clobber(opt_load);
    loadOptions(shoreEnv->get_opts());

    if (opt_crashDelay >= 0 && !opt_crashDelayAfterInit) {
        pre_init_crash_thread =
            std::make_shared<CrashThread<ShoreEnv>>(shoreEnv, opt_crashDelay, false);
        pre_init_crash_thread->fork();
    }

    if (opt_skew) {
        bool shifting = opt_skewShiftDelay > 0;
        shoreEnv->set_skew(10, 90, 1, 1, shifting);
        shoreEnv->start_load_imbalance();
    }

    shoreEnv->start();
}

void KitsCommand::mkdirs(string path)
{
    // if directory does not exist, create it
    fs::path fspath(path);
    if (!fs::exists(fspath)) {
        fs::create_directories(fspath);
    }
    else {
        if (!fs::is_directory(fspath)) {
            throw runtime_error("Provided path is not a directory!");
        }
    }
}

/**
 * Given path should be a file name (e.g., DB file).
 * Ensures that the directory containing the file exists, to avoid
 * OS failures in the SM when creating the file.
 */
void KitsCommand::ensureParentPathExists(string path)
{

    fs::path fspath(path);
    fspath.remove_filename();
    string parent = fspath.string();
    // can happen if relative path is used
    if (parent.empty()) { return; }

    mkdirs(parent);
}

/**
 * If called on directory, remove all its contents, leaving an empty directory
 * behind.
 */
void KitsCommand::ensureEmptyPath(string path)
{
    fs::path fspath(path);
    if (!fs::exists(path)) {
        return;
    }

    if (!fs::is_empty(fspath)) {
        if (fs::is_directory(fspath)) {
            // delete all contents
            fs::directory_iterator end, it(fspath);
            while (it != end) {
                fs::remove_all(it->path());
                it++;
            }
            w_assert1(fs::is_empty(fspath));
        }
    }
}

void KitsCommand::loadOptions(sm_options& options)
{
    options.set_bool_option("sm_format", opt_load);

    // ticker always turned on
    options.set_bool_option("sm_ticker_enable", true);
}

void KitsCommand::finish()
{
    // shoreEnv has stop() and close(), and close calls stop (confusing!)
    shoreEnv->close();
}

bool KitsCommand::running()
{
    return clientsForked;
}
