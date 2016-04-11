#include "kits_cmd.h"

#include <stdexcept>
#include <string>

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

#include "shore_env.h"
#include "tpcb/tpcb_env.h"
#include "tpcb/tpcb_client.h"
#include "tpcc/tpcc_env.h"
#include "tpcc/tpcc_client.h"

#include "util/stopwatch.h"

int MAX_THREADS = 1000;

class CrashThread : public smthread_t
{
public:
    CrashThread(unsigned delay)
        : smthread_t(t_regular, "CrashThread"),
        delay(delay)
    {
    }

    virtual ~CrashThread() {}

    virtual void run()
    {
        ::sleep(delay);
        cerr << "Crash thread will now abort program" << endl;
        abort();
    }

private:
    unsigned delay;
};

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
        ("duration", po::value<unsigned>(&opt_duration)->default_value(0),
            "Run benchmark for the given number of seconds (overrides the \
            trxs option)")
        ("threads,t", po::value<int>(&opt_num_threads)->default_value(4),
            "Number of threads to execute benchmark with")
        ("select_trx,s", po::value<int>(&opt_select_trx)->default_value(0),
            "Transaction code or mix identifier (0 = all trxs)")
        ("queried_sf,q", po::value<int>(&opt_queried_sf)->default_value(1),
            "Scale factor to which to restrict queries")
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
        ("warmup", po::value<unsigned>(&opt_warmup)->default_value(0),
            "Warmup buffer before running for duration or number of trxs")
        ("crashDelay", po::value<int>(&opt_crashDelay)->default_value(-1),
            "Time (sec) to wait before aborting execution to simulate system \
            failure (negative disables)")
    ;
    options.add(kits);
}

KitsCommand::KitsCommand()
    : mtype(MT_UNDEF), clientsForked(false)
{
}

/*
 * Thread object usied for the simple purpose of setting the skew parameters.
 * This is required because the random number generator is available as a
 * member of the Kits thread_t class. Thus, calling URand from a non-Kits
 * thread causes a failure.
 *
 * TODO: Get rid of this nonsense design and decouple random number generation
 * from thread objetcs. In fact, we could try to get rid of thread_t completely
 */
class skew_setter_thread : public thread_t
{
public:
    skew_setter_thread(ShoreEnv* shoreEnv)
        : thread_t("skew_setter"), shoreEnv(shoreEnv)
    {}

    virtual ~skew_setter_thread()
    {}

    virtual void work()
    {
        // area, load, start_imbalance, skew_type
        shoreEnv->set_skew(20, 80, 1, 1);
        shoreEnv->start_load_imbalance();
    }

private:
    ShoreEnv* shoreEnv;
};

void KitsCommand::run()
{
    init();

    if (opt_load) {
        shoreEnv->load();
        cout << "Loading finished!" << endl;
    }

    // Spawn crash thread if requested
    if (opt_crashDelay >= 0) {
        CrashThread* t = new CrashThread(opt_crashDelay);
        t->fork();
    }

    if (opt_num_trxs > 0 || opt_duration > 0) {
        runBenchmark();
    }

    finish();
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

    if (opt_warmup > 0) {
        TRACE(TRACE_ALWAYS, "warming up buffer\n");
        WarmupThread t;
        t.fork();
        t.join();

        // run transactions for the given number of seconds
        createClients<Client, Environment>();
        forkClients();

        int remaining = opt_warmup;
        while (remaining > 1) {
            remaining = ::sleep(remaining);
        }

        joinClients();

        if (opt_warmup > 1) {
            // sleep some more to get a gap in the log ticks
            ::sleep(5);
        }
    }

    stopwatch_t timer;

    if (opt_num_trxs > 0 || opt_duration > 0) {
        TRACE(TRACE_ALWAYS, "begin measurement\n");
        createClients<Client, Environment>();
    }

    doWork();

    if (opt_num_trxs > 0 || opt_duration > 0) {
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

    mtype = opt_duration > 0 ? MT_TIME_DUR : MT_NUM_OF_TRXS;
    int trxsPerThread = opt_num_trxs / opt_num_threads;
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
                wh_id, opt_queried_sf);
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
    forkClients();

    // If running for a time duration, wait specified number of seconds
    if (mtype == MT_TIME_DUR) {
        int remaining = opt_duration;
        while (remaining > 0) {
            remaining = ::sleep(remaining);
        }
    }
}

template<class Environment>
void KitsCommand::initShoreEnv()
{
    shoreEnv = new Environment(optionValues);

    loadOptions(shoreEnv->get_opts());

    shoreEnv->set_sf(opt_queried_sf);
    shoreEnv->set_qf(opt_queried_sf);
    shoreEnv->set_loaders(opt_num_threads);

    shoreEnv->init();
    shoreEnv->set_clobber(opt_load);
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
