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

void KitsCommand::setupOptions()
{
    boost::program_options::options_description kits("Kits Options");
    kits.add_options()
        ("benchmark,b", po::value<string>(&opt_benchmark)->required(),
            "Benchmark to execute. Possible values: tpcb, tpcc")
        // ("config,c", po::value<string>(&opt_conffile)->required(),
        //     "Path to configuration file")
        ("dbfile,d", po::value<string>(&opt_dbfile)->default_value("db"),
            "Path to database file (required only for loading)")
        ("backup", po::value<string>(&opt_backup)->default_value(""),
            "Path on which to store backup file")
        ("sharpBackup", po::value<bool>(&opt_sharpBackup)
            ->default_value(false)->implicit_value(true),
            "Whether to flush log archive prior to taking a backup")
        ("logdir,l", po::value<string>(&logdir)->default_value("log"),
            "Directory containing log to be scanned")
        ("archdir,a", po::value<string>(&archdir)->default_value("archive"),
            "Directory in which to store the log archive")
        ("load", po::value<bool>(&opt_load)->default_value(false)
            ->implicit_value(true),
            "If set, log and archive folders are emptied, database files \
            and backups are deleted, and dataset is loaded from scratch")
        ("bufsize", po::value<int>(&opt_bufsize)->default_value(0),
            "Size of buffer pool in MB")
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
        ("logsize", po::value<unsigned>(&opt_logsize)
            ->default_value(10000),
            "Maximum size of log (in MB) (default 10GB)")
        ("logbufsize", po::value<unsigned>(&opt_logbufsize)
            ->default_value(80),
            "Size of log buffer (in MB) (default 80)")
        ("eager", po::value<bool>(&opt_eager)->default_value(true)
            ->implicit_value(true),
            "Run log archiving in eager mode")
        ("truncateLog", po::value<bool>(&opt_truncateLog)->default_value(false)
            ->implicit_value(true),
            "Truncate log until last checkpoint after loading")
        ("archWorkspace", po::value<unsigned>(&opt_archWorkspace)
            ->default_value(100),
            "Size of log archiver sort workspace in MB")
        ("skew", po::value<bool>(&opt_skew)->default_value(false)
            ->implicit_value(true),
            "Activate skew on transaction inputs (currently only 80:20 skew \
            is supported, i.e., 80% of access to 20% of data")
        ("warmup", po::value<unsigned>(&opt_warmup)->default_value(0),
            "Warmup buffer before running for duration or number of trxs")
    ;
    options.add(kits);
    setupSMOptions();
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

    if (!opt_backup.empty()) {
        ensureParentPathExists(opt_backup);
    }

    if (opt_load) {
        shoreEnv->load();
    }

    cout << "Loading finished!" << endl;

    if (opt_num_trxs > 0 || opt_duration > 0) {
        runBenchmark();
    }

    if (!opt_backup.empty()) {
        // Make sure all workers are done
        shoreEnv->stop();

        vol_t* vol = smlevel_0::vol;
        w_assert1(vol);

        if (!opt_eager) {
            archiveLog();
        }

        cout << "Taking backup ... ";
        W_COERCE(vol->take_backup(opt_backup, opt_sharpBackup));
        // add call to sx_add_backup
        cout << "done!" << endl;
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
        while (remaining > 0) {
            remaining = ::sleep(remaining);
        }

        joinClients();
        // sleep some more to get a gap in the log ticks
        ::sleep(5);
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
    if (opt_load) {
        if (opt_dbfile.empty()) {
            throw runtime_error("Option dbfile cannot be empty!");
        }

        // delete existing logs and db files
        ensureEmptyPath(logdir);
        if (!archdir.empty()) {
            ensureEmptyPath(archdir);
        }
        ensureParentPathExists(opt_dbfile);

        shoreEnv->set_device(opt_dbfile);
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
 * Given path should be a file name (e.g., DB file or backup).
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
    options.set_string_option("sm_dbfile", opt_dbfile);
    options.set_bool_option("sm_truncate", opt_load);
    options.set_string_option("sm_logdir", logdir);
    mkdirs(logdir);

    options.set_int_option("sm_logsize", opt_logsize * 1024);
    options.set_int_option("sm_logbufsize", opt_logbufsize * 1024);

    if (!archdir.empty()) {
        options.set_bool_option("sm_archiving", true);
        options.set_string_option("sm_archdir", archdir);
        options.set_bool_option("sm_archiver_eager", opt_eager);
        options.set_int_option("sm_archiver_workspace_size",
                opt_archWorkspace * 1048576);
        mkdirs(archdir);
    }

    // ticker always turned on
    options.set_bool_option("sm_ticker_enable", true);

    if (opt_bufsize <= 0) {
        // TODO: default size for buffer pool may depend on SF and benchmark
        opt_bufsize = 8; // 8 MB
    }
    options.set_int_option("sm_bufpoolsize", opt_bufsize * 1024);

    options.set_bool_option("sm_truncate_log", opt_truncateLog);
}

void KitsCommand::archiveLog()
{
    // archive whole log
    smlevel_0::logArchiver->activate(smlevel_0::log->curr_lsn(), true);
    while (smlevel_0::logArchiver->getNextConsumedLSN() < smlevel_0::log->curr_lsn()) {
        usleep(1000);
    }
    smlevel_0::logArchiver->shutdown();
    smlevel_0::logArchiver->join();
}

void KitsCommand::finish()
{
    // shoreEnv has stop() and close(), and close calls stop (confusing!)
    shoreEnv->close();
}
