#include "restore_cmd.h"

#include "shore_env.h"
#include "vol.h"

class FailureThread : public smthread_t
{
public:
    FailureThread(unsigned delay, bool evict, bool* flag)
        : smthread_t(t_regular, "FailureThread"),
        delay(delay), evict(evict), flag(flag)
    {
    }

    virtual ~FailureThread() {}

    virtual void run()
    {
        ::sleep(delay);

        vol_t* vol = smlevel_0::vol;
        w_assert0(vol);
        vol->mark_failed(evict);

        // disable eager archiving
        smlevel_0::logArchiver->setEager(false);

        *flag = true;
        lintel::atomic_thread_fence(lintel::memory_order_release);
    }

private:
    unsigned delay;
    bool evict;
    bool* flag;
};

void RestoreCmd::setupOptions()
{
    KitsCommand::setupOptions();
    options.add_options()
        ("segmentSize", po::value<unsigned>(&opt_segmentSize)
            ->default_value(1024),
            "Size of restore segment in number of pages")
        ("singlePass", po::value<bool>(&opt_singlePass)->default_value(false)
            ->implicit_value(true),
            "Whether to use single-pass restore scheduler from the start")
        ("instant", po::value<bool>(&opt_instant)->default_value(true)
            ->implicit_value(true),
            "Use instant restore (i.e., access data before restore is done)")
        ("ondemand", po::value<bool>(&opt_onDemand)->default_value(true)
            ->implicit_value(true),
            "Support on-demand restore scheduling")
        ("offline", po::value<bool>(&opt_offline)->default_value(false)
            ->implicit_value(true),
            "Perform restore offline (i.e., without concurrent transactions)")
        ("randomOrder", po::value<bool>(&opt_randomOrder)->default_value(false)
            ->implicit_value(true),
            "Single-pass policy of scheduler proceeds in random order among \
            segments, instead of sequential")
        ("evict", po::value<bool>(&opt_evict)->default_value(false)
            ->implicit_value(true),
            "Evict all pages from buffer pool when failure happens")
        ("failDelay", po::value<unsigned>(&opt_failDelay)->default_value(60),
            "Time to wait before marking the volume as failed")
        ("waitForRestore", po::value<bool>(&opt_waitForRestore)
            ->default_value(false)->implicit_value(true),
            "Finish benchmark only when restore is finished")
    ;
}

void RestoreCmd::loadOptions(sm_options& options)
{
    KitsCommand::loadOptions(options);

    if (opt_offline) {
        opt_singlePass = true;
        opt_onDemand = false;
    }
    options.set_int_option("sm_restore_segsize", opt_segmentSize);
    options.set_bool_option("sm_restore_instant", opt_instant);
    options.set_bool_option("sm_restore_sched_singlepass", opt_singlePass);
    options.set_bool_option("sm_restore_sched_ondemand", opt_onDemand);
    options.set_bool_option("sm_restore_sched_random", opt_randomOrder);
}

void RestoreCmd::run()
{
    if (archdir.empty()) {
        throw runtime_error("Log Archive is required to perform restore. \
                Specify path to archive directory with -a");
    }

    // STEP 1 - load database and take backup
    if (opt_load) {
        // delete existing backups
        if (!opt_backup.empty()) {
            ensureEmptyPath(opt_backup);
        }
    }
    init();

    if (opt_load) {
        shoreEnv->load();
    }

    vol_t* vol = smlevel_0::vol;

    if (!opt_backup.empty()) {
        W_COERCE(vol->take_backup(opt_backup));
    }

    // STEP 2 - spawn failure thread and run benchmark
    FailureThread* t = NULL;
    if (!opt_offline) {
        hasFailed = false;
        t = new FailureThread(opt_failDelay, opt_evict,
                &hasFailed);
        t->fork();
    }

    // TODO if crash is on, move runBenchmark into a separate thread
    // and crash after specified delay. To crash, look at the restart
    // test classes and shutdown the SM like it's done there. Then,
    // bring it back up again and call mark_failed after system comes
    // back. If instant restart is on, then REDO will invoke restore.
    // Meanwhile, the thread running the benchmark will accumulate
    // errors, which should be ok (see trx_worker_t::_serve_action).

    // This will call doWork()
    runBenchmark();

    finish();

    if (t) {
        t->join();
        delete t;
    }
}

void RestoreCmd::doWork()
{
    vol_t* vol = smlevel_0::vol;
    w_assert0(vol);

    if (opt_offline) {
        // run benchmark until end and then mark failed
        if (opt_num_trxs > 0 || opt_duration > 0) {
            KitsCommand::doWork();
            joinClients();
        }
        vol->mark_failed(opt_evict);
    }
    else {
        // Start benchmark and wait for failure thread to mark device failed
        forkClients();
        sleep(opt_failDelay);
        while (!hasFailed) {
            sleep(1);
            lintel::atomic_thread_fence(lintel::memory_order_consume);
        }
    }

    // Now wait for device to be restored -- check every 1 second
    while (vol->is_failed()) {
        sleep(1);
        vol->check_restore_finished();
    }

    // In online restore, wait for duration only after restore is complete
    if (!opt_offline && (opt_num_trxs > 0 || opt_duration > 0)) {
        if (mtype == MT_TIME_DUR) {
            int remaining = opt_duration;
            while (remaining > 0) {
                remaining = ::sleep(remaining);
            }
        }
    }
}
