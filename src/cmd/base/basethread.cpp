#include "basethread.h"

#include <chkpt.h>
#include <sm.h>
#include <restart.h>
#include <vol.h>
#include <btree.h>
#include <bf_tree.h>

#include <log_lsn_tracker.h>
#include <log_core.h>
#include <log_carray.h>

sm_options basethread_t::_options;

basethread_t::basethread_t()
    : finished(false), current_xct(NULL)
{
    DO_PTHREAD(pthread_mutex_init(&running_mutex, NULL));
}

basethread_t::~basethread_t()
{
    DO_PTHREAD(pthread_mutex_destroy(&running_mutex));
}

void basethread_t::before_run()
{
    DO_PTHREAD(pthread_mutex_lock(&running_mutex));
}

void basethread_t::after_run()
{
    DO_PTHREAD(pthread_mutex_unlock(&running_mutex));
}

void basethread_t::start_base()
{
}

void basethread_t::start_buffer()
{
    if (!smlevel_0::bf) {
        cerr << "Initializing buffer manager ... ";
        smlevel_0::bf = new bf_tree_m(_options);
        assert(smlevel_0::bf);
        cerr << "OK" << endl;
    }
}

void basethread_t::start_log(string logdir)
{
    if (!smlevel_0::log) {
        // instantiate log manager
        log_core* log;
        cerr << "Initializing log manager ... " << flush;
        _options.set_string_option("sm_logdir", logdir);
        _options.set_int_option("sm_logsize", 10000 * 1024);
        log = new log_core(_options);
        smlevel_0::log = log;
        cerr << "OK" << endl;
    }
}

void basethread_t::start_archiver(string archdir, size_t wsize, size_t bsize)
{
    LogArchiver* logArchiver;

    cerr << "Initializing log archiver ... " << flush;
    _options.set_string_option("sm_archdir", archdir);
    _options.set_int_option("sm_archiver_workspace_size", wsize);
    _options.set_int_option("sm_archiver_block_size", bsize);
    logArchiver = new LogArchiver(_options);
    cerr << "OK" << endl;

    smlevel_0::logArchiver = logArchiver;
}

void basethread_t::start_merger(string /*archdir*/)
{
    //ArchiveMerger* archiveMerger;

    //cerr << "Initializing archive merger ... " << flush;
    //W_COERCE(
            //ArchiveMerger::constructOnce(archiveMerger, archdir, 1000,
                //1024 * 1024));
    //cerr << "OK" << endl;

    //smlevel_0::archiveMerger = archiveMerger;
}

void basethread_t::start_other()
{
    if (!smlevel_0::lm) {
        cerr << "Initializing lock manager ... ";
        smlevel_0::lm = new lock_m(_options);
        cerr << "OK" << endl;

        cerr << "Initializing checkpoint manager ... ";
        smlevel_0::chkpt = new chkpt_m(_options);
        cerr << "OK" << endl;

        cerr << "Initializing b-tree manager ... ";
        btree_m::construct_once();

        assert(
                smlevel_0::lm &&
                smlevel_0::chkpt
              );

        cerr << "OK" << endl;
    }
}

void basethread_t::print_stats()
{
    sm_stats_info_t stats;
    ss_m::gather_stats(stats);
    cout << stats << endl;
}

/*
 * WARNING: if the transaction produces any log record (i.e., makes any
 * modification), then log_m must be started.
 */
void basethread_t::begin_xct()
{
    assert(current_xct == NULL);
    int timeout = timeout_t::WAIT_SPECIFIED_BY_THREAD;
    current_xct = new xct_t(NULL, timeout, false, false, false);
    smlevel_0::log->get_oldest_lsn_tracker()
        ->enter(reinterpret_cast<uintptr_t>(current_xct),
                smlevel_0::log->curr_lsn());
}

void basethread_t::commit_xct()
{
    assert(current_xct != NULL);
    current_xct->commit(false, NULL);
    delete current_xct;
}
