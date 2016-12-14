/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore'>

 $Id: sm.cpp,v 1.501 2010/12/17 19:36:26 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#include "w_defines.h"
#include "w.h"
#include "sm_base.h"
#include "btree.h"
#include "chkpt.h"
#include "sm.h"
#include "vol.h"
#include "bf_tree.h"
#include "restart.h"
#include "sm_options.h"
#include "tid_t.h"
#include "log_carray.h"
#include "log_lsn_tracker.h"
#include "bf_tree.h"
#include "stopwatch.h"
#include "alloc_cache.h"
#include "btree_page.h"
#include "allocator.h"
#include "log_core.h"


sm_tls_allocator smlevel_0::allocator;

memalign_allocator<char, smlevel_0::IO_ALIGN> smlevel_0::aligned_allocator;


bool         smlevel_0::shutdown_clean = false;
bool         smlevel_0::shutting_down = false;
            //controlled by AutoTurnOffLogging:
bool        smlevel_0::lock_caching_default = true;
bool        smlevel_0::logging_enabled = true;
bool        smlevel_0::do_prefetch = false;
bool        smlevel_0::statistics_enabled = true;

/*
 * _being_xct_mutex: Used to prevent xct creation during volume dismount.
 * Its sole purpose is to be sure that we don't have transactions
 * running while we are  creating or destroying volumes or
 * mounting/dismounting devices, which are generally
 * start-up/shut-down operations for a server.
 */

vol_t* smlevel_0::vol = 0;
bf_tree_m* smlevel_0::bf = 0;
log_core* smlevel_0::log = 0;
LogArchiver* smlevel_0::logArchiver = 0;

lock_m* smlevel_0::lm = 0;

char smlevel_0::zero_page[page_sz];

chkpt_m* smlevel_0::chkpt = 0;

restart_m* smlevel_0::recovery = 0;

ss_m* smlevel_top::SSM = 0;

/*
 *  Class ss_m code
 */

/*
 *  Order is important!!
 */
int ss_m::_instance_cnt = 0;
sm_options ss_m::_options;


static queue_based_block_lock_t ssm_once_mutex;
ss_m::ss_m(const sm_options &options)
{
    _options = options;

    // Start the store during ss_m constructor if caller is asking for it
    bool started = startup();
    // If error encountered, raise fatal error if it was not raised already
    if (!started)
        W_FATAL_MSG(eINTERNAL, << "Failed to start the store from ss_m constructor");
}

bool ss_m::startup()
{
    CRITICAL_SECTION(cs, ssm_once_mutex);
    if (0 == _instance_cnt)
    {
        // Start the store if it is not running currently
        // Caller can start and stop the store independent of construct and destory
        // the ss_m object.

        // Note: before each startup() call, including the initial one from ssm
        //          constructor choicen (default setting currently), caller can
        //          optionally clear the log files and data files if a clean start is
        //          required (no recovery in this case).
        //          If the log files and data files are intact from previous runs,
        //          either normal or crash shutdowns, the startup() call will go
        //          through the recovery logic when starting up the store.
        //          After the store started, caller can call 'format_dev', 'mount_dev',
        //          'generate_new_lvid', 'and create_vol' if caller would like to use
        //          new devics and volumes for operations in the new run.

        _construct_once();
        return true;
    }
    // Store is already running, cannot have multiple instances running concurrently
    return false;
}

bool ss_m::shutdown()
{
    CRITICAL_SECTION(cs, ssm_once_mutex);
    if (0 < _instance_cnt)
    {
        // Stop the store if it is running currently,
        // do not destroy the ss_m object, caller can start the store again using
        // the same ss_m object, therefore all the option setting remain the same

        // Note: If caller would like to use the simulated 'crash' shutdown logic,
        //          caller must call set_shutdown_flag(false) to set the crash
        //          shutdown flag before the shutdown() call.
        //          The simulated crash shutdown flag would be reset in every
        //          startup() call.

        // This is a force shutdown, meaning:
        // Clean shutdown - abort all active in-flight transactions, flush buffer pool
        //                            take a checkpoint which would record the mounted vol
        //                            then destroy all the managers and free memory
        // Dirty shutdown (false == shutdown_clean) - destroy all active in-flight
        //                            transactions without aborting, then destroy all the managers
        //                            and free memory.  No flush and no checkpoint

        _destruct_once();
        return true;
    }
    // If the store is not running currently, no-op
    return true;
}

void
ss_m::_construct_once()
{
    stopwatch_t timer;

    // smthread_t::init_fingerprint_map();

    if (_instance_cnt++)  {
        cerr << "ss_m cannot be instantiated more than once" << endl;
        W_FATAL_MSG(eINTERNAL, << "instantiating sm twice");
    }

    w_assert1(page_sz >= 1024);

    /*
     *  Reset flags
     */
    shutting_down = false;
    shutdown_clean = _options.get_bool_option("sm_shutdown_clean", false);
    // if (_options.get_bool_option("sm_format", false)) {
    //     shutdown_clean = true;
    // }

    ERROUT(<< "[" << timer.time_ms() << "] Initializing lock manager");

    lm = new lock_m(_options);
    if (! lm)  {
        W_FATAL(eOUTOFMEMORY);
    }

    ERROUT(<< "[" << timer.time_ms() << "] Initializing log manager (part 1)");

    /*
     *  Level 1
     */
    log = new log_core(_options);
    ERROUT(<< "[" << timer.time_ms() << "] Initializing log manager (part 2)");
    W_COERCE(log->init());

    ERROUT(<< "[" << timer.time_ms() << "] Initializing log archiver");

    // LOG ARCHIVER
    bool archiving = _options.get_bool_option("sm_archiving", false);
    if (archiving) {
        logArchiver = new LogArchiver(_options);
        logArchiver->fork();
    }

    ERROUT(<< "[" << timer.time_ms() << "] Initializing restart manager");

    // Log analysis provides info required to initialize vol_t
    recovery = new restart_m(_options);
    recovery->log_analysis();
    chkpt_t* chkpt_info = recovery->get_chkpt();

    bool instantRestart = _options.get_bool_option("sm_restart_instant", true);
    bool format = _options.get_bool_option("sm_format", false);

    ERROUT(<< "[" << timer.time_ms() << "] Initializing volume manager");

    // If not instant restart, pass null dirty page table, which disables REDO
    // recovery based on SPR so that it is done explicitly by restart_m below.
    vol = new vol_t(_options,
            instantRestart ? chkpt_info : NULL);

    ERROUT(<< "[" << timer.time_ms() << "] Initializing buffer manager");

    bf = new bf_tree_m(_options);
    if (! bf) {
        W_FATAL(eOUTOFMEMORY);
    }

    ERROUT(<< "[" << timer.time_ms() << "] Building volume manager caches");

    if (instantRestart) {
        vol->build_caches(format);
    }

    // Initialize cleaner once vol caches are built
    int cleaner_int = _options.get_int_option("sm_cleaner_interval", 0);
    if (cleaner_int >= 0) {
        // Getter will initialize cleaner on demand
        bf->get_cleaner();
    }

    smlevel_0::statistics_enabled = _options.get_bool_option("sm_statistics", true);

    ERROUT(<< "[" << timer.time_ms() << "] Initializing buffer cleaner and other services");

    btree_m::construct_once();

    chkpt = new chkpt_m(_options, chkpt_info->get_last_scan_start());
    if (! chkpt)  {
        W_FATAL(eOUTOFMEMORY);
    }

    SSM = this;

    smthread_t::mark_pin_count();

    do_prefetch = _options.get_bool_option("sm_prefetch", false);

    ERROUT(<< "[" << timer.time_ms() << "] Performing offline recovery");

    // If not using instant restart, perform log-based REDO before opening up
    if (instantRestart) {
        recovery->spawn_recovery_thread();
    }
    else {
        if (_options.get_bool_option("sm_restart_log_based_redo", true)) {
            recovery->redo_log_pass();
        }
        else {
            recovery->redo_page_pass();
        }
        // metadata caches can only be constructed now
        vol->build_caches(format);
        // system now ready for UNDO
        // CS TODO: can this be done concurrently by restart thread?
        recovery->undo_pass();

        log->discard_fetch_buffers();

        // CS: added this for debugging, but consistency check fails
        // even right after loading -- so it's not a recovery problem
        // vector<StoreID> stores;
        // vol->get_stnode_cache()->get_used_stores(stores);
        // for (size_t i = 0; i < stores.size(); i++) {
        //     bool consistent;
        //     W_COERCE(ss_m::verify_index(stores[i], 31, consistent));
        //     w_assert0(consistent);
        // }
    }

    ERROUT(<< "[" << timer.time_ms() << "] Finished SM initialization");
}

ss_m::~ss_m()
{
    // This looks like a candidate for pthread_once(), but then smsh
    // would not be able to
    // do multiple startups and shutdowns in one process, alas.
    CRITICAL_SECTION(cs, ssm_once_mutex);

    if (0 < _instance_cnt)
        _destruct_once();
}

void
ss_m::_destruct_once()
{
    --_instance_cnt;

    if (_instance_cnt)  {
        cerr << "ss_m::~ss_m() : \n"
            << "\twarning --- destructor called more than once\n"
            << "\tignored" << endl;
        return;
    }

    // CS TODO: get rid of this shutting_down flag, or at least use proper fences.
    // Set shutting_down so that when we disable bg flushing, if the
    // log flush daemon is running, it won't just try to re-activate it.
    shutting_down = true;

    // get rid of all non-prepared transactions
    // First... disassociate me from any tx
    if(xct()) {
        smthread_t::detach_xct(xct());
    }

    ERROUT(<< "Terminating recovery manager");

    // CS TODO: this should not be necessary with proper shutdown (shared_ptr-based)
    // CS TODO: dirty shutdown should interrupt restore threads and finish them abruptly
    while (!vol->check_restore_finished()) {
        ::usleep(100 * 1000); // 100ms
    }
    if (recovery) {
        delete recovery;
        recovery = 0;
    }

    // retire chkpt thread (calling take() directly still possible)
    chkpt->retire_thread();

    // remove all transactions, aborting them in case of clean shutdown
    xct_t::cleanup(shutdown_clean);
    w_assert1(xct_t::num_active_xcts() == 0);

    // log truncation requires clean shutdown
    bool truncate = _options.get_bool_option("sm_truncate_log", false);
    bool truncate_archive = _options.get_bool_option("sm_truncate_archive", false);
    if (shutdown_clean || truncate) {
        ERROUT(<< "SM performing clean shutdown");

        W_COERCE(log->flush_all());
        bf->wakeup_cleaner(true, 1 /* wait for 1 full round */);
        smthread_t::check_actual_pin_count(0);

        // Force alloc and stnode pages
        lsn_t dur_lsn = smlevel_0::log->durable_lsn();
        if (!bf->is_no_db_mode()) {
            W_COERCE(vol->get_alloc_cache()->write_dirty_pages(dur_lsn));
        }

        if (truncate) { W_COERCE(_truncate_log(truncate_archive)); }
        else { chkpt->take(); }

        ERROUT(<< "All pages cleaned successfully");
    }
    else {
        ERROUT(<< "SM performing dirty shutdown");
    }

    delete chkpt; chkpt = 0;

    ERROUT(<< "Terminating log archiver");
    if (logArchiver) { logArchiver->shutdown(); }

    ERROUT(<< "Terminating other services");
    lm->assert_empty(); // no locks should be left
    btree_m::destruct_once();
    delete lm; lm = 0;

    ERROUT(<< "Terminating buffer manager");
    bf->shutdown();
    delete bf; bf = 0; // destroy buffer manager last because io/dev are flushing them!

    if(logArchiver) {
        delete logArchiver; // LL: decoupled cleaner in bf still needs archiver
        logArchiver = 0;    //     so we delete it only after bf is gone
    }

    ERROUT(<< "Terminating volume");
    // this should come before xct and log shutdown so that any
    // ongoing restore has a chance to finish cleanly. Should also come after
    // shutdown of buffer, since forcing the buffer requires the volume.
    // destroy() will stop cleaners
    vol->shutdown(!shutdown_clean);
    delete vol; vol = 0; // io manager

    ERROUT(<< "Terminating log manager");
    if(log) {
        log->shutdown();
        delete log;
    }
    log = 0;

     ERROUT(<< "SM shutdown complete!");
}

rc_t ss_m::_truncate_log(bool truncate_archive)
{
    DBGTHRD(<< "Truncating log on LSN " << log->durable_lsn());

    // Wait for cleaner to finish its current round
    bf->shutdown();
    W_DO(log->flush_all());

    if (logArchiver) {
        logArchiver->archiveUntilLSN(log->durable_lsn());
        if (truncate_archive) { logArchiver->getIndex()->deleteRuns(); }
    }

    W_DO(log->truncate());
    W_DO(log->flush_all());

    // this should be an "empty" checkpoint
    chkpt->take();

    // generate an "empty" log archive run
    if(logArchiver) {
        logArchiver->archiveUntilLSN(log->durable_lsn());
    }

    log->get_storage()->delete_old_partitions();

    return RCOK;
}

void ss_m::set_shutdown_flag(bool clean)
{
    shutdown_clean = clean;
}


#if defined(__GNUC__) && __GNUC_MINOR__ > 6
ostream& operator<<(ostream& o, const smlevel_0::xct_state_t& xct_state)
{
// NOTE: these had better be kept up-to-date wrt the enumeration
// found in sm_base.h
    const char* names[] = {"xct_stale",
                        "xct_active",
                        "xct_prepared",
                        "xct_aborting",
                        "xct_chaining",
                        "xct_committing",
                        "xct_freeing_space",
                        "xct_ended"};

    o << names[xct_state];
    return o;
}
#endif


/*--------------------------------------------------------------*
 *  ss_m::gather_xct_stats()                            *
 *  Add the stats from this thread into the per-xct stats structure
 *  and return a copy in the given struct _stats.
 *  If reset==true,  clear the per-xct copy.
 *  Doing this has the side-effect of clearing the per-thread copy.
 *--------------------------------------------------------------*/
rc_t
ss_m::gather_xct_stats(sm_stats_info_t& _stats, bool reset)
{
    w_assert3(xct() != 0);
    xct_t& x = *xct();

    if(x.is_instrumented()) {
        DBGTHRD(<<"instrumented, reset= " << reset );
        // detach_xct adds the per-thread stats to the xct's stats,
        // then clears the per-thread stats so that
        // the next time some stats from this thread are gathered like this
        // into an xct, they aren't duplicated.
        // They are added to the global_stats before they are cleared, so
        // they don't get lost entirely.
        smthread_t::detach_xct(&x);
        smthread_t::attach_xct(&x);

        // Copy out the stats structure stored for this xct.
        _stats = x.const_stats_ref();

        if(reset) {
            DBGTHRD(<<"clearing stats " );
            // clear
            // NOTE!!!!!!!!!!!!!!!!!  NOT THREAD-SAFE:
            x.clear_stats();
        }
    } else {
        DBGTHRD(<<"xct not instrumented");
    }

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::gather_stats()                            *
 *  NOTE: the client is assumed to pass in a copy that's not
 *  referenced by any other threads right now.
 *  Resetting is not an option. Clients have to gather twice, then
 *  subtract.
 *  NOTE: you do not have to be in a transaction to call this.
 *--------------------------------------------------------------*/
rc_t
ss_m::gather_stats(sm_stats_info_t& _stats)
{
    class GatherSmthreadStats : public SmthreadFunc
    {
    public:
        GatherSmthreadStats(sm_stats_info_t &s) : _stats(s)
        {
            new (&_stats) sm_stats_info_t; // clear the stats
            // by invoking the constructor.
        };
        void operator()(const smthread_t& t)
        {
            t.add_from_TL_stats(_stats);
        }
        void compute() { _stats.compute(); }
    private:
        sm_stats_info_t &_stats;
    } F(_stats);

    //Gather all the threads' statistics into the copy given by
    //the client.
    // CS TODO: new thread stats mechanism!
    // smthread_t::for_each_smthread(F);
    // F.compute();

    // Now add in the global stats.
    // Global stats contain all the per-thread stats that were collected
    // before a per-thread stats structure was cleared.
    // (This happens when per-xct stats get gathered for instrumented xcts.)
    add_from_global_stats(_stats); // from finished threads and cleared stats
	_stats.compute();
    return RCOK;
}

#if W_DEBUG_LEVEL > 0
extern void dump_all_sm_stats();
void dump_all_sm_stats()
{
    static sm_stats_info_t s;
    W_COERCE(ss_m::gather_stats(s));
    std::cerr << s << endl;
}
#endif

ostream &
operator<<(ostream &o, const sm_stats_info_t &s)
{
    o << s.bfht;
    o << s.sm;
    return o;
}

