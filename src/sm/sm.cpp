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

// Certain operations have to exclude xcts
static srwlock_t          _begin_xct_mutex;

BackupManager* smlevel_0::bk = 0;
vol_t* smlevel_0::vol = 0;
bf_tree_m* smlevel_0::bf = 0;
log_core* smlevel_0::log = 0;
LogArchiver* smlevel_0::logArchiver = 0;

lock_m* smlevel_0::lm = 0;

char smlevel_0::zero_page[page_sz];

chkpt_m* smlevel_0::chkpt = 0;

restart_m* smlevel_0::recovery = 0;

btree_m* smlevel_0::bt = 0;

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

    bt = new btree_m;
    if (! bt) {
        W_FATAL(eOUTOFMEMORY);
    }
    bt->construct_once();

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
    bt->destruct_once();
    delete bt; bt = 0; // btree manager
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


/*--------------------------------------------------------------*
 *  ss_m::begin_xct()                                *
 *
 *\details
 *
 * You cannot start a transaction while any thread is :
 * - mounting or unmounting a device, or
 * - creating or destroying a volume.
 *--------------------------------------------------------------*/
rc_t
ss_m::begin_xct(
        sm_stats_info_t*             _stats, // allocated by caller
        int timeout)
{
    tid_t tid;
    W_DO(_begin_xct(_stats, tid, timeout));
    return RCOK;
}
rc_t
ss_m::begin_xct(int timeout)
{
    tid_t tid;
    W_DO(_begin_xct(0, tid, timeout));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::begin_xct() - for Markos' tests                       *
 *--------------------------------------------------------------*/
rc_t
ss_m::begin_xct(tid_t& tid, int timeout)
{
    W_DO(_begin_xct(0, tid, timeout));
    return RCOK;
}

rc_t ss_m::begin_sys_xct(bool single_log_sys_xct,
    sm_stats_info_t *stats, int timeout)
{
    tid_t tid;
    W_DO (_begin_xct(stats, tid, timeout, true, single_log_sys_xct));
    return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::commit_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::commit_xct(sm_stats_info_t*& _stats, bool lazy,
                 lsn_t* plastlsn)
{

    W_DO(_commit_xct(_stats, lazy, plastlsn));

    return RCOK;
}

rc_t
ss_m::commit_sys_xct()
{
    sm_stats_info_t *_stats = NULL;
    W_DO(_commit_xct(_stats, true, NULL)); // always lazy commit
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::commit_xct_group()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::commit_xct_group(xct_t *list[], int listlen)
{
    W_DO(_commit_xct_group(list, listlen));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::commit_xct()                                          *
 *--------------------------------------------------------------*/
rc_t
ss_m::commit_xct(bool lazy, lsn_t* plastlsn)
{

    sm_stats_info_t*             _stats=0;
    W_DO(_commit_xct(_stats,lazy,plastlsn));
    /*
     * throw away the _stats, since user isn't harvesting...
     */
    delete _stats;

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::abort_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::abort_xct(sm_stats_info_t*&             _stats)
{

    // Temp removed for debugging purposes only
    // want to see what happens if the abort proceeds (scripts/alloc.10)
    // bool was_sys_xct = xct() && xct()->is_sys_xct();
    W_DO(_abort_xct(_stats));

    return RCOK;
}
rc_t
ss_m::abort_xct()
{
    sm_stats_info_t*             _stats=0;

    W_DO(_abort_xct(_stats));
    /*
     * throw away _stats, since user is not harvesting them
     */
    delete _stats;

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::save_work()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::save_work(sm_save_point_t& sp)
{
    // For now, consider this a read/write operation since you
    // wouldn't be doing this unless you intended to write and
    // possibly roll back.
    W_DO( _save_work(sp) );
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::rollback_work()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::rollback_work(const sm_save_point_t& sp)
{
    W_DO( _rollback_work(sp) );
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::state_xct()                                *
 *--------------------------------------------------------------*/
ss_m::xct_state_t ss_m::state_xct(const xct_t* x)
{
    w_assert3(x != NULL);
    return x->state();
}

/*--------------------------------------------------------------*
 *  ss_m::chain_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::chain_xct( sm_stats_info_t*&  _stats, bool lazy)
{
    W_DO( _chain_xct(_stats, lazy) );
    return RCOK;
}
rc_t
ss_m::chain_xct(bool lazy)
{
    sm_stats_info_t        *_stats = 0;
    W_DO( _chain_xct(_stats, lazy) );
    /*
     * throw away the _stats, since user isn't harvesting...
     */
    delete _stats;
    return RCOK;
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

lil_global_table* ss_m::get_lil_global_table() {
    if (lm) {
        return lm->get_lil_global_table();
    } else {
        return NULL;
    }
}

rc_t ss_m::lock(const lockid_t& n, const okvl_mode& m,
           bool check_only, int timeout)
{
    W_DO( lm->lock(n.hash(), m, true, true, !check_only, NULL, timeout) );
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_begin_xct(sm_stats_info_t *_stats, int timeout) *
 *
 * @param[in] _stats  If called by begin_xct without a _stats, then _stats is NULL here.
 *                    If not null, the transaction is instrumented.
 *                    The stats structure may be returned to the
 *                    client through the appropriate version of
 *                    commit_xct, abort_xct, prepare_xct, or chain_xct.
 *--------------------------------------------------------------*/
rc_t
ss_m::_begin_xct(sm_stats_info_t *_stats, tid_t& tid, int timeout, bool sys_xct,
    bool single_log_sys_xct)
{
    w_assert1(!single_log_sys_xct || sys_xct); // SSX is always system-transaction

    // system transaction can be a nested transaction, so
    // xct() could be non-NULL
    if (!sys_xct && xct() != NULL) {
        return RC (eINTRANS);
    }

    xct_t* x;
    if (sys_xct) {
        x = xct();
        if (single_log_sys_xct && x) {
            // in this case, we don't need an independent transaction object.
            // we just piggy back on the outer transaction
            if (x->is_piggy_backed_single_log_sys_xct()) {
                // SSX can't nest SSX, but we can chain consecutive SSXs.
                ++(x->ssx_chain_len());
            } else {
                x->set_piggy_backed_single_log_sys_xct(true);
            }
            tid = x->tid();
            return RCOK;
        }
        x = _new_xct(_stats, timeout, sys_xct, single_log_sys_xct);
    } else {
        spinlock_read_critical_section cs(&_begin_xct_mutex);
        x = _new_xct(_stats, timeout, sys_xct);
        if(log) {
            // This transaction will make no events related to LSN
            // smaller than this. Used to control garbage collection, etc.
            log->get_oldest_lsn_tracker()->enter(reinterpret_cast<uintptr_t>(x), log->curr_lsn());
        }
    }

    if (!x)
        return RC(eOUTOFMEMORY);

    w_assert3(xct() == x);
    w_assert3(x->state() == xct_t::xct_active);
    tid = x->tid();

    return RCOK;
}

xct_t* ss_m::_new_xct(
        sm_stats_info_t* stats,
        int timeout,
        bool sys_xct,
        bool single_log_sys_xct)
{
    return new xct_t(stats, timeout, sys_xct, single_log_sys_xct, false);
}

/*--------------------------------------------------------------*
 *  ss_m::_commit_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_commit_xct(sm_stats_info_t*& _stats, bool lazy,
                  lsn_t* plastlsn)
{
    w_assert3(xct() != 0);
    xct_t* xp = xct();
    xct_t& x = *xp;
    DBGOUT5(<<"commit " << ((char *)lazy?" LAZY":"") << x );

    if (x.is_piggy_backed_single_log_sys_xct()) {
        // then, commit() does nothing
        // It just "resolves" the SSX on piggyback
        if (x.ssx_chain_len() > 0) {
            --x.ssx_chain_len(); // multiple SSXs on piggyback
        } else {
            x.set_piggy_backed_single_log_sys_xct(false);
        }
        return RCOK;
    }

    w_assert3(x.state()==xct_active);
    w_assert1(x.ssx_chain_len() == 0);

    W_DO( x.commit(lazy,plastlsn) );

    if(x.is_instrumented()) {
        _stats = x.steal_stats();
        _stats->compute();
    }
    bool was_sys_xct = x.is_sys_xct();
    delete xp;
    w_assert3(was_sys_xct || xct() == 0);

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_commit_xct_group( xct_t *list[], len)                *
 *--------------------------------------------------------------*/

rc_t
ss_m::_commit_xct_group(xct_t *list[], int listlen)
{
    // We don't care what, if any, xct is attached
    xct_t* x = xct();
    if(x) smthread_t::detach_xct(x);

    DBG(<<"commit group " );

    // 1) verify either all are participating in 2pc
    // in same way (not, prepared, not prepared)
    // Some may be read-only
    // 2) do the first part of the commit for each one.
    // 3) write the group-commit log record.
    // (TODO: we should remove the read-only xcts from this list)
    //
    int participating=0;
    for(int i=0; i < listlen; i++) {
        // verify list
        x = list[i];
        w_assert3(x->state()==xct_active);
    }
    if(participating > 0 && participating < listlen) {
        // some transaction is not participating in external 2-phase commit
        // but others are. Don't delete any xcts.
        // Leave it up to the server to decide how to deal with this; it's
        // a server error.
        return RC(eNOTEXTERN2PC);
    }

    for(int i=0; i < listlen; i++) {
        x = list[i];
        /*
         * Do a partial commit -- all but logging the
         * commit and freeing the locks.
         */
        smthread_t::attach_xct(x);
        {
        W_DO( x->commit_as_group_member() );
        }
        w_assert1(smthread_t::xct() == NULL);

        if(x->is_instrumented()) {
            // remove the stats, delete them
            sm_stats_info_t* _stats = x->steal_stats();
            delete _stats;
        }
    }

    // Write group commit record
    // Failure here requires that the server abort them individually.
    // I don't know why the compiler won't convert from a
    // non-const to a const xct_t * list.
    W_DO(xct_t::group_commit((const xct_t **)list, listlen));

    // Destroy the xcts
    for(int i=0; i < listlen; i++) {
        /*
         *  Free all locks for each transaction
         */
        x = list[i];
        w_assert1(smthread_t::xct() == NULL);
        smthread_t::attach_xct(x);
        W_DO(x->commit_free_locks());
        smthread_t::detach_xct(x);
        delete x;
    }
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_chain_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_chain_xct(
        sm_stats_info_t*&  _stats, /* pass in a new one, get back the old */
        bool lazy)
{
    sm_stats_info_t*  new_stats = _stats;
    w_assert3(xct() != 0);
    xct_t* x = xct();

    W_DO( x->chain(lazy) );
    w_assert3(xct() == x);
    if(x->is_instrumented()) {
        _stats = x->steal_stats();
        _stats->compute();
    }
    x->give_stats(new_stats);

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_abort_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_abort_xct(sm_stats_info_t*&             _stats)
{
    w_assert3(xct() != 0);
    xct_t* xp = xct();
    xct_t& x = *xp;

    // if this is "piggy-backed" ssx, just end the status
    if (x.is_piggy_backed_single_log_sys_xct()) {
        x.set_piggy_backed_single_log_sys_xct(false);
        return RCOK;
    }

    bool was_sys_xct W_IFDEBUG3(= x.is_sys_xct());

    W_DO( x.abort(true /* save _stats structure */) );
    if(x.is_instrumented()) {
        _stats = x.steal_stats();
        _stats->compute();
    }

    delete xp;
    w_assert3(was_sys_xct || xct() == 0);

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::save_work()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_save_work(sm_save_point_t& sp)
{
    w_assert3(xct() != 0);
    xct_t* x = xct();

    W_DO(x->save_point(sp));
    sp._tid = x->tid();
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::rollback_work()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::_rollback_work(const sm_save_point_t& sp)
{
    w_assert3(xct() != 0);
    xct_t* x = xct();
    if (sp._tid != x->tid())  {
        return RC(eBADSAVEPOINT);
    }
    W_DO( x->rollback(sp) );
    return RCOK;
}


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
#ifdef COMMENT
        /* help debugging sort stuff -see also code in bf.cpp  */
        {
            // print -grot
            extern int bffix_SH[];
            extern int bffix_EX[];
        FIXME: THIS CODE IS ROTTEN AND OUT OF DATE WITH tag_t!!!
            static const char *names[] = {
                "t_bad_p",
                "t_alloc_p",
                "t_stnode_p",
                "t_btree_p",
                "none"
                };
            cout << "PAGE FIXES " <<endl;
            for (int i=0; i<=14; i++) {
                    cout  << names[i] << "="
                        << '\t' << bffix_SH[i] << "+"
                    << '\t' << bffix_EX[i] << "="
                    << '\t' << bffix_EX[i] + bffix_SH[i]
                     << endl;

            }
            int sumSH=0, sumEX=0;
            for (int i=0; i<=14; i++) {
                    sumSH += bffix_SH[i];
                    sumEX += bffix_EX[i];
            }
            cout  << "TOTALS" << "="
                        << '\t' << sumSH<< "+"
                    << '\t' << sumEX << "="
                    << '\t' << sumSH+sumEX
                     << endl;
        }
        if(reset) {
            extern int bffix_SH[];
            extern int bffix_EX[];
            for (int i=0; i<=14; i++) {
                bffix_SH[i] = 0;
                bffix_EX[i] = 0;
            }
        }
#endif /* COMMENT */
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

