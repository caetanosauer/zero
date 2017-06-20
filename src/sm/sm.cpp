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
#include "xct_logger.h"


sm_tls_allocator smlevel_0::allocator;

memalign_allocator<char, smlevel_0::IO_ALIGN> smlevel_0::aligned_allocator;


bool         smlevel_0::shutdown_filthy = false;
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

restart_thread_t* smlevel_0::recovery = 0;

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

    if (_options.get_bool_option("sm_log_benchmark_start", false)) {
            Logger::log_sys<benchmark_start_log>();
    }

    // Log analysis provides info required to initialize vol_t
    Logger::log_sys<loganalysis_begin_log>();
    recovery = new restart_thread_t(_options);
    recovery->log_analysis();
    chkpt_t* chkpt_info = recovery->get_chkpt();

    bool logBasedRedo = _options.get_bool_option("sm_restart_log_based_redo", true);
    bool format = _options.get_bool_option("sm_format", false);

    ERROUT(<< "[" << timer.time_ms() << "] Initializing volume manager");

    vol = new vol_t(_options);

    ERROUT(<< "[" << timer.time_ms() << "] Initializing buffer manager");

    bf = new bf_tree_m(_options);
    if (! bf) {
        W_FATAL(eOUTOFMEMORY);
    }

    ERROUT(<< "[" << timer.time_ms() << "] Building volume manager caches");

    if (recovery->isInstant() || !logBasedRedo) {
        vol->build_caches(format, chkpt_info);
    }

    smlevel_0::statistics_enabled = _options.get_bool_option("sm_statistics", true);

    ERROUT(<< "[" << timer.time_ms() << "] Initializing buffer cleaner and other services");

    bt = new btree_m;
    if (! bt) { W_FATAL(eOUTOFMEMORY); }
    bt->construct_once();

    chkpt = new chkpt_m(_options, chkpt_info);
    if (! chkpt)  { W_FATAL(eOUTOFMEMORY); }

    SSM = this;

    smthread_t::mark_pin_count();

    do_prefetch = _options.get_bool_option("sm_prefetch", false);

    ERROUT(<< "[" << timer.time_ms() << "] Starting recovery thread");

    bf->post_init();

    if (!recovery->isInstant()) {
        recovery->wakeup();
        recovery->join();
        // metadata caches can only be constructed now
        if (logBasedRedo) { vol->build_caches(format, nullptr); }
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
    if (shutdown_filthy)
        shutdown_clean = false;
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

    lsn_t shutdown_lsn = log->durable_lsn();
    fs::path current_log_path = log->get_storage()->make_log_path(shutdown_lsn.hi());


    // get rid of all non-prepared transactions
    // First... disassociate me from any tx
    if(xct()) {
        smthread_t::detach_xct(xct());
    }

    // retire chkpt thread (calling take() directly still possible)
    chkpt->stop();

    ERROUT(<< "Terminating recovery manager");

    if (recovery) {
        if (shutdown_clean) {
            recovery->wakeup();
            recovery->join();
        }
        else { recovery->stop(); }
    }

    // remove all transactions, aborting them in case of clean shutdown
    xct_t::cleanup(shutdown_clean);
    w_assert1(xct_t::num_active_xcts() == 0);

    // log truncation requires clean shutdown
    bool truncate = _options.get_bool_option("sm_truncate_log", false);
    if (shutdown_clean || truncate) {
        ERROUT(<< "SM performing clean shutdown");

        W_COERCE(log->flush_all());
        do {
            bf->wakeup_cleaner(true, 1 /* wait for 1 full round */);
        } while (bf->has_dirty_frames());
        smthread_t::check_actual_pin_count(0);

        if (truncate) { W_COERCE(_truncate_log()); }
        else { chkpt->take(); }

        ERROUT(<< "All pages cleaned successfully");
    }
    else {
        ERROUT(<< "SM performing dirty shutdown");
    }

    // Stop cleaner and evictioner
    bf->shutdown();

    delete chkpt; chkpt = 0;

    if (recovery) {
        delete recovery;
        recovery = 0;
    }

    ERROUT(<< "Terminating log archiver");
    if (logArchiver) { logArchiver->shutdown(); }

    ERROUT(<< "Terminating buffer manager");
    delete bf; bf = 0;

    ERROUT(<< "Terminating other services");
    lm->assert_empty(); // no locks should be left
    bt->destruct_once();
    delete bt; bt = 0; // btree manager
    delete lm; lm = 0;

    if(logArchiver) {
        delete logArchiver; // LL: decoupled cleaner in bf still needs archiver
        logArchiver = 0;    //     so we delete it only after bf is gone
    }

    ERROUT(<< "Terminating volume");
    vol->shutdown();
    delete vol; vol = 0;

    ERROUT(<< "Terminating log manager");
    log->shutdown();
    delete log; log = 0;


     if (shutdown_filthy) {
         ERROUT(<< "Executing Shutdown Filthy");
         auto offset = shutdown_lsn.lo();
         resize_file(current_log_path, offset);
         if ((offset % partition_t::XFERSIZE)> 0 )
    	     offset = (offset/ partition_t::XFERSIZE + 1) * partition_t::XFERSIZE;
         resize_file(current_log_path, offset);
     }

     shutdown_filthy = false;
     shutdown_clean = true;

     ERROUT(<< "SM shutdown complete!");
}

rc_t ss_m::_truncate_log()
{
    DBGTHRD(<< "Truncating log on LSN " << log->durable_lsn());

    // Wait for cleaner to finish its current round
    W_DO(log->flush_all());

    // CS this first archiveUntilLSN() is only needed if we want to truncate
    // the log archive. Since I never actually used this, it's commented out.
    // if (logArchiver) {
    //     logArchiver->archiveUntilLSN(log->durable_lsn());
    //     if (_options.get_bool_option("sm_truncate_archive", false)) {
    //         unsigned replFactor =
    //             _options.get_int_option("sm_archiver_replication_factor", 0);
    //         logArchiver->getIndex()->deleteRuns(replFactor);
    //     }
    // }

    // create new, empty partition on log
    W_DO(log->truncate());
    // CS TODO: little hack -- empty log causes file not found in archiveUntilLSN
    Logger::log_sys<comment_log>("o hi there");
    W_DO(log->flush_all());

    // generate an empty log archive run to cover the new durable LSN
    if(logArchiver) {
        unsigned replFactor =
            _options.get_int_option("sm_archiver_replication_factor", 1);
        logArchiver->archiveUntilLSN(log->durable_lsn());
        logArchiver->getIndex()->deleteRuns(replFactor);
    }

    // this should be an "empty" checkpoint
    chkpt->take();

    log->get_storage()->delete_old_partitions();

    return RCOK;
}

void ss_m::set_shutdown_flag(bool clean)
{
    shutdown_clean = clean;
}

void ss_m::set_shutdown_filthy(bool filthy)
{
    shutdown_filthy = filthy;
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
        sm_stats_t*             _stats, // allocated by caller
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
    sm_stats_t *stats, int timeout)
{
    tid_t tid;
    W_DO (_begin_xct(stats, tid, timeout, true, single_log_sys_xct));
    return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::commit_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::commit_xct(sm_stats_t*& _stats, bool lazy,
                 lsn_t* plastlsn)
{

    W_DO(_commit_xct(_stats, lazy, plastlsn));

    return RCOK;
}

rc_t
ss_m::commit_sys_xct()
{
    sm_stats_t *_stats = NULL;
    W_DO(_commit_xct(_stats, true, NULL)); // always lazy commit
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::commit_xct()                                          *
 *--------------------------------------------------------------*/
rc_t
ss_m::commit_xct(bool lazy, lsn_t* plastlsn)
{

    sm_stats_t*             _stats=0;
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
ss_m::abort_xct(sm_stats_t*&             _stats)
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
    sm_stats_t*             _stats=0;

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
 *  ss_m::num_active_xcts()                            *
 *--------------------------------------------------------------*/
uint32_t
ss_m::num_active_xcts()
{
    return xct_t::num_active_xcts();
}
/*--------------------------------------------------------------*
 *  ss_m::tid_to_xct()                                *
 *--------------------------------------------------------------*/
xct_t* ss_m::tid_to_xct(const tid_t& tid)
{
    return xct_t::look_up(tid);
}

/*--------------------------------------------------------------*
 *  ss_m::xct_to_tid()                                *
 *--------------------------------------------------------------*/
tid_t ss_m::xct_to_tid(const xct_t* x)
{
    w_assert0(x != NULL);
    return x->tid();
}

/*--------------------------------------------------------------*
 *  ss_m::dump_xcts()                                           *
 *--------------------------------------------------------------*/
rc_t ss_m::dump_xcts(ostream& o)
{
    xct_t::dump(o);
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
ss_m::chain_xct( sm_stats_t*&  _stats, bool lazy)
{
    W_DO( _chain_xct(_stats, lazy) );
    return RCOK;
}
rc_t
ss_m::chain_xct(bool lazy)
{
    sm_stats_t        *_stats = 0;
    W_DO( _chain_xct(_stats, lazy) );
    /*
     * throw away the _stats, since user isn't harvesting...
     */
    delete _stats;
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::checkpoint()
 *  For debugging, smsh
 *--------------------------------------------------------------*/
rc_t
ss_m::checkpoint()
{
    // Just kick the chkpt thread
    chkpt->take();
    return RCOK;
}

rc_t
ss_m::activate_archiver()
{
    if (logArchiver) {
        logArchiver->activate(lsn_t::null, false);
    }
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::dump_buffers()                            *
 *  For debugging, smsh
 *--------------------------------------------------------------*/
rc_t
ss_m::dump_buffers(ostream &o)
{
    bf->debug_dump(o);
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::config_info()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::config_info(sm_config_info_t& info) {
    info.page_size = ss_m::page_sz;

    //however, fixable_page_h.space.acquire aligns() the whole mess (hdr + record)
    //which rounds up the space needed, so.... we have to figure that in
    //here: round up then subtract one aligned entity.
    //
    // OK, now that _data is already aligned, we don't have to
    // lose those 4 bytes.
    info.lg_rec_page_space = btree_page::data_sz;
    info.buffer_pool_size = bf->get_block_cnt() * ss_m::page_sz / 1024;
    info.max_btree_entry_size  = btree_m::max_entry_size();
    info.exts_on_page  = 0;
    info.pages_per_ext = smlevel_0::ext_sz;

    info.logging  = (ss_m::log != 0);

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::sync_log()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::sync_log(bool block)
{
    return log? log->flush_all(block) : RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::flush_until()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::flush_until(lsn_t& anlsn, bool block)
{
  return log->flush(anlsn, block);
}

/*--------------------------------------------------------------*
 *  ss_m::get_curr_lsn()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::get_curr_lsn(lsn_t& anlsn)
{
  anlsn = log->curr_lsn();
  return (RCOK);
}

/*--------------------------------------------------------------*
 *  ss_m::get_durable_lsn()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::get_durable_lsn(lsn_t& anlsn)
{
  anlsn = log->durable_lsn();
  return (RCOK);
}

void ss_m::dump_page_lsn_chain(std::ostream &o) {
    dump_page_lsn_chain(o, 0, lsn_t::max);
}
void ss_m::dump_page_lsn_chain(std::ostream &o, const PageID &pid) {
    dump_page_lsn_chain(o, pid, lsn_t::max);
}
void ss_m::dump_page_lsn_chain(std::ostream &o, const PageID &pid, const lsn_t &max_lsn) {
    // using static method since restart_thread_t is not guaranteed to be active
    restart_thread_t::dump_page_lsn_chain(o, pid, max_lsn);
}

rc_t ss_m::verify_volume(
    int hash_bits, verify_volume_result &result)
{
    W_DO(btree_m::verify_volume(hash_bits, result));
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


/*--------------------------------------------------------------*
 *  ss_m::dump_locks()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::dump_locks(ostream &o)
{
    lm->dump(o);
    return RCOK;
}

rc_t
ss_m::dump_locks() {
  return dump_locks(std::cout);
}



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
 *  ss_m::_begin_xct(sm_stats_t *_stats, int timeout) *
 *
 * @param[in] _stats  If called by begin_xct without a _stats, then _stats is NULL here.
 *                    If not null, the transaction is instrumented.
 *                    The stats structure may be returned to the
 *                    client through the appropriate version of
 *                    commit_xct, abort_xct, prepare_xct, or chain_xct.
 *--------------------------------------------------------------*/
rc_t
ss_m::_begin_xct(sm_stats_t *_stats, tid_t& tid, int timeout, bool sys_xct,
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
        sm_stats_t* stats,
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
ss_m::_commit_xct(sm_stats_t*& _stats, bool lazy,
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
    }
    bool was_sys_xct = x.is_sys_xct();
    delete xp;
    w_assert3(was_sys_xct || xct() == 0);

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_chain_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_chain_xct(
        sm_stats_t*&  _stats, /* pass in a new one, get back the old */
        bool lazy)
{
    sm_stats_t*  new_stats = _stats;
    w_assert3(xct() != 0);
    xct_t* x = xct();

    W_DO( x->chain(lazy) );
    w_assert3(xct() == x);
    if(x->is_instrumented()) {
        _stats = x->steal_stats();
    }
    x->give_stats(new_stats);

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_abort_xct()                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_abort_xct(sm_stats_t*&             _stats)
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
ss_m::gather_xct_stats(sm_stats_t& _stats, bool reset)
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
ss_m::gather_stats(sm_stats_t& _stats)
{
    class GatherSmthreadStats
    {
    public:
        GatherSmthreadStats(sm_stats_t &s) : _stats(s)
        {
            _stats.fill(0);
        };
        void operator()(sm_stats_t& st)
        {
            for (size_t i = 0; i < st.size(); i++) {
                _stats[i] += st[i];
            }
        }
    private:
        sm_stats_t &_stats;
    } F(_stats);

    //Gather all the threads' statistics into the copy given by
    //the client.
    // CS TODO: new thread stats mechanism!
    // smthread_t::for_each_smthread(F);
    smthread_t::for_each_thread_stats(F);

    // Now add in the global stats.
    // Global stats contain all the per-thread stats that were collected
    // before a per-thread stats structure was cleared.
    // (This happens when per-xct stats get gathered for instrumented xcts.)
    add_from_global_stats(_stats); // from finished threads and cleared stats
    return RCOK;
}

extern void dump_all_sm_stats();
void dump_all_sm_stats()
{
    static sm_stats_t s;
    W_COERCE(ss_m::gather_stats(s));
    print_sm_stats(s, std::cerr);
}


extern "C" {
/* Debugger-callable functions to dump various SM tables. */

    void        sm_dumplocks()
    {
        if (smlevel_0::lm) {
                W_IGNORE(ss_m::dump_locks(cout));
        }
        else
                cout << "no smlevel_0::lm" << endl;
        cout << flush;
    }

    void   sm_dumpxcts()
    {
        W_IGNORE(ss_m::dump_xcts(cout));
        cout << flush;
    }

    void        sm_dumpbuffers()
    {
        W_IGNORE(ss_m::dump_buffers(cout));
        cout << flush;
    }
}
