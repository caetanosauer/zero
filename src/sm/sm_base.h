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

/*<std-header orig-src='shore' incl-file-exclusion='SM_BASE_H'>

 $Id: sm_base.h,v 1.158 2010/12/08 17:37:43 nhall Exp $

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

#ifndef SM_BASE_H
#define SM_BASE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <vector>
#include "sthread.h"
#include "basics.h"
#include <w_debug.h>
#include <sysdefs.h>
#include <vec_t.h>
#include <latch.h>
#if defined(SM_SOURCE)
/* Do not force this on VASs */
#include <sm_s.h>
#endif /* SM_SOURCE */
#include <smthread.h>
#include <tid_t.h>
#include "smstats.h"


/**\file sm_base.h
 * \ingroup Macros
 */

#include <climits>

class ErrLog;
class sm_stats_info_t;
class xct_t;
class xct_i;

class vol_t;
class BackupManager;
class bf_tree_m;
class comm_m;
class log_m;
class log_core;
class lock_m;
class LogArchiver;
class ticker_thread_t;

class tid_t;
class option_t;
class rid_t;
class lsn_t;

class sm_naive_allocator;
class sm_tls_allocator;

class chkpt_m;
class restart_m;
class btree_m;
class ss_m;

#ifndef        SM_EXTENTSIZE
#define        SM_EXTENTSIZE        8
#endif
#ifndef        SM_LOG_PARTITIONS
#define        SM_LOG_PARTITIONS        8
#endif

class w_rc_t;
typedef   w_rc_t        rc_t;


/**\cond skip
 * This structure collects the depth on construction
 * and checks that it matches the depth on destruction; this
 * is to ensure that we haven't forgotten to release
 * an anchor somewhere.
 *
 * We're defining the CHECK_NESTING_VARIABLES macro b/c
 * this work is spread out and we want to have 1 place to
 * determine whether it's turned on or off; don't want to
 * make the mistake of changing the debug level (on which
 * it depends) in only one of several places.
 *
 * NOTE: this doesn't work in a multi-threaded xct context.
 * That's b/c the check is too late -- once the count goes
 * to zero, another thread can change it and throw off all the
 * counts. To be sure, we'd have to use a TLS copy as well
 * as the common copy of these counts.
 *
 * This was on for debug level > 0 but it's been stable
 * enough to change it to > 2
 */
#if W_DEBUG_LEVEL > 2
#define CHECK_NESTING_VARIABLES 1
#else
#define CHECK_NESTING_VARIABLES 0
#endif
struct check_compensated_op_nesting {
#if CHECK_NESTING_VARIABLES
    xct_t* _xd;
    int _depth;
    int _line;
    const char *const _file;
    // static methods are so we can avoid having to
    // include xct.h here.
    static int compensated_op_depth(xct_t* xd, int dflt);

    check_compensated_op_nesting(xct_t* xd, int line, const char *const file)
    : _xd(xd),
    _depth(_xd? compensated_op_depth(_xd, 0) : 0),
    _line(line),
    _file(file)
    {
    }

    ~check_compensated_op_nesting() {
        if(_xd) {
            if( _depth != compensated_op_depth(_xd, _depth) ) {
                fprintf(stderr,
                    "th.%d check_compensated_op_nesting(%d,%s) depth was %d is %d\n",
                    sthread_t::me()->id,
                    _line, _file, _depth, compensated_op_depth(_xd, _depth));
            }


            w_assert0(_depth == compensated_op_depth(_xd, _depth));
        }
    }
#else
    check_compensated_op_nesting(xct_t*, int, const char *const) { }
#endif
};


/**\brief Encapsulates a few types uses in the API */
class smlevel_0 : public w_base_t {
public:
    // Give these enums names for doxygen purposes:
    enum error_constant_t { eNOERROR = 0, eFAILURE = -1 };
    enum sm_constant_t {
        page_sz = SM_PAGESIZE,        // page size (SM_PAGESIZE is set by makemake)
        ext_sz = SM_EXTENTSIZE,        // extent size
#if defined(_POSIX_PATH_MAX)
        max_devname = _POSIX_PATH_MAX,        // max length of unix path name
    // BEWARE: this might be larger than you want.  Array sizes depend on it.
    // The default might be small enough, e.g., 256; getconf() yields the upper
    // bound on this value.
#elif defined(MAXPATHLEN)
        max_devname = MAXPATHLEN,
#else
        max_devname = 1024,
#endif
        max_xct_thread = 20,        // max threads in a xct
        max_servers = 15,       // max servers to be connected with
        max_keycomp = 20,        // max key component (for btree)
        max_openlog = SM_LOG_PARTITIONS,        // max # log partitions

        /* XXX I want to propogate sthread_t::iovec_max here, but
           it doesn't work because of sm_app.h not including
           the thread package. */
        max_many_pages = 64,

        srvid_map_sz = (max_servers - 1) / 8 + 1,
        ext_map_sz_in_bytes = ((ext_sz + 7) / 8),

        dummy = 0
    };

    enum {
        max_rec_len = max_uint4
    };

    typedef sthread_base_t::fileoff_t fileoff_t;
    /*
     * Sizes-in-Kbytes for for things like volumes and devices.
     * A KB is assumes to be 1024 bytes.
     * Note: a different type was used for added type checking.
     */
    typedef sthread_t::fileoff_t smksize_t;
    typedef w_base_t::base_stat_t base_stat_t;

    /**\endcond skip */

    /*
     * rather than automatically aborting the transaction, when the
     * _log_warn_percent is exceeded, this callback is made, with a
     * pointer to the xct that did the writing, and with the
     * expectation that the result will be one of:
     * - return value == RCOK --> proceed
     * - return value == eUSERABORT --> victim to abort is given in the argument
     *
     * The server has the responsibility for choosing a victim and
     * for aborting the victim transaction.
     *
     */

    /**\brief Log space warning callback function type.
     *
     * For more details of how this is used, see the constructor ss_m::ss_m().
     *
     * Storage manager methods check the available log space.
     * If the log is in danger of filling to the point that it will be
     * impossible to abort a transaction, a
     * callback is made to the server.  The callback function is of this type.
     * The danger point is a threshold determined by the option sm_log_warn.
     *
     * The callback
     * function is meant to choose a victim xct and
     * tell if the xct should be
     * aborted by returning RC(eUSERABORT).
     *
     * Any other RC value is returned to the server through the call stack.
     *
     * The arguments:
     * @param[in] iter    Pointer to an iterator over all xcts.
     * @param[out] victim    Victim will be returned here. This is an in/out
     * paramter and is initially populated with the transaction that is
     * attached to the running thread.
     * @param[in] curr    Bytes of log consumed by active transactions.
     * @param[in] thresh   Threshhold just exceeded.
     * @param[in] logfile   Character string name of oldest file to archive.
     *
     *  This function must be careful not to return the same victim more
     *  than once, even though the callback may be called many
     *  times before the victim is completely aborted.
     *
     *  When this function has archived the given log file, it needs
     *  to notify the storage manager of that fact by calling
     *  ss_m::log_file_was_archived(logfile)
     */
    typedef w_rc_t (*LOG_WARN_CALLBACK_FUNC) (
            xct_i*      iter,
            xct_t *&    victim,
            fileoff_t   curr,
            fileoff_t   thresh,
            const char *logfile
        );
    /**\brief Callback function type for restoring an archived log file.
     *
     * @param[in] fname   Original file name (with path).
     * @param[in] needed   Partition number of the file needed.
     *
     *  An alternative to aborting a transaction (when the log fills)
     *  is to archive log files.
     *  The server can use the log directory name to locate these files,
     *  and may use the iterator and the static methods of xct_t to
     *  determine which log file(s) to archive.
     *
     *  Archiving and removing the older log files will work only if
     *  the server also provides a LOG_ARCHIVED_CALLBACK_FUNCTION
     *  to restore the
     *  archived log files when the storage manager needs them for
     *  rollback.
     *  This is the function type used for that purpose.
     *
     *  The function must locate the archived log file containing for the
     *  partition number \a num, which was a suffix of the original log file's
     *  name.
     *  The log file must be restored with its original name.
     */
    typedef    uint32_t partition_number_t;
    typedef w_rc_t (*LOG_ARCHIVED_CALLBACK_FUNC) (
            const char *fname,
            partition_number_t num
        );

    typedef w_rc_t (*RELOCATE_RECORD_CALLBACK_FUNC) (
	   vector<rid_t>&    old_rids,
           vector<rid_t>&    new_rids
       );


/**\cond skip */
    enum switch_t {
        ON = 1,
        OFF = 0
    };
/**\endcond skip */

    /**\brief Comparison types used in scan_index_i
     * \enum cmp_t
     * Shorthand for CompareOp.
     */
    enum cmp_t { bad_cmp_t=badOp, eq=eqOp,
                 gt=gtOp, ge=geOp, lt=ltOp, le=leOp };

    /**\enum concurrency_t
     * \brief
     * Lock granularities
     * \details
     * - t_cc_bad Illegal
     * - t_cc_none No locking
     * - t_cc_vol Volume-level locking
     * - t_cc_store Index-level locking
     * - t_cc_keyrange Key-range locking using fence-keys
     */
    enum concurrency_t {
        t_cc_bad,                // this is an illegal value
        t_cc_none,                // no locking
        t_cc_vol,
        t_cc_store,
        t_cc_keyrange,
        t_cc_append
    };

    /**\enum pg_policy_t
     * \brief
     * File-compaction policy for creating records.
     * \details
     * - t_append : append new record to file (preserve order)
     * - t_cache  : look in cache for pages with space for new record (does
     *              not preserve order)
     * - t_compact: keep file compact even if it means searching the file
     *              for space in which to create the file (does not preserve
     *              order)
     *
     * These are masks - the following combinations are sensible:
     *
     * - t_append                        -- preserve sort order
     * - t_cache | t_append              -- check the cache first,
     *                                      append if no luck
     * - t_cache | t_compact | t_append  -- append to file as a last resort
     */
    enum pg_policy_t {
        t_append        = 0x01, // retain sort order (cache 0 pages)
        t_cache        = 0x02, // look in n cached pgs
        t_compact        = 0x04 // scan file for space in pages

    };


/**\cond skip */

    /*
     * smlevel_0::operating_mode is always set to
     * ONE of these, but the function in_recovery() tests for
     * any of them, so we'll give them bit-mask values
     */
    enum operating_mode_t {
        t_not_started = 0,
        t_in_analysis = 0x1,
        t_in_redo = 0x2,
        t_in_undo = 0x4,
        t_forward_processing = 0x8   // System is opened for transaction
    };

    // smlevel_0::concurrent_restart_mode_t is used for concurrent restart when
    // the system is opened for user access after Log Analysis phase but the restart
    // continue running concurrently.  It is to expose the active restart phase.
    // It is not valid for pure on-demand restart operation (Instant Restart milestone 3).
    //
    enum concurrent_restart_mode_t {
        t_concurrent_before,  // before REDO and UNDO
        t_concurrent_redo,    // working on REDO
        t_concurrent_undo,    // working on UNDO
        t_concurrent_done     // after REDO and UNDO
    };

    // smlevel_0::restart_internal_mode_t is an internal setting, not exposed
    // to external callers.  It is used to control the internal logic in recovery
    // in order to test different implementations.
    // Multiple features could be turned on/off, so they are in bit-mast values.
    //
    // Only the Recovery related operations should check these values.
    // The only place to get/set the value is in 'sm.cpp' (restart_internal_mode),
    // the setting is determined during system startup and cannot be changed
    // dynamiclly, because we cannot change restart behavior in the middle
    // of recovery process
    // The actual recovery mode is determined by startup option 'sm_restart',
    // no recompile required.
    // If the startup option is not set, the default setting is to use the initialization
    // value set in sm.cpp

    enum restart_internal_mode_t {
        t_restart_disable = 0,
        t_restart_serial = 0x1,            // M1 implementation:
                                           //    System is not opened until Recovery completed
        t_restart_redo_log = 0x2,          // M1 traditional implementation:
                                           //    REDO is forward log scan driven
        t_restart_undo_reverse = 0x4,      // M1 traditional implementation:
                                           //    UNDO is using reverse order with heap

        t_restart_concurrent_log = 0x8,    // M2 implementation:
                                           //    System is opened after Log Analysis.
                                           //    Using commit_lsn for new transactions
        t_restart_redo_page = 0x10,        // M2 implementation:
                                           //    REDO is page driven using Single-Page-Recovery with minimal logging
        t_restart_redo_full_logging = 0x20,// M2 implementation:
                                           //    REDO is page driven using Single-Page-Recovery with full logging
        t_restart_undo_txn = 0x40,         // M2 implementation:
                                           //    UNDO is transaction driven
        t_restart_redo_delay = 0x80,       // M2 Testing hook:
                                           //    Internal delay before REDO phase
                                           //    only if we have actual REDO work
        t_restart_undo_delay = 0x100,      // M2 Testing hook:
                                           //    Internal delay before UNDO phase
                                           //    only if we have actual UNDO work

        t_restart_concurrent_lock = 0x200, // M3, M4 and M5 implementation:
                                           //    System is opened after Log Analysis.
                                           //    Using lock acquisition for new transactions
        t_restart_redo_demand = 0x400,     // M3 implementation:
                                           //    REDO is on-demand using Single-Page-Recovery with minimal logging
        t_restart_undo_demand = 0x800,     // M3 implementation:
                                           //    UNDO is on-demand using user transaction driven

        t_restart_redo_mix = 0x1000,       // M4 and M5 implementation:
                                           //    REDO is using both page driven and
                                           //    on-demand using Single-Page-Recovery with minimal logging
        t_restart_undo_mix = 0x2000,       // M4 and M5 implementation:
                                           //    UNDO is using both transaction driven and
                                           //    on-demand using user transaction driven

        t_restart_aries_open = 0x4000,     // M5 implementation:
                                           // ARIES implementation, using lock conflict
                                           // open the system after REDO but before UNDO
        t_restart_alt_rebalance = 0x8000,  // Alternative implementation for page driven REDO with
                                           // page rebalance operation using Singe Page Recovery (M2 - M5)
                                           // The alternative implementation uses self-contained
                                           // log record instead of recursive calls

    };
    static restart_internal_mode_t restart_internal_mode;

    // Set of functions to check individual Restart implementations
    // No setting because the bit values are set staticlly, not dynamiclly
    static bool use_serial_restart()
    {
        // Restart M1
        return ((restart_internal_mode & t_restart_serial ) !=0);
    }
    static bool use_redo_log_restart()
    {
        // Restart M1
        return ((restart_internal_mode & t_restart_redo_log ) !=0);
    }
    static bool use_undo_reverse_restart()
    {
        // Restart M1
        return ((restart_internal_mode & t_restart_undo_reverse ) !=0);
    }

    static bool use_concurrent_commit_restart()
    {
        // Restart M2
        return ((restart_internal_mode & t_restart_concurrent_log ) !=0);
    }
    static bool use_redo_page_restart()
    {
        // Restart M2
        return ((restart_internal_mode & t_restart_redo_page ) !=0);
    }
    static bool use_redo_full_logging_restart()
    {
        // Restart M2
        return ((restart_internal_mode & t_restart_redo_full_logging ) !=0);
    }
    static bool use_undo_txn_restart()
    {
        // Restart M2
        return ((restart_internal_mode & t_restart_undo_txn ) !=0);
    }
    static bool use_redo_delay_restart()
    {
        // Restart M2, testing purpose
        return ((restart_internal_mode & t_restart_redo_delay ) !=0);
    }
    static bool use_undo_delay_restart()
    {
        // Restart M2, testing purpose
        return ((restart_internal_mode & t_restart_undo_delay ) !=0);
    }

    static bool use_concurrent_lock_restart()
    {
        // Restart M3, M4 and M5
        return ((restart_internal_mode & t_restart_concurrent_lock ) !=0);
    }
    static bool use_redo_demand_restart()
    {
        // Restart M3
        return ((restart_internal_mode & t_restart_redo_demand ) !=0);
    }
    static bool use_undo_demand_restart()
    {
        // Restart M3
        return ((restart_internal_mode & t_restart_undo_demand ) !=0);
    }

    static bool use_redo_mix_restart()
    {
        // Restart M4 and M5
        return ((restart_internal_mode & t_restart_redo_mix ) !=0);
    }
    static bool use_undo_mix_restart()
    {
        // Restart M4 and M5
        return ((restart_internal_mode & t_restart_undo_mix ) !=0);
    }

    static bool use_aries_restart()
    {
        // Restart M5
        return ((restart_internal_mode & t_restart_aries_open ) !=0);
    }

    static bool use_alt_rebalance()
    {
        // Restart M2 - M5
        return ((restart_internal_mode & t_restart_alt_rebalance ) !=0);
    }


    static void  add_to_global_stats(const sm_stats_info_t &from);
    static void  add_from_global_stats(sm_stats_info_t &to);

    static BackupManager* bk;
    static vol_t* vol;
    static bf_tree_m* bf;
    static lock_m* lm;

    static log_m* log;
    static log_core* clog;
    static LogArchiver* logArchiver;

    static ticker_thread_t* _ticker;

    static LOG_WARN_CALLBACK_FUNC log_warn_callback;
    static LOG_ARCHIVED_CALLBACK_FUNC log_archived_callback;
    static fileoff_t              log_warn_trigger;
    static int                    log_warn_exceed_percent;

    static int    dcommit_timeout; // to convey option to coordinator,
                                   // if it is created by VAS

    static ErrLog* errlog;

    // TODO: allocator flag should be specified by cmake
#define USE_TLS_ALLOCATOR

#ifdef USE_TLS_ALLOCATOR
    static sm_tls_allocator allocator;
#else
    static sm_naive_allocator allocator;
#endif

    static bool         shutdown_clean;
    static bool         shutting_down;
    static bool         logging_enabled;
    static bool         lock_caching_default;
    static bool         do_prefetch;
    static bool         statistics_enabled;

    static lsn_t        commit_lsn;      // commit_lsn is for use_concurrent_commit_restart() only
                                         // this is the validation lsn for all concurrent user txns

    // The following variables are used by concurrent recovery process only
    // they should be stored in class 'restart_m'
    // Currently resides here for prototype converient access only
    static lsn_t        redo_lsn;        // redo_lsn is used by child thread as the start scanning point for redo
    static lsn_t        last_lsn;        // last_lsn is used by page driven REDO operation Single-Page-Recovery emlsn if encounter
                                         // a virgin or corrupted page
    static uint32_t     in_doubt_count;  // in_doubt_count is used to child thread during the REDO phase


    static operating_mode_t operating_mode;
    static bool in_recovery() {
        return ((operating_mode &
                (t_in_redo | t_in_undo | t_in_analysis)) !=0); }
    static bool in_recovery_analysis() {
        return ((operating_mode & t_in_analysis) !=0); }
    static bool in_recovery_undo() {
        // Valid for use_serial_restart() only
        // If concurrent recovery, system is opened after Log Analysis
        // and the operating mode changed to t_forward_processing after Log Analysis
        return ((operating_mode & t_in_undo ) !=0); }
    static bool in_recovery_redo() {
        // Valid for use_serial_restart() only
        // If concurrent recovery, system is opened after Log Analysis
        // and the operating mode changed to t_forward_processing after Log Analysis
        return ((operating_mode & t_in_redo ) !=0); }

    static bool before_recovery() {
        if (t_not_started == operating_mode)
            return true;
        else
            return false;
        }

    // This variable controls checkpoint frequency.
    // Checkpoints are taken every chkpt_displacement bytes
    // written to the log.
    static fileoff_t chkpt_displacement;

    // This is a zeroed page for use wherever initialized memory
    // is needed.
    static char zero_page[page_sz];

    /// NB: this had better match sm_store_property_t (sm_int_3.h) !!!
    // or at least be converted properly every time we come through the API
    enum store_flag_t {
        /// No flags means a store is not currently allocated
        st_unallocated = 0,

        st_regular     = 0x01, // fully logged
        st_tmp         = 0x02, // space logging only,
                               // file destroy on dismount/restart
        st_load_file   = 0x04, // not stored in the stnode_t,
                               // only passed down to io_m and then
                               // converted to tmp and added to the
                               // list of load files for the xct.  no
                               // longer needed
        st_insert_file = 0x08, // stored in stnode, but not on page.
                               // new pages are saved as tmp, old pages as regular.
        st_empty       = 0x100 // store might be empty - used ONLY
                               // as a function argument, NOT stored
                               // persistently.  Nevertheless, it's
                               // defined here to be sure that if
                               // other store flags are added, this
                               // doesn't conflict with them.
    };

    /*
     * for use by set_store_deleting_log;
     * type of operation to perform on the stnode
     */
    enum store_operation_t {
            t_delete_store,
            t_create_store,
            t_set_deleting,
            t_set_store_flags,
            t_set_root};

    enum store_deleting_t  {
            t_not_deleting_store = 0,  // must be 0: code assumes it
            t_deleting_store,
            t_unknown_deleting};

    // CS: stuff below was from smlevels 1-4

    /**\todo xct_state_t */
    // The numeric equivalents of state are not significant; they are
    // given here only for convenience in debugging/grepping
    // Well, their ORDER is significant, so that you can only
    // change state to a larger state with change_state().
    enum xct_state_t {  xct_stale = 0x0,
                        xct_active = 0x1,  // active or rolling back in
                                           // doing rollback_work
                                           // It is also used in Recovery for loser transaction
                                           // because it is using the standard rollback logic
                                           // for loser txn, check the _loser_xct flag
                                           // in xct_t
                        xct_chaining = 0x3,
                        xct_committing = 0x4,
                        xct_aborting = 0x5,  // normal transaction abort
                        xct_freeing_space = 0x6,
                        xct_ended = 0x7
    };

    // xct implementation
    enum xct_impl_t {
        XCT_TRADITIONAL = 0,
        XCT_PLOG = 1
    };

    static xct_impl_t xct_impl;

    // Checkpoint manager
    static chkpt_m*    chkpt;

    // Recovery manager
    static restart_m*  recovery;

    static btree_m* bt;

    static ss_m*    SSM;    // we will change to lower case later

    /**\brief Store property that controls logging of pages in the store.
     * \ingroup SSMSTORE
     * \details
     * - t_regular: All pages in the store are fully logged for ACID properties.
     * - t_temporary: Structural changes to the store are guaranteed by
     *             logging but user data are not.   In the case of indexes,
     *             the ihtegrity of the index is not guaranteed: only the
     *             integrity of the store is guaranteed. In the event of
     *             abort, the index must be destroyed.
     *             Temporary stores are destroyed when a volume is mounted or
     *             dismounted, so they do not survive restart, regardless
     *             whether a crash occurred.
     * - t_load_file: A store that is created with this property starts out
     *             as a t_tempory store and is converted to a t_regular
     *             store upon commit.
     * - t_insert_file: Updates to existing pages are fully logged (as if the
     *             store were t_regular), but pages allocated while the
     *             store has t_insert_file are not logged. This is useful for
     *             bulk-loading, e.g., a store is bulk-loaded in one
     *             transaction (t_load_file), which commits (now the file is
     *             t_regular); subsequent appends to the file would incur
     *             full logging, so the subsequent transaction can change the
     *             store's property to t_insert_file, append the data,
     *             and change the store's property back to t_regular.
     *
     * \verbatim
     * ------------------------------------------------------------
     * ------------------------------------------------------------
     * Permissible uses of store property by storage manager client:
     * ------------------------------------------------------------
     * Create a btree index: | Change it to:
     *    t_tmp NO
     *    t_load_file YES    | tmp NO load_file NO insert_file NO regular YES
     *    t_insert_file YES  | tmp NO load_file NO insert_file YES regular YES
     *    t_regular YES      | tmp NO load_file NO insert_file NO regular YES
     *
     * Create a file:        | Change it to:
     *    t_tmp YES          | tmp NO load_file NO insert_file YES regular YES
     *    t_load_file YES    | tmp NO load_file NO insert_file YES regular YES
     *    t_insert_file YES  | tmp NO load_file NO insert_file YES regular YES
     *    t_regular YES      | tmp NO load_file NO insert_file YES regular YES
     * ------------------------------------------------------------
     * Effects of changing a file to regular:
     *    This causes the buffer pool to
     *    force to disk all dirty pages for the store, and
     *    to discard (evict from the buffer pool) all the store's
     *    pages, clean or dirty.  When these pages are next read
     *    into the buffer pool, they will be tagged as regular.
     * ------------------------------------------------------------
     * Effects of commit:
     *    t_tmp              remains t_tmp
     *    t_load_file        store is t_regular**
     *    t_insert_file      store is t_regular**
     *    t_regular          ACID
     *          ** Upon creation of such a store, the storage manager pushes
     *          this store on a list to traverse and convert to regular
     *          upon commit.
     *          Upon changing a store's property to t_insert_file,
     *          the storage manager pushes the store on this same list.
     * ------------------------------------------------------------
     * Effects of abort on user data:
     *    t_tmp              undefined: client must remove store
     *    t_load_file        undefined: client must remove store
     *    t_insert_file      undefined: client must remove store
     *    t_regular          ACID
     * ------------------------------------------------------------
     * Effects of dismount/mount/restart:
     *    t_tmp              store removed
     *    t_load_file        undefined if not commited
     *    t_insert_file      undefined if not commited
     *    t_regular          ACID
     * ------------------------------------------------------------
     * \endverbatim
     */
    enum sm_store_property_t {
    // NB: this had better match store_flag_t!!! (sm_base.h)
    t_regular     = 0x1,

    /// allowed only in create
    t_temporary    = 0x2,

    /// allowed only in create, these files start out
    /// as temp and are converted to regular on commit
    t_load_file    = 0x4,

    /// current pages logged, new pages not logged
    /// EX lock is acquired on file.
    /// only valid with a normal file, not indices.
    t_insert_file = 0x08,

    t_bad_storeproperty = 0x80// no bits in common with good properties
    };
/**\endcond skip */
};

/**\cond skip */
typedef smlevel_0 smlevel_top;

ostream&
operator<<(ostream& o, smlevel_0::sm_store_property_t p);

class xct_log_warn_check_t : public smlevel_0 {
public:
    static w_rc_t check(xct_t*&);
};

ostream&
operator<<(ostream& o, smlevel_0::store_flag_t flag);

ostream&
operator<<(ostream& o, const smlevel_0::store_operation_t op);

ostream&
operator<<(ostream& o, const smlevel_0::store_deleting_t value);

#if defined(__GNUC__) && __GNUC_MINOR__ > 6
ostream& operator<<(ostream& o, const smlevel_0::xct_state_t& xct_state);
#endif

#if defined(SM_SOURCE)
#    include <fixable_page_h.h>
#    include <pmap.h>
#    include <log.h>
#    include <vol.h>

#    if defined(FILE_C) || defined(SMFILE_C)
#    define BTREE_H
#    endif
#    include <btree.h>

#    include <btcursor.h>
#    include <xct_dependent.h>
#    include <prologue.h>

#    include <lock.h>
#    include <logrec.h>
#    include <xct.h>
#    include <logarchiver.h>
#endif

/**\endcond skip */

/*<std-footer incl-file-exclusion='SM_BASE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/

