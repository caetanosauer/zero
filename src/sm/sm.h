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

/*<std-header orig-src='shore' incl-file-exclusion='SM_H'>

 $Id: sm.h,v 1.322 2010/10/27 17:04:23 nhall Exp $

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

#ifndef SM_H
#define SM_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
 *  Stuff needed by value-added servers.  NOT meant to be included by
 *  internal SM .c files, except to the extent that they need these
 *  definitions used in the API.
 */

#include <sm_base.h>
#include <smstats.h> // declares sm_stats_info_t and sm_config_info_t
#include <lsn.h>
#include <string>
#include "sm_options.h"

/* DOXYGEN Documentation : */

/**\addtogroup SSMOPT
 *
 * These are the run-time options for the storage manager.
 *
 * -sm_backup_dir :
 *      - type: string
 *      - description: Path of the folder containing backup files.
 *      - default: "."
 *      - required?: no
 *
 * -sm_bufpoolsize :
 *      - type: number
 *      - description: This is the size of
 *      the buffer pool in Kb.  Must be large enough to hold at least 32 pages,
 *      so it depends on the configured page size.
 *      - default: none
 *      - required?: yes
 *
 * -sm_hugetlbfs_path
 *      - type: string (full absolute path name)
 *      - description: Needed only if you configured --with-hugetlbfs.
 *      - default: see \ref CONFIGOPT
 *      - required?: no
 *
 * -sm_reformat_log
 *      - type: Boolean
 *      - description: If "yes", your log will be clobbered and the storage
 *      manager will start up with an entirely new log.
 *      - default: no
 *      - required?: no
 *
 * -sm_logdir
 *      - type: string (relative or absolutee path name)
 *      - description: Location of the log files.
 *      - default: none
 *      - required?: yes
 *
 * -sm_logbufsize
 *      - type: number
 *      - description: size of log buffer in KB.
 *      Must be greater than or equal to the larger of
 *      (4 times the page size, 64 Kb)
 *      and less than or equal to
 *      128 times the page_size. This is the size of
 *      the log buffer in byte.
 *      - default: 128KB
 *      - default with the new log buffer: 1MB
 *      - required?: no
 *
 * -sm_logsize
 *      - type: number
 *      - description: greater than or equal to 8256
 *      This is the maximum size of the log in Kb.  It is a function of
 *      the log buffer size, and  the default is the minimum allowable for
 *      the default sm_logbufsize.
 *      - default: 10000
 *      - default with the new log buffer: 128*1024 (128MB)
 *      - required?: yes
 *
 * -sm_errlog
 *      - type: string (relative or absolute path name OR - )
 *      - description: Destination for error messages.  If "-" is given,
 *      the destination is stderr.
 *      - default: \b -
 *      - required?: no
 *
 * -sm_errlog_level
 *      - type: string  (one of none|emerg|fatal|internal|error|warning|info|debug)
 *      - description: filter.  Message of this priority or higher are issued to
 *      the error log; messages with lower priority are not issued.
 *      The priorities are listed from high to low. "none" means no logging
 *      will happen.
 *      - default: error
 *      - required?: no
 *
 * -sm_locktablesize :
 *      - type: number greater than or equal to 64
 *      - description: size of lock manager's hash table will be a prime
 *      number near and greater than the given number.
 *      - default: 64000 (yields a hash table with 65521 buckets)
 *      - required?: no
 *
 * -sm_backgroundflush
 *      - type: Boolean
 *      - description: Enables background-flushing of volumes.
 *      Must be set to "yes" for sm_num_page_writers to have any effect.
 *      - default: yes
 *      - required?: no
 *
 * -sm_bufferpool_replacement_policy
 *      - type: string (one of clock|random)
 *      - description: Sets the page replacement policy of the buffer pool.
 *      - default: clock
 *      - required?: no
 *
 * -sm_bufferpool_swizzle
 *      - type: Boolean
 *      - description: Enables pointer swizzling in buffer pool.
 *      - default: no
 *      - required?: no
 *
 * -sm_num_page_writers
 *      - type: number
 *      - description: greater than or equal to 1; this is the number of
 *      background-flushing threads.
 *      - default: 1
 *      - required?: no
 *
 * -sm_prefetch
 *      - type: Boolean
 *      - description: Enables prefetching for scans.
 *      - default: no
 *      - required?: no
 *
 * -sm_logging
 *      - type: Boolean
 *      - description: Allows you to turn off logging for a run of
 *      the storage manager. This is only for experimentation, to
 *      measure logging overhead in a limited way.
 *      Aborts, rollbacks and restart/recovery
 *      do not work without logging.   Independent concurrent
 *      transactions using btrees might not work without logging (this is
 *      not well-tested).
 *      Each time you start the server, you had better start with a
 *      clean device or a device that resulted from a clean shutdown
 *      of the prior run.
 *      - default: yes
 *      - required?: no
 *
 * -sm_lock_caching
 *      - type: Boolean
 *      - description: Enables caching of transaction locks in transaction.
 *      Can be turned off for experimentation. If no, the default is not
 *      to cache locks, but any transaction can turn on caching for itself
 *      by calling the ss_m method  set_lock_cache_enable(bool enable).
 *      - default: yes
 *      - required?: no
 *
 * -sm_statistics
 *      - type: Boolean
 *      - description: Enables collecting statistics.
 *      - default: no
 *      - required?: no
 *
 * -sm_restart
 *  - type: number
 *  - description: control internal restart/recovery mode
 *     this option is for internal testing purpose and must be used with caution
 *     valid values: see sm.cpp
 *  - default: see sm.cpp for initial setting
 *  - required?: no
 *
 *  -sm_archdir;
 *      - type: string
 *      - description: directory in which to store log archive runs
 *      - default: none
 *      - required?: no (only if archiving is activated below)
 *
 *  -sm_archiving;
 *      - type: Boolean
 *      - description: Activates log archiving, but just instatiates a
 *      LogArchiver object without sending it an activation signal. See the SM
 *      method activate_archiver for that.
 *      - default: no
 *      - required?: no
 *
 *  -sm_async_merging;
 *      - type: Boolean
 *      - description: Activates asynchronous merging of log archive runs
 *      - default: no
 *      - required?: no
 *
 *  -sm_sort_archive;
 *      - type: Boolean
 *      - description: Whether to partially-sort the log archive, resulting in
 *      run files. If turned off, the archiver thread simply copies blocks from
 *      log files to the log archive. The purpose is to simulate a traditional
 *      log archiving operation in experiments.
 *      - default: yes
 *      - required?: no
 *
 *  -sm_merge_factor;
 *      - type: int
 *      - description: Maximum merge factor (or fan-in) to be used by the log
 *      archive merger
 *      - default: 100
 *      - required?: no
 *
 *  -sm_merge_blocksize;
 *      - type: int (>=8192)
 *      - description: Size in bytes of the IO unit used by the archive merger
 *      - default: 1048576 (1MB)
 *      - required?: no
 *
 *  -sm_archiver_workspace_size;
 *      - type:  int
 *      - description: Size of sort workspace of log archiver
 *      - default: 104857600 (100 MB)
 *      - required?: no
 *
  */


/**\addtogroup SSMXCT
 * All storage manager operations on data must be done within the scope of
 * a transaction (ss_m::begin_xct, ss_m::commit_xct, ss_m::abort_xct,
 * ss_m::chain_xct).
 *
 * A very few storage manager operations, such as formatting a volume, are
 * called outside the scope of a transaction and the storage manager begins
 * its own transaction to do the work.
 *
 * Operations that fail return an error indication and the storage
 * manager assumes that the server will thereafter abort the
 * transaction in which the error occurred, when abort is indicated.
 * Abort is indicated when eUSERABORT or eDEADLOCK is returned and
 * when the erver chooses to abort rather than to work around the problem
 * (whatever it might be, such as eRETRY).
 *
 * The storage manager does not enforce the aborting of any erroneous
 * transactions except, possibly, those that are in danger of
 * running out of log space.
 * (This is done with the destructor of the prologue used on each call
 * to the storage manager, see next paragraph).
 *
 * It is always the server's responsibility to abort.
 * When the storage manager
 * encounters a eLOGSPACEWARN condition (the log hasn't enough
 * space \e at \e this \e moment to abort the running transaction,
 * assuming a 1:1 ration of rollback-logging overhead to forward-processing
 * logging overhead), it does one of two things:
 * - passes the error code eLOGSPACEWARN up the call stack back to the server
 *   if the storage manager was constructed with no log-space-warning callback
 *   argument (see LOG_WARN_CALLBACK_FUNC, ss_m::ss_m).
 * - tries to abort a transaction before passing an error code back up
 *   the call stack to the server. Choosing a victim transaction to abort
 *   is done by the server in its log-space-warning callback function (passed
 *   in on ss_m::ss_m, q.v.
 *   Only if that callback function returns a non-null victim transaction
 *   and returns eUSERABORT does the storage manager abort that victim
 *   before returning eUSERABORT up the call stack. Any other
 *   error code returned by the callback function is just returned up
 *   the call stack.
 *
 * \section LOCKS Locks
 *
 * The storage manager automatically acquires the
 * necessary locks when the data are read or written.
 * The locks thus acquired are normally released at the end of a transaction,
 * thus, by default, transactions are two-phase and well-formed (degree 3).
 *
 * \section DISTXCT Distributed Transactions
 * Storage manager transactions may be used as "threads" (to
 * overload this term) of distributed transactions.
 * Coordination of 2-phase commit must be done externally,
 * but the storage manager supports preparing the (local) transaction "thread"
 * for two-phase commit, and it will log the necessary
 * data for recovering in-doubt transactions.
 *
 * \section ATTACH Threads and Transactions
 * Transactions are not tied to storage manager threads (smthread_t, not
 * to be confused with a local "thread" of a distributed transaction) in any
 * way other than that a transaction must be \e attached to a
 * thread while any storage manager work is being done on behalf of
 * that transaction.   This is how the storage manager knows \e which
 * transaction is to acquire the locks and latches, etc.
 * But a thread can attach and detach from transactions at will, so
 * work may be performed by different threads each time the storage
 * manager is called on behalf of a given transaction; this allows the
 * server to keep a pool of threads to perform work and allows them to
 * perform work on behalf of any active transaction.
 *
 * \warning
 * While there are limited circumstances in which multiple threads can be
 * attached to the same transaction \e concurrently and perform storage
 * manager operations on behalf of that transaction concurrently,
 * which is a hold-over from the original storage manager, this
 * functionality will be deprecated soon.  The reason for this being
 * removed is that it is extremely difficult to handle errors internally
 * when multiple threads are attached to a transaction because
 * partial rollback is impossible in the absence of multiple log streams
 * for a transaction.
 *
 * Under no circumstances may a thread attach to more than one transaction
 * at a time.
 *
 *
 * \section EXOTICA Exotica
 * The storage manager also provides
 * - partial rollback (ss_m::save_work and ss_m::rollback_work),
 *   which undoes actions but does not release locks,
 * - transaction chaining (ss_m::chain_xct), which commits, but retains locks
 *   and gives them to a new transaction,
 * - lock release (ss_m::unlock), allowing less-than-3-degree
 *   transactions.
 *
 *  To reduce the cost (particularly in logging) of loading databases,
 *  the storage manager provides for unlogged loading of stores.
 *  See \ref SSMSTORE.
 */

class fixable_page_h;
class btree_page_h;
class xct_t;
class vec_t;
class log_m;
class lock_m;
class pool_m;
class dir_m;
class chkpt_m;
class lid_m;
class sm_stats_cache_t;
class prologue_rc_t;
class w_keystr_t;
class verify_volume_result;
class lil_global_table;
struct okvl_mode;

class key_ranges_map;
/**\addtogroup SSMSP
 * A transaction may perform a partial rollback using savepoints.
 * The transaction populates a savepoint by calling ss_m::save_work,
 * then it may roll back to that point with ss_m::rollback_work.
 * Locks acquired between the save_work and rollback_work are \e not
 * released.
 */

/**\brief A point to which a transaction can roll back.
 * \ingroup SSMSP
 *\details
 * A transaction an do partial rollbacks with
 * save_work  and rollback_work, which use this class to determine
 * how far to roll back.
 * It is nothing more than a log sequence number for the work done
 * to the point when save_work is called.
 */
class sm_save_point_t : public lsn_t {
public:
    sm_save_point_t(): _tid(0) {};
    friend ostream& operator<<(ostream& o, const sm_save_point_t& p) {
        return o << p._tid << ':' << (const lsn_t&) p;
    }
    friend istream& operator>>(istream& i, sm_save_point_t& p) {
        char ch;
        return i >> p._tid >> ch >> (lsn_t&) p;
    }
    tid_t            tid() const { return _tid; }
private:
    friend class ss_m;
    tid_t            _tid;
};

/**
 * \brief \b This \b is \b the \b SHORE \b Storage \b Manager \b API.
 * \ingroup SSMBTREE
 * \details
 * Most of the API for using the storage manager is through this
 * interface class.
 */
class ss_m : public smlevel_top
{
public:

    typedef smlevel_0::concurrency_t concurrency_t;
    typedef smlevel_0::xct_state_t xct_state_t;

  public:
    /**\brief  Initialize the storage manager.
     * \ingroup SSMINIT
     * \details
     * @param[in] options Start-up parameters.
     * @param[in] warn   A callback function. This is called
     * when/if the log is in danger of becoming "too full".
     * @param[in] get   A callback function. This is called
     * when the storage manager needs an archived log file to be restored.
     *
     * When an ss_m object is created, the storage manager initializes itself
     * and,
     * if the sthreads package has not already been initialized by virtue
     * of an sthread_t running, the sthreads package is initialized now.
     *
     * The log is read and recovery is performed (\ref MHLPS),
     * and control returns to
     * the caller, after which time
     * storage manager threads (instances of smthread_t) may be constructed and
     * storage manager may be used.
     *
     * The storage manager is used by invoking its static methods.
     * You may use them as follows:
     * \code
     * ss_m *UNIQ = new ss_m();
     *
     * W_DO(UNIQ->mount_vol(...))
     *     // or
     * W_DO(ss_m::mount_vol(...))
     * \endcode
     * ).
     *
     * Only one ss_m object may be extant at any time. If you try
     * to create another while the one exists, a fatal error will occur
     * (your program will choke with a message about your mistake).
     *
     * The callback argument given to the storage manager constructor
     * is called when the storage manager determines that it is in danger
     * of running out of log space.  Heuristics are used to guess when
     * this is the case.
     *
     * If the function \a warn archives and removes log files, the function
     * \a get must be provided to restore those log files when the
     * storage manager needs them.
     *
     * For details and examples, see  \ref smlevel_0::LOG_WARN_CALLBACK_FUNC,
     *  \ref smlevel_0::LOG_ARCHIVED_CALLBACK_FUNC, and
     *  \ref SSMLOG.
     */
    ss_m(const sm_options &options);

    /**\brief  Shut down the storage manager.
     * \ingroup SSMINIT
     * \details
     * When the storage manager object is deleted, it shuts down.
     * Thereafter it is not usable until another ss_m object is
     * constructed.
     */
    ~ss_m();

    // Manually start the store if the store is not running already.
    // Only one instance of store can be running at any time.
    // The function starts the store using the input parameters from sm constructor,
    // no change allowed after the ss_m constructor
    // Internal settings will be reset, i.e. shutting_down, shutdown_clean flags
    // Return: true if store started successfully
    //             false if store was running already
    bool startup();

    // Manually stop a running store.  If the store was not running, no-op.
    // Only one instance of store can be running at any time.
    // The shutdown process obeys the internal settings,
    // i.e. shutting_down, shutdown_clean flags
    // Return: if store shutdown successfully or was not running
    bool shutdown();

    /**\brief Cause the storage manager's shutting down do be done cleanly
     * or to simulate a crash.
     * \ingroup SSMINIT
     * \details
     * @param[in] clean   True means shut down gracefully, false means simulate a crash.
     *
     * When the storage manager's destructor is called
     * the buffer pool is flushed to disk, unless this method is called
     * with \a clean == \e false.
     *
     * \note If this method is used, it
     * must be called after the storage manager is
     * constructed if it is to take effect. Each time the storage
     * manager is constructed, the state associated with this is set
     * to \e true, i.e., "shut down properly".
     *
     * \note This method is not thread-safe, only one thread should use this
     * at any time, presumably just before shutting down.
     */
    static void         set_shutdown_flag(bool clean);

    rc_t                _truncate_log(bool truncate_archive = false);

private:
    void                _construct_once();
    void                _destruct_once();

public:

    /**\brief Get a copy of the statistics from an attached instrumented transaction.
     * \ingroup SSMXCT
     * \details
     * @param[out] stats Returns a copy of the statistics for this transaction.
     * @param[in] reset  If true, the statistics for this transaction will be zeroed.
     */
    static rc_t            gather_xct_stats(
        sm_stats_info_t&       stats,
        bool                   reset = false);

    /**\brief Get a copy of the global statistics.
     * \ingroup SSMSTATS
     * \details
     * @param[out] stats A pre-allocated structure.
     */
    static rc_t            gather_stats(
        sm_stats_info_t&       stats
        );

    /*****************************************************************
     * Locking related functions
     *
     * NOTE: there are standard conversions from PageID, and
     *       StoreID to lockid_t, so wherever a lockid_t parameter is
     *         specified a PageID or StoreID can be used.
     *
     *****************************************************************/

    /** Start-up parameters for the storage engine. */
    static sm_options _options;

    static const sm_options& get_options() { return _options; }

private:

    static int _instance_cnt;
};

ostream& operator<<(ostream& o, const sm_stats_info_t& s);
template<class ostream>
ostream& operator<<(ostream& o, const sm_config_info_t& s)
{
    o    << "  page_size " << s.page_size
     << "  lg_rec_page_space " << s.lg_rec_page_space
     << "  buffer_pool_size " << s.buffer_pool_size
     << "  max_btree_entry_size " << s.max_btree_entry_size
     << "  logging " << s.logging
      ;
    return o;
}

/*<std-footer incl-file-exclusion='SM_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
