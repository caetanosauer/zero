// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='API_H'>

 $Id: api.h,v 1.8 2010/10/27 17:04:20 nhall Exp $

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

/*  -- do not edit anything above this line --   </std-header>*/

/* 
 * This file contains doxygen documentation only 
 * Its purpose is to determine the layout of the SSMAPI
 * page, which is the starting point for the on-line 
 * storage manager documentation.
 */

/**\defgroup SSMAPI SHORE Storage Manager Application Programming Interface (SSM API)
 *\section SSMINTRO Introduction
 *
 * Most of the SHORE Storage Manager functionality is presented to the
 * user (server-writer) in two C++ classes, ss_m and smthread_t.
 * The ss_m is the SHORE storage manager, a single instance of which must be 
 * constructed before any storage manager methods may be used.
 * There cannot be more than one ss_m instance extant.  
 * The construction of the single instance performs recovery.  Deletion
 * of the single instance shuts down the storage manager.
 *
 * The storage manager stores state information in per-thread variables; for
 * this reason, storage manager methods must be called in the context
 * of a storage manager thread, smthread_t.
 * This means they must be called
 * (directly or indirectly) by the run() method of a class derived from
 * smthread_t.   As a result, you must write a class to encapsulate
 * your server functionality, and that class must derive from smthread_t, as
 * described in the following pseudo-code:
 *
 * \verbatim
main()
{
    // Marshall all the resources needed to get going:
    // "Open" the sources of work for your server, such as
    // listening on a network socket, or opening an input
    // file for reading.
    //
    // Start up a storage manager:
    your_server_thread_t *server = new your_server_thread_t(...);
    // let the server thread do its thing
    server->fork();
    // wait until it's done
    server->join();
    // clean up
    delete server;
    // un-marshall resources (close files, clean up, etc)
}

// This class creates and destroys a storage manager,
// and forks the worker threads that use the storage manager.
class \em your_server_thread_t : public smthread_t 
{
    your_server_thread_t ( ...sources... ) ;
    ~your_server_thread_t () ;
    void run(); // this is virtual in smthread_t
};

// This class performs work given to it by a source.
// It uses the given storage manager instance to perform that work.

class \em your_worker_thread_t : public smthread_t 
{
    your_worker_thread_t (ss_m *ssm, ...source... ) ;
    ~your_worker_thread_t () ;
    void run(); // this waits for work from its assigned source
    // (e.g., from terminal input or from a network connection), and
    // performs the necessary work
};

void your_server_thread_t::run() 
{
     // marshal resources neeeded for storage manager work
     // including run-time options (see discussion below).
     ss_m *ssm = new ss_m(...);

     // Fork off some number of threads 
     // worker threads that use the instance ssm.
     // Either pass ssm to these threads' constructors or
     // make ssm global.
     for(int i = 0; i < num_threads; i++) {
         workers[i] = new your_worker_t(ssm, ... source ... );
     }

     // smthreads' run() method is not called until thread is forked.
     for(int i = 0; i < NUM_THREADS; i++) {
         workers[i]->fork();
     }

     // Await and join worker threads. Join() returns when thread's
     // run() method returns.
     for(int i = 0; i < NUM_THREADS; i++) {
         workers[i]->join();
     }
     for(int i = 0; i < NUM_THREADS; i++) {
         delete workers[i];
     }

     delete ssm;
     // un-marshal (clean up) resources neeeded for storage manager work
} // a join() on this thread now returns.

 * \endverbatim
 *
 * The storage manager relies heavily on certain programming idioms 
 * to make sure return values from all methods are checked.
 * The idioms are encapsulated in preprocessor macros.
 * As a user of the storage manager, you
 * strongly encouraged to use these idioms. 
 * Although the use of the macros is optional,
 * perusal of the storage manager source code and examples 
 * will be easier if you are aware of them. 
 * They are described in \ref IDIOMS, \ref MACROS, and \ref w_rc.h. Before you
 * spend much time looking at examples, it would be worthwhile to look at the
 * macro definitions in \ref w_rc.h. 
 *
 * The storage manager is parameterized with options and their associated values.
 * The options determine such things as the size of the buffer pool
 * and lock table,
 * the location of the log,
 * lock granularity, 
 * certain caching behavior.
 * Most of these have default values, but some  options 
 * (such as a path name indicating the location of the log)
 * do not have a default value and must be given values at run time.
 *
 * An options-processing package is provided for this
 * purpose.  It handles the parsing of option names and values, which
 * may be given on the command line, or in a file or input stream.
 * Because certain options \e must be given a value at run-time, 
 * the use of this package is \e not optional: every server must at
 * least create a minimal set of options and give them values.
 * In the above pseudo-code, invoking the run-time options package would
 * be inserted in 
 * \code your_server_thread_t::run() \endcode
 * or in 
 * \code main(). \endcode
 *
 * The storage manager release comes with small examples illustrating 
 * how the options are to be used.
 *
 * See
 *   - \ref SSMOPT for an inventory of the storage manager's options, 
 *   - \ref OPTIONS for a discussion of code to initialize the options, 
 *   - \ref SSMINIT and \ref smthread_t for discussion of how to initialize and start up
 *   a storage manager,
 *   - \ref startstop.cpp for an example of the minimal required use of
 *   options in a server, and
 *   - the example consisting of \ref create_rec.cpp and \ref init_config_options.cpp for a more complete example.
 *
 * With few exceptions, the storage manager does work on behalf of a 
 * \e transaction and the storage manager acquires locks for that transaction,
 * e.g., to read a record, the storage manager acquires read locks and to update a record it
 * acquires write locks.  
 * Rather than expect the transaction of interest to be given as an argument to every storage manager method, the storage manager
 * assumes an \e attachment between a storage manager thread and a transaction.
 * The attachment is not fixed and permanent; rather,
 * a worker thread can choose which transaction it is serving,
 * and can set aside transaction A and proceed to serve transaction B, while another
 * thread can pick up transaction B and do work on its behalf.
 * A thread cannot serve more than one transaction at any time, and, except under
 * limited circumstances,
 * a transaction cannot be served by more than one thread at a time.
 * Through the API, a storage manager thread can :
 * - start a new transaction (implictly attaching the transaction to the thread)
 * - detach the attached transaction 
 * - attach an arbitrary transaction 
 * - perform work on behalf of the attached transaction
 * - prepare the attached transaction
 * - commit or abort the attached transaction (implicitly detaching it)
 *
 * See \ref SSMXCT for details.
 *
 * Persistent data are contained in a variety of storage structures (files of records, indexes, etc.).
 * All data structures reside in volumes, which are \e mounted.
 * Identifiers for data on the volume contain that volume number. The act of mounting a volume
 * creates the association of the volume number with the path name of a Unix file.
 * OK, that's a lie. The original design of SHORE called for multiple volumes per Unix file,
 * and so the file was associated with a \e device, and volumes were contained in a device.
 * SHORE never supported multiple volumes on a device, but the device-volume distinction remains.
 * Thus, a server mounts (and formats, if necessary) a device, which is identified by a path name.
 * Then the server creates a volume (numbered), which resides on the device.
 *
 * See \ref SSMSTG for details about storage structures, and see \ref IDS for a description of the
 * identifiers used for storage structures and transactions. 
 *
 * Please refer to the list of modules at the bottom of this page for more information.
 */

/**\defgroup IDIOMS Programming Idioms
 * \ingroup SSMAPI
 */

/**\defgroup MACROS Significant C Preprocessor Macros 
 * \ingroup SSMAPI
 *
 * See the "Defines" sections in the files listed below to see
 * significant macros used in the storage manager source code.
 * (This is not a complete list.)
 * 
 * The macros that are useful for value-added-server code are
 * those found in \ref w_rc.h.
 */

/**\defgroup IDS Identifiers
 * \ingroup SSMAPI
 *
 * Identifiers for persistent storage entities are used throughout
 * the storage manager API. This page collects them for convenience of
 * reference.  
 */

/**\defgroup SSMINIT Starting Up, Shutting Down, Thread Context
 * \ingroup SSMAPI
 *
 * \section SSMSTART Starting a Storage Manager
 * Starting the Storage Manager consists in 2 major things:
 * - Initializing the options the storage manager expects to be set.
 *   See
 *   - \ref OPTIONS for a discussion of code to initialize the options
 *   - \ref SSMOPT for an inventory of the storage manager's options.
 * - Constructing an instance of the class ss_m.
 *   The constructor ss_m::ss_m performs recovery, and when 
 *   it returns to the caller, the caller may begin 
 *   using the storage manager.  
 *
 * No more than one instance may exist at any time.
 *
 * Storage manager functions must be called in the context of
 * a run() method of an smthread_t. 
 *
 * See \ref create_rec.cpp for an example of how this is done.
 *
 * See also \ref SSMLOGSPACEHANDLING and \ref LOGSPACE for discussions 
 * relating to the constructor and its arguments.
 *
 * \section SSMSHUTDOWN Shutting Down a Storage Manager
 * Shutting down the storage manager consists of deleting the instance
 * of ss_m created above.
 *
 * The storage manager normally shuts down gracefully; if you want
 * to force an unclean shutdown (for testing purposes), you can do so.
 * See ss_m::set_shutdown_flag.
 *
 * \section SSMLOGSPACEHANDLING Handling Log Space
 * The storage manager contains a primitive mechanism for responding
 * to potential inability to rollback or recover due to lack of log
 * space.
 * When it detects a potential problem, it can issue a callback to the
 * server, which can then deal with the situation as it sees fit.
 * The use of such a callback mechanism is entirely optional.
 *
 * The steps that are necessary are:
 * - The server constructs the storage manager ( ss_m::ss_m() ) with two callback function
 *   pointers,
 *   the first of type \ref ss_m::LOG_WARN_CALLBACK_FUNC, and 
 *   the second of type \ref ss_m::LOG_ARCHIVED_CALLBACK_FUNC.
 * - The server is run with a value given to the sm_log_warn option,
 *   which determines the threshold at which the storage manager will
 *   invoke *LOG_WARN_CALLBACK_FUNC.  This is a percentage of the
 *   total log space in use by active transactions.
 *   This condition is checked when any thread calls a storage  manager
 *   method that acts on behalf of a transaction.
 * - When the server calls the given LOG_WARN_CALLBACK_FUNC, that function
 *   is given these arguments:
 *    - iter    Pointer to an iterator over all xcts.
 *    - victim    Victim will be returned here.
 *    - curr    Bytes of log consumed by active transactions.
 *    - thresh   Threshhold just exceeded. 
 *    - logfile   Character string name of oldest file to archive.
 *
 *    The initial value of the victim parameter is the transaction that
 *    is attached to the running thread.  The callback function might choose
 *    a different victim and this in/out parameter is used to convey its choice.
 *
 *    The callback function can use the iterator to iterate over all
 *    the transactions in the system. The iterator owns the transaction-list
 *    mutex, and if this function is not using that mutex, or if it
 *    invokes other static methods on xct_t, it must release the mutex by
 *    calling iter->never_mind().
 *
 *    The curr parameter indicates whte bytes of log consumed by the
 *    active transactions and the thresh parameter indicates the threshold
 *    that was just exceeded.
 *
 *    The logfile parameter is the name (including path) of the log file
 *    that contains the oldest log record (minimum lsn) needed to
 *    roll back any of the active transactions, so it is the first
 *    log file candidate for archiving.
 *
 *    If the server's policy is to abort a victim, it needs only set
 *    the victim parameter and return eUSERABORT.  The storage manager
 *    will then abort that transaction, and the storage manager
 *    method that was called by the victim will return to the running
 *    thread with eUSERABORT.
 *
 *    If the server's policy is not to abort a victim, it can use
 *    xct_t::log_warn_disable() to prevent the callback function
 *    from being called with this same transaction as soon as
 *    it re-enters the storage manager.
 *
 *    If the policy is to archive the indicated log file, and an abort
 *    of some long-running transaction ensues, that log file might be
 *    needed again, in which case, a failure to open that log file will
 *    result in a call to the second callback function, indicated by the
 *    LOG_ARCHIVED_CALLBACK_FUNC pointer.  If this function returns \ref RCOK,
 *    the log manager will re-try opening the file before it chokes.
 *
 *    This is only a stub of an experimental handling of the problem.
 *    It does not yet provide any means of resetting the counters that
 *    cause the tripping of the LOG_WARN_CALLBACK_FUNC.
 *    Nor does it handle the problem well in the face of true physical
 *    media limits.  For example, if, in recovery undo, it needs to
 *    restore archived log files, there is no automatic means of 
 *    setting aside the tail-of-log files to make room for the older
 *    log files; and similarly, when undo is finished, it assumes that
 *    the already-opened log files are still around.
 *    If a callback function renames or unlinks a log file, because the
 *    log might have the files opened, the rename/unlink will not
 *    effect a removal of these files until the log is finished with them.
 *    Thus, these hooks are just a start in dealing with the problem.
 *    The system must be stopped and more disks added to enable the
 *    log size to increase, or a fully-designed log-archiving feature
 *    needs to be added.
 *    Nor is this well-tested.
 *
 *    The example \ref log_exceed.cpp is a primitive
 *    example using these callbacks. That example shows how you must
 *    compile the module that uses the API for xct_t.
 *
 */


/**\defgroup OPTIONS Run-Time Options 
 * \ingroup SSMAPI
 */

/**\defgroup SSMOPT List of Run-Time Options
 * \ingroup OPTIONS
 */

 /**\defgroup SSMSTG Storage Structures
  *
  * The modules below describe the storage manager's storage structures. 
  * In summary, 
  * - devices contain
  *   - volumes, which contain
  *     - stores, upon which are built 
  *       - files of records,
  *       - conventional indexes (B+-trees), and
  *
  *
 * \ingroup SSMAPI
 */

 /**\defgroup SSMVOL Devices and Volumes
 * \ingroup SSMSTG
 */

/**\defgroup SSMSTORE Stores
 * \ingroup SSMSTG
 */

/**\defgroup SSMFILE Files of Records
 * \ingroup SSMSTG
 */

/**\defgroup SSMPIN Pinning Records
 * \ingroup SSMFILE
 */

/**\defgroup SSMBTREE B+-Tree Indexes
 * \ingroup SSMSTG
 */

/**\defgroup SSMSCAN Scanning
 * \ingroup SSMSTG
 */

/**\defgroup SSMSCANI Scanning B+-Tree Indexes
 * \ingroup SSMSCAN
 * To iterate over the {key,value} pairs in an index, 
 * construct an instance of the class btcursur_t.
 * That page contains examples. 
 */

/**\defgroup SSMXCT  Transactions, Locking and Logging
 * \ingroup SSMAPI
 */

/**\defgroup SSMLOCK Locking 
 * \ingroup SSMXCT
 */

/**\defgroup SSMSP  Partial Rollback: Savepoints
 * \ingroup SSMXCT
 */

/**\defgroup SSMQK  Early Lock Release: Quarks
 * \ingroup SSMXCT
 */

/**\defgroup SSM2PC  Distributed Transactions: Two-Phase Commit
 * \ingroup SSMXCT
 */
/**\defgroup SSMMULTIXCT Multi-threaded Transactions
 * \ingroup SSMXCT
 */

/**\defgroup LOGSPACE Running Out of Log Space  
 *   \ingroup SSMXCT
 */

/**\defgroup LSNS How Log Sequence Numbers are Used
 * \ingroup SSMXCT
 */

/**\defgroup SSMSTATS Storage Manager Statistics
 * \ingroup SSMAPI
 *
 * The storage manager contains functions to gather statistics that
 * it collects. These are mostly counters and are described here.
 *
 * Volumes can be analyzed to gather usage statistics.  
 * See ss_m::get_du_statistics and ss_m::get_volume_meta_stats.
 *
 * \note A Perl script facilitates modifying the statistics gathered by
 * generating much of the supporting code, including
 * structure definitions and output operators.  
 * The server-writer can generate her own sets of statistics using
 * the same Perl tool.
 * See \ref STATS for
 * more information about how these statistics sets are built.
 *
 */

/**\defgroup SSMVTABLE Virtual Tables
 * \ingroup SSMAPI
 * \details
 *
 * Virtual tables are string representations of internal
 * storage manager tables.
 * These tables are experimental. If the tables get to be very
 * large, they might fail.
 * - lock table (see ss_m::lock_collect)
 *   Columns are:
 *   - mode
 *   - duration
 *   - number of children 
 *   - id  of owning transaction
 *   - status (granted, waiting)
 * - transaction table (see ss_m::xct_collect)
 *   Columns are:
 *   - number of threads attached
 *   - global transaction id
 *   - transaction id
 *   - transaction state (in integer form)
 *   - coordinator  
 *   - forced-readonly (Boolean)
 * - threads table (see ss_m::thread_collect)
 *   Columns are:
 *   - sthread ID
 *   - sthread status
 *   - number of I/Os issued
 *   - number of reads issued
 *   - number of writes issued
 *   - number of syncs issued
 *   - number of truncates issued
 *   - number of writev issued
 *   - number of readv issued
 *   - smthread name
 *   - smthread thread type (integer)
 *   - smthread pin count
 *   - is in storage manager
 *   - transaction ID of any attached transaction
*/
 /**\example vtable_example.cpp */

/**\defgroup MISC Miscellaneous
 * \ingroup SSMAPI
 */
/**\defgroup SSMSYNC Synchronization, Mutual Exclusion, Deadlocks
 * \ingroup MISC
 *
 * Within the storage manager are a variety of primitives that provide for
 * ACID properties of transactions and for correct behavior of concurrent
 * threads. These include:
 * - read-write locking primitives for concurrent threads  (occ_rwlock,
 *   mcs_rwlock)
 * - mutexes (pthread_mutex_t, queue_based_lock_t)
 * - condition variables (pthread_cond_t)
 * - latches (latch_t)
 * - database locks 
 *
 * The storage manager uses database locks to provide concurrency control
 * among transactions; 
 * latches are used for syncronize concurrent threads' accesses to pages in the 
 * buffer pool.  The storage manager's threads use carefully-designed
 * orderings of the entities they "lock" with synchronization primitives
 * to avoid any sort of deadlock.  All synchronization primitives
 * except data base locks are meant to be held for short durations; they
 * are not even held for the duration of a disk write, for example. 
 *
 * Deadlock detection is done only for database locks.
 * Latches are covered by locks, which is
 * to say that locks are acquired before latches are requested, so that
 * deadlock detection in the lock manager is generally sufficient to prevent
 * deadlocks among concurrent threads in a properly-written server.
 *
 * Care must be taken, when writing server code, to avoid deadlocks of
 * other sorts such as latch-mutex, or latch-latch deadlocks.
 * For example, multiple threads may cooperate on behalf of the same
 * transaction; if they are trying to pin records without a well-designed
 * ordering protocol, they may deadlock with one thread holding page
 * A pinned (latched) and waiting to pin (latch) B, while the other holds
 * B pinned and waits for a pin of A.
 */

/**\defgroup SSMAPIDEBUG Storage Manager API Methods for Debugging
 * \ingroup SSMAPI
 */

/**\defgroup TLS Thread-Local Variables
 * \ingroup MISC
 */
/**\defgroup UNUSED Unused code 
 * \ingroup MISC
 */

/**
 * \defgroup SSMBUFPOOL Bufferpool
 * \ingroup SSMAPI
 * The buffer-pool management code.
 */

/**\defgroup OPT Configuring and Building the Storage Manager
 */

/**\defgroup IMPLGRP Implementation Notes
 * See \ref IMPLNOTES "this page" for some implementation details.
 */

/**\defgroup REFSGRP References
 * See \ref REFERENCES "this page" for references to selected papers 
 * from which ideas are used in the Shore Storage Manager. 
 */
