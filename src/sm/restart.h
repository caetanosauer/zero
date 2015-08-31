/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

     /*<std-header orig-src='shore' incl-file-exclusion='RESTART_H'>

        $Id: restart.h,v 1.27 2010/07/01 00:08:22 nhall Exp $

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

#ifndef RESTART_H
#define RESTART_H

#include "w_defines.h"
#include "w_heap.h"

class dirty_pages_tab_t;

#include "sm_base.h"
#include "lock.h"               // Lock re-acquisition

#include <map>

// Used for:
//    internal wait for testing purpose
//    normal shutdown wait
const uint32_t wait_interval = 1000;   // 1 seconds


// Map data structure for undeterminstic in-flight transactions
// key (first) is the associated transaction id (uint64_t)
// data (second) is the counter for normal and compensation log records
typedef std::map<uint64_t, signed int> tid_CLR_map;

////////////////////////////
// Heap for reverse UNDO phase
////////////////////////////

class CmpXctUndoLsns
{
    public:
        bool                        gt(const xct_t* x, const xct_t* y) const;
};

inline bool
CmpXctUndoLsns::gt(const xct_t* x, const xct_t* y) const
{
    return x->undo_nxt() > y->undo_nxt();
}

// Special heap for UNDO phase
typedef class Heap<xct_t*, CmpXctUndoLsns> XctPtrHeap;


////////////////////////////
// Heap for mount operation
////////////////////////////

// Structure for mount heap
struct comp_mount_log_t
{
    comp_mount_log_t(): mount_log_rec_buf(NULL) {};

    logrec_t*  mount_log_rec_buf;   // log record of mount/un-mount
    lsn_t      lsn;                 // LSN of the log record
};

// Heap for mount operation
class CmpMountLsns
{
    public:
        bool                        gt(const comp_mount_log_t* x, const comp_mount_log_t* y) const;
};

inline bool
CmpMountLsns::gt(const comp_mount_log_t* x, const comp_mount_log_t* y) const
{
    return x->lsn > y->lsn;
}

// Special heap for mount operation
typedef class Heap<comp_mount_log_t*, CmpMountLsns> MountPtrHeap;

////////////////////////////
// Heap for lock re-acquisition tracking
////////////////////////////

// Structure for lock heap
struct comp_lock_info_t
{
    comp_lock_info_t(const okvl_mode& mode): lock_mode(mode) {};

    tid_t      tid;          // Owning transaction id of the lock
    okvl_mode  lock_mode;    // lock mode
    uint32_t   lock_hash;    // lock hash
};

class CmpXctLockTids
{
    public:
        bool                        gt(const comp_lock_info_t* x, const comp_lock_info_t* y) const;
};

inline bool
CmpXctLockTids::gt(const comp_lock_info_t* x, const comp_lock_info_t* y) const
{
    bool gt;
    if (x->tid > y->tid)
        gt = true;
    else if (x->tid < y->tid)
        gt = false;
    else
    {
        // Two lock entries belong to the same transaction
        // use hash value to compare
        if (x->lock_hash > y->lock_hash)
            gt = true;
        else if (x->lock_hash < y->lock_hash)
            gt = false;
        else
        {
            // Same txn, same hash, check lock mode on key (ignore gap and partition)
            // while X is the greater than the rest
            //
            // no lock                                    N = 0
            // intention share (read)               IS = 1
            // intention exclusive (write)         IX = 2
            // share (read)                            S = 3
            // share with intention exclusive   SIX = 4
            // exclusive (write)                      X = 5

            if (x->lock_mode.get_key_mode()> y->lock_mode.get_key_mode())
                gt = true;
            else
                gt = false;
        }
    }

    return gt;
}

// Special heap for lock re-acquisition in backward log scan Log Analysis phase
typedef class Heap<comp_lock_info_t*, CmpXctLockTids> XctLockHeap;


////////////////////////////
// Class restart_thread_t
////////////////////////////

// Child thread created by restart_m for concurrent recovery operation
// It is to carry out the REDO and UNDO phases while the system is
// opened for user transactions
class restart_thread_t : public smthread_t
{
public:

    NORET restart_thread_t()
        : smthread_t(t_regular, "restart", WAIT_NOT_USED)
    {
        // Construct the restart thread to perform the REDO and UNDO work,
        // Give the thread priority as t_regular instead of t_time_critical
        // It performs the recovery work while concurrent user transactions
        // are coming in

        w_assert1(false == smlevel_0::use_serial_restart());

        working = smlevel_0::t_concurrent_before;
    };
    NORET ~restart_thread_t()
    {
        DBGOUT1(<< "restart_thread_t: Exiting child thread");
    };

    // Main body of the child thread
    void run();
    smlevel_0::concurrent_restart_mode_t in_restart() { return working; }

private:

    smlevel_0::concurrent_restart_mode_t working;  // what is the child working on?

private:
    // disabled
    NORET restart_thread_t(const restart_thread_t&);
    restart_thread_t& operator=(const restart_thread_t&);

};

class restart_m : public smlevel_0 {
    friend class restart_thread_t;

public:
    NORET                        restart_m():_restart_thread(0){};
    NORET                        ~restart_m()
    {
        // If we are still in Log Analysis phase, no child thread yet, go ahead and terminate

        if (_restart_thread)
        {
            // Child thread (for REDO and UNDO if open system early)
            // is still active
            w_assert1(false == smlevel_0::use_serial_restart());

            if (shutdown_clean)
            {
                // Clean shutdown, try to let the child thread finish its work
                // This would happen only if user shutdowns the system
                // as soon as the system is open (concurrent recovery just started)
                DBGOUT2(<< "waiting for recovery child thread to finish...");

                // Wait for the child thread to join, we want to give the child thread some time to
                // finish its work but we don't want to wait forever, so we are giving
                // some time to the child thread
                // If the child thread did not have enough time to finish the recovery
                // work (e.g., a lot of recovery work to do) after the wait, terminate the
                // child thread with a message
                // In this case, the normal shutdown becomes a force shutdown, meaning
                // the next server startup will need to recovery again
                // This can happen in concurrent recovery mode because system is opened
                // while the recovery is still going on

                if (smlevel_0::t_concurrent_done != _restart_thread->in_restart())
                {
                    DBGOUT1(<< "Child thread is still busy, extra sleep");
                    g_me()->sleep(wait_interval*2); // 2 second, this is a very long time
                }
                if (smlevel_0::t_concurrent_done != _restart_thread->in_restart())
                {
                    DBGOUT1(<< "Force a shutdown before restart child thread finished it work");
                    smlevel_0::errlog->clog << info_prio
                        << "Force a shutdown before restart child thread finished it work" << flushl;
                }

                w_rc_t rc = _restart_thread->join(wait_interval);  // Another wait to give child thread more time
                if (rc.is_error())
                {
                    DBGOUT1(<< "Normal shutdown - child thread join error: " << rc);
                    smlevel_0::errlog->clog << info_prio
                        << "Normal shutdown - child thread join error: " << rc << flushl;
                }
            }
            else
            {
                // Simulated crash, just kill the child thread, no wait
            }

            // Terminate the child thread
            delete _restart_thread;
            _restart_thread = 0;
        }
        w_assert1(!_restart_thread);

        // Okay to destroy the restart_m now
    };

    // Function used for concurrent operations, open system after Log Analysis
    // we need a child thread to carry out the REDO and UNDO operations
    // while concurrent user transactions are coming in
    void                        spawn_recovery_thread()
    {
        // In case caller calls this function while chid thread is alive (accident), no-op
        if (_restart_thread)
            return;

        if (false == smlevel_0::use_serial_restart())
        {
            DBGOUT1(<< "Spawn child recovery thread");

            // Only if not serial operation, create a child thread to perform recovery
            _restart_thread = new restart_thread_t;
            if (!_restart_thread)
                W_FATAL(eOUTOFMEMORY);
            W_COERCE(_restart_thread->fork());
            w_assert1(_restart_thread);
        }
        else
        {
            // If open system after Recovery (serialized operations)
            // do not create a child thread, simply return
            w_assert1(!_restart_thread);
        }
    }

    // Return true if the restart operation is still on-going
    bool                        restart_in_progress()
    {
        // This function is to find out whether the 'restart' is still on-goiing
        //
        // M1 - serial recovery mode
        // M2 - restart thread for REDO and UNDO
        // M3 - on-demand driven by user transactions, no restart thread
        // M4 - mixed mode, behaves the same for both M2 and M4
        // M5 - ARIES mode, behaves the same for both M2 and M4

        // Restart is in progress if one of the conditions is true:
        // Serial mode (M1) and
        //     Operating mode is not in t_forward_processing
        // Concurrent mode (M2, M4 and M5) and:
        //     Workign on Log Analysis
        //     or
        //     Child thread is alive
        // On-demand mode (M3):
        //     Workign on Log Analysis
        //     or
        //     If passed Log Analysis phase, unknown whether we are still in REDO or UNDO phase
        //
        if (true == smlevel_0::use_serial_restart())
        {
            // Serial mode
            return in_recovery();
        }
        else
        {
            // Concurrent mode

            // Log Analysis
            if (in_recovery_analysis())
                return true;

            // System is opened after Log Analysis, are we still in Restart?
            if (false == smlevel_0::use_redo_demand_restart())
            {
                // Not pure on-demand (M3), only check redo mode since redo and undo
                // must have the same 'on_demand' mode
                // For M2, M4 and M5, using child restart thread to determine the current status
                // if the child thread exists
                if (_restart_thread)
                {
                    if (smlevel_0::t_concurrent_done != _restart_thread->in_restart())
                        return true;
                    else
                        return false;
                }
                // If child thread does not exist, fall through
            }
            else
            {
                // M3, on-demand, the only way to find out whether the Restart finished or
                // not is to scan both buffer pool and transaction table.  Due to concurrent access,
                // the information would not be reliable even if we scan them.
                // Return false (not in Restart) although this return code cannot be trusted

                return false;
            }

            // Child thread does not exist, and the operating mode is
            // not in recovery anymore
            // For concurrent mode, the operating mode changes to
            // t_forward_processing after the child thread has been spawn
            // Corner case, if this function gets called after Log Analysis but
            // before the child thread was created, the operating mode would
            // be in Log Analysis

            // If operating mode is t_forward_processing and child restart thread does not exist
            // we are not in 'restart'
            if (false == in_recovery())
                return false;

            // Child restart thread does not exist but the operating mode is
            // not in t_forward_processing.  A very corner case, we are still in 'restart'
            return true;
        }
    }

    // Return true if the REDO operation is still on-going
    bool                        redo_in_progress()
    {
        if (true == smlevel_0::use_serial_restart())
        {
            // Serial mode
            return in_recovery_redo();
        }

        if (true == smlevel_0::use_redo_demand_restart())
        {
           // Pure on-demand REDO (M3), we don't know whether REDO is still on or not
           // Return false (not in REDO) although this return code cannot be trusted

           return false;
        }
        else
        {
            // M2 or M4
            if (smlevel_0::t_concurrent_redo == _restart_thread->in_restart())
                return true;   // In REDO
            else
                return false;  // Not in REDO
        }
    }

    // Return true if the UNDO operation is still on-going
    bool                        undo_in_progress()
    {
        if (true == smlevel_0::use_serial_restart())
        {
            // Serial mode
            return in_recovery_undo();
        }

        if (true == smlevel_0::use_undo_demand_restart())
        {
            // Pure on-demand UNDO (M3), we don't know whether UNDO is still on or not
            // Return false (not in UNDO) although this return code cannot be trusted

            return false;
        }
        else
        {
            // M2 or M4 or M5
            if (smlevel_0::t_concurrent_undo == _restart_thread->in_restart())
                return true;   // In UNDO
            else
                return false;  // Not in UNDO
        }
    }


    // Top function to start the restart process
    static void                 restart(
        lsn_t                   master,         // In: Starting point for log scan
        lsn_t&                  commit_lsn,     // Out: used if use_concurrent_log_restart()
        lsn_t&                  redo_lsn,       // Out: used if log driven REDO with use_concurrent_XXX_restart()
        lsn_t&                  last_lsn,       // Out: used if page driven REDO with use_concurrent_XXX_restart()
        uint32_t&               in_doubt_count  // Out: used if log driven REDO with use_concurrent_XXX_restart()
        );

private:

    // Shared by all restart modes
    static void                 log_analysis(
        const lsn_t             master,
        bool                    restart_with_lock,
        lsn_t&                  redo_lsn,
        lsn_t&                  undo_lsn,
        lsn_t&                  commit_lsn,
        lsn_t&                  last_lsn,
        uint32_t&               in_doubt_count,
        XctPtrHeap&             loser_heap, 
        XctLockHeap&            lock_heap
    );

    // Forward log scan without lock acquisition and use commit_lsn (M2)
    // Warning: currently this forward scan Log Analysis is NOT beging used,
    // all Instarnt Restart methods (M1 - M4) are using backward log scan.
    static void                 analysis_pass_forward(
        const lsn_t             master,          // In: LSN for the starting point of the forward scan
        lsn_t&                  redo_lsn,        // Out: Starting point for REDO forward scan
        uint32_t&               in_doubt_count,  // Out: total in_doubt page count
        lsn_t&                  undo_lsn,        // Out: Stopping point for UNDO backward log scan (if used)
        XctPtrHeap&             loser_heap,      // Out: heap contains the loser transactions (for M1 traditional restart)
        lsn_t&                  commit_lsn,      // Out: Commit lsn for concurrent transaction (for M2 concurrency control)
        lsn_t&                  last_lsn         // Last lsn in recovery log (forward scan)
        );

    // Backward log scan with lock acquisition (used if M3/M4) and commit_lsn (used for M2 only)
    static void                 analysis_pass_backward(
        const lsn_t             master,            // In: End point for backward log scan, for verification purpose only
        lsn_t&                  redo_lsn,          // Out: Starting point for REDO forward log scan (if used),
        uint32_t&               in_doubt_count,    // Out: Counter for in_doubt page count in buffer pool
        lsn_t&                  undo_lsn,          // Out: Stopping point for UNDO backward log scan (if used)
        XctPtrHeap&             loser_heap,        // Out: Heap to record all the loser transactions,
                                                   //       used only for reverse chronological order
                                                   //       UNDO phase (if used)
        lsn_t&                  commit_lsn,        // Out: Commit lsn for concurrent transaction (for M2 concurrency control)
        lsn_t&                  last_lsn,          // Out: Last lsn in recovery log
        const bool              restart_with_lock, // In: true to acquire lock (M3/M4), false to use commit_lsn (M2) or no early open (M1)
        XctLockHeap&            lock_heap          // Out: all re-acquired locks (M3 and M4 only)
        );

    // Function used for log scan REDO operations
    static void                 redo_log_pass(
        const lsn_t              redo_lsn,       // In: Starting point for REDO forward log scan
        const lsn_t              &highest,       // Out: for debugging
        const uint32_t           in_doubt_count  // In: How many in_doubt pages in buffer pool
        );

    // Function used for reverse order UNDO operations
    static void                 undo_reverse_pass(
        XctPtrHeap&             heap,       // In: heap populated with loser transactions
        const lsn_t             curr_lsn,   // In: current lsn, the starting point of backward scan
                                            //     not used in current implementation
        const lsn_t             undo_lsn    // In: undo lsn, the end point of backward scan
                                            //     not used in current implementation

        );

    // Child thread, used only if open system after Log Analysis phase while REDO and UNDO
    // will be performed with concurrent user transactions
    restart_thread_t*           _restart_thread;

    // Function used for concurrent REDO operations, page driven REDO
    static void                 redo_concurrent_pass();

    // Function used for concurrent UNDO operations, transaction driven UNDO
    static void                 undo_concurrent_pass();

    /*
     * SINGLE-PAGE RECOVERY (SPR)
     */

    // CS: These functions were moved from log_core
private:
    /**
    * \brief Collect relevant logs to recover the given page.
    * \ingroup Single-Page-Recovery
    * \details
    * This method starts from the log record at EMLSN and follows
    * the page-log-chain to go backward in the log file until
    * it hits a page-img log from which we can reconstruct the
    * page or it reaches the current_lsn.
    * Defined in log_spr.cpp.
    * \NOTE This method returns an error if the user had truncated
    * the transaction logs required for the recovery.
    * @param[in] pid ID of the page to recover.
    * @param[in] current_lsn the LSN the page is currently at.
    * @param[in] emlsn the LSN up to which we should recover the page.
    * @param[out] buffer into which the log records will be copied
    * @param[out] buffer_size end position of the log records in the buffer
    * @pre current_lsn < emlsn
    */
    static rc_t _collect_spr_logs(
        const PageID& pid, const lsn_t &current_lsn, const lsn_t &emlsn,
        char*& log_copy_buffer, size_t& buffer_size);

    /**
    * \brief Apply the given logs to the given page.
    * \ingroup Single-Page-Recovery
    * Defined in log_spr.cpp.
    * @param[in, out] p the page to recover.
    * @param[in] buffer buffer containing the log records to apply
    * @param[in] bufsize total usable size of buffer (i.e., the end)
    * @pre p is already fixed with exclusive latch
    */
    static rc_t _apply_spr_logs(fixable_page_h &p, char* buffer,
            size_t bufsize);


public:

    /**
     * \ingroup Single-Page-Recovery
     * Defined in log_spr.cpp.
     * @copydoc ss_m::dump_page_lsn_chain(std::ostream&, const PageID &, const lsn_t&)
     */
    static void dump_page_lsn_chain(std::ostream &o, const PageID &pid, const lsn_t &max_lsn);


    /**
    * \brief Apply single-page-recovery to the given page.
    * \ingroup Single-Page-Recovery
    * Defined in log_spr.cpp.
    * \NOTE This method returns an error if the user had truncated
    * the transaction logs required for the recovery.
    * @param[in, out] p the page to recover.
    * @param[in] emlsn the LSN up to which we should recover the page
    *            (null if EMLSN not available -- must scan log to find it)
    * @param[in] from_lsn true if we can use the last write lsn on the page as
    *            the starting point for recovery, do not rely on backup file only.
    * @pre p.is_fixed() (could be bufferpool managed or non-bufferpool managed)
    */
    static rc_t recover_single_page(fixable_page_h &p, const lsn_t& emlsn,
                                     const bool from_lsn = false);

private:
    // TODO(Restart)... it was for a space-recovery hack, not needed
    // keep track of tid from log record that we're redoing
    // for a horrid space-recovery handling hack
    // static tid_t                _redo_tid;

    // Function used for serialized operations, open system after the entire restart process finished
    // brief sub-routine of redo_pass() for logs that have pid.
    static void                 _redo_log_with_pid(
                                logrec_t& r,                   // In: Incoming log record
                                lsn_t &lsn,                    // In: LSN of the incoming log record
                                const lsn_t &end_logscan_lsn,  // In: This is the current LSN, validation purpose
                                PageID page_updated,           // In: Store ID (vol + store number) + page number
                                                               //      mainly used for multi-page log
                                bool &redone,                  // Out: did REDO occurred?  Validation purpose
                                uint32_t &dirty_count);        // Out: dirty page count, validation purpose


    // Function used for concurrent operations, open system after Log Analysis
    // Transaction driven UNDO phase, it handles both commit_lsn and lock acquisition
    static void                 _undo_txn_pass();

    // Function used for concurrent operations, open system after Log Analysis
    // Page driven REDO phase, it handles both commit_lsn and lock acquisition
    static void                 _redo_page_pass();


    // Helper function to process a system log record, called from Log Analysis pass
    static bool                 _analysis_system_log(
                                logrec_t& r,                 // In: Log record to process
                                lsn_t lsn,                   // In: LSN of the log record
                                uint32_t& in_doubt_count);   // In/out: in_doubt count

    // Helper function to process the chkpt_bf_tab log record, called from Log Analysis pass
    static void                 _analysis_ckpt_bf_log(
                                logrec_t& r,                 // In: Log record to process
                                uint32_t& in_doubt_count);   // In/out: in_doubt count

    // Helper function to process the chkpt_xct_tab log record, called from backward Log Analysis pass only
    static void                 _analysis_ckpt_xct_log(
                                logrec_t& r,                // In: Log record to process
                                lsn_t lsn,                  // In: LSN of current log record
                                tid_CLR_map& mapCLR);       // In/Out: map to hold counters for in-flight transactions

    // Helper function to process the t_chkpt_dev_tab log record, called from Log Analysis pass
    static void                 _analysis_ckpt_dev_log(
                                logrec_t& r,                // In: Log record to process
                                bool& mount);               // Out: whether the mount occurred

    // Helper function to process the rest of meaningful log records, called from Log Analysis pass
    static void                 _analysis_other_log(
                                logrec_t& r,                // In: log record
                                lsn_t lsn,                  // In: LSN for the log record
                                uint32_t& in_doubt_count,   // Out: in_doubt_count
                                xct_t *xd);                 // In: associated txn object

    // Helper function to process lock for the meaningful log records, called from backward Log Analysis pass only
    static void                 _analysis_process_lock(
                                logrec_t& r,                // In: Current log record
                                tid_CLR_map& mapCLR,        // In/Out: Map to track undecided in-flight transactions
                                XctLockHeap& lock_heap,     // Out: Heap to gather all re-acquired locks
                                xct_t *xd);                 // In: Associated transaction

    // Helper function to process the lock re-acquisition based on the log record, called from backward log analysis only
    static void                 _analysis_acquire_lock_log(
                                logrec_t& r,               // In: log record
                                xct_t *xd,                 // In: associated txn object
                                XctLockHeap& lock_heap);   // Out: heap to gather lock info

    // Helper function to process the lock re-acquisition for one active transaction in checkpoint log record, called from backward log analysis only
    static void                 _analysis_acquire_ckpt_lock_log(
                                logrec_t& r,              // In: log record
                                xct_t *xd,                // In: associated txn object
                                XctLockHeap& lock_heap);  // Out: heap to gather lock info

    // Helper function to process the extra mount operation, called from Log Analysis pass
    // static void                 _analysis_process_extra_mount(
    //                             lsn_t& theLastMountLSNBeforeChkpt,  // In/Out: last LSN
    //                             lsn_t& redo_lsn,                    // In: starting point of REDO log scan
    //                             bool& mount);                       // Out: whether mount occurred

    // Helper function to process the compensation map , called from backward log analysis only
    static void                 _analysis_process_compensation_map(
                                tid_CLR_map& mapCLR);     // In: map to track log record count for all undecided in-flight transaction

    // Helper function to process the transaction table after finished log scan, called from Log Analysis pass
    static void                 _analysis_process_txn_table(
                                XctPtrHeap& heap,     // Out: heap to store all in-flight transactions, for serial mode only
                                lsn_t& commit_lsn);   // In/Out: update commit_lsn value

    // Helper function to add one entry into the lock heap
    static void                 _re_acquire_lock(
                                XctLockHeap& lock_heap, // In: heap to record all re-acquired locks
                                const okvl_mode& mode,  // In: lock mode to acquire
                                const uint32_t hash,    // In: hash value of the lock to acquire
                                xct_t* xd);             // In: associated txn object

    // Helper function to compare entries in two lock heaps, tracking and debugging purpose
    // Current usage:
    //     Heap 1: from Log Analysis
    //     Heap 2: from checkpoint after Log Analysis
    static void                 _compare_lock_entries(
                                XctLockHeap& lock_heap1,   // In/out: first heap for the comparision, contains lock entries
                                XctLockHeap& lock_heap2);  // In/out: second heap for the comparision, contains lock entries

    // Helper function to print and clean up all entries in a heap
    static void                 _print_lock_entries(
                                XctLockHeap& lock_heap);   // In: heap object contains lock entries

public:
    // TODO(Restart)... it was for a space-recovery hack, not needed
    // tid_t                        *redo_tid() { return &_redo_tid; }

};


class AutoTurnOffLogging {
         bool _original_value;
    public:
        AutoTurnOffLogging()
        {
            w_assert1(smlevel_0::logging_enabled);
            _original_value = smlevel_0::logging_enabled;
            smlevel_0::logging_enabled = false;
        };

        ~AutoTurnOffLogging()
        {
            w_assert1(!smlevel_0::logging_enabled);
            // restore original value
            smlevel_0::logging_enabled = _original_value;
        };
    private:
        AutoTurnOffLogging& operator=(const AutoTurnOffLogging&);
        AutoTurnOffLogging(const AutoTurnOffLogging&);
};


#endif
