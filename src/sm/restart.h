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
        working = false;
    };
    NORET ~restart_thread_t()
    {
        DBGOUT1(<< "restart_thread_t: Exiting child thread");
    };

    // Main body of the child thread
    void run();
    bool in_restart() { return working; }

private:

    bool working;

private:
    // disabled
    NORET restart_thread_t(const restart_thread_t&);
    restart_thread_t& operator=(const restart_thread_t&);

};

class restart_m {
    friend class restart_thread_t;

public:
    restart_m(const sm_options&);
    ~restart_m();

    // Function used for concurrent operations, open system after Log Analysis
    // we need a child thread to carry out the REDO and UNDO operations
    // while concurrent user transactions are coming in
    void spawn_recovery_thread()
    {
        // CS TODO: concurrency?

        DBGOUT1(<< "Spawn child recovery thread");

        _restart_thread = new restart_thread_t;
        W_COERCE(_restart_thread->fork());
        w_assert1(_restart_thread);
    }

    // Return true if the restart operation is still on-going
    bool restart_in_progress()
    {
        // CS TODO: delete
        return true;
    }

    bool redo_in_progress()
    {
        // CS TODO: delete
        return true;
    }

    bool undo_in_progress()
    {
        // CS TODO: delete
        return true;
    }


    // Top function to start the restart process
    void                 restart(
        lsn_t                   master,         // In: Starting point for log scan
        lsn_t&                  commit_lsn,     // Out: used if use_concurrent_log_restart()
        lsn_t&                  redo_lsn,       // Out: used if log driven REDO with use_concurrent_XXX_restart()
        lsn_t&                  last_lsn,       // Out: used if page driven REDO with use_concurrent_XXX_restart()
        uint32_t&               in_doubt_count  // Out: used if log driven REDO with use_concurrent_XXX_restart()
        );

private:

    bool instantRestart;

    // Shared by all restart modes
    void                 log_analysis(
        bool                    restart_with_lock,
        lsn_t&                  redo_lsn,
        lsn_t&                  undo_lsn,
        lsn_t&                  commit_lsn,
        lsn_t&                  last_lsn,
        uint32_t&               in_doubt_count
    );

    // Function used for log scan REDO operations
    void                 redo_log_pass(
        const lsn_t              redo_lsn,       // In: Starting point for REDO forward log scan
        const lsn_t              &highest,       // Out: for debugging
        const uint32_t           in_doubt_count  // In: How many in_doubt pages in buffer pool
        );

    // Child thread, used only if open system after Log Analysis phase while REDO and UNDO
    // will be performed with concurrent user transactions
    restart_thread_t*           _restart_thread;

    // Function used for concurrent REDO operations, page driven REDO
    void                 redo_concurrent_pass();

    // Function used for concurrent UNDO operations, transaction driven UNDO
    void                 undo_concurrent_pass();

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
    // Function used for serialized operations, open system after the entire restart process finished
    // brief sub-routine of redo_pass() for logs that have pid.
    void                 _redo_log_with_pid(
                                logrec_t& r,                   // In: Incoming log record
                                lsn_t &lsn,                    // In: LSN of the incoming log record
                                const lsn_t &end_logscan_lsn,  // In: This is the current LSN, validation purpose
                                PageID page_updated,           // In: Store ID (vol + store number) + page number
                                                               //      mainly used for multi-page log
                                bool &redone,                  // Out: did REDO occurred?  Validation purpose
                                uint32_t &dirty_count);        // Out: dirty page count, validation purpose


    // Function used for concurrent operations, open system after Log Analysis
    // Transaction driven UNDO phase, it handles both commit_lsn and lock acquisition
    void                 _undo_txn_pass();

    // Function used for concurrent operations, open system after Log Analysis
    // Page driven REDO phase, it handles both commit_lsn and lock acquisition
    void                 _redo_page_pass();

    // Helper function to add one entry into the lock heap
    void                 _re_acquire_lock(
                                const okvl_mode& mode,  // In: lock mode to acquire
                                const uint32_t hash,    // In: hash value of the lock to acquire
                                xct_t* xd);             // In: associated txn object

};

#endif
