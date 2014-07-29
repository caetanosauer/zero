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

#ifndef BF_S_H
#include <bf_s.h>
#endif

#include "sm_int_1.h"

// Used for:
//    internal wait for testing purpose
//    normal shutdown wait
const uint32_t wait_interval = 1000;   // 1 seconds

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

// Class restart_thread_t
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
    smlevel_0::concurrent_restart_mode_t in_recovery() { return working; }

private:

    smlevel_0::concurrent_restart_mode_t working;  // what is the child working on?

private:
    // disabled
    NORET restart_thread_t(const restart_thread_t&);
    restart_thread_t& operator=(const restart_thread_t&);

};

class restart_m : public smlevel_1 {
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
                
                if (smlevel_0::t_concurrent_done != _restart_thread->in_recovery())
                {
                    DBGOUT1(<< "Child thread is still busy, extra sleep");
                    g_me()->sleep(wait_interval*2); // 2 second, this is a very long time
                }
                if (smlevel_0::t_concurrent_done != _restart_thread->in_recovery())
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
        // M4 - mixed mode, both M2 and M4
        
        // Restart is in progress if one of the conditions is true:
        // Serial mode (M1) and 
        //     Operating mode is not in t_forward_processing
        // Concurrent mode (M2 and M4) and:
        //     Workign on Log Analysis
        //     or
        //     Child thread is alive
        // On-demand mode (M3):
        //     Workign on Log Analysis
        //     or
        //     If passed Log Analysis phase, unknow whether we are still in REDO or UNDO phase
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
                // Not pure on-demand (M3)
                // For M2 and M4, using child restart thread to determine the current status
                // if the child thread exists
                if (_restart_thread)
                {
                    if (smlevel_0::t_concurrent_done != _restart_thread->in_recovery())
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
            if (smlevel_0::t_concurrent_redo == _restart_thread->in_recovery())            
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
            // M2 or M4
            if (smlevel_0::t_concurrent_undo == _restart_thread->in_recovery()) 
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

    // Shared by all recovery modes
    static void                 analysis_pass(
        const lsn_t             master,
        lsn_t&                  redo_lsn,
        uint32_t&               in_doubt_count,
        lsn_t&                  undo_lsn,
        XctPtrHeap&             heap,
        lsn_t&                  commit_lsn, // Commit lsn for concurrent transaction (if used)        
        lsn_t&                  last_lsn    // Last lsn in recovery log (forward scan)
        );

    // Function used for log scan REDO operations, open system after the entire recovery process finished
    static void                 redo_log_pass(
        const lsn_t              redo_lsn, 
        const lsn_t              &highest,  /* for debugging */
        const uint32_t           in_doubt_count  // How many in_doubt pages in buffer pool
        );

    // Function used for reverse order UNDO operations, open system after the entire recovery process finished
    static void                 undo_reverse_pass(
        XctPtrHeap&             heap,       // heap populated with doomed transactions
        const lsn_t             curr_lsn,   // current lsn, the starting point of backward scan
                                            // not used in current implementation
        const lsn_t             undo_lsn    // undo lsn, the end point of backward scan
                                            // not used in current implementation        

        );

    // Child thread, used only if open system after Log Analysis phase while REDO and UNDO
    // will be performed with concurrent user transactions
    restart_thread_t*           _restart_thread;

    // Function used for concurrent operations, open system after Log Analysis
    static void                 redo_concurrent();

    // Function used for concurrent operations, open system after Log Analysis
    static void                 undo_concurrent();

private:
    // TODO(Restart)... it was for a space-recovery hack, not needed
    // keep track of tid from log record that we're redoing
    // for a horrid space-recovery handling hack
    // static tid_t                _redo_tid;

    // Function used for serialized operations, open system after the entire recovery process finished
    // brief sub-routine of redo_pass() for logs that have pid.    
    static void                 _redo_log_with_pid(
        logrec_t& r, lsn_t &lsn, const lsn_t &end_logscan_lsn,
        lpid_t page_updated, bool &redone, uint32_t &dirty_count);

    // Function used for concurrent operations, open system after Log Analysis
    // The function could be used for serialized operation with some minor work
    // Transaction driven UNDO phase, it handles both commit_lsn and lock acquisition
    static void                 _undo_txn_pass();

    // Function used for concurrent operations, open system after Log Analysis
    // Page driven REDO phase, it handles both commit_lsn and lock acquisition
    static void                 _redo_page_pass();

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
