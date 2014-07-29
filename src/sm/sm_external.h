/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef SM_EXTERNAL_H
#define SM_EXTERNAL_H

// Definitions exposed to caller for testing purpose.

// Return code when asking for the status of a given restart phase: Log Analysis, REDO, UNDO
enum restart_phase_t {
    t_restart_phase_unknown,      // unknown status, mainly for M3 pure on-demand REDO and UNDO
    t_restart_phase_active,       // The phase is currently active
    t_restart_phase_not_active,   // The phase is currently not active, not started yet
    t_restart_phase_done          // The phase is done
};


////////////////////////////////////////
// Set the internal restart mode to control which restart
// logic (mainly for REDO and UNDO phases) to use during 
// the restart process
// These modes are for testing purpose only, the final code 
// should use features based on test results and the most
// stable implementations.
////////////////////////////////////////

// Define the supported modes for Restart process
// not all bit combinations would be supported or tested
// use int64_t to make sure we have enough bits:

// Basic modes:
// Milestone 1
const int64_t m1_default_restart =          // sm_restart = 10 (default)
    smlevel_0::t_restart_serial |           // Serial operation
    smlevel_0::t_restart_redo_log |         // Log scan driven REDO
    smlevel_0::t_restart_undo_reverse;      // Reverse UNDO

// Milestone 2 with minimal logging    
const int64_t m2_default_restart =          // sm_restart = 20
    smlevel_0::t_restart_concurrent_log |   // Concurrent operation using log                << new
    smlevel_0::t_restart_redo_page |        // Page driven REDO with minimal logging   << new
    smlevel_0::t_restart_undo_txn;          // Transaction driven UNDO                       << new   
const int64_t m2_redo_delay_restart =       // sm_restart = 21, concurrent testing purpose
    smlevel_0::t_restart_concurrent_log |   // Concurrent operation using log
    smlevel_0::t_restart_redo_page |        // Page driven REDO with minimal logging
    smlevel_0::t_restart_undo_txn |         // Transaction driven UNDO
    smlevel_0::t_restart_redo_delay;        // Delay before REDO                              << new
const int64_t m2_undo_delay_restart =       // sm_restart = 22, concurrent testing purpose
    smlevel_0::t_restart_concurrent_log |   // Concurrent operation using log
    smlevel_0::t_restart_redo_page |        // Page driven REDO with minimal logging
    smlevel_0::t_restart_undo_txn |         // Transaction driven UNDO
    smlevel_0::t_restart_undo_delay;        // Delay before UNDO                              << new    
const int64_t m2_both_delay_restart =       // sm_restart = 23, concurrent testing purpose
    smlevel_0::t_restart_concurrent_log |   // Concurrent operation using log
    smlevel_0::t_restart_redo_page |        // Page driven REDO with minimal logging
    smlevel_0::t_restart_undo_txn |         // Transaction driven UNDO
    smlevel_0::t_restart_redo_delay |       // Delay before REDO                              << new
    smlevel_0::t_restart_undo_delay;        // Delay before UNDO                              << new

// Milestone 2 with full logging    
const int64_t m2_full_logging_restart =      // sm_restart = 24
    smlevel_0::t_restart_concurrent_log |    // Concurrent operation using log             << new
    smlevel_0::t_restart_redo_full_logging | // Page driven REDO with full logging       << new
    smlevel_0::t_restart_undo_txn;           // Transaction driven UNDO                    << new
const int64_t m2_redo_fl_delay_restart =     // sm_restart = 25, concurrent testing purpose
    smlevel_0::t_restart_concurrent_log |    // Concurrent operation using log
    smlevel_0::t_restart_redo_full_logging | // Page driven REDO with full logging
    smlevel_0::t_restart_undo_txn |          // Transaction driven UNDO
    smlevel_0::t_restart_redo_delay;         // Delay before REDO                              << new
const int64_t m2_undo_fl_delay_restart =     // sm_restart = 26, concurrent testing purpose
    smlevel_0::t_restart_concurrent_log |    // Concurrent operation using log
    smlevel_0::t_restart_redo_full_logging | // Page driven REDO with full logging
    smlevel_0::t_restart_undo_txn |          // Transaction driven UNDO
    smlevel_0::t_restart_undo_delay;         // Delay before UNDO                              << new    
const int64_t m2_both_fl_delay_restart =     // sm_restart = 27, concurrent testing purpose
    smlevel_0::t_restart_concurrent_log |    // Concurrent operation using log
    smlevel_0::t_restart_redo_full_logging | // Page driven REDO with full logging
    smlevel_0::t_restart_undo_txn |          // Transaction driven UNDO
    smlevel_0::t_restart_redo_delay |        // Delay before REDO                              << new
    smlevel_0::t_restart_undo_delay;         // Delay before UNDO                              << new

// Alternative modes:
// Compare with m2_default_restart, difference in REDO 
const int64_t alternative_log_log_restart =     // sm_restart = 70
    smlevel_0::t_restart_concurrent_log |       // Concurrent operation using log 
    smlevel_0::t_restart_redo_log |             // Log scan driven REDO             << compare with sm_restart 20
    smlevel_0::t_restart_undo_txn;              // Transaction driven UNDO
// Compare with m2_default_restart, difference in concurrent
const int64_t alternative_lock_page_restart =   // sm_restart = 80
    smlevel_0::t_restart_concurrent_lock |      // Concurrent operation using lock  << compare with sm_restart 20
    smlevel_0::t_restart_redo_page |            // Page driven REDO with minimal logging
    smlevel_0::t_restart_undo_txn;              // Transaction driven UNDO
// Compare with alternative_log_log_restart, difference in concurrent
const int64_t alternative_lock_log_restart =    // sm_restart = 90
    smlevel_0::t_restart_concurrent_lock |      // Concurrent operation using lock  << compare with sm_restart 70
    smlevel_0::t_restart_redo_log |             // Log scan driven REDO
    smlevel_0::t_restart_undo_txn;              // Transaction driven UNDO

// Milestone 3 with minimal logging        
const int64_t m3_default_restart =          // sm_restart = 30
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock          << new
    smlevel_0::t_restart_redo_demand |      // On-demand driven REDO                  << new
    smlevel_0::t_restart_undo_demand;       // On-demand driven UNDO

// Milestone 4 with minimal logging        
const int64_t m4_default_restart =          // sm_restart = 40
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock
    smlevel_0::t_restart_redo_mix |         // Mixed REDO                                   << new
    smlevel_0::t_restart_undo_mix;          // Mixed UNDO
const int64_t m4_redo_delay_restart =       // sm_restart = 41, concurrent testing purpose
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock
    smlevel_0::t_restart_redo_mix |         // Mixed REDO                                   << new
    smlevel_0::t_restart_undo_mix |         // Mixed UNDO
    smlevel_0::t_restart_redo_delay;        // Delay before REDO                         << new        
const int64_t m4_undo_delay_restart =       // sm_restart = 42, concurrent testing purpose
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock
    smlevel_0::t_restart_redo_mix |         // Mixed REDO                                   << new
    smlevel_0::t_restart_undo_mix |         // Mixed UNDO
    smlevel_0::t_restart_undo_delay;        // Delay before UNDO                         << new        
const int64_t m4_both_delay_restart =       // sm_restart = 43, concurrent testing purpose
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock
    smlevel_0::t_restart_redo_mix |         // Mixed REDO                                   << new
    smlevel_0::t_restart_undo_mix |         // Mixed UNDO
    smlevel_0::t_restart_redo_delay |       // Delay before REDO                          << new
    smlevel_0::t_restart_undo_delay;        // Delay before REDO                          << new


#endif          /*</std-footer>*/
