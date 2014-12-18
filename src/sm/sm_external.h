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
// use int32_t, if not enough bits in the future, change it to int64_t

// Basic modes:
// Milestone 1, log driven REDO, reverse txn UNDO
const int32_t m1_default_restart =          // sm_restart, serial and default (if caller did not specify mode)
    smlevel_0::t_restart_serial |           // Serial operation
    smlevel_0::t_restart_redo_log |         // Log scan driven REDO
    smlevel_0::t_restart_undo_reverse;      // Reverse UNDO

// Milestone 2 with minimal logging, page driven REDO, txn driven UNDO, commit_lsn
const int32_t m2_default_restart =          // sm_restart, M2 minimal logging
    smlevel_0::t_restart_concurrent_log |   // Concurrent operation using log                << new
    smlevel_0::t_restart_redo_page |        // Page driven REDO with minimal logging   << new
    smlevel_0::t_restart_undo_txn;          // Transaction driven UNDO                       << new   
const int32_t m2_redo_delay_restart =       // sm_restart, concurrent testing purpose
    smlevel_0::t_restart_concurrent_log |   // Concurrent operation using log
    smlevel_0::t_restart_redo_page |        // Page driven REDO with minimal logging
    smlevel_0::t_restart_undo_txn |         // Transaction driven UNDO
    smlevel_0::t_restart_redo_delay;        // Delay before REDO                              << new
const int32_t m2_undo_delay_restart =       // sm_restart, concurrent testing purpose
    smlevel_0::t_restart_concurrent_log |   // Concurrent operation using log
    smlevel_0::t_restart_redo_page |        // Page driven REDO with minimal logging
    smlevel_0::t_restart_undo_txn |         // Transaction driven UNDO
    smlevel_0::t_restart_undo_delay;        // Delay before UNDO                              << new    
const int32_t m2_both_delay_restart =       // sm_restart, concurrent testing purpose
    smlevel_0::t_restart_concurrent_log |   // Concurrent operation using log
    smlevel_0::t_restart_redo_page |        // Page driven REDO with minimal logging
    smlevel_0::t_restart_undo_txn |         // Transaction driven UNDO
    smlevel_0::t_restart_redo_delay |       // Delay before REDO                              << new
    smlevel_0::t_restart_undo_delay;        // Delay before UNDO                              << new

// Milestone 2 with self_contained logging, page driven REDO, txn driven UNDO, commit_lsn
const int32_t m2_alt_rebalance_restart =    // sm_restart, M2 minimal logging
    smlevel_0::t_restart_concurrent_log |   // Concurrent operation using log                << new
    smlevel_0::t_restart_redo_page |        // Page driven REDO with minimal logging   << new
    smlevel_0::t_restart_undo_txn |         // Transaction driven UNDO                       << new   
    smlevel_0::t_restart_alt_rebalance;     // Use self_contained log record for page rebalance  << new

// Milestone 2 with full logging, page driven REDO, txn driven UNDO, commit_lsn
const int32_t m2_full_logging_restart =      // sm_restart, M2 full logging
    smlevel_0::t_restart_concurrent_log |    // Concurrent operation using log             << new
    smlevel_0::t_restart_redo_full_logging | // Page driven REDO with full logging       << new
    smlevel_0::t_restart_undo_txn;           // Transaction driven UNDO                    << new
const int32_t m2_redo_fl_delay_restart =     // sm_restart, concurrent testing purpose
    smlevel_0::t_restart_concurrent_log |    // Concurrent operation using log
    smlevel_0::t_restart_redo_full_logging | // Page driven REDO with full logging
    smlevel_0::t_restart_undo_txn |          // Transaction driven UNDO
    smlevel_0::t_restart_redo_delay;         // Delay before REDO                              << new
const int32_t m2_undo_fl_delay_restart =     // sm_restart, concurrent testing purpose
    smlevel_0::t_restart_concurrent_log |    // Concurrent operation using log
    smlevel_0::t_restart_redo_full_logging | // Page driven REDO with full logging
    smlevel_0::t_restart_undo_txn |          // Transaction driven UNDO
    smlevel_0::t_restart_undo_delay;         // Delay before UNDO                              << new    
const int32_t m2_both_fl_delay_restart =     // sm_restart, concurrent testing purpose
    smlevel_0::t_restart_concurrent_log |    // Concurrent operation using log
    smlevel_0::t_restart_redo_full_logging | // Page driven REDO with full logging
    smlevel_0::t_restart_undo_txn |          // Transaction driven UNDO
    smlevel_0::t_restart_redo_delay |        // Delay before REDO                              << new
    smlevel_0::t_restart_undo_delay;         // Delay before UNDO                              << new

// Alternative modes:
// Compare with m2_default_restart, difference in REDO 
const int32_t alternative_log_log_restart =     // sm_restart, measurement purpose
    smlevel_0::t_restart_concurrent_log |       // Concurrent operation using log 
    smlevel_0::t_restart_redo_log |             // Log scan driven REDO             << compare with sm_restart 20
    smlevel_0::t_restart_undo_txn;              // Transaction driven UNDO
// Compare with m2_default_restart, difference in concurrent
const int32_t alternative_lock_page_restart =   // sm_restart, measurement purpose
    smlevel_0::t_restart_concurrent_lock |      // Concurrent operation using lock  << compare with sm_restart 20
    smlevel_0::t_restart_redo_page |            // Page driven REDO with minimal logging
    smlevel_0::t_restart_undo_txn;              // Transaction driven UNDO
// Compare with alternative_log_log_restart, difference in concurrent
const int32_t alternative_lock_log_restart =    // sm_restart, measurement purpose
    smlevel_0::t_restart_concurrent_lock |      // Concurrent operation using lock  << compare with sm_restart 70
    smlevel_0::t_restart_redo_log |             // Log scan driven REDO
    smlevel_0::t_restart_undo_txn;              // Transaction driven UNDO

// Milestone 3 with minimal logging, pure on_demand REDO and UNDO, lock conflict
const int32_t m3_default_restart =          // sm_restart, M3
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock          << new
    smlevel_0::t_restart_redo_demand |      // On-demand driven REDO                  << new
    smlevel_0::t_restart_undo_demand;       // On-demand driven UNDO

// Milestone 3 with self_contained logging, pure on_demand REDO and UNDO, lock conflict
const int32_t m3_alt_rebalance_restart =    // sm_restart, M3 minimal logging
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock          << new
    smlevel_0::t_restart_redo_demand |      // On-demand driven REDO                  << new
    smlevel_0::t_restart_undo_demand |      // On-demand driven UNDO
    smlevel_0::t_restart_alt_rebalance;     // Use self_contained log record for page rebalance  << new

// Milestone 4 with minimal logging, page driven REDO, txn driven UNDO, on_demand REDO/UNDO, lock conflict
const int32_t m4_default_restart =          // sm_restart, M4
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock
    smlevel_0::t_restart_redo_mix |         // Mixed REDO                                   << new
    smlevel_0::t_restart_undo_mix;          // Mixed UNDO
const int32_t m4_redo_delay_restart =       // sm_restart, concurrent testing purpose
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock
    smlevel_0::t_restart_redo_mix |         // Mixed REDO                                   << new
    smlevel_0::t_restart_undo_mix |         // Mixed UNDO
    smlevel_0::t_restart_redo_delay;        // Delay before REDO                         << new        
const int32_t m4_undo_delay_restart =       // sm_restart, concurrent testing purpose
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock
    smlevel_0::t_restart_redo_mix |         // Mixed REDO                                   << new
    smlevel_0::t_restart_undo_mix |         // Mixed UNDO
    smlevel_0::t_restart_undo_delay;        // Delay before UNDO                         << new        
const int32_t m4_both_delay_restart =       // sm_restart, concurrent testing purpose
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock
    smlevel_0::t_restart_redo_mix |         // Mixed REDO                                   << new
    smlevel_0::t_restart_undo_mix |         // Mixed UNDO
    smlevel_0::t_restart_redo_delay |       // Delay before REDO                          << new
    smlevel_0::t_restart_undo_delay;        // Delay before REDO                          << new

// Milestone 4 with self_contained logging, page driven REDO, txn driven UNDO, on_demand REDO/UNDO, lock conflict
const int32_t m4_alt_rebalance_restart =    // sm_restart, M4
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock
    smlevel_0::t_restart_redo_mix |         // Mixed REDO                                   << new
    smlevel_0::t_restart_undo_mix |         // Mixed UNDO
    smlevel_0::t_restart_alt_rebalance;     // Use self_contained log record for page rebalance  << new

// Milestone 5 ARIES method - page driven REDO, txn driven UNDO, lock conflict, open after REDO before UNDO
const int32_t m5_default_restart =          // sm_restart, M3
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock
    smlevel_0::t_restart_redo_mix |         // Mixed REDO
    smlevel_0::t_restart_undo_mix |         // Mixed UNDO            
    smlevel_0::t_restart_aries_open;        // Open system after REDO                  << new

// Milestone 5 ARIES method with self_contained logging, page driven REDO, txn driven UNDO, lock conflict, open after REDO before UNDO
const int32_t m5_alt_rebalance_restart =    // sm_restart, M3
    smlevel_0::t_restart_concurrent_lock |  // Concurrent operation using lock
    smlevel_0::t_restart_redo_mix |         // Mixed REDO
    smlevel_0::t_restart_undo_mix |         // Mixed UNDO            
    smlevel_0::t_restart_aries_open |       // Open system after REDO                  << new
    smlevel_0::t_restart_alt_rebalance;     // Use self_contained log record for page rebalance  << new


#endif          /*</std-footer>*/
