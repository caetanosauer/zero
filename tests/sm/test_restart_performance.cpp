#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"
#include "xct.h"
#include "sm_base.h"
#include "sm_external.h"
#include "sthread.h"
#include "../fc/local_random.h"

// For CPU cycle measurement
#include <cstdlib>
#include <vector>
#include <algorithm>

// For elapsed time
#include <unistd.h>    // POSIX flags
#include <time.h>      // clock_gettime(), time()
#include <sys/time.h>  // gethrtime(), gettimeofday()

#include <limits>       // std::numeric_limits

// Restart performance test to compare various restart methods
// M1 -traditional restart, open system after Restart finished
//        all operations in post_shutdown should succeed
// M2 -Enhanced traditional restart, open system after Log Analysis phase,
//        concurrency check based on commit_lsn
//        restart carried out by restart child thread
//        some operations in post_shutdown might fail
// M3 -On_demand restart, open system after Log Analysis phase,
//        concurrency check based on lock acquisition
//        restart carried out by user transaction threads
//        all operations in post_shutdown should succeed although some
//        operations might take longer (on_demand recovery)
// M4 -Mixed restart, open system after Log Analysis phase,
//        concurrency check based on lock acquisition
//        restart carried out by both restart child thread and user thransaction threads
//        all operations in post_shutdown should succeed although some
//        operations might take longer (on_demand recovery)

// Test machine:
//       Xeon CPU X5550 @ 2.67GHz
//       CPU MHz 1596.000 (cat /proc/cpuinfo current CPU speed)
//       CPU cores 4
//       RAM 6GB (free -mt) with 6M of swap space (swap space is used when the physical RAM is full)


// TODO(Restart)...
//    Might need to change the report frequency
//
//    Look for 'TODO(Restart)... performance', these are the special changes for performance measurement purpose
//
//    Restart - in_doubt page and transaction count reporting, change from DBGOUT0 back to DBGOUT1 (2 - 3 locations)
//
//    restart.cpp - change from DBGOUT0 back to DBGOUT1 for restart finished timing (2 locations)
//
//    chkpt - change from DBGOUT0 back to DBGOUT1 (1 locations) for reporting
//
//    Bug: Uncomment out the assertion in UNDO (two locations) - btree_logrec.cpp, lines 61 and 153  <-- M3/M4 only
//
//    CMakeLists.txt - comment out test_restart_performance for the regular build
//
//    Backward log scan code from Caetano - do not check in Caetano's code
//
//    logbuf_common.h - disable #define LOG_BUFFER to use the original log scan
//    partition.h
//    partition.cpp
//    log_core.cpp
//
//    Bug: bf_tree.cpp (3020) bf_tree_m::_try_recover_page: Parent page does not have emlsn, no recovery  <-- infinite loop sometimes but not consistent
//
//    Bug: btree_page_h::suggest_fence_for_split (btree_page_h.cpp:1127) - when spliting a page to create a foster relation,
//                try to find the key for split, it does not exist, it appears that a synch() call at the beginning of pre_shutdown causes this error consistently
//


// Control concurrent transactions exit criteria in post_shutdown (phase 3)
enum concurrent_xct_exit_t
{
    no_txn    = 0x0,   // Do not execute any user transaction
    txn_count = 0x1,   // Based on successful transaction count
    txn_time  = 0x2    // Based on execution time
};

// TODO(Restart)... begin

// Uncomment the following line  to run CPU cycle performance measurment...
//
#define MEASURE_PERFORMANCE 1

// !!! Determine exit criteria of concurrent transactions !!!
const concurrent_xct_exit_t EXEC_TYPE     = no_txn;

// TODO(Restart)... end


// Define constants
const double   TARGET_EXECUTION_TIME      = 60000.0;        // Duration of the post_shutdown execution time
const int      TOTAL_INDEX_COUNT          = 5;              // Number of indexes.  Each index would have around 9000 pages (probably more),
                                                            // with 8K page, each index would take up at least ~70MB of space, while
                                                            // we have 1GB buffer pool space
                                                            // On the test machine, during the initial population phase, it starts hitting
                                                            // the hard drive after the first 5 indexes (file system cache was full)
const int      DIRTY_INDEX_COUNT          = 5;              // Number of indexes to be updated after the initial population
const int      TOTAL_IN_FLIGHT_THREADS    = 20;             // Worker threads for in-flight transactions, this is the number of in_flight transactions
const uint64_t POPULATE_RECORDS           = 60000;          // Record count per big population, it is used in initial_shutdown phase to
                                                            // pre-populated the empty store.
                                                            // With record skipping (based on the last number in key), we would insert
                                                            // 42000 records each time and would use just below 10000 pages each call
const uint64_t TOTAL_RECOVERY_TRANSACTION = 10000;          // Total transactions during phase 2 (pre_shutdown), all these transactions needed
                                                            // to be recovered after system crash, 10000 is about as high as I can go without
                                                            // hittning weird bugs
const int      TOTAL_SUCCESS_TRANSACTIONS = 4500;           // Target total completed concurrent transaction count in phase 3 (post_shutdown),
                                                            // the time measurement is based on this transaction count
                                                            // For M4 has bugs related to memory, can oly go as high as 500
                                                            // For M2, if set it higher than 4500, cannot get to the asked successful transaction
                                                            //    count, recovery finished but core dump at the end
const int      M4_SUCCESS_TRANSACTIONS    = 500;            // M4 target total completed concurrent transaction count in phase 3 (post_shutdown)
                                                            // lower number due to bugs
const int      REPORT_COUNT               = 10;             // How many times to take the execution time information
const int      RECORD_FREQUENCY           = TOTAL_SUCCESS_TRANSACTIONS/REPORT_COUNT; // Record the time every RECORD_FREQUENCY successful
                                                            // transactions, so we get REPORT_COUNT reports.  Ignore failure transactions
const int      TOTAL_CYCLE_SLOTS          = TOTAL_SUCCESS_TRANSACTIONS/RECORD_FREQUENCY+1; // Total avaliable slots to store time information,
                                                            // this is only for information and graph dwaring purposes, to show the behavior of
                                                            // different methods/milestones
const int      OUT_OF_BOUND_FREQUENCY     = 5;              // During concurrent updates in phase 3 (post_shutdown), insert some new
                                                            // records which should not triger on_demand recovery, this number is to
                                                            // control the frequency of inserting this type of new record, meaning for every
                                                            // OUT_OF_BOUND_FREQUENCY update operations, one would be a new
                                                            // out-of-bound insertion which should not trigger on_demand recovery in M3
const uint32_t SHORT_SLEEP_MICROSEC       = 20000;          // Sleep time before start a child thread, this is allow thread manager to catch up
                                                            // if we are going to create many child threads for in_flight transactions
const int64_t  DISK_QUOTA_IN_KB           = ((int64_t) 1 << 21); // 2GB, disk space quota for the entire operation
const uint64_t NO_EXTRA_UPDATE            = 0;              // For the transactions during phase 2 (pre_shutdown), we can piggy back some extra
                                                            // update operations in the same transaction to generate more dirty pages
                                                            // Use NO_EXTRA_UPDATE if we do not want to piggy back extra update
const uint64_t SEED_INCREMENT             = 10;             // In phase 2 (pre_shutdown), each transaction can have extra update operations to generate
                                                            // more dirty pages for each transaction.  If extra update was used, then a seed
                                                            // value would be given, the SEED_INCREMENT is used to perform multiple update
                                                            // operations, SEED_INCREMENT is set to 10 so the ending key value does not change
                                                            // per iteration
const int      EXTRA_UPDATE_COUNT         = 10;             // If extra update operations were used in phase 2 (pre_shutdown) per transaction, this
                                                            // is to control how many extra update operations per transaction
const double   MICROSECONDS_IN_SECOND     = 1000000.0;      // microseconds in a second
const double   MILLISECS_IN_SECOND        = 1000.0;         // millisecs in a second
const int      KEY_BUF_SIZE               = 10;             // Max. key size of the record
const int      DATA_BUF_SIZE              = 800;            // btree_m::max_entry_size() is calculated as 1620 bytes currently
                                                            // the calculation is based on a 8K page to have sufficient space to
                                                            // hold at least 2 max. size user records plus space needed for internal
                                                            // data structures (include overhead and 3 max. size reocrds for high, low
                                                            // and fence records)
                                                            // For performance testing purpose, we want a B-tree to span over many
                                                            // pages, therefore we are using larger record size and many records
                                                            // (~POPULATE_RECORDS) in each B-tree
                                                            // With the current setting, record size is ~ 810 (key + data although
                                                            // key size might vary, but at most 10), so each page could hold ~ 6 records

// Data structure for the execution elapse time information for reporting purpose
struct elapse_info_t
{
    unsigned long long _total_cycles; // record CPU cycles
    double             _total_elapse; // record elapse time
    double             _total_txn_count;
    double             _successful_txn_count;
};

// Test environment
btree_test_env *test_env;

// Operation type for the in-flight worker thread
enum test_op_type_t
{
    t_op_insert,
    t_op_delete,
    t_op_update
};

// Helper function to set generic options
sm_options make_perf_options()
{
    // For Instarnt Restart performance run, we need larger buffer pool size,
    // it allows many in-memory dirty pages (more in_doubt pages to recovery)
    // Also use larger size of log, lock, disk space, and transaction just in case

    sm_options options;

    // Buffer pool
    options.set_int_option("sm_bufpoolsize", (1 << 20));               // in KB, 1GB, while the test machine has 6GB of memory
//    options.set_int_option("sm_bufpoolsize", (1 << 18));               // in KB, 256MB, while the test machine has 6GB of memory

    // Log, do not set 'sm_logdir' so we would use default
    // options.set_string_option("sm_logdir", global_log_dir);
    options.set_int_option("sm_logbufsize",  (1 << 20));               // In bytes, 1MB
    options.set_int_option("sm_logsize", (1 << 23));                   // In KB, 8GB

    // Lock
    options.set_int_option("sm_locktablesize", (1 << 16));             // 64KB
    options.set_int_option("sm_rawlock_lockpool_initseg", (1 << 8));   // 256 bytes
    options.set_int_option("sm_rawlock_lockpool_segsize", (1 << 13));  // 8KB
    options.set_int_option("sm_rawlock_gc_generation_count", 100);
    options.set_int_option("sm_rawlock_gc_interval_ms", 50);
    options.set_int_option("sm_rawlock_gc_free_segment_count", 30);
    options.set_int_option("sm_rawlock_gc_max_segment_count", 40);

    // Transaction
    options.set_int_option("sm_rawlock_xctpool_initseg", (1 << 8));    // 256 bytes
    options.set_int_option("sm_rawlock_xctpool_segsize", (1 << 9));    // 512 bytes

    // Cleaner
    options.set_int_option("sm_cleaner_interval_millisec_min", 1000);
    options.set_int_option("sm_cleaner_interval_millisec_max", 256000);
    options.set_int_option("sm_cleaner_write_buffer_pages", 64);

    // Misc.
    options.set_bool_option("sm_bufferpool_swizzle", false);           // Turn swizzling off, Restart does not support swizzling
    options.set_int_option("sm_num_page_writers", 1);
    options.set_bool_option("sm_backgroundflush", true);

    return options;
}

// Helper function for CPU cycles/ticks, exclude disk I/O
static __inline__ unsigned long long rdtsc(void)
{
    // The rdtsc might not be perfrect for performance measurement
    // the problem is that the CPU might throttle (with it does most of the time,
    // when all processes are blocked), so a cpu clock might mean more
    // time in that case. This is of course CPU and OS dependant.
    // Also this is to measure CPU cycles, it exclude disk I/O, therefore
    // it is not very useful for this performance measurement which is affected
    // by disk I/O
    //
    // Alternatively, get the elapsed time based on wall clock.

    unsigned int hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

// Helper function for the initial storage population
w_rc_t populate_performance_records(stid_t &stid,
                                          bool one_txn,           // True if one big txn for all insertions
                                          uint64_t seed_key_int,  // Starting key value
                                          const uint64_t count)   // Number of records to insert
{
    // Insert POPULATE_RECORDS records into the store
    //   Key - seed_key_int - (POPULATE_RECORDS + seed_key_int)
    //   Key - for key ending with 3, 5 or 7, skip the insertions

    // Get the initial record count before start
    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));
    int recordCount = s.rownum;

    // Now start the insertions
    char key_buf[KEY_BUF_SIZE+1];
    char data_buf[DATA_BUF_SIZE+1];
    memset(data_buf, '\0', DATA_BUF_SIZE+1);
    memset(data_buf, 'D', DATA_BUF_SIZE);

    if (true == one_txn)
        W_DO(test_env->begin_xct());

    for (uint64_t i=0; i < count; ++i)
    {
        // Prepare the record first
        ++seed_key_int;                          // Key starts from 1 and ends with POPULATE_RECORDS
        memset(key_buf, '\0', KEY_BUF_SIZE+1);
        test_env->itoa(seed_key_int, key_buf, 10);   // Fill the key portion with integer converted to string

        int last_digit = strlen(key_buf)-1;
        if (('3' == key_buf[last_digit]) || ('5' == key_buf[last_digit]) || ('7' == key_buf[last_digit]))
        {
            // Skip records with keys ending in 3, 5, or 7
        }
        else
        {
            if (true == one_txn)
                W_DO(test_env->btree_insert(stid, key_buf, data_buf));            // One big transaction
            else
                W_DO(test_env->btree_insert_and_commit(stid, key_buf, data_buf)); // Many small transactions
        }
    }

    if (true == one_txn)
        W_DO(test_env->commit_xct());

    // Verify the record count after the insertion
    W_DO(test_env->btree_scan(stid, s));
    recordCount += count/10*7;    // From empty sotre, skip all the 3, 5 and 7s

    EXPECT_EQ(recordCount, s.rownum);
    std::cout << "Large population, record count in current B-tree: " << s.rownum << std::endl;

    return RCOK;
}

// Helper function to update some existing records, goal is to generate multiple dirty pages within one transaction
w_rc_t update_for_dirty_page(stid_t &stid,
                               uint64_t seed_key_int)    // Starting point of the key value to update
{
    // Caller has an active transaction already
    // This fuction does not create, commit or abort the current transaction

    w_rc_t rc;
    char key_buf[KEY_BUF_SIZE+1];
    char data_buf[DATA_BUF_SIZE+1];
    // Update operation uses the key value to locate existing record
    // and then update the data field, no change in the key field
    memset(data_buf, '\0', DATA_BUF_SIZE+1);
    memset(data_buf, 'P', DATA_BUF_SIZE);

    for (int i = 0; i < EXTRA_UPDATE_COUNT; ++i)
    {
        memset(key_buf, '\0', KEY_BUF_SIZE+1);
        test_env->itoa(seed_key_int, key_buf, 10);  // Fill the key portion with integer converted to string

        // Update only change the data field, not the key field
        rc = test_env->btree_update(stid, key_buf, data_buf);
        // Update the same record again
        memset(data_buf, 'Q', DATA_BUF_SIZE);
        rc = test_env->btree_update(stid, key_buf, data_buf);
        if (rc.is_error())
        {
            // If the update failed (e.g. key does not exist, which is possible if
            // the given seed was out-of-bound, or the ending key value does not exist),
            // simply ignore it, because these are extra operations to generate multiple
            // dirty pages in one transaction, no big deal if we cannot generate multiple dirty pages
            // Do not abort the transaction because caller decides what to do with the transaction
        }
        // Set the new key value
        seed_key_int += SEED_INCREMENT;
    }

    return RCOK;
}

// Helper function to insert one record, and potentially some extra update operations
w_rc_t insert_performance_records(stid_t &stid,
                                       uint64_t key_int,           // Key value to insert
                                       test_txn_state_t txnState,  // What to do with the transaction
                                       uint64_t seed_key_int,      // If <> NO_EXTRA_UPDATE, seed for extra update operations
                                       bool extra)                 // True if update the same record after insertion
{
    char key_buf[KEY_BUF_SIZE+1];
    char data_buf[DATA_BUF_SIZE+1];
    memset(data_buf, '\0', DATA_BUF_SIZE+1);
    memset(data_buf, 'D', DATA_BUF_SIZE);

    memset(key_buf, '\0', KEY_BUF_SIZE+1);
    test_env->itoa(key_int, key_buf, 10);  // Fill the key portion with integer converted to string

    // Perform the operation
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert(stid, key_buf, data_buf));

    if (true == extra)
    {
        // Immediatelly update the record just inserted
        memset(data_buf, 'E', DATA_BUF_SIZE);
        W_DO(test_env->btree_update(stid, key_buf, data_buf));
    }

    if (NO_EXTRA_UPDATE != seed_key_int)
    {
        // Extra update operations to generate more dirty pages
        W_DO(update_for_dirty_page(stid, seed_key_int));
    }

    if (t_test_txn_commit == txnState)
        W_DO(test_env->commit_xct());
    else if (t_test_txn_abort == txnState)
        W_DO(test_env->abort_xct());
    else // t_test_txn_in_flight
    {
        // If in-flight, it is caller's responsibility to detach from the active transaction
    }

    return RCOK;
}

// Helper function to delete one record, and potentially some extra update operations
w_rc_t delete_performance_records(stid_t &stid,
                                       uint64_t key_int,           // Key value to delete
                                       test_txn_state_t txnState,  // What to do with the transaction
                                       uint64_t seed_key_int)      // If <> NO_EXTRA_UPDATE, seed for extra update operations
{
    char key_buf[KEY_BUF_SIZE+1];
    char data_buf[DATA_BUF_SIZE+1];
    memset(data_buf, '\0', DATA_BUF_SIZE+1);
    memset(data_buf, 'D', DATA_BUF_SIZE);

    memset(key_buf, '\0', KEY_BUF_SIZE+1);
    test_env->itoa(key_int, key_buf, 10);  // Fill the key portion with integer converted to string

    // Perform the operation
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_remove(stid, key_buf));

    if ( NO_EXTRA_UPDATE != seed_key_int)
    {
        // Extra update operations to generate more dirty pages
        W_DO(update_for_dirty_page(stid, seed_key_int));
    }

    if (t_test_txn_commit == txnState)
        W_DO(test_env->commit_xct());
    else if (t_test_txn_abort == txnState)
        W_DO(test_env->abort_xct());
    else // t_test_txn_in_flight
    {
        // If in-flight, it is caller's responsibility to detach from the active transaction
    }

    return RCOK;
}

// Helper function to update one record, and potentially some extra update operations
// Update operation updates the data portion, not the key portion
w_rc_t update_performance_records(stid_t &stid,
                                        uint64_t key_int,           // Key value to update
                                        test_txn_state_t txnState,  // What to do with the transaction
                                        uint64_t seed_key_int)      // If <> NO_EXTRA_UPDATE, seed for extra update operations
{
    char key_buf[KEY_BUF_SIZE+1];
    char data_buf[DATA_BUF_SIZE+1];
    // Update operation uses the key to locate existing record
    // and then update the data field, no change in the key field
    memset(data_buf, '\0', DATA_BUF_SIZE+1);
    memset(data_buf, 'Y', DATA_BUF_SIZE);

    memset(key_buf, '\0', KEY_BUF_SIZE+1);
    test_env->itoa(key_int, key_buf, 10);  // Fill the key portion with integer converted to string

    // Update only change the data filed, not the key field
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_update(stid, key_buf, data_buf));

    if ( NO_EXTRA_UPDATE != seed_key_int)
    {
        // Update the same record again
        memset(data_buf, 'Z', DATA_BUF_SIZE);
        W_DO(test_env->btree_update(stid, key_buf, data_buf));

        // Extra update operations to generate more dirty pages
        W_DO(update_for_dirty_page(stid, seed_key_int));
    }

    if (t_test_txn_commit == txnState)
        W_DO(test_env->commit_xct());
    else if (t_test_txn_abort == txnState)
        W_DO(test_env->abort_xct());
    else // t_test_txn_in_flight
    {
        // If in-flight, it is caller's responsibility to detach from the active transaction
    }

    return RCOK;
}

// Helper class for the in-flight worker thread
int next_thid = 0;
class op_worker_t : public smthread_t
{
public:
    op_worker_t(): smthread_t(t_regular, "op_worker_t"), _running(true)
    {
        _thid = next_thid++;
    }

    virtual void run()
    {
        tlr_t rand (_thid);
        _rc = run_core();
        _running = false;
    }

    rc_t run_core()
    {
        // Perform the specified operation during phase 2 (pre_shutdown)
        // all transactions are in-flight transactions
        // The seeding key in each operation should be apart from each other to avoid deadlocks

        // Note that all these transactions are in-flight transactions with specified key ending in
        // 4, 6, 8 - update (extra updates)
        // 7 - insert
        // 2 , 9 - update (primary update)

        if (t_op_insert == op_type)
        {
            // If insert operation, insert the specified key_int first, and then
            // update multiple existing records with seeding key, so the in-flight
            // transaction generates multiple dirty pages (~ 5 pages)

            // Perform the insertion first
            W_DO(insert_performance_records(stid, key_int, t_test_txn_in_flight, NO_EXTRA_UPDATE, true)); // extra update

            // then update multiple records with seeding key (ending with 4)
            W_DO(update_for_dirty_page(stid, seed_key_int));

            // Cause in-flight transaction
            ss_m::detach_xct();
        }
        else if (t_op_delete == op_type)
        {
            // If delete operation, delete the specified key_int first, and then
            // update multiple existing records with seeding key, so the in-flight
            // transaction generates multiple dirty pages (~ 5 pages)

            W_DO(delete_performance_records(stid, key_int, t_test_txn_in_flight, NO_EXTRA_UPDATE));

            // then update multiple records with seeding key (ending with 6)
            W_DO(update_for_dirty_page(stid, seed_key_int));

            // Cause in-flight transaction
            ss_m::detach_xct();
        }
        else
        {
            // If update operation, update the specified key_int first, and then
            // update multiple existing records with seeding key, so the in-flight
            // transaction generates multiple dirty pages (~ 5 pages)

            w_assert1(t_op_update == op_type);
            W_DO(update_performance_records(stid, key_int, t_test_txn_in_flight, NO_EXTRA_UPDATE));

            // then update multiple records with seeding key (ending with 8)
            W_DO(update_for_dirty_page(stid, seed_key_int));

            // Cause in-flight transaction
            ss_m::detach_xct();
        }

        // Now we are done
        return RCOK;
    }

    rc_t _rc;
    bool _running;           // Indicate whether the thread is still running or not
    ss_m *ssm;               // Set by caller before thread started
    stid_t stid;             // Set by caller before thread started
    uint64_t key_int;        // Key value for the primary operation, set by caller before thread started
    uint64_t seed_key_int;   // Seeding key value for extra updates, set by caller before thread started
    test_op_type_t op_type;  // Operation type, set by caller before thread started
    int _thid;               // Thread id

};


// Main performance test case used by:
//    Normal shutdown - using M1 code path, due to normal shutdown, minor differences in milestone code path
//    M1 crash shutdown
//    M2 crash shutdown
//    M3 crash shutdown
//    M4 crash shutdown
class restart_multi_performance : public restart_performance_test_base
{
public:
    int                _cycle_slot_index;                // Index into cycle slots
/**
    unsigned long long _total_cycles[TOTAL_CYCLE_SLOTS]; // Structure to record CPU cycles
    double             _total_elapse[TOTAL_CYCLE_SLOTS]; // Structure to record elapse time
                                                         //record it every RECORD_FREQUENCY
                                                         // successful transactions
    double             _total_txn_count[TOTAL_CYCLE_SLOTS];
    double             _successful_txn_count[TOTAL_CYCLE_SLOTS];
**/
    elapse_info_t      elapse_info[TOTAL_CYCLE_SLOTS];

    void record_cycles(const double total, const double success)
    {
        // Record the CPU cycle into the next slot
        w_assert1(_cycle_slot_index < TOTAL_CYCLE_SLOTS);
        _end = rdtsc();
        elapse_info[_cycle_slot_index]._total_cycles = _end;

        // Record the elapse time information into the next slot
        // (do not use actual execution time because it excludes the disk I/O wait time)
        // 1. gettimeofday() to get microseconds, although it
        // does not exclude CPU sleep time
        // 2. clock_gettime() which has even higher resolution
        //     the functions clock_gettime() and clock_settime()
        //     retrieve and set the time of the specified clock clk_id.
        // See http://nadeausoftware.com/articles/2012/04/c_c_tip_how_measure_elapsed_real_time_benchmarking

        struct timeval tm;
        gettimeofday( &tm, NULL );
        // Store in millisecs seconds
        _end_time = ((double)tm.tv_sec * MILLISECS_IN_SECOND) +
                    ((double)tm.tv_usec / (MICROSECONDS_IN_SECOND/MILLISECS_IN_SECOND));
        elapse_info[_cycle_slot_index]._total_elapse = _end_time;

        // Record the transaction information
        elapse_info[_cycle_slot_index]._total_txn_count = total;
        elapse_info[_cycle_slot_index]._successful_txn_count = success;

        // Advance to the next slot
        ++_cycle_slot_index;
        return;
    }

    w_rc_t initial_shutdown(ss_m *ssm)
    {
        // Populate the base store which was enpty initially,
        // follow by a normal shutdown to persist data on disk
        // Single thread
        //     Data - insert POPULATE_RECORDS records per index
        //     Key - 1 - POPULATE_RECORDS
        //     Key - for key ending with 3, 5 or 7, skip the insertions
        // Normal shutdown

        w_assert1(NULL != ssm);
        std::cout << std::endl << "Start initial_shutdown (phase 1)..." << std::endl << std::endl;

        // Create and populate each index
        // have one extra index slot but do not create or populate it in the initial phase
        _stid_list = new stid_t[TOTAL_INDEX_COUNT];
        for (int i = 0; i < TOTAL_INDEX_COUNT; ++i)
        {
            W_DO(x_btree_create_index(ssm, &_volume, _stid_list[i], _root_pid));
            populate_performance_records(_stid_list[i], false, 0, POPULATE_RECORDS);  // Multiple small transactions
                                                                                      // seed = 0 so the first key = 1
                                                                                      // Insert POPULATE_RECORDS records
        }
        return RCOK;
    }

    w_rc_t pre_shutdown(ss_m *ssm)
    {
        // Start the system with existing data from phase 1 (initial_shutdown),
        // use multiple worker threads to generate some in_flight transactions,
        // and main thread to generate some committed transactions
        // Only up to TOTAL_IN_FLIGHT_THREADS in-flight transactions
        // In order to generate an in-flight transaction, the worker thread
        // has to detach from the active transaction, and the worker thread
        // would not be able to start a new transaction once it detached from
        // an active transaction
        //       Key - insert
        //                Key - if the key ending with 3, commit
        //                Key - if the key ending with 5, skip
        //                Key - if the key ending with 7, some in-flight and some commit
        //       Key - update
        //                Key - if the key ending with 2 or 9, some in-flight and some commit
        //                Key - if the key ending with 0, 1 commit
        //       Key - no change
        //                Key - 4, 8, a few of the 4s and 8s will be affected by in_flight transactions
        //                Key - 6,  if in-flight contains delete operations, then some of the 6s would be affected
        // Either normal shutdown or simulate system crash

        // With TOTAL_INDEX_COUNT (7) indexes, the mail loop generated more than 10000 in_doubt pages
        // which require REDOs, TOTAL_IN_FLIGHT_THREADS (20) in_flight transactions which require UNDOs

        w_assert1(NULL != ssm);
        std::cout << std::endl << "Start pre_shutdown (phase 2)..." << std::endl << std::endl;

        w_rc_t rc;
        char key_buf[KEY_BUF_SIZE+1];
        int current_in_flight__count = 0;   // Active in-flight thread count

        // Initialize the in-flight worker threads but not started
        op_worker_t workers[TOTAL_IN_FLIGHT_THREADS];
        for (int i = 0; i < TOTAL_IN_FLIGHT_THREADS; ++i)
        {
            workers[i].ssm = ssm;
        }

        // Set the initial for key_int which starts from the beginning
        uint64_t key_int = 0;

        // Make sure the transaction count is not too high, beause in case of
        // crash shutdown without checkpoint, each transaction before the crash
        // will cause a new transaction being created during Log Analysis phase,
        // if the transaction count is too high, we will either run out of memory, or
        // other weird error occurs, such as 'cb' cannot be found in hash table, etc.
        // while TOTAL_RECOVERY_TRANSACTION transactions is a reason number
        // the code can handle

        uint64_t transaction_count = (POPULATE_RECORDS > TOTAL_RECOVERY_TRANSACTION)
                                     ? TOTAL_RECOVERY_TRANSACTION : POPULATE_RECORDS;

        int index_count = 0;

        // Main loop, perform 'transaction_count' transactions but spread out to all indexes
        // also generates in_flight transactions across all indexes to cause UNDO during recovery
        for (uint64_t i=0; i < transaction_count;)
        {
            // Each committed transaction contains one operation only
            // while each in_flight transaction contains multiple operations (multiple pages)

            // Prepare the record first
            ++key_int;                              // Key starts from 1
            memset(key_buf, '\0', KEY_BUF_SIZE+1);
            test_env->itoa(key_int, key_buf, 10);   // Fill the key portion with integer converted to string

            // Determine which index to use, purpose use (DIRTY_INDEX_COUNT+1)
            // so the first index gets more operations
            index_count = i%(DIRTY_INDEX_COUNT+1);   // 0 -5, so we only update on the first 5 index
            if (index_count >= DIRTY_INDEX_COUNT)
                index_count = 0;

            int last_digit = strlen(key_buf)-1;
            if ('3' == key_buf[last_digit])
            {
                // This key does not exist, insert using main thread
                rc = insert_performance_records(_stid_list[index_count], key_int,
                                                t_test_txn_commit, NO_EXTRA_UPDATE, true ); // ectra update
                EXPECT_FALSE(rc.is_error());
            }
            else if ('5' == key_buf[last_digit])
            {
                // Do nothing so key endign with 5 does not exist
                // Skip advance of transaction count
                continue;
            }
            else if ('7' == key_buf[last_digit])
            {
                // This key does not exist

                // Insert in-flight using child threads up to
                // TOTAL_IN_FLIGHT_THREADS
                // The child thread generates multiple dirty pages via extra updates
                // and insert one record ending with '7'
                //
                // After TOTAL_IN_FLIGHT_THREADS in_flight transactions, insert and commit
                // using main thread
                if (current_in_flight__count < TOTAL_IN_FLIGHT_THREADS)
                {
                    // Prepare the worker thread
                    workers[current_in_flight__count].key_int = key_int;
                    // Set the seeding value for update operations in in_flight
                    // the seed in each iteration is at least 200 away from the previous seed
                    // to prevent deadlock
                    workers[current_in_flight__count].seed_key_int = 4 + (current_in_flight__count*200);   // Seed ending with 4
                    workers[current_in_flight__count].op_type = t_op_insert;
                    workers[current_in_flight__count].stid = _stid_list[index_count];

                    // Sleep for a short duration before start the thread,
                    // so the thread manager can catch up
                    ::usleep(SHORT_SLEEP_MICROSEC);
                    W_DO(workers[current_in_flight__count].fork());

                    // Increment the worker thread counter
                    ++current_in_flight__count;
                }
                else
                {
                    // Used up all the threads, insert and commit the rest using main thread
                    rc = insert_performance_records(_stid_list[index_count], key_int,
                                                    t_test_txn_commit, NO_EXTRA_UPDATE, true);  // extra update
                    EXPECT_FALSE(rc.is_error());
                }
            }
            else
            {
                // For the rest, the key exist already
                if (('2' == key_buf[last_digit]) || ('9' == key_buf[last_digit]))
                {
                    // Update in-flight using child threads up to
                    // TOTAL_IN_FLIGHT_THREADS
                    // The child thread generates multiple dirty pages via update
                    // start with key ending with key_int ('2' or '9'), and then multiple
                    // extra updates
                    //
                    // After TOTAL_IN_FLIGHT_THREADS in_flight transactions, update and commit
                    // using main thread
                    if (current_in_flight__count < TOTAL_IN_FLIGHT_THREADS)
                    {
                        // Prepare the worker thread
                        workers[current_in_flight__count].key_int = key_int;
                        // Set the seeding value for update operations in in_flight
                        // the seed in each iteration is at least 100 away from the previous seed
                        // to prevent deadlock
                        workers[current_in_flight__count].seed_key_int = 8 + (current_in_flight__count*100);   // Seed ending with 8
                        workers[current_in_flight__count].op_type = t_op_update;
                        workers[current_in_flight__count].stid = _stid_list[index_count];

                        // Sleep for a short duration before start the thread,
                        // so the thread manager can catch up
                        ::usleep(SHORT_SLEEP_MICROSEC);
                        W_DO(workers[current_in_flight__count].fork());

                        // Increment the worker thread counter
                        ++current_in_flight__count;
                    }
                    else
                    {
                        // Used up all the threads, update and commit the rest using main thread
                        // extra updates
                        rc = update_performance_records(_stid_list[index_count], key_int, t_test_txn_commit, key_int + TOTAL_RECOVERY_TRANSACTION);
                        EXPECT_FALSE(rc.is_error());
                    }
                }
                else if (('0' == key_buf[last_digit]) || ('1' == key_buf[last_digit]))
                {
                    // Update using main thread, extra updates
                    rc = update_performance_records(_stid_list[index_count], key_int, t_test_txn_commit, key_int + TOTAL_RECOVERY_TRANSACTION);
                    EXPECT_FALSE(rc.is_error());
                }
                else
                {
                    // For existing records ending with '4', '6' and '8', no change
                    // skip advance of transaction count
                    continue;
                }
            }

            // Next transaction
            ++i;
        }

        if (current_in_flight__count < TOTAL_IN_FLIGHT_THREADS)
            std::cerr << "!!!!! Expected " << TOTAL_IN_FLIGHT_THREADS
                      << " in-flight transactions but only created "
                      << current_in_flight__count << std::endl;

        // Sleep for a while first to give child threads some time, and then
        // terminate all the started in-flight worker threads
        ::usleep(SHORT_SLEEP_MICROSEC*100);
        for (int i=0; i < current_in_flight__count; ++i)
        {
            W_DO(workers[i].join());
            EXPECT_FALSE(workers[i]._running) << i;
            EXPECT_FALSE(workers[i]._rc.is_error()) << i;
        }

        // Issue a checkpoint so in the case of crash shutdwon,
        // restart Log Analysis only need to start from the last completed checkpoint
        // this is to reduce the number of transactions the Log Analysis has to create
        // This is needed only if crash shutdown and there are too many transaction
        // in this phase
//        W_DO(ss_m::checkpoint());


        // Now we are done, ready to shutdown (either normal or crash)
        // At this point, we should have POPULATE_RECORDS/10*9 records (~5000)
        // per B-tree although some of them are in-flights which would get
        // rollback later

        // Initialize the data structure for time recording
        _cycle_slot_index = 0;
        for (int i = 0; i < TOTAL_CYCLE_SLOTS; ++i)
        {
            elapse_info[i]._total_cycles = 0;
            elapse_info[i]._total_elapse = 0.0;
            elapse_info[i]._total_txn_count = 0.0;
            elapse_info[i]._successful_txn_count = 0.0;
        }

        // Force a flush of system cache before shutdown, the goal is to
        // make sure after the simulated system crash, the recovery process
        // has to get data from disk, not file system cache
        int i;
        i = system("free -m");  // before free cache
        i = system("/bin/sync");  // flush but not free the page cache
        i = system("echo 3 | sudo tee /proc/sys/vm/drop_caches");  // free page cache and inode cache
        i = system("free -m");  // after free cache

        // Populate the system cache with some random data
        //   i = system("cat /home/weyg/build/opt-ubuntu-12.04-x86_64/Zero/tests/sm/test.core > /dev/null");
        //   i = system("free -m");  // after free cache

        // Performance test on raw I/O, simulate the same amount of dirty page I/Os:
        //    dd if=/home/weyg/build/opt-ubuntu-12.04-x86_64/Zero/tests/sm/test.core of=/dev/null bs=8k count=10000                   // okay to use system cache
        //    dd if=/home/weyg/build/opt-ubuntu-12.04-x86_64/Zero/tests/sm/test.core of=/dev/null bs=8k count=10000 iflag=direct  // force direct I/O
        // Hard code the path and name of the test input file:
        // The actual data file for this test program is in: /dev/shm/weyg/btree_test_env/volumes/dev_test
        //    i = system("dd if=/home/weyg/test.core of=/dev/null bs=8k count=10000 iflag=direct");

        // Get hard drive speed information on the system:
        // sudo hdparm -tT /dev/sda

        // Command to turn off swap space, it does not work with Express implementation:
        //      sudo swapoff -a  // Turn off swap space
        //      sudo swapon -a  // Turn on swap space

        //Commnad to watch for disk I/O, only watch program from 'weyg', and update interval = 0.1 second:
        //       sudo iotop -u weyg -a -o -d .1

        std::cout << "Return value from system command: " << i << std::endl;

        // Start the timer to measure time from before test code shutdown (normal or crash)
        // to finish TOTAL_SUCCESS_TRANSACTIONS user transactions
        ::usleep(SHORT_SLEEP_MICROSEC*100);

        // CUP cycles
        _start = rdtsc();

        // Elapse time in millisecs seconds
        struct timeval tm;
        gettimeofday( &tm, NULL );
        _start_time = ((double)tm.tv_sec*MILLISECS_IN_SECOND) +
                      ((double)tm.tv_usec/(MICROSECONDS_IN_SECOND/MILLISECS_IN_SECOND));
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *)  // Not using the ss_m input parameter
    {
        // Restart the system, start concurrent user transactions
        // Measuer the time required to finish TOTAL_SUCCESS_TRANSACTIONS concurrent transactions
        //       Key - insert records with key ending in 5 (did not exist)
        //       Key - delete if key ending with 2 or 0 (if in_flight with 2 should rollback first)
        //       Key - if key ending with 3, update (inserted during pre_shutdown)
        //       Key - if key ending with 7, update (inserted during pre_shutdown, if in_flight with 7,
        //                they should rollback first and the record does not exist after rollback)
        //       Key - if key ending with 9, some of them were in_flight updates from phase 2
        //                while others were committed updates from phase 2
        //                Not using these records, therefore
        //                M1, M2, M4 - all transactions rolled back, so these records would be recovered
        //                M3 - these records won't be rolled back due to pure on-demand rollback
        // Measure and draw a curve that shows the numbers of completed transactions over time

        // If using execution time, we need to determine reporting interval
        double time_interval = TARGET_EXECUTION_TIME/REPORT_COUNT;
        double current_interval = 0.0;  // recording point

        // Measure and record the amount of time took for system to shutdown and restart (no recovery)
        record_cycles(0, 0);
        current_interval += time_interval;  // Increase the recording point

        std::cout << std::endl << "Start post_shutdown (phase 3)..." << std::endl << std::endl;

        int32_t restart_mode = test_env->_restart_options->restart_mode;
        bool crash_shutdown = test_env->_restart_options->shutdown_mode;  // false = normal shutdown

        // Now start measuring the Instant Restart duration based on the
        // first TOTAL_SUCCESS_TRANSACTIONS successful transactions
        // All concurrent transactions are coming in using the main thread (single threaded)

        w_rc_t rc;
        char key_buf[KEY_BUF_SIZE+1];
        double started_txn_count = 0;  // Started transactions
        double succeed_txn_count = 0;  // Completed transaction

        // Mix up the record for user transactions, some of the operations are on the
        // existing records, while some are on non-existing (out-of-bound) records which should
        // not trigger on_demand recovery
        uint64_t key_int = 1;                           // Key from the existing records
        uint64_t high_key_int = POPULATE_RECORDS * 60;  // Key from out-of-bound (non-existing) records
        uint64_t actual_key;                            // Key value used for the current operation
        int out_of_bound = 0;                           // Frequency to use an out-of-bound record (OUT_OF_BOUND_FREQUENCY)
        int index_count = 0;                            // Which index to use for the operation?

        // Elapse time in millisecs seconds
        double begin_time;      // Time when the concurrent transactions start
        double current_time;    // Current time of the concurrent transaction
        double execution_time; // Elapse time of the concurrent transactions
        struct timeval tm;
        gettimeofday( &tm, NULL );
        begin_time = ((double)tm.tv_sec*MILLISECS_IN_SECOND) +
                     ((double)tm.tv_usec/(MICROSECONDS_IN_SECOND/MILLISECS_IN_SECOND));

        // Determine how many successful transactions to reach
        // M4 has a much lower number due to bugs
        double target_count;
        if (restart_mode == m4_default_restart)
            target_count = M4_SUCCESS_TRANSACTIONS;
        else
            target_count = TOTAL_SUCCESS_TRANSACTIONS;

        if (no_txn == EXEC_TYPE)
            target_count = 0;  // disable concurrent transactions
        else if (txn_time == EXEC_TYPE)
            target_count = std::numeric_limits<double>::max();  // disable concurrent transactions

        // Spread out the operations to all indexes
        for (succeed_txn_count=0; succeed_txn_count < target_count;)
        {
            // User transactions in M2 might fail due to commit_lsn check,
            // in such case the failed transactions are not counted toward
            // the total successful transaction count

            // Once we reached the target execution time, break out from the loop
            // no matter whether we are using transaction cout or time out as exit criteria
            gettimeofday( &tm, NULL );
            current_time = ((double)tm.tv_sec*MILLISECS_IN_SECOND) +
                           ((double)tm.tv_usec/(MICROSECONDS_IN_SECOND/MILLISECS_IN_SECOND));
            execution_time = current_time - begin_time;

            if ((txn_time == EXEC_TYPE) && ((target_count - 1) == started_txn_count))
                std::cout << std::endl << "!!! Execution time: Transaction count overflow !!!" << std::endl << std::endl;

            if ((txn_time == EXEC_TYPE) && (TARGET_EXECUTION_TIME <= execution_time))
            {
                std::cout << std::endl << "Exit on time out, transaction count: " << started_txn_count
                          << std::endl << std::endl;
                break;
            }
            // A safe guard to prevent infinite loop if we are using transaction count as exit criteria
            if ((txn_count == EXEC_TYPE) && (started_txn_count > (1 << 21)))  // > 2G
            {
                std::cout << std::endl << "Exit on transaction count, executed more than " << (1<< 21)
                          << "transactions, break!!!!!" << std::endl << std::endl;
                break;
            }

            // Determine which index to use, purposely use (DIRTY_INDEX_COUNT+1)
            // so first index gets more operations
            // Note we are using the started count, not success count
            // therefore we will keep looping through all indexes even
            // if transaction failed
            index_count = (int)started_txn_count%(DIRTY_INDEX_COUNT+1);   // 0 -5, so we only update on the first 5 index
            if (index_count >= DIRTY_INDEX_COUNT)
                index_count = 0;

            // Prepare the key value into key buffer
            memset(key_buf, '\0', KEY_BUF_SIZE+1);
            ++out_of_bound;
            if (out_of_bound >= OUT_OF_BOUND_FREQUENCY)
            {
                // The operation is an insert operation for a non-existing
                // out-of-bound key record, if the key value is overlapping
                // with the inbound key value, reset it to the original out-of-bound
                // value.  We need this reset because we need to keep the
                // concurrent operation running for 60 seconds, and we do not
                // want the out-of-bound value get into the in-bound range
                if (POPULATE_RECORDS >= high_key_int)
                    high_key_int = POPULATE_RECORDS * 60;
                test_env->itoa(high_key_int, key_buf, 10);  // Fill the key portion with integer converted to string
                actual_key = high_key_int;
                --high_key_int;                             // Key starts from POPULATE_RECORDS*60 and decreasing
                out_of_bound = 0;
            }
            else
            {
                // The operation is on a record with in-bound key value,
                // if the key is hitting the boundary, reset it so the key value
                // does not go out-of-bound
                // We need this reset because we need to keep the concurrent
                // operation running for 60 seconds and we cannot let the key value
                // continue going up which would increase the size of index too much
                if (POPULATE_RECORDS <= key_int)
                    key_int = 1;
                test_env->itoa(key_int, key_buf, 10);  // Fill the key portion with integer converted to string
                actual_key = key_int;
                ++key_int;                             // Key starts from 1 and increasing
            }
            int last_digit = strlen(key_buf)-1;

            // Decide what to do with this record
            if ('5' == key_buf[last_digit])
            {
                // Insert, this should be okay regardless we are in or out of bound (POPULATE_RECORDS)
                // because it does not exist in the store
                // In M2 it might fail due to commit_lsn checking, in such case, skip
                rc = insert_performance_records(_stid_list[index_count], actual_key,
                                                t_test_txn_commit, NO_EXTRA_UPDATE, false);  // no extra update
                if (rc.is_error())
                {
#ifndef MEASURE_PERFORMANCE
                    if ((eDUPLICATE != rc.err_num()) && (eNOTFOUND != rc.err_num()))
                        std::cout << "Insert error with key ending in 5, key: "
                                  << ", error no: " << rc.err_num() << actual_key << ", error: " << rc.get_message() << std::endl;
#endif

                    // Go to the next key value, do not increase succeed_txn_count
                    test_env->abort_xct();
                    ++started_txn_count;
                    // If duplicate or not found error, it is a successful transaction,
                    // because we only consider blocking (M2) or unknown error as a failure
                    // eDUPLICATE = 34
                    // eACCESS_CONFLICT = 83 (commit_lsn error)
                    // eNOTFOUND = 22
                    if ((eDUPLICATE == rc.err_num()) || (eNOTFOUND == rc.err_num()))
                        ++succeed_txn_count;
                    continue;
                }
            }
            else if (('7' == key_buf[last_digit]) && (actual_key < POPULATE_RECORDS))
            {
                // Some of the keys ending with 7 might be in-flight insertions from phase 2 (not many),
                // they would be rollback so those records do not exist anymore after rollback,
                // while some of the 7 were inserted and committed during phase 2 and they do exist.
                // Due to multiple indexes and we only insert some of the 7s, most of the '7' do not exist.
                // Therefore doing insertions in all cases, some of them might fail (duplicate)
                rc = insert_performance_records(_stid_list[index_count], actual_key,
                                                t_test_txn_commit, NO_EXTRA_UPDATE, false);  // no extra update
                if (rc.is_error())
                {
#ifndef MEASURE_PERFORMANCE
                    // If record was committed insertion during phase 2, we should not be able
                    // to insert it again, so duplicate error is expected.
                    // Except that in M2 which might fail due to commit_lsn check
                    if ((eDUPLICATE != rc.err_num()) && (eNOTFOUND != rc.err_num()))
                        std::cout << "Insert error with key ending in 7, key: " << actual_key
                                  << ", error no: " << rc.err_num() << ", error: " << rc.get_message() << std::endl;
#endif

                    // Go to the next key value, do not increase succeed_txn_count
                    test_env->abort_xct();
                    ++started_txn_count;
                    // If duplicate or not found error, it is a successful transaction,
                    // because we only consider blocking (M2) or unknown error as a failure
                    if ((eDUPLICATE == rc.err_num()) || (eNOTFOUND == rc.err_num()))
                        ++succeed_txn_count;
                    continue;
                }
            }
            else if (('3' == key_buf[last_digit]) && (actual_key < POPULATE_RECORDS))
            {
                // Some of the key was inserted during pre_shutdown (phase 2), but there are
                // multiple index, so a lot of 7s do not exist.
                // Therefore doing insertions in all cases, some of them might fail (duplicate)
                rc = insert_performance_records(_stid_list[index_count], actual_key,
                                                t_test_txn_commit, NO_EXTRA_UPDATE, false);  // no extra update
                if (rc.is_error())
                {
#ifndef MEASURE_PERFORMANCE
                    // Because some of these records were inserted during phase 2, we should not
                    // be able to insert it again, so duplicate error is expected.
                    // Also M2 might fail due to commit_lsn error
                    // 34 - duplicate key
                    // 83 - commit_lsn conflict
                    if ((eDUPLICATE != rc.err_num()) && (eNOTFOUND != rc.err_num()))
                        std::cout << "Insert error with key ending in 3, key: " << actual_key
                                  << ", error no: " << rc.err_num() << ", error: " << rc.get_message() << std::endl;
#endif
                    // Go to the next key value, do not increase succeed_txn_count
                    test_env->abort_xct();
                    ++started_txn_count;
                    // If duplicate or not found error, it is a successful transaction,
                    // because we only consider blocking (M2) or unknown error as a failure
                    if ((eDUPLICATE == rc.err_num()) || (eNOTFOUND == rc.err_num()))
                        ++succeed_txn_count;
                    continue;
                }
            }
            else if ((('2' == key_buf[last_digit]) || ('0' == key_buf[last_digit])) && (actual_key < POPULATE_RECORDS))
            {
                // This key should exist already since initial_shutdown (phase 1), delete it
                // Some of the key ending with '2' were in-flight updates from phase 2
                // these records must be rollback first and then they can be deleted
                // Others were commmited operations from phase 2, no need to rollback
                rc = delete_performance_records(_stid_list[index_count], actual_key, t_test_txn_commit, NO_EXTRA_UPDATE);
                if (rc.is_error())
                {
#ifndef MEASURE_PERFORMANCE
                    // We should not encounter error except M2 due to commit_lsn check
                    if ((eDUPLICATE != rc.err_num()) && (eNOTFOUND != rc.err_num()))
                    {
                    if ('2' == key_buf[last_digit])
                        std::cout << "Delete error with key ending in 2, key: " << actual_key
                                  << ", error no: " << rc.err_num() << ", error: " << rc.get_message() << std::endl;
                    else
                        std::cout << "Delete error with key ending in 0, key: " << actual_key
                                  << ", error no: " << rc.err_num() << ", error: " << rc.get_message() << std::endl;
                    }
#endif

                    // Go to the next key value, do not increase succeed_txn_count
                    test_env->abort_xct();
                    ++started_txn_count;
                    // If duplicate or not found error, it is a successful transaction,
                    // because we only consider blocking (M2) or unknown error as a failure
                    if ((eDUPLICATE == rc.err_num()) || (eNOTFOUND == rc.err_num()))
                        ++succeed_txn_count;
                    continue;
                }
            }
            else if (actual_key > POPULATE_RECORDS)
            {
                // If key value is out of bound (does not exist), and we don't have
                // enough successful user transactions yet, insert the key value
                rc = insert_performance_records(_stid_list[index_count], actual_key,
                                                t_test_txn_commit, NO_EXTRA_UPDATE, false);  // no extra update
                if (rc.is_error())
                {
#ifndef MEASURE_PERFORMANCE
                    // We should not encounter error except M2 due to commit_lsn check
                    if ((eDUPLICATE != rc.err_num()) && (eNOTFOUND != rc.err_num()))
                        std::cout << "Insert error with out-of-bound key value, key: " << actual_key
                                  << ", error no: " << rc.err_num() << ", error: " << rc.get_message() << std::endl;
#endif

                    // Go to the next key value, do not increase succeed_txn_count
                    test_env->abort_xct();
                    ++started_txn_count;
                    // If duplicate or not found error, it is a successful transaction,
                    // because we only consider blocking (M2) or unknown error as a failure
                    if ((eDUPLICATE == rc.err_num()) || (eNOTFOUND == rc.err_num()))
                        ++succeed_txn_count;
                    continue;
                }
            }
            else
            {
                // These are the remaining already existed records (ending with 1, 4, 6, 8, 9), no change
                // therefore in the case of M3 (pure on_demand), some loser transactions (ended with '9')
                // and many in_doubt pages won't be rolled back during this loop (long tail)

                // Do not update started or succeed counter, go to the next one
                continue;
            }

            // If we get here, the current transaction succeeded
            // Update both transaction counters
            ++started_txn_count;       // Started counter
            ++succeed_txn_count;       // Succeeded counter

            // Is it time to record the performance information?
            // Note we are recording information based on succeed transactions
            // it ignores the failed transactions
            if (txn_count == EXEC_TYPE)
            {
                if (0 == ((int)succeed_txn_count % RECORD_FREQUENCY))
                {
                    // Record cycles based on successful transaction count, we wll
                    // take 10 slots
                    record_cycles(started_txn_count, succeed_txn_count);
                }
            }
            else if (txn_time == EXEC_TYPE)
            {
                // Based on execution time, the transaction count can get very high
                // take one slot every 1/10 of execution time
                if (execution_time > current_interval)
                {
                    // Time to record information
                    record_cycles(started_txn_count, succeed_txn_count);
                    current_interval += time_interval;  // Increase the recording point
                }
            }
        }

        std::cout << "**** Finished sufficient concurrent transactions, start reporting..." << std::endl;

        // If M3, some of the in-flight transactions (ending with 9)
        // and in_doubt pages might not have been recovered at this point (long tail)

        // Reporting:
        // Report performance information
        //     _total_cycles[0] - from shutdown to the beginning of restart, no recovery
        //     ...
        //     _total_cycles[i] - record information every RECORD_FREQUENCY successful user transactions
        //                              or every time interval
        //     ...
        //     _total_cycles[TOTAL_CYCLE_SLOTS] - from shutdown to exit the main loop, either based on
        //                                                              succeed_txn_count user transactions or time limit
#ifdef MEASURE_PERFORMANCE
        std::cout << std::endl << "Successful transaction count, elapse time(milliseconds) and CPU cycles: " << std::endl;
        for (int iIndex = 0; iIndex < _cycle_slot_index; ++iIndex)
        {
            std::cout << " Total txn: " << elapse_info[iIndex]._total_txn_count
                      << ", successful txn: " << elapse_info[iIndex]._successful_txn_count
                      << ", elapse time: " << (elapse_info[iIndex]._total_elapse - _start_time)
                      << ", CPU cycles: " << (elapse_info[iIndex]._total_cycles - _start);
            if (0 < iIndex)
            {
                std::cout << ", elapse delta: " << (elapse_info[iIndex]._total_elapse - elapse_info[iIndex-1]._total_elapse);
                std::cout << ", cycle delta: " << (elapse_info[iIndex]._total_cycles - elapse_info[iIndex-1]._total_cycles) << std::endl;
            }
            else
            {
                // First one, this is the duration from system crash to store open
                // Clean shutdown - including rollback
                // M1 - including the entire Restart
                // M2 - M4 - Log Analysis only
                std::cout << ", elapse delta: " << (elapse_info[iIndex]._total_elapse - _start_time);
                std::cout << ", cycle delta: " << (elapse_info[iIndex]._total_cycles - _start);
                if (false == crash_shutdown)
                    std::cout << ", normal cleanup time" << std::endl;
                else
                {
                    if (m1_default_restart == restart_mode)
                        std::cout << ", restart process completed" << std::endl;
                    else
                        std::cout << ", Log Analysis completed" << std::endl;
                }
            }
        }
#endif
        if (m1_default_restart == restart_mode)
            std::cout << std::endl << "M1 ...";
        else if (m2_default_restart == restart_mode)
            std::cout << std::endl << "M2...";
        else if (m3_default_restart == restart_mode)
            std::cout << std::endl << "M3...";
        else if (m4_default_restart == restart_mode)
            std::cout << std::endl << "M4...";
        else
            std::cerr << std::endl << "UNKNOWN RESTART MODE, ERROR...";

        if (true == crash_shutdown)
            std::cout << " CRASH SHUTDOWN..." << std::endl;
        else
            std::cout << " NORMAL SHUTDOWN..." << std::endl;

        std::cout << "Total started transactions: " << started_txn_count << std::endl;
        std::cout << "Total completed transactions: " << succeed_txn_count << std::endl;

#ifndef MEASURE_PERFORMANCE
        // Get record count from each index, we are not measuring
        // performance number for the scan operation
        int recordCountTotal = 0;
        x_btree_scan_result s;
        for (int i = 0; i < TOTAL_INDEX_COUNT; ++i)
        {
            W_DO(test_env->btree_scan(_stid_list[index_count], s));
            recordCountTotal += s.rownum;
            std::cout << "Index " << i << " record count: " << s.rownum << std::endl;
        }
        std::cout << "Existing record count in B-tree: " << recordCountTotal << std::endl;
#endif

        // Duration starts from 'store open', so if normal shutdown or M1, recovery has completed already
        unsigned long long duration_CPU = (elapse_info[_cycle_slot_index - 1]._total_cycles - elapse_info[0]._total_cycles);
        double duration_time = (elapse_info[_cycle_slot_index - 1]._total_elapse - elapse_info[0]._total_elapse);

        std::cout << std::endl << "Total CPU cycles for " << succeed_txn_count << " successful user transactions: "
                  << duration_CPU << std::endl;

        // Convert from CPU cycles to time
        // Note the magic conversion number below is from 'cat /proc/cpuinfo: cpu MHz'
        // the value is machine dependent so run it on the machine which is doing
        // the performance tests, also the magic number is the current cpu efficency
        // when /proc/cpuinfo' was executed, while the cpu usage might be very low
        // therefore not reliable
        // unsigned time =0;
        // time = (duration_CPU/1596000);
        // std::cout << "CPU Time in milliseconds(after store opened): " << time << std::endl;

        std::cout << std::endl << "Total elapsed time (after store opened) in milliseconds: " << duration_time << std::endl << std::endl;

        return RCOK;
    }
};


///////////////////////////
// Testing
///////////////////////////

/**
// Passing - M1
TEST (RestartPerfTest, MultiPerformanceNormal)
{
    test_env->empty_logdata_dir();
    restart_multi_performance context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;

    // Normal shutdown
    EXPECT_EQ(test_env->runRestartPerfTest(&context,
                                           &options,             // Restart option, which milestone, normal or crash shutdown
                                           true,                 // Turn locking ON
                                           DISK_QUOTA_IN_KB,     // 2GB, disk_quota_in_pages, how much disk space is allowed,
                                           make_perf_options()), // Other options
                                           0);
}
**/

/**
// Passing - M1
TEST (RestartPerfTest, MultiPerformanceM1)
{
    test_env->empty_logdata_dir();
    restart_multi_performance context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m1_default_restart;

    // M1 crash shutdown
    EXPECT_EQ(test_env->runRestartPerfTest(&context,
                                           &options,         // Restart option, which milestone, normal or crash shutdown
                                           true,             // Turn locking ON
                                           DISK_QUOTA_IN_KB, // 2GB, disk_quota_in_pages, how much disk space is allowed,
                                           make_perf_options()),  // Other options
                                           0);
}
**/

/**/
// Passing - M2
TEST (RestartPerfTest, MultiPerformanceM2)
{
    test_env->empty_logdata_dir();
    restart_multi_performance context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart;

    // M2 crash shutdown
    EXPECT_EQ(test_env->runRestartPerfTest(&context,
                                           &options,         // Restart option, which milestone, normal or crash shutdown
                                           false,            // Turn locking OFF  <= using commit lsn, recovery is done
                                                             // through the child thread without lock acquisition
                                                             // Locking on with user transaction does not affect recovery
                                           DISK_QUOTA_IN_KB, // 2GB, disk_quota_in_pages, how much disk space is allowed,
                                           make_perf_options()),  // Other options
                                           0);
}
/**/

/**
// Passing - M3
TEST (RestartPerfTest, MultiPerformanceM3)
{
    test_env->empty_logdata_dir();
    restart_multi_performance context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart;

    // M3 crash shutdown
    EXPECT_EQ(test_env->runRestartPerfTest(&context,
                                           &options,         // Restart option, which milestone, normal or crash shutdown
                                           true,             // Turn locking ON
                                           DISK_QUOTA_IN_KB, // 2GB, disk_quota_in_pages, how much disk space is allowed,
                                           make_perf_options()),  // Other options
                                           0);
}
**/

/**
// Passing - M4
TEST (RestartPerfTest, MultiPerformanceM4)
{
    test_env->empty_logdata_dir();
    restart_multi_performance context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m4_default_restart;

    // M4 crash shutdown
    EXPECT_EQ(test_env->runRestartPerfTest(&context,
                                           &options,         // Restart option, which milestone, normal or crash shutdown
                                           true,             // Turn locking ON
                                           DISK_QUOTA_IN_KB, // 2GB, disk_quota_in_pages, how much disk space is allowed,
                                           make_perf_options()),  // Other options
                                           0);
}
**/

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
