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


// Uncomment the following line  to run CPU cycle performance measurment...
//
// #define MEASURE_PERFORMANCE 1

// Define constants
const int TOTAL_IN_FLIGHT_THREADS    = 400;                 // Worker threads for in-flight transactions, tested up to 400
const uint64_t TOTAL_RECORDS         = 5000;                // Total record count of the pre-populated store, tested up to 5000
                                                            // M2 crash
                                                            //    1000 - okay with 300 threads and 700 transactions
                                                            //    1000 - failed with 10 threads and 800 transactions
                                                            //    950 - dump with 10 threads and 800 transactions
                                                            //              assertion failure: sep_key != NULL
                                                            //              error in /home/weyg/projects/Zero/src/sm/btree_page_h.cpp:1127 Assertion failed
                                                            // M3:
                                                            //    1000 - got the time with 200 threads and 700 transactions, but core dump at the end with eNOTFOUND
                                                            //    5000 - dump with 200 threads and 1000 transactions, see error above

const int TOTAL_SUCCESS_TRANSACTIONS = 1000;                // Target total completed concurrent transaction count, tested up to 1000
                                                            // during Instant Restart (post_shutdown)

const int RECORD_FREQUENCY           = 10;                  // Record the CPU cycles every RECORD_FREQUENCY successful transactions
const int TOTAL_CYCLE_SLOTS = TOTAL_SUCCESS_TRANSACTIONS/RECORD_FREQUENCY+1; // Total avaliable slots to record CPU cycles
const int OUT_OF_BOUND_FREQUENCY     = 5;                   // During concurrent update, how often to use out_of_bound record
const uint32_t SHORT_SLEEP_MICROSEC  = 20000;               // Sleep time before start a child thread, this is allow thread manager to catch up
const int64_t DISK_QUOTA_IN_KB       = ((int64_t) 1 << 21); // 2GB

const int key_buf_size               = 10;                  // Max. key size of the record
const int data_buf_size              = 20;                  // Data size of the record, max record size would be 30 bytes or less
                                                            // because the size of key field might be < 10 bytes
                                                            // therefore a page (8192) probably could hold couple
                                                            // hundred records

// Test environment
btree_test_env *test_env;


/**
// TODO -
//             lower the in-flights (20), and each in-flight does a big transaction which touches many pages ~5 pages
//             we should have thousands of dirty pages in the buffer pool upon restart
//             phase 2 has couple thousand transactions, some committed, some in-flights
//             Record size must be larger, and the B-tree contains many pages


One index with ~1000 records, 300 in-flights, and 700 successful concurrent transactions, the total time includes the shutdown time in pre_shutdown:
    M1 (Normal) -
              Total started transactions: 700
              Total completed transactions: 700
              Existing record count in B-tree: 1032
              Total CPU cycles for 700 successful user transactions: 110234014
              Time in milliseconds: 69
    M1 (Crash)   -
              Total started transactions: 700
              Total completed transactions: 700
              Existing record count in B-tree: 1032
              Total CPU cycles for 700 successful user transactions: 89380094
              Time in milliseconds: 56
    M2 (Crash)   -                                                           <<<<  conflict error (expected), many duplicate errors on records with key > 1000
              Total started transactions: 1259
              Total completed transactions: 700
              Existing record count in B-tree: 594
              Total CPU cycles for 700 successful user transactions: 110017388
              Time in milliseconds: 68
    M3 (Crash)   -                                                           <<<<  ending with '3', item not found
              Total started transactions: 781
              Total completed transactions: 700
              Existing record count in B-tree: 412
              Total CPU cycles for 700 successful user transactions: 134411594
              Time in milliseconds: 84
    M4 (Crash)   -                                                           <<<<  not working


One index with ~5000 records, 400 in-flights, and 1000 successful concurrent transactions, the total time icludes the shutdown time in pre_shutdown:
     M1 (Normal) -
              Total started transactions: 1000
              Total completed transactions: 1000
              Existing record count in B-tree: 4699
              Total CPU cycles for 1000 successful user transactions: 127423700
              Time in milliseconds: 79
     M1 (Crash) -
              Total started transactions: 1000
              Total completed transactions: 1000
              Existing record count in B-tree: 4699
              Total CPU cycles for 1000 successful user transactions: 172504660
              Time in milliseconds: 108
    M2 (Crash) -                                                             <<<< infinite loop on no parent emlsn to recover from
    M3 (Crash) -                                                             <<<< sep_key != NULL and '3' key not found
    M4 (Crash) -                                                             <<<< not working
**/

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
    options.set_int_option("sm_bufpoolsize", (1 << 21));               // 2GB, while the test machine has 6GB of memory

    // Log, do not set 'sm_logdir' so we would use default
    // options.set_string_option("sm_logdir", global_log_dir);
    options.set_int_option("sm_logbufsize",  (1 << 20));               // 1MB
    options.set_int_option("sm_logsize", (1 << 23));                   // 8GB

    // Lock
    options.set_int_option("sm_locktablesize", (1 << 15));             // 16KB
    options.set_int_option("sm_rawlock_lockpool_initseg", (1 << 7));   // 128 bytes
    options.set_int_option("sm_rawlock_lockpool_segsize", (1 << 12));  // 4KB
    options.set_int_option("sm_rawlock_gc_generation_count", 100);
    options.set_int_option("sm_rawlock_gc_interval_ms", 50);
    options.set_int_option("sm_rawlock_gc_free_segment_count", 20);
    options.set_int_option("sm_rawlock_gc_max_segment_count", 30);

    // Transaction
    options.set_int_option("sm_rawlock_xctpool_initseg", (1 << 7));    // 128 bytes
    options.set_int_option("sm_rawlock_xctpool_segsize", (1 << 8));    // 256 bytes

    // Cleaner
    options.set_int_option("sm_cleaner_interval_millisec_min", 1000);
    options.set_int_option("sm_cleaner_interval_millisec_max", 256000);
    options.set_int_option("sm_cleaner_write_buffer_pages", 64);

    // Misc.
    options.set_bool_option("sm_bufferpool_swizzle", false);
    options.set_int_option("sm_num_page_writers", 1);
    options.set_bool_option("sm_backgroundflush", true);

    return options;
}

// Helper function for CPU cycles/ticks
static __inline__ unsigned long long rdtsc(void)
{
    // Measurement:
    // We are using rdtsc in this performance measurement,
    // although rdtsc might not be perfrect for time measurement.
    // The problem is that the CPU might throttle (with it does most of the time,
    // when all processes are blocked), so a cpu clock might mean more
    // time in that case. This is of course CPU and OS dependant.
    //
    // Alternatively, using gettimeofday() to get microseconds, but this
    // does not exclude CPU sleep time

    unsigned int hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

// Helper function for the initial storage population
w_rc_t populate_performance_records(stid_t &stid)
{
    // Insert TOTAL_RECORDS records into the store
    //   Key - 1 - TOTAL_RECORDS
    //   Key - for key ending with 3, 5 or 7, skip the insertions

    char key_buf[key_buf_size+1];
    char data_buf[data_buf_size+1];
    memset(data_buf, '\0', data_buf_size+1);
    memset(data_buf, 'D', data_buf_size);

    uint64_t key_int = 0;

    // Insert
    for (uint64_t i=0; i < TOTAL_RECORDS; ++i)
    {
        // Prepare the record first
        ++key_int;                              // Key starts from 1 and ends with TOTAL_RECORDS
        memset(key_buf, '\0', key_buf_size+1);
        test_env->itoa(key_int, key_buf, 10);   // Fill the key portion with integer converted to string

        int last_digit = strlen(key_buf)-1;
        if (('3' == key_buf[last_digit]) || ('5' == key_buf[last_digit]) || ('7' == key_buf[last_digit]))
        {
            // Skip records with keys ending in 3, 5, or 7
        }
        else
        {
            W_DO(test_env->btree_insert_and_commit(stid, key_buf, data_buf));
        }
    }

    // Verify the record count
    x_btree_scan_result s;
    const int recordCount = TOTAL_RECORDS/10*7;  // Because we skip all the 3, 5 and 7s
    W_DO(test_env->btree_scan(stid, s));
    EXPECT_EQ(recordCount, s.rownum);
    std::cout << "Initial shutdown, record count in B-tree: " << s.rownum << std::endl;

    return RCOK;
}

// Helper function to insert one record
w_rc_t insert_performance_records(stid_t &stid,
                                       uint64_t key_int,           // Key value to insert
                                       test_txn_state_t txnState)  // What to do with the transaction
{
    char key_buf[key_buf_size+1];
    char data_buf[data_buf_size+1];
    memset(data_buf, '\0', data_buf_size+1);
    memset(data_buf, 'D', data_buf_size);

    memset(key_buf, '\0', key_buf_size+1);
    test_env->itoa(key_int, key_buf, 10);  // Fill the key portion with integer converted to string

    // Perform the operation
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert(stid, key_buf, data_buf));
    if (t_test_txn_commit == txnState)
        W_DO(test_env->commit_xct());
    else if (t_test_txn_abort == txnState)
        W_DO(test_env->abort_xct());
    else // t_test_txn_in_flight
        ss_m::detach_xct();

    return RCOK;
}

// Helper functtion to delete one record
w_rc_t delete_performance_records(stid_t &stid,
                                       uint64_t key_int,           // Key value to delete
                                       test_txn_state_t txnState)  // What to do with the transaction
{
    char key_buf[key_buf_size+1];
    char data_buf[data_buf_size+1];
    memset(data_buf, '\0', data_buf_size+1);
    memset(data_buf, 'D', data_buf_size);

    memset(key_buf, '\0', key_buf_size+1);
    test_env->itoa(key_int, key_buf, 10);  // Fill the key portion with integer converted to string

    // Perform the operation
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_remove(stid, key_buf));
    if (t_test_txn_commit == txnState)
        W_DO(test_env->commit_xct());
    else if (t_test_txn_abort == txnState)
        W_DO(test_env->abort_xct());
    else // t_test_txn_in_flight
        ss_m::detach_xct();

    return RCOK;
}

// Helper function to update one record, update the data portion, not the key portion
w_rc_t update_performance_records(stid_t &stid,
                                        uint64_t key_int,           // Key value to update
                                        test_txn_state_t txnState)  // What to do with the transaction
{
    char key_buf[key_buf_size+1];
    char data_buf[data_buf_size+1];
    // Update operation uses the key to locate existing record
    // and then update the data field, no change in the key field
    memset(data_buf, '\0', data_buf_size+1);
    memset(data_buf, 'A', data_buf_size);

    memset(key_buf, '\0', key_buf_size+1);
    test_env->itoa(key_int, key_buf, 10);  // Fill the key portion with integer converted to string

    // Update only change the data filed, not the key field
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_update(stid, key_buf, data_buf));
    if (t_test_txn_commit == txnState)
        W_DO(test_env->commit_xct());
    else if (t_test_txn_abort == txnState)
        W_DO(test_env->abort_xct());
    else // t_test_txn_in_flight
        ss_m::detach_xct();

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
        // Perform the specified operation, make it an in-flight transaction
        if (t_op_insert == op_type)
        {
            W_DO(insert_performance_records(stid, key_int, t_test_txn_in_flight));
        }
        else if (t_op_delete == op_type)
        {
            W_DO(delete_performance_records(stid, key_int, t_test_txn_in_flight));
        }
        else
        {
            w_assert1(t_op_update == op_type);
            W_DO(update_performance_records(stid, key_int, t_test_txn_in_flight));
        }

        // Now we are done
        return RCOK;
    }

    rc_t _rc;
    bool _running;           // Indicate whether the thread is still running or not
    ss_m *ssm;               // Set by caller before thread started
    stid_t stid;             // Set by caller before thread started
    uint64_t key_int;        // Key value, set by caller before thread started
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
    unsigned long long _total_cycles[TOTAL_CYCLE_SLOTS]; // Performance measurement total cycle counter
                                                         //record cycle count every
                                                         // RECORD_FREQUENCY transactions

    void record_cycles()
    {
        // Record the cycle count into the next slot
        w_assert1(_cycle_slot_index < TOTAL_CYCLE_SLOTS);
        _end = rdtsc();
        _total_cycles[_cycle_slot_index] = (_end - _start);
        ++_cycle_slot_index;
        return;
    }

    w_rc_t initial_shutdown(ss_m *ssm)
    {
        // Populate the base database, follow by a normal shutdown to persist data on disk
        // Single thread, single index
        //     Data - insert TOTAL_RECORDS records
        //     Key - 1 - TOTAL_RECORDS
        //     Key - for key ending with 3, 5 or 7, skip the insertions
        // Normal shutdown

        w_assert1(NULL != ssm);
        std::cout << std::endl << "Start initial_shutdown (phase 1)..." << std::endl << std::endl;

        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        populate_performance_records(_stid);
        return RCOK;
    }

    w_rc_t pre_shutdown(ss_m *ssm)
    {
        // Start the system with existing data from phase 1,
        // Use multiple worker threads to generate many user transactions,
        // including commit and in-flight transactions
        // Only up to TOTAL_IN_FLIGHT_THREADS in-flight transactions
        // due to Express implementation limitation and the number of worker
        // threads in test code.  In order to generate an in-flight transaction,
        // the worker thread has to detach from the active transaction, but
        // the worker thread would not be able to start a new transaction
        // once it detached from an active transaction
        //       Key - insert
        //                Key - if the key ending with 3, commit
        //                Key - if the key ending with 5, skip
        //                Key - if the key ending with 7, in-flight insertions
        //       Key - all other keys, update the existing records
        //                Key - if the key ending with 2 or 9, in-flight updates
        // Either normal shutdown or simulate system crash

        w_assert1(NULL != ssm);
        std::cout << std::endl << "Start pre_shutdown (phase 2)..." << std::endl << std::endl;

        w_rc_t rc;
        char key_buf[key_buf_size+1];
        uint64_t key_int = 0;
        int current_in_flight__count = 0;   // Active in-flight thread count

        // Initialize the in-flight worker threads but not started
        op_worker_t workers[TOTAL_IN_FLIGHT_THREADS];
        for (int i = 0; i < TOTAL_IN_FLIGHT_THREADS; ++i)
        {
            workers[i].ssm = ssm;
            workers[i].stid = _stid;
        }

        for (uint64_t i=0; i < TOTAL_RECORDS; ++i)
        {
            // Prepare the record first
            ++key_int;                              // Key starts from 1 and ends with TOTAL_RECORDS
            memset(key_buf, '\0', key_buf_size+1);
            test_env->itoa(key_int, key_buf, 10);   // Fill the key portion with integer converted to string

            int last_digit = strlen(key_buf)-1;
            if ('3' == key_buf[last_digit])
            {
                // Insert and commit using main thread
                rc = insert_performance_records(_stid, key_int, t_test_txn_commit);
                EXPECT_FALSE(rc.is_error());
            }
            else if ('5' == key_buf[last_digit])
            {
                // Skip
            }
            else if ('7' == key_buf[last_digit])
            {
                // Insert and in-flight using child threads up to
                // TOTAL_IN_FLIGHT_THREADS
                // After TOTAL_IN_FLIGHT_THREADS, insert and commit
                // using main thread
                if (current_in_flight__count < TOTAL_IN_FLIGHT_THREADS)
                {
                    // Prepare the worker thread
                    workers[current_in_flight__count].key_int = key_int;
                    workers[current_in_flight__count].op_type = t_op_insert;

                    // Sleep for a short duration before start the thread,
                    // so the thread manager can catch up
                    ::usleep(SHORT_SLEEP_MICROSEC);
                    W_DO(workers[current_in_flight__count].fork());

                    // Increment the worker thread counter
                    ++current_in_flight__count;
                }
                else
                {
                    // Used up all the threads, commit the rest using main thread
                    rc = insert_performance_records(_stid, key_int, t_test_txn_commit);
                    EXPECT_FALSE(rc.is_error());
                }
            }
            else
            {
                // For the rest, update operation
                if (('2' == key_buf[last_digit]) || ('9' == key_buf[last_digit]))
                {
                    // Update and in-flight using child threads up to
                    // TOTAL_IN_FLIGHT_THREADS
                    // After TOTAL_IN_FLIGHT_THREADS, update and commit
                    // using main thread
                    if (current_in_flight__count < TOTAL_IN_FLIGHT_THREADS)
                    {
                        // Prepare the worker thread
                        workers[current_in_flight__count].key_int = key_int;
                        workers[current_in_flight__count].op_type = t_op_update;

                        // Sleep for a short duration before start the thread,
                        // so the thread manager can catch up
                        ::usleep(SHORT_SLEEP_MICROSEC);
                        W_DO(workers[current_in_flight__count].fork());

                        // Increment the worker thread counter
                        ++current_in_flight__count;
                    }
                    else
                    {
                        // Used up all the threads, commit the rest using main thread
                        rc = update_performance_records(_stid, key_int, t_test_txn_commit);
                        EXPECT_FALSE(rc.is_error());
                    }
                }
                else
                {
                    // Update and commit using main thread
                    rc = update_performance_records(_stid, key_int, t_test_txn_commit);
                    EXPECT_FALSE(rc.is_error());
                }
            }
        }

        // Terminate the started in-flight worker threads, sleep for a while first
        ::usleep(SHORT_SLEEP_MICROSEC*100);
        w_assert1(current_in_flight__count < TOTAL_IN_FLIGHT_THREADS);
        for (int i=0; i < current_in_flight__count; ++i)
        {
            W_DO(workers[i].join());
            EXPECT_FALSE(workers[i]._running) << i;
            EXPECT_FALSE(workers[i]._rc.is_error()) << i;
        }

        // Now we are done, ready to shutdown (either normal or crash)
        // At this point, we should have TOTAL_RECORDS/10*9 records (= 2700)
        // in the B-tree although some of them are in-flights which would get
        // rollback later
        // There are 'current_in_flight__count' in-flight transactions
        // including both insert and update transactions,
        // while TOTAL_IN_FLIGHT_THREADS/3+1 are for insertions (= 34)
        // Therefore after system re-started the next time (after rollbacked),
        // we should have TOTAL_RECORDS/10*9 - (TOTAL_IN_FLIGHT_THREADS/3+1)
        // valid records (=2666) if there is additional insert or delete operations.

        // Initialize the cycle recorder
        _cycle_slot_index = 0;
        for (int i = 0; i < TOTAL_CYCLE_SLOTS; ++i)
            _total_cycles[i] = 0;

        // Start the timer to measure time from before test code shutdown (normal or crash)
        // to finish TOTAL_SUCCESS_TRANSACTIONS user transactions
        _start = rdtsc();

        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *)  // Not using the ss_m input parameter
    {
        // Restart the system, start concurrent user transactions
        // Measuer the time required to finish TOTAL_SUCCESS_TRANSACTIONS concurrent transactions
        //       Key - insert records with key ending in 5
        //       Key - delete if key ending with 2 or 0 (all the 2's should rollback first)
        //       Key - if key ending with 3, update
        //       Key - if key ending with 7, update (rollback first)
        //       Key - if key ending with 9
        //                M2, M4 - all transactions rolled back
        //                M3 - - these transactions won't be rolled back due to on-demand
        // Measure and draw a curve that shows the numbers of completed transactions over time

        // Measure and record the amount of time took for system to shutdown and restart (no recovery)
        record_cycles();

        std::cout << std::endl << "Start post_shutdown (phase 3)..." << std::endl << std::endl;

        // Now start measuring the Instant Restart duration based on the
        // first 1000 successful transactions
        // All concurrent transactions are coming in using the main thread (single threaded)

        w_rc_t rc;
        char key_buf[key_buf_size+1];
        bool failed = false;        // Safy net to break out from infinite loop (bug)
        int started_txn_count = 0;  // Started transactions
        int succeed_txn_count = 0;  // Completed transaction

        // Mix up the record for user transactions, some of the operations are on the
        // existing records, while some are on non-existing (out-of-bound) records
        uint64_t key_int = 1;                        // Key from the existing records
        uint64_t high_key_int = TOTAL_RECORDS * 60;  // Key from out-of-bound (non-existing) records
        uint64_t actual_key;                         // Key value used for the current operation
        int out_of_bound = 0;                        // Frequency to use an out-of-bound record (OUT_OF_BOUND_FREQUENCY)

        for (succeed_txn_count=0; succeed_txn_count < TOTAL_SUCCESS_TRANSACTIONS;)
        {
            // User transactions in M2 might fail, in such case the failed transactions
            // are not counted toward the total transaction count

            // A safy to prevent infinite loop (bug) in code
            if (key_int > (TOTAL_RECORDS*50))
            {
                failed = true;
                break;
            }

            // Prepare the key value into key buffer
            memset(key_buf, '\0', key_buf_size+1);
            ++out_of_bound;
            if (out_of_bound >= OUT_OF_BOUND_FREQUENCY)
            {
                // The operation is on a non-existing out-of-bound record, so insert only
                test_env->itoa(high_key_int, key_buf, 10);  // Fill the key portion with integer converted to string
                actual_key = high_key_int;
                --high_key_int;                             // Key starts from TOTAL_RECORDS*60 and decreasing
                out_of_bound = 0;
            }
            else
            {
                // The operation is on a record with in-bound key value, need to decide insert/update or delete
                test_env->itoa(key_int, key_buf, 10);  // Fill the key portion with integer converted to string
                actual_key = key_int;
                ++key_int;                             // Key starts from 1 and increasing
            }
            int last_digit = strlen(key_buf)-1;

            // Decide what to do with this record
            if ('5' == key_buf[last_digit])
            {
                // Insert, this should be okay regardless we are within or out of bound (TOTAL_RECORDS)
                // In M2 it might fail due to commit_lsn checking, in such case, skip
                rc = insert_performance_records(_stid, actual_key, t_test_txn_commit);
                if (rc.is_error())
                {
#ifndef MEASURE_PERFORMANCE
                    std::cout << "Insert error with key ending in 5, key: " << actual_key << ", error: " << rc.get_message() << std::endl;
#endif

                    // Retry with a different key, do not increase succeed_txn_count
                    W_DO(test_env->abort_xct());
                    ++started_txn_count;
                    continue;
                }
            }
            else if (('7' == key_buf[last_digit]) && (actual_key < TOTAL_RECORDS))
            {
                // Some of the keys ending with 7 might be in-flight insertions from phase 2,
                // they would be rollback so those records do not exist anymore,
                // while some of the insertions were committed, so the records do exist.
                rc = insert_performance_records(_stid, actual_key, t_test_txn_commit);
                if (rc.is_error())
                {
#ifndef MEASURE_PERFORMANCE
                    if (34 == rc.err_num()) // Duplicate error
                    {
                        // Record was inserted and committed during phase 2, not an error
                    }
                    else
                    {
                        // If record was an in-flight during phase 2, we should be able to insert
                        // this record, so the error is un-expected
                        // Except that in M2, it might fail due to commitLsn check

                        std::cout << "Insert error with key ending in 7, key: " << actual_key << ", error: " << rc.get_message() << std::endl;
                    }
#endif

                    // Retry with a different key, do not increase succeed_txn_count
                    W_DO(test_env->abort_xct());
                    ++started_txn_count;
                    continue;
                }
            }
            else if (('3' == key_buf[last_digit]) && (actual_key < TOTAL_RECORDS))
            {
                // This key should exist already, update the data field
                rc = update_performance_records(_stid, actual_key, t_test_txn_commit);
                if (rc.is_error())
                {
#ifndef MEASURE_PERFORMANCE
                    std::cout << "Update error with key ending in 3, key: " << actual_key << ", error: " << rc.get_message() << std::endl;
#endif
                    // Retry with a different key, do not increase succeed_txn_count
                    W_DO(test_env->abort_xct());
                    ++started_txn_count;
                    continue;
                }
            }
            else if ((('2' == key_buf[last_digit]) || ('0' == key_buf[last_digit])) && (actual_key < TOTAL_RECORDS))
            {
                // This key should exist already, delete
                // Key ending with '2' was an in-flight update from phase 2, it has to be rollback first
                rc = delete_performance_records(_stid, actual_key, t_test_txn_commit);
                if (rc.is_error())
                {
#ifndef MEASURE_PERFORMANCE
                    if ('2' == key_buf[last_digit])
                        std::cout << "Delete error with key ending in 2, key: " << actual_key << ", error: " << rc.get_message() << std::endl;
                    else
                        std::cout << "Delete error with key ending in 0, key: " << actual_key << ", error: " << rc.get_message() << std::endl;
#endif

                    // Retry with a different key, do not increase succeed_txn_count
                    W_DO(test_env->abort_xct());
                    ++started_txn_count;
                    continue;
                }
            }
            else if (actual_key > TOTAL_RECORDS)
            {
                // If key value is out of the original bound, and we don't have
                // enough user transactions yet, insert
                rc = insert_performance_records(_stid, actual_key, t_test_txn_commit);
                if (rc.is_error())
                {
#ifndef MEASURE_PERFORMANCE
                    std::cout << "Insert error with out-of-bound key value, key: " << actual_key << ", error: " << rc.get_message() << std::endl;
#endif

                    // Retry with a different key, do not increase succeed_txn_count
                    W_DO(test_env->abort_xct());
                    ++started_txn_count;
                    continue;
                }
            }
            else
            {
                // These are the remaining already existed records, do not touch them
                // therefore in the case of M3, some loser transactions (ended with '9') won't be rolled back

                // Do not update started or succeed counter, go to the next one
                continue;
            }

            // If we get here, issued a transaction and succeeded
            // Update both transaction counters
            ++started_txn_count;       // Started counter
            ++succeed_txn_count;       // Succeeded counter

            // Is it time to record the cycles?
            // Note we are recording cycles based on succeed transactions only
            // not counting the failed transactions
            if (0 == (succeed_txn_count % RECORD_FREQUENCY))
            {
                // Record cycles
                record_cycles();
            }
        }

        // If M3, some of the in-flight transactions (ending with 9)
        // might not have been rollback at this point
        // Issue a scan to get record count, we are not measuring
        // CPU cycles for the scan operation
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));

        if (true == failed)
        {
            // We did not have enough successful user transaction due to too many failures
            // this is unexpected behavior, report it
            std::cerr << "ERROR..." << std::endl;
            std::cerr << "Failed to complete " << TOTAL_SUCCESS_TRANSACTIONS << " transactions, potential infinite loop." << std::endl;
            std::cerr << "Total started transactions: " << started_txn_count << std::endl;
            std::cerr << "Total completed transactions: " << succeed_txn_count << std::endl;
            std::cerr << "Existing record count in B-tree: " << s.rownum << std::endl;
        }
        else
        {
            // Reporting:
            int32_t restart_mode = test_env->_restart_options->restart_mode;
            bool crash_shutdown = test_env->_restart_options->shutdown_mode;  // false = normal shutdown

            // Report CPU cycles
            //     _total_cycles[0] - cycles from shutdown to restart, no recovery
            //     ...
            //     _total_cycles[i] - record cycle every 10 user transactions
            //     ...
            //     _total_cycles[TOTAL_CYCLE_SLOTS] - cycles from shutdown to finish
            //                                                              TOTAL_SUCCESS_TRANSACTIONS user transactions
            int count = 0;
            std::cout << "Successful transaction count and accumulated CPU cycles: " << std::endl;
            for (int iIndex = 0; iIndex < _cycle_slot_index; ++iIndex)
            {

                std::cout << " Count: " << count << ", CPU cycles: " << _total_cycles[iIndex];
                if (0 < iIndex)
                    std::cout << ", delta: " << _total_cycles[iIndex] - _total_cycles[iIndex-1]
                              << " CPU cycles" << std::endl;
                else
                    std::cout << std::endl;

                count += RECORD_FREQUENCY;
            }

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
            std::cout << "Existing record count in B-tree: " << s.rownum << std::endl;

            unsigned long long duration = (_total_cycles[_cycle_slot_index - 1] - _total_cycles[0]);

            std::cout << std::endl << "Total CPU cycles for " << TOTAL_SUCCESS_TRANSACTIONS << " successful user transactions: "
                      << duration << std::endl;

            // Convert from CPU cycles to time
            // Note the magic conversion number below is from 'cat /proc/cpuinfo: cpu MHz'
            // the value is machine dependent so run it on the machine which is doing the performance tests
            unsigned time =0;
            time = (duration/1596000);
            std::cout << "Time in milliseconds: " << time << std::endl << std::endl;
        }

        return RCOK;
    }
};

//////////////////////////////////////////////////////////////////////////
// TODO(Restart).... see TODO in 'runRestartPerfTest' for potentially adjusting raw lock options
//////////////////////////////////////////////////////////////////////////

/**/
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
/**/

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

/**
// Not passing - M2
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
                                           true,             // Turn locking ON  <= using commit lsn, recovery is done
                                                             // through the child thread without lock acquisition
                                                             // Locking on with user transaction does not affect recovery
                                           DISK_QUOTA_IN_KB, // 2GB, disk_quota_in_pages, how much disk space is allowed,
                                           make_perf_options()),  // Other options
                                           0);
}
**/

/**
// Not passing - M3
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
// Not passing - M4
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
