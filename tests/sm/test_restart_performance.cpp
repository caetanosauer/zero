#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"
#include "xct.h"
#include "sm_base.h"
#include "sm_external.h"

#include <cstdlib>
#include <vector>
#include <algorithm>

btree_test_env *test_env;

// Restart performance test to compare various restart methods
// M1 - traditional restart, open system after Restart finished
// M2 - Enhanced traditional restart, open system after Log Analysis phase,
//         concurrency check based on commit_lsn
//         restart carried out by restart child thread
// M3 - On_demand restart, open system after Log Analysis phase,
//         concurrency check based on lock acquisition
//         restart carried out by user transaction threads
// M4 - Mixed restart, open system after Log Analysis phase,
//         concurrency check based on lock acquisition
//         restart carried out by both restart child thread and user thransaction threads


// Define constants
const int TOTAL_THREAD = 100;     // Worker threads
const int TOTAL_RECORDS = 3000;   // Total record count
const int key_size = 10;          // Record key size
const int data_size = 10;         // Record data size

static __inline__ unsigned long long rdtsc(void)
{
    unsigned int hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

lsn_t get_durable_lsn()
{
    lsn_t ret;
    ss_m::get_durable_lsn(ret);
    return ret;
}
void output_durable_lsn(int W_IFDEBUG1(num))
{
    DBGOUT1( << num << ".durable LSN=" << get_durable_lsn());
}

w_rc_t populate_performance_records(stid_t &stid)
{
    // Insert TOTAL_RECORDS records into the store
    //   Key - 1 - TOTAL_RECORDS
    //   Key - for key ending with 3, 5 or 7, skip the insertions

    char key_buf[key_size+1];
    char data_buf[data_size+1];
    memset(data_buf, '\0', data_size+1);
    memset(data_buf, 'D', data_size);

    uint64_t key_int = 0;

    // Insert
    for (int i=0; i < TOTAL_RECORDS; ++i)
    {
        // Prepare the record first
        ++key_int;
        memset(key_buf, '\0', key_size+1);
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
    return RCOK;
}

w_rc_t insert_performance_records()
//                                  stid_t &stid,
//                                  test_txn_state_t txnState,  // What to do with the transaction
//                                  bool splitIntoSmallTrans)   // Default: false
                                                              // Ture if one insertion per transaction
{

    return RCOK;
}


w_rc_t delete_performance_records()
//                                        stid_t &stid,
//                                        test_txn_state_t txnState) // What to do with the transaction
{

    return RCOK;
}

w_rc_t update_performance_records()
//                                        stid_t &stid,
//                                        test_txn_state_t txnState)  // What to do with the transaction
{

    return RCOK;
}

// Main performance test case used by:
//    Normal shutdown - using M1 code path, due to normal shutdown, minor differences in milestone code path
//    M1 crash shutdown
//    M2 crash shutdown
//    M3 crash shutdown
//    M4 crash shutdown
class restart_multi_performance : public restart_performance_test_base
{
public:

    w_rc_t initial_shutdown(ss_m *ssm)
    {
        // Populate the base database, follow by a normal shutdown to persist data on disk
        // Single thread, single index
        //     Data - insert TOTAL_RECORDS records
        //     Key - 1 - 3000
        //     Key - for key ending with 3, 5 or 7, skip the insertions

        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        populate_performance_records(_stid);
        output_durable_lsn(2);
        return RCOK;
    }

    w_rc_t pre_shutdown(ss_m *ssm)
    {
//////////////////////////////////////////////////
// TODO(Restart)... phase 2, in-flights and crash
//////////////////////////////////////////////////
        // Start the system with existing data from phase 1,
        // Use multiple worker threads to generate many user transactions,
        // including commit and in-flight (maybe abort) transactions
        // Only up to TOTAL_THREAD in-flight transactions due to Express implementation
        // limitation and the number of worker threads in test code.  In order to
        // generate an in-flight transaction, the worker thread detach from the active
        // transaction, but the worker thread would not be able to start a new transaction
        // once it detached from an active transaction
        //       Key - insert
        //                Key - if the key ending with 3, commit
        //                Key - if the key ending with 5, skip
        //                Key - if the key ending with 7, in-flight insertions
        //       Key - all other keys, update the existing records
        //                Key - if the key ending with 2 or 9, in-flight updates
        // Simulate system crash

        w_assert1(NULL != ssm);

        output_durable_lsn(3);

        output_durable_lsn(4);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *ssm)
    {
//////////////////////////////////////////////////
// TODO(Restart)... phase 3, concurrent
//////////////////////////////////////////////////
        // Restart the system, start concurrent user transactions
        // Measuer the time required to finish 1000 concurrent transactions
        //       Key - insert records with key ending in 5
        //       Key - delete if key ending with 2 or 0 (all the 2's should rollback first)
        //       Key - if key ending with 3, update
        //       Key - if key ending with 7, update (rollback first)
        // Measure and draw a curve that shows the numbers of completed transactions over time

/**
unsigned long long totalCycles = 0;
unsigned long long start = rdtsc();

unsigned long long end = rdtsc();
totalCycles += (end - start);
**/

       w_assert1(NULL != ssm);

        output_durable_lsn(5);

        output_durable_lsn(6);
        return RCOK;
    }
};

/******** from test_logbuf_multi_thread.cpp, use it as an example
class op_worker_t : public smthread_t {
public:
    static const int total = REC_COUNT; // number of records inserted per thread
    char buf[data_size];
    char key[key_size];

    op_worker_t()
        : smthread_t(t_regular, "op_worker_t"), _running(true) {
        _thid = next_thid++;
    }
    virtual void run() {
        _rc = run_core();
        _running = false;
    }
    rc_t run_core() {
        tlr_t rand (_thid);
        std::cout << "Worker-" << _thid << " started" << std::endl;

        uint64_t key_int = 0;

        uint64_t *keys = new uint64_t[total];  // store the keys

        // insert
        for (int i=1; i<=total; i++) {
            key_int = lintel::unsafe::atomic_fetch_add<uint64_t>(&next_history, 1);
            keys[i-1] = key_int;
            itoa(key_int, key, 10);
            itoa(key_int, buf, 10);

            //W_DO(consume(4096,ssm));

            W_DO(test_env->begin_xct());
            W_DO(test_env->btree_insert(stid, key, buf));

            // abort if the key ends with a 9
            if (key[strlen(key)-1]=='9') {
                W_DO(test_env->abort_xct());
            }
            else {
                W_DO(test_env->commit_xct());
            }
        }

        for (int i=1; i<=total; i++) {
            key_int = keys[i-1];
            itoa(key_int, key, 10);

            //W_DO(consume(4096,ssm));

            W_DO(test_env->begin_xct());
            if(key_int%1000==0) {
                // overwrite records whose key % 1000 == 0
                buf[0]='z';
                buf[1]='\0';
                W_DO(test_env->btree_overwrite(stid, key, buf, strlen(key)-1));
                // abort if the second to last character is 9
                if (key[strlen(key)-2]=='9') {
                    W_DO(test_env->abort_xct());
                }
                else {
                    W_DO(test_env->commit_xct());
                }
            }
            else {
                // update records whose key % 100 == 0 && key % 1000 != 0
                if(key_int%100==0) {
                    itoa(key_int+1, buf, 10);
                    W_DO(test_env->btree_update(stid, key, buf));
                    // abort if the second to last character is 9
                    if (key[strlen(key)-2]=='9') {
                        W_DO(test_env->abort_xct());
                    }
                    else {
                        W_DO(test_env->commit_xct());
                    }
                }
                else {
                    // remove records whose key % 10 == 0 && key % 100 !=0 && key % 1000 != 0
                    if(key_int%10==0) {
                        W_DO(test_env->btree_remove(stid, key));
                        // abort if the second to last character is 9
                        if (key[strlen(key)-2]=='9') {
                            W_DO(test_env->abort_xct());
                        }
                        else {
                            W_DO(test_env->commit_xct());
                        }
                    }
                    else {
                        W_DO(test_env->commit_xct());
                    }
                }
            }
        }


        delete []keys;


        std::cout << "Worker-" << _thid << " done" << std::endl;

        return RCOK;
    }

    rc_t _rc;
    bool _running;
    int _thid;
    ss_m *ssm;
    stid_t stid;
};

w_rc_t restart_multi_performance(ss_m *ssm, test_volume_t *volume) {
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, volume, stid, root_pid));

    // run workload
    op_worker_t workers[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; ++i) {
        workers[i].ssm = ssm;
        workers[i].stid = stid;
        W_DO(workers[i].fork());
    }

    for (int i = 0; i < THREAD_COUNT; ++i) {
        W_DO(workers[i].join());
        EXPECT_FALSE(workers[i]._running) << i;
        EXPECT_FALSE(workers[i]._rc.is_error()) << i;
    }


    // verify results

    char buf[data_size];
    char key[key_size];

    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));


    int total = REC_COUNT*THREAD_COUNT;
    EXPECT_EQ (total - total/10 - total/100*8, s.rownum);

    for (int i=1; i<=total; i++) {
        std::string result="";
        itoa(i, key, 10);
        if(i%1000==0) {
            // overwrite records whose key % 1000 == 0
            test_env->btree_lookup_and_commit(stid, key, result);
            // abort if the second to last character is 9
            if (key[strlen(key)-2]=='9') {
                EXPECT_EQ(std::string(key), result);
            }
            else {
                EXPECT_EQ('z', result[result.length()-1]);
                result[result.length()-1]='0';
                EXPECT_EQ(std::string(key), result);
            }
        }
        else {
            if(i%100==0) {
                // update records whose key % 100 == 0 && key % 1000 != 0
                test_env->btree_lookup_and_commit(stid, key, result);
                // abort if the second to last character is 9
                if (key[strlen(key)-2]=='9') {
                    EXPECT_EQ(std::string(key), result);
                }
                else {
                    itoa(i+1, buf, 10);
                    EXPECT_EQ(std::string(buf), result);
                }
            }
            else {
                if(i%10==0) {
                    // remove records whose key % 10 == 0 && key % 100 !=0 && key % 1000 != 0
                    // total/100*9 records are removed
                    result="data";
                    test_env->btree_lookup_and_commit(stid, key, result);
                    if (key[strlen(key)-2]=='9') {
                        EXPECT_EQ(std::string(key), result);
                    }
                    else {
                        // if not found, the result would become empty
                        EXPECT_EQ(true, result.empty());
                    }
                }
                else {
                    // non modified records
                    result="data";
                    test_env->btree_lookup_and_commit(stid, key, result);
                    if (key[strlen(key)-1]=='9') {
                        // if not found, the result would become empty
                        EXPECT_EQ(true, result.empty());
                    }
                    else {
                        EXPECT_EQ(std::string(key), result);
                    }
                }
            }
        }
    }
    return RCOK;
}
**/

// TODO(Restart)... might need to increase lock table size for performance test, all milestones
//                          for M3/M4 suign raw lock, might need to set other stuff, see test_raw.cpp
/**
int runRestartTest (restart_test_base *context,
  restart_test_options *restart_options,
  bool use_locks = false,            // Disable locking by default, M3/M4 need to enable locking
  int32_t lock_table_size = default_locktable_size,
  int disk_quota_in_pages = default_quota_in_pages,
  int bufferpool_size_in_pages = default_bufferpool_size_in_pages,
  uint32_t cleaner_threads = 1,
  uint32_t cleaner_interval_millisec_min	   = 1000,
  uint32_t cleaner_interval_millisec_max	   = 256000,
  uint32_t cleaner_write_buffer_pages		   = 64,
  bool initially_enable_cleaners = true,
  bool enable_swizzling = default_enable_swizzling
  );
 **/
// TODO(Restart)...   for M3/M4 suign raw lock, might need to set other stuff, see test_lock_raw.cpp
/**
sm_options options;
options.set_int_option("sm_locktablesize", small ? 100 : 6400);
options.set_int_option("sm_rawlock_lockpool_initseg", has_init ? 2 : 0);
options.set_int_option("sm_rawlock_xctpool_initseg", has_init ? 2 : 0);
options.set_int_option("sm_rawlock_lockpool_segsize", small ? 1 << 3 : 1 << 5);
options.set_int_option("sm_rawlock_xctpool_segsize", small ? 1 << 2 : 1 << 4);
options.set_int_option("sm_rawlock_gc_interval_ms", 50);
options.set_int_option("sm_rawlock_gc_generation_count", 4);
options.set_int_option("sm_rawlock_gc_free_segment_count", 2);
options.set_int_option("sm_rawlock_gc_max_segment_count", 3);
**/


TEST (RestartPerfTest, MultiPerformanceNormal)
{
    test_env->empty_logdata_dir();
    restart_multi_performance context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;

    // Normal shutdown
    EXPECT_EQ(test_env->runRestartPerfTest(&context, &options, true), 0);   // Turn locking on
}

TEST (RestartPerfTest, MultiPerformanceM1)
{
    test_env->empty_logdata_dir();
    restart_multi_performance context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m1_default_restart;

    // M1 crash shutdown
    EXPECT_EQ(test_env->runRestartPerfTest(&context, &options, false), 0);   // Turn locking off
}

TEST (RestartPerfTest, MultiPerformanceM2)
{
    test_env->empty_logdata_dir();
    restart_multi_performance context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart;

    // M2 crash shutdown
    EXPECT_EQ(test_env->runRestartPerfTest(&context, &options, false), 0);   // Turn locking off
}

TEST (RestartPerfTest, MultiPerformanceM3)
{
    test_env->empty_logdata_dir();
    restart_multi_performance context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart;

    // M3 crash shutdown
    EXPECT_EQ(test_env->runRestartPerfTest(&context, &options, true), 0);     // Turn locking on
}

TEST (RestartPerfTest, MultiPerformanceM4)
{
    test_env->empty_logdata_dir();
    restart_multi_performance context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m4_default_restart;

    // M4 crash shutdown
    EXPECT_EQ(test_env->runRestartPerfTest(&context, &options, true), 0);    // Turn locking on
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
