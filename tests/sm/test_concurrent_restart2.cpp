#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"
#include "xct.h"
#include "sm_base.h"
#include "sm_external.h"

btree_test_env *test_env;

const int WAIT_TIME = 1000; // Wait 1 second
const int SHORT_WAIT_TIME = 100; // Wait 1/10 of a  second

// Test cases to test concurrent restart - system opens after restart Log Analysis phase finished
//       Single thread
//       Single index
// Depending on the restart mode, the test results might vary and therefore tricky

// Due to the number of test cases, broke them into 2 suites:
// Test_concurrent_restart1
// Test_concurrent_restart2 - this file

lsn_t get_durable_lsn() {
    lsn_t ret;
    ss_m::get_durable_lsn(ret);
    return ret;
}
void output_durable_lsn(int W_IFDEBUG1(num)) {
    DBGOUT1( << num << ".durable LSN=" << get_durable_lsn());
}


// Test case with simple transactions (1 in-flight)
// one concurrent txn with conflict during undo phase
class restart_simple_concurrent_undo : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa3", "data3"));

        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa2", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "aa4", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        bool fCrash = test_env->_restart_options->shutdown_mode;
        int32_t restart_mode = test_env->_restart_options->restart_mode;

        if (restart_mode < m3_default_restart)
        {
            // Wait a short time if not M3, this is to allow REDO to finish,
            // but hit the UNDO phase using specified restart mode which waits before UNDO
            ::usleep(SHORT_WAIT_TIME);
        }

        // Verify
        w_rc_t rc = test_env->btree_scan(_stid_list[0], s);   // Should have only one page of data
                                                      // while restart is on for this page
                                                      // although REDO is done, UNDO is not
                                                      // therefore the concurrent txn should not be allowed
        if (fCrash && restart_mode < m3_default_restart)
        {
            // M2, error is expected
            if (rc.is_error())
            {
                DBGOUT3(<<"restart_simple_concurrent_undo: tree_scan conflict: " << rc);

                // Abort the failed scan txn
                test_env->abort_xct();

                // Sleep to give Recovery sufficient time to finish
                while (true == test_env->in_restart()) {
                    // Concurrent restart is still going on, wait
                    ::usleep(WAIT_TIME);
                }

                // Try again
                W_DO(test_env->btree_scan(_stid_list[0], s));

                EXPECT_EQ (3, s.rownum);
                EXPECT_EQ (std::string("aa1"), s.minkey);
                EXPECT_EQ (std::string("aa3"), s.maxkey);
                return RCOK;
            }
            else
            {
                std::cerr << "restart_simple_concurrent_undo: scan operation should not succeed"<< std::endl;
                return RC(eINTERNAL);
            }
        }
        else
        {
            // M3, blocking operation, it should succeed
            W_DO(test_env->btree_scan(_stid_list[0], s));
            EXPECT_EQ (3, s.rownum);
            EXPECT_EQ (std::string("aa1"), s.minkey);
            EXPECT_EQ (std::string("aa3"), s.maxkey);
            return RCOK;
        }
    }
};

/* Passing */
TEST (RestartTest, SimpleConcurrentUndoN) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_undo context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_undo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, SimpleConcurrentUndoNF) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_undo context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_undo_fl_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, SimpleConcurrentUndoC) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_undo context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_undo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, SimpleConcurrentUndoCF) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_undo context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_undo_fl_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, SimpleConcurrentUndoN3) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_undo context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, SimpleConcurrentUndoC3) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_undo context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, scan query triggers on_demand recovery
                                               // No delay in undo because no restart child thread
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with more than one page of data (1 in-flight), one concurrent txn to
// access a non-dirty page so it should be allowed
class restart_concurrent_no_conflict : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        // Multiple committed transactions with many pages
        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit, true));  // flags: No checkpoint, commit, one transaction per insert

        // Issue a checkpoint to make sure these committed txns are flushed
        W_DO(ss_m::checkpoint());

        // Now insert more records, these records are at the beginning of B-tree
        // therefore if these records cause a page rebalance, it would be in the parent page
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa2", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "aa4", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        int32_t restart_mode = test_env->_restart_options->restart_mode;

        if (restart_mode < m3_default_restart)
        {
            // If not in M3, wait a while, this is to give REDO a chance to reload the root page
            // but still wait in REDO phase due to test mode
            ::usleep(SHORT_WAIT_TIME*5);
        }

        // Wait in restart both REDO and UNDO, this is to ensure
        // user transaction encounter concurrent restart
        // Insert into the first page, depending on how far the REDO goes,
        // the insertion might or might not succeed
        W_DO(test_env->begin_xct());
        w_rc_t rc = test_env->btree_insert(_stid_list[0], "aa7", "data4");
        if (rc.is_error())
        {
            // Conflict, it should not happen if M3
            // In M2 restart mode using commit_lsn, conflict is possible if
            // the entire buffer pool was never flushed, the commit_lsn would
            // be the earliest LSN, therefore it blocks all user transactions
            if (restart_mode < m3_default_restart)
            {
                // M2
                DBGOUT3(<<"restart_concurrent_no_conflict: tree_insertion conflict");
                W_DO(test_env->abort_xct());
            }
            else
            {
                // M3, insert should succeed
                DBGOUT3(<<"restart_concurrent_no_conflict: tree_insertion conflict in M3, un-expected error!!!");
                EXPECT_FALSE(rc.is_error());   // It should trigger a rollback and record insertion should succeed
            }
        }
        else
        {
            // Succeed
            DBGOUT3(<<"restart_concurrent_no_conflict: tree_insertion succeeded");
            W_DO(test_env->commit_xct());
        }

        // Wait before the final verfication
        // Note this 'in_restart' check is not reliable if on_demand restart (m3),
        // but it is okay because with on_demand restart, it blocks concurrent
        // transactions instead of failing concurrent transactions
        if (restart_mode < m3_default_restart)
        {
            // Not wait if M3 or M4
            while (true == test_env->in_restart())
            {
                // Concurrent restart is still going on, wait
                ::usleep(WAIT_TIME);
            }
        }

        // Verify
        W_DO(test_env->btree_scan(_stid_list[0], s));
        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        recordCount += 3;  // Count after checkpoint
        if (!rc.is_error())
            recordCount += 1;  // Count after concurrent insert

        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);

        return RCOK;
    }
};

/* Passing, WOD with minimal logging */
TEST (RestartTest, ConcurrentNoConflictN) {
    test_env->empty_logdata_dir();
    restart_concurrent_no_conflict context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, WOD with minimal logging */
TEST (RestartTest, ConcurrentNoConflictNF) {
    test_env->empty_logdata_dir();
    restart_concurrent_no_conflict context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, minimal logging */
TEST (RestartTest, ConcurrentNoConflictC) {
    test_env->empty_logdata_dir();
    restart_concurrent_no_conflict context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, full logging */
TEST (RestartTest, ConcurrentNoConflictCF) {
    test_env->empty_logdata_dir();
    restart_concurrent_no_conflict context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ConcurrentNoConflictN3) {
    test_env->empty_logdata_dir();
    restart_concurrent_no_conflict context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ConcurrentNoConflictC3) {
    test_env->empty_logdata_dir();
    restart_concurrent_no_conflict context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, insert triggers on_demand recovery
                                               // No delay because no restart child thread
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with more than one page of data (1 in-flight), one concurrent txn to
// access an in_doubt page so it should not be allowed if in M2, but passing in M3
class restart_concurrent_conflict : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        // Multiple committed transactions with many pages
        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit, true));   // flags: No checkpoint, commit, one transaction per insert

        // Issue a checkpoint to make sure these committed txns are flushed
        W_DO(ss_m::checkpoint());

        // Now insert more records, make sure these records are at
        // the end of B-tree (append)
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "zz3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "zz1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "zz2", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "zz4", "data4"));     // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        const bool fCrash = test_env->_restart_options->shutdown_mode;
        const int32_t restart_mode = test_env->_restart_options->restart_mode;

        if (restart_mode < m3_default_restart)
        {
            // If not M3, wait a while, this is to give REDO a chance to reload the root page
            // but still wait in REDO phase due to test mode
            ::usleep(SHORT_WAIT_TIME*5);
        }

        // Wait in restart if restart child thread (M2), this is to ensure user transaction
        // encounter concurrent restart
        // Insert into the last page which should cause a conflict if commit_lsn (m2)
        // No conflict if locking (M3) and it should trigger on_demand recovery
        W_DO(test_env->begin_xct());
        w_rc_t rc = test_env->btree_insert(_stid_list[0], "zz5", "data5");
        if (rc.is_error())
        {
            // Failed to insert
            if (!fCrash)
            {
                // Normal shutdown, we should not have failure
                std::cerr << "restart_concurrent_conflict: tree_insertion failed on a normal shutdown, un-expected"<< std::endl;
                return RC(eINTERNAL);
            }
            else
            {
                // Crash shutdown
                if (restart_mode < m3_default_restart)
                {
                    // M2, error is expected, simply abort the transaction
                    W_DO(test_env->abort_xct());           // M2 behavior, expected
                }
                else
                {
                    // M3, we should not see failure
                    std::cerr << "restart_concurrent_conflict: tree_insertion failed on a M3 crash shutdown, un-expected"<< std::endl;
                    return RC(eINTERNAL);
                }
            }
        }
        else
        {
            // Insertion succeeded
            if ((fCrash) && (restart_mode < m3_default_restart))
            {
                // M2 crash shutdown, insertion should not succeed
                std::cerr << "restart_concurrent_conflict: tree_insertion succeed on M2 crash shutdown, un-expected"<< std::endl;
                return RC(eINTERNAL);
            }
            else
            {
                // If normal shutdown or in M3 (both normal or crash)
                // insertion should succeed
                // Roll it back so the record count stays the same
                W_DO(test_env->abort_xct());
            }
        }
        // Wait before the final verfication
        // Note this 'in_restart' check is not reliable if on_demand restart (m3),
        // but it is okay because with on_demand restart, it blocks concurrent
        // transactions instead of failing concurrent transactions
        if (restart_mode < m3_default_restart)
        {
            // Not wait if M3 or M4
            while (true == test_env->in_restart())
            {
                // Concurrent restart is still going on, wait
                ::usleep(WAIT_TIME);
            }
        }

        // Verify
        rc = test_env->btree_scan(_stid_list[0], s);
        if (rc.is_error())
        {
            test_env->abort_xct();
            std::cerr << "restart_concurrent_conflict: failed to scan the tree, try again: " << rc << std::endl;
            ::usleep(WAIT_TIME);
            rc = test_env->btree_scan(_stid_list[0], s);
            if (rc.is_error())
            {
                std::cerr << "restart_concurrent_conflict: 2nd try failed again: " << rc << std::endl;
                return RC(eINTERNAL);
            }
        }

        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;  // Count before checkpoint
        recordCount += 3;  // Count after checkpoint

        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ (std::string("zz3"), s.maxkey);
        return RCOK;
    }
};

/* Passing, WOD with minimal logging */
TEST (RestartTest, ConcurrentConflictN) {
    test_env->empty_logdata_dir();
    restart_concurrent_conflict context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, full logging */
TEST (RestartTest, ConcurrentConflictNF) {
    test_env->empty_logdata_dir();
    restart_concurrent_conflict context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, minimal logging */
TEST (RestartTest, ConcurrentConflictC) {
    test_env->empty_logdata_dir();
    restart_concurrent_conflict context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, full logging */
TEST (RestartTest, ConcurrentConflictCF) {
    test_env->empty_logdata_dir();
    restart_concurrent_conflict context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ConcurrentConflictN3) {
    test_env->empty_logdata_dir();
    restart_concurrent_conflict context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ConcurrentConflictC3) {
    test_env->empty_logdata_dir();
    restart_concurrent_conflict context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, insert triggers on_demand recovery
                                               // No delay because no restart child thread
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with more than one page of data (1 in-flight), multiple concurrent txns
// some should succeeded (no conflict) while others failed if M2 (conflict)
// also one 'conflict' user transaction after restart which should succeed
class restart_multi_concurrent_conflict : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        // Multiple committed transactions with many pages
        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit, true));   // flags: No checkpoint, commit, one transaction per insert

        // Issue a checkpoint to make sure these committed txns are flushed
        W_DO(ss_m::checkpoint());

        // Now insert more records, make sure these records are at
        // the end of B-tree (append)
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "zz3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "zz1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "zz2", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "zz7", "data4"));     // in-flight

        if (test_env->_restart_options->enable_checkpoints)
            W_DO(ss_m::checkpoint());

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        bool fCrash = test_env->_restart_options->shutdown_mode;
        bool m3_restart = test_env->_restart_options->restart_mode >= m3_default_restart;
        bool checkpoints_enabled = test_env->_restart_options->enable_checkpoints;
        int32_t restart_mode = test_env->_restart_options->restart_mode;

        if (false == m3_restart)
        {
            // If not in M3, wait a while, this is to give REDO a chance to reload the root page
            // but still wait in REDO phase due to test mode
            ::usleep(SHORT_WAIT_TIME*5);
        }

        // If restart child thread (M2), wait in restart, this is to ensure user transaction encounter concurrent restart

        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;  // Count before checkpoint
        recordCount += 3;  // Count after checkpoint

        // Insert into the first page which might or might not succeed depending
        // on the restart mode (M2 or M3, meaning commit_lsn or lock)
        // if using commit_lsn and if the entire buffer pool was never flushed then
        // commit_lsn would be the first lsn and no concurrent transactions would
        // be allowed
        W_DO(test_env->begin_xct());
        w_rc_t rc = test_env->btree_insert(_stid_list[0], "aa1", "data4");
        if (rc.is_error())
        {
            if (true == m3_restart)
            {
                // M3, the insert triggers recovery and the insert operation should not failed
                std::cerr << "restart_multi_concurrent_conflict in M3: insertion of 'aa1' should have succeeded but failed" << rc;
                return RC(eINTERNAL);
            }

            // Will failed if the REDO phase did not process far enough, this is not an error
            W_DO(test_env->abort_xct());
        }
        else
        {
            // Succeeded
            recordCount += 1;
            W_DO(test_env->commit_xct());
        }
        if(checkpoints_enabled)
            W_DO(ss_m::checkpoint());
        // Insert into the last page which should cause a conflict
        W_DO(test_env->begin_xct());
        rc = test_env->btree_insert(_stid_list[0], "zz5", "data4");
        if ((rc.is_error() && fCrash && !m3_restart) || (!rc.is_error() && (!fCrash || m3_restart))) // Only m2 restart mode with crash shutdown should fail,
        {
            // m3 rm and m2 rm with normal shutdown should succeed
            // Expected behavior, abort the transaction
            W_DO(test_env->abort_xct());
        }
        else
        {
            // The rest of the scenarios
            if ((!fCrash || m3_restart) && rc.is_error())
            {
                // Normal shutdown or M3, should have succeeded, did not
                std::cerr << "restart_multi_concurrent_conflict: tree_insertion should have succeeded but failed" << rc;
                return RC(eINTERNAL);
            }
            else if (fCrash && !m3_restart && !rc.is_error())
            {
                // Crash, not M3, insertion should failed but succeeded

                std::cerr << "restart_multi_concurrent_conflict: tree_insertion should failed but succeeded"<< std::endl;
                return RC(eINTERNAL);
            }
        if (rc.is_error())
            test_env->abort_xct();
        }

        // Wait before the final verfication
        // Note this 'in_restart' check is not reliable if on_demand restart (m3),
        // but it is okay because with on_demand restart, it blocks concurrent
        // transactions instead of failing concurrent transactions
        if (restart_mode < m3_default_restart)
        {
            // Not wait if M3 or M4
            while (true == test_env->in_restart())
            {
                // Concurrent restart is still going on, wait
                ::usleep(WAIT_TIME);
            }
        }

        if(checkpoints_enabled)
            W_DO(ss_m::checkpoint());

        // Tried the failed txn again and it should succeed this time
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "zz5", "data4"));
        W_DO(test_env->commit_xct());
        recordCount += 1;

        // Verify
        W_DO(test_env->btree_scan(_stid_list[0], s));

        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ (std::string("zz5"), s.maxkey);
        return RCOK;
    }
};

/* Passing, WOD with minimal logging */
TEST (RestartTest, MultiConcurrentConflictN) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_conflict context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, full logging */
TEST (RestartTest, MultiConcurrentConflictNF) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_conflict context;

    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, minimal logging */
TEST (RestartTest, MultiConcurrentConflictC) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_conflict context;

    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, full logging */
TEST (RestartTest, MultiConcurrentConflictCF) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_conflict context;

    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_fl_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);   // true = simulated crash
                                                                  // full logging
}
/**/

/* Passing, full logging, checkpoints */
TEST (RestartTest, MultiConcurrentConflictNFC) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_conflict context;

    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_fl_delay_restart; // full logging
    options.enable_checkpoints = true;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, MultiConcurrentConflictN3) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_conflict context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, MultiConcurrentConflictC3) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_conflict context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, insert triggers on_demand recovery
                                               // No delay because no restart child thread
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with simple transactions (1 in-flight)
// one concurrent txn with exact same insert during redo phase
class restart_concurrent_same_insert : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa3", "data3"));

        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa2", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "aa4", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        const bool fCrash = test_env->_restart_options->shutdown_mode;
        const int32_t restart_mode = test_env->_restart_options->restart_mode;
        x_btree_scan_result s;
        // No wait in test code, but wait in restart if M2
        // This is to ensure concurrency

        if (fCrash && restart_mode < m3_default_restart)
        {
            // M2
            W_DO(test_env->begin_xct());
            w_rc_t rc = test_env->btree_insert(_stid_list[0], "aa4", "data4");  // Insert same record that was in-flight, should be possible.
            // Will fail in m2 due to conflict, should succeed in m3 (not immediately).

            if (rc.is_error())
            {
                // Expected failure
                DBGOUT3(<<"restart_concurrent_same_insert: insert failed: " << rc);

                // Abort the failed scan txn
                test_env->abort_xct();

                // Sleep to give Recovery sufficient time to finish
                while (true == test_env->in_restart())
                {
                    // Concurrent restart is still going on, wait
                    ::usleep(WAIT_TIME);
                }

                // Try again, should work now
                W_DO(test_env->begin_xct());
                W_DO(test_env->btree_insert(_stid_list[0], "aa4", "data4"));
                W_DO(test_env->commit_xct());
            }
            else
            {
                std::cerr << "restart_concurrent_same_insert: insert operation should not succeed"<< std::endl;
                return RC(eINTERNAL);
            }
        }
        else
        {
            // M3 behavior, either normal or crash shutdown
            // blocking and insertion should succeed
            W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa4", "data4"));
        }

        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (4, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, ConcurrentSameInsertN) {
    test_env->empty_logdata_dir();
    restart_concurrent_same_insert context;

    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ConcurrentSameInsertNF) {
    test_env->empty_logdata_dir();
    restart_concurrent_same_insert context;

    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ConcurrentSameInsertC) {
    test_env->empty_logdata_dir();
    restart_concurrent_same_insert context;

    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ConcurrentSameInsertCF) {
    test_env->empty_logdata_dir();
    restart_concurrent_same_insert context;

    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ConcurrentSameInsertN3) {
    test_env->empty_logdata_dir();
    restart_concurrent_same_insert context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ConcurrentSameInsertC3) {
    test_env->empty_logdata_dir();
    restart_concurrent_same_insert context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, insert triggers on_demand recovery
                                               // No delay because no restart child thread
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with simple transactions (1 in-flight)
// one concurrent txn with an existing commited record which should fail
class restart_concurrent_duplicate_insert : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit, true));   // flags: No checkpoint, commit, one transaction per insert

        // Enough inserts to cause more page split
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq2", "data2"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq7", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq9", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq0", "data1"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "qq4", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        const bool fCrash = test_env->_restart_options->shutdown_mode;
        const int32_t restart_mode = test_env->_restart_options->restart_mode;
        x_btree_scan_result s;
        // No wait in test code, but wait in restart if M2
        // This is to ensure concurrency

        if (fCrash && restart_mode < m3_default_restart)
        {
            // M2
            w_rc_t rc = test_env->btree_insert_and_commit(_stid_list[0], "qq1", "data1");  // Should error on duplicate
            // Will fail in m2 due to conflict, should succeed in m3 (not immediately).

            if (rc.is_error())
            {
                // Expected failure
                DBGOUT3(<<"restart_concurrent_same_insert: insert failed: " << rc);

                // Abort the failed scan txn
                test_env->abort_xct();

                // Sleep to give Recovery sufficient time to finish
                while (true == test_env->in_restart())
                {
                    // Concurrent restart is still going on, wait
                    ::usleep(WAIT_TIME);
                }

                // Try again, should work now
                rc = test_env->btree_insert_and_commit(_stid_list[0], "qq1", "data1");  // should error on duplicate
                if (rc.is_error())
                {
                    if (34 == rc.err_num())
                    {
                        // Correct behavior
                    }
                    else
                    {
                        // Unexpected error
                        std::cerr << "Unexpected error on insertion, error:" << rc.get_message() << std::endl;
                        return RC(eINTERNAL);
                    }
                }
                else
                {
                    // Should not succeed
                    std::cerr << "Insertion of duplicated key should not succeed" << std::endl;
                    return RC(eINTERNAL);
                }
            }
            else
            {
                std::cerr << "restart_concurrent_same_insert: insert operation should not succeed"<< std::endl;
                return RC(eINTERNAL);
            }
        }
        else
        {
            // M3 behavior, either normal or crash shutdown
            // blocking and insertion should succeed
            w_rc_t rc = test_env->btree_insert_and_commit(_stid_list[0], "qq1", "data1");  // Should error on duplicate
            if (rc.is_error())
            {
                if (34 == rc.err_num())
                {
                    // Correct behavior
                }
                else
                {
                    // Unexpected error
                    std::cerr << "Unexpected error on insertion, error:" << rc.get_message() << std::endl;
                    return RC(eINTERNAL);
                }
            }
            else
            {
                // Should not succeed
                std::cerr << "Insertion of duplicated key should not succeed" << std::endl;
                return RC(eINTERNAL);
            }
        }

        W_DO(test_env->btree_scan(_stid_list[0], s));
        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;  // Count through big population
        recordCount += 6;
        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ (std::string("qq9"), s.maxkey);
        return RCOK;
    }
};

/* Passing - M2 */
TEST (RestartTest, ConcurrentDuplicateInsertC) {
    test_env->empty_logdata_dir();
    restart_concurrent_duplicate_insert context;

    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ConcurrentDuplicateInsertC3) {
    test_env->empty_logdata_dir();
    restart_concurrent_duplicate_insert context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, insert triggers on_demand recovery
                                               // No delay because no restart child thread
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
