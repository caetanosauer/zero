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
// Test_concurrent_restart1 - this file
// Test_concurrent_restart2


lsn_t get_durable_lsn() {
    lsn_t ret;
    ss_m::get_durable_lsn(ret);
    return ret;
}
void output_durable_lsn(int W_IFDEBUG1(num)) {
    DBGOUT1( << num << ".durable LSN=" << get_durable_lsn());
}

// Test case without any operation, start and normal shutdown SM
class restart_empty : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *) {
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, EmptyN) {
    test_env->empty_logdata_dir();
    restart_empty context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, EmptyC) {
    test_env->empty_logdata_dir();
    restart_empty context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, EmptyN3) {
    test_env->empty_logdata_dir();
    restart_empty context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover and does not get into backward log scan loop
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, EmptyC3) {
    test_env->empty_logdata_dir();
    restart_empty context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover but
                                               // go through Log Analysis backward scan loop
                                               // although only checkpoint log records to process
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with simple transactions (1 in-flight) and normal shutdown, no concurrent activities during restart
class restart_simple : public restart_test_base  {
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
        int32_t restart_mode = test_env->_restart_options->restart_mode;
        w_rc_t rc;

        // Wait before the final verfication
        // Note this 'in_restart' check is not reliable if on_demand restart (M3),
        // but it is okay because with on_demand restart, it blocks concurrent
        // transactions instead of failing concurrent transactions
        // If M2, recovery is done via a child restart thread
        if (restart_mode < m3_default_restart)
        {
            // Not wait if M3 or M4
            while (true == test_env->in_restart())
            {
                // Concurrent restart is still going on, wait
                ::usleep(WAIT_TIME);
            }
        }

        // Verify, if M3, the update query triggers on_demand REDO (page loading)
        // and UNDO (transaction rollback)

        // Both normal and crash shutdowns, regardless Instart Restart milestone,
        // the update should fail due to in-flight transaction rolled back alreadly
        rc = test_env->btree_update_and_commit(_stid_list[0], "aa4", "dataXXX");
        if (rc.is_error())
        {
            std::cout << "Update failed, expected behavior" << std::endl;
        }
        else
        {
            std::cout << "Update succeed, this is not expected behavior" << std::endl;
            EXPECT_FALSE(rc.is_error());   // It should trigger a rollback and record should not exist after rollback
        }

        // Verify, if M3, the scan query trigger the on_demand REDO (page loading)
        // and UNDO (transaction rollback)
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, SimpleN) {
    test_env->empty_logdata_dir();
    restart_simple context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, SimpleNF) {
    test_env->empty_logdata_dir();
    restart_simple context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, SimpleC) {
    test_env->empty_logdata_dir();
    restart_simple context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, SimpleCF) {
    test_env->empty_logdata_dir();
    restart_simple context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, SimpleN3) {
    test_env->empty_logdata_dir();
    restart_simple context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, SimpleC3) {
    test_env->empty_logdata_dir();
    restart_simple context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, update triggers on_demand recovery
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with transactions (1 in-flight with multiple operations)
// no concurrent activities during restart
class restart_complex_in_flight : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa3", "data3"));

        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa4", "data4"));

        W_DO(test_env->begin_xct());                                     // in-flight
        W_DO(test_env->btree_insert(_stid_list[0], "aa7", "data5"));
        W_DO(test_env->btree_insert(_stid_list[0], "aa2", "data2"));
        W_DO(test_env->btree_insert(_stid_list[0], "aa5", "data7"));

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        int32_t restart_mode = test_env->_restart_options->restart_mode;
        x_btree_scan_result s;

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
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, ComplexInFlightN) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ComplexInFlightNF) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ComplexInFlightC) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ComplexInFlightCF) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ComplexInFlightN3) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ComplexInFlightC3) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, scan query triggers on_demand recovery
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with transactions (1 in-flight) with checkpoint
// no concurrent activities during restart
class restart_complex_in_flight_chkpt : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "aa3", "data3"));
        W_DO(test_env->btree_insert(_stid_list[0], "aa1", "data1"));
        W_DO(test_env->btree_insert(_stid_list[0], "aa4", "data4"));
        W_DO(test_env->commit_xct());

        W_DO(test_env->begin_xct());                                     // in-flight
        W_DO(test_env->btree_insert(_stid_list[0], "aa5", "data5"));
        W_DO(test_env->btree_insert(_stid_list[0], "aa2", "data2"));
        W_DO(test_env->btree_insert(_stid_list[0], "aa7", "data7"));
        W_DO(ss_m::checkpoint());

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        int32_t restart_mode = test_env->_restart_options->restart_mode;
        x_btree_scan_result s;

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
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, ComplexInFlightChkptN) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight_chkpt context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ComplexInFlightChkptNF) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight_chkpt context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ComplexInFlightChkptC) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight_chkpt context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ComplexInFlightChkptCF) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight_chkpt context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ComplexInFlightChkptN3) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight_chkpt context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ComplexInFlightChkptC3) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight_chkpt context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, scan query triggers on_demand recovery
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with 1 transaction (in-flight with more than one page of data)
// no concurrent activities during restart
class restart_multi_page_in_flight : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        // One big uncommitted txn
        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_in_flight));  // false: No checkpoint; false: Do not commit, in-flight

        output_durable_lsn(3);

        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        int32_t restart_mode = test_env->_restart_options->restart_mode;
        w_rc_t rc;

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

        // Verify, if M3, the insert query triggers on_demand REDO (page loading)
        // and UNDO (transaction rollback)

        // Both normal and crash shutdowns, regardless Instart Restart milestone,
        // the insert should succeed due to in-flight transaction rolled back alreadly
        rc = test_env->btree_insert_and_commit(_stid_list[0], "aa4", "dataXXX");
        if (rc.is_error())
        {
            std::cout << "Insertion failed, not expected behavior" << std::endl;
            EXPECT_TRUE(rc.is_error());   // In M3, insert operation should trigger UNDO
        }
        else
        {
            std::cout << "Insertion succeed, this is expected behavior" << std::endl;
        }

        // Verify
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (1, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultiPageInFlightN) {
    test_env->empty_logdata_dir();
    restart_multi_page_in_flight context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, full logging */
TEST (RestartTest, MultiPageInFlightNF) {
    test_env->empty_logdata_dir();
    restart_multi_page_in_flight context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, minimal logging */
TEST (RestartTest, MultiPageInFlightC) {
    test_env->empty_logdata_dir();
    restart_multi_page_in_flight context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, full logging */
TEST (RestartTest, MultiPageInFlightCF) {
    test_env->empty_logdata_dir();
    restart_multi_page_in_flight context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, MultiPageInFlightN3) {
    test_env->empty_logdata_dir();
    restart_multi_page_in_flight context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, MultiPageInFlightC3) {
    test_env->empty_logdata_dir();
    restart_multi_page_in_flight context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, insert triggers on_demand recovery
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with 1 aborted transaction (more than one page of data)
// no concurrent activities during restart
class restart_multi_page_abort : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        // One big uncommitted txn
        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_abort));  // false: No checkpoint;
                                                                                         // t_test_txn_abort: abort the txn
                                                                                         // one big transaction, no key prefix
        output_durable_lsn(3);

        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        int32_t restart_mode = test_env->_restart_options->restart_mode;
        x_btree_scan_result s;

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
        EXPECT_EQ (0, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultiPageAbortN) {
    test_env->empty_logdata_dir();
    restart_multi_page_abort context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiPageAbortNF) {
    test_env->empty_logdata_dir();
    restart_multi_page_abort context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiPageAbortC) {
    test_env->empty_logdata_dir();
    restart_multi_page_abort context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, full logging */
TEST (RestartTest, MultiPageAbortCF) {
    test_env->empty_logdata_dir();
    restart_multi_page_abort context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, MultiPageAbortN3) {
    test_env->empty_logdata_dir();
    restart_multi_page_abort context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, MultiPageAbortC3) {
    test_env->empty_logdata_dir();
    restart_multi_page_abort context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, scan query triggers on_demand recovery
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with simple transactions (1 in-flight) and crash shutdown, one concurrent chkpt
class restart_concurrent_chkpt : public restart_test_base  {
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
        int32_t restart_mode = test_env->_restart_options->restart_mode;

        // Concurrent chkpt
        W_DO(ss_m::checkpoint());

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
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));  // Should have only one page of data
                                               // while restart is on for this page
                                               // therefore the concurrent txn should not be allowed
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, ConcurrentChkptN) {
    test_env->empty_logdata_dir();
    restart_concurrent_chkpt context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ConcurrentChkptNF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chkpt context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ConcurrentChkptC) {
    test_env->empty_logdata_dir();
    restart_concurrent_chkpt context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ConcurrentChkptCF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chkpt context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ConcurrentChkptN3) {
    test_env->empty_logdata_dir();
    restart_concurrent_chkpt context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, ConcurrentChkptC3) {
    test_env->empty_logdata_dir();
    restart_concurrent_chkpt context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, scan query triggers on_demand recovery
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with simple transactions (1 in-flight)
// one concurrent txn with conflict during redo phase
class restart_simple_concurrent_redo : public restart_test_base  {
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
        const bool fCrash = test_env->_restart_options->shutdown_mode;
        const int32_t restart_mode = test_env->_restart_options->restart_mode;
        // No wait in test code, but wait in restart
        // This is to ensure concurrency

        if (fCrash && (restart_mode < m3_default_restart))
        {
            // M2 crash shutdown
            w_rc_t rc = test_env->btree_scan(_stid_list[0], s);  // Should have only one page of data (root)
                                                         // while restart is on for this page
                                                         // the wait only happens after root
                                                         // page being recoverd
            if (rc.is_error())
            {
                // Expected
                DBGOUT3(<<"restart_simple_concurrent_redo: tree_scan error: " << rc);

                // Abort the failed scan txn
                test_env->abort_xct();
            }
            else
            {
                std::cerr << "restart_simple_concurrent_redo: scan operation should not succeed"<< std::endl;
                return RC(eINTERNAL);
            }

            // Sleep to give Recovery sufficient time to finish
            while (true == test_env->in_restart())
            {
                // Concurrent restart is still going on, wait
                ::usleep(WAIT_TIME);
            }

            // Try again
            W_DO(test_env->btree_scan(_stid_list[0], s));

        }
        else
        {
            // M3 crash shutdown, blocking operation and it should succeed
            W_DO(test_env->btree_scan(_stid_list[0], s));
        }

        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, SimpleConcurrentRedoN) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_redo context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, SimpleConcurrentRedoNF) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_redo context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_fl_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, SimpleConcurrentRedoC) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_redo context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, SimpleConcurrentRedoCF) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_redo context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, SimpleConcurrentRedoN3) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_redo context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, SimpleConcurrentRedoC3) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_redo context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, scan query triggers on_demand recovery
                                               // No delay in redo because no restart child thread
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

// Test case with multi-page b-tree, simple transactions (1 in-flight)
// one concurrent txn with conflict during redo phase
class restart_multi_concurrent_redo : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        // One big committed txn
        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit));  // flags: no checkpoint, commit

        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa4", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "aa2", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        bool fCrash = test_env->_restart_options->shutdown_mode;
        int32_t restart_mode = test_env->_restart_options->restart_mode;
        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5 + 1;
        // No wait in test code, but wait in restart
        // This is to ensure concurrency

        if (fCrash && restart_mode < m3_default_restart)
        {
            // if m2 crash shutdown
            // Verify
            w_rc_t rc = test_env->btree_scan(_stid_list[0], s); // Should have multiple pages of data
                                                        // the concurrent txn is a read/scan txn
                                                        // should still not be allowed due to delay in REDO in m2 crash shutdown
            if (rc.is_error())
            {
                // Expected
                DBGOUT3(<<"restart_multi_concurrent_redo: tree_scan error: " << rc);

                // Abort the failed scan txn
                test_env->abort_xct();

                // Sleep to give Recovery sufficient time to finish
                while (true == test_env->in_restart()) {
                    // Concurrent restart is still going on, wait
                    ::usleep(WAIT_TIME);
                }

                // Try again
                W_DO(test_env->btree_scan(_stid_list[0], s));

                EXPECT_EQ (recordCount, s.rownum);
                EXPECT_EQ (std::string("aa4"), s.minkey);
                return RCOK;
            }
            else
            {
                std::cerr << "restart_multi_concurrent_redo: scan operation should not succeed"<< std::endl;
                return RC(eINTERNAL);
            }
        }
        else
        {
            // M3, both crash and non-crash, it should block and succeed

            W_DO(test_env->btree_scan(_stid_list[0], s));
            EXPECT_EQ (recordCount, s.rownum);
            EXPECT_EQ(std::string("aa4"), s.minkey);
            return RCOK;
        }
    }
};

/* Passing, WOD with minimal logging, in-flight is in the first page */
TEST (RestartTest, MultiConcurrentRedoN) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_redo context;

    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, full logging, in-flight is in the first page */
TEST (RestartTest, MultiConcurrentRedoNF) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_redo context;

    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - minimal logging */
TEST (RestartTest, MultiConcurrentRedoC) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_redo context;

    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - full logging */
TEST (RestartTest, MultiConcurrentRedoCF) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_redo context;

    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, MultiConcurrentRedoN3) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_redo context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

/* Passing - M3 */
TEST (RestartTest, MultiConcurrentRedoC3) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_redo context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, scan query triggers on_demand recovery
                                               // No delay in redo because no restart child thread
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true /*use_locks*/), 0);
}
/**/

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
