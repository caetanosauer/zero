#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "xct.h"
#include "sm_base.h"

btree_test_env *test_env;

// Test cases to test serial and traditional restart - system opens after the entire 'restart' finished
//       Single thread
//       Single index
//       Multiple threads
// Caller specify restart mode.

lsn_t get_durable_lsn() {
    lsn_t ret;
    ss_m::get_durable_lsn(ret);
    return ret;
}
void output_durable_lsn(int W_IFDEBUG1(num)) {
    DBGOUT1( << num << ".durable LSN=" << get_durable_lsn());
}

std::string getMaxKeyString(char maxPrefix) {
    const int recordsPerThrd = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
    char a = '0' + (recordsPerThrd-1) / 10;
    char b = '0' + (recordsPerThrd-1) % 10;
    char maxkeystr[10] = {"key000"};
    if (maxPrefix != '\0') {
        maxkeystr[3] = maxPrefix;
        maxkeystr[4] = a;
        maxkeystr[5] = b;
    }
    else {
        maxkeystr[3] = a;
        maxkeystr[4] = b;
    }
    return std::string(maxkeystr);
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
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, EmptyC) {
    test_env->empty_logdata_dir();
    restart_empty context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


// Test case without checkpoint and normal shutdown
class restart_basic : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa5", "data5"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa2", "data2"));
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (5, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa5"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, BasicN) {
    test_env->empty_logdata_dir();
    restart_basic context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, BasicC) {
    test_env->empty_logdata_dir();
    restart_basic context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


// Test case with checkpoint and normal shutdown
class restart_checkpoint : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa4", "data4"));
        W_DO(ss_m::checkpoint());
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa5", "data5"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa2", "data2"));
        // If enabled the 2nd checkpoint, it is passing also
        // W_DO(ss_m::checkpoint());
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (5, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa5"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, CheckpointN) {
    test_env->empty_logdata_dir();
    restart_checkpoint context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, CheckpointC) {
    test_env->empty_logdata_dir();
    restart_checkpoint context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


// Test case with more than one page of data, without checkpoint and normal shutdown
class restart_many : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit));  // flags: No checkpoint, commit
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, ManySimpleN) {
    test_env->empty_logdata_dir();
    restart_many context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ManySimpleC) {
    test_env->empty_logdata_dir();
    restart_many context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


// Test case with more than one page of data, with checkpoint and normal shutdown
class restart_many_checkpoint : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        W_DO(test_env->btree_populate_records(_stid_list[0], true, t_test_txn_commit));  // flags: Checkpoint, commit

        // If enabled the 2nd checkpoint, it is passing also
        // W_DO(ss_m::checkpoint());

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, ManyCheckpointN) {
    test_env->empty_logdata_dir();
    restart_many_checkpoint context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ManyCheckpointC) {
    test_env->empty_logdata_dir();
    restart_many_checkpoint context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


// Test case with an uncommitted transaction, no checkpoint, normal shutdown
class restart_inflight : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));

        // Start a transaction but no commit before normal shutdown
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "aa2", "data2"));
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, InflightN) {
    test_env->empty_logdata_dir();
    restart_inflight context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, InflightC) {
    test_env->empty_logdata_dir();
    restart_inflight context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


// Test case with an uncommitted transaction, checkpoint, normal shutdown
class restart_inflight_checkpoint : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
        W_DO(ss_m::checkpoint());

        // Start a transaction but no commit, checkpoint, and then normal shutdown
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "aa2", "data2"));
        W_DO(ss_m::checkpoint());
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, InflightCheckpointN) {
    test_env->empty_logdata_dir();
    restart_inflight_checkpoint context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
// In this test case, the user checkpoint was aborted due to crash shutdown
TEST (RestartTest, InflightCheckpointC) {
    test_env->empty_logdata_dir();
    restart_inflight_checkpoint context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Test case with an uncommitted transaction, no checkpoint */
class restart_complic_inflight : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));

        // Start a transaction but no commit, normal shutdown
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "aa7", "data7"));
        W_DO(test_env->btree_insert(_stid_list[0], "aa2", "data2"));
        W_DO(test_env->btree_insert(_stid_list[0], "aa5", "data5"));
        W_DO(test_env->btree_insert(_stid_list[0], "aa0", "data0"));
        W_DO(test_env->btree_insert(_stid_list[0], "aa9", "data9"));
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, ComplicInflightN) {
    test_env->empty_logdata_dir();
    restart_complic_inflight context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ComplicInflightC) {
    test_env->empty_logdata_dir();
    restart_complic_inflight context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


// Test case with an uncommitted transaction, more than one page of data, no checkpoint, simulated crash shutdown
class restart_inflight_many : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit));  // flags: No checkpoint, commit
        output_durable_lsn(3);

        // In-flight transaction, no commit
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "aa1", "data1"));

        output_durable_lsn(4);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(5);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, InflightManyN) {
    test_env->empty_logdata_dir();
    restart_inflight_many context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, InflightManyC) {
    test_env->empty_logdata_dir();
    restart_inflight_many context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


// Test case with an uncommitted transaction, more than one page of data, checkpoint, simulated crash shutdown
class restart_inflight_ckpt_many : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        W_DO(test_env->btree_populate_records(_stid_list[0], true, t_test_txn_commit));  // flags: Checkpoint, commit
        output_durable_lsn(3);

        // 2nd checkpoint before the in-flight transaction
        // W_DO(ss_m::checkpoint());

        // In-flight transaction, no commit
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "aa1", "data1"));

        output_durable_lsn(4);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(5);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, InflightCkptManyN) {
    test_env->empty_logdata_dir();
    restart_inflight_ckpt_many context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, InflightCkptManyC) {
    test_env->empty_logdata_dir();
    restart_inflight_ckpt_many context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Test case with a committed insert, an aborted removal and an aborted update */
class restart_aborted_remove : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa0", "data0"));
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "aa1", "data1"));
        W_DO(test_env->btree_remove(_stid_list[0], "aa0"));
        W_DO(test_env->abort_xct());
        test_env->btree_update_and_commit(_stid_list[0], "aa0", "data0000");
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ(1, s.rownum);
        EXPECT_EQ(std::string("aa0"), s.maxkey);
        std::string data;
        test_env->btree_lookup_and_commit(_stid_list[0], "aa0", data);
        EXPECT_EQ(std::string("data0000"), data);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, AbortedRemoveN) {
    test_env->empty_logdata_dir();
    restart_aborted_remove context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, AbortedRemoveC) {
    test_env->empty_logdata_dir();
    restart_aborted_remove context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Multi-thread test cases from here on */

/* Test case with 2 threads, 1 committed transaction each, no checkpoints, normal shutdown */
class restart_multithrd_basic : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->btree_insert_and_commit(stid_list[0], "aa1", "data1");
    }

    static void t2Run(StoreID* stid_list) {
        test_env->btree_insert_and_commit(stid_list[0], "aa2", "data2");
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        output_durable_lsn(3);

        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t1.join());
        W_DO(t2.join());

        EXPECT_TRUE(t1._finished);
        EXPECT_TRUE(t2._finished);
        return RCOK;
    }


    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (2, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa2"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdBasicN) {
    test_env->empty_logdata_dir();
    restart_multithrd_basic context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultithrdBasicC) {
    test_env->empty_logdata_dir();
    restart_multithrd_basic context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


/* Test case with 3 threads, 1 insert&commit, 1 abort, 1 in-flight */
class restart_multithrd_inflight1 : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->btree_insert_and_commit(stid_list[0], "aa1", "data1");
    }
    static void t2Run(StoreID* stid_list) {
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa2", "data2");
        test_env->abort_xct();
    }
    static void t3Run(StoreID* stid_list) {
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa3", "data3");
        ss_m::detach_xct();
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        transact_thread_t t3 (_stid_list, t3Run);
        output_durable_lsn(3);
        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t3.fork());
        W_DO(t1.join());
        W_DO(t2.join());
        W_DO(t3.join());
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ(1, s.rownum);
        EXPECT_EQ(std::string("aa1"), s.minkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdInflight1N) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight1 context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultithrdInflight1C) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight1 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


/* Test case with 3 threads, 1 insert&commit, 1 aborted, 1 with two inserts, one update and commit*/
class restart_multithrd_abort1 : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->btree_insert_and_commit(stid_list[0], "aa1", "data1");
    }
    static void t2Run(StoreID* stid_list) {
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa2", "data2");
        test_env->abort_xct();
    }
    static void t3Run(StoreID* stid_list) {
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa3", "data3");
        test_env->btree_insert(stid_list[0], "aa4", "data4");
        test_env->btree_update(stid_list[0], "aa3", "data33");
        test_env->commit_xct();
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        transact_thread_t t3 (_stid_list, t3Run);
        output_durable_lsn(3);
        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t3.fork());
        W_DO(t1.join());
        W_DO(t2.join());
        W_DO(t3.join());
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ(3, s.rownum);
        EXPECT_EQ(std::string("aa1"), s.minkey);
        EXPECT_EQ(std::string("aa4"), s.maxkey);
        std::string data;
        test_env->btree_lookup_and_commit(_stid_list[0], "aa3", data);
        EXPECT_EQ(std::string("data33"), data);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdAbort1N) {
    test_env->empty_logdata_dir();
    restart_multithrd_abort1 context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


/* Passing */
TEST (RestartTest, MultithrdAbort1C) {
    test_env->empty_logdata_dir();
    restart_multithrd_abort1 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


/* Test case with 3 threads, 1 insert&commit, 1 aborted, 1 with two inserts, one update and commit*/
class restart_multithrd_chckp : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->btree_insert_and_commit(stid_list[0], "aa1", "data1");
        test_env->btree_insert_and_commit(stid_list[0], "aa3", "data3");
        test_env->btree_insert_and_commit(stid_list[0], "aa2", "data2");
        test_env->btree_insert_and_commit(stid_list[0], "aa4", "data4");
    }
    static void t2Run(StoreID* stid_list) {
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa5", "data5");
        test_env->btree_insert(stid_list[0], "aa0", "data0");
        test_env->btree_update(stid_list[0], "aa5", "data55");
        ss_m::checkpoint();
        test_env->btree_update(stid_list[0], "aa5", "data555");
        test_env->abort_xct();
        test_env->btree_insert_and_commit(stid_list[0], "aa5", "data5555");
    }
    static void t3Run(StoreID* stid_list) {
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa6", "data6");
        test_env->btree_insert(stid_list[0], "aa7", "data7");
        test_env->btree_update(stid_list[0], "aa6", "data66");
        test_env->commit_xct();
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        transact_thread_t t3 (_stid_list, t3Run);
        output_durable_lsn(3);
        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t3.fork());
        W_DO(t1.join());
        W_DO(t2.join());
        W_DO(t3.join());
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ(7, s.rownum);
        EXPECT_EQ(std::string("aa1"), s.minkey);
        EXPECT_EQ(std::string("aa7"), s.maxkey);
        std::string data;
        test_env->btree_lookup_and_commit(_stid_list[0], "aa5", data);
        EXPECT_EQ(std::string("data5555"), data);
        data = "";
        test_env->btree_lookup_and_commit(_stid_list[0], "aa6", data);
        EXPECT_EQ(std::string("data66"), data);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdCheckpN) {
    test_env->empty_logdata_dir();
    restart_multithrd_chckp context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultithrdCheckpC) {
    test_env->empty_logdata_dir();
    restart_multithrd_chckp context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Test case with 3 threads:
 * t1:  2 committed inserts, one inflight update
 * t2:  1 aborted insert, 1 committed insert, 1 inflight remove
 * t3:  1 inflight transaction with several inserts and one update
 */
class restart_multithrd_inflight2 : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa0", "data0");
        test_env->btree_insert(stid_list[0], "aa2", "data2");
        test_env->commit_xct();
        test_env->begin_xct();
        test_env->btree_update(stid_list[0], "aa0", "data00");
        test_env->btree_update(stid_list[0], "aa1", "data1");
        ss_m::detach_xct();
    }
    static void t2Run(StoreID* stid_list) {
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa3", "data3");
        test_env->abort_xct();
        test_env->btree_insert_and_commit(stid_list[0], "aa4", "data4");
        test_env->begin_xct();
        test_env->btree_remove(stid_list[0], "aa4");
        ss_m::detach_xct();
    }
    static void t3Run(StoreID* stid_list) {
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa5", "data5");
        test_env->btree_insert(stid_list[0], "aa6", "data6");
        test_env->btree_insert(stid_list[0], "aa7", "data7");
        test_env->btree_insert(stid_list[0], "aa8", "data8");
        test_env->btree_insert(stid_list[0], "aa9", "data9");
        test_env->btree_update(stid_list[0], "aa7", "data77");
        test_env->btree_insert(stid_list[0], "aa10", "data10");
        ss_m::detach_xct();
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        transact_thread_t t3 (_stid_list, t3Run);
        output_durable_lsn(3);
        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t3.fork());
        W_DO(t1.join());
        W_DO(t2.join());
        W_DO(t3.join());
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ(3, s.rownum);
        EXPECT_EQ(std::string("aa0"), s.minkey);
        EXPECT_EQ(std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Normal Shutdown scenario for restart_multithrd_inflight2 -- Passing */
TEST (RestartTest, MultithrdInflightN) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight2 context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Crash shutdown scenario for restart_multithrd_inflight2 */
/* Passing */
TEST (RestartTest, MultithrdInflightC) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight2 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Test case with 3 threads:
 * t1:  1 committed trans w/ 2 inserts, 1 aborted trans w/ 1 update & 1 remove
 * t2:  1 committed trans w/ 2 inserts, 1 aborted trans w/ 1 remove & 1 update, 1 aborted trans w/ 1 update & 1 remove
 * t3:  1 committed trans w/ 2 inserts, 1 aborted trans w/ 2 updates & 1 insert
 */
class restart_multithrd_abort2 : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa0", "data0");
        test_env->btree_insert(stid_list[0], "aa1", "data1");
        test_env->commit_xct();
        test_env->begin_xct();
        test_env->btree_update(stid_list[0], "aa0", "data00");
        test_env->btree_remove(stid_list[0], "aa1");
        test_env->abort_xct();
    }
    static void t2Run(StoreID* stid_list) {
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa2", "data2");
        test_env->btree_insert(stid_list[0], "aa3", "data3");
        test_env->commit_xct();
        test_env->begin_xct();
        test_env->btree_remove(stid_list[0], "aa2");
        test_env->btree_update(stid_list[0], "aa3", "data33");
        test_env->abort_xct();
        test_env->begin_xct();
        test_env->btree_update(stid_list[0], "aa2", "data22");
        test_env->btree_remove(stid_list[0], "aa3");
        test_env->abort_xct();
    }
    static void t3Run(StoreID* stid_list) {
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa4", "data4");
        test_env->btree_insert(stid_list[0], "aa5", "data5");
        test_env->commit_xct();
        test_env->begin_xct();
        test_env->btree_update(stid_list[0], "aa4", "data44");
        test_env->btree_update(stid_list[0], "aa5", "data55");
        test_env->btree_insert(stid_list[0], "aa6", "data6");
        test_env->abort_xct();
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        transact_thread_t t3 (_stid_list, t3Run);
        output_durable_lsn(3);
        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t3.fork());
        W_DO(t1.join());
        W_DO(t2.join());
        W_DO(t3.join());
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ(6, s.rownum);
        EXPECT_EQ(std::string("aa0"), s.minkey);
        EXPECT_EQ(std::string("aa5"), s.maxkey);
        std::string data;
        test_env->btree_lookup_and_commit(_stid_list[0], "aa0", data);
        EXPECT_EQ(std::string("data0"), data);
        data = "";
        test_env->btree_lookup_and_commit(_stid_list[0], "aa1", data);
        EXPECT_EQ(std::string("data1"), data);
        data = "";
        test_env->btree_lookup_and_commit(_stid_list[0], "aa2", data);
        EXPECT_EQ(std::string("data2"), data);
        data = "";
        test_env->btree_lookup_and_commit(_stid_list[0], "aa3", data);
        EXPECT_EQ(std::string("data3"), data);
        data = "";
        test_env->btree_lookup_and_commit(_stid_list[0], "aa4", data);
        EXPECT_EQ(std::string("data4"), data);
        data = "";
        test_env->btree_lookup_and_commit(_stid_list[0], "aa5", data);
        EXPECT_EQ(std::string("data5"), data);
        return RCOK;
    }
};

/* Normal Shutdown scenario for restart_multithrd_abort2 -- Passing */
TEST (RestartTest, MultithrdAbort2N) {
    test_env->empty_logdata_dir();
    restart_multithrd_abort2 context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Crash Shutdown scenario for restart_multithrd_abort2 */
/* Passing */
TEST (RestartTest, MultithrdAbort2C) {
    test_env->empty_logdata_dir();
    restart_multithrd_abort2 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Test case with 3 threads:
 * t1:  1 committed insert, 1 in-flight trans with an update, insert, remove
 * t2:  1 committed insert, 1 in-flight trans with an insert, update, insert, update, remove
 * t3:  3 committed inserts, 1 in-flight trans with a remove, update, remove, update, insert
 */
class restart_multithrd_inflight3 : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->btree_insert_and_commit(stid_list[0], "aa0", "data0");
        test_env->begin_xct();
        test_env->btree_update(stid_list[0], "aa0", "data00");
        test_env->btree_insert(stid_list[0], "aa1", "data1");
        test_env->btree_remove(stid_list[0], "aa0");
        ss_m::detach_xct();
    }
    static void t2Run(StoreID* stid_list) {
        test_env->btree_insert_and_commit(stid_list[0], "aa2", "data2");
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa3", "data3");
        test_env->btree_update(stid_list[0], "aa3", "data33");
        test_env->btree_insert(stid_list[0], "aa4", "data4");
        test_env->btree_update(stid_list[0], "aa2", "data2");
        test_env->btree_remove(stid_list[0], "aa2");
        ss_m::detach_xct();
    }
    static void t3Run(StoreID* stid_list) {
        test_env->btree_insert_and_commit(stid_list[0], "aa5", "data5");
        test_env->btree_insert_and_commit(stid_list[0], "aa6", "data6");
        test_env->btree_insert_and_commit(stid_list[0], "aa7", "data7");
        test_env->begin_xct();
        test_env->btree_remove(stid_list[0], "aa5");
        test_env->btree_update(stid_list[0], "aa6", "data66");
        test_env->btree_remove(stid_list[0], "aa7");
        test_env->btree_update(stid_list[0], "aa6", "data666");
        test_env->btree_insert(stid_list[0], "aa5", "data55");
        ss_m::detach_xct();
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        transact_thread_t t3 (_stid_list, t3Run);
        output_durable_lsn(3);
        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t3.fork());
        W_DO(t1.join());
        W_DO(t2.join());
        W_DO(t3.join());
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ(5, s.rownum);
        EXPECT_EQ(std::string("aa0"), s.minkey);
        EXPECT_EQ(std::string("aa7"), s.maxkey);
        std::string data;
        test_env->btree_lookup_and_commit(_stid_list[0], "aa0", data);
        EXPECT_EQ(std::string("data0"), data);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdInflight3N) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight3 context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultithrdInflight3C) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight3 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Same test code as restart_multithrd_inflight3 above, with 2 checkpoints in between */
class restart_multithrd_inflight_chckp1 : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->btree_insert_and_commit(stid_list[0], "aa0", "data0");
        test_env->begin_xct();
        test_env->btree_update(stid_list[0], "aa0", "data00");
        test_env->btree_insert(stid_list[0], "aa1", "data1");
        ss_m::checkpoint();
        test_env->btree_remove(stid_list[0], "aa0");
        ss_m::detach_xct();
    }
    static void t2Run(StoreID* stid_list) {
        test_env->btree_insert_and_commit(stid_list[0], "aa2", "data2");
        test_env->begin_xct();
        test_env->btree_insert(stid_list[0], "aa3", "data3");
        test_env->btree_update(stid_list[0], "aa3", "data33");
        test_env->btree_insert(stid_list[0], "aa4", "data4");
        test_env->btree_update(stid_list[0], "aa2", "data2");
        test_env->btree_remove(stid_list[0], "aa2");
        ss_m::detach_xct();
    }
    static void t3Run(StoreID* stid_list) {
        test_env->btree_insert_and_commit(stid_list[0], "aa5", "data5");
        test_env->btree_insert_and_commit(stid_list[0], "aa6", "data6");
        test_env->btree_insert_and_commit(stid_list[0], "aa7", "data7");
        test_env->begin_xct();
        test_env->btree_remove(stid_list[0], "aa5");
        test_env->btree_update(stid_list[0], "aa6", "data66");
        test_env->btree_remove(stid_list[0], "aa7");
        ss_m::checkpoint();
        test_env->btree_update(stid_list[0], "aa6", "data666");
        test_env->btree_insert(stid_list[0], "aa5", "data55");
        ss_m::detach_xct();
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        transact_thread_t t3 (_stid_list, t3Run);
        output_durable_lsn(3);
        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t3.fork());
        W_DO(t1.join());
        W_DO(t2.join());
        W_DO(t3.join());
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ(5, s.rownum);
        EXPECT_EQ(std::string("aa0"), s.minkey);
        EXPECT_EQ(std::string("aa7"), s.maxkey);
        std::string data;
        test_env->btree_lookup_and_commit(_stid_list[0], "aa0", data);
        EXPECT_EQ(std::string("data0"), data);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdInflightChckp1N) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight_chckp1 context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultithrdInflightChckp1C) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight_chckp1 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/// Test case with 3 threads, each with a committed transaction containing a large amount of inserts, no checkpoints
class restart_multithrd_ldata1 : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], false, t_test_txn_commit, false, '1');   // No checkpoint, commit, one big transaction, keyPrefix '1'
    }
    static void t2Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], false, t_test_txn_commit, false, '2');   //                                             keyPrefix '2'
    }
    static void t3Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], false, t_test_txn_commit, false, '3');   //                                             keyPrefix '3'
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        transact_thread_t t3 (_stid_list, t3Run);
        output_durable_lsn(3);
        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t3.fork());
        W_DO(t1.join());
        W_DO(t2.join());
        W_DO(t3.join());
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 15;
        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ(std::string("key100"), s.minkey);
        EXPECT_EQ(getMaxKeyString('3'), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdLData1N) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata1 context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}

/* Passing */
TEST (RestartTest, MultithrdLData1C) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata1 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/// Test case with 3 threads, each with a committed transaction containing a large amount of inserts, 2 checkpoints
class restart_multithrd_ldata2 : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], true, t_test_txn_commit, false, '1');    // with checkpoint, commit, one big transaction, keyPrefix '1'
    }
    static void t2Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], false, t_test_txn_commit, false, '2');   // without checkpoint                            keyPrefix '2'
    }
    static void t3Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], true, t_test_txn_commit, false, '3');    // with checkpoint                               keyPrefix '3'
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        transact_thread_t t3 (_stid_list, t3Run);
        output_durable_lsn(3);
        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t3.fork());
        W_DO(t1.join());
        W_DO(t2.join());
        W_DO(t3.join());
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 15;
        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ(std::string("key100"), s.minkey);
        EXPECT_EQ(getMaxKeyString('3'), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdLData2N) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata2 context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}

/* Passing */
TEST (RestartTest, MultithrdLData2C) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata2 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


/// Test case with 3 threads, each with a transaction containing a large amount of inserts, 2 inflight, 1 committed, no checkpoints
class restart_multithrd_ldata3 : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], false, t_test_txn_in_flight, false, '1'); // without checkpoint, don't commit, one big transaction, keyPrefix '1'
    }
    static void t2Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], false, t_test_txn_commit, false, '2');  //                     commit                             keyPrefix '2'
    }
    static void t3Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], false, t_test_txn_in_flight, false, '3'); //                     don't commit                       keyPrefix '3'
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        transact_thread_t t3 (_stid_list, t3Run);
        output_durable_lsn(3);
        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t3.fork());
        W_DO(t1.join());
        W_DO(t2.join());
        W_DO(t3.join());
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ(std::string("key200"), s.minkey);
        EXPECT_EQ(getMaxKeyString('2'), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdLData3N) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata3 context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}

/* Passing */
TEST (RestartTest, MultithrdLData3C) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata3 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

// Test case with 3 threads, each with a transaction containing a large amount of inserts, 2 inflight, 1 committed, 2 checkpoints
class restart_multithrd_ldata4 : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], true, t_test_txn_in_flight, false, '1');   // with checkpoint, don't commit, one big transaction, keyPrefix '1'
    }
    static void t2Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], true, t_test_txn_commit, false, '2');    //                  commit                             keyPrefix '2'
    }
    static void t3Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], false, t_test_txn_in_flight, false, '3');  // without checkpoint                                  keyPrefix '3'
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        transact_thread_t t3 (_stid_list, t3Run);
        output_durable_lsn(3);
        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t3.fork());
        W_DO(t1.join());
        W_DO(t2.join());
        W_DO(t3.join());
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ(std::string("key200"), s.minkey);
        EXPECT_EQ(getMaxKeyString('2'), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdLData4N) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata4 context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}

/* Passing */
TEST (RestartTest, MultithrdLData4C) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata4 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

// Test case with 3 threads, each with a transaction containing a large amount of inserts, 2 inflight, 1 committed, 2 checkpoints
class restart_multithrd_ldata5 : public restart_test_base
{
public:
    static void t1Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], true, t_test_txn_in_flight, false, '1'); // w/ checkpoint, don't commit, keyPrefix '1'
    }
    static void t2Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], true, t_test_txn_commit, false, '2');  //                commit        keyPrefix '2'
        test_env->delete_records(stid_list[0], false, t_test_txn_commit, '2');  // Delete half of those, w/o checkpoint, commit, keyPrefix '2'
    }
    static void t3Run(StoreID* stid_list) {
        test_env->btree_populate_records(stid_list[0], false, t_test_txn_commit, false, '3'); // w/o checkpoint, commit, keyPrefix '3'
        test_env->delete_records(stid_list[0], false, t_test_txn_in_flight, '3');    // Delete half of those, w/o checkpoint, don't commit, keyPrefix '3'
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new StoreID[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid_list, t1Run);
        transact_thread_t t2 (_stid_list, t2Run);
        transact_thread_t t3 (_stid_list, t3Run);
        output_durable_lsn(3);
        W_DO(t1.fork());
        W_DO(t2.fork());
        W_DO(t3.fork());
        W_DO(t1.join());
        W_DO(t2.join());
        W_DO(t3.join());
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5; // Inserts from t3 (not deleted)
        recordCount += ((SM_PAGESIZE / btree_m::max_entry_size()) * 5) / 2;  // Inserts from t2 (half deleted)
        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ(std::string("key201"), s.minkey);
        EXPECT_EQ(getMaxKeyString('3'), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdLData5N) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata5 context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultithrdLData5C) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata5 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
