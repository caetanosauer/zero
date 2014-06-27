#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"
#include "xct.h"

btree_test_env *test_env;

// Test cases to test serial and traditional restart.

lsn_t get_durable_lsn() {
    lsn_t ret;
    ss_m::get_durable_lsn(ret);
    return ret;
}
void output_durable_lsn(int W_IFDEBUG1(num)) {
    DBGOUT1( << num << ".durable LSN=" << get_durable_lsn());
}

w_rc_t populate_records(ss_m *ssm, stid_t &stid, bool fCheckPoint) {
    // Set the data size is the max_entry_size minus key size
    // because the total size must be smaller than or equal to
    // btree_m::max_entry_size()
    const int key_size = 5;
    const int data_size = btree_m::max_entry_size() - key_size;

    vec_t data;
    char data_str[data_size];
    memset(data_str, '\0', data_size);
    data.set(data_str, data_size);
    w_keystr_t key;
    char key_str[key_size];
    key_str[0] = 'k';
    key_str[1] = 'e';
    key_str[2] = 'y';

    // Insert enough records to ensure page split
    // One big transaction with multiple insertions
    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
    for (int i = 0; i < recordCount; ++i) {
        int num;
        num = recordCount - 1 - i;

        key_str[3] = ('0' + ((num / 10) % 10));
        key_str[4] = ('0' + (num % 10));
        key.construct_regularkey(key_str, key_size);

        if (true == fCheckPoint) {
            // Take one checkpoint half way through insertions
            if (num == recordCount/2)
                W_DO(ss_m::checkpoint()); 
        }
        W_DO(test_env->begin_xct());
        W_DO(ssm->create_assoc(stid, key, data));
        W_DO(test_env->commit_xct());
    }

    return RCOK;
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
TEST (RestartTest, Empty) {
    test_env->empty_logdata_dir();
    restart_empty context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);  // false = no simulated crash, normal shutdown
                                                                  // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case without checkpoint and normal shutdown
class restart_normal_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (4, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, NormalShutdown) {
    test_env->empty_logdata_dir();
    restart_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);  // false = no simulated crash, normal shutdown
                                                                  // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with checkpoint and normal shutdown
class restart_checkpoint_normal_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(ss_m::checkpoint());
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));
        // If enabled the 2nd checkpoint, it is passing also
        // W_DO(ss_m::checkpoint());
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (4, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, NormalCheckpointShutdown) {
    test_env->empty_logdata_dir();
    restart_checkpoint_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);  // false = no simulated crash, normal shutdown
                                                                  // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with more than one page of data, without checkpoint and normal shutdown
class restart_many_normal_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        W_DO(populate_records(ssm, _stid, false));  // No checkpoint
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, NormalManyShutdown) {
    test_env->empty_logdata_dir();
    restart_many_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);  // false = no simulated crash, normal shutdown
                                                                  // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with more than one page of data, with checkpoint and normal shutdown
class restart_many_checkpoint_normal_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        W_DO(populate_records(ssm, _stid, true));  // Checkpoint

        // If enabled the 2nd checkpoint, it is passing also
        // W_DO(ss_m::checkpoint()); 

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, NormalManyCheckpointShutdown) {
    test_env->empty_logdata_dir();
    restart_many_checkpoint_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);  // false = no simulated crash, normal shutdown
                                                                  // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with an uncommitted transaction, no checkpoint, normal shutdown
class restart_inflight_normal_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));

        // Start a transaction but no commit before normal shutdown
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, InflightNormalShutdown) {
    test_env->empty_logdata_dir();
    restart_inflight_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);  // false = no simulated crash, normal shutdown
                                                                  // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with an uncommitted transaction, checkpoint, normal shutdown
class restart_inflight_checkpoint_normal_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(ss_m::checkpoint()); 

        // Start a transaction but no commit, checkpoint, and then normal shutdown
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        W_DO(ss_m::checkpoint()); 
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, InflightcheckpointNormalShutdown) {
    test_env->empty_logdata_dir();
    restart_inflight_checkpoint_normal_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);  // false = no simulated crash, normal shutdown
                                                                  // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with an uncommitted transaction, no checkpoint, simulated crash shutdown
class restart_inflight_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));

        // Start a transaction but no commit, normal shutdown
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, InflightCrashShutdown) {
    test_env->empty_logdata_dir();
    restart_inflight_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);  // true = simulated crash
                                                                 // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with an uncommitted transaction, checkpoint, simulated crash shutdown
class restart_inflight_checkpoint_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(ss_m::checkpoint()); 

        // Start a transaction but no commit, checkpoint, and then normal shutdown
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        W_DO(ss_m::checkpoint()); 
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

// In this test case, the user checkpoint was aborted due to crash shutdown

/* Passing */
TEST (RestartTest, InflightCheckpointCrashShutdown) {
    test_env->empty_logdata_dir();
    restart_inflight_checkpoint_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);  // true = simulated crash
                                                                 // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with an uncommitted transaction, more than one page of data, no checkpoint, simulated crash shutdown
class restart_inflight_many_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        W_DO(populate_records(ssm, _stid, false));  // No checkpoint
        output_durable_lsn(3);

        // In-flight transaction, no commit
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa1", "data1"));
        
        output_durable_lsn(4);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(5);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, InflightManyCrashShutdown) {
    test_env->empty_logdata_dir();
    restart_inflight_many_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);  // true = simulated crash
                                                                 // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with an uncommitted transaction, more than one page of data, checkpoint, simulated crash shutdown
class restart_inflight_ckpt_many_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        W_DO(populate_records(ssm, _stid, true));  // Checkpoint
        output_durable_lsn(3);

        // 2nd checkpoint before the in-flight transaction
        // W_DO(ss_m::checkpoint());

        // In-flight transaction, no commit
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa1", "data1"));
        
        output_durable_lsn(4);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(5);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, InflightCkptManyCrashShutdown) {
    test_env->empty_logdata_dir();
    restart_inflight_ckpt_many_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);  // true = simulated crash
                                                                 // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with committed transactions,  no checkpoint, simulated crash shutdown
class restart_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa5", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data1"));
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (5, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa5"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, CrashShutdown) {
    test_env->empty_logdata_dir();
    restart_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);  // true = simulated crash
                                                                 // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with committed transactions,  checkpoint, simulated crash shutdown
class restart_checkpoint_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data1"));
        W_DO(ss_m::checkpoint());
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa5", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data1"));
        // If enabled the 2nd checkpoint...
        // W_DO(ss_m::checkpoint());
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (5, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa5"), s.maxkey);
        return RCOK;
    }
};

/* Passing  */
TEST (RestartTest, CheckpointCrashShutdown) {
    test_env->empty_logdata_dir();
    restart_checkpoint_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);  // true = simulated crash
                                                                 // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with committed transactions,  more than one page of data, no checkpoint, simulated crash shutdown
class restart_many_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        W_DO(populate_records(ssm, _stid, false));  // No checkpoint
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, ManyCrashShutdown) {
    test_env->empty_logdata_dir();
    restart_many_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);  // true = simulated crash
                                                                 // 10 = recovery mode, m1 default serial mode
}
/**/

// Test case with committed transactions,  more than one page of data, checkpoint, simulated crash shutdown
class restart_many_ckpt_crash_shutdown : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        W_DO(populate_records(ssm, _stid, true));  // Checkpoint
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, ManyCkptCrashShutdown) {
    test_env->empty_logdata_dir();
    restart_many_ckpt_crash_shutdown context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);  // true = simulated crash
                                                                 // 10 = recovery mode, m1 default serial mode
}
/**/

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
