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

/* Multi-thread test cases from here on */

/* Test case with 2 threads, 1 committed transaction each, no checkpoints, normal shutdown */
class restart_multithrd_basic : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
        test_env->btree_insert_and_commit(pstid, "aa1", "data1");
    }   

    static void t2Run(stid_t pstid) {
        test_env->btree_insert_and_commit(pstid, "aa2", "data2");
    }   

    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid, t1Run);
        transact_thread_t t2 (_stid, t2Run);
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
        W_DO(test_env->btree_scan(_stid, s));
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
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0); 
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
}
/**/

/* Passing */
TEST (RestartTest, MultithrdBasicC) {
    test_env->empty_logdata_dir();
    restart_multithrd_basic context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0); 
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
/**/


/* Test case with 3 threads, 1 insert&commit, 1 abort, 1 in-flight */
class restart_multithrd_inflight1 : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
	test_env->btree_insert_and_commit(pstid, "aa1", "data1");
    }
    static void t2Run(stid_t pstid) {
	test_env->begin_xct();
	test_env->btree_insert(pstid, "aa2", "data2");
	test_env->abort_xct();
    }
    static void t3Run(stid_t pstid) {
	test_env->begin_xct();

	test_env->btree_insert(pstid, "aa3", "data3");

	ss_m::detach_xct();
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
	output_durable_lsn(1);
	W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
	output_durable_lsn(2);
	transact_thread_t t1 (_stid, t1Run);
	transact_thread_t t2 (_stid, t2Run);
	transact_thread_t t3 (_stid, t3Run);
	output_durable_lsn(3);
	W_DO(t1.fork());
	W_DO(t2.fork());
	W_DO(t3.fork());
	W_DO(t1.join());
	W_DO(t2.join());
	return RCOK;
    }
    
    w_rc_t post_shutdown(ss_m *) {
	output_durable_lsn(4);
	x_btree_scan_result s;
	W_DO(test_env->btree_scan(_stid, s));
	EXPECT_EQ(1, s.rownum);
	EXPECT_EQ(std::string("aa1"), s.minkey);
	return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdInflight1N) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight1 context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0); 
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
}
/**/

/* Passing */ 
TEST (RestartTest, MultithrdInflight1C) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight1 context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0); 
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
/**/


/* Test case with 3 threads, 1 insert&commit, 1 aborted, 1 with two inserts, one update and commit*/
class restart_multithrd_abort1 : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
	test_env->btree_insert_and_commit(pstid, "aa1", "data1");
    }
    static void t2Run(stid_t pstid) {
	test_env->begin_xct();
	test_env->btree_insert(pstid, "aa2", "data2");
	test_env->abort_xct();
    }
    static void t3Run(stid_t pstid) {
	test_env->begin_xct();
	test_env->btree_insert(pstid, "aa3", "data3");
	test_env->btree_insert(pstid, "aa4", "data4");
	test_env->btree_update(pstid, "aa3", "data33");
	test_env->commit_xct();
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
	output_durable_lsn(1);
	W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
	output_durable_lsn(2);
	transact_thread_t t1 (_stid, t1Run);
	transact_thread_t t2 (_stid, t2Run);
	transact_thread_t t3 (_stid, t3Run);
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
	W_DO(test_env->btree_scan(_stid, s));
	EXPECT_EQ(3, s.rownum);
	EXPECT_EQ(std::string("aa1"), s.minkey);
	EXPECT_EQ(std::string("aa4"), s.maxkey);
	std::string data;
	test_env->btree_lookup_and_commit(_stid, "aa3", data);
	EXPECT_EQ(std::string("data33"), data);
	return RCOK;
    }
};

/* Passing */ 
TEST (RestartTest, MultithrdAbort1N) {
    test_env->empty_logdata_dir();
    restart_multithrd_abort1 context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0); 
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
}
/**/


/* Passing */ 
TEST (RestartTest, MultithrdAbort1C) {
    test_env->empty_logdata_dir();
    restart_multithrd_abort1 context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0); 
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
/**/


/* Test case with 3 threads, 1 insert&commit, 1 aborted, 1 with two inserts, one update and commit*/
class restart_multithrd_chckp : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
	test_env->btree_insert_and_commit(pstid, "aa1", "data1");
	test_env->btree_insert_and_commit(pstid, "aa3", "data3");
	test_env->btree_insert_and_commit(pstid, "aa2", "data2");
	test_env->btree_insert_and_commit(pstid, "aa4", "data4");
    }
    static void t2Run(stid_t pstid) {
	test_env->begin_xct();
	test_env->btree_insert(pstid, "aa5", "data5");
	test_env->btree_insert(pstid, "aa0", "data0");
	test_env->btree_update(pstid, "aa5", "data55");
	ss_m::checkpoint();
	test_env->btree_update(pstid, "aa5", "data555");
	test_env->abort_xct();
	test_env->btree_insert_and_commit(pstid, "aa5", "data5555");
    }
    static void t3Run(stid_t pstid) {
	test_env->begin_xct();
	test_env->btree_insert(pstid, "aa6", "data6");
	test_env->btree_insert(pstid, "aa7", "data7");
	test_env->btree_update(pstid, "aa6", "data66");
	test_env->commit_xct();
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
	output_durable_lsn(1);
	W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
	output_durable_lsn(2);
	transact_thread_t t1 (_stid, t1Run);
	transact_thread_t t2 (_stid, t2Run);
	transact_thread_t t3 (_stid, t3Run);
	output_durable_lsn(3);
	W_DO(t1.fork());
	W_DO(t2.fork());
	W_DO(t3.fork());
	W_DO(t1.join());
	W_DO(t2.join());
	return RCOK;
    }
    
    w_rc_t post_shutdown(ss_m *) {
	output_durable_lsn(4);
	x_btree_scan_result s;
	W_DO(test_env->btree_scan(_stid, s));
	EXPECT_EQ(7, s.rownum);
	EXPECT_EQ(std::string("aa1"), s.minkey);
	EXPECT_EQ(std::string("aa7"), s.maxkey);
	std::string data;
	test_env->btree_lookup_and_commit(_stid, "aa5", data);
	EXPECT_EQ(std::string("data5555"), data);
	data = "";
	test_env->btree_lookup_and_commit(_stid, "aa6", data);
	EXPECT_EQ(std::string("data66"), data);
	return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdCheckpN) {
    test_env->empty_logdata_dir();
    restart_multithrd_chckp context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0); 
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
}
/**/

/* Passing */  
TEST (RestartTest, MultithrdCheckpC) {
    test_env->empty_logdata_dir();
    restart_multithrd_chckp context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
/**/

/* Test case with 3 threads:
 * t1:	2 committed inserts, one inflight update
 * t2:	1 aborted insert, 1 committed insert, 1 inflight remove
 * t3:	1 inflight transaction with several inserts and one update
 */
class restart_multithrd_inflight2 : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
	test_env->begin_xct();
	test_env->btree_insert(pstid, "aa0", "data0");	
	test_env->btree_insert(pstid, "aa2", "data2");
	test_env->commit_xct();
	test_env->begin_xct();
	test_env->btree_update(pstid, "aa0", "data00");
	//test_env->btree_update(pstid, "aa1", "data1");
	ss_m::detach_xct();
    }
    static void t2Run(stid_t pstid) {
	test_env->begin_xct();
	test_env->btree_insert(pstid, "aa3", "data3");
	test_env->abort_xct();
	test_env->btree_insert_and_commit(pstid, "aa4", "data4");
	test_env->begin_xct();
	test_env->btree_remove(pstid, "aa4");
	ss_m::detach_xct();
    }
    static void t3Run(stid_t pstid) {
	test_env->begin_xct();
	test_env->btree_insert(pstid, "aa5", "data5");	
	test_env->btree_insert(pstid, "aa6", "data6");	
	test_env->btree_insert(pstid, "aa7", "data7");	
	test_env->btree_insert(pstid, "aa8", "data8");	
	test_env->btree_insert(pstid, "aa9", "data9");
	test_env->btree_update(pstid, "aa7", "data77");
	test_env->btree_insert(pstid, "aa10", "data10");
	ss_m::detach_xct();
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
	output_durable_lsn(1);
	W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
	output_durable_lsn(2);
	transact_thread_t t1 (_stid, t1Run);
	transact_thread_t t2 (_stid, t2Run);
	transact_thread_t t3 (_stid, t3Run);
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
	W_DO(test_env->btree_scan(_stid, s));
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
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
}
/**/

/* Crash shutdown scenario for restart_multithrd_inflight2 -- Failing
 * Failing due to issue ZERO-183 (see test_restart_bugs)
TEST (RestartTest, MultithrdInflightC) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight2 context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
*/

/* Test case with 3 threads:
 * t1:	1 committed trans w/ 2 inserts, 1 aborted trans w/ 1 update & 1 remove
 * t2:	1 committed trans w/ 2 inserts, 1 aborted trans w/ 1 remove & 1 update, 1 aborted trans w/ 1 update & 1 remove
 * t3:	1 committed trans w/ 2 inserts, 1 aborted trans w/ 2 updates & 1 insert
 */
class restart_multithrd_abort2 : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
        test_env->begin_xct();
	test_env->btree_insert(pstid, "aa0", "data0");
	test_env->btree_insert(pstid, "aa1", "data1");
	test_env->commit_xct();
	test_env->begin_xct();
	test_env->btree_update(pstid, "aa0", "data00");
	test_env->btree_remove(pstid, "aa1");
	test_env->abort_xct();
    }   
    static void t2Run(stid_t pstid) {
        test_env->begin_xct();
	test_env->btree_insert(pstid, "aa2", "data2");
	test_env->btree_insert(pstid, "aa3", "data3");
	test_env->commit_xct();
	test_env->begin_xct();
	test_env->btree_remove(pstid, "aa2");
	test_env->btree_update(pstid, "aa3", "data33");
	test_env->abort_xct();
	test_env->begin_xct();
	test_env->btree_update(pstid, "aa2", "data22");
	test_env->btree_remove(pstid, "aa3");
	test_env->abort_xct();
    }   
    static void t3Run(stid_t pstid) {
        test_env->begin_xct();
	test_env->btree_insert(pstid, "aa4", "data4");
	test_env->btree_insert(pstid, "aa5", "data5");
	test_env->commit_xct();
	test_env->begin_xct();
	test_env->btree_update(pstid, "aa4", "data44");
	test_env->btree_update(pstid, "aa5", "data55");
	test_env->btree_insert(pstid, "aa6", "data6");
	test_env->abort_xct();
    }   

    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        transact_thread_t t1 (_stid, t1Run);
        transact_thread_t t2 (_stid, t2Run);
        transact_thread_t t3 (_stid, t3Run);
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
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ(6, s.rownum);
        EXPECT_EQ(std::string("aa0"), s.minkey);
        EXPECT_EQ(std::string("aa5"), s.maxkey);
	std::string data;
        test_env->btree_lookup_and_commit(_stid, "aa0", data);
        EXPECT_EQ(std::string("data0"), data);
	data = "";
        test_env->btree_lookup_and_commit(_stid, "aa1", data);
        EXPECT_EQ(std::string("data1"), data);
	data = "";
        test_env->btree_lookup_and_commit(_stid, "aa2", data);
        EXPECT_EQ(std::string("data2"), data);
	data = "";
        test_env->btree_lookup_and_commit(_stid, "aa3", data);
        EXPECT_EQ(std::string("data3"), data);
	data = "";
        test_env->btree_lookup_and_commit(_stid, "aa4", data);
        EXPECT_EQ(std::string("data4"), data);
	data = "";
        test_env->btree_lookup_and_commit(_stid, "aa5", data);
        EXPECT_EQ(std::string("data5"), data);
        return RCOK;
    }
};

/* Normal Shutdown scenario for restart_multithrd_abort2 -- Passing */
TEST (RestartTest, MultithrdAbort2N) {
    test_env->empty_logdata_dir();
    restart_multithrd_abort2 context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
    }
/**/

/* Crash Shutdown scenario for restart_multithrd_abort2 -- Failing
 * Failing due to issue ZERO-183 (see test_restart_bugs)
TEST (RestartTest, MultithrdAbort2C) {
    test_env->empty_logdata_dir();
    restart_multithrd_abort2 context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
*/

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
