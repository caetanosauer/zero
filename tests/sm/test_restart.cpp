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

w_rc_t populate_records(stid_t &stid, bool fCheckPoint, bool fInflight, char keySuffix) {
    // Set the data size is the max_entry_size minus key size
    // because the total size must be smaller than or equal to
    // btree_m::max_entry_size()
    bool isMultiThrd = (keySuffix!='\0');
    const int key_size = isMultiThrd ? 6 : 5; // When this is used in multi-threaded tests, each thread needs to pass a different keySuffix to prevent duplicate records
    const int data_size = btree_m::max_entry_size() - key_size;

    vec_t data;
    char data_str[data_size];
    memset(data_str, '\0', data_size);
    data.set(data_str, data_size);
    char key_str[key_size];
    key_str[0] = 'k';
    key_str[1] = 'e';
    key_str[2] = 'y';

    // Insert enough records to ensure page split
    // One big transaction with multiple insertions
    W_DO(test_env->begin_xct());
    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
    for (int i = 0; i < recordCount; ++i) {
        int num;
        num = recordCount - 1 - i;

        key_str[3] = ('0' + ((num / 10) % 10));
        key_str[4] = ('0' + (num % 10));
    if(isMultiThrd) key_str[5] = keySuffix;
        if (true == fCheckPoint && num == recordCount/2) {
            // Take one checkpoint half way through insertions
            W_DO(ss_m::checkpoint()); 
        }
        W_DO(test_env->btree_insert(stid, key_str, data_str));
    }
    if(true == fInflight) ss_m::detach_xct();
    else W_DO(test_env->commit_xct());
    return RCOK;
}

w_rc_t populate_records(stid_t stid, bool fCheckPoint) {
    return populate_records(stid, fCheckPoint, false, '\0');
}

w_rc_t delete_records(stid_t &stid, bool fCheckPoint, bool fInflight, char keySuffix) {
    const bool isMultiThrd = (keySuffix!='\0');
    const int key_size = isMultiThrd ? 6 : 5; // When this is used in multi-threaded tests, each thread needs to pass a different keySuffix to prevent duplicate records
    char key_str[key_size];
    key_str[0] = 'k';
    key_str[1] = 'e';
    key_str[2] = 'y';

    // Delete every second record, will lead to page merge
    // One big transaction with multiple deletions
    W_DO(test_env->begin_xct());
    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
    for (int i=0; i < recordCount; i+=2) {
    key_str[3] = ('0' + ((i / 10) % 10));
    key_str[4] = ('0' + (i % 10));
    if(isMultiThrd) key_str[5] = keySuffix;
    if (true == fCheckPoint && i == recordCount/2) {
        // Take one checkpoint halfway through deletions
        W_DO(ss_m::checkpoint());
    }
    W_DO(test_env->btree_remove(stid, key_str));
    }
    if (true == fInflight) ss_m::detach_xct();
    else W_DO(test_env->commit_xct());
    return RCOK;
}

std::string getMaxKeyString(char maxSuffix) {
    const int recordsPerThrd = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
    char a = '0' + (recordsPerThrd-1) / 10;
    char b = '0' + (recordsPerThrd-1) % 10;
    char maxkeystr[10] = {"key000"};
    maxkeystr[3] = a;
    maxkeystr[4] = b;
    if (maxSuffix != '\0')
    maxkeystr[5] = maxSuffix;
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

        W_DO(populate_records(_stid, false));  // No checkpoint
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

        W_DO(populate_records(_stid, true));  // Checkpoint

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

        W_DO(populate_records(_stid, false));  // No checkpoint
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

        W_DO(populate_records(_stid, true));  // Checkpoint
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

        W_DO(populate_records(_stid, false));  // No checkpoint
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

        W_DO(populate_records(_stid, true));  // Checkpoint
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
    test_env->btree_update(pstid, "aa1", "data1");
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
 * Failing due to issue ZERO-183 and ZERO-182 (see test_restart_bugs)
TEST (RestartTest, MultithrdInflightC) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight2 context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
**/

/* Test case with 3 threads:
 * t1:    1 committed trans w/ 2 inserts, 1 aborted trans w/ 1 update & 1 remove
 * t2:    1 committed trans w/ 2 inserts, 1 aborted trans w/ 1 remove & 1 update, 1 aborted trans w/ 1 update & 1 remove
 * t3:    1 committed trans w/ 2 inserts, 1 aborted trans w/ 2 updates & 1 insert
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
 * Failing due to issue ZERO-183 (see test_restart_bugs) *
TEST (RestartTest, MultithrdAbort2C) {
    test_env->empty_logdata_dir();
    restart_multithrd_abort2 context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
**/

/* Test case with 3 threads:
 * t1:	1 committed insert, 1 in-flight trans with an update, insert, remove
 * t2:	1 committed insert, 1 in-flight trans with an insert, update, insert, update, remove
 * t3:	3 committed inserts, 1 in-flight trans with a remove, update, remove, update, insert
 */
class restart_multithrd_inflight3 : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
        test_env->btree_insert_and_commit(pstid, "aa0", "data0");
    test_env->begin_xct();
    test_env->btree_update(pstid, "aa0", "data00");
    test_env->btree_insert(pstid, "aa1", "data1");
    test_env->btree_remove(pstid, "aa0");
    ss_m::detach_xct();
    }   
    static void t2Run(stid_t pstid) {
    test_env->btree_insert_and_commit(pstid, "aa2", "data2");
        test_env->begin_xct();
    test_env->btree_insert(pstid, "aa3", "data3");
    test_env->btree_update(pstid, "aa3", "data33");
    test_env->btree_insert(pstid, "aa4", "data4");
    test_env->btree_update(pstid, "aa2", "data2");
    test_env->btree_remove(pstid, "aa2");
    ss_m::detach_xct();
    }   
    static void t3Run(stid_t pstid) {
        test_env->btree_insert_and_commit(pstid, "aa5", "data5");
        test_env->btree_insert_and_commit(pstid, "aa6", "data6");
        test_env->btree_insert_and_commit(pstid, "aa7", "data7");
    test_env->begin_xct();
    test_env->btree_remove(pstid, "aa5");
    test_env->btree_update(pstid, "aa6", "data66");
    test_env->btree_remove(pstid, "aa7");
    test_env->btree_update(pstid, "aa6", "data666");
    test_env->btree_insert(pstid, "aa5", "data55");
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
        EXPECT_EQ(5, s.rownum);
        EXPECT_EQ(std::string("aa0"), s.minkey);
        EXPECT_EQ(std::string("aa7"), s.maxkey);
        std::string data;
    test_env->btree_lookup_and_commit(_stid, "aa0", data);
    EXPECT_EQ(std::string("data0"), data);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdInflight3N) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight3 context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
}
/**/
/* Failing, see ZERO-182 / ZERO-183
TEST (RestartTest, MultithrdInflight3C) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight3 context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
*/
    
/* Same test code as restart_multithrd_inflight3 above, with 2 checkpoints in between */
class restart_multithrd_inflight_chckp1 : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
        test_env->btree_insert_and_commit(pstid, "aa0", "data0");
        test_env->begin_xct();
        test_env->btree_update(pstid, "aa0", "data00");
        test_env->btree_insert(pstid, "aa1", "data1");
    ss_m::checkpoint();
        test_env->btree_remove(pstid, "aa0");
        ss_m::detach_xct();
    }    
    static void t2Run(stid_t pstid) {
        test_env->btree_insert_and_commit(pstid, "aa2", "data2");
        test_env->begin_xct();
        test_env->btree_insert(pstid, "aa3", "data3");
        test_env->btree_update(pstid, "aa3", "data33");
        test_env->btree_insert(pstid, "aa4", "data4");
        test_env->btree_update(pstid, "aa2", "data2");
        test_env->btree_remove(pstid, "aa2");
        ss_m::detach_xct();
    }    
    static void t3Run(stid_t pstid) {
        test_env->btree_insert_and_commit(pstid, "aa5", "data5");
        test_env->btree_insert_and_commit(pstid, "aa6", "data6");
        test_env->btree_insert_and_commit(pstid, "aa7", "data7");
        test_env->begin_xct();
        test_env->btree_remove(pstid, "aa5");
        test_env->btree_update(pstid, "aa6", "data66");
        test_env->btree_remove(pstid, "aa7");
    ss_m::checkpoint();
        test_env->btree_update(pstid, "aa6", "data666");
        test_env->btree_insert(pstid, "aa5", "data55");
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
        EXPECT_EQ(5, s.rownum);
        EXPECT_EQ(std::string("aa0"), s.minkey);
        EXPECT_EQ(std::string("aa7"), s.maxkey);
        std::string data;
        test_env->btree_lookup_and_commit(_stid, "aa0", data);
        EXPECT_EQ(std::string("data0"), data);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdInflightChckp1N) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight_chckp1 context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
}
/**/

/* Failing, see ZERO-182 / ZERO-183
TEST (RestartTest, MultithrdInflightChckp1C) {
    test_env->empty_logdata_dir();
    restart_multithrd_inflight_chckp1 context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
*/

/// Test case with 3 threads, each with a committed transaction containing a large amount of inserts, no checkpoints
class restart_multithrd_ldata1 : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
    populate_records(pstid, false, false, '1'); // Populate records without checkpoint, commit, keySuffix '1'
    }
    static void t2Run(stid_t pstid) {
        populate_records(pstid, false, false, '2');
    }
    static void t3Run(stid_t pstid) {
        populate_records(pstid, false, false, '3');
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
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 15;
        EXPECT_EQ (recordCount, s.rownum); 
    EXPECT_EQ(std::string("key001"), s.minkey);
    EXPECT_EQ(getMaxKeyString('3'), s.maxkey);
    return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdLData1N) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata1 context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
}
/* Passing */
TEST (RestartTest, MultithrdLData1C) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata1 context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
/**/

/// Test case with 3 threads, each with a committed transaction containing a large amount of inserts, 2 checkpoints
class restart_multithrd_ldata2 : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
    populate_records(pstid, true, false, '1'); // Populate records with checkpoint, commit, keySuffix '1'
    }
    static void t2Run(stid_t pstid) {
    populate_records(pstid, false, false, '2'); // Populate records without checkpoint, commit, keySuffix '2'
    }
    static void t3Run(stid_t pstid) {
    populate_records(pstid, true, false, '3');
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
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 15;
        EXPECT_EQ (recordCount, s.rownum); 
    EXPECT_EQ(std::string("key001"), s.minkey);
    EXPECT_EQ(getMaxKeyString('3'), s.maxkey);
    return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdLData2N) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata2 context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
}
/* Passing */
TEST (RestartTest, MultithrdLData2C) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata2 context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
/**/


/// Test case with 3 threads, each with a transaction containing a large amount of inserts, 2 inflight, 1 committed, no checkpoints
class restart_multithrd_ldata3 : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
    populate_records(pstid, false, true, '1'); // Populate records without checkpoint, don't commit, keySuffix '1'
    }
    static void t2Run(stid_t pstid) {
    populate_records(pstid, false, false, '2'); // Populate records without checkpoint, commit, keySuffix '2'
    }
    static void t3Run(stid_t pstid) {
    populate_records(pstid, false, true, '3');
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
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum); 
    EXPECT_EQ(std::string("key002"), s.minkey);
    EXPECT_EQ(getMaxKeyString('2'), s.maxkey);
    return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdLData3N) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata3 context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
}
/* Failing - Assertion failure in src/sm/log_core.cpp:2552 during restart 
TEST (RestartTest, MultithrdLData3C) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata3 context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
*/

/// Test case with 3 threads, each with a transaction containing a large amount of inserts, 2 inflight, 1 committed, 2 checkpoints
class restart_multithrd_ldata4 : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
    populate_records(pstid, true, true, '1'); // Populate records with checkpoint, don't commit, keySuffix '1'
    }
    static void t2Run(stid_t pstid) {
    populate_records(pstid, true, false, '2'); // Populate records with checkpoint, commit, keySuffix '2'
    }
    static void t3Run(stid_t pstid) {
    populate_records(pstid, false, true, '3'); // Populate reacords without checkpoint, don't commit, keySuffix '3'
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
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        EXPECT_EQ (recordCount, s.rownum); 
    EXPECT_EQ(std::string("key002"), s.minkey);
    EXPECT_EQ(getMaxKeyString('2'), s.maxkey);
    return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdLData4N) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata4 context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
}
/* Failing - Assertion failure in src/sm/log_core.cpp:2552 during restart
TEST (RestartTest, MultithrdLData4C) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata4 context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 10), 0);
    // true = crash shutdown, 10 = recovery mode, m1 default serial mode
}
*/

/// Test case with 3 threads, each with a transaction containing a large amount of inserts, 2 inflight, 1 committed, 2 checkpoints
class restart_multithrd_ldata5 : public restart_test_base
{
public:
    static void t1Run(stid_t pstid) {
    populate_records(pstid, true, true, '1'); // Populate records w/ checkpoint, don't commit, keySuffix '1'
    }
    static void t2Run(stid_t pstid) {
    populate_records(pstid, true, false, '2'); // Populate records w/ checkpoint, commit, keySuffix '2'
    delete_records(pstid, false, false, '2');  // Delete half of those, w/o checkpoint, commit, keySuffix '2'
    }
    static void t3Run(stid_t pstid) {
    populate_records(pstid, false, false, '3'); // Populate records w/o checkpoint, commit, keySuffix '3'
    delete_records(pstid, false, true, '3');    // Delete half of those, w/o checkpoint, don't commit, keySuffix '3'
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
        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5; // Inserts from t3 (not deleted)
        recordCount += ((SM_PAGESIZE / btree_m::max_entry_size()) * 5) / 2;  // Inserts from t2 (half deleted)
    EXPECT_EQ (recordCount, s.rownum); 
    EXPECT_EQ(std::string("key003"), s.minkey);
    EXPECT_EQ(getMaxKeyString('3'), s.maxkey);
    return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultithrdLData5N) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata5 context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 10), 0);
    // false = normal shutdown, 10 = recovery mode, m1 default serial mode
}
/**/
/* Failing (ghost page bug)  
TEST (RestartTest, MultithrdLData5C) {
    test_env->empty_logdata_dir();
    restart_multithrd_ldata5 context;
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
