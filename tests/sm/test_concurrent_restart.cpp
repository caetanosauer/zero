#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"
#include "xct.h"

btree_test_env *test_env;

const int WAIT_TIME = 1000; // Wait 1 second

// Test cases to test concurrent restart.
// Depending on the recovery mode, the test results might vary and therefore tricky

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
    EXPECT_EQ(test_env->runRestartTest(&context, false, 20), 0);  // false = no simulated crash, normal shutdown
                                                                  // 20 = recovery mode, m2 default concurrent mode
}
/**/

// Test case with simple transactions (1 in-flight) and normal shutdown, no concurrent activities during recovery
class restart_simple_normal : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));

        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa4", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;

        while (true == test_env->in_recovery())
        {
            // Concurrent recovery is still going on, wait
            ::usleep(WAIT_TIME);            
        }

        // Verify
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, SimpleNormal) {
    test_env->empty_logdata_dir();
    restart_simple_normal context;
    EXPECT_EQ(test_env->runRestartTest(&context, false, 20), 0);  // false = no simulated crash, normal shutdown
                                                                  // 20 = recovery mode, m2 default concurrent mode, no wait
}
/**/

// Test case with simple transactions (1 in-flight) and crash shutdown, no concurrent activities during recovery
class restart_simple_crash : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));

        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa4", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;

        while (true == test_env->in_recovery())
        {
            // Concurrent recovery is still going on, wait
            ::usleep(WAIT_TIME);            
        }

        // Verify
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
        return RCOK;
    }
};

/* Not passing in UNDO phase during 'abort' *
TEST (RestartTest, SimpleCrash) {
    test_env->empty_logdata_dir();
    restart_simple_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 20), 0);   // true = simulated crash
                                                                  // 20 = recovery mode, m2 default concurrent mode, no wait
}
**/

// Test case with transactions (1 in-flight with multiple operations) and crash shutdown
// no concurrent activities during recovery
class restart_complex_in_flight_crash : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));

        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));

        W_DO(test_env->begin_xct());                                     // in-flight
        W_DO(test_env->btree_insert(_stid, "aa5", "data5"));
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        W_DO(test_env->btree_insert(_stid, "aa7", "data7"));

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;

        while (true == test_env->in_recovery())
        {
            // Concurrent recovery is still going on, wait
            ::usleep(WAIT_TIME);            
        }

        // Verify
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Not passing in UNDO phase during 'abort' *
TEST (RestartTest, ComplexInFlightCrash) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 20), 0);   // true = simulated crash
                                                                  // 20 = recovery mode, m2 default concurrent mode, no wait
}
**/

// Test case with transactions (1 in-flight with multiple operations) with checkpoint and crash shutdown
// no concurrent activities during recovery
class restart_complex_in_flight_chkpt_crash : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));

        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));

        W_DO(test_env->begin_xct());                                     // in-flight
        W_DO(test_env->btree_insert(_stid, "aa5", "data5"));
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        W_DO(test_env->btree_insert(_stid, "aa7", "data7"));

        W_DO(ss_m::checkpoint()); 

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;

        while (true == test_env->in_recovery())
        {
            // Concurrent recovery is still going on, wait
            ::usleep(WAIT_TIME);            
        }

        // Verify
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa4"), s.maxkey);
        return RCOK;
    }
};

/* Not passing in UNDO phase during 'abort' *
TEST (RestartTest, ComplexInFlightChkptCrash) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight_chkpt_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 20), 0);   // true = simulated crash
                                                                  // 20 = recovery mode, m2 default concurrent mode, no wait
}
**/

// Test case with simple transactions (1 in-flight) and crash shutdown, one concurrent txn with conflict
class restart_simple_concurrent_crash : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));

        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa4", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;

        // Verify
        W_DO(test_env->btree_scan(_stid, s));  // Should have only one page of data
                                               // while recovery is on for this page
                                               // therefore the concurrent txn should not be allowed
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
        return RCOK;
    }
};

/* Not passing in UNDO phase during 'abort' *
TEST (RestartTest, SimpleConcurrentCrash) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 21), 0);   // true = simulated crash
                                                                  // 21 = recovery mode, m2 concurrent mode with delay in REDO
}
**/

// Test case with more than one page of data (1 in-flight) and crash shutdown, one concurrent txn to
// access a non-dirty page so it should be allowed
class restart_concurrent_no_conflict_crash : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

/** TODO...
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));

        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));
**/

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa4", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;

/** TODO...
        // Verify
        W_DO(test_env->btree_scan(_stid, s));  // Should have only one page of data
                                               // while recovery is on for this page
                                               // therefore the concurrent txn should not be allowed
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
**/        
        return RCOK;
    }
};

/* Not passing in UNDO phase during 'abort' *
TEST (RestartTest, ConcurrentNoConflictCrash) {
    test_env->empty_logdata_dir();
    restart_concurrent_no_conflict_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 23), 0);   // true = simulated crash
                                                                  // 23 = recovery mode, m2 concurrent mode with delay in REDO and UNDO
}
**/

// Test case with more than one page of data (1 in-flight) and crash shutdown, one concurrent txn to
// access an in_doubt page so it should not be allowed
class restart_concurrent_conflict_crash : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

/** TODO...
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));

        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));
**/

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa4", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;

/** TODO...
        // Verify
        W_DO(test_env->btree_scan(_stid, s));  // Should have only one page of data
                                               // while recovery is on for this page
                                               // therefore the concurrent txn should not be allowed
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
**/        
        return RCOK;
    }
};

/* Not passing in UNDO phase during 'abort' *
TEST (RestartTest, ConcurrentConflictCrash) {
    test_env->empty_logdata_dir();
    restart_concurrent_conflict_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 23), 0);   // true = simulated crash
                                                                  // 23 = recovery mode, m2 concurrent mode with delay in REDO and UNDO
}
**/


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
