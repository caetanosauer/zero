#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"
#include "xct.h"
#include "sm_base.h"
#include "sm_external.h"

const int WAIT_TIME = 1000; // Wait 1 second
const int SHORT_WAIT_TIME = 100; // Wait 1/10 of a  second


/* This class contains only test cases that are failing at the time.
 * The issues that cause the test cases to fail are tracked in the bug reporting system,
 * the associated issue ID is noted beside each test case. 
 * Since they would block the check-in process, all test cases are disabled. 
 */

btree_test_env *test_env;

lsn_t get_durable_lsn() {
    lsn_t ret;
    ss_m::get_durable_lsn(ret);
    return ret;
}
void output_durable_lsn(int W_IFDEBUG1(num)) {
    DBGOUT1( << num << ".durable LSN=" << get_durable_lsn());
}

w_rc_t populate_multi_page_record(ss_m *ssm, stid_t &stid, bool fCommit) 
{
    // One transaction, caller decide commit the txn or not

    // Set the data size is the max_entry_size minus key size
    // because the total size must be smaller than or equal to
    // btree_m::max_entry_size()
    const int key_size = 5;
    const int data_size = btree_m::max_entry_size() - key_size;

    vec_t data;
    char data_str[data_size];
    memset(data_str, 'D', data_size);
    data.set(data_str, data_size);
    w_keystr_t key;
    char key_str[key_size];
    key_str[0] = 'k';
    key_str[1] = 'e';
    key_str[2] = 'y';

    // Insert enough records to ensure page split
    // One big transaction with multiple insertions
    
    W_DO(test_env->begin_xct());    

//    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 1;       // Passing
//    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 1 + 1;   // Passing
//    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 1 + 2;   // Passing
    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;   // Passing


    for (int i = 0; i < recordCount; ++i) 
    {
        int num;
        num = recordCount - 1 - i;

        key_str[3] = ('0' + ((num / 10) % 10));
        key_str[4] = ('0' + (num % 10));
        key.construct_regularkey(key_str, key_size);

        W_DO(ssm->create_assoc(stid, key, data));
    }

    // Commit the record only if told
    if (true == fCommit)
        W_DO(test_env->commit_xct());

    return RCOK;
}


// Test case with multi-page b-tree, simple transactions (1 in-flight)
// one concurrent txn with conflict during redo phase
class restart_multi_concurrent_redo : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // One big committed txn
        W_DO(populate_multi_page_record(ssm, _stid, true));  // true: commit
        
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        bool fCrash = test_env->_fCrash;
        int32_t recovery_mode = test_env->_recovery_mode;
        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5 + 1;
        // No wait in test code, but wait in recovery
        // This is to ensure concurrency
        
        if (fCrash && recovery_mode < m3_default_restart) 
        {   
            // if m2 crash shutdown
            // Verify
            w_rc_t rc = test_env->btree_scan(_stid, s); // Should have multiple pages of data
                                // the concurrent txn is a read/scan txn
                                // should still not be allowed due to delay in REDO in m2 crash shutdown
            if (rc.is_error()) 
            {
                DBGOUT3(<<"restart_multi_concurrent_redo: tree_scan error: " << rc);

                // Abort the failed scan txn
                test_env->abort_xct();

                // Sleep to give Recovery sufficient time to finish
                while (true == test_env->in_restart()) 
                {
                    // Concurrent recovery is still going on, wait
                    ::usleep(WAIT_TIME);
                }

            // Try again
            W_DO(test_env->btree_scan(_stid, s));
            EXPECT_EQ (std::string("aa4"), s.minkey);
            EXPECT_EQ (recordCount, s.rownum);
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
            W_DO(test_env->btree_scan(_stid, s));
            EXPECT_EQ (recordCount, s.rownum);
            EXPECT_EQ(std::string("aa4"), s.minkey);
            return RCOK;
        }
    }    
};

/* Passing */
TEST (RestartTest, MultiConcurrentRedoCF) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_redo context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, m2_redo_fl_delay_restart), 0);   // true = simulated crash
                                                                  // full logging
}
/**/


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
