#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"
#include "xct.h"

btree_test_env *test_env;

const int WAIT_TIME = 1000; // Wait 1 second
const int SHORT_WAIT_TIME = 100; // Wait 1/10 of a  second


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

    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;

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

w_rc_t populate_records(ss_m *ssm, stid_t &stid, bool fCheckPoint) 
{
    // Multiple committed transactions, caller decide whether to include a checkpount or not

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
    // Multiple transactions with one insertion per transaction
    // Commit all transactions
    
    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
    for (int i = 0; i < recordCount; ++i) 
    {
        int num;
        num = recordCount - 1 - i;

        key_str[3] = ('0' + ((num / 10) % 10));
        key_str[4] = ('0' + (num % 10));
        key.construct_regularkey(key_str, key_size);

        if (true == fCheckPoint) 
        {
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

/* Passing */
TEST (RestartTest, SimpleCrash) {
    test_env->empty_logdata_dir();
    restart_simple_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 20), 0);   // true = simulated crash
                                                                  // 20 = recovery mode, m2 default concurrent mode, no wait
}
/**/

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
        W_DO(test_env->btree_insert(_stid, "aa7", "data5"));
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        W_DO(test_env->btree_insert(_stid, "aa5", "data7"));

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

/* When there are multiple insertions in one txn, it rolls back only the very first */
/* insertion in the txn, but not the rest of the insertions.  Issue in xct_t::rollback undo_nxt */
/* for this test case, the result: 5 records instead of 3 records, 'aa7' was rollback, so the max is 'aa5' instead of ''aa4' */
/* Not passing *
TEST (RestartTest, ComplexInFlightCrash) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 20), 0);   // true = simulated crash
                                                                  // 20 = recovery mode, m2 default concurrent mode, no wait
}
**/

// Test case with transactions (1 in-flight) with checkpoint and crash shutdown
// no concurrent activities during recovery
class restart_complex_in_flight_chkpt_crash : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert(_stid, "aa4", "data4"));
        W_DO(test_env->commit_xct());

        W_DO(test_env->begin_xct());                                     // in-flight
        W_DO(test_env->btree_insert(_stid, "aa5", "data5"));

        // Commented out, same issue as the previous test 'restart_complex_in_flight_crash'
//        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
//        W_DO(test_env->btree_insert(_stid, "aa7", "data7"));

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

/* Passing */
TEST (RestartTest, ComplexInFlightChkptCrash) {
    test_env->empty_logdata_dir();
    restart_complex_in_flight_chkpt_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 20), 0);   // true = simulated crash
                                                                  // 20 = recovery mode, m2 default concurrent mode, no wait
}
/**/

// Test case with 1 transaction (in-flight with more than one page of data) and crash shutdown
// no concurrent activities during recovery
class restart_multi_page_in_flight_crash : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // One big uncommitted txn
        W_DO(populate_multi_page_record(ssm, _stid, false));  // false: Do not commit, in-flight
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
        EXPECT_EQ (0, s.rownum);
        return RCOK;
    }
};

/* Multiple issues when there are multiple pages of data in one in-flight transaction */
/* 1. During REDO, multi-pages, WOD is not followed for SPR */
/* 2. Same issue as previous tests when the in-flight txn has more than one operations */
/* Not passing *
TEST (RestartTest, MultiPageInFlightCrash) {
    test_env->empty_logdata_dir();
    restart_multi_page_in_flight_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 20), 0);   // true = simulated crash
                                                                  // 20 = recovery mode, m2 default concurrent mode, no wait
}
**/

// Test case with simple transactions (1 in-flight) and crash shutdown, one concurrent chkpt
class restart_concurrent_chkpt_crash : public restart_test_base  {
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

        // Concurrent chkpt
        W_DO(ss_m::checkpoint()); 

        while (true == test_env->in_recovery())
        {
            // Concurrent recovery is still going on, wait
            ::usleep(WAIT_TIME);            
        }

        // Verify
        x_btree_scan_result s;        
        W_DO(test_env->btree_scan(_stid, s));  // Should have only one page of data
                                               // while recovery is on for this page
                                               // therefore the concurrent txn should not be allowed
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, ConcurrentChkptCrash) {
    test_env->empty_logdata_dir();
    restart_concurrent_chkpt_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 21), 0);   // true = simulated crash
                                                                  // 21 = recovery mode, m2 concurrent mode with delay in REDO
}
/**/

// Test case with simple transactions (1 in-flight) and crash shutdown, 
// one concurrent txn with conflict during redo phase
class restart_simple_concurrent_redo_crash : public restart_test_base  {
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

        // No wait in test code, but wait in recovery
        // This is to ensure concurrency
        
        // Verify
        w_rc_t rc = test_env->btree_scan(_stid, s);  // Should have only one page of data
                                                     // while recovery is on for this page
                                                     // therefore even the concurrent txn is a
                                                     // read/scan txn, it should not be allowed
        if (rc.is_error())
        {
            DBGOUT3(<<"restart_simple_concurrent_redo_crash: tree_scan error: " << rc);        

            // Abort the failed scan txn
            test_env->abort_xct();

            // Sleep to give Recovery sufficient time to finish
            while (true == test_env->in_recovery())
            {
                // Concurrent recovery is still going on, wait
                ::usleep(WAIT_TIME);            
            }

            // Try again
            W_DO(test_env->btree_scan(_stid, s));
        }
        else
        {
            cerr << "restart_simple_concurrent_redo_crash: scan operation should not succeed"<< endl;         
            return RC(eINTERNAL);
        }

        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, SimpleConcurrentRedoCrash) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_redo_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 21), 0);   // true = simulated crash
                                                                  // 21 = recovery mode, m2 concurrent mode with delay in REDO
}
/**/

// Test case with multi-page b-tree, simple transactions (1 in-flight) and crash shutdown, 
// one concurrent txn with conflict during redo phase
class restart_multi_concurrent_redo_crash : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // One big committed txn
        W_DO(populate_multi_page_record(ssm, _stid, true));  // true: commit
        
        W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "aa2", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;

        // No wait in test code, but wait in recovery
        // This is to ensure concurrency
        
        // Verify
        w_rc_t rc = test_env->btree_scan(_stid, s);  // Should have multiple pages of data
                                                     // the concurrent txn is a read/scan txn
                                                     // should still not be allowed due to delay in REDO

        if (rc.is_error())
        {
            DBGOUT3(<<"restart_multi_concurrent_redo_crash: tree_scan error: " << rc);

            // Abort the failed scan txn
            test_env->abort_xct();

            // Sleep to give Recovery sufficient time to finish
            while (true == test_env->in_recovery())
            {
                // Concurrent recovery is still going on, wait
                ::usleep(WAIT_TIME);
            }

            // Try again
            W_DO(test_env->btree_scan(_stid, s));
        }
        else
        {
            cerr << "restart_multi_concurrent_redo_crash: scan operation should not succeed"<< endl;         
            return RC(eINTERNAL);
        }

        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5 + 1;
        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ (std::string("aa4"), s.minkey);
        return RCOK;
    }
};


/* During REDO, multi-pages, WOD is not followed for SPR */
/* but the conflict detection is working */
/* Not passing - currently using this one to debug *
TEST (RestartTest, MultiConcurrentRedoCrash) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_redo_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 21), 0);   // true = simulated crash
                                                                  // 21 = recovery mode, m2 concurrent mode with delay in REDO
}
**/

// Test case with simple transactions (1 in-flight) and crash shutdown, 
// one concurrent txn with conflict during undo phase
class restart_simple_concurrent_undo_crash : public restart_test_base  {
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

        // Wiat a short time, this is to allow REDO to finish,
        // but hit the UNDO phase using specified recovery mode which waits before UNDO
        ::usleep(SHORT_WAIT_TIME);

        // Verify
        w_rc_t rc = test_env->btree_scan(_stid, s);   // Should have only one page of data
                                                      // while recovery is on for this page
                                                      // although REDO is done, UNDO is not
                                                      // therefore the concurrent txn should not be allowed

        if (rc.is_error())
        {
            DBGOUT3(<<"restart_simple_concurrent_undo_crash: tree_scan error: " << rc);

            // Abort the failed scan txn
            test_env->abort_xct();

            // Sleep to give Recovery sufficient time to finish
            while (true == test_env->in_recovery())
            {
                // Concurrent recovery is still going on, wait
                ::usleep(WAIT_TIME);
            }

            // Try again
            W_DO(test_env->btree_scan(_stid, s));
        }
        else
        {
            cerr << "restart_simple_concurrent_undo_crash: scan operation should not succeed"<< endl;         
            return RC(eINTERNAL);
        }

        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, SimpleConcurrentUndoCrash) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_undo_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 22), 0);   // true = simulated crash
                                                                  // 22 = recovery mode, m2 concurrent mode with delay in UNDO
}
/**/

// Test case with more than one page of data (1 in-flight) and crash shutdown, one concurrent txn to
// access a non-dirty page so it should be allowed
class restart_concurrent_no_conflict_crash : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // Multiple committed transactions with many pages
        W_DO(populate_records(ssm, _stid, false));  // false: No checkpoint

        // Issue a checkpoint to make sure these committed txns are flushed
        W_DO(ss_m::checkpoint());         

        // Now insert more records, make sure these records are at 
        // the end of B-tree (append)
        W_DO(test_env->btree_insert_and_commit(_stid, "zz3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "zz1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "zz2", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "zz4", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;

        // Wait a while, this is to give REDO a chance to reload the root page
        // but still wait in REDO phase due to test mode
        ::usleep(SHORT_WAIT_TIME*5);

        // Wait in recovery both REDO and UNDO, this is to ensure 
        // user transaction encounter concurrent recovery
        // Insert into the first page, depending on how far the REDO goes,
        // the insertion might or might not succeed
        W_DO(test_env->begin_xct());
        w_rc_t rc = test_env->btree_insert(_stid, "aa1", "data4");
        if (rc.is_error())
        {
            // Conflict        
            cerr << "restart_concurrent_no_conflict_crash: tree_insertion failed"<< endl;
            W_DO(test_env->abort_xct());
        }
        else
        {
            // Succeed
            DBGOUT3(<<"restart_concurrent_no_conflict_crash: tree_insertion succeeded");           
            W_DO(test_env->commit_xct());
        }

        // Wait before the final verfication
        while (true == test_env->in_recovery())
        {
            // Concurrent recovery is still going on, wait
            ::usleep(WAIT_TIME);            
        }

        // Verify
        W_DO(test_env->btree_scan(_stid, s));

        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;  // Count before checkpoint
        recordCount += 3;  // Count after checkpoint
        if (!rc.is_error())        
            recordCount += 1;  // Count after concurrent insert

        EXPECT_EQ (recordCount, s.rownum);
        if (!rc.is_error())
            EXPECT_EQ (std::string("aa1"), s.minkey);

        return RCOK;
    }
};

/* During REDO, multi-pages, WOD is not followed for SPR */
/* Not passing *
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

        // Multiple committed transactions with many pages
        W_DO(populate_records(ssm, _stid, false));   // false: No checkpoint

        // Issue a checkpoint to make sure these committed txns are flushed
        W_DO(ss_m::checkpoint());

        // Now insert more records, make sure these records are at 
        // the end of B-tree (append)
        W_DO(test_env->btree_insert_and_commit(_stid, "zz3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "zz1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "zz2", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "zz4", "data4"));     // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;

        // Wait a while, this is to give REDO a chance to reload the root page
        // but still wait in REDO phase due to test mode
        ::usleep(SHORT_WAIT_TIME*5);

        // Wait in recovery, this is to ensure user transaction encounter concurrent recovery
        // Insert into the last page which should cause a conflict        
        W_DO(test_env->begin_xct());
        w_rc_t rc = test_env->btree_insert(_stid, "zz5", "data4");
        if (rc.is_error()) 
        {
            // Expected behavior
            W_DO(test_env->abort_xct());            
        }
        else
        {
            cerr << "restart_concurrent_conflict_crash: tree_insertion should not succeed"<< endl;
            // Should not succeed
            RC(eINTERNAL);
        }

        // Wait before the final verfication
        while (true == test_env->in_recovery())
        {
            // Concurrent recovery is still going on, wait
            ::usleep(WAIT_TIME);            
        }

        // Verify
        W_DO(test_env->btree_scan(_stid, s));

        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;  // Count before checkpoint
        recordCount += 3;  // Count after checkpoint

        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ (std::string("zz3"), s.maxkey);
        return RCOK;
    }
};

/* During REDO, multi-pages, WOD is not followed for SPR */
/* Not passing *
TEST (RestartTest, ConcurrentConflictCrash) {
    test_env->empty_logdata_dir();
    restart_concurrent_conflict_crash context;
    EXPECT_EQ(test_env->runRestartTest(&context, true, 23), 0);   // true = simulated crash
                                                                  // 23 = recovery mode, m2 concurrent mode with delay in REDO and UNDO
}
**/

// Test case with more than one page of data (1 in-flight) and crash shutdown, multiple concurrent txns
// some should succeeded (no conflict) while others failed (conflict), also one 'conflict' user transaction
// after recovery which should succeed
class restart_multi_concurrent_conflict_crash : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // Multiple committed transactions with many pages
        W_DO(populate_records(ssm, _stid, false));   // false: No checkpoint

        // Issue a checkpoint to make sure these committed txns are flushed
        W_DO(ss_m::checkpoint());

        // Now insert more records, make sure these records are at 
        // the end of B-tree (append)
        W_DO(test_env->btree_insert_and_commit(_stid, "zz3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "zz1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "zz2", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "zz7", "data4"));     // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;

        // Wait a while, this is to give REDO a chance to reload the root page
        // but still wait in REDO phase due to test mode
        ::usleep(SHORT_WAIT_TIME*5);

        // Wait in recovery, this is to ensure user transaction encounter concurrent recovery

        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;  // Count before checkpoint
        recordCount += 3;  // Count after checkpoint

        // Insert into the first page which should not cause a conflict        
        W_DO(test_env->begin_xct());
        w_rc_t rc = test_env->btree_insert(_stid, "aa1", "data4");
        if (rc.is_error()) 
        {
            // Will failed if the REDO phase did not process far enough
            W_DO(test_env->abort_xct());            
        }
        else
        {
            // Succeeded
            recordCount += 1;
            W_DO(test_env->commit_xct());        
        }

        W_DO(test_env->commit_xct());
        
        // Insert into the last page which should cause a conflict        
        W_DO(test_env->begin_xct());
        rc = test_env->btree_insert(_stid, "zz5", "data4");
        if (rc.is_error()) 
        {
            // Expected behavior
            W_DO(test_env->abort_xct());            
        }
        else
        {
            // Should not succeed
            cerr << "restart_multi_concurrent_conflict_crash: tree_insertion should not succeed"<< endl;            
            RC(eINTERNAL);
        }

        // Wait before the final verfication
        while (true == test_env->in_recovery())
        {
            // Concurrent recovery is still going on, wait
            ::usleep(WAIT_TIME);            
        }

        // Tried the failed txn again and it should succeed this time
        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid, "zz5", "data4"));
        W_DO(test_env->commit_xct());
        recordCount += 1;

        // Verify
        W_DO(test_env->btree_scan(_stid, s));

        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ (std::string("zz5"), s.maxkey);
        return RCOK;
    }
};

/* During REDO, multi-pages, WOD is not followed for SPR */
/* Not passing *
TEST (RestartTest, MultiConcurrentConflictCrash) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_conflict_crash context;
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
