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

// Test cases to test concurrent restart.
// Depending on the restart mode, the test results might vary and therefore tricky

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


// Test case with simple transactions (1 in-flight) and normal shutdown, no concurrent activities during restart
class restart_simple : public restart_test_base  {
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

        while (true == test_env->in_restart())
        {
            // Concurrent restart is still going on, wait
            ::usleep(WAIT_TIME);            
        }
        // An extra wait
        ::usleep(WAIT_TIME);

        // Verify
        W_DO(test_env->btree_scan(_stid, s));
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


// Test case with transactions (1 in-flight with multiple operations)
// no concurrent activities during restart
class restart_complex_in_flight : public restart_test_base  {
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

        while (true == test_env->in_restart())
        {
            // Concurrent restart is still going on, wait
            ::usleep(WAIT_TIME);            
        }
        // An extra wait
        ::usleep(WAIT_TIME);

        // Verify
        W_DO(test_env->btree_scan(_stid, s));
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


// Test case with transactions (1 in-flight) with checkpoint
// no concurrent activities during restart
class restart_complex_in_flight_chkpt : public restart_test_base  {
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
        W_DO(test_env->btree_insert(_stid, "aa2", "data2"));
        W_DO(test_env->btree_insert(_stid, "aa7", "data7"));
        W_DO(ss_m::checkpoint()); 

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;

        while (true == test_env->in_restart())
        {
            // Concurrent restart is still going on, wait
            ::usleep(WAIT_TIME);            
        }
        // An extra wait
        ::usleep(WAIT_TIME);

        // Verify
        W_DO(test_env->btree_scan(_stid, s));
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


// Test case with 1 transaction (in-flight with more than one page of data)
// no concurrent activities during restart
class restart_multi_page_in_flight : public restart_test_base  {
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

        while (true == test_env->in_restart())
        {
            // Concurrent restart is still going on, wait
            ::usleep(WAIT_TIME);            
        }
        // An extra wait
        ::usleep(WAIT_TIME);

        // Verify
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (0, s.rownum);
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

/* Not passing, full logging, btree_impl::_ux_undo_ghost_mark but the record is already a ghost *
TEST (RestartTest, MultiPageInFlightNF) {
    test_env->empty_logdata_dir();
    restart_multi_page_in_flight context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* See btree_impl::_ux_traverse_recurse, the '_ux_traverse_try_opportunistic_adopt' call */
/*    is returning eGOODRETRY and infinite loop, need further investigation, why?  A similar */
/* test case 'restart_multi_concurrent_redo' is passing but it commits the txn*/
/* Not passing, WOD with minimal logging *
TEST (RestartTest, MultiPageInFlightC) {
    test_env->empty_logdata_dir();
    restart_multi_page_in_flight context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* Not passing, full logging, infinite loop, same issue as minimal logging *
TEST (RestartTest, MultiPageInFlightCF) {
    test_env->empty_logdata_dir();
    restart_multi_page_in_flight context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

// Test case with simple transactions (1 in-flight) and crash shutdown, one concurrent chkpt
class restart_concurrent_chkpt : public restart_test_base  {
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

        while (true == test_env->in_restart())
        {
            // Concurrent restart is still going on, wait
            ::usleep(WAIT_TIME);            
        }
        // An extra wait
        ::usleep(WAIT_TIME);

        // Verify
        x_btree_scan_result s;        
        W_DO(test_env->btree_scan(_stid, s));  // Should have only one page of data
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


// Test case with simple transactions (1 in-flight)
// one concurrent txn with conflict during redo phase
class restart_simple_concurrent_redo : public restart_test_base  {
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
        const bool fCrash = test_env->_restart_options->shutdown_mode;
        const int32_t restart_mode = test_env->_restart_options->restart_mode;
        // No wait in test code, but wait in restart
        // This is to ensure concurrency
       

    if (fCrash && restart_mode < m3_default_restart ) {
        // Verify
        w_rc_t rc = test_env->btree_scan(_stid, s);  // Should have only one page of data
                                                     // while restart is on for this page
                                                     // therefore even the concurrent txn is a
                                                     // read/scan txn, it should not be allowed
        if (rc.is_error())
        {
        DBGOUT3(<<"restart_simple_concurrent_redo: tree_scan error: " << rc);        

        // Abort the failed scan txn
        test_env->abort_xct();

        // Sleep to give Recovery sufficient time to finish
        while (true == test_env->in_restart())
        {
            // Concurrent restart is still going on, wait
            ::usleep(WAIT_TIME);            
        }
        // An extra wait
        ::usleep(WAIT_TIME);

        // Try again
        W_DO(test_env->btree_scan(_stid, s));
        }
        else
        {
        std::cerr << "restart_simple_concurrent_redo: scan operation should not succeed"<< std::endl;         
        return RC(eINTERNAL);
        }
    } 
    else {
        W_DO(test_env->btree_scan(_stid, s));
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

/* Passing * - error in retail, timing?
TEST (RestartTest, SimpleConcurrentRedoCF) {
    test_env->empty_logdata_dir();
    restart_simple_concurrent_redo context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/


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
        W_DO(test_env->btree_insert(_stid, "aa2", "data4"));             // in-flight

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
        
        if (fCrash && restart_mode < m3_default_restart) {    // if m2 crash shutdown
            // Verify
            w_rc_t rc = test_env->btree_scan(_stid, s); // Should have multiple pages of data
                                                        // the concurrent txn is a read/scan txn
                                                        // should still not be allowed due to delay in REDO in m2 crash shutdown
            if (rc.is_error()) {
                DBGOUT3(<<"restart_multi_concurrent_redo: tree_scan error: " << rc);

                // Abort the failed scan txn
                test_env->abort_xct();

                // Sleep to give Recovery sufficient time to finish
                while (true == test_env->in_restart()) {
                    // Concurrent restart is still going on, wait
                    ::usleep(WAIT_TIME);
                }
                // An extra wait
                ::usleep(WAIT_TIME);

                // Try again
                W_DO(test_env->btree_scan(_stid, s));
                EXPECT_EQ (recordCount, s.rownum);
                EXPECT_EQ (std::string("aa4"), s.minkey);
                return RCOK;
            }
            else {
                std::cerr << "restart_multi_concurrent_redo: scan operation should not succeed"<< std::endl;         
                return RC(eINTERNAL);
            }
        }
        else {
            W_DO(test_env->btree_scan(_stid, s));
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

/* Passing, WOD with minimal logging, in-flight is in the first page */
TEST (RestartTest, MultiConcurrentRedoC) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_redo context;
  
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing, full logging, in-flight is in the first page */
TEST (RestartTest, MultiConcurrentRedoCF) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_redo context;
   
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0); 
}
/**/


// Test case with simple transactions (1 in-flight) and crash shutdown, 
// one concurrent txn with conflict during undo phase
class restart_simple_concurrent_undo : public restart_test_base  {
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
        bool fCrash = test_env->_restart_options->shutdown_mode;
        int32_t restart_mode = test_env->_restart_options->restart_mode;
        // Wiat a short time, this is to allow REDO to finish,
        // but hit the UNDO phase using specified restart mode which waits before UNDO
        ::usleep(SHORT_WAIT_TIME);

        // Verify
        w_rc_t rc = test_env->btree_scan(_stid, s);   // Should have only one page of data
                                                      // while restart is on for this page
                                                      // although REDO is done, UNDO is not
                                                      // therefore the concurrent txn should not be allowed
        if (fCrash && restart_mode < m3_default_restart) {
            if (rc.is_error()) {
                DBGOUT3(<<"restart_simple_concurrent_undo: tree_scan error: " << rc);

                // Abort the failed scan txn
                test_env->abort_xct();

                // Sleep to give Recovery sufficient time to finish
                while (true == test_env->in_restart()) {
                    // Concurrent restart is still going on, wait
                    ::usleep(WAIT_TIME);
                }
                // An extra wait
                ::usleep(WAIT_TIME);

                // Try again
                W_DO(test_env->btree_scan(_stid, s));

                EXPECT_EQ (3, s.rownum);
                EXPECT_EQ (std::string("aa1"), s.minkey);
                EXPECT_EQ (std::string("aa3"), s.maxkey);
                return RCOK;
            }
            else {
                std::cerr << "restart_simple_concurrent_undo: scan operation should not succeed"<< std::endl;
                return RC(eINTERNAL);
            }
        }
        else {
            W_DO(test_env->btree_scan(_stid, s));
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


// Test case with more than one page of data (1 in-flight), one concurrent txn to
// access a non-dirty page so it should be allowed
class restart_concurrent_no_conflict : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // Multiple committed transactions with many pages
        W_DO(populate_records(ssm, _stid, false));  // false: No checkpoint

        // Issue a checkpoint to make sure these committed txns are flushed
        W_DO(ss_m::checkpoint());         

        // Now insert more records, these records are at the beginning of B-tree
        // therefore if these records cause a page rebalance, it would be in the parent page        
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

        // Wait a while, this is to give REDO a chance to reload the root page
        // but still wait in REDO phase due to test mode
        ::usleep(SHORT_WAIT_TIME*5);

        // Wait in restart both REDO and UNDO, this is to ensure 
        // user transaction encounter concurrent restart
        // Insert into the first page, depending on how far the REDO goes,
        // the insertion might or might not succeed
        W_DO(test_env->begin_xct());
        w_rc_t rc = test_env->btree_insert(_stid, "aa7", "data4");
        if (rc.is_error())
        {
            // Conflict        
            std::cerr << "restart_concurrent_no_conflict: tree_insertion failed"<< std::endl;
            W_DO(test_env->abort_xct());
        }
        else
        {
            // Succeed
            DBGOUT3(<<"restart_concurrent_no_conflict: tree_insertion succeeded");           
            W_DO(test_env->commit_xct());
        }

        // Wait before the final verfication
        while (true == test_env->in_restart())
        {
            // Concurrent restart is still going on, wait
            ::usleep(WAIT_TIME);            
        }
        // An extra wait
        ::usleep(WAIT_TIME);

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


// Test case with more than one page of data (1 in-flight) and crash shutdown, one concurrent txn to
// access an in_doubt page so it should not be allowed
class restart_concurrent_conflict : public restart_test_base  {
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
        const bool fCrash = test_env->_restart_options->shutdown_mode;
        const int32_t restart_mode = test_env->_restart_options->restart_mode;

        // Wait a while, this is to give REDO a chance to reload the root page
        // but still wait in REDO phase due to test mode
        ::usleep(SHORT_WAIT_TIME*5);

        // Wait in restart, this is to ensure user transaction encounter concurrent restart
        // Insert into the last page which should cause a conflict        
        W_DO(test_env->begin_xct());
        w_rc_t rc = test_env->btree_insert(_stid, "zz5", "data4");
        if (rc.is_error() || !(fCrash && restart_mode<m3_default_restart))
        {
            // Expected behavior
            W_DO(test_env->abort_xct());            
        }
        else if (!rc.is_error())
        {
            std::cerr << "restart_concurrent_conflict: tree_insertion should not succeed"<< std::endl;
            // Should not succeed
            return RC(eINTERNAL);
        }

        // Wait before the final verfication
        while (true == test_env->in_restart())
        {
            // Concurrent restart is still going on, wait
            ::usleep(WAIT_TIME);            
        }
        // Extra sleep just in case
        ::usleep(WAIT_TIME);

        // Verify
        rc = test_env->btree_scan(_stid, s);
        if (rc.is_error())
        {
            test_env->abort_xct();       
            std::cerr << "restart_concurrent_conflict: failed to scan the tree, try again: " << rc << std::endl;        
            ::usleep(WAIT_TIME);
            rc = test_env->btree_scan(_stid, s);
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

/* Passing, minimal logging * - error in retail, timing?
TEST (RestartTest, ConcurrentConflictC) {
    test_env->empty_logdata_dir();
    restart_concurrent_conflict context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* Passing, full logging *  - error in retail, timing?
TEST (RestartTest, ConcurrentConflictCF) {
    test_env->empty_logdata_dir();
    restart_concurrent_conflict context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

// Test case with more than one page of data (1 in-flight) and crash shutdown, multiple concurrent txns
// some should succeeded (no conflict) while others failed (conflict), also one 'conflict' user transaction
// after restart which should succeed
class restart_multi_concurrent_conflict : public restart_test_base  {
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
        // Wait a while, this is to give REDO a chance to reload the root page
        // but still wait in REDO phase due to test mode
        ::usleep(SHORT_WAIT_TIME*5);

        // Wait in restart, this is to ensure user transaction encounter concurrent restart

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
        if(checkpoints_enabled)
            W_DO(ss_m::checkpoint());
        // Insert into the last page which should cause a conflict        
        W_DO(test_env->begin_xct());
        rc = test_env->btree_insert(_stid, "zz5", "data4");
        if ((rc.is_error() && fCrash && !m3_restart) || (!rc.is_error() && (!fCrash || m3_restart))) // Only m2 restart mode with crash shutdown should fail,
        {    // m3 rm and m2 rm with normal shutdown should succeed
            // Expected behavior
            W_DO(test_env->abort_xct());            
        }
        else
        {
            if ((!fCrash || m3_restart) && rc.is_error())
            { // Normal shutdown or M3, should have succeeded, did not
                std::cerr << "restart_multi_concurrent_conflict: tree_insertion should have succeeded but failed" << rc;
                return RC(eINTERNAL);
            }
            else if (fCrash && !m3_restart && !rc.is_error())
            { // Crash, not M3, insertion should failed but succeeded
            
                std::cerr << "restart_multi_concurrent_conflict: tree_insertion should failed but succeeded"<< std::endl;            
                return RC(eINTERNAL);
            }
        if (rc.is_error())
            test_env->abort_xct();
        }

        // Wait before the final verfication
        while (true == test_env->in_restart())
        {
            // Concurrent restart is still going on, wait
            ::usleep(WAIT_TIME);            
        }
        // An extra wait
        ::usleep(WAIT_TIME);

        if(checkpoints_enabled)
            W_DO(ss_m::checkpoint());

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

/* Passing, minimal logging *  - error in retail, timing?
TEST (RestartTest, MultiConcurrentConflictC) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_conflict context;
 
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0); 
}
**/

/* Passing, full logging *  - error in retail, timing?
TEST (RestartTest, MultiConcurrentConflictCF) {
    test_env->empty_logdata_dir();
    restart_multi_concurrent_conflict context;
 
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_fl_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);   // true = simulated crash
                                                                  // full logging
}
**/

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


// Test case with simple transactions (1 in-flight)
// one concurrent txn with exact same insert during redo phase
class restart_concurrent_same_insert : public restart_test_base  {
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
        const bool fCrash = test_env->_restart_options->shutdown_mode;
        const int32_t restart_mode = test_env->_restart_options->restart_mode;
        x_btree_scan_result s;
        // No wait in test code, but wait in restart
        // This is to ensure concurrency
        
        if (fCrash && restart_mode < m3_default_restart)
        {    
            W_DO(test_env->begin_xct());
            w_rc_t rc = test_env->btree_insert(_stid, "aa4", "data4");  // Insert same record that was in-flight, should be possible.
                               // Will fail in m2 due to conflict, should succeed in m3 (not immediately).

            if (rc.is_error())
            {
                DBGOUT3(<<"restart_concurrent_same_insert: insert error: " << rc);        

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
                W_DO(test_env->btree_insert(_stid, "aa4", "data4"));
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
            // M3 behavior
            W_DO(test_env->btree_insert_and_commit(_stid, "aa4", "data4"));
        }

        W_DO(test_env->btree_scan(_stid, s));
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

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
