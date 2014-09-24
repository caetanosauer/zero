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
//       Multiple threads
//       Multiple index
// Depending on the restart mode, the test results might vary and therefore tricky

lsn_t get_durable_lsn() {
    lsn_t ret;
    ss_m::get_durable_lsn(ret);
    return ret;
}
void output_durable_lsn(int W_IFDEBUG1(num)) {
    DBGOUT1( << num << ".durable LSN=" << get_durable_lsn());
}

class restart_concurrent_chckpt_multi_index : public restart_test_base {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[3];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[1], _root_pid));
        output_durable_lsn(3);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[2], _root_pid));
        output_durable_lsn(4);

        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit, true, '0'));    // flags: no checkpoint, commit, one transaction per insert, keyPrefix '0'
        W_DO(test_env->btree_populate_records(_stid_list[1], false, t_test_txn_commit, false, '1'));   // flags:                        all inserts in one transaction, keyPrefix '1'
        W_DO(test_env->btree_populate_records(_stid_list[2], false, t_test_txn_commit, false, '2'));   // flags:                        all inserts in one transaction, keyPrefix '2'

        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[1], "aa2", "data2"));
        W_DO(test_env->btree_populate_records(_stid_list[2], false, t_test_txn_in_flight, false, '3'));   // flags: no checkpoint, no commit, one big transaction which will include page split, keyPrefix '3'
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(5);
        int32_t restart_mode = test_env->_restart_options->restart_mode;
        x_btree_scan_result s;

        if(restart_mode < m3_default_restart) 
        {
            // M2
            if(restart_mode == m2_redo_delay_restart || restart_mode == m2_redo_fl_delay_restart 
                || restart_mode == m2_both_delay_restart || restart_mode == m2_both_fl_delay_restart) // Check if redo delay has been set in order to take a checkpoint
            {
                if(ss_m::in_REDO() == t_restart_phase_active) // Just a sanity check that the redo phase is truly active
                    W_DO(ss_m::checkpoint());
            }
            
            if(restart_mode == m2_undo_delay_restart || restart_mode == m2_undo_fl_delay_restart
                || restart_mode == m2_both_delay_restart || restart_mode == m2_both_fl_delay_restart) // Check if undo delay has been set in order to take a checkpoint
            {
                while(ss_m::in_UNDO() == t_restart_phase_not_active) // Wait until undo phase is starting
                    ::usleep(SHORT_WAIT_TIME);
                if(ss_m::in_UNDO() == t_restart_phase_active) // Sanity check that undo is really active (instead of over) 
                    W_DO(ss_m::checkpoint());
            }
            
            while(ss_m::in_restart()) // Wait while restart is going on
                ::usleep(WAIT_TIME); 
        }
        else 
        {
            // m3 restart mode, no phases, just take a checkpoint randomly
            W_DO(ss_m::checkpoint());
        }
        
        output_durable_lsn(6);
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;         
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ(recordCount+1, s.rownum);
        EXPECT_EQ(std::string("aa1"), s.minkey);
        
        W_DO(test_env->btree_scan(_stid_list[1], s));
        EXPECT_EQ(recordCount+1, s.rownum);
        EXPECT_EQ(std::string("aa2"), s.minkey);
        
        W_DO(test_env->btree_scan(_stid_list[2], s));
        EXPECT_EQ(recordCount, s.rownum);
        EXPECT_EQ(std::string("key200"), s.minkey);
        

        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultiIndexConcChckptN) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiIndexConcChckptC) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiIndexConcChckptNF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_full_logging_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* core dump if crash with in-flight multiple statements, including page split 
btree_insert_log::undo() - undo an insert operation by delete it, but record not found */
/* Not passing, full logging *
TEST (RestartTest, MultiIndexConcChckptCF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* Passing */
TEST (RestartTest, MultiIndexConcChckptNR) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiIndexConcChckptCR) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiIndexConcChckptNRF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* core dump if crash with in-flight multiple statements, including page split 
btree_insert_log::undo() - undo an insert operation by delete it, but record not found */
/* Not passing, full logging *
TEST (RestartTest, MultiIndexConcChckptCRF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* Passing */
TEST (RestartTest, MultiIndexConcChckptNU) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_undo_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiIndexConcChckptCU) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_undo_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiIndexConcChckptNUF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_undo_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* core dump if crash with in-flight multiple statements, including page split 
btree_insert_log::undo() - undo an insert operation by delete it, but record not found */
/* Not passing, full logging *
TEST (RestartTest, MultiIndexConcChckptCUF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* Passing */
TEST (RestartTest, MultiIndexConcChckptNB) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiIndexConcChckptCB) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiIndexConcChckptNBF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* core dump if crash with in-flight multiple statements, including page split 
btree_insert_log::undo() - undo an insert operation by delete it, but record not found */
/* Not passing, full logging *
TEST (RestartTest, MultiIndexConcChckptCBF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

// Test case that populates 3 indexes with committed records and one of them with some in-flights before shutdown
// After shutdown, concurrent transactions are executed to test the rejection logic for concurrent transactions
// Test case is suitable for calls with many different options, 20 in total 
class restart_concurrent_trans_multi_index : public restart_test_base {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[3];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[1], _root_pid));
        output_durable_lsn(3);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[2], _root_pid));
        output_durable_lsn(4);

        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit, true, '0'));    // flags: no checkpoint, commit, one transaction per insert, keyPrefix '0'
        W_DO(test_env->btree_populate_records(_stid_list[1], false, t_test_txn_commit, false, '1'));   // flags:                        all inserts in one transaction, keyPrefix '1'
        W_DO(test_env->btree_populate_records(_stid_list[2], false, t_test_txn_commit, false, '2'));   // flags:                        all inserts in one transaction, keyPrefix '2'

        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[1], "aa2", "data2"));

// TODO(Restart)... need to enable the following call which populate the index with many reocrds and page split in one transaction, in-flight transaction
        // W_DO(test_env->btree_populate_records(_stid_list[2], false, t_test_txn_in_flight, false, '3'));  // flags: no checkpoint, no commit, one big transaction which cause page split, keyPrefix '3'
        
        W_DO(ss_m::checkpoint());
        W_DO(test_env->begin_xct());                                                         // Just do the one in-flight insertion that is needed for post_shutdown verification
        W_DO(test_env->btree_insert(_stid_list[2], "key300", "D"));
        W_DO(test_env->btree_insert(_stid_list[2], "key301", "D"));
        W_DO(test_env->btree_update(_stid_list[2], "key223", "A"));
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(5);
        int32_t restart_mode = test_env->_restart_options->restart_mode;
        bool crash_shutdown = test_env->_restart_options->shutdown_mode;  // false = normal shutdown
        x_btree_scan_result s;
        bool insert_occurred = false;
        w_rc_t rc;
        bool redo_delay = restart_mode == m2_redo_delay_restart || restart_mode == m2_redo_fl_delay_restart 
                || restart_mode == m2_both_delay_restart || restart_mode == m2_both_fl_delay_restart;
        bool undo_delay = restart_mode == m2_undo_delay_restart || restart_mode == m2_undo_fl_delay_restart 
                || restart_mode == m2_both_delay_restart || restart_mode == m2_both_fl_delay_restart;
        
        // Wait a while, this is to give REDO a chance to reload the root page
        // but still wait in REDO phase due to test mode
        ::usleep(SHORT_WAIT_TIME*5);
        
        if(restart_mode < m3_default_restart) 
        {
            // M2
            if (true == crash_shutdown)
            {
                // If crahsed shutdown, try to cause some concurrent transactions
                if(redo_delay) // Check if redo delay has been set in order to take a checkpoint
                {
                    if(ss_m::in_REDO() == t_restart_phase_active) // Just a sanity check that the redo phase is truly active
                    {                   
                        rc = test_env->btree_insert_and_commit(_stid_list[0], "key0181", "data0"); 
                        // Although there is no existing key "key0181", this should raise a conflict, because it would have to be inserted 
                        // in the fourth page, which is still dirty
                        EXPECT_TRUE(rc.is_error());
                    
                        if(test_env->_restart_options->enable_checkpoints)
                            W_DO(ss_m::checkpoint());

                        rc = test_env->btree_update_and_commit(_stid_list[1], "key110", "A");
                        EXPECT_TRUE(rc.is_error());
                    }
                }
                if(undo_delay) // Check if undo delay has been set in order to take a checkpoint
                {
                    while(ss_m::in_UNDO() == t_restart_phase_not_active) // Wait until undo phase is starting
                        ::usleep(SHORT_WAIT_TIME);
                    if(ss_m::in_UNDO() == t_restart_phase_active) // Sanity check that undo is really active (instead of over) 
                    {
                        rc = test_env->btree_update_and_commit(_stid_list[2], "key300", "C"); // Does not make sure that the error is due to rejection by undo logic
                        EXPECT_TRUE(rc.is_error());                                           // Could also just be that undo is complete and the record thus doesn't exist anymore
                    
                        // This is failing, it seems that the in-flights have already been undone and therefore all pages are accessible, test cases have been commented out
                        rc = test_env->btree_insert_and_commit(_stid_list[2], "zz1", "data1"); //  This record would be inserted into the last page (which has records belonging to in-flight transactions)
                        EXPECT_TRUE(rc.is_error());                                            //  and should therefore be aborted
                
                        rc = test_env->btree_insert_and_commit(_stid_list[2], "aa0", "data0"); // This should succeed, as all records in page 1 have been redone and there are no in-flights
                                                                                               // but if the entire b-tree was never flushed to disk before the system crash
                                                                                               // then commit_lsn would block all operations until the end of Restart operation and
                                                                                               // this operation would fail, so the result here is not really determiistic based on timing
                                                                                               // and data situation before crash
                        // Insert succeeded
                        if (!rc.is_error())
                            insert_occurred = true;
                                                                                               
                    }
                }
            }
            else
            {
                // If normal shutdown, REDO and UNDO phases should be very brief and no real operation
            }

            while(ss_m::in_restart()) // Wait while restart is going on
                ::usleep(WAIT_TIME); 
        }
        else
        {
            // m3 restart mode, everything should succeed
            W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa0", "data0"));
            W_DO(test_env->btree_update_and_commit(_stid_list[1], "key110", "A"));
            W_DO(test_env->btree_insert_and_commit(_stid_list[2], "key300", "data0")); 
        }
        output_durable_lsn(6);
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;         
        
        // Check index 0
        W_DO(test_env->btree_scan(_stid_list[0], s));
        if(restart_mode < m3_default_restart) 
        {
            // M2
            EXPECT_EQ(std::string("aa1"), s.minkey);
            EXPECT_EQ(recordCount+1, s.rownum);
        }
        else 
        {
            // M3
            EXPECT_EQ(std::string("aa0"), s.minkey);
            EXPECT_EQ(recordCount+2, s.rownum);
        }

        // Check index 1
        W_DO(test_env->btree_scan(_stid_list[1], s));
        EXPECT_EQ(recordCount+1, s.rownum);
        EXPECT_EQ(std::string("aa2"), s.minkey);
        std::string actual;

        // Data portion, because we are using prefix in btree_populate_records() call
        // so the key size is 6 (instead of 5)
        // the data size: btree_m::max_entry_size() - key_size - 1 = 7
        char expected[btree_m::max_entry_size()-7];
        memset(expected, 'D', btree_m::max_entry_size()-7);

        W_DO(test_env->btree_lookup_and_commit(_stid_list[1], "key110", actual));
        if(restart_mode > m3_default_restart)
        {
            // M3
            EXPECT_EQ(std::string("A"), actual);
        }
        else
        {
            // M2
            EXPECT_EQ(std::string(expected, btree_m::max_entry_size()-7), actual);
        }
        // Check index 2
        W_DO(test_env->btree_scan(_stid_list[2], s));
        if(restart_mode < m3_default_restart)
        {
            // M2, because some concurrent transaction might fail so the result set might be different
            if(s.maxkey.length()==6) // Make sure "zz1" hasn't been submitted by accident (at would get out of bounds). If so, the check in pre_shutdown will have failed.
                EXPECT_EQ('2', s.maxkey.at(3));
            if ((true == crash_shutdown) && (undo_delay) && (true == insert_occurred))
            {
                EXPECT_EQ(std::string("aa0"), s.minkey);
                EXPECT_EQ(recordCount+1, s.rownum);
            }
            else
            {
                // Normal shutdown or 
                // Crash shutdown but no delay in undo phase, so we did not insert a record with 'aa0'
                EXPECT_EQ(std::string("key200"), s.minkey);
                EXPECT_EQ(recordCount, s.rownum);
            }
        }
        else
        {
            // m3
            EXPECT_EQ(recordCount+1, s.rownum);
            EXPECT_EQ(std::string("key200"), s.minkey);
            EXPECT_EQ(std::string("key300"), s.maxkey); 
        }

        return RCOK;
    }
};

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransNR) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiIndexConcTransCR) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransNRF) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiIndexConcTransCRF) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransNU) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_undo_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransCU) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_undo_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransNUF) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_undo_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransCUF) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransNB) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransCB) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransNBF) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransCBF) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransChckptNR) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_delay_restart;
    options.enable_checkpoints = true;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiIndexConcTransChckptCR) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_delay_restart;
    options.enable_checkpoints = true;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransChckptNRF) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_fl_delay_restart;
    options.enable_checkpoints = true;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiIndexConcTransChckptCRF) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart;
    options.enable_checkpoints = true;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/


/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransChckptNB) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_delay_restart;
    options.enable_checkpoints = true;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransChckptCB) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_delay_restart;
    options.enable_checkpoints = true;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransChckptNBF) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_fl_delay_restart;
    options.enable_checkpoints = true;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing - ZERO-184 */
TEST (RestartTest, MultiIndexConcTransChckptCBF) {
    test_env->empty_logdata_dir();
    restart_concurrent_trans_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_fl_delay_restart;
    options.enable_checkpoints = true;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

class restart_multi_page_inflight_multithrd : public restart_test_base
{
public:
    static void t1Run (stid_t* stid_list) {
        w_rc_t rc = test_env->btree_populate_records(stid_list[0], false, t_test_txn_in_flight, false, '1'); // flags: no checkpoint, don't commit, one big transaction
        EXPECT_FALSE(rc.is_error());
    }

    static void t2Run (stid_t* stid_list) {
        w_rc_t rc = test_env->btree_populate_records(stid_list[0], false, t_test_txn_in_flight, false, '2'); // flags: no checkpoint, don't commit, one big transaction
        EXPECT_FALSE(rc.is_error());
    }

    static void t3Run (stid_t* stid_list) {
        w_rc_t rc = test_env->btree_populate_records(stid_list[0], true, t_test_txn_in_flight, false, '3'); // flags: checkpoint, don't commit, one big transaction
        EXPECT_FALSE(rc.is_error());
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
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

        // Wait before the final verfication
        // Note this 'in_restart' check is not reliable if on_demand restart (m3),
        // but it is okay because with on_demand restart, it blocks concurrent 
        // transactions instead of failing concurrent transactions       
        while(ss_m::in_restart()) // Wait while restart is going on
            ::usleep(WAIT_TIME); 

        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ(0, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, MultiPageInFlightMultithrdN) {
    test_env->empty_logdata_dir();
    restart_multi_page_inflight_multithrd context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiPageInFlightMultithrdNF) {
    test_env->empty_logdata_dir();
    restart_multi_page_inflight_multithrd context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, MultiPageInFlightMultithrdC) {
    test_env->empty_logdata_dir();
    restart_multi_page_inflight_multithrd context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Failing - see Jira issue ZERO-186 */
/* core dump if crash with in-flight multiple statements, including page split 
btree_insert_log::undo() - undo an insert operation by delete it, but record not found */
/* Not passing, full logging *
TEST (RestartTest, MultiPageInFlightMultithrdCF) {
    test_env->empty_logdata_dir();
    restart_multi_page_inflight_multithrd context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

class restart_many_conflicts_multithrd : public restart_test_base {
public:
    static void t1Run(stid_t* stid_list) {
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;         
        char key_str[7] = "key100";
        char data_str[8] = "data100";

        for (int i = 0; i<recordCount; i++) {
            key_str[4] = ('0' + ((i / 10) % 10));
            key_str[5] = ('0' + (i % 10));
            data_str[5] = key_str[4];
            data_str[6] = key_str[5];
            w_rc_t rc = test_env->btree_update_and_commit(stid_list[0], key_str, data_str);
            EXPECT_TRUE(rc.is_error());
        }
    }

    static void t2Run(stid_t* stid_list) {
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;         
        char key_str[7] = "key200";
        char data_str[8] = "data200";

        for (int i = 0; i<recordCount; i++) {
            key_str[4] = ('0' + ((i / 10) % 10));
            key_str[5] = ('0' + (i % 10));
            data_str[5] = key_str[4];
            data_str[6] = key_str[5];
            w_rc_t rc = test_env->btree_update_and_commit(stid_list[0], key_str, data_str);
            EXPECT_TRUE(rc.is_error());
        }
    }

    static void t3Run(stid_t* stid_list) {
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;         
        char key_str[7] = "key300";
        char data_str[8] = "data300";
        w_rc_t rc;
        for (int i = 0; i<recordCount; i++) {
            key_str[4] = ('0' + ((i / 10) % 10));
            key_str[5] = ('0' + (i % 10));
            data_str[5] = key_str[4];
            data_str[6] = key_str[5];
            rc = test_env->btree_update_and_commit(stid_list[0], key_str, data_str);
            EXPECT_FALSE(rc.is_error());
        }
    }

    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        _stid_list = new stid_t[1];
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit, false, '1')); // flags: no checkpoint, commit, one big transaction
        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit, false, '2')); // flags: no checkpoint, commit, one big transaction
        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit, false, '3')); // flags: no checkpoint, commit, one big transaction
        output_durable_lsn(3);
        return RCOK;
    }
    
    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        
        const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;         
        int32_t restart_mode = test_env->_restart_options->restart_mode;
        bool crash_shutdown = test_env->_restart_options->shutdown_mode;   // false if normal shutdown
        bool redo_delay = restart_mode == m2_redo_delay_restart || restart_mode == m2_redo_fl_delay_restart 
                || restart_mode == m2_both_delay_restart || restart_mode == m2_both_fl_delay_restart;
        bool undo_delay = restart_mode == m2_undo_delay_restart || restart_mode == m2_undo_fl_delay_restart 
                || restart_mode == m2_both_delay_restart || restart_mode == m2_both_fl_delay_restart;

        // Wait a while, this is to give REDO a chance to reload the root page
        // but still wait in REDO phase due to test mode
        ::usleep(SHORT_WAIT_TIME*5);
        output_durable_lsn(5);

        if(restart_mode < m3_default_restart) 
        {
            // M2
            if (true == crash_shutdown)
            {
                // Fork the child threads if crash shutdown
                // because if normal shutdown, there is no recovery work
                if(redo_delay) 
                {
                    transact_thread_t t1 (_stid_list, t1Run);
                    transact_thread_t t2 (_stid_list, t2Run);
                    if(ss_m::in_REDO() == t_restart_phase_active) { // Just a sanity check that the redo phase is truly active
                        W_DO(t1.fork());   // Start thread 1
                        W_DO(t2.fork());   // Start thread 2
                        W_DO(t1.join());   // Stop thread 1
                        W_DO(t2.join());   // Stop thread 2
                    }
                }
                if(undo_delay) 
                {
                    transact_thread_t t3 (_stid_list, t3Run);
                    // Do not come here for a normal shutdown 
                    // becuase this state does not happen and the loop will wait forever
                    while(ss_m::in_UNDO() == t_restart_phase_not_active) // Wait until undo phase is starting
                        ::usleep(SHORT_WAIT_TIME);
                    W_DO(t3.fork());  // Start thread 3
                    W_DO(t3.join());  // Stop thread 3
                }
            }
            while(ss_m::in_restart()) // Wait while restart is going on
                ::usleep(WAIT_TIME); 
        }
        else 
        {
            // M3, concurrent transactions should block and then succeed
            char key_str[7] = "key300";
            char data_str[8] = "data300";

            for (int i = 0; i<recordCount; i++) {
                key_str[4] = ('0' + ((i / 10) % 10));
                key_str[5] = ('0' + (i % 10));
                data_str[5] = key_str[4];
                data_str[6] = key_str[5];
                W_DO(test_env->btree_update_and_commit(_stid_list[0], key_str, data_str));
            }
        }

        output_durable_lsn(6);
        x_btree_scan_result s;

        // If M2, the restart finished already
        // If M3, concurrent transaction should be blocked and then succeed
        // In other words, the following query should succeed anyway
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ(3*recordCount, s.rownum);
        EXPECT_EQ(std::string("key100"), s.minkey);
        EXPECT_EQ(std::string("key324"), s.maxkey);
        
        std::string actual;
        char expected[btree_m::max_entry_size()-6];
        memset(expected, 'D', btree_m::max_entry_size()-7);
        expected[btree_m::max_entry_size()-7] = '\0';
        W_DO(test_env->btree_lookup_and_commit(_stid_list[0], "key300", actual));
        if((undo_delay && (true == crash_shutdown)) || restart_mode >= m3_default_restart) 
        {
            // M3 or M2 with delay undo on crash shutdown
            EXPECT_EQ(std::string("data300"), actual);
        }
        else
        {
            // M2
            EXPECT_EQ(std::string(expected), actual);
        }
        return RCOK;
    }
};

/* Passing */
TEST (RestartTest, ManyConflictsMultithrdN) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ManyConflictsMultithrdNF) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ManyConflictsMultihthrdC) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Failing - see Jira issue ZERO-186*/
/* core dump if crash with in-flight multiple statements, including page split 
btree_insert_log::undo() - undo an insert operation by delete it, but record not found */
/* Not passing, full logging *
TEST (RestartTest, ManyConflictsMultithrdCF) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* Passing */
TEST (RestartTest, ManyConflictsMultithrdNR) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Occasionally failing because of a assertion fail in btree_impl_split:335 *
TEST (RestartTest, ManyConflictsMultithrdCR) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* Passing */
TEST (RestartTest, ManyConflictsMultithrdNRF) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_redo_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* core dump if crash with in-flight multiple statements, including page split 
btree_insert_log::undo() - undo an insert operation by delete it, but record not found */
/* Not passing, full logging *
TEST (RestartTest, ManyConflictsMultithrdCRF) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* Passing */
TEST (RestartTest, ManyConflictsMultithrdNU) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_undo_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ManyConflictsMultithrdCU) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_undo_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ManyConflictsMultithrdNUF) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_undo_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* core dump if crash with in-flight multiple statements, including page split 
btree_insert_log::undo() - undo an insert operation by delete it, but record not found */
/* Not passing, full logging *
TEST (RestartTest, ManyConflictsMultithrdCUF) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* Passing */
TEST (RestartTest, ManyConflictsMultithrdNB) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ManyConflictsMultithrdCB) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* Passing */
TEST (RestartTest, ManyConflictsMultithrdNBF) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_both_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

/* core dump if crash with in-flight multiple statements, including page split 
btree_insert_log::undo() - undo an insert operation by delete it, but record not found */
/* Not passing, full logging *
TEST (RestartTest, ManyConflictsMultithrdCBF) {
    test_env->empty_logdata_dir();
    restart_many_conflicts_multithrd context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_fl_delay_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
