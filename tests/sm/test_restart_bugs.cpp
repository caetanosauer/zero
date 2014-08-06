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


// Test case with more than one page of data (1 in-flight), one concurrent txn to
// access a non-dirty page so it should be allowed
class restart_concurrent_no_conflict : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

        // Multiple committed transactions with many pages
        W_DO(test_env->btree_populate_records(_stid, false, true));  // flags: No checkpoint, commit


        // Issue a checkpoint to make sure these committed txns are flushed
// If enabled, getting 'd > 0' error, low fence key
// Note that checkpoint flush the recovery log, it causes problems only if page split before checkpoint
// and need one statement after 'asynch' checkpoint, did the checkpoint finished?
        W_DO(ss_m::checkpoint());         

        // Now insert more records, these records are at the beginning of B-tree
        // therefore if these records cause a page rebalance, it would be in the parent page
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
//        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
//        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));

// If only this checkpoint, test is passing
// W_DO(ss_m::checkpoint());

//        W_DO(test_env->begin_xct());
//        W_DO(test_env->btree_insert(_stid, "aa4", "data4"));             // in-flight

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
            cerr << "restart_concurrent_no_conflict: tree_insertion failed"<< endl;
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

        // Verify
        W_DO(test_env->btree_scan(_stid, s));

//        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;  // Count before checkpoint
        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 1;

        recordCount += 3;  // Count after checkpoint
        if (!rc.is_error())        
            recordCount += 1;  // Count after concurrent insert

        EXPECT_EQ (recordCount, s.rownum);
        if (!rc.is_error())
            EXPECT_EQ (std::string("aa1"), s.minkey);

        return RCOK;
    }
};

/* Failing: bfull logging, btree_impl_search.cpp:303, d > 0 *
TEST (RestartTest, ConcurrentNoConflictCF) {
    test_env->empty_logdata_dir();
    restart_concurrent_no_conflict context;
    restart_test_options options;
	options.shutdown_mode = simulated_crash;
	options.restart_mode = m2_both_fl_delay_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
