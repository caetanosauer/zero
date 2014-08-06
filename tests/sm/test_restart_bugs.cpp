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


// Test case with 1 transaction (in-flight with more than one page of data)
// no concurrent activities during restart
class restart_multi_page_in_flight : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);

       // One big uncommitted txn
        W_DO(test_env->btree_populate_records(_stid, false, false));  // flags: No checkpoint, don't commit
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

        // Verify
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (0, s.rownum);
        return RCOK;
    }
};

/* Passing *
TEST (RestartTest, MultiPageInFlightN) {
    test_env->empty_logdata_dir();
    restart_multi_page_in_flight context;
    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* Issue is related to page split, if reduce the size of record so no page split, then it works fine */
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
/*    is returning eGOODRETRY and infinite loop, need further investigation */
/* Issue is related to page split, if reduce the size of record so no page split, then it works fine */
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


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
