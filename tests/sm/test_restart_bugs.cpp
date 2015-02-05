#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
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

w_rc_t btree_populate_records_local(stid_t &stid,
                                                  bool fCheckPoint,           // Issue checkpointt in the middle of insertion
                                                  test_txn_state_t txnState,  // What to do with the transaction
                                                  bool splitIntoSmallTrans,   // Default: false
                                                                              // Ture if one insertion per transaction
                                                  char keyPrefix)             // Default: '\0'
                                                                              // Use as key prefix is not '\0'
{
    // If split into multiple transactions, caller cannot ask for in-flight transaction state
    if (t_test_txn_in_flight == txnState)
        w_assert1(false == splitIntoSmallTrans);

    // Set the data size is the max_entry_size minus key size
    // because the total size must be smaller than or equal to
    // btree_m::max_entry_size()
    bool isMulti = keyPrefix != '\0';
    const int key_size = isMulti ? 6 : 5;
    const int data_size = btree_m::max_entry_size() - key_size - 1;
//    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
//    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 1 -2;    // 3 records, works, with 2 previous records, 5 total
                                                                                 //       insert aa1
                                                                                 //       insert aa2
                                                                                 //       insert key302
                                                                                 //       page split, moved aa2, key302
                                                                                 //       insert key301
                                                                                 //       insert key300
                                                                                 //       crash
                                                                                 //       5 in_doubt pages, 1 in-flight txn
                                                                                 //       4th page, page index 6 - page rebalance followed by 4 insertions
                                                                                 //            insert aa2
                                                                                 //            insert key302
                                                                                 //            insert key301
                                                                                 //            insert key300
                                                                                 //       rollback on page 6
                                                                                 //            delete key300
                                                                                 //            delete key301
                                                                                 //            delete key300

//    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 1 + 1;
    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 1;



//    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 1 - 1;  // 4 records, core dump, with 2 previous records, 6 total
                                                                                //        insert aa1
                                                                                //        insert aa2
                                                                                //        insert key303
                                                                                //        page split, low fence: +aa2, high: infinite
                                                                                //            move  aa2
                                                                                //            move  key303
                                                                                //        insert key302
                                                                                //        insert key301
                                                                                //        page split, low fence: +key302, high: infinite
                                                                                //            move key302
                                                                                //            move key303
                                                                                //        insert key300
                                                                                //
                                                                                //        3 pages:
                                                                                //            Page 1: aa1
                                                                                //            Page 2: aa2, key300, key301
                                                                                //            Page 3: key302, key303
                                                                                //        System crash - all the key3XX records need to be rollback
                                                                                //
                                                                                //        6 in_doubt pages, 1 in-flight txn
                                                                                //        REDO:
                                                                                //          Page 3 - page format
                                                                                //          Page 4 - page format
                                                                                //          Page 5 - page format, foster adoption (source page: 7, key: +aa2)
                                                                                //          Page 6 - page rebalance (child page) followed by 4 insertions
                                                                                //                                             page rebalance (parent page) again
                                                                                //                                             followed by 2 deletions
                                                                                //                                             and then 1 insertion
                                                                                //              Rebalance - Recovery child page, empty
                                                                                //              insert aa2
                                                                                //              insert key303
                                                                                //              insert key302
                                                                                //              insert key301
                                                                                //              Allocate a new page for page split, page 8
                                                                                //              Rebalance - Recovery parent page, 4 existing records, move 2 of them
                                                                                //              page split, new low aa2, new high key302, delete 2 records
                                                                                //              delete key302 but not found (btree_logrec.cpp, line 354)
                                                                                //              delete key303 but not found (btree_logrec.cpp, line 354)
                                                                                //              insert key300
                                                                                //        Page 7 - page format, foster adoption (source page: 7, key: +aa2)
                                                                                //        Page 8 - page rebalance (child page) followed by 2 insertions
                                                                                //              Rebalance - Recovery child page 8
                                                                                //              insert key302
                                                                                //              insert key303
                                                                                //
                                                                                //      UNDO:
                                                                                //        Page 6:
                                                                                //              delete key300 -- ok
                                                                                //                     failed in btree_impl::_ux_adopt_foster_core() when checking
                                                                                //                     child page consistency: child.is_consistent()
                                                                                //              delete key301 -- not found (btree_impl.cpp, _ux_remove_core),
                                                                                //                                        btree_logrec.cpp (line 68)
                                                                                //                                        It was a generic search of the tree, why did it
                                                                                //                                        failed to find this key?  The incorrect result indicates
                                                                                //                                        the record does exist but we failed to find it
                                                                                //
                                                                                //              delete key302 -- ok
                                                                                //              delete key303 -- ok


//    vec_t data;
    char data_str[data_size+1];
    data_str[data_size] = '\0';
    memset(data_str, 'D', data_size);
    w_keystr_t key;
    char key_str[key_size];
    key_str[0] = 'k';
    key_str[1] = 'e';
    key_str[2] = 'y';
    if(isMulti) key_str[3] = keyPrefix;

    // Insert enough records to ensure page split
    // Multiple transactions with one insertion per transaction

    if(!splitIntoSmallTrans) W_DO(test_env->begin_xct());

    for (int i = 0; i < recordCount; ++i)
    {
        int num;
        num = recordCount - 1 - i;

        key_str[key_size-2] = ('0' + ((num / 10) % 10));
        key_str[key_size-1] = ('0' + (num % 10));

        if (true == fCheckPoint)
        {
            // Take one checkpoint half way through insertions
            if (num == recordCount/2)
                W_DO(ss_m::checkpoint());
        }

        if(splitIntoSmallTrans) {
            W_DO(test_env->begin_xct());
            W_DO(test_env->btree_insert(stid, key_str, data_str));
            if(t_test_txn_commit == txnState)
                W_DO(test_env->commit_xct());
            else if (t_test_txn_abort == txnState)
                W_DO(test_env->abort_xct());
            else
                w_assert1(false);
        }
        else
            W_DO(test_env->btree_insert(stid, key_str, data_str));
    }

    if(!splitIntoSmallTrans) {
        if(t_test_txn_commit == txnState)
            W_DO(test_env->commit_xct());
        else if (t_test_txn_abort == txnState)
            W_DO(test_env->abort_xct());
        else
            ss_m::detach_xct();
    }

    return RCOK;
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

//        W_DO(btree_populate_records_local(_stid_list[0], false, t_test_txn_commit, true, '0'));    // flags: no checkpoint, commit, one transaction per insert, keyPrefix '0'
//        W_DO(btree_populate_records_local(_stid_list[1], false, t_test_txn_commit, false, '1'));   // flags:                        all inserts in one transaction, keyPrefix '1'
//        W_DO(btree_populate_records_local(_stid_list[2], false, t_test_txn_commit, false, '2'));   // flags:                        all inserts in one transaction, keyPrefix '2'

/**/
        // These 2 insertions cause the AV, but 1 insertion works, assertion in 'btree_page_h::_is_consistent_keyorder' from btree_impl::_ux_adopt_foster_core
        const int data_size = btree_m::max_entry_size() - 4;
        vec_t data;
        char data_str[data_size+1];
        data_str[data_size] = '\0';
        memset(data_str, 'D', data_size);
        W_DO(test_env->btree_insert_and_commit(_stid_list[2], "aa1", data_str));
        W_DO(test_env->btree_insert_and_commit(_stid_list[2], "aa2", data_str));
/**/

//       W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
//       W_DO(test_env->btree_insert_and_commit(_stid_list[1], "aa2", "data2"));
        W_DO(btree_populate_records_local(_stid_list[2], false, t_test_txn_in_flight, false, '3'));   // flags: no checkpoint, no commit, one big transaction which will include page split, keyPrefix '3'
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(5);
        int32_t restart_mode = test_env->_restart_options->restart_mode;
        x_btree_scan_result s;

        if(restart_mode < m3_default_restart) {
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
        else    // m3 restart mode, no phases, just take a checkpoint randomly
            W_DO(ss_m::checkpoint());

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

// Not working, see control flow in btree_populate_records_local, need to use the special test which insert 2 records first
// In retail, incorrect result because failed to UNDO an insertion (delete existing) but the actual record is there
// All commented out test cases in test_concurrent_complex are due to this error, the test case below is another repro
/* Not passing, full logging, crash if debug build, incorrect if retail build *
TEST (RestartTest, MultiIndexConcChckptCF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;

    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

class restart_multi_page_inflight_multithrd2 : public restart_test_base
{
public:
    static void t1Run (stid_t* stid_list) {
        w_rc_t rc = test_env->btree_populate_records(stid_list[0], false, t_test_txn_in_flight, false, '1'); // flags: no checkpoint, don't commit, one big transaction
//        w_rc_t rc = btree_populate_records_local(stid_list[0], false, t_test_txn_in_flight, false, '1'); // flags: no checkpoint, don't commit, one big transaction
        EXPECT_FALSE(rc.is_error());
    }

    static void t2Run (stid_t* stid_list) {
        w_rc_t rc = test_env->btree_populate_records(stid_list[0], false, t_test_txn_in_flight, false, '2'); // flags: no checkpoint, don't commit, one big transaction
//        w_rc_t rc = btree_populate_records_local(stid_list[0], false, t_test_txn_in_flight, false, '2'); // flags: no checkpoint, don't commit, one big transaction
        EXPECT_FALSE(rc.is_error());
    }

    static void t3Run (stid_t* stid_list) {
       w_rc_t rc = test_env->btree_populate_records(stid_list[0], true, t_test_txn_in_flight, false, '3'); // flags: checkpoint, don't commit, one big transaction
//        rc = btree_populate_records_local(stid_list[0], true, t_test_txn_in_flight, false, '3'); // flags: checkpoint, don't commit, one big transaction
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


/* this is the same error as above test case */
/* if running in debug: assertion in 'btree_page_h::_is_consistent_keyorder' from btree_impl::_ux_adopt_foster_core */
/* if running in retail: incorrect result */
/* Not passing, full logging - incorrect result, want 0 but got some*
TEST (RestartTest, MultiPageInFlightMultithrdCF) {
    test_env->empty_logdata_dir();
    restart_multi_page_inflight_multithrd2 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart; // full logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/


/* Passing - M3 *
TEST (RestartTest, MultiPageInFlightMultithrdN3) {
    test_env->empty_logdata_dir();
    restart_multi_page_inflight_multithrd2 context;

    restart_test_options options;
    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m3_default_restart; // minimal logging, nothing to recover
                                               // but go through Log Analysis backward scan loop and
                                               // process log records
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true), 0); // use_locks
}
**/

/* Passing - M3 *
TEST (RestartTest, MultiPageInFlightMultithrdC3) {
    test_env->empty_logdata_dir();
    restart_multi_page_inflight_multithrd2 context;

    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, scan query triggers on_demand recovery
                                               // No delay because no restart child thread
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true), 0);  // use_locks
}
**/

class restart_concurrent_same_insert2 : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit, true, '1'));   // flags: No checkpoint, commit, one transaction per insert
//        W_DO(btree_populate_records_local(_stid_list[0], false, t_test_txn_commit, true, '1'));   // flags: No checkpoint, commit, one transaction per insert

        // Enough inserts to cause more page split
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq2", "data2"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq7", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq9", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq0", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq33", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq77", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq22", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq99", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq55", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "qq1", "data1"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "qq4", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        const bool fCrash = test_env->_restart_options->shutdown_mode;
        const int32_t restart_mode = test_env->_restart_options->restart_mode;
        x_btree_scan_result s;
        // No wait in test code, but wait in restart if M2
        // This is to ensure concurrency

        if (fCrash && restart_mode < m3_default_restart)
        {
            // M2
            w_rc_t rc = test_env->btree_insert_and_commit(_stid_list[0], "qq1", "data1");  // Should error on duplicate
            // Will fail in m2 due to conflict, should succeed in m3 (not immediately).

            if (rc.is_error())
            {
                // Expected failure
                DBGOUT3(<<"restart_concurrent_same_insert: insert failed: " << rc);

                // Abort the failed scan txn
                test_env->abort_xct();

                // Sleep to give Recovery sufficient time to finish
                while (true == test_env->in_restart())
                {
                    // Concurrent restart is still going on, wait
                    ::usleep(WAIT_TIME);
                }

                // Try again, should work now
                rc = test_env->btree_insert_and_commit(_stid_list[0], "qq1", "data1");  // should error on duplicate
                if (rc.is_error())
                {
                    if (34 == rc.err_num())
                    {
                        // Correct behavior
                    }
                    else
                    {
                        // Unexpected error
                        std::cerr << "Unexpected error on insertion, error:" << rc.get_message() << std::endl;
                        return RC(eINTERNAL);
                    }
                }
                else
                {
                    // Should not succeed
                    std::cerr << "Insertion of duplicated key should not succeed" << std::endl;
                    return RC(eINTERNAL);
                }
            }
            else
            {
                std::cerr << "restart_concurrent_same_insert: insert operation should not succeed"<< std::endl;
                return RC(eINTERNAL);
            }
        }
        else
        {
            // M3 behavior, either normal or crash shutdown
            // blocking and insertion should succeed
            w_rc_t rc = test_env->btree_insert_and_commit(_stid_list[0], "qq1", "data1");  // Should error on duplicate
            if (rc.is_error())
            {
                if (34 == rc.err_num())
                {
                    // Correct behavior
                }
                else
                {
                    // Unexpected error
                    std::cerr << "Unexpected error on insertion, error:" << rc.get_message() << std::endl;
                    return RC(eINTERNAL);
                }
            }
            else
            {
                // Should not succeed
                std::cerr << "Insertion of duplicated key should not succeed" << std::endl;
                return RC(eINTERNAL);
            }
        }

        W_DO(test_env->btree_scan(_stid_list[0], s));
        int recordCount = 0;
        recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;  // Count through big population
        recordCount += 11;
        EXPECT_EQ (std::string("qq99"), s.maxkey);
        EXPECT_EQ (std::string("key100"), s.minkey);
        EXPECT_EQ (recordCount, s.rownum);
        return RCOK;
    }
};

/* Passing - M2 *
TEST (RestartTest, ConcurrentSameInsertC) {
    test_env->empty_logdata_dir();
    restart_concurrent_same_insert2 context;

    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_redo_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* Passing - M3 *
TEST (RestartTest, ConcurrentSameInsertC3) {
    test_env->empty_logdata_dir();
    restart_concurrent_same_insert2 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, insert triggers on_demand recovery
                                               // No delay because no restart child thread
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true ), 0);   // use_locks
}
**/

class restart_concurrent_no_conflict2 : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        // Multiple committed transactions with many pages
        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit, true, '1'));   // flags: No checkpoint, commit, one transaction per insert
//        W_DO(btree_populate_records_local(_stid_list[0], false, t_test_txn_commit, true, '1'));	// flags: No checkpoint, commit, one transaction per insert

// TODO(Restart)... Why the above test passing and this one does not when using extended minimal logging with Single Page Recovery????
//                          Insert to the beginning fails, while append to the end succeeds

        // Issue a checkpoint to make sure these committed txns are flushed
        W_DO(ss_m::checkpoint());

        // Now insert more records, these records are at the beginning of B-tree
        // therefore if these records cause a page rebalance, it would be in the parent page
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa2", "data2"));

        W_DO(test_env->begin_xct());
        W_DO(test_env->btree_insert(_stid_list[0], "aa4", "data4"));             // in-flight

        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        int32_t restart_mode = test_env->_restart_options->restart_mode;

        if (restart_mode < m3_default_restart)
        {
            // If not in M3, wait a while, this is to give REDO a chance to reload the root page
            // but still wait in REDO phase due to test mode
            ::usleep(SHORT_WAIT_TIME*5);
        }

        // Wait in restart both REDO and UNDO, this is to ensure
        // user transaction encounter concurrent restart
        // Insert into the first page, depending on how far the REDO goes,
        // the insertion might or might not succeed
        W_DO(test_env->begin_xct());
        w_rc_t rc = test_env->btree_insert(_stid_list[0], "aa7", "data4");
        if (rc.is_error())
        {
            // Conflict, it should not happen if M3
            // In M2 restart mode using commit_lsn, conflict is possible if
            // the entire buffer pool was never flushed, the commit_lsn would
            // be the earliest LSN, therefore it blocks all user transactions
            if (restart_mode < m3_default_restart)
            {
                // M2
                DBGOUT3(<<"restart_concurrent_no_conflict: tree_insertion conflict");
                W_DO(test_env->abort_xct());
            }
            else
            {
                // M3, insert should succeed
                DBGOUT3(<<"restart_concurrent_no_conflict: tree_insertion conflict in M3, un-expected error!!!");
                EXPECT_FALSE(rc.is_error());   // It should trigger a rollback and record insertion should succeed
            }
        }
        else
        {
            // Succeed
            DBGOUT3(<<"restart_concurrent_no_conflict: tree_insertion succeeded");
            W_DO(test_env->commit_xct());
        }

        // Wait before the final verfication
        // Note this 'in_restart' check is not reliable if on_demand restart (m3),
        // but it is okay because with on_demand restart, it blocks concurrent
        // transactions instead of failing concurrent transactions
        if (restart_mode < m3_default_restart)
        {
            // Not wait if M3 or M4
            while (true == test_env->in_restart())
            {
                // Concurrent restart is still going on, wait
                ::usleep(WAIT_TIME);
            }
        }

        // Verify
        W_DO(test_env->btree_scan(_stid_list[0], s));
        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 5;
        recordCount += 3;  // Count after checkpoint
//        int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 1+1;
//        recordCount += 1;  // Count after checkpoint

        if (!rc.is_error())
            recordCount += 1;  // Count after concurrent insert

        EXPECT_EQ (recordCount, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);

        return RCOK;
    }
};

/* Passing, minimal logging *
TEST (RestartTest, ConcurrentNoConflictC) {
    test_env->empty_logdata_dir();
    restart_concurrent_no_conflict2 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_both_delay_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

/* Passing - M3 *
TEST (RestartTest, ConcurrentNoConflictC3) {
    test_env->empty_logdata_dir();
    restart_concurrent_no_conflict2 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m3_default_restart; // minimal logging, insert triggers on_demand recovery
                                               // No delay because no restart child thread
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true), 0);  // use_locks
}
**/

class restart_multi_page_in_flight2 : public restart_test_base  {
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));
        output_durable_lsn(2);

        // One big uncommitted txn
        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_in_flight, false, '1'));  // false: No checkpoint; false: Do not commit, in-flight
//        W_DO(btree_populate_records_local(_stid_list[0], false, t_test_txn_in_flight, false, '1'));  // flags: No checkpoint, commit

        output_durable_lsn(3);

        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        int32_t restart_mode = test_env->_restart_options->restart_mode;
        w_rc_t rc;

        // Wait before the final verfication
        // Note this 'in_restart' check is not reliable if on_demand restart (m3),
        // but it is okay because with on_demand restart, it blocks concurrent
        // transactions instead of failing concurrent transactions
        if (restart_mode < m3_default_restart)
        {
            // Not wait if M3 or M4
            while (true == test_env->in_restart())
            {
                // Concurrent restart is still going on, wait
                ::usleep(WAIT_TIME);
            }
        }

        // Verify, if M3, the insert query triggers on_demand REDO (page loading)
        // and UNDO (transaction rollback)

        // Both normal and crash shutdowns, regardless Instart Restart milestone,
        // the insert should succeed due to in-flight transaction rolled back alreadly
        rc = test_env->btree_insert_and_commit(_stid_list[0], "aa4", "dataXXX");
        if (rc.is_error())
        {
            std::cout << "Insertion failed, not expected behavior" << std::endl;
            EXPECT_TRUE(rc.is_error());   // In M3, insert operation should trigger UNDO
        }
        else
        {
            std::cout << "Insertion succeed, this is expected behavior" << std::endl;
        }

        // Verify
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (1, s.rownum);
        return RCOK;
    }
};

/* Passing, minimal logging */
TEST (RestartTest, MultiPageInFlightC) {
    test_env->empty_logdata_dir();
    restart_multi_page_in_flight2 context;
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_default_restart; // minimal logging
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
/**/

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
