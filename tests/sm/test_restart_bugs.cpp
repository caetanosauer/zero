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
                                                                                 
    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 1 - 1;  // 4 records, core dump, with 2 previous records, 6 total
                                                                                //        insert aa1
                                                                                //        insert aa2
                                                                                //        insert key303
                                                                                //        page split, moved aa2, key303
                                                                                //        insert key302
                                                                                //        insert key301
                                                                                //        page split, moved key302, key303
                                                                                //        insert key300
                                                                                //        3 pages:
                                                                                //            Page 1: aa1
                                                                                //            Page 2: aa2, key300, key301
                                                                                //            Page 3: key302, key303
                                                                                //        System crash
                                                                                //
                                                                                //        6 in_doubt pages, 1 in-flight txn
                                                                                //        REDO:
                                                                                //          Page 3 - page format
                                                                                //          Page 4 - page format
                                                                                //          Page 5 - page format, foster adoption
                                                                                //          Page 6 - page rebalance (child page) followed by 4 insertions
                                                                                //                                             page rebalance (parent page) again
                                                                                //                                             followed by 2 deletions 
                                                                                //                                             and then 1 insertion
                                                                                //              Rebalance - Recovery child page, empty
                                                                                //              insert aa2
                                                                                //              insert key303
                                                                                //              insert key302
                                                                                //              insert key301
                                                                                //              Allocate a new page for page split
                                                                                //              Rebalance - Recovery parent page, 4 existing records, move 2 of them
                                                                                //              page split, new low aa2, new high key302
                                                                                //              delete key302 but not found (btree_logrec.cpp, line 354)
                                                                                //              delete key303 but not found (btree_logrec.cpp, line 354)                                                                                
                                                                                //              insert key300
                                                                                //        Page 7 - page format, foster adoption
                                                                                //        Page 8 - page rebalance (child page) followed by 2 insertions
                                                                                //              insert key302
                                                                                //              insert key303
                                                                                //      UNDO:
                                                                                //        Page 6:
                                                                                //              delete key300 -- failed in fence key checking of low fence key and first key 
                                                                                //                                        (Btree_page.h.cpp, btree_page_h::_is_consistent_keyorder(), line 1312)
                                                                                //              delete key301
                                                                                //              delete key302
                                                                                //              delete key303                                                                                
                                                                                

    vec_t data;
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
        // These 2 insertions cause the AV, but 1 insertion works
        const int data_size = btree_m::max_entry_size() - 4;
        vec_t data;
        char data_str[data_size+1];
        data_str[data_size] = '\0';
        memset(data_str, 'D', data_size);
        W_DO(test_env->btree_insert_and_commit(_stid_list[2], "aa1", data_str));
        W_DO(test_env->btree_insert_and_commit(_stid_list[2], "aa2", data_str));
/**/

//        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
//        W_DO(test_env->btree_insert_and_commit(_stid_list[1], "aa2", "data2"));
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

// Enabled for testing purpose
/* Not passing, full logging, crash if crash with in-flight multiple statements, including page split *
TEST (RestartTest, MultiIndexConcChckptCF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

w_rc_t btree_populate_records_local2(stid_t &stid,
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
    const int recordCount = (SM_PAGESIZE / btree_m::max_entry_size()) * 1;
// TODO(Restart)... bug with full logging and multiple splits on the same page
//                          current it generate incorrect data, the records should be rollback
//                          but did not happen (key301, key302)
//                          The test case above (restart_concurrent_chckpt_multi_index)
//                          should be very similar to this test case but it is generating
//                          core dump instead of incorrect result currently
// btree_insert_log::undo() - record not found when trying to undo an insertion
// btree_page_h::init_fence_keys() is calling btree_page_data::remove_items()
//               but it seems the page does not handle direct record removal well
//               I might need to use something similar to steal_records() instead
//               meaning use a separate page to copy over the fence record and then
//               copy over the qualified records (eliminate the not quailifed records)
//               and then turn the new page into the new souce page
/** Order of operation with this repro:
Start -
Insert key204
Insert key203
Insert key202
Insert key201
Page split - infinite -> key203, key203 -> infinite
Move key203
Move key204
Insert key200
Insert key304
Insert key303
Page split - infinite -> key203, key203 -> key3, key3 ->infinite
Move key303
Move key304
Insert key302
Insert key301
Page split - infinite -> key203, key203 -> key3, key3 -> key303, key303 -> infinite
Move key303
Move key304
Insert key300

Crash -
    Page # - key200, key201, key202    what happened to this page of REDO?
    Page 6 - key203, key204
   Page 8 - key300, key301, key302
    Page 9 - key303, key304

Restart - REDO
Page 3 - 
Format
Page 4 - 
Format
Page 5 - 
Format
Adopt (key203)
Adopt (key3)
Page 6 - 
Allocate
Rebalance - child page
Insert - REOD key203
Insert - REDO key204
Insert - REOD key304
Insert - REDO key303
Allocate
Rebalance - parent page (4 existing records), low fence: key203, high fence: key3
Delete - REDO key303 (not found)
Delete - REDO key304 (not found)
Adopt
Page 7 -
	Format
	Adopt (key203)
Page 8 - 
Allocate
Rebalance - child page
Insert - REDO key303
Insert - REDO key304
Insert - REDO key302
Insert - REDO key301
Allocate 
Rebalance - parent page (4 existing records), low fence: key3, high fence key303
Delete - REDO key303 (not found)
Delete - REDO key304 (not found)
Insert - REDO key300
Page 9 - 
Allocate
Rebalance - child page
Insert - REDO key303
Insert - REDO key304

Restart - UNDO
UNDO insertion - key300 - page 8
UNDO insertion - key301 - not found - page 8
UNDO insertion - key302 - not found - page 8
UNDO insertion - key303 - page 9
UNDO insertion - key304 - page 9
**/


    vec_t data;
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

class restart_concurrent_chckpt_multi_index2 : public restart_test_base {
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

//        W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit, true, '0'));    // flags: no checkpoint, commit, one transaction per insert, keyPrefix '0'
//        W_DO(test_env->btree_populate_records(_stid_list[1], false, t_test_txn_commit, false, '1'));   // flags:                        all inserts in one transaction, keyPrefix '1'
//        W_DO(test_env->btree_populate_records(_stid_list[2], false, t_test_txn_commit, false, '2'));   // flags: no checkpoint, commit, one transaction, keyPrefix '2'        
        W_DO(btree_populate_records_local2(_stid_list[2], false, t_test_txn_commit, false, '2'));   // flags: no checkpoint, commit, one transaction, keyPrefix '2'

//        W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
//        W_DO(test_env->btree_insert_and_commit(_stid_list[1], "aa2", "data2"));
//        W_DO(test_env->btree_populate_records(_stid_list[2], false, t_test_txn_in_flight, false, '3'));   // flags: no checkpoint, no commit, one big transaction which will include page split, keyPrefix '3'
        W_DO(btree_populate_records_local2(_stid_list[2], false, t_test_txn_in_flight, false, '3'));   // flags: no checkpoint, no commit, one big transaction which will include page split, keyPrefix '3'
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
        DBGOUT3(<<"Index 0 validation:");
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ(recordCount+1, s.rownum);
        EXPECT_EQ(std::string("aa1"), s.minkey);

        DBGOUT3(<<"Index 1 validation:");        
        W_DO(test_env->btree_scan(_stid_list[1], s));
        EXPECT_EQ(recordCount+1, s.rownum);
        EXPECT_EQ(std::string("aa2"), s.minkey);

        DBGOUT3(<<"Index 2 validation:");        
        W_DO(test_env->btree_scan(_stid_list[2], s));
        EXPECT_EQ(recordCount, s.rownum);
        EXPECT_EQ(std::string("key200"), s.minkey);
        

        return RCOK;
    }
};

/* core dump if crash with in-flight multiple statements, including page split 
btree_insert_log::undo() - undo an insert operation by delete it, but record not found */
/* Not passing, full logging *
TEST (RestartTest, MultiIndexConcChckptCF) {
    test_env->empty_logdata_dir();
    restart_concurrent_chckpt_multi_index2 context;
    
    restart_test_options options;
    options.shutdown_mode = simulated_crash;
    options.restart_mode = m2_full_logging_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options), 0);
}
**/

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
