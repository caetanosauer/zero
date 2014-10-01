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

// Restart performance test to compare various restart methods
// M1 - traditional restart, open system after Restart finished
// M2 - Enhanced traditional restart, open system after Log Analysis phase,
//         concurrency check based on commit_lsn
//         restart carried out by restart child thread
// M3 - On_demand restart, open system after Log Analysis phase,
//         concurrency check based on lock acquisition
//         restart carried out by user transaction threads
// M4 - Mixed restart, open system after Log Analysis phase, 
//         concurrency check based on lock acquisition
//         restart carried out by both restart child thread and user thransaction threads

// Three phases in the test run:
// 1. Populate the base database, follow by a normal shutdown to persist data on disk
//     Data - insert 3000 records
//     Key - 1 - 3000
//     Key - for key ending with 3, 5 or 7, skip the insertions
// 2. Start the system with existing data from phase 1, 
//     use multiple worker threads to generate many user transactions, 
//     including commit and in-flight (maybe abort)
//     Key - for key ending with 3 or 7, insert the record
//         Key - if the key ending with 7, in-flight insertions
//         Key - if the key ending with 3, commit
//     Key - all other keys, update
//         Key - if the key ending with 2 or 9, in-flight updates
//     simulate system crash
// 3. Restart the system, start concurrent user transactions
//     measuer the time required to finish 1000 concurrent transactions
//     Key - insert records ending with 5
//     Key - delete if key ending with 2 or 0 (all the 2's should roll back first)
//     Key - if key ending with 3, update




lsn_t get_durable_lsn() {
    lsn_t ret;
    ss_m::get_durable_lsn(ret);
    return ret;
}
void output_durable_lsn(int W_IFDEBUG1(num)) {
    DBGOUT1( << num << ".durable LSN=" << get_durable_lsn());
}



int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
