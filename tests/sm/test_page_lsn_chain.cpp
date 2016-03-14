#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "log_core.h"

btree_test_env *test_env;

/**
 * \file test_page_lsn_chain.cpp
 * \brief Unit tests for Single-Page-Recovery's Page LSN Chain.
 * \ingroup Single-Page-Recovery
 */

w_rc_t prepare_test(ss_m* ssm, test_volume_t *test_volume, StoreID &stid, PageID &root_pid) {
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    // very large records to cause splits
    const int recsize = SM_PAGESIZE / 6;
    char datastr[recsize];
    ::memset (datastr, 'a', recsize);
    vec_t data;
    data.set(datastr, recsize);

    W_DO(ssm->begin_xct());
    w_keystr_t key;
    char keystr[5] = "";
    memset(keystr, '\0', 5);
    keystr[0] = 'k';
    keystr[1] = 'e';
    keystr[2] = 'y';
    for (int i = 0; i < 20; ++i) {
        keystr[3] = ('0' + (i / 10));
        keystr[4] = ('0' + (i % 10));
        key.construct_regularkey(keystr, 5);
        W_DO(ssm->create_assoc(stid, key, data));
    }
    W_DO(ssm->commit_xct());
    W_DO(x_btree_verify(ssm, stid));
    smlevel_0::bf->get_cleaner()->wakeup(true);
    W_DO(ssm->checkpoint());
    return RCOK;
}


w_rc_t dump_simple(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO (prepare_test(ssm, test_volume, stid, root_pid));
    ssm->dump_page_lsn_chain(std::cout, 3);
    ssm->dump_page_lsn_chain(std::cout, 4);
    ssm->dump_page_lsn_chain(std::cout, 5);
    ssm->dump_page_lsn_chain(std::cout);
    return RCOK;
}

TEST (PageLsnChainTest, DumpSimple) {
    test_env->empty_logdata_dir();
    sm_options options;
    EXPECT_EQ(test_env->runBtreeTest(dump_simple, options), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
