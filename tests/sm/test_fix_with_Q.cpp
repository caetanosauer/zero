/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */


#include "btree_test_env.h"
#include "gtest/gtest.h"

#include "btree_page_h.h"



btree_test_env *test_env;


w_rc_t test_root_page(ss_m* ssm, test_volume_t *test_volume) {
    // create a root page:
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    // Can we fix it in Q mode?
    btree_page_h root_p;
    W_DO(root_p.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_Q));
    root_p.unfix();


    return RCOK;
}




TEST (FixWithQTest, RootPage) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(test_root_page, true, default_locktable_size, 4096, 1024), 0);
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
