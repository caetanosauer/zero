#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include <sys/time.h>

btree_test_env *test_env;

/**
 * Unit test for transaction chaining (flush-pipeline).
 */

w_rc_t insert_twice(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert (stid, "key004", "data4"));
    W_DO(ss_m::chain_xct(true));
    W_DO(test_env->btree_insert (stid, "key005", "data5"));
    W_DO(ss_m::chain_xct(false));
    W_DO(test_env->btree_insert (stid, "key006", "data6"));
    W_DO(test_env->commit_xct());

    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));
    EXPECT_EQ (3, s.rownum);
    EXPECT_EQ (std::string("key004"), s.minkey);
    EXPECT_EQ (std::string("key006"), s.maxkey);

    return RCOK;
}

TEST (ChainXctTest, InsertTwice) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_twice), 0);
}
TEST (ChainXctTest, InsertTwiceLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_twice, true), 0);
}

w_rc_t pipeline_many(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    char keystr[7];
    keystr[0] = 'k';
    keystr[1] = 'e';
    keystr[2] = 'y';
    keystr[6] = '\0';

    // because of flush pipeline, this should be quick
    timeval start,stop,result;
    ::gettimeofday(&start,NULL);
    W_DO(test_env->begin_xct());
    for (int i = 0; i < 500; ++i) {
        keystr[3] = '0' + (i / 100);
        keystr[4] = '0' + ((i / 10) % 10);
        keystr[5] = '0' + (i % 10);
        W_DO(test_env->btree_insert (stid, keystr, "data"));
        W_DO(ss_m::chain_xct(true));
    }
    W_DO(test_env->commit_xct());
    ::gettimeofday(&stop,NULL);
    timersub(&stop, &start,&result);
    cout << "flush pipeline of 500 chain (lazy) commits: " << (result.tv_sec + result.tv_usec/1000000.0) << " sec" << endl;

    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));
    EXPECT_EQ (500, s.rownum);
    EXPECT_EQ (std::string("key000"), s.minkey);
    EXPECT_EQ (std::string("key499"), s.maxkey);

    ::gettimeofday(&start,NULL);
    W_DO(test_env->begin_xct());
    for (int i = 500; i < 510; ++i) {
        keystr[3] = '0' + (i / 100);
        keystr[4] = '0' + ((i / 10) % 10);
        keystr[5] = '0' + (i % 10);
        W_DO(test_env->btree_insert (stid, keystr, "data"));
        W_DO(ss_m::chain_xct(false));
    }
    W_DO(test_env->commit_xct());
    ::gettimeofday(&stop,NULL);
    timersub(&stop, &start,&result);
    cout << "flush pipeline of 10 chain (non-lazy) commits: " << (result.tv_sec + result.tv_usec/1000000.0) << " sec" << endl;

    W_DO(test_env->btree_scan(stid, s));
    EXPECT_EQ (510, s.rownum);
    EXPECT_EQ (std::string("key000"), s.minkey);
    EXPECT_EQ (std::string("key509"), s.maxkey);
    return RCOK;
}

TEST (ChainXctTest, PipelineMany) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(pipeline_many), 0);
}
TEST (ChainXctTest, PipelineManyLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(pipeline_many, true), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
