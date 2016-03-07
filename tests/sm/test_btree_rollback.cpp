#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "w_key.h"

btree_test_env *test_env;

/**
 * Unit test for rolling back transaction in BTree.
 */
w_rc_t rollback_insert(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "aa1", "data1", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa2", "data2", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa3", "data3", test_env->get_use_locks()));

    W_DO (x_btree_verify(ssm, stid));
    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    W_DO(x_btree_insert(ssm, stid, "aa3a", "data4"));
    W_DO(ssm->abort_xct());
    W_DO (x_btree_verify(ssm, stid));

    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s, test_env->get_use_locks()));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
    }

    W_DO(x_btree_insert_and_commit (ssm, stid, "aa3a", "data4", test_env->get_use_locks()));

    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s, test_env->get_use_locks()));
        EXPECT_EQ (4, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3a"), s.maxkey);
    }

    W_DO (x_btree_verify(ssm, stid));
    return RCOK;
}

TEST (BtreeRollbackTest, RollbackInsert) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(rollback_insert), 0);
}
TEST (BtreeRollbackTest, RollbackInsertLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(rollback_insert, true), 0);
}

w_rc_t rollback_delete(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "aa1", "data1", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa2", "data2", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa3", "data3", test_env->get_use_locks()));

    W_DO (x_btree_verify(ssm, stid));
    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    W_DO(x_btree_remove(ssm, stid, "aa1"));
    W_DO(ssm->abort_xct());
    W_DO (x_btree_verify(ssm, stid));

    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s, test_env->get_use_locks()));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
    }

    W_DO(x_btree_remove_and_commit(ssm, stid, "aa1", test_env->get_use_locks()));

    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s, test_env->get_use_locks()));
        EXPECT_EQ (2, s.rownum);
        EXPECT_EQ (std::string("aa2"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
    }
    W_DO (x_btree_verify(ssm, stid));

    return RCOK;
}

TEST (BtreeRollbackTest, RollbackDelete) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(rollback_delete), 0);
}
TEST (BtreeRollbackTest, RollbackDeleteLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(rollback_delete, true), 0);
}

w_rc_t rollback_mixed(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "aa1", "data1", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa2", "data2", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa3", "data3", test_env->get_use_locks()));

    W_DO (x_btree_verify(ssm, stid));
    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    W_DO(x_btree_insert(ssm, stid, "aa3a", "data4"));
    W_DO(x_btree_remove(ssm, stid, "aa1"));
    W_DO(ssm->abort_xct());
    W_DO (x_btree_verify(ssm, stid));

    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s, test_env->get_use_locks()));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
    }

    W_DO(x_btree_remove_and_commit(ssm, stid, "aa1", test_env->get_use_locks()));

    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s, test_env->get_use_locks()));
        EXPECT_EQ (2, s.rownum);
        EXPECT_EQ (std::string("aa2"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
    }
    W_DO (x_btree_verify(ssm, stid));

    return RCOK;
}

TEST (BtreeRollbackTest, RollbackMixed) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(rollback_mixed), 0);
}
TEST (BtreeRollbackTest, RollbackMixedLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(rollback_mixed, true), 0);
}

w_rc_t rollback_split(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "aa1", "data1", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa2", "data2", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa3", "data3", test_env->get_use_locks()));

    W_DO (x_btree_verify(ssm, stid));
    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    // insert a few so that split will happen
    for (int i = 0; i < 6; ++i) {
        const size_t datalen = SM_PAGESIZE / 6;
        char data[datalen + 1];
        data[datalen] = '\0';
        memset(data, 'a', datalen);
        char key[5];
        key[0] = 'k';
        key[1] = 'e';
        key[2] = 'y';
        key[3] = '0' + i;
        key[4] = '\0';
        W_DO(x_btree_insert(ssm, stid, key, data));
    }
    W_DO(ssm->abort_xct());
    W_DO (x_btree_verify(ssm, stid));

    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s, test_env->get_use_locks()));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
    }

    W_DO(x_btree_remove_and_commit(ssm, stid, "aa1", test_env->get_use_locks()));

    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s, test_env->get_use_locks()));
        EXPECT_EQ (2, s.rownum);
        EXPECT_EQ (std::string("aa2"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
    }
    W_DO (x_btree_verify(ssm, stid));

    return RCOK;
}

TEST (BtreeRollbackTest, RollbackSplit) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(rollback_split), 0);
}
TEST (BtreeRollbackTest, RollbackSplitLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(rollback_split, true), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
