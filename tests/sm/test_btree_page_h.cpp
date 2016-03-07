#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "btree_page_h.h"

btree_test_env *test_env;

/**
 * Unit tests for btree_page_h.
 * It's an internal class thus indirectly tested by other testcases.
 * However, it also does a lot to be tested, so this testcase
 * specifically and directly tests its functions.
 */

w_rc_t test_search_leaf(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "a1", "data1"));
    W_DO(x_btree_insert_and_commit (ssm, stid, "b3", "data3"));
    W_DO(x_btree_insert_and_commit (ssm, stid, "c2", "data2"));

    btree_page_h root;
    W_DO(root.fix_root(stid, LATCH_SH));
    w_keystr_t key;
    bool found;
    slotid_t slot;

    // "slot" will tell
    // - if found: the slot
    // - if not found: slot to place the key

    // a0 is smaller than all,
    key.construct_regularkey("a0", 2);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (0, slot); // so a0 should go here

    // a1 is there
    key.construct_regularkey("a1", 2);
    root.search(key, found, slot);
    EXPECT_TRUE(found);
    EXPECT_EQ (0, slot);

    // same as a0
    key.construct_regularkey("a", 1);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (0, slot);

    // larger than a1, thus
    key.construct_regularkey("b", 1);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (1, slot);

    // same as above
    key.construct_regularkey("b2", 2);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (1, slot);

    // it's there
    key.construct_regularkey("b3", 2);
    root.search(key, found, slot);
    EXPECT_TRUE(found);
    EXPECT_EQ (1, slot);

    key.construct_regularkey("c", 1);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (2, slot);

    key.construct_regularkey("c2", 2);
    root.search(key, found, slot);
    EXPECT_TRUE(found);
    EXPECT_EQ (2, slot);

    key.construct_regularkey("d", 1);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (3, slot);

    return RCOK;
}

TEST (BtreePTest, SearchLeaf) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(test_search_leaf), 0);
}

// the above only uses poorman's key. below appends a bit

w_rc_t test_search_leaf_long(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "a100", "data1"));
    W_DO(x_btree_insert_and_commit (ssm, stid, "b301", "data3"));
    W_DO(x_btree_insert_and_commit (ssm, stid, "c202", "data2"));

    btree_page_h root;
    W_DO(root.fix_root(stid, LATCH_SH));
    w_keystr_t key;
    bool found;
    slotid_t slot;

    key.construct_regularkey("a04", 3);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (0, slot);

    key.construct_regularkey("a100", 4);
    root.search(key, found, slot);
    EXPECT_TRUE(found);
    EXPECT_EQ (0, slot);

    key.construct_regularkey("a", 1);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (0, slot);

    key.construct_regularkey("b", 1);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (1, slot);

    key.construct_regularkey("b244", 4);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (1, slot);

    key.construct_regularkey("b301", 4);
    root.search(key, found, slot);
    EXPECT_TRUE(found);
    EXPECT_EQ (1, slot);

    key.construct_regularkey("c00", 3);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (2, slot);

    key.construct_regularkey("c202", 4);
    root.search(key, found, slot);
    EXPECT_TRUE(found);
    EXPECT_EQ (2, slot);

    key.construct_regularkey("d", 1);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (3, slot);

    return RCOK;
}

TEST (BtreePTest, SearchLeafLong) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(test_search_leaf_long), 0);
}

w_rc_t test_search_leaf_long2(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "00a1", "data1"));
    W_DO(x_btree_insert_and_commit (ssm, stid, "00b3", "data3"));
    W_DO(x_btree_insert_and_commit (ssm, stid, "00c2", "data2"));

    btree_page_h root;
    W_DO(root.fix_root(stid, LATCH_SH));
    w_keystr_t key;
    bool found;
    slotid_t slot;

    key.construct_regularkey("00a0", 4);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (0, slot);

    key.construct_regularkey("00a1", 4);
    root.search(key, found, slot);
    EXPECT_TRUE(found);
    EXPECT_EQ (0, slot);

    key.construct_regularkey("00a", 3);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (0, slot);

    key.construct_regularkey("00b", 3);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (1, slot);

    key.construct_regularkey("00b2", 4);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (1, slot);

    key.construct_regularkey("00b3", 4);
    root.search(key, found, slot);
    EXPECT_TRUE(found);
    EXPECT_EQ (1, slot);

    key.construct_regularkey("00c", 3);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (2, slot);

    key.construct_regularkey("00c2", 4);
    root.search(key, found, slot);
    EXPECT_TRUE(found);
    EXPECT_EQ (2, slot);

    key.construct_regularkey("00d", 3);
    root.search(key, found, slot);
    EXPECT_FALSE(found);
    EXPECT_EQ (3, slot);

    return RCOK;
}


TEST (BtreePTest, SearchLeafLong2) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(test_search_leaf_long2), 0);
}

// TODO more and more testcases here

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
