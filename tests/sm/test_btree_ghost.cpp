#define SM_SOURCE
#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btree_page_h.h"

// lots of non-sense to do REDO from testcase.
#include "sm_base.h"
#include "sm_base.h"
#include "logrec.h"
#include "logdef_gen.cpp"

btree_test_env *test_env;

/**
 * Unit test for ghost records.
 */
w_rc_t ghost_mark(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "key1", "data1", test_env->get_use_locks()));
    W_DO(x_btree_remove_and_commit (ssm, stid, "key1", test_env->get_use_locks()));

    btree_page_h root_p;
    W_DO (root_p.fix_root (stid, LATCH_SH));
    EXPECT_EQ (1, root_p.nrecs());
    EXPECT_TRUE (root_p.is_ghost(0));
    root_p.unfix();

    W_DO(x_btree_insert_and_commit (ssm, stid, "key2", "data2", test_env->get_use_locks()));
    W_DO (root_p.fix_root (stid, LATCH_SH));
    EXPECT_EQ (2, root_p.nrecs());
    EXPECT_TRUE (root_p.is_ghost(0));
    EXPECT_FALSE (root_p.is_ghost(1));
    root_p.mark_ghost (1);
    EXPECT_TRUE (root_p.is_ghost(1));
    return RCOK;
}

TEST (BtreeGhostTest, Mark) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(ghost_mark), 0);
}
TEST (BtreeGhostTest, MarkLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(ghost_mark, true), 0);
}

w_rc_t ghost_reclaim(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "key1", "data", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "key2", "data", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "key3", "data", test_env->get_use_locks()));
    W_DO(x_btree_remove_and_commit (ssm, stid, "key1", test_env->get_use_locks()));
    W_DO(x_btree_remove_and_commit (ssm, stid, "key3", test_env->get_use_locks()));

    btree_page_h root_p;
    W_DO (root_p.fix_root (stid, LATCH_SH));
    EXPECT_EQ (3, root_p.nrecs());
    EXPECT_TRUE (root_p.is_ghost(0));
    EXPECT_FALSE (root_p.is_ghost(1));
    EXPECT_TRUE (root_p.is_ghost(2));

    root_p.unmark_ghost (0);
    EXPECT_FALSE (root_p.is_ghost(0));
    EXPECT_FALSE (root_p.is_ghost(1));
    EXPECT_TRUE (root_p.is_ghost(2));
    return RCOK;
}

TEST (BtreeGhostTest, Reclaim) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(ghost_reclaim), 0);
}
TEST (BtreeGhostTest, ReclaimLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(ghost_reclaim, true), 0);
}

w_rc_t ghost_reserve(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "key1", "data", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "key3", "data", test_env->get_use_locks()));

    btree_page_h root_p;
    W_DO (root_p.fix_root (stid, LATCH_EX));
    EXPECT_EQ (2, root_p.nrecs());
    EXPECT_FALSE (root_p.is_ghost(0));
    EXPECT_FALSE (root_p.is_ghost(1));

    EXPECT_TRUE (root_p.is_consistent(true, true));
    w_keystr_t key;
    key.construct_regularkey("key2", 4);
    root_p.reserve_ghost (key, 10);
    EXPECT_FALSE (root_p.is_ghost(0));
    EXPECT_TRUE (root_p.is_ghost(1));
    EXPECT_FALSE (root_p.is_ghost(2));
    EXPECT_TRUE (root_p.is_consistent(true, true));

    btrec_t rec (root_p, 1);
    EXPECT_EQ (0, rec.key().compare(key)) << "incorrect key " << rec.key();
    EXPECT_TRUE (rec.is_ghost_record());
    return RCOK;
}

TEST (BtreeGhostTest, Reserve) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(ghost_reserve), 0);
}
TEST (BtreeGhostTest, ReserveLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(ghost_reserve, true), 0);
}

w_rc_t ghost_reserve_xct(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "key1", "data", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "key3", "data", test_env->get_use_locks()));

    w_keystr_t key;
    key.construct_regularkey("key2", 4);

    W_DO(ssm->begin_xct());
    W_DO(ssm->begin_sys_xct(true));
    btree_page_h root_p;
    W_DO (root_p.fix_root (stid, LATCH_EX));
    log_btree_ghost_reserve(root_p, key, 10);
    W_DO(ssm->commit_sys_xct());
    W_DO(ssm->commit_xct());
    EXPECT_EQ (2, root_p.nrecs()); // we don't applied yet!
    EXPECT_FALSE (root_p.is_ghost(0));
    EXPECT_FALSE (root_p.is_ghost(1));
    root_p.unfix();

    W_DO(ssm->begin_xct());
    W_DO(ssm->begin_sys_xct(true));
    W_DO (root_p.fix_root (stid, LATCH_EX));
    // TODO should use restart_m to do this.
    // currently directly use btree_ghost_reserve_log to test REDO
    btree_ghost_reserve_log logs (root_p, key, 10);
    logs.redo (&root_p);
    EXPECT_EQ (3, root_p.nrecs());
    EXPECT_FALSE (root_p.is_ghost(0));
    EXPECT_TRUE (root_p.is_ghost(1));
    EXPECT_FALSE (root_p.is_ghost(2));
    W_DO(ssm->commit_sys_xct());
    W_DO(ssm->commit_xct());

    btrec_t rec (root_p, 1);
    EXPECT_EQ (0, rec.key().compare(key)) << "incorrect key " << rec.key();
    EXPECT_TRUE (rec.is_ghost_record());
    return RCOK;
}

TEST (BtreeGhostTest, ReserveXct) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(ghost_reserve_xct), 0);
}
TEST (BtreeGhostTest, ReserveXctLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(ghost_reserve_xct, true), 0);
}

w_rc_t insert_remove_defrag(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "key005", "data5", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "key004", "data4", test_env->get_use_locks()));
    W_DO(x_btree_insert_and_commit (ssm, stid, "key006", "data6", test_env->get_use_locks()));
    W_DO(x_btree_remove_and_commit (ssm, stid, "key004", test_env->get_use_locks()));
    W_DO(x_btree_remove_and_commit (ssm, stid, "key005", test_env->get_use_locks()));

    btree_page_h root_p;
    W_DO (root_p.fix_root (stid, LATCH_SH));
    smsize_t before_defrag = root_p.usable_space();
    root_p.unfix();
    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s, test_env->get_use_locks()));
        EXPECT_EQ (1, s.rownum);
        EXPECT_EQ (std::string("key006"), s.minkey);
        EXPECT_EQ (std::string("key006"), s.maxkey);
    }
    W_DO(ssm->begin_xct());
    W_DO (root_p.fix_root (stid, LATCH_EX));
    W_DO(ssm->defrag_index_page(root_p));
    root_p.unfix();
    W_DO(ssm->commit_xct());
    W_DO (root_p.fix_root (stid, LATCH_SH));
    smsize_t after_defrag = root_p.usable_space();
    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s, test_env->get_use_locks()));
        EXPECT_EQ (1, s.rownum);
        EXPECT_EQ (std::string("key006"), s.minkey);
        EXPECT_EQ (std::string("key006"), s.maxkey);
    }
    cout << "usable_space() before defrag:" << before_defrag
        << ". after defrag:" << after_defrag << endl;
    EXPECT_GT (after_defrag, before_defrag);

    return RCOK;
}

TEST (BtreeGhostTest, InsertRemoveDefrag) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_remove_defrag), 0);
}
TEST (BtreeGhostTest, InsertRemoveDefragLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_remove_defrag, true), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
