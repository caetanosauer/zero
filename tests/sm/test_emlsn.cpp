#include "btree_test_env.h"
#include "generic_page.h"
#include "btree.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "log_core.h"
#include "w_error.h"

#include "bf_fixed.h"
#include "bf_tree_cb.h"
#include "bf_tree.h"
#include "sm_base.h"

#include <vector>

btree_test_env *test_env;

/**
 * Tests for EMLSN get/set in B-tree pages.
 */

// make big enough database for tests
const int MAX_PAGES = 100;
w_rc_t prepare_test(ss_m* ssm, test_volume_t *test_volume, stid_t &stid, lpid_t &root_pid) {
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    const int recsize = SM_PAGESIZE / 6;
    char datastr[recsize];
    ::memset (datastr, 'a', recsize);
    vec_t data;
    data.set(datastr, recsize);

    // create at least 11 pages.
    W_DO(ssm->begin_xct());
    w_keystr_t key;
    char keystr[6] = "";
    ::memset(keystr, '\0', 6);
    keystr[0] = 'k';
    keystr[1] = 'e';
    keystr[2] = 'y';
    for (int i = 0; i < 66; ++i) {
        keystr[3] = ('0' + ((i / 100) % 10));
        keystr[4] = ('0' + ((i / 10) % 10));
        keystr[5] = ('0' + ((i / 1) % 10));
        key.construct_regularkey(keystr, 6);
        test_env->set_xct_query_lock();
        W_DO(ssm->create_assoc(stid, key, data));
    }
    W_DO(ssm->commit_xct());
    W_DO(x_btree_verify(ssm, stid));
    W_DO(ssm->force_buffers()); // clean them up
    return RCOK;
}

w_rc_t test_all(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    W_DO (prepare_test(ssm, test_volume, stid, root_pid));
    btree_page_h root_p;
    W_DO(root_p.fix_root(stid, LATCH_SH));
    EXPECT_TRUE (root_p.is_node());
    EXPECT_TRUE (root_p.nrecs() >= 11);
    EXPECT_TRUE (root_p.nrecs() < MAX_PAGES);

    EXPECT_FALSE (root_p.is_dirty());
    lsn_t root_lsn_before = root_p.lsn();
    root_p.unfix();

    // Now, to invoke eviction and EMLSN updates, evict all as much as possible.
    uint32_t evicted_count, unswizzled_count;
    W_DO(ssm->bf->evict_blocks(evicted_count, unswizzled_count, EVICT_COMPLETE));

    // Because of the evictions, the parent page should have been updated.
    W_DO(root_p.fix_root(stid, LATCH_SH));
    EXPECT_GT(root_p.lsn(), root_lsn_before);

    root_p.unfix();
    W_DO(x_btree_verify(ssm, stid));

    return RCOK;
}
TEST (EmlsnTest, All) {
    test_env->empty_logdata_dir();
    sm_options options;
    EXPECT_EQ(0, test_env->runBtreeTest(test_all, false, default_quota_in_pages, options));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
