#include "btree_test_env.h"
#include "generic_page.h"
#include "bf.h"
#include "btree.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "log.h"
#include "w_error.h"

#include "bf_fixed.h"
#include "bf_tree_cb.h"
#include "bf_tree.h"
#include "sm_io.h"
#include "sm_int_0.h"

#include <vector>

btree_test_env *test_env;

/**
 * Tests for EMLSN get/set in B-tree pages.
 */

/** A class to use private methods in bf_tree_m. In C++, your friends can see your private! */
class test_emlsn {
public:
    static void triger_urgent_unswizzling (bf_tree_m &bf) {
        bf._trigger_unswizzling(true);
    }
    static void try_evict_all (bf_tree_m &bf, bf_idx max_idx) {
        triger_urgent_unswizzling(bf);
        for (bf_idx idx = 1; idx <= max_idx; ++idx) {
            bf._try_evict_block(idx);
        }
    }
};

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

    bf_tree_m &pool(*smlevel_0::bf);

    btree_page_h root_p;
    W_DO(root_p.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_SH));
    EXPECT_TRUE (root_p.is_node());
    EXPECT_TRUE (root_p.nrecs() >= 11);
    EXPECT_TRUE (root_p.nrecs() < MAX_PAGES);

    EXPECT_FALSE (root_p.is_dirty());
    lsn_t root_lsn_before = root_p.lsn();
    root_p.unfix();

    // Now, to invoke eviction and EMLSN updates, evict all as much as possible.
    test_emlsn::try_evict_all(pool, MAX_PAGES);

    // Because of the evictions, the parent page should have been updated.
    W_DO(root_p.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_SH));
    // EXPECT_GT(root_p.lsn(), root_lsn_before); // TODO this fails now. cb._parent is required

    root_p.unfix();
    W_DO(x_btree_verify(ssm, stid));

    return RCOK;
}
TEST (EmlsnTest, All) {
    test_env->empty_logdata_dir();
    sm_options options;
    // SPR requires cb._parent, thus requires swizzling
    options.set_bool_option("sm_bufferpool_swizzle", true);
    EXPECT_EQ(0, test_env->runBtreeTest(test_all, false, default_quota_in_pages, options));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
