#include "btree_test_env.h"
#include "generic_page.h"
#include "btree.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "bf_tree.h"

btree_test_env *test_env;
/**
 * Unit test for page checksum logics.
 */
TEST (ChecksumTest, Calculate) {
    generic_page p1, p2, p3, p4;
    char *c1 = (char*) &p1;
    char *c2 = (char*) &p2;
    char *c3 = (char*) &p3;
    ::memset (&p4, 0, sizeof (generic_page));
    for (size_t i = 0; i < sizeof (generic_page); ++i) {
        c1[i] = (char) i;
        c2[i] = (char) i;
        c3[i] = (char) (i + 10);
    }

    EXPECT_EQ (p1.calculate_checksum(), p2.calculate_checksum());
    EXPECT_NE (p1.calculate_checksum(), p3.calculate_checksum());
    EXPECT_NE (p1.calculate_checksum(), p4.calculate_checksum());
    EXPECT_NE (p3.calculate_checksum(), p4.calculate_checksum());
}
w_rc_t btree_page(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_verify(ssm, stid));

    W_DO(ssm->begin_xct());
    {
        char keystr[4], datastr[4];
        keystr[3] = '\0';
        datastr[3] = '\0';
        for (int i = 0; i < 100; ++i) {
            keystr[0] = datastr[0] = '0' + (i / 100);
            keystr[1] = datastr[1] = '0' + ((i / 10) % 10);
            keystr[2] = datastr[2] = '0' + (i % 10);
            W_DO(x_btree_insert(ssm, stid, keystr, datastr));
        }
    }
    W_DO(ssm->commit_xct());
    W_DO(x_btree_verify(ssm, stid));

    w_keystr_t neginf;
    neginf.construct_neginfkey();

    // get the left most page
    uint32_t correct_checksum;
    W_DO(ssm->begin_xct());
    {
        btree_page_h leaf;
        W_DO (btree_impl::_ux_traverse(stid, neginf, btree_impl::t_fence_low_match, LATCH_SH, leaf));
        EXPECT_TRUE (leaf.is_fixed());
        EXPECT_TRUE (leaf.is_leaf());

        generic_page dummy_p;
        ::memset (&dummy_p, 0, sizeof (generic_page));
        correct_checksum = leaf.get_generic_page()->calculate_checksum();
        EXPECT_NE (correct_checksum, dummy_p.calculate_checksum());
    }
    W_DO(ssm->commit_xct());

    // write out the page to set checksum by bufferpool
    W_DO(ss_m::bf->get_cleaner()->force_volume());
    // also discards the pages from bufferpool (this test requires to re-read from disk!)

    // check it again
    W_DO(ssm->begin_xct());
    {
        btree_page_h leaf;
        W_DO (btree_impl::_ux_traverse(stid, neginf, btree_impl::t_fence_low_match, LATCH_SH, leaf));
        EXPECT_TRUE (leaf.is_fixed());
        EXPECT_TRUE (leaf.is_leaf());
        EXPECT_FALSE (leaf.is_dirty());

        EXPECT_EQ (correct_checksum, leaf.get_generic_page()->calculate_checksum()) << "page content has changed?";
        EXPECT_EQ (correct_checksum, leaf.get_generic_page()->checksum) << "checksum hasn't been updated on write?";
    }
    W_DO(ssm->commit_xct());

    return RCOK;
}

TEST (ChecksumTest, BtreePage) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(btree_page), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
