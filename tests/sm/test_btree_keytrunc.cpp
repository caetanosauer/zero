#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "w_key.h"

btree_test_env *test_env;

/**
 * Unit test to check key truncation (prefix/suffix truncation) is working.
 */

w_rc_t suffix_test(ss_m* ssm, test_volume_t *test_volume) {
    // insert long keys first
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    // so that one leaf page can have only a few tuples
    const int keysize = SM_PAGESIZE / 10;
    // these keys are not good for prefix truncation, but great for suffix truncation.
    // we need only the first 3 bytes as separator key
    W_DO(test_env->begin_xct());
    w_keystr_t key;
    vec_t data;
    char keystr[keysize] = "";
    char datastr[3] = "";
    for (int i = 0; i < 100; ++i) {
        datastr[0] = keystr[0] = ('0' + ((i / 100) % 10));
        datastr[1] = keystr[1] = ('0' + ((i / 10) % 10));
        datastr[2] = keystr[2] = ('0' + ((i / 1) % 10));
        memset(keystr + 3, ('0' + (i % 10)), keysize - 3);
        key.construct_regularkey(keystr, keysize);
        data.set(datastr, 3);
        W_DO(ssm->create_assoc(stid, key, data));
    }
    W_DO(test_env->commit_xct());

    W_DO(x_btree_verify(ssm, stid));

    // and let's check the root node's entries
    btree_page_h root_p;
    W_DO (root_p.fix_root (stid, LATCH_EX));
    EXPECT_TRUE (root_p.is_node());
    EXPECT_GT (root_p.nrecs(), 0);
    int minlen = keysize, maxlen = -1;
    for (slotid_t slot = 0; slot < root_p.nrecs(); ++slot) {
        btrec_t r (root_p, slot);
        minlen = minlen > (int) r.key().get_length_as_nonkeystr() ? r.key().get_length_as_nonkeystr() : minlen;
        maxlen = maxlen > (int) r.key().get_length_as_nonkeystr() ? maxlen : r.key().get_length_as_nonkeystr();
    }
    // 3 bytes should be enough
    EXPECT_LE (minlen, 3);
    EXPECT_LE (maxlen, 3);
    cout << "key length before suffix truncation=" << keysize
        << ", after: min=" << minlen << ", max=" << maxlen << endl;

    return RCOK;
}

TEST (BtreeKeyTruncTest, Suffix) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(suffix_test, false, default_locktable_size, 512, 32), 0);
}
TEST (BtreeKeyTruncTest, SuffixLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(suffix_test, true, default_locktable_size, 512, 32), 0);
}

w_rc_t suffix_test_shortest(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    const size_t datsize = (SM_PAGESIZE / 100); // less than 100 tuples per page (about 80-90)
    // key 0 = "00", key 1 = "01"
    // key 10 = "10", key 11 = "11"
    // so, it should pick x mod 10 == 0 as separator key.
    char keystr[3] = "";
    char datastr[datsize + 1];
    keystr[2] = '\0';
    datastr[datsize] = '\0';
    ::memset (datastr, 'a', datsize);
    int prev_recs = 0;
    const int first_digits[] = {3,6,2,9,0, 1,5,4,8,7}; // to avoid skewed insertions
    for (int i = 0;; ++i) {
        w_assert0 (i < 100);
        keystr[0] = '0' + (i / 10);
        keystr[1] = '0' + first_digits[i % 10];
        W_DO(x_btree_insert (ssm, stid, keystr, datastr));

        btree_page_h root_p;
        W_DO (root_p.fix_root (stid, LATCH_SH));
        if (root_p.nrecs() <= prev_recs) {
            EXPECT_TRUE (root_p.is_fence_low_infimum());
            EXPECT_FALSE (root_p.is_fence_high_supremum());
            EXPECT_NE (root_p.get_foster(), (uint) 0);
            w_keystr_t fence;
            root_p.copy_fence_high_key(fence);
            cout << "shortest-test: split happend at " << i << "-th insertion!"
                << " separator key was:" << fence << endl;
            EXPECT_EQ (fence.get_length_as_nonkeystr(), (size_t) 1)
                << "couldn't choose the shortest separator?";
            break;
        }
        prev_recs = root_p.nrecs();
    }
    W_DO(test_env->commit_xct());

    return RCOK;
}

TEST (BtreeKeyTruncTest, SuffixShortest) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(suffix_test_shortest), 0);
}
TEST (BtreeKeyTruncTest, SuffixShortestLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(suffix_test_shortest, true), 0);
}

w_rc_t suffix_test_posskew(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    const size_t datsize = (SM_PAGESIZE / 30); // much less tuples are enough
    // this time we insert in order, so skewed on right-most
    char keystr[3] = "";
    char datastr[datsize + 1];
    keystr[2] = '\0';
    datastr[datsize] = '\0';
    ::memset (datastr, 'a', datsize);
    int prev_recs = 0;
    for (int i = 0;; ++i) {
        w_assert0 (i < 100);
        keystr[0] = '0' + (i / 10);
        keystr[1] = '0' + (i % 10);
        W_DO(x_btree_insert (ssm, stid, keystr, datastr));

        btree_page_h root_p;
        W_DO (root_p.fix_root (stid, LATCH_SH));
        if (root_p.nrecs() <= prev_recs) {
            EXPECT_TRUE (root_p.is_fence_low_infimum());
            EXPECT_FALSE (root_p.is_fence_high_supremum());
            EXPECT_NE (root_p.get_foster(), (uint) 0);
            w_keystr_t fence;
            root_p.copy_fence_high_key(fence);
            int percent = root_p.nrecs() * 100 / prev_recs;
            cout << "pos-skew-test: split happend at " << i << "-th insertion!"
                << " separator key was:" << fence
                << "(" << (percent) << "% point)" << endl;

            EXPECT_GT (percent, 80); // should be right-most skewed split
            break;
        }
        prev_recs = root_p.nrecs();
    }
    W_DO(test_env->commit_xct());

    return RCOK;
}

TEST (BtreeKeyTruncTest, SuffixPosSkew) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(suffix_test_posskew), 0);
}
TEST (BtreeKeyTruncTest, SuffixPosSkewLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(suffix_test_posskew, true), 0);
}

w_rc_t suffix_test_negskew(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    const size_t datsize = (SM_PAGESIZE / 30); // much less tuples are enough
    // this time we insert in reverse order, so skewed on left-most
    char keystr[3] = "";
    char datastr[datsize + 1];
    keystr[2] = '\0';
    datastr[datsize] = '\0';
    ::memset (datastr, 'a', datsize);
    int prev_recs = 0;
    for (int i = 99;; --i) {
        w_assert0 (i >= 0);
        keystr[0] = '0' + (i / 10);
        keystr[1] = '0' + (i % 10);
        W_DO(x_btree_insert (ssm, stid, keystr, datastr));

        btree_page_h root_p;
        W_DO (root_p.fix_root (stid, LATCH_SH));
        if (root_p.nrecs() <= prev_recs) {
            EXPECT_TRUE (root_p.is_fence_low_infimum());
            EXPECT_FALSE (root_p.is_fence_high_supremum());
            EXPECT_NE (root_p.get_foster(), (uint) 0);
            w_keystr_t fence;
            root_p.copy_fence_high_key(fence);
            int percent = root_p.nrecs() * 100 / prev_recs;
            cout << "neg-skew-test: split happend at " << (99 - i) << "-th insertion!"
                << " separator key was:" << fence
                << "(" << (percent) << "% point)" << endl;

            EXPECT_LT (percent, 20); // should be left-most skewed split
            break;
        }
        prev_recs = root_p.nrecs();
    }
    W_DO(test_env->commit_xct());

    return RCOK;
}

TEST (BtreeKeyTruncTest, SuffixNegSkew) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(suffix_test_negskew), 0);
}
TEST (BtreeKeyTruncTest, SuffixNegSkewLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(suffix_test_negskew, true), 0);
}

w_rc_t prefix_test(ss_m* ssm, test_volume_t *test_volume) {
    // insert long keys first
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    // so that one leaf page can have only 4 or 5 tuples
    const int keysize = SM_PAGESIZE / 6;
    //const int keysize = btree_m::max_entry_size() - 3; // for max size key test instead
    // these keys are good for prefix truncation, but not great for suffix truncation.
    // in leaf pages (in root pages, no prefix truncation), these keys should be significantly shortened
    w_keystr_t key;
    vec_t data;
    char keystr[keysize];
    char datastr[3] = "";
    w_assert1(keysize <= (int)btree_m::max_entry_size() - (int)sizeof(datastr));
    W_DO(test_env->begin_xct());
    for (int i = 0; i < 20; ++i) {
        datastr[0] = keystr[keysize - 1] = ('0' + ((i / 100) % 10));
        datastr[1] = keystr[keysize - 2] = ('0' + ((i / 10) % 10));
        datastr[2] = keystr[keysize - 3] = ('0' + ((i / 1) % 10));
        memset (keystr, '0', keysize - 3);
        key.construct_regularkey(keystr, keysize);
        data.set(datastr, 3);
        W_DO(ssm->create_assoc(stid, key, data));
    }
    W_DO(test_env->commit_xct());

    W_DO(x_btree_verify(ssm, stid));

    // and let's check the child node that has the middle key.
    memset (keystr, '0', keysize - 3);
    memcpy(keystr + keysize - 3, "500", 3); // middle key
    key.construct_regularkey(keystr, keysize);
    btree_page_h leaf;
    W_DO (btree_impl::_ux_traverse(stid, key, btree_impl::t_fence_contain, LATCH_SH, leaf));
    EXPECT_TRUE (leaf.is_fixed());
    EXPECT_TRUE (leaf.is_leaf());

    // 3 bytes should be enough
    size_t prefix_len = leaf.get_prefix_length();
    EXPECT_GE (prefix_len, (size_t) keysize - 3);

    w_keystr_t fence_low, fence_high;
    leaf.copy_fence_low_key(fence_low);
    leaf.copy_fence_high_key(fence_high);
    size_t prefix_len_correct = fence_low.common_leading_bytes(fence_high);
    EXPECT_EQ (prefix_len_correct, prefix_len);
    cout << "key length (pid=" << leaf.pid() << ") before prefix truncation=" << keysize
        << ", after=" << (keysize - prefix_len)
        << endl;

    return RCOK;
}

TEST (BtreeKeyTruncTest, Prefix) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(prefix_test), 0);
}
TEST (BtreeKeyTruncTest, Lock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(prefix_test, true), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
