#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "generic_page.h"
#include "stnode_page.h"
#include "alloc_cache.h"
#include "vol.h"

btree_test_env *test_env;

/**
 * Unit test for page allocation/deallocation.
 */

const PageID FIRST_PID = 2; // 0=alloc_p, 1=stnode_p

inline w_rc_t allocate_one(ss_m* ssm, test_volume_t* tvol, PageID& pid)
{
    return ssm->vol->alloc_a_page(pid);
}

// inline w_rc_t allocate_consecutive(ss_m* ssm, test_volume_t* tvol, size_t count,
//         PageID& pid)
// {
//     return ssm->vol->get(tvol->_vid)->alloc_consecutive_pages(count, pid);
// }

inline w_rc_t deallocate_one(ss_m* ssm, test_volume_t* tvol, PageID& pid)
{
    return ssm->vol->deallocate_page(pid);
}

inline alloc_cache_t* get_alloc_cache(ss_m* ssm)
{
    return ssm->vol->get_alloc_cache();
}

w_rc_t allocate_test(ss_m* ssm, test_volume_t *test_volume) {
    W_DO(ssm->begin_xct());
    PageID pid, pid2, pid3;

    alloc_cache_t *ac = get_alloc_cache(ssm);

    for (PageID pid = 0; pid < FIRST_PID; ++pid) {
        EXPECT_TRUE(ac->is_allocated(pid));
    }

    EXPECT_FALSE(ac->is_allocated(FIRST_PID));
    W_DO(allocate_one(ssm, test_volume, pid));
    EXPECT_EQ (pid, FIRST_PID);
    EXPECT_TRUE(ac->is_allocated(FIRST_PID));

    EXPECT_FALSE(ac->is_allocated(FIRST_PID + 1));
    W_DO(allocate_one(ssm, test_volume, pid2));
    EXPECT_EQ (pid2, FIRST_PID + 1);
    EXPECT_TRUE(ac->is_allocated(FIRST_PID + 1));

    EXPECT_FALSE(ac->is_allocated(FIRST_PID + 2));
    W_DO(allocate_one(ssm, test_volume, pid3));
    EXPECT_EQ (pid3, FIRST_PID + 2);
    EXPECT_TRUE(ac->is_allocated(FIRST_PID + 2));

    EXPECT_FALSE(ac->is_allocated(FIRST_PID + 3));

    W_DO(ssm->commit_xct());

    return RCOK;
}

TEST (AllocTest, Allocate) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(allocate_test), 0);
}

w_rc_t deallocate_test(ss_m* ssm, test_volume_t *test_volume) {
    W_DO(ssm->begin_xct());
    PageID pid, pid2, pid3;

    alloc_cache_t *ac = get_alloc_cache(ssm);
    W_DO(allocate_one(ssm, test_volume, pid));
    EXPECT_EQ (pid, FIRST_PID);

    W_DO(allocate_one(ssm, test_volume, pid2));
    EXPECT_EQ (pid2, FIRST_PID + 1);

    W_DO(allocate_one(ssm, test_volume, pid3));
    EXPECT_EQ (pid3, FIRST_PID + 2);

    // dealloc some of them
    EXPECT_TRUE(ac->is_allocated(FIRST_PID + 1));
    W_DO(deallocate_one(ssm, test_volume, pid2));
    EXPECT_FALSE(ac->is_allocated(FIRST_PID + 1));

    EXPECT_TRUE(ac->is_allocated(FIRST_PID + 2));
    W_DO(deallocate_one(ssm, test_volume, pid3));
    EXPECT_FALSE(ac->is_allocated(FIRST_PID + 2));

    W_DO(ssm->commit_xct());

    return RCOK;
}

TEST (AllocTest, Deallocate) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(deallocate_test), 0);
}

w_rc_t reuse_test(ss_m* ssm, test_volume_t *test_volume) {
    W_DO(ssm->begin_xct());
    PageID pid, pid2, pid3;

    alloc_cache_t *ac = get_alloc_cache(ssm);
    W_DO(allocate_one(ssm, test_volume, pid));
    EXPECT_EQ (pid, FIRST_PID);

    W_DO(allocate_one(ssm, test_volume, pid2));
    EXPECT_EQ (pid2, FIRST_PID + 1);

    W_DO(allocate_one(ssm, test_volume, pid3));
    EXPECT_EQ (pid3, FIRST_PID + 2);

    EXPECT_TRUE(ac->is_allocated(FIRST_PID + 1));
    W_DO(deallocate_one(ssm, test_volume, pid2));
    EXPECT_FALSE(ac->is_allocated(FIRST_PID + 1));

    // TODO CS: current page allocator does not reuse pids
//     PageID pid4, pid5;
//     W_DO(allocate_one(ssm, test_volume, pid4));
//     EXPECT_EQ (pid4, FIRST_PID + 1); // reused!

//     W_DO(allocate_one(ssm, test_volume, pid5));
//     EXPECT_EQ (pid5, FIRST_PID + 3); // moved on

    W_DO(ssm->commit_xct());

    return RCOK;
}

TEST (AllocTest, Reuse) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(reuse_test), 0);
}

w_rc_t reuse_serialize_test(ss_m* ssm, test_volume_t *test_volume) {
    W_DO(ssm->begin_xct());
    PageID pid, pid2, pid3;

    alloc_cache_t *ac = get_alloc_cache(ssm);
    W_DO(allocate_one(ssm, test_volume, pid));
    EXPECT_EQ (pid, FIRST_PID);

    W_DO(allocate_one(ssm, test_volume, pid2));
    EXPECT_EQ (pid2, FIRST_PID + 1);

    W_DO(allocate_one(ssm, test_volume, pid3));
    EXPECT_EQ (pid3, FIRST_PID + 2);

    EXPECT_TRUE(ac->is_allocated(FIRST_PID + 1));
    W_DO(deallocate_one(ssm, test_volume, pid2));
    EXPECT_FALSE(ac->is_allocated(FIRST_PID + 1));
    W_DO(ssm->commit_xct());

    ac = get_alloc_cache(ssm);

    for (PageID pid = 0; pid < FIRST_PID; ++pid) {
        EXPECT_TRUE(ac->is_allocated(pid));
    }
    EXPECT_TRUE(ac->is_allocated(FIRST_PID));
    EXPECT_FALSE(ac->is_allocated(FIRST_PID + 1));
    EXPECT_TRUE(ac->is_allocated(FIRST_PID + 2));

    // CS TODO: current page allocator does not reuse pids
//     W_DO(ssm->begin_xct());
//     PageID pid4, pid5;
//     W_DO(allocate_one(ssm, test_volume, pid4));
//     EXPECT_EQ (pid4, FIRST_PID + 1); // reused!

//     W_DO(allocate_one(ssm, test_volume, pid5));
//     EXPECT_EQ (pid5, FIRST_PID + 3); // moved on
//     W_DO(ssm->commit_xct());


    return RCOK;
}

TEST (AllocTest, ReuseSerialize) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(reuse_serialize_test), 0);
}

// w_rc_t allocate_consecutive(ss_m* ssm, test_volume_t *test_volume) {
//     W_DO(ssm->begin_xct());
//     PageID pid;

//     alloc_cache_t *ac = get_alloc_cache(ssm, test_volume->_vid);

//     for (PageID pid = 0; pid < FIRST_PID; ++pid) {
//         EXPECT_TRUE(ac->is_allocated(pid));
//     }

//     W_DO(allocate_one(ssm, test_volume, pid));
//     EXPECT_EQ (pid, FIRST_PID);

//     PageID pid_begin;
//     W_DO(allocate_consecutive(ssm, test_volume, 30, pid_begin));
//     EXPECT_EQ (pid_begin, FIRST_PID + 1);

//     for (PageID pid = FIRST_PID + 1; pid < FIRST_PID + 1 + 30; ++pid) {
//         EXPECT_TRUE(ac->is_allocated(pid));
//     }
//     EXPECT_FALSE(ac->is_allocated(FIRST_PID + 1 + 30));

//     W_DO(ssm->commit_xct());

//     return RCOK;
// }

// TEST (AllocTest, AllocateConsecutive) {
//     test_env->empty_logdata_dir();
//     EXPECT_EQ(test_env->runBtreeTest(allocate_consecutive), 0);
// }

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
