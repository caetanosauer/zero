#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "generic_page.h"
#include "alloc_cache.h"
#include "vol.h"

btree_test_env *test_env;

/**
 * Unit test for page allocation/deallocation.
 */

const shpid_t FIRST_PID = 1 + 1 + 1; // 0=vol_hdr, 1=alloc_p, 2=stnode_p

inline w_rc_t allocate_one(ss_m* ssm, test_volume_t* tvol, lpid_t& pid)
{
    return ssm->vol->get(tvol->_vid)->alloc_a_page(pid.page);
}

inline w_rc_t allocate_consecutive(ss_m* ssm, test_volume_t* tvol, size_t count,
        lpid_t& pid)
{
    return ssm->vol->get(tvol->_vid)->alloc_consecutive_pages(count, pid.page);
}

inline w_rc_t deallocate_one(ss_m* ssm, test_volume_t* tvol, lpid_t& pid)
{
    return ssm->vol->get(tvol->_vid)->deallocate_page(pid.page);
}

inline alloc_cache_t* get_alloc_cache(ss_m* ssm, vid_t vid)
{
    return ssm->vol->get(vid)->get_alloc_cache();
}

w_rc_t allocate_test(ss_m* ssm, test_volume_t *test_volume) {
    W_DO(ssm->begin_xct());
    stid_t stid (test_volume->_vid, 10);
    lpid_t pid, pid2, pid3;

    alloc_cache_t *ac = get_alloc_cache(ssm, test_volume->_vid);

    for (shpid_t pid = 0; pid < FIRST_PID; ++pid) {
        EXPECT_TRUE(ac->is_allocated_page(pid));
    }

    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID));
    W_DO(allocate_one(ssm, test_volume, pid));
    EXPECT_EQ (pid.page, FIRST_PID);
    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID));

    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 1));
    W_DO(allocate_one(ssm, test_volume, pid2));
    EXPECT_EQ (pid2.page, FIRST_PID + 1);
    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 1));

    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 2));
    W_DO(allocate_one(ssm, test_volume, pid3));
    EXPECT_EQ (pid3.page, FIRST_PID + 2);
    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 2));

    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 3));

    W_DO(ssm->commit_xct());

    return RCOK;
}

TEST (AllocTest, Allocate) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(allocate_test), 0);
}

w_rc_t deallocate_test(ss_m* ssm, test_volume_t *test_volume) {
    W_DO(ssm->begin_xct());
    stid_t stid (test_volume->_vid, 10);
    lpid_t pid, pid2, pid3;

    alloc_cache_t *ac = get_alloc_cache(ssm, test_volume->_vid);
    W_DO(allocate_one(ssm, test_volume, pid));
    EXPECT_EQ (pid.page, FIRST_PID);

    W_DO(allocate_one(ssm, test_volume, pid2));
    EXPECT_EQ (pid2.page, FIRST_PID + 1);

    W_DO(allocate_one(ssm, test_volume, pid3));
    EXPECT_EQ (pid3.page, FIRST_PID + 2);

    // dealloc some of them
    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 1));
    W_DO(deallocate_one(ssm, test_volume, pid2));
    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 1));

    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 2));
    W_DO(deallocate_one(ssm, test_volume, pid3));
    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 2));

    W_DO(ssm->commit_xct());

    return RCOK;
}

TEST (AllocTest, Deallocate) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(deallocate_test), 0);
}

w_rc_t reuse_test(ss_m* ssm, test_volume_t *test_volume) {
    W_DO(ssm->begin_xct());
    stid_t stid (test_volume->_vid, 10);
    lpid_t pid, pid2, pid3;

    alloc_cache_t *ac = get_alloc_cache(ssm, test_volume->_vid);
    W_DO(allocate_one(ssm, test_volume, pid));
    EXPECT_EQ (pid.page, FIRST_PID);

    W_DO(allocate_one(ssm, test_volume, pid2));
    EXPECT_EQ (pid2.page, FIRST_PID + 1);

    W_DO(allocate_one(ssm, test_volume, pid3));
    EXPECT_EQ (pid3.page, FIRST_PID + 2);

    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 1));
    W_DO(deallocate_one(ssm, test_volume, pid2));
    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 1));

    lpid_t pid4, pid5;
    W_DO(allocate_one(ssm, test_volume, pid4));
    EXPECT_EQ (pid4.page, FIRST_PID + 1); // reused!

    W_DO(allocate_one(ssm, test_volume, pid5));
    EXPECT_EQ (pid5.page, FIRST_PID + 3); // moved on

    W_DO(ssm->commit_xct());

    return RCOK;
}

TEST (AllocTest, Reuse) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(reuse_test), 0);
}

w_rc_t reuse_serialize_test(ss_m* ssm, test_volume_t *test_volume) {
    W_DO(ssm->begin_xct());
    stid_t stid (test_volume->_vid, 10);
    lpid_t pid, pid2, pid3;

    alloc_cache_t *ac = get_alloc_cache(ssm, test_volume->_vid);
    W_DO(allocate_one(ssm, test_volume, pid));
    EXPECT_EQ (pid.page, FIRST_PID);

    W_DO(allocate_one(ssm, test_volume, pid2));
    EXPECT_EQ (pid2.page, FIRST_PID + 1);

    W_DO(allocate_one(ssm, test_volume, pid3));
    EXPECT_EQ (pid3.page, FIRST_PID + 2);

    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 1));
    W_DO(deallocate_one(ssm, test_volume, pid2));
    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 1));
    W_DO(ssm->commit_xct());

    // re-mount the device to check if the allocation information is saved
    W_DO(ssm->dismount_vol(test_volume->_device_name));

    W_DO(ssm->mount_vol(test_volume->_device_name, test_volume->_vid));
    stid = stid_t (test_volume->_vid, 10); // _vid might have been changed!
    ac = get_alloc_cache(ssm, test_volume->_vid);

    for (shpid_t pid = 0; pid < FIRST_PID; ++pid) {
        EXPECT_TRUE(ac->is_allocated_page(pid));
    }
    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID));
    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 1));
    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 2));

    W_DO(ssm->begin_xct());
    lpid_t pid4, pid5;
    W_DO(allocate_one(ssm, test_volume, pid4));
    EXPECT_EQ (pid4.page, FIRST_PID + 1); // reused!

    W_DO(allocate_one(ssm, test_volume, pid5));
    EXPECT_EQ (pid5.page, FIRST_PID + 3); // moved on
    W_DO(ssm->commit_xct());


    return RCOK;
}

TEST (AllocTest, ReuseSerialize) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(reuse_serialize_test), 0);
}

w_rc_t allocate_consecutive(ss_m* ssm, test_volume_t *test_volume) {
    W_DO(ssm->begin_xct());
    stid_t stid (test_volume->_vid, 10);
    lpid_t pid;

    alloc_cache_t *ac = get_alloc_cache(ssm, test_volume->_vid);

    for (shpid_t pid = 0; pid < FIRST_PID; ++pid) {
        EXPECT_TRUE(ac->is_allocated_page(pid));
    }

    W_DO(allocate_one(ssm, test_volume, pid));
    EXPECT_EQ (pid.page, FIRST_PID);

    lpid_t pid_begin;
    W_DO(allocate_consecutive(ssm, test_volume, 30, pid_begin));
    EXPECT_EQ (pid_begin.page, FIRST_PID + 1);

    for (shpid_t pid = FIRST_PID + 1; pid < FIRST_PID + 1 + 30; ++pid) {
        EXPECT_TRUE(ac->is_allocated_page(pid));
    }
    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 1 + 30));

    W_DO(ssm->commit_xct());

    return RCOK;
}

TEST (AllocTest, AllocateConsecutive) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(allocate_consecutive), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
