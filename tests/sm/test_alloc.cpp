#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "generic_page.h"
#include "sm_io.h"
#include "alloc_cache.h"

btree_test_env *test_env;

/**
 * Unit test for page allocation/deallocation.
 */

const shpid_t FIRST_PID = 1 + 1 + 1; // 0=vol_hdr, 1=alloc_p, 2=stnode_p

w_rc_t allocate_test(ss_m* ssm, test_volume_t *test_volume) {
    W_DO(ssm->begin_xct());
    stid_t stid (test_volume->_vid, 10);
    lpid_t pid, pid2, pid3;

    alloc_cache_t *ac = io_m::get_vol_alloc_cache(test_volume->_vid);

    for (shpid_t pid = 0; pid < FIRST_PID; ++pid) {
        EXPECT_TRUE(ac->is_allocated_page(pid));
    }

    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID));
    W_DO(io_m::sx_alloc_a_page(stid, pid));
    EXPECT_EQ (pid.page, FIRST_PID);
    EXPECT_EQ (pid.store(), (uint) 10);
    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID));

    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 1));
    W_DO(io_m::sx_alloc_a_page(stid, pid2));
    EXPECT_EQ (pid2.page, FIRST_PID + 1);
    EXPECT_EQ (pid2.store(), (uint) 10);
    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 1));

    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 2));
    W_DO(io_m::sx_alloc_a_page(stid, pid3));
    EXPECT_EQ (pid3.page, FIRST_PID + 2);
    EXPECT_EQ (pid3.store(), (uint) 10);
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

    alloc_cache_t *ac = io_m::get_vol_alloc_cache(test_volume->_vid);
    W_DO(io_m::sx_alloc_a_page(stid, pid));
    EXPECT_EQ (pid.page, FIRST_PID);

    W_DO(io_m::sx_alloc_a_page(stid, pid2));
    EXPECT_EQ (pid2.page, FIRST_PID + 1);

    W_DO(io_m::sx_alloc_a_page(stid, pid3));
    EXPECT_EQ (pid3.page, FIRST_PID + 2);

    // dealloc some of them
    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 1));
    W_DO(io_m::sx_dealloc_a_page(pid2));
    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 1));

    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 2));
    W_DO(io_m::sx_dealloc_a_page(pid3));
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

    alloc_cache_t *ac = io_m::get_vol_alloc_cache(test_volume->_vid);
    W_DO(io_m::sx_alloc_a_page(stid, pid));
    EXPECT_EQ (pid.page, FIRST_PID);

    W_DO(io_m::sx_alloc_a_page(stid, pid2));
    EXPECT_EQ (pid2.page, FIRST_PID + 1);

    W_DO(io_m::sx_alloc_a_page(stid, pid3));
    EXPECT_EQ (pid3.page, FIRST_PID + 2);

    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 1));
    W_DO(io_m::sx_dealloc_a_page(pid2));
    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 1));
   
    lpid_t pid4, pid5;
    W_DO(io_m::sx_alloc_a_page(stid, pid4));
    EXPECT_EQ (pid4.page, FIRST_PID + 1); // reused!

    W_DO(io_m::sx_alloc_a_page(stid, pid5));
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

    alloc_cache_t *ac = io_m::get_vol_alloc_cache(test_volume->_vid);
    W_DO(io_m::sx_alloc_a_page(stid, pid));
    EXPECT_EQ (pid.page, FIRST_PID);

    W_DO(io_m::sx_alloc_a_page(stid, pid2));
    EXPECT_EQ (pid2.page, FIRST_PID + 1);

    W_DO(io_m::sx_alloc_a_page(stid, pid3));
    EXPECT_EQ (pid3.page, FIRST_PID + 2);

    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 1));
    W_DO(io_m::sx_dealloc_a_page(pid2));
    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 1));
    W_DO(ssm->commit_xct());

    // re-mount the device to check if the allocation information is saved
    W_DO(ssm->dismount_all());

    W_DO(ssm->mount_dev(test_volume->_device_name, test_volume->_vid));    
    stid = stid_t (test_volume->_vid, 10); // _vid might have been changed!
    ac = io_m::get_vol_alloc_cache(test_volume->_vid);

    for (shpid_t pid = 0; pid < FIRST_PID; ++pid) {
        EXPECT_TRUE(ac->is_allocated_page(pid));
    }
    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID));
    EXPECT_FALSE(ac->is_allocated_page(FIRST_PID + 1));
    EXPECT_TRUE(ac->is_allocated_page(FIRST_PID + 2));
    
    W_DO(ssm->begin_xct());
    lpid_t pid4, pid5;
    W_DO(io_m::sx_alloc_a_page(stid, pid4));
    EXPECT_EQ (pid4.page, FIRST_PID + 1); // reused!

    W_DO(io_m::sx_alloc_a_page(stid, pid5));
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

    alloc_cache_t *ac = io_m::get_vol_alloc_cache(test_volume->_vid);

    for (shpid_t pid = 0; pid < FIRST_PID; ++pid) {
        EXPECT_TRUE(ac->is_allocated_page(pid));
    }

    W_DO(io_m::sx_alloc_a_page(stid, pid));
    EXPECT_EQ (pid.page, FIRST_PID);
    
    lpid_t pid_begin;
    W_DO(io_m::sx_alloc_consecutive_pages(stid, 30, pid_begin));
    EXPECT_EQ (pid_begin.page, FIRST_PID + 1);
    EXPECT_EQ (pid_begin.store(), (uint) 10);

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
