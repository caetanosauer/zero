#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "generic_page.h"
#include "fixable_page_h.h"
#include "bf.h"
#include "sm_io.h"
#include "backup.h"
#include <iostream>
#include <sstream>
#include <cstdio>
#include <string>

// for mkdir
#include <sys/stat.h>
#include <sys/types.h>

btree_test_env *test_env;

/**
 * Unit tests for backup manager.
 */

const shpid_t FIRST_PID = 1 + 1 + 1; // 0=vol_hdr, 1=alloc_p, 2=stnode_p

w_rc_t initial_test(ss_m* ssm, test_volume_t *test_volume) {
    BackupManager *bk = ssm->bk;
    volid_t vid = test_volume->_vid;
    x_delete_backup(ssm, test_volume);
    EXPECT_FALSE(bk->volume_exists(vid));

    // do nothing and immediately take backup
    W_DO(x_take_backup(ssm, test_volume));
    EXPECT_TRUE(bk->volume_exists(vid));

    // this is initial state, so all data pages are unused.
    for (shpid_t pid = FIRST_PID; pid < default_quota_in_pages; ++pid) {
        EXPECT_FALSE(bk->page_exists(vid, pid));
    }
    x_delete_backup(ssm, test_volume);
    EXPECT_FALSE(bk->volume_exists(vid));
    return RCOK;
}

TEST (BackupTest, Initial) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(initial_test), 0);
}

const snum_t STNUM = 10;
/** helper to allocate a few pages */
w_rc_t allocate_few(ss_m* ssm, test_volume_t *test_volume, shpid_t alloc_count) {
    volid_t vid = test_volume->_vid;
    W_DO(ssm->begin_xct());
    stid_t stid (vid, STNUM);
    for (shpid_t i = 0; i < alloc_count; ++i) {
        lpid_t pid;
        W_DO(io_m::sx_alloc_a_page(stid, pid));
        EXPECT_EQ (pid.page, FIRST_PID + i);

        // set minimal headers in the page
        fixable_page_h page;
        W_DO(page.fix_direct(vid, pid.page, LATCH_EX, false, true));
        page.set_lsns(lsn_t(0, 1));
        page.set_dirty();
        page.get_generic_page()->pid = pid;
        page.get_generic_page()->tag = t_btree_p;
        page.unfix();
    }
    W_DO(ssm->commit_xct());
    return RCOK;
}

w_rc_t allocate_few_test(ss_m* ssm, test_volume_t *test_volume) {
    BackupManager *bk = ssm->bk;
    volid_t vid = test_volume->_vid;
    x_delete_backup(ssm, test_volume);
    EXPECT_FALSE(bk->volume_exists(vid));

    // allocate a few pages and then take backup
    const shpid_t ALLOCATE_COUNT = 4;
    W_DO(allocate_few(ssm, test_volume, ALLOCATE_COUNT));
    W_DO(x_take_backup(ssm, test_volume));
    EXPECT_TRUE(bk->volume_exists(vid));

    generic_page buf;
    for (shpid_t pid = FIRST_PID; pid < default_quota_in_pages; ++pid) {
        SCOPED_TRACE(pid);
        if (pid < FIRST_PID + ALLOCATE_COUNT) {
            // this page exists in backup
            EXPECT_TRUE(bk->page_exists(vid, pid));
            W_DO(bk->retrieve_page(buf, vid, pid));
            EXPECT_EQ(pid, buf.pid.page);
            EXPECT_EQ(vid, buf.pid.vol().vol);
            EXPECT_EQ(STNUM, buf.pid.store());
            EXPECT_EQ(t_btree_p, buf.tag);
        } else {
            // this page doesn't exist in backup
            EXPECT_FALSE(bk->page_exists(vid, pid));
        }
    }
    x_delete_backup(ssm, test_volume);
    EXPECT_FALSE(bk->volume_exists(vid));
    return RCOK;
}

TEST (BackupTest, AllocateFew) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(allocate_few_test), 0);
}

w_rc_t mixed_test(ss_m* ssm, test_volume_t *test_volume) {
    BackupManager *bk = ssm->bk;
    volid_t vid = test_volume->_vid;
    x_delete_backup(ssm, test_volume);
    EXPECT_FALSE(bk->volume_exists(vid));

    // allocate a few pages, then deallocate a few, then take backup
    const shpid_t ALLOCATE_COUNT = 20;
    W_DO(allocate_few(ssm, test_volume, ALLOCATE_COUNT));

    const shpid_t DEALLOCATE_START = FIRST_PID + ALLOCATE_COUNT - 13;
    const shpid_t DEALLOCATE_END = DEALLOCATE_START + 4;
    W_DO(ssm->begin_xct());
    for (shpid_t pid = DEALLOCATE_START; pid < DEALLOCATE_END; ++pid) {
        W_DO(io_m::sx_dealloc_a_page(lpid_t(vid, STNUM, pid)));
    }
    W_DO(ssm->commit_xct());

    W_DO(x_take_backup(ssm, test_volume));
    EXPECT_TRUE(bk->volume_exists(vid));

    generic_page buf;
    for (shpid_t pid = FIRST_PID; pid < default_quota_in_pages; ++pid) {
        SCOPED_TRACE(pid);
        if (pid < FIRST_PID + ALLOCATE_COUNT
                && (pid < DEALLOCATE_START || pid >= DEALLOCATE_END)) {
            // this page exists in backup
            EXPECT_TRUE(bk->page_exists(vid, pid));
            W_DO(bk->retrieve_page(buf, vid, pid));
            EXPECT_EQ(pid, buf.pid.page);
            EXPECT_EQ(vid, buf.pid.vol().vol);
            EXPECT_EQ(STNUM, buf.pid.store());
            EXPECT_EQ(t_btree_p, buf.tag);
        } else {
            // this page doesn't exist in backup
            EXPECT_FALSE(bk->page_exists(vid, pid));
        }
    }
    x_delete_backup(ssm, test_volume);
    EXPECT_FALSE(bk->volume_exists(vid));
    return RCOK;
}

TEST (BackupTest, Mixed) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(mixed_test), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
