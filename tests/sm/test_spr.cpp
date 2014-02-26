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
 * Tests for Single-page recovery (SPR).
 */

w_rc_t flush_and_evict(ss_m* ssm) {
    W_DO(ssm->force_buffers()); // clean them up
    // also, evict all to update EMLSN
    uint32_t evicted_count, unswizzled_count;
    W_DO(ssm->bf->evict_blocks(evicted_count, unswizzled_count, bf_tree_m::EVICT_COMPLETE));
    // then flush it, this time just for root node
    W_DO(ssm->force_buffers());
    return RCOK;
}

w_rc_t prepare_test(ss_m* ssm, test_volume_t *test_volume, stid_t &stid, lpid_t &root_pid,
                    shpid_t &target_pid, w_keystr_t &target_key0, w_keystr_t &target_key1) {
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    const int recsize = SM_PAGESIZE / 6;
    char datastr[recsize];
    ::memset (datastr, 'a', recsize);
    vec_t data;
    data.set(datastr, recsize);

    W_DO(ssm->begin_xct());
    w_keystr_t key;
    char keystr[6] = "";
    ::memset(keystr, '\0', 6);
    keystr[0] = 'k';
    keystr[1] = 'e';
    keystr[2] = 'y';
    for (int i = 0; i < 30; ++i) {
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

    {
        btree_page_h root_p;
        W_DO(root_p.fix_root(root_pid.vol().vol, root_pid.store(), LATCH_SH));
        EXPECT_TRUE(root_p.nrecs() > 4);
        target_pid = root_p.child(1);
        btree_page_h target_p;
        W_DO(target_p.fix_nonroot(root_p, stid.vol.vol, target_pid, LATCH_SH));
        EXPECT_GE(2, target_p.nrecs());
        target_p.get_key(0, target_key0);
        target_p.get_key(1, target_key1);
    }
    W_DO(flush_and_evict(ssm));

    // then take a backup. this is the page image to start SPR from.
    x_delete_backup(ssm, test_volume);
    W_DO(x_take_backup(ssm, test_volume));
    return RCOK;
}

void corrupt_page(test_volume_t *test_volume, shpid_t target_pid) {
    std::cout << "=========== Corrupting page " << target_pid << " in "
        << test_volume->_device_name << " for test===============" << std::endl;
    // Bypass bufferpool to corrupt it
    generic_page page;
    {
        std::ifstream file(test_volume->_device_name, std::ios::binary);
        file.seekg(sizeof(generic_page) * target_pid);
        file.read(reinterpret_cast<char*>(&page), sizeof(generic_page));
        EXPECT_EQ(page.pid.page, target_pid);
    }

    ::memset(reinterpret_cast<char*>(&page) + 1234, 42, 987);
    {
        std::ofstream file(test_volume->_device_name, std::ios::binary | std::ios::out);
        file.seekp(sizeof(generic_page) * target_pid);
        file.write(reinterpret_cast<char*>(&page), sizeof(generic_page));
        file.flush();
    }
}

w_rc_t test_nochange(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    shpid_t target_pid;
    w_keystr_t target_key0, target_key1;
    W_DO (prepare_test(ssm, test_volume, stid, root_pid, target_pid, target_key0, target_key1));
    // no change after backup and immediately corrupt
    corrupt_page(test_volume, target_pid);

    // this should invoke SPR with no REDO application
    char buf[SM_PAGESIZE / 6];
    smsize_t buf_len = SM_PAGESIZE / 6;
    bool found;
    W_DO(ssm->find_assoc(stid, target_key0, buf, buf_len, found));
    EXPECT_TRUE(found);
    EXPECT_EQ(SM_PAGESIZE / 6, buf_len);
    for (int i = 0; i < SM_PAGESIZE / 6; ++i) {
        EXPECT_EQ('a', buf[i]) << i;
        if (buf[i] != 'a') {
            break;
        }
    }

    return RCOK;
}
TEST (SprTest, NoChange) {
    test_env->empty_logdata_dir();
    sm_options options;
    EXPECT_EQ(0, test_env->runBtreeTest(test_nochange, options));
}

w_rc_t test_one_change(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    shpid_t target_pid;
    w_keystr_t target_key0, target_key1;
    W_DO (prepare_test(ssm, test_volume, stid, root_pid, target_pid, target_key0, target_key1));
    // After taking backup, remove target_key1, then propagate EMLSN change
    W_DO(ssm->begin_xct());
    W_DO(ssm->destroy_assoc(stid, target_key1));
    W_DO(ssm->commit_xct());
    W_DO(flush_and_evict(ssm));

    corrupt_page(test_volume, target_pid);

    // this should invoke SPR with one REDO application
    W_DO(ssm->begin_xct());
    char buf[SM_PAGESIZE / 6];
    smsize_t buf_len = SM_PAGESIZE / 6;
    bool found;
    W_DO(ssm->find_assoc(stid, target_key0, buf, buf_len, found));
    EXPECT_TRUE(found);
    EXPECT_EQ(SM_PAGESIZE / 6, buf_len);
    for (int i = 0; i < SM_PAGESIZE / 6; ++i) {
        EXPECT_EQ('a', buf[i]) << i;
        if (buf[i] != 'a') {
            break;
        }
    }
    W_DO(ssm->find_assoc(stid, target_key1, buf, buf_len, found));
    EXPECT_FALSE(found);
    W_DO(ssm->commit_xct());

    return RCOK;
}
TEST (SprTest, OneChange) {
    test_env->empty_logdata_dir();
    sm_options options;
    EXPECT_EQ(0, test_env->runBtreeTest(test_one_change, options));
}
w_rc_t test_two_changes(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    shpid_t target_pid;
    w_keystr_t target_key0, target_key1;
    W_DO (prepare_test(ssm, test_volume, stid, root_pid, target_pid, target_key0, target_key1));
    // After taking backup, remove target_key0/1, then propagate EMLSN change
    W_DO(ssm->begin_xct());
    W_DO(ssm->destroy_assoc(stid, target_key0));
    W_DO(ssm->destroy_assoc(stid, target_key1));
    W_DO(ssm->commit_xct());
    W_DO(flush_and_evict(ssm));

    corrupt_page(test_volume, target_pid);

    // this should invoke SPR with two REDO applications
    W_DO(ssm->begin_xct());
    char buf[SM_PAGESIZE / 6];
    smsize_t buf_len = SM_PAGESIZE / 6;
    bool found;
    W_DO(ssm->find_assoc(stid, target_key0, buf, buf_len, found));
    EXPECT_FALSE(found);
    W_DO(ssm->find_assoc(stid, target_key1, buf, buf_len, found));
    EXPECT_FALSE(found);
    W_DO(ssm->commit_xct());

    return RCOK;
}
TEST (SprTest, TwoChanges) {
    test_env->empty_logdata_dir();
    sm_options options;
    EXPECT_EQ(0, test_env->runBtreeTest(test_two_changes, options));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
