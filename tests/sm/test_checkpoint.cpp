#define private public

#include "btree_test_env.h"
#include "sm_base.h"
#include "log_core.h"
#include "vol.h"
#include "chkpt.h"

btree_test_env *test_env;
vol_t* volMgr;
std::string volpath_base;

void init()
{
    volMgr = smlevel_0::vol;
    volpath_base = string(test_env->vol_dir) + "/volume";
}

/*
w_rc_t backupTable(ss_m* ssm, test_volume_t *test_volume) {
    BackupManager *bk = ssm->bk;
    vid_t vid = test_volume->_vid;
    x_delete_backup(ssm, test_volume);
    EXPECT_FALSE(bk->volume_exists(vid));

    W_DO(x_take_backup(ssm, test_volume));
    EXPECT_TRUE(bk->volume_exists(vid));

    W_DO(ss_m::checkpoint_sync());

    x_delete_backup(ssm, test_volume);
    EXPECT_FALSE(bk->volume_exists(vid));
    return RCOK;
}

TEST (CheckpointTest, backupTable) {
    test_env->empty_logdata_dir();
    sm_options options;
    options.set_bool_option("sm_testenv_init_vol", true);
    EXPECT_EQ(test_env->runBtreeTest(backupTable, options), 0);
}
*/


rc_t bufferTable(ss_m* ssm, test_volume_t* test_vol)
{
    StoreID* _stid_list = new StoreID[1];
    PageID  _root_pid;

    W_DO(x_btree_create_index(ssm, test_vol, _stid_list[0], _root_pid));
    W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit));  // flags: Checkpoint, commit
    W_DO(ss_m::checkpoint_sync());

    //chkpt_t chkpt;
    //ssm->chkpt->scan_log(begin_scan, end_scan, chkpt);

    W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_in_flight, false, '1'));  // flags: Checkpoint, commit
    W_DO(ss_m::checkpoint_sync());

    W_DO(ss_m::force_volume());
    //W_DO(ss_m::checkpoint_sync());

    return RCOK;
}
TEST (CheckpointTest, bufferTable) {
    test_env->empty_logdata_dir();
    sm_options options;
    options.set_bool_option("sm_testenv_init_vol", true);
    EXPECT_EQ(test_env->runBtreeTest(bufferTable, options), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
