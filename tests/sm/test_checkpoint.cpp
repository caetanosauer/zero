#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "xct.h"
#include "sm_base.h"
#include "sm_external.h"

btree_test_env *test_env;
vol_m* volMgr;
std::string volpath_base;

//#define DEFAULT_TEST(test, function) \
//    TEST (test, function) { \
//        test_env->empty_logdata_dir(); \
//        options.set_bool_option("sm_testenv_init_vol", false); \
//        EXPECT_EQ(test_env->runBtreeTest(function, options), 0); \
//    }

void init()
{
    volMgr = smlevel_0::vol;
    volpath_base = string(test_env->vol_dir) + "/volume";
}

void checkEmpty()
{
    EXPECT_EQ(0, volMgr->num_vols());
    std::vector<string> names;
    std::vector<vid_t> vids;
    W_COERCE(volMgr->list_volumes(names, vids));
    EXPECT_EQ(0, names.size());
    EXPECT_EQ(0, vids.size());
}

rc_t deviceTable(ss_m*, test_volume_t*)
{
    init();
    shpid_t numPages = shpid_t(8);
    vid_t vid;
    std::string volpath;

    checkEmpty();    

    //Device 1
    volpath = volpath_base + "_1";
    W_COERCE(volMgr->sx_format(volpath.c_str(), numPages, vid, true));
    W_COERCE(volMgr->sx_mount(volpath.c_str(), true));

    EXPECT_EQ(1, volMgr->num_vols());
    EXPECT_EQ(vid + 1, volMgr->get_next_vid());

     //Device 2
    volpath = volpath_base + "_2";
    W_COERCE(volMgr->sx_format(volpath.c_str(), numPages, vid, true));
    W_COERCE(volMgr->sx_mount(volpath.c_str(), true));

    EXPECT_EQ(2, volMgr->num_vols());
    EXPECT_EQ(vid + 1, volMgr->get_next_vid());

     //Device 3
    volpath = volpath_base + "_3";
    W_COERCE(volMgr->sx_format(volpath.c_str(), numPages, vid, true));
    W_COERCE(volMgr->sx_mount(volpath.c_str() , true));

    EXPECT_EQ(3, volMgr->num_vols());
    EXPECT_EQ(vid + 1, volMgr->get_next_vid());

    W_DO(ss_m::checkpoint_sync());

     //Device 4
    volpath = volpath_base + "_4";
    W_COERCE(volMgr->sx_format(volpath.c_str(), numPages, vid, true));
    W_COERCE(volMgr->sx_mount(volpath.c_str() , true));

    EXPECT_EQ(4, volMgr->num_vols());
    EXPECT_EQ(vid + 1, volMgr->get_next_vid());

    W_DO(ss_m::checkpoint_sync());


    // now dismount and check volMgr state
    volpath = volpath_base + "_1";
    W_COERCE(volMgr->sx_dismount(volpath.c_str(), false));

    volpath = volpath_base + "_2";
    W_COERCE(volMgr->sx_dismount(volpath.c_str(), false));

    volpath = volpath_base + "_3";
    W_COERCE(volMgr->sx_dismount(volpath.c_str(), false));

    volpath = volpath_base + "_4";
    W_COERCE(volMgr->sx_dismount(volpath.c_str(), false));

    checkEmpty();

    return RCOK;
}
TEST (CheckpointTest, deviceTable) {
    test_env->empty_logdata_dir();
    sm_options options;
    options.set_bool_option("sm_testenv_init_vol", false);
    EXPECT_EQ(test_env->runBtreeTest(deviceTable, options), 0);
}

rc_t bufferTable(ss_m* ssm, test_volume_t* test_vol)
{
    stid_t* _stid_list = new stid_t[1];
    lpid_t  _root_pid;

    W_DO(x_btree_create_index(ssm, test_vol, _stid_list[0], _root_pid));

    for(int i=0; i<10; i++) {
        ostringstream key;
        ostringstream data;
        key << "aa" << i;
        data << "data" << i;
        W_DO(test_env->btree_insert_and_commit(_stid_list[0], key.str().c_str(), data.str().c_str()));
    }
    W_DO(ss_m::force_buffers());
    //W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa4", "data4"));
    //W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
    //W_DO(test_env->btree_populate_records(_stid_list[0], false, t_test_txn_commit));  // flags: Checkpoint, commit

    W_DO(ss_m::checkpoint_sync());

    //W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa4", "data4"));
    //W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa1", "data1"));
    //W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa5", "data5"));
    //W_DO(test_env->btree_insert_and_commit(_stid_list[0], "aa2", "data2"));

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