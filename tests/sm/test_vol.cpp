#include "btree_test_env.h"

#include <sstream>

#include "sm_options.h"
#include "log_core.h"
#include "vol.h"

btree_test_env* test_env;
sm_options options;
vol_m* volMgr;

std::string volpath;

#define DEFAULT_TEST(test, function) \
    TEST (test, function) { \
        test_env->empty_logdata_dir(); \
        options.set_bool_option("sm_testenv_init_vol", false); \
        EXPECT_EQ(test_env->runBtreeTest(function, options), 0); \
    }

void init()
{
    volMgr = smlevel_0::vol;
    volpath = string(test_env->vol_dir) + "/volume";
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

rc_t createAndMountTest(ss_m*, test_volume_t*)
{
    init();
    shpid_t numPages = shpid_t(8);
    vid_t vid;
    checkEmpty();

    W_COERCE(volMgr->sx_format(volpath.c_str(), numPages, vid, false));
    W_COERCE(volMgr->sx_mount(volpath.c_str(), false));

    EXPECT_EQ(1, volMgr->num_vols());
    EXPECT_EQ(vid + 1, volMgr->get_next_vid());

    vol_t* vol = volMgr->get(vid);
    vol_t* vol2 = volMgr->get(volpath.c_str());

    EXPECT_TRUE(vol != NULL);
    EXPECT_TRUE(vol == vol2);
    EXPECT_TRUE(vol->vid() == vid);
    EXPECT_TRUE(vol->num_pages() == numPages);

    std::vector<string> names;
    std::vector<vid_t> vids;
    W_COERCE(volMgr->list_volumes(names, vids));

    EXPECT_EQ(1, names.size());
    EXPECT_EQ(1, vids.size());
    EXPECT_EQ(volpath, names[0]);
    EXPECT_EQ(vid, vids[0]);

    // now dismount and check volMgr state
    W_COERCE(volMgr->sx_dismount(volpath.c_str(), false));

    checkEmpty();

    EXPECT_EQ(vid + 1, volMgr->get_next_vid());
    EXPECT_EQ(NULL, volMgr->get(vid));
    EXPECT_EQ(NULL, volMgr->get(volpath.c_str()));

    return RCOK;
}
DEFAULT_TEST(VolMgrState, createAndMountTest);

void createAndMountMultiple(bool format)
{
    shpid_t numPages = shpid_t(8);
    std::vector<vid_t> vids;
    vids.resize(vol_m::MAX_VOLS, vid_t(0));
    checkEmpty();

    for (int i = 0; i < vol_m::MAX_VOLS; i++) {
        stringstream ss;
        ss << volpath << i;

        if (format) {
            W_COERCE(volMgr->sx_format(ss.str().c_str(), numPages + i, vids[i], false));
        }
        W_COERCE(volMgr->sx_mount(ss.str().c_str(), false));
        if (!format) {
            // Very weird bug! path changes after calling sx_mount above!
            // It changes inside bf_fixed_m::init, when allocating memory for the
            // fixed buffer, which makes absolutely no sense!
            vids[i] = volMgr->get(ss.str().c_str())->vid();
        }
    }

    EXPECT_EQ(vol_m::MAX_VOLS, volMgr->num_vols());
    EXPECT_EQ(vol_m::MAX_VOLS + 1, volMgr->get_next_vid());

    std::vector<string> names;
    std::vector<vid_t> m_vids;
    W_COERCE(volMgr->list_volumes(names, m_vids));

    EXPECT_EQ(vol_m::MAX_VOLS, names.size());
    EXPECT_EQ(vol_m::MAX_VOLS, m_vids.size());

    for (int i = 0; i < vol_m::MAX_VOLS; i++) {
        stringstream ss;
        ss << volpath << i;

        vol_t* vol = volMgr->get(vids[i]);
        vol_t* vol2 = volMgr->get(ss.str().c_str());

        EXPECT_TRUE(vol != NULL);
        EXPECT_TRUE(vol == vol2);
        EXPECT_TRUE(vol->vid() == vids[i]);
        EXPECT_TRUE(vol->num_pages() == numPages + i);

        EXPECT_EQ(ss.str(), names[i]);
        EXPECT_EQ(vids[i], m_vids[i]);

        W_COERCE(volMgr->sx_dismount(names[i].c_str()));
    }

    checkEmpty();

    for (int i = 0; i < vol_m::MAX_VOLS; i++) {
        stringstream ss;
        ss << volpath << i;
        EXPECT_EQ(NULL, volMgr->get(vids[i]));
        EXPECT_EQ(NULL, volMgr->get(ss.str().c_str()));
    }
}

rc_t createAndMountMultipleTest(ss_m*, test_volume_t*)
{
    init();
    createAndMountMultiple(true);
    return RCOK;
}
DEFAULT_TEST(VolMgrState, createAndMountMultipleTest);

rc_t remountMultipleTest(ss_m*, test_volume_t*)
{
    init();
    createAndMountMultiple(true);
    createAndMountMultiple(false);
    return RCOK;
}
DEFAULT_TEST(VolMgrState, remountMultipleTest);


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
