#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "vol.h"
#include "btree_page_h.h"

btree_test_env *test_env;

/**
 * Unit test for creating BTree.
 */

w_rc_t create_test(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(ssm->begin_xct());
    W_DO(ssm->print_index(stid));
    W_DO(ssm->commit_xct());
    return RCOK;
}

TEST (BtreeCreateTest, Create) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(create_test), 0);
}

w_rc_t create_test2(ss_m* ssm, test_volume_t *test_volume) {
    W_DO(ssm->begin_xct());
    stid_t stid;
    W_DO(ssm->create_index(test_volume->_vid, stid));
    W_DO(ssm->abort_xct());

    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(ssm->begin_xct());
    W_DO(ssm->print_index(stid));
    W_DO(ssm->commit_xct());
    return RCOK;
}

TEST (BtreeCreateTest, Create2) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(create_test2), 0);
}

w_rc_t create_check(ss_m* ssm, test_volume_t *test_volume) {
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(ssm->force_buffers());
    generic_page buf;

    // CS: why reading 5 pages?? (magic number)
    for (shpid_t shpid = 1; shpid < 5; ++shpid) {
        lpid_t pid (stid, shpid);

        cout << "checking pid " << shpid << ":";
        W_IGNORE(smlevel_0::vol->get(test_volume->_vid)->read_page(pid.page, &buf));
        cout << "full-pid=" << buf.pid << ",";
        switch (buf.tag) {
            case t_bad_p: cout << "t_bad_p"; break;
            case t_alloc_p: cout << "t_alloc_p"; break;
            case t_stnode_p: cout << "t_stnode_p"; break;
            case t_btree_p: cout << "t_btree_p"; break;
            default:
                cout << "wtf?? " << buf.tag; break;
        }
        if (buf.tag == t_btree_p) {
            btree_page_h p;
            p.fix_nonbufferpool_page(&buf);
            cout << "(level=" << p.level() << ")";
        }

        cout << endl;
    }

    return RCOK;
}

TEST (BtreeCreateTest, CreateCheck) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(create_check), 0);
}

//#include <google/profiler.h>

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    // even this simple test takes substantial time. might be good to tune startup/shutdown
    //::ProfilerStart("create.prof");
    int ret;
    //for (int i = 0; i < 20; ++i)
        ret = RUN_ALL_TESTS();
    //::ProfilerStop();
    return ret;
}
