#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "w_key.h"
#include "smthread.h"
#include "xct.h"

btree_test_env *test_env;

w_rc_t empty_xct(ss_m*, test_volume_t *) {
    size_t original_depth = me()->get_tcb_depth();

    // commit
    {
        sys_xct_section_t sxs;
        EXPECT_EQ(original_depth + 1, me()->get_tcb_depth());
        EXPECT_FALSE(sxs.check_error_on_start().is_error());
        EXPECT_FALSE(sxs.end_sys_xct (RCOK).is_error());
    }
    EXPECT_EQ(original_depth, me()->get_tcb_depth());

    // abort
    {
        sys_xct_section_t sxs;
        EXPECT_FALSE(sxs.check_error_on_start().is_error());
        EXPECT_FALSE(sxs.end_sys_xct (RC(eOUTOFSPACE)).is_error());
    }
    EXPECT_EQ(original_depth, me()->get_tcb_depth());

    // abort without explicit report
    {
        sys_xct_section_t sxs;
        EXPECT_FALSE(sxs.check_error_on_start().is_error());
    }
    EXPECT_EQ(original_depth, me()->get_tcb_depth());

    return RCOK;
}

TEST (SystemTransactionTest, EmptyXct) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(empty_xct), 0);
}

w_rc_t empty_nested_xct(ss_m *ssm, test_volume_t *) {
    size_t original_depth = me()->get_tcb_depth();

    // commit -> commit
    W_DO(ssm->begin_xct());
    EXPECT_EQ(original_depth + 1, me()->get_tcb_depth());
    {
        sys_xct_section_t sxs;
        EXPECT_EQ(original_depth + 2, me()->get_tcb_depth());
        EXPECT_FALSE(sxs.check_error_on_start().is_error());
        {
            sys_xct_section_t sxs2;
            EXPECT_EQ(original_depth + 3, me()->get_tcb_depth());
            EXPECT_FALSE(sxs2.check_error_on_start().is_error());
            EXPECT_FALSE(sxs2.end_sys_xct (RCOK).is_error());
        }
        EXPECT_FALSE(sxs.end_sys_xct (RCOK).is_error());
    }
    W_DO(ssm->commit_xct());
    EXPECT_EQ(original_depth, me()->get_tcb_depth());

    // commit -> abort
    W_DO(ssm->begin_xct());
    {
        sys_xct_section_t sxs;
        EXPECT_FALSE(sxs.check_error_on_start().is_error());
        {
            sys_xct_section_t sxs2;
            EXPECT_FALSE(sxs2.check_error_on_start().is_error());
            EXPECT_FALSE(sxs2.end_sys_xct (RC(eOUTOFSPACE)).is_error());
        }
        EXPECT_FALSE(sxs.end_sys_xct (RCOK).is_error());
    }
    W_DO(ssm->commit_xct());
    EXPECT_EQ(original_depth, me()->get_tcb_depth());

    // abort -> commit
    W_DO(ssm->begin_xct());
    {
        sys_xct_section_t sxs;
        EXPECT_FALSE(sxs.check_error_on_start().is_error());
        {
            sys_xct_section_t sxs2;
            EXPECT_FALSE(sxs2.check_error_on_start().is_error());
            EXPECT_FALSE(sxs2.end_sys_xct (RCOK).is_error());
        }
        EXPECT_FALSE(sxs.end_sys_xct (RC(eOUTOFSPACE)).is_error());
    }
    W_DO(ssm->commit_xct());
    EXPECT_EQ(original_depth, me()->get_tcb_depth());

    // abort -> abort
    W_DO(ssm->begin_xct());
    {
        sys_xct_section_t sxs;
        EXPECT_FALSE(sxs.check_error_on_start().is_error());
        {
            sys_xct_section_t sxs2;
            EXPECT_FALSE(sxs2.check_error_on_start().is_error());
            EXPECT_FALSE(sxs2.end_sys_xct (RC(eOUTOFSPACE)).is_error());
        }
        EXPECT_FALSE(sxs.end_sys_xct (RC(eOUTOFSPACE)).is_error());
    }
    W_DO(ssm->commit_xct());
    EXPECT_EQ(original_depth, me()->get_tcb_depth());

    return RCOK;
}

TEST (SystemTransactionTest, EmptyNestedXct) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(empty_nested_xct), 0);
}

w_rc_t fail_user_nest(ss_m *ssm, test_volume_t *) {
    size_t original_depth = me()->get_tcb_depth();
    W_DO(ssm->begin_xct());
    EXPECT_EQ(original_depth + 1, me()->get_tcb_depth());

    rc_t result = ssm->begin_xct();
    EXPECT_EQ(eINTRANS, result.err_num());
    EXPECT_EQ(original_depth + 1, me()->get_tcb_depth());

    W_DO(ssm->commit_xct());
    EXPECT_EQ(original_depth, me()->get_tcb_depth());

    return RCOK;
}

TEST (SystemTransactionTest, FailUserNest) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(fail_user_nest), 0);
}

w_rc_t usercommit_syscommit(ss_m *ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "aa1", "data1"));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa3", "data3"));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa5", "data5"));

    W_DO (x_btree_verify(ssm, stid));

    size_t original_depth = me()->get_tcb_depth();

    // user commit -> sys commit
    W_DO(ssm->begin_xct());
    W_DO(x_btree_insert(ssm, stid, "aa6", "data6"));
    {
        sys_xct_section_t sxs;
        W_DO(x_btree_insert(ssm, stid, "aa7", "data7"));
        EXPECT_FALSE(sxs.end_sys_xct (RCOK).is_error());
    }
    W_DO(ssm->commit_xct());
    EXPECT_EQ(original_depth, me()->get_tcb_depth());
    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s));
        EXPECT_EQ (5, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa7"), s.maxkey);
    }

    W_DO (x_btree_verify(ssm, stid));

    return RCOK;
}

TEST (SystemTransactionTest, UserCommitSysCommit) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(usercommit_syscommit), 0);
}

w_rc_t userabort_syscommit(ss_m *ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "aa1", "data1"));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa3", "data3"));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa5", "data5"));

    W_DO (x_btree_verify(ssm, stid));

    size_t original_depth = me()->get_tcb_depth();

    // user abort -> sys commit
    W_DO(ssm->begin_xct());
    W_DO(x_btree_insert(ssm, stid, "aa6", "data6"));
    {
        sys_xct_section_t sxs;
        W_DO(x_btree_insert(ssm, stid, "aa7", "data7"));
        EXPECT_FALSE(sxs.end_sys_xct (RCOK).is_error());
    }
    W_DO(ssm->abort_xct());
    EXPECT_EQ(original_depth, me()->get_tcb_depth());
    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s));
        EXPECT_EQ (4, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa7"), s.maxkey);
    }

    W_DO (x_btree_verify(ssm, stid));

    return RCOK;
}

TEST (SystemTransactionTest, UserAbortSysCommit) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(userabort_syscommit), 0);
}


w_rc_t userabort_sysabort(ss_m *ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_insert_and_commit (ssm, stid, "aa1", "data1"));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa3", "data3"));
    W_DO(x_btree_insert_and_commit (ssm, stid, "aa5", "data5"));

    W_DO (x_btree_verify(ssm, stid));

    size_t original_depth = me()->get_tcb_depth();

    // user abort -> sys abort
    W_DO(ssm->begin_xct());
    W_DO(x_btree_remove(ssm, stid, "aa1"));
    {
        sys_xct_section_t sxs;
        W_DO(x_btree_remove(ssm, stid, "aa5"));
        EXPECT_FALSE(sxs.end_sys_xct (RC(eOUTOFSPACE)).is_error());
    }
    W_DO(ssm->abort_xct());
    EXPECT_EQ(original_depth, me()->get_tcb_depth());
    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa5"), s.maxkey);
    }

    W_DO (x_btree_verify(ssm, stid));

    return RCOK;
}

TEST (SystemTransactionTest, UserAbortSysAbort) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(userabort_sysabort), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
