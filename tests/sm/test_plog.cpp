#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"

#include "sm_base.h"
#include "sm_external.h"

#include "logbuf_common.h"
#include "logbuf_core.h"
#include "logbuf_seg.h"
#include "log_core.h"
#include "log_storage.h"

#include "logrec.h"
#include "lsn.h"

#include "w_debug.h"

#include <pthread.h>
#include <memory.h>
#include <Lintel/AtomicCounter.hpp>

#define protected public // to access protected fields of plog_t
#include "xct.h"
#include "plog_xct.h"

btree_test_env *test_env = NULL;
plog_xct_t* _xct = NULL;


void init()
{
    _xct = new plog_xct_t();
}

void finish()
{
    delete _xct;
}

rc_t test_scan(ss_m* ssm, test_volume_t* vol)
{
    init();
    W_DO(log_comment("test1"));
    W_DO(log_comment("test2"));
    W_DO(log_comment("test3"));

    { // forward scan
        plog_t::plog_iter_t* iter = _xct->plog.iterate_forwards();
        logrec_t* lr = NULL;
        EXPECT_EQ(iter->next(lr), true);
        EXPECT_EQ(lr->type(), logrec_t::t_comment);
        EXPECT_EQ(strncmp(lr->data(), "test1", 5), 0);
        EXPECT_EQ(iter->next(lr), true);
        EXPECT_EQ(lr->type(), logrec_t::t_comment);
        EXPECT_EQ(strncmp(lr->data(), "test2", 5), 0);
        EXPECT_EQ(iter->next(lr), true);
        EXPECT_EQ(lr->type(), logrec_t::t_comment);
        EXPECT_EQ(strncmp(lr->data(), "test3", 5), 0);
        EXPECT_EQ(iter->next(lr), false);
        delete iter;
    }
    
    { // backward scan
        plog_t::plog_iter_t* iter = _xct->plog.iterate_backwards();
        logrec_t* lr = NULL;
        EXPECT_EQ(iter->next(lr), true);
        EXPECT_EQ(lr->type(), logrec_t::t_comment);
        EXPECT_EQ(strncmp(lr->data(), "test3", 5), 0);
        EXPECT_EQ(iter->next(lr), true);
        EXPECT_EQ(lr->type(), logrec_t::t_comment);
        EXPECT_EQ(strncmp(lr->data(), "test2", 5), 0);
        EXPECT_EQ(iter->next(lr), true);
        EXPECT_EQ(lr->type(), logrec_t::t_comment);
        EXPECT_EQ(strncmp(lr->data(), "test1", 5), 0);
        EXPECT_EQ(iter->next(lr), false);
        delete iter;
    }

    _xct->commit();

    finish();
    return RCOK;
}

TEST(PLogTest, Scan)
{
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(test_scan), 0);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
