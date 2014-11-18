#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"

#include "xct.h"
#include "plog_xct.h"

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

btree_test_env *test_env = NULL;
plog_xct_t* _xct = NULL;


void init()
{
    _xct = new plog_xct_t();
}

rc_t test_insert(ss_m* ssm, test_volume_t* vol)
{
    init();
    return RCOK;
}

TEST(PLogTest, Insert) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(test_insert), 0);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
