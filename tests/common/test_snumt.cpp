#include "w_defines.h"
#include "w.h"
#include "basics.h"
#include "stid_t.h"
#include "gtest/gtest.h"

TEST (SnumtTest, Compare) {
    compare_snum_t compartor;
    EXPECT_TRUE (compartor.operator()(100, 101));
    EXPECT_TRUE (compartor.operator()(100, 102));
    EXPECT_TRUE (compartor.operator()(101, 102));
    EXPECT_FALSE (compartor.operator()(102, 101));
    EXPECT_FALSE (compartor.operator()(101, 101));
    EXPECT_FALSE (compartor.operator()(101, 100));
}


