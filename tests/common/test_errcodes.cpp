#include "w_error.h"
#include "gtest/gtest.h"

TEST(ErrcodesTest, FcFull) {
    EXPECT_STREQ(w_error_name(fcFULL), "fcFULL");
    EXPECT_STREQ(w_error_message(fcFULL), "Container is full");
}

TEST(ErrcodesTest, eDEADLOCK) {
    EXPECT_STREQ(w_error_name(eDEADLOCK), "eDEADLOCK");
    EXPECT_STREQ(w_error_message(eDEADLOCK), "deadlock detected");
}
