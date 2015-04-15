#include "gtest/gtest.h"
#include "logfactory.h"

TEST(LogFactory, Header) {
    LogFactory lf;
    logrec_t* lr = new logrec_t;

    for(int i=0; i<1000; i++) {
        lsn_t oldLSN = lf.getNextLSN();
        EXPECT_TRUE(lf.next(lr));
        EXPECT_TRUE(lr->valid_header(oldLSN));
        EXPECT_EQ(lf.getNextLSN(), oldLSN + lr->length());
    }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}