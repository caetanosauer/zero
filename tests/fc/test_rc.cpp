#include "w_defines.h"
#include "w_stream.h"
#include <cstddef>
#include "w.h"
#include "gtest/gtest.h"


w_rc_t _testing2()
{

    w_rc_t rc = RC(fcOS);
    RC_AUGMENT(rc);
    RC_AUGMENT(rc);
    RC_AUGMENT(rc);
    RC_AUGMENT(rc);
    RC_AUGMENT(rc);

    // This constitutes a check on THIS rc
    if (rc.is_error())  {; } 

    return rc;
}
w_rc_t _testing1()
{
    return _testing2();
}

w_rc_t _testing()
{

    w_rc_t rc = RC(fcOS);
    RC_AUGMENT(rc);
    RC_AUGMENT(rc);
    RC_AUGMENT(rc);
    RC_AUGMENT(rc);
    RC_AUGMENT(rc);

    // This constitutes a check on THIS rc
    if (rc.is_error())  {; } 

    return rc;
}

w_rc_t _testing_ok()
{
    return RCOK;
}

TEST(RcTest, All) {
    {
        w_rc_t rc;
        rc = _testing();
    }

    std::cout << "Expect another 'error not checked' message" << std::endl;
    _testing_ok();
    EXPECT_FALSE(_testing_ok().is_error()) << "FAILURE: This should never happen!";

    std::cout << "Expect 3 forms of the string of errors" << std::endl;
    {
        w_rc_t rc = _testing();
        std::cout << std::endl << rc << std::endl;
    }

    {
        w_rc_t rc = _testing1();
        EXPECT_TRUE(rc.is_error());
        std::cout << " ORIG:" << rc << std::endl;

        w_rc_t rc2(rc);
        std::cout << " COPY CONSTRUCTOR: " << rc2 << std::endl;

        rc2 = rc;
        EXPECT_TRUE(rc2.is_error());
        std::cout << " COPY OPERATOR: " << rc2 << std::endl;
    }
}

