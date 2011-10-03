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
    RC_PUSH(rc, fcINTERNAL);
    RC_AUGMENT(rc);
    RC_PUSH(rc, fcFULL);
    RC_AUGMENT(rc);
    RC_PUSH(rc, fcEMPTY);
    RC_AUGMENT(rc);
    RC_PUSH(rc, fcNOTFOUND);
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
        // Turn on checking but turn off W_FATAL response.
        w_rc_t::set_return_check(true, false);

    {
        w_rc_t rc;
                cout << "Expect one 'error not checked' message" << endl;
                rc = _testing();
    }

    {
                cout << "Expect another 'error not checked' message" << endl;
        _testing_ok();
    }

    EXPECT_EQ(_testing_ok().is_error(), 0)
        << "FAILURE: This should never happen!";

    cout << "Expect 3 forms of the string of errors" << endl;
        {
                        w_rc_t rc = _testing();
                {
                        //////////////////////////////////////////////////// 
                        // this gets you to the integer values, one at a time
                        //////////////////////////////////////////////////// 
                        cout << "*************************************" << endl;
                        w_rc_i iter(rc);
                        cout << endl << "1 : List of error numbers:" << endl;
                        for(int32_t e = iter.next_errnum();
                                e!=0; e = iter.next_errnum()) {
                        cout << "error = " << e << endl;
                        }
                        cout << "End list of error numbers:" << endl;
                }
                {
                        //////////////////////////////////////////////////// 
                        // this gets you to the w_error_t structures, one
                        // at a time.  If you print each one, though, you
                        // get it PLUS everything attached to it
                        //////////////////////////////////////////////////// 
                        w_rc_i iter(rc);
                        cout << "*************************************" << endl;
                        cout << endl << "2 : List of w_error_t:" << endl;
                        for(const w_error_t *e = iter.next();
                                e; 
                                e = iter.next()) {
                        cout << "error = " << *e << endl;
                        }
                        cout << "End list of w_error_t:" << endl;
                }
                {
                        cout << "*************************************" << endl;
                        cout << endl << "3 : print the rc:" << endl;
                        cout << "error = " << rc << endl;
                        cout << "End print the rc:" << endl;
                }
        }

        {
                w_rc_t rc = _testing1();
                w_assert1(rc.is_error());
                cout << " ORIG:" << rc << endl;
                w_rc_i it(rc);

        w_rc_t rc2(rc);
                cout << " COPY CONSTRUCTOR: " << rc2 << endl;

        rc2 = rc;
                w_assert1(rc2.is_error());
                cout << " COPY OPERATOR: " << rc2 << endl;

        }
}

