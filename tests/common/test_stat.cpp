#include "w_defines.h"
#include "w.h"
#include "w_base.h"
#include "w_stat.h"

#include <iostream>
#include "gtest/gtest.h"


class test_stat {
public:
    /* add the stats */
#include "test_stat_struct_gen.h"

    test_stat() : 
        b(1),
        f(5.4321),
        i(300),
        j((unsigned)0x333),
        u(3),
        k((float)1.2345),
        l(4),
        v(0xffffffffffffffffull),
        x(5),
        d(6.789),
        sum(0.0) { }

    void inc();
    void dec();
    void compute() {
        sum = (float)(i + j + k);
    }
};

#include "test_stat_out_gen.cpp"

// the strings:
const char *test_stat ::stat_names[] = {
#include "test_stat_msg_gen.h"
        NULL
};


void
test_stat::dec() 
{
    i--;
    j--;
    k-=1.0;
    v --;
    compute();
}
void
test_stat::inc() 
{
    i++;
    j++;
    k+=1.0;
    v ++;
    compute();
}

// make it easy to change LINENO to __LINE__
// but have __LINE__ disabled for now because it makes it easy
// to get false failures in testall
#define LINENO " "
TEST (StatTest, All) {
    class test_stat TSTA; // my test class that uses stats
    class test_stat TSTB; // my test class that uses stats
    TSTA.compute();
    TSTB.dec();

    EXPECT_EQ (TSTA.b, TSTB.b);
    EXPECT_FLOAT_EQ (TSTA.f, TSTB.f);
    EXPECT_EQ (TSTA.i, TSTB.i + 1);
    EXPECT_EQ (TSTA.j, TSTB.j + 1);
    EXPECT_EQ (TSTA.u, TSTB.u);
    EXPECT_FLOAT_EQ (TSTA.k, TSTB.k + 1.0f);
    EXPECT_EQ (TSTA.l, TSTB.l);
    EXPECT_EQ (TSTA.v, TSTB.v + 1);
    EXPECT_EQ (TSTA.x, TSTB.x);
    EXPECT_FLOAT_EQ (TSTA.d, TSTB.d);

    EXPECT_FLOAT_EQ (TSTA.sum, TSTB.sum + 3.0f);
}

