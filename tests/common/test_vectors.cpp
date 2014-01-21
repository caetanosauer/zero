#include "w_defines.h"
#include "w.h"
#include "basics.h"
#include "vec_t.h"
#include "w_debug.h"

#include <iostream>
#include "w_strstream.h"
#include "gtest/gtest.h"

const char *d = "dddddddddd";
const char *djunk = "Djunk";
const char *b = "bbbbbbbbbb";
const char *bjunk = "Bjunk";
const char *c = "cccccccccc";
const char *cjunk = "Cjunk";
const char *a = "aaaaaaaaaa";
const char *ajunk = "Ajunk";

std::string tostr(const vec_t &v) {
    std::stringstream str;
    str << v;
    return str.str();
}

void V(const vec_t &a, int b, int c, vec_t &d)
{

    DBG(<<"*******BEGIN TEST("  << b << "," << c << ")");

    for(int l = 0; l<100; l++) {
        if(c > (int) a.size()) break;
        a.mkchunk(b, c, d);

        c+=b;
    }
    DBG(<<"*******END TEST");
}

std::string P(const char *s) {
    w_istrstream anon(s);
    vec_t    t;
    anon >> t;
    std::string ret = tostr (t);
    t.vecdelparts();
    return ret;
}

TEST(VectorTest, Misc) {
    vec_t test;
    vec_t tout;

#define TD(i,j) test.put(&d[i], j); 
#define TB(i,j) test.put(&b[i], j);
#define TA(i,j) test.put(&a[i], j); 
#define TC(i,j) test.put(&c[i], j);

    TA(0,10);
    TB(0,10);
    TC(0,10);
    TD(0,10);


    V(test, 5, 7, tout);
    V(test, 5, 10, tout);
    V(test, 5, 22, tout);

    V(test, 11, 0, tout);
    V(test, 11, 7, tout);
    V(test, 11, 9, tout);

    V(test, 30, 9, tout);
    V(test, 30, 29, tout);
    V(test, 30, 40, tout);
    V(test, 40, 30, tout);

    V(test, 100, 9, tout);

    EXPECT_STREQ (P("{ {1   \"}\" }").c_str(), "{ {1 \"}\" }}");
    /*{{{*/
    EXPECT_STREQ (P("{ {3 \"}}}\"      }}").c_str(), "{ {3 \"}<2 times>\" }}");
    EXPECT_STREQ (P("{ {30 \"xxxxxxxxxxyyyyyyyyyyzzzzzzzzzz\"} }").c_str(), "{ {30 \"x<9 times>y<9 times>z<9 times>\" }}");
    EXPECT_STREQ (P("{ {30 \"xxxxxxxxxxyyyyyyyyyyzzzzzzzzzz\"}{10    \"abcdefghij\"} }").c_str(), "{ {30 \"x<9 times>y<9 times>z<9 times>\" }{10 \"abcdefghij\" }}");
}
TEST(VectorTest, Put) {
    vec_t t;
    t.reset();
    t.put("abc",3);
    EXPECT_STREQ(tostr(t).c_str(), "{ {3 \"abc\" }}");
}

TEST(VectorTest, Endian) {
// This is endian-dependent, so let's adjust the test program
// to match the "vector.out" file, since this is easier than
// dealing with different .out files.
#ifdef WORDS_BIGENDIAN
                int n = 0x03000080;
                int m = 0xfcffffef;
#else
                int n = 0x80000003;
                int m = 0xeffffffc;
#endif
                vec_t   num( (void*)&n, sizeof(n));
                vec_t   num2( (void*)&m, sizeof(m));

                // vec containing 0x80000003 (little-endian)
                EXPECT_STREQ(tostr(num).c_str(), "{ {4 \"\\03\\0<1 times>\\0200\" }}");

                // vec containing 0xeffffffc (little-endian)
                EXPECT_STREQ(tostr(num2).c_str(), "{ {4 \"\\0374\\0377<1 times>\\0357\" }}");
}

TEST(VectorTest, Print) {
    char c = 'h';
    char last = (char)'\377';
    char last1 = '\377';
    char last2 = (char)0xff;

    vec_t   cv( (void*)&c, sizeof(c));
    vec_t   lastv( (void*)&last, sizeof(last));
    vec_t   last1v( (void*)&last1, sizeof(last1));
    vec_t   last2v( (void*)&last2, sizeof(last2));

    EXPECT_LT(cv, lastv);
    EXPECT_STREQ(tostr(cv).c_str(), "{ {1 \"h\" }}");
    EXPECT_STREQ(tostr(lastv).c_str(), "{ {1 \"\\0377\" }}");
    EXPECT_STREQ(tostr(last1v).c_str(), "{ {1 \"\\0377\" }}");
    EXPECT_STREQ(tostr(last2v).c_str(), "{ {1 \"\\0377\" }}");
}

