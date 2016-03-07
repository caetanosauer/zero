// test w_ostrstream_buf, w_ostrstream

#include "w_strstream.h"
#include <iostream>
#include <cstdlib>
#include "gtest/gtest.h"

std::string testit(bool terminate, w_ostrstream &s,
    int argc, const char **argv)
{
    for (int i = 0; i < argc; i++) {
        if (i>0) s << ' ' ;
        s << argv[i];
    }

    if (terminate)
        s << ends;

    cout << "c_str @ " << (void*) s.c_str() << endl;
    const char *t = s.c_str();
    cout << "strlen = " << strlen(t) << endl;
    cout << "buf '" << t << "'" << endl;
    return t;
}

TEST (SSTest, Test1) {
    w_ostrstream_buf    s(40);
    w_ostrstream        s2;
    const char * args[] = {
        "abcd", "fghi", "klmn", "pqrs", "uvwxyz"
    };
    std::string res1 = testit(false, s, 5, args);
    std::string res2 = testit(false, s2, 5, args);
    EXPECT_STREQ (res1.c_str(), res2.c_str());
}

TEST (SSTest, Test2) {
    w_ostrstream_buf    s(30);
    w_ostrstream        s2;
    const char * args[] = {
        "sdkjfhkjsd", "dkjfshkjdhf", "sdfdf", "sss"
    };
    std::string res1 = testit(true, s, 4, args);
    std::string res2 = testit(true, s2, 4, args);
    EXPECT_STREQ (res1.c_str(), res2.c_str());
}
