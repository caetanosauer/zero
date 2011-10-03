#include "w_defines.h"

#include "basics.h"
#include <cassert>
#include <iostream>
#include "tid_t.h"
#include "gtest/gtest.h"

#ifdef __GNUG__
template ostream &operator<<(ostream &, const opaque_quantity<max_server_handle_len> &);
#endif


TEST(OpaqueTest, All) {
    // test unaligned vectors if possible;

    char dummy[500];

    char *d = &dummy[3];
    server_handle_t *s = 0;

    int j(0);
    int &k = *((int *)&dummy[3]);
    char * junk = (char *)&k;
    // memcpy((char *)&k, &j, sizeof(j)); // dumps core
    memcpy(junk, &j, sizeof(j)); // ok
    memcpy(&dummy[3], &j, sizeof(j)); // ok


    {
        s = (server_handle_t *)dummy; // aligned, if possible
        *s = "COPY";
        server_handle_t &th = *s;
        server_handle_t ch = th;
        std::stringstream str;
        str << ch;
        EXPECT_STREQ (str.str().c_str(), "opaque[4]\"COPY\"");
        EXPECT_EQ (ch.length(), (uint) 4);
    }
    {
        s = (server_handle_t *)dummy; // aligned, if possible
        *s = "ALIGNED";
        std::stringstream str;
        str << *s;
        EXPECT_STREQ (str.str().c_str(), "opaque[7]\"ALIGNED\"");
        EXPECT_EQ (s->length(), (uint) 7);
        cout << "(not an error: )address of s = " << W_ADDR(s) << endl;
    }

    {
        s = (server_handle_t *)d; // unaligned, if possible
        *s = "NOTALIGNED";
        std::stringstream str;
        str << *s;
        EXPECT_STREQ (str.str().c_str(), "opaque[10]\"NOTALIGNED\"");
        EXPECT_EQ (s->length(), (uint) 10);
        cout << "(not an error: )address of s = " << W_ADDR(s) << endl;
    }

    {
        s = (server_handle_t *)dummy; // aligned, if possible
        *s = "BYTEORDER";
        std::stringstream str;
        str << *s;
        EXPECT_STREQ (str.str().c_str(), "opaque[9]\"BYTEORDER\"");
        EXPECT_EQ (s->length(), (uint) 9);

        s->hton();
        // net order can change depending on platform
        cout << "hton length of s = " << hex << s->length() << endl;
        
        // but the bytes should always be the same!
        cout << "hton bytes of s = ";
        // XXX magic types/numbers, but "well known"
        union {
            uint32_t    l;
            uint8_t    c[4];
        } un;
        un.l = s->length();
        EXPECT_EQ (sizeof(un.c), (uint) 4);
        EXPECT_EQ (un.c[0], 0);
        EXPECT_EQ (un.c[1], 0);
        EXPECT_EQ (un.c[2], 0);
        EXPECT_EQ (un.c[3], 9);

        s->ntoh();
        EXPECT_EQ (s->length(), (uint) 9);
    }

    {
        tid_t uninit;
        tid_t t1 = uninit;
        tid_t t2 = tid_t();
        tid_t t3 = t2;
        t3 = t1;
    }
}

