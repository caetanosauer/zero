#include "w_defines.h"

#include <iostream>

#include "w.h"
#include "basics.h"
#include "vec_t.h"
#include "kvl_t.h"
#include "w_strstream.h"
#include "gtest/gtest.h"

void test_kvl (uint16_t v, uint32_t st, const char *keystr, const char *elemstr) {
    StoreID s = st;

    cvec_t key(keystr, strlen(keystr));
    uint32_t key_hash = 0;
    key.calc_kvl (key_hash);

    cvec_t elem;
    if (elemstr != NULL) {
        elem.set (elemstr, strlen (elemstr));
    }
    uint32_t el_hash = 0;
    elem.calc_kvl (el_hash);

    {
        kvl_t  kvl(s, key, elem);
        EXPECT_EQ (kvl.stid, st);
        EXPECT_EQ (kvl.h, key_hash);
        EXPECT_EQ (kvl.g, el_hash);
    }

    // calclulate kvl for null vector
    {
        //key = nul
        vec_t nul;
        kvl_t kvl(s, nul, elem);
        EXPECT_EQ (kvl.stid, st);
        EXPECT_EQ (kvl.h, (uint32_t) 0);
        EXPECT_EQ (kvl.g, el_hash);
    }

    {
        // el = null
        vec_t nul;
        kvl_t kvl(s, key, nul);
        EXPECT_EQ (kvl.stid, st);
        EXPECT_EQ (kvl.h, key_hash);
        EXPECT_EQ (kvl.g, (uint32_t) 0);
    }

    {
        // both = null
        vec_t nul;
        kvl_t kvl(s, nul, nul);
        EXPECT_EQ (kvl.stid, st);
        EXPECT_EQ (kvl.h, (uint32_t) 0);
        EXPECT_EQ (kvl.g, (uint32_t) 0);
    }
}

TEST (KvlTest, Test1) {
    test_kvl (3, 2, "abc", NULL);
}

TEST (KvlTest, Test2) {
    test_kvl (3, 2, "abc", "xxxx");
}

TEST (KvlTest, Test3) {
    test_kvl (1, 8, "Paradise", "Paradise"); // where is it...
}
