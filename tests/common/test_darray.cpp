#include "shore-config.h"
#include "w_base.h"
#include "dynarray.h"
#include <errno.h>
#include <sys/mman.h>
#include <algorithm>
#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio>

// no system I know of *requires* larger pages than this
// DEAD static size_t const MM_PAGE_SIZE = 8192;
// most systems can't handle bigger than this, and we need a sanity check
// DEAD static size_t const MM_MAX_CAPACITY = MM_PAGE_SIZE*1024*1024*1024;

#include <unistd.h>
#include <cstdio>
#include "gtest/gtest.h"

static int foocount = 0;
struct foo {
    int id;
    foo() : id(++foocount) { cout << "foo#" << id << endl; }
    ~foo() { cout << "~foo#" << id << endl; }
};

template struct dynvector<foo>;

TEST (DarrayTest, All) {
    {
        dynarray mm;
        int64_t err;

#ifdef ARCH_LP64
#define BIGNUMBER 1024*1024*1024
#else
#define BIGNUMBER 1024*1024
#endif
        err = mm.init(size_t(5l*BIGNUMBER));
        // char const* base = mm;
        // std::fprintf(stdout, "&mm[0] = %p\n", base);
        err = mm.resize(10000);
        err = mm.resize(100000);
        err = mm.resize(1000000);
        err = mm.fini();
        EXPECT_EQ(err, 0);
    }
    {
        // test alignment
        dynarray mm;
        int64_t err =
        mm.init(size_t(5l*BIGNUMBER), size_t(BIGNUMBER));
        EXPECT_EQ(err, 0);
    }

    {
        int err;
        dynvector<foo> dv;
        err = dv.init(100000);
        cout << "size:" << dv.size() << "  capacity:" << dv.capacity() << "  limit:" << dv.limit() << endl; 
        err = dv.resize(4);
        cout << "size:" << dv.size() << "  capacity:" << dv.capacity() << "  limit:" << dv.limit() << endl; 
        err = dv.resize(10);
        cout << "size:" << dv.size() << "  capacity:" << dv.capacity() << "  limit:" << dv.limit() << endl; 
        foo f;
        err = dv.push_back(f);
        err = dv.push_back(f);
        err = dv.push_back(f);
        cout << "size:" << dv.size() << "  capacity:" << dv.capacity() << "  limit:" << dv.limit() << endl; 
        err = dv.resize(16);
        cout << "size:" << dv.size() << "  capacity:" << dv.capacity() << "  limit:" << dv.limit() << endl; 
        err = dv.fini();
        EXPECT_EQ(err, 0);
    }
}
