#include "w_defines.h"

#include "w_stream.h"
#include <cstddef>
#include "w.h"
#include "gtest/gtest.h"

const int htsz = 3;
const int nrecs = 20;

#ifdef EXPLICIT_TEMPLATE
template class w_hash_t<element_t, unsafe_list_dummy_lock_t, int_key_t>;
template class w_hash_i<element_t, unsafe_list_dummy_lock_t, int_key_t>;
template class w_list_t<element_t, unsafe_list_dummy_lock_t>;
template class w_list_i<element_t, unsafe_list_dummy_lock_t>;
#endif

class int_key_t {
        int i;
public:
        int_key_t(int _i) : i(_i) {}
        ~int_key_t() {}

        operator int () const { return i; }

        uint32_t hash() const { return uint32_t(i); }
};

struct element_t {
    int         i;
    w_link_t        link;
};

TEST(HashTest, Hash1) {
    w_hash_t<element_t, unsafe_list_dummy_lock_t, int_key_t> 
                h(htsz, W_HASH_ARG(element_t, i, link), unsafe_nolock);
    element_t array[nrecs];

    int i;
    for (i = 0; i < nrecs; i++)  {
        array[i].i = i;
        h.push(&array[i]);
    }

    for (i = 0; i < nrecs; i++)  {
#if W_DEBUG_LEVEL>0
        element_t* p = h.remove(i);
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(p->i, i);
#else
        (void) h.remove(i);
#endif
    }

    for (i = 0; i < nrecs; i++)  {
        h.append(&array[i]);
    }

    for (i = 0; i < nrecs; i++)  {
        h.remove(&array[i]);
    }
}

TEST(HashTest, Hash2) {
    w_hash_t<element_t, unsafe_list_dummy_lock_t, int_key_t> h(htsz, W_HASH_ARG(element_t, i, link), unsafe_nolock);
    element_t array[nrecs];

    int i;
    for (i = 0; i < nrecs; i++)  {
        array[i].i = i;
        h.push(&array[i]);
    }

    for (i = 0; i < nrecs; i++)  {
#if W_DEBUG_LEVEL>0
        element_t* p = h.lookup(i);
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(p->i, i);
#else
        (void) h.lookup(i);
#endif
    }

    {
        int flag[nrecs];
        for (i = 0; i < nrecs; i++) flag[i] = 0;
        w_hash_i<element_t,unsafe_list_dummy_lock_t,int_key_t> iter(h);

        while (iter.next())  {
            i = iter.curr()->i;
            EXPECT_GE(i, 0);
            EXPECT_LT(i, nrecs);
            ++flag[i];
        }

        for (i = 0; i < nrecs; i++)  {
            EXPECT_EQ(flag[i], 1);
        }
    }

    for (i = 0; i < nrecs; i++)  {
#if W_DEBUG_LEVEL>0
        element_t* p = h.remove(i);
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(p->i, i);
#else
        (void) h.remove(i);
#endif
    }

    for (i = 0; i < nrecs; i++)  {
        h.append(&array[i]);
    }

    for (i = 0; i < nrecs; i++)  {
        h.remove(&array[i]);
    }
}


