#include "w_defines.h"
#include "w_stream.h"
#include <cstddef>
#include "w.h"
#include "gtest/gtest.h"

struct elem1_t {
    int         i;
    w_link_t        link;
};

#ifdef EXPLICIT_TEMPLATE
template class w_list_t<elem1_t, unsafe_list_dummy_lock_t>;
template class w_list_i<elem1_t, unsafe_list_dummy_lock_t>;
#endif

TEST(ListTest, List1) {
    w_list_t<elem1_t, unsafe_list_dummy_lock_t> l(W_LIST_ARG(elem1_t, link),
                        unsafe_nolock);

    elem1_t array[10];
    
    int i;
    for (i = 0; i < 10; i++)  {
        array[i].i = i;
        l.push(&array[i]);
    }

    EXPECT_EQ (l.num_members(), (uint) 10);

    for (i = 0; i < 10; i++)  {
        elem1_t* p = l.pop();
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(p->i, 9 - i);
    }

    EXPECT_TRUE(l.pop() == NULL);

    for (i = 0; i < 10; i++)  {
        l.append(&array[i]);
    }

    for (i = 0; i < 10; i++)  {
        elem1_t* p = l.chop();
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(p->i, 9 - i);
    }
    EXPECT_TRUE(l.chop() == NULL);
}


struct elem2_t {
    int         i;
    w_link_t        link;
};

#ifdef EXPLICIT_TEMPLATE
template class w_list_t<elem2_t, unsafe_list_dummy_lock_t>;
template class w_list_i<elem2_t, unsafe_list_dummy_lock_t>;
template class w_list_const_i<elem2_t, unsafe_list_dummy_lock_t>;
#endif

TEST(ListTest, List2) {
    w_list_t<elem2_t, unsafe_list_dummy_lock_t> l(W_LIST_ARG(elem2_t, link),
                        unsafe_nolock);

    elem2_t array[10];

    int i;
    for (i = 0; i < 10; i++)  {
        array[i].i = i;
        l.push(&array[i]);
    }

    {
        w_list_i<elem2_t, unsafe_list_dummy_lock_t> iter(l);
        for (i = 0; i < 10; i++)  {
            elem2_t* p = iter.next();
            ASSERT_TRUE(p != NULL);
            EXPECT_EQ(p->i, 9 - i);
        }
        EXPECT_TRUE(iter.next() == NULL);

        w_list_const_i<elem2_t, unsafe_list_dummy_lock_t> const_iter(l);
        for (i = 0; i < 10; i++)  {
            const elem2_t* p = const_iter.next();
            ASSERT_TRUE(p != NULL);
            EXPECT_EQ(p->i, 9 - i);
        }
        EXPECT_TRUE(const_iter.next() == NULL);
    }

    for (i = 0; i < 10; i++)  {
        elem2_t* p = l.pop();
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(p->i, 9 - i);
    }

    EXPECT_TRUE(l.pop() == NULL);

    for (i = 0; i < 10; i++)  {
        l.append(&array[i]);
    }

    {
        w_list_i<elem2_t, unsafe_list_dummy_lock_t> iter(l);
        for (i = 0; i < 10; i++)  {
            elem2_t* p = iter.next();
            ASSERT_TRUE(p != NULL);
            EXPECT_EQ(p->i, i);
        }
    }

    for (i = 0; i < 10; i++)  {
        elem2_t* p = l.chop();
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(p->i, 9 - i);
    }
    EXPECT_TRUE(l.chop() == NULL);
}

struct elem3_t {
    int         i;
    w_link_t        link;
};

typedef w_ascend_list_t<elem3_t, unsafe_list_dummy_lock_t, int>   elem_ascend_list_t;
typedef w_descend_list_t<elem3_t, unsafe_list_dummy_lock_t, int>  elem_descend_list_t;

#ifdef EXPLICIT_TEMPLATE
template class w_ascend_list_t<elem3_t, unsafe_list_dummy_lock_t, int>;
template class w_descend_list_t<elem3_t, unsafe_list_dummy_lock_t, int>;
template class w_keyed_list_t<elem3_t, unsafe_list_dummy_lock_t, int>;
template class w_list_t<elem3_t, unsafe_list_dummy_lock_t>;
template class w_list_i<elem3_t, unsafe_list_dummy_lock_t>;
#endif

TEST(ListTest, List3) {
    elem3_t array[10];
    elem3_t* p;

    int i;
    for (i = 0; i < 10; i++)
        array[i].i = i;

    {
        elem_ascend_list_t u(W_KEYED_ARG(elem3_t, i, link), unsafe_nolock);

        for (i = 0; i < 10; i += 2)   {
            u.put_in_order(&array[9 - i]);        // insert 9, 7, 5, 3, 1
        }

        for (i = 0; i < 10; i += 2)  {
            u.put_in_order(&array[i]);        // insert 0, 2, 4, 6, 8
        }

        for (i = 0; i < 10; i++)  {
            p = u.search(i);
            ASSERT_TRUE(p != NULL);
            EXPECT_EQ(p->i, i);
        }

        {
            w_list_i<elem3_t, unsafe_list_dummy_lock_t> iter(u);
            for (i = 0; i < 10; i++)  {
                p = iter.next();
                ASSERT_TRUE(p != NULL);
                EXPECT_EQ(p->i, i);
            }
            EXPECT_TRUE(iter.next() == NULL);
        }

        p = u.first();
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(p->i, 0);

        p = u.last();
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(p->i, 9);

        for (i = 0; i < 10; i++)  {
            p = u.first();
            ASSERT_TRUE(p != NULL);
            EXPECT_EQ(p->i, i);
            p = u.pop();
            ASSERT_TRUE(p != NULL);
            EXPECT_EQ(p->i, i);
        }

        p = u.pop();
        EXPECT_TRUE(p == NULL);
    }

    {
        elem_descend_list_t d(W_KEYED_ARG(elem3_t, i, link), unsafe_nolock);

        for (i = 0; i < 10; i += 2)  {
            d.put_in_order(&array[9 - i]);        // insert 9, 7, 5, 3, 1
        }
    
        for (i = 0; i < 10; i += 2)  {
            d.put_in_order(&array[i]);        // insert 0, 2, 4, 6, 8
        }

        for (i = 0; i < 10; i++)  {
            p = d.search(i);
            EXPECT_TRUE(p == &array[i]);
        }

        {
            w_list_i<elem3_t, unsafe_list_dummy_lock_t> iter(d);
            for (i = 0; i < 10; i++)  {
                p = iter.next();
                ASSERT_TRUE(p != NULL);
                EXPECT_EQ(p->i, 9 - i);
            }
            EXPECT_TRUE(iter.next() == NULL);
        }

        p = d.first();
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(p->i, 9);

        p = d.last();
        ASSERT_TRUE(p != NULL);
        EXPECT_EQ(p->i, 0);

        for (i = 0; i < 10; i++)  {
            p = d.first();
            ASSERT_TRUE(p != NULL);
            EXPECT_EQ(p->i, 9 - i);
            p = d.pop();
            ASSERT_TRUE(p != NULL);
            EXPECT_EQ(p->i, 9 - i);
        }

        p = d.pop();
        EXPECT_TRUE(p == NULL);
    }
}
