#include "w_defines.h"

#include "w_stream.h"
#include <cstddef>
#include "w.h"
#include "w_bitvector.h"
#include "gtest/gtest.h"


TEST(BitvectorTest, All) {
    w_bitvector_t<256> v, w, tmp;

    EXPECT_TRUE(v.is_empty());
    EXPECT_TRUE(w.is_empty());

    // Word 0 : 0->63
    v.set_bit(0);
    EXPECT_TRUE(v.is_set(0));
    EXPECT_FALSE(v.is_empty());
    EXPECT_EQ(v.num_bits_set(), 1);

    v.set_bit(1);
    EXPECT_TRUE(v.is_set(1));
    EXPECT_EQ(v.num_bits_set(), 2);

    v.set_bit(35);
    EXPECT_TRUE(v.is_set(35));
    EXPECT_EQ(v.num_bits_set(), 3);
    v.clear_bit(35);
    EXPECT_EQ(v.is_set(35), false);
    EXPECT_EQ(v.num_bits_set(), 2);
    v.set_bit(35);
    EXPECT_TRUE(v.is_set(35));
    EXPECT_EQ(v.num_bits_set(), 3);

    // Word 1 : 64->127
    v.set_bit(72);
    EXPECT_TRUE(v.is_set(72));
    EXPECT_EQ(v.num_bits_set(), 4);

    // Word 2 : 128 -> 191
    v.set_bit(172);
    EXPECT_TRUE(v.is_set(172));
    EXPECT_EQ(v.num_bits_set(), 5);

    // Word 3 : 192 -> 255
    v.set_bit(200);
    EXPECT_TRUE(v.is_set(200));
    EXPECT_EQ(v.num_bits_set(), 6);

    v.set_bit(255);
    EXPECT_TRUE(v.is_set(255));
    EXPECT_EQ(v.num_bits_set(), 7);

    EXPECT_EQ(v.is_empty(), false);

    int i;
    for (i = 0; i < v.num_bits(); i++)  {
            w.set_bit(i);
            EXPECT_EQ(w.num_bits_set(), i+1);
    }
    EXPECT_EQ(w.is_empty(), false);
    EXPECT_EQ(w.is_full(), true);
#if defined(ARCH_LP64)
    EXPECT_EQ(w.num_words(), 4);
#else
    EXPECT_EQ(w.num_words(), 8);
#endif

    w.copy(v);
    EXPECT_EQ(w.num_bits_set(), 7);
    EXPECT_EQ(w.is_empty(), false);
    EXPECT_EQ(w.is_full(), false);
    for (i = 0; i < v.num_words(); i++)  {
        EXPECT_EQ(w.get_bit(i), v.get_bit(i));
    }

    w.clear();
    EXPECT_EQ(w.is_empty(), true);
    EXPECT_EQ(w.is_full(), false);
    for (i = 0; i < v.num_words(); i++)  {
        EXPECT_EQ(w.get_bit(i), (w_bitvector_t<256>::Word) 0);
    }

    w.set_bit(0);
    int n=w.words_overlap(tmp, v);
#if defined(ARCH_LP64)
    EXPECT_EQ(n, 4); // bit 0 is in both
#else
    EXPECT_EQ(n, 8); // bit 0 is in both
#endif

    w.set_bit(5);
    n=w.words_overlap(tmp, v);

#if defined(ARCH_LP64)
    EXPECT_EQ(n, 3); // bit 5 is not in both
#else
    EXPECT_EQ(n, 7); // bit 5 is not in both
#endif
    w.clear_bit(5);
    n=w.words_overlap(tmp, v);

#if defined(ARCH_LP64)
    EXPECT_EQ(n, 4); // back to former state
#else
    EXPECT_EQ(n, 8); // back to former state
#endif

    w.set_bit(200);
    n=w.words_overlap(tmp, v);
#if defined(ARCH_LP64)
    EXPECT_EQ(n, 4); 
#else
    EXPECT_EQ(n, 8); 
#endif

    w.set_bit(255);
    n=w.words_overlap(tmp, v);
#if defined(ARCH_LP64)
    EXPECT_EQ(n, 4); 
#else
    EXPECT_EQ(n, 8); 
#endif

    w.set_bit(72);
    n=w.words_overlap(tmp, v);
#if defined(ARCH_LP64)
    EXPECT_EQ(n, 4); 
#else
    EXPECT_EQ(n, 8); 
#endif

    // Is all of w found in v?
    EXPECT_EQ(w.overlap(tmp,v) ,  true);
    // Is all of v found in w?
    EXPECT_EQ(v.overlap(tmp,w) ,  false);

    w.set_bit(172);
    EXPECT_EQ(w.overlap(tmp,v) ,  true);
    EXPECT_EQ(v.overlap(tmp,w) ,  false);
    w.set_bit(35);
    EXPECT_EQ(w.overlap(tmp,v) ,  true);
    EXPECT_EQ(v.overlap(tmp,w) ,  false);

    w.set_bit(1);
    n=w.words_overlap(tmp, v);
    EXPECT_EQ(w.overlap(tmp,v) ,  true);
    EXPECT_EQ(v.overlap(tmp,w) ,  true);
}

