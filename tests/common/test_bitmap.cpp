#include "bitmap.h"
#include "gtest/gtest.h"

const int    numBits = 71;
const int    numBytes = (numBits - 1) / 8 + 1;
const int    iterations = 10000;

/**
 * Unit test for bitmap class.
 */
class BitmapTest : public ::testing::Test {
    protected:
    BitmapTest() {
    }

    virtual ~BitmapTest() {
    }

    virtual void SetUp() {
        map = new u_char[numBytes];
        ASSERT_TRUE (map != NULL);
        ::srand (123); // fixed seed for repeatability
        for (int i = 0; i < numBytes; i++)
            map[i] = rand();
    }

    virtual void TearDown() {
        delete[] map;
        map = NULL;
    }

    u_char    *map;
};

TEST_F(BitmapTest, ClearMap) {
    bm_zero(map, numBits);
    for (int i = 0; i < numBits; i++)  {
        EXPECT_FALSE(bm_is_set(map, i))  <<  "Clear Map error in " << i << " ";
    }
}
TEST_F(BitmapTest, SetMap) {
    bm_zero(map, numBits);
    bm_fill(map, numBits);
    for (int i = 0; i < numBits; i++)  {
        EXPECT_TRUE(bm_is_set(map, i))  <<  "Set Map error in " << i << " ";
    }
}
TEST_F(BitmapTest, Set) {    
    bm_zero(map, numBits);
    for (int i = 0; i < iterations; i++)  {
        int    bit = i % numBits;
        bm_set(map, bit);
        EXPECT_TRUE(bm_is_set(map, bit))  <<  "Set error in " << bit << " ";
    }
}
TEST_F(BitmapTest, Clear) {    
    bm_fill(map, numBits);
    for (int i = 0; i < iterations; i++)  {
        int    bit = i % numBits;
        bm_clr(map, bit);
        EXPECT_FALSE(bm_is_set(map, bit))  <<  "Clear error in " << bit << " ";
    }
}
TEST_F(BitmapTest, FirstSet) {    
    bm_zero(map, numBits);
    for (int i = 0; i < iterations; i++)  {
        int    bit = i % numBits;
        bm_set(map, bit);
        EXPECT_EQ(bm_first_set(map, numBits, 0), bit)  <<  "First Set (zero) error in " << bit << " ";        
        EXPECT_EQ(bm_first_set(map, numBits, bit), bit)  <<  "First Set (bit) error in " << bit << " ";
        bm_clr(map, bit);
    }
}
TEST_F(BitmapTest, FirstClear) {    
    bm_fill(map, numBits);
    for (int i = 0; i < iterations; i++)  {
        int    bit = i % numBits;
        bm_clr(map, bit);
        EXPECT_EQ(bm_first_clr(map, numBits, 0), bit)  <<  "First Clear (zero) error in " << bit << " ";        
        EXPECT_EQ(bm_first_clr(map, numBits, bit), bit)  <<  "First Clear (bit) error in " << bit << " ";        
        bm_set(map, bit);
    }
}
TEST_F(BitmapTest, LastSet) {
    bm_zero(map, numBits);
    for (int i = 0; i < iterations; i++)  {
        int    bit = i % numBits;
        bm_set(map, bit);
        EXPECT_EQ(bm_last_set(map, numBits, numBits-1), bit)  <<  "Last Set (numBits-1) error in " << bit << " ";        
        EXPECT_EQ(bm_last_set(map, numBits, bit), bit)  <<  "Last Set (bit) error in " << bit << " ";                
        bm_clr(map, bit);
    }
}
TEST_F(BitmapTest, LastClear) {
    bm_fill(map, numBits);
    for (int i = 0; i < iterations; i++)  {
        int    bit = i % numBits;
        bm_clr(map, bit);
        EXPECT_EQ(bm_last_clr(map, numBits, numBits-1), bit)  <<  "Last Clear (numBits-1) error in " << bit << " ";        
        EXPECT_EQ(bm_last_clr(map, numBits, bit), bit)  <<  "Last Clear (bit) error in " << bit << " ";        
        bm_set(map, bit);
    }
}
TEST_F(BitmapTest, NumSet) {
    bm_zero(map, numBits);
    EXPECT_EQ(bm_num_set(map, numBits), 0)  <<  "Num Set (all) error";        
    for (int i = 0; i < numBits; i++)  {
        bm_set(map, i);
        EXPECT_EQ(bm_num_set(map, numBits), i+1)  <<  "Num Set error in " << i << " ";        
    }
}
TEST_F(BitmapTest, NumClear) {
    bm_fill(map, numBits);
    EXPECT_EQ(bm_num_clr(map, numBits), 0)  <<  "Num Clear (all) error";        
    for (int i = 0; i < numBits; i++)  {
        bm_clr(map, i);
        EXPECT_EQ(bm_num_clr(map, numBits), i+1)  <<  "Num Clear error in " << i << " ";        
    }
}
