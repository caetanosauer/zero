#include "gtest/gtest.h"
#include "w_endian.h"
#include <stdint.h>
#include <iostream>
#include <vector>

/** Tests a few obvious things about endianness. */

TEST(EndianTest, OutputEndian) {
    if (is_big_endian()) {
        std::cout << "It's BigEndian!" << std::endl;
    } else {
        EXPECT_TRUE (is_little_endian());
        std::cout << "It's LittleEndian!" << std::endl;
    }
}


TEST(EndianTest, Int16Convert) {
    std::vector<uint16_t> values;
    values.push_back(0);
    values.push_back(1);
    values.push_back(50);
    values.push_back(128);
    values.push_back(130);
    values.push_back(256);
    values.push_back(320);
    values.push_back(5000);
    values.push_back(32767);
    values.push_back(32768);
    values.push_back(32769);
    values.push_back(50000);
    values.push_back(65535);
    unsigned char buf[8];
    unsigned char prev[8];
    for (size_t i = 0; i < values.size(); ++i) {
        uint16_t v = values[i];
        serialize16_be(buf, v);
        uint16_t v2 = deserialize16_ho(buf);
        EXPECT_EQ(v, v2);
        // after serialize_be, memcmp should work as a comparison
        if (i > 0) {
            EXPECT_GT(0, ::memcmp (prev, buf, sizeof(v))) << "failed (" << v << ", prev= " << values[i - 1] << ")";
        }
        ::memcpy (prev, buf, sizeof(v));
    }
}

TEST(EndianTest, Int32Convert) {
    std::vector<uint32_t> values;
    values.push_back(0);
    values.push_back(1);
    values.push_back(50);
    values.push_back(128);
    values.push_back(130);
    values.push_back(256);
    values.push_back(320);
    values.push_back(5000);
    values.push_back(32767);
    values.push_back(32768);
    values.push_back(32769);
    values.push_back(50000);
    values.push_back(65535);
    values.push_back(65536);
    values.push_back(65537);
    values.push_back(5432132);
    values.push_back(0x7FFFFFFF);
    unsigned char buf[8];
    unsigned char prev[8];
    for (size_t i = 0; i < values.size(); ++i) {
        uint32_t v = values[i];
        serialize32_be(buf, v);
        uint32_t v2 = deserialize32_ho(buf);
        EXPECT_EQ(v, v2);
        // after serialize_be, memcmp should work as a comparison
        if (i > 0) {
            EXPECT_GT(0, ::memcmp (prev, buf, sizeof(v))) << "failed (" << v << ", prev= " << values[i - 1] << ")";
        }
        ::memcpy (prev, buf, sizeof(v));
    }
}
