#include "w_key.h"
#include "gtest/gtest.h"

#include <string>

/**
 * The class to test w_keystr_t.
 * We don't need any gtest fixture, but this class is defined
 * to utilize friendship with w_keystr_t.
 */
class w_keystr_t_test : public ::testing::Test {
protected:
// In Soviet C++, Friends can see your Private!
    unsigned char *get_internal_data (const w_keystr_t &str) const {
        return str._data;
    }
    size_t get_internal_len (const w_keystr_t &str) const {
        return str._strlen;
    }
    size_t get_memory_len (const w_keystr_t &str) const {
        return str._memlen;
    }
    bool check_len (const w_keystr_t &str) const {
        return str._strlen <= str._memlen;
    }

    bool serialize_then_deserialize (const w_keystr_t &original, w_keystr_t &deserialized);
};

TEST_F (w_keystr_t_test, Basic) {
    
    w_keystr_t str1;
    EXPECT_FALSE (str1.is_constructed());
    EXPECT_TRUE(check_len(str1));

    w_keystr_t str2;
    str2.construct_regularkey("testaaa", 7);
    ASSERT_TRUE (str2.is_constructed());
    EXPECT_EQ (get_internal_len(str2), (uint) 7 + 1);
    EXPECT_EQ (str2.get_length_as_keystr(), (uint) 7 + 1);
    EXPECT_EQ (str2.get_length_as_nonkeystr(), (uint) 7);
    EXPECT_EQ (str2.serialize_as_nonkeystr(), std::basic_string<unsigned char>((const unsigned char*)"testaaa"));
    EXPECT_EQ (((const char*)get_internal_data(str2))[0], SIGN_REGULAR);
    EXPECT_TRUE (get_internal_data(str2) != NULL);
    EXPECT_TRUE(check_len(str2));
    std::string in_str (reinterpret_cast<const char*>(get_internal_data(str2) + 1), (size_t) 7);
    EXPECT_TRUE (in_str == "testaaa");
}

TEST_F (w_keystr_t_test, CompareRegular) {
    w_keystr_t str1;
    ASSERT_TRUE (str1.construct_regularkey("testabc", 7));
    ASSERT_TRUE (str1.is_constructed());
    EXPECT_TRUE(check_len(str1));

    w_keystr_t str2;
    ASSERT_TRUE (str2.construct_regularkey("testabd", 7));
    ASSERT_TRUE (str2.is_constructed());
    EXPECT_TRUE(check_len(str2));
    
    EXPECT_LT (str1.compare(str2), 0);
    EXPECT_GT (str2.compare(str1), 0);
    
    ASSERT_TRUE (str1.construct_regularkey("testabe", 7));
    EXPECT_GT (str1.compare(str2), 0);
    EXPECT_LT (str2.compare(str1), 0);
    
    ASSERT_TRUE (str1.construct_regularkey("testa", 5));
    EXPECT_LT (str1.compare(str2), 0);
    EXPECT_GT (str2.compare(str1), 0);    
    
    std::string tmp ("test");
    tmp += "abd";
    
    ASSERT_TRUE (str1.construct_regularkey(tmp.data(), tmp.size()));
    EXPECT_EQ (str1.compare(str2), 0);
    EXPECT_EQ (str2.compare(str1), 0);
}

TEST_F (w_keystr_t_test, CompareInfinity) {
    w_keystr_t neginf;
    ASSERT_TRUE (neginf.construct_neginfkey());
    EXPECT_EQ (neginf.get_length_as_keystr(), (uint) 1);
    EXPECT_EQ (neginf.get_length_as_nonkeystr(), (uint) 0);
    EXPECT_TRUE (neginf.is_neginf());
    EXPECT_FALSE (neginf.is_posinf());
    EXPECT_FALSE (neginf.is_regular());
    EXPECT_EQ (get_internal_len(neginf), (uint) 1);
    EXPECT_EQ (((const char*)get_internal_data(neginf))[0], SIGN_NEGINF);
    EXPECT_TRUE(check_len(neginf));

    w_keystr_t posinf;
    ASSERT_TRUE (posinf.construct_posinfkey());
    EXPECT_EQ (posinf.get_length_as_keystr(), (uint) 1);
    EXPECT_EQ (posinf.get_length_as_nonkeystr(), (uint) 0);
    EXPECT_FALSE (posinf.is_neginf());
    EXPECT_TRUE (posinf.is_posinf());
    EXPECT_FALSE (posinf.is_regular());
    EXPECT_EQ (get_internal_len(posinf), (uint) 1);
    EXPECT_EQ (((const char*)get_internal_data(posinf))[0], SIGN_POSINF);
    EXPECT_TRUE(check_len(posinf));
    
    w_keystr_t str1;
    ASSERT_TRUE (str1.construct_regularkey("testabc", 7));
    EXPECT_FALSE (str1.is_neginf());
    EXPECT_FALSE (str1.is_posinf());
    EXPECT_TRUE (str1.is_regular());
    EXPECT_GT (str1.compare(neginf), 0);
    EXPECT_LT (neginf.compare(str1), 0);
    EXPECT_LT (str1.compare(posinf), 0);
    EXPECT_GT (posinf.compare(str1), 0);
    EXPECT_TRUE(check_len(str1));

    ASSERT_TRUE (str1.construct_regularkey("", 0));
    EXPECT_GT (str1.compare(neginf), 0);
    EXPECT_LT (neginf.compare(str1), 0);
    EXPECT_LT (str1.compare(posinf), 0);
    EXPECT_GT (posinf.compare(str1), 0);
    EXPECT_TRUE(check_len(str1));

    str1 = neginf;
    ASSERT_TRUE (str1.is_constructed());
    EXPECT_TRUE (str1.is_neginf());
    EXPECT_EQ (str1.compare(neginf), 0);
    EXPECT_EQ (neginf.compare(str1), 0);
    EXPECT_LT (str1.compare(posinf), 0);
    EXPECT_GT (posinf.compare(str1), 0);
    EXPECT_TRUE(check_len(str1));

    str1 = posinf;
    ASSERT_TRUE (str1.is_constructed());
    EXPECT_TRUE (str1.is_posinf());
    EXPECT_GT (str1.compare(neginf), 0);
    EXPECT_LT (neginf.compare(str1), 0);
    EXPECT_EQ (str1.compare(posinf), 0);
    EXPECT_EQ (posinf.compare(str1), 0);
    EXPECT_TRUE(check_len(str1));
}

bool w_keystr_t_test::serialize_then_deserialize (const w_keystr_t &original, w_keystr_t &deserialized) {
    char *buffer = new char[original._strlen];
    original.serialize_as_keystr(buffer);
    bool ret = deserialized.construct_from_keystr(buffer, original._strlen);
    delete[] buffer;
    return ret;
}

TEST_F (w_keystr_t_test, Serialize) {
    w_keystr_t neginf, neginf2;
    ASSERT_TRUE (neginf.construct_neginfkey());
    ASSERT_TRUE (serialize_then_deserialize(neginf, neginf2));
    EXPECT_TRUE (neginf.is_neginf());
    EXPECT_TRUE (neginf2.is_neginf());
    EXPECT_TRUE(check_len(neginf));
    EXPECT_TRUE(check_len(neginf2));

    w_keystr_t posinf, posinf2;
    ASSERT_TRUE (posinf.construct_posinfkey());
    ASSERT_TRUE (serialize_then_deserialize(posinf, posinf2));
    EXPECT_TRUE (posinf.is_posinf());
    EXPECT_TRUE (posinf2.is_posinf());
    EXPECT_TRUE(check_len(posinf));
    EXPECT_TRUE(check_len(posinf2));

    w_keystr_t str1, str2;
    ASSERT_TRUE (str1.construct_regularkey("testabc", 7));
    ASSERT_TRUE (serialize_then_deserialize(str1, str2));
    EXPECT_TRUE(check_len(str1));
    EXPECT_TRUE(check_len(str2));
    w_keystr_t str3;
    ASSERT_TRUE (str3.construct_regularkey("testabd", 7));
    EXPECT_TRUE(check_len(str3));

    EXPECT_GT (str1.compare(neginf), 0);
    EXPECT_GT (str1.compare(neginf2), 0);
    EXPECT_EQ (neginf.compare(neginf2), 0);
    
    EXPECT_LT (str1.compare(posinf), 0);
    EXPECT_LT (str1.compare(posinf2), 0);
    EXPECT_EQ (posinf.compare(posinf2), 0);
    EXPECT_GT (posinf2.compare(neginf2), 0);

    EXPECT_EQ (str1.compare(str2), 0);
    EXPECT_LT (str1.compare(str3), 0);
    EXPECT_LT (str2.compare(str3), 0);
}

TEST_F (w_keystr_t_test, Expand) {
    w_keystr_t str;
    ASSERT_TRUE (str.construct_regularkey("testabc", 7));
    EXPECT_EQ (str.serialize_as_nonkeystr(), std::basic_string<unsigned char>((const unsigned char*)"testabc"));
    EXPECT_TRUE(check_len(str));
    EXPECT_EQ((uint) 7 + 1, get_memory_len(str));
    ASSERT_TRUE (str.construct_regularkey("testabcaaaa", 11));
    EXPECT_TRUE(check_len(str));
    EXPECT_EQ((uint) 11 + 1, get_memory_len(str));
    EXPECT_EQ (str.serialize_as_nonkeystr(), std::basic_string<unsigned char>((const unsigned char*)"testabcaaaa"));

    w_keystr_t str2;
    ASSERT_TRUE (str2.construct_regularkey("testg", 5));
    EXPECT_EQ((uint) 5 + 1, get_memory_len(str2));
    EXPECT_TRUE(check_len(str2));

    str = str2;
    EXPECT_TRUE(check_len(str));
    EXPECT_EQ (str.serialize_as_nonkeystr(), std::basic_string<unsigned char>((const unsigned char*)"testg"));
    EXPECT_EQ((uint) 11 + 1, get_memory_len(str));
    EXPECT_TRUE(check_len(str));

    w_keystr_t str3;
    ASSERT_TRUE (str3.construct_regularkey("test0123456789", 14));
    EXPECT_EQ((uint) 14 + 1, get_memory_len(str3));
    EXPECT_TRUE(check_len(str3));

    str = str3;
    EXPECT_EQ((uint) 14 + 1, get_memory_len(str));
    EXPECT_EQ (str.serialize_as_nonkeystr(), std::basic_string<unsigned char>((const unsigned char*)"test0123456789"));
    EXPECT_TRUE(check_len(str));
}
