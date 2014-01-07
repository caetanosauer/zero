#include "gtest/gtest.h"

#include "w_okvl.h"
#include "w_okvl_inl.h"
#include <cstdlib>

TEST(OkvlWhiteboxTest, ConstantCompatibility) {
    EXPECT_TRUE (ALL_N_GAP_N.is_compatible_grant(ALL_X_GAP_S));
    EXPECT_TRUE (ALL_X_GAP_S.is_compatible_grant(ALL_N_GAP_N));
    EXPECT_TRUE (ALL_N_GAP_N.is_compatible_request(ALL_X_GAP_S));
    EXPECT_TRUE (ALL_X_GAP_S.is_compatible_request(ALL_N_GAP_N));

    EXPECT_TRUE (ALL_S_GAP_N.is_compatible_grant(ALL_S_GAP_S));
    EXPECT_TRUE (ALL_S_GAP_S.is_compatible_grant(ALL_S_GAP_N));
    EXPECT_TRUE (ALL_S_GAP_N.is_compatible_request(ALL_S_GAP_S));
    EXPECT_TRUE (ALL_S_GAP_S.is_compatible_request(ALL_S_GAP_N));

    EXPECT_FALSE (ALL_S_GAP_S.is_compatible_grant(ALL_X_GAP_S));
    EXPECT_FALSE (ALL_X_GAP_S.is_compatible_grant(ALL_S_GAP_S));
    EXPECT_FALSE (ALL_S_GAP_S.is_compatible_request(ALL_X_GAP_S));
    EXPECT_FALSE (ALL_X_GAP_S.is_compatible_request(ALL_S_GAP_S));

    EXPECT_FALSE (ALL_X_GAP_N.is_compatible_grant(ALL_S_GAP_S));
    EXPECT_FALSE (ALL_S_GAP_S.is_compatible_grant(ALL_X_GAP_N));
    EXPECT_FALSE (ALL_X_GAP_N.is_compatible_request(ALL_S_GAP_S));
    EXPECT_FALSE (ALL_S_GAP_S.is_compatible_request(ALL_X_GAP_N));

    EXPECT_TRUE (ALL_N_GAP_N.is_empty());
    EXPECT_TRUE (ALL_N_GAP_N.is_keylock_empty());

    EXPECT_FALSE (ALL_N_GAP_S.is_empty());
    EXPECT_TRUE (ALL_N_GAP_S.is_keylock_empty());

    EXPECT_FALSE (ALL_X_GAP_N.is_empty());
    EXPECT_FALSE (ALL_X_GAP_N.is_keylock_empty());

    EXPECT_FALSE (ALL_X_GAP_S.is_empty());
    EXPECT_FALSE (ALL_X_GAP_S.is_keylock_empty());
    EXPECT_EQ (okvl_mode::X, ALL_X_GAP_S.get_key_mode());
    EXPECT_EQ (okvl_mode::S, ALL_X_GAP_S.get_gap_mode());
    for (okvl_mode::part_id part = 0; part < OKVL_PARTITIONS; ++part) {
        EXPECT_EQ (okvl_mode::N, ALL_X_GAP_S.get_partition_mode(part));
    }
}


TEST(OkvlWhiteboxTest, Partition) {
    if (OKVL_PARTITIONS < 2) {
        std::cout << "WARNING: this test case makes sense only when OKVL_PARTITIONS>=2" << std::endl;
        return;
    }

    okvl_mode left, right;
    EXPECT_TRUE (left.is_empty());
    EXPECT_TRUE (right.is_empty());

    left.set_partition_mode(0, okvl_mode::S);
    EXPECT_EQ (okvl_mode::IS, left.get_key_mode());
    EXPECT_FALSE (left.is_empty());

    right.set_partition_mode(1, okvl_mode::X);
    EXPECT_EQ (okvl_mode::IX, right.get_key_mode());
    EXPECT_FALSE (right.is_empty());

    EXPECT_TRUE (left.is_compatible_grant(right));
    EXPECT_TRUE (right.is_compatible_grant(left));
    EXPECT_TRUE (left.is_compatible_request(right));
    EXPECT_TRUE (right.is_compatible_request(left));
    EXPECT_TRUE (okvl_mode::is_compatible(left, right));
    EXPECT_TRUE (okvl_mode::is_compatible(right, left));

    left.set_partition_mode(1, okvl_mode::S);
    EXPECT_EQ (okvl_mode::IS, left.get_key_mode());

    EXPECT_FALSE (left.is_compatible_grant(right));
    EXPECT_FALSE (right.is_compatible_grant(left));
    EXPECT_FALSE (left.is_compatible_request(right));
    EXPECT_FALSE (right.is_compatible_request(left));
    EXPECT_FALSE (okvl_mode::is_compatible(left, right));
    EXPECT_FALSE (okvl_mode::is_compatible(right, left));
}

TEST(OkvlWhiteboxTest, Gap) {
    okvl_mode left, right;
    EXPECT_TRUE (left.is_empty());
    EXPECT_TRUE (right.is_empty());

    left.set_gap_mode(okvl_mode::S);
    EXPECT_EQ (okvl_mode::N, left.get_key_mode());
    EXPECT_EQ (okvl_mode::S, left.get_gap_mode());
    EXPECT_FALSE (left.is_empty());
    EXPECT_TRUE (left.is_keylock_empty());

    right.set_gap_mode(okvl_mode::X);
    EXPECT_EQ (okvl_mode::N, right.get_key_mode());
    EXPECT_EQ (okvl_mode::X, right.get_gap_mode());
    EXPECT_FALSE (right.is_empty());
    EXPECT_TRUE (right.is_keylock_empty());

    EXPECT_FALSE (left.is_compatible_grant(right));
    EXPECT_FALSE (right.is_compatible_grant(left));
    EXPECT_FALSE (left.is_compatible_request(right));
    EXPECT_FALSE (right.is_compatible_request(left));
    EXPECT_FALSE (okvl_mode::is_compatible(left, right));
    EXPECT_FALSE (okvl_mode::is_compatible(right, left));
}

TEST(OkvlWhiteboxTest, KeyVsGap) {
    okvl_mode partition, key, gap;

    partition.set_partition_mode(0, okvl_mode::S);
    key.set_key_mode(okvl_mode::S);
    gap.set_gap_mode(okvl_mode::X);

    EXPECT_TRUE (okvl_mode::is_compatible(partition, gap));
    EXPECT_TRUE (okvl_mode::is_compatible(key, gap));
}

TEST(OkvlWhiteboxTest, Equality) {
    okvl_mode left, right;
    EXPECT_TRUE (left == right);
    EXPECT_FALSE (left != right);
    EXPECT_TRUE (left.is_implied_by(right));
    EXPECT_TRUE (right.is_implied_by(left));

    if (OKVL_PARTITIONS < 2) {
        std::cout << "WARNING: this test case makes sense only when OKVL_PARTITIONS>=2" << std::endl;
        return;
    }

    left.set_partition_mode(0, okvl_mode::S);
    EXPECT_FALSE (left == right);
    EXPECT_TRUE (left != right);
    EXPECT_FALSE (left.is_implied_by(right));
    EXPECT_TRUE (right.is_implied_by(left));

    right.set_key_mode(okvl_mode::IS);
    EXPECT_FALSE (left.is_implied_by(right));
    EXPECT_TRUE (right.is_implied_by(left));

    left.clear();
    right.clear();
    EXPECT_TRUE (left == right);
    EXPECT_FALSE (left != right);
    EXPECT_TRUE (left == left);
    EXPECT_TRUE (left.is_implied_by(right));
    EXPECT_TRUE (right.is_implied_by(left));

    left.set_partition_mode(1, okvl_mode::S);
    right.set_partition_mode(1, okvl_mode::S);
    EXPECT_TRUE (left == right);
    EXPECT_FALSE (left != right);
    EXPECT_TRUE (left.is_implied_by(right));
    EXPECT_TRUE (right.is_implied_by(left));

    left.clear();
    right.clear();
    EXPECT_TRUE (left == right);
    EXPECT_FALSE (left != right);
    EXPECT_TRUE (left == left);

    left.set_partition_mode(1, okvl_mode::S);
    right.set_partition_mode(1, okvl_mode::X);
    EXPECT_FALSE (left == right);
    EXPECT_TRUE (left != right);
    EXPECT_TRUE (left == left);
    EXPECT_TRUE (left.is_implied_by(right));
    EXPECT_FALSE (right.is_implied_by(left));

    left.clear();
    right.clear();
    EXPECT_TRUE (left == right);
    EXPECT_FALSE (left != right);
    EXPECT_TRUE (left == left);

    left.set_partition_mode(0, okvl_mode::S);
    right.set_partition_mode(1, okvl_mode::S);
    EXPECT_FALSE (left == right);
    EXPECT_TRUE (left != right);
    EXPECT_TRUE (left == left);
    EXPECT_FALSE (left.is_implied_by(right));
    EXPECT_FALSE (right.is_implied_by(left));

    left.clear();
    right.clear();
    left.set_partition_mode(1, okvl_mode::S);
    right.set_partition_mode(1, okvl_mode::S);
    left.set_gap_mode(okvl_mode::S);
    EXPECT_FALSE (left == right);
    EXPECT_TRUE (left != right);
    EXPECT_TRUE (left == left);
    EXPECT_FALSE (left.is_implied_by(right));
    EXPECT_TRUE (right.is_implied_by(left));

    left.clear();
    right.clear();
    left.set_key_mode(okvl_mode::S);
    right.set_key_mode(okvl_mode::S);
    EXPECT_TRUE (left == right);
    EXPECT_FALSE (left != right);
    EXPECT_TRUE (left == left);
    EXPECT_TRUE (left.is_implied_by(right));
    EXPECT_TRUE (right.is_implied_by(left));

    right.set_key_mode(okvl_mode::SIX);
    EXPECT_FALSE (left == right);
    EXPECT_TRUE (left != right);
    EXPECT_TRUE (left == left);
    EXPECT_TRUE (left.is_implied_by(right));
    EXPECT_FALSE (right.is_implied_by(left));
}

TEST(OkvlWhiteboxTest, HashRandomness) {
    // randomly invoke compute_part_id and see if it's reasonably good.
    const int TRIALS = 1 << 16;
    uint32_t counts[OKVL_PARTITIONS];
    for (uint32_t i = 0; i < OKVL_PARTITIONS; ++i) {
        counts[i] = 0;
    }

    const int BUFFER_SIZE = 16;
    unsigned char buffer[BUFFER_SIZE];
    for (int i = 0; i < TRIALS; ++i) {
        for (int j = 0; j < BUFFER_SIZE; ++j) {
            buffer[j] = std::rand() % 256;
        }

        okvl_mode::part_id part = okvl_mode::compute_part_id(buffer, BUFFER_SIZE);
        EXPECT_GE (part, (uint32_t)0);
        EXPECT_LT (part, OKVL_PARTITIONS);
        ++counts[part];
    }

    // Assuming TRIALS >> OKVL_PARTITIONS, there shouldn't be too much skews.
    for (uint32_t i = 0; i < OKVL_PARTITIONS; ++i) {
        EXPECT_GE (counts[i], (uint32_t)(TRIALS * 8 / 10 / OKVL_PARTITIONS));
        EXPECT_LT (counts[i], (uint32_t)(TRIALS * 12 / 10 / OKVL_PARTITIONS));
    }
}

