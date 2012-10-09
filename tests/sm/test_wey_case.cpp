#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "bf.h"
#include "page_s.h"
#include "btree.h"
#include "btree_p.h"
#include "btree_impl.h"
#include "btcursor.h"
#include "w_key.h"

btree_test_env *test_env;

/**
 * Unit test to check B-tree error Wey found.
 */

w_rc_t wey_testcase(ss_m* ssm, test_volume_t *test_volume) {
    // insert long keys first
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));
    
    W_DO(test_env->begin_xct());
    w_keystr_t key;
    vec_t data;
    const int DATA_BUFF_LEN = 500;
    char cKey[DATA_BUFF_LEN];
    const int iDataLen = 452;
    const uint itotal_records = 100;
    // const uint itotal_records = 1000;
    // const uint itotal_records = 10000;
    // const uint itotal_records = 100000;
    for (uint i = 0; i < itotal_records; ++i) {
        ::memset (cKey, 0, DATA_BUFF_LEN);
        serialize32_be(cKey, i);
        ::memset (cKey + 4, 'A', iDataLen);
        data.set('\0', 0);
        key.construct_regularkey(cKey, 4 + iDataLen);
        W_DO(ssm->create_assoc(stid, key, data));
        
        if (i % 500  == 1) {
            W_DO(ssm->commit_xct());
            cout << "forcing buffer..." << endl;
            W_DO(ss_m::force_buffers());
            W_DO(ssm->begin_xct());
            test_env->set_xct_query_lock();
        }
    }
    W_DO(test_env->commit_xct());

    W_DO(x_btree_verify(ssm, stid));
    
    {
        x_btree_scan_result s;
        W_DO(x_btree_scan(ssm, stid, s, test_env->get_use_locks()));
        EXPECT_EQ (itotal_records, (uint) s.rownum);
    }

    W_DO(test_env->begin_xct());
    bt_cursor_t cursor (stid.vol.vol, stid.store, true);
    for (uint i = 0; i < itotal_records; ++i) {
        W_DO(cursor.next());
        EXPECT_FALSE (cursor.eof()) << i << "th element";
        EXPECT_EQ ((uint) (4 + iDataLen), cursor.key().get_length_as_nonkeystr()) << i << "th element";
        std::basic_string<unsigned char> thekey (cursor.key().serialize_as_nonkeystr());
        uint32_t deserialized = deserialize32_ho(thekey.data());
        EXPECT_EQ (i, deserialized) << i << "th element";
        for (int j = 0; j < iDataLen; ++j) {
            EXPECT_EQ ('A', thekey.data()[4 + j]) << i << "th element, " << j << "th byte";
        }
    }
    W_DO(cursor.next());
    EXPECT_TRUE (cursor.eof());
    W_DO(test_env->commit_xct());
    
    return RCOK;
}

TEST (WeyTest, Test) {
    test_env->empty_logdata_dir();
    std::vector<std::pair<const char*, const char*> > additional_params;
    additional_params.push_back (std::pair<const char*, const char*>("sm_logbufsize", "512000"));
    additional_params.push_back (std::pair<const char*, const char*>("sm_logsize", "64000000"));
    EXPECT_EQ(test_env->runBtreeTest(wey_testcase, false, default_locktable_size, 1 << 15, 1 << 14,
        1,
        1000,
        256000,
        64,
        true,
        default_enable_swizzling,
        additional_params
    ), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
