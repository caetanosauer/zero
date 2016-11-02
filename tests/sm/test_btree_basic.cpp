#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"

btree_test_env *test_env;

/**
 * Unit test for basic Insert/Select/Delete features of BTree.
 */

w_rc_t insert_simple(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert(stid, "a1", "data1"));
    W_DO(test_env->btree_insert(stid, "aa2", "data2"));
    W_DO(test_env->btree_insert(stid, "aaaa3", "data3"));
    W_DO(test_env->commit_xct());

    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));
    EXPECT_EQ (3, s.rownum);
    EXPECT_EQ (std::string("a1"), s.minkey);
    EXPECT_EQ (std::string("aaaa3"), s.maxkey);
    return RCOK;
}

TEST (BtreeBasicTest, InsertSimple) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_simple), 0);
}

TEST (BtreeBasicTest, InsertSimpleLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_simple, true), 0);
}

w_rc_t insert_toolong_fail(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert (stid, "a1", "data1lkjdflgjldfjgkldfjg1"));
    W_DO(test_env->btree_insert (stid, "ab2", "data1lkjdflgjldfjgkldfjg2"));
    W_DO(test_env->btree_insert (stid, "abc3", "data1lkjdflgjldfjgkldfjg3"));
    W_DO(test_env->btree_insert (stid, "abcd4", "data1lkjdflgjldfjgkldfjg4"));
    // this one is fine. though the key is longer than the definition, we actually don't care the schema.
    W_DO(test_env->btree_insert (stid, "abcde5", "data1lkjdflgjldfjgkldfjg5"));

    // should fail here because one page can't hold this record
    char long_data[SM_PAGESIZE + 1] = "";
    memset (long_data, 'a', SM_PAGESIZE);
    long_data[SM_PAGESIZE] = '\0';
    cout << "We should see too-long error here:" << endl;
    w_rc_t  rc = test_env->btree_insert (stid, "abcdef6", long_data);
    EXPECT_EQ (rc.err_num(), (w_error_codes) eRECWONTFIT);
    if (rc.err_num() == eRECWONTFIT) {
        cout << "yep, we did. the following message is NOT an error." << endl;
    } else {
        cerr << "wtf" << endl;
    }
    W_DO(test_env->commit_xct());

    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));
    EXPECT_EQ (5, s.rownum);
    EXPECT_EQ (std::string("a1"), s.minkey);
    EXPECT_EQ (std::string("abcde5"), s.maxkey);
    return rc;
}

TEST (BtreeBasicTest, InsertTooLongfail) {
    test_env->empty_logdata_dir();
    EXPECT_NE(test_env->runBtreeTest(insert_toolong_fail), 0);
}
TEST (BtreeBasicTest, InsertTooLongfailLock) {
    test_env->empty_logdata_dir();
    EXPECT_NE(test_env->runBtreeTest(insert_toolong_fail, true), 0);
}

w_rc_t insert_dup_fail(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert (stid, "key005", "data5"));
    W_DO(test_env->btree_insert (stid, "key004", "data4"));
    W_DO(test_env->btree_insert (stid, "key006", "data6"));
    // should fail here
    cout << "We should see a duplicate error here:" << endl;
    w_rc_t  rc = test_env->btree_insert (stid, "key006", "data7");
    EXPECT_EQ (rc.err_num(), (w_error_codes) eDUPLICATE);
    if (rc.err_num() == eDUPLICATE) {
        cout << "yep, we did. the following message is NOT an error." << endl;
    } else {
        cerr << "wtf: no duplicate?" << endl;
    }
    W_DO(test_env->commit_xct());

    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));
    EXPECT_EQ (3, s.rownum);
    EXPECT_EQ (std::string("key004"), s.minkey);
    EXPECT_EQ (std::string("key006"), s.maxkey);
    return rc;
}

TEST (BtreeBasicTest, InsertDupFail) {
    test_env->empty_logdata_dir();
    EXPECT_NE(test_env->runBtreeTest(insert_dup_fail), 0);
}
TEST (BtreeBasicTest, InsertDupFailLock) {
    test_env->empty_logdata_dir();
    EXPECT_NE(test_env->runBtreeTest(insert_dup_fail, true), 0);
}

w_rc_t insert_remove(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert (stid, "key005", "data5"));
    W_DO(test_env->btree_insert (stid, "key004", "data4"));
    W_DO(test_env->btree_insert (stid, "key006", "data6"));
    W_DO(test_env->btree_remove (stid, "key006"));
    W_DO(test_env->btree_insert (stid, "key006", "data7"));
    W_DO(test_env->commit_xct());

    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));
    EXPECT_EQ (3, s.rownum);
    EXPECT_EQ (std::string("key004"), s.minkey);
    EXPECT_EQ (std::string("key006"), s.maxkey);
    return RCOK;
}

TEST (BtreeBasicTest, InsertRemove) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_remove), 0);
}
TEST (BtreeBasicTest, InsertRemoveLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_remove, true), 0);
}

w_rc_t insert_remove_fail(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert (stid, "key005", "data5"));
    W_DO(test_env->btree_insert (stid, "key004", "data4"));
    W_DO(test_env->btree_insert (stid, "key006", "data6"));
    // should fail here
    cout << "We should see a NOTFOUND error here:" << endl;
    w_rc_t  rc = test_env->btree_remove (stid, "key003");
    EXPECT_EQ (rc.err_num(), eNOTFOUND);
    if (rc.err_num() == eNOTFOUND) {
        cout << "yep, we did. the following message is NOT an error." << endl;
    } else {
        cerr << "wtf?" << endl;
    }
    W_DO(test_env->commit_xct());
    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));
    EXPECT_EQ (3, s.rownum);
    EXPECT_EQ (std::string("key004"), s.minkey);
    EXPECT_EQ (std::string("key006"), s.maxkey);
    return rc;
}

TEST (BtreeBasicTest, InsertRemoveFail) {
    test_env->empty_logdata_dir();
    EXPECT_NE(test_env->runBtreeTest(insert_remove_fail), 0);
}
TEST (BtreeBasicTest, InsertRemoveFailLock) {
    test_env->empty_logdata_dir();
    EXPECT_NE(test_env->runBtreeTest(insert_remove_fail, true), 0);
}

w_rc_t insert_remove_fail_repeat(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert (stid, "key005", "data5"));
    W_DO(test_env->btree_insert (stid, "key004", "data4"));
    W_DO(test_env->btree_insert (stid, "key006", "data6"));
    W_DO(test_env->btree_remove (stid, "key005"));
    // should fail here
    cout << "We should see a NOTFOUND error here:" << endl;
    w_rc_t  rc = test_env->btree_remove (stid, "key005");
    EXPECT_EQ (rc.err_num(), (w_error_codes) eNOTFOUND);
    if (rc.err_num() == eNOTFOUND) {
        cout << "yep, we did. the following message is NOT an error." << endl;
    } else {
        cerr << "wtf?" << endl;
    }
    W_DO(test_env->commit_xct());

    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));
    EXPECT_EQ (2, s.rownum);
    EXPECT_EQ (std::string("key004"), s.minkey);
    EXPECT_EQ (std::string("key006"), s.maxkey);
    return rc;
}

TEST (BtreeBasicTest, InsertRemoveFailRepeat) {
    test_env->empty_logdata_dir();
    EXPECT_NE(test_env->runBtreeTest(insert_remove_fail_repeat), 0);
}
TEST (BtreeBasicTest, InsertRemoveFailRepeatLock) {
    test_env->empty_logdata_dir();
    EXPECT_NE(test_env->runBtreeTest(insert_remove_fail_repeat, true), 0);
}

w_rc_t insert_update(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert (stid, "key005", "data5"));
    W_DO(test_env->btree_insert (stid, "key004", "data4"));
    W_DO(test_env->btree_insert (stid, "key006", "data6"));
    W_DO(test_env->btree_update (stid, "key006", "data7"));
    W_DO(test_env->commit_xct());

    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));
    EXPECT_EQ (3, s.rownum);
    EXPECT_EQ (std::string("key004"), s.minkey);
    EXPECT_EQ (std::string("key006"), s.maxkey);

    W_DO(x_btree_verify(ssm, stid));

    W_DO(test_env->begin_xct());
    std::string data;
    test_env->btree_lookup(stid, "key006", data);
    EXPECT_EQ (std::string("data7"), data);

    W_DO(test_env->btree_update (stid, "key006", "d"));
    test_env->btree_lookup(stid, "key006", data);
    EXPECT_EQ (std::string("d"), data);

    W_DO(test_env->btree_update (stid, "key006", "dksjdfljslkdfjskldjf"));
    test_env->btree_lookup(stid, "key006", data);
    EXPECT_EQ (std::string("dksjdfljslkdfjskldjf"), data);
    W_DO(test_env->commit_xct());

    W_DO(x_btree_verify(ssm, stid));

    return RCOK;
}

TEST (BtreeBasicTest, InsertUpdate) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_update), 0);
}
TEST (BtreeBasicTest, InsertUpdateLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_update, true), 0);
}

w_rc_t insert_overwrite(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert (stid, "key005", "data5"));
    W_DO(test_env->btree_insert (stid, "key004", "data4"));
    W_DO(test_env->btree_insert (stid, "key006", "data6"));
    W_DO(test_env->btree_overwrite (stid, "key006", "b", 2));
    W_DO(test_env->commit_xct());

    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));
    EXPECT_EQ (3, s.rownum);
    EXPECT_EQ (std::string("key004"), s.minkey);
    EXPECT_EQ (std::string("key006"), s.maxkey);

    W_DO(x_btree_verify(ssm, stid));

    std::string data;
    test_env->btree_lookup_and_commit(stid, "key006", data);
    EXPECT_EQ (std::string("daba6"), data);

    return RCOK;
}

TEST (BtreeBasicTest, InsertOverwrite) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_overwrite), 0);
}
TEST (BtreeBasicTest, InsertOverwriteLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_overwrite, true), 0);
}

w_rc_t insert_overwrite_fail(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert (stid, "key005", "data5"));
    W_DO(test_env->btree_insert (stid, "key004", "data4"));
    W_DO(test_env->btree_insert (stid, "key006", "data6"));
    W_DO(test_env->btree_overwrite (stid, "key006", "b", 4));
    rc_t rc1 = test_env->btree_overwrite (stid, "key006", "b", 5);
    EXPECT_EQ (eRECWONTFIT, rc1.err_num());
    rc_t rc2 = test_env->btree_overwrite (stid, "key006", "b", 6);
    EXPECT_EQ (eRECWONTFIT, rc2.err_num());
    W_DO(test_env->commit_xct());

    return RCOK;
}

TEST (BtreeBasicTest, InsertOverwriteFail) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_overwrite_fail), 0);
}
TEST (BtreeBasicTest, InsertOverwriteFailLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_overwrite_fail, true), 0);
}

w_rc_t insert_many(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    w_keystr_t key;
    vec_t data;
    char keystr[6] = "";
    char datastr[50] = "";
    memset(keystr, '\0', 6);
    memset(datastr, '\0', 50);
    keystr[0] = 'k';
    keystr[1] = 'e';
    keystr[2] = 'y';
    datastr[0] = 'd';
    datastr[1] = 'a';
    datastr[2] = 't';
    cout << "Inserting 200 records into BTree..." << endl;
    for (int i = 0; i < 200; ++i) {
        datastr[3] = keystr[3] = ('0' + ((i / 100) % 10));
        datastr[4] = keystr[4] = ('0' + ((i / 10) % 10));
        datastr[5] = keystr[5] = ('0' + ((i / 1) % 10));
        for (int j = 6; j < 50; ++j) {
            ++datastr[j];
            if (datastr[j] > '9') datastr[j] = '0';
        }
        key.construct_regularkey(keystr, 6);
        data.set(datastr, 50);
        W_DO(ssm->create_assoc(stid, key, data));
    }
    W_DO(ssm->commit_xct());
    cout << "Inserted." << endl;

    x_btree_scan_result s;
    W_DO(x_btree_scan(ssm, stid, s, test_env->get_use_locks()));
    EXPECT_EQ (200, s.rownum);
    EXPECT_EQ (std::string("key000"), s.minkey);
    EXPECT_EQ (std::string("key199"), s.maxkey);
    return RCOK;
}

TEST (BtreeBasicTest, InsertMany) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_many), 0);
}

TEST (BtreeBasicTest, InsertManyLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_many, true), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
