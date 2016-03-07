#include "btree_test_env.h"
#include "btree.h"
#include "btree_impl.h"
#include "xct.h"

btree_test_env *test_env;
/**
 * Unit test for verification features of BTree.
 */
TEST (BtreeVerificationTest, ContextSimple) {
    verification_context context (15);
    EXPECT_EQ (context._hash_bits, 15);
    EXPECT_EQ (context._bitmap_size, (1 << 12));
    EXPECT_EQ (context._pages_checked, 0);
    EXPECT_TRUE (context.is_bitmap_clean());

    // correctly matching fact/expect
    context.add_expectation(100, 2, true, 5, "abcde");
    EXPECT_FALSE (context.is_bitmap_clean());
    context.add_fact(100, 2, true, 5, "abcde");
    EXPECT_TRUE (context.is_bitmap_clean());

    // key is different
    context.add_expectation(100, 2, true, 5, "vvvdd");
    EXPECT_FALSE (context.is_bitmap_clean());
    context.add_fact(100, 2, true, 5, "abcde");
    EXPECT_FALSE (context.is_bitmap_clean());
}

TEST (BtreeVerificationTest, ContextLevel) {
    verification_context context (15);
    EXPECT_TRUE (context.is_bitmap_clean());
    context.add_expectation(100, 2, true, 5, "abcde");
    EXPECT_FALSE (context.is_bitmap_clean());
    context.add_fact(100, 3, true, 5, "abcde");
    EXPECT_FALSE (context.is_bitmap_clean());
}

TEST (BtreeVerificationTest, ContextLength) {
    verification_context context (15);
    EXPECT_TRUE (context.is_bitmap_clean());
    context.add_expectation(100, 2, true, 5, "abcde");
    EXPECT_FALSE (context.is_bitmap_clean());
    context.add_fact(100, 2, true, 4, "abcde");
    EXPECT_FALSE (context.is_bitmap_clean());
}

TEST (BtreeVerificationTest, ContextPage) {
    verification_context context (15);
    EXPECT_TRUE (context.is_bitmap_clean());
    context.add_expectation(100, 2, true, 5, "abcde");
    EXPECT_FALSE (context.is_bitmap_clean());
    context.add_fact(10, 2, true, 5, "abcde");
    EXPECT_FALSE (context.is_bitmap_clean());
}

TEST (BtreeVerificationTest, ContextLoop) {
    verification_context context (15);
    EXPECT_TRUE (context.is_bitmap_clean());

    // correctly matching fact/expect
    for (PageID i = 1; i <= 100; ++i) {
        char key[5];
        for (int j = 0; j < 5; ++j) {
            key[j] = (char) (i ^ j);
        }
        context.add_expectation(i, (int16_t) (i / 10), false, 5, key);
        context.add_expectation(i, (int16_t) (i / 10), true, 4, key + 1);
    }
    EXPECT_FALSE (context.is_bitmap_clean());
    for (PageID i = 1; i <= 100; ++i) {
        char key[5];
        for (int j = 0; j < 5; ++j) {
            key[j] = (char) (i ^ j);
        }
        context.add_fact(i, (int16_t) (i / 10), false, 5, key);
        context.add_fact(i, (int16_t) (i / 10), true, 4, key + 1);
    }
    EXPECT_TRUE(context.is_bitmap_clean());

    // only one in 100 is incorrect
    for (PageID i = 1; i <= 100; ++i) {
        char key[5];
        for (int j = 0; j < 5; ++j) {
            key[j] = (char) (i ^ j);
        }
        context.add_expectation(i, (int16_t) (i / 10), false, 5, key);
        context.add_expectation(i, (int16_t) (i / 10), true, 4, key + 1);
    }
    EXPECT_FALSE (context.is_bitmap_clean());
    for (PageID i = 1; i <= 100; ++i) {
        char key[5];
        for (int j = 0; j < 5; ++j) {
            key[j] = (char) (i ^ j);
        }
        if (i == 50) {
            // incorrect fact
            context.add_fact(i + 1, (int16_t) (i / 10), false, 5, key);
        } else {
            context.add_fact(i, (int16_t) (i / 10), false, 5, key);
        }
        context.add_fact(i, (int16_t) (i / 10), true, 4, key + 1);
    }
    EXPECT_FALSE(context.is_bitmap_clean());
}

w_rc_t helper_create_assoc(ss_m* ssm, StoreID stid,
    const char *key, size_t keylen,
    const char *data, size_t datalen) {
    w_keystr_t k;
    k.construct_regularkey(key, keylen);
    vec_t d (data, datalen);
    W_DO(ssm->create_assoc(stid, k, d));
    return RCOK;
}
w_rc_t helper_destroy_assoc(ss_m* ssm, StoreID stid,
    const char *key, size_t keylen) {
    w_keystr_t k;
    k.construct_regularkey (key, keylen);
    W_DO(ssm->destroy_assoc(stid, k));
    return RCOK;
}
w_rc_t verify_simle(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_verify(ssm, stid));

    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    W_DO(helper_create_assoc(ssm, stid, "abcd4", 5, "datadata", 8));
    W_DO(helper_create_assoc(ssm, stid, "abcd2", 5, "datadata", 8));
    W_DO(helper_create_assoc(ssm, stid, "abcd3", 5, "datadata", 8));
    W_DO(helper_create_assoc(ssm, stid, "abcd5", 5, "datadata", 8));
    W_DO(helper_create_assoc(ssm, stid, "abcd1", 5, "datadata", 8));
    W_DO(ssm->commit_xct());

    W_DO(x_btree_verify(ssm, stid));

    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    W_DO(helper_destroy_assoc(ssm, stid, "abcd4", 5));
    W_DO(helper_destroy_assoc(ssm, stid, "abcd5", 5));
    W_DO(ssm->commit_xct());

    W_DO(x_btree_verify(ssm, stid));

    return RCOK;
}

TEST (BtreeVerificationTest, VerifySimple) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(verify_simle), 0);
}
TEST (BtreeVerificationTest, VerifySimpleLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(verify_simle, true), 0);
}

w_rc_t verify_splits(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(x_btree_verify(ssm, stid));

    // so that one leaf page can have only 2 tuples:
    const int data_size = 1;
    const int key_size  = btree_m::max_entry_size() - data_size;

    char data[data_size];
    char key[key_size];
    memset (data, 'a', data_size);
    memset (key,  '_', key_size);

    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    for (int i=1; i<=5; i++) {
        // ensure prefix is minimal so we use maximum possible space:
        key[0] = '0' + i;
        W_DO(helper_create_assoc(ssm, stid, key, key_size, data, data_size));
    }
    W_DO(ssm->commit_xct());
    // now they are just b-linked.

    W_DO(x_btree_verify(ssm, stid));

    return RCOK;
}

TEST (BtreeVerificationTest, VerifySplits) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(verify_splits), 0);
}
TEST (BtreeVerificationTest, VerifySplitsLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(verify_splits, true), 0);
}

w_rc_t insert_quite_many(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    w_keystr_t key;
    vec_t data;
    char keystr[7] = "";
    char datastr[50] = "";
    memset(keystr, '\0', 7);
    memset(datastr, '\0', 50);
    keystr[0] = 'k';
    keystr[1] = 'e';
    keystr[2] = 'y';
    datastr[0] = 'd';
    datastr[1] = 'a';
    datastr[2] = 't';
    cout << "Inserting 5050 records into BTree..." << endl;
    for (int i = 0; i < 5050; ++i) {
        if (i % 100 == 0) {
            // occasionally commit/begin to make sure bufferpool is not full
            W_DO(ssm->commit_xct());
            W_DO(ssm->begin_xct());
            test_env->set_xct_query_lock();
            bool consistent;
            W_DO(ssm->verify_index(stid, 19, consistent));
            EXPECT_TRUE (consistent)
                << " BTree verification failed after " << i << " records are inserted";
        }
        datastr[3] = keystr[3] = ('0' + ((i / 1000) % 10));
        datastr[4] = keystr[4] = ('0' + ((i / 100) % 10));
        datastr[5] = keystr[5] = ('0' + ((i / 10) % 10));
        datastr[6] = keystr[6] = ('0' + ((i / 1) % 10));
        for (int j = 7; j < 50; ++j) {
            ++datastr[j];
            if (datastr[j] > '9') datastr[j] = '0';
        }
        key.construct_regularkey(keystr, 7);
        data.set(datastr, 50);
        W_DO(ssm->create_assoc(stid, key, data));
    }
    W_DO(ssm->commit_xct());
    W_DO(x_btree_verify(ssm, stid));

    cout << "Inserted." << endl;
    return RCOK;
}

TEST (BtreeVerificationTest, InsertQuiteMany) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_quite_many, false, default_locktable_size,512, 64), 0);
}
TEST (BtreeVerificationTest, InsertQuiteManyLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(insert_quite_many, true, default_locktable_size, 512, 64), 0);
}

w_rc_t volume_empty(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));
    verify_volume_result result;
    W_DO(ssm->verify_volume(19, result));
    verification_context *context = result.get_context(stid);
    EXPECT_TRUE (context != NULL);
    if (context != NULL) {
        EXPECT_TRUE (context->_pages_checked == 1); // only root page
        EXPECT_TRUE (context->_pages_inconsistent == 0);
        EXPECT_TRUE (context->is_bitmap_clean());
    }
    return RCOK;
}

TEST (BtreeVerificationTest, VolumeEmpty) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(volume_empty), 0);
}

w_rc_t volume_insert(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    const int recsize = SM_PAGESIZE / 10;
    char datastr[recsize];
    memset (datastr, 'a', recsize);
    vec_t data;
    data.set(datastr, recsize);

    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    w_keystr_t key;
    char keystr[6] = "";
    memset(keystr, '\0', 6);

    keystr[0] = 'k';
    keystr[1] = 'e';
    keystr[2] = 'y';
    for (int i = 0; i < 100; ++i) {
        keystr[3] = ('0' + ((i / 100) % 10));
        keystr[4] = ('0' + ((i / 10) % 10));
        keystr[5] = ('0' + ((i / 1) % 10));
        key.construct_regularkey(keystr, 6);
        W_DO(ssm->create_assoc(stid, key, data));
    }
    W_DO(ssm->commit_xct());
    W_DO(x_btree_verify(ssm, stid));

    verify_volume_result result;
    W_DO(ssm->verify_volume(19, result));
    verification_context *context = result.get_context(stid);
    EXPECT_TRUE (context != NULL);
    if (context != NULL) {
        cout << "checked " << context->_pages_checked << " pages" << endl;
        EXPECT_TRUE (context->_pages_inconsistent == 0);
        EXPECT_TRUE (context->is_bitmap_clean());
    }
    return RCOK;
}

TEST (BtreeVerificationTest, VolumeInsert) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(volume_insert), 0);
}
TEST (BtreeVerificationTest, VolumeInsertLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(volume_insert, true), 0);
}

w_rc_t volume_many(ss_m* ssm, test_volume_t *test_volume) {
    // so that one leaf page can have only 8 or 9 tuples
    const int recsize = SM_PAGESIZE / 10;
    char datastr[recsize];
    memset (datastr, 'a', recsize);
    vec_t data;
    data.set(datastr, recsize);

    // create almost same two BTrees.
    StoreID stids[2];
    for (int store = 0; store < 2; ++store) {
        StoreID stid;
        PageID root_pid;
        W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));
        stids[store] = stid;

        W_DO(ssm->begin_xct());
        test_env->set_xct_query_lock();
        w_keystr_t key;
        char keystr[6] = "";
        memset(keystr, '\0', 6);

        keystr[0] = 's';
        keystr[1] = 't';
        keystr[2] = '1' + (store);
        cout << "Many records into BTree(" << store << ")..." << endl;
        for (int i = 0; i < 200; i += 2) {
            keystr[3] = ('0' + ((i / 100) % 10));
            keystr[4] = ('0' + ((i / 10) % 10));
            keystr[5] = ('0' + ((i / 1) % 10));
            key.construct_regularkey(keystr, 6);
            W_DO(ssm->create_assoc(stid, key, data));
            if (i % 50 == 0) {
                // occasionally commit/begin to make sure bufferpool is not full
                W_DO(ssm->commit_xct());
                W_DO(ssm->begin_xct());
                test_env->set_xct_query_lock();
            }
        }
        W_DO(ssm->commit_xct());
        W_DO(x_btree_verify(ssm, stid));
    }
    cout << "Inserted." << endl;
    // check it once
    {
        verify_volume_result result;
        W_DO(ssm->verify_volume(19, result));
        for (int store = 0; store < 2; ++store) {
            verification_context *context = result.get_context(stids[store]);
            EXPECT_TRUE (context != NULL);
            if (context != NULL) {
                cout << "store(" << store << "). checked " << context->_pages_checked << " pages" << endl;
                EXPECT_TRUE (context->_pages_inconsistent == 0);
                EXPECT_TRUE (context->is_bitmap_clean());
            }
        }
    }

    // add even more foster pages (this time no adopt)
    for (int store = 0; store < 2; ++store) {
        StoreID stid = stids[store];

        W_DO(ssm->begin_xct());
        test_env->set_xct_query_lock();
        w_keystr_t key;
        char keystr[6] = "";
        memset(keystr, '\0', 6);

        keystr[0] = 's';
        keystr[1] = 't';
        keystr[2] = '1' + (store);
        cout << "More records into BTree(" << store << ")..." << endl;
        for (int i = 1; i < 200; i += 2) {
            keystr[3] = ('0' + ((i / 100) % 10));
            keystr[4] = ('0' + ((i / 10) % 10));
            keystr[5] = ('0' + ((i / 1) % 10));
            key.construct_regularkey(keystr, 6);
            W_DO(ssm->create_assoc(stid, key, data));
            if (i % 50 == 0) {
                // occasionally commit/begin to make sure bufferpool is not full
                W_DO(ssm->commit_xct());
                W_DO(ssm->begin_xct());
                test_env->set_xct_query_lock();
            }
        }
        W_DO(ssm->commit_xct());
        W_DO(x_btree_verify(ssm, stid));
    }

    // check it again
    {
        verify_volume_result result;
        W_DO(ssm->verify_volume(19, result));
        for (int store = 0; store < 2; ++store) {
            verification_context *context = result.get_context(stids[store]);
            EXPECT_TRUE (context != NULL);
            if (context != NULL) {
                cout << "store(" << store << "). checked " << context->_pages_checked << " pages" << endl;
                EXPECT_TRUE (context->_pages_inconsistent == 0);
                EXPECT_TRUE (context->is_bitmap_clean());
            }
        }
    }
    return RCOK;
}

TEST (BtreeVerificationTest, VolumeMany) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(volume_many, false, default_locktable_size, 512, 64), 0);
}
TEST (BtreeVerificationTest, VolumeManyLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(volume_many, true, default_locktable_size, 512, 64), 0);
}

w_rc_t inquery_verify(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    const int recsize = SM_PAGESIZE / 10;
    char datastr[recsize];
    memset (datastr, 'a', recsize);
    vec_t data;
    data.set(datastr, recsize);

    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    xct_t * x = xct();
    EXPECT_EQ(x->inquery_verify_context().pages_checked, 0);
    EXPECT_EQ(x->inquery_verify_context().pids_inconsistent.size(), (uint) 0);
    x->set_inquery_verify(true); // verification mode on
    x->set_inquery_verify_keyorder(true); // detailed check for sortedness/uniqueness
    x->set_inquery_verify_space(true); // detailed check for space overlap
    w_keystr_t key;
    char keystr[6] = "";
    memset(keystr, '\0', 6);

    //insert many
    keystr[0] = 'k';
    keystr[1] = 'e';
    keystr[2] = 'y';
    for (int i = 0; i < 100; ++i) {
        keystr[3] = ('0' + ((i / 100) % 10));
        keystr[4] = ('0' + ((i / 10) % 10));
        keystr[5] = ('0' + ((i / 1) % 10));
        key.construct_regularkey(keystr, 6);
        W_DO(ssm->create_assoc(stid, key, data));
    }
    cout << x->inquery_verify_context().pages_checked << " pages checked so far" << endl;
    EXPECT_GT(x->inquery_verify_context().pages_checked, 0);
    EXPECT_EQ(x->inquery_verify_context().pids_inconsistent.size(), (uint) 0);
    //remove a few
    W_DO(x_btree_remove(ssm, stid, "key035"));
    W_DO(x_btree_remove(ssm, stid, "key078"));
    W_DO(x_btree_remove(ssm, stid, "key012"));
    cout << x->inquery_verify_context().pages_checked << " pages checked so far" << endl;
    EXPECT_GT(x->inquery_verify_context().pages_checked, 0);
    EXPECT_EQ(x->inquery_verify_context().pids_inconsistent.size(), (uint) 0);
    W_DO(ssm->commit_xct());

    W_DO(x_btree_verify(ssm, stid));


    W_DO(ssm->begin_xct());
    test_env->set_xct_query_lock();
    x = xct();
    EXPECT_EQ(x->inquery_verify_context().pages_checked, 0);
    EXPECT_EQ(x->inquery_verify_context().pids_inconsistent.size(), (uint) 0);
    x->set_inquery_verify(true); // verification mode on
    x->set_inquery_verify_keyorder(true); // detailed check for sortedness/uniqueness
    x->set_inquery_verify_space(true); // detailed check for space overlap

    // inserts more
    for (int i = 100; i < 200; ++i) {
        keystr[3] = ('0' + ((i / 100) % 10));
        keystr[4] = ('0' + ((i / 10) % 10));
        keystr[5] = ('0' + ((i / 1) % 10));
        key.construct_regularkey(keystr, 6);
        W_DO(ssm->create_assoc(stid, key, data));
    }
    cout << x->inquery_verify_context().pages_checked << " pages checked so far" << endl;
    EXPECT_GT(x->inquery_verify_context().pages_checked, 0);
    EXPECT_EQ(x->inquery_verify_context().pids_inconsistent.size(), (uint) 0);

    // remove and insert a few more
    W_DO(x_btree_remove(ssm, stid, "key022"));
    W_DO(x_btree_insert(ssm, stid, "key035", "assdd"));
    cout << x->inquery_verify_context().pages_checked << " pages checked so far" << endl;
    EXPECT_GT(x->inquery_verify_context().pages_checked, 0);
    EXPECT_EQ(x->inquery_verify_context().pids_inconsistent.size(), (uint) 0);

    smsize_t elen = recsize;
    bool found;
    w_keystr_t dummy_key;
    dummy_key.construct_regularkey("key150", 6);
    W_DO(ssm->find_assoc(stid, dummy_key, datastr, elen, found));
    EXPECT_TRUE(found);

    dummy_key.construct_regularkey("key078", 6);
    W_DO(ssm->find_assoc(stid, dummy_key, datastr, elen, found));
    EXPECT_FALSE(found);

    cout << x->inquery_verify_context().pages_checked << " pages checked so far" << endl;
    EXPECT_GT(x->inquery_verify_context().pages_checked, 0);
    EXPECT_EQ(x->inquery_verify_context().pids_inconsistent.size(), (uint) 0);

    W_DO(ssm->commit_xct());

    return RCOK;
}

TEST (BtreeVerificationTest, InQueryVerify) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(inquery_verify, false, default_locktable_size, 512, 64), 0);
}
TEST (BtreeVerificationTest, InQueryVerifyLock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(inquery_verify, true, default_locktable_size, 512, 64), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
