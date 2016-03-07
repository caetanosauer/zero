#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "xct.h"
#include "sm_base.h"

btree_test_env *test_env;

// Test cases to test serial and traditional restart.
// Caller does not specify restart mode, default to serial mode.

class crash_empty : public crash_test_base {
public:
    w_rc_t pre_crash(ss_m *) {
        return RCOK;
    }

    w_rc_t post_crash(ss_m *) {
        return RCOK;
    }
};

/* Passing */
TEST (CrashTest, Empty) {
    test_env->empty_logdata_dir();
    crash_empty context;
    EXPECT_EQ(test_env->runCrashTest(&context), 0);
}
/**/

lsn_t get_durable_lsn() {
    lsn_t ret;
    ss_m::get_durable_lsn(ret);
    return ret;
}
void output_durable_lsn(int W_IFDEBUG1(num)) {
    DBGOUT1( << num << ".durable LSN=" << get_durable_lsn());
}


// this one is trivial as we call checkpoint
class crash_createindex_clean : public crash_test_base {
public:
    w_rc_t pre_crash(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(ss_m::checkpoint());
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_crash(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (0, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (CrashTest, CreateIndexClean) {
    test_env->empty_logdata_dir();
    crash_createindex_clean context;
    EXPECT_EQ(test_env->runCrashTest(&context), 0);  // default to serial mode
}
/**/

class crash_createindex_dirty : public crash_test_base {
public:
    w_rc_t pre_crash(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        return RCOK;
    }

    w_rc_t post_crash(ss_m *) {
        output_durable_lsn(3);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (0, s.rownum);
        return RCOK;
    }
};

/* Passing */
TEST (CrashTest, CreateIndexDirty) {
    test_env->empty_logdata_dir();
    crash_createindex_dirty context;
    EXPECT_EQ(test_env->runCrashTest(&context), 0);  // default to serial mode
}
/**/

class crash_insert_single : public crash_test_base {
public:
    w_rc_t pre_crash(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_crash(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (1, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa1"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (CrashTest, InsertSingle) {
    test_env->empty_logdata_dir();
    crash_insert_single context;
    EXPECT_EQ(test_env->runCrashTest(&context), 0);  // default to serial mode
}
/**/

class crash_insert_multi : public crash_test_base {
public:
    w_rc_t pre_crash(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_crash(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa3"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (CrashTest, InsertMulti) {
    test_env->empty_logdata_dir();
    crash_insert_multi context;
    EXPECT_EQ(test_env->runCrashTest(&context), 0);  // default to serial mode
}
/**/

class crash_insert_delete : public crash_test_base {
public:
    w_rc_t pre_crash(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa3", "data3"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));
        output_durable_lsn(3);
        W_DO(test_env->btree_remove_and_commit(_stid, "aa3"));
        output_durable_lsn(4);
        return RCOK;
    }

    w_rc_t post_crash(ss_m *) {
        output_durable_lsn(5);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (2, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa2"), s.maxkey);
        return RCOK;
    }
};

/* Passing */
TEST (CrashTest, InsertDelete) {
    test_env->empty_logdata_dir();
    crash_insert_delete context;
    EXPECT_EQ(test_env->runCrashTest(&context), 0);  // default to serial mode
}
/**/

class crash_insert_update : public crash_test_base {
public:
    w_rc_t pre_crash(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));
        W_DO(test_env->btree_update_and_commit(_stid, "aa2", "data3"));
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_crash(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (2, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa2"), s.maxkey);
        std::string data;
        W_DO(test_env->btree_lookup_and_commit(_stid, "aa2", data));
        EXPECT_EQ (data, "data3");
        return RCOK;
    }
};

/* Passing */
TEST (CrashTest, InsertUpdate) {
    test_env->empty_logdata_dir();
    crash_insert_update context;
    EXPECT_EQ(test_env->runCrashTest(&context), 0);  // default to serial mode
}
/**/

class crash_insert_overwrite : public crash_test_base {
public:
    w_rc_t pre_crash(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->btree_insert_and_commit(_stid, "aa1", "data1"));
        W_DO(test_env->btree_insert_and_commit(_stid, "aa2", "data2"));
        W_DO(test_env->btree_overwrite_and_commit(_stid, "aa2", "3", 4));
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_crash(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (2, s.rownum);
        EXPECT_EQ (std::string("aa1"), s.minkey);
        EXPECT_EQ (std::string("aa2"), s.maxkey);
        std::string data;
        W_DO(test_env->btree_lookup_and_commit(_stid, "aa2", data));
        EXPECT_EQ (data, "data3");
        return RCOK;
    }
};

/* Passing */
TEST (CrashTest, InsertOverwrite) {
    test_env->empty_logdata_dir();
    crash_insert_overwrite context;
    EXPECT_EQ(test_env->runCrashTest(&context), 0);  // default to serial mode
}
/**/

class crash_insert_many : public crash_test_base {
public:
    crash_insert_many(bool sorted, int recs) {
        w_assert0(recs < 100);
        _sorted = sorted;
        _recs = recs;
    }
    w_rc_t pre_crash(ss_m *ssm) {
        output_durable_lsn(1);
        W_DO(x_btree_create_index(ssm, &_volume, _stid, _root_pid));
        output_durable_lsn(2);
        W_DO(test_env->begin_xct());
        // Set the data size is the max_entry_size minus key size
        // because the total size must be smaller than or equal to
        // btree_m::max_entry_size()
        const int key_size = 5;
        const int data_size = btree_m::max_entry_size() - key_size;

        vec_t data;
        char datastr[data_size];
        memset(datastr, '\0', data_size);
        data.set(datastr, data_size);
        w_keystr_t key;
        char keystr[key_size];
        keystr[0] = 'k';
        keystr[1] = 'e';
        keystr[2] = 'y';
        for (int i = 0; i < _recs; ++i) {
            int num;
            if (_sorted) {
                num = i;
            } else {
                num = _recs - 1 - i;
            }
            keystr[3] = ('0' + ((num / 10) % 10));
            keystr[4] = ('0' + (num % 10));
            key.construct_regularkey(keystr, key_size);
            W_DO(ssm->create_assoc(_stid, key, data));
        }
        W_DO(test_env->commit_xct());
        output_durable_lsn(3);
        return RCOK;
    }

    w_rc_t post_crash(ss_m *) {
        output_durable_lsn(4);
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid, s));
        EXPECT_EQ (_recs, s.rownum);
        EXPECT_EQ (std::string("key00", 5), s.minkey);

        char keystr[5];
        keystr[0] = 'k';
        keystr[1] = 'e';
        keystr[2] = 'y';
        keystr[3] = ('0' + (((_recs - 1) / 10) % 10));
        keystr[4] = ('0' + ((_recs - 1) % 10));
        EXPECT_EQ (std::string(keystr, 5), s.maxkey);
        return RCOK;
    }
    bool _sorted;
    int _recs;
};

/* Passing */
TEST (CrashTest, InsertFewSorted) {
    test_env->empty_logdata_dir();
    crash_insert_many context (true, 5);
    EXPECT_EQ(test_env->runCrashTest(&context), 0);  // default to serial mode
}
/**/

/* Passing */
TEST (CrashTest, InsertFewUnsorted) {
    test_env->empty_logdata_dir();
    crash_insert_many context (false, 5);
    EXPECT_EQ(test_env->runCrashTest(&context), 0);  // default to serial mode
}
/**/

/* Passing */
TEST (CrashTest, InsertManySorted) {
    test_env->empty_logdata_dir();
    crash_insert_many context (true, 30);
    EXPECT_EQ(test_env->runCrashTest(&context), 0);  // default to serial mode
}
/**/

/* Passing */
TEST (CrashTest, InsertManyUnsorted) {
    test_env->empty_logdata_dir();
    crash_insert_many context (false, 7);
    EXPECT_EQ(test_env->runCrashTest(&context), 0);  // default to serial mode
}
/**/

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
