#define SM_SOURCE

#include "sm_base.h"
#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "xct.h"
#include <sys/time.h>

#include "smthread.h"
#include "sthread.h"
#include "lock_x.h"
#include "lock_s.h"
#include "lock.h"
#include "lock_core.h"


btree_test_env *test_env;
bool is_too_small_okvl() {
    if (OKVL_PARTITIONS < 20) {
        // this experiment needs to distinguish by partition ID. so, too small partition
        // does not work. well, even 20 might cause a partition collision, but so are usual
        // lock test cases.
        std::cout << "OKVL_PARTITIONS is too small for this experiment. no test" << std::endl;
        return true;
    }
    return false;
}

/**
 * Testcases for Deadlock detection.
 */
int next_thid = 10;
int locktable_size = 1 << 12;
const int LONGTIME_USEC = 20000; // like forever in the tests.

rc_t _prep(ss_m* ssm, test_volume_t *test_volume, StoreID &stid) {
    OKVL_EXPERIMENT = true;
    OKVL_INIT_STR_PREFIX_LEN = 1 + 1; // "a" or "b". +1 for sign byte
    OKVL_INIT_STR_UNIQUEFIER_LEN = 4; // "unqX"
    EXPECT_TRUE(test_env->_use_locks);
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert(stid, "aunq1", "data"));
    W_DO(test_env->btree_insert(stid, "aunq2", "data"));
    W_DO(test_env->btree_insert(stid, "aunq3", "data"));
    W_DO(test_env->btree_insert(stid, "aunq4", "data"));
    W_DO(test_env->btree_insert(stid, "bunq1", "data"));
    W_DO(test_env->btree_insert(stid, "bunq2", "data"));
    W_DO(test_env->commit_xct());
    return RCOK;
}

class access_thread_t : public smthread_t {
public:
        access_thread_t(StoreID stid, const char* key, bool write)
                : smthread_t(t_regular, "access_thread_t"),
                _stid(stid), _key(key), _write(write), _done(false), _exitted(false) {
            _thid = next_thid++;
        }
        ~access_thread_t()  {}
        virtual void run() {
            _begin();
            w_keystr_t key;
            key.construct_regularkey(_key, ::strlen(_key));
            std::cout << ":T" << _thid << ":" << (_write ? "writing " : "reading ") << _key << ".." << std::endl;
            if (_write) {
                _rc = ss_m::overwrite_assoc(_stid, key, "datb", 0, 4);
            } else {
                char data[20];
                smsize_t datalen = 20;
                bool found;
                _rc = ss_m::find_assoc(_stid, key, data, datalen, found);
                EXPECT_TRUE (found);
            }
            report_time();
            if (_rc.is_error()) {
                std::cout << ":T" << _thid << ":" << "received an error " << _rc << ". abort." << std::endl;
                rc_t abort_rc = ss_m::abort_xct();
                std::cout << ":T" << _thid << ": aborted. rc=" << abort_rc << std::endl;
                _exitted = true;
                return;
            }
            std::cout << ":T" << _thid << ":done " << _key << ". committing..." << std::endl;
            _done = true;

            _commit ();
        }
        void _begin() {
            ::gettimeofday(&_start,NULL);
            _rc = ss_m::begin_xct();
            EXPECT_FALSE(_rc.is_error()) << _rc;
            g_xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);
            report_time();
            std::cout << ":T" << _thid << " begins. " << std::endl;
        }
        void _commit() {
            _rc = ss_m::commit_xct();
            report_time();
            std::cout << ":T" << _thid << ":committed." << std::endl;
            EXPECT_FALSE(_rc.is_error()) << _rc;
            _exitted = true;
        }
        void report_time() {
            timeval now, result;
            ::gettimeofday(&now,NULL);
            timersub(&now, &_start, &result);
            cout << (result.tv_sec * 1000000 + result.tv_usec);
        }

        int  return_value() const { return 0; }

    StoreID _stid;
    const char* _key;
    bool _write;
    rc_t _rc;
    bool _done;
    bool _exitted;
    timeval _start;
    int _thid;
};

class write_thread_t : public access_thread_t {
public:
    write_thread_t(StoreID stid, const char* key) : access_thread_t(stid, key, true) {}
};
class read_thread_t : public access_thread_t {
public:
    read_thread_t(StoreID stid, const char* key) : access_thread_t(stid, key, false) {}
};
class multiaccess_thread_t : public access_thread_t {
public:
    multiaccess_thread_t(StoreID stid, const std::vector<const char*> &keys, const std::vector<bool> &writes)
        : access_thread_t(stid, NULL, false) {
        _init (keys, writes);
    }
    multiaccess_thread_t(StoreID stid, const char *key1, bool write1, const char *key2, bool write2)
        : access_thread_t(stid, NULL, false) {
        std::vector<const char*> keys;
        keys.push_back(key1);
        keys.push_back(key2);
        std::vector<bool> writes;
        writes.push_back(write1);
        writes.push_back(write2);
        _init (keys, writes);
    }
    multiaccess_thread_t(StoreID stid, const char *key1, bool write1, const char *key2, bool write2, const char *key3, bool write3)
        : access_thread_t(stid, NULL, false) {
        std::vector<const char*> keys;
        keys.push_back(key1);
        keys.push_back(key2);
        keys.push_back(key3);
        std::vector<bool> writes;
        writes.push_back(write1);
        writes.push_back(write2);
        writes.push_back(write3);
        _init (keys, writes);
    }
    multiaccess_thread_t(StoreID stid, const char *key1, bool write1, const char *key2, bool write2, const char *key3, bool write3, const char *key4, bool write4)
        : access_thread_t(stid, NULL, false) {
        std::vector<const char*> keys;
        keys.push_back(key1);
        keys.push_back(key2);
        keys.push_back(key3);
        keys.push_back(key4);
        std::vector<bool> writes;
        writes.push_back(write1);
        writes.push_back(write2);
        writes.push_back(write3);
        writes.push_back(write4);
        _init (keys, writes);
    }
    multiaccess_thread_t(StoreID stid, const char *key1, bool write1, const char *key2, bool write2, const char *key3, bool write3, const char *key4, bool write4, const char *key5, bool write5)
        : access_thread_t(stid, NULL, false) {
        std::vector<const char*> keys;
        keys.push_back(key1);
        keys.push_back(key2);
        keys.push_back(key3);
        keys.push_back(key4);
        keys.push_back(key5);
        std::vector<bool> writes;
        writes.push_back(write1);
        writes.push_back(write2);
        writes.push_back(write3);
        writes.push_back(write4);
        writes.push_back(write5);
        _init (keys, writes);
    }
    void _init (const std::vector<const char*> &keys, const std::vector<bool> &writes) {
        EXPECT_EQ(keys.size(), writes.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            _key_multi.push_back(keys[i]);
            _write_multi.push_back(writes[i]);
            _done_multi.push_back(false);
        }
    }

    virtual void run() {
        _begin();

        for (size_t i = 0; i < _key_multi.size(); ++i) {
            w_keystr_t key;
            key.construct_regularkey(_key_multi[i], ::strlen(_key_multi[i]));
            report_time();
            std::cout << ":T" << _thid << "(" << i << "): " << (_write_multi[i] ? "writing " : "reading ") << _key_multi[i] << ".." << std::endl;
            if (_write_multi[i]) {
                _rc = ss_m::overwrite_assoc(_stid, key, "datb", 0, 4);
            } else {
                char data[20];
                smsize_t datalen = 20;
                bool found;
                _rc = ss_m::find_assoc(_stid, key, data, datalen, found);
                EXPECT_TRUE (found);
            }
            report_time();
            if (_rc.is_error()) {
                std::cout << ":T" << _thid << "(" << i << "): received an error " << _rc << ". aborting..." << std::endl;
                rc_t abort_rc = ss_m::abort_xct();
                std::cout << ":T" << _thid << "(" << i << "): aborted. rc=" << abort_rc << std::endl;
                _exitted = true;
                return;
            }
            std::cout << ":T" << _thid << "(" << i << "):done." << std::endl;
            _done_multi[i] = true;
        }
        _done = true;

        _commit ();
    }

    std::vector<const char*> _key_multi;
    std::vector<bool> _write_multi;
    std::vector<bool> _done_multi;
};

w_rc_t read_write_livelock(ss_m* ssm, test_volume_t *test_volume) {
    EXPECT_TRUE(test_env->_use_locks);
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));

    std::string data;

    // read a2
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_lookup(stid, "aunq2", data));

    // write a2
    write_thread_t t2 (stid, "aunq2");
    W_DO(t2.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_FALSE(t2._done);
    EXPECT_FALSE(t2._exitted);

    W_DO(test_env->commit_xct());
    W_DO(t2.join());
    EXPECT_TRUE(t2._done);
    EXPECT_TRUE(t2._exitted);

    return RCOK;
}

TEST (LockOkvlTest, ReadWriteLivelock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(read_write_livelock, true, locktable_size), 0);
}

w_rc_t write_read_livelock(ss_m* ssm, test_volume_t *test_volume) {
    EXPECT_TRUE(test_env->_use_locks);
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));

    // write a2
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_overwrite(stid, "aunq2", "datb", 0));

    // read a2
    read_thread_t t2 (stid, "aunq2");
    W_DO(t2.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_FALSE(t2._done);
    EXPECT_FALSE(t2._exitted);

    W_DO(test_env->commit_xct());
    W_DO(t2.join());
    EXPECT_TRUE(t2._done);
    EXPECT_TRUE(t2._exitted);

    return RCOK;
}


TEST (LockOkvlTest, WriteReadLivelock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(write_read_livelock, true, locktable_size), 0);
}

w_rc_t write_read_write_livelock(ss_m* ssm, test_volume_t *test_volume) {
    EXPECT_TRUE(test_env->_use_locks);
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));

    // write a2
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_overwrite(stid, "aunq2", "datb", 0));

    // read a3 and then read a2
    multiaccess_thread_t t2 (stid, "aunq3", false, "aunq2", false);
    W_DO(t2.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_TRUE(t2._done_multi[0]);
    EXPECT_FALSE(t2._done_multi[1]);
    EXPECT_FALSE(t2._exitted);

    // write a3
    write_thread_t t3 (stid, "aunq3");
    W_DO(t3.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_FALSE(t3._done);
    EXPECT_FALSE(t3._exitted);
    EXPECT_FALSE(t2._exitted);

    W_DO(test_env->commit_xct());
    W_DO(t2.join());
    W_DO(t3.join());

    EXPECT_TRUE(t2._done_multi[0]);
    EXPECT_TRUE(t2._done_multi[1]);
    EXPECT_TRUE(t2._done);
    EXPECT_TRUE(t2._exitted);
    EXPECT_TRUE(t3._done);
    EXPECT_TRUE(t3._exitted);

    return RCOK;
}

TEST (LockOkvlTest, WriteReadWriteLivelock) {
    if (is_too_small_okvl()) {
        return;
    }
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(write_read_write_livelock, true, locktable_size), 0);
}

w_rc_t write_read_write_deadlock(ss_m* ssm, test_volume_t *test_volume) {
    EXPECT_TRUE(test_env->_use_locks);
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));

    // write a2 (just to pause t2/t3)
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_overwrite(stid, "aunq2", "datb", 0));

    // read a3, (pause), write a4
    multiaccess_thread_t t2 (stid, "aunq3", false, "aunq2", false, "aunq4", true);
    W_DO(t2.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_TRUE(t2._done_multi[0]);
    EXPECT_FALSE(t2._done_multi[1]);
    EXPECT_FALSE(t2._exitted);

    // write a4, (pause), write a3
    multiaccess_thread_t t3 (stid, "aunq4", true, "aunq2", false, "aunq3", true);
    W_DO(t3.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_TRUE(t3._done_multi[0]);
    EXPECT_FALSE(t3._done_multi[1]);
    EXPECT_FALSE(t3._exitted);

    W_DO(test_env->commit_xct());
    W_DO(t2.join());
    W_DO(t3.join());

    // now that we use RAW-style lock manager, we can't choose deadlock victim by any policy.
    // So, though t3 is younger, t2 might be the victim. both cases are possible.
    EXPECT_TRUE(t2._exitted);
    EXPECT_TRUE(t3._exitted);
    EXPECT_TRUE(t2._done_multi[0]);
    EXPECT_TRUE(t2._done_multi[1]);
    EXPECT_TRUE(t3._done_multi[0]);
    EXPECT_TRUE(t3._done_multi[1]);
    if (t3._rc.is_error()) {
        // t3 was the victim
        EXPECT_FALSE(t2._rc.is_error());
        EXPECT_EQ(t3._rc.err_num(), (w_error_codes) eDEADLOCK);
        EXPECT_TRUE(t2._done_multi[2]);
        EXPECT_FALSE(t3._done_multi[2]);
    } else {
        // t2 was the victim
        EXPECT_TRUE(t2._rc.is_error());
        EXPECT_EQ(t2._rc.err_num(), (w_error_codes) eDEADLOCK);
        EXPECT_FALSE(t2._done_multi[2]);
        EXPECT_TRUE(t3._done_multi[2]);
    }
    return RCOK;
}

TEST (LockOkvlTest, WriteReadWriteDeadlock) {
    if (is_too_small_okvl()) {
        return;
    }
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(write_read_write_deadlock, true, locktable_size), 0);
}

w_rc_t conversion_deadlock(ss_m* ssm, test_volume_t *test_volume) {
    EXPECT_TRUE(test_env->_use_locks);
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));

    // write a2 (just to pause t2/t3)
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_overwrite(stid, "aunq2", "datb", 0));

    // read a3, (pause), write a3
    multiaccess_thread_t t2 (stid, "aunq3", false, "aunq2", false, "aunq3", true);
    W_DO(t2.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_TRUE(t2._done_multi[0]);
    EXPECT_FALSE(t2._done_multi[1]);
    EXPECT_FALSE(t2._exitted);

    // read a3, (pause), write a3
    multiaccess_thread_t t3 (stid, "aunq3", false, "aunq2", false, "aunq3", true);
    W_DO(t3.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_TRUE(t3._done_multi[0]);
    EXPECT_FALSE(t3._done_multi[1]);
    EXPECT_FALSE(t3._exitted);

    W_DO(test_env->commit_xct());
    W_DO(t2.join());
    W_DO(t3.join());

    // now that we use RAW-style lock manager, we can't choose deadlock victim by any policy.
    // So, though t3 is younger, t2 might be the victim. both cases are possible.
    EXPECT_TRUE(t2._exitted);
    EXPECT_TRUE(t3._exitted);
    EXPECT_TRUE(t2._done_multi[0]);
    EXPECT_TRUE(t2._done_multi[1]);
    EXPECT_TRUE(t3._done_multi[0]);
    EXPECT_TRUE(t3._done_multi[1]);
    if (t3._rc.is_error()) {
        // t3 was the victim
        EXPECT_FALSE(t2._rc.is_error());
        EXPECT_EQ(t3._rc.err_num(), (w_error_codes) eDEADLOCK);
        EXPECT_TRUE(t2._done_multi[2]);
        EXPECT_FALSE(t3._done_multi[2]);
    } else {
        // t2 was the victim
        EXPECT_TRUE(t2._rc.is_error());
        EXPECT_EQ(t2._rc.err_num(), (w_error_codes) eDEADLOCK);
        EXPECT_FALSE(t2._done_multi[2]);
        EXPECT_TRUE(t3._done_multi[2]);
    }

    return RCOK;
}

TEST (LockOkvlTest, ConversionDeadlock) {
    if (is_too_small_okvl()) {
        return;
    }
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(conversion_deadlock, true, locktable_size), 0);
}

// this one is motivated by a bug that existed before.
// the problem is that when the waiting happens only because of conversion of other xct,
// previous dreadlock code didn't consider it as "waiting for it".
// see jira ticket:103 "[Experiment] ELR" (originally trac ticket:105)
w_rc_t indirect_conversion_deadlock(ss_m* ssm, test_volume_t *test_volume) {
    EXPECT_TRUE(test_env->_use_locks);
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));

    // write a2 (just to pause t2/t3/t4)
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_overwrite(stid, "aunq2", "datb", 0));

    // read a3, read a4, (pause), write b1
    multiaccess_thread_t t2 (stid, "aunq3", false, "aunq4", false, "aunq2", false, "bunq1", true);
    W_DO(t2.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_TRUE(t2._done_multi[0]);
    EXPECT_TRUE(t2._done_multi[1]);
    EXPECT_FALSE(t2._done_multi[2]);
    EXPECT_FALSE(t2._exitted);

    // read a3, write a3 (no pause. because this has to come BEFORE t4 to reproduce the bug)
    multiaccess_thread_t t3 (stid, "aunq3", false, "aunq3", true);
    W_DO(t3.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_TRUE(t3._done_multi[0]);
    EXPECT_FALSE(t3._done_multi[1]);
    EXPECT_FALSE(t3._exitted);

    // write b1, (pause), read a3
    multiaccess_thread_t t4 (stid, "bunq1", true, "aunq2", false, "aunq3", false);
    W_DO(t4.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_TRUE(t3._done_multi[0]);
    EXPECT_FALSE(t3._done_multi[1]);
    EXPECT_FALSE(t3._exitted);

    W_DO(test_env->commit_xct()); // go!
    ::usleep (LONGTIME_USEC * 20);
    EXPECT_TRUE(t2._exitted);
    EXPECT_TRUE(t3._exitted);
    EXPECT_TRUE(t4._exitted);
    if (!t2._exitted || !t3._exitted || !t4._exitted) {
        cout << "oops! the bug was reproduced!" << endl;
        if (!t2._exitted) {
            rc_t rc = t2.smthread_unblock(eDEADLOCK);
            cout << "killed t2. rc=" << rc << endl;
        }
        if (!t3._exitted) {
            rc_t rc = t3.smthread_unblock(eDEADLOCK);
            cout << "killed t3. rc=" << rc << endl;
        }
        if (!t4._exitted) {
            rc_t rc = t4.smthread_unblock(eDEADLOCK);
            cout << "killed t4. rc=" << rc << endl;
        }
        ::usleep (LONGTIME_USEC);
    }

    W_DO(t2.join());
    W_DO(t3.join());
    W_DO(t4.join());

    // this is triangular and complex. so I'm not sure which one should be victim.
    // but some of them has to be killed
    EXPECT_TRUE(t2._rc.is_error() || t3._rc.is_error() || t4._rc.is_error());
    if (t2._rc.is_error()) {
        cout << "victim was t2!" << endl;
        EXPECT_EQ(t2._rc.err_num(), eDEADLOCK);
    }
    if (t3._rc.is_error()) {
        cout << "victim was t3!" << endl;
        EXPECT_EQ(t3._rc.err_num(), eDEADLOCK);
    }
    if (t4._rc.is_error()) {
        cout << "victim was t4!" << endl;
        EXPECT_EQ(t4._rc.err_num(), eDEADLOCK);
    }

    return RCOK;
}

TEST (LockOkvlTest, IndirectConversionDeadlock) {
    if (is_too_small_okvl()) {
        return;
    }
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(indirect_conversion_deadlock, true, locktable_size), 0);
}

w_rc_t complex1_deadlock(ss_m* ssm, test_volume_t *test_volume) {
    EXPECT_TRUE(test_env->_use_locks);
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));

    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_overwrite(stid, "aunq2", "datb", 0));

    // read a3, (pause), write a4
    multiaccess_thread_t t2 (stid, "aunq3", false, "aunq2", false, "aunq4", true);
    W_DO(t2.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_TRUE(t2._done_multi[0]);
    EXPECT_FALSE(t2._done_multi[1]);
    EXPECT_FALSE(t2._exitted);

    // read a4, (pause), write a3
    multiaccess_thread_t t3 (stid, "aunq4", false, "aunq2", false, "aunq3", true);
    W_DO(t3.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_TRUE(t3._done_multi[0]);
    EXPECT_FALSE(t3._done_multi[1]);
    EXPECT_FALSE(t3._exitted);

    // and a few others to be reading a3/a4 and soon commit without deadlock
    multiaccess_thread_t t4 (stid, "aunq3", false, "aunq2", false);
    multiaccess_thread_t t5 (stid, "aunq3", false, "aunq2", false);
    multiaccess_thread_t t6 (stid, "aunq3", false, "aunq2", false);
    multiaccess_thread_t t7 (stid, "aunq4", false, "aunq2", false);
    multiaccess_thread_t t8 (stid, "aunq4", false, "aunq2", false);
    W_DO(t4.fork());
    W_DO(t5.fork());
    W_DO(t6.fork());
    W_DO(t7.fork());
    W_DO(t8.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_FALSE(t2._exitted);
    EXPECT_FALSE(t3._exitted);
    EXPECT_FALSE(t4._exitted);
    EXPECT_FALSE(t5._exitted);
    EXPECT_FALSE(t6._exitted);
    EXPECT_FALSE(t7._exitted);
    EXPECT_FALSE(t8._exitted);

    W_DO(test_env->commit_xct());
    cout << "t2 join" << endl;
    W_DO(t2.join());
    cout << "t3 join" << endl;
    W_DO(t3.join());
    cout << "t4 join" << endl;
    W_DO(t4.join());
    cout << "t5 join" << endl;
    W_DO(t5.join());
    cout << "t6 join" << endl;
    W_DO(t6.join());
    cout << "t7 join" << endl;
    W_DO(t7.join());
    cout << "t8 join" << endl;
    W_DO(t8.join());
    cout << "joined all!" << endl;

    // now that we use RAW-style lock manager, we can't choose deadlock victim by any policy.
    // So, though t3 is younger, t2 might be the victim. both cases are possible.
    EXPECT_TRUE(t2._exitted);
    EXPECT_TRUE(t3._exitted);
    EXPECT_FALSE(t4._rc.is_error());
    EXPECT_FALSE(t5._rc.is_error());
    EXPECT_FALSE(t6._rc.is_error());
    EXPECT_FALSE(t7._rc.is_error());
    EXPECT_FALSE(t8._rc.is_error());
    EXPECT_TRUE(t2._done_multi[0]);
    EXPECT_TRUE(t2._done_multi[1]);
    EXPECT_TRUE(t3._done_multi[0]);
    EXPECT_TRUE(t3._done_multi[1]);
    if (!t2._rc.is_error()) {
        // t3 was victim
        EXPECT_EQ(t3._rc.err_num(), (w_error_codes) eDEADLOCK);
        EXPECT_TRUE(t2._done_multi[2]);
        EXPECT_FALSE(t3._done_multi[2]);
    } else if (!t3._rc.is_error()) {
        // t2 was victim
        EXPECT_TRUE(t2._rc.is_error());
        EXPECT_EQ(t2._rc.err_num(), (w_error_codes) eDEADLOCK);
        EXPECT_FALSE(t2._done_multi[2]);
        EXPECT_TRUE(t3._done_multi[2]);
    } else {
        // both were victims. because this involves multiple transactions, it's possible
        EXPECT_EQ(t2._rc.err_num(), (w_error_codes) eDEADLOCK);
        EXPECT_EQ(t3._rc.err_num(), (w_error_codes) eDEADLOCK);
        EXPECT_FALSE(t2._done_multi[2]);
        EXPECT_FALSE(t3._done_multi[2]);
    }

    return RCOK;
}

TEST (LockOkvlTest, Complex1Deadlock) {
    if (is_too_small_okvl()) {
        return;
    }
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(complex1_deadlock, true, locktable_size), 0);
}

w_rc_t complex2_deadlock(ss_m* ssm, test_volume_t *test_volume) {
    EXPECT_TRUE(test_env->_use_locks);
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));

    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_overwrite(stid, "aunq2", "datb", 0));

    // read a3, read a4, (pause), write a3
    multiaccess_thread_t t2 (stid, "aunq3", false, "aunq4", false, "aunq2", false, "aunq3", true);
    W_DO(t2.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_TRUE(t2._done_multi[0]);
    EXPECT_TRUE(t2._done_multi[1]);
    EXPECT_FALSE(t2._done_multi[2]);
    EXPECT_FALSE(t2._exitted);

    // read a3, read a4, (pause), write a4
    multiaccess_thread_t t3 (stid, "aunq3", false, "aunq4", false, "aunq2", false, "aunq4", true);
    W_DO(t3.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_TRUE(t3._done_multi[0]);
    EXPECT_TRUE(t3._done_multi[1]);
    EXPECT_FALSE(t3._done_multi[2]);
    EXPECT_FALSE(t3._exitted);

    // and a few others to be reading a3/a4 and soon commit without deadlock
    multiaccess_thread_t t4 (stid, "aunq3", false, "aunq2", false);
    multiaccess_thread_t t5 (stid, "aunq3", false, "aunq2", false);
    multiaccess_thread_t t6 (stid, "aunq3", false, "aunq2", false);
    multiaccess_thread_t t7 (stid, "aunq4", false, "aunq2", false);
    multiaccess_thread_t t8 (stid, "aunq4", false, "aunq2", false);
    W_DO(t4.fork());
    W_DO(t5.fork());
    W_DO(t6.fork());
    W_DO(t7.fork());
    W_DO(t8.fork());
    ::usleep (LONGTIME_USEC);
    EXPECT_FALSE(t2._exitted);
    EXPECT_FALSE(t3._exitted);
    EXPECT_FALSE(t4._exitted);
    EXPECT_FALSE(t5._exitted);
    EXPECT_FALSE(t6._exitted);
    EXPECT_FALSE(t7._exitted);
    EXPECT_FALSE(t8._exitted);

    W_DO(test_env->commit_xct());
    cout << "t2 join" << endl;
    W_DO(t2.join());
    cout << "t3 join" << endl;
    W_DO(t3.join());
    cout << "t4 join" << endl;
    W_DO(t4.join());
    cout << "t5 join" << endl;
    W_DO(t5.join());
    cout << "t6 join" << endl;
    W_DO(t6.join());
    cout << "t7 join" << endl;
    W_DO(t7.join());
    cout << "t8 join" << endl;
    W_DO(t8.join());
    cout << "joined all!" << endl;

    // now that we use RAW-style lock manager, we can't choose deadlock victim by any policy.
    // So, though t3 is younger, t2 might be the victim. both cases are possible.
    EXPECT_TRUE(t2._exitted);
    EXPECT_TRUE(t3._exitted);
    EXPECT_FALSE(t4._rc.is_error());
    EXPECT_FALSE(t5._rc.is_error());
    EXPECT_FALSE(t6._rc.is_error());
    EXPECT_FALSE(t7._rc.is_error());
    EXPECT_FALSE(t8._rc.is_error());
    EXPECT_TRUE(t2._done_multi[0]);
    EXPECT_TRUE(t2._done_multi[1]);
    EXPECT_TRUE(t2._done_multi[2]);
    EXPECT_TRUE(t3._done_multi[0]);
    EXPECT_TRUE(t3._done_multi[1]);
    EXPECT_TRUE(t3._done_multi[2]);
    if (!t2._rc.is_error()) {
        // t3 was victim
        EXPECT_EQ(t3._rc.err_num(), (w_error_codes) eDEADLOCK);
        EXPECT_TRUE(t2._done_multi[3]);
        EXPECT_FALSE(t3._done_multi[3]);
    } else if (!t3._rc.is_error()) {
        // t2 was victim
        EXPECT_TRUE(t2._rc.is_error());
        EXPECT_EQ(t2._rc.err_num(), (w_error_codes) eDEADLOCK);
        EXPECT_FALSE(t2._done_multi[3]);
        EXPECT_TRUE(t3._done_multi[3]);
    } else {
        // both were victims. because this involves multiple transactions, it's possible
        EXPECT_EQ(t2._rc.err_num(), (w_error_codes) eDEADLOCK);
        EXPECT_EQ(t3._rc.err_num(), (w_error_codes) eDEADLOCK);
        EXPECT_FALSE(t2._done_multi[3]);
        EXPECT_FALSE(t3._done_multi[3]);
    }
    return RCOK;
}

TEST (LockOkvlTest, Complex2Deadlock) {
    if (is_too_small_okvl()) {
        return;
    }
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(complex2_deadlock, true, locktable_size), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
