#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "xct.h"
#include <sys/time.h>

btree_test_env *test_env;

/**
 * Testcases for Early Lock Release.
 */

w_rc_t read_single(ss_m* ssm, test_volume_t *test_volume) {
    EXPECT_TRUE(test_env->_use_locks);
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));

    W_DO(test_env->begin_xct());
    g_xct()->set_elr_mode(xct_t::elr_none);
    EXPECT_EQ (xct_t::elr_none, g_xct()->get_elr_mode());
    W_DO(test_env->btree_insert(stid, "a1", "data"));
    W_DO(test_env->commit_xct());

    W_DO(test_env->begin_xct());
    g_xct()->set_elr_mode(xct_t::elr_s);
    EXPECT_EQ (xct_t::elr_s, g_xct()->get_elr_mode());
    W_DO(test_env->btree_insert(stid, "a2", "data"));
    W_DO(test_env->commit_xct());

    W_DO(test_env->begin_xct());
    g_xct()->set_elr_mode(xct_t::elr_sx);
    EXPECT_EQ (xct_t::elr_sx, g_xct()->get_elr_mode());
    W_DO(test_env->btree_insert(stid, "a3", "data"));
    W_DO(test_env->commit_xct());

    W_DO(test_env->begin_xct());
    g_xct()->set_elr_mode(xct_t::elr_clv);
    EXPECT_EQ (xct_t::elr_clv, g_xct()->get_elr_mode());
    W_DO(test_env->btree_insert(stid, "a4", "data"));
    W_DO(test_env->commit_xct());

    {
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(stid, s));
        EXPECT_EQ (4, s.rownum);
        EXPECT_EQ (std::string("a1"), s.minkey);
        EXPECT_EQ (std::string("a4"), s.maxkey);
    }

    std::string data;

    W_DO(test_env->begin_xct());
    g_xct()->set_elr_mode(xct_t::elr_none);
    W_DO(test_env->btree_lookup(stid, "a1", data));
    W_DO(test_env->commit_xct());

    W_DO(test_env->begin_xct());
    g_xct()->set_elr_mode(xct_t::elr_s);
    W_DO(test_env->btree_lookup(stid, "a2", data));
    W_DO(test_env->commit_xct());

    W_DO(test_env->begin_xct());
    g_xct()->set_elr_mode(xct_t::elr_sx);
    W_DO(test_env->btree_lookup(stid, "a3", data));
    W_DO(test_env->commit_xct());

    W_DO(test_env->begin_xct());
    g_xct()->set_elr_mode(xct_t::elr_clv);
    W_DO(test_env->btree_lookup(stid, "a4", data));
    W_DO(test_env->commit_xct());


    {
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(stid, s));
        EXPECT_EQ (4, s.rownum);
        EXPECT_EQ (std::string("a1"), s.minkey);
        EXPECT_EQ (std::string("a4"), s.maxkey);
    }

    return RCOK;
}

TEST (ElrTest, ReadSingle) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(read_single, true), 0);
}

rc_t _prep(ss_m* ssm, test_volume_t *test_volume, StoreID &stid) {
    EXPECT_TRUE(test_env->_use_locks);
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, test_volume, stid, root_pid));
    W_DO(test_env->begin_xct());
    W_DO(test_env->btree_insert(stid, "a1", "data1"));
    W_DO(test_env->btree_insert(stid, "a2", "data2"));
    W_DO(test_env->btree_insert(stid, "a3", "data3"));
    W_DO(test_env->commit_xct());
    return RCOK;
}

w_rc_t read_write_single(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));
    // this testcase isn't multi-threaded. so tests are limited.
    // testcases below do more than this. (but longer code)

    std::string data;
    const xct_t::elr_mode_t modes[4] = {xct_t::elr_none, xct_t::elr_s, xct_t::elr_sx, xct_t::elr_clv};
    for (int i = 0; i < 4; ++i) {
        xct_t::elr_mode_t mode = modes[i];

        W_DO(test_env->begin_xct());
        g_xct()->set_elr_mode(mode);
        W_DO(test_env->btree_insert(stid, "a4", "data4"));
        W_DO(test_env->btree_lookup(stid, "a2", data));
        W_DO(test_env->btree_remove(stid, "a4"));
        W_DO(test_env->commit_xct());
    }


    {
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(stid, s));
        EXPECT_EQ (3, s.rownum);
        EXPECT_EQ (std::string("a1"), s.minkey);
        EXPECT_EQ (std::string("a3"), s.maxkey);
    }

    return RCOK;
}

TEST (ElrTest, ReadWriteSingle) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(read_write_single, true), 0);
}

// global variable used in following testcases
xct_t::elr_mode_t s_elr_mode = xct_t::elr_none;

class lookup_thread_t : public smthread_t {
public:
        lookup_thread_t(StoreID stid, const char* key)
                : smthread_t(t_regular, "lookup_thread_t"),
                _stid(stid), _key(key), _read(false), _exitted(false) {}
        ~lookup_thread_t()  {}
        void run() {
            ::gettimeofday(&_start,NULL);
            w_keystr_t key;
            key.construct_regularkey(_key, ::strlen(_key));
            rc_t rc;
            rc = ss_m::begin_xct();
            EXPECT_FALSE(rc.is_error()) << rc;
            g_xct()->set_elr_mode(s_elr_mode);
            g_xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);

            report_time();
            std::cout << ":thread:looking up " << _key << ".." << std::endl;
            char data[20];
            smsize_t datalen = 20;
            bool found;
            rc = ss_m::find_assoc(_stid, key, data, datalen, found);
            EXPECT_FALSE(rc.is_error()) << rc;
            EXPECT_TRUE (found);
            report_time();
            std::cout << ":thread:found " << _key << ". committing..." << std::endl;
            _read = true;

            rc = ss_m::commit_xct();
            report_time();
            std::cout << ":thread:committed after seeing " << _key << "." << std::endl;
            EXPECT_FALSE(rc.is_error()) << rc;
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
    bool _read;
    bool _exitted;
    timeval _start;
};

// In some OS, usleep() has very low accuracy, so we can't rely on it in this testcase.
// we simply spin and keep checking time
void spin_sleep (int usec) {
    cout << "sleep " << usec << " usec... ";
    timeval start, now, result;
    ::gettimeofday(&start,NULL);
    while (true) {
        ::gettimeofday(&now,NULL);
        timersub(&now, &start, &result);
        int elapsed = (result.tv_sec * 1000000 + result.tv_usec);
        if (elapsed > usec) {
            cout << "slept (elapsed=" << elapsed << " usec)." << endl;
            return;
        }
    }
}

// no ELR, same-tuple case
w_rc_t read_write_multi_no_elr_same(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));

    // xct1: insert a tuple to root page
    W_DO(test_env->begin_xct());
    g_xct()->set_elr_mode(s_elr_mode);
    W_DO(test_env->btree_insert(stid, "a4", "data"));

    // xct2: read it
    lookup_thread_t lookup_thread (stid, "a4");
    EXPECT_FALSE(lookup_thread._read);
    EXPECT_FALSE(lookup_thread._exitted);
    W_DO(lookup_thread.fork());
    spin_sleep(ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC * 10); // should be enough

    // now xct2 should be blocking on a4
    EXPECT_FALSE(lookup_thread._read);
    EXPECT_FALSE(lookup_thread._exitted);

    W_DO(ss_m::chain_xct(true));
    spin_sleep(ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC * 10);

    EXPECT_FALSE(lookup_thread._read);
    EXPECT_FALSE(lookup_thread._exitted);

    W_DO(test_env->commit_xct());
    spin_sleep(ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC * 10);

    EXPECT_TRUE(lookup_thread._read);
    EXPECT_TRUE(lookup_thread._exitted);

    W_DO(lookup_thread.join());

    return RCOK;
}

TEST (ElrTest, ReadWriteMultiNoELRSame) {
    s_elr_mode = xct_t::elr_none;
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(read_write_multi_no_elr_same, true), 0);
}

TEST (ElrTest, ReadWriteMultiSOnlyELRSame) {
    // S-only ELR behaves same in this test
    s_elr_mode = xct_t::elr_s;
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(read_write_multi_no_elr_same, true), 0);
}

// SX and CLV ELR, same-tuple case. this behaves differently
w_rc_t read_write_multi_sx_elr_same(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));

    // xct1: insert a tuple to root page
    W_DO(test_env->begin_xct());
    g_xct()->set_elr_mode(s_elr_mode);
    W_DO(test_env->btree_insert(stid, "a4", "data"));

    // xct2: read it
    lookup_thread_t lookup_thread (stid, "a4");
    EXPECT_FALSE(lookup_thread._read);
    EXPECT_FALSE(lookup_thread._exitted);
    W_DO(lookup_thread.fork());
    spin_sleep(ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC * 10);

    // now xct2 should be blocking on a4
    EXPECT_FALSE(lookup_thread._read);
    EXPECT_FALSE(lookup_thread._exitted);

    // now, let's commit xct1. because of ELR on the X lock or CLV,
    // xct2 should read the tuple soon.
    W_DO(ss_m::chain_xct(true)); // here, we asynchronously commit by chaining

    // THIS DOESN'T WORK RELIABLY
    // this test (below) sometimes fails because background flushing might luckily (unluckily)
    // flushed it right after chain_xct()!
    // very annoying, but we can't test the exact behavior in that detail.
#if 0
    spin_sleep(ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC / 10);
    EXPECT_TRUE(lookup_thread._read);
    //// however, on committing the xct, xct2 should be waiting
    //// for watermark, so they are not yet done
    EXPECT_FALSE(lookup_thread._exitted);
#endif

    // by waiting a bit, xct2 will give up and writes its own
    // xct_end record and flush, so they will be done.
    spin_sleep(ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC * 10); // wait long enough so that xct2 surely flushes and exits
    EXPECT_TRUE(lookup_thread._read);
    EXPECT_TRUE(lookup_thread._exitted);
    // xct2 is ended even before xct1 ends!
    W_DO(ss_m::commit_xct(true));

    W_DO(lookup_thread.join());

    return RCOK;
}
TEST (ElrTest, ReadWriteMultiSXELRSame) {
    s_elr_mode = xct_t::elr_sx;
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(read_write_multi_sx_elr_same, true), 0);
}
TEST (ElrTest, ReadWriteMultiCLVELRSame) {
    s_elr_mode = xct_t::elr_clv;
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(read_write_multi_sx_elr_same, true), 0);
}

// no ELR, different-tuple case
w_rc_t read_write_multi_no_elr_different(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));

    // xct1: insert a tuple to root page
    W_DO(test_env->begin_xct());
    g_xct()->set_elr_mode(s_elr_mode);
    W_DO(test_env->btree_insert(stid, "a5", "data"));

    // xct2: read irrelevant tuple, but in same page
    lookup_thread_t lookup_thread (stid, "a2");
    EXPECT_FALSE(lookup_thread._read);
    EXPECT_FALSE(lookup_thread._exitted);
    W_DO(lookup_thread.fork());

    spin_sleep(ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC * 5); // before SX/CLV-ELR times out...

    // It should be already over -- no waiting for watermarks
    EXPECT_TRUE(lookup_thread._read);
    EXPECT_TRUE(lookup_thread._exitted);

    W_DO(test_env->commit_xct());

    W_DO(lookup_thread.join());

    return RCOK;
}

TEST (ElrTest, ReadWriteMultiNoELRDifferent) {
    s_elr_mode = xct_t::elr_none;
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(read_write_multi_no_elr_different, true), 0);
}

TEST (ElrTest, ReadWriteMultiSOnlyELRDifferent) {
    // S-only ELR behaves same in this test
    s_elr_mode = xct_t::elr_s;
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(read_write_multi_no_elr_different, true), 0);
}

// SX & CLV ELR, different-tuple case. VERY different from others
w_rc_t read_write_multi_sx_elr_different(ss_m* ssm, test_volume_t *test_volume) {
    StoreID stid;
    W_DO(_prep (ssm, test_volume, stid));

    // xct1: insert a tuple to root page
    W_DO(test_env->begin_xct());
    g_xct()->set_elr_mode(s_elr_mode);
    W_DO(test_env->btree_insert(stid, "a4", "data"));

    // xct2: read irrelevant tuple, but in same page
    lookup_thread_t lookup_thread (stid, "a2");
    EXPECT_FALSE(lookup_thread._read);
    EXPECT_FALSE(lookup_thread._exitted);
    W_DO(lookup_thread.fork());

    // THIS DOESN'T WORK RELIABLY
    // this test (below) sometimes fails because background flushing might luckily (unluckily)
    // flushed it right after chain_xct()!
    // very annoying, but we can't test the exact behavior in that detail.
#if 0
//    spin_sleep(ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC / 10);
    spin_sleep(ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC / 2);
    EXPECT_TRUE(lookup_thread._read);
    //// unlike others, the readonly xct should be waiting for watermark
    EXPECT_FALSE(lookup_thread._exitted);
#endif

    // after waiting long enough, they will give up and logs its own one
    spin_sleep(ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC * 10);
    EXPECT_TRUE(lookup_thread._read);
    EXPECT_TRUE(lookup_thread._exitted);

    W_DO(test_env->commit_xct());

    W_DO(lookup_thread.join());

    return RCOK;
}
TEST (ElrTest, ReadWriteMultiSXELRDifferent) {
    s_elr_mode = xct_t::elr_sx;
    test_env->empty_logdata_dir();
    // EXPECT_EQ(test_env->runBtreeTest(read_write_multi_sx_elr_different, true), 0);
    // 2 different couples in same page do not cause lock conflict!
    EXPECT_EQ(test_env->runBtreeTest(read_write_multi_no_elr_different, true), 0);
}
TEST (ElrTest, ReadWriteMultiCLVELRDifferent) {
    s_elr_mode = xct_t::elr_clv;
    test_env->empty_logdata_dir();
    // EXPECT_EQ(test_env->runBtreeTest(read_write_multi_sx_elr_different, true), 0);
    // 2 different couples in same page do not cause lock conflict!
    EXPECT_EQ(test_env->runBtreeTest(read_write_multi_no_elr_different, true), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
