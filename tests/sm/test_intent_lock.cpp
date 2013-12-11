#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"
#include "xct.h"
#include "w_gettimeofday.h"
#include "lock.h"

btree_test_env *test_env;

/**
 * Testcases for intent lock.
 */
int next_thid = 10;
int locktable_size = 1 << 12;
const int SHORTTIME_USEC = 500; // only a short sleep
const int LONGTIME_USEC = 20000; // like forever in the tests.

const int TEST_VOL_ID = 1;
const int TEST_STORE_ID = 2;
const int TEST_STORE_ID2 = 3;
const int TEST_STORE_ID3 = 4;

class lock_thread_t : public smthread_t {
public:
        lock_thread_t(vid_t vol, snum_t store, lock_mode_t vol_mode, lock_mode_t store_mode) 
                : smthread_t(t_regular, "lock_thread_t"),
                _vol(vol), _store(store), _vol_mode(vol_mode), _store_mode(store_mode),
                _done(false), _exitted(false) {
            _thid = next_thid++;
        }
        ~lock_thread_t()  {}
        virtual void run() {
            _begin();
            std::cout << ":T" << _thid << ": req vol=" << _vol << ", mode=" << _vol_mode << ".." << std::endl;
            for (int retries = 100; retries>0; retries--) {
                _rc = ss_m::lm->intent_vol_lock(_vol, _vol_mode);
                if (_rc.is_error() && _rc.err_num() != smlevel_0::eDEADLOCK)
                    break;
            }
            if (_rc.is_error()) {
                report_time();
                std::cout << ":T" << _thid << ":" << "received an error " << _rc << ". abort." << std::endl;
                rc_t abort_rc = ss_m::abort_xct();
                report_time();
                std::cout << ":T" << _thid << ": aborted. rc=" << abort_rc << std::endl;
                _exitted = true;
                return;
            }
            report_time();
            std::cout << ":T" << _thid << ": got vol=" << _vol << ", mode=" << _vol_mode << "." << std::endl;
            if (_store != 0) {
                report_time();
                std::cout << ":T" << _thid << ": req store=" << _store << ", mode=" << _store_mode << ".." << std::endl;
                stid_t stid (_vol, _store);
                _rc = ss_m::lm->intent_store_lock(stid, _store_mode);
                report_time();
                if (_rc.is_error()) {
                    report_time();
                    std::cout << ":T" << _thid << ":" << "received an error " << _rc << ". abort." << std::endl;
                    rc_t abort_rc = ss_m::abort_xct();
                    report_time();
                    std::cout << ":T" << _thid << ": aborted. rc=" << abort_rc << std::endl;
                    _exitted = true;
                    return;
                }
                std::cout << ":T" << _thid << ": got store=" << _store << ", mode=" << _store_mode << "." << std::endl;
            }
            std::cout << ":T" << _thid << ":done. committing..." << std::endl;
            _done = true;

            _commit ();
        }
        void _begin() {
            ::gettimeofday(&_start,NULL);
            _rc = ss_m::begin_xct();
            EXPECT_FALSE(_rc.is_error()) << _rc;
            g_xct()->set_query_concurrency(smlevel_0::t_cc_keyrange);            
            report_time();
            std::cout << ":T" << _thid << " begins." << std::endl;
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

    vid_t _vol;
    snum_t _store;
    lock_mode_t _vol_mode, _store_mode;
    rc_t _rc;
    bool _done;
    bool _exitted;
    timeval _start;
    int _thid;
};

w_rc_t read_write_livelock(ss_m*, test_volume_t *) {
    EXPECT_TRUE(test_env->_use_locks);

    W_DO(test_env->begin_xct());
    W_DO(ss_m::lm->intent_vol_lock(TEST_VOL_ID, EX));

    // write a2
    lock_thread_t t2 (TEST_VOL_ID, 0, SH, SH);
    W_DO(t2.fork());
    ::usleep (SHORTTIME_USEC);
    EXPECT_FALSE(t2._done);
    EXPECT_FALSE(t2._exitted);
    
    W_DO(test_env->commit_xct());
    W_DO(t2.join());
    EXPECT_TRUE(t2._done);
    EXPECT_TRUE(t2._exitted);
    
    return RCOK;
}

TEST (IntentLockTest, ReadWriteLivelock) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(read_write_livelock, true, locktable_size), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
