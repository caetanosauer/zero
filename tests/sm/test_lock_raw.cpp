#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sthread.h"
#include "sm_options.h"
#include "sthread.h"
#include "lock_raw.h"
#include "lock_core.h"
#include "sm_base.h"
#include "log_core.h"
#include "log_lsn_tracker.h"
#include "lock.h"
#include "w_okvl_inl.h"
#include "w_endian.h"
#include "../common/local_random.h"

sm_options make_options(bool has_init = true, bool small = true) {
    sm_options options;
    options.set_int_option("sm_locktablesize", small ? 100 : 6400);
    options.set_bool_option("sm_truncate", true);
    options.set_bool_option("sm_testenv_init_vol", true);
    options.set_int_option("sm_rawlock_lockpool_initseg", has_init ? 2 : 0);
    options.set_int_option("sm_rawlock_xctpool_initseg", has_init ? 2 : 0);
    options.set_int_option("sm_rawlock_lockpool_segsize", small ? 1 << 3 : 1 << 5);
    options.set_int_option("sm_rawlock_xctpool_segsize", small ? 1 << 2 : 1 << 4);
    options.set_int_option("sm_rawlock_gc_interval_ms", 50);
    options.set_int_option("sm_rawlock_gc_generation_count", 4);
    options.set_int_option("sm_rawlock_gc_free_segment_count", 2);
    options.set_int_option("sm_rawlock_gc_max_segment_count", 3);
    return options;
}

TEST (LockRawTest, Create) {
    lock_core_m core(make_options());
}
TEST (LockRawTest, CreateNoInit) {
    lock_core_m core(make_options(false));
}
TEST (LockRawTest, CreateLarge) {
    lock_core_m core(make_options(false, false));
}

TEST (LockRawTest, InvokeGc) {
    lock_core_m core(make_options(false, true));
    RawXct *xct = core.allocate_xct();
    // allocate many so that GC allocation and garbage collection kicks in
    for (int i = 0; i < 50; ++i) {
        RawLock *lock = NULL;
        EXPECT_EQ(w_error_ok, core.acquire_lock(xct, 123, ALL_S_GAP_S,
                                                true, true, true, 100, &lock));
        EXPECT_TRUE(lock != NULL);
        core.release_lock(lock);
    }
    core.deallocate_xct(xct);
}

sm_options make_options_huge(bool catchup) {
    sm_options options;
    options.set_int_option("sm_locktablesize", 1 << 8); // small so that more races happen
    options.set_bool_option("sm_truncate", true);
    options.set_bool_option("sm_testenv_init_vol", true);
    options.set_int_option("sm_rawlock_lockpool_initseg", catchup ? 2 : 20);
    options.set_int_option("sm_rawlock_xctpool_initseg", catchup ? 2 : 20);
    options.set_int_option("sm_rawlock_lockpool_segsize", catchup ? 1 << 10 : 1 << 12);
    options.set_int_option("sm_rawlock_xctpool_segsize", catchup ? 1 << 6 : 1 << 8);
    options.set_int_option("sm_rawlock_gc_interval_ms", catchup ? 100 : 1000);
    options.set_int_option("sm_rawlock_gc_generation_count", catchup ? 10 : 50);
    options.set_int_option("sm_rawlock_gc_free_segment_count", 20);
    options.set_int_option("sm_rawlock_gc_max_segment_count", catchup ? 50 : 200);
    return options;
}

const int THREAD_COUNT = 6;
const int LOCK_COUNT = 10;
const int REP_COUNT = 10000;

struct TestSharedContext {
    TestSharedContext(lock_core_m &core_arg, okvl_mode mode_arg)
        : core(&core_arg), mode(mode_arg) {}
    lock_core_m*    core;
    okvl_mode       mode;
};

struct TestThreadContext {
    int id;
    TestSharedContext *shared;
};

void *test_parallel_standalone_worker(void *t) {
    TestThreadContext &context = *reinterpret_cast<TestThreadContext*>(t);
    TestSharedContext &shared = *context.shared;
    tlr_t rand (context.id);
    std::cout << "Worker-" << context.id << " started" << std::endl;
    for (int i = 0; i < REP_COUNT; ++i) {
        RawXct* xct = shared.core->allocate_xct();
        RawLock *out[LOCK_COUNT];
        for (int j = 0; j < LOCK_COUNT; ++j) {
            uint32_t hash = rand.nextInt32();
            EXPECT_EQ(w_error_ok, shared.core->acquire_lock(
                xct, hash, shared.mode, true, true, true, 100, out + j));
            EXPECT_TRUE(out[j] != NULL);
        }
        for (int j = 0; j < LOCK_COUNT; ++j) {
            shared.core->release_lock(out[j]);
        }
        shared.core->deallocate_xct(xct);
    }
    std::cout << "done:" << context.id << std::endl;
    ::pthread_exit(NULL);
    return NULL;
}

void test_parallel_standalone(okvl_mode mode, bool catchup) {
    // this one doesn't involve entire engine. only lock_core
    lock_core_m core(make_options_huge(catchup));

    TestSharedContext shared(core, mode);

    pthread_attr_t join_attr;
    void *join_status;
    ::pthread_attr_init(&join_attr);
    ::pthread_attr_setdetachstate(&join_attr, PTHREAD_CREATE_JOINABLE);

    TestThreadContext contexts[THREAD_COUNT];
    pthread_t *threads = new pthread_t[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; ++i) {
        contexts[i].id = i;
        contexts[i].shared = &shared;
        int rc = ::pthread_create(threads + i, &join_attr,
                                  test_parallel_standalone_worker, contexts + i);
        EXPECT_EQ(0, rc) << "pthread_create failed";
    }

    for (int i = 0; i < THREAD_COUNT; ++i) {
        int rc = ::pthread_join(threads[i], &join_status);
        EXPECT_EQ(0, rc) << "pthread_join failed";
    }

    ::pthread_attr_destroy(&join_attr);
    delete[] threads;
}
TEST (LockRawTest, ParallelStandaloneRead) {
    test_parallel_standalone(ALL_S_GAP_S, false);
}
TEST (LockRawTest, ParallelStandaloneWrite) {
    test_parallel_standalone(ALL_X_GAP_X, false);
}
TEST (LockRawTest, ParallelStandaloneReadCatchup) {
    test_parallel_standalone(ALL_S_GAP_S, true);
}
TEST (LockRawTest, ParallelStandaloneWriteCatchup) {
    test_parallel_standalone(ALL_X_GAP_X, true);
}

btree_test_env *test_env;
int next_thid = 0;
class lock_worker_t : public smthread_t {
public:
    lock_worker_t() : smthread_t(t_regular, "lock_worker_t"), _running(true) {
        _thid = next_thid++;
    }
    virtual void run() {
        tlr_t rand (_thid);
        std::cout << "Worker-" << _thid << " started" << std::endl;
        for (int i = 0; i < REP_COUNT; ++i) {
            _rc = test_env->begin_xct();
            EXPECT_FALSE(_rc.is_error());
            RawLock *out[LOCK_COUNT];
            for (int j = 0; j < LOCK_COUNT; ++j) {
                lockid_t lockid; // internally 128 bit int
                uint32_t* hack = reinterpret_cast<uint32_t*>(&lockid);
                hack[0] = rand.nextInt32();
                hack[1] = rand.nextInt32();
                hack[2] = rand.nextInt32();
                hack[3] = rand.nextInt32();
                out[j] = NULL;
                _rc = smlevel_0::lm->lock(lockid.hash(), ALL_S_GAP_S, true, true, true, g_xct(), 100, out + j);
                EXPECT_FALSE(_rc.is_error());
                EXPECT_TRUE(out[j] != NULL);
            }
            _rc = test_env->commit_xct();
            EXPECT_FALSE(_rc.is_error());
        }
        _running = false;
        std::cout << "done:" << _thid << std::endl;
    }
    int  return_value() const { return 0; }
    rc_t _rc;
    bool _running;
    int _thid;
};

w_rc_t parallel_locks(ss_m*, test_volume_t *) {
    lock_worker_t workers[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; ++i) {
        W_DO(workers[i].fork());
    }

    for (int i = 0; i < THREAD_COUNT; ++i) {
        W_DO(workers[i].join());
        EXPECT_FALSE(workers[i]._running) << i;
        EXPECT_FALSE(workers[i]._rc.is_error()) << i;
    }
    return RCOK;
}

TEST (LockRawTest, ParallelSsmInvolved) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(parallel_locks, true, make_options_huge(false)), 0);
}
TEST (LockRawTest, ParallelSsmInvolvedCatchup) {
    test_env->empty_logdata_dir();
    EXPECT_EQ(test_env->runBtreeTest(parallel_locks, true, make_options_huge(true)), 0);
}


uint64_t next_history = 0;
#if W_DEBUG_LEVEL>0
const int APPEND_COUNT = 1000;
#else // W_DEBUG_LEVEL>0
const int APPEND_COUNT = 10000;
#endif // W_DEBUG_LEVEL>0

class append_worker_t : public smthread_t {
public:
    append_worker_t()
        : smthread_t(t_regular, "append_worker_t"), _running(true) {
        _thid = next_thid++;
    }
    virtual void run() {
        tlr_t rand (_thid);
        std::cout << "Worker-" << _thid << " started" << std::endl;
        _rc = x_begin_xct(ssm, true);
        EXPECT_FALSE(_rc.is_error());
        w_keystr_t key;
        uint64_t key_int = 0;
        char key_be[8];

        char data[1]; // whatever
        data[0] = 0;
        vec_t vec(data, 1);

        for (int i = 0; i < APPEND_COUNT; ++i) {
            if (i % 1000 == 0) {
                std::cout << "Worker-" << _thid << " " << i << "/" << APPEND_COUNT << std::endl;
            }
            if (i % 10 == 0) {
                if (i % 100 == 0) {
                    _rc = ssm->abort_xct();
                    EXPECT_FALSE(_rc.is_error());
                } else {
                    _rc = ssm->commit_xct(true);
                    EXPECT_FALSE(_rc.is_error());
                }

                _rc = x_begin_xct(ssm, true);
                EXPECT_FALSE(_rc.is_error());
            }
            key_int = lintel::unsafe::atomic_fetch_add<uint64_t>(&next_history, 1);
            serialize64_be(key_be, key_int);
            key.construct_regularkey(key_be, sizeof(key_int));
            _rc = ssm->create_assoc(stid, key, vec);
            EXPECT_FALSE(_rc.is_error()) << "key=" << key_int << ": rc=" << _rc;
        }
        _rc = x_commit_xct(ssm);
        EXPECT_FALSE(_rc.is_error());
        _running = false;
        std::cout << "done:" << _thid << std::endl;
    }
    int  return_value() const { return 0; }
    rc_t _rc;
    bool _running;
    int _thid;
    ss_m *ssm;
    test_volume_t *volume;
    StoreID stid;
    PageID root_pid;
};

w_rc_t parallel_appends(ss_m *ssm, test_volume_t *volume) {
    StoreID stid;
    PageID root_pid;
    W_DO(x_btree_create_index(ssm, volume, stid, root_pid));

    append_worker_t workers[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; ++i) {
        workers[i].ssm = ssm;
        workers[i].volume = volume;
        workers[i].stid = stid;
        workers[i].root_pid = root_pid;
        W_DO(workers[i].fork());
    }

    for (int i = 0; i < THREAD_COUNT; ++i) {
        W_DO(workers[i].join());
        EXPECT_FALSE(workers[i]._running) << i;
        EXPECT_FALSE(workers[i]._rc.is_error()) << i;
    }
    return RCOK;
}
TEST (LockRawTest, ParallelAppends) {
    test_env->empty_logdata_dir();
    sm_options options = make_options_huge(false);
    options.set_int_option("sm_logbufsize", 128 << 10);
    options.set_int_option("sm_logsize", 8192 << 10);
    EXPECT_EQ(test_env->runBtreeTest(parallel_appends, true, options), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
