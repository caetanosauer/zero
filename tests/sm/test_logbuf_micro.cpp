/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

// lock_raw
#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sthread.h"
#include "sm_options.h"
#include "sthread.h"
#include "lock_raw.h"
#include "lock_core.h"
#include "sm_base.h"
#include "log.h"
#include "log_lsn_tracker.h"
#include "lock.h"
#include "w_okvl_inl.h"
#include "w_endian.h"
#include "../fc/local_random.h"
#include <sys/time.h>


#include "logbuf_common.h"
#include "logbuf_core.h"
#include "log_core.h"


btree_test_env *test_env;


const uint64_t SEG_SIZE = 1024*1024;
const uint64_t LOG_SIZE = 8*1024*1024;


sm_options make_options_huge(bool catchup) {
    sm_options options;
    options.set_int_option("sm_locktablesize", 1 << 8); // small so that more races happen
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

const int LOCK_COUNT = 10;
const int REP_COUNT = 10000;

int next_thid = 0;

uint64_t next_history = 0;



int THREAD_COUNT = 1;
int APPEND_COUNT = 4096*5;
//10000;
int COMMIT_SIZE = 1048576; 

// class append_worker_t : public smthread_t {
// public:
//     append_worker_t()
//         : smthread_t(t_regular, "append_worker_t"), _running(true) {
//         _thid = next_thid++;
//     }
//     virtual void run() {
//         tlr_t rand (_thid);
//         std::cout << "Worker-" << _thid << " started" << std::endl;
//         w_keystr_t key;

//         uint64_t key_int = 0;

//         char key_be[8];
        
//         const int BUF_SIZE = 4096;
//         char buf[BUF_SIZE];
//         memset(buf, 'z', BUF_SIZE);
//         buf[BUF_SIZE-1] = '\0';

//         int commit_interval = COMMIT_SIZE/BUF_SIZE;

//         char data[1]; // whatever
//         data[0] = 0;
//         vec_t vec(data, 1);


//         _rc = x_begin_xct(ssm, true);
//         EXPECT_FALSE(_rc.is_error());

//         for (int i = 0; i < APPEND_COUNT; ++i) {
//             if (i % 1000 == 0) {
//                 std::cout << "Worker-" << _thid << " " << i << "/" << APPEND_COUNT << std::endl;
//             }
//             if (i % commit_interval == 0) {
//                 key_int = lintel::unsafe::atomic_fetch_add<uint64_t>(&next_history, 1);
            
//                 serialize64_be(key_be, key_int);
//                 key.construct_regularkey(key_be, sizeof(key_int));

//                 _rc = ssm->create_assoc(stid, key, vec);
//                 EXPECT_FALSE(_rc.is_error()) << "key=" << key_int << ": rc=" << _rc;

//             }

//             // artificially increase log buffer usage...            
//             _rc = ssm->log_message(buf);
//             EXPECT_FALSE(_rc.is_error()) << "log_message failed?" << _rc;

//         }
//         // _rc = x_commit_xct(ssm);
//         // EXPECT_FALSE(_rc.is_error());


//         timeval start,stop,result;
//         ::gettimeofday(&start,NULL);
//         _rc = ssm->abort_xct();
//         EXPECT_FALSE(_rc.is_error());
//         ::gettimeofday(&stop,NULL);
//         timersub(&stop, &start,&result);

//         cout << "total elapsed time=" << (result.tv_sec + result.tv_usec/1000000.0) << " sec" << endl;
        
        

//         _running = false;
//         std::cout << "done:" << _thid << std::endl;
//     }
//     int  return_value() const { return 0; }
//     rc_t _rc;
//     bool _running;
//     int _thid;
//     ss_m *ssm;
//     test_volume_t *volume;
//     stid_t stid;
//     lpid_t root_pid;
// };

// w_rc_t parallel_appends(ss_m *ssm, test_volume_t *volume) {
//     stid_t stid;
//     lpid_t root_pid;
//     W_DO(x_btree_create_index(ssm, volume, stid, root_pid));

//     append_worker_t workers[THREAD_COUNT];
//     for (int i = 0; i < THREAD_COUNT; ++i) {
//         workers[i].ssm = ssm;
//         workers[i].volume = volume;
//         workers[i].stid = stid;
//         workers[i].root_pid = root_pid;
//         W_DO(workers[i].fork());
//     }

//     for (int i = 0; i < THREAD_COUNT; ++i) {
//         W_DO(workers[i].join());
//         EXPECT_FALSE(workers[i]._running) << i;
//         EXPECT_FALSE(workers[i]._rc.is_error()) << i;
//     }
//     return RCOK;
// }

// TEST (LbStressTest, ParallelAppends) {
//     test_env->empty_logdata_dir();
//     sm_options options = make_options_huge(false);
//     options.set_int_option("sm_logbufsize", 128 << 10);
//     options.set_int_option("sm_logsize", 8192 << 10);
//     EXPECT_EQ(test_env->runBtreeTest(parallel_appends, true, 1 << 16, options), 0);
// }


class append_worker_t : public smthread_t {
public:
    append_worker_t()
        : smthread_t(t_regular, "append_worker_t"), _running(true) {
        _thid = next_thid++;
    }
    virtual void run() {
        tlr_t rand (_thid);
        std::cout << "Worker-" << _thid << " started" << std::endl;
        w_keystr_t key;

        uint64_t key_int = 0;

        char key_be[8];
        
        const int BUF_SIZE = 4096*5;
        char buf[BUF_SIZE];
        memset(buf, 'z', BUF_SIZE);
        buf[BUF_SIZE-1] = '\0';

        int commit_interval = COMMIT_SIZE/BUF_SIZE;

        char data[1]; // whatever
        data[0] = 3;
        vec_t vec(data, 1);


        std::cout << "sizeof lsn_t " << sizeof(lsn_t) << std::endl;

        // _rc = x_begin_xct(ssm, true);
        // buf[APPEND_COUNT-i-1] = '\0';
        // std::cout << "size @ " << APPEND_COUNT-i << endl;
        // _rc = ssm->log_message(buf);
        // _rc = x_commit_xct(ssm);



//         _rc = x_begin_xct(ssm, true);
//         EXPECT_FALSE(_rc.is_error());

//         //        for (int i = 0; i < APPEND_COUNT; ++i) {
//             // key_int = lintel::unsafe::atomic_fetch_add<uint64_t>(&next_history, 1);
            
//             // serialize64_be(key_be, key_int);
//             // key.construct_regularkey(key_be, sizeof(key_int));
                
//             // _rc = ssm->create_assoc(stid, key, vec);
//             // EXPECT_FALSE(_rc.is_error()) << "key=" << key_int << ": rc=" << _rc;
//         int i=0;
//             buf[APPEND_COUNT-i-1] = '\0';
//             std::cout << "size @ " << APPEND_COUNT-i << endl;
//             _rc = ssm->log_message(buf);
//             EXPECT_FALSE(_rc.is_error()) << "log_message failed?" << _rc;

//             _rc = x_commit_xct(ssm);
//             _rc = x_begin_xct(ssm, true);

//             //}


// #ifdef LOG_BUFFER
//         logbuf_core *log_buffer = ((log_core*)ssm->log)->_log_buffer;
//         log_buffer->logbuf_print("WORKER");
// #endif

//         _rc = x_commit_xct(ssm);
//         //_rc = x_abort_xct(ssm);
//         EXPECT_FALSE(_rc.is_error());


        _running = false;
        std::cout << "Worker-" << _thid << " done" << std::endl;
  
    }
    int  return_value() const { return 0; }
    rc_t _rc;
    bool _running;
    int _thid;
    ss_m *ssm;
    test_volume_t *volume;
    stid_t stid;
    lpid_t root_pid;
};

w_rc_t parallel_appends(ss_m *ssm, test_volume_t *volume) {
    stid_t stid;
    lpid_t root_pid;
    //W_DO(x_btree_create_index(ssm, volume, stid, root_pid));

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

TEST (LbStressTest, ParallelAppends) {
    test_env->empty_logdata_dir();
    sm_options options = make_options_huge(false);
    options.set_int_option("sm_logbufsize", 128 << 10);
    options.set_int_option("sm_logsize", 8192 << 10);
    EXPECT_EQ(test_env->runBtreeTest(parallel_appends, true, 1 << 16, options), 0);
}



int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
