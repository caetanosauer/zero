/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

// modified from test_lock_raw.cpp
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

btree_test_env *test_env;

// basic parameters
const int THREAD_COUNT = 5;

const int REC_COUNT = 1000;  // total number of records inserted per thread

const int SEG_SIZE = 1024*1024; // _segsize 1MB
const int LOG_SIZE =  1024*8*1024; // log size 8G, so every partition is about 1G


const int BUF_SIZE = 4096*5;
char buf[BUF_SIZE];

void itoa(int i, char *buf, int base) {
    // ignoring the base
    if(base)
        sprintf(buf, "%d", i);
}

// insert and flush a log record of size bytes
// use this function to stress the log buffer
rc_t consume(int size, ss_m *ssm) {
    if (size>BUF_SIZE) {
        return RC(fcINTERNAL);
    }

    int buf_size = (size - 48 - 48 - 56) + 1;
    memset(buf, 'z', buf_size-1);
    buf[buf_size] = '\0';

    W_DO(x_begin_xct(ssm, true));
    W_DO(ssm->log_message(buf));
    W_DO(x_commit_xct(ssm));

    return RCOK;
}

int next_thid = 0;
uint64_t next_history = 1; // key values start from 1


const int data_size = 64;
const int key_size = 64;

class op_worker_t : public smthread_t {
public:
    static const int total = REC_COUNT; // number of records inserted per thread
    char buf[data_size];
    char key[key_size];

    op_worker_t()
        : smthread_t(t_regular, "op_worker_t"), _running(true) {
        _thid = next_thid++;
    }
    virtual void run() {
        _rc = run_core();
        _running = false;
    }
    rc_t run_core() {
        tlr_t rand (_thid);
        std::cout << "Worker-" << _thid << " started" << std::endl;

        uint64_t key_int = 0;

        uint64_t *keys = new uint64_t[total];  // store the keys 

        // insert
        for (int i=1; i<=total; i++) {
            key_int = lintel::unsafe::atomic_fetch_add<uint64_t>(&next_history, 1);
            keys[i-1] = key_int;
            itoa(key_int, key, 10);
            itoa(key_int, buf, 10);

            //W_DO(consume(4096,ssm));

            W_DO(test_env->begin_xct());
            W_DO(test_env->btree_insert(stid, key, buf));

            // abort if the key ends with a 9
            if (key[strlen(key)-1]=='9') {
                W_DO(test_env->abort_xct());
            }
            else {
                W_DO(test_env->commit_xct());
            }
        }

        for (int i=1; i<=total; i++) {
            key_int = keys[i-1];
            itoa(key_int, key, 10);

            //W_DO(consume(4096,ssm));

            W_DO(test_env->begin_xct());        
            if(key_int%1000==0) {
                // overwrite records whose key % 1000 == 0
                buf[0]='z';
                buf[1]='\0';
                W_DO(test_env->btree_overwrite(stid, key, buf, strlen(key)-1));
                // abort if the second to last character is 9
                if (key[strlen(key)-2]=='9') {
                    W_DO(test_env->abort_xct());
                }
                else {
                    W_DO(test_env->commit_xct());
                }
            }
            else {
                // update records whose key % 100 == 0 && key % 1000 != 0
                if(key_int%100==0) {
                    itoa(key_int+1, buf, 10);
                    W_DO(test_env->btree_update(stid, key, buf));
                    // abort if the second to last character is 9
                    if (key[strlen(key)-2]=='9') {
                        W_DO(test_env->abort_xct());
                    }
                    else {
                        W_DO(test_env->commit_xct());
                    }
                }
                else {
                    // remove records whose key % 10 == 0 && key % 100 !=0 && key % 1000 != 0
                    if(key_int%10==0) {
                        W_DO(test_env->btree_remove(stid, key));
                        // abort if the second to last character is 9
                        if (key[strlen(key)-2]=='9') {
                            W_DO(test_env->abort_xct());
                        }
                        else {
                            W_DO(test_env->commit_xct());
                        }
                    }
                    else {
                        W_DO(test_env->commit_xct());
                    }
                }
            }
        }


        delete []keys;

            
        std::cout << "Worker-" << _thid << " done" << std::endl;
        
        return RCOK;
    }

    rc_t _rc;
    bool _running;
    int _thid;
    ss_m *ssm;
    stid_t stid;
};



w_rc_t parallel_ops(ss_m *ssm, test_volume_t *volume) {
    stid_t stid;
    lpid_t root_pid;
    W_DO(x_btree_create_index(ssm, volume, stid, root_pid));

    // run workload
    op_worker_t workers[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; ++i) {
        workers[i].ssm = ssm;
        workers[i].stid = stid;
        W_DO(workers[i].fork());
    }

    for (int i = 0; i < THREAD_COUNT; ++i) {
        W_DO(workers[i].join());
        EXPECT_FALSE(workers[i]._running) << i;
        EXPECT_FALSE(workers[i]._rc.is_error()) << i;
    }


    // verify results

    char buf[data_size];
    char key[key_size];

    x_btree_scan_result s;
    W_DO(test_env->btree_scan(stid, s));


    int total = REC_COUNT*THREAD_COUNT;
    EXPECT_EQ (total - total/10 - total/100*8, s.rownum);

    for (int i=1; i<=total; i++) {
        std::string result="";
        itoa(i, key, 10);
        if(i%1000==0) {
            // overwrite records whose key % 1000 == 0
            test_env->btree_lookup_and_commit(stid, key, result);
            // abort if the second to last character is 9
            if (key[strlen(key)-2]=='9') {
                EXPECT_EQ(std::string(key), result);                    
            }
            else {
                EXPECT_EQ('z', result[result.length()-1]);
                result[result.length()-1]='0';
                EXPECT_EQ(std::string(key), result);
            }
        }
        else {
            if(i%100==0) {
                // update records whose key % 100 == 0 && key % 1000 != 0
                test_env->btree_lookup_and_commit(stid, key, result);
                // abort if the second to last character is 9
                if (key[strlen(key)-2]=='9') {
                    EXPECT_EQ(std::string(key), result);                    
                }
                else {
                    itoa(i+1, buf, 10);
                    EXPECT_EQ(std::string(buf), result);
                }
            }
            else {
                if(i%10==0) {
                    // remove records whose key % 10 == 0 && key % 100 !=0 && key % 1000 != 0
                    // total/100*9 records are removed
                    result="data";
                    test_env->btree_lookup_and_commit(stid, key, result);
                    if (key[strlen(key)-2]=='9') {
                        EXPECT_EQ(std::string(key), result);                    
                    }
                    else {
                        // if not found, the result would become empty
                        EXPECT_EQ(true, result.empty());
                    }
                }
                else {
                    // non modified records
                    result="data";
                    test_env->btree_lookup_and_commit(stid, key, result);
                    if (key[strlen(key)-1]=='9') {
                        // if not found, the result would become empty
                        EXPECT_EQ(true, result.empty());
                    }
                    else {
                        EXPECT_EQ(std::string(key), result);
                    }
                }
            }
        }
    }
        


    return RCOK;
}

TEST (LogBufferTest, MultiThread) {
    test_env->empty_logdata_dir();
    sm_options options;
    options.set_int_option("sm_logbufsize", SEG_SIZE);
    options.set_int_option("sm_logsize", LOG_SIZE);
    EXPECT_EQ(test_env->runBtreeTest(parallel_ops, true, 1 << 16, options), 0);
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
