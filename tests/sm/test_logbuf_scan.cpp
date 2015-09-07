/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "xct.h"
#include "sm_base.h"
#include "sm_external.h"

#include "logbuf_common.h"
#include "logbuf_core.h"
#include "logbuf_seg.h"
#include "log_core.h"

#include "logrec.h"
#include "lsn.h"

#include "w_debug.h"

#include <pthread.h>
#include <memory.h>
#include <AtomicCounter.hpp>

#include "log_carray.h"

#include "log.h"


btree_test_env *test_env;


// some parameters for the new log buffer

const int SEG_SIZE = 1024*1024; // _segsize 1MB
const int PART_SIZE = 15*1024*1024; // _partition_data_size 15MB

const int LOG_SIZE =  16*1024*8; // so that _partition_data_size is 15MB 
                                        // (see log_core::_set_size)   

// some utility functions that are used by the following test cases



const int BUF_SIZE = 4096*5;
char buf[BUF_SIZE];


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


#ifdef LOG_BUFFER
// the forward scan handles a log record spanning two segments
class forward_scan1  : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((logbuf_core*)ssm->log);

        EXPECT_EQ(lsn_t(1,1696), log_buffer->_to_insert_lsn);    

        // consume 4096 bytes
        W_DO(consume(4096-1696,ssm));

        // consume the entire segment other than the last 4096 bytes
        for (int i=1; i<=(SEG_SIZE-4096)/4096-1; i++) {
            W_DO(consume(4096,ssm));            
        }

        // this log record spans two segment
        W_DO(consume(4096*2,ssm));            

        return RCOK;
    }

    // when starting from an non-empty log, there are several log records inserted during startup
    // mount/dismount, chkpt + one more async chkpt
    // 320 + 320 + (56 + 320 + 64 + 88)*2 = 1696
    w_rc_t post_shutdown(ss_m *) {


        // there is an async checkpoint going on, so let's wait a couple of seconds
        // TODO: how to avoid this sleep?
        sleep(5);

        log_i scan(*(smlevel_0::log), lsn_t(1,0), true);
        logrec_t log_rec_buf;
        lsn_t lsn;

        //std::cout << "START " << std::endl;
        while (scan.xct_next(lsn, log_rec_buf)) {
            EXPECT_EQ(lsn, log_rec_buf->get_lsn_ck());
        }
        //std::cout << "END " << std::endl;

        return RCOK;
    }
};

// the forward scan goes across two partitions
class forward_scan2  : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {
        logbuf_core *log_buffer = ((logbuf_core*)ssm->log);

        // after startup
        EXPECT_EQ(lsn_t(1,1696), log_buffer->_to_insert_lsn);    

        // consume 4096 bytes
        W_DO(consume(4096-1696,ssm));

        // consume the entire partition other than the last 4096 bytes
        for (int i=1; i<=(PART_SIZE-4096)/4096-1; i++) {
            W_DO(consume(4096,ssm));            
        }

        // after this log record, the checkpoint during normal shutdown would insert log records
        // to both the current partition and a new partition
        // both partitions would be kept after the shutdown
        W_DO(consume(4000,ssm));            

        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {

        // there is an async checkpoint going on, so let's wait a couple of seconds
        // TODO: how to avoid this sleep?
        sleep(5);

        log_i scan(*(smlevel_0::log), lsn_t(1,0), true);
        logrec_t log_rec_buf;
        lsn_t lsn;

        //std::cout << "START " << std::endl;
        while (scan.xct_next(lsn, log_rec_buf)) {
            EXPECT_EQ(lsn, log_rec_buf->get_lsn_ck());
        }
        //std::cout << "END " << std::endl;

        return RCOK;
    }
};


TEST (LogBufferTest, ForwardScan1) {
    test_env->empty_logdata_dir();
    forward_scan1 context;
    restart_test_options options;
    sm_options sm_options;

    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);
    sm_options.set_string_option("sm_log_impl", logbuf_core::IMPL_NAME);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, default_quota_in_pages, sm_options), 0);
}


TEST (LogBufferTest, ForwardScan2) {
    test_env->empty_logdata_dir();
    forward_scan2 context;
    restart_test_options options;
    sm_options sm_options;

    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);
    sm_options.set_string_option("sm_log_impl", logbuf_core::IMPL_NAME);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, default_quota_in_pages, sm_options), 0);
}


// the backward scan handles a log record spanning two segments
class backward_scan1 : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((logbuf_core*)ssm->log);

        // after startup
        // (64 + 72 + 96)*2 + 328 + 328 + 328 + (64 + 328 + 72 + 96) = 2008
        EXPECT_EQ(lsn_t(1,1696), log_buffer->_to_insert_lsn);    

        // consume 4096 bytes
        W_DO(consume(4096-1696,ssm));

        // consume the entire segment other than the last 4096 bytes
        for (int i=1; i<=(SEG_SIZE-4096)/4096-1; i++) {
            W_DO(consume(4096,ssm));            
        }

        // this log record spans two segment
        W_DO(consume(4096*2,ssm));            

        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((logbuf_core*)ssm->log);

        // there is an async checkpoint going on, so let's wait a couple of seconds
        // TODO: how to avoid this sleep?
        sleep(5);

        log_i scan(*(smlevel_0::log), log_buffer->_to_insert_lsn, false);
        logrec_t log_rec_buf;
        lsn_t lsn;

        //std::cout << "START " << std::endl;
        while (scan.xct_next(lsn, log_rec_buf)) {
            EXPECT_EQ(lsn, log_rec_buf->get_lsn_ck());
        }
        EXPECT_EQ(lsn, lsn_t(1,0));

        //std::cout << "END " << std::endl;

        return RCOK;
    }
};

// the backward scan goes across two partitions
class backward_scan2 : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((logbuf_core*)ssm->log);

        // after startup
        EXPECT_EQ(lsn_t(1,1696), log_buffer->_to_insert_lsn);    

        // consume 4096 bytes
        W_DO(consume(4096-1696,ssm));

        // consume the entire partition other than the last 4096 bytes
        for (int i=1; i<=(PART_SIZE-4096)/4096-1; i++) {
            W_DO(consume(4096,ssm));            
        }

        // after this log record, the checkpoint during normal shutdown would insert log records
        // to both the current partition and a new partition
        // both partitions would be kept after the shutdown
        W_DO(consume(4000,ssm));            

        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((logbuf_core*)ssm->log);

        // there is an async checkpoint going on, so let's wait a couple of seconds
        // TODO: how to avoid this sleep?
        sleep(5);

        log_i scan(*(smlevel_0::log), log_buffer->_to_insert_lsn, false);
        logrec_t log_rec_buf;
        lsn_t lsn;

        //std::cout << "START " << std::endl;
        while (scan.xct_next(lsn, log_rec_buf)) {
            EXPECT_EQ(lsn, log_rec_buf->get_lsn_ck());
        }
        EXPECT_EQ(lsn, lsn_t(1,0));
        //std::cout << "END " << std::endl;

        return RCOK;
    }
};



// the backward scan only scans the current partition
// the previous partition during shutdown is deleted 
class backward_scan3 : public restart_test_base 
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((logbuf_core*)ssm->log);

        // after startup
        EXPECT_EQ(lsn_t(1,1696), log_buffer->_to_insert_lsn);    

        // consume 4096 bytes
        W_DO(consume(4096-1696,ssm));

        // consume the entire partition other than the last 4096 bytes
        for (int i=1; i<=(PART_SIZE-4096)/4096-1; i++) {
            W_DO(consume(4096,ssm));            
        }

        // this log record is inserted to the new partition
        // only the new partition would be kept after the shutdown
        W_DO(consume(4096*2,ssm));            

        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((logbuf_core*)ssm->log);

        // there is an async checkpoint going on, so let's wait a couple of seconds
        // TODO: how to avoid this sleep?
        sleep(5);

        log_i scan(*(smlevel_0::log), log_buffer->_to_insert_lsn, false);
        logrec_t log_rec_buf;
        lsn_t lsn;

        //std::cout << "START " << std::endl;
        while (scan.xct_next(lsn, log_rec_buf)) {
            EXPECT_EQ(lsn, log_rec_buf->get_lsn_ck());
        }
        // the scan ends with lsn_t(2,0)
        // partition 1 was deleted during the shutdown
        EXPECT_EQ(lsn, lsn_t(2,0));
        //std::cout << "END " << std::endl;

        return RCOK;
    }
};


TEST (LogBufferTest, BackwardScan1) {
    test_env->empty_logdata_dir();
    backward_scan1 context;
    restart_test_options options;
    sm_options sm_options;

    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);
    sm_options.set_string_option("sm_log_impl", logbuf_core::IMPL_NAME);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, default_quota_in_pages, sm_options), 0);
}


TEST (LogBufferTest, BackwardScan2) {
    test_env->empty_logdata_dir();
    backward_scan2 context;
    restart_test_options options;
    sm_options sm_options;

    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);
    sm_options.set_string_option("sm_log_impl", logbuf_core::IMPL_NAME);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, default_quota_in_pages, sm_options), 0);
}


TEST (LogBufferTest, BackwardScan3) {
    test_env->empty_logdata_dir();
    backward_scan3 context;
    restart_test_options options;
    sm_options sm_options;

    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);
    sm_options.set_string_option("sm_log_impl", logbuf_core::IMPL_NAME);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, default_quota_in_pages, sm_options), 0);
}



#endif

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}
