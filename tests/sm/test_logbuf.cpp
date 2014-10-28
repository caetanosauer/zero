/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "btree_test_env.h"
#include "gtest/gtest.h"
#include "sm_vas.h"
#include "btree.h"
#include "btcursor.h"
#include "bf.h"
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
#include <Lintel/AtomicCounter.hpp>

#include "log_carray.h"


btree_test_env *test_env;


#ifdef LOG_BUFFER

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
#endif // LOG_BUFFER



// ========== test the standalone log buffer (internal states, single thread) ==========

#ifdef LOG_BUFFER

#else
#ifdef LOG_DIRECT_IO

#else
// these test cases do not work with DIRECT IO

// macros

#define PRIME(lsn)\
    (log_buffer->logbuf_prime(lsn))

#define INSERT(size)\
    (log_buffer->logbuf_insert(size))

#define FLUSH(lsn)\
    (log_buffer->logbuf_flush(lsn))

#define FETCH(lsn)\
    (log_buffer->logbuf_fetch(lsn))

#define PRINT()\
    (log_buffer->logbuf_print())


class logbuf_tester {
public:
    logbuf_tester(uint32_t count = LOGBUF_SEG_COUNT, uint32_t flush_trigger =
              LOGBUF_FLUSH_TRIGGER, uint32_t block_size =
              LOGBUF_BLOCK_SIZE, uint32_t seg_size = LOGBUF_SEG_SIZE, uint32_t part_size
              = LOGBUF_PART_SIZE);
    ~logbuf_tester();

    w_rc_t test_init(int case_no);
    w_rc_t test_insert();
    w_rc_t test_flush();
    w_rc_t test_fetch();
    w_rc_t test_replacement();

public:
    logbuf_core *log_buffer;

};

// test module
logbuf_tester *tester = NULL;



logbuf_tester::logbuf_tester(uint32_t count, uint32_t flush_trigger, uint32_t
                     block_size, uint32_t seg_size, uint32_t part_size) {
    log_buffer = new logbuf_core(count, flush_trigger, block_size, seg_size,
                             part_size, ConsolidationArray::DEFAULT_ACTIVE_SLOT_COUNT);
}

logbuf_tester::~logbuf_tester() {
    delete log_buffer;
}

// initialize the log buffer
w_rc_t logbuf_tester::test_init(int case_no) {
    switch(case_no) {
    case 1: {
        // case 1: the log starts at offset 0 in a partition
        PRIME(lsn_t(1,0));
        EXPECT_EQ(0, log_buffer->_start);
        EXPECT_EQ(0, log_buffer->_end);
        EXPECT_EQ(1000, log_buffer->_free);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_to_insert_lsn);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_to_flush_lsn);
        EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_old_epoch.base);
        EXPECT_EQ(0, log_buffer->_old_epoch.start);
        EXPECT_EQ(0, log_buffer->_old_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_cur_epoch.base);
        EXPECT_EQ(0, log_buffer->_cur_epoch.start);
        EXPECT_EQ(0, log_buffer->_cur_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_buf_epoch.base);
        EXPECT_EQ(0, log_buffer->_buf_epoch.start);
        EXPECT_EQ(0, log_buffer->_buf_epoch.end);
        break;
    }
    case 2: {
        // case 2: the log starts at offset 0 in a segment,
        // but not at offset 0 in a partition
        PRIME(lsn_t(1,1000));
        EXPECT_EQ(1000, log_buffer->_start);
        EXPECT_EQ(1000, log_buffer->_end);
        EXPECT_EQ(0, log_buffer->_free);
        EXPECT_EQ(lsn_t(1,1000), log_buffer->_to_insert_lsn);
        EXPECT_EQ(lsn_t(1,1000), log_buffer->_to_flush_lsn);
        EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_old_epoch.base);
        EXPECT_EQ(0, log_buffer->_old_epoch.start);
        EXPECT_EQ(0, log_buffer->_old_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_cur_epoch.base);
        EXPECT_EQ(1000, log_buffer->_cur_epoch.start);
        EXPECT_EQ(1000, log_buffer->_cur_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_buf_epoch.base);
        EXPECT_EQ(1000, log_buffer->_buf_epoch.start);
        EXPECT_EQ(1000, log_buffer->_buf_epoch.end);
        break;
    }
    case 3: {
        // case 3: the log starts in the middle of a segment
        PRIME(lsn_t(2,1250));
        EXPECT_EQ(1250, log_buffer->_start);
        EXPECT_EQ(1250, log_buffer->_end);
        EXPECT_EQ(750, log_buffer->_free);
        EXPECT_EQ(lsn_t(2,1250), log_buffer->_to_insert_lsn);
        EXPECT_EQ(lsn_t(2,1250), log_buffer->_to_flush_lsn);
        EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_old_epoch.base);
        EXPECT_EQ(0, log_buffer->_old_epoch.start);
        EXPECT_EQ(0, log_buffer->_old_epoch.end);
        EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_cur_epoch.base);
        EXPECT_EQ(1250, log_buffer->_cur_epoch.start);
        EXPECT_EQ(1250, log_buffer->_cur_epoch.end);
        EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_buf_epoch.base);
        EXPECT_EQ(1250, log_buffer->_buf_epoch.start);
        EXPECT_EQ(1250, log_buffer->_buf_epoch.end);
        break;
    }
    }
    return RCOK;
}


// insert log records at to_insert
w_rc_t logbuf_tester::test_insert() {
    PRIME(lsn_t(1,0));

    // case 1: to_insert starts at offset 0 in a partition
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_insert_lsn);

    W_DO(INSERT(300));
    EXPECT_EQ(0, log_buffer->_start);
    EXPECT_EQ(300, log_buffer->_end);
    EXPECT_EQ(700, log_buffer->_free);
    EXPECT_EQ(lsn_t(1,300), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(0, log_buffer->_old_epoch.start);
    EXPECT_EQ(0, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_cur_epoch.base);
    EXPECT_EQ(0, log_buffer->_cur_epoch.start);
    EXPECT_EQ(300, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(300, log_buffer->_buf_epoch.end);

    // case 2: to_insert starts at offset 0 in a segment,
    // but not at offset 0 in a partition
    W_DO(INSERT(300));
    W_DO(INSERT(400));

    EXPECT_EQ(lsn_t(1,1000), log_buffer->_to_insert_lsn);

    W_DO(INSERT(200));
    EXPECT_EQ(0, log_buffer->_start);
    EXPECT_EQ(1200, log_buffer->_end);
    EXPECT_EQ(800, log_buffer->_free);
    EXPECT_EQ(lsn_t(1,1200), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,1000), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(0, log_buffer->_old_epoch.start);
    EXPECT_EQ(0, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_cur_epoch.base);
    EXPECT_EQ(0, log_buffer->_cur_epoch.start);
    EXPECT_EQ(1200, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(1200, log_buffer->_buf_epoch.end);

    // case 3: to_insert starts in the middle of a segment
    EXPECT_EQ(lsn_t(1,1200), log_buffer->_to_insert_lsn);

    // 3.1: a log record is entirely stored within a segment
    W_DO(INSERT(300));
    EXPECT_EQ(0, log_buffer->_start);
    EXPECT_EQ(1500, log_buffer->_end);
    EXPECT_EQ(500, log_buffer->_free);
    EXPECT_EQ(lsn_t(1,1500), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,1000), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(0, log_buffer->_old_epoch.start);
    EXPECT_EQ(0, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_cur_epoch.base);
    EXPECT_EQ(0, log_buffer->_cur_epoch.start);
    EXPECT_EQ(1500, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(1500, log_buffer->_buf_epoch.end);

    // 3.2: a log record spans two segments
    W_DO(INSERT(300));

    EXPECT_EQ(lsn_t(1,1800), log_buffer->_to_insert_lsn);

    W_DO(INSERT(300));
    EXPECT_EQ(0, log_buffer->_start);
    EXPECT_EQ(2100, log_buffer->_end);
    EXPECT_EQ(900, log_buffer->_free);
    EXPECT_EQ(lsn_t(1,2100), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,2000), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(0, log_buffer->_old_epoch.start);
    EXPECT_EQ(0, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_cur_epoch.base);
    EXPECT_EQ(0, log_buffer->_cur_epoch.start);
    EXPECT_EQ(2100, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(2100, log_buffer->_buf_epoch.end);

    // 3.3: a log record does not fit in the current partition
    EXPECT_EQ(lsn_t(1,2100), log_buffer->_to_insert_lsn);
    for(int i=1; i<=9; i++) {
        W_DO(INSERT(300));
    }
    W_DO(FLUSH(lsn_t(1,4800)));
    EXPECT_EQ(lsn_t(1,4800), log_buffer->_to_insert_lsn);

    W_DO(INSERT(300));
    EXPECT_EQ(4800, log_buffer->_start);
    EXPECT_EQ(5300, log_buffer->_end);
    EXPECT_EQ(700, log_buffer->_free);
    EXPECT_EQ(lsn_t(2,300), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(1,4800), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(1,4000), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(4800, log_buffer->_old_epoch.start);
    EXPECT_EQ(4800, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(5000, log_buffer->_cur_epoch.base);
    EXPECT_EQ(0, log_buffer->_cur_epoch.start);
    EXPECT_EQ(300, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(5000, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(300, log_buffer->_buf_epoch.end);

    return RCOK;
}


// flush log records
w_rc_t logbuf_tester::test_flush() {
    lsn_t lsn(1,0);

    PRIME(lsn_t(1,0));

    // case 1: there is no unflushed log record
    W_DO(FLUSH(lsn));
    EXPECT_EQ(0, log_buffer->_start);
    EXPECT_EQ(0, log_buffer->_end);
    EXPECT_EQ(1000, log_buffer->_free);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(0, log_buffer->_old_epoch.start);
    EXPECT_EQ(0, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_cur_epoch.base);
    EXPECT_EQ(0, log_buffer->_cur_epoch.start);
    EXPECT_EQ(0, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(0, log_buffer->_buf_epoch.end);


    // case 2: the unflushed log records are within one segment
    W_DO(INSERT(300));
    W_DO(INSERT(300));
    W_DO(INSERT(300));

    lsn = lsn_t(1, 900);
    W_DO(FLUSH(lsn));
    EXPECT_EQ(900, log_buffer->_start);
    EXPECT_EQ(900, log_buffer->_end);
    EXPECT_EQ(100, log_buffer->_free);
    EXPECT_EQ(lsn_t(1,900), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(1,900), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(0, log_buffer->_old_epoch.start);
    EXPECT_EQ(0, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_cur_epoch.base);
    EXPECT_EQ(900, log_buffer->_cur_epoch.start);
    EXPECT_EQ(900, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(900, log_buffer->_buf_epoch.end);


    // case 3: the unflushed log records span two or more segments
    // (1, 900) to (1, 4500)
    for (int i=1; i<=12; i++) {
        W_DO(INSERT(300));
    }

    lsn = lsn_t(1, 4500);
    W_DO(FLUSH(lsn));
    EXPECT_EQ(4500, log_buffer->_start);
    EXPECT_EQ(4500, log_buffer->_end);
    EXPECT_EQ(500, log_buffer->_free);
    EXPECT_EQ(lsn_t(1,4500), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(1,4500), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(1,4000), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,4000), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(0, log_buffer->_old_epoch.start);
    EXPECT_EQ(0, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_cur_epoch.base);
    EXPECT_EQ(4500, log_buffer->_cur_epoch.start);
    EXPECT_EQ(4500, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(4500, log_buffer->_buf_epoch.end);


    // case 4: the unflushed log records span two partitions
    // (1, 4500) to (2, 1200)
    // 2*200 + 6*200
    for (int i=1; i<=8; i++) {
        W_DO(INSERT(200));
    }

    lsn = lsn_t(2, 1200);
    W_DO(FLUSH(lsn));
    EXPECT_EQ(6200, log_buffer->_start);
    EXPECT_EQ(6200, log_buffer->_end);
    EXPECT_EQ(800, log_buffer->_free);
    EXPECT_EQ(lsn_t(2,1200), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,1200), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(2,1000), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,1000), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(4900, log_buffer->_old_epoch.start);
    EXPECT_EQ(4900, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(5000, log_buffer->_cur_epoch.base);
    EXPECT_EQ(1200, log_buffer->_cur_epoch.start);
    EXPECT_EQ(1200, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(5000, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(1200, log_buffer->_buf_epoch.end);


    // corner case 1: the specified lsn is greater than to_insert
    // all log records up to to_insert should have been flushed when FLUSH returns
    W_DO(INSERT(300));
    EXPECT_EQ(lsn_t(2,1500), log_buffer->_to_insert_lsn);

    lsn = lsn_t(2, 2000);
    W_DO(FLUSH(lsn));
    EXPECT_EQ(6500, log_buffer->_start);
    EXPECT_EQ(6500, log_buffer->_end);
    EXPECT_EQ(500, log_buffer->_free);
    EXPECT_EQ(lsn_t(2,1500), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,1500), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(2,1000), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,1000), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(4900, log_buffer->_old_epoch.start);
    EXPECT_EQ(4900, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(5000, log_buffer->_cur_epoch.base);
    EXPECT_EQ(1500, log_buffer->_cur_epoch.start);
    EXPECT_EQ(1500, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(5000, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(1500, log_buffer->_buf_epoch.end);


    // corner case 2: the specified lsn is less than to_insert but greater than to_flush
    // all log records up to the specified lsn must have been flushed when FLUSH returns
    // at the same time, the flush daemon in the background would keep flushing until
    // there is no unflushed portion in the log; we use EXPECT_LE to catch changes made by
    // the flush daemon
    W_DO(INSERT(100));
    W_DO(INSERT(100));
    EXPECT_EQ(lsn_t(2,1700), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,1500), log_buffer->_to_flush_lsn);

    lsn = lsn_t(2, 1600);
    W_DO(FLUSH(lsn));
    EXPECT_LE(6600, log_buffer->_start);
    EXPECT_EQ(6700, log_buffer->_end);
    EXPECT_EQ(300, log_buffer->_free);
    EXPECT_EQ(lsn_t(2,1700), log_buffer->_to_insert_lsn);
    EXPECT_LE(lsn_t(2,1600), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(2,1000), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,1000), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(4900, log_buffer->_old_epoch.start);
    EXPECT_EQ(4900, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(5000, log_buffer->_cur_epoch.base);
    EXPECT_LE(1600, log_buffer->_cur_epoch.start);
    EXPECT_EQ(1700, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(5000, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(1700, log_buffer->_buf_epoch.end);



    // corner case 3: the specified lsn is less than to_flush
    // flush nothing!

    // before the actual test, make sure everything is flushed!
    lsn = lsn_t(2, 1700);
    W_DO(FLUSH(lsn));
    EXPECT_EQ(lsn_t(2,1700), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,1700), log_buffer->_to_flush_lsn);

    // the actual test
    W_DO(INSERT(100));
    EXPECT_EQ(lsn_t(2,1800), log_buffer->_to_insert_lsn);
    lsn = lsn_t(2, 1600);
    W_DO(FLUSH(lsn));
    EXPECT_EQ(6700, log_buffer->_start);
    EXPECT_EQ(6800, log_buffer->_end);
    EXPECT_EQ(200, log_buffer->_free);
    EXPECT_EQ(lsn_t(2,1800), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,1700), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(2,1000), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,1000), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(4900, log_buffer->_old_epoch.start);
    EXPECT_EQ(4900, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(5000, log_buffer->_cur_epoch.base);
    EXPECT_EQ(1700, log_buffer->_cur_epoch.start);
    EXPECT_EQ(1800, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(5000, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(1800, log_buffer->_buf_epoch.end);


    // // corner case: when the unflushed log records span two partitions
    // // and the specified lsn is in the first partition:
    // // all log records up to to_insert should be flushed

    // // (2, 1800) to (3, 300)
    // // 10*300 + 1*300
    // for (int i=1; i<=11; i++) {
    //     W_DO(INSERT(300));
    // }

    // lsn = lsn_t(2, 3000);
    // W_DO(FLUSH(lsn));
    // EXPECT_EQ(10300, log_buffer->_start);
    // EXPECT_EQ(10300, log_buffer->_end);
    // EXPECT_EQ(700, log_buffer->_free);
    // EXPECT_EQ(lsn_t(3,300), log_buffer->_to_insert_lsn);
    // EXPECT_EQ(lsn_t(3,300), log_buffer->_to_flush_lsn);
    // EXPECT_EQ(lsn_t(3,0), log_buffer->_to_flush_seg->base_lsn);
    // EXPECT_EQ(lsn_t(3,0), log_buffer->_to_insert_seg->base_lsn);
    // EXPECT_EQ(lsn_t(2,0), log_buffer->_old_epoch.base_lsn);
    // EXPECT_EQ(5000, log_buffer->_old_epoch.base);
    // EXPECT_EQ(4800, log_buffer->_old_epoch.start);
    // EXPECT_EQ(4800, log_buffer->_old_epoch.end);
    // EXPECT_EQ(lsn_t(3,0), log_buffer->_cur_epoch.base_lsn);
    // EXPECT_EQ(10000, log_buffer->_cur_epoch.base);
    // EXPECT_EQ(300, log_buffer->_cur_epoch.start);
    // EXPECT_EQ(300, log_buffer->_cur_epoch.end);
    // EXPECT_EQ(lsn_t(3,0), log_buffer->_buf_epoch.base_lsn);
    // EXPECT_EQ(10000, log_buffer->_buf_epoch.base);
    // EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    // EXPECT_EQ(300, log_buffer->_buf_epoch.end);


    return RCOK;
}


// fetch a log record starting at a given lsn
// this test case is policy-specific
w_rc_t logbuf_tester::test_fetch() {
    PRIME(lsn_t(1,0));

    int i=0, size=0;

    // fill up one partition first
    for (i=1, size=0; i<=5; i++) {
        W_DO(INSERT(300));
        W_DO(INSERT(300));
        W_DO(INSERT(300));
        W_DO(INSERT(100));
        size+=1000;
    }
    W_DO(FLUSH(lsn_t(1,size)));

    // fill up another partition, except the last segment
    for (i=1, size=0; i<=4; i++) {
        W_DO(INSERT(300));
        W_DO(INSERT(300));
        W_DO(INSERT(300));
        W_DO(INSERT(100));
        size+=1000;
    }
    W_DO(FLUSH(lsn_t(1,size)));

    // then add some records to the lastest segment
    W_DO(INSERT(300));
    W_DO(INSERT(300));
    W_DO(INSERT(300));


    // now the buffer looks like this
    // (2,0) (2,1000), (2,2000), (2,3000), (2,4000)
    PRINT();


    // case 1: lsn is at offset 0 in a partition
    // hit
    EXPECT_EQ(1, FETCH(lsn_t(2,0)));
    // miss
    EXPECT_EQ(0, FETCH(lsn_t(1,0)));


    // case 2: lsn is at offset 0 in a segment,
    // but not at offset 0 in a partition
    // miss
    EXPECT_EQ(0, FETCH(lsn_t(1,1000)));
    // hit
    EXPECT_EQ(1, FETCH(lsn_t(2,4000)));


    // case 3: lsn is in the middle of a segment
    // miss
    EXPECT_EQ(0, FETCH(lsn_t(1,2200)));
    // hit
    EXPECT_EQ(1, FETCH(lsn_t(2,4200)));


    // corner case 1
    // the lsn is greater than or equal to to_insert
    // invalid
    EXPECT_EQ(lsn_t(2,4900), log_buffer->_to_insert_lsn);
    EXPECT_EQ(-1, FETCH(lsn_t(2,4900)));
    EXPECT_EQ(-1, FETCH(lsn_t(3,0)));


    // corner case 2
    // the lsn is less than to_insert but greater than or equal to to_flush
    // hit
    EXPECT_EQ(lsn_t(2,4900), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,4000), log_buffer->_to_flush_lsn);
    EXPECT_EQ(1, FETCH(lsn_t(2,4000)));
    EXPECT_EQ(1, FETCH(lsn_t(2,4300)));


    // corner case 3
    // the lsn is the very end point of a partition
    // N/A

    return RCOK;
}


// this test case is policy-specific
w_rc_t logbuf_tester::test_replacement() {
    // N=5, M=3



    // assuming segments (1, 0-3500) is valid
    PRIME(lsn_t(1,3500));

    // case 1: not full
    // initially only one segment
    EXPECT_EQ((uint32_t)1, log_buffer->_seg_list->count());
    EXPECT_EQ(log_buffer->_to_insert_seg, log_buffer->_to_flush_seg);
    EXPECT_EQ(lsn_t(1,3000), log_buffer->_to_flush_seg->base_lsn);


    // insert
    W_DO(INSERT(300));
    W_DO(INSERT(200)); // (1, 4000)

    W_DO(INSERT(300));
    W_DO(INSERT(300));
    W_DO(INSERT(300));
    W_DO(INSERT(100)); // (1, 5000)

    // 2 segments now
    EXPECT_EQ((uint32_t)2, log_buffer->_seg_list->count());
    // the list looks like this: (1,3000) -> (1,4000)
    EXPECT_EQ(log_buffer->_to_flush_seg,
              log_buffer->_seg_list->prev_of(log_buffer->_to_insert_seg));
    EXPECT_EQ(lsn_t(1,3000), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,4000), log_buffer->_to_insert_seg->base_lsn);

    // fetch
    // miss
    EXPECT_EQ(0, FETCH(lsn_t(1,500)));

    // 3 segments now
    EXPECT_EQ((uint32_t)3, log_buffer->_seg_list->count());
    // the list looks like this: (1,0) -> (1,3000) -> (1,4000)
    EXPECT_EQ(log_buffer->_seg_list->top(),
              log_buffer->_seg_list->prev_of(log_buffer->_to_flush_seg));
    EXPECT_EQ(log_buffer->_to_flush_seg,
              log_buffer->_seg_list->prev_of(log_buffer->_to_insert_seg));
    EXPECT_EQ(lsn_t(1,0), log_buffer->_seg_list->top()->base_lsn);
    EXPECT_EQ(lsn_t(1,3000), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,4000), log_buffer->_to_insert_seg->base_lsn);


    // case 3: full, and forced flush

    int i=0, size=0;
    // fill up 4 segments
    for (i=1, size=0; i<=3; i++) {
        W_DO(INSERT(300));
        W_DO(INSERT(300));
        W_DO(INSERT(300));
        W_DO(INSERT(100));
        size+=1000;
    }

    // 5 segments now
    EXPECT_EQ((uint32_t)5, log_buffer->_seg_list->count());
    // the list looks like this:
    // (1,3000) -> (1,4000) -> (2,0) -> (2,1000) -> (2,2000)
    EXPECT_EQ(log_buffer->_seg_list->top(), log_buffer->_to_flush_seg);
    EXPECT_EQ(lsn_t(1,3000), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,2000), log_buffer->_to_insert_seg->base_lsn);

    // insert
    W_DO(INSERT(300)); // (2,3300)

    // 5 segments now
    EXPECT_EQ((uint32_t)5, log_buffer->_seg_list->count());
    // (1,4000) -> (2,0) -> (2,1000) -> (2,2000) -> (2,3000)
    EXPECT_EQ(log_buffer->_seg_list->bottom(), log_buffer->_to_insert_seg);
    // the forced flush returns after flushing (1,3000)
    // but the flush daemon continues flushing in the background
    // so the _to_flush_seg is changing...
    EXPECT_LE(lsn_t(1,3000), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,3000), log_buffer->_to_insert_seg->base_lsn);


    // case 2: full, and no forced flush
    W_DO(FLUSH(lsn_t(2,3300)));

    // insert
    W_DO(INSERT(300)); // (2,3600)
    W_DO(INSERT(300)); // (2,3900)
    W_DO(INSERT(300)); // (2,4200)

    // 5 segments now
    EXPECT_EQ((uint32_t)5, log_buffer->_seg_list->count());
    // (2,0) -> (2,1000) -> (2,2000) -> (2,3000) -> (2,4000)
    EXPECT_EQ(log_buffer->_seg_list->bottom(), log_buffer->_to_insert_seg);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_seg_list->top()->base_lsn);
    EXPECT_EQ(lsn_t(2,3000), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,4000), log_buffer->_to_insert_seg->base_lsn);
    //PRINT();

    // fetch
    // miss
    EXPECT_EQ(0, FETCH(lsn_t(1,500)));

    // 5 segments now
    EXPECT_EQ((uint32_t)5, log_buffer->_seg_list->count());
    // (1,0) -> (2,1000) -> (2,2000) -> (2,3000) -> (2,4000)
    EXPECT_EQ(log_buffer->_seg_list->bottom(), log_buffer->_to_insert_seg);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_seg_list->top()->base_lsn);
    EXPECT_EQ(lsn_t(2,3000), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,4000), log_buffer->_to_insert_seg->base_lsn);

    //PRINT();


    return RCOK;
}



// test cases for standalone log buffer
TEST (LogBufferTest, Init) {
    uint32_t seg_count = 5;
    uint32_t flush_trigger = 3;
    uint32_t block_size = 100; // 3*block_size is the max size of a log record
    uint32_t seg_size = 1000;
    uint32_t part_size = 5000;

    for (int i=1; i<=3; i++) {
        tester = new logbuf_tester(seg_count, flush_trigger, block_size, seg_size,
                               part_size);
        tester->test_init(i);
        delete tester;
    }
}

TEST (LogBufferTest, Insert) {
    uint32_t seg_count = 5;
    uint32_t flush_trigger = 3;
    uint32_t block_size = 100; // 3*block_size is the max size of a log record
    uint32_t seg_size = 1000;
    uint32_t part_size = 5000;

    tester = new logbuf_tester(seg_count, flush_trigger, block_size, seg_size,
                           part_size);


    tester->test_insert();


    delete tester;
}


TEST (LogBufferTest, Flush) {
    uint32_t seg_count = 5;
    uint32_t flush_trigger = 3;
    uint32_t block_size = 100; // 3*block_size is the max size of a log record
    uint32_t seg_size = 1000;
    uint32_t part_size = 5000;

    tester = new logbuf_tester(seg_count, flush_trigger, block_size, seg_size,
                           part_size);


    tester->test_flush();


    delete tester;
}


TEST (LogBufferTest, Fetch) {
    uint32_t seg_count = 5;
    uint32_t flush_trigger = 3;
    uint32_t block_size = 100; // 3*block_size is the max size of a log record
    uint32_t seg_size = 1000;
    uint32_t part_size = 5000;

    tester = new logbuf_tester(seg_count, flush_trigger, block_size, seg_size,
                           part_size);


    tester->test_fetch();


    delete tester;
}


TEST (LogBufferTest, Replacement) {
    uint32_t seg_count = 5;
    uint32_t flush_trigger = 3;
    uint32_t block_size = 100; // 3*block_size is the max size of a log record
    uint32_t seg_size = 1000;
    uint32_t part_size = 5000;

    tester = new logbuf_tester(seg_count, flush_trigger, block_size, seg_size,
                           part_size);


    tester->test_replacement();


    delete tester;
}
#endif // LOG_DIRECT_IO
#endif // LOG_BUFFER


// ========== test the entire system with the new log buffer (internal states, single thread) ==========
#ifdef LOG_BUFFER


// size % 8 == 0
#define INSERT(size)\
    (insert(size, ssm))

// this is hacky, but it is like another transaction or thread is calling commit
#define FLUSH(to_lsn)\
    (log_buffer->flush(to_lsn))

#define FETCH(lsn) \
    (log_buffer->fetch_for_test(lsn, rp))

// #define FETCH(lsn)
//     (log_buffer->logbuf_fetch(lsn))

#define PRINT()\
    (log_buffer->logbuf_print())


// insert a log record of size bytes
rc_t insert(int size, ss_m *ssm) {
    if (size>BUF_SIZE) {
        return RC(fcINTERNAL);
    }

    // size = floor((buf_size-1)/8) * 8 + 56 (log message)
    int buf_size = (size - 56) + 1;
    memset(buf, 'z', buf_size-1);
    buf[buf_size] = '\0';

    W_DO(ssm->log_message(buf));

    return RCOK;
}

// flush all dirty log records
rc_t flush(ss_m *ssm) {
    return x_commit_xct(ssm);
}


// test_init

// case 1: the log starts at offset 0 in a partition
// unfortunately, we cannot verify the internal state immediately after _prime
// when starting from an empty log, there are several log records inserted during startup
// 2*chkpt, abort, dismount, chkpt
// (56 + 64 + 88)*2 + 320 + 320 + 320 + (56 + 320 + 64 + 88) = 1904
// so we can only verify the internal state after the entire startup process is done
class init_test_case1  : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((log_core*)ssm->log)->_log_buffer;

        EXPECT_EQ(1904, log_buffer->_start);
        EXPECT_EQ(1904, log_buffer->_end);
        EXPECT_EQ(SEG_SIZE-1904, log_buffer->_free);
        EXPECT_EQ(lsn_t(1,1904), log_buffer->_to_insert_lsn);
        EXPECT_EQ(lsn_t(1,1904), log_buffer->_to_flush_lsn);
        EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_old_epoch.base);
        EXPECT_EQ(0, log_buffer->_old_epoch.start);
        EXPECT_EQ(0, log_buffer->_old_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_cur_epoch.base);
        EXPECT_EQ(1904, log_buffer->_cur_epoch.start);
        EXPECT_EQ(1904, log_buffer->_cur_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_buf_epoch.base);
        EXPECT_EQ(0, log_buffer->_buf_epoch.start);
        EXPECT_EQ(1904, log_buffer->_buf_epoch.end);


        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        return RCOK;
    }
};



// case 2: the log starts at offset 0 in a segment,
// but not at offset 0 in a partition
// consume one segment during pre_shutdown
// verify internal state during post_shutdown
class init_test_case2  : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((log_core*)ssm->log)->_log_buffer;

        // after startup
        EXPECT_EQ(lsn_t(1,1904), log_buffer->_to_insert_lsn);

        // consume the entire segment
        // one checkpoint will be taken during shutdown (56 + 320 + 64 + 88) = 528
        W_DO(consume(4096-1904-528,ssm));
        for (int i=1; i<=SEG_SIZE/4096-1; i++) {
            W_DO(consume(4096,ssm));
        }

        EXPECT_EQ(SEG_SIZE-528, log_buffer->_start);
        EXPECT_EQ(SEG_SIZE-528, log_buffer->_end);
        EXPECT_EQ(528, log_buffer->_free);
        EXPECT_EQ(lsn_t(1,SEG_SIZE-528), log_buffer->_to_insert_lsn);
        EXPECT_EQ(lsn_t(1,SEG_SIZE-528), log_buffer->_to_flush_lsn);
        EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_old_epoch.base);
        EXPECT_EQ(0, log_buffer->_old_epoch.start);
        EXPECT_EQ(0, log_buffer->_old_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_cur_epoch.base);
        EXPECT_EQ(SEG_SIZE-528, log_buffer->_cur_epoch.start);
        EXPECT_EQ(SEG_SIZE-528, log_buffer->_cur_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_buf_epoch.base);
        EXPECT_EQ(0, log_buffer->_buf_epoch.start);
        EXPECT_EQ(SEG_SIZE-528, log_buffer->_buf_epoch.end);

        return RCOK;
    }

    // when starting from an non-empty log, there are several log records inserted during startup
    // chkpt, abort, dismount, chkpt
    // (56 + 320 + 64 + 88) + 320 + 320 + (56 + 320 + 64 + 88) = 1696
    // one more async chkpt
    // 1696 + (56 + 320 + 64 + 88) = 2224
    w_rc_t post_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((log_core*)ssm->log)->_log_buffer;

        // there is an async checkpoint going on, so let's wait a couple of seconds
        // TODO: how to avoid this sleep?
        sleep(5);

        // after startup
        EXPECT_EQ(SEG_SIZE+2224, log_buffer->_start);
        EXPECT_EQ(SEG_SIZE+2224, log_buffer->_end);
        EXPECT_EQ(SEG_SIZE-2224, log_buffer->_free);
        EXPECT_EQ(lsn_t(1,SEG_SIZE+2224), log_buffer->_to_insert_lsn);
        EXPECT_EQ(lsn_t(1,SEG_SIZE+2224), log_buffer->_to_flush_lsn);
        EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_old_epoch.base);
        EXPECT_EQ(0, log_buffer->_old_epoch.start);
        EXPECT_EQ(0, log_buffer->_old_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_cur_epoch.base);
        EXPECT_EQ(SEG_SIZE+2224, log_buffer->_cur_epoch.start);
        EXPECT_EQ(SEG_SIZE+2224, log_buffer->_cur_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_buf_epoch.base);
        EXPECT_EQ(SEG_SIZE, log_buffer->_buf_epoch.start);
        EXPECT_EQ(SEG_SIZE+2224, log_buffer->_buf_epoch.end);

        return RCOK;
    }
};



// case 3: the log starts in the middle of a segment
// consume one segment during pre_shutdown
// verify internal state during post_shutdown
class init_test_case3  : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((log_core*)ssm->log)->_log_buffer;

        // after startup
        EXPECT_EQ(lsn_t(1,1904), log_buffer->_to_insert_lsn);

        // to consume 4096 bytes
        int size = 4096;

        // one checkpoint will be taken during shutdown (56 + 320 + 64 + 88) = 528
        W_DO(consume(size-1904-528,ssm));

        EXPECT_EQ(size-528, log_buffer->_start);
        EXPECT_EQ(size-528, log_buffer->_end);
        EXPECT_EQ(SEG_SIZE-(size-528), log_buffer->_free);
        EXPECT_EQ(lsn_t(1,size-528), log_buffer->_to_insert_lsn);
        EXPECT_EQ(lsn_t(1,size-528), log_buffer->_to_flush_lsn);
        EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_old_epoch.base);
        EXPECT_EQ(0, log_buffer->_old_epoch.start);
        EXPECT_EQ(0, log_buffer->_old_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_cur_epoch.base);
        EXPECT_EQ(size-528, log_buffer->_cur_epoch.start);
        EXPECT_EQ(size-528, log_buffer->_cur_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_buf_epoch.base);
        EXPECT_EQ(0, log_buffer->_buf_epoch.start);
        EXPECT_EQ(size-528, log_buffer->_buf_epoch.end);

        return RCOK;
    }

    // when starting from an non-empty log, there are several log records inserted during startup
    // chkpt, mount/dismount, chkpt
    // (56 + 320 + 64 + 88) + 320 + 320 + (56 + 320 + 64 + 88) = 1696
    // one more async chkpt
    // 1696 + (56 + 320 + 64 + 88) = 2224
    w_rc_t post_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((log_core*)ssm->log)->_log_buffer;

        // there is an async checkpoint going on, so let's wait a couple of seconds
        // TODO: how to avoid this sleep?
        sleep(5);

        int size = 4096;

        // after startup
        EXPECT_EQ(size+2224, log_buffer->_start);
        EXPECT_EQ(size+2224, log_buffer->_end);
        EXPECT_EQ(SEG_SIZE-(size+2224), log_buffer->_free);
        EXPECT_EQ(lsn_t(1,size+2224), log_buffer->_to_insert_lsn);
        EXPECT_EQ(lsn_t(1,size+2224), log_buffer->_to_flush_lsn);
        EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_old_epoch.base);
        EXPECT_EQ(0, log_buffer->_old_epoch.start);
        EXPECT_EQ(0, log_buffer->_old_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_cur_epoch.base);
        EXPECT_EQ(size+2224, log_buffer->_cur_epoch.start);
        EXPECT_EQ(size+2224, log_buffer->_cur_epoch.end);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
        EXPECT_EQ(0, log_buffer->_buf_epoch.base);
        EXPECT_EQ(size, log_buffer->_buf_epoch.start);
        EXPECT_EQ(size+2224, log_buffer->_buf_epoch.end);

        return RCOK;
    }
};



TEST (LogBufferTest2, Init1) {
    test_env->empty_logdata_dir();
    init_test_case1 context;
    restart_test_options options;
    sm_options sm_options;


    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, default_quota_in_pages, sm_options), 0);
}

TEST (LogBufferTest2, Init2) {
    test_env->empty_logdata_dir();
    init_test_case2 context;
    restart_test_options options;
    sm_options sm_options;

    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, default_quota_in_pages, sm_options), 0);
}

TEST (LogBufferTest2, Init3) {
    test_env->empty_logdata_dir();
    init_test_case3 context;
    restart_test_options options;
    sm_options sm_options;

    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, default_quota_in_pages, sm_options), 0);
}


// test_insert
w_rc_t test_insert(ss_m *ssm, test_volume_t *) {
    logbuf_core *log_buffer = ((log_core*)ssm->log)->_log_buffer;

    // after startup
    EXPECT_EQ(lsn_t(1,1904), log_buffer->_to_insert_lsn);

    // case 1: to_insert starts at offset 0 in a partition
    // consume the entire partition
    W_DO(consume(4096-1904,ssm));
    for (int i=1; i<=PART_SIZE/4096-1; i++) {
        W_DO(consume(4096,ssm));
    }
    EXPECT_EQ(lsn_t(1,PART_SIZE), log_buffer->_to_insert_lsn);

    W_DO(x_begin_xct(ssm,true));
    W_DO(INSERT(128));

    EXPECT_EQ(PART_SIZE, log_buffer->_start);
    EXPECT_EQ(PART_SIZE+128, log_buffer->_end);
    EXPECT_EQ(SEG_SIZE-128, log_buffer->_free);
    EXPECT_EQ(lsn_t(2,128), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(1,PART_SIZE), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(1,PART_SIZE-SEG_SIZE), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(PART_SIZE, log_buffer->_old_epoch.start);
    EXPECT_EQ(PART_SIZE, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_cur_epoch.base);
    EXPECT_EQ(0, log_buffer->_cur_epoch.start);
    EXPECT_EQ(128, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(128, log_buffer->_buf_epoch.end);


    // case 2: to_insert starts at offset 0 in a segment,
    // but not at offset 0 in a partition
    W_DO(x_commit_xct(ssm)); // adding 48*2 bytes due to the commit
    // this commit triggers two async checkpoints for opening a new partition for append
    // so let's wait a couple of seconds
    // TODO: how to avoid this sleep?
    sleep(5);

    // consume the entire segment
    // one checkpoint (56 + 320 + 64 + 88) = 528
    W_DO(consume(4096-128-48*2-528*2,ssm));
    for (int i=1; i<=SEG_SIZE/4096-1; i++) {
        W_DO(consume(4096,ssm));
    }
    EXPECT_EQ(lsn_t(2,SEG_SIZE), log_buffer->_to_insert_lsn);

    W_DO(x_begin_xct(ssm,true));
    W_DO(INSERT(128));

    EXPECT_EQ(PART_SIZE+SEG_SIZE, log_buffer->_start);
    EXPECT_EQ(PART_SIZE+SEG_SIZE+128, log_buffer->_end);
    EXPECT_EQ(SEG_SIZE-128, log_buffer->_free);
    EXPECT_EQ(lsn_t(2,SEG_SIZE+128), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,SEG_SIZE), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,SEG_SIZE), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(PART_SIZE, log_buffer->_old_epoch.start);
    EXPECT_EQ(PART_SIZE, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_cur_epoch.base);
    EXPECT_EQ(SEG_SIZE, log_buffer->_cur_epoch.start);
    EXPECT_EQ(SEG_SIZE+128, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(SEG_SIZE+128, log_buffer->_buf_epoch.end);

    // case 3: to_insert starts in the middle of a segment

    // 3.1: a log record is entirely stored within a segment
    W_DO(INSERT(128));

    EXPECT_EQ(PART_SIZE+SEG_SIZE, log_buffer->_start);
    EXPECT_EQ(PART_SIZE+SEG_SIZE+128+128, log_buffer->_end);
    EXPECT_EQ(SEG_SIZE-(128+128), log_buffer->_free);
    EXPECT_EQ(lsn_t(2,SEG_SIZE+128+128), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,SEG_SIZE), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,SEG_SIZE), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(PART_SIZE, log_buffer->_old_epoch.start);
    EXPECT_EQ(PART_SIZE, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_cur_epoch.base);
    EXPECT_EQ(SEG_SIZE, log_buffer->_cur_epoch.start);
    EXPECT_EQ(SEG_SIZE+128+128, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(SEG_SIZE+128+128, log_buffer->_buf_epoch.end);

    // 3.2: a log record spans two segments
    // let's move towards the end of this segment
    W_DO(x_commit_xct(ssm)); // adding 48*2 bytes due to the commit
    W_DO(consume(4096-128-128-48*2,ssm));
    for (int i=1; i<=SEG_SIZE/4096-1-1; i++) {
        W_DO(consume(4096,ssm));
    }
    EXPECT_EQ(lsn_t(2,SEG_SIZE+SEG_SIZE-4096), log_buffer->_to_insert_lsn);

    W_DO(x_begin_xct(ssm,true));
    W_DO(INSERT(4096*2));
    EXPECT_EQ(PART_SIZE+SEG_SIZE*2-4096, log_buffer->_start);
    EXPECT_EQ(PART_SIZE+SEG_SIZE*2+4096, log_buffer->_end);
    EXPECT_EQ(SEG_SIZE-4096, log_buffer->_free);
    EXPECT_EQ(lsn_t(2,SEG_SIZE*2+4096), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,SEG_SIZE*2-4096), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(2,SEG_SIZE), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,SEG_SIZE*2), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(PART_SIZE, log_buffer->_old_epoch.start);
    EXPECT_EQ(PART_SIZE, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_cur_epoch.base);
    EXPECT_EQ(SEG_SIZE*2-4096, log_buffer->_cur_epoch.start);
    EXPECT_EQ(SEG_SIZE*2+4096, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(SEG_SIZE*2+4096, log_buffer->_buf_epoch.end);

    // 3.3: a log record does not fit in the current partition
    // let's move towards the end of this partition
    W_DO(x_commit_xct(ssm)); // adding 48*2 bytes due to the commit
    W_DO(consume(4096-48*2,ssm));
    for (int i=1; i<=(PART_SIZE-SEG_SIZE*2-4096*2)/4096-1; i++) {
        W_DO(consume(4096,ssm));
    }
    EXPECT_EQ(lsn_t(2,PART_SIZE-4096), log_buffer->_to_insert_lsn);

    W_DO(x_begin_xct(ssm,true));
    W_DO(INSERT(4096*2));
    EXPECT_EQ(PART_SIZE*2-4096, log_buffer->_start);
    EXPECT_EQ(PART_SIZE*2+4096*2, log_buffer->_end);
    EXPECT_EQ(SEG_SIZE-4096*2, log_buffer->_free);
    EXPECT_EQ(lsn_t(3,4096*2), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,PART_SIZE-4096), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(2,PART_SIZE-SEG_SIZE), log_buffer->_to_flush_seg->base_lsn);
    EXPECT_EQ(lsn_t(3,0), log_buffer->_to_insert_seg->base_lsn);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_old_epoch.base);
    EXPECT_EQ(PART_SIZE-4096, log_buffer->_old_epoch.start);
    EXPECT_EQ(PART_SIZE-4096, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(3,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE*2, log_buffer->_cur_epoch.base);
    EXPECT_EQ(0, log_buffer->_cur_epoch.start);
    EXPECT_EQ(4096*2, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(3,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE*2, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(4096*2, log_buffer->_buf_epoch.end);

    W_DO(x_commit_xct(ssm));

    return RCOK;
}


TEST (LogBufferTest2, Insert) {
    test_env->empty_logdata_dir();
    sm_options sm_options;
    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);
    EXPECT_EQ(test_env->runBtreeTest(test_insert, true, 1<<16, sm_options), 0);
}


// test_flush
w_rc_t test_flush(ss_m *ssm, test_volume_t *) {
    logbuf_core *log_buffer = ((log_core*)ssm->log)->_log_buffer;

    // after startup
    EXPECT_EQ(lsn_t(1,1904), log_buffer->_to_insert_lsn);

    // case 1: there is no unflushed log record
    W_DO(FLUSH(lsn_t(1,1904)));

    EXPECT_EQ(1904, log_buffer->_start);
    EXPECT_EQ(1904, log_buffer->_end);
    EXPECT_EQ(SEG_SIZE-1904, log_buffer->_free);
    EXPECT_EQ(lsn_t(1,1904), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(1,1904), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(0, log_buffer->_old_epoch.start);
    EXPECT_EQ(0, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_cur_epoch.base);
    EXPECT_EQ(1904, log_buffer->_cur_epoch.start);
    EXPECT_EQ(1904, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(1904, log_buffer->_buf_epoch.end);

    // case 2: the unflushed log records are within one segment
    W_DO(x_begin_xct(ssm,true));
    W_DO(INSERT(128));
    W_DO(x_commit_xct(ssm)); // adding 48*2 bytes due to the commit

    EXPECT_EQ(1904+128+48*2, log_buffer->_start);
    EXPECT_EQ(1904+128+48*2, log_buffer->_end);
    EXPECT_EQ(SEG_SIZE-1904-128-48*2, log_buffer->_free);
    EXPECT_EQ(lsn_t(1,1904+128+48*2), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(1,1904+128+48*2), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(0, log_buffer->_old_epoch.start);
    EXPECT_EQ(0, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_cur_epoch.base);
    EXPECT_EQ(1904+128+48*2, log_buffer->_cur_epoch.start);
    EXPECT_EQ(1904+128+48*2, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(1904+128+48*2, log_buffer->_buf_epoch.end);

    // case 3: the unflushed log records span two or more segments
    W_DO(consume(4096-1904-128-48*2,ssm));

    // (1, 4096) to (1, SEG_SIZE+4096)
    W_DO(x_begin_xct(ssm,true));
    for (int i=1; i<=(SEG_SIZE)/4096; i++) {
        W_DO(INSERT(4096));
    }
    W_DO(x_commit_xct(ssm)); // adding 48*2 bytes due to the commit

    EXPECT_EQ(SEG_SIZE+4096+48*2, log_buffer->_start);
    EXPECT_EQ(SEG_SIZE+4096+48*2, log_buffer->_end);
    EXPECT_EQ(SEG_SIZE-4096-48*2, log_buffer->_free);
    EXPECT_EQ(lsn_t(1,SEG_SIZE+4096+48*2), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(1,SEG_SIZE+4096+48*2), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(0,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(0, log_buffer->_old_epoch.start);
    EXPECT_EQ(0, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_cur_epoch.base);
    EXPECT_EQ(SEG_SIZE+4096+48*2, log_buffer->_cur_epoch.start);
    EXPECT_EQ(SEG_SIZE+4096+48*2, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(SEG_SIZE+4096+48*2, log_buffer->_buf_epoch.end);

    // case 4: the unflushed log records span two partitions
    // let's move towards the end of this partition
    W_DO(consume(4096-48*2,ssm));
    for (int i=1; i<=(PART_SIZE-SEG_SIZE-4096*2)/4096-1; i++) {
        W_DO(consume(4096,ssm));
    }
    EXPECT_EQ(lsn_t(1,PART_SIZE-4096), log_buffer->_to_insert_lsn);

    // (1, PART_SIZE-4096) to (2, 4096)
    W_DO(x_begin_xct(ssm,true));
    W_DO(INSERT(128));
    W_DO(INSERT(4096));
    W_DO(x_commit_xct(ssm));

    // this flush triggers two async checkpoints for opening a new partition for append
    // so let's wait a couple of seconds
    // one checkpoint (56 + 320 + 64 + 88) = 528
    // TODO: how to avoid this sleep?
    sleep(5);

    EXPECT_EQ(PART_SIZE+4096+48*2+528*2, log_buffer->_start);
    EXPECT_EQ(PART_SIZE+4096+48*2+528*2, log_buffer->_end);
    EXPECT_EQ(SEG_SIZE-4096-48*2-528*2, log_buffer->_free);
    EXPECT_EQ(lsn_t(2,4096+48*2+528*2), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,4096+48*2+528*2), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(PART_SIZE-4096+128, log_buffer->_old_epoch.start);
    EXPECT_EQ(PART_SIZE-4096+128, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_cur_epoch.base);
    EXPECT_EQ(4096+48*2+528*2, log_buffer->_cur_epoch.start);
    EXPECT_EQ(4096+48*2+528*2, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(4096+48*2+528*2, log_buffer->_buf_epoch.end);


    // corner case 1: the specified lsn is greater than to_insert
    // all log records up to to_insert should have been flushed when FLUSH returns
    W_DO(consume(4096-528*2-48*2,ssm));
    W_DO(x_begin_xct(ssm,true));
    W_DO(INSERT(4096));
    EXPECT_EQ(lsn_t(2,4096*3), log_buffer->_to_insert_lsn);

    W_DO(FLUSH(lsn_t(2,4096*4)));

    EXPECT_EQ(PART_SIZE+4096*3, log_buffer->_start);
    EXPECT_EQ(PART_SIZE+4096*3, log_buffer->_end);
    EXPECT_EQ(SEG_SIZE-4096*3, log_buffer->_free);
    EXPECT_EQ(lsn_t(2,4096*3), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,4096*3), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(PART_SIZE-4096+128, log_buffer->_old_epoch.start);
    EXPECT_EQ(PART_SIZE-4096+128, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_cur_epoch.base);
    EXPECT_EQ(4096*3, log_buffer->_cur_epoch.start);
    EXPECT_EQ(4096*3, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(4096*3, log_buffer->_buf_epoch.end);


    // corner case 2: the specified lsn is less than to_insert but greater than to_flush
    // all log records up to the specified lsn must have been flushed when FLUSH returns
    // at the same time, the flush daemon in the background would keep flushing until
    // there is no unflushed portion in the log; we use EXPECT_LE to catch changes made by
    // the flush daemon
    W_DO(x_commit_xct(ssm));
    W_DO(consume(4096-48*2,ssm));
    W_DO(x_begin_xct(ssm,true));
    W_DO(INSERT(4096*2));
    EXPECT_EQ(lsn_t(2,4096*6), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,4096*4), log_buffer->_to_flush_lsn);

    W_DO(FLUSH(lsn_t(2,4096*5)));

    EXPECT_LE(PART_SIZE+4096*5, log_buffer->_start);
    EXPECT_EQ(PART_SIZE+4096*6, log_buffer->_end);
    EXPECT_EQ(SEG_SIZE-4096*6, log_buffer->_free);
    EXPECT_EQ(lsn_t(2,4096*6), log_buffer->_to_insert_lsn);
    EXPECT_LE(lsn_t(2,4096*5), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(PART_SIZE-4096+128, log_buffer->_old_epoch.start);
    EXPECT_EQ(PART_SIZE-4096+128, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_cur_epoch.base);
    EXPECT_LE(4096*5, log_buffer->_cur_epoch.start);
    EXPECT_EQ(4096*6, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(4096*6, log_buffer->_buf_epoch.end);


    // corner case 3: the specified lsn is less than to_flush
    // flush nothing!
    W_DO(x_commit_xct(ssm));
    W_DO(consume(4096-48*2,ssm));
    W_DO(x_begin_xct(ssm,true));
    W_DO(INSERT(4096));
    EXPECT_EQ(lsn_t(2,4096*8), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(2,4096*7), log_buffer->_to_flush_lsn);

    W_DO(FLUSH(lsn_t(2,4096*6)));

    EXPECT_LE(PART_SIZE+4096*7, log_buffer->_start);
    EXPECT_EQ(PART_SIZE+4096*8, log_buffer->_end);
    EXPECT_EQ(SEG_SIZE-4096*8, log_buffer->_free);
    EXPECT_EQ(lsn_t(2,4096*8), log_buffer->_to_insert_lsn);
    EXPECT_LE(lsn_t(2,4096*7), log_buffer->_to_flush_lsn);
    EXPECT_EQ(lsn_t(1,0), log_buffer->_old_epoch.base_lsn);
    EXPECT_EQ(0, log_buffer->_old_epoch.base);
    EXPECT_EQ(PART_SIZE-4096+128, log_buffer->_old_epoch.start);
    EXPECT_EQ(PART_SIZE-4096+128, log_buffer->_old_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_cur_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_cur_epoch.base);
    EXPECT_LE(4096*7, log_buffer->_cur_epoch.start);
    EXPECT_EQ(4096*8, log_buffer->_cur_epoch.end);
    EXPECT_EQ(lsn_t(2,0), log_buffer->_buf_epoch.base_lsn);
    EXPECT_EQ(PART_SIZE, log_buffer->_buf_epoch.base);
    EXPECT_EQ(0, log_buffer->_buf_epoch.start);
    EXPECT_EQ(4096*8, log_buffer->_buf_epoch.end);


    W_DO(x_commit_xct(ssm));


    return RCOK;
}


TEST (LogBufferTest2, Flush) {
    test_env->empty_logdata_dir();
    sm_options sm_options;
    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);
    EXPECT_EQ(test_env->runBtreeTest(test_flush, true, 1<<16, sm_options), 0);
}


// test_fetch
// this test case is policy-specific
// TODO: assuming N=10
w_rc_t test_fetch(ss_m *ssm, test_volume_t *) {

    logbuf_core *log_buffer = ((log_core*)ssm->log)->_log_buffer;

    // after startup
    EXPECT_EQ(lsn_t(1,1904), log_buffer->_to_insert_lsn);

    // fill up the first segment, except the last 4096 bytes
    W_DO(consume(4096-1904,ssm));
    for (int i=1; i<=SEG_SIZE/4096-1-1; i++) {
        W_DO(consume(4096,ssm));
    }

    // insert a log record that spans two segments (1, SEG_SIZE-4096)
    W_DO(consume(4096*2,ssm));

    // fill up the second segment
    for (int i=1; i<=SEG_SIZE/4096-1; i++) {
        W_DO(consume(4096,ssm));
    }

    // fill up the entire partition 1, except the last 4096 bytes
    for (int i=1; i<=(PART_SIZE-SEG_SIZE*2)/4096-1; i++) {
        W_DO(consume(4096,ssm));
    }

    // insert an 2048-byte log record so that the skip record is at (1, PART_SIZE-2048)
    W_DO(consume(2048,ssm));

    // fill up partition 2
    // there are two async checkpoints
    W_DO(consume(4096-528*2,ssm));
    sleep(5);
    for (int i=1; i<=PART_SIZE/4096-1; i++) {
        W_DO(consume(4096,ssm));
    }

    // fill up partition 3, , except the last segment
    // there are two async checkpoints
    W_DO(consume(4096-528*2,ssm));
    sleep(5);
    for (int i=1; i<=(PART_SIZE-SEG_SIZE)/4096-1; i++) {
        W_DO(consume(4096,ssm));
    }
    // the skip record for partition 3 is at (3, PART_SIZE)

    // then add some records to the lastest segment
    W_DO(x_begin_xct(ssm, true));
    W_DO(INSERT(4096));
    W_DO(INSERT(4096));
    W_DO(INSERT(4096));


    PRINT();
    // now the buffer looks like this
    // (3,SEG_SIZE*5) ->
    // (3,SEG_SIZE*6) -> (3,SEG_SIZE*7) -> (3,SEG_SIZE*8) -> (3,SEG_SIZE*9) -> (3,SEG_SIZE*10) ->
    // (3,SEG_SIZE*11) -> (3,SEG_SIZE*12) -> (3,SEG_SIZE*13) -> (3,SEG_SIZE*14)
    //PRINT();


    lsn_t ll;
    logrec_t *rp = NULL;

    // case 1: lsn is at offset 0 in a partition
    // miss
    ll = lsn_t(2,0);
    EXPECT_EQ(0, FETCH(ll));

    // hit
    ll = lsn_t(2,0);
    EXPECT_EQ(1, FETCH(ll));


    // case 2: lsn is at offset 0 in a segment,
    // but not at offset 0 in a partition
    // miss
    ll = lsn_t(2,SEG_SIZE);
    EXPECT_EQ(0, FETCH(ll));

    // hit
    ll = lsn_t(3,SEG_SIZE*13);
    EXPECT_EQ(1, FETCH(ll));


    // case 3: lsn is in the middle of a segment
    // 3.1: the log record is entirely contained within a segment
    // miss
    ll = lsn_t(2,SEG_SIZE*2+4096);
    EXPECT_EQ(0, FETCH(ll));

    // hit
    ll = lsn_t(3,SEG_SIZE*13+4096);
    EXPECT_EQ(1, FETCH(ll));

    // 3.2: the log record spans two segments
    // miss
    ll = lsn_t(1,SEG_SIZE-4096);
    EXPECT_EQ(0, FETCH(ll));

    // hit
    ll = lsn_t(1,SEG_SIZE-4096);
    EXPECT_EQ(1, FETCH(ll));

    // 3.3: the log record is a skip record
    // miss
    ll = lsn_t(1,PART_SIZE-2048);
    EXPECT_EQ(0, FETCH(ll));
    EXPECT_EQ(lsn_t(2,0), ll);

    // still miss due to fetch_for_test
    // internally it's a hit (see fetch)
    ll = lsn_t(1,PART_SIZE-2048);
    EXPECT_EQ(0, FETCH(ll));
    EXPECT_EQ(lsn_t(2,0), ll);


    // corner case 2
    // the lsn is less than to_insert but greater than or equal to to_flush
    // hit
    EXPECT_EQ(lsn_t(3,SEG_SIZE*14+3*4096), log_buffer->_to_insert_lsn);
    EXPECT_EQ(lsn_t(3,SEG_SIZE*14), log_buffer->_to_flush_lsn);

    ll = lsn_t(3,SEG_SIZE*14);
    EXPECT_EQ(1, FETCH(ll));

    ll = lsn_t(3,SEG_SIZE*14+2*4096);
    EXPECT_EQ(1, FETCH(ll));


    // corner case 1
    // the lsn is greater than or equal to to_insert
    // invalid
    EXPECT_EQ(lsn_t(3,SEG_SIZE*14+3*4096), log_buffer->_to_insert_lsn);

    ll = lsn_t(3,SEG_SIZE*14+3*4096);
    EXPECT_EQ(-1, FETCH(ll));
    ll = lsn_t(4,0);
    EXPECT_EQ(-1, FETCH(ll));


    // corner case 3
    // the lsn is the very end point of a partition and it's a skip record

    // miss
    ll = lsn_t(2,SEG_SIZE*15);
    EXPECT_EQ(0, FETCH(ll));
    EXPECT_EQ(lsn_t(3,0), ll);


    // still miss due to fetch_for_test
    // internally it's a hit (see fetch)
    ll = lsn_t(2,SEG_SIZE*15);
    EXPECT_EQ(0, FETCH(ll));
    EXPECT_EQ(lsn_t(3,0), ll);


    W_DO(x_commit_xct(ssm));


    return RCOK;
}


TEST (LogBufferTest2, Fetch) {
    test_env->empty_logdata_dir();
    sm_options sm_options;
    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);
    EXPECT_EQ(test_env->runBtreeTest(test_fetch, true, 1<<16, sm_options), 0);
}



// test_replacement
// this test case is policy-specific
class replacement_test_case  : public restart_test_base
{
public:
    w_rc_t pre_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((log_core*)ssm->log)->_log_buffer;

        // after startup
        EXPECT_EQ(lsn_t(1,1904), log_buffer->_to_insert_lsn);

        // fill up one segment 0
        W_DO(consume(4096-1904,ssm));
        for (int i=1; i<=SEG_SIZE/4096-1; i++) {
            W_DO(consume(4096,ssm));
        }

        // fill up one segment 1
        for (int i=1; i<=SEG_SIZE/4096; i++) {
            W_DO(consume(4096,ssm));
        }

        // fill up one segment 2
        for (int i=1; i<=SEG_SIZE/4096; i++) {
            W_DO(consume(4096,ssm));
        }

        // then add some records to the lastest segment
        // the final 528 bytes are reserved for the checkpoint during shutdown
        W_DO(consume(4096-528,ssm));


        return RCOK;
    }

    // when starting from an non-empty log, there are several log records inserted during startup
    // chkpt, abort, dismount, chkpt
    // (56 + 320 + 64 + 88) + 320 + 320 + (56 + 320 + 64 + 88) = 1696
    // one more async chkpt
    // 1696 + (56 + 320 + 64 + 88) = 2224
    w_rc_t post_shutdown(ss_m *ssm) {

        logbuf_core *log_buffer = ((log_core*)ssm->log)->_log_buffer;

        lsn_t ll;
        logrec_t *rp = NULL;



        // after startup
        sleep(5);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*3+4096+2224), log_buffer->_to_insert_lsn);

        // N=10, M=8

        // case 1: not full
        // initially only one segment
        W_DO(consume(4096-2224,ssm));
        for (int i=1; i<=(SEG_SIZE-4096*2)/4096-1; i++) {
            W_DO(consume(4096,ssm));
        }

        //EXPECT_EQ(lsn_t(1,SEG_SIZE*4-4096), log_buffer->_to_insert_lsn);

        EXPECT_EQ((uint32_t)1, log_buffer->_seg_list->count());
        EXPECT_EQ(log_buffer->_to_insert_seg, log_buffer->_to_flush_seg);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*3), log_buffer->_to_flush_seg->base_lsn);


        // insert
        W_DO(x_begin_xct(ssm, true));
        W_DO(INSERT(4096*2));

        // 2 segments now
        EXPECT_EQ((uint32_t)2, log_buffer->_seg_list->count());
        // the list looks like this: (1,SEG_SIZE*3) -> (1,SEG_SIZE*4)
        EXPECT_EQ(log_buffer->_to_flush_seg,
                  log_buffer->_seg_list->prev_of(log_buffer->_to_insert_seg));
        EXPECT_EQ(lsn_t(1,SEG_SIZE*3), log_buffer->_to_flush_seg->base_lsn);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*4), log_buffer->_to_insert_seg->base_lsn);


        // fetch
        // miss
        ll = lsn_t(1,4096);
        EXPECT_EQ(0, FETCH(ll));
        // 3 segments now
        EXPECT_EQ((uint32_t)3, log_buffer->_seg_list->count());
        // the list looks like this: (1,0) -> (1,SEG_SIZE*3) -> (1,SEG_SIZE*4)
        EXPECT_EQ(log_buffer->_seg_list->top(),
                  log_buffer->_seg_list->prev_of(log_buffer->_to_flush_seg));
        EXPECT_EQ(log_buffer->_to_flush_seg,
                  log_buffer->_seg_list->prev_of(log_buffer->_to_insert_seg));
        EXPECT_EQ(lsn_t(1,0), log_buffer->_seg_list->top()->base_lsn);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*3), log_buffer->_to_flush_seg->base_lsn);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*4), log_buffer->_to_insert_seg->base_lsn);


        // case 3: full, and forced flush

        // fill up the current segment
        for (int i=1; i<=SEG_SIZE/4096-1; i++) {
            W_DO(INSERT(4096));
        }

        // fill up 8 more segments
        for (int j=1; j<=8; j++) {
            for (int i=1; i<=SEG_SIZE/4096; i++) {
                W_DO(INSERT(4096));
            }
            //PRINT();
        }
        // 10 segments now
        EXPECT_EQ((uint32_t)10, log_buffer->_seg_list->count());
        // the list looks like this:
        // (1,SEG_SIZE*3) -> (1,SEG_SIZE*4) -> (1,SEG_SIZE*5) -> (1,SEG_SIZE*6)  -> (1,SEG_SIZE*7) ->
        // (1,SEG_SIZE*8) -> (1,SEG_SIZE*9) -> (1,SEG_SIZE*10) -> (1,SEG_SIZE*11)  -> (1,SEG_SIZE*12)

        EXPECT_EQ(log_buffer->_seg_list->top(), log_buffer->_to_flush_seg);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*3), log_buffer->_to_flush_seg->base_lsn);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*12), log_buffer->_to_insert_seg->base_lsn);

        // insert
        W_DO(INSERT(4096));

        // 10 segments now
        EXPECT_EQ((uint32_t)10, log_buffer->_seg_list->count());
        // the list looks like this:
        // (1,SEG_SIZE*4) -> (1,SEG_SIZE*5) -> (1,SEG_SIZE*6)  -> (1,SEG_SIZE*7) -> (1,SEG_SIZE*8) ->
        // (1,SEG_SIZE*9) -> (1,SEG_SIZE*10) -> (1,SEG_SIZE*11)  -> (1,SEG_SIZE*12) -> (1,SEG_SIZE*13)

        EXPECT_EQ(log_buffer->_seg_list->bottom(), log_buffer->_to_insert_seg);
        // the forced flush returns after flushing (1,SEG_SIZE*3)
        // but the flush daemon continues flushing in the background
        // so the _to_flush_seg is changing...
        EXPECT_LE(lsn_t(1,SEG_SIZE*3), log_buffer->_to_flush_seg->base_lsn);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*13), log_buffer->_to_insert_seg->base_lsn);


        // case 2: full, and no forced flush
        W_DO(FLUSH(lsn_t(1,SEG_SIZE*13)));

        // fill up the current segment
        for (int i=1; i<=SEG_SIZE/4096-1; i++) {
            W_DO(INSERT(4096));
        }

        // insert
        W_DO(INSERT(4096));

        // 10 segments now
        EXPECT_EQ((uint32_t)10, log_buffer->_seg_list->count());
        //(1,SEG_SIZE*5) -> (1,SEG_SIZE*6)  -> (1,SEG_SIZE*7) -> (1,SEG_SIZE*8) -> (1,SEG_SIZE*9) ->
        //(1,SEG_SIZE*10) -> (1,SEG_SIZE*11)  -> (1,SEG_SIZE*12) -> (1,SEG_SIZE*13) -> (1,SEG_SIZE*14)

        EXPECT_EQ(log_buffer->_seg_list->bottom(), log_buffer->_to_insert_seg);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*5), log_buffer->_seg_list->top()->base_lsn);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*13), log_buffer->_to_flush_seg->base_lsn);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*14), log_buffer->_to_insert_seg->base_lsn);

        // fetch
        // miss
        ll = lsn_t(1,4096);
        EXPECT_EQ(0, FETCH(ll));

        // 10 segments now
        EXPECT_EQ((uint32_t)10, log_buffer->_seg_list->count());
        //(1,0) -> (1,SEG_SIZE*6)  -> (1,SEG_SIZE*7) -> (1,SEG_SIZE*8) -> (1,SEG_SIZE*9) ->
        //(1,SEG_SIZE*10) -> (1,SEG_SIZE*11)  -> (1,SEG_SIZE*12) -> (1,SEG_SIZE*13) -> (1,SEG_SIZE*14)

        EXPECT_EQ(log_buffer->_seg_list->bottom(), log_buffer->_to_insert_seg);
        EXPECT_EQ(lsn_t(1,0), log_buffer->_seg_list->top()->base_lsn);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*13), log_buffer->_to_flush_seg->base_lsn);
        EXPECT_EQ(lsn_t(1,SEG_SIZE*14), log_buffer->_to_insert_seg->base_lsn);


        W_DO(x_commit_xct(ssm));

        return RCOK;
    }
};

TEST (LogBufferTest2, Replacement) {
    test_env->empty_logdata_dir();
    replacement_test_case context;
    restart_test_options options;
    sm_options sm_options;

    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, 1<<16, sm_options), 0);
}
#endif // LOG_BUFFER



// ========== test the entire system with the new log buffer (external states, single thread) ==========
#ifdef LOG_BUFFER


// test operations that mostly write regular log records
class test_writes : public restart_test_base
{
public:
    // total number of insert attempts
    static const int total = 10000; // if 100000, assertion failure: context.clockhand_current_depth >= depth
    static const int data_size = 64;
    static const int key_size = 64;
    char buf[data_size];
    char key[key_size];


    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));

        // insert
        for (int i=1; i<=total; i++) {
            test_env->itoa(i, key, 10);
            test_env->itoa(i, buf, 10);
            W_DO(test_env->btree_insert_and_commit(_stid_list[0], key, buf));
            W_DO(consume(4096,ssm));
        }

        for (int i=1; i<=total; i++) {
            test_env->itoa(i, key, 10);
            W_DO(consume(4096,ssm));

            if(i%1000==0) {
                // overwrite records whose key % 1000 == 0
                buf[0]='z';
                buf[1]='\0';
                W_DO(test_env->btree_overwrite_and_commit(_stid_list[0], key, buf, strlen(key)-1));
            }
            else {
                // update records whose key % 100 == 0 && key % 1000 != 0
                if(i%100==0) {
                    test_env->itoa(i+1, buf, 10);
                    W_DO(test_env->btree_update_and_commit(_stid_list[0], key, buf));
                }
                else {
                    // remove records whose key % 10 == 0 && key % 100 !=0 && key % 1000 != 0
                    // total/100*9 records are removed
                    if(i%10==0) {
                        W_DO(test_env->btree_remove_and_commit(_stid_list[0], key));
                    }
                }

            }
        }

        // do a check here
        post_shutdown(NULL);

        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (total - total/100*9, s.rownum);

        for (int i=1; i<=total; i++) {
            std::string result="";
            test_env->itoa(i, key, 10);
            if(i%1000==0) {
                // overwrite records whose key % 1000 == 0
                test_env->btree_lookup_and_commit(_stid_list[0], key, result);
                EXPECT_EQ('z', result[result.length()-1]);
                result[result.length()-1]='0';
                EXPECT_EQ(std::string(key), result);
            }
            else {
                if(i%100==0) {
                    // update records whose key % 100 == 0 && key % 1000 != 0
                    test_env->btree_lookup_and_commit(_stid_list[0], key, result);
                    test_env->itoa(i+1, buf, 10);
                    EXPECT_EQ(std::string(buf), result);
                }
                else {
                    if(i%10==0) {
                        // remove records whose key % 10 == 0 && key % 100 !=0 && key % 1000 != 0
                        // total/100*9 records are removed
                        result="data";
                        test_env->btree_lookup_and_commit(_stid_list[0], key, result);
                        // if not found, the result would become empty
                        EXPECT_EQ(true, result.empty());
                    }
                    else {
                        // non modified records
                        test_env->btree_lookup_and_commit(_stid_list[0], key, result);
                        EXPECT_EQ(std::string(key), result);
                    }
                }
            }
        }

        return RCOK;
    }
};

TEST (LogBufferTest3, Write) {
    test_env->empty_logdata_dir();
    test_writes context;
    restart_test_options options;
    sm_options sm_options;

    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, 1<<16, sm_options), 0);
}



// test operations that fetches log records (and may insert CLRs)

// test transaction abort

// rollback transactions with only one operation
class test_abort_simple : public restart_test_base
{
public:
    // total number of insert attempts
    static const int total = 10000; // if 100000, assertion failure: context.clockhand_current_depth >= depth
    static const int data_size = 64;
    static const int key_size = 64;
    char buf[data_size];
    char key[key_size];


    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));

        // insert
        for (int i=1; i<=total; i++) {
            test_env->itoa(i, key, 10);
            test_env->itoa(i, buf, 10);

            //W_DO(consume(4096,ssm));

            W_DO(test_env->begin_xct());
            W_DO(test_env->btree_insert(_stid_list[0], key, buf));

            // abort if the key ends with a 9
            // total/10 records are gone
            if (key[strlen(key)-1]=='9') {
                W_DO(test_env->abort_xct());
            }
            else {
                W_DO(test_env->commit_xct());
            }
        }

        for (int i=1; i<=total; i++) {
            test_env->itoa(i, key, 10);

            //W_DO(consume(4096,ssm));

            W_DO(test_env->begin_xct());
            if(i%1000==0) {
                // overwrite records whose key % 1000 == 0
                buf[0]='z';
                buf[1]='\0';
                W_DO(test_env->btree_overwrite(_stid_list[0], key, buf, strlen(key)-1));
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
                if(i%100==0) {
                    test_env->itoa(i+1, buf, 10);
                    W_DO(test_env->btree_update(_stid_list[0], key, buf));
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
                    if(i%10==0) {
                        W_DO(test_env->btree_remove(_stid_list[0], key));
                        // abort if the second to last character is 9
                        if (key[strlen(key)-2]=='9') {
                            W_DO(test_env->abort_xct());
                        }
                        else {
                            // total/100*8 records are removed
                            W_DO(test_env->commit_xct());
                        }
                    }
                    else {
                        W_DO(test_env->commit_xct());
                    }
                }
            }
        }

        // do a check here
        post_shutdown(NULL);

        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (total - total/10 - total/100*8, s.rownum);

        for (int i=1; i<=total; i++) {
            std::string result="";
            test_env->itoa(i, key, 10);
            if(i%1000==0) {
                // overwrite records whose key % 1000 == 0
                test_env->btree_lookup_and_commit(_stid_list[0], key, result);
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
                    test_env->btree_lookup_and_commit(_stid_list[0], key, result);
                    // abort if the second to last character is 9
                    if (key[strlen(key)-2]=='9') {
                        EXPECT_EQ(std::string(key), result);
                    }
                    else {
                        test_env->itoa(i+1, buf, 10);
                        EXPECT_EQ(std::string(buf), result);
                    }
                }
                else {
                    if(i%10==0) {
                        // remove records whose key % 10 == 0 && key % 100 !=0 && key % 1000 != 0
                        // total/100*9 records are removed
                        result="data";
                        test_env->btree_lookup_and_commit(_stid_list[0], key, result);
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
                        test_env->btree_lookup_and_commit(_stid_list[0], key, result);
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
};

TEST (LogBufferTest3, AbortSimple) {
    test_env->empty_logdata_dir();
    test_abort_simple context;
    restart_test_options options;
    sm_options sm_options;

    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, 1<<16, sm_options), 0);
}


// rollback a big running transaction
class test_abort_big : public restart_test_base
{
public:
    // total number of insert attempts
    static const int total = 10000; // if 100000, assertion failure: context.clockhand_current_depth >= depth
    static const int data_size = 64;
    static const int key_size = 64;
    char buf[data_size];
    char key[key_size];


    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));


        W_DO(test_env->begin_xct());

        // insert
        for (int i=1; i<=total; i++) {
            test_env->itoa(i, key, 10);
            test_env->itoa(i, buf, 10);
            W_DO(test_env->btree_insert(_stid_list[0], key, buf));
        }

        for (int i=1; i<=total; i++) {
            test_env->itoa(i, key, 10);

            if(i%1000==0) {
                // overwrite records whose key % 1000 == 0
                buf[0]='z';
                buf[1]='\0';
                W_DO(test_env->btree_overwrite(_stid_list[0], key, buf, strlen(key)-1));
            }
            else {
                // update records whose key % 100 == 0 && key % 1000 != 0
                if(i%100==0) {
                    test_env->itoa(i+1, buf, 10);
                    W_DO(test_env->btree_update(_stid_list[0], key, buf));
                }
                else {
                    // remove records whose key % 10 == 0 && key % 100 !=0 && key % 1000 != 0
                    if(i%10==0) {
                        W_DO(test_env->btree_remove(_stid_list[0], key));
                    }
                }
            }
        }


        W_DO(test_env->abort_xct());


        // do a check here
        post_shutdown(NULL);

        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (0, s.rownum);

        return RCOK;
    }
};

TEST (LogBufferTest3, AbortBig) {
    test_env->empty_logdata_dir();
    test_abort_big context;
    restart_test_options options;
    sm_options sm_options;

    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, 1<<16, sm_options), 0);
}



// rollback a long running transaction

pthread_mutex_t wait_comment_lock;
pthread_cond_t wait_cond;
pthread_cond_t comment_cond;

bool waiting_for_comment;
bool shutting_down;

#define WAIT_FOR_COMMENT_THREAD() \
    {\
    CRITICAL_SECTION(cs, wait_comment_lock);    \
    waiting_for_comment = true;                         \
    DO_PTHREAD(pthread_cond_signal(&comment_cond));                     \
    DO_PTHREAD(pthread_cond_wait(&wait_cond, &wait_comment_lock));      \
    }


class test_abort_long;

// a log comment thread inserts log records between every two opeartions performed by the main thread
class log_comment_thread_t : public smthread_t {
public:
    log_comment_thread_t(test_abort_long *owner_)
        : smthread_t(t_regular, "log_comment_thread_t"), active(true), owner(owner_)
    {
    }
    virtual void run() {
        _rc = run_core();
        active = false;
    }

    rc_t run_core();

    int  return_value() const { return 0; }

    rc_t _rc;
    bool active;

    test_abort_long *owner;

};



class test_abort_long : public restart_test_base
{
public:
    static const int total = 100;  //better be small
    static const int interval = 512; //approximated amount (KB) of log records inserted by the comment thread


    static const int data_size = 64;
    static const int key_size = 64;
    char buf[data_size];
    char key[key_size];


    w_rc_t pre_shutdown(ss_m *ssm) {
        _stid_list = new stid_t[1];
        W_DO(x_btree_create_index(ssm, &_volume, _stid_list[0], _root_pid));


        // init locks and conds
        DO_PTHREAD(pthread_mutex_init(&wait_comment_lock, NULL));
        DO_PTHREAD(pthread_cond_init(&wait_cond, NULL));
        DO_PTHREAD(pthread_cond_init(&comment_cond, NULL));

        waiting_for_comment = false;
        shutting_down = false;

        // start the log comment thread
        log_comment_thread_t *comment = new log_comment_thread_t(this);
        W_DO(comment->fork());


        // perform workload
        W_DO(test_env->begin_xct());

        // insert
        for (int i=1; i<=total; i++) {
            test_env->itoa(i, key, 10);
            test_env->itoa(i, buf, 10);
            W_DO(test_env->btree_insert(_stid_list[0], key, buf));
            WAIT_FOR_COMMENT_THREAD();
        }

        for (int i=1; i<=total; i++) {
            test_env->itoa(i, key, 10);

            if(i%1000==0) {
                // overwrite records whose key % 1000 == 0
                buf[0]='z';
                buf[1]='\0';
                W_DO(test_env->btree_overwrite(_stid_list[0], key, buf, strlen(key)-1));
                WAIT_FOR_COMMENT_THREAD();
            }
            else {
                // update records whose key % 100 == 0 && key % 1000 != 0
                if(i%100==0) {
                    test_env->itoa(i+1, buf, 10);
                    W_DO(test_env->btree_update(_stid_list[0], key, buf));
                    WAIT_FOR_COMMENT_THREAD();
                }
                else {
                    // remove records whose key % 10 == 0 && key % 100 !=0 && key % 1000 != 0
                    if(i%10==0) {
                        W_DO(test_env->btree_remove(_stid_list[0], key));
                        WAIT_FOR_COMMENT_THREAD();
                    }
                }
            }
        }


        W_DO(test_env->abort_xct());


        // shutdown the log comment thread
        shutting_down = true;
        while (shutting_down == true) {
            DO_PTHREAD(pthread_cond_broadcast(&comment_cond));
        }

        comment->join();
        delete comment;



        // do a check here
        post_shutdown(NULL);

        return RCOK;
    }

    w_rc_t post_shutdown(ss_m *) {
        x_btree_scan_result s;
        W_DO(test_env->btree_scan(_stid_list[0], s));
        EXPECT_EQ (0, s.rownum);

        return RCOK;
    }
};


rc_t log_comment_thread_t::run_core() {

    int interval = owner->interval;


    bool done = false;
    uint64_t total = 0;

    int buf_size = 4; // KB
    int buf_bytes = 4*1024; // Bytes
    char buf[buf_bytes];
    memset(buf, 'z', buf_bytes);
    buf[buf_bytes-1] = '\0';

    while (true) {

        {
            CRITICAL_SECTION(cs, wait_comment_lock);
            if (done == true) {
                waiting_for_comment = false;
                done = false;
                DO_PTHREAD(pthread_cond_broadcast(&wait_cond));
            }

            if (shutting_down) {
                shutting_down = false;
                break;
            }

            if (waiting_for_comment == false) {
                DO_PTHREAD(pthread_cond_wait(&comment_cond, &wait_comment_lock));
            }

        }

        int goal = interval;

        W_DO(ss_m::begin_xct());
        while(goal>0) {
            if (goal <= 5*1024) {
                W_DO(ss_m::commit_xct());
                W_DO(ss_m::begin_xct());
            }
            W_DO(ss_m::log_message(buf));
            goal-=buf_size;
        }
        W_DO(ss_m::commit_xct());

        done = true;

    }


    std::cout << "LogComment" << " " << total << " KB" << std::endl;

    std::cout << "LogComment done" << std::endl;

    return RCOK;
}


TEST (LogBufferTest3, AbortLong) {
    test_env->empty_logdata_dir();
    test_abort_long context;
    restart_test_options options;
    sm_options sm_options;

    sm_options.set_int_option("sm_logbufsize", SEG_SIZE);
    sm_options.set_int_option("sm_logsize", LOG_SIZE);

    options.shutdown_mode = normal_shutdown;
    options.restart_mode = m1_default_restart;
    EXPECT_EQ(test_env->runRestartTest(&context, &options, true, 1<<16, sm_options), 0);
}

#endif // LOG_BUFFER



int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    test_env = new btree_test_env();
    ::testing::AddGlobalTestEnvironment(test_env);
    return RUN_ALL_TESTS();
}


