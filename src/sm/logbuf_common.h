/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef LOGBUF_COMMON_H
#define LOGBUF_COMMON_H

// switch for the new log buffer
// define LOG_BUFFER when using the new log buffer
// disable LOG_BUFFER when using the original buffer
// disable LOG_BUFFER when testing the standalone log buffer
//#define LOG_BUFFER // CS: disabled until refactoring is complete


// direct IO switch
// works for both org and new log buffer
// disable LOG_DIRECT_IO when testing the standalone log buffer
//#define LOG_DIRECT_IO


// enable verification of flushes
//#define LOG_VERIFY_FLUSHES


// since the current implementation uses files in the local file system as log partitions
// with direct IO, every read and write must be aligned to the FS block size (4096 in xfs)
// if a raw device is used, we should use the physical block size (e.g., 512)
// always keep this on
#define LOG_DIO_ALIGN 4096



// basic parameters for the log buffer
// these are default values, NOT magic numbers
const uint32_t LOGBUF_SEG_COUNT = 10;  // max number of segments in the log buffer

// max number of "dirty" segments in the write buffer 
// if the number is reached when the log buffer is full, 
// a forced flush is triggered to flush all "dirty" log records to the log 
// NOTE that 2 segments are reserved for _to_archive_seg and _to_flush_seg
// so this parameter cannot be greater than LOGBUF_SEG_COUNT - 2
const uint32_t LOGBUF_FLUSH_TRIGGER = LOGBUF_SEG_COUNT - 2;  

// default block size: 8KB
const uint32_t LOGBUF_BLOCK_SIZE = 8192;

// default segment size: 1MB
const uint32_t LOGBUF_SEG_SIZE = 128 * LOGBUF_BLOCK_SIZE;  

// default partition size
// it's actually calculated in log_core::_set_size
// it must be an integral number of _segsize
const uint32_t LOGBUF_PART_SIZE = 128 * LOGBUF_SEG_SIZE;  

// hints for fetch, not used for now
enum hints_op {
    LOG_ARCHIVING=0, 
    SINGLE_PAGE_RECOVERY, 
    TRANSACTION_ROLLBACK, 
    LOG_ANALYSIS_FORWARD,
    LOG_ANALYSIS_BACKWARD,
    TRADITIONAL_REDO,
    TRADITIONAL_UNDO,
    PAGE_DRIVEN_REDO,
    TRANSACTION_DRIVEN_UNDO,
    ON_DEMAND_REDO,
    ON_DEMAND_UNDO,
    DEFAULT_HINTS,
};


#endif // LOGBUF_COMMON_H
