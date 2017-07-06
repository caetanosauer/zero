#ifndef LOG_CONSUMER_H
#define LOG_CONSUMER_H

#include <bitset>

#include "worker_thread.h"
#include "ringbuffer.h"
#include "log_storage.h"

/** \brief Parses log records from a stream of binary data.
 *
 * This class is not a scanner per se, as it does not perform any I/O.
 * However, it enables efficient scanning by reading whole blocks of binary
 * data from the recovery log and parsing log records from them using this
 * class. This is an improvement over the traditional "scan" implemented in
 * Shore-MT, which performs one random read for each log record.  The major
 * task implemented in this class is the control of block boundaries, which can
 * occur in the middle of a log record (i.e., log records may spawn multiple
 * blocks). To that end, it maintains an internal log record buffer to
 * reconstruct such log records.
 *
 * Log records are delivered via the nextLogrec() method, which takes the block
 * address and offset within the block as parameters. Once the log record is
 * parsed, the offset is updated (i.e., it is an output parameter). The method
 * returns true if the log record was found entirely in the current block.
 * Otherwise, it saves the partial data into its internal buffer and returns
 * false, indicating to the caller that a new block must be provided. Upon
 * invoking nextLogrec() once again, the caller then receives the complete log
 * record.
 *
 * \author Caetano Sauer
 */
class LogScanner {
public:
    bool nextLogrec(char* src, size_t& pos, logrec_t*& lr,
            lsn_t* nextLSN = NULL, lsn_t* stopLSN = NULL,
            int* lrLength = NULL);

    bool hasPartialLogrec();
    void reset();

    LogScanner(size_t blockSize)
        : truncCopied(0), truncMissing(0), toSkip(0), blockSize(blockSize)
    {
        // maximum logrec size = 3 pages
        truncBuf = new char[3 * log_storage::BLOCK_SIZE];
    }

    ~LogScanner() {
        delete[] truncBuf;
    }

    size_t getBlockSize() {
        return blockSize;
    }

    void setIgnore(kind_t type) {
        ignore.set(type);
    }

    void ignoreAll() {
        ignore.set();
    }

    void unsetIgnore(kind_t type) {
        ignore.reset(type);
    }

    bool isIgnored(kind_t type) {
        return ignore[type];
    }

private:
    size_t truncCopied;
    size_t truncMissing;
    size_t toSkip;
    const size_t blockSize;
    char* truncBuf;
    bitset<t_max_logrec> ignore;
};

/** \brief Object to control execution of background threads.
 *
 * Encapsulates an activation loop that relies on pthread condition variables.
 * The background thread calls waitForActivation() while it waits for an
 * activation from an orchestrating thread. Before calling this method,
 * however, it must acquire the mutex, in order to obey the pthread wait
 * protocol.  Once its work is complete, the activated state is unsed and the
 * mutex must be released. In practice, therefore, waitForActivation() is
 * usually invoked as follows.
 *
 * \code{.cpp}
    while (true) {
        CRITICAL_SECTION(cs, control.mutex);
        bool activated = control.waitForActivation();
        if (!activated) {
            break;
        }

        // do work...

    } // mutex released due to CRITICAL_SECTION macro
 * \endcode
 *
 * The wait for an activation is interrupted either by receiving a signal or by
 * setting a shutdown flag, in which case the method returns false. The flag is
 * simply a pointer to some external boolean variable, which means that the
 * background thread "watches for" a shutdown flag somewhere else. The
 * orchestrating thread calls activate() to wake up the background thread,
 * causing the waitForActivation() call to return with true.  The wait
 * parameter makes the activation wait to acquire the mutex, which guarantees
 * that the signal was sent. Otherwise, if the mutex is already held (i.e.,
 * background thread is already running) the method returns false immediately.
 *
 * Optionally, the activate method accepts an lsn, which is stored in endLSN,
 * but only if it is greater than the currently set value. This class does not
 * interpret this LSN value. It is only used by the background thread itself
 * as a marker for the end of its job. This is useful for threads that consume
 * its owrk units from the log, such as LogArchiver or ReaderThread. Other
 * thread classes may completely ignore this variable.
 *
 * \sa LogArchiver, LogArchiver::ReaderThread
 *
 * \author Caetano Sauer
 */
struct ArchiverControl {
    pthread_mutex_t mutex;
    pthread_cond_t activateCond;
    lsn_t endLSN;
    bool activated;
    bool listening;
    std::atomic<bool>* shutdownFlag;

    ArchiverControl(std::atomic<bool>* shutdown);
    ~ArchiverControl();
    bool activate(bool wait, lsn_t lsn = lsn_t::null);
    bool waitForActivation();
};

/** \brief Asynchronous reader thread for the recovery log.
 *
 * Similarly to the LogArchiver itself, this thread operates on activation
 * cycles based on given "end LSN" values (see LogArchiver). This thread is
 * controlled by LogArchiver::LogConsumer
 *
 * The recovery log is read one block at a time, and each block is placed
 * on an asynchronous ring buffer (see AsyncRingBuffer). Once the buffer is
 * full, it automatically blocks waiting for a free slot.
 *
 * Once the thread is shutdown, it exits its current/next wait and marks
 * the buffer as "finished", which makes consumers stop waiting for new
 * blocks once the buffer is empty.
 *
 * \author Caetano Sauer
 */
class ReaderThread : public log_worker_thread_t {
protected:
    uint nextPartition;
    rc_t openPartition();

    AsyncRingBuffer* buf;
    int currentFd;
    off_t pos;
    lsn_t localEndLSN;

public:
    virtual void do_work();

    ReaderThread(AsyncRingBuffer* readbuf, lsn_t startLSN);
    virtual ~ReaderThread() {}

    size_t getBlockSize() { return buf->getBlockSize(); }
};

/** \brief Provides a record-at-a-time interface to the recovery log using
 * asynchronous read operations.
 *
 * This class manages an asynchronous reader thread (see ReaderThread) and
 * the corresponding read buffer (see AsyncRingBuffer). It provides a
 * record-at-a-time synchronous interface to the caller. It is used to read
 * log records from the recovery log and push them into the archiver heap.
 *
 * Access requires a preliminary call to the open() method, which activates
 * the reader thread with the given end LSN. Otherwise, the next() method
 * bay block indefinitely.
 *
 * next() returns false when it reaches the end LSN, implying that the
 * returned log record is invalid.
 *
 * \author Caetano Sauer
 */
class LogConsumer {
public:
    LogConsumer(lsn_t startLSN, size_t blockSize, bool ignore = true);
    virtual ~LogConsumer();
    void shutdown();

    void open(lsn_t endLSN, bool readWholeBlocks = false);
    bool next(logrec_t*& lr);
    lsn_t getNextLSN() { return nextLSN; }

    static void initLogScanner(LogScanner* logScanner);

private:
    AsyncRingBuffer* readbuf;
    ReaderThread* reader;
    LogScanner* logScanner;

    lsn_t nextLSN;
    lsn_t endLSN;

    char* currentBlock;
    size_t blockSize;
    size_t pos;
    bool readWholeBlocks;

    bool nextBlock();
};


#endif
