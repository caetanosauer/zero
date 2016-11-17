#include "w_defines.h"

#define SM_SOURCE
#define LOGARCHIVER_C

#include "logarchiver.h"
#include "sm_options.h"
#include "log_core.h"

#include <algorithm>
#include <sm_base.h>
#include <sstream>
#include <sys/stat.h>
#include <boost/regex.hpp>

#include "stopwatch.h"

typedef mem_mgmt_t::slot_t slot_t;

// definition of static members
const string LogArchiver::ArchiveDirectory::RUN_PREFIX = "archive_";
const string LogArchiver::ArchiveDirectory::CURR_RUN_FILE = "current_run";
const string LogArchiver::ArchiveDirectory::CURR_MERGE_FILE = "current_merge";
const string LogArchiver::ArchiveDirectory::run_regex =
    "^archive_[1-9][0-9]*\\.[0-9]+-[1-9][0-9]*\\.[0-9]+$";
const string LogArchiver::ArchiveDirectory::current_regex = "current_run|current_merge";

// CS: Aligning with the Linux standard FS block size
// We could try using 512 (typical hard drive sector) at some point,
// but none of this is actually standardized or portable
const size_t LogArchiver::IO_ALIGN = 512;

#include "logdef_gen.cpp"
baseLogHeader SKIP_LOGREC;

typedef LogArchiver::ArchiveIndex::ProbeResult ProbeResult;

ArchiverControl::ArchiverControl(bool* shutdownFlag)
    : endLSN(lsn_t::null), activated(false), listening(false), shutdownFlag(shutdownFlag)
{
    DO_PTHREAD(pthread_mutex_init(&mutex, NULL));
    DO_PTHREAD(pthread_cond_init(&activateCond, NULL));
}

ArchiverControl::~ArchiverControl()
{
    DO_PTHREAD(pthread_mutex_destroy(&mutex));
    DO_PTHREAD(pthread_cond_destroy(&activateCond));
}

bool ArchiverControl::activate(bool wait, lsn_t lsn)
{
    if (wait) {
        DO_PTHREAD(pthread_mutex_lock(&mutex));
    }
    else {
        if (pthread_mutex_trylock(&mutex) != 0) {
            return false;
        }
    }
    // now we hold the mutex -- signal archiver thread and set endLSN

    /* Make sure signal is sent only if thread is listening.
     * TODO: BUG? The mutex alone cannot guarantee that the signal is not lost,
     * since the activate call may happen before the thread ever starts
     * listening. If we ever get problems with archiver getting stuck, this
     * would be one of the first things to try. We could, e.g., replace
     * the listening flag with something like "gotSignal" and loop this
     * method until it's true.
     */
    // activation may not decrease the endLSN
    w_assert0(lsn >= endLSN);
    endLSN = lsn;
    activated = true;
    DO_PTHREAD(pthread_cond_signal(&activateCond));
    DO_PTHREAD(pthread_mutex_unlock(&mutex));

    /*
     * Returning true only indicates that signal was sent, and not that the
     * archiver thread is running with the given endLSN. Another thread
     * calling activate may get the mutex before the log archiver and set
     * another endLSN. In fact, it does not even mean that the signal was
     * received, since the thread may not be listening yet.
     */
    return activated;
}

bool ArchiverControl::waitForActivation()
{
    // WARNING: mutex must be held by caller!
    listening = true;
    while(!activated) {
        struct timespec timeout;
        sthread_t::timeout_to_timespec(100, timeout); // 100ms
        int code = pthread_cond_timedwait(&activateCond, &mutex, &timeout);
        if (code == ETIMEDOUT) {
            //DBGTHRD(<< "Wait timed out -- try again");
            if (*shutdownFlag) {
                DBGTHRD(<< "Activation failed due to shutdown. Exiting");
                return false;
            }
        }
        DO_PTHREAD_TIMED(code);
    }
    listening = false;
    return true;
}

LogArchiver::ReaderThread::ReaderThread(AsyncRingBuffer* readbuf,
        lsn_t startLSN)
    :
      BaseThread(readbuf, "LogArchiver_ReaderThread"),
      shutdownFlag(false), control(&shutdownFlag)
{
    // position initialized to startLSN
    pos = startLSN.lo();
    nextPartition = startLSN.hi();
}

void LogArchiver::ReaderThread::shutdown()
{
    shutdownFlag = true;
    // make other threads see new shutdown value
    lintel::atomic_thread_fence(lintel::memory_order_release);
}

void LogArchiver::ReaderThread::activate(lsn_t endLSN)
{
    // pos = startLSN.lo();
    DBGTHRD(<< "Activating reader thread until " << endLSN);
    control.activate(true, endLSN);
}

rc_t LogArchiver::ReaderThread::openPartition()
{
    if (currentFd != -1) {
        W_DO(me()->close(currentFd));
    }
    currentFd = -1;

    // open file for read -- copied from partition_t::peek()
    int fd;
    string fname = smlevel_0::log->make_log_name(nextPartition);

    int flags = smthread_t::OPEN_RDONLY;
    W_COERCE(me()->open(fname.c_str(), flags, 0744, fd));

    sthread_base_t::filestat_t statbuf;
    W_DO(me()->fstat(fd, statbuf));
    if (statbuf.st_size == 0) {
        return RC(eEOF);
    }
    sm_diskaddr_t partSize = statbuf.st_size;
    /*
     * The size of the file must be at least the offset of endLSN, otherwise
     * the given endLSN was incorrect. If this is not the partition of
     * endLSN.hi(), then we simply assert that its size is not zero.
     */
    if (control.endLSN.hi() == nextPartition) {
        w_assert0(partSize >= control.endLSN.lo());
    }
    else {
        w_assert1(partSize > 0);
    }

    DBGTHRD(<< "Opened log partition for read " << fname);

    currentFd = fd;
    nextPartition++;
    return RCOK;
}

void LogArchiver::ReaderThread::run()
{
    while(true) {
        CRITICAL_SECTION(cs, control.mutex);

        bool activated = control.waitForActivation();
        if (!activated) {
            break;
        }

        lintel::atomic_thread_fence(lintel::memory_order_release);
        if (shutdownFlag) {
            break;
        }

        DBGTHRD(<< "Reader thread activated until " << control.endLSN);

        /*
         * CS: The code was changed to not rely on the file size anymore,
         * because we may read from a file that is still being appended to.
         * The correct behavior is to rely on the given endLSN, which must
         * be guaranteed to be persistent on the file. Therefore, we cannot
         * read past the end of the file if we only read until endLSN.
         * A physical read past the end is OK because we use pread_short().
         * The position from which the first logrec will be read is set in pos
         * by the activate method, which takes the startLSN as parameter.
         */

        while(true) {
            unsigned currPartition =
                currentFd == -1 ? nextPartition : nextPartition - 1;
            if (control.endLSN.hi() == currPartition
                    && pos >= control.endLSN.lo())
            {
                /*
                 * The requested endLSN is within a block which was already
                 * read. Stop and wait for next activation, which must start
                 * reading from endLSN, since anything beyond that might
                 * have been updated alread (usually, endLSN is the current
                 * end of log). Hence, we update pos with it.
                 */
                pos = control.endLSN.lo();
                DBGTHRD(<< "Reader thread reached endLSN -- sleeping."
                        << " New pos = " << pos);
                break;
            }

            // get buffer space to read into
            char* dest = buf->producerRequest();
            if (!dest) {
                W_FATAL_MSG(fcINTERNAL,
                     << "Error requesting block on reader thread");
                break;
            }


            if (currentFd == -1) {
                W_COERCE(openPartition());
            }

            // Read only the portion which was ignored on the last round
            size_t blockPos = pos % blockSize;
            int bytesRead = 0;
            W_COERCE(me()->pread_short(
                        currentFd, dest + blockPos, blockSize - blockPos,
                        pos, bytesRead));

            if (bytesRead == 0) {
                // Reached EOF -- open new file and try again
                DBGTHRD(<< "Reader reached EOF (bytesRead = 0)");
                W_COERCE(openPartition());
                pos = 0;
                blockPos = 0;
                W_COERCE(me()->pread_short(
                            currentFd, dest, blockSize, pos, bytesRead));
                if (bytesRead == 0) {
                    W_FATAL_MSG(fcINTERNAL,
                        << "Error reading from partition "
                        << nextPartition - 1);
                }
            }

            DBGTHRD(<< "Read block " << (void*) dest << " from fpos " << pos <<
                    " with size " << bytesRead << " into blockPos "
                    << blockPos);
            w_assert0(bytesRead > 0);

            pos += bytesRead;
            buf->producerRelease();
        }

        control.activated = false;
    }
}

void LogArchiver::WriterThread::run()
{
    DBGTHRD(<< "Writer thread activated");

    while(true) {
        char* src = buf->consumerRequest();
        if (!src) {
            /* Is the finished flag necessary? Yes.
             * The reader thread stops once it reaches endLSN, and then it
             * sleeps and waits for the next activate signal. The writer
             * thread, on the other hand, does not need an activation signal,
             * because it runs indefinitely, just waiting for blocks to be
             * written. The only stop condition is when the write buffer itself
             * is marked finished, which is done in shutdown().
             * Nevertheless, a null block is only returned once the finished
             * flag is set AND there are no more blocks. Thus, we gaurantee
             * that all pending blocks are written out before shutdown.
             */
            DBGTHRD(<< "Finished flag set on writer thread");
            W_COERCE(directory->closeCurrentRun(maxLSNInRun));
            return; // finished is set on buf
        }

        DBGTHRD(<< "Picked block for write " << (void*) src);

        run_number_t run = BlockAssembly::getRunFromBlock(src);
        if (currentRun != run) {
            // when writer is restarted, currentRun resets to zero
            w_assert1(currentRun == 0 || run == currentRun + 1);
            /*
             * Selection (producer) guarantees that logrec fits in block.
             * lastLSN is the LSN of the first log record in the new block
             * -- it will be used as the upper bound when renaming the file
             *  of the current run. This same LSN will be used as lower
             *  bound on the next run, which allows us to verify whether
             *  holes exist in the archive.
             */
            W_COERCE(directory->closeCurrentRun(maxLSNInRun));
            w_assert1(directory->getLastLSN() == maxLSNInRun);
            currentRun = run;
            maxLSNInRun = lsn_t::null;
            DBGTHRD(<< "Opening file for new run " << run
                    << " starting on LSN " << directory->getLastLSN());
        }

        lsn_t blockLSN = BlockAssembly::getLSNFromBlock(src);
        if (blockLSN > maxLSNInRun) {
            maxLSNInRun = blockLSN;
        }

        size_t blockEnd = BlockAssembly::getEndOfBlock(src);
        size_t actualBlockSize= blockEnd - sizeof(BlockAssembly::BlockHeader);
        memmove(src, src + sizeof(BlockAssembly::BlockHeader), actualBlockSize);

        W_COERCE(directory->append(src, actualBlockSize));

        DBGTHRD(<< "Wrote out block " << (void*) src
                << " with max LSN " << blockLSN);

        buf->consumerRelease();
    }
}

LogArchiver::LogArchiver(
        ArchiveDirectory* d, LogConsumer* c, ArchiverHeap* h, BlockAssembly* b)
    :
    smthread_t(t_regular, "LogArchiver"),
    directory(d), consumer(c), heap(h), blkAssemb(b),
    shutdownFlag(false), control(&shutdownFlag), selfManaged(false),
    flushReqLSN(lsn_t::null)
{
    nextActLSN = directory->getStartLSN();
}

LogArchiver::LogArchiver(const sm_options& options)
    : smthread_t(t_regular, "LogArchiver"),
    shutdownFlag(false), control(&shutdownFlag), selfManaged(true),
    flushReqLSN(lsn_t::null)
{
    size_t workspaceSize = 1024 * 1024 * // convert MB -> B
        options.get_int_option("sm_archiver_workspace_size", DFT_WSPACE_SIZE);
    size_t blockSize = DFT_BLOCK_SIZE;
    // CS TODO: archiver currently only works with 1MB blocks
        // options.get_int_option("sm_archiver_block_size", DFT_BLOCK_SIZE);

    eager = options.get_bool_option("sm_archiver_eager", DFT_EAGER);
    readWholeBlocks = options.get_bool_option(
            "sm_archiver_read_whole_blocks", DFT_READ_WHOLE_BLOCKS);
    slowLogGracePeriod = options.get_int_option(
            "sm_archiver_slow_log_grace_period", DFT_GRACE_PERIOD);

    directory = new ArchiveDirectory(options);
    nextActLSN = directory->getStartLSN();

    consumer = new LogConsumer(directory->getStartLSN(), blockSize);
    heap = new ArchiverHeap(workspaceSize);
    blkAssemb = new BlockAssembly(directory);
}

void LogArchiver::initLogScanner(LogScanner* logScanner)
{
    logScanner->setIgnore(logrec_t::t_comment);
    logScanner->setIgnore(logrec_t::t_compensate);
    logScanner->setIgnore(logrec_t::t_chkpt_begin);
    logScanner->setIgnore(logrec_t::t_chkpt_bf_tab);
    logScanner->setIgnore(logrec_t::t_chkpt_xct_tab);
    logScanner->setIgnore(logrec_t::t_chkpt_xct_lock);
    logScanner->setIgnore(logrec_t::t_chkpt_backup_tab);
    logScanner->setIgnore(logrec_t::t_chkpt_end);
    logScanner->setIgnore(logrec_t::t_chkpt_restore_tab);
    logScanner->setIgnore(logrec_t::t_xct_abort);
    logScanner->setIgnore(logrec_t::t_xct_end);
    logScanner->setIgnore(logrec_t::t_xct_freeing_space);
    logScanner->setIgnore(logrec_t::t_restore_begin);
    logScanner->setIgnore(logrec_t::t_restore_segment);
    logScanner->setIgnore(logrec_t::t_restore_end);
    logScanner->setIgnore(logrec_t::t_tick_sec);
    logScanner->setIgnore(logrec_t::t_tick_msec);
    logScanner->setIgnore(logrec_t::t_page_read);
    logScanner->setIgnore(logrec_t::t_page_write);
}

/*
 * Shutdown sets the finished flag on read and write buffers, which makes
 * the reader and writer threads finish processing the current block and
 * then exit. The replacement-selection will exit as soon as it requests
 * a block and receives a null pointer. If the shutdown flag is set, the
 * method exits without error.
 *
 * Thread safety: since all we do with the shutdown flag is set it to true,
 * we do not worry about race conditions. A memory barrier is also not
 * required, because other threads don't have to immediately see that
 * the flag was set. As long as it is eventually set, it is OK.
 */
void LogArchiver::shutdown()
{
    // CS TODO BUG: we need some sort of pin mechanism (e.g., shared_ptr) for shutdown,
    // because threads may still be accessing the log archive here.
    DBGTHRD(<< "LOG ARCHIVER SHUTDOWN STARTING");
    // this flag indicates that reader and writer threads delivering null
    // blocks is not an error, but a termination condition
    shutdownFlag = true;
    // make other threads see new shutdown value
    lintel::atomic_thread_fence(lintel::memory_order_release);
    join();
    consumer->shutdown();
    blkAssemb->shutdown();
}

LogArchiver::~LogArchiver()
{
    if (!shutdownFlag) {
        shutdown();
    }
    if (selfManaged) {
        delete blkAssemb;
        delete consumer;
        delete heap;
        delete directory;
    }
}

/**
 * Extracts one LSN from a run's file name.
 * If end == true, get the end LSN (the upper bound), otherwise
 * get the begin LSN.
 */
lsn_t LogArchiver::ArchiveDirectory::parseLSN(const char* str, bool end)
{
    char delim = end ? '-' : '_';
    const char* hpos = strrchr(str, delim) + 1;
    const char* lpos = strchr(hpos, '.') + 1;

    // the highest 48-bit integer has 15 digits
    char* hstr = new char[15];
    memset(hstr, 0, 15);
    strncpy(hstr, hpos, lpos - hpos - 1);
    char* lstr = (char*) lpos;
    if (!end) {
        lstr = new char[15];
        memset(lstr, 0, 15);
        const char* lend = strchr(lpos, '-');
        strncpy(lstr, lpos, lend - lpos);
    }
    // use atol to avoid overflow in atoi, which generates signed int
    return lsn_t(atol(hstr), atol(lstr));
}

size_t LogArchiver::ArchiveDirectory::getFileSize(int fd)
{
    filestat_t fs;
    W_COERCE(me()->fstat(fd, fs));
    return fs.st_size;
}

LogArchiver::ArchiveDirectory::ArchiveDirectory(const sm_options& options)
    : appendFd(-1), mergeFd(-1), appendPos(0)
{
    archdir = options.get_string_option("sm_archdir", "archive");
    // CS TODO: archiver currently only works with 1MB blocks
    blockSize = DFT_BLOCK_SIZE;
        // options.get_int_option("sm_archiver_block_size", DFT_BLOCK_SIZE);
    size_t bucketSize =
        options.get_int_option("sm_archiver_bucket_size", 0);
    bool reformat = options.get_bool_option("sm_format", false);

    if (archdir.empty()) {
        W_FATAL_MSG(fcINTERNAL,
                << "Option for archive directory must be specified");
    }

    if (!fs::exists(archdir)) {
        if (reformat) {
            fs::create_directories(archdir);
        } else {
            cerr << "Error: could not open the log directory " << archdir <<endl;
            W_COERCE(RC(eOS));
        }
    }

    archpath = archdir;
    fs::directory_iterator it(archpath), eod;
    boost::regex run_rx(run_regex, boost::regex::perl);
    boost::regex current_rx(current_regex, boost::regex::perl);
    lsn_t highestLSN = lsn_t::null;

    for (; it != eod; it++) {
        fs::path fpath = it->path();
        string fname = fpath.filename().string();

        if (boost::regex_match(fname, run_rx)) {
            if (reformat) {
                fs::remove(fpath);
                continue;
            }
            // parse lsn from file name
            lsn_t currLSN = parseLSN(fname.c_str());
            if (currLSN > highestLSN) {
                DBGTHRD("Highest LSN found so far in archdir: " << currLSN);
                highestLSN = currLSN;
            }
        }
        else if (boost::regex_match(fname, current_rx)) {
            DBGTHRD(<< "Found unfinished log archive run. Deleting");
            fs::remove(fpath);
        }
        else {
            cerr << "ArchiveDirectory cannot parse filename " << fname << endl;
            W_FATAL(fcINTERNAL);
        }
    }
    startLSN = highestLSN;

    // no runs found in archive log -- start from first available log file
    if (startLSN.hi() == 0 && smlevel_0::log) {
        int nextPartition = startLSN.hi();

        int max = smlevel_0::log->durable_lsn().hi();

        while (nextPartition <= max) {
            string fname = smlevel_0::log->make_log_name(nextPartition);
            if (fs::exists(fname)) { break; }
            nextPartition++;
        }

        if (nextPartition > max) {
            W_FATAL_MSG(fcINTERNAL,
                << "Could not find partition files in log manager");
        }

        startLSN = lsn_t(nextPartition, 0);
    }

    // nothing worked -- start from 1.0 and hope for the best
    if (startLSN.hi() == 0) {
        startLSN = lsn_t(1,0);
    }

    // lastLSN is the end LSN of the last generated initial run
    lastLSN = startLSN;

    // create/load index
    archIndex = new ArchiveIndex(blockSize, startLSN, bucketSize);

    {
        std::vector<std::string> runFiles;
        listFiles(runFiles);
        std::vector<std::string>::const_iterator it;
        for(it=runFiles.begin(); it!=runFiles.end(); ++it) {
            std::string fname = archdir + "/" + *it;
            W_COERCE(archIndex->loadRunInfo(fname.c_str()));
        }

        // sort runinfo vector by lsn
        if (runFiles.size() > 0) {
            archIndex->init();
        }
    }

    // CS TODO this should be initialized statically, but whatever...
    memset(&SKIP_LOGREC, 0, sizeof(baseLogHeader));
    SKIP_LOGREC._len = sizeof(baseLogHeader);
    SKIP_LOGREC._type = logrec_t::t_skip;
    SKIP_LOGREC._cat = 1; // t_status is protected...

    DO_PTHREAD(pthread_mutex_init(&mutex, NULL));

    // ArchiveDirectory invariant is that current_run file always exists
    openNewRun();
}

LogArchiver::ArchiveDirectory::~ArchiveDirectory()
{
    if(archIndex) {
        delete archIndex;
    }
    DO_PTHREAD(pthread_mutex_destroy(&mutex));
}

rc_t LogArchiver::ArchiveDirectory::listFiles(std::vector<std::string>& list)
{
    list.clear();

    fs::directory_iterator it(archpath), eod;
    boost::regex run_rx(run_regex, boost::regex::perl);
    for (; it != eod; it++) {
        string fname = it->path().filename().string();
        if (boost::regex_match(fname, run_rx)) {
            list.push_back(fname);
        }
    }

    return RCOK;
}

rc_t LogArchiver::ArchiveDirectory::listFileStats(list<RunFileStats>& list)
{
    list.clear();
    vector<string> fnames;
    listFiles(fnames);

    RunFileStats stats;
    for (size_t i = 0; i < fnames.size(); i++) {
        stats.beginLSN = parseLSN(fnames[i].c_str(), false);
        stats.endLSN = parseLSN(fnames[i].c_str(), true);

        int fd;
        openForScan(fd, stats.beginLSN, stats.endLSN);
        stats.fileSize = getFileSize(fd);
        closeScan(fd);

        list.push_back(stats);
    }

    return RCOK;
}

/**
 * Opens a new run file of the log archive, closing the current run
 * if it exists. Upon closing, the file is renamed to contain the LSN
 * range of the log records contained in that run. The upper boundary
 * (lastLSN) is exclusive, meaning that it will be found on the beginning
 * of the following run. This also allows checking the filenames for any
 * any range of the LSNs which was "lost" when archiving.
 *
 * We assume the rename operation is atomic, even in case of OS crashes.
 *
 */
rc_t LogArchiver::ArchiveDirectory::openNewRun()
{
    if (appendFd >= 0) {
        return RC(fcINTERNAL);
    }

    int flags = smthread_t::OPEN_WRONLY | smthread_t::OPEN_SYNC
        | smthread_t::OPEN_CREATE;
    int fd;
    // 0744 is the mode_t for the file permissions (like in chmod)
    std::string fname = archdir + "/" + CURR_RUN_FILE;
    W_DO(me()->open(fname.c_str(), flags, 0744, fd));
    DBGTHRD(<< "Opened new output run");

    appendFd = fd;
    appendPos = 0;
    return RCOK;
}

fs::path LogArchiver::ArchiveDirectory::make_run_path(lsn_t begin, lsn_t end) const
{
    return archpath / fs::path(RUN_PREFIX + begin.str() + "-" + end.str());
}

fs::path LogArchiver::ArchiveDirectory::make_current_run_path() const
{
    return archpath / fs::path(CURR_RUN_FILE);
}

rc_t LogArchiver::ArchiveDirectory::closeCurrentRun(lsn_t runEndLSN)
{
    CRITICAL_SECTION(cs, mutex);

    if (appendFd >= 0) {
        if (appendPos == 0 && runEndLSN == lsn_t::null) {
            // nothing was appended -- just close file and return
            W_DO(me()->close(appendFd));
            appendFd = -1;
            return RCOK;
        }

        if (lastLSN != runEndLSN) {
            // register index information and write it on end of file
            if (archIndex && appendPos > 0) {
                // take into account space for skip log record
                appendPos += sizeof(baseLogHeader);
                // and make sure data is written aligned to block boundary
                appendPos -= appendPos % blockSize;
                appendPos += blockSize;
                archIndex->finishRun(lastLSN, runEndLSN, appendFd, appendPos);
            }

            fs::path new_path = make_run_path(lastLSN, runEndLSN);
            fs::rename(make_current_run_path(), new_path);

            DBGTHRD(<< "Closing current output run: " << new_path.string());
        }

        W_DO(me()->close(appendFd));
        appendFd = -1;
    }

    lastLSN = runEndLSN;

    openNewRun();

    return RCOK;
}

rc_t LogArchiver::ArchiveDirectory::append(char* data, size_t length)
{
    // make sure there is always a skip log record at the end
    w_assert1(length + sizeof(baseLogHeader) <= blockSize);
    memcpy(data + length, &SKIP_LOGREC, sizeof(baseLogHeader));

    INC_TSTAT(la_block_writes);
    W_DO(me()->pwrite(appendFd, data, length + sizeof(baseLogHeader),
                appendPos));
    appendPos += length;
    return RCOK;
}

rc_t LogArchiver::ArchiveDirectory::openForScan(int& fd, lsn_t runBegin,
        lsn_t runEnd)
{
    fs::path fpath = make_run_path(runBegin, runEnd);

    // Using direct I/O
    int flags = smthread_t::OPEN_RDONLY | smthread_t::OPEN_DIRECT;
    W_DO(me()->open(fpath.string().c_str(), flags, 0744, fd));

    return RCOK;
}

/** Note: buffer must be allocated for at least readSize + IO_ALIGN bytes,
 * otherwise direct I/O with alignment will corrupt memory.
 */
rc_t LogArchiver::ArchiveDirectory::readBlock(int fd, char* buf,
        size_t& offset, size_t readSize)
{
    stopwatch_t timer;

    if (readSize == 0) { readSize = blockSize; }
    size_t actualOffset = IO_ALIGN * (offset / IO_ALIGN);
    size_t diff = offset - actualOffset;
    // make sure we don't read more than a block worth of data
    w_assert1(actualOffset <= offset);
    w_assert1(offset % blockSize != 0 || readSize == blockSize);
    w_assert1(diff < IO_ALIGN);

    size_t actualReadSize = readSize + diff;
    if (actualReadSize % IO_ALIGN != 0) {
        actualReadSize = (1 + actualReadSize / IO_ALIGN) * IO_ALIGN;
    }

    int howMuchRead = 0;
    W_COERCE(me()->pread_short(
                fd, buf, actualReadSize, actualOffset, howMuchRead));
    if (howMuchRead == 0) {
        // EOF is signalized by setting offset to zero
        offset = 0;
        return RCOK;
    }

    if (diff > 0) {
        memmove(buf, buf + diff, readSize);
    }

    ADD_TSTAT(la_read_time, timer.time_us());
    ADD_TSTAT(la_read_volume, howMuchRead);
    INC_TSTAT(la_read_count);

    offset += readSize;
    return RCOK;
}

rc_t LogArchiver::ArchiveDirectory::closeScan(int& fd)
{
    W_DO(me()->close(fd));
    fd = -1;
    return RCOK;
}

LogArchiver::LogConsumer::LogConsumer(lsn_t startLSN, size_t blockSize, bool ignore)
    : nextLSN(startLSN), endLSN(lsn_t::null), currentBlock(NULL),
    blockSize(blockSize)
{
    DBGTHRD(<< "Starting log archiver at LSN " << nextLSN);

    // pos must be set to the correct offset within a block
    pos = startLSN.lo() % blockSize;

    readbuf = new AsyncRingBuffer(blockSize, IO_BLOCK_COUNT);
    reader = new ReaderThread(readbuf, startLSN);
    logScanner = new LogScanner(blockSize);

    if(ignore) {
        initLogScanner(logScanner);
    }
    reader->fork();
}

LogArchiver::LogConsumer::~LogConsumer()
{
    if (!readbuf->isFinished()) {
        shutdown();
    }
    delete reader;
    delete readbuf;
}

void LogArchiver::LogConsumer::shutdown()
{
    if (!readbuf->isFinished()) {
        readbuf->set_finished();
        reader->shutdown();
        reader->join();
    }
}

void LogArchiver::LogConsumer::open(lsn_t endLSN, bool readWholeBlocks)
{
    this->endLSN = endLSN;
    this->readWholeBlocks = readWholeBlocks;

    reader->activate(endLSN);

    nextBlock();
}

bool LogArchiver::LogConsumer::nextBlock()
{
    if (currentBlock) {
        readbuf->consumerRelease();
        DBGTHRD(<< "Released block for replacement " << (void*) currentBlock);
        currentBlock = NULL;
    }

    // get a block from the reader thread
    currentBlock = readbuf->consumerRequest();
    if (!currentBlock) {
        if (!readbuf->isFinished()) {
            // This happens if log scanner finds a skip logrec, but
            // then the next partition does not exist. This would be a bug,
            // because endLSN should always be an existing LSN, or one
            // immediately after an existing LSN but in the same partition.
            W_FATAL_MSG(fcINTERNAL, << "Consume request failed!");
        }
        return false;
    }
    DBGTHRD(<< "Picked block for replacement " << (void*) currentBlock);
    if (pos >= blockSize) {
        // If we are reading the same block but from a continued reader cycle,
        // pos should be maintained. For this reason, pos should be set to
        // blockSize on constructor.
        pos = 0;
    }

    return true;
}

bool LogArchiver::LogConsumer::next(logrec_t*& lr)
{
    w_assert1(nextLSN <= endLSN);

    int lrLength;
    bool scanned = logScanner->nextLogrec(currentBlock, pos, lr, &nextLSN,
            &endLSN, &lrLength);

    bool stopReading = nextLSN == endLSN;
    if (!scanned && readWholeBlocks && !stopReading) {
        /*
         * If the policy is to read whole blocks only, we must also stop
         * reading when an incomplete log record was fetched on the last block.
         * Under normal circumstances, we would fetch the next block to
         * assemble the remainder of the log record. In this case, however, we
         * must wait until the next activation. This case is detected when the
         * length of the next log record is larger than the space remaining in
         * the current block, or if the length is negative (meaning there are
         * not enough bytes left on the block to tell the length).
         */
        stopReading = endLSN.hi() == nextLSN.hi() &&
            (lrLength <= 0 || (endLSN.lo() - nextLSN.lo() < lrLength));
    }

    if (!scanned && stopReading) {
        DBGTHRD(<< "Consumer reached end LSN on " << nextLSN);
        /*
         * nextLogrec returns false if it is about to read the LSN given in the
         * last argument (endLSN). This means we should stop and not read any
         * further blocks.  On the next archiver activation, replacement must
         * start on this LSN, which will likely be in the middle of the block
         * currently being processed. However, we don't have to worry about
         * that because reader thread will start reading from this LSN on the
         * next activation.
         */
        return false;
    }

    w_assert1(nextLSN <= endLSN);
    w_assert1(!scanned || lr->lsn_ck() + lr->length() == nextLSN);

    if (!scanned || (lrLength > 0 && lr->type() == logrec_t::t_skip)) {
        /*
         * nextLogrec returning false with nextLSN != endLSN means that we are
         * suppose to read another block and call the method again.
         */
        if (scanned && lr->type() == logrec_t::t_skip) {
            // Try again if reached skip -- next block should be from next file
            nextLSN = lsn_t(nextLSN.hi() + 1, 0);
            pos = 0;
            DBGTHRD(<< "Reached skip logrec, set nextLSN = " << nextLSN);
            logScanner->reset();
            w_assert1(!logScanner->hasPartialLogrec());
        }
        if (!nextBlock()) {
            // reader thread finished and consume request failed
            DBGTHRD(<< "LogConsumer next-block request failed");
            return false;
        }
        return next(lr);
    }

    return true;
}

/**
 * Selection part of replacement-selection algorithm. Takes the smallest
 * record from the heap and copies it to the write buffer, one IO block
 * at a time. The block header contains the run number (1 byte) and the
 * logical size of the block (4 bytes). The former is required so that
 * the asynchronous writer thread knows when to start a new run file.
 * The latter simplifies the write process by not allowing records to
 * be split in the middle by block boundaries.
 */
bool LogArchiver::selection()
{
    if (heap->size() == 0) {
        // if there are no elements in the heap, we have nothing to write
        // -> return and wait for next activation
        DBGTHRD(<< "Selection got empty heap -- sleeping");
        return false;
    }

    run_number_t run = heap->topRun();
    if (!blkAssemb->start(run)) {
        return false;
    }

    DBGTHRD(<< "Producing block for selection on run " << run);
    while (true) {
        if (heap->size() == 0 || run != heap->topRun()) {
            break;
        }

        logrec_t* lr = heap->top();
        if (blkAssemb->add(lr)) {
            // DBGTHRD(<< "Selecting for output: " << *lr);
            heap->pop();
            // w_assert3(run != heap->topRun() ||
            //     heap->top()->pid()>= lr->pid());
        }
        else {
            break;
        }
    }
    blkAssemb->finish();

    return true;
}

LogArchiver::BlockAssembly::BlockAssembly(ArchiveDirectory* directory)
    : dest(NULL), maxLSNInBlock(lsn_t::null), maxLSNLength(0),
    lastRun(-1), bucketSize(0), nextBucket(0)
{
    archIndex = directory->getIndex();
    blockSize = directory->getBlockSize();
    writebuf = new AsyncRingBuffer(blockSize, IO_BLOCK_COUNT);
    writer = new WriterThread(writebuf, directory);
    writer->fork();

    if (archIndex) {
        bucketSize = archIndex->getBucketSize();
    }
}

LogArchiver::BlockAssembly::~BlockAssembly()
{
    if (!writebuf->isFinished()) {
        shutdown();
    }
    delete writer;
    delete writebuf;
}

bool LogArchiver::BlockAssembly::hasPendingBlocks()
{
    return !writebuf->isEmpty();
}

run_number_t LogArchiver::BlockAssembly::getRunFromBlock(const char* b)
{
    BlockHeader* h = (BlockHeader*) b;
    return h->run;
}

lsn_t LogArchiver::BlockAssembly::getLSNFromBlock(const char* b)
{
    BlockHeader* h = (BlockHeader*) b;
    return h->lsn;
}

size_t LogArchiver::BlockAssembly::getEndOfBlock(const char* b)
{
    BlockHeader* h = (BlockHeader*) b;
    return h->end;
}

bool LogArchiver::BlockAssembly::start(run_number_t run)
{
    DBGTHRD(<< "Requesting write block for selection");
    dest = writebuf->producerRequest();
    if (!dest) {
        DBGTHRD(<< "Block request failed!");
        if (!writebuf->isFinished()) {
            W_FATAL_MSG(fcINTERNAL,
                    << "ERROR: write ring buffer refused produce request");
        }
        return false;
    }
    DBGTHRD(<< "Picked block for selection " << (void*) dest);

    pos = sizeof(BlockHeader);

    if (run != lastRun) {
        if (archIndex && lastRun >= 0) {
            archIndex->appendNewEntry();
        }
        nextBucket = 0;
        fpos = 0;
        lastRun = run;
    }

    if (bucketSize > 0) {
        buckets.clear();
    }

    return true;
}

bool LogArchiver::BlockAssembly::add(logrec_t* lr)
{
    w_assert0(dest);

    size_t available = blockSize - (pos + sizeof(baseLogHeader));
    if (lr->length() > available) {
        return false;
    }

    if (firstPID == 0) {
        firstPID = lr->pid();
    }

    if (maxLSNInBlock < lr->lsn_ck()) {
        maxLSNInBlock = lr->lsn_ck();
        maxLSNLength = lr->length();
    }

    if (bucketSize > 0 && lr->pid() / bucketSize >= nextBucket) {
        PageID shpid = (lr->pid() / bucketSize) * bucketSize;
        buckets.push_back(
                pair<PageID, size_t>(shpid, fpos));
        nextBucket = shpid / bucketSize + 1;
    }

    memcpy(dest + pos, lr, lr->length());
    pos += lr->length();
    fpos += lr->length();
    return true;
}

void LogArchiver::BlockAssembly::finish()
{
    DBGTHRD("Selection produced block for writing " << (void*) dest <<
            " in run " << (int) lastRun << " with end " << pos);
    w_assert0(dest);

    if (archIndex) {
        if (bucketSize == 0) {
            archIndex->newBlock(firstPID);
        }
        else {
            archIndex->newBlock(buckets);
        }
    }
    firstPID = 0;

    // write block header info
    BlockHeader* h = (BlockHeader*) dest;
    h->run = lastRun;
    h->end = pos;
    /*
     * CS: end LSN of a block/run has to be an exclusive boundary, whereas
     * lastLSN is an inclusive one (i.e., the LSN of the last logrec in this
     * block). To fix that, we simply add the length of the last log record to
     * its LSN, which yields the LSN of the following record in the recovery
     * log. It doesn't matter if this following record does not get archived or
     * if it is a skip log record, since the property that must be respected is
     * simply that run boundaries must match (i.e., endLSN(n) == beginLSN(n+1)
     */
    h->lsn = maxLSNInBlock.advance(maxLSNLength);

#if W_DEBUG_LEVEL>=3
    // verify that all log records are within end boundary
    size_t vpos = sizeof(BlockHeader);
    while (vpos < pos) {
        logrec_t* lr = (logrec_t*) (dest + vpos);
        w_assert3(lr->lsn_ck() < h->lsn);
        vpos += lr->length();
    }
#endif

    maxLSNInBlock = lsn_t::null;
    writebuf->producerRelease();
    dest = NULL;
}

void LogArchiver::BlockAssembly::shutdown()
{
    w_assert0(!dest);
    writebuf->set_finished();
    writer->join();
}

LogArchiver::ArchiveScanner::ArchiveScanner(ArchiveDirectory* directory)
    : directory(directory), archIndex(directory->getIndex())
{
    if (!archIndex) {
        W_FATAL_MSG(fcINTERNAL,
                << "ArchiveScanner requires a valid archive index!");
    }
}

LogArchiver::ArchiveScanner::RunMerger*
LogArchiver::ArchiveScanner::open(PageID startPID, PageID endPID,
        lsn_t startLSN, size_t readSize)
{
    RunMerger* merger = new RunMerger();
    vector<ProbeResult> probes;

    // probe for runs
    archIndex->probe(probes, startPID, endPID, startLSN);

    // construct one run scanner for each probed run
    for (size_t i = 0; i < probes.size(); i++) {
        RunScanner* runScanner = new RunScanner(
                probes[i].runBegin,
                probes[i].runEnd,
                probes[i].pidBegin,
                probes[i].pidEnd,
                probes[i].offset,
                directory,
                readSize
        );

        merger->addInput(runScanner);
    }

    if (merger->heapSize() == 0) {
        // all runs pruned from probe
        delete merger;
        return NULL;
    }

    INC_TSTAT(la_open_count);

    return merger;
}

LogArchiver::ArchiveScanner::RunScanner::RunScanner(lsn_t b, lsn_t e,
        PageID f, PageID l, fileoff_t o, ArchiveDirectory* directory,
        size_t readSize)
: runBegin(b), runEnd(e), firstPID(f), lastPID(l), offset(o),
    fd(-1), blockCount(0), readSize(readSize), directory(directory)
{
    if (readSize == 0) {
        readSize = directory->getBlockSize();
    }

    // Using direct I/O
    int res = posix_memalign((void**) &buffer, IO_ALIGN, readSize + IO_ALIGN);
    w_assert0(res == 0);
    // buffer = new char[directory->getBlockSize()];

    if (directory->getIndex()) {
        bucketSize = directory->getIndex()->getBucketSize();
    }
    else {
        bucketSize = 0;
    }

    // bpos at the end of block triggers reading of the first block
    // when calling next()
    bpos = readSize;
    w_assert1(bpos > 0);

    scanner = new LogScanner(readSize);
}

LogArchiver::ArchiveScanner::RunScanner::~RunScanner()
{
    if (fd > 0) {
        W_COERCE(directory->closeScan(fd));
    }

    delete scanner;

    // Using direct I/O
    free(buffer);
    // delete[] buffer;
}

bool LogArchiver::ArchiveScanner::RunScanner::nextBlock()
{
    size_t blockSize = directory->getBlockSize();

    if (fd < 0) {
        W_COERCE(directory->openForScan(fd, runBegin, runEnd));

        if (directory->getIndex()) {
            directory->getIndex()->getBlockCounts(fd, NULL, &blockCount);
        }
    }

    // do not read past data blocks into index blocks
    if (blockCount == 0 || offset >= blockCount * blockSize)
    {
        W_COERCE(directory->closeScan(fd));
        return false;
    }

    // offset is updated by readBlock
    W_COERCE(directory->readBlock(fd, buffer, offset, readSize));

    // offset set to zero indicates EOF
    if (offset == 0) {
        W_COERCE(directory->closeScan(fd));
        return false;
    }

    bpos = 0;

    return true;
}

bool LogArchiver::ArchiveScanner::RunScanner::next(logrec_t*& lr)
{
    while (true) {
        if (scanner->nextLogrec(buffer, bpos, lr)) { break; }
        if (!nextBlock()) { return false; }
    }

    if (lr->type() == logrec_t::t_skip ||
                (lastPID != 0 && lr->pid() >= lastPID))
    {
        // end of scan
        return false;
    }

    return true;
}

std::ostream& operator<< (ostream& os,
        const LogArchiver::ArchiveScanner::RunScanner& m)
{
    os << m.runBegin << "-" << m.runEnd << " endPID=" << m.lastPID;
    return os;
}

LogArchiver::ArchiveScanner::MergeHeapEntry::MergeHeapEntry(RunScanner* runScan)
    : active(true), runScan(runScan)
{
    PageID startPID = runScan->firstPID;
    // bring scanner up to starting point
    logrec_t* next = NULL;
    if (runScan->next(next)) {
        lr = next;
        lsn = lr->lsn();
        pid = lr->pid();
        active = true;
        DBG(<< "Run scan opened on pid " << lr->pid() << " afer " << startPID);

        // advance index until startPID is reached
        if (pid < startPID) {
            bool hasNext = true;
            while (hasNext && lr->pid() < startPID) {
                hasNext = runScan->next(lr);
            }
            if (hasNext) {
                DBG(<< "Run scan advanced to pid " << lr->pid() << " afer " << startPID);
                pid = lr->pid();
                lsn = lr->lsn();
            }
            else { active = false; }
        }
    }
    else { active = false; }
}

void LogArchiver::ArchiveScanner::MergeHeapEntry::moveToNext()
{
    if (runScan->next(lr)) {
        pid = lr->pid();
        lsn = lr->lsn_ck();
    }
    else {
        active = false;
    }
}

void LogArchiver::ArchiveScanner::RunMerger::addInput(RunScanner* r)
{
    w_assert0(!started);
    MergeHeapEntry entry(r);
    heap.AddElementDontHeapify(entry);

    if (endPID == 0) {
        endPID = r->lastPID;
    }
    w_assert1(endPID == r->lastPID);
}

bool LogArchiver::ArchiveScanner::RunMerger::next(logrec_t*& lr)
{
    stopwatch_t timer;

    if (heap.NumElements() == 0) {
        return false;
    }

    if (!started) {
        started = true;
        heap.Heapify();
    }
    else {
        /*
         * CS: Before returning the next log record, the scanner at the top of
         * the heap must be recomputed and the heap re-organized. This is
         * because the caller maintains a pointer into the scanner's buffer,
         * and calling next before the log record is consumed may cause the
         * pointer to be invalidated if a new block is read into the buffer.
         */
        heap.First().moveToNext();
        heap.ReplacedFirst();
    }

    if (!heap.First().active) {
        /*
         * CS: If top run is inactive, then all runs are and scan is done
         * Memory of each scanner must be released here instead of when
         * destructing heap, because the heap internally copies entries and
         * destructs these copies in operations like SiftDown(). Therefore the
         * underlying buffer may get wrongly deleted
         */
        close();
        return false;
    }

    ADD_TSTAT(la_merge_heap_time, timer.time_us());

    lr = heap.First().lr;
    return true;
}

void LogArchiver::ArchiveScanner::RunMerger::close()
{
    while (heap.NumElements() > 0) {
        delete heap.RemoveFirst().runScan;
    }
}

void LogArchiver::ArchiveScanner::RunMerger::dumpHeap(ostream& out)
{
    heap.Print(out);
}

LogArchiver::ArchiverHeap::ArchiverHeap(size_t workspaceSize)
    : currentRun(0), filledFirst(false), w_heap(heapCmp)
{
    workspace = new fixed_lists_mem_t(workspaceSize);
}

LogArchiver::ArchiverHeap::~ArchiverHeap()
{
    delete workspace;
}

slot_t LogArchiver::ArchiverHeap::allocate(size_t length)
{
    slot_t dest(NULL, 0);
    W_COERCE(workspace->allocate(length, dest));

    if (!dest.address) {
        // workspace full -> do selection until space available
        DBGTHRD(<< "Heap full! Size: " << w_heap.NumElements()
                << " alloc size: " << length);
        if (!filledFirst) {
            // first run generated by first full load of w_heap
            currentRun++;
            filledFirst = true;
            DBGTHRD(<< "Heap full for the first time; start run 1");
        }
    }

    return dest;
}

bool LogArchiver::ArchiverHeap::push(logrec_t* lr, bool duplicate)
{
    slot_t dest = allocate(lr->length());
    if (!dest.address) {
        DBGTHRD(<< "heap full for logrec: " << lr->type_str()
                << " at " << lr->lsn());
        return false;
    }

    PageID pid = lr->pid();
    lsn_t lsn = lr->lsn();
    memcpy(dest.address, lr, lr->length());

    // CS: Multi-page log records are replicated so that each page can be
    // recovered from the log archive independently.  Note that this is not
    // required for Restart or Single-page recovery because following the
    // per-page log chain of both pages eventually lands on the same multi-page
    // log record. For restore, it must be duplicated because log records are
    // sorted and there is no chain.
    if (duplicate) {
        // If we have to duplciate the log record, make sure there is room by
        // calling recursively without duplication. Note that the original
        // contents were already saved with the memcpy operation above.
        lr->set_pid(lr->pid2());
        lr->set_page_prev_lsn(lr->page2_prev_lsn());
        if (!push(lr, false)) {
            // If duplicated did not fit, then insertion of the original must
            // also fail. We have to (1) restore the original contents of
            // the log record for the next attempt; and (2) free its memory
            // from the workspace. Since nothing was added to the heap yet, it
            // stays untouched.
            memcpy(lr, dest.address, lr->length());
            W_COERCE(workspace->free(dest));
            return false;
        }
    }
    else {
        // If all records of the current run are gone, start new run. But only
        // if we are not duplicating a log record -- otherwise two new runs
        // would be created.
        if (filledFirst &&
                (size() == 0 || w_heap.First().run == currentRun)) {
            currentRun++;
            DBGTHRD(<< "Replacement starting new run " << (int) currentRun
                    << " on LSN " << lr->lsn_ck());
        }
    }

    //DBGTHRD(<< "Processing logrec " << lr->lsn_ck() << ", type " <<
    //        lr->type() << "(" << lr->type_str() << ") length " <<
    //        lr->length() << " into run " << (int) currentRun);

    // insert key and pointer into w_heap
    HeapEntry k(currentRun, pid, lsn, dest);

    // CS: caution: AddElementDontHeapify does NOT work!!!
    w_heap.AddElement(k);

    return true;
}

void LogArchiver::ArchiverHeap::pop()
{
    // DBGTHRD(<< "Selecting for output: "
    //         << *((logrec_t*) w_heap.First().slot.address));

    workspace->free(w_heap.First().slot);
    w_heap.RemoveFirst();

    if (size() == 0) {
        // If heap becomes empty, run generation must be reset with a new run
        filledFirst = false;
        currentRun++;
    }
}

logrec_t* LogArchiver::ArchiverHeap::top()
{
    return (logrec_t*) w_heap.First().slot.address;
}

// gt is actually a less than function, to produce ascending order
bool LogArchiver::ArchiverHeap::Cmp::gt(const HeapEntry& a,
        const HeapEntry& b) const
{
    if (a.run != b.run) {
        return a.run < b.run;
    }
    if (a.pid != b.pid) {
        return a.pid< b.pid;
    }
    return a.lsn < b.lsn;
}

/**
 * Replacement part of replacement-selection algorithm. Fetches log records
 * from the read buffer into the sort workspace and adds a correspondent
 * entry to the heap. When workspace is full, invoke selection until there
 * is space available for the current log record.
 *
 * Unlike standard replacement selection, runs are limited to the size of
 * the workspace, in order to maintain a simple non-overlapping mapping
 * between regions of the input file (i.e., the recovery log) and the runs.
 * To achieve that, we change the logic that assigns run numbers to incoming
 * records:
 *
 * a) Standard RS: if incoming key is larger than the last record written,
 * assign to current run, otherwise to the next run.
 * b) Log-archiving RS: keep track of run number currently being written,
 * always assigning the incoming records to a greater run. Once all records
 * from the current run are removed from the heap, increment the counter.
 * To start, initial input records are assigned to run 1 until the workspace
 * is full, after which incoming records are assigned to run 2.
 */
void LogArchiver::replacement()
{
    while(true) {
        logrec_t* lr;
        if (!consumer->next(lr)) {
            w_assert0(readWholeBlocks ||
                    control.endLSN <= consumer->getNextLSN());
            if (control.endLSN < consumer->getNextLSN()) {
                // nextLSN may be greater than endLSN due to skip
                control.endLSN = consumer->getNextLSN();
                // TODO: in which correct situation can this assert fail???
                // w_assert0(control.endLSN.hi() == 0);
                DBGTHRD(<< "Replacement changed endLSN to " << control.endLSN);
            }
            return;
        }

        if (!lr->is_redo()) {
            continue;
        }

        pushIntoHeap(lr, lr->is_multi_page());
    }
}

void LogArchiver::pushIntoHeap(logrec_t* lr, bool duplicate)
{
    while (!heap->push(lr, duplicate)) {
        if (heap->size() == 0) {
            W_FATAL_MSG(fcINTERNAL,
                    << "Heap empty but push not possible!");
        }

        // heap full -- invoke selection and try again
        if (heap->size() == 0) {
            // CS TODO this happens sometimes for very large page_img_format
            // logrecs. Inside this if, we should "reset" the heap and also
            // makesure that the log record is smaller than the max block.
            W_FATAL_MSG(fcINTERNAL,
                    << "Heap empty but push not possible!");
        }

        DBGTHRD(<< "Heap full! Invoking selection");
        bool success = selection();

        w_assert0(success || heap->size() == 0);
    }
}

void LogArchiver::activate(lsn_t endLSN, bool wait)
{
    if (eager) return;

    w_assert0(smlevel_0::log);
    if (endLSN == lsn_t::null) {
        endLSN = smlevel_0::log->durable_lsn();
    }

    while (!control.activate(wait, endLSN)) {
        if (!wait) break;
    }
}

bool LogArchiver::waitForActivation()
{
    if (eager) {
        lsn_t newEnd = smlevel_0::log->durable_lsn();
        while (control.endLSN == newEnd) {
            // we're going faster than log, sleep a bit (1ms)
            ::usleep(1000);
            newEnd = smlevel_0::log->durable_lsn();

            lintel::atomic_thread_fence(lintel::memory_order_consume);
            if (shutdownFlag) {
                return false;
            }

            // Flushing requested (e.g., by restore manager)
            if (flushReqLSN != lsn_t::null) {
                return true;
            }

            if (newEnd.lo() == 0) {
                // If durable_lsn is at the beginning of a new log partition,
                // it can happen that at this point the file was not created
                // yet, which would cause the reader thread to fail.
                continue;
            }
        }
        control.endLSN = newEnd;
    }
    else {
        bool activated = control.waitForActivation();
        if (!activated) {
            return false;
        }
    }

    lintel::atomic_thread_fence(lintel::memory_order_consume);
    if (shutdownFlag) {
        return false;
    }

    return true;
}

bool LogArchiver::processFlushRequest()
{
    if (flushReqLSN != lsn_t::null) {
        DBGTHRD(<< "Archive flush requested until LSN " << flushReqLSN);
        if (getNextConsumedLSN() < flushReqLSN) {
            // if logrec hasn't been read into heap yet, then selection
            // will never reach it. Do another round until heap has
            // consumed it.
            if (control.endLSN < flushReqLSN) {
                control.endLSN = flushReqLSN;
            }
            DBGTHRD(<< "LSN requested for flush hasn't been consumed yet. "
                    << "Trying again after another round");
            return false;
        }
        else {
            // consume whole heap
            while (selection()) {}
            // Heap empty: Wait for all blocks to be consumed and writen out
            w_assert0(heap->size() == 0);
            while (blkAssemb->hasPendingBlocks()) {
                ::usleep(10000); // 10ms
            }

            // Forcibly close current run to guarantee that LSN is persisted
            W_COERCE(directory->closeCurrentRun(flushReqLSN));
            blkAssemb->resetWriter();

            /* Now we know that the requested LSN has been processed by the
             * heap and all archiver temporary memory has been flushed. Thus,
             * we know it has been fully processed and all relevant log records
             * are available in the archive.
             */
            flushReqLSN = lsn_t::null;
            lintel::atomic_thread_fence(lintel::memory_order_release);
            return true;
        }
    }
    return false;
}

bool LogArchiver::isLogTooSlow()
{
    if (!eager) { return false; }

    int minActWindow = directory->getBlockSize();

    auto isSmallWindow = [minActWindow](lsn_t endLSN, lsn_t nextLSN) {
        int nextHi = nextLSN.hi();
        int nextLo = nextLSN.lo();
        int endHi = endLSN.hi();
        int endLo = endLSN.lo();
        return (endHi == nextHi && endLo - nextLo< minActWindow) ||
            (endHi == nextHi + 1 && endLo < minActWindow);
    };

    if (isSmallWindow(control.endLSN, nextActLSN))
    {
        // If this happens to often, the block size should be decreased.
        ::usleep(slowLogGracePeriod);
        // To better exploit device bandwidth, we only start archiving if
        // at least one block worth of log is available for consuption.
        // This happens when the log is growing too slow.
        // However, if it seems like log activity has stopped (i.e.,
        // durable_lsn did not advance since we started), then we proceed
        // with the small activation window.
        bool logStopped = control.endLSN == smlevel_0::log->durable_lsn();
        if (!isSmallWindow(control.endLSN, nextActLSN) && !logStopped) {
            return false;
        }
        INC_TSTAT(la_log_slow);
        DBGTHRD(<< "Log growing too slow");
        return true;
    }
    return false;
}

bool LogArchiver::shouldActivate(bool logTooSlow)
{
    if (flushReqLSN == control.endLSN) {
        return control.endLSN > nextActLSN;
    }

    if (logTooSlow && control.endLSN == smlevel_0::log->durable_lsn()) {
        // Special case: log is not only groing too slow, but it has actually
        // halted. This means the application/experiment probably already
        // finished and is just waiting for the archiver. In that case, we
        // allow the activation with a small window. However, it may not be
        // a window of size zero (s.t. endLSN == nextActLSN)
        DBGTHRD(<< "Log seems halted -- accepting small window");
        return control.endLSN > nextActLSN;
    }

    // Try to keep activation window at block boundaries to better utilize
    // I/O bandwidth
    if (eager && readWholeBlocks && !logTooSlow) {
        size_t boundary = directory->getBlockSize() *
            (control.endLSN.lo() / directory->getBlockSize());
        control.endLSN = lsn_t(control.endLSN.hi(), boundary);
        if (control.endLSN <= nextActLSN) {
            return false;
        }
        if (control.endLSN.lo() == 0) {
            // If durable_lsn is at the beginning of a new log partition,
            // it can happen that at this point the file was not created
            // yet, which would cause the reader thread to fail. This does
            // not happen with eager archiving, so we should eventually
            // remove it
            return false;
        }
        DBGTHRD(<< "Adjusted activation window to block boundary " <<
                control.endLSN);
    }

    if (control.endLSN == lsn_t::null
            || control.endLSN <= nextActLSN)
    {
        DBGTHRD(<< "Archiver already passed this range. Continuing...");
        return false;
    }

    w_assert1(control.endLSN > nextActLSN);
    return true;
}

void LogArchiver::run()
{
    while(true) {
        CRITICAL_SECTION(cs, control.mutex);

        if (!waitForActivation()) {
            break;
        }
        bool logTooSlow = isLogTooSlow();

        if (processFlushRequest()) {
            continue;
        }

        if (!shouldActivate(logTooSlow)) {
            continue;
        }
        INC_TSTAT(la_activations);

        DBGTHRD(<< "Log archiver activated from " << nextActLSN << " to "
                << control.endLSN);

        consumer->open(control.endLSN, readWholeBlocks && !logTooSlow);

        replacement();

        /*
         * Selection is not invoked here because log archiving should be a
         * continuous process, and so the heap should not be emptied at
         * every invocation. Instead, selection is invoked by the replacement
         * method when the heap is full. This also has the advantage that the
         * heap is kept as full as possible, which generates larger runs.
         * A consequence of this scheme is that the activation of the log
         * archiver until an LSN X just means that all log records up to X will
         * be inserted into the heap, and not that they will be persited into
         * runs. This means that log recycling must not rely on activation
         * cycles, but on signals/events generated by the writer thread (TODO)
         */

        // nextActLSN = consumer->getNextLSN();
        nextActLSN = control.endLSN;
        DBGTHRD(<< "Log archiver consumed all log records until LSN "
                << nextActLSN);

        if (!eager) {
            control.endLSN = lsn_t::null;
            control.activated = false;
        }
    }

    // Perform selection until all remaining entries are flushed out of
    // the heap into runs. Last run boundary is also enqueued.
    DBGTHRD(<< "Archiver exiting -- last round of selection to empty heap");
    while (selection()) {}

    w_assert0(heap->size() == 0);
}

bool LogArchiver::requestFlushAsync(lsn_t reqLSN)
{
    if (reqLSN == lsn_t::null) {
        return false;
    }
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
    if (flushReqLSN != lsn_t::null) {
        return false;
    }
    flushReqLSN = reqLSN;
    lintel::atomic_thread_fence(lintel::memory_order_release);

    // Other thread may race with us and win -- recheck
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
    if (flushReqLSN != reqLSN) {
        return false;
    }
    return true;
}

void LogArchiver::requestFlushSync(lsn_t reqLSN)
{
    DBGTHRD(<< "Requesting flush until LSN " << reqLSN);
    if (!eager) {
        activate(reqLSN);
    }
    while (!requestFlushAsync(reqLSN)) {
        usleep(1000); // 1ms
    }
    // When log archiver is done processing the flush request, it will set
    // flushReqLSN back to null. This method only guarantees that the flush
    // request was processed. The caller must still wait for the desired run to
    // be persisted -- if it so wishes.
    while(true) {
        lintel::atomic_thread_fence(lintel::memory_order_acquire);
        if (flushReqLSN == lsn_t::null) {
            break;
        }
        ::usleep(10000); // 10ms
    }
}

void LogArchiver::archiveUntilLSN(lsn_t lsn)
{
    // if lsn.lo() == 0, archiver will not activate and it will get stuck
    w_assert1(lsn.lo() > 0);

    // wait for log record to be consumed
    while (getNextConsumedLSN() < lsn) {
        activate(lsn, true);
        ::usleep(10000); // 10ms
    }

    if (getDirectory()->getLastLSN() < lsn) {
        requestFlushSync(lsn);
    }
}

void LogArchiver::ArchiveDirectory::deleteAllRuns()
{
    fs::directory_iterator it(archpath), eod;
    boost::regex run_rx(run_regex, boost::regex::perl);
    for (; it != eod; it++) {
        string fname = it->path().filename().string();
        if (boost::regex_match(fname, run_rx)) {
            fs::remove(it->path());
        }
    }
}

bool LogScanner::hasPartialLogrec()
{
    return truncMissing > 0;
}

void LogScanner::reset()
{
    truncMissing = 0;
}

/**
 * Fetches a log record from the read buffer ('src' in offset 'pos').
 * Handles incomplete records due to block boundaries in the buffer
 * and skips checkpoints and skip log records. Returns false if whole
 * record could not be read in the current buffer block, indicating that
 * the caller must fetch a new block into 'src' and invoke method again.
 *
 * Method loops until any in-block skipping is completed.
 */
bool LogScanner::nextLogrec(char* src, size_t& pos, logrec_t*& lr, lsn_t* nextLSN,
        lsn_t* stopLSN, int* lrLength)
{
tryagain:
    if (nextLSN && stopLSN && *stopLSN == *nextLSN) {
        return false;
    }

    // whole log record is not guaranteed to fit in a block
    size_t remaining = blockSize - pos;
    if (remaining == 0) {
        return false;
    }

    lr = (logrec_t*) (src + pos);

    if (truncMissing > 0) {
        // finish up the trunc logrec from last block
        DBG5(<< "Reading partial log record -- missing: "
                << truncMissing << " of " << truncCopied + truncMissing);
        w_assert1(truncMissing <= remaining);
        memcpy(truncBuf + truncCopied, src + pos, truncMissing);
        pos += truncMissing;
        lr = (logrec_t*) truncBuf;
        truncCopied += truncMissing;
        truncMissing = 0;
        w_assert1(truncCopied == lr->length());
    }
    // we need at least two bytes to read the length
    else if (remaining == 1 || lr->length() > remaining) {
        // remainder of logrec must be read from next block
        w_assert0(remaining < sizeof(baseLogHeader) || lr->valid_header());
        DBG5(<< "Log record with length "
                << (remaining > 1 ? lr->length() : -1)
                << " does not fit in current block of " << remaining);
        w_assert0(remaining <= sizeof(logrec_t));
        memcpy(truncBuf, src + pos, remaining);
        truncCopied = remaining;
        truncMissing = lr->length() - remaining;
        pos += remaining;

        if (lrLength) {
            *lrLength = (remaining > 1) ? lr->length() : -1;
        }

        return false;
    }

    // assertions to check consistency of logrec
#if W_DEBUG_LEVEL >=1
    // TODO add assert macros with DBG message
    if (nextLSN != NULL && !lr->valid_header(*nextLSN)) {
        DBGTHRD(<< "Unexpected LSN in scanner at pos " << pos
                << " : " << lr->lsn_ck()
                << " expected " << *nextLSN);
    }
#endif

    w_assert1(lr->valid_header(nextLSN == NULL ? lsn_t::null : *nextLSN));

    if (nextLSN) {
        *nextLSN += lr->length();
    }

    if (lrLength) {
        *lrLength = lr->length();
    }

    // handle ignorred logrecs
    if (ignore[lr->type()]) {
        // if logrec was assembled from truncation, pos was already
        // incremented, and skip is not necessary
        if ((void*) lr == (void*) truncBuf) {
            goto tryagain;
        }
        // DBGTHRD(<< "Found " << lr->type_str() << " on " << lr->lsn_ck()
        //         << " pos " << pos << ", skipping " << lr->length());
        toSkip += lr->length();
    }

    // see if we have something to skip
    if (toSkip > 0) {
        if (toSkip <= remaining) {
            // stay in the same block after skipping
            pos += toSkip;
            //DBGTHRD(<< "In-block skip for replacement, new pos = " << pos);
            toSkip = 0;
            goto tryagain;
        }
        else {
            DBGTHRD(<< "Skipping to next block until " << toSkip);
            toSkip -= remaining;
            return false;
        }
    }

    // if logrec was assembled from truncation, pos was already incremented
    if ((void*) lr != (void*) truncBuf) {
        pos += lr->length();
    }

    // DBGTHRD(<< "Log scanner returning  " << lr->type_str()
    //         << " on pos " << pos << " lsn " << lr->lsn_ck());


    return true;
}

LogArchiver::ArchiveIndex::ArchiveIndex(size_t blockSize, lsn_t startLSN,
        size_t bucketSize)
    : blockSize(blockSize), lastFinished(-1),
    bucketSize(bucketSize)
{
    DO_PTHREAD(pthread_mutex_init(&mutex, NULL));
    writeBuffer = new char[blockSize];
    // Using direct I/O
    int res = posix_memalign((void**) &readBuffer, IO_ALIGN, blockSize);
    w_assert0(res == 0);
    // readBuffer = new char[blockSize];

    // last run in the array is always the one being currently generated
    RunInfo r;
    r.firstLSN = startLSN;
    runs.push_back(r);
}

LogArchiver::ArchiveIndex::~ArchiveIndex()
{
    DO_PTHREAD(pthread_mutex_destroy(&mutex));
    delete[] writeBuffer;

    // Using direct I/O
    free(readBuffer);
    // delete[] readBuffer;
}

void LogArchiver::ArchiveIndex::newBlock(PageID firstPID)
{
    CRITICAL_SECTION(cs, mutex);

    w_assert1(bucketSize == 0);
    w_assert1(runs.size() > 0);

    BlockEntry e;
    e.offset = blockSize * runs.back().entries.size();
    e.pid = firstPID;
    runs.back().entries.push_back(e);
}

void LogArchiver::ArchiveIndex::newBlock(const vector<pair<PageID, size_t> >&
        buckets)
{
    CRITICAL_SECTION(cs, mutex);

    w_assert1(bucketSize > 0);

    size_t prevOffset = 0;
    for (size_t i = 0; i < buckets.size(); i++) {
        BlockEntry e;
        e.pid = buckets[i].first;
        e.offset = buckets[i].second;
        w_assert1(e.offset == 0 || e.offset > prevOffset);
        prevOffset = e.offset;
        runs.back().entries.push_back(e);
    }
}

rc_t LogArchiver::ArchiveIndex::finishRun(lsn_t first, lsn_t last, int fd,
        fileoff_t offset)
{
    CRITICAL_SECTION(cs, mutex);
    w_assert1(offset % blockSize == 0);

    // check if it isn't an empty run (from truncation)
    if (offset > 0 && lastFinished < (int) runs.size()) {
        lastFinished++;
        w_assert1(lastFinished == 0 || first == runs[lastFinished-1].lastLSN);
        w_assert1(lastFinished < (int) runs.size());

        runs[lastFinished].firstLSN = first;
        runs[lastFinished].lastLSN = last;
        W_DO(serializeRunInfo(runs[lastFinished], fd, offset));
    }

    return RCOK;
}

rc_t LogArchiver::ArchiveIndex::serializeRunInfo(RunInfo& run, int fd,
        fileoff_t offset)
{
    // Assumption: mutex is held by caller

    // lastPID is stored on first block, but we reserve space for it in every
    // block to simplify things
    int entriesPerBlock =
        (blockSize - sizeof(BlockHeader) - sizeof(PageID)) / sizeof(BlockEntry);
    int remaining = run.entries.size();
    int i = 0;
    size_t currEntry = 0;

    while (remaining > 0) {
        int j = 0;
        size_t bpos = sizeof(BlockHeader);
        while (j < entriesPerBlock && remaining > 0)
        {
            memcpy(writeBuffer + bpos, &run.entries[currEntry],
                        sizeof(BlockEntry));
            j++;
            currEntry++;
            remaining--;
            bpos += sizeof(BlockEntry);
        }
        BlockHeader* h = (BlockHeader*) writeBuffer;
        h->entries = j;
        h->blockNumber = i;

        // copy lastPID into last block (space was reserved above)
        // if (remaining == 0) {
        //     memcpy(writeBuffer + bpos, &run.lastPID, sizeof(PageID));
        // }

        W_COERCE(me()->pwrite(fd, writeBuffer, blockSize, offset));
        offset += blockSize;
        i++;
    }

    return RCOK;
}

rc_t LogArchiver::ArchiveIndex::deserializeRunInfo(RunInfo& run,
        const char* fname)
{
    // Assumption: mutex is held by caller
    int fd;
    // Using direct I/O
    int flags = smthread_t::OPEN_RDONLY | smthread_t::OPEN_DIRECT;
    W_DO(me()->open(fname, flags, 0744, fd));

    run.firstLSN = ArchiveDirectory::parseLSN(fname, false /* end */);
    run.lastLSN = ArchiveDirectory::parseLSN(fname, true /* end */);

    size_t indexBlockCount = 0;
    size_t dataBlockCount = 0;
    W_DO(getBlockCounts(fd, &indexBlockCount, &dataBlockCount));

    fileoff_t offset = dataBlockCount * blockSize;
    w_assert1(dataBlockCount == 0 || offset > 0);
    size_t lastOffset = 0;

    while (indexBlockCount > 0) {
        W_COERCE(me()->pread(fd, readBuffer, blockSize, offset));
        BlockHeader* h = (BlockHeader*) readBuffer;

        unsigned j = 0;
        size_t bpos = sizeof(BlockHeader);
        while(j < h->entries)
        {
            BlockEntry* e = (BlockEntry*)(readBuffer + bpos);
            w_assert1(lastOffset == 0 || e->offset > lastOffset);
            run.entries.push_back(*e);

            lastOffset = e->offset;
            bpos += sizeof(BlockEntry);
            j++;
        }
        indexBlockCount--;
        offset += blockSize;

        // if (indexBlockCount == 0) {
        //     // read lasPID from last block
        //     run.lastPID = *((PageID*) (readBuffer + bpos));
        // }
    }

    W_DO(me()->close(fd));
    return RCOK;
}

void LogArchiver::ArchiveIndex::init()
{
    std::sort(runs.begin(), runs.end());
    lastFinished = runs.size() - 2;
}

void LogArchiver::ArchiveIndex::appendNewEntry()
{
    CRITICAL_SECTION(cs, mutex);

    // if (runs.size() > 0) {
    //     runs.back().lastPID = lastPID;
    // }

    RunInfo newRun;
    runs.push_back(newRun);
}

rc_t LogArchiver::ArchiveIndex::loadRunInfo(const char* fname)
{
    RunInfo r;
    W_DO(deserializeRunInfo(r, fname));

    runs.push_back(r);

    return RCOK;
}

rc_t LogArchiver::ArchiveIndex::getBlockCounts(int fd, size_t* indexBlocks,
        size_t* dataBlocks)
{
    size_t fsize = ArchiveDirectory::getFileSize(fd);
    w_assert1(fsize % blockSize == 0);

    // skip emtpy runs
    if (fsize == 0) {
        if(indexBlocks) { *indexBlocks = 0; };
        if(dataBlocks) { *dataBlocks = 0; };
        return RCOK;
    }

    // read header of last block in file -- its number is the block count
    // Using direct I/O -- must read whole align block
    char* buffer;
    int res = posix_memalign((void**) &buffer, IO_ALIGN, IO_ALIGN);
    w_assert0(res == 0);
    W_DO(me()->pread(fd, buffer, IO_ALIGN, fsize - blockSize));

    BlockHeader* header = (BlockHeader*) buffer;
    if (indexBlocks) {
        *indexBlocks = header->blockNumber + 1;
    }
    if (dataBlocks) {
        *dataBlocks = (fsize / blockSize) - (header->blockNumber + 1);
        w_assert1(*dataBlocks > 0);
    }
    free(buffer);

    return RCOK;
}

size_t LogArchiver::ArchiveIndex::findRun(lsn_t lsn)
{
    // Assumption: mutex is held by caller
    if (lsn == lsn_t::null) {
        // full log replay (backup-less)
        return 0;
    }

    /*
     * CS: requests are more likely to access the last runs, so
     * we do a linear search instead of binary search.
     */
    w_assert1(lastFinished >= 0);

    if(lsn >= runs[lastFinished].lastLSN) {
        return lastFinished + 1;
    }

    int result = lastFinished;
    while (result > 0 && runs[result].firstLSN > lsn) {
        result--;
    }

    // skip empty runs
    while (runs[result].entries.size() == 0 && result <= lastFinished) {
        result++;
    }

    // caller must check if returned index is valid
    return result >= 0 ? result : runs.size();
}

size_t LogArchiver::ArchiveIndex::findEntry(RunInfo* run,
        PageID pid, int from, int to)
{
    // Assumption: mutex is held by caller

    if (from > to) {
        if (from == 0) {
            // Queried pid lower than first in run
            return 0;
        }
        // Queried pid is greater than last in run.  This should not happen
        // because probes must not consider this run if that's the case
        W_FATAL_MSG(fcINTERNAL, << "Invalid probe on archiver index! "
                << " PID = " << pid << " run = " << run->firstLSN);
    }

    // negative value indicates first invocation
    if (to < 0) { to = run->entries.size() - 1; }
    if (from < 0) { from = 0; }

    w_assert1(run);
    w_assert1(run->entries.size() > 0);

    // binary search for page ID within run
    size_t i;
    if (from == to) {
        i = from;
    }
    else {
        i = from/2 + to/2;
    }

    w_assert0(i < run->entries.size());

    if (run->entries[i].pid <= pid &&
            (i == run->entries.size() - 1 || run->entries[i+1].pid >= pid))
    {
        // found it! must first check if previous does not contain same pid
        while (i > 0 && run->entries[i].pid == pid)
                //&& run->entries[i].pid == run->entries[i-1].pid)
        {
            i--;
        }
        return i;
    }

    // not found: recurse down
    if (run->entries[i].pid > pid) {
        return findEntry(run, pid, from, i-1);
    }
    else {
        return findEntry(run, pid, i+1, to);
    }
}

void LogArchiver::ArchiveIndex::probeInRun(ProbeResult& res)
{
    // Assmuptions: mutex is held; run index and pid are set in given result
    size_t index = res.runIndex;
    w_assert1((int) index <= lastFinished);
    RunInfo* run = &runs[index];

    res.runBegin = runs[index].firstLSN;
    res.runEnd = runs[index].lastLSN;

    size_t entryBegin = 0;
    if (res.pidBegin == 0) {
        res.offset = 0;
    }
    else {
        entryBegin = findEntry(run, res.pidBegin);
        // decide if we mean offset zero or entry zero
        if (entryBegin == 0 && run->entries[0].pid >= res.pidBegin)
        {
            res.offset = 0;
        }
        else {
            res.offset = run->entries[entryBegin].offset;
        }
    }
}

void LogArchiver::ArchiveIndex::probe(std::vector<ProbeResult>& probes,
        PageID startPID, PageID endPID, lsn_t startLSN)
{
    CRITICAL_SECTION(cs, mutex);

    size_t index = findRun(startLSN);
    probes.clear();

    ProbeResult res;
    while ((int) index <= lastFinished) {
        if (runs[index].entries.size() > 0) {
            res.pidBegin = startPID;
            res.pidEnd = endPID;
            res.runIndex = index;
            probeInRun(res);
            probes.push_back(res);
        }
        index++;
    }

}

void LogArchiver::ArchiveIndex::dumpIndex(ostream& out)
{
    for (size_t i = 0; i < runs.size(); i++) {
        size_t offset = 0, prevOffset = 0;
        for (size_t j = 0; j < runs[i].entries.size(); j++) {
            offset = runs[i].entries[j].offset;
            out << "run " << i << " entry " << j <<
                " pid " << runs[i].entries[j].pid <<
                " offset " << offset <<
                " delta " << offset - prevOffset <<
                endl;
            prevOffset = offset;
        }
    }
}

LogArchiver::MergerDaemon::MergerDaemon(ArchiveDirectory* in,
        ArchiveDirectory* out)
    : indir(in), outdir(out)
{
    if (!outdir) { outdir = indir; }
    w_assert0(indir && outdir);
}

typedef LogArchiver::ArchiveDirectory::RunFileStats RunFileStats;

bool runComp(const RunFileStats& a, const RunFileStats& b)
{
    return a.beginLSN < b.beginLSN;
}

// CS TODO: this currently only works when merging contiguous runs in ascending
// order, and only for all available runs at once. It fits the purposes of our
// restore experiments, but it should be fixed in the future. See comments in
// the header file.
rc_t LogArchiver::MergerDaemon::runSync(size_t fanin, size_t maxRunSize)
{
    list<RunFileStats> stats;
    indir->listFileStats(stats);

    // sort list by LSN, since only contiguous runs are merged
    stats.sort(runComp);

    LogArchiver::BlockAssembly blkAssemb(outdir);

    int runNumber = 0;
    list<RunFileStats>::iterator iter = stats.begin();
    while (iter != stats.end()) {
        list<RunFileStats>::iterator begin = iter;
        size_t accumSize = 0, i = 0;

        // pick runs until getting fan-in or maxRunSize
        while (i < fanin && iter != stats.end() &&
                (maxRunSize == 0 || accumSize + iter->fileSize <= maxRunSize))
        {
            accumSize += iter->fileSize;
            iter++;
            i++;
        }
        w_assert0(maxRunSize == 0 || accumSize <= maxRunSize);

        // if (i == 0) {
        //     iter++;
        //     i++;
        //     continue;
        // }

        // if (i == 1 || (minRunSize > 0 && accumSize < minRunSize)) {
        //     // output too small or only one input -- don't merge
        //     continue;
        // }

        // now do the merge
        W_DO(doMerge(runNumber++, begin, iter, blkAssemb));
    }

    blkAssemb.shutdown();

    return RCOK;
}

rc_t LogArchiver::MergerDaemon::doMerge(int runNumber,
        list<RunFileStats>::const_iterator begin,
        list<RunFileStats>::const_iterator end,
        LogArchiver::BlockAssembly& blkAssemb)
{
    ArchiveScanner::RunMerger merger;

    ERROUT(<< "doMerge");
    list<RunFileStats>::const_iterator iter = begin;
    while (iter != end) {
        ERROUT(<< "Merging " << iter->beginLSN << "-" << iter->endLSN);
        ArchiveScanner::RunScanner* runScanner =
            new ArchiveScanner::RunScanner(
                iter->beginLSN, iter->endLSN, 0, 0, 0 /* offset */, indir);

        merger.addInput(runScanner);
        iter++;
    }

    if (merger.heapSize() > 0) {
        logrec_t* lr;
        blkAssemb.start(runNumber);
        while (merger.next(lr)) {
            if (!blkAssemb.add(lr)) {
                blkAssemb.finish();
                blkAssemb.start(runNumber);
                blkAssemb.add(lr);
            }
        }
        blkAssemb.finish();
    }

    return RCOK;
}
