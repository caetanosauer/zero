#include "w_defines.h"

#define SM_SOURCE
#define LOGARCHIVER_C

#include "logarchiver.h"
#include "sm_options.h"

#include <algorithm>
#include <sm_base.h>
#include <sstream>
#include <sys/stat.h>

// needed for skip_log
//#include "logdef_gen.cpp"

typedef mem_mgmt_t::slot_t slot_t;

// definition of static members
const char* LogArchiver::RUN_PREFIX = "archive_";
const char* LogArchiver::CURR_RUN_FILE = "current_run";
const char* LogArchiver::CURR_MERGE_FILE = "current_merge";
const size_t LogArchiver::MAX_LOGREC_SIZE = 3 * log_storage::BLOCK_SIZE;

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
    char *const fname = new char[smlevel_0::max_devname];
    if (!fname) W_FATAL(fcOUTOFMEMORY);
    w_auto_delete_array_t<char> ad_fname(fname);
    smlevel_0::log->make_log_name(nextPartition, fname,
            smlevel_0::max_devname);
    int flags = smthread_t::OPEN_RDONLY;
    W_COERCE(me()->open(fname, flags, 0744, fd));

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
    directory->openNewRun();

    while(true) {
        char const* src = buf->consumerRequest();
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
            if (lastLSN != lsn_t::null) {
                W_COERCE(directory->closeCurrentRun(lastLSN));
            }
            return; // finished is set on buf
        }

        DBGTHRD(<< "Picked block for write " << (void*) src);

        int run = BlockAssembly::getRunFromBlock(src);
        if (currentRun != run) {
            w_assert1(run == currentRun + 1);
            /*
             * Selection (producer) guarantees that logrec fits in block.
             * lastLSN is the LSN of the first log record in the new block
             * -- it will be used as the upper bound when renaming the file
             *  of the current run. This same LSN will be used as lower
             *  bound on the next run, which allows us to verify whether
             *  holes exist in the archive.
             */
            W_COERCE(directory->closeCurrentRun(lastLSN));
            W_COERCE(directory->openNewRun());
            currentRun = run;
            DBGTHRD(<< "Opening file for new run " << run
                    << " starting on LSN " << directory->getLastLSN());
        }

        lastLSN = BlockAssembly::getLSNFromBlock(src);
        W_COERCE(directory->append(src, blockSize));

        DBGTHRD(<< "Wrote out block " << (void*) src);

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
    std::string archdir = options.get_string_option("sm_archdir", "");
    size_t workspaceSize =
        options.get_int_option("sm_archiver_workspace_size", DFT_WSPACE_SIZE);
    size_t blockSize =
        options.get_int_option("sm_archiver_block_size", DFT_BLOCK_SIZE);

    eager = options.get_bool_option("sm_archiver_eager", DFT_EAGER);
    readWholeBlocks = options.get_bool_option(
            "sm_archiver_read_whole_blocks", DFT_READ_WHOLE_BLOCKS);
    slowLogGracePeriod = options.get_int_option(
            "sm_archiver_slow_log_grace_period", DFT_GRACE_PERIOD);

    if (archdir.empty()) {
        W_FATAL_MSG(fcINTERNAL,
                << "Option for archive directory must be specified");
    }

    directory = new ArchiveDirectory(archdir, blockSize);
    nextActLSN = directory->getStartLSN();

    consumer = new LogConsumer(directory->getStartLSN(), blockSize);
    heap = new ArchiverHeap(workspaceSize);
    blkAssemb = new BlockAssembly(directory);
}

void LogArchiver::initLogScanner(LogScanner* logScanner)
{
    // Commented out logrecs that have not been ported to Zero yet
    logScanner->setIgnore(logrec_t::t_comment);
    logScanner->setIgnore(logrec_t::t_compensate);
    logScanner->setIgnore(logrec_t::t_chkpt_begin);
    logScanner->setIgnore(logrec_t::t_chkpt_bf_tab);
    logScanner->setIgnore(logrec_t::t_chkpt_xct_tab);
    logScanner->setIgnore(logrec_t::t_chkpt_dev_tab);
    logScanner->setIgnore(logrec_t::t_chkpt_end);
    logScanner->setIgnore(logrec_t::t_mount_vol);
    logScanner->setIgnore(logrec_t::t_dismount_vol);
    logScanner->setIgnore(logrec_t::t_xct_abort);
    logScanner->setIgnore(logrec_t::t_xct_end);
    logScanner->setIgnore(logrec_t::t_xct_freeing_space);
    logScanner->setIgnore(logrec_t::t_restore_begin);
    logScanner->setIgnore(logrec_t::t_restore_segment);
    logScanner->setIgnore(logrec_t::t_restore_end);
    logScanner->setIgnore(logrec_t::t_chkpt_restore_tab);
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
    DBGTHRD(<< "LOG ARCHIVER SHUTDOWN STARTING");
    // this flag indicates that reader and writer threads delivering null
    // blocks is not an error, but a termination condition
    shutdownFlag = true;
    // make other threads see new shutdown value
    lintel::atomic_thread_fence(lintel::memory_order_release);
    consumer->shutdown();
    blkAssemb->shutdown();
    join();
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
    const char* hpos = strchr(str, delim) + 1;
    const char* lpos = strchr(hpos, '.') + 1;

    char* hstr = new char[11]; // the highest 32-bit integer has 10 digits
    strncpy(hstr, hpos, lpos - hpos - 1);
    char* lstr = (char*) lpos;
    if (!end) {
        lstr = new char[11];
        const char* lend = strchr(lpos, '-');
        strncpy(lstr, lpos, lend - lpos);
    }
    // use atol to avoid overflow in atoi, which generates signed int
    return lsn_t(atol(hstr), atol(lstr));
}

os_dirent_t* LogArchiver::ArchiveDirectory::scanDir(os_dir_t& dir)
{
    if (dir == NULL) {
        dir = os_opendir(archdir.c_str());
        if (!dir) {
            smlevel_0::errlog->clog << fatal_prio <<
                "Error: could not open log archive dir: " <<
                archdir << flushl;
            W_COERCE(RC(eOS));
        }
        //DBGTHRD(<< "Opened log archive directory " << archdir);
    }

    return os_readdir(dir);
}

LogArchiver::ArchiveDirectory::ArchiveDirectory(std::string archdir,
        size_t blockSize, bool createIndex)
    : archdir(archdir),
    appendFd(-1), mergeFd(-1), appendPos(0), blockSize(blockSize)
{
    // open archdir and extract last archived LSN
    {
        lsn_t highestLSN = lsn_t::null;
        os_dir_t dir = NULL;
        os_dirent_t* entry = scanDir(dir);
        while (entry != NULL) {
            const char* runName = entry->d_name;
            if (strncmp(RUN_PREFIX, runName, strlen(RUN_PREFIX)) == 0) {
                // parse lsn from file name
                lsn_t currLSN = parseLSN(runName);

                if (currLSN > highestLSN) {
                    DBGTHRD("Highest LSN found so far in archdir: "
                            << currLSN);
                    highestLSN = currLSN;
                }
            }
            if (strcmp(CURR_RUN_FILE, runName) == 0
                    || strcmp(CURR_MERGE_FILE, runName) == 0)
            {
                DBGTHRD(<< "Found unfinished log archive run. Deleting");
                string path = archdir + "/" + runName;
                if (unlink(path.c_str()) < 0) {
                    smlevel_0::errlog->clog << fatal_prio
                        << "Log archiver: failed to delete "
                        << runName << endl;
                    W_FATAL(fcOS);
                }
            }
            entry = scanDir(dir);
        }
        startLSN = highestLSN;
        os_closedir(dir);
    }

    // no runs found in archive log -- start from first available partition
    if (startLSN.hi() == 0) {
        int nextPartition = startLSN.hi();
        char *const fname = new char[smlevel_0::max_devname];
        if (!fname) W_FATAL(fcOUTOFMEMORY);

        w_assert0(smlevel_0::log);
        int max = smlevel_0::log->durable_lsn().hi();

        while (nextPartition <= max) {
            smlevel_0::log->make_log_name(nextPartition, fname,
                    smlevel_0::max_devname);

            // check if file exists
            struct stat st;
            if (stat(fname, &st) == 0) {
                break;
            }
            nextPartition++;
        }

        delete[] fname;

        if (nextPartition > max) {
            W_FATAL_MSG(fcINTERNAL,
                << "Could not find partition files in log manager");
        }

        startLSN = lsn_t(nextPartition, 0);
    }

    // lastLSN is the end LSN of the last generated initial run
    lastLSN = startLSN;

    // create/load index
    if (createIndex) {
        archIndex = new ArchiveIndex(blockSize, startLSN);

        std::vector<std::string> runFiles;
        listFiles(&runFiles);
        std::vector<std::string>::const_iterator it;
        for(it=runFiles.begin(); it!=runFiles.end(); ++it) {
            std::string fname = archdir + "/" + *it;
            archIndex->loadRunInfo(fname.c_str());
        }

        // sort runinfo vector by lsn
        if (runFiles.size() > 0) {
            archIndex->sortRunVector();
            archIndex->setLastFinished(runFiles.size() - 1);
        }
    }
    else {
        archIndex = NULL;
    }
}

LogArchiver::ArchiveDirectory::~ArchiveDirectory()
{
    if(archIndex) {
        delete archIndex;
    }
}

rc_t LogArchiver::ArchiveDirectory::listFiles(std::vector<std::string>* list)
{
    w_assert0(list && list->size() == 0);
    os_dir_t dir = NULL;
    os_dirent_t* entry = scanDir(dir);
    while (entry != NULL) {
        const char* runName = entry->d_name;
        if (strncmp(RUN_PREFIX, runName, strlen(RUN_PREFIX)) == 0) {
            list->push_back(std::string(runName));
        }
        entry = scanDir(dir);
    }
    os_closedir(dir);

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

rc_t LogArchiver::ArchiveDirectory::closeCurrentRun(lsn_t runEndLSN)
{
    if (appendFd < 0) {
        W_FATAL_MSG(fcINTERNAL,
            << "Attempt to close unopened run");
    }
    if (appendPos == 0) {
        // nothing was appended -- just close file and return
        w_assert0(runEndLSN == lsn_t::null);
        W_DO(me()->close(appendFd));
        appendFd = -1;
        return RCOK;
    }
    w_assert1(runEndLSN != lsn_t::null);

    std::stringstream fname;
    fname << archdir << "/" << LogArchiver::RUN_PREFIX
        << lastLSN << "-" << runEndLSN;

    std::string currentFName = archdir + "/" + CURR_RUN_FILE;
    W_DO(me()->frename(appendFd, currentFName.c_str(), fname.str().c_str()));

    // register index information and write it on end of file
    if (archIndex) {
        archIndex->finishRun(lastLSN, runEndLSN, appendFd, appendPos);
    }

    W_DO(me()->close(appendFd));
    appendFd = -1;
    DBGTHRD(<< "Closed current output run: " << fname.str());

    lastLSN = runEndLSN;

    return RCOK;
}

rc_t LogArchiver::ArchiveDirectory::append(const char* data, size_t length)
{
    W_COERCE(me()->pwrite(appendFd, data, length, appendPos));
    appendPos += length;
    return RCOK;
}

rc_t LogArchiver::ArchiveDirectory::openForScan(int& fd, lsn_t runBegin,
        lsn_t runEnd)
{
    std::stringstream fname;
    fname << archdir << "/" << LogArchiver::RUN_PREFIX
        << runBegin << "-" << runEnd;

    int flags = smthread_t::OPEN_RDONLY;
    W_DO(me()->open(fname.str().c_str(), flags, 0744, fd));

    return RCOK;
}

rc_t LogArchiver::ArchiveDirectory::readBlock(int fd, char* buf,
        fileoff_t& offset)
{
    rc_t rc = (me()->pread(fd, buf, blockSize, offset));
    if (rc.is_error()) {
        if (rc.err_num() == stSHORTIO) {
            // EOF is signalized by setting offset to zero
            offset = 0;
            return RCOK;
        }
        return rc;
    }

    offset += blockSize;
    return RCOK;
}

rc_t LogArchiver::ArchiveDirectory::closeScan(int& fd)
{
    W_DO(me()->close(fd));
    fd = -1;
    return RCOK;
}

LogArchiver::LogConsumer::LogConsumer(lsn_t startLSN, size_t blockSize)
    : nextLSN(startLSN), endLSN(lsn_t::null), currentBlock(NULL),
    blockSize(blockSize)
{
    DBGTHRD(<< "Starting log archiver at LSN " << nextLSN);

    // pos must be set to the correct offset within a block
    pos = startLSN.lo() % blockSize;

    readbuf = new AsyncRingBuffer(blockSize, IO_BLOCK_COUNT);
    reader = new ReaderThread(readbuf, startLSN);
    logScanner = new LogScanner(blockSize);

    initLogScanner(logScanner);
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
        DBGTHRD(<< "Replacement reached end LSN on " << nextLSN);
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

    if (!scanned || lr->type() == logrec_t::t_skip) {
        /*
         * nextLogrec returning false with nextLSN != endLSN means that we are
         * suppose to read another block and call the method again.
         */
        if (lr->type() == logrec_t::t_skip) {
            // Try again if reached skip -- next block should be from next file
            nextLSN = lsn_t(nextLSN.hi() + 1, 0);
            DBGTHRD(<< "Reached skip logrec, set nextLSN = " << nextLSN);
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

    if (!blkAssemb->start()) {
        return false;
    }

    int run = heap->topRun();
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
            //     heap->top()->construct_pid()>= lr->construct_pid());
        }
        else {
            break;
        }
    }
    blkAssemb->finish(run);

    return true;
}

LogArchiver::BlockAssembly::BlockAssembly(ArchiveDirectory* directory)
    : dest(NULL), lastLSN(lsn_t::null), lastLength(0),
    lastRun(-1)
{
    archIndex = directory->getIndex();
    blockSize = directory->getBlockSize();
    writebuf = new AsyncRingBuffer(blockSize, IO_BLOCK_COUNT);
    writer = new WriterThread(writebuf, directory);
    writer->fork();
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

int LogArchiver::BlockAssembly::getRunFromBlock(const char* b)
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

bool LogArchiver::BlockAssembly::start()
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
    return true;
}

bool LogArchiver::BlockAssembly::add(logrec_t* lr)
{
    w_assert0(dest);

    if (lr->length() > blockSize - pos) {
        return false;
    }

    if (firstPID == lpid_t::null) {
        firstPID = lr->construct_pid();
    }

    lastLSN = lr->lsn_ck();
    lastLength = lr->length();

    memcpy(dest + pos, lr, lr->length());
    pos += lr->length();
    return true;
}

void LogArchiver::BlockAssembly::finish(int run)
{
    DBGTHRD("Selection produced block for writing " << (void*) dest <<
            " in run " << (int) run << " with end " << pos);

    if (lastRun < 0) {
        // lazy initialization
        lastRun = run;
    }

    if (archIndex) {
        // Add new entry to index when we see a new run coming.
        if (run != lastRun) {
            archIndex->appendNewEntry();
            lastRun = run;
        }

        archIndex->newBlock(firstPID);
    }
    firstPID = lpid_t::null;

    // write block header info
    BlockHeader* h = (BlockHeader*) dest;
    h->run = run;
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
    h->lsn = lastLSN.advance(lastLength);

    writebuf->producerRelease();
    dest = NULL;
}

void LogArchiver::BlockAssembly::shutdown()
{
    w_assert0(!dest);
    while (dest) {
        DBGTHRD(<< "BlockAssembly still has an active block -- waiting");
        ::usleep(1000); // 1ms
    }

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
LogArchiver::ArchiveScanner::open(lpid_t startPID, lpid_t endPID,
        lsn_t startLSN, lsn_t endLSN)
{
    RunMerger* merger = new RunMerger();

    // probe for runs
    ArchiveIndex::ProbeResult* runProbe =
        archIndex->probeFirst(startPID, startLSN);

    while (runProbe) {
        RunScanner* runScanner = new RunScanner(
                runProbe->runBegin,
                runProbe->runEnd,
                startPID,
                endPID,
                runProbe->offset,
                directory
        );

        merger->addInput(runScanner);
        archIndex->probeNext(runProbe, endLSN);
    }

    return merger;
}

LogArchiver::ArchiveScanner::RunScanner::RunScanner(lsn_t b, lsn_t e,
        lpid_t f, lpid_t l, fileoff_t o, ArchiveDirectory* directory)
: runBegin(b), runEnd(e), firstPID(f), lastPID(l), offset(o),
    directory(directory), fd(-1), blockCount(0)
{
    buffer = new char[directory->getBlockSize()];
    bpos = 0;
    blockEnd = 0;
}

LogArchiver::ArchiveScanner::RunScanner::~RunScanner()
{
    if (fd > 0) {
        W_COERCE(directory->closeScan(fd));
    }

    delete[] buffer;
}

bool LogArchiver::ArchiveScanner::RunScanner::nextBlock()
{
    if (fd < 0) {
        W_COERCE(directory->openForScan(fd, runBegin, runEnd));

        if (directory->getIndex()) {
            directory->getIndex()->getBlockCounts(fd, NULL, &blockCount);
        }
        w_assert1(offset / directory->getBlockSize() < blockCount);
    }

    // do not read past data blocks into index blocks
    if (blockCount > 0 &&
            (offset / directory->getBlockSize()) >= blockCount)
    {
        W_COERCE(directory->closeScan(fd));
        return false;
    }

    // offset is updated by readBlock
    W_COERCE(directory->readBlock(fd, buffer, offset));

    // offset set to zero indicates EOF
    if (offset == 0) {
        W_COERCE(directory->closeScan(fd));
        return false;
    }

    bpos = sizeof(BlockAssembly::BlockHeader);
    blockEnd = BlockAssembly::getEndOfBlock(buffer);
    w_assert1(blockEnd > bpos);
    w_assert1(blockEnd <= directory->getBlockSize());

    return true;
}

bool LogArchiver::ArchiveScanner::RunScanner::next(logrec_t*& lr)
{
    if (bpos >= blockEnd) {
        if (!nextBlock()) {
            return false;
        }
    }

    lr = (logrec_t*) (buffer + bpos);
    w_assert1(lr->valid_header(lr->lsn_ck()));

    if (lastPID != lpid_t::null && lr->pid() >= lastPID) {
        // end of scan
        return false;
    }

    bpos += lr->length();
    return true;
}

std::ostream& operator<< (ostream& os,
        const LogArchiver::ArchiveScanner::RunScanner& m)
{
    os << m.runBegin << "-" << m.runEnd;
    return os;
}

LogArchiver::ArchiveScanner::MergeHeapEntry::MergeHeapEntry(RunScanner* runScan)
    : active(true), runScan(runScan)
{
    lpid_t startPID = runScan->firstPID;
    // bring scanner up to starting point
    logrec_t* next = NULL;
    bool hasNext = false;
    bool matches = false;
    do {
        hasNext = runScan->next(next);
        if (hasNext) {
            matches = next->construct_pid() >= startPID;
        }
    } while (!matches && hasNext);

    // if all PIDs are lower than search criterion, run is not needed
    if (!hasNext)
    {
        active = false;
        return;
    }

    lr = next;
    lsn = lr->lsn_ck();
    pid = lr->construct_pid();

    w_assert1(lr->valid_header());
    w_assert1(!active || startPID <= pid);
    w_assert1(!active || startPID == lpid_t::null || startPID.vol() == pid.vol());
}

void LogArchiver::ArchiveScanner::MergeHeapEntry::moveToNext()
{
    if (runScan->next(lr)) {
        pid = lr->construct_pid();
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
    if (entry.active) {
        heap.AddElementDontHeapify(entry);
    }
    else {
        delete entry.runScan;
    }
}

bool LogArchiver::ArchiveScanner::RunMerger::next(logrec_t*& lr)
{
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
        while (heap.NumElements() > 0) {
            delete heap.RemoveFirst().runScan;
        }
        return false;
    }

    lr = heap.First().lr;
    return true;
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

bool LogArchiver::ArchiverHeap::push(logrec_t* lr)
{
    slot_t dest(NULL, 0);
    W_COERCE(workspace->allocate(lr->length(), dest));

    if (!dest.address) {
        // workspace full -> do selection until space available
        DBGTHRD(<< "Heap full! Size: " << w_heap.NumElements());
        if (!filledFirst) {
            // first run generated by first full load of w_heap
            currentRun++;
            filledFirst = true;
            DBGTHRD(<< "Heap full for the first time; start run 1");
        }
        return false;
    }
    memcpy(dest.address, lr, lr->length());

    // if all records of the current run are gone, start new run
    if (filledFirst &&
            (size() == 0 || w_heap.First().run == currentRun)) {
        currentRun++;
        DBGTHRD(<< "Replacement starting new run " << (int) currentRun
                << " on LSN " << lr->lsn_ck());
    }

    //DBGTHRD(<< "Processing logrec " << lr->lsn_ck() << ", type " <<
    //        lr->type() << "(" << lr->type_str() << ") length " <<
    //        lr->length() << " into run " << (int) currentRun);

    // insert key and pointer into w_heap
    HeapEntry k(currentRun, lr->construct_pid(), lr->lsn_ck(), dest);

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

        pushIntoHeap(lr);

        if (lr->is_multi_page()) {
            // CS: Multi-page log records are replicated so that each page can
            // be recovered from the log archive independently.  Note that this
            // is not required for Restart or Single-page recovery because
            // following the per-page log chain of both pages eventually lands
            // on the same multi-page log record. For restore, it must be
            // duplicated because log records are sorted and there is no chain.
            lr->set_pid(lr->construct_pid2());
            lr->set_page_prev_lsn(lr->page2_prev_lsn());
            pushIntoHeap(lr);
        }
    }
}

void LogArchiver::pushIntoHeap(logrec_t* lr)
{
    while (!heap->push(lr)) {
        // heap full -- invoke selection and try again
        DBGTHRD(<< "Heap full! Invoking selection");
        bool success = selection();

        w_assert0(success || heap->size() == 0);

        if (heap->size() == 0) {
            W_FATAL_MSG(fcINTERNAL,
                    << "Heap empty but push not possible!");
        }
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
        w_assert0(flushReqLSN <= smlevel_0::log->durable_lsn());
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
            // Heap empty: Wait for writer thread to consume all blocks
            w_assert0(heap->size() == 0);
            while (blkAssemb->hasPendingBlocks()) {
                ::usleep(10000); // 10ms
            }

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
    int minActWindow = directory->getBlockSize();
    if (eager && control.endLSN.hi() == nextActLSN.hi() &&
            control.endLSN.lo() - nextActLSN.lo() < minActWindow)
    {
        // If this happens to often, the block size should be decreased.
        ::usleep(slowLogGracePeriod);
        // To better exploit device bandwidth, we only start archiving if
        // at least one block worth of log is available for consuption.
        // This happens when the log is growing too slow.
        // However, if it seems like log activity has stopped (i.e.,
        // durable_lsn did not advance since we started), then we proceed
        // with the small activation window.
        if (control.endLSN.hi() != nextActLSN.hi() ||
                control.endLSN.lo() - nextActLSN.lo() >= minActWindow)
        {
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

        processFlushRequest();

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
    while (!requestFlushAsync(reqLSN))
    {}
    // when log archiver is done processing the flush request, it will set
    // flushReqLSN back to null
    while(true) {
        lintel::atomic_thread_fence(lintel::memory_order_acquire);
        if (flushReqLSN == lsn_t::null) {
            break;
        }
        ::usleep(1000); // 1ms
    }
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
        DBGTHRD(<< "Reading partial log record -- missing: "
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
        DBGTHRD(<< "Log record with length "
                << (remaining > 1 ? lr->length() : -1)
                << " does not fit in current block of " << remaining);
        w_assert0(remaining <= LogArchiver::MAX_LOGREC_SIZE);
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
        DBGTHRD(<< "Unexpected LSN in scanner: " << lr->lsn_ck()
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

/*
 * TODO initialize from existing runs at startup
 */
LogArchiver::ArchiveIndex::ArchiveIndex(size_t blockSize, lsn_t startLSN)
    : blockSize(blockSize), lastLSN(startLSN), lastFinished(-1)
{
    DO_PTHREAD(pthread_mutex_init(&mutex, NULL));
    writeBuffer = new char[blockSize];
    readBuffer = new char[blockSize];

    // last run in the array is always the one being currently generated
    RunInfo r;
    r.firstLSN = startLSN;
    runs.push_back(r);
}

LogArchiver::ArchiveIndex::~ArchiveIndex()
{
    DO_PTHREAD(pthread_mutex_destroy(&mutex));
    delete[] writeBuffer;
    delete[] readBuffer;
}

void LogArchiver::ArchiveIndex::newBlock(lpid_t lastPID)
{
    CRITICAL_SECTION(cs, mutex);

    w_assert1(runs.size() > 0);

    BlockEntry e;
    e.offset = blockSize * runs.back().entries.size();
    e.pid = lastPID;
    runs.back().entries.push_back(e);
}

rc_t LogArchiver::ArchiveIndex::finishRun(lsn_t first, lsn_t last, int fd,
        fileoff_t offset)
{
    CRITICAL_SECTION(cs, mutex);

    w_assert0(lastLSN == first);

    lastFinished++;

    runs[lastFinished].firstLSN = first;
    W_DO(serializeRunInfo(runs[lastFinished], fd, offset));
    lastLSN = last;

    return RCOK;
}

rc_t LogArchiver::ArchiveIndex::serializeRunInfo(RunInfo& run, int fd, fileoff_t offset)
{
    // Assumption: mutex is held by caller

    int entriesPerBlock =
        (blockSize - sizeof(BlockHeader)) / sizeof(BlockEntry);
    int remaining = run.entries.size();
    int i = 0;

    while (remaining > 0) {
        int j = 0;
        while (j < entriesPerBlock && remaining > 0)
        {
            unsigned pos1 = sizeof(BlockHeader) + (j * sizeof(BlockEntry));
            unsigned pos2 = (i*entriesPerBlock) + j;
            memcpy(writeBuffer + pos1, &run.entries[pos2],
                        sizeof(BlockEntry));
            j++;
            remaining--;
        }
        BlockHeader* h = (BlockHeader*) writeBuffer;
        h->entries = j;
        h->blockNumber = i;

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
    int flags = smthread_t::OPEN_RDONLY;
    W_DO(me()->open(fname, flags, 0744, fd));

    run.firstLSN = ArchiveDirectory::parseLSN(fname, false /* end */);

    size_t indexBlockCount = 0;
    size_t dataBlockCount = 0;
    getBlockCounts(fd, &indexBlockCount, &dataBlockCount);

    fileoff_t offset = dataBlockCount * blockSize;

    int i = 0;
    while (indexBlockCount > 0) {
        W_DO(me()->pread(fd, readBuffer, blockSize, offset));
        BlockHeader* h = (BlockHeader*) readBuffer;

        unsigned j = 0;
        while(j < h->entries)
        {
            unsigned pos = sizeof(BlockHeader) + (j * sizeof(BlockEntry));
            BlockEntry* e = (BlockEntry*)(readBuffer + pos);
            run.entries.push_back(*e);

            j++;
            i++;
        }
        indexBlockCount--;
        offset = offset + blockSize;
    }

    W_DO(me()->close(fd));
    return RCOK;
}

void LogArchiver::ArchiveIndex::sortRunVector()
{
    std::sort(runs.begin(), runs.end());
}

void LogArchiver::ArchiveIndex::appendNewEntry()
{
    CRITICAL_SECTION(cs, mutex);

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
    filestat_t fs;
    W_DO(me()->fstat(fd, fs));
    fileoff_t fsize = fs.st_size;
    w_assert1(fsize % blockSize == 0);

    // read header of last block in file -- its number is the block count
    BlockHeader header;
    W_DO(me()->pread(fd, &header, sizeof(BlockHeader), fsize - blockSize));
    if (indexBlocks) {
        *indexBlocks = header.blockNumber + 1;
    }
    if (dataBlocks) {
        *dataBlocks = (fsize / blockSize) - (header.blockNumber + 1);
    }

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
    int result = lastFinished;
    while (result > 0 && runs[result].firstLSN >= lsn) {
        result--;
    }

    if(result == 0 && runs[result].firstLSN > lsn) {
        // looking for an LSN which is not contained in the archive
        // (can only happen if old runs were recycled)
        result = -1;
    }

    // caller must check if returned index is valid
    return result >= 0 ? result : runs.size();
}

smlevel_0::fileoff_t LogArchiver::ArchiveIndex::findEntry(RunInfo* run,
        lpid_t pid, int from, int to)
{
    // Assumption: mutex is held by caller

    // binary search for page ID within run

    if (from > to) {
        if (from == 0) {
            // Queried pid lower than first in run. Just return first entry.
            return run->entries[0].offset;
        }
        // Queried pid is greater than last in run.  This should not happen
        // because probes must not consider this run if that's the case
        W_FATAL_MSG(fcINTERNAL, << "Invalid probe on archiver index! "
                << " PID = " << pid << " run = " << run->firstLSN);
    }

    w_assert1(run);
    w_assert1(run->entries.size() > 0);

    if (to < 0) { // negative value indicates first invocation
        from = 0;
        to = run->entries.size() - 1;
    }

    size_t i;
    if (from == to) {
        i = from;
    }
    else {
        i = from/2 + to/2;
    }

    w_assert0(i < run->entries.size());

    /*
     * CS comparisons use just page number instead of whole lpid_t, which
     * means that multiple volumes are not supported (TODO)
     * To fix this, we ought to look for usages of the lpid_t operators
     * in sm_s.h and see what the semantics is.
     */
    if (run->entries[i].pid <= pid &&
            (i == run->entries.size() - 1 || run->entries[i+1].pid >= pid))
    {
        // found it! must first check if previous does not contain same pid
        while (i > 0 && run->entries[i].pid == pid)
                //&& run->entries[i].pid == run->entries[i-1].pid)
        {
            i--;
        }
        return run->entries[i].offset;
    }

    // not found: recurse down
    if (run->entries[i].pid > pid) {
        return findEntry(run, pid, from, i-1);
    }
    else {
        return findEntry(run, pid, i+1, to);
    }
}

typedef LogArchiver::ArchiveIndex::ProbeResult ProbeResult;

void LogArchiver::ArchiveIndex::probeInRun(ProbeResult* result)
{
    // Assmuptions: mutex is held; run index and pid are set in given result
    size_t index = result->runIndex;
    result->runBegin = runs[index].firstLSN;
    if ((int) index < lastFinished) { // unfinished runs are unavailable for probes
        result->runEnd = runs[index + 1].firstLSN;
    }
    else {
        result->runEnd = lastLSN;
    }
    result->offset = findEntry(&runs[index], result->pid);
}

ProbeResult* LogArchiver::ArchiveIndex::probeFirst(lpid_t pid, lsn_t lsn)
{
    CRITICAL_SECTION(cs, mutex);

    lsn_t runEndLSN;
    size_t index = findRun(lsn);

    if ((int) index > lastFinished) {
        // runs beyond lastFinished are unavailable for probing
        return NULL;
    }

    ProbeResult* result = new ProbeResult();
    result->pid = pid;
    result->runIndex = index;
    probeInRun(result);

    return result;
}

void LogArchiver::ArchiveIndex::probeNext(ProbeResult*& prev, lsn_t endLSN)
{
    CRITICAL_SECTION(cs, mutex);

    size_t index = prev->runIndex + 1;
    w_assert0(index <= runs.size());
    if (
        (endLSN != lsn_t::null && endLSN < prev->runEnd) || // passed given end
        ((int) index > lastFinished) // no more (finished) runs available
       )
    {
        delete prev;
        prev = NULL;
        return;
    }

    prev->runIndex = index;
    probeInRun(prev);
}

ArchiveMerger::ArchiveMerger(const sm_options& options)
    : smthread_t(t_regular, "ArchiveMerger"),
      shutdownFlag(false), control(&shutdownFlag)
{
    archdir = options.get_string_option("sm_archdir", "");
    mergeFactor = options.get_int_option("sm_merge_factor", DFT_MERGE_FACTOR);
    blockSize = options.get_int_option("sm_archiving_blocksize",
            LogArchiver::DFT_BLOCK_SIZE);
}

bool ArchiveMerger::activate(bool wait)
{
    return control.activate(wait);
}

void ArchiveMerger::run()
{
    while(true) {
        CRITICAL_SECTION(cs, control.mutex);
        bool activated = control.waitForActivation();
        if (!activated) {
            return;
        }

        MergeOutput* mergeOut = offlineMerge(true);
        if (mergeOut) {
            DBGTHRD(<< "Archive merger activated");

            char* lrbuf = new char[blockSize];
            size_t fpos = 0, pos = 0, copied = 0;

            // open new merged run file
            int fd = -1;
            string mergeFile = string(archdir)
                + "/current_merge";
            const int flags = smthread_t::OPEN_WRONLY | smthread_t::OPEN_SYNC
                | smthread_t::OPEN_CREATE;
            const int rwmode = 0744;
            W_COERCE(me()->open(mergeFile.c_str(), flags, rwmode, fd));

            while((copied = mergeOut->copyNext(lrbuf + pos)) > 0) {
                pos += copied;
                if (pos > blockSize - LogArchiver::MAX_LOGREC_SIZE) {
                    // maximum logrec won't fit in this block -- flush it and reset
                    DBGTHRD(<< "Writing block to current merge output");
                    W_COERCE(me()->pwrite(fd, lrbuf, pos, fpos));
                    fpos += pos;
                    pos = 0;
                }
            }

            if (pos > 0) {
                DBGTHRD(<< "Writing LAST block of current merge output");
                W_COERCE(me()->pwrite(fd, lrbuf, pos, fpos));
                fpos += pos;
                // Append skip log record
                //W_COERCE(me()->pwrite(fd, FAKE_SKIP_LOGREC, 3, fpos));
            }
            delete[] lrbuf;

            // rename and close merged run
            stringstream newFile;
            newFile << archdir << "/"
                << LogArchiver::RUN_PREFIX
                << mergeOut->firstLSN << "-" << mergeOut->lastLSN;
            W_COERCE(me()->frename(fd, mergeFile.c_str(),
                        newFile.str().c_str()));
            W_COERCE(me()->close(fd));

            // delete consumed input runs
            mergeOut->dumpHeap(cout);
            W_COERCE(mergeOut->cleanup());


            DBGTHRD(<< "Archive merger finished");
        }

        control.activated = false;
    }
}

ArchiveMerger::MergeInput::MergeInput(char* fname, int blockSize)
    : logrec(NULL), fpos(0), bpos(0), fname(fname)
{
    scanner = new LogScanner(blockSize);
    buf = new char[blockSize];

    rc_t rc = me()->open(fname, smthread_t::OPEN_RDONLY, 0, fd);
    if (rc.is_error()) {
        W_COERCE(rc);
    }

#if W_DEBUG_LEVEL>=1
    truncated = false;
#endif

    fetchFromNextBlock();
    w_assert1(hasNext); // file is empty
};

void ArchiveMerger::MergeInput::fetchFromNextBlock()
{
    int howmuch = 0;
    W_COERCE(me()->pread_short(fd, buf, scanner->getBlockSize(), fpos,
                howmuch));
    fpos += howmuch;

    if (howmuch == 0) { // EOF
        hasNext = false;
        return;
    }

#if W_DEBUG_LEVEL>=1
    if (howmuch < (int) scanner->getBlockSize()) {
        DBGTHRD(<< "SHORT IO on fetchFromNextBlock. fpos = " << fpos
                << " fd = " << fd << " howmuch = " << howmuch);
    }
#endif

    // scanner does not need to know about 'howmuch', because it finds
    // a skip log record at the end of every run
    bpos = 0;
    hasNext = scanner->nextLogrec(buf, bpos, logrec);
    w_assert1(!hasNext || logrec != NULL);
}

void ArchiveMerger::MergeInput::next()
{
    if (!hasNext) {
        return;
    }

    hasNext = scanner->nextLogrec(buf, bpos, logrec);
#if W_DEBUG_LEVEL>=1
    truncated = !hasNext;
#endif

    if (!hasNext) {
        fetchFromNextBlock();
    }

    /*
#if W_DEBUG_LEVEL>=1
    // TODO make a conditional DBG output macro
    if (hasNext) {
        DBGTHRD(<< "Merge input: " << *this);
    }
#endif
    */
}

char** ArchiveMerger::pickRunsToMerge(int& count, lsn_t& firstLSN,
        lsn_t& lastLSN, bool async)
{
    (void) count;
    (void) firstLSN;
    (void) lastLSN;
    (void) async;
    // TODO reimplement for new ArchiveDirectory
//    RunKeyCmp runCmp;
//    Heap<RunKey, RunKeyCmp> runHeap(runCmp);
//    os_dir_t dir = NULL;
//    os_dirent_t* dent = LogArchiver::scanDir(archdir, dir);
//    count = 0;
//
//    while(dent != NULL) {
//        char *const fname = new char[smlevel_0::max_devname];
//        strcpy(fname, dent->d_name);
//        if (strncmp(LogArchiver::RUN_PREFIX, fname,
//                    strlen(LogArchiver::RUN_PREFIX)) == 0)
//        {
//            RunKey k(LogArchiver::parseLSN(fname, false), fname);
//            if (count < mergeFactor) {
//                runHeap.AddElement(k);
//                count++;
//            }
//            else {
//                // replace if current is smaller than largest in the heap
//                if (k.lsn < runHeap.First().lsn) {
//                    delete runHeap.First().filename;
//                    runHeap.RemoveFirst();
//                    runHeap.AddElement(k);
//                }
//            }
//        }
//        dent = LogArchiver::scanDir(archdir, dir);
//    }
//    os_closedir(dir);
//
//    if (count == 0) {
//        return NULL;
//    }
//
//    // Heuristic: only start merging if there are at least mergeFactor/2 runs available
//    if (!async || count >= mergeFactor/2) {
//        // Now the heap contains the oldest runs in descending order (newest first)
//        lastLSN = LogArchiver::parseLSN(runHeap.First().filename, true);
//        char** files = new char*[count];
//        for (int i = 0; i < count; i++) {
//            char *const fname = new char[smlevel_0::max_devname];
//            if (!fname) { W_FATAL(fcOUTOFMEMORY); }
//            w_ostrstream s(fname, smlevel_0::max_devname);
//            s << archdir << "/" <<
//                runHeap.First().filename << ends;
//            DBGTHRD(<< "Picked run for merge: "
//                    << runHeap.First().filename);
//            delete runHeap.First().filename;
//            files[i] = fname;
//            runHeap.RemoveFirst();
//            if (runHeap.NumElements() == 1) {
//                firstLSN = runHeap.First().lsn;
//            }
//        }
//        return files;
//    }

    return NULL;
}

ArchiveMerger::MergeOutput* ArchiveMerger::offlineMerge(bool async)
{
    int count;
    lsn_t firstLSN, lastLSN;
    char** files = pickRunsToMerge(count, firstLSN, lastLSN, async);

    if (files != NULL) {
        MergeOutput* output = new MergeOutput(firstLSN, lastLSN);
        for (int i = 0; i < count; i++) {
            DBGTHRD(<< "Opening run for merge: " << files[i]);
            output->addInputEntry(new MergeInput(files[i], blockSize));
        }

        delete[] files;
        return output;
    }

    return NULL;
}

size_t ArchiveMerger::MergeOutput::copyNext(char* dest)
{
    while (heap.First().active) {
        MergeInput* m = heap.First().input;
        if (m->hasNext) {
            size_t recsize = m->logrec->length();
#if W_DEBUG_LEVEL >= 1
            if(!m->logrec->valid_header(lsn_t::null)) {
                dumpHeap(cout);
            }
#endif
            w_assert1(m->logrec->valid_header(lsn_t::null));
            memcpy(dest, m->logrec, recsize);
            m->next();
            heap.ReplacedFirst();
            return recsize;
        }

        DBGTHRD(<< "Finished merge input: " << *m);
        heap.First().active = false;
        heap.ReplacedFirst();
    }
    return 0;
}

void ArchiveMerger::MergeOutput::dumpHeap(ostream& out)
{
    heap.Print(out);
}

/*
 * Delete all files opened as inputs for this merge.
 */
rc_t ArchiveMerger::MergeOutput::cleanup()
{
    while (heap.NumElements() > 0) {
        w_assert0(!heap.First().active);
        DBGTHRD(<< "Deleting input run " << heap.First().input->fname);
        int res = unlink(heap.First().input->fname);
        w_assert0(res == 0);
        delete heap.First().input;
        heap.RemoveFirst();
    }
    return RCOK;
}

void ArchiveMerger::shutdown()
{
    DBGTHRD(<< "ARCHIVE MERGER SHUTDOWN STARTING");
    shutdownFlag = true;
    // If merger thread is not running, it is woken up and terminated
    // imediately afterwards due to shutdown flag being set
    DO_PTHREAD(pthread_cond_signal(&control.activateCond));
}
