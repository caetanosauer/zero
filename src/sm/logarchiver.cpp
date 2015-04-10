#include "w_defines.h"

#define SM_SOURCE
#define LOGARCHIVER_C

#include "logarchiver.h"
#include "sm_options.h"

#include <sm_int_1.h>
#include <sstream>
#include <sys/stat.h>

// needed for skip_log
//#include "logdef_gen.cpp"

#define CHECK_ERROR_BASE(x, y) \
    do { \
        w_rc_t rc = x; \
        if (rc.is_error()) { \
            returnRC = rc; \
            y; \
        } \
    } while (false);
#define CHECK_ERROR(x) CHECK_ERROR_BASE(x, return);
#define CHECK_ERROR_BOOL(x) CHECK_ERROR_BASE(x, return false);


typedef mem_mgmt_t::slot_t slot_t;

// definition of static members
const char* LogArchiver::RUN_PREFIX = "archive_";
const char* LogArchiver::CURR_RUN_FILE = "current_run";
const char* LogArchiver::CURR_MERGE_FILE = "current_merge";
const size_t LogArchiver::MAX_LOGREC_SIZE = 3 * log_storage::BLOCK_SIZE;
// TODO this does not work because of bug with invalid xtc objects in smthread
//const logrec_t* WriterThread::SKIP_LOGREC = new skip_log();
//
// Fake skip logrec: first 2 bytes are length of 3 in little endian.
// Third byte indicates the logrec type.
// These three bytes when read by LogScanner will signalize end of partition,
// even though it is not a real skip logrec due to bug above.
const char FAKE_SKIP_LOGREC [3] = { 0x03, 0x00, (char) logrec_t::t_skip };

ArchiverControl::ArchiverControl(bool* shutdown)
    : endLSN(lsn_t::null), activated(false), shutdown(shutdown)
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

    DBGTHRD(<< "Sending activate signal to controlled thread");
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
    return true;
}

bool ArchiverControl::waitForActivation()
{
    // WARNING: mutex must be held by caller!
    while(!activated) {
        struct timespec timeout;
        sthread_t::timeout_to_timespec(100, timeout); // 100ms
        int code = pthread_cond_timedwait(&activateCond, &mutex, &timeout);
        if (code == ETIMEDOUT) {
            //DBGTHRD(<< "Wait timed out -- try again");
            if (*shutdown) {
                DBGTHRD(<< "Activation failed due to shutdown. Exiting");
                return false;
            }
        }
        DO_PTHREAD_TIMED(code);
    }
    return true;
}

LogArchiver::ReaderThread::ReaderThread(AsyncRingBuffer* readbuf, lsn_t startLSN) :
      BaseThread(readbuf, "LogArchiver_ReaderThread"),
      shutdown(false), control(&shutdown), prevPos(0)
{
    // position initialized to startLSN
    pos = startLSN.lo();
    nextPartition = startLSN.hi();
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
            if (control.endLSN.hi() == nextPartition - 1
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
                CHECK_ERROR(openPartition());
            }


            int bytesRead = 0;
            CHECK_ERROR(me()->pread_short(
                        currentFd, dest, blockSize, pos, bytesRead));

            if (bytesRead == 0) {
                // Reached EOF -- open new file and try again
                CHECK_ERROR(openPartition());
                pos = 0;
                CHECK_ERROR(me()->pread_short(
                            currentFd, dest, blockSize, pos, bytesRead));
                if (bytesRead == 0) {
                    W_FATAL_MSG(fcINTERNAL,
                        << "Error reading from partition "
                        << nextPartition - 1);
                }
            }

            DBGTHRD(<< "Read block " << (void*) dest << " from fpos " << pos <<
                    " with size " << bytesRead);

            pos += bytesRead;
            if (control.endLSN.hi() == nextPartition - 1
                    && pos >= control.endLSN.lo())
            {
                /*
                 * CS: If we've just read the block containing the endLSN,
                 * manually insert a fake skip logrec at the endLSN position,
                 * to guarantee that replacement will not read beyond it, even
                 * if archiver gets activated multiple times while the reader
                 * thread is still running (i.e., reader "misses" some
                 * activations) [Bug of Sep 26, 2014]
                 */
                // amount read from file in this current activation
                size_t readSoFar = control.endLSN.lo() - prevPos;
                w_assert1(readSoFar > 0);
                size_t skipPos = readSoFar % blockSize;
                logrec_t* lr = (logrec_t*) (dest + skipPos);
                if (lr->type() != logrec_t::t_skip) {
                    DBGTHRD(<< "Reader setting skip logrec manually on block "
                            << " offset " << skipPos << " LSN "
                            << control.endLSN);
                    // TODO -- get rid of fake logrec
                    memcpy(lr, FAKE_SKIP_LOGREC, 3);
                }
                w_assert0(lr->type() == logrec_t::t_skip);
            }

            buf->producerRelease();
        }

        control.activated = false;
    }

    returnRC = RCOK;
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
             * is marked finished, which is done in start_shutdown().
             */
            DBGTHRD(<< "Finished flag set on writer thread");
            returnRC = directory->closeCurrentRun(lastLSN);
            return; // finished is set on buf
        }
        
        DBGTHRD(<< "Picked block for write " << (void*) src);

        // CS: changed writer behavior to write raw blocks instead of a
        // contiguous stream of log records, as done in log partitions.
        // TODO: restore code should consider block format when reading
        // from the log archive!
        
        //BlockHeader* h = (BlockHeader*) src;
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
            CHECK_ERROR(directory->closeCurrentRun(lastLSN));
            CHECK_ERROR(directory->openNewRun());
            currentRun = run;
            DBGTHRD(<< "Opening file for new run " << run
                    << " starting on LSN " << directory->getLastLSN());
        }

        lastLSN = BlockAssembly::getLSNFromBlock(src);
        CHECK_ERROR(directory->append(src, blockSize));

        DBGTHRD(<< "Wrote out block " << (void*) src);

        buf->consumerRelease();
    }
}

LogArchiver::LogArchiver(
        ArchiveDirectory* d, LogConsumer* c, ArchiverHeap* h, BlockAssembly* b)
    :
    smthread_t(t_regular, "LogArchiver"),
    directory(d), consumer(c), heap(h), blkAssemb(b),
    shutdown(false), control(&shutdown), selfManaged(false)
{
}

LogArchiver::LogArchiver(const sm_options& options)
    : smthread_t(t_regular, "LogArchiver"),
    shutdown(false), control(&shutdown), selfManaged(true)
{
    std::string archdir = options.get_string_option("sm_archdir", "");
    size_t workspaceSize =
        options.get_int_option("sm_archiver_workspace_size", DFT_WSPACE_SIZE);
    size_t blockSize =
        options.get_int_option("sm_archiver_block_size", DFT_BLOCK_SIZE);

    if (archdir.empty()) {
        W_FATAL_MSG(fcINTERNAL, 
                << "Option for archive directory must be specified");
    }

    directory = new ArchiveDirectory(archdir, blockSize);
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
    //logScanner->setIgnore(logrec_t::t_page_flush);
    //logScanner->setIgnore(logrec_t::t_app_custom);
    //logScanner->setIgnore(logrec_t::t_cleaner_begin);
    //logScanner->setIgnore(logrec_t::t_cleaner_end);
    logScanner->setIgnore(logrec_t::t_xct_abort);
    logScanner->setIgnore(logrec_t::t_xct_end);
    logScanner->setIgnore(logrec_t::t_xct_freeing_space);
    //logScanner->setIgnore(logrec_t::t_tick);

    // alloc_a_page contains a "dummy" page ID and it is 
    // actually not required for current mrestore
    // (TODO: implement restore of allocated pages and test this)
    logScanner->setIgnore(logrec_t::t_alloc_a_page);
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
void LogArchiver::start_shutdown()
{
    DBGTHRD(<< "LOG ARCHIVER SHUTDOWN STARTING");
    // this flag indicates that reader and writer threads delivering null
    // blocks is not an error, but a termination condition
    shutdown = true;
    // make other threads see new shutdown value
    lintel::atomic_thread_fence(lintel::memory_order_release);
    // If archiver thread is not running, it is woken up and terminated
    // imediately afterwards due to shutdown flag being set
    DO_PTHREAD(pthread_cond_signal(&control.activateCond));
}

LogArchiver::~LogArchiver()
{
    if (!shutdown) {
        start_shutdown();
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
        size_t blockSize)
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

        delete fname;

        if (nextPartition > max) {
            W_FATAL_MSG(fcINTERNAL,
                << "Could not find partition files in log manager");
        }        

        startLSN = lsn_t(nextPartition, 0);
    }

    // lastLSN is the end LSN of the last generated initial run
    lastLSN = startLSN;

    // create index
    archIndex = new ArchiveIndex(blockSize, startLSN);
}

LogArchiver::ArchiveDirectory::~ArchiveDirectory()
{
    delete archIndex;
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
    }
    w_assert1(runEndLSN != lsn_t::null);

    std::stringstream fname;
    fname << archdir << "/" << LogArchiver::RUN_PREFIX
        << lastLSN << "-" << runEndLSN;

    // register index information and write it on end of file
    archIndex->finishRun(lastLSN, runEndLSN, appendFd, appendPos);

    std::string currentFName = archdir + "/" + CURR_RUN_FILE;
    W_DO(me()->frename(appendFd, currentFName.c_str(), fname.str().c_str()));
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

LogArchiver::LogConsumer::LogConsumer(lsn_t startLSN, size_t blockSize)
    : nextLSN(startLSN), endLSN(lsn_t::null), currentBlock(NULL),
    blockSize(blockSize), pos(0)
{
    DBGTHRD(<< "Starting log archiver at LSN " << nextLSN);

    readbuf = new AsyncRingBuffer(blockSize, IO_BLOCK_COUNT);
    reader = new ReaderThread(readbuf, startLSN);
    //reader = new FactoryThread(readbuf, startLSN);
    logScanner = new LogScanner(reader->getBlockSize());
    
    initLogScanner(logScanner);
    reader->fork();    
}

LogArchiver::LogConsumer::~LogConsumer()
{
    reader->start_shutdown();
    reader->join();
    delete reader;
    delete readbuf;
}

void LogArchiver::LogConsumer::open(lsn_t endLSN)
{
    this->endLSN = endLSN;
    do {
        reader->activate(nextLSN, endLSN);
    } while (!reader->isActive());

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
        //if (!shutdown) { // TODO handle shutdown
            // This happens if log scanner finds a skip logrec, but
            // then the next partition does not exist. This would be a bug,
            // because endLSN should always be an existing LSN, or one
            // immediately after an existing LSN but in the same partition.
            W_FATAL_MSG(fcINTERNAL, << "Consume request failed!");
        //}
        //returnRC = RCOK;
        return false;
    }
    DBGTHRD(<< "Picked block for replacement " << (void*) currentBlock);
    pos = 0;
    
    return true;
}

bool LogArchiver::LogConsumer::next(logrec_t*& lr)
{
    if (nextLSN >= endLSN) {
        DBGTHRD(<< "Replacement reached end LSN on " << nextLSN);
        /*
         * On the next activation, replacement must start on this LSN,
         * which will likely be in the middle of the block currently
         * being processed. However, we don't have to worry about that
         * because reader thread will start reading from this LSN on the
         * next activation.
         */
        return false;
    }

    if (!logScanner->nextLogrec(currentBlock, pos, lr, &nextLSN))
    {
        if (lr->type() == logrec_t::t_skip) {
            /*
             * Skip log record does not necessarily mean we should read
             * a new partition. The durable_lsn read from the log manager
             * is always a skip log record (at least for a brief moment),
             * since the flush daemon ends every flush with it. In this
             * case, however, the skip would have the same LSN as endLSN.
             */
            if (nextLSN < endLSN) {
                //lastSkipLSN = lr->lsn_ck();
                nextLSN = lsn_t(lr->lsn_ck().hi() + 1, 0);
                DBGTHRD(<< "Replacement got skip logrec on " << lr->lsn_ck()
                       << " setting nextLSN " << nextLSN);
            }
        }
        if (nextLSN >= endLSN) {
            DBGTHRD(<< "Replacement reached end LSN on " << nextLSN);
            return false;
        }
        if (!nextBlock()) {
            // reader thread finished and consume request failed
            return false;
        }
        return next(lr);
    }
    
    // nextLSN is updated by log scanner, so we must check again
    if (nextLSN >= endLSN) {
        return false;
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
        blkAssemb->shutdown();
        return false;
    }

    if (!blkAssemb->start()) {
        return false;
    }

    int run = heap->topRun();
    while (true) {
        if (heap->size() == 0 || run != heap->topRun()) {
            break;
        }

        if (blkAssemb->add(heap->top())) {
            //DBGTHRD(<< "Selecting for output: " << k);
            heap->pop();
        }
        else {
            break;
        }
    }
    blkAssemb->finish(run);

    return true;
}

LogArchiver::BlockAssembly::BlockAssembly(ArchiveDirectory* directory)
    : dest(NULL), writerForked(false), firstPID(lpid_t::null),
    lastPID(lpid_t::null), lastLSN(lsn_t::null)
{
    archIndex = directory->getIndex();
    blockSize = directory->getBlockSize();
    writebuf = new AsyncRingBuffer(blockSize, IO_BLOCK_COUNT); 
    writer = new WriterThread(writebuf, directory);
}

LogArchiver::BlockAssembly::~BlockAssembly()
{
    shutdown();
    delete writebuf;
    delete writer;
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

bool LogArchiver::BlockAssembly::start()
{
    DBGTHRD(<< "Requesting write block for selection");
    dest = writebuf->producerRequest();
    if (!dest) {
        DBGTHRD(<< "Block request failed!");
        if (writerForked) {
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

    if (firstPID.is_null()) {
        firstPID = lr->construct_pid();
    }
    lastPID = lr->construct_pid();
    lastLSN = lr->lsn_ck();

    // guarantees that whole log record fits in a block
    memcpy(dest + pos, lr, lr->length());
    pos += lr->length();
    return true;
}

void LogArchiver::BlockAssembly::finish(int run)
{
    if (!writerForked) {
        writer->fork();
        writerForked = true;
    }

    DBGTHRD("Selection produced block for writing " << (void*) dest <<
            " in run " << (int) run << " with end " << pos);

    archIndex->newBlock(firstPID, lastPID);
    firstPID = lpid_t::null;
    lastPID = lpid_t::null;

    // write block header info
    BlockHeader* h = (BlockHeader*) dest;
    h->run = run;
    h->end = pos;
    h->lsn = lastLSN;

    writebuf->producerRelease();
}

void LogArchiver::BlockAssembly::shutdown()
{
    writebuf->set_finished();
    writer->join();
    writerForked = false;
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
    bool firstJustFilled = false;
    slot_t dest(NULL, 0);
    // This is required to detect the case where the w_heap becomes empty
    // after the first invocation of selection(). In the old code, the
    // same log record would cause the increment of currentRun twice below.
    W_COERCE(workspace->allocate(lr->length(), dest));

    if (!dest.address) {
        // workspace full -> do selection until space available
        DBGTHRD(<< "Heap full! Size: " << w_heap.NumElements());
        if (!filledFirst) {
            // first run generated by first full load of w_heap
            currentRun++;
            w_heap.Heapify();
            filledFirst = true;
            firstJustFilled = true;
            DBGTHRD(<< "Heap full for the first time; start run 1");
        }
        return false;
    }
    memcpy(dest.address, lr, lr->length());

    // if all records of the current run are gone, start new run
    if (filledFirst && !firstJustFilled &&
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
    if (filledFirst) {
        w_heap.AddElement(k);
    }
    else {
        w_heap.AddElementDontHeapify(k);
    }

    return true;
}

void LogArchiver::ArchiverHeap::pop()
{
    workspace->free(w_heap.First().slot);
    w_heap.RemoveFirst();

    if (size() == 0) {
        filledFirst = false;
    }
}

logrec_t* LogArchiver::ArchiverHeap::top()
{
    return (logrec_t*) w_heap.First().slot.address;
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
            w_assert0(control.endLSN <= consumer->getNextLSN());
            if (control.endLSN < consumer->getNextLSN()) {
                // nextLSN may be greater than endLSN due to skip
                control.endLSN = consumer->getNextLSN();
            }
            return;
        }
    
        if (!heap->push(lr)) {
            // heap full -- invoke selection and try again
            // method returns bool, but result is not used for now (TODO)
            selection();
            // Try push once again
            if (!heap->push(lr)) {
                // this must be an error -- selection was invoked to free up
                // one block worth of records and there is still no space.
                // In theory, this could happen with a heavily fragmented
                // workpsace, but in that case, we prefer to catch the error
                // and so something about it
                W_FATAL_MSG(fcINTERNAL,
                    << "Heap still full after selection -- aborting");
            }
        }
    }
}

bool LogArchiver::activate(lsn_t endLSN, bool wait)
{
    w_assert0(smlevel_0::log);
    if (endLSN == lsn_t::null) {
        endLSN = smlevel_0::log->durable_lsn();
    }
    return control.activate(wait, endLSN);
}

void LogArchiver::run()
{
    while(true) {
        CRITICAL_SECTION(cs, control.mutex);
        bool activated = control.waitForActivation();
        if (!activated || shutdown) {
            break;
        }

        if (control.endLSN == lsn_t::null
                || control.endLSN <= directory->getLastLSN())
        {
            continue;
        }
        w_assert1(control.endLSN > directory->getLastLSN());
        
        DBGTHRD(<< "Log archiver activated from " << directory->getLastLSN() << " to "
                << control.endLSN);

        consumer->open(control.endLSN);

        replacement();
        W_COERCE(returnRC);

        w_assert1(consumer->getNextLSN() >= control.endLSN);

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

        DBGTHRD(<< "Log archiver consumed all log records until LSN "
                << control.endLSN);

        // TODO assert that last run filename contains this endlsn
        control.endLSN = lsn_t::null;
        // TODO use a "done" method that also asserts I hold the mutex
        control.activated = false;
    }

    // Perform selection until all remaining entries are flushed out of
    // the heap into runs. Last run boundary is also enqueued.
    DBGTHRD(<< "Archiver exiting -- last round of selection to empty heap");
    while (selection()) {}
    W_COERCE(returnRC);

    w_assert0(heap->size() == 0);
    blkAssemb->shutdown();
    // TODO shut down consumer
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
bool LogScanner::nextLogrec(char* src, size_t& pos, logrec_t*& lr, lsn_t* nextLSN)
{
tryagain:
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
    else if (lr->length() > remaining || remaining == 1) {
        // remainder of logrec must be read from next block
        DBGTHRD(<< "Log record with length "
                << (remaining > 1 ? lr->length() : -1)
                << " does not fit in current block of " << remaining);
        w_assert0(remaining <= LogArchiver::MAX_LOGREC_SIZE);
        memcpy(truncBuf, src + pos, remaining);
        truncCopied = remaining;
        truncMissing = lr->length() - remaining;
        pos += remaining;
        return false;
    }

    // handle skip log record, i.e., end of partition
    if (lr->type() == logrec_t::t_skip) {
        // assumption: space between skip logrec and next partition
        // is less than the block size (it should be < 8KB)
        // TODO because we use a fake skip logrec in archive runs, the LSN will
        // just be garbage. This also means that valid_header() will fail,
        // so we have to do this before the consistency checks below.
        DBGTHRD(<< "LogScanner reached skip log record on " << lr->lsn_ck());
        pos = blockSize;
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
    // nextLSN should be incremented only if not a skip logrec
    if (nextLSN != NULL) *nextLSN += lr->length();

    // handle ignorred logrecs
    if (ignore[lr->type()]) {
        // if logrec was assembled from truncation, pos was already
        // incremented, and skip is not necessary
        if ((void*) lr == (void*) truncBuf) {
            goto tryagain;
        }
        //DBGTHRD(<< "Found " << lr->type_str() << ", skipping "
        //       << lr->length());
        toSkip += lr->length();
    }

    // see if we have something to skip
    if (toSkip > 0) {
        if (toSkip < remaining) {
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

    //DBGTHRD(<< "logrec type " << lr->type()
    //        << " on pos " << pos << " lsn " << lr->lsn_ck());


    return true;
}

/*
 * TODO initialize from existing runs at startup
 */
LogArchiver::ArchiveIndex::ArchiveIndex(size_t blockSize, lsn_t startLSN)
    : blockSize(blockSize), lastPID(lpid_t::null), lastLSN(startLSN)
{
    DO_PTHREAD(pthread_mutex_init(&mutex, NULL));
    writeBuffer = new char[blockSize];

    // last run in the array is always the one being currently generated
    RunInfo r;
    r.firstLSN = startLSN;
    runs.push_back(r);
}

LogArchiver::ArchiveIndex::~ArchiveIndex()
{
    DO_PTHREAD(pthread_mutex_destroy(&mutex));
}

void LogArchiver::ArchiveIndex::newBlock(lpid_t first, lpid_t last)
{
    CRITICAL_SECTION(cs, mutex);
    
    w_assert1(runs.size() > 0);

    BlockEntry e;
    e.offset = blockSize * (runs.back().entries.size() - 1);
    e.pid = first;
    lastPID = last;
    runs.back().entries.push_back(e);
}

rc_t LogArchiver::ArchiveIndex::finishRun(lsn_t first, lsn_t last, int fd,
        fileoff_t offset)
{
    CRITICAL_SECTION(cs, mutex);

    w_assert0(runs.back().firstLSN == first);
    w_assert0(lastLSN == first);

    W_DO(serializeRunInfo(runs.back(), fd, offset));

    RunInfo newRun;
    newRun.firstLSN = last;
    runs.push_back(newRun);
    lastLSN = last;

    return RCOK;
}

// TODO implement deserialize at startup
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
            memcpy(writeBuffer + (j * sizeof(BlockEntry)), &run.entries[i+j],
                        sizeof(BlockEntry));
            j++;
            remaining--;
        }
        BlockHeader* h = (BlockHeader*) writeBuffer;
        h->entries = j;
        h->blockNumber = i;

        W_COERCE(me()->pwrite(fd, writeBuffer, blockSize, offset));
        i++;
    }

    return RCOK;
}

size_t LogArchiver::ArchiveIndex::findRun(lsn_t lsn)
{
    // Assumption: mutex is held by caller
    
    // CS: requests are more likely to access the last runs, so
    // we do a linear search instead of binary search.
    for (int i = runs.size() - 2; i >= 0; i--) {
        if (runs[i].firstLSN > lsn) {
            return i+1;
        }
    }
    // caller must check if returned index is valid
    return runs.size();
}

smlevel_0::fileoff_t LogArchiver::ArchiveIndex::findEntry(RunInfo* run,
        lpid_t pid, size_t from, size_t to)
{
    // Assumption: mutex is held by caller

    // binary search for page ID within run

    if (from > to) {
        // pid is either lower than first or greater than last
        // This should not happen because probes must not consider
        // this run if that's the case
        W_FATAL_MSG(fcINTERNAL, << "Invalid probe on archiver index! "
                << " PID = " << pid << " run = " << run->firstLSN);
    }

    size_t i;
    if (to == 0) {
        // last entry is artificial -- to hold last PID of run
        i = (run->entries.size() - 1) / 2;
    }
    else if (from == to) {
        i = from;
    }
    else {
        i = from/2 + to/2;
    }

    w_assert0(i < run->entries.size() - 1);

    /*
     * CS comparisons use just page number instead of whole lpid_t, which
     * means that multiple volumes are not supported (TODO)
     * To fix this, we ought to look for usages of the lpid_t operators
     * in sm_s.h and see what the semantics is.
     */
    if (run->entries[i].pid.page < pid.page &&
            run->entries[i+1].pid.page > pid.page)
    {
        // found it! must first check if previous does not contain same pid
        while (i > 0 && run->entries[i].pid.page == pid.page &&
                run->entries[i].pid.page == run->entries[i-1].pid.page)
        {
            i--;
        }
        return run->entries[i].offset;
    }

    // not found: recurse down
    if (run->entries[i].pid.page > pid.page) {
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
    if (index < runs.size() - 1) { // last run is the current one (unavailable)
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

    if (index <= runs.size() - 1) {
        // last run is the current one, thus unavailable for probing
        return NULL;
    }

    ProbeResult* result = new ProbeResult();
    result->pid = pid;
    result->runIndex = index;
    probeInRun(result);

    return result;
}

void LogArchiver::ArchiveIndex::probeNext(ProbeResult* prev, lsn_t endLSN)
{
    CRITICAL_SECTION(cs, mutex);

    size_t index = prev->runIndex + 1;
    if (
        (endLSN != lsn_t::null && endLSN < prev->runEnd) || // passed given end
        (index >= runs.size() - 1) // no more runs (last is current)
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
      shutdown(false), control(&shutdown)
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
            delete lrbuf;

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

void ArchiveMerger::start_shutdown()
{
    DBGTHRD(<< "ARCHIVE MERGER SHUTDOWN STARTING");
    shutdown = true;
    // If merger thread is not running, it is woken up and terminated
    // imediately afterwards due to shutdown flag being set
    DO_PTHREAD(pthread_cond_signal(&control.activateCond));
}
