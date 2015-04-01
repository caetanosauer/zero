#include "w_defines.h"

#define SM_SOURCE
#define LOGARCHIVER_C

#include "logarchiver.h"
#include "logfactory.h"

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
LogArchiver* LogArchiver::INSTANCE;
ArchiveMerger* ArchiveMerger::INSTANCE;
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

LogArchiver::FactoryThread::FactoryThread(AsyncRingBuffer* readbuf, lsn_t startLSN) :
		ReaderThread(readbuf, startLSN) {
	lf = new LogFactory(readbuf->getBlockSize(), "tpcc_log_records", 1048576, 100000, 1.05);
}

/*
 * IMPORTANT: This code was copied from ReaderThread::run() and barely adapted. Factory does not
 * read from a file, so the variable  nextPartition does not make sense. Here the value of
 * nextPartition will be the "current partition" (so no -1 required).
 */
void LogArchiver::FactoryThread::run() {
	while(true) {
		CRITICAL_SECTION(cs, control.mutex);
		bool activated = control.waitForActivation();
		if (!activated) {
			break;
		}

		DBGTHRD(<< "Factory thread activated until " << control.endLSN);


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
			if (control.endLSN.hi() == nextPartition
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

			int bytesRead = 0;

			char* src = lf->nextBlock();
			memcpy(dest, src, blockSize);
			bytesRead = blockSize;

			DBGTHRD(<< "Read block " << (void*) dest << " from fpos " << pos <<
					" with size " << bytesRead);

			pos += bytesRead;
			if (control.endLSN.hi() == nextPartition
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
rc_t LogArchiver::WriterThread::openNewRun()
{
    if (currentFd != -1) {
        // append skip log record -- TODO get rid of fake
        W_DO(me()->pwrite(currentFd, FAKE_SKIP_LOGREC,
                    3, pos));

        lsn_t lastLSN = dequeueRun();
        w_assert1(lastLSN != lsn_t::null);

        char *const fname = new char[smlevel_0::max_devname];
        if (!fname) {
            W_FATAL(fcOUTOFMEMORY);
        }
        w_auto_delete_array_t<char> ad_fname(fname);        
        w_ostrstream s(fname, smlevel_0::max_devname);
        s << archdir << "/" << LogArchiver::RUN_PREFIX
            << firstLSN << "-" << lastLSN << ends;

        W_DO(me()->frename(currentFd, currentFName, fname));
        W_DO(me()->close(currentFd));
        DBGTHRD(<< "Closed current output run: " << fname);

        firstLSN = lastLSN;
    }
    else {
        // start LSN is enqueued when log archiver is initialized
        firstLSN = dequeueRun();
        w_assert1(firstLSN != lsn_t::null);
    }

    int flags = smthread_t::OPEN_WRONLY | smthread_t::OPEN_SYNC
        | smthread_t::OPEN_CREATE;
    int fd;
    // 0744 is the mode_t for the file permissions (like in chmod)
    W_DO(me()->open(currentFName, flags, 0744, fd));
    DBGTHRD(<< "Opened new output run");

    currentFd = fd;
    pos = 0;
    return RCOK;
}

void LogArchiver::WriterThread::enqueueRun(lsn_t lsn)
{
    CRITICAL_SECTION(cs, queueMutex);
    lsnQueue.push(lsn);
    DBGTHRD(<< "New run boundary enqueued: " << lsn);
}

lsn_t LogArchiver::WriterThread::dequeueRun()
{
    CRITICAL_SECTION(cs, queueMutex);
    if (lsnQueue.empty()) {
        return lsn_t::null;
    }
    lsn_t res = lsnQueue.front();
    lsnQueue.pop();
    DBGTHRD(<< "Run boundary dequeued: " << res);
    return res;
}

void LogArchiver::WriterThread::run()
{
    DBGTHRD(<< "Writer thread activated");
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
            // call openNewRun to rename current_run file
            returnRC = openNewRun();
            return; // finished is set on buf
        }
        
        DBGTHRD(<< "Picked block for write " << (void*) src);

        // CS: changed writer behavior to write raw blocks instead of a
        // contiguous stream of log records, as done in log partitions.
        // TODO: restore code should consider block format when reading
        // from the log archive!
        
        //BlockHeader* h = (BlockHeader*) src;
        int run = BlockAssembly::getRunFromBlock(src);
        if (firstLSN == lsn_t::null || currentRun != run) {
            w_assert1((run == 0 && currentRun == 0) || run == currentRun + 1);
            /*
             * Selection (producer) guarantees that logrec fits in block.
             * lastLSN is the LSN of the first log record in the new block
             * -- it will be used as the upper bound when renaming the file
             *  of the current run. This same LSN will be used as lower
             *  bound on the next run, which allows us to verify whether
             *  holes exist in the archive.
             */
            DBGTHRD(<< "Opening file for new run " << (int) h->run
                    << " starting on LSN " << firstLSN);
            CHECK_ERROR(openNewRun());
            currentRun = run;
        }

        //fileoff_t dataLength = h->end - sizeof(BlockHeader);
        //char const* dataBegin = src + sizeof(BlockHeader);

        CHECK_ERROR(me()->pwrite(currentFd, 0, blockSize, pos));

        DBGTHRD(<< "Wrote out block " << (void*) src << " with size " << dataLength);

        pos += blockSize;
        buf->consumerRelease();
    }

    returnRC = RCOK;
}

LogArchiver::LogArchiver(const char* archdir, bool sort, size_t workspaceSize)
    : smthread_t(t_regular, "LogArchiver"),
    currentRun(0), archdir(archdir), filledFirst(false),
    startLSN(lsn_t::null), lastSkipLSN(lsn_t::null),
    shutdown(false), control(&shutdown), sortArchive(sort),
    heap(heapCmp) 
{
    // open archdir and extract last archived LSN
    {
        lsn_t highestLSN = lsn_t::null;
        os_dir_t dir = NULL;
        os_dirent_t* entry = scanDir(archdir, dir);
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
                string path = string(archdir) + "/" + runName;
                if (unlink(path.c_str()) < 0) {
                    smlevel_0::errlog->clog << fatal_prio
                        << "Log archiver: failed to delete "
                        << runName << endl;
                    W_FATAL(fcOS);
                }
            }
            entry = scanDir(archdir, dir);
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
    nextLSN = startLSN;
    DBGTHRD(<< "Starting log archiver at LSN " << startLSN);

    readbuf = new AsyncRingBuffer(IO_BLOCK_SIZE, IO_BLOCK_COUNT);
    reader = new ReaderThread(readbuf, startLSN);
    //reader = new FactoryThread(readbuf, startLSN);


    blk = new BlockAssembly(archdir);
    // first run will begin with startLSN
    blk->newRunBoundary(startLSN);

    workspace = new fixed_lists_mem_t(workspaceSize);
    logScanner = new LogScanner(reader->getBlockSize());
    initLogScanner(logScanner);
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
    reader->start_shutdown();
    // If archiver thread is not running, it is woken up and terminated
    // imediately afterwards due to shutdown flag being set
    DO_PTHREAD(pthread_cond_signal(&control.activateCond));
}

LogArchiver::~LogArchiver()
{
    if (!shutdown) {
        start_shutdown();
        reader->join();
        blk->shutdown();
    }
    delete reader;
    delete readbuf;
    delete blk;
    delete logScanner;
    delete INSTANCE;
}

rc_t LogArchiver::constructOnce(LogArchiver*& la, const char* archdir,
        bool sort, size_t workspaceSize)
{
    if (INSTANCE) {
        smlevel_0::errlog->clog << error_prio
            << "Archiver already created" << endl;
        return RC(eINTERNAL);
    }
    INSTANCE = new LogArchiver(archdir, sort, workspaceSize);
    la = INSTANCE;
    return RCOK;
}

/**
 * Extracts one LSN from a run's file name.
 * If end == true, get the end LSN (the upper bound), otherwise
 * get the begin LSN.
 */
lsn_t LogArchiver::parseLSN(const char* str, bool end)
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

os_dirent_t* LogArchiver::scanDir(const char* archdir, os_dir_t& dir)
{
    if (dir == NULL) {
        dir = os_opendir(archdir);
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
    if (heap.NumElements() == 0) {
        // if there are no elements in the heap, we have nothing to write
        // -> return and wait for next activation
        DBGTHRD(<< "Selection got empty heap -- sleeping");
        blk->shutdown();
        return false;
    }

    if (!blk->start()) {
        return false;
    }

    int run = heap.First().run;
    while (true) {
        if (heap.NumElements() == 0) {
            break;
        }

        HeapEntry k = heap.First();
        if (run != k.run) {
            break;
        }

        if (blk->add((logrec_t*) k.slot.address)) {
            //DBGTHRD(<< "Selecting for output: " << k);
            workspace->free(k.slot);
            heap.RemoveFirst();
        }
        else {
            break;
        }
    }
    blk->finish(run);

    return true;
}

LogArchiver::BlockAssembly::BlockAssembly(const char* archdir)
    : writerForked(false), pos(sizeof(BlockHeader))
{
    writebuf = new AsyncRingBuffer(IO_BLOCK_SIZE, IO_BLOCK_COUNT); 
    writer = new WriterThread(writebuf, archdir);

    blockSize = writer->getBlockSize();
}

LogArchiver::BlockAssembly::~BlockAssembly()
{
    shutdown();
    delete writebuf;
    delete writer;
}

void LogArchiver::BlockAssembly::newRunBoundary(lsn_t lsn)
{
    writer->enqueueRun(lsn);
}

int LogArchiver::BlockAssembly::getRunFromBlock(const char* b)
{
    BlockHeader* h = (BlockHeader*) b;
    return h->run;
}

bool LogArchiver::BlockAssembly::start()
{
    DBGTHRD(<< "Requesting write block for selection");
    char* dest = writebuf->producerRequest();
    if (!dest) {
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
    if (lr->length() > blockSize - pos) {
        return false;
    }

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

    // write run number and block end
    BlockHeader* h = (BlockHeader*) dest;
    h->run = run;
    h->end = pos;

    writebuf->producerRelease();
}

void LogArchiver::BlockAssembly::shutdown()
{
    writebuf->set_finished();
    writer->join();
    writerForked = false;
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
bool LogArchiver::replacement()
{
    size_t blockSize = reader->getBlockSize();

    // get a block from the reader thread
    char* src = readbuf->consumerRequest();
    if (!src) {
        if (!shutdown) {
            // TODO reader may finish because last partition was processed
            // better solution? reader check for endLSN? it could check if
            // pos >= endLSN.lo()
            // THIS also happens if log scanner finds a skip logrec, but
            // then the next partition does not exist
            // --> finish triggered by reader, no by control.endLSN
            //
            // TODO does this ever happen???
            blk->newRunBoundary(lastSkipLSN);
            DBGTHRD(<< "Consume request failed!");
            return false;
        }
        returnRC = RCOK;
        return false;
    }
    DBGTHRD(<< "Picked block for replacement " << (void*) src);
    
    size_t pos = 0;
    while(pos < blockSize) {
        if (nextLSN >= control.endLSN) {
            DBGTHRD(<< "Replacement reached end LSN on " << nextLSN);
            // nextLSN may be greater than endLSN due to skip
            control.endLSN = nextLSN;
            /*
             * On the next activation, replacement must start on this LSN,
             * which will likely be in the middle of the block currently
             * being processed. However, we don't have to worry about that
             * because reader thread will start reading from this LSN on the
             * next activation.
             */
            break;
        }

        logrec_t* lr;
        if (!logScanner->nextLogrec(src, pos, lr, &nextLSN))
        {
            if (lr->type() == logrec_t::t_skip) {
                /*
                 * Skip log record does not necessarily mean we should read
                 * a new partition. The durable_lsn read from the log manager
                 * is always a skip log record (at least for a brief moment),
                 * since the flush daemon ends every flush with it. In this
                 * case, however, the skip would have the same LSN as endLSN.
                 */
                if (nextLSN < control.endLSN) {
                    lastSkipLSN = lr->lsn_ck();
                    nextLSN = lsn_t(lr->lsn_ck().hi() + 1, 0);
                    DBGTHRD(<< "Replacement got skip logrec on " << lastSkipLSN
                           << " setting nextLSN " << nextLSN);
                }
            }
            break; // must fetch next block
        }

        // nextLSN is updated by log scanner, so we must check again
        if (nextLSN >= control.endLSN) {
            continue;
        }
    
        
        // copy log record to sort workspace
        slot_t dest(NULL, 0);
        // This is required to detect the case where the heap becomes empty
        // after the first invocation of selection(). In the old code, the
        // same log record would cause the increment of currentRun twice below.
        bool firstJustFilled = false;
        while(!dest.address) {
            CHECK_ERROR_BOOL(workspace->allocate(lr->length(), dest));

            if (!dest.address) {
                // workspace full -> do selection until space available
                DBGTHRD(<< "Heap full! Size: " << heap.NumElements());
                if (!filledFirst) {
                    // first run generated by first full load of heap
                    currentRun++;
                    heap.Heapify();
                    filledFirst = true;
                    firstJustFilled = true;
                    blk->newRunBoundary(lr->lsn_ck());
                    DBGTHRD(<< "Heap full for the first time; start run 1");
                }
                if (!selection()) {
                    // TODO this is an error
                    // If heap is empty and workspace is full, we need a defrag
                    // Otherwise, selection could not reserve a block for writing,
                    // which means that an error ocurred or finished flag was set.
                    // In those cases, we could just set the same returnRC
                    DBGTHRD(<< "Selection returned false -- aborting!");
                    return false;
                }
            }
        }
        memcpy(dest.address, lr, lr->length());

        // if all records of the current run are gone, start new run
        if (filledFirst && !firstJustFilled &&
                (heap.NumElements() == 0 || heap.First().run == currentRun)) {
            currentRun++;
            DBGTHRD(<< "Replacement starting new run " << (int) currentRun
                    << " on LSN " << lr->lsn_ck());
            // add new LSN run boundary to writer
            blk->newRunBoundary(lr->lsn_ck());
        }

        //DBGTHRD(<< "Processing logrec " << lr->lsn_ck() << ", type " <<
        //        lr->type() << "(" << lr->type_str() << ") length " <<
        //        lr->length() << " into run " << (int) currentRun);

        // insert key and pointer into heap
        HeapEntry k(currentRun, lr->construct_pid(), lr->lsn_ck(), dest);
        if (filledFirst) {
            heap.AddElement(k);
        }
        else {
            heap.AddElementDontHeapify(k);
        }
    }

    readbuf->consumerRelease();
    DBGTHRD(<< "Released block for replacement " << (void*) src);

    return nextLSN < control.endLSN;
}

/**
 * This code is mostly copied from replacement(). It is currently used only to
 * measure the CPU and IO overhead of traditional log archiving, i.e., without
 * generating sorted runs. The archive generated by this process is not useful
 * at all.
 */
bool LogArchiver::copy()
{
    size_t blockSize = reader->getBlockSize();

    // get a block from the reader thread
    char *const src = readbuf->consumerRequest();
    if (!src | !blk->start()) {
        if (!shutdown) {
            DBGTHRD(<< "Block request failed!");
            returnRC =  RC(eINTERNAL);
            return false;
        }
        blk->shutdown();
        returnRC = RCOK;
        return false;
    }
    
    size_t pos = 0;
    while(pos < blockSize) {
        if (nextLSN >= control.endLSN) {
            DBGTHRD(<< "Replacement reached end LSN on " << nextLSN);
            control.endLSN = nextLSN;
            break;
        }

        logrec_t* lr;
        if (!logScanner->nextLogrec(src, pos, lr, &nextLSN))
        {
            if (lr->type() == logrec_t::t_skip) {
                if (nextLSN < control.endLSN) {
                    lastSkipLSN = lr->lsn_ck();
                    nextLSN = lsn_t(lr->lsn_ck().hi() + 1, 0);
                    DBGTHRD(<< "Replacement got skip logrec on " << lastSkipLSN
                           << " setting nextLSN " << nextLSN);
                }
            }
            break; // must fetch next block
        }
        
        /*
         * copy logrec to write buffer
         * WARNING: We assume that all logrecs in a read block will fit in a
         * write block, which is not necessarily true because write blocks
         * need to reserve 5 bytes for a header. It is, however, very unlikely,
         * because many log record types are fitered by the log scanner. If that
         * happens (i.e., add method returns false), we simply ignore it and drop
         * that one log record). This is OK because this method is just for
         * performance analysis.
         */
        blk->add(lr);

        // nextLSN is updated by log scanner, so we must check again
        if (nextLSN >= control.endLSN) {
            continue;
        }
    }

    readbuf->consumerRelease();
    blk->finish(0);
    DBGTHRD(<< "Released block for copy " << (void*) src);

    return nextLSN < control.endLSN;
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
    reader->fork();

    while(true) {
        CRITICAL_SECTION(cs, control.mutex);
        bool activated = control.waitForActivation();
        if (!activated || shutdown) {
            break;
        }

        if (control.endLSN == lsn_t::null || control.endLSN <= startLSN) {
            continue;
        }
        w_assert1(control.endLSN > startLSN);
        
        DBGTHRD(<< "Log archiver activated from " << startLSN << " to "
                << control.endLSN);

        while (!reader->isActive()) {
            reader->activate(startLSN, control.endLSN);
        }
        // writer thread doesn't need to be activated

        if (sortArchive) {
            while (replacement() && !shutdown) {}
            W_COERCE(returnRC);
        }
        else {
            while (copy() && !shutdown) {}
            W_COERCE(returnRC);
        }

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
        startLSN = control.endLSN;
        control.endLSN = lsn_t::null;
        // TODO use a "done" method that also asserts I hold the mutex
        control.activated = false;
    }

    // Perform selection until all remaining entries are flushed out of
    // the heap into runs. Last run boundary is also enqueued.
    DBGTHRD(<< "Archiver exiting -- last round of selection to empty heap");
    // nextLSN contains the next LSN that would be consumed by replacement,
    // i.e., it is the end boundary of the current run
    blk->newRunBoundary(nextLSN);
    while (selection()) {}
    W_COERCE(returnRC);

    w_assert0(heap.NumElements() == 0);

    reader->join();
    blk->shutdown();
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

ArchiveMerger::ArchiveMerger(const char* archdir, int mergeFactor,
        size_t blockSize)
    : smthread_t(t_regular, "ArchiveMerger"),
      archdir(archdir), mergeFactor(mergeFactor), blockSize(blockSize),
      shutdown(false), control(&shutdown)
{}

rc_t ArchiveMerger::constructOnce(ArchiveMerger*& ret, const char* archdir,
        int mergeFactor, size_t blockSize)
{
    if (INSTANCE) {
        smlevel_0::errlog->clog << error_prio
            << "Merger already created" << endl;
        return RC(eINTERNAL);
    }
    INSTANCE = new ArchiveMerger(archdir, mergeFactor, blockSize);
    ret = INSTANCE;
    return RCOK;
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
                W_COERCE(me()->pwrite(fd, FAKE_SKIP_LOGREC, 3, fpos));
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
    if (howmuch < scanner->getBlockSize()) {
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
    RunKeyCmp runCmp;
    Heap<RunKey, RunKeyCmp> runHeap(runCmp);
    os_dir_t dir = NULL;
    os_dirent_t* dent = LogArchiver::scanDir(archdir, dir);
    count = 0;

    while(dent != NULL) {
        char *const fname = new char[smlevel_0::max_devname];
        strcpy(fname, dent->d_name);
        if (strncmp(LogArchiver::RUN_PREFIX, fname,
                    strlen(LogArchiver::RUN_PREFIX)) == 0)
        {
            RunKey k(LogArchiver::parseLSN(fname, false), fname);
            if (count < mergeFactor) {
                runHeap.AddElement(k);
                count++;
            }
            else {
                // replace if current is smaller than largest in the heap
                if (k.lsn < runHeap.First().lsn) {
                    delete runHeap.First().filename;
                    runHeap.RemoveFirst();
                    runHeap.AddElement(k);
                }
            }
        }
        dent = LogArchiver::scanDir(archdir, dir);
    }
    os_closedir(dir);

    if (count == 0) {
        return NULL;
    }

    // Heuristic: only start merging if there are at least mergeFactor/2 runs available
    if (!async || count >= mergeFactor/2) {
        // Now the heap contains the oldest runs in descending order (newest first)
        lastLSN = LogArchiver::parseLSN(runHeap.First().filename, true);
        char** files = new char*[count];
        for (int i = 0; i < count; i++) {
            char *const fname = new char[smlevel_0::max_devname];
            if (!fname) { W_FATAL(fcOUTOFMEMORY); }
            w_ostrstream s(fname, smlevel_0::max_devname);
            s << archdir << "/" << 
                runHeap.First().filename << ends;
            DBGTHRD(<< "Picked run for merge: "
                    << runHeap.First().filename);
            delete runHeap.First().filename;
            files[i] = fname;
            runHeap.RemoveFirst();
            if (runHeap.NumElements() == 1) {
                firstLSN = runHeap.First().lsn;
            }
        }
        return files;
    }

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
