#include "w_defines.h"

#ifndef LOGARCHIVER_H
#define LOGARCHIVER_H

#define SM_SOURCE

#include "sm_int_1.h"

#include "w_heap.h"
#include "ringbuffer.h"
#include "mem_mgmt.h"
#include "log_storage.h"

#include <bitset>
#include <queue>

class LogFactory;
class LogScanner;

/**
 * Used by both archiver and merger threads to control their
 * execution in the background.
 */
struct ArchiverControl {
    pthread_mutex_t mutex;
    pthread_cond_t activateCond;
    lsn_t endLSN;
    bool activated;
    bool* shutdown;

    ArchiverControl(bool* shutdown);
    ~ArchiverControl();
    bool activate(bool wait, lsn_t lsn = lsn_t::null);
    bool waitForActivation();
};

/**
 * Implementation of a log archiver using asynchronous reader and writer
 * threads.
 *
 * Shore's log manager maintains a total of 8 partitions open, of which
 * the last one is the currently active, i.e., the one being appended to
 * in log flushes. The archiver is free to process any log partition
 * except for the currently active one. When the active partition is
 * released by the log manager (i.e., when it is closed for appends),
 * a condition variable is broadcast. This allows the log archiver to
 * simply test and wait on this condition in order to start processing
 * a partition (see log_core::wait_for_close()).
 *
 * The logic in this class can be separated into two independent components:
 * 1) An asynchronous buffered log scanner based on a ring buffer; and
 * 2) An implementation of replacement selection without the logic that
 * delays current input items to later runs.
 *
 * A note on processing older log partitions (TODO):
 * Before we implemented the archiver, the log manager would delete a partition
 * once it was eliminated from the list of 8 open partitions. The compiler flag
 * KEEP_LOG_PARTITIONS was used to omit the delete operation, leaving the
 * complete history of the database in the log directory. However, if log
 * archiving is enabled, it should assume the responsibility of deleting old
 * log partitions. Currently, if the flag is not set and the archiver cannot
 * keep up with the growth of the log, partitions would be lost from archiving.
 *
 * @author: Caetano Sauer
 */
class LogArchiver : public smthread_t {
    friend class ArchiveMerger;
public:

    static rc_t constructOnce(LogArchiver*& la, const char* archdir,
            bool sort, size_t workspaceSize);


    const char * getArchiveDir() { return archdir; }
    rc_t getRC() { return returnRC; }
    lsn_t getLastArchivedLSN() { return startLSN; }

    virtual void run();
    bool activate(lsn_t endLSN = lsn_t::null, bool wait = true);
    void start_shutdown();

    static void initLogScanner(LogScanner* logScanner);
    static lsn_t parseLSN(const char* str, bool end = true);
    static os_dirent_t* scanDir(const char* archdir, os_dir_t& dir);

    /*
     * IMPORTANT: the block size must be a multiple of the log
     * page size to ensure that logrec headers are not truncated
     */
    const static int IO_BLOCK_SIZE = 1024 * 1024; // 1MB = 128 pages
    const static int IO_BLOCK_COUNT = 8; // total buffer = 8MB
    const static char* RUN_PREFIX;
    const static char* CURR_RUN_FILE;
    const static char* CURR_MERGE_FILE;
    const static size_t MAX_LOGREC_SIZE;
   
    // abstract class
    class BaseThread : public smthread_t {
    protected:
        AsyncRingBuffer* buf;
        int currentFd;
        off_t pos;
        rc_t returnRC;
        size_t blockSize;

    public:
        size_t getBlockSize() { return blockSize; }
        rc_t getReturnRC() { return returnRC; }


        BaseThread(AsyncRingBuffer* buf, const char* tname)
            : smthread_t(t_regular, tname),
              buf(buf), currentFd(-1), pos(0)
        {
            blockSize = buf->getBlockSize();
        }

        virtual void after_run()
        {
            if (returnRC.is_error()) {
                W_COERCE(returnRC);
            }
        }
    };

    class ReaderThread : public BaseThread {
    protected:
        uint nextPartition;
        rc_t openPartition();

        bool shutdown;
        ArchiverControl control;
        off_t prevPos;
    public:
        virtual void run();

        ReaderThread(AsyncRingBuffer* readbuf, lsn_t startLSN);

        void start_shutdown()
        {
            shutdown = true;
            // if finished is set, the ring buffer denies producer requests
            buf->set_finished();
        }

        void activate(lsn_t startLSN, lsn_t endLSN)
        {
            if (currentFd == -1) { // first activation
                w_assert0(nextPartition == (uint) startLSN.hi());
            }
            else {
                w_assert0(nextPartition - 1 == (uint) startLSN.hi());
            }
            // ignore if invoking activate on the same LSN repeatedly
            if (control.endLSN > startLSN) {
                pos = startLSN.lo();
                prevPos = pos;
            }
            control.activate(true, endLSN);
        }

        bool isActive() { return control.activated; }
    };


    class FactoryThread : public ReaderThread {
    private:
    	LogFactory* lf;
    public:
    	virtual void run();

    	FactoryThread(AsyncRingBuffer* readbuf, lsn_t startLSN);
    };


    /**
     * Simple implementation of a (naive) log archive index.
     * No caching and  one single mutex for all operations.
     * When log archiver is initialized, the information of every
     * run is loaded in main memory.
     */
    class ArchiveIndex {
    public:
        ArchiveIndex(size_t blockSize);
        virtual ~ArchiveIndex();

        struct ProbeResult {
            lpid_t pid;
            lsn_t runBegin;
            lsn_t runEnd;
            fileoff_t offset;
            size_t runIndex; // used internally for probeNext
        };


        void newBlock(lpid_t first, lpid_t last);
        rc_t finishRun(lsn_t first, lsn_t last, int fd, fileoff_t);
        ProbeResult* probeFirst(lpid_t pid, lsn_t lsn);
        void probeNext(ProbeResult* prev, lsn_t endLSN = lsn_t::null);

    private:
        struct BlockEntry {
            fileoff_t offset;
            lpid_t pid;
        };
        struct BlockHeader {
            uint32_t entries;
            uint8_t blockNumber;
        };
        struct RunInfo {
            lsn_t firstLSN;
            // one entry reserved for last pid with offset = block size
            std::vector<BlockEntry> entries;
        };

        size_t blockSize;
        std::vector<RunInfo> runs;
        lpid_t lastPID;
        lsn_t lastLSN;
        pthread_mutex_t mutex;
        char* writeBuffer;

        size_t findRun(lsn_t lsn);
        void probeInRun(ProbeResult*);
        // binary search
        fileoff_t findEntry(RunInfo* run, lpid_t pid,
                size_t from = 0, size_t to = 0);
        rc_t serializeRunInfo(RunInfo&, int fd, fileoff_t);

    };

    class WriterThread : public BaseThread {
    private:
        ArchiveIndex* archIndex;
        uint8_t currentRun;
        const char* archdir;
        lsn_t firstLSN;
        char * currentFName;

        queue<lsn_t> lsnQueue;
        pthread_mutex_t queueMutex;

        rc_t openNewRun();

    public:

        virtual void run();
        void enqueueRun(lsn_t lsn);
        lsn_t dequeueRun();

        static const logrec_t* SKIP_LOGREC;

        WriterThread(AsyncRingBuffer* writebuf, ArchiveIndex* archIndex,
                const char* archdir)
            :
              BaseThread(writebuf, "LogArchiver_WriterThread"),
              archIndex(archIndex),
              currentRun(0), archdir(archdir), firstLSN(lsn_t::null)
        {
            // set name of current run file
            const char * suffix = "/current_run";
            currentFName = new char[smlevel_0::max_devname];
            strncpy(currentFName, archdir, strlen(archdir));
            strcat(currentFName, suffix);
            DO_PTHREAD(pthread_mutex_init(&queueMutex, NULL));
        }
    };

    class BlockAssembly {
    public:
        BlockAssembly(const char* archdir);
        virtual ~BlockAssembly();

        bool start();
        bool add(logrec_t* lr);
        void finish(int run);
        void shutdown();
        void newRunBoundary(lsn_t lsn);
        static int getRunFromBlock(const char* b);
    private:
        char* dest;
        AsyncRingBuffer* writebuf;
        WriterThread* writer;
        ArchiveIndex* archIndex;
        bool writerForked;
        size_t blockSize;
        size_t pos;
        lpid_t firstPID;
        lpid_t lastPID;
    public:
        struct BlockHeader {
            uint8_t run;
            uint32_t end;
        };

    };

    class ArchiverHeap {
    public:
        ArchiverHeap(BlockAssembly* blkAssemb, size_t workspaceSize);
        virtual ~ArchiverHeap();

        bool push(logrec_t* lr);
        logrec_t* top();
        void pop();

        int topRun() { return w_heap.First().run; }
        size_t size() { return w_heap.NumElements(); }
    private:
        uint8_t currentRun;
        bool filledFirst;
        mem_mgmt_t* workspace;

        /*
         * Access to BlockAssembly is required to enqueue new run boundaries.
         * TODO -- see if queue method can be replaced by a more decoupled
         * mechanism.
         */
        BlockAssembly* blkAssemb;

        struct HeapEntry {
            uint8_t run;
            lpid_t pid;
            lsn_t lsn;
            mem_mgmt_t::slot_t slot;

            HeapEntry(uint8_t run, lpid_t pid, lsn_t lsn, mem_mgmt_t::slot_t slot)
                : run(run), pid(pid), lsn(lsn), slot(slot)
            {}

            HeapEntry()
                : run(0), pid(lpid_t::null), lsn(lsn_t::null), slot(NULL, 0)
            {}

            friend std::ostream& operator<<(std::ostream& os, const HeapEntry& e)
            {
                os << "[run " << e.run << ", " << e.pid << ", " << e.lsn <<
                    ", slot(" << e.slot.address << ", " << e.slot.length << ")]";
                return os;
            }
        };

        struct Cmp {
            /*
             * gt is actually a less than function, to produce ascending order
             */
            bool gt(const HeapEntry& a, const HeapEntry& b) const {
                if (a.run != b.run) {
                    return a.run < b.run;
                }
                // TODO no support for multiple volumes
                if (a.pid.page != b.pid.page) {
                    return a.pid.page < b.pid.page;
                }
                return a.lsn < b.lsn;
            }
        };

        Cmp heapCmp;
        Heap<HeapEntry, Cmp> w_heap;
    };

private:
    static LogArchiver* INSTANCE;

    ArchiverHeap* heap;
    ReaderThread* reader;
    AsyncRingBuffer* readbuf;
    BlockAssembly* blkAssemb;

    const char * archdir;
    lsn_t startLSN;
    lsn_t lastSkipLSN;
    lsn_t nextLSN;
    LogScanner* logScanner;

    bool shutdown;
    rc_t returnRC;
    ArchiverControl control;
    bool sortArchive;

    LogArchiver(const char* archdir, bool sort, size_t workspaceSize);
    ~LogArchiver();

    bool replacement();
    bool selection();
    bool copy();

};

class LogScanner {
public:
    bool nextLogrec(char* src, size_t& pos, logrec_t*& lr,
            lsn_t* nextLSN = NULL);

    LogScanner(size_t blockSize) 
        : truncCopied(0), truncMissing(0), toSkip(0), blockSize(blockSize)
    {
        // maximum logrec size = 3 pages
        truncBuf = new char[3 * log_storage::BLOCK_SIZE];
    }

    ~LogScanner() {
        delete truncBuf;
    }

    size_t getBlockSize() {
        return blockSize;
    }

    void setIgnore(logrec_t::kind_t type) {
        ignore.set(type);
    }

    void ignoreAll() {
        ignore.set();
    }
    
    void unsetIgnore(logrec_t::kind_t type) {
        ignore.reset(type);
    }

private:
    size_t truncCopied;
    size_t truncMissing;
    size_t toSkip;
    size_t blockSize;
    char* truncBuf;
    bitset<logrec_t::t_max_logrec> ignore;
};

class ArchiveMerger : public smthread_t {
    friend class LogArchiver;
public:

    class MergeOutput; // forward
    static rc_t constructOnce(ArchiveMerger*&, const char*, int, size_t);

    virtual void run();
    MergeOutput* offlineMerge(bool async = false);
    bool activate(bool wait = true);
    void start_shutdown();

private:
    static ArchiveMerger* INSTANCE;

    const char* archdir;
    int mergeFactor;
    size_t blockSize;

    bool shutdown;
    ArchiverControl control;

    ArchiveMerger(const char* archdir, int mergeFactor, size_t blockSize);

    char** pickRunsToMerge(int& count, lsn_t& firstLSN, lsn_t& lastLSN,
            bool async = false);

    struct MergeInput {
        logrec_t* logrec;
        int fd;
        off_t fpos;
        char* buf;
        size_t bpos;
        LogScanner* scanner;
        bool hasNext;
        char* fname;
#if W_DEBUG_LEVEL>=1
        bool truncated;
#endif

        MergeInput(char* fname, int blockSize);

        virtual ~MergeInput() {
            delete scanner;
            delete buf;
            delete fname;
        }

        friend std::ostream& operator<< (std::ostream& stream, const MergeInput& m)
        {
            stream
                << "PID " << m.logrec->construct_pid()
                << " LSN " << m.logrec->lsn_ck()
                << " length "   << m.logrec->length()
                << " FD " << m.fd << " offset "
                << m.fpos - m.scanner->getBlockSize() + m.bpos
                << " File " << m.fname
#if W_DEBUG_LEVEL>=1
                << " Truncated " << m.truncated
#endif
                ;
            return stream;
        }

        void fetchFromNextBlock();
        void next();
    };

    /*
     * This indirection is required because w_heap cannot properly
     * destroy objects and deallocate memory.
     */
    struct MergeEntry {
        MergeInput* input;
        bool active;

        MergeEntry() : input(NULL), active(true) {}; // required by w_heap
        MergeEntry(MergeInput* input) : input(input), active(true) {};

        virtual ~MergeEntry() {}

        friend std::ostream& operator<< (std::ostream& stream,
                const MergeEntry& m)
        {
            return stream << *(m.input) << " Active " << m.active;
        }

        lpid_t pid() const { return input->logrec->construct_pid(); }
        lsn_t lsn() const { return input->logrec->lsn_ck(); }
    };

    struct MergeEntryCmp {
        // we want lowest on top, so function is actually a less-than
        bool gt(const MergeEntry& a, const MergeEntry& b) const
        {
            if (!a.active) return false;
            if (!b.active) return true;
            if (a.pid().page != b.pid().page) {
                return a.pid().page < b.pid().page;
            }
            return a.lsn() < b.lsn();
        }
    };

    /* These are used by the method pickRunsToMerge().
     * We always pick the oldest runs (i.e., lowest LSN range) to be merged
     * first. This maintains the simple mapping from runs to contiguous LSN
     * ranges.
     */
    struct RunKey {
        lsn_t lsn;
        char* filename;

        RunKey(lsn_t l, char* n)
            : lsn(l), filename(n)
        {}

        RunKey() : lsn(lsn_t::null), filename(NULL)
        {}
    };

    struct RunKeyCmp {
        bool gt(const RunKey& a, const RunKey& b) const {
            return a.lsn > b.lsn;
        }
    };

public:
    class MergeOutput {
        friend class ArchiveMerger;
    public:
        size_t copyNext(char* dest);
        void dumpHeap(ostream& out);

        const lsn_t firstLSN;
        const lsn_t lastLSN;
    private:
        MergeEntryCmp mergeCmp;
        Heap<MergeEntry, MergeEntryCmp> heap;

        MergeOutput(lsn_t firstLSN, lsn_t lastLSN)
            : firstLSN(firstLSN), lastLSN(lastLSN), heap(mergeCmp)
        {};

        void addInputEntry(MergeInput* input)
        {
            MergeEntry entry(input);
            heap.AddElement(entry);
        }

        rc_t cleanup();
    };

};
#endif
