#ifndef LOGARCHIVER_H
#define LOGARCHIVER_H

#include "worker_thread.h"
#include "sm_base.h"
#include "log_consumer.h"
#include "logarchive_index.h"
#include "logarchive_writer.h"
#include "w_heap.h"
#include "log_storage.h"
#include "mem_mgmt.h"

#include <queue>
#include <set>

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

class sm_options;
class LogScanner;

/** \brief Heap data structure that supports log archive run generation
 *
 * This class encapsulates a heap data structure in a way which is
 * transparent to the replacement-selection logic of the enclosing
 * LogArchiver instance. It contains a heap data structure as well as a
 * memory manager for the variable-length log records.
 *
 * The heap contains instances of HeapEntry, which contains the sort key of
 * log records (run number, page id, lsn) and a pointer to the log record
 * data in the memory manager workspace (slot_t).
 *
 * This class is more than just a heap data structure because it is aware
 * of run boundaries.  Therefore, it can be seen as a replacement-selection
 * module, with the particularity that records are not delayed to future
 * runs, in order to maintain a fixed mapping from regions of the recovery
 * log (i.e., LSN ranges) to runs in the log archive. The goal of such
 * mapping is to facilitate the resume of log archiving after a system
 * failure (see our BTW 2015 paper on Single-Pass Restore for more
 * details).
 *
 * This class participates in the log archiving pipeline in which the input
 * stream of log records coming from the log consumer is fed into the heap
 * by invoking push() for each log record. This is analogous to the
 * replacement step of the sorting algorithm. On the other side, the
 * selection step pops log records out of the heap and feeds them to the
 * BlockAssembly component.
 *
 * \author Caetano Sauer
 */
class ArchiverHeap {
    public:
        ArchiverHeap(size_t workspaceSize);
        virtual ~ArchiverHeap();

        bool push(logrec_t* lr, bool duplicate);
        logrec_t* top();
        void pop();

        run_number_t topRun() { return w_heap.First().run; }
        size_t size() { return w_heap.NumElements(); }
    private:
        run_number_t currentRun;
        bool filledFirst;
        mem_mgmt_t* workspace;

        mem_mgmt_t::slot_t allocate(size_t length);

        struct HeapEntry {
            mem_mgmt_t::slot_t slot;
            lsn_t lsn;
            run_number_t run;
            PageID pid;

            HeapEntry(run_number_t run, PageID pid, lsn_t lsn,
                    mem_mgmt_t::slot_t slot)
                : slot(slot), lsn(lsn), run(run), pid(pid)
            {}

            HeapEntry()
                : slot(NULL, 0), lsn(lsn_t::null), run(0), pid(0)
            {}

            friend std::ostream& operator<<(std::ostream& os,
                    const HeapEntry& e)
            {
                os << "[run " << e.run << ", " << e.pid << ", " << e.lsn <<
                    ", slot(" << e.slot.address << ", " << e.slot.length << ")]";
                return os;
            }
        };

        struct Cmp {
            bool gt(const HeapEntry& a, const HeapEntry& b) const;
        };

        Cmp heapCmp;
        Heap<HeapEntry, Cmp> w_heap;
};

/**
 * Basic service to merge existing log archive runs into larger ones.
 * Currently, the merge logic only supports the *very limited* use case of
 * merging all N run files into a smaller n, depending on a given fan-in
 * and size limits. Currently, it is used simply to run our restore
 * experiments with different number of runs for the same log archive
 * volume.
 *
 * In a proper implementation, we have to support useful policies, with the
 * restriction that only consecutive runs can be merged. The biggest
 * limitation right now is that we reuse the logic of BlockAssembly, but
 * its control logic -- especially the coordination with the WriterThread
 * -- is quite restricted to the usual case of a consumption of log records
 * from the standard recovery log, i.e., ascending LSNs and run numbers,
 * startLSN coming from the existing run files, etc. We have to make that
 * logic clever and more abstract; or simply don't reuse the BlockAssembly
 * infrastructure.
 */
class MergerDaemon : public worker_thread_t {
public:
    MergerDaemon(const sm_options&,
            ArchiveIndex* in, ArchiveIndex* out = nullptr);

    virtual ~MergerDaemon() {}

    virtual void do_work();

    rc_t doMerge(unsigned level, unsigned fanin);

private:
    ArchiveIndex* indir;
    ArchiveIndex* outdir;
    unsigned _fanin;
};

/** \brief Implementation of a log archiver using asynchronous reader and
 * writer threads.
 *
 * The log archiver runs as a background daemon whose execution is controlled
 * by an ArchiverControl object. Once a log archiver thread is created and
 * forked, it waits for an activation to start working. The caller thread must
 * invoke the activate() method to perform this activation.
 *
 * Log archiving works in <b>activation cycles</b>, in which it first waits for
 * an activation and then consumes the recovery log up to a given LSN value
 * (\see activate(bool, lsn_t)).  This cycle is executed in an infinite loop
 * until the method shutdown() is invoked.  Once shutdown is invoked, the
 * current cycle is <b>not</b> interrupted. Instead, it finishes consuming the
 * log until the LSN given in the last successful activation and only then it
 * exits. The destructor also invokes shutdown() if not done yet.
 *
 * The class LogArchiver itself serves merely as an orchestrator of its
 * components, which are:
 * - LogArchiver::LogConsumer, which encapsulates a reader thread and parsing
 *   individual log records from the recovery log.
 * - LogArchiver::ArchiverHeap, which performs run generation by sorting the
 *   input stream given by the log consumer.
 * - LogArchiver::BlockAssembly, which consumes the sorted output from the
 *   heap, builds indexed blocks of log records (used for instant restore), and
 *   passes them over to the asynchronous writer thread
 * - LogArchiver::ArchiveDirectory, which represents the set of sorted runs
 *   that compose the log archive itself. It manages filesystem operations to
 *   read from and write to the log archive, controls access to the archive
 *   index, and provides scanning facilities used by restore.
 *
 * One activation cycle consists of consuming all log records from the log
 * consumer, which must first be opened with the given "end LSN". Each log
 * record is then inserted into the heap until it becomes full. Then, log
 * records are removed from the heap (usually in bulk, e.g., one block at a
 * time) and passed to the block assembly component. The cycle finishes once
 * all log records up to the given LSN are <b>inserted into the heap</b>, which
 * does not necessarily mean that the persistent log archive will contain all
 * those log records. The only way to enforce that is to perform a shutdown.
 * This design maintains the heap always as full as possible, which generates
 * runs whose size is (i) as large as possible and (ii) independent of the
 * activation behavior.
 *
 * In the typical operation mode, a LogArchiver instance is constructed using
 * the sm_options provided by the user, but for tests and external experiments,
 * it can also be constructed by passing instances of these four components
 * above.
 *
 * A note on processing older log partitions (TODO): Before we implemented the
 * archiver, the log manager would delete a partition once it was eliminated
 * from the list of 8 open partitions. The compiler flag KEEP_LOG_PARTITIONS
 * was used to omit the delete operation, leaving the complete history of the
 * database in the log directory. However, if log archiving is enabled, it
 * should take over the responsibility of deleting old log partitions.
 * Currently, if the flag is not set and the archiver cannot keep up with the
 * growth of the log, partitions would be lost from archiving.
 *
 * \sa LogArchiver::LogConsumer
 * \sa LogArchiver::ArchiverHeap
 * \sa LogArchiver::BlockAssembly
 * \sa LogArchiver::ArchiveDirectory
 *
 * \author Caetano Sauer
 */
class LogArchiver : public thread_wrapper_t {
public:
    LogArchiver(const sm_options& options);
    LogArchiver(
            ArchiveIndex*,
            LogConsumer*,
            ArchiverHeap*,
            BlockAssembly*
    );

    virtual ~LogArchiver();

    virtual void run();
    void activate(lsn_t endLSN = lsn_t::null, bool wait = true);
    void shutdown();
    bool requestFlushAsync(lsn_t);
    void requestFlushSync(lsn_t);
    void archiveUntilLSN(lsn_t);

    ArchiveIndex* getIndex() { return index; }
    lsn_t getNextConsumedLSN() { return consumer->getNextLSN(); }
    void setEager(bool e)
    {
        eager = e;
        lintel::atomic_thread_fence(lintel::memory_order_release);
    }

    bool getEager() const { return eager; }

    /*
     * IMPORTANT: the block size must be a multiple of the log
     * page size to ensure that logrec headers are not truncated
     */
    const static int DFT_WSPACE_SIZE= 100; // 100MB
    const static bool DFT_EAGER = true;
    const static bool DFT_READ_WHOLE_BLOCKS = true;
    const static int DFT_GRACE_PERIOD = 1000000; // 1 sec

private:
    ArchiveIndex* index;
    LogConsumer* consumer;
    ArchiverHeap* heap;
    BlockAssembly* blkAssemb;
    MergerDaemon* merger;

    bool shutdownFlag;
    ArchiverControl control;
    bool selfManaged;
    bool eager;
    bool readWholeBlocks;
    int slowLogGracePeriod;
    lsn_t nextActLSN;
    lsn_t flushReqLSN;

    void replacement();
    bool selection();
    void pushIntoHeap(logrec_t*, bool duplicate);
    bool waitForActivation();
    bool processFlushRequest();
    bool isLogTooSlow();
    bool shouldActivate(bool logTooSlow);

};

#endif
