#include "w_defines.h"

#ifndef RESTORE_H
#define RESTORE_H

#include "sm_base.h"
#include "logarchiver.h"

#include <queue>
#include <map>
#include <vector>
#include <atomic>

class sm_options;
class RestoreBitmap;
class RestoreScheduler;
class generic_page;
class BackupReader;
class SegmentWriter;
class RestoreThread;

/** \brief Class that controls the process of restoring a failed volume
 *
 * \author Caetano Sauer
 */
class RestoreMgr {
public:
    /** \brief Constructor method
     *
     * @param[in] useBackup If set, restore volume from a backup file, which
     * must have been registered with the volume instance with sx_add_backup.
     * Otherwise, perform backup-less restore, i.e., replaying log only.
     *
     * @param[in] takeBackup If set, restore manager runs in a special mode
     * in which restored segments are written to a backup file, also specified
     * in vol_t with the take_backup() method.
     */
    RestoreMgr(const sm_options&, LogArchiver::ArchiveDirectory*, vol_t*,
            bool useBackup, bool takeBackup = false);
    virtual ~RestoreMgr();

    /** \brief Returns true if given page is already restored.
     *
     * This method is used to check if a page has already been restored, i.e.,
     * if it can be read from the volume already.
     */
    bool isRestored(const PageID& pid);

    /** \brief Request restoration of a given page
     *
     * This method is used by on-demand restore to signal the intention of
     * reading a specific page which is not yet restored. This method simply
     * generates a request with the restore scheduler -- no guarantees are
     * provided w.r.t. when page will be restored.
     *
     * The restored contents of the page will be copied into the given
     * address (if not null). This enables reuse in a buffer pool "fix" call,
     * foregoing the need for an extra read on the restored device. However,
     * this copy only happens if the segment still happens to be unrestored
     * when this method enters the critical section. If it gets restored
     * immediately before that, then the request is ignored and the method
     * returns false. This condition tells the caller it must read the page
     * contents from the restored device.
     */
    bool requestRestore(const PageID& pid, generic_page* addr = NULL);

    /** \brief Blocks until given page is restored
     *
     * This method will block until the given page ID is restored or the given
     * timeout is reached. It returns false in case of timeout and true if the
     * page is restored. When this method returns true, the caller is allowed
     * to read the page from the volume. This is basically equivalent to
     * polling on the isRestored() method.
     */
    bool waitUntilRestored(const PageID& pid, size_t timeout_in_ms = 0);

    /** \brief Set instant policy
     *
     * If true, access to segments will be enabled incrementally, as proposed
     * in instant restore. Otherwise, a single-pass restore is performed. Used
     * for taking a backup.
     */
    void setInstant(bool instant = true);

    /** \brief Sets the LSN of the restore_begin log record
     *
     * This is required so that we know (up to) which LSN to request from the
     * log archiver. It must be set before the thread is forked.
     */
    void setFailureLSN(lsn_t l) { failureLSN = l; }

    /** \brief Gives the segment number of a certain page ID.
     */
    unsigned getSegmentForPid(const PageID& pid);

    /** \brief Gives the first page ID of a given segment number.
     */
    PageID getPidForSegment(unsigned segment);

    /** \brief Kick-off restore threads and start recovering.
     */
    void start();

    /** \brief Waits until try_shutdown() returns true
     */
    void shutdown();

    /** \brief True if all segments have been restored
     *
     * CS TODO -- concurrency control?
     */
    bool all_pages_restored()
    { return numRestoredPages == lastUsedPid; }

    size_t getSegmentSize() { return segmentSize; }
    PageID getFirstDataPid() { return firstDataPid; }
    PageID getLastUsedPid() { return lastUsedPid; }
    RestoreBitmap* getBitmap() { return bitmap; }
    BackupReader* getBackup() { return backup; }
    unsigned getPrefetchWindow() { return prefetchWindow; }

    bool try_shutdown();

protected:
    RestoreBitmap* bitmap;
    RestoreScheduler* scheduler;
    LogArchiver::ArchiveDirectory* archive;
    vol_t* volume;

    // CS TODO: get rid of this annoying dependence on smthread_t
    std::vector<std::unique_ptr<RestoreThread>> restoreThreads;
    // std::vector<std::unique_ptr<std::thread>> restoreThreads;

    std::map<PageID, generic_page*> bufferedRequests;
    srwlock_t requestMutex;

    pthread_cond_t restoreCond;
    pthread_mutex_t restoreCondMutex;

    /** \brief Number of pages restored so far
     * (must be a multiple of segmentSize)
     */
    std::atomic<size_t> numRestoredPages;

    /** \brief First page ID to be restored (i.e., skipping metadata pages)
     */
    PageID firstDataPid;

    /** \brief Last page ID to be restored (after that only unused pages)
     */
    PageID lastUsedPid;

    /** \brief Reader object that abstracts access to backup segments
     */
    BackupReader* backup;

    /** \brief Number of segments to prefetch for each restore invocation
     */
    unsigned prefetchWindow;

    /** \brief Asynchronous writer for restored segments
     * If NULL, then only synchronous writes are performed.
     */
    SegmentWriter* asyncWriter;

    /** \brief Size of a segment in pages
     *
     * The segment is the unit of restore, i.e., one segment is restored at a
     * time. The bitmap keeps track of segments already restored, i.e., one bit
     * per segment.
     */
    size_t segmentSize;

    /** \brief Whether to copy restored pages into caller's buffers, avoiding
     * extra reads
     */
    bool reuseRestoredBuffer;

    /** \brief Whether to use a backup or restore from complete log history
     */
    bool useBackup;

    /** \brief Whether to write restored volume into a new backup file,
     * instead of into failed volume */
    bool takeBackup;

    /** \brief Whether to permit access to already restored pages
     * (false only for experiments that simulate traditional restore)
     */
    bool instantRestore;

    /** \brief Number of concurrent threads performing restore
    */
    size_t restoreThreadCount;

    /** \brief Always restore sequentially from the requested segment until
     * the next already-restored segment or EOF, unless a new request is
     * waiting in the scheduler. In this case, the current restore is
     * preempted before moving to the next adjacent segment and the enqueued
     * request is processed. This technique aims to optimize for restore
     * latency when there is high demand for segments, and for bandwidth when
     * most of the application working set is already restored or in the buffer
     * pool.
     */
    bool preemptive;

    /** \brief Block size with which the log archive is read.
    */
    size_t logReadSize;

    /** \brief LSN of restore_begin log record, indicating at which LSN the
     * volume failure was detected. At startup, we must wait until this LSN
     * is made available in the archiver to avoid lost updates.
     */
    lsn_t failureLSN;

    /** \brief Pin mechanism used to avoid the restore manager being destroyed
     * while other reader or writer thredas may still access it, even if just
     * to check whether restore finished or not
     */
    int32_t pinCount;

    bool pin() {
        while (true) {
            int32_t v = pinCount;
            // if pin count is -1, we are shutting down
            if (v < 0) { return false; }
            if (lintel::unsafe::atomic_compare_exchange_strong(
                        &pinCount, &v, v + 1))
            {
                return true;
            }
        }
    }

    void unpin() {
        lintel::unsafe::atomic_fetch_sub(&pinCount, 1);
    }

    /** \brief Method that executes the actual restore operations in a loop
     *
     * This method continuously gets page IDs to be restored from the scheduler
     * and performs the restore operation on the corresponding segment. The
     * method only returns once all segments have been restored.
     *
     * In the future, we may consider partitioning the volume and restore it in
     * parallel with multiple threads. To that end, this method should receive
     * a page ID interval to be restored.
     */
    void restoreLoop(unsigned id);

    /** \brief Performs restore of a single segment; invoked from restoreLoop()
     *
     * Returns the number of pages that were restored in the segment. It may be
     * less than the segment size when the last segment is restored.
     */
    void restoreSegment(char* workspace,
            LogArchiver::ArchiveScanner::RunMerger* merger,
            PageID firstPage, unsigned thread_id);

    /** \brief Concludes restore of a segment
     * Processes buffer pool requests when reuse is activated and calls
     * writeSegment() if backup is on synchronous mode. Otherwise places
     * a write request on the asynchronous SegmentWriter;
     */
    void finishSegment(char* workspace, unsigned segment, size_t count);

    /** \brief Writes a segment to the replacement device and marks it restored
     * Used by finishSegment() and SegmentWriter
     */
    void writeSegment(char* workspace, unsigned segment, size_t count);

    /** \brief Mark a segment as restored in the bitmap
     * Used by writeSegment() and restore_segment_log::redo
     */
    void markSegmentRestored(unsigned segment, bool redo = false);

    // Allow protected access from vol_t (for recovery)
    friend class vol_t;
    // .. and from asynchronous writer (declared and defined on cpp file)
    friend class SegmentWriter;

    friend class RestoreThread;
};

// TODO get rid of smthread_t and use std::thread
struct RestoreThread : smthread_t
{
    RestoreThread(RestoreMgr* mgr, unsigned id) : mgr(mgr), id(id) {};
    RestoreMgr* mgr;
    unsigned id;

    virtual void run()
    {
        mgr->restoreLoop(id);
    }
};

/** \brief Bitmap data structure that controls the progress of restore
 *
 * The bitmap contains one bit for each segment of the failed volume.  All bits
 * are initially "false", and a bit is set to "true" when the corresponding
 * segment has been restored. This class is completely oblivious to pages
 * inside a segment -- it is the callers resposibility to interpret what a
 * segment consists of.
 */
class RestoreBitmap {
public:

    enum class State {
        UNRESTORED = 0,
        RESTORING = 1,
        REPLAYED = 2,
        RESTORED = 3
    };

    RestoreBitmap(size_t size)
        : states(size, State::UNRESTORED)
    {
    }

    ~RestoreBitmap()
    {
    }

    size_t getSize() { return states.size(); }

    bool attempt_restore(unsigned i);
    void mark_restored(unsigned i);
    void mark_replayed(unsigned i);

    bool is_unrestored(unsigned i);
    bool is_restoring(unsigned i);
    bool is_replayed(unsigned i);
    bool is_restored(unsigned i);

    // void serialize(char* buf, size_t from, size_t to);
    // void deserialize(char* buf, size_t from, size_t to);

    /** Get lowest false value and highest true value in order to compress
     * serialized format. Such compression is effective in a single-pass or
     * schedule. It is basically a run-length encoding, but supporting only a
     * run of ones in the beginning and a run of zeroes in the end of the
     * bitmap
     */
    // void getBoundaries(size_t& lowestFalse, size_t& highestTrue);

protected:
    std::vector<State> states;
    srwlock_t mutex;
};

/** \brief Scheduler for restore operations. Decides what page to restore next.
 *
 * The restore loop in RestoreMgr restores segments in the order dictated
 * by this scheduler, using its next() method. The current implementation
 * is a simple FIFO queue. When the queue is empty, the first non-restored
 * segment in disk order is returned. This means that if no requests come in,
 * the restore loop behaves like a single-pass restore.
 */
class RestoreScheduler {
public:
    RestoreScheduler(const sm_options& options, RestoreMgr* restore);
    virtual ~RestoreScheduler();

    void enqueue(const PageID& pid);
    bool next(PageID& next, unsigned thread_id, bool peek = false);
    bool hasWaitingRequest();

    bool isOnDemand() { return onDemand; }

protected:
    RestoreMgr* restore;

    srwlock_t mutex;
    std::queue<PageID> queue;

    /// Support on-demand scheduling (if false, trySinglePass must be true)
    bool onDemand;

    PageID firstDataPid;
    PageID lastUsedPid;

    /** Keep track of first pid not restored to continue single-pass restore.
     * This is just a guess to prune the search for the next not restored.
     */
    PageID firstNotRestored;
    std::vector<PageID> firstNotRestoredPerThread;
    unsigned segmentsPerThread;
};

inline unsigned RestoreMgr::getSegmentForPid(const PageID& pid)
{
    // return (unsigned) (std::max(pid, firstDataPid) - firstDataPid)
    //     / segmentSize;
    return (unsigned) pid / segmentSize;
}

inline PageID RestoreMgr::getPidForSegment(unsigned segment)
{
    // return PageID(segment * segmentSize) + firstDataPid;
    return PageID(segment * segmentSize);
}

inline bool RestoreMgr::isRestored(const PageID& pid)
{
    if (!instantRestore) {
        return all_pages_restored();
    }

    unsigned seg = getSegmentForPid(pid);
    if (seg >= bitmap->getSize()) {
        return true;
    }

    return bitmap->is_restored(seg);
}

#endif
