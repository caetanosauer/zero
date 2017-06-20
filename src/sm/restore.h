#include "w_defines.h"

#ifndef RESTORE_H
#define RESTORE_H

#include "worker_thread.h"
#include "sm_base.h"
#include "logarchive_scanner.h"

#include <queue>
#include <map>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <chrono>
#include <condition_variable>

class sm_options;
class RestoreBitmap;
class RestoreScheduler;
class generic_page;
class BackupReader;
class SegmentWriter;
class RestoreThread;
class ArchiveIndex;

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
    RestoreMgr(const sm_options&, ArchiveIndex*, vol_t*, PageID lastDataPid,
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

    /** \brief Marks all segments in the given container as restored.
     * Used during log analysis if there is a restore going on.
     */
    void markRestoredFromList(const std::vector<uint32_t>& segments);

    /** \brief True if all segments have been restored
     *
     * CS TODO -- concurrency control?
     */
    bool all_pages_restored()
    { return numRestoredPages == lastUsedPid; }

    size_t getNumRestoredPages()
    {
      return numRestoredPages;
    }

    size_t getSegmentSize() { return segmentSize; }
    PageID getFirstDataPid() { return firstDataPid; }
    PageID getLastUsedPid() { return lastUsedPid; }
    RestoreBitmap* getBitmap() { return bitmap; }
    BackupReader* getBackup() { return backup; }
    unsigned getPrefetchWindow() { return prefetchWindow; }

    bool try_shutdown(bool wait = true);

protected:
    RestoreBitmap* bitmap;
    RestoreScheduler* scheduler;
    ArchiveIndex* archIndex;
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
            std::shared_ptr<ArchiveScanner::RunMerger> merger,
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

    /** \brief Flag to signalize shutdown to restore threads
     */
    std::atomic<bool> shutdownFlag;

    // Allow protected access from vol_t (for recovery)
    friend class vol_t;
    // .. and from asynchronous writer (declared and defined on cpp file)
    friend class SegmentWriter;

    friend class RestoreThread;
};

struct RestoreThread : thread_wrapper_t
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
        : _size(size)
    {
        states = new std::atomic<State>[size];
        for (size_t i = 0; i < size; i++) {
            states[i] = State::UNRESTORED;
        }
    }

    ~RestoreBitmap()
    {
        delete[] states;
    }

    size_t get_size() { return _size; }


    bool is_unrestored(unsigned i) const
    {
        return states[i] == State::UNRESTORED;
    }

    bool is_restoring(unsigned i) const
    {
        return states[i] == State::RESTORING;
    }

    bool is_replayed(unsigned i) const
    {
        return states[i] >= State::REPLAYED;
    }

    bool is_restored(unsigned i) const
    {
        return states[i] == State::RESTORED;
    }

    bool attempt_restore(unsigned i)
    {
        auto expected = State::UNRESTORED;
        return states[i].compare_exchange_strong(expected, State::RESTORING);
    }

    void mark_replayed(unsigned i)
    {
        w_assert1(states[i] == State::RESTORING);
        states[i] = State::REPLAYED;
    }

    void mark_restored(unsigned i)
    {
        // w_assert1(states[i] == State::REPLAYED);
        states[i] = State::RESTORED;
    }

    unsigned get_first_unrestored() const
    {
        for (unsigned i = 0; i < _size; i++) {
            if (states[i] == State::UNRESTORED) { return i; }
        }
        return _size;
    }

    // TODO: implement these to checkpoint bitmap state
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
    std::atomic<State>* states;
    const size_t _size;
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

/** \brief Coordinator that synchronizes multi-threaded decentralized restore
 * (i.e., without RestoreMgr)
 */
template <typename RestoreFunctor>
class RestoreCoordinator
{
public:

    RestoreCoordinator(size_t segSize, size_t segCount, RestoreFunctor f,
            bool virgin_pages, bool start_locked)
        : _segmentSize{segSize}, _bitmap{new RestoreBitmap {segCount}},
        _restoreFunctor{f}, _virgin_pages{virgin_pages}, _start_locked(start_locked)
    {
        if (_start_locked) {
            _mutex.lock();
        }
    }

    void fetch(PageID pid)
    {
        using namespace std::chrono_literals;

        auto segment = pid / _segmentSize;
        if (segment >= _bitmap->get_size() || _bitmap->is_replayed(segment)) {
            return;
        }

        std::unique_lock<std::mutex> lck {_mutex};

        // check again in critical section
        if (_bitmap->is_replayed(segment)) { return; }

        // Segment not restored yet: we must attempt to restore it ourselves or
        // wait on a ticket if it's already being restored
        auto ticket = getWaitingTicket(segment);

        if (_bitmap->attempt_restore(segment)) {
            lck.unlock();
            doRestore(segment, ticket);
        }
        else {
            auto pred = [this, segment] { return _bitmap->is_replayed(segment); };
            while (!pred()) { ticket->wait_for(lck, 1ms, pred); }
        }
    }

    bool tryBackgroundRestore(bool& done)
    {
        done = false;

        // If no restore requests are pending, restore the first
        // not-yet-restored segment.
        if (!_waiting_table.empty()) { return false; }

        std::unique_lock<std::mutex> lck {_mutex};
        auto segment = _bitmap->get_first_unrestored();

        if (segment == _bitmap->get_size()) {
            // All segments restored
            done = true;
            return false;
        }

        auto ticket = getWaitingTicket(segment);

        if (_bitmap->attempt_restore(segment)) {
            lck.unlock();
            doRestore(segment, ticket);
            return true;
        }

        return false;
    }

    bool isPidRestored(PageID pid) const
    {
        auto segment = pid / _segmentSize;
        return segment >= _bitmap->get_size() || _bitmap->is_replayed(segment);
    }

    void start()
    {
        if (_start_locked) {
            _mutex.unlock();
        }
    }

private:
    using Ticket = std::shared_ptr<std::condition_variable>;

    const size_t _segmentSize;

    std::mutex _mutex;
    std::unordered_map<unsigned, Ticket> _waiting_table;
    std::unique_ptr<RestoreBitmap> _bitmap;
    RestoreFunctor _restoreFunctor;
    const bool _virgin_pages;
    // This is used to make threads wait for log archiver reach a certain LSN
    const bool _start_locked;

    Ticket getWaitingTicket(unsigned segment)
    {
        auto it = _waiting_table.find(segment);
        if (it == _waiting_table.end()) {
            auto ticket = make_shared<std::condition_variable>();
            _waiting_table[segment] = ticket;
            return ticket;
        }
        else { return it->second; }
    }

    void doRestore(unsigned segment, Ticket ticket)
    {
        _restoreFunctor(segment, _segmentSize, _virgin_pages);

        _bitmap->mark_replayed(segment);
        ticket->notify_all();

        std::unique_lock<std::mutex> lck {_mutex};
        _waiting_table.erase(segment);
    }
};

struct LogReplayer
{
    template <class LogScan, class PageIter>
    static void replay(LogScan logs, PageIter& pagesBegin, PageIter pagesEnd);
};

struct SegmentRestorer
{
    static void bf_restore(unsigned segment, size_t segmentSize, bool virgin_pages);
};

/** Thread that restores untouched segments in the background with low priority */
template <class Coordinator, class OnDoneCallback>
class BackgroundRestorer : public worker_thread_t
{
public:
    BackgroundRestorer(std::shared_ptr<Coordinator> coord, OnDoneCallback callback)
        : _coord(coord), _notify_done(callback)
    {
    }

    virtual void do_work()
    {
        constexpr int sleep_time = 5;
        bool done = false;
        bool restored_last = false;

        while (true) {
            if (!restored_last) {
                std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
            }
            restored_last = _coord->tryBackgroundRestore(done);

            if (done) {
                _notify_done();
                break;
            }

            if (should_exit()) { break; }
        }

        quit();
    }

private:
    std::shared_ptr<Coordinator> _coord;
    OnDoneCallback _notify_done;
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
    if (seg >= bitmap->get_size()) {
        return true;
    }

    return bitmap->is_restored(seg);
}

#endif
