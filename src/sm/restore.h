#include "w_defines.h"

#ifndef RESTORE_H
#define RESTORE_H

#include "sm_base.h"
#include "logarchiver.h"

#include <queue>

class sm_options;
class RestoreBitmap;
class RestoreScheduler;
class generic_page;
class BackupReader;

/** \brief Class that controls the process of restoring a failed volume
 *
 * \author Caetano Sauer
 */
class RestoreMgr : public smthread_t {
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
            bool useBackup, lsn_t failureLSN = lsn_t::null,
            bool takeBackup = false);
    virtual ~RestoreMgr();

    /** \brief Returns true if given page is already restored.
     *
     * This method is used to check if a page has already been restored, i.e.,
     * if it can be read from the volume already.
     */
    bool isRestored(const shpid_t& pid);

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
    bool requestRestore(const shpid_t& pid, generic_page* addr = NULL);

    /** \brief Blocks until given page is restored
     *
     * This method will block until the given page ID is restored or the given
     * timeout is reached. It returns false in case of timeout and true if the
     * page is restored. When this method returns true, the caller is allowed
     * to read the page from the volume. This is basically equivalent to
     * polling on the isRestored() method.
     */
    bool waitUntilRestored(const shpid_t& pid, size_t timeout_in_ms = 0);

    /** \brief Set single-pass policy on scheduler
     *
     * If restore has so far worked only on-demand, this method activates (or
     * deactivates) the single-pass policy on the scheduler, which causes pages
     * to be restored eagerly and in disk order -- concurrently with on-demand
     * restore of accessed pages.
     */
    void setSinglePass(bool singlePass = true);

    /** \brief True if all segments have been restored
     *
     * CS TODO -- concurrency control?
     */
    bool finished()
    { return metadataRestored && numRestoredPages == numPages; }

    size_t getNumPages() { return numPages; }
    size_t getSegmentSize() { return segmentSize; }
    shpid_t getFirstDataPid() { return firstDataPid; }
    shpid_t getLastUsedPid() { return lastUsedPid; }
    RestoreBitmap* getBitmap() { return bitmap; }

    virtual void run();

protected:
    RestoreBitmap* bitmap;
    RestoreScheduler* scheduler;
    LogArchiver::ArchiveDirectory* archive;
    vol_t* volume;

    std::map<shpid_t, generic_page*> bufferedRequests;
    srwlock_t requestMutex;

    pthread_cond_t restoreCond;
    pthread_mutex_t restoreCondMutex;

    /** \brief Number of pages restored so far
     * (must be a multiple of segmentSize)
     */
    size_t numRestoredPages;

    /** \brief Total number of pages in the failed volume
     */
    size_t numPages;

    /** \brief First page ID to be restored (i.e., skipping metadata pages)
     */
    shpid_t firstDataPid;

    /** \brief Last page ID to be restored (after that only unused pages)
     */
    shpid_t lastUsedPid;

    /** \brief Reader object that abstracts access to backup segments
     */
    BackupReader* backup;

    /** \brief Size of a segment in pages
     *
     * The segment is the unit of restore, i.e., one segment is restored at a
     * time. The bitmap keeps track of segments already restored, i.e., one bit
     * per segment.
     */
    int segmentSize;

    /** \brief Whether volume metadata is alread restored or not
     */
    bool metadataRestored;

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

    /** \brief LSN of restore_begin log record, indicating at which LSN the
     * volume failure was detected. At startup, we must wait until this LSN
     * is made available in the archiver to avoid lost updates.
     */
    lsn_t failureLSN;

    /** \brief Gives the segment number of a certain page ID.
     */
    unsigned getSegmentForPid(const shpid_t& pid);

    /** \brief Gives the first page ID of a given segment number.
     */
    shpid_t getPidForSegment(unsigned segment);

    /** \brief Restores metadata by replaying store operation log records
     *
     * This method is invoked before the restore loop starts (i.e., before any
     * data page is restored). It replays all store operations -- which are
     * logged on page id 0 -- in order to correctly restore volume metadata,
     * i.e., stnode_cache_t. Allocation pages (i.e., alloc_cache_t) doesn't
     * have to be restored explicitly, because pages are re-allocated when
     * replaying their first log records (e.g., page_img_format, btree_split,
     * etc.)
     */
    void restoreMetadata();

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
    void restoreLoop();

    /** \brief Concludes restore of a segment and flushes to replacement device
     */
    void finishSegment(char* workspace, unsigned segment, size_t count);

    /** \brief Mark a segment as restored in the bitmap
     * Used by finishSegment() and restore_segment_log::redo
     */
    void markSegmentRestored(char* workspace, unsigned segment,
            bool redo = false);

    // Allow protected access from vol_t (for recovery)
    friend class vol_t;
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
    RestoreBitmap(size_t size);
    virtual ~RestoreBitmap();

    size_t getSize() { return bits.size(); }

    bool get(unsigned i);
    void set(unsigned i);

    void serialize(char* buf, size_t from, size_t to);
    void deserialize(char* buf, size_t from, size_t to);

    /** Get lowest false value and highest true value in order to compress
     * serialized format. Such compression is effective in a single-pass or
     * schedule. It is basically a run-length encoding, but supporting only a
     * run of ones in the beginning and a run of zeroes in the end of the
     * bitmap
     */
    void getBoundaries(size_t& lowestFalse, size_t& highestTrue);

protected:
    std::vector<bool> bits;
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

    void enqueue(const shpid_t& pid);
    shpid_t next();
    void setSinglePass(bool singlePass = true);

protected:
    RestoreMgr* restore;

    srwlock_t mutex;
    std::queue<shpid_t> queue;

    /// Perform single-pass restore while no requests are available
    bool trySinglePass;

    size_t numPages;

    /** Keep track of first pid not restored to continue single-pass restore.
     * This is just a guess to prune the search for the next not restored.
     */
    shpid_t firstNotRestored;

};

#endif
