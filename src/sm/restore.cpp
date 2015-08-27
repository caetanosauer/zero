#include "w_defines.h"

#define SM_SOURCE

#include "restore.h"
#include "logarchiver.h"
#include "vol.h"
#include "sm_options.h"
#include "backup_reader.h"

#include <algorithm>

#include "stopwatch.h"

RestoreBitmap::RestoreBitmap(size_t size)
    : bits(size, false) // initialize all bits to false
{
}

RestoreBitmap::~RestoreBitmap()
{
}

bool RestoreBitmap::get(unsigned i)
{
    spinlock_read_critical_section cs(&mutex);
    return bits.at(i);
}

void RestoreBitmap::set(unsigned i)
{
    spinlock_write_critical_section cs(&mutex);
    bits.at(i) = true;
}

void RestoreBitmap::serialize(char* buf, size_t from, size_t to)
{
    spinlock_read_critical_section cs(&mutex);
    w_assert0(from < to);
    w_assert0(to <= bits.size());

    /*
     * vector<bool> does not provide access to the internal bitmap structure,
     * so we stick with this non-efficient loop mechanism. If this becomes a
     * performance concern, the options are:
     * 1) Implement our own bitmap
     * 2) Reuse a bitmap with serialization support (e.g., Boost)
     * 3) In C++11, there is a data() method that returns the underlying array.
     *    Maybe that would work here.
     */
    size_t byte = 0, j = 0;
    for (size_t i = from; i < to; i++) {
        // set bit j on current byte
        // C++ guarantees that "true" expands to the integer 1
        buf[byte] |= (bits[i] << j++);

        // wrap around next byte
        if (j == 8) {
            j = 0;
            byte++;
        }
    }
}

void RestoreBitmap::deserialize(char* buf, size_t from, size_t to)
{
    spinlock_write_critical_section cs(&mutex);
    w_assert0(from < to);
    w_assert0(to <= bits.size());

    size_t byte = 0, j = 0;
    for (size_t i = from; i < to; i++) {
        // set if bit on position j is a one
        bits[i] = buf[byte] & (1 << j++);

        // wrap around next byte
        if (j == 8) {
            j = 0;
            byte++;
        }
    }
}

void RestoreBitmap::getBoundaries(size_t& lowestFalse, size_t& highestTrue)
{
    spinlock_read_critical_section cs(&mutex);

    lowestFalse = 0;
    highestTrue = 0;
    bool allTrueSoFar = true;
    for (size_t i = 0; i < bits.size(); i++) {
        if (bits[i]) highestTrue = i;
        if (allTrueSoFar && bits[i]) {
            lowestFalse = i + 1;
        }
        else {
            allTrueSoFar = false;
        }
    }
    w_assert0(lowestFalse <= bits.size());
    w_assert0(highestTrue < bits.size());
}

RestoreScheduler::RestoreScheduler(const sm_options& options,
        RestoreMgr* restore)
    : restore(restore)
{
    w_assert0(restore);
    numPages = restore->getNumPages();
    firstNotRestored = restore->getFirstDataPid();
    trySinglePass =
        options.get_bool_option("sm_restore_sched_singlepass", true);
    onDemand =
        options.get_bool_option("sm_restore_sched_ondemand", true);
    randomOrder =
        options.get_bool_option("sm_restore_sched_random", false);

    if (!onDemand) {
        // override single-pass option
        trySinglePass = true;
    }

    unsigned firstSegment = 0;
    if (randomOrder) {
        // create list of segments in random order
        size_t numSegments = numPages / restore->getSegmentSize() + 1;
        randomSegments.resize(numSegments);
        for (size_t i = 0; i < numSegments; i++) {
            randomSegments[i] = i;
        }
        std::random_shuffle(randomSegments.begin(), randomSegments.end());
        currentRandomSegment = 0;
        firstSegment = randomSegments[0];
    }

    if (!onDemand) {
        // prefetch first segment
        restore->getBackup()->prefetch(firstSegment, 0 /* priority */);
    }
}

RestoreScheduler::~RestoreScheduler()
{
}

void RestoreScheduler::enqueue(const shpid_t& pid)
{
    spinlock_write_critical_section cs(&mutex);
    queue.push(pid);
}

shpid_t RestoreScheduler::next()
{
    spinlock_write_critical_section cs(&mutex);

    shpid_t next = firstNotRestored;
    if (onDemand && queue.size() > 0) {
        next = queue.front();
        queue.pop();
        INC_TSTAT(restore_sched_queued);
    }
    else if (trySinglePass) {
        BackupReader* backup = restore->getBackup();
        size_t prefetchWindow = 2; // TODO make option for this

        if (randomOrder) {
            next = randomSegments[currentRandomSegment]
                * restore->getSegmentSize();
            currentRandomSegment++;

            // prefetch segments
            if (currentRandomSegment % prefetchWindow == 0) {
                for (size_t i = 0; i < prefetchWindow; i++) {
                    backup->prefetch(randomSegments[currentRandomSegment + i],
                            0 /* priority */);
                }
            }
        }
        else {
            // if queue is empty, find the first not-yet-restored PID
            while (next <= numPages && restore->isRestored(next)) {
                // if next pid is already restored, then the whole segment is
                next = next + restore->getSegmentSize();
            }
            firstNotRestored = next + restore->getSegmentSize();

            // prefetch segments
            unsigned seg = restore->getSegmentForPid(firstNotRestored);
            if (seg % prefetchWindow == 0) {
                for (size_t i = 0; i < prefetchWindow; i++) {
                    backup->prefetch(seg + i, 0 /* priority */);
                }
            }
        }
        INC_TSTAT(restore_sched_seq);
    }
    else {
        w_assert0(onDemand);
        next = shpid_t(0); // no next pid for now
    }

    /*
     * CS: there is no guarantee (from the scheduler) that next is indeed not
     * restored yet, because we do not control the bitmap from here.  The only
     * guarantee is that only one thread will be executing the restore loop (or
     * that multiple threads will restore disjunct segment sets). In the
     * current implementation, it turns out that the scheduler is only invoked
     * from the restore loop, which means that a page returned by the scheduler
     * is guaranteed to not be restored when is is picked up by the restore
     * loop, but that may change in the future.
     */
    return next;
}

void RestoreScheduler::setSinglePass(bool singlePass)
{
    spinlock_write_critical_section cs(&mutex);
    trySinglePass = singlePass;
}

/** Asynchronous writer for restored segments
 *  CS: Placed here on cpp file because it isn't used anywhere else.
 */
class SegmentWriter : public smthread_t {
public:
    SegmentWriter(RestoreMgr* restore);
    virtual ~SegmentWriter();

    /** \brief Request async write of a segment.
     *
     * If there is a write going on, i.e., mutex is held, then we wait
     * until that write is completed to place the request. This makes the
     * request mechanism simpler, since it does not require a queue.
     * Furthermore, we don't expect writes to the replacement device to be
     * (much) slower than reads from the backup device, which means this
     * situation should not occur often.
     */
    void requestWrite(char* workspace, unsigned segment, size_t count);

    virtual void run();

    void shutdown();

    struct Request {
        char* workspace;
        unsigned segment;
        size_t count;

        Request(char* w, unsigned s, size_t c)
            : workspace(w), segment(s), count(c) {}
    };

private:
    // Queue of requests
    std::queue<Request> requests;

    // Signal to writer thread that it must exit
    bool shutdownFlag;

    // Restore manager which owns this object
    RestoreMgr* restore;

    pthread_cond_t requestCond;
    pthread_mutex_t requestMutex;
};

RestoreMgr::RestoreMgr(const sm_options& options,
        LogArchiver::ArchiveDirectory* archive, vol_t* volume, bool useBackup,
        bool takeBackup)
    : smthread_t(t_regular, "Restore Manager"),
    archive(archive), volume(volume), numRestoredPages(0),
    metadataRestored(false), useBackup(useBackup), takeBackup(takeBackup),
    failureLSN(lsn_t::null)
{
    w_assert0(archive);
    w_assert0(volume);

    instantRestore = options.get_bool_option("sm_restore_instant", true);

    segmentSize = options.get_int_option("sm_restore_segsize", 1024);
    if (segmentSize <= 0) {
        W_FATAL_MSG(fcINTERNAL,
                << "Restore segment size must be a positive number");
    }

    reuseRestoredBuffer =
        options.get_bool_option("sm_restore_reuse_buffer", false);

    DO_PTHREAD(pthread_mutex_init(&restoreCondMutex, NULL));
    DO_PTHREAD(pthread_cond_init(&restoreCond, NULL));

    /**
     * We assume that the given vol_t contains the valid metadata of the
     * volume. If the device is lost in/with a system failure -- meaning that
     * it cannot be properly mounted --, it should contain the metadata of the
     * backup volume. By "metadata", we mean at least the number of pages in
     * the volume, which is required to control restore progress. The maximum
     * allocated page id, delivered by alloc_cache, is also needed
     */
    numPages = volume->num_pages();
    firstDataPid = volume->first_data_pageid();
    lastUsedPid = volume->last_used_pageid();

    scheduler = new RestoreScheduler(options, this);
    bitmap = new RestoreBitmap(numPages / segmentSize + 1);
    asyncWriter = NULL;

    // Construct backup reader/buffer based on system options
    if (useBackup) {
        string backupImpl = options.get_string_option("sm_backup_kind",
                BackupPrefetcher::IMPL_NAME);
        if (backupImpl == BackupOnDemandReader::IMPL_NAME) {
            backup = new BackupOnDemandReader(volume, segmentSize);
        }
        else if (backupImpl == BackupPrefetcher::IMPL_NAME) {
            int numSegments = options.get_int_option(
                    "sm_backup_prefetcher_segments", 3);
            w_assert0(numSegments > 0);
            backup = new BackupPrefetcher(volume, numSegments, segmentSize);
            dynamic_cast<BackupPrefetcher*>(backup)->fork();

            // Construct asynchronous writer object
            // Note that this is only valid with a prefetcher reader, because
            // otherwise only one segment can be fixed at a time
            if (options.get_bool_option("sm_backup_async_write", true)) {
                asyncWriter = new SegmentWriter(this);
                asyncWriter->fork();
            }
        }
        else {
            W_FATAL_MSG(eBADOPTION,
                    << "Invalid value for sm_backup_kind: " << backupImpl);
        }
    }
    else {
        /*
         * Even if we're not using a backup (i.e., backup-less restore), a
         * BackupReader object is still used for the restore workspace, which
         * is basically the buffer on which pages are restored.
         */
        backup = new DummyBackupReader(segmentSize);
    }
}

RestoreMgr::~RestoreMgr()
{
    if (asyncWriter) {
        asyncWriter->shutdown();
        asyncWriter->join();
        delete asyncWriter;
    }

    backup->finish();
    delete backup;

    delete bitmap;
    delete scheduler;

    DO_PTHREAD(pthread_mutex_destroy(&restoreCondMutex));
    DO_PTHREAD(pthread_cond_destroy(&restoreCond));
}

bool RestoreMgr::waitUntilRestored(const shpid_t& pid, size_t timeout_in_ms)
{
    DO_PTHREAD(pthread_mutex_lock(&restoreCondMutex));
    struct timespec timeout;
    while (!isRestored(pid)) {
        if (timeout_in_ms > 0) {
            sthread_t::timeout_to_timespec(timeout_in_ms, timeout);
            int code = pthread_cond_timedwait(&restoreCond, &restoreCondMutex,
                    &timeout);
            if (code == ETIMEDOUT) {
                return false;
            }
            DO_PTHREAD(code);
        }
        else {
            DO_PTHREAD(pthread_cond_wait(&restoreCond, &restoreCondMutex));
        }
    }
    DO_PTHREAD(pthread_mutex_unlock(&restoreCondMutex));

    return true;
}

void RestoreMgr::setSinglePass(bool singlePass)
{
    scheduler->setSinglePass(singlePass);
}

bool RestoreMgr::requestRestore(const shpid_t& pid, generic_page* addr)
{
    if (pid < firstDataPid || pid >= numPages) {
        return false;
    }

    if (!scheduler->isOnDemand()) {
        return false;
    }

    DBGTHRD(<< "Requesting restore of page " << pid);
    scheduler->enqueue(pid);

    backup->prefetch(getSegmentForPid(pid), 1 /* priority */);

    if (addr && reuseRestoredBuffer) {
        spinlock_write_critical_section cs(&requestMutex);

        /*
         * CS: Once mutex is held, check if page hasn't already been restored.
         * This avoids a race condition in which the segment gets restored
         * after the caller invoked isRestored() but before it places its
         * request. Since the restore loop also acquires this mutex before
         * copying restored page contents into the buffered addresses,
         * we guarantee that the segment cannot go from not-restored to
         * restored inside this critical section.
         */
        if (!isRestored(pid)) {
            // only one request for each page ID at a time
            // (buffer pool logic is responsible for ensuring this)
            w_assert1(bufferedRequests.find(pid) == bufferedRequests.end());

            DBGTHRD(<< "Adding request " << pid);
            bufferedRequests[pid] = addr;
            return true;
        }
    }
    return false;
}

/**
 * Metadata pages consist of
 * - PID 1: List of stores, managed by stnode_cache_t
 * - PIDs 2 until firstDataPid-1: Page allocation bitmap, managed by
 *   alloc_cache_t
 *
 * Current implementation does log allocations and store operations, but they
 * do not refer to the updated metadata page ID, and thus cannot be recovered
 * in a traditional way using a pageLSN.
 *
 * In order to restore a device's metadata, which is a pre-requisite for
 * correct recovery (and thus must be done before opening the system for
 * transactions), we require that all metadata-related log records since the
 * device creation are replayed.  This introduces two assumptions: 1) metadata
 * operations must be idempotent; and 2) recycling of old log archive
 * partitions (currently not implemented) must not throw away metadata log
 * records
 *
 * The two classes of metadata operation are page allocations and store
 * operations, both of which satisfy assumption 1.  Assumption 2 is not a
 * concern yet, because we do not support recycling the log archive.
 *
 * Currently, log records of metadata operations are always found on the
 * special page ID 0, but they could refer to any page ID outside the data
 * region (pid < firstDataPid). Thus, proper metadata restore consists of
 * simply replaying all such log records. Since the whole history must be kept,
 * a backup is also not required and never used.
 *
 * In the future, if we implement log archive recycling, this could be
 * optimized by regenerating new metadata log records based on the current
 * system state (i.e., one alloc_a_page log record for each allocated page)
 */
void RestoreMgr::restoreMetadata()
{
    if (metadataRestored) {
        return;
    }

    LogArchiver::ArchiveScanner logScan(archive);

    // open scan on pages [0, firstDataPid) to get all metadata operations
    LogArchiver::ArchiveScanner::RunMerger* merger =
        logScan.open(lpid_t(volume->vid(), 0),
                lpid_t(volume->vid(), firstDataPid),
                lsn_t::null);

    logrec_t* lr;
    while (merger->next(lr)) {
        lpid_t lrpid = lr->construct_pid();
        w_assert1(lrpid.vol() == volume->vid());
        w_assert1(lrpid.page < firstDataPid);
        lr->redo(NULL);
    }

    // no need to write back pages: done either asynchronously by
    // bf_fixed_m/page cleaner or unmount/shutdown.

    metadataRestored = true;

    // send signal to waiting threads (acquire mutex to avoid lost signal)
    DO_PTHREAD(pthread_mutex_lock(&restoreCondMutex));
    DO_PTHREAD(pthread_cond_broadcast(&restoreCond));
    DO_PTHREAD(pthread_mutex_unlock(&restoreCondMutex));

    delete merger;
}

void RestoreMgr::singlePassLoop()
{
    stopwatch_t timer;

    lsn_t startLSN = useBackup ? volume->get_backup_lsn() : lsn_t(1,0);

    lpid_t startPid = lpid_t(volume->vid(), firstDataPid);
    lpid_t endPid = lpid_t(volume->vid(), volume->num_pages());
    LogArchiver::ArchiveScanner logScan(archive);
    LogArchiver::ArchiveScanner::RunMerger* merger =
        logScan.open(startPid, endPid, startLSN);
    if (!merger) {
        // nothing to replay
        return;
    }

    logrec_t* lr = new logrec_t();
    unsigned segment = 0;
    lpid_t pid = lpid_t::null;
    shpid_t current = firstDataPid;

    char* workspace = backup->fix(segment);
    size_t pagesInSegment = 0;
    bool virgin = false;

    generic_page* page = (generic_page*) workspace;
    fixable_page_h fixable;

    while (merger->next(lr)) {
        DBGOUT4(<< "Would restore " << *lr);
        pid = lr->pid();
        timer.reset();

        if (getSegmentForPid(pid.page) != segment) {
            finishSegment(workspace, segment, segmentSize);
            ADD_TSTAT(restore_time_write, timer.time_us());

            segment = getSegmentForPid(pid.page);
            workspace = backup->fix(segment);
            ADD_TSTAT(restore_time_read, timer.time_us());

            pagesInSegment = 0;
            page = (generic_page*) workspace;
            current = getPidForSegment(segment);
        }

        w_assert1(pid.page >= current);
        while (pid.page > current) {
            // Done with current page -- move to next
            virgin = !volume->is_allocated_page(pid.page);
            if (!virgin) {
                // Restored pages are always written out with proper checksum.
                page->checksum = page->calculate_checksum();
            }
            page++;
            current++;
            pagesInSegment++;
        }

        if (virgin) { continue; }

        if (!fixable.is_fixed() || fixable.pid().page != pid.page) {
            // PID is manually set for virgin pages
            // this guarantees correct redo of multi-page logrecs
            if (page->pid != pid) {
                page->pid = pid;
            }
            fixable.setup_for_restore(page);
        }

        if (lr->lsn_ck() <= page->lsn) {
            // update may already be reflected on page
            continue;
        }

        w_assert1(page->pid == pid);
        w_assert1(lr->page_prev_lsn() == lsn_t::null ||
                lr->page_prev_lsn() == page->lsn);

        lr->redo(&fixable);
        fixable.update_initial_and_last_lsn(lr->lsn_ck());
        fixable.update_clsn(lr->lsn_ck());

        ADD_TSTAT(restore_time_replay, timer.time_us());
    }

    if (pagesInSegment > 0) {
        finishSegment(workspace, segment, segmentSize);
        ADD_TSTAT(restore_time_write, timer.time_us());
    }

    // signalize that we're done
    numRestoredPages = numPages;
}

void RestoreMgr::restoreSegment(char* workspace,
        LogArchiver::ArchiveScanner::RunMerger* merger, shpid_t firstPage)
{
    stopwatch_t timer;

    generic_page* page = (generic_page*) workspace;
    fixable_page_h fixable;
    shpid_t current = firstPage;
    shpid_t prevPage = 0;
    size_t redone = 0;
    bool virgin = false;
    unsigned segment = getSegmentForPid(firstPage);

    logrec_t* lr;
    while (merger->next(lr)) {
        DBGOUT4(<< "Would restore " << *lr);

        lpid_t lrpid = lr->construct_pid();
        w_assert1(lrpid.vol() == volume->vid());
        w_assert1(lrpid.page >= firstPage);
        w_assert1(lrpid.page >= prevPage);

        ADD_TSTAT(restore_log_volume, lr->length());

        while (lrpid.page > current) {
            // Done with current page -- move to next
            virgin = !volume->is_allocated_page(current);
            if (!virgin) {
                // Restored pages are always written out with proper checksum.
                page->checksum = page->calculate_checksum();
            }
            current++;
            page++;

            if (getSegmentForPid(current) != segment) {
                // Time to move to a new segment (multiple-segment restore)
                ADD_TSTAT(restore_time_replay, timer.time_us());

                finishSegment(workspace, segment, current - firstPage);
                ADD_TSTAT(restore_time_write, timer.time_us());

                segment = getSegmentForPid(lrpid.page);
                workspace = backup->fix(segment);
                ADD_TSTAT(restore_time_read, timer.time_us());

                page = (generic_page*) workspace;
                current = getPidForSegment(segment);
                firstPage = current;
            }
        }

        w_assert1(lrpid.page < firstPage + segmentSize);

        if (virgin) { continue; }

        w_assert1(page->pid.page == 0 || page->pid.page == current);

        if (!fixable.is_fixed() || fixable.pid().page != lrpid.page) {
            // PID is manually set for virgin pages
            // this guarantees correct redo of multi-page logrecs
            if (page->pid != lrpid) {
                page->pid = lrpid;
            }
            fixable.setup_for_restore(page);
        }

        if (lr->lsn_ck() <= page->lsn) {
            // update may already be reflected on page
            continue;
        }

        w_assert1(page->pid == lrpid);
        w_assert1(lr->page_prev_lsn() == lsn_t::null ||
                lr->page_prev_lsn() == page->lsn);

        lr->redo(&fixable);
        fixable.update_initial_and_last_lsn(lr->lsn_ck());
        fixable.update_clsn(lr->lsn_ck());

        prevPage = lrpid.page;
        redone++;

        virgin = false;
    }

    // current should point to the first not-restored page (excl. bound)
    if (redone > 0) { // i.e., something was restored
        current++;
    }

    ADD_TSTAT(restore_time_replay, timer.time_us());

    finishSegment(workspace, segment, current - firstPage);
    ADD_TSTAT(restore_time_write, timer.time_us());
}

void RestoreMgr::restoreLoop()
{
    LogArchiver::ArchiveScanner logScan(archive);

    stopwatch_t timer;

    while (numRestoredPages < numPages) {
        shpid_t requested = scheduler->next();
        if (isRestored(requested)) {
            continue;
        }
        if (requested == shpid_t(0)) {
            // no page available for now
            usleep(2000); // 2 ms
            continue;
        }
        w_assert0(requested >= firstDataPid);

        timer.reset();

        // FOR EACH SEGMENT
        unsigned segment = getSegmentForPid(requested);
        shpid_t firstPage = getPidForSegment(segment);
        size_t segmentCount = 1;

        if (firstPage > lastUsedPid) {
            DBG(<< "Restored finished on last used page ID " << lastUsedPid);
            numRestoredPages = numPages;
            break;
        }

        lpid_t start = lpid_t(volume->vid(), firstPage);
        lpid_t end = lpid_t(volume->vid(), firstPage + segmentSize);
        lsn_t backupLSN = volume->get_backup_lsn();

        /* CS TODO:
         * This optimization is incompatible with our current scheme of
         * restoring multiple segments, because the LSN must be determined when
         * opening the scan, but only after opening the scan do we know if we
         * are restoring multiple segments or not; so there is a cyclic
         * dependence. We could just fetch the first segment and use it to
         * determine the minLN, but if it turns out we can restore multiple
         * segments, we have to potentially re-fetch an earlier segment. Leave
         * the optimization out for now and think of a better solution later.
         */
#if 0
        {
            /*
             * minLSN is the highest LSN which guarantees that all logs required to
             * correclty replay updates on the given segment have a higher LSN. It
             * is used to determine where the log scan should begin. In this "if"
             * block, we try to increase minLSN by looking at each pageLSN in the
             * segment. If this is a fairly recent segment, chances are we can
             * start the scan much later than the given (pessimistic) minLSN.
             */
            lsn_t minPageLSN = lsn_t::null;
            generic_page* page = (generic_page*) workspace;
            shpid_t p = firstPage;
            for (size_t i = 0; i < numPages; i++, page++, p++) {
                // If page ID does not match, we consider it a virgin page
                if (!volume->is_allocated_page(p)) {
                    continue;
                }
                if (minPageLSN == lsn_t::null || page->lsn < minPageLSN) {
                    minPageLSN = page->lsn;
                }
            }
            if (minPageLSN > backupLSN) {
                backupLSN = minPageLSN;
            }
        }
#endif

        LogArchiver::ArchiveScanner::RunMerger* merger =
            logScan.open(start, end, backupLSN);

        if (merger) {
            // Adjust start and finish points of log archiver scan, in the
            // hope that we can restore multiple segments with a single block
            // from each run.
            // CS TODO: this trick only works if we assume all log records
            // belong to the same volume! Otherwise we have to adjust the
            // PID retriever methods in merger to consider only a given vid.

            // largest of the first (sometimes second) PIDs found in each block
            lpid_t maxStartPID = merger->getHighestFirstPID();
            lpid_t minLastPID = merger->getLowestLastPID();

            // We must start on the segment border which comes after the given
            // PID or exactly at it
            unsigned segmentsBegin = getSegmentForPid(maxStartPID.page);
            if (getPidForSegment(segmentsBegin) < maxStartPID.page) {
                segmentsBegin++;
            }
            unsigned segmentsEnd = getSegmentForPid(minLastPID.page);

            if (minLastPID.vol() == maxStartPID.vol() &&
                    segmentsEnd > segmentsBegin + 1)
            {
                // optimization is worth -- we can restore more than one segment
                vid_t vid = minLastPID.vol();
                shpid_t pidBegin = getPidForSegment(segmentsBegin);
                shpid_t pidEnd = getPidForSegment(segmentsEnd);

                merger->advanceToPID(lpid_t(vid, pidBegin));
                merger->setEndPID(lpid_t(vid, pidEnd));

                segmentCount = segmentsEnd - segmentsBegin;
                segment = segmentsBegin;
                firstPage = pidBegin;

                INC_TSTAT(restore_multiple_segments);
            }
            else {
                merger->advanceToPID(start);
            }
        }

        DBGOUT3(<< "RunMerger opened with " << merger->heapSize() << " runs"
                << " for " << segmentCount << " segments starting on LSN "
                << backupLSN);

        ADD_TSTAT(restore_time_openscan, timer.time_us());

        char* workspace = backup->fix(segment);

        ADD_TSTAT(restore_time_read, timer.time_us());

        if (!merger) {
            // segment does not need any log replay
            // CS TODO BUG -- this may be the last seg, so short I/O happens
            finishSegment(workspace, segment, segmentSize);
            INC_TSTAT(restore_skipped_segs);
            continue;
        }

        DBG(<< "Restoring segment " << getSegmentForPid(firstPage)
                << "(pages " << firstPage << " - "
                << firstPage + segmentSize << ")");

        restoreSegment(workspace, merger, firstPage);

        delete merger;
    }

    DBG(<< "Restore thread finished! " << numRestoredPages
            << " pages restored");
}

#if 0
size_t RestoreMgr::restoreSegment(char* workspace,
        LogArchiver::ArchiveScanner::RunMerger* merger,
        shpid_t firstPage)
{
    generic_page* page = (generic_page*) workspace;
    fixable_page_h fixable;
    shpid_t current = firstPage;
    shpid_t prevPage = 0;
    size_t redone = 0;
    bool virgin = false;

    DBG(<< "Restoring segment " << getSegmentForPid(firstPage)
            << "(pages " << current << " - "
            << firstPage + segmentSize << ")");

    logrec_t* lr;
    while (merger->next(lr)) {
        DBGOUT4(<< "Would restore " << *lr);

        lpid_t lrpid = lr->construct_pid();
        w_assert1(lrpid.vol() == volume->vid());
        w_assert1(lrpid.page >= firstPage);
        w_assert1(lrpid.page >= prevPage);
        w_assert1(lrpid.page < firstPage + segmentSize);

        ADD_TSTAT(restore_log_volume, lr->length());

        while (lrpid.page > current) {
            // Done with current page -- move to next
            virgin = !volume->is_allocated_page(current);
            if (!virgin) {
                // Restored pages are always written out with proper checksum.
                page->checksum = page->calculate_checksum();
            }

            current++;
            page++;
        }

        if (virgin) { continue; }

        w_assert1(page->pid.page == 0 || page->pid.page == current);

        if (!fixable.is_fixed() || fixable.pid().page != lrpid.page) {
            // PID is manually set for virgin pages
            // this guarantees correct redo of multi-page logrecs
            if (page->pid != lrpid) {
                page->pid = lrpid;
            }
            fixable.setup_for_restore(page);
        }

        if (lr->lsn_ck() <= page->lsn) {
            // update may already be reflected on page
            continue;
        }

        w_assert1(page->pid == lrpid);
        w_assert1(lr->page_prev_lsn() == lsn_t::null ||
                lr->page_prev_lsn() == page->lsn);

        lr->redo(&fixable);
        fixable.update_initial_and_last_lsn(lr->lsn_ck());
        fixable.update_clsn(lr->lsn_ck());

        prevPage = lrpid.page;
        redone++;

        virgin = false;
    }

    // current should point to the first not-restored page (excl. bound)
    if (redone > 0) { // i.e., something was restored
        current++;
        DBGTHRD(<< "Restore applied " << redone << " logrecs in segment "
                << getSegmentForPid(firstPage));
    }

    return current - firstPage;
}
#endif

void RestoreMgr::finishSegment(char* workspace, unsigned segment, size_t count)
{
    /*
     * Now that the segment is restored, copy it into the buffer pool frame
     * of each matching request. Acquire the mutex for that to avoid race
     * condition in which segment gets restored after caller checks but
     * before its request is placed.
     */
    if (reuseRestoredBuffer && count > 0) {
        spinlock_write_critical_section cs(&requestMutex);

        shpid_t firstPage = getPidForSegment(segment);

        for (size_t i = 0; i < count; i++) {
            map<shpid_t, generic_page*>::iterator pos =
                bufferedRequests.find(firstPage + i);

            if (pos != bufferedRequests.end()) {
                char* wpage = workspace + (sizeof(generic_page) * i);

                w_assert1(((generic_page*) wpage)->pid.page ==
                        firstPage + i);
                memcpy(pos->second, wpage, sizeof(generic_page));
                w_assert1(pos->second->pid.page == firstPage + i);

                DBGTHRD(<< "Deleting request " << pos->first);
                bufferedRequests.erase(pos);
            }
        }
    }

    if (asyncWriter) {
        // place write request on asynchronous writer and move on
        // (it basically calls writeSegment from another thread)
        asyncWriter->requestWrite(workspace, segment, count);
    }
    else {
        writeSegment(workspace, segment, count);
    }
}

void RestoreMgr::writeSegment(char* workspace, unsigned segment, size_t count)
{
    if (count > 0) {
        shpid_t firstPage = getPidForSegment(segment);
        w_assert0((int) count <= segmentSize);

        // write pages back to replacement device (or backup)
        if (takeBackup) {
            W_COERCE(volume->write_backup(firstPage, count, workspace));
        }
        else {
            W_COERCE(volume->write_many_pages(firstPage, (generic_page*) workspace,
                        count, true /* ignoreRestore */));
        }
        DBG(<< "Wrote out " << count << " pages of segment " << segment);
    }

    // taking a backup should not generate log records, so pass the redo flag
    markSegmentRestored(segment, takeBackup /* redo */);
    backup->unfix(segment);
}

void RestoreMgr::markSegmentRestored(unsigned segment, bool redo)
{
    // Mark whole segment as restored, even if no page was actually replayed
    // (i.e., segment contains only unused pages)
    numRestoredPages += segmentSize;
    if (numRestoredPages >= numPages) {
        numRestoredPages = numPages;
    }

    if (!redo) {
        sys_xct_section_t ssx(true);
        log_restore_segment(volume->vid(), segment);
        smlevel_0::log->flush(smlevel_0::log->curr_lsn());
        ssx.end_sys_xct(RCOK);
    }

    bitmap->set(segment);

    // send signal to waiting threads (acquire mutex to avoid lost signal)
    DO_PTHREAD(pthread_mutex_lock(&restoreCondMutex));
    DO_PTHREAD(pthread_cond_broadcast(&restoreCond));
    DO_PTHREAD(pthread_mutex_unlock(&restoreCondMutex));
}

void RestoreMgr::run()
{
    if (failureLSN == lsn_t::null && !takeBackup) {
        W_FATAL_MSG(fcINTERNAL,
                << "failureLSN must be set before forking restore mgr");
    }

    LogArchiver* la = smlevel_0::logArchiver;
    w_assert0(la);
    w_assert0(la->getDirectory());

    if (failureLSN != lsn_t::null) {
        // Wait for archiver to persist (or at least make available for
        // probing) all runs until this LSN.
        DBGTHRD(<< "Restore waiting for log archiver to reach LSN "
                << failureLSN);

        // wait for log record to be consumed
        while (la->getNextConsumedLSN() < failureLSN) {
            ::usleep(10000); // 10ms
        }

        // Time to wait until requesting a log archive flush (msec). If we're
        // lucky, log is archiving very fast and a flush request is not needed.
        int waitBeforeFlush = 100;
        ::usleep(waitBeforeFlush * 1000);

        if (la->getDirectory()->getLastLSN() < failureLSN) {
            la->requestFlushSync(failureLSN);
        }
    }

    // wait until volume is actually marked as failed
    while(!takeBackup && !volume->is_failed()) {
        usleep(1000); // 1 ms
    }

    restoreMetadata();

    if (instantRestore) {
        restoreLoop();
    }
    else {
        singlePassLoop();
    }

    w_assert1(bufferedRequests.size() == 0);
}

SegmentWriter::SegmentWriter(RestoreMgr* restore)
    : smthread_t(t_regular, "SegmentWriter"),
    shutdownFlag(false), restore(restore)
{
    w_assert1(restore);
    DO_PTHREAD(pthread_mutex_init(&requestMutex, NULL));
    DO_PTHREAD(pthread_cond_init(&requestCond, NULL));
}

SegmentWriter::~SegmentWriter()
{
    DO_PTHREAD(pthread_cond_destroy(&requestCond));
    DO_PTHREAD(pthread_mutex_destroy(&requestMutex));
}

void SegmentWriter::requestWrite(char* workspace,
        unsigned segment, size_t count)
{
    CRITICAL_SECTION(cs, &requestMutex);
    // mutex held => writer thread waiting for us

    Request req(workspace, segment, count);
    requests.push(req);

    pthread_cond_signal(&requestCond);
}

void SegmentWriter::run()
{
    while (true) {
        DO_PTHREAD(pthread_mutex_lock(&requestMutex));

        while(requests.size() == 0) {
            struct timespec timeout;
            sthread_t::timeout_to_timespec(100, timeout); // 100ms
            int code = pthread_cond_timedwait(&requestCond, &requestMutex,
                    &timeout);
            if (code == ETIMEDOUT) {
                if (shutdownFlag) {
                    DO_PTHREAD(pthread_mutex_unlock(&requestMutex));
                    return;
                }
            }
            DO_PTHREAD_TIMED(code);
        }

        // copy values to perform write without holding mutex
        Request req = requests.front();
        requests.pop();

        DO_PTHREAD(pthread_mutex_unlock(&requestMutex));

        stopwatch_t timer;

        restore->writeSegment(req.workspace, req.segment, req.count);

        ADD_TSTAT(restore_async_write_time, timer.time_us());
    }
}

void SegmentWriter::shutdown()
{
    CRITICAL_SECTION(cs, &requestMutex);
    shutdownFlag = true;
}
