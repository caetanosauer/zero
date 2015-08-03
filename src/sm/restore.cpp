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

    if (randomOrder) {
        // create list of segments in random order
        size_t numSegments = numPages / restore->getSegmentSize() + 1;
        randomSegments.resize(numSegments);
        for (size_t i = 0; i < numSegments; i++) {
            randomSegments[i] = i;
        }
        std::random_shuffle(randomSegments.begin(), randomSegments.end());
        currentRandomSegment = 0;
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
        if (randomOrder) {
            next = randomSegments[currentRandomSegment]
                * restore->getSegmentSize();
            currentRandomSegment++;
        }
        else {
            // if queue is empty, find the first not-yet-restored PID
            while (next <= numPages && restore->isRestored(next)) {
                // if next pid is already restored, then the whole segment is
                next = next + restore->getSegmentSize();
            }
            firstNotRestored = next;
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

    // Construct backup reader/buffer based on system options
    if (useBackup) {
        string backupImpl = options.get_string_option("sm_backup_kind",
                BackupOnDemandReader::IMPL_NAME);
        if (backupImpl == BackupOnDemandReader::IMPL_NAME) {
            backup = new BackupOnDemandReader(volume, segmentSize);
        }
        else if (backupImpl == BackupPrefetcher::IMPL_NAME) {
            int numSegments = options.get_int_option(
                    "sm_backup_prefetcher_segments", 3);
            w_assert0(numSegments > 0);
            backup = new BackupPrefetcher(volume, numSegments, segmentSize);
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
    delete bitmap;
    delete scheduler;
    delete backup;
}

inline unsigned RestoreMgr::getSegmentForPid(const shpid_t& pid)
{
    return (unsigned) std::max(pid, firstDataPid) / segmentSize;
}

inline shpid_t RestoreMgr::getPidForSegment(unsigned segment)
{
    return shpid_t(segment * segmentSize) + firstDataPid;
}

inline bool RestoreMgr::isRestored(const shpid_t& pid)
{
    if (!instantRestore) {
        return false;
    }

    if (pid < firstDataPid) {
        // first pages are metadata
        return metadataRestored;
    }

    unsigned seg = getSegmentForPid(pid);
    return bitmap->get(seg);
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

void RestoreMgr::restoreLoop()
{
    // wait until volume is actually marked as failed
    while(!takeBackup && !volume->is_failed()) {
        usleep(1000); // 1 ms
    }

    restoreMetadata();

    LogArchiver::ArchiveScanner logScan(archive);
    fixable_page_h fixable;

    W_IFDEBUG1(stopwatch_t timer;);

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

        W_IFDEBUG1(timer.reset(););

        unsigned segment = getSegmentForPid(requested);
        shpid_t firstPage = getPidForSegment(segment);

        if (firstPage > lastUsedPid) {
            DBG(<< "Restored finished on last used page ID " << lastUsedPid);
            numRestoredPages = numPages;
            break;
        }

        lpid_t start = lpid_t(volume->vid(), firstPage);
        lpid_t end = lpid_t(volume->vid(), firstPage + segmentSize);

        lsn_t lsn = lsn_t::null;
        char* workspace = backup->fix(segment);

        if (useBackup) {
            // Log archiver is queried with lowest the LSN in the segment, to
            // guarantee that all required log records will be retrieved in one
            // query. If one page is particularly old (an outlier), then the
            // query will fetch too many log records which won't be replayed.
            // We need to think of optimizations for this scenario, e.g.,
            // performing multiple queries for subsets of pages of the same
            // segment.
            generic_page* page = (generic_page*) workspace;
            for (int i = 0; i < segmentSize; i++, page++) {
                // If page ID does not match, we consider it a virgin page
                if (page->pid.page != firstPage + i) {
                    continue;
                }
                if (lsn == lsn_t::null || page->lsn < lsn) {
                    lsn = page->lsn;
                }
            }
        }

#if W_DEBUG_LEVEL>=1
        ADD_TSTAT(restore_time_read, timer.time_ms());
#endif

        LogArchiver::ArchiveScanner::RunMerger* merger =
            logScan.open(start, end, lsn);

#if W_DEBUG_LEVEL>=1
        ADD_TSTAT(restore_time_openscan, timer.time_ms());
#endif

        generic_page* page = (generic_page*) workspace;
        shpid_t current = firstPage;
        shpid_t prevPage = 0;
        size_t redone = 0;

        DBG(<< "Restoring segment " << segment << "(pages " << current << " - "
                << firstPage + segmentSize << ")");

        logrec_t* lr;
        while (merger->next(lr)) {
            DBGOUT4(<< "Would restore " << *lr);

            lpid_t lrpid = lr->construct_pid();
            w_assert1(lrpid.vol() == volume->vid());
            w_assert1(lrpid.page >= firstPage);
            w_assert1(lrpid.page >= prevPage);
            w_assert1(lrpid.page < firstPage + segmentSize);

            while (lrpid.page > current) {
                current++;
                page++;
                DBGOUT4(<< "Restoring page " << current);
            }

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

            w_assert1(lr->page_prev_lsn() == lsn_t::null ||
                lr->page_prev_lsn() == page->lsn);

            lr->redo(&fixable);
            fixable.update_initial_and_last_lsn(lr->lsn_ck());
            fixable.update_clsn(lr->lsn_ck());

            // Restored pages are always written out with proper checksum.
            // If restore thread is actually generating a new backup, this
            // is a requirement.
            page->checksum = page->calculate_checksum();
            w_assert1(page->pid == lrpid);
            w_assert1(page->checksum == page->calculate_checksum());

            prevPage = lrpid.page;
            redone++;
        }

        // current should point to the first not-restored page (excl. bound)
        if (redone > 0) { // i.e., something was restored
            current++;
            DBGTHRD(<< "Restore applied " << redone << " logrecs in segment "
                    << segment);
        }
        else {
            INC_TSTAT(restore_skipped_segs);
        }

#if W_DEBUG_LEVEL>=1
        if (redone > 0) { ADD_TSTAT(restore_time_replay, timer.time_ms()); }
        else { ADD_TSTAT(restore_time_replay_useless, timer.time_ms()); }
#endif

        // in the last segment, we may write less than the segment size
        finishSegment(workspace, segment, current - firstPage);

        backup->unfix(segment);

#if W_DEBUG_LEVEL>=1
        ADD_TSTAT(restore_time_write, timer.time_ms());
#endif

        delete merger;
    }

    DBG(<< "Restore thread finished! " << numRestoredPages
            << " pages restored");
}

void RestoreMgr::finishSegment(char* workspace, unsigned segment, size_t count)
{
    shpid_t firstPage = getPidForSegment(segment);

    if (count > 0) {
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
    markSegmentRestored(workspace, segment, takeBackup /* redo */);

    // send signal to waiting threads (acquire mutex to avoid lost signal)
    DO_PTHREAD(pthread_mutex_lock(&restoreCondMutex));
    DO_PTHREAD(pthread_cond_broadcast(&restoreCond));
    DO_PTHREAD(pthread_mutex_unlock(&restoreCondMutex));
}

void RestoreMgr::markSegmentRestored(char* workspace, unsigned segment, bool redo)
{
    spinlock_write_critical_section cs(&requestMutex);

    /*
     * Now that the segment is restored, copy it into the buffer pool frame of
     * each matching request. Acquire the mutex for that to avoid race
     * condition in which segment gets restored after caller checks but before
     * its request is placed.
     */
    if (reuseRestoredBuffer && workspace && !redo) {
        shpid_t firstPage = getPidForSegment(segment);

        for (int i = 0; i < segmentSize; i++) {
            map<shpid_t, generic_page*>::iterator pos =
                bufferedRequests.find(firstPage + i);

            if (pos != bufferedRequests.end()) {
                char* wpage = workspace + (sizeof(generic_page) * i);

                w_assert1(((generic_page*) wpage)->pid.page == firstPage + i);
                memcpy(pos->second, wpage, sizeof(generic_page));
                w_assert1(pos->second->pid.page == firstPage + i);

                DBGTHRD(<< "Deleting request " << pos->first);
                bufferedRequests.erase(pos);
            }
        }
    }

    // Mark whole segment as restored, even if no page was actually replayed
    // (i.e., segment contains only unused pages)
    numRestoredPages += segmentSize;
    if (numRestoredPages >= numPages) {
        numRestoredPages = numPages;
    }

    if (!redo) {
        sys_xct_section_t ssx(true);
        log_restore_segment(volume->vid(), segment);
        ssx.end_sys_xct(RCOK);
    }

    bitmap->set(segment);
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

    restoreLoop();

    w_assert1(bufferedRequests.size() == 0);
}
