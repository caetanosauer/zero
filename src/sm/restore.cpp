#include "w_defines.h"

#define SM_SOURCE

#include "restore.h"
#include "logarchiver.h"
#include "log_core.h"
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
    firstNotRestored = PageID(0);
    lastUsedPid = restore->getLastUsedPid();
    trySinglePass =
        options.get_bool_option("sm_restore_sched_singlepass", true);
    onDemand =
        options.get_bool_option("sm_restore_sched_ondemand", true);

    if (!onDemand) {
        // override single-pass option
        trySinglePass = true;
    }
}

RestoreScheduler::~RestoreScheduler()
{
}

void RestoreScheduler::enqueue(const PageID& pid)
{
    spinlock_write_critical_section cs(&mutex);
    queue.push(pid);
}

bool RestoreScheduler::hasWaitingRequest()
{
    return onDemand && queue.size() > 0;
}

bool RestoreScheduler::next(PageID& next, bool peek)
{
    spinlock_write_critical_section cs(&mutex);

    next = firstNotRestored;
    if (onDemand && queue.size() > 0) {
        next = queue.front();
        if (!peek) {
            queue.pop();
            INC_TSTAT(restore_sched_queued);
        }
    }
    else if (trySinglePass) {
        // if queue is empty, find the first not-yet-restored PID
        while (next <= lastUsedPid && restore->isRestored(next)) {
            // if next pid is already restored, then the whole segment is
            next = next + restore->getSegmentSize();
        }
        if (!peek) {
            firstNotRestored = next + restore->getSegmentSize();
        }

        if (next > lastUsedPid) { next = 0; }

        if (!peek) { INC_TSTAT(restore_sched_seq); }
    }
    else {
        w_assert0(onDemand);
        return false;
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
    return true;
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
    useBackup(useBackup), takeBackup(takeBackup),
    failureLSN(lsn_t::null), pinCount(0)
{
    w_assert0(archive);
    w_assert0(volume);

    instantRestore = options.get_bool_option("sm_restore_instant", true);
    preemptive = options.get_bool_option("sm_restore_preemptive", false);

    segmentSize = options.get_int_option("sm_restore_segsize", 1024);
    if (segmentSize <= 0) {
        W_FATAL_MSG(fcINTERNAL,
                << "Restore segment size must be a positive number");
    }
    prefetchWindow =
        options.get_int_option("sm_restore_prefetcher_window", 1);

    reuseRestoredBuffer =
        options.get_bool_option("sm_restore_reuse_buffer", false);

    logReadSize =
        options.get_int_option("sm_restore_log_read_size", 1048576);

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
    lastUsedPid = volume->get_last_allocated_pid();

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
                    "sm_backup_prefetcher_segments", 5);
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

    scheduler = new RestoreScheduler(options, this);
    bitmap = new RestoreBitmap(lastUsedPid / segmentSize + 1);
    replayedBitmap = new RestoreBitmap(lastUsedPid / segmentSize + 1);
}

void RestoreMgr::shutdown()
{
    while (true) {
        // Try to set pin from 0 to -1 until we succeed. Only then it is
        // guaranteed that nobody will access the restore manager anymore.
        usleep(1000); // 1ms
        int32_t z = 0;
        if (lintel::unsafe::atomic_compare_exchange_strong(&pinCount, &z, -1))
        { break; }
    }

    w_assert0(finished());
    join();

    if (asyncWriter) {
        asyncWriter->shutdown();
        asyncWriter->join();
    }

    backup->finish();
}

RestoreMgr::~RestoreMgr()
{
    if (asyncWriter) { delete asyncWriter; }
    delete backup;
    delete bitmap;
    delete scheduler;

    DO_PTHREAD(pthread_mutex_destroy(&restoreCondMutex));
    DO_PTHREAD(pthread_cond_destroy(&restoreCond));
}

bool RestoreMgr::waitUntilRestored(const PageID& pid, size_t timeout_in_ms)
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

void RestoreMgr::setInstant(bool instant)
{
    instantRestore = instant;
}

bool RestoreMgr::requestRestore(const PageID& pid, generic_page* addr)
{
    if (pid > lastUsedPid) {
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
        if (!replayedBitmap->get(getSegmentForPid(pid))) {
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

void RestoreMgr::restoreSegment(char* workspace,
        LogArchiver::ArchiveScanner::RunMerger* merger, PageID firstPage)
{
    stopwatch_t timer;

    generic_page* page = (generic_page*) workspace;
    fixable_page_h fixable;
    PageID current = firstPage;
    PageID prevPage = 0;
    size_t redone = 0, redoneOnPage = 0;
    unsigned segment = getSegmentForPid(firstPage);

    logrec_t* lr;
    while (merger->next(lr)) {
        DBGOUT4(<< "Would restore " << *lr);

        PageID lrpid = lr->pid();
        w_assert1(lrpid >= firstPage);
        w_assert1(lrpid >= prevPage);

        ADD_TSTAT(restore_log_volume, lr->length());

        while (lrpid > current) {
            // Done with current page -- move to next
            if (redoneOnPage > 0) {
                // write checksum on non-free pages
                page->checksum = page->calculate_checksum();
            }

            current++;
            page++;
            redoneOnPage = 0;

            if (lrpid > lastUsedPid || getSegmentForPid(current) != segment) {
                // Time to move to a new segment (multiple-segment restore)
                ADD_TSTAT(restore_time_replay, timer.time_us());

                int count = current - firstPage;
                if (count > (int) segmentSize) { count = segmentSize; }
                finishSegment(workspace, segment, count);
                ADD_TSTAT(restore_time_write, timer.time_us());

                segment = getSegmentForPid(lrpid);

                // If segment already restored or someone is waiting in the
                // scheduler, terminate earlier. We also don't have to replay
                // pages created after the failure.
                if (!scheduler->isSinglePass() || lrpid > lastUsedPid
                        || replayedBitmap->get(segment)
                        || (preemptive && scheduler->hasWaitingRequest()))
                {
                    merger->close();
                    return;
                }

                workspace = backup->fix(segment);
                ADD_TSTAT(restore_time_read, timer.time_us());

                page = (generic_page*) workspace;
                current = getPidForSegment(segment);
                firstPage = current;

                INC_TSTAT(restore_multiple_segments);
            }
        }

        w_assert1(lrpid < firstPage + segmentSize);

        w_assert1(page->pid == 0 || page->pid == current);

        if (!fixable.is_fixed() || fixable.pid() != lrpid) {
            // Set PID and null LSN manually on virgin pages
            if (page->pid != lrpid) {
                page->pid = lrpid;
                page->lsn = lsn_t::null;
            }
            fixable.setup_for_restore(page);
        }

        if (lr->lsn_ck() <= page->lsn) {
            // update may already be reflected on page
            continue;
        }

        w_assert1(page->pid == lrpid);
        w_assert0(lr->page_prev_lsn() == lsn_t::null ||
                lr->page_prev_lsn() == page->lsn);

        // DBG3(<< "Replaying " << *lr);
        lr->redo(&fixable);
        fixable.update_initial_and_last_lsn(lr->lsn_ck());
        fixable.update_clsn(lr->lsn_ck());

        prevPage = lrpid;
        redone++;
        redoneOnPage++;
    }

    // current should point to the first not-restored page (excl. bound)
    if (redone > 0) { // i.e., something was restored
        DBG(<< "Replayed " << redone << " log records");
        current++;
    }

    ADD_TSTAT(restore_time_replay, timer.time_us());

    // Last page is not checksumed above, so we do it here
    page->checksum = page->calculate_checksum();

    finishSegment(workspace, segment, segmentSize);
    ADD_TSTAT(restore_time_write, timer.time_us());

    INC_TSTAT(restore_invocations);
}

void RestoreMgr::restoreLoop()
{
    LogArchiver::ArchiveScanner logScan(archive);

    stopwatch_t timer;

    while (numRestoredPages < lastUsedPid) {
        PageID requested;
        if (!scheduler->next(requested)) {
            // no page available for now
            usleep(2000); // 2 ms
            continue;
        }

        timer.reset();

        // FOR EACH SEGMENT
        unsigned segment = getSegmentForPid(requested);
        PageID firstPage = getPidForSegment(segment);

        if (replayedBitmap->get(segment)) {
            continue;
        }

        timer.reset();

        char* workspace = backup->fix(segment);
        ADD_TSTAT(restore_time_read, timer.time_us());

        PageID startPID = firstPage;
        PageID endPID = preemptive ? 0 : firstPage + segmentSize;

        lsn_t backupLSN = volume->get_backup_lsn();

        LogArchiver::ArchiveScanner::RunMerger* merger =
            logScan.open(startPID, endPID, backupLSN, 0);

        DBG3(<< "RunMerger opened with " << merger->heapSize() << " runs"
                << " starting on LSN " << backupLSN);

        ADD_TSTAT(restore_time_openscan, timer.time_us());

        if (!merger || merger->heapSize() == 0) {
            // segment does not need any log replay
            // CS TODO BUG -- this may be the last seg, so short I/O happens
            finishSegment(workspace, segment, segmentSize);
            INC_TSTAT(restore_skipped_segs);
            continue;
        }

        DBG(<< "Restoring segment " << getSegmentForPid(firstPage) << " (pages "
                << firstPage << " - " << firstPage + segmentSize << ")");

        restoreSegment(workspace, merger, firstPage);

        delete merger;
    }

    DBG(<< "Restore thread finished! " << numRestoredPages
            << " pages restored");
}

void RestoreMgr::finishSegment(char* workspace, unsigned segment, size_t count)
{
    replayedBitmap->set(segment);

    /*
     * Now that the segment is restored, copy it into the buffer pool frame
     * of each matching request. Acquire the mutex for that to avoid race
     * condition in which segment gets restored after caller checks but
     * before its request is placed.
     */
    if (reuseRestoredBuffer && count > 0) {
        spinlock_write_critical_section cs(&requestMutex);

        PageID firstPage = getPidForSegment(segment);

        for (size_t i = 0; i < count; i++) {
            map<PageID, generic_page*>::iterator pos =
                bufferedRequests.find(firstPage + i);

            if (pos != bufferedRequests.end()) {
                char* wpage = workspace + (sizeof(generic_page) * i);

                w_assert1(((generic_page*) wpage)->pid ==
                        firstPage + i);
                memcpy(pos->second, wpage, sizeof(generic_page));
                w_assert1(pos->second->pid == firstPage + i);

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

    INC_TSTAT(restore_segment_count);

    // as we're done with one segment, prefetch the next
    if (scheduler->isOnDemand()) {
        if (scheduler->hasWaitingRequest()) {
            PageID next;
            if (scheduler->next(next, true /* peek */)) {
                backup->prefetch(next);
            }
        } else {
            backup->prefetch(segment + 1);
        }
    }
}

void RestoreMgr::writeSegment(char* workspace, unsigned segment, size_t count)
{
    if (count > 0) {
        PageID firstPage = getPidForSegment(segment);
        w_assert0(count <= segmentSize);

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
    if (numRestoredPages >= lastUsedPid) {
        numRestoredPages = lastUsedPid;
    }

    if (!redo) {
        sys_xct_section_t ssx(true);
        log_restore_segment(segment);
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
        stopwatch_t timer;
        ERROUT(<< "Restore waiting for log archiver to reach LSN "
                << failureLSN);

        // wait for log record to be consumed
        while (la->getNextConsumedLSN() < failureLSN) {
            la->activate(failureLSN, true);
            ::usleep(10000); // 10ms
        }

        // Time to wait until requesting a log archive flush (msec). If we're
        // lucky, log is archiving very fast and a flush request is not needed.
        int waitBeforeFlush = 100;
        ::usleep(waitBeforeFlush * 1000);

        if (la->getDirectory()->getLastLSN() < failureLSN) {
            la->requestFlushSync(failureLSN);
        }

        ERROUT(<< "Log archiver finished in " << timer.time() << " seconds");
    }

    // wait until volume is actually marked as failed
    while(!takeBackup && !volume->is_failed()) {
        usleep(1000); // 1 ms
    }

    // if doing offline or single-pass restore, prefetch all segments
    if (!scheduler->isOnDemand() || !instantRestore) {
        unsigned last = getSegmentForPid(lastUsedPid);
        for (unsigned i = 0; i <= last; i++) {
            backup->prefetch(i);
        }
    }


    restoreLoop();

    w_assert1(bufferedRequests.size() == 0);

    sys_xct_section_t ssx(true);
    log_restore_end();
    ssx.end_sys_xct(RCOK);
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
