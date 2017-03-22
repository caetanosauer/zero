#include "w_defines.h"

#define SM_SOURCE

#include "restore.h"
#include "logarchiver.h"
#include "log_core.h"
#include "bf_tree.h"
#include "vol.h"
#include "sm_options.h"
#include "backup_reader.h"
#include "xct_logger.h"

#include <algorithm>
#include <random>

#include "stopwatch.h"

// void RestoreBitmap::serialize(char* buf, size_t from, size_t to)
// {
//     spinlock_read_critical_section cs(&mutex);
//     w_assert0(from < to);
//     w_assert0(to <= bits.size());

//     /*
//      * vector<bool> does not provide access to the internal bitmap structure,
//      * so we stick with this non-efficient loop mechanism. If this becomes a
//      * performance concern, the options are:
//      * 1) Implement our own bitmap
//      * 2) Reuse a bitmap with serialization support (e.g., Boost)
//      * 3) In C++11, there is a data() method that returns the underlying array.
//      *    Maybe that would work here.
//      */
//     size_t byte = 0, j = 0;
//     for (size_t i = from; i < to; i++) {
//         // set bit j on current byte
//         // C++ guarantees that "true" expands to the integer 1
//         buf[byte] |= (bits[i] << j++);

//         // wrap around next byte
//         if (j == 8) {
//             j = 0;
//             byte++;
//         }
//     }
// }

// void RestoreBitmap::deserialize(char* buf, size_t from, size_t to)
// {
//     spinlock_write_critical_section cs(&mutex);
//     w_assert0(from < to);
//     w_assert0(to <= bits.size());

//     size_t byte = 0, j = 0;
//     for (size_t i = from; i < to; i++) {
//         // set if bit on position j is a one
//         bits[i] = buf[byte] & (1 << j++);

//         // wrap around next byte
//         if (j == 8) {
//             j = 0;
//             byte++;
//         }
//     }
// }

// void RestoreBitmap::getBoundaries(size_t& lowestFalse, size_t& highestTrue)
// {
//     spinlock_read_critical_section cs(&mutex);

//     lowestFalse = 0;
//     highestTrue = 0;
//     bool allTrueSoFar = true;
//     for (size_t i = 0; i < bits.size(); i++) {
//         if (bits[i]) highestTrue = i;
//         if (allTrueSoFar && bits[i]) {
//             lowestFalse = i + 1;
//         }
//         else {
//             allTrueSoFar = false;
//         }
//     }
//     w_assert0(lowestFalse <= bits.size());
//     w_assert0(highestTrue < bits.size());
// }

RestoreScheduler::RestoreScheduler(const sm_options& options, RestoreMgr* restore)
    : restore(restore)
{
    w_assert0(restore);
    firstNotRestored = PageID(0);
    lastUsedPid = restore->getLastUsedPid();
    // trySinglePass =
    //     options.get_bool_option("sm_restore_sched_singlepass", true);
    onDemand = options.get_bool_option("sm_restore_sched_ondemand", true);
    unsigned threads = options.get_int_option("sm_restore_threads", 1);

    segmentsPerThread = (lastUsedPid / restore->getSegmentSize()) / threads;
    for (unsigned i = 0; i < threads; i++) {
        firstNotRestoredPerThread.push_back(segmentsPerThread * i * restore->getSegmentSize());
    }

    // if (!onDemand) {
    //     // override single-pass option
    //     trySinglePass = true;
    // }
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

// bool RestoreScheduler::next(PageID& next, unsigned thread_id, bool peek)
// {
//     spinlock_write_critical_section cs(&mutex);

//     // TODO ignoring queue for now
//     // if (queue.size() > 0) {
//     //     next = queue.front();
//     //     if (!peek && is_mine(next)) {
//     //         queue.pop();
//     //         INC_TSTAT(restore_sched_queued);
//     //     }
//     // }

//     next = firstNotRestoredPerThread[thread_id];
//     // if queue is empty, find the first not-yet-restored PID
//     while (next <= lastUsedPid && restore->isRestored(next)) {
//         // if next pid is already restored, then the whole segment is
//         next = next + restore->getSegmentSize();
//     }
//     if (!peek) {
//         firstNotRestoredPerThread[thread_id] = next + restore->getSegmentSize();
//         INC_TSTAT(restore_sched_seq);
//     }

//     if (next > lastUsedPid) { return false; }

//     return true;
// }

// CS TODO: provide different implementations of scheduler
bool RestoreScheduler::next(PageID& next, unsigned thread_id, bool peek)
{
    static std::default_random_engine gen;
    static std::uniform_int_distribution<unsigned> distr(0, 100);

    spinlock_write_critical_section cs(&mutex);

    bool singlePass = (thread_id == 0);
    if (queue.size() > 0) {
        next = queue.front();
        if (!peek) {
            queue.pop();
            INC_TSTAT(restore_sched_queued);
        }
    }
    else if (singlePass) {
        next = firstNotRestored;
        // if queue is empty, find the first not-yet-restored PID
        while (next <= lastUsedPid && restore->isRestored(next)) {
            // if next pid is already restored, then the whole segment is
            next = next + restore->getSegmentSize();
        }
        if (!peek) {
            firstNotRestored = next + restore->getSegmentSize();
            INC_TSTAT(restore_sched_seq);
        }
    }
    else {
        // if not in singlePass mode, pick a random segment after firstNotRestored
        INC_TSTAT(restore_sched_random);
        unsigned rnd = distr(gen);
        next = firstNotRestored + (rnd * ((lastUsedPid - firstNotRestored) / 100));
    }

    if (next > lastUsedPid) { return false; }

    return true;
}

void SegmentRestorer::bf_restore(unsigned segment, size_t segmentSize)
{
    PageID first_pid = segment * segmentSize;
    GenericPageIterator pbegin {first_pid, segmentSize, true /*virgin*/};
    GenericPageIterator pend;

    ArchiveScanner logScan {smlevel_0::logArchiver->getIndex()};
    // TODO get from sm options
    size_t readSize = 32768;
    auto logiter = logScan.open(first_pid, 0, lsn_t::null, readSize);

    // CS TODO: how about log records still in the recovery log?
    LogReplayer::replay(logiter, pbegin, pend);

    Logger::log_sys<restore_segment_log>(segment);
}

template <class LogScan, class PageIter>
void LogReplayer::replay(LogScan logs, PageIter pagesBegin, PageIter pagesEnd)
{
    fixable_page_h fixable;
    auto page = pagesBegin;
    logrec_t* lr;

    lsn_t prev_lsn = lsn_t::null;
    PageID prev_pid = 0;
    unsigned replayed = 0;

    while (logs->next(lr)) {
        auto pid = lr->pid();
        w_assert0(pid > prev_pid || (pid == prev_pid && lr->lsn() > prev_lsn));

        while (page != pagesEnd && (*page)->pid < pid) {
            ++page;
        }
        if (page == pagesEnd) { return; }

        auto p = *page;

        fixable.setup_for_restore(p);

        if (lr->lsn() > fixable.get_page_lsn()) {
            w_assert0(lr->page_prev_lsn() == lsn_t::null ||
                    lr->page_prev_lsn() == p->lsn);

            lr->redo(&fixable);
        }

        prev_pid = pid;
        prev_lsn = lr->lsn();

        ADD_TSTAT(restore_log_volume, lr->length());
        replayed++;
    }
}

// void RestoreScheduler::setSinglePass(bool singlePass)
// {
//     spinlock_write_critical_section cs(&mutex);
//     trySinglePass = singlePass;
// }

/** Asynchronous writer for restored segments
 *  CS: Placed here on cpp file because it isn't used anywhere else.
 */
class SegmentWriter : public thread_wrapper_t {
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
        ArchiveIndex* index, vol_t* volume, PageID lastUsedPid, bool useBackup,
        bool takeBackup)
    :
    archIndex(index), volume(volume), numRestoredPages(0),
    lastUsedPid(lastUsedPid), useBackup(useBackup), takeBackup(takeBackup),
    failureLSN(lsn_t::null), pinCount(0),
    shutdownFlag(false)
{
    w_assert0(archIndex);
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

    restoreThreadCount =
        options.get_int_option("sm_restore_threads", 1);

    logReadSize =
        options.get_int_option("sm_restore_log_read_size", 1048576);

    DO_PTHREAD(pthread_mutex_init(&restoreCondMutex, NULL));
    DO_PTHREAD(pthread_cond_init(&restoreCond, NULL));

    asyncWriter = NULL;

    // Construct backup reader/buffer based on system options
    if (useBackup) {
        string backupImpl = options.get_string_option("sm_backup_kind",
                BackupOnDemandReader::IMPL_NAME);
        if (backupImpl == BackupOnDemandReader::IMPL_NAME) {
            backup = new BackupOnDemandReader(volume, segmentSize, restoreThreadCount);
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
        backup = new DummyBackupReader(segmentSize, restoreThreadCount);
    }

    scheduler = new RestoreScheduler(options, this);
    bitmap = new RestoreBitmap(lastUsedPid / segmentSize + 1);
}

bool RestoreMgr::try_shutdown(bool wait)
{
    if (pinCount < 0) {
        // we've already shut down
        return true;
    }

    if (wait && !all_pages_restored()) {
        // restore not finished yet -- can't shutdown
        return false;
    }

    shutdownFlag = true;

    // Try to atomically switch pin count from 0 to -1. If that fails, it means
    // someone is still using us and we can't shutdown.
    int32_t z = 0;
    if (!lintel::unsafe::atomic_compare_exchange_strong(&pinCount, &z, -1)) {
        return false;
    }

    w_assert1(restoreThreads.size() == restoreThreadCount);
    for (size_t i = 0; i < restoreThreads.size(); i++) {
        restoreThreads[i]->join();
    }
    restoreThreads.clear();

    if (asyncWriter) {
        asyncWriter->shutdown();
        asyncWriter->join();
    }

    backup->finish();

    sys_xct_section_t ssx(true);
    Logger::log_sys<restore_end_log>();
    ssx.end_sys_xct(RCOK);

    return true;
}

RestoreMgr::~RestoreMgr()
{
    shutdown();
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
            smthread_t::timeout_to_timespec(timeout_in_ms, timeout);
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
        auto seg = getSegmentForPid(pid);
        if (!bitmap->is_replayed(seg)) {
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
        std::shared_ptr<ArchiveScanner::RunMerger> merger, PageID firstPage,
        unsigned thread_id)
{
    INC_TSTAT(restore_invocations);
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

            if (shutdownFlag) { return; }

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
                if (lrpid > lastUsedPid || (preemptive && scheduler->hasWaitingRequest()))
                {
                    // we were preempted
                    INC_TSTAT(restore_preempt_queue);
                    merger->close();
                    return;
                }

                bool got_attempt = bitmap->attempt_restore(segment);
                if (!got_attempt) {
                    // someone is already restoring the segment
                    INC_TSTAT(restore_preempt_bitmap);
                    merger->close();
                    return;
                }

                workspace = backup->fix(segment, thread_id);
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

void RestoreMgr::restoreLoop(unsigned id)
{
    ArchiveScanner logScan(archIndex);

    stopwatch_t timer;

    while (numRestoredPages < lastUsedPid) {
        PageID requested;
        if (!scheduler->next(requested, id)) {
            // no page available for now
            usleep(2000); // 2 ms
            continue;
        }

        if (shutdownFlag) { break; }

        timer.reset();

        // FOR EACH SEGMENT
        unsigned segment = getSegmentForPid(requested);
        PageID firstPage = getPidForSegment(segment);

        if (!bitmap->attempt_restore(segment)) {
            // someone is already restoring or has already restored the segment
            continue;
        }

        timer.reset();

        char* workspace = backup->fix(segment, id);
        ADD_TSTAT(restore_time_read, timer.time_us());

        PageID startPID = firstPage;
        PageID endPID = preemptive ? 0 : firstPage + segmentSize;

        lsn_t backupLSN = volume->get_backup_lsn();

        auto merger = logScan.open(startPID, endPID, backupLSN, 0);

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

        restoreSegment(workspace, merger, firstPage, id);
    }

    DBG(<< "Restore thread finished! " << numRestoredPages
            << " pages restored");
}

void RestoreMgr::finishSegment(char* workspace, unsigned segment, size_t count)
{
    bitmap->mark_replayed(segment);

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
    // if (scheduler->isOnDemand()) {
    //     if (scheduler->hasWaitingRequest()) {
    //         PageID next;
    //         if (scheduler->next(next, true /* singlePass */, true /* peek */)) {
    //             backup->prefetch(next);
    //         }
    //     } else {
    //         backup->prefetch(segment + 1);
    //     }
    // }
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
            // CS TODO: disable this temporarily for EDBT demo; reason is that
            // we have a bug when overwriting a page that was already cleaned from
            // the buffer pool with an older version
            // W_COERCE(volume->write_many_pages(firstPage, (generic_page*) workspace,
            //             count, true /* ignoreRestore */));
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
        Logger::log_sys<restore_segment_log>(segment);
        smlevel_0::log->flush(smlevel_0::log->curr_lsn());
        ssx.end_sys_xct(RCOK);
    }

    bitmap->mark_restored(segment);

    if (!redo) {
        // send signal to waiting threads (acquire mutex to avoid lost signal)
        DO_PTHREAD(pthread_mutex_lock(&restoreCondMutex));
        DO_PTHREAD(pthread_cond_broadcast(&restoreCond));
        DO_PTHREAD(pthread_mutex_unlock(&restoreCondMutex));
    }
}

void RestoreMgr::markRestoredFromList(const std::vector<uint32_t>& segments)
{
    for (auto s : segments) {
        markSegmentRestored(s, true);
    }
}

void RestoreMgr::start()
{
    LogArchiver* la = smlevel_0::logArchiver;
    w_assert0(la);
    w_assert0(la->getIndex());

    if (failureLSN != lsn_t::null) {
        // Wait for archiver to persist (or at least make available for
        // probing) all runs until this LSN.
        stopwatch_t timer;
        ERROUT(<< "Restore waiting for log archiver to reach LSN "
                << failureLSN);

        la->archiveUntilLSN(failureLSN);

        ERROUT(<< "Log archiver finished in " << timer.time() << " seconds");
    }

    // if doing offline or single-pass restore, prefetch all segments
    if (!scheduler->isOnDemand() || !instantRestore) {
        unsigned last = getSegmentForPid(lastUsedPid);
        for (unsigned i = 0; i <= last; i++) {
            backup->prefetch(i);
        }
    }

    // kick-off restore threads
    w_assert0(restoreThreadCount > 0);
    for (unsigned i = 0; i < restoreThreadCount; i++) {
        // restoreThreads.emplace_back(new std::thread {&RestoreMgr::restoreLoop, this});
        RestoreThread* t = new RestoreThread {this, i};
        t->fork();
        restoreThreads.emplace_back(std::move(t));
    }
}

void RestoreMgr::shutdown()
{
    w_assert1(bufferedRequests.size() == 0);
    while (!try_shutdown(false)) {
        ::usleep(1000000); // 1 sec
    }
}

SegmentWriter::SegmentWriter(RestoreMgr* restore)
    : shutdownFlag(false), restore(restore)
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
            smthread_t::timeout_to_timespec(100, timeout); // 100ms
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
