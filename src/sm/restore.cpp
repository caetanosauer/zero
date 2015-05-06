#include "w_defines.h"

#define SM_SOURCE

#include "restore.h"
#include "logarchiver.h"
#include "vol.h"
#include "sm_options.h"
#include <cstring> // memcpy

RestoreBitmap::RestoreBitmap(size_t size)
    : bits(size, false) // initialize all bits to false
{
}

RestoreBitmap::~RestoreBitmap()
{
}

bool RestoreBitmap::get(size_t i)
{
    spinlock_read_critical_section cs(&mutex);
    return bits.at(i);
}

void RestoreBitmap::set(size_t i)
{
    spinlock_write_critical_section cs(&mutex);
    bits.at(i) = true;
}

RestoreScheduler::RestoreScheduler(RestoreMgr* restore)
    : restore(restore)
{
    w_assert0(restore);
    numPages = restore->getNumPages();
    firstNotRestored = restore->getFirstDataPid();
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
    if (queue.size() > 0) {
        next = queue.front();
        queue.pop();
    }
    else {
        // if queue is empty, find the first not-yet-restored PID
        while (next <= numPages && restore->isRestored(next)) {
            // if next pid is already restored, then the whole segment is
            next = next + restore->getSegmentSize();
        }
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

RestoreMgr::RestoreMgr(const sm_options& options,
        LogArchiver::ArchiveDirectory* archive, vol_t* volume)
    : smthread_t(t_regular, "Restore Manager"),
    archive(archive), volume(volume), numRestoredPages(0),
    metadataRestored(false)
{
    w_assert0(archive);
    w_assert0(volume);

    segmentSize = options.get_int_option("sm_restore_segsize", 1024);
    if (segmentSize <= 0) {
        W_FATAL_MSG(fcINTERNAL,
                << "Restore segment size must be a positive number");
    }

    DO_PTHREAD(pthread_mutex_init(&restoreCondMutex, NULL));
    DO_PTHREAD(pthread_cond_init(&restoreCond, NULL));

    /**
     * We assume that the given vol_t contains the valid metadata of the
     * volume. If the device is lost in/with a system failure -- meaning that
     * it cannot be properly mounted --, it should contain the metadata of the
     * backup volume. By "metadata", we mean at least the number of pages in
     * the volume, which is required to control restore progress. Note that
     * the number of "used" pages is of no value for restore, because pages
     * may get allocated and deallocated (possibly multiple times) during log
     * replay.
     */
    numPages = volume->num_pages();
    firstDataPid = volume->first_data_pageid();

    scheduler = new RestoreScheduler(this);
    bitmap = new RestoreBitmap(numPages);
}

RestoreMgr::~RestoreMgr()
{
    delete bitmap;
    delete scheduler;
}

inline size_t RestoreMgr::getSegmentForPid(const shpid_t& pid)
{
    return (size_t) std::max(pid, firstDataPid) / segmentSize;
}

inline shpid_t RestoreMgr::getPidForSegment(size_t segment)
{
    return shpid_t(segment * segmentSize) + firstDataPid;
}

inline bool RestoreMgr::isRestored(const shpid_t& pid)
{
    if (pid == 0) {
        // pid 0 signifies metadata (stnode cache)
        return metadataRestored;
    }

    size_t seg = getSegmentForPid(pid);
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

bool RestoreMgr::requestRestore(const shpid_t& pid, generic_page* addr)
{
    if (pid < firstDataPid || pid >= numPages) {
        return false;
    }

    scheduler->enqueue(pid);

    if (addr) {
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

            bufferedRequests[pid] = addr;
            return true;
        }
    }
    return false;
}

void RestoreMgr::restoreMetadata()
{
    LogArchiver::ArchiveScanner logScan(archive);

    // open scan on pages [0,1) to get all store operation log records
    LogArchiver::ArchiveScanner::RunMerger* merger =
        logScan.open(lpid_t(volume->vid(), 0), lpid_t(volume->vid(), 1),
                lsn_t::null);

    // TODO: is redo of store operations idempotent?
    // should we use a specific LSN as starting point?

    logrec_t* lr;
    while (merger->next(lr)) {
        lr->redo(NULL);
    }

    // TODO: my guess is that we don't have to write back the restored
    // metadata pages because there are only two possibilites:
    // 1) system eventually crashes -- metadata restored in restart from
    // checkpoint and log analysis
    //    (In theory, all metadata could be considered "server state" and be
    //    maintained exclusively in the log with checkpoints)
    // 2) device is eventually unmounted (e.g., during clean shutdown) --
    // pages get written back

    metadataRestored = true;
}

void RestoreMgr::restoreLoop()
{
    // wait until volume is actually marked as failed
    while(!volume->is_failed()) {
        usleep(1000);
        lintel::atomic_thread_fence(lintel::memory_order_consume);
    }

    restoreMetadata();

    LogArchiver::ArchiveScanner logScan(archive);

    char* workspace = new char[sizeof(generic_page) * segmentSize];

    while (numRestoredPages < numPages) {
        shpid_t requested = scheduler->next();
        if (isRestored(requested)) {
            continue;
        }
        w_assert0(requested >= firstDataPid);

        size_t segment = getSegmentForPid(requested);
        shpid_t firstPage = getPidForSegment(segment);

        lpid_t start = lpid_t(volume->vid(), firstPage);
        lpid_t end = lpid_t(volume->vid(), firstPage + segmentSize);

        /*
         * CS: for the current milestone, we are ignoring backups and
         * performing restore based on the complete log archive (since creation of the
         * database).
         */
        lsn_t lsn = lsn_t::null;
        LogArchiver::ArchiveScanner::RunMerger* merger =
            logScan.open(start, end, lsn);

        generic_page* page = (generic_page*) workspace;
        fixable_page_h fixable;
        shpid_t current = firstPage;
        shpid_t prevPage = 0;
        size_t redone = 0;

        DBG(<< "Restoring segment " << segment << " page " << current);

        logrec_t* lr;
        while (merger->next(lr)) {
            DBG(<< "Would restore " << *lr);

            lpid_t lrpid = lr->construct_pid();
            w_assert1(lrpid.vol() == volume->vid());
            w_assert1(lrpid.page >= firstPage);
            w_assert1(lrpid.page >= prevPage);
            w_assert1(lrpid.page < firstPage + segmentSize);

            if (!fixable.is_fixed() || fixable.pid().page != lrpid.page) {
                // PID is manually set for virgin pages
                if (page->pid != lrpid) {
                    page->pid = lrpid;
                }
                fixable.setup_for_restore(page);
            }

            while (lrpid.page > current) {
                current++;
                page++;
                DBG(<< "Restoring page " << current);

                // TODO: should we really zero-out the checksum here?  In
                // principle, checksum does not have to match after applying a
                // REDO operation, since it may be purely (physio)logical. On
                // the other hand, REDO operations should update the checksum
                // correctly... Zeroing out now because checksum failure seems
                // to always happen.
                page->checksum = 0;
            }

            lr->redo(&fixable);
            fixable.update_initial_and_last_lsn(lr->lsn_ck());
            fixable.update_clsn(lr->lsn_ck());

            prevPage = lrpid.page;
            redone++;
        }

        // current should point to the first not-restored page (excl. bound)
        if (redone > 0) { // i.e., something was restored
            current++;
        }

        // in the last segment, we may write less than the segment size
        finishSegment(segment, workspace, current - firstPage);
    }

    DBG(<< "Restore thread finished!");
}

void RestoreMgr::finishSegment(size_t segment, char* workspace, size_t count)
{
    /*
     * Now that the segment is restored, copy it into the buffer pool
     * frame of each matching request.
     */
    requestMutex.acquire_write();

    shpid_t firstPage = getPidForSegment(segment);
    for (int i = 0; i < segmentSize; i++) {
        map<shpid_t, generic_page*>::iterator pos =
            bufferedRequests.find(firstPage + i);

        if (pos != bufferedRequests.end()) {
            char* wpage = workspace + (sizeof(generic_page) * i);

            memcpy(pos->second, wpage, sizeof(generic_page));
            bufferedRequests.erase(pos);
        }
    }

    // Mark whole segment as restored, even if no page was actually replayed
    // (i.e., segment contains only unused pages)
    numRestoredPages += segmentSize;
    if (numRestoredPages >= numPages) {
        numRestoredPages = numPages;
    }

    // to avoid race conditions with incoming requests,
    // mark segment as restored while holding the mutex
    bitmap->set(segment);

    // TODO generate log records

    requestMutex.release_write();

    // send signal to waiting threads (acquire mutex to avoid lost signal)
    DO_PTHREAD(pthread_mutex_lock(&restoreCondMutex));
    DO_PTHREAD(pthread_cond_broadcast(&restoreCond));
    DO_PTHREAD(pthread_mutex_unlock(&restoreCondMutex));

    if (count > 0) {
        w_assert1((int) count <= segmentSize);
        // write pages back to replacement device
        // TODO: replacement device must be properly initialized
        W_COERCE(volume->write_many_pages(firstPage, (generic_page*) workspace,
                    count));
    }
}

void RestoreMgr::run()
{
    // for now, restore thread only runs restore loop
    restoreLoop();
}
