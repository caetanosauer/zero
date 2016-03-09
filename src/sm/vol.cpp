/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define VOL_C

#include "w_stream.h"
#include <sys/types.h>
#include <boost/concept_check.hpp>
#include "sm_base.h"
#include "stnode_page.h"
#include "vol.h"
#include "log_core.h"
#include "sm_options.h"

#include "alloc_cache.h"
#include "bf_tree.h"
#include "restore.h"
#include "logarchiver.h"
#include "eventlog.h"
#include "restart.h"

#include "sm.h"

// Needed to get LSN of restore_begin log record
#include "logdef_gen.cpp"

vol_t::vol_t(const sm_options& options, chkpt_t* chkpt_info)
             : _unix_fd(-1),
               _apply_fake_disk_latency(false),
               _fake_disk_latency(0),
               _alloc_cache(NULL), _stnode_cache(NULL),
               _failed(false),
               _restore_mgr(NULL), _dirty_pages(NULL), _backup_fd(-1),
               _current_backup_lsn(lsn_t::null), _backup_write_fd(-1),
               _log_page_reads(false), _log_page_writes(false)
{
    string dbfile = options.get_string_option("sm_dbfile", "db");
    bool truncate = options.get_bool_option("sm_format", false);
    _readonly = options.get_bool_option("sm_vol_readonly", false);
    _log_page_reads = options.get_bool_option("sm_vol_log_reads", false);
    _log_page_writes = options.get_bool_option("sm_vol_log_writes", true);
    _use_o_direct = options.get_bool_option("sm_vol_o_direct", false);

    spinlock_write_critical_section cs(&_mutex);

    // CS TODO: do we need/want OPEN_SYNC?
    int open_flags = smthread_t::OPEN_SYNC | smthread_t::OPEN_DIRECT;
    open_flags |= _readonly ? smthread_t::OPEN_RDONLY : smthread_t::OPEN_RDWR;
    if (truncate) {
        open_flags |= smthread_t::OPEN_TRUNC | smthread_t::OPEN_CREATE;
    }

    W_COERCE(me()->open(dbfile.c_str(), open_flags, 0666, _unix_fd));

    if (chkpt_info) {
        _dirty_pages = new buf_tab_t(chkpt_info->buf_tab);
    }
}

vol_t::~vol_t()
{
    if (_alloc_cache) {
        delete _alloc_cache;
        _alloc_cache = NULL;
    }
    if (_stnode_cache) {
        delete _stnode_cache;
        _stnode_cache = NULL;
    }

    w_assert1(_unix_fd == -1);
    w_assert1(_backup_fd == -1);
    if (_restore_mgr) {
        delete _restore_mgr;
    }
}

rc_t vol_t::sync()
{
    W_DO(me()->fsync(_unix_fd));
    return RCOK;
}

void vol_t::build_caches(bool truncate)
{
    _stnode_cache = new stnode_cache_t(truncate);
    w_assert1(_stnode_cache);
    _stnode_cache->dump(cerr);

    _alloc_cache = new alloc_cache_t(*_stnode_cache, truncate);
    w_assert1(_alloc_cache);
}

lsn_t vol_t::get_dirty_page_emlsn(PageID pid) const
{
    if (!_dirty_pages) { return lsn_t::null; }

    buf_tab_t::const_iterator it = _dirty_pages->find(pid);
    if (it == _dirty_pages->end()) { return lsn_t::null; }
    return it->second.page_lsn;
}

void vol_t::delete_dirty_page(PageID pid)
{
    if (!_dirty_pages) { return; }

    spinlock_write_critical_section cs(&_mutex);

    buf_tab_t::iterator it = _dirty_pages->find(pid);
    if (it != _dirty_pages->end()) {
        _dirty_pages->erase(it);
    }
}

rc_t vol_t::open_backup()
{
    // mutex held by caller -- no concurrent backup being added
    string backupFile = _backups.back();
    // Using direct I/O
    int open_flags = smthread_t::OPEN_RDONLY | smthread_t::OPEN_SYNC;
    if (_use_o_direct) {
        open_flags |= smthread_t::OPEN_DIRECT;
    }
    W_DO(me()->open(backupFile.c_str(), open_flags, 0666, _backup_fd));
    w_assert0(_backup_fd > 0);
    _current_backup_lsn = _backup_lsns.back();

    return RCOK;
}

lsn_t vol_t::get_backup_lsn()
{
    spinlock_read_critical_section cs(&_mutex);
    return _current_backup_lsn;
}

rc_t vol_t::mark_failed(bool /*evict*/, bool redo)
{
    spinlock_write_critical_section cs(&_mutex);

    if (is_failed()) {
        return RCOK;
    }

    bool useBackup = _backups.size() > 0;

    /*
     * The order of operations in this method is crucial. We may only set
     * failed after the restore manager is created, otherwise read/write
     * operations will find a null restore manager and thus will not be able to
     * wait for restore. Generating a failure LSN must occur after we've set
     * the failed flag, because we must guarantee that no read or write
     * occurred after the failure LSN (restore_begin log record).  Finally, the
     * restore manager may only be forked once the failure LSN has been set,
     * lsn_t::null, which is why we cannot pass the failureLSN in the
     * constructor.
     */

    // open backup file -- may already be open due to new backup being taken
    if (useBackup && _backup_fd < 0) {
        W_DO(open_backup());
    }

    if (!ss_m::logArchiver) {
        throw runtime_error("Cannot simulate restore with mark_failed \
                without a running log archiver");
    }

    _restore_mgr = new RestoreMgr(ss_m::get_options(),
            ss_m::logArchiver->getDirectory(), this, useBackup);

    set_failed(true);

    lsn_t failureLSN = lsn_t::null;
    if (!redo) {
        // Create and insert logrec manually to get its LSN
        new (_logrec_buf) restore_begin_log();
        W_DO(ss_m::log->insert(*((logrec_t*) _logrec_buf), &failureLSN));
        W_DO(ss_m::log->flush(failureLSN));
    }

    _restore_mgr->setFailureLSN(failureLSN);
    _restore_mgr->fork();

    return RCOK;
}

void vol_t::chkpt_restore_progress(chkpt_restore_tab_t* tab)
{
    w_assert0(tab);
    spinlock_read_critical_section cs(&_mutex);

    if (!is_failed()) {
        return;
    }
    w_assert0(_restore_mgr);

    RestoreBitmap* bitmap = _restore_mgr->getBitmap();
    size_t bitmapSize = bitmap->getSize();
    size_t firstNotRestored = 0;
    size_t lastRestored = 0;
    bitmap->getBoundaries(firstNotRestored, lastRestored);
    tab->firstNotRestored = firstNotRestored;
    tab->bitmapSize = bitmapSize - firstNotRestored;

    if (tab->bitmapSize > chkpt_restore_tab_t::maxSegments) {
        W_FATAL_MSG(eINTERNAL,
                << "RestoreBitmap of " << bitmapSize
                << " does not fit in checkpoint");
    }

    if (firstNotRestored < lastRestored) {
        // "to" is exclusive boundary
        bitmap->serialize(tab->bitmap, firstNotRestored, lastRestored + 1);
    }
}

bool vol_t::check_restore_finished()
{
    if (!is_failed()) {
        return true;
    }

    // with a read latch, check if finished -- most likely no
    {
        spinlock_read_critical_section cs(&_mutex);
        if (!is_failed()) {
            return true;
        }
        if (!_restore_mgr->finished()) {
            return false;
        }
    }
    // restore finished -- update status with write latch
    {
        spinlock_write_critical_section cs(&_mutex);
        // check again for race
        if (!is_failed()) {
            return true;
        }

        // close restore manager
        if (_restore_mgr->try_shutdown()) {
            // join should be immediate, since thread is not running
            _restore_mgr->join();
            delete _restore_mgr;
            _restore_mgr = NULL;

            // close backup file
            if (_backup_fd > 0) {
                W_COERCE(me()->close(_backup_fd));
                _backup_fd = -1;
                _current_backup_lsn = lsn_t::null;
            }

            set_failed(false);
            return true;
        }
    }

    return false;
}

void vol_t::redo_segment_restore(unsigned segment)
{
    w_assert0(_restore_mgr && is_failed());
    _restore_mgr->markSegmentRestored(segment, true /* redo */);
}

rc_t vol_t::dismount(bool abrupt)
{
    spinlock_write_critical_section cs(&_mutex);

    DBG(<<" vol_t::dismount flush=" << flush);

    INC_TSTAT(vol_cache_clears);

    w_assert1(_unix_fd >= 0);

    if (!abrupt) {
        if (is_failed()) {
            // wait for ongoing restore to complete
            _restore_mgr->setSinglePass();
            _restore_mgr->join();
            if (_backup_fd > 0) {
                W_COERCE(me()->close(_backup_fd));
                _backup_fd = -1;
                _current_backup_lsn = lsn_t::null;
            }
            set_failed(false);
        }
        // CS TODO -- also make sure no restart is ongoing
    }
    else if (is_failed()) {
        DBG(<< "WARNING: Volume shutting down abruptly during restore!");
    }

    W_DO(me()->close(_unix_fd));
    _unix_fd = -1;

    return RCOK;
}

unsigned vol_t::num_backups() const
{
    spinlock_read_critical_section cs(&_mutex);
    return _backups.size();
}

void vol_t::list_backups(
    std::vector<string>& backups)
{
    spinlock_read_critical_section cs(&_mutex);

    for (size_t k = 0; k < _backups.size(); k++) {
        backups.push_back(_backups[k]);
    }
}

rc_t vol_t::sx_add_backup(string path, bool redo)
{
    // Make sure backup volume header matches this volume
    stnode_page stpage;
    {
        int fd = -1;
        int open_flags = smthread_t::OPEN_RDWR | smthread_t::OPEN_SYNC;
        rc_t rc = me()->open(path.c_str(), open_flags, 0666, fd);
        if (rc.is_error())  {
            W_IGNORE(me()->close(fd));
            return RC_AUGMENT(rc);
        }
        rc = me()->read(fd, &stpage, sizeof(generic_page));
        if (rc.is_error())  {
            W_IGNORE(me()->close(fd));
            return RC_AUGMENT(rc);
        }

        W_DO(me()->close(fd));
    }

    lsn_t backupLSN = stpage.getBackupLSN();

    // will change vol_t state -- start critical section
    // Multiple adds of the same backup file are weird, but not an error.
    // The mutex is just ot protect against mounts and checkpoints
    spinlock_write_critical_section cs(&_mutex);

    _backups.push_back(path);
    _backup_lsns.push_back(backupLSN);
    w_assert1(_backups.size() == _backup_lsns.size());

    if (!redo) {
        sys_xct_section_t ssx(true);
        W_DO(log_add_backup(path.c_str()));
        W_DO(ssx.end_sys_xct(RCOK));
    }

    return RCOK;
}

void vol_t::shutdown(bool abrupt)
{
    // bf_uninstall causes a force on the volume through the bf_cleaner
    W_COERCE(dismount(abrupt));
}

rc_t vol_t::alloc_a_page(PageID& shpid, bool redo)
{
    w_assert1(_alloc_cache);
    W_DO(_alloc_cache->sx_allocate_page(shpid, redo));
    INC_TSTAT(page_alloc_cnt);

    return RCOK;
}

rc_t vol_t::deallocate_page(const PageID& pid, bool redo)
{
    w_assert1(_alloc_cache);
    W_DO(_alloc_cache->sx_deallocate_page(pid, redo));
    INC_TSTAT(page_dealloc_cnt);

    return RCOK;
}

size_t vol_t::num_used_pages() const
{
    return _alloc_cache->get_last_allocated_pid();
}

rc_t vol_t::create_store(PageID& root_pid, StoreID& snum)
{
    W_DO(_alloc_cache->sx_allocate_page(root_pid));
    W_DO(_stnode_cache->sx_create_store(root_pid, snum));
    return RCOK;
}

bool vol_t::is_alloc_store(StoreID f) const
{
    return _stnode_cache->is_allocated(f);
}

PageID vol_t::get_store_root(StoreID f) const
{
    return _stnode_cache->get_root_pid(f);
}

void vol_t::fake_disk_latency(long start)
{
    if(!_apply_fake_disk_latency)
        return;
    long delta = gethrtime() - start;
    delta = _fake_disk_latency - delta;
    if(delta <= 0)
        return;
    int max= 99999999;
    if(delta > max) delta = max;

    struct timespec req, rem;
    req.tv_sec = 0;
    w_assert0(delta > 0);
    w_assert0(delta <= max);
    req.tv_nsec = delta;
    while(nanosleep(&req, &rem) != 0)
    {
        if (errno != EINTR)  return;
        req = rem;
    }
}

void vol_t::enable_fake_disk_latency(void)
{
    spinlock_write_critical_section cs(&_mutex);
    _apply_fake_disk_latency = true;
}

void vol_t::disable_fake_disk_latency(void)
{
    spinlock_write_critical_section cs(&_mutex);
    _apply_fake_disk_latency = false;
}

bool vol_t::set_fake_disk_latency(const int adelay)
{
    spinlock_write_critical_section cs(&_mutex);
    if (adelay<0) {
        return (false);
    }
    _fake_disk_latency = adelay;
    return (true);
}

/*********************************************************************
 *
 *  vol_t::read_page(pnum, page)
 *
 *  Read the page at "pnum" of the volume to the buffer "page".
 *
 *********************************************************************/
rc_t vol_t::read_page(PageID pnum, generic_page* const buf)
{
    return read_many_pages(pnum, buf, 1);
}

rc_t vol_t::read_page_verify(PageID pnum, generic_page* const buf, lsn_t emlsn)
{
    W_DO(read_many_pages(pnum, buf, 1));

    // check for more recent LSN in dirty page table
    lsn_t dirty_lsn = get_dirty_page_emlsn(pnum);
    if (dirty_lsn > emlsn) { emlsn = dirty_lsn; }

    // CS TODO: ignoring page corruption for now
    // uint32_t checksum = buf->calculate_checksum();
    // if (checksum != buf->checksum && !emlsn.is_null())

    if (buf->lsn < emlsn) {
        if (buf->pid == 0) { // virgin page
            buf->lsn = lsn_t::null;
            buf->pid = pnum;
            buf->tag = t_btree_p;
        }

        btree_page_h p;
        p.fix_nonbufferpool_page(buf);
        W_DO(smlevel_0::recovery->recover_single_page(p, emlsn));
        delete_dirty_page(pnum);
        // cerr << "Recovered " << pnum << " to LSN " << emlsn << endl;
    }

    // if (buf->pid != pnum) {
    //     W_FATAL_MSG(eINTERNAL, <<"inconsistent disk page: "
    //         << pnum << " was " << buf->pid);
    // }

    return RCOK;
}

/*********************************************************************
 *
 *  vol_t::read_many_pages(first_page, buf, cnt)
 *
 *  Read "cnt" buffers in "buf" from pages starting at "first_page"
 *  of the volume.
 *
 *********************************************************************/
rc_t vol_t::read_many_pages(PageID first_page, generic_page* const buf, int cnt,
        bool ignoreRestore)
{
    DBG(<< "Page read: from " << first_page << " to " << first_page + cnt);

    #ifdef ZERO_INIT
    /*
     * When a write into the buffer pool of potentially uninitialized
     * memory occurs (such as padding)
     * there is a purify/valgrind supression to keep the SM from being gigged
     * for the SM-using application's legitimate behavior.  However, this
     * uninitialized memory writes to a page in the buffer pool
     * colors the corresponding bytes in the buffer pool with the
     * "uninitialized" memory color.  When a new page is read in from
     * disk, nothing changes the color of the page back to "initialized",
     * and you suddenly see UMR or UMC errors from valid buffer pool pages.
     */
    memset(buf, '\0', cnt * sizeof(generic_page));
    #endif


    /*
     * CS: If volume is marked as failed, we must invoke restore manager and
     * wait until the requested page is restored. If we succeed in placing a
     * copy request, the page contents will be copied into &page, eliminating
     * the need for the actual read from the restored device.
     *
     * Note that we read from the same file descriptor after a failure. This is
     * because we currently just simulate device failures. To support real
     * media recovery, the code needs to detect I/O errors and remount the
     * volume into a new file descriptor for the replacement device. The logic
     * for restore, however, would remain the same.
     */

    while (is_failed()) {
        if(ignoreRestore) {
            // volume is failed, but we don't want to restore
            return RC(eVOLFAILED);
        }
        else {
            {
                // Pin avoids restore mgr being destructed while we access it.
                // If it returns false, then restore manager was terminated,
                // which implies that restore process is done and we can safely
                // read the volume
                spinlock_read_critical_section cs(&_mutex);
                if (!_restore_mgr->pin()) { break; }
            }

            // volume is failed, but we want restore to take place
            int i = 0;
            while(i < cnt) {
                if (!_restore_mgr->isRestored(first_page + i)) {
                    DBG(<< "Page read triggering restore of " << first_page + i);
                    bool reqSucceeded = false;
                    if(cnt == 1) {
                        reqSucceeded = _restore_mgr->requestRestore(first_page + i, buf);
                    }
                    else {
                        reqSucceeded = _restore_mgr->requestRestore(first_page + i, NULL);
                    }
                    _restore_mgr->waitUntilRestored(first_page + i);
                    w_assert1(_restore_mgr->isRestored(first_page));
                    if (reqSucceeded) {
                        // page is loaded in buffer pool already
                        w_assert1(buf->pid == first_page + i);
                        if (_log_page_reads) {
                            sysevent::log_page_read(first_page + i);
                        }
                        _restore_mgr->unpin();
                        return RCOK;
                    }
                }
                i++;
            }
            _restore_mgr->unpin();
            check_restore_finished();
            break;
        }
    }

    w_assert1(cnt > 0);
    size_t offset = size_t(first_page) * sizeof(generic_page);
    memset(buf, '\0', cnt * sizeof(generic_page));
    int read_count = 0;
    W_DO(me()->pread_short(_unix_fd, (char *) buf, cnt * sizeof(generic_page),
                offset, read_count));

    if (_log_page_reads) {
        sysevent::log_page_read(first_page, cnt);
    }

    return RCOK;
}

rc_t vol_t::read_backup(PageID first, size_t count, void* buf)
{
    if (_backup_fd < 0) {
        W_FATAL_MSG(eINTERNAL,
                << "Cannot read from backup because it is not active");
    }

    // adjust count to avoid short I/O
    if (first + count > num_used_pages()) {
        count = num_used_pages() - first;
    }

    size_t offset = size_t(first) * sizeof(generic_page);
    memset(buf, 0, sizeof(generic_page) * count);

    int read_count = 0;
    W_DO(me()->pread_short(_backup_fd, (char *) buf, count * sizeof(generic_page),
                offset, read_count));

    // Short I/O is still possible because backup is only taken until last used
    // page, i.e., the file may be smaller than the total quota.
    if (read_count < (int) count) {
        // Actual short I/O only happens if we are not reading past last page
        w_assert0(first + count <= num_used_pages());
    }

    // Here, unlike in read_page, virgin pages don't have to be zeroed, because
    // backups guarantee that the checksum matches for all valid (non-virgin)
    // pages. Thus a virgin page is actually *defined* as one for which the
    // checksum does not match. If the page is actually corrupted, then the
    // REDO logic will detect it, because the first log records replayed on
    // virgin pages must incur a format and allocation. If it tries to replay
    // any other kind of log record, then the page is corrupted.

    return RCOK;
}

rc_t vol_t::take_backup(string path, bool flushArchive)
{
    // Open old backup file, if available
    bool useBackup = false;
    {
        spinlock_write_critical_section cs(&_mutex);

        if (_backup_write_fd >= 0) {
            return RC(eBACKUPBUSY);
        }

        _backup_write_path = path;
        int flags = smthread_t::OPEN_SYNC | smthread_t::OPEN_WRONLY
            | smthread_t::OPEN_TRUNC | smthread_t::OPEN_CREATE;
        W_DO(me()->open(path.c_str(), flags, 0666, _backup_write_fd));

        useBackup = _backups.size() > 0;

        if (useBackup && _backup_fd < 0) {
            // no ongoing restore -- we must open old backup ourselves
            W_DO(open_backup());
        }
    }

    // No need to hold latch here -- mutual exclusion is guaranteed because
    // only one thread may set _backup_write_fd (i.e., open file) above.

    if (flushArchive) {
        LogArchiver* la = smlevel_0::logArchiver;
        W_DO(smlevel_0::log->flush_all());
        lsn_t currLSN = smlevel_0::log->curr_lsn();
        // wait for log record to be consumed
        while (la->getNextConsumedLSN() < currLSN) {
            ::usleep(10000); // 10ms
        }

        // Time to wait until requesting a log archive flush (msec). If we're
        // lucky, log is archiving very fast and a flush request is not needed.
        int waitBeforeFlush = 5000; // 5 sec
        ::usleep(waitBeforeFlush * 1000);

        DBGTHRD(<< "Taking sharp backup until " << currLSN);

        if (la->getDirectory()->getLastLSN() < currLSN) {
            la->requestFlushSync(currLSN);
        }
    }

    // Maximum LSN which is guaranteed to be reflected in the backup
    lsn_t backupLSN = ss_m::logArchiver->getDirectory()->getLastLSN();

    DBG1(<< "Taking backup until LSN " << backupLSN);

    // Instantiate special restore manager for taking backup
    RestoreMgr restore(ss_m::get_options(), ss_m::logArchiver->getDirectory(),
            this, useBackup, true /* takeBackup */);
    restore.setSinglePass(true);
    restore.setInstant(false);
    restore.fork();
    restore.join();
    // TODO -- do we have to catch errors from restore thread?

    // Write volume header and metadata to new backup
    // (must be done after restore so that alloc pages are correct)
    // CS TODO
    // volhdr_t vhdr(_vid, _num_pages, backupLSN);
    // W_DO(vhdr.write(_backup_write_fd));

    // At this point, new backup is fully written
    // add_backup(path, backupLSN);
    {
        // critical section to guarantee visibility of the fd update
        spinlock_write_critical_section cs(&_mutex);
        W_DO(me()->close(_backup_write_fd));
        _backup_write_fd = -1;
    }

    DBG1(<< "Finished taking backup");

    return RCOK;
}

rc_t vol_t::write_backup(PageID first, size_t count, void* buf)
{
    w_assert0(_backup_write_fd > 0);
    w_assert1(first + count <= (PageID) num_used_pages());
    w_assert1(count > 0);
    size_t offset = size_t(first) * sizeof(generic_page);

    W_DO(me()->pwrite(_backup_write_fd, buf, sizeof(generic_page) * count,
                offset));

    DBG(<< "Wrote out " << count << " pages into backup offset " << offset);

    return RCOK;
}


/*********************************************************************
 *
 *  vol_t::write_many_pages(pnum, pages, cnt)
 *
 *  Write "cnt" buffers in "pages" to pages starting at "pnum"
 *  of the volume.
 *
 *********************************************************************/
rc_t vol_t::write_many_pages(PageID first_page, const generic_page* const buf, int cnt,
        bool ignoreRestore)
{
    if (_readonly) {
        // Write elision!
        return RCOK;
    }

    /** CS: If volume has failed, writes are suspended until the area being
     * written to is fully restored. This is required to avoid newer versions
     * of a page (which are written from the buffer pool with this method call)
     * being overwritten by older restored versions. This situation could lead
     * to lost updates.
     *
     * During restore, the cleaner should ignore the failed volume, meaning
     * that its dirty pages should remain in the buffer pool. A better design
     * would be to either perform "attepmted" writes, i.e., returning some kind
     * of "not succeeded" message in this method; or integrate the cleaner with
     * the restore manager. Since we don't expect high transaction trhoughput
     * during restore (unless we have dozens of mounted, working volumes) and
     * typical workloads maintain a low dirty page ratio, this is not a concern
     * for now.
     */
    // For small buffer pools, the sytem can get stuck because of eviction waiting
    // for restore waiting for eviction.
    //
    // while (is_failed() && !ignoreRestore) {
    //     w_assert1(_restore_mgr);

    //     { // pin avoids restore mgr being destructed while we access it
    //         spinlock_read_critical_section cs(&_mutex);
    //         if (!_restore_mgr->pin()) { break; }
    //     }

    //     // For each segment involved in this bulk write, request and wait for
    //     // it to be restored. The order is irrelevant, since we have to wait
    //     // for all segments anyway.
    //     int i = 0;
    //     while (i < cnt) {
    //         _restore_mgr->requestRestore(pnum + i);
    //         _restore_mgr->waitUntilRestored(pnum + i);
    //         i += _restore_mgr->getSegmentSize();
    //     }

    //     _restore_mgr->unpin();
    //     check_restore_finished();
    //     break;
    // }

    if (is_failed() && !ignoreRestore) {
        check_restore_finished();
    }

    w_assert1(cnt > 0);
    size_t offset = size_t(first_page) * sizeof(generic_page);

    smthread_t* t = me();

    long start = 0;
    if(_apply_fake_disk_latency) start = gethrtime();

    // do the actual write now
    W_COERCE(t->pwrite(_unix_fd, buf, sizeof(generic_page)*cnt, offset));

    fake_disk_latency(start);
    ADD_TSTAT(vol_blks_written, cnt);
    INC_TSTAT(vol_writes);

    if (_log_page_writes) {
        sysevent::log_page_write(first_page, cnt);
    }

    return RCOK;
}

uint32_t vol_t::get_last_allocated_pid() const
{
    w_assert1(_alloc_cache);
    return _alloc_cache->get_last_allocated_pid();
}

bool vol_t::is_allocated_page(PageID pid) const
{
    w_assert1(_alloc_cache);
    return _alloc_cache->is_allocated(pid);
}

