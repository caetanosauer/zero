/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define VOL_C

#include <boost/concept_check.hpp>
#include "sm_base.h"
#include "stnode_page.h"
#include "vol.h"
#include "log_core.h"
#include "sm_options.h"

#include "alloc_cache.h"
#include "restore.h"
#include "logarchiver.h"
#include "restart.h"
#include "xct_logger.h"

// files and stuff
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "sm.h"

// TODO proper exception mechanism
#define CHECK_ERRNO(n) \
    if (n == -1) { \
        W_FATAL_MSG(fcOS, << "Kernel errno code: " << errno); \
    }

/*
 * replacement for solaris gethrtime(), which is based in any case
 * on this clock:
 */
int64_t gethrtime()
{
    struct timespec tsp;
    long e = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tsp);
    w_assert0(e == 0);
    // tsp.tv_sec is time_t
    return (tsp.tv_sec * 1000* 1000 * 1000) + tsp.tv_nsec; // nanosecs
}

vol_t::vol_t(const sm_options& options, chkpt_t* chkpt_info)
             : _fd(-1),
               _apply_fake_disk_latency(false),
               _fake_disk_latency(0),
               _alloc_cache(NULL), _stnode_cache(NULL),
               _failed(false),
               _restore_mgr(NULL), _dirty_pages(NULL), _backup_fd(-1),
               _current_backup_lsn(lsn_t::null), _backup_write_fd(-1),
               _log_page_reads(false), _prioritize_archive(true)
{
    string dbfile = options.get_string_option("sm_dbfile", "db");
    bool truncate = options.get_bool_option("sm_format", false);
    _log_page_reads = options.get_bool_option("sm_vol_log_reads", false);
    _use_o_sync = options.get_bool_option("sm_vol_o_sync", false);
    _use_o_direct = options.get_bool_option("sm_vol_o_direct", false);
    _readonly = options.get_bool_option("sm_vol_readonly", false);
    _prioritize_archive =
        options.get_bool_option("sm_recovery_prioritize_archive", false);
    _cluster_stores = options.get_bool_option("sm_vol_cluster_stores", false);

    _no_db_mode = options.get_bool_option("sm_no_db", false);
    if (_no_db_mode) {
        _readonly = true;
    }

    int open_flags = 0;
    open_flags |= _readonly ? O_RDONLY : O_RDWR;
    if (truncate) { open_flags |= O_TRUNC | O_CREAT; }
    if(_use_o_sync) { open_flags |= O_SYNC; }
    if(_use_o_direct) { open_flags |= O_DIRECT; }

    auto fd = open(dbfile.c_str(), open_flags, 0666 /*mode*/);
    CHECK_ERRNO(fd);
    _fd = fd;

    bool instantRestart = options.get_bool_option("sm_restart_instant", true);
    if (chkpt_info) {
        // If not instant restart, do not use dirty page table, which disables REDO
        // recovery based on SPR so that it is done explicitly by restart_thread_t.
        if (instantRestart) {
            _dirty_pages = new buf_tab_t(chkpt_info->buf_tab);
        }
        if (!chkpt_info->bkp_path.empty()) {
            sx_add_backup(chkpt_info->bkp_path, true);
        }
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

    w_assert1(_fd == -1);
    w_assert1(_backup_fd == -1);
    if (_restore_mgr) {
        delete _restore_mgr;
    }

    if (_dirty_pages) {
        delete _dirty_pages;
    }
}

void vol_t::sync()
{
    auto ret = fsync(_fd);
    CHECK_ERRNO(ret);
}

void vol_t::build_caches(bool truncate, chkpt_t* chkpt_info)
{
    _stnode_cache = new stnode_cache_t(truncate);
    w_assert1(_stnode_cache);
    _stnode_cache->dump(cerr);

    _alloc_cache = new alloc_cache_t(*_stnode_cache, truncate, _cluster_stores);
    w_assert1(_alloc_cache);

    // kick out pre-failure restore
    // (unless in nodb mode, where restore_segment log records are generated
    // during buffer pool warmup)
    if (!_no_db_mode && chkpt_info && chkpt_info->ongoing_restore) {
        mark_failed(false, true, chkpt_info->restore_page_cnt);
        _restore_mgr->markRestoredFromList(chkpt_info->restore_tab);
        _restore_mgr->start();
    }
}

lsn_t vol_t::get_dirty_page_emlsn(PageID pid) const
{
    if (!_dirty_pages) { return lsn_t::null; }

    spinlock_read_critical_section cs(&_mutex);

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

bool vol_t::grab_a_dirty_page(PageID& pid) const
{
    if (!_dirty_pages) { return false; }

    spinlock_read_critical_section cs(&_mutex);
    if (_dirty_pages->size() == 0) {
        return false;
    }

    auto it = _dirty_pages->cbegin();
    pid = it->first;
    return true;

}

PageID vol_t::get_dirty_page_count() const
{
    if (!_dirty_pages) { return 0; }

    spinlock_read_critical_section cs(&_mutex);
    return _dirty_pages->size();
}

void vol_t::checkpoint_dirty_pages(chkpt_t& chkpt) const
{
    if (!_dirty_pages) { return; }

    spinlock_read_critical_section cs(&_mutex);
    for (auto e : *_dirty_pages) {
        chkpt.mark_page_dirty(e.first, e.second.page_lsn, e.second.rec_lsn);
    }
}

void vol_t::open_backup()
{
    // mutex held by caller -- no concurrent backup being added
    string backupFile = _backups.back();
    // Using direct I/O
    int open_flags = O_RDONLY | O_SYNC;
    if (_use_o_direct) { open_flags |= O_DIRECT; }

    auto fd = open(backupFile.c_str(), open_flags, 0666 /*mode*/);
    CHECK_ERRNO(fd);
    _backup_fd = fd;
    _current_backup_lsn = _backup_lsns.back();
}

lsn_t vol_t::get_backup_lsn()
{
    spinlock_read_critical_section cs(&_mutex);
    return _current_backup_lsn;
}

rc_t vol_t::mark_failed(bool /*evict*/, bool redo, PageID lastUsedPid)
{
    spinlock_write_critical_section cs(&_mutex);

    if (_failed) {
        // failure-upon-failure -- just destroy current stat so we can start restore anew
        _restore_mgr->shutdown();
        delete _restore_mgr;
        _restore_mgr = nullptr;
        _failed = false;
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
        open_backup();
    }

    if (!ss_m::logArchiver) {
        throw runtime_error("Cannot simulate restore with mark_failed \
                without a running log archiver");
    }

    if (lastUsedPid == 0) {
        lastUsedPid = num_used_pages();
    }

    _restore_mgr = new RestoreMgr(ss_m::get_options(),
            ss_m::logArchiver->getIndex(), this, lastUsedPid, useBackup);

    _failed = true;

    lsn_t failureLSN = lsn_t::null;
    if (!redo) {
        // Create and insert logrec manually to get its LSN
        failureLSN = Logger::log_sys<restore_begin_log>(lastUsedPid);
    }

    _restore_mgr->setFailureLSN(failureLSN);
    if (!redo) { _restore_mgr->start(); }

    return RCOK;
}

bool vol_t::check_restore_finished()
{
    // with a read latch, check if finished -- most likely no
    {
        spinlock_read_critical_section cs(&_mutex);
        if (!_failed) { return true; }
        if (!_restore_mgr) { return true; }
        if (!_restore_mgr->all_pages_restored()) { return false; }
    }
    // restore finished -- update status with write latch
    {
        spinlock_write_critical_section cs(&_mutex);
        // check again for race
        if (!_failed) { return true; }

        // close restore manager
        if (_restore_mgr->try_shutdown()) {
            // join should be immediate, since thread is not running
            delete _restore_mgr;
            _restore_mgr = NULL;

            // close backup file
            if (_backup_fd > 0) {
                auto ret = close(_backup_fd);
                CHECK_ERRNO(ret);
                _backup_fd = -1;
                _current_backup_lsn = lsn_t::null;
            }

            _failed = false;
            return true;
        }
    }

    return false;
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

rc_t vol_t::sx_add_backup(const string& path, lsn_t backupLSN, bool redo)
{
    spinlock_write_critical_section cs(&_mutex);

    _backups.push_back(path);
    _backup_lsns.push_back(backupLSN);
    w_assert1(_backups.size() == _backup_lsns.size());

    if (!redo) {
        sys_xct_section_t ssx(true);
        Logger::log_sys<add_backup_log>(path, backupLSN);
        W_DO(ssx.end_sys_xct(RCOK));
    }

    return RCOK;
}

void vol_t::shutdown()
{
    spinlock_write_critical_section cs(&_mutex);

    DBG(<<" vol_t::dismount flush=" << flush);

    // INC_TSTAT(vol_cache_clears);

    w_assert1(_fd >= 0);

    auto ret = close(_fd);
    CHECK_ERRNO(ret);
    _fd = -1;
}

void vol_t::finish_restore()
{
    if (_failed) {
        _restore_mgr->shutdown();
        if (_backup_fd > 0) {
            auto ret = close(_backup_fd);
            CHECK_ERRNO(ret);
            _backup_fd = -1;
            _current_backup_lsn = lsn_t::null;
        }
        _failed = false;
    }
}

rc_t vol_t::alloc_a_page(PageID& shpid, StoreID stid)
{
    if (!_cluster_stores) { stid = 0; }
    w_assert1(_alloc_cache);
    W_DO(_alloc_cache->sx_allocate_page(shpid, stid));
    INC_TSTAT(page_alloc_cnt);

    return RCOK;
}

rc_t vol_t::deallocate_page(const PageID& pid)
{
    w_assert1(_alloc_cache);
    W_DO(_alloc_cache->sx_deallocate_page(pid));
    INC_TSTAT(page_dealloc_cnt);

    return RCOK;
}

size_t vol_t::num_used_pages() const
{
    return _alloc_cache->get_last_allocated_pid() + 1;
}

size_t vol_t::get_num_to_restore_pages() const
{
    if (!_restore_mgr) {
        return _alloc_cache->get_last_allocated_pid();
    }
    return _restore_mgr->getLastUsedPid();
}

size_t vol_t::get_num_restored_pages() const
{
    if (!_restore_mgr) {
        return _alloc_cache->get_last_allocated_pid();
    }
    return _restore_mgr->getNumRestoredPages();
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

rc_t vol_t::read_page_verify(PageID pid, generic_page* const buf, lsn_t emlsn)
{
    if (!_no_db_mode) {
        W_DO(read_many_pages(pid, buf, 1));
    }
    else {
        memset(buf, '\0', sizeof(generic_page));
    }

    // check for more recent LSN in dirty page table
    lsn_t dirty_lsn = get_dirty_page_emlsn(pid);
    if (dirty_lsn > emlsn) { emlsn = dirty_lsn; }

    // CS TODO: ignoring page corruption for now
    // uint32_t checksum = buf->calculate_checksum();
    // if (checksum != buf->checksum && !emlsn.is_null())

    if (buf->lsn < emlsn || _no_db_mode) {
        // if (buf->lsn == lsn_t::null) { // virgin page
        //     buf->lsn = lsn_t::null;
        //     buf->pid = pid;
        //     buf->tag = t_btree_p;
        // }

        btree_page_h p;
        buf->pid = pid;
        p.fix_nonbufferpool_page(buf);
        p.update_page_lsn(buf->lsn);

        SprIterator iter {pid, p.lsn(), emlsn, _prioritize_archive};
        iter.apply(p);
        w_assert0(_no_db_mode || p.lsn() == emlsn);
        w_assert0(!_no_db_mode || p.lsn() >= emlsn);
        w_assert0(!p.lsn().is_null());
        // cerr << "Recovered " << pid << " to LSN " << emlsn << endl;
    }

    if (!dirty_lsn.is_null()) {
        delete_dirty_page(pid);
    }

    // if (buf->pid != pid) {
    //     W_FATAL_MSG(eINTERNAL, <<"inconsistent disk page: "
    //         << pid << " was " << buf->pid);
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
    ADD_TSTAT(vol_reads, cnt);


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

    while (_failed) { // unsafe read at first -- latch acquired to verify it below
        if(ignoreRestore) {
            // volume is failed, but we don't want to restore
            return RC(eVOLFAILED);
        }

        {
            // Pin avoids restore mgr being destructed while we access it.
            // If it returns false, then restore manager was terminated,
            // which implies that restore process is done and we can safely
            // read the volume
            spinlock_read_critical_section cs(&_mutex);
            if (!_failed) { break; }
            if (!_restore_mgr->pin()) { break; }
        }

        // volume is failed, but we want restore to take place
        int i = 0;
        bool success = false;
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
                success = _restore_mgr->waitUntilRestored(first_page + i);
                if (!success) { break; }
                w_assert1(_restore_mgr->isRestored(first_page));
                if (reqSucceeded) {
                    // page is loaded in buffer pool already
                    w_assert1(buf->pid == first_page + i);
                    if (_log_page_reads) {
                        Logger::log_sys<page_read_log>(first_page + i, 1);
                    }
                    _restore_mgr->unpin();
                    return RCOK;
                }
            }
            i++;
        }
        _restore_mgr->unpin();
        if (success) { break; }
        else { check_restore_finished(); }
    }

    w_assert1(cnt > 0);
    size_t offset = size_t(first_page) * sizeof(generic_page);
    memset(buf, '\0', cnt * sizeof(generic_page));
    int read_count = pread(_fd, (char *) buf, cnt * sizeof(generic_page), offset);
    CHECK_ERRNO(read_count);

    if (_log_page_reads) {
        Logger::log_sys<page_read_log>(first_page, cnt);
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

    int read_count = pread(_backup_fd, (char *) buf, count * sizeof(generic_page), offset);
    CHECK_ERRNO(read_count);

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
        int flags = O_SYNC | O_WRONLY | O_TRUNC | O_CREAT;
        auto fd = open(path.c_str(), flags, 0666 /*mode*/);
        CHECK_ERRNO(fd);
        _backup_write_fd = fd;

        useBackup = _backups.size() > 0;

        if (useBackup && _backup_fd < 0) {
            // no ongoing restore -- we must open old backup ourselves
            open_backup();
        }
    }

    // No need to hold latch here -- mutual exclusion is guaranteed because
    // only one thread may set _backup_write_fd (i.e., open file) above.

    // Maximum LSN which is guaranteed to be reflected in the backup
    lsn_t backupLSN = ss_m::logArchiver->getIndex()->getLastLSN();
    DBG1(<< "Taking backup until LSN " << backupLSN);

    // Instantiate special restore manager for taking backup
    RestoreMgr restore(ss_m::get_options(), ss_m::logArchiver->getIndex(),
            this, useBackup, true /* takeBackup */);

    restore.setInstant(false);
    if (flushArchive) {
        lsn_t currLSN = smlevel_0::log->durable_lsn();
        restore.setFailureLSN(currLSN);
        DBGTHRD(<< "Taking sharp backup until " << currLSN);
        backupLSN = currLSN;
    }

    restore.start();
    restore.shutdown();
    // TODO -- do we have to catch errors from restore thread?

    // Write volume header and metadata to new backup
    // (must be done after restore so that alloc pages are correct)
    // CS TODO
    // volhdr_t vhdr(_vid, _num_pages, backupLSN);
    // W_DO(vhdr.write(_backup_write_fd));

    // At this point, new backup is fully written
    W_DO(sx_add_backup(path, backupLSN));
    {
        // critical section to guarantee visibility of the fd update
        spinlock_write_critical_section cs(&_mutex);
        auto ret = close(_backup_write_fd);
        CHECK_ERRNO(ret);
        _backup_write_fd = -1;
    }

    DBG1(<< "Finished taking backup");

    return RCOK;
}

rc_t vol_t::write_backup(PageID first, size_t count, void* buf)
{
    w_assert0(_backup_write_fd > 0);
    w_assert1(count > 0);
    size_t offset = size_t(first) * sizeof(generic_page);

    auto ret = pwrite(_backup_write_fd, buf, sizeof(generic_page) * count, offset);
    CHECK_ERRNO(ret);

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

    if (_failed && !ignoreRestore) {
        check_restore_finished();
    }

    w_assert1(cnt > 0);
    size_t offset = size_t(first_page) * sizeof(generic_page);

    long start = 0;
    if(_apply_fake_disk_latency) start = gethrtime();

#if W_DEBUG_LEVEL>0
    for (int i = 0; i < cnt; i++) {
        w_assert1(buf[i].pid == first_page + i);
    }
#endif

    // do the actual write now
    auto ret = pwrite(_fd, buf, sizeof(generic_page)*cnt, offset);
    CHECK_ERRNO(ret);

    fake_disk_latency(start);
    ADD_TSTAT(vol_blks_written, cnt);
    INC_TSTAT(vol_writes);

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
