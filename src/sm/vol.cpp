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
#include "sm_du_stats.h"
#include "sm_options.h"

#include "sm_vtable_enum.h"

#include "alloc_cache.h"
#include "bf_tree.h"
#include "restore.h"
#include "logarchiver.h"
#include "eventlog.h"

#include "sm.h"

// Needed to get LSN of restore_begin log record
#include "logdef_gen.cpp"

const int vol_m::MAX_VOLS;

vol_m::vol_m(const sm_options&)
    : vol_cnt(0), _next_vid(1)
{
    for (uint32_t i = 0; i < MAX_VOLS; i++)  {
        volumes[i] = NULL;
    }
}

vol_m::~vol_m()
{
}

/*
 * CS TODO -- index volume array by vid, since max vid is also MAX_VOLS
 */

void vol_m::shutdown(bool abrupt)
{
    spinlock_write_critical_section cs(&_mutex);

    for (int i = 0; i < MAX_VOLS; i++) {
        if (volumes[i]) {
            volumes[i]->shutdown(abrupt);
            delete volumes[i];
            volumes[i] = NULL;
        }
    }
}

vol_t* vol_m::get(const char* path)
{
    spinlock_read_critical_section cs(&_mutex);

    for (uint32_t i = 0; i < MAX_VOLS; i++)  {
        if (volumes[i] && strcmp(volumes[i]->devname(), path) == 0) {
            return volumes[i];
        }
    }
    return NULL;
}

vol_t* vol_m::get(vid_t vid)
{
    spinlock_read_critical_section cs(&_mutex);

    if (vid == vid_t(0)) return NULL;

    // vid N is mounted at index N-1
    return volumes[size_t(vid) - 1];
}

rc_t vol_m::sx_format(
    const char* devname,
    vid_t& vid,
    bool logit)
{
    spinlock_write_critical_section cs(&_mutex);


    sys_xct_section_t ssx(true);

    // CS TODO -- WHY LOG OFF???
    /*
     *  No log needed.
     *  WHOLE FUNCTION is a critical section
     */
    // xct_log_switch_t log_off(smlevel_0::OFF);

    if (_next_vid > MAX_VOLS) {
        W_RETURN_RC_MSG(eINTERNAL,
            << "Maximum number of volumes already reached: " << MAX_VOLS);
    }

    vid = _next_vid++;

    // CS TODO: remove support for multiple volumes
    w_assert0(vid == 1);

    DBG( << "formating volume " << devname << ">" );
    int flags = smthread_t::OPEN_CREATE | smthread_t::OPEN_RDWR
        | smthread_t::OPEN_TRUNC | smthread_t::OPEN_SYNC;
    int fd;
    W_DO(me()->open(devname, flags, 0666, fd));
    W_DO(vol_t::write_metadata(fd, vid));
    W_DO(me()->close(fd));
    DBG(<<"format_vol: done" );

    /*
     * In principle, logging volume formats should not be required, since we
     * force the folume header before returning from this method.  The only
     * reason why we log is because of the _next_vid variable, which is part of
     * persistent server state and therefore must be logged and checkpointed.
     */
    if (logit) {
        log_format_vol(devname, vid);
    }
    W_DO (ssx.end_sys_xct(RCOK));

    return RCOK;
}

rc_t vol_m::sx_mount(const char* device, const bool logit)
{
    spinlock_write_critical_section cs(&_mutex);

    /**
     * Mounts cannot occur while a checkpoint is being taken, because if the
     * checkpoint log record is generated before the volume is added to the
     * list of mounted volumes but after a mount log record was generated, then
     * the mount will not be replayed if there is a crash. However, instead of
     * acquiring the checkpoint mutex, we rely on the fact that checkpoints
     * call list_volumes, which acquire the volume spinlock as well. Therefore,
     * checkpoints and mounts/dismounts end up being implicitly serialized by
     * the volume spinklock.
     */

    sys_xct_section_t ssx (true);

    DBG( << "sx_mount(" << device << ")");

    vol_t* v = new vol_t();
    W_DO(v->mount(device));
    size_t index = v->vid() - 1;

    if (volumes[index]) {
        return RC(eALREADYMOUNTED);
    }

    volumes[index] = v;
    ++vol_cnt;

    if (logit)  {
        log_mount_vol(v->devname());
    }

    W_DO (ssx.end_sys_xct(RCOK));

    return RCOK;
}

rc_t vol_m::sx_dismount(const char* device, bool logit)
{
    DBG( << "_dismount(" << device << ")");
    vol_t* vol = get(device);
    w_assert0(vol);

    // lock after get() to avoid deadlock
    // checkpoints also serialized -- see comment on sx_mount
    spinlock_write_critical_section cs(&_mutex);

    sys_xct_section_t ssx (true);

    W_DO(vol->dismount());

    for (int i = 0; i < MAX_VOLS; i++) {
        if (volumes[i] == vol) {
            delete volumes[i];
            volumes[i] = 0;
        }
    }
    --vol_cnt;

    if (logit) {
        log_dismount_vol(device);
    }

    W_DO (ssx.end_sys_xct(RCOK));

    DBG( << "_dismount done.");
    return RCOK;
}

rc_t vol_m::sx_add_backup(vid_t vid, string path, bool logit)
{
    vol_t* vol = get(vid);
    w_assert0(vol);

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

    // mount/dismount may occur before we acquire mutex
    if (vol != volumes[size_t(vid) - 1]) {
        W_RETURN_RC_MSG(eRETRY, << "Volume changed while adding backup file");
    }

    if (logit) {
        sys_xct_section_t ssx(true);
        W_DO(log_add_backup(vid, path.c_str()));
        W_DO(ssx.end_sys_xct(RCOK));
    }

    vol->add_backup(path, backupLSN);

    return RCOK;
}

rc_t vol_m::list_volumes(
    std::vector<string>& names,
    std::vector<vid_t>& vids,
    size_t start,
    size_t count)
{
    spinlock_read_critical_section cs(&_mutex);

    w_assert0(names.size() == vids.size());

    if (count == 0) {
        count = MAX_VOLS;
    }

    for (size_t i = 0, j = start; i < MAX_VOLS && j < count; i++)  {
        if (volumes[i])  {
            vids.push_back(volumes[i]->vid());
            names.push_back(volumes[i]->devname());
            j++;
        }
    }
    return RCOK;
}

rc_t vol_m::list_backups(
    std::vector<string>& backups,
    std::vector<vid_t>& vids,
    size_t start,
    size_t count)
{
    spinlock_read_critical_section cs(&_mutex);

    w_assert0(backups.size() == vids.size());

    if (count == 0) {
        count = MAX_VOLS;
    }

    for (size_t i = 0, j = start; i < MAX_VOLS && j < count; i++)  {
        if (volumes[i])  {
            for (size_t k = 0; k < volumes[i]->_backups.size(); k++) {
                vids.push_back(volumes[i]->vid());
                backups.push_back(volumes[i]->_backups[k]);
                j++;
            }
        }
    }
    return RCOK;
}

vol_t::vol_t()
             : _unix_fd(-1),
               _apply_fake_disk_latency(false),
               _fake_disk_latency(0),
               _alloc_cache(NULL), _stnode_cache(NULL),
               _failed(false), _readonly(false),
               _restore_mgr(NULL), _backup_fd(-1),
               _current_backup_lsn(lsn_t::null), _backup_write_fd(-1)
{}

vol_t::~vol_t() {
    clear_caches();
    w_assert1(_unix_fd == -1);
    w_assert1(_backup_fd == -1);
    if (_restore_mgr) {
        delete _restore_mgr;
    }
}

rc_t vol_t::sync()
{
    W_DO_MSG(me()->fsync(_unix_fd), << "volume id=" << vid());
    return RCOK;
}

rc_t vol_t::mount(const char* devname)
{
    spinlock_write_critical_section cs(&_mutex);

    if (_unix_fd >= 0) return RC(eALREADYMOUNTED);

    /*
     *  Save the device name
     */
    w_assert1(strlen(devname) < sizeof(_devname));
    strcpy(_devname, devname);

    w_rc_t rc;
    int open_flags = smthread_t::OPEN_RDWR | smthread_t::OPEN_SYNC
        | smthread_t::OPEN_DIRECT;

    rc = me()->open(devname, open_flags, 0666, _unix_fd);
    if (rc.is_error()) {
        _unix_fd = -1;
        return rc;
    }

    // CS TODO: get rid of multiple logical volumes
    _vid = 1;

    clear_caches();
    build_caches(true);
    W_DO(init_metadata());

    return RCOK;
}

void vol_t::clear_caches()
{
    // caller must hold mutex
    if (_alloc_cache) {
        delete _alloc_cache;
        _alloc_cache = NULL;
    }
    if (_stnode_cache) {
        delete _stnode_cache;
        _stnode_cache = NULL;
    }
}

void vol_t::build_caches(bool virgin)
{
    // caller must hold mutex
    stnode_page stpage;
    if (virgin) {
        stpage.format_empty(_vid);
    }
    else {
        W_COERCE(read_page(stnode_page::stpid, (generic_page&) stpage));
    }

    _stnode_cache = new stnode_cache_t(stpage);
    w_assert1(_stnode_cache);

    _alloc_cache = new alloc_cache_t(*_stnode_cache, virgin);
    w_assert1(_alloc_cache);

    // CS TODO: should we write back pages of stnode and alloc caches here?
}

rc_t vol_t::init_metadata()
{
    W_DO(smlevel_0::bf->install_volume(this));

    return RCOK;
}

rc_t vol_t::write_metadata(int /*fd*/, vid_t /*vid*/)
{
    // CS TODO: flush pages of stnode and alloc caches
#if 0
    /*
     *  Set up and write the volume header
     */
    volhdr_t vhdr(vid, num_pages);
    W_DO(vhdr.write(fd));

    /*
     *  Skip first page ... seek to first info page.
     */
    W_DO(me()->lseek(fd, sizeof(generic_page), sthread_t::SEEK_AT_SET));

    generic_page buf;
    // initialize page with zeroes (for valgrind)
    // ::memset(&buf, 0, sizeof(generic_page));

    {
        // volume not formatted yet -- write metadata for empty volume
        shpid_t alloc_pages = alloc_page::num_alloc_pages(num_pages);
        shpid_t spid = alloc_pages + 1;

        //  Format alloc_page pages
        for (shpid_t apid = 1; apid <= alloc_pages; ++apid)  {
            alloc_page_h ap(&buf, lpid_t(vid, apid));  // format page
            // set bits for the header pages
            if (apid == 1) {
                for (shpid_t p = 0; p <= spid; p++)
                {
                    ap.set_bit(p);
                }
            }
            buf.checksum = buf.calculate_checksum();
            W_DO(me()->write(fd, &buf, sizeof(generic_page)));
        }
        DBG(<<" done formatting extent region");

        // Format stnode_page
        DBGTHRD(<<"Formatting stnode_page page " << spid);
        stnode_page_h fp(&buf, lpid_t(vid, spid));
        buf.checksum = buf.calculate_checksum();
        W_DO(me()->write(fd, &buf, sizeof(generic_page)));
        DBG(<<" done formatting store node region");
    }
#endif

    return RCOK;
}

rc_t vol_t::open_backup()
{
    // mutex held by caller -- no concurrent backup being added
    string backupFile = _backups.back();
    // Using direct I/O
    int open_flags = smthread_t::OPEN_RDONLY | smthread_t::OPEN_SYNC
        | smthread_t::OPEN_DIRECT;
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

rc_t vol_t::mark_failed(bool evict, bool redo)
{
    spinlock_write_critical_section cs(&_mutex);

    if (is_failed()) {
        return RCOK;
    }

    // empty metadata caches
    // clear_caches();
    // build_caches();

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

    _restore_mgr = new RestoreMgr(ss_m::get_options(),
            ss_m::logArchiver->getDirectory(), this, useBackup);

    set_failed(true);

    // evict device pages from buffer pool
    if (evict) {
        w_assert0(smlevel_0::bf);
        W_DO(smlevel_0::bf->uninstall_volume(_vid, true));
        // no need to call install because root pages are loaded on demand in
        // bf_tree_m::fix_root
    }

    lsn_t failureLSN = lsn_t::null;
    if (!redo) {
        // Create and insert logrec manually to get its LSN
        new (_logrec_buf) restore_begin_log(_vid);
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
        _restore_mgr->shutdown();

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

inline void vol_t::check_metadata_restored() const
{
    /* During restore, we must wait for shpid 0 to be restored.  Even though it
     * is not an actual pid restored in log replay, the log records of store
     * operations have pid 0, and they must be restored first so that the
     * stnode cache is restored.  See RestoreMgr::restoreMetadata()
     *
     * All operations that access metadata, i.e., allocations, store
     * operations, and root page ids, must wait for metadata to be restored
     */
    if (is_failed()) {
        w_assert1(_restore_mgr);
        _restore_mgr->waitUntilRestored(shpid_t(0));
    }
}

void vol_t::redo_segment_restore(unsigned segment)
{
    w_assert0(_restore_mgr && is_failed());
    _restore_mgr->markSegmentRestored(segment, true /* redo */);
}

rc_t vol_t::dismount(bool bf_uninstall, bool abrupt)
{
    spinlock_write_critical_section cs(&_mutex);

    DBG(<<" vol_t::dismount flush=" << flush);

    INC_TSTAT(vol_cache_clears);

    w_assert1(_unix_fd >= 0);
    if (bf_uninstall && smlevel_0::bf) { // might have shut down already
        W_DO(smlevel_0::bf->uninstall_volume(_vid, !abrupt /* clear_cb */));
    }

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

    clear_caches();

    return RCOK;
}

unsigned vol_t::num_backups() const
{
    spinlock_read_critical_section cs(&_mutex);
    return _backups.size();
}

void vol_t::add_backup(string path, lsn_t backupLSN)
{
    spinlock_write_critical_section cs(&_mutex);
    _backups.push_back(path);
    _backup_lsns.push_back(backupLSN);
    w_assert1(_backups.size() == _backup_lsns.size());
}

void vol_t::shutdown(bool abrupt)
{
    // bf_uninstall causes a force on the volume through the bf_cleaner
    W_COERCE(dismount(!abrupt /* uninstall */, abrupt));
}

rc_t vol_t::alloc_a_page(shpid_t& shpid, bool redo)
{
    // if (!redo) check_metadata_restored();

    w_assert1(_alloc_cache);
    // CS TODO: add store parameter
    W_DO(_alloc_cache->sx_allocate_page(shpid, 0, redo));

    if (!redo) {
        sys_xct_section_t ssx(true);
        ssx.end_sys_xct(log_alloc_page(vid(), shpid));
    }

    INC_TSTAT(page_alloc_cnt);

    return RCOK;
}

rc_t vol_t::deallocate_page(const shpid_t& pid, bool redo)
{
    // if (!redo) check_metadata_restored();

    w_assert1(_alloc_cache);
    // CS TODO: add store parameter
    W_DO(_alloc_cache->sx_deallocate_page(pid, 0, redo));

    if (!redo) {
        sys_xct_section_t ssx(true);
        ssx.end_sys_xct(log_dealloc_page(vid(), pid));
    }

    INC_TSTAT(page_dealloc_cnt);

    return RCOK;
}

size_t vol_t::num_used_pages() const
{
    return _alloc_cache->get_last_allocated_pid();
}

rc_t vol_t::create_store(lpid_t root_pid, snum_t& snum)
{
    // check_metadata_restored();
    return _stnode_cache->sx_create_store(root_pid.page, snum);
}

bool vol_t::is_alloc_store(snum_t f) const
{
    // check_metadata_restored();
    return _stnode_cache->is_allocated(f);
}

shpid_t vol_t::get_store_root(snum_t f) const
{
    // check_metadata_restored();
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
 *  vol_t::read_page(pnum, page, past_end)
 *
 *  Read the page at "pnum" of the volume into the buffer "page".
 *
 *********************************************************************/
rc_t vol_t::read_page(shpid_t pnum, generic_page& page)
{
    DBG(<< "Page read: vol " << _vid << " page " << pnum);
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
        w_assert1(_restore_mgr);

        { // pin avoids restore mgr being destructed while we access it
            spinlock_read_critical_section cs(&_mutex);
            if (!_restore_mgr->pin()) { break; }
        }

        if (!_restore_mgr->isRestored(pnum)) {
            DBG(<< "Page read triggering restore of " << pnum);
            bool reqSucceeded = _restore_mgr->requestRestore(pnum, &page);
            _restore_mgr->waitUntilRestored(pnum);
            w_assert1(_restore_mgr->isRestored(pnum));

            if (reqSucceeded) {
                // page is loaded in buffer pool already
                w_assert1(page.pid == lpid_t(_vid, pnum));
                sysevent::log_page_read(pnum);
                return RCOK;
            }
        }

        _restore_mgr->unpin();
        check_restore_finished();
        break;
    }

    size_t offset = size_t(pnum) * sizeof(page);

    smthread_t* t = me();

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
    memset(&page, '\0', sizeof(page));
#endif

    w_rc_t err = t->pread(_unix_fd, (char *) &page, sizeof(page), offset);
    if(err.err_num() == stSHORTIO) {
        /*
         * If we read past the end of the file, this means it is a virgin page,
         * so we simply fill the buffer with zeroes. Note that we can't read
         * past the logical size of the device due to the assert above.
         */
        memset(&page, 0, sizeof(page));
    }
    else {
        W_DO(err);
        w_assert1(page.pid == lpid_t(_vid, pnum));
    }

    sysevent::log_page_read(pnum);

    return RCOK;
}

rc_t vol_t::read_backup(shpid_t first, size_t count, void* buf)
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
    add_backup(path, backupLSN);
    {
        // critical section to guarantee visibility of the fd update
        spinlock_write_critical_section cs(&_mutex);
        W_DO(me()->close(_backup_write_fd));
        _backup_write_fd = -1;
    }

    DBG1(<< "Finished taking backup");

    return RCOK;
}

rc_t vol_t::write_backup(shpid_t first, size_t count, void* buf)
{
    w_assert0(_backup_write_fd > 0);
    w_assert1(first + count <= (shpid_t) num_used_pages());
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
rc_t vol_t::write_many_pages(shpid_t pnum, const generic_page* const pages, int cnt,
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

    w_assert1(pnum > 0 && pnum < (shpid_t) num_used_pages());
    w_assert1(cnt > 0);
    size_t offset = size_t(pnum) * sizeof(generic_page);

#if W_DEBUG_LEVEL > 2
    for (int j = 1; j < cnt; j++) {
        w_assert1(ignoreRestore ||
                pages[j].pid.page - 1 == pages[j-1].pid.page);
        w_assert1(ignoreRestore || pages[j].pid.vol() == _vid);
        // CS: this assertion should hold, but some test cases fail it
        // e.g., TreeBufferpoolTest.Swizzle in test_bf_tree.cpp
        // w_assert1(pages[j].tag != t_btree_p || pages[j].lsn != lsn_t::null);
    }
#endif

    smthread_t* t = me();

    long start = 0;
    if(_apply_fake_disk_latency) start = gethrtime();

    // do the actual write now
    W_COERCE_MSG(t->pwrite(_unix_fd, pages, sizeof(generic_page)*cnt, offset), << "volume id=" << vid());

    fake_disk_latency(start);
    ADD_TSTAT(vol_blks_written, cnt);
    INC_TSTAT(vol_writes);

    sysevent::log_page_write(pnum, cnt);

    return RCOK;
}

uint32_t vol_t::get_last_allocated_pid() const
{
    w_assert1(_alloc_cache);
    return _alloc_cache->get_last_allocated_pid();
}

bool vol_t::is_allocated_page(shpid_t pid) const
{
    w_assert1(_alloc_cache);
    return _alloc_cache->is_allocated(pid);
}

