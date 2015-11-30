/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef VOL_H
#define VOL_H

#include "w_defines.h"

#include <list>
#include <stdlib.h>

class alloc_cache_t;
class stnode_cache_t;
class RestoreMgr;
class store_operation_param;
class sm_options;
class chkpt_restore_tab_t;

#include "stnode_page.h"

class vol_t
{
    friend class vol_m;

public:
    vol_t(const sm_options&);
    virtual ~vol_t();

    void shutdown(bool abrupt);

    const char* devname() const { return _devname; }
    vid_t       vid() const { return _vid; }
    // CS TODO
    shpid_t     first_data_pageid() const { return 0; }
    size_t      num_used_pages() const;

    alloc_cache_t*           get_alloc_cache() {return _alloc_cache;}
    stnode_cache_t*          get_stnode_cache() {return _stnode_cache;}


    /**
     *
     * Thread safety: the underlying POSIX calls pwrite and pread are
     * guaranteed to be atomic, so no additional latching is required for these
     * methods. Mounting/dismounting during reads and writes causes the file
     * descriptor to change, resulting in the expected errors in the return
     * code.
     */
    rc_t                write_many_pages(
        shpid_t             first_page,
        const generic_page* buf,
        int                 cnt,
        bool ignoreRestore = false);

    rc_t write_page(shpid_t page, generic_page* buf) {
        return write_many_pages(page, buf, 1);
    }

    rc_t                read_page(
        shpid_t             page,
        generic_page&       buf);

    rc_t read_backup(shpid_t first, size_t count, void* buf);
    rc_t write_backup(shpid_t first, size_t count, void* buf);

    /** Add a backup file to be used for restore */
    rc_t sx_add_backup(string path, bool redo = false);

    void list_backups(std::vector<string>& backups);

    rc_t            sync();

    /**
     *  Impose a fake IO penalty. Assume that each batch of pages requires
     *  exactly one seek. A real system might perform better due to sequential
     *  access, or might be worse because the pages in the batch are not
     *  actually contiguous. Close enough...
     */
    void            enable_fake_disk_latency(void);
    void            disable_fake_disk_latency(void);
    bool            set_fake_disk_latency(const int adelay);
    void            fake_disk_latency(long start);

    rc_t            alloc_a_page(shpid_t& pid, bool redo = false);
    rc_t            deallocate_page(const shpid_t& pid, bool redo = false);

    bool                is_allocated_page(shpid_t pid) const;

    bool                is_valid_store(snum_t f) const;

    /**  Return true if the store "store" is allocated. false otherwise. */
    bool                is_alloc_store(snum_t f) const;

    /** Sets root page ID of the specified index. */
    rc_t            set_store_root(snum_t snum, shpid_t root);
    /** Returns root page ID of the specified index. */
    shpid_t         get_store_root(snum_t f) const;

    rc_t            create_store(lpid_t, snum_t&);

    /** Mark device as failed and kick off Restore */
    rc_t            mark_failed(bool evict = false, bool redo = false);

    lsn_t get_backup_lsn();

    /** Turn on write elision (i.e., ignore all writes from now on) */
    void set_readonly(bool r)
    {
        spinlock_write_critical_section cs(&_mutex);
        _readonly = r;
    }

    /** Take a backup on the given file path. */
    rc_t take_backup(string path, bool forceArchive = false);

    bool is_failed() const
    {
        lintel::atomic_thread_fence(lintel::memory_order_acquire);
        return _failed;
    }

    unsigned num_backups() const;

    bool check_restore_finished();

    void redo_segment_restore(unsigned segment);

    /** Used for checkpointing bitmap of restored segments */
    void chkpt_restore_progress(chkpt_restore_tab_t* tab);

    /** Return largest PID allocated for this volume yet **/
    shpid_t get_last_allocated_pid() const;

private:
    // variables read from volume header -- remain constant after mount
    char             _devname[smlevel_0::max_devname];
    int              _unix_fd;
    vid_t            _vid;

    mutable srwlock_t _mutex;


    // fake disk latency
    bool             _apply_fake_disk_latency;
    int              _fake_disk_latency;

    alloc_cache_t*   _alloc_cache;
    stnode_cache_t*  _stnode_cache;

    /** Set to simulate a failed device for Restore **/
    bool             _failed;

    /** Writes are ignored and old page versions are kept.  This means that
     * clean status on buffer pool is invalid, and thus single-page recovery is
     * required when reading page back.  Due to a current bug on the page
     * cleaner, this is already the case anyway. I.e., write elision is already
     * taking place due to the bug. If readonly is set, all writes are elided.
     */
    bool             _readonly;

    /** Restore Manager is activated when volume has failed */
    RestoreMgr*      _restore_mgr;

    /** Paths to backup files, added with add_backup() */
    std::vector<string> _backups;
    std::vector<lsn_t> _backup_lsns;

    /** Currently opened backup (during restore only) */
    int _backup_fd;
    lsn_t _current_backup_lsn;

    /** Backup being currently taken */
    int _backup_write_fd;
    string _backup_write_path;

    /** Buffer to create restore_begin lorec manually
     *  (128 bytes are enough since it contains only vid) */
    char _logrec_buf[128];

    rc_t mount(const char* devname, bool truncate = false);
    rc_t dismount(bool bf_uninstall = true, bool abrupt = false);

    /** Methods to create and destroy _alloc_cache and _stnode_cache */
    void clear_caches();
    void build_caches(bool virgin);

    /** Open backup file descriptor for retore or taking new backup */
    rc_t open_backup();

    /** Initialize caches by reading from (restored/healthy) device */
    rc_t init_metadata();

    /** Initialize metadata region of physical device **/
    static rc_t write_metadata(int fd, vid_t vid);

    // setting failed status only allowed internally (private method)
    void set_failed(bool failed)
    {
        _failed = failed;
        lintel::atomic_thread_fence(lintel::memory_order_release);
    }

    /** If media failure happened, wait for metadata to be restored */
    void check_metadata_restored() const;
};

inline bool vol_t::is_valid_store(snum_t f) const
{
    return (f < stnode_page::max);
}

#endif          /*</std-footer>*/
