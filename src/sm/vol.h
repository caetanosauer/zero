/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef VOL_H
#define VOL_H

#include "w_defines.h"

#include <list>
#include <stdlib.h>

struct volume_hdr_stats_t;
class alloc_cache_t;
class stnode_cache_t;
class bf_fixed_m;
class RestoreMgr;
class store_operation_param;
class sm_options;
class vol_t;

#include "stnode_page.h"

class vol_m {
public:
    vol_m(const sm_options& options);
    virtual ~vol_m();

    void shutdown(bool abrupt);

    vol_t* get(vid_t vid);
    vol_t* get(const char* path);

    rc_t sx_mount(const char* device, bool logit = true);
    rc_t sx_dismount(const char* device, bool logit = true);

    rc_t sx_format(
            const char* devname,
            shpid_t num_pages,
            vid_t& vid,
            bool logit = false
    );

    /** Add a backup file to be used for restore */
    rc_t sx_add_backup(vid_t vid, string path, bool redo = false);

    rc_t list_volumes(
            std::vector<string>& names,
            std::vector<vid_t>& vids,
            size_t start = 0,
            size_t count = 0
    );

    rc_t list_backups(
            std::vector<string>& backups,
            std::vector<vid_t>& vids,
            size_t start = 0,
            size_t count = 0
    );

    rc_t force_fixed_buffers();

    int num_vols() { return vol_cnt; }

    bool is_mounted(vid_t vid) { return get(vid) != NULL; }

    vid_t get_next_vid() {
        // TODO must be in mutual exclusion with create_vol
        return _next_vid;
    }

    void set_next_vid(vid_t vid) {
        // TODO must be in mutual exclusion with create_vol
        _next_vid = vid;
    }

    static const int MAX_VOLS = 32;

private:
    int    vol_cnt;
    vol_t* volumes[MAX_VOLS];
    vid_t _next_vid;

    srwlock_t _mutex;
};

struct volhdr_t {
    // For compatibility checking, we record a version number
    // number of the Shore SM version which formatted the volume.
    static const uint32_t FORMAT_VERSION = 19;

    uint32_t   version;
    vid_t      vid;
    shpid_t    apid;        // first alloc_page pid
    shpid_t    spid;        // the only stnode_page pid
    uint32_t   num_pages;
    shpid_t    hdr_pages;   // # pages in hdr includes entire store 0

    rc_t             write(int fd);
    rc_t             read(int fd);

private:
    static const char*       prolog[]; // string array for volume hdr
};

/*
Volume layout:
   volume header
   alloc_page pages -- Starts on page 1.
   stnode_page -- only one page
   data pages -- rest of volume
*/
class vol_t
{
    friend class vol_m;
protected: // access restricted to vol_m
    vol_t();
    virtual ~vol_t();

    void shutdown(bool abrupt);
    rc_t mount(const char* devname);
    rc_t dismount(bool bf_uninstall = true, bool abrupt = false);

public:
    const char* devname() const { return _devname; }
    vid_t       vid() const { return _vid; }
    shpid_t     first_data_pageid() const { return _first_data_pageid; }
    uint32_t    num_pages() const { return _num_pages; }
    uint32_t    num_used_pages() const;

    alloc_cache_t*           get_alloc_cache() {return _alloc_cache;}
    stnode_cache_t*          get_stnode_cache() {return _stnode_cache;}
    bf_fixed_m*              get_fixed_bf() {return _fixed_bf;}


    /**
     *
     * Thread safety: the underlying POSIX calls pwrite and pread are
     * guaranteed to be atomic, so no additional latching is required for these
     * methods. Mounting/dismounting during reads and writes causes the file
     * descriptor to change, resulting in the expected errors in the return
     * code.
     */
    rc_t                write_page(shpid_t page, generic_page& buf);
    rc_t                write_many_pages(
        shpid_t             first_page,
        const generic_page* buf,
        int                 cnt,
        bool ignoreRestore = false);

    rc_t                read_page(
        shpid_t             page,
        generic_page&       buf);

    rc_t read_backup(shpid_t first, size_t count, generic_page* buf);

    rc_t            sync();

    // get space usage statistics for this volume
    rc_t             get_du_statistics(
        struct              volume_hdr_stats_t&,
        bool                audit);

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

    /**
    * Print out meta info about the volume.
    *  It is the caller's responsibility to worry about mt-safety of this;
    *  it is for the use of smsh & debugging
    *  and is not called from anywhere w/in the ss_m
    */
    rc_t                check_disk();

    rc_t            alloc_a_page(shpid_t& pid, bool redo = false);
    rc_t            alloc_consecutive_pages(size_t page_count,
                        shpid_t &pid_begin, bool redo = false);
    rc_t            deallocate_page(const shpid_t& pid, bool redo = false);

    bool                is_allocated_page(shpid_t pid) const;

    bool                is_valid_store(snum_t f) const;

    /**  Return true if the store "store" is allocated. false otherwise. */
    bool                is_alloc_store(snum_t f) const;

    rc_t            store_operation(const store_operation_param&    param,
                                    bool redo = false);
    /** Sets root page ID of the specified index. */
    rc_t            set_store_root(snum_t snum, shpid_t root);
    /** Returns root page ID of the specified index. */
    shpid_t         get_store_root(snum_t f) const;

    rc_t            create_store(smlevel_0::store_flag_t, snum_t&);

    rc_t            set_store_flags(
        snum_t                 fnum,
        smlevel_0::store_flag_t           flags);

    rc_t            get_store_flags(
        snum_t                 fnum,
        smlevel_0::store_flag_t&          flags,
        bool                   ok_if_deleting = false);

    /** Mark device as failed and kick off Restore */
    rc_t            mark_failed(bool evict = false, bool redo = false);

    void add_backup(string path);

    bool is_failed() const
    {
        lintel::atomic_thread_fence(lintel::memory_order_acquire);
        return _failed;
    }

    unsigned num_backups() const;

    bool check_restore_finished(bool redo = false);

    void redo_segment_restore(unsigned segment);

private:
    // variables read from volume header -- remain constant after mount
    char             _devname[smlevel_0::max_devname];
    int              _unix_fd;
    vid_t            _vid;
    shpid_t          _first_data_pageid;
    uint             _num_pages;
    uint             _hdr_pages;
    lpid_t           _apid;
    lpid_t           _spid;

    mutable srwlock_t _mutex;


    // fake disk latency
    bool             _apply_fake_disk_latency;
    int              _fake_disk_latency;

    alloc_cache_t*   _alloc_cache;
    stnode_cache_t*  _stnode_cache;
    /** buffer manager for special pages. */
    bf_fixed_m*      _fixed_bf;

    /** Set to simulate a failed device for Restore **/
    bool             _failed;

    /** Restore Manager is activated when volume has failed */
    RestoreMgr*      _restore_mgr;

    /** Paths to backup files, added with add_backup() */
    std::vector<string> _backups;

    /** Currently opened backup (during restore only) */
    int _backup_fd;

    /** Methods to create and destroy _alloc_cache, _stnode_cache, and
     * _fixed_bf */
    void clear_caches();
    void build_caches();

    /** Initialize caches by reading from (restored/healthy) device */
    rc_t init_metadata();

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
    return (f < stnode_page_h::max);
}

#endif          /*</std-footer>*/
