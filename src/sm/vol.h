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
class store_operation_param;
class sm_options;
class vol_t;

#include "stnode_page.h"

class vol_m {
public:
    vol_m(const sm_options& options);
    virtual ~vol_m();

    vol_t* get(vid_t vid);
    vol_t* get(const char* path);

    rc_t sx_mount(const char* device, bool logit = true);
    rc_t sx_dismount(const char* device, bool logit = true);
    rc_t sx_dismount_all();

    rc_t sx_format(
            const char* devname,
            shpid_t num_pages,
            vid_t& vid,
            bool logit = false
    );

    rc_t list_volumes(
            std::vector<string>& names,
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

class vol_t
{
public:
    /*WARNING: THIS CODE MUST MATCH THAT IN sm_io.h!!! */
    typedef srwlock_t  VolumeLock;
    typedef void *     lock_state;

    NORET               vol_t(const bool apply_fake_io_latency = false,
                                      const int fake_disk_latency = 0);
    NORET               ~vol_t();

    /** Mount the volume at "devname" and give it a an id "vid". */
    rc_t                mount(const char* devname);

    /** Dismount the volume. */
    rc_t                dismount(bool flush = true, const bool clear_cb = true);

    /**
    * Print out meta info about the volume.
    *  It is the caller's responsibility to worry about mt-safety of this;
    *  it is for the use of smsh & debugging
    *  and is not called from anywhere w/in the ss_m
    */
    rc_t                check_disk();

    const char*         devname() const;
    vid_t               vid() const ;
    /** returns the page ID of the first non-fixed pages (after stnode_page and alloc_page).*/
    shpid_t             first_data_pageid() const { return _first_data_pageid;}
    uint32_t            num_pages() const;
    uint32_t            num_used_pages() const;

    int                 fill_factor(snum_t fnum);

    bool                is_valid_page_num(const lpid_t& p) const;
    bool                is_valid_store(snum_t f) const;

    bool                is_allocated_page(shpid_t pid) const;

    /**  Return true if the store "store" is allocated. false otherwise. */
    bool                is_alloc_store(snum_t f) const;

    rc_t                write_page(shpid_t page, generic_page& buf);

    rc_t                write_many_pages(
        shpid_t             first_page,
        const generic_page*       buf,
        int                 cnt);

    rc_t                read_page(
        shpid_t             page,
        generic_page&       buf);

    rc_t            alloc_a_page(shpid_t& pid);
    rc_t            alloc_consecutive_pages(size_t page_count, shpid_t &pid_begin);
    rc_t            free_page(const shpid_t& pid);

    rc_t            redo_alloc_a_page(shpid_t pid);
    rc_t            redo_alloc_consecutive_pages(size_t page_count, shpid_t pid_begin);
    rc_t            redo_free_page(shpid_t pid);

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

public:

    rc_t             num_pages(snum_t fnum, uint32_t& cnt);
    rc_t             get_quota_kb(size_t& total, size_t& used);

    /** Sync the volume. */
    rc_t            sync();

    // methods for space usage statistics for this volume
    rc_t             get_du_statistics(
        struct              volume_hdr_stats_t&,
        bool                audit);

    // Sometimes the sm_io layer acquires this mutex:
    void            acquire_mutex(lock_state* me, bool for_write); // used by sm_io.cpp
    VolumeLock&     vol_mutex() const { return _mutex; } // used by sm_io.cpp

    // fake disk latency
    void            enable_fake_disk_latency(void);
    void            disable_fake_disk_latency(void);
    bool            set_fake_disk_latency(const int adelay);
    void            fake_disk_latency(long start);

private:
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

    /** releases _alloc_cache and _stnode_cache. */
    void clear_caches();
 public:
    void                     shutdown();
    void                     shutdown(snum_t s);

    alloc_cache_t*           get_alloc_cache() {return _alloc_cache;}
    stnode_cache_t*          get_stnode_cache() {return _stnode_cache;}
    bf_fixed_m*              get_fixed_bf() {return _fixed_bf;}

};

inline const char* vol_t::devname() const
{
    return _devname;
}

inline vid_t vol_t::vid() const
{
    return _vid;
}

inline uint vol_t::num_pages() const
{
    return (uint) _num_pages;
}

inline bool vol_t::is_valid_page_num(const lpid_t& p) const
{
    return (_num_pages > p.page);
}

inline bool vol_t::is_valid_store(snum_t f) const
{
    return (f < stnode_page_h::max);
}

/*<std-footer incl-file-exclusion='VOL_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
