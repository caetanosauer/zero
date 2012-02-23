#ifndef VOL_H
#define VOL_H

#include "w_defines.h"

#ifdef __GNUG__
#pragma interface
#endif

#include <list>
#include "stnode_p.h"


struct volume_hdr_stats_t;
class alloc_cache_t;
class stnode_cache_t;
class bf_fixed_m;

class volhdr_t {
    // For compatibility checking, we record a version number
    // number of the Shore SM version which formatted the volume.
    // This number is called volume_format_version in sm_base.h.
    uint32_t   _format_version;
    sm_diskaddr_t       _device_quota_KB;
    lvid_t              _lvid;
    shpid_t             _apid;        // first alloc_p pid
    shpid_t             _spid;        // the only stnode_p pid
    uint32_t            _num_pages;
    shpid_t             _hdr_pages;   // # pages in hdr includes entire store 0
    uint32_t            _page_sz;    // page size in bytes
public:
    uint32_t   format_version() const { 
                            return _format_version; }
    void            set_format_version(uint v) { 
                        _format_version = v; }

    sm_diskaddr_t   device_quota_KB() const { 
                            return _device_quota_KB; }
    void            set_device_quota_KB(sm_diskaddr_t q) { 
                            _device_quota_KB = q; }

    const lvid_t&   lvid() const { return _lvid; }
    void            set_lvid(const lvid_t &l) { 
                             _lvid = l; }

    const shpid_t&   apid() const { return _apid; }
    void             set_apid(const shpid_t& p) { _apid = p; }

    const shpid_t&   spid() const { return _spid; }
    void             set_spid(const shpid_t& p) { _spid = p; }

    uint32_t         num_pages() const {  return _num_pages; }
    void             set_num_pages(uint32_t n) {  _num_pages = n; }

    shpid_t          hdr_pages() const {  return _hdr_pages; }
    void             set_hdr_pages(shpid_t n) {  _hdr_pages = n; }

    uint32_t         page_sz() const {  return _page_sz; }
    void             set_page_sz(uint32_t n) {  _page_sz = n; }

};

#include <vector>
#include <set>
#include <map>

class vol_t : public smlevel_1 
{
public:
    /*WARNING: THIS CODE MUST MATCH THAT IN sm_io.h!!! */
    typedef srwlock_t  VolumeLock;
    typedef void *     lock_state;
    
    NORET               vol_t(const bool apply_fake_io_latency = false, 
                                      const int fake_disk_latency = 0);
    NORET               ~vol_t();
    
    /** Mount the volume at "devname" and give it a an id "vid". */
    rc_t                mount(const char* devname, vid_t vid);

    /** Dismount the volume. */
    rc_t                dismount(bool flush = true);

    /**
    * Print out meta info about the volume.
    *  It is the caller's responsibility to worry about mt-safety of this;
    *  it is for the use of smsh & debugging
    *  and is not called from anywhere w/in the ss_m
    */
    rc_t                check_disk();
    rc_t                check_store_pages(snum_t snum, page_p::tag_t tag);
    rc_t                check_store_page(const lpid_t &pid, page_p::tag_t tag);

    const char*         devname() const;
    vid_t               vid() const ;
    lvid_t              lvid() const ;
    uint32_t            num_pages() const;
    uint32_t            num_used_pages() const;

    int                 fill_factor(snum_t fnum);
 
    bool                is_valid_page_num(const lpid_t& p) const;
    bool                is_valid_store(snum_t f) const;

    bool                is_allocated_page(shpid_t pid) const;

    /**  Return true if the store "store" is allocated. false otherwise. */
    bool                is_alloc_store(snum_t f) const;
    
    rc_t                write_page(shpid_t page, page_s& buf);

    rc_t                write_many_pages(
        shpid_t             first_page,
        const page_s*       buf, 
        int                 cnt);

    rc_t                read_page(
        shpid_t             page,
        page_s&             buf);

    rc_t            alloc_a_page(const stid_t &stid, lpid_t &pid);
    rc_t            alloc_consecutive_pages(const stid_t &stid, size_t page_count, lpid_t &pid_begin);
    rc_t            free_page(const lpid_t& pid);
    
    rc_t            redo_alloc_a_page(shpid_t pid);
    rc_t            redo_alloc_consecutive_pages(size_t page_count, shpid_t pid_begin);
    rc_t            redo_free_page(shpid_t pid);

    rc_t            store_operation(const store_operation_param&    param);
    /** Sets root page ID of the specified index. */
    rc_t            set_store_root(snum_t snum, shpid_t root);
    /** Returns root page ID of the specified index. */
    shpid_t         get_store_root(snum_t f) const;

    /** Find an unused store and return it in "snum". */
    rc_t            find_free_store(snum_t& fnum);
    rc_t            alloc_store(
        snum_t                 fnum,
        store_flag_t           flags);

    rc_t            set_store_flags(
        snum_t                 fnum,
        store_flag_t           flags,
        bool                   sync_volume);
    rc_t            get_store_flags(
        snum_t                 fnum,
        store_flag_t&          flags,
        bool                   ok_if_deleting);

public:
    // The following functinos return space utilization statistics
    // on the volume or selected stores.  These functions use only
    // the store and page/extent meta information.

    rc_t                     get_volume_meta_stats(
        SmVolumeMetaStats&          volume_stats);

    rc_t                     get_store_meta_stats(
        snum_t                      snum,
        SmStoreMetaStats&           storeStats);
    
public:

    rc_t             num_pages(snum_t fnum, uint32_t& cnt);
    bool             is_raw() { return _is_raw; };

    /** Sync the volume. */
    rc_t            sync();

    // format a device (actually, just zero out the header for now)
    static rc_t            format_dev(
        const char*          devname,
        shpid_t              num_pages,
        bool                 force);

    static rc_t            format_vol(
        const char*          devname,
        lvid_t               lvid,
        vid_t                vid,
        shpid_t              num_pages,
        bool                 skip_raw_init);

    static rc_t            read_vhdr(const char* devname, volhdr_t& vhdr);
    static rc_t            read_vhdr(int fd, volhdr_t& vhdr);

    static rc_t            write_vhdr(           // SMUF-SC3: moved to public
        int                  fd, 
        volhdr_t&            vhdr, 
        bool                 raw_device);

    /**
    * Check if "devname" is a raw device. Return result in "raw".
    * XXX This has problems.  Once the file is opened it should
    * never be closed.  Otherwise the file can be switched underneath
    * the system and havoc can ensue.
    */
    static rc_t            check_raw_device(
        const char*          devname,
        bool&                raw);

    

    // methods for space usage statistics for this volume
    rc_t             get_du_statistics(
        struct              volume_hdr_stats_t&,
        bool                audit);

    void            assert_mutex_mine(lock_state *) {}
    void            assert_mutex_notmine(lock_state *) {}

    // Sometimes the sm_io layer acquires this mutex:
    void            acquire_mutex(lock_state* me, bool for_write); // used by sm_io.cpp
    VolumeLock&     vol_mutex() const { return _mutex; } // used by sm_io.cpp

    // fake disk latency
    void            enable_fake_disk_latency(void);
    void            disable_fake_disk_latency(void);
    bool            set_fake_disk_latency(const int adelay);    
    void            fake_disk_latency(long start);    

private:
    char             _devname[max_devname];
    int              _unix_fd;
    vid_t            _vid;
    lvid_t           _lvid;
    uint             _num_pages;
    uint             _hdr_pages;
    lpid_t           _apid;
    lpid_t           _spid;
    int              _page_sz;  // page size in bytes
    bool             _is_raw;   // notes if volume is a raw device

    mutable VolumeLock _mutex;   

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
    
    /** USED ONLY FROM TESTCASES!. */
    alloc_cache_t*           get_alloc_cache() {return _alloc_cache;}
    stnode_cache_t*          get_stnode_cache() {return _stnode_cache;}
    bf_fixed_m*              get_fixed_bf() {return _fixed_bf;}

    static const char*       prolog[]; // string array for volume hdr

};

inline const char* vol_t::devname() const
{
    return _devname;
}

inline vid_t vol_t::vid() const
{
    return _vid;
}

inline lvid_t vol_t::lvid() const
{
    return _lvid;
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
    return (f < stnode_p::max);
}
    
/*<std-footer incl-file-exclusion='VOL_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
