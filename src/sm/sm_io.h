/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef SM_IO_H
#define SM_IO_H

#include "w_defines.h"
#include "sm_base.h"
#include "smthread.h"

class vol_t;
class sdesc_t;
class SmVolumeMetaStats;
class SmFileMetaStats;
class SmStoreMetaStats;
class xct_t; // forward
struct lvid_t;

struct volume_hdr_stats_t;
class alloc_cache_t;

class store_operation_param  {
    friend ostream & operator<<(ostream&, const store_operation_param &);
    
public:
        typedef smlevel_0::store_operation_t        store_operation_t;
        typedef smlevel_0::store_flag_t             store_flag_t;
        typedef smlevel_0::store_deleting_t         store_deleting_t;

private:
        snum_t                _snum;
        uint16_t               _op;
        fill2                 _filler; // for purify
        union {
            struct {
                uint16_t      _value1;
                uint16_t      _value2;
            } values;
            shpid_t page;
        } _u;


    public:
        store_operation_param(snum_t snum, store_operation_t theOp) :
            _snum(snum), _op(theOp)
        {
            w_assert2(_op == smlevel_0::t_delete_store);
            _u.page=0;
        };
        store_operation_param(snum_t snum, store_operation_t theOp, 
                              store_flag_t theFlags) :
            _snum(snum), _op(theOp)
        {
            w_assert2(_op == smlevel_0::t_create_store);
            _u.values._value1 = theFlags;
            _u.values._value2 = 0; // unused
        };
        store_operation_param(snum_t snum, store_operation_t theOp, 
                              store_deleting_t newValue, 
                              store_deleting_t oldValue = smlevel_0::t_unknown_deleting) :
            _snum(snum), _op(theOp)
        {
            w_assert2(_op == smlevel_0::t_set_deleting);
            _u.values._value1 = newValue;
            _u.values._value2 = oldValue;
        };
        store_operation_param(snum_t snum, store_operation_t theOp, 
                              store_flag_t newFlags, 
                              store_flag_t oldFlags) :
            _snum(snum), _op(theOp)
        {
            w_assert2(_op == smlevel_0::t_set_store_flags);
            _u.values._value1 = newFlags;
            _u.values._value2 = oldFlags;
        };
        store_operation_param(snum_t snum, store_operation_t theOp, 
                              shpid_t root) :
            _snum(snum), _op(theOp)
        {
            w_assert2(_op == smlevel_0::t_set_root);
            _u.page=root;
        };


        snum_t snum()  const { return _snum; };
        store_operation_t op()  const { return (store_operation_t)_op; };
        store_flag_t new_store_flags()  const {
            w_assert2(_op == smlevel_0::t_create_store 
                      || _op == smlevel_0::t_set_store_flags);
            return (store_flag_t)_u.values._value1;
        };
        store_flag_t old_store_flags()  const {
            w_assert2(_op == smlevel_0::t_set_store_flags);
            return (store_flag_t)_u.values._value2;
        };
        void set_old_store_flags(store_flag_t flag) {
            w_assert2(_op == smlevel_0::t_set_store_flags);
            _u.values._value2 = flag;
        }
        shpid_t root()  const {
            w_assert2(_op == smlevel_0::t_set_root);
            return _u.page;
        };
        store_deleting_t new_deleting_value()  const {
            w_assert2(_op == smlevel_0::t_set_deleting);
            return (store_deleting_t)_u.values._value1;
        };
        store_deleting_t old_deleting_value()  const {
            w_assert2(_op == smlevel_0::t_set_deleting);
            return (store_deleting_t)_u.values._value2;
        };
        void set_old_deleting_value(store_deleting_t old_value) {
            w_assert2(_op == smlevel_0::t_set_deleting);
            _u.values._value2 = old_value;
        }
        int size()  const { return sizeof (*this); };

    private:
        store_operation_param();
};

class io_m_test;
class btree_impl;
class generic_page;

/**
 * IO Manager.
 */
class io_m : public smlevel_0 {
    friend class io_m_test;
    friend class btree_impl; // for volume-wide verification
public:
    NORET                       io_m();
    NORET                       ~io_m();
    
    static void                 clear_stats();
    static int                  num_vols();
    
  
    /*
     * Device related
     */
    static bool                 is_mounted(const char* dev_name);
    static rc_t                 mount_dev(const char* device, u_int& vol_cnt);
    static rc_t                 dismount_dev(const char* device);
    static rc_t                 dismount_all_dev();
    static rc_t                 get_lvid(const char* dev_name, lvid_t& lvid);
    static rc_t                 list_devices(
        const char**&                 dev_list, 
        devid_t*&                     devid_list, 
        u_int&                        dev_cnt);

    static rc_t                 get_device_quota(
        const char*                   device, 
        smksize_t&                    quota_KB, 
        smksize_t&                    quota_used_KB);
    

    /*
     * Volume related
     */

    static rc_t                 get_vols(
        int                           start,
        int                           count, 
        char                          **dname, 
        vid_t                         vid[],
        int&                          return_cnt);
    static rc_t                 check_disk(const vid_t &vid);
    // return an unused vid_t
    static rc_t                 get_new_vid(vid_t& vid);
    static bool                 is_mounted(vid_t vid);
    static vol_t*               get_volume(vid_t vid); // so far only for testing and debugging
    static vid_t                get_vid(const lvid_t& lvid);
    static lvid_t               get_lvid(const vid_t vid);
    static const char*          dev_name(vid_t vid);
    static lsn_t                GetLastMountLSN();                // used for logging/recovery purposes
    static void                 SetLastMountLSN(lsn_t theLSN);

    static rc_t                 read_page(const lpid_t& pid, generic_page& buf);
    static void                 write_many_pages(const generic_page* bufs, int cnt);
    
    static rc_t                 mount(
         const char*                  device, 
         vid_t                        vid, 
         const bool                   apply_fake_io_latency = false, 
         const int                    fake_disk_latency = 0);
    static rc_t                 dismount(vid_t vid, bool flush = true);
    static rc_t                 dismount_all(bool flush = true);
    static rc_t                 sync_all_disks();
    
    /** flushes bf_fixed of all volumes currently mounted. */
    static rc_t                 flush_all_fixed_buffer ();
    /** flushes bf_fixed of the specified volume. */
    static rc_t                 flush_vol_fixed_buffer (vid_t vid);


    // fake_disk_latency
    static rc_t                 enable_fake_disk_latency(vid_t vid);
    static rc_t                 disable_fake_disk_latency(vid_t vid);
    static rc_t                 set_fake_disk_latency(
        vid_t                          vid, 
        const int                      adelay);


    static rc_t                 get_volume_quota(
        vid_t                          vid, 
        smksize_t&                     quota_KB, 
        smksize_t&                     quota_used_KB
        );
    
    /**
    *  Allocates one page for store "stid" and return the page id
    *  allocated in pid.
    *  Called from btree to allocate a single page.
    */
    static rc_t alloc_a_page(const stid_t &stid, lpid_t &pid);


    /** Create a single-log system transaction and call alloc_a_page. */
    static rc_t sx_alloc_a_page(const stid_t &stid, lpid_t &pid);

    /**
    *  Allocates multple consecutive pages for store "stid" and return
    * the first allocated page id  in pid.
    *  Called from btree to speed up bulk-allocation.
    */
    static rc_t alloc_consecutive_pages(const stid_t &stid, size_t page_count, lpid_t &pid_begin);

    /** Create a single-log system transaction and call alloc_consecutive_pages. */
    static rc_t sx_alloc_consecutive_pages(const stid_t &stid, size_t page_count, lpid_t &pid_begin);

    /**
     * Deallocate one page. Only called from bufferpool.
     */
    static rc_t dealloc_a_page(const lpid_t& pid);

    /** Create a single-log system transaction and call dealloc_a_page. */
    static rc_t sx_dealloc_a_page(const lpid_t &pid);

    // for REDO. these don't log
    static rc_t redo_alloc_a_page(vid_t vid, shpid_t pid);
    static rc_t redo_alloc_consecutive_pages(vid_t vid, size_t page_count, shpid_t pid_begin);
    static rc_t redo_dealloc_a_page(vid_t vid, shpid_t pid);

    static rc_t                 create_store(
        vid_t                          vid, 
        store_flag_t                   flags,
        stid_t&                        stid);
    static rc_t                 get_store_flags(
        const stid_t&                  stid,
        store_flag_t&                  flags,
        bool                           ok_if_deleting = false);
    static rc_t                 set_store_flags(
        const stid_t&                  stid,
        store_flag_t                   flags,
        bool                           sync_volume = true);
    static bool                 is_valid_store(const stid_t& stid);
    static shpid_t              get_root(const stid_t& stid);
    static rc_t                 set_root(const stid_t& stid, shpid_t root_pid);

    
    // The following functinos return space utilization statistics
    // on the volume or selected stores.  These functions use only
    // the store and page/extent meta information.

    static rc_t                 get_volume_meta_stats(
        vid_t                          vid,
        SmVolumeMetaStats&             volume_stats);
    static rc_t                 get_store_meta_stats(
        stid_t                         snum,
        SmStoreMetaStats&              storeStats);

    // this reports du statistics
    static rc_t                 get_du_statistics( // DU DF
        vid_t                            vid,
        volume_hdr_stats_t&              stats,
        bool                             audit);

    // This function sets a milli_sec delay to occur before 
    // each disk read/write operation.  This is useful in discovering
    // thread sync bugs
    static rc_t                 set_disk_delay(
        uint32_t                milli_sec) { 
                                        _msec_disk_delay = milli_sec; 
                                        return RCOK; 
                                    }
  
    //
    // Statistics information
    //
    static void                 io_stats(
        u_long&                         reads, 
        u_long&                         writes, 
        u_long&                         allocs,
        u_long&                         deallocs, 
        bool                            reset);


    static rc_t                 store_operation(
        vid_t                           vid,
        const store_operation_param&    param);
    
    /** USED ONLY FROM TESTCASES!. */
    static alloc_cache_t*       get_vol_alloc_cache(vid_t vid);

private:

    // This is used to enter and leave the io monitor under normal
    // circumstances.

    class auto_leave_t {
    private:
        xct_t *_x;
        check_compensated_op_nesting ccon;
        void on_entering();
        void on_leaving() const;
    public:
        auto_leave_t(): _x(xct()), ccon(_x, __LINE__, __FILE__) {\
                                       if(_x) on_entering(); }
        ~auto_leave_t()               { if(_x) on_leaving(); }
    };
    // This is used to enter and leave while grabbing the
    // checkpoint-serialization mutex, used on mount
    // and dismount, since a checkpoint records the mounted
    // volumes, it can't be fuzzy wrt mounts and dismounts.
    class auto_leave_and_trx_release_t; // forward decl - in sm_io.cpp
    
    static int                  vol_cnt;
    static vol_t*               vol[max_vols];
    static uint32_t    _msec_disk_delay;
    static lsn_t                _lastMountLSN;

private:

    static rc_t                 _mount_dev(const char* device, u_int& vol_cnt);
    static rc_t                 _dismount_dev(const char* device);
    static rc_t                 _get_lvid(const char* dev_name, lvid_t& lvid);
    
    static const char*          _dev_name(vid_t vid);
    static int                  _find(vid_t vid);

    typedef void *     lock_state;

    static vol_t*               _find_and_grab(
        vid_t                          vid, 
        lock_state*                    me,
        bool                           for_write
    ); 

    static rc_t                 _get_volume_quota(
        vid_t                             vid, 
        smksize_t&                        quota_KB, 
        smksize_t&                        quota_used_KB
        );
    
    static vid_t                _get_vid(const lvid_t& lvid);
    static lvid_t               _get_lvid(const vid_t vid);
    static rc_t                 _dismount(vid_t vid, bool flush);
    static rc_t                 _dismount_all(bool flush);
    static rc_t                 _create_store(
        vid_t                           vid, 
        store_flag_t                    flags,
        stid_t&                         stid);
    static rc_t                 _get_store_flags(
        const stid_t&                   stid,
        store_flag_t&                   flags);
    static rc_t                 _set_store_flags(
        const stid_t&                   stid,
        store_flag_t                    flags,
        bool                            sync_volume);
};




inline int
io_m::num_vols()
{
    return vol_cnt;
}

inline const char* 
io_m::dev_name(vid_t vid) 
{
    auto_leave_t enter;
    return _dev_name(vid);
}

inline lsn_t
io_m::GetLastMountLSN()
{
    return _lastMountLSN;
}

inline void
io_m::SetLastMountLSN(lsn_t theLSN)
{
    w_assert2(theLSN >= _lastMountLSN);
    _lastMountLSN = theLSN;
}


inline rc_t 
io_m::get_volume_quota(
        vid_t                             vid, 
        smksize_t&                    quota_KB, 
        smksize_t&                    quota_used_KB
        )
{
    auto_leave_t enter;
    return _get_volume_quota(vid, quota_KB, quota_used_KB);
}


inline rc_t 
io_m::dismount_all(bool flush)
{
    auto_leave_t enter;
    return _dismount_all(flush);
}

inline rc_t
io_m::set_store_flags(const stid_t& stid, store_flag_t flags, bool sync_volume)
{
    rc_t r;
    if (stid.store)  {
        auto_leave_t enter;
        r = _set_store_flags(stid, flags, sync_volume);
        // exchanges i/o mutex for volume mutex
    }
    return r;
}

#endif          /*</std-footer>*/
