#include "w_defines.h"

#define SM_SOURCE
#define IO_C

#ifdef __GNUG__
#pragma implementation
#endif

#include "sm_int_2.h"
#include "chkpt_serial.h"
#include "sm_du_stats.h"
#include "device.h"
#include "lock.h"
#include "xct.h"
#include "logrec.h"
#include "logdef_gen.cpp"
#include "crash.h"
#include "vol.h"
#include "bf_fixed.h"
#include <auto_release.h>

#include <new>
#include <map>
#include <set>
#include "sm_vtable_enum.h"

#ifdef EXPLICIT_TEMPLATE
template class vtable_func<vol_t>;
#endif


/*********************************************************************
 *
 *  Class static variables
 *
 *        _msec_disk_delay        : delay in performing I/O (for debugging)
 *        _mutex                  : make io_m a monitor
 *        vol_cnt                 : # volumes mounted in vol[]
 *        vol[]                   : array of volumes mounted
 *
 *********************************************************************/
uint32_t                  io_m::_msec_disk_delay = 0;
int                      io_m::vol_cnt = 0;
vol_t*                   io_m::vol[io_m::max_vols] = { 0 };
lsn_t                    io_m::_lastMountLSN = lsn_t::null;

// used for most io_m methods:
void
io_m::auto_leave_t::on_entering() 
{
    // The number of update threads _might_ include me 
    // NOTE: this business dates back to the days when we were trying
    // to allow multi-threaded xcts to have multiple updating threads.
    // But in the end, recovery issues precluded multiple updating threads.
    // The start_crit and stop_crit routines have become no-ops in xct_t.
    // The number of update threads is managed through the sm prologue
    // upon entering the ssm through the API.
    if(_x->update_threads()) _x->start_crit();
    else _x = NULL;
}
void
io_m::auto_leave_t::on_leaving() const
{
    _x->stop_crit();
}

// used for mount/dismount.  Same as auto_leave_t but
// also grabs/releases the checkpoint-serialization mutex.
// The order in which we want to do this precludes just
// inheriting from auto_leave_t.
class io_m::auto_leave_and_trx_release_t {
private:
    xct_t *_x;
    void on_entering() { 
        if(_x) _x->start_crit();
        else _x = NULL;
    }
    void on_leaving() const { _x->stop_crit(); }
public:
    auto_leave_and_trx_release_t() : _x(xct()) {
        chkpt_serial_m::trx_acquire();
        if(_x) on_entering();
    }
    ~auto_leave_and_trx_release_t() {
        if(_x) on_leaving(); 
        chkpt_serial_m::trx_release();
    }
};

io_m::io_m()
{
    _lastMountLSN = lsn_t::null;

}

/*********************************************************************
 *
 *  io_m::~io_m()
 *
 *  Destructor. Dismount all volumes.
 *
 *********************************************************************/
io_m::~io_m()
{
    W_COERCE(_dismount_all(shutdown_clean));
}

/*********************************************************************
 *
 *  io_m::find(vid)
 *
 *  Search and return the index for vid in vol[]. 
 *  If not found, return -1.
 *
 *********************************************************************/
int 
io_m::_find(vid_t vid)
{
    if (!vid) return -1;
    uint32_t i;
    for (i = 0; i < max_vols; i++)  {
        if (vol[i] && vol[i]->vid() == vid) break;
    }
    return (i >= max_vols) ? -1 : int(i);
}

inline vol_t * 
io_m::_find_and_grab(vid_t vid, lock_state* _me,
        bool for_write)
{
    if (!vid) {
        DBG(<<"vid " << vid);
        return 0;
    }
    vol_t** v = &vol[0];
    uint32_t i;
    for (i = 0; i < max_vols; i++, v++)  {
        if (*v) {
            if ((*v)->vid() == vid) break;
        }
    }
    if (i < max_vols) {
        w_assert1(*v);
        (*v)->assert_mutex_notmine(_me);
        (*v)->acquire_mutex(_me, for_write);
        w_assert3(*v && (*v)->vid() == vid);
        return *v;
    } else {
        return 0;
    }
}

vol_t* io_m::get_volume(vid_t vid)
{
    if (!vid) return NULL;
    for (uint32_t i = 0; i < max_vols; i++)  {
        if (vol[i] && vol[i]->vid() == vid) {
            return vol[i];
        }
    }
    return NULL;
}


/*********************************************************************
 *
 *  io_m::is_mounted(vid)
 *
 *  Return true if vid is mounted. False otherwise.
 *
 *********************************************************************/
bool
io_m::is_mounted(vid_t vid)
{
    auto_leave_t enter;
    return (_find(vid) >= 0);
}



/*********************************************************************
 *
 *  io_m::_dismount_all(flush)
 *
 *  Dismount all volumes mounted. If "flush" is true, then ask bf
 *  to flush dirty pages to disk. Otherwise, ask bf to simply
 *  invalidate the buffer pool.
 *
 *********************************************************************/
rc_t
io_m::_dismount_all(bool flush)
{
    for (int i = 0; i < max_vols; i++)  {
        if (vol[i])        {
            if(errlog) {
                errlog->clog 
                    << warning_prio
                    << "warning: volume " << vol[i]->vid() << " still mounted\n"
                << "         automatic dismount" << flushl;
            }
            W_DO(_dismount(vol[i]->vid(), flush));
        }
    }
    
    w_assert3(vol_cnt == 0);

    SET_TSTAT(vol_reads,0);
    SET_TSTAT(vol_writes,0);
    return RCOK;
}




/*********************************************************************
 *
 *  io_m::sync_all_disks()
 *
 *  Sync all volumes.
 *
 *********************************************************************/
rc_t
io_m::sync_all_disks()
{
    for (int i = 0; i < max_vols; i++) {
            if (_msec_disk_delay > 0)
                    me()->sleep(_msec_disk_delay, "io_m::sync_all_disks");
            if (vol[i])
                    vol[i]->sync();
    }
    return RCOK;
}




/*********************************************************************
 *
 *  io_m::_dev_name(vid)
 *
 *  Return the device name for volume vid if it is mounted. Otherwise,
 *  return NULL.
 *
 *********************************************************************/
const char* 
io_m::_dev_name(vid_t vid)
{
    int i = _find(vid);
    return i >= 0 ? vol[i]->devname() : 0;
}




/*********************************************************************
 *
 *  io_m::_is_mounted(dev_name)
 *
 *********************************************************************/
bool
io_m::is_mounted(const char* dev_name)
{
    auto_leave_t enter;
    return dev->is_mounted(dev_name);
}



/*********************************************************************
 *
 *  io_m::_mount_dev(dev_name, vol_cnt)
 *
 *********************************************************************/
rc_t
io_m::mount_dev(const char* dev_name, u_int& _vol_cnt)
{
    w_assert1(dev);
    auto_leave_t enter;
    FUNC(io_m::_mount_dev);

    volhdr_t vhdr;
    W_DO(vol_t::read_vhdr(dev_name, vhdr));

    DBG(<<"mount_dev " << dev_name << " read header done ");

        /* XXX possible bit-loss */
    device_hdr_s dev_hdr(vhdr.format_version(), 
                         vhdr.device_quota_KB(), vhdr.lvid());
    rc_t result = dev->mount(dev_name, dev_hdr, _vol_cnt);
    DBG(<<"mount_dev " << dev_name 
            << " vol_cnt " << vol_cnt
            << " returns " << result);
    return result;
}



/*********************************************************************
 *
 *  io_m::_dismount_dev(dev_name)
 *
 *********************************************************************/
rc_t
io_m::dismount_dev(const char* dev_name)
{
    auto_leave_t enter;
    return dev->dismount(dev_name);
}


/*********************************************************************
 *
 *  io_m::_dismount_all_dev()
 *
 *********************************************************************/
rc_t
io_m::dismount_all_dev()
{
    auto_leave_t enter;
    return dev->dismount_all();
}


/*********************************************************************
 *
 *  io_m::_list_devices(dev_list, devid_list, dev_cnt)
 *
 *********************************************************************/
rc_t
io_m::list_devices(
    const char**&         dev_list, 
    devid_t*&                 devid_list, 
    u_int&                 dev_cnt)
{
    auto_leave_t enter;
    return dev->list_devices(dev_list, devid_list, dev_cnt);
}


/*********************************************************************
 *
 *  io_m::_get_vid(lvid)
 *
 *********************************************************************/
vid_t 
io_m::_get_vid(const lvid_t& lvid)
{
    uint32_t i;
    for (i = 0; i < max_vols; i++)  {
        if (vol[i] && vol[i]->lvid() == lvid) break;
    }

    // egcs 1.1.1 croaks on this stmt:
    // return (i >= max_vols) ? vid_t::null : vol[i]->vid();
    if(i >= max_vols) {
        return vid_t::null;
    } else {
        return vol[i]->vid();
    }
}


/*********************************************************************
 *  io_m::_get_device_quota()
 *********************************************************************/
rc_t
io_m::get_device_quota(const char* device, smksize_t& quota_KB,
                        smksize_t& quota_used_KB)
{
    auto_leave_t enter;
    W_DO(dev->quota(device, quota_KB));

    lvid_t lvid;
    W_DO(_get_lvid(device, lvid));
    if (lvid == lvid_t::null) {
        // no device on volume
        quota_used_KB = 0;
    } else {
        smksize_t _dummy;
        W_DO(_get_volume_quota(_get_vid(lvid), quota_used_KB, _dummy));
    }
    return RCOK;
}


/*********************************************************************
 *
 *  io_m::_get_lvid(dev_name, lvid)
 *
 *********************************************************************/
rc_t
io_m::_get_lvid(const char* dev_name, lvid_t& lvid)
{
    if (!dev->is_mounted(dev_name)) return RC(eDEVNOTMOUNTED);
    uint32_t i;
    for (i = 0; i < max_vols; i++)  {
        if (vol[i] && (strcmp(vol[i]->devname(), dev_name) == 0) ) break;
    }
    lvid = (i >= max_vols) ? lvid_t::null : vol[i]->lvid();
    return RCOK;
}



/*********************************************************************
 *
 *  io_m::_get_vols(start, count, dname, vid, ret_cnt)
 *
 *  Fill up dname[] and vid[] starting from volumes mounted at index
 *  "start". "Count" indicates number of entries in dname and vid.
 *  Return number of entries filled in "ret_cnt".
 *
 *********************************************************************/
rc_t
io_m::get_vols(
    int         start, 
    int         count,
    char        **dname,
    vid_t       vid[], 
    int&        ret_cnt)
{
    auto_leave_t enter;
    ret_cnt = 0;
    w_assert1(start + count <= max_vols);
   
    /*
     *  i iterates over vol[] and j iterates over dname[] and vid[]
     */
    int i, j;
    for (i = start, j = 0; i < max_vols; i++)  {
        if (vol[i])  {
            w_assert0(j < count); // caller's programming error if we fail here
            vid[j] = vol[i]->vid();
            strncpy(dname[j], vol[i]->devname(), max_devname);
            j++;
        }
    }
    ret_cnt = j;
    return RCOK;
}



/*********************************************************************
 *
 *  io_m::_get_lvid(vid)
 *
 *********************************************************************/
lvid_t
io_m::get_lvid(const vid_t vid)
{
    auto_leave_t enter;
    int i = _find(vid);
    return (i >= max_vols) ? lvid_t::null : vol[i]->lvid();
}


/*********************************************************************
 *
 *  io_m::get_lvid(dev_name, lvid)
 *
 *********************************************************************/
rc_t
io_m::get_lvid(const char* dev_name, lvid_t& lvid)
{
    auto_leave_t enter;
    return _get_lvid(dev_name, lvid);
}

/*********************************************************************
 *
 *  io_m::mount(device, vid)
 *
 *  Mount "device" with vid "vid".
 *
 *********************************************************************/
rc_t
io_m::mount(const char* device, vid_t vid,
            const bool apply_fake_io_latency, const int fake_disk_latency)
{
    FUNC(io_m::mount);
    // grab chkpt_mutex to prevent mounts during chkpt
    // need to serialize writing dev_tab and mounts
    auto_leave_and_trx_release_t acquire_and_enter;
    DBG( << "_mount(name=" << device << ", vid=" << vid << ")");
    uint32_t i;
    for (i = 0; i < max_vols && vol[i]; i++) ;
    if (i >= max_vols) return RC(eNVOL);

    vol_t* v = new vol_t(apply_fake_io_latency,fake_disk_latency);  // deleted on dismount
    if (! v) return RC(eOUTOFMEMORY);

    // Get ready to roll back to here if we get an error between
    // here and ... where this scope is closed.
    AUTO_ROLLBACK_work

    w_rc_t rc = v->mount(device, vid);
    if (rc.is_error())  {
        delete v;
        return RC_AUGMENT(rc);
    }

    int j = _find(vid);
    if (j >= 0)  {
        W_DO( v->dismount(false) );
        delete v;
        return RC(eALREADYMOUNTED);
    }
    
    ++vol_cnt;

    w_assert9(vol[i] == 0);
    vol[i] = v;

    if (log && smlevel_0::logging_enabled)  {
        logrec_t* logrec = new logrec_t; //deleted at end of scope
        w_assert1(logrec);

        new (logrec) mount_vol_log(device, vid);
        logrec->fill_xct_attr(tid_t::null, GetLastMountLSN());
        lsn_t theLSN;
        W_DO( log->insert(*logrec, &theLSN) );

        DBG( << "mount_vol_log(" << device << ", vid=" << vid 
                << ") lsn=" << theLSN << " prevLSN=" << GetLastMountLSN());
        SetLastMountLSN(theLSN);

        delete logrec;
    }

    SSMTEST("io_m::_mount.1");
    DBG( << "_mount(name=" << device << ", vid=" << vid << ") done");

    work.ok();

    return RCOK;
}

/*********************************************************************
 *
 *  io_m::dismount(vid, flush)
 *  io_m::_dismount(vid, flush)
 *
 *  Dismount the volume "vid". "Flush" indicates whether to write
 *  dirty pages of the volume in bf to disk.
 *
 *********************************************************************/
rc_t
io_m::dismount(vid_t vid, bool flush)
{
    // grab chkpt_mutex to prevent dismounts during chkpt
    // need to serialize writing dev_tab and dismounts

    auto_leave_and_trx_release_t acquire_and_enter;
    return _dismount(vid, flush);
}


rc_t
io_m::_dismount(vid_t vid, bool flush)
{
    FUNC(io_m::_dismount);
    DBG( << "_dismount(" << "vid=" << vid << ")");
    int i = _find(vid); 
    if (i < 0) return RC(eBADVOL);

    W_COERCE(vol[i]->dismount(flush));

    if (log && smlevel_0::logging_enabled)  {
        logrec_t* logrec = new logrec_t; //deleted at end of scope
        w_assert1(logrec);

        new (logrec) dismount_vol_log(_dev_name(vid), vid);
        logrec->fill_xct_attr(tid_t::null, GetLastMountLSN());
        lsn_t theLSN;
        W_COERCE( log->insert(*logrec, &theLSN) );
        DBG( << "dismount_vol_log(" << _dev_name(vid) 
                << endl
                << ", vid=" << vid << ") lsn=" << theLSN << " prevLSN=" << GetLastMountLSN());;
        SetLastMountLSN(theLSN);

        delete logrec;
    }

    delete vol[i];
    vol[i] = 0;
    
    --vol_cnt;
  
    SSMTEST("io_m::_dismount.1");

    DBG( << "_dismount done.");
    return RCOK;
}


/*********************************************************************
 *
 *  io_m::_{enable,disable,set}_fake_disk_latency
 *
 *  Manipulate the disk latency of a volume
 *
 *********************************************************************/
rc_t
io_m::enable_fake_disk_latency(vid_t vid)
{
    auto_leave_t enter;
    int i = _find(vid);
    if (i < 0)  return RC(eBADVOL);

    vol[i]->enable_fake_disk_latency();
    return (RCOK);
}

rc_t
io_m::disable_fake_disk_latency(vid_t vid)
{
    auto_leave_t enter;
    int i = _find(vid);
    if (i < 0)  return RC(eBADVOL);

    vol[i]->disable_fake_disk_latency();
    return (RCOK);
}

rc_t
io_m::set_fake_disk_latency(vid_t vid, const int adelay)
{
    auto_leave_t enter;
    int i = _find(vid);
    if (i < 0)  return RC(eBADVOL);

    if (!vol[i]->set_fake_disk_latency(adelay)) 
      return RC(eBADVOL); // IP: should return more appropriate eror code

    return (RCOK);
}



/*********************************************************************
 *
 *  io_m::_get_volume_quota(vid, quota_KB, quota_used_KB)
 *
 *  Return the "capacity" of the volume and number of Kbytes "used"
 *  (allocated to extents)
 *
 *********************************************************************/
rc_t
io_m::_get_volume_quota(vid_t vid, smksize_t& quota_KB, smksize_t& quota_used_KB)
{
    int i = _find(vid);
    if (i < 0)  return RC(eBADVOL);

    quota_used_KB = vol[i]->num_used_pages()*(page_sz/1024);

    quota_KB = vol[i]->num_pages()*(page_sz/1024);
    return RCOK;
}



// WARNING: this MUST MATCH the code in vol.h
// The compiler requires this definition just for _find_and_grab.
// We could have vol.cpp #include sm_io.h but we really don't want that
// either.
typedef srwlock_t  VolumeLock;

// GRAB_*
// Since the statistics warrant, perhaps we've changed the
// volume mutex to an mcs_rwlock
#define GRAB_R \
    lock_state rme_node; \
    vol_t *v = _find_and_grab(volid, &rme_node, false); \
    if (!v)  return RC(eBADVOL); \
    auto_release_r_t<VolumeLock> release_on_return(v->vol_mutex());

#define GRAB_W \
    lock_state wme_node; \
    vol_t *v = _find_and_grab(volid, &wme_node, true); \
    if (!v)  return RC(eBADVOL); \
    auto_release_w_t<VolumeLock> release_on_return(v->vol_mutex());

/*********************************************************************
 *
 *  io_m::check_disk(vid)
 *
 *  Check the volume "vid".
 *
 *********************************************************************/
rc_t
io_m::check_disk(const vid_t &volid)
{
    auto_leave_t enter;
    GRAB_R;

    W_DO( v->check_disk() );

    return RCOK;
}


/*********************************************************************
 *
 *  io_m::_get_new_vid(vid)
 *
 *********************************************************************/
rc_t
io_m::get_new_vid(vid_t& vid)
{
    auto_leave_t enter;
    for (vid = vid_t(1); vid != vid_t::null; vid.incr_local()) {
        int i = _find(vid);
        if (i < 0) return RCOK;;
    }
    return RC(eNVOL);
}


vid_t
io_m::get_vid(const lvid_t& lvid)
{
    auto_leave_t enter;
    return _get_vid(lvid);
}



/*********************************************************************
 *
 *  io_m::read_page(pid, buf)
 * 
 *  Read the page "pid" on disk into "buf".
 *
 *********************************************************************/
rc_t io_m::read_page(const lpid_t& pid, page_s& buf) {
    int i = _find(pid.vol());
    if (i < 0) {
        return RC(eBADVOL);
    }
    W_DO( vol[i]->read_page(pid.page, buf) );
    return RCOK;
}



/*********************************************************************
 *
 *  io_m::write_many_pages(bufs, cnt)
 *
 *  Write "cnt" pages in "bufs" to disk.
 *
 *********************************************************************/
void 
io_m::write_many_pages(const page_s* bufs, int cnt)
{
    // NEVER acquire monitor to write page
    vid_t vid = bufs->pid.vol();
    int i = _find(vid);
    w_assert1(i >= 0);

    if (_msec_disk_delay > 0)
            me()->sleep(_msec_disk_delay, "io_m::write_many_pages");

#if W_DEBUG_LEVEL > 2
    {
        for (int j = 1; j < cnt; j++) {
            w_assert1(bufs[j].pid.page - 1 == bufs[j-1].pid.page);
            w_assert1(bufs[j].pid.vol() == vid);
        }
    }
#endif 

    W_COERCE( vol[i]->write_many_pages(bufs[0].pid.page, bufs, cnt) );
}

rc_t io_m::alloc_a_page(const stid_t &stid, lpid_t &pid) 
{
    FUNC(io_m::alloc_a_page);
    vid_t volid = stid.vol;
    int i = _find(volid);
    if (i < 0) return RC(eBADVOL);
    vol_t *v = vol[i];
    W_DO(v->alloc_a_page(stid, pid));
#if W_DEBUG_LEVEL > 2
    cout << "allocated page:" << pid << endl;
#endif // W_DEBUG_LEVEL > 2
    return RCOK;
}

rc_t io_m::sx_alloc_a_page(const stid_t &stid, lpid_t &pid)
{
    sys_xct_section_t sxs (true); // this transaction will output only one log!
    W_DO(sxs.check_error_on_start());
    rc_t ret = alloc_a_page(stid, pid);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}

rc_t io_m::alloc_consecutive_pages(const stid_t &stid, size_t page_count, lpid_t &pid_begin)
{
    FUNC(io_m::alloc_consecutive_pages);
    vid_t volid = stid.vol;
    int i = _find(volid);
    if (i < 0) return RC(eBADVOL);
    vol_t *v = vol[i];
    W_DO(v->alloc_consecutive_pages(stid, page_count, pid_begin));
#if W_DEBUG_LEVEL > 2
    cout << "allocated consecutive pages: from " << pid_begin << " count:" << page_count << endl;
#endif // W_DEBUG_LEVEL > 2
    return RCOK;
}

rc_t io_m::sx_alloc_consecutive_pages(const stid_t &stid, size_t page_count, lpid_t &pid_begin)
{
    // as it might span multiple alloc_p, this might emit multiple logs.
    // (however, could be ssx later when we can have multiple pages covered by a single log)
    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());
    rc_t ret = alloc_consecutive_pages(stid, page_count, pid_begin);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}


rc_t io_m::dealloc_a_page(const lpid_t& pid)
{
    FUNC(io_m::dealloc_a_page);
    vid_t volid = pid.vol();
    int i = _find(volid);
    if (i < 0) return RC(eBADVOL);
    vol_t *v = vol[i];
    W_DO(v->free_page(pid));
#if W_DEBUG_LEVEL > 2
    cout << "deallocated page:" << pid << endl;
#endif // W_DEBUG_LEVEL > 2
    return RCOK;
}

rc_t io_m::sx_dealloc_a_page(const lpid_t &pid)
{
    sys_xct_section_t sxs (true); // this transaction will output only one log!
    W_DO(sxs.check_error_on_start());
    rc_t ret = dealloc_a_page(pid);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}

alloc_cache_t* io_m::get_vol_alloc_cache(vid_t vid)
{
    int i = _find(vid);
    if (i < 0) {
        w_assert1(false);
        return NULL;
    }
    vol_t *v = vol[i];
    return v->get_alloc_cache();
}

rc_t io_m::redo_alloc_a_page(vid_t volid, shpid_t pid)
{
    int i = _find(volid);
    if (i < 0) return RC(eBADVOL);
    vol_t *v = vol[i];
    W_DO(v->redo_alloc_a_page(pid));
    return RCOK;
}
rc_t io_m::redo_alloc_consecutive_pages(vid_t volid, size_t page_count, shpid_t pid_begin)
{
    int i = _find(volid);
    if (i < 0) return RC(eBADVOL);
    vol_t *v = vol[i];
    W_DO(v->redo_alloc_consecutive_pages(pid_begin, page_count));
    return RCOK;
}
rc_t io_m::redo_dealloc_a_page(vid_t volid, shpid_t pid)
{
    int i = _find(volid);
    if (i < 0) return RC(eBADVOL);
    vol_t *v = vol[i];
    W_DO(v->redo_free_page(pid));
    return RCOK;
}

/*********************************************************************
 *
 *  io_m::_set_store_flags(stid, flags)
 *
 *  Set the store flag for "stid" to "flags".
 *
 *********************************************************************/
rc_t
io_m::_set_store_flags(const stid_t& stid, store_flag_t flags, bool sync_volume)
{
    FUNC(io_m::_set_store_flags);
    vid_t volid = stid.vol;
    GRAB_W;

    W_DO( v->set_store_flags(stid.store, flags, sync_volume) );
    if ((flags & st_insert_file) && !smlevel_0::in_recovery())  {
        xct()->AddLoadStore(stid);
    }
    return RCOK;
}


/*********************************************************************
 *
 *  io_m::_get_store_flags(stid, flags, ok_if_deleting)
 *
 *  Get the store flag for "stid" in "flags".
 *
 *********************************************************************/
rc_t
io_m::get_store_flags(const stid_t& stid, store_flag_t& flags, 
        bool ok_if_deleting /* = false */)
{
    // Called from bf- can't use monitor mutex because
    // that could cause us to re-enter the I/O layer
    // (embedded calls)
    //
    // v->get_store_flags grabs the mutex
    int i = _find(stid.vol);
    if (i < 0) return RC(eBADVOL);
    vol_t *v = vol[i];

    if (!v)  W_FATAL(eINTERNAL);
    W_DO( v->get_store_flags(stid.store, flags, ok_if_deleting) );
    return RCOK;
}



/*********************************************************************
 *
 *  io_m::create_store(volid, flags, stid, first_ext)
 *  io_m::_create_store(volid, flags, stid, first_ext)
 *
 *  Create a store on "volid" and return its id in "stid". The store
 *  flag for the store is set to "flags". "First_ext" is a hint to
 *  vol_t to allocate the first extent for the store at "first_ext"
 *  if possible.
 *
 *********************************************************************/
rc_t
io_m::create_store(
    vid_t                       volid, 
    store_flag_t                flags,
    stid_t&                     stid)
{
    rc_t r; 
    { 
        auto_leave_t enter;
    
        r = _create_store(volid, flags, stid);
        // replaced io mutex with volume mutex
    }

    if(r.is_error()) return r;

    /* load and insert stores get converted to regular on commit */
    if (flags & st_load_file || flags & st_insert_file)  {
        xct()->AddLoadStore(stid);
    }

    return r;
}

rc_t
io_m::_create_store(
    vid_t                       volid, 
    store_flag_t                flags,
    stid_t&                     stid)
{
    FUNC(io_m::create_store);
    w_assert1(flags);

    GRAB_W;

    //  Find a free store slot
    stid.vol = volid;
    W_DO(v->find_free_store(stid.store));
    W_DO(v->alloc_store(stid.store, flags) );
    return RCOK;
}




/*********************************************************************
 *
 *  io_m::_is_valid_store(stid)
 *
 *  Return true if store "stid" is valid. False otherwise.
 *
 *********************************************************************/
bool
io_m::is_valid_store(const stid_t& stid)
{
    auto_leave_t enter;
    vid_t volid = stid.vol;

    // essentially GRAB_R but doesn't return RC
    lock_state rme_node ;
    vol_t *v = _find_and_grab(volid, &rme_node, false); 
    if (!v)  return false;
    auto_release_r_t<VolumeLock> release_on_return(v->vol_mutex());

    if ( ! v->is_valid_store(stid.store) )   {
        return false;
    }
    
    return v->is_alloc_store(stid.store);
}

shpid_t io_m::get_root(const stid_t& stid)
{
    auto_leave_t enter;
    vid_t volid = stid.vol;

    lock_state rme_node ;
    vol_t *v = _find_and_grab(volid, &rme_node, false); 
    if (!v)  return false;
    auto_release_r_t<VolumeLock> release_on_return(v->vol_mutex());
    return v->get_store_root(stid.store);
}
rc_t io_m::set_root(const stid_t& stid, shpid_t root_pid)
{
    auto_leave_t enter;
    vid_t volid = stid.vol;

    lock_state rme_node ;
    vol_t *v = _find_and_grab(volid, &rme_node, false); 
    w_assert1(v);
    auto_release_r_t<VolumeLock> release_on_return(v->vol_mutex());
    return v->set_store_root(stid.store, root_pid);
}

/*********************************************************************
 *
 *  io_m::get_volume_meta_stats(vid, volume_stats)
 *
 *  Returns in volume_stats the statistics calculated from the volumes
 *  meta information.
 *
 *********************************************************************/
rc_t
io_m::get_volume_meta_stats(vid_t volid, SmVolumeMetaStats& volume_stats)
{
    auto_leave_t enter;
    FUNC(io_m::_get_volume_meta_stats);
    GRAB_R;

    W_DO( v->get_volume_meta_stats(volume_stats) );
    return RCOK;
}

/*********************************************************************
 *
 *  io_m::get_store_meta_stats_batch(stid_t, stats)
 *
 *  Returns the pages usage statistics for the given store.
 *
 *********************************************************************/
rc_t
io_m::get_store_meta_stats(stid_t stid, SmStoreMetaStats& mapping)
{
    vid_t volid = stid.vol;
    GRAB_R;
    W_DO( v->get_store_meta_stats(stid.store, mapping) );
    return RCOK;
}

/*********************************************************************
 *
 *, extnum  io_m::get_du_statistics()         DU DF
 *
 *********************************************************************/
rc_t io_m::get_du_statistics(vid_t volid, volume_hdr_stats_t& _stats, bool audit)
{
    auto_leave_t enter;
    GRAB_R;
    W_DO( v->get_du_statistics(_stats, audit) );

    return RCOK;
}

rc_t
io_m::store_operation(vid_t volid, const store_operation_param& param)
{
    FUNC(io_m::store_operation);
    auto_leave_t enter;

    GRAB_W;

    w_assert3(v->vid() == volid);

    W_DO( v->store_operation(param) );

    return RCOK;
}

rc_t io_m::flush_all_fixed_buffer ()
{
    for (uint32_t i = 0; i < max_vols; i++)  {
        if (vol[i]) {
            W_DO(vol[i]->get_fixed_bf()->flush());
        }
    }
    return RCOK;
}
rc_t io_m::flush_vol_fixed_buffer (vid_t vid)
{
    int i = _find(vid);
    if (i < 0) return RC(eBADVOL);
    vol_t *v = vol[i];
    W_DO(v->get_fixed_bf()->flush());
    return RCOK;
}

ostream& operator<<(ostream& o, const store_operation_param& param)
{
    o << "snum="    << param.snum()
      << ", op="    << param.op();
    
    switch (param.op())  {
        case smlevel_0::t_delete_store:
            break;
        case smlevel_0::t_create_store:
            o << ", flags="        << param.new_store_flags();
            break;
        case smlevel_0::t_set_deleting:
            o << ", newValue="        << param.new_deleting_value()
              << ", oldValue="        << param.old_deleting_value();
            break;
        case smlevel_0::t_set_store_flags:
            o << ", newFlags="        << param.new_store_flags()
              << ", oldFlags="        << param.old_store_flags();
            break;
        case smlevel_0::t_set_root:
            o << ", ext="        << param.root();
            break;
    }

    return o;
}
