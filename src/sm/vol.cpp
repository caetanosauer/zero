/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define VOL_C
#ifdef __GNUG__
#   pragma implementation
#endif


#include "w_stream.h"
#include <sys/types.h>
#include "sm_int_1.h"
#include "stnode_p.h"
#include "vol.h"
#include "sm_du_stats.h"
#include "crash.h"

#include "sm_vtable_enum.h"
#include "st_error_enum_gen.h"

#include "bf_fixed.h"
#include "alloc_cache.h"
#include "bf_tree.h"

#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_t<page_s>;
template class w_auto_delete_array_t<stnode_t>;
#endif

/** sector_size : reserved space at beginning of volume. */
static const int sector_size = 512; 

/*
Volume layout:
   volume header 
   alloc_p pages -- Starts on page 1.
   stnode_p -- only one page
   data pages -- rest of volume

   alloc_p pages are bitmaps indicating which of its pages are allocated.
   alloc_p pages are read and modified without any locks in any time.
   It's supposed to be extremely fast to allocate/deallocate pages
   unlike the original Shore-MT code. See jira ticket:72 "fix extent management" (originally trac ticket:74) for more details.
*/

/*
 * STORES:
 * Each volume contains a few stores that are "overhead":
 * 0 -- is reserved for the page-allocation and the store map
 * 1 -- directory (see dir.cpp)
 * 2 -- root index (see sm.cpp)
 *
 * After that, for each file created, 2 stores are used, one for
 * small objects, one for large objects.
 *
 * Each index(btree) uses one store. 
 */
vol_t::vol_t(const bool apply_fake_io_latency, const int fake_disk_latency) 
             : _unix_fd(-1),
               _apply_fake_disk_latency(apply_fake_io_latency), 
               _fake_disk_latency(fake_disk_latency),
               _alloc_cache(NULL), _stnode_cache(NULL), _fixed_bf(NULL)
{}

vol_t::~vol_t() { 
    shutdown();
    w_assert1(_unix_fd == -1); 
}

void vol_t::clear_caches() {
    if (_alloc_cache) {
        delete _alloc_cache;
        _alloc_cache = NULL;
    }
    if (_stnode_cache) {
        delete _stnode_cache;
        _stnode_cache = NULL;
    }
    if (_fixed_bf) {
        delete _fixed_bf;
        _fixed_bf = NULL;
    }
}


void vol_t::shutdown() {
    clear_caches();
}

rc_t vol_t::sync()
{
    smthread_t* t = me();
    W_DO_MSG(t->fsync(_unix_fd), << "volume id=" << vid());
    return RCOK;
}

rc_t
vol_t::check_raw_device(const char* devname, bool& raw)
{
    w_rc_t        e;
    int        fd;

    raw = false;

    /* XXX should add a stat() to sthread for instances such as this */
    e = me()->open(devname, smthread_t::OPEN_RDONLY, 0, fd);

    if (!e.is_error()) {
            e = me()->fisraw(fd, raw);
            W_IGNORE(me()->close(fd));
    }

    return e;
}
    
rc_t vol_t::mount(const char* devname, vid_t vid)
{
    if (_unix_fd >= 0) return RC(eALREADYMOUNTED);

    /*
     *  Save the device name
     */
    w_assert1(strlen(devname) < sizeof(_devname));
    strcpy(_devname, devname);

    /*
     *  Check if device is raw, and open it.
     */
    W_DO(check_raw_device(devname, _is_raw));

    w_rc_t e;
    int        open_flags = smthread_t::OPEN_RDWR;
    {
            char *s = getenv("SM_VOL_RAW");
        if (s && s[0] && atoi(s) > 0)
                open_flags |= smthread_t::OPEN_RAW;
        else if (s && s[0] && atoi(s) == 0)
                open_flags &= ~smthread_t::OPEN_RAW;
    }
    e = me()->open(devname, open_flags, 0666, _unix_fd);
    if (e.is_error()) {
        _unix_fd = -1;
        return e;
    }

    //  Read the volume header on the device
    volhdr_t vhdr;
    {
        rc_t rc = read_vhdr(_devname, vhdr);
        if (rc.is_error())  {
            W_DO_MSG(me()->close(_unix_fd), << "volume id=" << vid);
            _unix_fd = -1;
            return RC_AUGMENT(rc);
        }
    }
    //  Save info on the device
    _vid = vid;
    
#if W_DEBUG_LEVEL > 4
    w_ostrstream_buf sbuf(64);                /* XXX magic number */
    sbuf << "vol(vid=" << (int) _vid.vol << ")" << ends;
    //_mutex.rename("m:", sbuf.c_str());
    /* XXX how about restoring the old mutex name when done? */
#endif 
    _lvid = vhdr.lvid();
    _num_pages =  vhdr.num_pages();
    _apid = lpid_t(vid, 0, vhdr.apid()); // 0 if no volumes formatted yet
    _spid = lpid_t(vid, 0, vhdr.spid()); // 0 if no volumes formatted yet
    _hdr_pages = vhdr.hdr_pages(); // 0 if no volumes formatted yet
    
    clear_caches();
    _fixed_bf = new bf_fixed_m();
    w_assert1(_fixed_bf);
    W_DO(_fixed_bf->init(this, _unix_fd, _num_pages));
    _first_data_pageid = _fixed_bf->get_page_cnt() + 1; // +1 for volume header

    _alloc_cache = new alloc_cache_t(_vid, _fixed_bf);
    w_assert1(_alloc_cache);
    W_DO(_alloc_cache->load_by_scan(_num_pages));

    _stnode_cache = new stnode_cache_t(_vid, _fixed_bf);
    w_assert1(_stnode_cache);

    W_DO( bf->install_volume(this));

    return RCOK;
}

rc_t
vol_t::dismount(bool flush)
{
    DBG(<<" vol_t::dismount flush=" << flush);

    INC_TSTAT(vol_cache_clears);

    w_assert1(_unix_fd >= 0);
    W_DO(bf->uninstall_volume(_vid.vol));

    /*
     *  Close the device
     */
    w_rc_t e;
    e = me()->close(_unix_fd);
    if (e.is_error())
            return e;

    _unix_fd = -1;
    
    clear_caches();

    return RCOK;
}

rc_t vol_t::check_disk()
{
    FUNC(vol_t::check_disk);
    volhdr_t vhdr;
    W_DO( read_vhdr(_devname, vhdr));
    smlevel_0::errlog->clog << info_prio << "vol_t::check_disk()\n";
    smlevel_0::errlog->clog << info_prio 
        << "\tvolid      : " << vhdr.lvid() << flushl;
    smlevel_0::errlog->clog << info_prio 
        << "\tnum_pages   : " << vhdr.num_pages() << flushl;
    smlevel_0::errlog->clog << info_prio 
        << "\thdr_pages   : " << vhdr.hdr_pages() << flushl;

    smlevel_0::errlog->clog << info_prio 
        << "\tstore  #   flags   status: [ extent-list ]" << "." << endl;
    for (shpid_t i = 1; i < stnode_p::max; i++)  {
        stnode_t stnode;
        _stnode_cache->get_stnode(i, stnode);
        if (stnode.root)  {
            smlevel_0::errlog->clog << info_prio 
                << "\tstore " << i << "(root=" << stnode.root << ")\t flags " << stnode.flags;
            if(stnode.deleting) {
                smlevel_0::errlog->clog << info_prio 
                << " is deleting: ";
            } else {
                smlevel_0::errlog->clog << info_prio 
                << " is active: ";
            }
            smlevel_0::errlog->clog << info_prio << " ]" << flushl;
        }
    }

    return RCOK;
}


rc_t vol_t::alloc_a_page(const stid_t &stid, lpid_t &pid)
{
    FUNC(vol_t::alloc_a_page);
    w_assert1(_alloc_cache);
    shpid_t shpid;
    W_DO(_alloc_cache->allocate_one_page(shpid));
    pid = lpid_t (stid, shpid);
    return RCOK;
}
rc_t vol_t::alloc_consecutive_pages(const stid_t &stid, size_t page_count, lpid_t &pid_begin)
{
    FUNC(vol_t::alloc_consecutive_pages);
    w_assert1(_alloc_cache);
    shpid_t shpid;
    W_DO(_alloc_cache->allocate_consecutive_pages(shpid, page_count));
    pid_begin = lpid_t (stid, shpid);
    return RCOK;
}

rc_t vol_t::store_operation(const store_operation_param& param)
{
    w_assert1(param.snum() < stnode_p::max);
    w_assert1(_stnode_cache);
    W_DO(_stnode_cache->store_operation(param));
    return RCOK;
}

rc_t vol_t::free_page(const lpid_t& pid)
{
    FUNC(free_page);
    w_assert1(_alloc_cache);
    W_DO(_alloc_cache->deallocate_one_page(pid.page));
    return RCOK;
}

rc_t vol_t::find_free_store(snum_t& snum)
{
    FUNC(find_free_store);
    w_assert1(_stnode_cache);
    snum = _stnode_cache->get_min_unused_store_id();
    if (snum >= stnode_p::max) {
        W_RETURN_RC_MSG(eOUTOFSPACE, << "volume id = " << _vid);
    }
    return RCOK;
}

rc_t vol_t::redo_alloc_a_page(shpid_t pid)
{
    w_assert1(_alloc_cache);
    W_DO(_alloc_cache->redo_allocate_one_page(pid));
    return RCOK;
}
rc_t vol_t::redo_alloc_consecutive_pages(size_t page_count, shpid_t pid_begin)
{
    w_assert1(_alloc_cache);
    W_DO(_alloc_cache->redo_allocate_consecutive_pages(pid_begin, page_count));
    return RCOK;
}
rc_t vol_t::redo_free_page(shpid_t pid)
{
    w_assert1(_alloc_cache);
    W_DO(_alloc_cache->redo_deallocate_one_page(pid));
    return RCOK;
}

/*********************************************************************
 *
 *  vol_t::set_store_flags(snum, flags, sync_volume)
 *
 *  Set the store flag to "flags".  sync the volume if sync_volume is
 *  true and flags is regular.
 *
 *********************************************************************/
rc_t
vol_t::set_store_flags(snum_t snum, store_flag_t flags, bool sync_volume)
{
    w_assert2(flags & st_regular
           || flags & st_tmp
           || flags & st_insert_file);

    if (snum == 0 || !is_valid_store(snum))    {
        DBG(<<"set_store_flags: BADSTID");
        return RC(eBADSTID);
    }

    store_operation_param param(snum, t_set_store_flags, flags);
    W_DO( store_operation(param) );

    return RCOK;
}

    
/*********************************************************************
 *
 *  vol_t::get_store_flags(snum, flags, bool ok_if_deleting)
 *
 *  Return the store flags for "snum" in "flags".
 *
 *********************************************************************/
rc_t
vol_t::get_store_flags(snum_t snum, store_flag_t& flags, bool ok_if_deleting)
{
    FUNC(get_store_flags);
    if (!is_valid_store(snum))    {
        DBG(<<"get_store_flags: BADSTID");
        return RC(eBADSTID);
    }

    if (snum == 0)  {
        flags = smlevel_0::st_bad;
        return RCOK;
    }

    stnode_t stnode;
    _stnode_cache->get_stnode(snum, stnode);

    /*
     *  Make sure the store for this page is marked as allocated.
     *  However, this is not necessarily true during recovery-redo
     *  since it depends on the order pages made it to disk before
     *  a crash.
     */
    if (!in_recovery()) {
        if (!stnode.root) {
            DBG(<<"get_store_flags: BADSTID for snum " << snum);
            return RC(eBADSTID);
        }
        if ( (!ok_if_deleting) &&  stnode.deleting) {
            DBG(<<"get_store_flags: BADSTID for snum " << snum);
            return RC(eBADSTID);
        }
    }

    flags = (store_flag_t)stnode.flags;

    return RCOK;
}



/*********************************************************************
 *
 *  vol_t::alloc_store(snum, flags)
 *
 *  Allocate a store at "snum" with attribute "flags".
 *
 *********************************************************************/
rc_t
vol_t::alloc_store(snum_t snum, store_flag_t flags)
{
    FUNC(alloc_store);
    w_assert9(flags & st_regular
           || flags & st_tmp
           || flags & st_insert_file);

    if (!is_valid_store(snum))    {
        DBG(<<"alloc_store: BADSTID");
        return RC(eBADSTID);
    }
    
    // Fill in the store node
    store_operation_param param(snum, t_create_store, flags);
    W_DO( store_operation(param) );

    DBGTHRD(<<"alloc_store done");
    return RCOK;
}

rc_t vol_t::set_store_root(snum_t snum, shpid_t root)
{
    if (!is_valid_store(snum))    {
        DBG(<<"set_store_root: BADSTID");
        return RC(eBADSTID);
    }

    store_operation_param param(snum, t_set_root, root);
    W_DO( store_operation(param) );

    return RCOK;
}

/*********************************************************************
 *
 *  vol_t::get_volume_meta_stats(volume_stats)
 *
 *  Collects simple space utilization statistics on the volume.
 *  Includes number of pages, number of pages reserved by stores,
 *  number of pages allocated to stores, number of available stores,
 *  number of stores in use.
 *
 *********************************************************************/
rc_t
vol_t::get_volume_meta_stats(SmVolumeMetaStats& volume_stats)
{
    volume_stats.numStores = stnode_p::max;

    {
        volume_stats.numAllocStores = 0;
        for (slotid_t i = 1; i < stnode_p::max; ++i) {
            
            if (_stnode_cache->get_root_pid(i) != 0) {
                ++volume_stats.numAllocStores;
            }
        }
    } // unpins stnode_p

    volume_stats.numPages = _num_pages;
    volume_stats.numSystemPages = _hdr_pages;

    return RCOK;
}

/*********************************************************************
 *
 *  vol_t::get_store_meta_stats(snum, storeStats)
 *
 *  Collects simple statistics on the store requested.  Includes number
 *  of pages reserved by the stores and the number of pages allocated
 *  to the stores.
 *
 *********************************************************************/
rc_t
vol_t::get_store_meta_stats(snum_t, SmStoreMetaStats&)
{
    // TODO this needs to traverse the tree!

    return RCOK;
}

bool vol_t::is_alloc_store(snum_t f) const
{
    FUNC(is_alloc_store);
    return _stnode_cache->get_root_pid(f) != 0;
}
shpid_t vol_t::get_store_root(snum_t f) const
{
    FUNC(get_root);
    return _stnode_cache->get_root_pid(f);
}

/*********************************************************************
 *
 *  vol_t::fake_disk_latency(long)
 *
 *  Impose a fake IO penalty. Assume that each batch of pages
 *  requires exactly one seek. A real system might perform better
 *  due to sequential access, or might be worse because the pages
 *  in the batch are not actually contiguous. Close enough...
 *
 *********************************************************************/
void 
vol_t::fake_disk_latency(long start) 
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

// IP: assuming no concurrent requests. No thread-safe.
void                        
vol_t::enable_fake_disk_latency(void)
{
  _apply_fake_disk_latency = true;
}

void                        
vol_t::disable_fake_disk_latency(void)
{
  _apply_fake_disk_latency = false;
}

bool                        
vol_t::set_fake_disk_latency(const int adelay)
{
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
 *  Read the page at "pnum" of the volume into the buffer "page".
 *
 *********************************************************************/
rc_t
vol_t::read_page(shpid_t pnum, page_s& page)
{
    w_assert1(pnum > 0 && pnum < (shpid_t)(_num_pages));
    fileoff_t offset = fileoff_t(pnum) * sizeof(page);

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
    if(err.err_num() == sthread_t::stSHORTIO && err.sys_err_num() == 0) {
        // read past end of OS file. return all zeros
        memset(&page, 0, sizeof(page));
    } else {
        W_COERCE_MSG(err, << "volume id=" << vid()
              << " err_num " << err.err_num()
              << " sys_err_num " << err.sys_err_num()
              );
    }
    return RCOK;
}




/*********************************************************************
 *
 *  vol_t::write_page(pnum, page)
 *
 *  Write the buffer "page" to the page at "pnum" of the volume.
 *
 *********************************************************************/
rc_t
vol_t::write_page(shpid_t pnum, page_s& page)
{
  return write_many_pages(pnum, &page, 1);
}


/*********************************************************************
 *
 *  vol_t::write_many_pages(pnum, pages, cnt)
 *
 *  Write "cnt" buffers in "pages" to pages starting at "pnum"
 *  of the volume.
 *
 *********************************************************************/
rc_t
vol_t::write_many_pages(shpid_t pnum, const page_s* const pages, int cnt)
{
    w_assert1(pnum > 0 && pnum < (shpid_t)(_num_pages));
    w_assert1(cnt > 0);
    fileoff_t offset = fileoff_t(pnum) * sizeof(page_s);

    smthread_t* t = me();

    long start = 0;
    if(_apply_fake_disk_latency) start = gethrtime();

    // do the actual write now
    W_COERCE_MSG(t->pwrite(_unix_fd, pages, sizeof(page_s)*cnt, offset), << "volume id=" << vid());
    
    fake_disk_latency(start);    
    ADD_TSTAT(vol_blks_written, cnt);
    INC_TSTAT(vol_writes);

    return RCOK;
}

const char* vol_t::prolog[] = {
    "%% SHORE VOLUME VERSION ",
    "%% device quota(KB)  : ",
    "%% volume_id         : ",
    "%% ext_size          : ",
    "%% num_exts          : ",
    "%% hdr_exts          : ",
    "%% hdr_pages          : ",
    "%% epid              : ",
    "%% spid              : ",
    "%% page_sz           : "
};

rc_t
vol_t::format_dev(
    const char* devname,
    shpid_t num_pages,
    bool force)
{
    FUNC(vol_t::format_dev);

    // WHOLE FUNCTION is a critical section
    xct_log_switch_t log_off(OFF);
    
    DBG( << "formating device " << devname);
    int flags = smthread_t::OPEN_CREATE | smthread_t::OPEN_RDWR
            | (force ? smthread_t::OPEN_TRUNC : smthread_t::OPEN_EXCL);
    int fd;
    w_rc_t e;
    e = me()->open(devname, flags, 0666, fd);
    if (e.is_error()) {
        DBG(<<" open " << devname <<  " failed " << e);
        return e;
    }
    
    volhdr_t vhdr;
    vhdr.set_format_version(volume_format_version);
    vhdr.set_device_quota_KB(num_pages*(page_sz/1024));
    vhdr.set_num_pages(num_pages); // # extents on volume
    vhdr.set_hdr_pages(0); // hdr pages
    vhdr.set_apid(0); // first extent-map page
    vhdr.set_spid(0); // first store-map page
    vhdr.set_page_sz(page_sz);
   
    // determine if the volume is on a raw device
    bool raw;
    rc_t rc = me()->fisraw(fd, raw);
    if (rc.is_error()) {
        W_IGNORE(me()->close(fd));
        DBG(<<" fisraw " << fd << " failed " << rc);
        return RC_AUGMENT(rc);
    }
    rc = write_vhdr(fd, vhdr, raw);
    if (rc.is_error())  {
        W_IGNORE(me()->close(fd));
        DBG(<<" write_vhdr  fd " << fd << " failed " << rc);
        return RC_AUGMENT(rc);
    }

    W_COERCE_MSG(me()->close(fd), << "device name=" << devname);

    DBG(<<" format " << devname << " done ");
    return RCOK;
}



/*********************************************************************
 *
 *  vol_t::format_vol(devname, lvid, num_pages, skip_raw_init, 
 *                    apply_fake_io_latency, fake_disk_latency)
 *
 *  Format the volume "devname" for long volume id "lvid" and
 *  a size of "num_pages". "Skip_raw_init" indicates whether to
 *  zero out all pages in the volume during format.
 *
 *********************************************************************/
rc_t
vol_t::format_vol(
    const char*         devname,
    lvid_t              lvid,
    vid_t               vid,
    shpid_t             num_pages,
    bool                skip_raw_init)
{
    FUNC(vol_t::format_vol);

    /*
     *  No log needed.
     *  WHOLE FUNCTION is a critical section
     */
    xct_log_switch_t log_off(OFF);

    /*
     *  Read the volume header
     */
    volhdr_t vhdr;
    W_DO(read_vhdr(devname, vhdr));
    if (vhdr.lvid() == lvid) return RC(eVOLEXISTS);
    if (vhdr.lvid() != lvid_t::null) return RC(eDEVICEVOLFULL); 

    /* XXX possible bit loss */
    uint quota_pages = (uint) (vhdr.device_quota_KB()/(page_sz/1024));

    if (num_pages > quota_pages) {
        return RC(eVOLTOOLARGE);
    }

    /*
     *  Determine if the volume is on a raw device
     */
    bool raw;
    rc_t rc = check_raw_device(devname, raw);
    if (rc.is_error())  {
        return RC_AUGMENT(rc);
    }


    DBG( << "formating volume " << lvid << " <"
         << devname << ">" );
    int flags = smthread_t::OPEN_RDWR;
    if (!raw) flags |= smthread_t::OPEN_TRUNC;
    int fd;
    rc = me()->open(devname, flags, 0666, fd);
    if (rc.is_error()) {
        DBG(<<" open " << devname << " failed " << rc );
        return rc;
    }
    
    shpid_t alloc_pages = num_pages / alloc_p::bits_held + 1; // # alloc_p pages
    shpid_t hdr_pages = alloc_pages + 1 + 1; // +1 for stnode_p, +1 for volume header

    lpid_t apid (vid, 0 , 1);
    lpid_t spid (vid, 0 , 1 + alloc_pages);

    /*
     *  Set up the volume header
     */
    vhdr.set_format_version(volume_format_version);
    vhdr.set_lvid(lvid);
    vhdr.set_num_pages(num_pages);
    vhdr.set_hdr_pages(hdr_pages);
    vhdr.set_apid(apid.page);
    vhdr.set_spid(spid.page);
    vhdr.set_page_sz(page_sz);
   
    /*
     *  Write volume header
     */
    rc = write_vhdr(fd, vhdr, raw);
    if (rc.is_error())  {
        W_IGNORE(me()->close(fd));
        return RC_AUGMENT(rc);
    }

    /*
     *  Skip first page ... seek to first info page.
     *
     * FRJ: this seek is safe because no other thread can access the
     * file descriptor we just opened.
     */    
    rc = me()->lseek(fd, sizeof(page_s), sthread_t::SEEK_AT_SET);
    if (rc.is_error()) {
        W_IGNORE(me()->close(fd));
        return rc;
    }

    {
        page_s buf;
#ifdef ZERO_INIT
        // zero out data portion of page to keep purify/valgrind happy.
        // Unfortunately, this isn't enough, as the format below
        // seems to assign an uninit value.
        memset(&buf, '\0', sizeof(buf));
#endif

        //  Format alloc_p pages
        {
            for (apid.page = 1; apid.page < alloc_pages + 1; ++apid.page)  {
                alloc_p ap(&buf);
                W_COERCE( ap.format(apid));
                // set bits for the header pages
                if (apid.page == 1) {
                    for (shpid_t hdr_pid = 0; hdr_pid < hdr_pages; ++hdr_pid) {
                        ap.set_bit(hdr_pid);
                    }
                }
                alloc_page& page (*ap._page);
                w_assert9(&buf == &page);
                w_assert1(page.pid.vol() == vid);

                rc = me()->write(fd, &page, sizeof(page));
                if (rc.is_error()) {
                    W_IGNORE(me()->close(fd));
                    return rc;
                }
            }
        }
        DBG(<<" done formatting extent region");

        // Format stnode_p
        { 
            stnode_p fp(&buf);
            DBG(<<" formatting stnode_p");
            DBGTHRD(<<"stnode_p page " << spid.page);
            W_COERCE( fp.format(spid));
            page_s& page (*fp._pp);
            w_assert1(page.pid.vol() == vid);
            rc = me()->write(fd, &page, sizeof(page));
            if (rc.is_error()) {
                W_IGNORE(me()->close(fd));
                return rc;
            }
        }
        DBG(<<" done formatting store node region");
    }

    /*
     *  For raw devices, we must zero out all unused pages
     *  on the device.  This is needed so that the recovery algorithm
     *  can distinguish new pages from used pages.
     */
    if (raw) {
        page_s buf;
        memset(&buf, 0, sizeof(buf));

        DBG(<<" raw device: zeroing...");

        //  This is expensive, so see if we should skip it
        if (skip_raw_init) {
            DBG( << "skipping zero-ing of raw device: " << devname );
        } else {

            DBG( << "zero-ing of raw device: " << devname << " ..." );
            // zero out rest of pages
            for (size_t cur = hdr_pages;cur < num_pages; ++cur) {
                rc = me()->write(fd, &buf, sizeof(page_s));
                if (rc.is_error()) {
                    W_IGNORE(me()->close(fd));
                    return rc;
                }
            }
            DBG( << "finished zero-ing of raw device: " << devname);
        }

    }

    W_COERCE(me()->close(fd));
    DBG(<<"format_vol: done" );

    return RCOK;
}

/*********************************************************************
 *
 *  vol_t::write_vhdr(fd, vhdr, raw_device)
 *
 *  Write the volume header to the volume.
 *
 *********************************************************************/
rc_t
vol_t::write_vhdr(int fd, volhdr_t& vhdr, bool raw_device)
{
    /*
     *  The  volume header is written after the first 512 bytes of
     *  page 0.
     *  This is necessary for raw disk devices since disk labels
     *  are often placed on the first sector.  By not writing on
     *  the first 512bytes of the volume we avoid accidentally 
     *  corrupting the disk label.
     *  
     *  However, for debugging its nice to be able to "cat" the
     *  first few bytes (sector) of the disk (since the volume header is
     *  human-readable).  So, on volumes stored in a unix file,
     *  the volume header is replicated at the beginning of the
     *  first page.
     */
    W_IFDEBUG1(if (raw_device) w_assert1(page_sz >= 1024);)

    /*
     *  tmp holds the volume header to be written
     */
    const int tmpsz = page_sz/2;
    char* tmp = new char[tmpsz]; // auto-del
    if(!tmp) {
        return RC(eOUTOFMEMORY);
    }
    w_auto_delete_array_t<char> autodel(tmp);
    int i;
    for (i = 0; i < tmpsz; i++) tmp[i] = '\0';

    /*
     *  Open an ostream on tmp to write header bytes
     */
    w_ostrstream s(tmp, tmpsz);
    if (!s)  {
            /* XXX really eCLIBRARY */
        return RC(eOS);
    }
    s.seekp(0, ios::beg);
    if (!s)  {
        return RC(eOS);
    }

    // write out the volume header
    i = 0;
    s << prolog[i++] << vhdr.format_version() << endl;
    s << prolog[i++] << vhdr.device_quota_KB() << endl;
    s << prolog[i++] << vhdr.lvid() << endl;
    s << prolog[i++] << vhdr.num_pages() << endl;
    s << prolog[i++] << vhdr.hdr_pages() << endl;
    s << prolog[i++] << vhdr.apid() << endl;
    s << prolog[i++] << vhdr.spid() << endl;
    s << prolog[i++] << vhdr.page_sz() << endl;
    if (!s)  {
        return RC(eOS);
    }

    if (!raw_device) {
        /*
         *  Write a non-official copy of header at beginning of volume
         */
        W_DO(me()->pwrite(fd, tmp, tmpsz, 0));
    }

    /*
     *  write volume header in middle of page
     */
    W_DO(me()->pwrite(fd, tmp, tmpsz, sector_size));

    return RCOK;
}



/*********************************************************************
 *
 *  vol_t::read_vhdr(fd, vhdr)
 *
 *  Read the volume header from the file "fd".
 *
 *********************************************************************/
rc_t
vol_t::read_vhdr(int fd, volhdr_t& vhdr)
{
    /*
     *  tmp place to hold header page (need only 2nd half)
     */
    const int tmpsz = page_sz/2;
    char* tmp = new char[tmpsz]; // auto-del
    if(!tmp) {
        return RC(eOUTOFMEMORY);
    }
    w_auto_delete_array_t<char> autodel(tmp);

    // for (int i = 0; i < tmpsz; i++) tmp[i] = '\0';
        memset(tmp, 0, tmpsz);

    /* 
     *  Read in first page of volume into tmp.
     */
    W_DO(me()->pread(fd, tmp, tmpsz, sector_size));
         
    /*
     *  Read the header strings from tmp using an istream.
     */
    w_istrstream s(tmp, tmpsz);
    s.seekg(0, ios::beg);
    if (!s)  {
            /* XXX c library */ 
        return RC(eOS);
    }

    /* XXX magic number should be maximum of strlens of the
       various prologs. */
    {
    char buf[80];
    uint32_t temp;
    int i = 0;
    s.read(buf, strlen(prolog[i++])) >> temp;
        vhdr.set_format_version(temp);
    s.read(buf, strlen(prolog[i++])) >> temp;
        vhdr.set_device_quota_KB(temp);

    lvid_t t;
    s.read(buf, strlen(prolog[i++])) >> t; vhdr.set_lvid(t);

    s.read(buf, strlen(prolog[i++])) >> temp; vhdr.set_num_pages(temp);
    s.read(buf, strlen(prolog[i++])) >> temp; vhdr.set_hdr_pages(temp);
    s.read(buf, strlen(prolog[i++])) >> temp; vhdr.set_apid(temp);
    s.read(buf, strlen(prolog[i++])) >> temp; vhdr.set_spid(temp);
    s.read(buf, strlen(prolog[i++])) >> temp; vhdr.set_page_sz(temp);

    if ( !s || vhdr.page_sz() != page_sz ||
        vhdr.format_version() != volume_format_version ) {

        cout << "Volume format bad:" << endl;
        cout << "version " << vhdr.format_version() << endl;
        cout << "   expected " << volume_format_version << endl;
        cout << "page size " << vhdr.page_sz() << endl;
        cout << "   expected " << page_sz << endl;

        cout << "Other: " << endl;
        cout << "# pages " << vhdr.num_pages() << endl;
        cout << "# hdr pages " << vhdr.hdr_pages() << endl;
        cout << "1st apid " << vhdr.apid() << endl;
        cout << "spid " << vhdr.spid() << endl;

        cout << "Buffer: " << endl;
        cout << buf << endl;

        if (smlevel_0::log) {
            return RC(eBADFORMAT);
        }
    }

    }

    return RCOK;
}
    
    

/*********************************************************************
 *
 *  vol_t::read_vhdr(devname, vhdr)
 *
 *  Read the volume header for "devname" and return it in "vhdr".
 *
 *********************************************************************/
rc_t
vol_t::read_vhdr(const char* devname, volhdr_t& vhdr)
{
    w_rc_t e;
    int fd;

    e = me()->open(devname, smthread_t::OPEN_RDONLY, 0, fd);
    if (e.is_error())
        return e;
    
    e = read_vhdr(fd, vhdr);

    W_IGNORE(me()->close(fd));

    if (e.is_error())  {
        W_DO_MSG(e, << "device name=" << devname);
    }

    return RCOK;
}




/*--------------------------------------------------------------*
 *  vol_t::get_du_statistics()           DU DF
 *--------------------------------------------------------------*/
rc_t vol_t::get_du_statistics(struct volume_hdr_stats_t& st, bool)
{
    volume_hdr_stats_t new_stats;
    uint32_t unalloc_ext_cnt = 0;
    uint32_t alloc_ext_cnt = 0;
    new_stats.unalloc_ext_cnt = (unsigned) unalloc_ext_cnt;
    new_stats.alloc_ext_cnt = (unsigned) alloc_ext_cnt;
    new_stats.alloc_ext_cnt -= _hdr_pages;
    new_stats.hdr_ext_cnt = _hdr_pages;
    new_stats.extent_size = 0;

    /*
    if (audit) {
        if (!(new_stats.alloc_ext_cnt + new_stats.hdr_ext_cnt + 
                    new_stats.unalloc_ext_cnt == _num_exts)) {
            // return RC(fcINTERNAL);
            W_FATAL(eINTERNAL);
        };
        W_DO(new_stats.audit());
    }
    */
    st.add(new_stats);
    return RCOK;
}

/**\brief
 * Use attempt at first so we can get a rough idea of
 * the contention on this mutex.
 */
void                        
vol_t::acquire_mutex(vol_t::lock_state* _me, bool for_write) 
{
    assert_mutex_notmine(_me);
    if(for_write) {
        INC_TSTAT(need_vol_lock_w);  
        if(_mutex.attempt_write() ) {
            INC_TSTAT(nowait_vol_lock_w);  
            return;
        }
        _mutex.acquire_write();
    } else {
        INC_TSTAT(need_vol_lock_r);  
        if(_mutex.attempt_read() ) {
            INC_TSTAT(nowait_vol_lock_r);  
            return;
        }
        _mutex.acquire_read();
    }
    assert_mutex_mine(_me);
}


uint32_t vol_t::num_used_pages() const
{
    w_assert1(_alloc_cache);
    return num_pages() - _alloc_cache->get_total_free_page_count();
}

bool vol_t::is_allocated_page(shpid_t pid) const
{
    w_assert1(_alloc_cache);
    return _alloc_cache->is_allocated_page(pid);
}
