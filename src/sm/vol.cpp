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
#include "sm_int_1.h"
#include "stnode_page.h"
#include "vol.h"
#include "sm_du_stats.h"
#include "crash.h"

#include "sm_vtable_enum.h"

#include "bf_fixed.h"
#include "alloc_cache.h"
#include "bf_tree.h"


#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_t<generic_page>;
template class w_auto_delete_array_t<stnode_t>;
#endif

// CS TODO read this from log at recovery
vid_t vol_t::_next_vid = vid_t(1);

/** sector_size : reserved space at beginning of volume. */
static const int sector_size = 512;

/*
Volume layout:
   volume header
   alloc_page pages -- Starts on page 1.
   stnode_page -- only one page
   data pages -- rest of volume

   alloc_page pages are bitmaps indicating which of its pages are allocated.
   alloc_page pages are read and modified without any locks in any time.
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
std::cout << "!!!!!!!!!!!!!!!! NO-op sync -- 4" << std::endl;

    smthread_t* t = me();
    W_DO_MSG(t->fsync(_unix_fd), << "volume id=" << vid());
    return RCOK;
}

rc_t vol_t::mount(const char* devname)
{
    if (_unix_fd >= 0) return RC(eALREADYMOUNTED);

    /*
     *  Save the device name
     */
    w_assert1(strlen(devname) < sizeof(_devname));
    strcpy(_devname, devname);

    w_rc_t rc;
    int open_flags = smthread_t::OPEN_RDWR;

    rc = me()->open(devname, open_flags, 0666, _unix_fd);
    if (rc.is_error()) {
        _unix_fd = -1;
        return rc;
    }

    //  Read the volume header on the device
    volhdr_t vhdr;
    {
        rc_t rc = vhdr.read(_unix_fd);
        if (rc.is_error())  {
            W_IGNORE(me()->close(_unix_fd));
            _unix_fd = -1;
            return RC_AUGMENT(rc);
        }
    }

    _vid = vhdr.vid;
    _num_pages =  vhdr.num_pages;
    _apid = lpid_t(_vid, vhdr.apid); // 0 if no volumes formatted yet
    _spid = lpid_t(_vid, vhdr.spid); // 0 if no volumes formatted yet
    _hdr_pages = vhdr.hdr_pages; // 0 if no volumes formatted yet

    clear_caches();
    _fixed_bf = new bf_fixed_m();
    w_assert1(_fixed_bf);
    rc = _fixed_bf->init(this, _unix_fd, _num_pages);
    if (rc.is_error()) {
        W_IGNORE(me()->close(_unix_fd));
        _unix_fd = -1;
        return rc;
    }
    _first_data_pageid = _fixed_bf->get_page_cnt() + 1; // +1 for volume header

    _alloc_cache = new alloc_cache_t(_vid, _fixed_bf);
    w_assert1(_alloc_cache);
    W_DO(_alloc_cache->load_by_scan(_num_pages));

    _stnode_cache = new stnode_cache_t(_vid, _fixed_bf);
    w_assert1(_stnode_cache);

    W_DO( bf->install_volume(this));
    return RCOK;
}

/** @todo flush argument is never used. Backtrace through the callers and maybe
 * eliminate it entirely? */
rc_t
vol_t::dismount(bool /* flush */, const bool clear_cb)
{
    DBG(<<" vol_t::dismount flush=" << flush);

    INC_TSTAT(vol_cache_clears);

    w_assert1(_unix_fd >= 0);
    W_DO(bf->uninstall_volume(_vid, clear_cb));

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
    W_DO(vhdr.read(_unix_fd));
    smlevel_0::errlog->clog << info_prio << "vol_t::check_disk()\n";
    smlevel_0::errlog->clog << info_prio
        << "\tvolid      : " << vhdr.vid << flushl;
    smlevel_0::errlog->clog << info_prio
        << "\tnum_pages   : " << vhdr.num_pages << flushl;
    smlevel_0::errlog->clog << info_prio
        << "\thdr_pages   : " << vhdr.hdr_pages << flushl;

    smlevel_0::errlog->clog << info_prio
        << "\tstore  #   flags   status: [ extent-list ]" << "." << endl;
    for (shpid_t i = 1; i < stnode_page_h::max; i++)  {
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


rc_t vol_t::alloc_a_page(lpid_t &pid)
{
    FUNC(vol_t::alloc_a_page);
    w_assert1(_alloc_cache);
    shpid_t shpid;
    W_DO(_alloc_cache->allocate_one_page(shpid));
    pid = lpid_t (_vid, shpid);
    return RCOK;
}
rc_t vol_t::alloc_consecutive_pages(size_t page_count, lpid_t &pid_begin)
{
    FUNC(vol_t::alloc_consecutive_pages);
    w_assert1(_alloc_cache);
    shpid_t shpid;
    W_DO(_alloc_cache->allocate_consecutive_pages(shpid, page_count));
    pid_begin = lpid_t (_vid, shpid);
    return RCOK;
}

rc_t vol_t::store_operation(const store_operation_param& param, bool redo)
{
    w_assert1(param.snum() < stnode_page_h::max);
    w_assert1(_stnode_cache);
    W_DO(_stnode_cache->store_operation(param, redo));
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
    snum = _stnode_cache->get_min_unused_store_ID();
    if (snum >= stnode_page_h::max) {
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
/** @todo sync_volume is never used. Backtrack through the code and remove it entirely? */
rc_t
vol_t::set_store_flags(snum_t snum, store_flag_t flags, bool /* sync_volume */)
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
        flags = smlevel_0::st_unallocated;
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
vol_t::get_volume_meta_stats(SmVolumeMetaStats& volume_stats) {
    volume_stats.numStores      = stnode_page_h::max;
    volume_stats.numAllocStores = _stnode_cache->get_all_used_store_ID().size();
    volume_stats.numPages       = _num_pages;
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

bool vol_t::is_alloc_store(snum_t f) const {
    FUNC(is_alloc_store);
    return _stnode_cache->is_allocated(f);
}
shpid_t vol_t::get_store_root(snum_t f) const {
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
 *  vol_t::read_page(pnum, page, past_end)
 *
 *  Read the page at "pnum" of the volume into the buffer "page".
 *
 *********************************************************************/
rc_t
vol_t::read_page(shpid_t pnum, generic_page& page, bool& past_end)
{
    w_assert1(pnum > 0 && pnum < (shpid_t)(_num_pages));
    fileoff_t offset = fileoff_t(pnum) * sizeof(page);

    // Notify caller it is trying to read past the end of OS file
    // The function returns a good return code in this case because not all
    // caller cares about reading past the end
    past_end = false;

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
        DBGOUT3 (<< "vol_t::read_page, read passed the end, zero out the page");

        // read past end of OS file. return all zeros
        memset(&page, 0, sizeof(page));
        past_end = true;
    } else {
        W_COERCE_MSG(err, << "volume id=" << vid()
              << " err_num " << err.err_num()
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
vol_t::write_page(shpid_t pnum, generic_page& page)
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
vol_t::write_many_pages(shpid_t pnum, const generic_page* const pages, int cnt)
{
    w_assert1(pnum > 0 && pnum < (shpid_t)(_num_pages));
    w_assert1(cnt > 0);
    fileoff_t offset = fileoff_t(pnum) * sizeof(generic_page);

    smthread_t* t = me();

    long start = 0;
    if(_apply_fake_disk_latency) start = gethrtime();

    // do the actual write now
    W_COERCE_MSG(t->pwrite(_unix_fd, pages, sizeof(generic_page)*cnt, offset), << "volume id=" << vid());

    fake_disk_latency(start);
    ADD_TSTAT(vol_blks_written, cnt);
    INC_TSTAT(vol_writes);

    return RCOK;
}

const char* volhdr_t::prolog[] = {
    "%% SHORE VOLUME VERSION ",
    "%% device quota(KB)  : ",
    "%% volume_id         : ",
    "%% num_pages         : ",
    "%% hdr_pages         : ",
    "%% epid              : ",
    "%% spid              : ",
};

/*********************************************************************
 *
 *  vol_t::format_vol(devname, lvid, num_pages,
 *                    apply_fake_io_latency, fake_disk_latency)
 *
 *  Format the volume "devname" for long volume id "lvid" and
 *  a size of "num_pages".
 *********************************************************************/
rc_t
vol_t::format_vol(
    const char*  devname,
    shpid_t      num_pages,
    vid_t&       vid)
{
    FUNC(vol_t::format_vol);
    // CS TODO latch here

    /*
     *  No log needed.
     *  WHOLE FUNCTION is a critical section
     */
    xct_log_switch_t log_off(OFF);

    DBG( << "formating volume " << devname << ">" );
    int flags = smthread_t::OPEN_CREATE | smthread_t::OPEN_RDWR
        | smthread_t::OPEN_EXCL | smthread_t::OPEN_TRUNC;
    int fd;
    W_DO(me()->open(devname, flags, 0666, fd));

    shpid_t alloc_pages = num_pages / alloc_page_h::bits_held + 1; // # alloc_page_h pages
    shpid_t hdr_pages = alloc_pages + 1 + 1; // +1 for stnode_page, +1 for volume header

    shpid_t apid = shpid_t(1);
    shpid_t spid = shpid_t(1 + alloc_pages);

    vid = _next_vid++;

    /*
     *  Set up the volume header
     */
    volhdr_t vhdr;
    vhdr.version = volhdr_t::FORMAT_VERSION;
    vhdr.vid = vid;
    vhdr.num_pages = num_pages;
    vhdr.hdr_pages = hdr_pages;
    vhdr.apid = apid;
    vhdr.spid = spid;

    /*
     *  Write volume header
     */
    W_DO(vhdr.write(fd));

    /*
     *  Skip first page ... seek to first info page.
     *
     * FRJ: this seek is safe because no other thread can access the
     * file descriptor we just opened.
     */
    W_DO(me()->lseek(fd, sizeof(generic_page), sthread_t::SEEK_AT_SET));

    {
        generic_page buf;
#ifdef ZERO_INIT
        // zero out data portion of page to keep purify/valgrind happy.
        // Unfortunately, this isn't enough, as the format below
        // seems to assign an uninit value.
        memset(&buf, '\0', sizeof(buf));
#endif

        //  Format alloc_page pages
        {
            for (apid = 1; apid < alloc_pages + 1; ++apid)  {
                alloc_page_h ap(&buf, lpid_t(vid, apid));  // format page
                // set bits for the header pages
                if (apid == 1) {
                    for (shpid_t hdr_pid = 0; hdr_pid < hdr_pages; ++hdr_pid) {
                        ap.set_bit(hdr_pid);
                    }
                }
                generic_page* page = ap.get_generic_page();
                w_assert9(&buf == page);
                page->checksum = page->calculate_checksum();

                W_DO(me()->write(fd, page, sizeof(*page)));
            }
        }
        DBG(<<" done formatting extent region");

        // Format stnode_page
        {
            DBG(<<" formatting stnode_page");
            DBGTHRD(<<"stnode_page page " << spid);
            stnode_page_h fp(&buf, lpid_t(vid, spid));  // formatting...
            generic_page* page = fp.get_generic_page();
            page->checksum = page->calculate_checksum();
            W_DO(me()->write(fd, page, sizeof(*page)));
        }
        DBG(<<" done formatting store node region");
    }

    W_COERCE(me()->close(fd));
    DBG(<<"format_vol: done" );

    return RCOK;
}

/*********************************************************************
 *
 *  vol_t::write_vhdr(fd, vhdr)
 *
 *  Write the volume header to the volume.
 *
 *********************************************************************/
rc_t
volhdr_t::write(int fd)
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
     *
     *  CS (TODO): I'm nut sure this is true. As far as I understand,
     *  the "disklabel" is only used in old versions of BSD. In a typical
     *  Linux scenario, the MBR would be overwritten, but that should be OK,
     *  since the user will not want to boot from a device he is using for
     *  raw DB storage. Furthermore, it would be a better practice to use
     *  a raw partition (e.g., /dev/sdb1) instead of the whole device.
     */
    w_assert1(sizeof(generic_page) >= 1024);

    /*
     *  tmp holds the volume header to be written
     */
    const int tmpsz = sizeof(generic_page) / 2;
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
    s << prolog[i++] << version << endl;
    s << prolog[i++] << vid << endl;
    s << prolog[i++] << num_pages << endl;
    s << prolog[i++] << hdr_pages << endl;
    s << prolog[i++] << apid << endl;
    s << prolog[i++] << spid << endl;
    if (!s)  {
        return RC(eOS);
    }

    /*
     *  Write a non-official copy of header at beginning of volume
     *
     *  CS: this second location could be used for writing vhdr atomically,
     *  but it requires proper "recovery" during mount.
     */
    W_DO(me()->pwrite(fd, tmp, tmpsz, 0));

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
volhdr_t::read(int fd)
{
    /*
     *  tmp place to hold header page (need only 2nd half)
     */
    const int tmpsz = sizeof(generic_page) / 2;
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
    version = temp;

    s.read(buf, strlen(prolog[i++])) >> vid;
    s.read(buf, strlen(prolog[i++])) >> num_pages;
    s.read(buf, strlen(prolog[i++])) >> hdr_pages;
    s.read(buf, strlen(prolog[i++])) >> apid;
    s.read(buf, strlen(prolog[i++])) >> spid;

    if ( !s || version != FORMAT_VERSION ) {

        cout << "Volume format bad:" << endl;
        cout << "version " << version << endl;
        cout << "   expected " << FORMAT_VERSION << endl;

        cout << "Other: " << endl;
        cout << "# pages " << num_pages << endl;
        cout << "# hdr pages " << hdr_pages << endl;
        cout << "1st apid " << apid << endl;
        cout << "spid " << spid << endl;

        cout << "Buffer: " << endl;
        cout << buf << endl;

        if (smlevel_0::log) {
            return RC(eBADFORMAT);
        }
    }

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
