/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define LOGREC_C

#include "eventlog.h"

#include "sm_base.h"
#include "logdef_gen.cpp"
#include "vec_t.h"
#include "alloc_cache.h"
#include "allocator.h"
#include "vol.h"
#include "restore.h"
#include "log_spr.h"
#include <sstream>

#include <iomanip>
typedef        ios::fmtflags        ios_fmtflags;

#include <new>

DEFINE_SM_ALLOC(logrec_t);

/*********************************************************************
 *
 *  logrec_t::cat_str()
 *
 *  Return a string describing the category of the log record.
 *
 *********************************************************************/
const char*
logrec_t::cat_str() const
{
    switch (cat())  {
    case t_logical:
        return "l---";

    case t_logical | t_cpsn:
        return "l--c";

    case t_status:
        return "s---";

    case t_undo:
        return "--u-";

    case t_redo:
        return "-r--";

    case t_redo | t_cpsn:
        return "-r-c";

    case t_undo | t_redo:
        return "-ru-";

    case t_undo | t_redo | t_logical:
        return "lru-";

    case t_redo | t_logical | t_cpsn:
        return "lr_c";

    case t_redo | t_logical : // used in I/O layer
        return "lr__";

    case t_undo | t_logical | t_cpsn:
        return "l_uc";

    case t_undo | t_logical :
        return "l-u-";

    case t_redo | t_single_sys_xct:
        return "ssx-";
    case t_multi | t_redo | t_single_sys_xct:
        return "ssxm";

#if W_DEBUG_LEVEL > 0
    case t_bad_cat:
        // for debugging only
        return "BAD-";
#endif
    default:
      return 0;
    }
}

/*********************************************************************
 *
 *  logrec_t::type_str()
 *
 *  Return a string describing the type of the log record.
 *
 *********************************************************************/
const char*
logrec_t::get_type_str(kind_t type)
{
    switch (type)  {
#        include "logstr_gen.cpp"
    default:
      return 0;
    }

    /*
     *  Not reached.
     */
    W_FATAL(eINTERNAL);
    return 0;
}




/*********************************************************************
 *
 *  logrec_t::fill(pid, len)
 *
 *  Fill the "pid" and "length" field of the log record.
 *
 *********************************************************************/
void
logrec_t::fill(PageID p, StoreID store, uint16_t tag, smsize_t l)
{
    w_assert9(w_base_t::is_aligned(_data));

    /* adjust _cat */
    xct_t *x = xct();
    if(x && (x->rolling_back() || x->state() == smlevel_0::xct_aborting))
    {
        header._cat |= t_rollback;
    }
    set_pid(0);
    if (!is_single_sys_xct()) { // prv does not exist in single-log system transaction
        set_xid_prev(lsn_t::null);
    }
    header._page_tag = tag;
    header._pid = p;
    header._stid = store;
    char *dat = is_single_sys_xct() ? data_ssx() : data();
    if (l != ALIGN_BYTE(l)) {
        // zero out extra space to keep purify happy
        memset(dat+l, 0, ALIGN_BYTE(l)-l);
    }
    unsigned int tmp = ALIGN_BYTE(l) + (is_single_sys_xct() ? hdr_single_sys_xct_sz : hdr_non_ssx_sz) + sizeof(lsn_t);
    tmp = (tmp + 7) & unsigned(-8); // force 8-byte alignment
    w_assert1(tmp <= sizeof(*this));
    header._len = tmp;
    if(type() != t_skip) {
        DBG( << "Creat log rec: " << *this
                << " size: " << header._len << " xid_prevlsn: " << (is_single_sys_xct() ? lsn_t::null : xid_prev()) );
    }
}



/*********************************************************************
 *
 *  logrec_t::fill_xct_attr(tid, xid_prev_lsn)
 *
 *  Fill the transaction related fields of the log record.
 *
 *********************************************************************/
void
logrec_t::fill_xct_attr(const tid_t& tid, const lsn_t& last)
{
    w_assert0(!is_single_sys_xct()); // prv/xid doesn't exist in single-log system transaction!
    xidInfo._xid = tid;
    if(xid_prev().valid()) {
        w_assert2(is_cpsn());
    } else {
        set_xid_prev (last);
    }
}

/*
 * Determine whether the log record header looks valid
 */
bool
logrec_t::valid_header(const lsn_t & lsn) const
{
    return header.is_valid() && (lsn == lsn_t::null || lsn == *_lsn_ck());
}


/*********************************************************************
 *
 *  logrec_t::redo(page)
 *
 *  Invoke the redo method of the log record.
 *
 *********************************************************************/
void logrec_t::redo(fixable_page_h* page)
{
    DBG( << "Redo  log rec: " << *this
        << " size: " << header._len << " xid_prevlsn: " << (is_single_sys_xct() ? lsn_t::null : xid_prev()) );

    // Could be either user transaction or compensation operatio,
    // not system transaction because currently all system transactions
    // are single log

    // This is used by both Single-Page-Recovery and serial recovery REDO phase

    // Not all REDO operations have associated page
    // If there is a page, mark the page for recovery access
    // this is for page access validation purpose to allow recovery
    // operation to by-pass the page concurrent access check


    switch (header._type)  {
#include "redo_gen.cpp"
    }

    page->update_page_lsn(lsn());
    page->set_img_page_lsn(lsn());
}

static __thread logrec_t::kind_t undoing_context = logrec_t::t_max_logrec; // for accounting TODO REMOVE


/*********************************************************************
 *
 *  logrec_t::undo(page)
 *
 *  Invoke the undo method of the log record. Automatically tag
 *  a compensation lsn to the last log record generated for the
 *  undo operation.
 *
 *********************************************************************/
void
logrec_t::undo(fixable_page_h* page)
{
    w_assert0(!is_single_sys_xct()); // UNDO shouldn't be called for single-log sys xct
    undoing_context = logrec_t::kind_t(header._type);
    DBG( << "Undo  log rec: " << *this
        << " size: " << header._len  << " xid_prevlsn: " << xid_prev());

    // Only system transactions involve multiple pages, while there
    // is no UNDO for system transactions, so we only need to mark
    // recovery flag for the current UNDO page

    // If there is a page, mark the page for recovery, this is for page access
    // validation purpose to allow recovery operation to by-pass the
    // page concurrent access check
    // In most cases we do not have a page from caller, therefore
    // we need to go to individual undo function to mark the recovery flag.
    // All the page related operations are in Btree_logrec.cpp, including
    // operations for system and user transactions, note that operations
    // for system transaction have REDO but no UNDO
    // The actual UNDO implementation in Btree_impl.cpp

    switch (header._type) {
#include "undo_gen.cpp"
    }

    xct()->compensate_undo(xid_prev());

    undoing_context = logrec_t::t_max_logrec;
}

/*********************************************************************
 *
 *  logrec_t::corrupt()
 *
 *  Zero out most of log record to make it look corrupt.
 *  This is for recovery testing.
 *
 *********************************************************************/
void
logrec_t::corrupt()
{
    char* end_of_corruption = ((char*)this)+length();
    char* start_of_corruption = (char*)&header._type;
    size_t bytes_to_corrupt = end_of_corruption - start_of_corruption;
    memset(start_of_corruption, 0, bytes_to_corrupt);
}

/*********************************************************************
 *
 *  xct_freeing_space
 *
 *  Status Log to mark the end of transaction and the beginning
 *  of space recovery.
 *  Synchronous for commit. Async for abort.
 *
 *********************************************************************/
xct_freeing_space_log::xct_freeing_space_log()
{
    fill((PageID) 0, 0);
}


/*********************************************************************
 *
 *  xct_end_group_log
 *
 *  Status Log to mark the end of transaction and space recovery
 *  for a group of transactions.
 *
 *********************************************************************/
xct_list_t::xct_list_t(
    const xct_t*                        xct[],
    int                                 cnt)
    : count(cnt)
{
    w_assert1(count <= max);
    for (uint i = 0; i < count; i++)  {
        xrec[i].tid = xct[i]->tid();
    }
}

xct_end_group_log::xct_end_group_log(const xct_t *list[], int listlen)
{
    fill((PageID) 0, (new (_data) xct_list_t(list, listlen))->size());
}
/*********************************************************************
 *
 *  xct_end_log
 *  xct_abort_log
 *
 *  Status Log to mark the end of transaction and space recovery.
 *
 *********************************************************************/
xct_end_log::xct_end_log()
{
    fill((PageID) 0, 0);
}

// We use a different log record type here only for debugging purposes
xct_abort_log::xct_abort_log()
{
    fill((PageID) 0, 0);
}

/*********************************************************************
 *
 *  comment_log
 *
 *  For debugging
 *
 *********************************************************************/
comment_log::comment_log(const char *msg)
{
    w_assert1(strlen(msg) < sizeof(_data));
    memcpy(_data, msg, strlen(msg)+1);
    DBG(<<"comment_log: L: " << (const char *)_data);
    fill((PageID) 0, strlen(msg)+1);
}

void
comment_log::redo(fixable_page_h *page)
{
    w_assert9(page == 0);
    DBG(<<"comment_log: R: " << (const char *)_data);
    ; // just for the purpose of setting breakpoints
}

void
comment_log::undo(fixable_page_h *page)
{
    w_assert9(page == 0);
    DBG(<<"comment_log: U: " << (const char *)_data);
    ; // just for the purpose of setting breakpoints
}

/*********************************************************************
 *
 *  compensate_log
 *
 *  Needed when compensation rec is written rather than piggybacked
 *  on another record
 *
 *********************************************************************/
compensate_log::compensate_log(const lsn_t& rec_lsn)
{
    fill((PageID) 0, 0);
    set_clr(rec_lsn);
}


/*********************************************************************
 *
 *  skip_log partition
 *
 *  Filler log record -- for skipping to end of log partition
 *
 *********************************************************************/
skip_log::skip_log()
{
    fill((PageID) 0, 0);
}

/*********************************************************************
 *
 *  chkpt_begin_log
 *
 *  Status Log to mark start of fussy checkpoint.
 *
 *********************************************************************/
chkpt_begin_log::chkpt_begin_log(const lsn_t &lastMountLSN)
{
    new (_data) lsn_t(lastMountLSN);
    fill((PageID) 0, sizeof(lsn_t));
}



/*********************************************************************
 *
 *  chkpt_end_log(const lsn_t &master, const lsn_t& min_rec_lsn, const lsn_t& min_txn_lsn)
 *
 *  Status Log to mark completion of fussy checkpoint.
 *  Master is the lsn of the record that began this chkpt.
 *  min_rec_lsn is the earliest lsn for all dirty pages in this chkpt.
 *  min_txn_lsn is the earliest lsn for all txn in this chkpt.
 *
 *********************************************************************/
chkpt_end_log::chkpt_end_log(const lsn_t& lsn, const lsn_t& min_rec_lsn,
                                const lsn_t& min_txn_lsn)
{
    // initialize _data
    lsn_t *l = new (_data) lsn_t(lsn);
    l++; //grot
    *l = min_rec_lsn;
    l++; //grot
    *l = min_txn_lsn;

    fill((PageID) 0, (3 * sizeof(lsn_t)) + (3 * sizeof(int)));
}

xct_latency_dump_log::xct_latency_dump_log(unsigned long nsec)
{
    *((unsigned long*) _data) = nsec;
    fill((PageID) 0, sizeof(unsigned long));
}

/*********************************************************************
 *
 *  chkpt_bf_tab_log
 *
 *  Data Log to save dirty page table at checkpoint.
 *  Contains, for each dirty page, its pid, minimum recovery lsn and page (latest) lsn.
 *
 *********************************************************************/

chkpt_bf_tab_t::chkpt_bf_tab_t(
    int                 cnt,        // I-  # elements in pids[] and rlsns[]
    const PageID*         pids,        // I-  id of of dirty pages
    const lsn_t*         rlsns,        // I-  rlsns[i] is recovery lsn of pids[i], the oldest
    const lsn_t*         plsns)        // I-  plsns[i] is page lsn lsn of pids[i], the latest
    : count(cnt)
{
    w_assert1( sizeof(*this) <= logrec_t::max_data_sz );
    w_assert1(count <= max);
    for (uint i = 0; i < count; i++) {
        brec[i].pid = pids[i];
        brec[i].rec_lsn = rlsns[i];
        brec[i].page_lsn = plsns[i];
    }
}


chkpt_bf_tab_log::chkpt_bf_tab_log(
    int                 cnt,        // I-  # elements in pids[] and rlsns[]
    const PageID*         pid,        // I-  id of of dirty pages
    const lsn_t*         rec_lsn,// I-  rec_lsn[i] is recovery lsn (oldest) of pids[i]
    const lsn_t*         page_lsn)// I-  page_lsn[i] is page lsn (latest) of pids[i]
{
    fill((PageID) 0, (new (_data) chkpt_bf_tab_t(cnt, pid, rec_lsn, page_lsn))->size());
}




/*********************************************************************
 *
 *  chkpt_xct_tab_log
 *
 *  Data log to save transaction table at checkpoint.
 *  Contains, for each active xct, its id, state, last_lsn
 *
 *********************************************************************/
chkpt_xct_tab_t::chkpt_xct_tab_t(
    const tid_t&                         _youngest,
    int                                 cnt,
    const tid_t*                         tid,
    const smlevel_0::xct_state_t*         state,
    const lsn_t*                         last_lsn,
    const lsn_t*                         first_lsn)
    : youngest(_youngest), count(cnt)
{
    w_assert1(count <= max);
    for (uint i = 0; i < count; i++)  {
        xrec[i].tid = tid[i];
        xrec[i].state = state[i];
        xrec[i].last_lsn = last_lsn[i];
        xrec[i].first_lsn = first_lsn[i];
    }
}

chkpt_xct_tab_log::chkpt_xct_tab_log(
    const tid_t&                         youngest,
    int                                 cnt,
    const tid_t*                         tid,
    const smlevel_0::xct_state_t*         state,
    const lsn_t*                         last_lsn,
    const lsn_t*                         first_lsn)
{
    fill((PageID) 0, (new (_data) chkpt_xct_tab_t(youngest, cnt, tid, state,
                                         last_lsn, first_lsn))->size());
}


/*********************************************************************
 *
 *  chkpt_xct_lock_log
 *
 *  Data log to save acquired transaction locks for an active transaction at checkpoint.
 *  Contains, each active lock, its hash and lock mode
 *
 *********************************************************************/
chkpt_xct_lock_t::chkpt_xct_lock_t(
    const tid_t&                        _tid,
    int                                 cnt,
    const okvl_mode*                    lock_mode,
    const uint32_t*                     lock_hash)
    : tid(_tid), count(cnt)
{
    w_assert1(count <= max);
    for (uint i = 0; i < count; i++)  {
        xrec[i].lock_mode = lock_mode[i];
        xrec[i].lock_hash = lock_hash[i];
    }
}

chkpt_xct_lock_log::chkpt_xct_lock_log(
    const tid_t&                        tid,
    int                                 cnt,
    const okvl_mode*                    lock_mode,
    const uint32_t*                     lock_hash)
{
    fill((PageID) 0, (new (_data) chkpt_xct_lock_t(tid, cnt, lock_mode,
                                         lock_hash))->size());
}

chkpt_backup_tab_t::chkpt_backup_tab_t(
        int cnt,
        const string* paths)
    : count(cnt)
{
    std::stringstream ss;
    for (uint i = 0; i < count; i++) {
        ss << paths[i] << endl;
    }
    data_size = ss.tellp();
    w_assert0(data_size <= logrec_t::max_data_sz);
    ss.read(data, data_size);
}

void chkpt_backup_tab_t::read(
        std::vector<string>& paths)
{
    std::string s;
    std::stringstream ss;
    ss.write(data, data_size);

    for (uint i = 0; i < count; i++) {
        ss >> s;
        paths.push_back(s);
    }
}

chkpt_backup_tab_log::chkpt_backup_tab_log(int cnt,
                                           const string* paths)
{
    // CS TODO
    fill(0, (new (_data) chkpt_backup_tab_t(cnt, paths))->size());
}

void chkpt_backup_tab_log::redo(fixable_page_h*)
{
    // CS TODO
    chkpt_backup_tab_t* tab = (chkpt_backup_tab_t*) _data;
    std::vector<string> paths;
    tab->read(paths);
    w_assert0(tab->count == paths.size());

    for (size_t i = 0; i < tab->count; i++) {
        W_COERCE(smlevel_0::vol->sx_add_backup(paths[i], lsn_t::null, false /* log */));
    }
}

chkpt_restore_tab_log::chkpt_restore_tab_log()
{
    chkpt_restore_tab_t* tab =
        new (_data) chkpt_restore_tab_t();

    smlevel_0::vol->chkpt_restore_progress(tab);
    fill((PageID) 0, tab->length());
}

void chkpt_restore_tab_log::redo(fixable_page_h*)
{
    // CS TODO: disabled for now
    return;

    chkpt_restore_tab_t* tab = (chkpt_restore_tab_t*) _data;

    vol_t* vol = smlevel_0::vol;

    w_assert0(vol);
    if (!vol->is_failed()) {
        // Marking the device failed will kick-off the restore thread, initialize
        // its state, and restore the metadata (even if already done - idempotence)
        W_COERCE(vol->mark_failed(false /* evict */, true /* redo */));
    }

    for (size_t i = 0; i < tab->firstNotRestored; i++) {
        vol->redo_segment_restore(i);
    }

    // CS TODO
    // RestoreBitmap bitmap(vol->num_used_pages());
    // bitmap.deserialize(tab->bitmap, tab->firstNotRestored,
    //         tab->firstNotRestored + tab->bitmapSize);
    // // Bitmap of RestoreMgr might have been initialized already
    // for (size_t i = tab->firstNotRestored; i < bitmap.getSize(); i++) {
    //     if (bitmap.get(i)) {
    //         vol->redo_segment_restore(i);
    //     }
    // }
}

add_backup_log::add_backup_log(const string& path, lsn_t backupLSN)
{
    *((lsn_t*) data_ssx()) = backupLSN;
    w_assert0(path.length() < smlevel_0::max_devname);
    memcpy(data_ssx() + sizeof(lsn_t), path.data(), path.length());
    fill((PageID) 0, sizeof(lsn_t) + path.length());
}

void add_backup_log::redo(fixable_page_h*)
{
    lsn_t backupLSN = *((lsn_t*) data_ssx());
    const char* dev_name = (const char*) (data_ssx() + sizeof(lsn_t));
    W_COERCE(smlevel_0::vol->sx_add_backup(string(dev_name), backupLSN, false));
}

undo_done_log::undo_done_log()
{
    fill((PageID) 0, 0);
}

redo_done_log::redo_done_log()
{
    fill((PageID) 0, 0);
}

loganalysis_end_log::loganalysis_end_log()
{
    fill((PageID) 0, 0);
}

loganalysis_begin_log::loganalysis_begin_log()
{
    fill((PageID) 0, 0);
}

restore_begin_log::restore_begin_log()
{
#ifdef TIMED_LOG_RECORDS
    unsigned long tstamp = sysevent_timer::timestamp();
    memcpy(_data, &tstamp, sizeof(unsigned long));
    fill((PageID) 0, sizeof(unsigned long));
#else
    fill((PageID) 0, 0);
#endif
}

void restore_begin_log::redo(fixable_page_h*)
{
    return; // CS TODO: disabled for now

    vol_t* volume = smlevel_0::vol;
    // volume must be mounted
    w_assert0(volume);

    // Marking the device failed will kick-off the restore thread, initialize
    // its state, and restore the metadata (even if already done - idempotence)
    W_COERCE(volume->mark_failed(false /* evict */, true /* redo */));
}

restore_end_log::restore_end_log()
{
#ifdef TIMED_LOG_RECORDS
    unsigned long tstamp = sysevent_timer::timestamp();
    memcpy(_data, &tstamp, sizeof(unsigned long));
    fill((PageID) 0, sizeof(unsigned long));
#else
    fill((PageID) 0, 0);
#endif
}

void restore_end_log::redo(fixable_page_h*)
{
    return; // CS TODO: disabled for now

    vol_t* volume = smlevel_0::vol;
    // volume must be mounted and failed
    w_assert0(volume && volume->is_failed());

    // CS TODO: fix this
    // bool finished = volume->check_restore_finished(true /* redo */);
    // w_assert0(finished);
}

restore_segment_log::restore_segment_log(uint32_t segment)
{
    char* pos = data_ssx();

    memcpy(pos, &segment, sizeof(uint32_t));
    pos += sizeof(uint32_t);

#ifdef TIMED_LOG_RECORDS
    unsigned long tstamp = sysevent_timer::timestamp();
    memcpy(pos, &tstamp, sizeof(unsigned long));
    pos += sizeof(unsigned long);
#endif

    fill((PageID) 0, pos - data_ssx());
}

void restore_segment_log::redo(fixable_page_h*)
{
    return; // CS TODO: disabled for now

    vol_t* volume = smlevel_0::vol;
    // volume must be mounted and failed
    w_assert0(volume && volume->is_failed());

    uint32_t segment = *((uint32_t*) data_ssx());

    volume->redo_segment_restore(segment);
}

alloc_page_log::alloc_page_log(PageID pid)
{
    memcpy(data_ssx(), &pid, sizeof(PageID));
    PageID alloc_pid = pid - (pid % alloc_cache_t::extent_size);
    fill(alloc_pid, sizeof(PageID));
}

void alloc_page_log::redo(fixable_page_h* p)
{
    PageID pid = *((PageID*) data_ssx());

    alloc_page* apage = (alloc_page*) p->get_generic_page();
    uint32_t index = pid % alloc_page::bits_held;
    apage->set_bit(index);
}

dealloc_page_log::dealloc_page_log(PageID pid)
{
    memcpy(data_ssx(), &pid, sizeof(PageID));
    PageID alloc_pid = pid - (pid % alloc_cache_t::extent_size);
    fill(alloc_pid, sizeof(PageID));
}

void dealloc_page_log::redo(fixable_page_h* p)
{
    PageID pid = *((PageID*) data_ssx());

    alloc_page* apage = (alloc_page*) p->get_generic_page();
    uint32_t index = pid % alloc_page::bits_held;
    apage->unset_bit(index);
}

page_img_format_t::page_img_format_t (const btree_page_h& page)
{
    size_t unused_length;
    char* unused = page.page()->unused_part(unused_length);

    const char *pp_bin = (const char *) page._pp;
    beginning_bytes = unused - pp_bin;
    ending_bytes    = sizeof(btree_page) - (beginning_bytes + unused_length);

    ::memcpy (data, pp_bin, beginning_bytes);
    ::memcpy (data + beginning_bytes, unused + unused_length, ending_bytes);
    w_assert1(beginning_bytes >= btree_page::hdr_sz);
    w_assert1(beginning_bytes + ending_bytes <= sizeof(btree_page));
}

void page_img_format_t::apply(fixable_page_h* page)
{
    w_assert1(beginning_bytes >= btree_page::hdr_sz);
    w_assert1(beginning_bytes + ending_bytes <= sizeof(btree_page));
    char *pp_bin = (char *) page->get_generic_page();
    ::memcpy (pp_bin, data, beginning_bytes);
    ::memcpy (pp_bin + sizeof(btree_page) - ending_bytes,
            data + beginning_bytes, ending_bytes);
}

page_img_format_log::page_img_format_log(const btree_page_h &page) {
    fill(page,
         (new (_data) page_img_format_t(page))->size());
}

void page_img_format_log::undo(fixable_page_h*) {
    // we don't have to do anything for UNDO
    // because this is a page creation!
}
void page_img_format_log::redo(fixable_page_h* page) {
    // REDO is simply applying the image
    page_img_format_t* dp = (page_img_format_t*) _data;
    dp->apply(page);
}



/*********************************************************************
 *
 *  operator<<(ostream, logrec)
 *
 *  Pretty print a log record to ostream.
 *
 *********************************************************************/
#include "logtype_gen.h"
ostream&
operator<<(ostream& o, const logrec_t& l)
{
    ios_fmtflags        f = o.flags();
    o.setf(ios::left, ios::left);

    o << "LSN=" << l.lsn_ck() << " ";
    const char *rb = l.is_rollback()? "U" : "F"; // rollback/undo or forward

    o << "len=" << l.length() << " ";

    if (!l.is_single_sys_xct()) {
        o << "TID=" << l.tid() << ' ';
    } else {
        o << "TID=SSX" << ' ';
    }
    o << l.type_str() << ":" << l.cat_str() << ":" << rb;
    o << "  p(" << l.pid() << ")";
    if (l.is_multi_page()) {
        o << " src-" << l.pid2();
    }

    switch(l.type()) {
        case t_chkpt_xct_tab:
            {
                chkpt_xct_tab_t* dp = (chkpt_xct_tab_t*) l.data();
                o << " xct_count: " << dp->count;
                break;
            }
        case t_chkpt_bf_tab:
            {
                chkpt_bf_tab_t* dp = (chkpt_bf_tab_t*) l.data();
                o << " dirty_page_count: " << dp->count;
                break;
            }
        case t_comment :
            {
                o << (const char *)l._data;
                break;
            }
        case t_page_evict:
            {
                page_evict_t* pev = (page_evict_t*) l._data;
                o << " slot: " << pev->_child_slot << " emlsn: "
                    << pev->_child_lsn;
                break;
            }
        case t_alloc_page:
        case t_dealloc_page:
            {
                o << " page: " << *((PageID*) (l.data_ssx()));
                break;
            }
        case t_create_store:
            {
                o << " stid: " <<  *((StoreID*) l.data_ssx());
                o << " root_pid: " << *((PageID*) (l.data_ssx() + sizeof(StoreID)));
                break;
            }
        case t_page_write:
            {
                PageID first = *((PageID*) (l.data()));
                PageID last = first + *((uint32_t*) (l.data() + sizeof(PageID) + sizeof(lsn_t))) - 1;
                o << " pids: " << first << "-" << last;
                break;
            }
        case t_restore_segment:
            {
                o << " segment: " << *((uint32_t*) l.data_ssx());
                break;
            }


        default: /* nothing */
                break;
    }

    if (!l.is_single_sys_xct()) {
        if (l.is_cpsn())  o << " (UNDO-NXT=" << l.undo_nxt() << ')';
        else  o << " [UNDO-PRV=" << l.xid_prev() << "]";
    }

    o << " [page-prv " << l.page_prev_lsn();
    if (l.is_multi_page()) {
        o << " page2-prv " << l.page2_prev_lsn();
    }
    o << "]";

    o.flags(f);
    return o;
}

// nothing needed so far..
class page_set_to_be_deleted_t {
public:
    page_set_to_be_deleted_t(){}
    int size()  { return 0;}
};

page_set_to_be_deleted_log::page_set_to_be_deleted_log(const fixable_page_h& p)
{
    fill(p, (new (_data) page_set_to_be_deleted_t()) ->size());
}


void page_set_to_be_deleted_log::redo(fixable_page_h* page)
{
    rc_t rc = page->set_to_be_deleted(false); // no log
    if (rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

void page_set_to_be_deleted_log::undo(fixable_page_h* page)
{
    page->unset_to_be_deleted();
}

create_store_log::create_store_log(PageID root_pid, StoreID snum)
{
    memcpy(data_ssx(), &snum, sizeof(StoreID));
    memcpy(data_ssx() + sizeof(StoreID), &root_pid, sizeof(PageID));
    PageID stpage_pid(stnode_page::stpid);
    fill(stpage_pid, snum, 0, sizeof(StoreID) + sizeof(PageID));
}

void create_store_log::redo(fixable_page_h* page)
{
    StoreID snum = *((StoreID*) data_ssx());
    PageID root_pid = *((PageID*) (data_ssx() + sizeof(StoreID)));

    stnode_page* stpage = (stnode_page*) page->get_generic_page();
    if (stpage->pid != stnode_page::stpid) {
        stpage->pid = stnode_page::stpid;
    }
    stpage->set_root(snum, root_pid);
}

append_extent_log::append_extent_log(extent_id_t ext)
{
    memcpy(data_ssx(), &ext, sizeof(extent_id_t));
    PageID stpage_pid(stnode_page::stpid);
    fill(stpage_pid, 0, 0, sizeof(extent_id_t));
}

void append_extent_log::redo(fixable_page_h* page)
{
    extent_id_t ext = *((extent_id_t*) data_ssx());
    stnode_page* stpage = (stnode_page*) page->get_generic_page();
    stpage->set_last_extent(ext);
}

#if LOGREC_ACCOUNTING

class logrec_accounting_impl_t {
private:
    static __thread uint64_t bytes_written_fwd [t_max_logrec];
    static __thread uint64_t bytes_written_bwd [t_max_logrec];
    static __thread uint64_t bytes_written_bwd_cxt [t_max_logrec];
    static __thread uint64_t insertions_fwd [t_max_logrec];
    static __thread uint64_t insertions_bwd [t_max_logrec];
    static __thread uint64_t insertions_bwd_cxt [t_max_logrec];
    static __thread double            ratio_bf       [t_max_logrec];
    static __thread double            ratio_bf_cxt   [t_max_logrec];

    static const char *type_str(int _type);
    static void reinit();
public:
    logrec_accounting_impl_t() {  reinit(); }
    ~logrec_accounting_impl_t() {}
    static void account(logrec_t &l, bool fwd);
    static void account_end(bool fwd);
    static void print_account_and_clear();
};
static logrec_accounting_impl_t dummy;
void logrec_accounting_impl_t::reinit()
{
    for(int i=0; i < t_max_logrec; i++) {
        bytes_written_fwd[i] =
        bytes_written_bwd[i] =
        bytes_written_bwd_cxt[i] =
        insertions_fwd[i] =
        insertions_bwd[i] =
        insertions_bwd_cxt[i] =  0;
        ratio_bf[i] = 0.0;
        ratio_bf_cxt[i] = 0.0;
    }
}
// this doesn't have to be thread-safe, as I'm using it only
// to figure out the ratios
void logrec_accounting_t::account(logrec_t &l, bool fwd)
{
    logrec_accounting_impl_t::account(l,fwd);
}
void logrec_accounting_t::account_end(bool fwd)
{
    logrec_accounting_impl_t::account_end(fwd);
}

void logrec_accounting_impl_t::account_end(bool fwd)
{
    // Set the context to end so we can account for all
    // overhead related to that.
    if(!fwd) {
        undoing_context = logrec_t::t_xct_end;
    }
}
void logrec_accounting_impl_t::account(logrec_t &l, bool fwd)
{
    unsigned b = l.length();
    int      t = l.type();
    int      tcxt = l.type();
    if(fwd) {
        w_assert0((undoing_context == logrec_t::t_max_logrec)
               || (undoing_context == logrec_t::t_xct_end));
    } else {
        if(undoing_context != logrec_t::t_max_logrec) {
            tcxt = undoing_context;
        } else {
            // else it's something like a compensate  or xct_end
            // and we'll chalk it up to t_xct_abort, which
            // is not undoable.
            tcxt = t_xct_abort;
        }
    }
    if(fwd) {
        bytes_written_fwd[t] += b;
        insertions_fwd[t] ++;
    }
    else {
        bytes_written_bwd[t] += b;
        bytes_written_bwd_cxt[tcxt] += b;
        insertions_bwd[t] ++;
        insertions_bwd_cxt[tcxt] ++;
    }
    if(bytes_written_fwd[t]) {
        ratio_bf[t] = double(bytes_written_bwd_cxt[t]) /
            double(bytes_written_fwd[t]);
    } else {
        ratio_bf[t] = 1;
    }
    if(bytes_written_fwd[tcxt]) {
        ratio_bf_cxt[tcxt] = double(bytes_written_bwd_cxt[tcxt]) /
            double(bytes_written_fwd[tcxt]);
    } else {
        ratio_bf_cxt[tcxt] = 1;
    }
}

const char *logrec_accounting_impl_t::type_str(int _type) {
    switch (_type)  {
#        include "logstr_gen.cpp"
    default:
      return 0;
    }
}

void logrec_accounting_t::print_account_and_clear()
{
    logrec_accounting_impl_t::print_account_and_clear();
}
void logrec_accounting_impl_t::print_account_and_clear()
{
    uint64_t anyb=0;
    for(int i=0; i < t_max_logrec; i++) {
        anyb += insertions_bwd[i];
    }
    if(!anyb) {
        reinit();
        return;
    }
    // don't bother unless there was an abort.
    // I mean something besides just compensation records
    // being chalked up to bytes backward or insertions backward.
    if( insertions_bwd[t_compensate] == anyb ) {
        reinit();
        return;
    }

    char out[200]; // 120 is adequate
    sprintf(out,
        "%s %20s  %8s %8s %8s %12s %12s %12s %10s %10s PAGESIZE %d\n",
        "LOGREC",
        "record",
        "ins fwd", "ins bwd", "rec undo",
        "bytes fwd", "bytes bwd",  "bytes undo",
        "B:F",
        "BUNDO:F",
        SM_PAGESIZE
        );
    fprintf(stdout, "%s", out);
    uint64_t btf=0, btb=0, btc=0;
    uint64_t itf=0, itb=0, itc=0;
    for(int i=0; i < t_max_logrec; i++) {
        btf += bytes_written_fwd[i];
        btb += bytes_written_bwd[i];
        btc += bytes_written_bwd_cxt[i];
        itf += insertions_fwd[i];
        itb += insertions_bwd[i];
        itc += insertions_bwd_cxt[i];

        if( insertions_fwd[i] + insertions_bwd[i] + insertions_bwd_cxt[i] > 0)
        {
            sprintf(out,
            "%s %20s  %8lu %8lu %8lu %12lu %12lu %12lu %10.7f %10.7f PAGESIZE %d \n",
            "LOGREC",
            type_str(i) ,
            insertions_fwd[i],
            insertions_bwd[i],
            insertions_bwd_cxt[i],
            bytes_written_fwd[i],
            bytes_written_bwd[i],
            bytes_written_bwd_cxt[i],
            ratio_bf[i],
            ratio_bf_cxt[i],
            SM_PAGESIZE
            );
            fprintf(stdout, "%s", out);
        }
    }
    sprintf(out,
    "%s %20s  %8lu %8lu %8lu %12lu %12lu %12lu %10.7f %10.7f PAGESIZE %d\n",
    "LOGREC",
    "TOTAL",
    itf, itb, itc,
    btf, btb, btc,
    double(btb)/double(btf),
    double(btc)/double(btf),
    SM_PAGESIZE
    );
    fprintf(stdout, "%s", out);
    reinit();
}

__thread uint64_t logrec_accounting_impl_t::bytes_written_fwd [t_max_logrec];
__thread uint64_t logrec_accounting_impl_t::bytes_written_bwd [t_max_logrec];
__thread uint64_t logrec_accounting_impl_t::bytes_written_bwd_cxt [t_max_logrec];
__thread uint64_t logrec_accounting_impl_t::insertions_fwd [t_max_logrec];
__thread uint64_t logrec_accounting_impl_t::insertions_bwd [t_max_logrec];
__thread uint64_t logrec_accounting_impl_t::insertions_bwd_cxt [t_max_logrec];
__thread double            logrec_accounting_impl_t::ratio_bf       [t_max_logrec];
__thread double            logrec_accounting_impl_t::ratio_bf_cxt   [t_max_logrec];

#endif
