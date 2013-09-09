/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define LOGREC_C
#ifdef __GNUG__
#   pragma implementation
#endif
#include "sm_int_2.h"
#include "logdef_gen.cpp"
#include "vec_t.h"
#include "alloc_cache.h"
#include "page_bf_inline.h"

#include <iomanip>
typedef        ios::fmtflags        ios_fmtflags;

#include <new>

enum {         eINTERNAL = smlevel_0::eINTERNAL,
        eOUTOFMEMORY = smlevel_0::eOUTOFMEMORY
        };

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
logrec_t::type_str() const
{
    switch (header._type)  {
#        include "logstr_gen.cpp"
    default:
      return 0;
    }

    /*
     *  Not reached.
     */
    W_FATAL(smlevel_0::eINTERNAL);
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
logrec_t::fill(const lpid_t* p, uint16_t tag, smsize_t l)
{
    w_assert9(hdr_sz == _data - (char*) this);
    w_assert9(w_base_t::is_aligned(_data));

    /* adjust _cat */
    xct_t *x = xct();
    if(smlevel_0::in_recovery_undo() ||
        (x && ( x->rolling_back() ||
			   x->state() == smlevel_1::xct_aborting))
	) 
    {
        header._cat |= t_rollback;
    }
    set_pid(lpid_t::null);
    if (!is_single_sys_xct()) { // prv does not exist in single-log system transaction
        set_xid_prev(lsn_t::null);
    }
    header._page_tag = tag;
    if (p) set_pid(*p);
    char *dat = is_single_sys_xct() ? data_ssx() : data();
    if (l != align(l)) {
        // zero out extra space to keep purify happy
        memset(dat+l, 0, align(l)-l);
    }
    unsigned int tmp = align(l) + (is_single_sys_xct() ? hdr_single_sys_xct_sz : hdr_non_ssx_sz) + sizeof(lsn_t);
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
    if (header._len < (is_single_sys_xct() ? hdr_single_sys_xct_sz : hdr_non_ssx_sz)
        || header._type > 100 || cat() == t_bad_cat || 
        lsn != *_lsn_ck()) {
        return false;
    }
    return true;
}


/*********************************************************************
 *
 *  logrec_t::redo(page)
 *
 *  Invoke the redo method of the log record.
 *
 *********************************************************************/
void logrec_t::redo(generic_page_h* page)
{
    FUNC(logrec_t::redo);
    DBG( << "Redo  log rec: " << *this 
        << " size: " << header._len << " xid_prevlsn: " << (is_single_sys_xct() ? lsn_t::null : xid_prev()) );

    switch (header._type)  {
#include "redo_gen.cpp"
    }
    
    /*
     *  Page is dirty after redo.
     *  (not all redone log records have a page)
     *  NB: the page lsn in set by the caller (in restart.cpp)
     *  This is ok in recovery because in this phase, there
     *  is not a bf_cleaner thread running. (that thread asserts
     *  that if the page is dirty, its lsn is non-null, and we
     *  have a short-lived violation of that right here).
     */
    if(page) page->set_dirty();
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
logrec_t::undo(generic_page_h* page)
{
    w_assert0(!is_single_sys_xct()); // UNDO shouldn't be called for single-log sys xct
    undoing_context = logrec_t::kind_t(header._type);
    FUNC(logrec_t::undo);
    DBG( << "Undo  log rec: " << *this 
        << " size: " << header._len  << " xid_prevlsn: " << xid_prev());
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
    fill(0, 0, 0);
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
    w_assert9( SIZEOF(*this) <= logrec_t::data_sz);
    for (uint i = 0; i < count; i++)  {
        xrec[i].tid = xct[i]->tid();
    }
}

xct_end_group_log::xct_end_group_log(const xct_t *list[], int listlen)
{
    fill(0, 0, (new (_data) xct_list_t(list, listlen))->size());
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
    fill(0, 0, 0);
}

// We use a different log record type here only for debugging purposes
xct_abort_log::xct_abort_log()
{
    fill(0, 0, 0);
}



/*********************************************************************
 *
 *  xct_prepare_st_log -- start prepare info for a tx
 *  xct_prepare_lk_log -- add locks to the tx
 *  xct_prepare_fi_log -- fin prepare info for the tx
 *
 *  For 2 phase commit.
 *
 *********************************************************************/

xct_prepare_st_log::xct_prepare_st_log(const gtid_t *g, 
        const server_handle_t &h)
{
    fill(0, 0, (new (_data) prepare_info_t(g, h))->size());

}

void
xct_prepare_st_log::redo(generic_page_h *) 
{
    w_assert0(!is_single_sys_xct()); // single-log sys xct shouldn't call prepare
    /*
     *  Update xct state 
     */
    const prepare_info_t*         pt = (prepare_info_t*) _data;
    xct_t *                        xd = xct_t::look_up(tid());
    w_assert9(xd);
    if(pt->is_external==1) {
        W_COERCE(xd->enter2pc(pt->g));
    }
    xd->set_coordinator(pt->h);
}


xct_prepare_lk_log::xct_prepare_lk_log(int num, 
        lock_base_t::lmode_t mode, lockid_t *locks)
{
    fill(0, 0, (new (_data) prepare_lock_t(num, mode, locks))->size());
}

xct_prepare_alk_log::xct_prepare_alk_log(int num, 
        lockid_t *locks,
        lock_base_t::lmode_t *modes
        )
{
    fill(0, 0, (new (_data) prepare_all_lock_t(num, locks, modes))->size());
}

void
xct_prepare_lk_log::redo(generic_page_h *) 
{
    w_assert0(!is_single_sys_xct()); // single-log sys xct shouldn't call prepare
    /*
     *  Acquire all the locks within. (all of same mode)
     *  For lm->lock() to work, we have to 
     *  attach the xct
     */
    const prepare_lock_t*         pt = (prepare_lock_t*) _data;
    xct_t *                        xd = xct_t::look_up(tid());
    me()->attach_xct(xd);
    W_COERCE(xd->obtain_locks(pt->mode, pt->num_locks, pt->name));
    me()->detach_xct(xd);
}

void
xct_prepare_alk_log::redo(generic_page_h *) 
{
    w_assert0(!is_single_sys_xct()); // single-log sys xct shouldn't call prepare
    /*
     *  Acquire all the locks within. (different modes)
     *  For lm->lock() to work, we have to 
     *  attach the xct
     */
    const prepare_all_lock_t*         pt = (prepare_all_lock_t*) _data;
    xct_t *                        xd = xct_t::look_up(tid());
    me()->attach_xct(xd);
    uint32_t i;
    for (i=0; i<pt->num_locks; i++) {
        W_COERCE(xd->obtain_one_lock(pt->pair[i].mode, pt->pair[i].name));
    }
    me()->detach_xct(xd);
}


xct_prepare_fi_log::xct_prepare_fi_log(
        int num_ex, int num_ix, int num_six, 
        const lsn_t &first,
        int rsvd, int ready, int used
        ) 
{
    fill(0, 0, (new (_data) 
        prepare_end_t(num_ex, num_ix, num_six, first,
                      rsvd, ready, used))->size());
}
void 
xct_prepare_fi_log::redo(generic_page_h *) 
{
    w_assert0(!is_single_sys_xct()); // single-log sys xct shouldn't call prepare
    /*
     *  check that the right number of
     *  locks was acquired, and set the state
     *  -- don't set the state any earlier so that
     *  txs that didn't get the prepare fin logged
     *  are aborted during the undo phase
     */
    xct_t *                        xd = xct_t::look_up(tid());
    const prepare_end_t* endinfo = (prepare_end_t*) _data;
    const prepare_lock_totals_t* pt =  &endinfo->_lock;
    me()->attach_xct(xd);
    xd->change_state(xct_t::xct_prepared);

    w_assert9( xd->first_lsn() == lsn_t::null );
    xd->set_first_lsn(pt->first_lsn);

    const prepare_log_resv_t* pl =  &endinfo->_log;
    xd->prepare_restore_log_resv(pl->_rsvd, pl->_ready, pl->_used, 
            endinfo->size());

    me()->detach_xct(xd);
    w_assert9(xd->state() == xct_t::xct_prepared);
}

xct_prepare_stores_log::xct_prepare_stores_log(int num, const stid_t* stids)
{
    fill(0, 0, (new (_data) prepare_stores_to_free_t(num, stids))->size());
}

void
xct_prepare_stores_log::redo(generic_page_h *)
{
    w_assert0(!is_single_sys_xct()); // single-log sys xct shouldn't call prepare
    xct_t*        xd = xct_t::look_up(tid());
    const prepare_stores_to_free_t* info = (prepare_stores_to_free_t*)_data;
    me()->attach_xct(xd);
    for (uint32_t i = 0; i < info->num; i++)  {
        xd->AddStoreToFree(info->stids[i]);
    }
    me()->detach_xct(xd);
    w_assert9(xd->state() == xct_t::xct_prepared);
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
    fill(0, 0, strlen(msg)+1);
}

void 
comment_log::redo(generic_page_h * W_IFDEBUG9(page))
{
    w_assert9(page == 0);
    DBG(<<"comment_log: R: " << (const char *)_data);
    ; // just for the purpose of setting breakpoints
}

void 
comment_log::undo(generic_page_h * W_IFDEBUG9(page))
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
    fill(0, 0, 0);
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
    fill(0, 0, 0);
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
    fill(0, 0, sizeof(lsn_t));
}




/*********************************************************************
 *
 *  chkpt_end_log(const lsn_t &master, const lsn_t& min_rec_lsn)
 *
 *  Status Log to mark completion of fussy checkpoint.
 *  Master is the lsn of the record that began this chkpt.
 *  min_rec_lsn is the lsn of the record that began this chkpt.
 *
 *********************************************************************/
chkpt_end_log::chkpt_end_log(const lsn_t &lsn, const lsn_t& min_rec_lsn)
{
    // initialize _data
    lsn_t *l = new (_data) lsn_t(lsn);
    l++; //grot
    *l = min_rec_lsn;

    fill(0, 0, 2 * sizeof(lsn_t));
}



/*********************************************************************
 *
 *  chkpt_bf_tab_log
 *
 *  Data Log to save dirty page table at checkpoint.
 *  Contains, for each dirty page, its pid and recovery lsn.
 *
 *********************************************************************/

chkpt_bf_tab_t::chkpt_bf_tab_t(
    int                 cnt,        // I-  # elements in pids[] and rlsns[]
    const lpid_t*         pids,        // I-  id of of dirty pages
    const lsn_t*         rlsns)        // I-  rlsns[i] is recovery lsn of pids[i]
    : count(cnt)
{
    w_assert1( sizeof(*this) <= logrec_t::max_data_sz );
    w_assert1(count <= max);
    for (uint i = 0; i < count; i++) {
        brec[i].pid = pids[i];
        brec[i].rec_lsn = rlsns[i];
    }
}


chkpt_bf_tab_log::chkpt_bf_tab_log(
    int                 cnt,        // I-  # elements in pids[] and rlsns[]
    const lpid_t*         pid,        // I-  id of of dirty pages
    const lsn_t*         rec_lsn)// I-  rlsns[i] is recovery lsn of pids[i]
{
    fill(0, 0, (new (_data) chkpt_bf_tab_t(cnt, pid, rec_lsn))->size());
}




/*********************************************************************
 *
 *  chkpt_xct_tab_log
 *
 *  Data log to save transaction table at checkpoint.
 *  Contains, for each active xct, its id, state, last_lsn
 *  and undo_nxt lsn. 
 *
 *********************************************************************/
chkpt_xct_tab_t::chkpt_xct_tab_t(
    const tid_t&                         _youngest,
    int                                 cnt,
    const tid_t*                         tid,
    const smlevel_1::xct_state_t*         state,
    const lsn_t*                         last_lsn,
    const lsn_t*                         undo_nxt)
    : youngest(_youngest), count(cnt)
{
    w_assert1(count <= max);
    w_assert9( SIZEOF(*this) <= logrec_t::data_sz);
    for (uint i = 0; i < count; i++)  {
        xrec[i].tid = tid[i];
        xrec[i].state = state[i];
        xrec[i].last_lsn = last_lsn[i];
        xrec[i].undo_nxt = undo_nxt[i];
    }
}
    
chkpt_xct_tab_log::chkpt_xct_tab_log(
    const tid_t&                         youngest,
    int                                 cnt,
    const tid_t*                         tid,
    const smlevel_1::xct_state_t*         state,
    const lsn_t*                         last_lsn,
    const lsn_t*                         undo_nxt)
{
    fill(0, 0, (new (_data) chkpt_xct_tab_t(youngest, cnt, tid, state,
                                         last_lsn, undo_nxt))->size());
}




/*********************************************************************
 *
 *  chkpt_dev_tab_log
 *
 *  Data log to save devices mounted at checkpoint.
 *  Contains, for each device mounted, its devname and vid.
 *
 *********************************************************************/
chkpt_dev_tab_t::chkpt_dev_tab_t(
    int                 cnt,
    const char          **dev,
    const vid_t*        vid)
    : count(cnt)
{
    w_assert9( sizeof(*this) <= logrec_t::data_sz );
    w_assert1(count <= max);
    for (uint i = 0; i < count; i++) {
        // zero out everything and then set the string
        memset(devrec[i].dev_name, 0, sizeof(devrec[i].dev_name));
        strncpy(devrec[i].dev_name, dev[i], sizeof(devrec[i].dev_name)-1);
        devrec[i].vid = vid[i];
    }
}

chkpt_dev_tab_log::chkpt_dev_tab_log(
    int                 cnt,
    const char                 **dev,
    const vid_t*         vid)
{
    fill(0, 0, (new (_data) chkpt_dev_tab_t(cnt, dev, vid))->size());
}


/*********************************************************************
 *
 *  mount_vol_log
 *
 *  Data log to save device mounts.
 *
 *********************************************************************/
mount_vol_log::mount_vol_log(
    const char*         dev,
    const vid_t&         vid)
{
    const char                *devArray[1];

    devArray[0] = dev;
    DBG(<< "mount_vol_log dev_name=" << devArray[0] << " volid =" << vid);
    fill(0, 0, (new (_data) chkpt_dev_tab_t(1, devArray, &vid))->size());
}


void mount_vol_log::redo(generic_page_h* W_IFDEBUG9(page))
{
    w_assert9(page == 0);
    chkpt_dev_tab_t* dp = (chkpt_dev_tab_t*) _data;

    w_assert9(dp->count == 1);

    // this may fail since this log record is only redone on crash/restart and the
        // user may have destroyed the volume after using, but then there won't be
        // and pages that need to be updated on this volume.
    W_IGNORE(io_m::mount(dp->devrec[0].dev_name, dp->devrec[0].vid));
}


/*********************************************************************
 *
 *  dismount_vol_log
 *
 *  Data log to save device dismounts.
 *
 *********************************************************************/
dismount_vol_log::dismount_vol_log(
    const char*                dev,
    const vid_t&         vid)
{
    const char                *devArray[1];

    devArray[0] = dev;
    DBG(<< "dismount_vol_log dev_name=" << devArray[0] << " volid =" << vid);
    fill(0, 0, (new (_data) chkpt_dev_tab_t(1, devArray, &vid))->size());
}


void dismount_vol_log::redo(generic_page_h* W_IFDEBUG9(page))
{
    w_assert9(page == 0);
    chkpt_dev_tab_t* dp = (chkpt_dev_tab_t*) _data;

    w_assert9(dp->count == 1);
    // this may fail since this log record is only redone on crash/restart and the
        // user may have destroyed the volume after using, but then there won't be
        // and pages that need to be updated on this volume.
    W_IGNORE(io_m::dismount(dp->devrec[0].vid, true));
}

/**
 * This is a special way of logging the creation of a new page.
 * New page creation is usually a page split, so the new page has many
 * records in it. To simplify and to avoid many log entries in that case,
 * we log ALL bytes from the beginning to the end of slot vector,
 * and from the record_head8 to the end of page.
 * We can assume totally defragmented page image because this is page creation.
 * We don't need UNDO (again, this is page creation!), REDO is just two memcpy().
 */
struct page_img_format_t {
    size_t      beginning_bytes;
    size_t      ending_bytes;
    char        data[logrec_t::max_data_sz - 2 * sizeof(size_t)];
    int size()        { return 2 * sizeof(size_t) + beginning_bytes + ending_bytes; }
    page_img_format_t (const generic_page_h& page);
};
page_img_format_t::page_img_format_t (const generic_page_h& page)
{
    beginning_bytes = generic_page::hdr_sz + page.nslots() * generic_page::slot_sz;
    ending_bytes = generic_page::page_sz - page._pp->get_record_head_byte() - generic_page::hdr_sz;
    const char *pp_bin = (const char *) page._pp;
    ::memcpy (data, pp_bin, beginning_bytes);
    ::memcpy (data + beginning_bytes, pp_bin + generic_page::hdr_sz + page._pp->get_record_head_byte(), ending_bytes);
}

page_img_format_log::page_img_format_log(const generic_page_h &page)
{
    fill(&page.pid(), page.tag(),
         (new (_data) page_img_format_t(page))->size());
}

void page_img_format_log::undo(generic_page_h*)
{
    // we don't have to do anything for UNDO
    // because this is a page creation!
}
void page_img_format_log::redo(generic_page_h* page)
{
    // REDO is simply applying the image
    page_img_format_t* dp = (page_img_format_t*) _data;
    w_assert1(dp->beginning_bytes >= generic_page::hdr_sz);
    w_assert1(dp->beginning_bytes + dp->ending_bytes <= generic_page::page_sz);
    char *pp_bin = (char *) page->_pp;
    ::memcpy (pp_bin, dp->data, dp->beginning_bytes);
    ::memcpy (pp_bin + generic_page::page_sz - dp->ending_bytes, dp->data + dp->beginning_bytes, dp->ending_bytes);
    page->set_dirty();
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

    const char *rb = l.is_rollback()? "U" : "F"; // rollback/undo or forward

    if (!l.is_single_sys_xct()) {
        o << l.tid() << ' ';
    }
    W_FORM(o)("%19s%5s:%1s", l.type_str(), l.cat_str(), rb );
    o << "  " << l.pid();

    switch(l.type()) {
        case t_comment : 
                {
                    o << (const char *)l._data;
                }
                break;

        case t_store_operation:
                {
                    store_operation_param& param = *(store_operation_param*)l._data;
                    o << ' ' << param;
                }
                break;

        default: /* nothing */
                break;
    }

    if (!l.is_single_sys_xct()) {
        if (l.is_cpsn())  o << " (" << l.undo_nxt() << ')';
        else  o << " [" << l.xid_prev() << "]";
    }

    o.flags(f);
    return o;
}

// nothing needed so far..
class page_set_tobedeleted_t {
public:
    page_set_tobedeleted_t(){}
    int size()  { return 0;}
};

page_set_tobedeleted_log::page_set_tobedeleted_log(const generic_page_h& p)
{
    fill(&p.pid(), p.tag(), 
        (new (_data) page_set_tobedeleted_t()) ->size());
}


void page_set_tobedeleted_log::redo(generic_page_h* page)
{
    rc_t rc = page->set_tobedeleted(false); // no log
    if (rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

void page_set_tobedeleted_log::undo(generic_page_h* page)
{
    page->unset_tobedeleted();
}



 
alloc_a_page_log::alloc_a_page_log (vid_t vid, shpid_t pid)
{
    // page alloation is single-log system transaction. so, use data_ssx()
    char *buf = data_ssx();
    *reinterpret_cast<shpid_t*>(buf) = pid; // only data is the page ID
    lpid_t dummy (vid, 0, 0);
    fill(&dummy, 0, sizeof(shpid_t));
    w_assert0(is_single_sys_xct());
}

void alloc_a_page_log::redo(generic_page_h*)
{
    w_assert1(g_xct());
    w_assert1(g_xct()->is_single_log_sys_xct());
    // page alloation is single-log system transaction. so, use data_ssx()
    shpid_t pid = *((shpid_t*) data_ssx());

    // actually this is logical REDO    
    rc_t rc = io_m::redo_alloc_a_page(header._vid, pid);
    if (rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

alloc_consecutive_pages_log::alloc_consecutive_pages_log (vid_t vid, shpid_t pid_begin, uint32_t page_count)
{
    // page alloation is single-log system transaction. so, use data_ssx()
    uint32_t *buf = reinterpret_cast<uint32_t*>(data_ssx());
    buf[0] = pid_begin;
    buf[1] = page_count;
    lpid_t dummy (vid, 0, 0);
    fill(&dummy, 0, sizeof(uint32_t) * 2);
    w_assert0(is_single_sys_xct());
}

void alloc_consecutive_pages_log::redo(generic_page_h*)
{
    // page alloation is single-log system transaction. so, use data_ssx()
    uint32_t *buf = reinterpret_cast<uint32_t*>(data_ssx());
    shpid_t pid_begin = buf[0];
    uint32_t page_count = buf[1];

    // logical redo.
    rc_t rc = io_m::redo_alloc_consecutive_pages(header._vid, page_count, pid_begin);
    if (rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

 
dealloc_a_page_log::dealloc_a_page_log (vid_t vid, shpid_t pid)
{
    // page dealloation is single-log system transaction. so, use data_ssx()
    char *buf = data_ssx();
    *reinterpret_cast<shpid_t*>(buf) = pid; // only data is the page ID
    lpid_t dummy (vid, 0, 0);
    fill(&dummy, 0, sizeof(shpid_t));
    w_assert0(is_single_sys_xct());
}

void dealloc_a_page_log::redo(generic_page_h*)
{
    // page dealloation is single-log system transaction. so, use data_ssx()
    shpid_t pid = *((shpid_t*) data_ssx());

    // logical redo.
    rc_t rc = io_m::redo_dealloc_a_page(header._vid, pid);
    if (rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

store_operation_log::store_operation_log(const store_operation_param& param)
{
    fill(0, 0, (new (_data) store_operation_param(param))->size());
}

void store_operation_log::redo(generic_page_h* /*page*/)
{
    store_operation_param& param = *(store_operation_param*)_data;
    DBG( << "store_operation_log::redo(page=" << pid() 
        << ", param=" << param << ")" );
    W_COERCE( smlevel_0::io->store_operation(vid(), param) );
}

void store_operation_log::undo(generic_page_h* /*page*/)
{
    store_operation_param& param = *(store_operation_param*)_data;
    DBG( << "store_operation_log::undo(page=" << shpid() << ", param=" << param << ")" );

    switch (param.op())  {
        case smlevel_0::t_delete_store:
            /* do nothing, not undoable */
            break;
        case smlevel_0::t_create_store:
            // TODO implement destroy_store
            /*
            {
                stid_t stid(vid(), param.snum());
                W_COERCE( smlevel_0::io->destroy_store(stid) );
            }
            */
            break;
        case smlevel_0::t_set_deleting:
            switch (param.new_deleting_value())  {
                case smlevel_0::t_not_deleting_store:
                case smlevel_0::t_deleting_store:
                    {
                        store_operation_param new_param(param.snum(), 
                                smlevel_0::t_set_deleting,
                                param.old_deleting_value(), 
                                param.new_deleting_value());
                        W_COERCE( smlevel_0::io->store_operation(vid(), 
                                new_param) );
                    }
                    break;
                case smlevel_0::t_unknown_deleting:
                    W_FATAL(smlevel_0::eINTERNAL);
                    break;
            }
            break;
        case smlevel_0::t_set_store_flags:
            {
                store_operation_param new_param(param.snum(), 
                        smlevel_0::t_set_store_flags,
                        param.old_store_flags(), param.new_store_flags());
                W_COERCE( smlevel_0::io->store_operation(vid(), 
                        new_param) );
            }
            break;
        case smlevel_0::t_set_root:
            /* do nothing, not undoable */
            break;
    }
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
