/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define LOGREC_C

#include "eventlog.h"

#include "sm_base.h"
#include "vec_t.h"
#include "alloc_cache.h"
#include "vol.h"
#include "restore.h"
#include <sstream>
#include "logrec_support.h"
#include "btree_page_h.h"

#include <iomanip>
typedef        ios::fmtflags        ios_fmtflags;

#include <new>

#include "allocator.h"
DECLARE_TLS(block_pool<logrec_t>, logrec_pool);
template<>
logrec_t* sm_tls_allocator::allocate(size_t)
{
    return (logrec_t*) logrec_pool->acquire();
}

template<>
void sm_tls_allocator::release(logrec_t* p, size_t)
{
    logrec_pool->release(p);
}

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
    auto c = cat();
    switch (c)  {
    case t_logical:
        return "l---";

    case t_system:
        return "s---";

    case t_undo:
        return "--u-";

    case t_redo:
        return "-r--";

    case t_undo | t_redo:
        return "-ru-";

    case t_undo | t_redo | t_logical:
        return "lru-";

    case t_redo | t_logical : // used in I/O layer
        return "lr__";

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
	case t_comment :
		return "comment";
	case t_compensate :
		return "compensate";
	case t_skip :
		return "skip";
	case t_chkpt_begin :
		return "chkpt_begin";
	case t_chkpt_bf_tab :
		return "chkpt_bf_tab";
	case t_chkpt_xct_tab :
		return "chkpt_xct_tab";
	case t_chkpt_xct_lock :
		return "chkpt_xct_lock";
	case t_chkpt_restore_tab :
		return "chkpt_restore_tab";
	case t_chkpt_backup_tab :
		return "chkpt_backup_tab";
	case t_chkpt_end :
		return "chkpt_end";
	case t_add_backup :
		return "add_backup";
	case t_xct_abort :
		return "xct_abort";
	case t_xct_end :
		return "xct_end";
	case t_xct_latency_dump :
		return "xct_latency_dump";
	case t_alloc_page :
		return "alloc_page";
	case t_dealloc_page :
		return "dealloc_page";
	case t_create_store :
		return "create_store";
	case t_stnode_format :
		return "stnode_format";
	case t_append_extent :
		return "append_extent";
	case t_loganalysis_begin :
		return "loganalysis_begin";
	case t_loganalysis_end :
		return "loganalysis_end";
	case t_redo_done :
		return "redo_done";
	case t_undo_done :
		return "undo_done";
	case t_restore_begin :
		return "restore_begin";
	case t_restore_segment :
		return "restore_segment";
	case t_restore_end :
		return "restore_end";
	case t_page_img_format :
		return "page_img_format";
	case t_page_evict :
		return "page_evict";
	case t_btree_norec_alloc :
		return "btree_norec_alloc";
	case t_btree_insert :
		return "btree_insert";
	case t_btree_insert_nonghost :
		return "btree_insert_nonghost";
	case t_btree_update :
		return "btree_update";
	case t_btree_overwrite :
		return "btree_overwrite";
	case t_btree_ghost_mark :
		return "btree_ghost_mark";
	case t_btree_ghost_reclaim :
		return "btree_ghost_reclaim";
	case t_btree_ghost_reserve :
		return "btree_ghost_reserve";
	case t_btree_foster_adopt :
		return "btree_foster_adopt";
	case t_btree_split :
		return "btree_split";
	case t_btree_compress_page :
		return "btree_compress_page";
	case t_tick_sec :
		return "tick_sec";
	case t_tick_msec :
		return "tick_msec";
	case t_benchmark_start :
		return "benchmark_start";
	case t_page_write :
		return "page_write";
	case t_page_read :
		return "page_read";
    default:
      return "UNKNOWN";
    }

    /*
     *  Not reached.
     */
    W_FATAL(eINTERNAL);
    return 0;
}

void logrec_t::init_header(kind_t type)
{
    header._flags = 0;
    header._type = type;
    header._pid = 0;
    header._page_tag = 0;
    header._stid = 0;
    // CS TODO: for most logrecs, set_size is called twice
    set_size(0);
}

void logrec_t::set_size(size_t l)
{
    char *dat = is_single_sys_xct() ? data_ssx() : data();
    if (l != ALIGN_BYTE(l)) {
        // zero out extra space to keep purify happy
        memset(dat+l, 0, ALIGN_BYTE(l)-l);
    }
    unsigned int tmp = ALIGN_BYTE(l)
        + (is_single_sys_xct() ? hdr_single_sys_xct_sz : hdr_non_ssx_sz) + sizeof(lsn_t);
    tmp = (tmp + 7) & unsigned(-8); // force 8-byte alignment
    w_assert1(tmp <= sizeof(*this));
    header._len = tmp;
}

void logrec_t::init_xct_info()
{
    /* adjust _cat */
    if (!is_single_sys_xct()) { // prv does not exist in single-log system transaction
        set_xid_prev(lsn_t::null);
    }
    set_tid(0);
}

void logrec_t::set_xid_prev(tid_t tid, lsn_t last)
{
    if (!is_single_sys_xct()) {
        set_tid(tid);
        if(xid_prev().valid()) {
            w_assert2(is_cpsn());
        } else {
            set_xid_prev (last);
        }
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
 *  Invoke the redo method of the log record.
 *********************************************************************/
template <class PagePtr>
void logrec_t::redo(PagePtr page)
{
    DBG( << "Redo  log rec: " << *this
        << " size: " << header._len << " xid_prevlsn: " << (is_single_sys_xct() ? lsn_t::null : xid_prev()) );

    switch (header._type)  {
	case t_comment :
		W_FATAL(eINTERNAL);
		break;
	case t_compensate :
		W_FATAL(eINTERNAL);
		break;
	case t_skip :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_begin :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_bf_tab :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_xct_tab :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_xct_lock :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_restore_tab :
		((chkpt_restore_tab_log *) this)->redo(page);
		break;
	case t_chkpt_backup_tab :
		((chkpt_backup_tab_log *) this)->redo(page);
		break;
	case t_chkpt_end :
		W_FATAL(eINTERNAL);
		break;
	case t_add_backup :
		W_FATAL(eINTERNAL);
		break;
	case t_xct_abort :
		W_FATAL(eINTERNAL);
		break;
	case t_xct_end :
		W_FATAL(eINTERNAL);
		break;
	case t_xct_latency_dump :
		W_FATAL(eINTERNAL);
		break;
	case t_alloc_page :
		((alloc_page_log *) this)->redo(page);
		break;
	case t_dealloc_page :
		((dealloc_page_log *) this)->redo(page);
		break;
	case t_stnode_format :
		((stnode_format_log *) this)->redo(page);
		break;
	case t_create_store :
		((create_store_log *) this)->redo(page);
		break;
	case t_append_extent :
		((append_extent_log *) this)->redo(page);
		break;
	case t_loganalysis_begin :
		W_FATAL(eINTERNAL);
		break;
	case t_loganalysis_end :
		W_FATAL(eINTERNAL);
		break;
	case t_redo_done :
		W_FATAL(eINTERNAL);
		break;
	case t_undo_done :
		W_FATAL(eINTERNAL);
		break;
	case t_restore_begin :
		((restore_begin_log *) this)->redo(page);
		break;
	case t_restore_segment :
		((restore_segment_log *) this)->redo(page);
		break;
	case t_restore_end :
		((restore_end_log *) this)->redo(page);
		break;
	case t_page_img_format :
		((page_img_format_log *) this)->redo(page);
		break;
	case t_page_evict :
		((page_evict_log *) this)->redo(page);
		break;
	case t_btree_norec_alloc :
		((btree_norec_alloc_log *) this)->redo(page);
		break;
	case t_btree_insert :
		((btree_insert_log *) this)->redo(page);
		break;
	case t_btree_insert_nonghost :
		((btree_insert_nonghost_log *) this)->redo(page);
		break;
	case t_btree_update :
		((btree_update_log *) this)->redo(page);
		break;
	case t_btree_overwrite :
		((btree_overwrite_log *) this)->redo(page);
		break;
	case t_btree_ghost_mark :
		((btree_ghost_mark_log *) this)->redo(page);
		break;
	case t_btree_ghost_reclaim :
		((btree_ghost_reclaim_log *) this)->redo(page);
		break;
	case t_btree_ghost_reserve :
		((btree_ghost_reserve_log *) this)->redo(page);
		break;
	case t_btree_foster_adopt :
		((btree_foster_adopt_log *) this)->redo(page);
		break;
	case t_btree_split :
		((btree_split_log *) this)->redo(page);
		break;
	case t_btree_compress_page :
		((btree_compress_page_log *) this)->redo(page);
		break;
	case t_tick_sec :
		W_FATAL(eINTERNAL);
		break;
	case t_tick_msec :
		W_FATAL(eINTERNAL);
		break;
	case t_benchmark_start :
		W_FATAL(eINTERNAL);
		break;
	case t_page_write :
		W_FATAL(eINTERNAL);
		break;
	case t_page_read :
		W_FATAL(eINTERNAL);
		break;
	default :
		W_FATAL(eINTERNAL);
		break;
    }

    page->update_page_lsn(lsn());
    page->set_img_page_lsn(lsn());
}

void logrec_t::redo()
{
    redo<btree_page_h*>(nullptr);
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
template <class PagePtr>
void logrec_t::undo(PagePtr page)
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
	case t_comment :
		W_FATAL(eINTERNAL);
		break;
	case t_compensate :
		W_FATAL(eINTERNAL);
		break;
	case t_skip :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_begin :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_bf_tab :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_xct_tab :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_xct_lock :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_restore_tab :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_backup_tab :
		W_FATAL(eINTERNAL);
		break;
	case t_chkpt_end :
		W_FATAL(eINTERNAL);
		break;
	case t_add_backup :
		W_FATAL(eINTERNAL);
		break;
	case t_xct_abort :
		W_FATAL(eINTERNAL);
		break;
	case t_xct_end :
		W_FATAL(eINTERNAL);
		break;
	case t_xct_latency_dump :
		W_FATAL(eINTERNAL);
		break;
	case t_alloc_page :
		W_FATAL(eINTERNAL);
		break;
	case t_dealloc_page :
		W_FATAL(eINTERNAL);
		break;
	case t_stnode_format :
		W_FATAL(eINTERNAL);
		break;
	case t_create_store :
		W_FATAL(eINTERNAL);
		break;
	case t_append_extent :
		W_FATAL(eINTERNAL);
		break;
	case t_loganalysis_begin :
		W_FATAL(eINTERNAL);
		break;
	case t_loganalysis_end :
		W_FATAL(eINTERNAL);
		break;
	case t_redo_done :
		W_FATAL(eINTERNAL);
		break;
	case t_undo_done :
		W_FATAL(eINTERNAL);
		break;
	case t_restore_begin :
		W_FATAL(eINTERNAL);
		break;
	case t_restore_segment :
		W_FATAL(eINTERNAL);
		break;
	case t_restore_end :
		W_FATAL(eINTERNAL);
		break;
	case t_page_img_format :
		((page_img_format_log *) this)->undo(page);
		break;
	case t_page_evict :
		W_FATAL(eINTERNAL);
		break;
	case t_btree_norec_alloc :
		W_FATAL(eINTERNAL);
		break;
	case t_btree_insert :
		((btree_insert_log *) this)->undo(page);
		break;
	case t_btree_insert_nonghost :
		((btree_insert_nonghost_log *) this)->undo(page);
		break;
	case t_btree_update :
		((btree_update_log *) this)->undo(page);
		break;
	case t_btree_overwrite :
		((btree_overwrite_log *) this)->undo(page);
		break;
	case t_btree_ghost_mark :
		((btree_ghost_mark_log *) this)->undo(page);
		break;
	case t_btree_ghost_reclaim :
		W_FATAL(eINTERNAL);
		break;
	case t_btree_ghost_reserve :
		W_FATAL(eINTERNAL);
		break;
	case t_btree_foster_adopt :
		W_FATAL(eINTERNAL);
		break;
	case t_btree_split :
		W_FATAL(eINTERNAL);
		break;
	case t_btree_compress_page :
		W_FATAL(eINTERNAL);
		break;
	case t_tick_sec :
		W_FATAL(eINTERNAL);
		break;
	case t_tick_msec :
		W_FATAL(eINTERNAL);
		break;
	case t_benchmark_start :
		W_FATAL(eINTERNAL);
		break;
	case t_page_write :
		W_FATAL(eINTERNAL);
		break;
	case t_page_read :
		W_FATAL(eINTERNAL);
		break;
	default :
		W_FATAL(eINTERNAL);
		break;
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
 *  xct_end_log
 *  xct_abort_log
 *
 *  Status Log to mark the end of transaction and space recovery.
 *
 *********************************************************************/
void xct_end_log::construct()
{
}

// We use a different log record type here only for debugging purposes
void xct_abort_log::construct()
{
}

/*********************************************************************
 *
 *  comment_log
 *
 *  For debugging
 *
 *********************************************************************/
void comment_log::construct(const char *msg)
{
    w_assert1(strlen(msg) < max_data_sz);
    memcpy(_data, msg, strlen(msg)+1);
    set_size(strlen(msg)+1);
}

/*********************************************************************
 *
 *  compensate_log
 *
 *  Needed when compensation rec is written rather than piggybacked
 *  on another record
 *
 *********************************************************************/
void compensate_log::construct(const lsn_t& rec_lsn)
{
    set_clr(rec_lsn);
}


/*********************************************************************
 *
 *  skip_log partition
 *
 *  Filler log record -- for skipping to end of log partition
 *
 *********************************************************************/
void skip_log::construct()
{
}

/*********************************************************************
 *
 *  chkpt_begin_log
 *
 *  Status Log to mark start of fussy checkpoint.
 *
 *********************************************************************/
void chkpt_begin_log::construct(const lsn_t &lastMountLSN)
{
    new (_data) lsn_t(lastMountLSN);
    set_size(sizeof(lsn_t));
}

template <class PagePtr>
void page_evict_log::construct (const PagePtr /*p*/,
                                general_recordid_t child_slot, lsn_t child_lsn)
{
    new (data_ssx()) page_evict_t(child_lsn, child_slot);
    set_size(sizeof(page_evict_t));
}

template <class PagePtr>
void page_evict_log::redo(PagePtr page) {
    borrowed_btree_page_h bp(page);
    page_evict_t *dp = (page_evict_t*) data_ssx();
    bp.set_emlsn_general(dp->_child_slot, dp->_child_lsn);
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
void chkpt_end_log::construct(const lsn_t& lsn, const lsn_t& min_rec_lsn,
                                const lsn_t& min_txn_lsn)
{
    // initialize _data
    lsn_t *l = new (_data) lsn_t(lsn);
    l++; //grot
    *l = min_rec_lsn;
    l++; //grot
    *l = min_txn_lsn;

    set_size((3 * sizeof(lsn_t)) + (3 * sizeof(int)));
}

void xct_latency_dump_log::construct(unsigned long nsec)
{
    *((unsigned long*) _data) = nsec;
    set_size(sizeof(unsigned long));
}


/*********************************************************************
 *
 *  chkpt_bf_tab_log
 *
 *  Data Log to save dirty page table at checkpoint.
 *  Contains, for each dirty page, its pid, minimum recovery lsn and page (latest) lsn.
 *
 *********************************************************************/
void chkpt_bf_tab_log::construct(
    int                 cnt,        // I-  # elements in pids[] and rlsns[]
    const PageID*         pid,        // I-  id of of dirty pages
    const lsn_t*         rec_lsn,// I-  rec_lsn[i] is recovery lsn (oldest) of pids[i]
    const lsn_t*         page_lsn)// I-  page_lsn[i] is page lsn (latest) of pids[i]
{
    set_size((new (_data) chkpt_bf_tab_t(cnt, pid, rec_lsn, page_lsn))->size());
}


/*********************************************************************
 *
 *  chkpt_xct_tab_log
 *
 *  Data log to save transaction table at checkpoint.
 *  Contains, for each active xct, its id, state, last_lsn
 *
 *********************************************************************/
void chkpt_xct_tab_log::construct(
    const tid_t&                         youngest,
    int                                 cnt,
    const tid_t*                         tid,
    const smlevel_0::xct_state_t*         state,
    const lsn_t*                         last_lsn,
    const lsn_t*                         first_lsn)
{
    set_size((new (_data) chkpt_xct_tab_t(youngest, cnt, tid, state,
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
void chkpt_xct_lock_log::construct(
    const tid_t&                        tid,
    int                                 cnt,
    const okvl_mode*                    lock_mode,
    const uint32_t*                     lock_hash)
{
    set_size((new (_data) chkpt_xct_lock_t(tid, cnt, lock_mode,
                                         lock_hash))->size());
}

void chkpt_backup_tab_log::construct(int cnt,
                                           const string* paths)
{
    // CS TODO
    set_size((new (_data) chkpt_backup_tab_t(cnt, paths))->size());
}

template <class PagePtr>
void chkpt_backup_tab_log::redo(PagePtr)
{
    // CS TODO should not be a redo logrec!
    chkpt_backup_tab_t* tab = (chkpt_backup_tab_t*) _data;
    std::vector<string> paths;
    tab->read(paths);
    w_assert0(tab->count == paths.size());

    for (size_t i = 0; i < tab->count; i++) {
        W_COERCE(smlevel_0::vol->sx_add_backup(paths[i], lsn_t::null, false /* log */));
    }
}

void chkpt_restore_tab_log::construct()
{
    chkpt_restore_tab_t* tab =
        new (_data) chkpt_restore_tab_t();

    smlevel_0::vol->chkpt_restore_progress(tab);
    set_size(tab->length());
}

template <class PagePtr>
void chkpt_restore_tab_log::redo(PagePtr)
{
    // CS TODO should not be a redo logrec!
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

void add_backup_log::construct(const string& path, lsn_t backupLSN)
{
    *((lsn_t*) data_ssx()) = backupLSN;
    w_assert0(path.length() < smlevel_0::max_devname);
    memcpy(data_ssx() + sizeof(lsn_t), path.data(), path.length());
    set_size(sizeof(lsn_t) + path.length());
}

template <class PagePtr>
void add_backup_log::redo(PagePtr)
{
    lsn_t backupLSN = *((lsn_t*) data_ssx());
    const char* dev_name = (const char*) (data_ssx() + sizeof(lsn_t));
    W_COERCE(smlevel_0::vol->sx_add_backup(string(dev_name), backupLSN, false));
}

void undo_done_log::construct()
{
}

void redo_done_log::construct()
{
}

void loganalysis_end_log::construct()
{
}

void loganalysis_begin_log::construct()
{
}

void restore_begin_log::construct()
{
#ifdef TIMED_LOG_RECORDS
    unsigned long tstamp = sysevent_timer::timestamp();
    memcpy(_data, &tstamp, sizeof(unsigned long));
    set_size(sizeof(unsigned long));
#endif
}

template <class PagePtr>
void restore_begin_log::redo(PagePtr)
{
    return; // CS TODO: disabled for now

    vol_t* volume = smlevel_0::vol;
    // volume must be mounted
    w_assert0(volume);

    // Marking the device failed will kick-off the restore thread, initialize
    // its state, and restore the metadata (even if already done - idempotence)
    W_COERCE(volume->mark_failed(false /* evict */, true /* redo */));
}

void restore_end_log::construct()
{
#ifdef TIMED_LOG_RECORDS
    unsigned long tstamp = sysevent_timer::timestamp();
    memcpy(_data, &tstamp, sizeof(unsigned long));
    set_size(sizeof(unsigned long));
#endif
}

template <class PagePtr>
void restore_end_log::redo(PagePtr)
{
    return; // CS TODO: disabled for now

    vol_t* volume = smlevel_0::vol;
    // volume must be mounted and failed
    w_assert0(volume && volume->is_failed());

    // CS TODO: fix this
    // bool finished = volume->check_restore_finished(true /* redo */);
    // w_assert0(finished);
}

void restore_segment_log::construct(uint32_t segment)
{
    char* pos = data_ssx();

    memcpy(pos, &segment, sizeof(uint32_t));
    pos += sizeof(uint32_t);

#ifdef TIMED_LOG_RECORDS
    unsigned long tstamp = sysevent_timer::timestamp();
    memcpy(pos, &tstamp, sizeof(unsigned long));
    pos += sizeof(unsigned long);
#endif

    set_size(pos - data_ssx());
}

template <class PagePtr>
void restore_segment_log::redo(PagePtr)
{
    return; // CS TODO: disabled for now

    vol_t* volume = smlevel_0::vol;
    // volume must be mounted and failed
    w_assert0(volume && volume->is_failed());

    uint32_t segment = *((uint32_t*) data_ssx());

    volume->redo_segment_restore(segment);
}

void alloc_page_log::construct(PageID pid)
{
    memcpy(data_ssx(), &pid, sizeof(PageID));
    // CS TODO: not necessary -- XctLogger sets alloc pid correctly
    PageID alloc_pid = pid - (pid % alloc_cache_t::extent_size);
    // fill(alloc_pid, 0, 0, sizeof(PageID));
    set_size(sizeof(PageID));
    // CS TODO: clean up eventlog.cpp and get rid of this
    set_pid(alloc_pid);
}

template <class PagePtr>
void alloc_page_log::redo(PagePtr p)
{
    PageID pid = *((PageID*) data_ssx());

    alloc_page* apage = (alloc_page*) p->get_generic_page();
    uint32_t index = pid % alloc_page::bits_held;
    apage->set_bit(index);
}

void dealloc_page_log::construct(PageID pid)
{
    memcpy(data_ssx(), &pid, sizeof(PageID));
    PageID alloc_pid = pid - (pid % alloc_cache_t::extent_size);
    // fill(alloc_pid, 0, 0, sizeof(PageID));
    set_size(sizeof(PageID));
    // CS TODO: clean up eventlog.cpp and get rid of this
    set_pid(alloc_pid);
}

template <class PagePtr>
void dealloc_page_log::redo(PagePtr p)
{
    PageID pid = *((PageID*) data_ssx());

    alloc_page* apage = (alloc_page*) p->get_generic_page();
    uint32_t index = pid % alloc_page::bits_held;
    apage->unset_bit(index);
}

template <class PagePtr>
void page_img_format_log::construct(const PagePtr page) {
    set_size((new (_data) page_img_format_t<PagePtr>(page))->size());
}

template <class PagePtr>
void page_img_format_log::undo(PagePtr) {
    // we don't have to do anything for UNDO
    // because this is a page creation!
    // CS TODO: then why do we need an undo method????
}
template <class PagePtr>
void page_img_format_log::redo(PagePtr page) {
    // REDO is simply applying the image
    page_img_format_t<PagePtr>* dp = (page_img_format_t<PagePtr>*) _data;
    dp->apply(page);
}

void tick_sec_log::construct()
{
}

void tick_msec_log::construct()
{
}

void benchmark_start_log::construct()
{
}

void page_read_log::construct(PageID pid, uint32_t count)
{
    memcpy(data(), &pid, sizeof(PageID));
    memcpy(_data + sizeof(PageID), &count, sizeof(uint32_t));
    set_size(sizeof(PageID) + sizeof(uint32_t));
}

void page_write_log::construct(PageID pid, lsn_t lsn, uint32_t count)
{
    char* pos = _data;

    memcpy(pos, &pid, sizeof(PageID));
    pos += sizeof(PageID);

    memcpy(pos, &lsn, sizeof(lsn_t));
    pos += sizeof(lsn_t);

    memcpy(pos, &count, sizeof(uint32_t));
    pos += sizeof(uint32_t);


    set_size(pos - _data);
}



/*********************************************************************
 *
 *  operator<<(ostream, logrec)
 *
 *  Pretty print a log record to ostream.
 *
 *********************************************************************/
ostream&
operator<<(ostream& o, const logrec_t& l)
{
    ios_fmtflags        f = o.flags();
    o.setf(ios::left, ios::left);

    o << "LSN=" << l.lsn_ck() << " ";

    o << "len=" << l.length() << " ";

    if (!l.is_single_sys_xct()) {
        o << "TID=" << l.tid() << ' ';
    } else {
        o << "TID=SSX" << ' ';
    }
    o << l.type_str() << ":" << l.cat_str();
    if (l.is_cpsn()) { o << " CLR"; }
    if (l.is_root_page()) { o << " ROOT"; }
    o << " p(" << l.pid() << ")";
    if (l.is_multi_page()) {
        o << " src-" << l.pid2();
    }

    switch(l.type()) {
        case logrec_t::t_chkpt_xct_tab:
            {
                chkpt_xct_tab_t* dp = (chkpt_xct_tab_t*) l.data();
                o << " xct_count: " << dp->count;
                break;
            }
        case logrec_t::t_chkpt_bf_tab:
            {
                chkpt_bf_tab_t* dp = (chkpt_bf_tab_t*) l.data();
                o << " dirty_page_count: " << dp->count;
                break;
            }
        case logrec_t::t_comment :
            {
                o << (const char *)l._data;
                break;
            }
        case logrec_t::t_page_evict:
            {
                page_evict_t* pev = (page_evict_t*) l._data;
                o << " slot: " << pev->_child_slot << " emlsn: "
                    << pev->_child_lsn;
                break;
            }
        case logrec_t::t_alloc_page:
        case logrec_t::t_dealloc_page:
            {
                o << " page: " << *((PageID*) (l.data_ssx()));
                break;
            }
        case logrec_t::t_create_store:
            {
                o << " stid: " <<  *((StoreID*) l.data_ssx());
                o << " root_pid: " << *((PageID*) (l.data_ssx() + sizeof(StoreID)));
                break;
            }
        case logrec_t::t_page_write:
            {
                PageID first = *((PageID*) (l.data()));
                PageID last = first + *((uint32_t*) (l.data() + sizeof(PageID) + sizeof(lsn_t))) - 1;
                o << " pids: " << first << "-" << last;
                break;
            }
        case logrec_t::t_restore_segment:
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

template <class PagePtr>
void create_store_log::construct(PagePtr page, PageID root_pid, StoreID snum)
{
    memcpy(data_ssx(), &snum, sizeof(StoreID));
    memcpy(data_ssx() + sizeof(StoreID), &root_pid, sizeof(PageID));
    set_size(sizeof(StoreID) + sizeof(PageID));
}

template <class PagePtr>
void create_store_log::redo(PagePtr page)
{
    StoreID snum = *((StoreID*) data_ssx());
    PageID root_pid = *((PageID*) (data_ssx() + sizeof(StoreID)));

    stnode_page* stpage = (stnode_page*) page->get_generic_page();
    if (stpage->pid != stnode_page::stpid) {
        stpage->pid = stnode_page::stpid;
    }
    stpage->set_root(snum, root_pid);
}

template <class PagePtr>
void append_extent_log::construct(PagePtr, extent_id_t ext)
{
    memcpy(data_ssx(), &ext, sizeof(extent_id_t));
    set_size(sizeof(extent_id_t));
}

template <class PagePtr>
void append_extent_log::redo(PagePtr page)
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
    for(int i=0; i < logrec_t::t_max_logrec; i++) {
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
            tcxt = logrec_t::t_xct_abort;
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

void logrec_accounting_t::print_account_and_clear()
{
    logrec_accounting_impl_t::print_account_and_clear();
}
void logrec_accounting_impl_t::print_account_and_clear()
{
    uint64_t anyb=0;
    for(int i=0; i < logrec_t::t_max_logrec; i++) {
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
    for(int i=0; i < logrec_t::t_max_logrec; i++) {
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


template void logrec_t::template redo<btree_page_h*>(btree_page_h*);
template void logrec_t::template redo<fixable_page_h*>(fixable_page_h*);
template void logrec_t::template undo<fixable_page_h*>(fixable_page_h*);

template void page_evict_log::template construct<btree_page_h*>(btree_page_h* p,
                                general_recordid_t child_slot, lsn_t child_lsn);

template void page_img_format_log::template construct<btree_page_h*>(btree_page_h*);

template void create_store_log::template construct<fixable_page_h*>(fixable_page_h*, PageID, StoreID);
template void append_extent_log::template construct<fixable_page_h*>(fixable_page_h*, extent_id_t);
