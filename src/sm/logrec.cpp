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
#include "btree_logrec.h"

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
	case t_add_backup :
		return "add_backup";
	case t_evict_page :
		return "evict_page";
	case t_fetch_page :
		return "fetch_page";
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
	case t_alloc_format :
		return "alloc_format";
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
	case t_warmup_done :
		return "warmup_done";
	case t_page_img_format :
		return "page_img_format";
	case t_update_emlsn :
		return "update_emlsn";
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
	case t_add_backup :
		W_FATAL(eINTERNAL);
		break;
	case t_fetch_page :
		W_FATAL(eINTERNAL);
		break;
	case t_evict_page :
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
	case t_alloc_format :
		((alloc_format_log *) this)->redo(page);
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
		W_FATAL(eINTERNAL);
		break;
	case t_restore_segment :
		W_FATAL(eINTERNAL);
		break;
	case t_warmup_done :
		W_FATAL(eINTERNAL);
		break;
	case t_restore_end :
		((restore_end_log *) this)->redo(page);
		break;
	case t_page_img_format :
		((page_img_format_log *) this)->redo(page);
		break;
	case t_update_emlsn :
		((update_emlsn_log *) this)->redo(page);
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
	case t_add_backup :
		W_FATAL(eINTERNAL);
		break;
	case t_fetch_page :
		W_FATAL(eINTERNAL);
		break;
	case t_evict_page :
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
	case t_alloc_format :
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
	case t_warmup_done :
		W_FATAL(eINTERNAL);
		break;
	case t_restore_end :
		W_FATAL(eINTERNAL);
		break;
	case t_page_img_format :
		((page_img_format_log *) this)->undo(page);
		break;
	case t_update_emlsn :
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

void logrec_t::remove_info_for_pid(PageID pid)
{
    w_assert1(is_multi_page());
    w_assert1(pid == this->pid() || pid == pid2());
    lsn_t lsn = lsn_ck();

    if (type() == t_btree_split) {
        size_t img_offset = reinterpret_cast<btree_bulk_delete_t*>(data_ssx())->size();
        char* img = data_ssx() + img_offset;
        size_t img_size = reinterpret_cast<page_img_format_t<btree_page_h>*>(img)->size();
        char* end = reinterpret_cast<char*>(this) + length();

        if (pid == this->pid()) {
            // just cut off 2nd half of logrec (page_img)
            set_size(img_offset);
        }
        else if (pid == pid2()) {
            // Use empty bulk delete and move page img
            // CS TODO: create a normal page_img_format log record
            btree_bulk_delete_t* bulk = new (data_ssx()) btree_bulk_delete_t(pid2(),
                    this->pid());
            ::memmove(data_ssx() + bulk->size(), img, end - img);
            set_size(bulk->size() + img_size);
        }
    }

    set_lsn_ck(lsn);
    w_assert1(valid_header());
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
void chkpt_begin_log::construct()
{
}

struct update_emlsn_t {
    lsn_t                   _child_lsn;
    general_recordid_t      _child_slot;
    update_emlsn_t(const lsn_t &child_lsn, general_recordid_t child_slot)
        : _child_lsn (child_lsn), _child_slot(child_slot) {}
};

template <class PagePtr>
void update_emlsn_log::construct (const PagePtr /*p*/,
                                general_recordid_t child_slot, lsn_t child_lsn)
{
    new (data_ssx()) update_emlsn_t(child_lsn, child_slot);
    set_size(sizeof(update_emlsn_t));
}

template <class PagePtr>
void update_emlsn_log::redo(PagePtr page) {
    borrowed_btree_page_h bp(page);
    update_emlsn_t *dp = (update_emlsn_t*) data_ssx();
    bp.set_emlsn_general(dp->_child_slot, dp->_child_lsn);
}


void xct_latency_dump_log::construct(unsigned long nsec)
{
    *((unsigned long*) _data) = nsec;
    set_size(sizeof(unsigned long));
}

void add_backup_log::construct(const string& path, lsn_t backupLSN)
{
    *((lsn_t*) data_ssx()) = backupLSN;
    w_assert0(path.length() < smlevel_0::max_devname);
    memcpy(data_ssx() + sizeof(lsn_t), path.c_str(), path.length() + 1);
    set_size(sizeof(lsn_t) + path.length());
}

void evict_page_log::construct(PageID pid, bool was_dirty, lsn_t page_lsn)
{
    char* data = data_ssx();
    *(reinterpret_cast<PageID*>(data)) = pid;
    data += sizeof(PageID);

    *(reinterpret_cast<bool*>(data)) = was_dirty;
    data += sizeof(bool);

    *(reinterpret_cast<lsn_t*>(data)) = page_lsn;
    data += sizeof(lsn_t);

    set_size(sizeof(data - data_ssx()));
}

void fetch_page_log::construct(PageID pid, lsn_t page_lsn, StoreID store)
{
    char* data = data_ssx();
    *(reinterpret_cast<PageID*>(data)) = pid;
    data += sizeof(PageID);

    *(reinterpret_cast<lsn_t*>(data)) = page_lsn;
    data += sizeof(lsn_t);

    *(reinterpret_cast<StoreID*>(data)) = store;
    data += sizeof(StoreID);

    set_size(sizeof(data - data_ssx()));
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

void restore_begin_log::construct(PageID page_cnt)
{
    memcpy(data_ssx(), &page_cnt, sizeof(PageID));
    set_size(sizeof(PageID));
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
    // CS TODO: cleanup!
}

template <class PagePtr>
void alloc_page_log::construct(PagePtr, PageID pid)
{
    memcpy(data_ssx(), &pid, sizeof(PageID));
    set_size(sizeof(PageID));
}

template <class PagePtr>
void alloc_page_log::redo(PagePtr p)
{
    PageID alloc_pid = pid();
    // CS TODO: little hack to fix bug quickly for my experiments
    // Proper fix is to introduce a alloc_page_format log record
    if (alloc_pid != p->pid()) {
        ::memset(p->get_generic_page(), 0, sizeof(generic_page));
        p->get_generic_page()->pid = alloc_pid;
    }
    PageID pid = *((PageID*) data_ssx());
    alloc_page* page = (alloc_page*) p->get_generic_page();
    // assertion fails after page-img compression
    // w_assert1(!page->get_bit(pid - alloc_pid));
    page->set_bit(pid - alloc_pid);
}

template <class PagePtr>
void dealloc_page_log::construct(PagePtr, PageID pid)
{
    memcpy(data_ssx(), &pid, sizeof(PageID));
    set_size(sizeof(PageID));
}

template <class PagePtr>
void dealloc_page_log::redo(PagePtr p)
{
    PageID alloc_pid = p->pid();
    PageID pid = *((PageID*) data_ssx());
    alloc_page* page = (alloc_page*) p->get_generic_page();
    // assertion fails after page-img compression
    // w_assert1(page->get_bit(pid - alloc_pid));
    page->unset_bit(pid - alloc_pid);
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
operator<<(ostream& o, logrec_t& l)
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
        case logrec_t::t_comment :
            {
                o << " " << (const char *)l._data;
                break;
            }
        case logrec_t::t_update_emlsn:
            {
                update_emlsn_t* pev = (update_emlsn_t*) l._data;
                o << " slot: " << pev->_child_slot << " emlsn: "
                    << pev->_child_lsn;
                break;
            }
        case logrec_t::t_evict_page:
            {
                PageID pid = *(reinterpret_cast<PageID*>(l.data_ssx()));
                bool was_dirty = *(reinterpret_cast<bool*>(l.data_ssx() + sizeof(PageID)));
                lsn_t page_lsn = *(reinterpret_cast<lsn_t*>(l.data_ssx() + sizeof(PageID) + sizeof(bool)));
                o << " pid: " << pid << (was_dirty ? " dirty" : " clean") << " page_lsn: "
                    << page_lsn;
                break;
            }
        case logrec_t::t_fetch_page:
            {
                char* pos = l.data_ssx();
                PageID pid = *(reinterpret_cast<PageID*>(pos));
                pos += sizeof(PageID);

                lsn_t plsn = *(reinterpret_cast<lsn_t*>(pos));
                pos += sizeof(lsn_t);

                StoreID store = *(reinterpret_cast<StoreID*>(pos));
                pos += sizeof(StoreID);

                o << " pid: " << pid << " page_lsn: " << plsn << " store: " << store;
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
        case logrec_t::t_page_read:
            {
                char* pos = (char*) l._data;

                PageID pid = *((PageID*) pos);
                pos += sizeof(PageID);

                uint32_t count = *((uint32_t*) pos);
                PageID end = pid + count - 1;

                o << " pids: " << pid << "-" << end;
                break;
            }
        case logrec_t::t_page_write:
            {
                char* pos = (char*) l._data;

                PageID pid = *((PageID*) pos);
                pos += sizeof(PageID);

                lsn_t clean_lsn = *((lsn_t*) pos);
                pos += sizeof(lsn_t);

                uint32_t count = *((uint32_t*) pos);
                PageID end = pid + count - 1;

                o << " pids: " << pid << "-" << end << " clean_lsn: " << clean_lsn;
                break;
            }
        case logrec_t::t_restore_segment:
            {
                o << " segment: " << *((uint32_t*) l.data_ssx());
                break;
            }
        case logrec_t::t_append_extent:
            {
                o << " extent: " << *((extent_id_t*) l.data_ssx());
                o << " store: " << *((StoreID*) (l.data_ssx() + sizeof(extent_id_t)));
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
    stpage->set_last_extent(snum, 0);
}

template <class PagePtr>
void append_extent_log::construct(PagePtr, StoreID snum, extent_id_t ext)
{
    char* data = data_ssx();

    memcpy(data, &ext, sizeof(extent_id_t));
    data += sizeof(extent_id_t);

    memcpy(data, &snum, sizeof(StoreID));
    data += sizeof(StoreID);

    set_size(data - data_ssx());
}

template <class PagePtr>
void append_extent_log::redo(PagePtr page)
{
    extent_id_t ext = *((extent_id_t*) data_ssx());
    StoreID snum = *((StoreID*) (data_ssx() + sizeof(extent_id_t)));
    auto spage = reinterpret_cast<stnode_page*>(page->get_generic_page());
    spage->set_last_extent(snum, ext);
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

template void update_emlsn_log::template construct<btree_page_h*>(btree_page_h* p,
                                general_recordid_t child_slot, lsn_t child_lsn);

template void page_img_format_log::template construct<fixable_page_h*>(fixable_page_h*);
template void page_img_format_log::template construct<btree_page_h*>(btree_page_h*);

template void create_store_log::template construct<fixable_page_h*>(fixable_page_h*, PageID, StoreID);
template void append_extent_log::template construct<fixable_page_h*>(fixable_page_h*, StoreID, extent_id_t);

template void alloc_page_log::template construct<fixable_page_h*>(fixable_page_h*, PageID);
template void dealloc_page_log::template construct<fixable_page_h*>(fixable_page_h*, PageID);
