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
#include "logrec_handler.h"
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
	case comment_log :
		return "comment";
	case compensate_log :
		return "compensate";
	case skip_log :
		return "skip";
	case chkpt_begin_log :
		return "chkpt_begin";
	case add_backup_log :
		return "add_backup";
	case evict_page_log :
		return "evict_page";
	case fetch_page_log :
		return "fetch_page";
	case xct_abort_log :
		return "xct_abort";
	case xct_end_log :
		return "xct_end";
	case xct_latency_dump_log :
		return "xct_latency_dump";
	case alloc_page_log :
		return "alloc_page";
	case dealloc_page_log :
		return "dealloc_page";
	case create_store_log :
		return "create_store";
	case alloc_format_log :
		return "alloc_format";
	case stnode_format_log :
		return "stnode_format";
	case append_extent_log :
		return "append_extent";
	case loganalysis_begin_log :
		return "loganalysis_begin";
	case loganalysis_end_log :
		return "loganalysis_end";
	case redo_done_log :
		return "redo_done";
	case undo_done_log :
		return "undo_done";
	case restore_begin_log :
		return "restore_begin";
	case restore_segment_log :
		return "restore_segment";
	case restore_end_log :
		return "restore_end";
	case warmup_done_log :
		return "warmup_done";
	case page_img_format_log :
		return "page_img_format";
	case update_emlsn_log :
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
	case tick_sec_log :
		return "tick_sec";
	case tick_msec_log :
		return "tick_msec";
	case benchmark_start_log :
		return "benchmark_start";
	case page_write_log :
		return "page_write";
	case page_read_log :
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
	case alloc_page_log :
                LogrecHandler<alloc_page_log, PagePtr>::redo(this, page);
		break;
	case dealloc_page_log :
                LogrecHandler<dealloc_page_log, PagePtr>::redo(this, page);
		break;
	case alloc_format_log :
                LogrecHandler<alloc_format_log, PagePtr>::redo(this, page);
		break;
	case stnode_format_log :
                LogrecHandler<stnode_format_log, PagePtr>::redo(this, page);
		break;
	case create_store_log :
                LogrecHandler<create_store_log, PagePtr>::redo(this, page);
		break;
	case append_extent_log :
                LogrecHandler<append_extent_log, PagePtr>::redo(this, page);
		break;
	case page_img_format_log :
                LogrecHandler<page_img_format_log, PagePtr>::redo(this, page);
		break;
	case update_emlsn_log :
                LogrecHandler<update_emlsn_log, PagePtr>::redo(this, page);
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

static __thread kind_t undoing_context = t_max_logrec; // for accounting TODO REMOVE


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
    undoing_context = kind_t(header._type);
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
	default :
		W_FATAL(eINTERNAL);
		break;
    }

    xct()->compensate_undo(xid_prev());

    undoing_context = t_max_logrec;
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
        size_t img_size = reinterpret_cast<page_img_format_t*>(img)->size();
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
        case comment_log :
            {
                o << " " << (const char *)l._data;
                break;
            }
        case update_emlsn_log:
            {
                general_recordid_t slot;
                lsn_t lsn;
                deserialize_log_fields(&l, slot, lsn);
                o << " slot: " << slot << " emlsn: " << lsn;
                break;
            }
        case evict_page_log:
            {
                PageID pid;
                bool was_dirty;
                lsn_t page_lsn;
                deserialize_log_fields(&l, pid, was_dirty, page_lsn);
                o << " pid: " << pid << (was_dirty ? " dirty" : " clean") << " page_lsn: "
                    << page_lsn;
                break;
            }
        case fetch_page_log:
            {
                PageID pid;
                lsn_t plsn;
                StoreID store;
                deserialize_log_fields(&l, pid, plsn, store);
                o << " pid: " << pid << " page_lsn: " << plsn << " store: " << store;
                break;
            }
        case alloc_page_log:
        case dealloc_page_log:
            {
                PageID pid;
                deserialize_log_fields(&l, pid);
                o << " page: " << pid;
                break;
            }
        case create_store_log:
            {
                StoreID stid;
                PageID root_pid;
                deserialize_log_fields(&l, stid, root_pid);
                o << " stid: " <<  stid;
                o << " root_pid: " << root_pid;
                break;
            }
        case page_read_log:
            {
                PageID pid;
                uint32_t count;
                PageID end = pid + count - 1;
                deserialize_log_fields(&l, pid, count);
                o << " pids: " << pid << "-" << end;
                break;
            }
        case page_write_log:
            {
                PageID pid;
                lsn_t clean_lsn;
                uint32_t count;
                deserialize_log_fields(&l, pid, clean_lsn, count);
                PageID end = pid + count - 1;
                o << " pids: " << pid << "-" << end << " clean_lsn: " << clean_lsn;
                break;
            }
        case restore_segment_log:
            {
                uint32_t segment;
                deserialize_log_fields(&l, segment);
                o << " segment: " << segment;
                break;
            }
        case append_extent_log:
            {
                extent_id_t ext;
                StoreID snum;
                deserialize_log_fields(&l, ext, snum);
                o << " extent: " << ext << " store: " << snum;
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
        undoing_context = t_xct_end;
    }
}
void logrec_accounting_impl_t::account(logrec_t &l, bool fwd)
{
    unsigned b = l.length();
    int      t = l.type();
    int      tcxt = l.type();
    if(fwd) {
        w_assert0((undoing_context == t_max_logrec)
               || (undoing_context == t_xct_end));
    } else {
        if(undoing_context != t_max_logrec) {
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


template void logrec_t::template redo<btree_page_h*>(btree_page_h*);
template void logrec_t::template redo<fixable_page_h*>(fixable_page_h*);
template void logrec_t::template undo<fixable_page_h*>(fixable_page_h*);
