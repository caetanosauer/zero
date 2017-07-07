/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore' incl-file-exclusion='LOGREC_H'>

 $Id: logrec.h,v 1.73 2010/12/08 17:37:42 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef LOGREC_H
#define LOGREC_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

class rangeset_t;
struct multi_page_log_t;
class RestoreBitmap;
class xct_t;

#include "lsn.h"
#include "tid_t.h"
#include "generic_page.h" // logrec size == 3 * page size
#include "allocator.h"

struct baseLogHeader
{
    uint16_t _len;  // length of the log record
    u_char _type; // kind_t (included from logtype_gen.h)
    u_char _flags;
    /* 4 */

    // Was _pid; broke down to save 2 bytes:
    // May be used ONLY in set_pid() and pid()
    PageID             _pid; // 4 bytes
    /* 4 + 4=8 */


    // CS TODO: temporary placeholder for old vid
    uint16_t _fill_vid;

    uint16_t             _page_tag; // tag_t 2 bytes
    /* 8 + 4= 12 */
    StoreID              _stid; // 4 bytes
    /* 12 + 4= 16*/



    // lsn_t            _undo_nxt; // (xct) used in CLR only
    /*
     * originally: you might think it would be nice to use one lsn_t for
     * both _xid_prev and for _undo_lsn, but for the moment we need both because
     * at the last minute, fill_xct_attr() is called and that fills in
     * _xid_prev, clobbering its value with the prior generated log record's lsn.
     * It so happens that set_clr() is called prior to fill_xct_attr().
     * It might do to set _xid_prev iff it's not already set, in fill_xct_attr().
     * NB: this latter suggestion is what we have now done.
     */

    /**
     * For per-page chains of log-records.
     * Note that some types of log records (split, merge) impact two pages.
     * The page_prev_lsn is for the "primary" page.
     * \ingroup Single-Page-Recovery
     */
    lsn_t               _page_prv;
    /* 16+8 = 24 */

    bool is_valid() const;
};

struct xidChainLogHeader
{


    tid_t               _xid;      // NOT IN SINGLE-LOG SYSTEM TRANSACTION!  (xct)tid of this xct
    /* 24+8 = 32 */
    lsn_t               _xid_prv;     // NOT IN SINGLE-LOG SYSTEM TRANSACTION! (xct)previous logrec of this xct
    /* 32+8 = 40 */
};

enum kind_t {
    comment_log = 0,
    compensate_log = 1,
    skip_log = 2,
    chkpt_begin_log = 3,
    // t_chkpt_bf_tab = 4,
    // t_chkpt_xct_tab = 5,
    // t_chkpt_xct_lock = 6,
    warmup_done_log = 7,
    alloc_format_log = 8,
    evict_page_log = 9,
    add_backup_log = 10,
    xct_abort_log = 11,
    fetch_page_log = 12,
    xct_end_log = 13,
    // t_xct_end_group = 14,
    xct_latency_dump_log = 15,
    alloc_page_log = 16,
    dealloc_page_log = 17,
    create_store_log = 18,
    append_extent_log = 19,
    loganalysis_begin_log = 20,
    loganalysis_end_log = 21,
    redo_done_log = 22,
    undo_done_log = 23,
    restore_begin_log = 24,
    restore_segment_log = 25,
    restore_end_log = 26,
    // t_page_set_to_be_deleted = 27,
    stnode_format_log = 27,
    page_img_format_log = 28,
    update_emlsn_log = 29,
    // t_btree_norec_alloc = 30,
    btree_insert_log = 31,
    btree_insert_nonghost_log = 32,
    btree_update_log = 33,
    btree_overwrite_log = 34,
    btree_ghost_mark_log = 35,
    btree_ghost_reclaim_log = 36,
    btree_ghost_reserve_log = 37,
    t_btree_foster_adopt = 38,
    // t_btree_foster_merge = 39,
    // t_btree_foster_rebalance = 40,
    // t_btree_foster_rebalance_norec = 41,
    // t_btree_foster_deadopt = 42,
    t_btree_split = 43,
    btree_compress_page_log = 44,
    tick_sec_log = 45,
    tick_msec_log = 46,
    benchmark_start_log = 47,
    page_write_log = 48,
    page_read_log = 49,
    t_max_logrec = 50
};

/**
 * \brief Represents a transactional log record.
 * \ingroup SSMLOG
 * \details
 * A log record's space is divided between a header and data.
 * All log records' headers include the information contained in baseLogHeader.
 * Log records pertaining to transactions that produce multiple log records
 * also persist a transaction id chain (_xid and _xid_prv).
 *
 * \section OPT Optimization for single-log system transaction
 * For single-log system transaction, header items in xidChainLogHeader are not stored.
 * instead, we use these area as data area to save 16 bytes.
 * we do need to keep these 8 bytes aligned. and this is a bit dirty trick.
 * however, we really need it to reduce the volume of log we output for system transactions.
 */
class logrec_t {
public:
    friend class XctLogger;
    friend class sysevent;
    friend class baseLogHeader;

    bool             is_page_update() const;
    bool             is_redo() const;
    bool             is_skip() const;
    bool             is_undo() const;
    bool             is_cpsn() const;
    bool             is_multi_page() const;
    bool             is_root_page() const;
    bool             is_logical() const;
    bool             is_system() const;
    bool             is_single_sys_xct() const;
    bool             valid_header(const lsn_t & lsn_ck = lsn_t::null) const;
    smsize_t         header_size() const;

    template <class PagePtr>
    void             redo(PagePtr);

    static constexpr u_char get_logrec_cat(kind_t type);

    void redo();

    template <class PagePtr>
    void             undo(PagePtr);

    void init_header(kind_t);

    template <class PagePtr>
    void init_page_info(const PagePtr p)
    {
        header._page_tag = p->tag();
        header._pid = p->pid();
        header._stid = p->store();
    }

    void set_size(size_t l);

    void init_xct_info();

    void set_xid_prev(tid_t tid, lsn_t last);

    enum {
        max_sz = 3 * sizeof(generic_page),
        hdr_non_ssx_sz = sizeof(baseLogHeader) + sizeof(xidChainLogHeader),
        hdr_single_sys_xct_sz = sizeof(baseLogHeader),
        // max_data_sz is conservative.
        // we don't allow the last 16 bytes to be used (anyway very rarely used)
        max_data_sz = max_sz - hdr_non_ssx_sz - sizeof(lsn_t)
    };

       static_assert(hdr_non_ssx_sz == 40, "Wrong logrec header size");
       static_assert(hdr_single_sys_xct_sz == 40 - 16, "Wrong logrec header size");

       tid_t   tid() const;
       StoreID        stid() const;
       PageID         pid() const;
       PageID         pid2() const;

public:
    uint16_t              tag() const;
    smsize_t             length() const;
    const lsn_t&         undo_nxt() const;
    /**
     * Returns the LSN of previous log that modified this page.
     * \ingroup Single-Page-Recovery
     */
    const lsn_t&         page_prev_lsn() const;
    const lsn_t&         page2_prev_lsn() const;
    /**
     * Sets the LSN of previous log that modified this page.
     * \ingroup Single-Page-Recovery
     */
    void                 set_page_prev_lsn(const lsn_t &lsn);
    const lsn_t&         xid_prev() const;
    void                 set_xid_prev(const lsn_t &lsn);
    void                 set_undo_nxt(const lsn_t &lsn);
    void                 set_tid(tid_t tid);
    void                 set_root_page();
    void                 set_pid(const PageID& p);
    kind_t               type() const;
    const char*          type_str() const
    {
        return get_type_str(type());
    }
    static const char*   get_type_str(kind_t);
    const char*          cat_str() const;
    const char*          data() const;
    char*                data();
    const char*          data_ssx() const;
    char*                data_ssx();
    /** Returns the log record data as a multi-page SSX log. */
    multi_page_log_t*           data_ssx_multi();
    /** Const version */
    const multi_page_log_t*     data_ssx_multi() const;
    const lsn_t&         lsn_ck() const {  return *_lsn_ck(); }
    const lsn_t&         lsn() const {  return *_lsn_ck(); }
    const lsn_t          get_lsn_ck() const {
                                lsn_t    tmp = *_lsn_ck();
                                return tmp;
                            }
    void                 set_lsn_ck(const lsn_t &lsn_ck) {
                                // put lsn in last bytes of data
                                lsn_t& where = *_lsn_ck();
                                where = lsn_ck;
                            }
    void                 corrupt();

    // Hack to support compensation with generic loggers
    // (see xct_logger.h)
    void set_clr() {}

    void set_clr(const lsn_t& c)
    {
        w_assert0(!is_single_sys_xct()); // CLR shouldn't be output in this case
        header._flags |= t_cpsn;

        // To shrink log records,
        // we've taken out _undo_nxt and
        // overloaded _prev.
        // _undo_nxt = c;
        xidInfo._xid_prv = c; // and _xid_prv is data area if is_single_sys_xct
    }

    const char* get_data_offset() const
    {
        return data_ssx();
    }

    char* get_data_offset()
    {
        return data_ssx();
    }

    void remove_info_for_pid(PageID pid);

    // Tells whether this log record restores a full page image, meaning
    // that the previous history is not needed during log replay.
    bool has_page_img(PageID page_id)
    {
        return
            (type() == t_btree_split && page_id == pid())
            || (type() == page_img_format_log)
            || (type() == stnode_format_log)
            || (type() == alloc_format_log)
            ;
    }

    friend ostream& operator<<(ostream&, logrec_t&);

protected:

    enum category_t {
        /** should not happen. */
        t_bad_cat   = 0x00,
        /** System log record: not transaction- or page-related; no undo/redo */
        t_system    = 0x01,
        /** log with UNDO action? */
        t_undo      = 0x02,
        /** log with REDO action? */
        t_redo      = 0x04,
        /** log for multi pages? */
        t_multi     = 0x08,
        /**
         * is the UNDO logical? If so, do not fix the page for undo.
         * Irrelevant if not an undoable log record.
         */
        t_logical   = 0x10,

        /** log by system transaction which is fused with begin/commit record. */
        t_single_sys_xct    = 0x80
    };

    enum flag_t {
        // If this logrec is a CLR
        t_cpsn          = 0x01,
        // If this logrec refers to a root page (in a general sense, a root is
        // any page which cannot be recovered by SPR because no other page
        // points to it
        t_root_page     = 0x02
    };

    u_char             cat() const;

    baseLogHeader header;

    // single-log system transactions will overwrite this with _data
    xidChainLogHeader xidInfo;

    /*
     * NOTE re sizeof header:
     * NOTE For single-log system transaction, NEVER use this directly.
     * Always use data_ssx() to get the pointer because it starts
     * from 16 bytes ahead. See comments about single-log system transaction.
    */
    char            _data[max_sz - sizeof(baseLogHeader) - sizeof(xidChainLogHeader)];


    // The last sizeof(lsn_t) bytes of data are used for
    // recording the lsn.
    // Should always be aligned to 8 bytes.
    lsn_t*            _lsn_ck() {
        w_assert3(alignon(header._len, 8));
        char* this_ptr = reinterpret_cast<char*>(this);
        return reinterpret_cast<lsn_t*>(this_ptr + header._len - sizeof(lsn_t));
    }
    const lsn_t*            _lsn_ck() const {
        w_assert3(alignon(header._len, 8));
        const char* this_ptr = reinterpret_cast<const char*>(this);
        return reinterpret_cast<const lsn_t*>(this_ptr + header._len - sizeof(lsn_t));
    }

public:
    // overloaded new/delete operators for tailored memory management
    void* operator new(size_t);
    void operator delete(void*, size_t);

    // CS: apparently we have to define placement new as well if the standard
    // new is overloaded
    void* operator new(size_t, void* p) { return p; }
};

inline bool baseLogHeader::is_valid() const
{
    return (_len >= sizeof(baseLogHeader)
            && _type < t_max_logrec
            && _len <= sizeof(logrec_t));
}

/**
 * \brief Base struct for log records that touch multi-pages.
 * \ingroup SSMLOG
 * \details
 * Such log records are so far _always_ single-log system transaction that touches 2 pages.
 * If possible, such log record should contain everything we physically need to recover
 * either page without the other page. This is an important property
 * because otherwise it imposes write-order-dependency and a careful recovery.
 * In such a case "page2" is the data source page while "page" is the data destination page.
 * \NOTE a REDO operation of multi-page log must expect _either_ of page/page2 are given.
 * It must first check if which page is requested to recover, then apply right changes
 * to the page.
 */
struct multi_page_log_t {
    /**
     * _page_prv for another page touched by the operation.
     * \ingroup Single-Page-Recovery
     */
    lsn_t       _page2_prv; // +8

    /** Page ID of another page touched by the operation. */
    PageID     _page2_pid; // +4

    /** for alignment only. */
    uint32_t    _fill4;    // +4.

    multi_page_log_t(PageID page2_pid) : _page2_prv(lsn_t::null), _page2_pid(page2_pid) {
    }
};

// for single-log system transaction, we use tid/_xid_prev as data area!
inline const char*  logrec_t::data() const
{
    return _data;
}
inline char*  logrec_t::data()
{
    return _data;
}
inline const char*  logrec_t::data_ssx() const
{
    return _data - sizeof(xidChainLogHeader);
}
inline char*  logrec_t::data_ssx()
{
    return _data - sizeof(xidChainLogHeader);
}
inline smsize_t logrec_t::header_size() const
{
    if (is_single_sys_xct()) {
        return hdr_single_sys_xct_sz;
    } else {
        return hdr_non_ssx_sz;
    }
}

inline PageID
logrec_t::pid() const
{
    return header._pid;
}

inline StoreID
logrec_t::stid() const
{
    return header._stid;
}

inline PageID logrec_t::pid2() const
{
    if (!is_multi_page()) { return 0; }

    const multi_page_log_t* multi_log = reinterpret_cast<const multi_page_log_t*> (data_ssx());
    return multi_log->_page2_pid;
}

inline void
logrec_t::set_pid(const PageID& p)
{
    header._pid = p;
}

inline void
logrec_t::set_tid(tid_t tid)
{
    xidInfo._xid = tid;
}

inline void
logrec_t::set_undo_nxt(const lsn_t& undo_nxt)
{
    xidInfo._xid_prv = undo_nxt;
}

inline uint16_t
logrec_t::tag() const
{
    return header._page_tag;
}

inline smsize_t
logrec_t::length() const
{
    return header._len;
}

inline const lsn_t&
logrec_t::undo_nxt() const
{
    // To shrink log records,
    // we've taken out _undo_nxt and
    // overloaded _xid_prev.
    // return _undo_nxt;
    return xid_prev();
}

inline const lsn_t&
logrec_t::page_prev_lsn() const
{
    // What do we need to assert in order to make sure there IS a page_prv?
    return header._page_prv;
}

inline const lsn_t&
logrec_t::page2_prev_lsn() const
{
    if (!is_multi_page()) { return lsn_t::null; }
    return data_ssx_multi()->_page2_prv;
}
inline void
logrec_t::set_page_prev_lsn(const lsn_t &lsn)
{
    // What do we need to assert in order to make sure there IS a page_prv?
    header._page_prv = lsn;
}

inline tid_t logrec_t::tid() const
{
    if (is_single_sys_xct()) {
        return tid_t {0};
    }
    return xidInfo._xid;
}

inline const lsn_t&
logrec_t::xid_prev() const
{
    w_assert1(!is_single_sys_xct()); // otherwise this part is in data area!
    return xidInfo._xid_prv;
}
inline void
logrec_t::set_xid_prev(const lsn_t &lsn)
{
    w_assert1(!is_single_sys_xct()); // otherwise this part is in data area!
    xidInfo._xid_prv = lsn;
}

inline kind_t
logrec_t::type() const
{
    return (kind_t) header._type;
}

inline u_char
logrec_t::cat() const
{
    return get_logrec_cat(static_cast<kind_t>(header._type));
}

inline void
logrec_t::set_root_page()
{
    header._flags |= t_root_page;
}

inline bool
logrec_t::is_system() const
{
    return (cat() & t_system) != 0;
}

inline bool
logrec_t::is_redo() const
{
    return (cat() & t_redo) != 0;
}

inline bool logrec_t::is_multi_page() const {
    return (cat() & t_multi) != 0;
}


inline bool
logrec_t::is_skip() const
{
    return type() == skip_log;
}

inline bool
logrec_t::is_undo() const
{
    return (cat() & t_undo) != 0;
}

inline bool
logrec_t::is_cpsn() const
{
    return (header._flags & t_cpsn) != 0;
}

inline bool
logrec_t::is_root_page() const
{
    return (header._flags & t_root_page) != 0;
}

inline bool
logrec_t::is_page_update() const
{
    // CS: I have no idea why a compensation log record is not considered a
    // page update. In fact every check of in_page_update() is or'ed with
    // is_cpsn()
    return is_redo() && !is_cpsn();
}

inline bool
logrec_t::is_logical() const
{
    return (cat() & t_logical) != 0;
}

inline bool
logrec_t::is_single_sys_xct() const
{
    return (cat() & t_single_sys_xct) != 0;
}

inline multi_page_log_t* logrec_t::data_ssx_multi() {
    w_assert1(is_multi_page());
    return reinterpret_cast<multi_page_log_t*>(data_ssx());
}
inline const multi_page_log_t* logrec_t::data_ssx_multi() const {
    w_assert1(is_multi_page());
    return reinterpret_cast<const multi_page_log_t*>(data_ssx());
}

constexpr u_char logrec_t::get_logrec_cat(kind_t type)
{
    switch (type) {
	case comment_log : return t_system;
	case tick_sec_log : return t_system;
	case tick_msec_log : return t_system;
	case benchmark_start_log : return t_system;
	case page_write_log : return t_system;
	case page_read_log : return t_system;
	case skip_log : return t_system;
	case chkpt_begin_log : return t_system;
	case loganalysis_begin_log : return t_system;
	case loganalysis_end_log : return t_system;
	case redo_done_log : return t_system;
	case undo_done_log : return t_system;
        case warmup_done_log: return t_system;
	case restore_begin_log : return t_system;
	case restore_segment_log : return t_system;
	case restore_end_log : return t_system;
	case xct_latency_dump_log : return t_system;
	case add_backup_log : return t_system;
	case evict_page_log : return t_system;
	case fetch_page_log : return t_system;

	case compensate_log : return t_logical;
	case xct_abort_log : return t_logical;
	case xct_end_log : return t_logical;

	case alloc_page_log : return t_redo|t_single_sys_xct;
	case stnode_format_log : return t_redo|t_single_sys_xct;
	case alloc_format_log : return t_redo|t_single_sys_xct;
	case dealloc_page_log : return t_redo|t_single_sys_xct;
	case create_store_log : return t_redo|t_single_sys_xct;
	case append_extent_log : return t_redo|t_single_sys_xct;
	case page_img_format_log : return t_redo;
	case update_emlsn_log : return t_redo|t_single_sys_xct;
	case btree_insert_log : return t_redo|t_undo|t_logical;
	case btree_insert_nonghost_log : return t_redo|t_undo|t_logical;
	case btree_update_log : return t_redo|t_undo|t_logical;
	case btree_overwrite_log : return t_redo|t_undo|t_logical;
	case btree_ghost_mark_log : return t_redo|t_undo|t_logical;
	case btree_ghost_reclaim_log : return t_redo|t_single_sys_xct;
	case btree_ghost_reserve_log : return t_redo|t_single_sys_xct;
	case t_btree_foster_adopt : return t_redo|t_multi|t_single_sys_xct;
	case t_btree_split : return t_redo|t_multi|t_single_sys_xct;
	case btree_compress_page_log : return t_redo|t_single_sys_xct;

        default: return t_bad_cat;
    }
}

// define 0 or 1
// Should never use this in production. This code is in place
// so that we can empirically estimate the fudge factors
// for rollback for the various log record types.
#define LOGREC_ACCOUNTING 0
#if LOGREC_ACCOUNTING
class logrec_accounting_t {
public:
    static void account(logrec_t &l, bool fwd);
    static void account_end(bool fwd);
    static void print_account_and_clear();
};
#define LOGREC_ACCOUNTING_PRINT logrec_accounting_t::print_account_and_clear();
#define LOGREC_ACCOUNT(x,y) \
        if(!smlevel_0::in_recovery()) { \
            logrec_accounting_t::account((x),(y)); \
        }
#define LOGREC_ACCOUNT_END_XCT(y) \
        if(!smlevel_0::in_recovery()) { \
            logrec_accounting_t::account_end((y)); \
        }
#else
#define LOGREC_ACCOUNTING_PRINT
#define LOGREC_ACCOUNT(x,y)
#define LOGREC_ACCOUNT_END_XCT(y)
#endif

/*<std-footer incl-file-exclusion='LOGREC_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
