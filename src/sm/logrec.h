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

#include "logfunc_gen.h"
#include "xct.h"
#include "w_okvl.h"

#include <boost/static_assert.hpp>

struct baseLogHeader
{
    uint16_t            _len;  // length of the log record
    u_char             _type; // kind_t (included from logtype_gen.h)
    u_char             _cat;  // category_t
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
    friend rc_t xct_t::give_logbuf(logrec_t*, const fixable_page_h *, const fixable_page_h *);
    friend class sysevent;
    friend class baseLogHeader;

#include "logtype_gen.h"
    void             fill(
                            const PageID  pid,
                            StoreID         store,
                            uint16_t        tag,
                            smsize_t       length);
    void             fill_xct_attr(
                            const tid_t&   tid,
                            const lsn_t&   last_lsn);
    bool             is_page_update() const;
    bool             is_redo() const;
    bool             is_skip() const;
    bool             is_undo() const;
    bool             is_cpsn() const;
    bool             is_multi_page() const;
    bool             is_rollback() const;
    bool             is_undoable_clr() const;
    bool             is_logical() const;
    bool             is_single_sys_xct() const;
    bool             valid_header(const lsn_t & lsn_ck = lsn_t::null) const;
    smsize_t         header_size() const;

    void             redo(fixable_page_h*);
    void             undo(fixable_page_h*);

    void fill(PageID pid, uint16_t tag, smsize_t length)
    {
        fill(pid, 0, tag, length);
    }

    void fill(PageID pid, smsize_t length)
    {
        fill(pid, 0, 0, length);
    }

    void fill(const generic_page_h& p, smsize_t length)
    {
        w_assert3(p.store() != 0);
        fill(p.pid(), p.store(), p.tag(), length);
    }

    enum {
        max_sz = 3 * sizeof(generic_page),
        hdr_non_ssx_sz = sizeof(baseLogHeader) + sizeof(xidChainLogHeader),
        hdr_single_sys_xct_sz = sizeof(baseLogHeader),
        // max_data_sz is conservative. we don't allow the last 16 bytes to be used (anyway very rarely used)
        max_data_sz = max_sz - hdr_non_ssx_sz - sizeof(lsn_t)
    };

       BOOST_STATIC_ASSERT(hdr_non_ssx_sz == 40);
       BOOST_STATIC_ASSERT(hdr_single_sys_xct_sz == 40 - 16);

       const tid_t&   tid() const;
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
    void                 set_tid(const tid_t& tid);
    void                 set_clr(const lsn_t& c);
    void                 set_undoable_clr(const lsn_t& c);
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

    friend ostream& operator<<(ostream&, const logrec_t&);

protected:
    /**
     * Bit flags for the properties of log records.
     */
    enum category_t {
    /** should not happen. */
    t_bad_cat   = 0x00,
    /** No property. */
    t_status    = 0x01,
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
    /**
     * Note: compensation records are not undo-able
     * (ie. they compensate around themselves as well)
     * So far this limitation has been fine.
     */
    t_cpsn      = 0x20,
    /**
     * Not a category, but means log rec was issued in rollback/abort/undo.
     * adding a bit is cheaper than adding a comment log record.
     */
    t_rollback  = 0x40,
    /** log by system transaction which is fused with begin/commit record. */
    t_single_sys_xct    = 0x80
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
            && _type < logrec_t::t_max_logrec
            && _cat != logrec_t::t_bad_cat
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

struct chkpt_bf_tab_t {
    struct brec_t {
    PageID    pid;      // +8 -> 8
    /*
     *  CS: store is required to mark as in-doubt on buffer pool.
     *  Perhaps we can remove the store number from buffer control blocks
     *  (bf_tree_cb_t), provided that they are not required. (TODO)
     */
    StoreID    store;    // +4 -> 12
    fill4    fill;      // for purify, +4 -> 16
    lsn_t    rec_lsn;   // +8 -> 24, this is the minimum (earliest) LSN
    lsn_t    page_lsn;  // +8 -> 32, this is the latest (page) LSN
    };

    // max is set to make chkpt_bf_tab_t fit in logrec_t::data_sz
    enum { max = (logrec_t::max_data_sz - 2 * sizeof(uint32_t)) / sizeof(brec_t) };
    uint32_t              count;
    fill4              filler;
    brec_t             brec[max];

    NORET            chkpt_bf_tab_t(
    int                 cnt,
    const PageID*             p,
    const StoreID*            s,
    const lsn_t*             l,
    const lsn_t*             pl);

    int                size() const;
};

struct prepare_stores_to_free_t
{
    enum { max = (logrec_t::max_data_sz - sizeof(uint32_t)) / sizeof(StoreID) };
    uint32_t            num;
    StoreID            stids[max];

    prepare_stores_to_free_t(uint32_t theNum, const StoreID* theStids)
    : num(theNum)
    {
        w_assert3(theNum <= max);
        for (uint32_t i = 0; i < num; i++)
        stids[i] = theStids[i];
    };

    int size() const  { return sizeof(uint32_t) + num * sizeof(StoreID); };
};

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
    page_img_format_t (const btree_page_h& page);

    void apply(fixable_page_h* p);
};

struct chkpt_xct_tab_t {
    struct xrec_t {
    tid_t                 tid;
    lsn_t                last_lsn;
    lsn_t                first_lsn;
    smlevel_0::xct_state_t        state;
    };

    // max is set to make chkpt_xct_tab_t fit in logrec_t::data_sz
    enum {     max = ((logrec_t::max_data_sz - sizeof(tid_t) -
            2 * sizeof(uint32_t)) / sizeof(xrec_t))
    };
    tid_t            youngest;    // maximum tid in session
    uint32_t            count;
    fill4            filler;
    xrec_t             xrec[max];

    NORET            chkpt_xct_tab_t(
    const tid_t&             youngest,
    int                 count,
    const tid_t*             tid,
    const smlevel_0::xct_state_t* state,
    const lsn_t*             last_lsn,
    const lsn_t*             first_lsn);
    int             size() const;
};

struct chkpt_xct_lock_t {
    struct lockrec_t {
    okvl_mode            lock_mode;
    uint32_t             lock_hash;
    };

    // max is set to make chkpt_xct_lock_t fit in logrec_t::data_sz
    enum {     max = ((logrec_t::max_data_sz - sizeof(tid_t) -
            2 * sizeof(uint32_t)) / sizeof(lockrec_t))
    };

    tid_t            tid;    // owning transaction tid
    uint32_t         count;
    fill4            filler;
    lockrec_t        xrec[max];

    NORET            chkpt_xct_lock_t(
    const tid_t&        tid,
    int                 count,
    const okvl_mode*    lock_mode,
    const uint32_t*     lock_hash);
    int             size() const;
};

struct chkpt_backup_tab_t
{
    uint32_t count;
    uint32_t data_size;
    char     data[logrec_t::max_data_sz];

    enum {
        max = (logrec_t::max_data_sz - 2 * sizeof(uint32_t))
                / (smlevel_0::max_devname)
    };

    chkpt_backup_tab_t(
        const std::vector<string>& paths);

    chkpt_backup_tab_t(int cnt, const string* paths);

    int size() const {
        return data_size + sizeof(uint32_t) * 2;
    }

    void read(std::vector<string>& paths);
};

struct chkpt_restore_tab_t
{
    enum {
        maxBitmapSize = logrec_t::max_data_sz - 2*sizeof(PageID)
            - sizeof(uint32_t),
        // one segment for each bit in the bitmap
        maxSegments = maxBitmapSize * 8
    };

    PageID firstNotRestored;
    uint32_t bitmapSize;
    char bitmap[maxBitmapSize];

    chkpt_restore_tab_t()
        : firstNotRestored(0), bitmapSize(0)
    {}

    size_t length()
    {
        return sizeof(PageID)
            + sizeof(uint32_t)
            + bitmapSize;
    }
};

struct xct_list_t {
    struct xrec_t {
        tid_t                 tid;
    };

    // max is set to make chkpt_xct_tab_t fit in logrec_t::data_sz
    enum {     max = ((logrec_t::max_data_sz - sizeof(tid_t) -
            2 * sizeof(uint32_t)) / sizeof(xrec_t))
    };
    uint32_t            count;
    fill4              filler;
    xrec_t             xrec[max];

    NORET             xct_list_t(const xct_t* list[], int count);
    int               size() const;
};

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
    if (!(header._cat & t_multi)) {
        return 0;
    }

    const multi_page_log_t* multi_log = reinterpret_cast<const multi_page_log_t*> (data_ssx());
    return multi_log->_page2_pid;
}

inline void
logrec_t::set_pid(const PageID& p)
{
    header._pid = p;
}

inline void
logrec_t::set_tid(const tid_t& tid)
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
    if (!(header._cat & t_multi)) {
        return lsn_t::null;
    }

    return data_ssx_multi()->_page2_prv;
}
inline void
logrec_t::set_page_prev_lsn(const lsn_t &lsn)
{
    // What do we need to assert in order to make sure there IS a page_prv?
    header._page_prv = lsn;
}

inline const tid_t&
logrec_t::tid() const
{
    if (is_single_sys_xct()) {
        return tid_t::null;
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

inline logrec_t::kind_t
logrec_t::type() const
{
    return (kind_t) header._type;
}

inline u_char
logrec_t::cat() const
{
    return header._cat & ~t_rollback;
}

inline bool
logrec_t::is_rollback() const
{
    return (header._cat & t_rollback) != 0;
}

inline void
logrec_t::set_clr(const lsn_t& c)
{
    w_assert0(!is_single_sys_xct()); // CLR shouldn't be output in this case
    header._cat &= ~t_undo; // can't undo compensated
             // log records, whatever kind they might be
             // except for special case below
             // Thus, if you set_clr, you're meaning to compensate
             // around this log record (not undo it).
             // The t_undo bit is what distinguishes this normal
             // compensate-around case from the special undoable-clr
             // case, which requires set_undoable_clr.
             // NOTE: the t_undo bit is set by the log record constructor.
             // Once we turn it off, we do not re-insert that bit (except
             // as done with the special-case set_undoable_clr).

     w_assert0(!is_undoable_clr());
    header._cat |= t_cpsn;

    // To shrink log records,
    // we've taken out _undo_nxt and
    // overloaded _prev.
    // _undo_nxt = c;
    xidInfo._xid_prv = c; // and _xid_prv is data area if is_single_sys_xct
}

inline bool
logrec_t::is_undoable_clr() const
{
    return (header._cat & (t_cpsn|t_undo)) == (t_cpsn|t_undo);
}


inline bool
logrec_t::is_redo() const
{
    return (header._cat & t_redo) != 0;
}

inline bool logrec_t::is_multi_page() const {
    return (header._cat & t_multi) != 0;
}


inline bool
logrec_t::is_skip() const
{
    return type() == t_skip;
}

inline bool
logrec_t::is_undo() const
{
    return (header._cat & t_undo) != 0;
}


/* The only case of undoable_clr now is the alloc_file_page.
 * This log record is not redoable, so it is not is_page_update.
 * If you add more cases of undoable_clr, you will have to analyze
 * the code in analysis_pass carefully, esp where is_page_update() is
 * concerned.
 */
inline void
logrec_t::set_undoable_clr(const lsn_t& c)
{
    bool undoable = is_undo();
    set_clr(c);
    if(undoable) header._cat |= t_undo;
}

inline bool
logrec_t::is_cpsn() const
{
    return (header._cat & t_cpsn) != 0;
}

inline bool
logrec_t::is_page_update() const
{
    // old: return is_redo() && ! is_cpsn();
    return is_redo() && !is_cpsn();
}

inline bool
logrec_t::is_logical() const
{
    return (header._cat & t_logical) != 0;
}

inline bool
logrec_t::is_single_sys_xct() const
{
    return (header._cat & t_single_sys_xct) != 0;
}

inline multi_page_log_t* logrec_t::data_ssx_multi() {
    w_assert1(is_multi_page());
    return reinterpret_cast<multi_page_log_t*>(data_ssx());
}
inline const multi_page_log_t* logrec_t::data_ssx_multi() const {
    w_assert1(is_multi_page());
    return reinterpret_cast<const multi_page_log_t*>(data_ssx());
}

inline int
chkpt_bf_tab_t::size() const
{
    return (char*) &brec[count] - (char*) this;
}

inline int
chkpt_xct_tab_t::size() const
{
    return (char*) &xrec[count] - (char*) this;
}

inline int
chkpt_xct_lock_t::size() const
{
    return (char*) &xrec[count] - (char*) this;
}

inline int
xct_list_t::size() const
{
    return (char*) &xrec[count] - (char*) this;
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
