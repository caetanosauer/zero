/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
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

#include "logfunc_gen.h"
#include "xct.h"

/**
 * A log record's space is divided between a header and data. 
 * All log records' headers include the information contained in baseLogHeader.
 * Log records pertaining to transactions that produce multiple log records
 * also persist a transaction id chain (_xid and _xid_prv).
 **/ 

struct baseLogHeader
{
    uint16_t            _len;  // length of the log record
    u_char             _type; // kind_t (included from logtype_gen.h)
    u_char             _cat;  // category_t
    /* 4 */

    // Was _pid; broke down to save 2 bytes:
    // May be used ONLY in set_pid() and pid()
    // lpid_t            _pid;  // page on which action is performed
    shpid_t             _shpid; // 4 bytes
    /* 4 + 4=8 */


    vid_t               _vid;   // 2 bytes
    uint16_t             _page_tag; // tag_t 2 bytes
    /* 8 + 4= 12 */
    snum_t              _snum; // 4 bytes
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

    // For per-page chains of log-records.
    // Note that some types of log records (split, merge) impact two pages.
    // The page_prev_lsn is for the "primary" page.
    lsn_t               _page_prv;     // for per-page log chain
    /* 16+8 = 24 */
};

struct xidChainLogHeader 
{

    // NOTE for single-log system transaction following header items are not stored.
    // instead, we use these area as data area to save 16 bytes.
    // we do need to keep these 8 bytes aligned. and this is a bit dirty trick.
    // however, we really need it to reduce the volume of log we output for system transactions.
    
    tid_t               _xid;      // NOT IN SINGLE-LOG SYSTEM TRANSACTION!  (xct)tid of this xct
    /* 24+8 = 32 */
    lsn_t               _xid_prv;     // NOT IN SINGLE-LOG SYSTEM TRANSACTION! (xct)previous logrec of this xct
    /* 32+8 = 40 */
};

class logrec_t {
public:
    friend rc_t xct_t::give_logbuf(logrec_t*, const fixable_page_h *);

#include "logtype_gen.h"
    void             fill(
                            const lpid_t*  pid,
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
    bool             is_rollback() const;
    bool             is_undoable_clr() const;
    bool             is_logical() const;
    bool             is_single_sys_xct() const;
    bool             valid_header(const lsn_t & lsn_ck) const;
    smsize_t         header_size() const;

    void             redo(fixable_page_h*);
    void             undo(fixable_page_h*);

    enum {
        max_sz = 3 * sizeof(generic_page),
        hdr_non_ssx_sz = sizeof(baseLogHeader) + sizeof(xidChainLogHeader),
        hdr_single_sys_xct_sz = sizeof(baseLogHeader),
        // max_data_sz is conservative. we don't allow the last 16 bytes to be used (anyway very rarely used)
        max_data_sz = max_sz - hdr_non_ssx_sz - sizeof(lsn_t)
    };

    static_assert(hdr_non_ssx_sz == 40, "header not expected size");
    static_assert(hdr_single_sys_xct_sz == 40 - 16, "header not expected size");

       const tid_t&         tid() const;
       const vid_t&         vid() const;
       const shpid_t&       shpid() const;
       // put construct_pid() here just to make sure we can
       // easily locate all non-private/non-protected uses of pid()
       lpid_t               construct_pid() const;
         protected:
    lpid_t               pid() const;
private:
    void                 set_pid(const lpid_t& p);
public:
    bool                 null_pid() const; // needed in restart.cpp
    uint16_t              tag() const;
    smsize_t             length() const;
    const lsn_t&         undo_nxt() const;
    const lsn_t&         page_prev_lsn() const;
    void                 set_page_prev_lsn(const lsn_t &lsn);
    const lsn_t&         xid_prev() const;
    void                 set_xid_prev(const lsn_t &lsn);
    void                 set_clr(const lsn_t& c);
    void                 set_undoable_clr(const lsn_t& c);
    kind_t               type() const;
    const char*          type_str() const;
    const char*          cat_str() const;
    const char*          data() const;
    char*                data();
    const char*          data_ssx() const;
    char*                data_ssx();
    const lsn_t&         lsn_ck() const {  return *_lsn_ck(); }
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
    enum category_t {
    t_bad_cat = 0,
    t_status = 01,
    t_undo = 02,
    t_redo = 04,
    t_logical = 010,
        // Note: compensation records are not undo-able
        // (ie. they compensate around themselves as well)
        // So far this limitation has been fine.
    // old: t_cpsn = 020 | t_redo,
    t_cpsn = 020,
    t_rollback = 040, // Not a category, but means log rec was issued in 
        // rollback/abort/undo --
        // adding a bit is cheaper than adding a comment log record
    t_single_sys_xct = 80 // log by system transaction which is fused with begin/commit record
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
    lpid_t    pid;
    fill4    fill; // for purify
    lsn_t    rec_lsn;
    };

    // max is set to make chkpt_bf_tab_t fit in logrec_t::data_sz
    enum { max = (logrec_t::max_data_sz - 2 * sizeof(uint32_t)) / sizeof(brec_t) };
    uint32_t              count;
    fill4              filler;
    brec_t             brec[max];

    NORET            chkpt_bf_tab_t(
    int                 cnt, 
    const lpid_t*             p, 
    const lsn_t*             l);
    
    int                size() const;
};

struct prepare_stores_to_free_t  
{
    enum { max = (logrec_t::max_data_sz - sizeof(uint32_t)) / sizeof(stid_t) };
    uint32_t            num;
    stid_t            stids[max];

    prepare_stores_to_free_t(uint32_t theNum, const stid_t* theStids)
    : num(theNum)
    {
        w_assert3(theNum <= max);
        for (uint32_t i = 0; i < num; i++)
        stids[i] = theStids[i];
    };
    
    int size() const  { return sizeof(uint32_t) + num * sizeof(stid_t); };
};

struct chkpt_xct_tab_t {
    struct xrec_t {
    tid_t                 tid;
    lsn_t                last_lsn;
    lsn_t                undo_nxt;
    smlevel_1::xct_state_t        state;
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
    const smlevel_1::xct_state_t* state,
    const lsn_t*             last_lsn,
    const lsn_t*             undo_nxt);
    int             size() const;
};

struct chkpt_dev_tab_t 
{
    struct devrec_t {
        // pretty-much guaranteed to be an even number
        char        dev_name[smlevel_0::max_devname+1];
        fill1        byte; // for valgrind/purify
        vid_t       vid;  // (won't be needed in future)
        fill2        halfword; // for valgrind/purify
    };

    // max is set to make chkpt_dev_tab_t fit in logrec_t::data_sz
    enum { max = ((logrec_t::max_data_sz - 2*sizeof(uint32_t)) / sizeof(devrec_t))
    };
    uint32_t         count;
    fill4           filler;
    devrec_t        devrec[max];
    
    NORET           chkpt_dev_tab_t(
                            int                 count,
                            const char          **dev_name,
                            const vid_t*        vid);
    int             size() const;
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

/************************************************************************
 * Structures for prepare records
 *
 ***********************************************************************/
struct prepare_lock_totals_t {
    int32_t    num_EX;
    int32_t    num_IX;
    int32_t    num_SIX;
    fill4   filler; //for 8-byte alignment
    lsn_t    first_lsn;
    prepare_lock_totals_t(
        int a, int b, int c, const lsn_t &l) :
        num_EX(a), num_IX(b), num_SIX(c),
        first_lsn(l){ }
    int size() const     // in bytes
        { return 3 * sizeof(int32_t) + sizeof(lsn_t) + sizeof(fill4); }
};

struct prepare_log_resv_t  {
    int _rsvd;
    int _ready;
    int _used;
    prepare_log_resv_t(int a, int b, int c) : _rsvd(a), _ready(b), _used(c) {}
};

struct prepare_end_t {
    prepare_end_t(int a, int b, int c, const lsn_t &l,
            int rsvd, int ready, int used) :
        _lock(a,b,c,l), _log(rsvd, ready, used) {}
    struct prepare_lock_totals_t _lock;
    struct prepare_log_resv_t    _log;
    int size() const  { return sizeof(_lock) + sizeof(_log); }
};

struct prepare_info_t {
    // don't use bool - its size changes with compilers
    char               is_external;
    fill1               dummy1;
    fill2               dummy2;
    server_handle_t         h;
    gtid_t                  g;
    prepare_info_t(const gtid_t *_g, 
        const server_handle_t &_h) 
    { 
#ifdef ZERO_INIT
            memset(&g, '\0', sizeof(g));
            memset(&h, '\0', sizeof(h));
#endif
        if(_g) {
        is_external = 1; g = *_g;
        } else is_external = 0;
        h = _h; 
    }
    int size() const { 
        return sizeof(is_external) + 
        sizeof(dummy1) + sizeof(dummy2) +
        sizeof(server_handle_t) +
        (is_external? sizeof(gtid_t) :0);
        }
};

struct prepare_lock_t  {
    // -tid is stored in the log rec hdr
    // -all locks are long-term

    w_base_t::lock_mode_t    mode; // for this group of locks
    uint32_t     num_locks; // in the array below
    enum            { max_locks_logged = (logrec_t::max_data_sz - sizeof(w_base_t::lock_mode_t) - sizeof(uint32_t)) / sizeof(lockid_t) };

    lockid_t    name[max_locks_logged];

    prepare_lock_t(uint32_t num, lock_base_t::lmode_t _mode, 
        lockid_t *locks){
        num_locks = num;
        mode =  _mode;
        uint32_t i;
        for(i=0; i<num; i++) { name[i]=locks[i]; }
    }
    int size() const     // in bytes
        { 
            w_assert3(((num_locks * sizeof(lockid_t)) 
            + sizeof(mode) + sizeof(num_locks)) <=
            logrec_t::max_data_sz); 
            return (num_locks * sizeof(lockid_t)) 
            + sizeof(mode) + sizeof(num_locks); 
        }
};

struct prepare_all_lock_t 
{
    // -tid is stored in the log rec hdr
    // -all locks are long-term
    // 
    struct LockAndModePair {
        lockid_t    name;
        w_base_t::lock_mode_t    mode; // for this lock
    };

    uint32_t             num_locks; // in the array below
    enum            { max_locks_logged = (logrec_t::max_data_sz - sizeof(uint32_t)) / sizeof(LockAndModePair) };

    LockAndModePair pair[max_locks_logged];


    prepare_all_lock_t(uint32_t num, 
                       lockid_t *locks,
                       w_base_t::lock_mode_t *modes) {
        num_locks = num;
        uint32_t i;
        for(i=0; i<num; i++) { pair[i].name=locks[i]; pair[i].mode = modes[i]; }
    }
    int size() const     // in bytes
        { return num_locks * sizeof(pair[0]) + sizeof(num_locks); }
};

inline const shpid_t&
logrec_t::shpid() const
{
    return header._shpid;
}

inline const vid_t&
logrec_t::vid() const
{
    return header._vid;
}

inline lpid_t
logrec_t::pid() const
{
    return lpid_t(header._vid, header._snum, header._shpid);
}

inline lpid_t
logrec_t::construct_pid() const
{
// public version of pid(), renamed for grepping 
    return lpid_t(header._vid, header._snum, header._shpid);
}

inline void
logrec_t::set_pid(const lpid_t& p)
{
    header._shpid = p.page;
    header._vid = p.vol();
    header._snum = p.store();
}

inline bool 
logrec_t::null_pid() const
{
    // see lpid_t::is_null() for necessary and 
    // sufficient conditions
    bool result = (header._shpid == 0);
    w_assert3(result == (pid().is_null())); 
    return result;
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
inline void
logrec_t::set_page_prev_lsn(const lsn_t &lsn)
{
    // What do we need to assert in order to make sure there IS a page_prv?
    header._page_prv = lsn;
}

inline const tid_t&
logrec_t::tid() const
{
    w_assert1(!is_single_sys_xct()); // otherwise this part is in data area!
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
    return is_redo() && !is_cpsn() && (!null_pid());
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
xct_list_t::size() const
{
    return (char*) &xrec[count] - (char*) this; 
}

inline int
chkpt_dev_tab_t::size() const
{
    return (char*) &devrec[count] - (char*) this; 
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
