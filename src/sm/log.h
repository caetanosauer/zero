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

/*<std-header orig-src='shore' incl-file-exclusion='LOG_H'>

 $Id: log.h,v 1.86 2010/08/03 14:24:46 nhall Exp $

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

#ifndef LOG_H
#define LOG_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#undef ACQUIRE

class logrec_t;
class log_buf;
class generic_page;

/**
 * \defgroup SPR  Single-Page Recovery
 * \brief \b Single-Page-Recovery (\b SPR) is a novel feature to recover a single page
 * without going through the entire restart procedure.
 * \ingroup SSMXCT
 * \details
 * \section Benefits Key Benefits
 * Suppose the situation where we have 999,999 pages that are intact and flushed
 * since the previous checkpoint. Now, while running our database, we updated one page,
 * flushed it to disk, evicted from bufferpool, then later re-read it from disk, finding that
 * the page gets corrupted.
 *
 * The time to recover the single page in traditional database would be hours, analyzing the
 * logs, applying REDOs and UNDOs.
 * SPR, on the other hand, finishes in milliseconds. It fetches a single page from the backup,
 * collects only relevant transactional logs for the page, and applies them to the page.
 *
 * \section Terminology Terminology
 * \li A \b page here refers to a B-tree page. It does NOT refer to
 * other types of pages (e.g., page-allocation bitmap pages, store-node pages).
 *
 * \li \b Page-LSN of page X, or PageLSN(X), refers to the log sequence number
 * of the latest update to the logical page X, regardless of whether the page is currently
 * in the buffer pool or its cleanliness.
 *
 * \li \b Recorded-Page-LSN of page X in data source Y, or RecordedPageLSN(X, Y), refers to
 * the log sequence number of the latest update to a particular image of page X stored in
 * data source Y (e.g., bufferpool, media, backup as of Wed, backup as of Thr, etc)
 *
 * \li \b Expected-Minimum-LSN of page X, or EMLSN(X), refers to the Page-LSN of X at the
 * time of its latest eviction from the buffer pool when the page X was dirty. EM-LSN of X
 * is stored in X's parent page, except in the case of the root page.
 *
 * \section Invariantes Invariantes
 * \li EMLSN(X) <= PageLSN(X)
 * \li EMLSN(X) = PageLSN(X) only when X had no updates since previous write-out.
 * \li EMLSN(X) > 0
 *
 * \section Algorithm Algorithm Overview
 * Single page recovery is invoked only when a page is first brought into the buffer pool.
 * When we bring a page, X, into the buffer pool, we compare RecordedPageLSN(X, media)
 * to EMLSN(X). If we see that that EMLSN(X) > RecordedPageLSN(X, media),
 * then we will invoke single page recovery.
 *
 * Given a parent page P with a child page C, EMLSN(C) is updated only when the child page C is
 * evicted from the buffer pool. This is in keeping with the invariant EMLSN(C) <= PageLSN(C).
 * Given a parent page P with a child page C, updating EMLSN(C) is a logged system transaction
 * that does change PageLSN(P).
 *
 * The buffer pool will evict only complete subtrees. Evicting a page from the buffer pool
 * means that all of that page's children must have already been evicted.
 * \li The buffer pool evicts pages bottom-up (leaf pages first, then parents,
 * then grandparents, etc).
 * \li Single page recovery works top-down --- a page cannot be recovered until all of its
 * ancestors have been recovered.
 * \li Conversely, bringing a page into the buffer pool requires that all of that page's
 * ancestors must also be in the buffer pool.
 *
 * Initially, the log records to be recovered will fit in memory. Ultimately, we intend to
 * create a recovery index (so that the log records won't have to fit into memory).
 *
 * \section LSN-CHAIN Per-Page LSN Chain
 * To maintain the per-page LSN chain mentioned above, we have baseLogHeader#_page_prv
 * in logrec_t and multi_page_log_t#_page2_prv.
 * These properties are populated in log manager when we call
 * xct_t#give_logbuf(). We use these fields to collect relevant logs in
 * log_core#_collect_single_page_recovery_logs().
 *
 * \section EMLSN-BTREE EMLSN in B-tree pages
 * We also have EMLSN fields for all page pointers in B-tree pages, which is required
 * to tell "from where we should start collecting relevant per-page logs" in case of SPR.
 * See the setters/getters of btree_page_h linked to this HTML.
 *
 * \section References References
 * More details are given in the following papers.
 * \li G. Graefe et al., "Definition, Detection, and Recovery of Single-page Failures,
 *  a Fourth Class of Database Failures". VLDB'12.
 * \li G. Graefe et al., "Self-diagnosing and self-healing indexes". DBTest'12.
 * \li G. Graefe et al., "Foster B-trees". TODS'12.
 */

/**\brief Log manager interface class.
 *\details
 * This is is exposed to the rest of the server.
 * A small amount of the implementation is in here, because
 * such part is needed for things like handling the out-of-log-space
 * callbacks.
 * The details are in the log_core (derived) class.
 *
 * A log is created by the server by
 * calling static new_log_m, not with new/constructor.
 * This is in part because there are so many ways for failure and we
 * need to be able to return a w_rc_t.
 */
class log_m : public smlevel_0 
{
    static uint32_t const _version_major;
    static uint32_t const _version_minor;
    static const char    _master_prefix[];
    static const char    _log_prefix[];
    static char          _logdir[max_devname];

protected: 
    static const char    _SLASH; 
    // give implementation class access to these.
    // partition and checkpoint management
    mutable queue_based_block_lock_t _partition_lock;
    lsn_t                   _curr_lsn;
    lsn_t                   _durable_lsn;
    lsn_t                   _master_lsn;
    lsn_t                   _min_chkpt_rec_lsn;
    fileoff_t       _space_available; // how many unreserved bytes left
    fileoff_t       _space_rsvd_for_chkpt; // can we run a chkpt now?
    fileoff_t               _partition_size;
    fileoff_t               _partition_data_size;
    bool                    _log_corruption;
    bool                    _waiting_for_space; 
    pthread_mutex_t         _space_lock; // tied to _space_cond
    pthread_cond_t          _space_cond; // tied to _space_lock

protected:
    log_m();
    // needed by log_core
    static const char *master_prefix() { return _master_prefix; }
    static const char *log_prefix() { return _log_prefix; }
    fileoff_t           partition_data_size() const { 
                            return _partition_data_size; }
    // used by implementation
    w_rc_t              _read_master( 
                            const char *fname,
                            int prefix_len,
                            lsn_t &tmp,
                            lsn_t& tmp1,
                            lsn_t* lsnlist,
                            int&   listlength,
                            bool&  old_style
                            );
    void                _make_master_name(
                            const lsn_t&        master_lsn, 
                            const lsn_t&        min_chkpt_rec_lsn,
                            char*               buf,
                            int                 bufsz,
                            bool                old_style = false);

public:
    // public for use in xct_impl in log-full handling... 
    /**\brief 
     * \details 
     * Set at constructor time and when a new master is created (set_master)
     */
    lsn_t               min_chkpt_rec_lsn() const {
                            ASSERT_FITS_IN_POINTER(lsn_t);
                            // else need to grab the partition mutex
                            return _min_chkpt_rec_lsn;
                        }

    rc_t                file_was_archived(const char *file);

private:
    /**\brief Helper for _write_master */
    static void         _create_master_chkpt_contents(
                            ostream&        s,
                            int             arraysize,
                            const lsn_t*    array
                            );

    /**\brief Helper for _make_master_name */
    static void         _create_master_chkpt_string(
                            ostream&        o,
                            int             arraysize,
                            const lsn_t*    array,
                            bool            old_style = false
                            );

    /**\brief Helper for _read_master */
    static rc_t         _parse_master_chkpt_contents(
                            istream&      s,
                            int&          listlength,
                            lsn_t*        lsnlist
                            );

    /**\brief Helper for _read_master */
    static rc_t         _parse_master_chkpt_string(
                            istream&      s,
                            lsn_t&        master_lsn,
                            lsn_t&        min_chkpt_rec_lsn,
                            int&          number_of_others,
                            lsn_t*        others,
                            bool&         old_style
                            );

    /**\brief Helper for parse_master_chkpt_string */
    static rc_t         _check_version(
                            uint32_t        major,
                            uint32_t        minor
                            );
    // helper for set_master
    void                _write_master(const lsn_t &l, const lsn_t &min);


public:
    /**\brief Do whatever needs to be done before destructor is called, then destruct.
     *\details
     * Shutdown calls the desctructor; the server, after calling shutdown,
     * nulls out its pointer.
     */
    void           shutdown(); 

protected:
    /// shutdown must be used instead.
    virtual NORET  ~log_m();
public:


    /**\brief Create a log manager
     * @param[out] the_log
     * @param[in] path  Absolute or relative path name for directory of 
     * log files.
     * @param[in] wrlogbufsize  Size of log buffer, see ss_m run-time options.
     * @param[in] reformat  If true, the manager will blow away the log and start over.
     * This precludes recovery.
     *
     * \todo explain the logbuf size and log size options
     */
    static rc_t         new_log_m(
                             log_m        *&the_log,
                             const char   *path,
                             int          wrlogbufsize,
                             bool         reformat);

    /**\brief log segment size; exported for use by ss_m::options processing 
     * \details
     * \todo explain log_m::segment_size
     */
    static fileoff_t          segment_size();
    static fileoff_t          partition_size(long psize);
    static fileoff_t          min_partition_size();
    static fileoff_t          max_partition_size();

    /**\brief Return name of directory holding log files 
     * \details
     * Used by xct_t for error reporting, callback-handling.
     */
    static const char * dir_name() { return _logdir; }

    /**\brief  Return the amount of space left in the log.
     * \details
     * Used by xct_impl for error-reporting. 
     */
    fileoff_t           space_left() const { return *&_space_available; }
    fileoff_t           space_for_chkpt() const { return *&_space_rsvd_for_chkpt ; }

    /**\brief Return name of log file for given partition number.
     * \details
     * Used by xct for error-reporting and callback-handling.
     */
    static const char * make_log_name(uint32_t n,
                        char*              buf,
                        int                bufsz);
    
    /**\brief Infect the log.
     * \details
     * Used by ss_m for testing.
     * When log_corruption is turned on,
     * insertion of a log record will cause the record to be zeroed
     * in such a way to make it look like the end of the log was
     * hit; this should cause a crash and recovery.
     * Corruption is turned off right after the log record is corrupted.
     */
    void                start_log_corruption() { _log_corruption = true; }

    /**\brief Return first lsn of a given partition. 
     * \details
     * Used by xct_impl.cpp in handling of emergency log flush.
     */
    static lsn_t        first_lsn(uint32_t pnum) { return lsn_t(pnum, 0); }

    /**\brief Return current lsn of the log (for insert purposes)
     * \details
     * Used by xct_impl.cpp in handling of emergency log flush.
     * Used by force_until_lsn all pages after recovery in
     *   ss_m constructor and destructor.
     * Used by restart.
     * Used by crash to flush log to the end.
     */
    lsn_t               curr_lsn()  const  {
                              // no lock needed -- atomic read of a monotonically 
                              // increasing value
                              return _curr_lsn;
                        }

    bool                squeezed_by(const lsn_t &)  const ;


    /**\brief used by crash.cpp, but only for assertions */
    lsn_t               durable_lsn() const {
                            ASSERT_FITS_IN_POINTER(lsn_t);
                            // else need to join the insert queue
                            return _durable_lsn;
                        }
    /**\brief used by restart.recover */
    lsn_t               master_lsn() const {
                            ASSERT_FITS_IN_POINTER(lsn_t);
                            // else need to grab the partition mutex
                            return _master_lsn;
                        }

    // not called from the implementation:
    rc_t                scavenge(const lsn_t &min_rec_lsn, 
                               const lsn_t &min_xct_lsn);
    rc_t                insert(logrec_t &r, lsn_t* ret);
    rc_t                insert_multiple(size_t count, logrec_t** rs, lsn_t** ret_lsns);
    rc_t                compensate(const lsn_t& orig_lsn, 
                               const lsn_t& undo_lsn);
    // used by log_i and xct_impl
    rc_t                fetch(lsn_t &lsn, logrec_t* &rec, lsn_t* nxt=NULL);

            // used in implementation also:
    virtual void        release(); // used by log_i
    virtual rc_t        flush(const lsn_t& lsn, bool block=true, bool signal=true, bool *ret_flushed=NULL);

    fileoff_t           reserve_space(fileoff_t howmuch);
    void                release_space(fileoff_t howmuch);
    rc_t                wait_for_space(fileoff_t &amt, timeout_in_ms timeout);
    static fileoff_t    take_space(fileoff_t *ptr, int amt) ;

    long                max_chkpt_size() const;
    bool                verify_chkpt_reservation();
    fileoff_t           consume_chkpt_reservation(fileoff_t howmuch);
    void                activate_reservations() ;
                     
    void                set_master(const lsn_t& master_lsn, 
                            const lsn_t& min_lsn, 
                            const lsn_t& min_xct_lsn);


    // used by bf_m
    lsn_t               global_min_lsn() const { 
                          return std::min(master_lsn(), min_chkpt_rec_lsn()); }
    lsn_t               global_min_lsn(lsn_t const &a) const { 
                          return std::min(global_min_lsn(), a); }
    // used by implementation
    lsn_t               global_min_lsn(lsn_t const &a, lsn_t const &b) const { 
                          return std::min(global_min_lsn(a), b); }
    
    // flush won't return until target lsn before durable_lsn(), so
    // back off by one byte so we don't depend on other inserts to
    // arrive after us
    // used by bf_m
    rc_t    flush_all(bool block=true) { 
                          return flush(curr_lsn().advance(-1), block); }

    /**
    * \brief Apply single-page-recovery to the given page.
    * \ingroup SPR
    * \NOTE This method returns an error if the user had truncated
    * the transaction logs required for the recovery.
    * @param[in, out] p the page to recover.
    * @param[in] emlsn the LSN up to which we should recover the page.
    * @pre p->lsn < emlsn (otherwise, you shouldn't have called this method)
    * @pre p is already fixed with exclusive latch
    */
    rc_t recover_single_page(generic_page* p, const lsn_t &emlsn);

    /**
    * \brief Pretty-prints the content of log file to stdout in a way we
    * can easily debug single-page recovery.
    * \ingroup SPR
    * \details
    * This is for debugging, so performance is not guaranteed.
    * @param[in] pid If given, we only dump logs relevant to the page.
    * @param[in] max_lsn If given, we only dump logs required to recover
    * the page up to this LSN. We omit the logs after that.
    */
    void dump_page_lsn_chain(const lpid_t &pid, const lsn_t &max_lsn);
    /** Overload to receive only pid. \ingroup SPR */
    void dump_page_lsn_chain(const lpid_t &pid);
    /** Overload to receive neither. \ingroup SPR */
    void dump_page_lsn_chain();

    /**\brief used by partition */
    fileoff_t limit() const { return _partition_size; }
    
private:
    // no copying allowed
    log_m &operator=(log_m const &);
    log_m(log_m const &);
}; // log_m

/**\brief Log-scan iterator
 * \details
 * Used in restart to scan the log.
 */
class log_i {
public:
    /// start a scan of the given log a the given log sequence number.
    NORET                        log_i(log_m& l, const lsn_t& lsn) ;
    NORET                        ~log_i();

    /// Get the next log record for transaction, put its sequence number in argument \a lsn
    bool                         xct_next(lsn_t& lsn, logrec_t*& r);
    /// Get the return code from the last next() call.
    w_rc_t&                      get_last_rc();
private:
    log_m&                       log;
    lsn_t                        cursor;
    w_rc_t                       last_rc;
}; // log_i

inline NORET
log_i::log_i(log_m& l, const lsn_t& lsn) 
    : log(l), cursor(lsn)
{ }

inline
log_i::~log_i()
{ last_rc.verify(); }

inline w_rc_t&
log_i::get_last_rc()
{ return last_rc; }

/*<std-footer incl-file-exclusion='LOG_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
