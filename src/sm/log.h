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

public:
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
