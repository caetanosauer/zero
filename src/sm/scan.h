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

/*<std-header orig-src='shore' incl-file-exclusion='SCAN_H'>

 $Id: scan.h,v 1.94 2010/12/08 17:37:43 nhall Exp $

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

#ifndef SCAN_H
#define SCAN_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifndef XCT_DEPENDENT_H
#include <xct_dependent.h>
#endif /* XCT_DEPENDENT_H */

#include <page_alias.h>
#include "w_okvl.h"

/**\addtogroup SSMSCAN 
 * Scans can be performed on B+tree and R-tree indexes and on files
 * of records.  Iterator classes scan_index_i, scan_rt_i and scan_file_i are 
 * used for Btree's, Rtree's and files, respectively. 
 * 
 * Scans begin by creating an iterator to specify what
 * to scan and the range of the scan.
 * 
 * An iterator's next() function is used to retrieve values from
 * the scan (including the first).  Next() will set the eof
 * parameter to true only when no value can be retrieved for the
 * current call, so
 * if a file contains 2 records and next() has been called
 * twice, eof will return false on the first and second calls, but
 * true on the third.
 * 
 * The eof() function reports the value of eof returned
 * from the \b last call to next().  This means you might find
 * eof to be false, call next(), and get nothing back from next()
 * (eof is now true).
 * 
 * The scan destructor frees and un-fixes (un-pins) all resources
 * used by the scan.
 */
/**\addtogrup SSMSCAN
 * 
 * The finish() function frees (and un-fixes) resources used
 * by the scan.  finish() is called by the destructor,
 * but it is sometimes useful to call it early for scan objects
 * on the stack when the destructor might not be called until
 * the end of the function.
 * 
 * All of the scan constructors have a concurrency control
 * parameter.  This controls the granularity of locking
 * done by the scan.  The possible values of cc are:
 * 
 * - t_cc_none:    IS lock the file/index and 
 *    obtain no other locks during the scan
 * - t_cc_kvl:    IS lock the index and obtain SH key-value locks
 *    on every entry as encountered;  used only for btrees (scan_index_i)
 * - t_cc_record:IS lock the file and obtain SH locks on every 
 *   record as encountered
 * - t_cc_page:  IS lock the file/index and obtain SH locks on pages 
 *   (leaf pages, used only for rtrees)
 * - t_cc_file:  SH lock the file/index 
 *
 */

class bt_cursor_t;

/**\brief Iterator over an index.
 * \details
 * \ingroup SSMSCANI
 * To iterate over the {key,value} pairs in an index, 
 * construct an instance of this class,
 * and use its next() method to advance the cursor and the curr() method
 * to copy out keys and values into server-space.
 * It is unwise to delete or insert associations while you have a scan open on
 * the index (in the same transaction).
 *
 * Example code:
 * \code
 * stid_t fid(1,7);
 * scan_index_i scan(fid,             
                scan_index_i::ge, vec_t::neg_inf,
                scan_index_i::le, vec_t::pos_inf, false,
                ss_m::t_cc_kvl);
 * bool         eof(false);
 * do {
 *    w_rc_t rc = scan.next(eof);
 *    if(rc.is_error()) {
 *       // handle error
 *       ...
 *    }
 *    if(eof) break;
 *
 *   // get the key len and element len
 *   W_DO(scan.curr(NULL, klen, NULL, elen));
 *
 *   // Create vectors for the given lengths.
 *   vec_t key(keybuf, klen);
 *   vec_t elem(&info, elen);
 *
 *   // Get the key and element value
 *   W_DO(scan.curr(&key, klen, &elem, elen));
 *    ...
 * } while (1);
 * \endcode
 *
 */
class scan_index_i : public smlevel_top, public xct_dependent_t {
public:
    /**\brief Construct an iterator.
     * \details
     * @param[in] stid   ID of the B-Tree to be scanned.
     * @param[in] c1     Comparison type to be used in the scan for
     *                   lower bound comparison :
     *                   eq, gt, ge, lt, le
     * @param[in] bound1 Lower bound
     * @param[in] c2     Comparison type to be used in the scan for
     *                   upper bound comparison :
     *                   eq, gt, ge, lt, le
     * @param[in] bound2 Upper bound
     * @param[in] include_nulls   If true, we will consider null keys
     *        as satisfying the condition.
     * @param[in] cc   Can be used to override the concurrency_t stored in
     * the store descriptor (see ss_m::get_store_info and sm_store_info_t))
     * in a few situations.
     * @param[in] mode  Can be used to override the mode in which the
     *                   store is locked. 
     *
     * The following table describes the way these two values are used.
     * The columns represent the values of this parameter, cc.
     * The rows represent the value with which the store was created,
     * and may be determined with ss_m::get_store_info.
     * The table entries tell what kinds of locks are acquired on the store
     * and on the key-value pairs. 
     * \verbatim
        cc | t_cc_none | t_cc_modkvl | t_cc_im   | t_cc_kvl  | t_cc_file
Created    |
-------------------------------------------------------------------
t_cc_none  | IS/none   | IS/modkvl   | IS/im     | IS/kvl    | SH/none
-------------------------------------------------------------------
t_cc_modkvl| IS/none   | IS/none     | IS/modkvl | IS/modkvl | SH/none  
-------------------------------------------------------------------
t_cc_im    | IS/im     | IS/im       | IS/im     | IS/im     | SH/file 
-------------------------------------------------------------------
t_cc_kvl   | IS/kvl    | IS/kvl      | IS/kvl    | IS/kvl    | SH/file
-------------------------------------------------------------------
t_cc_file  | error     | error       | error     | error     | SH/none
-------------------------------------------------------------------
      \endverbatim
     *
     * \anchor MODKVL
     * The protocol t_cc_modkvl is a modified key-value locking protocol
     * created for the Paradise project, and behaves as follows:
     * -only allow "eq" comparsons :  bound1 : eq X and boudn2: eq X
     * -grabs an SH lock on the bound X, whether or not the index is unique
     *
     * \anchor IM
     * The protocol t_cc_im treats the value part of a key-value pair as
     * a record id, and it acquires a lock on the record rather than on
     * they key-value pair. Otherwise it is like normal key-value locking.
     *
     * Although they are called "lower bound" and "upper bound", 
     * technically the two conditions can be reversed, for example,
     * c1= le, bound1= vec_t::pos_inf 
     * and
     * c2= ge, bound2= vec_t::neg_inf.
     */
    NORET            scan_index_i(
        const stid_t&             stid,
        cmp_t                     c1,
        const cvec_t&             bound1, 
        cmp_t                     c2,
        const cvec_t&             bound2,
        bool                      include_nulls = false,
        concurrency_t             cc = t_cc_none, //TODO: SHORE-KITS-API : In Shore-sm-6.0.1 this was t_cc_kvl
        okvl_mode::element_lock_mode mode = okvl_mode::S,
        const bool                bIgnoreLatches = false
        );


    NORET            ~scan_index_i();

    /**brief Get the key and value where the cursor points.
     *
     * @param[out] key   Pointer to vector supplied by caller.  curr() writes into this vector.
     * A null pointer indicates that the caller is not interested in the key.
     * @param[out] klen   Pointer to sm_size_t variable.  Length of key is written here.
     * @param[out] el   Pointer to vector supplied by caller.  curr() writes into this vector.
     * A null pointer indicates that the caller is not interested in the value.
     * @param[out] elen   Pointer to sm_size_t variable.  Length of value is written here.
     */
    rc_t            curr(
        vec_t*           key, 
        smsize_t&        klen,
        vec_t*           el,
        smsize_t&        elen)  {
            return _fetch(key, &klen, el, &elen, false);
        }

    /**brief Move to the next key-value pair in the index.
     *
     * @param[out] eof   True is returned if there are no more pairs in the index.
     * If false, curr() may be called.
     */
    rc_t             next(bool& eof)  {
        rc_t rc = _fetch(0, 0, 0, 0, true);
        eof = _eof;
        return rc.reset();
    }

    /// Free the resources used by this iterator. Called by desctructor if
    /// necessary.
    void             finish();
    
    /// If false, curr() may be called.
    bool             eof()    { return _eof; }

    /// If false, curr() may be called.
    tid_t            xid() const { return tid; }
    ndx_t            ndx() const { return ntype; }
    const rc_t &     error_code() const { return _error_occurred; }

private:
    stid_t               _stid;
    tid_t                tid;
    ndx_t                ntype;
    cmp_t                cond2;
    bool                 _eof;

    w_rc_t               _error_occurred;
    bt_cursor_t*         _btcursor;
    bool                 _finished;
    bool                 _skip_nulls;
    concurrency_t        _cc;
    okvl_mode::element_lock_mode _mode;

    rc_t            _fetch(
        vec_t*                key, 
        smsize_t*             klen, 
        vec_t*                el, 
        smsize_t*             elen,
        bool                  skip);

    void             _init(
        cmp_t                 cond,
        const cvec_t&         bound,
        cmp_t                 c2,
        const cvec_t&         b2,
        okvl_mode::element_lock_mode           mode = okvl_mode::S);

    void            xct_state_changed(
        xct_state_t            old_state,
        xct_state_t            new_state);

    // disabled
    NORET            scan_index_i(const scan_index_i&);
    scan_index_i&    operator=(const scan_index_i&);
};

class bf_prefetch_thread_t;


/** \brief Iterator over a file of records. 
 * \ingroup SSMSCANF
 * \details
 * To iterate over the records in a file, construct an instance of this class,
 * and use its next, next_page and eof methods to located the
 * records in the file.  
 * The methods next  and next_page return a pin_i, 
 * which lets you manipulate the record (pin it in the buffer pool while you
 * use it).
 * It is unwise to delete or insert records while you have a scan open on
 * the file (in the same transaction).
 *
 * \code
 * stid_t fid(1,7);
 * scan_file_i scan(fid);
 * pin_i*      cursor(NULL);
 * bool        eof(false);
 * do {
 *    w_rc_t rc = scan.next(cursor, 0, eof);
 *    if(rc.is_error()) {
 *       // handle error
 *       ...
 *    }
 *    if(eof) break;
 *    // handle record
 *    ...
 *    const char *body = cursor->body();
 *    ...
 * } while (1);
 * \endcode
 */
class scan_file_i : public smlevel_top, public xct_dependent_t {
public:
    stid_t                stid;
    rid_t                 curr_rid;
    
    /**\brief Construct an iterator over the given store (file).
     *
     * @param[in] stid  ID of the file over which to iterate.
     * @param[in] start   ID of the first record of interest, can be 
     *                  used to start a scan in the middle of the file.
     * @param[in] cc   Locking granularity to be used.  See discussion
     *                  for other constructor.
     * @param[in] prefetch   If true, a background thread will pre-fetch
     *                 pages in support of this scan. For this to work
     *                 the server option sm_prefetch must be enabled.
     * @param[in] ignored   \e not used
     *
     * Record-level locking is not supported here because there are problems
     * with phantoms if another transaction is altering the file while
     * this scan is going on.  Thus, if you try to use record-level
     * locking, you will get page-level locking instead.
     */
    NORET            scan_file_i(
        const stid_t&            stid, 
        const rid_t&             start,
        concurrency_t            cc = t_cc_none, //TODO: SHORE-KITS-API : In Shore-sm-6.0.1 this was t_cc_file
        bool                     prefetch=false,
        okvl_mode::element_lock_mode ignored = okvl_mode::S,
        const bool               bIgnoreLatches = false);

    /**\brief Construct an iterator over the given store (file).
     *
     * @param[in] stid  ID of the file over which to iterate.
     * @param[in] cc   Locking granularity to be used.  The following are
     *                 permissible, and their implied store, page and record
     *                 lock modes are given:
     * - t_cc_none   : store - IS  page - none  record - none
     * - t_cc_record : store - IS  page - SH    record - none
     * - t_cc_page   : store - IS  page - SH    record - none
     * - t_cc_append : store - IX  page - EX    record - EX
     * - t_cc_file   : store - SH  page - none  record - none
     * @param[in] prefetch   If true, a background thread will pre-fetch
     *                 pages in support of this scan. For this to work
     *                 the server option sm_prefetch must be enabled.
     * @param[in] ignored   \e not used
     *
     * Record-level locking is not supported here because there are problems
     * with phantoms if another transaction is altering the file while
     * this scan is going on.  Thus, if you try to use record-level
     * locking, you will get page-level locking instead.
     */
    NORET            scan_file_i(
        const stid_t&            stid,
        concurrency_t            cc = t_cc_none, //TODO: SHORE-KITS-API : In Shore-sm-6.0.1 this was t_cc_file
        bool                     prefetch=false,
        okvl_mode::element_lock_mode ignored = okvl_mode::S,
        const bool               bIgnoreLatches = false);

    NORET            ~scan_file_i();
    
    /* needed for tcl scripts */
    /**\brief Return the pin_i that represents the scan state.
     * @param[out] pin_ptr   Populate the caller's pointer.  
     * @param[out] eof  False if the pin_i points to a record, 
     * true if there were no next record to pin.
     *
     */
    void            cursor(
                      pin_i*&    pin_ptr,
                      bool&       eof
                    ) { pin_ptr = &_cursor; eof = _eof; }

    /**\brief Advance to the next record of the file.
     *
     * @param[out] pin_ptr   Populate the caller's pointer.  
     * @param[in] start_offset  Pin the next record at this offset. Meaningful 
     * only for large records.
     * @param[out] eof  False if the pin_i points to a record, 
     * true if there were no next record to pin.
     *
     * This must be called once to get the first record of the file.
     */
    rc_t            next(
        pin_i*&                pin_ptr,
        smsize_t               start_offset, 
        bool&                  eof);

    /**\brief Advance to the first record on the next page in the file.
     *
     * @param[out] pin_ptr   Populate the caller's pointer.  
     * This pin_i represents the state of this scan.
     * @param[in] start_offset  Pin the next record at this offset. 
     * This is meaningful only for large records.
     * @param[out] eof  False if the pin_i points to a record, 
     * true if there were no next record to pin.
     *
     * If next_page is
     * called after the scan_file_i is initialized it will advance
     * the cursor to the first record in the file, just as
     * next() would do.
     */
    rc_t            next_page(
        pin_i*&                pin_ptr,
        smsize_t               start_offset, 
        bool&                  eof);

    /**\brief Free resources acquired by this iterator. Useful if
     * you are finished with the scan but not ready to delete it.
     */
    void            finish();
    /**\brief End of file was reached. Cursor is not usable.*/
    bool            eof()        { return _eof; }
    /**\brief Error code returned from last method call. */
    const rc_t &    error_code() const { return _error_occurred; }
    /**\brief ID of the transaction that created this iterator */
    tid_t           xid() const { return tid; }

protected:
    tid_t            tid;
    bool             _eof;
    w_rc_t           _error_occurred;
    pin_i            _cursor;
    lpid_t           _next_pid;
    concurrency_t    _cc;  // concurrency control
    okvl_mode::element_lock_mode      _page_lock_mode;
    okvl_mode::element_lock_mode      _rec_lock_mode;

    rc_t             _init(bool for_append=false);

    rc_t            _next(
        pin_i*&                pin_ptr,
        smsize_t               start_offset, 
        bool&                   eof);

    void            xct_state_changed(
        xct_state_t            old_state,
        xct_state_t            new_state);

private:
    bool              _do_prefetch;
    bf_prefetch_thread_t*    _prefetch;

    // disabled
    NORET            scan_file_i(const scan_file_i&);
    scan_file_i&        operator=(const scan_file_i&);
};

/*<std-footer incl-file-exclusion='SCAN_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
