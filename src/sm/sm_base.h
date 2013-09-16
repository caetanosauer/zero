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

/*<std-header orig-src='shore' incl-file-exclusion='SM_BASE_H'>

 $Id: sm_base.h,v 1.158 2010/12/08 17:37:43 nhall Exp $

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

#ifndef SM_BASE_H
#define SM_BASE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <vector>
#include "sthread.h"
#include "basics.h"


/**\file sm_base.h
 * \ingroup Macros
 */

#ifdef __GNUG__
#pragma interface
#endif

#include <climits>
#ifndef OPTION_H
#include "option.h"
#endif
#ifndef __opt_error_def_gen_h__
#include "opt_error_def_gen.h"
#endif


class ErrLog;
class sm_stats_info_t;
class xct_t;
class xct_i;

class device_m;
class io_m;
class bf_m;
class bf_tree_m;
class comm_m;
class log_m;
class lock_m;

class tid_t;
class option_t;

class rid_t;

#ifndef        SM_EXTENTSIZE
#define        SM_EXTENTSIZE        8
#endif
#ifndef        SM_LOG_PARTITIONS
#define        SM_LOG_PARTITIONS        8
#endif

class w_rc_t;
typedef   w_rc_t        rc_t;


/**\cond skip
 * This structure collects the depth on construction
 * and checks that it matches the depth on destruction; this
 * is to ensure that we haven't forgotten to release
 * an anchor somewhere.
 *
 * We're defining the CHECK_NESTING_VARIABLES macro b/c
 * this work is spread out and we want to have 1 place to
 * determine whether it's turned on or off; don't want to 
 * make the mistake of changing the debug level (on which
 * it depends) in only one of several places.
 *
 * NOTE: this doesn't work in a multi-threaded xct context.
 * That's b/c the check is too late -- once the count goes
 * to zero, another thread can change it and throw off all the
 * counts. To be sure, we'd have to use a TLS copy as well
 * as the common copy of these counts.
 *
 * This was on for debug level > 0 but it's been stable
 * enough to change it to > 2
 */
#if W_DEBUG_LEVEL > 2
#define CHECK_NESTING_VARIABLES 1
#else
#define CHECK_NESTING_VARIABLES 0
#endif
struct check_compensated_op_nesting {
#if CHECK_NESTING_VARIABLES
    xct_t* _xd;
    int _depth;
    int _line;
    const char *const _file;
    // static methods are so we can avoid having to
    // include xct.h here.
    static int compensated_op_depth(xct_t* xd, int dflt);

    check_compensated_op_nesting(xct_t* xd, int line, const char *const file)
    : _xd(xd), 
    _depth(_xd? compensated_op_depth(_xd, 0) : 0), 
    _line(line),
    _file(file)
    {
    }

    ~check_compensated_op_nesting() {
        if(_xd) {
            if( _depth != compensated_op_depth(_xd, _depth) ) {
                fprintf(stderr, 
                    "th.%d check_compensated_op_nesting(%d,%s) depth was %d is %d\n",
                    sthread_t::me()->id,
                    _line, _file, _depth, compensated_op_depth(_xd, _depth));
            }


            w_assert0(_depth == compensated_op_depth(_xd, _depth));
        }
    }
#else
    check_compensated_op_nesting(xct_t*, int, const char *const) { }
#endif
};


/**\brief Encapsulates a few types uses in the API */
class smlevel_0 : public w_base_t {
public:
    // Give these enums names for doxygen purposes:
    enum error_constant_t { eNOERROR = 0, eFAILURE = -1 };
    enum sm_constant_t { 
        page_sz = SM_PAGESIZE,        // page size (SM_PAGESIZE is set by makemake)
        ext_sz = SM_EXTENTSIZE,        // extent size
#if defined(_POSIX_PATH_MAX)
        max_devname = _POSIX_PATH_MAX,        // max length of unix path name
    // BEWARE: this might be larger than you want.  Array sizes depend on it.
    // The default might be small enough, e.g., 256; getconf() yields the upper
    // bound on this value.
#elif defined(MAXPATHLEN)
        max_devname = MAXPATHLEN,
#else
        max_devname = 1024,        
#endif
        max_vols = 20,                // max mounted volumes
        max_xct_thread = 20,        // max threads in a xct
        max_servers = 15,       // max servers to be connected with
        max_keycomp = 20,        // max key component (for btree)
        max_openlog = SM_LOG_PARTITIONS,        // max # log partitions
        max_dir_cache = max_vols * 10,

        /* XXX I want to propogate sthread_t::iovec_max here, but
           it doesn't work because of sm_app.h not including
           the thread package. */
        max_many_pages = 64,

        srvid_map_sz = (max_servers - 1) / 8 + 1,
        ext_map_sz_in_bytes = ((ext_sz + 7) / 8),

        dummy = 0
    };

    enum {
        max_rec_len = max_uint4
    };

    typedef sthread_base_t::fileoff_t fileoff_t;
    /*
     * Sizes-in-Kbytes for for things like volumes and devices.
     * A KB is assumes to be 1024 bytes.
     * Note: a different type was used for added type checking.
     */
    typedef sthread_t::fileoff_t smksize_t;
    typedef w_base_t::base_stat_t base_stat_t; 

    /**\endcond skip */

    /*
     * rather than automatically aborting the transaction, when the
     * _log_warn_percent is exceeded, this callback is made, with a
     * pointer to the xct that did the writing, and with the
     * expectation that the result will be one of:
     * - return value == RCOK --> proceed
     * - return value == eUSERABORT --> victim to abort is given in the argument
     *
     * The server has the responsibility for choosing a victim and 
     * for aborting the victim transaction. 
     *
     */

    /**\brief Log space warning callback function type.  
     *
     * For more details of how this is used, see the constructor ss_m::ss_m().
     *
     * Storage manager methods check the available log space. 
     * If the log is in danger of filling to the point that it will be
     * impossible to abort a transaction, a
     * callback is made to the server.  The callback function is of this type.
     * The danger point is a threshold determined by the option sm_log_warn. 
     *
     * The callback
     * function is meant to choose a victim xct and 
     * tell if the xct should be
     * aborted by returning RC(eUSERABORT).  
     *
     * Any other RC value is returned to the server through the call stack.
     *
     * The arguments:
     * @param[in] iter    Pointer to an iterator over all xcts.
     * @param[out] victim    Victim will be returned here. This is an in/out
     * paramter and is initially populated with the transaction that is
     * attached to the running thread.
     * @param[in] curr    Bytes of log consumed by active transactions.
     * @param[in] thresh   Threshhold just exceeded. 
     * @param[in] logfile   Character string name of oldest file to archive.
     *                     
     *  This function must be careful not to return the same victim more
     *  than once, even though the callback may be called many 
     *  times before the victim is completely aborted.
     *
     *  When this function has archived the given log file, it needs
     *  to notify the storage manager of that fact by calling
     *  ss_m::log_file_was_archived(logfile)
     */
    typedef w_rc_t (*LOG_WARN_CALLBACK_FUNC) (
            xct_i*      iter,     
            xct_t *&    victim, 
            fileoff_t   curr, 
            fileoff_t   thresh, 
            const char *logfile
        );
    /**\brief Callback function type for restoring an archived log file.
     *
     * @param[in] fname   Original file name (with path).
     * @param[in] needed   Partition number of the file needed.
     *
     *  An alternative to aborting a transaction (when the log fills)
     *  is to archive log files.
     *  The server can use the log directory name to locate these files,
     *  and may use the iterator and the static methods of xct_t to 
     *  determine which log file(s) to archive.
     *
     *  Archiving and removing the older log files will work only if
     *  the server also provides a LOG_ARCHIVED_CALLBACK_FUNCTION 
     *  to restore the
     *  archived log files when the storage manager needs them for
     *  rollback.
     *  This is the function type used for that purpose.
     *
     *  The function must locate the archived log file containing for the
     *  partition number \a num, which was a suffix of the original log file's
     *  name.
     *  The log file must be restored with its original name.  
     */
    typedef    uint32_t partition_number_t; 
    typedef w_rc_t (*LOG_ARCHIVED_CALLBACK_FUNC) (
            const char *fname,
            partition_number_t num
        );

    typedef w_rc_t (*RELOCATE_RECORD_CALLBACK_FUNC) (
	   vector<rid_t>&    old_rids, 
           vector<rid_t>&    new_rids
       );


/**\cond skip */
    enum switch_t {
        ON = 1,
        OFF = 0
    };
/**\endcond skip */

    /**\brief Comparison types used in scan_index_i
     * \enum cmp_t
     * Shorthand for CompareOp.
     */
    enum cmp_t { bad_cmp_t=badOp, eq=eqOp,
                 gt=gtOp, ge=geOp, lt=ltOp, le=leOp };

    //TODO: SHORE-KITS-API
    //Define the right types (include foster btree type?)
    /**\brief Index types */
    enum ndx_t {
        t_bad_ndx_t,             // illegal value
        t_btree,                 // B+tree with duplicates
        t_uni_btree,             // Unique-key btree
        t_rtree,                  // R*tree
    	t_mrbtree,       // Multi-rooted B+tree with regular heap files   
    	t_uni_mrbtree,          
    	t_mrbtree_l,          // Multi-rooted B+tree where a heap file is pointed by only one leaf page 
    	t_uni_mrbtree_l,               
    	t_mrbtree_p,     // Multi-rooted B+tree where a heap file belongs to only one partition
    	t_uni_mrbtree_p
    };



    /**\enum concurrency_t 
     * \brief 
     * Lock granularities 
     * \details
     * - t_cc_bad Illegal
     * - t_cc_none No locking
     * - t_cc_vol Volume-level locking
     * - t_cc_store Index-level locking
     * - t_cc_keyrange Key-range locking using fence-keys
     */
    enum concurrency_t {
        t_cc_bad,                // this is an illegal value
        t_cc_none,                // no locking
        t_cc_vol,
        t_cc_store,
        t_cc_keyrange,
        t_cc_append
    };

    /**\enum pg_policy_t 
     * \brief 
     * File-compaction policy for creating records.
     * \details
     * - t_append : append new record to file (preserve order)
     * - t_cache  : look in cache for pages with space for new record (does
     *              not preserve order)
     * - t_compact: keep file compact even if it means searching the file
     *              for space in which to create the file (does not preserve
     *              order)
     *
     * These are masks - the following combinations are sensible:
     *
     * - t_append                        -- preserve sort order
     * - t_cache | t_append              -- check the cache first, 
     *                                      append if no luck
     * - t_cache | t_compact | t_append  -- append to file as a last resort
     */
    enum pg_policy_t {
        t_append        = 0x01, // retain sort order (cache 0 pages)
        t_cache        = 0x02, // look in n cached pgs 
        t_compact        = 0x04 // scan file for space in pages 
        
    };


/**\cond skip */

    /* 
     * smlevel_0::operating_mode is always set to 
     * ONE of these, but the function in_recovery() tests for
     * any of them, so we'll give them bit-mask values
     */
    enum operating_mode_t {
        t_not_started = 0, 
        t_in_analysis = 0x1,
        t_in_redo = 0x2,
        t_in_undo = 0x4,
        t_forward_processing = 0x8
    };

#include "e_error_enum_gen.h"

    static const w_error_info_t error_info[];
    static void init_errorcodes();

    static void  add_to_global_stats(const sm_stats_info_t &from);
    static void  add_from_global_stats(sm_stats_info_t &to);

    static device_m* dev;
    static io_m* io;
    static bf_tree_m* bf;
    static lock_m* lm;

    static log_m* log;
    static tid_t* redo_tid;

    static LOG_WARN_CALLBACK_FUNC log_warn_callback;
    static LOG_ARCHIVED_CALLBACK_FUNC log_archived_callback;
    static fileoff_t              log_warn_trigger; 
    static int                    log_warn_exceed_percent; 

    static int    dcommit_timeout; // to convey option to coordinator,
                                   // if it is created by VAS

    static ErrLog* errlog;

    static bool        shutdown_clean;
    static bool        shutting_down;
    static bool        logging_enabled;
    static bool        lock_caching_default;
    static bool        do_prefetch;
    static bool        statistics_enabled;

    static operating_mode_t operating_mode;
    static bool in_recovery() { 
        return ((operating_mode & 
                (t_in_redo | t_in_undo | t_in_analysis)) !=0); }
    static bool in_recovery_analysis() { 
        return ((operating_mode & t_in_analysis) !=0); }
    static bool in_recovery_undo() { 
        return ((operating_mode & t_in_undo ) !=0); }
    static bool in_recovery_redo() { 
        return ((operating_mode & t_in_redo ) !=0); }

    // These variables control the size of the log.
    static fileoff_t max_logsz; // max log file size

    // This variable controls checkpoint frequency.
    // Checkpoints are taken every chkpt_displacement bytes
    // written to the log.
    static fileoff_t chkpt_displacement;

    // The volume_format_version is used to test compatability
    // of software with a volume.  Whenever a change is made
    // to the SM software that makes it incompatible with
    // previouly formatted volumes, this volume number should
    // be incremented.  The value is set in sm.cpp.
    static uint32_t volume_format_version;

    // This is a zeroed page for use wherever initialized memory
    // is needed.
    static char zero_page[page_sz];

    // option for controlling background buffer flush thread
    static option_t* _backgroundflush;

    enum {
            eINTERNAL = fcINTERNAL,
            eOS = fcOS,
            eOUTOFMEMORY = fcOUTOFMEMORY,
            eNOTFOUND = fcNOTFOUND,
            eNOTIMPLEMENTED = fcNOTIMPLEMENTED
    };

    /// NB: this had better match sm_store_property_t (sm_int_3.h) !!!
    // or at least be converted properly every time we come through the API
    enum store_flag_t {
        /// No flags means a store is not currently allocated
        st_unallocated = 0,

        st_regular     = 0x01, // fully logged
        st_tmp         = 0x02, // space logging only, 
                               // file destroy on dismount/restart
        st_load_file   = 0x04, // not stored in the stnode_t, 
                               // only passed down to io_m and then
                               // converted to tmp and added to the
                               // list of load files for the xct.  no
                               // longer needed
        st_insert_file = 0x08, // stored in stnode, but not on page.
                               // new pages are saved as tmp, old pages as regular.
        st_empty       = 0x100 // store might be empty - used ONLY
                               // as a function argument, NOT stored
                               // persistently.  Nevertheless, it's
                               // defined here to be sure that if
                               // other store flags are added, this
                               // doesn't conflict with them.
    };

    /* 
     * for use by set_store_deleting_log; 
     * type of operation to perform on the stnode 
     */
    enum store_operation_t {
            t_delete_store, 
            t_create_store, 
            t_set_deleting, 
            t_set_store_flags, 
            t_set_root};

    enum store_deleting_t  {
            t_not_deleting_store = 0,  // must be 0: code assumes it
            t_deleting_store, 
            t_unknown_deleting};
/**\endcond skip */
};

/**\cond skip */
ostream&
operator<<(ostream& o, smlevel_0::store_flag_t flag);

ostream&
operator<<(ostream& o, const smlevel_0::store_operation_t op);

ostream&
operator<<(ostream& o, const smlevel_0::store_deleting_t value);

/**\endcond skip */

/*<std-footer incl-file-exclusion='SM_BASE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/

