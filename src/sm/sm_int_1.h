/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

/*<std-header orig-src='shore' incl-file-exclusion='SM_INT_1_H'>

 $Id: sm_int_1.h,v 1.14 2010/10/27 17:04:23 nhall Exp $

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

#ifndef SM_INT_1_H
#define SM_INT_1_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#if defined(SM_SOURCE) && !defined(SM_LEVEL)
#    define SM_LEVEL 1
#endif

#ifndef SM_INT_0_H
#include "sm_int_0.h"
#endif

class chkpt_m;
class restart_m;
class btree_m;
class ss_m;

/* xct_freeing_space implies that the xct is completed, but not yet freed stores and
   extents.  xct_ended implies completed and freeing space completed */
class smlevel_1 : public smlevel_0 {
public:

    /**\todo xct_state_t */
    // The numeric equivalents of state are not significant; they are
    // given here only for convenience in debugging/grepping
    // Well, their ORDER is significant, so that you can only
    // change state to a larger state with change_state().
    enum xct_state_t {  xct_stale = 0x0,  
                        xct_active = 0x1,  // active or rolling back in
                                           // doing rollback_work
                                           // It is also used in Recovery for loser transaction
                                           // because it is using the standard rollback logic
                                           // for loser txn, check the _loser_xct flag
                                           // in xct_t
                        xct_chaining = 0x3, 
                        xct_committing = 0x4, 
                        xct_aborting = 0x5,  // normal transaction abort
                        xct_freeing_space = 0x6, 
                        xct_ended = 0x7
    };

    // xct implementation
    enum xct_impl_t {
        XCT_TRADITIONAL = 0,
        XCT_PLOG = 1
    };

    static xct_impl_t xct_impl;

    // Checkpoint manager
    static chkpt_m*    chkpt;

    // Recovery manager
    static restart_m*  recovery;

    static btree_m* bt;

    static ss_m*    SSM;    // we will change to lower case later

    /**\brief Store property that controls logging of pages in the store.
     * \ingroup SSMSTORE
     * \details
     * - t_regular: All pages in the store are fully logged for ACID properties.
     * - t_temporary: Structural changes to the store are guaranteed by
     *             logging but user data are not.   In the case of indexes,
     *             the ihtegrity of the index is not guaranteed: only the
     *             integrity of the store is guaranteed. In the event of
     *             abort, the index must be destroyed.
     *             Temporary stores are destroyed when a volume is mounted or
     *             dismounted, so they do not survive restart, regardless
     *             whether a crash occurred.
     * - t_load_file: A store that is created with this property starts out
     *             as a t_tempory store and is converted to a t_regular
     *             store upon commit.
     * - t_insert_file: Updates to existing pages are fully logged (as if the
     *             store were t_regular), but pages allocated while the 
     *             store has t_insert_file are not logged. This is useful for
     *             bulk-loading, e.g., a store is bulk-loaded in one 
     *             transaction (t_load_file), which commits (now the file is
     *             t_regular); subsequent appends to the file would incur
     *             full logging, so the subsequent transaction can change the
     *             store's property to t_insert_file, append the data, 
     *             and change the store's property back to t_regular.
     *
     * \verbatim
     * ------------------------------------------------------------
     * ------------------------------------------------------------
     * Permissible uses of store property by storage manager client:
     * ------------------------------------------------------------
     * Create a btree index: | Change it to:
     *    t_tmp NO
     *    t_load_file YES    | tmp NO load_file NO insert_file NO regular YES
     *    t_insert_file YES  | tmp NO load_file NO insert_file YES regular YES
     *    t_regular YES      | tmp NO load_file NO insert_file NO regular YES
     *
     * Create a file:        | Change it to:
     *    t_tmp YES          | tmp NO load_file NO insert_file YES regular YES
     *    t_load_file YES    | tmp NO load_file NO insert_file YES regular YES
     *    t_insert_file YES  | tmp NO load_file NO insert_file YES regular YES
     *    t_regular YES      | tmp NO load_file NO insert_file YES regular YES
     * ------------------------------------------------------------
     * Effects of changing a file to regular:
     *    This causes the buffer pool to 
     *    force to disk all dirty pages for the store, and 
     *    to discard (evict from the buffer pool) all the store's 
     *    pages, clean or dirty.  When these pages are next read 
     *    into the buffer pool, they will be tagged as regular. 
     * ------------------------------------------------------------
     * Effects of commit:
     *    t_tmp              remains t_tmp  
     *    t_load_file        store is t_regular**
     *    t_insert_file      store is t_regular**
     *    t_regular          ACID
     *          ** Upon creation of such a store, the storage manager pushes
     *          this store on a list to traverse and convert to regular
     *          upon commit.
     *          Upon changing a store's property to t_insert_file,
     *          the storage manager pushes the store on this same list. 
     * ------------------------------------------------------------
     * Effects of abort on user data:
     *    t_tmp              undefined: client must remove store
     *    t_load_file        undefined: client must remove store
     *    t_insert_file      undefined: client must remove store
     *    t_regular          ACID
     * ------------------------------------------------------------
     * Effects of dismount/mount/restart:
     *    t_tmp              store removed
     *    t_load_file        undefined if not commited
     *    t_insert_file      undefined if not commited
     *    t_regular          ACID
     * ------------------------------------------------------------
     * \endverbatim
     */
    enum sm_store_property_t {
    // NB: this had better match store_flag_t!!! (sm_base.h)
    t_regular     = 0x1,

    /// allowed only in create
    t_temporary    = 0x2,

    /// allowed only in create, these files start out
    /// as temp and are converted to regular on commit
    t_load_file    = 0x4,    

    /// current pages logged, new pages not logged
    /// EX lock is acquired on file.
    /// only valid with a normal file, not indices.
    t_insert_file = 0x08,    

    t_bad_storeproperty = 0x80// no bits in common with good properties
    };
};

typedef smlevel_1 smlevel_top;

ostream&
operator<<(ostream& o, smlevel_1::sm_store_property_t p);

class xct_log_warn_check_t : public smlevel_0 {
public:
    static w_rc_t check(xct_t*&);
};

#if (SM_LEVEL >= 1)
#    if defined(FILE_C) || defined(SMFILE_C)
#    define BTREE_H
#    endif
#    include <btree.h>

#    include <btcursor.h>
#    include <xct_dependent.h>
#    include <prologue.h>

#    include <lock.h>
#    include <logrec.h>
#    include <xct.h>
#    include <logarchiver.h>
#endif

#if defined(__GNUC__) && __GNUC_MINOR__ > 6
ostream& operator<<(ostream& o, const smlevel_1::xct_state_t& xct_state);
#endif

/*<std-footer incl-file-exclusion='SM_INT_1_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
