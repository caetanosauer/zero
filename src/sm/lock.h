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

/*<std-header orig-src='shore' incl-file-exclusion='LOCK_H'>

 $Id: lock.h,v 1.67 2010/10/27 17:04:23 nhall Exp $

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

#ifndef LOCK_H
#define LOCK_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include "kvl_t.h"
#include "lock_s.h"

class xct_lock_info_t;
class lock_core_m;
class lil_global_table;

#ifdef __GNUG__
#pragma interface
#endif

class lock_m : public lock_base_t {
public:

    typedef lock_base_t::lmode_t lmode_t;
    typedef lock_base_t::duration_t duration_t;
    typedef lock_base_t::status_t status_t;

    // initialize/takedown functions for thread-local state
    static void on_thread_init();
    static void on_thread_destroy();

    NORET                        lock_m(int sz);
    NORET                        ~lock_m();

    int                          collect(vtable_t&, bool names_too);
    void                         assert_empty() const;
    void                         dump(ostream &o);

    void                         stats(
                                    u_long & buckets_used,
                                    u_long & max_bucket_len, 
                                    u_long & min_bucket_len, 
                                    u_long & mode_bucket_len, 
                                    float & avg_bucket_len,
                                    float & var_bucket_len,
                                    float & std_bucket_len
                                    ) const;

    static const lmode_t         parent_mode[NUM_MODES];

    bool                         get_parent(const lockid_t& c, lockid_t& p);

    lil_global_table*            get_lil_global_table();

    rc_t                        lock(
        const lockid_t&             n, 
        lmode_t                     m,
        duration_t                  duration = t_long,
        timeout_in_ms               timeout = WAIT_SPECIFIED_BY_XCT,
        lmode_t*                    prev_mode = 0,
        lmode_t*                    prev_pgmode = 0,
        lockid_t**                  nameInLockHead = 0
        );

    /**
     * Take an intent lock on the given volume.
     * lock mode must be IS/IX/S/X.
     */
    rc_t                        intent_vol_lock(vid_t vid, lmode_t m);
    /**
     * Take an intent lock on the given store.
     */
    rc_t                        intent_store_lock(const stid_t &stid, lmode_t m);
    /**
     * Take intent locks on the given store and its volume in the same mode.
     * This is used in usual operations like create_assoc/lookup.
     * Call intent_vol_lock() and intent_store_lock() for store-wide
     * operations where you need different lock modes for store and volume.
     * If you only need volume lock, just use intent_vol_lock().
     */
    rc_t                        intent_vol_store_lock(const stid_t &stid, lmode_t m);
     
    rc_t                        unlock(const lockid_t& n);

    rc_t                        unlock_duration(duration_t duration, bool read_lock_only = false);

    rc_t                        query(
        const lockid_t&              n, 
        lmode_t&                     m, 
        const tid_t&                 tid = tid_t::null);
   

    static void                 lock_stats(
        u_long&                      locks,
        u_long&                      acquires,
        u_long&                      cache_hits, 
        u_long&                      unlocks,
        bool                         reset);

private:
    lock_core_m*                core() const { return _core; }

    rc_t                        _lock(
        const lockid_t&              n, 
        lmode_t                      m,
        lmode_t&                     prev_mode,
        lmode_t&                     prev_pgmode,
        duration_t                   duration,
        timeout_in_ms                timeout,
        lockid_t**                   nameInLockHead
        );

    lock_core_m*                _core;
};


/*<std-footer incl-file-exclusion='LOCK_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
