/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifdef COMMENTED_OUT
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

/*<std-header orig-src='shore' incl-file-exclusion='BF_H'>

 $Id: bf.h,v 1.102 2010/08/23 14:28:18 nhall Exp $

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

#ifndef BF_H
#define BF_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
 *  Buffer manager interface.  
 *  Do not put any data members in bf_m.
 *  Implementation is in bf.cpp bf_core.[ch].
 *
 *  Everything in bf_m is static since there is only one
 *        buffer manager.  
 */

#include <bf_s.h>
#include <generic_page.h>

class bfcb_t;
class fixable_page_h;
class bf_core_m;
class bf_cleaner_thread_t;
class bf_filter_t;
struct bf_page_writer_control_t; // forward
class bf_m_test;

class bf_m : public smlevel_0 {
    friend class bf_cleaner_thread_t;
    friend class page_writer_thread_t;
    friend class bfcb_t;
    friend class bf_m_test;
#ifdef HTAB_UNIT_TEST_C
    friend class htab_tester; 
#endif

public:
    NORET                        bf_m(uint32_t max, char *bf, uint32_t pg_writer_cnt);
    NORET                        ~bf_m();

    static int                   collect(vtable_t&, bool names_too);
    static long                  mem_needed(int n);
    
    static int                   npages();

    static bool                  is_cached(const bfcb_t* e);

    static rc_t                  refix(
        const generic_page*                     p,
        latch_mode_t                     mode);

    static rc_t                  get_page(
        const lpid_t&               pid,
        bfcb_t*                     b,
        uint16_t                     ptag,
        bool                        no_read,
        bool                        ignore_store_id);

    // upgrade page latch, only if would not block
    // set would_block to true if upgrade would block
    static void                  upgrade_latch_if_not_block(
        const generic_page*                     p,
        // MULTI-SERVER only: remove
        bool&                             would_block);

    static latch_mode_t          latch_mode(
        const generic_page*                     p
        );

    static void                  upgrade_latch(
        generic_page*&                     p,
        latch_mode_t                    m
        );
    static void                  downgrade_latch(generic_page*& p);

    static void                  unfix(
        const generic_page*                    buf, 
        bool                            dirty = false,
        int                            refbit = 1);

    static void                  unfix_dirty(
        const generic_page*&                    buf, 
            int                            refbit = 1) {
        unfix(buf, true, refbit); 
    }

    /**
     * Adds a write-order dependency such that successor is written out after predecessor.
     * @param[in] successor the page to be written later
     * @param[in] predecessor the page to be written earlier
     * @return when this registration causes a loop, this method immediately returns
     * with an error code eWRITEORDERLOOP.
     */
    static rc_t                  register_write_order_dependency(
        const generic_page* successor,
        const generic_page* predecessor);

    static rc_t                  set_dirty(const generic_page* buf);
    static bool                  is_dirty(const generic_page* buf) ;

    static void                  discard_pinned_page(const generic_page* buf);
    static rc_t                  discard_store(stid_t stid);
    static rc_t                  discard_volume(vid_t vid);
private:
    static bool                  _set_dirty(bfcb_t *b);
    static rc_t                  _discard_all();

    /**
     * To prevent cycle of write-order, this method recursively checks
     * whether there is a cycle.
     */
    static rc_t                  _check_write_order_cycle(
        int32_t suc_idx,
        bfcb_t &pre,
        bool &has_cycle);
public:
    
    // for debugging: used only with W_DEBUG_LEVEL > 0
    static  bool                 check_lsn_invariant(const generic_page *page);
    static  bool                 check_lsn_invariant(const bfcb_t *b);

    static rc_t                  force_store(
        stid_t                           stid,
        bool                             flush);
    static rc_t                  force_page(
        const lpid_t&                     pid,
        bool                             flush = false);
    static rc_t                  force_until_lsn(
        const lsn_t&                     lsn,
        bool                             flush = false);
    static rc_t                  force_all(bool flush = false);
    static rc_t                  force_volume(
        vid_t                             vid, 
        bool                             flush = false);

    static bool                 is_mine(const generic_page* buf) ;
    static const latch_t*       my_latch(const generic_page* buf) ;
    static bool                 fixed_by_me(const generic_page* buf) ;
    static bool                 is_bf_page(const generic_page* p, 
                                                  bool and_in_htab = true);
    // true == no longer hold any old dirty pages
    // false == unable to flush all old dirty pages we hold
    bool                        force_my_dirty_old_pages(lpid_t const* 
                                                       wal_page=0) const;
    
    static bfcb_t*              get_cb(const generic_page*) ;

    static void                 dump(ostream &o);
    static void                 stats(
        u_long&                     fixes, 
        u_long&                     unfixes,
        bool                        reset);
    void                        htab_stats(bf_htab_stats_t &) const;
        

    static void                 snapshot(
        u_int&                             ndirty, 
        u_int&                             nclean,
        u_int&                             nfree, 
        u_int&                             nfixed);

    static void                 snapshot_me(
        u_int&                             nsh, 
        u_int&                             nex,
        u_int&                             ndiff
        );

    static lsn_t                min_rec_lsn();
    static rc_t                 get_rec_lsn(
        int                                &start_idx, 
        int                                &count,
        lpid_t                             pid[],
        lsn_t                              rec_lsn[],
        lsn_t                              &min_rec_lsn
        );

    static rc_t                 enable_background_flushing(vid_t v);
    static rc_t                 disable_background_flushing(vid_t v);
    static rc_t                 disable_background_flushing(); // all

    static void                 activate_background_flushing(vid_t *v=0, bool aggressive=false);

private:
    static bf_core_m*           _core;

    static rc_t                 _scan(
        const bf_filter_t&               filter,
        bool                             write_dirty,
        bool                             discard);
    
    static rc_t                 _write_out(const generic_page* b, uint32_t cnt);
    static rc_t                 _replace_out(bfcb_t* b);

    static w_list_t<bf_cleaner_thread_t, queue_based_block_lock_t>*  
                                        _cleaner_threads;
    static queue_based_block_lock_t        _cleaner_threads_list_mutex;  

    static rc_t                        _clean_buf(
        bf_page_writer_control_t *         pwc,
        const std::vector<uint32_t>&       bufidxes, // indexes of chosen buftab[] entries
        timeout_in_ms                      timeout,
        bool*                              retire_flag);
    static rc_t                        _clean_segment(
        int                                count, 
        lpid_t                             pids[],
        generic_page*                            pbuf,
        timeout_in_ms                      last_pass_timeout, 
                                            // WAIT_IMMEDIATE or WAIT_FOREVER
        bool*                              cancel_flag);

    // more stats
    static void                 _incr_page_write(int number, bool bg);

};

/*<std-footer incl-file-exclusion='BF_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/

#endif // COMMENTED_OUT
