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

// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='BF_CORE_H'>

 $Id: bf_core.h,v 1.32 2010/12/17 19:36:26 nhall Exp $

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


#include "w_defines.h"
/*  -- do not edit anything above this line --   </std-header>*/

#ifndef BF_CORE_H
#define BF_CORE_H

#include "atomic_container.h"

#ifdef __GNUG__
#pragma interface
#endif

#ifndef SM_INT_0_H
#include <sm_int_0.h>
#endif

class page_s;

class bfcb_unused_list : private atomic_container {
    int _count;
public:
    bfcb_unused_list()
        : atomic_container(w_offsetof(bfcb_t, _next_free)), _count(0)
    {
    }
    ~bfcb_unused_list() {}
        int count() const { return _count; }
    void release(bfcb_t* b);
    bfcb_t* take();
    void shutdown() {  /*grot: clear */ this->~bfcb_unused_list(); }
};

class bf_core_m : public smlevel_0 
{
    friend class bf_m;
    friend class bf_cleaner_thread_t;
    friend class bfcb_t;

    struct htab;  // forward

#ifdef HTAB_UNIT_TEST_C
    // For the unit test, we define some hooks
    // into the hash table; these are defined at the end of bf_core.cpp
    // because struct htab is defined in bf_core.cpp
    friend class htab_tester; 
#endif

#ifdef HTAB_UNIT_TEST_C
    typedef bf_htab_stats_t Tstats;

    // NOTE: these static funcs are NOT thread-safe; they are
    // for unit-testing only.  
    friend bfcb_t* htab_lookup(bf_core_m *, bfpid_t const &pid, Tstats &);
    friend bfcb_t* htab_insert(bf_core_m *, bfpid_t const &pid, Tstats &);
    friend  bool    htab_remove(bf_core_m *, bfpid_t const &pid, Tstats &);
    friend  void htab_dumplocks(bf_core_m*);
    friend  void htab_count(bf_core_m *core, int &frames, int &slots);
#endif

public:
    NORET                        bf_core_m(
        uint32_t             n, 
        char*                         bp
        );
    NORET                        ~bf_core_m();

    static int                   collect(vtable_t&, bool names_too);

    bool                         get_cb(const bfpid_t& p, bfcb_t*& ret,
			                           bool keep_pinned=false) const;

    bfcb_t*                      replacement();
    w_rc_t                       grab(
        bfcb_t*&                      ret,
        const bfpid_t&                p,
        bool&                         found,
        latch_mode_t                  mode = LATCH_EX,
        timeout_in_ms                 timeout = sthread_base_t::WAIT_FOREVER);

    w_rc_t                       find(
        bfcb_t*&                      ret,
        const bfpid_t&                p, 
        latch_mode_t                  mode = LATCH_EX,
        timeout_in_ms                 timeout = sthread_base_t::WAIT_FOREVER,
        int32_t              ref_bit = 0
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
        ,
        base_stat_t*                  wait_stat = NULL
#endif
        );

    void                         publish_partial(bfcb_t* p);
    bool                         latched_by_me(bfcb_t* p) const;

    // true == no longer hold any old dirty pages
    // false == unable to flush all old dirty pages we hold
    bool                         force_my_dirty_old_pages(lpid_t const* 
                                                       wal_page=0) const;
    
    void                         publish(
        bfcb_t*                       p,
        latch_mode_t                  mode,
        const w_rc_t &                error_occured);
    
    bool                         is_mine(const bfcb_t* p) const;
    const latch_t*               my_latch(const bfcb_t* p) const;
    latch_mode_t                 latch_mode(const bfcb_t* p) const;

    w_rc_t                       pin(
        bfcb_t*                     p,
        latch_mode_t                mode = LATCH_EX);

    void                         upgrade_latch_if_not_block(
        bfcb_t*                     p,
        bool&                       would_block);
    void                         downgrade_latch(bfcb_t *p);

    void                         unpin(
        bfcb_t*&                     p,
        int                          ref_bit = 0,
        bool                         in_htab = true);

    w_rc_t                       remove(bfcb_t*& p) { 
                                        w_rc_t rc = _remove(p);
                                        return rc;
                                                                }

    void                         dump(ostream &o, bool debugging=1)const;

#if W_DEBUG_LEVEL > 2
    int                          audit() const;
#endif

    void                         snapshot(u_int& npinned, u_int& nfree);
    void                         snapshot_me(u_int& sh, u_int& ex, u_int& nd);

    friend ostream&              operator<<(ostream& out, const bf_core_m& mgr);
 
    bool                         can_replace(bfcb_t* p, int rounds);

    void                         htab_stats(bf_htab_stats_t &out) const;

private:
    struct init_thread_t;
    w_rc_t                      _remove(bfcb_t*& p);
    bool                        _in_htab(const bfcb_t* e) const;

    // FOR DEBUGGING:
    bool                        _in_htab(const lpid_t &) const;

    static queue_based_lock_t   _bfc_mutex; // never needs long lock 

    static int                  _num_bufs;
    static page_s*              _bufpool; // array of size _num_bufs
    static bfcb_t*              _buftab; // array of size _num_bufs

    static htab*                _htab;
    static void* volatile*      _htab_markers;
    static bfcb_t* volatile*    _htab_cache;
    static bfcb_unused_list     _unused; // NOTE: this cache IS USED; it
                                // holds the unused control blocks

    static int                  _hand; // clock hand

    // disabled
    NORET                        bf_core_m(const bf_core_m&);
    bf_core_m&                  operator=(const bf_core_m&);
};


extern ostream&         operator<<(ostream& out, const bf_core_m& mgr);

/**\brief Protects _clean_segment, _replace_out, and replacement() from conflicting. 
 * \details
 * Before calling _replace_out or _write_out (from _clean_segment)
 * the appropriate mutex is grabbed. This does not have anything to do
 * with pin/fix.
 * bf_core_m::replacement grabs the page mutex before making the
 * page in-transit-out (then releases it).
 */
class page_write_mutex_t {
private:
    pthread_mutex_t _page_mutex; //initialized by constructor
    operator pthread_mutex_t*() { return &_page_mutex; }
    static int const PWM_COUNT = 64;
    static page_write_mutex_t page_write_mutex[PWM_COUNT];
public:

    page_write_mutex_t() { DO_PTHREAD(pthread_mutex_init(*this, NULL)); }
    ~page_write_mutex_t() { DO_PTHREAD(pthread_mutex_destroy(*this)); }

    /// Return a pointer to the mutex. This does NOT acquire the mutex.
    static pthread_mutex_t* locate(lpid_t const &pid) 
    {
        /* each mutex protects a run of 8 pages (plus other runs which
           alias its mutex). We need to be sure that no matter where a run
           starts all pages in it acquire the same mutex (or 2 mutexes) 
         */
        return 
        page_write_mutex[(pid.page/smlevel_0::max_many_pages) % PWM_COUNT];
    }
    static void dump_all() ;
};

// For use in debugger ONLY
extern "C" void dump_page_mutexes() ;

/*******************************************************************/
/*<std-footer incl-file-exclusion='BF_CORE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
#endif // COMMENTED_OUT
