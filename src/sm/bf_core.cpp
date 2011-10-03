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
/*<std-header orig-src='shore'>

 $Id: bf_core.cpp,v 1.82 2010/12/17 19:36:26 nhall Exp $

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


#ifndef BF_CORE_C
#define BF_CORE_C

#ifdef __GNUG__
#pragma implementation "bf_core.h"
#endif

#include <stdio.h>
#include <cstdlib>
#include "sm_int_0.h"
#include "bf_s.h"
#include "bf_core.h"
#include <auto_release.h>
#include "w_findprime.h"
#include "page_s.h"
#include "log.h"
#include <w_strstream.h>
#include "atomic_templates.h"
#include <vector>
#include <algorithm>
#include <sstream>
#include <list>
#include "w_hashing.h"
#include "bf_htab.h"

page_write_mutex_t page_write_mutex_t::page_write_mutex[page_write_mutex_t::PWM_COUNT];

extern "C" void dumpthreads();
void dump_page_mutexes() 
{
    page_write_mutex_t::dump_all();
}

void page_write_mutex_t::dump_all() 
{
    for(int i=0; i < PWM_COUNT; i++) 
    {
        cerr <<  " page_mutex " << i << " @ " 
            <<  ::hex 
            << u_long(&(page_write_mutex[i]._page_mutex)) << ::dec
#ifdef Linux
// This is COMPLETELY unportable code but it's helpful for the moment...
            << " owner " << page_write_mutex[i]._page_mutex.__data.__owner
#endif
            << endl;
    }
    cerr <<  flushl;
}

#if W_DEBUG_LEVEL > 1
void bfcb_t::check() const
{
    // I can only make checks when I hold the latch at least in SH mode.
    int count = latch.held_by_me(); 
    bool mine = latch.is_mine();  // true iff I hold it in EX mode
    if(count > 0) {
        if(mine) {
            // EX mode
            // NOTE: the latch_cnt can be > 1 because THIS thread can
            // hold it more than once.
            w_assert2(latch.latch_cnt() == count);

            // NOTE: the pin count can be > 1 because the
            // protocol is : pin_frame, then latch
            // release latch, then unpin_frame
            // Thus, _pin_cnt > 1 means someone else is waiting for the latch.
            w_assert2(_pin_cnt >= 1);
        } else {
            // This is racy at best.
            // Even if it weren't racy, 
            // the latch count could be < _pin_cnt  because the
            // protocol is : pin_frame, then latch
            // release latch, then unpin_frame
            // Thus, _pin_cnt > latch_cnt means someone else 
            // is waiting for the latch.
            // w_assert2(latch.latch_cnt() <= _pin_cnt);
            w_assert2(_pin_cnt >= 1);
            w_assert2(latch.mode() == LATCH_SH);
        }
    }
}
#endif

void bfcb_t::pin_frame() { 
    atomic_inc(_pin_cnt);
    w_assert1(_pin_cnt > 0); // should never go below 0
}

void bfcb_t::unpin_frame() { 
#if W_DEBUG_LEVEL > 1
    w_assert2(_pin_cnt > 0); // should NEVER go below 0
#endif
    atomic_dec(_pin_cnt); 
    w_assert1(_pin_cnt >= 0); // should NEVER go below 0
}

// this function is important because of our rule that a page can 
// become pinned (rather than becoming more pinned) iff the caller
// holds the appropriate bucket lock. This function lets threads pin
// hot pages without breaking the rule.  
// That is to say, the caller can either increase the pin count
// or it can go back and grab the appropriate bucket lock before
// doing a first-time pin.
// 
// Returns false if it wasn't pinned (and still isn't);  
// true if it was pinned (and we incremented the pin count).
//
// Callers: bf_core_m::pin
//          bf_core_m::htab::lookup
//
// NOTE: this corresponds to the change described in 6.2.1 (page 8) and
// 7.2 (page 10) of
// the Shore-MT paper.
bool bfcb_t::pin_frame_if_pinned() {
    int old_pin_cnt = _pin_cnt;
    while(1) {
        if(old_pin_cnt == 0)
            return false;
        // if pin_cnt == old_pin_cnt increment pin_cnt and return
        // the original value of pin_cnt, whether or not we incremented it.
        int orig_pin_cnt = atomic_cas_32((unsigned*) &_pin_cnt, 
                                old_pin_cnt, old_pin_cnt+1);
        if(orig_pin_cnt == old_pin_cnt) // increment occurred
            return true;

        // if we get here it's because another thread raced in here,
        // and updated the pin count before we could.
        old_pin_cnt = orig_pin_cnt;
    }
}


/*********************************************************************
 *
 *  bf_core_m class static variables
 *
 *      _num_bufs       : number of frames
 *      _bufpool        : array of frames
 *      _buftab         : array of bf control blocks (one per frame)
 *
 *********************************************************************/

// protects bf_core itself, including the clock hand
queue_based_lock_t      bf_core_m::_bfc_mutex;

int                     bf_core_m::_num_bufs = 0;
page_s*                 bf_core_m::_bufpool = 0;
bfcb_t*                 bf_core_m::_buftab = 0;

bf_core_m::htab*        bf_core_m::_htab = 0;
bfcb_unused_list        bf_core_m::_unused;

#ifdef SUN4V
char const* db_pretty_print(lpid_t const* pid, int, char const*) {
    static char tmp[100];
    snprintf(tmp, sizeof(tmp), "%d.%d.%d", pid->_stid.vol.vol, pid->_stid.store, pid->page);
    return tmp;
}
#endif // SUN4V

#include "bf_transit_bucket.h"
transit_bucket_t   transit_bucket_t::_transit_buckets[
                              transit_bucket_t::NUM_TRANSIT_BUCKETS];


/* FRJ: Because a central _bfc_mutex was a massive bottleneck, there are
   now a bunch of different mutexen in use. Here are the rules:

   1. _bfc_mutex wholly owns the _hand, _unused and _transit. You must
   hold _bfc_mutex to acquire latches for frames in either list. 

   2. The _htab_mutexen protect their corresponding _htab buckets. No
   frame may be added to or removed from _htab without holding the
   appropriate bucket mutex. You must hold the appropriate bucket
   mutex to acquire latches for frames in that bucket, or to search
   the table for a pid. However, it is safe (though perhaps not
   useful) to check whether a frame is in the table, because that is a
   single pointer test.

   3. Each frame latch protects its frame from changes of pid, _pin_cnt,
   etc. Also, latched/pinned frames may not be removed from the _htab.

   4. In order to avoid deadlocks, blocking acquires should always go
   in _bfc_mutex -> _htab_mutexen -> latches order. See the next two rules
   for ways to deal with the gaps when going the wrong way.
   
   5. There are times where we've decided a page is not in the _htab,
   and must ensure that this remains true during the gap between
   releasing the bucket and acquiring _bfc_mutex. Any thread that adds a
   page to a given _htab bucket will overwrite the corresponding
   _htab_markers entry with a unique value; threads wishing to detect
   additions during their gap may set a marker before releasing the
   bucket. If the marker remains intact across the gap the thread can
   safely continue, else it best abort/retry.

   6. For gaps between bucket mutexen and latches, simply test whether
   the frame is still in the table and has the correct pid once the
   latch has been acquired.
   
 */
int                bf_core_m::_hand = 0; // hand of clock

inline ostream&
bfcb_t::print_frame(ostream& o, bool in_htab)
{
    if (in_htab) {
        o << pid() << '\t'
      << (dirty() ? "X" : " ") << '\t'
      << curr_rec_lsn() << '\t'
      << safe_rec_lsn() << '\t'
      << pin_cnt() << '\t'
      << latch_t::latch_mode_str[latch.mode()] << '\t'
      << latch.latch_cnt() << '\t'
      << is_hot() << '\t'
      << refbit() << '\t'
      << latch.id() 
      << endl;
    } else {
    o << pid() << '\t' 
      << " InTransit " << (old_pid_valid() ? (lpid_t)old_pid() : lpid_t::null)
      << endl << flush;
    }
    return o;
}

/*********************************************************************
 *
 *  bf_core_m::latched_by_me(p)
 *  return true if the latch is held by me() (this thread)
 *
 *********************************************************************/
bool 
bf_core_m::latched_by_me( bfcb_t* p) const
{
    return (p->latch.held_by_me() > 0); 
}

bool
bf_core_m::force_my_dirty_old_pages(lpid_t const* /*wal_page*/) const {
// #warning TODO: actually look for dirty pages instead of just guessing they exist
    return false;
}

/*********************************************************************
 *
 *  bf_core_m::is_mine(p)
 *
 *  Return true if p is latched exclussively by current thread.
 *  false otherwise.
 *
 *********************************************************************/
bool 
bf_core_m::is_mine(const bfcb_t* p) const
{
    w_assert2(p - _buftab >= 0 && p - _buftab < _num_bufs);
    w_assert2(_in_htab(p)); // we must hold *some* latch, just maybe not EX
    return p->latch.is_mine();
}

/*********************************************************************
 *
 *  bf_core_m::latch_mode()
 *
 *********************************************************************/
latch_mode_t 
bf_core_m::latch_mode(const bfcb_t* p) const
{
    w_assert2(p - _buftab >= 0 && p - _buftab < _num_bufs);
    w_assert2(_in_htab(p));
    return (latch_mode_t) p->latch.mode();
}

/*********************************************************************
 *
 *  bf_core_m::my_latch()
 *  For debugging use only.
 *
 *********************************************************************/
const latch_t * 
bf_core_m::my_latch(const bfcb_t* p) const
{
    w_assert2(p - _buftab >= 0 && p - _buftab < _num_bufs);
    w_assert2(_in_htab(p));
    return &p->latch;
}

void bfcb_t::initialize(const char *const name,
                        page_s*           bufpoolframe,
                        uint32_t           hfunc
                        )
{
    _frame = bufpoolframe;
#if W_DEBUG_LEVEL > 0
    // Let's trash the frame in a recognizable way.
    // We need to flush out bogus assertions; this will help.
    (void) memset(bufpoolframe, 'f', SM_PAGESIZE); // 'f' is 0x66
#endif
    _dirty = false;
    _pid = lpid_t::null;
    _rec_lsn = lsn_t::null;

    latch.setname(name);
    zero_pin_cnt();

    _refbit = 0;
    _hotbit = 0;
    _hash = 0;
    _hash_func = hfunc;
    
    _write_order_dependencies = new std::list<int32_t>();
    w_assert1(_write_order_dependencies);
    _wod_back_pointers = new std::list<int32_t>();
    w_assert1(_wod_back_pointers);
}
void bfcb_t::destroy() {
    delete _write_order_dependencies;
    _write_order_dependencies = NULL;
    delete _wod_back_pointers;
    _wod_back_pointers = NULL;
}

/*********************************************************************
 *
 *  bf_core_m::bf_core_m(n, extra, desc)
 *
 *  Create the buffer manager data structures and the shared memory
 *  buffer pool. "n" is the size of the buffer_pool (number of frames).
 *  "Desc" is an optional parameter used for naming latches; it is used only
 *  for debugging purposes.
 *
 *********************************************************************/


struct bf_core_m::init_thread_t : public smthread_t 
{
    bf_core_m* _bfc;
    long _begin;
    long _end;
    // char const* _desc;
    init_thread_t(bf_core_m* bfc, long b, long e)
        : smthread_t(t_regular, "bf_core_m::init_thread", WAIT_NOT_USED),
             _bfc(bfc), _begin(b), _end(e)
             // , _desc(d)
    {
    }
    
    virtual void run() {
        const char *nayme = name();
        for(long i=_begin; i < _end; i++) {
            _bfc->_buftab[i].initialize(nayme, _bfc->_bufpool+i,
                                   htab::HASH_COUNT
                                   );
            _bfc->_unused.release(_bfc->_buftab+i);
        }
    }
};


NORET
bf_core_m::bf_core_m(uint32_t n, char *bp)
{
    _num_bufs = n;

    _bufpool = (page_s *)bp;
    w_assert1(_bufpool);
    w_assert1(is_aligned(_bufpool));

    int buckets = w_findprime((16*n+8)/9); // maximum load factor of 56%
    _htab = new htab(buckets);
    if (!_htab) { W_FATAL(eOUTOFMEMORY); }
    
    /*
     *  Allocate and initialize array of control info 
     */
    _buftab = new bfcb_t [_num_bufs];

    if (!_buftab) { W_FATAL(eOUTOFMEMORY); }

    // 512MB per thread...
    static int const CHUNK_SIZE = 1<<16;
    static int const MAX_THREADS = 30;
    int thread_count = std::min(
                       (_num_bufs+CHUNK_SIZE-1)/CHUNK_SIZE, 
                       int(MAX_THREADS));

    int chunk_size = (_num_bufs+thread_count-1)/thread_count;
    std::vector<init_thread_t*> threads;

    /* Create threads (thread_count) to initialize the buffer manager's 
     * control blocks
     */
    for(int i=0; i < thread_count; i++) {
        int begin = i*chunk_size;
        int end = std::min(begin+chunk_size, _num_bufs);
        threads.push_back(new init_thread_t(this, begin, end/*, desc*/));
    }
    for(unsigned int i=0; i < threads.size(); i++) {
        W_COERCE(threads[i]->fork());
    }
    for(unsigned int i=0; i < threads.size(); i++) {
        W_COERCE(threads[i]->join());
        delete threads[i];
    }
    /* Await threads that init the buffer manager's control blocks */

#if W_DEBUG_LEVEL > 5
// Check the alignment of the buffer pool entries. This code left
// in until we figure out how to guarantee the contiguity of the
// mmaped area under Linux.
    union {
      page_s *ptr;
      unsigned u;
    } pun1, pun2;
    cout << " sizeof(page_s) " 
    << ::hex << "0x" << sizeof(page_s) 
    << ::dec << " " << sizeof(page_s) << endl;
    for(int i=0; i < _num_bufs; i++) {
        pun2.ptr = _buftab[i].frame;
        cout << " check entry " << i 
             <<  " in entry located at 0x" 
            << ::hex
        << (unsigned)(&_buftab[i])
                << " frame  @ " 
        << ::hex << pun2.u << ::dec 
                << endl;
        w_assert1(pun2.ptr != 0);
        if(i>0) {
           w_assert1(pun1.u + sizeof(page_s) == pun2.u);
        }
        pun1.ptr = pun2.ptr;
    }
#endif
}


/*********************************************************************
 *
 *  bf_core_m::~bf_core_m()
 *
 *  Destructor. There should not be any frames pinned when 
 *  bf_core_m is being destroyed.
 *
 *********************************************************************/
NORET
bf_core_m::~bf_core_m()
{
    for (int i = 0; i < _num_bufs; i++) {
        w_assert9(! _in_htab(_buftab + i) );
        _buftab[i].destroy();
    }
    delete _htab;

    delete [] _buftab;
    _unused.shutdown(); // reinitialize it 
}


/*********************************************************************
 *
 *  bf_core_m::_in_htab(e)
 *
 *  Return true iff e is in the hash table.
 *
 *********************************************************************/
bool
bf_core_m::_in_htab(const bfcb_t* e) const
{
    return e->hash_func() != htab::HASH_COUNT;
}


/*********************************************************************
 *
 *  bf_core_m::get_cb(p)
 *
 *  Returns true if page "p" is cached. false otherwise.
 *
 *********************************************************************/
bool
bf_core_m::get_cb(const bfpid_t& p, bfcb_t*& ret, bool keep_pinned
        /*=false*/) const
{
    ret = 0;

    /*
     * We don't want to catch any cuckoo
     * operations in mid-flight, which could have been going on while
     * we did the lookup.
     */
    bfcb_t* f = _htab->lookup(p);

    if (f)  {
        // if we found it at all, 
        // it should have already been pinned (by this thread)
        w_assert2(f->pin_cnt() > 0);
        if(!keep_pinned) f->unpin_frame(); // get rid of the extra pin
        ret = f;
        return true;
    }
    return false;
}

/* This pair atomically updates a singly linked list.
 */
bfcb_t* bfcb_unused_list::take() {
    union {void* v; bfcb_t* b; } u = { pop() };
    if(u.b) {
        w_assert1(u.b->pin_cnt() == 0);
        u.b->zero_pin_cnt();
        atomic_dec(_count);
    }
    return u.b;
}
void bfcb_unused_list::release(bfcb_t* frame) 
{
    w_assert9(!frame->dirty);
    w_assert0(frame->pin_cnt() == 0);
    w_assert9(!frame->latch.is_latched());

    push(frame);
    atomic_inc(_count);
}


/*********************************************************************
 *
 *  bf_core_m::grab(ret, pid, found, is_new, mode, timeout)
 *
 *  Assumptions: "ret" refers to a free frame, that is, it's not
 *  in the hash table, as it came off the free list or is in the
 *  in-transit-out table.
 *
 * The frame is already EX-latched by the caller.
 *
 *  Check that the given pid isn't elsewhere in the buffer pool. 
 *  If so,
 *     release the latch on "ret" frame, 
 *     grab the "mode" latch on the frame already in the buffer pool, 
 *     set "found" to true,
 *     set "ret" to point to that frame and 
 *     return.
 *  If not,
 *     hang onto the EX latch on the given frame,
 *     insert the frame/pid in the hash table,
 *     set "found" to false,
 *     and return the given frame in "ret".
 *
 * FRJ: If find() fails the caller is
 * expected to obtain a replacement frame by calling replacement(), which
 * it then passes to grab(). 
 *
 * Regardless of whether grab() needs the replacement, 
 * the old page, if dirty, became in-transit-out (this was already
 * by the  caller). 
 *
 *********************************************************************/
// only caller is bf_m::_fix
w_rc_t 
bf_core_m::grab(
    bfcb_t* &        ret, // already-allocated replacement frame, not latched.
    const bfpid_t&   pid, // page we seek
    bool&            found, // found in the hash table so we didn't use the
                     // control block passed in via "ret".
    latch_mode_t     mode, // desired latch mode
    timeout_in_ms    timeout
    )
{
    w_assert2(mode != LATCH_NL); // NEH changed to assert2
    w_assert2(ret != NULL); // allocated replacement frame

    INC_TSTAT(bf_look_cnt);

    // We already have the EX-latch. It was acquired in replacement(),
    // and no longer freed in publish_partial.
    w_assert1(ret->latch.is_latched()); 
    w_assert1(ret->latch.is_mine());  // EX mode

    // Not yet pinned.  Everywhere else we pin before latching; here we latch then pin.
    w_assert2(!ret->pin_cnt()); 
    ret->pin_frame(); 

    ret->check();  // EX mode so strong check

    /* now make sure the frame we want isn't already there. It could
       be in-transit-out or we might have missed it in the htab. Our
       caller will deal with waiting on an in-transit-out page, but we
       have to be sure it's not in the htab anywhere
    */
    
    // compute the hashes before grabbing the mutex (~20
    // cycles/hash). Really we should be caching the ones we computed
    // in find() just before this function got called!
    static int const COUNT = htab::HASH_COUNT;
    int idx[COUNT];
    for(int i=0; i < COUNT; i++) {
        idx[i] = _htab->hash(i, pid);
    }

    transit_bucket_t* volatile tb = &transit_bucket_t::get(pid);
    CRITICAL_SECTION(cs, tb->_tb_mutex); // PROTOCOL

    tb->await_not_in_transit_out(pid);

    /* NOTE: If our victim is non-null and dirty, we should really
       wait after flushing it -- there's a good chance we would exit
       transit during the flush. However, this is expected to be rare,
       so we'll hold off until we see it's a problem.
    */

    /* See if the pid is in the hash table 
     * TODO: NANCY: WHY CAN'T WE JUST DO _htab->lookup()? DOCUMENT THIS
     * From Ryan: this is because of the race BUG_HTAB_RACE (gnats #35)
     * ...so perhaps if we fix the race we can use lookup() here.
     *
     * See 
     * Shore-MT paper, section 7.3 (page 10) 2nd paragraph, #19
     * */
    int i;
    int residents[COUNT];
    int same = 0;

    bfcb_t* p = NULL; 
    for(int attempt=0; p == NULL && same != COUNT; attempt++) 
    {
        same = 0;
        for(i=0; i < COUNT; i++) {
            htab::bucket &b = _htab->_table[idx[i]];
            w_assert2(b._lock.is_mine()==false);
            b._lock.acquire(); // PROTOCOL
            w_assert2(b._lock.is_mine());
            // With multi-slotted buckets, get_frame
            // returns the slot containing the pid if it's
            // there, o.w. it returns null
            // NOTE: we must hold the lock on the bucket before
            // we can do this (get_frame)
            p = b.get_frame(pid);
            if(p && p->pid() == pid) {
                w_assert2(p->hash() == idx[i]);
                w_assert2(_in_htab(p));
                break;
            }
            // On first try, record the #residents in the bucket;
            // on subsequent attempts, if the #residents hasn't
            // changed, count that as same, meaning no change
            // in that bucket. If there's a lot going on elsewhere,
            // presumably some bucket counts will change, and
            // we might be able to find what we're seeking
            if(attempt == 0) {
                residents[i] = b._count;
            }
            else {
                if(residents[i] == b._count)
                    same++;
                else
                    residents[i] = b._count;
            }
            
            p = NULL;
            b._lock.release(); // PROTOCOL
            w_assert2(b._lock.is_mine()==false);
        }
    }

    w_assert1(ret != p);
    found = (p!=NULL);

    if( found ) {
        // oops... we found it so we don't
        // need the replacement.  put the replacement on the freelist
        // Release the (EX) latch we acquired on the replacement frame.
        // (We acquired EX latch on the frame to cover the in-transit-in
        // case. All other fixers will await the release of the page latch.)
        ret->check(); // EX mode : should be strong check
        ret->latch.latch_release(); // PROTOCOL
        ret->unpin_frame();
        _unused.release(ret); // put on free list
        
        INC_TSTAT(bf_hit_cnt);
        ret = p;
        p->pin_frame();
        
        // release the last bucket lock and acquire the latch on the
        // frame we're about to return
        _htab->_table[idx[i]]._lock.release(); // PROTOCOL
        cs.exit(); // PROTOCOL
        
        rc_t rc = p->latch.latch_acquire(mode, timeout); // PROTOCOL
        if (rc.is_error()) {
            /*
             *  Clean up and bail out.
             *  (this should not happen in the case where
             *  we've put it on the in-transit list)
             */
            p->unpin_frame();
            INC_TSTAT(bf_grab_latch_failed);
            /*
            fprintf(stderr, 
                "Unable to acquire latch %d.%d in mode %d with timeout %d\n",
                pid.store(), pid.page, mode, timeout);
            */
            return RC_AUGMENT(rc);
        }
        p->check(); // could be weak check (if SH mode)
    } else {
        // not found
        // all bucket locks already released
        // use the replacement frame. It's latched in EX mode.
        p = ret;
        w_assert2(pid.page != 0); // should never try to fix page 0
        p->set_pid (pid);
        // insert now tells us if something was moved, not evicted.
        // new htab cannot evict anyone.
        (void) _htab->insert(p);
        cs.exit(); // PROTOCOL
    }
    w_assert2(
        found? (ret->latch.mode() == mode) : (ret->latch.mode() == LATCH_EX)
    ); 

    return RCOK;
}


/*********************************************************************
 *
 *  bf_core_m::find(ret, pid, mode, timeout, ref_bit)
 *
 *  If page "pid" is cached, find() acquires a "mode" latch and returns,
 *  in "ret", a pointer to the associated bf control block; returns an
 *   error if the resource is not cached (eFRAMENOTFOUND),
 *   if we cannot latch it with the given timeout (stTIMEOUT)
 *   if the pid given is bogus (e.g., eBADVOL)
 *   or if it's cached and something's wrong with it, e.g.,eBADSTID
 *
 *
 *********************************************************************/
/*
  WARNING: FRJ: the semantics of this function have changed to no longer
   search the in-transit list for pages that might (or might not)
   enter the pool soon. This was done for the following reasons:

   (a) if we really want the page that badly, a call to _core->grab()
   would follow an eFRAMENOTFOUND, and _core->grab() checks the in-transit
   list.

   (b) if the page is in transit, it's either been evicted, in which
   case waiting for it to finish transit is a waste of time, or it's
   currently being brought into the buffer pool. Arguably, it's not
   there, and whether we could determine (with effort) that it will be
   there soon is irrelevant.

   (c) If the page is in transit, it could be a very long time (msec)
   before the call returns, and half the time we will discover the
   page was in-transit-out anyway. It's not worth extra complexity
   (and bugs) to slow down the program unless we *really* need to.

   A search of all current callers shows three types:
   
   1. If find() misses a page grab will follow immediately and check
   the in-transit list anyway (bf_m::_fix)

   2. If the page isn't in the bpool we really don't care whether it's
   on the in-transit list because we intend to put pages we find on
   that list (checkpoint threads)

   3. Unsupported and/or unused features where it's not clear whether
   the caller cares to test the in-transit list
   (bf_m::fix_if_cached). I suspect that even these don't care to
   worry about the in-transit list, but I've left a warning just in
   case it ever trips somebody up.
   
 */

w_rc_t 
bf_core_m::find(
    bfcb_t*&          ret,
    const bfpid_t&    _pid, // NOTE: vol and page identify frame; store ignored
    latch_mode_t      mode,
    timeout_in_ms     timeout,
    int32_t            ref_bit
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
        ,
        base_stat_t*                  wait_stat /*= NULL*/
#endif
)
{
    const bfpid_t        pid = _pid;
    w_assert3(ref_bit >= 0);
    
    w_assert1(mode != LATCH_NL); // NEH: changed to assert2
    bfcb_t* p = NULL;
    ret = 0;
    INC_TSTAT(bf_look_cnt);

    if( (p=_htab->lookup(pid)) == NULL )
        return RC(eFRAMENOTFOUND);
    
    DBGTHRD(<< "lookup page " << pid
            << " returned frame " 
            << ::hex << (unsigned long) (p->frame()) << ::dec
            << " frame's pid " << p->frame()->pid
            );

    w_assert2(p->pin_cnt() > 0);
    /*
      The page came to us pinned, so we can update the control
      block and acquire the latch at our leisure.
    */
    w_assert3(p->refbit() >= 0);
    if (p->refbit() < ref_bit)  p->set_refbit(ref_bit);
    w_assert3(p->refbit() >= 0);

    INC_TSTAT(bf_hit_cnt);

#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
    int before = GET_STH_STATS(latch_uncondl_nowait);
#endif
    w_rc_t rc = p->latch.latch_acquire(mode, timeout); // PROTOCOL
    if (rc.is_error())  {
        // could be stINUSE if we already have the latch and
        // this consitutes an upgrade. 
        // else it could be stTIMEOUT
        /*
         *  Clean up and bail out.
         */
        p->unpin_frame();
        return RC_AUGMENT(rc);
    }
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
    int after = *(
            (volatile base_stat_t *volatile)&(GET_STH_STATS(latch_uncondl_nowait)));
    if((after > before) || (timeout == WAIT_IMMEDIATE)) {
        // nowait 
    } else {
        // waited
        INC_TSTAT(bf_hit_wait);
        if(wait_stat) (*wait_stat)++;
    }
#endif

    if(p->old_pid_valid() && p->old_pid() == pid) {
        // a repin should never fail this way!
        // We have it pinned, so from the time we found it
        // in the htab via lookup and now, it should
        // not have been possible for someone else to have
        // removed it from the htab.
        // And... it's removed from the htab before 
        // old pid is made valid; and old pid is cleared
        // before the frame is put back in the htab or back
        // onto the free list (by grab())
        W_FATAL_MSG(eINTERNAL, << "old pid valid in find() " << p->old_pid() );
    }

    ret = p;
    w_assert2((pid == p->pid()));
    // Moved this check to the caller because if the caller
    // is a "no-read" case, it's b/c it's about to format the
    // page and the pid in the frame could be anything.
    // w_assert2((pid == p->frame()->pid) && (pid == p->pid()));
    w_assert1(p->latch.mode() >= mode); 
    w_assert1(p->latch.held_by_me() >= 1); // since mode is not NL
    if(mode == LATCH_EX) {
        w_assert1(p->latch.is_mine()); 
    }
    return RCOK;
}


/*********************************************************************/
/**\brief Publish frame p (already grabbed), awaken waiting threads.
 *
 *\details
 *  Publishes the frame "p" that was previously grab()ed with 
 *  a cache-miss. All threads waiting on the frame are awakened.
 *
 *  Leave the frame latched in 'mode', downgrading or releasing as
 *  required to get us there.
 *
 */
void
bf_core_m::publish( bfcb_t* p, latch_mode_t mode, const w_rc_t &error_occurred)
{
    /*
     *  Sanity checks
     */
    w_assert2(p - _buftab >= 0 && p - _buftab < _num_bufs);
    w_assert2(p->pin_cnt() > 0);


    // mode is LATCH_NL in error case
    w_assert2( (mode != LATCH_NL) || error_occurred.is_error()); 
    w_assert2(p->latch.is_mine()); 
    w_assert1(!p->old_pid_valid());

    /*
     *  If error, cancel request (i.e. release the latch).
     *  If there exist other requestors, leave the frame in the transit
     *  list, otherwise move it to the free list.
     */
    if (error_occurred.is_error())  {
        /* FRJ: the page is already in the htab, and there could
           easily be other threads waiting on it at this point. 
         */
        
        //cs.exit();// no need to hold _mutex for this...// FRJ: yes, there is!
        
        w_assert2(p->latch.is_mine());
        p->latch.latch_release(); // PROTOCOL
        p->unpin_frame(); // NEH: moved unpin here after latch release,
        // since the protocol seems to be: 
        // pin, then acquire latch
        // release latch, then unpin
    }
    else {
        p->check();
        // downgrade the latch to whatever mode we actually wanted
        if(mode == LATCH_SH)  {
            p->latch.downgrade(); // PROTOCOL
        }
        else if(mode == LATCH_EX) {
            // do nothing
        }
        else if(mode == LATCH_NL)  {
            // is this really allowed?
            // Do we need to unpin the frame here?
            // The assertion will tell us if this ever happens.
            w_assert0(false);
            p->latch.latch_release(); // PROTOCOL
        }
    }
}


/*********************************************************************
 *
 *  bf_core_m::publish_partial(p)
 *
 *  Partially publish the frame "p" that was acquired as a replacement
 *  frame after a cache miss.
 *
 *  This is called after bf_m::_fix had a cache miss, called replacement()
 *  to get the frame, found that the frame was in-use, and either
 *  explicitly sent it to disk or waited for a cleaner to finish
 *  sending it to disk.  
 *  This takes the frame off the in_transit_out list and invalidates
 *  the old_pid, so now this frame is indistinguishable from
 *  an unused frame returned by replacement().
 *
 * If discard==true, the frame goes back on the freelist as well
 * NOTE: discard is no longer used.
 *
 *********************************************************************/
void 
bf_core_m::publish_partial(bfcb_t* p)
{
    w_assert9(p - _buftab >= 0 && p - _buftab < _num_bufs);

    // The next assertion is not valid if pages can be pinned w/o being
    // latched. For now, it is ok in the case of page-level locking only.
    // FRJ: to cover a couple of races we need to latch now...
    w_assert2(p->latch.is_latched());
    w_assert2(p->latch.is_mine());
    w_assert2(p->old_pid_valid());

    /*
     *  invalidate old key
     */
    lpid_t pid = p->old_pid();
    p->clr_old_pid();
    
    transit_bucket_t &tb = transit_bucket_t::get(pid);
    CRITICAL_SECTION(cs, tb._tb_mutex); // PROTOCOL

    tb.make_not_in_transit_out(pid);

}


/*********************************************************************
 *
 *  bf_core_m::snapshot(npinned, nfree)
 *
 *  Return # frames pinned and # unused frames in "npinned" and
 *  "nfree" respectively.
 *
 *********************************************************************/
void 
bf_core_m::snapshot( u_int& npinned, u_int& nfree)
{
    /*
     *  No need to obtain mutex since this is only an estimate.
     */
    int count = 0;
    for (int i = _num_bufs - 1; i; i--)  {
    if (_in_htab(&_buftab[i]))  {
        if (_buftab[i].latch.is_latched() || _buftab[i].pin_cnt() > 0) ++count;
    } 
    }

    npinned = count;
    nfree = _unused.count();
}

/*********************************************************************
 *
 *  bf_core_m::snapshot_me(nsh, nex, ndiff)
 *
 *  Return # frames fixed  *BY ME()* in SH, EX mode, total diff latched
 *  frames, respectively. The last argument is because a single thread
 *  can have > 1 latch on a single page.
 *
 *********************************************************************/
void 
bf_core_m::snapshot_me( u_int& nsh, u_int& nex, u_int& ndiff)
{
    /*
     *  No need to obtain mutex since me() cannot fix or unfix
     *  a page  while me() is calling this function.
     */
    nsh = nex = ndiff = 0;
    for (int i = _num_bufs - 1; i; i--)  {
        if (_in_htab(&_buftab[i]))  {
            /* FRJ: If the latch is not locked, held_by() will return
               almost as quickly as is_latched(); if not, we have to
               call held_by anyway. Therefore, the test for
               is_latched() is unnecessary.
             */
            if (1 || _buftab[i].latch.is_latched() ) {
                // NB: don't use is_mine() because that
                // checks for EX latch.
                int times = _buftab[i].latch.held_by_me();
                if(times > 0) {
                    ndiff ++;  // different latches only
                    if (_buftab[i].latch.mode() == LATCH_SH ) {
                        nsh += times;
                    } else {
                        w_assert9 (_buftab[i].latch.mode() == LATCH_EX );
                        // NB: here, we can't *really* tell how many times
                        // we hold the EX latch vs how many times we
                        // hold a SH latch
                        nex += times;
                    }
                }
            }
        } 
    }
}

/*********************************************************************
 *
 *  bf_core_m::pin(p, mode)
 *
 *  Pin resource "p" in latch "mode".
 *
 *********************************************************************/
w_rc_t 
bf_core_m::pin(bfcb_t* p, latch_mode_t mode)
{
    /* FRJ: Our only caller is bf_m::refix(), which means we don't
       need to lock the page's bucket -- the page is already pinned by
       this thread and won't go anywhere during this call.

       Update - bf_m::_scan also calls us, and the page is *not*
       pinned first!
     */
    w_assert9(p - _buftab >= 0 && p - _buftab < _num_bufs);
    w_assert9(_in_htab(p)); // it should already be pinned!

    // pin_frame_if_pinned returns true IFF it's pinned.
    if(!p->pin_frame_if_pinned()) {
        // we need to acquire the bucket lock before grabbing a free
        // latch. The page's pid may have changed in the meantime.
        bfpid_t pid = p->pid();
        htab::bucket &b = _htab->_table[p->hash()];

        w_assert2(b._lock.is_mine()==false);
        CRITICAL_SECTION(cs, b._lock); // PROTOCOL
        w_assert2(b._lock.is_mine());
        p = b.get_frame(pid); // in case we raced...
        if(p == NULL || p->pid() != pid)
            return RC(eFRAMENOTFOUND);
        
        p->pin_frame();
    }

    // now acquire the latch (maybe again)
    w_rc_t rc = p->latch.latch_acquire(mode) ; // PROTOCOL
    p->check();
    w_assert1(p->pin_cnt() > 0);

    return rc;    
}


/*********************************************************************
 *
 *  bf_core_m::upgrade_latch_if_not_block(p, would_block)
 *
 *********************************************************************/
void 
bf_core_m::upgrade_latch_if_not_block(bfcb_t* p, bool& would_block)
{
  // FRJ: nobody will remove a latched/pinned entry from the _htab
    w_assert9(p - _buftab >= 0 && p - _buftab < _num_bufs);
    w_assert9(_in_htab(p));
    // p->pin();    // DO NOT Increment!!

    W_COERCE( p->latch.upgrade_if_not_block(would_block) );
    if(!would_block) {
        w_assert9(p->latch.mode() == LATCH_EX);
    }
}

void  bf_core_m::downgrade_latch(bfcb_t* p)
{
    p->latch.downgrade();
}

/*********************************************************************
 *
 *  bf_core_m::unpin(p, int ref_bit)
 *
 *  Unlatch the frame "p". 
 *
 *********************************************************************/
void
bf_core_m::unpin(bfcb_t*& p, int ref_bit, bool W_IFDEBUG4(in_htab))
{

    w_assert3(ref_bit >= 0);
    w_assert3(p - _buftab >= 0 && p - _buftab < _num_bufs);
    w_assert9(!in_htab || _in_htab(p));
    w_assert1(p->pin_cnt() > 0);

    /*  
     * if we were given a hint about the page's
     * about-to-be-referenced-ness, apply it.
     * but o.w., don't make it referenced. (That
     * shouldn't happen in unfix().)
     */
    w_assert3(p->refbit() >= 0);
    if (p->refbit() < ref_bit)  p->set_refbit(ref_bit);
    w_assert3(p->refbit() >= 0);


    // The following code used to be included to get the page reused
    // sooner.  However, performance tests show that that this can
    // cause recently referenced pages to be "swept" early
    // by the clock hand.
    /*
      if (ref_bit == 0) {
      _hand = p - _buftab;  // reset hand for MRU
      }
    */

    // LSN integrity checks (only if really unpinning)
    if(p->pin_cnt() == 1) {
        if(p->dirty()) {
            // NOTE: this assertion is racy: we just checked dirty, but
            // before we can check the rec_lsn, it could have been
            // cleaned, if we have an SH latch.
            // w_assert1(
            //       p->curr_rec_lsn().valid() || !smlevel_0::logging_enabled
            //     || !p->dirty()
            //   );

            // We must maintain the following invariant else we'll never be
            // able to debug recovery issues.
            // HOWEVER there are some legit exceptions
            // 1) we are in the redo part of recovery, and we
            // dirty pages but don't log them.  In that case, 
            // we get in here, and the rec_lsn reflects the tail end
            // of the log (way in the future), but any update JUST
            // done didn't cause the page lsn to change .. yet.
            //
            // 2) we pinned a dirty page, did not update it, and are
            // unpinning it;  the control block says it's  dirty.
            // This can happen if after redo_pass  we are doing an undo
            // and pin a still-dirty-but-unlogged page.  So in order to
            // prevent this, I have inserted force_all in restart.cpp between
            // the redo_pass and the undo_pass. It should make this case
            // never happen, leaving the assert below valid again.
            // 
            // 3) Page cleaners cleared the dirty bit and rec_lsn of
            // an SH-latched page before writing it out. In this case
            // safe_rec_lsn() != curr_rec_lsn() and the former should
            // be used. However, either way the relationship with the
            // page lsn remains valid.
            //
            // The recovery code is about to see that it gets changed
            // and that the rec_lsn gets "repaired".
            if (( 
                (p->safe_rec_lsn() <= p->frame()->lsn) 
                ||
                ((p->get_storeflags() & smlevel_0::st_tmp) 
                     == smlevel_0::st_tmp) 
                ||
                smlevel_0::in_recovery_redo()
                ) == false) {
                // Print some useful info before we croak on the
                // assert directly below this.
                cerr 
                    << " frame " << (void *)(p->frame()) << endl
                    << " pid " << p->pid() << endl
                    << " curr_rec_lsn " << p->curr_rec_lsn() << endl
                    << " old_rec_lsn " << p->old_rec_lsn() << endl
                    << " safe_rec_lsn " << p->safe_rec_lsn() << endl
                    << " frame lsn " << p->frame()->lsn << endl
                    << " (p->rec_lsn <= p->frame->lsn) "
                           << int(p->safe_rec_lsn() <= p->frame()->lsn) << endl
                    << " p->frame->store_flags & smlevel_0::st_tmp "
                        << int(p->get_storeflags() & smlevel_0::st_tmp) << endl
                    << " smlevel_0::in_recovery_redo() " <<
                        int(smlevel_0::in_recovery_redo()) << endl
                    << endl;
                std::list<int32_t>& dependencies = p->write_order_dependencies();
                if (!dependencies.empty()) {
                    cerr << "    dependencies:";
                    for (std::list<int32_t>::const_iterator dep_it = dependencies.begin(); dep_it != dependencies.end(); ++dep_it) {
                        cerr << "[" << *dep_it << "],";
                    }
                    cerr << endl;
                }
                
            }
            w_assert0 ( 
                (p->safe_rec_lsn() <= p->frame()->lsn) ||
                ((p->get_storeflags() & smlevel_0::st_tmp) 
                     == smlevel_0::st_tmp) ||
                smlevel_0::in_recovery_redo()
                );
        }
        else {
            /* Clean pages should not have a rec_lsn.
             * GNATS 64. See also "GNATS 64 in bf.cpp"
             * It's possible that we missed
             * the pin count turning to 1 and so here we'll get a
             * 2nd chance. There's still a race, though, isn't there?
             * Pin count might not be 1 yet and we don't have a chance
             * to clear the rec_lsn.
             * And we can't simply clear the lsn because if there's
             * a double-latch by the same thread, and the other thread
             * is about to make an update, this would create problems.
             */
             
             // FRJ: fixed a race here by ensuring that page latches are
             // released only after unpinning, and also by having
             // bf_m::unfix use latch information rather than pin
             // counts to decide whether to clear the rec_lsn.
             
             w_assert1(! p->curr_rec_lsn().valid() );
        }
    }
          
    w_assert1(p->latch.held_by_me()); 
    p->check();

    DBGTHRD(<<"unpin frame " << ::hex << (unsigned long) (p->frame()) << ::dec
            << " frame's pid " << p->frame()->pid);

    /* FRJ: CLEAN_REC_LSN_RACE arises (at least partly) because we
       would unlatch pages before unpinning. 
       However, we can avoid this by unpinning and then unlatching the page -- 
       nobody else will touch the page until they can both pin and latch it.
       See also comments in bf.cpp; search for CLEAN_REC_LSN_RACE
     */
    p->unpin_frame(); // atomic decrement
    (void) p->latch.latch_release(); // PROTOCOL

    // prevent future use of p
    p = 0;
}


/*********************************************************************
 *
 *  bf_core_m::_remove(p)
 *
 *  Remove frame "p" from hash table. Insert into unused list.
 *  Called from ::remove while _bfc_mutex is held.
 *
 *********************************************************************/
rc_t 
bf_core_m::_remove(bfcb_t*& p)
{
    w_assert9(p - _buftab >= 0 && p - _buftab < _num_bufs);
    w_assert2(_in_htab(p));

    w_assert2(p->latch.is_mine());
    w_assert2(p->latch.latch_cnt() == 1);
    p->check();

    if (p->is_hot())  {
        // someone's waiting for the page.
        INC_TSTAT(bf_discarded_hot);
#if W_DEBUG_LEVEL > 2
        // W_FATAL(fcINTERNAL);
        // Not sure what to do here. It seems it's a legit situation.
        cerr << "is_hot.  latch=" << &(p->latch) << endl;
        dumpthreads();
#endif 
    }
    
    /* have to make sure it's not being cleaned. Because we have an
       EX-latch we only have to worry about a page write that was
       already in-progress before we latched. Acquiring the page write
       mutex for this pid will guarantee that any such in-progress
       write has completed (because the writer will clear the
       old_rec_lsn before releasing the mutex)
    */
    if(p->old_rec_lsn().valid()) {
        CRITICAL_SECTION(cs, page_write_mutex_t::locate(p->pid()));
        w_assert0(!p->old_rec_lsn().valid());
    }
    
    // fail if the page is pinned more than once, retry if the page
    // got moved while we were acquiring the lock
    {
        CRITICAL_SECTION(cs, transit_bucket_t::get(p->pid())._tb_mutex); // PROTOCOL
        static int const PATIENCE = 10;
        int i;
        for(i=0; i < PATIENCE; i++) {
            htab::bucket &b = _htab->_table[p->hash()];
            // Note: we don't have the bucket locked, so this
            // could yield an ephemeral value  
            if(b.get_frame(p->pid()) != p)
                continue;
            w_assert2(b._lock.is_mine()==false);

            // lock the bucket and check again...
            CRITICAL_SECTION(bcs, b._lock); // PROTOCOL
            w_assert2(b._lock.is_mine());

            if(b.get_frame(p->pid()) != p)
                continue; // cuckoo!

            p->latch.latch_release(); // PROTOCOL
            p->unpin_frame(); // adjust pin count; so the 
            // remove can succeed; we still hold the lock

            w_assert1(b._lock.is_mine());
            if(!_htab->remove(p)) {
                // W_FATAL(fcINTERNAL);
                // This had better be the reason it failed:
                // someone jumped in after we released the latch
                // and grabbed it. This is a hot page...
                // A test script that tripped this case is
                // file.perf3, with a large buffer pool, in which
                // case a file is being destroyed while the bf cleaner
                // is trying to write out its dirty pages. The
                // destroying thread gets here, trying to discard
                // the pages.  
                // If the page doesn't get removed from the buffer 
                // pool, it's not the end of the world; it'll get
                // thrown out eventually.
                w_assert3(p->pin_cnt() > 0);
                return RC(eHOTPAGE);
            }
            
            p->clear_bfcb();
            break;
        }
        if(i == PATIENCE) {
            // oh well...
            p->latch.latch_release(); // PROTOCOL
            p->unpin_frame();  // adjust pin count
            return RC(eFRAMENOTFOUND);
        }
    }
    _unused.release(p); // put on free list

    p = NULL;

    return RCOK;
}

bool bf_core_m::can_replace(bfcb_t* p, int rounds) 
{
    w_assert0(p != NULL); // had better not call with null p
    /*
     * On the first partial-round, consider only clean pages.
     * After that, dirty ones are ok.
     *
     * FRJ: These used to test whether the latch was locked. However,
     * pin_cnt supercedes the latch -- anyone who holds the latch has
     * incremented pin_cnt (as well as anybody trying to acquire the
     * latch).
     * NEH: well, that's not strictly true, however, it's true for
     * frames that are in the hash table,and we call this only for
     * frames in the hash table.
     */
    bool found =false;
    if(!p->pin_cnt() && !p->old_rec_lsn().valid()) {
        switch(rounds) {
            case 3:
                // all pages, but we still check dependency below
                found=true; 
                break;
            case 2:
                // Like 1 but will accept dirty pages
                if(!p->refbit() && !p->hotbit()) {
                    found=true; 
                }
                break;

            case 1:
                // Unpinned, not hot, not dirty, no refbit 
                if( !p->dirty() && !p->refbit() && !p->hotbit() ) {
                    found=true; 
                }
                break;

            case 0: 
                // nothing is satisfactory. Not used at the moment.
                w_assert0(0); 
                found = false; 
                break;

            default:
                // Anything unpinned is ok. 
                    found=true; 
            break;
        }
    }

    // make sure there is a free in-transit-out slot
    if(found && p->dirty()) {
        // Note: transit bucket is not in any way locked.
        // Declaring this available here does not guarantee it 
        // to the caller, (but that caller turns out to
        // be replacment() only), and replacement holds a lock
        // on this bucket when it calls us.
        // Everywhere else that transit_bucket_t::get is used,
        // it is locked.
        transit_bucket_t* volatile buck = &transit_bucket_t::get(p->pid());

        found = buck->empty();
        if(!found) {
            INC_TSTAT(bf_no_transit_bucket);
        }
    }
    
    // FRJ: only true if we've got the bucket locked, which me might not.
    // NEH: In one call we have it locked; in the other, no.
    // w_assert1(found==false || p->latch.is_latched()==false);


    // a dirty page that has write order dependency shouldn't be evicted by clock algorithm
    // it messes up all dependency stuff (and impossible to also write out dependency because
    // they might have uncommitted changes). Only cleaners can write them out.
    if(found && p->dirty() && rounds < 4) { //if rounds>=4, we allow even dependent ones (very emergent).
        if (!p->write_order_dependencies().empty()) {
            return false;
        }
    }

    return found;
}

/*********************************************************************
 *
 *  bf_core_m::_replacement()
 *
 *  Find a replacement resource.  Called from outside to provide a
 *  frame for grab() to fill. If the replacement comes from the
 *  freelist, it will have an invalid old_pid. Otherwise, it is marked
 *  as in-transit-out and must be flushed if dirty.
 *
 *  When this method is finished, the caller does not hold any
 *  synchronization lock (except an EX latch on the frame); 
 *  what it returns is either taken from the
 *  _unused list (in which case, it's no longer accessible to another
 *  thread), or it's taken  out of the hash table and put into the
 *  in-transit-out table.  
 *  Preference is given to non-dirty pages.
 *
 * The frame is not pinned.  Just EX-latched.
 *
 *********************************************************************/
bfcb_t* 
bf_core_m::replacement()
{
    /*
      Freelist?
    */
    bfcb_t* p = _unused.take(); // get a free frame

    if(p) {
        w_assert2(p->frame() != 0);
        p->clr_old_pid();
        p->mark_clean();
        w_assert1 (! _in_htab(p)); // Not in hash table
        INC_TSTAT(bf_replaced_unused);
        
        // Get the latch just so that we are consistently
        // returning with the EX latch; this allows all the
        // threads --namely those in write_out(from clean_segment, 
        // _scan, and update_rec_lsn) or replace_out (us) --
        // to use the same protocol: latch then acquire 
        // page_writer_mutex.
        w_rc_t rc = p->latch.latch_acquire(LATCH_EX, WAIT_IMMEDIATE); 
        w_assert0(!rc.is_error());
        return p;
    }
    
    /*
     *  Use the clock algorithm to find replacement resource.
     *
     * Because replacement is expensive, but need not be serialized,
     * we release the clock hand whenever we think we have a
     * candidate. If we were wrong, we retry from the top, 
     * reacquiring the clock hand and all.
     *
     * patience: equiv of 4 rounds == limit of # tries before we give up
     * looked_at: total number of tries so far
     * rounds: number of passes through the buffer pool so far 
     *    This is passed in to can_replace and 
     *    determines behavior of can_replace, which is why we keep 
     *    track of it.
     *    1: clean pages only
     *    2: dirty pages considered (no dependent ones)
     *    3: dirty pages considered  (dependent ones if they are already written out)
     *    4 and larger: anything not pinned
     * next_round: #tries that indicates we've hit the next round
     *
     * We start with the clock hand, hence the counts(looked_at) 
     * kept separately from the index (i, starts with hand)
     */
    int looked_at = 0;
    int patience = 4*_num_bufs;
    int next_round = _num_bufs;
    int rounds = 1;
    while(1) {
        { // critical section
            CRITICAL_SECTION(cs, _bfc_mutex); // PROTOCOL
            int start = _hand;
            int i;
            for (i = start; ++looked_at < patience; i++)  {
                
                if (i == _num_bufs) {
                    i = 0;
                }
                if( looked_at == next_round) {
                    rounds++;
                    next_round += _num_bufs;
                }
                
                /*
                 *  p is current entry.
                 */
                p = _buftab + i;
                if (! _in_htab(p))  {
                    // p could be in transit
                    continue;
                }
                w_assert3(p->refbit() >= 0);
                DBG(<<"rounds: " << rounds
                    << " dirty:" << p->dirty()
                    << " refbit:" << p->refbit()
                    << " hotbit:" << p->hotbit()
                    << " pin_cnt:" << p->pin_cnt()
                    << " locked:" << p->latch.is_latched()
                    );
                /*
                 * On the first partial-round, consider only clean pages.
                 * After that, dirty ones are ok.
                 */
                if(can_replace(p, rounds)) {
                    /*
                     *  Update clock hand and release the mutex.
                     *  We'll reacquire if this doesn't work...
                     */
                    _hand = (i+1 == _num_bufs) ? 0 : i+1;        
                    break;
                }
                
                /*
                 *  Unsuccessful. Decrement ref count. Try next entry.
                 *  Note that this is racy.  It's an approximation.
                 */
                if (p->refbit()>0) p->decr_refbit();
                w_assert3(p->refbit() >= 0);
            }
            if(looked_at >= patience) {
                cerr << "bf_core_m: cannot find free resource" << endl;
                cerr << *this;
                /*
                 * W_FATAL(fcFULL);
                 */
                return (bfcb_t*)0; 
            }
        } // end critical section

        /* 
         * Found one!
         *
         * Now we have to lock the htbucket down and check for real.
         *
         * It may be that the frame's page has changed, or
         * that it was removed from the htable, or that its
         * status changed to something unacceptable.
         *
         * Note 1: once we hold the bucket lock, the page
         * cannot change from unpinned to pinned. So, we
         * remove it immediately while we still hold the
         * bucket lock. If a thread tries to grab() this
         * frame, it will block on the main mutex (which we
         * hold) until we've had a chance to put this frame in
         * its new home.
         *
         * Note 2: the page could get moved by a cuckoo hash
         * insert. if that happens while we're finding the
         * page, the hash may point to the wrong bucket. While
         * we *could* deal with this (by retrying to locate
         * this page) we don't currently bother to do so. 
         * Instead we just look for another victim.
         */
            
        int idx = p->hash();
        bfpid_t pid = p->pid();

        transit_bucket_t* volatile tb = &transit_bucket_t::get(pid);
        {
        CRITICAL_SECTION(tcs, tb->_tb_mutex); // PROTOCOL

        htab::bucket &b = _htab->_table[idx];
        
        w_assert2(b._lock.is_mine()==false);
        {
            CRITICAL_SECTION(bcs, b._lock); // PROTOCOL
            w_assert2(b._lock.is_mine());

            w_rc_t rc = p->latch.latch_acquire(LATCH_EX, WAIT_IMMEDIATE); 
            // otherwise who knows what other threads are doing...
            if(!rc.is_error()) 
            {
                // I don't like this - other threads hold onto the
                // page mutex for a long time (_write_out, _replace_out).
                //
                // Rather than preventing the replacement() from happening,
                // we should cope with the disappearance of the page.
                //
                // We try the page mutex.

                pthread_mutex_t* page_write_mutex = 
                    page_write_mutex_t::locate(pid);
                if(pthread_mutex_trylock(page_write_mutex) == EBUSY) {
                    page_write_mutex = NULL;
                }

                // hold onto it long enough to do the following check
                // If the pointer is null, 
                // it means we don't have the mutex, and auto_release does nothing.
                auto_release_t<pthread_mutex_t> cs(page_write_mutex);

                if(page_write_mutex) // we hold the mutex...
                {
                    // In event of race, the pid in the frame could have changed
                    if(b.get_frame(pid) == p && // We have the htab bucket lock. 
                        p->pid() == pid && 
                        _in_htab(p) && // could have been removed altogether
                        can_replace(p, rounds) && 
                        _htab->remove(p))  // changes p->hash_func
                    {
                        w_assert2(p->hash() == idx);
                        w_assert2(!_in_htab(p));
                        // Note : we have both the transit-bucket lock and
                        // the htab bucket lock here.
                        p->set_old_pid(); // now old_pid_valid() is true.
                        if(p->dirty())  {
                            tb->make_in_transit_out(p->pid());
                        }
                        w_assert1(p->frame() != 0);
                        // In this case, the frame is latched
                        w_assert1(p->latch.is_mine() == true);
                        return p;
                    }
                }
                p->latch.latch_release();
            } 
            // We didn't acquire the latch if rc.is_error
            // so let's assert here
            w_assert1(p->latch.is_mine() == false);
        } // end critical section
        
        } // end critical section
        // drat! try again
    }
}


#if W_DEBUG_LEVEL > 2
/*********************************************************************
 *
 *  bf_core_m::audit()
 *
 *  Check invarients for integrity.
 *
 *********************************************************************/
int 
bf_core_m::audit() const
{
    int total_locks=0 ;

    for (int i = 0; i < _num_bufs; i++)  {
        bfcb_t* p = _buftab + i;
        if(p->hash_func() == htab::HASH_COUNT)
            continue; // not in the table

        int idx = p->hash();
        htab::bucket &b = _htab->_table[idx];

        w_assert2(b._lock.is_mine()==false);
        CRITICAL_SECTION(bcs, b._lock); // PROTOCOL
        w_assert2(b._lock.is_mine());

        bool found=false;
        for(int j=0; j < b._count; j++) {
            if (b._slots[j] == p)  {
                found = true;
                w_assert2(p->hash() == idx);
                w_assert2(_in_htab(p));
                if(p->latch.is_latched()) { 
                     w_assert2(p->latch.latch_cnt()>0);
                }
                total_locks += p->latch.latch_cnt() ;
            }
        }
        if(!found) {
            // the frame moved or got evicted...
            // nobody calls this function so I don't know whether
            // that's a bad thing or not
        }
    }
    DBG(<< "end of bf_core_m::audit");
    return total_locks;
}
#endif


/*********************************************************************
 *
 *  bf_core_m::dump(ostream, debugging)
 *
 *  Dump content to ostream. If "debugging" is true, print
 *  synchronization info as well.
 *
 *********************************************************************/
void
bf_core_m::dump(ostream &o, bool /*debugging*/)const
{
    o << "bf_core_m:"
      << ' ' << _num_bufs << " frames"
      << endl;

    o   << "frame#"
    << '\t' << "pid" << '\t'
    << '\t' << "dirty?" 
    << '\t' << "rec_lsn" 
    << '\t' << "pin_cnt" 
    << '\t' << "l_mode" 
    << '\t' << "l_cnt" 
    << '\t' << "l_hot" 
    << '\t' << "refbit" 
    << '\t' << "l_id" 
    << endl << flush;

    int n = 0;
    int t = 0;
    int pincnt = 0;
    for (int i = 0; i < _num_bufs; i++)  {
        bfcb_t* p = _buftab + i;
        pincnt += p->pin_cnt();
        if (_in_htab(p))  {
            n++;
            o << i << '\t';
            p->print_frame(o, true);
        }
    }

    o << "number of frames in the HASH TABLE: " << n << endl;
    o << "number of frames in TRANSIT: " << t << endl;
    o << endl << flush;
}

ostream &
operator<<(ostream& out, const bf_core_m& mgr)
{
    mgr.dump(out, 0);
    return out;
}

#include <sm_vtable_enum.h>
#ifdef __GNUG__
template class vtable_func<bfcb_t>;
#endif /* __GNUG__ */

enum {
    /* for buffer pool */
    bp_pid_attr,
    bp_pin_cnt_attr,
    bp_mode_attr,
    bp_dirty_attr,
    bp_hot_attr,

    /* last number! */
    bp_last 
};

const char *bp_vtable_attr_names[] =
{
    "Pid",
    "Pin count",
    "Pin mode",
    "Dirty",
    "Hot"
};

static vtable_names_init_t names_init(bp_last, bp_vtable_attr_names);

void    
bfcb_t::vtable_collect(vtable_row_t &t)
{
    if(pid().valid()) {
        {
            // circumvent with const-ness
            // attribute. Needs special handling b/c we don't have
            // a set_pid() method for this.
            w_ostrstream o;
            o << pid() <<  ends;
            t.set_string(bp_pid_attr, o.c_str());
        }


        t.set_int(bp_pin_cnt_attr, pin_cnt());
        t.set_string(bp_mode_attr, latch_t::latch_mode_str[latch.mode()] );
        t.set_string(bp_dirty_attr, 
                (const char *) (dirty()?"true":"false") );
        t.set_string(bp_hot_attr, 
                (const char *)(is_hot()?"true":"false"));
    }
}

void    
bfcb_t::vtable_collect_names(vtable_row_t &t)
{
    names_init.collect_names(t);
}


int
bf_core_m::collect(vtable_t &v, bool names_too)
{

    // _num_bufs is larger than needed
    // but we have no quick way to find out
    // how many are empty/non-empty frames, so this
    // is ok
    // num_bufs: number of rows.
    // bp_last: number of attributes
    // names_init.max_size: maximum length of attribute
    int n = _num_bufs;
    if(names_too) n++;
    if(v.init(n, bp_last, names_init.max_size())) return -1;
    vtable_func<bfcb_t> f(v);

    if(names_too) f.insert_names();

    for (int i = 0; i < _num_bufs; i++)  {
        if(_buftab[i].pid().valid()) {
            f(_buftab[i]);
        }
    }
    return 0; // no error

}

void                        
bf_core_m::htab_stats(bf_htab_stats_t &out) const
{
    if(_htab) _htab->stats(out);
    *(&out.bf_htab_entries) = _num_bufs;
}

void bfcb_t::clear_bfcb() 
{
    _pid = lpid_t::null;
    _old_pid = lpid_t::null;
    _old_pid_valid = false;
    _hotbit = 0;
    _refbit = 0;
    mark_clean();
    w_assert1(pin_cnt() == 0);
    w_assert3(latch.num_holders() <= 1);
}

#endif /* BF_CORE_C */

