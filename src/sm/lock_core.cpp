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

 $Id: lock_core.cpp,v 1.123 2010/10/27 17:04:23 nhall Exp $

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

#define LOCK_CORE_C
#define SM_SOURCE

#ifdef __GNUG__
#pragma implementation "lock_s.h"
#pragma implementation "lock_x.h"
#pragma implementation "lock_core.h"
#endif

#define USE_BLOCK_ALLOC_FOR_LOCK_STRUCTS 1

#if USE_BLOCK_ALLOC_FOR_LOCK_STRUCTS 
#include "block_alloc.h"
#endif

#include "st_error_enum_gen.h"
#include "sm_int_1.h"
#include "kvl_t.h"
#include "lock_s.h"
#include "lock_x.h"
#include "lock_core.h"
#include "tls.h"

#include <w_strstream.h>

#ifdef EXPLICIT_TEMPLATE
template class w_list_const_i<lock_request_t, queue_based_lock_t>;
template class w_auto_delete_array_t<unsigned>;
#endif

#if USE_BLOCK_ALLOC_FOR_LOCK_STRUCTS 
DECLARE_TLS(block_alloc<lock_head_t>, lockHeadPool);
typedef object_cache<lock_request_t, object_cache_initializing_factory<lock_request_t> > lock_request_cache_t;
DECLARE_TLS(lock_request_cache_t, lock_request_pool);
#endif

/*********************************************************************
 *
 *  parent_mode[i] is the lock mode of parent of i
 *        e.g. parent_mode[EX] is IX.
 *        e.g. parent_mode[UD] is IX.
 *        e.g. parent_mode[SIX] is IX.
 *        e.g. parent_mode[SH] is IS.
 *        e.g. parent_mode[IX] is IX.
 *        e.g. parent_mode[IS] is IS.
 *        e.g. parent_mode[NL] is NL.
 *
 *********************************************************************/
const lock_base_t::lmode_t lock_m::parent_mode[NUM_MODES] = {
//  NL, IS, IX, SH, SIX,UD, EX
    NL, IS, IX, IS, IX, IX, IX,
//  NS, NU, NX, SN, SU, SX, UN, US, UX, XN, XS, XU
    IS, IX, IX, IS, IX, IX, IX, IX, IX, IX, IX, IX
};


/*********************************************************************
 *
 *   duration_str[i]:         string describing duration i
 *   mode_str[i]:        string describing lock mode i
 *
 *********************************************************************/
const char* const lock_base_t::duration_str[t_num_durations] = {
    "INSTANT", "SHORT", "MEDIUM", "LONG", "VERY_LONG"
};

const char* const lock_base_t::mode_str[NUM_MODES] = {
    "NL", "IS", "IX", "SH", "SIX", "UD", "EX",
    "NS", "NU", "NX", "SN", "SU", "SX", "UN", "US", "UX", "XN", "XS", "XU"
};


/*********************************************************************
 *
 *  Compatibility Table (diff xact)
 *        Page 408, Table 7.11, "Transaction Processing" by Gray & Reuter
 *
 *  compat[r][g] returns bool value if a requested mode r is compatible
 *        with a granted mode g.
 *
 *********************************************************************/
const bool T = true; // to make following easier to read
const bool F = false;

// NOTE Intent-lock vs. key range lock such as "IS - NX" are all T because it never happens
const bool lock_base_t::compat
[NUM_MODES] /* request mode */
[NUM_MODES] /* granted mode */
= {
/*req*/  //NL  IS  IX  SH SIX  UD  EX    NS  NU  NX  SN  SU  SX  UN  US  UX  XN  XS  XU (granted)
/*NL*/    { T,  T,  T,  T,  T,  T,  T,    T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T }, 
/*IS*/    { T,  T,  T,  T,  T,  F,  F,    T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T },
/*IX*/    { T,  T,  T,  F,  F,  F,  F,    T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T },
/*SH*/    { T,  T,  F,  T,  F,  F,  F,    T,  F,  F,  T,  F,  F,  F,  F,  F,  F,  F,  F },
/*SIX*/   { T,  T,  F,  F,  F,  F,  F,    T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T,  T },
/*UD*/    { T,  F,  F,  T,  F,  F,  F,    T,  F,  F,  T,  F,  F,  F,  F,  F,  F,  F,  F },
/*EX*/    { T,  F,  F,  F,  F,  F,  F,    F,  F,  F,  F,  F,  F,  F,  F,  F,  F,  F,  F },

/*NS*/    { T,  T,  T,  T,  T,  F,  F,    T,  F,  F,  T,  F,  F,  T,  T,  F,  T,  T,  F },
/*NU*/    { T,  T,  T,  T,  T,  F,  F,    T,  F,  F,  T,  F,  F,  T,  T,  F,  T,  F,  F },
/*NX*/    { T,  T,  T,  F,  T,  F,  F,    F,  F,  F,  T,  F,  F,  T,  F,  F,  T,  F,  F },
/*SN*/    { T,  T,  T,  T,  T,  F,  F,    T,  T,  T,  T,  T,  T,  F,  F,  F,  F,  F,  F },
/*SU*/    { T,  T,  T,  T,  T,  F,  F,    T,  F,  F,  T,  F,  F,  F,  F,  F,  F,  F,  F },
/*SX*/    { T,  T,  T,  F,  T,  F,  F,    F,  F,  F,  T,  F,  F,  F,  F,  F,  F,  F,  F },
/*UN*/    { T,  T,  T,  T,  T,  F,  F,    T,  T,  T,  T,  T,  T,  F,  F,  F,  F,  F,  F },
/*US*/    { T,  T,  T,  T,  T,  F,  F,    T,  F,  F,  T,  F,  F,  F,  F,  F,  F,  F,  F },
/*UX*/    { T,  T,  T,  F,  T,  F,  F,    F,  F,  F,  T,  F,  F,  F,  F,  F,  F,  F,  F },
/*XN*/    { T,  T,  T,  F,  T,  F,  F,    T,  T,  T,  F,  F,  F,  F,  F,  F,  F,  F,  F },
/*XS*/    { T,  T,  T,  F,  T,  F,  F,    T,  F,  F,  F,  F,  F,  F,  F,  F,  F,  F,  F },
/*XU*/    { T,  T,  T,  F,  T,  F,  F,    T,  F,  F,  F,  F,  F,  F,  F,  F,  F,  F,  F }
};

/*********************************************************************
 *
 *  Supremum Table (Page 467, Figure 8.6)
 *  (called Lock Conversion Matrix there)
 *
 *        supr[i][j] returns the supremum of two lock modes i, j.
 *
 *********************************************************************/
// NOTE Intent-lock + key range lock such as "IS + NX" are all "LL" (invalid) because it never happens
const lock_base_t::lmode_t lock_base_t::supr[NUM_MODES][NUM_MODES] = {
        //NL    IS    IX    SH    SIX   UD    EX     NS  NU  NX  SN  SU  SX  UN  US  UX  XN  XS  XU
/*NL*/  { NL,   IS,   IX,   SH,   SIX,  UD,   EX,    NS, NU, NX, SN, SU, SX, UN, US, UX, XN, XS, XU },
/*IS*/  { IS,   IS,   IX,   SH,   SIX,  UD,   EX,    LL, LL, LL, LL, LL, LL, LL, LL, LL, LL, LL, LL },
/*IX*/  { IX,   IX,   IX,   SIX,  SIX,  EX,   EX,    LL, LL, LL, LL, LL, LL, LL, LL, LL, LL, LL, LL },
/*SH*/  { SH,   SH,   SIX,  SH,   SIX,  UD,   EX,    SH, SU, SX, SH, SU, SX, US, US, UX, XS, XS, XU },
/*SIX*/ { SIX,  SIX,  SIX,  SIX,  SIX,  SIX,  EX,    LL, LL, LL, LL, LL, LL, LL, LL, LL, LL, LL, LL },
/*UD*/  { UD,   UD,   EX,   UD,   SIX,  UD,   EX,    UD, UD, UX, UD, UD, UX, UD, UD, UX, XU, XU, XU },
/*EX*/  { EX,   EX,   EX,   EX,   EX,   EX,   EX,    EX, EX, EX, EX, EX, EX, EX, EX, EX, EX, EX, EX },

/*NS*/  { NS,   LL,   LL,   SH,   LL,   US,   EX,    NS, NU, NX, SH, SU, SX, US, US, UX, XS, XS, XU },
/*NU*/  { NU,   LL,   LL,   SU,   LL,   UD,   EX,    NU, NU, NX, SU, SU, SX, UD, UD, UX, XU, XU, XU },
/*NX*/  { NX,   LL,   LL,   SX,   LL,   UX,   EX,    NX, NX, NX, SX, SX, SX, UX, UX, UX, EX, EX, EX },
/*SN*/  { SN,   LL,   LL,   SH,   LL,   UD,   EX,    SH, SU, SX, SN, SU, SX, UN, US, UX, XN, XS, XU },
/*SU*/  { SU,   LL,   LL,   SU,   LL,   UD,   EX,    SU, SU, SX, SU, SU, SX, UD, UD, UX, XU, XU, XU },
/*SX*/  { SX,   LL,   LL,   SX,   LL,   UX,   EX,    SX, SX, SX, SX, SX, SX, UX, UX, UX, EX, EX, EX },
/*UN*/  { UN,   LL,   LL,   US,   LL,   UD,   EX,    US, UD, UX, UN, UD, UX, UN, US, UX, XN, XS, XU },
/*US*/  { US,   LL,   LL,   US,   LL,   UD,   EX,    US, UD, UX, US, UD, UX, US, US, UX, XS, XS, XU },
/*UX*/  { UX,   LL,   LL,   UX,   LL,   UX,   EX,    UX, UX, UX, UX, UX, UX, UX, UX, UX, EX, EX, EX },
/*XN*/  { XN,   LL,   LL,   XS,   LL,   XU,   EX,    XS, XU, EX, XN, XU, EX, XN, XS, EX, XN, XS, XU },
/*XS*/  { XS,   LL,   LL,   XS,   LL,   XU,   EX,    XS, XU, EX, XS, XU, EX, XS, XS, EX, XS, XS, XU },
/*XU*/  { XU,   LL,   LL,   XU,   LL,   XU,   EX,    XU, XU, EX, XU, XU, EX, XU, XU, EX, XU, XU, XU },
};

LockCoreFunc::~LockCoreFunc()
{
}


/* Lock table hash table bucket.
 * Lock table's hash table is _htab, a list of bucket_t's,
 * each bucket contains a mutex that protects the list
 * connected through the lock_head_t's "chain" links.
 */
class bucket_t {
public:

    DEF_LOCK_X_TYPE(3);              // declare/define lock_x type
    // lock_x: see lock_x.h
    lock_x                        mutex;   // serialize access to lock_info_t
    // lock_x's mutex is of type queue_based_lock_t
    lock_core_m::chain_list_t     chain;

    NORET                         bucket_t() :
                                    chain(W_LIST_ARG(lock_head_t, chain),
                                            &mutex.mutex) {
                                  }

    private:
    // disabled
    NORET                         bucket_t(const bucket_t&);
    bucket_t&                     operator=(const bucket_t&);
};

#include <sstream>

/*********************************************************************
 *
 *  xct_lock_info_t::xct_lock_info_t()
 *
 *********************************************************************/
xct_lock_info_t::xct_lock_info_t()
:
    _wait_request(NULL),
    _noblock(false)
{
    for (int i = 0; i < t_num_durations; i++)  {
        my_req_list[i].set_offset(W_LIST_ARG(lock_request_t, xlink));
    }
    _wait_map_obsolete = false;
    clear_wait_map();
}

// allows reuse rather than free/malloc of the structure
xct_lock_info_t* xct_lock_info_t::reset_for_reuse() 
{
#if W_DEBUG_LEVEL>0
    // make sure the lock lists are empty
    for(int i=0; i < t_num_durations; i++) {
        w_assert1(my_req_list[i].is_empty());
    }
#endif // W_DEBUG_LEVEL>0
    new (this) xct_lock_info_t;
    
    return this;
}
                                                    

/*********************************************************************
 *
 *  xct_lock_info_t::set_nonblocking()
 *
 *  used in xct_t::force_nonblocking, called in chkpt when an
 *  xct is long-running and is preventing the scavenging of log
 *  files.
 *
 *  When set, is_nonblocking() returns true; this is checked
 *  after the thread that's waiting gets awakened, and if found
 *  to be true, returns eDEADLOCK. We count the times this
 *  happens with a stat.
 *  This is how the log-is-full condition kills a blocked-on-lock
 *  transaction that's preventing scavenging.
 *
 *********************************************************************/
void xct_lock_info_t::set_nonblocking()
{
    w_assert2(!MUTEX_IS_MINE(lock_info_mutex));
    MUTEX_ACQUIRE(lock_info_mutex);
    _noblock = true;
    if(_wait_request) {
        // nothing should be able to go wrong...
        INC_TSTAT(log_full_old_xct);
        w_assert2(_wait_request->thread());
        W_COERCE(_wait_request->thread()->smthread_unblock(eDEADLOCK)); 
    }
    MUTEX_RELEASE(lock_info_mutex);
}

/*********************************************************************
 *
 *  xct_lock_info_t::~xct_lock_info_t()
 *
 *********************************************************************/
xct_lock_info_t::~xct_lock_info_t()
{
#if W_DEBUG_LEVEL > 2
    for (int i = 0; i < t_num_durations; i++)  {
        if (! my_req_list[i].is_empty() ) {
            DBGTHRD(<<"memory leak: non-empty list in xct_lock_info_t: " <<i);
        }
    }
#endif 
}


#define DEBUG_DEADLOCK 0
bool xct_lock_info_t::update_wait_map(atomic_thread_map_t const &other) 
{
    w_assert2(MUTEX_IS_MINE(lock_info_mutex));

    atomic_thread_map_t _wait_map_copy;
    w_assert1(_wait_map_copy.has_reader() == false); 
    w_assert1(_wait_map_copy.has_writer() == false); 

    _wait_map.lock_for_read();
    w_assert1(_wait_map.has_reader() == true); 
    _wait_map_copy = _wait_map;
    _wait_map.unlock_reader();

    w_assert1(_wait_map_copy.has_reader() == false); 
    w_assert1(_wait_map_copy.has_writer() == false); 

    // Work with a copy until I am ready to write it,
    // so that I'm never holding a lock on 2 maps at a time.
    // Since no other thread will try to update this,
    // this is ok.
    // I need these R/W locks only to see that the reads
    // and updates don't overlap.

    if(_wait_map_copy.is_empty()) {
        // Pick up the thread's fingerprint map
        _wait_map_copy = me()->get_fingerprint_map();
    }
    w_assert1(_wait_map_copy.has_reader() == false); 
    w_assert1(_wait_map_copy.has_writer() == false); 

#if DEBUG_DEADLOCK
    {
        other.lock_for_read();
        w_ostrstream s;
        s << "----------mine "
            << (void *)(this)
            << " is " ;
        _wait_map_copy.print(s) << endl;
        s << "----------other "
            << (void *)(&other)
            << " is " ;
        other.print(s) << endl;
        other.unlock_reader();
        fprintf(stderr,  "%s\n", s.c_str());
    }

    // This should be a tautology:
    w_assert1(_wait_map_copy.overlap(_wait_map_copy, _wait_map_copy));;
    w_assert1(_wait_map_copy.has_reader() == false); 
    w_assert1(_wait_map_copy.has_writer() == false); 
#endif

    atomic_thread_map_t new_map;
    other.lock_for_read();
    bool matches = _wait_map_copy.overlap(new_map, other);
    other.unlock_reader();
    w_assert1(new_map.has_reader() == false); 
    w_assert1(new_map.has_writer() == false); 
    w_assert1(_wait_map_copy.has_reader() == false); 
    w_assert1(_wait_map_copy.has_writer() == false); 
    
    // matches is the number of words in the other that contain.
    // If that is the same as the total # words in the map, then
    // the entire map matches. 
    if( !matches)
    {
        _wait_map.lock_for_write();
        w_assert1(_wait_map.has_writer() == true); 
        _wait_map = new_map;
#if DEBUG_DEADLOCK
        {
            w_ostrstream s;
            s << "----------mine "
                << (void *)(this)
                << " updated to " ;
            _wait_map.print(s) << endl;
            fprintf(stderr, 
            "%s ------ matches=%d\n", s.c_str(),
            matches
            );
        }
#endif
        _wait_map.unlock_writer();
        w_assert1(_wait_map.has_writer() == false); 
        // RACY w_assert1(_wait_map.has_reader() == false); 
        w_assert1(new_map.has_reader() == false); 
        w_assert1(new_map.has_writer() == false); 
        w_assert1(_wait_map_copy.has_reader() == false); 
        w_assert1(_wait_map_copy.has_writer() == false); 
        return true;
    }
    else
    {
        // deadlock!
#if W_DEBUG_LEVEL>2
        {
            w_ostrstream s;
            s << "----------other " << (void *)(&other);
            other.lock_for_read();
            other.print(s);
            other.unlock_reader();
            s << endl 
                << "----------mine  " << (void *)(this);
                _wait_map.print(s) << endl;

            fprintf(stderr, 
            "%s ------ matches=%d\n", s.c_str(),
            matches
            );
                
        }
#endif //W_DEBUG_LEVEL>2
        _wait_map.lock_for_write();
        w_assert1(_wait_map.has_writer() == true); 
        _wait_map.clear(); 
        _wait_map.unlock_writer();

        w_assert1(_wait_map.has_writer() == false); 
        // RACY w_assert1(_wait_map.has_reader() == false); 
        w_assert1(new_map.has_reader() == false); 
        w_assert1(new_map.has_writer() == false); 
        w_assert1(_wait_map_copy.has_reader() == false); 
        w_assert1(_wait_map_copy.has_writer() == false); 
        return false; 
    }
}

void xct_lock_info_t::done_waiting() {
    w_assert1(MUTEX_IS_MINE(lock_info_mutex));
    set_waiting_request(NULL);
    clear_wait_map();
}

// Obviously not mt-safe:
ostream &                        
xct_lock_info_t::dump_locks(ostream &out) const
{
    const lock_request_t         *req;
    const lock_head_t                 *lh;
    int                                                i;
    for(i=0; i< t_num_durations; i++) {
        out << "***Duration " << i <<endl;

        request_list_i   iter(my_req_list[i]); // obviously not mt-safe
        while ((req = iter.next())) {
            w_assert9(req->xd == xct());
            lh = req->get_lock_head();
            out << "Lock: " << lh->name 
                << " Mode: " << int(req->mode())
                << " State: " << int(req->status()) <<endl;
        }
    }
    return out;
}

/*********************************************************************
 *
 *  xct_lock_info_t output operator
 *
 *********************************************************************/
ostream &            
operator<<(ostream &o, const xct_lock_info_t &x)
{
        lock_request_t *waiting = x.waiting_request();
        if (waiting) {
                o << " wait: " << *waiting;
        }
        return o;
}

/*********************************************************************
 *
 *   lock_head_t::granted_mode_other(exclude)
 *
 *   Compute group mode of *other* members of the granted group.
 *   The lock request "exclude" is specifically neglected.
 *
 *********************************************************************/
inline lock_base_t::lmode_t
lock_head_t::granted_mode_other(const lock_request_t* exclude)
{
    w_assert9(!exclude || exclude->status() == lock_m::t_granted ||
                          exclude->status() == lock_m::t_converting);

    lock_base_t::lmode_t gmode = smlevel_0::NL;
    lock_head_t::safe_queue_iterator_t iter(*this); 

    const lock_request_t* f;
    while ((f = iter.next())) {
        if (f->status() == lock_m::t_waiting) break;

        // f is granted -- make sure it's got a mode that really
        // should have been granted, i.e., it's compatible with all the other
        // granted modes.  UD cases aren't symmetric, so we do both checks here:
        w_assert9(lock_m::compat[f->mode][gmode] || lock_m::compat[gmode][f->mode]);

        if (f != exclude) {
            gmode = lock_base_t::supr[f->mode()][gmode];
            w_assert1(gmode != lock_base_t::LL);
        }
    }

    return gmode;
}

// The assumption here is that any xct can have at most one
// lock request for a given lock (lock_head_t) in the table.
// does not check whether the request is reclaimed
lock_request_t*
lock_head_t::find_lock_request(const xct_lock_info_t* xdli)
{
    lock_head_t::safe_queue_iterator_t iter(*this); 
    lock_request_t* request;
    while ((request = iter.next()) && request->get_lock_info() != xdli) ;
    return request;
}

/*********************************************************************
 *
 *  lock_request_t::reset()
 *
 *  Clear a lock request's internal state (marking it not-used).
 *
 *********************************************************************/
inline void
lock_request_t::reset() {
    // very strict: if they gave it back they better really be done with it
    w_assert1(rlink.member_of() == 0);
    w_assert1(xlink.member_of() == 0);
    
    _state = lock_m::t_no_status;
    _mode = NL;
    _convert_mode = NL;
    _lock_info = 0;
    _thread = 0;
    _ref_count = 0;
    _duration = t_long; // have to assign it something...
    _xct_chain_len = NULL;
}

NORET
lock_request_t::lock_request_t()
{
    reset();
}

/*********************************************************************
 *
 *  lock_request_t::init(xct, mode, duration)
 *
 *  Set up a lock request for transaction "xct" with "mode" and on
 *  "duration".
 *
 *********************************************************************/
inline void
lock_request_t::init(xct_t* x, lmode_t m, duration_t d)
{
    w_assert1(!_lock_info);
    _mode = m;
    _lock_info = x->lock_info();
    _duration = d;
    _xct_chain_len = &(x->_xct_chain_len); // take address of it so that we can get up-to-date value
    
    // since d is unsigned, the >= comparison must be split to avoid
    // gcc warning.
    w_assert1((d == 0 || d > 0) && d < t_num_durations);

    // protected by the lock_info_mutex, acquired by caller
    x->lock_info()->my_req_list[d].push(this);
#if defined(W_TRACE) && W_DEBUG_LEVEL > 4
    {
        w_ostrstream o;
        o <<"size of lock_head_t " << sizeof(lock_head_t)  << endl;
        o <<"size of request_list_t " << sizeof(request_list_t)  << endl;
        fprintf(stderr, "%s", o.c_str());

        lock_head_t* lock = NULL;
        if(rlink.member_of() != 0) lock = this->get_lock_head();
        if(lock) {
            w_reset_strstream(o);
            o <<"pushed lock request " << this << endl;
            o <<"get_lock_head returns " << lock << endl;
            o <<"rlink.member_of() returns " << ((void*)rlink.member_of()) << endl;
            fprintf(stderr, "%s", o.c_str());

            w_reset_strstream(o);
            o << " " << lock->name << endl;
            fprintf(stderr, "%s", o.c_str());

            w_reset_strstream(o);
            o << " on my_req_list["<<int(_duration)<<"] : "  << endl;
            fprintf(stderr, "%s", o.c_str());

            w_reset_strstream(o);
            o << *this << endl;
            fprintf(stderr, "%s", o.c_str());
        } else {
            DBGTHRD(<<"pushed lock request " << this << " " 
                << "NO NAME"
                << " on my_req_list["<<int(_duration)<<"] : " << *this);
        }
    }
#endif 
}

// use this to compute highest prime # 
// less that requested hash table size. 
// Actually, it uses the highest prime less
// than the next power of 2 larger than the
// number requested.  Lowest allowable
// hash table option size is 64.

static const uint32_t primes[] = {
        /* 0x40, 64, 2**6 */ 61,
        /* 0x80, 128, 2**7  */ 127,
        /* 0x100, 256, 2**8 */ 251,
        /* 0x200, 512, 2**9 */ 509,
        /* 0x400, 1024, 2**10 */ 1021,
        /* 0x800, 2048, 2**11 */ 2039,
        /* 0x1000, 4096, 2**12 */ 4093,
        /* 0x2000, 8192, 2**13 */ 8191,
        /* 0x4000, 16384, 2**14 */ 16381,
        /* 0x8000, 32768, 2**15 */ 32749,
        /* 0x10000, 65536, 2**16 */ 65521,
        /* 0x20000, 131072, 2**17 */ 131071,
        /* 0x40000, 262144, 2**18 */ 262139,
        /* 0x80000, 524288, 2**19 */ 524287,
        /* 0x100000, 1048576, 2**20 */ 1048573,
        /* 0x200000, 2097152, 2**21 */ 2097143,
        /* 0x400000, 4194304, 2**22 */ 4194301,
        /* 0x800000, 8388608, 2**23   */ 8388593
};


inline lock_head_t*
lock_core_m::GetNewLockHeadFromPool(const lockid_t& name, lmode_t mode)
{
    //    fprintf(stderr, "Claiming lock head 0x%p\n", dlh);
    //assert(dlh->seed);
#if USE_BLOCK_ALLOC_FOR_LOCK_STRUCTS
    return new (*lockHeadPool) lock_head_t(name, mode);
#else
    return new lock_head_t(name, mode);
#endif
}


inline void
lock_core_m::FreeLockHeadToPool(lock_head_t* theLockHead)
{
    // Called while holding the bucket mutex
#if USE_BLOCK_ALLOC_FOR_LOCK_STRUCTS
    lockHeadPool->destroy_object(theLockHead);
#else
    delete theLockHead;
#endif
    //fprintf(stderr, "Releasing lock head 0x%p\n", theLockHead);
}



// Find lock head with a given lockid in the given chain.
// Helper for find_lock_head.
inline lock_head_t*
lock_core_m::_find_lock_head_in_chain(
    lock_core_m::chain_list_t& l, const lockid_t& n)
{
    // WE HOLD THE BUCKET MUTEX
    chain_list_i  iter(l);
    int i=0; 
    lock_head_t* lock=NULL;
    while ((lock = iter.next()) && lock->name != n) i++;

#if DEBUG_LOCK_HASH
    if((W_DEBUG_LEVEL>1) && i > 1) {
        uint32_t idx = _table_bucket(n.hash());

        // this junk is just for debugging bad hash functions:
        compute_lock_hash_numbers();
        fprintf(stderr, 
            "-+-+- Lock hash bucket %d has %d or more entries!\n", idx, i);
    }
#endif

    // pin it to protect the gap between releasing the bucket lock and
    // acquiring the lock->head_mutex
    if(lock)
        atomic_inc(lock->pin_cnt);
    return lock;
}

// Given a lock id, 
// find its lock_head_t  or create one and insert it in the chain.
//
// return it with the lock_head's mutex ACQUIRED
//
lock_head_t*
lock_core_m::find_lock_head(const lockid_t& n, bool create)
{
    uint32_t idx = _table_bucket(n.hash());

    ACQUIRE_BUCKET_MUTEX(idx);
    lock_head_t* lock = _find_lock_head_in_chain(_htab[idx].chain, n);
    if (!lock && create) {
        lock = GetNewLockHeadFromPool(n, NL);
        w_assert1(lock);
        // Set the value before we put it in the htab and release the 
        // bucket mutex.
        lock->pin_cnt = 1; // so there's something to decrement...
        _htab[idx].chain.push(lock);
    }
    RELEASE_BUCKET_MUTEX(idx);
    
    if(lock) {
        ASSERT_HEAD_MUTEX_NOT_MINE(lock);
        MUTEX_ACQUIRE(lock->head_mutex); // this is find_lock_head

        // unpin now that we hold the mutex
        atomic_dec(lock->pin_cnt);
        ASSERT_HEAD_MUTEX_IS_MINE(lock);
    }

#ifdef W_TRACE
    DBGTHRD(<< " find_lock_head returns " << lock);
    if(lock) { DBGTHRD(<< "=" << *lock); }
#endif

    return lock;
}

lock_core_m::lock_core_m(uint sz)
: 
  _htab(0),
  _htabsz(0),
  _requests_allocated(0)
{
    // find _htabsz, a power of 2 greater than sz
    int b=0; // count bits shifted
    for (_htabsz = 1; _htabsz < sz; _htabsz <<= 1) b++;

    w_assert1(!_htab); // just to check size

    w_assert1(_htabsz >= 0x40);
    w_assert1(b >= 6 && b <= 23);
    // if anyone wants a hash table bigger,
    // he's probably in trouble.

    // Now convert to a prime number in that range.
    // get highest prime for that numer:
    b -= 6;

    _htabsz = primes[b];

    _htab = new bucket_t[_htabsz];

    w_assert1(_htab);
    
    _lil_global_table = new lil_global_table;
    w_assert1(_lil_global_table);
    _lil_global_table->clear();    
}

lock_core_m::~lock_core_m()
{
    DBG( << " lock_core_m::~lock_core_m()" );

#if W_DEBUG_LEVEL > 2
    for (uint i = 0; i < _htabsz; i++)  {
        ACQUIRE_BUCKET_MUTEX(i);
        w_assert2( _htab[i].chain.is_empty() );
        RELEASE_BUCKET_MUTEX(i);
    }
#endif 

    delete[] _htab;
    _htab = NULL;
    
    delete _lil_global_table;
    _lil_global_table = NULL;
}


//
// NOTE: if "lock" is not 0, then the lock_head pointed should be protected
//    (via lock->head_mutex) before calling this function. Before the function
//    returns, the mutex on the lock is released
//
extern "C" void dumpthreads();

w_rc_t::errcode_t
lock_core_m::acquire_lock(
    xct_t*                 xd,
    const lockid_t&        name,
    lock_head_t*           lock,
    lock_request_t**       reqPtr,
    lmode_t                mode,
    lmode_t&               prev_mode,
    duration_t             duration,
    timeout_in_ms          timeout,
    lmode_t&               ret)
{
    FUNC(lock_core_m::acquire);
    lock_request_t*        req = reqPtr ? *reqPtr : 0;
    xct_lock_info_t*       the_xlinfo = xd->lock_info();

    w_assert2(MUTEX_IS_MINE(the_xlinfo->lock_info_mutex));
    if (lock) {
        ASSERT_HEAD_MUTEX_IS_MINE(lock);
    }
    w_assert2(xd == xct());

    ret = NL;

    if (!lock) {
        lock = find_lock_head(name, true); // create one if necessary
        DBGTHRD(<< " find_lock_head returns " << lock <<"=" << *lock);
    }

    if (!req) {
        req = lock->find_lock_request(the_xlinfo);
    }

    {
        /*
         *  See if this is a conversion request
         */
        if (req) {
            // conversion
            DBGTHRD(<< "conversion from req=" << *req);
            if(lock) { DBGTHRD(<< " check lock again " << lock <<"=" << *lock); }
            
            prev_mode = req->mode();

            if (req->status() == lock_m::t_waiting)  {
                // this should only occur if there are multiple threads per xct
                w_assert9(xd->num_threads()>1);
            }  else  {
                w_assert9(req->status() == lock_m::t_granted);

                mode = supr[mode][req->mode()];
                w_assert1(mode != LL);
        
                // optimization: case of re-request of an 
                // equivalent or lesser mode
                if (req->mode() == mode)  { 
                    INC_TSTAT(lock_extraneous_req_cnt);
                    if(lock) {DBGTHRD(<< " check lock again " << lock <<"=" << *lock); }
                    DBGTHRD(<<"goto success");
                    goto success; 
                }
                INC_TSTAT(lock_conversion_cnt);

                // first do the cheap test in case our upgrade 
                // can be granted outright
                lmode_t granted_mode_other = supr[lock->granted_mode][req->mode()];
                w_assert1(granted_mode_other != LL);
                if(!compat[mode][granted_mode_other]) {
                    // am I the one that would prevent the upgrade?
                    granted_mode_other = lock->granted_mode_other(req);
                    w_assert9(lock->granted_mode == supr[granted_mode_other][req->mode()]);
                }

                if (compat[mode][granted_mode_other]) {
                    /* compatible --> no wait */
                    req->set_mode(mode);
                    lock->granted_mode = supr[mode][granted_mode_other];
                    w_assert1(lock->granted_mode != LL);
                    DBGTHRD(<<"goto success");
                    goto success;
                }

                // don't bother with all the stuff 
                // below if we know we failed now...
                if(!timeout) {
                    RELEASE_HEAD_MUTEX(lock);//acquired in find_lock_head or in-arg
                    
                    return eLOCKTIMEOUT;
                }

                /*
                 * in the special case of multiple threads in a transaction,
                 * we will soon (below) return without awaiting this lock if
                 * another thread in this tx is awaiting a lock; in that case,
                 * we don't want to have changed these data members of the 
                 * request.
                 */
                if(the_xlinfo->waiting_request())  {
                    //Another thread, on behalf of this xct, is either
                    //running the deadlock detector (waking up, trying again)
                    //or is blocked waiting for this lock.
                    goto has_waiter;
                }

                // now that we know we need to wait...
                me()->prepare_to_block();
                req->set_status(lock_m::t_converting);
                req->set_convert_mode(mode);
                req->set_thread(me());
            }
        } else {
            // it is a new request
            prev_mode = NL;

            bool compatible = !lock->waiting 
                && compat[mode][lock->granted_mode];

            if(!compatible) {
                if(!timeout) {
                    RELEASE_HEAD_MUTEX(lock);//acquired in find_lock_head or in-arg
                    return eLOCKTIMEOUT;
                }
                if(the_xlinfo->waiting_request()) {
                    //Another thread, on behalf of this xct, is either
                    //running the deadlock detector (waking up, trying again)
                    //or is blocked waiting for this lock.
                    goto has_waiter;
                }
            }

            // now that we know we actually need a request...
            w_assert2(MUTEX_IS_MINE(xd->lock_info()->lock_info_mutex));

#if USE_BLOCK_ALLOC_FOR_LOCK_STRUCTS
            req = lock_request_pool->acquire(xd, mode, duration);
#else
            req = new lock_request_t();
            req->init(xd, mode, duration);
#endif

            atomic_inc(_requests_allocated);
            DBG(<< "appending request " << req << " to lock " << lock
                    << " lock name=" << lock->name);

            // do this before joining the queue -- we don't want to confuse DLD
            req->set_status(compatible? lock_m::t_granted : lock_m::t_waiting);

            req->set_thread(me());
            me()->prepare_to_block();
            membar_producer();
            lock->queue_append(req);
            if (compatible) {
                /* compatible ---> no wait */
                lock->granted_mode = supr[mode][lock->granted_mode];
                w_assert1(lock->granted_mode != LL);

                DBGTHRD(<<"goto success");
                goto success;
            }

        }

        /* need to wait */
        w_assert3(ret == NL);
        DBGTHRD(<<" will wait");

        w_assert1(timeout && !the_xlinfo->waiting_request());
        lock->waiting = true;

        int count = 0;
#if W_DEBUG_LEVEL > 3
        enum { DREADLOCKS_INTERVAL_MS =1000 };
#else
        enum { DREADLOCKS_INTERVAL_MS =10 };
#endif
        int max_count = 
            (timeout+DREADLOCKS_INTERVAL_MS-1)/DREADLOCKS_INTERVAL_MS;
        
        the_xlinfo->set_waiting_request(req);

        w_rc_t::errcode_t  rce(eOK);
        INC_TSTAT(lock_wait_cnt);
     again:
        {
            DBGTHRD(<<" again: timeout " << timeout);
            w_assert1(timeout != WAIT_IMMEDIATE);
            w_assert2(MUTEX_IS_MINE(the_xlinfo->lock_info_mutex));
            rce = _check_deadlock(xd, count == 0, req);
//cout<<"after check_dl";dump();

            ++count;
            if (rce == eOK) {
                // either no deadlock or there is a deadlock but 
                // some other xact was selected as victim

                the_xlinfo->set_waiting_request_is_blocking(true);
                RELEASE_HEAD_MUTEX(lock);//acquired in find_lock_head or in-arg
                MUTEX_RELEASE(the_xlinfo->lock_info_mutex);

                DBGTHRD(<< "waiting (blocking) for:"
                      << " xd=" << xd->tid() 
                      << " name=" << name 
                      << " mode=" << int(mode)
                      << " duration=" << int(duration)
                      << " timeout=" << timeout );

                if(the_xlinfo->is_nonblocking()) {
                    // die immediately if we've been poisoned 
                    INC_TSTAT(log_full_old_xct);
                    rce = eDEADLOCK;
                } else {
                    const char* blockname = "lock";
                    // TODO: non-rc version of smthread_block
                    INC_TSTAT(lock_block_cnt);
                    rce = me()->smthread_block(DREADLOCKS_INTERVAL_MS, 0, 
                                                                blockname);

                    // No other thread can block on behalf of this
                    // xct as long as waiting_request() is still non-null,
                    // so it is safe to change this here.
                    // The whole point of this is to cut down the likelihood
                    // of our trying to unblock a thread that's
                    // part of deadlock but is not actually blocked.
                    // Think of two threads involved in a deadlock, each
                    // one running dld and never catching the other while
                    // it's in fact blocked.
                    the_xlinfo->set_waiting_request_is_blocking(false);
                }

                DBGTHRD(<< "acquired (unblocked):"
                        << " xd=" << xd->tid() 
                        << " name="<< name 
                        << " mode="<< int(mode)
                        << " duration=" << int(duration) 
                            << " timeout=" << timeout );

#if W_DEBUG_LEVEL > 2
                if (rce) {
                    w_assert3(rce == stTIMEOUT 
                            || rce == eDEADLOCK);
                }
#endif

                // unblock any other thread waiters
                xd->lockunblock();

                MUTEX_ACQUIRE(the_xlinfo->lock_info_mutex);
                ACQUIRE_HEAD_MUTEX(lock); // this is acquire_lock

                DBGTHRD(<<" LOCK HEAD==" << (long) lock
                        << " lock->name " <<  lock->name << " name " <<  name);
                w_assert9(lock->name == name);
                if((rce == stTIMEOUT)
                        && (req->status() != lock_m::t_granted) ) {
#if W_DEBUG_LEVEL > 0
                    if(count > 1000000) {
#if W_DEBUG_LEVEL > 1
                        fprintf(stderr, " possible deadlock : locks \n");
                        MUTEX_RELEASE(the_xlinfo->lock_info_mutex);
                        RELEASE_HEAD_MUTEX(lock);//acquired in find_lock_head or HERE

                        dump();
                        ACQUIRE_HEAD_MUTEX(lock); // this is acquire_lock
                        MUTEX_ACQUIRE(the_xlinfo->lock_info_mutex);
                        fprintf(stderr, " possible deadlock: threads \n");
                        dumpthreads();
#endif

                        // Make it a deadlock
                        rce = eDEADLOCK;
                    } else 
#endif
                    if((timeout == WAIT_FOREVER) || (count < max_count)) {
                        // this is effects a spin.
                        goto again;
                    }
                }
            }
        }

        w_assert9(the_xlinfo->get_cycle() == 0);
        DBGTHRD(<<" request->status()=" << int(req->status()));

        /* FRJ: The lock release code doesn't do proper locking, so it's
           entirely possible to time out or get axed by DLD and also get
           the lock. Fortunately, neither error actually changes the state
           of the lock, so we can ignore it if the request was granted.

           NEH: It looks to me like the release code uses the lock head 
           mutex to ensure exclusivity, so I don't think this should
           happen.
         */
        the_xlinfo->done_waiting();
        if((req->status() != lock_m::t_granted) && rce) {
            // The only errors we should get from
            // _check_deadlock() are deadlock or timeout
            // Status choices are t_converting, t_waiting.
            w_assert2((rce == eDEADLOCK) ||
                      (rce == stTIMEOUT) );

            if(req->status() == lock_m::t_converting)
            {
                // We deny the upgrade but
                // leave the lock request in place with its former
                // status.
                req->set_status(lock_m::t_granted);// revert to granted
            } else {
                // Request denied: remove the request
                w_assert1(req->status() == lock_m::t_waiting);
                w_assert1(MUTEX_IS_MINE(the_xlinfo->lock_info_mutex));
                req->xlink.detach();
                ASSERT_HEAD_MUTEX_IS_MINE(lock);
                w_assert1(lock == req->get_lock_head());
                req->rlink.detach();
#if USE_BLOCK_ALLOC_FOR_LOCK_STRUCTS
                lock_request_pool->release(req);
#else
                delete req;
#endif
                atomic_dec(_requests_allocated);
                req = 0;
            }
            if(rce == eDEADLOCK) {
                if (lock->queue_length() == 0) {
                    // This can happen if T1 is waiting for T2 and 
                    // then T2 blocks forming a cycle.
                    // If both T1 and T2 are
                    // victimized and abort, 
                    // then lock can have no requests. 
                    // The lock head should be removed, but there might
                    // be a race here that allows it to remain.
                    lock->granted_mode = NL;
                    w_assert1(!lock->waiting);
                }
            }
            else {
                w_assert1(rce == stTIMEOUT);
                rce = eLOCKTIMEOUT;
            }
        }
        else {
            w_assert1(req->status() == lock_m::t_granted);
            DBGTHRD(<<"goto success");
            goto success;
        }

        // failed...
        DBGTHRD(<<" waking up waiters");
        wakeup_waiters(lock);

        w_assert9(ret == NL);
        ret = NL;

        return rce;
    }

    
  success:
    DBGTHRD(<< " success: check lock again " << lock <<"=" << *lock);
    w_assert9(req->status() == lock_m::t_granted);
    w_assert9(lock);

    if (req->get_duration() < duration) {
        req->set_duration(duration);
        w_assert2(MUTEX_IS_MINE(the_xlinfo->lock_info_mutex));
        req->xlink.detach();
#ifdef W_TRACE
        {
            lock_head_t*lock = req->get_lock_head();
            DBGTHRD(<<"pushing lock request "
                    << lock->name
                    << " on my_req_list["<<int(duration)<<"] : " << *req);
        }
#endif 
        the_xlinfo->my_req_list[duration].push(req);
    }

    ret = mode;
    if (reqPtr)
        *reqPtr = req;
    req->inc_count();
    RELEASE_HEAD_MUTEX(lock);//acquired in find_lock_head or in-arg
    INC_TSTAT(lock_acquire_cnt);

    return eOK;

 has_waiter:
    {
        // Another thread in our xct is blocking. We're going to have to
        // wait on another resource, until our partner thread unblocks,
        // and then try again.

        DBGTHRD(<< "waiting for other thread in :"
        << " xd=" << xd->tid() 
        << " name=" << name 
        << " mode=" << int(mode)
        << " duration=" << int(duration) << " timeout=" << timeout );

        // GNATS 119. A race between the time we
        // determine that we have to block and the time we grab
        // the _waiters mutex in xct_impl.cpp.  We need to
        // grab that mutex and then recheck the condition before
        // we block.
        RELEASE_HEAD_MUTEX(lock);//acquired in find_lock_head or in-arg
        MUTEX_RELEASE(the_xlinfo->lock_info_mutex);
        INC_TSTAT(lock_await_alt_cnt);
        w_rc_t rc = xd->lockblock(timeout);
        MUTEX_ACQUIRE(the_xlinfo->lock_info_mutex);

        // don't reacquire the lock mutex -- we're about to return

        if (!rc.is_error()) {
            return eRETRY;
        }
        return rc.err_num();
    }
}

rc_t
lock_core_m::release_lock(
        xct_lock_info_t*        the_xlinfo,
        const lockid_t&         name,
        lock_head_t*            lock,
        lock_request_t*         request,
        bool                    force
        // force is true in release_duration (lock_m::unlock_duration)
        )
{
    FUNC(lock_core_m::release);
    DBGTHRD(<<"lock_core_m::release " << " lockid " <<name);

    w_assert3(the_xlinfo->tid() == me()->xct()->tid());
    w_assert3(MUTEX_IS_MINE(the_xlinfo->lock_info_mutex));

    
    if (!lock) {
        lock = find_lock_head(name, false);  // don't create if not found;
        // acquires lock head mutex if found
    } else {
        ACQUIRE_HEAD_MUTEX(lock); // this is release_lock
    }

    if (!lock) {
        return RCOK;
    }

    if (!request) {
        ASSERT_HEAD_MUTEX_IS_MINE(lock);
        request = lock->find_lock_request(the_xlinfo);
    }

    if (! request) {
        // lock does not belong to me --- no need to unlock
        RELEASE_HEAD_MUTEX(lock);//acquired in find_lock_head or in-arg
        return RCOK;
    }

    w_assert2(lock == request->get_lock_head());
    return _release_lock(request, force);
}

rc_t
lock_core_m::_release_lock(lock_request_t* request, bool force
        // force is true in release_duration (lock_m::unlock_duration)
        ) 
{
    DBGTHRD(<<"lock_core_m::_release " 
            << " request " <<request << " force " << force);
    // get these before deleting the request...
    lock_head_t* lock = request->get_lock_head();
    xct_lock_info_t* the_xlinfo = request->get_lock_info();
    the_xlinfo->set_wait_map_obsolete(false);
    
    if (!force && 
            // Don't unlock t_long or longer. That happens only
            // with unlock_duration
            (request->get_duration() >= t_long 
            || request->get_count() > 1
            // Also don't release if reference count is > 1 -- in that
            // case just decrement the count.
            // This is regardless of the duration.
            )
        ) {
#if W_DEBUG_LEVEL > 0
        // It should be impossible for two threads of an xct
        // to acquire t_instant locks at the same time, therefore
        // driving up the ref count and making the instant locks
        // hang around.  This assert is part of checking that.
        if(request->get_duration() == t_instant) {
            w_assert1(request->get_count() == 1);
        }
#endif
        if (request->get_count() > 1) request->dec_count();
        RELEASE_HEAD_MUTEX(lock);//acquired in calling method
        DBGTHRD(<<"lock_core_m::_release dec count only " );
        return RCOK;
    }
    // if force, we'll release regardless of the duration
    // or the reference count.
    // This is called at the end of tx.

    ASSERT_HEAD_MUTEX_IS_MINE(lock);
    request->rlink.detach();
    w_assert1(MUTEX_IS_MINE(the_xlinfo->lock_info_mutex));
    request->xlink.detach();
#if USE_BLOCK_ALLOC_FOR_LOCK_STRUCTS
    lock_request_pool->release(request);
#else
    delete request;
#endif
    DBGTHRD(<<"lock_core_m::_release deleted request" );

    atomic_dec(_requests_allocated);

    lock->granted_mode = lock->granted_mode_other(0);
    wakeup_waiters(lock);

    return RCOK;
}

void
lock_core_m::wakeup_waiters(lock_head_t*& lock)
{
    // The lock was released by someone or else its acquisition failed.
    // Try to free the lock head if it's not in use anymore. 
    //
    // If we hit any contention, give up -- the
    // lock will probably get used again pretty quickly if it's that
    // hot...
    ASSERT_HEAD_MUTEX_IS_MINE(lock);

    if (lock->queue_length() == 0) 
    {
        uint32_t idx = _table_bucket(lock->name.hash());
        
        // lock out other threads arriving
        ACQUIRE_BUCKET_MUTEX(idx);
        if(!lock->pin_cnt) {
            // empty queue and nobody else is around
            
            RELEASE_HEAD_MUTEX(lock);//acquired in calling method
            FreeLockHeadToPool(lock);
            lock = 0;
        }

        RELEASE_BUCKET_MUTEX(idx);

        if (!lock) return;
    }

    if(lock->waiting) 
    {
        lock_request_t* request = 0;
        
        {   // **BEFORE** we wake-up each waiter, we
            // immediately invalidate the wait map so that subsequent
            // spins will suspect this waiter's bitmap might cause false positives.
            // this is only to reduce false positives. not required for consistency.
            // @see xct_lock_info_t::_wait_map_obsolete
            lock_head_t::safe_queue_iterator_t iter(*lock); 
            while ((request = iter.next()) != NULL) {
                if (request->status() != lock_m::t_granted) {
                    xct_lock_info_t *lock_info = request->get_lock_info();
                    lock_info->set_wait_map_obsolete(true);
                }
            }
        }

        lock->waiting = false;
        bool cvt_pending = false;
        lock_head_t::safe_queue_iterator_t iter(*lock); 

        while (!lock->waiting && (request = iter.next())) {
            bool wake_up = false;
            
            switch (request->status()) {
            case lock_m::t_converting: {
                lmode_t gmode = lock->granted_mode_other(request);
                w_assert9(lock->granted_mode == supr[gmode][request->mode]);
                wake_up = compat[request->convert_mode()][gmode];
                if (wake_up)
                    request->set_mode (request->convert_mode());
                else
                    cvt_pending = true;
                break;
            }
            case lock_m::t_waiting:
                if (cvt_pending)  {
                    // do not wake up waiter because of pending conversions
                    lock->waiting = true;
                    break;
                }
                wake_up = compat[request->mode()][lock->granted_mode];
                lock->waiting = ! wake_up;
                break;
            case lock_m::t_granted:
                break;
            case lock_m::t_no_status:
            default:
                W_FATAL(eINTERNAL);
            }

            if (wake_up) {
                lock->granted_mode = supr[request->mode()][lock->granted_mode];
                w_assert1(lock->granted_mode != LL);
                smthread_t *thr = request->thread();
                w_assert1(thr);
                // Assert that this thread is the one that's the ONLY
                // waiting thread for the xct.  That means that
                // the request we are looking at is the waiting_request 
                w_assert2(request == 
                             request->get_lock_info()->waiting_request());
                request->set_status(lock_m::t_granted);
                rc_t rc = thr->smthread_unblock(eOK); 
                if(rc.is_error() && rc.err_num() == eNOTBLOCKED) {
                    // We hit a race in which the waiting thread is
                    // no longer blocked -- perhaps it was running
                    // deadlock detection. This is a problem because
                    // it missed its wakeup call.
                    rc = RCOK;
                }
                W_COERCE(rc);
            }
        }

        if (cvt_pending) lock->waiting = true;
    }
    RELEASE_HEAD_MUTEX(lock);//acquired in calling method
}

rc_t
lock_core_m::release_duration(
    xct_lock_info_t*    the_xlinfo,
    duration_t          duration,
    bool                read_lock_only
    )
{
    FUNC(lock_core_m::release_duration);
    DBG(<<"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" << "RELEASE DURATION");
    DBGTHRD(<<"lock_core_m::release_duration "
            << " tid=" << the_xlinfo->tid()
                << " duration=" << int(duration)
                << " read_lock_only=" << read_lock_only
				);
    w_assert2(MUTEX_IS_MINE(the_xlinfo->lock_info_mutex));

    lock_head_t* lock = 0;
    lock_request_t* request = 0;
    w_assert1((duration == 0 || duration > 0) && duration < t_num_durations);

    /*
     *  release from 0 to "duration"
     */
    for (int i = t_instant; i <= duration; i++) 
    {
        if (read_lock_only) {
            // releases only read locks
            lock_head_t*        lock = 0;
            lock_request_t*     request = 0;

            request_list_i iter(the_xlinfo->my_req_list[t_short]);
            while ((request = iter.next()))  {
                if (request->mode() == IS || request->mode() == SH
                    || request->mode() == SN || request->mode() == NS
                ) {
                    request->xlink.detach();
                    lock = request->get_lock_head();
                    W_COERCE(release_lock(the_xlinfo, lock->name, lock, request, true) );
                }
            }
            // we don't "downgrade" SX/XS to SN/NS for laziness. see ticket:101
        } else {
            //backwards:
            //requests(the_xlinfo->my_req_list[i], true);
            request_list_t &requests =  the_xlinfo->my_req_list[i];
            DBGTHRD(<<"lock_core_m::release_duration "
                << " i=" << i << " duration=" << int(duration)
                << " request list length " << requests.num_members());
		while ((request = requests.top()))  {
			DBG(<<"popped request "  << request << "==" << *request);
			lock = request->get_lock_head();
			W_COERCE(release_lock(the_xlinfo, lock->name, lock, request, true) );
		}
        }
    }
    DBGTHRD(<<"lock_core_m::release_duration DONE");

    return RCOK;
}

w_rc_t::errcode_t
lock_core_m::_check_deadlock(xct_t* self, 
        bool first_time, 
        lock_request_t *myreq
)
{
    xct_lock_info_t* myli = self->lock_info();
    w_assert2(MUTEX_IS_MINE(myli->lock_info_mutex));

    lock_head_t* lock = myreq->get_lock_head();

    ASSERT_HEAD_MUTEX_IS_MINE(lock);

    /*************************************************************************
     *
     * "Dreadlocks" Deadlock Detector Algorithm, based on the
     * algorithm by Eric Koshkinen and Maurice Herlihy.
     * With some modifications.
     *
     * Each thread chooses three random bits (without
     * replacement) from a bitmap as its fingerprint ("hash function").
     * 
     * Whenever a  thread must wait on a lock, it sets its 
     * bitmap to its fingerprint and then to the
     * bitwise OR of its own bitmap and the bitmap of its predecessor. 
     * While not waiting for a resource, its bitmap is empty. This is a
     * modification to the Herlihy algorithm.  It seems it would reduce
     * the chances of a false positive by making the wait maps less dense
     * (T1 waits for T2, which isn't waiting). However, the cost is that
     * we have to timeout at least once and retry the algorithm before
     * we find the deadlock. 
     *
     * Its predecessor is some thread waiting on the same lock that the
     * thread is trying to acquire but must wait for.
     * There is a lot of potential for races in here, threads waking up
     * just before the dld tries to unblock them, false positives, etc. 
     * For each predecessor, we OR in the wait map for that thread's xct.
     * When computing the bitwise OR, if looks for its own fingerprint in
     * the predecessor's bitmap: this would indicate a very likely deadlock. 
     *
     * Upon detecting a deadlock then chooses the younger transaction
     * of the two (xcts attached to the two threads) as the victim.
     *
     * NOTE:
     * -- dead lock detection doesn't actually do an abort -- somewhere up
     *  the call chain, eDEADLOCK is detected and the caller aborts.
     *
     * -- the predecessor thread cannot be attached to the same transaction as
     *  the thread running the deadlock detection. This is enforced by the
     *  the fact that at most one thread of an xct can be in the lock manager
     *  at a time (unless blocked waiting), 
     *  by locking the xct_lock_info_t mutex (freed while blocking waiting
     *  for a lock), and by the lock manager checking waiting_request() to
     *  disallow two threads waiting at once.
     *
     * -- the algorithm only has to traverse the lock_head queue for the
     *  lock we are trying to acquire; it does not have to traverse any
     *  other queues. At the time it traverses this queue, it holds the
     *  lock head's mutex so it should be safe.
     *
     * -- Herlihy, et al has you spinning on the lock's free status as
     *  well as on the wait map changing. We don't do this, as it's not
     *  really feasible here.  It is our calling _check_deadlock in a
     *  loop that effects the spin on both the wait map. Whenever we
     *  stop blocking, we clean our wait map and restart the DLD. This
     *  is how updates to other xct wait maps are propagated to us.
     *
     *  -- We can (and do) have multiple simultaneous victims for the
     *  same cycle, by virtue of the fact that any number of xct/threads 
     *  in the cycle can be doing simultaneous DLD.  Picking the younger
     *  xct of a pair is all we do to reduce the likelihood of this
     *  happening. 
     *
     *  In the hope of minimizing the chance of having too-dense wait maps,
     *  we have chosen to associate the fingerprints with the smthread_t,
     *  under the assumption that a server will create a pool of smthreads
     *  to run the client requests, and so there are likely to be
     *  fewer smthreads than total xcts serviced.  An xct can participate
     *  in a cycle only if it's got an attached thread, and any
     *  xct that has no attached threads has an empty wait map.  
     *  This way, if we needed to do so, we could insert code in smthreads
     *  to check the uniqueness of a map, and also the density of the
     *  union of all smthread wait maps.  It increases the cost of
     *  starting an smthread, but that should not be in the critical
     *  path of a server.
     * 
     *************************************************************************
     */


    bool deadlock_found = false;
    lock_request_t* req = 0;
    if(first_time) {
        INC_TSTAT(lock_dld_first_call_cnt);
    } else {
        INC_TSTAT(lock_dld_call_cnt);
    }

    // if myself seems to be based on obsolete information,
    // let's start over
    if (myli->is_wait_map_obsolete()) {
        myli->clear_wait_map();
        myli->set_wait_map_obsolete(false);
    }

    {
        /* We really have to check for deadlock involving any of the
         * xcts that have requests queued ahead of us.
        */
        int Qlen = lock->queue_length();
        w_assert2(Qlen> 1);
        // We should not be calling _check_deadlock unless we have
        // to wait for something so the length of the queue had better
        // not be 1. If we are converting, we have to look beyond
        // our request in the queue, but if not, we can stop when we
        // hit our request.
        bool converting = (myreq->status() == lock_m::t_converting) ;
        lmode_t mymode = converting ? myreq->convert_mode() : myreq->mode();

        // This iterator is ok because we have the lock's head_mutex
        // and if we block, we'll restart the search anyway.
        lock_head_t::safe_queue_iterator_t it(*lock); 
        bool so_far_everyone_granted = true;
        bool ahead_of_us = true;
        for(int i=0; i < Qlen; i++)
        {
            req = it.next();
            if(req == myreq) 
            {
                ahead_of_us = false;
                // If we are not converting, then the only request we
                // depend on should be ahead of us in the queue.
                if(!converting) {
                    if(DEBUG_DEADLOCK)
                    fprintf(stderr, 
                "%p found myreq, not converting -- quit looking, i %d len %d\n",
                        myreq, i, Qlen);
                    break;
                }

                // On the other hand, if we are converting, we could depend
                // on something anywhere in the queue, so keep looking
                if(DEBUG_DEADLOCK)
                    fprintf(stderr, 
                    "%p found my req -- skipping, keep looking, i %d len %d\n",
                    myreq, i, Qlen);
                continue;
            }

            // compatibility is not just with predecessor's current lock mode.
            // Suppose this case.
            // Lock head A: T1-S-granted, T2-S-granted-upgrading-to-X, T3-S-waiting
            // Lock head B: T3-X-granted, T1-X-waiting
            // because T2 has prior upgrade-request, T3 can't get S lock on A.
            // We have to detect this as an incompatible case too.
            // see acquire_lock() code. see ticket:105            
            bool predecessor_compatible;
            if (ahead_of_us && (req->status() == lock_m::t_waiting || req->status() == lock_m::t_converting)) {
                if (so_far_everyone_granted) {
                    // this is the first waiter ahead of us, so we are waiting for him too
                    // regardless of lock mode
                    predecessor_compatible = false;
                    so_far_everyone_granted = false;
                } else {
                    // otherwise (2nd, 3rd... waiter ahead us), just check lock mode compatibility.
                    // alternatively, we can always consider them as incompatible and it might detect deadlock
                    // earlier (before the first waiter or this thread get victimized), but could
                    // cause false positives instead (consider this: S, S->X (1st waiter), S(2nd),S,..., S(me) ).
                    if (req->status() == lock_m::t_converting) {
                        predecessor_compatible = compat[req->mode()][mymode] && compat[req->convert_mode()][mymode];
                    } else {
                        predecessor_compatible = compat[req->mode()][mymode];
                    }
                }
            } else {
                predecessor_compatible = compat[req->mode()][mymode];
            }
            if(predecessor_compatible) {
                if(DEBUG_DEADLOCK) 
                    fprintf(stderr, 
"%p found compat i %d len %d status %d mode %d req %p (my status %d mode %d)\n",
                    myreq, i, Qlen,
                    req->status(), req->mode(), req,
                myreq->status(), mymode);
                // We can't be waiting for this guy to go away...
                continue;
            }


            // We have a candidate.
            // thread map update fails if it detects deadlock...
            xct_lock_info_t* theirli = req->get_lock_info();
            w_assert1(theirli != myli);
            // if the candidate seems to be based on obsolete information,
            // let's ignore it for now. It will be updated at next spin
            if (theirli->is_wait_map_obsolete()) {
                continue;
            }
            if(!myli->update_wait_map(theirli->get_wait_map())) {
                if(DEBUG_DEADLOCK)
                    fprintf(stderr, 
"%p Deadlock found@%d i %d len %d their status %d mode %d req %p (my status %d mode %d)\n", 
                myreq, __LINE__, i, Qlen,
                req->status(), req->mode(), req,
                myreq->status(), mymode);
                deadlock_found = true;
                break;
            }

            if(DEBUG_DEADLOCK)
                fprintf(stderr, 
                        "%p N/A i %d len %d status %d mode %d req %p\n", 
                        myreq, i, Qlen, 
                        req->status(), req->mode(), req);

        }
        w_assert1(req != NULL);

    }

    if(deadlock_found) {
        INC_TSTAT(lock_deadlock_cnt);

        if(0)
        {
            ostringstream os;
            os << *lock << ends;
            fprintf(stderr, 
                "Possible deadlock waiting on %s req %p\n", os.str().c_str(),
                    req);
        }

        xct_lock_info_t *their_lock_info = req->get_lock_info();
        uint32_t my_chain_len = self->get_xct_chain_len();
        uint32_t their_chain_len = req->get_xct_chain_len();
        
        bool killhim;
        // If one of them is chained transaction, let's victimize
        // shorter chain because it will quickly converge in dominated lock table.
        // See ticket:102
        if (their_chain_len < my_chain_len) {
            killhim = true;
        } else if (their_chain_len > my_chain_len) {
            killhim = false;
        } else {
            // if chain length is same, kill younger xct (larger tid)
            killhim = myli->tid() < their_lock_info->tid();
        }

        // Grab the mutex and check whether this makes sense.  This should
        // avoid doing this while the other thread is in DLD.  It could
        // be blocked and just waking up from a timeout though.
        if(killhim) {
            // NOTE: I hold my lock_info_mutex, but
            // since I am necessarily acquiring in order (old->young)
            // this should not cause a mutex-mutex deadlock.
            // MUTEX_ACQUIRE(their_lock_info->lock_info_mutex);
            // NEH: The problem  here is that a thread cannot hold 2 of these
            // mutexes because they use a single TLS qnode for this.
        // FRJ: Also note that we indirectly have locked their
        // node (we hold the lock head, so they can't free
        // it). The critical section is both unnecessary and
        // deadlock-prone
            
            // I'm older -- abort other
            INC_TSTAT(lock_dld_victim_other_cnt);
            // We have to unblock the thread that's waiting for this xct,
            // which might not be the one associated with the req we found.
            
            lock_request_t *that_waiting = their_lock_info->waiting_request();
            if(!that_waiting) {
                // No longer waiting...
                INC_TSTAT(lock_false_deadlock_cnt);
                return eOK;
            }
            w_assert2 (myli->tid() != that_waiting->get_lock_info()->tid());

            if(that_waiting->status() != lock_m::t_waiting
                &&
               that_waiting->status() != lock_m::t_converting) 
            {
                // no longer waiting - might be granted now.
                INC_TSTAT(lock_false_deadlock_cnt);
                return eOK;
            }


            // This is still racy, but with this we might reduce the
            // chance of trying to unblock a thread that's running 
            // DLD and not blocked yet, or is no longer blocked.  
            // We can't acquire the
            // mutex on their_lock_info in order to change any state
            // and cause the other to go away on its own.  The best
            // we can do is try to avoid the unblock and go back to sleep,
            // and try again.   If the deadlock still persists, we
            // can hope that the younger thread will notice that
            // it should kill itself.
            if(their_lock_info->waiting_request_is_blocking()==false) 
            {
                // Might be running DLD but not blocked at the moment.
                // Or no longer waiting - might be granted now.
                INC_TSTAT(lock_dld_false_victim_cnt);
                return eOK;
            }

            DBGTHRD(<<"about to unblock " << *that_waiting);
            smthread_t *thr = that_waiting->thread();
            if(!thr) {
                // No longer has  a waiting thread.
                // This can happen only if the request got destructed
                // and hasn't yet been reused.  We should not
                // encounter such a request in the queue since we hold
                // the lock head mutex.
                W_FATAL(eINTERNAL);
            }
            // TODO: non-rc-t version of smthread_unblock
            rc_t rc = thr->smthread_unblock(eDEADLOCK);

            if(rc.is_error()) {
                if(rc.err_num() != eNOTBLOCKED) {
                    // programming error 
                    W_FATAL(rc.err_num());
                }
                if(0)
                {
                    ostringstream os;
                    os << req << ends;
                    fprintf(stderr, 
        "Unblocked non-waiting deadlock victim while acquiring on %s\n", 
                        os.str().c_str());
                }
                INC_TSTAT(lock_dld_false_victim_cnt);
                return eOK;
            }
            return rc.err_num();
        } else {
            // I'm younger -- I'm the victim
            INC_TSTAT(lock_dld_victim_self_cnt);

            return eDEADLOCK;
        }
    }

    return eOK;
}



/*********************************************************************
 *
 *  lock_core_m::dump()
 *
 *  Dump the lock hash table (for debugging).
 *
 *********************************************************************/
void
lock_core_m::dump(ostream & o)
{
    // disabled because there's no safe way to iterate over the lock table
    // but you can use it in a debugger.  It is used by smsh in
    // single-thread cases.
    o << "WARNING: lock_core_m::dump is not thread-safe:" << endl;
    o << "lock_core_m:"
      << " _htabsz=" << _htabsz
      << endl;
    for (unsigned h = 0; h < _htabsz; h++)  {
        ACQUIRE_BUCKET_MUTEX(h);
        chain_list_i i(_htab[h].chain);
        lock_head_t* lock;
        lock = i.next();
        if (lock) {
            o << h << ": ";
        }
        while (lock)  {
            // First, verify the hash function:
            unsigned hh = _table_bucket(lock->name.hash());
            if(hh != h) {
                o << "ERROR!  hash table bucket h=" << h 
                    << " contains lock head " << *lock
                    << " which hashes to " << hh
                    << endl;
            }
            
            ACQUIRE_HEAD_MUTEX(lock); // this is dump
            o << "\t " << *lock << endl;
            lock_request_t* request;
            lock_head_t::safe_queue_iterator_t r(*lock);

            while ((request = r.next()))  {
                o << "\t\t" << *request << endl;
            }
            RELEASE_HEAD_MUTEX(lock); // acquired here NOT through find_lock_head
            lock = i.next();
        }
        RELEASE_BUCKET_MUTEX(h);
    }
}


void lock_core_m::dump()
{
    dump(cerr);
    cerr << flushl;
}

/*********************************************************************
 *
 *  lock_core_m::assert_empty()
 *
 *  Unsafely check that the lock table is empty. For debugging -
 *  and assertions at shutdown, when MT-safety shouldn't be an issue.
 *
 *********************************************************************/
void
lock_core_m::assert_empty() const
{
    int found_lock=0;
    int found_request=0;

    for (uint h = 0; h < _htabsz; h++)  
    {
        chain_list_t &C(_htab[h].chain);
        if(C.is_empty()) continue;

        chain_list_i i(C);
        lock_head_t* lock;
        lock = i.next();
        if (lock) {
            cerr << h << ": ";
        }
        while (lock)  {
            found_lock++;
            cerr << "\t " << *lock << endl;
            lock_request_t* request;
            lock_head_t::unsafe_queue_iterator_t r(*lock); // ok - debugging only

            while ((request = r.next()))  {
                found_request++;
                cerr << "\t\t" << *request << endl;
            }
            lock = i.next();
        }
    }
    w_assert1(found_request == 0);
    w_assert1(found_lock == 0);
}


/*********************************************************************
 *
 *  lock_core_m::_dump()
 *
 *  Unsafely dump the lock hash table (for debugging).
 *  Doesn't acquire the mutexes it should for safety, but allows
 *  you dump the table while inside the lock manager core.
 *
 *********************************************************************/
void
lock_core_m::_dump(ostream &o)
{
    for (uint h = 0; h < _htabsz; h++)  {
        chain_list_i i(_htab[h].chain);
        lock_head_t* lock;
        lock = i.next();
        if (lock) {
            o << h << ": ";
        }
        while (lock)  {
            o << "\t " << *lock << endl;
            lock_request_t* request;
            lock_head_t::unsafe_queue_iterator_t r(*lock); // ok - debugging only
            while ((request = r.next()))  {
                o << "\t\t" << *request << endl;
            }
            lock = i.next();
        }
    }
    o << "--end of lock table--" << endl;
}


/*********************************************************************
 *
 *  operator<<(ostream, lock_request)
 *
 *  Pretty print a lock request to "ostream".
 *
 *********************************************************************/
ostream& 
operator<<(ostream& o, const lock_request_t& r)
{
    o << "xct:" << r.get_lock_info()->tid()
      << " mode:" << lock_base_t::mode_str[r.mode()]
      << " cnt:" << r.get_count()
      << " dur:" << lock_base_t::duration_str[r.get_duration()]
      << " stat:";


    switch (r.status()) {
    case lock_m::t_granted:
        o << 'G';
        break;
    case lock_m::t_converting:
        o << 'U' << lock_base_t::mode_str[r.convert_mode()];
        break;
    case lock_m::t_waiting:
        o << 'W';
        break;
    case lock_m::t_no_status:
    default:
        o << "BAD STATE (" << int(r.status()) << ")"  ;
        // W_FATAL(smlevel_0::eINTERNAL);
    }

    return o;
}



/*********************************************************************
 *
 *  operator<<(ostream, lock_head)
 *
 *  Pretty print a lock_head to "ostream".
 *
 *********************************************************************/
ostream& 
operator<<(ostream& o, const lock_head_t& l)
{
    o << l.name << ' ' << lock_base_t::mode_str[l.granted_mode];
    if (l.waiting) o << " W";
    return o;
}




/*********************************************************************
 *
 *  operator<<(ostream, lockid)
 *
 *  Pretty print a lockid to "ostream".
 *
 *********************************************************************/
ostream& 
operator<<(ostream& o, const lockid_t& i)
{
    stid_t s;
    i.extract_stid(s);
    o << "L(" << s << ": key-hash=" << i.l[1];
    return o << ')';
}

/****************************************************************************************************************
 * virtual tables for lock_m, lock_core. I would put this in vtable_lock.cpp but for the
 * fact that the bucket_t is static in scope.
****************************************************************************************************************/


int                 
lock_m::collect( vtable_t & res, bool names_too) 
{
    return _core->collect(res, names_too);
}

#include <vtable.h>
#include <sm_vtable_enum.h>


enum {
    /* for locks */
    lock_name_attr,
    lock_mode_attr,
    lock_duration_attr,
    lock_tid_attr,
    lock_status_attr,

    /* last number! */
    lock_last 
};

const char *lock_vtable_attr_names[] =
{
    "Name",
    "Lock mode",
    "Duration",
    "Children",
    "Tid",
    "Status"
};

static vtable_names_init_t names_init(lock_last, lock_vtable_attr_names);

int
lock_core_m::collect( vtable_t& v, bool names_too)
{
    // NOTE: This does not have to be atomic or thread-safe.
    // It yields approximate statistics and is used by ss_m.
    int n = _requests_allocated /* for names */;
    w_assert1(n>=0);
    int found = 0;
    int per_bucket = 0;

    if(names_too) n++;

    // n = # lock request = # rows
    // Number of attributes = lock_last.
    // names_init.max_size() is max attribute length.
    int max_size = names_init.max_size();
    max_size = max_size < 100 ? 100 : max_size; // for lock names
    if(v.init(n, lock_last, max_size)) return -1;

    vtable_func<lock_request_t> f(v);
    if(names_too) f.insert_names();

    if (n > 0) {
        for (uint h = 0; h < _htabsz; h++)  {
            w_assert9(v.size() == n);
            per_bucket=0;
            lock_head_t* lock;
            ACQUIRE_BUCKET_MUTEX(h);
            chain_list_i i(_htab[h].chain);
            lock = i.next();
            while (lock)  {
                ACQUIRE_HEAD_MUTEX(lock); //collect vtable 
                lock_request_t* request;
                lock_head_t::safe_queue_iterator_t r(*lock); 
                while ((request = r.next()))  {
                    if(found <= n)  {
                        f(*request);
                        per_bucket++;
                        found++;
                    } else {
                        // back out of this bucket
                        f.back_out(per_bucket);
                        break;
                    }
                }
                RELEASE_HEAD_MUTEX(lock); // acquired here NOT through find_lock_head
                if(found <= n)  {
                    lock = i.next();
                } else {
                    lock = 0;
                }
            }
            RELEASE_BUCKET_MUTEX(h);
            if(found > n)  {
                // realloc and re-start with same bucket
                if(f.realloc()<0) return -1;
                h--;
                found -= per_bucket;
                n = v.size(); // get new size
            }
        }
    }
    w_assert9(found <= n);
    return 0;
}

#ifdef __GNUG__
template class vtable_func<lock_request_t>;
#endif /* __GNUG__ */
 
void    
lock_request_t::vtable_collect_names(vtable_row_t &t)
{
    names_init.collect_names(t);
}



void        
lock_request_t::vtable_collect(vtable_row_t & t)
{
    // NOTE: This does not have to be atomic or thread-safe.
    // It yields approximate statistics and is used by ss_m.
    const lock_head_t                 *lh = get_lock_head();

    {
        w_ostrstream o;
        o<< lh->name <<ends;
        t.set_string(lock_name_attr, o.c_str());
    }
    t.set_string(lock_mode_attr, lock_base_t::mode_str[mode()]);
    t.set_string(lock_duration_attr, lock_base_t::duration_str[get_duration()] );

    {
        w_ostrstream o;
        o << get_lock_info()->tid() <<  ends;
        t.set_string(lock_tid_attr, o.c_str());
    }

    const char *c=0;
    switch (status()) {
    case lock_m::t_granted:
        c = "G";
        break;
    case lock_m::t_converting:
        c = "U";
        // TODO: add attribute for mode to which we're converting
        break;
    case lock_m::t_waiting:
        c = "W";
        break;
    case lock_m::t_no_status:
    default:
        W_FATAL(smlevel_0::eINTERNAL);
    }
    t.set_string(lock_status_attr, c);
}
