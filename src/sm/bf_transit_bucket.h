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

 $Id: bf_transit_bucket.h,v 1.5 2010/07/01 00:08:19 nhall Exp $

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

#ifndef BF_TRANSIT_BUCKET_H
#define BF_TRANSIT_BUCKET_H

/**\brief Bucket for in-transit pages. We put only in-transit-out pages in here.
 * \details
 * For now, at most one page can be in a bucket. We haven't needed to
 * expand this yet.
 * We keep a small table of these buckets (NUM_TRANSIT_BUCKETS).
 */
class transit_bucket_t
{
public:
    enum { MAX_IN_TRANSIT=1 };
    pthread_mutex_t _tb_mutex; // paired with _tb_cond
private:
    pthread_cond_t _tb_cond;
    bfpid_t        _pages[MAX_IN_TRANSIT];
    int            _page_count;

    static int const          NUM_TRANSIT_BUCKETS = 128;
    static transit_bucket_t   _transit_buckets[NUM_TRANSIT_BUCKETS];

public:
    NORET transit_bucket_t()
        : _page_count(0)
    {
        DO_PTHREAD(pthread_mutex_init(&_tb_mutex, NULL));
        DO_PTHREAD(pthread_cond_init(&_tb_cond, NULL));
    }

    NORET ~transit_bucket_t() 
    {
        DO_PTHREAD(pthread_mutex_destroy(&_tb_mutex));
        DO_PTHREAD(pthread_cond_destroy(&_tb_cond));
    }

    /// True iff this bucket has no pages in it.
    bool empty() { return _page_count < transit_bucket_t::MAX_IN_TRANSIT; }

    /// Put this control block on in-transit-out list iff it's marked dirty. 
    /// The control block is NOT in the hash table when this is called.
    /// Caller (bf_core_m::replacement) holds the page_write_mutex_t,
    /// the transit bucket lock and the
    /// appropriate hash table bucket lock. 
    /// and now the frame latch.
    void make_in_transit_out(const bfpid_t &pid) {
        // add to in-transit list
#if W_DEBUG_LEVEL > 2
        for(int i=0; i < _page_count; i++)
            w_assert3(_pages[i] != pid);
#endif

        w_assert1(_page_count < transit_bucket_t::MAX_IN_TRANSIT);
        _pages[_page_count++] = pid;
        w_assert2(_page_count <= transit_bucket_t::MAX_IN_TRANSIT);
    }

    /// Caller is bf_core_m::grab
    /// It holds the transit bucket mutex and awaits the condition,
    /// which is signalled by make_not_in_transit_out.
    void await_not_in_transit_out(const bfpid_t &pid) {
        // find out if it's in-transit-out
        for(int i=0; i < _page_count; i++) {
            if( _pages[i] == pid ) {
                DO_PTHREAD(pthread_cond_wait(&_tb_cond, &_tb_mutex));
                i=-1; // start over in case the pid changed slots
            }
        }
        // no longer in-transit-out
    }

    /// Called by publish_partial.
    /// Signal waiters that this page is no longer in-transit-out.
    /// Removes the page from the bucket if it is in there. (Is only
    /// in there if the page was dirty when made in_transit_out.
    void make_not_in_transit_out(const bfpid_t &pid) 
    {
        int i;
        for(i=0; i < _page_count; i++) {
            if(_pages[i] == pid)
                break;
        }
        
        // clear out the entry and wake up any waiters
        if(i < _page_count) {
            // only happens if the page was dirty before
            w_assert1(_page_count <= transit_bucket_t::MAX_IN_TRANSIT);
            _pages[i] = _pages[--_page_count];
            pthread_cond_broadcast(&_tb_cond);
        }
    }

    /// Return a ref to the transit bucket; used for critical sections
    /// on the bucket's _tb_mutex.
    static  transit_bucket_t & get(bfpid_t const &pid) 
    {
        // compiler should be smart enough not to use modulo here:
        return _transit_buckets[pid.page % NUM_TRANSIT_BUCKETS];
    }
};

/* Turns out that we can't even afford a central mutex long enough to
   traverse the in-transit list. So, the New Way is not to have an in- 
   transit list at all.

   When a page gets evicted, its entry in the htab disappears
   atomically. If a new thread comes looking for the page later,
   it won't find anything in the htab and will have to call grab().

   At the start of a page grab, the thread acquires the transit bucket
   mutex in order to prevent other grabs to the same page. 
   
 */
/*
 * This corresponds to part of the changes described in Shore-MT paper,
 * 6.2.3, page 10, handling of in-transit-out pages.
 *
 * Each bucket supports only a limited number of
 * in-transit-out pages; the replacement algorithm is aware of the
 * limitation and will not evict a page when its transit bucket is
 * full. 
 *
 * At the beginning of grab(),
 * Latch the new frame in EX mode and install it in the
 * htab. Then, (maybe) wait for in-transit-out to complete, perform
 * read, and (maybe) downgrade the latch.
 */


#endif
