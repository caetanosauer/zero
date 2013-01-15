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

 $Id: bf_htab.cpp,v 1.7 2010/12/17 19:36:26 nhall Exp $

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
#endif

#include <stdio.h>
#include <cstdlib>
#include "sm_int_0.h"

#include "bf_s.h"
#include "bf_core.h"

/* This cuckoo-hashing corresponds to the changes in the
 * Shore-MT paper in 6.2.3 (page 8), second stage,
 * but it has been rewritten since then, to use a variant
 * of cuckoo hashing in which buckets are defined a little differently-
 * each "tub" has N slots (say 4) to hold up to N entries, 
 * and "buckets" are collections of slots.
 */

#include "w_hashing.h"
#include "bf_htab.h"

/*
 *  Move item from src->slot[which] to the next 
 *  empty slot in dest->slot[] if there is one.
 *  Compress src->slot[], update slot _counts in both src and dest
 *
 *  true -> success
 *  false -> failed - no room
 */

bool bf_core_m::htab::cuckold(bucket* dest, bucket* src, int which, 
        int hashfunc, bfcb_t* &moved) 
{
    ADD_BFSTAT(_slots_tried, 1); 
    if(dest->_count < SLOT_COUNT) 
    {
        ADD_BFSTAT(_cuckolds, 1); 
        // dest has room
        
        // Order the lock-grabbing to avoid deadlock
        bucket* b1 = (src < dest)? src : dest;
        bucket* b2 = (src < dest)? dest : src;

        CRITICAL_SECTION(cs1, b1->_lock);
        CRITICAL_SECTION(cs2, b2->_lock);

        if(src->_count < SLOT_COUNT) 
        {
            // src now has room - don't do anything after all
            return true;
        }
        
        if(dest->_count < SLOT_COUNT)
        {
            // good to go : dest still has room.  
            // Copy over src, update slot _count
            bfcb_t *b;
            w_assert1(which < src->_count);
            dest->_slots[dest->_count++] = b = src->_slots[which];
            b->set_hash_func(hashfunc);
            b->set_hash(dest-_table);

            // adjust dest hash info while we have the locks.
            //
            // compress src, update slot _count
            src->_slots[which] = src->_slots[--src->_count];
            moved = src->_slots[which];
            CHECK_ENTRY(src-_table, true);
            CHECK_ENTRY(dest-_table, true);
            return true;
        }
    }
    CHECK_TABLE();
    return false; // possibly stale
}

/*
 * Try to free up a slot in bucket[h].
 * If necessary, make recursive calls to free up
 * space in alternative location. for entries in bucket[h]
 * We limit the number of
 * buckets visited in our attempt to make space for 1 slot.
 * We do not detect revisiting a bucket, so we cannot distinguish 
 * a cycle from a non-cyclic long search for space.
 * 
 * Note that this makes room in bucket _table[h], but leaves it
 * up to the caller to populate the newly free-up slot.
 * 
 * true -> success
 * false -> failure
 *
 */ 
bool bf_core_m::htab::ensure_space(int h, int &limit, bfcb_t *&moved,
                const bfpid_t &save_pid_for_debug) 
{
    CHECK_ENTRY(h, false);
    ADD_BFSTAT(_ensures, 1); 

    // This is the bucket we're trying to make room in
    bucket* b = &_table[h];

    if(b->_count < SLOT_COUNT)
        return true;

    // the bucket is full... try to make room below,
    // first by a 1-level move
    const int NALTERNATIVES=SLOT_COUNT*(HASH_COUNT-1);
    struct alternative {
        int altbucket; // to -- alt bucket location tried
        int slotindex; // from -- original slot location in this bucket
        int hashfunc; // hash func used to get the alternative destination
    } alternatives[ NALTERNATIVES ];
    ::memset(alternatives, 0, sizeof(alternative) * NALTERNATIVES);
    int alts_index=0;

    // See if we can move the resident of one of our _slots
    // to one of its alternative locations where there is
    // already room, i.e., without any recursion. 
    // NOTE: b->_count could change underneath us, but
    // any attempt a actually moving data (cuckold) will
    // lock the bucket and re-check
    for(int i=0; i < b->_count; i++) 
    { 
        bfpid_t pid = b->_slots[i]->pid();

        // find an alternative location for slot[i] 
        // and hope that it can take the contents of slot[i]
        for(int j=0; j < HASH_COUNT; j++) 
        {
            //  alternative location index : alt
            int alt = hash(j,pid);
            //  skip this hash if it's the one that
            //  puts us in this bucket (b)
            if(alt != h) 
            {
                // Store the alternative location tried 
                // for use later, in case we don't succeed here.
                // (To prevent recursive calls favoring
                // the last-attempted hash, we store *all* hashes
                // tried.)
                alternatives[alts_index].altbucket = alt;
                alternatives[alts_index].hashfunc = j;
                alternatives[alts_index++].slotindex = i;

                // try to move b->slots[i] to its alternative location
                if(cuckold(&_table[alt], b, i, j, moved)) {
                    return true;
                }

                // break; <--- removed:  Keep going ... HASH_COUNT could be > 2
                // and we want to exhaust all possibilities
                //
                if(++limit > BMAX) {
                    ADD_BFSTAT(_limit_exceeds, 1);
                    // Hit our limit.  See note below
                    return false;
                }
            }
        }
    }

    // Unable to make room by moving a single slot resident to
    // one of its alternatives.  Now we have to make room at
    // an alternative location.  Which alternative to use?
    // Pick a slot index at "random".  Well, actually,
    // we'll try them all in "random" order.
    int idx[NALTERNATIVES];
    for(int i=0; i < NALTERNATIVES; i++) idx[i] = i;

    std::random_shuffle(idx,idx+NALTERNATIVES, bf_core_m::htab::rand);

    for(int a=0; a < NALTERNATIVES; a++) 
    {
        // See if we can make space in the alternative bucket
        int alti = idx[a];
        // For this alternative, restor the slot index and bucket index
        int alt  = alternatives[alti].altbucket;
        int i    = alternatives[alti].slotindex;
        int j    = alternatives[alti].hashfunc;

        // Try to make space in the alternative bucket
        // and if that works, try again to 
        if( ensure_space( alt, limit, moved, save_pid_for_debug) ) 
        {
            if( cuckold( &_table[alt], b, i, j, moved) ) {
                CHECK_ENTRY(h, false);
                return true;
            }
        }
        // If ensure_space failed because it hit the limit,
        // notice that ...
        if(limit > BMAX) {
            // Hit our limit.  See note below
            break;
        }
    }
    // Tried every alternative or hit our limit. Could be
    // a cycle or a too-full _table (bad planning).
    // failure: will have to take drastic action.
    CHECK_ENTRY(h, false);
    return false;
}

/*
 * Insert key t in the _table. Assumes it's not already there.
 * Returns frame control block of someone moved, if any.
 * Detects need to rebuild table.
 */

bfcb_t* bf_core_m::htab::insert(bfcb_t* t)
{
    ADD_BFSTAT(_insertions, 1);
    // this hash_func value distinguishes those that
    // are in the HT from those that are not.
    // We assume that the client (buffer pool manager)
    // is doing things right and never inserting a frame
    // that's already in the table.
    t->set_hash_func(HASH_COUNT);

    bfcb_t *moved(NULL);

    bool ok = _insert(t, moved);
    if(!ok) 
    {
        // TODO: rebuild: filed  GNATS 47
        W_FATAL_MSG(fcINTERNAL, << "Must rebuild the hash table");
    }

    return moved;
}

/*
 * This is for updating _max_limit
 * If needed elsewhere, put somewhere more accessible.
 */
static void atomic_max_32(volatile int *target, int arg)
{
    int orig=*target;
    int larger;
    do {
        larger = (arg > orig) ? arg : orig;
        // Atomically replace with larger if it hasn't
        // changed in the meantime.
    } while (!lintel::unsafe::atomic_compare_exchange_strong(const_cast<int*>(target), &orig,  larger));
}

/*
 * insert key t in the _table. Assumes it's not already there.
 * true -> success
 * false -> failure, and we'll have to take drastic measures,
 * reconsruct the _table, ....
 */
bool bf_core_m::htab::_insert(bfcb_t* t, bfcb_t* &moved)
{
    CHECK_TABLE();
    bucket* b[HASH_COUNT];
    int     hashes[HASH_COUNT];

    // First, try all slots in all bucket alternatives. 
    for(int i=0; i < HASH_COUNT; i++) 
    {
        int h = hashes[i] = hash(i,t->pid());
        bucket* bb = b[i] = &_table[h];

        ADD_BFSTAT(_slots_tried, 1);
        if(bb->_count < SLOT_COUNT) 
        {
            CRITICAL_SECTION(cs, bb->_lock);
            if(bb->_count < SLOT_COUNT) {
                // there was space. insert and return
                bb->_slots[bb->_count++] = t;

                t->set_hash(h);
                t->set_hash_func(i);
                w_assert2(bb->_lock.is_mine());
                cs.exit();
                w_assert2(bb->_lock.is_mine()==false);
                CHECK_ENTRY(i, false);
                return true;
            }
        }
    }

#if W_DEBUG_LEVEL > 4
    // This is to debug the case of HASH_COUNT==2
    // in the case of only 2 hash funcs, this says that there is only
    // one place where I can stuff this page.  So I have to try
    // to ensure_space on it.
    for(int q=0; q < HASH_COUNT-1; q++)
    {
        if(hashes[q] == hashes[q+1]) {
            ADD_BFSTAT(_hash_collisions, 1);
        }
    }
#endif

    bfpid_t save_pid_for_debug = t->pid();
    // No room in any _slots.  We have to move something
    // from one slot to (one of) its alternative location(s)
    // and then try again to insert.
    int limit=0;
    for(int i=0; i < HASH_COUNT; i++) 
    {
        int h = hashes[i]; // cached in the loop above

        // Try to make space in location b[i]; if it fails,
        // go on to try other alternatives.
        if(ensure_space(h, limit, moved, save_pid_for_debug)) 
        {
            bucket* balt = b[i];  // cached in loop above

            ADD_BFSTAT(_slots_tried, 1);  
            CRITICAL_SECTION(cs, balt->_lock);
            if(balt->_count < SLOT_COUNT) 
            {
                // there was space. insert and return
                balt->_slots[balt->_count++] = t;

                t->set_hash(hashes[i]);
                t->set_hash_func(i);
                w_assert2(balt->_lock.is_mine());
                cs.exit();
                w_assert2(balt->_lock.is_mine()==false);
                CHECK_TABLE();

                // "limit" increases when we tried to move something
                // to an alternate location and it failed, and we must
                // either
                // try to move to a different alternate location 
                // or
                // try to move a different slot entry 
                if(limit > 0) {
                    ADD_BFSTAT(_slow_inserts, 1);
                    // Exhausted the alternatives or we hit our depth limit.
                    // Right now we don't distinguish between these
                    // two cases.
                    atomic_max_32(&_max_limit, limit); 

                }
                return true;
            }
        }
    }
    CHECK_TABLE();

    if(limit > 1) {
        ADD_BFSTAT(_slow_inserts, 1);
        // Exhausted the alternatives or we hit our depth limit.
        // Right now we don't distinguish.
        atomic_max_32(&_max_limit, limit); 
    }
    W_FATAL_MSG(fcINTERNAL, << "Must rebuild the hash table");
    return false;
}


// GNATS 35: item could be moved from bucket while we
// are searching, but not from 1 slot to another. 
bfcb_t *bf_core_m::htab::lookup(bfpid_t const &pid) const
{
    bfcb_t* p = NULL;
    ADD_BFSTAT(_lookups, 1);

	/* NOTE: 
	 * I ran the kits  without the cheap lookup first.  Results are mixed; 
	 * some things ran faster with, some without, some scaled
	 * better with, some without. On the whole, the raw performance
	 * was slightly worse. 
	 *
	 * With the cheap lookup first, there is likely less
	 * contention on the bucket locks when the item is found in the htab.
	 *
	 * When the item is in the htab & not moving, 
	 * the critical path is shorter with the cheap lookup first, 
	 * but it is longer when the item is not in the htab
	 * or is moved to a different bucket while the lookup is going on.
	 * 
	 * With the kits, most of the lookups are successful, and I haven't
	 * seen any instances of the unlikely case of moving during
	 * lookup.
	 *
	 * With only 2 hash functions, it's only 2 bucket locks and 2 items
	 * to sort with the slow lookup, so it's not surprising that the
	 * differences aren't significant.
	 *
	 * WARNING: if you change this, you will get different results with
	 * the htab unit tests in sm/tests.
	 */
#define USE_CHEAP_LOOKUP_FIRST
#ifdef USE_CHEAP_LOOKUP_FIRST

    for(int i=0; i < HASH_COUNT && !p; i++) 
    {
        int idx = hash(i, pid);

        bucket &b = _table[idx];

        // Go through b._count slots in bucket b
        for(int s=0; s < b._count && !p; s++) 
        {
            p = b._slots[s];

            if(!p) {
                ADD_BFSTAT(_probe_empty, 1);
                continue; // empty bucket
            }
            ADD_BFSTAT(_probes, 1);

            
            if(p->pin_frame_if_pinned()) 
            {
                // we didn't need to lock the bucket because the frame
                // was pinned anyway. Is it the right frame, though?
                if(p->pid() != pid) {
                    // oops! got the wrong one
                    p->unpin_frame();
                    p = NULL;
                }
                // we incremented the pin count.
            }
            else {
                // we need to acquire the bucket lock before grabbing
                // a free latch. The nice part is we know the latch
                // acquire will (usually) succeed immediately...
                w_assert2(b._lock.is_mine()==false);
                CRITICAL_SECTION(cs, b._lock);
                w_assert2(b._lock.is_mine());
                p = b._slots[s]; // in case we raced...
                
                if(p != NULL && p->pid() == pid) 
                    p->pin_frame();
                else
                    p = NULL;
            }
        } // we drop out if p is non-null
    } // we drop out of p is non-null
#endif

    if(!p) p = _lookup_harsh(pid); // workaround for GNATS 35
    if(!p) ADD_BFSTAT(_lookups_failed, 1);
    return p;
}

// If we fail to find the page the first time around,
// we'll do another look in case it moved while we were looking.
// This is slower but safe b/c we lock *all* the possible buckets
// before we search them for the frame we seek.
bfcb_t *bf_core_m::htab::_lookup_harsh(bfpid_t const &pid) const
{
    ADD_BFSTAT(_harsh_lookups, 1);

    int hash_count = HASH_COUNT;

    // pre-hash into this array
    int idx[HASH_COUNT];

    for(int i=0; i < HASH_COUNT; i++) {
        idx[i] = hash(i, pid);
        w_assert1(idx[i] < _size);
        w_assert1(idx[i] >= 0);
    }

    // sort the indices. This can be a dumb sort since
    // HASH_COUNT is going to be 2 or 3, no more.
    {
        bool changed=true;
        do {
            changed=false;
            for(int i=0; i < (HASH_COUNT-1); i++) {
                int j = i+1;
                if(idx[i] == idx[j]) {
                    // had better not be equal, but it could
                    // be if the hash functions are lousy
                    /*
                    fprintf(stderr, "Lousy hash funcs for pid %d: %d,%d\n",
                            pid.page, idx[i], idx[j]);
                    */
                    ADD_BFSTAT(_hash_collisions, 1);
                    idx[j] = _size; // illegitimate #
                    hash_count--;
                    w_assert1(hash_count > 0);
                }
                else if( idx[i] > idx[j]) {
                    // swap
                    int tmp = idx[i];
                    idx[i] = idx[j];
                    idx[j] = tmp;
                    changed = true;
                }
                w_assert1(idx[i] <= idx[j]);
            }
        } while(changed);
    }

    bucket *b[HASH_COUNT];
    
    // hash_count is the number of unique hashes,
    // less than HASH_COUNT if two or more hashes yield the
    // same value.  Must detect collisions so we don't
    // try to double-acquire the bucket locks.
    for(int i=0; i < hash_count; i++) {
        b[i]   = &_table[idx[i]];
        w_assert2(i==0 || (b[i] >= b[i-1]));
        // Acquire the mutexes in given order.
        w_assert1(b[i]->_lock.is_mine()==false);
        w_assert1(b[i]!=NULL);
        b[i]->_lock.acquire();
    }

    // Now for each bucket, check all the slots in the
    // bucket to see if the item is there.
    bfcb_t* result = NULL;
    for(int i=0; (i < hash_count) && !result; i++) 
    {
        w_assert1(b[i]->_count <= SLOT_COUNT);
        for(int s=0; (s < b[i]->_count) && !result; s++) 
        {
            bfcb_t* p = b[i]->_slots[s];
            ADD_BFSTAT(_harsh_probes, 1);
            if(p != NULL && p->pid() == pid)  {
                // found. Pin it.
                result = p; result->pin_frame();
            }
        }
    }

    for(int i=0; i < hash_count; i++) 
    {
        // Release all the mutexes 
        w_assert1(b[i]->_lock.is_mine()==true);
        b[i]->_lock.release();
    }
    return result;
}

bool bf_core_m::htab::remove(bfcb_t* p) 
{
    w_assert2(p->hash_func() != HASH_COUNT);
    w_assert2(hash(p->hash_func(), p->pid()) == unsigned(p->hash())); 
    bucket &b = _table[p->hash()];

    // don't hold lock: programming error
    // pin count > 0 : don't remove yet
    if((b._lock.is_mine()==false) || (p->pin_cnt() > 0)) return false;

    ADD_BFSTAT(_removes, 1);
    w_assert1(b._lock.is_mine());

    for(int i=0; i < b._count; i++)
    {
        if(b._slots[i] == p) 
        {
            p->set_hash_func(HASH_COUNT);
            b._slots[i] = NULL;
            b._count--;
            // Compress the slots array while we have the lock
            for(int j=i; j < b._count; j++) {
                b._slots[j] = b._slots[j+1];
            }
            // don't release lock
            CHECK_ENTRY(i, true /* have lock */);

            return true;
        }
    }
    CHECK_TABLE();
    // didn't find it in this bucket: don't remove
    return false;
}


/*
 * For debugging/unit-testing ONLY
 * Look up by pid, not by frame
 */
bool bf_core_m::htab::_lookup(const lpid_t &pid) const
{
    static int const COUNT = htab::HASH_COUNT;
    int idx;

    for(int i=0; i < COUNT; i++) 
    {
        idx = _htab->hash(i, pid);
        bucket &b = _htab->_table[idx];

        for(int j=0; j < b._count; j++) 
        {
        // NOT THREAD-SAFE
        // if(b._lock.try_lock()) {
            if(b._slots[j] && b._slots[j]->pid() == pid) 
            {
                // w_assert1( _in_htab(b._slots[j]) );
                // b._lock.release();
                return true;
            }
        // }
        }
    }

    return false;
}

#if W_DEBUG_LEVEL > 4
/* this has been never implemented
int
bf_core_m::htab::check_table() const
{
    int sum=0;
    for(int i=0; i < _size; i++) sum += check(i, false);
    return sum;
}

int
bf_core_m::htab::check_entry(int bki, bool have_lock) const
{
    bucket &b = _table[bki];

    w_assert1(b._count <= SLOT_COUNT);
    int hashes[HASH_COUNT];
    for(int s=0; s < b._count; s++)
    {
        bfcb_t *cb = b._slots[s];
        w_assert1(cb != NULL);
        if(!have_lock) w_assert2(b._lock.is_mine() == false);

        bfpid_t pid = cb->pid;
        // One of the hashes had better match
        // compute all the hashes
        for(int j=0; j< HASH_COUNT; j++) {
            hashes[j] = hash(j,pid);
        }
        w_assert1(cb->hash_func != HASH_COUNT);
        w_assert1(hashes[cb->hash_func] == bki);
        w_assert1(cb->hash == bki);
        w_assert1(cb->pin_cnt == 0);

        // Make sure lookup works
        // the problem here is the holding of the lock
        // bfcb_t *tmp = lookup(pid);
        // w_assert1(tmp == cb);
        if(cb->pin_cnt >0) cb->pin_cnt = 0;
    }
    return b._count;
}
*/
#endif


void                        
bf_core_m::htab::stats(bf_htab_stats_t &out) const
{
    *(&out.bf_htab_bucket_size) = sizeof(bucket);
    *(&out.bf_htab_table_size) = sizeof(bucket) * _size;
    *(&out.bf_htab_buckets) =  _size;
    *(&out.bf_htab_slot_count) =  SLOT_COUNT;
    *(&out.bf_htab_max_limit) =  _max_limit;
}
