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

// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='BF_CORE_H'>

 $Id: bf_htab.h,v 1.3 2010/07/01 00:08:19 nhall Exp $

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

#ifndef BF_HTAB_H
#define BF_HTAB_H

/*
 *
 * Cuckoo-hashing-based hash table for the buffer pool manager.
 * 
 * Each (direct)bucket in the table has SLOT_COUNT slots (compile-time
 * parameter).  With the proper hash functions and #slots/bucket, 
 * it should be rare that a(n) (extended)bucket includes more than 
 * one table bucket, i.e., that an item has to move to an alternative
 * location.  That's the theory, anyway.  In practice, as with all
 * hashing, it depends on the hash functions.  With a single volume
 * and integer page #s in a small range (e.g., 0->10K) this is not
 * necessarily the case, and our unit-testing shows that collisions 
 * happen enough that we need to have SLOT_COUNT >2.
 *
 * The hash table insert fails to insert if it reaches a 
 * limit: the # of moves that it has to perform to get an item to fit,
 * or the number of times it looks for an empty slot.  (These could
 * be different if we had cycles, in which case it might not actually
 * move something, but cycles through table buckets looking for
 * space and not finding that it can move anything.) These two limits
 * are combined into one limit here: BMAX (compile-time constant)
 *
 * \bug bf_core_m::htab
 * GNATS 47 :
 * In event of failure, the hash table will have to be rebuilt with
 * different hash functions, or will have to be modified in some way, and
 * at this writing, we haven't handled this case. 
 *
 *
 * NOTE: re: unit-testing.  The tests/ directory has a test called 
 * htab, which can be used to exercise the htab in various ways to
 * see its behavior with different buffer sizes and page ranges.
 * It is a single-threaded, contrived scenario; and (as we hope)
 * it might be the only way you can force a failure case to test
 * the handling of that case. 
 */
#include <algorithm>

class bf_core_m::htab 
{
#ifdef HTAB_UNIT_TEST_C
    // NOTE: these static funcs are NOT thread-safe; they are
    // for unit-testing only.  
    friend bfcb_t* htab_lookup(bf_core_m *, bfpid_t const &pid, Tstats &) ;
    friend bfcb_t* htab_insert(bf_core_m *, bfpid_t const &pid, Tstats &);
    friend  bool   htab_remove(bf_core_m *, bfpid_t const &pid, Tstats &) ;
    friend  void   htab_dumplocks(bf_core_m*);
    friend  void   htab_count(bf_core_m *core, int &frames, int &slots);
#endif
    friend struct bf_core_m::init_thread_t ;
    friend class bf_core_m;
protected:
    static const int HASH_COUNT = 2;
private:
    static const int SLOT_COUNT = 3;
    // Maximum number of buckets we'll visit in search of room for 
    // a slot insert. Start with recursion-depth 10.  
    // I have no idea what's a good limit...
    // Probably with large _tables, the potential bucket length or 
    // recursion depth should be short/shallow, and with larger 
    // SLOT_COUNT, it should be shallow,
    // so this formula is warped, but for now I want it to allow for checking
    // all alternatives for each slot at least once.
    static const int  BMAX = (SLOT_COUNT-1) * (HASH_COUNT-1) * 10; 


    uint32_t    _hash_seeds[HASH_COUNT];
    int      _size;
    int      _avg;

public: // grot: this should be protected
    class bucket {
    public:
        // According to Ryan, no noticable contention on this, so
        // (fast/not-scalable) tatas locks seem to be ok. 
        tatas_lock            _lock;
        bfcb_t* volatile      _slots[SLOT_COUNT];
        int                   _count;
        NORET               bucket() : _count(0) { 
                                for(int i=0; i<SLOT_COUNT; i++) _slots[i]=0;
                            }

        bfcb_t *get_frame(bfpid_t pid) { 
            for(int i=0; i < _count; i++) {
                bfcb_t *p = _slots[i];
                if(p && p->pid() == pid) return p;
            }
            return NULL;
        }
    };

    bucket* _table;
public:
#define ADD_BFSTAT(x,w) me()->TL_stats().bfht.bf_htab##x++
    mutable int _max_limit; // max depth descended

private:
    bool       cuckold(bucket* dest, bucket* src, int which, int hashfunc,
                        bfcb_t* &moved) ;
    bool       ensure_space(int h, int &limit, bfcb_t *&moved, const bfpid_t
                        &save_pid_for_debug) ;
    static int rand(int n) { return me()->randn(n); }
    bool       _insert(bfcb_t* p, bfcb_t *&moved);

#if W_DEBUG_LEVEL > 4
/*
    // This is extremely costly.  Probably should
    // use it only to run unit tests.  
    int check_table() const;
    int check_entry(int i, bool have_lock) const;
#define CHECK_TABLE() check_table()
#define CHECK_ENTRY(i,j) check_entry(i,j)
*/
#define CHECK_TABLE() 
#define CHECK_ENTRY(i,j)
#else
#define CHECK_TABLE() 
#define CHECK_ENTRY(i,j)

#endif

public:
    NORET   htab(int i) : _size(i) , _avg(0), _max_limit(0)
    {
        ::srand (4344); // for repeatability
        for (int j = 0; j < HASH_COUNT; ++j) {
            _hash_seeds[j] = ((uint32_t) ::rand() << 16) + ::rand();
        }
        _table = new bucket[i];
    }

    NORET   ~htab()     { delete[] _table; } 
    // Copy some stats out to the caller's structure
    void   stats(bf_htab_stats_t &) const;

    bfcb_t *lookup(bfpid_t const &pid) const;
    bfcb_t *_lookup_harsh(bfpid_t const &pid) const; 
    bool   _lookup(const lpid_t &pid) const; // for unit-testing only

    // For now, let's leave the evict test in place; we might
    // still want to use it, probably as an argument to ensure_space
    bfcb_t* insert(bfcb_t* p);
    bool   remove(bfcb_t *p);

    uint32_t hash(int h, bfpid_t const &pid) const {
        w_assert1(h >= 0 && h < HASH_COUNT);
        uint64_t x = ((uint64_t)pid.vol() << 32) + pid.page;
        uint32_t ret = w_hashing::uhash::hash64(_hash_seeds[h], x) % (uint) _size;
        w_assert1(ret < (uint) _size);
        return ret;
    }

    void print_histo() const;
    void print_holders() const;
    void print_occupants() const;

};

#endif /* BF_HTAB_H */
#endif // COMMENTED_OUT
