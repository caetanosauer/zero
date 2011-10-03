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

 $Id: bf_htab_test.cpp,v 1.3 2010/06/08 22:28:55 nhall Exp $

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

/*
 * These functions are for use by the unit-tester of the
 * bf_core hash table.  Since they are not meant to be invoked by
 * any "real" sm client, they are put into a separate object file
 * so they can be bypassed by the linker.
 */

#include <stdio.h>
#include <cstdlib>
#include "sm_int_0.h"

#define   HTAB_UNIT_TEST_C
#include "bf_s.h"
#include "bf_core.h"
#include "w_hashing.h"
#include "bf_htab.h"

/* 
 * Support for HTAB_UNIT_TEST_C (sm/tests)
 *
 * From bf_core.cpp:
 * #ifdef HTAB_UNIT_TEST_C
 * // NOTE: these static funcs are NOT thread-safe; they are
 * // for unit-testing only.  
 * friend bfcb_t* htab_lookup(bf_core_m *, bfpid_t const &pid, Tstats &) ;
 * friend bfcb_t* htab_insert(bf_core_m *, bfpid_t const &pid, bool, Tstats &);
 * friend bool    htab_remove(bf_core_m *, bfpid_t const &pid, Tstats &) ;
 * friend void htab_dumplocks();
 * #endif
 * 
 */
void htab_dumplocks(bf_core_m *core)
{
    int sz= core->_htab->_size;

    for(int i=0; i < sz; i++)
    {
    bf_core_m::htab::bucket &b = core->_htab->_table[i];
    if(b._lock.is_mine())
    {
         cerr << "bucket " << i << " held" << endl;
    }
    }
}

void htab_count(bf_core_m *core, int &frames, int &slots)
{
    slots = 0;
    int sz= core->_htab->_size;
    for(int i=0; i < sz; i++)
    {
    bf_core_m::htab::bucket &b = core->_htab->_table[i];
    slots += b._count;
    }
    frames = core->_num_bufs;
}

bfcb_t* htab_lookup(bf_core_m *core, bfpid_t const &pid,
    bf_core_m::Tstats &s) 
{
    bfcb_t *ret  = core->_htab->lookup(pid);

    s = me()->TL_stats().bfht;
    return ret;
}


bfcb_t* htab_insert(bf_core_m *core, bfpid_t const &pid, bf_core_m::Tstats &s) 
{
    // avoid double-insertions w/o a removal.
    bool already_there(false);

    bfcb_t *ret  = core->_htab->lookup(pid);
    if(ret) {
        already_there = true;
        htab_remove(core, pid, s);
    }

    bfcb_t *ret2 = core->_htab->lookup(pid);
    w_assert0(ret2 == NULL);

    bfcb_t *cb ;
    if(already_there) {
        cb = ret;
    }
    else
    {
        ret = NULL;
        cb = core->replacement();
        w_assert0(cb->latch.is_mine());
        cb->latch.latch_release();
    }
    if(cb == NULL) {
        cerr << " htab_insert could not get a replacement frame "
        << endl;
    }

    if(cb) {
        if(cb->old_pid_valid()) { 
            // it's a replacement
            // ... obsolete check removed..
        }
        cb->set_pid(pid);
        cb->zero_pin_cnt();

        ret  = core->_htab->insert(cb);


        s = me()->TL_stats().bfht;
    }

#if W_DEBUG_LEVEL > 1
    int sz= core->_htab->_size;
    for(int i=0; i < sz; i++)
    {
        bf_core_m::htab::bucket &b = core->_htab->_table[i];
        w_assert2(b._lock.is_mine()==false);
    }
#endif

    return ret;
}

bool htab_remove(bf_core_m *core, bfpid_t const &pid, bf_core_m::Tstats &s) 
{
    bool ret(false);
    bfcb_t *cb  = core->_htab->lookup(pid);

    if(cb) {
        // find the bucket so we can acquire the lock,
        // necessary for removal.
        // also ensure pin count is zero.
        int idx = core->_htab->hash(cb->hash_func(), pid);
        bf_core_m::htab::bucket &b = core->_htab->_table[idx];
        cb->zero_pin_cnt();
        CRITICAL_SECTION(cs, b._lock);

        bool bull = core->_htab->remove(cb);
        w_assert0(bull);
        w_assert1(cb->pin_cnt() == 0);
    }

    // It's possible that it couldn't remove the item
    // because the lock is not held or the pin count is > 0
    if(ret) {
        w_assert2(cb->hash_func() == bf_core_m::htab::HASH_COUNT);
    }

    s = me()->TL_stats().bfht;
    return ret;
}

