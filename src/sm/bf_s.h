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

/*<std-header orig-src='shore' incl-file-exclusion='BF_S_H'>

 $Id: bf_s.h,v 1.49 2010/12/17 19:36:26 nhall Exp $

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

#ifndef BF_S_H
#define BF_S_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <sm_s.h>
#include "w_hashing.h"

class page_s;

const uint32_t BFPID_T_HASH_SEED = 0xAC046148;

class bfpid_t : public lpid_t {
public:
    NORET            bfpid_t();
    NORET            bfpid_t(const lpid_t& p);
    bfpid_t&         operator=(const lpid_t& p);
    bool             operator==(const bfpid_t& p) const;
    static const bfpid_t    null;
public:
    uint32_t hash() const {
        return  w_hashing::uhash::hash32(BFPID_T_HASH_SEED, ((uint64_t) vol() << 32) + page);
    }
};

inline NORET
bfpid_t::bfpid_t()
{
}

inline NORET
bfpid_t::bfpid_t(const lpid_t& p) : lpid_t(p)
{
}

inline bfpid_t&
bfpid_t::operator=(const lpid_t& p)  
{
    *(lpid_t*)this = p;
    return *this;
}

inline bool 
bfpid_t::operator==(const bfpid_t& p) const
{
    return vol() == p.vol() && page == p.page;
}


/*
 *  bfcb_t: buffer frame control block.
 */
class bfcb_t {
    friend class bfcb_unused_list;
private:
    int32_t _pin_cnt;        // count of pins on the page
    uint32_t _store_flags; // a copy of what's in the page, alas
    bfpid_t     _pid;            // page currently stored in the frame
    bfpid_t     _old_pid;        // previous page in the frame
    bool        _old_pid_valid;  // is the previous page in-transit-out?
    page_s*     _frame;          // pointer to the frame

    bool        _dirty;  // true if page is dirty
    lsn_t       _rec_lsn;        // recovery lsn
    lsn_t        _old_rec_lsn;   // valid if the page is being cleaned

    bfcb_t*     _next_free;    // used by the (singly-linked) freelist
    
    std::list<int32_t> *_write_order_dependencies; // used to check write order dependency
    std::list<int32_t> *_wod_back_pointers; // doubly linked list to remove _write_order_dependencies when this block is written out

    int32_t _refbit;// ref count (for strict clock algorithm)
                // for replacement policy only

    int32_t _hotbit;// copy of refbit 
                // for use by the cleaner algorithm
                // without interfering with clock (replacement)
                // algorithm.

    int32_t       _hash_func; // which hash function was this frame placed with?
    int32_t _hash;        // and what was the hash value?
public:
    latch_t     latch;          // latch on the frame

public:
    NORET       bfcb_t()    {};
    NORET       ~bfcb_t()   {};

    void           vtable_collect(vtable_row_t &t);
    static void    vtable_collect_names(vtable_row_t&);

    void    clear_bfcb();

    const page_s *frame() const { return _frame; }
    page_s *frame_nonconst() { return _frame; }
    void  set_storeflags(uint32_t f);
    uint32_t  read_page_storeflags();
    uint32_t  get_storeflags() const;
    uint32_t  get_storeflags_nocheck() const { return _store_flags; }
    std::list<int32_t>& write_order_dependencies() { return *_write_order_dependencies; }
    std::list<int32_t>& wod_back_pointers() { return *_wod_back_pointers; }

    const bfpid_t &pid() const  { return _pid;   }
    void  set_pid(const bfpid_t &p) { _pid = p;   }

    bool  old_pid_valid() const  { return _old_pid_valid;  }
    const bfpid_t &old_pid() const  { return _old_pid;   }
    void  clr_old_pid() { _old_pid = lpid_t::null; _old_pid_valid=false;  }
    void  set_old_pid() { _old_pid = pid();  _old_pid_valid=true; }

    bool  dirty() const  { return _dirty;  }
    void  set_dirty_bit() { _dirty = true;  }

    lsn_t safe_rec_lsn() const {
        /* Work with a copy of _old_rec_lsn in case the original gets
           cleared while we're deciding whether to return it.

           We don't care if we get a stale rec_lsn (recovery would
           just take slightly longer that way), but we can't afford to
           return lsn_t::null if _rec_lsn is valid.

           If that's the case, we have to return an lsn rather than
           a reference to an lsn.
         */
        lsn_t old = _old_rec_lsn;
        return old.valid()? old : _rec_lsn;
    }
    
    const lsn_t&  curr_rec_lsn() const  { return _rec_lsn;  }
    const lsn_t&  old_rec_lsn() const { return _old_rec_lsn; }
    void  set_rec_lsn(const lsn_t &what);
    void  save_rec_lsn() { _old_rec_lsn = _rec_lsn; }
    void  clr_old_rec_lsn() { _old_rec_lsn = lsn_t::null; }

    // clears the dirty bit and the rec_lsn
    // NOTE: it must happen in that order to avoid races!
    void  mark_clean();
    void  mark_clean_dependencies();

    int32_t      refbit() const { return _refbit; }
    void        set_refbit(uint32_t b) { _refbit=b; }
    void        decr_refbit() { if(--_refbit < 0) _refbit = 0; }

    // NOTE: hotbit is an approximation so we don't want to use
    // atomic operations.  Let's just allow these operations to be
    // racy and try to cope with the bit going < 0 via two cleaners
    // decrementing simulatneously.
    int32_t      hotbit() const { return _hotbit; }
    uint32_t     set_hotbit(int32_t b) { return (_hotbit = b); }
    void        decr_hotbit() { if(--_hotbit < 0) _hotbit = 0; }

    void        update_rec_lsn(latch_mode_t, bool check);

    void        initialize(
                        page_s*           _bufpool,
                        uint32_t           hashfunc
                        );
    void        destroy();
    int32_t       hash_func() const { return _hash_func; }
    void         set_hash_func(int32_t h) { _hash_func=h; }
    int32_t       hash() const { return _hash;}
    void         set_hash(int32_t h) { _hash=h;}


public:
    inline ostream&    print_frame(ostream& o, bool in_htab);
    void         unpin_frame();
    void         pin_frame();
    bool         pin_frame_if_pinned();
#if W_DEBUG_LEVEL > 1
    void         check() const;
#else
    void         check() const {}  // no-op
#endif

    // I'm making _pin_cnt private just so I can be sure all updates
    // are through the right methods.
    void                zero_pin_cnt() { _pin_cnt=0; }
    int32_t              pin_cnt() const { return _pin_cnt; }

    // is_hot: is someone waiting for the latch?
    // NOTE: this is somewhat racy, in that it returns false negatives
    //  but if when it returns true, there is or just was a hot state
    //  on this page.    This is only used now for debugging purposes.
    bool                is_hot() const { return 
                                    latch.latch_cnt() < _pin_cnt; }
private:

    // disabled
    NORET       bfcb_t(const bfcb_t&);
    bfcb_t&     operator=(const bfcb_t&);
};

/*<std-footer incl-file-exclusion='BF_S_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
#endif // COMMENTED_OUT
