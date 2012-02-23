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

/*<std-header orig-src='shore' incl-file-exclusion='PAGE_H'>

 $Id: page.h,v 1.122 2010/11/08 15:06:55 nhall Exp $

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

#ifndef PAGE_H
#define PAGE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

class stnode_p;
class alloc_p;

#ifdef __GNUG__
#pragma interface
#endif

/**
 *  Basic page handle class. This class is used to fix a page
 *  and operate on it.
 */
class page_p : public smlevel_0 
{

friend class dir_vol_m;  // for access to page_p::splice();

protected:
    typedef page_s::slot_offset_t slot_offset_t;
    typedef page_s::slot_length_t slot_length_t;

public:
    enum {
        data_sz = page_s::data_sz,
        hdr_sz = page_s::hdr_sz,
        slot_sz = page_s::slot_sz
    };
    enum logical_operation {
        l_none=0,
        l_set, // same as a 1-byte splice
        l_or,
        l_and,
        l_xor,
        l_not
    };
    enum tag_t {
        t_bad_p            = 0,        // not used
        t_alloc_p        = 1,        // free-page allocation page 
        t_stnode_p         = 2,        // store node page
        t_btree_p          = 5,        // btree page 
        t_any_p            = 11        // indifferent
    };
    enum page_flag_t {
        t_tobedeleted    = 0x01,        // this page will be deleted as soon as the page is evicted from bufferpool
        t_virgin         = 0x02,        // newly allocated page
        t_written        = 0x08        // read in from disk
    };

    // override these in derived classes.
    virtual tag_t get_page_tag () const { return t_any_p; }
    virtual int get_default_refbit () const { return 1; }
    virtual void inc_fix_cnt_stat () const { INC_TSTAT(any_p_fix_cnt);}

    // initialization functions
    rc_t fix(const lpid_t& pid, latch_mode_t mode,
        uint32_t page_flags = 0, store_flag_t store_flags = st_bad,
        bool ignore_store_id = false){
        return _fix(false, pid, mode, page_flags, store_flags, ignore_store_id, get_default_refbit());
    }
    rc_t conditional_fix(const lpid_t& pid, latch_mode_t mode,
        uint32_t page_flags = 0, store_flag_t store_flags = st_bad, bool ignore_store_id = false){
        return _fix(true, pid, mode, page_flags, store_flags, ignore_store_id, get_default_refbit());
    }
    rc_t _fix(bool condl, const lpid_t& pid, latch_mode_t mode,
        uint32_t page_flags, store_flag_t store_flags,
        bool ignore_store_id, int refbit)
    {
        inc_fix_cnt_stat();
        store_flag_t store_flags_save = store_flags;
        w_assert2((page_flags & ~t_virgin) == 0);
        W_DO( _fix_core(condl, pid, get_page_tag(), mode, page_flags, store_flags,
                                                        ignore_store_id,refbit));
        if (page_flags & t_virgin)   W_DO(format(pid, get_page_tag(), page_flags,
                                                        store_flags_save));
        w_assert3(tag() == get_page_tag());
        return RCOK;
    } 
    virtual rc_t format(const lpid_t& pid, tag_t tag, uint32_t page_flags,
             store_flag_t store_flags) {
        return _format(pid, tag, page_flags, store_flags);
    }


    /** Return the tag name of enum tag. For debugging purposes. */
    static const char*          tag_name(tag_t t);
    
    const lsn_t&                lsn() const;
    void                        set_lsns(const lsn_t& lsn);

    /**
    *  Ensures the buffer pool's rec_lsn for this page is no larger than
    *  the page lsn. If not repaired, the faulty rec_lsn can lead to
    *  recovery errors. Invalid rec_lsn can occur when
    *  1) there are unlogged updates to a page -- log redo, for instance, or updates to a tmp page.
    *  2) when a clean page is fixed in EX mode but an update is never
    *  made and the page is unfixed.  Now the rec_lsn reflects the tail
    *  end of the log but the lsn on the page reflects something earlier.
    *  At least in this case, we should expect the bfcb_t to say the
    *  page isn't dirty.
    *  3) when a st_tmp page is fixed in EX mode and an update is 
    *  made and the page is unfixed.  Now the rec_lsn reflects the tail
    *  end of the log but the lsn on the page reflects something earlier.
    *  In this case, the page IS dirty.
    *
    *  FRJ: I don't see any evidence that this function is actually
    *  called because of (3) above...
    */
    void  repair_rec_lsn(bool was_dirty,  lsn_t const &new_rlsn);

    const lpid_t&               pid() const;
    shpid_t                     btree_root() const { return _pp->btree_root;}

    // used when page is first read from disk
    void                        set_vid(vid_t vid);

    smsize_t                    used_space()  const;
    // Total usable space on page
    smsize_t                     usable_space()  const;
    
    /** Return true if the page is pinned by this thread (me()). */
    bool                         pinned_by_me() const;
    
    /** Reserve this page to be deleted when bufferpool evicts this page. */
    rc_t                         set_tobedeleted (bool log_it);
    /** Unset the deletion flag. This is only used by UNDO, so no logging. and no failure possible. */
    void                         unset_tobedeleted ();

    slotid_t                     nslots() const;
    smsize_t                     tuple_size(slotid_t idx) const;
    slot_offset_t                tuple_offset(slotid_t idx) const;
    void*                        tuple_addr(slotid_t idx) const;
    void                         overwrite_slot (slotid_t idx, slot_offset_t offset, slot_length_t length);
    bool                         is_ghost_record(slotid_t idx) const;

    /**
     * Expands an existing record for given size.
     * Caller should make sure there is enough space to expand.
     */
    void                         expand_rec(slotid_t idx, slot_length_t rec_len);
    
    /**
     * Mark the given slot to be a ghost record.
     * If the record is already a ghost, does nothing.
     * This is used by delete and insert (UNDO).
     * This function itself does NOT log, so the caller is responsible for it.
     * @see defrag()
     */
    void                        mark_ghost(slotid_t slot);

    /**
     * Un-Mark the given slot to be a regular record.
     * If the record is already a non-ghost, does nothing.
     * This is only used by delete (UNDO).
     * This function itself does NOT log, so the caller is responsible for it.
     */
    void                        unmark_ghost(slotid_t slot);

    /**
     * \brief Defrags thispage to remove holes and ghost records in the page.
     * \details
     * A page can have unused holes between records and ghost records as a result
     * of inserts and deletes. This method removes those dead spaces to compress
     * the page. The best thing of this is that we have to log only
     * the slot numbers of ghost records that are removed because there are
     * 'logically' no changes.
     * Context: System transaction.
     * @param[in] popped the record to be popped out to the head. -1 to not specify.
     */
    rc_t                         defrag(slotid_t popped = -1);

    uint32_t            page_flags() const;
    uint32_t            get_store_flags() const;
    void                         set_store_flags(uint32_t);
    page_s&                      persistent_part();
    const page_s&                persistent_part_const() const;
    bool                         is_fixed() const;
    bool                         is_latched_by_me() const;
    bool                         is_mine() const;
    const latch_t*               my_latch() const; // for debugging
    void                         set_dirty() const;
    bool                         is_dirty() const;

    NORET                        page_p() : _pp(0), _mode(LATCH_NL), _refbit(0) {};
    NORET                        page_p(page_s* s, 
                                        uint32_t store_flags,
                                        int refbit = 1) 
                                 : _pp(s), _mode(LATCH_NL), _refbit(refbit)  {
                                     // _pp->set_page_storeflags(store_flags); 
                                     // If we aren't fixed yet, we'll get an error here
                                     set_store_flags (store_flags);
                                 }
    NORET                        page_p(const page_p& p) { W_COERCE(_copy(p)); }
    /** Destructor. Unfix the page. */
    virtual NORET                ~page_p() {
        if (bf->is_bf_page(_pp))  unfix();
        _pp = 0;
    }
    /** Unfix my page and fix the page of p. */
    page_p&                      operator=(const page_p& p);

    rc_t                         fix(
        const lpid_t&                    pid, 
        tag_t                            tag,
        latch_mode_t                     mode, 
        uint32_t                page_flags,
        store_flag_t&                    store_flags, // only used if virgin
        bool                             ignore_store_id = false,
        int                              refbit = 1) {
        return _fix_core(false, pid, tag, mode, page_flags, store_flags,
                ignore_store_id, refbit); 

    }
    rc_t                         _fix_core(
        bool                             conditional,
        const lpid_t&                    pid, 
        tag_t                            tag,
        latch_mode_t                     mode, 
        uint32_t                page_flags,
        store_flag_t&                    store_flags, // only used if virgin
        bool                             ignore_store_id,
        int                              refbit);
    void                         unfix();
    void                         discard();
    void                         unfix_dirty();
    // set_ref_bit sets the value to use for the buffer page reference
    // bit when the page is unfixed. 
    void                        set_ref_bit(int value) {_refbit = value;}

    // get EX latch if acquiring it will not block (otherwise set
    // would_block to true.
    /** Upgrade latch, even if you have to block. */
    void                         upgrade_latch(latch_mode_t m);

    /** Downgrade latch, from EX to SH. */
    void                         downgrade_latch();

    /**
     * Upgrade latch to EX if possible w/o blocking.
     * @param[out] would_block set to be true if it would block (and exit instantaneously)
     */
    rc_t                         upgrade_latch_if_not_block(bool &would_block); 

    latch_mode_t                 latch_mode() const;
    bool                         check_lsn_invariant() const;

    /** this is used by du/df to get page statistics DU DF. */
    void                        page_usage(
        int&                            data_sz,
        int&                            hdr_sz,
        int&                            unused,
        int&                             alignmt,
        tag_t&                             t,
        slotid_t&                     no_used_slots);

    tag_t                       tag() const;

    /** checks space correctness. */
    bool             is_consistent_space () const;
    
    /** Returns the stored value of checksum of this page. */
    uint32_t          get_checksum () const {return _pp->checksum;}
    /** Calculate the correct value of checksum of this page. */
    uint32_t          calculate_checksum () const {return _pp->calculate_checksum();}
    /** Renew the stored value of checksum of this page. */
    void             update_checksum () const {_pp->update_checksum();}

private:
    w_rc_t                      _copy(const page_p& p) ;

protected:
    // If a page type doesn't define its 4-argument format(), this
    // method is used by default.  Nice C++ trick, but it's a bit
    // obfuscating, and since the MAKEPAGE macro forces a declaration,
    // if not definition, of the page-type-specific format(), I'm going
    // to rename this _format.
    // Update: I'm removing the log_it argument because it's never used,
    // that is, is always false. 
    
    rc_t                         _format(
        lpid_t                     pid, // receive as entity, not reference or pointer. because this function might nuke the pointed object!
        tag_t                             tag,
        uint32_t                 page_flags,
        store_flag_t                      store_flags
        );
    
    /**
    *  Insert a record at slot idx. Slots on the left of idx
    *  are pushed further to the left to make space. 
    *  By this it's meant that the slot table entries are moved; the
    *  data themselves are NOT moved.
    *  Vec[] contains the data for these new slots. 
    */
    rc_t                        insert_expand_nolog(
        slotid_t idx, const cvec_t &tp);

    /**
     * Replaces the record of specified slot with the given data,
     * expanding the slot length if needed.
     */
    rc_t                        replace_expand_nolog(
        slotid_t                     idx,
        const cvec_t                &tp);

    /**
     * This is used when it's known that we are adding the new record
     * to the end, and the page is like a brand-new page; no holes,
     * and enough spacious. Basically only for page-format case.
     * This is much more efficient!
     * This doesn't log, so the caller is responsible for it.
     */
    void                        append_nolog(const cvec_t &tp, bool ghost);

    /**
     * Returns if there is enough free space to accomodate the
     * given new record.
     * @return true if there is free space
     */
    bool check_space_for_insert(size_t rec_size);    

    /**
    *  Remove the slot and up-shift slots after the hole to fill it up.
    * @param idx  the slot to remove.
    */
    rc_t                        remove_shift_nolog(slotid_t idx);

    /*  
     * DATA 
     */
    page_s*                     _pp;
    latch_mode_t                _mode;
    int                         _refbit;

    friend class page_img_format_t;
    friend class page_img_format_log;
    friend class page_set_byte_log;
    friend class btree_header_t;
    friend class btree_impl;
    friend class btree_ghost_reserve_log;
};

inline const lpid_t&
page_p::pid() const
{
    return _pp->pid;
}

inline void
page_p::set_vid(vid_t vid)
{
    _pp->pid._stid.vol = vid;
}
inline smsize_t 
page_p::used_space() const
{
    return (data_sz - _pp->record_head + nslots() * slot_sz); 
}

inline smsize_t
page_p::usable_space() const
{
    size_t contiguous_free_space = _pp->record_head - slot_sz * nslots();
    return contiguous_free_space; 
}

inline smsize_t
page_p::tuple_size(slotid_t idx) const
{
    w_assert3(idx >= 0 && idx < _pp->nslots);
    return *reinterpret_cast<const slot_length_t*>(_pp->data + slot_sz * idx + sizeof(slot_offset_t));
}

inline page_s::slot_offset_t
page_p::tuple_offset(slotid_t idx) const
{
    w_assert3(idx >= 0 && idx < _pp->nslots);
    slot_offset_t offset = *reinterpret_cast<const slot_offset_t*>(_pp->data + slot_sz * idx);
    return offset;
}

inline void*
page_p::tuple_addr(slotid_t idx) const
{
    slot_offset_t offset = tuple_offset(idx);
    if (offset < 0) offset = -offset; // ghost record.
    return _pp->data + offset;
}
inline bool
page_p::is_ghost_record(slotid_t idx) const
{
    slot_offset_t offset = tuple_offset(idx);
    return (offset < 0);
}

inline void page_p::overwrite_slot (slotid_t idx, slot_offset_t offset, slot_length_t length) {
    slot_offset_t* offset_p = reinterpret_cast<slot_offset_t*>(_pp->data + slot_sz * idx);
    slot_length_t* length_p = reinterpret_cast<slot_length_t*>(_pp->data + slot_sz * idx + sizeof(slot_offset_t));
    *offset_p = offset;
    *length_p = length;
}

inline uint32_t
page_p::page_flags() const
{
    return _pp->page_flags;
}

inline page_s&
page_p::persistent_part()
{
    return *(page_s*) _pp;
}

inline const page_s&
page_p::persistent_part_const() const
{
    return *(page_s*) _pp; 
}

inline bool
page_p::is_fixed() const
{
#if W_DEBUG_LEVEL > 1
    // The problem here is that _pp might be a
    // heap-based page_s, not a buffer-pool frame.
    // Let's call this iff it's a known frame:
    if(_pp && bf_m::get_cb(_pp)) w_assert1(is_latched_by_me());
#endif
    return _pp != 0;
}

inline latch_mode_t
page_p::latch_mode() const
{
#if W_DEBUG_LEVEL > 1
    // The problem here is that I might have more than one
    // holding of the latch at some point, starting out with
    // SH and then upgrading (possibly via double-acquire) it.  
    // The latch itself doesn't hold the mode.
    // of the identities of the holders. But now the page_p thinks
    // its mode is not what the actual latch's mode is.
    // Rather than try to update the page_p's mode, we'll
    // just relax the assert here.
    // if(_pp) w_assert2( ((_mode == LATCH_EX) == mine) || times>1);
    if(_pp) {
        bool mine = is_mine();
        if(_mode == LATCH_EX) w_assert2(mine); 
    }
#endif
    return _pp ? _mode : LATCH_NL;
}

inline bool 
page_p::check_lsn_invariant() const
{
    if(_pp) return bf_m::check_lsn_invariant(_pp);
    return true;
}

inline page_p::tag_t
page_p::tag() const
{
    return (tag_t) _pp->tag;
}

inline slotid_t
page_p::nslots() const
{
    return _pp->nslots;
}

inline const lsn_t& 
page_p::lsn() const
{
    return _pp->lsn;
}

inline void 
page_p::set_lsns(const lsn_t& lsn)
{
    _pp->lsn = lsn;
}

inline void 
page_p::discard()
{
    w_assert3(!_pp || bf->is_bf_page(_pp));
    if (_pp)  bf->discard_pinned_page(_pp);
    _pp = 0;
}

inline void 
page_p::unfix()
{
    w_assert2(!_pp || bf->is_bf_page(_pp, true));
    if(_pp) {
        bf->unfix(_pp, false, _refbit);
        DBGTHRD(<<"page fix " << " pid " << _pp->pid);
    } 
    _pp = 0;
}

inline void
page_p::unfix_dirty()
{
    w_assert2(!_pp || bf->is_bf_page(_pp));
    if (_pp)  {
        if(smlevel_0::logging_enabled == false) {
            // fake the lsn on the page. This is an attempt to
            // trick the btree code into being able to tell if
            // an mt scenario changed the page between fixes.
            lsn_t mylsn = lsn();
            mylsn.advance(1);
            const_cast<page_p*>(this)->set_lsns(mylsn);
        }
        bf->unfix(_pp, true, _refbit);
    }
    _pp = 0;
}

inline void
page_p::set_dirty() const
{
    if (bf->is_bf_page(_pp))  W_COERCE(bf->set_dirty(_pp));
    if(smlevel_0::logging_enabled == false) {
        // fake the lsn on the page. This is an attempt to
        // trick the btree code into being able to tell if
        // an mt scenario changed the page between fixes.
        lsn_t mylsn = lsn();
        mylsn.advance(1);
        const_cast<page_p*>(this)->set_lsns(mylsn);
    }
}

/** for debugging. */
inline bool
page_p::is_dirty() const
{
    if (bf->is_bf_page(_pp))  return bf->is_dirty(_pp);
    return false;
}

/*<std-footer incl-file-exclusion='PAGE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
