#ifndef PAGE_H
#define PAGE_H

#include "w_defines.h"

class stnode_p;
class alloc_p;

#ifdef __GNUG__
#pragma interface
#endif

#include "page_s.h"
#include <string.h>

/**
 *  Basic page handle class. This class is used to fix a page
 *  and operate on it.
 */
class page_p : public smlevel_0 
{

friend class dir_vol_m;  // for access to page_p::splice();

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
    
    char*                        data_addr8(slot_offset8_t offset8);
    const char*                  data_addr8(slot_offset8_t offset8) const;
    slotid_t                     nslots() const;
    slot_offset8_t               tuple_offset8(slotid_t idx) const;
    poor_man_key                 tuple_poormkey (slotid_t idx) const;
    void                         tuple_both (slotid_t idx, slot_offset8_t &offset8, poor_man_key &poormkey) const;
    void*                        tuple_addr(slotid_t idx) const;

    char*                        slot_addr(slotid_t idx) const;
    /**
     * Add a new slot to the given position.
     * If idx == nrecs(), this is an 'append' and more efficient because we don't have to move
     * existing slots.
     */
    void                         insert_slot (slotid_t idx, slot_offset8_t offset8, poor_man_key poormkey);
    /**
     * Changes only the offset part of the specified slot.
     * Used to turn a ghost record into a usual record, or to expand a record.
     */
    void                         change_slot_offset (slotid_t idx, slot_offset8_t offset8);

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

    tag_t                       tag() const;
    
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
     * Returns if there is enough free space to accomodate the
     * given new record.
     * @return true if there is free space
     */
    bool check_space_for_insert(size_t rec_size);    

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
    return (data_sz - _pp->get_record_head_byte() + nslots() * slot_sz); 
}

inline smsize_t
page_p::usable_space() const
{
    size_t contiguous_free_space = _pp->get_record_head_byte() - slot_sz * nslots();
    return contiguous_free_space; 
}

inline char* page_p::data_addr8(slot_offset8_t offset8)
{
    return _pp->data_addr8(offset8);
}
inline const char* page_p::data_addr8(slot_offset8_t offset8) const
{
    return _pp->data_addr8(offset8);
}
inline char* page_p::slot_addr(slotid_t idx) const
{
    w_assert3(idx >= 0 && idx <= _pp->nslots);
    return _pp->data + (slot_sz * idx);
}

inline slot_offset8_t
page_p::tuple_offset8(slotid_t idx) const
{
    return *reinterpret_cast<const slot_offset8_t*>(slot_addr(idx));
}
inline poor_man_key page_p::tuple_poormkey (slotid_t idx) const
{
    return *reinterpret_cast<const poor_man_key*>(slot_addr(idx) + sizeof(slot_offset8_t));
}
inline void page_p::tuple_both (slotid_t idx, slot_offset8_t &offset8, poor_man_key &poormkey) const
{
    const char* slot = slot_addr(idx);
    offset8 = *reinterpret_cast<const slot_offset8_t*>(slot);
    poormkey = *reinterpret_cast<const poor_man_key*>(slot + sizeof(slot_offset8_t));
}

inline void*
page_p::tuple_addr(slotid_t idx) const
{
    slot_offset8_t offset8 = tuple_offset8(idx);
    if (offset8 < 0) offset8 = -offset8; // ghost record.
    return _pp->data_addr8(offset8);
}

inline void page_p::change_slot_offset (slotid_t idx, slot_offset8_t offset) {
    char* slot = slot_addr(idx);
    *reinterpret_cast<slot_offset8_t*>(slot) = offset;
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
    //alloc_p/stnode_p doesn't use the main bufferpool.
    if (_pp->tag != t_alloc_p && _pp->tag != t_stnode_p) {
        if (bf->is_bf_page(_pp))  W_COERCE(bf->set_dirty(_pp));
    }
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

#endif
