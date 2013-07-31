/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef PAGE_H
#define PAGE_H

#include "w_defines.h"

class stnode_p;
class alloc_p;

#ifdef __GNUG__
#pragma interface
#endif

#include "bf_idx.h"
#include "stid_t.h"
#include "vid_t.h"
#include "page_s.h"
#include "latch.h"
#include <string.h>

/**
 *  Basic page handle class.
 */
class page_p
{
public:
    enum {
        page_sz = sizeof(page_s),
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

    page_p() : _pp(NULL), _mode(LATCH_NL) {}
    /**
     * Imaginery 'fix' for a non-bufferpool-managed page.
     */
    page_p(page_s* s) : _pp(s), _mode(LATCH_NL) {
        w_assert1(s != NULL);
    }

    /** release the page from bufferpool. */
    void                        unfix ();


    ~page_p() {
        unfix();
    }
    page_p& operator=(page_p& p) {
        // this steals the ownership of the page/latch
        steal_ownership(p);
        return *this;
    }
    void steal_ownership (page_p& p) {
        unfix();
        _pp = p._pp;
        _mode = p._mode;
        p._pp = NULL;
        p._mode = LATCH_NL;
    }
    
    /**
     * Fixes a non-root page in the bufferpool. This method receives the parent page and efficiently
     * fixes the page if the shpid (pointer) is already swizzled by the parent page.
     * The optimization is transparent for most of the code because the shpid stored in the parent
     * page is automatically (and atomically) changed to a swizzled pointer by the bufferpool.
     * @param[in] parent parent of the page to be fixed. has to be already latched. if you can't provide this,
     * use fix_direct() though it can't exploit pointer swizzling and thus will be slower.
     * @param[in] vol volume ID.
     * @param[in] shpid ID of the page to fix (or bufferpool index when swizzled)
     * @param[in] mode latch mode. has to be SH or EX.
     * @param[in] conditional whether the fix is conditional (returns immediately even if failed).
     * @param[in] virgin_page whether the page is a new page thus doesn't have to be read from disk.
     * To use this method, you need to include page_bf_inline.h.
     */
    w_rc_t                      fix_nonroot (const page_p &parent, volid_t vol, shpid_t shpid, latch_mode_t mode, bool conditional = false, bool virgin_page = false);

    /**
     * Fixes any page (root or non-root) in the bufferpool without pointer swizzling.
     * In some places, we need to fix a page without fixing the parent, e.g., recovery or re-fix in cursor.
     * For such code, this method allows fixing without parent. However, this method can be used only when
     * the pointer swizzling is off.
     * @see bf_tree_m::fix_direct()
     * @param[in] vol volume ID.
     * @param[in] shpid ID of the page to fix. If the shpid looks like a swizzled pointer, this method returns an error (see above).
     * @param[in] mode latch mode. has to be SH or EX.
     * @param[in] conditional whether the fix is conditional (returns immediately even if failed).
     * @param[in] virgin_page whether the page is a new page thus doesn't have to be read from disk.
     * To use this method, you need to include page_bf_inline.h.
     */
    w_rc_t                      fix_direct (volid_t vol, shpid_t shpid, latch_mode_t mode, bool conditional = false, bool virgin_page = false);

    /**
     * Adds an additional pin count for the given page (which must be already latched).
     * This is used to re-fix the page later without parent pointer. See fix_direct() why we need this feature.
     * Never forget to call a corresponding unpin_for_refix() for this page. Otherwise, the page will be in the bufferpool forever.
     * @return slot index of the page in this bufferpool. Use this value to the subsequent refix_direct() and unpin_for_refix() call.
     * To use this method, you need to include page_bf_inline.h.
     */
    bf_idx                      pin_for_refix();

    /**
     * Fixes a page with the already known slot index, assuming the slot has at least one pin count.
     * Used with pin_for_refix() and unpin_for_refix().
     * To use this method, you need to include page_bf_inline.h.
     */
    w_rc_t                      refix_direct (bf_idx idx, latch_mode_t mode, bool conditional = false);

    /**
     * Fixes a new (virgin) root page for a new store with the specified page ID.
     * Implicitly, the latch will be EX and non-conditional.
     * To use this method, you need to include page_bf_inline.h.
     */
    w_rc_t                      fix_virgin_root (volid_t vol, snum_t store, shpid_t shpid);

    /**
     * Fixes an existing (not virgin) root page for the given store.
     * This method doesn't receive page ID because it's already known by bufferpool.
     * To use this method, you need to include page_bf_inline.h.
     */
    w_rc_t                      fix_root (volid_t vol, snum_t store, latch_mode_t mode, bool conditional = false);


    /** Marks this page in the bufferpool dirty. If this page is not a bufferpool-managed page, does nothing. */
    void                        set_dirty() const;
    /** Returns if this page in the bufferpool is marked dirty. If this page is not a bufferpool-managed page, returns false. */
    bool                        is_dirty() const;

    const lsn_t&                lsn() const;
    void                        set_lsns(const lsn_t& lsn);

    const lpid_t&               pid() const;
    volid_t                     vol() const;
    snum_t                      store() const;
    tag_t                       tag() const { return (tag_t) _pp->tag;}
    shpid_t                     btree_root() const { return _pp->btree_root;}

    // used when page is first read from disk
    void                        set_vid(vid_t vid);

    smsize_t                    used_space()  const;
    // Total usable space on page
    smsize_t                     usable_space()  const;
    
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
     * Changes only the offset part of the specified slot.
     * Used to turn a ghost record into a usual record, or to expand a record.
     */
    void                         change_slot_offset (slotid_t idx, slot_offset8_t offset8);

    uint32_t                     page_flags() const;
    page_s&                      persistent_part();
    const page_s&                persistent_part_const() const;
    bool                         is_fixed() const;
    latch_mode_t                 latch_mode() const { return _mode; }
    bool                         is_latched() const { return _mode != LATCH_NL; }
    /** conditionally upgrade the latch to EX. returns if successfully upgraded. */
    bool                         upgrade_latch_conditional();
    
    /** Returns the stored value of checksum of this page. */
    uint32_t          get_checksum () const {return _pp->checksum;}
    /** Calculate the correct value of checksum of this page. */
    uint32_t          calculate_checksum () const {return _pp->calculate_checksum();}
    /** Renew the stored value of checksum of this page. */
    void             update_checksum () const {_pp->update_checksum();}

protected:

    /**
     * Returns if there is enough free space to accomodate the
     * given new record.
     * @return true if there is free space
     */
    bool check_space_for_insert(size_t rec_size);    

    page_s*                     _pp;
    latch_mode_t                _mode;

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
inline volid_t
page_p::vol() const
{
    return _pp->pid.vol().vol;
}
inline snum_t
page_p::store() const
{
    return _pp->pid.store();
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
    return _pp != 0;
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

#include "page_bf_inline.h"

#endif
