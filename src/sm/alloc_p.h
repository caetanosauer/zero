/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef ALLOC_P_H
#define ALLOC_P_H

#include "w_defines.h"

#ifdef __GNUG__
#pragma interface
#endif

#include "page_s.h"
#include "w_key.h"
#include "sm_base.h"



/**
 * \brief Free-page allocation/deallocation page.
 *
 * \details
 * These pages contain bitmaps that encode which pages are already
 * allocated.  They replace extlink_p and related classes in the
 * original Shore-MT.  They are drastically simpler and more efficient
 * than prior extent management.  See jira ticket:72 "fix extent
 * management" (originally trac ticket:74) for more details.
 *
 * The implementation is spread between this class and alloc_page_h;
 * this class contains the basic fields and accessors for the
 * bitmap's bits.  The linkage between these and their interpretation
 * is contained in the handler class.
 */
class alloc_page : public generic_page_header {
    friend class alloc_p;


    /// the smallest page-ID that the bitmap in this page represents
    shpid_t pid_offset;        
    /// smallest pid in this page such that all higher pid's
    /// represented have their bits OFF
    shpid_t pid_highwatermark; 


    /**
     * \brief The actual bitmap.
     *
     * \details
     * Holds the allocation status for pid's in [pid_offset..pid_offset+bits_held);
     * for those pids, a pid p is allocated iff bitmap[bit_place(p-pid_offset)]&bit_mask(p-pid_offset) != 0
     */
    uint8_t bitmap[data_sz - sizeof(shpid_t)*2]; 

    /// Number of pages one alloc_page can cover
    static const int bits_held = (sizeof(alloc_page::bitmap)) * 8;
    
    uint32_t byte_place(uint32_t index) { return index >> 3; }
    uint32_t bit_place (uint32_t index) { return index & 0x7; }
    uint32_t bit_mask  (uint32_t index) { return 1 << bit_place(index); }

    bool get_bit  (uint32_t index) { return (bitmap[byte_place(index)]&bit_mask(index)) != 0; }
    void unset_bit(uint32_t index) { bitmap[byte_place(index)] &= ~bit_mask(index); }
    void set_bit  (uint32_t index) { bitmap[byte_place(index)] |=  bit_mask(index); }
    /// set all bits in [from, to)
    void set_bits (uint32_t from, uint32_t to);
};



/**
 ** \brief Handler class for a free-page allocation/deallocation page.
 **/
class alloc_p {
    alloc_page *_page;

public:
    alloc_p(page_s* s) : _page(reinterpret_cast<alloc_page*>(s)) {
        w_assert1(sizeof(alloc_page) == generic_page_header::page_sz);
    }
    ~alloc_p()  {}

    page_s* generic_page() const { return reinterpret_cast<page_s*>(_page); }


    rc_t format(const lpid_t& pid);

    /// Number of pages one alloc_page can cover
    static const int bits_held = alloc_page::bits_held;

    /** Returns the smallest page ID bitmaps in this page represent. */
    shpid_t get_pid_offset() const { return _page->pid_offset; }

    /** Smallest pid in this page from which all bits are OFF. */
    shpid_t get_pid_highwatermark() const { return _page->pid_highwatermark; }
    /** Reset the pid_highwatermark. */
    void clear_pid_highwatermark() { _page->pid_highwatermark = 0; }
    /** If needed, update pid_highwatermark. */
    void update_pid_highwatermark(shpid_t pid_touched);


    /// Is the given page already allocated?
    bool is_set_bit(shpid_t pid) const;

    /**
     * turn OFF (deallocate) the bit for the given page ID.
     * This function doesn't log. It should be done in alloc_cache before calling this.
     */
    void unset_bit(shpid_t pid);

    /**
     * turn ON (allocate) the bit for the given page ID.
     * This function doesn't log. It should be done in alloc_cache before calling this.
     */
    void set_bit(shpid_t pid);

    /** Do the same for multiple pages together. */
    void set_consecutive_bits(shpid_t pid_begin, shpid_t pid_end);


    
    /** determines the pid_offset for the given alloc page. */
    inline static shpid_t alloc_pid_to_pid_offset (shpid_t alloc_pid) {
        uint32_t alloc_p_seq = alloc_pid - 1; // -1 for volume header
        return alloc_page::bits_held * alloc_p_seq;
    }

    /** determines the alloc page the given page should belong to. */
    inline static shpid_t pid_to_alloc_pid (shpid_t pid) {
        uint32_t alloc_p_seq = pid / alloc_page::bits_held;
        return alloc_p_seq + 1; // +1 for volume header
    }
};



inline bool alloc_p::is_set_bit(shpid_t pid) const {
    w_assert1(pid >= get_pid_offset());
    w_assert1(pid < get_pid_offset() + alloc_page::bits_held);

    return _page->get_bit(pid - _page->pid_offset);
}

inline void alloc_p::unset_bit(shpid_t pid) {
    w_assert1(pid >= get_pid_offset());
    w_assert1(pid < get_pid_offset() + alloc_page::bits_held);

    // except possibly during redo, we should never be trying to deallocate a page twice:
    w_assert1(smlevel_0::operating_mode == smlevel_0::t_in_redo ||
              is_set_bit(pid));

    _page->unset_bit(pid - _page->pid_offset);
    // don't bother to recalculate high water mark.
}

inline void alloc_p::set_bit(shpid_t pid) {
    w_assert1(pid >= get_pid_offset());
    w_assert1(pid < get_pid_offset() + alloc_page::bits_held);

    // except possibly during redo, we should never be trying to allocate a page twice:
    w_assert1(smlevel_0::operating_mode == smlevel_0::t_in_redo ||
              !is_set_bit(pid));

    _page->set_bit(pid - _page->pid_offset);
    update_pid_highwatermark(pid);
}

inline void alloc_p::set_consecutive_bits(shpid_t pid_begin, shpid_t pid_end) {
    w_assert1(pid_begin >= get_pid_offset());
    w_assert1(pid_begin <= pid_end);
    w_assert1(pid_end <= get_pid_offset() + alloc_page::bits_held);

    _page->set_bits(pid_begin-_page->pid_offset, pid_end-_page->pid_offset);

    update_pid_highwatermark(pid_end - 1); // -1 because "end" itself is not touched
}








inline void alloc_p::update_pid_highwatermark(shpid_t pid_touched) {
    if (pid_touched + 1 > _page->pid_highwatermark) {
        _page->pid_highwatermark = pid_touched + 1;
    }
}

inline rc_t alloc_p::format(const lpid_t& pid) {
    ::memset (_page, 0, sizeof(alloc_page));
    _page->pid = pid;
    _page->tag = t_alloc_p;

    // no records or whatever.  this is just a huge bitmap
    shpid_t pid_offset = alloc_pid_to_pid_offset(pid.page);

    _page->pid_offset        = pid_offset;
    _page->pid_highwatermark = pid_offset;
    // _page->bitmap initialized to all OFF's by memset above

    return RCOK;
}
    
#endif // ALLOC_P_H
