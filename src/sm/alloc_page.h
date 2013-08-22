/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef ALLOC_PAGE_H
#define ALLOC_PAGE_H

#include "w_defines.h"
#include "sm_base.h"
#include "generic_page.h"



/**
 * \brief Free-page allocation/deallocation page.
 *
 * \details
 * These pages contain bitmaps that encode which pages are already
 * allocated.  In particular an alloc_page p encodes allocation
 * information for pages with pids in
 * [p.pid_offset..p.pid_offset+p.bits_held).
 *
 * The implementation is spread between this class and its handle
 * class, alloc_page_h.  This class contains the basic fields and
 * accessors for the bitmap's bits.  The linkage between these and
 * their interpretation is contained in the handle class.
 */
class alloc_page : public generic_page_header {
    friend class alloc_page_h;


    /// the smallest page ID that the bitmap in this page represents
    shpid_t pid_offset;        
    /// smallest pid represented by this page that has never had its
    /// corresponding bit set or pid_offset+bits_held if no such pid
    /// exists.
    shpid_t pid_highwatermark; 


    /**
     * \brief The actual bitmap.
     *
     * \details
     * Holds the allocation status for pid's in [pid_offset..pid_offset+bits_held);
     * for those pids, a pid p is allocated iff 
     *   bitmap[bit_place(p-pid_offset)]&bit_mask(p-pid_offset) != 0
     */
    uint8_t bitmap[data_sz - sizeof(shpid_t)*2]; 

    /// Number of pages one alloc_page can cover
    static const int bits_held = (sizeof(alloc_page::bitmap)) * 8;
    
    uint32_t byte_place(uint32_t index) { return index >> 3; }
    uint32_t bit_place (uint32_t index) { return index & 0x7; }
    uint32_t bit_mask  (uint32_t index) { return 1 << bit_place(index); }

    bool       bit(uint32_t index) { return (bitmap[byte_place(index)]&bit_mask(index)) != 0; }
    void unset_bit(uint32_t index) { bitmap[byte_place(index)] &= ~bit_mask(index); }
    void   set_bit(uint32_t index) { bitmap[byte_place(index)] |=  bit_mask(index); }
    /// set all bits in [from, to)
    void  set_bits(uint32_t from, uint32_t to);
};



/**
 * \brief Handler class for a free-page allocation/deallocation page.
 *
 * \details 
 * None of these methods log; logging should be done in alloc_cache
 * when necessary before calling these methods.
 */
class alloc_page_h {
    alloc_page *_page;

public:
    /// format given page with page-ID pid as an alloc page then return a handle to it
    alloc_page_h(generic_page* s, const lpid_t& pid);
    /// construct handle from an existing alloc page
    alloc_page_h(generic_page* s) : _page(reinterpret_cast<alloc_page*>(s)) {
        w_assert1(s->tag == t_alloc_p);
    }
    ~alloc_page_h() {}

    /// return pointer to underlying page
    generic_page* to_generic_page() const { return reinterpret_cast<generic_page*>(_page); }


    /// smallest page ID that bitmaps in this page represent
    shpid_t get_pid_offset() const { return _page->pid_offset; }

    /// number of pages one alloc_page can cover
    static const int bits_held = alloc_page::bits_held;

    /// smallest pid represented by this page that has never had its
    /// corresponding bit set or pid_offset+bits_held if no such pid
    /// exists.
    ///
    /// Invariant: !get_bit(p) for p in [get_pid_highwatermark()
    ///                                  .. get_pid_offset()+bits_held)
    shpid_t get_pid_highwatermark() const { return _page->pid_highwatermark; }


    /// Is the given page allocated?
    bool is_bit_set(shpid_t pid) const;

    /// Turn OFF (deallocate) the bit for the given page ID.
    void unset_bit(shpid_t pid);

    /// Turn ON (allocate) the bit for the given page ID.
    void set_bit(shpid_t pid);

    /// Turn ON (allocate) the bits for the pids in [pid_begin..pid_end)
    void set_consecutive_bits(shpid_t pid_begin, shpid_t pid_end);

    
    /// determines the pid_offset for the given alloc page
    inline static shpid_t alloc_pid_to_pid_offset(shpid_t alloc_pid) {
        uint32_t alloc_page_h_seq = alloc_pid - 1; // -1 for volume header
        return alloc_page::bits_held * alloc_page_h_seq;
    }

    /// determines the alloc page the given page should belong to
    inline static shpid_t pid_to_alloc_pid (shpid_t pid) {
        uint32_t alloc_page_h_seq = pid / alloc_page::bits_held;
        return alloc_page_h_seq + 1; // +1 for volume header
    }

private:
    void update_pid_highwatermark(shpid_t pid_touched);
};



inline bool alloc_page_h::is_bit_set(shpid_t pid) const {
    w_assert1(pid >= get_pid_offset());
    w_assert1(pid < get_pid_offset() + alloc_page::bits_held);

    return _page->bit(pid - _page->pid_offset);
}

inline void alloc_page_h::unset_bit(shpid_t pid) {
    w_assert1(pid >= get_pid_offset());
    w_assert1(pid < get_pid_offset() + alloc_page::bits_held);

    // except possibly during redo, we should never be trying to deallocate a page twice:
    w_assert1(smlevel_0::operating_mode == smlevel_0::t_in_redo ||
              is_bit_set(pid));

    _page->unset_bit(pid - _page->pid_offset);
}

inline void alloc_page_h::set_bit(shpid_t pid) {
    w_assert1(pid >= get_pid_offset());
    w_assert1(pid < get_pid_offset() + alloc_page::bits_held);

    // except possibly during redo, we should never be trying to allocate a page twice:
    w_assert1(smlevel_0::operating_mode == smlevel_0::t_in_redo ||
              !is_bit_set(pid));

    _page->set_bit(pid - _page->pid_offset);
    update_pid_highwatermark(pid);
}

inline void alloc_page_h::set_consecutive_bits(shpid_t pid_begin, shpid_t pid_end) {
    w_assert1(pid_begin >= get_pid_offset());
    w_assert1(pid_begin <= pid_end);
    w_assert1(pid_end <= get_pid_offset() + alloc_page::bits_held);

    _page->set_bits(pid_begin-_page->pid_offset, pid_end-_page->pid_offset);

    update_pid_highwatermark(pid_end - 1); // -1 because "end" itself is not touched
}


inline void alloc_page_h::update_pid_highwatermark(shpid_t pid_touched) {
    if (pid_touched + 1 > _page->pid_highwatermark) {
        _page->pid_highwatermark = pid_touched + 1;
    }
}

#endif // ALLOC_PAGE_H
