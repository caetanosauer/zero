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
 * These pages contain bitmaps to represent which page is already
 * allocated.  They replace extlink_p and related classes in the
 * original Shore-MT.  They are drastically simpler and more efficient
 * than prior extent management.  See jira ticket:72 "fix extent
 * management" (originally trac ticket:74) for more details.
 */
class alloc_page : public generic_page_header {
public:
    shpid_t pid_offset;        ///< the smallest page-ID that the bitmap in this page represents
    shpid_t pid_highwatermark; ///< smallest pid in this page such that all higher pid's represented have their bits OFF

    /// The actual bitmap
    uint8_t bitmap[data_sz - sizeof(shpid_t)*2]; 
};


/**
 ** \brief Handler class for a free-page allocation/deallocation page.
 **/
class alloc_p {
public:
    alloc_p(page_s* s) : _pp(reinterpret_cast<alloc_page*>(s)) {
        w_assert1(sizeof(alloc_page) == generic_page_header::page_sz);
    }
    ~alloc_p()  {}
    
    rc_t format(const lpid_t& pid);

    /** Returns the smallest page ID bitmaps in this page represent. */
    shpid_t get_pid_offset() const { return _pp->pid_offset; }

    /** Smallest pid in this page from which all bits are OFF. */
    shpid_t get_pid_highwatermark() const { return _pp->pid_highwatermark; }
    /** Reset the pid_highwatermark. */
    void clear_pid_highwatermark() { _pp->pid_highwatermark = 0; }
    /** If needed, update pid_highwatermark. */
    void update_pid_highwatermark(shpid_t pid_touched);

private:    
    /** Returns the page allocation bitmap (ON if allocated).*/
          uint8_t* get_bitmap();
    const uint8_t* get_bitmap() const;
public:

    /// Is the given page already allocated?
    bool is_set_bit(shpid_t pid) const;

    /**
     * turn ON (allocate) the bit for the given page id.
     * This function doesn't log. It should be done in alloc_cache before calling this.
     */
    void set_bit(shpid_t pid);

    /** Do the same for multiple pages together. */
    void set_consecutive_bits(shpid_t pid_begin, shpid_t pid_end);

    /**
     * turn OFF (deallocate) the bit for the given page id.
     * This function doesn't log. It should be done in alloc_cache before calling this.
     */
    void unset_bit(shpid_t pid);
    
    enum {
        /** Number of pages one alloc_p can cover. */
	alloc_max = (sizeof(alloc_page::bitmap)) * 8
    };
    
    /** determines the pid_offset for the given alloc page. */
    inline static shpid_t alloc_pid_to_pid_offset (shpid_t alloc_pid) {
        uint32_t alloc_p_seq = alloc_pid - 1; // -1 for volume header
        return alloc_max * alloc_p_seq;
    }

    /** determines the alloc page the given page should belong to. */
    inline static shpid_t pid_to_alloc_pid (shpid_t pid) {
        uint32_t alloc_p_seq = pid / alloc_max;
        return alloc_p_seq + 1; // +1 for volume header
    }
    alloc_page *_pp;
};
inline void alloc_p::update_pid_highwatermark(shpid_t pid_touched) {
    if (pid_touched + 1 > _pp->pid_highwatermark) {
        _pp->pid_highwatermark = pid_touched + 1;
    }
}
inline uint8_t* alloc_p::get_bitmap() {
    return &_pp->bitmap[0];
}
inline const uint8_t* alloc_p::get_bitmap() const {
    return (const uint8_t*)&_pp->bitmap[0];
}

inline void _set_bit (uint8_t *bitmap, uint32_t index) {
    uint32_t byte_place = (index >> 3);
    uint32_t bit_place = index & 0x7;
    uint8_t* byte = bitmap + byte_place;
    
    w_assert1(smlevel_0::operating_mode == smlevel_0::t_in_redo ||
        (*byte & (1 << bit_place)) == 0); // during redo, it's possible that the page is already allocated. it's fine. allocation is idempotent
    *byte |= (1 << bit_place);
}
inline void _unset_bit (uint8_t *bitmap, uint32_t index) {
    uint32_t byte_place = (index >> 3);
    uint32_t bit_place = index & 0x7;
    uint8_t* byte = bitmap + byte_place;
    w_assert1(smlevel_0::operating_mode == smlevel_0::t_in_redo ||
        (*byte & (1 << bit_place)) != 0); // during redo, it's possible that the page is already deallocated. it's fine. deallocation is idempotent
    *byte &= ~(1 << bit_place);
}

inline void alloc_p::set_bit(shpid_t pid) {
    w_assert1(pid >= get_pid_offset());
    w_assert1(pid < get_pid_offset() + alloc_max);

    _set_bit (get_bitmap(), pid - get_pid_offset());    
    update_pid_highwatermark(pid);
}
inline void alloc_p::set_consecutive_bits(shpid_t pid_begin, shpid_t pid_end)
{
    w_assert1(pid_begin >= get_pid_offset());
    w_assert1(pid_end <= get_pid_offset() + alloc_max);
    w_assert1(pid_begin <= pid_end);
    uint8_t *bitmap = get_bitmap();
    //we need to do bit-wise opeartion only for first and last bytes. other bytes are all "FF".

    uint32_t index_begin = pid_begin - get_pid_offset();
    uint32_t index_begin_cut = index_begin;
    if ((index_begin & 0x7) != 0) {
        index_begin_cut = index_begin + 8 - (index_begin & 0x7);
        for (uint32_t index = index_begin; index < index_begin_cut; ++index) {
            _set_bit (bitmap, index);
        }
    }

    uint32_t index_end = pid_end - get_pid_offset();
    uint32_t index_end_cut = index_end;
    if ((index_end & 0x7) != 0) {
        index_end_cut = (index_end / 8) * 8;
        for (uint32_t index = index_end_cut; index < index_end; ++index) {
            _set_bit (bitmap, index);
        }
    }

    w_assert1(index_begin_cut % 8 == 0);
    w_assert1(index_end_cut % 8 == 0);
    ::memset (bitmap + (index_begin_cut / 8), (uint8_t) 0xFF, (index_end_cut - index_begin_cut) / 8);

    update_pid_highwatermark(pid_end - 1); // -1 because "end" itself is not touched
}

inline void alloc_p::unset_bit(shpid_t pid) {
    w_assert1(pid >= get_pid_offset());
    w_assert1(pid < get_pid_offset() + alloc_max);

    _unset_bit (get_bitmap(), pid - get_pid_offset());    
    // don't bother to recalculate high water mark.
}
inline bool alloc_p::is_set_bit(shpid_t pid) const {
    w_assert1(pid >= get_pid_offset());
    w_assert1(pid < get_pid_offset() + alloc_max);

    const uint8_t *bitmap = get_bitmap();
    uint32_t index = pid - get_pid_offset();
    
    uint32_t byte_place = (index >> 3);
    w_assert1(byte_place < (uint) page_s::data_sz);
    uint32_t bit_place = index & 0x7;
    const uint8_t* byte = bitmap + byte_place;
    return ((*byte & (1 << bit_place)) != 0);
}

inline rc_t alloc_p::format(const lpid_t& pid)
{
    ::memset (_pp, 0, sizeof(alloc_page));
    _pp->pid = pid;
    _pp->tag = t_alloc_p;

    // no records or whatever. this is just a huge bitmap
    shpid_t pid_offset = alloc_pid_to_pid_offset(pid.page);

    _pp->pid_offset        = pid_offset;
    _pp->pid_highwatermark = pid_offset;
    ::memset (&_pp->bitmap[0], 0, sizeof(_pp->bitmap));
    return RCOK;
}
    
#endif // ALLOC_P_H
