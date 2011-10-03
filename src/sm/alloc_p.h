#ifndef ALLOC_P_H
#define ALLOC_P_H

#include "w_defines.h"

#ifdef __GNUG__
#pragma interface
#endif

#include "page.h"
#include "w_key.h"

/**
 * \brief Free-Page allocation/deallocation page.
 * \details
 * These pages contain bitmaps to represent which page is already
 * allocated. They replace extlink_p and related classes in original Shore-MT.
 * They are drastically simpler and more efficient than prior extent management.
 * See ticket:74 for more details.
 */
class alloc_p : public page_p {
public:
    alloc_p() : page_p() {}
    alloc_p(page_s* s, uint32_t store_flags) : page_p(s, store_flags) {}
    ~alloc_p()  {}
    
    tag_t get_page_tag () const { return t_alloc_p; }
    int get_default_refbit () const { return 20; } // allocate page should be very hot
    void inc_fix_cnt_stat () const { INC_TSTAT(alloc_p_fix_cnt);}
    rc_t format(const lpid_t& pid, tag_t tag, uint32_t page_flags, store_flag_t store_flags);

    
    /** Returns the smallest page ID bitmaps in this page represent. */
    shpid_t get_pid_offset() const;

    /** Smallest pid in this page from which all bits are OFF. */
    shpid_t get_pid_highwatermark() const;
    /** Reset the pid_highwatermark. */
    void clear_pid_highwatermark();
    /** If needed, update pid_highwatermark. */
    void update_pid_highwatermark(shpid_t pid_touched);
    
    /** Returns the page allocation bitmap (ON if allocated).*/
    uint8_t* get_bitmap();
    /** const version. */
    const uint8_t* get_bitmap() const;

    /**
     * Returns if the given page is already allocated.
     */
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
        alloc_max = page_p::data_sz * 8
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
};
inline shpid_t alloc_p::get_pid_offset() const {
    return reinterpret_cast<const shpid_t*>(_pp->data)[0];
}
inline shpid_t alloc_p::get_pid_highwatermark() const {
    return reinterpret_cast<const shpid_t*>(_pp->data)[1];
}
inline void alloc_p::clear_pid_highwatermark() {
    reinterpret_cast<shpid_t*>(_pp->data)[1] = 0;
}
inline void alloc_p::update_pid_highwatermark(shpid_t pid_touched) {
    shpid_t *array = reinterpret_cast<shpid_t*>(_pp->data);
    if (pid_touched + 1 > array[1]) {
        array[1] = pid_touched + 1;
    }
}
inline uint8_t* alloc_p::get_bitmap() {
    return (uint8_t*) (_pp->data + sizeof(shpid_t) * 2);
}
inline const uint8_t* alloc_p::get_bitmap() const {
    return (const uint8_t*) (_pp->data + sizeof(shpid_t) * 2);
}

inline void _set_bit (uint8_t *bitmap, uint32_t index) {
    uint32_t byte_place = (index >> 3);
    uint32_t bit_place = index & 0x7;
    uint8_t* byte = bitmap + byte_place;
    w_assert1((*byte & (1 << bit_place)) == 0);
    *byte |= (1 << bit_place);
}
inline void _unset_bit (uint8_t *bitmap, uint32_t index) {
    uint32_t byte_place = (index >> 3);
    uint32_t bit_place = index & 0x7;
    uint8_t* byte = bitmap + byte_place;
    w_assert1((*byte & (1 << bit_place)) != 0);
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
    w_assert1(byte_place < (uint) page_p::data_sz);
    uint32_t bit_place = index & 0x7;
    const uint8_t* byte = bitmap + byte_place;
    return ((*byte & (1 << bit_place)) != 0);
}

inline rc_t alloc_p::format(const lpid_t& pid, tag_t tag, uint32_t flags, store_flag_t store_flags)
{
    W_DO(page_p::_format(pid, tag, flags, store_flags) );

    // no records or whatever. this is just a huge bitmap
    shpid_t *headers = reinterpret_cast<shpid_t*>(_pp->data);
    shpid_t pid_offset = alloc_pid_to_pid_offset(pid.page);
    headers[0] = pid_offset;
    headers[1] = pid_offset;
    ::memset (_pp->data + sizeof(shpid_t) * 2, 0, sizeof(_pp->data) - sizeof(shpid_t) * 2);
    return RCOK;
}
    
#endif // ALLOC_P_H
