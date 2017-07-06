/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef ALLOC_PAGE_H
#define ALLOC_PAGE_H

#include "generic_page.h"
#include "sm_base.h"
#include "w_defines.h"

typedef uint32_t extent_id_t;

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
public:

    // First half of page (4KB) is the bitmap

    /**
     * Holds the allocation status for pid's in [pid_offset..pid_offset+bits_held);
     * for those pids, a pid p is allocated iff
     *   bitmap[bit_place(p-pid_offset)]&bit_mask(p-pid_offset) != 0
     *
     * Bitmap holds a fixed amount of 32768 (32k) bits. This means that one
     * extent consists of this many pages plus one allocation page responsible
     * for it, yielding exactly 32k pages, or 256MB. The first bit is
     * redundant, as it refers to the alloc page itself. It is always set to
     * one.
     */
    static constexpr int bitmapsize = sizeof(generic_page) / 2;
    static constexpr int bits_held = bitmapsize * 8;
    uint8_t bitmap[bitmapsize];

    // Fill second half of the page with char array (unused part). This is
    // currently 4KB, whereas the bitmap occupies the remaining 4KB.
    // This reserved space of 4KB is kept here just for future uses; it is
    // currently not used.
    char _fill[sizeof(generic_page)/2 - sizeof(generic_page_header)];

    static uint32_t byte_place(uint32_t index) { return index >> 3; }
    static uint32_t bit_place (uint32_t index) { return index & 0x7; }
    static uint32_t bit_mask  (uint32_t index) { return 1 << bit_place(index); }

    bool get_bit(uint32_t index) const
    { return (bitmap[byte_place(index)]&bit_mask(index)) != 0; }

    void unset_bit(uint32_t index) { bitmap[byte_place(index)] &= ~bit_mask(index); }
    void set_bit(uint32_t index) { bitmap[byte_place(index)] |=  bit_mask(index); }

    uint32_t get_last_set_bit() const;
    void set_bits(uint32_t from, uint32_t to);
    void format_empty();

    // Used to generate page_img_format log records
    const char* unused_part(size_t& length) const
    {
        auto first_unset_byte = byte_place(get_last_set_bit()) + 1;
        const char* pos = reinterpret_cast<const char*>(&bitmap[first_unset_byte]);
        length = sizeof(alloc_page) - (pos - reinterpret_cast<const char*>(this));
        w_assert1(length >= sizeof(_fill));
        return pos;
    }
};
BOOST_STATIC_ASSERT(sizeof(alloc_page) == generic_page_header::page_sz);

#endif // ALLOC_PAGE_H
