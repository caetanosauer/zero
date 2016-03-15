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
    friend class alloc_page_h;
public:

    extent_id_t extent_id;

    // Fill first section of page with char array (unused part). This is
    // currently 4KB, whereas the bitmap occupies the rest 4KB.
    char _fill[sizeof(generic_page)/2 - sizeof(generic_page_header)
                - sizeof(extent_id_t)];


    /**
     * \brief The actual bitmap.
     *
     * \details
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
    static const int bitmapsize = sizeof(generic_page) / 2;
    static const int bits_held = bitmapsize * 8;

    uint8_t bitmap[bitmapsize];

    uint32_t byte_place(uint32_t index) { return index >> 3; }
    uint32_t bit_place (uint32_t index) { return index & 0x7; }
    uint32_t bit_mask  (uint32_t index) { return 1 << bit_place(index); }

    bool get_bit(uint32_t index) { return (bitmap[byte_place(index)]&bit_mask(index)) != 0; }
    void unset_bit(uint32_t index) { bitmap[byte_place(index)] &= ~bit_mask(index); }
    void set_bit(uint32_t index) { bitmap[byte_place(index)] |=  bit_mask(index); }

    void set_bits(uint32_t from, uint32_t to);
    void reset_all();
};
BOOST_STATIC_ASSERT(sizeof(alloc_page) == generic_page_header::page_sz);

#endif // ALLOC_PAGE_H
