/*
 * (c) Copyright 2013, Hewlett-Packard Development Company, LP
 */

#include "alloc_page.h"


void alloc_page::set_bits(uint32_t from, uint32_t to) {
    // We need to do bit-wise operations only for first and last
    // bytes.  Other bytes are all "FF".
    uint32_t last_byte = byte_place(to);
    for (uint32_t i=from; i<to; i++) {
        if (bit_place(i) != 0) {
            set_bit(i);
            continue;
        }

        uint32_t byte = byte_place(i);
        if (byte < last_byte) {
            ::memset(&bitmap[byte], (uint8_t) 0xFF, last_byte-byte);
            i = last_byte*8 - 1;
            continue;
        }

        set_bit(i);
    }
}


alloc_page_h::alloc_page_h(generic_page* s, const lpid_t& pid):
    _page(reinterpret_cast<alloc_page*>(s)) 
{
    w_assert1(sizeof(alloc_page) == generic_page_header::page_sz);

    ::memset (_page, 0, sizeof(*_page));
    _page->pid = pid;
    _page->tag = t_alloc_p;

    shpid_t pid_offset = alloc_pid_to_pid_offset(pid.page);
    _page->pid_offset        = pid_offset;
    _page->pid_highwatermark = pid_offset;
    // _page->bitmap initialized to all OFF's by memset above
}
