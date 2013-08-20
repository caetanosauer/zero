/*
 * (c) Copyright 2013, Hewlett-Packard Development Company, LP
 */

#include "alloc_p.h"


void alloc_page::set_bits(uint32_t from, uint32_t to) {
    if (!(from < to))
        return;

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
            i = (last_byte-1)*8;
            continue;
        }

        set_bit(i);
    }
}
