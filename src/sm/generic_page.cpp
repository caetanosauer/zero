/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "generic_page.h"



uint32_t generic_page_header::calculate_checksum () const {
    const uint32_t CHECKSUM_MULT = 0x35D0B891;

    // FIXME: The current checksum ignores the headers and most of the
    // data bytes, presumably for speed reasons.  If you start
    // checksumming the headers, be careful of the checksum field.

    const unsigned char *data      = (const unsigned char *)(this + 1);          // start of data section of this page: right after these headers
    const unsigned char *data_last = (const unsigned char *)(this) + page_sz - sizeof(uint32_t);  // the last 32-bit word of the data section of this page

    uint64_t value = 0;
    // these values (23/511) are arbitrary
    for (const unsigned char *p = (const unsigned char *) data + 23; p <= data_last; p += 511) {
        // be aware of alignment issue on spark! so this code is not safe
        // const uint32_t*next = reinterpret_cast<const uint32_t*>(p);
        // value ^= *next;
        value = value * CHECKSUM_MULT + p[0];
        value = value * CHECKSUM_MULT + p[1];
        value = value * CHECKSUM_MULT + p[2];
        value = value * CHECKSUM_MULT + p[3];
    }
    return ((uint32_t) (value >> 32)) ^ ((uint32_t) (value & 0xFFFFFFFF));
}
