/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "generic_page.h"



uint32_t generic_page_header::calculate_checksum () const {
    // Region checksum can cover is [start..end):
    const unsigned char *start = (const unsigned char *)this + sizeof(checksum);  // do not cover checksum field!
    const unsigned char *end   = (const unsigned char *)this + page_sz;


    // FIXME: The current checksum ignores most of the headers and
    // data bytes, presumably for speed reasons.

    const uint32_t CHECKSUM_MULT = 0x35D0B891;
    const uint64_t CHECKSUM_INIT = 0x5CC31574A49F933B;

    uint64_t value = CHECKSUM_INIT;
    // these values (23/511) are arbitrary
    for (const unsigned char *p = start + 23; p+3 < end; p += 511) {
        // be aware of alignment issue on spark! so this code is not safe:
        // const uint32_t*next = reinterpret_cast<const uint32_t*>(p);
        // value ^= *next;
        value = value * CHECKSUM_MULT + p[0];
        value = value * CHECKSUM_MULT + p[1];
        value = value * CHECKSUM_MULT + p[2];
        value = value * CHECKSUM_MULT + p[3];
    }
    return ((uint32_t) (value >> 32)) ^ ((uint32_t) (value & 0xFFFFFFFF));
}

std::ostream& operator<<(std::ostream& os, generic_page_header& p)
{
    os << "PAGE " << p.pid
        << " LSN: " << p.lsn
        << " TAG: " << p.tag
        << " FLAGS: " << p.page_flags
        << " STORE: " << p.store;
    return os;
}
