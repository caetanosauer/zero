/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef GENERIC_PAGE_H
#define GENERIC_PAGE_H

#include "w_defines.h"
#include "w_endian.h"
#include "sm_s.h"


enum tag_t {
    t_bad_p        = 0,        // not used
    t_alloc_p      = 1,        // free-page allocation page 
    t_stnode_p     = 2,        // store node page
    t_btree_p      = 5,        // btree page 
    t_any_p        = 11        // indifferent
};
enum page_flag_t {
    t_tobedeleted  = 0x01,     // this page will be deleted as soon as the page is evicted from bufferpool
    t_virgin       = 0x02,     // newly allocated page
    t_written      = 0x08      // read in from disk
};



/**
* offset divided by 8 (all records are 8-byte aligned).
* negative value means ghost records.
*/
typedef int16_t  slot_offset8_t;
typedef uint16_t slot_length_t;
typedef int16_t  slot_index_t; // to avoid explicit sized-types below
/** convert a byte offset to 8-byte-divided offset. */
inline slot_offset8_t to_offset8(int32_t byte_offset) {
    w_assert1(byte_offset % 8 == 0);
    w_assert1(byte_offset < (1 << 18));
    w_assert1(byte_offset >= -(1 << 18));
    return byte_offset / 8;
}
/** convert a byte offset to 8-byte-divided offset with alignment (if %8!=0, put padding bytes). */
inline slot_offset8_t to_aligned_offset8(int32_t byte_offset) {
    w_assert1(byte_offset >= 0); // as we are aligning the offset, it should be positive
    w_assert1(byte_offset < (1 << 18));
    if (byte_offset % 8 == 0) {
        return byte_offset / 8;
    } else {
        return (byte_offset / 8) + 1;
    }
}
/** convert a 8-byte-divided offset to a byte offset. */
inline int32_t to_byte_offset(slot_offset8_t offset8) {
    return offset8 * 8;
}


/** Poor man's normalized key type. to speed up comparison this should be an integer type, not char[]. */
typedef uint16_t poor_man_key;

/** Returns the value of poor-man's normalized key for the given key string WITHOUT prefix.*/
inline poor_man_key extract_poor_man_key (const void* key, size_t key_len) {
    if (key_len == 0) {
        return 0;
    } else if (key_len == 1) {
        return *reinterpret_cast<const unsigned char*>(key) << 8;
    } else {
        return deserialize16_ho(key);
    }
}
/** Returns the value of poor-man's normalized key for the given key string WITH prefix.*/
inline poor_man_key extract_poor_man_key (const void* key_with_prefix, size_t key_len_with_prefix, size_t prefix_len) {
    w_assert3(prefix_len <= key_len_with_prefix);
    return extract_poor_man_key (((const char*)key_with_prefix) + prefix_len, key_len_with_prefix - prefix_len);
}


class generic_page_header {
public:
    enum {
        page_sz         = SM_PAGESIZE,
//        hdr_sz        = 64, // NOTICE always sync with the offsets below
        //generic_hdr_sz  = 40, // NOTICE always sync with the offsets below
        /** Poor man's normalized key length. */
        poormkey_sz     = sizeof (poor_man_key),
        slot_sz         = sizeof(slot_offset8_t) + poormkey_sz
    };

 
    /** LSN (Log Sequence Number) of the last write to this page. */
    lsn_t    lsn;      // +8 -> 8
    
    /** ID of the page.*/
    lpid_t    pid;      // +12 -> 20
    
    /** tag_t. */
    uint16_t    tag;     // +2 -> 22
    
    /** total number of slots including every type of slot. */
    slot_index_t  nslots;   // +2 -> 24
    
    /** number of ghost records. */
    slot_index_t  nghosts; // +2 -> 26
    
    /** offset to beginning of record area (location of record that is located left-most). */
    slot_offset8_t  record_head8;     // +2 -> 28
    int32_t     get_record_head_byte() const {return to_byte_offset(record_head8);}

    /** unused. */
    uint32_t    _private_store_flags; // +4 -> 32

    uint32_t    get_page_storeflags() const { return _private_store_flags;}
    uint32_t    set_page_storeflags(uint32_t f) { 
                         return (_private_store_flags=f);}
    /** page_flag_t. */
    uint32_t    page_flags;        //  +4 -> 36


    /**
     * Checksum of this page.
     * Checksum is calculated from various parts of this page
     * and stored when this page is written out to disk from bufferpool.
     * It's checked when the bufferpool first reads the page from
     * the disk and not checked afterwards.
     */
    mutable uint32_t    checksum; // +4 -> 64
    
    /** Calculate the correct value of checksum of this page. */
    uint32_t    calculate_checksum () const;
    /**
     * Renew the stored value of checksum of this page.
     * Note that this is a const function. checksum is mutable property.
     */
    void       update_checksum () const {checksum = calculate_checksum();}
};


inline uint32_t generic_page_header::calculate_checksum () const {
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



/**
 * \brief Basic page structure for all pages.
 * 
 * \details
 * These are persistent things. There is no hierarchy here
 * for the different page types. All the differences between
 * page types are handled by the handle classes, generic_page_h and its
 * derived classes.
 * 
 * \section BTree-specific page headers
 * This page layout also contains the headers just for BTree to optimize on
 * the performance of BTree.
 * Anyways, remaining page-types other than BTree are only stnode_page and alloc_page
 * For those page types, this header part is unused but not a big issue.
 * @see btree_page
 */
class generic_page : public generic_page_header {
public:
    generic_page() {
        w_assert1(data - (const char *)this == sizeof(generic_page_header));
    }
    ~generic_page() { }


private:
    /* MUST BE 8-BYTE ALIGNED HERE */
    char     data[page_sz - sizeof(generic_page_header)];        // must be aligned
};

#endif
