#ifndef PAGE_S_H
#define PAGE_S_H

#include "w_defines.h"
#include "w_endian.h"

#ifdef __GNUG__
#pragma interface
#endif

enum tag_t {
    t_bad_p            = 0,        // not used
    t_alloc_p        = 1,        // free-page allocation page 
    t_stnode_p         = 2,        // store node page
    t_btree_p          = 5,        // btree page 
    t_any_p            = 11        // indifferent
};
enum page_flag_t {
    t_tobedeleted    = 0x01,        // this page will be deleted as soon as the page is evicted from bufferpool
    t_virgin         = 0x02,        // newly allocated page
    t_written        = 0x08        // read in from disk
};

class xct_t;

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


/**\brief Basic page structure for all pages.
 * \details
 * These are persistent things. There is no hierarchy here
 * for the different page types. All the differences between
 * page types are handled by the handle classes, page_p and its
 * derived classes.
 * 
 * \section BTree-specific page headers
 * This page layout also contains the headers just for BTree to optimize on
 * the performance of BTree.
 * Anyways, remaining page-types other than BTree are only stnode and extlink.
 * For those page types, this header part is unused but not a big issue.
 * @see btree_p
 */
class page_s {
public:
    enum {
        page_sz = SM_PAGESIZE,
        hdr_sz = 64, // NOTICE always sync with the offsets below
        data_sz = page_sz - hdr_sz,
        /** Poor man's normalized key length. */
        poormkey_sz = sizeof (poor_man_key),
        slot_sz = sizeof(slot_offset8_t) + poormkey_sz
    };

 
    /** LSN (Log Sequence Number) of the last write to this page. */
    lsn_t    lsn;      // +8 -> 8
    
    /** id of the page.*/
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

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: BTree specific headers
///==========================================
#endif // DOXYGEN_HIDE

    /**
    * root page used for recovery (root page is never changed even while grow/shrink).
    * This can be removed by retrieving it from full pageid (storeid->root page id),
    * but let's do that later.
    */
    shpid_t    btree_root; // +4 -> 40
    /** first ptr in non-leaf nodes. used only in left-most non-leaf nodes. */
    shpid_t    btree_pid0; // +4 -> 44
    /**
    * B-link page (0 if not linked).
    * kind of "next", but other nodes don't know about it yet.
    */
    shpid_t    btree_blink;  // +4 -> 48
    /** 1 if leaf, >1 if non-leaf. */
    int16_t    btree_level; // +2 -> 50
    /**
    * length of low-fence key.
    * Corresponding data is stored in the first slot.
    */
    int16_t    btree_fence_low_length;  // +2 -> 52
    /**
    * length of high-fence key.
    * Corresponding data is stored in the first slot after low fence key.
    */
    int16_t    btree_fence_high_length;  // +2 -> 54
    /**
     * length of high-fence key of the Blink chain. 0 if not in a Blink chain or right-most of a chain.
     * Corresponding data is stored in the first slot after high fence key.
     * When this page belongs to a Blink chain,
     * we need to store high-fence of right-most sibling in every sibling
     * to do batch-verification with bitmaps.
     * @see btree_impl::_ux_verify_volume()
     */
    int16_t    btree_chain_fence_high_length; // +2 -> 56
    /**
    * counts of common leading bytes of the fence keys,
    * thereby of all entries in this page too.
    * 0=no prefix compression.
    * Corresponding data is NOT stored.
    * We can just use low fence key data.
    */
    int16_t    btree_prefix_length;  // +2 -> 58
    /**
    * Count of consecutive insertions to right-most or left-most.
    * Positive values mean skews towards right-most.
    * Negative values mean skews towards left-most.
    * Whenever this page receives an insertion into the middle,
    * this value is reset to zero.
    * Changes of this value will NOT be logged. It doesn't matter
    * in terms of correctness, so we don't care about undo/redo
    * of this header item.
    */
    int16_t   btree_consecutive_skewed_insertions; // +2 -> 60
#ifdef DOXYGEN_HIDE
///==========================================
///   END: BTree specific headers
///==========================================
#endif // DOXYGEN_HIDE

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

    char*      data_addr8(slot_offset8_t offset8) {
        return data + to_byte_offset(offset8);
    }
    const char* data_addr8(slot_offset8_t offset8) const {
        return data + to_byte_offset(offset8);
    }

    /* MUST BE 8-BYTE ALIGNED HERE */
    char     data[data_sz];        // must be aligned


    page_s() {
        w_assert1(data - (const char *) this == hdr_sz);
    }
    ~page_s() { }
};

const uint32_t PAGE_S_CHECKSUM_MULT = 0x35D0B891;

inline uint32_t page_s::calculate_checksum () const
{
    const unsigned char *end_p = (const unsigned char *) (data + SM_PAGESIZE - sizeof(uint32_t));
    uint64_t value = 0;
    // these values(23/511) are arbitrary, but make sure it doesn't touch
    // the checksum field (located around 60-th bytes) itself!
    for (const unsigned char *p = (const unsigned char *) data + 23; p < end_p; p += 511) {
        // be aware of alignment issue on spark! so this code is not safe
        // const uint32_t*next = reinterpret_cast<const uint32_t*>(p);
        // value ^= *next;
        value = value * PAGE_S_CHECKSUM_MULT + p[0];
        value = value * PAGE_S_CHECKSUM_MULT + p[1];
        value = value * PAGE_S_CHECKSUM_MULT + p[2];
        value = value * PAGE_S_CHECKSUM_MULT + p[3];
    }
    return ((uint32_t) (value >> 32)) ^ ((uint32_t) (value & 0xFFFFFFFF));
}

#endif
