/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef BTREE_PAGE_H
#define BTREE_PAGE_H

#include "fixable_page_h.h"
#include "w_defines.h"
#include "w_endian.h"




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


/**
 * \brief Poor man's normalized key type.
 *
 * \details
 * To speed up comparison this should be an integer type, not char[]. 
 */
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




class btree_page_header : public generic_page_header {
    friend class btree_page;
public: // FIXME: kludge to allow test_bf_tree.cpp to function for now <<<>>>

    enum {
        /** Poor man's normalized key length. */
        poormkey_sz     = sizeof (poor_man_key),
        slot_sz         = sizeof(slot_offset8_t) + poormkey_sz
    };


    // ======================================================================
    //   BEGIN: BTree specific headers
    // ======================================================================

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
    shpid_t    btree_foster;  // +4 -> 48
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
     * length of high-fence key of the foster chain. 0 if not in a foster chain or right-most of a chain.
     * Corresponding data is stored in the first slot after high fence key.
     * When this page belongs to a foster chain,
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

    // ======================================================================
    //   END: BTree specific headers
    // ======================================================================
};



class btree_page : public btree_page_header {
public: // FIXME: kludge to allow test_bf_tree.cpp to function for now <<<>>>

    enum {
        data_sz = page_sz - sizeof(btree_page_header) - 8, // <<<>>>
        hdr_sz  = sizeof(btree_page_header) + 8,
    };



    friend class btree_ghost_mark_log;
    friend class btree_ghost_reclaim_log;
    friend class btree_ghost_t;
    friend class btree_header_t;
    friend class btree_impl;
    friend class btree_page_h;

    btree_page() {
        //w_assert1(0);  // FIXME: is this constructor ever called? yes it is (test_btree_ghost)
        w_assert1((body[0].raw - (const char *)this) % 4 == 0);     // check alignment<<<>>>
        w_assert1(((const char *)&nitems - (const char *)this) % 4 == 0);     // check alignment<<<>>>
        //w_assert1(((const char *)&record_head8 - (const char *)this) % 8 == 0);     // check alignment<<<>>>
        w_assert1((body[0].raw - (const char *)this) % 8 == 0);     // check alignment
        w_assert1(body[0].raw - (const char *) this == hdr_sz);
    }
    ~btree_page() { }


//private:
    /// total number of items
    slot_index_t  nitems;   // +2 -> 24
public:
    
private:
    /// number of ghost items
    slot_index_t  nghosts; // +2 -> 26
public:

    /** offset to beginning of record area (location of record that is located left-most). */
    slot_offset8_t  record_head8;     // +2 -> 28
    int32_t     get_record_head_byte() const {return to_byte_offset(record_head8);}

    uint16_t padding; // <<<>>>



    




    int number_of_items()  { return nitems;}
    int number_of_ghosts() { return nghosts;}

    bool is_ghost(int item) const;
    void set_ghost(int item);
    void unset_ghost(int item);






    typedef struct {
        slot_offset8_t offset;
        poor_man_key   poor;
    } slot_head;

    typedef struct {
        union {
            char raw[8];
            struct {
                slot_length_t slot_len;
                slot_length_t key_len;
                char          key[4]; // <<<>>>
            } leaf;
            struct {
                shpid_t       child;
                slot_length_t slot_len;
                char          key[2]; // <<<>>>
            } interior;
            struct {
                slot_length_t slot_len;
                char          low[6]; // low[btree_fence_low_length]
                //char        high_noprefix[btree_fence_high_length - btree_prefix_length]
                //char        chain[btree_chain_fence_high_length]
            } fence;
        };
    } slot_body;
        
    /* MUST BE 8-BYTE ALIGNED HERE */
    union {
        slot_head head[data_sz/sizeof(slot_head)];
        slot_body body[data_sz/sizeof(slot_body)];
    };

    void init_slots();

    poor_man_key poor(slot_index_t slot) const { return head[slot].poor; }



    int usable_space() const {
        return record_head8*8 - nitems*4; // <<<>>>
    }


    void* slot_start(slot_index_t slot) {
        slot_offset8_t offset = head[slot].offset;
        if (offset < 0) offset = -offset; // ghost record
        return body[offset].raw;
    }
    slot_body& slot_value(slot_index_t slot) {
        slot_offset8_t offset = head[slot].offset;
        if (offset < 0) offset = -offset; // ghost record
        return body[offset];
    }


    slot_length_t slot_length(slot_index_t slot) const {
        if (slot == 0) {
            return sizeof(slot_length_t) + btree_fence_low_length 
                + btree_fence_high_length-btree_prefix_length 
                + btree_chain_fence_high_length;
        }

        slot_offset8_t offset = head[slot].offset;
        if (offset < 0) offset = -offset; // ghost record

        if (btree_level == 1) {
            return body[offset].leaf.slot_len;
        } else {
            return body[offset].interior.slot_len;
        }
    }
    slot_length_t slot_length8(slot_index_t slot) const { return (slot_length(slot)-1)/8+1; }

    bool insert_slot(slot_index_t slot, bool ghost, size_t length, poor_man_key poor_key);
    bool resize_slot(slot_index_t slot, size_t length, bool keep_old);
    void delete_slot(slot_index_t slot);

    bool _slots_are_consistent() const;
    void compact();

private:
    char*      data_addr8(slot_offset8_t offset8) {
        return &body[offset8].raw[0];
    }
    const char* data_addr8(slot_offset8_t offset8) const {
        return &body[offset8].raw[0];
    }
};
static_assert(sizeof(btree_page) == sizeof(generic_page), 
              "btree_page has wrong length");
static_assert(sizeof(btree_page::slot_head) == 4, "slot_head has wrong length");
static_assert(sizeof(btree_page::slot_body) == 8, "slot_body has wrong length");



inline bool btree_page::is_ghost(int item) const { 
    w_assert1(item>=0 && item<nitems);

    return head[item].offset < 0; 
}

#endif // BTREE_PAGE_H
