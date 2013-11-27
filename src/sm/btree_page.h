/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef BTREE_PAGE_H
#define BTREE_PAGE_H

#include "fixable_page_h.h"
#include "w_defines.h"
#include "w_endian.h"
#include "vec_t.h"



/**
* offset divided by 8 (all records are 8-byte aligned).
* negative value means ghost records.
*/
typedef int16_t  slot_offset8_t;
typedef uint16_t slot_length_t;
typedef int16_t  slot_index_t; // to avoid explicit sized-types below
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
inline poor_man_key extract_poor_man_key (const cvec_t& key) {
    char start[2];
    key.copy_to(start, 2);
    return extract_poor_man_key(start, key.size());
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



    




    void init_items();

    int number_of_items()  const { return nitems;}
    int number_of_ghosts() const { return nghosts;}

    bool is_ghost(int item) const;
    void set_ghost(int item);
    void unset_ghost(int item);

    uint16_t& item_data16(int item);
    uint16_t  item_data16(int item) const;
    int32_t&  item_data32(int item);

    char* item_data(int item);
    int item_length(int item) const;

    bool insert_item(int item, bool ghost, uint16_t data16, int32_t data32, size_t data_length);
    bool insert_item(int item, bool ghost, uint16_t data16, int32_t data32, const cvec_t& data);
    bool resize_item(int item, size_t new_length, size_t keep_old);
    bool replace_item_data(int item, const cvec_t& new_data, size_t keep_old);



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
                char          key[4]; // <<<>>> really key_len - prefix_len
            } leaf;
            struct {
                shpid_t       child;
                slot_length_t slot_len;
                char          key[2]; // <<<>>> really slot_len - sizeof(child) - sizeof(slot_len)
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




    int usable_space() const {
        return record_head8*8 - nitems*4; // <<<>>>
    }


    char* slot_start(slot_index_t slot) {
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
        slot_offset8_t offset = head[slot].offset;
        if (offset < 0) offset = -offset; // ghost record

        if (slot == 0) {
            return body[offset].fence.slot_len;
        }

        if (btree_level == 1) {
            return body[offset].leaf.slot_len;
        } else {
            return body[offset].interior.slot_len;
        }
    }
    slot_length_t slot_length8(slot_index_t slot) const { return (slot_length(slot)-1)/8+1; }

    void set_slot_length(slot_index_t slot, slot_length_t length) {
        slot_offset8_t offset = head[slot].offset;
        if (offset < 0) offset = -offset; // ghost record

        if (slot == 0) {
            body[offset].fence.slot_len = length;
            return;
        }

        if (btree_level == 1) {
            body[offset].leaf.slot_len = length;
        } else {
            body[offset].interior.slot_len = length;
        }
    }

    bool insert_slot(slot_index_t slot, bool ghost, size_t length, poor_man_key poor_key);
    bool resize_slot(slot_index_t slot, size_t length, bool keep_old);
    void delete_slot(slot_index_t slot);

    bool _slots_are_consistent() const;
    void compact();
};
static_assert(sizeof(btree_page) == sizeof(generic_page), 
              "btree_page has wrong length");
static_assert(sizeof(btree_page::slot_head) == 4, "slot_head has wrong length");
static_assert(sizeof(btree_page::slot_body) == 8, "slot_body has wrong length");



inline bool btree_page::is_ghost(int item) const { 
    w_assert1(item>=0 && item<nitems);
    return head[item].offset < 0; 
}

inline uint16_t& btree_page::item_data16(int item) {
    w_assert1(item>=0 && item<nitems);
    return head[item].poor;    
}
inline uint16_t btree_page::item_data16(int item) const {
    w_assert1(item>=0 && item<nitems);
    return head[item].poor;    
}

inline int32_t& btree_page::item_data32(int item) {
    w_assert1(item>=0 && item<nitems);
    w_assert1(btree_level != 1); // <<<>>>
    return *(int32_t*)&slot_value(item).interior.child;
}

inline int btree_page::item_length(int item) const {
    w_assert1(item>=0 && item<nitems);
    int length = slot_length(item) - sizeof(slot_length_t);
    if (item != 0 && btree_level != 1) {
        length -= sizeof(shpid_t);
    }
    return length;
}
inline char* btree_page::item_data(int item) {
    w_assert1(item>=0 && item<nitems);
    if (item == 0) {
        return &slot_value(item).fence.low[0];
    } if (btree_level == 1) {
        return (char*)&slot_value(item).leaf.key_len;
    } else {
        return &slot_value(item).interior.key[0];
    }
}

#endif // BTREE_PAGE_H
