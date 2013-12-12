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
 * The guts of btree_page, separated out from btree_page to increase
 * access control flexibility.  (When btree_page friends classes, it
 * conveys access only to the protected members of this class not its
 * private members.)
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */
class btree_page_data : public generic_page_header {
public:
    enum {
        /// size of all header fields combined
        hdr_sz  = sizeof(generic_page_header) + 32,
        // 32 above must be kept in sync with size of headers below!
        // (checked by static asserts after class)

        /// size of region available to store items
        data_sz = sizeof(generic_page) - hdr_sz,
    };


protected:
    // ======================================================================
    //   BEGIN: BTree but not item-specific headers
    // ======================================================================

    /**
     * Root page used for recovery (root page is never changed even
     * while grow/shrink).
     * This field could later be removed by instead retrieving this
     * value from full pageid (storeid->root page id), but let's do
     * that later.
     */
    shpid_t btree_root;                         // +4 -> 4

    /// first ptr in non-leaf nodes.  Used only in left-most non-leaf nodes. 
    shpid_t btree_pid0;                         // +4 -> 8

    /**
     * B-link page (0 if not linked).
     * kind of "next", but other nodes don't know about it yet.
     */
    shpid_t btree_foster;                       // +4 -> 12

    /// 1 if leaf, >1 if non-leaf. 
    int16_t btree_level;                        // +2 -> 14

    /**
     * length of low-fence key.
     * Corresponding data is stored in the first item.
     */
    int16_t btree_fence_low_length;             // +2 -> 16

    /**
     * length of high-fence key.
     * Corresponding data is stored in the first item after low fence key.
     */
    int16_t btree_fence_high_length;            // +2 -> 18

    /**
     * length of high-fence key of the foster chain.  0 if not in a foster chain or right-most of a chain.
     * Corresponding data is stored in the first item after high fence key.
     * When this page belongs to a foster chain,
     * we need to store high-fence of right-most sibling in every sibling
     * to do batch-verification with bitmaps.
     * @see btree_impl::_ux_verify_volume()
     */
    int16_t btree_chain_fence_high_length;      // +2 -> 20

    /**
     * counts of common leading bytes of the fence keys,
     * thereby of all entries in this page too.
     * 0=no prefix compression.
     * Corresponding data is NOT stored.
     * We can just use low fence key data.
     */
    int16_t btree_prefix_length;                // +2 -> 22

    /**
     * Count of consecutive insertions to right-most or left-most.
     * Positive values mean skews towards right-most.
     * Negative values mean skews towards left-most.
     * Whenever this page receives an insertion into the middle,
     * this value is reset to zero.
     * Changes of this value will NOT be logged.  It doesn't matter
     * in terms of correctness, so we don't care about undo/redo
     * of this header item.
     */
    int16_t btree_consecutive_skewed_insertions; // +2 -> 24

    // ======================================================================
    //   END: BTree but not item-specific headers
    // ======================================================================


protected:
    // ======================================================================
    //   BEGIN: protected item interface
    // ======================================================================

    /// initialize item storage area, erasing any existing items.
    /// btree_level must be set before hand and not changed afterwards.
    void          init_items();


    int           number_of_items()  const { return nitems;}

    /// return number of current items that are ghosts
    int           number_of_ghosts() const { return nghosts;}


    /// is the given item a ghost?
    bool          is_ghost(int item) const;

    /// turn the given item into a ghost item
    void          set_ghost(int item);

    /// turn the given item into a non-ghost item
    void          unset_ghost(int item);


    /// The type of poor_man_key data
    typedef uint16_t poor_man_key;

    /// return the poor_man_key data for the given item
    poor_man_key  item_poor(int item) const;

    /// return a reference to the poor_man_key data for the given item
    poor_man_key& item_poor(int item);

    /// return a reference to the child pointer data for the given
    /// item.  The reference will be word aligned and thus a suitable
    /// target for atomic operations.
    /// @pre this is a interior page
    shpid_t&      item_child(int item);

    /// return a pointer to the variable-length data of the given item
    ///
    /// the variable length data occupies item_data(item) ... item_data(item)+item_length-1
    char*         item_data(int item);

    /// return the amount of variable-length data belonging to the given item
    size_t        item_length(int item) const;


    /**
     * Attempt to insert a new item at given item position, pushing
     * existing items at and after that position upwards.
     * E.g. inserting to item 1 makes the old item 1 (if any) now item
     * 2, old item 2 (if any) now item 3, and so on.  Returns false
     * iff it fails due to inadequate available space (i.e.,
     * predict_item_space(data_length) < usable_space()).
     * 
     * The inserted item is a ghost if ghost is set and has
     * poor_man_key data poor, child child, and variable-length data
     * of length data_length.  child must be 0 if this is a leaf page.
     * 
     * The variable-length data is allocated but not initialized.
     */
    bool          insert_item(int item, bool ghost, poor_man_key poor, shpid_t child, size_t data_length);

    /**
     * Attempt to insert a new item at given item position, pushing
     * existing items at and after that position upwards.
     * E.g. inserting to item 1 makes the old item 1 (if any) now item
     * 2, old item 2 (if any) now item 3, and so on.  Returns false
     * iff it fails due to inadequate available space (i.e.,
     * predict_item_space(data_length) < usable_space()).
     * 
     * The inserted item is a ghost if ghost is set and has
     * poor_man_key data poor, child child, and variable-length data data.
     * child must be 0 if this is a leaf page.
     */
    bool          insert_item(int item, bool ghost, poor_man_key poor, shpid_t child, const cvec_t& data);

    /**
     * Attempt to resize the variable-length data of the given item to
     * new_length.  Preserves only the first keep_old bytes of the old
     * data; later bytes, if any, acquire undefined values.  Returns
     * false iff it fails due to inadequate available space.  (Growing
     * an item may require available space equivalent to inserting a
     * new item of the larger size.)
     * 
     * @pre keep_old <= item_length(item)
     */
    bool          resize_item(int item, size_t new_length, size_t keep_old);

    /**
     * Attempt to replace the variable-length data of the given item
     * after the first keep_old bytes with new_data, resizing the
     * item's variable length data as needed.  Returns false iff it
     * fails due to inadequate available space.
     * 
     * @pre keep_old <= item_length(item)
     */
    bool          replace_item_data(int item, const cvec_t& new_data, size_t keep_old);

    /**
     * delete the given item, moving down items to take its place.
     * E.g., deleting item 1 makes the old item 2, if any, now item 1
     * and so on.  Compaction (compact()) may be required to make the
     * freed space available.
     */
    void          delete_item(int item);


    /// return total space currently occupied by given item, including
    /// any overhead such as padding for alignment.
    size_t        item_space(int item) const;

    /// calculate how much space would be occupied by a new item that
    /// was added to this page with data_length amount of
    /// variable-length data.  (E.g., after insert, what will
    /// item_space return?)
    size_t        predict_item_space(size_t data_length);

    /// return amount of available space for inserting/resizing.
    /// Calling compact() may increase this number.
    size_t        usable_space() const;

    /// compact items, making all freed space available.
    void          compact();


    /**
     * returns the part of this page holding the available space; this
     * region does not contain data and may be discarded when saving
     * and filled with undefined values when restoring.  Ideally, call
     * compact() first when doing this to maximize the bytes that may
     * be ignored.  (See page_img_format_t for a use of this.)
     */
    char*         unused_part(size_t& length);

    /// the maximum possible item overhead beyond a item's
    /// variable-length data.  That is, item_space(X) <=
    /// max_item_overhead + X always.
    static const size_t max_item_overhead;

    /// [debugging] are item allocations consistent?  E.g., do two
    /// item allocations overlap?
    bool          _items_are_consistent() const;


private:
    /**
     * offset divided by 8 (all records are 8-byte aligned).
     * negative value means ghost records.
     */
    typedef int16_t body_offset_t;

    typedef int16_t item_index_t; // to avoid explicit sized-types below


    // ======================================================================
    //   BEGIN: item-specific headers
    // ======================================================================

    /// current number of items
    item_index_t  nitems;                          // +2 -> 26

    /// number of current ghost items
    item_index_t  nghosts;                         // +2 -> 28

    /// offset to beginning of record area (location of record that is located left-most). 
    body_offset_t  record_head8;                   // +2 -> 30

    /// padding to ensure header size is a multiple of 8
    uint16_t padding;                              // +2 -> 32

    // ======================================================================
    //   END: item-specific headers
    // ======================================================================


private:
    // ======================================================================
    //   BEGIN: 
    // ======================================================================
    btree_page_data() {
        //w_assert1(0);  // FIXME: is this constructor ever called? yes it is (test_btree_ghost)
        w_assert1((body[0].raw - (const char *)this) % 4 == 0);     // check alignment<<<>>>
        w_assert1(((const char *)&nitems - (const char *)this) % 4 == 0);     // check alignment<<<>>>
        //w_assert1(((const char *)&record_head8 - (const char *)this) % 8 == 0);     // check alignment<<<>>>
        w_assert1((body[0].raw - (const char *)this) % 8 == 0);     // check alignment
        w_assert1(body[0].raw - (const char *) this == hdr_sz);
    }
    ~btree_page_data() { }


    typedef uint16_t item_length_t;

    typedef struct {
        body_offset_t offset;
        poor_man_key       poor;
    } slot_head;

    typedef uint16_t tmp_key_length_t;  // <<<>>>

    typedef struct {
        union {
            char raw[8];
            struct {
                item_length_t slot_len;
                tmp_key_length_t  key_len;
                char          key[4]; // <<<>>> really key_len - prefix_len
            } leaf;
            struct {
                shpid_t       child;
                item_length_t slot_len;
                char          key[2]; // <<<>>> really slot_len - sizeof(child) - sizeof(slot_len)
            } interior;
            struct {
                item_length_t slot_len;
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

//    static_assert(sizeof(slot_head) == 4, "slot_head has wrong length");
    BOOST_STATIC_ASSERT(sizeof(slot_head) == 4);
//    static_assert(sizeof(slot_body) == 8, "slot_body has wrong length");
    BOOST_STATIC_ASSERT(sizeof(slot_body) == 8);




    char* slot_start(item_index_t slot) {
        body_offset_t offset = head[slot].offset;
        if (offset < 0) offset = -offset; // ghost record
        return body[offset].raw;
    }
    slot_body& slot_value(item_index_t slot) {
        body_offset_t offset = head[slot].offset;
        if (offset < 0) offset = -offset; // ghost record
        return body[offset];
    }


    item_length_t slot_length(item_index_t slot) const {
        body_offset_t offset = head[slot].offset;
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
    item_length_t slot_length8(item_index_t slot) const { return (slot_length(slot)-1)/8+1; }

    void set_slot_length(item_index_t slot, item_length_t length) {
        body_offset_t offset = head[slot].offset;
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
};



// these for friending test...
class ss_m;
class test_volume_t;

class btree_page : public btree_page_data {
    friend class btree_page_h;

    // _ux_deadopt_foster_apply_foster_parent
    // _ux_adopt_foster_apply_child
    friend class btree_impl;
    friend class btree_header_t;

    friend w_rc_t test_bf_fix_virgin_root(ss_m* /*ssm*/, test_volume_t *test_volume);

    friend class page_img_format_t; // for unused_part()
    friend class test_bf_tree;

    // these are for access to headers from btree_page_headers:
    friend w_rc_t test_bf_fix_virgin_child(ss_m* /*ssm*/, test_volume_t *test_volume);
    friend w_rc_t test_bf_evict(ss_m* /*ssm*/, test_volume_t *test_volume);
    friend w_rc_t _test_bf_swizzle(ss_m* /*ssm*/, test_volume_t *test_volume, bool enable_swizzle);
};
//static_assert(sizeof(btree_page) == sizeof(generic_page), 
//              "btree_page has wrong length");
BOOST_STATIC_ASSERT(sizeof(btree_page) == sizeof(generic_page));


inline bool btree_page_data::is_ghost(int item) const { 
    w_assert1(item>=0 && item<nitems);
    return head[item].offset < 0; 
}

inline btree_page_data::poor_man_key& btree_page_data::item_poor(int item) {
    w_assert1(item>=0 && item<nitems);
    return head[item].poor;    
}
inline btree_page_data::poor_man_key btree_page_data::item_poor(int item) const {
    w_assert1(item>=0 && item<nitems);
    return head[item].poor;    
}

inline shpid_t& btree_page_data::item_child(int item) {
    w_assert1(item>=0 && item<nitems);
    w_assert1(btree_level != 1); // <<<>>>
    return *(shpid_t*)&slot_value(item).interior.child;
}

inline size_t btree_page_data::item_length(int item) const {
    w_assert1(item>=0 && item<nitems);
    int length = slot_length(item) - sizeof(item_length_t);
    if (item != 0 && btree_level != 1) {
        length -= sizeof(shpid_t);
    }
    return length;
}
inline char* btree_page_data::item_data(int item) {
    w_assert1(item>=0 && item<nitems);
    if (item == 0) {
        return &slot_value(item).fence.low[0];
    } if (btree_level == 1) {
        return (char*)&slot_value(item).leaf.key_len;
    } else {
        return &slot_value(item).interior.key[0];
    }
}

inline size_t btree_page_data::usable_space() const {
    return record_head8*8 - nitems*4; // <<<>>>
}

#endif // BTREE_PAGE_H
