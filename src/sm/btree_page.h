/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BTREE_PAGE_H
#define BTREE_PAGE_H

#include "fixable_page_h.h"
#include "vec_t.h"


/**
 * \brief This class holds B-tree-specific headers as well as an ordered list
 * of \e items.
 * \ingroup SSMBTREE
 * \details
 * Each item contains the following fixed-size fields:
 * \li ghost? (1 bit):   am I a ghost item?
 * \li poor   (2 bytes): leading bytes of an associated key for speeding up search
 *    (type poor_man_key)
 *
 * \li child  (4 bytes): child page ID; this field is present only in interior nodes
 *
 * The first two fields are stored so that for nearby items (in the
 * list order) they are likely to be in the same cache line; e.g.,
 * item_poor(n) and item_poor(n+1) are likely in the same cache line.
 * Each item also contains a variable-length data field, which can be
 * dynamically resized.
 *
 * For how these fields are used, including the representations used
 * with the variable-length data field, see the btree_page_h class.
 *
 * The contents of this class are separated out from btree_page to
 * increase access-control flexibility.  Its private members are
 * accessible by only itself while its protected members are
 * accessible to only those classes/members friended by btree_page.
 */
class btree_page_data : public generic_page_header {
public:
    enum {
        /// size of all header fields combined
        hdr_sz  = sizeof(generic_page_header) + 48,
        // 48 above must be kept in sync with size of headers below!
        // (checked by static asserts after class)

        /// size of region available to store items
        data_sz = sizeof(generic_page) - hdr_sz,
    };

protected:
    // ======================================================================
    //   BEGIN: BTree but not item-specific headers
    // ======================================================================

    /**
     * Page ID of the root page of the B-tree this node belongs to.
     * The root page ID of a B-tree is never changed even while the
     * tree grows or shrinks.
     *
     * This field is redundant and could later be removed by replacing
     * references to it with retrieving the root pid from pid via the
     * stnode_cache_t associated with the volume pid.vol() by looking
     * up store pid.store().
     */
    PageID btree_root;                         // +4 -> 4

    /// First child pointer in non-leaf nodes.
    PageID btree_pid0;                         // +4 -> 8

    /// Foster link page ID (0 if not linked).
    PageID btree_foster;                       // +4 -> 12

    /**
     * Level of this node in the B-tree, starting from the bottom
     * with 1.  In particular, 1 if we are a leaf and >1 if a
     * non-leaf (interior) node.
     */
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
     * length of high-fence key of the foster chain.  0 if not in a
     * foster chain or at the end of a foster chain.  Corresponding
     * data is stored in the first item after high fence key.
     */
    int16_t btree_chain_fence_high_length;      // +2 -> 20

    /**
     * Common prefix length for this page.  All keys of this page
     * except possibly the Foster key share (at least) this many
     * prefix bytes.
     *
     * btree_page_t compresses all keys except the low fence key and
     * the Foster key by removing these prefix bytes.
     */
    int16_t btree_prefix_length;                // +2 -> 22

    /**
     * Count of consecutive insertions to right-most or left-most.
     *
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


    // ======================================================================
    //   BEGIN: protected item interface
    // ======================================================================

    /**
     * Initialize item storage area, erasing any existing items.
     * btree_level must be set before hand and not changed afterwards.
     */
    void          init_items();

    // Remove the largest 'item_count' items from the storage area
    // It erases existing items from storage therefore use with caution
    // The function should only be called by full logging page rebalance
    // restart operation to recovery the source page
    // item_count - number of records to remove
    // high - the new high fence after record removal
    void          remove_items(const int item_count, const w_keystr_t &high);

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

    /**
     * Return a reference to the child pointer data for the given
     * item.  The reference will be 4 byte aligned and thus a suitable
     * target for atomic operations.
     * @pre this is an interior page
     */
    PageID&      item_child(int item);

    /**
     * return a pointer to the variable-length data of the given item
     *
     * the variable length data occupies item_data(item)
     * ... item_data(item)+item_length(item)-1
     */
    char*         item_data(int item);

    /**
     * return the amount of variable-length data belonging to the
     * given item in bytes
     */
    size_t        item_length(int item) const;


    /**
     * Attempt to insert a new item at given item position, pushing
     * existing items at and after that position upwards.
     * E.g. inserting to item 1 makes the old item 1 (if any) now item
     * 2, old item 2 (if any) now item 3, and so on.  Item position
     * may be one beyond the last existing item position (i.e.,
     * number_of_items()) in order to insert the new item at the end.
     *
     * Returns false iff the insert fails due to inadequate available
     * space (i.e., predict_item_space(data_length) > usable_space()).
     *
     * The inserted item is a ghost iff ghost is set and has
     * poor_man_key data poor, child child, and variable-length data
     * of length data_length.  child must be 0 if this is a leaf page.
     *
     * The new item's variable-length data is allocated but not
     * initialized.
     */
    bool          insert_item(int item, bool ghost, poor_man_key poor, PageID child,
                              size_t data_length);

    /**
     * Attempt to insert a new item at given item position, pushing
     * existing items at and after that position upwards.
     * E.g. inserting to item 1 makes the old item 1 (if any) now item
     * 2, old item 2 (if any) now item 3, and so on.  Item position
     * may be one beyond the last existing item position (i.e.,
     * number_of_items()) in order to insert the new item at the end.
     *
     * Returns false iff the insert fails due to inadequate available
     * space (i.e., predict_item_space(data_length) > usable_space()).
     *
     * The inserted item is a ghost iff ghost is set and has
     * poor_man_key data poor, child child, and variable-length data
     * data.  child must be 0 if this is a leaf page.
     */
    bool          insert_item(int item, bool ghost, poor_man_key poor, PageID child,
                              const cvec_t& data);

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
     * Attempt to replace all the variable-length data of the given
     * item after the first offset bytes with new_data, resizing the
     * item's variable length data as needed.  Returns false iff it
     * fails due to inadequate available space.
     *
     * @pre keep_old <= item_length(item)
     */
    bool          replace_item_data(int item, size_t offset, const cvec_t& new_data);

    /**
     * delete the given item, moving down items to take its place.
     * E.g., deleting item 1 makes the old item 2, if any, now item 1
     * and so on.  Compaction (compact()) may be required to make the
     * freed space available.
     */
    void          delete_item(int item);

    /**
     * delete the given range of items. This is used in a page split to delete
     * the mived records from the overflowing page. "to" is an exclusive
     * boundary, while "from" is inclusive, i.e., (from - to) items are
     * deleted in total
     */
    void          delete_range(int from, int to);

    /**
     * remove 'amount' leading bytes from each item data, starting at offset
     * 'pos"
     *
     * Example:
     * current data = AABBCC
     * after truncate_all(2, 2) = AACC
     */
    void truncate_all(size_t amount, size_t pos);


    /**
     * return total space currently occupied by given item, including
     * any overhead such as padding for alignment.
     */
    size_t        item_space(int item) const;

    /**
     * Calculate how much space would be occupied by a new item that
     * was added to this page with data_length amount of
     * variable-length data.  (E.g., after insert, what will
     * item_space return?)
     */
    size_t        predict_item_space(size_t data_length) const;

    /**
     * Return amount of available space for inserting/resizing.
     * Calling compact() may increase this number.
     */
    size_t        usable_space() const;

    /// compact item space, making all freed space available.
    void          compact();


    /**
     * returns the part of this page holding the available space; this
     * region does not contain data and may be discarded when saving
     * and filled with undefined values when restoring.  Ideally, call
     * compact() first when doing this to maximize the bytes that may
     * be ignored.  (See page_img_format_t for a use of this.)
     */
    char*         unused_part(size_t& length);

    /**
     * the maximum possible item overhead beyond a item's
     * variable-length data.  That is, item_space(X) <=
     * max_item_overhead + X always.
     */
    static const size_t max_item_overhead;

    /**
     * [debugging] are item allocations consistent?  E.g., do two item
     * allocations overlap?  This should always be true.
     */
    bool          _items_are_consistent() const;

private:
// ======================================================================
//   BEGIN: private implementation details
// ======================================================================

    typedef uint16_t item_index_t;
    typedef uint16_t item_length_t;
    /**
     * An offset denoting a particular item body, namely body[abs(<offset>)].
     * In some cases, the sign bit is used to encode ghostness.
     */
    typedef int16_t  body_offset_t;

    // ======================================================================
    //   BEGIN: item-specific headers
    // ======================================================================

    /// current number of items
    item_index_t  nitems;                          // +2 -> 26

    /// number of current ghost items
    item_index_t  nghosts;                         // +2 -> 28

    /// offset to beginning of used item bodies (# of used item body that is located left-most).
    body_offset_t first_used_body;                 // +2 -> 30

    /// padding to ensure header size is a multiple of 8
    uint16_t      padding;                         // +2 -> 32

    // ======================================================================
    //   END: item-specific headers
    // ======================================================================

protected:
    // ======================================================================
    //   BEGIN: Single-Page-Recovery-related headers (placed at last so that frequently used
    //  headers like nitems are in the first 64 bytes (one cacheline).
    // ======================================================================
    /**
     * Expected-Minimum LSN for the first child pointer.
     * 0 if this page is leaf or left-most.
     * \ingroup Single-Page-Recovery
     */
    lsn_t   btree_pid0_emlsn;   // +8 -> 40

    /**
     * Expected-Minimum LSN for the foster-child pointer.
     * 0 if this page doesn't have foster child.
     * \ingroup Single-Page-Recovery
     */
    lsn_t   btree_foster_emlsn; // +8 -> 48
    // ======================================================================
    //   END: Single-Page-Recovery-related headers
    // ======================================================================

private:
    /*
     * The item space is organized as follows:
     *
     *   head_0 head_1 ... head_I  <possible gap> body_J body_J+1 ... body_N
     *
     * where head_i is a fixed sized value of type item_head that
     * contains the poor_man_key, the ghost bit, and a offset to a
     * starting item_body for item # i.  As items are added, space is
     * consumed on both sides of the gap: at the beginning for another
     * item_head and at the end for one or more item bodies to store
     * the remaining data, namely the variable-size data field and
     * (for interior nodes only) the child pointer.
     *
     * Here, I is nitems+1, J is first_used_body, and N is max_bodies-1.
     */

    typedef struct {
        /**
         * sign bit: is this a ghost item?  (<0 => yes)
         * first item_body belonging to this item is body[abs(offset)]
         */
        body_offset_t offset;
        poor_man_key  poor;
    } item_head;
    //static_assert(sizeof(item_head) == 4, "item_head has wrong length");
    BOOST_STATIC_ASSERT(sizeof(item_head) == 4);

    typedef struct {
        // item format depends on whether we are a leaf or not:
        union {
            struct {
                item_length_t item_len;
                /// really of size item_len - sizeof(item_len):
                char          item_data[6];
            } leaf;
            struct {
                PageID       child;
                item_length_t item_len;
                /// really of size item_len - sizeof(item_len) - sizeof(child):
                char          item_data[2];
            } interior;
            /**
             * We use 8 byte alignment instead of the required 4 for
             * historical reasons at this point:
             */
            int64_t _for_alignment_only;
        };
    } item_body;
    BOOST_STATIC_ASSERT(sizeof(item_body) == 8);

    BOOST_STATIC_ASSERT(data_sz%sizeof(item_body) == 0);
    enum {
        max_heads  = data_sz/sizeof(item_head),
        max_bodies = data_sz/sizeof(item_body),
        /** Bytes that should be subtracted from item_len for actual data in leaf. */
        leaf_overhead = sizeof(item_length_t),
        /** Bytes that should be subtracted from item_len for actual data in interior. */
        interior_overhead = sizeof(item_length_t) + sizeof(PageID),
    };

    union {
        item_head head[max_heads];
        item_body body[max_bodies];
    };
    // check field sizes are large enough:
    /*
     * #define here is a workaround for ebrowse cannot handle < in
     * marcos calls, numeric_limits::min() is not constant
     */
#define STATIC_LESS_THAN(x,y)  BOOST_STATIC_ASSERT((x) < (y))
    STATIC_LESS_THAN(data_sz,    1<<(sizeof(item_length_t)*8));
    STATIC_LESS_THAN(max_heads,  1<<(sizeof(item_index_t) *8));
    STATIC_LESS_THAN(max_bodies, 1<<(sizeof(body_offset_t)*8-1)); // -1 for ghost bit


    /// are we a leaf node?
    bool is_leaf() const { return btree_level == 1; }

    /// align to item_body alignment boundary (integral multiple of item_body's)
    static size_t _item_align(size_t i) { return (i+sizeof(item_body)-1)&~(sizeof(item_body)-1); }

    /// add this to data length to get bytes used in item bodies (not counting padding)
    size_t _item_body_overhead() const;

    /**
     * return reference to current number of bytes used in item bodies
     * (not counting padding) associated with the item starting at
     * offset offset
     */
    item_length_t& _item_body_length(body_offset_t offset);
    /**
     * return current number of bytes used in item bodies (not
     * counting padding) associated with the item starting at offset
     * offset
     */
    item_length_t _item_body_length(body_offset_t offset) const;

    /**
     * return number of item bodies holding data for the items
     * starting at offset offset
     */
    body_offset_t _item_bodies(body_offset_t offset) const;

public:
    friend std::ostream& operator<<(std::ostream&, btree_page_data&);

    bool eq(const btree_page_data&) const;
};



// forward references for friending test...
class ss_m;
class test_volume_t;

/**
 * \brief B-tree page.
 *
 * \details
 * These pages contain the data that makes up B-trees.
 *
 * The implementation is spread between this class' superclass,
 * btree_page_data, and its handle class, btree_page_h.  The
 * superclass contains the basic fields and dynamically-sized--item
 * list implementation while the secrets of their usage are contained
 * in the handle class.
 *
 * This class itself contains only friend declarations; those
 * classes/members it friends have access to the protected but not
 * private members of the superclass.
 */
class btree_page : public btree_page_data {
    friend class btree_page_h;
    friend class page_img_format_t; // for unused_part()
    friend class btree_split_log;


    // _ux_deadopt_foster_apply_foster_parent
    // _ux_adopt_foster_apply_child
    friend class btree_impl;


    friend class test_bf_tree;
    friend w_rc_t test_bf_fix_virgin_root(ss_m* /*ssm*/, test_volume_t *test_volume);
    // these are for access to headers from btree_page_headers:
    friend w_rc_t test_bf_fix_virgin_child(ss_m* /*ssm*/, test_volume_t *test_volume);
    friend w_rc_t test_bf_evict(ss_m* /*ssm*/, test_volume_t *test_volume);
    friend w_rc_t _test_bf_swizzle(ss_m* /*ssm*/, test_volume_t *test_volume, bool enable_swizzle);
};
//static_assert(sizeof(btree_page) == sizeof(generic_page),
//              "btree_page has wrong length");
BOOST_STATIC_ASSERT(sizeof(btree_page) == sizeof(generic_page));



/***************************************************************************/
/*                                                                         */
/* Rest of file is inlined method implementations                          */
/*                                                                         */
/***************************************************************************/

inline size_t btree_page_data::_item_body_overhead() const {
    if (is_leaf()) {
        return leaf_overhead;
    } else {
        return interior_overhead;
    }
}

inline btree_page_data::item_length_t& btree_page_data::_item_body_length(body_offset_t offset) {
    w_assert1(offset >= 0);
    if (is_leaf()) {
        return body[offset].leaf.item_len;
    } else {
        return body[offset].interior.item_len;
    }
}
inline btree_page_data::item_length_t btree_page_data::_item_body_length(body_offset_t offset) const {
    w_assert1(offset >= 0);
    if (is_leaf()) {
        return body[offset].leaf.item_len;
    } else {
        return body[offset].interior.item_len;
    }
}

inline btree_page_data::body_offset_t btree_page_data::_item_bodies(body_offset_t offset) const {
    w_assert1(offset >= 0);
    return _item_align(_item_body_length(offset))/sizeof(item_body);
}


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

inline PageID& btree_page_data::item_child(int item) {
    w_assert1(item>=0 && item<nitems);
    w_assert1(!is_leaf());

    body_offset_t offset = head[item].offset;
    if (offset < 0) {
        offset = -offset;
    }
    return body[offset].interior.child;
}


inline char* btree_page_data::item_data(int item) {
    w_assert1(item>=0 && item<nitems);
    body_offset_t offset = head[item].offset;
    if (offset < 0) {
        offset = -offset;
    }
    if (is_leaf()) {
        return body[offset].leaf.item_data;
    } else {
        return body[offset].interior.item_data;
    }
}

inline size_t btree_page_data::item_length(int item) const {
    w_assert1(item>=0 && item<nitems);
    body_offset_t offset = head[item].offset;
    if (offset < 0) {
        offset = -offset;
    }

    int length;
    if (is_leaf()) {
        length = body[offset].leaf.item_len - leaf_overhead;
    } else {
        length = body[offset].interior.item_len - interior_overhead;
    }
    w_assert1(length >= 0);
    return length;
}


inline size_t btree_page_data::predict_item_space(size_t data_length) const {
    size_t body_length = data_length + _item_body_overhead();
    return _item_align(body_length) + sizeof(item_head);
}

inline size_t btree_page_data::item_space(int item) const {
    w_assert1(item>=0 && item<nitems);
    body_offset_t offset = head[item].offset;
    if (offset < 0) {
        offset = -offset;
    }

    return _item_align(_item_body_length(offset)) + sizeof(item_head);
}


inline size_t btree_page_data::usable_space() const {
    w_assert1(first_used_body*sizeof(item_body) >= nitems*sizeof(item_head));
    return    first_used_body*sizeof(item_body) -  nitems*sizeof(item_head);
}



/**
 * C++ version of Linux kernel's ACCESS_ONCE() macro
 *
 * Prevent the compiler from merging or refetching accesses.  The compiler
 * is also forbidden from reordering successive instances of ACCESS_ONCE(),
 * but only when the compiler is aware of some particular ordering.  One way
 * to make the compiler aware of ordering is to put the two invocations of
 * ACCESS_ONCE() in different C statements.
 *
 * This does absolutely -nothing- to prevent the CPU from reordering,
 * merging, or refetching absolutely anything at any time.
 */
template<typename T>
inline T volatile &ACCESS_ONCE(T &t) {
    return static_cast<T volatile &>(t);
}

#endif // BTREE_PAGE_H

