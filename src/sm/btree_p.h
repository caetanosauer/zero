/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef BTREE_P_H
#define BTREE_P_H

#include "w_defines.h"
#include "page.h"
#include "w_key.h"

struct btree_lf_stats_t;
struct btree_int_stats_t;



class btree_page : public generic_page_header {
    friend class btree_p;

    btree_page() {
        w_assert1(data - (const char *) this == hdr_sz);
    }
    ~btree_page() { }


    /* MUST BE 8-BYTE ALIGNED HERE */
    char     data[data_sz];        // must be aligned

    char*      data_addr8(slot_offset8_t offset8) {
        return data + to_byte_offset(offset8);
    }
    const char* data_addr8(slot_offset8_t offset8) const {
        return data + to_byte_offset(offset8);
    }
};



class btree_p;
/**
 * \brief Represents a record in BTree.
 * \ingroup SSMBTREE
 * \details
 * This class abstracts how we store key/record in our BTree,
 * which might be quite un-intuitive without this class.
 */
class btrec_t {
public:
    NORET            btrec_t()        {};
    NORET            btrec_t(const btree_p& page, slotid_t slot);
    NORET            ~btrec_t()        {};

    /**
    *  \brief Load up a reference to the tuple at "slot" in "page".
    * \details
    *  NB: here we are talking about record, not absolute slot# (slot
    *  0 is special on every page).   So here we use ORIGIN 0
    */
    btrec_t&           set(const btree_p& page, slotid_t slot);
    
    smsize_t           elen() const    { return _elem.size(); }

    const w_keystr_t&  key() const    { return _key; }
    const cvec_t&      elem() const     { return _elem; }
    shpid_t            child() const    { return _child; }
    bool               is_ghost_record() const { return _ghost_record; }

private:
    shpid_t         _child;
    w_keystr_t      _key;
    cvec_t          _elem;
    bool            _ghost_record;
    friend class btree_p;

    // disabled
    NORET            btrec_t(const btrec_t&);
    btrec_t&            operator=(const btrec_t&);
};

inline NORET
btrec_t::btrec_t(const btree_p& page, slotid_t slot)  
{
    set(page, slot);
}
class btree_impl;
class btree_ghost_t;
class btree_ghost_mark_log;
class btree_ghost_reclaim_log;



/**
 * \brief Page handle for BTree data page.
 * \ingroup SSMBTREE
 * \details
 * BTree data page uses the common data layout defined in generic_page.
 * However, it also has the BTree-specific header placed in the beginning of "data".
 *
 * \section FBTREE Fence Keys and B-link Tree
 * Our version of BTree header contains low-high fence keys and pointers as a b-link tree.
 * fence keys are NEVER changed once the page is created until the page gets splitted.
 * prefix compression in this scheme utilizes it by storing the common leading bytes
 * of the two fence keys and simply getting rid of them from all entries in this page.
 * See generic_page for the header definitions. They are defined as part of generic_page.
 *
 * \section PAGELAYOUT BTree Page Layout
 * The page layout of BTree is as follows.
 * \verbatim
  [common-headers in generic_page]
  [btree-specific-headers in generic_page]
  (data area in generic_page:
    [slot data which is growing forward]
    [(contiguous) free area]
    [record data which is growing forward]
 )\endverbatim
 * The first record contains fence keys:
 * [record-len + low-fence-key data (including prefix) + high-fence-key data (without prefix)
 * + chain-fence-high-key data (complete string. chain-fence-high doesn't share prefix!)]
 * 
 * \section SLOTLAYOUT BTree Slot Layout
 * btree_page is the only slotted subclass of generic_page_h. All the other classes  FIXME
 * use _pp.data just as a chunk of char. (Slot-related functions and typedefs
 * should be moved from generic_page_h to btree_p, but I haven't done the surgery yet.)
 * 
 * The Btree slot uses poor-man's normalized key to speed up searches.
 * Each slot stores the first few bytes of the key as an unsigned
 * integer (PoorMKey) so that comparison with most records are done without
 * going to the record itself, avoiding L1 cache misses. The whole point of
 * poormkey is cachemiss! So, we minimize the size of slots. Only poormkey and
 * offset (not tuple length or key length. it's at the beginning of the record).
 * 
 * NOTE So far, poor-man's normalied key is 2 byte integer (uint16_t) and
 * the corresponding bytes are NOT eliminated from the key string in the record.
 * This is to speed up the retrieval of the complete key at the cost of additional
 * 2 bytes to store it. I admit this is arguable, but deserilizing the part
 * everytime (it's likely little-endian, so we need to flip it) will slow down retrieval.
 * 
 * Each slot also stores the offset (divided by 8 to enable larger page sizes)
 * to the record. If the offset is negative, it means a ghost record and
 * the offset of reversed-sign gives the actual offset.
 * 
 * \section RECORDLAYOUT Record Layout
 * Internally, record data is stored as follows:
 * 
 * (If the page is leaf page)
 * - physical record length (uint16_t) of the entire record (including everything
 * AFTER prefix trunction; it's physical length!).
 * - key length (uint16_t) BEFORE prefix truncation
 * - key + el (contiguous char[]) AFTER prefix truncation
 * NOTE el length can be calculated from record length and key length
 * (el length = record length - 4 - key length + prefix length).
 * 
 * (If the page is node page)
 * - pid (shpid_t; 4 bytes.)
 * - physical record length (uint16_t) of the entire record.
 * - key AFTER prefix truncation
 * NOTE key length is calculated from record length (record length - 6 + prefix_len).
 * pid is placed first to make it 4-byte aligned (causes a trouble in SPARC otherwise).
 * 
 * Finally,
 * - [0-7] bytes of unused space (record is 8 bytes aligned). This is just a padding,
 * so not included in the record length.
 * 
 * Opaque pointers:
 * - Default is to return page id.
 * - When a page is swizzled though we can avoid hash table lookup to map page id 
 *   to frame id by using frame id directly. Opaque pointers server this purpose by 
 *   hiding from the user whether a pointer is a frame id or page id. 
 *
 */
class btree_p : public generic_page_h {
    friend class btree_impl;
    friend class btree_ghost_t;
    friend class btree_ghost_mark_log;
    friend class btree_ghost_reclaim_log;

    btree_page* page() const { return reinterpret_cast<btree_page*>(_pp); }

public:
#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Struct/Enum/Constructor
///==========================================
#endif // DOXYGEN_HIDE

    btree_p() {}
    btree_p(generic_page* s) : generic_page_h(s) {}
    btree_p(const btree_p&p) : generic_page_h(p) {} 
    ~btree_p() {}
    btree_p& operator=(btree_p& p)    { generic_page_h::operator=(p); return *this; }

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Header Get/Set functions
///==========================================
#endif // DOXYGEN_HIDE
    
    /** Returns 1 if leaf, >1 if non-leaf. */
    int               level() const;
    /** Returns left-most ptr (used only in non-leaf nodes). */
    shpid_t        pid0() const;
    /** Returns left-most opaque pointer (used only in non-leaf nodes). */
    shpid_t        pid0_opaqueptr() const;
    /** Returns root page used for recovery. */
    lpid_t           root() const;

    /** Returns if this page is a leaf page. */
    bool             is_leaf() const;
    /** Returns if this page is NOT a leaf page.*/
    bool             is_node() const;
    /**
    *    return true if this node is the lowest interior node,     *
    *    i.e., the parent of a leaf.  Used to tell how we should   *
    *    latch a child page : EX or SH                             *
    */
    bool             is_leaf_parent() const;
    
    /** Returns ID of B-link page (0 if not linked). */
    shpid_t         get_foster() const;
    /** Returns opaque pointer of B-link page (0 if not linked). */
    shpid_t         get_foster_opaqueptr() const;
    /** Clears the foster page and also clears the chain high fence key. */
    rc_t               clear_foster();
    /** Returns the prefix which are removed from all entries in this page. */
    const char* get_prefix_key() const;
    /** Returns the length of prefix key (0 means no prefix compression). */
    int16_t           get_prefix_length() const;
    /** Returns the low fence key, which is same OR smaller than all entries in this page and its descendants. */
    const char*  get_fence_low_key() const;
    /** Returns the length of low fence key. */
    int16_t           get_fence_low_length() const;
    /** Constructs w_keystr_t object containing the low-fence key of this page. */
    void                copy_fence_low_key(w_keystr_t &buffer) const {buffer.construct_from_keystr(get_fence_low_key(), get_fence_low_length());}
    /** Returns if the low-fence key is infimum. */
    bool              is_fence_low_infimum() const { return get_fence_low_key()[0] == SIGN_NEGINF;}

    /**
     * Returns the high fence key (without prefix), which is larger than all entries in this page and its descendants.
     * NOTE we don't provide get_fence_high_key() with prefix because the page eliminates prefix from fence-high.
     */
    const char*       get_fence_high_key_noprefix() const;
    /** Returns the length of high fence key with prefix. */
    int16_t           get_fence_high_length() const;
    /** Returns the length of high fence key without prefix. */
    int16_t           get_fence_high_length_noprefix() const {
        return get_fence_high_length() - get_prefix_length();
    }
    /** Constructs w_keystr_t object containing the low-fence key of this page. */
    void                copy_fence_high_key(w_keystr_t &buffer) const {
        buffer.construct_from_keystr(get_prefix_key(), get_prefix_length(),
            get_fence_high_key_noprefix(), get_fence_high_length_noprefix());
    }
    /** Returns if the high-fence key is supremum. */
    bool              is_fence_high_supremum() const { return get_prefix_length() == 0 && get_fence_high_key_noprefix()[0] == SIGN_POSINF;}

    /** Returns the high fence key of foster chain. */
    const char*  get_chain_fence_high_key() const;
    /** Returns the length of high fence key of foster chain. */
    int16_t           get_chain_fence_high_length() const;
    /** Constructs w_keystr_t object containing the low-fence key of this page. */
    void                copy_chain_fence_high_key(w_keystr_t &buffer) const {buffer.construct_from_keystr(get_chain_fence_high_key(), get_chain_fence_high_length());}
    /**
     * Returns if the given key can exist in the range specified by fence keys,
     * which is low-fence <= key < high-fence.
     */
    bool              fence_contains(const w_keystr_t &key) const;
    /**
     * Return value : 0 if equal.
     * : <0 if key < fence-low.
     * : >0 if key > fence-low.
     */
    int                 compare_with_fence_low (const w_keystr_t &key) const;
    /** overload for char*. */
    int                 compare_with_fence_low (const char* key, size_t key_len) const;
    /** used when the prefix part is already checked key/key_len must be WITHOUT prefix. */
    int                 compare_with_fence_low_noprefix (const char* key, size_t key_len) const;
    /**
     * Return value : 0 if equal.
     * : <0 if key < fence-high.
     * : >0 if key > fence-high.
     */
    int                 compare_with_fence_high (const w_keystr_t &key) const;
    /** overload for char*. */
    int                 compare_with_fence_high (const char* key, size_t key_len) const;
    /** used when the prefix part is already checked key/key_len must be WITHOUT prefix. */
    int                 compare_with_fence_high_noprefix (const char* key, size_t key_len) const;
    /**
     * Return value : 0 if equal.
     * : <0 if key < fence-high.
     * : >0 if key > fence-high.
     */
    int                 compare_with_chain_fence_high (const w_keystr_t &key) const;
    /** overload for char*. */
    int                 compare_with_chain_fence_high (const char* key, size_t key_len) const;
    // no 'noprefix' version because chain_fence_high might not share the prefix!

    /**
     * When allocating a new BTree page, use this instead of fix().
     * This sets all headers, fence/prefix keys, and initial records altogether.
     * As our new BTree header has variable-size part (fence keys),
     * setting fence keys later than the first format() causes a problem.
     * So, the 4-argumets format(which is called from default fix() on virgin page) is disabled.
     * Also, this outputs just a single record for everything, so much more efficient.
     */
    rc_t init_fix_steal(
        btree_p*             parent,
        const lpid_t&        pid,
        shpid_t              root, 
        int                  level,
        shpid_t              pid0,
        shpid_t              foster,
        const w_keystr_t&    fence_low,
        const w_keystr_t&    fence_high,
        const w_keystr_t&    chain_fence_high,
        btree_p*             steal_src = NULL,
        int                  steal_from = 0,
        int                  steal_to = 0,
        bool                 log_it = true
                                );

    /**
     * This sets all headers, fence/prefix keys and initial records altogether. Used by init_fix_steal.
     * Steal records from steal_src1. Then steal records from steal_src2 (this is used only when merging).
     * if steal_src2_pid0 is true, it also steals src2's pid0 with low-fence key.
     */
    rc_t format_steal(
        const lpid_t&        pid,
        shpid_t              root, 
        int                  level,
        shpid_t              pid0,
        shpid_t              foster,
        const w_keystr_t&    fence_low,
        const w_keystr_t&    fence_high,
        const w_keystr_t&    chain_fence_high,
        bool                 log_it = true,
        btree_p*             steal_src1 = NULL,
        int                  steal_from1 = 0,
        int                  steal_to1 = 0,
        btree_p*             steal_src2 = NULL,
        int                  steal_from2 = 0,
        int                  steal_to2 = 0,
        bool                 steal_src2_pid0 = false
        );

    /** Steal records from steal_src. Called by format_steal. */
    void _steal_records(
        btree_p*             steal_src,
        int                  steal_from,
        int                  steal_to);

    /**
     * Called when we did a split from this page but didn't move any record to new page.
     * This method can't be undone. Use this only for REDO-only system transactions.
     */
    rc_t norecord_split (shpid_t foster,
        const w_keystr_t& fence_high, const w_keystr_t& chain_fence_high,
        bool log_it = true);

    /** Returns if whether we can do norecord insert now. */
    bool                 check_chance_for_norecord_split(const w_keystr_t& key_to_insert) const;
    
#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Search and Record Access functions
///==========================================
#endif // DOXYGEN_HIDE

    char*                        data_addr8(slot_offset8_t offset8);
    const char*                  data_addr8(slot_offset8_t offset8) const;

    slot_offset8_t               tuple_offset8(slotid_t idx) const;
    poor_man_key                 tuple_poormkey (slotid_t idx) const;
    void                         tuple_both (slotid_t idx, slot_offset8_t &offset8, poor_man_key &poormkey) const;
    void*                        tuple_addr(slotid_t idx) const;

    char*                        slot_addr(slotid_t idx) const;
    /**
     * Changes only the offset part of the specified slot.
     * Used to turn a ghost record into a usual record, or to expand a record.
     */
    void                         change_slot_offset (slotid_t idx, slot_offset8_t offset8);

    
    /**
    *  Search for key in this page. Return true in "found_key" if
    *  the key is found. 
    * 
    * If the page is a leaf page:
    * If found_key, always returns the slot number of the found key.
    * If !found_key, return the slot where key should go.
    * 
    * If the page is an interior page:
    *  Basically same as leaf, but it can return -1 as slot number.
    *  Only when the page is an interior left-most page and the search
    *  key is same or smaller than left-most key in this page, this function returns
    *  ret_slot=-1, which means we should follow the pid0 pointer.
    */
    void            search(
                        const w_keystr_t&             key,
                        bool&                     found_key,
                        slotid_t&             ret_slot
                        ) const;

    /**
    * Used from search() for leaf pages.
    * Simply finds the slot matching with the search key.
    */
    inline void         search_leaf(
                        const w_keystr_t&             key,
                        bool&                     found_key,
                        slotid_t&             ret_slot
                        ) const {
        search_leaf((const char*) key.buffer_as_keystr(), key.get_length_as_keystr(), found_key, ret_slot);
    }
    // to make it slightly faster. not a neat kind of optimization
    void            search_leaf(
                        const char *key_raw, size_t key_raw_len,
                        bool&                     found_key,
                        slotid_t&             ret_slot
                        ) const;
    /**
    * Used from search() for interior pages.
    * A bit more complicated because keys are separator keys.
    * Like fence keys, a separator key is exclusive for left and
    * inclusive for right. For example, a separator key "AB"
    * sends "AA" to left, "AAZ" to left, "AB" to right,
    * "ABA" to right, "AC" to right.
    */
    void            search_node(
                        const w_keystr_t&             key,
                        slotid_t&             ret_slot
                        ) const;

    /**
     * Returns the number of records in this page.
     * Use this instead of generic_page_h::nslots to acount for one hidden slots.
     */
    int              nrecs() const;

    /** Retrieves the key and record of specified slot in a leaf page.*/
    void            rec_leaf(slotid_t idx,  w_keystr_t &key, cvec_t &el, bool &ghost) const;
    /**
     * Overload to receive raw buffer for el.
     * @param[in,out] elen both input (size of el buffer) and output(length of el set).
     */
    void            rec_leaf(slotid_t idx,  w_keystr_t &key, char *el, smsize_t &elen, bool &ghost) const;
    /**
     * Returns only the el part.
     * @param[in,out] elen both input (size of el buffer) and output(length of el set).
     * @return true if el is large enough. false if el was too short and just returns elen.
     */
    bool            dat_leaf(slotid_t idx, char *el, smsize_t &elen, bool &ghost) const;

    /** This version returns the pointer to element without copying. */
    void            dat_leaf_ref(slotid_t idx, const char *&el, smsize_t &elen, bool &ghost) const;

    /** Retrieves only key from a leaf page.*/
    void            leaf_key(slotid_t idx,  w_keystr_t &key) const;

    /**
     * Retrieves the key and corresponding page pointer of specified slot
     * in an intermediate node page.
     */
    void            rec_node(slotid_t idx,  w_keystr_t &key, shpid_t &el) const;

    /** Retrieves only key from a node page.*/
    void            node_key(slotid_t idx,  w_keystr_t &key) const;

    /** Retrieves only key from this page.*/
    void            get_key(slotid_t idx,  w_keystr_t &key) const {
        if (is_node()) {
            node_key(idx, key);
        } else {
            leaf_key(idx, key);
        }
    }

    /** Retrieves only the length of key (before prefix compression).*/
    slot_length_t       get_key_len(slotid_t idx) const;

    /** Retrieves only the physical record length.*/
    slot_length_t       get_rec_size(slotid_t idx) const;
    /** Retrieves only the physical record length (use this if you already know it's a leaf page).*/
    slot_length_t       get_rec_size_leaf(slotid_t idx) const;
    /** Retrieves only the physical record length (use this if you already know it's a node page).*/
    slot_length_t       get_rec_size_node(slotid_t idx) const;

    /** for the special record which stores fence keys. */
    slot_length_t       get_fence_rec_size() const;

    /**
    *  Return the child pointer of tuple at "slot".
    *  equivalent to rec_node(), but doesn't return key (thus faster).
    */
    shpid_t       child(slotid_t slot) const;
    /**
    *  Return the child opaque pointer of tuple at "slot".
    */
    shpid_t       child_opaqueptr(slotid_t slot) const;

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Insert/Update/Delete functions
///==========================================
#endif // DOXYGEN_HIDE

    /**
    *  Insert a new entry at "slot". This is used only for non-leaf pages.
    * For leaf pages, always use replace_ghost() and reserve_ghost().
    * @param child child pointer to add
    */
    rc_t            insert_node(
                        const w_keystr_t&             key,
                        slotid_t            slot, 
                        shpid_t             child);
    /**
     * Mark the given slot to be a ghost record.
     * If the record is already a ghost, does nothing.
     * This is used by delete and insert (UNDO).
     * This function itself does NOT log, so the caller is responsible for it.
     * @see defrag()
     */
    void                        mark_ghost(slotid_t slot);

    /** Returns if the specified record is a ghost record. */
    bool                        is_ghost(slotid_t slot) const;
    
    /**
     * Un-Mark the given slot to be a regular record.
     * If the record is already a non-ghost, does nothing.
     * This is only used by delete (UNDO).
     * This function itself does NOT log, so the caller is responsible for it.
     */
    void                        unmark_ghost(slotid_t slot);

    /**
    * Replace an existing ghost record to insert the given tuple.
    * This assumes the ghost record is enough spacious.
    * @param[in] key inserted key
    * @param[in] elem record data
    */
    rc_t            replace_ghost(
        const w_keystr_t &key, const cvec_t &elem);

    /**
     * Replaces the special fence record with the given new data,
     * expanding the slot length if needed.
     */
    rc_t            replace_expand_fence_rec_nolog(const cvec_t &fences);

    /**
    *  Remove the slot and up-shift slots after the hole to fill it up.
    * @param slot the slot to remove.
    */
    rc_t            remove_shift_nolog(slotid_t slot);

    /**
     * Replaces the given slot with the new data. key is not changed.
     * If we need to expand the record and the page doesn't have
     * enough space, eRECWONTFIT is thrown.
     */
    rc_t            replace_el_nolog(slotid_t slot, const cvec_t &elem);
    
    /**
     * Similar to replace_el_nolog(), but this overwrites specific
     * part of the element and never changes the size of record,
     * so it's simpler, faster and can't throw error.
     * 
     */
    void            overwrite_el_nolog(slotid_t slot, smsize_t offset,
                                       const char *new_el, smsize_t elen);

    /**
     * Creates a dummy ghost record with the given key and length
     * as a preparation for subsequent insertion.
     * This is used by insertion (its nested system transaction).
     * This function itself does NOT log, so the caller is responsible for it.
     */
    void             reserve_ghost(const w_keystr_t &key, int record_size) {
        reserve_ghost((const char *)key.buffer_as_keystr(), key.get_length_as_keystr(), record_size);
    }
    // to make it slightly faster. not a neat kind of optimization
    void             reserve_ghost(const char *key_raw, size_t key_raw_len, int record_size);

    /**
     * Tell if the slot is a ghost record and enough spacious to store the
     * given key/data.
     */
    bool            _is_enough_spacious_ghost(
        const w_keystr_t &key, slotid_t slot,
        const cvec_t&        el);

    /**
     * Returns if there is enough free space to accomodate the
     * given new record.
     * @return true if there is free space
     */
    bool           check_space_for_insert_leaf(const w_keystr_t &key, const cvec_t &el);
    /** for intermediate node (no element). */
    bool           check_space_for_insert_node(const w_keystr_t &key);

    /**
     * \brief Suggests a new fence key, assuming this page is being split.
     *  \details
     * The new "mid" fence key will be the new left sibling's high fence key
     * and also the right sibling's low fence key after split.
     * right_begins_from returns the slot id from which (including it)
     * the new right sibling steal the entires.
     * @param[out] mid suggested fence key in the middle
     * @param[out] right_begins_from slot id from which (including it)
     * the new right sibling steal the entires
     * @param[in] triggering_key the key to be inserted after this split. used to determine split policy.
     */
    void                 suggest_fence_for_split(
                             w_keystr_t &mid, slotid_t& right_begins_from, const w_keystr_t &triggering_key) const;
    /** For recovering the separator key from boundary place. @see suggest_fence_for_split(). */
    w_keystr_t           recalculate_fence_for_split(slotid_t right_begins_from) const;

    bool                 is_insertion_extremely_skewed_right() const;
    bool                 is_insertion_skewed_right() const;
    bool                 is_insertion_skewed_left()  const;

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Statistics/Debug etc functions
///==========================================
#endif // DOXYGEN_HIDE

    /**
     * \brief Defrags this page to remove holes and ghost records in the page.
     * \details
     * A page can have unused holes between records and ghost records as a result
     * of inserts and deletes. This method removes those dead spaces to compress
     * the page. The best thing of this is that we have to log only
     * the slot numbers of ghost records that are removed because there are
     * 'logically' no changes.
     * Context: System transaction.
     * @param[in] popped the record to be popped out to the head. -1 to not specify.
     */
    rc_t                         defrag(slotid_t popped = -1);

    /** stats for leaf nodes. */
    rc_t             leaf_stats(btree_lf_stats_t& btree_lf);
    /** stats for interior nodes. */
    rc_t             int_stats(btree_int_stats_t& btree_int);

    /** this is used by du/df to get page statistics DU DF. */
    void                        page_usage(
        int&                            data_sz,
        int&                            hdr_sz,
        int&                            unused,
        int&                             alignmt,
        tag_t&                             t,
        slotid_t&                     no_used_slots);

    /** Debugs out the contents of this page. */
    void             print(bool print_elem=false);

    /** Return how many bytes will we spend to store the record (without slot). */
    size_t           calculate_rec_size (const w_keystr_t &key, const cvec_t &el) const;

    /**
     * Checks integrity of this page.
     * This function does NOT check integrity across multiple pages.
     * For that purpose, use btree_m::verify_tree().
     * If the two arguments are both false (which is default), this function should be very
     * efficient.
     * @param[in] check_keyorder whether to check the sortedness and uniqueness
     * of the keys in this page. setting this to true makes this function expensive.
     * @param[in] check_space whether to check any overlaps of
     * records and integrity of space offset. setting this to true makes this function expensive.
     * @return true if this page is in a consistent state
     */
    bool             is_consistent (bool check_keyorder = false, bool check_space = false) const;


    static smsize_t         max_entry_size;
    static smsize_t         overhead_requirement_per_entry;

private:
    /** checks space correctness. */
    bool             _is_consistent_space () const;

    /** internal method used from is_consistent() to check keyorder correctness. */
    bool             _is_consistent_keyorder () const;

    /** checks if the poor-man's normalized keys are valid. */
    bool             _is_consistent_poormankey () const;

    /** Given the place to insert, update btree_consecutive_skewed_insertions. */
    void             _update_btree_consecutive_skewed_insertions(slotid_t slot);

    /** Retrieves the key of specified slot WITHOUT prefix in a leaf page.*/
    const char*     _leaf_key_noprefix(slotid_t idx,  size_t &len) const;
    /** Retrieves only the key of specified slot WITHOUT prefix in an intermediate node page.*/
    const char*     _node_key_noprefix(slotid_t idx,  size_t &len) const;

    /** returns compare(specified-key, key_noprefix) for lead page. */
    int             _compare_leaf_key_noprefix(slotid_t idx, const void *key_noprefix, size_t key_len) const;
    /** returns compare(specified-key, key_noprefix) for node page. */
    int             _compare_node_key_noprefix(slotid_t idx, const void *key_noprefix, size_t key_len) const;

    /** skips comparing the first sizeof(poormankey) bytes. */
    int             _compare_leaf_key_noprefix_remain(slot_offset8_t slot_offset8, const void *key_noprefix_remain, int key_len_remain) const;
    /** skips comparing the first sizeof(poormankey) bytes. */
    int             _compare_node_key_noprefix_remain(slot_offset8_t slot_offset8, const void *key_noprefix_remain, int key_len_remain) const;

    /**
    *  Insert a record at slot idx. Slots on the left of idx
    *  are pushed further to the left to make space. 
    *  By this it's meant that the slot table entries are moved; the
    *  data themselves are NOT moved.
    *  Vec[] contains the data for these new slots. 
    */
    rc_t             _insert_expand_nolog(slotid_t slot, const cvec_t &tp, poor_man_key poormkey);


    /**
     * This is used when it's known that we are adding the new record
     * to the end, and the page is like a brand-new page; no holes,
     * and enough spacious. Basically only for page-format case.
     * This is much more efficient!
     * This doesn't log, so the caller is responsible for it.
     */
    void             _append_nolog(const cvec_t &tp, poor_man_key poormkey, bool ghost);


    /**
     * Expands an existing record for given size.
     * Caller should make sure there is enough space to expand.
     */
    void             _expand_rec(slotid_t slot, slot_length_t rec_len);
};

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Inline function implementations
///==========================================
#endif // DOXYGEN_HIDE

inline lpid_t btree_p::root() const
{
    lpid_t p = pid();
    p.page = _pp->btree_root;
    return p;
}

inline int btree_p::level() const
{
    return _pp->btree_level;
}

inline shpid_t btree_p::pid0_opaqueptr() const
{
    return _pp->btree_pid0;
}

inline shpid_t btree_p::pid0() const
{
    shpid_t shpid = _pp->btree_pid0;
    if (shpid) {
        return smlevel_0::bf->normalize_shpid(shpid);
    }
    return shpid;
}

inline bool btree_p::is_leaf() const
{
    return level() == 1;
}

inline bool btree_p::is_leaf_parent() const
{
    return level() == 2;
}

inline bool btree_p::is_node() const
{
    return ! is_leaf();
}
   
inline shpid_t btree_p::get_foster_opaqueptr() const
{
    return _pp->btree_foster;
}

inline shpid_t btree_p::get_foster() const
{
    shpid_t shpid = _pp->btree_foster;
    if (shpid) {
        return smlevel_0::bf->normalize_shpid(shpid);
    }
    return shpid;
}

inline int16_t btree_p::get_prefix_length() const
{
    return _pp->btree_prefix_length;
}
inline int16_t btree_p::get_fence_low_length() const
{
    return _pp->btree_fence_low_length;
}
inline int16_t btree_p::get_fence_high_length() const
{
    return _pp->btree_fence_high_length;
}
inline int16_t btree_p::get_chain_fence_high_length() const
{
    return _pp->btree_chain_fence_high_length;
}

inline const char* btree_p::get_fence_low_key() const
{
    const char*s = (const char*) btree_p::tuple_addr(0);
    return s + sizeof(slot_length_t);
}
inline const char* btree_p::get_fence_high_key_noprefix() const
{
    int16_t fence_low_length = get_fence_low_length();
    const char*s = (const char*) btree_p::tuple_addr(0);
    return s + sizeof(slot_length_t) + fence_low_length;
}
inline const char* btree_p::get_chain_fence_high_key() const
{
    int16_t fence_low_length = get_fence_low_length();
    int16_t fence_high_length_noprefix = get_fence_high_length_noprefix();
    const char*s = (const char*) btree_p::tuple_addr(0);
    return s + sizeof(slot_length_t) + fence_low_length + fence_high_length_noprefix;
}
inline const char* btree_p::get_prefix_key() const
{
    return get_fence_low_key(); // same thing. only the length differs.
}

inline int btree_p::nrecs() const
{
    return nslots() - 1;
}
inline int btree_p::compare_with_fence_low (const w_keystr_t &key) const
{
    return key.compare_keystr(get_fence_low_key(), get_fence_low_length());
}
inline int btree_p::compare_with_fence_low (const char* key, size_t key_len) const
{
    return w_keystr_t::compare_bin_str (key, key_len, get_fence_low_key(), get_fence_low_length());
}
inline int btree_p::compare_with_fence_low_noprefix (const char* key, size_t key_len) const
{
    return w_keystr_t::compare_bin_str (key, key_len, get_fence_low_key() + get_prefix_length(), get_fence_low_length() - get_prefix_length());
}

inline int btree_p::compare_with_fence_high (const w_keystr_t &key) const
{
    return compare_with_fence_high ((const char*) key.buffer_as_keystr(), key.get_length_as_keystr());
}
inline int btree_p::compare_with_fence_high (const char* key, size_t key_len) const
{
    size_t prefix_len = get_prefix_length();
    if (prefix_len > key_len) {
        return w_keystr_t::compare_bin_str (key, key_len, get_prefix_key(), key_len);
    } else {
        // first, compare with prefix part
        int ret = w_keystr_t::compare_bin_str (key, prefix_len, get_prefix_key(), prefix_len);
        if (ret != 0) {
            return ret;
        }
        // then, compare with suffix part
        return w_keystr_t::compare_bin_str (key + prefix_len, key_len - prefix_len,
            get_fence_high_key_noprefix(), get_fence_high_length_noprefix());
    }
}
inline int btree_p::compare_with_fence_high_noprefix (const char* key, size_t key_len) const
{
    return w_keystr_t::compare_bin_str (key, key_len, get_fence_high_key_noprefix(), get_fence_high_length_noprefix());
}

inline int btree_p::compare_with_chain_fence_high (const w_keystr_t &key) const
{
    return key.compare_keystr(get_chain_fence_high_key(), get_chain_fence_high_length());
}
inline int btree_p::compare_with_chain_fence_high (const char* key, size_t key_len) const
{
    return w_keystr_t::compare_bin_str (key, key_len, get_chain_fence_high_key(), get_chain_fence_high_length());
}
inline bool btree_p::fence_contains(const w_keystr_t &key) const
{
    // fence-low is inclusive
    if (compare_with_fence_low(key) < 0) {
        return false;
    }
    // fence-high is exclusive  (but if it's supremum, we allow it to handle to-the-end scan)
    if (!is_fence_high_supremum() && compare_with_fence_high(key) >= 0) {
        return false;
    }
    return true;
}

inline size_t btree_p::calculate_rec_size (const w_keystr_t &key, const cvec_t &el) const
{
    if (is_leaf()) {
        return key.get_length_as_keystr() - get_prefix_length()
            + el.size() + sizeof(slot_length_t) * 2;
    } else {
        return key.get_length_as_keystr() - get_prefix_length()
            + sizeof(shpid_t) + sizeof(slot_length_t);
    }
}

inline bool btree_p::is_insertion_extremely_skewed_right() const
{
    // this means completely pre-sorted insertion like bulk loading.
    int ins = _pp->btree_consecutive_skewed_insertions;
    return ins > 50
        || ins > nrecs() * 9 / 10
        || (ins > 1 && ins >= nrecs() - 1)
        ;
}    
inline bool btree_p::is_insertion_skewed_right() const
{
    return _pp->btree_consecutive_skewed_insertions > 5;
}
inline bool btree_p::is_insertion_skewed_left() const
{
    return _pp->btree_consecutive_skewed_insertions < -5;
}
inline shpid_t btree_p::child_opaqueptr(slotid_t slot) const
{
    // same as rec_node except we don't need to read key
    w_assert1(is_node());
    w_assert1(slot >= 0);
    w_assert1(slot < nrecs());
    const void* p = btree_p::tuple_addr(slot + 1);
    return *reinterpret_cast<const shpid_t*>(p);
}

inline shpid_t btree_p::child(slotid_t slot) const
{
    shpid_t shpid = child_opaqueptr(slot);
    if (shpid) {
        return smlevel_0::bf->normalize_shpid(shpid);
    }
    return shpid;
}

inline slot_length_t btree_p::get_key_len(slotid_t idx) const {
    if (is_leaf()) {
        return ((const slot_length_t*) btree_p::tuple_addr(idx + 1))[1];
    } else {
        // node page doesn't keep key_len. It's calculated from rec_len
        const char *p = ((const char*) btree_p::tuple_addr(idx + 1)) + sizeof(shpid_t);
        slot_length_t rec_len = *((const slot_length_t*) p);
        return rec_len - sizeof(shpid_t) - sizeof(slot_length_t) + get_prefix_length();
    }
}
inline slot_length_t btree_p::get_rec_size_leaf(slotid_t slot) const {
    w_assert1(is_leaf());
    w_assert1(slot >= 0);
    w_assert1(slot < nrecs());
    const char *base = (const char *) btree_p::tuple_addr(slot + 1);
    return *((const slot_length_t*) base);
}
inline slot_length_t btree_p::get_rec_size_node(slotid_t slot) const {
    w_assert1(is_node());
    w_assert1(slot >= 0);
    w_assert1(slot < nrecs());
    const char *base = (const char *) btree_p::tuple_addr(slot + 1);
    const char *p = base + sizeof(shpid_t);
    return *((const slot_length_t*) p);
}
inline slot_length_t btree_p::get_rec_size(slotid_t idx) const {
    if (is_leaf()) {
        return get_rec_size_leaf(idx);
    } else {
        return get_rec_size_node(idx);
    }
}
inline slot_length_t btree_p::get_fence_rec_size() const {
    const char *base = (const char *) btree_p::tuple_addr(0);
    return *((const slot_length_t*) base);
}

inline const char* btree_p::_leaf_key_noprefix(slotid_t idx,  size_t &len) const {
    w_assert1(is_leaf());
    const char* base = (char*) btree_p::tuple_addr(idx + 1);
    slot_length_t key_len = ((slot_length_t*) base)[1];
    len = key_len - get_prefix_length();
    return base + sizeof(slot_length_t) * 2;    
}
inline const char* btree_p::_node_key_noprefix(slotid_t idx,  size_t &len) const {
    w_assert1(is_node());
    const char *p = ((const char*) btree_p::tuple_addr(idx + 1)) + sizeof(shpid_t);
    slot_length_t rec_len = *((const slot_length_t*) p);
    w_assert1(rec_len >= sizeof(shpid_t) + sizeof(slot_length_t));
    len = rec_len - sizeof(shpid_t) - sizeof(slot_length_t);
    return p + sizeof(slot_length_t);
}
inline int btree_p::_compare_leaf_key_noprefix(slotid_t idx, const void *key_noprefix, size_t key_len) const {
    size_t curkey_len;
    const char *curkey = _leaf_key_noprefix(idx, curkey_len);
    return w_keystr_t::compare_bin_str(curkey, curkey_len, key_noprefix, key_len);
}
inline int btree_p::_compare_node_key_noprefix(slotid_t idx, const void *key_noprefix, size_t key_len) const {
    size_t curkey_len;
    const char *curkey = _node_key_noprefix(idx, curkey_len);
    return w_keystr_t::compare_bin_str(curkey, curkey_len, key_noprefix, key_len);
}
inline int btree_p::_compare_leaf_key_noprefix_remain(slot_offset8_t slot_offset8, const void *key_noprefix_remain, int key_len_remain) const {
    w_assert1(is_leaf());
    const char* base = btree_p::data_addr8(slot_offset8 < 0 ? -slot_offset8 : slot_offset8);
    int curkey_len_remain = ((slot_length_t*) base)[1] - get_prefix_length() - sizeof(poor_man_key);
    // because this function was called, the poor man's key part was equal.
    // now we just compare the remaining part
    if (key_len_remain > 0 && curkey_len_remain > 0) {
        const char *curkey_remain = base + sizeof(slot_length_t) * 2 + sizeof(poor_man_key);
        return w_keystr_t::compare_bin_str(curkey_remain, curkey_len_remain, key_noprefix_remain, key_len_remain);
    } else {
        // the key was so short that poorman's key was everything.
        // then, compare the key length.
        return curkey_len_remain - key_len_remain;
    }
}

inline int btree_p::_compare_node_key_noprefix_remain(slot_offset8_t slot_offset8, const void *key_noprefix_remain, int key_len_remain) const {
    w_assert1(is_node());
    const char* base = btree_p::data_addr8(slot_offset8 < 0 ? -slot_offset8 : slot_offset8);
    const char *p = ((const char*) base) + sizeof(shpid_t);
    slot_length_t rec_len = *((const slot_length_t*) p);
    w_assert1(rec_len >= sizeof(shpid_t) + sizeof(slot_length_t));
    int curkey_len_remain = rec_len - sizeof(shpid_t) - sizeof(slot_length_t) - sizeof(poor_man_key);
    const char *curkey_remain = p + sizeof(slot_length_t) + sizeof(poor_man_key);
    if (key_len_remain > 0 && curkey_len_remain > 0) {
        return w_keystr_t::compare_bin_str(curkey_remain, curkey_len_remain, key_noprefix_remain, key_len_remain);
    } else {
        return curkey_len_remain - key_len_remain;
    }
}
inline bool btree_p::is_ghost(slotid_t slot) const
{
    slot_offset8_t offset8 = btree_p::tuple_offset8(slot + 1);
    return (offset8 < 0);
}


inline char* btree_p::data_addr8(slot_offset8_t offset8)
{
    return page()->data_addr8(offset8);
}
inline const char* btree_p::data_addr8(slot_offset8_t offset8) const
{
    return page()->data_addr8(offset8);
}
inline char* btree_p::slot_addr(slotid_t idx) const
{
    w_assert3(idx >= 0 && idx <= _pp->nslots);
    return page()->data + (slot_sz * idx);
}

inline slot_offset8_t
btree_p::tuple_offset8(slotid_t idx) const
{
    return *reinterpret_cast<const slot_offset8_t*>(slot_addr(idx));
}
inline poor_man_key btree_p::tuple_poormkey (slotid_t idx) const
{
    return *reinterpret_cast<const poor_man_key*>(slot_addr(idx) + sizeof(slot_offset8_t));
}
inline void btree_p::tuple_both (slotid_t idx, slot_offset8_t &offset8, poor_man_key &poormkey) const
{
    const char* slot = slot_addr(idx);
    offset8 = *reinterpret_cast<const slot_offset8_t*>(slot);
    poormkey = *reinterpret_cast<const poor_man_key*>(slot + sizeof(slot_offset8_t));
}

inline void*
btree_p::tuple_addr(slotid_t idx) const
{
    slot_offset8_t offset8 = tuple_offset8(idx);
    if (offset8 < 0) offset8 = -offset8; // ghost record.
    return page()->data_addr8(offset8);
}

inline void btree_p::change_slot_offset (slotid_t idx, slot_offset8_t offset) {
    char* slot = slot_addr(idx);
    *reinterpret_cast<slot_offset8_t*>(slot) = offset;
}

#endif // BTREE_P_H
