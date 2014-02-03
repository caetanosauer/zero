/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BTREE_PAGE_H_H
#define BTREE_PAGE_H_H

#include "btree_page.h"

#include "w_defines.h"
#include "fixable_page_h.h"
#include "w_key.h"
#include "w_endian.h"

#include "bf_tree_inline.h" // for normalize_shpid <<<>>>

struct btree_lf_stats_t;
struct btree_int_stats_t;



     /*
     * When this page belongs to a foster chain, we need to store
     * high-fence of right-most sibling in every sibling to do
     * batch-verification with bitmaps.  @see
     * btree_impl::_ux_verify_volume()
     */






class btree_page_h;

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
    NORET            btrec_t(const btree_page_h& page, slotid_t slot);
    NORET            ~btrec_t()        {};

    /// Load up a reference to the tuple at slot in "page".
    btrec_t&           set(const btree_page_h& page, slotid_t slot);
    
    smsize_t           elen() const    { return _elem.size(); }

    const w_keystr_t&  key() const    { return _key; }
    const cvec_t&      elem() const     { return _elem; }
    /// returns the opaque version
    shpid_t            child() const    { return _child; }
    bool               is_ghost_record() const { return _ghost_record; }

private:
    friend class btree_page_h;

    bool            _ghost_record;
    w_keystr_t      _key;
    shpid_t         _child;  // opaque pointer
    cvec_t          _elem;

    // disabled
    NORET            btrec_t(const btrec_t&);
    btrec_t&         operator=(const btrec_t&);
};

inline NORET
btrec_t::btrec_t(const btree_page_h& page, slotid_t slot) {
    set(page, slot);
}


class btree_impl;
class btree_ghost_t;
class btree_ghost_mark_log;
class btree_ghost_reclaim_log;



/**
 * \brief Page handle for B-Tree data page.
 * \ingroup SSMBTREE
 * 
 * \details
 * 
 * \section FBTREE Fence Keys and B-link Tree
 * 
 * Our version of B-Tree header contains low-high fence keys and
 * pointers as a b-link tree.  Fence keys are NEVER changed once the
 * page is created until the page gets splitted.  Prefix compression
 * in this scheme utilizes it by storing the common leading bytes of
 * the two fence keys and simply getting rid of them from all entries
 * in this page.  See btree_page for the header definitions.
 *
 * 
 * \section PAGELAYOUT B-Tree Page Layout
 * 
 * The page layout of a B-Tree page is as follows:
 * 
 * \verbatim
  [common-headers from generic_page_header]
  [B-tree-specific-headers in btree_page]
  (item area in btree_page:
    [item storage part that is growing forwards]
    [(contiguous) free area]
    [item storage part that is growing backwards]
   )\endverbatim
 * 
 * The first item contains the fence keys and foster key, if any:
 * [low-fence-key data (including prefix) + high-fence-key data
 * (without prefix) + chain-fence-high-key data (complete string;
 * chain-fence-high doesn't share prefix!)].  The other items contain
 * the B-tree page's records, one each.  These locations are called \e
 * slots; slot 0 corresponds to item 1 and so on.
 * 
 * 
 * \section SLOTLAYOUT B-Tree Slot Layout
 * 
 * We use poor-man's normalized key to speed up searches.  Each slot
 * has a field, _poor, that holds the first few bytes of that slot's
 * key as an unsigned integer (poor_man_key) so that comparison with
 * most slot keys can be done without going to the key itself,
 * avoiding L1 cache misses.  The whole point of poor_man_key is
 * avoiding cache misses!
 * 
 * NOTE So far, poor-man's normalied key is 2 byte integer (uint16_t)
 * and the corresponding bytes are NOT eliminated from the key string
 * in the record.  This is to speed up the retrieval of the
 * (truncated) complete key at the cost of an additional 2 bytes to
 * store it.  I admit this is arguable, but deserilizing the first
 * part everytime (it's likely little-endian, so we need to flip it)
 * will slow down retrieval.
 * 
 * Also associated with each slot is a ghost bit (is this record a
 * ghost record?), a variable-size data portion used to hold the
 * record details (i.e., key and element if a leaf node), and in the
 * case of interior nodes, a child pointer.
 * 
 * 
 * \section RECORDLAYOUT Record Layout
 * 
 * Internally, record data is stored in the item variable-size data part as follows:
 * 
 * (If the page is leaf page)
 * - key length (uint16_t) AFTER prefix truncation
 * - key + element (contiguous char[]) AFTER prefix truncation
 * NOTE: element length can be calculated from item length and key length
 * (element length = item length - key length - sizeof(key length)).
 * 
 * (If the page is interior node page)
 * - key AFTER prefix truncation
 * NOTE: key-AFTER-prefix-truncation length here is simply the item length.
 * 
 * Opaque pointers:
 * - Default is to return page ID.
 * - When a page is swizzled though we can avoid hash table lookup to map page ID 
 *   to frame ID by using frame ID directly.  Opaque pointers serve this purpose by 
 *   hiding from the user whether a pointer is a frame ID or page ID. 
 */
class btree_page_h : public fixable_page_h {
    friend class btree_impl;
    friend class btree_ghost_t;
    friend class btree_ghost_mark_log;
    friend class btree_ghost_reclaim_log;
    friend class btree_header_t;
    friend class page_img_format_t;

    btree_page* page() const { return reinterpret_cast<btree_page*>(_pp); }


    enum {
        data_sz = btree_page::data_sz,
        hdr_sz  = btree_page::hdr_sz,
    };

public:
    // ======================================================================
    //   BEGIN: Struct/Enum/Constructor
    // ======================================================================

    btree_page_h() {}
    btree_page_h(generic_page* s) : fixable_page_h(s) {
        w_assert1(s->tag == t_btree_p);
    }
    btree_page_h(const btree_page_h& p) : fixable_page_h(p) {} 
    ~btree_page_h() {}
    btree_page_h& operator=(btree_page_h& p) { 
        fixable_page_h::operator=(p); 
        w_assert1(_pp->tag == t_btree_p);
        return *this; 
    }


    // ======================================================================
    //   BEGIN: Header Get/Set functions
    // ======================================================================

    shpid_t                     btree_root() const { return page()->btree_root;}
    smsize_t                    used_space()  const;

    // Total usable space on page
    smsize_t                     usable_space()  const;
    


    
    /// Returns 1 if leaf, >1 if non-leaf.
    int               level() const;
    /// Returns left-most ptr (used only in non-leaf nodes).
    shpid_t        pid0() const;
    /// Returns left-most opaque pointer (used only in non-leaf nodes).
    shpid_t        pid0_opaqueptr() const;
    /// Returns root page; used for recovery
    lpid_t           root() const;

    /// Is associated page a leaf?
    bool         is_leaf() const;
    /// Returns if this page is NOT a leaf
    bool             is_node() const;
    /**
    *    return true if this node is the lowest interior node,     *
    *    i.e., the parent of a leaf.  Used to tell how we should   *
    *    latch a child page : EX or SH                             *
    */
    bool             is_leaf_parent() const;
    
    /// Returns ID of B-link page (0 if not linked).
    shpid_t         get_foster() const;
    /// Returns opaque pointer of B-link page (0 if not linked).
    shpid_t         get_foster_opaqueptr() const;
    /// Clears the foster page and also clears the chain high fence key.
    rc_t               clear_foster();

    /// Returns the prefix which is removed from all entries in this page.
    const char* get_prefix_key() const;
    /// Returns the length of prefix key (0 means no prefix compression).
    int16_t           get_prefix_length() const;
    /// Returns the low fence key, which is same OR smaller than all entries in this page and its descendants.
    const char*  get_fence_low_key() const;
    /// Returns the length of low fence key.
    int16_t           get_fence_low_length() const;
    /// Constructs w_keystr_t object containing the low-fence key of this page.
    void                copy_fence_low_key(w_keystr_t &buffer) const {buffer.construct_from_keystr(get_fence_low_key(), get_fence_low_length());}
    /// Returns if the low-fence key is infimum.
    bool              is_fence_low_infimum() const { return get_fence_low_key()[0] == SIGN_NEGINF;}

    /**
     * Returns the high fence key (without prefix), which is larger
     * than all entries in this page and its descendants.  NOTE we
     * don't provide get_fence_high_key() with prefix because the page
     * eliminates prefix from fence-high.
     */
    const char*       get_fence_high_key_noprefix() const;
    /// Returns the length of high fence key with prefix.
    int16_t           get_fence_high_length() const;
    /// Returns the length of high fence key without prefix.
    int16_t           get_fence_high_length_noprefix() const {
        return get_fence_high_length() - get_prefix_length();
    }
    /// Constructs w_keystr_t object containing the low-fence key of this page.
    void                copy_fence_high_key(w_keystr_t &buffer) const {
        buffer.construct_from_keystr(get_prefix_key(), get_prefix_length(),
                                     get_fence_high_key_noprefix(),
                                     get_fence_high_length_noprefix());
    }
    /// Returns if the high-fence key is supremum.
    bool              is_fence_high_supremum() const { return get_prefix_length() == 0 && get_fence_high_key_noprefix()[0] == SIGN_POSINF;}

    /// Returns the high fence key of foster chain.
    const char*  get_chain_fence_high_key() const;
    /// Returns the length of high fence key of foster chain.
    int16_t           get_chain_fence_high_length() const;
    /// Constructs w_keystr_t object containing the low-fence key of this page.
    void                copy_chain_fence_high_key(w_keystr_t &buffer) const {
        buffer.construct_from_keystr(get_chain_fence_high_key(), get_chain_fence_high_length());
    }
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
    /// overload for char*.
    int                 compare_with_fence_low (const char* key, size_t key_len) const;
    /// used when the prefix part is already checked key/key_len must be WITHOUT prefix.
    int                 compare_with_fence_low_noprefix (const char* key, size_t key_len) const;
    /**
     * Return value : 0 if equal.
     * : <0 if key < fence-high.
     * : >0 if key > fence-high.
     */
    int                 compare_with_fence_high (const w_keystr_t &key) const;
    /// overload for char*.
    int                 compare_with_fence_high (const char* key, size_t key_len) const;
    /// used when the prefix part is already checked key/key_len must be WITHOUT prefix.
    int                 compare_with_fence_high_noprefix (const char* key, size_t key_len) const;
    /**
     * Return value : 0 if equal.
     * : <0 if key < fence-high.
     * : >0 if key > fence-high.
     */
    int                 compare_with_chain_fence_high (const w_keystr_t &key) const;
    /// overload for char*.
    int                 compare_with_chain_fence_high (const char* key, size_t key_len) const;
    // no 'noprefix' version because chain_fence_high might not share the prefix!

    /**
     * When allocating a new B-Tree page, use this instead of fix().
     * This sets all headers, fence/prefix keys, and initial records altogether.
     * As our new B-Tree header has variable-size part (fence keys),
     * setting fence keys later than the first format() causes a problem.
     * So, the 4-argumets format(which is called from default fix() on virgin page) is disabled.
     * Also, this outputs just a single record for everything, so much more efficient.
     */
    rc_t init_fix_steal(
        btree_page_h*        parent,
        const lpid_t&        pid,
        shpid_t              root, 
        int                  level,
        shpid_t              pid0,
        shpid_t              foster,
        const w_keystr_t&    fence_low,
        const w_keystr_t&    fence_high,
        const w_keystr_t&    chain_fence_high,
        btree_page_h*        steal_src  = NULL,
        int                  steal_from = 0,
        int                  steal_to   = 0,
        bool                 log_it     = true
        );

    /**
     * This sets all headers, fence/prefix keys and initial records altogether.  Used by init_fix_steal.
     * Steal records from steal_src1.  Then steal records from steal_src2 (this is used only when merging).
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
        btree_page_h*        steal_src1 = NULL,
        int                  steal_from1 = 0,
        int                  steal_to1 = 0,
        btree_page_h*        steal_src2 = NULL,
        int                  steal_from2 = 0,
        int                  steal_to2 = 0,
        bool                 steal_src2_pid0 = false
        );

    /// Steal records from steal_src.  Called by format_steal.
    void _steal_records(btree_page_h* steal_src,
                        int           steal_from,
                        int           steal_to);

    /**
     * Called when we did a split from this page but didn't move any record to new page.
     * This method can't be undone.  Use this only for REDO-only system transactions.
     */
    rc_t norecord_split (shpid_t foster,
                         const w_keystr_t& fence_high, 
                         const w_keystr_t& chain_fence_high,
                         bool log_it = true);

    /// Returns if whether we can do norecord insert now.
    bool                 check_chance_for_norecord_split(const w_keystr_t& key_to_insert) const;
    

    // ======================================================================
    //   BEGIN: Public record access functions
    // ======================================================================

     /// Returns the number of records in this page.
    int             nrecs() const;

    /// Returns if the specified record is a ghost record.
    bool            is_ghost(slotid_t slot) const;
    
    /// Retrieves key from given record #
    void            get_key(slotid_t slot,  w_keystr_t &key) const;

    /**
     * Return pointer to, length of element of given record.  Also
     * returns ghost status of given record.
     * 
     * @pre we are a leaf page
     */
    const char*     element(int slot, smsize_t &len, bool &ghost) const;

    /**
     * Attempt to copy element of given record to provided buffer
     * (out_buffer[0..len-1]).  Returns false iff failed due to
     * insufficient buffer size.
     * 
     * Sets len to actual element size in all cases.  Also returns
     * ghost status of given record.
     * 
     * @pre we are a leaf page
     */
    bool            copy_element(int slot, char *out_buffer, smsize_t &len, bool &ghost) const;

    /// Return the (non-opaque) child pointer of record in slot.
    shpid_t       child(slotid_t slot) const;
    /// Return the opaque child pointer of record in slot.
    shpid_t       child_opaqueptr(slotid_t slot) const;


    /**
     * Returns a pointer to given page pointer (e.g., shpid_t).
     * 
     * Offset may be -2 for leaf nodes or [-2,nrecs()-1] for internal
     * nodes.  It denotes:
     * 
     *   -2: the foster pointer
     *   -1: pid0
     *    0..nrec()-1: the child pointer of the record in the corresponding slot
     * 
     * The page pointer will be at a 4-byte aligned address and
     * opaque.
     *
     * (This method is used to implement swizzling of page pointers
     * atomically.)
     */
    shpid_t* page_pointer_address(int offset);


    /**
     * Returns physical space used by the record currently in the
     * given slot (including padding and other overhead due to that
     * slot being occupied).
     */
    size_t              get_rec_space(int slot) const;


    // ======================================================================
    //   BEGIN: Search functions
    // ======================================================================

    /**
     * Search for given key in this B-tree page.  Stores in found_key
     * the key found status and in return_slot the slot where the key
     * was found or (if not found) the slot where the key should go if
     * inserted.  Note in the latter case that return_slot may be
     * nrecs().
     */
    void            search(const w_keystr_t& key,
                           bool&             found_key,
                           slotid_t&         return_slot) const {
        search((const char*) key.buffer_as_keystr(), key.get_length_as_keystr(), found_key, return_slot);
    }

    /**
     * Search for given key in this B-tree page.  Stores in found_key
     * the key found status and in return_slot the slot where the key
     * was found or (if not found) the slot where the key should go if
     * inserted.  Note in the latter case that return_slot may be
     * nrecs().
     */
    void            search(const char *key_raw, size_t key_raw_len,
                           bool& found_key, slotid_t& return_slot) const;
    /**
     * This method provides the same results as the normal search
     * method when our associated B-tree page is not being
     * concurrently modified.
     * 
     * When the B-tree page is being concurrently modified, however,
     * unlike the normal method this version does not trigger
     * assertions or cause other faults (e.g., segmentation fault);
     * it may however in this case provide garbage values.
     */
    void            robust_search(const char *key_raw, size_t key_raw_len,
                                  bool& found_key, slotid_t& return_slot) const;


    /**
     * Search for given key in this interior node, determining which
     * child pointer should be taken to continue searching down the
     * B-tree.
     *
     * Stores in return_slot the slot whose child pointer should be
     * followed, with -1 denoting that the pid0 child pointer should
     * be followed.
     * 
     * Note: keys in interior nodes are separator keys.  Like fence
     * keys, a separator key is exclusive for left and inclusive for
     * right.  For example, a separator key "AB" sends "AA" to left,
     * "AAZ" to left, "AB" to right, "ABA" to right, and "AC" to
     * right.
     * 
     * @pre this is an interior node
     */
    void            search_node(const w_keystr_t& key,
                                slotid_t&         return_slot) const;


    // ======================================================================
    //   BEGIN: Insert/Update/Delete functions
    // ======================================================================

    /**
     *  Insert a new entry at "slot".  This is used only for non-leaf pages.
     * For leaf pages, always use replace_ghost() and reserve_ghost().
     * @param child child pointer to add
     */
    rc_t            insert_node(const w_keystr_t&   key,
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
    rc_t            replace_ghost(const w_keystr_t &key, const cvec_t &elem);

    /**
     * Replaces the special fence record with the given new data,
     * expanding the slot length if needed.
     */
    rc_t            replace_fence_rec_nolog(const w_keystr_t& low, const w_keystr_t& high, const w_keystr_t& chain, int new_prefix_length= -1);

    /**
     *  Remove the slot and up-shift slots after the hole to fill it up.
     * @param slot the slot to remove.
     */
    rc_t            remove_shift_nolog(slotid_t slot);

    /**
     * Replaces the given slot with the new data.  key is not changed.
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
     * Creates a dummy ghost record with the given key and element
     * length as a preparation for subsequent insertion.
     * This is used by insertion (its nested system transaction).
     * This function itself does NOT log, so the caller is responsible for it.
     */
    void             reserve_ghost(const w_keystr_t &key, size_t element_length) {
        reserve_ghost((const char *)key.buffer_as_keystr(), key.get_length_as_keystr(), element_length);
    }
    // to make it slightly faster.  not a neat kind of optimization
    void             reserve_ghost(const char *key_raw, size_t key_raw_len, size_t element_length);

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
    bool           check_space_for_insert_leaf(const w_keystr_t &trunc_key, const cvec_t &el);
    bool           check_space_for_insert_leaf(size_t trunc_key_length, size_t element_length);
    /// for intermediate node (no element).
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
     * @param[in] triggering_key the key to be inserted after this split.  used to determine split policy.
     */
    void                 suggest_fence_for_split(
                             w_keystr_t &mid, slotid_t& right_begins_from, const w_keystr_t &triggering_key) const;
    /// For recovering the separator key from boundary place.  @see suggest_fence_for_split().
    w_keystr_t           recalculate_fence_for_split(slotid_t right_begins_from) const;

    bool                 is_insertion_extremely_skewed_right() const;
    bool                 is_insertion_skewed_right() const;
    bool                 is_insertion_skewed_left()  const;


    // ======================================================================
    //   BEGIN: Statistics/Debug etc functions
    // ======================================================================

    /**
     * \brief Defrags this page to remove holes and ghost records in the page.
     * \details
     * A page can have unused holes between records and ghost records as a result
     * of inserts and deletes.  This method removes those dead spaces to compress
     * the page.  The best thing of this is that we have to log only
     * the slot numbers of ghost records that are removed because there are
     * 'logically' no changes.
     * Context: System transaction.
     */
    rc_t                         defrag();

    /// stats for leaf nodes.
    rc_t             leaf_stats(btree_lf_stats_t& btree_lf);
    /// stats for interior nodes.
    rc_t             int_stats(btree_int_stats_t& btree_int);

    /// Debugs out the contents of this page.
    void             print(bool print_elem=false);

    /**
     * Checks integrity of this page.
     * This function does NOT check integrity across multiple pages.
     * For that purpose, use btree_m::verify_tree().
     * If the two arguments are both false (which is default), this function should be very
     * efficient.
     * @param[in] check_keyorder whether to check the sortedness and uniqueness
     * of the keys in this page.  setting this to true makes this function expensive.
     * @param[in] check_space whether to check any overlaps of
     * records and integrity of space offset.  setting this to true makes this function expensive.
     * @return true if this page is in a consistent state
     */
    bool             is_consistent (bool check_keyorder = false, bool check_space = false) const;


    /*
     * The combined sizes of the key (i.e., the number of actual data
     * bytes it contains) and value must be less than or equal to \ref
     * max_entry_size, which is a function of the page size, and is
     * such that two entries of this size fit on a page along with all
     * the page and entry metadata.  See sm_config_info_t and
     * ss_m::config_info.
     */
    static smsize_t         max_entry_size;


private:
    // ======================================================================
    //   BEGIN: Private record data packers
    // ======================================================================

    /// A field to hold a B-tree key length
    typedef uint16_t key_length_t;


    /**
     * Pack a node record's information into out, suitable for use
     * with insert_item.
     * 
     * trunc_key is the record's key (including its w_keystr_t sign
     * byte) without its prefix.
     */
    void _pack_node_record(cvec_t& out, const cvec_t& trunc_key) const;

    /// type of scratch space needed by _pack_leaf_record
    typedef key_length_t pack_scratch_t;
    /**
     * Pack a leaf record's information into out, suitable for use
     * with insert_item.
     * 
     * trunc_key is the record's key (including its w_keystr_t sign
     * byte) without its prefix; element is the record's associated
     * value.
     * 
     * out_scratch is a scratch work area that the caller must
     * provide; it must remain in scope in order for out to be used
     * (i.e., out will point to part(s) of out_scratch).
     */
    void _pack_leaf_record(cvec_t& out, pack_scratch_t& out_scratch,
                           const cvec_t& trunc_key,
                           const char* element, size_t element_len) const;
    /**
     * Pack a leaf record's key information *only* into out, suitable
     * for use with insert_item with variable-size--data length
     * computed by _predict_leaf_data_length using in addition the
     * expected element length.  (This is used by reserve_ghost to set
     * only the key information leaving reserved space for the future
     * element.)
     * 
     * trunc_key is the record's key (including its w_keystr_t sign
     * byte) without its prefix.
     * 
     * out_scratch is a scratch work area that the caller must
     * provide; it must remain in scope in order for out to be used
     * (i.e., out will point to part(s) of out_scratch).
     */
    void _pack_leaf_record_prefix(cvec_t& out, pack_scratch_t& out_scratch,
                                  const cvec_t& trunc_key) const;

    /**
     * Pack a B-tree page's fence and foster key information into out,
     * suitable for use with insert_item.
     * 
     * All input keys are uncompressed.  During packing, prefix
     * elimination is used.  The prefix length to truncate is
     * new_prefix_len if non-negative; otherwise, the prefix length to
     * use is computed as the maximum number of common prefix bytes of
     * the low and high fence keys.  The prefix length used is
     * returned either way.
     * 
     * The caller is responsible for ensuring that
     * btree_fence_low_length, btree_fence_high_length,
     * btree_chain_fence_high_length are set to the lengths of the
     * corresponding keys and that btree_prefix_length is set to the
     * returned prefix length.
     */
    int _pack_fence_rec(cvec_t& out, const w_keystr_t& low,
                        const w_keystr_t& high, 
                        const w_keystr_t& chain, 
                        int new_prefix_len) const;

    /**
     * Compute needed length of variable-size data to store a leaf
     * record with the given truncated key and element lengths.
     */
    size_t _predict_leaf_data_length(int trunc_key_length, int element_length) const;


    /**
     * \brief Poor man's normalized key type.
     *
     * \details
     * To speed up comparison this should be an integer type, not char[]. 
     */
    typedef uint16_t poor_man_key;

    /// Returns the value of poor-man's normalized key for the given key string WITHOUT prefix.
    poor_man_key _extract_poor_man_key(const void* trunc_key, size_t trunc_key_len) const;
    /// Returns the value of poor-man's normalized key for the given key string WITHOUT prefix.
    poor_man_key _extract_poor_man_key(const cvec_t& trunc_key) const;

    /// Returns the value of poor-man's normalized key for the given key string WITH prefix.
    poor_man_key _extract_poor_man_key(const void* key_with_prefix, 
                                       size_t key_len_with_prefix, size_t prefix_len) const;


    // ======================================================================
    //   BEGIN: Private record accessors
    // ======================================================================

    /// Return poor man's key data for given slot
    poor_man_key _poor(int slot) const;

    /**
     * Retrieves the key WITHOUT prefix of specified slot in a leaf
     * 
     * @pre we are a leaf node
     */
    const char*     _leaf_key_noprefix(slotid_t slot,  size_t &len) const;

    /**
     * Retrieves the key WITHOUT prefix of specified slot in an interior node
     * 
     * @pre we are an interior node
     */
    const char*     _node_key_noprefix(slotid_t slot,  size_t &len) const;

    /**
     * Calculate offset within slot's variable-sized data to its
     * element data.  All data from that point onwards makes up the
     * element data.
     * 
     * @pre we are a leaf node
     */
    size_t _element_offset(int slot) const;

    /// returns compare(specified-key, key_noprefix)
    int _compare_key_noprefix(slotid_t slot, const void *key_noprefix, size_t key_len) const;

    /// compare slot slot's key with given key (as key_noprefix,key_len,poor tuple)
    /// result <0 if slot's key is before given key
    int _compare_slot_with_key(int slot, const void* key_noprefix, size_t key_len, poor_man_key poor) const;


    // ======================================================================
    //   BEGIN: Private robust record accessors
    // ======================================================================

    /*
     * These methods provide the same results as the corresponding
     * non-robust versions when our associated B-tree page is not
     * being concurrently modified.
     * 
     * When the B-tree page is being concurrently modified, however,
     * unlike the normal methods these versions do not trigger
     * assertions or cause other faults (e.g., segmentation fault);
     * they may however in this case provide garbage values.
     * 
     * Slot arguments here should be less than a result of
     * page()->robust_number_of_items()-1.
     * 
     * The memory regions indicated by _robust_*_key_noprefix() are
     * always safe to access, but may contain garbage or have a length
     * different from the actual slot's key_noprefix.
     */

    /**
     * [Robust] Retrieves the key WITHOUT prefix of specified slot in a leaf
     * 
     * @pre we are a leaf node
     */
    const char*     _robust_leaf_key_noprefix(slotid_t slot,  size_t &len) const;
    /**
     * [Robust] Retrieves the key WITHOUT prefix of specified slot in an interior node
     * 
     * @pre we are an interior node
     */
    const char*     _robust_node_key_noprefix(slotid_t slot,  size_t &len) const;

    /// [Robust] returns compare(specified-key, key_noprefix)
    int _robust_compare_key_noprefix(slotid_t slot, const void *key_noprefix, size_t key_len) const;

    /// [Robust] compare slot slot's key with given key (as key_noprefix,key_len,poor tuple)
    /// result <0 if slot's key is before given key
    int _robust_compare_slot_with_key(int slot, const void* key_noprefix, size_t key_len, poor_man_key poor) const;


    // ======================================================================
    //   BEGIN: Miscellaneous private members
    // ======================================================================

    /// internal method used from is_consistent() to check keyorder correctness.
    bool             _is_consistent_keyorder() const;

    /// checks if the poor-man's normalized keys are valid.
    bool             _is_consistent_poormankey() const;

    /// Given the place to insert, update btree_consecutive_skewed_insertions.
    void             _update_btree_consecutive_skewed_insertions(slotid_t slot);

    /**
     * Returns if there is enough free space to accomodate the given
     * item.
     * @return true if there is free space
     */
    bool _check_space_for_insert(size_t data_length);  
};


/**
 * \brief Specialized variant of btree_page_h that borrows a B-tree
 * page from a fixable_page_h.
 *
 * \details 
 * Borrows the latch of a fixable_page_h for the duration of our
 * existence.  Returns the latch when destroyed.  Do not use the
 * original handle while its latch is borrowed.  Transitive borrowing
 * is fine.
 */
class borrowed_btree_page_h : public btree_page_h {
    fixable_page_h* _source;

public:
    borrowed_btree_page_h(fixable_page_h* source) :
        btree_page_h(source->get_generic_page()),
        _source(source)
    {
        _mode = _source->_mode;
        _source->_mode = LATCH_NL;
    }

    ~borrowed_btree_page_h() {
        w_assert1(_source->_mode == LATCH_NL);
        _source->_mode = _mode;
        _mode = LATCH_NL;
    }
};



// ======================================================================
//   BEGIN: Inline function implementations
// ======================================================================

inline lpid_t btree_page_h::root() const {
    lpid_t p = pid();
    p.page = page()->btree_root;
    return p;
}

inline int btree_page_h::level() const {
    return page()->btree_level;
}

inline shpid_t btree_page_h::pid0_opaqueptr() const {
    return page()->btree_pid0;
}

inline shpid_t btree_page_h::pid0() const {
    shpid_t shpid = page()->btree_pid0;
    if (shpid) {
        return smlevel_0::bf->normalize_shpid(shpid);
    }
    return shpid;
}

inline bool btree_page_h::is_leaf() const {
    return level() == 1;
}

inline bool btree_page_h::is_leaf_parent() const {
    return level() == 2;
}

inline bool btree_page_h::is_node() const {
    return ! is_leaf();
}
   
inline shpid_t btree_page_h::get_foster_opaqueptr() const {
    return page()->btree_foster;
}

inline shpid_t btree_page_h::get_foster() const {
    shpid_t shpid = page()->btree_foster;
    if (shpid) {
        return smlevel_0::bf->normalize_shpid(shpid);
    }
    return shpid;
}

inline int16_t btree_page_h::get_prefix_length() const {
    return page()->btree_prefix_length;
}
inline int16_t btree_page_h::get_fence_low_length() const {
    return page()->btree_fence_low_length;
}
inline int16_t btree_page_h::get_fence_high_length() const {
    return page()->btree_fence_high_length;
}
inline int16_t btree_page_h::get_chain_fence_high_length() const {
    return page()->btree_chain_fence_high_length;
}

inline const char* btree_page_h::get_fence_low_key() const {
    return page()->item_data(0);
}
inline const char* btree_page_h::get_fence_high_key_noprefix() const {
    return page()->item_data(0) + get_fence_low_length();
}
inline const char* btree_page_h::get_chain_fence_high_key() const {
    return page()->item_data(0) + get_fence_low_length() + get_fence_high_length_noprefix();
}
inline const char* btree_page_h::get_prefix_key() const {
    return get_fence_low_key(); // same thing.  only the length differs.
}

inline int btree_page_h::nrecs() const {
    return page()->number_of_items() - 1;
}
inline int btree_page_h::compare_with_fence_low (const w_keystr_t &key) const {
    return key.compare_keystr(get_fence_low_key(), get_fence_low_length());
}
inline int btree_page_h::compare_with_fence_low (const char* key, size_t key_len) const {
    return w_keystr_t::compare_bin_str (key, key_len, get_fence_low_key(), get_fence_low_length());
}
inline int btree_page_h::compare_with_fence_low_noprefix (const char* key, size_t key_len) const {
    return w_keystr_t::compare_bin_str (key, key_len, get_fence_low_key() + get_prefix_length(), get_fence_low_length() - get_prefix_length());
}

inline int btree_page_h::compare_with_fence_high (const w_keystr_t &key) const {
    return compare_with_fence_high ((const char*) key.buffer_as_keystr(), key.get_length_as_keystr());
}
inline int btree_page_h::compare_with_fence_high (const char* key, size_t key_len) const {
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
inline int btree_page_h::compare_with_fence_high_noprefix (const char* key, size_t key_len) const {
    return w_keystr_t::compare_bin_str (key, key_len, get_fence_high_key_noprefix(), get_fence_high_length_noprefix());
}

inline int btree_page_h::compare_with_chain_fence_high (const w_keystr_t &key) const {
    return key.compare_keystr(get_chain_fence_high_key(), get_chain_fence_high_length());
}
inline int btree_page_h::compare_with_chain_fence_high (const char* key, size_t key_len) const {
    return w_keystr_t::compare_bin_str (key, key_len, get_chain_fence_high_key(), get_chain_fence_high_length());
}
inline bool btree_page_h::fence_contains(const w_keystr_t &key) const {
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

inline bool btree_page_h::is_insertion_extremely_skewed_right() const {
    // this means completely pre-sorted insertion like bulk loading.
    int ins = page()->btree_consecutive_skewed_insertions;
    return ins > 50
        || ins > nrecs() * 9 / 10
        || (ins > 1 && ins >= nrecs() - 1)
        ;
}    
inline bool btree_page_h::is_insertion_skewed_right() const {
    return page()->btree_consecutive_skewed_insertions > 5;
}
inline bool btree_page_h::is_insertion_skewed_left() const {
    return page()->btree_consecutive_skewed_insertions < -5;
}

inline shpid_t btree_page_h::child_opaqueptr(slotid_t slot) const {
    w_assert1(is_node());
    w_assert1(slot >= 0);
    w_assert1(slot < nrecs());
    return page()->item_child(slot+1);
}
inline shpid_t btree_page_h::child(slotid_t slot) const {
    shpid_t shpid = child_opaqueptr(slot);
    if (shpid) {
        return smlevel_0::bf->normalize_shpid(shpid);
    }
    return shpid;
}

inline shpid_t* btree_page_h::page_pointer_address(int offset) {
    if (offset == -2) {
        return &page()->btree_foster;
    }

    w_assert1(!is_leaf());
    w_assert1(-2<offset && offset<nrecs());

    if (offset == -1) {
        return &page()->btree_pid0;
    }

    return &page()->item_child(offset+1);
}



inline size_t btree_page_h::get_rec_space(int slot) const {
    w_assert1(slot>=0);
    return page()->item_space(slot + 1);
}


inline bool btree_page_h::is_ghost(slotid_t slot) const {
    return page()->is_ghost(slot + 1);
}


inline smsize_t 
btree_page_h::used_space() const {
    return data_sz - page()->usable_space();
}

inline smsize_t
btree_page_h::usable_space() const {
    return page()->usable_space();
}


// ======================================================================
//   BEGIN: Private record data packers inline implementation
// ======================================================================

inline void btree_page_h::_pack_node_record(cvec_t& out, const cvec_t& trunc_key) const {
    w_assert1(!is_leaf());
    out.put(trunc_key);
}

inline void btree_page_h::_pack_leaf_record_prefix(cvec_t& out, pack_scratch_t& out_scratch,
                                                   const cvec_t& trunc_key) const {
    w_assert1(is_leaf());
    out_scratch = trunc_key.size();
    out.put(&out_scratch, sizeof(out_scratch));
    out.put(trunc_key);
}
inline void btree_page_h::_pack_leaf_record(cvec_t& out, pack_scratch_t& out_scratch,
                                            const cvec_t& trunc_key,
                                            const char* element, size_t element_len) const {
    _pack_leaf_record_prefix(out, out_scratch, trunc_key);
    out.put(element, element_len);
}

inline int btree_page_h::_pack_fence_rec(cvec_t& out, const w_keystr_t& low,
                                  const w_keystr_t& high, 
                                  const w_keystr_t& chain, 
                                  int new_prefix_len) const {
    int prefix_len;
    if (new_prefix_len >= 0) {
        w_assert1(low.common_leading_bytes(high) >= (size_t)new_prefix_len);
        prefix_len = new_prefix_len;
    } else {
        prefix_len = low.common_leading_bytes(high);
    }

    out.put(low);
    // eliminate prefix part from high:
    out.put((const char*)high.buffer_as_keystr()     + prefix_len, 
            high.get_length_as_keystr() - prefix_len);
    out.put(chain);
    return prefix_len;
}

inline size_t btree_page_h::_predict_leaf_data_length(int trunc_key_length, 
                                                      int element_length) const {
    return sizeof(key_length_t) + trunc_key_length + element_length;
}

inline btree_page_h::poor_man_key btree_page_h::_extract_poor_man_key(const void* trunc_key, 
                                                                      size_t trunc_key_len) const {
    if (trunc_key_len == 0) {
        return 0;
    } else if (trunc_key_len == 1) {
        return (*reinterpret_cast<const unsigned char*>(trunc_key)) << 8;
    } else {
        return deserialize16_ho(trunc_key);
    }
}
inline btree_page_h::poor_man_key btree_page_h::_extract_poor_man_key(const cvec_t& trunc_key) const {
    char start[2];
    trunc_key.copy_to(start, 2);
    return _extract_poor_man_key(start, trunc_key.size());
}
inline btree_page_h::poor_man_key 
btree_page_h::_extract_poor_man_key(const void* key_with_prefix, size_t key_len_with_prefix, 
                                    size_t prefix_len) const {
    w_assert1(prefix_len <= key_len_with_prefix);
    return _extract_poor_man_key (((const char*)key_with_prefix) + prefix_len, key_len_with_prefix - prefix_len);
}


// ======================================================================
//   BEGIN: Private [robust] record accessors inline implementation
// ======================================================================

inline btree_page_h::poor_man_key btree_page_h::_poor(int slot) const {
    w_assert1(slot>=0);
    return page()->item_poor(slot+1);
}

inline const char* btree_page_h::_leaf_key_noprefix(slotid_t slot,  size_t &len) const {
    w_assert1(is_leaf());
    w_assert1(slot>=0);

    key_length_t* data = (key_length_t*)page()->item_data(slot+1);
    len = *data++;
    return (const char*)data;
}
inline const char* btree_page_h::_robust_leaf_key_noprefix(slotid_t slot,  size_t &len) const {
    w_assert1(slot>=0);

    size_t variable_length;
    key_length_t* data = (key_length_t*)page()->robust_item_data(slot+1, variable_length);
    if (variable_length < sizeof(key_length_t)) {
        len = 0;
        return (const char*)data;
    }
    len = *data++;
    if (len+sizeof(key_length_t) > variable_length) {
        len = 0;
        return (const char*)data;
    }
    return (const char*)data;
}

inline const char* btree_page_h::_node_key_noprefix(slotid_t slot,  size_t &len) const {
    w_assert1(is_node());
    w_assert1(slot>=0);

    len = page()->item_length(slot+1);
    return page()->item_data(slot+1);
}
inline const char* btree_page_h::_robust_node_key_noprefix(slotid_t slot,  size_t &len) const {
    w_assert1(slot>=0);

    return page()->robust_item_data(slot+1, len);
}

inline size_t btree_page_h::_element_offset(int slot) const {
    w_assert1(is_leaf());
    w_assert1(slot>=0);

    size_t key_noprefix_length;
    (void)_leaf_key_noprefix(slot, key_noprefix_length);

    return key_noprefix_length + sizeof(key_length_t);
}

inline int btree_page_h::_compare_key_noprefix(slotid_t slot, const void *key_noprefix, 
                                               size_t key_len) const {
    size_t      curkey_len;
    const char *curkey;
    if (is_leaf()) {
        curkey = _leaf_key_noprefix(slot, curkey_len);
    } else {
        curkey = _node_key_noprefix(slot, curkey_len);
    }

    return w_keystr_t::compare_bin_str(curkey, curkey_len, key_noprefix, key_len);
}
inline int btree_page_h::_robust_compare_key_noprefix(slotid_t slot, const void *key_noprefix, 
                                                      size_t key_len) const {
    size_t      curkey_len;
    const char *curkey;
    if (page()->robust_is_leaf()) {
        curkey = _robust_leaf_key_noprefix(slot, curkey_len);
    } else {
        curkey = _robust_node_key_noprefix(slot, curkey_len);
    }

    return w_keystr_t::compare_bin_str(curkey, curkey_len, key_noprefix, key_len);
}

#endif // BTREE_PAGE_H_H
