#ifndef BTREE_P_H
#define BTREE_P_H

#include "w_defines.h"

#ifdef __GNUG__
#pragma interface
#endif

#include "page.h"
#include "w_key.h"

struct btree_lf_stats_t;
struct btree_int_stats_t;
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
 * BTree data page uses the common data layout defined in page_s.
 * However, it also has the BTree-specific header placed in the beginning of "data".
 *
 * \section FBTREE Fence Keys and B-link Tree
 * Our version of BTree header contains low-high fence keys and pointers as a b-link tree.
 * fence keys are NEVER changed once the page is created until the page gets splitted.
 * prefix compression in this scheme utilizes it by storing the common leading bytes
 * of the two fence keys and simply getting rid of them from all entries in this page.
 * See page_s for the header definitions. They are defined as part of page_s.
 *
 * \section PAGELAYOUT BTree Page Layout
 * The page layout of BTree is as follows.
 * \verbatim
  [common-headers in page_s]
  [btree-specific-headers in page_s]
  (data area in page_s:
    [slot data which is growing forward]
    [(contiguous) free area]
    [record data which is growing forward]
      [first-slot's record (somewhere in here)] [low-fence-key data + high-fence-key data]
 )\endverbatim
 * 
 * \section RECORDLAYOUT Record Layout
 * Internally, record data is stored as follows:
 * (If the page is leaf page)
 * - length (int16_t) of key BEFORE prefix truncation
 * - length (int16_t) of el (el part gets no prefix truncation)
 * - key + el (contiguous char[]) AFTER prefix truncation
 * - [0-7] bytes of unused space (record is 8 bytes aligned).
 * 
 * (If the page is node page)
 * - length (int16_t) of key BEFORE prefix truncation
 * - pid (shpid_t; 4 bytes)
 * - key AFTER prefix truncation
 * - [0-7] bytes of unused space (record is 8 bytes aligned).
 */
class btree_p : public page_p {
    friend class btree_impl;
    friend class btree_ghost_t;
    friend class btree_ghost_mark_log;
    friend class btree_ghost_reclaim_log;
public:
#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Struct/Enum/Constructor
///==========================================
#endif // DOXYGEN_HIDE

    btree_p() {}
    btree_p(page_s* s, uint32_t store_flags) : page_p(s, store_flags) {}
    btree_p(const btree_p&p) : page_p(p) {} 
    ~btree_p() {}
    btree_p& operator=(const btree_p& p)    { page_p::operator=(p); return *this; }

    tag_t get_page_tag () const { return t_btree_p; }
    void inc_fix_cnt_stat () const { INC_TSTAT(btree_p_fix_cnt);}
    rc_t format(const lpid_t &, tag_t, uint32_t, store_flag_t)
    {
        DBG(<< "This shouldn't be called! For initial allocation of Btree page, use init_fix_steal() instead of fix()");
        return RC(fcINTERNAL);
    }


#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Header Get/Set functions
///==========================================
#endif // DOXYGEN_HIDE
    
    /** Returns 1 if leaf, >1 if non-leaf. */
    int               level() const;
    /** Returns left-most ptr (used only in non-leaf nodes). */
    shpid_t        pid0() const;
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
   shpid_t         get_blink() const;
    /** Clears the blink page and also clears the chain high fence key. */
   rc_t               clear_blink();
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
    /** Returns the high fence key, which is larger than all entries in this page and its descendants. */
    const char*  get_fence_high_key() const;
    /** Returns the length of high fence key. */
    int16_t           get_fence_high_length() const;
    /** Constructs w_keystr_t object containing the low-fence key of this page. */
    void                copy_fence_high_key(w_keystr_t &buffer) const {buffer.construct_from_keystr(get_fence_high_key(), get_fence_high_length());}
    /** Returns if the high-fence key is supremum. */
    bool              is_fence_high_supremum() const { return get_fence_high_key()[0] == SIGN_POSINF;}
    /** Returns the high fence key of Blink chain. */
    const char*  get_chain_fence_high_key() const;
    /** Returns the length of high fence key of Blink chain. */
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
    /**
     * Return value : 0 if equal.
     * : <0 if key < fence-high.
     * : >0 if key > fence-high.
     */
    int                 compare_with_fence_high (const w_keystr_t &key) const;
    /**
     * Return value : 0 if equal.
     * : <0 if key < fence-high.
     * : >0 if key > fence-high.
     */
    int                 compare_with_chain_fence_high (const w_keystr_t &key) const;

    /**
     * When allocating a new BTree page, use this instead of fix().
     * This sets all headers, fence/prefix keys, and initial records altogether.
     * As our new BTree header has variable-size part (fence keys),
     * setting fence keys later than the first format() causes a problem.
     * So, the 4-argumets format(which is called from default fix() on virgin page) is disabled.
     * Also, this outputs just a single record for everything, so much more efficient.
     */
    rc_t init_fix_steal(
        const lpid_t&        pid,
        shpid_t              root, 
        int                  level,
        shpid_t              pid0,
        shpid_t              blink,
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
        shpid_t              blink,
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
    rc_t norecord_split (shpid_t blink,
        const w_keystr_t& fence_high, const w_keystr_t& chain_fence_high,
        bool log_it = true);

    /** Returns if whether we can do norecord insert now. */
    bool                 check_chance_for_norecord_split(const w_keystr_t& key_to_insert) const;
    
#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Search and Record Access functions
///==========================================
#endif // DOXYGEN_HIDE
    
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
     * Use this instead of page_p::nslots to acount for one hidden slots.
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

    /**
    *  Return the child pointer of tuple at "slot".
    *  equivalent to rec_node(), but doesn't return key (thus faster).
    */
    shpid_t       child(slotid_t slot) const;

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
    * Replace an existing ghost record to insert the given tuple.
    * This assumes the ghost record is enough spacious.
    * @param[in] key inserted key
    * @param[in] elem record data
    */
    rc_t            replace_ghost(
        const w_keystr_t &key, const cvec_t &elem);

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

    /** stats for leaf nodes. */
    rc_t             leaf_stats(btree_lf_stats_t& btree_lf);
    /** stats for interior nodes. */
    rc_t             int_stats(btree_int_stats_t& btree_int);

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
    /** internal method used from is_consistent() to check keyorder correctness. */
    bool             _is_consistent_keyorder () const;

    /** Given the place to insert, update btree_consecutive_skewed_insertions. */
    void             _update_btree_consecutive_skewed_insertions(slotid_t slot);
    
    /** Retrieves the key of specified slot WITHOUT prefix in a leaf page.*/
    const char*     _leaf_key_noprefix(slotid_t idx,  size_t &len) const;
    /** Retrieves only the key of specified slot WITHOUT prefix in an intermediate node page.*/
    const char*     _node_key_noprefix(slotid_t idx,  size_t &len) const;
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

inline shpid_t btree_p::pid0() const
{
    return _pp->btree_pid0;
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
   
inline shpid_t btree_p::get_blink() const
{
    return _pp->btree_blink;
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
    const char*s = (const char*) page_p::tuple_addr(0);
    return s;
}
inline const char* btree_p::get_fence_high_key() const
{
    int16_t fence_low_length = get_fence_low_length();
    const char*s = (const char*) page_p::tuple_addr(0);
    return s + fence_low_length;
}
inline const char* btree_p::get_chain_fence_high_key() const
{
    int16_t fence_low_length = get_fence_low_length();
    int16_t fence_high_length = get_fence_high_length();
    const char*s = (const char*) page_p::tuple_addr(0);
    return s + fence_low_length + fence_high_length;
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
inline int btree_p::compare_with_fence_high (const w_keystr_t &key) const
{
    return key.compare_keystr(get_fence_high_key(), get_fence_high_length());
}
inline int btree_p::compare_with_chain_fence_high (const w_keystr_t &key) const
{
    return key.compare_keystr(get_chain_fence_high_key(), get_chain_fence_high_length());
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
    return key.get_length_as_keystr() - get_prefix_length()
        + el.size() + sizeof(int16_t) * 2;
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
inline shpid_t btree_p::child(slotid_t slot) const
{
    // same as rec_node except we don't need to read key
    w_assert1(is_node());
    w_assert1(slot >= 0);
    w_assert1(slot < nrecs());
    FUNC(btree_p::child);
    const char* p = (const char*) page_p::tuple_addr(slot + 1);
    // to avoid mis-aligned access on solaris, we can't do this!
    //// return *((const shpid_t*) (p + sizeof(int16_t)));
    // we have to do following
    shpid_t child;
    ::memcpy(&child, p + sizeof(int16_t), sizeof(shpid_t));
    // TODO, but on linux, this might cause unwanted overhead..
    // maybe ifdef? let's try that when this turns out to be bottleneck.
    return child;
}


#endif //BTREE_P_H
