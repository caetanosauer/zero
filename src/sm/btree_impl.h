/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BTREE_IMPL_H
#define BTREE_IMPL_H

#include "w_defines.h"

#include "btree.h"
#include "kvl_t.h"
#include "btree_verify.h"
#include "w_okvl.h"
#include "xct.h"

/**
 * \brief The internal implementation class which actually implements the
 * functions of btree_m.
 *
 * \ingroup SSMBTREE
 * \details
 * To abstract implementation details,
 * all functions of this class should be used only from btree_m except testcases.
 *
 * Like btree_m, this class is stateless, meaning all functions are
 * static and there are no class properties. Each function receives
 * the page(s) to work on and has no global side-effect.
 *
 * \section TRANSACTIONS User and System Transactions
 * The functions of this class fall into two categories.
 *
 * 1. (func name starts with \e "_ux_") functions that can be used
 * both in user and system transactions.  Such a function should
 * have only a local change and local latch.
 *
 * 2. (func name starts with \e "_sx_") functions that starts a nested
 * system transactions in it. Such a function can affect
 * the structure of BTree.
 *
 * User transactions do \e both \e logical and \e physical changes
 * and should do \e only \e local physical changes.
 *
 * System transactions do \e only \e physical changes. They sometimes
 * do \e global \e physical changes like page splitting and key-adopts.
 * They should be kept short-lived.
 *
 * \section REFERENCES References
 *
 * Basic Btree technique is from Mohan, et. al.
 * IBM Research Report # RJ 7008
 * 9/6/89
 * C. Mohan,
 * Aries/KVL: A Key-Value
 * Locking Method for Concurrency Control of Multiaction Transactions
 * Operating on B-Tree Indexes
 *
 * IBM Research Report # RJ 6846
 * 8/29/89
 * C. Mohan, Frank Levine
 * Aries/IM: An Efficient and High Concureency Index Management
 * Method using Write-Ahead Logging
 *
 * Advanced Btree techniques added as of 2011 Summer is from Graefe 2011.
 * ACM Transactions on Database Systems (TODS), 2011
 * Modern B-tree techniques
 */
class btree_impl : public smlevel_0  {
public:

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Insert/Delete functions. implemented in btree_impl.cpp
///==========================================
#endif // DOXYGEN_HIDE

    /**
     * \brief Function finds the leaf page for a key, locks the key if it exists, and determines if
     * it was found and if it was a ghost.
     * \details
     *  Context: User transaction
     * @param[in] store Store ID
     * @param[in] key key of the tuple
     * @param[out] need_lock if locking is needed
     * @param[out] slot where in the page the key was found
     * @param[out] found if the key was found
     * @param[out] took_XN if we took an XN latch on the key
     * @param[out] is_ghost if the slot was a ghost
     * @param[out] leaf the leaf the key should be in (if it exists or if it did exist)
     */
    static rc_t _ux_get_page_and_status
    (StoreID store,
     const w_keystr_t& key,
     bool& need_lock, slotid_t& slot, bool& found, bool& took_XN, bool& is_ghost, btree_page_h& leaf);

    /**
    *  \brief This function finds the leaf page to insert the given tuple,
    *  inserts it to the page and, if needed, splits the page.
    * \details
    *  Context: User transaction.
    * @param[in] store Store ID
    * @param[in] key key of the inserted tuple
    * @param[in] elem data of the inserted tuple
    */
    static rc_t                        _ux_insert(
        StoreID store,
        const w_keystr_t&                 key,
        const cvec_t&                     elem);
    /** _ux_insert()'s internal function without retry by itself.*/
    static rc_t                        _ux_insert_core(
        StoreID store,
        const w_keystr_t&                 key,
        const cvec_t&                     elem);
    /** Last half of _ux_insert, after traversing, finding (or not) and ghost determination.*/
    static rc_t _ux_insert_core_tail
    (StoreID store,
     const w_keystr_t& key,const cvec_t& el,
     bool& need_lock, slotid_t& slot, bool& found, bool& alreay_took_XN,
     bool& is_ghost, btree_page_h& leaf);

    /**
    *  \brief This function finds the given key, updates the element if found.
    * If needed, this method also splits the page.
    * \details
    *  Context: User transaction.
    * @param[in] store Store ID
    * @param[in] key key of the existing tuple
    * @param[in] elem new data of the tuple
    */
    static rc_t                        _ux_update(
        StoreID store,
        const w_keystr_t&                 key,
        const cvec_t&                     elem,
        const bool                        undo);
    /** _ux_update()'s internal function without retry by itself.*/
    static rc_t                        _ux_update_core(
        StoreID store,
        const w_keystr_t&                 key,
        const cvec_t&                     elem,
        const bool                        undo);
    /** Last half of _ux_update, after traversing, finding (or not) and ghost determination.*/
    static rc_t _ux_update_core_tail(
     StoreID store,
     const w_keystr_t& key, const cvec_t& elem,
     bool& need_lock, slotid_t& slot, bool& found, bool& is_ghost,
     btree_page_h& leaf);

   /**
    *  \brief This function finds the given key, updates the element if found and inserts it if
    * not.  If needed, this method also splits the page.  Could also be called "insert or update"
    *
    * \details
    *  Context: User transaction.
    * @param[in] store Store ID
    * @param[in] key key of the existing tuple
    * @param[in] elem new data of the tuple
    */
    static rc_t                        _ux_put(
        StoreID store,
        const w_keystr_t&                 key,
        const cvec_t&                     elem);
    /** _ux_put()'s internal function without retry by itself.  Uses _ux_insert_core_tail and
        _ux_update_core_tail for the heavy lifting*/
    static rc_t                        _ux_put_core(
        StoreID store,
        const w_keystr_t&                 key,
        const cvec_t&                     elem);


    /**
    *  \brief This function finds the given key, updates the specific part of element if found.
    * \details
    *  Context: User transaction.
    * @param[in] store Store ID
    * @param[in] key key of the existing tuple
    * @param[in] el new data of the tuple
    * @param[in] offset overwrites to this position of the record
    * @param[in] elen number of bytes to overwrite
    */
    static rc_t                        _ux_overwrite(
        StoreID store,
        const w_keystr_t&                 key,
        const char *el, smsize_t offset, smsize_t elen,
        const bool undo);
    /** _ux_overwrite()'s internal function without retry by itself.*/
    static rc_t                        _ux_overwrite_core(
        StoreID store,
        const w_keystr_t&                 key,
        const char *el, smsize_t offset, smsize_t elen,
        const bool undo);

    /**
     * \brief Creates a ghost record for the key as a preparation for insert.
     *  Context: System transaction.
     *  @param[in] leaf the page to which we insert ghost record.
     *  @param[in] key key of the ghost record.
     *  @param[in] elem_len size of elem to be inserted
     */
    static rc_t                        _sx_reserve_ghost(
        btree_page_h &leaf, const w_keystr_t &key, int elem_len);
    /** @see _sx_reserve_ghost() */
    static rc_t                        _ux_reserve_ghost_core(
        btree_page_h &leaf, const w_keystr_t &key, int elem_len);

    /**
    *  \brief Removes the specified key from B+Tree.
    * \details
    *  If the key doesn't exist, returns eNOTFOUND.
    *  Context: User transaction.
    * @param[in] store Store ID
    * @param[in] key key of the removed tuple
    */
    static rc_t                        _ux_remove(
        StoreID store,
        const w_keystr_t&   key,
        const bool          undo);

    /** _ux_remove()'s internal function without retry by itself.*/
    static rc_t _ux_remove_core(StoreID store, const w_keystr_t &key, const bool undo);

    /**
    *  \brief Reverses the ghost record of specified key to regular state.
    * \details
    *  This is only used when a delete transaction aborts.
    *  In that case, there must still exist the record with ghost mark.
    *  This method removes the ghost mark from the record.
    *  If the key doesn't exist, returns eNOTFOUND (shouldn't happen).
    *  Context: User transaction (UNDO of delete).
    * @param[in] store Store ID
    * @param[in] key key of the removed tuple
    * @see btree_ghost_mark_log::undo()
    */
    static rc_t                        _ux_undo_ghost_mark(
        StoreID store,
        const w_keystr_t&                key);

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Search/Lookup functions. implemented in btree_impl_search.cpp
///==========================================
#endif // DOXYGEN_HIDE

    /** \brief 3 modes of traverse(). */
    enum traverse_mode_t {
        /**
         * Finds a leaf page whose low-fence <= key < high-fence.
        * This is used for insert/remove and many other cases.
        */
        t_fence_contain,
        /**
        * Finds a leaf page whose low-fence == key.
        * This is used for forward cursor (similar to page.next()).
        */
        t_fence_low_match,
        /**
         * Finds a leaf page whose high-fence == key.
        * This is used for backward cursor (similar to page.prev()).
        */
        t_fence_high_match
    };
    /** Used to specify which child/foster to follow next. */
    enum slot_follow_t {
        t_follow_invalid = -3,
        t_follow_foster = -2,
        t_follow_pid0 = -1
        // 0 to nrecs-1 is child
    };

    /**
    * \brief Traverse the btree starting at root node to find an appropriate leaf page.
    * \details
    * The returned leaf page is latched in leaf_latch_mode mode
    * and the caller has to release the latch.
    *
    * This function provides 3 search mode.
    *
    * 1. t_fence_contain. find a leaf page whose low-fence <= key < high-fence.
    * This is used for insert/remove and many other cases.
    *
    * 2. t_fence_low_match. find a leaf page whose low-fence == key.
    * This is used for forward cursor (similar to page.next()).
    *
    * 3. t_fence_high_match. find a leaf page whose high-fence == key.
    * This is used for backward cursor (similar to page.prev()).
    *
    *  Context: Both user and system transaction.
    * @see traverse_mode_t
    * @param[in] store Store ID
    * @param[in] key  target key
    * @param[in] traverse_mode search mode
    * @param[in] leaf_latch_mode EX for insert/remove, SH for lookup
    * @param[out] leaf leaf satisfying search
    * @param[in] allow_retry only when leaf_latch_mode=EX. whether to retry from root if latch upgrade fails
    * @param[in] from_undo is true if caller is from an UNDO operation
    */
    static rc_t                 _ux_traverse(
        StoreID store,
        const w_keystr_t&          key,
        traverse_mode_t            traverse_mode,
        latch_mode_t               leaf_latch_mode,
        btree_page_h&                   leaf,
        bool                       allow_retry = true,
        const bool                 from_undo = false
        );

    /**
    * \brief For internal recursion. Assuming start is non-leaf, check children recursively.
    * \details
    * start has to be already fixed and SH latched, and the caller is
    * responsible to release the latch on it.
    *  Context: Both user and system transaction.
    * @param[in] start we recursively search starting from this page
    * @param[in] key  target key
    * @param[in] traverse_mode search mode
    * @param[in] leaf_latch_mode EX for insert/remove, SH for lookup
    * @param[out] leaf leaf satisfying search
    * @param[in,out] leaf_pid_causing_failed_upgrade [out:] If the latch-mode is EX,
    * and it fails upgrading the leaf page, this function returns eRETRY and fills this value.
    * [in:] On next try, put the page id in this param. This function will try EX-acquire, not upgrade.
    */
    static rc_t                 _ux_traverse_recurse(
        btree_page_h&                   start,
        const w_keystr_t&          key,
        traverse_mode_t            traverse_mode,
        latch_mode_t               leaf_latch_mode,
        btree_page_h&              leaf,
        PageID&                   leaf_pid_causing_failed_upgrade,
        const bool                 from_undo
        );

    /**
     * \brief Internal helper function to actually search for the correct slot and test fence
     * assumptions.
     * \details
     * Called only from _ux_traverse_recurse.
     * @param[in] key  target key
     * @param[in] traverse_mode search mode
     * @param[in] current The page we are currently searching.
     * @param[out] this_is_the_leaf_page True if we have found the actually target page.
     * @param[out] slot_to_follow The slot that we will to go down (or sideways) next.
     */
    static inline void _ux_traverse_search(btree_impl::traverse_mode_t traverse_mode,
                                    btree_page_h *current,
                                    const w_keystr_t& key,
                                    bool &this_is_the_leaf_page, slot_follow_t &slot_to_follow);

    /**
     * Call this function when it seems like the next page will have VERY high contention
     * and the page should adopt childrens.
     * to avoid excessive latch contention, we use mutex only in this case
     * This mutex doesn't assure correctness. latch does it.
     * it just gives us chance to avoid wasting CPU for latch contention.
     * and, false positive is fine. it's just one mutex call overhead.
     * See jira ticket:78 "Eager-Opportunistic Hybrid Latching" (originally trac ticket:80).
     */
    static rc_t _ux_traverse_try_eager_adopt(btree_page_h &current, PageID next_pid, const bool from_recovery);

    /**
     * If next has foster pointer and real-parent wants to adopt it, call this function.
     * This tries opportunistic adoption by upgrading latches to EX.
     * This has a very low cost, though might do nothing in high contention.
     * If such a writer-starvation frequently happens, the above eager_adopt function
     * will be called to do it.
     * See jira ticket:78 "Eager-Opportunistic Hybrid Latching" (originally trac ticket:80).
     */
    static rc_t _ux_traverse_try_opportunistic_adopt(btree_page_h &current, btree_page_h &next, const bool from_recovery);

    /**
    *  Find key in btree. If found, copy up to elen bytes of the
    *  entry element into el.
    *  Context: user transaction.
    * @param[in] store Store ID
    * @param[in] key key we want to find
    * @param[out] found true if key is found
    * @param[out] el buffer to put el if !cursor
    * @param[out] elen size of el if !cursor
    */
    static rc_t                 _ux_lookup(
        StoreID store,
        const w_keystr_t&          key,
        bool&                      found,
        void*                      el,
        smsize_t&                  elen
        );
    /** _ux_lookup()'s internal function which doesn't rety for locks by itself. */
    static rc_t                 _ux_lookup_core(
        StoreID store,
        const w_keystr_t&          key,
        bool&                      found,
        void*                      el,
        smsize_t&                  elen
        );

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Split/Adopt functions. implemented in btree_impl_split.cpp
///==========================================
#endif // DOXYGEN_HIDE

    /**
     * \brief Creates a new empty page as a foster-child of the given page
     * \details
     * This is the primary way of allocating a new page in Zero.
     * Whenever we need a new page, whether no-record-split or not, we allocate a
     * new page with empty key ranges. This is done as one SSX.
     * @param[in] page the new page belongs to this page as foster-child.
     * @param[out] new_page_id Page ID of the new page.
     */
    static rc_t _sx_norec_alloc(btree_page_h &page, PageID &new_page_id);

    /**
     * this version assumes system transaction as the active transaction on current thread.
     * @see _sx_norec_alloc()
     * @param[out] new_page_id Page ID of the new page.
     * @pre In SSX
     * @pre In page is EX-latched
     */
    static rc_t _ux_norec_alloc_core(btree_page_h &page, PageID &new_page_id);

    /**
     * \brief Checks all direct children of parent and, if some child has foster,
     * pushes them up to parent, resolving foster status.
     *  \details
     * If you already know which child has foster, you can use
     * _sx_adopt_foster instead.
     * Context: only in system transaction.
     * @param[in] root root node.
     * @param[in] recursive whether we recursively check all descendants and fosters.
     */
    static rc_t                 _sx_adopt_foster_all(
        btree_page_h &root, bool recursive=false);
    /** overload for recursion. */
    static rc_t                 _sx_adopt_foster_all_core(
        btree_page_h &parent, bool is_root, bool recursive);

    /**
     * \brief Pushes up a foster pointer of child to the parent.
     *  \details
     * This method resolves only one foster pointer. If there might be many
     * fosters in this child, or its siblings, consider _sx_adopt_foster_sweep().
     * Adopt consists of one or two system transactions.
     * If the parent has enough space: one SSX to move the pointer from child to parent.
     * If not: one SSX to split the parent and then another SSX to move the pointer.
     * So, it checks if we have enough space first.
     * @param[in] parent the interior node to store new children.
     * @param[in] child child page of the parent that (might) has foster-children.
     */
    static rc_t                 _sx_adopt_foster(btree_page_h &parent, btree_page_h &child);

    /**
     * this version assumes we have already split the parent if needed.
     * Context: in system transaction.
     * @see _sx_adopt_foster()
     */
    static rc_t                 _ux_adopt_foster_core(btree_page_h &parent,
                                        btree_page_h &child, const w_keystr_t &new_child_key);

    /**
     * \brief Pushes up a foster pointer of child to the parent IF we can get EX latches immediately.
     * \details
     * latch upgrade might block, in that case this method immediately returns without doing anything.
     * Context: only in system transaction.
     * @param[in] parent the interior node to store new children.
     * @param[in] child child page of the parent that (might) has foster-children.
     * @param[out] pushedup whether the adopt was done
     */
    static rc_t _sx_opportunistic_adopt_foster(btree_page_h &parent, btree_page_h &child,
                                                    bool &pushedup, const bool from_recovery);

    /**
     * \brief Pushes up all foster-children of children to the parent.
     *  \details
     * This method also follows foster-children of the parent.
     * @param[in] parent the interior node to store new children.
     */
    static rc_t _sx_adopt_foster_sweep (btree_page_h &parent_arg);

    /**
     * @see _sx_adopt_foster_sweep()
     * @see _ux_opportunistic_adopt_foster_core()
     * The difference from _sx_adopt_foster_sweep()
     * is that this function doesn't exactly check children have foster-child.
     * So, this is much faster but some foster-child might be not adopted!
     */
    static rc_t _sx_adopt_foster_sweep_approximate (btree_page_h &parent, PageID surely_need_child_pid,
                                                            const bool from_recovery);

    /** Applies the changes of one adoption on parent node. Used by both usual adoption and REDO. */
    static void _ux_adopt_foster_apply_parent (btree_page_h &parent_arg,
        PageID new_child_pid, lsn_t new_child_emlsn, const w_keystr_t &new_child_key);
    /** Applies the changes of one adoption on child node. Used by both usual adoption and REDO. */
    static void _ux_adopt_foster_apply_child (btree_page_h &child);

    /**
     * \brief Splits a page, making the new page as foster-child.
     *  \details
     * Alternative version which uses full logging and is independent of the
     * buffer pool, i.e., it does not require fixing pages. This version should
     * work for single page recovery, instant restart, as well as instant
     * restore.
     *
     * It generates a system transaction with two operations:
     * 1. On the new page (foster child), a page_img_format log record contains
     * the raw binary image with the moved log records and all metadata, fence
     * keys, and pointers correctly set. Only the "used" portion of the page is
     * logged, meaning that the data portion of the log record is at most one
     * page in size, but it should typically be around half of a page.
     *
     * 2. On the overflowing page (foster parent), a "bulk deletion" is logged.
     * It indicates that a certain range of the page slots has been deleted.
     * This corresponds to the records that have been moved to the new foster
     * child. The same log record is used to set the foster pointer and keys,
     * but this could also be implemented as a third log record.
     *
     * \author Caetano Sauer
     *
     * Context: only in system transaction.
     * @param[in] page the page to split. also called "old" page.
     * @param[out] new_page_id ID of the newly created page.
     * @param[in] triggering_key the key to be inserted after this split.
     * used to determine split policy.
     */
    static rc_t                 _sx_split_foster(btree_page_h &page,
            PageID &new_page_id, const w_keystr_t &triggering_key);

    /**
     * \brief Splits the given page if we need to do so for inserting the given key.
     * @param[in,out] page the page that might split. When this method splits the page and
     * also new_key should belong to the new page (foster-child), then this in/out param
     * is switched to the new page. Remember, we receive a reference here.
     * @param[in] new_key the key that has to be inserted.
     */
    static rc_t                 _sx_split_if_needed (btree_page_h &page,
                                                      const w_keystr_t &new_key);

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Lock related functions. implemented in btree_impl_lock.cpp
///==========================================
#endif // DOXYGEN_HIDE
    /**
     * \brief Acquires a lock on the given leaf page, tentatively unlatching the page if needed.
     * \details
     * A lock might be not immediately granted. During the wait time, we shouldn't hold
     * latch on it. So, this function first conditionally request the given lock
     * and returns if succeeds. If it doesn't succeed immediately, it unlatches the page
     * and request the lock with blocking. During this short time, it is possible that
     * other transaction updates the page, so this function also checks the page LSN
     * and if it's updated returns with eLOCKRETRY.
     * The caller is responsible to restart the operation if eLOCKRETRY is thrown.
     * When this method returns with RCOK, the page is always latched so that
     * the caller can still access the page after this method
     * although it might be "re-latched".
     * @param[in] store store id of the index
     * @param[in] leaf the page that contains the key to lock
     * @param[in] key the key to lock
     * @param[in] latch_mode if this has to un-latch/re-latch, this mode is used.
     * @param[in] lock_mode the lock mode to be acquired
     * @param[in] check_only whether the lock goes away right after grant
     */
    static rc_t _ux_lock_key(
        const StoreID&      store,
        btree_page_h&      leaf,
        const w_keystr_t&   key,
        latch_mode_t        latch_mode,
        const okvl_mode&       lock_mode,
        bool                check_only
    );

    /** raw string and length version. */
    static rc_t _ux_lock_key(
        const StoreID&      store,
        btree_page_h&      leaf,
        const void         *keystr,
        size_t              keylen,
        latch_mode_t        latch_mode,
        const okvl_mode&       lock_mode,
        bool                check_only
    );

    /**
     * Lock gap containing nonexistent key key in page leaf with locking mode
     * miss_lock_mode; exception: if key equals the low fence key of leaf, instead lock
     * just that key with lock mode exact_hit_lock_mode (the gap is not locked in this
     * case).
     *
     * @param[in] slot          The slot where key would be placed if inserted (usually a
     *                          return value of btree_page_h::search()) or -1, in which
     *                          case the leaf will be searched to determine the correct slot.
     * @param[in] latch_mode    If this has to un-latch/re-latch, this mode is used.
     * @param[in] check_only    If set, release the lock immediately after acquiring it.
     *
     * @pre key no record with key key exists in leaf (low fence is fine),
     * exact_hit_lock_mode is N for the gap, and miss_lock_mode is N for the key.
     *
     * Used when the exact key is not found and range locking is needed.
     * @see _ux_lock_key()
     */
    static rc_t _ux_lock_range(const StoreID&     store,
                               btree_page_h&     leaf,
                               const w_keystr_t& key,
                               slotid_t          slot,
                               latch_mode_t      latch_mode,
                               const okvl_mode&  exact_hit_lock_mode,
                               const okvl_mode&  miss_lock_mode,
                               bool              check_only);

    /** raw string version. */
    static rc_t _ux_lock_range(const StoreID&    store,
                               btree_page_h&    leaf,
                               const void*      keystr,
                               size_t           keylen,
                               slotid_t         slot,
                               latch_mode_t     latch_mode,
                               const okvl_mode& exact_hit_lock_mode,
                               const okvl_mode& miss_lock_mode,
                               bool             check_only);

    /**
     * \brief Assures the given leaf page has an entry whose key is the low-fence.
     * \details
     * When we are doing structural modification on leaf pages such as
     * Merge and Rebalance, we will change the fence-low key of
     * the right (foster-child) page. However, the fence-low key
     * might be used as the target of locks. Also, the fence-low key
     * might not exist as an entry. In that case, we might not be able
     * to assure serializability.
     * Therefore, this method creates a ghost entry with fence-low key
     * if the given leaf page does not have a desired entry.
     * This method is called for foster-child when Merge/Rebalance happens.
     * See jira ticket:84 "Key Range Locking" (originally trac ticket:86) for more details.
     */
    static rc_t _ux_assure_fence_low_entry(btree_page_h &leaf);

    #ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Tree Grow/Shrink/Create. implemented in btree_impl_grow.cpp
///==========================================
#endif // DOXYGEN_HIDE
    /**
     * this version assumes system transaction as the active transaction on current thread.
     * @see _sx_shrink_tree()
     */
    static rc_t                        _ux_create_tree_core(const StoreID &stid, const PageID &root_pid);

    /**
    *  \brief Shrink the tree. Copy the child page over the root page so the
    *  tree is effectively one level shorter.
     *  \details
     * Context: only in system transaction.
     * @param[in] root current root page.
    */
    static rc_t                        _sx_shrink_tree(btree_page_h& root);
    /**
     * this version assumes system transaction as the active transaction on current thread.
     * @see _sx_shrink_tree()
     */
    static rc_t                        _ux_shrink_tree_core(btree_page_h& root);
    /**
    *  \brief On root page split, allocates a new child, shifts all entries of
    *  root to new child, and has the only entry in root (pid0) point
    *  to child. Tree grows by 1 level.
     *  \details
     * Context: only in system transaction.
     * @param[in] root current root page.
    */
    static rc_t                        _sx_grow_tree(btree_page_h& root);

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: BTree Verification. implemented in btree_impl_verify.cpp
///==========================================
#endif // DOXYGEN_HIDE

    /**
    *  \brief Verifies the integrity of whole tree using the fence-key bitmap technique.
    *  \details
    * This method constructs a bitmap, traverses all pages in this BTree
    * and flips each bit based on facts collected from each page.
    * The size of bitmap is 2^hash_bits bits = 2^(hash_bits - 3) bytes.
    * We recommend hash_bits to be around 20.
    * For more details of this algorithm, check out TODS'11 paper.
    * Context: in both user and system transaction.
    * \note This method holds a tree-wide read latch/lock. No concurrent update is allowed.
    * @param[in] store Store ID
    * @param[in] hash_bits the number of bits we use for hashing, at most 31.
    * @param[out] consistent whether the BTree is consistent
    */
    static rc_t                        _ux_verify_tree(
        StoreID store, int hash_bits, bool &consistent);


    /**
    * Internal method to be called from _ux_verify_tree() for recursively check foster and children.
    * @param[in] parent page to check
    * @param[in] context context object to maintain
    */
    static rc_t                        _ux_verify_tree_recurse(
        btree_page_h &parent, verification_context &context);

    /**
    * Internal method to check each page. Called both from tree-recurse type and
    * sequential-scan type. This function doesn't follow child or foster pointers.
    * @param[in] page the page to check
    * @param[in] context context object to maintain
    */
    static rc_t                        _ux_verify_feed_page(
        btree_page_h &page, verification_context &context);

    /**
     * \brief Verifies consistency of all BTree indexes in the volume.
     * \details Unlike verify_index() this method sequentially scans
     * all pages in this volume to efficiently conduct the batch-verification.
     * However, you cannot have concurrent update operations while
     * you are running this verification. It might cause deadlocks!
     * To allow concurrent transaction while verifying, consider using _ux_verify_tree().
     * @param[in] vid The volume of interest.
     * @param[in] hash_bits the number of bits we use for hashing per BTree, at most 31.
     * @param[out] result Results of the verification.
     * @see _ux_verify_tree()
     */
    static rc_t                       _ux_verify_volume(
        int hash_bits, verify_volume_result &result);

    /** initialize context for in-query verification.*/
    static void inquery_verify_init(StoreID store);
    /** checks one page against the given expectation. */
    static void inquery_verify_fact(btree_page_h &page);
    /** adds expectation for next page. */
    static void inquery_verify_expect(btree_page_h &page, slot_follow_t next_follow);

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Defrag/Reorg functions. implemented in btree_impl_defrag.cpp
///==========================================
#endif // DOXYGEN_HIDE
    /**
    *  \brief Checks the whole tree to opportunistically adopt, in-page defrag and rebalance.
    *  \details
    * This method recursively checks the entire tree and to find
    * something that needs maintenance, such as B-link chain, ghost records and unbalanced nodes.
    * If it finds such pages, it does adopt/defrag/merge etc.
    * This method is completely opportunistic, meaning it doesn't require a global latch or lock.
    * If there is some page this method can't get EX latch immediately, it skips the page.
    * Context: in system transaction.
    * @param[in] store Store ID
    * @param[in] inpage_defrag_ghost_threshold 0 to 100 (percent). if page has this many ghosts, and:
    * @param[in] inpage_defrag_usage_threshold 0 to 100 (percent). and it has this many used space, it does defrag.
    * @param[in] does_adopt whether we do adopts
    * @param[in] does_merge whether we do merge/rebalance (which might trigger de-adopt as well)
    */
    static rc_t                        _sx_defrag_tree(
        StoreID store,
        uint16_t inpage_defrag_ghost_threshold = 10,
        uint16_t inpage_defrag_usage_threshold = 50,
        bool does_adopt = true,
        bool does_merge = true);

    /**
     * @see _sx_defrag_tree()
     */
    static rc_t                        _ux_defrag_tree_core(
        StoreID store,
        uint16_t inpage_defrag_ghost_threshold,
        uint16_t inpage_defrag_usage_threshold,
        bool does_adopt,
        bool does_merge);

    /**
     * \brief Defrags the given page to remove holes and ghost records in the page.
     * \details
     * A page can have unused holes between records and ghost records as a result
     * of inserts and deletes. This method removes those dead spaces to compress
     * the page. The best thing of this is that we have to log only
     * the slot numbers of ghost records that are removed because there are
     * 'logically' no changes.
     * Context: System transaction.
     * @param[in] pid the page to be defraged
     */
    static rc_t _sx_defrag_page(btree_page_h &page);

    /**
     * this version assumes system transaction as the active transaction on current thread.
     * @see _sx_defrag_page()
     */
    static rc_t _ux_defrag_page_core(btree_page_h &p);

    /**
    * Helper method to create an OKVL instance on one partition,
    * using the given key.
    */
    static okvl_mode create_part_okvl(okvl_mode::element_lock_mode mode, const w_keystr_t& key);

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: Global Approximate (non-protected) Counters to guide opportunistic/eager latching.
///==========================================
#endif // DOXYGEN_HIDE
    // see jira ticket:78 "Eager-Opportunistic Hybrid Latching" (originally trac ticket:80)
    // these are used to help determine when we should do eager EX latching.
    // these don't have to be exact or transactionally protected, so just static variables.
    enum {
        GAC_HASH_BITS = 16, // 64K
        GAC_HASH_MOD = 65521 // some prime number smaller than 2^GAC_HASH_BITS
    };
    /**
     * The corresponding page is a real-parent of some foster-parent page
     * The value means an approximate count of failed Upgrade of the page.
     */
    static uint8_t s_ex_need_counts[1 << GAC_HASH_BITS];
    /**
     * To avoid excessive spin locks on the same page,
     * use this mutex when you are suspicious that the page is in high contention (should be rare).
     * Note that you don't have to. The data protection is still done by latch.
     * This is just optional to avoid CPU usage.
     */
    static queue_based_lock_t s_ex_need_mutex[1 << GAC_HASH_BITS];
    /**
     * The corresponding page is a foster-parent.
     * The value means an approximate count of foster-children.
     * The value is incremented when a page-split happens.
     */
    static uint8_t s_foster_children_counts[1 << GAC_HASH_BITS];

    /** simple modular hashing. this must be cheap. */
    inline static uint32_t shpid2hash (PageID pid) {
        return pid % GAC_HASH_MOD;
    }
    /** Returns the mutex we should use when the given page is expected to be high-contended. */
    inline static queue_based_lock_t* mutex_for_high_contention (PageID pid) {
        return s_ex_need_mutex + shpid2hash(pid);
    }

    /** Returns if the page should be fixed with EX latch. */
    inline static bool is_ex_recommended (PageID pid) {
        uint32_t hash = shpid2hash(pid);
        w_assert1(hash < (1 << GAC_HASH_BITS));
        return (s_ex_need_counts[hash] > 30);
    }
    /** Returns if the page is likely to have foster-child. */
    inline static uint8_t get_expected_childrens (PageID pid) {
        uint32_t hash = shpid2hash(pid);
        w_assert1(hash < (1 << GAC_HASH_BITS));
        return s_foster_children_counts[hash];
    }
    /** Call this when encountered a failed upgrade. Again, doesn't need to be exact! */
    inline static void increase_ex_need (PageID real_parent_pid) {
        uint32_t hash = shpid2hash(real_parent_pid);
        w_assert1(hash < (1 << GAC_HASH_BITS));
        if (s_ex_need_counts[hash] < 255) {
            ++s_ex_need_counts[hash];
        }
    }
    /** Call this when there happened a split. Again, doesn't need to be exact! */
    inline static void increase_forster_child (PageID new_foster_parent_pid) {
        uint32_t hash = shpid2hash(new_foster_parent_pid);
        w_assert1(hash < (1 << GAC_HASH_BITS));
        ++s_foster_children_counts[hash];
        if (s_foster_children_counts[hash] < 255) {
            ++s_foster_children_counts[hash];
        }
    }
    /** Call this when adopted children under the page.*/
    inline static void clear_ex_need (PageID real_parent_pid) {
        uint32_t hash = shpid2hash(real_parent_pid);
        w_assert1(hash < (1 << GAC_HASH_BITS));
        s_ex_need_counts[hash] = 0;
    }
    /** Call this when you cleared foster status of the page.*/
    inline static void clear_forster_child (PageID foster_parent_pid) {
        uint32_t hash = shpid2hash(foster_parent_pid);
        w_assert1(hash < (1 << GAC_HASH_BITS));
        s_foster_children_counts[hash] = 0;
    }
};
#endif //BTREE_IMPL_H
