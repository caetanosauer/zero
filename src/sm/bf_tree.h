/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BF_TREE_H
#define BF_TREE_H

#include "w_defines.h"
#include "latch.h"
#include "tatas.h"
#include "vol.h"
#include "generic_page.h"
#include "bf_idx.h"
#include "bf_hashtable.h"
#include "bf_tree_cb.h"
#include <iosfwd>
#include "page_cleaner_base.h"

class sm_options;
class lsn_t;
struct bf_tree_cb_t; // include bf_tree_cb.h in implementation codes

class test_bf_tree;
class test_bf_fixed;
class bf_tree_cleaner;
class bf_tree_cleaner_slave_thread_t;
class page_cleaner_mgr;
class btree_page_h;
struct EvictionContext;

/** Specifies how urgent we are to evict pages. \NOTE Order does matter.  */
enum evict_urgency_t {
    /** Not urgent at all. We don't even try multiple rounds of traversal. */
    EVICT_NORMAL = 0,
    /** Continue until we evict the given number of pages or a few rounds of traversal. */
    EVICT_EAGER,
    /** We evict the given number of pages, even trying unswizzling some pages. */
    EVICT_URGENT,
    /** No mercy. Unswizzle/evict completely. Mainly for testcases/experiments. */
    EVICT_COMPLETE,
};

/** a swizzled pointer (page ID) has this bit ON. */
const uint32_t SWIZZLED_PID_BIT = 0x80000000;
inline bool is_swizzled_pointer (PageID shpid) {
    return (shpid & SWIZZLED_PID_BIT) != 0;
}


// A flag for the experiment to simulate a bufferpool without swizzling.
// "_enable_swizzling" in bf_tree_m merely turns on/off swizzling of non-root pages.
// to make it completely off, define this flag.
// this's a compile-time flag because this simulation is just a redundant code (except the experiment)
// which should go away for best performance.
// #define SIMULATE_NO_SWIZZLING

// A flag to additionally take EX latch on swizzling a pointer.
// This is NOT required because reading/writing a 4-bytes integer is always atomic,
// and because we keep hashtable entry for swizzled pages. So, it's fine even if
// another thread reads the stale value (non-swizzled page id).
// this flag is just for testing the contribution of the above idea.
// #define EX_LATCH_ON_SWIZZLING
// allow avoiding swizzling. this is also just for an experiment.
// #define PAUSE_SWIZZLING_ON

// A flag for the experiment to simulate a bufferpool without eviction.
// All pages are fixed and never evicted, assuming a bufferpool larger than data.
// Also, this flag assumes there is only one volume.
// #define SIMULATE_MAINMEMORYDB

// A flag whether the bufferpool maintains replacement priority per page.
#define BP_MAINTAIN_REPLACEMENT_PRIORITY

// A flag whether the bufferpool can evict pages of btree inner nodes
#define BP_CAN_EVICT_INNER_NODE

// A flag whether the bufferpool should alternate location of latches and control blocks
// starting at an odd multiple of 64B as follows: |CB0|L0|L1|CB1|CB2|L2|L3|CB3|...
// This layout addresses a pathology that we attribute to the hardware spatial prefetcher.
// The default layout allocates a latch right after a control block so that
// the control block and latch live in adjacent cache lines (in the same 128B sector).
// The pathology happens because when we write-access the latch, the processor prefetches
// the control block in read-exclusive mode even if we late really only read-access the
// control block. This causes unnecessary coherence traffic. With the new layout, we avoid
// having a control block and latch in the same 128B sector.
#define BP_ALTERNATE_CB_LATCH

// A flag whether the bufferpool maintains a per-frame counter that tracks how many
// swizzled pointers are in each frame. This counter is a conservative hint rather than
// an accurate counter as the bufferpool does not track removals of pointers from a page
// which can happen during merges.
#define BP_TRACK_SWIZZLED_PTR_CNT

// Use the new layout with swizzling
#if !defined SIMULATE_MAINMEMORYDB && !defined SIMULATE_NO_SWIZZLING
# define BP_ALTERNATE_CB_LATCH
#endif


#ifndef PAUSE_SWIZZLING_ON
const bool _bf_pause_swizzling = false; // compiler will strip this out from if clauses. so, no overhead.
#endif // PAUSE_SWIZZLING_ON

/**
 * When unswizzling is triggered, _about_ this number of frames will be unswizzled at once.
 * The smaller this number, the more frequent you need to trigger unswizzling.
 */
const uint32_t UNSWIZZLE_BATCH_SIZE = 1000;

/**
* When eviction is triggered, _about_ this number of frames will be evicted at once.
* Given as a ratio of the buffer size (currently 1%)
*/
const float EVICT_BATCH_RATIO = 0.01;

/**
* We don't go through frames for each evict/unswizzle try.
*/
const uint16_t EVICT_MAX_ROUNDS = 20;

/**
 * Maximum value of the per-frame refcount (reference counter).
 * We cap the refcount to avoid contention on the cacheline of the frame's control
 * block (due to ping-pongs between sockets) when multiple sockets read-access the same frame.
 * The refcount max value should have enough granularity to separate cold from hot pages.
 */
const uint16_t BP_MAX_REFCOUNT = 16;

/**
 * Initial value of the per-frame refcount (reference counter).
 */
const uint16_t BP_INITIAL_REFCOUNT = 0;

class bf_eviction_thread_t : public smthread_t
{
public:
    bf_eviction_thread_t();

    virtual void run();
};

/**
 * \Brief The new buffer manager that exploits the tree structure of indexes.
 * \ingroup SSMBUFPOOL
 * \Details
 * This is the new buffer manager in Foster B-tree which only deals with
 * tree-structured stores such as B-trees.
 * This class and bf_fixed_m effectively replace the old bf_core_m.
 *
 * \section{Pointer-Swizzling}
 * See bufferpool_design.docx.
 *
 * \section{Hierarchical-Bufferpool}
 * This bufferpool assumes hierarchical data dastucture like B-trees.
 * fix() receives the already-latched parent pointer and uses it to find
 * the requested page. Especially when the pointer to the child is swizzled,
 *
 */
class bf_tree_m {
    friend class test_bf_tree; // for testcases
    friend class test_bf_fixed; // for testcases
    friend class bf_tree_cleaner; // for page cleaning
    friend class bf_tree_cleaner_slave_thread_t; // for page cleaning
    friend class bf_eviction_thread_t;
    friend class WarmupThread;
    friend class page_cleaner_mgr;
    friend class page_cleaner_slave;

public:
#ifdef PAUSE_SWIZZLING_ON
    static bool _bf_pause_swizzling; // this can be turned on/off from any place. ugly, but it's just for an experiment.
    static uint64_t _bf_swizzle_ex; // approximate statistics. how many times ex-latch were taken on page swizzling
    static uint64_t _bf_swizzle_ex_fails; // approximate statistics. how many times ex-latch upgrade failed on page swizzling
#endif // PAUSE_SWIZZLING_ON

    /** constructs the buffer pool. */
    bf_tree_m (const sm_options&);

    /** destructs the buffer pool.  */
    ~bf_tree_m ();

    void set_cleaner(LogArchiver* _archiver, const sm_options& _options);

    /** returns the total number of blocks in this bufferpool. */
    inline bf_idx get_block_cnt() const {return _block_cnt;}

    /**
     * returns if pointer swizzling is currently enabled.
     */
    inline bool is_swizzling_enabled() const {return _enable_swizzling;}
    /**
     * enables or disables pointer swizzling in this bufferpool.
     * this method will essentially re-create the bufferpool,
     * flushing all dirty pages and evicting all pages.
     * this method should be used only when it has to be, such as before/after REDO recovery.
     */
    w_rc_t set_swizzling_enabled(bool enabled);

    /** does additional initialization that might return error codes (thus can't be done in constructor). */
    w_rc_t init (const sm_options& options);
    /** does additional clean-up that might return error codes (thus can't be done in destructor). */
    w_rc_t destroy ();

    /** returns the control block corresponding to the given memory frame index */
    bf_tree_cb_t& get_cb(bf_idx idx) const;

    /** returns a pointer to the control block corresponding to the given memory frame index */
    bf_tree_cb_t* get_cbp(bf_idx idx) const;

    /** returns the control block corresponding to the given bufferpool page. mainly for debugging. */
    bf_tree_cb_t* get_cb(const generic_page *page);

    /** returns the memory-frame index corresponding to the given control block */
    bf_idx get_idx(const bf_tree_cb_t* cb) const;

    /** returns the bufferpool page corresponding to the given control block. mainly for debugging. */
    generic_page* get_page(const bf_tree_cb_t *cb);
    generic_page* get_page(const bf_idx& idx);

    /** returns the page ID of the root page (which is already loaded in this bufferpool) in given store. mainly for debugging or approximate purpose. */
    PageID get_root_page_id(StoreID store);

    /** returns the root-page index of the root page, which is always kept in the volume descriptor:*/
    bf_idx get_root_page_idx(StoreID store);


    /**
     * Fixes a non-root page in the bufferpool. This method receives the parent page and efficiently
     * fixes the page if the shpid (pointer) is already swizzled by the parent page.
     * The optimization is transparent for most of the code because the shpid stored in the parent
     * page is automatically (and atomically) changed to a swizzled pointer by the bufferpool.
     *
     * @param[out] page         the fixed page.
     * @param[in]  parent       parent of the page to be fixed. has to be already latched. if you can't provide this,
     *                          use fix_direct() though it can't exploit pointer swizzling.
     * @param[in]  vol          volume ID.
     * @param[in]  shpid        ID of the page to fix (or bufferpool index when swizzled)
     * @param[in]  mode         latch mode.  has to be SH or EX.
     * @param[in]  conditional  whether the fix is conditional (returns immediately even if failed).
     * @param[in]  virgin_page  whether the page is a new page thus doesn't have to be read from disk.
     *
     * To use this method, you need to include bf_tree_inline.h.
     */
    w_rc_t fix_nonroot (generic_page*& page, generic_page *parent, PageID shpid,
                          latch_mode_t mode, bool conditional, bool virgin_page,
                          lsn_t emlsn = lsn_t::null);

    /**
     * Fixes a non-root page in the bufferpool given a swizzled pointer that may be stale.
     * Because of the possibility of staleness, the actual page fixed may be different
     * from the page ID given.
     *
     * @param[out] page         the fixed page.
     * @param[in]  shpid        ID of the page to fix
     * @param[in]  mode         latch mode.  has to be Q, SH, or EX.
     * @param[in]  conditional  whether the fix is conditional (returns immediately even if failed).
     * @param[out] ticket       the resulting Q ticket if mode is LATCH_Q
     *
     * @pre shpid is a swizzled pointer
     *
     * To use this method, you need to include bf_tree_inline.h.
     */
    w_rc_t fix_unsafely_nonroot(generic_page*& page, PageID shpid, latch_mode_t mode, bool conditional, q_ticket_t& ticket);

    /**
     * Special function for the REDO phase in system Recovery process
     * The page has been loaded into buffer pool and in the hashtable with known idx
     * This function associates the page in buffer pool with fixable_page data structure.
     * also store the vol and store number into the buffer (store number is not in cb)
     * There is no parent involved, and swizzling must be disabled.
     * @param[out] page the fixed page.
     * @param[in] idx idx of the page.
     */
    void associate_page(generic_page*&_pp, bf_idx idx, PageID page_updated);

    /**
     * New version of fix_direct: requires a given EMLSN
     */

    /**
     * Adds an additional pin count for the given page (which must be already latched).
     * This is used to re-fix the page later without parent pointer. See fix_direct() why we need this feature.
     * Never forget to call a corresponding unpin_for_refix() for this page. Otherwise, the page will be in the bufferpool forever.
     * @param[in] page the page that is currently latched and will be re-fixed later.
     * @return slot index of the page in this bufferpool. Use this value to the subsequent refix_direct() and unpin_for_refix() call.
     */
    bf_idx pin_for_refix(const generic_page* page);

    /**
     * Removes the additional pin count added by pin_for_refix().
     */
    void unpin_for_refix(bf_idx idx);

    /**
     * Fixes a page with the already known slot index, assuming the slot has at least one pin count.
     * Used with pin_for_refix() and unpin_for_refix().
     */
    w_rc_t refix_direct (generic_page*& page, bf_idx idx, latch_mode_t mode, bool conditional);

    /**
     * Fixes an existing (not virgin) root page for the given store.
     * This method doesn't receive page ID because it's already known by bufferpool.
     * To use this method, you need to include bf_tree_inline.h.
     */
    w_rc_t fix_root (generic_page*& page, StoreID store, latch_mode_t mode,
                     bool conditional, bool virgin);


    /** returns the current latch mode of the page. */
    latch_mode_t latch_mode(const generic_page* p);

    /**
     * upgrade SH-latch on the given page to EX-latch.
     * This method is always conditional, immediately returning if there is a conflicting latch.
     * Returns if successfully upgraded.
     */
    bool upgrade_latch_conditional(const generic_page* p);

    /** downgrade EX-latch on the given page to SH-latch. */
    void downgrade_latch(const generic_page* p);

    /**
     * Release the latch on the page.
     */
    void unfix(const generic_page* p);

    /**
     * Mark the page as dirty.
     */
    void set_dirty(const generic_page* p);
    /**
     * Returns if the page is already marked dirty.
     */
    bool is_dirty(const generic_page* p) const;

    /**
     * Returns if the page is already marked dirty.
     */
    bool is_dirty(const bf_idx idx) const;

    bf_idx lookup(PageID pid) const;

    /**
     * Update the initial dirty lsn in the page if needed.
     */
    void update_initial_dirty_lsn(const generic_page* p,
                                   const lsn_t new_lsn);

    /**
     * Set the _rec_lsn (the LSN which made the page dirty initially) in page cb
     * if it is later than the new_lsn.
     * This function is mainly used when a page format log record was generated
     */
    void set_initial_rec_lsn(PageID pid, const lsn_t new_lsn, const lsn_t current_lsn);

    /**
     * Returns true if the page's _used flag is on
     */
    bool is_used (bf_idx idx) const;

    /**
     * Adds a write-order dependency such that one is always written out after another.
     * @param[in] page the page to be written later. must be latched.
     * @param[in] dependency the page to be written first. must be latched.
     * @return whether the dependency is successfully registered. for a number of reasons,
     * the dependency might be rejected. Thus, the caller must check the returned value
     * and give up the logging optimization if rejected.
     */
    bool register_write_order_dependency(const generic_page* page, const generic_page* dependency);

    /**
     * Whenever the parent of a page is changed (adoption or de-adoption),
     * this method must be called to switch it in bufferpool.
     * The caller must make sure the page itself, old and new parent pages
     * don't go away while this switch (i.e., latch them).
     */
    void switch_parent (PageID, generic_page*);

    /**
     * Swizzle a child pointer in the parent page to speed-up accesses on the child.
     * @param[in] parent parent of the requested page. this has to be latched (but SH latch is enough).
     * @param[in] slot identifier of the slot to swizzle. 0 is pid0, -1 is foster.
     * If these child pages aren't in bufferpool yet, this method ignores the child.
     * It should be loaded beforehand.
     */
    void swizzle_child (generic_page* parent, general_recordid_t slot);

    /**
     * Swizzle a bunch of child pointers in the parent page to speed-up accesses on them.
     * @param[in] parent parent of the requested page. this has to be latched (but SH latch is enough).
     * @param[in] slots identifiers of the slots to swizzle. 0 is pid0, -1 is foster.
     * If these child pages aren't in bufferpool yet, this method ignores the child.
     * They should be loaded beforehand.
     * @param[in] slots_size length of slots.
     */
    void swizzle_children (generic_page* parent, const general_recordid_t *slots,
                            uint32_t slots_size);

    /**
     * Search in the given page to find the slot that contains the page id as a child.
     * Returns >0 if a normal slot, 0 if pid0, -1 if foster, -2 if not found.
     */
    general_recordid_t find_page_id_slot (generic_page* page, PageID shpid) const;

    /**
     * Returns if the page is swizzled by parent or the volume descriptor.
     * Do NOT call this method without a latch.
     * Also, do not call this function when is_swizzling_enabled() is false.
     * It returns a bogus result in that case (or asserts).
     */
    bool is_swizzled (const generic_page* page) const;

    /** Normalizes the page identifier to a disk page identifier.
      * If the page identifier is a memory frame index (in case of swizzling)
      * then it returns the disk page index, otherwise it returns the page
      * identifier as it is.
      * Do NOT call this method without a latch.
      */
    PageID normalize_shpid(PageID shpid) const;

    /** Immediately writes out all dirty pages in the given volume.*/
    w_rc_t force_volume ();
    /** Immediately writes out all dirty pages.*/
    w_rc_t force_all ();
    /** Wakes up all cleaner threads, starting them if not started yet. */
    w_rc_t wakeup_cleaners ();

    /**
     * Dumps all contents of this bufferpool.
     * this method is solely for debugging. It's slow and unsafe.
     */
    void  debug_dump (std::ostream &o) const;
    /**
     * Dumps the pointers in the given page, accounting for pointer swizzling.
     * this method is solely for debugging. It's slow and unsafe.
     */
    void  debug_dump_page_pointers (std::ostream &o, generic_page *page) const;
    void  debug_dump_pointer (std::ostream &o, PageID shpid) const;

    /**
     * Returns the non-swizzled page-ID for the given pointer that might be swizzled.
     * This is NOT safe against concurrent eviction and should be used just for debugging.
     */
    PageID  debug_get_original_pageid (PageID shpid) const;

    /**
     * Returns if the given page is managed by this bufferpool.
     */
    inline bool  is_bf_page (const generic_page *page) const {
        int32_t idx = page - _buffer;
        return _is_valid_idx(idx);
    }

    /**
     * Get recovery lsn of "count" frames in the buffer pool starting at
     *  index "start". The pids, rec_lsns and page_lsn are returned in "pid",
     *  "rec_lsn" and "page_lsn" arrays, respectively. The values of "start" and "count"
     *  are updated to reflect where the search ended and how many dirty
     *  pages it found, respectively.
     *  'master' and 'current_lsn' are used only for 'in_doubt' pages which are not loaded
    */
    void get_rec_lsn(bf_idx &start, uint32_t &count, PageID *pid, StoreID* store,
                     lsn_t *rec_lsn, lsn_t *page_lsn, lsn_t &min_rec_lsn,
                     const lsn_t master, const lsn_t current_lsn,
                     lsn_t last_mount_lsn);

    /**
     * Returns true if the node has any swizzled pointers to its children.
     * In constrast to the swizzled_ptr_cnt_hint counter, which is just a
     * a hint, this method is accurate as it scans the node * and counts
     * its swizzled pointers. It requires the caller to have the node latched.
     */
    bool has_swizzled_child(bf_idx node_idx);


    /**
     * New eviction algorithm. Sweeps the buffer pool sequentially (like
     * clock), simply evicting every leaf page for which:
     * 1) An EX latch can be acquired conditionally
     * 2) A parent pointer is available and up-to-date
     * 3) The parent can be latched in SH mode conditionally
     * 4) The pin count is zero
     *
     * This is not as good as clock or LRU in terms of hit ratio, but unlike
     * the previous hierarchical algorithm, it is thread-safe. It is also
     * single-threaded, i.e., only one thread evicts at a time.
     */
    w_rc_t evict_blocks(
        uint32_t &evicted_count,
        uint32_t &unswizzled_count,
        evict_urgency_t urgency = EVICT_NORMAL,
        uint32_t preferred_count = 0);


    /**
     * Used during REDO phase in Recovery only
     * The specified page has cb in buffer pool, also registered in hashtable
     * but the actual page is not in buffer pool yet
     * Load the actual page into buffer pool
     */
    w_rc_t load_for_redo(bf_idx idx, PageID shpid);

    size_t get_size() { return _block_cnt; }

private:

    /** fixes a non-swizzled page. */
    w_rc_t _fix_nonswizzled(generic_page* parent, generic_page*& page, PageID shpid,
                               latch_mode_t mode, bool conditional, bool virgin_page,
                               lsn_t emlsn = lsn_t::null);

    /**
     * Given an image of page which might have swizzled pointers,
     * convert it to a disk page without swizzled pointers.
     * Used to write out dirty pages.
     * NOTE this method assumes the swizzled pointers in the page are
     * not being unswizzled concurrently.
     * Take SH latch on this page (not pointed pages) or make sure
     * there aren't such concurrent threads by other means.
     */
    void   _convert_to_disk_page (generic_page* page) const;
    /** if the shpid is a swizzled pointer, convert it to the original page id. */
    void   _convert_to_pageid (PageID* shpid) const;

    /** finds a free block and returns its index. if free list is empty and 'evict' = true, it evicts some page. */
    w_rc_t _grab_free_block(bf_idx& ret, bool evict = true);

    /**
     * evict some number of blocks.
     */
    w_rc_t _get_replacement_block();

    /**
     * try to evict a given block.
     * @return whether evicted the page or not
     */
    bool _try_evict_block(bf_idx parent_idx, bf_idx idx);

    /**
     * Subroutine of _try_evict_block() called after the CAS on pin_cnt.
     * @pre cb.pin_cnt() == -1
     */
    bool _try_evict_block_pinned(bf_tree_cb_t &parent_cb, bf_tree_cb_t &cb,
        bf_idx parent_idx, bf_idx idx);
    /**
     * Subroutine of _try_evict_block_pinned() to update parent's EMLSN.
     * @pre cb.pin_cnt() == -1
     * @pre parent_cb.latch().is_latched()
     */
    bool _try_evict_block_update_emlsn(bf_tree_cb_t &parent_cb, bf_tree_cb_t &cb,
        bf_idx parent_idx, bf_idx idx, general_recordid_t child_slotid);

    /** Adds a free block to the freelist. */
    void   _add_free_block(bf_idx idx);

    /// returns true iff idx is in the valid range.  for assertion.
    bool   _is_valid_idx (bf_idx idx) const;

    /**
     * returns true if idx is in the valid range and also the block is used.  for assertion.
     *
     * @pre hold get_cb(idx).latch() in read or write mode
     */
    bool   _is_active_idx (bf_idx idx) const;

    /** Core implementation of evict_blocks(). */
    w_rc_t _evict_blocks(EvictionContext &context);

    /**
     * Tries to unswizzle the given child page from the parent page.
     * If, for some reason, unswizzling was impossible or troublesome, gives up and returns false.
     * @return whether the child page has been unswizzled
     */
    bool   _unswizzle_a_frame(bf_idx parent_idx, uint32_t child_slot);

    bool   _are_there_many_swizzled_pages() const;

    /**
     * Deletes the given block from this buffer pool. This method must be called when
     *  1. there is no concurrent accesses on the page (thus no latch)
     *  2. the page's _used and _dirty are true
     *  3. the page's _pin_cnt is 0 (so, it must not be swizzled, nor being evicted)
     * Used from the dirty page cleaner to delete a page with "tobedeleted" flag.
     */
    void   _delete_block (bf_idx idx);

    /**
     * Returns if the dependency FROM cb is still active.
     * If it turns out that the dependency is no longer active, it also clears _dependency_xxx to speed up future function call.
     * cb must be pinned.
     */
    bool   _check_dependency_still_active (bf_tree_cb_t &cb);

    bool   _check_dependency_cycle(bf_idx source, bf_idx start_idx);

    bool   _compare_dependency_lsn(const bf_tree_cb_t& cb, const bf_tree_cb_t &dependency_cb) const;

    void   _swizzle_child_pointer(generic_page* parent, PageID* pointer_addr);

    /**
     * \brief System transaction for upadting child EMLSN in parent
     * \ingroup Single-Page-Recovery
     * @param[in,out] parent parent page
     * @param[in] child_slotid slot id of child
     * @param[in] child_emlsn new emlsn to store in parent
     * @pre parent.is_latched()
     * \NOTE parent must be latched, but does not have to be EX-latched.
     * This is because EMLSN are not viewed/updated by multi threads (only accessed during
     * page eviction or cache miss of the particular page).
     */
    w_rc_t _sx_update_child_emlsn(btree_page_h &parent,
                                  general_recordid_t child_slotid, lsn_t child_emlsn);

private:
    /** count of blocks (pages) in this bufferpool. */
    bf_idx               _block_cnt;

    // CS TODO: concurrency???
    bf_idx _root_pages[stnode_page::max];

    /** Array of control blocks. array size is _block_cnt. index 0 is never used (means NULL). */
    bf_tree_cb_t*        _control_blocks;

    /** Array of page contents. array size is _block_cnt. index 0 is never used (means NULL). */
    generic_page*              _buffer;

    /** hashtable to locate a page in this bufferpool. swizzled pages are removed from bufferpool. */
    bf_hashtable<bf_idx_pair>*        _hashtable;

    /**
     * singly-linked freelist. index is same as _buffer/_control_blocks. zero means no link.
     * This logically belongs to _control_blocks, but is an array by itself for efficiency.
     * index 0 is always the head of the list (points to the first free block, or 0 if no free block).
     */
    bf_idx*              _freelist;

    /** count of free blocks. */
    uint32_t _freelist_len;

// Be VERY careful on deadlock to use the following.

    /** spin lock to protect all freelist related stuff. */
    tatas_lock           _freelist_lock;


    bf_idx _eviction_current_frame;

    /**
     * Lock that provides mutual exclusion for the eviction algorithm.
     * Only one thread may perform eviction at a time.
     */
    pthread_mutex_t _eviction_lock;

    // queue_based_lock_t   _eviction_mutex;

    /** the dirty page cleaner. */
    page_cleaner_mgr*    _dcleaner;
    page_cleaner_base*   _cleaner;

    /**
     * Unreliable count of dirty pages in this bufferpool.
     * The value is incremented and decremented without atomic operations.
     * So, this should be only used as statistics.
     */
    int32_t              _dirty_page_count_approximate;
    /**
     * Unreliable count of swizzled pages in this bufferpool.
     * The value is incremented and decremented without atomic operations.
     * So, this should be only used as statistics.
     */
    int32_t              _swizzled_page_count_approximate;

    /** whether to swizzle non-root pages. */
    bool                 _enable_swizzling;
};

/**
 * Holds the buffer slot index of additionally pinned page and
 * releases the pin count when it's destructed.
 * @see bf_tree_m::pin_for_refix(), bf_tree_m::unpin_for_refix(), bf_tree_m::refix_direct().
 */
class pin_for_refix_holder {
public:
    pin_for_refix_holder() : _idx(0) {}
    pin_for_refix_holder(bf_idx idx) : _idx(idx) {}
    pin_for_refix_holder(pin_for_refix_holder &h) {
        steal_ownership(h);
    }
    ~pin_for_refix_holder () {
        if (_idx != 0) {
            release();
        }
    }
    pin_for_refix_holder& operator=(pin_for_refix_holder& h) {
        steal_ownership(h);
        return *this;
    }
    void steal_ownership (pin_for_refix_holder& h) {
        if (_idx != 0) {
            release();
        }
        _idx = h._idx;
        h._idx = 0;
    }

    void set(bf_idx idx) {
        if (_idx != 0) {
            release();
        }
        _idx = idx;
    }
    bf_idx idx() const { return _idx;}
    void release ();

private:
    bf_idx _idx;
};

// Thread that fetches pages into the buffer for warming up.
// Instead of reading a contiguous chunk, it iterates over all
// B-trees so that higher levels are loaded first.
class WarmupThread : public smthread_t {
public:
    WarmupThread() {};
    virtual ~WarmupThread() {}

    virtual void run();
    void fixChildren(btree_page_h& parent, size_t& fixed, size_t max);
};

// tiny macro to help swizzled-LRU and freelist access
#define FREELIST_HEAD _freelist[0]
// #define SWIZZLED_LRU_HEAD _swizzled_lru[0]
// #define SWIZZLED_LRU_TAIL _swizzled_lru[1]
// #define SWIZZLED_LRU_PREV(x) _swizzled_lru[x * 2]
// #define SWIZZLED_LRU_NEXT(x) _swizzled_lru[x * 2 + 1]


#endif // BF_TREE_H
