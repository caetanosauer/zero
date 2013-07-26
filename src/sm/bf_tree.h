/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef BF_TREE_H
#define BF_TREE_H

#include "w_defines.h"
#include "vid_t.h"
#include "latch.h"
#include "tatas.h"
#include "sm_s.h"
#include "page_s.h"
#include "bf_idx.h"
#include <iosfwd>

class vol_t;
class lsn_t;
struct bf_tree_cb_t; // include bf_tree_cb.h in implementation codes
struct bf_tree_vol_t; // include bf_tree_vol.h in implementation codes
class bf_hashtable; // include bf_hashtable.h in implementation codes

class test_bf_tree;
class test_bf_fixed;
class bf_tree_cleaner;
class bf_tree_cleaner_slave_thread_t;

/** a swizzled pointer (page ID) has this bit ON. */
const uint32_t SWIZZLED_PID_BIT = 0x80000000;
inline bool is_swizzled_pointer (shpid_t shpid) {
    return (shpid & SWIZZLED_PID_BIT) != 0;
}

inline uint64_t bf_key(volid_t vid, shpid_t shpid) {
    return ((uint64_t) vid << 32) + shpid;
}
inline uint64_t bf_key(const lpid_t &pid) {
    return bf_key(pid.vol().vol, pid.page);
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

// A flag whether the bufferpool maintains a parent pointer in each buffer frame.
// Maintaining a parent pointer is tricky especially when several pages change
// their parent at the same time (e.g., page split/merge/rebalance in a node page).
// Without this flag, we simply don't have parent pointer, thus do a hierarchical
// walking to find a pge to evict.
// #define BP_MAINTAIN_PARNET_PTR

// A flag whether the bufferpool maintains per-frame latches in a separate table from 
// per-frame control blocks. 
//#define BP_MAINTAIN_SEPARATE_LATCH_TABLE

// A flag whether the bufferpool maintains replacement priority per page.
#define BP_MAINTAIN_REPLACEMENT_PRIORITY

#ifndef PAUSE_SWIZZLING_ON
const bool _bf_pause_swizzling = false; // compiler will strip this out from if clauses. so, no overhead.
#endif // PAUSE_SWIZZLING_ON    

/**
 * recursive clockhand's maximum depth used while choosing a page to unswizzle.
 * To make this depth reasonably short, we never swizzle foster-child pointer.
 * Foster relationship is tentative and short-lived, so it doesn't need the performance boost in BP.
 */
const uint16_t MAX_SWIZZLE_CLOCKHAND_DEPTH = 10;

/**
 * When unswizzling is triggered, _about_ this number of frames will be unswizzled at once.
 * The smaller this number, the more frequent you need to trigger unswizzling.
 */
const uint32_t UNSWIZZLE_BATCH_SIZE = 1000;

/**\enum replacement_policy_t 
 * \brief Page replacement policy.
 */
enum replacement_policy_t { 
    POLICY_CLOCK = 0,
    POLICY_CLOCK_PRIORITY,
    POLICY_RANDOM
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

public:
    void print_slots(page_s* page) const;
#ifdef PAUSE_SWIZZLING_ON
    static bool _bf_pause_swizzling; // this can be turned on/off from any place. ugly, but it's just for an experiment.
    static uint64_t _bf_swizzle_ex; // approximate statistics. how many times ex-latch were taken on page swizzling
    static uint64_t _bf_swizzle_ex_fails; // approximate statistics. how many times ex-latch upgrade failed on page swizzling
#endif // PAUSE_SWIZZLING_ON    

    /** constructs the buffer pool. */
    bf_tree_m (bf_idx block_cnt, uint32_t cleaner_threads = 1,
               uint32_t cleaner_interval_millisec_min       =   1000,
               uint32_t cleaner_interval_millisec_max       = 256000,
               uint32_t cleaner_write_buffer_pages          =     64,
               const char* replacement_policy = "clock",
               bool initially_enable_cleaners = true,
               bool enable_swizzling = false
              );
    /** destructs the buffer pool.  */
    ~bf_tree_m ();
    
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
    w_rc_t init ();
    /** does additional clean-up that might return error codes (thus can't be done in destructor). */
    w_rc_t destroy ();

    /** returns the control block corresponding to the given bufferpool page. mainly for debugging. */
    bf_tree_cb_t* get_cb(const page_s *page);
    /** returns the bufferpool page corresponding to the given control block. mainly for debugging. */
    page_s* get_page(const bf_tree_cb_t *cb);
    /** returns the page ID of the root page (which is already loaded in this bufferpool) in given store. mainly for debugging or approximate purpose. */
    shpid_t get_root_page_id(volid_t vol, snum_t store);

    /**
     * Fixes a non-root page in the bufferpool. This method receives the parent page and efficiently
     * fixes the page if the shpid (pointer) is already swizzled by the parent page.
     * The optimization is transparent for most of the code because the shpid stored in the parent
     * page is automatically (and atomically) changed to a swizzled pointer by the bufferpool.
     * @param[out] page the fixed page.
     * @param[in] parent parent of the page to be fixed. has to be already latched. if you can't provide this,
     * use fix_direct() though it can't exploit pointer swizzling.
     * @param[in] vol volume ID.
     * @param[in] shpid ID of the page to fix (or bufferpool index when swizzled)
     * @param[in] mode latch mode. has to be SH or EX.
     * @param[in] conditional whether the fix is conditional (returns immediately even if failed).
     * @param[in] virgin_page whether the page is a new page thus doesn't have to be read from disk.
     * To use this method, you need to include bf_tree_inline.h.
     */
    w_rc_t fix_nonroot (page_s*& page, page_s *parent, volid_t vol, shpid_t shpid, latch_mode_t mode, bool conditional, bool virgin_page);

    /**
     * Fixes any page (root or non-root) in the bufferpool without pointer swizzling.
     * In some places, we need to fix a page without fixing the parent, e.g., recovery or re-fix in cursor.
     * For such code, this method allows fixing without parent. However, this method can be used only when
     * the pointer swizzling is off. The reason is as follows.
     * If the requested page is swizzled (shpid is a swizzled pointer) and the parent page is not fixed,
     * the page might be already evicted and placed in other slots. Hence, this method rejects a swizzled pointer.
     * To prevent errors caused by this, consider following:
     *  1. Disable pointer swizzling  (see bf_tree_m::is_swizzling_enabled()) while you need to call this method.
     * This will be the choice for REDO in restart_m.
     *  2. If you are to 're-fix' a page such as in cursor, use pin_for_refix()/refix_direct()/unpin_for_refix().
     * @param[out] page the fixed page.
     * @param[in] vol volume ID.
     * @param[in] shpid ID of the page to fix. If the shpid looks like a swizzled pointer, this method returns an error eBF_DIRECTFIX_SWIZZLED_PTR (see above).
     * @param[in] mode latch mode. has to be SH or EX.
     * @param[in] conditional whether the fix is conditional (returns immediately even if failed).
     * @param[in] virgin_page whether the page is a new page thus doesn't have to be read from disk.
     */
    w_rc_t fix_direct (page_s*& page, volid_t vol, shpid_t shpid, latch_mode_t mode, bool conditional, bool virgin_page);

    /**
     * Adds an additional pin count for the given page (which must be already latched).
     * This is used to re-fix the page later without parent pointer. See fix_direct() why we need this feature.
     * Never forget to call a corresponding unpin_for_refix() for this page. Otherwise, the page will be in the bufferpool forever.
     * @param[in] page the page that is currently latched and will be re-fixed later.
     * @return slot index of the page in this bufferpool. Use this value to the subsequent refix_direct() and unpin_for_refix() call.
     */
    bf_idx pin_for_refix(const page_s* page);
    
    /**
     * Removes the additional pin count added by pin_for_refix().
     */
    void unpin_for_refix(bf_idx idx);

    /**
     * Fixes a page with the already known slot index, assuming the slot has at least one pin count.
     * Used with pin_for_refix() and unpin_for_refix().
     */
    w_rc_t refix_direct (page_s*& page, bf_idx idx, latch_mode_t mode, bool conditional);

    /**
     * Fixes a new (virgin) root page for a new store with the specified page ID.
     * Implicitly, the latch will be EX and non-conditional.
     * To use this method, you need to include bf_tree_inline.h.
     */
    w_rc_t fix_virgin_root (page_s*& page, volid_t vol, snum_t store, shpid_t shpid);

    /**
     * Fixes an existing (not virgin) root page for the given store.
     * This method doesn't receive page ID because it's already known by bufferpool.
     * To use this method, you need to include bf_tree_inline.h.
     */
    w_rc_t fix_root (page_s*& page, volid_t vol, snum_t store, latch_mode_t mode, bool conditional);
    
    /** returns the current latch mode of the page. */
    latch_mode_t latch_mode(const page_s* p);

    /**
     * upgrade SH-latch on the given page to EX-latch.
     * This method is always conditional, immediately returning if there is a conflicting latch.
     * Returns if successfully upgraded.
     */
    bool upgrade_latch_conditional(const page_s* p);

    /** downgrade EX-latch on the given page to SH-latch. */
    void downgrade_latch(const page_s* p);

    /**
     * Release the latch on the page.
     */
    void unfix(const page_s* p);

    /**
     * Mark the page as dirty.
     */
    void set_dirty(const page_s* p);
    /**
     * Returns if the page is already marked dirty.
     */
    bool is_dirty(const page_s* p) const;

    /**
     * Adds a write-order dependency such that one is always written out after another.
     * @param[in] page the page to be written later. must be latched.
     * @param[in] dependency the page to be written first. must be latched.
     * @return whether the dependency is successfully registered. for a number of reasons,
     * the dependency might be rejected. Thus, the caller must check the returned value
     * and give up the logging optimization if rejected.
     */
    bool register_write_order_dependency(const page_s* page, const page_s* dependency);

    /**
     * Creates a volume descriptor for the given volume and install it
     * to this bufferpool. Called when a volume is mounted.
     * Because mounting a volume is protected by mutextes, this method is indirectly protected too.
     */
    w_rc_t install_volume (vol_t* volume);
    /**
     * Removes the volume descriptor for the given volume.
     * Called when a volume is unmounted.
     * Like install_volume(), this method is indirectly protected too.
     */
    w_rc_t uninstall_volume (volid_t vid);

#ifdef BP_MAINTAIN_PARNET_PTR
    /**
     * Whenever the parent of a page is changed (adoption or de-adoption),
     * this method must be called to switch it in bufferpool.
     * The caller must make sure the page itself, old and new parent pages
     * don't go away while this switch (i.e., latch them).
     */
    void switch_parent (page_s* page, page_s* new_parent);
#endif // BP_MAINTAIN_PARNET_PTR
    
    /**
     * Swizzle a child pointer in the parent page to speed-up accesses on the child.
     * @param[in] parent parent of the requested page. this has to be latched (but SH latch is enough).
     * @param[in] slot identifier of the slot to swizzle. 0 is pid0, -1 is foster.
     * If these child pages aren't in bufferpool yet, this method ignores the child.
     * It should be loaded beforehand.
     */
    void swizzle_child (page_s* parent, slotid_t slot);

    /**
     * Swizzle a bunch of child pointers in the parent page to speed-up accesses on them.
     * @param[in] parent parent of the requested page. this has to be latched (but SH latch is enough).
     * @param[in] slots identifiers of the slots to swizzle. 0 is pid0, -1 is foster.
     * If these child pages aren't in bufferpool yet, this method ignores the child.
     * They should be loaded beforehand.
     * @param[in] slots_size length of slots.
     */
    void swizzle_children (page_s* parent, const slotid_t *slots, uint32_t slots_size);

    /**
     * Search in the given page to find the slot that contains the page id as a child.
     * Returns >0 if a normal slot, 0 if pid0, -1 if foster, -2 if not found.
     */
    slotid_t find_page_id_slot (page_s* page, shpid_t shpid) const;

    /**
     * Returns if the page is swizzled by parent or the volume descriptor.
     * Do NOT call this method without a latch.
     * Also, do not call this function when is_swizzling_enabled() is false.
     * It returns a bogus result in that case (or asserts).
     */
    bool is_swizzled (const page_s* page) const;
    
    /** Normalizes the page identifier to a disk page identifier. 
      * If the page identifier is a memory frame index (in case of swizzling) 
      * then it returns the disk page index, otherwise it returns the page 
      * identifier as it is. 
      * Do NOT call this method without a latch.
      */
    shpid_t normalize_shpid(shpid_t shpid) const;

    /** Immediately writes out all dirty pages in the given volume.*/
    w_rc_t force_volume (volid_t vol);
    /** Immediately writes out all dirty pages.*/
    w_rc_t force_all ();
    /** Immediately writes out all dirty pages up to the given LSN. used while checkpointing. */
    w_rc_t force_until_lsn (lsndata_t lsn);
    /** Immediately writes out all dirty pages up to the given LSN. used while checkpointing. */
    w_rc_t force_until_lsn (const lsn_t &lsn) {
        return force_until_lsn(lsn.data());
    }
    /** Wakes up all cleaner threads, starting them if not started yet. */
    w_rc_t wakeup_cleaners ();
    /** Wakes up the cleaner thread assigned to the given volume. */
    w_rc_t wakeup_cleaner_for_volume (volid_t vol);

    /**
     * Dumps all contents of this bufferpool.
     * this method is solely for debugging. It's slow and unsafe.
     */
    void  debug_dump (std::ostream &o) const;
    /**
     * Dumps the pointers in the given page, accounting for pointer swizzling.
     * this method is solely for debugging. It's slow and unsafe.
     */
    void  debug_dump_page_pointers (std::ostream &o, page_s *page) const;
    void  debug_dump_pointer (std::ostream &o, shpid_t shpid) const;

    /**
     * Returns the non-swizzled page-ID for the given pointer that might be swizzled.
     * This is NOT safe against concurrent eviction and should be used just for debugging.
     */
    shpid_t  debug_get_original_pageid (shpid_t shpid) const;

    /**
     * Returns if the given page is managed by this bufferpool.
     */
    inline bool  is_bf_page (const page_s *page) const {
        int32_t idx = page - _buffer;
        return idx > 0 && idx < (int32_t) _block_cnt;
    }

    /**
     * Get recovery lsn of "count" frames in the buffer pool starting at
     *  index "start". The pids and rec_lsns are returned in "pid" and
     *  "rec_lsn" arrays, respectively. The values of "start" and "count"
     *  are updated to reflect where the search ended and how many dirty
     *  pages it found, respectively.
    */
    void get_rec_lsn(bf_idx &start, uint32_t &count, lpid_t *pid, lsn_t *rec_lsn, lsn_t &min_rec_lsn);
    
    /**
    *  Ensures the buffer pool's rec_lsn for this page is no larger than
    *  the page lsn. If not repaired, the faulty rec_lsn can lead to
    *  recovery errors. Invalid rec_lsn can occur when
    *  1) there are unlogged updates to a page -- log redo, for instance, or updates to a tmp page.
    *  2) when a clean page is fixed in EX mode but an update is never
    *  made and the page is unfixed.  Now the rec_lsn reflects the tail
    *  end of the log but the lsn on the page reflects something earlier.
    *  At least in this case, we should expect the bfcb_t to say the
    *  page isn't dirty.
    *  3) when a st_tmp page is fixed in EX mode and an update is 
    *  made and the page is unfixed.  Now the rec_lsn reflects the tail
    *  end of the log but the lsn on the page reflects something earlier.
    *  In this case, the page IS dirty.
    *
    *  FRJ: I don't see any evidence that this function is actually
    *  called because of (3) above...
    * 
    * NOTE call this method only while REDO!
    */
    void repair_rec_lsn (page_s *page, bool was_dirty, const lsn_t &new_rlsn);

private:

    /** called when a volume is mounted. */
    w_rc_t _preload_root_page (bf_tree_vol_t* desc, vol_t* volume, snum_t store, shpid_t shpid, bf_idx idx);

    /** fixes a non-swizzled page. */
    w_rc_t _fix_nonswizzled(page_s* parent, page_s*& page, volid_t vol, shpid_t shpid, latch_mode_t mode, bool conditional, bool virgin_page);
    
    /** used by fix_root and fix_virgin_root. */
    w_rc_t _latch_root_page(page_s*& page, bf_idx idx, latch_mode_t mode, bool conditional);

    /**
     * Given an image of page which might have swizzled pointers,
     * convert it to a disk page without swizzled pointers.
     * Used to write out dirty pages.
     * NOTE this method assumes the swizzled pointers in the page are
     * not being unswizzled concurrently.
     * Take SH latch on this page (not pointed pages) or make sure
     * there aren't such concurrent threads by other means.
     */
    void   _convert_to_disk_page (page_s* page) const;
    /** if the shpid is a swizzled pointer, convert it to the original page id. */
    void   _convert_to_pageid (shpid_t* shpid) const;
    
#ifdef BP_MAINTAIN_PARNET_PTR
    /**
     * Newly adds the speficied block to the head of swizzled-pages LRU.
     * This page must be a swizzled page that can be unswizzled.
     */
    void   _add_to_swizzled_lru (bf_idx idx);
    /**
     * Eliminates the speficied block from swizzled-pages LRU.
     */
    void   _remove_from_swizzled_lru (bf_idx idx);
    /** Tells if the block is contained in swizzled-pages LRU. */
    bool   _is_in_swizzled_lru (bf_idx idx) const;

    /**
     * Brings the speficied block to the head of swizzled-pages LRU, assuming the block is already on the LRU.
     * This is costly because of locks, so call this only once in 100 or 1000 times.
     * If the page is worth swizzling, even once in 100 would be very frequent.
     */
    void   _update_swizzled_lru (bf_idx idx);
#endif // BP_MAINTAIN_PARNET_PTR

    /** finds a free block and returns its index. if free list is empty, it evicts some page. */
    w_rc_t _grab_free_block(bf_idx& ret);
    
    /**
     * evict a block and get its exclusive ownership.
     */
    w_rc_t _get_replacement_block(bf_idx& ret);

    /**
     * evict a block using a CLOCK replacement policy and get its exclusive ownership.
     */
    w_rc_t _get_replacement_block_clock(bf_idx& ret, bool use_priority);

    /**
     * evict a block using a RANDOM replacement policy and get its exclusive ownership.
     */
    w_rc_t _get_replacement_block_random(bf_idx& ret);

    /** try to evict a given block. */
    int _try_evict_block(bf_idx idx);

    /** Adds a free block to the freelist. */
    void   _add_free_block(bf_idx idx);
    
    /** returns if the idx is in the valid range and also the block is used. for assertion. */
    bool   _is_active_idx (bf_idx idx) const;

    /**
     * \brief Looks for some buffered frames that are swizzled and can be unswizzled, then
     * unswizzles UNSWIZZLE_BATCH_SIZE frames to make room for eviction.
     * \details
     * Below are the contracts of this method. In short, it's supposed to be very safe but not that precise.
     * 
     * 1. Restarts tree traversal from the clockhand place to make sure it's _LARGELY_ fair.
     *   It might have some hiccups like skipping or double-visiting a few pages, but it's not that often.
     * 2. It's very highly likely that this method unswizzles at least a bunch of frames.
     *   But, there is a slight chance that you need to call this again to make room for eviction.
     * 3. Absolutely no deadlocks nor waits. If this method finds that a frame is being write-locked
     *   by someone, it simply skips that frame. All locks by this method is conditional and for very short-time.
     * 4. Absolutely multi-thread safe. Again, it might have some hiccups on fairness because of
     *   multi-threading, but concurrent threads calling this method at the same time will cause no severe problems.
     * 5. Lastly, this method isn't supposed to be super-fast (if it has to be, something about unswizzling policy is wrong..),
     *   but should be okay to call it for every millisec or so.
     * 
     * @param[in] urgent whether this method thoroughly traverses even if there seems few swizzled pages.
     * @see UNSWIZZLE_BATCH_SIZE
     */
    void   _trigger_unswizzling(bool urgent = false);

    void   _dump_swizzle_clockhand() const;

    /** called from _trigger_unswizzling(). traverses the given volume. */
    void   _unswizzle_traverse_volume(uint32_t &unswizzled_frames, volid_t vol);
    /** called from _trigger_unswizzling(). traverses the given store. */
    void   _unswizzle_traverse_store(uint32_t &unswizzled_frames, volid_t vol, snum_t store);
    /** called from _trigger_unswizzling(). traverses the given interemediate node. */
    void   _unswizzle_traverse_node(uint32_t &unswizzled_frames, volid_t vol, snum_t store, bf_idx node_idx, uint16_t cur_clockhand_depth);

#ifdef BP_MAINTAIN_PARNET_PTR
    /** implementation of _trigger_unswizzling assuming parent pointer in each bufferpool frame. */
    void   _unswizzle_with_parent_pointer();
#endif // BP_MAINTAIN_PARNET_PTR

    /**
     * Tries to unswizzle the given child page from the parent page.
     * If, for some reason, unswizzling was impossible or troublesome, gives up and returns false.
     * @return whether the child page has been unswizzled
     */
    bool   _unswizzle_a_frame(bf_idx parent_idx, uint32_t child_slot);

    bool   _are_there_many_swizzled_pages() const;
    
    /**
     * Increases the pin_cnt of the given block and makes sure the block is not being evicted or invalid.
     * This method assumes no knowledge of the current state of the block, so this might be costly.
     * Also, this might fail (the block is being evicted or invalid), in which case this returns false.
     */
    bool   _increment_pin_cnt_no_assumption (bf_idx idx);
    
    /**
     * Releases one pin. This method must be used only for decreasing pin_cnt from n+1 to n where n>=0.
     * In other words, setting -1 to pin_cnt must not happen in this method.
     * Assuming it, this function is just an atomic_dec().
     */
    void   _decrement_pin_cnt_assume_positive (bf_idx idx);
    
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

    void   _swizzle_child_pointer(page_s* parent, shpid_t* pointer_addr);

    // these are used only in the mainmemory-db experiment
    w_rc_t _install_volume_mainmemorydb(vol_t* volume);
    w_rc_t _fix_nonswizzled_mainmemorydb(page_s* parent, page_s*& page, shpid_t shpid, latch_mode_t mode, bool conditional, bool virgin_page);


private:
    /** count of blocks (pages) in this bufferpool. */
    bf_idx               _block_cnt;
    
    /** current clock hand for eviction. */
    bf_idx volatile      _clock_hand;
    
    /**
     * Array of pointers to root page descriptors of all currently mounted volumes.
     * The array index is volume ID.
     * 
     * All pointers are initially NULL.
     * When a volume is mounted, a bf_tree_vol_t is instantiated
     * in this array. When the volume is unmounted, the object is revoked
     * and the pointer is reset to NULL.
     * 
     * Because there is no race condition in loading a volume,
     * this array does not have to be protected by mutex or spinlocks.
     */
    bf_tree_vol_t*       _volumes[MAX_VOL_COUNT];
    
    /** Array of control blocks. array size is _block_cnt. index 0 is never used (means NULL). */
    bf_tree_cb_t*        _control_blocks;

    /** Array of page contents. array size is _block_cnt. index 0 is never used (means NULL). */
    page_s*              _buffer;
    
    /** hashtable to locate a page in this bufferpool. swizzled pages are removed from bufferpool. */
    bf_hashtable*        _hashtable;
    
    /**
     * singly-linked freelist. index is same as _buffer/_control_blocks. zero means no link.
     * This logically belongs to _control_blocks, but is an array by itself for efficiency.
     * index 0 is always the head of the list (points to the first free block, or 0 if no free block).
     */
    bf_idx*              _freelist;

    /** count of free blocks. */
    uint32_t volatile    _freelist_len;

// Be VERY careful on deadlock to use the following.
    
    /** spin lock to protect all freelist related stuff. */
    tatas_lock           _freelist_lock;

#ifdef BP_MAINTAIN_PARNET_PTR
    /**
     * doubly-linked LRU list for swizzled pages THAT CAN BE UNSWIZZLED.
     * (in other words, root pages aren't included in this counter and the linked list)
     * The array size is 2 * _block_cnt. [block i's prev][block i's next][block i+1's prev]...
     * This logically belongs to _control_blocks, but is an array by itself for efficiency.
     * [0] and [1] are special, meaning list head and tail.
     */
    bf_idx*              _swizzled_lru;
    /** count of swizzled pages that can be unswizzled. */
    uint32_t volatile    _swizzled_lru_len;
    /** spin lock to protect swizzled page LRU list. */
    tatas_lock           _swizzled_lru_lock;
#endif // BP_MAINTAIN_PARNET_PTR

    /**
     * Hierarchical clockhand to maintain where we lastly visited
     * to choose a victim page for unswizzling.
     * The first element is the volume to check,
     * the second element is the store to check,
     * the third element is the ordinal in the root page (0=first child,1=second child),...
     * 
     * We need this hierarchical clockhand because we must
     * go _back_ to the parent page to unswizzle the child.
     * The swizzled pointer is stored in the parent page, we need to
     * modify it back to a usual page ID. The hierarchical clockhand
     * tells what is the parent page without parent pointers.
     * 
     * Like the usual clockhand, this is an imprecise data structure
     * that doesn't use locks or other heavy-weight methods to maintain.
     *   If we skip over some node, that's fine. We will visit it later anyways.
     *   If we visit some node twice, that's fine. Not much overhead.
     *   If any other inconsistency occured, that's fine. We just restart the search.
     * We are just going to find a few pages to unswizzle, nothing more.
     */
    uint32_t             _swizzle_clockhand_pathway[MAX_SWIZZLE_CLOCKHAND_DEPTH];
    /** the lastly visited element in _swizzle_clockhand_pathway that might have some remaining descendants to visit. */
    uint16_t             _swizzle_clockhand_current_depth;

    /** threshold temperature above which to not unswizzle a frame */
    uint32_t             _swizzle_clockhand_threshold;

    // queue_based_lock_t   _eviction_mutex;
    
    /** the dirty page cleaner. */
    bf_tree_cleaner*     _cleaner;
    
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
    
    /** page replacement policy */
    replacement_policy_t _replacement_policy;
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

// tiny macro to help swizzled-LRU and freelist access
#define FREELIST_HEAD _freelist[0]
#define SWIZZLED_LRU_HEAD _swizzled_lru[0]
#define SWIZZLED_LRU_TAIL _swizzled_lru[1]
#define SWIZZLED_LRU_PREV(x) _swizzled_lru[x * 2]
#define SWIZZLED_LRU_NEXT(x) _swizzled_lru[x * 2 + 1]


#endif // BF_TREE_H
