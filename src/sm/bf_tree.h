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
#include "bf_hashtable.h"
#include "bf_tree_cb.h"
#include <iosfwd>
#include "page_cleaner.h"
#include "page_evictioner.h"
#include "restore.h"
#include <boost/lockfree/queue.hpp>
#include <boost/atomic.hpp>

#include <array>

class sm_options;
class lsn_t;
struct bf_tree_cb_t; // include bf_tree_cb.h in implementation codes

class test_bf_tree;
class test_bf_fixed;
class page_evictioner_base;
class bf_tree_cleaner;
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

/**
* When eviction is triggered, _about_ this number of frames will be evicted at once.
* Given as a ratio of the buffer size (currently 1%)
*/
const float EVICT_BATCH_RATIO = 0.01;

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
    friend class page_evictioner_base;  // for page evictioning
    friend class WarmupThread;
    friend class page_cleaner_decoupled;
    friend class page_evictioner_gclock;
    friend class page_evictioner_car;
    friend class GenericPageIterator;

public:
    /** constructs the buffer pool. */
    bf_tree_m (const sm_options&);

    /** destructs the buffer pool.  */
    ~bf_tree_m ();

    void post_init();

    void shutdown();

    /** returns the total number of blocks in this bufferpool. */
    inline bf_idx get_block_cnt() const {return _block_cnt;}

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

    static bool is_swizzled_pointer (PageID pid) {
        return (pid & SWIZZLED_PID_BIT) != 0;
    }

    // Used for debugging
    bool _is_frame_latched(generic_page* frame, latch_mode_t mode);

    void recover_if_needed(bf_tree_cb_t& cb, generic_page* page, bool only_if_dirty = true);

    /**
     * Fixes a non-root page in the bufferpool. This method receives the parent page and efficiently
     * fixes the page if the pid (pointer) is already swizzled by the parent page.
     * The optimization is transparent for most of the code because the pid stored in the parent
     * page is automatically (and atomically) changed to a swizzled pointer by the bufferpool.
     *
     * @param[out] page         the fixed page.
     * @param[in]  parent       parent of the page to be fixed. has to be already latched. if you can't provide this,
     *                          use fix_direct() though it can't exploit pointer swizzling.
     * @param[in]  vol          volume ID.
     * @param[in]  pid        ID of the page to fix (or bufferpool index when swizzled)
     * @param[in]  mode         latch mode.  has to be SH or EX.
     * @param[in]  conditional  whether the fix is conditional (returns immediately even if failed).
     * @param[in]  only_if_hit  fix is only successful if frame is already on buffer (i.e., hit)
     * @param[in]  virgin_page  whether the page is a new page thus doesn't have to be read from disk.
     *
     * To use this method, you need to include bf_tree_inline.h.
     */
    w_rc_t fix_nonroot (generic_page*& page, generic_page *parent, PageID pid,
                          latch_mode_t mode, bool conditional, bool virgin_page,
                          bool only_if_hit = false, bool do_recovery = true,
                          lsn_t emlsn = lsn_t::null);

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
    void unfix(const generic_page* p, bool evict = false);

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
     * Returns true if the page's _used flag is on
     */
    bool is_used (bf_idx idx) const;

    /**
     * Sets the page_lsn field on the control block. Used by every update
     * operation on a page, including redo.
     */
    void set_page_lsn(generic_page*, lsn_t);
    lsn_t get_page_lsn(generic_page*);

    /***
     * Marks the frame as in possible need of recovery
     * (used for prefetched pages during warmup)
     */
    void set_check_recovery(generic_page*, bool);

    /**
     * Gets the value of the log volume counter in the control block.
     */
    uint32_t get_log_volume(generic_page*);
    void reset_log_volume(generic_page*);
    void increment_log_volume(generic_page*, uint32_t);

    /**
     * Whenever the parent of a page is changed (adoption or de-adoption),
     * this method must be called to switch it in bufferpool.
     * The caller must make sure the page itself, old and new parent pages
     * don't go away while this switch (i.e., latch them).
     */
    void switch_parent (PageID, generic_page*);

    /**
     * Search in the given page to find the slot that contains the page id as a child.
     * Returns >0 if a normal slot, 0 if pid0, -1 if foster, -2 if not found.
     */
    general_recordid_t find_page_id_slot (generic_page* page, PageID pid) const;

    /**
     * Returns if the page is swizzled by parent or the volume descriptor.
     * Do NOT call this method without a latch.
     */
    bool is_swizzled (const generic_page* page) const;

    /** Normalizes the page identifier to a disk page identifier.
      * If the page identifier is a memory frame index (in case of swizzling)
      * then it returns the disk page index, otherwise it returns the page
      * identifier as it is.
      * Do NOT call this method without a latch.
      */
    PageID normalize_pid(PageID pid) const;

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
    void  debug_dump_pointer (std::ostream &o, PageID pid) const;

    /**
     * Returns the non-swizzled page-ID for the given pointer that might be swizzled.
     * This is NOT safe against concurrent eviction and should be used just for debugging.
     */
    PageID  debug_get_original_pageid (PageID pid) const;

    /**
     * Returns if the given page is managed by this bufferpool.
     */
    inline bool  is_bf_page (const generic_page *page) const {
        int32_t idx = page - _buffer;
        return _is_valid_idx(idx);
    }

    /**
     * Returns true if the node has any swizzled pointers to its children.
     * In constrast to the swizzled_ptr_cnt_hint counter, which is just a
     * a hint, this method is accurate as it scans the node * and counts
     * its swizzled pointers. It requires the caller to have the node latched.
     */
    bool has_swizzled_child(bf_idx node_idx);

    size_t get_size() { return _block_cnt; }

    page_cleaner_base* get_cleaner();

    template <typename... Args>
    void wakeup_cleaner(Args... args)
    {
        auto cleaner = get_cleaner();
        if (cleaner) { cleaner->wakeup(args...); }
    }

    bool is_no_db_mode() const { return _no_db_mode; }

    bool is_warmup_done() const { return _warmup_done; }

    bool has_dirty_frames() const;

    void fuzzy_checkpoint(chkpt_t& chkpt) const;

    /**
     * Tries to unswizzle the given child page from the parent page.  If, for
     * some reason, unswizzling was impossible or troublesome, gives up and
     * returns false
     *
     * @pre parent is latched in any mode; child is latched in EX mode (if apply=true)
     * @return whether the child page has been unswizzled
     *
     * @param[out] pid_ret Unswizzled PageID is returned in pid_ret (if not null)
     * @param[in] apply If apply == true, pointer is actually unswizzled
     * in parent; otherwise just return what the unswizzled pointer would be
     * (i.e., the ret_pid)
     */
    bool unswizzle(generic_page* parent, general_recordid_t child_slot, bool apply = true,
            PageID* ret_pid = nullptr);

    // Used for debugging
    void print_page(PageID pid);

private:

    /** fixes a non-swizzled page. */
    w_rc_t fix(generic_page* parent, generic_page*& page, PageID pid,
                               latch_mode_t mode, bool conditional, bool virgin_page,
                               bool only_if_hit = false, bool do_recovery = true,
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

    /** finds a free block and returns its index. if free list is empty and 'evict' = true, it evicts some page. */
    w_rc_t _grab_free_block(bf_idx& ret);

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

    /**
     * Deletes the given block from this buffer pool. This method must be called when
     *  1. there is no concurrent accesses on the page (thus no latch)
     *  2. the page's _used is true
     *  3. the page's _pin_cnt is 0 (so, it must not be swizzled, nor being evicted)
     * Used from the dirty page cleaner to delete a page with "tobedeleted" flag.
     */
    void   _delete_block (bf_idx idx);

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

    /**
     * Check if the buffer pool is warmed up by looking at the hit ratio
     * observed for the current thread. This is for cases where the dataset
     * fits in main memory and the buffer pool never (or just takes really
     * long to) fill up.
     */
    void check_warmup_done();

    /// Buffer is considered warm when hit ratio goes above this
    static constexpr float WARMUP_HIT_RATIO = 0.98;

    void set_warmup_done();

private:
    /** count of blocks (pages) in this bufferpool. */
    bf_idx               _block_cnt;

    // CS TODO: concurrency???
    std::array<bf_idx, stnode_page::max> _root_pages;

    /** Array of control blocks. array size is _block_cnt. index 0 is never used (means NULL). */
    bf_tree_cb_t*        _control_blocks;

    /** Array of page contents. array size is _block_cnt. index 0 is never used (means NULL). */
    generic_page*              _buffer;

    /** hashtable to locate a page in this bufferpool. swizzled pages are removed from bufferpool. */
    bf_hashtable<bf_idx_pair>*        _hashtable;
    
    boost::lockfree::queue<bf_idx>*     _freelist;
    
    mutable boost::atomic<int>          _approx_freelist_length;

// Be VERY careful on deadlock to use the following.

    /** the dirty page cleaner. */
    page_cleaner_base*   _cleaner;

    /** worker thread responsible for evicting pages. */
    page_evictioner_base* _evictioner;

    /** whether to swizzle non-root pages. */
    bool                 _enable_swizzling;

    /** whether to update emlsn on parent upon eviction */
    bool                 _maintain_emlsn;

    bool _write_elision;

    bool _cleaner_decoupled;

    bool _no_db_mode;

    bool _batch_warmup;
    size_t _batch_segment_size;
    bool _warmup_done;

    static thread_local unsigned _fix_cnt;
    static thread_local unsigned _hit_cnt;

    using RestoreCoord = RestoreCoordinator<
        std::function<decltype(SegmentRestorer::bf_restore)>>;
    std::shared_ptr<RestoreCoord> _restore_coord;
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
class WarmupThread : public thread_wrapper_t {
public:
    WarmupThread() {};
    virtual ~WarmupThread() {}

    virtual void run();
    void fixChildren(btree_page_h& parent, size_t& fixed, size_t max);
};

class GenericPageIterator
{
public:
    generic_page* operator*()
    {
        if (!_current || _current_pid >= _first + _count) { return nullptr; }
        return _current;
    }

    GenericPageIterator& operator++()
    {
        if (!_current) { return *this; }

        unfix_current();

        _current_pid++;
        if (_current_pid >= _first + _count) { return *this; }

        fix_current();
        return *this;
    }

    /// This essentially yields an "end" iterator
    GenericPageIterator()
        : _first(0), _count(0), _virgin(false), _current(nullptr),
        _current_pid(0), fix_depth(0)
    {
    }

    /// This essentially yields a "begin" iterator
    GenericPageIterator(PageID first, PageID count, bool virgin)
        : _first(first), _count(count), _virgin(virgin), _current(nullptr),
        fix_depth(0)
    {
        if (count > 0) {
            _current_pid = _first;
            fix_current();
        }
    }

    GenericPageIterator(const GenericPageIterator& other)
        : _first(other._first), _count(other._count),
        _virgin(other._virgin), _current(nullptr), fix_depth(0)
    {
        if (_count > 0) {
            _current_pid = _first;
            fix_current();
        }
    }

    ~GenericPageIterator()
    {
        unfix_current();
        w_assert1(fix_depth == 0);
    }

    GenericPageIterator& operator=(GenericPageIterator other)
    {
        swap(*this, other);
        return *this;
    }

    friend void swap(GenericPageIterator& a, GenericPageIterator& b)
    {
        swap(a._first, b._first);
        swap(a._count, b._count);
        swap(a._virgin, b._virgin);
        swap(a._current, b._current);
        swap(a.fix_depth, b.fix_depth);
    }

    bool operator==(GenericPageIterator& other)
    {
        if (!_current && !other._current) { return true; }
        return other._current_pid == _current_pid && _current && other._current;
    }

    bool operator!=(GenericPageIterator& other)
    {
        return !(*this == other);
    }

private:
    PageID _first;
    PageID _count;
    bool _virgin;

    generic_page* _current;
    PageID _current_pid;

    int fix_depth;

    void fix_current()
    {
        w_assert1(fix_depth == 0);
        constexpr bool conditional = false;
        constexpr bool do_recovery = false;
        constexpr bool only_if_hit = false;
        W_COERCE(smlevel_0::bf->fix(nullptr, _current, _current_pid, LATCH_EX,
                conditional, _virgin, only_if_hit, do_recovery));
        fix_depth++;
    }

    void unfix_current()
    {
        if (_current) {
            w_assert1(fix_depth == 1);
            smlevel_0::bf->unfix(_current);
            _current = nullptr;
            fix_depth--;
        }
    }
};

// tiny macro to help swizzled-LRU and freelist access
// #define FREELIST_HEAD _freelist[0]
// #define SWIZZLED_LRU_HEAD _swizzled_lru[0]
// #define SWIZZLED_LRU_TAIL _swizzled_lru[1]
// #define SWIZZLED_LRU_PREV(x) _swizzled_lru[x * 2]
// #define SWIZZLED_LRU_NEXT(x) _swizzled_lru[x * 2 + 1]


#endif // BF_TREE_H
