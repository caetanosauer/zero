#ifndef BF_TREE_H
#define BF_TREE_H

#include "w_defines.h"
#include "vid_t.h"
#include "latch.h"
#include "tatas.h"
#include "sm_s.h"
#include "bf_idx.h"

class page_s;
class vol_t;
class lsn_t;
struct bf_tree_cb_t; // include bf_tree_cb.h in implementation codes
struct bf_tree_vol_t; // include bf_tree_vol.h in implementation codes
class bf_hashtable; // include bf_hashtable.h in implementation codes

class test_bf_tree;
class test_bf_fixed;

/**
 * \Brief The new buffer manager that exploits the tree structure of indexes.
 * \ingroup SSMBUFPOOL
 * \Details
 * This is the new buffer manager in Foster B-tree which only deals with
 * tree-structured stores such as B-trees.
 * This class and bf_fixed_m effectively replace the old bf_core_m.
 * 
 * \section{Pointer-Swizzling}
 * TODO explain
 * 
 * \section{Hierarchical-Bufferpool}
 * This bufferpool assumes hierarchical data dastucture like B-trees.
 * fix() receives the already-latched parent pointer and uses it to find
 * the requested page. Especially when the pointer to the child is swizzled,
 * 
 * 
 * For more details of the design of this new buffer manager, see bufferpool_design.docx.
 */
class bf_tree_m {
    friend class test_bf_tree; // for testcases
    friend class test_bf_fixed; // for testcases
public:
    bf_tree_m ();
    ~bf_tree_m ();

    /** constructs the buffer pool. the ctor does nothing because initialization must return an error code. */
    w_rc_t init (bf_idx block_cnt);
    /** destructs the buffer pool. the dtor does nothing because release must return an error code.  */
    w_rc_t release ();


    /**
     * Take a latch on the page of the given page ID and returns it, reading from disk if not currently in the bufferpool.
     * @param[in] parent parent of the requested page. NULL if reading the root page. Must be latched.
     * @param[out] page the latched page
     * @param[in] pid page ID
     * @param[in] mode the latch to take
     * @param[in] conditional if true, take a latch on the page of the given page ID and returns it only if would not block,
     * immediately returning an error if it can't take the latch immediately.
     * @param[in] virgin_page if true, we don't read the page content from disk even if not exists in the bufferpool yet.
     * Also, such a non-initialized virgin page doesn't have stnum. This method sets it (so, don't forget to set stnum in pid).
     */
    w_rc_t fix(page_s* parent, page_s*& page, const lpid_t& pid, latch_mode_t mode, bool conditional, bool virgin_page);

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
     * @param[in] dirty if true, we mark the page as dirty and do whatever we need to do for durability.
     */
    void unfix(const page_s* p, bool dirty = false);

    /**
     * Adds a write-order dependency such that one is always written out after another.
     * @param[in] page the page to be written later. must be latched.
     * @param[in] dependency the page to be written first. must be latched.
     * @return whether the dependency is successfully registered. for a number of reasons,
     * the dependency might be rejected. Thus, the caller must check the returned value
     * and give up the logging optimization if rejected.
     */
    bool register_write_order_dependency(const page_s* page, const page_s* dependency);

private:
    /** constructs a volume descriptor. */
    w_rc_t _construct_tree_vol (bf_tree_vol_t* &ret, vol_t* volume);
    /** destructs a volume descriptor. */
    w_rc_t _destruct_tree_vol (bf_tree_vol_t* vol);

    /** fixes a non-swizzled page. */
    w_rc_t _fix_nonswizzled(page_s* parent, page_s*& page, const lpid_t& pid, latch_mode_t mode, bool conditional, bool virgin_page);

    /** fixes a root page. */
    w_rc_t _fix_root(page_s*& page, const lpid_t& pid, latch_mode_t mode, bool conditional, bool virgin_page);
    
    /**
     * Newly adds the speficied block to the head of swizzled-pages LRU.
     * This page must be a swizzled page that can be unswizzled.
     */
    void   _add_to_swizzled_lru (bf_idx idx);
    /**
     * Eliminates the speficied block from swizzled-pages LRU.
     */
    void   _remove_from_swizzled_lru (bf_idx idx);

    /**
     * Brings the speficied block to the head of swizzled-pages LRU, assuming the block is already on the LRU.
     * This is costly because of locks, so call this only once in 100 or 1000 times.
     * If the page is worth swizzling, even once in 100 would be very frequent.
     */
    void   _update_swizzled_lru (bf_idx idx);

    /** finds a free block and returns its index. if free list is empty, it evicts some page. */
    w_rc_t _grab_free_block(bf_idx& ret);

    /** Adds a free block to the freelist. */
    void   _add_free_block(bf_idx idx);
    
    /** returns if the idx is in the valid range and also the block is used. for assertion. */
    bool   _is_active_idx (bf_idx idx);
    
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
     * Returns if the dependency FROM cb is still active.
     * If it turns out that the dependency is no longer active, it also clears _dependency_xxx to speed up future function call.
     * cb must be pinned.
     */
    bool   _check_dependency_still_active (bf_tree_cb_t &cb);
    
    bool   _check_dependency_cycle(bf_idx source, bf_idx start_idx);

    bool   _compare_dependency_lsn(const bf_tree_cb_t& cb, const bf_tree_cb_t &dependency_cb) const;
private:
    /** count of blocks (pages) in this bufferpool. */
    bf_idx              _block_cnt;
    
    /** current clock hand for eviction. */
    bf_idx volatile     _clock_hand;
    
    /**
     * Array of pointers to root page descriptors of all currently mounted volumes.
     * The array index is volume ID.
     * 
     * All pointers are initiall NULL.
     * When a volume is mounted, a bf_tree_vol_t is instantiated
     * in this array. When the volume is unmounted, the object is revoked
     * and the pointer is reset to NULL.
     * 
     * Because there is no race condition in loading a volume,
     * this array does not have to be protected by mutex or spinlocks.
     */
    bf_tree_vol_t*      _volumes[MAX_VOL_COUNT];
    
    /** Array of control blocks. array size is _block_cnt. index 0 is never used (means NULL). */
    bf_tree_cb_t*       _control_blocks;

    /** Array of page contents. array size is _block_cnt. index 0 is never used (means NULL). */
    page_s*             _buffer;
    
    /** hashtable to locate a page in this bufferpool. swizzled pages are removed from bufferpool. */
    bf_hashtable*       _hashtable;
    
    /**
     * doubly-linked LRU list for swizzled pages THAT CAN BE UNSWIZZLED.
     * (in other words, root pages aren't included in this counter and the linked list)
     * The array size is 2 * _block_cnt. [block i's prev][block i's next][block i+1's prev]...
     * This logically belongs to _control_blocks, but is an array by itself for efficiency.
     * [0] and [1] are special, meaning list head and tail.
     */
    bf_idx*             _swizzled_lru;

    /**
     * singly-linked freelist. index is same as _buffer/_control_blocks. zero means no link.
     * This logically belongs to _control_blocks, but is an array by itself for efficiency.
     * index 0 is always the head of the list (points to the first free block, or 0 if no free block).
     */
    bf_idx*             _freelist;

    /** count of swizzled pages that can be unswizzled. */
    uint32_t volatile   _swizzled_lru_len;

    /** count of free blocks. */
    uint32_t volatile   _freelist_len;

// Be VERY careful on deadlock to use the following.
    
    /** spin lock to protect swizzled page LRU list. */
    tatas_lock          _swizzled_lru_lock;
    /** spin lock to protect all freelist related stuff. */
    tatas_lock          _freelist_lock;
    
    // queue_based_lock_t   _eviction_mutex;
};

#endif // BF_TREE_H