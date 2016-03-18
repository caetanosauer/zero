/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BF_TREE_CB_H
#define BF_TREE_CB_H

#include "w_defines.h"
#include "latch.h"
#include "bf_tree.h"
#include <string.h>

#include <assert.h>

/**
 * \Brief Control block in the new buffer pool class.
 * \ingroup SSMBUFPOOL
 *
 * \Details
 * The design of control block had at least 2 big surgeries.  The first one happened in
 * the summer of 2011, making our first version of Foster B-tree based on Shore-MT.  We
 * added a few relatively-minor things in bufferpool
 *
 * Next happened in early 2012 when we overhauled the whole bufferpool code to implement
 * page swizzling and do related surgeries.  At this point, all bufferpool classes are
 * renamed and rewritten from scratch.
 *
 *
 * \Section PinCount (_pin_cnt)
 *
 * The value is incremented when 1) if the page is non-swizzled and some thread fixes this
 * page, 2) when the page has been swizzled, 3) when the page's child page is brought into
 * buffer pool.  Decremented on a corresponding anti-actions of them (e.g., unfix).  Both
 * increments and decrements as well as reading must be done atomically.
 *
 * The block can be evicted from bufferpool only when this value is 0.  So, when the block
 * is selected as the eviction victim, the eviction thread should atomically set this
 * value to be -1 from 0.  In other words, it must be atomic CAS.
 *
 * Whenever this value is -1, everyone should ignore this block as non-existent like NULL.
 * It's similar to the "in-transit" in the old buffer pool, but this is simpler and more
 * efficient.  The thread that increments this value should check it, too.  Thus, the
 * atomic increments must be atomic-CAS (not just atomic-INC) because the original value
 * might be -1 as of the action!  However, there are some cases you can do inc/dec just as
 * atomic-INC/DEC.
 *
 * Decrement is always safe to be atomic-dec because (assuming there is no bug) you should
 * be always decrementing from 1 or larger.  Also, increment is sometimes safe to be
 * atomic-inc when for some reason you are sure there are at least one more pins on the
 * block, such as when you are incrementing for the case of 3) above.
 *
 */
struct bf_tree_cb_t {
    /** clears all properties. */
    inline void clear () {
        clear_latch();
        clear_except_latch();
    }

    /** clears all properties but latch. */
    inline void clear_except_latch () {
#ifdef BP_ALTERNATE_CB_LATCH
        signed char latch_offset = _latch_offset;
        ::memset(this, 0, sizeof(bf_tree_cb_t));
        _latch_offset = latch_offset;
#else
        ::memset(this, 0, sizeof(bf_tree_cb_t)-sizeof(latch_t));
#endif
    }

    /** clears latch */
    inline void clear_latch() {
        ::memset(latchp(), 0, sizeof(latch_t));
    }

    // control block is bulk-initialized by malloc and memset. It has to be aligned.

    /**
     * short page ID of the page currently pinned on this block.  (we don't have stnum in
     * bufferpool) protected by ??
     */
    PageID _pid;     // +4  -> 4

    /// Count of pins on this block.  See class comments; protected by ??
    int32_t _pin_cnt;       // +4 -> 8

    /**
     * Reference count (for clock algorithm).  Approximate, so not protected by latches.
     * We increment it whenever (re-)fixing the page in the bufferpool.  We limit the
     * maximum value of the refcount by BP_MAX_REFCOUNT to avoid the scalability
     * bottleneck caused by excessive cache coherence traffic (cacheline ping-pongs
     * between sockets).  The counter still has enough granularity to separate cold from
     * hot pages.  Clock decrements the counter when it visits the page.
     */
    uint16_t _ref_count;// +2  -> 10

    /// Reference count incremented only by X-latching
    uint16_t _ref_count_ex; // +2 -> 12

    /// true if this block is actually used
    bool _used;          // +1  -> 13
    /// Whether this page is swizzled from the parent
    bool _swizzled;      // +1 -> 14
    /// Whether this page is concurrently being swizzled by another thread
    bool _concurrent_swizzling;      // +1 -> 15
    /// Filler
    uint8_t _fill16;        // +1 -> 16

    // CS TODO: testing approach of maintaining page LSN in CB
    lsn_t _page_lsn; // +8 -> 24
    void set_page_lsn(lsn_t lsn) { _page_lsn = lsn; }
    lsn_t get_page_lsn() const { return _page_lsn; }

    // CS: page_lsn value when it was last picked for cleaning
    // Replaces the old dirty flag, because dirty is defined as
    // page_lsn > clean_lsn
    lsn_t _clean_lsn; // +8 -> 32
    void set_clean_lsn(lsn_t lsn) { _clean_lsn = lsn; }
    lsn_t get_clean_lsn() const { return _clean_lsn; }

    bool is_dirty() const { return _page_lsn > _clean_lsn; }


    /**
     * number of swizzled pointers to children; protected by ??
     */
    uint16_t                    _swizzled_ptr_cnt_hint; // +2 -> 34

    // Add padding to align control block at cacheline boundary (64 bytes)
    uint8_t _fill63[29];    // +29 -> 63

#ifdef BP_ALTERNATE_CB_LATCH
    /** offset to the latch to protect this page. */
    int8_t                      _latch_offset;  // +1 -> 64
#else
    fill8                       _fill64;      // +1 -> 64
    latch_t                     _latch;         // +64 ->128
#endif

    // increment pin count atomically
    void pin()
    {
        lintel::unsafe::atomic_fetch_add(&_pin_cnt, 1);
    }

    // decrement pin count atomically
    void unpin()
    {
        lintel::unsafe::atomic_fetch_sub(&_pin_cnt, 1);
    }

    // disabled (no implementation)
    bf_tree_cb_t();
    bf_tree_cb_t(const bf_tree_cb_t&);
    bf_tree_cb_t& operator=(const bf_tree_cb_t&);

    latch_t* latchp() const {
#ifdef BP_ALTERNATE_CB_LATCH
        uintptr_t p = reinterpret_cast<uintptr_t>(this) + _latch_offset;
        return reinterpret_cast<latch_t*>(p);
#else
        return const_cast<latch_t*>(&_latch);
#endif
    }

    latch_t &latch() {
        return *latchp();
    }
};

#endif // BF_TREE_CB_H
