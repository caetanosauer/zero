/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BF_TREE_CB_H
#define BF_TREE_CB_H

#include "w_defines.h"
#include "latch.h"
#include "bf_tree.h"
#include <string.h>
#include <atomic>

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
    /**
     * Maximum value of the per-frame refcount (reference counter).  We cap the
     * refcount to avoid contention on the cacheline of the frame's control
     * block (due to ping-pongs between sockets) when multiple sockets
     * read-access the same frame.  The refcount max value should have enough
     * granularity to separate cold from hot pages.
     *
     * CS TODO: but doesnt the latch itself already incur such cacheline
     * bouncing?  If so, then we could simply move the refcount inside latch_t
     * (which must have the same size as a cacheline) and be done with it. No
     * additional overhead on cache coherence other than the latching itself is
     * expected. We could reuse the field _total_count in latch_t, or even
     * split it into to 16-bit integers: one for shared and one for exclusive
     * latches. This field is currently only used for tests, but it doesn't
     * make sense to count CB references and latch acquisitions in separate
     * variables.
     */
    static const uint16_t BP_MAX_REFCOUNT = 1024;

    /**
     * Initial value of the per-frame refcount (reference counter).
     */
    static const uint16_t BP_INITIAL_REFCOUNT = 0;

    /** clears all properties. */
    inline void clear () {
        clear_latch();
        clear_except_latch();
    }

    /** clears all properties but latch. */
    inline void clear_except_latch () {
        signed char latch_offset = _latch_offset;
        ::memset(this, 0, sizeof(bf_tree_cb_t));
        _latch_offset = latch_offset;
    }

    /** Initializes all fields -- called by fix when fetching a new page */
    void init(PageID pid, lsn_t page_lsn)
    {
        clear_except_latch();
        _pin_cnt = 0;
        _pid = pid;
        _used = true;
        _swizzled = false;
        _ref_count = BP_INITIAL_REFCOUNT;
        _ref_count_ex = BP_INITIAL_REFCOUNT;
        _clean_lsn = page_lsn;
        _page_lsn = page_lsn;
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

    /// Filler
    uint16_t _fill14;        // +2 -> 14

    /// true if this block is actually used
    std::atomic<bool> _used;          // +1  -> 15
    /// Whether this page is swizzled from the parent
    std::atomic<bool> _swizzled;      // +1 -> 16

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

    /* The bufferpool should alternate location of latches and control blocks
     * starting at an odd multiple of 64B as follows:
     *                  ...|CB0|L0|L1|CB1|CB2|L2|L3|CB3|...
     * This layout addresses a pathology that we attribute to the hardware
     * spatial prefetcher. The default layout allocates a latch right after a
     * control block so that the control block and latch live in adjacent cache
     * lines (in the same 128B sector). The pathology happens because when we
     * write-access the latch, the processor prefetches the control block in
     * read-exclusive mode even if we late really only read-access the control
     * block. This causes unnecessary coherence traffic. With the new layout, we
     * avoid having a control block and latch in the same 128B sector.
     */

    /** offset to the latch to protect this page. */
    int8_t                      _latch_offset;  // +1 -> 64

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

    void inc_ref_count()
    {
        if (_ref_count < BP_MAX_REFCOUNT) {
            ++_ref_count;
        }
    }

    void inc_ref_count_ex()
    {
        if (_ref_count < BP_MAX_REFCOUNT) {
            ++_ref_count_ex;
        }
    }

    // disabled (no implementation)
    bf_tree_cb_t();
    bf_tree_cb_t(const bf_tree_cb_t&);
    bf_tree_cb_t& operator=(const bf_tree_cb_t&);

    latch_t* latchp() const {
        uintptr_t p = reinterpret_cast<uintptr_t>(this) + _latch_offset;
        return reinterpret_cast<latch_t*>(p);
    }

    latch_t &latch() {
        return *latchp();
    }
};

#endif // BF_TREE_CB_H
