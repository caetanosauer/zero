/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/**
 * Implementation of lock-related internal functions in btree_impl.h.
 */

#define SM_SOURCE
#define BTREE_C

#include "sm_int_2.h"
#include "bf_tree.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "sm_base.h"
#include "w_key.h"
#include "xct.h"
#include "w_okvl_inl.h"

rc_t
btree_impl::_ux_lock_key(
    btree_page_h&      leaf,
    const w_keystr_t&   key,
    latch_mode_t        latch_mode,
    const okvl_mode&       lock_mode,
    bool                check_only
    )        
{
    return _ux_lock_key(leaf, key.buffer_as_keystr(), key.get_length_as_keystr(),
                         latch_mode, lock_mode, check_only);
}

rc_t
btree_impl::_ux_lock_key(
    btree_page_h&            leaf,
    const void         *keystr,
    size_t              keylen,
    latch_mode_t        latch_mode,
    const okvl_mode&       lock_mode,
    bool                check_only
    )        
{
    lockid_t lid (leaf.pid().stid(), (const unsigned char*) keystr, keylen);
    // first, try conditionally
    rc_t lock_rc = lm->lock(lid, lock_mode, check_only, WAIT_IMMEDIATE);
    if (!lock_rc.is_error()) {
        // lucky! we got it immediately. just return.
        return RCOK;
    } else {
        // if it caused deadlock and it was chosen to be victim, give up! (not retry)
        if (lock_rc.err_num() == eDEADLOCK) {
            return lock_rc;
        }
        // couldn't immediately get it. then we unlatch the page and wait.
        w_assert2(lock_rc.err_num() == eLOCKTIMEOUT);
        
        // we release the latch here. However, we increment the pin count before that
        // to prevent the page from being evicted.
        pin_for_refix_holder pin_holder(leaf.pin_for_refix()); // automatically releases the pin
        lsn_t prelsn = leaf.lsn(); // to check if it's modified after this unlatch
        leaf.unfix();
        // then, we try it unconditionally (this will block)
        W_DO(lm->lock(lid, lock_mode, check_only));
        // now we got the lock.. but it might be changed because we unlatched.
        W_DO(leaf.refix_direct(pin_holder.idx(), latch_mode));
        if (leaf.lsn() != prelsn) { // unluckily, it's the case
            return RC(eLOCKRETRY); // retry!
        }
        return RCOK;
    }
}

rc_t
btree_impl::_ux_lock_range(
    btree_page_h&      leaf,
    const w_keystr_t&   key,
    slotid_t slot,
    latch_mode_t        latch_mode,
    const okvl_mode&       exact_hit_lock_mode,
    const okvl_mode&       miss_lock_mode,
    bool                check_only
    )        
{
    return _ux_lock_range(leaf, key.buffer_as_keystr(), key.get_length_as_keystr(),
                slot, latch_mode, exact_hit_lock_mode, miss_lock_mode, check_only);    
}
rc_t
btree_impl::_ux_lock_range(
    btree_page_h&            leaf,
    const void         *keystr,
    size_t              keylen,
    slotid_t            slot,
    latch_mode_t        latch_mode,
    const okvl_mode&       exact_hit_lock_mode,
    const okvl_mode&       miss_lock_mode,
    bool                check_only
    )        
{
    // the interval from previous key is locked
    w_assert1(slot >= -1 && slot <= leaf.nrecs());
    w_assert1(exact_hit_lock_mode.get_gap_mode() == okvl_mode::N);
    w_assert1(miss_lock_mode.is_keylock_empty());
    if (slot == -1) { // this means we should search it again
        bool found;
        leaf.search((const char *) keystr, keylen, found, slot);
        w_assert1(!found); // otherwise why taking range lock?
    }
    w_assert1(slot >= 0 && slot <= leaf.nrecs());
    
    // if "slot" says the key should be placed in the end of this page,
    // take range lock from the one before (well, except the page has no entry)
    if (slot == leaf.nrecs() && slot != 0) --slot;
    
    if (slot == 0 &&
        w_keystr_t::compare_bin_str(keystr, keylen,
        leaf.get_fence_low_key(), leaf.get_fence_low_length()) == 0) {
        // we were searching for the low-fence key!
        // then, we take key lock on it. and subsequent
        // structural modification (e.g., merge) will add
        // the low-fence as ghost record to be aware of the lock
        W_DO (_ux_lock_key(leaf,
            leaf.get_fence_low_key(), leaf.get_fence_low_length(),
            latch_mode, exact_hit_lock_mode, check_only));
    } else {
        // range lock from previous key
        w_keystr_t prevkey;
        if (slot == leaf.nrecs()) {
            // this happens when the page has no entry
            w_assert1(slot == 0);
            leaf.copy_fence_low_key(prevkey);
        } else {
          leaf.get_key(slot, prevkey);
        }
        W_DO (_ux_lock_key(leaf, prevkey, latch_mode, miss_lock_mode, check_only));
    }
    return RCOK;
}

rc_t btree_impl::_ux_assure_fence_low_entry(btree_page_h &leaf)
{
    w_assert1(leaf.is_fixed());
    w_assert1(leaf.latch_mode() == LATCH_EX);
    if (!leaf.is_leaf()) {
        // locks are taken only for leaf-page entries. this case isn't an issue
        return RCOK;
    }
    w_keystr_t fence_low;
    leaf.copy_fence_low_key(fence_low);
    bool needs_to_create = false;
    if (leaf.nrecs() == 0) {
        if (leaf.compare_with_fence_high(fence_low) == 0) {
            // low==high happens only during page split. In that case, no one can have a lock
            // in the page being created. No need to assure the record.
            return RCOK;
        }
        needs_to_create = true;
    } else {
        w_keystr_t first_key;
        leaf.get_key(0, first_key);
        w_assert1(fence_low.compare(first_key) <= 0); // can't be fence_low>first_key
        if (fence_low.compare(first_key) < 0) {
            // fence-low doesn't exist as an entry!
            needs_to_create = true;
        }
    }
    if (needs_to_create) {
        W_DO(_sx_reserve_ghost(leaf, fence_low, 0)); // no data is needed
    }
    return RCOK;
}
