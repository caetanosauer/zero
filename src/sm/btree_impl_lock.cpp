#include "w_defines.h"

/**
 * Implementation of lock-related internal functions in btree_impl.h.
 */

#define SM_SOURCE
#define BTREE_C

#include "sm_int_2.h"
#ifdef __GNUG__
#   pragma implementation "btree_impl.h"
#endif
#include "btree_p.h"
#include "btree_impl.h"
#include "sm_base.h"
#include "w_key.h"
#include "xct.h"

rc_t
btree_impl::_ux_lock_key(
    btree_p&      leaf,
    const w_keystr_t&   key,
    latch_mode_t        latch_mode,
    lock_mode_t         lock_mode,
    bool                check_only
    )        
{
    return _ux_lock_key(leaf, key.buffer_as_keystr(), key.get_length_as_keystr(),
                         latch_mode, lock_mode, check_only);
}

rc_t
btree_impl::_ux_lock_key(
    btree_p&            leaf,
    const void         *keystr,
    size_t              keylen,
    latch_mode_t        latch_mode,
    lock_mode_t         lock_mode,
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
        lsn_t prelsn = leaf.lsn(); // to check if it's modified after this unlatch
        lpid_t leaf_pid = leaf.pid();
        leaf.unfix();
        // then, we try it unconditionally (this will block)
        W_DO(lm->lock(lid, lock_mode, check_only));
        // now we got the lock.. but it might be changed because we unlatched.
        W_DO(leaf.fix(leaf_pid, latch_mode) );
        if (leaf.lsn() != prelsn) { // unluckily, it's the case
            return RC(eLOCKRETRY); // retry!
        }
        return RCOK;
    }
}

rc_t
btree_impl::_ux_lock_range(
    btree_p&      leaf,
    const w_keystr_t&   key,
    slotid_t slot,
    latch_mode_t        latch_mode,
    lock_mode_t         lock_key_mode,
    lock_mode_t         lock_range_mode,
    bool                check_only
    )        
{
    return _ux_lock_range(leaf, key.buffer_as_keystr(), key.get_length_as_keystr(),
                slot, latch_mode, lock_key_mode, lock_range_mode, check_only);    
}
rc_t
btree_impl::_ux_lock_range(
    btree_p&            leaf,
    const void         *keystr,
    size_t              keylen,
    slotid_t            slot,
    latch_mode_t        latch_mode,
    lock_mode_t         lock_key_mode,
    lock_mode_t         lock_range_mode,
    bool                check_only
    )        
{
    // the interval from previous key is locked
    w_assert1(slot >= -1 && slot <= leaf.nrecs());
    w_assert1(lock_key_mode == XN || lock_key_mode == SN);
    w_assert1(lock_range_mode == NX || lock_range_mode == NS);
    if (slot == -1) { // this means we should search it again
        bool found;
        leaf.search_leaf((const char *) keystr, keylen, found, slot);
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
            latch_mode, lock_key_mode, check_only));
    } else {
        // range lock from previous key
        w_keystr_t prevkey;
        if (slot == leaf.nrecs()) {
            // this happens when the page has no entry
            w_assert1(slot == 0);
            leaf.copy_fence_low_key(prevkey);
        } else {
          leaf.leaf_key(slot, prevkey);
        }
        W_DO (_ux_lock_key(leaf, prevkey, latch_mode, lock_range_mode, check_only));
    }
    return RCOK;
}

rc_t btree_impl::_ux_assure_fence_low_entry(btree_p &leaf)
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
        needs_to_create = true;
    } else {
        w_keystr_t first_key;
        leaf.leaf_key(0, first_key);
        w_assert1(fence_low.compare(first_key) <= 0); // can't be fence_low>first_key
        if (fence_low.compare(first_key) < 0) {
            // fence-low doesn't exist as an entry!
            needs_to_create = true;
        }
    }
    if (needs_to_create) {
        W_DO(_sx_reserve_ghost(leaf, fence_low, 0, false)); // no data is needed
    }
    return RCOK;
}
