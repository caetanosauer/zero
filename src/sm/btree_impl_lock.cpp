/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/**
 * Implementation of lock-related internal functions in btree_impl.h.
 */

#define SM_SOURCE
#define BTREE_C

#include "sm_base.h"
#include "bf_tree.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "sm_base.h"
#include "w_key.h"
#include "xct.h"
#include "w_okvl_inl.h"

struct RawLock;

rc_t
btree_impl::_ux_lock_key(
    const StoreID&      stid,
    btree_page_h&      leaf,
    const w_keystr_t&   key,
    latch_mode_t        latch_mode,
    const okvl_mode&       lock_mode,
    bool                check_only
    )
{
    // Top level function used by I/U/D (EX) and search (SH) operations to acquire a lock
    // Lock conflict is possible

    return _ux_lock_key(stid, leaf, key.buffer_as_keystr(), key.get_length_as_keystr(),
                         latch_mode, lock_mode, check_only);
}

rc_t
btree_impl::_ux_lock_key(
    const StoreID&      store,
    btree_page_h&      leaf,
    const void*        keystr,
    size_t             keylen,
    latch_mode_t       latch_mode,
    const okvl_mode&   lock_mode,
    bool               check_only
    )
{
    // Callers:
    // 1. Top level _ux_lock_key() - I/U/D and search operations, lock conflict is possible
    // 2. _ux_lock_range() - lock conflict is possible
    //
    // Lock conflict:
    // 1. Deadlock - the asking lock is held by another transaction currently, and the
    //                      current transaction is holding other locks already, failed
    // 2. Timeout -  the asking lock is held by another transaction currently, but the
    //                     current transaction does not hold other locks, okay to retry

    // For restart operation using lock re-acquisition:
    // 1. On_demand or mixed UNDO - when lock conflict. it triggers UNDO transaction rollback
    //                                                 this is a blocking operation, meaning the other concurrent
    //                                                 transactions asking for the same lock are blocked, no deadlock
    // 2. Traditional UNDO - original behavior, either deadlock error or timeout and retry

    lockid_t lid (store, (const unsigned char*) keystr, keylen);
    // first, try conditionally. we utilize the inserted lock entry even if it fails
    RawLock* entry = NULL;

    // The lock request does the following:
    // If the lock() failed to acquire lock (trying to acquire lock while holding the latch) and
    // if the transaction doesn't have any other locks, because 'condition' is true, lock()
    // returns immediatelly with eCONDLOCKTIMEOUT which indicates it failed to
    // acquire lock but no deadlock worry and the lock entry has been created already.
    // In this case caller (this function) releases latch and try again using retry_lock()
    // which is a blocking operation, this is because it is safe to forever retry without
    // risking deadlock
    // If the lock() returns eDEADLOCK, it means lock acquisition failed and
    // the current transaction already held other locks, it is not safe to retry (will cause
    // further deadlocks) therefore caller must abort the current transaction
    rc_t lock_rc = lm->lock(lid.hash(), lock_mode, true /*check */, false /* wait */,
            !check_only /* acquire */, g_xct(),WAIT_IMMEDIATE, &entry);

    if (!lock_rc.is_error()) {
        // lucky! we got it immediately. just return.
        return RCOK;
    } else {
        // if it caused deadlock and it was chosen to be victim, give up! (not retry)
        if (lock_rc.err_num() == eDEADLOCK)
        {
            // The user transaction will abort and rollback itself upon deadlock detection.
            // Because Express does not have a deadlock monitor and policy to determine
            // which transaction to rollback during a deadlock (should abort the cheaper
            // transaction), the user transaction which detects deadlock will be aborted.
            w_assert1(entry == NULL);
            return lock_rc;
        }

        // couldn't immediately get it. then we unlatch the page and wait.
        w_assert1(lock_rc.err_num() == eCONDLOCKTIMEOUT);
        w_assert1(entry != NULL);

        // we release the latch here. However, we increment the pin count before that
        // to prevent the page from being evicted.
        pin_for_refix_holder pin_holder(leaf.pin_for_refix()); // automatically releases the pin
        lsn_t prelsn = leaf.lsn(); // to check if it's modified after this unlatch
        leaf.unfix();
        // then, we try it unconditionally (this will block)
        W_DO(lm->retry_lock(&entry, !check_only /* acquire */));
        // now we got the lock.. but it might be changed because we unlatched.
        w_rc_t refix_rc = leaf.refix_direct(pin_holder.idx(), latch_mode);
        if (refix_rc.is_error() || leaf.lsn() != prelsn)
        {
            // release acquired lock
            if (entry != NULL) {
                w_assert1(!check_only);
                lm->unlock(entry);
            } else {
                w_assert1(check_only);
            }
            if (refix_rc.is_error())
            {
                return refix_rc;
            }
            else
            {
                w_assert1(leaf.lsn() != prelsn); // unluckily, it's the case
                return RC(eLOCKRETRY); // retry!
            }
        }
        return RCOK;
    }
}

rc_t
btree_impl::_ux_lock_range(const StoreID&     stid,
                           btree_page_h&     leaf,
                           const w_keystr_t& key,
                           slotid_t          slot,
                           latch_mode_t      latch_mode,
                           const okvl_mode&  exact_hit_lock_mode,
                           const okvl_mode&  miss_lock_mode,
                           bool              check_only) {
    return _ux_lock_range(stid, leaf, key.buffer_as_keystr(), key.get_length_as_keystr(),
                          slot, latch_mode, exact_hit_lock_mode, miss_lock_mode, check_only);
}
rc_t
btree_impl::_ux_lock_range(const StoreID&    stid,
                           btree_page_h&    leaf,
                           const void*      keystr,
                           size_t           keylen,
                           slotid_t         slot,
                           latch_mode_t     latch_mode,
                           const okvl_mode& exact_hit_lock_mode,
                           const okvl_mode& miss_lock_mode,
                           bool             check_only) {
    w_assert1(slot >= -1 && slot <= leaf.nrecs());
    w_assert1(exact_hit_lock_mode.get_gap_mode() == okvl_mode::N);
    w_assert1(miss_lock_mode.is_keylock_empty());

    if (slot == -1) { // this means we should search it again
        bool found;
        leaf.search((const char *) keystr, keylen, found, slot);
        w_assert1(!found); // precondition
    }
    w_assert1(slot >= 0 && slot <= leaf.nrecs());
#if W_DEBUG_LEVEL > 1
    w_keystr_t key, key_at_slot;
    key.construct_from_keystr(keystr, keylen);
    if (slot<leaf.nrecs()) {
        leaf.get_key(slot, key_at_slot);
        w_assert1(key_at_slot.compare(key)>0);
    }
#endif // W_DEBUG_LEVEL > 1

    slot--;  // want range lock from previous key
    if (slot == -1 &&
        w_keystr_t::compare_bin_str(keystr, keylen,
                                    leaf.get_fence_low_key(), leaf.get_fence_low_length()) == 0) {
            // We were searching for the low-fence key!  then, we take key lock on it and
            // subsequent structural modification (e.g., merge) will add the low-fence as
            // ghost record to be aware of the lock.
            W_DO (_ux_lock_key(stid, leaf,
                               leaf.get_fence_low_key(), leaf.get_fence_low_length(),
                               latch_mode, exact_hit_lock_mode, check_only));
    } else {
        w_keystr_t prevkey;
        if (slot == -1) {
            leaf.copy_fence_low_key(prevkey);
        } else {
            leaf.get_key(slot, prevkey);
        }
#if W_DEBUG_LEVEL > 1
        w_assert1(prevkey.compare(key) < 0);
#endif // W_DEBUG_LEVEL > 1
        W_DO (_ux_lock_key(stid, leaf, prevkey, latch_mode, miss_lock_mode, check_only));
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
