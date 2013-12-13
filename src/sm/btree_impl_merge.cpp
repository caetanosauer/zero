/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/**
 * Implementation of tree merge/rebalance related functions in btree_impl.h.
 */

#define SM_SOURCE
#define BTREE_C

#include "sm_int_2.h"
#include "sm_base.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "w_key.h"
#include "xct.h"
#include "bf.h"
#include "bf_tree.h"

rc_t btree_impl::_sx_rebalance_foster(btree_page_h &page)
{
    FUNC(btree_impl::_sx_rebalance_foster);
    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_rebalance_foster_core(page);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}

rc_t btree_impl::_ux_rebalance_foster_core(btree_page_h &page)
{
    w_assert1 (xct()->is_sys_xct());
    w_assert1 (page.latch_mode() == LATCH_EX);
    if (page.get_foster() == 0) {
        return RCOK; // nothing to rebalance
    }

    btree_page_h foster_p;
    W_DO(foster_p.fix_nonroot(page, page.vol(), page.get_foster_opaqueptr(), LATCH_EX));

    smsize_t used = page.used_space();
    smsize_t foster_used = foster_p.used_space();
    smsize_t balanced_size = (used + foster_used) / 2 + SM_PAGESIZE / 10;
    
    int move_count = 0;
    // worth rebalancing?
    if (used < foster_used * 3) {
        return RCOK; // don't bother
    }
    if (page.is_insertion_skewed_right()) {
        // then, anyway the foster-child will receive many more tuples. leave it.
        // also, this is needed to keep skewed-split meaningful
        return RCOK;
    }
    smsize_t move_size = 0;
    while (used - move_size > balanced_size && -move_count < page.nrecs() - 1) {
        ++move_count;
        move_size += page.get_rec_space(page.nitems() - move_count - 1);
    }
    
    if (move_count == 0) {
        return RCOK;
    }
    return _ux_rebalance_foster_core (page, foster_p, move_count);
}

rc_t btree_impl::_ux_rebalance_foster_core(btree_page_h &page, btree_page_h &foster_p, int32_t move_count)
{
    w_assert1 (page.latch_mode() == LATCH_EX);
    w_assert1 (foster_p.latch_mode() == LATCH_EX);
    w_assert1 (page.get_foster() == foster_p.pid().page);
    if (move_count == 0) {
        return RCOK;
    }
    
    // assure foster-child page has an entry same as fence-low for locking correctness. 
    // See jira ticket:84 "Key Range Locking" (originally trac ticket:86).
    W_DO(_ux_assure_fence_low_entry(foster_p));

    // foster-parent should be written later because it's the data source
    // first, mark both dirty.
    page.set_dirty();
    foster_p.set_dirty();
    bool registered = smlevel_0::bf->register_write_order_dependency(page._pp, foster_p._pp);
    if (!registered) {
        // TODO in this case we should do full logging.
        DBGOUT1 (<< "oops, couldn't force write order dependency in rebalance. this should be treated with care");
    }
    // this can't cause cycle as it's always right-to-left depdencency.

    // this is the only log we need
    W_DO (log_btree_foster_rebalance (foster_p, page.pid().page, move_count));

    // we are moving significant fraction of records in the pages,
    // so the move will most likely change the fence keys in the middle, thus prefix too.
    // it will not be a simple move, so we have to make the page image from scratch.
    // this is also useful as defragmentation.

    // scratch block of foster_p
    generic_page scratch;
    ::memcpy (&scratch, foster_p._pp, sizeof(scratch));
    btree_page_h scratch_p (&scratch);
    
    w_keystr_t new_low_key, high_key, chain_high_key;
    scratch_p.copy_fence_high_key(high_key);
    scratch_p.copy_chain_fence_high_key(chain_high_key);
    
    btrec_t lowest (page, page.nrecs() - move_count);
    if (foster_p.is_node()) {
        // then, choosing the new fence-low is easier, but handling of pid0 is uglier.
        new_low_key = lowest.key();
        shpid_t new_pid0 = lowest.child();

        W_DO(foster_p.format_steal(scratch_p.pid(), scratch_p.btree_root(), scratch_p.level(), new_pid0,
            scratch_p.get_foster(),
            new_low_key, high_key, chain_high_key,
            false, // don't log it
            &page, page.nrecs() - move_count + 1, page.nrecs(), // steal from foster-parent (+1 to skip lowest record)
            &scratch_p, 0, scratch_p.nrecs(), // steal from old page
            true // also steal old page's pid0 with low-fence key as a regular record
        ));
    } else {
        // then, choosing the new fence-low is uglier, but handling of pid0 is easier.
        // pick new fence-low. this is basically same as split code though we don't look for shorter keys.
        btrec_t k1 (page, page.nrecs() - move_count - 1);
        btrec_t k2 (page, page.nrecs() - move_count);        
        size_t common_bytes = k1.key().common_leading_bytes(k2.key());
        w_assert1(common_bytes < k2.key().get_length_as_keystr()); // otherwise the two keys are the same.
        new_low_key.construct_from_keystr(k2.key().buffer_as_keystr(), common_bytes + 1);
        W_DO(foster_p.format_steal(scratch_p.pid(), scratch_p.btree_root(), scratch_p.level(), 0,
            scratch_p.get_foster(),
            new_low_key, high_key, chain_high_key,
            false, // don't log it
            &page, page.nrecs() - move_count, page.nrecs(), // steal from foster-parent
            &scratch_p, 0, scratch_p.nrecs() // steal from old page
        ));
    }

    // next, also scratch and build foster-parent
    ::memcpy (&scratch, page._pp, sizeof(scratch));
    w_keystr_t low_key;
    scratch_p.copy_fence_low_key(low_key);
    scratch_p.copy_chain_fence_high_key(chain_high_key);
    W_DO(page.format_steal(scratch_p.pid(), scratch_p.btree_root(), scratch_p.level(), scratch_p.pid0(),
        scratch_p.get_foster(),
        low_key, new_low_key, chain_high_key, // high key is changed!
        false, // don't log it
        &scratch_p, 0, scratch_p.nrecs() - move_count // steal from old page
    ));
    
    w_assert3(page.is_consistent(true, true));
    w_assert3(foster_p.is_consistent(true, true));

    return RCOK;
}

rc_t btree_impl::_sx_merge_foster(btree_page_h &page)
{
    FUNC(btree_impl::_sx_merge_foster);
    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_merge_foster_core(page);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}

/** this function conservatively estimates.*/
smsize_t estimate_required_space_to_merge (btree_page_h &page, btree_page_h &merged)
{
    smsize_t ret = merged.used_space();

    // we need to replace fence-high key too. (fence-low will be same)
    ret += merged.get_fence_high_length() - page.get_fence_low_length();

    // if there is no more sibling on the right, we will delete chain-fence-high
    if (merged.get_chain_fence_high_length() == 0) {
        ret -= page.get_chain_fence_high_length();
    }
    
    // prefix length might be shorter, resulting in larger record size
    size_t prefix_len_after_merge = w_keystr_t::common_leading_bytes(
        (const unsigned char*) page.get_prefix_key(), page.get_prefix_length(),
        (const unsigned char*) merged.get_prefix_key(), merged.get_prefix_length()
    );
    ret += page.nrecs() * (page.get_prefix_length() - prefix_len_after_merge);
    ret += merged.nrecs() * (merged.get_prefix_length() - prefix_len_after_merge);
    ret += (page.nrecs() + merged.nrecs()) * (8 - 1); // worst-case of alignment
    
    return ret;
}

rc_t btree_impl::_ux_merge_foster_core(btree_page_h &page)
{
    w_assert1 (xct()->is_sys_xct());
    w_assert1 (page.latch_mode() == LATCH_EX);
    if (page.get_foster() == 0) {
        return RCOK; // nothing to rebalance
    }

    btree_page_h foster_p;
    W_DO(foster_p.fix_nonroot(page, page.vol(), page.get_foster_opaqueptr(), LATCH_EX));
    
    // assure foster-child page has an entry same as fence-low for locking correctness. 
    // See jira ticket:84 "Key Range Locking" (originally trac ticket:86).
    W_DO(_ux_assure_fence_low_entry(foster_p));

    // can we fully absorb it?
    size_t additional = estimate_required_space_to_merge(page, foster_p);
    if (page.usable_space() < additional) {
        return RCOK; // don't do it
    }
    
    // foster-child should be written later because it's the data source
    // first, mark them dirty.
    page.set_dirty();
    foster_p.set_dirty();
    bool registered = smlevel_0::bf->register_write_order_dependency(foster_p._pp, page._pp);
    if (!registered) {
        // this means the merging will cause a cycle in write-order.
        // so, let's not do the merging now.
        // see ticket:39 for more details (jira ticket:39 "Node removal and rebalancing" (originally trac ticket:39))
        return RCOK;
    }
    
    // log for foster-parent.
    W_DO(log_btree_foster_merge (page));
    W_DO(foster_p.set_to_be_deleted(true)); // log it (for foster-child)

    // like split, use scratch block to cleanly make a new page image
    w_keystr_t low_key, high_key, chain_high_key; // fence keys after merging
    page.copy_fence_low_key(low_key);
    foster_p.copy_fence_high_key(high_key); // after merging, foster-child's high-key is high-key
    if (foster_p.get_foster() != 0) {
        // if no foster after merging, chain-high will disappear
        page.copy_chain_fence_high_key(chain_high_key);
    }
    generic_page scratch;
    ::memcpy (&scratch, page._pp, sizeof(scratch));
    btree_page_h scratch_p (&scratch);
    W_DO(page.format_steal(scratch_p.pid(), scratch_p.btree_root(), scratch_p.level(), scratch_p.pid0(),
        foster_p.get_foster(), // foster-child's foster will be the next one after merge
        low_key, high_key, chain_high_key,
        false, // don't log it
        &scratch_p, 0, scratch_p.nrecs(), // steal from foster-parent
        &foster_p, 0, foster_p.nrecs() // steal from foster-child
    ));
    w_assert3(page.is_consistent(true, true));
    w_assert1(page.is_fixed());
    return RCOK;
}

rc_t btree_impl::_sx_deadopt_foster(btree_page_h &real_parent, slotid_t foster_parent_slot)
{
    FUNC(btree_impl::_sx_deadopt_foster);
    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_deadopt_foster_core(real_parent, foster_parent_slot);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}

void btree_impl::_ux_deadopt_foster_apply_real_parent(btree_page_h &real_parent,
                                                      shpid_t W_IFDEBUG1(foster_child_id),
                                                      slotid_t foster_parent_slot) {
#if W_DEBUG_LEVEL>0
    w_assert1 (real_parent.latch_mode() == LATCH_EX);
    w_assert1(real_parent.is_node());
    w_assert1 (foster_parent_slot + 1 < real_parent.nrecs());
    btrec_t rec (real_parent, foster_parent_slot + 1);
    w_assert1(rec.child() == foster_child_id);
#endif // W_DEBUG_LEVEL>0
    real_parent.remove_shift_nolog (foster_parent_slot + 1);
}

void btree_impl::_ux_deadopt_foster_apply_foster_parent(btree_page_h &foster_parent,
                                                        shpid_t foster_child_id,
                                                        const w_keystr_t & W_IFDEBUG1(low_key),
                                                        const w_keystr_t &high_key) {
#if W_DEBUG_LEVEL>0
    w_assert1 (foster_parent.latch_mode() == LATCH_EX);
    w_assert1 (foster_parent.get_foster() == 0);
    w_assert1 (low_key.compare(high_key) < 0);
    w_assert1 (foster_parent.compare_with_fence_high(low_key) == 0);
#endif // W_DEBUG_LEVEL>0
    // set new chain-fence-high
    // note: we need to copy them into w_keystr_t because we are using
    // these values to modify foster_parent itself!
    w_keystr_t org_low_key, org_high_key;
    foster_parent.copy_fence_low_key(org_low_key);
    foster_parent.copy_fence_high_key(org_high_key);
    w_assert1 ((size_t)foster_parent.get_prefix_length() == org_low_key.common_leading_bytes(org_high_key));

    // high_key of adoped child is now chain-fence-high:
    rc_t rc = foster_parent.replace_fence_rec_nolog(org_low_key, org_high_key, high_key);
    if (rc.is_error()) {
        //eRECWONTFIT is very slighly possible...
        w_assert1(rc.err_num() == eRECWONTFIT);
        rc = foster_parent.defrag();
        w_assert1(!rc.is_error());
        rc_t rc = foster_parent.replace_fence_rec_nolog(org_low_key, org_high_key, high_key);
        if (rc.is_error()) {
            w_assert1(false);        
            W_FATAL(eINTERNAL); 
        }
    }
    foster_parent.page()->btree_chain_fence_high_length = high_key.get_length_as_keystr();
    
    foster_parent.page()->btree_foster = foster_child_id;
}
rc_t btree_impl::_ux_deadopt_foster_core(btree_page_h &real_parent, slotid_t foster_parent_slot)
{
    w_assert1 (xct()->is_sys_xct());
    w_assert1 (real_parent.is_node());
    w_assert1 (real_parent.latch_mode() == LATCH_EX);
    w_assert1 (foster_parent_slot + 1 < real_parent.nrecs());
    shpid_t foster_parent_pid;
    if (foster_parent_slot < 0) {
        w_assert1 (foster_parent_slot == -1);
        foster_parent_pid = real_parent.pid0_opaqueptr();
    } else {
        foster_parent_pid = real_parent.child_opaqueptr(foster_parent_slot);
    }
    btree_page_h foster_parent;
    W_DO(foster_parent.fix_nonroot(real_parent, real_parent.vol(), foster_parent_pid, LATCH_EX));
    
    if (foster_parent.get_foster() != 0) {
        // De-Adopt can't be processed when foster-parent already has foster-child. Do nothing
        // see ticket:39 (jira ticket:39 "Node removal and rebalancing" (originally trac ticket:39))
        return RCOK; // maybe error?
    }
    
    // get low_key
    btrec_t rec (real_parent, foster_parent_slot + 1);
    const w_keystr_t &low_key = rec.key();
    shpid_t foster_child_id = rec.child();

    // get high_key. if it's the last record, fence-high of real parent
    w_keystr_t high_key;
    if (foster_parent_slot + 2 < real_parent.nrecs()) {
        btrec_t next_rec (real_parent, foster_parent_slot + 2);
        high_key = next_rec.key();
    } else {
        w_assert1(foster_parent_slot + 1 == real_parent.nrecs());
        real_parent.copy_fence_high_key(high_key);
    }
    w_assert1(low_key.compare(high_key) < 0);

    // logs to REDO each page
    W_DO(log_btree_foster_deadopt_real_parent(real_parent, foster_child_id, foster_parent_slot));
    W_DO(log_btree_foster_deadopt_foster_parent(foster_parent, foster_child_id, low_key, high_key));
    
    _ux_deadopt_foster_apply_real_parent (real_parent, foster_child_id, foster_parent_slot);
    _ux_deadopt_foster_apply_foster_parent (foster_parent, foster_child_id, low_key, high_key);

    w_assert3(real_parent.is_consistent(true, true));
    w_assert3(foster_parent.is_consistent(true, true));
    return RCOK;
}
