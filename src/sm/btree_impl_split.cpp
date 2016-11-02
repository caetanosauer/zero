/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/**
 * Implementation of split/propagate related functions in btree_impl.h.
 * Separated from btree_impl.cpp.
 */

#define SM_SOURCE
#define BTREE_C

#include "sm_base.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "vec_t.h"
#include "w_key.h"
#include "sm.h"
#include "xct.h"
#include "bf_tree.h"
#include "vol.h"

rc_t btree_impl::_sx_norec_alloc(btree_page_h &page, PageID &new_page_id) {
    sys_xct_section_t sxs(true);
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_norec_alloc_core(page, new_page_id);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}

rc_t btree_impl::_ux_norec_alloc_core(btree_page_h &page, PageID &new_page_id) {
    // This is called only in REDO-only SSX, so no compensation logging. Just apply.
    w_assert1 (xct()->is_single_log_sys_xct());
    w_assert1 (page.latch_mode() == LATCH_EX);

    W_DO(smlevel_0::vol->alloc_a_page(new_page_id));
    btree_page_h new_page;
    w_rc_t rc;
    rc = new_page.fix_nonroot(page, new_page_id, LATCH_EX, false, true);

    if (rc.is_error()) {
        // if failed for any reason, we release the allocated page.
        W_DO(smlevel_0::vol ->deallocate_page(new_page_id));
        return rc;
    }

    // The new page has an empty key range; parent's high to high.
    w_keystr_t fence, chain_high;
    page.copy_fence_high_key(fence);
    bool was_right_most = (page.get_chain_fence_high_length() == 0);
    page.copy_chain_fence_high_key(chain_high);
    if (was_right_most) {
        // this means there was no chain or the page was the right-most of it.
        // (so its high=high of chain)
        // upon the first foster split, we start setting the chain-high.
        page.copy_fence_high_key(chain_high);
    }

#if W_DEBUG_LEVEL >= 3
    lsn_t old_lsn = page.get_page_lsn();
#endif //W_DEBUG_LEVEL

    W_DO(log_btree_norec_alloc(page, new_page, new_page_id, fence, chain_high));
    DBGOUT3(<< "btree_impl::_ux_norec_alloc_core, fence=" << fence << ", old-LSN="
        << old_lsn << ", new-LSN=" << page.get_page_lsn() << ", PID=" << new_page_id);

    // initialize as an empty child:
    new_page.format_steal(page.get_page_lsn(), new_page_id, page.store(),
                          page.root(), page.level(), 0, lsn_t::null,
                          page.get_foster_opaqueptr(), page.get_foster_emlsn(),
                          fence, fence, chain_high, false);
    page.accept_empty_child(page.get_page_lsn(), new_page_id, false /*not from redo*/);

    // in this operation, the log contains everything we need to recover without any
    // write-order-dependency. So, no registration for WOD.
    w_assert3(new_page.is_consistent(true, true));
    w_assert1(new_page.is_fixed());
    w_assert1(new_page.latch_mode() == LATCH_EX);

    w_assert3(page.is_consistent(true, true));
    w_assert1(page.is_fixed());
    return RCOK;
}

rc_t btree_impl::_sx_split_foster(btree_page_h& page, PageID& new_page_id,
        const w_keystr_t& triggering_key)
{
    sys_xct_section_t sxs(true);
    W_DO(sxs.check_error_on_start());

    w_assert1 (page.latch_mode() == LATCH_EX);

    // DBG(<< "SPLITTING " << page);

    /*
     * Step 1: Allocate a new page for the foster child
     */
    W_DO(smlevel_0::vol->alloc_a_page(new_page_id));

    /*
     * Step 2: Create new foster child and move records into it, logging its
     * raw contents as a page_img_format operation
     */
    btree_page_h new_page;
    rc_t rc = new_page.fix_nonroot(page, new_page_id,
            LATCH_EX, false, true);
    if (rc.is_error()) {
        W_DO(smlevel_0::vol ->deallocate_page(new_page_id));
        return rc;
    }

    // assure foster-child page has an entry same as fence-low for locking correctness.
    // See jira ticket:84 "Key Range Locking" (originally trac ticket:86).
    // CS TODO - why is this required
    // There may be a bug, since error happens if we uncomment this
    // W_DO(_ux_assure_fence_low_entry(new_page)); // this might be another SSX

    int move_count = 0;
    w_keystr_t split_key;
    W_DO(new_page.format_foster_child(page, new_page_id, triggering_key, split_key,
            move_count));
    w_assert0(move_count > 0);
    // DBG5(<< "NEW FOSTER CHILD " << new_page);

    /*
     * Step 3: Delete moved records and update foster child pointer and high
     * fence on overflowing page. Foster parent is not recompressed after
     * moving records (CS TODO)
     */
    page.delete_range(page.nrecs() - move_count, page.nrecs());
    // DBG5(<< "AFTER RANGE DELETE " << page);

    w_keystr_t new_chain;
    new_page.copy_chain_fence_high_key(new_chain);
    bool foster_set =
        page.set_foster_child(new_page_id, split_key, new_chain);
    w_assert0(foster_set);

    /*
     * Step 4: Update parent pointers of the moved records. The new foster
     * child will have the correct parent set by the fix call above. This is
     * only required because of swizzling.
     */

    // set parent pointer on hash table
    // smlevel_0::bf->switch_parent(new_page_id, page.get_generic_page());

    // set parent pointer for children that moved to new page
    int max_slot = new_page.max_child_slot();
    for (general_recordid_t i = GeneralRecordIds::FOSTER_CHILD; i <= max_slot; ++i)
    {
        // CS TODO: Slot 1 (which is actually 0 in the internal page
        // representation) is not used when inserting into an empty node (see
        // my comment on btree_page_h.cpp::insert_nonghost), so in *some*
        // cases, the slot i=1 will yield and invalid page in switch_parent
        // below. Because of this great design feature, switch_parent has to
        // cope with an invalid page.
        smlevel_0::bf->switch_parent(*new_page.child_slot_address(i),
                new_page.get_generic_page());
    }

    /*
     * Step 5: Log bulk deletion and foster update on parent
     */
    W_DO(log_btree_split(new_page, page, move_count, split_key, new_chain));

    w_assert1(new_page.get_page_lsn() != lsn_t::null);

    // hint for subsequent accesses
    increase_forster_child(page.pid());

    W_DO (sxs.end_sys_xct (RCOK));

    DBG1(<< "Split page " << page.pid() << " into " << new_page_id);

    return RCOK;
}

rc_t btree_impl::_sx_split_if_needed (btree_page_h &page, const w_keystr_t &new_key) {
    bool need_split =
        !page.check_space_for_insert_node(new_key)
        || (page.is_insertion_extremely_skewed_right()
            && page.check_chance_for_norecord_split(new_key));
    if (!need_split) {
        return RCOK; // easy
    }

    PageID new_page_id;
    // we are running user transaction. simply call SSX split.
    W_DO(_sx_split_foster(page, new_page_id, new_key));

    // After split, the new page might be the parent of the new_key now.
    if (!page.fence_contains(new_key)) {
        btree_page_h new_page;
        W_DO(new_page.fix_nonroot(page, new_page_id, LATCH_EX));
        w_assert1(new_page.fence_contains(new_key));
        page.unfix();
        page = new_page;
    }

    return RCOK;
}

rc_t btree_impl::_sx_adopt_foster_all (btree_page_h &root, bool recursive)
{
    W_DO(_sx_adopt_foster_all_core(root, true, recursive));
    return RCOK;
}
rc_t btree_impl::_sx_adopt_foster_all_core (
    btree_page_h &parent, bool is_root, bool recursive)
{
    // TODO this should use the improved tree-walk-through
    // See jira ticket:60 "Tree walk-through without more than 2 pages latched" (originally trac ticket:62)
    w_assert1 (xct()->is_sys_xct());
    w_assert1 (parent.is_fixed());
    w_assert1 (parent.latch_mode() == LATCH_EX);
    if (parent.is_node()) {
        w_assert1(parent.pid0());
        W_DO(_sx_adopt_foster_sweep(parent));
        if (recursive) {
            // also adopt at all children recursively
            for (int i = -1; i < parent.nrecs(); ++i) {
                btree_page_h child;
                PageID shpid_opaqueptr = i == -1 ? parent.get_foster_opaqueptr() : parent.child_opaqueptr(i);
                W_DO(child.fix_nonroot(parent, shpid_opaqueptr, LATCH_EX));
                W_DO(_sx_adopt_foster_all_core(child, false, true));
            }
        }

    }
    // after all adopts, if this parent is the root and has foster,
    // let's grow the tree
    if  (is_root && parent.get_foster()) {
        W_DO(_sx_grow_tree(parent));
        W_DO(_sx_adopt_foster_sweep(parent));
    }
    w_assert3(parent.is_consistent(true, true));
    return RCOK;
}
rc_t btree_impl::_sx_adopt_foster (btree_page_h &parent, btree_page_h &child) {
    w_keystr_t new_child_key;
    child.copy_fence_high_key(new_child_key);
    W_DO(_sx_split_if_needed(parent, new_child_key));

    // Now, another SSX to move the pointer
    sys_xct_section_t sxs(true);
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_adopt_foster_core(parent, child, new_child_key);
    W_DO (sxs.end_sys_xct (ret));

    DBG(<< "Adopted " << child.pid() << " into " << parent.pid());

    return ret;
}
rc_t btree_impl::_ux_adopt_foster_core (btree_page_h &parent, btree_page_h &child,
    const w_keystr_t &new_child_key)
{
    w_assert1 (g_xct()->is_single_log_sys_xct());
    w_assert1 (parent.is_fixed());
    w_assert1 (parent.latch_mode() == LATCH_EX);
    w_assert1 (parent.is_node());
    w_assert1 (child.is_fixed());
    w_assert1 (child.latch_mode() == LATCH_EX);
    w_assert0 (child.get_foster() != 0);

    PageID new_child_pid = child.get_foster();
    if (smlevel_0::bf->is_swizzled_pointer(new_child_pid)) {
        smlevel_0::bf->unswizzle(parent.get_generic_page(),
                GeneralRecordIds::FOSTER_CHILD, true, &new_child_pid);
    }
    w_assert1(!smlevel_0::bf->is_swizzled_pointer(new_child_pid));

    lsn_t child_emlsn = child.get_foster_emlsn();
    W_DO(log_btree_foster_adopt (parent, child, new_child_pid, child_emlsn, new_child_key));
    _ux_adopt_foster_apply_parent (parent, new_child_pid, child_emlsn, new_child_key);
    _ux_adopt_foster_apply_child (child);

    // Switch parent of newly adopted child
    // CS TODO: I'm not sure we can do this because we don't hold a latch on new_child_pid
    smlevel_0::bf->switch_parent(new_child_pid, parent.get_generic_page());

    w_assert3(parent.is_consistent(true, true));
    w_assert3(child.is_consistent(true, true));
    return RCOK;
}

rc_t btree_impl::_sx_opportunistic_adopt_foster (btree_page_h &parent,
                                                      btree_page_h &child, bool &pushedup)
{
    w_assert1 (parent.is_fixed());
    w_assert1 (parent.is_node());
    w_assert1 (child.is_fixed());

    pushedup = false;
    // let's try upgrading parent to EX latch. This highly likely fails in high-load situation,
    // so let's do it here to avoid system transaction creation cost.
    // we start from parent because EX latch on child is assured to be available in this order
    if (!parent.upgrade_latch_conditional()) {
        DBGOUT1(<< "opportunistic_adopt gave it up because of parent. "
                << parent.pid() << ". do nothing.");
        increase_ex_need(parent.pid()); // give a hint to subsequent accesses
        return RCOK;
    }

    // okay, got EX latch on parent. So, we can eventually get EX latch on child always.
    // do EX-acquire, not upgrade. As we have Ex latch on parent. this is assured to work eventually
    PageID surely_need_child_pid = child.pid();
    child.unfix(); // he will be also done in the following loop. okay to unfix here because:
    pushedup = true; // this assures the caller will immediatelly restart any access from root

    // this is a VERY good chance. So, why not sweep all (but a few unlucky execptions)
    // foster-children.
    W_DO(_sx_adopt_foster_sweep_approximate(parent, surely_need_child_pid));
    // note, this function might switch parent upon its split.
    // so, the caller is really responsible to restart search on seeing pushedup == true
    return RCOK;
}

rc_t btree_impl::_sx_adopt_foster_sweep_approximate (btree_page_h &parent,
                                                             PageID surely_need_child_pid)
{
    w_assert1 (parent.is_fixed());
    w_assert1 (parent.latch_mode() == LATCH_EX);
    w_assert1 (parent.is_node());
    while (true) {
        clear_ex_need(parent.pid()); // after these adopts, we don't need to be eager on this page.
        for (slotid_t i = -1; i < parent.nrecs(); ++i) {
            PageID shpid = i == -1 ? parent.pid0() : parent.child(i);
            PageID shpid_opaqueptr = i == -1 ? parent.pid0_opaqueptr() : parent.child_opaqueptr(i);
            if (shpid != surely_need_child_pid && get_expected_childrens(shpid) == 0) {
                continue; // then doesn't matter (this could be false in low probability, but it's fine)
            }
            btree_page_h child;
            // CS TODO: modified method to only try this if ifx is not a miss; otherwise,
            // some pages will be fetched and replaced just to see that they don't have
            // anything to adopt -- i.e., this "opportunistic" adopt is actually very
            // disturbing
            rc_t rc = child.fix_nonroot(parent, shpid_opaqueptr, LATCH_EX, true /*conditional*/,
                                        false /*virgin_page*/, true /* only_if_hit */);
            // if we can't instantly get latch, just skip it. we can defer it arbitrary
            if (rc.is_error()) {
                continue;
            }
            if (child.get_foster() == 0) {
                continue; // no foster. just ignore
            }
            W_DO(_sx_adopt_foster (parent, child));
        }
        // go on to foster-child of this parent, if exists
        if (parent.get_foster() == 0) {
            break;
        }
        btree_page_h foster_p;
        W_DO(foster_p.fix_nonroot(parent, parent.get_foster_opaqueptr(), LATCH_EX,
                                  false /*conditional*/, false /*virgin_page*/));// latch coupling
        parent.unfix();
        parent = foster_p;
    }
    parent.unfix(); // unfix right now. Some one might be waiting for us
    return RCOK;
}

rc_t btree_impl::_sx_adopt_foster_sweep (btree_page_h &parent_arg)
{
    btree_page_h parent = parent_arg; // because it might be switched below
    w_assert1 (parent.is_fixed());
    w_assert1 (parent.latch_mode() == LATCH_EX);
    w_assert1 (parent.is_node());
    while (true) {
        for (slotid_t i = -1; i < parent.nrecs(); ++i) {
            PageID pid_opaqueptr = i == -1 ? parent.pid0_opaqueptr() : parent.child_opaqueptr(i);
            btree_page_h child;
            W_DO(child.fix_nonroot(parent, pid_opaqueptr, LATCH_SH));
            if (child.get_foster() == 0) {
                continue; // no foster. just ignore
            }
            // we need to push this up. so, let's re-get EX latch
            if (!child.upgrade_latch_conditional()) {
                continue; // then give up. no hurry.
            }
            W_DO(_sx_adopt_foster (parent, child));
        }
        // go on to foster-child of this parent, if exists
        if (parent.get_foster() == 0) {
            break;
        }
        btree_page_h foster_p;
        W_DO(foster_p.fix_nonroot(parent, parent.get_foster_opaqueptr(), LATCH_EX)); // latch coupling
        parent = foster_p;
    }
    return RCOK;
}

void btree_impl::_ux_adopt_foster_apply_parent (btree_page_h &parent,
    PageID new_child_pid, lsn_t new_child_emlsn, const w_keystr_t &new_child_key)
{
    w_assert1 (parent.is_fixed());
    w_assert1 (parent.latch_mode() == LATCH_EX);
    w_assert1 (parent.is_node());
    w_assert1 (parent.check_space_for_insert_node(new_child_key)); // otherwise split_and_adopt should have been called
    w_assert1 (parent.fence_contains(new_child_key));

    // where to insert?
    bool     key_found;
    slotid_t slot_to_insert;
    parent.search(new_child_key, key_found, slot_to_insert);
    w_assert1(!key_found);
    w_assert2 (slot_to_insert >= 0);
    w_assert2 (slot_to_insert <= parent.nrecs());

    // okay, do it!
    W_COERCE (parent.insert_node(new_child_key, slot_to_insert,
                                 new_child_pid, new_child_emlsn));

    // DBG(<< "AFTER ADOPTION " << parent);
}
void btree_impl::_ux_adopt_foster_apply_child (btree_page_h &child)
{
    w_assert1 (child.is_fixed());
    w_assert1 (child.latch_mode() == LATCH_EX);
    // just clears up foster and chain-fence-high
    // note, chain-fence-high's string data is left there, but it does no harm
    child.page()->btree_foster = 0;
    child.page()->btree_chain_fence_high_length = 0;
    clear_forster_child(child.pid()); // give hint to subsequent accesses
}
