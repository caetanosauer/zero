#include "w_defines.h"

/**
 * Implementation of split/propagate related functions in btree_impl.h.
 * Separated from btree_impl.cpp.
 */

#define SM_SOURCE
#define BTREE_C

#include "sm_int_2.h"
#include "btree_p.h"
#include "btree_impl.h"
#include "crash.h"
#include "vec_t.h"
#include "w_key.h"
#include "sm.h"
#include "xct.h"
#include "bf.h"

rc_t btree_impl::_sx_split_blink(btree_p &page, lpid_t &new_page_id, const w_keystr_t &triggering_key)
{
    FUNC(btree_impl::_sx_split_blink);
    W_DO(io_m::sx_alloc_a_page (page.pid().stid(), new_page_id)); // allocate a page as separate system transaction
    // start system transaction for this split
    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_split_blink_core(page, new_page_id, triggering_key);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}
rc_t btree_impl::_ux_split_blink_core(btree_p &page, const lpid_t &new_page_id, const w_keystr_t &triggering_key,
    const w_keystr_t *new_child_key, shpid_t new_child_pid)
{
    w_assert1 (xct()->is_sys_xct());
    w_assert1(page.latch_mode() == LATCH_EX);
    w_assert1((page.is_leaf() && new_child_key == NULL) || (page.is_node() && new_child_key != NULL));
    INC_TSTAT(bt_splits);

    w_keystr_t mid_key;
    slotid_t right_begins_from;
    page.suggest_fence_for_split(mid_key, right_begins_from, triggering_key); // get new fence key in the middle

    if (right_begins_from == page.nrecs()) {
#if W_DEBUG_LEVEL>2
        cout << "yay, it's no record-split! new pid=" << new_page_id << endl;
#endif // W_DEBUG_LEVEL>2
        
        // no-record split. this doesn't cause write-order dependency
        w_keystr_t chain_high, new_chain_high; // chain-high of parent/new_page
        if (page.get_chain_fence_high_length() == 0) {
            page.copy_fence_high_key(chain_high);
            // new_chain_high is empty
        } else {
            page.copy_chain_fence_high_key(chain_high);
            page.copy_chain_fence_high_key(new_chain_high);
        }
        w_keystr_t old_high;
        page.copy_fence_high_key(old_high);

        // these both log and apply
        // child is just making an empty page
        btree_p new_page;
        W_DO(new_page.init_fix_steal(new_page_id, page.btree_root(), page.level(), new_child_pid,
            page.get_blink(), mid_key, old_high, new_chain_high)); // nothing to steal
        // page is just setting new fence key and blink pointer
        w_assert1 (new_page.lsn().valid());
        W_DO(page.norecord_split(new_page_id.page, mid_key, chain_high));
        w_assert1 (page.lsn().valid());
    } else {
#if W_DEBUG_LEVEL>2
        cout << "usual split. new pid=" << new_page_id << endl;
#endif // W_DEBUG_LEVEL>2
        // usual split (if non-leaf, + adopt)
        W_DO(log_btree_blink_split (page, new_page_id.page, right_begins_from, new_child_key, new_child_pid));
        W_DO(_ux_split_blink_apply (page, right_begins_from, mid_key, new_page_id, new_child_key, new_child_pid));
    }
    increase_forster_child(page.pid().page); // give hint to subsequent accesses
    return RCOK;
}
rc_t btree_impl::_ux_split_blink_apply(btree_p &page,
    slotid_t right_begins_from, const w_keystr_t &mid_key, const lpid_t &new_pid,
    const w_keystr_t *new_child_key, shpid_t new_child_pid)
{
    w_assert1(page.latch_mode() == LATCH_EX);

    w_keystr_t low_key, high_key, chain_high_key;
    page.copy_fence_low_key(low_key);
    page.copy_fence_high_key(high_key);

    bool was_right_most = (page.get_chain_fence_high_length() == 0);
    page.copy_chain_fence_high_key(chain_high_key);
    if (was_right_most) {
        // this means there was no chain or the page was the right-most of it. (so it's high=high of chain)
        // upon the blink split, we start setting the chain-high
        page.copy_fence_high_key(chain_high_key);
    }

#if W_DEBUG_LEVEL > 3
    cout << "low=" << low_key << ", mid=" << mid_key << ", high=" << high_key << endl;
#endif //W_DEBUG_LEVEL

    // create a new page as a right sibling of the page, stealing half of entries    
    shpid_t new_pid0 = 0;
    int steal_from, steal_to;
    if (page.is_node()) {
        // if it's non-leaf, separator key's pointer becomes the pid0 of right page
        new_pid0 = page.child(right_begins_from);
        steal_from = right_begins_from + 1; // +1: so it's removed in this level
        steal_to = page.nrecs();
    } else {
        steal_from = right_begins_from;
        steal_to = page.nrecs();
    }

    // not fix(), but a special fix() for initial allocation of BTree page.
    btree_p new_page;
    w_keystr_t empty_key;
    W_DO(new_page.init_fix_steal(new_pid, page.btree_root(), page.level(), new_pid0,
        page.get_blink(), // the new page jumps b/w old and its b-link (if exists)
        mid_key, high_key, // [mid - high]
        was_right_most ? empty_key : chain_high_key, // if the left page was right-most, the new-page is the new right-most. so no chain-high
        &page, steal_from, steal_to,
        false // do NOT log it.
    ));
    W_DO (log_btree_noop(new_page));// just for updating the LSN of new_page
    // foster-parent should be written later because it's the data source
    page.set_dirty();
    new_page.set_dirty();
    W_DO (bf_m::register_write_order_dependency(page._pp, new_page._pp));

    w_assert3(new_page.is_consistent(true, false));
    w_assert1(new_page.is_fixed());
    w_assert1(new_page.latch_mode() == LATCH_EX);
    
    // next, we refactor the left page in a similar way. 
    // However, we can't use the "page" itself to construct "page".
    page_s scratch;
    ::memcpy (&scratch, page._pp, sizeof(scratch)); // thus get a copy
    btree_p scratch_p (&scratch, 0);
    W_DO(page.format_steal(scratch_p.pid(), scratch_p.btree_root(), scratch_p.level(), scratch_p.pid0(),
        new_page.pid().page,  // also set blink pointer to new page
        low_key, mid_key, chain_high_key, // mid_key is the new high key
        false, // don't log it
        &scratch_p, 0, right_begins_from
    ));
    page.set_lsns(scratch.lsn); // format_steal also clears LSN, so recover it from the copy

    if (page.is_node()) {
        // if this split is because of adopt, we also combine adopt, so insert the new record here.
        if (page.fence_contains(*new_child_key)) {
            W_DO(_ux_adopt_blink_apply_parent (page, new_child_pid, *new_child_key));
        } else {
            w_assert1 (new_page.fence_contains(*new_child_key));
            W_DO(_ux_adopt_blink_apply_parent (new_page, new_child_pid, *new_child_key));
        }
    }

    w_assert3(page.is_consistent(true, false));
    w_assert1(page.is_fixed());
    return RCOK;
}

rc_t btree_impl::_sx_adopt_blink_all (
    const lpid_t &root_pid, btree_p &parent, bool recursive)
{
    FUNC(btree_impl::_sx_adopt_blink_all);
    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_adopt_blink_all_core(root_pid, parent, recursive);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}
rc_t btree_impl::_ux_adopt_blink_all_core (
    const lpid_t &root_pid, btree_p &parent, bool recursive)
{
    // TODO this should use the improved tree-walk-through ticket:62
    w_assert1 (xct()->is_sys_xct());
    w_assert1 (parent.is_fixed());
    w_assert1 (parent.latch_mode() == LATCH_EX);
    if (parent.is_node()) {
        w_assert1(parent.pid0());
        W_DO(_ux_adopt_blink_sweep(parent));
        if (recursive) {
            // also adopt at all children recursively
            for (int i = -1; i < parent.nrecs(); ++i) {
                btree_p child;
                lpid_t pid = parent.pid();
                pid.page = i == -1 ? parent.pid0() : parent.child(i);
                W_DO(child.fix(pid, LATCH_EX));
                W_DO(_ux_adopt_blink_all_core(root_pid, child, true));
            }
        }

    }
    // after all adopts, if this parent is the root and has blink,
    // let's grow the tree
    if  (parent.pid() == root_pid && parent.get_blink()) {
        W_DO(_sx_grow_tree(parent));
        W_DO(_ux_adopt_blink_sweep(parent));
    }
    return RCOK;
}
rc_t btree_impl::_sx_adopt_blink (btree_p &parent, btree_p &child)
{
    FUNC(btree_impl::_sx_adopt_blink);
    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_adopt_blink_core(parent, child);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}
rc_t btree_impl::_ux_adopt_blink_core (btree_p &parent, btree_p &child)
{
    w_assert1 (xct()->is_sys_xct());
    w_assert1 (parent.is_fixed());
    w_assert1 (parent.latch_mode() == LATCH_EX);
    w_assert1 (parent.is_node());
    w_assert1 (child.is_fixed());
    w_assert1 (child.latch_mode() == LATCH_EX);
    w_assert0 (child.get_blink() != 0);
    w_keystr_t new_child_key;
    child.copy_fence_high_key(new_child_key);
    shpid_t new_child_pid = child.get_blink();
    if (!parent.check_space_for_insert_node(new_child_key)
        || (parent.is_insertion_extremely_skewed_right() && parent.check_chance_for_norecord_split(new_child_key))
    ) {
        // we need to split
        lpid_t new_parent_id;
        W_DO(io_m::sx_alloc_a_page (parent.pid().stid(), new_parent_id)); // allocate a page as separate system transaction
        // this combines adoption too
        W_DO (_ux_split_blink_core(parent, new_parent_id, new_child_key, &new_child_key, new_child_pid));

    } else {
        // no split needed
        W_DO(log_btree_blink_adopt_parent (parent, new_child_pid, new_child_key));
        W_DO(_ux_adopt_blink_apply_parent (parent, new_child_pid, new_child_key));
    }
    // then clear child's blink pointer and chain-high
    W_DO(log_btree_blink_adopt_child (child));
    _ux_adopt_blink_apply_child (child);
    return RCOK;
}
rc_t btree_impl::_sx_opportunistic_adopt_blink (btree_p &parent, btree_p &child, bool &pushedup)
{
    FUNC(btree_impl::_sx_opportunistic_adopt_blink);

    pushedup = false;
    // let's try upgrading parent to EX latch. This highly likely fails in high-load situation,
    // so let's do it here to avoid system transaction creation cost.
    // we start from parent because EX latch on child is assured to be available in this order
    bool would_block = false;
    W_DO(parent.upgrade_latch_if_not_block(would_block));
    if (would_block) {
#if W_DEBUG_LEVEL>1
        cout << "opportunistic_adopt gave it up because of parent. " << parent.pid() << ". do nothing." << endl;
#endif
        increase_ex_need(parent.pid().page); // give a hint to subsequent accesses
        return RCOK;
    }
    {
        sys_xct_section_t sxs;
        W_DO(sxs.check_error_on_start());
        rc_t ret = _ux_opportunistic_adopt_blink_core(parent, child, pushedup);
        W_DO (sxs.end_sys_xct (ret));
        return ret;
    }
}
rc_t btree_impl::_ux_opportunistic_adopt_blink_core (btree_p &parent, btree_p &child, bool &pushedup)
{
    w_assert1 (xct()->is_sys_xct());
    w_assert1 (parent.is_fixed());
    w_assert1 (parent.is_node());
    w_assert1 (child.is_fixed());
    // okay, got EX latch on parent. So, we can eventually get EX latch on child always.
    // do EX-acquire, not upgrade. As we have Ex latch on parent. this is assured to work eventually
    shpid_t surely_need_child_pid = child.pid().page;
    child.unfix(); // he will be also done in the following loop. okay to unfix here because:
    pushedup = true; // this assures the caller will immediatelly restart any access from root

    // this is a VERY good chance. So, why not sweep all (but a few unlucky execptions)
    // foster-children.
    W_DO(_ux_adopt_blink_sweep_approximate(parent, surely_need_child_pid));
    // note, this function might switch parent upon its split.
    // so, the caller is really responsible to restart search on seeing pushedup == true
    return RCOK;
}

rc_t btree_impl::_sx_adopt_blink_sweep_approximate (btree_p &parent, shpid_t surely_need_child_pid)
{
    FUNC(btree_impl::_sx_adopt_blink_sweep_approximate);
    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_adopt_blink_sweep_approximate(parent, surely_need_child_pid);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}
rc_t btree_impl::_ux_adopt_blink_sweep_approximate (btree_p &parent, shpid_t surely_need_child_pid)
{
    w_assert1 (xct()->is_sys_xct());
    w_assert1 (parent.is_fixed());
    w_assert1 (parent.latch_mode() == LATCH_EX);
    w_assert1 (parent.is_node());
    while (true) {
        clear_ex_need(parent.pid().page); // after these adopts, we don't need to be eager on this page.
        for (slotid_t i = -1; i < parent.nrecs(); ++i) {
            lpid_t pid = parent.pid();
            pid.page = i == -1 ? parent.pid0() : parent.child(i);
            if (pid.page != surely_need_child_pid && get_expected_childrens(pid.page) == 0) {
                continue; // then doesn't matter (this could be false in low probability, but it's fine)
            }
            btree_p child;
            rc_t rc = child.conditional_fix(pid, LATCH_EX);
            // if we can't instantly get latch, just skip it. we can defer it arbitrary
            if (rc.is_error()) {
                continue;
            }
            if (child.get_blink() == 0) {
                continue; // no blink. just ignore
            }
            W_DO(_ux_adopt_blink_core (parent, child));
        }
        // go on to foster-child of this parent, if exists
        if (parent.get_blink() == 0) {
            break;
        }
        lpid_t pid = parent.pid();
        pid.page = parent.get_blink();
        btree_p blink_p;
        W_DO(blink_p.fix(pid, LATCH_EX)); // latch coupling
        parent.unfix();
        parent = blink_p;
    }
    parent.unfix(); // unfix right now. Some one might be waiting for us
    return RCOK;
}


rc_t btree_impl::_ux_adopt_blink_sweep (btree_p &parent_arg)
{
    w_assert1 (xct()->is_sys_xct());
    btree_p parent = parent_arg; // because it might be switched below
    w_assert1 (parent.is_fixed());
    w_assert1 (parent.latch_mode() == LATCH_EX);
    w_assert1 (parent.is_node());
    while (true) {
        for (slotid_t i = -1; i < parent.nrecs(); ++i) {
            lpid_t pid = parent.pid();
            pid.page = i == -1 ? parent.pid0() : parent.child(i);
            btree_p child;
            W_DO(child.fix(pid, LATCH_SH));
            if (child.get_blink() == 0) {
                continue; // no blink. just ignore
            }
            // we need to push this up. so, let's re-get EX latch
            bool would_block = false;
            W_DO(child.upgrade_latch_if_not_block(would_block));
            if (would_block) {
                continue; // then give up. no hurry.
            }
            W_DO(_ux_adopt_blink_core (parent, child));
        }
        // go on to foster-child of this parent, if exists
        if (parent.get_blink() == 0) {
            break;
        }
        lpid_t pid = parent.pid();
        pid.page = parent.get_blink();
        btree_p blink_p;
        W_DO(blink_p.fix(pid, LATCH_EX)); // latch coupling
        parent = blink_p;
    }
    return RCOK;
}

rc_t btree_impl::_ux_adopt_blink_apply_parent (btree_p &parent,
    shpid_t new_child_pid, const w_keystr_t &new_child_key)
{
    w_assert1 (parent.is_fixed());
    w_assert1 (parent.latch_mode() == LATCH_EX);
    w_assert1 (parent.is_node());
    w_assert1 (parent.check_space_for_insert_node(new_child_key)); // otherwise split_and_adopt should have been called
    w_assert1 (parent.fence_contains(new_child_key));

    // where to insert?
    slotid_t slot_to_insert;
    parent.search_node(new_child_key, slot_to_insert);
    // search_node returns the slot the key potentially belongs to.
    // we want to adopt this key AFTER the slot, so ++.
    ++slot_to_insert;
    w_assert2 (slot_to_insert >= 0);
    w_assert2 (slot_to_insert <= parent.nrecs());

    // okay, do it!
    W_DO (parent.insert_node(new_child_key, slot_to_insert, new_child_pid));
    return RCOK;
}
void btree_impl::_ux_adopt_blink_apply_child (btree_p &child)
{
    w_assert1 (child.is_fixed());
    w_assert1 (child.latch_mode() == LATCH_EX);
    // just clears up blink and chain-fence-high
    // note, chain-fence-high's string data is left there, but it does no harm
    child._pp->btree_blink = 0;
    child._pp->btree_chain_fence_high_length = 0;
    clear_forster_child(child.pid().page); // give hint to subsequent accesses
}
