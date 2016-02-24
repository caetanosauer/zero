/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/**
 * Implementation of tree grow/shrink related functions in btree_impl.h.
 * Separated from btree_impl.cpp.
 */

#define SM_SOURCE
#define BTREE_C

#include "sm_base.h"
#include "sm_base.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "crash.h"
#include "w_key.h"
#include "xct.h"
#include "vol.h"

rc_t btree_impl::_ux_create_tree_core(const StoreID& stid, const PageID& root_pid)
{
    w_assert1(root_pid != 0);
    w_assert1(stid != 0);
    btree_page_h page;
    // Format/init the page
    w_keystr_t infimum, supremum, dummy_chain_high; // empty fence keys=infimum-supremum
    infimum.construct_neginfkey();
    w_assert1(infimum.is_constructed());
    supremum.construct_posinfkey();
    w_assert1(supremum.is_constructed());
    W_DO(page.fix_root(stid, LATCH_EX, false, true));
    W_DO(page.format_steal(page.lsn(), root_pid, stid, root_pid,
                           1, // level=1. initial tree has only one level
                           0, lsn_t::null,// no pid0
                           0, lsn_t::null,// no foster child
                           infimum, supremum, dummy_chain_high // start from infimum/supremum fence keys
                           ));
    w_assert1(page.root() == page.pid());

    return RCOK;
}

rc_t
btree_impl::_sx_shrink_tree(btree_page_h& rp)
{
    if( rp.nrecs() > 0 || rp.get_foster() != 0) {
        // then still not the time for shrink
        W_DO (_sx_adopt_foster_all_core (rp, true, false));
        return RCOK;
    }

    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_shrink_tree_core(rp);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}

rc_t
btree_impl::_ux_shrink_tree_core(btree_page_h& rp)
{
    w_assert1 (xct()->is_sys_xct());
    INC_TSTAT(bt_shrinks);

    w_assert3( rp.is_fixed());
    w_assert3( rp.latch_mode() == LATCH_EX);
    PageID rp_pid = rp.pid();

    if( rp.nrecs() > 0 || rp.get_foster() != 0) {
        return RCOK; // just to make sure
    }
    w_assert1( rp.nrecs() == 0 && rp.get_foster() == 0);

    if (rp.pid0() != 0)  {
        //  The root has pid0. Copy child page over parent,
        //  and free child page.
        btree_page_h cp;
        W_DO( cp.fix_nonroot(rp, rp.pid0(), LATCH_EX));

        // steal all from child
        w_keystr_t fence_low, fence_high, dummy_chain_high;
        cp.copy_fence_low_key(fence_low);
        cp.copy_fence_high_key(fence_high);
        cp.copy_chain_fence_high_key(dummy_chain_high);
        W_DO(rp.format_steal(rp.lsn(), rp_pid, rp.store(), rp_pid, // root page id is not changed.
                             cp.level(), // one level shorter
                             cp.pid(), cp.lsn(), // left-most is cp's left-most
                             cp.get_foster(), cp.get_foster_emlsn(),// foster is cp's foster
                             fence_low, fence_high, dummy_chain_high,
                             true, // log it to avoid write-order dependency. anyway it's very rare!
                             &cp, 0, cp.nrecs()));

        w_assert3( cp.latch_mode() == LATCH_EX);
        W_DO( cp.set_to_be_deleted(true)); // delete the page
    } else {
        // even pid0 doesn't exist. this is now an empty tree.
        w_keystr_t infimum, supremum, dummy_chain_high;
        infimum.construct_neginfkey();
        supremum.construct_posinfkey();
        W_DO(rp.format_steal(rp.lsn(), rp_pid, rp.store(),
                             rp_pid, // root page id is not changed.
                             1, // root is now leaf
                             0, lsn_t::null, // leaf has no pid0
                             0, lsn_t::null, // no foster
                             infimum, supremum, dummy_chain_high // empty fence keys=infimum-supremum
                 )); // nothing to steal
    }
    return RCOK;
}


rc_t
btree_impl::_sx_grow_tree(btree_page_h& rp)
{
    PageID new_pid;
    // allocate a page as separate system transaction
    W_DO(smlevel_0::vol->alloc_a_page(new_pid));

    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());

    INC_TSTAT(bt_grows);

    w_assert1(rp.latch_mode() == LATCH_EX);
    w_assert1(rp.is_fence_low_infimum()); // this should be left-most.

    if (rp.get_foster () == 0) {
        // other concurrent thread might have done it
        W_DO(smlevel_0::vol->deallocate_page(new_pid));
        return RCOK;
    }
    DBGOUT1("TREE grow");

    // create a new page that will take over all entries currently in the root.
    // this page will be the left-most child (pid0) of the root
    w_keystr_t cp_fence_low, cp_fence_high, cp_chain_high;
    rp.copy_fence_low_key(cp_fence_low);
    rp.copy_fence_high_key(cp_fence_high);
    rp.copy_chain_fence_high_key(cp_chain_high);

    btree_page_h cp;
    W_DO(cp.fix_nonroot(rp, new_pid, LATCH_EX, false, true));
    W_DO(cp.format_steal(cp.lsn(), new_pid, rp.store(), rp.pid(), rp.level(),
        rp.pid0(), rp.get_pid0_emlsn(), // copy pid0 of root too
        rp.get_foster(), rp.get_foster_emlsn(),
                         cp_fence_low, cp_fence_high, cp_chain_high, // use current root's fence keys
                         true, // log it
                         &rp, 0, rp.nrecs() // steal everything from root
                         ));

    // now rp is empty, so we can call format_steal() to re-set the fence keys.
    w_keystr_t infimum, supremum, dummy_chain_high;
    infimum.construct_neginfkey();
    supremum.construct_posinfkey();
    W_DO(rp.format_steal(rp.lsn(), rp.pid(), rp.store(),
                         rp.pid(), // root page id is not changed.
                         rp.level() + 1, // grow one level
                         cp.pid(), cp.lsn(), // left-most is cp
                         0, lsn_t::null,// no foster
                            infimum, supremum, dummy_chain_high // empty fence keys=infimum-supremum
             )); // nothing to steal

    // If old root had any children, now they all have a wrong parent in the
    // buffer pool hash table. Therefore, we need to update it for each child
    int max_slot = cp.max_child_slot();
    for (general_recordid_t i = GeneralRecordIds::FOSTER_CHILD; i <= max_slot;
            ++i)
    {
        PageID shpid = *cp.child_slot_address(i);
        if ((shpid & SWIZZLED_PID_BIT) == 0) {
            smlevel_0::bf->switch_parent(shpid, cp.get_generic_page());
        }
        else {
            // CS TODO handle swizzled case
            w_assert0(false);
        }
    }

    w_assert3(cp.is_consistent(true, true));
    cp.unfix();

    // that's it. then, we adopt keys to the new root page later
    w_assert3(rp.is_consistent(true, true));

    W_DO (sxs.end_sys_xct (RCOK));
    return RCOK;
}

