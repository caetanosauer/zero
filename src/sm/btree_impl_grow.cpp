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

#include "sm_int_2.h"
#include "sm_base.h"
#include "btree_p.h"
#include "btree_impl.h"
#include "crash.h"
#include "w_key.h"
#include "xct.h"
#include "page_bf_inline.h"

rc_t btree_impl::_sx_create_tree(const stid_t &stid, lpid_t &root_pid)
{
    FUNC(btree_impl::_sx_create_tree);
    W_DO( io_m::sx_alloc_a_page(stid, root_pid)); // allocate a root page as separate ssx
    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_create_tree_core(stid, root_pid);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}
rc_t btree_impl::_ux_create_tree_core(const stid_t &stid, const lpid_t &root_pid)
{
    w_assert1(root_pid.page != 0);
    btree_p page;
    // Format/init the page
    w_keystr_t infimum, supremum, dummy_chain_high; // empty fence keys=infimum-supremum
    infimum.construct_neginfkey();
    w_assert1(infimum.is_constructed());
    supremum.construct_posinfkey();
    w_assert1(supremum.is_constructed());
    W_DO(page.init_fix_steal(NULL, root_pid, root_pid.page,
        1, // level=1. initial tree has only one level
        0, // no child
        0, // no b-link page
        infimum, supremum, dummy_chain_high // start from infimum/supremum fence keys
    ));

    // also register it in stnode_page
    W_DO(io->set_root(stid, root_pid.page));

    return RCOK;
}

rc_t
btree_impl::_sx_shrink_tree(btree_p& rp)
{
    FUNC(btree_impl::_sx_shrink_tree);
    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_shrink_tree_core(rp);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}

rc_t
btree_impl::_ux_shrink_tree_core(btree_p& rp)
{
    w_assert1 (xct()->is_sys_xct());
    INC_TSTAT(bt_shrinks);

    w_assert3( rp.is_fixed());
    w_assert3( rp.latch_mode() == LATCH_EX);
    lpid_t rp_pid = rp.pid();
    
    if( rp.nrecs() > 0 || rp.get_foster() != 0) {
        // then still not the time for shrink
        W_DO (_ux_adopt_foster_all_core (rp, true, false));
        return RCOK;
    }
    w_assert1( rp.nrecs() == 0 && rp.get_foster() == 0);

    if (rp.pid0() != 0)  {
        //  The root has pid0. Copy child page over parent,
        //  and free child page.
        btree_p cp;
        W_DO( cp.fix_nonroot(rp, rp.vol(), rp.pid0(), LATCH_EX));

        // steal all from child
        w_keystr_t fence_low, fence_high, dummy_chain_high;
        cp.copy_fence_low_key(fence_low);
        cp.copy_fence_high_key(fence_high);
        cp.copy_chain_fence_high_key(dummy_chain_high);
        W_DO( rp.format_steal(rp_pid, rp_pid.page, // root page id is not changed.
            cp.level(), // one level shorter
            cp.pid().page, // left-most is cp's left-most
            cp.get_foster(), // foster is cp's foster
            fence_low, fence_high, dummy_chain_high,
            true, // log it to avoid write-order dependency. anyway it's very rare!
            &cp, 0, cp.nrecs()));
    
        w_assert3( cp.latch_mode() == LATCH_EX);
        W_DO( cp.set_tobedeleted(true)); // delete the page
    } else {
        // even pid0 doesn't exist. this is now an empty tree.
        w_keystr_t infimum, supremum, dummy_chain_high;
        infimum.construct_neginfkey();
        supremum.construct_posinfkey();
        W_DO( rp.format_steal(rp_pid, rp_pid.page, // root page id is not changed.
            1, // root is now leaf
            0, // leaf has no pid0
            0, // no foster
            infimum, supremum, dummy_chain_high // empty fence keys=infimum-supremum
            ) ); // nothing to steal
    }
    return RCOK;
}


rc_t
btree_impl::_sx_grow_tree(btree_p& rp)
{
    FUNC(btree_impl::_sx_grow_tree);
    lpid_t new_pid;
    W_DO(io_m::sx_alloc_a_page (rp.pid().stid(), new_pid)); // allocate a page as separate system transaction
    sys_xct_section_t sxs;
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_grow_tree_core(rp, new_pid);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}

rc_t
btree_impl::_ux_grow_tree_core(btree_p& rp, const lpid_t &cp_pid)
{
    w_assert1 (xct()->is_sys_xct());
    FUNC(btree_impl::_sx_grow_tree);
    INC_TSTAT(bt_grows);
    
    w_assert1(rp.latch_mode() == LATCH_EX);
    w_assert1(rp.is_fence_low_infimum()); // this should be left-most.

    if (rp.get_foster () == 0) {
        return RCOK; // other concurrent thread might have done it
    }
#if W_DEBUG_LEVEL > 0
    cout << "TREE grow" <<endl;
#endif 

    // create a new page that will take over all entries currently in the root.
    // this page will be the left-most child (pid0) of the root
    w_keystr_t cp_fence_low, cp_fence_high, cp_chain_high;
    rp.copy_fence_low_key(cp_fence_low);
    rp.copy_fence_high_key(cp_fence_high);
    rp.copy_chain_fence_high_key(cp_chain_high);

    btree_p cp;
    W_DO (cp.init_fix_steal(&rp, cp_pid, rp.pid().page, rp.level(), rp.pid0(), // copy pid0 of root too
        rp.get_foster(),
        cp_fence_low, cp_fence_high, cp_chain_high, // use current root's fence keys
        &rp, 0, rp.nrecs() // steal everything from root
    ));

    // now rp is empty, so we can call format_steal() to re-set the fence keys.
    w_keystr_t infimum, supremum, dummy_chain_high;
    infimum.construct_neginfkey();
    supremum.construct_posinfkey();
    W_DO( rp.format_steal(rp.pid(), rp.pid().page, // root page id is not changed.
        rp.level() + 1, // grow one level
        cp.pid().page, // left-most is cp
        0, // no foster
        infimum, supremum, dummy_chain_high // empty fence keys=infimum-supremum
        ) ); // nothing to steal
    
    w_assert3(cp.is_consistent(true, true));
    cp.unfix();
    
    // that's it. then, we adopt keys to the new root page later
    w_assert3(rp.is_consistent(true, true));

    return RCOK;
}

