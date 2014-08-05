/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/**
 * Implementation of insert/remove/alloc functions in btree_impl.h.
 * Other functions are defined in btree_impl_xxx.cpp.
 */

#define SM_SOURCE
#define BTREE_C

#include "sm_int_2.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "btcursor.h"
#include "crash.h"
#include "xct.h"
#include "lock_s.h"
#include <vector>
#include "restart.h"


rc_t
btree_impl::_ux_insert(
    volid_t vol, snum_t store,
    const w_keystr_t&    key,
    const cvec_t&        el)
{
    FUNC(btree_impl::_ux_insert);
    INC_TSTAT(bt_insert_cnt);
    while (true) {
        rc_t rc = _ux_insert_core (vol, store, key, el);
        if (rc.is_error() && rc.err_num() == eLOCKRETRY) {
            continue;
        }
        return rc;
    }
    return RCOK;
}
rc_t
btree_impl::_ux_insert_core(
    volid_t vol, snum_t store,
    const w_keystr_t&    key,
    const cvec_t&        el)
{

    // find the leaf (potentially) containing the key
    btree_page_h       leaf;
    W_DO( _ux_traverse(vol, store, key, t_fence_contain, LATCH_EX, leaf));
    w_assert1( leaf.is_fixed());
    w_assert1( leaf.is_leaf());
    w_assert1( leaf.latch_mode() == LATCH_EX);

    bool need_lock = g_xct_does_need_lock();

    // check if the same key already exists
    slotid_t slot;
    bool found;
    leaf.search(key, found, slot);
    bool alreay_took_XN = false;
    if (found) {
        // found! then we just lock the key (XN)
        if (need_lock) {
            W_DO (_ux_lock_key(leaf, key, LATCH_EX, create_part_okvl(okvl_mode::X, key), false));
            alreay_took_XN = true;
        }

        bool is_ghost = leaf.is_ghost(slot);

        //If the same key exists and non-ghost, exit with error (duplicate).
        if (!is_ghost) {
            return RC(eDUPLICATE);
        }

        // if the ghost record is enough spacious, we can just reuse it
        if (leaf._is_enough_spacious_ghost (key, slot, el)) {
            W_DO(leaf.replace_ghost(key, el));
            return RCOK;
        }
    }

    // then, we need to create (or expand) a ghost record for this key as a preparation to insert.
    // first, make sure this page is enough spacious (a bit conservative test).
    while (!leaf.check_space_for_insert_leaf(key, el)
        || (leaf.is_insertion_extremely_skewed_right() && leaf.check_chance_for_norecord_split(key))) {
        // See if there's room for the insert.
        // here, we start system transaction to split page
        lpid_t new_page_id;
        W_DO( _sx_split_foster(leaf, new_page_id, key) );

        // after split, should the old page contain the new tuple?
        if (!leaf.fence_contains(key)) {
            // if not, we should now insert to the new page.
            // because "leaf" is EX locked beforehand, no one
            // can have any latch on the new page, so we can always get this latch
            btree_page_h another_leaf; // latch coupling
            w_assert1(leaf.get_foster() == new_page_id.page);
            W_DO( another_leaf.fix_nonroot(leaf, leaf.vol(), new_page_id.page, LATCH_EX));
            w_assert1(another_leaf.is_fixed());
            w_assert2(another_leaf.fence_contains(key));
            leaf = another_leaf;
            w_assert2( leaf.is_fixed());
        }
    } // check for need to split

    // now we are sure the current page is enough spacious in any cases
    if (!found) {
        // corresponding ghost record didn't exist even before split.
        // so, it surely doesn't exist. we just create a new ghost record
        // by system transaction.

        if (need_lock) {
            W_DO(_ux_lock_range(leaf, key, -1, // search again because it might be split
                LATCH_EX, create_part_okvl(okvl_mode::X, key), ALL_N_GAP_X, true)); // this lock "goes away" once it's taken
        }

        // do it in one-go
// TODO(Restart)...        
DBGOUT3( << "&&&& Log for regular insertion, key: " << key);  

        W_DO(log_btree_insert_nonghost(leaf, key, el));
        leaf.insert_nonghost(key, el);
        // W_DO (_sx_reserve_ghost(leaf, key, el.size()));
    }

    // now we know the page has the desired ghost record. let's just replace it.
    if (need_lock && !alreay_took_XN) { // if "expand" case, do not need to get XN again
        W_DO (_ux_lock_key(leaf, key, LATCH_EX, create_part_okvl(okvl_mode::X, key), false));
    }
    if (found) {
        W_DO(leaf.replace_ghost(key, el));
    }

    return RCOK;
}

rc_t
btree_impl::_ux_put(
    volid_t vol, snum_t store,
    const w_keystr_t&    key,
    const cvec_t&        el)
{
    while (true) {
        rc_t rc = _ux_put_core (vol, store, key, el);
        if (rc.is_error() && rc.err_num() == eLOCKRETRY) {
            continue;
        }
        return rc;
    }
    return RCOK;
}
rc_t
btree_impl::_ux_put_core(
    volid_t vol, snum_t store,
    const w_keystr_t&    key,
    const cvec_t&        el)
{
    bool need_lock;
    slotid_t slot;
    bool found;
    bool took_XN;
    bool is_ghost;
    btree_page_h leaf;
    W_DO(_ux_get_page_and_status(vol, store, key, need_lock, slot, found, took_XN, is_ghost, leaf));
    if (!found || is_ghost) {
        return _ux_insert_core_tail(vol, store, key, el, need_lock, slot, found, took_XN, is_ghost, leaf);
    } else {
        return _ux_update_core_tail(vol, store, key, el, need_lock, slot, found, is_ghost, leaf);
    }
}


rc_t
btree_impl::_ux_get_page_and_status(volid_t vol, snum_t store,
                        const w_keystr_t& key,
                                    bool& need_lock, slotid_t& slot, bool& found, bool& took_XN,
                                    bool& is_ghost, btree_page_h& leaf) {

    // find the leaf (potentially) containing the key
    // passed in...btree_page_h       leaf;
    W_DO( _ux_traverse(vol, store, key, t_fence_contain, LATCH_EX, leaf));
    w_assert1( leaf.is_fixed());
    w_assert1( leaf.is_leaf());
    w_assert1( leaf.latch_mode() == LATCH_EX);

    // passed in...bool need_lock
    need_lock = g_xct_does_need_lock();

    // check if the same key already exists
    // passed in...slotid_t slot;
    // passed in...bool found;
    leaf.search(key, found, slot);
    // passed in...bool alreay_took_XN
    took_XN = false;
    if (found) {
        // found! then we just lock the key (XN)
        if (need_lock) {
            W_DO (_ux_lock_key(leaf, key, LATCH_EX, ALL_X_GAP_N, false));
            took_XN = true;
        }

        is_ghost = leaf.is_ghost(slot);
    }
    return RCOK;
}
rc_t btree_impl::_ux_insert_core_tail(volid_t /* vol */, snum_t /* store */,
                                      const w_keystr_t& key, const cvec_t& el,
                                      bool& need_lock, slotid_t& slot, bool& found,
                                      bool& alreay_took_XN, bool& is_ghost, btree_page_h& leaf) {
    if (found) {

        //If the same key exists and non-ghost, exit with error (duplicate).
        if (!is_ghost) {
            return RC(eDUPLICATE);
        }

        // if the ghost record is enough spacious, we can just reuse it
        if (leaf._is_enough_spacious_ghost (key, slot, el)) {
            W_DO(leaf.replace_ghost(key, el));
            return RCOK;
        }
    }

    // then, we need to create (or expand) a ghost record for this key as a preparation to insert.
    // first, make sure this page is enough spacious (a bit conservative test).
    while (!leaf.check_space_for_insert_leaf(key, el)
        || (leaf.is_insertion_extremely_skewed_right() && leaf.check_chance_for_norecord_split(key))) {
        // See if there's room for the insert.
        // here, we start system transaction to split page
        lpid_t new_page_id;
        W_DO( _sx_split_foster(leaf, new_page_id, key) );

        // after split, should the old page contain the new tuple?
        if (!leaf.fence_contains(key)) {
            // if not, we should now insert to the new page.
            // because "leaf" is EX locked beforehand, no one
            // can have any latch on the new page, so we can always get this latch
            btree_page_h another_leaf; // latch coupling
            W_DO( another_leaf.fix_nonroot(leaf, leaf.vol(), new_page_id.page, LATCH_EX) );
            w_assert1(another_leaf.is_fixed());
            w_assert2(another_leaf.fence_contains(key));
            leaf = another_leaf;
            w_assert2( leaf.is_fixed());
        }
    } // check for need to split

    // now we are sure the current page is enough spacious in any cases
    if (!found) {
        // corresponding ghost record didn't exist even before split.
        // so, it surely doesn't exist. we just create a new ghost record
        // by system transaction.

        if (need_lock) {
            W_DO(_ux_lock_range(leaf, key, -1, // search again because it might be split
                LATCH_EX, create_part_okvl(okvl_mode::X, key), ALL_N_GAP_X, true)); // this lock "goes away" once it's taken
        }

        W_DO (_sx_reserve_ghost(leaf, key, el.size()));
    }

    // now we know the page has the desired ghost record. let's just replace it.
    if (need_lock && !alreay_took_XN) { // if "expand" case, do not need to get XN again
        W_DO (_ux_lock_key(leaf, key, LATCH_EX, create_part_okvl(okvl_mode::X, key), false));
    }
    W_DO(leaf.replace_ghost(key, el));

    return RCOK;
}


rc_t btree_impl::_sx_reserve_ghost(btree_page_h &leaf, const w_keystr_t &key, int elem_len)
{
    FUNC(btree_impl::_sx_reserve_ghost);
    sys_xct_section_t sxs (true); // this transaction will output only one log!
    W_DO(sxs.check_error_on_start());
    rc_t ret = _ux_reserve_ghost_core(leaf, key, elem_len);
    W_DO (sxs.end_sys_xct (ret));
    return ret;
}

rc_t btree_impl::_ux_reserve_ghost_core(btree_page_h &leaf, const w_keystr_t &key, int elem_len) {
    w_assert1 (xct()->is_sys_xct());
    w_assert1 (leaf.fence_contains(key));

    w_assert1(leaf.check_space_for_insert_leaf(key.get_length_as_keystr()-leaf.get_prefix_length(), elem_len));

    W_DO (log_btree_ghost_reserve (leaf, key, elem_len));
    leaf.reserve_ghost(key, elem_len);
    return RCOK;
}

rc_t
btree_impl::_ux_update(volid_t vol, snum_t store, const w_keystr_t &key, const cvec_t &el, const bool undo)
{
    while (true) {
        rc_t rc = _ux_update_core (vol, store, key, el, undo);
        if (rc.is_error() && rc.err_num() == eLOCKRETRY) {
            continue;
        }
        return rc;
    }
    return RCOK;
}

rc_t
btree_impl::_ux_update_core(volid_t vol, snum_t store, const w_keystr_t &key, const cvec_t &el, const bool undo)
{
    bool need_lock = g_xct_does_need_lock();
    btree_page_h         leaf;

    // find the leaf (potentially) containing the key
    W_DO( _ux_traverse(vol, store, key, t_fence_contain, LATCH_EX, leaf, true /*allow retry*/, undo /*from undo*/));

    w_assert3(leaf.is_fixed());
    w_assert3(leaf.is_leaf());

    slotid_t       slot = -1;
    bool            found = false;
    leaf.search(key, found, slot);

    if(!found) {
        if (need_lock) {
            // re-latch mode is SH because this is "not-found" case.
            W_DO(_ux_lock_range(leaf, key, slot,
                        LATCH_SH, create_part_okvl(okvl_mode::X, key), ALL_N_GAP_S, false));
        }
        return RC(eNOTFOUND);
    }

    // it's found (whether it's ghost or not)! so, let's just
    // lock the key.
    if (need_lock) {
        // only the key is locked (XN)
        W_DO (_ux_lock_key(leaf, key, LATCH_EX, create_part_okvl(okvl_mode::X, key), false));
    }

    // get the old data and log
    bool ghost;
    smsize_t old_element_len;
    const char *old_element = leaf.element(slot, old_element_len, ghost);
    // it might be ghost..
    if (ghost) {
        return RC(eNOTFOUND);
    }

    // are we expanding?
    if (old_element_len < el.size()) {
        if (!leaf.check_space_for_insert_leaf(key, el)) {
            // this page needs split. As this is a rare case,
            // we just call remove and then insert to simplify the code
            W_DO(_ux_remove(vol, store, key, undo));
            W_DO(_ux_insert(vol, store, key, el));
            return RCOK;
        }
    }

    W_DO(log_btree_update (leaf, key, old_element, old_element_len, el));

    W_DO(leaf.replace_el_nolog(slot, el));
    return RCOK;
}

rc_t
btree_impl::_ux_update_core_tail(volid_t vol, snum_t store,
                                 const w_keystr_t &key, const cvec_t &el,
                                 bool& need_lock, slotid_t& slot, bool& found, bool& is_ghost,
                                 btree_page_h& leaf) {
    if(!found) {
        if (need_lock) {
            // re-latch mode is SH because this is "not-found" case.
            W_DO(_ux_lock_range(leaf, key, slot,
                                LATCH_SH, create_part_okvl(okvl_mode::X, key), ALL_N_GAP_S, false));
        }
        return RC(eNOTFOUND);
    }

    // it might be ghost..
    if (is_ghost) {
        return RC(eNOTFOUND);
    }
    smsize_t old_element_len;
    const char *old_element = leaf.element(slot, old_element_len, is_ghost);

    // are we expanding?
    if (old_element_len < el.size()) {
        if (!leaf.check_space_for_insert_leaf(key, el)) {
            // this page needs split. As this is a rare case,
            // we just call remove and then insert to simplify the code
            W_DO(_ux_remove(vol, store, key, false));  // Not from UNDO
            W_DO(_ux_insert(vol, store, key, el));
            return RCOK;
        }
    }

    W_DO(log_btree_update (leaf, key, old_element, old_element_len, el));

    W_DO(leaf.replace_el_nolog(slot, el));
    return RCOK;
}

rc_t btree_impl::_ux_overwrite(
        volid_t vol, snum_t store,
        const w_keystr_t&                 key,
        const char *el, smsize_t offset, smsize_t elen,
        const bool undo)
{
    while (true) {
        rc_t rc = _ux_overwrite_core (vol, store, key, el, offset, elen, undo);
        if (rc.is_error() && rc.err_num() == eLOCKRETRY) {
            continue;
        }
        return rc;
    }
    return RCOK;
}

rc_t btree_impl::_ux_overwrite_core(
        volid_t vol, snum_t store,
        const w_keystr_t& key,
        const char *el, smsize_t offset, smsize_t elen, const bool undo)
{
    // basically same as ux_update
    bool need_lock = g_xct_does_need_lock();
    btree_page_h leaf;

    W_DO( _ux_traverse(vol, store, key, t_fence_contain, LATCH_EX, leaf, true /*allow retry*/, undo /*from undo*/));

    w_assert3(leaf.is_fixed());
    w_assert3(leaf.is_leaf());

    slotid_t slot  = -1;
    bool     found = false;
    leaf.search(key, found, slot);

    if(!found) {
        if (need_lock) {
            W_DO(_ux_lock_range(leaf, key, slot,
                                LATCH_SH, create_part_okvl(okvl_mode::X, key), ALL_N_GAP_S, false));
        }
        return RC(eNOTFOUND);
    }

    if (need_lock) {
        W_DO (_ux_lock_key(leaf, key, LATCH_EX, create_part_okvl(okvl_mode::X, key), false));
    }

    // get the old data and log
    bool ghost;
    smsize_t old_element_len;
    const char *old_element = leaf.element(slot, old_element_len, ghost);
    if (ghost) {
        return RC(eNOTFOUND);
    }
    if (old_element_len < offset + elen) {
        return RC(eRECWONTFIT);
    }

    W_DO(log_btree_overwrite (leaf, key, old_element, el, offset, elen));
    leaf.overwrite_el_nolog(slot, offset, el, elen);
    return RCOK;
}

rc_t
btree_impl::_ux_remove(volid_t vol, snum_t store, const w_keystr_t &key, const bool undo)
{
    FUNC(btree_impl::_ux_remove);
    INC_TSTAT(bt_remove_cnt);
    while (true) {
        rc_t rc = _ux_remove_core (vol, store, key, undo);
        if (rc.is_error() && rc.err_num() == eLOCKRETRY) {
            continue;
        }
        return rc;
    }
    return RCOK;
}

rc_t
btree_impl::_ux_remove_core(volid_t vol, snum_t store, const w_keystr_t &key, const bool undo)
{
    bool need_lock = g_xct_does_need_lock();
    btree_page_h         leaf;

    // find the leaf (potentially) containing the key
    W_DO( _ux_traverse(vol, store, key, t_fence_contain, LATCH_EX, leaf, true /*allow retry*/, undo /*from undo*/));

    w_assert3(leaf.is_fixed());
    w_assert3(leaf.is_leaf());

    slotid_t       slot = -1;
    bool            found = false;
    leaf.search(key, found, slot);

    if(!found) {
        if (need_lock) {
            // re-latch mode is SH because this is "not-found" case.
            W_DO(_ux_lock_range(leaf, key, slot,
                        LATCH_SH, create_part_okvl(okvl_mode::X, key), ALL_N_GAP_S, false));
        }
        return RC(eNOTFOUND);
    }

    // it's found (whether it's ghost or not)! so, let's just
    // lock the key.
    if (need_lock) {
        // only the key is locked (XN)
        W_DO (_ux_lock_key(leaf, key, LATCH_EX, create_part_okvl(okvl_mode::X, key), false));
    }

    // it might be already ghost..
    if (leaf.is_ghost(slot)) 
    {
        return RC(eNOTFOUND);
    }
    else
    {
// TODO(Restart)... 
DBGOUT3( << "&&&& Log for deletion, key: " << key);
    
        // log first
        vector<slotid_t> slots;
        slots.push_back(slot);
        W_DO(log_btree_ghost_mark (leaf, slots));

        // then mark it as ghost
        leaf.mark_ghost (slot);
    }
    return RCOK;
}

rc_t
btree_impl::_ux_undo_ghost_mark(volid_t vol, snum_t store, const w_keystr_t &key)
{
    FUNC(btree_impl::_ux_undo_ghost_mark);
    w_assert1(key.is_regular());
    btree_page_h         leaf;
    W_DO( _ux_traverse(vol, store, key, t_fence_contain, LATCH_EX, leaf, true/*allow retry*/, true /*from undo*/));
    w_assert3(leaf.is_fixed());
    w_assert3(leaf.is_leaf());

    slotid_t       slot = -1;
    bool            found = false;
    leaf.search(key, found, slot);

    if(!found) {
        return RC(eNOTFOUND);
    }

    // Undo a remove (delete operation), which becomes an insert
    // the ghost (deleted) record is available in page, so we only need to unmark the ghost
    
    // The undo operation (compensation) is used for 1) transaction abort, 2) recovery undo,
    // generate an insert log record for the operation so the REDO phase
    // handles the txn abort behavior correctly

    // Get the existing data for logging purpose, because this is a transaction abort, proper lock
    // should be held and the original record should still be intact
    bool ghost;
    smsize_t existing_element_len;
    const char *existing_element = leaf.element(slot, existing_element_len, ghost);
    cvec_t el (existing_element, existing_element_len);

    W_DO(log_btree_insert_nonghost(leaf, key, el));

    leaf.unmark_ghost (slot);
    return RCOK;
}

okvl_mode btree_impl::create_part_okvl(
    okvl_mode::element_lock_mode mode,
    const w_keystr_t &key) {
    okvl_mode ret;

    okvl_mode::part_id part = 0;
    if (OKVL_EXPERIMENT) {
        //Use the uniquefier part
        if (OKVL_INIT_STR_UNIQUEFIER_LEN != 0) {
            w_assert1(key.get_length_as_keystr() >= OKVL_INIT_STR_UNIQUEFIER_LEN);
            const char* uniquefier = reinterpret_cast<const char*>(key.buffer_as_keystr());
            uniquefier += key.get_length_as_keystr();
            uniquefier -= OKVL_INIT_STR_UNIQUEFIER_LEN;
            part = okvl_mode::compute_part_id(uniquefier, OKVL_INIT_STR_UNIQUEFIER_LEN);
        }
    }
    ret.set_partition_mode(part, mode);
    return ret;
}

uint8_t btree_impl::s_ex_need_counts[1 << btree_impl::GAC_HASH_BITS];
uint8_t btree_impl::s_foster_children_counts[1 << btree_impl::GAC_HASH_BITS];
queue_based_lock_t btree_impl::s_ex_need_mutex[1 << GAC_HASH_BITS];
