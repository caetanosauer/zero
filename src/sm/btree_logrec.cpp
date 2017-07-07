/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

/**
 * Logging and its UNDO/REDO code for BTrees.
 * Separated from logrec.cpp.
 */

#include "logrec_support.h"
#include "vol.h"
#include "bf_tree_cb.h"
#include "btree_page_h.h"
#include "btree_impl.h"

template <class PagePtr>
void btree_foster_adopt_log::construct(const PagePtr /*p*/, const PagePtr p2,
    PageID new_child_pid, lsn_t new_child_emlsn, const w_keystr_t& new_child_key) {
    set_size((new (data_ssx()) btree_foster_adopt_t(
        p2->pid(), new_child_pid, new_child_emlsn, new_child_key))->size());
}

template <class PagePtr>
void btree_foster_adopt_log::redo(PagePtr p) {
    w_assert1(is_single_sys_xct());
    borrowed_btree_page_h bp(p);
    btree_foster_adopt_t *dp = reinterpret_cast<btree_foster_adopt_t*>(data_ssx());

    w_keystr_t new_child_key;
    new_child_key.construct_from_keystr(dp->_data, dp->_new_child_key_len);

    PageID target_pid = p->pid();
    DBGOUT3 (<< *this << " target_pid=" << target_pid << ", new_child_pid="
        << dp->_new_child_pid << ", new_child_key=" << new_child_key);
    if (target_pid == dp->_page2_pid) {
        // we are recovering "page2", which is real-child.
        w_assert0(target_pid == dp->_page2_pid);
        btree_impl::_ux_adopt_foster_apply_child(bp);
    } else {
        // we are recovering "page", which is real-parent.
        btree_impl::_ux_adopt_foster_apply_parent(bp, dp->_new_child_pid,
                                                  dp->_new_child_emlsn, new_child_key);
    }
}

template <class PagePtr>
void btree_split_log::construct(
        const PagePtr child_p,
        const PagePtr parent_p,
        uint16_t move_count,
        const w_keystr_t& new_high_fence,
        const w_keystr_t& new_chain
)
{
    // If you change this, please make according adjustments in
    // logrec_t::remove_info_for_pid
    btree_bulk_delete_t* bulk =
        new (data_ssx()) btree_bulk_delete_t(parent_p->pid(),
                    child_p->pid(), move_count,
                    new_high_fence, new_chain);
    page_img_format_t* format = new (data_ssx() + bulk->size())
        page_img_format_t(child_p->get_generic_page());

    // Logrec will have the child pid as main pid (i.e., destination page).
    // Parent pid is stored in btree_bulk_delete_t, which is a
    // multi_page_log_t (i.e., source page)
    set_size(bulk->size() + format->size());
}

template <class PagePtr>
void btree_split_log::redo(PagePtr p)
{
    btree_bulk_delete_t* bulk = (btree_bulk_delete_t*) data_ssx();
    page_img_format_t* format = (page_img_format_t*)
        (data_ssx() + bulk->size());

    if (p->pid() == bulk->new_foster_child) {
        // redoing the foster child
        format->apply(p->get_generic_page());
    }
    else {
        // redoing the foster parent
        borrowed_btree_page_h bp(p);
        w_assert1(bp.nrecs() > bulk->move_count);
        bp.delete_range(bp.nrecs() - bulk->move_count, bp.nrecs());

        w_keystr_t new_high_fence, new_chain;
        bulk->get_keys(new_high_fence, new_chain);

        bp.set_foster_child(bulk->new_foster_child, new_high_fence, new_chain);
    }
}

template void btree_split_log::template construct<btree_page_h*>(
        btree_page_h* child_p,
        btree_page_h* parent_p,
        uint16_t move_count,
        const w_keystr_t& new_high_fence,
        const w_keystr_t& new_chain
);

template void btree_foster_adopt_log::template construct<btree_page_h*>(
        btree_page_h* p, btree_page_h* p2,
    PageID new_child_pid, lsn_t new_child_emlsn, const w_keystr_t& new_child_key);


template void btree_foster_adopt_log::template redo<btree_page_h*>(btree_page_h*);
template void btree_split_log::template redo<btree_page_h*>(btree_page_h*);

template void btree_foster_adopt_log::template redo<fixable_page_h*>(fixable_page_h*);
template void btree_split_log::template redo<fixable_page_h*>(fixable_page_h*);
