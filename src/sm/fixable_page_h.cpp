/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "fixable_page_h.h"

#define SM_SOURCE
#include "sm_base.h"

#include "btree_logrec.h"
#include "restart.h"
#include "logrec.h"
#include "bf_tree.h"


int fixable_page_h::force_Q_fixing = 0;  // <<<>>>

void fixable_page_h::unfix(bool evict)
{
    if (_pp) {
        if (_bufferpool_managed) {
            smlevel_0::bf->unfix(_pp, evict);
        }
        _pp   = NULL;
        _mode = LATCH_NL;
    }
}

w_rc_t fixable_page_h::fix_nonroot(const fixable_page_h &parent,
                                   PageID shpid, latch_mode_t mode,
                                   bool conditional, bool virgin_page, bool only_if_hit)
{
    w_assert1(parent.is_fixed());
    w_assert1(mode != LATCH_NL);

    unfix();
    W_DO(smlevel_0::bf->fix_nonroot(_pp, parent._pp, shpid, mode, conditional,
                virgin_page, only_if_hit));
    w_assert1(bf_tree_m::is_swizzled_pointer(shpid)
            || smlevel_0::bf->get_cb(_pp)->_pid == shpid);
    _bufferpool_managed = true;
    _mode               = mode;

    return RCOK;
}

w_rc_t fixable_page_h::fix_direct(PageID shpid, latch_mode_t mode,
                                   bool conditional, bool virgin_page)
{
    w_assert1(mode != LATCH_NL);

    unfix();

    W_DO(smlevel_0::bf->fix_nonroot(_pp, NULL, shpid, mode, conditional, virgin_page));

    w_assert1(bf_tree_m::is_swizzled_pointer(shpid)
            || smlevel_0::bf->get_cb(_pp)->_pid == shpid);

    _bufferpool_managed = true;
    _mode               = mode;

    return RCOK;
}

bf_idx fixable_page_h::pin_for_refix() {
    w_assert1(_bufferpool_managed);
    w_assert1(is_latched());
    return smlevel_0::bf->pin_for_refix(_pp);
}

w_rc_t fixable_page_h::refix_direct (bf_idx idx, latch_mode_t mode, bool conditional) {
    w_assert1(idx != 0);
    w_assert1(mode != LATCH_NL);

    unfix();
    W_DO(smlevel_0::bf->refix_direct(_pp, idx, mode, conditional));
    _bufferpool_managed = true;
    _mode               = mode;
    return RCOK;
}

w_rc_t fixable_page_h::fix_root (StoreID store, latch_mode_t mode,
        bool conditional, bool virgin)
{
    w_assert1(mode != LATCH_NL);

    unfix();
    W_DO(smlevel_0::bf->fix_root(_pp, store, mode, conditional, virgin));

    _bufferpool_managed = true;
    _mode               = mode;
    return RCOK;
}

void fixable_page_h::fix_nonbufferpool_page(generic_page* s)
{
    w_assert1(s != NULL);
    unfix();
    _pp                 = s;
    _bufferpool_managed = false;
    _mode               = LATCH_EX;
}

bool fixable_page_h::is_dirty() const {

    if (_bufferpool_managed) {
        return smlevel_0::bf->is_dirty(_pp);
    } else {
        return false;
    }
}

lsn_t fixable_page_h::get_page_lsn() const
{
    w_assert1(_pp);
    return smlevel_0::bf->get_page_lsn(_pp);
}

void fixable_page_h::update_page_lsn(const lsn_t & lsn) const
{
    w_assert1(_pp);
    smlevel_0::bf->set_page_lsn(_pp, lsn);
}

void fixable_page_h::set_img_page_lsn(const lsn_t & lsn)
{
    if (_pp) { _pp->lsn = lsn; }
}

bool fixable_page_h::is_to_be_deleted() {
    return (_pp->page_flags&t_to_be_deleted) != 0;
}

rc_t fixable_page_h::set_to_be_deleted (bool log_it) {
    w_assert1(is_latched());
    if ((_pp->page_flags & t_to_be_deleted) == 0) {
        if (log_it) {
            W_DO(log_page_set_to_be_deleted (*this));
        }
        _pp->page_flags ^= t_to_be_deleted;
    }
    return RCOK;
}

void fixable_page_h::unset_to_be_deleted() {
    w_assert1(is_latched());
    if ((_pp->page_flags & t_to_be_deleted) != 0) {
        _pp->page_flags ^= t_to_be_deleted;
        // we don't need set_dirty() as it's always dirty if this is ever called
        // (UNDOing this means the page wasn't deleted yet by bufferpool, so it's dirty)
    }
}


bool fixable_page_h::change_possible_after_fix() const {
    w_assert1(is_fixed());
    return false; // for now assume no interference <<<>>>
}


bool fixable_page_h::upgrade_latch_conditional(latch_mode_t mode) {
    w_assert1(_pp != NULL);
    w_assert1(mode >= LATCH_SH);

    if (_mode >= mode) {
        return true;
    };
    if (!_bufferpool_managed) {
        _mode = mode;
        return true;
    }

    if (_mode == LATCH_SH) {
        w_assert1(mode == LATCH_EX);
        bool success = smlevel_0::bf->upgrade_latch_conditional(_pp);
        if (success) {
            _mode = LATCH_EX;
        }
        return success;

    } else {
        return false;
    }
}

void fixable_page_h::setup_for_restore(generic_page* pp)
{
    w_assert1(!is_bufferpool_managed());

    // make assertions happy, even though this is not a buffer pool page
    _mode = LATCH_EX;

    _pp = pp;
}





// <<<>>>

#include "btree_page_h.h"

bool fixable_page_h::has_children() const {
    btree_page_h downcast;
    downcast.fix_nonbufferpool_page(get_generic_page());

    return !downcast.is_leaf();
}

int fixable_page_h::max_child_slot() const {
    btree_page_h downcast;
    downcast.fix_nonbufferpool_page(get_generic_page());

    if (downcast.level()<=1)
        return -1;  // if a leaf page, foster is the only pointer
    return downcast.nrecs();
}

PageID* fixable_page_h::child_slot_address(int child_slot) const {
    btree_page_h downcast;
    downcast.fix_nonbufferpool_page(get_generic_page());
    return downcast.page_pointer_address(child_slot -1);
}
