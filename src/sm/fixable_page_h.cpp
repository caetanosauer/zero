/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "fixable_page_h.h"

#define SM_SOURCE
#include "sm_int_1.h"

#include "bf_tree_inline.h"


void fixable_page_h::unfix() {
    if (_bufferpool_managed && _mode != LATCH_NL) {
        w_assert1(_pp);
        smlevel_0::bf->unfix(_pp);
        _mode = LATCH_NL;
        _pp   = NULL;
    }
}

w_rc_t fixable_page_h::fix_nonroot (const fixable_page_h &parent, volid_t vol,
                                    shpid_t shpid, latch_mode_t mode, 
                                    bool conditional, bool virgin_page) {
    w_assert1(shpid != 0);
    unfix();
    if (mode == LATCH_Q) {
        if (virgin_page || !is_swizzled_pointer(shpid)) {
            return RC(eLATCHQFAIL);
        }

        bool success;
        W_DO(smlevel_0::bf->fix_with_Q_nonroot(_pp, vol, shpid, success));
        if (!success) {
            return RC(eLATCHQFAIL);
        }
        _mode = LATCH_SH; // <<<>>>
    } else {
        W_DO(smlevel_0::bf->fix_nonroot(_pp, parent._pp, vol, shpid, mode, conditional, virgin_page));
        _mode = mode;
        w_assert1(smlevel_0::bf->get_cb(_pp)->_pid_vol == vol);
        w_assert1(is_swizzled_pointer(shpid) || smlevel_0::bf->get_cb(_pp)->_pid_shpid == shpid);
    }
    _bufferpool_managed = true;
    return RCOK;
}

w_rc_t fixable_page_h::fix_direct(volid_t vol, shpid_t shpid,
                                  latch_mode_t mode, bool conditional, 
                                  bool virgin_page) {
    w_assert1(shpid != 0);
    unfix();
    if (mode == LATCH_Q) {
        return RC(eLATCHQFAIL);
    }
    W_DO(smlevel_0::bf->fix_direct(_pp, vol, shpid, mode, conditional, virgin_page));
    _bufferpool_managed = true;
    _mode = mode;
    w_assert1(smlevel_0::bf->get_cb(_pp)->_pid_vol   == vol);
    w_assert1(smlevel_0::bf->get_cb(_pp)->_pid_shpid == shpid);
    return RCOK;
}

bf_idx fixable_page_h::pin_for_refix() {
    w_assert1(_bufferpool_managed);
    w_assert1(is_latched());
    return smlevel_0::bf->pin_for_refix(_pp);
}

w_rc_t fixable_page_h::refix_direct (bf_idx idx, latch_mode_t mode, bool conditional) {
    w_assert1(idx != 0);
    unfix();
    if (mode == LATCH_Q) {
        return RC(eLATCHQFAIL);
    }
    W_DO(smlevel_0::bf->refix_direct(_pp, idx, mode, conditional));
    _bufferpool_managed = true;
    _mode = mode;
    return RCOK;
}

w_rc_t fixable_page_h::fix_virgin_root (volid_t vol, snum_t store, shpid_t shpid) {
    w_assert1(shpid != 0);
    unfix();
    W_DO(smlevel_0::bf->fix_virgin_root(_pp, vol, store, shpid));
    _bufferpool_managed = true;
    _mode = LATCH_EX;
    w_assert1(smlevel_0::bf->get_cb(_pp)->_pid_vol   == vol);
    w_assert1(smlevel_0::bf->get_cb(_pp)->_pid_shpid == shpid);
    return RCOK;
}

w_rc_t fixable_page_h::fix_root (volid_t vol, snum_t store, latch_mode_t mode, bool conditional) {
    unfix();
    if (mode == LATCH_Q) {
        W_DO(smlevel_0::bf->fix_with_Q_root(_pp, vol, store));
        // later deal with possibility of latching failure (not yet possible) <<<>>>
        _mode = LATCH_SH; // <<<>>>
    } else {
        W_DO(smlevel_0::bf->fix_root(_pp, vol, store, mode, conditional));
        _mode = mode;
    }
    _bufferpool_managed = true;
    return RCOK;
}

void fixable_page_h::fix_nonbufferpool_page(generic_page* s) {
    w_assert1(s != NULL);
    w_assert1(s->tag == t_btree_p);  // make sure page type is fixable
    unfix();
    _pp                 = s;
    _bufferpool_managed = false;
    _mode               = LATCH_EX;
}

void fixable_page_h::set_dirty() const {
    w_assert1(_pp);
    w_assert1(_mode != LATCH_Q);
    if (_bufferpool_managed) {
        smlevel_0::bf->set_dirty(_pp);
    }
}

bool fixable_page_h::is_dirty() const {
    w_assert1(_mode != LATCH_Q);
    if (_bufferpool_managed) {
        return smlevel_0::bf->is_dirty(_pp);
    } else {
        return false;
    }
}


rc_t fixable_page_h::set_to_be_deleted (bool log_it) {
    w_assert1(is_latched());
    if ((_pp->page_flags & t_to_be_deleted) == 0) {
        if (log_it) {
            W_DO(log_page_set_to_be_deleted (*this));
        }
        _pp->page_flags ^= t_to_be_deleted;
        set_dirty();
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

bool fixable_page_h::is_to_be_deleted() {
    w_assert1(_mode != LATCH_Q);
    return (_pp->page_flags&t_to_be_deleted) != 0; 
}



bool fixable_page_h::upgrade_latch_conditional() {
    w_assert1(_pp != NULL);
    w_assert1(_mode == LATCH_SH);
    if (!_bufferpool_managed) {
        return false;
    }
    bool success = smlevel_0::bf->upgrade_latch_conditional(_pp);
    if (success) {
        _mode = LATCH_EX;
    }
    return success;
}





// <<<>>> 

#include "btree_page_h.h"

bool fixable_page_h::has_children() const {
    w_assert1(_mode != LATCH_Q);
    btree_page_h downcast;
    downcast.fix_nonbufferpool_page(get_generic_page());

    return !downcast.is_leaf();
}

int fixable_page_h::max_child_slot() const {
    w_assert1(_mode != LATCH_Q);
    btree_page_h downcast;
    downcast.fix_nonbufferpool_page(get_generic_page());

    if (downcast.level()<=1)
        return -1;  // if a leaf page, foster is the only pointer
    return downcast.nrecs();
}

shpid_t* fixable_page_h::child_slot_address(int child_slot) const {
    w_assert1(_mode != LATCH_Q);
    btree_page_h downcast;
    downcast.fix_nonbufferpool_page(get_generic_page());
    return downcast.page_pointer_address(child_slot -1);
}
