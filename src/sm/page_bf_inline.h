/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef PAGE_BF_INLINE_H
#define PAGE_BF_INLINE_H
// bufferpool-related inline methods for fixable_page_h.
// these methods are small and frequently called, thus inlined.

// also, they are separated from page.h because these implementations
// have more dependency that need to be inlined (eg bf_tree_m).
// To hide unnecessary details from the caller except when the caller
// actually uses these methods, I separated them to this file.

// also inline bf_tree_m methods.
#include "bf_tree_inline.h"
#include "generic_page_h.h"
#include "sm_int_0.h"

inline w_rc_t fixable_page_h::fix_nonroot (const fixable_page_h &parent, volid_t vol, shpid_t shpid, latch_mode_t mode, bool conditional, bool virgin_page) {
    w_assert1(shpid != 0);
    if (is_fixed()) {
        unfix();
    }
    W_DO(smlevel_0::bf->fix_nonroot(_pp, parent._pp, vol, shpid, mode, conditional, virgin_page));
    _mode = mode;
    w_assert1(smlevel_0::bf->get_cb(_pp)->_pid_vol == vol);
    w_assert1(is_swizzled_pointer(shpid) || smlevel_0::bf->get_cb(_pp)->_pid_shpid == shpid);
    return RCOK;
}

inline w_rc_t fixable_page_h::fix_direct (volid_t vol, shpid_t shpid, latch_mode_t mode, bool conditional, bool virgin_page) {
    w_assert1(shpid != 0);
    if (is_fixed()) {
        unfix();
    }
    W_DO(smlevel_0::bf->fix_direct(_pp, vol, shpid, mode, conditional, virgin_page));
    _mode = mode;
    w_assert1(smlevel_0::bf->get_cb(_pp)->_pid_vol == vol);
    w_assert1(smlevel_0::bf->get_cb(_pp)->_pid_shpid == shpid);
    return RCOK;
}

inline bf_idx fixable_page_h::pin_for_refix() {
    w_assert1(is_latched());
    return smlevel_0::bf->pin_for_refix(_pp);
}

inline w_rc_t fixable_page_h::refix_direct (bf_idx idx, latch_mode_t mode, bool conditional) {
    w_assert1(idx != 0);
    if (is_fixed()) {
        unfix();
    }
    W_DO(smlevel_0::bf->refix_direct(_pp, idx, mode, conditional));
    _mode = mode;
    return RCOK;
}

inline w_rc_t fixable_page_h::fix_virgin_root (volid_t vol, snum_t store, shpid_t shpid) {
    w_assert1(shpid != 0);
    if (is_fixed()) {
        unfix();
    }
    W_DO(smlevel_0::bf->fix_virgin_root(_pp, vol, store, shpid));
    _mode = LATCH_EX;
    w_assert1(smlevel_0::bf->get_cb(_pp)->_pid_vol == vol);
    w_assert1(smlevel_0::bf->get_cb(_pp)->_pid_shpid == shpid);
    return RCOK;
}

inline w_rc_t fixable_page_h::fix_root (volid_t vol, snum_t store, latch_mode_t mode, bool conditional) {
    if (is_fixed()) {
        unfix();
    }
    W_DO(smlevel_0::bf->fix_root(_pp, vol, store, mode, conditional));
    _mode = mode;
    return RCOK;
}


inline void fixable_page_h::unset_tobedeleted() {
    if ((_pp->page_flags & t_tobedeleted) != 0) {
        _pp->page_flags ^= t_tobedeleted;
        // we don't need set_dirty() as it's always dirty if this is ever called
        // (UNDOing this means the page wasn't deleted yet by bufferpool, so it's dirty)
    }
}


inline void  fixable_page_h::unfix() {
    if (_mode != LATCH_NL) {
        w_assert1(_pp);
        smlevel_0::bf->unfix(_pp);
        _mode = LATCH_NL;
        _pp = NULL;
    }
}

inline void fixable_page_h::set_dirty() const {
    w_assert1(_pp);
    if (_mode != LATCH_NL) {
        smlevel_0::bf->set_dirty(_pp);
    }
}

inline bool fixable_page_h::is_dirty() const {
    if (_mode == LATCH_NL) {
        return false;
    } else {
        return smlevel_0::bf->is_dirty(_pp);
    }
}

inline bool fixable_page_h::upgrade_latch_conditional() {
    w_assert1(_pp != NULL);
    w_assert1(_mode == LATCH_SH);
    bool success = smlevel_0::bf->upgrade_latch_conditional(_pp);
    if (success) {
        _mode = LATCH_EX;
    }
    return success;
}

#endif // PAGE_BF_INLINE_H

