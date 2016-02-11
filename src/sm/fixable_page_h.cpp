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

void fixable_page_h::unfix() {
    if (_pp) {
        if (_bufferpool_managed && _mode != LATCH_Q) {
            smlevel_0::bf->unfix(_pp);
        }
        _pp   = NULL;
        _mode = LATCH_NL;
    }
}

w_rc_t fixable_page_h::fix_nonroot(const fixable_page_h &parent,
                                   PageID shpid, latch_mode_t mode,
                                   bool conditional, bool virgin_page)
{
    w_assert1(parent.is_fixed());
    w_assert1(mode != LATCH_NL);

    if (force_Q_fixing > 1 && mode == LATCH_SH) mode = LATCH_Q; // <<<>>>
    unfix();
    if (mode == LATCH_Q || parent.latch_mode() == LATCH_Q) {
        if (virgin_page || !is_swizzled_pointer(shpid)) {
            return RC(eNEEDREALLATCH);
        }

        W_DO(smlevel_0::bf->fix_unsafely_nonroot(_pp, shpid, mode, conditional, _Q_ticket));
        if (mode == LATCH_Q) {
            if (false) { // test ticket later for validity <<<>>>
                _pp = NULL;
                return RC(eLATCHQFAIL);
            }
        }
        // Check crabbing from Q case:
        if (parent.latch_mode() == LATCH_Q) {
            if (parent.change_possible_after_fix()) {
                unfix();
                return RC(ePARENTLATCHQFAIL);
            }
        }
    } else {
        W_DO(smlevel_0::bf->fix_nonroot(_pp, parent._pp, shpid, mode, conditional, virgin_page));
        w_assert1(is_swizzled_pointer(shpid) || smlevel_0::bf->get_cb(_pp)->_pid_shpid == shpid);
    }
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

    w_assert1(is_swizzled_pointer(shpid) || smlevel_0::bf->get_cb(_pp)->_pid_shpid == shpid);

    _bufferpool_managed = true;
    _mode               = mode;

    return RCOK;
}

w_rc_t fixable_page_h::fix_recovery_redo(bf_idx idx, PageID page_updated,
                                            const bool managed) {

    // Special function for Recovery REDO phase
    // Caller of this function is responsible for acquire and release EX latch on this page

    // This is a newly loaded page during REDO phase in Recovery
    // The idx is already in hashtable, all we need to do is to associate it with
    // fixable page data structure.
    // Swizzling must be off when calling this function.

    // For Recovery REDO page loading, we do not use 'fix_direct' because
    // before the loading, the page idx is known and in hash table already, which
    // is different from the assumption in 'fix_direct' (idx is not in hashtable).
    // The Recovery REDO logic for each 'in_doubt' page:
    //    Acquire EX latch on the page
    //    bf_tree_m::load_for_redo - load the physical page if it has not been loaded
    //    fixable_page_h::fix_recovery_redo - associate the idx with fixable_page_h data structure
    //    perform REDO operation on the page
    //    Release EX latch on the page

    w_assert1(!smlevel_0::bf->is_swizzling_enabled());

    smlevel_0::bf->associate_page(_pp, idx, page_updated);
    _bufferpool_managed = managed;
    _mode = LATCH_EX;

    return RCOK;
}

w_rc_t fixable_page_h::fix_recovery_redo_managed()
{
    // Special function for Recovery REDO phase

    // For normal operation, page should always be managed by buffer pool.
    // During REDO pass:
    // if we encounter a page rebalance operation (split, merge) and if page
    // is not managed by buffer pool, a different code path would be used
    // to recovery from the page rebalance (via Single Page Recovery).
    // If page is managed, then the code page does not use Single Page
    // Recovery, in such case full logger must be used to recovery the page

    // This function is only used for page driven REDO (Single-Page-Recovery)
    // with minimal logging, the page would be set to 'not managed'
    // before the page recovery, and set to managed again (this function)
    // after the recovery operation

    _bufferpool_managed = true;
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
    if (mode == LATCH_Q) {
        return RC(eNEEDREALLATCH);
    }
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

void fixable_page_h::fix_nonbufferpool_page(generic_page* s) {
    w_assert1(s != NULL);
    // w_assert1(s->tag == t_btree_p);  // make sure page type is fixable

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

// CS: a function that updates a field should NOT be declared const (TODO)
void fixable_page_h::update_initial_and_last_lsn(const lsn_t & lsn) const
{
    // Update both initial dirty lsn (if needed) and last write lsn
    // Caller should have latch on the page
    w_assert1(_pp);
    if (_pp)
    {
        ((generic_page_h*)this)->set_lsns(lsn);
        update_initial_dirty_lsn(lsn);
    }
}

void fixable_page_h::update_clsn(const lsn_t& lsn)
{
    if (_pp) {
        _pp->clsn = lsn;
    }
}

void fixable_page_h::update_initial_dirty_lsn(const lsn_t & lsn) const
{
    // Whenever updating a page lsn (last write through set_lsns())
    // caller should update the initial dirty lsn on the page also

    w_assert1(_pp);
    w_assert1(_mode != LATCH_Q);

    if (_bufferpool_managed)
    {
        smlevel_0::bf->update_initial_dirty_lsn(_pp, lsn);
    }
}

bool fixable_page_h::is_to_be_deleted() {
    w_assert1(_mode != LATCH_Q);
    return (_pp->page_flags&t_to_be_deleted) != 0;
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
        w_assert1(_mode == LATCH_Q);
        return false; // later need to call latch operation and appropriately set _mode <<<>>>
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

PageID* fixable_page_h::child_slot_address(int child_slot) const {
    w_assert1(_mode != LATCH_Q);
    btree_page_h downcast;
    downcast.fix_nonbufferpool_page(get_generic_page());
    return downcast.page_pointer_address(child_slot -1);
}
