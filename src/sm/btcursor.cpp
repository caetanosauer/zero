/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define BTREE_C

#include "sm_base.h"
#include "sm_base.h"
#include "btree_page_h.h"
#include "btcursor.h"
#include "btree_impl.h"
#include "bf_tree.h"
#include "vec_t.h"
#include "xct.h"
#include "lock.h"
#include "sm.h"

bt_cursor_t::bt_cursor_t(StoreID store, bool forward)
{
    w_keystr_t infimum, supremum;
    infimum.construct_neginfkey();
    supremum.construct_posinfkey();
    _init (store, infimum, true, supremum,  true, forward);
}

bt_cursor_t::bt_cursor_t(
    StoreID store,
    const w_keystr_t& bound, bool inclusive,
    bool              forward)
{
    w_keystr_t termination;
    if (forward) {
        termination.construct_posinfkey();
        _init (store, bound, inclusive, termination,  true, forward);
    }
    else {
        termination.construct_neginfkey();
        _init (store, termination, true, bound,  inclusive, forward);
    }
}

bt_cursor_t::bt_cursor_t(
    StoreID store,
    const w_keystr_t& lower, bool lower_inclusive,
    const w_keystr_t& upper, bool upper_inclusive,
    bool              forward)
{
    _init (store, lower, lower_inclusive,
        upper,  upper_inclusive, forward);
}

void bt_cursor_t::_init(
    StoreID store,
    const w_keystr_t& lower, bool lower_inclusive,
    const w_keystr_t& upper, bool upper_inclusive,
    bool              forward)
{
    _lower = lower;
    _upper = upper;

    _store = store;
    _lower_inclusive = lower_inclusive;
    _upper_inclusive = upper_inclusive;
    _forward = forward;
    _first_time = true;
    _dont_move_next = false;
    _eof = false;
    _pid = 0;
    _slot = -1;
    _lsn = lsn_t::null;
    _elen = 0;

    _needs_lock = g_xct_does_need_lock();
    _ex_lock = g_xct_does_ex_lock_for_select();
}


void bt_cursor_t::close()
{
    _eof = true;
    _first_time = false;
    _elen = 0;
    _slot = -1;
    _key.clear();
    _release_current_page();
    _lsn = lsn_t::null;
}

void bt_cursor_t::_release_current_page() {
    if (_pid != 0) {
        w_assert1(_pid_bfidx.idx() != 0);
        _pid_bfidx.release();
        _pid = 0;
    }
}

void bt_cursor_t::_set_current_page(btree_page_h &page) {
    if (_pid != 0) {
        _release_current_page();
    }
    w_assert1(_pid == 0);
    w_assert1(_pid_bfidx.idx() == 0);
    _pid = page.pid();
    // pin this page for subsequent refix()
    _pid_bfidx.set(page.pin_for_refix());
    _lsn = page.lsn();
#ifndef USE_ATOMIC_COMMIT
    w_assert1(_lsn.valid()); // must have a valid LSN for _check_page_update to work
#endif
}

rc_t bt_cursor_t::_locate_first() {
    // at the first access, we get an intent lock on store/volume
    if (_needs_lock) {
        W_DO(smlevel_0::lm->intent_store_lock(_store, _ex_lock ? okvl_mode::IX : okvl_mode::IS));
    }

    if (_lower > _upper || (_lower == _upper && (!_lower_inclusive || !_upper_inclusive))) {
        _eof = true;
        return RCOK;
    }

    // loop because btree_impl::_ux_lock_key might return eLOCKRETRY
    while (true) {
        // find the leaf (potentially) containing the key
        const w_keystr_t &key = _forward ? _lower : _upper;
        btree_page_h leaf;
        bool found = false;
        W_DO( btree_impl::_ux_traverse(_store, key, btree_impl::t_fence_contain, LATCH_SH, leaf));
        w_assert3 (leaf.fence_contains(key));
        _set_current_page(leaf);

        w_assert1(leaf.is_fixed());
        w_assert1(leaf.is_leaf());

        // then find the tuple in the page
        leaf.search(key, found, _slot);

        const okvl_mode *mode = NULL;
        if (found) {
            // exact match!
            _key = key;
            if (_forward) {
                if (_lower_inclusive) {
                    // let's take range lock too to reduce lock manager calls
                    mode = _ex_lock ? &ALL_X_GAP_X : &ALL_S_GAP_S;
                    _dont_move_next = true;
                } else {
                    mode = _ex_lock ? &ALL_N_GAP_X : &ALL_N_GAP_S;
                    _dont_move_next = false;
                }
            } else {
                // in backward case we definitely don't need the range part
                if (_upper_inclusive) {
                    mode = _ex_lock ? &ALL_X_GAP_N : &ALL_S_GAP_N;
                    _dont_move_next = true;
                } else {
                    // in this case, we don't need lock at all
                    mode = &ALL_N_GAP_N;
                    _dont_move_next = false;
                    // only in this case, _key might disappear. otherwise,
                    // _key will exist at least as a ghost entry.
                }
            }
        } else {
            // key not found. and search_leaf returns the slot the key will be inserted.
            // in other words, val(slot - 1) < key < val(slot).
            w_assert1(_slot >= 0);
            w_assert1(_slot <= leaf.nrecs());

            if (_forward) {
                --_slot; // subsequent next() will read the slot
                if (_slot == -1) {
                    // we are hitting the left-most of the page. (note: found=false)
                    // then, we take lock on the fence-low key
                    _dont_move_next = false;
                    leaf.copy_fence_low_key(_key);
                } else {
                    _dont_move_next = false;
                    leaf.get_key(_slot, _key);
                }
                mode = _ex_lock ? &ALL_N_GAP_X : &ALL_N_GAP_S;
            } else {
                // subsequent next() will read the previous slot
                --_slot;
                if (_slot == -1) {
                    // then, we need to move to even more previous slot in previous page
                    _dont_move_next = false;
                    leaf.copy_fence_low_key(_key);
                    mode = _ex_lock ? &ALL_N_GAP_X : &ALL_N_GAP_S;
                } else {
                    _dont_move_next = true;
                    leaf.get_key(_slot, _key);
                    // let's take range lock too to reduce lock manager calls
                    mode = _ex_lock ? &ALL_X_GAP_X : &ALL_S_GAP_S;
                }
            }
        }
        if (_needs_lock && !mode->is_empty()) {
            rc_t rc = btree_impl::_ux_lock_key (_store, leaf, _key, LATCH_SH, *mode, false);
            if (rc.is_error()) {
                if (rc.err_num() == eLOCKRETRY) {
                    continue;
                } else {
                    return rc;
                }
            }
        }
        break;
    }
    return RCOK;
}

rc_t bt_cursor_t::_check_page_update(btree_page_h &p)
{
    // was the page changed?
    if (_pid != p.pid() || p.lsn() != _lsn) {
        // check if the page still contains the key we are based on
        bool found = false;
        if (p.fence_contains(_key)) {
            // it still contains. just re-locate _slot
            p.search(_key, found, _slot);
        } else {
            // we have to re-locate the page
            W_DO( btree_impl::_ux_traverse(_store, _key, btree_impl::t_fence_contain, LATCH_SH, p));
            p.search(_key, found, _slot);
        }
        w_assert1(found || !_needs_lock
            || (!_forward && !_upper_inclusive && !_dont_move_next)); // see _locate_first
        _set_current_page(p);
    }
    return RCOK;
}

w_rc_t bt_cursor_t::_refix_current_key(btree_page_h &p) {
    while (true) {
        w_rc_t fix_rt = p.refix_direct(_pid_bfidx.idx(), LATCH_SH);
        if (!fix_rt.is_error()) {
            break; // mostly no error.
        }
        if(fix_rt.err_num() != eBF_DIRECTFIX_SWIZZLED_PTR) {
            return fix_rt; // unexpected error code
        }

        W_DO(btree_impl::_ux_traverse(_store, _key, btree_impl::t_fence_contain,
                                      LATCH_SH, p));
        _slot = _forward ? 0 : p.nrecs() - 1;
        _set_current_page(p);
        // now let's re-locate the key
    }
    return RCOK;
}

rc_t bt_cursor_t::next()
{
    if (!is_valid()) {
        return RCOK; // EOF
    }

    if (_first_time) {
        _first_time = false;
        W_DO(_locate_first ());
        if (_eof) {
            return RCOK;
        }
    }

    w_assert3(_pid);
    btree_page_h p;
    W_DO(_refix_current_key(p));
    w_assert3(p.is_fixed());
    w_assert3(p.pid() == _pid);

    W_DO(_check_page_update(p));

    // Move one slot to the right(left if backward scan)
    bool eof_ret = false;
    W_DO(_find_next(p, eof_ret));

    if (eof_ret) {
        close();
        return RCOK;
    }

    w_assert3(p.is_fixed());
    w_assert3(p.is_leaf());

    w_assert3(_slot >= 0);
    w_assert3(_slot < p.nrecs());

    // get the current slot's values
    W_DO( _make_rec(p) );
    return RCOK;
}

rc_t bt_cursor_t::_find_next(btree_page_h &p, bool &eof)
{
    while (true) {
        if (_dont_move_next) {
            _dont_move_next = false;
        } else {
            W_DO(_advance_one_slot(p, eof));
        }
        if (eof) {
            break;
        }

        // skip ghost entries
        if (p.is_ghost(_slot)) {
            continue;
        }
        break;
    }
    return RCOK;
}

rc_t bt_cursor_t::_advance_one_slot(btree_page_h &p, bool &eof)
{
    w_assert1(p.is_fixed());
    w_assert1(_slot <= p.nrecs());

    if(_forward) {
        ++_slot;
    } else {
        --_slot;
    }
    eof = false;

    // keep following the next page.
    // because we might see empty pages to skip consecutively!
    while (true) {
        bool time2move = _forward ? (_slot >= p.nrecs()) : _slot < 0;

        if (time2move) {
            //  Move to right(left) sibling
            bool reached_end = _forward ? p.is_fence_high_supremum() : p.is_fence_low_infimum();
            if (reached_end) {
                eof = true;
                return RCOK;
            }
            // now, use fence keys to tell where the neighboring page exists
            w_keystr_t neighboring_fence;
            btree_impl::traverse_mode_t traverse_mode;
            bool only_low_fence_exact_match = false;
            if (_forward) {
                p.copy_fence_high_key(neighboring_fence);
                traverse_mode = btree_impl::t_fence_low_match;
                int d = _upper.compare(neighboring_fence);
                if (d < 0 || (d == 0 && !_upper_inclusive)) {
                    eof = true;
                    return RCOK;
                }
                if (d == 0 && _upper_inclusive) {
                    // we will check the next page, but the only
                    // possible matching is an entry with
                    // the low-fence..
                    only_low_fence_exact_match = true;
                }
            } else {
                // if we are going backwards, the current page had
                // low = [current-fence-low], high = [current-fence-high]
                // and the previous page should have
                // low = [?], high = [current-fence-low].
                p.copy_fence_low_key(neighboring_fence);
                // let's find a page which has this value as high-fence
                traverse_mode = btree_impl::t_fence_high_match;
                int d = _lower.compare(neighboring_fence);
                if (d >= 0) {
                    eof = true;
                    return RCOK;
                }
            }
            p.unfix();

            // take lock for the fence key
            if (_needs_lock) {
                lockid_t lid (_store, (const unsigned char*) neighboring_fence.buffer_as_keystr(), neighboring_fence.get_length_as_keystr());
                okvl_mode lock_mode;
                if (only_low_fence_exact_match) {
                    lock_mode = _ex_lock ? ALL_X_GAP_N: ALL_S_GAP_N;
                } else {
                    lock_mode = _ex_lock ? ALL_X_GAP_X : ALL_S_GAP_S;
                }
                // we can unconditionally request lock because we already released latch
                W_DO(ss_m::lm->lock(lid.hash(), lock_mode, true, true, true));
            }

            // TODO this part should check if we find an exact match of fence keys.
            // because we unlatch above, it's possible to not find exact match.
            // in that case, we should change the traverse_mode to fence_contains and continue
            W_DO(btree_impl::_ux_traverse(_store, neighboring_fence, traverse_mode, LATCH_SH, p));
            _slot = _forward ? 0 : p.nrecs() - 1;
            _set_current_page(p);
            continue;
        }

        // take lock on the next key.
        // NOTE: until we get locks, we aren't sure the key really becomes
        // the next key. So, we use the temporary variable _tmp_next_key_buf.
        const okvl_mode *mode = NULL;
        {
            p.get_key(_slot, _tmp_next_key_buf);
            if (_forward) {
                int d = _tmp_next_key_buf.compare(_upper);
                if (d < 0) {
                    mode = _ex_lock ? &ALL_X_GAP_X : &ALL_S_GAP_S;
                } else if (d == 0 && _upper_inclusive) {
                    mode = _ex_lock ? &ALL_X_GAP_N : &ALL_S_GAP_N;
                } else {
                    eof = true;
                    mode = &ALL_N_GAP_N;
                }
            } else {
                int d = _tmp_next_key_buf.compare(_lower);
                if (d > 0) {
                    mode = _ex_lock ? &ALL_X_GAP_X : &ALL_S_GAP_S;
                } else if (d == 0 && _lower_inclusive) {
                    mode = _ex_lock ? &ALL_X_GAP_X : &ALL_S_GAP_S;
                } else {
                    eof = true;
                    mode = _ex_lock ? &ALL_N_GAP_X : &ALL_N_GAP_S;
                }
            }
        }
        if (_needs_lock && !mode->is_empty()) {
            rc_t rc = btree_impl::_ux_lock_key (_store, p, _tmp_next_key_buf,
                    LATCH_SH, *mode, false);
            if (rc.is_error()) {
                if (rc.err_num() == eLOCKRETRY) {
                    W_DO(_check_page_update(p));
                    continue;
                } else {
                    return rc;
                }
            }
        }
        // okay, now we are sure the _tmp_next_key_buf is the key we want to use
        _key = _tmp_next_key_buf;
        return RCOK; // found a record! (or eof)
    }
    return RCOK;
}

rc_t bt_cursor_t::_make_rec(const btree_page_h& page)
{
    // Copy the record to buffer
    bool ghost;
    _elen = sizeof(_elbuf);
    page.copy_element(_slot, _elbuf, _elen, ghost);

#if W_DEBUG_LEVEL>0
    w_assert1(_elen <= sizeof(_elbuf));
    // this should have been skipped at _advance_one_slot()
    w_assert1(!ghost);

    w_keystr_t key_again;
    page.get_key(_slot, key_again);
    w_assert1(key_again.compare(_key) == 0);
#endif // W_DEBUG_LEVEL>0

    return RCOK;
}
