/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BF_TREE_INLINE_H
#define BF_TREE_INLINE_H
// inline methods for bf_tree_m
// these methods are small and very frequently called, thus inlined here.

#include "sm_int_0.h"
#include "bf_tree.h"
#include "bf_tree_cb.h"
#include "bf_tree_vol.h"
#include "bf_hashtable.h"
#include "fixable_page_h.h"

// Following includes are to have the ability to check restart mode
#include "sm_int_1.h"
#include "xct.h"
#include "restart.h"
#include "vol.h"

void swizzling_stat_swizzle();
void swizzling_stat_print(const char* prefix);
void swizzling_stat_reset();

inline bf_tree_cb_t* bf_tree_m::get_cbp(bf_idx idx) const {
#ifdef BP_ALTERNATE_CB_LATCH
    bf_idx real_idx;
    real_idx = (idx << 1) + (idx & 0x1); // more efficient version of: real_idx = (idx % 2) ? idx*2+1 : idx*2
    return &_control_blocks[real_idx];
#else
    return &_control_blocks[idx];
#endif
}

inline bf_tree_cb_t& bf_tree_m::get_cb(bf_idx idx) const {
    return *get_cbp(idx);
}

inline bf_idx bf_tree_m::get_idx(const bf_tree_cb_t* cb) const {
    bf_idx real_idx = cb - _control_blocks;
#ifdef BP_ALTERNATE_CB_LATCH
    return real_idx / 2;
#else
    return real_idx;
#endif
}

inline bf_tree_cb_t* bf_tree_m::get_cb(const generic_page *page) {
    bf_idx idx = page - _buffer;
    w_assert1(_is_valid_idx(idx));
    return get_cbp(idx);
}

inline generic_page* bf_tree_m::get_page(const bf_tree_cb_t *cb) {
    bf_idx idx = get_idx(cb);
    w_assert1(_is_valid_idx(idx));
    return _buffer + idx;
}
inline shpid_t bf_tree_m::get_root_page_id(stid_t store) {
    if (_volumes[store.vol] == NULL) {
        return 0;
    }
    bf_idx idx = _volumes[store.vol]->_root_pages[store.store];
    if (!_is_valid_idx(idx)) {
        return 0;
    }
    generic_page* page = _buffer + idx;
    return page->pid.page;
}

inline bf_idx bf_tree_m::get_root_page_idx(stid_t store) {
    if (_volumes[store.vol] == NULL)
        return 0;

    // root-page index is always kept in the volume descriptor:
    bf_idx idx = _volumes[store.vol]->_root_pages[store.store];
    if (!_is_valid_idx(idx))
        return 0;
    else
        return idx;
}

///////////////////////////////////   Page fix/unfix BEGIN         ///////////////////////////////////

const uint32_t SWIZZLED_LRU_UPDATE_INTERVAL = 1000;

inline w_rc_t bf_tree_m::refix_direct (generic_page*& page, bf_idx
                                       idx, latch_mode_t mode, bool conditional) {
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb.pin_cnt() > 0);
    W_DO(cb.latch().latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
#ifdef BP_MAINTAIN_PARENT_PTR
    ++cb._counter_approximate;
#endif // BP_MAINTAIN_PARENT_PTR
    ++cb._refbit_approximate;
    assert(false == cb._in_doubt);
    page = &(_buffer[idx]);
    return RCOK;
}

inline w_rc_t bf_tree_m::fix_nonroot(generic_page*& page, generic_page *parent,
                                     vid_t vol, shpid_t shpid,
                                     latch_mode_t mode, bool conditional,
                                     bool virgin_page,
                                     const bool from_recovery) {
    INC_TSTAT(bf_fix_nonroot_count);
#ifdef SIMULATE_MAINMEMORYDB
    if (virgin_page) {
        W_DO (_fix_nonswizzled(parent, page, vol, shpid, mode, conditional, virgin_page));
    } else {
        w_assert1((shpid & SWIZZLED_PID_BIT) == 0);
        bf_idx idx = shpid;
        w_assert1(_is_valid_idx(idx));
        bf_tree_cb_t &cb = get_cb(idx);
        W_DO(cb.latch().latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
        w_assert1 (_is_active_idx(idx));
        w_assert1(false == cb._in_doubt);
        page = &(_buffer[idx]);
    }
    if (true) return RCOK;
#endif // SIMULATE_MAINMEMORYDB
    w_assert1(parent !=  NULL);
    if (!is_swizzling_enabled()) {
        return _fix_nonswizzled(parent, page, vol, shpid, mode, conditional, virgin_page, from_recovery);
    }

    // the parent must be latched
    w_assert1(latch_mode(parent) == LATCH_SH || latch_mode(parent) == LATCH_EX);
    if((shpid & SWIZZLED_PID_BIT) == 0) {
        // non-swizzled page. or even worse it might not exist in bufferpool yet!
        W_DO (_fix_nonswizzled(parent, page, vol, shpid, mode, conditional, virgin_page, from_recovery));
        // also try to swizzle this page
        // TODO so far we swizzle all pages as soon as we load them to bufferpool
        // but, we might want to consider a more advanced policy.
        fixable_page_h p;
        p.fix_nonbufferpool_page(parent);
        if (!_bf_pause_swizzling && is_swizzled(parent) && !is_swizzled(page)
            //  don't swizzle foster-child
            && *p.child_slot_address(GeneralRecordIds::FOSTER_CHILD) != shpid) {
            general_recordid_t slot = find_page_id_slot (parent, shpid);
            w_assert1(slot != GeneralRecordIds::FOSTER_CHILD); // because we ruled it out

            // this is a new (virgin) page which has not been linked yet.
            // skip swizzling this page
            if (slot == GeneralRecordIds::INVALID && virgin_page) {
                return RCOK;
            }

            // benign race: if (slot < -1 && is_swizzled(page)) then some other
            // thread swizzled it already. This can happen when two threads that
            // have the page latched as shared need to swizzle it
            w_assert1(slot >= GeneralRecordIds::FOSTER_CHILD || is_swizzled(page)
                || (slot == GeneralRecordIds::INVALID && virgin_page));

#ifdef EX_LATCH_ON_SWIZZLING
            if (latch_mode(parent) != LATCH_EX) {
                ++_bf_swizzle_ex;
                bool upgraded = upgrade_latch_conditional(parent);
                if (!upgraded) {
                    ++_bf_swizzle_ex_fails;
                    DBGOUT2(<< "gave up swizzling for now because we failed to upgrade parent's latch");
                    return RCOK;
                }
            }
#endif //EX_LATCH_ON_SWIZZLING
            if (!is_swizzled(page)) {
                swizzle_child(parent, slot);
            }
        }
    } else {
        w_assert1(!virgin_page); // virgin page can't be swizzled
        // the pointer is swizzled! we can bypass pinning
        bf_idx idx = shpid ^ SWIZZLED_PID_BIT;
        w_assert1(_is_valid_idx(idx));
        bf_tree_cb_t &cb = get_cb(idx);
        W_DO(cb.latch().latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
        w_assert1 (_is_active_idx(idx));
        w_assert1(cb.pin_cnt() > 0);
        w_assert1(cb._pid_vol == vol);
        w_assert1(false == cb._in_doubt);
        w_assert1(cb._pid_shpid == _buffer[idx].pid.page);

        // We limit the maximum value of the refcount by BP_MAX_REFCOUNT to avoid the scalability
        // bottleneck caused by excessive cache coherence traffic (cacheline ping-pongs between sockets).
        if (get_cb(idx)._refbit_approximate < BP_MAX_REFCOUNT) {
            ++cb._refbit_approximate;
        }
        // also, doesn't have to unpin whether there happens an error or not. easy!
        page = &(_buffer[idx]);

#ifdef BP_MAINTAIN_PARENT_PTR
        ++cb._counter_approximate;
        // infrequently update LRU.
        if (cb._counter_approximate % SWIZZLED_LRU_UPDATE_INTERVAL == 0) {
            _update_swizzled_lru(idx);
        }
#endif // BP_MAINTAIN_PARENT_PTR
    }
    return RCOK;
}

inline w_rc_t bf_tree_m::fix_unsafely_nonroot(generic_page*& page, shpid_t shpid, latch_mode_t mode, bool conditional, q_ticket_t& ticket) {
    w_assert1((shpid & SWIZZLED_PID_BIT) != 0);

    INC_TSTAT(bf_fix_nonroot_count);

    bf_idx idx = shpid ^ SWIZZLED_PID_BIT;
    w_assert1(_is_valid_idx(idx));

    bf_tree_cb_t &cb = get_cb(idx);

    if (mode == LATCH_Q) {
        // later we will acquire the latch in Q mode <<<>>>
        //W_DO(get_cb(idx).latch().latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
        ticket = 42; // <<<>>>
    } else {
        W_DO(get_cb(idx).latch().latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
    }
    w_assert1(false == cb._in_doubt);
    page = &(_buffer[idx]);

    // We limit the maximum value of the refcount by BP_MAX_REFCOUNT to avoid the scalability
    // bottleneck caused by excessive cache coherence traffic (cacheline ping-pongs between sockets).
    if (get_cb(idx)._refbit_approximate < BP_MAX_REFCOUNT) {
        ++cb._refbit_approximate;
    }

#ifdef BP_MAINTAIN_PARENT_PTR
    ++cb._counter_approximate;
    // infrequently update LRU.
    if (cb._counter_approximate % SWIZZLED_LRU_UPDATE_INTERVAL == 0) {
        // Cannot call _update_swizzled_lru without S or X latch
        // because page might not still be a swizzled page so disable!
        // [JIRA issue ZERO-175]
        BOOST_STATIC_ASSERT(false);
        _update_swizzled_lru(idx);
    }
#endif // BP_MAINTAIN_PARENT_PTR

    return RCOK;
}


inline w_rc_t bf_tree_m::fix_virgin_root (generic_page*& page, stid_t store, shpid_t shpid) {
    w_assert1(store.vol != vid_t(0));
    w_assert1(store.store != 0);
    w_assert1(shpid != 0);
    w_assert1((shpid & SWIZZLED_PID_BIT) == 0);
    bf_tree_vol_t *volume = _volumes[store.vol];
    w_assert1(volume != NULL);
    w_assert1(volume->_root_pages[store.store] == 0);

    bf_idx idx;

#ifdef SIMULATE_MAINMEMORYDB
    idx = shpid;
    volume->_root_pages[store.store] = idx;
    bf_tree_cb_t &cb = get_cb(idx);
    cb.clear();
    cb._pid_vol = store.vol;
    cb._store_num = store.store;
    cb._pid_shpid = shpid;
    cb.pin_cnt_set(1); // root page's pin count is always positive
    cb._used = true;
    cb._dirty = true;
    cb._uncommitted_cnt = 0;
    get_cb(idx)._in_doubt = false;
    if (true) return _latch_root_page(page, idx, LATCH_EX, false);
#endif // SIMULATE_MAINMEMORYDB

    // this page will be the root page of a new store.
    W_DO(_grab_free_block(idx));
    w_assert1(_is_valid_idx(idx));
    volume->_root_pages[store.store] = idx;

    get_cb(idx).clear();
    get_cb(idx)._pid_vol = store.vol;
    get_cb(idx)._pid_shpid = shpid;
    get_cb(idx)._store_num = store.store;
    get_cb(idx).pin_cnt_set(1); // root page's pin count is always positive
    get_cb(idx)._used = true;
    get_cb(idx)._dirty = true;
    get_cb(idx)._in_doubt = false;
    get_cb(idx)._recovery_access = false;
    get_cb(idx)._uncommitted_cnt = 0;
    ++_dirty_page_count_approximate;
    get_cb(idx)._swizzled = true;
    bool inserted = _hashtable->insert_if_not_exists(bf_key(store.vol, shpid), idx); // for some type of caller (e.g., redo) we still need hashtable entry for root
    if (!inserted) {
        ERROUT (<<"failed to insert a virgin root page to hashtable. this must not have happened because there shouldn't be any race. wtf");
        return RC(eINTERNAL);
    }
    return _latch_root_page(page, idx, LATCH_EX, false);
}

inline w_rc_t bf_tree_m::fix_root (generic_page*& page, stid_t store,
                                   latch_mode_t mode, bool conditional, const bool from_undo) {
    w_assert1(store.vol != vid_t(0));
    w_assert1(store.store != 0);
    bf_tree_vol_t *volume = _volumes[store.vol];
    w_assert1(volume != NULL);

    // root-page index is always kept in the volume descriptor:
    bf_idx idx = volume->_root_pages[store.store];
    if (!_is_valid_idx(idx)) {
        /*
         * CS: During restore, root page is not pre-loaded. As with any other
         * page, a fix incurs a read if the page is not found. In this case,
         * the read will trigger restore of the page. Once it's restored, its
         * frame is registered in the volume manager.
         *
         * This code eliminates the need to call install_volume.
         */
        // currently, only restore should get into this if-block
        w_assert0(volume->_volume->is_failed());

        shpid_t root_shpid = volume->_volume->get_store_root(store.store);
        W_DO(_grab_free_block(idx));
        W_DO(_preload_root_page(volume, volume->_volume, store.store,
                    root_shpid, idx));
    }

    w_assert1(_is_valid_idx(idx));
    w_assert1(_is_active_idx(idx));
    w_assert1(get_cb(idx)._pid_vol == vid_t(store.vol));

    w_assert1(true == get_cb(idx)._used);

    // Root page is pre-loaded into buffer pool when loading volume
    // this function is called for both normal and Recovery operations
    // In concurrent recovery mode, the root page might still be in_doubt
    // when called, need to block user txn in such case, but allow recovery
    // operation to go through

    if (true == get_cb(idx)._in_doubt)
    {
        DBGOUT3(<<"bf_tree_m::fix_root: root page is still in_doubt");

        if ((false == from_undo) && (false == get_cb(idx)._recovery_access))
        {
            // Page still in_doubt, caller is from concurrent transaction.
            // if we are not using on_demand or mixed restart modes,
            // raise error because concurrent transaction is not allowed
            // to load in_doubt page in traditional restart mode (not on_demand)

            if ((false == restart_m::use_redo_demand_restart()) &&  // pure on-demand
                (false == restart_m::use_redo_mix_restart()))       // midxed mode
            {
                DBGOUT3(<<"bf_tree_m::fix_root: user transaction but not on_demand, cannot fix in_doubt root page");
                return RC(eACCESS_CONFLICT);
            }
        }
    }

    W_DO(_latch_root_page(page, idx, mode, conditional));

    if ((false == get_cb(idx)._in_doubt) &&                                 // Page not in_doubt
        (false == from_undo) && (false == get_cb(idx)._recovery_access))    // From concurrent user transaction
    {
        // validate the accessability of the page (validation happens only if using commit_lsn)
        w_rc_t rc = _validate_access(page);
        if (rc.is_error())
        {
            get_cb(idx).latch().latch_release();
            return rc;
        }
    }

#ifndef SIMULATE_MAINMEMORYDB
    /*
     * Verify when swizzling off & non-main memory DB that lookup table handles this page correctly:
     */
#ifdef SIMULATE_NO_SWIZZLING
    w_assert1(idx == _hashtable->lookup(bf_key(vol, get_cb(volume->_root_pages[store])._pid_shpid)));
#else // SIMULATE_NO_SWIZZLING
    if (!is_swizzling_enabled()) {
        w_assert1(idx == _hashtable->lookup(bf_key(store.vol, get_cb(volume->_root_pages[store.store])._pid_shpid)));
    }
#endif // SIMULATE_NO_SWIZZLING
#endif // SIMULATE_MAINMEMORYDB

    return RCOK;
}

inline w_rc_t bf_tree_m::_latch_root_page(generic_page*& page, bf_idx idx, latch_mode_t mode, bool conditional) {

#ifdef SIMULATE_NO_SWIZZLING
    _increment_pin_cnt_no_assumption(idx);
#else // SIMULATE_NO_SWIZZLING
    if (!is_swizzling_enabled()) {
        _increment_pin_cnt_no_assumption(idx);
    }
#endif // SIMULATE_NO_SWIZZLING

    // root page is always swizzled. thus we don't need to increase pin. just take latch.
    W_DO(get_cb(idx).latch().latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
    // also, doesn't have to unpin whether there happens an error or not. easy!
    page = &(_buffer[idx]);

#ifdef SIMULATE_NO_SWIZZLING
    _decrement_pin_cnt_assume_positive(idx);
#else // SIMULATE_NO_SWIZZLING
    if (!is_swizzling_enabled()) {
        _decrement_pin_cnt_assume_positive(idx);
    }
#endif // SIMULATE_NO_SWIZZLING
    return RCOK;
}

inline w_rc_t bf_tree_m::fix_with_Q_root(generic_page*& page, stid_t store, q_ticket_t& ticket) {
    w_assert1(store.vol != vid_t(0));
    w_assert1(store.store != 0);
    bf_tree_vol_t *volume = _volumes[store.vol];
    w_assert1(volume != NULL);

    // root-page index is always kept in the volume descriptor:
    bf_idx idx = volume->_root_pages[store.store];
    w_assert1(_is_valid_idx(idx));

    // later we will acquire the latch in Q mode <<<>>>
    //W_DO(get_cb(idx).latch().latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
    ticket = 42; // <<<>>>
    page = &(_buffer[idx]);

    /*
     * We do not bother verifying we got the right page as root page IDs only change when
     * tables are dropped.
     */

    return RCOK;
}


inline void bf_tree_m::unfix(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb.latch().held_by_me());
    cb.latch().latch_release();
}

inline void bf_tree_m::set_dirty(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    if (!cb._dirty) {
        cb._dirty = true;
        ++_dirty_page_count_approximate;
    }
    cb._used = true;
#ifdef USE_ATOMIC_COMMIT
    /*
     * CS: We assume that all updates made by transactions go through
     * this method (usually via the methods in logrec_t but also in
     * B-tree maintenance code).
     * Therefore, it is the only place where the count of
     * uncommitted updates is incrementes.
     */
    cb._uncommitted_cnt++;
    // assert that transaction attached is of type plog_xct_t*
    w_assert1(smlevel_1::xct_impl == smlevel_1::XCT_PLOG);
#endif
}
inline bool bf_tree_m::is_dirty(const generic_page* p) const {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx)._dirty;
}

inline bool bf_tree_m::is_dirty(const bf_idx idx) const {
    // Caller has latch on page
    // Used by REDO phase in Recovery
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx)._dirty;
}

inline void bf_tree_m::update_initial_dirty_lsn(const generic_page* p,
                                                const lsn_t new_lsn)
{
    w_assert3(new_lsn.hi() > 0);
    // Update the initial dirty lsn (if needed) for the page regardless page is dirty or not
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    (void) new_lsn; // avoid compiler warning of unused var

#ifndef USE_ATOMIC_COMMIT // otherwise rec_lsn is only set when fetching page
    bf_tree_cb_t &cb = get_cb(idx);
    if ((new_lsn.data() < cb._rec_lsn) || (0 == cb._rec_lsn))
        cb._rec_lsn = new_lsn.data();
#endif
}

inline void bf_tree_m::set_recovery_access(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    cb._recovery_access = true;
}
inline bool bf_tree_m::is_recovery_access(const generic_page* p) const {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx)._recovery_access;
}

inline void bf_tree_m::clear_recovery_access(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    cb._recovery_access = false;
}

inline void bf_tree_m::set_in_doubt(const bf_idx idx, lsn_t first_lsn,
                                      lsn_t last_lsn) {
    // Caller has latch on page
    // From Log Analysis phase in Recovery, page is not in buffer pool
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    cb._in_doubt = true;
    cb._dirty = false;
    cb._used = true;
    cb._uncommitted_cnt = 0;

    // _rec_lsn is the initial LSN which made the page dirty
    // Update the earliest LSN only if first_lsn is earlier than the current one
    if ((first_lsn.data() < cb._rec_lsn) || (0 == cb._rec_lsn))
        cb._rec_lsn = first_lsn.data();

    // During recovery, _dependency_lsn is used to store the last write lsn on
    // the in_doubt page.  Update it if the last lsn is later than the current one
    if ((last_lsn.data() > cb._dependency_lsn) || (0 == cb._dependency_lsn))
        cb._dependency_lsn = last_lsn.data();

}

inline void bf_tree_m::clear_in_doubt(const bf_idx idx, bool still_used, uint64_t key) {
    // Caller has latch on page
    // 1. From Log Analysis phase in Recovery, page is not in buffer pool
    // 2. From REDO phase in Recovery, page is in buffer pool but does not exist on disk

    // Only reasons to call this function:
    // Log record indicating allocating or deallocating a page
    // Allocating: the page might be used for a non-logged operation (i.e. bulk load),
    //                 clear the in_doubt flag but not the 'used' flag, also don't remove it
    //                 from hashtable
    // Deallocating: clear both in_doubt and used flags, also remove it from
    //                 hashtable so the page can be used by others
    //
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);

    // Clear both 'in_doubt' and 'used' flags, no change to _dirty flag
    cb._in_doubt = false;
    cb._dirty = false;
    cb._uncommitted_cnt = 0;

    // Page is no longer needed, caller is from de-allocating a page log record
    if (false == still_used)
    {
        cb._used = false;

        // Give this page back to freelist, so this block can be reused from now on
        bool removed = _hashtable->remove(key);
        w_assert1(removed);
        _add_free_block(idx);
    }
}

inline void bf_tree_m::in_doubt_to_dirty(const bf_idx idx) {
    // Caller has latch on page
    // From REDO phase in Recovery, page just loaded into buffer pool

    // Change a page from in_doubt to dirty by setting the flags

    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);

    // Clear both 'in_doubt' and 'used' flags, no change to _dirty flag
    cb._in_doubt = false;
    cb._dirty = true;
    cb._used = true;
    cb._refbit_approximate = BP_INITIAL_REFCOUNT;
    cb._uncommitted_cnt = 0;

    // Page has been loaded into buffer pool, no need for the
    // last write LSN any more (only used for Single-Page-Recovery during system crash restart
    // purpose), stop overloading this field
    cb._dependency_lsn = 0;
}


inline bool bf_tree_m::is_in_doubt(const bf_idx idx) const {
    // Caller has latch on page
    // From Log Analysis phase in Recovery, page is not in buffer pool
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx)._in_doubt;
}

inline bf_idx bf_tree_m::lookup_in_doubt(const int64_t key) const
{
    // Look up the hashtable using the provided key
    // return 0 if the page cb is not in the buffer pool
    // Otherwise returning the index
    // This function is for the Recovery to handle the in_doubt flag in cb
    // note that the actual page (_buffer) may or may not in the buffer pool
    // use this function with caution

    return _hashtable->lookup(key);
}

inline void bf_tree_m::set_initial_rec_lsn(const lpid_t& pid,
                       const lsn_t new_lsn,       // In-coming LSN
                       const lsn_t current_lsn)   // Current log LSN
{
    // Caller has latch on page
    // Special function called from btree_page_h::format_steal() when the
    // page format log record was generated, this can happen during a b-tree
    // operation or from redo during recovery

    // Reset the _rec_lsn in page cb (when the page was dirtied initially) if
    // it is later than the new_lsn, we want the earliest lsn in _rec_lsn

    uint64_t key = bf_key(pid.vol(), pid.page);
    bf_idx idx = _hashtable->lookup(key);
    if (0 != idx)
    {
        // Page exists in buffer pool hash table
        bf_tree_cb_t &cb = smlevel_0::bf->get_cb(idx);

        lsn_t lsn = new_lsn;
        if (0 == new_lsn.data())
        {
           lsn = current_lsn;
           w_assert1(0 != lsn.data());
           w_assert3(lsn.hi() > 0);
        }

        // Update the initial LSN which is when the page got dirty initially
        // Update only if the existing initial LSN was later than the incoming LSN
        // or the original LSN did not exist
        if ((cb._rec_lsn > lsn.data()) || (0 == cb._rec_lsn))
            cb._rec_lsn = new_lsn.data();
        cb._used = true;
        cb._dirty = true;
        cb._uncommitted_cnt = 0;

        // Either from regular b-tree operation or from redo during recovery
        // do not change the in_doubt flag setting, caller handles it
    }
    else
    {
        // Page does not exist in buffer pool hash table
        // This should not happen, no-op and we are not raising an error
    }
}


inline bool bf_tree_m::is_used (bf_idx idx) const {
    return _is_active_idx(idx);
}


inline latch_mode_t bf_tree_m::latch_mode(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx).latch().mode();
}

inline void bf_tree_m::downgrade_latch(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb.latch().held_by_me());
    cb.latch().downgrade();
}

inline bool bf_tree_m::upgrade_latch_conditional(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb.latch().held_by_me());
    if (cb.latch().mode() == LATCH_EX) {
        return true;
    }
    bool would_block = false;
    cb.latch().upgrade_if_not_block(would_block);
    if (!would_block) {
        w_assert1 (cb.latch().mode() == LATCH_EX);
        return true;
    } else {
        return false;
    }
}

void print_latch_holders(latch_t* latch);


///////////////////////////////////   Page fix/unfix END         ///////////////////////////////////

///////////////////////////////////   LRU/Freelist BEGIN ///////////////////////////////////
#ifdef BP_MAINTAIN_PARENT_PTR
inline bool bf_tree_m::_is_in_swizzled_lru (bf_idx idx) const {
    w_assert1 (_is_active_idx(idx));
    return SWIZZLED_LRU_NEXT(idx) != 0 || SWIZZLED_LRU_PREV(idx) != 0 || SWIZZLED_LRU_HEAD == idx;
}
#endif // BP_MAINTAIN_PARENT_PTR
inline bool bf_tree_m::is_swizzled(const generic_page* page) const
{
    bf_idx idx = page - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx)._swizzled;
}

///////////////////////////////////   LRU/Freelist END ///////////////////////////////////

inline bool bf_tree_m::_is_valid_idx(bf_idx idx) const {
    return idx > 0 && idx < _block_cnt;
}


inline bool bf_tree_m::_is_active_idx (bf_idx idx) const {
    return _is_valid_idx(idx) && get_cb(idx)._used;
}


inline void pin_for_refix_holder::release() {
    if (_idx != 0) {
        smlevel_0::bf->unpin_for_refix(_idx);
        _idx = 0;
    }
}


inline shpid_t bf_tree_m::normalize_shpid(shpid_t shpid) const {
    generic_page* page;
#ifdef SIMULATE_MAINMEMORYDB
    bf_idx idx = shpid;
    page = &_buffer[idx];
    return page->pid.page;
#else
    if (is_swizzling_enabled()) {
        if (is_swizzled_pointer(shpid)) {
            bf_idx idx = shpid ^ SWIZZLED_PID_BIT;
            page = &_buffer[idx];
            return page->pid.page;
        }
    }
    return shpid;
#endif
}


#endif // BF_TREE_INLINE_H
