/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
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

#include "btree_page.h"  // FIXME for 1 occurrence of btree_page_h <<<>>>

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
    w_assert1(idx > 0 && idx < _block_cnt);
    return get_cbp(idx);
}

inline generic_page* bf_tree_m::get_page(const bf_tree_cb_t *cb) {
    bf_idx idx = get_idx(cb);
    w_assert1(idx > 0 && idx < _block_cnt);
    return _buffer + idx;
}
inline shpid_t bf_tree_m::get_root_page_id(volid_t vol, snum_t store) {
    if (_volumes[vol] == NULL) {
        return 0;
    }
    bf_idx idx = _volumes[vol]->_root_pages[store];
    if (idx == 0 || idx >= _block_cnt) {
        return 0;
    }
    generic_page* page = _buffer + idx;
    return page->pid.page;
}

///////////////////////////////////   Page fix/unfix BEGIN         ///////////////////////////////////  
const uint32_t SWIZZLED_LRU_UPDATE_INTERVAL = 1000;

inline w_rc_t bf_tree_m::refix_direct (generic_page*& page, bf_idx
                                       idx, latch_mode_t mode, bool conditional) {
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb.pin_cnt() > 0);
    W_DO(cb.latch().latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
#ifdef BP_MAINTAIN_PARNET_PTR
    ++cb._counter_approximate;
#endif // BP_MAINTAIN_PARNET_PTR
    ++cb._refbit_approximate;
    page = &(_buffer[idx]);
    return RCOK;
}

inline w_rc_t bf_tree_m::fix_nonroot(generic_page*& page, generic_page *parent, 
                                     volid_t vol, shpid_t shpid, 
                                     latch_mode_t mode, bool conditional, 
                                     bool virgin_page) {
#ifdef SIMULATE_MAINMEMORYDB
    if (virgin_page) {
        W_DO (_fix_nonswizzled(parent, page, vol, shpid, mode, conditional, virgin_page));
    } else {
        w_assert1((shpid & SWIZZLED_PID_BIT) == 0);
        bf_idx idx = shpid;
        w_assert1 (_is_active_idx(idx));
        bf_tree_cb_t &cb = get_cb(idx);
        W_DO(cb.latch().latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
        page = &(_buffer[idx]);
    }
    if (true) return RCOK;
#endif // SIMULATE_MAINMEMORYDB
    w_assert1(parent !=  NULL);
    if (!is_swizzling_enabled()) {
        return _fix_nonswizzled(NULL, page, vol, shpid, mode, conditional, virgin_page);
    }
    
    // the parent must be latched
    w_assert1(latch_mode(parent) == LATCH_SH || latch_mode(parent) == LATCH_EX);
    if((shpid & SWIZZLED_PID_BIT) == 0) {
        // non-swizzled page. or even worse it might not exist in bufferpool yet!
        W_DO (_fix_nonswizzled(parent, page, vol, shpid, mode, conditional, virgin_page));
        // also try to swizzle this page
        // TODO so far we swizzle all pages as soon as we load them to bufferpool
        // but, we might want to consider a more advanced policy.
        btree_page_h p(parent); // FIXME: really my_btree_page_h <<<>>>
        if (!_bf_pause_swizzling && is_swizzled(parent) && !is_swizzled(page)
                && p.get_foster_opaqueptr() != shpid // don't swizzle foster child
            ) {
            slotid_t slot = find_page_id_slot (parent, shpid);
            // this is a new (virgin) page which has not been linked yet. 
            // skip swizzling this page
            if (slot == -2 && virgin_page) {
                return RCOK;
            }

            // benign race: if (slot < -1 && is_swizzled(page)) then some other 
            // thread swizzled it already. This can happen when two threads that 
            // have the page latched as shared need to swizzle it
            w_assert1(slot >= -1 || is_swizzled(page) || (slot == -2 && virgin_page));
            
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
        w_assert1 (_is_active_idx(idx));
        bf_tree_cb_t &cb = get_cb(idx);
        W_DO(cb.latch().latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
        w_assert1(cb.pin_cnt() > 0);
        w_assert1(cb._pid_vol == vol);
        w_assert1(cb._pid_shpid == _buffer[idx].pid.page);
#ifdef BP_MAINTAIN_PARNET_PTR
        ++cb._counter_approximate;
#endif // BP_MAINTAIN_PARNET_PTR

        // If we keep incrementing the cb._refbit_approximate then we cause a scalability 
        // bottleneck (as the associated cacheline ping-pongs between sockets).
        // Intead we limit the maximum value of the refcount. The refcount still has
        // enough granularity to separate cold from hot pages. 
        if (get_cb(idx)._refbit_approximate < BP_MAX_REFCOUNT) {
            ++cb._refbit_approximate;
        }
        // also, doesn't have to unpin whether there happens an error or not. easy!
        page = &(_buffer[idx]);
        
#ifdef BP_MAINTAIN_PARNET_PTR
        // infrequently update LRU.
        if (cb._counter_approximate % SWIZZLED_LRU_UPDATE_INTERVAL == 0) {
            _update_swizzled_lru(idx);
        }
#endif // BP_MAINTAIN_PARNET_PTR
    }
    return RCOK;
}

inline w_rc_t bf_tree_m::fix_virgin_root (generic_page*& page, volid_t vol, snum_t store, shpid_t shpid) {
    w_assert1(vol != 0);
    w_assert1(store != 0);
    w_assert1(shpid != 0);
    w_assert1((shpid & SWIZZLED_PID_BIT) == 0);
    bf_tree_vol_t *volume = _volumes[vol];
    w_assert1(volume != NULL);
    w_assert1(volume->_root_pages[store] == 0);

    bf_idx idx;
    
#ifdef SIMULATE_MAINMEMORYDB
    idx = shpid;
    volume->_root_pages[store] = idx;
    bf_tree_cb_t &cb = get_cb(idx);
    cb.clear();
    cb._pid_vol = vol;
    cb._pid_shpid = shpid;
    cb.pin_cnt_set(1); // root page's pin count is always positive
    cb._used = true;
    cb._dirty = true;
    if (true) return _latch_root_page(page, idx, LATCH_EX, false);
#endif // SIMULATE_MAINMEMORYDB

    // this page will be the root page of a new store.
    W_DO(_grab_free_block(idx));
    w_assert1 (idx > 0 && idx < _block_cnt);
    volume->_root_pages[store] = idx;

    get_cb(idx).clear();
    get_cb(idx)._pid_vol = vol;
    get_cb(idx)._pid_shpid = shpid;
    get_cb(idx).pin_cnt_set(1); // root page's pin count is always positive
    get_cb(idx)._used = true;
    get_cb(idx)._dirty = true;
    ++_dirty_page_count_approximate;
    get_cb(idx)._swizzled = true;
    bool inserted = _hashtable->insert_if_not_exists(bf_key(vol, shpid), idx); // for some type of caller (e.g., redo) we still need hashtable entry for root
    if (!inserted) {
        ERROUT (<<"failed to insert a virgin root page to hashtable. this must not have happened because there shouldn't be any race. wtf");
        return RC(smlevel_0::eINTERNAL);
    }
    return _latch_root_page(page, idx, LATCH_EX, false);
}

inline w_rc_t bf_tree_m::fix_root (generic_page*& page, volid_t vol, snum_t store, latch_mode_t mode, bool conditional) {
    w_assert1(vol != 0);
    w_assert1(store != 0);
    bf_tree_vol_t *volume = _volumes[vol];
    w_assert1(volume != NULL);
    w_assert1(volume->_root_pages[store] != 0);

    bf_idx idx = volume->_root_pages[store];

#ifdef SIMULATE_MAINMEMORYDB
    w_assert1(get_cb(idx)._pid_vol == vol);
    w_assert1(_buffer[idx].pid.store() == store);
    if (true) return _latch_root_page(page, idx, mode, conditional);
#endif

    // root page is always kept in the volume descriptor
#ifdef SIMULATE_NO_SWIZZLING
    bf_idx idx_dummy = _hashtable->lookup(bf_key(vol, get_cb(volume->_root_pages[store])._pid_shpid));
    w_assert1(idx == idx_dummy);
    idx = idx_dummy;
#else // SIMULATE_NO_SWIZZLING
    if (!is_swizzling_enabled()) {
        bf_idx idx_dummy = _hashtable->lookup(bf_key(vol, get_cb(volume->_root_pages[store])._pid_shpid));
        w_assert1(idx == idx_dummy);
        idx = idx_dummy;
    }
#endif // SIMULATE_NO_SWIZZLING

    w_assert1 (_is_active_idx(idx));
    w_assert1(get_cb(idx)._pid_vol == vol);
    w_assert1(_buffer[idx].pid.store() == store);

    return _latch_root_page(page, idx, mode, conditional);
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
}
inline bool bf_tree_m::is_dirty(const generic_page* p) const {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx)._dirty;
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
#ifdef BP_MAINTAIN_PARNET_PTR
inline bool bf_tree_m::_is_in_swizzled_lru (bf_idx idx) const {
    w_assert1 (_is_active_idx(idx));
    return SWIZZLED_LRU_NEXT(idx) != 0 || SWIZZLED_LRU_PREV(idx) != 0 || SWIZZLED_LRU_HEAD == idx;
}
#endif // BP_MAINTAIN_PARNET_PTR
inline bool bf_tree_m::is_swizzled(const generic_page* page) const
{
    bf_idx idx = page - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx)._swizzled;
}

///////////////////////////////////   LRU/Freelist END ///////////////////////////////////  

inline bool bf_tree_m::_is_active_idx (bf_idx idx) const {
    if (idx <= 0 || idx > _block_cnt) {
        return false;
    }
    return get_cb(idx)._used;
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
