#include "bf_hashtable.h"
#include "bf_tree_cb.h"
#include "bf_tree.h"
#include "btree_page_h.h"

/** Context object that is passed around during eviction. */
struct EvictionContext {
    /** The number of blocks evicted. */
    uint32_t        evicted_count;
    /** The number of blocks unswizzled. */
    uint32_t        unswizzled_count;
    /** Specifies how thorough the eviction should be */
    evict_urgency_t urgency;
    /** Number of blocks to evict. Might evict less or more blocks for some reason. */
    uint32_t        preferred_count;

    /**
     * Hierarchical clockhand. Initialized by copying _clockhand_pathway in _bf_tree
     * which are shared by all eviction threads. Copied back to the _clockhand_pathway
     * when the eviction is done.
     * @see bf_tree#_clockhand_pathway
     */
    uint32_t        clockhand_pathway[MAX_CLOCKHAND_DEPTH];
    /**
     * Same as above, but in terms of buffer index in this bufferpool.
     * The buffer index is not protected by latches, so we must check validity when
     * we really evict/unswizzle.
     * The first entry is dummy as it's vol. (store -> root page's bufidx)
     */
    bf_idx          bufidx_pathway[MAX_CLOCKHAND_DEPTH];
    /** Same as above, whether the block was swizzled. */
    bool            swizzled_pathway[MAX_CLOCKHAND_DEPTH];
    /** Same as above. @see bf_tree#_clockhand_current_depth */
    uint16_t        clockhand_current_depth;
    /** The depth of the current thread-local traversal (as of entering the method). */
    uint16_t        traverse_depth;

    /**
     * How many times we visited all frames during the eviction.
     * It's rarely more than 0 except a very thorough eviction mode.
     */
    uint16_t        rounds;

    /** Returns current volume. */
    vid_t         get_vol() const { return (vid_t) clockhand_pathway[0]; }
    /** Returns current store. */
    snum_t          get_store() const { return (snum_t) clockhand_pathway[1]; }

    /** Did we evict enough frames? */
    bool            is_enough() const { return evicted_count >= preferred_count; }

    /** Are we now even unswizzling frames? */
    bool            is_unswizzling() const {
        return urgency >= EVICT_COMPLETE || (urgency >= EVICT_URGENT && rounds > 0);
    }

    EvictionContext() : evicted_count(0), unswizzled_count(0), traverse_depth(0),
    rounds(0) {}
};

#ifdef BP_MAINTAIN_PARENT_PTR
void bf_tree_m::_add_to_swizzled_lru(bf_idx idx) {
    w_assert1 (is_swizzling_enabled());
    w_assert1 (_is_active_idx(idx));
    w_assert1 (!get_cb(idx)._swizzled);
    CRITICAL_SECTION(cs, &_swizzled_lru_lock);
    ++_swizzled_lru_len;
    if (SWIZZLED_LRU_HEAD == 0) {
        // currently the LRU is empty
        w_assert1(SWIZZLED_LRU_TAIL == 0);
        SWIZZLED_LRU_HEAD = idx;
        SWIZZLED_LRU_TAIL = idx;
        SWIZZLED_LRU_PREV(idx) = 0;
        SWIZZLED_LRU_NEXT(idx) = 0;
        return;
    }
    w_assert1(SWIZZLED_LRU_TAIL != 0);
    // connect to the current head
    SWIZZLED_LRU_PREV(idx) = 0;
    SWIZZLED_LRU_NEXT(idx) = SWIZZLED_LRU_HEAD;
    SWIZZLED_LRU_PREV(SWIZZLED_LRU_HEAD) = idx;
    SWIZZLED_LRU_HEAD = idx;
}

void bf_tree_m::_update_swizzled_lru(bf_idx idx) {
    w_assert1 (is_swizzling_enabled());
    w_assert1 (_is_active_idx(idx));
    w_assert1 (get_cb(idx)._swizzled);

    CRITICAL_SECTION(cs, &_swizzled_lru_lock);
    w_assert1(SWIZZLED_LRU_HEAD != 0);
    w_assert1(SWIZZLED_LRU_TAIL != 0);
    w_assert1(_swizzled_lru_len > 0);
    if (SWIZZLED_LRU_HEAD == idx) {
        return; // already the head
    }
    if (SWIZZLED_LRU_TAIL == idx) {
        bf_idx new_tail = SWIZZLED_LRU_PREV(idx);
        SWIZZLED_LRU_NEXT(new_tail) = 0;
        SWIZZLED_LRU_TAIL = new_tail;
    } else {
        bf_idx old_prev = SWIZZLED_LRU_PREV(idx);
        bf_idx old_next = SWIZZLED_LRU_NEXT(idx);
        w_assert1(old_prev != 0);
        w_assert1(old_next != 0);
        SWIZZLED_LRU_NEXT (old_prev) = old_next;
        SWIZZLED_LRU_PREV (old_next) = old_prev;
    }
    bf_idx old_head = SWIZZLED_LRU_HEAD;
    SWIZZLED_LRU_PREV(idx) = 0;
    SWIZZLED_LRU_NEXT(idx) = old_head;
    SWIZZLED_LRU_PREV(old_head) = idx;
    SWIZZLED_LRU_HEAD = idx;
}

void bf_tree_m::_remove_from_swizzled_lru(bf_idx idx) {
    w_assert1 (is_swizzling_enabled());
    w_assert1 (_is_active_idx(idx));
    w_assert1 (get_cb(idx)._swizzled);

    get_cb(idx)._swizzled = false;
    CRITICAL_SECTION(cs, &_swizzled_lru_lock);
    w_assert1(SWIZZLED_LRU_HEAD != 0);
    w_assert1(SWIZZLED_LRU_TAIL != 0);
    w_assert1(_swizzled_lru_len > 0);
    --_swizzled_lru_len;

    bf_idx old_prev = SWIZZLED_LRU_PREV(idx);
    bf_idx old_next = SWIZZLED_LRU_NEXT(idx);
    SWIZZLED_LRU_PREV(idx) = 0;
    SWIZZLED_LRU_NEXT(idx) = 0;
    if (SWIZZLED_LRU_HEAD == idx) {
        w_assert1(old_prev == 0);
        if (old_next == 0) {
            w_assert1(_swizzled_lru_len == 0);
            SWIZZLED_LRU_HEAD = 0;
            SWIZZLED_LRU_TAIL = 0;
            return;
        }
        SWIZZLED_LRU_HEAD = old_next;
        SWIZZLED_LRU_PREV(old_next) = 0;
        return;
    }

    w_assert1(old_prev != 0);
    if (SWIZZLED_LRU_TAIL == idx) {
        w_assert1(old_next == 0);
        SWIZZLED_LRU_NEXT(old_prev) = 0;
        SWIZZLED_LRU_TAIL = old_prev;
    } else {
        w_assert1(old_next != 0);
        SWIZZLED_LRU_NEXT (old_prev) = old_next;
        SWIZZLED_LRU_PREV (old_next) = old_prev;
    }
}
#endif // BP_MAINTAIN_PARENT_PTR

w_rc_t bf_tree_m::_grab_free_block(bf_idx& ret, bool evict) {
#ifdef SIMULATE_MAINMEMORYDB
    if (true) {
        ERROUT (<<"MAINMEMORY-DB. _grab_free_block() shouldn't be called. wtf");
        return RC(eINTERNAL);
    }
#endif // SIMULATE_MAINMEMORYDB
    ret = 0;
    while (true) {
        // once the bufferpool becomes full, getting _freelist_lock everytime will be
        // too costly. so, we check _freelist_len without lock first.
        //   false positive : fine. we do real check with locks in it
        //   false negative : fine. we will eventually get some free block anyways.
        if (_freelist_len > 0) {
            CRITICAL_SECTION(cs, &_freelist_lock);
            if (_freelist_len > 0) { // here, we do the real check
                bf_idx idx = FREELIST_HEAD;
                w_assert1(_is_valid_idx(idx));
                w_assert1 (!get_cb(idx)._used);
                --_freelist_len;
                if (_freelist_len == 0) {
                    FREELIST_HEAD = 0;
                } else {
                    FREELIST_HEAD = _freelist[idx];
                    w_assert1 (FREELIST_HEAD > 0 && FREELIST_HEAD < _block_cnt);
                }
                ret = idx;
                return RCOK;
            }
        } // exit the scope to do the following out of the critical section

        // if the freelist was empty, let's evict some page.
        if (true == evict)
        {
            W_DO (_get_replacement_block());
        }
        else
        {
            // Freelist is empty and caller does not want to evict pages (Recovery M1)
            return RC(eBFFULL);
        }
    }
    return RCOK;
}

w_rc_t bf_tree_m::_get_replacement_block() {
#ifdef SIMULATE_MAINMEMORYDB
    if (true) {
        ERROUT (<<"MAINMEMORY-DB. _get_replacement_block() shouldn't be called. wtf");
        return RC(eINTERNAL);
    }
#endif // SIMULATE_MAINMEMORYDB
    uint32_t evicted_count, unswizzled_count;
    // evict with gradually higher urgency
    for (int urgency = EVICT_NORMAL; urgency <= EVICT_COMPLETE; ++urgency) {
        W_DO(evict_blocks(evicted_count, unswizzled_count, (evict_urgency_t) urgency));
        if (evicted_count > 0 || _freelist_len > 0) {
            return RCOK;
        }
        W_DO(wakeup_cleaners());
        g_me()->sleep(100);
        DBGOUT1(<<"woke up. now there should be some page to evict. urgency=" << urgency);
        // debug_dump(std::cout);
    }

    ERROUT(<<"whoa, couldn't find an evictable page for long time. gave up!");
    // debug_dump(std::cerr);
    W_DO(evict_blocks(evicted_count, unswizzled_count, EVICT_COMPLETE));
    if (evicted_count > 0 || _freelist_len > 0) {
        return RCOK;
    }
    return RC(eFRAMENOTFOUND);
}

bool bf_tree_m::_try_evict_block(bf_idx parent_idx, bf_idx idx) {
    bf_tree_cb_t &parent_cb = get_cb(parent_idx);
    bf_tree_cb_t &cb = get_cb(idx);

    // do not consider dirty pages (at this point)
    // we check this again later because we don't take locks as of this.
    // we also avoid grabbing unused block because it has to be grabbed via freelist
    if (cb._dirty || !cb._used || !parent_cb._used || cb._in_doubt) {
        return -1;
    }

    // find a block that has no pinning (or not being evicted by others).
    // this check is approximate as it's without lock.
    // false positives are fine, and we do the real check later
    if (cb.pin_cnt() != 0) {
        return false;
    }

    // if it seems someone latches it, give up.
    if (cb.latch().latch_cnt() != 0) { // again, we check for real later
        return false;
    }

    // okay, let's try evicting this page.
    // first, we have to make sure the page's pin_cnt is exactly 0.
    // we atomically change it to -1.
    int zero = 0;
    if (lintel::unsafe::atomic_compare_exchange_strong(const_cast<int32_t*>(&cb._pin_cnt),
        (int32_t*) &zero , (int32_t) -1)) {
        // CAS did it job. the current thread has an exclusive access to this block
        bool evicted = _try_evict_block_pinned(parent_cb, cb, parent_idx, idx);
        if (!evicted) {
            cb.pin_cnt_set(0);
        }
        return evicted;
    }
    // it can happen. we just give up this block
    return false;
}
bool bf_tree_m::_try_evict_block_pinned(
    bf_tree_cb_t &parent_cb, bf_tree_cb_t &cb,
    bf_idx parent_idx, bf_idx idx) {
    w_assert1(cb.pin_cnt() == -1);

    // let's do a real check.
    if (cb._dirty || !cb._used || cb._in_doubt) {
        DBGOUT1(<<"very unlucky, this block has just become dirty.");
        // oops, then put this back and give up this block
        return false;
    }

    w_assert1(_buffer[parent_idx].tag == t_btree_p);
    generic_page *parent = &_buffer[parent_idx];
    general_recordid_t child_slotid = find_page_id_slot(parent, cb._pid_shpid);
    if (child_slotid == GeneralRecordIds::INVALID) {
        // because the searches are approximate, this can happen
        DBGOUT1(<< "Unlucky, the parent/child pair is no longer valid");
        return false;
    }
    // check latches too. just conditionally test it to avoid long waits.
    // We take 2 latches here; parent and child *in this order* to avoid deadlocks.

    // Parent page can be just an SH latch because we are updating EMLSN only
    // which no one else should be updating
    bool parent_latched = false;
    if (!parent_cb.latch().held_by_me()) {
        w_rc_t latch_parent_rc = parent_cb.latch().latch_acquire(LATCH_SH, WAIT_IMMEDIATE);
        if (latch_parent_rc.is_error()) {
            DBGOUT1(<<"Unlucky, failed to latch parent block while evicting. skipping block");
            return false;
        }
        parent_latched = true;
    }

    bool updated = _try_evict_block_update_emlsn(parent_cb, cb, parent_idx, idx, child_slotid);
    // as soon as we are done with parent, unlatch it
    if (parent_latched) {
        parent_cb.latch().latch_release();
    }
    if (!updated) {
        return false;
    }

    // remove it from hashtable.
    bool removed = _hashtable->remove(bf_key(cb._pid_vol, cb._pid_shpid));
    w_assert1(removed);
#ifdef BP_MAINTAIN_PARENT_PTR
    w_assert1(!_is_in_swizzled_lru(idx));
    if (is_swizzling_enabled()) {
        w_assert1(cb._parent != 0);
        _decrement_pin_cnt_assume_positive(cb._parent);
    }
#endif // BP_MAINTAIN_PARENT_PTR
    cb.clear();
    return true; // success
}
bool bf_tree_m::_try_evict_block_update_emlsn(
    bf_tree_cb_t &parent_cb, bf_tree_cb_t &cb,
    bf_idx parent_idx, bf_idx idx, general_recordid_t child_slotid) {
    w_assert1(cb.pin_cnt() == -1);
    w_assert1(parent_cb.latch().is_latched());

    w_rc_t latch_rc = cb.latch().latch_acquire(LATCH_EX, WAIT_IMMEDIATE);
    if (latch_rc.is_error()) {
        DBGOUT1(<<"very unlucky, someone has just latched this block.");
        return false;
    }
    // we can immediately release EX latch because no one will newly take latch as _pin_cnt==-1
    cb.latch().latch_release();
    DBGOUT1(<<"evicting page idx = " << idx << " shpid = " << cb._pid_shpid
            << " pincnt = " << cb.pin_cnt());

    // Output Single-Page-Recovery log for updating EMLSN in parent.
    // safe to disguise as LATCH_EX for the reason above.
    btree_page_h parent_h;
    generic_page *parent = &_buffer[parent_idx];
    parent_h.fix_nonbufferpool_page(parent);
    lsn_t old = parent_h.get_emlsn_general(child_slotid);
    if (old < _buffer[idx].lsn) {
        DBGOUT1(<< "Updated EMLSN on page " << parent_h.pid() << " slot=" << child_slotid
                << " (child pid=" << cb._pid_shpid << ")"
                << ", OldEMLSN=" << old << " NewEMLSN=" << _buffer[idx].lsn);
        W_COERCE(_sx_update_child_emlsn(parent_h, child_slotid, _buffer[idx].lsn));
        // Note that we are not grabbing EX latch on parent here.
        // This is safe because no one else should be touching these exact bytes,
        // and because EMLSN values are aligned to be "regular register".
        set_dirty(parent); // because fix_nonbufferpool_page() would skip set_dirty().
        w_assert1(parent_h.get_emlsn_general(child_slotid) == _buffer[idx].lsn);
    }
    return true;
}

void bf_tree_m::_add_free_block(bf_idx idx) {
    CRITICAL_SECTION(cs, &_freelist_lock);
    ++_freelist_len;
    _freelist[idx] = FREELIST_HEAD;
    FREELIST_HEAD = idx;
#ifdef BP_MAINTAIN_PARENT_PTR
    // if the following fails, you might have forgot to remove it from the LRU before calling this method
    w_assert1(SWIZZLED_LRU_NEXT(idx) == 0);
    w_assert1(SWIZZLED_LRU_PREV(idx) == 0);
#endif // BP_MAINTAIN_PARENT_PTR
}

void bf_tree_m::_delete_block(bf_idx idx) {
    w_assert1(_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb._dirty);
    w_assert1(cb.pin_cnt() == 0);
    w_assert1(!cb.latch().is_latched());
    cb._used = false; // clear _used BEFORE _dirty so that eviction thread will ignore this block.
    cb._dirty = false;
    cb._in_doubt = false; // always set in_doubt bit to false
    cb._uncommitted_cnt = 0;

    DBGOUT1(<<"delete block: remove page shpid = " << cb._pid_shpid);
    bool removed = _hashtable->remove(bf_key(cb._pid_vol, cb._pid_shpid));
    w_assert1(removed);
#ifdef BP_MAINTAIN_PARENT_PTR
    w_assert1(!_is_in_swizzled_lru(idx));
    if (is_swizzling_enabled()) {
        _decrement_pin_cnt_assume_positive(cb._parent);
    }
#endif // BP_MAINTAIN_PARENT_PTR

    // after all, give back this block to the freelist. other threads can see this block from now on
    _add_free_block(idx);
}

void bf_tree_m::_dump_evict_clockhand(const EvictionContext &context) const {
    DBGOUT1(<< "current clockhand depth=" << context.clockhand_current_depth
        << ". _swizzled_page_count_approximate=" << _swizzled_page_count_approximate << " / "
        << _block_cnt << "\n so far evicted " << context.evicted_count << ", "
        <<  "unswizzled " << context.unswizzled_count << " frames. rounds=" << context.rounds);
    for (int i = 0; i < context.clockhand_current_depth; ++i) {
        DBGOUT1(<< "current clockhand pathway[" << i << "]:" << context.clockhand_pathway[i]);
    }
}

w_rc_t bf_tree_m::evict_blocks(uint32_t& evicted_count, uint32_t& unswizzled_count,
                               evict_urgency_t urgency, uint32_t preferred_count)
{
    EvictionContext context;
    context.urgency = urgency;
    context.preferred_count = preferred_count;
    ::memset(context.bufidx_pathway, 0, sizeof(bf_idx) * MAX_CLOCKHAND_DEPTH);
    ::memset(context.swizzled_pathway, 0, sizeof(bool) * MAX_CLOCKHAND_DEPTH);
    // copy-from the shared clockhand_pathway with latch
    {
        CRITICAL_SECTION(cs, &_clockhand_copy_lock);
        context.clockhand_current_depth = _clockhand_current_depth;
        ::memcpy(context.clockhand_pathway, _clockhand_pathway,
                 sizeof(uint32_t) * MAX_CLOCKHAND_DEPTH);
    }

    W_DO(_evict_blocks(context));

    // copy-back to the shared clockhand_pathway with latch
    {
        CRITICAL_SECTION(cs, &_clockhand_copy_lock);
        _clockhand_current_depth = context.clockhand_current_depth;
        ::memcpy(_clockhand_pathway, context.clockhand_pathway,
                 sizeof(uint32_t) * MAX_CLOCKHAND_DEPTH);
    }
    evicted_count = context.evicted_count;
    unswizzled_count = context.unswizzled_count;
    return RCOK;
}

w_rc_t bf_tree_m::_evict_blocks(EvictionContext& context) {
    w_assert1(context.traverse_depth == 0);
    while (context.rounds < EVICT_MAX_ROUNDS) {
        _dump_evict_clockhand(context);
        uint32_t old = context.clockhand_pathway[0] % vol_m::MAX_VOLS;
        for (uint16_t i = 0; i < vol_m::MAX_VOLS; ++i) {
            vid_t vol = (old + i) % vol_m::MAX_VOLS;
            if (_volumes[vol] == NULL) {
                continue;
            }
            context.traverse_depth = 1;
            if (i != 0 || context.clockhand_current_depth == 0) {
                // This means now we are moving on to another volume.
                // When i == 0, we are just continuing the traversal from the
                // last time, i.e., reuising whatever was already on the clock
                // hand path.
                context.clockhand_current_depth = context.traverse_depth; // reset descendants
                context.clockhand_pathway[context.traverse_depth - 1] = vol;
            }
            W_DO(_evict_traverse_volume(context));
            if (context.is_enough()) {
                return RCOK;
            }
        }
        // checked everything. next round.
        context.clockhand_current_depth = 0;
        ++context.rounds;
        if (context.urgency <= EVICT_NORMAL) {
            break;
        }
        // most likely we have too many dirty pages
        // let's try writing out dirty pages
        W_DO(wakeup_cleaners());
    }

    _dump_evict_clockhand(context);
    return RCOK;
}

w_rc_t bf_tree_m::_evict_traverse_volume(EvictionContext &context) {
    uint32_t depth = context.traverse_depth;
    w_assert1(depth == 1);
    w_assert1(context.clockhand_current_depth >= depth);
    w_assert1(context.clockhand_pathway[depth - 1] != 0);
    vid_t vol = context.get_vol();
    uint32_t old = context.clockhand_pathway[depth];
    for (uint32_t i = 0; i < MAX_STORE_COUNT; ++i) {
        snum_t store = (old + i) % MAX_STORE_COUNT;
        if (_volumes[vol] == NULL) {
            // just give up in unlucky case (probably the volume has been just uninstalled)
            return RCOK;
        }
        /*
         * TODO CS -- this is a racing condition. We can test 1000 times and
         * _volumes[vol] may still be NULL just before we access it below.
         * Proper concurrency control is needed to access the array.
         */
        bf_idx root_idx = _volumes[vol]->_root_pages[store];
        if (root_idx == 0) {
            continue;
        }

        context.traverse_depth = depth + 1;
        if (i != 0 || context.clockhand_current_depth == depth) {
            // this means now we are moving on to another store.
            context.clockhand_current_depth = depth + 1; // reset descendants
            context.clockhand_pathway[depth] = store;
        }

        // bufidx_pathway is empty at first, so it must always be set
        context.bufidx_pathway[depth] = root_idx;

        W_DO(_evict_traverse_store(context));
        if (context.is_enough()) {
            return RCOK;
        }
    }
    // exhaustively checked this volume. this volume is 'done'
    context.clockhand_current_depth = depth;
    return RCOK;
}

w_rc_t bf_tree_m::_evict_traverse_store(EvictionContext &context) {
    uint32_t depth = context.traverse_depth;
    w_assert1(depth == 2);
    w_assert1(context.clockhand_current_depth >= depth);
    w_assert1(context.clockhand_pathway[depth - 1] != 0);
    w_assert1(context.bufidx_pathway[depth - 1] != 0);
    bf_idx root_idx = context.bufidx_pathway[depth - 1];
    btree_page_h root_p;
    root_p.fix_nonbufferpool_page(&_buffer[root_idx]);
    uint32_t child_count = (uint32_t) root_p.nrecs() + 1;
    if (child_count == 0) {
        return RCOK;
    }

    if (root_p.is_leaf()) {
        // happens for very small trees
        return RCOK;
    }

    // root is never evicted/unswizzled, so it's simpler.
    uint32_t old = context.clockhand_pathway[depth];
    for (uint32_t i = 0; i < child_count; ++i) {
        uint32_t slot = (old + i) % child_count;
        context.traverse_depth = depth + 1;
        if (i != 0 || context.clockhand_current_depth == depth) {
            // this means now we are moving on to another child.
            bool swizzled;
            bf_idx idx;
            _lookup_buf_imprecise(root_p, slot, idx, swizzled);
            if (idx == 0 || idx >= _block_cnt) {
                continue;
            }

            context.clockhand_current_depth = depth + 1; // reset descendants
            context.clockhand_pathway[depth] = slot;
            context.bufidx_pathway[depth] = idx;
            context.swizzled_pathway[depth] = swizzled;
        }

        W_DO(_evict_traverse_page (context));
        if (context.is_enough()) {
            return RCOK;
        }
    }

    // exhaustively checked this store. this store is 'done'
    context.clockhand_current_depth = depth;
    return RCOK;
}

w_rc_t bf_tree_m::_evict_traverse_page(EvictionContext &context) {
    const uint16_t depth = context.traverse_depth;
    w_assert1(depth >= 3);
    w_assert1(depth < MAX_CLOCKHAND_DEPTH);
    w_assert1(context.clockhand_current_depth >= depth);

    bf_idx idx = context.bufidx_pathway[depth - 1];
    bf_tree_cb_t &cb = get_cb(idx);
    if (!cb._used) {
        return RCOK;
    }

    btree_page_h p;
    p.fix_nonbufferpool_page(_buffer + idx);
    if (!p.is_leaf()) {
        uint32_t old = context.clockhand_pathway[depth];
        uint32_t child_count = (uint32_t) p.nrecs() + 1;
        if (child_count > 0) {
            // check children
            for (uint32_t i = 0; i < child_count; ++i) {
                uint32_t slot = (old + i) % child_count;
                bf_idx child_idx;
                context.traverse_depth = depth + 1;
                if (i != 0 || context.clockhand_current_depth == depth) {
                    bool swizzled;
                    _lookup_buf_imprecise(p, slot, child_idx, swizzled);
                    if (child_idx == 0) {
                        continue;
                    }

                    context.clockhand_current_depth = depth + 1;
                    context.clockhand_pathway[depth] = slot;
                    context.bufidx_pathway[depth] = child_idx;
                    context.swizzled_pathway[depth] = swizzled;
                }

                W_DO(_evict_traverse_page(context));
                if (context.is_enough()) {
                    return RCOK;
                }
            }
        }
    }

    // exhaustively checked this node's descendants. this node is 'done'
    context.clockhand_current_depth = depth;

    // consider evicting this page itself
    if (!cb._dirty) {
        W_DO(_evict_page(context, p));
    }
    return RCOK;
}

w_rc_t bf_tree_m::_evict_page(EvictionContext& context, btree_page_h& p) {
    bf_idx idx = context.bufidx_pathway[context.traverse_depth - 1];
    uint32_t slot = context.clockhand_pathway[context.traverse_depth - 1];
    bool swizzled = context.swizzled_pathway[context.traverse_depth - 1];
    bf_idx parent_idx = context.bufidx_pathway[context.traverse_depth - 2];

    bf_tree_cb_t &cb = get_cb(idx);
    DBGOUT1(<<"_evict_page: idx=" << idx << ", slot=" << slot << ", pid=" << p.pid()
        << ", swizzled=" << swizzled << ", hot-ness=" << cb._refbit_approximate
        << ", dirty=" << cb._dirty << " parent_idx=" << parent_idx);

    // do not evict hot pages, dirty pages, in_doubt pages
    if (!cb._used || cb._dirty || cb._in_doubt) {
        return RCOK;
    }
    if (context.urgency >= EVICT_URGENT) {
        // If in hurry, everyone is victim
        cb._refbit_approximate = 0;
    } else if (cb._refbit_approximate > 0) {
        // decrease refbit for exponential to how long time we are repeating this
        uint32_t decrease = (1 << context.rounds);
        if (decrease > cb._refbit_approximate) {
            cb._refbit_approximate = 0; // still victm
        } else {
            cb._refbit_approximate -= decrease;
            return RCOK; // enough hot
        }
    }

    // do we want to unswizzle this?
    if (context.is_unswizzling() && swizzled && cb._swizzled_ptr_cnt_hint == 0
        && p.is_leaf()) { // so far we don't consider unswizzling intermediate nodes.
        // this is just a hint. try conditionally latching the child and do the actual check
        w_rc_t latch_rc = cb.latch().latch_acquire(LATCH_SH, sthread_t::WAIT_IMMEDIATE);
        if (latch_rc.is_error()) {
            DBGOUT2(<<"unswizzle: oops, unlucky. someone is latching this page. skipping this."
                        " rc=" << latch_rc);
        } else {
            if (!has_swizzled_child(idx)) {
                // unswizzle_a_frame will try to conditionally latch a parent while
                // we hold a latch on a child. While this is latching in the reverse order,
                // it is still safe against deadlock as the operation is conditional.
                if (_unswizzle_a_frame (parent_idx, slot)) {
                    ++context.unswizzled_count;
                }
            }
            cb.latch().latch_release();
        }
    }

    // do we want to evict this page?
    if (_try_evict_block(parent_idx, idx)) {
        _add_free_block(idx);
        ++context.evicted_count;
    }
    return RCOK;
}
