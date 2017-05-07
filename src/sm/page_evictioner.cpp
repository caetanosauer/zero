#include "page_evictioner.h"

#include "bf_tree.h"
#include "log_core.h"
#include "btree_page_h.h"

// Template definitions
#include "bf_hashtable.cpp"

constexpr unsigned MAX_ROUNDS = 1000;

page_evictioner_base::page_evictioner_base(bf_tree_m* bufferpool, const sm_options& options)
    :
    worker_thread_t(options.get_int_option("sm_evictioner_interval_millisec", 1000)),
    _bufferpool(bufferpool)
{
    _swizzling_enabled = options.get_bool_option("sm_bufferpool_swizzle", false);
    _maintain_emlsn = options.get_bool_option("sm_bf_maintain_emlsn", false);
    _current_frame = 0;
}

page_evictioner_base::~page_evictioner_base()
{
}

void page_evictioner_base::do_work()
{
    uint32_t preferred_count = EVICT_BATCH_RATIO * _bufferpool->_block_cnt + 1;

    // In principle, _freelist_len requires a fence, but here it should be OK
    // because we don't need to read a consistent value every time.
    while(_bufferpool->_freelist_len < preferred_count)
    {
        DBG5(<< "Waiting for pick_victim...");
        bf_idx victim = pick_victim();
        DBG5(<< "Found victim idx=" << victim);

        if(victim == 0) {
            /* idx 0 is never used, so this means pick_victim() exited without
             * finding a victim. This might happen when the page_evictioner is
             * shutting down, for example. */
            notify_all();
            return;
        }

        w_assert1(victim != 0);

        bf_tree_cb_t& cb = _bufferpool->get_cb(victim);
        w_assert1(cb.latch().is_mine());

        if (!unswizzle_and_update_emlsn(victim)) {
            /* We were not able to unswizzle/update parent, therefore we cannot
             * proceed with this victim. We just jump to the next iteration and
             * hope for better luck next time. */
            cb.latch().latch_release();
            continue;
        }

        // Try to atomically set pin from 0 to -1; give up if it fails
        if (!cb.prepare_for_eviction()) {
            cb.latch().latch_release();
            continue;
        }

        // remove it from hashtable.
        w_assert1(cb._pid ==  _bufferpool->_buffer[victim].pid);
        w_assert1(cb._pin_cnt < 0);
        bool removed = _bufferpool->_hashtable->remove(cb._pid);
        w_assert1(removed);

        // DBG2(<< "EVICTED " << victim << " pid " << pid
        //                          << " log-tail " << smlevel_0::log->curr_lsn());

        _bufferpool->_add_free_block(victim);

        cb.latch().latch_release();

        INC_TSTAT(bf_evict);

        /* Rather than waiting for all the pages to be evicted, we notify
         * waiting threads every time a page is evicted. One of them is going to
         * be able to re-use the freed slot, the others will go back to waiting.
         */
        notify_one();
    }

    // cerr << "Eviction done; free frames: " << _bufferpool->_freelist_len << endl;
}

void page_evictioner_base::ref(bf_idx) {}

bf_idx page_evictioner_base::pick_victim()
{
    /*
     * CS: strategy is to try acquiring an EX latch imediately. If it works,
     * page is not that busy, so we can evict it. But only evict leaf pages.
     * This is like a random policy that only evicts uncontented pages. It is
     * not as effective as LRU or CLOCK, but it is better than RANDOM, simple
     * to implement and, most importantly, does not have concurrency bugs!
     */

    bool ignore_dirty = _bufferpool->is_no_db_mode() || _bufferpool->_write_elision;

     bf_idx idx = _current_frame;
     unsigned rounds = 0;
     while(true) {

        if (should_exit()) return 0; // in bf_tree.h, 0 is never used, means null

        if (idx == _bufferpool->_block_cnt) { idx = 1; }

        if (idx == _current_frame - 1) {
            // We iterate over all pages and no victim was found.
            DBG3(<< "Eviction did a full round");
            _bufferpool->wakeup_cleaner(false);
            if (rounds++ == MAX_ROUNDS) {
                W_FATAL_MSG(fcINTERNAL, << "Eviction got stuck!");
            }
            INC_TSTAT(bf_eviction_stuck);
        }

        auto& cb = _bufferpool->get_cb(idx);

        if (!cb._used) {
            idx++;
            continue;
        }

        // Step 1: latch page in EX mode and check if eligible for eviction
        rc_t latch_rc;
        latch_rc = cb.latch().latch_acquire(LATCH_EX, timeout_t::WAIT_IMMEDIATE);
        if (latch_rc.is_error()) {
            idx++;
            DBG3(<< "Eviction failed on latch for " << idx);
            continue;
        }
        w_assert1(cb.latch().is_mine());

        // now we hold an EX latch -- check if leaf and not dirty
        btree_page_h p;
        p.fix_nonbufferpool_page(_bufferpool->_buffer + idx);
        if (p.tag() != t_btree_p || !p.is_leaf()
                || (!ignore_dirty && cb.is_dirty())
                || !cb._used || p.pid() == p.root() || p.get_foster() != 0)
        {
            cb.latch().latch_release();
            DBG5(<< "Eviction failed on flags for " << idx);
            idx++;
            continue;
        }

        // page is a B-tree leaf -- check if pin count is zero
        if (cb._pin_cnt != 0)
        {
            // pin count -1 means page was already evicted
            cb.latch().latch_release();
            DBG3(<< "Eviction failed on for " << idx
                    << " pin count is " << cb._pin_cnt);
            idx++;
            continue;
        }
        w_assert1(_bufferpool->_is_active_idx(idx));

        // If we got here, we passed all tests and have a victim!
        _current_frame = idx +1;
        return idx;
    }
}

bool page_evictioner_base::unswizzle_and_update_emlsn(bf_idx idx)
{
    if (!_maintain_emlsn && !_swizzling_enabled) { return true; }

    bf_tree_cb_t& cb = _bufferpool->get_cb(idx);
    w_assert1(cb.latch().is_mine());

    //==========================================================================
    // STEP 1: Look for parent.
    //==========================================================================
    PageID pid = _bufferpool->_buffer[idx].pid;
    bf_idx_pair idx_pair;
    bool found = _bufferpool->_hashtable->lookup(pid, idx_pair);
    w_assert1(found);

    bf_idx parent_idx = idx_pair.second;
    w_assert1(!found || idx == idx_pair.first);

    // If there is no parent, but write elision is off and the frame is not swizzled,
    // then it's OK to evict
    if (parent_idx == 0) {
        return !_bufferpool->_write_elision && !cb._swizzled;
    }

    bf_tree_cb_t& parent_cb = _bufferpool->get_cb(parent_idx);
    rc_t r = parent_cb.latch().latch_acquire(LATCH_EX, timeout_t::WAIT_IMMEDIATE);
    if (r.is_error()) {
        /* Just give up. If we try to latch it unconditionally, we may deadlock,
         * because other threads are also waiting on the eviction mutex. */
        return false;
    }
    w_assert1(parent_cb.latch().is_mine());

    /* Look for emlsn slot on parent (must be found because parent pointer is
     * kept consistent at all times). */
    w_assert1(_bufferpool->_is_active_idx(parent_idx));
    generic_page *parent = &_bufferpool->_buffer[parent_idx];
    btree_page_h parent_h;
    parent_h.fix_nonbufferpool_page(parent);

    general_recordid_t child_slotid;
    if (_swizzling_enabled && cb._swizzled) {
        // Search for swizzled address
        PageID swizzled_pid = idx | SWIZZLED_PID_BIT;
        child_slotid = _bufferpool->find_page_id_slot(parent, swizzled_pid);
    }
    else {
        child_slotid = _bufferpool->find_page_id_slot(parent, pid);
    }
    w_assert1 (child_slotid != GeneralRecordIds::INVALID);

    //==========================================================================
    // STEP 2: Unswizzle pointer on parent before evicting.
    //==========================================================================
    if (_swizzling_enabled && cb._swizzled) {
        bool ret = _bufferpool->unswizzle(parent, child_slotid);
        w_assert0(ret);
        w_assert1(!cb._swizzled);
    }

    //==========================================================================
    // STEP 3: Page will be evicted -- update EMLSN on parent.
    //==========================================================================
    lsn_t old = parent_h.get_emlsn_general(child_slotid);
    _bufferpool->_buffer[idx].lsn = cb.get_page_lsn();
    if (_maintain_emlsn && old < _bufferpool->_buffer[idx].lsn) {
        DBG3(<< "Updated EMLSN on page " << parent_h.pid()
                << " slot=" << child_slotid
                << " (child pid=" << pid << ")"
                << ", OldEMLSN=" << old << " NewEMLSN=" <<
                _bufferpool->_buffer[idx].lsn);

        w_assert1(parent_cb.latch().is_mine());

        _bufferpool->_sx_update_child_emlsn(parent_h, child_slotid,
                                            _bufferpool->_buffer[idx].lsn);

        w_assert1(parent_h.get_emlsn_general(child_slotid)
                    == _bufferpool->_buffer[idx].lsn);
    }

    parent_cb.latch().latch_release();
    return true;
}

page_evictioner_gclock::page_evictioner_gclock(bf_tree_m* bufferpool, const sm_options& options)
    : page_evictioner_base(bufferpool, options)
{
    _k = options.get_int_option("sm_bufferpool_gclock_k", 10);
    _counts = new uint16_t [_bufferpool->_block_cnt];
    _current_frame = 0;

}

page_evictioner_gclock::~page_evictioner_gclock()
{
    delete [] _counts;
}

void page_evictioner_gclock::ref(bf_idx idx)
{
    _counts[idx] = _k;
}

bf_idx page_evictioner_gclock::pick_victim()
{
    // Check if we still need to evict
    bf_idx idx = _current_frame;
    while(true)
    {
        if(should_exit()) return 0; // bf_idx 0 is never used, means NULL

        // Circular iteration, jump idx 0
        idx = (idx % (_bufferpool->_block_cnt-1)) + 1;
        w_assert1(idx != 0);

        // Before starting, let's fire some prefetching for the next step.
        bf_idx next_idx = ((idx+1) % (_bufferpool->_block_cnt-1)) + 1;
        __builtin_prefetch(&_bufferpool->_buffer[next_idx]);
        __builtin_prefetch(_bufferpool->get_cbp(next_idx));

        // Now we do the real work.
        bf_tree_cb_t& cb = _bufferpool->get_cb(idx);

        rc_t latch_rc = cb.latch().latch_acquire(LATCH_SH, timeout_t::WAIT_IMMEDIATE);
        if (latch_rc.is_error())
        {
            idx++;
            continue;
        }

        w_assert1(cb.latch().held_by_me());

        /* There are some pages we want to ignore in our policy:
         * 1) Non B+Tree pages
         * 2) Dirty pages (the cleaner should have cleaned it already)
         * 3) Pages being used by someon else
         * 4) The root
         */
        btree_page_h p;
        p.fix_nonbufferpool_page(_bufferpool->_buffer + idx);
        if (p.tag() != t_btree_p || cb.is_dirty() ||
            !cb._used || p.pid() == p.root())
        {
            // LL: Should we also decrement the clock count in this case?
            cb.latch().latch_release();
            idx++;
            continue;
        }

        // Ignore pages that still have swizzled children
        if(_swizzling_enabled && _bufferpool->has_swizzled_child(idx))
        {
            // LL: Should we also decrement the clock count in this case?
            cb.latch().latch_release();
            idx++;
            continue;
        }

        if(_counts[idx] <= 0)
        {
            // We have found our victim!
            bool would_block;
            cb.latch().upgrade_if_not_block(would_block); //Try to upgrade latch
            if(!would_block) {
                w_assert1(cb.latch().is_mine());

                /* No need to re-check the values above, because the cb was
                 * already latched in SH mode, so they cannot change. */

                if (cb._pin_cnt != 0) {
                    cb.latch().latch_release(); // pin count -1 means page was already evicted
                    idx++;
                    continue;
                }

                _current_frame = idx + 1;
                return idx;
            }
        }
        cb.latch().latch_release();
        --_counts[idx]; //TODO: MAKE ATOMIC
        idx++;
    }
}
