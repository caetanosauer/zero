#include "bf_tree_cb.h"
#include "bf_tree.h"
#include "btree_page_h.h"

#include "bf_hashtable.cpp"

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
                DBGTHRD(<< "Grabbing idx " << idx);
                w_assert1(_is_valid_idx(idx));
                // w_assert1 (!get_cb(idx)._used);
                ret = idx;

                --_freelist_len;
                if (_freelist_len == 0) {
                    FREELIST_HEAD = 0;
                } else {
                    FREELIST_HEAD = _freelist[idx];
                    w_assert1 (FREELIST_HEAD > 0 && FREELIST_HEAD < _block_cnt);
                }
                DBGTHRD(<< "New head " << FREELIST_HEAD);
                w_assert1(ret != FREELIST_HEAD);
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

    /*
     * Changed clenaer behavior to be less aggressive and more persistent,
     * i.e., always run with normal urgency but keep trying until free frames
     * are available. This behavior plays nicer with instant restore.
     */

    // evict with gradually higher urgency
    int urgency = EVICT_NORMAL;
    while (true) {
    // for (int urgency = EVICT_NORMAL; urgency <= EVICT_COMPLETE; ++urgency) {
        W_DO(wakeup_cleaners());
        W_DO(evict_blocks(evicted_count, unswizzled_count, (evict_urgency_t) urgency));
        if (evicted_count > 0 || _freelist_len > 0) {
            return RCOK;
        }
        g_me()->sleep(100);
        DBGOUT1(<<"woke up. now there should be some page to evict. urgency=" << urgency);
        // debug_dump(std::cout);
    }

    return RC(eFRAMENOTFOUND);
}


void bf_tree_m::_add_free_block(bf_idx idx)
{
    CRITICAL_SECTION(cs, &_freelist_lock);
    // CS TODO: Eviction is apparently broken, since I'm seeing the same
    // frame being freed twice by two different threads.
    w_assert1(idx != FREELIST_HEAD);
    ++_freelist_len;
    _freelist[idx] = FREELIST_HEAD;
    FREELIST_HEAD = idx;
}

w_rc_t bf_tree_m::evict_blocks(uint32_t& evicted_count,
        uint32_t& unswizzled_count, evict_urgency_t /* urgency */,
        uint32_t preferred_count)
{
    if (preferred_count == 0) {
        preferred_count = EVICT_BATCH_RATIO * _block_cnt + 1;
    }

    CRITICAL_SECTION(cs, &_eviction_lock);

    // CS once mutex is finally acquired, check if we still need to evict
    // CS TODO: using an arbitrary number as the target free count. This
    // should probably be something like the number of threads plus some
    // factor
    if (_freelist_len >= preferred_count) {
        evicted_count = 0;
        unswizzled_count = 0;
        return RCOK;
    }

    bf_idx idx = _eviction_current_frame;
    evicted_count = 0;

    unsigned rounds = 0;
    unsigned nonleaf_count = 0;
    unsigned invalid_parents = 0;

    /*
     * CS: strategy is to try acquiring an EX latch imediately. If it works,
     * page is not that busy, so we can evict it. But only evict leaf pages.
     * This is like a random policy that only evicts uncontented pages. It is
     * not as effective as LRU or CLOCK, but it is better than RANDOM, simple
     * to implement and, most importantly, does not have concurrency bugs!
     */
    while (evicted_count < preferred_count) {
        if (idx == _block_cnt) {
            idx = 0;
        }
        // one round and no eviction yet -- error
        // TODO CS: how to handle this?
        if (idx == _eviction_current_frame - 1) {
            if (evicted_count == 0) {
                ERROUT(<< "Nonleafs: " << nonleaf_count << " invalid parents: "
                        << invalid_parents);
                rounds++;
                if (rounds == 3) {
                    w_assert1(false);
                    return RC(eFRAMENOTFOUND);
                }
            }
            else { return RCOK; }
        }

        // CS TODO -- why do we latch CB manually instead of simply fixing
        // the page??
        // CS TODO: if parent pointer is invalid, we can assume the
        // pointer is unswizzled and ignore the parent below

        bf_tree_cb_t& cb = get_cb(idx);
        rc_t latch_rc;

        // Step 1: latch page in EX mode and check if eligible for eviction
        latch_rc = cb.latch().latch_acquire(LATCH_EX,
               sthread_t::WAIT_IMMEDIATE);
        if (latch_rc.is_error()) {
            idx++;
            DBG3(<< "Eviction failed on latch for " << idx);
            continue;
        }
        w_assert1(cb.latch().held_by_me());

        // now we hold an EX latch -- check if leaf and not dirty
        btree_page_h p;
        p.fix_nonbufferpool_page(_buffer + idx);
        // CS TODO: ignoring used flag for now
        if (p.tag() != t_btree_p || !p.is_leaf() || cb._dirty
                || cb._in_doubt || p.pid() == p.root())
        {
            cb.latch().latch_release();
            DBG3(<< "Eviction failed on flags for " << idx);
            if (!p.is_leaf()) {
                nonleaf_count++;
            }
            idx++;
            continue;
        }

        // page is a B-tree leaf -- check if pin count is zero
        if (cb._pin_cnt > 0)
        {
            cb.latch().latch_release();
            DBG3(<< "Eviction failed on for " << idx
                    << " pin count is " << cb._pin_cnt);
            idx++;
            continue;
        }

        // Step 2: latch parent in SH mode (latch coupling)
        generic_page *page = &_buffer[idx];
        lpid_t pid = page->pid;
        bf_idx_pair idx_pair;
        bool found = _hashtable->lookup(bf_key(pid.vol(), pid.page), idx_pair);
        bf_idx parent_idx = idx_pair.second;
        w_assert1(!found || idx == idx_pair.first);

        // Index zero is never used, so it means invalid pointer
        // (see bf_tree_m constructor)
        if (!found || parent_idx == 0) {
            cb.latch().latch_release();
            DBG3(<< "Eviction failed on parent idx for " << idx);
            idx++;
            continue;
        }

        // parent is latched in SH mode, OK because it just overwrites an emlsn
        bf_tree_cb_t& parent_cb = get_cb(parent_idx);
        latch_rc = parent_cb.latch().latch_acquire(LATCH_SH,
                sthread_t::WAIT_IMMEDIATE);
        if (latch_rc.is_error()) {
            // just give up. If we try to latch it unconditionally, we may
            // deadlock, because other threads are also waiting on the eviction
            // mutex
            cb.latch().latch_release();
            DBG3(<< "Eviction failed on parent latch for " << idx);
            idx++;
            continue;
        }
        w_assert1(parent_cb.latch().held_by_me());

        // Step 3: look for emlsn slot on parent
        generic_page *parent = &_buffer[parent_idx];
        w_assert0(parent->tag == t_btree_p);

        general_recordid_t child_slotid = find_page_id_slot(parent, cb._pid_shpid);
        // How can this happen if we have latch on both?
        if (child_slotid == GeneralRecordIds::INVALID) {
            DBG3(<< "Eviction failed on slot for " << idx
                    << " pin count is " << cb._pin_cnt);
            parent_cb.latch().latch_release();
            cb.latch().latch_release();
            invalid_parents++;
            idx++;
            continue;
        }

        // Step 4: Page will be evicted -- update EMLSN on parent
        btree_page_h parent_h;
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

        // eviction finally suceeded

        // remove it from hashtable.
        bool removed = _hashtable->remove(bf_key(cb._pid_vol, cb._pid_shpid));
        w_assert1(removed);

        DBG3(<< "EVICTED " << idx << " pid " << cb._pid_shpid);
        cb.clear_except_latch();
        // -1 indicates page was evicted (i.e., it's invalid and can be read into)
        cb._pin_cnt = -1;
        parent_cb.latch().latch_release();
        cb.latch().latch_release();

        _add_free_block(idx);
        idx++;
        evicted_count++;
    }

    _eviction_current_frame = idx;
    return RCOK;
}
