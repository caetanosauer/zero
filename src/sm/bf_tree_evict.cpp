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
        if (evict)
        {
            W_DO (_get_replacement_block());
        }
        else
        {
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
    W_DO(wakeup_cleaners());
    if (preferred_count == 0) {
        preferred_count = EVICT_BATCH_RATIO * _block_cnt + 1;
    }

    CRITICAL_SECTION(cs, &_eviction_lock);

    // CS once mutex is finally acquired, check if we still need to evict
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
    unsigned dirty_count = 0;

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
        if (idx == _eviction_current_frame - 1) {
            W_DO(wakeup_cleaners());
            if (evicted_count == 0) {
                DBG(<< "Eviction stuck! Nonleafs: " << nonleaf_count
                        << " invalid parents: " << invalid_parents
                        << " dirty: " << dirty_count);
                rounds++;
                usleep(5000); //5ms
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
        if (p.tag() != t_btree_p || !p.is_leaf() || cb._dirty
                || !cb._used || p.pid() == p.root())
        {
            cb.latch().latch_release();
            DBG3(<< "Eviction failed on flags for " << idx);
            if (!p.is_leaf()) { nonleaf_count++; }
            if (cb._dirty) { dirty_count++; }
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

        // Step 2: latch parent in SH mode
        generic_page *page = &_buffer[idx];
        PageID pid = page->pid;
        w_assert1(cb._pin_cnt < 0 || pid == cb._pid_shpid);

        bf_idx_pair idx_pair;
        bool found = _hashtable->lookup(pid, idx_pair);
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
        // CS TODO: this assertion fails sometimes, but then if I print
        // it on gdb right after, I guet true. NO IDEA what's happening!
        // w_assert0(parent->tag == t_btree_p);

        general_recordid_t child_slotid = find_page_id_slot(parent, pid);
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
            DBGOUT1(<< "Updated EMLSN on page " << parent_h.pid()
                    << " slot=" << child_slotid
                    << " (child pid=" << pid << ")"
                    << ", OldEMLSN=" << old << " NewEMLSN=" << _buffer[idx].lsn);
            w_assert1(parent_cb.latch().held_by_me());
            W_COERCE(_sx_update_child_emlsn(parent_h, child_slotid, _buffer[idx].lsn));
            // Note that we are not grabbing EX latch on parent here.
            // This is safe because no one else should be touching these exact bytes,
            // and because EMLSN values are aligned to be "regular register".
            // However, we still need to mark the frame as dirty.
            //
            // CS TODO: the SH latch is OK as long as no other thread also
            // updates the page LSN with an SH latch. It should not be the
            // case, but I have observed this in experiments, namely an adopt
            // log record where the page2-prev (i.e, child chain) and a
            // page_evict on the same page point to the same log record. This
            // could only happen if the two log records are being generated at
            // the same time with a race, i.e., the adopt does NOT have an EX
            // latch on the child! The adopt code has an assertion that child
            // has EX latch, so I don't know what's happening!
            // - Perhaps the SH latch release does not issue a memory fence?
            //    Nope -- it does issue memory fence
            set_dirty(parent);
            w_assert1(parent_h.get_emlsn_general(child_slotid) == _buffer[idx].lsn);
        }

        // eviction finally suceeded

        // remove it from hashtable.
        bool removed = _hashtable->remove(pid);
        w_assert1(removed);

        DBG3(<< "EVICTED " << idx << " pid " << pid);
        cb.clear_except_latch();
        // -1 indicates page was evicted (i.e., it's invalid and can be read into)
        cb._pin_cnt = -1;

        _add_free_block(idx);
        idx++;
        evicted_count++;

        parent_cb.latch().latch_release();
        cb.latch().latch_release();

        INC_TSTAT(bf_evict);
    }

    _eviction_current_frame = idx;
    return RCOK;
}
