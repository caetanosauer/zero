#include "page_evictioner.h"

#include "bf_tree.h"
#include "bf_hashtable.cpp"
#include "generic_page.h"

page_evictioner_base::page_evictioner_base(bf_tree_m* bufferpool, const sm_options& options)
    :
    worker_thread_t(_options.get_int_option("sm_evictioner_interval_millisec", 1000)),
    _bufferpool(bufferpool)
{
    _swizziling_enabled = _options.get_bool_option("sm_bufferpool_swizzle", false);
    _current_frame = 0;
}

page_evictioner_base::~page_evictioner_base()
{
}

void page_evictioner_base::do_work()
{
    uint32_t preferred_count = EVICT_BATCH_RATIO * _bufferpool->_block_cnt + 1;
    
    uint32_t evicted_count = 0;
    unsigned rounds = 0;
    unsigned nonleaf_count = 0;
    unsigned dirty_count = 0;

    bf_idx idx = _current_frame;
    while(_bufferpool->_freelist_len < preferred_count) // TODO: increment _freelist_len atomically
    {
        /*
         * CS: strategy is to try acquiring an EX latch imediately. If it works,
         * page is not that busy, so we can evict it. But only evict leaf pages.
         * This is like a random policy that only evicts uncontented pages. It is
         * not as effective as LRU or CLOCK, but it is better than RANDOM, simple
         * to implement and, most importantly, does not have concurrency bugs!
         */

         if (idx == _bufferpool->_block_cnt) {
             idx = 1;	// idx 0 is never used
         }

         if (idx == _current_frame - 1) {
             // Wake up and wait for cleaner
             _bufferpool->get_cleaner()->wakeup(true);
             if (rounds > 0 && evicted_count == 0) {
                 DBG(<< "Eviction stuck! Nonleafs: " << nonleaf_count
                         << " dirty: " << dirty_count);
                 nonleaf_count = dirty_count = 0;
                 rounds++;
             }
             else if (evicted_count > 0) {
                 // best-effort approach: sorry, we evicted as many as we could
                 return;
             }
         }

        // CS TODO -- why do we latch CB manually instead of simply fixing
        // the page??

        bf_tree_cb_t& cb = _bufferpool->get_cb(idx);

        // Step 1: latch page in EX mode and check if eligible for eviction
        rc_t latch_rc;         
        latch_rc = cb.latch().latch_acquire(LATCH_EX, sthread_t::WAIT_IMMEDIATE);
        if (latch_rc.is_error()) {
            idx++;
            DBG3(<< "Eviction failed on latch for " << idx);
            continue;
        }
        w_assert1(cb.latch().held_by_me())

        // now we hold an EX latch -- check if leaf and not dirty
        btree_page_h p;
        p.fix_nonbufferpool_page(_bufferpool->_buffer + idx);
        if (p.tag() != t_btree_p || !p.is_leaf() || cb.is_dirty()
                || !cb._used || p.pid() == p.root() || p.get_foster() != 0)
        {
            cb.latch().latch_release();
            DBG5(<< "Eviction failed on flags for " << idx);
            if (!p.is_leaf()) { nonleaf_count++; }
            if (cb.is_dirty()) { dirty_count++; }
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

        // Step 2: latch parent in SH mode
        generic_page *page = &_bufferpool->_buffer[idx];
        PageID pid = page->pid;
        w_assert1(cb._pin_cnt < 0 || pid == cb._pid);

        bf_idx_pair idx_pair;
        bool found = _bufferpool->_hashtable->lookup(pid, idx_pair);
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
        // CS TODO: bugfix -- latch must be acquired in EX mode because we will
        // generate a log record, which will update the PageLSN and set the
        // previous-page pointer based on the old value. This means that all
        // operations that update the PageLSN must be serialized: thus the EX
        // mode is required.
        bf_tree_cb_t& parent_cb = _bufferpool->get_cb(parent_idx);
        // latch_rc = parent_cb.latch().latch_acquire(LATCH_SH,
        latch_rc = parent_cb.latch().latch_acquire(LATCH_EX,
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
        w_assert1(parent_cb.latch().mode() == LATCH_EX);

        // Parent may have been evicted if it is a foster parent of a leaf node.
        // In that case, parent_cb._used will be false

        // Step 3: look for emlsn slot on parent (must be found because parent
        // pointer is kept consistent at all times)
        w_assert1(_bufferpool->_is_active_idx(parent_idx));
        generic_page *parent = &_bufferpool->_buffer[parent_idx];
        btree_page_h parent_h;
        parent_h.fix_nonbufferpool_page(parent);

        general_recordid_t child_slotid;
        bool is_swizzled = cb._swizzled;
        if (is_swizzled) {
            // Search for swizzled address
            PageID swizzled_pid = idx | SWIZZLED_PID_BIT;
            child_slotid = _bufferpool->find_page_id_slot(parent, swizzled_pid);
        }
        else {
            child_slotid = _bufferpool->find_page_id_slot(parent, pid);
        }
        w_assert1 (child_slotid != GeneralRecordIds::INVALID);

        // Unswizzle pointer on parent before evicting
        if (is_swizzled) {
            bool ret = _bufferpool->unswizzle(parent, child_slotid);
            w_assert0(ret);
            w_assert1(!cb._swizzled);
        }

        // Step 4: Page will be evicted -- update EMLSN on parent
        lsn_t old = parent_h.get_emlsn_general(child_slotid);
        _bufferpool->_buffer[idx].lsn = cb.get_page_lsn();
        if (old < _bufferpool->_buffer[idx].lsn) {
            DBG3(<< "Updated EMLSN on page " << parent_h.pid()
                    << " slot=" << child_slotid
                    << " (child pid=" << pid << ")"
                    << ", OldEMLSN=" << old << " NewEMLSN=" << 
                    _bufferpool->_buffer[idx].lsn);

            w_assert1(parent_cb.latch().held_by_me());
            w_assert1(parent_cb.latch().mode() == LATCH_EX);

            W_COERCE(_bufferpool->_sx_update_child_emlsn(parent_h, child_slotid,
                                                _bufferpool->_buffer[idx].lsn));
            
            w_assert1(parent_h.get_emlsn_general(child_slotid) 
                        ==
                        _bufferpool->_buffer[idx].lsn);
        }

        // eviction finally suceeded

        // remove it from hashtable.
        bool removed = _bufferpool->_hashtable->remove(pid);
        w_assert1(removed);

        DBG2(<< "EVICTED " << idx << " pid " << pid
                << " log-tail " << smlevel_0::log->curr_lsn());
        cb.clear_except_latch();
        //-1 indicates page was evicted(i.e., it's invalid and can be read into)
        cb._pin_cnt = -1;

        _bufferpool->_add_free_block(idx);
        idx++;
        evicted_count++;

        parent_cb.latch().latch_release();
        cb.latch().latch_release();

        INC_TSTAT(bf_evict);

        /* Rather than waiting for all the pages to be evicted, we 
        * notify waiting threads every time a page is evicted. One of
        * them is going to be able to re-use the freed slot, the others
        * will go back to waiting. */
        notify_one();
    }

    _current_frame = idx;
    return;
} 