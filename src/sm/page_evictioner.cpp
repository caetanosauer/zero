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

page_evictioner_base::~page_evictioner_base() {}

void page_evictioner_base::do_work()
{
    uint32_t preferred_count = EVICT_BATCH_RATIO * _bufferpool->_block_cnt + 1;

    while(_bufferpool->_approx_freelist_length < preferred_count)
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

    // cerr << "Eviction done; free frames: " << _bufferpool->_approx_freelist_length << endl;
}

void page_evictioner_base::hit_ref(bf_idx idx) {}

void page_evictioner_base::unfix_ref(bf_idx idx) {}

void page_evictioner_base::miss_ref(bf_idx b_idx, PageID pid) {}

void page_evictioner_base::used_ref(bf_idx idx) {}

void page_evictioner_base::dirty_ref(bf_idx idx) {}

void page_evictioner_base::block_ref(bf_idx idx) {}

void page_evictioner_base::swizzle_ref(bf_idx idx) {}

void page_evictioner_base::unbuffered(bf_idx idx) {}

bf_idx page_evictioner_base::pick_victim() {
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

        PageID evicted_page;
        if (evict_page(idx, evicted_page)) {
            w_assert1(_bufferpool->_is_active_idx(idx));
            _current_frame = idx + 1;
            
            return idx;
        } else {
            idx++;
            continue;
        }
    }
}

bool page_evictioner_base::evict_page(bf_idx idx, PageID &evicted_page) {
    bool ignore_dirty = _bufferpool->is_no_db_mode() || _bufferpool->_write_elision;
    
    // Step 1: Get the control block of the eviction candidate
    bf_tree_cb_t &cb = _bufferpool->get_cb(idx);
    evicted_page = cb._pid;
    
    // Step 2: Latch page in EX mode and check if eligible for eviction
    rc_t latch_rc;
    latch_rc = cb.latch().latch_acquire(LATCH_EX, timeout_t::WAIT_IMMEDIATE);
    if (latch_rc.is_error()) {
        idx++;
        DBG3(<< "Eviction failed on latch for " << idx);
        return false;
    }
    w_assert1(cb.latch().is_mine());
    
    btree_page_h p;
    p.fix_nonbufferpool_page(_bufferpool->_buffer + idx);
    
    // Step 3: Check if the page is evictable (root pages and non-b+tree pages cannot be evicted)
    if (p.tag() != t_btree_p || p.pid() == p.root() /* || !p.is_leaf() */) {
        block_ref(idx);
        cb.latch().latch_release();
        DBG3(<< "Eviction failed on page type for " << idx);
        return false;
    }
    
    // Step 4: Check if the page is dirty and therefore cannot be evicted (when write elision is not used)
    if (!ignore_dirty && cb.is_dirty()) {
        dirty_ref(idx);
        cb.latch().latch_release();
        DBG3(<< "Eviction failed on update propagation for " << idx);
        return false;
    }
    
    // Step 5: Check if the buffer frame is used as an empty buffer frame cannot be freed
    if (!cb._used) {
        unbuffered(idx);
        cb.latch().latch_release();
        DBG3(<< "Eviction failed on emptiness for " << idx);
        return false;
    }
    
    // Step 6: Check if the page contains a swizzled pointer as this prevents the eviction
    if (_swizzling_enabled && _bufferpool->has_swizzled_child(idx)) {
        swizzle_ref(idx);
        cb.latch().latch_release();
        DBG3(<< "Eviction failed on swizzled pointer for " << idx);
        return false;
    }
    
    // Step 7: Check if page is currently evictable (not pinned)
    if (p.get_foster() != 0 || cb._pin_cnt != 0) {
        used_ref(idx);
        cb.latch().latch_release();
        DBG3(<< "Eviction failed on usage for " << idx);
        return false;
    }
    
    return true;
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
             << ", OldEMLSN=" << old
             << " NewEMLSN=" << _bufferpool->_buffer[idx].lsn);

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

void page_evictioner_gclock::hit_ref(bf_idx idx) {
    _counts[idx] = _k;
}

void page_evictioner_gclock::unfix_ref(bf_idx idx) {
    _counts[idx] = _k;
}

void page_evictioner_gclock::miss_ref(bf_idx b_idx, PageID pid) {}

void page_evictioner_gclock::used_ref(bf_idx idx) {
    _counts[idx] = _k;
}

void page_evictioner_gclock::dirty_ref(bf_idx idx) {}

void page_evictioner_gclock::block_ref(bf_idx idx) {
    _counts[idx] = std::numeric_limits<uint16_t>::max();
}

void page_evictioner_gclock::swizzle_ref(bf_idx idx) {}

void page_evictioner_gclock::unbuffered(bf_idx idx) {
    _counts[idx] = 0;
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

page_evictioner_car::page_evictioner_car(bf_tree_m *bufferpool, const sm_options &options)
        : page_evictioner_base(bufferpool, options)
{
    _clocks = new multi_clock<bf_idx, bool>(_bufferpool->_block_cnt, 2, 0);
    
    _b1 = new hashtable_queue<PageID>(1 | SWIZZLED_PID_BIT);
    _b2 = new hashtable_queue<PageID>(1 | SWIZZLED_PID_BIT);
    
    _p = 0;
    _c = _bufferpool->_block_cnt - 1;
    
    _hand_movement = 0;
    
    DO_PTHREAD(pthread_mutex_init(&_lock, nullptr));
}

page_evictioner_car::~page_evictioner_car() {
    DO_PTHREAD(pthread_mutex_destroy(&_lock));
    
    delete(_clocks);
    
    delete(_b1);
    delete(_b2);
}

void page_evictioner_car::hit_ref(bf_idx idx) {}

void page_evictioner_car::unfix_ref(bf_idx idx) {
    _clocks->set(idx, true);
}

void page_evictioner_car::miss_ref(bf_idx b_idx, PageID pid) {
    DO_PTHREAD(pthread_mutex_lock(&_lock));
    if (!_b1->contains(pid) && !_b2->contains(pid)) {
        if (_clocks->size_of(T_1) + _b1->length() >= _c) {
            _b1->pop();
        } else if (_clocks->size_of(T_1) + _clocks->size_of(T_2) + _b1->length() + _b2->length() >= 2 * (_c)) {
            _b2->pop();
        }
        w_assert0(_clocks->add_tail(T_1, b_idx));
        DBG5(<< "Added to T_1: " << b_idx << "; New size: " << _clocks->size_of(T_1) << "; Free frames: " << _bufferpool->_approx_freelist_length);
        _clocks->set(b_idx, false);
    } else if (_b1->contains(pid)) {
        _p = std::min(_p + std::max(u_int32_t(1), (_b2->length() / _b1->length())), _c);
        w_assert0(_b1->remove(pid));
        w_assert0(_clocks->add_tail(T_2, b_idx));
        DBG5(<< "Added to T_2: " << b_idx << "; New size: " << _clocks->size_of(T_2) << "; Free frames: " << _bufferpool->_approx_freelist_length);
        _clocks->set(b_idx, false);
    } else {
        _p = std::max<int32_t>(int32_t(_p) - std::max<int32_t>(1, (_b1->length() / _b2->length())), 0);
        w_assert0(_b2->remove(pid));
        w_assert0(_clocks->add_tail(T_2, b_idx));
        DBG5(<< "Added to T_2: " << b_idx << "; New size: " << _clocks->size_of(T_2) << "; Free frames: " << _bufferpool->_approx_freelist_length);
        _clocks->set(b_idx, false);
    }
    w_assert1(0 <= _clocks->size_of(T_1) + _clocks->size_of(T_2) && _clocks->size_of(T_1) + _clocks->size_of(T_2) <= _c);
    w_assert1(0 <= _clocks->size_of(T_1) + _b1->length() && _clocks->size_of(T_1) + _b1->length() <= _c);
    w_assert1(0 <= _clocks->size_of(T_2) + _b2->length() && _clocks->size_of(T_2) + _b2->length() <= 2 * (_c));
    w_assert1(0 <= _clocks->size_of(T_1) + _clocks->size_of(T_2) + _b1->length() + _b2->length() && _clocks->size_of(T_1) + _clocks->size_of(T_2) + _b1->length() + _b2->length() <= 2 * (_c));
    DO_PTHREAD(pthread_mutex_unlock(&_lock));
}

void page_evictioner_car::used_ref(bf_idx idx) {
    hit_ref(idx);
}

void page_evictioner_car::dirty_ref(bf_idx idx) {}

void page_evictioner_car::block_ref(bf_idx idx) {}

void page_evictioner_car::swizzle_ref(bf_idx idx) {}

void page_evictioner_car::unbuffered(bf_idx idx) {
    DO_PTHREAD(pthread_mutex_lock(&_lock));
    _clocks->remove(idx);
    DO_PTHREAD(pthread_mutex_unlock(&_lock));
}

bf_idx page_evictioner_car::pick_victim() {
    bool evicted_page = false;
    u_int32_t blocked_t_1 = 0;
    u_int32_t blocked_t_2 = 0;
    
    while (!evicted_page) {
        if (_hand_movement >= _c) {
            _bufferpool->get_cleaner()->wakeup(false);
            DBG3(<< "Run Page_Cleaner ...");
            _hand_movement = 0;
        }
        u_int32_t iterations = (blocked_t_1 + blocked_t_2) / _c;
        if ((blocked_t_1 + blocked_t_2) % _c == 0 && (blocked_t_1 + blocked_t_2) > 0) {
            DBG1(<< "Iterated " << iterations << "-times in CAR's pick_victim().");
        }
        w_assert1(iterations < 3);
        DBG3(<< "p = " << _p);
        DO_PTHREAD(pthread_mutex_lock(&_lock));
        if ((_clocks->size_of(T_1) >= std::max<u_int32_t>(u_int32_t(1), _p) || blocked_t_2 >= _clocks->size_of(T_2)) && blocked_t_1 < _clocks->size_of(T_1)) {
            bool t_1_head = false;
            bf_idx t_1_head_index = 0;
            _clocks->get_head(T_1, t_1_head);
            _clocks->get_head_index(T_1, t_1_head_index);
            w_assert1(t_1_head_index != 0);
            
            if (!t_1_head) {
                PageID evicted_pid;
                evicted_page = evict_page(t_1_head_index, evicted_pid);
                
                if (evicted_page) {
                    w_assert0(_clocks->remove_head(T_1, t_1_head_index));
                    w_assert0(_b1->push(evicted_pid));
                    DBG5(<< "Removed from T_1: " << t_1_head_index << "; New size: " << _clocks->size_of(T_1) << "; Free frames: " << _bufferpool->_approx_freelist_length);
                    
                    DO_PTHREAD(pthread_mutex_unlock(&_lock));
                    return t_1_head_index;
                } else {
                    _clocks->move_head(T_1);
                    blocked_t_1++;
                    _hand_movement++;
                    DO_PTHREAD(pthread_mutex_unlock(&_lock));
                    continue;
                }
            } else {
                w_assert0(_clocks->set_head(T_1, false));
                
                _clocks->switch_head_to_tail(T_1, T_2, t_1_head_index);
                DBG5(<< "Removed from T_1: " << t_1_head_index << "; New size: " << _clocks->size_of(T_1) << "; Free frames: " << _bufferpool->_approx_freelist_length);
                DBG5(<< "Added to T_2: " << t_1_head_index << "; New size: " << _clocks->size_of(T_2) << "; Free frames: " << _bufferpool->_approx_freelist_length);
                DO_PTHREAD(pthread_mutex_unlock(&_lock));
                continue;
            }
        } else if (blocked_t_2 < _clocks->size_of(T_2)) {
            bool t_2_head = false;
            bf_idx t_2_head_index = 0;
            _clocks->get_head(T_2, t_2_head);
            _clocks->get_head_index(T_2, t_2_head_index);
            w_assert1(t_2_head_index != 0);
            
            if (!t_2_head) {
                PageID evicted_pid;
                evicted_page = evict_page(t_2_head_index, evicted_pid);
                
                if (evicted_page) {
                    w_assert0(_clocks->remove_head(T_2, t_2_head_index));
                    w_assert0(_b2->push(evicted_pid));
                    DBG5(<< "Removed from T_2: " << t_2_head_index << "; New size: " << _clocks->size_of(T_2) << "; Free frames: " << _bufferpool->_approx_freelist_length);
                    
                    DO_PTHREAD(pthread_mutex_unlock(&_lock));
                    return t_2_head_index;
                } else {
                    _clocks->move_head(T_2);
                    blocked_t_2++;
                    _hand_movement++;
                    DO_PTHREAD(pthread_mutex_unlock(&_lock));
                    continue;
                }
            } else {
                w_assert0(_clocks->set_head(T_2, false));
                
                _clocks->move_head(T_2);
                _hand_movement++;
                DO_PTHREAD(pthread_mutex_unlock(&_lock));
                continue;
            }
        } else {
            DO_PTHREAD(pthread_mutex_unlock(&_lock));
            return 0;
        }
        
        DO_PTHREAD(pthread_mutex_unlock(&_lock));
    }
    
    return 0;
}

template<class key>
bool hashtable_queue<key>::contains(key k) {
    return _direct_access_queue->count(k);
}

template<class key>
hashtable_queue<key>::hashtable_queue(key invalid_key) {
    _direct_access_queue = new std::unordered_map<key, key_pair>();
    _invalid_key = invalid_key;
    _back = _invalid_key;
    _front = _invalid_key;
}

template<class key>
hashtable_queue<key>::~hashtable_queue() {
    delete(_direct_access_queue);
    _direct_access_queue = nullptr;
}

template<class key>
bool hashtable_queue<key>::push(key k) {
    if (!_direct_access_queue->empty()) {
        auto old_size = _direct_access_queue->size();
        key old_back = _back;
        key_pair old_back_entry = (*_direct_access_queue)[old_back];
        w_assert1(old_back != _invalid_key);
        w_assert1(old_back_entry._next == _invalid_key);
        
        if (this->contains(k)) {
            return false;
        }
        (*_direct_access_queue)[k] = key_pair(old_back, _invalid_key);
        (*_direct_access_queue)[old_back]._next = k;
        _back = k;
        w_assert1(_direct_access_queue->size() == old_size + 1);
    } else {
        w_assert1(_back == _invalid_key);
        w_assert1(_front == _invalid_key);
        
        (*_direct_access_queue)[k] = key_pair(_invalid_key, _invalid_key);
        _back = k;
        _front = k;
        w_assert1(_direct_access_queue->size() == 1);
    }
    return true;
}

template<class key>
bool hashtable_queue<key>::pop() {
    if (_direct_access_queue->empty()) {
        return false;
    } else if (_direct_access_queue->size() == 1) {
        w_assert1(_back == _front);
        w_assert1((*_direct_access_queue)[_front]._next == _invalid_key);
        w_assert1((*_direct_access_queue)[_front]._previous == _invalid_key);
        
        _direct_access_queue->erase(_front);
        _front = _invalid_key;
        _back = _invalid_key;
        w_assert1(_direct_access_queue->size() == 0);
    } else {
        auto old_size = _direct_access_queue->size();
        key old_front = _front;
        key_pair old_front_entry = (*_direct_access_queue)[_front];
        w_assert1(_back != _front);
        w_assert1(_back != _invalid_key);
        
        _front = old_front_entry._next;
        (*_direct_access_queue)[old_front_entry._next]._previous = _invalid_key;
        _direct_access_queue->erase(old_front);
        w_assert1(_direct_access_queue->size() == old_size - 1);
    }
    return true;
}

template<class key>
bool hashtable_queue<key>::remove(key k) {
    if (!this->contains(k)) {
        return false;
    } else {
        auto old_size = _direct_access_queue->size();
        key_pair old_key = (*_direct_access_queue)[k];
        if (old_key._next != _invalid_key) {
            (*_direct_access_queue)[old_key._next]._previous = old_key._previous;
        } else {
            _back = old_key._previous;
        }
        if (old_key._previous != _invalid_key) {
            (*_direct_access_queue)[old_key._previous]._next = old_key._next;
        } else {
            _front = old_key._next;
        }
        _direct_access_queue->erase(k);
        w_assert1(_direct_access_queue->size() == old_size - 1);
    }
    return true;
}

template<class key>
u_int32_t hashtable_queue<key>::length() {
    return _direct_access_queue->size();
}

template<class key, class value>
multi_clock<key, value>::multi_clock(key clocksize, clk_idx clocknumber, key invalid_index) {
    _clocksize = clocksize;
    _values = new value[_clocksize]();
    _clocks = new index_pair[_clocksize]();
    _invalid_index = invalid_index;
    
    _clocknumber = clocknumber;
    _hands = new key[_clocknumber]();
    _sizes = new key[_clocknumber]();
    for (int i = 0; i <= _clocknumber - 1; i++) {
        _hands[i] = _invalid_index;
    }
    _invalid_clock_index = _clocknumber;
    _clock_membership = new clk_idx[_clocksize]();
    for (int i = 0; i <= _clocksize - 1; i++) {
        _clock_membership[i] = _invalid_clock_index;
    }
}

template<class key, class value>
multi_clock<key, value>::~multi_clock() {
    _clocksize = 0;
    delete[](_values);
    delete[](_clocks);
    delete[](_clock_membership);
    
    _clocknumber = 0;
    delete[](_hands);
    delete[](_sizes);
}

template<class key, class value>
bool multi_clock<key, value>::get_head(const clk_idx clock, value &head_value) {
    if (!empty(clock)) {
        head_value = _values[_hands[clock]];
        w_assert1(_clock_membership[_hands[clock]] == clock);
        return true;
    } else {
        head_value = _invalid_index;
        w_assert1(_hands[clock] == _invalid_index);
        return false;
    }
}

template<class key, class value>
bool multi_clock<key, value>::set_head(const clk_idx clock, const value new_value) {
    if (!empty(clock)) {
        _values[_hands[clock]] = new_value;
        return true;
    } else {
        return false;
    }
}

template<class key, class value>
bool multi_clock<key, value>::get_head_index(const clk_idx clock, key &head_index) {
    if (!empty(clock)) {
        head_index = _hands[clock];
        w_assert1(_clock_membership[_hands[clock]] == clock);
        return true;
    } else {
        head_index = _invalid_index;
        w_assert1(head_index == _invalid_index);
        return false;
    }
}

template<class key, class value>
bool multi_clock<key, value>::move_head(const clk_idx clock) {
    if (!empty(clock)) {
        _hands[clock] = _clocks[_hands[clock]]._after;
        w_assert1(_clock_membership[_hands[clock]] == clock);
        return true;
    } else {
        return false;
    }
}

template<class key, class value>
bool multi_clock<key, value>::add_tail(const clk_idx clock, const key index) {
    if (valid_index(index) && !contained_index(index)
     && valid_clock_index(clock)) {
        if (empty(clock)) {
            _hands[clock] = index;
            _clocks[index]._before = index;
            _clocks[index]._after = index;
        } else {
            _clocks[index]._before = _clocks[_hands[clock]]._before;
            _clocks[index]._after = _hands[clock];
            _clocks[_clocks[_hands[clock]]._before]._after = index;
            _clocks[_hands[clock]]._before = index;
        }
        _sizes[clock]++;
        _clock_membership[index] = clock;
        return true;
    } else {
        return false;
    }
}

template<class key, class value>
bool multi_clock<key, value>::add_after(const key inside, const key new_entry) {
    if (valid_index(new_entry) && !contained_index(new_entry)
     && contained_index(inside)) {
        w_assert1(_sizes[_clock_membership[inside]] >= 1);
        _clocks[new_entry]._after = _clocks[inside]._after;
        _clocks[new_entry]._before = inside;
        _clocks[inside]._after = new_entry;
        _clock_membership[new_entry] = _clock_membership[inside];
        _sizes[_clock_membership[inside]]++;
        return true;
    } else {
        return false;
    }
}

template<class key, class value>
bool multi_clock<key, value>::add_before(const key inside, const key new_entry) {
    if (valid_index(new_entry) && !contained_index(new_entry)
     && contained_index(inside)) {
        w_assert1(_sizes[_clock_membership[inside]] >= 1);
        _clocks[new_entry]._before = _clocks[inside]._before;
        _clocks[new_entry]._after = inside;
        _clocks[inside]._before = new_entry;
        _clock_membership[new_entry] = _clock_membership[inside];
        _sizes[_clock_membership[inside]]++;
        return true;
    } else {
        return false;
    }
}

template<class key, class value>
bool multi_clock<key, value>::remove_head(const clk_idx clock, key &removed_index) {
    if (!empty(clock)) {
        removed_index = _hands[clock];
        w_assert0(remove(removed_index));
        return true;
    } else {
        removed_index = _invalid_index;
        w_assert1(_hands[clock] == _invalid_index);
        return false;
    }
}

template<class key, class value>
bool multi_clock<key, value>::remove(key &index) {
    if (contained_index(index)) {
        clk_idx clock = _clock_membership[index];
        if (_sizes[clock] == 1) {
            w_assert1(_hands[clock] >= 0 && _hands[clock] <= _clocksize - 1 && _hands[clock] != _invalid_index);
            w_assert1(_clocks[_hands[clock]]._before == _hands[clock]);
            w_assert1(_clocks[_hands[clock]]._after == _hands[clock]);
            
            _clocks[index]._before = _invalid_index;
            _clocks[index]._after = _invalid_index;
            _hands[clock] = _invalid_index;
            _clock_membership[index] = _invalid_clock_index;
            _sizes[clock]--;
            return true;
        } else {
            _clocks[_clocks[index]._before]._after = _clocks[index]._after;
            _clocks[_clocks[index]._after]._before = _clocks[index]._before;
            _hands[clock] = _clocks[index]._after;
            _clocks[index]._before = _invalid_index;
            _clocks[index]._after = _invalid_index;
            _clock_membership[index] = _invalid_clock_index;
            _sizes[clock]--;
            
            w_assert1(_hands[clock] != _invalid_index);
            return true;
        }
    } else {
        return false;
    }
}

template<class key, class value>
bool multi_clock<key, value>::switch_head_to_tail(const clk_idx source, const clk_idx destination,
                                                  key &moved_index) {
    moved_index = _invalid_index;
    if (!empty(source) && valid_clock_index(destination)) {
        w_assert0(remove_head(source, moved_index));
        w_assert1(moved_index != _invalid_index);
        w_assert0(add_tail(destination, moved_index));
        
        return true;
    } else {
        return false;
    }
}
