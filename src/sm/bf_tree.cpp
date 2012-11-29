#include "w_defines.h"

#include "bf_hashtable.h"
#include "bf_tree_cb.h"
#include "bf_tree_vol.h"
#include "bf_tree.h"

#include "smthread.h"
#include "vid_t.h"
#include "page_s.h"
#include <string.h>
#include "w_findprime.h"

#include "atomic_templates.h"


// tiny macro to help swizzled-LRU and freelist access
#define FREELIST_HEAD _freelist[0]
#define SWIZZLED_LRU_HEAD _swizzled_lru[0]
#define SWIZZLED_LRU_TAIL _swizzled_lru[1]
#define SWIZZLED_LRU_PREV(x) _swizzled_lru[x * 2]
#define SWIZZLED_LRU_NEXT(x) _swizzled_lru[x * 2 + 1]

///////////////////////////////////   Initialization and Release BEGIN ///////////////////////////////////  
bf_tree_m::bf_tree_m() {
    ::memset (this, 0, sizeof(bf_tree_m));
}

bf_tree_m::~bf_tree_m() {
    // release() must have been called before destruction
    w_assert0(_control_blocks == NULL);
    w_assert0(_buffer == NULL);
    w_assert0(_swizzled_lru == NULL);
    w_assert0(_freelist == NULL);
    w_assert0(_hashtable == NULL);
}

w_rc_t bf_tree_m::init (uint32_t block_cnt) {
    w_assert0 (_block_cnt == 0); // otherwise it's double init
    _block_cnt = block_cnt;
    _control_blocks = reinterpret_cast<bf_tree_cb_t*>(new char[sizeof(bf_tree_cb_t) * block_cnt]);
    ::memset (_control_blocks, 0, sizeof(bf_tree_cb_t) * block_cnt);

    char *buf;
    W_DO(smthread_t::set_bufsize(SM_PAGESIZE * block_cnt, buf));
    _buffer = reinterpret_cast<page_s*>(buf);
    
    // the index 0 is never used. to make sure no one can successfully use it,
    // fill the block-0 with garbages
    ::memset (_buffer, 0x27, sizeof(page_s));
    
    // swizzled-LRU is initially empty
    _swizzled_lru = new bf_idx[block_cnt * 2];
    ::memset (_swizzled_lru, 0, sizeof(bf_idx) * block_cnt * 2);

    // initially, all blocks are free
    _freelist = new bf_idx[block_cnt];
    _freelist[0] = 1; // [0] is a special entry. it's the list head
    for (bf_idx i = 1; i < block_cnt - 1; ++i) {
        _freelist[i] = i + 1;
    }
    _freelist[block_cnt - 1] = 0;
    _freelist_len = block_cnt - 1; // -1 because [0] isn't a valid block
    
    //initialize hashtable
    int buckets = w_findprime(1024 + (block_cnt / 4)); // maximum load factor is 25%. this is lower than original shore-mt because we have swizzling
    _hashtable = new bf_hashtable(buckets);

    return RCOK;
}

w_rc_t bf_tree_m::release () {
    if (_control_blocks != NULL) {
        delete[] _control_blocks;
        _control_blocks = NULL;
    }
    if (_buffer != NULL) {
        char *buf = reinterpret_cast<char*>(_buffer);
        W_DO(smthread_t::set_bufsize(0, buf));
        _buffer = NULL;
    }
    if (_swizzled_lru != NULL) {
        delete[] _swizzled_lru;
        _swizzled_lru = NULL;
    }
    if (_freelist != NULL) {
        delete[] _freelist;
        _freelist = NULL;
    }
    if (_hashtable != NULL) {
        delete _hashtable;
        _hashtable = NULL;
    }

    return RCOK;
}

///////////////////////////////////   Initialization and Release END ///////////////////////////////////  

const uint32_t SWIZZLED_LRU_UPDATE_INTERVAL = 1000;

w_rc_t bf_tree_m::fix(page_s* parent, page_s*& page, const lpid_t& pid, latch_mode_t mode, bool conditional, bool virgin_page) {
    if (parent == NULL) {
        return _fix_root(page, pid, mode, conditional, virgin_page);
    }
    
    // the parent must be latched
    w_assert1(latch_mode(parent) != LATCH_SH || latch_mode(parent) != LATCH_EX);
    shpid_t pointer = pid.page;
    if((pointer & SWIZZLED_PID_BIT) == 0) {
        // non-swizzled page. or even worse it might not exist in bufferpool yet!
        W_DO (_fix_nonswizzled(parent, page, pid, mode, conditional, virgin_page));
    } else {
        w_assert1(!virgin_page); // virgin page can't be swizzled
        // the pointer is swizzled! we can bypass pinning
        bf_idx idx = pointer ^ SWIZZLED_PID_BIT;
        w_assert1 (_is_active_idx(idx));
        bf_tree_cb_t &cb(_control_blocks[idx]);
        w_assert1(cb._pin_cnt > 0);
        W_DO(cb._latch.latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
        ++cb._counter_approximate;
        ++cb._refbit_approximate;
        // also, doesn't have to unpin whether there happens an error or not. easy!
        page = &(_buffer[idx]);
        
        // infrequently update LRU.
        if (cb._counter_approximate % SWIZZLED_LRU_UPDATE_INTERVAL == 0) {
            _update_swizzled_lru(idx);
        }
    }
    return RCOK;
}

w_rc_t bf_tree_m::_fix_nonswizzled(page_s* parent, page_s*& page, const lpid_t& pid, latch_mode_t mode, bool conditional, bool virgin_page) {
    w_assert1(parent);
    w_assert1((pid.page & SWIZZLED_PID_BIT) == 0);
    // unlike swizzled case, this is complex and inefficient.
    // we need to carefully follow the protocol to make it safe.

    // note that the hashtable is separated from this bufferpool.
    // we need to make sure the returned block is still there, and retry otherwise.
    while (true) {
        bf_idx idx = _hashtable->lookup(pid);
        if (idx == 0) {
            //TODO
        } else {
            // unlike swizzled case, we have to atomically pin it while verifying it's still there.
            bf_tree_cb_t &cb(_control_blocks[idx]);
            //TODO
        }
    }
}

w_rc_t bf_tree_m::_fix_root(page_s*& page, const lpid_t& pid, latch_mode_t mode, bool conditional, bool virgin_page) {
    w_assert1(pid.vol() != vid_t::null);
    w_assert1(pid.vol().vol < MAX_VOL_COUNT);
    w_assert1(pid.store() != 0);
    w_assert1((pid.page & SWIZZLED_PID_BIT) == 0); // we never rewrite root page's ID
    bf_tree_vol_t *vol = _volumes[pid.vol().vol];
    w_assert1(vol);
    
    bf_idx idx;
    if (!virgin_page) {
        // root page is always kept in the volume descriptor
        idx = vol->_root_pages[pid.store()];
        w_assert1 (_is_active_idx(idx));
        w_assert1(_control_blocks[idx]._pid_vol == pid.vol().vol);
        w_assert1(_buffer[idx].pid.store() == pid.store());
        w_assert1(_control_blocks[idx]._pid_shpid == pid.page);
    } else {
        // this page will be the root page of a new store.
        w_assert1(vol->_root_pages[pid.store()] == 0);
        W_DO(_grab_free_block(idx));
        w_assert1 (idx > 0 && idx < _block_cnt);
        vol->_root_pages[pid.store()] = idx;
        _control_blocks[idx].clear();
        _control_blocks[idx]._used = true;
        _control_blocks[idx]._pid_vol = pid.vol().vol;
        _control_blocks[idx]._pid_shpid = pid.page;
        _control_blocks[idx]._pin_cnt = 1; // root page's pin count is always 1
    }
    // root page is always swizzled. thus we don't need to increase pin. just take latch.
    W_DO(_control_blocks[idx]._latch.latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
    // also, doesn't have to unpin whether there happens an error or not. easy!
    page = &(_buffer[idx]);
    return RCOK;
}

void bf_tree_m::unfix(const page_s* p, bool dirty) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb(_control_blocks[idx]);
    w_assert1(cb._latch.held_by_me()); 
    if (dirty) {
        cb._dirty = true;
    }
    cb._latch.latch_release();
}

latch_mode_t bf_tree_m::latch_mode(const page_s* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return _control_blocks[idx]._latch.mode();
}

void bf_tree_m::downgrade_latch(const page_s* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb(_control_blocks[idx]);
    w_assert1(cb._latch.held_by_me()); 
    cb._latch.downgrade();
}

bool bf_tree_m::upgrade_latch_conditional(const page_s* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb(_control_blocks[idx]);
    w_assert1(cb._latch.held_by_me()); 
    if (cb._latch.mode() == LATCH_EX) {
        return true;
    }
    bool would_block = false;
    cb._latch.upgrade_if_not_block(would_block);
    if (!would_block) {
        w_assert1 (cb._latch.mode() == LATCH_EX);
        return true;
    } else {
        return false;
    }
}

///////////////////////////////////   LRU/Freelist BEGIN ///////////////////////////////////  

void bf_tree_m::_add_to_swizzled_lru(bf_idx idx) {
    w_assert1 (_is_active_idx(idx));
    w_assert1 (_control_blocks[idx]._parent != 0);
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
    w_assert1 (_is_active_idx(idx));
    w_assert1 (_control_blocks[idx]._parent != 0);

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
    w_assert1 (_is_active_idx(idx));
    w_assert1 (_control_blocks[idx]._parent != 0);

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

w_rc_t bf_tree_m::_grab_free_block(bf_idx& ret) {
    while (true) {
        {
            CRITICAL_SECTION(cs, &_freelist_lock);
            if (_freelist_len > 0) {
                bf_idx idx = FREELIST_HEAD;
                w_assert1 (idx > 0 && idx < _block_cnt);
                w_assert1 (!_control_blocks[idx]._used);
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
        // TODO implement eviction
    }
    return RCOK;
}

void bf_tree_m::_add_free_block(bf_idx idx) {
    CRITICAL_SECTION(cs, &_freelist_lock);
    ++_freelist_len;
    _freelist[idx] = FREELIST_HEAD;
    FREELIST_HEAD = idx;
}


///////////////////////////////////   LRU/Freelist END ///////////////////////////////////  

bool bf_tree_m::_is_active_idx (bf_idx idx) {
    if (idx <= 0 || idx > _block_cnt) {
        return false;
    }
    return _control_blocks[idx]._used;
}

bool bf_tree_m::_increment_pin_cnt_no_assumption(bf_idx idx) {
    w_assert1(idx > 0 && idx < _block_cnt);
    bf_tree_cb_t &cb(_control_blocks[idx]);
    int32_t cur = cb._pin_cnt;
    while (true) {
        w_assert1(cur >= -1);
        if (cur == -1) {
            break; // being evicted! fail
        }
    
	if(lintel::unsafe::atomic_compare_exchange_strong(const_cast<int32_t*>(&cb._pin_cnt), &cur, cur + 1)) {
	  return true;
	}    
    }
    return false;
}

void bf_tree_m::_decrement_pin_cnt_assume_positive(bf_idx idx) {
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb(_control_blocks[idx]);
    w_assert1 (cb._pin_cnt >= 1);
    lintel::unsafe::atomic_fetch_sub(const_cast<int32_t*>(&cb._pin_cnt), 1);
}

///////////////////////////////////   WRITE-ORDER-DEPENDENCY BEGIN ///////////////////////////////////  
bool bf_tree_m::register_write_order_dependency(const page_s* page, const page_s* dependency) {
    w_assert1(page);
    w_assert1(dependency);
    w_assert1(page->pid != dependency->pid);

    uint32_t idx = page - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb(_control_blocks[idx]);
    w_assert1(cb._latch.held_by_me()); 

    uint32_t dependency_idx = dependency - _buffer;
    w_assert1 (_is_active_idx(dependency_idx));
    bf_tree_cb_t &dependency_cb(_control_blocks[dependency_idx]);
    w_assert1(dependency_cb._latch.held_by_me()); 

    // each page can have only one out-going dependency
    if (cb._dependency_idx != 0) {
        w_assert1 (cb._dependency_shpid != 0); // the OLD dependency pid
        if (cb._dependency_idx == dependency_idx) {
            // okay, it points to the same block

            if (cb._dependency_shpid == dependency_cb._pid_shpid) {
                // fine. it's just update of minimal lsn with max of the two.
                cb._dependency_lsn = dependency_cb._rec_lsn > cb._dependency_lsn ? dependency_cb._rec_lsn : cb._dependency_lsn;
                return true;
            } else {
                // this means now the old dependency is already evicted. so, we can forget about it.
                cb._dependency_idx = 0;
                cb._dependency_shpid = 0;
                cb._dependency_lsn = 0;
            }
        } else {
            // this means we might be requesting more than one dependency...
            // let's check the old dependency is still active
            if  (_check_dependency_still_active(cb)) {
                // the old dependency is still active. we can't make another dependency
                return false;
            }
        }
    }

    // this is the first dependency 
    w_assert1(cb._dependency_idx == 0);
    w_assert1(cb._dependency_shpid == 0);
    w_assert1(cb._dependency_lsn == 0);
    
    // check a cycle of dependency
    if (dependency_cb._dependency_idx != 0) {
        if (_check_dependency_cycle (idx, dependency_idx)) {
            return false;
        }
    }

    //okay, let's register the dependency
    cb._dependency_idx = dependency_idx;
    cb._dependency_shpid = dependency_cb._pid_shpid;
    cb._dependency_lsn = dependency_cb._rec_lsn;
    return true;
}

bool bf_tree_m::_check_dependency_cycle(bf_idx source, bf_idx start_idx) {
    w_assert1(source != start_idx);
    bf_idx dependency_idx = start_idx;
    bool dependency_needs_unpin = false;
    bool found_cycle = false;
    while (true) {
        if (dependency_idx == source) {
            found_cycle = true;
            break;
        }
        bf_tree_cb_t &dependency_cb(_control_blocks[dependency_idx]);
        w_assert1(dependency_cb._pin_cnt >= 1); // it must be already pinned
        bf_idx next_dependency_idx = dependency_cb._dependency_idx;
        if (next_dependency_idx == 0) {
            break;
        }
        bool increased = _increment_pin_cnt_no_assumption (next_dependency_idx);
        if (!increased) {
            // it's already evicted or being evicted. we can ignore it.
            break; // we can stop here.
        } else {
            // move on to next
            bf_tree_cb_t &next_dependency_cb(_control_blocks[next_dependency_idx]);
            bool still_active = _compare_dependency_lsn(dependency_cb, next_dependency_cb);
            // okay, we no longer need the previous. unpin the previous one.
            if (dependency_needs_unpin) {
                _decrement_pin_cnt_assume_positive(dependency_idx);
            }
            dependency_idx = next_dependency_idx;
            dependency_needs_unpin = true;
            if (!still_active) {
                break;
            }
        }
    }
    if (dependency_needs_unpin) {
        _decrement_pin_cnt_assume_positive(dependency_idx);
    }
    return found_cycle;
}

bool bf_tree_m::_compare_dependency_lsn(const bf_tree_cb_t& cb, const bf_tree_cb_t &dependency_cb) const {
    w_assert1(cb._pin_cnt >= 1);
    w_assert1(cb._dependency_idx != 0);
    w_assert1(cb._dependency_shpid != 0);
    w_assert1(dependency_cb._pin_cnt >= 1);
    return dependency_cb._used && dependency_cb._dirty // it's still dirty
        && dependency_cb._pid_vol == cb._pid_vol // it's still the vol and 
        && dependency_cb._pid_shpid == cb._dependency_shpid // page it was referring..
        && dependency_cb._rec_lsn <= cb._dependency_lsn; // and not flushed after the registration
}
bool bf_tree_m::_check_dependency_still_active(bf_tree_cb_t& cb) {
    w_assert1(cb._pin_cnt >= 1);
    bf_idx next_idx = cb._dependency_idx;
    if (next_idx == 0) {
        return false;
    }

    w_assert1(cb._dependency_shpid != 0);

    bool still_active;
    {
        bool increased = _increment_pin_cnt_no_assumption (next_idx);
        if (!increased) {
            // it's already evicted or being evicted. we can ignore it.
            still_active = false;
        } else {
            still_active = _compare_dependency_lsn(cb, _control_blocks[next_idx]);
            _decrement_pin_cnt_assume_positive(next_idx);
        }
    }
    
    if (!still_active) {
        // reset the values to help future inquiry
        cb._dependency_idx = 0;
        cb._dependency_shpid = 0;
        cb._dependency_lsn = 0;
    }
    return still_active;
}
///////////////////////////////////   WRITE-ORDER-DEPENDENCY END ///////////////////////////////////  
