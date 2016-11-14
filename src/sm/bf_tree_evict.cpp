#include "bf_tree_cb.h"
#include "bf_tree.h"
#include "btree_page_h.h"
#include "log_core.h" // debug: for printing log tail

#include "bf_hashtable.cpp"

w_rc_t bf_tree_m::_grab_free_block(bf_idx& ret, bool evict)
{
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
                DBG5(<< "Grabbing idx " << idx);
                w_assert1(_is_valid_idx(idx));
                w_assert1 (!get_cb(idx)._used);
                ret = idx;

                --_freelist_len;
                if (_freelist_len == 0) {
                    FREELIST_HEAD = 0;
                } else {
                    FREELIST_HEAD = _freelist[idx];
                    w_assert1 (FREELIST_HEAD > 0 && FREELIST_HEAD < _block_cnt);
                }
                DBG5(<< "New head " << FREELIST_HEAD);
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

w_rc_t bf_tree_m::_get_replacement_block()
{
    // Evictioner should be responsible for waking up cleaner.
    while(_freelist_len == 0)
    {
        get_evictioner()->wakeup();
        get_evictioner()->wait_for_notify();
    }
    return RCOK;
}


void bf_tree_m::_add_free_block(bf_idx idx)
{
    CRITICAL_SECTION(cs, &_freelist_lock);
    // CS TODO: Eviction is apparently broken, since I'm seeing the same
    // frame being freed twice by two different threads.
    w_assert1(idx != FREELIST_HEAD);
    w_assert1(!get_cb(idx)._used);
    ++_freelist_len;
    _freelist[idx] = FREELIST_HEAD;
    FREELIST_HEAD = idx;
}