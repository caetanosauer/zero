/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define BTREE_C

#include "sm_int_2.h"

#include "vec_t.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "sm_du_stats.h"
#include "crash.h"
#include "w_key.h"
#include <string>
#include <algorithm>


btrec_t& 
btrec_t::set(const btree_page_h& page, slotid_t slot) {
    FUNC(btrec_t::set);
    w_assert3(slot >= 0 && slot < page.nrecs());
    // Invalidate old _elem.
    _elem.reset();
    
    if (page.is_leaf())  {
        page.rec_leaf(slot, _key, _elem, _ghost_record);
    } else {
        page.rec_node(slot, _key, _child);
        _ghost_record = false;
        // this might not be needed, but let's also add the _child value
        // to _elem.
        _elem.put (&_child, sizeof(_child));
    }

    return *this;
}


rc_t btree_page_h::init_fix_steal(btree_page_h*     parent,
                                  const lpid_t&     pid,
                                  shpid_t           root, 
                                  int               l,
                                  shpid_t           pid0,
                                  shpid_t           foster,
                                  const w_keystr_t& fence_low,
                                  const w_keystr_t& fence_high,
                                  const w_keystr_t& chain_fence_high,
                                  btree_page_h*     steal_src,
                                  int               steal_from,
                                  int               steal_to,
                                  bool              log_it) {
    FUNC(btree_page_h::init_fix_steal);
    INC_TSTAT(btree_p_fix_cnt);
    if (parent == NULL) {
        W_DO(fix_virgin_root(pid.vol().vol, pid.store(), pid.page));
    } else {
        W_DO(fix_nonroot(*parent, parent->vol(), pid.page, LATCH_EX, false, true));
    }
    W_DO(format_steal(pid, root, l, pid0, foster, fence_low, fence_high, chain_fence_high, log_it, steal_src, steal_from, steal_to));
    return RCOK;
}

rc_t btree_page_h::format_steal(const lpid_t&     pid,
                                shpid_t           root, 
                                int               l,
                                shpid_t           pid0,
                                shpid_t           foster,
                                const w_keystr_t& fence_low,
                                const w_keystr_t& fence_high,
                                const w_keystr_t& chain_fence_high,
                                bool              log_it,
                                btree_page_h*     steal_src1,
                                int               steal_from1,
                                int               steal_to1,
                                btree_page_h*     steal_src2,
                                int               steal_from2,
                                int               steal_to2,
                                bool              steal_src2_pid0) {
    w_assert1 (l == 1 || pid0 != 0); // all interemediate node should have pid0 at least initially

    //first, nuke the page
    lpid_t pid_copy = pid; // take a copy first, because pid might point to a part of this page itself!
#ifdef ZERO_INIT
    /* NB -- NOTE -------- NOTE BENE
    *  Note this is not exactly zero-init, but it doesn't matter
    * WHAT we use to init each byte for the purpose of purify or valgrind
    */
    // because we do this, note that we shouldn't receive any arguments
    // as reference or pointer. It might be also nuked!
    memset(page(), '\017', sizeof(generic_page)); // trash the whole page
#endif //ZERO_INIT
    page()->lsn          = lsn_t(0, 1);
    page()->pid          = pid_copy;
    page()->tag          = t_btree_p;
    page()->page_flags   = 0;
    page()->init_items();

    page()->btree_consecutive_skewed_insertions = 0;

    page()->btree_root                    = root;
    page()->btree_pid0                    = pid0;
    page()->btree_level                   = l;
    page()->btree_foster                  = foster;
    page()->btree_fence_low_length        = (int16_t) fence_low.get_length_as_keystr();
    page()->btree_fence_high_length       = (int16_t) fence_high.get_length_as_keystr();
    page()->btree_chain_fence_high_length = (int16_t) chain_fence_high.get_length_as_keystr();

    // set fence keys in first slot
    cvec_t fences;
    size_t prefix_len = _pack_fence_rec(fences, fence_low, fence_high, chain_fence_high, -1);
    w_assert1(prefix_len <= fence_low.get_length_as_keystr() && prefix_len <= fence_high.get_length_as_keystr());
    w_assert1(prefix_len <= (1<<15));
    page()->btree_prefix_length = (int16_t) prefix_len;

    // fence-key record doesn't need poormkey; set to 0:
    if (!page()->insert_item(nitems(), false, 0, 0, fences)) {
        assert(false);
    }

    // steal records from old page
    if (steal_src1) {
        _steal_records (steal_src1, steal_from1, steal_to1);
    }
    if (steal_src2_pid0) {
        w_assert1(steal_src2);
        w_assert1(is_node());
        w_assert1(steal_src2->pid0() != pid0);
        w_assert1(steal_src2->pid0() != 0);
        // before stealing regular records from src2, steal it's pid0
        cvec_t v;
        key_length_t key_len = (key_length_t) steal_src2->get_fence_low_length();
        v.put(&key_len, sizeof(key_len));
        v.put(steal_src2->get_fence_low_key() + prefix_len, steal_src2->get_fence_low_length() - prefix_len);
        poor_man_key poormkey = extract_poor_man_key (steal_src2->get_fence_low_key(), steal_src2->get_fence_low_length(), prefix_len);
        shpid_t      stolen_pid0 = steal_src2->pid0();
        if (!page()->insert_item(nitems(), false, poormkey, stolen_pid0, v)) {
            assert(false);
        }
    }
    if (steal_src2) {
        _steal_records (steal_src2, steal_from2, steal_to2);
    }

    // log as one record
    if (log_it) {
        W_DO(log_page_img_format(*this));
        w_assert1(lsn().valid());
    }
    
    return RCOK;
}

void btree_page_h::_steal_records(btree_page_h* steal_src,
                                  int           steal_from,
                                  int           steal_to) {
    w_assert2(steal_src);
    w_assert2(steal_from <= steal_to);
    w_assert2(steal_from >= 0);
    w_assert2(steal_to <= steal_src->nrecs());

    key_length_t src_prefix_length = steal_src->get_prefix_length();
    key_length_t new_prefix_length = get_prefix_length();
    for (int i = steal_from; i < steal_to; ++i) {
        cvec_t v;
        cvec_t new_trunc_key;
        shpid_t child;

        cvec_t key, dummy;
        key.put(steal_src->get_prefix_key(), steal_src->get_prefix_length());
        if (is_leaf()) {
            int   key_length;
            char* trunc_key_data;
            int   data_length;
            char* data;
            steal_src->_get_leaf_fields(i, key_length, trunc_key_data, data_length, data);
            int trunc_key_length = key_length - src_prefix_length;
            key.put(trunc_key_data, trunc_key_length);

            key.split(new_prefix_length, dummy, new_trunc_key);
            key_length_t klen = key_length;
            v.put(&klen, sizeof(klen));
            v.put(new_trunc_key);
            v.put(data, data_length);
            child = 0;
        } else {
            int   trunc_key_length;
            char* trunc_key_data;
            steal_src->_get_node_key_fields(i, trunc_key_length, trunc_key_data);
            key.put(trunc_key_data, trunc_key_length);

            key.split(new_prefix_length, dummy, new_trunc_key);
            v.put(new_trunc_key);
            child = steal_src->page()->item_child(i+1);
        }

        if (!page()->insert_item(nitems(), steal_src->is_ghost(i), 
                                 extract_poor_man_key(new_trunc_key), 
                                 child, v)) {
            assert(false);
        }

        w_assert3(is_consistent());
        w_assert5(_is_consistent_keyorder());
    }
}
rc_t btree_page_h::norecord_split (shpid_t foster,
                                   const w_keystr_t& fence_high, const w_keystr_t& chain_fence_high,
                                   bool log_it) {
    w_assert1(compare_with_fence_low(fence_high) > 0);
    w_assert1(compare_with_fence_low(chain_fence_high) > 0);
    if (log_it) {
        W_DO(log_btree_foster_norecord_split (*this, foster, fence_high, chain_fence_high));
    }

    w_keystr_t fence_low;
    copy_fence_low_key(fence_low);
    key_length_t new_prefix_len = fence_low.common_leading_bytes(fence_high);

    if (new_prefix_len > get_prefix_length() + 3) { // this +3 is arbitrary
        // then, let's defrag this page to compress keys
        generic_page scratch;
        ::memcpy (&scratch, _pp, sizeof(scratch));
        btree_page_h scratch_p (&scratch);
        W_DO(format_steal(scratch_p.pid(), scratch_p.btree_root(), scratch_p.level(), scratch_p.pid0(),
                          foster,
                          fence_low, fence_high, chain_fence_high,
                          false, // don't log it
                          &scratch_p, 0, scratch_p.nrecs()
        ));
        set_lsns(scratch.lsn); // format_steal() also clears lsn, so recover it from the copied page
    } else {
        // otherwise, just sets the fence keys and headers
        //sets new fence
        rc_t rc = replace_fence_rec_nolog(fence_low, fence_high, chain_fence_high, new_prefix_len);
        w_assert1(rc.err_num() != smlevel_0::eRECWONTFIT);// then why it passed check_chance_for_norecord_split()?
        w_assert1(!rc.is_error());

        //updates headers
        page()->btree_foster                        = foster;
        page()->btree_fence_high_length             = (int16_t) fence_high.get_length_as_keystr();
        page()->btree_chain_fence_high_length       = (int16_t) chain_fence_high.get_length_as_keystr();
        page()->btree_consecutive_skewed_insertions = 0; // reset this value too.
    }
    return RCOK;
}

rc_t btree_page_h::clear_foster() {
    // note that we don't have to change the chain-high fence key.
    // we just leave it there, and update the length only.
    // chain-high-fence key is placed after low/high, so it doesn't matter.
    W_DO(log_btree_header(*this, pid0(), level(), 0, // foster=0
                          0 // chain-high fence key is disabled
             )); // log first
    page()->btree_foster = 0;
    page()->btree_chain_fence_high_length = 0;
    return RCOK;
}

void
btree_page_h::search(const w_keystr_t& key,
                     bool&             found_key, 
                     slotid_t&         ret_slot    // origin 0 for first record
    ) const
{
    if (is_leaf()) {
        search_leaf(key, found_key, ret_slot);
    } else {
        found_key = false; // no meaning on exact match.
        search_node(key, ret_slot);
    }
}

// simple sequential search with poorman's key. very simple.
// #define POORMKEY_SEQ_SEARCH
#ifdef POORMKEY_SEQ_SEARCH
void btree_page_h::search_leaf(const char *key_raw, size_t key_raw_len,
                               bool& found_key, slotid_t& ret_slot) const {
    w_assert3(is_leaf());
    w_assert1((uint) get_prefix_length() <= key_raw_len);
    w_assert1(::memcmp (key_raw, get_prefix_key(), get_prefix_length()) == 0);

    const char * key_noprefix        = key_raw + get_prefix_length();
    int          key_len             = key_raw_len - get_prefix_length();
    poor_man_key poormkey            = extract_poor_man_key(key_noprefix, key_len);
    const void * key_noprefix_remain = key_noprefix + sizeof(poor_man_key);
    int          key_len_remain      = key_len - sizeof(poor_man_key);
    found_key = false;
    
    for (int slot= 0; slot < nrecs(); slot++) {
        poor_man_key cur_poormkey = page()->item_data16(slot+1);
        if (cur_poormkey < poormkey) {
            w_assert1(_compare_leaf_key_noprefix(slot, key_noprefix, key_len) < 0);
            continue;
        }
        if (cur_poormkey > poormkey) {
            ret_slot = slot;
            w_assert1(slot == 0 || _compare_leaf_key_noprefix(slot - 1, key_noprefix, key_len) < 0);
            w_assert1(_compare_leaf_key_noprefix(slot, key_noprefix, key_len) > 0);
            return;
        }
        int d = _compare_leaf_key_noprefix_remain(slot, key_noprefix_remain, key_len_remain);
        if (d == 0) {
            found_key = true;
            ret_slot = slot;
            w_assert1(_compare_leaf_key_noprefix(slot, key_noprefix, key_len) == 0);
            return;
        } else if (d > 0) {
            ret_slot = slot;
            w_assert1(slot == 0 || _compare_leaf_key_noprefix(slot - 1, key_noprefix, key_len) < 0);
            w_assert1(_compare_leaf_key_noprefix(slot, key_noprefix, key_len) > 0);
            return;
        }
        w_assert1(_compare_leaf_key_noprefix(slot, key_noprefix, key_len) < 0);
    }
    ret_slot = nrecs();
}

void btree_page_h::search_node(const w_keystr_t& key,
                               slotid_t&         ret_slot) const {
    w_assert3(!is_leaf());

    const char *key_raw = (const char *) key.buffer_as_keystr();
    w_assert1((uint) get_prefix_length() <= key.get_length_as_keystr());
    w_assert1(::memcmp (key_raw, get_prefix_key(), get_prefix_length()) == 0);

    const char*  key_noprefix        = key_raw + get_prefix_length();
    int          key_len             = key.get_length_as_keystr() - get_prefix_length();
    poor_man_key poormkey            = extract_poor_man_key(key_noprefix, key_len);
    const void*  key_noprefix_remain = key_noprefix + sizeof(poor_man_key);
    int          key_len_remain      = key_len - sizeof(poor_man_key);

    w_assert1 (pid0() != 0);

    for (int slot= 0; slot < nrecs(); slot++) {
        poor_man_key cur_poormkey = page()->item_data16(slot+1);
        if (cur_poormkey < poormkey) {
            w_assert1(_compare_node_key_noprefix(slot, key_noprefix, key_len) < 0);
            continue;
        }
        if (cur_poormkey > poormkey) {
            ret_slot = slot - 1;
            w_assert1(slot == 0 || _compare_node_key_noprefix(slot - 1, key_noprefix, key_len) < 0);
            w_assert1(_compare_node_key_noprefix(slot, key_noprefix, key_len) > 0);
            return;
        }
        int d = _compare_node_key_noprefix_remain(slot, key_noprefix_remain, key_len_remain);
        if (d == 0) {
            ret_slot = slot;
            w_assert1(_compare_node_key_noprefix(slot, key_noprefix, key_len) == 0);
            return;
        } else if (d > 0) {
            ret_slot = slot - 1;
            w_assert1(slot == 0 || _compare_node_key_noprefix(slot - 1, key_noprefix, key_len) < 0);
            w_assert1(_compare_node_key_noprefix(slot, key_noprefix, key_len) > 0);
            return;
        }
        w_assert1(_compare_node_key_noprefix(slot, key_noprefix, key_len) < 0);
    }
    ret_slot = nrecs() - 1;
}

#else // POORMKEY_SEQ_SEARCH

void btree_page_h::search_leaf(const char *key_raw, size_t key_raw_len,
                               bool& found_key, slotid_t& ret_slot) const {
    w_assert3(is_leaf());
    FUNC(btree_page_h::_search_leaf);
    
    w_assert1((uint) get_prefix_length() <= key_raw_len);
    w_assert1(::memcmp (key_raw, get_prefix_key(), get_prefix_length()) == 0);
    const void *key_noprefix = key_raw + get_prefix_length();
    size_t key_len = key_raw_len - get_prefix_length();
    
    found_key = false;

    poor_man_key poormkey = extract_poor_man_key(key_noprefix, key_len);

    // check the last record to speed-up sorted insert
    int last_slot = nrecs() - 1;
    if (last_slot >= 0) {
        poor_man_key last_poormkey = _poor(last_slot);
        if (last_poormkey < poormkey) {
            ret_slot = nrecs();
            w_assert1(_compare_leaf_key_noprefix(last_slot, key_noprefix, key_len) < 0);
            return;
        }
        // can check only "<", not "=", because same poormkey doesn't mean same key
        int d = _compare_leaf_key_noprefix(last_slot, key_noprefix, key_len);
        if (d == 0) {
            // oh, luckily found it.
            found_key = true;
            ret_slot = last_slot;
            return;
        } else if (d < 0) {
            // even larger than the highest key. done!
            ret_slot = nrecs();
            // found_key is false
            return;
        }
    }
    
    // Binary search. TODO later try interpolation search.
    int mi = 0, lo = 0;
    int hi = nrecs() - 2; // -2 because we already checked last record above
    while (lo <= hi)  {
        mi = (lo + hi) >> 1;    // ie (lo + hi) / 2

        poor_man_key cur_poormkey = _poor(mi);
        if (cur_poormkey < poormkey) {
            lo = mi + 1;
            w_assert1(_compare_leaf_key_noprefix(mi, key_noprefix, key_len) < 0);
            continue;
        } else if (cur_poormkey > poormkey) {
            hi = mi - 1;
            w_assert1(_compare_leaf_key_noprefix(mi, key_noprefix, key_len) > 0);
        }
        int d = _compare_leaf_key_noprefix(mi, key_noprefix, key_len);
        // d <  0 if curkey < key; key falls between mi and hi
        // d == 0 if curkey == key; match
        // d >  0 if curkey > key; key falls between lo and mi
        if (d < 0) 
            lo = mi + 1;
        else if (d > 0)
            hi = mi - 1;
        else {
            // exact match! we can exit at this point
            found_key = true;
            ret_slot = mi;
            return;
        }
    }
    ret_slot = (lo > mi) ? lo : mi;
#if W_DEBUG_LEVEL > 2
    w_assert3(ret_slot <= nrecs());
    if(ret_slot == nrecs() ) {
        // this happens only when !found_key
        w_assert3(!found_key);
    }
#endif 
}

void btree_page_h::search_node(const w_keystr_t& key,
                               slotid_t&         ret_slot) const {
    w_assert3(!is_leaf());
    FUNC(btree_page_h::_search_node);

    const char *key_raw = (const char *) key.buffer_as_keystr();
    w_assert1((uint) get_prefix_length() <= key.get_length_as_keystr());
    w_assert1(::memcmp (key_raw, get_prefix_key(), get_prefix_length()) == 0);
    const void *key_noprefix = key_raw + get_prefix_length();
    size_t key_len = key.get_length_as_keystr() - get_prefix_length();

    poor_man_key poormkey = extract_poor_man_key(key_noprefix, key_len);

    w_assert1 (pid0() != 0);
    bool return_pid0 = false;
    if (nrecs() == 0) {
        return_pid0 = true;
    } else  {
        poor_man_key cur_poormkey = _poor(0);
        if (cur_poormkey > poormkey) {
            return_pid0 = true;
            w_assert1(_compare_node_key_noprefix(0, key_noprefix, key_len) > 0);
        } else {
            // separator key is exclusive for left, so it's ">"
            int d = _compare_node_key_noprefix(0, key_noprefix, key_len);
            if (d > 0) {
                return_pid0 = true;
            } else if (d == 0) {
                ret_slot = 0;
                return;
            }
        }
    }

    if (return_pid0) {
        ret_slot = -1;
        return;
    }
    
#if W_DEBUG_LEVEL > 2
    // As we reach here, no search key can end up with pid0
    w_assert3 (nrecs() > 0);
    btrec_t first_tup(*this, 0); 
    // first_tup must be smaller than the search key
    w_assert3 (first_tup.key().compare(key) <= 0);
#endif  // W_DEBUG_LEVEL > 2

    // check the last record to speed-up sorted insert
    int last_slot = nrecs() - 1;
    if (last_slot >= 0) {
        poor_man_key last_poormkey = _poor(last_slot);
        if (last_poormkey < poormkey) { // note that it's "<", not "<=", because same poormkey doesn't mean same key
            ret_slot = last_slot;
            w_assert1(_compare_node_key_noprefix(last_slot, key_noprefix, key_len) < 0);
            return;
        } else {
            int d = _compare_node_key_noprefix(last_slot, key_noprefix, key_len);
            if (d <= 0) {
                ret_slot = last_slot;
                return;
            }
        }
    }
    
    // we are looking for a slot such that
    // key[slot] <= search_key < key[slot + 1]
    // or just key[slot] <= search_key if slot is the last slot

     // Binary search. TODO later try interpolation search.
    // search for biggest key that is smaller than search key
    
    // note: following code is a little bit different from leaf
    int lo = 0; // the biggest slot we already know <key
    int hi = nrecs(); //the smallest slot we already know >=key
    int mi;
    for (; lo < hi - 1; )  {
        mi = (lo + hi) >> 1;    // ie (lo + hi) / 2

        poor_man_key cur_poormkey = _poor(mi);
        if (cur_poormkey < poormkey) {
            lo = mi;
            w_assert1(_compare_node_key_noprefix(mi, key_noprefix, key_len) < 0);
            continue;
        } else if (cur_poormkey > poormkey) {
            hi = mi;
            w_assert1(_compare_node_key_noprefix(mi, key_noprefix, key_len) > 0);
            continue;
        }
        // again, poormkey can't strictly tell < or > if it's equal
        int d = _compare_node_key_noprefix(mi, key_noprefix, key_len);
        if (d < 0) 
            lo = mi;
        else if (d > 0)
            hi = mi;
        else {
            lo = mi; // separator key is lower-inclusive
            hi = mi + 1;
            break;
        }
    }
    w_assert3(lo == hi - 1);
    ret_slot = lo;
    w_assert3(ret_slot < nrecs());

#if W_DEBUG_LEVEL > 2
    size_t curkey_len;
    const char *curkey = _node_key_noprefix(ret_slot, curkey_len);
    int d2 = w_keystr_t::compare_bin_str(curkey, curkey_len, key_noprefix, key_len);
    w_assert2 (d2 <= 0);
    if (ret_slot < nrecs() - 1) {
        curkey = _node_key_noprefix(ret_slot + 1, curkey_len);
        int d3 = w_keystr_t::compare_bin_str(curkey, curkey_len, key_noprefix, key_len);
        w_assert2 (d3 > 0);
    }
#endif //W_DEBUG_LEVEL
}

#endif // POORMKEY_SEQ_SEARCH

void btree_page_h::_update_btree_consecutive_skewed_insertions(slotid_t slot) {
    if (nrecs() == 0) {
        return;
    }
    int16_t val = page()->btree_consecutive_skewed_insertions;
    if (slot == 0) {
        // if left-most insertion, start counting negative value (or decrement further)
        if (val >= 0) {
            val = -1;
        } else {
            --val;
        }
    } else if (slot == nrecs()) {
        // if right-most insertion, start counting positive value (or increment further)
        if (val <= 0) {
            val = 1;
        } else {
            ++val;
        }
    } else {
        val = 0;
    }
    // to prevent overflow
    if (val < -100) val = -100;
    if (val > 100) val = 100;
    page()->btree_consecutive_skewed_insertions = val;
}

rc_t btree_page_h::insert_node(const w_keystr_t &key, slotid_t slot, shpid_t child) {
    FUNC(btree_page_h::insert);
    
    w_assert3(is_node());
    w_assert1(slot >= 0 && slot <= nrecs()); // <= intentional to allow appending
    w_assert3(child);
    w_assert3(is_consistent(true, false));

#if W_DEBUG_LEVEL > 1
    if (slot == 0) {
        w_assert2 (pid0()); // pid0 always exists
        w_assert2 (compare_with_fence_low(key) >= 0);
    } else {
        btrec_t rt (*this, slot - 1); // prev key
        w_assert2 (key.compare(rt.key()) > 0);
    }
    if (slot < nrecs()) {
        btrec_t rt (*this, slot); // (after insert) next key
        w_assert2 (key.compare(rt.key()) < 0);
    }
#endif // W_DEBUG_LEVEL

    // Update btree_consecutive_skewed_insertions.  This is just statistics and not logged.
    _update_btree_consecutive_skewed_insertions (slot);

    // See the record format in btree_page_h class comments.
    vec_t v; // the record data
    key_length_t klen = key.get_length_as_keystr();
    // Prefix of the key is fixed in this page, so we can simply
    // peel off leading bytes from the key+el.
    key_length_t prefix_length = get_prefix_length();  // length of prefix of inserted tuple
    w_assert3(prefix_length <= klen);
    v.put(key, prefix_length, klen - prefix_length);
    poor_man_key poormkey = extract_poor_man_key(key.buffer_as_keystr(), klen, prefix_length);
    // we don't log it. btree_impl::adopt() does the logging
    if (!page()->insert_item(slot+1, false, poormkey, child, v)) {
        // This shouldn't happen; the caller should have checked with check_space_for_insert_for_node():
        return RC(smlevel_0::eRECWONTFIT);
    }

    w_assert3 (is_consistent(true, false));
    w_assert5 (is_consistent(true, true));

    return RCOK;
}

rc_t btree_page_h::replace_fence_rec_nolog(const w_keystr_t& low,
                                           const w_keystr_t& high, 
                                           const w_keystr_t& chain, int new_prefix_len) {
    w_assert1(nitems() > 0);

    cvec_t fences;
#if W_DEBUG_LEVEL > 0
    int prefix_len = 
#endif
        _pack_fence_rec(fences, low, high, chain, new_prefix_len);
    w_assert1(prefix_len == get_prefix_length());

    if (!page()->replace_item_data(0, fences, 0)) {
        return RC(smlevel_0::eRECWONTFIT);
    }

    w_assert1 (page()->item_length(0) == (key_length_t) fences.size());
    w_assert3(page()->_slots_are_consistent());
    return RCOK;
}


rc_t btree_page_h::remove_shift_nolog(slotid_t idx) {
    w_assert1(idx >= 0 && idx < nrecs());

    page()->delete_item(idx + 1);
    return RCOK;
}

bool btree_page_h::_is_enough_spacious_ghost(const w_keystr_t &key, slotid_t slot,
                                             const cvec_t&     el) {
    w_assert2(is_leaf());
    w_assert2(is_ghost(slot));

    size_t needed_data = key.get_length_as_keystr() - get_prefix_length()
        + el.size() + sizeof(key_length_t); // <<<>>>

    return page()->predict_item_space(needed_data) <= page()->item_space(slot+1);
}

rc_t btree_page_h::replace_ghost(const w_keystr_t &key,
                                 const cvec_t &elem) {
    w_assert2( is_fixed());
    w_assert2( is_leaf());

    // log FIRST. note that this might apply the deferred ghost creation too.
    // so, this cannot be done later than any of following
    W_DO (log_btree_insert (*this, key, elem));

    // which slot to replace?
    bool found;
    slotid_t slot;
    search_leaf(key, found, slot);
    w_assert0 (found);
    w_assert1 (is_ghost(slot));
#if W_DEBUG_LEVEL > 2
    btrec_t rec (*this, slot);
    w_assert3 (rec.key().compare(key) == 0);
#endif // W_DEBUG_LEVEL > 2

    int   key_length;
    char* trunc_key_data;
    _get_leaf_key_fields(slot, key_length, trunc_key_data);
    size_t data_offset = sizeof(key_length_t) + key_length - get_prefix_length();  // <<<>>>

    if (!page()->replace_item_data(slot+1, elem, data_offset)) {
        w_assert1(false); // should not happen because ghost should have had enough space
    }

    page()->unset_ghost(slot + 1);
    return RCOK;
}

rc_t btree_page_h::replace_el_nolog(slotid_t slot, const cvec_t &elem) {
    w_assert2( is_fixed());
    w_assert2( is_leaf());
    w_assert1(!is_ghost(slot));
    
    int   key_length;
    char* trunc_key_data;
    _get_leaf_key_fields(slot, key_length, trunc_key_data);
    size_t data_offset = sizeof(key_length_t) + key_length - get_prefix_length();  // <<<>>>

    if (!page()->replace_item_data(slot+1, elem, data_offset)) {
        return RC(smlevel_0::eRECWONTFIT);
    }
    return RCOK;
}

void btree_page_h::overwrite_el_nolog(slotid_t slot, smsize_t offset,
                                      const char *new_el, smsize_t elen) {
    w_assert2( is_fixed());
    w_assert2( is_leaf());
    w_assert1 (!is_ghost(slot));

    int   key_length;
    char* trunc_key_data;
    _get_leaf_key_fields(slot, key_length, trunc_key_data);
    size_t data_offset = sizeof(key_length_t) + key_length - get_prefix_length();  // <<<>>>

    w_assert1(data_offset+offset+elen <= page()->item_length(slot+1));
    ::memcpy(page()->item_data(slot+1)+data_offset+offset, new_el, elen);
}

void btree_page_h::reserve_ghost(const char *key_raw, size_t key_raw_len, size_t element_length) {
    w_assert1 (is_leaf()); // ghost only exists in leaf

    int16_t prefix_len       = get_prefix_length();
    int     trunc_key_length = key_raw_len - prefix_len;
    size_t  data_length      = _predict_leaf_data_length(trunc_key_length, element_length);

    w_assert1(check_space_for_insert_leaf(trunc_key_length, element_length));

    // where to insert?
    slotid_t slot;
    bool found;
    search_leaf(key_raw, key_raw_len, found, slot);
    w_assert1(!found); // this is unexpected!
    if (found) { // but can go on..
        return;
    }

    w_assert1(slot >= 0 && slot <= nrecs());

    // update btree_consecutive_skewed_insertions. this is just statistics and not logged.
    _update_btree_consecutive_skewed_insertions(slot);

#if W_DEBUG_LEVEL>1
    w_keystr_t key;
    key.construct_from_keystr(key_raw, key_raw_len);
    w_keystr_t key2;
    cvec_t el;
    bool ghost;
    w_assert1(compare_with_fence_low(key) >= 0);
    if (slot > 0) {
        // make sure previous record exists (slot=0 is fence)
        // otherwise, compare with previous key
        rec_leaf(slot - 1, key2, el, ghost);
        w_assert1(key.compare(key2) > 0);
    }
    w_assert1(compare_with_fence_high(key) < 0);
    if (slot < nrecs()) {
        // otherwise, compare with previous key
        rec_leaf(slot, key2, el, ghost);
        w_assert1(key.compare(key2) < 0);
    }
#endif // W_DEBUG_LEVEL>1
    
    poor_man_key poormkey = extract_poor_man_key(key_raw, key_raw_len, prefix_len);

    if (!page()->insert_item(slot+1, true, poormkey, 0, data_length)) {
        assert(false);
    }

    // make a dummy record that has the desired length:
    cvec_t dummy;
    key_length_t klen = key_raw_len;
    dummy.put(&klen, sizeof(klen));
    dummy.put(key_raw + prefix_len, trunc_key_length);
    dummy.copy_to(page()->item_data(slot+1));

    w_assert3(_poor(slot) == poormkey);
    w_assert3(page()->item_length(slot+1) == data_length);
}

void btree_page_h::mark_ghost(slotid_t slot) {
    w_assert1(!page()->is_ghost(slot+1));
    page()->set_ghost(slot+1);
    set_dirty();
}

void btree_page_h::unmark_ghost(slotid_t slot) {
    w_assert1(page()->is_ghost(slot+1));
    page()->unset_ghost(slot+1);
    set_dirty();
}


bool btree_page_h::check_space_for_insert_leaf(const w_keystr_t& trunc_key,
                                               const cvec_t&     el) {
    return check_space_for_insert_leaf(trunc_key.get_length_as_keystr(), el.size());
}
bool btree_page_h::check_space_for_insert_leaf(size_t trunc_key_length, size_t element_length) {
    w_assert1 (is_leaf());
    size_t data_length = 1 * sizeof(key_length_t) + trunc_key_length + element_length;
    return btree_page_h::check_space_for_insert (data_length);
}
bool btree_page_h::check_space_for_insert_node(const w_keystr_t& key) {
    w_assert1 (is_node());
    size_t data_length = key.get_length_as_keystr();
    return btree_page_h::check_space_for_insert (data_length);
}

bool btree_page_h::check_chance_for_norecord_split(const w_keystr_t& key_to_insert) const {
    if (!is_insertion_extremely_skewed_right()) {
        return false; // not a good candidate for norecord-split
    }
    if (nrecs() == 0) {
        return false;
    }
    if (usable_space() > used_space() * 3 / nrecs()
        && usable_space() > SM_PAGESIZE / 10
    ) {
        return false; // too early to split
    }

    const char* key_to_insert_raw = (const char*) key_to_insert.buffer_as_keystr();
    int key_to_insert_len = key_to_insert.get_length_as_keystr();
    int prefix_len = get_prefix_length();
    w_assert1(key_to_insert_len >= prefix_len && ::memcmp(get_prefix_key(), key_to_insert_raw, prefix_len) == 0); // otherwise why to insert to this page?
    
    int d;
    if (is_leaf()) {
        d = _compare_leaf_key_noprefix(nrecs() - 1, key_to_insert_raw + prefix_len, key_to_insert_len - prefix_len);
    } else {
        d = _compare_node_key_noprefix(nrecs() - 1, key_to_insert_raw + prefix_len, key_to_insert_len - prefix_len);
    }
    if (d <= 0) {
        return false; // not hitting highest. norecord-split will be useless
    }
    
    // we need some space for updated fence-high and chain-high
    smsize_t space_for_split = get_fence_low_length() + key_to_insert.get_length_as_keystr();
    if (get_chain_fence_high_length() == 0) {
        space_for_split += get_fence_high_length(); // newly set chain-high
    } else {
        space_for_split += get_chain_fence_high_length(); // otherwise chain-fence-high is unchanged
    }
    return (usable_space() >= align(space_for_split)); // otherwise it's too late
}

void btree_page_h::suggest_fence_for_split(w_keystr_t &mid,
                                           slotid_t& right_begins_from,
                                           const w_keystr_t &
#ifdef NORECORD_SPLIT_ENABLE
                                               triggering_key
#endif // NORECORD_SPLIT_ENABLE
    ) const {
// TODO for fair comparison with shore-mt, let's disable no-record-split for now
#ifdef NORECORD_SPLIT_ENABLE
    // if this is bulk-load case, simply make the new key as mid key (100% split)
    if (check_chance_for_norecord_split(triggering_key)) {
        right_begins_from = nrecs();
        if (is_leaf()) {
            w_keystr_t lastkey;
            leaf_key(nrecs() - 1, lastkey);
            size_t common_bytes = lastkey.common_leading_bytes(triggering_key);
            w_assert1(common_bytes < triggering_key.get_length_as_keystr());
            mid.construct_from_keystr(triggering_key.buffer_as_keystr(), common_bytes + 1);
        } else {
            mid = triggering_key;
        }
        return;
    }    
#endif // NORECORD_SPLIT_ENABLE
    
    w_assert1 (nrecs() >= 2);
    // pick the best separator key as follows.
    
    // first, pick the center point according to the skewness of past insertions to this page.
    slotid_t center_point = (nrecs() / 2); // usually just in the middle
    if (is_insertion_skewed_right()) {
        // last 5 inserts were on right-most, so let split on right-skewed point (90%)
        center_point = (nrecs() * 9 / 10);
    } else if (is_insertion_skewed_left()) {
        // last 5 inserts were on left-most, so let split on left-skewed point (10%)
        center_point = (nrecs() * 1 / 10);
    }
    
    // second, consider boundaries around the center point to pick the shortest separator key
    slotid_t start_point = (center_point - (nrecs() / 10) > 0) ? center_point - (nrecs() / 10) : 1;
    slotid_t end_point = (center_point + (nrecs() / 10) + 1 <= nrecs()) ? center_point + (nrecs() / 10) + 1 : nrecs();
    size_t sep_length = SM_PAGESIZE;
    const char *sep_key = NULL;
    right_begins_from = -1;
    for (slotid_t boundary = start_point; boundary < end_point; ++boundary) {
        if (is_leaf()) {
            // if we are splitting a leaf page, we are effectively designing a new separator key
            // which will be pushed up to parent. actually, this "mid" fence key is re-used
            // when we adopt a separator key later.
            size_t len1, len2;
            const char* k1 = _leaf_key_noprefix (boundary - 1, len1);
            const char* k2 = _leaf_key_noprefix (boundary, len2);
            // we apply suffix truncation here. We want a short separator key such that
            //   k1 < newkey <= k2.
            // For example, let k1=FEAR, k2=FFBC. new key can be
            //   FF, FEB, FFBC but not FFC or FEAR.
            // the newkey should send entries exclusively smaller than it to left,
            // inclusively larger than it to right. (remember, low fence key is inclusive (>=))
            
            // take common leading bytes +1 from right.
            // in above example, "FF". (common leading bytes=1)
            size_t common_bytes = w_keystr_t::common_leading_bytes((const unsigned char *) k1, len1, (const unsigned char *) k2, len2);
            w_assert1(common_bytes < len2); // otherwise the two keys are the same.
            // Note, we assume unique indexes, so the two keys are always different
            if (common_bytes + 1 < sep_length
                || (common_bytes + 1 == sep_length && boundary == center_point)) { // if tie, give a credit to center_point 
                right_begins_from = boundary;
                sep_length = common_bytes + 1;
                sep_key = k2;
            }
        } else {
            // for interior node, just return the existing key (we can shorten it though).
            size_t len;
            const char *k = _node_key_noprefix (boundary, len);
            if (len < sep_length
                || (len == sep_length && boundary == center_point)) { // if tie, give a credit to center_point 
                right_begins_from = boundary;
                sep_length = len;
                sep_key = k;
            }
        }
    }
    w_assert0(sep_key != NULL);
    w_assert1(sep_length != SM_PAGESIZE);
    mid.construct_from_keystr(get_prefix_key(), get_prefix_length(), sep_key, sep_length);
    w_assert0(right_begins_from >= 0 && right_begins_from <= nrecs());
    w_assert1(recalculate_fence_for_split(right_begins_from).compare(mid) == 0);
}

w_keystr_t btree_page_h::recalculate_fence_for_split(slotid_t right_begins_from) const {
    w_assert1(right_begins_from >= 0 && right_begins_from <= nrecs());
    w_keystr_t mid;
    if (is_leaf()) {
        size_t len1, len2;
        const char* k1 = _leaf_key_noprefix (right_begins_from - 1, len1);
        const char* k2 = _leaf_key_noprefix (right_begins_from, len2);        
        size_t common_bytes = w_keystr_t::common_leading_bytes((const unsigned char *) k1, len1, (const unsigned char *) k2, len2);
        w_assert1(common_bytes < len2); // otherwise the two keys are the same.
        mid.construct_from_keystr(get_prefix_key(), get_prefix_length(), k2, common_bytes + 1);
    } else {
        size_t len;
        const char *k = _node_key_noprefix (right_begins_from, len);
        mid.construct_from_keystr(get_prefix_key(), get_prefix_length(), k, len);
    }
    return mid;
}


void btree_page_h::rec_leaf(slotid_t slot,  w_keystr_t &key, cvec_t &el, bool &ghost) const {
    w_assert1(is_leaf());
    FUNC(btree_page_h::rec_leaf);

    int   key_length, data_length;
    char *trunc_key_data, *data;
    _get_leaf_fields(slot, key_length, trunc_key_data, data_length, data);

    int prefix_len = get_prefix_length();
    w_assert2 (prefix_len <= key_length);

    key.construct_from_keystr(get_prefix_key(), prefix_len, trunc_key_data, key_length - prefix_len);
    el.reset();
    el.put(data, data_length);
    ghost = is_ghost(slot);
}
void btree_page_h::rec_leaf(slotid_t slot,  w_keystr_t &key, char *el, smsize_t &elen, bool &ghost) const {
    w_assert1(is_leaf());
    FUNC(btree_page_h::rec_leaf);

    int   key_length, data_length;
    char *trunc_key_data, *data;
    _get_leaf_fields(slot, key_length, trunc_key_data, data_length, data);

    int prefix_len = get_prefix_length();
    w_assert2 (prefix_len <= key_length);

    key.construct_from_keystr(get_prefix_key(), prefix_len, trunc_key_data, key_length - prefix_len);
    w_assert1((int)elen >= data_length); // this method assumes the buffer is large enough!
    ::memcpy(el, data, data_length);
    elen = data_length;
    ghost = is_ghost(slot);
}
bool btree_page_h::dat_leaf(slotid_t slot,  char *el, smsize_t &elen, bool &ghost) const {
    w_assert1(is_leaf());
    FUNC(btree_page_h::dat_leaf);

    int   key_length, data_length;
    char *trunc_key_data, *data;
    _get_leaf_fields(slot, key_length, trunc_key_data, data_length, data);

    ghost = is_ghost(slot);
    if ((int)elen >= data_length) {
        ::memcpy(el, data, data_length);
        elen = data_length;
        return true;
    } else {
        // the buffer is too short
        elen = data_length;
        return false;
    }
}

void btree_page_h::dat_leaf_ref(slotid_t slot, const char *&el, smsize_t &elen, bool &ghost) const {
    w_assert1(is_leaf());
    FUNC(btree_page_h::dat_leaf_ref);

    int   key_length, data_length;
    char *trunc_key_data, *data;
    _get_leaf_fields(slot, key_length, trunc_key_data, data_length, data);

    elen  = data_length;
    el    = data;
    ghost = is_ghost(slot);
}

void btree_page_h::leaf_key(slotid_t slot,  w_keystr_t &key) const {
    w_assert1(is_leaf());

    int   key_length;
    char* trunc_key_data;
    _get_leaf_key_fields(slot, key_length, trunc_key_data);

    int prefix_len = get_prefix_length();
    w_assert2 (prefix_len <= key_length);

    key.construct_from_keystr(get_prefix_key(), prefix_len,
                              trunc_key_data, key_length - prefix_len);
}



void  btree_page_h::rec_node(slotid_t slot,  w_keystr_t &key, shpid_t &el) const {
    w_assert1(is_node());
    FUNC(btree_page_h::rec_node);
    w_assert1(!is_ghost(slot)); // non-leaf node can't be ghost

    int   trunc_key_length;
    char* trunc_key_data;
    _get_node_key_fields(slot, trunc_key_length, trunc_key_data);

    int prefix_len = get_prefix_length();

    key.construct_from_keystr(get_prefix_key(), prefix_len, trunc_key_data, trunc_key_length);
    el = page()->item_child(slot+1);
}
void btree_page_h::node_key(slotid_t slot,  w_keystr_t &key) const {
    w_assert1(is_node());

    int   trunc_key_length;
    char* trunc_key_data;
    _get_node_key_fields(slot, trunc_key_length, trunc_key_data);

    int prefix_len = get_prefix_length();

    key.construct_from_keystr(get_prefix_key(), prefix_len, trunc_key_data, trunc_key_length);
}

rc_t
btree_page_h::leaf_stats(btree_lf_stats_t& _stats) {
    _stats.hdr_bs    += hdr_sz + get_rec_space(-1);
    _stats.unused_bs += usable_space();

    int n = nrecs();
    _stats.entry_cnt += n;
    int16_t prefix_length = get_prefix_length();
    for (int i = 0; i < n; i++)  {
        btrec_t rec;
        rec.set(*this, i);
        ++_stats.unique_cnt; // always unique (otherwise a bug)
        _stats.key_bs            += rec.key().get_length_as_keystr() - prefix_length;
        _stats.data_bs           += rec.elen(); 
        _stats.entry_overhead_bs += page()->item_space(n+1) - (rec.key().get_length_as_keystr()-prefix_length) - rec.elen();
    }
    return RCOK;
}

rc_t
btree_page_h::int_stats(btree_int_stats_t& _stats) {
    _stats.unused_bs += usable_space();
    _stats.used_bs   += used_space();
    return RCOK;
}


smsize_t         
btree_page_h::max_entry_size = 
    // must be able to fit 2 entries to a page; data_sz must hold:
    //    fence record:                   max_item_overhead + max_entry_size*3   (low, high, chain keys)
    //    each of 2 regular leaf entries: max_item_overhead + max_entry_size + sizeof(key_length_t) [key len]
    //
    (btree_page::data_sz - 3*btree_page::max_item_overhead - 2*sizeof(key_length_t)) / 5;


void
btree_page_h::print(bool print_elem) {
    int i;
    const int L = 3;

    for (i = 0; i < L - level(); i++)  cout << '\t';
    cout << pid0() << "=" << pid0() << endl;

    for (i = 0; i < nrecs(); i++)  {
        for (int j = 0; j < L - level(); j++)  cout << '\t' ;

        btrec_t r(*this, i);

        cout << "<key = " << r.key() ;

        if ( is_leaf())  {
            if(print_elem) {
                cout << ", elen="  << r.elen() << " bytes: " << r.elem();
            }
        } else {
            cout << "pid = " << r.child();
        }
        cout << ">" << endl;
    }
    for (i = 0; i < L - level(); i++)  cout << '\t';
    cout << "]" << endl;
}

bool btree_page_h::is_consistent (bool check_keyorder, bool check_space) const {
    // does NOT check check-sum. the check can be done only by bufferpool
    // with seeing fresh data from the disk.
    
    // check poor-man's normalized key
    if (!_is_consistent_poormankey()) {
        w_assert1(false);
        return false;
    }

    // additionally check key-sortedness and uniqueness
    if (check_keyorder) {
        if (!_is_consistent_keyorder()) {
            w_assert1(false);
            return false;
        }
    }

    // additionally check record overlaps
    if (check_space) {
        if (!page()->_slots_are_consistent()) {
            w_assert1(false);
            return false;
        }
    }

    return true;
}
bool btree_page_h::_is_consistent_keyorder () const {
    const int    recs       = nrecs();
    const char*  lowkey     = get_fence_low_key();
    const size_t lowkey_len = get_fence_low_length();
    const size_t prefix_len = get_prefix_length();
    if (recs == 0) {
        // then just compare low-high and quit
        if (compare_with_fence_high(lowkey, lowkey_len) >= 0) {
            w_assert3(false);
            return false;
        }
        return true;
    }
    // now we know that first key exists
    
    if (is_leaf()) {
        // first key might be equal to low-fence which is inclusive
        size_t curkey_len;
        const char *curkey = _leaf_key_noprefix(0, curkey_len);
        if (w_keystr_t::compare_bin_str(lowkey + prefix_len, lowkey_len - prefix_len, curkey, curkey_len) > 0) {
            w_assert3(false);
            return false;
        }

        // then, check each record
        const char* prevkey = curkey;
        size_t prevkey_len = curkey_len;
        for (slotid_t slot = 1; slot < recs; ++slot) {
            curkey = _leaf_key_noprefix(slot, curkey_len);
            if (w_keystr_t::compare_bin_str(prevkey, prevkey_len, curkey, curkey_len) >= 0) { // this time must not be equal either
                w_assert3(false);
                return false;
            }
            prevkey     = curkey;
            prevkey_len = curkey_len;
        }
        
        // last record is also compared with high-fence
        if (compare_with_fence_high_noprefix(prevkey, prevkey_len) > 0) {
            w_assert3(false);
            return false;
        }
    } else {
        size_t curkey_len;
        const char *curkey = _node_key_noprefix(0, curkey_len);
        if (w_keystr_t::compare_bin_str(lowkey + prefix_len, lowkey_len - prefix_len, curkey, curkey_len) > 0) {
            w_assert3(false);
            return false;
        }

        const char* prevkey = curkey;
        size_t prevkey_len = curkey_len;
        for (slotid_t slot = 1; slot < recs; ++slot) {
            curkey = _node_key_noprefix(slot, curkey_len);
            if (w_keystr_t::compare_bin_str(prevkey, prevkey_len, curkey, curkey_len) >= 0) {
                w_assert3(false);
                return false;
            }
            prevkey = curkey;
            prevkey_len = curkey_len;
        }
        
        if (compare_with_fence_high_noprefix(prevkey, prevkey_len) > 0) {
            w_assert3(false);
            return false;
        }
    }
    
    return true;
}
bool btree_page_h::_is_consistent_poormankey () const {
    const int recs = nrecs();
    // the first record is fence key, so no poor man's key (always 0)
    poor_man_key fence_poormankey = page()->item_poor(0);
    if (fence_poormankey != 0) {
//        w_assert3(false);
        return false;
    }
    // for other records, check with the real key string in the record
    for (slotid_t slot = 0; slot < recs; ++slot) {
        poor_man_key poorman_key = _poor(slot);
        size_t curkey_len;
        const char* curkey = is_leaf() ? _leaf_key_noprefix(slot, curkey_len) : _node_key_noprefix(slot, curkey_len);
        poor_man_key correct_poormankey = extract_poor_man_key(curkey, curkey_len);
        if (poorman_key != correct_poormankey) {
//            w_assert3(false);
            return false;
        }
    }
    
    return true;
}


rc_t btree_page_h::defrag() { 
    w_assert1 (xct()->is_sys_xct());
    w_assert1 (is_fixed());
    w_assert1 (latch_mode() == LATCH_EX);

    vector<slotid_t> ghost_slots;
    for (int i=0; i<nitems(); i++) {
        if (page()->is_ghost(i)) {
            w_assert1(i >= 1); // fence record can't be ghost
            ghost_slots.push_back(i-1);
        }
    }
    // defrag doesn't need log if there were no ghost records:
    if (ghost_slots.size() > 0) {
        W_DO (log_btree_ghost_reclaim(*this, ghost_slots));
    }
    
    page()->compact();
    set_dirty();

    return RCOK;
}

bool btree_page_h::check_space_for_insert(size_t data_length) {
    size_t contiguous_free_space = usable_space();
    return contiguous_free_space >= page()->predict_item_space(data_length);
}
