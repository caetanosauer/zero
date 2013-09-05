/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define BTREE_C

#include "sm_int_2.h"

#include "vec_t.h"
#include "btree_page.h"
#include "btree_impl.h"
#include "page_bf_inline.h"
#include "sm_du_stats.h"
#include "crash.h"
#include "w_key.h"
#include <string>
#include <algorithm>

rc_t btree_page_h::init_fix_steal(
    btree_page_h*     parent,
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
    bool              log_it)
{
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

rc_t btree_page_h::format_steal(
    const lpid_t&     pid,
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
    bool              steal_src2_pid0)
{
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
    page()->lsn = lsn_t(0, 1);
    page()->pid = pid_copy;
    page()->tag = t_btree_p;
    page()->page_flags = 0;
    page()->record_head8 = to_offset8(data_sz);
    page()->nslots = page()->nghosts = page()->btree_consecutive_skewed_insertions = 0;

    page()->btree_root = root;
    page()->btree_pid0 = pid0;
    page()->btree_level = l;
    page()->btree_foster = foster;
    page()->btree_fence_low_length = (int16_t) fence_low.get_length_as_keystr();
    page()->btree_fence_high_length = (int16_t) fence_high.get_length_as_keystr();
    page()->btree_chain_fence_high_length = (int16_t) chain_fence_high.get_length_as_keystr();

    size_t prefix_len = fence_low.common_leading_bytes(fence_high);
    w_assert1(prefix_len <= fence_low.get_length_as_keystr() && prefix_len <= fence_high.get_length_as_keystr());
    w_assert1(prefix_len <= (1<<15));
    page()->btree_prefix_length = (int16_t) prefix_len;

    // set fence keys in first slot
    cvec_t fences;
    slot_length_t fence_total_len = sizeof(slot_length_t)
        + fence_low.get_length_as_keystr()
        + fence_high.get_length_as_keystr() - prefix_len
        + chain_fence_high.get_length_as_keystr();
    fences.put (&fence_total_len, sizeof(fence_total_len));
    fences.put (fence_low);
    // eliminate prefix part from fence_high
    fences.put ((const char*) fence_high.buffer_as_keystr() + prefix_len, fence_high.get_length_as_keystr() - prefix_len);
    fences.put(chain_fence_high);
    _append_nolog(fences, 0, false); // fence-key record doesn't need poormkey. set 0.

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
        slot_length_t key_len = (slot_length_t) steal_src2->get_fence_low_length();
        shpid_t stolen_pid0 = steal_src2->pid0();
        slot_length_t rec_len = sizeof(slot_length_t) * 2 + sizeof(shpid_t) + key_len - prefix_len;
        v.put(&rec_len, sizeof(rec_len));
        v.put(&key_len, sizeof(key_len));
        v.put(&stolen_pid0, sizeof(stolen_pid0));
        v.put(steal_src2->get_fence_low_key() + prefix_len, steal_src2->get_fence_low_length() - prefix_len);
        w_assert1(v.size() == rec_len);
        poor_man_key poormkey = extract_poor_man_key (steal_src2->get_fence_low_key(), steal_src2->get_fence_low_length(), prefix_len);
        _append_nolog(v, poormkey, false);
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

void btree_page_h::_steal_records(
    btree_page_h*             steal_src,
    int                  steal_from,
    int                  steal_to)
{
    w_assert2(steal_src);
    w_assert2(steal_from <= steal_to);
    w_assert2(steal_from >= 0);
    w_assert2(steal_to <= steal_src->nrecs());
    slot_length_t src_prefix_len = steal_src->get_prefix_length();
    slot_length_t new_prefix_len = get_prefix_length();
    int prefix_len_diff = (int) new_prefix_len - (int) src_prefix_len; // how many bytes do we reduce in the new page?
    const char* src_prefix_diff = steal_src->get_prefix_key() + src_prefix_len;
    if (prefix_len_diff < 0) { // then we have to increase
        src_prefix_diff += prefix_len_diff;
    }
    for (int i = steal_from; i < steal_to; ++i) {
        const unsigned char* src_rec = (const unsigned char*) steal_src->tuple_addr(i + 1);// +1 because it's btree_page_h
        cvec_t v;
        slot_length_t src_rec_len;
        slot_length_t klen;
        slot_length_t new_rec_len;
        slot_length_t src_consumed;
        if (is_leaf()) {
            src_rec_len = reinterpret_cast<const slot_length_t*>(src_rec)[0];
            klen = reinterpret_cast<const slot_length_t*>(src_rec)[1];
            new_rec_len = src_rec_len - prefix_len_diff;
            v.put(&new_rec_len, sizeof(new_rec_len));
            v.put(&klen, sizeof(klen));
            src_consumed = 2 * sizeof(slot_length_t);
        } else {
            v.put(src_rec, sizeof(shpid_t));
            const unsigned char *p = src_rec + sizeof(shpid_t);
            src_rec_len = *((const slot_length_t*) p);
            klen = src_rec_len - sizeof(shpid_t) - sizeof(slot_length_t) + src_prefix_len;
            new_rec_len = src_rec_len - prefix_len_diff;
            v.put(&new_rec_len, sizeof(new_rec_len));
            src_consumed = sizeof(slot_length_t) + sizeof(shpid_t);
        }

        //copy the remaining (and potentially a substring that was a prefix in old page)
        // also construct the poor man's normalized key
        poor_man_key new_poormkey;
        if (prefix_len_diff < 0) {
            // prefix is shorter in the new page. have to append a substring that was a prefix.
            v.put(src_prefix_diff, -prefix_len_diff);
            v.put(src_rec + src_consumed, src_rec_len - src_consumed);
            // this one is tricky.
            unsigned char poormkey_be[2]; // simply copied from the key, so still big-endian.
            if (prefix_len_diff == -1) {
                poormkey_be[0] = src_prefix_diff[0];
                if (src_rec_len - src_consumed > 0) {
                    poormkey_be[1] = src_rec[src_consumed];
                } else {
                    poormkey_be[1] = 0;
                }
            } else {
                poormkey_be[0] = src_prefix_diff[0];
                poormkey_be[1] = src_prefix_diff[1];
            }
            new_poormkey = deserialize16_ho(poormkey_be); // convert it to host endian
        } else {
            const unsigned char* remaining_rec = src_rec + src_consumed + prefix_len_diff;
            slot_length_t remaining_rec_len = src_rec_len - src_consumed - prefix_len_diff;
            v.put(remaining_rec, remaining_rec_len);
            // simply take the first few bytes
            new_poormkey = extract_poor_man_key(remaining_rec, klen - new_prefix_len);
        }
        w_assert1(new_rec_len == v.size());

        _append_nolog(v, new_poormkey, steal_src->is_ghost(i));
        w_assert3(is_consistent());
        w_assert5(_is_consistent_keyorder());
    }
}
rc_t btree_page_h::norecord_split (shpid_t foster,
    const w_keystr_t& fence_high, const w_keystr_t& chain_fence_high,
    bool log_it)
{
    w_assert1(compare_with_fence_low(fence_high) > 0);
    w_assert1(compare_with_fence_low(chain_fence_high) > 0);
    if (log_it) {
        W_DO(log_btree_foster_norecord_split (*this, foster, fence_high, chain_fence_high));
    }

    w_keystr_t fence_low;
    copy_fence_low_key(fence_low);
    slot_length_t new_prefix_len = fence_low.common_leading_bytes(fence_high);

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
        cvec_t fences;
        slot_length_t fence_total_len = sizeof(slot_length_t)
            + fence_low.get_length_as_keystr()
            + fence_high.get_length_as_keystr() - new_prefix_len
            + chain_fence_high.get_length_as_keystr();
        fences.put (&fence_total_len, sizeof(fence_total_len));
        fences.put (fence_low);
        // eliminate prefix part from fence_high
        fences.put ((const char*) fence_high.buffer_as_keystr() + new_prefix_len, fence_high.get_length_as_keystr() - new_prefix_len);
        fences.put(chain_fence_high);
        rc_t rc = replace_expand_fence_rec_nolog(fences);
        w_assert1(rc.err_num() != smlevel_0::eRECWONTFIT);// then why it passed check_chance_for_norecord_split()?
        w_assert1(!rc.is_error());

        //updates headers
        page()->btree_foster = foster;
        page()->btree_fence_high_length = (int16_t) fence_high.get_length_as_keystr();
        page()->btree_chain_fence_high_length = (int16_t) chain_fence_high.get_length_as_keystr();
        page()->btree_consecutive_skewed_insertions = 0; // reset this value too.
    }
    return RCOK;
}

rc_t btree_page_h::clear_foster()
{
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
btree_page_h::search(
    const w_keystr_t&     key,
    bool&         found_key, 
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
void btree_page_h::search_leaf(
    const char *key_raw, size_t key_raw_len,
    bool& found_key, slotid_t& ret_slot
) const
{
    w_assert3(is_leaf());
    w_assert1((uint) get_prefix_length() <= key_raw_len);
    w_assert1(::memcmp (key_raw, get_prefix_key(), get_prefix_length()) == 0);

    const char *key_noprefix = key_raw + get_prefix_length();
    int key_len = key_raw_len - get_prefix_length();
    poor_man_key poormkey = extract_poor_man_key(key_noprefix, key_len);
    const void *key_noprefix_remain = key_noprefix + sizeof(poor_man_key);
    int key_len_remain = key_len - sizeof(poor_man_key);
    found_key = false;
    
    const char* begin_slot = btree_page_h::slot_addr(0 + 1);
    const char* end_slot = begin_slot + slot_sz * nrecs();
    for (const char* cur_slot = begin_slot; cur_slot != end_slot; cur_slot += slot_sz) {
        poor_man_key cur_poormkey = *reinterpret_cast<const poor_man_key*>(cur_slot + sizeof(slot_offset8_t));
        if (cur_poormkey < poormkey) {
            w_assert1(_compare_leaf_key_noprefix(((cur_slot - begin_slot) / slot_sz), key_noprefix, key_len) < 0);
            continue;
        }
        int slot = ((cur_slot - begin_slot) / slot_sz);
        if (cur_poormkey > poormkey) {
            ret_slot = slot;
            w_assert1(slot == 0 || _compare_leaf_key_noprefix(slot - 1, key_noprefix, key_len) < 0);
            w_assert1(_compare_leaf_key_noprefix(slot, key_noprefix, key_len) > 0);
            return;
        }
        slot_offset8_t offset8 = *reinterpret_cast<const slot_offset8_t*>(cur_slot);
        int d = _compare_leaf_key_noprefix_remain(offset8, key_noprefix_remain, key_len_remain);
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

void btree_page_h::search_node(
    const w_keystr_t&     key,
    slotid_t&             ret_slot
) const
{
    w_assert3(!is_leaf());

    const char *key_raw = (const char *) key.buffer_as_keystr();
    w_assert1((uint) get_prefix_length() <= key.get_length_as_keystr());
    w_assert1(::memcmp (key_raw, get_prefix_key(), get_prefix_length()) == 0);

    const char *key_noprefix = key_raw + get_prefix_length();
    int key_len = key.get_length_as_keystr() - get_prefix_length();
    poor_man_key poormkey = extract_poor_man_key(key_noprefix, key_len);
    const void *key_noprefix_remain = key_noprefix + sizeof(poor_man_key);
    int key_len_remain = key_len - sizeof(poor_man_key);

    w_assert1 (pid0() != 0);

    const char* begin_slot = btree_page_h::slot_addr(0 + 1);
    const char* end_slot = begin_slot + slot_sz * nrecs();
    for (const char* cur_slot = begin_slot; cur_slot != end_slot; cur_slot += slot_sz) {
        poor_man_key cur_poormkey = *reinterpret_cast<const poor_man_key*>(cur_slot + sizeof(slot_offset8_t));
        if (cur_poormkey < poormkey) {
            w_assert1(_compare_node_key_noprefix(((cur_slot - begin_slot) / slot_sz), key_noprefix, key_len) < 0);
            continue;
        }
        int slot = ((cur_slot - begin_slot) / slot_sz);
        if (cur_poormkey > poormkey) {
            ret_slot = slot - 1;
            w_assert1(slot == 0 || _compare_node_key_noprefix(slot - 1, key_noprefix, key_len) < 0);
            w_assert1(_compare_node_key_noprefix(slot, key_noprefix, key_len) > 0);
            return;
        }
        slot_offset8_t offset8 = *reinterpret_cast<const slot_offset8_t*>(cur_slot);
        int d = _compare_node_key_noprefix_remain(offset8, key_noprefix_remain, key_len_remain);
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

void btree_page_h::search_leaf(
    const char *key_raw, size_t key_raw_len,
    bool& found_key, slotid_t& ret_slot
) const
{
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
        poor_man_key last_poormkey = btree_page_h::tuple_poormkey(last_slot + 1); // +1 because btree_page_h
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

        poor_man_key cur_poormkey = btree_page_h::tuple_poormkey(mi + 1); // +1 because btree_page_h
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

void btree_page_h::search_node(
    const w_keystr_t&             key,
    slotid_t&             ret_slot
) const
{
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
        poor_man_key cur_poormkey = btree_page_h::tuple_poormkey(0 + 1); // +1 because btree_page_h
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
        poor_man_key last_poormkey = btree_page_h::tuple_poormkey(last_slot + 1); // +1 because btree_page_h
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

        poor_man_key cur_poormkey = btree_page_h::tuple_poormkey(mi + 1); // +1 because btree_page_h
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

void btree_page_h::_update_btree_consecutive_skewed_insertions(slotid_t slot)
{
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

rc_t btree_page_h::insert_node(const w_keystr_t &key, slotid_t slot, shpid_t child)
{
    FUNC(btree_page_h::insert);
    
    w_assert3 (is_node());
    w_assert3(child);
    w_assert3 (is_consistent(true, false));

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

    // update btree_consecutive_skewed_insertions. this is just statistics and not logged.
    _update_btree_consecutive_skewed_insertions (slot);

    slot_length_t klen = key.get_length_as_keystr();

    // prefix of the key is fixed in this page, so we can simply
    // peel off leading bytes from the key+el.
    slot_length_t prefix_length = get_prefix_length();  // length of prefix of inserted tuple
    w_assert3(prefix_length <= klen);

    slot_length_t rec_len = sizeof(slot_length_t) + sizeof(child) + klen - prefix_length;

    // see the record format in btree_page_h class comments.
    vec_t v; // the record data
    v.put(&child, sizeof(child));
    v.put(&rec_len, sizeof(rec_len));  // because we do this, we have to hold the variable "rec_len" until the end of this function!
    v.put(key, prefix_length, klen - prefix_length);
    poor_man_key poormkey = extract_poor_man_key(key.buffer_as_keystr(), klen, prefix_length);

    W_DO( _insert_expand_nolog(slot, v, poormkey) ); // we don't log it. btree_impl::adopt() does the logging

    w_assert3 (is_consistent(true, false));
    w_assert5 (is_consistent(true, true));

    return RCOK;
}
rc_t btree_page_h::_insert_expand_nolog(slotid_t slot, const cvec_t &vec, poor_man_key poormkey)
{
    slotid_t idx = slot + 1; // slot index in btree_page_h
    w_assert1(idx >= 0 && idx <= nslots());
    w_assert3 (_is_consistent_space());
    // this shouldn't happen. the caller should have checked with check_space_for_insert()
    if (!check_space_for_insert(vec.size())) {
        return RC(smlevel_0::eRECWONTFIT);
    }

     //  Log has already been generated ... the following actions must succeed!
     // shift slot array. if we are inserting to the end (idx == nslots), do nothing
    if (idx != nslots())    {
        ::memmove(page()->data + slot_sz * (idx + 1),
                page()->data + slot_sz * (idx),
                (nslots() - idx) * slot_sz);
    }

    //  Fill up the slots and data
    slot_offset8_t new_record_head8 = page()->record_head8 - to_aligned_offset8(vec.size());
    char* slot_p = btree_page_h::slot_addr(idx);
    *reinterpret_cast<slot_offset8_t*>(slot_p) = new_record_head8;
    *reinterpret_cast<poor_man_key*>(slot_p + sizeof(slot_offset8_t)) = poormkey;
    vec.copy_to(page()->data_addr8(new_record_head8));
    page()->record_head8 = new_record_head8;
    ++page()->nslots;

    w_assert3(get_rec_size(slot) == vec.size());
    w_assert3(btree_page_h::tuple_poormkey(idx) == poormkey);
    w_assert3 (_is_consistent_space());
    return RCOK;
}

void btree_page_h::_append_nolog(const cvec_t &vec, poor_man_key poormkey, bool ghost)
{
    w_assert3 (check_space_for_insert(vec.size()));
    w_assert5 (_is_consistent_space());
    
    //  Fill up the slots and data
    slot_offset8_t new_record_head8 = page()->record_head8 - to_aligned_offset8(vec.size());
    char* slot_p = btree_page_h::slot_addr(nslots());
    *reinterpret_cast<slot_offset8_t*>(slot_p) = ghost ? -new_record_head8 : new_record_head8;
    *reinterpret_cast<poor_man_key*>(slot_p + sizeof(slot_offset8_t)) = poormkey;
    vec.copy_to(page()->data_addr8(new_record_head8));
    page()->record_head8 = new_record_head8;
    if (ghost) {
        ++page()->nghosts;
    }
    ++page()->nslots;

#if W_DEBUG_LEVEL>=1
    if (nslots() == 1) {
        // the inserted record was the special fence record!
        w_assert1(poormkey == 0);
        w_assert1(get_fence_rec_size() == vec.size());
    } else {
        w_assert1(get_rec_size(nrecs() - 1) == vec.size());
        w_assert1(btree_page_h::tuple_poormkey(nslots() - 1) == poormkey);
    }
    w_assert5 (_is_consistent_space());
#endif //W_DEBUG_LEVEL>=1
}

void btree_page_h::_expand_rec(slotid_t slot, slot_length_t rec_len)
{
    slotid_t idx = slot + 1; // slot index in btree_page_h
    w_assert1(idx >= 0 && idx < nslots());
    w_assert1(usable_space() >= align(rec_len));
    w_assert3(_is_consistent_space());

    bool ghost = is_ghost(slot);
    slot_length_t old_rec_len = get_rec_size(slot);
    void* old_rec = btree_page_h::tuple_addr(idx);
    slot_offset8_t new_record_head8 = page()->record_head8 - to_aligned_offset8(rec_len);
    btree_page_h::change_slot_offset(idx, ghost ? -new_record_head8 : new_record_head8);
    page()->record_head8 = new_record_head8;

    void* new_rec = btree_page_h::tuple_addr(idx);
    *reinterpret_cast<slot_length_t*>(new_rec) = rec_len; // set new size
    // ::memcpy (new_rec, &rec_len, sizeof(slot_length_t)); // set new size
    ::memcpy (((char*)new_rec) + sizeof(slot_length_t), ((char*)old_rec) + sizeof(slot_length_t),
              old_rec_len - sizeof(slot_length_t)); // copy the original data
#if W_DEBUG_LEVEL>0
    ::memset (old_rec, 0, old_rec_len); // clear old slot
#endif // W_DEBUG_LEVEL>0
    w_assert3(get_rec_size(slot) == rec_len);
    w_assert3 (_is_consistent_space());
}

rc_t btree_page_h::replace_expand_fence_rec_nolog(const cvec_t &fences)
{
    w_assert1(nslots() > 0);
    slot_offset8_t current_offset8 = btree_page_h::tuple_offset8(0);
    slot_length_t current_size = get_fence_rec_size();
    if (align(fences.size()) <= align(current_size)) {
        // then simply overwrite
        void* addr = btree_page_h::tuple_addr(0);
        fences.copy_to(addr);
        w_assert1(*reinterpret_cast<slot_length_t*>(addr) == (slot_length_t) fences.size());
        w_assert1 (get_fence_rec_size() == (slot_length_t) fences.size());
        return RCOK;
    }
    // otherwise, have to expand
    if (usable_space() < align(fences.size())) {
        return RC(smlevel_0::eRECWONTFIT);
    }
    
    w_assert3(_is_consistent_space());
    slot_offset8_t new_record_head8 = page()->record_head8 - to_aligned_offset8(fences.size());
    slot_offset8_t new_offset8 = current_offset8 < 0 ? -new_record_head8 : new_record_head8; // for ghost records
    btree_page_h::change_slot_offset(0, new_offset8);
    void* addr = btree_page_h::tuple_addr(0);
    fences.copy_to(addr);
    w_assert1(*reinterpret_cast<slot_length_t*>(addr) == (slot_length_t) fences.size());
    w_assert1 (get_fence_rec_size() == (slot_length_t) fences.size());
    page()->record_head8 = new_record_head8;
    w_assert3 (_is_consistent_space());    
    return RCOK;
}


rc_t btree_page_h::remove_shift_nolog(slotid_t slot)
{
    slotid_t idx = slot + 1; // slot index in btree_page_h
    w_assert1(idx >= 0 && idx < nslots());
    w_assert1(slot >= 0); // this method does NOT assume shifting fence record
    w_assert3 (_is_consistent_space());
    
    slot_offset8_t removed_offset8 = btree_page_h::tuple_offset8(idx);
    slot_length_t removed_length = get_rec_size(slot);

    // Shift slot array. if we are removing last (idx==nslots - 1), do nothing.
    if (idx < nslots() - 1) {
        ::memmove(page()->data + slot_sz * (idx),
            page()->data + slot_sz * (idx + 1),
            (nslots() - 1 - idx) * slot_sz);
    }
    --page()->nslots;

    bool ghost = false;
    if (removed_offset8 < 0) {
        removed_offset8 = -removed_offset8; // ghost record
        ghost = true;
    }
    if (page()->record_head8 == removed_offset8) {
        // then, we are pushing down the record_head8. lucky!
        w_assert3 (_is_consistent_space());
        page()->record_head8 += to_aligned_offset8(removed_length);
    }
    
    if (ghost) {
        --page()->nghosts;
    }

    w_assert3 (_is_consistent_space());
    return RCOK;
}

bool btree_page_h::_is_enough_spacious_ghost(
    const w_keystr_t &key, slotid_t slot,
    const cvec_t&        el)
{
    w_assert2(is_leaf());
    w_assert2(is_ghost(slot));
    size_t rec_size = calculate_rec_size(key, el);
    return (align(get_rec_size(slot)) >= rec_size);
}

rc_t btree_page_h::replace_ghost(
    const w_keystr_t &key,
    const cvec_t &elem)
{
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
    slot_length_t rec_size = calculate_rec_size(key, elem);
    slot_length_t org_rec_size = get_rec_size(slot);
    if (align(rec_size) > align(org_rec_size)) {
        _expand_rec (slot, rec_size);
    }
    
    w_assert1 (_is_enough_spacious_ghost(key, slot, elem));
#if W_DEBUG_LEVEL > 2
    btrec_t rec (*this, slot);
    w_assert3 (rec.key().compare(key) == 0);
#endif // W_DEBUG_LEVEL > 2

    slot_length_t klen = key.get_length_as_keystr();
    int16_t prefix_length = get_prefix_length();

    char *buf = (char*) btree_page_h::tuple_addr(slot + 1);
    if (rec_size != org_rec_size) {
        // update only when necessary
        w_assert1(reinterpret_cast<slot_length_t*>(buf)[0] == org_rec_size);
        *reinterpret_cast<slot_length_t*>(buf) = rec_size;
    }
    // klen should be same
    w_assert1(reinterpret_cast<slot_length_t*>(buf)[1] == klen);
    elem.copy_to(buf + sizeof(slot_length_t) * 2 + klen - prefix_length);

    // Reuse everything. just change the record data.
    slot_offset8_t offset8 = btree_page_h::tuple_offset8(slot + 1);
    w_assert1 (offset8 < 0); // it should be ghost
    btree_page_h::change_slot_offset(slot + 1, -offset8);
    return RCOK;
}

rc_t btree_page_h::replace_el_nolog(slotid_t slot, const cvec_t &elem)
{
    w_assert2( is_fixed());
    w_assert2( is_leaf());

    w_assert1 (!is_ghost(slot));
    
    char *buf = (char*) btree_page_h::tuple_addr(slot + 1);
    slot_length_t org_rec_size = reinterpret_cast<slot_length_t*>(buf)[0];
    slot_length_t klen = reinterpret_cast<slot_length_t*>(buf)[1];
    slot_length_t prefix_length = get_prefix_length();

    // do we need to expand?
    slot_length_t rec_size = sizeof(slot_length_t) * 2 + klen - prefix_length + elem.size();
    if (align(rec_size) > align(org_rec_size)) {
        if (!check_space_for_insert(rec_size)) {
            return RC(smlevel_0::eRECWONTFIT);
        }
        _expand_rec (slot, rec_size);
        w_assert1(btree_page_h::tuple_addr(slot + 1) != buf);
        buf = (char *) btree_page_h::tuple_addr(slot + 1);
    }

    slot_length_t* array = reinterpret_cast<slot_length_t*>(buf);
    array[0] = rec_size;
    array[1] = klen;
    elem.copy_to(buf + sizeof(slot_length_t) * 2 + klen - prefix_length);
    return RCOK;
}

void btree_page_h::overwrite_el_nolog(slotid_t slot, smsize_t offset,
                                    const char *new_el, smsize_t elen)
{
    w_assert2( is_fixed());
    w_assert2( is_leaf());
    w_assert1 (!is_ghost(slot));
    
    char *buf = (char*) btree_page_h::tuple_addr(slot + 1);
    slot_length_t klen = reinterpret_cast<slot_length_t*>(buf)[1];
    slot_length_t prefix_length = get_prefix_length();

    w_assert1 (get_rec_size(slot) >= offset + elen + sizeof(slot_length_t) * 2 + klen - prefix_length);

    ::memcpy (buf + sizeof(slot_length_t) * 2 + klen - prefix_length + offset, new_el, elen);
}

void btree_page_h::reserve_ghost(const char *key_raw, size_t key_raw_len, int record_size)
{
    w_assert1(check_space_for_insert(record_size));
    w_assert1 (is_leaf()); // ghost only exists in leaf

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
    
    int16_t prefix_len = get_prefix_length();
    poor_man_key poormkey = extract_poor_man_key(key_raw, key_raw_len, prefix_len);
    if (slot != nrecs())    {
        // note that slot=0 is fence
        ::memmove(page()->data + slot_sz * (slot + 2),
                page()->data + slot_sz * (slot + 1),
                (nrecs() - slot) * slot_sz);
    }
    slot_offset8_t new_record_head8 = page()->record_head8 - to_aligned_offset8(record_size);
    char* slot_p = btree_page_h::slot_addr(slot + 1);
    *reinterpret_cast<slot_offset8_t*>(slot_p) = -new_record_head8; // ghost record
    *reinterpret_cast<poor_man_key*>(slot_p + sizeof(slot_offset8_t)) = poormkey;

    // make a dummy record that has the desired length
    slot_length_t klen = key_raw_len;

    char *buf = page()->data_addr8(new_record_head8);
    slot_length_t *array = reinterpret_cast<slot_length_t*>(buf);
    array[0] = (slot_length_t) record_size;
    array[1] = klen;
    ::memcpy (buf + 2 * sizeof(slot_length_t), key_raw + prefix_len, klen - prefix_len);
    // that's it. doesn't have to write data. do nothing.

    page()->record_head8 = new_record_head8;
    ++page()->nslots;
    ++page()->nghosts;
    
    w_assert3(get_rec_size(slot) == (slot_length_t) record_size);
    w_assert3(btree_page_h::tuple_poormkey(slot + 1) == poormkey);
    w_assert3(_is_consistent_space());
}

void btree_page_h::mark_ghost(slotid_t slot)
{
    slotid_t idx = slot + 1; // slot index in btree_page_h
    w_assert0(tag() == t_btree_p);
    w_assert1(idx >= 0 && idx < nslots());
    w_assert1(slot >= 0); // fence record cannot be a ghost
    slot_offset8_t *offset8 = reinterpret_cast<slot_offset8_t*>(page()->data + slot_sz * idx);
    if (*offset8 < 0) {
        return; // already ghost. do nothing
    }
    w_assert1(*offset8 > 0);
    // reverse the sign to make it a ghost
    *offset8 = -(*offset8);
    ++page()->nghosts;
    set_dirty();
}

void btree_page_h::unmark_ghost(slotid_t slot)
{
    slotid_t idx = slot + 1; // slot index in btree_page_h
    w_assert0(tag() == t_btree_p);
    w_assert1(idx >= 0 && idx < nslots());
    w_assert1(slot >= 0); // fence record cannot be a ghost
    slot_offset8_t *offset8 = reinterpret_cast<slot_offset8_t*>(page()->data + slot_sz * idx);
    if (*offset8 > 0) {
        return; // already non-ghost. do nothing
    }
    w_assert1(*offset8 < 0);
    // reverse the sign to make it a non-ghost
    *offset8 = -(*offset8);
    --page()->nghosts;
    set_dirty();
}


bool btree_page_h::check_space_for_insert_leaf(
    const w_keystr_t&     key,
    const cvec_t&     el)
{
    w_assert1 (is_leaf());
    size_t rec_size = 2 * sizeof(slot_length_t) + key.get_length_as_keystr() + el.size();
    return btree_page_h::check_space_for_insert (rec_size);
}
bool btree_page_h::check_space_for_insert_node(const w_keystr_t&     key)
{
    w_assert1 (is_node());
    size_t rec_size = sizeof(slot_length_t) + key.get_length_as_keystr() + sizeof (shpid_t);
    return btree_page_h::check_space_for_insert (rec_size);
}

bool btree_page_h::check_chance_for_norecord_split(const w_keystr_t& key_to_insert) const
{
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

void btree_page_h::suggest_fence_for_split(
    w_keystr_t &mid,
    slotid_t& right_begins_from,
    const w_keystr_t &
#ifdef NORECORD_SPLIT_ENABLE
        triggering_key
#endif // NORECORD_SPLIT_ENABLE
    ) const
{
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


void btree_page_h::rec_leaf(slotid_t idx,  w_keystr_t &key, cvec_t &el, bool &ghost) const
{
    w_assert1(is_leaf());
    FUNC(btree_page_h::rec_leaf);
    ghost = is_ghost(idx);
    const char* base = (char*) btree_page_h::tuple_addr(idx + 1);
    const char* p = base;
    
    el.reset();

    slot_length_t rec_len = ((slot_length_t*) p)[0];
    slot_length_t key_len = ((slot_length_t*) p)[1];
    slot_length_t prefix_len = (slot_length_t) get_prefix_length();
    slot_length_t el_len = rec_len - sizeof(slot_length_t) * 2 - key_len + prefix_len;
    p += sizeof(slot_length_t) * 2;
    
    w_assert2 (prefix_len <= key_len);
    key.construct_from_keystr(get_prefix_key(), prefix_len, p, key_len - prefix_len); // also from p
    el.put(p + key_len - prefix_len, el_len);
}
void btree_page_h::rec_leaf(slotid_t idx,  w_keystr_t &key, char *el, smsize_t &elen, bool &ghost) const
{
    w_assert1(is_leaf());
    FUNC(btree_page_h::rec_leaf);
    ghost = is_ghost(idx);
    const char* base = (char*) btree_page_h::tuple_addr(idx + 1);
    const char* p = base;
    
    slot_length_t rec_len = ((slot_length_t*) p)[0];
    slot_length_t key_len = ((slot_length_t*) p)[1];
    slot_length_t prefix_len = (slot_length_t) get_prefix_length();
    slot_length_t el_len = rec_len - sizeof(slot_length_t) * 2 - key_len + prefix_len;
    w_assert2 (prefix_len <= key_len);
    w_assert1(elen >= (smsize_t) el_len); // this method assumes the buffer is large enough!
    p += sizeof(slot_length_t) * 2;
    
    key.construct_from_keystr(get_prefix_key(), prefix_len, p, key_len - prefix_len); // also from p
    ::memcpy(el, p + key_len - prefix_len, el_len);
    elen = el_len;
}
bool btree_page_h::dat_leaf(slotid_t idx,  char *el, smsize_t &elen, bool &ghost) const
{
    w_assert1(is_leaf());
    FUNC(btree_page_h::dat_leaf);
    ghost = is_ghost(idx);
    const char* base = (char*) btree_page_h::tuple_addr(idx + 1);
    const char* p = base;
    
    slot_length_t rec_len = ((slot_length_t*) p)[0];
    slot_length_t key_len = ((slot_length_t*) p)[1];
    slot_length_t prefix_len = (slot_length_t) get_prefix_length();
    slot_length_t el_len = rec_len - sizeof(slot_length_t) * 2 - key_len + prefix_len;
    p += sizeof(slot_length_t) * 2;
    w_assert2 (prefix_len <= key_len);
    if (elen >= (smsize_t) el_len) {
        ::memcpy(el, p + key_len - prefix_len, el_len);
        elen = el_len;
        return true;
    } else {
        // the buffer is too short
        elen = el_len;
        return false;
    }
}

void btree_page_h::dat_leaf_ref(slotid_t idx, const char *&el, smsize_t &elen, bool &ghost) const
{
    w_assert1(is_leaf());
    FUNC(btree_page_h::dat_leaf_ref);
    ghost = is_ghost(idx);
    const char* base = (char*) btree_page_h::tuple_addr(idx + 1);
    const char* p = base;
    
    slot_length_t rec_len = ((slot_length_t*) p)[0];
    slot_length_t key_len = ((slot_length_t*) p)[1];
    slot_length_t prefix_len = (slot_length_t) get_prefix_length();
    elen = rec_len - sizeof(slot_length_t) * 2 - key_len + prefix_len;
    p += sizeof(slot_length_t) * 2;
    
    w_assert2 (prefix_len <= key_len);
    el = p + key_len - prefix_len;
}

void btree_page_h::leaf_key(slotid_t idx,  w_keystr_t &key) const
{
    w_assert1(is_leaf());
    const char* p = (char*) btree_page_h::tuple_addr(idx + 1);
    slot_length_t key_len = ((slot_length_t*) p)[1];
    p += sizeof(slot_length_t) * 2;
    slot_length_t prefix_len = get_prefix_length();
    w_assert2 (prefix_len <= key_len);
    key.construct_from_keystr(get_prefix_key(), prefix_len, p, key_len - prefix_len); // also from p
}

void  btree_page_h::rec_node(slotid_t idx,  w_keystr_t &key, shpid_t &el) const
{
    w_assert1(is_node());
    FUNC(btree_page_h::rec_node);
    w_assert1(!is_ghost(idx)); // non-leaf node can't be ghost
    const char* p = (const char*) btree_page_h::tuple_addr(idx + 1);
    
    el = *((shpid_t*) p);
    p += sizeof(shpid_t);
    slot_length_t rec_len = *((slot_length_t*) p);
    slot_length_t key_len_noprefix = rec_len - sizeof(shpid_t) - sizeof(slot_length_t);
    p += sizeof(slot_length_t);
    slot_length_t prefix_len = get_prefix_length();
    key.construct_from_keystr(get_prefix_key(), prefix_len, p, key_len_noprefix); // also from p
}
void btree_page_h::node_key(slotid_t idx,  w_keystr_t &key) const
{
    w_assert1(is_node());
    const char* p = (char*) btree_page_h::tuple_addr(idx + 1);
    p += sizeof(shpid_t);
    slot_length_t rec_len = *((const slot_length_t*) p);
    slot_length_t prefix_len = get_prefix_length();
    slot_length_t key_len = rec_len - sizeof(shpid_t) - sizeof(slot_length_t) + prefix_len;
    p += sizeof(slot_length_t);
    w_assert2 (prefix_len <= key_len);
    key.construct_from_keystr(get_prefix_key(), prefix_len, p, key_len - prefix_len); // also from p
}

rc_t
btree_page_h::leaf_stats(btree_lf_stats_t& _stats)
{

    _stats.hdr_bs += (hdr_sz + slot_sz + align(get_fence_rec_size()));
    _stats.unused_bs += usable_space();

    int n = nrecs();
    _stats.entry_cnt += n;
    int16_t prefix_length = get_prefix_length();
    for (int i = 0; i < n; i++)  {
        btrec_t rec;
        rec.set(*this, i);
        ++_stats.unique_cnt; // always unique (otherwise a bug)
        _stats.key_bs += rec.key().get_length_as_keystr() - prefix_length;
        _stats.data_bs += rec.elen(); 
        _stats.entry_overhead_bs += slot_sz + sizeof(int16_t) * 2;
    }
    return RCOK;
}

rc_t
btree_page_h::int_stats(btree_int_stats_t& _stats)
{
    _stats.unused_bs += usable_space();
    _stats.used_bs += used_space();
    return RCOK;
}

void
btree_page_h::page_usage(int& data_size, int& header_size, int& unused,
                   int& alignment, tag_t& t, slotid_t& no_slots)
{
    // returns space allocated for headers in this page
    // returns unused space in this page
    data_size = unused = alignment = 0;

    // space used for headers
    header_size = sizeof(generic_page) - data_sz;
    
    // calculate space wasted in data alignment
    for (int i=0 ; i<nslots(); i++) {
        // if slot is not no-record slot
        if ( btree_page_h::tuple_offset8(i) != 0 ) {
            slot_length_t len = (i == 0 ? get_fence_rec_size() : get_rec_size(i - 1));
            data_size += len;
            alignment += int(align(len) - len);
        }
    }
    // unused space
    unused = sizeof(generic_page) - header_size - data_size - alignment;

    t        = tag();        // the type of page 
    no_slots = nslots();  // nu of slots in this page

    w_assert1(data_size + header_size + unused + alignment == sizeof(generic_page));
}
btrec_t& 
btrec_t::set(const btree_page_h& page, slotid_t slot)
{
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

smsize_t                        
btree_page_h::overhead_requirement_per_entry =
            4 // for the key length (in btree_page_h)
            +
            sizeof(shpid_t) // for the interior nodes (in btree_page_h)
            ;

smsize_t         
btree_page_h::max_entry_size = // must be able to fit 2 entries to a page
    (
        ( (smlevel_0::page_sz - hdr_sz - slot_sz)
            >> 1) 
        - 
        overhead_requirement_per_entry
    ) 
    // round down to aligned size
    & ~ALIGNON1 
    ;

void
btree_page_h::print(
    bool print_elem 
)
{
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

bool btree_page_h::is_consistent (bool check_keyorder, bool check_space) const
{
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
        if (!_is_consistent_space()) {
            w_assert1(false);
            return false;
        }
    }

    return true;
}
bool btree_page_h::_is_consistent_keyorder () const
{
    const int recs = nrecs();
    const char* lowkey = get_fence_low_key();
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
            prevkey = curkey;
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
bool btree_page_h::_is_consistent_poormankey () const
{
    const int recs = nrecs();
    // the first record is fence key, so no poor man's key (always 0)
    poor_man_key fence_poormankey = btree_page_h::tuple_poormkey(0);
    if (fence_poormankey != 0) {
//        w_assert3(false);
        return false;
    }
    // for other records, check with the real key string in the record
    for (slotid_t slot = 0; slot < recs; ++slot) {
        poor_man_key poorman_key = btree_page_h::tuple_poormkey(slot + 1); //+1 as btree_page_h
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


bool btree_page_h::_is_consistent_space () const
{
    // this is not a part of check. should be always true.
    w_assert1((size_t) slot_sz * nslots() <= (size_t) page()->get_record_head_byte());
    
    // check overlapping records.
    // rather than using std::map, use array and std::sort for efficiency.
    // high-16bits=offset, low-16bits=len
    const slotid_t slot_cnt = nslots();
    uint32_t *sorted_slots = new uint32_t[slot_cnt];
    w_auto_delete_array_t<uint32_t> sorted_slots_autodel (sorted_slots);
    for (slotid_t slot = 0; slot < slot_cnt; ++slot) {
        slot_offset8_t offset8 = btree_page_h::tuple_offset8(slot);
        slot_length_t len = (slot == 0 ? get_fence_rec_size() : get_rec_size(slot - 1));
        if (offset8 < 0) {
            sorted_slots[slot] = ((-offset8) << 16) + len;// this means ghost slot. reverse the sign
        } else if (offset8 == 0) {
            // this means no-record slot. ignore it.
            sorted_slots[slot] = 0;
        } else {
            sorted_slots[slot] = (offset8 << 16) + len;
        }
    }
    std::sort(sorted_slots, sorted_slots + slot_cnt);

    bool first = true;
    size_t prev_end = 0;
    for (slotid_t slot = 0; slot < slot_cnt; ++slot) {
        if (sorted_slots[slot] == 0) {
            continue;
        }
        size_t offset = to_byte_offset(sorted_slots[slot] >> 16);
        size_t len = sorted_slots[slot] & 0xFFFF;
        if (offset < (size_t) page()->get_record_head_byte()) {
            DBG(<<"the slot starting at offset " << offset <<  " is located before record_head " << page()->get_record_head_byte());
            w_assert1(false);
            return false;
        }
        if (offset + len > data_sz) {
            DBG(<<"the slot starting at offset " << offset <<  " goes beyond the the end of data area!");
            w_assert1(false);
            return false;
        }
        if (first) {
            first = false;
        } else {
            if (prev_end > offset) {
                DBG(<<"the slot starting at offset " << offset <<  " overlaps with another slot ending at " << prev_end);
                w_assert3(false);
                return false;
            }
        }
        prev_end = offset + len;
    }
    return true;
}

rc_t btree_page_h::defrag(slotid_t popped)
{
    w_assert1(popped >= -1 && popped < nslots());
    w_assert1 (xct()->is_sys_xct());
    w_assert1 (is_fixed());
    w_assert1 (latch_mode() == LATCH_EX);
    w_assert3 (_is_consistent_space());
    
    //  Copy headers to scratch area.
    btree_page scratch;
    char *scratch_raw = reinterpret_cast<char*>(&scratch);
    ::memcpy(scratch_raw, page(), hdr_sz);
#ifdef ZERO_INIT
    ::memset(scratch.data, 0, data_sz);
#endif // ZERO_INIT
    
    //  Move data back without leaving holes
    slot_offset8_t new_offset8 = to_offset8(data_sz);
    const slotid_t org_slots = nslots();
    vector<slotid_t> ghost_slots;
    slotid_t new_slots = 0;
    for (slotid_t i = 0; i < org_slots + 1; i++) {//+1 for popping
        if (i == popped)  continue;         // ignore this slot for now
        slotid_t slot = i;
        if (i == org_slots) {
            //  Move specified slot last
            if (popped < 0) {
                break;
            }
            slot = popped;
        }
        slot_offset8_t offset8;
        poor_man_key poormkey;
        btree_page_h::tuple_both(slot, offset8, poormkey);
        if (offset8 < 0) {
            // ghost record. reclaim it
            w_assert1(slot >= 1); // fence record can't be ghost
            ghost_slots.push_back (slot - 1);
            continue;
        }
        w_assert1(offset8 != 0);
        slot_length_t len = (i == 0 ? get_fence_rec_size() : get_rec_size(slot - 1));
        new_offset8 -= to_aligned_offset8(len);
        ::memcpy(scratch.data_addr8(new_offset8), page()->data_addr8(offset8), len);

        slot_offset8_t* offset8_p = reinterpret_cast<slot_offset8_t*>(scratch.data + slot_sz * new_slots);
        poor_man_key* poormkey_p = reinterpret_cast<poor_man_key*>(scratch.data + slot_sz * new_slots + sizeof(slot_offset8_t));
        *offset8_p = new_offset8;
        *poormkey_p = poormkey;

        ++new_slots;
    }

    scratch.nslots = new_slots;
    scratch.nghosts = 0;
    scratch.record_head8 = new_offset8;

    // defrag doesn't need log if there were no ghost records
    if (ghost_slots.size() > 0) {
        // ghost records are not supported in other types.
        // REDO/UNDO of ghosting relies on BTree
        w_assert0(tag() == t_btree_p);
        W_DO (log_btree_ghost_reclaim(*this, ghost_slots));
    }
    
    // okay, apply the change!
    ::memcpy(_pp, scratch_raw, sizeof(generic_page));
    set_dirty();

    w_assert3 (_is_consistent_space());
    return RCOK;
}

bool btree_page_h::check_space_for_insert(size_t rec_size) {
    size_t contiguous_free_space = usable_space();
    return contiguous_free_space >= align(rec_size) + slot_sz;
}
