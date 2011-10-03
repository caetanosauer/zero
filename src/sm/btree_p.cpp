#include "w_defines.h"

#define SM_SOURCE
#define BTREE_C

#ifdef __GNUG__
#       pragma implementation "btree_p.h"
#endif

#include "sm_int_2.h"

#include "vec_t.h"
#include "btree_p.h"
#include "btree_impl.h"
#include "sm_du_stats.h"
#include "crash.h"
#include "w_key.h"
#include <string>

rc_t btree_p::init_fix_steal(
    const lpid_t& pid,
    shpid_t                root, 
    int                       l,
    shpid_t                pid0,
    shpid_t                blink,
    const w_keystr_t&    fence_low,
    const w_keystr_t&    fence_high,
    const w_keystr_t&    chain_fence_high,
    btree_p*             steal_src,
    int                  steal_from,
    int                  steal_to,
    bool                 log_it)
{
    FUNC(btree_p::init_fix_steal);
    INC_TSTAT(btree_p_fix_cnt);
    store_flag_t store_flags = st_regular;
    W_DO(_fix_core(false, pid, t_btree_p, LATCH_EX, t_virgin, store_flags, false, _refbit));
    W_DO(format_steal(pid, root, l, pid0, blink, fence_low, fence_high, chain_fence_high, log_it, steal_src, steal_from, steal_to));
    return RCOK;
}

rc_t btree_p::format_steal(
    const lpid_t& pid,
    shpid_t                root, 
    int                       l,
    shpid_t                pid0,
    shpid_t                blink,
    const w_keystr_t&    fence_low,
    const w_keystr_t&    fence_high,
    const w_keystr_t&    chain_fence_high,
    bool                 log_it,
    btree_p*             steal_src1,
    int                  steal_from1,
    int                  steal_to1,
    btree_p*             steal_src2,
    int                  steal_from2,
    int                  steal_to2,
    bool                 steal_src2_pid0
    )
{
    w_assert1 (l == 1 || pid0 != 0); // all interemediate node should have pid0 at least initially

    //first, call page_p::_format to nuke the page
    W_DO( page_p::_format(pid, t_btree_p, t_virgin, st_regular) ); // this doesn't log

    _pp->btree_root = root;
    _pp->btree_pid0 = pid0;
    _pp->btree_level = l;
    _pp->btree_blink = blink;
    _pp->btree_fence_low_length = (int16_t) fence_low.get_length_as_keystr();
    _pp->btree_fence_high_length = (int16_t) fence_high.get_length_as_keystr();
    _pp->btree_chain_fence_high_length = (int16_t) chain_fence_high.get_length_as_keystr();

    size_t prefix_len = fence_low.common_leading_bytes(fence_high);
    w_assert1(prefix_len <= fence_low.get_length_as_keystr() && prefix_len <= fence_high.get_length_as_keystr());
    w_assert1(prefix_len <= (1<<15));
    _pp->btree_prefix_length = (int16_t) prefix_len;

    // set fence keys in first slot
    cvec_t fences;
    fences.put (fence_low).put (fence_high).put(chain_fence_high);
    append_nolog(fences, false);

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
        int16_t key_len = steal_src2->get_fence_low_length();
        shpid_t stolen_pid0 = steal_src2->pid0();
        v.put(&key_len, sizeof(int16_t));
        v.put(&stolen_pid0, sizeof(shpid_t));
        v.put(steal_src2->get_fence_low_key() + prefix_len, steal_src2->get_fence_low_length() - prefix_len);
        append_nolog(v, false);
    }
    if (steal_src2) {
        _steal_records (steal_src2, steal_from2, steal_to2);
    }

    // log as one record
    if (log_it) {
        W_DO(log_page_img_format(*this));
    }
    
    return RCOK;
}

void btree_p::_steal_records(
    btree_p*             steal_src,
    int                  steal_from,
    int                  steal_to)
{
    w_assert2(steal_src);
    w_assert2(steal_from <= steal_to);
    w_assert2(steal_from >= 0);
    w_assert2(steal_to <= steal_src->nrecs());
    size_t src_prefix_len = steal_src->get_prefix_length();
    int prefix_len_diff = (int) get_prefix_length() - (int) src_prefix_len; // how many bytes do we reduce in the new page?
    const char* src_prefix_diff = steal_src->get_prefix_key() + src_prefix_len;
    if (prefix_len_diff < 0) { // then we have to increase
        src_prefix_diff += prefix_len_diff;
    }
    for (int i = steal_from; i < steal_to; ++i) {
        slotid_t slot = i + 1;// +1 because it's page_p

        const char* rec = (const char*) steal_src->tuple_addr(slot);
        size_t rec_len = steal_src->tuple_size(slot);
        cvec_t v;
        if (is_leaf()) {
            v.put(rec, 2 * sizeof(int16_t)); // klen/elen is anyway without considering prefix
            if (prefix_len_diff < 0) {
                v.put(src_prefix_diff, -prefix_len_diff);
                v.put(rec + 2 * sizeof(int16_t), rec_len - 2 * sizeof(int16_t));
            } else {
                v.put(rec + 2 * sizeof(int16_t) + prefix_len_diff, rec_len - 2 * sizeof(int16_t) - prefix_len_diff);
            }
        } else {
            v.put(rec, sizeof(int16_t) + sizeof(shpid_t));
            if (prefix_len_diff < 0) {
                v.put(src_prefix_diff, -prefix_len_diff);
                v.put(rec + sizeof(int16_t) + sizeof(shpid_t), rec_len - sizeof(int16_t) - sizeof(shpid_t));
            } else {
                v.put(rec + sizeof(int16_t) + sizeof(shpid_t) + prefix_len_diff, rec_len - sizeof(int16_t) - sizeof(shpid_t) - prefix_len_diff);
            }
        }

        append_nolog(v, steal_src->is_ghost_record(slot));
        w_assert5(_is_consistent_keyorder());
    }
}
rc_t btree_p::norecord_split (shpid_t blink,
    const w_keystr_t& fence_high, const w_keystr_t& chain_fence_high,
    bool log_it)
{
    w_assert1(compare_with_fence_low(fence_high) > 0);
    w_assert1(compare_with_fence_low(chain_fence_high) > 0);
    if (log_it) {
        W_DO(log_btree_blink_norecord_split (*this, blink, fence_high, chain_fence_high));
    }

    w_keystr_t fence_low;
    copy_fence_low_key(fence_low);
    int16_t new_prefix_len = fence_low.common_leading_bytes(fence_high);

    if (new_prefix_len > get_prefix_length() + 3) { // this +3 is arbitrary
        // then, let's defrag this page to compress keys
        page_s scratch;
        ::memcpy (&scratch, _pp, sizeof(scratch));
        btree_p scratch_p (&scratch, 0);
        W_DO(format_steal(scratch_p.pid(), scratch_p.btree_root(), scratch_p.level(), scratch_p.pid0(),
            blink,
            fence_low, fence_high, chain_fence_high,
            false, // don't log it
            &scratch_p, 0, scratch_p.nrecs()
        ));
        set_lsns(scratch.lsn); // format_steal() also clears lsn, so recover it from the copied page
    } else {
        // otherwise, just sets the fence keys and headers
        //sets new fence
        cvec_t fences;
        fences.put (fence_low).put (fence_high).put(chain_fence_high);
        rc_t rc = replace_expand_nolog(0, fences);
        w_assert1(rc.err_num() != eRECWONTFIT);// then why it passed check_chance_for_norecord_split()?
        w_assert1(!rc.is_error());

        //updates headers
        _pp->btree_blink = blink;
        _pp->btree_fence_high_length = (int16_t) fence_high.get_length_as_keystr();
        _pp->btree_chain_fence_high_length = (int16_t) chain_fence_high.get_length_as_keystr();
        _pp->btree_consecutive_skewed_insertions = 0; // reset this value too.
    }
    return RCOK;
}

rc_t btree_p::clear_blink()
{
    // note that we don't have to change the chain-high fence key.
    // we just leave it there, and update the length only.
    // chain-high-fence key is placed after low/high, so it doesn't matter.
    W_DO(log_btree_header(*this, pid0(), level(), 0, // blink=0
            0 // chain-high fence key is disabled
            )); // log first
    _pp->btree_blink = 0;
    _pp->btree_chain_fence_high_length = 0;
    return RCOK;
}

void
btree_p::search(
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
void btree_p::search_leaf(
    const char *key_raw, size_t key_raw_len,
    bool& found_key, slotid_t& ret_slot
) const
{
    w_assert3(is_leaf());
    FUNC(btree_p::_search_leaf);
    
    w_assert1((uint) get_prefix_length() <= key_raw_len);
    w_assert1(::memcmp (key_raw, get_prefix_key(), get_prefix_length()) == 0);
    const void *key_noprefix = key_raw + get_prefix_length();
    size_t key_len = key_raw_len - get_prefix_length();
    
    found_key = false;

    // check the last record to speed-up sorted insert
    int last_slot = nrecs() - 1;
    if (last_slot >= 0) {
        size_t curkey_len;
        const char *curkey = _leaf_key_noprefix(last_slot, curkey_len);
        int d = w_keystr_t::compare_bin_str(curkey, curkey_len, key_noprefix, key_len);
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

        size_t curkey_len;
        const char *curkey = _leaf_key_noprefix(mi, curkey_len);
        int d = w_keystr_t::compare_bin_str(curkey, curkey_len, key_noprefix, key_len);
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

void btree_p::search_node(
    const w_keystr_t&             key,
    slotid_t&             ret_slot
) const
{
    w_assert3(!is_leaf());
    FUNC(btree_p::_search_node);

    const char *key_raw = (const char *) key.buffer_as_keystr();
    w_assert1((uint) get_prefix_length() <= key.get_length_as_keystr());
    w_assert1(::memcmp (key_raw, get_prefix_key(), get_prefix_length()) == 0);
    const void *key_noprefix = key_raw + get_prefix_length();
    size_t key_len = key.get_length_as_keystr() - get_prefix_length();

    w_assert1 (pid0() != 0);
    bool return_pid0 = false;
    if (nrecs() == 0) {
        return_pid0 = true;
    } else  {
        size_t curkey_len;
        const char *curkey = _node_key_noprefix(0, curkey_len);
        int d = w_keystr_t::compare_bin_str(curkey, curkey_len, key_noprefix, key_len);
        // separator key is exclusive for left, so it's ">"
        return_pid0 = (d > 0);
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
        size_t curkey_len;
        const char *curkey = _node_key_noprefix(last_slot, curkey_len);
        int d = w_keystr_t::compare_bin_str(curkey, curkey_len, key_noprefix, key_len);
        if (d <= 0) {
            ret_slot = last_slot;
            return;
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

        size_t curkey_len;
        const char *curkey = _node_key_noprefix(mi, curkey_len);
        int d = w_keystr_t::compare_bin_str(curkey, curkey_len, key_noprefix, key_len);
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

void btree_p::_update_btree_consecutive_skewed_insertions(slotid_t slot)
{
    if (nrecs() == 0) {
        return;
    }
    int16_t val = _pp->btree_consecutive_skewed_insertions;
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
    _pp->btree_consecutive_skewed_insertions = val;
}

rc_t btree_p::insert_node(const w_keystr_t &key, slotid_t slot, shpid_t child)
{
    FUNC(btree_p::insert);
    
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

    vec_t v; // the record data
    int16_t klen = key.get_length_as_keystr();

    // prefix of the key is fixed in this page, so we can simply
    // peel off leading bytes from the key+el.
    int16_t prefix_length = get_prefix_length();  // length of prefix of inserted tuple
    w_assert3(prefix_length <= klen);

    // see the record format in btree_p class comments.
    v.put(&klen, sizeof(klen)); // because we do this, we have to hold the variable "klen" until the end of this function!
    v.put(&child, sizeof(child));
    v.put(key, prefix_length, klen - prefix_length);

    W_DO( page_p::insert_expand_nolog(slot + 1, v) ); // we don't log it. btree_impl::adopt() does the logging

    w_assert3 (is_consistent(true, false));
    w_assert5 (is_consistent(true, true));

    return RCOK;
}
bool btree_p::_is_enough_spacious_ghost(
    const w_keystr_t &key, slotid_t slot,
    const cvec_t&        el)
{
    w_assert2(is_leaf());
    w_assert2(is_ghost_record(slot + 1));
    size_t rec_size = calculate_rec_size(key, el);
    return (align(tuple_size(slot + 1)) >= rec_size);
}

rc_t btree_p::replace_ghost(
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
    w_assert1 (is_ghost_record(slot + 1));
    int16_t rec_size = calculate_rec_size(key, elem);
    int16_t org_rec_size = tuple_size(slot + 1);
    if (align(rec_size) > align(org_rec_size)) {
        expand_rec (slot + 1, rec_size);
    }
    
    w_assert1 (_is_enough_spacious_ghost(key, slot, elem));
#if W_DEBUG_LEVEL > 2
    btrec_t rec (*this, slot);
    w_assert3 (rec.key().compare(key) == 0);
#endif // W_DEBUG_LEVEL > 2

    int16_t klen = key.get_length_as_keystr();
    int16_t prefix_length = get_prefix_length();
    int16_t elen = elem.size();

    char *buf = (char*) tuple_addr(slot + 1);
    // klen should be same
    w_assert1(reinterpret_cast<int16_t*>(buf)[0] == klen);
    if (elen != org_rec_size) {
        // update only when necessary
        ::memcpy (buf + sizeof(int16_t), &(elen), sizeof(int16_t));
    }
    w_assert1(reinterpret_cast<int16_t*>(buf)[1] == elen);
    elem.copy_to(buf + sizeof(int16_t) * 2 + klen - prefix_length);

    // Reuse everything. just change the record data.
    slot_offset_t offset = tuple_offset(slot + 1);
    w_assert1 (offset < 0); // it should be ghost
    
    overwrite_slot (slot + 1, -offset, rec_size);
    return RCOK;
}

rc_t btree_p::replace_el_nolog(slotid_t slot, const cvec_t &elem)
{
    w_assert2( is_fixed());
    w_assert2( is_leaf());

    w_assert1 (!is_ghost_record(slot + 1));
    
    char *buf = (char*) tuple_addr(slot + 1);
    int16_t klen = reinterpret_cast<int16_t*>(buf)[0];
    int16_t prefix_length = get_prefix_length();

    // do we need to expand?
    int16_t rec_size = sizeof(int16_t) * 2 + klen - prefix_length + elem.size();
    int16_t org_rec_size = tuple_size(slot + 1);
    if (align(rec_size) > align(org_rec_size)) {
        if (!check_space_for_insert(rec_size)) {
            return RC(eRECWONTFIT);
        }
        expand_rec (slot + 1, rec_size);
        w_assert1(tuple_addr(slot + 1) != buf);
        buf = (char *) tuple_addr(slot + 1);
        w_assert1(reinterpret_cast<int16_t*>(buf)[0] == klen);
    }

    reinterpret_cast<int16_t*>(buf)[1] = elem.size();
    elem.copy_to(buf + sizeof(int16_t) * 2 + klen - prefix_length);
    return RCOK;
}

void btree_p::overwrite_el_nolog(slotid_t slot, smsize_t offset,
                                    const char *new_el, smsize_t elen)
{
    w_assert2( is_fixed());
    w_assert2( is_leaf());
    w_assert1 (!is_ghost_record(slot + 1));
    
    char *buf = (char*) tuple_addr(slot + 1);
    int16_t klen = reinterpret_cast<int16_t*>(buf)[0];
    int16_t prefix_length = get_prefix_length();

    w_assert1 (tuple_size(slot + 1) >= offset + elen + sizeof(int16_t) * 2 + klen - prefix_length);

    ::memcpy (buf + sizeof(int16_t) * 2 + klen - prefix_length + offset, new_el, elen);
}

void btree_p::reserve_ghost(const char *key_raw, size_t key_raw_len, int record_size)
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
    
    if (slot != nrecs())    {
        // note that slot=0 is fence
        ::memmove(_pp->data + slot_sz * (slot + 2),
                _pp->data + slot_sz * (slot + 1),
                (nrecs() - slot) * slot_sz);
    }
    slot_offset_t new_record_head = _pp->record_head - align(record_size);
    overwrite_slot(slot + 1, -new_record_head, record_size); // ghost record

    // make a dummy record that has the desired length
    int16_t klen = key_raw_len;
    int16_t prefix_len = get_prefix_length();
    int16_t elen = record_size - sizeof(int16_t) * 2 - klen + prefix_len;

    char *buf = _pp->data + new_record_head;
    ::memcpy (buf, &(klen), sizeof(int16_t)); buf += sizeof(int16_t);
    ::memcpy (buf, &(elen), sizeof(int16_t)); buf += sizeof(int16_t);
    ::memcpy (buf, key_raw + prefix_len, klen - prefix_len);
    // that's it. doesn't have to write data. do nothing.

    _pp->record_head = new_record_head;
    ++_pp->nslots;
    ++_pp->nghosts;
    
    w_assert3(is_consistent_space());
}

bool btree_p::check_space_for_insert_leaf(
    const w_keystr_t&     key,
    const cvec_t&     el)
{
    w_assert1 (is_leaf());
    size_t rec_size = 2 * sizeof(int16_t) + key.get_length_as_keystr() + el.size();
    return page_p::check_space_for_insert (rec_size);
}
bool btree_p::check_space_for_insert_node(const w_keystr_t&     key)
{
    w_assert1 (is_node());
    size_t rec_size = sizeof(int16_t) + key.get_length_as_keystr() + sizeof (shpid_t);
    return page_p::check_space_for_insert (rec_size);
}

bool btree_p::check_chance_for_norecord_split(const w_keystr_t& key_to_insert) const
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
    // we need some space for updated fence-high and chain-high
    smsize_t space_for_split = get_fence_low_length();
    w_keystr_t lastkey;
    if (is_leaf()) {
        leaf_key(nrecs() - 1, lastkey);
        size_t common_bytes = key_to_insert.common_leading_bytes(lastkey);
        space_for_split += common_bytes + 1; //this will be fence-high
    } else {
        node_key(nrecs() - 1, lastkey);
        space_for_split += key_to_insert.get_length_as_keystr();
    }
    if (key_to_insert.compare(lastkey) <= 0) {
        return false; // not hitting highest. norecord-split will be useless
    }
    if (get_chain_fence_high_length() == 0) {
        space_for_split += get_fence_high_length(); // newly set chain-high
    } else {
        space_for_split += get_chain_fence_high_length(); // otherwise chain-fence-high is unchanged
    }
    return (usable_space() >= align(space_for_split)); // otherwise it's too late
}

void btree_p::suggest_fence_for_split(
    w_keystr_t &mid,
    slotid_t& right_begins_from,
    const w_keystr_t &triggering_key) const
{
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

w_keystr_t btree_p::recalculate_fence_for_split(slotid_t right_begins_from) const {
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


void btree_p::rec_leaf(slotid_t idx,  w_keystr_t &key, cvec_t &el, bool &ghost) const
{
    w_assert1(is_leaf());
    FUNC(btree_p::rec_leaf);
    ghost = page_p::is_ghost_record(idx + 1);
    const char* base = (char*) page_p::tuple_addr(idx + 1);
    const char* p = base;
    
    el.reset();

    int16_t key_len = ((int16_t*) p)[0];
    int16_t el_len = ((int16_t*) p)[1];
    p += sizeof(int16_t) * 2;
    
    int16_t prefix_len = get_prefix_length();
    w_assert2 (prefix_len <= key_len);
    key.construct_from_keystr(get_prefix_key(), prefix_len, p, key_len - prefix_len); // also from p
    el.put(p + key_len - prefix_len, el_len);
}
void btree_p::rec_leaf(slotid_t idx,  w_keystr_t &key, char *el, smsize_t &elen, bool &ghost) const
{
    w_assert1(is_leaf());
    FUNC(btree_p::rec_leaf);
    ghost = page_p::is_ghost_record(idx + 1);
    const char* base = (char*) page_p::tuple_addr(idx + 1);
    const char* p = base;
    
    int16_t key_len = ((int16_t*) p)[0];
    int16_t el_len = ((int16_t*) p)[1];
    w_assert1(elen >= (smsize_t) el_len); // this method assumes the buffer is large enough!
    p += sizeof(int16_t) * 2;
    
    int16_t prefix_len = get_prefix_length();
    w_assert2 (prefix_len <= key_len);
    key.construct_from_keystr(get_prefix_key(), prefix_len, p, key_len - prefix_len); // also from p
    ::memcpy(el, p + key_len - prefix_len, el_len);
    elen = el_len;
}
bool btree_p::dat_leaf(slotid_t idx,  char *el, smsize_t &elen, bool &ghost) const
{
    w_assert1(is_leaf());
    FUNC(btree_p::dat_leaf);
    ghost = page_p::is_ghost_record(idx + 1);
    const char* base = (char*) page_p::tuple_addr(idx + 1);
    const char* p = base;
    
    int16_t key_len = ((int16_t*) p)[0];
    int16_t el_len = ((int16_t*) p)[1];
    p += sizeof(int16_t) * 2;
    
    int16_t prefix_len = get_prefix_length();
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

void btree_p::dat_leaf_ref(slotid_t idx, const char *&el, smsize_t &elen, bool &ghost) const
{
    w_assert1(is_leaf());
    FUNC(btree_p::dat_leaf_ref);
    ghost = page_p::is_ghost_record(idx + 1);
    const char* base = (char*) page_p::tuple_addr(idx + 1);
    const char* p = base;
    
    int16_t key_len = ((int16_t*) p)[0];
    elen = ((int16_t*) p)[1];
    p += sizeof(int16_t) * 2;
    
    int16_t prefix_len = get_prefix_length();
    w_assert2 (prefix_len <= key_len);
    el = p + key_len - prefix_len;
}

void btree_p::leaf_key(slotid_t idx,  w_keystr_t &key) const
{
    w_assert1(is_leaf());
    const char* p = (char*) page_p::tuple_addr(idx + 1);
    int16_t key_len = ((int16_t*) p)[0];
    p += sizeof(int16_t) * 2;
    int16_t prefix_len = get_prefix_length();
    w_assert2 (prefix_len <= key_len);
    key.construct_from_keystr(get_prefix_key(), prefix_len, p, key_len - prefix_len); // also from p
}
const char* btree_p::_leaf_key_noprefix(slotid_t idx,  size_t &len) const {
    w_assert1(is_leaf());
    const char* base = (char*) page_p::tuple_addr(idx + 1);
    len = *((int16_t*) base) - get_prefix_length();
    return base + sizeof(int16_t) * 2;    
}

void  btree_p::rec_node(slotid_t idx,  w_keystr_t &key, shpid_t &el) const
{
    w_assert1(is_node());
    FUNC(btree_p::rec_node);
    w_assert1(!is_ghost_record(idx + 1)); // non-leaf node can't be ghost
    const char* p = (const char*) page_p::tuple_addr(idx + 1);
    
    el = 0;
    
    int16_t key_len = *((int16_t*) p);
    p += sizeof(int16_t);
    // el = *((shpid_t*) p); this causes Bus Error on solaris!
    ::memcpy(&el, p, sizeof(shpid_t));
    p += sizeof(shpid_t);
    
    int16_t prefix_len = get_prefix_length();
    w_assert2 (prefix_len <= key_len);
    key.construct_from_keystr(get_prefix_key(), prefix_len, p, key_len - prefix_len); // also from p
}
const char* btree_p::_node_key_noprefix(slotid_t idx,  size_t &len) const {
    w_assert1(is_node());
    const char* base = (const char*) page_p::tuple_addr(idx + 1);    
    len = *((int16_t*) base) - get_prefix_length();
    return base + sizeof(int16_t) + sizeof(shpid_t);
}
void btree_p::node_key(slotid_t idx,  w_keystr_t &key) const
{
    w_assert1(is_node());
    const char* p = (char*) page_p::tuple_addr(idx + 1);
    int16_t key_len = ((int16_t*) p)[0];
    p += sizeof(int16_t) + sizeof(shpid_t);
    int16_t prefix_len = get_prefix_length();
    w_assert2 (prefix_len <= key_len);
    key.construct_from_keystr(get_prefix_key(), prefix_len, p, key_len - prefix_len); // also from p
}

rc_t
btree_p::leaf_stats(btree_lf_stats_t& _stats)
{

    _stats.hdr_bs += (hdr_sz + slot_sz + 
             align(page_p::tuple_size(0)));
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
btree_p::int_stats(btree_int_stats_t& _stats)
{
    _stats.unused_bs += usable_space();
    _stats.used_bs += used_space();
    return RCOK;
}

btrec_t& 
btrec_t::set(const btree_p& page, slotid_t slot)
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
btree_p::overhead_requirement_per_entry =
            4 // for the key length (in btree_p)
            +
            sizeof(shpid_t) // for the interior nodes (in btree_p)
            ;

smsize_t         
btree_p::max_entry_size = // must be able to fit 2 entries to a page
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
btree_p::print(
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

bool btree_p::is_consistent (bool check_keyorder, bool check_space) const
{
    // does NOT check check-sum. the check can be done only by bufferpool
    // with seeing fresh data from the disk.

    // additionally check key-sortedness and uniqueness
    if (check_keyorder) {
        if (!_is_consistent_keyorder()) {
            w_assert1(false);
            return false;
        }
    }

    // additionally check record overlaps
    if (check_space) {
        if (!page_p::is_consistent_space()) {
            w_assert1(false);
            return false;
        }
    }

    return true;
}
bool btree_p::_is_consistent_keyorder () const
{
    const int recs = nrecs();
    const char* lowkey = get_fence_low_key();
    const size_t lowkey_len = get_fence_low_length();
    const char* highkey = get_fence_high_key();
    const size_t highkey_len = get_fence_high_length();
    const size_t prefix_len = get_prefix_length();
    if (recs == 0) {
        // then just compare low-high and quit
        if (w_keystr_t::compare_bin_str(lowkey, lowkey_len, highkey, highkey_len) >= 0) {
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
        if (w_keystr_t::compare_bin_str(prevkey, prevkey_len, highkey + prefix_len, highkey_len - prefix_len) > 0) {
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
        
        if (w_keystr_t::compare_bin_str(prevkey, prevkey_len, highkey + prefix_len, highkey_len - prefix_len) > 0) {
            w_assert3(false);
            return false;
        }
    }
    
    return true;
}
