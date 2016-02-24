/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/**
 * Implementation of verification related functions in btree_impl.h.
 */

#define SM_SOURCE
#define BTREE_C

#include "sm_base.h"
#include "btree_page_h.h"
#include "btree_impl.h"

// these are for volume-wide verifications
#include "sm.h"
#include "vol.h"
#include "xct.h"
#include "sm_base.h"
#include "bf_tree.h"

// NOTE we don't know the level of root until we start, so just give "-1" as magic value for root level
const int16_t NOCHECK_ROOT_LEVEL = -1;

rc_t  btree_impl::_ux_verify_tree(
        StoreID store, int hash_bits, bool &consistent)
{
    verification_context context (hash_bits);

    // add expectation for root node which is always infimum-supremum
    // though the supremum might appear in its foster page
    w_keystr_t infimum, supremum;
    infimum.construct_neginfkey();
    supremum.construct_posinfkey();
    btree_page_h rp;
    W_DO( rp.fix_root(store, LATCH_SH));
    context.add_expectation(rp.pid(), NOCHECK_ROOT_LEVEL, false, infimum);
    context.add_expectation(rp.pid(), NOCHECK_ROOT_LEVEL, true, supremum);
    W_DO (_ux_verify_tree_recurse(rp, context));
    rp.unfix();
    consistent = context.is_bitmap_clean();
    if (context._pages_inconsistent != 0) {
        consistent = false;
    }
    return RCOK;
}
rc_t btree_impl::_ux_verify_tree_recurse(
        btree_page_h &parent, verification_context &context)
{
    PageID org_pid = parent.pid();

    w_assert1(parent.is_latched());
    // feed this page itself
    _ux_verify_feed_page (parent, context);

    // recurse on real children first.
    if (parent.is_node()) {
        for (slotid_t slot = -1; slot < parent.nrecs(); ++slot) {
            btree_page_h child;
            PageID child_pid_opaqueptr = slot == -1 ? parent.pid0_opaqueptr() : parent.child_opaqueptr(slot);
            W_DO(child.fix_nonroot(parent, child_pid_opaqueptr, LATCH_SH));
            W_DO(_ux_verify_tree_recurse (child, context));
        }
    }

    // recursing on real children shouldn't change the current page
    w_assert1(parent.is_fixed());
    w_assert1(parent.pid() == org_pid);

    // also check right foster sibling.
    // unfix the current page after latch coupling
    // to prevent the stack from growing too long if there is a long foster chain

    if (parent.get_foster() != 0) {
        btree_page_h child;
        W_DO(child.fix_nonroot(parent, parent.get_foster_opaqueptr(), LATCH_SH));
        parent.unfix();
        W_DO(_ux_verify_tree_recurse (child, context));
    }

    return RCOK;
}

rc_t btree_impl::_ux_verify_feed_page(
    btree_page_h &page, verification_context &context)
{
    ++ context._pages_checked;
#if W_DEBUG_LEVEL > 1
    if (context._pages_checked % 100 == 0) {
        cout << "verify: checking " << context._pages_checked << "th page.." << endl;
    }
#endif // W_DEBUG_LEVEL
    // do in_page verification
    bool consistent = page.is_consistent(true, true);
    if (!consistent) {
        ++context._pages_inconsistent;
#if W_DEBUG_LEVEL > 1
        cout << "verify: found an in-page inconsistency in page:" << page.pid() << endl;
#endif // W_DEBUG_LEVEL
    }

    // submit low-fence fact of this page
    int16_t fact_level = page.level();
    if (page.pid() == page.root()) {
        fact_level = NOCHECK_ROOT_LEVEL; // this avoids root level check (see _ux_verify_tree)
    }
    context.add_fact(page.pid(), fact_level, false,
            page.get_fence_low_length(), page.get_fence_low_key());

    // then submit high-fence fact of this page, but
    if (page.get_foster()) {
        // in this case, we should submit chain-high-fence
        context.add_fact(page.pid(), fact_level, true,
            page.get_chain_fence_high_length(), page.get_chain_fence_high_key());
    } else {
        // if this is right-most, the high-fence key is actually the high-fence
        context.add_fact(page.pid(), fact_level, true,
            page.get_prefix_length(), page.get_prefix_key(),
            page.get_fence_high_length_noprefix(), page.get_fence_high_key_noprefix());
    }

    // add expectation on children
    if (page.is_node()) {
        // check children. add expectation on them
        PageID pid_next = page.pid();
        pid_next = 0;
        int16_t child_level = page.level() - 1;

        w_keystr_t key;

        w_assert1(page.pid0());
        pid_next = page.pid0();
        context.add_expectation (page.pid0(), child_level, false,
                page.get_fence_low_length(), page.get_fence_low_key());
        if (page.nrecs() == 0) {
            context.add_expectation (page.pid0(), child_level, true,
                page.get_prefix_length(), page.get_prefix_key(),
                page.get_fence_high_length_noprefix(), page.get_fence_high_key_noprefix());
        } else {
            // the first separator key will be high of pid0, low of child(0)
            page.get_key(0, key);
            context.add_expectation (page.pid0(), child_level, true, key);
            context.add_expectation (page.child(0), child_level, false, key);
        }

        for (slotid_t slot = 0; slot < page.nrecs(); ++slot) {
            pid_next = page.child(slot);
            if (slot + 1 < page.nrecs()) {
                // *next* separator key will be this slot's high and next slot's low
                page.get_key(slot + 1, key);
                context.add_expectation (pid_next, child_level, true, key);
                context.add_expectation (page.child(slot + 1), child_level, false, key);
            } else {
                //last child's high = parent's high
                context.add_expectation (pid_next, child_level, true,
                    page.get_prefix_length(), page.get_prefix_key(),
                    page.get_fence_high_length_noprefix(), page.get_fence_high_key_noprefix());
            }
        }
    }

    // add expectation on foster right sibling ("next" page).
    if (page.get_foster()) {
        // low-fence of next page should be high-fence of this page
        context.add_expectation (page.get_foster(), page.level(), false,
                page.get_prefix_length(), page.get_prefix_key(),
                page.get_fence_high_length_noprefix(), page.get_fence_high_key_noprefix());
        context.add_expectation (page.get_foster(), page.level(), true,
                page.get_chain_fence_high_length(), page.get_chain_fence_high_key());
    }
    return RCOK;
}


void btree_impl::inquery_verify_init(StoreID store)
{
    xct_t *x = xct();
    if (x == NULL || !x->is_inquery_verify())
        return;
    inquery_verify_context_t &context = x->inquery_verify_context();
    context.next_level = -1; // don't check level of root page
    context.next_low_key.construct_neginfkey();
    context.next_high_key.construct_posinfkey();
    context.next_pid = smlevel_0::bf->get_root_page_id(store);
}

void btree_impl::inquery_verify_fact(btree_page_h &page)
{
    xct_t *x = xct();
    if (x == NULL || !x->is_inquery_verify())
        return;
    inquery_verify_context_t &context = x->inquery_verify_context();
    if (context.pids_inconsistent.find(page.pid()) != context.pids_inconsistent.end()) {
        return;
    }
    ++context.pages_checked;
    bool inconsistent = false;
    if (!page.is_consistent(x->is_inquery_verify_keyorder(), x->is_inquery_verify_space())) {
        inconsistent = true;
    }
    if (context.next_level != -1 && context.next_level != page.level()) {
        inconsistent = true;
    }
    if (context.next_pid != page.pid()) {
        inconsistent = true;
    }

    // check low-fence fact of this page
    if (page.compare_with_fence_low(context.next_low_key) != 0) {
        inconsistent = true;
    }

    // check high-fence fact of this page, but
    if (page.get_foster()) {
        // in this case, we should check chain-high-fence
        if (page.compare_with_chain_fence_high(context.next_high_key) != 0) {
            inconsistent = true;
        }
    } else {
        // if this is right-most, the high-fence key is actually the high-fence
        if (page.compare_with_fence_high(context.next_high_key) != 0) {
            inconsistent = true;
        }
    }

    if (inconsistent) {
#if W_DEBUG_LEVEL>0
        cerr << "found an inconsistent page by in-query verification!! " << page.pid() << endl;
#endif // W_DEBUG_LEVEL>0
        context.pids_inconsistent.insert(page.pid());
    }
}
void btree_impl::inquery_verify_expect(btree_page_h &page, slot_follow_t next_follow)
{
    xct_t *x = xct();
    if (x == NULL || !x->is_inquery_verify())
        return;
    inquery_verify_context_t &context = x->inquery_verify_context();

    w_assert1(next_follow > t_follow_invalid && next_follow < page.nrecs());
    if (next_follow == t_follow_foster) {
        context.next_level = page.level();
        context.next_pid = page.get_foster();
        page.copy_fence_high_key(context.next_low_key);
        page.copy_chain_fence_high_key(context.next_high_key);
    } else if (next_follow == t_follow_pid0) {
        context.next_level = page.level() - 1;
        context.next_pid = page.pid0();
        page.copy_fence_low_key(context.next_low_key);
        if (page.nrecs() == 0) {
            page.copy_fence_high_key(context.next_high_key);
        } else {
            page.get_key(0, context.next_high_key);
        }
    } else {
        context.next_level = page.level() - 1;
        context.next_pid = page.child(next_follow);
        page.get_key(next_follow, context.next_low_key);
        if (next_follow + 1 == page.nrecs()) {
            page.copy_fence_high_key(context.next_high_key);
        } else {
            page.get_key(next_follow + 1, context.next_high_key);
        }
    }
}

verification_context::verification_context (int hash_bits)
    : _hash_bits (hash_bits), _pages_checked(0), _pages_inconsistent(0)
{
    w_assert1 (hash_bits > 3);
    w_assert1 (hash_bits < 32);
    _bitmap_size = 1 << (hash_bits - 3);
    // _bitmap_size should be multiply of 8 to make checking efficient.
    // see is_bitmap_clean() for the optimization
    if ((_bitmap_size & 0x7) != 0) {
        _bitmap_size = ((_bitmap_size >> 3) + 1) << 3;
    }
    w_assert1 ((_bitmap_size & 0x7) == 0);

    _bitmap = new char[_bitmap_size];
    w_assert1 (_bitmap);
    memset (_bitmap, 0, _bitmap_size);

    w_assert3 (is_bitmap_clean());
}

verification_context::~verification_context ()
{
    delete[] _bitmap;
    _bitmap = NULL;
}

void verification_context::add_fact (PageID pid, int16_t level, bool high,
    size_t prefix_len, const char* prefix, size_t suffix_len, const char* suffix)
{
    w_assert1(pid);
    w_assert1(level >= 0 || level == NOCHECK_ROOT_LEVEL);
    uint32_t hash_value = 0;
    hash_value = _modify_hash(pid, hash_value);
    hash_value = _modify_hash(level, hash_value);
    hash_value = _modify_hash(high, hash_value);
    hash_value = _modify_hash(prefix, prefix_len, hash_value);
    if (suffix != NULL) {
        hash_value = _modify_hash(suffix, suffix_len, hash_value);
    }
    uint byte_place = (hash_value >> 3) % (1 << (_hash_bits - 3));
    w_assert1(byte_place < (uint) _bitmap_size);
    uint bit_place = hash_value & 0x7;
    // flip the bit
    uint8_t* byte = reinterpret_cast<uint8_t*> (_bitmap + byte_place);
    *byte ^= (1 << bit_place);
}


void verification_context::add_expectation (
    PageID pid, int16_t level, bool high, size_t prefix_len, const char* prefix, size_t suffix_len, const char* suffix) {
    add_fact (pid, level, high, prefix_len, prefix, suffix_len, suffix);
}

uint32_t verification_context::_modify_hash (
    const char* data, size_t len, uint32_t hash_value)
{
    // simple hashing (as in Java's String.hashcode())
    // for less hash collisions, smarter hashing might be beneficial.
    // but not sure worth the complexity
    char const* const end = data + len;
    for (const char *it = data; it != end; ++it) {
        hash_value = hash_value * 0x6d2ac891 + (uint32_t) (*it);
    }
    return hash_value;
}
uint32_t verification_context::_modify_hash (
    uint32_t data, uint32_t hash_value)
{
    // for integer types, use this.
    return hash_value * 0x6d2ac891 + data;
}

bool verification_context::is_bitmap_clean () const
{
    // because _bitmap_size is multiply of 8, we can assume
    // the bitmap is an array of 8 byte integers.
    // the following check is more efficient than iterating each byte.
    w_assert1 ((_bitmap_size & 0x7) == 0);
    int64_t const* const begin = reinterpret_cast<const int64_t*>(_bitmap);
    int64_t const* const end = reinterpret_cast<const int64_t*>(_bitmap + _bitmap_size);
    // check each 8 bytes
    for (const int64_t *it = begin; it != end; ++it) {
        if (*it != 0) {
            return false;
        }
    }
    return true;
}

rc_t btree_impl::_ux_verify_volume(
    int hash_bits, verify_volume_result &result)
{
    W_DO(ss_m::force_volume()); // this might block if there is a concurrent transaction
    vol_t *vol = ss_m::vol;
    w_assert1(vol);
    generic_page buf;
    PageID endpid = (PageID) (vol->num_used_pages());
    for (PageID pid = vol->first_data_pageid(); pid < endpid; ++pid) {
        // TODO we should skip large chunks of unused areas to speedup.
        // TODO we should scan more than one page at a time to speedup.
        if (!vol->is_allocated_page(pid)) {
            continue;
        }
        W_DO (vol->read_page(pid, &buf));
        btree_page_h page;
        page.fix_nonbufferpool_page(&buf);
        if (page.tag() == t_btree_p && !page.is_to_be_deleted()) {
            // TODO FIX AFTER PID REFACTORING (use root PID instead of store id)
            verification_context *context = result.get_or_create_context(1, hash_bits);
            W_DO (_ux_verify_feed_page (page, *context));

            if (page.pid() == page.root()) {
                // root needs corresponding expectations from outside.
                w_keystr_t infimum, supremum;
                infimum.construct_neginfkey();
                supremum.construct_posinfkey();
                context->add_expectation(page.root(), NOCHECK_ROOT_LEVEL, false, infimum);
                context->add_expectation(page.root(), NOCHECK_ROOT_LEVEL, true, supremum);
            }
        }
    }
    return RCOK;
}

verify_volume_result::verify_volume_result ()
{
}
verify_volume_result::~verify_volume_result()
{
    for (std::map<StoreID, verification_context*>::iterator iter = _results.begin();
         iter != _results.end(); ++iter) {
        verification_context *context = iter->second;
        delete context;
    }
    _results.clear();
}
verification_context*
verify_volume_result::get_or_create_context (
    StoreID store_id, int hash_bits)
{
    verification_context *context = get_context(store_id);
    if (context != NULL) {
        return context;
    } else {
        // this store_id is first seen. let's create
        context = new verification_context (hash_bits);
        _results.insert(std::pair<StoreID, verification_context*>(store_id, context));
        return context;
    }
}
verification_context* verify_volume_result::get_context (StoreID store_id)
{
    std::map<StoreID, verification_context*>::const_iterator iter = _results.find(store_id);
    if (iter != _results.end()) {
        return iter->second;
    } else {
        return NULL;
    }
}
