/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#include "sm_base.h"
#include "bf_tree.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "btcursor.h"
#include "w_key.h"
#include "xct.h"
#include "vec_t.h"
#include "vol.h"
#include "lock.h"

void btree_m::construct_once()
{
    ::memset(btree_impl::s_ex_need_counts, 0, sizeof(btree_impl::s_ex_need_counts));
    ::memset(btree_impl::s_foster_children_counts, 0, sizeof(btree_impl::s_foster_children_counts));
    // initialize mutexes for high contention
    for (int i = 0; i < (1 << btree_impl::GAC_HASH_BITS); ++i) {
        queue_based_lock_t *addr = (btree_impl::s_ex_need_mutex + i);
        new (addr) queue_based_lock_t;
    }
}
void btree_m::destruct_once()
{
    for (int i = 0; i < (1 << btree_impl::GAC_HASH_BITS); ++i) {
        queue_based_lock_t *addr = (btree_impl::s_ex_need_mutex + i);
        addr->~queue_based_lock_t();
    }
}

smsize_t
btree_m::max_entry_size() {
    return btree_page_h::max_entry_size;
}

rc_t
btree_m::create(StoreID& stid)
{
    // CS TODO: page allocation should transfer ownership to stnode
    PageID root;
    W_DO(smlevel_0::vol->create_store(root, stid));

    W_DO(smlevel_0::lm->intent_store_lock(stid, okvl_mode::X)); // take X on this new index
    W_DO(btree_impl::_ux_create_tree_core(stid, root));

    bool empty=false;
    W_DO(is_empty(stid, empty));
    if(!empty) {
         DBGTHRD(<<"eNDXNOTEMPTY");
         return RC(eNDXNOTEMPTY);
    }

    return RCOK;
}

rc_t
btree_m::is_empty(
    StoreID store,
    bool&                 ret)        // O-  true if btree is empty
{
    bt_cursor_t cursor(store, true);
    W_DO( cursor.next());
    ret = cursor.eof();
    return RCOK;
}

rc_t btree_m::insert(StoreID store, const w_keystr_t &key, const cvec_t &el)
{
    W_DO(open_store(store, true));
    if (key.get_length_as_nonkeystr() + el.size() > btree_page_h::max_entry_size) {
        return RC(eRECWONTFIT);
    }
    W_DO(btree_impl::_ux_insert(store, key, el));
    return RCOK;
}

rc_t btree_m::update(
    StoreID store,
    const w_keystr_t&                 key,
    const cvec_t&                     elem)
{
    W_DO(open_store(store, true));
    if(key.get_length_as_nonkeystr() + elem.size() > btree_page_h::max_entry_size) {
        return RC(eRECWONTFIT);
    }
    W_DO(btree_impl::_ux_update(store, key, elem));
    return RCOK;
}

rc_t btree_m::put(
    StoreID store,
    const w_keystr_t&                 key,
    const cvec_t&                     elem)
{
    W_DO(open_store(store, true));
    if(key.get_length_as_nonkeystr() + elem.size() > btree_page_h::max_entry_size) {
        return RC(eRECWONTFIT);
    }
    W_DO(btree_impl::_ux_put(store, key, elem));
    return RCOK;
}

rc_t btree_m::overwrite(
    StoreID store,
    const w_keystr_t&                 key,
    const char*                       el,
    smsize_t                          offset,
    smsize_t                          elen)
{
    W_DO(open_store(store, true));
    W_DO(btree_impl::_ux_overwrite(store, key, el, offset, elen));
    return RCOK;
}

rc_t btree_m::remove(StoreID store, const w_keystr_t &key)
{
    W_DO(open_store(store, true));
    W_DO(btree_impl::_ux_remove(store, key));
    return RCOK;
}

rc_t btree_m::defrag_page(btree_page_h &page)
{
    W_DO( btree_impl::_sx_defrag_page(page));
    return RCOK;
}


rc_t btree_m::lookup(
    StoreID store,
    const w_keystr_t &key, void *el, smsize_t &elen, bool &found)
{
    bool for_update = g_xct_does_ex_lock_for_select();
    W_DO(open_store (store, for_update));

    W_DO( btree_impl::_ux_lookup(store, key, found, el, elen ));
    return RCOK;
}

rc_t btree_m::verify_tree(
        StoreID store, int hash_bits, bool &consistent)
{
    W_DO(open_store(store, true));
    return btree_impl::_ux_verify_tree(store, hash_bits, consistent);
}

rc_t btree_m::verify_volume(
        int hash_bits, verify_volume_result &result)
{
    return btree_impl::_ux_verify_volume(hash_bits, result);
}

rc_t btree_m::touch_all(StoreID stid, uint64_t &page_count)
{
    // no locking -- this method is for debugging
    btree_page_h page;
    W_DO( page.fix_root(stid, LATCH_SH));
    page_count = 0;
    return touch(page, page_count);
}

rc_t btree_m::touch(const btree_page_h& page, uint64_t &page_count) {
    ++page_count;
    if (page.get_foster_opaqueptr() != 0) {
        btree_page_h next;
        W_DO(next.fix_nonroot(page, page.get_foster_opaqueptr(), LATCH_SH));
        W_DO(touch(next, page_count));
    }
    if (page.is_node()) {
        if (page.pid0_opaqueptr())  {
            btree_page_h next;
            W_DO(next.fix_nonroot(page, page.pid0_opaqueptr(), LATCH_SH));
            W_DO(touch(next, page_count));
        }
        for (int i = 0; i < page.nrecs(); ++i) {
            btree_page_h next;
            W_DO(next.fix_nonroot(page, page.child_opaqueptr(i), LATCH_SH));
            W_DO(touch(next, page_count));
        }
    }
    return RCOK;
}

/*
 * for use by logrecs for logical undo of inserts/deletes
 */
rc_t
btree_m::remove_as_undo(StoreID store, const w_keystr_t &key)
{
    // UNDO insert operation
    no_lock_section_t nolock;
    return btree_impl::_ux_remove(store,key);
}

rc_t btree_m::update_as_undo(StoreID store, const w_keystr_t &key, const cvec_t &elem)
{
    // UNDO update operation
    no_lock_section_t nolock;
    return btree_impl::_ux_update(store, key, elem);
}

rc_t btree_m::overwrite_as_undo(StoreID store, const w_keystr_t &key,
    const char *el, smsize_t offset, smsize_t elen)
{
    // UNDO update operation
    no_lock_section_t nolock;
    return btree_impl::_ux_overwrite(store, key, el, offset, elen);
}
rc_t
btree_m::undo_ghost_mark(StoreID store, const w_keystr_t &key)
{
    // UNDO delete operation
    no_lock_section_t nolock;
    return btree_impl::_ux_undo_ghost_mark(store, key);
}

rc_t btree_m::open_store (StoreID stid, bool for_update)
{
    // take intent lock
    if (g_xct_does_need_lock()) {
        W_DO(lm->intent_store_lock(stid, for_update ? okvl_mode::IX : okvl_mode::IS));
    }
    return RCOK;
}
