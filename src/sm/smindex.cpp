/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"
#include "sm_base.h"
#include "sm.h"
#include "xct.h"
#include "btree.h"
#include "vol.h"
#include "lock.h"

/*==============================================================*
 *  Physical ID version of all the index operations                *
 *==============================================================*/

rc_t ss_m::create_index(StoreID &stid)
{
    // W_DO(lm->intent_vol_lock(vid, okvl_mode::IX)); // take IX on volume

    // CS TODO: page allocation should transfer ownership to stnode
    PageID root;
    W_DO(vol->create_store(root, stid));
    W_DO(bt->create(stid, root));

    W_DO(lm->intent_store_lock(stid, okvl_mode::X)); // take X on this new index

    return RCOK;
}

rc_t ss_m::destroy_index(const StoreID& stid)
{
    // take IX on volume, X on the index
    // W_DO(lm->intent_vol_lock(stid.vol, okvl_mode::IX));
    W_DO(lm->intent_store_lock(stid, okvl_mode::X));
    return RC(eNOTIMPLEMENTED);
    return RCOK;
}

rc_t ss_m::print_index(StoreID stid)
{
    PageID root_pid;
    W_DO(open_store_nolock (stid, root_pid)); // this method is for debugging
    bt->print(root_pid);
    return RCOK;
}

rc_t ss_m::touch_index(StoreID stid, uint64_t &page_count)
{
    PageID root_pid;
    W_DO(open_store_nolock (stid, root_pid)); // this method is for debugging
    bt->touch_all(stid, page_count);
    return RCOK;
}

rc_t ss_m::create_assoc(StoreID stid, const w_keystr_t& key, const vec_t& el)
{
    PageID root_pid;
    W_DO(open_store (stid, root_pid, true));
    W_DO( bt->insert(stid, key, el) );
    return RCOK;
}

rc_t ss_m::update_assoc(StoreID stid, const w_keystr_t& key, const vec_t& el)
{
    PageID root_pid;
    W_DO( open_store (stid, root_pid, true));
    W_DO( bt->update(stid, key, el) );
    return RCOK;
}

rc_t ss_m::put_assoc(StoreID stid, const w_keystr_t& key, const vec_t& el)
{
    PageID root_pid;
    W_DO( open_store (stid, root_pid, true));
    W_DO( bt->put(stid, key, el) );
    return RCOK;
}

rc_t ss_m::overwrite_assoc(StoreID stid, const w_keystr_t &key,
    const char *el, smsize_t offset, smsize_t elen)
{
    PageID root_pid;
    W_DO( open_store (stid, root_pid, true));
    W_DO( bt->overwrite(stid, key, el, offset, elen) );
    return RCOK;
}

rc_t ss_m::destroy_assoc(StoreID stid, const w_keystr_t& key)
{
    PageID root_pid;
    W_DO(open_store (stid, root_pid, true));
    W_DO( bt->remove(stid, key) );
    return RCOK;
}

rc_t ss_m::find_assoc(StoreID stid, const w_keystr_t& key,
                 void* el, smsize_t& elen, bool& found)
{
    PageID root_pid;
    bool for_update = g_xct_does_ex_lock_for_select();
    W_DO(open_store (stid, root_pid, for_update));
    W_DO( bt->lookup(stid, key, el, elen, found) );
    return RCOK;
}

rc_t ss_m::verify_index(StoreID stid, int hash_bits, bool &consistent)
{
    PageID root_pid;
    W_DO( open_store (stid, root_pid));
    W_DO( bt->verify_tree(stid,  hash_bits, consistent) );
    return RCOK;
}

rc_t ss_m::defrag_index_page(btree_page_h &page)
{
    W_DO( bt->defrag_page(page));
    return RCOK;
}

rc_t ss_m::open_store (StoreID stid, PageID &root_pid, bool for_update)
{
    // take intent lock
    if (g_xct_does_need_lock()) {
        W_DO(lm->intent_store_lock(stid, for_update ? okvl_mode::IX : okvl_mode::IS));
    }
    return open_store_nolock (stid, root_pid);
}
rc_t ss_m::open_store_nolock (StoreID stid, PageID &root_pid)
{
    PageID shpid = vol->get_store_root(stid);
    if (shpid == 0) {
        return RC(eBADSTID);
    }
    root_pid = shpid;
    return RCOK;
}

