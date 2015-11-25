/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define SMINDEX_C
#include "sm_base.h"
#include "sm_du_stats.h"
#include "sm.h"
#include "xct.h"
#include "btree.h"
#include "suppress_unused.h"
#include "vol.h"

/*==============================================================*
 *  Physical ID version of all the index operations                *
 *==============================================================*/

rc_t ss_m::create_index(vid_t vid, stid_t &stid)
{
    W_DO(lm->intent_vol_lock(vid, okvl_mode::IX)); // take IX on volume

    // CS TODO: page allocation should transfer ownership to stnode
    shpid_t root;
    W_DO(smlevel_0::vol->get(vid)->alloc_a_page(root));

    W_DO(vol->get(vid)->create_store(lpid_t(vid, root), stid.store));
    stid.vol = vid;

    W_DO(bt->create(stid_t(vid, stid.store), lpid_t(vid, root)));

    W_DO(lm->intent_store_lock(stid, okvl_mode::X)); // take X on this new index

    return RCOK;
}

rc_t ss_m::destroy_index(const stid_t& stid)
{
    // take IX on volume, X on the index
    W_DO(lm->intent_vol_lock(stid.vol, okvl_mode::IX));
    W_DO(lm->intent_store_lock(stid, okvl_mode::X));
    return RC(eNOTIMPLEMENTED);
    return RCOK;
}

rc_t ss_m::print_index(stid_t stid)
{
    lpid_t root_pid;
    W_DO(open_store_nolock (stid, root_pid)); // this method is for debugging
    bt->print(root_pid);
    return RCOK;
}

rc_t ss_m::touch_index(stid_t stid, uint64_t &page_count)
{
    lpid_t root_pid;
    W_DO(open_store_nolock (stid, root_pid)); // this method is for debugging
    bt->touch_all(stid, page_count);
    return RCOK;
}

rc_t ss_m::create_assoc(stid_t stid, const w_keystr_t& key, const vec_t& el)
{
    lpid_t root_pid;
    W_DO(open_store (stid, root_pid, true));
    W_DO( bt->insert(stid, key, el) );
    return RCOK;
}

rc_t ss_m::update_assoc(stid_t stid, const w_keystr_t& key, const vec_t& el)
{
    lpid_t root_pid;
    W_DO( open_store (stid, root_pid, true));
    W_DO( bt->update(stid, key, el) );
    return RCOK;
}

rc_t ss_m::put_assoc(stid_t stid, const w_keystr_t& key, const vec_t& el)
{
    lpid_t root_pid;
    W_DO( open_store (stid, root_pid, true));
    W_DO( bt->put(stid, key, el) );
    return RCOK;
}

rc_t ss_m::overwrite_assoc(stid_t stid, const w_keystr_t &key,
    const char *el, smsize_t offset, smsize_t elen)
{
    lpid_t root_pid;
    W_DO( open_store (stid, root_pid, true));
    W_DO( bt->overwrite(stid, key, el, offset, elen) );
    return RCOK;
}

rc_t ss_m::destroy_assoc(stid_t stid, const w_keystr_t& key)
{
    lpid_t root_pid;
    W_DO(open_store (stid, root_pid, true));
    W_DO( bt->remove(stid, key) );
    return RCOK;
}

rc_t ss_m::find_assoc(stid_t stid, const w_keystr_t& key,
                 void* el, smsize_t& elen, bool& found)
{
    lpid_t root_pid;
    bool for_update = g_xct_does_ex_lock_for_select();
    W_DO(open_store (stid, root_pid, for_update));
    W_DO( bt->lookup(stid, key, el, elen, found) );
    return RCOK;
}

rc_t ss_m::verify_index(stid_t stid, int hash_bits, bool &consistent)
{
    lpid_t root_pid;
    W_DO( open_store (stid, root_pid));
    W_DO( bt->verify_tree(stid,  hash_bits, consistent) );
    return RCOK;
}

rc_t ss_m::defrag_index_page(btree_page_h &page)
{
    W_DO( bt->defrag_page(page));
    return RCOK;
}

rc_t ss_m::open_store (const stid_t &stid, lpid_t &root_pid, bool for_update)
{
    // take intent lock
    if (g_xct_does_need_lock()) {
        W_DO(lm->intent_vol_store_lock(stid, for_update ? okvl_mode::IX : okvl_mode::IS));
    }
    return open_store_nolock (stid, root_pid);
}
rc_t ss_m::open_store_nolock (const stid_t &stid, lpid_t &root_pid)
{
    shpid_t shpid = vol->get(stid.vol)->get_store_root(stid.store);
    root_pid = lpid_t (stid.vol, shpid); // use nolock version
    if (root_pid.page == 0) {
        return RC(eBADSTID);
    }
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_get_store_info()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::_get_store_info(
    const stid_t&         stid,
    sm_store_info_t&        info
)
{
    lpid_t root_pid;
    W_DO(open_store_nolock(stid, root_pid));
    info.store = stid.store;
    info.root   = root_pid.page;
    return RCOK;
}
