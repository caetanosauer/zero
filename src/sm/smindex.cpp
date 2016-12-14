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

rc_t ss_m::create_assoc(StoreID stid, const w_keystr_t& key, const vec_t& el)
{
    PageID root_pid;
    W_DO(btree_m::open_store (stid, root_pid, true));
    W_DO( btree_m::insert(stid, key, el) );
    return RCOK;
}

rc_t ss_m::update_assoc(StoreID stid, const w_keystr_t& key, const vec_t& el)
{
    PageID root_pid;
    W_DO( btree_m::open_store (stid, root_pid, true));
    W_DO( btree_m::update(stid, key, el) );
    return RCOK;
}

rc_t ss_m::put_assoc(StoreID stid, const w_keystr_t& key, const vec_t& el)
{
    PageID root_pid;
    W_DO( btree_m::open_store (stid, root_pid, true));
    W_DO( btree_m::put(stid, key, el) );
    return RCOK;
}

rc_t ss_m::overwrite_assoc(StoreID stid, const w_keystr_t &key,
    const char *el, smsize_t offset, smsize_t elen)
{
    PageID root_pid;
    W_DO( btree_m::open_store (stid, root_pid, true));
    W_DO( btree_m::overwrite(stid, key, el, offset, elen) );
    return RCOK;
}

rc_t ss_m::destroy_assoc(StoreID stid, const w_keystr_t& key)
{
    PageID root_pid;
    W_DO(btree_m::open_store (stid, root_pid, true));
    W_DO( btree_m::remove(stid, key) );
    return RCOK;
}

rc_t ss_m::find_assoc(StoreID stid, const w_keystr_t& key,
                 void* el, smsize_t& elen, bool& found)
{
    PageID root_pid;
    bool for_update = g_xct_does_ex_lock_for_select();
    W_DO(btree_m::open_store (stid, root_pid, for_update));
    W_DO( btree_m::lookup(stid, key, el, elen, found) );
    return RCOK;
}

rc_t ss_m::verify_index(StoreID stid, int hash_bits, bool &consistent)
{
    PageID root_pid;
    W_DO( btree_m::open_store (stid, root_pid));
    W_DO( btree_m::verify_tree(stid,  hash_bits, consistent) );
    return RCOK;
}

rc_t ss_m::defrag_index_page(btree_page_h &page)
{
    W_DO( btree_m::defrag_page(page));
    return RCOK;
}

