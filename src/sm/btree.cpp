/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define BTREE_C

#include "sm_int_0.h"
#include "sm_int_2.h"
#include "bf_tree.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "btcursor.h"
#include "sm_du_stats.h"
#include "w_key.h"
#include "xct.h"
#include "vec_t.h"
#include <crash.h>

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
btree_m::create(
    const stid_t&           stid,                
    lpid_t&                 root
    )                // O-  root of new btree
{
    FUNC(btree_m::create);
    DBGTHRD(<<"btree create: stid " << stid);

    W_DO( io_m::sx_alloc_a_page(stid, root)); // allocate a root page as separate ssx
    W_DO(btree_impl::_ux_create_tree_core(stid, root));

    bool empty=false;
    W_DO(is_empty(stid.vol.vol, stid.store,empty)); 
    if(!empty) {
         DBGTHRD(<<"eNDXNOTEMPTY");
         return RC(eNDXNOTEMPTY);
    }
    DBGTHRD(<<"returning from btree_create, store " << stid);
    return RCOK;
}

rc_t
btree_m::is_empty(
    volid_t vol, snum_t store,
    bool&                 ret)        // O-  true if btree is empty
{
    bt_cursor_t cursor(vol, store, true);
    W_DO( cursor.next());
    ret = cursor.eof();
    return RCOK;
}

rc_t btree_m::insert(volid_t vol, snum_t store, const w_keystr_t &key, const cvec_t &el)
{
    if(key.get_length_as_keystr() + el.size() > btree_page_h::max_entry_size) {
        return RC(eRECWONTFIT);
    }
    W_DO(btree_impl::_ux_insert(vol, store, key, el));
    return RCOK;
}

rc_t btree_m::update(
    volid_t vol, snum_t store,
    const w_keystr_t&                 key,
    const cvec_t&                     elem)
{
    if(key.get_length_as_keystr() + elem.size() > btree_page_h::max_entry_size) {
        return RC(eRECWONTFIT);
    }
    W_DO(btree_impl::_ux_update(vol, store, key, elem));
    return RCOK;
}

rc_t btree_m::put(
    volid_t vol, snum_t store,
    const w_keystr_t&                 key,
    const cvec_t&                     elem)
{
    if(key.get_length_as_keystr() + elem.size() > btree_page_h::max_entry_size) {
        return RC(eRECWONTFIT);
    }
    W_DO(btree_impl::_ux_put(vol, store, key, elem));
    return RCOK;
}

rc_t btree_m::overwrite(
    volid_t vol, snum_t store,
    const w_keystr_t&                 key,
    const char*                       el,
    smsize_t                          offset,
    smsize_t                          elen)
{
    W_DO(btree_impl::_ux_overwrite(vol, store, key, el, offset, elen));
    return RCOK;
}

rc_t btree_m::remove(volid_t vol, snum_t store, const w_keystr_t &key)
{
    W_DO(btree_impl::_ux_remove(vol, store, key));
    return RCOK;
}

rc_t btree_m::defrag_page(btree_page_h &page)
{
    W_DO( btree_impl::_sx_defrag_page(page));
    return RCOK;
}


rc_t btree_m::lookup(
    volid_t vol, snum_t store,
    const w_keystr_t &key, void *el, smsize_t &elen, bool &found)
{
    W_DO( btree_impl::_ux_lookup(vol, store, key, found, el, elen ));
    return RCOK;
}
rc_t btree_m::verify_tree(
        volid_t vol, snum_t store, int hash_bits, bool &consistent)
{
    return btree_impl::_ux_verify_tree(vol, store, hash_bits, consistent);
}
rc_t btree_m::verify_volume(
        vid_t vid, int hash_bits, verify_volume_result &result)
{
    return btree_impl::_ux_verify_volume(vid, hash_bits, result);
}

rc_t
btree_m::_get_du_statistics_recurse(
    const lpid_t&        currentpid,
    btree_stats_t&        _stats,
    base_stat_t        &lf_cnt,
    base_stat_t        &int_cnt,
    btree_lf_stats_t        &lf_stats,
    btree_int_stats_t       &int_stats,
    bool                 audit)
{
    btree_page_h next_page;
    btree_page_h current;
    lpid_t nextpid = currentpid;
    // also check right foster sibling.
    // this part is now (partially) loop, not recursion to prevent the stack from growing too long
    while (nextpid.page != 0) {
        shpid_t original_pid = smlevel_0::bf->debug_get_original_pageid(nextpid.page);
        btree_page_h page;
        W_DO( next_page.fix_direct(currentpid.vol().vol, original_pid, LATCH_SH));
        current = next_page;// at this point (after latching next) we don't need to keep the "previous" fixed.
    
        if (current.level() > 1)  {
            int_cnt++;
            W_DO(current.int_stats(int_stats));
            if (audit) {
                W_DO(int_stats.audit());
            }
            _stats.int_pg.add(int_stats);
            if (current.pid0()) {
                nextpid.page = current.pid0();
                W_DO(_get_du_statistics_recurse(
                    nextpid, _stats, lf_cnt, int_cnt,
                    lf_stats, int_stats, audit));
            }
            for (int i = 0; i < current.nrecs(); ++i) {
                nextpid.page = current.child(i);
                W_DO(_get_du_statistics_recurse(
                    nextpid, _stats, lf_cnt, int_cnt,
                    lf_stats, int_stats, audit));
            }
        } else {
            lf_cnt++;
            W_DO(current.leaf_stats(lf_stats));
            if (audit) {
                W_DO(lf_stats.audit());
            }
            _stats.leaf_pg.add(lf_stats);
        }
        nextpid.page = current.get_foster();
    }
    return RCOK;
}
rc_t
btree_m::get_du_statistics(
    const lpid_t&        root,
    btree_stats_t&        _stats,
    bool                 audit)
{
    base_stat_t        lf_cnt = 0;
    base_stat_t        int_cnt = 0;
    base_stat_t        level_cnt = 0;

    /*
       Traverse the btree gathering stats.  This traversal scans across
       each level of the btree starting at the root.  Unfortunately,
       this scan misses "unlinked" pages.  Unlinked pages are empty
       and will be free'd during the next top-down traversal that
       encounters them.  This traversal should really be DFS so it
       can find "unlinked" pages, but we leave it as is for now.
       We account for the unlinked pages after the traversal.
    */
    btree_lf_stats_t        lf_stats;
    btree_int_stats_t       int_stats;
    W_DO(_get_du_statistics_recurse(
        root, _stats, lf_cnt, int_cnt,
        lf_stats, int_stats, audit));

    _stats.unalloc_pg_cnt = 0;
    _stats.unlink_pg_cnt = 0;
    _stats.leaf_pg_cnt += lf_cnt;
    _stats.int_pg_cnt += int_cnt;
    _stats.level_cnt = MAX(_stats.level_cnt, level_cnt);
    return RCOK;
}

void 
btree_m::print(const lpid_t& current, 
    bool print_elem 
)
{
    {
        shpid_t original_pid = smlevel_0::bf->debug_get_original_pageid(current.page);
        btree_page_h page;
        W_COERCE( page.fix_direct(current.vol().vol, original_pid, LATCH_SH));// coerce ok-- debugging

        for (int i = 0; i < 5 - page.level(); i++) {
            cout << '\t';
        }
        w_keystr_t fence_low, fence_high, chain_fence_high;
        page.copy_fence_low_key (fence_low);
        page.copy_fence_high_key(fence_high);
        page.copy_chain_fence_high_key(chain_fence_high);
        cout 
             << " "
             << "LEVEL " << page.level() 
             << ", page " << page.pid().page 
             << ", pid0 " << page.pid0()
             << ", foster " << page.get_foster()
             << ", nrec " << page.nrecs()
             << ", fence-low " << fence_low
             << ", fence-high " << fence_high
             << ", chain_fence-high " << chain_fence_high
             << ", prefix-len " << page.get_prefix_length()
             << endl;
        page.print(print_elem);
        cout << flush;
        //recursively print all descendants and siblings
        if (page.get_foster()) {
            lpid_t child = current;
            child.page = page.get_foster();
            print(child, print_elem);
        }
        if (page.is_node()) {
            if (page.pid0())  {
                lpid_t child = current;
                child.page = page.pid0();
                print(child, print_elem);
            }
            for (int i = 0; i < page.nrecs(); ++i) {
                lpid_t child = current;
                child.page = page.child(i);
                print(child, print_elem);
            }
        }
    }
}
/* 
 * for use by logrecs for logical undo of inserts/deletes
 */
rc_t                        
btree_m::remove_as_undo(volid_t vol, snum_t store, const w_keystr_t &key)
{
    no_lock_section_t nolock;
    return btree_impl::_ux_remove(vol, store,key);
}

rc_t btree_m::update_as_undo(volid_t vol, snum_t store, const w_keystr_t &key, const cvec_t &elem)
{
    no_lock_section_t nolock;
    return btree_impl::_ux_update(vol, store, key, elem);
}

rc_t btree_m::overwrite_as_undo(volid_t vol, snum_t store, const w_keystr_t &key,
    const char *el, smsize_t offset, smsize_t elen)
{
    no_lock_section_t nolock;
    return btree_impl::_ux_overwrite(vol, store, key, el, offset, elen);
}
rc_t
btree_m::undo_ghost_mark(volid_t vol, snum_t store, const w_keystr_t &key)
{
    no_lock_section_t nolock;
    return btree_impl::_ux_undo_ghost_mark(vol, store, key);
}
