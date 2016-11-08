/*

<std-header orig-src='shore' genfile='true'>

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include "alloc_page.h"
#include "btree_page_h.h"
#include "stnode_page.h"
#include "w_base.h"
#include "logrec.h"

#include "logfunc_gen.h"
#include "logdef_gen.h"

#include "xct_logger.h"

using Logger = XctLogger;

rc_t log_comment(const char* msg)
{
    return Logger::log<comment_log>(msg);
}
rc_t log_compensate(const lsn_t& rec_lsn)
{
    return Logger::log<compensate_log>(rec_lsn);
}
rc_t log_add_backup(const string& path, lsn_t backupLSN)
{
    return Logger::log<add_backup_log>(path, backupLSN);
}
rc_t log_xct_abort()
{
    return Logger::log<xct_abort_log>();
}
rc_t log_xct_freeing_space()
{
    return Logger::log<xct_freeing_space_log>();
}
rc_t log_xct_end()
{
    return Logger::log<xct_end_log>();
}
rc_t log_xct_end_group(const xct_t** l, int llen)
{
    return Logger::log<xct_end_group_log>(l, llen);
}
rc_t log_alloc_page(PageID pid)
{
    return Logger::log<alloc_page_log>(pid);
}
rc_t log_dealloc_page(PageID pid)
{
    return Logger::log<dealloc_page_log>(pid);
}
rc_t log_create_store(PageID root_pid, StoreID snum)
{
    return Logger::log<create_store_log>(root_pid, snum);
}
rc_t log_append_extent(extent_id_t ext)
{
    return Logger::log<append_extent_log>(ext);
}
rc_t log_restore_begin()
{
    return Logger::log<restore_begin_log>();
}
rc_t log_restore_segment(uint32_t segment)
{
    return Logger::log<restore_segment_log>(segment);
}
rc_t log_restore_end()
{
    return Logger::log<restore_end_log>();
}
rc_t log_page_set_to_be_deleted(const fixable_page_h& page)
{
    return Logger::log<page_set_to_be_deleted_log>(page);
}
rc_t log_page_img_format(const btree_page_h& page)
{
    return Logger::log<page_img_format_log>(page);
}
rc_t log_page_evict(const btree_page_h& page, general_recordid_t child_slot, lsn_t child_lsn)
{
    return Logger::log<page_evict_log>(page, child_slot, child_lsn);
}
rc_t log_btree_norec_alloc(const btree_page_h& page, const btree_page_h& page2, PageID new_page_id, const w_keystr_t& fence, const w_keystr_t& chain_fence_high)
{
    return Logger::log<btree_norec_alloc_log>(page, page2, new_page_id, fence, chain_fence_high);
}
rc_t log_btree_insert(const btree_page_h& page, const w_keystr_t& key, const cvec_t& el, const bool sys_txn)
{
    return Logger::log<btree_insert_log>(page, key, el, sys_txn);
}
rc_t log_btree_insert_nonghost(const btree_page_h& page, const w_keystr_t& key, const cvec_t& el, const bool sys_txn)
{
    return Logger::log<btree_insert_nonghost_log>(page, key, el, sys_txn);
}
rc_t log_btree_update(const btree_page_h& page, const w_keystr_t& key, const char* old_el, int old_elen, const cvec_t& new_el)
{
    return Logger::log<btree_update_log>(page, key, old_el, old_elen, new_el);
}
rc_t log_btree_overwrite(const btree_page_h& page, const w_keystr_t& key, const char* old_el, const char* new_el, size_t offset, size_t elen)
{
    return Logger::log<btree_overwrite_log>(page, key, old_el, new_el, offset, elen);
}
rc_t log_btree_ghost_mark(const btree_page_h& page, const vector<slotid_t>& slots, const bool sys_txn)
{
    return Logger::log<btree_ghost_mark_log>(page, slots, sys_txn);
}
rc_t log_btree_ghost_reclaim(const btree_page_h& page, const vector<slotid_t>& slots)
{
    return Logger::log<btree_ghost_reclaim_log>(page, slots);
}
rc_t log_btree_ghost_reserve(const btree_page_h& page, const w_keystr_t& key, int element_length)
{
    return Logger::log<btree_ghost_reserve_log>(page, key, element_length);
}
rc_t log_btree_foster_adopt(const btree_page_h& page, const btree_page_h& page2, PageID new_child_pid, lsn_t child_emlsn, const w_keystr_t& new_child_key)
{
    return Logger::log<btree_foster_adopt_log>(page, page2, new_child_pid, child_emlsn, new_child_key);
}
rc_t log_btree_foster_merge(const btree_page_h& page, const btree_page_h& page2, const w_keystr_t& high, const w_keystr_t& chain_high, PageID foster_pid0, lsn_t foster_emlsn, const int16_t prefix_len, const int32_t move_count, const smsize_t record_buffer_len, const cvec_t& record_data)
{
    return Logger::log<btree_foster_merge_log>(page, page2, high, chain_high, foster_pid0, foster_emlsn, prefix_len, move_count, record_buffer_len, record_data);
}
rc_t log_btree_foster_rebalance(const btree_page_h& page, const btree_page_h& page2, const w_keystr_t& fence, PageID new_pid0, lsn_t pid0_emlsn, const w_keystr_t& high, const w_keystr_t& chain_high, const int16_t prefix_len, const int32_t move_count, const smsize_t record_data_len, const cvec_t& record_data)
{
    return Logger::log<btree_foster_rebalance_log>(page, page2, fence, new_pid0, pid0_emlsn, high, chain_high, prefix_len, move_count, record_data_len, record_data);
}
rc_t log_btree_foster_rebalance_norec(const btree_page_h& page, const btree_page_h& page2, const w_keystr_t& fence)
{
    return Logger::log<btree_foster_rebalance_norec_log>(page, page2, fence);
}
rc_t log_btree_foster_deadopt(const btree_page_h& page, const btree_page_h& page2, PageID deadopted_pid, lsn_t deadopted_emlsn, int32_t foster_slot, const w_keystr_t& low, const w_keystr_t& high)
{
    return Logger::log<btree_foster_deadopt_log>(page, page2, deadopted_pid, deadopted_emlsn, foster_slot, low, high);
}
rc_t log_btree_split(const btree_page_h& page, const btree_page_h& page2, uint16_t move_count, const w_keystr_t& new_high_fence, const w_keystr_t& new_chain)
{
    return Logger::log<btree_split_log>(page, page2, move_count, new_high_fence, new_chain);
}
rc_t log_btree_compress_page(const btree_page_h& page, const w_keystr_t& low, const w_keystr_t& high, const w_keystr_t& chain)
{
    return Logger::log<btree_compress_page_log>(page, low, high, chain);
}
