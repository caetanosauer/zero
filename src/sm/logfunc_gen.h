#ifndef LOGFUNC_GEN_H
#define LOGFUNC_GEN_H

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

extern "C" rc_t log_comment(const char* msg);
extern "C" rc_t log_compensate(const lsn_t& rec_lsn);
extern "C" rc_t log_add_backup(const string& path, lsn_t backupLSN);
extern "C" rc_t log_xct_abort();
extern "C" rc_t log_xct_freeing_space();
extern "C" rc_t log_xct_end();
extern "C" rc_t log_xct_end_group(const xct_t** l, int llen);
extern "C" rc_t log_alloc_page(PageID pid);
extern "C" rc_t log_dealloc_page(PageID pid);
extern "C" rc_t log_create_store(PageID root_pid, StoreID snum);
extern "C" rc_t log_append_extent(extent_id_t ext);
extern "C" rc_t log_restore_begin();
extern "C" rc_t log_restore_segment(uint32_t segment);
extern "C" rc_t log_restore_end();
extern "C" rc_t log_page_set_to_be_deleted(const fixable_page_h& page);
extern "C" rc_t log_page_img_format(const btree_page_h& page);
extern "C" rc_t log_page_evict(const btree_page_h& page, general_recordid_t child_slot, lsn_t child_lsn);
extern "C" rc_t log_btree_norec_alloc(const btree_page_h& page, const btree_page_h& page2, PageID new_page_id, const w_keystr_t& fence, const w_keystr_t& chain_fence_high);
extern "C" rc_t log_btree_insert(const btree_page_h& page, const w_keystr_t& key, const cvec_t& el, const bool sys_txn);
extern "C" rc_t log_btree_insert_nonghost(const btree_page_h& page, const w_keystr_t& key, const cvec_t& el, const bool sys_txn);
extern "C" rc_t log_btree_update(const btree_page_h& page, const w_keystr_t& key, const char* old_el, int old_elen, const cvec_t& new_el);
extern "C" rc_t log_btree_overwrite(const btree_page_h& page, const w_keystr_t& key, const char* old_el, const char* new_el, size_t offset, size_t elen);
extern "C" rc_t log_btree_ghost_mark(const btree_page_h& page, const vector<slotid_t>& slots, const bool sys_txn);
extern "C" rc_t log_btree_ghost_reclaim(const btree_page_h& page, const vector<slotid_t>& slots);
extern "C" rc_t log_btree_ghost_reserve(const btree_page_h& page, const w_keystr_t& key, int element_length);
extern "C" rc_t log_btree_foster_adopt(const btree_page_h& page, const btree_page_h& page2, PageID new_child_pid, lsn_t child_emlsn, const w_keystr_t& new_child_key);
extern "C" rc_t log_btree_foster_merge(const btree_page_h& page, const btree_page_h& page2, const w_keystr_t& high, const w_keystr_t& chain_high, PageID foster_pid0, lsn_t foster_emlsn, const int16_t prefix_len, const int32_t move_count, const smsize_t record_buffer_len, const cvec_t& record_data);
extern "C" rc_t log_btree_foster_rebalance(const btree_page_h& page, const btree_page_h& page2, const w_keystr_t& fence, PageID new_pid0, lsn_t pid0_emlsn, const w_keystr_t& high, const w_keystr_t& chain_high, const int16_t prefix_len, const int32_t move_count, const smsize_t record_data_len, const cvec_t& record_data);
extern "C" rc_t log_btree_foster_rebalance_norec(const btree_page_h& page, const btree_page_h& page2, const w_keystr_t& fence);
extern "C" rc_t log_btree_foster_deadopt(const btree_page_h& page, const btree_page_h& page2, PageID deadopted_pid, lsn_t deadopted_emlsn, int32_t foster_slot, const w_keystr_t& low, const w_keystr_t& high);
extern "C" rc_t log_btree_split(const btree_page_h& page, const btree_page_h& page2, uint16_t move_count, const w_keystr_t& new_high_fence, const w_keystr_t& new_chain);
extern "C" rc_t log_btree_compress_page(const btree_page_h& page, const w_keystr_t& low, const w_keystr_t& high, const w_keystr_t& chain);


#endif
