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

#ifndef LOGDEF_GEN_H
#define LOGDEF_GEN_H

#include "w_defines.h"
#include "alloc_page.h"
#include "stnode_page.h"
#include "w_base.h"
#include "w_okvl.h"
#include "logrec.h"

    struct comment_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_comment;
    void construct (const char* msg);
    };

    struct compensate_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_compensate;
    void construct (const lsn_t& rec_lsn);
    };

    struct skip_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_skip;
    void construct ();
    };

    struct chkpt_begin_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_chkpt_begin;
    void construct (const lsn_t &lastMountLSN);
    };

    struct chkpt_bf_tab_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_chkpt_bf_tab;
    void construct (int cnt, const PageID* pid, const lsn_t* rec_lsn, const lsn_t* page_lsn);
    };

    struct chkpt_xct_tab_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_chkpt_xct_tab;
    void construct (const tid_t& youngest, int cnt, const tid_t* tid, const smlevel_0::xct_state_t* state, const lsn_t* last_lsn, const lsn_t* first_lsn);;
    };

    struct chkpt_xct_lock_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_chkpt_xct_lock;
    void construct (const tid_t& tid, int cnt, const okvl_mode* lock_mode, const uint32_t* lock_hash);
    };

    struct chkpt_restore_tab_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_chkpt_restore_tab;
    void construct ();
    template <class Ptr> void redo(Ptr);
    };

    struct chkpt_backup_tab_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_chkpt_backup_tab;
    void construct (int cnt, const string* paths);
    template <class Ptr> void redo(Ptr);
    };

    struct chkpt_end_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_chkpt_end;
    void construct (const lsn_t& master, const lsn_t& min_rec_lsn, const lsn_t& min_xct_lsn);
    };

    struct add_backup_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_add_backup;
    void construct (const string& path, lsn_t backupLSN);
    template <class Ptr> void redo(Ptr);
    };

    struct xct_abort_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_xct_abort;
    void construct ();
    };

    struct xct_end_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_xct_end;
    void construct ();
    };

    struct xct_latency_dump_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_xct_latency_dump;
    void construct (unsigned long nsec);
    };

    struct alloc_page_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_alloc_page;
    void construct (PageID pid);
    template <class Ptr> void redo(Ptr);
    };

    struct dealloc_page_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_dealloc_page;
    void construct (PageID pid);
    template <class Ptr> void redo(Ptr);
    };

    struct stnode_format_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_stnode_format;
        template <class Ptr> void construct (Ptr)
        {
        }
        template <class Ptr> void redo(Ptr p)
        {
            auto stpage = p->get_generic_page();
            if (stpage->pid != stnode_page::stpid) {
                stpage->pid = stnode_page::stpid;
            }
        }
    };

    struct create_store_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_create_store;
    template <class PagePtr>
    void construct (PagePtr page, PageID root_pid, StoreID snum);
    template <class Ptr> void redo(Ptr);
    };

    struct append_extent_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_append_extent;
    template <class Ptr> void construct (Ptr, extent_id_t ext);
    template <class Ptr> void redo(Ptr);
    };

    struct loganalysis_begin_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_loganalysis_begin;
    void construct ();
    };

    struct loganalysis_end_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_loganalysis_end;
    void construct ();
    };

    struct redo_done_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_redo_done;
    void construct ();
    };

    struct undo_done_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_undo_done;
    void construct ();
    };

    struct restore_begin_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_restore_begin;
    void construct ();
    template <class Ptr> void redo(Ptr);
    };

    struct restore_segment_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_restore_segment;
    void construct (uint32_t segment);
    template <class Ptr> void redo(Ptr);
    };

    struct restore_end_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_restore_end;
    void construct ();
    template <class Ptr> void redo(Ptr);
    };

    struct page_img_format_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_page_img_format;
    template <class PagePtr> void construct (const PagePtr page);
    template <class Ptr> void redo(Ptr);
    template <class Ptr> void undo(Ptr);
    };

    struct page_evict_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_page_evict;
    template <class PagePtr> void construct (const PagePtr page, general_recordid_t child_slot, lsn_t child_lsn);
    template <class Ptr> void redo(Ptr);
    };

    struct btree_norec_alloc_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_btree_norec_alloc;
    template <class PagePtr> void construct (const PagePtr page, const PagePtr page2, PageID new_page_id, const w_keystr_t& fence, const w_keystr_t& chain_fence_high);
    template <class Ptr> void redo(Ptr);
    };

    struct btree_insert_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_btree_insert;
    template <class PagePtr> void construct (const PagePtr page, const w_keystr_t& key, const cvec_t& el, const bool sys_txn);
    template <class Ptr> void redo(Ptr);
    template <class Ptr> void undo(Ptr);
    };

    struct btree_insert_nonghost_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_btree_insert_nonghost;
    template <class PagePtr> void construct (const PagePtr page, const w_keystr_t& key, const cvec_t& el, const bool sys_txn);
    template <class Ptr> void redo(Ptr);
    template <class Ptr> void undo(Ptr);
    };

    struct btree_update_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_btree_update;
    template <class PagePtr> void construct (const PagePtr page, const w_keystr_t& key, const char* old_el, int old_elen, const cvec_t& new_el);
    template <class Ptr> void redo(Ptr);
    template <class Ptr> void undo(Ptr);
    };

    struct btree_overwrite_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_btree_overwrite;
    template <class PagePtr> void construct (const PagePtr page, const w_keystr_t& key, const char* old_el, const char* new_el, size_t offset, size_t elen);
    template <class Ptr> void redo(Ptr);
    template <class Ptr> void undo(Ptr);
    };

    struct btree_ghost_mark_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_btree_ghost_mark;
    template <class PagePtr> void construct (const PagePtr page, const vector<slotid_t>& slots, const bool sys_txn);
    template <class Ptr> void redo(Ptr);
    template <class Ptr> void undo(Ptr);
    };

    struct btree_ghost_reclaim_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_btree_ghost_reclaim;
    template <class PagePtr> void construct (const PagePtr page, const vector<slotid_t>& slots);
    template <class Ptr> void redo(Ptr);
    };

    struct btree_ghost_reserve_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_btree_ghost_reserve;
    template <class PagePtr> void construct (const PagePtr page, const w_keystr_t& key, int element_length);
    template <class Ptr> void redo(Ptr);
    };

    struct btree_foster_adopt_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_btree_foster_adopt;
    template <class PagePtr> void construct (const PagePtr page, const PagePtr page2, PageID new_child_pid, lsn_t child_emlsn, const w_keystr_t& new_child_key);
    template <class Ptr> void redo(Ptr);
    };

    struct btree_split_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_btree_split;
    template <class PagePtr> void construct (const PagePtr page, const PagePtr page2, uint16_t move_count, const w_keystr_t& new_high_fence, const w_keystr_t& new_chain);
    template <class Ptr> void redo(Ptr);
    };

    struct btree_compress_page_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_btree_compress_page;
    template <class PagePtr> void construct (const PagePtr page, const w_keystr_t& low, const w_keystr_t& high, const w_keystr_t& chain);
    template <class Ptr> void redo(Ptr);
    };

    struct tick_sec_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_tick_sec;
        void construct();
    };

    struct tick_msec_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_tick_msec;
        void construct();
    };

    struct benchmark_start_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_benchmark_start;
        void construct();
    };

    struct page_write_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_page_write;
        void construct(PageID pid, lsn_t lsn, uint32_t count = 1);
    };

    struct page_read_log : public logrec_t {
        static constexpr kind_t TYPE = logrec_t::t_page_read;
        void construct(PageID pid, uint32_t count);
    };

#endif
