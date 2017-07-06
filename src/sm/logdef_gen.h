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
#include "encoding.h"

template <typename... T>
using LogEncoder = typename foster::VariadicEncoder<foster::InlineEncoder, T...>;

template <typename... T>
void serialize_log_fields(logrec_t* lr, const T&... fields)
{
    char* offset = lr->get_data_offset();
    char* end = LogEncoder<T...>::encode(offset, fields...);
    lr->set_size(end - offset);
}

template <typename... T>
void deserialize_log_fields(logrec_t* lr, T&... fields)
{
    const char* offset = lr->get_data_offset();
    LogEncoder<T...>::decode(offset, &fields...);
}

    struct alloc_page_log : public logrec_t {
        static constexpr kind_t TYPE = t_alloc_page;
    template <class Ptr> void construct (Ptr, PageID pid);
    template <class Ptr> void redo(Ptr);
    };

    struct dealloc_page_log : public logrec_t {
        static constexpr kind_t TYPE = t_dealloc_page;
    template <class Ptr> void construct (Ptr, PageID pid);
    template <class Ptr> void redo(Ptr);
    };

    struct stnode_format_log : public logrec_t {
        static constexpr kind_t TYPE = t_stnode_format;
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

    struct alloc_format_log : public logrec_t {
        static constexpr kind_t TYPE = t_alloc_format;
        template <class Ptr> void construct (Ptr)
        {
        }
        template <class Ptr> void redo(Ptr p)
        {
            auto page = reinterpret_cast<alloc_page*>(p->get_generic_page());
            page->format_empty();
        }
    };

    struct create_store_log : public logrec_t {
        static constexpr kind_t TYPE = t_create_store;
    template <class PagePtr>
    void construct (PagePtr page, PageID root_pid, StoreID snum);
    template <class Ptr> void redo(Ptr);
    };

    struct append_extent_log : public logrec_t {
        static constexpr kind_t TYPE = t_append_extent;
    template <class Ptr> void construct (Ptr, StoreID, extent_id_t ext);
    template <class Ptr> void redo(Ptr);
    };

    struct page_img_format_log : public logrec_t {
        static constexpr kind_t TYPE = t_page_img_format;
    template <class PagePtr> void construct (const PagePtr page);
    template <class Ptr> void redo(Ptr);
    template <class Ptr> void undo(Ptr);
    };

    struct update_emlsn_log : public logrec_t {
        static constexpr kind_t TYPE = t_update_emlsn;
    template <class PagePtr> void construct (const PagePtr page, general_recordid_t child_slot, lsn_t child_lsn);
    template <class Ptr> void redo(Ptr);
    };

    struct btree_norec_alloc_log : public logrec_t {
        static constexpr kind_t TYPE = t_btree_norec_alloc;
    template <class PagePtr> void construct (const PagePtr page, const PagePtr page2, PageID new_page_id, const w_keystr_t& fence, const w_keystr_t& chain_fence_high);
    template <class Ptr> void redo(Ptr);
    };

    struct btree_insert_log : public logrec_t {
        static constexpr kind_t TYPE = t_btree_insert;
    template <class PagePtr> void construct (const PagePtr page, const w_keystr_t& key, const cvec_t& el, const bool sys_txn);
    template <class Ptr> void redo(Ptr);
    template <class Ptr> void undo(Ptr);
    };

    struct btree_insert_nonghost_log : public logrec_t {
        static constexpr kind_t TYPE = t_btree_insert_nonghost;
    template <class PagePtr> void construct (const PagePtr page, const w_keystr_t& key, const cvec_t& el, const bool sys_txn);
    template <class Ptr> void redo(Ptr);
    template <class Ptr> void undo(Ptr);
    };

    struct btree_update_log : public logrec_t {
        static constexpr kind_t TYPE = t_btree_update;
    template <class PagePtr> void construct (const PagePtr page, const w_keystr_t& key, const char* old_el, int old_elen, const cvec_t& new_el);
    template <class Ptr> void redo(Ptr);
    template <class Ptr> void undo(Ptr);
    };

    struct btree_overwrite_log : public logrec_t {
        static constexpr kind_t TYPE = t_btree_overwrite;
    template <class PagePtr> void construct (const PagePtr page, const w_keystr_t& key, const char* old_el, const char* new_el, size_t offset, size_t elen);
    template <class Ptr> void redo(Ptr);
    template <class Ptr> void undo(Ptr);
    };

    struct btree_ghost_mark_log : public logrec_t {
        static constexpr kind_t TYPE = t_btree_ghost_mark;
    template <class PagePtr> void construct (const PagePtr page, const vector<slotid_t>& slots, const bool sys_txn);
    template <class Ptr> void redo(Ptr);
    template <class Ptr> void undo(Ptr);
    };

    struct btree_ghost_reclaim_log : public logrec_t {
        static constexpr kind_t TYPE = t_btree_ghost_reclaim;
    template <class PagePtr> void construct (const PagePtr page, const vector<slotid_t>& slots);
    template <class Ptr> void redo(Ptr);
    };

    struct btree_ghost_reserve_log : public logrec_t {
        static constexpr kind_t TYPE = t_btree_ghost_reserve;
    template <class PagePtr> void construct (const PagePtr page, const w_keystr_t& key, int element_length);
    template <class Ptr> void redo(Ptr);
    };

    struct btree_foster_adopt_log : public logrec_t {
        static constexpr kind_t TYPE = t_btree_foster_adopt;
    template <class PagePtr> void construct (const PagePtr page, const PagePtr page2, PageID new_child_pid, lsn_t child_emlsn, const w_keystr_t& new_child_key);
    template <class Ptr> void redo(Ptr);
    };

    struct btree_split_log : public logrec_t {
        static constexpr kind_t TYPE = t_btree_split;
    template <class PagePtr> void construct (const PagePtr page, const PagePtr page2, uint16_t move_count, const w_keystr_t& new_high_fence, const w_keystr_t& new_chain);
    template <class Ptr> void redo(Ptr);
    };

    struct btree_compress_page_log : public logrec_t {
        static constexpr kind_t TYPE = t_btree_compress_page;
    template <class PagePtr> void construct (const PagePtr page, const w_keystr_t& low, const w_keystr_t& high, const w_keystr_t& chain);
    template <class Ptr> void redo(Ptr);
    };

#endif
