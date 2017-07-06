#ifndef LOGREC_HANDLER_H
#define LOGREC_HANDLER_H

#include "w_defines.h"
#include "alloc_page.h"
#include "stnode_page.h"
#include "btree_page_h.h"
#include "w_base.h"
#include "w_okvl.h"
#include "btree.h"
#include "btree_impl.h"
#include "logrec.h"
#include "logrec_support.h"
#include "logrec_serialize.h"
#include "encoding.h"

template<kind_t LR, class PagePtr>
struct LogrecHandler
{
    // generic template, never instantiated
};

template<class PagePtr>
struct LogrecHandler<stnode_format_log, PagePtr>
{
    static void redo(logrec_t*, PagePtr p)
    {
        auto stpage = p->get_generic_page();
        if (stpage->pid != stnode_page::stpid) {
            stpage->pid = stnode_page::stpid;
        }
    }
};

template<class PagePtr>
struct LogrecHandler<alloc_format_log, PagePtr>
{
    static void redo(logrec_t*, PagePtr p)
    {
        auto page = reinterpret_cast<alloc_page*>(p->get_generic_page());
        page->format_empty();
    }
};

template <class PagePtr>
struct LogrecHandler<update_emlsn_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr page)
    {
        general_recordid_t slot;
        lsn_t lsn;
        deserialize_log_fields(lr, slot, lsn);
        borrowed_btree_page_h bp(page);
        bp.set_emlsn_general(slot, lsn);
    }
};

template <class PagePtr>
struct LogrecHandler<alloc_page_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr p)
    {
        PageID alloc_pid = lr->pid();
        PageID pid;
        deserialize_log_fields(lr, pid);

        alloc_page* page = (alloc_page*) p->get_generic_page();
        // assertion fails after page-img compression
        // w_assert1(!page->get_bit(pid - alloc_pid));
        page->set_bit(pid - alloc_pid);
    }
};

template <class PagePtr>
struct LogrecHandler<dealloc_page_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr p)
    {
        PageID pid;
        deserialize_log_fields(lr, pid);

        PageID alloc_pid = p->pid();
        alloc_page* page = (alloc_page*) p->get_generic_page();
        // assertion fails after page-img compression
        // w_assert1(page->get_bit(pid - alloc_pid));
        page->unset_bit(pid - alloc_pid);
    }
};

template <class PagePtr>
struct LogrecHandler<page_img_format_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr page) {
        page_img_format_t* dp = reinterpret_cast<page_img_format_t*>(lr->data());
        dp->apply(page->get_generic_page());
    }
};

template <class PagePtr>
struct LogrecHandler<create_store_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr page)
    {
        StoreID snum;
        PageID root_pid;
        deserialize_log_fields(lr, snum, root_pid);

        stnode_page* stpage = (stnode_page*) page->get_generic_page();
        if (stpage->pid != stnode_page::stpid) {
            stpage->pid = stnode_page::stpid;
        }
        stpage->set_root(snum, root_pid);
        stpage->set_last_extent(snum, 0);
    }
};

template <class PagePtr>
struct LogrecHandler<append_extent_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr page)
    {
        extent_id_t ext;
        StoreID snum;
        deserialize_log_fields(lr, ext, snum);
        auto spage = reinterpret_cast<stnode_page*>(page->get_generic_page());
        spage->set_last_extent(snum, ext);
    }
};

template <class PagePtr>
struct LogrecHandler<btree_compress_page_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr page)
    {
        char* ptr = lr->data_ssx();

        uint16_t low_len = *((uint16_t*) ptr);
        ptr += sizeof(uint16_t);
        uint16_t high_len = *((uint16_t*) ptr);
        ptr += sizeof(uint16_t);
        uint16_t chain_len = *((uint16_t*) ptr);
        ptr += sizeof(uint16_t);

        w_keystr_t low, high, chain;
        low.construct_from_keystr(ptr, low_len);
        ptr += low_len;
        high.construct_from_keystr(ptr, high_len);
        ptr += high_len;
        chain.construct_from_keystr(ptr, chain_len);

        borrowed_btree_page_h bp(page);
        bp.compress(low, high, chain, true /* redo */);
    }
};

template <class PagePtr>
struct LogrecHandler<btree_insert_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr page)
    {
        borrowed_btree_page_h bp(page);
        btree_insert_t* dp = (btree_insert_t*) lr->data();

        w_assert1(bp.is_leaf());
        w_keystr_t key;
        vec_t el;
        key.construct_from_keystr(dp->data, dp->klen);
        el.put(dp->data + dp->klen, dp->elen);

        W_COERCE(bp.replace_ghost(key, el, true /* redo */));
    }

    static void undo(logrec_t* lr)
    {
        btree_insert_t* dp = (btree_insert_t*) lr->data();

        if (true == dp->sys_txn)
        {
            return;
        }

        w_keystr_t key;
        key.construct_from_keystr(dp->data, dp->klen);
        W_COERCE(smlevel_0::bt->remove_as_undo(lr->stid(), key));
    }
};

template <class PagePtr>
struct LogrecHandler<btree_insert_nonghost_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr page)
    {
        borrowed_btree_page_h bp(page);
        btree_insert_t* dp = reinterpret_cast<btree_insert_t*>(lr->data());

        w_assert1(bp.is_leaf());
        w_keystr_t key;
        vec_t el;
        key.construct_from_keystr(dp->data, dp->klen);
        el.put(dp->data + dp->klen, dp->elen);

        bp.insert_nonghost(key, el);
    }

    static void undo(logrec_t* lr)
    {
        LogrecHandler<btree_insert_log, PagePtr>::undo(lr);
    }
};

template <class PagePtr>
struct LogrecHandler<btree_update_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr page)
    {
        borrowed_btree_page_h bp(page);
        btree_update_t* dp = (btree_update_t*) lr->data();

        w_assert1(bp.is_leaf());
        w_keystr_t key;
        key.construct_from_keystr(dp->_data, dp->_klen);
        vec_t old_el;
        old_el.put(dp->_data + dp->_klen, dp->_old_elen);
        vec_t new_el;
        new_el.put(dp->_data + dp->_klen + dp->_old_elen, dp->_new_elen);

        slotid_t       slot;
        bool           found;
        bp.search(key, found, slot);
        w_assert0(found);
        W_COERCE(bp.replace_el_nolog(slot, new_el));
    }

    static void undo(logrec_t* lr)
    {
        btree_update_t* dp = (btree_update_t*) lr->data();

        w_keystr_t key;
        key.construct_from_keystr(dp->_data, dp->_klen);
        vec_t old_el;
        old_el.put(dp->_data + dp->_klen, dp->_old_elen);

        W_COERCE(smlevel_0::bt->update_as_undo(lr->stid(), key, old_el));
    }
};

template <class PagePtr>
struct LogrecHandler<btree_overwrite_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr page)
    {
        borrowed_btree_page_h bp(page);
        btree_overwrite_t* dp = (btree_overwrite_t*) lr->data();

        w_assert1(bp.is_leaf());

        uint16_t elen = dp->_elen;
        uint16_t offset = dp->_offset;
        w_keystr_t key;
        key.construct_from_keystr(dp->_data, dp->_klen);
        const char* new_el = dp->_data + dp->_klen + elen;

        slotid_t       slot;
        bool           found;
        bp.search(key, found, slot);
        w_assert0(found);

#if W_DEBUG_LEVEL>0
        const char* old_el = dp->_data + dp->_klen;
        smsize_t cur_elen;
        bool ghost;
        const char* cur_el = bp.element(slot, cur_elen, ghost);
        w_assert1(!ghost);
        w_assert1(cur_elen >= offset + elen);
        w_assert1(::memcmp(old_el, cur_el + offset, elen) == 0);
#endif

        bp.overwrite_el_nolog(slot, offset, new_el, elen);
    }

    static void undo(logrec_t* lr)
    {
        btree_overwrite_t* dp = (btree_overwrite_t*) lr->data();

        uint16_t elen = dp->_elen;
        uint16_t offset = dp->_offset;
        w_keystr_t key;
        key.construct_from_keystr(dp->_data, dp->_klen);
        const char* old_el = dp->_data + dp->_klen;

        W_COERCE(smlevel_0::bt->overwrite_as_undo(lr->stid(), key, old_el,
                    offset, elen));
    }
};

template <class PagePtr>
struct LogrecHandler<btree_ghost_mark_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr page)
    {
        w_assert1(page);
        borrowed_btree_page_h bp(page);

        w_assert1(bp.is_leaf());
        btree_ghost_t<PagePtr>* dp = (btree_ghost_t<PagePtr>*) lr->data();

        for (size_t i = 0; i < dp->cnt; ++i) {
            w_keystr_t key (dp->get_key(i));

            // If full logging, data movement log records are generated to remove records
            // from source, we set the new fence keys for source page in page_rebalance
            // log record which happens before the data movement log records.
            // Which means the source page might contain records which will be moved
            // out after the page_rebalance log records.  Do not validate the fence keys
            // if full logging

            // Assert only if minmal logging
            w_assert2(bp.fence_contains(key));

            bool found;
            slotid_t slot;

            bp.search(key, found, slot);
            if (!found) {
                cerr << " key=" << key << endl << " not found in btree_ghost_mark_log::redo" << endl;
                w_assert1(false); // something unexpected, but can go on.
            }
            else
            {
                bp.mark_ghost(slot);
            }
        }
    }

    static void undo(logrec_t* lr)
    {
        btree_ghost_t<PagePtr>* dp = (btree_ghost_t<PagePtr>*) lr->data();

        if (1 == dp->sys_txn) {
            return;
        }

        for (size_t i = 0; i < dp->cnt; ++i) {
            w_keystr_t key (dp->get_key(i));
            W_COERCE(smlevel_0::bt->undo_ghost_mark(lr->stid(), key));
        }
    }
};

template <class PagePtr>
struct LogrecHandler<btree_ghost_reclaim_log, PagePtr>
{
    static void redo(logrec_t* /*unused*/, PagePtr page)
    {
        // REDO is to defrag it again
        borrowed_btree_page_h bp(page);
        // TODO actually should reclaim only logged entries because
        // locked entries might have been avoided.
        // (but in that case shouldn't defragging the page itself be avoided?)
        W_COERCE(btree_impl::_sx_defrag_page(bp));
    }
};

template <class PagePtr>
struct LogrecHandler<btree_ghost_reserve_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr page)
    {
        // REDO is to physically make the ghost record
        borrowed_btree_page_h bp(page);
        // ghost creation is single-log system transaction. so, use data_ssx()
        btree_ghost_reserve_t* dp = (btree_ghost_reserve_t*) lr->data_ssx();

        w_assert1(bp.is_leaf());
        bp.reserve_ghost(dp->data, dp->klen, dp->element_length);
        w_assert3(bp.is_consistent(true, true));
    }
};

template <class PagePtr>
struct LogrecHandler<btree_split_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr p)
    {
        btree_bulk_delete_t* bulk = (btree_bulk_delete_t*) lr->data_ssx();
        page_img_format_t* format = (page_img_format_t*)
            (lr->data_ssx() + bulk->size());

        if (p->pid() == bulk->new_foster_child) {
            // redoing the foster child
            format->apply(p->get_generic_page());
        }
        else {
            // redoing the foster parent
            borrowed_btree_page_h bp(p);
            w_assert1(bp.nrecs() > bulk->move_count);
            bp.delete_range(bp.nrecs() - bulk->move_count, bp.nrecs());

            w_keystr_t new_high_fence, new_chain;
            bulk->get_keys(new_high_fence, new_chain);

            bp.set_foster_child(bulk->new_foster_child, new_high_fence, new_chain);
        }
    }
};

template <class PagePtr>
struct LogrecHandler<btree_foster_adopt_log, PagePtr>
{
    static void redo(logrec_t* lr, PagePtr p)
    {
        w_assert1(lr->is_single_sys_xct());
        borrowed_btree_page_h bp(p);
        btree_foster_adopt_t *dp = reinterpret_cast<btree_foster_adopt_t*>(
                lr->data_ssx());

        w_keystr_t new_child_key;
        new_child_key.construct_from_keystr(dp->_data, dp->_new_child_key_len);

        PageID target_pid = p->pid();
        DBGOUT3 (<< *lr << " target_pid=" << target_pid << ", new_child_pid="
                << dp->_new_child_pid << ", new_child_key=" << new_child_key);
        if (target_pid == dp->_page2_pid) {
            // we are recovering "page2", which is real-child.
            w_assert0(target_pid == dp->_page2_pid);
            btree_impl::_ux_adopt_foster_apply_child(bp);
        } else {
            // we are recovering "page", which is real-parent.
            btree_impl::_ux_adopt_foster_apply_parent(bp, dp->_new_child_pid,
                    dp->_new_child_emlsn, new_child_key);
        }
    }
};

#endif
