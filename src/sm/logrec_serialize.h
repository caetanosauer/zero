#ifndef LOGREC_SERIALIZE_H
#define LOGREC_SERIALIZE_H

#include "encoding.h"
#include "logrec.h"
#include "logrec_support.h"

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

template <kind_t LR>
struct LogrecSerializer
{
    template <typename PagePtr, typename... T>
    static void serialize(PagePtr /*unused*/, logrec_t* lr, const T&... fields)
    {
        serialize_log_fields(lr, fields...);
    }

//     template <typename PagePtr, typename... T>
//     void deserialize(PagePtr /*unused*/, logrec_t* lr, const T&... fields)
//     {
//         deserialize_log_fields(lr, fields...);
//     }
};

template <>
struct LogrecSerializer<page_img_format_log>
{
    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        new (lr->data()) page_img_format_t(p->get_generic_page());
    }
};

template <>
struct LogrecSerializer<compensate_log>
{
    template <typename PagePtr, typename... T>
    static void serialize(PagePtr /*unused*/, logrec_t* lr, const T&... fields)
    {
        lr->set_clr(fields...);
    }
};

template <>
struct LogrecSerializer<btree_insert_log>
{
    static void construct(logrec_t* lr, PageID root_pid,
        const w_keystr_t& key, const cvec_t& el, bool is_sys_txn)
    {
        lr->set_size(
             (new (lr->data()) btree_insert_t(root_pid, key, el, is_sys_txn))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, fields...);
    }
};

template <>
struct LogrecSerializer<btree_compress_page_log>
{
    static void construct(logrec_t* lr,
            const w_keystr_t& low, const w_keystr_t& high, const w_keystr_t& chain)
    {
        uint16_t low_len = low.get_length_as_keystr();
        uint16_t high_len = high.get_length_as_keystr();
        uint16_t chain_len = chain.get_length_as_keystr();

        char* ptr = lr->data_ssx();
        memcpy(ptr, &low_len, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
        memcpy(ptr, &high_len, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
        memcpy(ptr, &chain_len, sizeof(uint16_t));
        ptr += sizeof(uint16_t);

        low.serialize_as_keystr(ptr);
        ptr += low_len;
        high.serialize_as_keystr(ptr);
        ptr += high_len;
        chain.serialize_as_keystr(ptr);
        ptr += chain_len;

        lr->set_size(ptr - lr->data_ssx());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, fields...);
    }
};

template <>
struct LogrecSerializer<btree_insert_nonghost_log>
{
    static void construct(logrec_t* lr, PageID root_pid,
            const w_keystr_t &key, const cvec_t &el, const bool is_sys_txn)
    {
        lr->set_size(
                (new (lr->data()) btree_insert_t(root_pid, key, el, is_sys_txn))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, fields...);
    }
};

template <>
struct LogrecSerializer<btree_update_log>
{
    static void construct(logrec_t* lr, PageID root_pid,
        const w_keystr_t& key, const char* old_el, int old_elen, const cvec_t& new_el)
    {
        lr->set_size(
             (new (lr->data()) btree_update_t(
                    root_pid, key, old_el, old_elen, new_el))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, fields...);
    }
};

template <>
struct LogrecSerializer<btree_overwrite_log>
{
    static void construct(logrec_t* lr, PageID root_pid, const w_keystr_t&
            key, const char* old_el, const char *new_el, size_t offset,
            size_t elen)
    {
        lr->set_size( (new (lr->data()) btree_overwrite_t(root_pid, key, old_el,
                        new_el, offset, elen))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, fields...);
    }
};

template <>
struct LogrecSerializer<btree_ghost_mark_log>
{
    template <typename PagePtr>
    static void construct(logrec_t* lr, const PagePtr p, const
            vector<slotid_t>& slots, bool is_sys_txn)
    {
        lr->set_size((new (lr->data()) btree_ghost_t<PagePtr>(p, slots,
                        is_sys_txn))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, p, fields...);
    }
};

template <>
struct LogrecSerializer<btree_ghost_reclaim_log>
{
    template <typename PagePtr>
    static void construct(logrec_t* lr, const PagePtr p, const
            vector<slotid_t>& slots)
    {
        lr->set_size((new (lr->data_ssx()) btree_ghost_t<PagePtr>(p, slots,
                        false))->size());
        w_assert0(lr->is_single_sys_xct());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, p, fields...);
    }
};

template <>
struct LogrecSerializer<btree_ghost_reserve_log>
{
    static void construct (logrec_t* lr,
        const w_keystr_t& key, int element_length)
    {
        lr->set_size((new (lr->data_ssx()) btree_ghost_reserve_t(key,
                        element_length))->size());
        w_assert0(lr->is_single_sys_xct());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p, logrec_t* lr, const T&... fields)
    {
        construct(lr, fields...);
    }
};

template <>
struct LogrecSerializer<btree_split_log>
{
    template <typename PagePtr>
    static void construct (logrec_t* lr, PagePtr child_p, PagePtr parent_p,
            uint16_t move_count, const w_keystr_t& new_high_fence,
            const w_keystr_t& new_chain)
    {
        // If you change this, please make according adjustments in
        // logrec_t::remove_info_for_pid
        btree_bulk_delete_t* bulk =
            new (lr->data_ssx()) btree_bulk_delete_t(parent_p->pid(),
                    child_p->pid(), move_count,
                    new_high_fence, new_chain);
        page_img_format_t* format = new (lr->data_ssx() + bulk->size())
            page_img_format_t(child_p->get_generic_page());

        // Logrec will have the child pid as main pid (i.e., destination page).
        // Parent pid is stored in btree_bulk_delete_t, which is a
        // multi_page_log_t (i.e., source page)
        lr->set_size(bulk->size() + format->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p1, PagePtr p2, logrec_t* lr, const T&... fields)
    {
        construct(lr, p1, p2, fields...);
    }
};

template <>
struct LogrecSerializer<btree_foster_adopt_log>
{
    template <typename PagePtr>
    static void construct (logrec_t* lr, PagePtr /*unused*/, PagePtr p2,
            PageID new_child_pid, lsn_t new_child_emlsn, const w_keystr_t&
            new_child_key)
    {
        lr->set_size((new (lr->data_ssx()) btree_foster_adopt_t( p2->pid(),
                        new_child_pid, new_child_emlsn,
                        new_child_key))->size());
    }

    template <typename PagePtr, typename... T>
    static void serialize(PagePtr p1, PagePtr p2, logrec_t* lr, const T&... fields)
    {
        construct(lr, p1, p2, fields...);
    }
};

#endif
