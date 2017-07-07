#ifndef LOGREC_SUPPORT_H
#define LOGREC_SUPPORT_H

#include "lock.h"
#include "xct.h"
#include "btree_page.h"
#include "alloc_page.h"
#include "stnode_page.h"

/**
 * This is a special way of logging the creation of a new page.
 * New page creation is usually a page split, so the new page has many
 * records in it. To simplify and to avoid many log entries in that case,
 * we log ALL bytes from the beginning to the end of slot vector,
 * and from the record_head8 to the end of page.
 * We can assume totally defragmented page image because this is page creation.
 * We don't need UNDO (again, this is page creation!), REDO is just two memcpy().
 */
struct page_img_format_t {
    size_t      beginning_bytes;
    size_t      ending_bytes;
    char        data[logrec_t::max_data_sz - 2 * sizeof(size_t)];

    int size()        { return 2 * sizeof(size_t) + beginning_bytes + ending_bytes; }

    page_img_format_t (const generic_page* p)
    {
        /*
         * The mid-section of a btree page is usually not used, since head
         * entries are stored on the beginning of the page and variable-sized
         * "bodies" (i.e., key-value data) at the end of the page. This method
         * returns a pointer to the beginning of the unused part and its length.
         * The loc record then just contains the parts before and after the
         * unused section. For pages other than btree ones, the unused part
         * is either at the beginning or at the end of the page, and it must
         * be set to zero when replaying the log record.
         */

        size_t unused_length;
        const char* unused;
        switch (p->tag) {
            case t_alloc_p: {
                auto page = reinterpret_cast<const alloc_page*>(p);
                unused = page->unused_part(unused_length);
                break;
            }
            case t_stnode_p: {
                auto page = reinterpret_cast<const stnode_page*>(p);
                unused = page->unused_part(unused_length);
                break;
            }
            case t_btree_p: {
                auto page = reinterpret_cast<const btree_page*>(p);
                unused = page->unused_part(unused_length);
                break;
            }
            default:
                W_FATAL(eNOTIMPLEMENTED);
        }

        const char *pp_bin = reinterpret_cast<const char *>(p);
        beginning_bytes = unused - pp_bin;
        ending_bytes    = sizeof(generic_page) - (beginning_bytes + unused_length);

        ::memcpy (data, pp_bin, beginning_bytes);
        ::memcpy (data + beginning_bytes, unused + unused_length, ending_bytes);
        // w_assert1(beginning_bytes >= btree_page::hdr_sz);
        w_assert1(beginning_bytes + ending_bytes <= sizeof(generic_page));
    }

    void apply(generic_page* page)
    {
        // w_assert1(beginning_bytes >= btree_page::hdr_sz);
        w_assert1(beginning_bytes + ending_bytes <= sizeof(generic_page));
        char *pp_bin = reinterpret_cast<char *>(page);
        ::memcpy (pp_bin, data, beginning_bytes);
        ::memcpy (pp_bin + sizeof(generic_page) - ending_bytes,
                data + beginning_bytes, ending_bytes);
    }
};

struct btree_insert_t {
    PageID     root_shpid;
    uint16_t    klen;
    uint16_t    elen;
    bool        sys_txn;   // True if the insertion was from a page rebalance full logging operation
    char        data[logrec_t::max_data_sz - sizeof(PageID) - 2*sizeof(int16_t) - sizeof(bool)];

    btree_insert_t(PageID root, const w_keystr_t& key,
                   const cvec_t& el, bool is_sys_txn)
        : klen(key.get_length_as_keystr()), elen(el.size())
    {
        root_shpid = root;
        w_assert1((size_t)(klen + elen) < sizeof(data));
        key.serialize_as_keystr(data);
        el.copy_to(data + klen);
        sys_txn = is_sys_txn;
    }
    int size()        { return sizeof(PageID) + 2*sizeof(int16_t) + klen + elen + sizeof(bool); }
};

struct btree_update_t {
    PageID     _root_shpid;
    uint16_t    _klen;
    uint16_t    _old_elen;
    uint16_t    _new_elen;
    char        _data[logrec_t::max_data_sz - sizeof(PageID) - 3*sizeof(int16_t)];

    btree_update_t(PageID root_pid, const w_keystr_t& key,
                   const char* old_el, int old_elen, const cvec_t& new_el) {
        _root_shpid = root_pid;
        _klen       = key.get_length_as_keystr();
        _old_elen   = old_elen;
        _new_elen   = new_el.size();
        key.serialize_as_keystr(_data);
        ::memcpy (_data + _klen, old_el, old_elen);
        new_el.copy_to(_data + _klen + _old_elen);
    }
    int size()        { return sizeof(PageID) + 3*sizeof(int16_t) + _klen + _old_elen + _new_elen; }
};

struct btree_overwrite_t {
    PageID     _root_shpid;
    uint16_t    _klen;
    uint16_t    _offset;
    uint16_t    _elen;
    char        _data[logrec_t::max_data_sz - sizeof(PageID) - 3*sizeof(int16_t)];

    btree_overwrite_t(PageID root_pid, const w_keystr_t& key,
            const char* old_el, const char *new_el, size_t offset, size_t elen) {
        _root_shpid = root_pid;
        _klen       = key.get_length_as_keystr();
        _offset     = offset;
        _elen       = elen;
        key.serialize_as_keystr(_data);
        ::memcpy (_data + _klen, old_el + offset, elen);
        ::memcpy (_data + _klen + elen, new_el, elen);
    }
    int size()        { return sizeof(PageID) + 3*sizeof(int16_t) + _klen + _elen * 2; }
};

template <class PagePtr>
struct btree_ghost_t {
    PageID       root_shpid;
    uint16_t      sys_txn:1,      // 1 if the insertion was from a page rebalance full logging operation
                  cnt:15;
    uint16_t      prefix_offset;
    size_t        total_data_size;
    // list of [offset], and then list of [length, string-data WITHOUT prefix]
    // this is analogous to BTree page structure on purpose.
    // by doing so, we can guarantee the total size is <data_sz.
    // because one log should be coming from just one page.
    char          slot_data[logrec_t::max_data_sz - sizeof(PageID)
                        - sizeof(uint16_t) * 2 - sizeof(size_t)];

    btree_ghost_t(const PagePtr p, const vector<slotid_t>& slots, const bool is_sys_txn)
    {
        root_shpid = p->root();
        cnt = slots.size();
        if (true == is_sys_txn)
            sys_txn = 1;
        else
            sys_txn = 0;
        uint16_t *offsets = reinterpret_cast<uint16_t*>(slot_data);
        char *current = slot_data + sizeof (uint16_t) * slots.size();

        // the first data is prefix
        {
            uint16_t prefix_len = p->get_prefix_length();
            prefix_offset = (current - slot_data);
            // *reinterpret_cast<uint16_t*>(current) = prefix_len; this causes Bus Error on solaris! so, instead:
            ::memcpy(current, &prefix_len, sizeof(uint16_t));
            if (prefix_len > 0) {
                ::memcpy(current + sizeof(uint16_t), p->get_prefix_key(), prefix_len);
            }
            current += sizeof(uint16_t) + prefix_len;
        }

        for (size_t i = 0; i < slots.size(); ++i) {
            size_t len;
            // w_assert3(p->is_leaf()); // ghost exists only in leaf
            const char* key = p->_leaf_key_noprefix(slots[i], len);
            offsets[i] = (current - slot_data);
            // *reinterpret_cast<uint16_t*>(current) = len; this causes Bus Error on solaris! so, instead:
            uint16_t len_u16 = (uint16_t) len;
            ::memcpy(current, &len_u16, sizeof(uint16_t));
            ::memcpy(current + sizeof(uint16_t), key, len);
            current += sizeof(uint16_t) + len;
        }
        total_data_size = current - slot_data;
        w_assert0(logrec_t::max_data_sz >= sizeof(PageID) + sizeof(uint16_t) * 2  + sizeof(size_t) + total_data_size);
    }

    w_keystr_t get_key (size_t i) const
    {
        w_keystr_t result;
        uint16_t prefix_len;
        // = *reinterpret_cast<const uint16_t*>(slot_data + prefix_offset); this causes Bus Error on solaris
        ::memcpy(&prefix_len, slot_data + prefix_offset, sizeof(uint16_t));
        w_assert1 (prefix_offset < sizeof(slot_data));
        w_assert1 (prefix_len < sizeof(slot_data));
        const char *prefix_key = slot_data + prefix_offset + sizeof(uint16_t);
        uint16_t offset = reinterpret_cast<const uint16_t*>(slot_data)[i];
        w_assert1 (offset < sizeof(slot_data));
        uint16_t len;
        // = *reinterpret_cast<const uint16_t*>(slot_data + offset); this causes Bus Error on solaris
        ::memcpy(&len, slot_data + offset, sizeof(uint16_t));
        w_assert1 (len < sizeof(slot_data));
        const char *key = slot_data + offset + sizeof(uint16_t);
        result.construct_from_keystr(prefix_key, prefix_len, key, len);
        return result;
    }

    int size() { return sizeof(PageID) + sizeof(uint16_t) * 2 + sizeof(size_t) + total_data_size; }
};

struct btree_ghost_reserve_t {
    uint16_t      klen;
    uint16_t      element_length;
    char          data[logrec_t::max_data_sz - sizeof(uint16_t) * 2];

    btree_ghost_reserve_t(const w_keystr_t& key, int elem_length)
        : klen (key.get_length_as_keystr()), element_length (elem_length)
    {
        key.serialize_as_keystr(data);
    }

    int size() { return sizeof(uint16_t) * 2 + klen; }
};

/**
 * A \b multi-page \b SSX log record for \b btree_norec_alloc.
 * This log is totally \b self-contained, so no WOD assumed.
 */
template <class PagePtr>
struct btree_norec_alloc_t : public multi_page_log_t {
    btree_norec_alloc_t(const PagePtr p,
        PageID new_page_id, const w_keystr_t& fence, const w_keystr_t& chain_fence_high)
        : multi_page_log_t(new_page_id)
    {
        w_assert1 (smthread_t::xct()->is_single_log_sys_xct());
        w_assert1 (new_page_id != p->btree_root());
        w_assert1 (p->latch_mode() != LATCH_NL);

        _root_pid       = p->btree_root();
        _foster_pid     = p->get_foster();
        _foster_emlsn   = p->get_foster_emlsn();
        _fence_len      = (uint16_t) fence.get_length_as_keystr();
        _chain_high_len = (uint16_t) chain_fence_high.get_length_as_keystr();
        _btree_level    = (int16_t) p->level();
        w_assert1(size() < logrec_t::max_data_sz);

        fence.serialize_as_keystr(_data);
        chain_fence_high.serialize_as_keystr(_data + _fence_len);
    }
    PageID     _root_pid, _foster_pid;       // +4+4 => 8
    lsn_t       _foster_emlsn;                // +8   => 16
    uint16_t    _fence_len, _chain_high_len;  // +2+2 => 20
    int16_t     _btree_level;                 // +2   => 22
    /** fence key and chain-high key. */
    char        _data[logrec_t::max_data_sz - sizeof(multi_page_log_t) - 22];

    int      size() const {
        return sizeof(multi_page_log_t) + 22 + _fence_len + _chain_high_len;
    }
};

/**
 * A \b multi-page \b SSX log record for \b btree_foster_adopt.
 * This log is totally \b self-contained, so no WOD assumed.
 */
struct btree_foster_adopt_t : multi_page_log_t {
    lsn_t   _new_child_emlsn;   // +8
    PageID _new_child_pid;     // +4
    int16_t _new_child_key_len; // +2
    char    _data[logrec_t::max_data_sz - sizeof(multi_page_log_t) - 14];

    btree_foster_adopt_t(PageID page2_id, PageID new_child_pid,
                         lsn_t new_child_emlsn, const w_keystr_t& new_child_key)
    : multi_page_log_t(page2_id), _new_child_emlsn(new_child_emlsn),
    _new_child_pid (new_child_pid)
    {
        _new_child_key_len = new_child_key.get_length_as_keystr();
        new_child_key.serialize_as_keystr(_data);
    }

    int size() const { return sizeof(multi_page_log_t) + 14 + _new_child_key_len; }
};

/**
 * Delete of a range of keys from a page which was split (i.e., a new
 * foster parent). Deletes the last move_count slots on the page, updating
 * the foster child pointer and the high fence key to the given values.
 */
struct btree_bulk_delete_t : public multi_page_log_t {
    uint16_t move_count;
    uint16_t new_high_fence_len;
    uint16_t new_chain_len;
    fill2 _fill;

    PageID new_foster_child;

    enum {
        fields_sz = sizeof(multi_page_log_t)
            + sizeof(uint16_t) * 4 // 3 uints + fill
            + sizeof(PageID)
    };
    char _data[logrec_t::max_data_sz - fields_sz];

    btree_bulk_delete_t(PageID foster_parent, PageID new_foster_child,
            uint16_t move_count, const w_keystr_t& new_high_fence,
            const w_keystr_t& new_chain)
        :   multi_page_log_t(foster_parent),
            move_count(move_count), new_foster_child(new_foster_child)
    {
        new_high_fence_len = new_high_fence.get_length_as_keystr();
        new_chain_len = new_chain.get_length_as_keystr();

        new_high_fence.serialize_as_keystr(_data);
        new_chain.serialize_as_keystr(_data + new_high_fence_len);
    }

    // Constructs empty logrec
    btree_bulk_delete_t(PageID foster_parent, PageID foster_child) :
        multi_page_log_t(foster_parent),
        move_count(0), new_high_fence_len(0), new_chain_len(0), new_foster_child(foster_child)
    {}

    size_t size()
    {
        return fields_sz + new_high_fence_len + new_chain_len;
    }

    void get_keys(w_keystr_t& new_high_fence, w_keystr_t& new_chain)
    {
        new_high_fence.construct_from_keystr(_data, new_high_fence_len);
        new_chain.construct_from_keystr(_data + new_high_fence_len,
                new_chain_len);
    }
};

#endif

