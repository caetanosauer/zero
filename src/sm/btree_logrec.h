/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

/**
 * Logging and its UNDO/REDO code for BTrees.
 * Separated from logrec.cpp.
 */

#ifndef BTREE_LOGREC_H
#define BTREE_LOGREC_H

#include "w_defines.h"

#define SM_SOURCE
#define LOGREC_C
#include "sm_base.h"
#include "logdef_gen.cpp"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "lock.h"
#include "log.h"
#include "vec_t.h"
#include "tls.h"
#include "block_alloc.h"
#include "restart.h"

/**
 * Page buffers used while Single-Page-Recovery as scratch space.
 * \ingroup Single-Page-Recovery
 */
DECLARE_TLS(block_alloc<generic_page>, scratch_space_pool);
/**
 * Automatically deletes generic_page obtained from scratch_space_pool.
 * \ingroup Single-Page-Recovery
 */

struct SprScratchSpace {
    SprScratchSpace(StoreID store, PageID pid) {
        p = new (*scratch_space_pool) generic_page();
        ::memset(p, 0, sizeof(generic_page));
        p->tag = t_btree_p;
        p->store = store;
        p->pid = pid;
        p->lsn = lsn_t::null;
    }
    ~SprScratchSpace() { scratch_space_pool->destroy_object(p); }
    generic_page* p;
};

struct btree_insert_t {
    PageID     root_shpid;
    uint16_t    klen;
    uint16_t    elen;
    bool        sys_txn;   // True if the insertion was from a page rebalance full logging operation
    char        data[logrec_t::max_data_sz - sizeof(PageID) - 2*sizeof(int16_t) - sizeof(bool)];

    btree_insert_t(const btree_page_h& page, const w_keystr_t& key,
                   const cvec_t& el, bool is_sys_txn);
    int size()        { return sizeof(PageID) + 2*sizeof(int16_t) + klen + elen + sizeof(bool); }
};

struct btree_update_t {
    PageID     _root_shpid;
    uint16_t    _klen;
    uint16_t    _old_elen;
    uint16_t    _new_elen;
    char        _data[logrec_t::max_data_sz - sizeof(PageID) - 3*sizeof(int16_t)];

    btree_update_t(const btree_page_h& page, const w_keystr_t& key,
                   const char* old_el, int old_elen, const cvec_t& new_el) {
        _root_shpid = page.btree_root();
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

    btree_overwrite_t(const btree_page_h& page, const w_keystr_t& key,
            const char* old_el, const char *new_el, size_t offset, size_t elen) {
        _root_shpid = page.btree_root();
        _klen       = key.get_length_as_keystr();
        _offset     = offset;
        _elen       = elen;
        key.serialize_as_keystr(_data);
        ::memcpy (_data + _klen, old_el + offset, elen);
        ::memcpy (_data + _klen + elen, new_el, elen);
    }
    int size()        { return sizeof(PageID) + 3*sizeof(int16_t) + _klen + _elen * 2; }
};

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

    btree_ghost_t(const btree_page_h& p, const vector<slotid_t>& slots, const bool is_sys_txn);
    w_keystr_t get_key (size_t i) const;
    int size() { return sizeof(PageID) + sizeof(uint16_t) * 2 + sizeof(size_t) + total_data_size; }
};

struct btree_ghost_reserve_t {
    uint16_t      klen;
    uint16_t      element_length;
    char          data[logrec_t::max_data_sz - sizeof(uint16_t) * 2];

    btree_ghost_reserve_t(const w_keystr_t& key,
                          int element_length);
    int size() { return sizeof(uint16_t) * 2 + klen; }
};

/**
 * A \b multi-page \b SSX log record for \b btree_norec_alloc.
 * This log is totally \b self-contained, so no WOD assumed.
 */
struct btree_norec_alloc_t : public multi_page_log_t {
    btree_norec_alloc_t(const btree_page_h &p,
        PageID new_page_id, const w_keystr_t& fence, const w_keystr_t& chain_fence_high);
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

// logs for Merge/Rebalance/De-Adopt
// see jira ticket:39 "Node removal and rebalancing" (originally trac ticket:39) for detailed spec

/**
 * A \b multi-page \b SSX log record for \b btree_foster_merge.
 * This log is \b NOT-self-contained, so it assumes WOD (foster-child is deleted later).
 */
struct btree_foster_merge_t : multi_page_log_t {
    PageID         _foster_pid0;        // +4 => 4, foster page ID (destination page)
    lsn_t           _foster_pid0_emlsn;  // +8 => 12, foster emlsn (destination page)
    uint16_t        _high_len;           // +2 => 14, high key length
    uint16_t        _chain_high_len;     // +2 => 16, chain_high key length
    int32_t         _move_count;         // +4 => 20, number of records to move
    uint16_t        _record_data_len;    // +2 => 22, length of record data
    uint16_t        _prefix_len;         // +2 => 24, source page prefix length

    // _data contains high and chain_high, and then followed by moved user records
    // in the case of merging, it means all the records from source (foster child) page
    // The max_data_sz of log record is 3 pages so we should have suffice space in
    // one log record
    char            _data[logrec_t::max_data_sz - sizeof(multi_page_log_t) - 24];

    btree_foster_merge_t(PageID page2_id, const w_keystr_t& high, const w_keystr_t& chain_high,
        PageID foster_pid0, lsn_t foster_pid0_emlsn, const int16_t prefix_len,
        const int32_t move_count, const smsize_t record_data_len, const cvec_t& record_data);
    int size() const { return sizeof(multi_page_log_t) + 24 + _high_len + _chain_high_len + _record_data_len; }
};

/**
 * A \b multi-page \b SSX log record for \b btree_foster_rebalance.
 * This log is \b NOT-self-contained, so it assumes WOD (foster-parent is written later).
 */
struct btree_foster_rebalance_t : multi_page_log_t {
    int32_t         _move_count;         // +4 => 4, number of records to move
    PageID         _new_pid0;           // +4 => 8, non-leaf node only
    lsn_t           _new_pid0_emlsn;     // +8 => 16, non-leaf node only
    uint16_t        _fence_len;          // +2 => 18, fence key length
    uint16_t        _high_len;           // +2 => 20, high key length
    uint16_t        _chain_high_len;     // +2 => 22, chain_high key length
    uint16_t        _record_data_len;    // +2 => 24, length of record data
    uint16_t        _prefix_len;         // +2 => 26, source page prefix length

    // _data contains fence, high and chain_high, and then followed by moved user records
    // in the case of spliting, it means some of the records from source (foster parent)
    // page.  The max_data_size of log record is 3 pages so we should have sufficient
    // space in one log record
    char            _data[logrec_t::max_data_sz - sizeof(multi_page_log_t) - 26];

    btree_foster_rebalance_t(PageID page2_id, const w_keystr_t& fence,
            PageID new_pid0, lsn_t new_pid0_emlsn, const w_keystr_t& high, const w_keystr_t& chain_high,
            const int16_t prefix_len, const int32_t move_count,
            const smsize_t record_data_len, const cvec_t& record_data);
    int size() const { return sizeof(multi_page_log_t) + 26 + _fence_len + _high_len + _chain_high_len + _record_data_len; }
};

/**
 * A \b multi-page \b SSX log record for \b btree_foster_rebalance_norec.
 * This log is totally \b self-contained, so no WOD assumed.
 */
struct btree_foster_rebalance_norec_t : multi_page_log_t {
    int16_t       _fence_len; // +2 -> 2
    char          _data[logrec_t::max_data_sz - sizeof(multi_page_log_t) - 2];

    btree_foster_rebalance_norec_t(const btree_page_h& p,
        const w_keystr_t& fence) : multi_page_log_t(p.get_foster()) {
        w_assert1 (g_xct()->is_single_log_sys_xct());
        w_assert1 (p.latch_mode() == LATCH_EX);
        _fence_len = fence.get_length_as_keystr();
        fence.serialize_as_keystr(_data);
    }
    int size() const { return sizeof(multi_page_log_t) + 2 + _fence_len; }
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
                         lsn_t new_child_emlsn, const w_keystr_t& new_child_key);
    int size() const { return sizeof(multi_page_log_t) + 14 + _new_child_key_len; }
};

/**
 * A \b multi-page \b SSX log record for \b btree_foster_deadopt.
 * This log is totally \b self-contained, so no WOD assumed.
 */
struct btree_foster_deadopt_t : multi_page_log_t {
    PageID     _deadopted_pid;         // +4
    int32_t     _foster_slot;           // +4
    lsn_t       _deadopted_emlsn;       // +8
    uint16_t    _low_len, _high_len;    // +2+2
    char        _data[logrec_t::max_data_sz - sizeof(multi_page_log_t) + 20];

    btree_foster_deadopt_t(PageID page2_id, PageID deadopted_pid, lsn_t deadopted_emlsn,
    int32_t foster_slot, const w_keystr_t &low, const w_keystr_t &high);
    // CS TODO -- why 12 and not 20???
    int size() const { return sizeof(multi_page_log_t) + 12 + _low_len + _high_len ; }
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
