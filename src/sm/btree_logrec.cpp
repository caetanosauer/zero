/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

/**
 * Logging and its UNDO/REDO code for BTrees.
 * Separated from logrec.cpp.
 */

#include "w_defines.h"

#define SM_SOURCE
#define LOGREC_C
#include "sm_int_2.h"
#include "logdef_gen.cpp"

#include "btree_page_h.h"
#include "btree_impl.h"
#include "vec_t.h"

struct btree_insert_t {
    shpid_t     root_shpid;
    uint16_t    klen;
    uint16_t    elen;
    char        data[logrec_t::max_data_sz - sizeof(shpid_t) - 2*sizeof(int16_t)];

    btree_insert_t(const btree_page_h& page, const w_keystr_t& key,
                   const cvec_t& el);
    int size()        { return sizeof(shpid_t) + 2*sizeof(int16_t) + klen + elen; }
};

btree_insert_t::btree_insert_t(
    const btree_page_h&   _page, 
    const w_keystr_t&     key, 
    const cvec_t&         el)
    : klen(key.get_length_as_keystr()), elen(el.size())
{
    root_shpid = _page.root().page;
    w_assert1((size_t)(klen + elen) < sizeof(data));
    key.serialize_as_keystr(data);
    el.copy_to(data + klen);
}

btree_insert_log::btree_insert_log(
    const btree_page_h& page, 
    const w_keystr_t&   key,
    const cvec_t&       el)
{
    fill(&page.pid(), page.tag(),
         (new (_data) btree_insert_t(page, key, el))->size());
}

void 
btree_insert_log::undo(fixable_page_h* page) {
    w_assert9(page == 0);
    btree_insert_t* dp = (btree_insert_t*) data();

    lpid_t root_pid (header._vid, header._snum, dp->root_shpid);
    w_keystr_t key;
    key.construct_from_keystr(dp->data, dp->klen);

    // ***LOGICAL*** don't grab locks during undo
    rc_t rc = smlevel_2::bt->remove_as_undo(header._vid.vol, header._snum, key); 
    if(rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

void
btree_insert_log::redo(fixable_page_h* page) {
    borrowed_btree_page_h bp(page);
    btree_insert_t* dp = (btree_insert_t*) data();
    
    w_assert1(bp.is_leaf());
    w_keystr_t key;
    vec_t el;
    key.construct_from_keystr(dp->data, dp->klen);
    el.put(dp->data + dp->klen, dp->elen);

    // PHYSICAL redo
    // see btree_impl::_ux_insert()
    // at the point we called log_btree_insert,
    // we already made sure the page has a ghost
    // record for the key that is enough spacious.
    // so, we just replace the record!
    w_rc_t rc = bp.replace_ghost(key, el);
    if(rc.is_error()) { // can't happen. wtf?
        W_FATAL_MSG(fcINTERNAL, << "btree_insert_log::redo " );
    }
}

struct btree_update_t {
    shpid_t     _root_shpid;
    uint16_t    _klen;
    uint16_t    _old_elen;
    uint16_t    _new_elen;
    char        _data[logrec_t::max_data_sz - sizeof(shpid_t) - 3*sizeof(int16_t)];

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
    int size()        { return sizeof(shpid_t) + 3*sizeof(int16_t) + _klen + _old_elen + _new_elen; }
};

btree_update_log::btree_update_log(
    const btree_page_h&   page, 
    const w_keystr_t&     key,
    const char* old_el, int old_elen, const cvec_t& new_el)
{
    fill(&page.pid(), page.tag(),
         (new (_data) btree_update_t(page, key, old_el, old_elen, new_el))->size());
}


void 
btree_update_log::undo(fixable_page_h*)
{
    btree_update_t* dp = (btree_update_t*) data();
    
    lpid_t root_pid (header._vid, header._snum, dp->_root_shpid);

    w_keystr_t key;
    key.construct_from_keystr(dp->_data, dp->_klen);
    vec_t old_el;
    old_el.put(dp->_data + dp->_klen, dp->_old_elen);

    // ***LOGICAL*** don't grab locks during undo
    rc_t rc = smlevel_2::bt->update_as_undo(header._vid.vol, header._snum, key, old_el); 
    if(rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

void
btree_update_log::redo(fixable_page_h* page)
{
    borrowed_btree_page_h bp(page);
    btree_update_t* dp = (btree_update_t*) data();
    
    w_assert1(bp.is_leaf());
    w_keystr_t key;
    key.construct_from_keystr(dp->_data, dp->_klen);
    vec_t old_el;
    old_el.put(dp->_data + dp->_klen, dp->_old_elen);
    vec_t new_el;
    new_el.put(dp->_data + dp->_klen + dp->_old_elen, dp->_new_elen);

    // PHYSICAL redo
    slotid_t       slot;
    bool           found;
    bp.search(key, found, slot);
    if (!found) {
        W_FATAL_MSG(fcINTERNAL, << "btree_update_log::redo(): not found");
        return;
    }
    w_rc_t rc = bp.replace_el_nolog(slot, new_el);
    if(rc.is_error()) { // can't happen. wtf?
        W_FATAL_MSG(fcINTERNAL, << "btree_update_log::redo(): couldn't replace");
    }
}

struct btree_overwrite_t {
    shpid_t     _root_shpid;
    uint16_t    _klen;
    uint16_t    _offset;
    uint16_t    _elen;
    char        _data[logrec_t::max_data_sz - sizeof(shpid_t) - 3*sizeof(int16_t)];

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
    int size()        { return sizeof(shpid_t) + 3*sizeof(int16_t) + _klen + _elen * 2; }
};


btree_overwrite_log::btree_overwrite_log (const btree_page_h& page, const w_keystr_t& key,
                                          const char* old_el, const char *new_el, size_t offset, size_t elen) {
    fill(&page.pid(), page.tag(),
         (new (_data) btree_overwrite_t(page, key, old_el, new_el, offset, elen))->size());
}

void btree_overwrite_log::undo(fixable_page_h*)
{
    btree_overwrite_t* dp = (btree_overwrite_t*) data();
    
    lpid_t root_pid (header._vid, header._snum, dp->_root_shpid);

    uint16_t elen = dp->_elen;
    uint16_t offset = dp->_offset;
    w_keystr_t key;
    key.construct_from_keystr(dp->_data, dp->_klen);
    const char* old_el = dp->_data + dp->_klen;

    // ***LOGICAL*** don't grab locks during undo
    rc_t rc = smlevel_2::bt->overwrite_as_undo(header._vid.vol, header._snum, key, old_el, offset, elen); 
    if(rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

void btree_overwrite_log::redo(fixable_page_h* page)
{
    borrowed_btree_page_h bp(page);
    btree_overwrite_t* dp = (btree_overwrite_t*) data();
    
    w_assert1(bp.is_leaf());

    uint16_t elen = dp->_elen;
    uint16_t offset = dp->_offset;
    w_keystr_t key;
    key.construct_from_keystr(dp->_data, dp->_klen);
    const char* new_el = dp->_data + dp->_klen + elen;

    // PHYSICAL redo
    slotid_t       slot;
    bool           found;
    bp.search(key, found, slot);
    if (!found) {
        W_FATAL_MSG(fcINTERNAL, << "btree_overwrite_log::redo(): not found");
        return;
    }

#if W_DEBUG_LEVEL>0
    const char* old_el = dp->_data + dp->_klen;
    smsize_t cur_elen;
    bool ghost;
    const char* cur_el = bp.element(slot, cur_elen, ghost);
    w_assert1(!ghost);
    w_assert1(cur_elen >= offset + elen);
    w_assert1(::memcmp(old_el, cur_el + offset, elen) == 0);
#endif //W_DEBUG_LEVEL>0

    bp.overwrite_el_nolog(slot, offset, new_el, elen);
}

struct btree_ghost_t {
    shpid_t       root_shpid;
    uint16_t      cnt;
    uint16_t      prefix_offset;
    size_t        total_data_size;
    // list of [offset], and then list of [length, string-data WITHOUT prefix]
    // this is analogous to BTree page structure on purpose.
    // by doing so, we can guarantee the total size is <data_sz.
    // because one log should be coming from just one page.
    char          slot_data[logrec_t::max_data_sz - sizeof(shpid_t)
                        - sizeof(uint16_t) * 2 - sizeof(size_t)];

    btree_ghost_t(const btree_page_h& p, const vector<slotid_t>& slots);
    w_keystr_t get_key (size_t i) const;
    int size() { return sizeof(shpid_t) + sizeof(uint16_t) * 2 + sizeof(size_t) + total_data_size; }
};
btree_ghost_t::btree_ghost_t(const btree_page_h& p, const vector<slotid_t>& slots)
{
    root_shpid = p.root().page;
    cnt = slots.size();
    uint16_t *offsets = reinterpret_cast<uint16_t*>(slot_data);
    char *current = slot_data + sizeof (uint16_t) * slots.size();

    // the first data is prefix
    {
        uint16_t prefix_len = p.get_prefix_length();
        prefix_offset = (current - slot_data);
        // *reinterpret_cast<uint16_t*>(current) = prefix_len; this causes Bus Error on solaris! so, instead:
        ::memcpy(current, &prefix_len, sizeof(uint16_t));
        if (prefix_len > 0) {
            ::memcpy(current + sizeof(uint16_t), p.get_prefix_key(), prefix_len);
        }
        current += sizeof(uint16_t) + prefix_len;
    }
    
     for (size_t i = 0; i < slots.size(); ++i) {
        size_t len;
        w_assert3(p.is_leaf()); // ghost exists only in leaf
        const char* key = p._leaf_key_noprefix(slots[i], len);
        offsets[i] = (current - slot_data);
        // *reinterpret_cast<uint16_t*>(current) = len; this causes Bus Error on solaris! so, instead:
        uint16_t len_u16 = (uint16_t) len;
        ::memcpy(current, &len_u16, sizeof(uint16_t));
        ::memcpy(current + sizeof(uint16_t), key, len);
        current += sizeof(uint16_t) + len;
    }
    total_data_size = current - slot_data;
    w_assert0(logrec_t::max_data_sz >= sizeof(shpid_t) + sizeof(uint16_t) * 2 + sizeof(size_t) + total_data_size);
}
w_keystr_t btree_ghost_t::get_key (size_t i) const {
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

btree_ghost_mark_log::btree_ghost_mark_log(const btree_page_h& p,
                                           const vector<slotid_t>& slots)
{
    fill(&p.pid(), p.tag(), (new (data()) btree_ghost_t(p, slots))->size());
}

void 
btree_ghost_mark_log::undo(fixable_page_h*)
{
    // UNDO of ghost marking is to get the record back to regular state
    btree_ghost_t* dp = (btree_ghost_t*) data();
    lpid_t root_pid (header._vid, header._snum, dp->root_shpid);
    for (size_t i = 0; i < dp->cnt; ++i) {
        w_keystr_t key (dp->get_key(i));
        rc_t rc = smlevel_2::bt->undo_ghost_mark(header._vid.vol, header._snum, key);
        if(rc.is_error()) {
            cerr << " key=" << key << endl << " rc =" << rc << endl;
            W_FATAL(rc.err_num());
        }
    }
}

void
btree_ghost_mark_log::redo(fixable_page_h *page)
{
    // REDO is physical. mark the record as ghost again.
    w_assert1(page);
    borrowed_btree_page_h bp(page);
    w_assert1(bp.is_leaf());
    btree_ghost_t* dp = (btree_ghost_t*) data();
    for (size_t i = 0; i < dp->cnt; ++i) {
        w_keystr_t key (dp->get_key(i));
        w_assert2(bp.fence_contains(key));
        bool found;
        slotid_t slot;
        bp.search(key, found, slot);
        if (!found) {
            cerr << " key=" << key << endl << " not found in btree_ghost_mark_log::redo" << endl;
            w_assert1(false); // something unexpected, but can go on.
        }
        bp.mark_ghost(slot);
    }
}

btree_ghost_reclaim_log::btree_ghost_reclaim_log(const btree_page_h& p,
                                                 const vector<slotid_t>& slots)
{
    // ghost reclaim is single-log system transaction. so, use data_ssx()
    fill(&p.pid(), p.tag(), (new (data_ssx()) btree_ghost_t(p, slots))->size());
    w_assert0(is_single_sys_xct());
}

void
btree_ghost_reclaim_log::redo(fixable_page_h* page)
{
    // REDO is to defrag it again
    borrowed_btree_page_h bp(page);
    // TODO actually should reclaim only logged entries because
    // locked entries might have been avoided.
    // (but in that case shouldn't defragging the page itself be avoided?)
    rc_t rc = btree_impl::_sx_defrag_page(bp);
    if (rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}


struct btree_ghost_reserve_t {
    uint16_t      klen;
    uint16_t      element_length;
    char          data[logrec_t::max_data_sz - sizeof(uint16_t) * 2];

    btree_ghost_reserve_t(const w_keystr_t& key,
                          int element_length);
    int size() { return sizeof(uint16_t) * 2 + klen; }
};

btree_ghost_reserve_t::btree_ghost_reserve_t(const w_keystr_t& key, int elem_length)
    : klen (key.get_length_as_keystr()), element_length (elem_length)
{
    key.serialize_as_keystr(data);
}

btree_ghost_reserve_log::btree_ghost_reserve_log (
    const btree_page_h& p, const w_keystr_t& key, int element_length) {
    // ghost creation is single-log system transaction. so, use data_ssx()
    fill(&p.pid(), p.tag(), (new (data_ssx()) btree_ghost_reserve_t(key, element_length))->size());
    w_assert0(is_single_sys_xct());
}

void btree_ghost_reserve_log::redo(fixable_page_h* page) {
    // REDO is to physically make the ghost record
    borrowed_btree_page_h bp(page);
    // ghost creation is single-log system transaction. so, use data_ssx()
    btree_ghost_reserve_t* dp = (btree_ghost_reserve_t*) data_ssx();

    // PHYSICAL redo.
    w_assert1(bp.is_leaf());
    bp.reserve_ghost(dp->data, dp->klen, dp->element_length);
    w_assert3(bp.is_consistent(true, true));
}

/**
 * A \b multi-page \b SSX log record for \b btree_norec_alloc.
 * This log is totally \b self-contained, so no WOD assumed.
 */
struct btree_norec_alloc_t : public multi_page_log_t {
    btree_norec_alloc_t(const btree_page_h &p,
        shpid_t new_page_id, const w_keystr_t& fence, const w_keystr_t& chain_fence_high);
    shpid_t     _root_pid, _foster_pid;       // +4+4 => 8
    uint16_t    _fence_len, _chain_high_len;  // +2+2 => 12
    int16_t     _btree_level;                 // +2   => 14
    /** fence key and chain-high key. */
    char        _data[logrec_t::max_data_sz - sizeof(multi_page_log_t) - 14];

    int      size() const {
        return sizeof(multi_page_log_t) + 14 + _fence_len + _chain_high_len;
    }
};

btree_norec_alloc_t::btree_norec_alloc_t(const btree_page_h &p,
        shpid_t new_page_id, const w_keystr_t& fence, const w_keystr_t& chain_fence_high)
    : multi_page_log_t(new_page_id) {
    w_assert1 (g_xct()->is_single_log_sys_xct());
    w_assert1 (new_page_id != p.btree_root());
    w_assert1 (p.latch_mode() != LATCH_NL);

    _root_pid       = p.btree_root();
    _foster_pid     = p.get_foster();
    _fence_len      = (uint16_t) fence.get_length_as_keystr();
    _chain_high_len = (uint16_t) chain_fence_high.get_length_as_keystr();
    _btree_level    = (int16_t) p.level();
    w_assert1(size() < logrec_t::max_data_sz);

    fence.serialize_as_keystr(_data);
    chain_fence_high.serialize_as_keystr(_data + _fence_len);
}

btree_norec_alloc_log::btree_norec_alloc_log(const btree_page_h &p, const btree_page_h &,
    shpid_t new_page_id, const w_keystr_t& fence, const w_keystr_t& chain_fence_high) {
    fill(&p.pid(), p.tag(), (new (data_ssx()) btree_norec_alloc_t(p,
        new_page_id, fence, chain_fence_high))->size());
}

void btree_norec_alloc_log::redo(fixable_page_h* p) {
    w_assert1(is_single_sys_xct());
    borrowed_btree_page_h bp(p);
    btree_norec_alloc_t *dp = reinterpret_cast<btree_norec_alloc_t*>(data_ssx());

    const lsn_t &new_lsn = lsn_ck();
    w_keystr_t fence, chain_high;
    fence.construct_from_keystr(dp->_data, dp->_fence_len);
    chain_high.construct_from_keystr(dp->_data + dp->_fence_len, dp->_chain_high_len);

    shpid_t target_pid = p->pid().page;
    DBGOUT3 (<< *this << ": new_lsn=" << new_lsn
        << ", target_pid=" << target_pid << ", bp.lsn=" << bp.lsn());
    if (target_pid == header._shpid) {
        // we are recovering "page", which is foster-parent.
        bp.accept_empty_child(new_lsn, dp->_page2_pid);
    } else {
        // we are recovering "page2", which is foster-child.
        w_assert0(target_pid == dp->_page2_pid);
        // This log is also a page-allocation log, so redo the page allocation.
        W_COERCE(io_m::redo_alloc_a_page(p->pid().vol(), dp->_page2_pid));
        lpid_t pid(header._vid, header._snum, dp->_page2_pid);
        bp.init_as_empty_child(new_lsn, pid, dp->_root_pid, dp->_foster_pid,
            dp->_btree_level, fence, fence, chain_high);
    }
}

// logs for Merge/Rebalance/De-Adopt
// see jira ticket:39 "Node removal and rebalancing" (originally trac ticket:39) for detailed spec

/**
 * A \b multi-page \b SSX log record for \b btree_foster_merge.
 * This log is \b NOT-self-contained, so it assumes WOD (foster-child is deleted later).
 */
struct btree_foster_merge_t : multi_page_log_t {
    btree_foster_merge_t(shpid_t page2_id) : multi_page_log_t(page2_id){}
    int size() const { return sizeof(multi_page_log_t); }
};

btree_foster_merge_log::btree_foster_merge_log (const btree_page_h& p, const btree_page_h& p2) {
    fill(&p.pid(), p.tag(), (new (data_ssx()) btree_foster_merge_t(p2.pid().page))->size());
}

void btree_foster_merge_log::redo(fixable_page_h* p) {
    // REDO is to merge it again.
    // WOD: "page" is the data source, which is written later.
    borrowed_btree_page_h bp(p);
    btree_foster_merge_t *dp = reinterpret_cast<btree_foster_merge_t*>(data_ssx());

    // WOD: "page" is the data source, which is written later.
    // this is in REDO, so fix_direct should be safe.
    shpid_t target_pid = p->pid().page;
    if (target_pid == header._shpid) {
        // we are recovering "page", which is foster-parent (dest).
        // thanks to WOD, "page2" (src) is also assured to be not recovered yet.
        btree_page_h src;
        W_COERCE(src.fix_direct(bp.vol(), dp->_page2_pid, LATCH_EX));

        w_assert0(src.lsn() < lsn_ck());
        btree_impl::_ux_merge_foster_apply_parent(bp, src);
        src.set_dirty();
        src.set_lsns(lsn_ck());
        W_COERCE(src.set_to_be_deleted(false));
    } else {
        // we are recovering "page2", which is foster-child (src).
        // in this case, foster-parent(dest) may or may not be written yet.
        w_assert0(target_pid == dp->_page2_pid);
        btree_page_h dest;
        W_COERCE(dest.fix_direct(bp.vol(), shpid(), LATCH_EX));
        if (dest.lsn() >= lsn_ck()) {
            // if page (destination) is already durable/recovered,
            // we just delete the foster child and done.
            W_COERCE(bp.set_to_be_deleted(false));
        } else {
            // dest is also old, so we are recovering both.
            btree_impl::_ux_merge_foster_apply_parent(dest, bp);
            dest.set_dirty();
            dest.set_lsns(lsn_ck());
        }
    }
}

/**
 * A \b multi-page \b SSX log record for \b btree_foster_rebalance.
 * This log is \b NOT-self-contained, so it assumes WOD (foster-parent is written later).
 */
struct btree_foster_rebalance_t : multi_page_log_t {
    int32_t         _move_count;    // +4
    shpid_t         _new_pid0;      // +4
    uint16_t        _fence_len;     // +2
    char            _data[logrec_t::max_data_sz - sizeof(multi_page_log_t) - 10];

    btree_foster_rebalance_t(shpid_t page2_id, int32_t move_count,
            const w_keystr_t& fence, shpid_t new_pid0);
    int size() const { return sizeof(multi_page_log_t) + 10 + _fence_len; }
};

btree_foster_rebalance_t::btree_foster_rebalance_t(shpid_t page2_id, int32_t move_count,
        const w_keystr_t& fence, shpid_t new_pid0) : multi_page_log_t(page2_id) {
    _move_count = move_count;
    _new_pid0   = new_pid0;
    _fence_len = fence.get_length_as_keystr();
    fence.serialize_as_keystr(_data);
}

btree_foster_rebalance_log::btree_foster_rebalance_log (const btree_page_h& p,
    const btree_page_h &p2, int32_t move_count, const w_keystr_t& fence, shpid_t new_pid0) {
    fill(&p.pid(), p.tag(), (new (data_ssx()) btree_foster_rebalance_t(p2.pid().page,
        move_count, fence, new_pid0))->size());
}

void btree_foster_rebalance_log::redo(fixable_page_h* p) {
    borrowed_btree_page_h bp(p);
    btree_foster_rebalance_t *dp = reinterpret_cast<btree_foster_rebalance_t*>(data_ssx());
    w_keystr_t fence;
    fence.construct_from_keystr(dp->_data, dp->_fence_len);

    // WOD: "page2" is the data source, which is written later.
    // this is in REDO, so fix_direct should be safe.
    const shpid_t target_pid = p->pid().page;
    const shpid_t page_id = shpid();
    const shpid_t page2_id = dp->_page2_pid;
    const lsn_t  &redo_lsn = lsn_ck();
    DBGOUT3 (<< *this << ": redo_lsn=" << redo_lsn << ", bp.lsn=" << bp.lsn());
    w_assert1(bp.lsn() < redo_lsn);
    if (target_pid == page_id) {
        // we are recovering "page", which is foster-child (dest).
        // thanks to WOD, "page2" (src) is also assured to be not recovered yet.
        btree_page_h src;
        W_COERCE(src.fix_direct(bp.vol(), page2_id, LATCH_EX));
        DBGOUT3 (<< "Recovering 'page'. page2.lsn=" << src.lsn());
        w_assert0(src.lsn() < redo_lsn);
        W_COERCE(btree_impl::_ux_rebalance_foster_apply(src, bp, dp->_move_count,
                                                        fence, dp->_new_pid0));
        src.set_dirty();
        src.set_lsns(redo_lsn);
    } else {
        // we are recovering "page2", which is foster-parent (src).
        w_assert0(target_pid == page2_id);
        btree_page_h dest;
        W_COERCE(dest.fix_direct(bp.vol(), page_id, LATCH_EX));
        DBGOUT3 (<< "Recovering 'page2'. page.lsn=" << dest.lsn());
        if (dest.lsn() >= redo_lsn) {
            // if page (destination) is already durable/recovered, we create a dummy scratch
            // space which will be thrown away after recovering "page2".
            w_keystr_t high, chain_high;
            bp.copy_fence_high_key(high);
            bp.copy_chain_fence_high_key(chain_high);
            scratch_btree_page_h scratch_p(dest.pid(), bp.btree_root(), 0, bp.level(),
                high, chain_high, chain_high);
            W_COERCE(btree_impl::_ux_rebalance_foster_apply(bp, scratch_p, dp->_move_count,
                                                        fence, dp->_new_pid0));
        } else {
            // dest is also old, so we are recovering both.
            W_COERCE(btree_impl::_ux_rebalance_foster_apply(bp, dest, dp->_move_count,
                                                        fence, dp->_new_pid0));
            dest.set_dirty();
            dest.set_lsns(redo_lsn);
        }
    }
}

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

btree_foster_rebalance_norec_log::btree_foster_rebalance_norec_log(
    const btree_page_h &p, const btree_page_h &, const w_keystr_t& fence) {
    fill(&p.pid(), p.tag(), (new (data_ssx()) btree_foster_rebalance_norec_t(
        p, fence))->size());
}
void btree_foster_rebalance_norec_log::redo(fixable_page_h* p) {
    w_assert1(is_single_sys_xct());
    borrowed_btree_page_h bp(p);
    btree_foster_rebalance_norec_t *dp =
        reinterpret_cast<btree_foster_rebalance_norec_t*>(data_ssx());

    w_keystr_t fence, chain_high;
    fence.construct_from_keystr(dp->_data, dp->_fence_len);
    bp.copy_chain_fence_high_key(chain_high);

    shpid_t target_pid = p->pid().page;
    if (target_pid == header._shpid) {
        // we are recovering "page", which is foster-parent.
        W_COERCE(bp.norecord_split(bp.get_foster(), fence, chain_high));
    } else {
        // we are recovering "page2", which is foster-child.
        w_assert0(target_pid == dp->_page2_pid);
        w_assert1(bp.nrecs() == 0); // this should happen only during page split.

        w_keystr_t high;
        bp.copy_fence_high_key(high);
        w_keystr_len_t prefix_len = fence.common_leading_bytes(high);
        W_COERCE(bp.replace_fence_rec_nolog_may_defrag(fence, high, chain_high, prefix_len));
    }
}

/**
 * A \b multi-page \b SSX log record for \b btree_foster_adopt.
 * This log is totally \b self-contained, so no WOD assumed.
 */
struct btree_foster_adopt_t : multi_page_log_t {
    shpid_t _new_child_pid;     // +4
    int16_t _new_child_key_len; // +2
    char    _data[logrec_t::max_data_sz - sizeof(multi_page_log_t) - 6];

    btree_foster_adopt_t(shpid_t page2_id, shpid_t new_child_pid,
                         const w_keystr_t& new_child_key);
    int size() const { return sizeof(multi_page_log_t) + 6 + _new_child_key_len; }
};
btree_foster_adopt_t::btree_foster_adopt_t(shpid_t page2_id, shpid_t new_child_pid,
                        const w_keystr_t& new_child_key)
    : multi_page_log_t(page2_id), _new_child_pid (new_child_pid) {
    _new_child_key_len = new_child_key.get_length_as_keystr();
    new_child_key.serialize_as_keystr(_data);
}

btree_foster_adopt_log::btree_foster_adopt_log (const btree_page_h& p, const btree_page_h& p2,
    shpid_t new_child_pid, const w_keystr_t& new_child_key) {
    fill(&p.pid(), p.tag(), (new (data_ssx()) btree_foster_adopt_t(
        p2.pid().page, new_child_pid, new_child_key))->size());
}
void btree_foster_adopt_log::redo(fixable_page_h* p) {
    w_assert1(is_single_sys_xct());
    borrowed_btree_page_h bp(p);
    btree_foster_adopt_t *dp = reinterpret_cast<btree_foster_adopt_t*>(data_ssx());

    w_keystr_t new_child_key;
    new_child_key.construct_from_keystr(dp->_data, dp->_new_child_key_len);

    shpid_t target_pid = p->pid().page;
    DBGOUT3 (<< *this << " target_pid=" << target_pid << ", new_child_pid="
        << dp->_new_child_pid << ", new_child_key=" << new_child_key);
    if (target_pid == header._shpid) {
        // we are recovering "page", which is real-parent.
        btree_impl::_ux_adopt_foster_apply_parent(bp, dp->_new_child_pid, new_child_key);
    } else {
        // we are recovering "page2", which is real-child.
        w_assert0(target_pid == dp->_page2_pid);
        btree_impl::_ux_adopt_foster_apply_child(bp);
    }
}

/**
 * A \b multi-page \b SSX log record for \b btree_foster_deadopt.
 * This log is totally \b self-contained, so no WOD assumed.
 */
struct btree_foster_deadopt_t : multi_page_log_t {
    shpid_t     _deadopted_pid;         // +4
    int32_t     _foster_slot;           // +4
    uint16_t    _low_len, _high_len;    // +2+2
    char        _data[logrec_t::max_data_sz - sizeof(multi_page_log_t) + 12];

    btree_foster_deadopt_t(shpid_t page2_id, shpid_t deadopted_pid,
    int32_t foster_slot, const w_keystr_t &low, const w_keystr_t &high);
    int size() const { return sizeof(multi_page_log_t) + 12 + _low_len + _high_len ; }
};
btree_foster_deadopt_t::btree_foster_deadopt_t(shpid_t page2_id, shpid_t deadopted_pid,
    int32_t foster_slot, const w_keystr_t &low, const w_keystr_t &high)
    : multi_page_log_t(page2_id) {
    _deadopted_pid = deadopted_pid;
    _foster_slot = foster_slot;
    _low_len = low.get_length_as_keystr();
    _high_len = high.get_length_as_keystr();
    low.serialize_as_keystr(_data);
    high.serialize_as_keystr(_data + _low_len);
}

btree_foster_deadopt_log::btree_foster_deadopt_log (
    const btree_page_h& p, const btree_page_h& p2, shpid_t deadopted_pid, int32_t foster_slot,
    const w_keystr_t &low, const w_keystr_t &high) {
    w_assert1(p.is_node());
    fill(&p.pid(), p.tag(), (new (data_ssx()) btree_foster_deadopt_t(p2.pid().page,
        deadopted_pid, foster_slot, low, high))->size());
}

void btree_foster_deadopt_log::redo(fixable_page_h* p) {
    // apply changes on real-parent again. no write-order dependency with foster-parent
    borrowed_btree_page_h bp(p);
    btree_foster_deadopt_t *dp = reinterpret_cast<btree_foster_deadopt_t*>(data_ssx());

    shpid_t target_pid = p->pid().page;
    if (target_pid == header._shpid) {
        // we are recovering "page", which is real-parent.
        w_assert1(dp->_foster_slot >= 0 && dp->_foster_slot < bp.nrecs());
        btree_impl::_ux_deadopt_foster_apply_real_parent(bp, dp->_deadopted_pid,
                                                         dp->_foster_slot);
    } else {
        // we are recovering "page2", which is foster-parent.
        w_assert0(target_pid == dp->_page2_pid);
        w_keystr_t low_key, high_key;
        low_key.construct_from_keystr(dp->_data, dp->_low_len);
        high_key.construct_from_keystr(dp->_data + dp->_low_len, dp->_high_len);
        btree_impl::_ux_deadopt_foster_apply_foster_parent(bp, dp->_deadopted_pid,
                                                           low_key, high_key);
    }
}
