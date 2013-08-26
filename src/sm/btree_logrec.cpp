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

#include "btree_p.h"
#include "btree_impl.h"
#include "vec_t.h"
#include "page_bf_inline.h"

struct btree_insert_t {
    shpid_t        root_shpid;
    uint16_t        klen;
    uint16_t        elen;
    char        data[logrec_t::max_data_sz - sizeof(shpid_t) - 2*sizeof(int16_t)];

    btree_insert_t(const btree_p& page, const w_keystr_t& key,
                   const cvec_t& el);
    int size()        { return sizeof(shpid_t) + 2*sizeof(int16_t) + klen + elen; }
};

btree_insert_t::btree_insert_t(
    const btree_p&         _page, 
    const w_keystr_t&         key, 
    const cvec_t&         el)
    : klen(key.get_length_as_keystr()), elen(el.size())
{
    root_shpid = _page.root().page;
    w_assert1((size_t)(klen + elen) < sizeof(data));
    key.serialize_as_keystr(data);
    el.copy_to(data + klen);
}

btree_insert_log::btree_insert_log(
    const generic_page_h&         page, 
    const w_keystr_t&         key,
    const cvec_t&         el)
{
    const btree_p& bp = * (btree_p*) &page;
    fill(&page.pid(), page.tag(),
         (new (_data) btree_insert_t(bp, key, el))->size());
}

void 
btree_insert_log::undo(generic_page_h* W_IFDEBUG9(page))
{
    w_assert9(page == 0);
    btree_insert_t* dp = (btree_insert_t*) data();

    lpid_t root_pid (_vid, _snum, dp->root_shpid);
    w_keystr_t key;
    key.construct_from_keystr(dp->data, dp->klen);

    // ***LOGICAL*** don't grab locks during undo
    rc_t rc = smlevel_2::bt->remove_as_undo(_vid.vol, _snum, key); 
    if(rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

void
btree_insert_log::redo(generic_page_h* page)
{
    btree_p* bp = (btree_p*) page;
    btree_insert_t* dp = (btree_insert_t*) data();
    
    w_assert1(bp->is_leaf());
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
    w_rc_t rc = bp->replace_ghost(key, el);
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

    btree_update_t(const generic_page_h& page, const w_keystr_t& key,
                   const char* old_el, int old_elen, const cvec_t& new_el) {
        _root_shpid = page.btree_root();
        _klen = key.get_length_as_keystr();
        _old_elen = old_elen;
        _new_elen = new_el.size();
        key.serialize_as_keystr(_data);
        ::memcpy (_data + _klen, old_el, old_elen);
        new_el.copy_to(_data + _klen + _old_elen);
    }
    int size()        { return sizeof(shpid_t) + 3*sizeof(int16_t) + _klen + _old_elen + _new_elen; }
};

btree_update_log::btree_update_log(
    const generic_page_h&         page, 
    const w_keystr_t&     key,
    const char* old_el, int old_elen, const cvec_t& new_el)
{
    fill(&page.pid(), page.tag(),
         (new (_data) btree_update_t(page, key, old_el, old_elen, new_el))->size());
}


void 
btree_update_log::undo(generic_page_h*)
{
    btree_update_t* dp = (btree_update_t*) data();
    
    lpid_t root_pid (_vid, _snum, dp->_root_shpid);

    w_keystr_t key;
    key.construct_from_keystr(dp->_data, dp->_klen);
    vec_t old_el;
    old_el.put(dp->_data + dp->_klen, dp->_old_elen);

    // ***LOGICAL*** don't grab locks during undo
    rc_t rc = smlevel_2::bt->update_as_undo(_vid.vol, _snum, key, old_el); 
    if(rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

void
btree_update_log::redo(generic_page_h* page)
{
    btree_p* bp = (btree_p*) page;
    btree_update_t* dp = (btree_update_t*) data();
    
    w_assert1(bp->is_leaf());
    w_keystr_t key;
    key.construct_from_keystr(dp->_data, dp->_klen);
    vec_t old_el;
    old_el.put(dp->_data + dp->_klen, dp->_old_elen);
    vec_t new_el;
    new_el.put(dp->_data + dp->_klen + dp->_old_elen, dp->_new_elen);

    // PHYSICAL redo
    slotid_t       slot;
    bool           found;
    bp->search(key, found, slot);
    if (!found) {
        W_FATAL_MSG(fcINTERNAL, << "btree_update_log::redo(): not found");
        return;
    }
    w_rc_t rc = bp->replace_el_nolog(slot, new_el);
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

    btree_overwrite_t(const generic_page_h& page, const w_keystr_t& key,
            const char* old_el, const char *new_el, size_t offset, size_t elen) {
        _root_shpid = page.btree_root();
        _klen = key.get_length_as_keystr();
        _offset = offset;
        _elen = elen;
        key.serialize_as_keystr(_data);
        ::memcpy (_data + _klen, old_el + offset, elen);
        ::memcpy (_data + _klen + elen, new_el, elen);
    }
    int size()        { return sizeof(shpid_t) + 3*sizeof(int16_t) + _klen + _elen * 2; }
};


btree_overwrite_log::btree_overwrite_log (const generic_page_h& page, const w_keystr_t& key,
    const char* old_el, const char *new_el, size_t offset, size_t elen) {
    fill(&page.pid(), page.tag(),
         (new (_data) btree_overwrite_t(page, key, old_el, new_el, offset, elen))->size());
}

void btree_overwrite_log::undo(generic_page_h*)
{
    btree_overwrite_t* dp = (btree_overwrite_t*) data();
    
    lpid_t root_pid (_vid, _snum, dp->_root_shpid);

    uint16_t elen = dp->_elen;
    uint16_t offset = dp->_offset;
    w_keystr_t key;
    key.construct_from_keystr(dp->_data, dp->_klen);
    const char* old_el = dp->_data + dp->_klen;

    // ***LOGICAL*** don't grab locks during undo
    rc_t rc = smlevel_2::bt->overwrite_as_undo(_vid.vol, _snum, key, old_el, offset, elen); 
    if(rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

void btree_overwrite_log::redo(generic_page_h* page)
{
    btree_p* bp = (btree_p*) page;
    btree_overwrite_t* dp = (btree_overwrite_t*) data();
    
    w_assert1(bp->is_leaf());

    uint16_t elen = dp->_elen;
    uint16_t offset = dp->_offset;
    w_keystr_t key;
    key.construct_from_keystr(dp->_data, dp->_klen);
    const char* new_el = dp->_data + dp->_klen + elen;

    // PHYSICAL redo
    slotid_t       slot;
    bool           found;
    bp->search(key, found, slot);
    if (!found) {
        W_FATAL_MSG(fcINTERNAL, << "btree_overwrite_log::redo(): not found");
        return;
    }

#if W_DEBUG_LEVEL>0
    const char* old_el = dp->_data + dp->_klen;
    const char* cur_el;
    smsize_t cur_elen;
    bool ghost;
    bp->dat_leaf_ref(slot, cur_el, cur_elen, ghost);
    w_assert1(!ghost);
    w_assert1(cur_elen >= offset + elen);
    w_assert1(::memcmp(old_el, cur_el + offset, elen) == 0);
#endif //W_DEBUG_LEVEL>0

    bp->overwrite_el_nolog(slot, offset, new_el, elen);
}

/** header log object for BTree pages. */
class btree_header_t {
public:
    shpid_t btree_pid0; // +4 -> 4
    shpid_t btree_foster; // +4 -> 8
    int16_t btree_level; // +2 -> 10
    int16_t btree_chain_fence_high_length; // +2 -> 12
    fill4          _fill; // +4 -> 16

    btree_header_t(
        shpid_t pid0,
        int16_t level,
        shpid_t foster,
        int16_t chain_fence_high_length
    ) : btree_pid0(pid0), btree_foster(foster), btree_level(level),
    btree_chain_fence_high_length(chain_fence_high_length)
         {};
    btree_header_t(const generic_page_h &p)
    : btree_pid0(p._pp->btree_pid0), btree_foster(p._pp->btree_foster), btree_level(p._pp->btree_level),
    btree_chain_fence_high_length(p._pp->btree_chain_fence_high_length)
         {};
    
    int size()  { return sizeof(*this); }
    void apply(generic_page_h* page); // used from both undo/redo
};

void btree_header_t::apply(generic_page_h* page)
{
    page->_pp->btree_pid0 = btree_pid0;
    page->_pp->btree_level = btree_level;
    page->_pp->btree_foster = btree_foster;
    page->_pp->btree_chain_fence_high_length = btree_chain_fence_high_length;
}

/** Log of Btree page header changes. */
class btree_header_change_t {
public:
    btree_header_t   before;
    btree_header_t   after;

    btree_header_change_t(const generic_page_h &p,
        shpid_t btree_pid0,
        int16_t btree_level,
        shpid_t btree_foster,
        int16_t btree_chain_fence_high_length
                    ) : 
           before (p),
           after (btree_pid0, btree_level, btree_foster, btree_chain_fence_high_length)
       {
       }

    int size()  { return before.size() + after.size();}
};

btree_header_log::btree_header_log(const generic_page_h& p,
    shpid_t btree_pid0,
    int16_t btree_level,
    shpid_t btree_foster,
    int16_t btree_chain_fence_high_length
)
{
    w_assert3(p.tag() == t_btree_p);
    fill(&p.pid(), p.tag(), (new (data()) btree_header_change_t(
        p, btree_pid0, btree_level, btree_foster,
                btree_chain_fence_high_length))->size());
}

void  btree_header_log::redo(generic_page_h* page)
{
    btree_header_change_t* df = (btree_header_change_t*) data();
    df->after.apply(page);
}

void  btree_header_log::undo(generic_page_h* page)
{
    btree_header_change_t* df = (btree_header_change_t*) data();
    df->before.apply(page);
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

    btree_ghost_t(const btree_p& p, const vector<slotid_t>& slots);
    w_keystr_t get_key (size_t i) const;
    int size() { return sizeof(shpid_t) + sizeof(uint16_t) * 2 + sizeof(size_t) + total_data_size; }
};
btree_ghost_t::btree_ghost_t(const btree_p& p, const vector<slotid_t>& slots)
{
    root_shpid = p.root().page;
    cnt = slots.size();
    uint16_t *offsets = reinterpret_cast<uint16_t*>(slot_data);
    char *current = slot_data + sizeof (uint16_t) * slots.size();

    // the first data is prefix
    {
        uint16_t prefix_len = p._pp->btree_prefix_length;
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

btree_ghost_mark_log::btree_ghost_mark_log(const generic_page_h& p,
    const vector<slotid_t>& slots)
{
    w_assert0(p.tag() == t_btree_p);
    const btree_p& bp = * (btree_p*) &p;
    fill(&p.pid(), p.tag(), (new (data()) btree_ghost_t(bp, slots))->size());
}

void 
btree_ghost_mark_log::undo(generic_page_h*)
{
    // UNDO of ghost marking is to get the record back to regular state
    btree_ghost_t* dp = (btree_ghost_t*) data();
    lpid_t root_pid (_vid, _snum, dp->root_shpid);
    for (size_t i = 0; i < dp->cnt; ++i) {
        w_keystr_t key (dp->get_key(i));
        rc_t rc = smlevel_2::bt->undo_ghost_mark(_vid.vol, _snum, key);
        if(rc.is_error()) {
            cerr << " key=" << key << endl << " rc =" << rc << endl;
            W_FATAL(rc.err_num());
        }
    }
}

void
btree_ghost_mark_log::redo(generic_page_h *page)
{
    // REDO is physical. mark the record as ghost again.
    w_assert1(page);
    btree_p *bp = (btree_p*) page;
    w_assert1(bp->is_leaf());
    btree_ghost_t* dp = (btree_ghost_t*) data();
    for (size_t i = 0; i < dp->cnt; ++i) {
        w_keystr_t key (dp->get_key(i));
        w_assert2(bp->fence_contains(key));
        bool found;
        slotid_t slot;
        bp->search_leaf(key, found, slot);
        if (!found) {
            cerr << " key=" << key << endl << " not found in btree_ghost_mark_log::redo" << endl;
            w_assert1(false); // something unexpected, but can go on.
        }
        bp->mark_ghost(slot);
    }
}

btree_ghost_reclaim_log::btree_ghost_reclaim_log(const generic_page_h& p,
    const vector<slotid_t>& slots)
{
    w_assert0(p.tag() == t_btree_p);
    const btree_p& bp = * (btree_p*) &p;
    // ghost reclaim is single-log system transaction. so, use data_ssx()
    fill(&p.pid(), p.tag(), (new (data_ssx()) btree_ghost_t(bp, slots))->size());
    w_assert0(is_single_sys_xct());
}

void
btree_ghost_reclaim_log::redo(generic_page_h* page)
{
    // REDO is to defrag it again
    btree_p* bp = (btree_p*) page;
    // TODO actually should reclaim only logged entries because
    // locked entries might have been avoided.
    // (but in that case shouldn't defragging the page itself be avoided?)
    rc_t rc = btree_impl::_sx_defrag_page(*bp);
    if (rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}


struct btree_ghost_reserve_t {
    uint16_t      klen;
    uint16_t      record_size;
    char          data[logrec_t::max_data_sz - sizeof(uint16_t) * 2];

    btree_ghost_reserve_t(const w_keystr_t& key,
        int record_size);
    int size() { return sizeof(uint16_t) * 2 + klen; }
};

btree_ghost_reserve_t::btree_ghost_reserve_t(const w_keystr_t& key, int rec_len)
    : klen (key.get_length_as_keystr()), record_size (rec_len)
{
    key.serialize_as_keystr(data);
}

btree_ghost_reserve_log::btree_ghost_reserve_log (
    const generic_page_h& p, const w_keystr_t& key, int record_size)
{
    w_assert0(p.tag() == t_btree_p);
    // ghost creation is single-log system transaction. so, use data_ssx()
    fill(&p.pid(), p.tag(), (new (data_ssx()) btree_ghost_reserve_t(key, record_size))->size());
    w_assert0(is_single_sys_xct());
}

void btree_ghost_reserve_log::redo(generic_page_h* page)
{
    // REDO is to physically make the ghost record
    btree_p* bp = (btree_p*) page;
    // ghost creation is single-log system transaction. so, use data_ssx()
    btree_ghost_reserve_t* dp = (btree_ghost_reserve_t*) data_ssx();

    // PHYSICAL redo.
    w_assert1(bp->is_leaf());
    bp->reserve_ghost(dp->data, dp->klen, dp->record_size);
    w_assert3(bp->is_consistent(true, true));
}

// log for Split (+adopt if intermediate node)
struct btree_foster_split_t {
    shpid_t       _foster_child_pid; // +4 -> 4
    int32_t       _right_begins_from; // +4 -> 8
    shpid_t       _new_child_pid; // +4 -> 12
    int32_t       _new_child_key_len; // +4 -> 16
    char          data[logrec_t::max_data_sz - 16];

    btree_foster_split_t(shpid_t foster_child_pid, int32_t right_begins_from,
        const w_keystr_t* new_child_key, shpid_t new_child_pid)
        : _foster_child_pid (foster_child_pid), _right_begins_from(right_begins_from) {
        _new_child_pid = new_child_pid;
        if (new_child_key == NULL) {
            _new_child_key_len = 0;
        } else {
            _new_child_key_len = new_child_key->get_length_as_keystr();
            new_child_key->serialize_as_keystr(data);
        }
    }
    int size() { return 16 + _new_child_key_len; }
};

btree_foster_split_log::btree_foster_split_log (const generic_page_h& p,
    shpid_t new_pid, int32_t right_begins_from,
    const w_keystr_t* new_child_key, shpid_t new_child_pid)
{
    w_assert0(p.tag() == t_btree_p);
    fill(&p.pid(), p.tag(), (new (_data) btree_foster_split_t(new_pid, right_begins_from, new_child_key, new_child_pid))->size());
}
void btree_foster_split_log::redo(generic_page_h* page)
{
    // by careful-write-ordering, foster-parent can't be written out before foster-child
    // so, foster-parent still holds all data to recover
    btree_p* foster_parent = (btree_p*) page;
    btree_foster_split_t *dp = (btree_foster_split_t*) _data;
    w_keystr_t mid_key = foster_parent->recalculate_fence_for_split(dp->_right_begins_from);
    lpid_t new_pid = page->pid();
    new_pid.page = dp->_foster_child_pid;
    // also, as a special case, we don't care what the new_page looks like at this point.
    // this is a split operation to make a new page, so we can just nuke it here.
    // that's why this REDO is on foster_parent. No REDO for foster_child page.
    w_keystr_t new_child_key;
    w_keystr_t *new_child_key_ptr = NULL;
    if (dp->_new_child_key_len > 0) {
        new_child_key.construct_from_keystr(dp->data, dp->_new_child_key_len);
        new_child_key_ptr = &new_child_key;
    }
    rc_t rc = btree_impl::_ux_split_foster_apply(*foster_parent,
        dp->_right_begins_from, mid_key, new_pid, new_child_key_ptr, dp->_new_child_pid);
    if (rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

// log for special split moving nothing
// this log is for foster-parent node.
struct btree_foster_norecord_split_t {
    shpid_t       _foster; // +4 -> 4
    int16_t       _fence_high_len; // +2 -> 6
    int16_t       _chain_fence_high_len; // +2 -> 8
    char          data[logrec_t::max_data_sz - 8];

    btree_foster_norecord_split_t(shpid_t foster,
        const w_keystr_t& fence_high, const w_keystr_t& chain_fence_high)
        : _foster(foster) {
        _fence_high_len = fence_high.get_length_as_keystr();
        _chain_fence_high_len = chain_fence_high.get_length_as_keystr();
        fence_high.serialize_as_keystr(data);
        chain_fence_high.serialize_as_keystr(data + _fence_high_len);
    }
    int size() { return 8 + _fence_high_len + _chain_fence_high_len; }
};

btree_foster_norecord_split_log::btree_foster_norecord_split_log(const generic_page_h& p, shpid_t foster,
    const w_keystr_t& fence_high, const w_keystr_t& chain_fence_high)
{
    w_assert0(p.tag() == t_btree_p);
    fill(&p.pid(), p.tag(), (new (_data) btree_foster_norecord_split_t(foster, fence_high, chain_fence_high))->size());
}
void btree_foster_norecord_split_log::redo(generic_page_h* page)
{
    btree_p* foster_parent = (btree_p*) page;
    btree_foster_norecord_split_t *dp = (btree_foster_norecord_split_t*) _data;
    w_keystr_t fence_high, chain_fence_high;
    fence_high.construct_from_keystr(dp->data, dp->_fence_high_len);
    chain_fence_high.construct_from_keystr(dp->data + dp->_fence_high_len, dp->_chain_fence_high_len);
    rc_t rc = foster_parent->norecord_split(dp->_foster, fence_high, chain_fence_high);
    if (rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}

// logs for Adopt
struct btree_foster_adopt_parent_t {
    shpid_t _new_child_pid;
    int32_t _new_child_key_len;
    char    data[logrec_t::max_data_sz - sizeof(shpid_t) - sizeof(int32_t)];

    btree_foster_adopt_parent_t(shpid_t new_child_pid, const w_keystr_t& new_child_key)
        : _new_child_pid (new_child_pid) {
        _new_child_key_len = new_child_key.get_length_as_keystr();
        new_child_key.serialize_as_keystr(data);
    }
    int size() { return sizeof(shpid_t) + sizeof(int32_t) + _new_child_key_len; }
};

btree_foster_adopt_parent_log::btree_foster_adopt_parent_log (const generic_page_h& p,
    shpid_t new_child_pid, const w_keystr_t& new_child_key)
{
    w_assert0(p.tag() == t_btree_p);
    fill(&p.pid(), p.tag(), (new (_data) btree_foster_adopt_parent_t(new_child_pid, new_child_key))->size());
}
void btree_foster_adopt_parent_log::redo(generic_page_h* page)
{
    // just call apply() function
    btree_p* foster_parent = (btree_p*) page;
    btree_foster_adopt_parent_t *dp = (btree_foster_adopt_parent_t*) _data;
    w_keystr_t new_child_key;
    new_child_key.construct_from_keystr(dp->data, dp->_new_child_key_len);
    rc_t rc = btree_impl::_ux_adopt_foster_apply_parent(*foster_parent, dp->_new_child_pid, new_child_key);
    if (rc.is_error()) {
        W_FATAL(rc.err_num());
    }
}
btree_foster_adopt_child_log::btree_foster_adopt_child_log (const generic_page_h& p)
{
    w_assert0(p.tag() == t_btree_p);
    fill(&p.pid(), p.tag(), 0);
}
void btree_foster_adopt_child_log::redo(generic_page_h* page)
{
    // just call apply() function
    btree_p* foster_child = (btree_p*) page;
    btree_impl::_ux_adopt_foster_apply_child(*foster_child);
}

// logs for Merge/Rebalance/De-Adopt
// see jira ticket:39 "Node removal and rebalancing" (originally trac ticket:39) for detailed spec
btree_foster_merge_log::btree_foster_merge_log (const generic_page_h& p)
{
    w_assert0(p.tag() == t_btree_p);
    const btree_p& bp = * (btree_p*) &p;
    // we just need merged page's id. that's it.
    *reinterpret_cast<shpid_t*> (_data) = bp.get_foster();
    fill(&p.pid(), p.tag(), sizeof(shpid_t));
}

void btree_foster_merge_log::redo(generic_page_h* page)
{
    // REDO is to merge it again.
    // Because of careful-write-order, the merged page must still exist.
    // Otherwise, this page's REDO shouldn't have been called.
    btree_p* bp = (btree_p*) page;
    W_IFDEBUG1(shpid_t merged_pid = *((shpid_t*) _data));
    w_assert1(bp->get_foster() == merged_pid); // otherwise, already merged!
    rc_t rc = btree_impl::_ux_merge_foster_core(*bp);
    w_assert1(!rc.is_error());
}

struct btree_foster_rebalance_t {
    shpid_t       _foster_parent_pid; // +4 -> 4
    int32_t       _move_count; // +4 -> 8

    btree_foster_rebalance_t(shpid_t parent_pid, int32_t move_count)
        : _foster_parent_pid (parent_pid), _move_count(move_count) {}
    int size() { return sizeof(*this); }
};

btree_foster_rebalance_log::btree_foster_rebalance_log (const generic_page_h& p,
        shpid_t parent_pid, int32_t move_count) {
    w_assert0(p.tag() == t_btree_p);
    fill(&p.pid(), p.tag(), (new (_data) btree_foster_rebalance_t(parent_pid, move_count))->size());
}

void btree_foster_rebalance_log::redo(generic_page_h* page)
{
    // REDO is to rebalance it again.
    // "This" page must be foster-child which received entries.
    btree_p* bp = (btree_p*) page;
    btree_foster_rebalance_t *dp = (btree_foster_rebalance_t*) _data;
    
    // TODO we should have two logs; one for receiver, one for sender.
    // moving from foster-parent to child. below code doesn't consider the case
    // where "this" is already written out but parent isn't yet.
    // so, "this" must be the child, stealing things from parent. oh lame kid.
    btree_p foster_parent_p;
    rc_t rc = foster_parent_p.fix_direct(bp->vol(), dp->_foster_parent_pid, LATCH_EX); // in REDO, so fix_direct should be safe
    w_assert1(!rc.is_error());
    rc_t rc_rb = btree_impl::_ux_rebalance_foster_core(foster_parent_p, *bp, dp->_move_count);
    w_assert1(!rc_rb.is_error());
}

struct btree_foster_deadopt_real_parent_t {
    shpid_t     _deadopted_pid; // +4 -> 4
    int32_t     _foster_slot; // +4 -> 8

    btree_foster_deadopt_real_parent_t(shpid_t deadopted_pid,
    int32_t foster_slot) : _deadopted_pid (deadopted_pid), _foster_slot(foster_slot) {
    }
    int size() { return sizeof(*this); }
};

btree_foster_deadopt_real_parent_log::btree_foster_deadopt_real_parent_log (
    const generic_page_h& p, shpid_t deadopted_pid, int32_t foster_slot) {
    w_assert0(p.tag() == t_btree_p);
#if W_DEBUG_LEVEL>0
    const btree_p& bp = * (btree_p*) &p;
    w_assert1(bp.is_node());
#endif // W_DEBUG_LEVEL>0
    fill(&p.pid(), p.tag(), (new (_data) btree_foster_deadopt_real_parent_t(deadopted_pid, foster_slot))->size());
}

void btree_foster_deadopt_real_parent_log::redo(generic_page_h* page)
{
    // apply changes on real-parent again. no write-order dependency with foster-parent
    btree_p* bp = (btree_p*) page;
    btree_foster_deadopt_real_parent_t *dp = (btree_foster_deadopt_real_parent_t*) _data;
    w_assert1(dp->_foster_slot >= 0 && dp->_foster_slot < page->nslots());
    btree_impl::_ux_deadopt_foster_apply_real_parent(*bp, dp->_deadopted_pid, dp->_foster_slot);
}

struct btree_foster_deadopt_foster_parent_t {
    shpid_t     _deadopted_pid;
    int32_t     _low_key_len, _high_key_len;
    char        data[logrec_t::max_data_sz - sizeof(shpid_t) - sizeof(int32_t) * 2];

    btree_foster_deadopt_foster_parent_t(shpid_t deadopted_pid,
    const w_keystr_t& low_key, const w_keystr_t& high_key) :
        _deadopted_pid (deadopted_pid),
        _low_key_len(low_key.get_length_as_keystr()),
        _high_key_len(high_key.get_length_as_keystr()) {
        low_key.serialize_as_keystr(data);
        high_key.serialize_as_keystr(data + _low_key_len);
    }
    int size() { return sizeof(shpid_t) + sizeof(int32_t) * 2 + _low_key_len + _high_key_len; }
};
btree_foster_deadopt_foster_parent_log::btree_foster_deadopt_foster_parent_log (const generic_page_h& p,
    shpid_t deadopted_pid, const w_keystr_t& low_key, const w_keystr_t& high_key) {
    w_assert0(p.tag() == t_btree_p);
    fill(&p.pid(), p.tag(), (new (_data) btree_foster_deadopt_foster_parent_t(deadopted_pid, low_key, high_key))->size());
}

void btree_foster_deadopt_foster_parent_log::redo(generic_page_h* page)
{
    // apply changes on foster-parent again. no write-order dependency with real-parent
    btree_p* bp = (btree_p*) page;
    btree_foster_deadopt_foster_parent_t *dp = (btree_foster_deadopt_foster_parent_t*) _data;
    
    w_keystr_t low_key, high_key;
    low_key.construct_from_keystr(dp->data, dp->_low_key_len);
    high_key.construct_from_keystr(dp->data + dp->_low_key_len, dp->_high_key_len);
    btree_impl::_ux_deadopt_foster_apply_foster_parent(*bp, dp->_deadopted_pid, low_key, high_key);
}

btree_noop_log::btree_noop_log (const generic_page_h& p) {
    fill(&p.pid(), p.tag(), 0);
}
