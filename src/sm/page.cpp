#include "w_defines.h"

#define SM_SOURCE
#define PAGE_C
#ifdef __GNUG__
#   pragma implementation "page.h"
#   pragma implementation "page_s.h"
#endif
#include "sm_int_1.h"
#include "page.h"
#include "btree_p.h"
#include "w_key.h"
#include <algorithm>

uint32_t
page_p::get_store_flags() const
{
    return _pp->get_page_storeflags();
}

void
page_p::set_store_flags(uint32_t f)
{
    // If fixed in ex mode, set through the buffer control block
    // Do not set if not fixed EX.
    bfcb_t *b = bf_m::get_cb(_pp);
    if(b && is_mine()) {
        b->set_storeflags(f);
    } else {
        // If this isn't a buffer-pool page, we don't care about
        // keeping any bfcb_t up-to-date. 
        // This is used when we are constructing a page_p with
        // a buffer on the stack. This happens in formatting
        // a volume, for example. 
        _pp->set_page_storeflags(f);
    }
}

void page_p::repair_rec_lsn(bool was_dirty, lsn_t const &new_rlsn) {
    if( !smlevel_0::logging_enabled) return;
    bfcb_t* bp = bf_m::get_cb(_pp);
    const lsn_t &rec_lsn = bp->curr_rec_lsn();
    w_assert2(is_latched_by_me());
    w_assert2(is_mine());
    if(was_dirty) {
        // never mind!
        w_assert0(rec_lsn <= lsn());
    }
    else {
        w_assert0(rec_lsn > lsn() );
        if(new_rlsn.valid()) {
            w_assert0(new_rlsn <= lsn());
            w_assert2(bp->dirty());
            bp->set_rec_lsn(new_rlsn);
            INC_TSTAT(restart_repair_rec_lsn);
        }
        else {
            bp->mark_clean();
        }
    }
}

const char*  page_p::tag_name(tag_t t)
{
    switch (t) {
    case t_alloc_p: 
        return "t_alloc_p";
    case t_stnode_p:
        return "t_stnode_p";
    case t_btree_p:
        return "t_btree_p";
    default:
        W_FATAL(eINTERNAL);
    }

    W_FATAL(eINTERNAL);
    return 0;
}



/*********************************************************************
 *
 *  page_p::_format(pid, tag, page_flags, store_flags)
 *
 *  Called from page-type-specific 4-argument method:
 *    xxx::format(pid, tag, page_flags, store_flags)
 *
 *  Format the page with "pid", "tag", and "page_flags" 
 *        and store_flags (goes into the log record, not into the page)
 *        If log_it is true, it issues a page_init log record
 *
 *********************************************************************/
rc_t
page_p::_format(lpid_t pid, tag_t tag, 
               uint32_t             page_flags, 
               store_flag_t /*store_flags*/
               ) 
{
    uint32_t             sf;

    w_assert3((page_flags & ~t_virgin) == 0); // expect only virgin 
    /*
     *  Check alignments
     */
    w_assert3(is_aligned(data_sz));
    w_assert3(is_aligned(_pp->data - (char*) _pp));
    w_assert3(sizeof(page_s) == page_sz);
    w_assert3(is_aligned(_pp->data));

    /*
     *  Do the formatting...
     *  ORIGINALLY:
     *  Note: store_flags must be valid before page is formatted
     *  unless we're in redo and DONT_TRUST_PAGE_LSN is turned on.
     *  NOW:
     *  store_flags are passed in. The fix() that preceded this
     *  will have stuffed some store_flags into the page(as before)
     *  but they could be wrong. Now that we are logging the store
     *  flags with the page_format log record, we can force the
     *  page to have the correct flags due to redo of the format.
     *  What this does NOT do is fix the store flags in the st_node.
     * See notes in bf_m::_fix
     *
     *  The following code writes all 1's into the page (except
     *  for store-flags) in the hope of helping debug problems
     *  involving updates to pages and/or re-use of deallocated
     *  pages.
     */
    sf = _pp->get_page_storeflags(); // save flags
#ifdef ZERO_INIT
    /* NB -- NOTE -------- NOTE BENE
    *  Note this is not exactly zero-init, but it doesn't matter
    * WHAT we use to init each byte for the purpose of purify or valgrind
    */
    // because we do this, note that we shouldn't receive any arguments
    // as reference or pointer. It might be also nuked!
    memset(_pp, '\017', sizeof(*_pp)); // trash the whole page
#endif //ZERO_INIT

    // _pp->set_page_storeflags(sf); // restore flag
    this->set_store_flags(sf); // changed to do it through the page_p, bfcb_t
    // TODO: any assertions on store_flags?

#if W_DEBUG_LEVEL > 2
    if(
     (smlevel_0::operating_mode == smlevel_0::t_in_undo)
     ||
     (smlevel_0::operating_mode == smlevel_0::t_forward_processing)
    )  // do the assert below
    w_assert3(sf != st_bad);
#endif 

    _pp->lsn= lsn_t(0, 1);
    _pp->pid = pid;
    _pp->page_flags = page_flags;
     w_assert3(tag != t_bad_p);
    _pp->tag = tag;  // must be set before rsvd_mode() is called
    _pp->record_head = data_sz;
    _pp->nslots = _pp->nghosts = _pp->btree_consecutive_skewed_insertions = 0;

    w_assert3 (is_consistent_space());
    return RCOK;
}


/*********************************************************************
 *
 *  page_p::_fix(bool,
 *    pid, ptag, mode, page_flags, store_flags, ignore_store_id, refbit)
 *
 *
 *  Fix a frame for "pid" in buffer pool in latch "mode". 
 *
 *  "Ignore_store_id" indicates whether the store ID
 *  on the page can be trusted to match pid.store; usually it can, 
 *  but if not, then passing in true avoids an extra assert check.
 *  "Refbit" is a page replacement hint to bf when the page is 
 *  unfixed.
 *
 *  NB: this does not set the tag() to ptag -- format does that
 *
 *********************************************************************/
rc_t
page_p::_fix_core(
    bool                 condl,
    const lpid_t&        pid,
    tag_t                ptag,
    latch_mode_t         m, 
    uint32_t              page_flags,
    store_flag_t&        store_flags,//used only if page_flags & t_virgin
    bool                 ignore_store_id, 
    int                  refbit)
{
    w_assert3(!_pp || bf->is_bf_page(_pp, false));
    store_flag_t        ret_store_flags = store_flags;

    // store flags will be st_bad in the t_virgin/no_read forward-processing
    // case because we are fixing the page before a format.

    if (store_flags & st_insert_file)  {
        store_flags = (store_flag_t) (store_flags|st_tmp); 
        // is st_tmp and st_insert_file
    }
    /* allow these only */
    w_assert1((page_flags & ~t_virgin) == 0);

    W_IFTRACE(const char * bf_fix="";)
    if (_pp && _pp->pid == pid) 
    {
        if(_mode >= m)  {
            /*
             *  We have already fixed the page... do nothing.
             */
            W_IFTRACE(bf_fix="no-op";)
        } else if(condl) {
            W_IFTRACE(bf_fix="latch-upgrade";)
              bool would_block = false;
              bf->upgrade_latch_if_not_block(_pp, would_block);
              if(would_block)
                       return RC(sthread_t::stINUSE);
              w_assert2(_pp && bf->is_bf_page(_pp, true));
              _mode = bf->latch_mode(_pp);
        } else {
            W_IFTRACE(bf_fix="latch-upgrade";)
            /*
             *  We have already fixed the page, but we need
             *  to upgrade the latch mode.
             */
            bf->upgrade_latch(_pp, m); // might block
            w_assert2(_pp && bf->is_bf_page(_pp, true));
            _mode = bf->latch_mode(_pp);
            w_assert3(_mode >= m);
        }
    } else {
        /*
         * wrong page or no page at all
         */

        if (_pp)  {
            bf->unfix(_pp, false, _refbit);
            _pp = 0;
        } else {
            W_IFTRACE(bf_fix="bf-fix **********************************";)
        }


        if(condl) {
            W_DO( bf->conditional_fix(_pp, pid, ptag, m, 
                      (page_flags & t_virgin) != 0,  // no_read
                      ret_store_flags,
                      ignore_store_id, store_flags) );
                        w_assert2(_pp && bf->is_bf_page(_pp, true));
        } else {
            W_DO( bf->fix(_pp, pid, ptag, m, 
                      (page_flags & t_virgin) != 0,  // no_read
                      ret_store_flags,
                      ignore_store_id, store_flags) );
                        w_assert2(_pp && bf->is_bf_page(_pp, true));
        }

#if W_DEBUG_LEVEL > 2
        if( (page_flags & t_virgin) != 0  )  {
            if(
             (smlevel_0::operating_mode == smlevel_0::t_in_undo)
             ||
             (smlevel_0::operating_mode == smlevel_0::t_forward_processing)
            )  // do the assert below
            w_assert3(ret_store_flags != st_bad);
        }
#endif 
        _mode = bf->latch_mode(_pp);
        w_assert3(_mode >= m);
    }

    _refbit = refbit;
    
    w_assert3(_mode >= m);
    store_flags = ret_store_flags;

    w_assert2(is_fixed());
    w_assert2(_pp && bf->is_bf_page(_pp, true));
    INC_TSTAT(page_fix_cnt);  
    DBGTHRD(<<"page fix: tag " << ptag << " pid " << pid);
    
    // update the read-watermark for read-only xct
    xct_t *x = xct();
    if (x) {
        x->update_read_watermark(_pp->lsn);
    }
    
    return RCOK;
}

bool                         
page_p::is_latched_by_me() const
{
    return _pp ? bf->fixed_by_me(_pp) : false;
}

const latch_t *                         
page_p::my_latch() const
{
    return _pp ? bf->my_latch(_pp) : NULL;
}

bool                         
page_p::is_mine() const
{
    return _pp ? bf->is_mine(_pp) : false;
}

rc_t page_p::insert_expand_nolog(slotid_t idx, const cvec_t &vec)
{
    w_assert1(idx >= 0 && idx <= _pp->nslots);
    w_assert3 (is_consistent_space());
    // this shouldn't happen. the caller should have checked with check_space_for_insert()
    if (!check_space_for_insert(vec.size())) {
        return RC(smlevel_0::eRECWONTFIT);
    }

     //  Log has already been generated ... the following actions must succeed!
     // shift slot array. if we are inserting to the end (idx == nslots), do nothing
    if (idx != _pp->nslots)    {
        ::memmove(_pp->data + slot_sz * (idx + 1),
                _pp->data + slot_sz * (idx),
                (_pp->nslots - idx) * slot_sz);
    }

    //  Fill up the slots and data
    slot_offset_t new_record_head = _pp->record_head - align(vec.size());
    overwrite_slot(idx, new_record_head, vec.size());
    vec.copy_to(_pp->data + new_record_head);
    _pp->record_head = new_record_head;
    ++_pp->nslots;

    w_assert3 (is_consistent_space());
    return RCOK;
}

void page_p::append_nolog(const cvec_t &vec, bool ghost)
{
    w_assert3 (check_space_for_insert(vec.size()));
    w_assert5 (is_consistent_space());
    
    //  Fill up the slots and data
    slot_offset_t new_record_head = _pp->record_head - align(vec.size());
    overwrite_slot(_pp->nslots, ghost ? -new_record_head : new_record_head, vec.size());
    vec.copy_to(_pp->data + new_record_head);
    _pp->record_head = new_record_head;
    if (ghost) {
        ++_pp->nghosts;
    }
    ++_pp->nslots;

    w_assert5 (is_consistent_space());
}
void page_p::expand_rec(slotid_t idx, slot_length_t rec_len)
{
    w_assert1(idx >= 0 && idx < _pp->nslots);
    w_assert1(usable_space() >= align(rec_len));
    w_assert3(is_consistent_space());

    bool ghost = is_ghost_record(idx);
    smsize_t old_rec_len = tuple_size(idx);
    void* old_rec = tuple_addr(idx);
    slot_offset_t new_record_head = _pp->record_head - align(rec_len);
    overwrite_slot(idx, ghost ? -new_record_head : new_record_head, rec_len);
    _pp->record_head = new_record_head;
    ::memcpy (tuple_addr(idx), old_rec, old_rec_len); // copy the original data
#if W_DEBUG_LEVEL>0
    ::memset (old_rec, 0, old_rec_len); // clear old slot
#endif // W_DEBUG_LEVEL>0
    
    w_assert3 (is_consistent_space());
}
rc_t page_p::replace_expand_nolog(slotid_t idx, const cvec_t &tp)
{
    w_assert1(idx >= 0 && idx < _pp->nslots);
    slot_offset_t current_offset = tuple_offset(idx);
    smsize_t current_size = tuple_size(idx);
    if (align(tp.size()) <= align(current_size)) {
        // then simply overwrite
        overwrite_slot(idx, current_offset, tp.size());
        void* addr = tuple_addr(idx);
        tp.copy_to(addr);
        return RCOK;
    }
    // otherwise, have to expand
    if (usable_space() < align(tp.size())) {
        return RC(smlevel_0::eRECWONTFIT);
    }
    
    w_assert3(is_consistent_space());
    slot_offset_t new_record_head = _pp->record_head - align(tp.size());
    slot_offset_t new_offset = current_offset < 0 ? -new_record_head : new_record_head; // for ghost records
    overwrite_slot(idx, new_offset, tp.size());
    void* addr = tuple_addr(idx);
    tp.copy_to(addr);
    _pp->record_head = new_record_head;
    w_assert3 (is_consistent_space());    
    return RCOK;
}

bool page_p::check_space_for_insert(size_t rec_size) {
    size_t contiguous_free_space = usable_space();
    return contiguous_free_space >= align(rec_size) + slot_sz;
}

rc_t page_p::remove_shift_nolog(slotid_t idx)
{
    w_assert1(idx >= 0 && idx < _pp->nslots);
    w_assert3 (is_consistent_space());
    
    slot_offset_t removed_offset = tuple_offset(idx);
    slot_length_t removed_length = tuple_size(idx);

    // Shift slot array. if we are removing last (idx==nslots - 1), do nothing.
    if (idx < _pp->nslots - 1) {
        ::memmove(_pp->data + slot_sz * (idx),
            _pp->data + slot_sz * (idx + 1),
            (_pp->nslots - 1 - idx) * slot_sz);
    }
    --_pp->nslots;

    bool ghost = false;
    if (removed_offset < 0) {
        removed_offset = -removed_offset; // ghost record
        ghost = true;
    }
    if (_pp->record_head == removed_offset) {
        // then, we are pushing down the record_head. lucky!
        w_assert3 (is_consistent_space());
        _pp->record_head += removed_length;
    }
    
    if (ghost) {
        --_pp->nghosts;
    }

    w_assert3 (is_consistent_space());
    return RCOK;
}

rc_t page_p::defrag(slotid_t popped)
{
    w_assert1(popped >= -1 && popped < _pp->nslots);
    w_assert1 (xct()->is_sys_xct());
    w_assert1 (is_fixed());
    w_assert1 (latch_mode() == LATCH_EX);
    w_assert3 (is_consistent_space());
    
    //  Copy headers to scratch area.
    page_s scratch;
    char *scratch_raw = reinterpret_cast<char*>(&scratch);
    ::memcpy(scratch_raw, _pp, hdr_sz);
#ifdef ZERO_INIT
    ::memset(scratch.data, 0, data_sz);
#endif // ZERO_INIT
    
    //  Move data back without leaving holes
    slot_offset_t new_offset = data_sz;
    const slotid_t org_slots = _pp->nslots;
    vector<slotid_t> ghost_slots;
    slotid_t new_slots = 0;
    for (slotid_t i = 0; i < org_slots + 1; i++) {//+1 for popping
        if (i == popped)  continue;         // ignore this slot for now
        slotid_t slot = i;
        if (i == org_slots) {
            //  Move specified slot last
            if (popped < 0) {
                break;
            }
            slot = popped;
        }

        slot_offset_t offset = tuple_offset(slot);
        if (offset < 0) {
            // ghost record. reclaim it
            ghost_slots.push_back (slot);
            continue;
        }
        w_assert1(offset != 0);
        smsize_t len = tuple_size(slot);
        new_offset -= align(len);
        ::memcpy(scratch.data + new_offset, _pp->data + offset, len);

        slot_offset_t* offset_p = reinterpret_cast<slot_offset_t*>(scratch.data + slot_sz * new_slots);
        slot_length_t* length_p = reinterpret_cast<slot_length_t*>(scratch.data + slot_sz * new_slots + sizeof(slot_offset_t));
        *offset_p = new_offset;
        *length_p = len;

        ++new_slots;
    }

    scratch.nslots = new_slots;
    scratch.nghosts = 0;
    scratch.record_head = new_offset;

    // defrag doesn't need log if there were no ghost records
    if (ghost_slots.size() > 0) {
        // ghost records are not supported in other types.
        // REDO/UNDO of ghosting relies on BTree
        w_assert0(tag() == page_p::t_btree_p);
        W_DO (log_btree_ghost_reclaim(*this, ghost_slots));
    }
    
    // okay, apply the change!
    ::memcpy(_pp, scratch_raw, sizeof(page_s));
    set_dirty();

    w_assert3 (is_consistent_space());
    return RCOK;
}

void page_p::mark_ghost(slotid_t slot)
{
    w_assert0(tag() == page_p::t_btree_p);
    w_assert1(slot >= 0 && slot < nslots());
    slot_offset_t *offset = reinterpret_cast<slot_offset_t*>(_pp->data + slot_sz * slot);
    if (*offset < 0) {
        return; // already ghost. do nothing
    }
    w_assert1(*offset > 0);
    // reverse the sign to make it a ghost
    *offset = -(*offset);
    ++_pp->nghosts;
    set_dirty();
}

void page_p::unmark_ghost(slotid_t slot)
{
    w_assert0(tag() == page_p::t_btree_p);
    w_assert1(slot >= 0 && slot < nslots());
    slot_offset_t *offset = reinterpret_cast<slot_offset_t*>(_pp->data + slot_sz * slot);
    if (*offset > 0) {
        return; // already non-ghost. do nothing
    }
    w_assert1(*offset < 0);
    // reverse the sign to make it a non-ghost
    *offset = -(*offset);
    --_pp->nghosts;
    set_dirty();
}

bool page_p::pinned_by_me() const
{
    return bf->fixed_by_me(_pp);
}

w_rc_t
page_p::_copy(const page_p& p) 
{
    _refbit = p._refbit;
    _mode = p._mode;
    _pp = p._pp;
    if (_pp) {
        if( bf->is_bf_page(_pp)) {
            W_DO(bf->refix(_pp, _mode));
        }
    }
    return RCOK;
}

page_p& 
page_p::operator=(const page_p& p)
{
    if (this != &p)  {
        if(_pp) {
            if (bf->is_bf_page(_pp))   {
                bf->unfix(_pp, false, _refbit);
                _pp = 0;
            }
        }

        W_COERCE(_copy(p));
    }
    return *this;
}

void
page_p::upgrade_latch(latch_mode_t m)
{
    w_assert3(bf->is_bf_page(_pp));
    bf->upgrade_latch(_pp, m);
    _mode = bf->latch_mode(_pp);
}
void page_p::downgrade_latch()
{
    w_assert1(_mode == LATCH_EX);
    w_assert3(bf->is_bf_page(_pp));
    bf->downgrade_latch(_pp);
    _mode = LATCH_SH;
}

rc_t
page_p::upgrade_latch_if_not_block(bool& would_block)
{
    w_assert3(bf->is_bf_page(_pp));
    bf->upgrade_latch_if_not_block(_pp, would_block);
    if (!would_block) _mode = LATCH_EX;
    return RCOK;
}

void
page_p::page_usage(int& data_size, int& header_size, int& unused,
                   int& alignment, page_p::tag_t& t, slotid_t& no_slots)
{
    // returns space allocated for headers in this page
    // returns unused space in this page
    data_size = unused = alignment = 0;

    // space used for headers
    header_size = page_sz - data_sz;
    
    // calculate space wasted in data alignment
    for (int i=0 ; i<_pp->nslots; i++) {
        // if slot is not no-record slot
        if ( tuple_offset(i) != 0 ) {
            smsize_t len = tuple_size(i);
            data_size += len;
            alignment += int(align(len) - len);
        }
    }
    // unused space
    unused = page_sz - header_size - data_size - alignment;

    t        = tag();        // the type of page 
    no_slots = _pp->nslots;  // nu of slots in this page

    w_assert1(data_size + header_size + unused + alignment == page_sz);
}

bool page_p::is_consistent_space () const
{
    // this is not a part of check. should be always true.
    w_assert1((size_t) slot_sz * nslots() <= (size_t) _pp->record_head);
    
    // check overlapping records.
    // rather than using std::map, use array and std::sort for efficiency.
    // high-16bits=offset, low-16bits=len
    const slotid_t slot_cnt = nslots();
    int32_t *sorted_slots = new int32_t[slot_cnt];
    w_auto_delete_array_t<int32_t> sorted_slots_autodel (sorted_slots);
    for (slotid_t slot = 0; slot < slot_cnt; ++slot) {
        slot_offset_t offset = page_p::tuple_offset(slot);
        smsize_t len = page_p::tuple_size(slot);
        if (offset < 0) {
            sorted_slots[slot] = ((-offset) << 16) + len;// this means ghost slot. reverse the sign
        } else if (offset == 0) {
            // this means no-record slot. ignore it.
            sorted_slots[slot] = 0;
        } else {
            sorted_slots[slot] = (offset << 16) + len;
        }
    }
    std::sort(sorted_slots, sorted_slots + slot_cnt);

    bool first = true;
    size_t prev_end = 0;
    for (slotid_t slot = 0; slot < slot_cnt; ++slot) {
        if (sorted_slots[slot] == 0) {
            continue;
        }
        size_t offset = sorted_slots[slot] >> 16;
        size_t len = sorted_slots[slot] & 0xFFFF;
        if (offset < (size_t) _pp->record_head) {
            DBG(<<"the slot starting at offset " << offset <<  " is located before record_head " << _pp->record_head);
            w_assert1(false);
            return false;
        }
        if (offset + len > data_sz) {
            DBG(<<"the slot starting at offset " << offset <<  " goes beyond the the end of data area!");
            w_assert1(false);
            return false;
        }
        if (first) {
            first = false;
        } else {
            if (prev_end > offset) {
                DBG(<<"the slot starting at offset " << offset <<  " overlaps with another slot ending at " << prev_end);
                w_assert3(false);
                return false;
            }
        }
        prev_end = offset + len;
    }
    return true;
}

rc_t page_p::set_tobedeleted (bool log_it) {
    if ((_pp->page_flags & t_tobedeleted) == 0) {
        if (log_it) {
            W_DO(log_page_set_tobedeleted (*this));
        }
        _pp->page_flags ^= t_tobedeleted;
        set_dirty();
    }
    return RCOK;
}

void page_p::unset_tobedeleted() {
    if ((_pp->page_flags & t_tobedeleted) != 0) {
        _pp->page_flags ^= t_tobedeleted;
        // we don't need set_dirty() as it's always dirty if this is ever called
        // (UNDOing this means the page wasn't deleted yet by bufferpool, so it's dirty)
    }
}
