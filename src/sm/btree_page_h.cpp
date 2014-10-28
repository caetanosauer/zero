/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define BTREE_C

#include "sm_int_2.h"

#include "vec_t.h"
#include "btree_page_h.h"
#include "btree_impl.h"
#include "sm_du_stats.h"
#include "crash.h"
#include "w_key.h"
#include <string>
#include <algorithm>
#include "restart.h"


btrec_t& 
btrec_t::set(const btree_page_h& page, slotid_t slot) {
    FUNC(btrec_t::set);
    w_assert3(slot >= 0 && slot < page.nrecs());

    _elem.reset();
    
    if (page.is_leaf())  {
        page.get_key(slot, _key);
        smsize_t element_len;
        const char* element_data = page.element(slot, element_len, _ghost_record);
        _elem.put(element_data, element_len);
        _child = 0;
        _child_emlsn = lsn_t::null;
    } else {
        _ghost_record = false;
        page.get_key(slot, _key);
        _child = page.child_opaqueptr(slot);
        _child_emlsn = page.get_emlsn_general(GeneralRecordIds::from_slot_to_general(slot));
        // this might not be needed, but let's also add the _child value
        // to _elem.
        _elem.put(&_child, sizeof(_child));
    }

    return *this;
}

void btree_page_h::accept_empty_child(lsn_t new_lsn, shpid_t new_page_id, const bool f_redo) {
    // If called from Recovery, i.e. btree_norec_alloc_log::redo, do not check for
    // is_single_log_sys_xct(), the transaction flags are not setup properly

    // Base the checking on passed in parameter instead of system flag smlevel_0::in_recovery(),
    // because we will be doing on-demand redo/undo during recovery,
    // therefore the system flag itself is not sufficient to indicate the type of the caller
    if (false == f_redo)
        w_assert1(g_xct()->is_single_log_sys_xct());   

    w_assert1(new_lsn != lsn_t::null || !smlevel_0::logging_enabled);

    // Slight change in foster-parent, touching only foster link and chain-high.
    page()->btree_foster = new_page_id;
    page()->lsn          = new_lsn;

    // the only case we have to change parent's chain-high is when this page is the first
    // foster child. otherwise, chain_fence_high is unchanged.
    if (get_chain_fence_high_length() == 0) {
        // we COPY them because this operation changes this page itself
        w_keystr_t low, high;
        copy_fence_low_key(low);
        copy_fence_high_key(high);

        // it passed check_chance_for_norecord_split(), so no error should happen here.
        W_COERCE(replace_fence_rec_nolog_may_defrag(low, high, high, get_prefix_length()));
    }
}

rc_t btree_page_h::format_steal(lsn_t            new_lsn,         // LSN of the operation that creates this new page
                                const lpid_t&     pid,             // Destination page pid
                                shpid_t           root, 
                                int               l,               // Level of the destination page
                                shpid_t           pid0,            // Destination page pid0 value, non-leaf only
                                lsn_t             pid0_emlsn,      // Destination page emlsn value, non-leaf only 
                                shpid_t           foster,          // Page ID of the foster-child (if exist)
                                lsn_t             foster_emlsn,
                                const w_keystr_t& fence_low,       // Low fence key of the destination page
                                const w_keystr_t& fence_high,      // Hig key of the destination page, confusing naming
                                                                   //this is actually the foster key
                                const w_keystr_t& chain_fence_high,// High fence chian key, does not exist if no foster chain
                                                                   // this is the high fence key for all foster child pages
                                bool              log_it,          // True if log the log_page_img_format log record
                                btree_page_h*     steal_src1,      // Source 1 to get records from
                                int               steal_from1,
                                int               steal_to1,
                                btree_page_h*     steal_src2,      // Source 2 to get records from
                                int               steal_from2,
                                int               steal_to2,
                                bool              steal_src2_pid0,
                                const bool        full_logging,    // True if doing full logging for record movement
                                const bool        log_src_1,       // Use only if full_logging = true
                                                                   // True if log movements from src1, used for
                                                                   // page rebalance
                                                                   // False if log movements from src2, used
                                                                   // for page merge
                                const bool        ghost)           // Should the fence key record be a ghost
{
    // Full logging for all record movements only if using page driven REDO operation
    // and we do not want to log the log_page_img_format log record
    if (true == full_logging)
    {
       w_assert1(true == restart_m::use_redo_full_logging_restart());
       w_assert1(false == log_it);
    }

    // Note that the method receives a copy, not reference, of pid/lsn here.
    // pid might point to a part of this page itself!   
    // Initialize the whole image of the destination page page as an empty page.
    // it sets the fence keys (prefix, low and high, all in one slot, slot 0)
    // The _init inserts into slot 0 which contains the low, high (confusing nameing, 
    // this is actually the foster key) and chain_high_fence (actually the high fence) keys,
    // but do not log it, since the actual record movement will move all the records
    _init(new_lsn, pid, root, pid0, pid0_emlsn, foster, foster_emlsn,
          l, fence_low, fence_high, chain_fence_high, ghost);

    // steal records from old page
    if (steal_src1) {
        _steal_records (steal_src1, steal_from1, steal_to1, 
              ((true == full_logging) && (true == log_src_1))? true : false);  // Turn on full logging if need to log src 1
    }
    if (steal_src2_pid0) {
        // For non-leaf page only
        w_assert1(steal_src2);
        w_assert1(is_node());
        w_assert1(steal_src2->pid0() != pid0);
        w_assert1(steal_src2->pid0() != 0);

        // before stealing regular records from src2, steal it's pid0:
        cvec_t       stolen_key(steal_src2->get_fence_low_key() + page()->btree_prefix_length,
                            steal_src2->get_fence_low_length() - page()->btree_prefix_length);
        poor_man_key poormkey    = _extract_poor_man_key(stolen_key);
        shpid_t      stolen_pid0 = steal_src2->pid0();
        lsn_t        stolen_pid0_emlsn = steal_src2->get_pid0_emlsn();
        cvec_t v;
        rc_t rc;
        _pack_node_record(v, stolen_key, stolen_pid0_emlsn.data());
        if ((true == full_logging) && (false == log_src_1))
        {
            // Need to log for src2, this is for page merge and non-leaf pages only
            // Log deletion from source(steal_src2), and insertion into target (page())
            // This is the low fence key from source(steal_src2) before the merge
            // which become a regular record in the merged page (page())

            // Log the insertion into destination first
            w_keystr_t keystr;   // Used for insertion log record if full logging
            // For logging purpose, it is the whole key, including prefix and PMNK
            cvec_t  whole_key(steal_src2->get_fence_low_key(), steal_src2->get_fence_low_length());  
            if (true == keystr.copy_from_vec(whole_key))
            {
                vec_t el;
                cvec_t empty_key;
                _pack_node_record(el, empty_key, stolen_pid0_emlsn.data());
                rc = log_btree_insert_nonghost(*this, keystr, el, true /*is_sys_txn*/);   // key: original key including prefix
                                                                                         // el: non-key portion only
            }
            else
            {
                W_FATAL_MSG(fcOUTOFMEMORY, << "Failed to generate log_btree_insert_nonghost log record due to OOM");            
            }

            // Now log the deletion from source next
            vector<slotid_t> slots;
            slots.push_back(0);  // Low fence key is in slot 0
            rc = log_btree_ghost_mark(*steal_src2, slots, true /*is_sys_txn*/);
            if (rc.is_error()) 
            {
                W_FATAL_MSG(fcINTERNAL, << "Failed to generate log_btree_ghost_mark log record during a full logging system transaction");
            }           
        }
        // Now the actual movement
        if (!page()->insert_item(nrecs()+1, false /*ghost*/, poormkey, stolen_pid0, v)) {
            w_assert0(false);
        }
    }

    if (steal_src2) {
        _steal_records (steal_src2, steal_from2, steal_to2, 
              ((true == full_logging) && (false == log_src_1))? true : false);  // Turn on full logging if need to log src 2
    }

    // log as one record
    if (log_it) {
        W_DO(log_page_img_format(*this));
        w_assert1(lsn().valid() || !smlevel_0::logging_enabled);
    }

    // This is the only place where a page format log record is being generated, 
    // although the function can be called from multiple places (mostly from B-tree functions)
    // the _init() function only set the last write LSN, but not the initial dirty LSN in page cb
    // For system crash recovery purpose, we need the initial dirty LSN to trace back to the
    // page format log record so everything can be REDO
    // Set the _rec_lsn using the new_lsn (which is the last write LSN) if _rec_lsn is later than 
    // new_lsn.
    smlevel_0::bf->set_initial_rec_lsn(pid, new_lsn, smlevel_0::log->curr_lsn());

    return RCOK;
}

void btree_page_h::_steal_records(btree_page_h* steal_src,
                                  int           steal_from,
                                  int           steal_to,
                                  const bool    full_logging) {  // True if turn on full logging for the record
                                                                 // movement, both deletion and insertion
    w_assert2(steal_src);
    w_assert2(steal_from <= steal_to);
    w_assert2(steal_from >= 0);
    w_assert2(steal_to <= steal_src->nrecs());
    w_rc_t rc = RCOK;
    w_keystr_t keystr;   // Used for insertion log record if full logging

    if (true == full_logging)
    {
        // Currently using full logging only if we are using page driven REDO recovery
        // The full logging flag is on when we are moving new records into destination page
        // not when we are re-copy existing (old) records into destination page
        w_assert1(true == restart_m::use_redo_full_logging_restart());
        DBGOUT3( << "btree_page_h::_steal_records for a system transaction - need full logging");
    }
    else
    {
        // Eigher minimal logging or full logging but caller asked not to log because no need for the current movement
        DBGOUT3( << "btree_page_h::_steal_records for a system transaction - either minimal logging or skip full logging");
    }

    key_length_t new_prefix_length = get_prefix_length();
    for (int i = steal_from; i < steal_to; ++i) {
        // get full uncompressed key from src slot #i into key:
        cvec_t key(steal_src->get_prefix_key(), steal_src->get_prefix_length());
        size_t      trunc_key_length;
        const char* trunc_key_data;
        if (is_leaf()) {
            trunc_key_data = steal_src->_leaf_key_noprefix(i, trunc_key_length);
        } else {
            trunc_key_data = steal_src->_node_key_noprefix(i, trunc_key_length);
        }
        key.put(trunc_key_data, trunc_key_length);

        // split off part after new_prefix_length into new_trunc_key:
        cvec_t dummy, new_trunc_key;
        key.split(new_prefix_length, dummy, new_trunc_key);

        cvec_t         v;
        pack_scratch_t v_scratch; // this needs to stay in scope until v goes out of scope...
        shpid_t        child;

        // If ask for full logging, generate record movement log records before
        // each insertion
        // Caller 'format_steal' is constructing a new page by copying existing
        // records from 1 page into 2 pages (rebalance) or from 2 pages 
        // into 1 page (merge).  We only need to log the record movement for
        // the records actually got moved to a different page.
        //
        // For each record movement, log both record deletion (ghost) from 
        // old page (source, steal_src) and record insertion into 
        // new page (destination, page())
        //
        // Note that there is no actual 'delete (ghost)' operation, this is 
        // because we are copying the needed records into new page, 
        // skip the not needed records, which is the same effect as delete 
        // from the original page, but we need to log the delete (ghost) operation
        // although there is no actual operation

        if (true == full_logging)
        {
            // Construct the key field for the insertion log record below, there is a
            // different behavior whether the page is a leaf or non-leaf node
            // For logging purpose, it is the whole key including prefix and PMNK
            // Log insertion first and then deletion
            if (false == keystr.copy_from_vec(key))
            {
                W_FATAL_MSG(fcOUTOFMEMORY, << "Failed to generate log_btree_insert_nonghost log record due to OOM");
            }
            else
            {
                DBGOUT3( << "btree_page_h::_steal_records, log insertion, key: " << keystr);
            }
        }

        if (is_leaf()) 
        {
            smsize_t data_length;
            bool is_ghost;
            const char* data = steal_src->element(i, data_length, is_ghost);
            _pack_leaf_record(v, v_scratch, new_trunc_key, data, data_length);
            child = 0;

            if (true == full_logging)
            {
                // Log the insertion into new page (leaf)
                vec_t el;
                el.put(data, data_length);
                rc = log_btree_insert_nonghost(*this, keystr, el, true /*is_sys_txn*/);   // key: original key including prefix
                                                                                          // el: non-key portion only
                // Clear the key string so it is ready for the next record
                keystr.clear();
                if (rc.is_error()) 
                {
                    W_FATAL_MSG(fcINTERNAL, 
                        << "Failed to generate log_btree_insert_nonghost log record for a leaf page during a full logging system transaction");
                }                                                                 
            }
        }
        else 
        {
            // Non-leaf node
            // EMLSN is after the key data
            const lsn_t* emlsn_ptr = reinterpret_cast<const lsn_t*>(
                trunc_key_data + trunc_key_length);
            _pack_node_record(v, new_trunc_key, *emlsn_ptr);
            child = steal_src->child_opaqueptr(i);

            if (true == full_logging)
            {
                // Log the insertion into new page (non-leaf)
                vec_t el;
                el.put(emlsn_ptr, sizeof(lsn_t));
                rc = log_btree_insert_nonghost(*this, keystr, el, true /*is_sys_txn*/);   // key: original key including prefix
                                                                                        // el: non-key portion only                
                // Clear the key string so it is ready for the next record
                keystr.clear();
                if (rc.is_error()) 
                {
                    W_FATAL_MSG(fcINTERNAL,
                        << "Failed to generate log_btree_insert_nonghost log record for a non-leaf page during a full logging system transaction");
                }                                                                 
            }
        }

        if (true == full_logging)
        {
            // Log the deletion from src page next
            // No difference between leaf or non-leaf page
            vector<slotid_t> slots;
            slots.push_back(i);    // Current 'i' is the slot for the deleted record
            rc = log_btree_ghost_mark(*steal_src, slots, true /*is_sys_txn*/);
            if (rc.is_error()) 
            {
                W_FATAL_MSG(fcINTERNAL, << "Failed to generate log_btree_ghost_mark log record during a full logging system transaction");
            }
        }

        // Now the actual insertion into the new page
        if (!page()->insert_item(nrecs()+1, steal_src->is_ghost(i), 
                                 _extract_poor_man_key(new_trunc_key), 
                                 child, v)) {
            w_assert0(false);
        }

        w_assert3(is_consistent());
        w_assert5(_is_consistent_keyorder());
    }
}

rc_t btree_page_h::init_fence_keys(
            const bool set_low, const w_keystr_t &low,               // Low fence key
            const bool set_high, const w_keystr_t &high,             // High key (foster)
            const bool set_chain, const w_keystr_t &chain_high,      // Chain high fence key
            const bool set_pid0, const shpid_t new_pid0,             // pid0, non-leaf node only
            const bool set_emlsn, const lsn_t new_pid0_emlsn,        // emlan,  non-leaf node only
            const bool set_foster, const shpid_t foster_pid0,        // foster page id 
            const bool set_foster_emlsn, const lsn_t foster_emlsn,   // foster page emlsn
            const int remove_count)                                  // Number of records to be removed 
                                                                     // Used only if reset fence key
{
    // Reset the fence key and other information in an existing page, 
    // do not change existing record data in the page and no change to number of records
    // This is a special function currently only used by Single-Page-Recovery REDO operation for 
    // page rebalance and page merge when full logging is on and the target page
    // contains data already (not an empty page)

    bool update_fence = false;

    if (true == set_pid0)
    {
        // This information is for non-leaf page only
        page()->btree_pid0 = new_pid0;       
    }
    if (true == set_emlsn)
    {
        // This information is for non-leaf page only
        page()->btree_pid0_emlsn = new_pid0_emlsn;        
    }
    if (true == set_foster)
    {
        // Foster page id
        page()->btree_foster = foster_pid0;        
    }
    if (true == set_foster_emlsn)
    {
        // Foster page emlsn
        page()->btree_foster_emlsn = foster_emlsn;
    }

    w_keystr_t low_fence;
    w_keystr_t high_key;
    w_keystr_t chain_fence_key;

    if (false == set_low)
    {
        // Get the existing low fence key
        copy_fence_low_key(low_fence);
    }
    else
    {
        // Get the low fence key from input parameter
        low_fence.construct_from_keystr(low.buffer_as_keystr(), low.get_length_as_keystr());
        update_fence = true;
    }

    if (false == set_high)
    {
        // Get the existing high key (foster)
        copy_fence_high_key(high_key);

    }
    else
    {
        // Get the high key from input parameter    
        high_key.construct_from_keystr(high.buffer_as_keystr(), high.get_length_as_keystr());
        update_fence = true;
    }

    if (false == set_chain)
    {
        // Get the existing chain high fence key
        copy_chain_fence_high_key(chain_fence_key);
    }
    else
    {
        chain_fence_key.construct_from_keystr(chain_high.buffer_as_keystr(), chain_high.get_length_as_keystr());
        update_fence = true;
    }

    if (false == update_fence)
        return RCOK;

    // Set the page dirty
    set_dirty();

    // Delete records from page, number_of_items() is the actual 
    // record count including the fence key record which is not an actual record
    // 'remove_count' is the number of records to remove from the page
    // due to page rebalance, this is because we are resetting the fence keys
    // which makes some of the existing records out-of-bound and they
    // need to be remvoed
    w_assert1(page()->number_of_items() > remove_count);
    page()->remove_items(remove_count, high);  // Remove items, it affects item count but not ghost count

    // Prepare for updating the fence key slot
    cvec_t fences;
    size_t prefix_len = _pack_fence_rec(fences, low_fence, high_key, chain_fence_key, -1);
    w_assert1(prefix_len <= low_fence.get_length_as_keystr());
    w_assert1(prefix_len <= max_key_length);
    page()->btree_prefix_length = (int16_t) prefix_len;

    // Update the original fence key slot which is the first slot
    DBGOUT3( << "btree_page_h::init_fence_keys - new fence keys.  Low: " 
             << low_fence << ", high key: " << high_key << ", chain high: " << chain_fence_key);    
    return replace_fence_rec_nolog_may_defrag(low_fence, high_key, chain_fence_key, prefix_len);
}

rc_t btree_page_h::norecord_split (shpid_t foster, lsn_t foster_emlsn,
                                const w_keystr_t& fence_high, const w_keystr_t& chain_fence_high) {
    w_assert1(compare_with_fence_low(fence_high) > 0);
    w_assert1(compare_with_fence_low(chain_fence_high) > 0);

    w_keystr_t fence_low;
    copy_fence_low_key(fence_low);
    key_length_t new_prefix_len = fence_low.common_leading_bytes(fence_high);

    if (new_prefix_len > get_prefix_length() + 3) { // this +3 is arbitrary
        // then, let's defrag this page to compress keys
        generic_page scratch;
        ::memcpy (&scratch, _pp, sizeof(scratch));
        btree_page_h scratch_p;
        scratch_p.fix_nonbufferpool_page(&scratch);
        W_DO(format_steal(scratch_p.lsn(),
                          scratch_p.pid(), scratch_p.btree_root(), scratch_p.level(),
                          scratch_p.pid0(), scratch_p.get_pid0_emlsn(),
                          foster, foster_emlsn,
                          fence_low, fence_high, chain_fence_high,
                          false, // don't log it
                          &scratch_p, 0, scratch_p.nrecs()
        ));
        update_initial_and_last_lsn(scratch.lsn); // format_steal() also clears lsn, so recover it from the copied page
    } else {
        // otherwise, just sets the fence keys and headers
        //sets new fence
        rc_t rc = replace_fence_rec_nolog_may_defrag(fence_low, fence_high, chain_fence_high,
            new_prefix_len);
        w_assert1(rc.err_num() != eRECWONTFIT);// then why it passed check_chance_for_norecord_split()?
        w_assert1(!rc.is_error());

        //updates headers
        page()->btree_foster                        = foster;
        page()->btree_foster_emlsn                  = foster_emlsn;
        page()->btree_consecutive_skewed_insertions = 0; // reset this value too.
    }
    return RCOK;
}

inline int btree_page_h::_compare_slot_with_key(int slot, const void* key_noprefix, size_t key_len, poor_man_key key_poor) const {
    // fast path using poor_man_key's:
    int result = _poor(slot) - (int)key_poor;
    if (result != 0) {
        w_assert1((result<0) == (_compare_key_noprefix(slot, key_noprefix, key_len)<0));
        return result;
    }

    // slow path:
    return _compare_key_noprefix(slot, key_noprefix, key_len);
}
inline int btree_page_h::_robust_compare_slot_with_key(int slot, const void* key_noprefix, size_t key_len, poor_man_key key_poor) const {
    // fast path using poor_man_key's:
    int result = page()->robust_item_poor(slot+1) - (int)key_poor;
    if (result != 0) {
        return result;
    }

    // slow path:
    return _robust_compare_key_noprefix(slot, key_noprefix, key_len);
}


void
btree_page_h::search(const char *key_raw, size_t key_raw_len,
                     bool& found_key, slotid_t& return_slot) const {
    w_assert1((uint) get_prefix_length() <= key_raw_len);
    w_assert1(::memcmp(key_raw, get_prefix_key(), get_prefix_length()) == 0);

    int number_of_records = nrecs();
    int prefix_length     = get_prefix_length();

    const void* key_noprefix  = key_raw     + prefix_length;
    size_t      key_len       = key_raw_len - prefix_length;
    
    poor_man_key poormkey = _extract_poor_man_key(key_noprefix, key_len);


    /*
     * Binary search.
     */

    found_key = false;
    int low = -1, high = number_of_records;
    // LOOP INVARIANT: low < high AND slot_key(low) < key < slot_key(high)
    // where slots before real ones hold -infinity and ones after hold +infinity

    // [optional] check the last record (high-1) if it exists to speed-up sorted insert:
    if (high > 0) {
        int d = _compare_slot_with_key(high-1, key_noprefix, key_len, poormkey);
        if (d < 0) { // search key bigger than highest slot
            return_slot = high;
            return;
        } else if (d == 0) {
            found_key   = true;
            return_slot = high-1;
            return;
        }
        high--;
    }

#if 0
    // [optional] check the first record (0) if it exists to speed-up reverse sorted insert:
    if (high > 0) {
        int d = _compare_slot_with_key(0, key_noprefix, key_len, poormkey);
        if (d > 0) { // search key lower than lowest slot
            return_slot = 0;
            return;
        } else if (d == 0) {
            found_key   = true;
            return_slot = 0;
            return;
        }
        low++;
    }
#endif
    
    while (low+1 < high) {
        int mid = (low + high) / 2;
        w_assert1(low<mid && mid<high);
        int d = _compare_slot_with_key(mid, key_noprefix, key_len, poormkey);
        if (d < 0) {        // search key after slot
            low = mid;
        } else if (d > 0) { // search key before slot
            high = mid;
        } else {
            found_key   = true;
            return_slot = mid;
            w_assert1(mid>=0 && mid<number_of_records);
            return;
        }
    }
    w_assert1(low+1 == high);
    return_slot = high;
    w_assert1(high>=0 && high<=number_of_records);
}

void
btree_page_h::robust_search(const char *key_raw, size_t key_raw_len,
                     bool& found_key, slotid_t& return_slot) const {
    int number_of_records = page()->robust_number_of_items() - 1;
    int prefix_length     = ACCESS_ONCE(page()->btree_prefix_length);

    if (number_of_records < 0 || prefix_length < 0 || prefix_length > (int)key_raw_len) {
        found_key   = false;
        return_slot = 0;
        return;
    }
    w_assert1((uint) prefix_length <= key_raw_len);

    const void* key_noprefix  = key_raw     + prefix_length;
    size_t      key_len       = key_raw_len - prefix_length;
    
    poor_man_key poormkey = _extract_poor_man_key(key_noprefix, key_len);


    /*
     * Binary search.
     */

    found_key = false;
    int low = -1, high = number_of_records;
    // LOOP INVARIANT: low < high AND slot_key(low) < key < slot_key(high)
    // where slots before real ones hold -infinity and ones after hold +infinity

    // [optional] check the last record (high-1) if it exists to speed-up sorted insert:
    if (high > 0) {
        int d = _robust_compare_slot_with_key(high-1, key_noprefix, key_len, poormkey);
        if (d < 0) { // search key bigger than highest slot
            return_slot = high;
            return;
        } else if (d == 0) {
            found_key   = true;
            return_slot = high-1;
            return;
        }
        high--;
    }

#if 0
    // [optional] check the first record (0) if it exists to speed-up reverse sorted insert:
    if (high > 0) {
        int d = _robust_compare_slot_with_key(0, key_noprefix, key_len, poormkey);
        if (d > 0) { // search key lower than lowest slot
            return_slot = 0;
            return;
        } else if (d == 0) {
            found_key   = true;
            return_slot = 0;
            return;
        }
        low++;
    }
#endif
    
    while (low+1 < high) {
        int mid = (low + high) / 2;
        w_assert1(low<mid && mid<high);
        int d = _robust_compare_slot_with_key(mid, key_noprefix, key_len, poormkey);
        if (d < 0) {        // search key after slot
            low = mid;
        } else if (d > 0) { // search key before slot
            high = mid;
        } else {
            found_key   = true;
            return_slot = mid;
            w_assert1(mid>=0 && mid<number_of_records);
            return;
        }
    }
    w_assert1(low+1 == high);
    return_slot = high;
    w_assert1(high>=0 && high<=number_of_records);
}

void btree_page_h::search_node(const w_keystr_t& key,
                               slotid_t&         return_slot) const {
    w_assert1(!is_leaf());
    FUNC(btree_page_h::_search_node);

    bool found_key; 
    search(key, found_key, return_slot);
    if (!found_key) {
        return_slot--;
    }
}


void btree_page_h::_update_btree_consecutive_skewed_insertions(slotid_t slot) {
    if (nrecs() == 0) {
        return;
    }
    int16_t val = page()->btree_consecutive_skewed_insertions;
    if (slot == 0) {
        // if left-most insertion, start counting negative value (or decrement further)
        if (val >= 0) {
            val = -1;
        } else {
            --val;
        }
    } else if (slot == nrecs()) {
        // if right-most insertion, start counting positive value (or increment further)
        if (val <= 0) {
            val = 1;
        } else {
            ++val;
        }
    } else {
        val = 0;
    }
    // to prevent overflow
    if (val < -100) val = -100;
    if (val > 100) val = 100;
    page()->btree_consecutive_skewed_insertions = val;
}

rc_t btree_page_h::insert_node(const w_keystr_t &key, slotid_t slot, shpid_t child,
    const lsn_t& child_emlsn) {
    FUNC(btree_page_h::insert);
    
    w_assert1(is_node());
    w_assert1(slot >= 0 && slot <= nrecs()); // <= intentional to allow appending
    w_assert1(child);
    w_assert3(is_consistent(true, false));

#if W_DEBUG_LEVEL > 1
    if (slot == 0) {
        w_assert2 (pid0()); // pid0 always exists
        w_assert2 (compare_with_fence_low(key) >= 0);
    } else {
        btrec_t rt (*this, slot - 1); // prev key
        w_assert2 (key.compare(rt.key()) > 0);
    }
    if (slot < nrecs()) {
        btrec_t rt (*this, slot); // (after insert) next key
        w_assert2 (key.compare(rt.key()) < 0);
    }
#endif // W_DEBUG_LEVEL

    // Update btree_consecutive_skewed_insertions.  This is just statistics and not logged.
    _update_btree_consecutive_skewed_insertions (slot);

    size_t       klen          = key.get_length_as_keystr();
    key_length_t prefix_length = get_prefix_length();  // length of prefix of inserted tuple
    w_assert1(prefix_length <= klen);
    cvec_t trunc_key((const char*)key.buffer_as_keystr()+prefix_length, klen-prefix_length);
    poor_man_key poormkey = _extract_poor_man_key(trunc_key);

    vec_t v;
    _pack_node_record(v, trunc_key, child_emlsn.data());
    // we don't log it. btree_impl::adopt() does the logging
    if (!page()->insert_item(slot+1, false, poormkey, child, v)) {
        // This shouldn't happen; the caller should have checked with check_space_for_insert_for_node():
        return RC(eRECWONTFIT);
    }

    w_assert3 (is_consistent(true, false));
    w_assert5 (is_consistent(true, true));

    return RCOK;
}

rc_t btree_page_h::replace_fence_rec_nolog_may_defrag(const w_keystr_t& low,
    const w_keystr_t& high, const w_keystr_t& chain, int new_prefix_length) {
    rc_t rc = replace_fence_rec_nolog_no_defrag(low, high, chain, new_prefix_length);
    if (rc.is_error()) {
        // if eRECWONTFIT, try to defrag the page to get the space.
        w_assert1(rc.err_num() == eRECWONTFIT);
        rc = defrag();
        w_assert1(!rc.is_error());
        rc = replace_fence_rec_nolog_no_defrag(low, high, chain, new_prefix_length);
        if (rc.is_error()) {
            return rc;
        }
    }
    return RCOK;
}

rc_t btree_page_h::replace_fence_rec_nolog_no_defrag(const w_keystr_t& low,
                                           const w_keystr_t& high, 
                                           const w_keystr_t& chain, int new_prefix_len) {
    w_assert1(page()->number_of_items() > 0);

    cvec_t fences;
    int prefix_len = _pack_fence_rec(fences, low, high, chain, new_prefix_len);
    w_assert1(prefix_len == get_prefix_length());

    if (!page()->replace_item_data(0, 0, fences)) {
        return RC(eRECWONTFIT);
    }
    page()->btree_fence_low_length        = (int16_t) low.get_length_as_keystr();
    page()->btree_fence_high_length       = (int16_t) high.get_length_as_keystr();
    page()->btree_chain_fence_high_length = (int16_t) chain.get_length_as_keystr();

    w_assert1 (page()->item_length(0) == (key_length_t) fences.size());
    w_assert3(page()->_items_are_consistent());
    return RCOK;
}


rc_t btree_page_h::remove_shift_nolog(slotid_t slot) {
    w_assert1(slot >= 0 && slot < nrecs());

    page()->delete_item(slot + 1);
    return RCOK;
}

bool btree_page_h::_is_enough_spacious_ghost(const w_keystr_t &key, slotid_t slot,
                                             const cvec_t&     el) {
    w_assert2(is_leaf());
    w_assert2(is_ghost(slot));

    size_t needed_data = _predict_leaf_data_length(key.get_length_as_keystr() - get_prefix_length(), el.size());

    return page()->predict_item_space(needed_data) <= page()->item_space(slot+1);
}

rc_t btree_page_h::replace_ghost(const w_keystr_t &key,
                                 const cvec_t &elem) {
    w_assert2( is_fixed());
    w_assert2( is_leaf());

    // log FIRST. note that this might apply the deferred ghost creation too.
    // so, this cannot be done later than any of following
    W_DO (log_btree_insert (*this, key, elem, false /*is_sys_txn*/));

    // which slot to replace?
    bool found;
    slotid_t slot;
    search(key, found, slot);
    w_assert0 (found);
    w_assert1 (is_ghost(slot));
#if W_DEBUG_LEVEL > 2
    btrec_t rec (*this, slot);
    w_assert3 (rec.key().compare(key) == 0);
#endif // W_DEBUG_LEVEL > 2

    if (!page()->replace_item_data(slot+1, _element_offset(slot), elem)) {
        w_assert1(false); // should not happen because ghost should have had enough space
    }

    page()->unset_ghost(slot + 1);
    return RCOK;
}

rc_t btree_page_h::replace_el_nolog(slotid_t slot, const cvec_t &elem) {
    w_assert2( is_fixed());
    w_assert2( is_leaf());
    w_assert1(!is_ghost(slot));
    
    if (!page()->replace_item_data(slot+1, _element_offset(slot), elem)) {
        return RC(eRECWONTFIT);
    }
    return RCOK;
}

void btree_page_h::overwrite_el_nolog(slotid_t slot, smsize_t offset,
                                      const char *new_el, smsize_t elen) {
    w_assert2( is_fixed());
    w_assert2( is_leaf());
    w_assert1 (!is_ghost(slot));

    size_t data_offset = _element_offset(slot);
    w_assert1(data_offset+offset+elen <= page()->item_length(slot+1));

    ::memcpy(page()->item_data(slot+1)+data_offset+offset, new_el, elen);
}

void btree_page_h::reserve_ghost(const char *key_raw, size_t key_raw_len, size_t element_length) {
    w_assert1 (is_leaf()); // ghost only exists in leaf

    int16_t prefix_len       = get_prefix_length();
    int     trunc_key_length = key_raw_len - prefix_len;
    size_t  data_length      = _predict_leaf_data_length(trunc_key_length, element_length);

    w_assert1(check_space_for_insert_leaf(trunc_key_length, element_length));

    // where to insert?
    bool     found;
    slotid_t slot;
    search(key_raw, key_raw_len, found, slot);
    w_assert1(!found); // this is unexpected!
    if (found) { // but can go on..
        return;
    }

    w_assert1(slot >= 0 && slot <= nrecs());

    // update btree_consecutive_skewed_insertions. this is just statistics and not logged.
    _update_btree_consecutive_skewed_insertions(slot);

#if W_DEBUG_LEVEL>1
    w_keystr_t key;
    key.construct_from_keystr(key_raw, key_raw_len);
    w_assert1(compare_with_fence_low(key) >= 0);
    w_assert1(compare_with_fence_high(key) < 0);

    // verify search worked properly, no record with that key:
    if (slot > 0) {
        w_assert1(_compare_key_noprefix(slot-1,key_raw+prefix_len,trunc_key_length) < 0);
    }
    if (slot < nrecs()) {
        w_assert1(_compare_key_noprefix(slot,key_raw+prefix_len,trunc_key_length) > 0);
    }
#endif // W_DEBUG_LEVEL>1
    
    cvec_t trunc_key(key_raw + prefix_len, trunc_key_length);
    poor_man_key poormkey = _extract_poor_man_key(trunc_key);

    if (!page()->insert_item(slot+1, true, poormkey, 0, data_length)) {
        w_assert0(false);
    }

    // make a dummy record that has the desired length:
    cvec_t         dummy;
    pack_scratch_t dummy_scratch;
    _pack_leaf_record_prefix(dummy, dummy_scratch, trunc_key);
    dummy.copy_to(page()->item_data(slot+1));

    w_assert3(_poor(slot) == poormkey);
    w_assert3(page()->item_length(slot+1) == data_length);
}
void btree_page_h::insert_nonghost(const w_keystr_t &key, const cvec_t &elem) {
    w_assert1 (is_leaf());

    w_assert1(compare_with_fence_low(key) >= 0);
    w_assert1(compare_with_fence_high(key) < 0);

    int16_t prefix_len       = get_prefix_length();
    int     trunc_key_length = key.get_length_as_keystr() - prefix_len;

    w_assert1(check_space_for_insert_leaf(trunc_key_length, elem.size()));

    // where to insert?
    bool     found;
    slotid_t slot;
    search(key, found, slot);
    if (found) {
        // because of logical UNDO, this might happen. in this case, we just reuse the ghost.
        w_assert1 (is_ghost(slot));
#if W_DEBUG_LEVEL > 2
        btrec_t rec (*this, slot);
        w_assert3 (rec.key().compare(key) == 0);
#endif // W_DEBUG_LEVEL > 2

        if (!page()->replace_item_data(slot+1, _element_offset(slot), elem)) {
            w_assert1(false); // should not happen because ghost should have had enough space
        }

        page()->unset_ghost(slot + 1);
        return;
    }
    w_assert1(slot >= 0 && slot <= nrecs());

    // update btree_consecutive_skewed_insertions. this is just statistics and not logged.
    _update_btree_consecutive_skewed_insertions(slot);

    cvec_t trunc_key(reinterpret_cast<const char*>(key.buffer_as_keystr()) + prefix_len,
                     trunc_key_length);
    poor_man_key poormkey = _extract_poor_man_key(trunc_key);

    cvec_t leaf_record;
    pack_scratch_t leaf_scratch;
    _pack_leaf_record_prefix(leaf_record, leaf_scratch, trunc_key);
    leaf_record.put(elem);
    if (!page()->insert_item(slot+1, false, poormkey, 0, leaf_record)) {
        w_assert0(false);
    }

    w_assert3(_poor(slot) == poormkey);
}

void btree_page_h::mark_ghost(slotid_t slot) {
    w_assert1(!page()->is_ghost(slot+1));
    page()->set_ghost(slot+1);
    set_dirty();
}

void btree_page_h::unmark_ghost(slotid_t slot) {
    w_assert1(page()->is_ghost(slot+1));
    page()->unset_ghost(slot+1);
    set_dirty();
}


bool btree_page_h::check_space_for_insert_leaf(const w_keystr_t& trunc_key,
                                               const cvec_t&     el) {
    return check_space_for_insert_leaf(trunc_key.get_length_as_keystr(), el.size());
}
bool btree_page_h::check_space_for_insert_leaf(size_t trunc_key_length, size_t element_length) {
    w_assert1 (is_leaf());
    size_t data_length = _predict_leaf_data_length(trunc_key_length, element_length);
    return btree_page_h::_check_space_for_insert(data_length);
}
bool btree_page_h::check_space_for_insert_node(const w_keystr_t& key) {
    w_assert1 (is_node());
    size_t data_length = key.get_length_as_keystr() + sizeof(lsn_t);
    return btree_page_h::_check_space_for_insert(data_length);
}

bool btree_page_h::check_chance_for_norecord_split(const w_keystr_t& key_to_insert) const {
    if (!is_insertion_extremely_skewed_right()) {
        return false; // not a good candidate for norecord-split
    }
    if (nrecs() == 0) {
        return false;
    }
    if (usable_space() > used_space() * 3 / nrecs()
        && usable_space() > SM_PAGESIZE / 10
    ) {
        return false; // too early to split
    }

    const char* key_to_insert_raw = (const char*) key_to_insert.buffer_as_keystr();
    int key_to_insert_len = key_to_insert.get_length_as_keystr();
    int prefix_len = get_prefix_length();
    w_assert1(key_to_insert_len >= prefix_len && ::memcmp(get_prefix_key(), key_to_insert_raw, prefix_len) == 0); // otherwise why to insert to this page?
    
    int d = _compare_key_noprefix(nrecs() - 1, key_to_insert_raw + prefix_len, key_to_insert_len - prefix_len);
    if (d <= 0) {
        return false; // not hitting highest. norecord-split will be useless
    }
    
    // we need some space for updated fence-high and chain-high
    smsize_t space_for_split = get_fence_low_length() + key_to_insert.get_length_as_keystr();
    if (get_chain_fence_high_length() == 0) {
        space_for_split += get_fence_high_length(); // newly set chain-high
    } else {
        space_for_split += get_chain_fence_high_length(); // otherwise chain-fence-high is unchanged
    }
    return (usable_space() >= align(space_for_split)); // otherwise it's too late
}

void btree_page_h::suggest_fence_for_split(w_keystr_t &mid,
                                           slotid_t& right_begins_from,
                                           const w_keystr_t &
#ifdef NORECORD_SPLIT_ENABLE
                                               triggering_key
#endif // NORECORD_SPLIT_ENABLE
    ) const {
// TODO for fair comparison with shore-mt, let's disable no-record-split for now
#ifdef NORECORD_SPLIT_ENABLE
    // if this is bulk-load case, simply make the new key as mid key (100% split)
    if (check_chance_for_norecord_split(triggering_key)) {
        right_begins_from = nrecs();
        if (is_leaf()) {
            w_keystr_t lastkey;
            get_key(nrecs() - 1, lastkey);
            size_t common_bytes = lastkey.common_leading_bytes(triggering_key);
            w_assert1(common_bytes < triggering_key.get_length_as_keystr());
            mid.construct_from_keystr(triggering_key.buffer_as_keystr(), common_bytes + 1);
        } else {
            mid = triggering_key;
        }
        return;
    }    
#endif // NORECORD_SPLIT_ENABLE
    
    w_assert1 (nrecs() >= 2);
    // pick the best separator key as follows.
    
    // first, pick the center point according to the skewness of past insertions to this page.
    slotid_t center_point = (nrecs() / 2); // usually just in the middle
    if (is_insertion_skewed_right()) {
        // last 5 inserts were on right-most, so let split on right-skewed point (90%)
        center_point = (nrecs() * 9 / 10);
    } else if (is_insertion_skewed_left()) {
        // last 5 inserts were on left-most, so let split on left-skewed point (10%)
        center_point = (nrecs() * 1 / 10);
    }
    
    // second, consider boundaries around the center point to pick the shortest separator key
    slotid_t start_point = (center_point - (nrecs() / 10) > 0) ? center_point - (nrecs() / 10) : 1;
    slotid_t end_point = (center_point + (nrecs() / 10) + 1 <= nrecs()) ? center_point + (nrecs() / 10) + 1 : nrecs();
    size_t sep_length = SM_PAGESIZE;
    const char *sep_key = NULL;
    right_begins_from = -1;
    for (slotid_t boundary = start_point; boundary < end_point; ++boundary) {
        if (is_leaf()) {
            // if we are splitting a leaf page, we are effectively designing a new separator key
            // which will be pushed up to parent. actually, this "mid" fence key is re-used
            // when we adopt a separator key later.
            size_t len1, len2;
            const char* k1 = _leaf_key_noprefix (boundary - 1, len1);
            const char* k2 = _leaf_key_noprefix (boundary, len2);
            // we apply suffix truncation here. We want a short separator key such that
            //   k1 < newkey <= k2.
            // For example, let k1=FEAR, k2=FFBC. new key can be
            //   FF, FEB, FFBC but not FFC or FEAR.
            // the newkey should send entries exclusively smaller than it to left,
            // inclusively larger than it to right. (remember, low fence key is inclusive (>=))
            
            // take common leading bytes +1 from right.
            // in above example, "FF". (common leading bytes=1)
            size_t common_bytes = w_keystr_t::common_leading_bytes((const unsigned char *) k1, len1, (const unsigned char *) k2, len2);
            w_assert1(common_bytes < len2); // otherwise the two keys are the same.
            // Note, we assume unique indexes, so the two keys are always different
            if (common_bytes + 1 < sep_length
                || (common_bytes + 1 == sep_length && boundary == center_point)) { // if tie, give a credit to center_point 
                right_begins_from = boundary;
                sep_length = common_bytes + 1;
                sep_key = k2;
            }
        } else {
            // for interior node, just return the existing key (we can shorten it though).
            size_t len;
            const char *k = _node_key_noprefix (boundary, len);
            if (len < sep_length
                || (len == sep_length && boundary == center_point)) { // if tie, give a credit to center_point 
                right_begins_from = boundary;
                sep_length = len;
                sep_key = k;
            }
        }
    }
    w_assert0(sep_key != NULL);
    w_assert1(sep_length != SM_PAGESIZE);
    mid.construct_from_keystr(get_prefix_key(), get_prefix_length(), sep_key, sep_length);
    w_assert0(right_begins_from >= 0 && right_begins_from <= nrecs());
    w_assert1(recalculate_fence_for_split(right_begins_from).compare(mid) == 0);
}

w_keystr_t btree_page_h::recalculate_fence_for_split(slotid_t right_begins_from) const {
    w_assert1(right_begins_from >= 0 && right_begins_from <= nrecs());
    w_keystr_t mid;
    if (is_leaf()) {
        size_t len1, len2;
        const char* k1 = _leaf_key_noprefix (right_begins_from - 1, len1);
        const char* k2 = _leaf_key_noprefix (right_begins_from, len2);        
        size_t common_bytes = w_keystr_t::common_leading_bytes((const unsigned char *) k1, len1, (const unsigned char *) k2, len2);
        w_assert1(common_bytes < len2); // otherwise the two keys are the same.
        mid.construct_from_keystr(get_prefix_key(), get_prefix_length(), k2, common_bytes + 1);
    } else {
        size_t len;
        const char *k = _node_key_noprefix (right_begins_from, len);
        mid.construct_from_keystr(get_prefix_key(), get_prefix_length(), k, len);
    }
    return mid;
}


void btree_page_h::get_key(slotid_t slot,  w_keystr_t &key) const {
    const char* key_noprefix;
    size_t      key_noprefix_length;
    if (is_leaf()) {
        key_noprefix = _leaf_key_noprefix(slot, key_noprefix_length);
    } else {
        key_noprefix = _node_key_noprefix(slot, key_noprefix_length);
    }

    key.construct_from_keystr(get_prefix_key(), get_prefix_length(),
                              key_noprefix, key_noprefix_length);
}


const char* btree_page_h::element(int slot, smsize_t &len, bool &ghost) const {
    w_assert1(is_leaf());

    size_t offset = _element_offset(slot);
    int    length = page()->item_length(slot+1) - offset;
    w_assert1(length >= 0);

    len   = length;
    ghost = is_ghost(slot);
    return page()->item_data(slot+1) + offset;
}
bool btree_page_h::copy_element(int slot, char *out_buffer, smsize_t &len, bool &ghost) const {
    smsize_t actual_length;
    const char* element_data = element(slot, actual_length, ghost);

    if (len >= actual_length) {
        ::memcpy(out_buffer, element_data, actual_length);
        len = actual_length;
        return true;
    } else {
        // the buffer is too short
        len = actual_length;
        return false;
    }
}


rc_t
btree_page_h::leaf_stats(btree_lf_stats_t& _stats) {
    _stats.hdr_bs    += hdr_sz + page()->item_space(0);
    _stats.unused_bs += usable_space();

    int n = nrecs();
    _stats.entry_cnt += n;
    int16_t prefix_length = get_prefix_length();
    for (int i = 0; i < n; i++)  {
        btrec_t rec;
        rec.set(*this, i);
        ++_stats.unique_cnt; // always unique (otherwise a bug)
        _stats.key_bs            += rec.key().get_length_as_keystr() - prefix_length;
        _stats.data_bs           += rec.elen(); 
        _stats.entry_overhead_bs += page()->item_space(n+1) - (rec.key().get_length_as_keystr()-prefix_length) - rec.elen();
    }
    return RCOK;
}

rc_t
btree_page_h::int_stats(btree_int_stats_t& _stats) {
    _stats.unused_bs += usable_space();
    _stats.used_bs   += used_space();
    return RCOK;
}


smsize_t         
btree_page_h::max_entry_size = 
    // must be able to fit 2 entries to a page; data_sz must hold:
    //    fence record:                   max_item_overhead + (max_entry_size+1)*3   (low, high, chain keys)
    //    each of 2 regular leaf entries: max_item_overhead + max_entry_size+1 + sizeof(key_length_t) [key len]
    //
    // +1's are for signed byte of keys
    (btree_page::data_sz - 3*btree_page::max_item_overhead - 2*sizeof(key_length_t)) / 5 - 1;


void
btree_page_h::print(bool print_elem) {
    int i;
    const int L = 3;

    for (i = 0; i < L - level(); i++)  cout << '\t';
    cout << pid0() << "=" << pid0() << endl;

    for (i = 0; i < nrecs(); i++)  {
        for (int j = 0; j < L - level(); j++)  cout << '\t' ;

        btrec_t r(*this, i);

        cout << "<key = " << r.key() ;

        if ( is_leaf())  {
            if(print_elem) {
                cout << ", elen="  << r.elen() << " bytes: " << r.elem();
            }
        } else {
            cout << "pid = " << r.child() << ", emlsn=" << r.child_emlsn();
        }
        cout << ">" << endl;
    }
    for (i = 0; i < L - level(); i++)  cout << '\t';
    cout << "]" << endl;
}

bool btree_page_h::is_consistent (bool check_keyorder, bool check_space) const {
    // does NOT check check-sum. the check can be done only by bufferpool
    // with seeing fresh data from the disk.
    
    // check poor-man's normalized key
    if (!_is_consistent_poormankey()) {
        w_assert1(false);
        return false;
    }

    // additionally check key-sortedness and uniqueness
    if (check_keyorder) {
        if (!_is_consistent_keyorder()) {
            w_assert1(false);
            return false;
        }
    }

    // additionally check record overlaps
    if (check_space) {
        if (!page()->_items_are_consistent()) {
            w_assert1(false);
            return false;
        }
    }

    return true;
}
bool btree_page_h::_is_consistent_keyorder() const {
    const int    recs       = nrecs();
    const char*  lowkey     = get_fence_low_key();
    const size_t lowkey_len = get_fence_low_length();
    const size_t prefix_len = get_prefix_length();
    const size_t chain_high_len = get_chain_fence_high_length();
    const shpid_t foster = get_foster_opaqueptr();
    // chain-high must be set if foster link exists.
    if(chain_high_len == 0 && foster != 0) {
        w_assert3(false);
        return false;
    }
    if (recs == 0) {
        // then just compare low-high and quit.
        // low==high is now allowed as part of page split.
        if (compare_with_fence_high(lowkey, lowkey_len) > 0) {
            w_assert3(false);
            return false;
        }
        return true;
    }
    // now we know that first key exists
    
    if (is_leaf()) {
        // first key might be equal to low-fence which is inclusive
        size_t curkey_len;
        const char *curkey = _leaf_key_noprefix(0, curkey_len);
        if (w_keystr_t::compare_bin_str(lowkey + prefix_len, lowkey_len - prefix_len, curkey, curkey_len) > 0) {

            w_assert3(false);
            return false;
        }

        // then, check each record
        const char* prevkey = curkey;
        size_t prevkey_len = curkey_len;
        for (slotid_t slot = 1; slot < recs; ++slot) {
            curkey = _leaf_key_noprefix(slot, curkey_len);
            if (w_keystr_t::compare_bin_str(prevkey, prevkey_len, curkey, curkey_len) >= 0) { // this time must not be equal either
                w_assert3(false);
                return false;
            }
            prevkey     = curkey;
            prevkey_len = curkey_len;
        }
        
        // last record is also compared with high-fence
        if (compare_with_fence_high_noprefix(prevkey, prevkey_len) > 0) {
            w_assert3(false);
            return false;
        }
    } else {
        size_t curkey_len;
        const char *curkey = _node_key_noprefix(0, curkey_len);
        if (w_keystr_t::compare_bin_str(lowkey + prefix_len, lowkey_len - prefix_len, curkey, curkey_len) > 0) {
            w_assert3(false);
            return false;
        }

        const char* prevkey = curkey;
        size_t prevkey_len = curkey_len;
        for (slotid_t slot = 1; slot < recs; ++slot) {
            curkey = _node_key_noprefix(slot, curkey_len);
            if (w_keystr_t::compare_bin_str(prevkey, prevkey_len, curkey, curkey_len) >= 0) {
                w_assert3(false);
                return false;
            }
            prevkey = curkey;
            prevkey_len = curkey_len;
        }
        
        if (compare_with_fence_high_noprefix(prevkey, prevkey_len) > 0) {
            w_assert3(false);
            return false;
        }
    }
    
    return true;
}
bool btree_page_h::_is_consistent_poormankey() const {
    const int recs = nrecs();
    // the first record is fence key, so no poor man's key (always 0)
    poor_man_key fence_poormankey = page()->item_poor(0);
    if (fence_poormankey != 0) {
//        w_assert3(false);
        return false;
    }
    // for other records, check with the real key string in the record
    for (slotid_t slot = 0; slot < recs; ++slot) {
        poor_man_key poorman_key = _poor(slot);
        size_t curkey_len;
        const char* curkey = is_leaf() ? _leaf_key_noprefix(slot, curkey_len) : _node_key_noprefix(slot, curkey_len);
        poor_man_key correct_poormankey = _extract_poor_man_key(curkey, curkey_len);
        if (poorman_key != correct_poormankey) {
//            w_assert3(false);
            return false;
        }
    }
    
    return true;
}


rc_t btree_page_h::defrag( const bool full_logging_redo) { 
    // defrag can be called from btree_ghost_mark_log::redo 
    // if caller is a log record generated for full logging page rebalance
    // then the context is not system transaction but it should be treated as a
    // system transaction and do not generate new log record

    if (false == full_logging_redo)
        w_assert1 (xct()->is_sys_xct());
    w_assert1 (is_fixed());
    w_assert1 (latch_mode() == LATCH_EX);

    vector<slotid_t> ghost_slots;

    for (int i=0; i<page()->number_of_items(); i++) {
        if (page()->is_ghost(i)) {
            w_assert1(i >= 1); // fence record can't be ghost
            ghost_slots.push_back(i-1);
        }
    }
    // defrag doesn't need log if there were no ghost records:
    if ((ghost_slots.size() > 0) && (false == full_logging_redo)){
        W_DO (log_btree_ghost_reclaim(*this, ghost_slots));
    }

    page()->compact();
    set_dirty();

    return RCOK;
}

bool btree_page_h::_check_space_for_insert(size_t data_length) {
    size_t contiguous_free_space = usable_space();
    return contiguous_free_space >= page()->predict_item_space(data_length);
}


void btree_page_h::_init(lsn_t lsn, lpid_t page_id,
    shpid_t root_pid, 
    shpid_t pid0, lsn_t pid0_emlsn,          // Non-leaf page only
    shpid_t foster_pid, lsn_t foster_emlsn,  // If foster child exists for this page
    int16_t btree_level,
    const w_keystr_t &low,                   // Low fence key
    const w_keystr_t &high,                  // High key, confusing naming, it is actually the foster key
    const w_keystr_t &chain_fence_high,      // Chain high fence key (if foster chain), 
                                             // it is the high fence key for all foster child nodes
    const bool ghost ) {                     // Should the fence key record be a ghost?                                             

    // Initialize the current page with fence keys and other information

    // A node contains low fence, high fence and foster key if a foster child exists
    // When a foster child or foster chain (multiple foster child nodes) exists, all
    // the foster child nodes have the same high fence key, which is the same as 
    // the foster parent's high fence key (chain_fence_high), while the foster key (high)
    // is different in each foster child node and it is used to determine record 
    // boundaries (same purpose  as a regular high fence key.
    // 
    // The naming in existing Express code is confusing:
    // Low - low fence key
    // High - foster key
    // chain_fence_high - high fence key in foster relationship, both foster parent
    //                             and foster child nodes

#ifdef ZERO_INIT
    // because we do this, note that we shouldn't receive any arguments
    // as reference or pointer. It might be also nuked!
    memset(page(), '\017', sizeof(generic_page)); // trash the whole page
#endif //ZERO_INIT

    page()->lsn          = lsn;
    page()->pid          = page_id;
    page()->tag          = t_btree_p;
    page()->page_flags   = 0;
    page()->init_items();
    page()->btree_consecutive_skewed_insertions = 0;
    page()->btree_root                    = root_pid;
    page()->btree_pid0                    = pid0;
    page()->btree_pid0_emlsn              = pid0_emlsn;
    page()->btree_level                   = btree_level;
    page()->btree_foster                  = foster_pid;
    page()->btree_foster_emlsn            = foster_emlsn;
    page()->btree_fence_low_length        = (int16_t) low.get_length_as_keystr();
    page()->btree_fence_high_length       = (int16_t) high.get_length_as_keystr();
    page()->btree_chain_fence_high_length = (int16_t) chain_fence_high.get_length_as_keystr();

    // set fence keys in first slot
    cvec_t fences;
    size_t prefix_len = _pack_fence_rec(fences, low, high, chain_fence_high, -1);
    w_assert1(prefix_len <= low.get_length_as_keystr());
    w_assert1(prefix_len <= max_key_length);
    page()->btree_prefix_length = (int16_t) prefix_len;

    // fence-key record doesn't need poormkey; set to 0:
    if (!page()->insert_item(nrecs() + 1, ghost /* ghost*/, 0, 0, fences)) {
        w_assert0(false);
    }
}
