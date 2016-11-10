/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#include "bf_hashtable.h"
#include "bf_tree_cb.h"
#include "bf_tree_cleaner.h"
#include "page_cleaner_decoupled.h"
#include "bf_tree.h"

#include "smthread.h"
#include "generic_page.h"
#include <string.h>
#include "w_findprime.h"
#include <stdlib.h>

#include "sm_base.h"
#include "sm.h"
#include "vol.h"
#include "alloc_cache.h"

#include <boost/static_assert.hpp>
#include <ostream>
#include <limits>
#include <algorithm>

#include "sm_options.h"
#include "latch.h"
#include "btree_page_h.h"
#include "log_core.h"
#include "xct.h"
#include <logfunc_gen.h>

#include "restart.h"

// Template definitions
#include "bf_hashtable.cpp"

///////////////////////////////////   Initialization and Release BEGIN ///////////////////////////////////

bf_tree_m::bf_tree_m(const sm_options& options)
{
    // sm_bufboolsize given in MB -- default 8GB
    long bufpoolsize = options.get_int_option("sm_bufpoolsize", 8192) * 1024 * 1024;
    uint32_t  nbufpages = (bufpoolsize - 1) / sizeof(generic_page) + 1;
    if (nbufpages < 10)  {
        cerr << "ERROR: buffer size ("
             << bufpoolsize
             << "-KB) is too small" << endl;
        cerr << "       at least " << 32 * sizeof(generic_page) / 1024
             << "-KB is needed" << endl;
        W_FATAL(eCRASH);
    }

    bool bufferpool_swizzle =
        options.get_bool_option("sm_bufferpool_swizzle", false);
    // clock or random
    std::string replacement_policy =
        options.get_string_option("sm_bufferpool_replacement_policy", "clock");

    ::memset (this, 0, sizeof(bf_tree_m));

    _block_cnt = nbufpages;
    _enable_swizzling = bufferpool_swizzle;
    // if (strcmp(replacement_policy.c_str(), "clock") == 0) {
    //     _replacement_policy = POLICY_CLOCK;
    // } else if (strcmp(replacement_policy.c_str(), "clock+priority") == 0) {
    //     _replacement_policy = POLICY_CLOCK_PRIORITY;
    // } else if (strcmp(replacement_policy.c_str(), "random") == 0) {
    //     _replacement_policy = POLICY_RANDOM;
    // }

    DBGOUT1 (<< "constructing bufferpool with " << nbufpages << " blocks of "
            << SM_PAGESIZE << "-bytes pages... enable_swizzling=" <<
            _enable_swizzling);

    // use posix_memalign to allow unbuffered disk I/O
    void *buf = NULL;
    if (::posix_memalign(&buf, SM_PAGESIZE, SM_PAGESIZE * ((uint64_t)
                    nbufpages)) != 0)
    {
        ERROUT (<< "failed to reserve " << nbufpages
                << " blocks of " << SM_PAGESIZE << "-bytes pages. ");
        W_FATAL(eOUTOFMEMORY);
    }
    _buffer = reinterpret_cast<generic_page*>(buf);

    // the index 0 is never used. to make sure no one can successfully use it,
    // fill the block-0 with garbages
    ::memset (&_buffer[0], 0x27, sizeof(generic_page));

    // this allocation scheme is sensible only for control block and latch sizes of 64B (cacheline size)
    BOOST_STATIC_ASSERT(sizeof(bf_tree_cb_t) == 64);
    BOOST_STATIC_ASSERT(sizeof(latch_t) == 64);
    // allocate one more pair of <control block, latch> as we want to align the table at an odd
    // multiple of cacheline (64B)
    size_t total_size = (sizeof(bf_tree_cb_t) + sizeof(latch_t))
        * (((uint64_t) nbufpages) + 1LLU);
    if (::posix_memalign(&buf, sizeof(bf_tree_cb_t) + sizeof(latch_t),
                total_size) != 0)
    {
        ERROUT (<< "failed to reserve " << nbufpages
                << " blocks of " << sizeof(bf_tree_cb_t) << "-bytes blocks.");
        W_FATAL(eOUTOFMEMORY);
    }
    ::memset (buf, 0, (sizeof(bf_tree_cb_t) + sizeof(latch_t)) * (((uint64_t)
                    nbufpages) + 1LLU));
    _control_blocks = reinterpret_cast<bf_tree_cb_t*>(reinterpret_cast<char
            *>(buf) + sizeof(bf_tree_cb_t));
    w_assert0(_control_blocks != NULL);
    for (bf_idx i = 0; i < nbufpages; i++) {
        BOOST_STATIC_ASSERT(sizeof(bf_tree_cb_t) < SCHAR_MAX);
        if (i & 0x1) { /* odd */
            get_cb(i)._latch_offset = -static_cast<int8_t>(sizeof(bf_tree_cb_t)); // place the latch before the control block
        } else { /* even */
            get_cb(i)._latch_offset = sizeof(bf_tree_cb_t); // place the latch after the control block
        }
    }

    // initially, all blocks are free
    _freelist = new bf_idx[nbufpages];
    w_assert0(_freelist != NULL);
    _freelist[0] = 1; // [0] is a special entry. it's the list head
    for (bf_idx i = 1; i < nbufpages - 1; ++i) {
        _freelist[i] = i + 1;
    }
    _freelist[nbufpages - 1] = 0;
    _freelist_len = nbufpages - 1; // -1 because [0] isn't a valid block

    //initialize hashtable
    int buckets = w_findprime(1024 + (nbufpages / 4)); // maximum load factor is 25%. this is lower than original shore-mt because we have swizzling
    _hashtable = new bf_hashtable<bf_idx_pair>(buckets);
    w_assert0(_hashtable != NULL);

    _eviction_current_frame = 0;
    DO_PTHREAD(pthread_mutex_init(&_eviction_lock, NULL));

    _cleaner_decoupled = options.get_bool_option("sm_cleaner_decoupled", false);
}

void bf_tree_m::shutdown()
{
    if (_cleaner) {
        _cleaner->stop();
        delete _cleaner;
        _cleaner = NULL;
    }
}

bf_tree_m::~bf_tree_m()
{
    if (_control_blocks != NULL) {
        char* buf = reinterpret_cast<char*>(_control_blocks) - sizeof(bf_tree_cb_t);

        // note we use free(), not delete[], which corresponds to posix_memalign
        ::free (buf);
        _control_blocks = NULL;
    }
    if (_freelist != NULL) {
        delete[] _freelist;
        _freelist = NULL;
    }
    if (_hashtable != NULL) {
        delete _hashtable;
        _hashtable = NULL;
    }
    if (_buffer != NULL) {
        void *buf = reinterpret_cast<void*>(_buffer);
        // note we use free(), not delete[], which corresponds to posix_memalign
        ::free (buf);
        _buffer = NULL;
    }

    DO_PTHREAD(pthread_mutex_destroy(&_eviction_lock));

}

page_cleaner_base* bf_tree_m::get_cleaner()
{
    if (!ss_m::vol || !ss_m::vol->caches_ready()) {
        // No volume manager initialized -- no point in starting cleaner
        return nullptr;
    }

    if (!_cleaner) {
        if(_cleaner_decoupled) {
            w_assert0(smlevel_0::logArchiver);
            _cleaner = new page_cleaner_decoupled(this,
                    ss_m::get_options());
        }
        else{
            _cleaner = new bf_tree_cleaner (this, ss_m::get_options());
        }
        _cleaner->fork();
    }

    return _cleaner;
}

///////////////////////////////////   Initialization and Release END ///////////////////////////////////

bf_idx bf_tree_m::lookup(PageID pid) const
{
    bf_idx idx = 0;
    bf_idx_pair p;
    if (_hashtable->lookup(pid, p)) {
        idx = p.first;
    }
    return idx;
}

///////////////////////////////////   Page fix/unfix BEGIN         ///////////////////////////////////
// NOTE most of the page fix/unfix functions are in bf_tree_inline.h.
// These functions are here are because called less frequently.

w_rc_t bf_tree_m::fix(generic_page* parent, generic_page*& page,
                                   PageID pid, latch_mode_t mode,
                                   bool conditional, bool virgin_page, bool only_if_hit,
                                   lsn_t emlsn)
{
    if (is_swizzled_pointer(pid)) {
        w_assert1(!virgin_page);

        bf_idx idx = pid ^ SWIZZLED_PID_BIT;
        w_assert1(_is_valid_idx(idx));
        bf_tree_cb_t &cb = get_cb(idx);

        W_DO(cb.latch().latch_acquire(mode,
                    conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));

        w_assert1(_is_active_idx(idx));
        w_assert1(cb._swizzled);
        w_assert1(cb._pid == _buffer[idx].pid);

        cb.pin();
        cb.inc_ref_count();
        if (mode == LATCH_EX) {
            cb.inc_ref_count_ex();
        }

        page = &(_buffer[idx]);

        return RCOK;
    }

#if W_DEBUG_LEVEL>0
    int retry_count = 0;
#endif
    // CS TODO: get rid of this loop
    while (true)
    {
#if W_DEBUG_LEVEL>0
        if (++retry_count % 10000 == 0) {
            DBGOUT1(<<"keep trying to fix.. " << pid << ". current retry count=" << retry_count);
        }
#endif
        bf_idx_pair p;
        bf_idx idx = 0;
        if (_hashtable->lookup(pid, p)) {
            idx = p.first;
            if (parent && p.second != parent - _buffer) {
                // need to fix update parent pointer
                // p.second = parent - _buffer;
                // _hashtable->update(pid, p);
            }
        }

        if (idx == 0)
        {
            if (only_if_hit) {
                return RC(stINUSE);
            }

            // STEP 1) Grab a free frame to read into
            W_DO(_grab_free_block(idx));
            w_assert1(_is_valid_idx(idx));
            bf_tree_cb_t &cb = get_cb(idx);
            w_assert1(!cb._used);

            // STEP 2) Acquire EX latch before hash table insert, to make sure
            // nobody will access this page until we're done
            w_rc_t check_rc = cb.latch().latch_acquire(LATCH_EX,
                    sthread_t::WAIT_IMMEDIATE);
            if (check_rc.is_error())
            {
                _add_free_block(idx);
                // TODO: add a sleep or wait mechanism whenever fix fails
                continue;
            }

            // Register the page on the hashtable atomically. This guarantees
            // that only one thread will attempt to read the page
            bf_idx parent_idx = parent ? parent - _buffer : 0;
            bool registered = _hashtable->insert_if_not_exists(pid,
                    bf_idx_pair(idx, parent_idx));
            if (!registered) {
                cb.latch().latch_release();
                _add_free_block(idx);
                continue;
            }

            // Read page from disk
            page = &_buffer[idx];
            cb.init(pid, lsn_t::null);

            if (!virgin_page) {
                INC_TSTAT(bf_fix_nonroot_miss_count);

                if (parent && emlsn.is_null()) {
                    // Get emlsn from parent
                    general_recordid_t recordid = find_page_id_slot(parent, pid);
                    btree_page_h parent_h;
                    parent_h.fix_nonbufferpool_page(parent);
                    emlsn = parent_h.get_emlsn_general(recordid);
                }

                w_rc_t read_rc = smlevel_0::vol->read_page_verify(pid, page, emlsn);
                if (read_rc.is_error())
                {
                    _hashtable->remove(pid);
                    cb.clear_except_latch();
                    cb.latch().latch_release();
                    _add_free_block(idx);
                    return read_rc;
                }
                cb.init(pid, page->lsn);
            }

            w_assert1(_is_active_idx(idx));

            // STEP 6) Fix successful -- pin page and downgrade latch
            cb.pin();
            w_assert1(cb.latch().is_mine());
            w_assert1(cb._pin_cnt > 0);
            DBG(<< "Fixed page " << pid << " (miss) to frame " << idx);

            if (mode != LATCH_EX) {
                w_assert1(mode == LATCH_SH);
                cb.latch().downgrade();
            }
        }
        else
        {
            // Page index is registered in hash table
            bf_tree_cb_t &cb = get_cb(idx);

            // Page is registered in hash table and it is not an in_doubt page,
            // meaning the actual page is in buffer pool already

            W_DO(cb.latch().latch_acquire(mode, conditional ?
                        sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));

            if (cb._pin_cnt < 0 || cb._pid != pid) {
                // Page was evicted between hash table probe and latching
                DBG(<< "Page evicted right before latching. Retrying.");
                cb.latch().latch_release();
                continue;
            }

            w_assert1(_is_active_idx(idx));
            cb.pin();
            cb.inc_ref_count();
            if (mode == LATCH_EX) {
                cb.inc_ref_count_ex();
            }

            page = &(_buffer[idx]);

            w_assert1(cb.latch().held_by_me());
            DBG(<< "Fixed page " << pid << " (hit) to frame " << idx);
            w_assert1(cb._pin_cnt > 0);
        }

        if (!is_swizzled(page) && _enable_swizzling && parent) {
            // swizzle pointer for next invocations
            bf_tree_cb_t &cb = get_cb(idx);

            // Get slot on parent page
            w_assert1(_is_active_idx(parent - _buffer));
            w_assert1(latch_mode(parent) != LATCH_NL);
            fixable_page_h p;
            p.fix_nonbufferpool_page(parent);
            general_recordid_t slot = find_page_id_slot (parent, pid);

            if (!is_swizzled(parent)) { return RCOK; }

            // Either a virgin page which hasn't been linked yet, or some other
            // thread won the race and already swizzled the pointer
            if (slot == GeneralRecordIds::INVALID) { return RCOK; }
            // Not worth swizzling foster children, since they will soon be
            // adopted (an thus unswizzled)
            if (slot == GeneralRecordIds::FOSTER_CHILD) { return RCOK; }
            w_assert1(slot > GeneralRecordIds::FOSTER_CHILD);
            w_assert1(slot <= p.max_child_slot());

            // Update _swizzled flag atomically
            bool old_value = false;
            if (!std::atomic_compare_exchange_strong(&cb._swizzled, &old_value, true)) {
                // CAS failed -- some other thread is swizzling
                return RCOK;
            }
            w_assert1(is_swizzled(page));

            // Replace pointer with swizzled version
            PageID* addr = p.child_slot_address(slot);
            *addr = idx | SWIZZLED_PID_BIT;

#if W_DEBUG_LEVEL > 0
            PageID swizzled_pid = idx | SWIZZLED_PID_BIT;
            general_recordid_t child_slotid = find_page_id_slot(parent, swizzled_pid);
            w_assert1(child_slotid != GeneralRecordIds::INVALID);
#endif
        }

        return RCOK;
    }
}

bool bf_tree_m::_is_frame_latched(generic_page* frame, latch_mode_t mode)
{
    bf_idx idx = frame - _buffer;
    w_assert1(_is_valid_idx(idx));
    if (!_is_active_idx(idx)) { return false; }
    bf_tree_cb_t& cb = get_cb(idx);
    return cb.latch().mode() == mode;
}

void bf_tree_m::print_page(PageID pid)
{
    bf_idx_pair p;
    bf_idx idx = 0;

    if (is_swizzled_pointer(pid)) {
        idx = pid ^ SWIZZLED_PID_BIT;
    }
    else if (_hashtable->lookup(pid, p)) {
        idx = p.first;
    }
    else {
        cout << "not cached" << endl;
        return;
    }

    generic_page* page = &_buffer[idx];
    btree_page_h bp;
    bp.fix_nonbufferpool_page(page);
    bp.print(true);
}

bf_idx bf_tree_m::pin_for_refix(const generic_page* page) {
    w_assert1(page != NULL);
    w_assert1(latch_mode(page) != LATCH_NL);

    bf_idx idx = page - _buffer;
    w_assert1(_is_active_idx(idx));

    w_assert1(get_cb(idx)._pin_cnt >= 0);
    w_assert1(get_cb(idx).latch().held_by_me());

    get_cb(idx).pin();
    DBG(<< "Refix set pin cnt to " << get_cb(idx)._pin_cnt);
    return idx;
}

void bf_tree_m::unpin_for_refix(bf_idx idx) {
    w_assert1(_is_active_idx(idx));

    w_assert1(get_cb(idx)._pin_cnt > 0);

    // CS TODO: assertion below fails when btcursor is destructed.  Therefore,
    // we are violating the rule that pin count can only be updated when page
    // is latched. But it seems that the program logic avoids something bad
    // happened. Still, it's quite edgy at the moment. I should probaly study
    // the btcursor code in detail before taking further action on this.
    // w_assert1(get_cb(idx).latch().held_by_me());
    get_cb(idx).unpin();
    DBG(<< "Unpin for refix set pin cnt to " << get_cb(idx)._pin_cnt);
    w_assert1(get_cb(idx)._pin_cnt >= 0);
}

///////////////////////////////////   Page fix/unfix END         ///////////////////////////////////

void bf_tree_m::switch_parent(PageID pid, generic_page* parent)
{
#if W_DEBUG_LEVEL > 0
    // Given PID must actually be an entry in the parent
    general_recordid_t child_slotid = find_page_id_slot(parent, pid);
    w_assert1 (child_slotid != GeneralRecordIds::INVALID);
#endif

    if (is_swizzled_pointer(pid)) {
        general_recordid_t child_slotid = find_page_id_slot(parent, pid);
        // passing apply=false just to convert pointer without actually unswizzling it
        bool success = unswizzle(parent, child_slotid, false, &pid);
        w_assert0(success);
    }
    w_assert1(!is_swizzled_pointer(pid));

    bf_idx_pair p;
    bool found = _hashtable->lookup(pid, p);
    // if page is not cached, there is nothing to update
    if (!found) { return; }

    bf_idx parent_idx = parent - _buffer;
    // CS TODO: this assertion fails when using slot 1 sometimes
    // w_assert1(parent_idx != p.second);
    DBG5(<< "Parent of " << pid << " updated to " << parent_idx
            << " from " << p.second);
    if (parent_idx != p.second) {
        p.second = parent_idx;
        found = _hashtable->update(pid, p);
    }

    // page cannot be evicted since first lookup because caller latched it
    w_assert0(found);
}

void bf_tree_m::_convert_to_disk_page(generic_page* page) const
{
    fixable_page_h p;
    p.fix_nonbufferpool_page(page);
    int max_slot = p.max_child_slot();
    for (general_recordid_t i = GeneralRecordIds::FOSTER_CHILD; i <= max_slot; ++i) {
        PageID* pid = p.child_slot_address(i);
        if (is_swizzled_pointer(*pid)) {
            bf_idx idx = (*pid) ^ SWIZZLED_PID_BIT;
            // CS TODO: Slot 1 (which is actually 0 in the internal page
            // representation) is not used sometimes (I think when a page is
            // first created?) so we must skip it manually here to avoid
            // getting an invalid page below.
            if (i == 1 && !_is_active_idx(idx)) { continue; }
            w_assert1(_is_active_idx(idx));
            bf_tree_cb_t &cb = get_cb(idx);
            *pid = cb._pid;
        }
    }
}

general_recordid_t bf_tree_m::find_page_id_slot(generic_page* page, PageID pid) const
{
    fixable_page_h p;
    p.fix_nonbufferpool_page(page);
    int max_slot = p.max_child_slot();

    for (general_recordid_t i = GeneralRecordIds::FOSTER_CHILD; i <= max_slot; ++i) {
        // ERROUT(<< "Looking for child " << pid << " on " <<
        //         *p.child_slot_address(i));
        if (*p.child_slot_address(i) == pid) {
            // ERROUT(<< "OK");
            return i;
        }
    }
    return GeneralRecordIds::INVALID;
}

///////////////////////////////////   SWIZZLE/UNSWIZZLE BEGIN ///////////////////////////////////

bool bf_tree_m::has_swizzled_child(bf_idx node_idx) {
    fixable_page_h node_p;
    node_p.fix_nonbufferpool_page(_buffer + node_idx);
    int max_slot = node_p.max_child_slot();
    // skipping foster pointer...
    for (int32_t j = 0; j <= max_slot; ++j) {
        PageID pid = *node_p.child_slot_address(j);
        if ((pid & SWIZZLED_PID_BIT) != 0) {
            return true;
        }
    }
    return false;
}

/*
 * CS TODO: this function is used to unswizzle a pointer (child_slot on page parent_idx)
 * when performing eviction using tree traversal. Since our current eviction mechanism
 * does not use traversal, it is currently unused.
 *
 * Parent and child must me latched in EX mode if apply == true.
 */
bool bf_tree_m::unswizzle(generic_page* parent, general_recordid_t child_slot, bool apply,
        PageID* ret_pid)
{
    bf_idx parent_idx = parent - _buffer;
    bf_tree_cb_t &parent_cb = get_cb(parent_idx);
    // CS TODO: foster parent of a node created during a split will not have a
    // swizzled pointer to the new node; breaking the rule for now
    // if (!parent_cb._used || !parent_cb._swizzled) {
    w_assert1(parent_cb._used);
    w_assert1(parent_cb.latch().held_by_me());

    fixable_page_h p;
    p.fix_nonbufferpool_page(parent);
    w_assert1(child_slot <= p.max_child_slot());

    PageID* pid_addr = p.child_slot_address(child_slot);
    PageID pid = *pid_addr;
    if (!is_swizzled_pointer(pid)) {
        return false;
    }

    bf_idx child_idx = pid ^ SWIZZLED_PID_BIT;
    bf_tree_cb_t &child_cb = get_cb(child_idx);

    w_assert1(child_cb._used);
    w_assert1(child_cb._swizzled);

    if (apply) {
        // Since we have EX latch, we can just set the _swizzled flag
        // Otherwise there would be a race between swizzlers and unswizzlers
        // Parent is updated without requiring EX latch. This is correct as
        // long as fix call can deal with swizzled pointers not being really
        // swizzled.
        w_assert1(child_cb.latch().held_by_me());
        w_assert1(child_cb.latch().mode() == LATCH_EX);
        w_assert1(parent_cb.latch().held_by_me());
        w_assert1(parent_cb.latch().mode() == LATCH_EX);
        child_cb._swizzled = false;
        *pid_addr = child_cb._pid;
        w_assert1(!is_swizzled_pointer(*pid_addr));
#if W_DEBUG_LEVEL > 0
        general_recordid_t child_slotid = find_page_id_slot(parent, child_cb._pid);
        w_assert1(child_slotid != GeneralRecordIds::INVALID);
#endif
    }

    if (ret_pid) { *ret_pid = child_cb._pid; }

    return true;
}

///////////////////////////////////   SWIZZLE/UNSWIZZLE END ///////////////////////////////////

void bf_tree_m::debug_dump(std::ostream &o) const
{
    o << "dumping the bufferpool contents. _block_cnt=" << _block_cnt << "\n";
    o << "  _freelist_len=" << _freelist_len << ", HEAD=" << FREELIST_HEAD << "\n";

    for (uint32_t store = 1; store < stnode_page::max; ++store) {
        if (_root_pages[store] != 0) {
            o << ", " << store << "=" << _root_pages[store];
        }
    }
    o << std::endl;
    for (bf_idx idx = 1; idx < _block_cnt && idx < 1000; ++idx) {
        o << "  frame[" << idx << "]:";
        bf_tree_cb_t &cb = get_cb(idx);
        if (cb._used) {
            o << "page-" << cb._pid;
            if (cb.is_dirty()) {
                o << " (dirty)";
            }
            o << ", _swizzled=" << cb._swizzled;
            o << ", _pin_cnt=" << cb._pin_cnt;
            o << ", _ref_count=" << cb._ref_count;
            o << ", _ref_count_ex=" << cb._ref_count_ex;
            o << ", ";
            cb.latch().print(o);
        } else {
            o << "unused (next_free=" << _freelist[idx] << ")";
        }
        o << std::endl;
    }
    if (_block_cnt >= 1000) {
        o << "  ..." << std::endl;
    }
}

void bf_tree_m::debug_dump_page_pointers(std::ostream& o, generic_page* page) const {
    bf_idx idx = page - _buffer;
    w_assert1(idx > 0);
    w_assert1(idx < _block_cnt);

    o << "dumping page:" << page->pid << ", bf_idx=" << idx << std::endl;
    o << "  ";
    fixable_page_h p;
    p.fix_nonbufferpool_page(page);
    for (int i= -1; i<=p.max_child_slot(); i++) {
        if (i > -1) {
            o << ", ";
        }
        o << "child[" << i << "]=";
        debug_dump_pointer(o, *p.child_slot_address(i));
    }
    o << std::endl;
}
void bf_tree_m::debug_dump_pointer(ostream& o, PageID pid) const
{
    if (pid & SWIZZLED_PID_BIT) {
        bf_idx idx = pid ^ SWIZZLED_PID_BIT;
        o << "swizzled(bf_idx=" << idx;
        o << ", page=" << get_cb(idx)._pid << ")";
    } else {
        o << "normal(page=" << pid << ")";
    }
}

PageID bf_tree_m::debug_get_original_pageid (PageID pid) const {
    if (is_swizzled_pointer(pid)) {
        bf_idx idx = pid ^ SWIZZLED_PID_BIT;
        return get_cb(idx)._pid;
    } else {
        return pid;
    }
}

w_rc_t bf_tree_m::_sx_update_child_emlsn(btree_page_h &parent, general_recordid_t child_slotid,
                                         lsn_t child_emlsn) {
    sys_xct_section_t sxs (true); // this transaction will output only one log!
    W_DO(sxs.check_error_on_start());
    w_assert1(parent.is_latched());
    W_DO(log_page_evict(parent, child_slotid, child_emlsn));
    parent.set_emlsn_general(child_slotid, child_emlsn);
    W_DO (sxs.end_sys_xct (RCOK));
    return RCOK;
}

void bf_tree_m::_delete_block(bf_idx idx) {
    w_assert1(_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb._pin_cnt == 0);
    w_assert1(!cb.latch().is_latched());
    w_assert1(!cb._swizzled);
    cb._used = false; // clear _used BEFORE _dirty so that eviction thread will ignore this block.

    DBGOUT1(<<"delete block: remove page pid = " << cb._pid);
    bool removed = _hashtable->remove(cb._pid);
    w_assert1(removed);

    // after all, give back this block to the freelist. other threads can see this block from now on
    _add_free_block(idx);
}

bf_tree_cb_t* bf_tree_m::get_cbp(bf_idx idx) const {
    bf_idx real_idx;
    real_idx = (idx << 1) + (idx & 0x1); // more efficient version of: real_idx = (idx % 2) ? idx*2+1 : idx*2
    return &_control_blocks[real_idx];
}

bf_tree_cb_t& bf_tree_m::get_cb(bf_idx idx) const {
    return *get_cbp(idx);
}

bf_idx bf_tree_m::get_idx(const bf_tree_cb_t* cb) const {
    bf_idx real_idx = cb - _control_blocks;
    return real_idx / 2;
}

bf_tree_cb_t* bf_tree_m::get_cb(const generic_page *page) {
    bf_idx idx = page - _buffer;
    w_assert1(_is_valid_idx(idx));
    return get_cbp(idx);
}

generic_page* bf_tree_m::get_page(const bf_tree_cb_t *cb) {
    bf_idx idx = get_idx(cb);
    w_assert1(_is_valid_idx(idx));
    return _buffer + idx;
}

generic_page* bf_tree_m::get_page(const bf_idx& idx) {
    w_assert1(_is_valid_idx(idx));
    return _buffer + idx;
}

PageID bf_tree_m::get_root_page_id(StoreID store) {
    bf_idx idx = _root_pages[store];
    if (!_is_valid_idx(idx)) {
        return 0;
    }
    generic_page* page = _buffer + idx;
    return page->pid;
}

bf_idx bf_tree_m::get_root_page_idx(StoreID store) {
    // root-page index is always kept in the volume descriptor:
    bf_idx idx = _root_pages[store];
    if (!_is_valid_idx(idx))
        return 0;
    else
        return idx;
}

///////////////////////////////////   Page fix/unfix BEGIN         ///////////////////////////////////

w_rc_t bf_tree_m::refix_direct (generic_page*& page, bf_idx
                                       idx, latch_mode_t mode, bool conditional) {
    bf_tree_cb_t &cb = get_cb(idx);
    W_DO(cb.latch().latch_acquire(mode, conditional ?
                sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
    w_assert1(cb._pin_cnt > 0);
    cb.pin();
    DBG(<< "Refix direct of " << idx << " set pin cnt to " << cb._pin_cnt);
    cb.inc_ref_count();
    if (mode == LATCH_EX) { ++cb._ref_count_ex; }
    page = &(_buffer[idx]);
    return RCOK;
}

w_rc_t bf_tree_m::fix_nonroot(generic_page*& page, generic_page *parent,
                                     PageID pid, latch_mode_t mode, bool conditional,
                                     bool virgin_page, bool only_if_hit, lsn_t emlsn)
{
    INC_TSTAT(bf_fix_nonroot_count);
    return fix(parent, page, pid, mode, conditional, virgin_page, only_if_hit, emlsn);
}

w_rc_t bf_tree_m::fix_root (generic_page*& page, StoreID store,
                                   latch_mode_t mode, bool conditional,
                                   bool virgin)
{
    w_assert1(store != 0);

    bf_idx idx = _root_pages[store];
    if (!_is_valid_idx(idx)) {
        // Load root page
        PageID root_pid = smlevel_0::vol->get_store_root(store);
        W_DO(fix(NULL, page, root_pid, mode, conditional, virgin));

        bf_idx_pair p;
        bool found = _hashtable->lookup(root_pid, p);
        w_assert0(found);
        idx = p.first;

        // if (_enable_swizzling) {
            // Swizzle pointer to root in _root_pages array
            bool old_value = false;
            if (!std::atomic_compare_exchange_strong(&(get_cb(idx)._swizzled),
                        &old_value, true))
            {
                // CAS failed -- some other thread is swizzling
                return RCOK;
            }
            w_assert1(is_swizzled(page));
            _root_pages[store] = idx;
        // }
    }
    else {
        // Pointer to root page was swizzled -- direct access to CB
        W_DO(get_cb(idx).latch().latch_acquire(
                    mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
        page = &(_buffer[idx]);
        get_cb(idx).pin();
    }

    w_assert1(_is_valid_idx(idx));
    w_assert1(_is_active_idx(idx));
    w_assert1(get_cb(idx)._used);

    w_assert1(get_cb(idx)._pin_cnt > 0);
    w_assert1(get_cb(idx).latch().held_by_me());
    DBG(<< "Fixed root " << idx << " pin cnt " << get_cb(idx)._pin_cnt);
    return RCOK;
}


void bf_tree_m::unfix(const generic_page* p, bool evict)
{
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb.latch().held_by_me());
    if (evict) {
        w_assert0(cb.latch().is_mine());
        bool removed = _hashtable->remove(p->pid);
        w_assert1(removed);

        cb.clear_except_latch();
        // -1 indicates page was evicted (i.e., it's invalid and can be read into)
        cb._pin_cnt = -1;

        _add_free_block(idx);
    }
    else {
        cb.unpin();
        w_assert1(cb._pin_cnt >= 0);
    }
    DBG(<< "Unfixed " << idx << " pin count " << cb._pin_cnt);
    cb.latch().latch_release();
}

bool bf_tree_m::is_dirty(const generic_page* p) const {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx).is_dirty();
}

bool bf_tree_m::is_dirty(const bf_idx idx) const {
    // Caller has latch on page
    // Used by REDO phase in Recovery
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx).is_dirty();
}

bool bf_tree_m::is_used (bf_idx idx) const {
    return _is_active_idx(idx);
}

void bf_tree_m::set_page_lsn(generic_page* p, lsn_t lsn)
{
    uint32_t idx = p - _buffer;

    // CS: workaround for design limitation of restore. When redoing a log
    // record, the LSN should only be updated if the page image being used
    // belongs to the buffer pool. Initially, we checked this by only calling
    // this method when _bufferpool_managed was set in fixable_page_h. However,
    // that would break single-page recovery, since fix_nonbufferpool_page is
    // required in that case. To fix that, I enabled updating page LSN for any
    // page image, and the check below makes sure it belongs to the buffer pool
    if (!_is_valid_idx(idx)) { return; }

    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t& cb = get_cb(idx);
    w_assert1(cb.latch().is_mine());
    w_assert1(cb.latch().mode() == LATCH_EX);
    w_assert1(cb.get_page_lsn() <= lsn);
    cb.set_page_lsn(lsn);
}

lsn_t bf_tree_m::get_page_lsn(generic_page* p)
{
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx).get_page_lsn();
}

latch_mode_t bf_tree_m::latch_mode(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx).latch().mode();
}

void bf_tree_m::downgrade_latch(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb.latch().held_by_me());
    cb.latch().downgrade();
}

bool bf_tree_m::upgrade_latch_conditional(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb.latch().held_by_me());
    if (cb.latch().mode() == LATCH_EX) {
        return true;
    }
    bool would_block = false;
    cb.latch().upgrade_if_not_block(would_block);
    if (!would_block) {
        w_assert1 (cb.latch().mode() == LATCH_EX);
        return true;
    } else {
        return false;
    }
}

///////////////////////////////////   Page fix/unfix END         ///////////////////////////////////

bool bf_tree_m::is_swizzled(const generic_page* page) const
{
    bf_idx idx = page - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx)._swizzled;
}

bool bf_tree_m::_is_valid_idx(bf_idx idx) const {
    return idx > 0 && idx < _block_cnt;
}


bool bf_tree_m::_is_active_idx (bf_idx idx) const {
    return _is_valid_idx(idx) && get_cb(idx)._used;
}


void pin_for_refix_holder::release() {
    if (_idx != 0) {
        smlevel_0::bf->unpin_for_refix(_idx);
        _idx = 0;
    }
}


PageID bf_tree_m::normalize_pid(PageID pid) const {
    generic_page* page;
    if (is_swizzled_pointer(pid)) {
        bf_idx idx = pid ^ SWIZZLED_PID_BIT;
        page = &_buffer[idx];
        return page->pid;
    }
    return pid;
}

bool pidCmp(const PageID& a, const PageID& b) { return a < b; }

void WarmupThread::fixChildren(btree_page_h& parent, size_t& fixed, size_t max)
{
    btree_page_h page;
    if (parent.get_foster() > 0) {
        page.fix_nonroot(parent, parent.get_foster_opaqueptr(), LATCH_SH);
        fixed++;
        fixChildren(page, fixed, max);
        page.unfix();
    }

    if (parent.is_leaf()) {
        return;
    }

    page.fix_nonroot(parent, parent.pid0_opaqueptr(), LATCH_SH);
    fixed++;
    w_assert1(parent.level() > page.level());
    fixChildren(page, fixed, max);
    page.unfix();

    size_t nrecs = parent.nrecs();
    for (size_t j = 0; j < nrecs; j++) {
        if (fixed >= max) {
            return;
        }
        PageID pid = parent.child_opaqueptr(j);
        page.fix_nonroot(parent, pid, LATCH_SH);
        fixed++;
        w_assert1(parent.level() > page.level());
        fixChildren(page, fixed, max);
        page.unfix();
    }
}

void WarmupThread::run()
{
    size_t npages = smlevel_0::bf->get_size();
    vol_t* vol = smlevel_0::vol;
    stnode_cache_t* stcache = vol->get_stnode_cache();
    vector<StoreID> stids;
    stcache->get_used_stores(stids);

    // Load all pages in depth-first until buffer is full or all pages read
    btree_page_h parent;
    vector<PageID> pids;
    size_t fixed = 0;
    for (size_t i = 0; i < stids.size(); i++) {
        parent.fix_root(stids[i], LATCH_SH);
        fixed++;
        fixChildren(parent, fixed, npages);
    }

    ERROUT(<< "Finished warmup! Pages fixed: " << fixed << " of " << npages <<
            " with DB size " << vol->get_alloc_cache()->get_last_allocated_pid());
}
