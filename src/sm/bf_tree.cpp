/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#include "bf_hashtable.h"
#include "bf_tree_cb.h"
#include "bf_tree_vol.h"
#include "bf_tree_cleaner.h"
#include "bf_tree.h"
#include "bf_tree_inline.h"

#include "smthread.h"
#include "vid_t.h"
#include "generic_page.h"
#include <string.h>
#include "w_findprime.h"
#include <stdlib.h>

#include "sm_int_0.h"
#include "sm_int_1.h"
#include "sm_io.h"
#include "vol.h"
#include "alloc_cache.h"
#include "chkpt_serial.h"

#include <boost/static_assert.hpp>
#include <ostream>
#include <limits>

#include "latch.h"
#include "btree_page_h.h"
#include "log.h"
#include "xct.h"
#include <logfunc_gen.h>

#include "restart.h"

///////////////////////////////////   Initialization and Release BEGIN ///////////////////////////////////

#ifdef PAUSE_SWIZZLING_ON
bool bf_tree_m::_bf_pause_swizzling = true;
uint64_t bf_tree_m::_bf_swizzle_ex = 0;
uint64_t bf_tree_m::_bf_swizzle_ex_fails = 0;
#endif // PAUSE_SWIZZLING_ON


bf_tree_m::bf_tree_m (uint32_t block_cnt,
    uint32_t cleaner_threads,
    uint32_t cleaner_interval_millisec_min,
    uint32_t cleaner_interval_millisec_max,
    uint32_t cleaner_write_buffer_pages,
    const char* replacement_policy,
    bool initially_enable_cleaners,
    bool enable_swizzling) {
    ::memset (this, 0, sizeof(bf_tree_m));

    _block_cnt = block_cnt;
    _enable_swizzling = enable_swizzling;
    if (strcmp(replacement_policy, "clock") == 0) {
        _replacement_policy = POLICY_CLOCK;
    } else if (strcmp(replacement_policy, "clock+priority") == 0) {
        _replacement_policy = POLICY_CLOCK_PRIORITY;
    } else if (strcmp(replacement_policy, "random") == 0) {
        _replacement_policy = POLICY_RANDOM;
    }

#ifdef SIMULATE_NO_SWIZZLING
    _enable_swizzling = false;
    enable_swizzling = false;
    DBGOUT0 (<< "THIS MESSAGE MUST NOT APPEAR unless you intended. Completely turned off swizzling in bufferpool.");
#endif // SIMULATE_NO_SWIZZLING

    DBGOUT1 (<< "constructing bufferpool with " << block_cnt << " blocks of " << SM_PAGESIZE << "-bytes pages... enable_swizzling=" << enable_swizzling);

    // use posix_memalign to allow unbuffered disk I/O
    void *buf = NULL;
    if (::posix_memalign(&buf, SM_PAGESIZE, SM_PAGESIZE * ((uint64_t) block_cnt)) != 0) {
        ERROUT (<< "failed to reserve " << block_cnt << " blocks of " << SM_PAGESIZE << "-bytes pages. ");
        W_FATAL(eOUTOFMEMORY);
    }
    _buffer = reinterpret_cast<generic_page*>(buf);

    // the index 0 is never used. to make sure no one can successfully use it,
    // fill the block-0 with garbages
    ::memset (&_buffer[0], 0x27, sizeof(generic_page));

#ifdef BP_ALTERNATE_CB_LATCH
    // this allocation scheme is sensible only for control block and latch sizes of 64B (cacheline size)
    BOOST_STATIC_ASSERT(sizeof(bf_tree_cb_t) == 64);
    BOOST_STATIC_ASSERT(sizeof(latch_t) == 64);
    // allocate one more pair of <control block, latch> as we want to align the table at an odd
    // multiple of cacheline (64B)
    if (::posix_memalign(&buf, sizeof(bf_tree_cb_t) + sizeof(latch_t), (sizeof(bf_tree_cb_t) + sizeof(latch_t)) * (((uint64_t) block_cnt) + 1LLU)) != 0) {
        ERROUT (<< "failed to reserve " << block_cnt << " blocks of " << sizeof(bf_tree_cb_t) << "-bytes blocks. ");
        W_FATAL(eOUTOFMEMORY);
    }
    ::memset (buf, 0, (sizeof(bf_tree_cb_t) + sizeof(latch_t)) * (((uint64_t) block_cnt) + 1LLU));
    _control_blocks = reinterpret_cast<bf_tree_cb_t*>(reinterpret_cast<char *>(buf) + sizeof(bf_tree_cb_t));
    w_assert0(_control_blocks != NULL);
    for (bf_idx i = 0; i < block_cnt; i++) {
        BOOST_STATIC_ASSERT(sizeof(bf_tree_cb_t) < SCHAR_MAX);
        if (i & 0x1) { /* odd */
            get_cb(i)._latch_offset = -static_cast<int8_t>(sizeof(bf_tree_cb_t)); // place the latch before the control block
        } else { /* even */
            get_cb(i)._latch_offset = sizeof(bf_tree_cb_t); // place the latch after the control block
        }
    }
#else
    if (::posix_memalign(&buf, sizeof(bf_tree_cb_t), sizeof(bf_tree_cb_t) * ((uint64_t) block_cnt)) != 0) {
        ERROUT (<< "failed to reserve " << block_cnt << " blocks of " << sizeof(bf_tree_cb_t) << "-bytes blocks. ");
        W_FATAL(eOUTOFMEMORY);
    }
    _control_blocks = reinterpret_cast<bf_tree_cb_t*>(buf);
    w_assert0(_control_blocks != NULL);
    ::memset (_control_blocks, 0, sizeof(bf_tree_cb_t) * block_cnt);
#endif

#ifdef BP_MAINTAIN_PARENT_PTR
    // swizzled-LRU is initially empty
    _swizzled_lru = new bf_idx[block_cnt * 2];
    w_assert0(_swizzled_lru != NULL);
    ::memset (_swizzled_lru, 0, sizeof(bf_idx) * block_cnt * 2);
    _swizzled_lru_len = 0;
#endif // BP_MAINTAIN_PARENT_PTR

    // initially, all blocks are free
    _freelist = new bf_idx[block_cnt];
    w_assert0(_freelist != NULL);
    _freelist[0] = 1; // [0] is a special entry. it's the list head
    for (bf_idx i = 1; i < block_cnt - 1; ++i) {
        _freelist[i] = i + 1;
    }
    _freelist[block_cnt - 1] = 0;
    _freelist_len = block_cnt - 1; // -1 because [0] isn't a valid block

    //initialize hashtable
    int buckets = w_findprime(1024 + (block_cnt / 4)); // maximum load factor is 25%. this is lower than original shore-mt because we have swizzling
    _hashtable = new bf_hashtable(buckets);
    w_assert0(_hashtable != NULL);

    ::memset (_volumes, 0, sizeof(bf_tree_vol_t*) * MAX_VOL_COUNT);

    // initialize page cleaner
    _cleaner = new bf_tree_cleaner (this, cleaner_threads, cleaner_interval_millisec_min, cleaner_interval_millisec_max, cleaner_write_buffer_pages, initially_enable_cleaners);

    _dirty_page_count_approximate = 0;
    _swizzled_page_count_approximate = 0;

    ::memset (_clockhand_pathway, 0, sizeof(uint32_t) * MAX_CLOCKHAND_DEPTH);
    _clockhand_current_depth = 1;
    _clockhand_pathway[0] = 1;
}

bf_tree_m::~bf_tree_m() {
    if (_control_blocks != NULL) {
#ifdef BP_ALTERNATE_CB_LATCH
        char* buf = reinterpret_cast<char*>(_control_blocks) - sizeof(bf_tree_cb_t);
#else
        char* buf = reinterpret_cast<char*>(_control_blocks);
#endif
        // note we use free(), not delete[], which corresponds to posix_memalign
        ::free (buf);
        _control_blocks = NULL;
    }
#ifdef BP_MAINTAIN_PARENT_PTR
    if (_swizzled_lru != NULL) {
        delete[] _swizzled_lru;
        _swizzled_lru = NULL;
    }
#endif // BP_MAINTAIN_PARENT_PTR
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

    if (_cleaner != NULL) {
        delete _cleaner;
        _cleaner = NULL;
    }
}

w_rc_t bf_tree_m::init ()
{
    W_DO(_cleaner->start_cleaners());
    return RCOK;
}

w_rc_t bf_tree_m::destroy ()
{
    for (volid_t vid = 1; vid < MAX_VOL_COUNT; ++vid) {
        if (_volumes[vid] != NULL) {
            W_DO (uninstall_volume(vid));
        }
    }
    W_DO(_cleaner->request_stop_cleaners());
    W_DO(_cleaner->join_cleaners());
    return RCOK;
}

///////////////////////////////////   Initialization and Release END ///////////////////////////////////

///////////////////////////////////   Volume Mount/Unmount BEGIN     ///////////////////////////////////
w_rc_t bf_tree_m::install_volume(vol_t* volume) {
    w_assert1(volume != NULL);
    volid_t vid = volume->vid().vol;
    w_assert1(vid != 0);
    w_assert1(vid < MAX_VOL_COUNT);

    // CS: introduced this check for now
    // See comment on io_m::mount and BitBucket ticket #3
    if (_volumes[vid] != NULL) {
        // already mounted
        return RCOK;
    }

    w_assert1(_volumes[vid] == NULL);
    DBGOUT1(<<"installing volume " << vid << " to buffer pool...");
#ifdef SIMULATE_MAINMEMORYDB
    W_DO(_install_volume_mainmemorydb(volume));
    if (true) return RCOK;
#endif // SIMULATE_MAINMEMORYDB

    bf_tree_vol_t* desc = new bf_tree_vol_t(volume);

    // load root pages. root pages are permanently fixed.
    stnode_cache_t *stcache = volume->get_stnode_cache();
    std::vector<snum_t> stores (stcache->get_all_used_store_ID());
    w_rc_t rc = RCOK;
    w_rc_t preload_rc;
    for (size_t i = 0; i < stores.size(); ++i) {
        snum_t store = stores[i];
        shpid_t shpid = stcache->get_root_pid(store);
        w_assert1(shpid > 0);

        uint64_t key = bf_key(vid, shpid);
        bf_idx idx = _hashtable->lookup(key);
        preload_rc = RCOK;
        if (0 != idx)
        {
            // Root page is in hash table already
            // Log Analysis forward scan:
            //    This is for a volume mount in concurrent recovery
            //    mode after Log Analysis phase, the root page was loaded
            //    during Log Analysis already, do not load again
            // Log Analysis backward scan:
            //    1. The caller might come from Log Analysis while the root page
            //        was marked as in_doubt but volumn was not loaded.
            //        Load the page for the first time
            //    2. The caller might come from volumn mount after Log Analysis,
            //        while the root page was already loaded during Log Analysis,
            //        do not load the page again
            DBGOUT3(<<"bf_tree_m::install_volume: root page is already in hash table");

            if ((true == restart_m::use_redo_demand_restart()) ||   // On_demand, backward log scan
                (true == restart_m::use_redo_mix_restart()))        // Mixed, backward log scan
            {
                // Root page is special, it is pre-loaded into memory and then recovered.
                //
                // In traditional restart mode (M1 and M2), the root page is pre-loaded during
                // Log Analysis phase but not recovered.  Root page is recovered during REDO
                // phase by the restart thread which blocks user transactions.
                //
                // In on_demand mode (M3 and M4, backward log scan), the root page is
                // pre-loaded and also recovered during Log Analysis phase, because
                // all REDOs are triggered by user transactions and b-tree traversal
                // starts with root page, therefore the root page must be recovered
                // before accepting user transactions, also normal Single Page Recovery is
                // relying on the last page LSN in both parent and child pages to perform
                // the recovery operation on child page, therefore the root page must be
                // recovered before everything else.

                bf_tree_cb_t &cb = get_cb(idx);
                w_rc_t latch_rc = cb.latch().latch_acquire(LATCH_EX, WAIT_IMMEDIATE);
                if (latch_rc.is_error())
                {
                    // Not expected
                    W_FATAL_MSG(fcINTERNAL, << "REDO (redo_page_pass()): unable to EX latch cb on root page");
                }
                w_assert1(true == cb._used);
                if (0 != cb._dependency_lsn)
                {
                    DBGOUT3(<<"bf_tree_m::install_volume: root page is not in buffer pool, loading...");

                    w_assert1(true == cb._in_doubt);

                    // The last page LSN is stored in cb._dependency_lsn during Log Analysis
                    // and cleared after the page has been restored
                    // Use Single Page Recovery to load and recover the root page
                    lsn_t emlsn = cb._dependency_lsn;
                    // Load the page first, no recovery at this point
                    preload_rc = _preload_root_page(desc, volume, store, shpid, idx);
                    if (false == preload_rc.is_error())
                    {
                        // Recover the root page through Single Page Recovery
                        // In order to use minimal logging for page rebalance operations, the root
                        // page must be unmanaged during Single Page Recover process
                        fixable_page_h page;
                        lpid_t store_id(vid, store, shpid);
                        W_COERCE(page.fix_recovery_redo(idx, store_id, false /* managed*/));
                        page.get_generic_page()->pid = store_id;
                        page.get_generic_page()->tag = t_btree_p;
                        w_assert1(page.is_fixed());
                        if (emlsn != page.lsn())
                            page.set_lsns(lsn_t::null);  // set last write lsn to null to force a complete recovery
                        W_COERCE(smlevel_1::recovery->recover_single_page(page, emlsn, true)); // we have the actual emlsn
                        W_COERCE(page.fix_recovery_redo_managed());  // set the page to be managed again

                        smlevel_0::bf->in_doubt_to_dirty(idx);  // Set use and dirty, clear the cb._dependency_lsn flag

                    }
                }
                else
                {
                    // Root page already loaded and recovered, no-op
                    w_assert1(false == cb._in_doubt);
                    DBGOUT3(<<"bf_tree_m::install_volume: root page is already in buffer pool, skip loading");
                }
                // Release EX latch before exit
                if (cb.latch().held_by_me())
                   cb.latch().latch_release();

            // Done with recovery the root page, at this point the
            // root page is in memory and it has been REDOne, but
            // not UNDOne yet, meaning the root page might contain
            // data which need to be rollback.  The UNDO operation
            // will be triggered by user transaction due to lock conflict (M3)
            // or transaction drive UNDO (M4) if the restart thread reaches it first
            }
            else
            {
                // Forward log scan with traditional recovery, page already loaded
                DBGOUT3(<<"bf_tree_m::install_volume: traditional restart, skip re-loading of root page");
            }

            // Store the root page index into descriptor
            desc->_root_pages[store] = idx;
        }
        else
        {
            DBGOUT3(<<"bf_tree_m::install_volume: root page is not in hash table, loading...");
            // Root page is not in hash table, pre-load it
            w_rc_t grab_rc = _grab_free_block(idx);
            if (grab_rc.is_error())
            {
                ERROUT(<<"failed to grab a free page while mounting a volume: " << grab_rc);
                rc = grab_rc;
                break;
            }
            // Load the root page
            preload_rc = _preload_root_page(desc, volume, store, shpid, idx);
        }

        if (preload_rc.is_error())
        {
            ERROUT(<<"failed to preload a root page " << shpid
                   << " of store " << store << " in volume "
                   << vid << ". to buffer frame " << idx << ". err=" << preload_rc);
            rc = preload_rc;
            _add_free_block(idx);
            break;
        }
    }
    if (rc.is_error()) {
        ERROUT(<<"install_volume failed: " << rc);
        for (size_t i = 0; i < stores.size(); ++i) {
            bf_idx idx = desc->_root_pages[stores[i]];
            if (idx != 0) {
                get_cb(idx).clear();
                _add_free_block(idx);
            }
        }
        delete desc;
        return rc;
    } else {
        _volumes[vid] = desc;
        return RCOK;
    }
}

w_rc_t bf_tree_m::_preload_root_page(bf_tree_vol_t* desc, vol_t* volume, snum_t store, shpid_t shpid, bf_idx idx) {
    volid_t vid = volume->vid().vol;
    DBGOUT2(<<"preloading root page " << shpid << " of store " << store << " in volume " << vid << ". to buffer frame " << idx);
    w_assert1(shpid >= volume->first_data_pageid());
    bool past_end;
    W_DO(volume->read_page(shpid, _buffer[idx], past_end));

    // _buffer[idx].checksum == 0 is possible when the root page has been never flushed out.
    // this method is called during volume mount (even before recover), crash tests like
    // test_restart, test_crash might encounter this scenario, swallow the error
    if (_buffer[idx].checksum == 0) {
        DBGOUT1(<<"empty check sum root page during volume mount. crash happened?"
            " pid=" << shpid << " of store " << store << " in volume "
            << vid << ". to buffer frame " << idx);
    }
    else if (_buffer[idx].calculate_checksum() != _buffer[idx].checksum) {
        return RC(eBADCHECKSUM);
    }

    bf_tree_cb_t &cb = get_cb(idx);
    cb.clear();
    cb._pid_vol = vid;
    cb._pid_shpid = shpid;
    cb._store_num = store;

    // as the page is read from disk, at least it's sure that
    // the page is flushed as of the page LSN (otherwise why we can read it!)
    // if the page does not exist on disk, the page would be zero out already
    // so the cb._rec_lsn (initial dirty lsn) would be 0
    cb._rec_lsn = _buffer[idx].lsn.data();
    w_assert3(_buffer[idx].lsn.hi() > 0);
    cb.pin_cnt_set(1); // root page's pin count is always positive
    cb._swizzled = true;

    // When install volume, if the root page was not already loaded, then the in_doubt
    // page is off initially
    // The only possibility the root page was already loaded: install volume after Log Analysis
    // during a concurrent recovery, while the root page was loaded during Log Analysis
    cb._in_doubt = false;

    cb._recovery_access = false;
    cb._used = true; // turn on _used at last
    cb._dependency_lsn = 0;
    bool inserted = _hashtable->insert_if_not_exists(bf_key(vid, shpid), idx); // for some type of caller (e.g., redo) we still need hashtable entry for root
    if (!inserted)
    {
        if (smlevel_0::in_recovery())
        {
            // If we are loading the root page from Recovery Log Analysis phase,
            // it is possible we have a regular log record accessing the root page
            // before the device mounting log record, in this case the root page
            // has been registered into the hash table before we mount the device.
            // This is not an error condition therefore swallow the error, also mark
            // the page as an in_doubt page
            DBGOUT3(<<"Loaded the root page but the index was in hashtable already, set the in_doubt flag to true");
            cb._in_doubt = true;
            inserted = true;
        }
        else
        {
            ERROUT (<<"failed to insert a root page to hashtable. this must not have happened because there shouldn't be any race. wtf");
            return RC(eINTERNAL);
        }
    }
    w_assert1(inserted);

    desc->_root_pages[store] = idx;
    return RCOK;
}

w_rc_t bf_tree_m::_install_volume_mainmemorydb(vol_t* volume) {
    volid_t vid = volume->vid().vol;
    DBGOUT1(<<"installing volume " << vid << " to MAINMEMORY-DB buffer pool...");
    for (volid_t v = 1; v < MAX_VOL_COUNT; ++v) {
        if (_volumes[v] != NULL) {
            ERROUT (<<"MAINMEMORY-DB mode allows only one volume to be loaded, but volume " << v << " is already loaded.");
            return RC(eINTERNAL);
        }
    }

    // load all pages. pin them forever
    bf_idx endidx = volume->num_pages();
    for (bf_idx idx = volume->first_data_pageid(); idx < endidx; ++idx) {
        if (volume->is_allocated_page(idx)) {
            bool past_end;
            W_DO(volume->read_page(idx, _buffer[idx], past_end));
            if (true == past_end)
            {
                // Page does not exist on disk, raise error since this should not happen
                return RC(stSHORTIO);
            }
            if (_buffer[idx].calculate_checksum() != _buffer[idx].checksum) {
                return RC(eBADCHECKSUM);
            }
            bf_tree_cb_t &cb = get_cb(idx);
            cb.clear();
            cb._pid_vol = vid;
            cb._pid_shpid = idx;
            cb._rec_lsn = _buffer[idx].lsn.data();
            w_assert3(_buffer[idx].lsn.hi() > 0);
            cb.pin_cnt_set(1);
            cb._in_doubt = false;
            cb._recovery_access = false;
            cb._used = true;
            cb._swizzled = true;
        }
    }
    // now freelist is inconsistent, but who cares. this is mainmemory-db experiment FIXME

    bf_tree_vol_t* desc = new bf_tree_vol_t(volume);
    stnode_cache_t *stcache = volume->get_stnode_cache();
    std::vector<snum_t> stores (stcache->get_all_used_store_ID());
    for (size_t i = 0; i < stores.size(); ++i) {
        snum_t store = stores[i];
        bf_idx idx = stcache->get_root_pid(store);
        w_assert1(idx > 0);
        desc->_root_pages[store] = idx;
    }
    _volumes[vid] = desc;
    return RCOK;
}

w_rc_t bf_tree_m::uninstall_volume(volid_t vid,
                                     const bool clear_cb) // In: true to clear buffer pool cb data
                                                          //     do not clear cb if
{
    // assuming this thread is the only thread working on this volume,

    // first, clean up all dirty pages
    DBGOUT1(<<"uninstalling volume " << vid << " from buffer pool...");
    bf_tree_vol_t* desc = _volumes[vid];
    if (desc == NULL) {
        DBGOUT0(<<"this volume is already uninstalled: " << vid);
        return RCOK;
    }
    W_DO(_cleaner->force_volume(vid));

    // If caller is a concurrent recovery and call 'dismount_all' after
    // Log Analysys phase, do not clear the buffer pool because the remaining
    // Recovery work (REDO and UNDO) is depending on infomration
    // stored in buffer pool cb and transaction table

    if (true == clear_cb)
    {
        // then, release all pages.
        for (bf_idx idx = 1; idx < _block_cnt; ++idx)
        {
            bf_tree_cb_t &cb = get_cb(idx);
            if (!cb._used || cb._pid_vol != vid)
            {
            continue;
            }
#ifdef BP_MAINTAIN_PARENT_PTR
            // if swizzled, remove from the swizzled-page LRU too
            if (_is_in_swizzled_lru(idx)) {
                _remove_from_swizzled_lru(idx);
            }
#endif // BP_MAINTAIN_PARENT_PTR
            _hashtable->remove(bf_key(vid, cb._pid_shpid));
            if (cb._swizzled)
            {
                --_swizzled_page_count_approximate;
            }
            get_cb(idx).clear();
            _add_free_block(idx);
        }
    }
    else
    {
        DBGOUT3(<<"bf_tree_m::uninstall_volume: no buffer pool cleanup");
    }

    _volumes[vid] = NULL;
    delete desc;
    return RCOK;
}
///////////////////////////////////   Volume Mount/Unmount END       ///////////////////////////////////

///////////////////////////////////   Page fix/unfix BEGIN         ///////////////////////////////////
// NOTE most of the page fix/unfix functions are in bf_tree_inline.h.
// These functions are here are because called less frequently.

w_rc_t bf_tree_m::fix_direct (generic_page*& page, volid_t vol, shpid_t shpid, latch_mode_t mode, bool conditional, bool virgin_page) {
    // fix_direct is for REDO operation, rollback and cursor re-fix, both root and non-root page, no parent
    // mark it as from Recovery so we do not check access availability
    return _fix_nonswizzled(NULL, page, vol, shpid, mode, conditional, virgin_page, true /*from_recovery*/);
}

void bf_tree_m::associate_page(generic_page*&_pp, bf_idx idx, lpid_t page_updated)
{
    // Special function for REDO phase of the system Recovery process
    // The physical page is loaded in buffer pool, idx is known but we
    // need to associate it with fixable_page data structure
    // Swizzling must be off

    w_assert1(!smlevel_0::bf->is_swizzling_enabled());
    w_assert1 (_is_active_idx(idx));
    _pp = &(smlevel_0::bf->_buffer[idx]);

    // Store lptid_t (vol and store IDs and page number) into the data buffer in the page
    _buffer[idx].pid = page_updated;

    return;
}

w_rc_t bf_tree_m::_fix_nonswizzled_mainmemorydb(generic_page* parent, generic_page*& page, shpid_t shpid, latch_mode_t mode, bool conditional, bool virgin_page) {
    bf_idx idx = shpid;
    bf_tree_cb_t &cb = get_cb(idx);
#ifdef BP_MAINTAIN_PARENT_PTR
    bf_idx parent_idx = 0;
    if (is_swizzling_enabled()) {
        parent_idx = parent - _buffer;
        w_assert1 (_is_active_idx(parent_idx));
        cb._parent = parent_idx;
    }
#endif // BP_MAINTAIN_PARENT_PTR
    if (virgin_page) {
        cb._rec_lsn = 0;
        cb._dirty = true;
        cb._in_doubt = false;
        cb._recovery_access = false;
        cb._uncommitted_cnt = 0;
        ++_dirty_page_count_approximate;
        bf_idx parent_idx = parent - _buffer;
        cb._pid_vol = get_cb(parent_idx)._pid_vol;
        cb._pid_shpid = idx;
        cb.pin_cnt_set(1);
    }
    cb._used = true;
    w_rc_t rc = cb.latch().latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER);
    if (rc.is_error()) {
        DBGOUT2(<<"bf_tree_m: latch_acquire failed in buffer frame " << idx << " rc=" << rc);
    } else {
        page = &(_buffer[idx]);
    }
    return rc;
}

w_rc_t bf_tree_m::_fix_nonswizzled(generic_page* parent, generic_page*& page,
                                   volid_t vol, shpid_t shpid,
                                   latch_mode_t mode, bool conditional,
                                   bool virgin_page,
                                   const bool from_recovery)  // True if caller is from recovery UNDO operation
{
    // Main function to set up a page in buffer pool before it can be used for various purposes.
    // Page index not in hashtable: loads a page from disk into buffer pool memory, setup proper
    //     fields in cb, register the page in hashtable, and then return page pointer to caller.
    // Page index in hashtable: assumping page is in memory already, does some validation
    //     and then return page pointer to caller.
    //
    // With concurrent Recovery, the system is opened after Log Analysis phase, and in_doubt
    // pages will be loaded into buffer pool while concurrent transactions are coming in.
    // It is possible a user transaction asks for a page before the REDO phase gets to
    // load the page (M2), in such case the page index is in hashtable but the actual page has
    // not been loaded into memory yet.

    // Note in 'restart_m', it does not use this function to load page into buffer pool, because
    // restart_m is driven by in_doubt flag and has different requirements on loading pages.

    w_assert1(vol != 0);
    w_assert1(shpid != 0);
    w_assert1((shpid & SWIZZLED_PID_BIT) == 0);
#ifdef BP_MAINTAIN_PARENT_PTR
    w_assert1(!is_swizzling_enabled() || parent != NULL);
#endif
    bf_tree_vol_t *volume = _volumes[vol];
    w_assert1(volume != NULL);
    w_assert1(shpid >= volume->_volume->first_data_pageid());
#ifdef SIMULATE_MAINMEMORYDB
    W_DO (_fix_nonswizzled_mainmemorydb(parent, page, shpid, mode, conditional, virgin_page));
    if (true) return RCOK;
#endif // SIMULATE_MAINMEMORYDB

    // unlike swizzled case, this is complex and inefficient.
    // we need to carefully follow the protocol to make it safe.
    uint64_t key = bf_key(vol, shpid);

    // Force loading the data even if the page is in hash table already, this could happen
    // during Recovery REDO phase, all 'in_doubt' pages are in hash table but not loaded
    // an page REDO operation might need to access a second page (multi-page operations
    // such as page rebalance and page merge), while the second page might still be in_doubt
    // This flag is used for Recovery operation only
    bool force_load = false;

    // note that the hashtable is separated from this bufferpool.
    // we need to make sure the returned block is still there, and retry otherwise.
#if W_DEBUG_LEVEL>0
    int retry_count = 0;
#endif
    while (true)
    {
        const uint32_t ONE_MICROSEC = 10000;
#if W_DEBUG_LEVEL>0
        if (++retry_count % 10000 == 0) {
            DBGOUT1(<<"keep trying to fix.. " << shpid << ". current retry count=" << retry_count);
        }
#endif
        bf_idx idx = _hashtable->lookup(key);
        if ((idx == 0) || (true == force_load))
        {
            // 1. page is not registered in hash table therefore it is not an in_doubt page either
            // 2. page is registered in hash table already but force loading, meaning it is an
            //    in_doubt page and the actual page has not been loaded into buffer pool yet
            // We need to read it from disk
            lsn_t page_emlsn = lsn_t::null;
            if (true == force_load)
            {
                // If force load, page is registered in hash table already but the
                // actual page was not loaded
                w_assert1(0 != idx);
            }
            else
            {
                W_DO(_grab_free_block(idx)); // get a frame that will be the new page
            }
            w_assert1(idx != 0);
            bf_tree_cb_t &cb = get_cb(idx);

            // Acquire latch before loading the page, this is necessary for 'force load' on in_doubt
            // page, because restart thread (M2/M4) or concurrent transactions (M4) might be
            // loading the same page at this moment
            w_rc_t check_rc = cb.latch().latch_acquire(mode, sthread_t::WAIT_IMMEDIATE);
            if (check_rc.is_error())
            {
                // Cannot latch cb, probably a concurrent user transaction is trying to load this
                // page, or in M4 (mixed mode) the page is being recoverd, rare but possible, try again
                if (false == force_load)
                    _add_free_block(idx);

                ERROUT(<<"bf_tree_m: unlucky! not able to acquire cb on a force load page "
                        << shpid << ". Discard my own work and try again.");

                force_load = false;
                // Sleep a while to give the other process time to finish its work and then retry
                ::usleep(ONE_MICROSEC);
                continue;
            }

            // Now we have a latch on page cb
            if ((true == force_load) && (false == cb._in_doubt))
            {
                // Someone beat us on loading the page so the page is no longer in_doubt
                // we do not want to reload the page, therefore stop the current load and retry
                // Do not free the block since we did not allocated it initially
                if (cb.latch().held_by_me())
                    cb.latch().latch_release();

                force_load = false;
                continue;
            }

            DBGOUT3(<<"unswizzled case: load shpid = " << shpid << " into frame = " << idx);
            // after here, we must either succeed or release the free block
            if (virgin_page)
            {
                // except a virgin page. then the page is anyway empty
                DBGOUT3(<<"bf_tree_m: adding a virgin page ("<<vol<<"."<<shpid<<")to bufferpool.");
            }
            else
            {
                DBGOUT3(<<"bf_tree_m: cache miss. reading page "<<vol<<"." << shpid << " to frame " << idx);
                INC_TSTAT(bf_fix_nonroot_miss_count);
                bool past_end;
                w_rc_t read_rc = volume->_volume->read_page(shpid, _buffer[idx], past_end);

                // Not checking 'past_end' (stSHORTIO), because if the page does not exist
                // on disk (only if fix_direct from REDO operation), the page context will be zero out,
                // and the following logic (_check_read_page) will fail in checksum and try Single-Page-Recovery immediatelly
                // Note that for fix_direct from REDO, we do not have parent page pointer

                if (read_rc.is_error())
                {
                    DBGOUT3(<<"bf_tree_m: error while reading page " << shpid << " to frame " << idx << ". rc=" << read_rc);

                    if (cb.latch().held_by_me())
                        cb.latch().latch_release();

                    if (false == force_load)
                        _add_free_block(idx);
                    return read_rc;
                    /* TODO: if this is an I/O error, we could try to do Single-Page-Recovery on it. */
                }
                else
                {
                    // Loaded the page from disk successfully
                    // for each page retrieved from disk, check validity, possibly apply Single-Page-Recovery.
                    // this is not a virgin page so if the page does not exist on disk, we need to
                    // try the backup
                    // Scenarios:
                    // 1. Normal page loading due to user transaction accessing a page not in buffer pool
                    // 2. Traditional recovery UNDO or REDO (force_load)
                    // 3. On-demand or mixed REDO triggered by user transaction (force_load)

                    if (true == force_load)
                    {
                        // If this is a force load, get the last page update lsn from page cb for recovery purpose
                        if (0 != cb._dependency_lsn)
                        {
                            // Copy over the last update lsn if it is valid lsn
                            // and reset it to 0 after copied it, this is to prevent other
                            // user transaction using this value for recovery purpose
                            // (note the last update lsn is gone after this reset)
                            // This is needed because the slot in buffer pool is already decided,
                            // we cannot have multiple user transactions recoverying the same
                            // page in buffer pool slot, it would corrupt the page context
                            page_emlsn = cb._dependency_lsn;
                        }
                        else
                        {
                            // Someone else beat us in recoverying this page, try again
                            ERROUT(<<"bf_tree_m: unlucky! it was a force load and no last update lsn"
                                    << ", most likely another thread is loading this page "
                                    << shpid << " to buffer pool currently. Discard my own work and try again.");

                            if (cb.latch().held_by_me())
                                cb.latch().latch_release();
                            force_load = false;
                            // Sleep a while to give the other process time to finish its work
                            ::usleep(ONE_MICROSEC);
                            continue;
                        }
                        w_assert1(lsn_t::null != page_emlsn);
                    }

                    if (true == force_load)
                    {
                        DBGOUT3(<< "Force load - caller ask for page: " << shpid);
                    }
                    else
                    {
                        shpid_t root_shpid = smlevel_0::bf->get_root_page_id(vol, cb._store_num);                    
                        DBGOUT3(<< "User load - Root page ID: " << root_shpid << ", caller ask for page: " << shpid);
                        if (root_shpid == shpid)
                        {
                            DBGOUT0(<< "User load - Root page is the same as asked page ID: " << root_shpid);
// TODO(Restart)... infinite loop when we encounter this situation, it is timing related issue, what is causing it?
//                            W_FATAL(fcINTERNAL);
                        }

                    }


                    check_rc = _check_read_page(parent, idx, vol, shpid, past_end, page_emlsn);
                    if (check_rc.is_error())
                    {
                        // Free the block even if we did not acquire it in the first place (force load), this is
                        // because we are erroring out
                        _add_free_block(idx);

                        if (cb.latch().held_by_me())
                            cb.latch().latch_release();
                        return check_rc;
                    }
                }
            }

            // initialize control block
            // we don't have to atomically pin it because it's not referenced by any other yet

            // We should still have the latch at this point
            w_assert1(cb.latch().held_by_me());

            // Before we clear the page cb, check again because if force load
            // the in_doubt flag should still be on at this point
            if (true == force_load)
                w_assert1(true == cb._in_doubt);

            cb.clear_except_latch();
            cb._pid_vol = vol;
            cb._pid_shpid = shpid;
            cb._dependency_lsn = 0;
#ifdef BP_MAINTAIN_PARENT_PTR
            bf_idx parent_idx = 0;
            if (is_swizzling_enabled()) {
                parent_idx = parent - _buffer;
                w_assert1 (_is_active_idx(parent_idx));
                cb._parent = parent_idx;
            }
#endif // BP_MAINTAIN_PARENT_PTR
            if (!virgin_page)
            {
                // if the page is read from disk, at least it's sure that
                // the page is flushed as of the page LSN (otherwise why we can read it!)
                cb._rec_lsn = _buffer[idx].lsn.data();
                w_assert3(_buffer[idx].lsn.hi() > 0);
            }
            else
            {
                // Virgin page, we are not setting _rec_lsn (initial dirty)
                // Page format would set the _rec_lsn
                cb._dirty = true;
                cb._in_doubt = false;
                cb._recovery_access = false;
                cb._uncommitted_cnt = 0;
                ++_dirty_page_count_approximate;
            }
            cb._used = true;
            cb._refbit_approximate = BP_INITIAL_REFCOUNT;
#ifdef BP_MAINTAIN_REPLACEMENT_PRIORITY
            cb._replacement_priority = me()->get_workload_priority();
#endif

            // finally, register the page to the hashtable.
            bool registered = _hashtable->insert_if_not_exists(key, idx);
            if (!registered)
            {
                if (false == force_load)
                {
                    // If force_load flag is off
                    // this pid already exists in bufferpool. this means another thread concurrently
                    // added the page to bufferpool. unlucky, but can happen.
                    DBGOUT1(<<"bf_tree_m: unlucky! another thread already added the page " << shpid << " to the bufferpool. discard my own work on frame " << idx);
                    if (cb.latch().held_by_me())
                        cb.latch().latch_release();
                    cb.clear(); // well, it should be enough to clear _used, but this is anyway a rare event. wouldn't hurt to clear all.
                    _add_free_block(idx);
                    continue;
                }
                else
                {
                    // If the force_load flag is on and the pid is already in hash table,
                    // this is a force load scenario
                    // In this case an in_doubt page is loaded due to page driven
                    // Single-Page-Recovery REDO operation or on-demand user
                    // transaction triggered REDO operation
                    // Clear the in_doubt flag on the page so we do not try to load
                    // this page again
                    DBGOUT1(<<"bf_tree_m: force_load with page in hash table already, mark page dirty, page id: " << shpid);
                    w_assert1(0 != idx);
                    force_load = false;
                    in_doubt_to_dirty(idx);        // Reset in_doubt and dirty flags accordingly
                }
            }

            // Clear the force_load flag since we are done
            w_assert1(false == force_load);

            // okay, all done
#ifdef BP_MAINTAIN_PARENT_PTR
            if (is_swizzling_enabled()) {
                lintel::unsafe::atomic_fetch_add((uint32_t*) &(_control_blocks[parent_idx]._pin_cnt), 1); // we installed a new child of the parent      to this bufferpool. add parent's count
            }
#endif // BP_MAINTAIN_PARENT_PTR
            page = &(_buffer[idx]);

            // Just loaded a page, the page loading was due to:
            // User transaction - page was not involved in the recovery operation
            // UNDO operation - the page is needed for b-tree search traversal of
            //                            an UNDO operation
            // REDO operation - on-demand or mixed REDO triggered by user transaction
            //                            page was in_doubt (using lock for concurrency control)
            // Page is safe to access, not calling _validate_access(page)
            // for page access validation purpose
            DBGOUT3(<<"bf_tree_m::_fix_nonswizzled: retrieved a new page: " << shpid);

            return RCOK;
        }
        else
        {
            // Page index is registered in hash table
            force_load = false;

            // unlike swizzled case, we have to atomically pin it while verifying it's still there.
            if (parent) {
                DBGOUT3(<<"swizzled case: parent = " << parent->pid << ", shpid = " << shpid << " frame=" << idx);
            } else {
                DBGOUT3(<<"swizzled case: parent = NIL"<< ", shpid = " << shpid << " frame=" << idx);
            }
            bf_tree_cb_t &cb = get_cb(idx);

            shpid_t root_shpid = smlevel_0::bf->get_root_page_id(vol, cb._store_num);

            if ((cb._in_doubt) && (root_shpid != shpid))
            {
                // If in_doubt flag is on and not the root page, the page is not
                // in buffer pool memory
                // we do not have latch on the page at this point, okay to return

                // bf_tree_m::_fix_nonswizzled is called by two different functions:
                //   fix_direct: for REDO operation, rollback and cursor re-fix, both root
                //                  and non-root page, no parent
                //   fix_nonroot: user txn and UNDO operations to traversal the tree
                //                      only non-root pages, has parent
                //
                // Exception: root page is pre-loaded into buffer pool when mounting
                // volume, although the root page could be either in_doubt or clean,
                // the root page is in memory already.

                DBGOUT3(<<"bf_tree_m::_fix_nonswizzled: page in hash table but not in buffer pool, page: "
                        << shpid << ", root page: " << root_shpid);

                if (NULL == parent)
                {
                    // No parent, caller is from fix_direct, do not block
                    // Set force_load to cause the retry logic to load the page
                    DBGOUT3(<<"bf_tree_m::_fix_nonswizzled: force load due to fix_direct");
                    force_load = true;
                    continue;
                }
                else
                {
                    // Has parent, caller is from fix_nonroot, it is either a user transaction
                    // tree traversal from UNDO recovery
                    if (true == from_recovery)
                    {
                        // If caller is from recovery (e.g. rollback), do not block
                        // page is in_doubt, set force_load to cause the retry logic to load the page
                        DBGOUT3(<<"bf_tree_m::_fix_nonswizzled: force load due to rollback");
                        force_load = true;
                        continue;
                    }
                    else if (true == restart_m::use_concurrent_commit_restart())  // Using commit_lsn
                    {
                        // User transaction
                        // Concurrent log mode (M2) is using commit_lsn for concurrency control
                        // M2 does not support pure on-demand REDO, therefore
                        // user transaction cannot load an in-doubt page

                        w_assert1(false == restart_m::use_redo_demand_restart());
                        w_assert1(false == restart_m::use_redo_mix_restart());

                        return RC(eACCESS_CONFLICT);
                    }
                    else if (true == restart_m::use_concurrent_lock_restart())  // Using lock acquisition
                    {
                        // User transaction
                        // This is the normal lock mode which is using lock for concurrency control

                        if ((true == restart_m::use_redo_demand_restart()) ||  // pure on-demand
                           (true == restart_m::use_redo_mix_restart()))        // midxed mode
                        {
                            // If either pure on-demand or mixed mode REDO, allow user transaction
                            // to trigger in_doubt page loading

                            // Set the force_load so we will load the in_doubt page and also trigger
                            // Single Page Recovery on the page
                            DBGOUT3(<<"bf_tree_m::_fix_nonswizzled: force load triggered by on-demand user transaction and lock");
                            force_load = true;
                            continue;
                        }
                        else if ((false == restart_m::use_redo_demand_restart()) &&    // Not pure on-demand
                                (false == restart_m::use_redo_mix_restart()))          // Not midxed mode
                        {
                            // Using lock for concurrency control but not on-demand REDO
                            // This is a mode mainly for performance measurement purpose

                            // User transaction cannot load an in_doubt page, only REDO can
                            // load an in_doubt page

                            return RC(eACCESS_CONFLICT);
                        }
                        else
                        {
                            // Unexpected mode
                            W_FATAL_MSG(fcINTERNAL,
                                        << "bf_tree_m::_fix_nonswizzled: unexpected recovery mode under lock concurrency control mode");
                        }
                    }
                    else
                    {
                        // Unexpected mode
                        W_FATAL_MSG(fcINTERNAL,
                                    << "bf_tree_m::_fix_nonswizzled: unexpected concurrency control recovery mode");
                    }
                }
            }

            // Page is registered in hash table and it is not an in_doubt page, meaning the actual page is in buffer pool already

            int32_t cur_cnt = cb.pin_cnt();
            if (cur_cnt < 0) {
                w_assert1(cur_cnt == -1);
                DBGOUT1(<<"bf_tree_m: very unlucky! buffer frame " << idx << " has been just evicted. retrying..");
                continue;
            }
#ifndef NO_PINCNT_INCDEC
            int32_t cur_ucnt = cur_cnt;
            if (lintel::unsafe::atomic_compare_exchange_strong(const_cast<int32_t*>(&cb._pin_cnt), &cur_ucnt, cur_ucnt + 1))
            {
#endif
                // okay, CAS went through
                if (cb._refbit_approximate < BP_MAX_REFCOUNT) {
                    ++cb._refbit_approximate;
                }
                w_rc_t rc = cb.latch().latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER);
                // either successfully or unsuccessfully, we latched the page.
                // we don't need the pin any more.
                // here we can simply use atomic_dec because it must be positive now.
                w_assert1(cb.pin_cnt() > 0);
                w_assert1(cb._pid_vol == vol);
                if (cb._pid_shpid != shpid) {
                    DBGOUT1(<<"cb._pid_shpid = " << cb._pid_shpid << ", shpid = " << shpid);
                }
                w_assert1(cb._pid_shpid == shpid);
#ifndef NO_PINCNT_INCDEC
                lintel::unsafe::atomic_fetch_sub((uint32_t*)(&cb._pin_cnt), 1);
#endif
                if (rc.is_error())
                {
                    DBGOUT2(<<"bf_tree_m: latch_acquire failed in buffer frame " << idx << " rc=" << rc);
                }
                else
                {
                    page = &(_buffer[idx]);

                    // Page was already loaded in buffer pool by:
                    //     Recovery operation - was an in_doubt page
                    // or
                    //     Previous user transaction - was not an in_doubt page
                    // we need to validate the page only if running in concurrent mode
                    // and the request coming from a user transaction
                    //
                    // Two scenarios:
                    //     User transaction - validate
                    //     Recovery operation - allow
                    //                    from_recovery - UNDO operation
                    //                    cb._recovery_access - REDO operation
                    //         Two different ways to indicate caller from recovery, because
                    //         REDO is page driven so we can mark cb._recovery_access
                    //         but UNDO is transaction driven, not able to associate it to target
                    //         page and also it has to traversal B-tree (visit multiple pages), so
                    //         it is relying on input parameter 'from_recovery'

                    if ((true == from_recovery) || (true == cb._recovery_access))
                    {
                        // From Recovery, no validation
                        DBGOUT3(<<"bf_tree_m::_fix_nonswizzled: an existing page from recovery, skip check for accessability, page: " << shpid);
                    }
                    else
                    {
                        DBGOUT3(<<"bf_tree_m::_fix_nonswizzled: an existing page from user txn, check for accessability, page: " << shpid);
                        rc = _validate_access(page);
                        if (rc.is_error())
                        {
                            ++cb._refbit_approximate;
                            if (cb.latch().held_by_me())
                                cb.latch().latch_release();
                        }
                    }

                    // On successful return, we are holding latch on this page
                }
                return rc;
#ifndef NO_PINCNT_INCDEC
            } else {
                // another thread is doing something. keep trying.
                DBGOUT1(<<"bf_tree_m: a bit unlucky! buffer frame " << idx << " has contention. cb._pin_cnt=" << cb._pin_cnt <<", expected=" << cur_ucnt);
                continue;
            }
#endif
        }
    }
}

w_rc_t bf_tree_m::_validate_access(generic_page*& page)   // pointer to the underlying page _pp
{
    // Special function to validate whether a page is safe to be accessed by
    // concurrent user transaction while the recovery is still going on
    // Page is already loaded in buffer pool, and the page_lsn (last write)
    // is available.

    // Validation is on only if using commit_lsn for concurrency control
    //    Commit_lsn: raise error if falid the commit_lsn validation
    //    Lock re-acquisition: block if lock conflict

    // If lock re-acquisition is used, if user transaction is trying to load an
    // in_doubt page, it triggers on-demand REDO, the code path should not
    // come here.
    // If user transaction is accessing an already loaded buffer pool page,
    // the blocking happens during lock acquisition (which happens later),
    // and there is no need to validate in the page level.

    // If the system is doing concurrent recovery and we are still in the middle
    // of recovery, need to validate whether a user transaction can access this page
    if ((smlevel_1::recovery) &&                      // The restart_m object is valid
        (smlevel_1::recovery->restart_in_progress())) // Restart is in progress, both serial and concurrent
                                                      // if pure on_demand, then this function returns
                                                      // false (not in restart) after Log Analysis phase
    {
        if (true ==  restart_m::use_concurrent_commit_restart()) // Using commit_lsn
        {
            // Accept a new transaction if the associated page met the following conditions:
            // REDO phase:
            //    1. Page is not an 'in_doubt' page - 'dirty' is okay.
            //    2. Page_LSN < Commit_LSN (minimum Txn LSN of all loser transactions).
            // UNDO phase:
            //    Page_LSN < Commit_LSN (minimum Txn LSN of all loser transactions).

            // With the M2 limitation, user transaction does not load in_doubt page (raise error),
            // so we only need to validate one condition for the newly loaded page:
            //    Page_LSN < Commit_LSN (minimum Txn LSN of all loser transactions)
            // The last write on the page was before the minimum TXN lsn of all loser transactions
            // Note this is an over conserve policy mainly because we do not have
            // lock acquisition to protect loser transactions.

            if (lsn_t::null == smlevel_0::commit_lsn)
            {
                // commit_lsn == null only if empty database or no
                // actual work for recovery
                // All access are allowed in this case
            }
            else
            {
                // If the page is not marked being access for recovery purpose
                // then we need to validate the accessability

                // Get the last write on the page
                lsn_t page_lsn = page->lsn;
                if (page_lsn >= smlevel_0::commit_lsn)
                {
                    // Condition not met, cannot continue
                    DBGOUT2(<<"bf_tree_m: page access condition not met, page_lsn: "
                            << page_lsn << ", commit_lsn:" << smlevel_0::commit_lsn);
                    return RC(eACCESS_CONFLICT);
                }
                else
                {
                    DBGOUT2(<<"bf_tree_m: page access condition met, page_lsn: "
                            << page_lsn << ", commit_lsn:" << smlevel_0::commit_lsn);
                }
            }
        }
        else if (true == restart_m::use_concurrent_lock_restart())
        {
            // Using lock for concurrency control, no need to validate page
            // which is already loaded into buffer pool (not in_doubt)
        }
    }
    else
    {
        // Serial recovery mode or concurrent recovery mode but recovery has completed
        // No need to validate for page accessability
    }
    return RCOK;
}

bf_idx bf_tree_m::pin_for_refix(const generic_page* page) {
    w_assert1(page != NULL);
    w_assert1(latch_mode(page) != LATCH_NL);
    bf_idx idx = page - _buffer;
    w_assert1(_is_active_idx(idx));
#ifdef SIMULATE_MAINMEMORYDB
    if (true) return idx;
#endif // SIMULATE_MAINMEMORYDB
    // this is just atomic increment, not a CAS, because we know
    // the page is latched and eviction thread wouldn't consider this block.
    w_assert1(get_cb(idx).pin_cnt() >= 0);
    get_cb(idx).pin_cnt_atomic_inc(1);
    return idx;
}

void bf_tree_m::unpin_for_refix(bf_idx idx) {
    w_assert1(_is_active_idx(idx));
    w_assert1(get_cb(idx).pin_cnt() > 0);
#ifdef SIMULATE_MAINMEMORYDB
    if (true) return;
#endif // SIMULATE_MAINMEMORYDB
    get_cb(idx).pin_cnt_atomic_dec(1);
}

///////////////////////////////////   Page fix/unfix END         ///////////////////////////////////

///////////////////////////////////   Dirty Page Cleaner BEGIN       ///////////////////////////////////
w_rc_t bf_tree_m::force_all() {
    // Buffer pool flush, it flushes dirty pages
    // It does not flush in_doubt pages
    return _cleaner->force_all();
}
w_rc_t bf_tree_m::force_until_lsn(lsndata_t lsn) {
    // Buffer pool flush, it flushes dirty pages
    // It does not flush in_doubt pages
    return _cleaner->force_until_lsn(lsn);
}
w_rc_t bf_tree_m::force_volume(volid_t vol) {
    return _cleaner->force_volume(vol);
}
w_rc_t bf_tree_m::wakeup_cleaners() {
    return _cleaner->wakeup_cleaners();
}
w_rc_t bf_tree_m::wakeup_cleaner_for_volume(volid_t vol) {
    return _cleaner->wakeup_cleaner_for_volume(vol);
}

void bf_tree_m::repair_rec_lsn (generic_page *page, bool was_dirty, const lsn_t &new_rlsn) {
    if( !smlevel_0::logging_enabled) return;

    bf_idx idx = page - _buffer;
    w_assert1 (_is_active_idx(idx));
    w_assert1(false == get_cb(idx)._in_doubt);

    lsn_t lsn = _buffer[idx].lsn;
    lsn_t rec_lsn (get_cb(idx)._rec_lsn);
    if (was_dirty) {
        // never mind!
        w_assert0(rec_lsn <= lsn);
    } else {
        w_assert0(rec_lsn > lsn);
        if(new_rlsn.valid()) {
            w_assert0(new_rlsn <= lsn);
            w_assert2(get_cb(idx)._dirty);
            get_cb(idx)._rec_lsn = new_rlsn.data();
            INC_TSTAT(restart_repair_rec_lsn);
        } else {
            get_cb(idx)._dirty = false;
            get_cb(idx)._in_doubt = false;
            get_cb(idx)._recovery_access = false;
        }
    }
}

///////////////////////////////////   Dirty Page Cleaner END       ///////////////////////////////////


///////////////////////////////////   LRU/Freelist BEGIN ///////////////////////////////////

#ifdef BP_MAINTAIN_PARENT_PTR
void bf_tree_m::_add_to_swizzled_lru(bf_idx idx) {
    w_assert1 (is_swizzling_enabled());
    w_assert1 (_is_active_idx(idx));
    w_assert1 (!get_cb(idx)._swizzled);
    CRITICAL_SECTION(cs, &_swizzled_lru_lock);
    ++_swizzled_lru_len;
    if (SWIZZLED_LRU_HEAD == 0) {
        // currently the LRU is empty
        w_assert1(SWIZZLED_LRU_TAIL == 0);
        SWIZZLED_LRU_HEAD = idx;
        SWIZZLED_LRU_TAIL = idx;
        SWIZZLED_LRU_PREV(idx) = 0;
        SWIZZLED_LRU_NEXT(idx) = 0;
        return;
    }
    w_assert1(SWIZZLED_LRU_TAIL != 0);
    // connect to the current head
    SWIZZLED_LRU_PREV(idx) = 0;
    SWIZZLED_LRU_NEXT(idx) = SWIZZLED_LRU_HEAD;
    SWIZZLED_LRU_PREV(SWIZZLED_LRU_HEAD) = idx;
    SWIZZLED_LRU_HEAD = idx;
}

void bf_tree_m::_update_swizzled_lru(bf_idx idx) {
    w_assert1 (is_swizzling_enabled());
    w_assert1 (_is_active_idx(idx));
    w_assert1 (get_cb(idx)._swizzled);

    CRITICAL_SECTION(cs, &_swizzled_lru_lock);
    w_assert1(SWIZZLED_LRU_HEAD != 0);
    w_assert1(SWIZZLED_LRU_TAIL != 0);
    w_assert1(_swizzled_lru_len > 0);
    if (SWIZZLED_LRU_HEAD == idx) {
        return; // already the head
    }
    if (SWIZZLED_LRU_TAIL == idx) {
        bf_idx new_tail = SWIZZLED_LRU_PREV(idx);
        SWIZZLED_LRU_NEXT(new_tail) = 0;
        SWIZZLED_LRU_TAIL = new_tail;
    } else {
        bf_idx old_prev = SWIZZLED_LRU_PREV(idx);
        bf_idx old_next = SWIZZLED_LRU_NEXT(idx);
        w_assert1(old_prev != 0);
        w_assert1(old_next != 0);
        SWIZZLED_LRU_NEXT (old_prev) = old_next;
        SWIZZLED_LRU_PREV (old_next) = old_prev;
    }
    bf_idx old_head = SWIZZLED_LRU_HEAD;
    SWIZZLED_LRU_PREV(idx) = 0;
    SWIZZLED_LRU_NEXT(idx) = old_head;
    SWIZZLED_LRU_PREV(old_head) = idx;
    SWIZZLED_LRU_HEAD = idx;
}

void bf_tree_m::_remove_from_swizzled_lru(bf_idx idx) {
    w_assert1 (is_swizzling_enabled());
    w_assert1 (_is_active_idx(idx));
    w_assert1 (get_cb(idx)._swizzled);

    get_cb(idx)._swizzled = false;
    CRITICAL_SECTION(cs, &_swizzled_lru_lock);
    w_assert1(SWIZZLED_LRU_HEAD != 0);
    w_assert1(SWIZZLED_LRU_TAIL != 0);
    w_assert1(_swizzled_lru_len > 0);
    --_swizzled_lru_len;

    bf_idx old_prev = SWIZZLED_LRU_PREV(idx);
    bf_idx old_next = SWIZZLED_LRU_NEXT(idx);
    SWIZZLED_LRU_PREV(idx) = 0;
    SWIZZLED_LRU_NEXT(idx) = 0;
    if (SWIZZLED_LRU_HEAD == idx) {
        w_assert1(old_prev == 0);
        if (old_next == 0) {
            w_assert1(_swizzled_lru_len == 0);
            SWIZZLED_LRU_HEAD = 0;
            SWIZZLED_LRU_TAIL = 0;
            return;
        }
        SWIZZLED_LRU_HEAD = old_next;
        SWIZZLED_LRU_PREV(old_next) = 0;
        return;
    }

    w_assert1(old_prev != 0);
    if (SWIZZLED_LRU_TAIL == idx) {
        w_assert1(old_next == 0);
        SWIZZLED_LRU_NEXT(old_prev) = 0;
        SWIZZLED_LRU_TAIL = old_prev;
    } else {
        w_assert1(old_next != 0);
        SWIZZLED_LRU_NEXT (old_prev) = old_next;
        SWIZZLED_LRU_PREV (old_next) = old_prev;
    }
}
#endif // BP_MAINTAIN_PARENT_PTR

w_rc_t bf_tree_m::_grab_free_block(bf_idx& ret, bool evict) {
#ifdef SIMULATE_MAINMEMORYDB
    if (true) {
        ERROUT (<<"MAINMEMORY-DB. _grab_free_block() shouldn't be called. wtf");
        return RC(eINTERNAL);
    }
#endif // SIMULATE_MAINMEMORYDB
    ret = 0;
    while (true) {
        // once the bufferpool becomes full, getting _freelist_lock everytime will be
        // too costly. so, we check _freelist_len without lock first.
        //   false positive : fine. we do real check with locks in it
        //   false negative : fine. we will eventually get some free block anyways.
        if (_freelist_len > 0) {
            CRITICAL_SECTION(cs, &_freelist_lock);
            if (_freelist_len > 0) { // here, we do the real check
                bf_idx idx = FREELIST_HEAD;
                w_assert1(_is_valid_idx(idx));
                w_assert1 (!get_cb(idx)._used);
                --_freelist_len;
                if (_freelist_len == 0) {
                    FREELIST_HEAD = 0;
                } else {
                    FREELIST_HEAD = _freelist[idx];
                    w_assert1 (FREELIST_HEAD > 0 && FREELIST_HEAD < _block_cnt);
                }
                ret = idx;
                return RCOK;
            }
        } // exit the scope to do the following out of the critical section

        // if the freelist was empty, let's evict some page.
        if (true == evict)
        {
            W_DO (_get_replacement_block());
        }
        else
        {
            // Freelist is empty and caller does not want to evict pages (Recovery M1)
            return RC(eBFFULL);
        }
    }
    return RCOK;
}

w_rc_t bf_tree_m::_get_replacement_block() {
#ifdef SIMULATE_MAINMEMORYDB
    if (true) {
        ERROUT (<<"MAINMEMORY-DB. _get_replacement_block() shouldn't be called. wtf");
        return RC(eINTERNAL);
    }
#endif // SIMULATE_MAINMEMORYDB
    uint32_t evicted_count, unswizzled_count;
    // evict with gradually higher urgency
    for (int urgency = EVICT_NORMAL; urgency <= EVICT_COMPLETE; ++urgency) {
        W_DO(evict_blocks(evicted_count, unswizzled_count, (evict_urgency_t) urgency));
        if (evicted_count > 0 || _freelist_len > 0) {
            return RCOK;
        }
        W_DO(wakeup_cleaners());
        g_me()->sleep(100);
        DBGOUT1(<<"woke up. now there should be some page to evict. urgency=" << urgency);
#if W_DEBUG_LEVEL>=1
        debug_dump(std::cout);
#endif // W_DEBUG_LEVEL>=1
    }

    ERROUT(<<"whoa, couldn't find an evictable page for long time. gave up!");
    debug_dump(std::cerr);
    W_DO(evict_blocks(evicted_count, unswizzled_count, EVICT_COMPLETE));
    if (evicted_count > 0 || _freelist_len > 0) {
        return RCOK;
    }
    return RC(eFRAMENOTFOUND);
}

bool bf_tree_m::_try_evict_block(bf_idx parent_idx, bf_idx idx) {
    bf_tree_cb_t &parent_cb = get_cb(parent_idx);
    bf_tree_cb_t &cb = get_cb(idx);

    // do not consider dirty pages (at this point)
    // we check this again later because we don't take locks as of this.
    // we also avoid grabbing unused block because it has to be grabbed via freelist
    if (cb._dirty || !cb._used || !parent_cb._used || cb._in_doubt) {
        return -1;
    }

    // find a block that has no pinning (or not being evicted by others).
    // this check is approximate as it's without lock.
    // false positives are fine, and we do the real check later
    if (cb.pin_cnt() != 0) {
        return false;
    }

    // if it seems someone latches it, give up.
    if (cb.latch().latch_cnt() != 0) { // again, we check for real later
        return false;
    }

    // okay, let's try evicting this page.
    // first, we have to make sure the page's pin_cnt is exactly 0.
    // we atomically change it to -1.
    int zero = 0;
    if (lintel::unsafe::atomic_compare_exchange_strong(const_cast<int32_t*>(&cb._pin_cnt),
        (int32_t*) &zero , (int32_t) -1)) {
        // CAS did it job. the current thread has an exclusive access to this block
        bool evicted = _try_evict_block_pinned(parent_cb, cb, parent_idx, idx);
        if (!evicted) {
            cb.pin_cnt_set(0);
        }
        return evicted;
    }
    // it can happen. we just give up this block
    return false;
}
bool bf_tree_m::_try_evict_block_pinned(
    bf_tree_cb_t &parent_cb, bf_tree_cb_t &cb,
    bf_idx parent_idx, bf_idx idx) {
    w_assert1(cb.pin_cnt() == -1);

    // let's do a real check.
    if (cb._dirty || !cb._used || cb._in_doubt) {
        DBGOUT1(<<"very unlucky, this block has just become dirty.");
        // oops, then put this back and give up this block
        return false;
    }

    w_assert1(_buffer[parent_idx].tag == t_btree_p);
    generic_page *parent = &_buffer[parent_idx];
    general_recordid_t child_slotid = find_page_id_slot(parent, cb._pid_shpid);
    if (child_slotid == GeneralRecordIds::INVALID) {
        // because the searches are approximate, this can happen
        DBGOUT1(<< "Unlucky, the parent/child pair is no longer valid");
        return false;
    }
    // check latches too. just conditionally test it to avoid long waits.
    // We take 2 latches here; parent and child *in this order* to avoid deadlocks.

    // Parent page can be just an SH latch because we are updating EMLSN only
    // which no one else should be updating
    bool parent_latched = false;
    if (!parent_cb.latch().held_by_me()) {
        w_rc_t latch_parent_rc = parent_cb.latch().latch_acquire(LATCH_SH, WAIT_IMMEDIATE);
        if (latch_parent_rc.is_error()) {
            DBGOUT1(<<"Unlucky, failed to latch parent block while evicting. skipping block");
            return false;
        }
        parent_latched = true;
    }

    bool updated = _try_evict_block_update_emlsn(parent_cb, cb, parent_idx, idx, child_slotid);
    // as soon as we are done with parent, unlatch it
    if (parent_latched) {
        parent_cb.latch().latch_release();
    }
    if (!updated) {
        return false;
    }

    // remove it from hashtable.
    bool removed = _hashtable->remove(bf_key(cb._pid_vol, cb._pid_shpid));
    w_assert1(removed);
#ifdef BP_MAINTAIN_PARENT_PTR
    w_assert1(!_is_in_swizzled_lru(idx));
    if (is_swizzling_enabled()) {
        w_assert1(cb._parent != 0);
        _decrement_pin_cnt_assume_positive(cb._parent);
    }
#endif // BP_MAINTAIN_PARENT_PTR
    cb.clear();
    return true; // success
}
bool bf_tree_m::_try_evict_block_update_emlsn(
    bf_tree_cb_t &parent_cb, bf_tree_cb_t &cb,
    bf_idx parent_idx, bf_idx idx, general_recordid_t child_slotid) {
    w_assert1(cb.pin_cnt() == -1);
    w_assert1(parent_cb.latch().is_latched());

    w_rc_t latch_rc = cb.latch().latch_acquire(LATCH_EX, WAIT_IMMEDIATE);
    if (latch_rc.is_error()) {
        DBGOUT1(<<"very unlucky, someone has just latched this block.");
        return false;
    }
    // we can immediately release EX latch because no one will newly take latch as _pin_cnt==-1
    cb.latch().latch_release();
    DBGOUT1(<<"evicting page idx = " << idx << " shpid = " << cb._pid_shpid
            << " pincnt = " << cb.pin_cnt());

    // Output Single-Page-Recovery log for updating EMLSN in parent.
    // safe to disguise as LATCH_EX for the reason above.
    btree_page_h parent_h;
    generic_page *parent = &_buffer[parent_idx];
    parent_h.fix_nonbufferpool_page(parent);
    lsn_t old = parent_h.get_emlsn_general(child_slotid);
    if (old < _buffer[idx].lsn) {
        DBGOUT1(<< "Updated EMLSN on page " << parent_h.pid() << " slot=" << child_slotid
                << " (child pid=" << cb._pid_shpid << ")"
                << ", OldEMLSN=" << old << " NewEMLSN=" << _buffer[idx].lsn);
        W_COERCE(_sx_update_child_emlsn(parent_h, child_slotid, _buffer[idx].lsn));
        // Note that we are not grabbing EX latch on parent here.
        // This is safe because no one else should be touching these exact bytes,
        // and because EMLSN values are aligned to be "regular register".
        set_dirty(parent); // because fix_nonbufferpool_page() would skip set_dirty().
        w_assert1(parent_h.get_emlsn_general(child_slotid) == _buffer[idx].lsn);
    }
    return true;
}

void bf_tree_m::_add_free_block(bf_idx idx) {
    CRITICAL_SECTION(cs, &_freelist_lock);
    ++_freelist_len;
    _freelist[idx] = FREELIST_HEAD;
    FREELIST_HEAD = idx;
#ifdef BP_MAINTAIN_PARENT_PTR
    // if the following fails, you might have forgot to remove it from the LRU before calling this method
    w_assert1(SWIZZLED_LRU_NEXT(idx) == 0);
    w_assert1(SWIZZLED_LRU_PREV(idx) == 0);
#endif // BP_MAINTAIN_PARENT_PTR
}

void bf_tree_m::_delete_block(bf_idx idx) {
    w_assert1(_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb._dirty);
    w_assert1(cb.pin_cnt() == 0);
    w_assert1(!cb.latch().is_latched());
    cb._used = false; // clear _used BEFORE _dirty so that eviction thread will ignore this block.
    cb._dirty = false;
    cb._in_doubt = false; // always set in_doubt bit to false
    cb._uncommitted_cnt = 0;

    DBGOUT1(<<"delete block: remove page shpid = " << cb._pid_shpid);
    bool removed = _hashtable->remove(bf_key(cb._pid_vol, cb._pid_shpid));
    w_assert1(removed);
#ifdef BP_MAINTAIN_PARENT_PTR
    w_assert1(!_is_in_swizzled_lru(idx));
    if (is_swizzling_enabled()) {
        _decrement_pin_cnt_assume_positive(cb._parent);
    }
#endif // BP_MAINTAIN_PARENT_PTR

    // after all, give back this block to the freelist. other threads can see this block from now on
    _add_free_block(idx);
}

///////////////////////////////////   LRU/Freelist END ///////////////////////////////////

bool bf_tree_m::_increment_pin_cnt_no_assumption(bf_idx idx) {
    w_assert1 (_is_valid_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
#if 0
    int32_t cur = cb._pin_cnt;
    while (true) {
        w_assert1(cur >= -1);
        if (cur == -1) {
            break; // being evicted! fail
        }

        if(lintel::unsafe::atomic_compare_exchange_strong(const_cast<int32_t*>(&cb._pin_cnt), &cur , cur + 1)) {
            return true; // increment occurred
        }

        // if we get here it's because another thread raced in here,
        // and updated the pin count before we could.
    }
    return false;
#endif
    return cb.pin_cnt_atomic_inc_no_assumption(1);
}

void bf_tree_m::_decrement_pin_cnt_assume_positive(bf_idx idx) {
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1 (cb.pin_cnt() >= 1);
    //lintel::unsafe::atomic_fetch_sub((uint32_t*) &(cb._pin_cnt),1);
    cb.pin_cnt_atomic_dec(1);
}

///////////////////////////////////   WRITE-ORDER-DEPENDENCY BEGIN ///////////////////////////////////
bool bf_tree_m::register_write_order_dependency(const generic_page* page, const generic_page* dependency) {
    w_assert1(page);
    w_assert1(dependency);
    w_assert1(page->pid != dependency->pid);

    uint32_t idx = page - _buffer;
    w_assert1 (_is_active_idx(idx));
    w_assert1(false == get_cb(idx)._in_doubt);
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb.latch().held_by_me());

    uint32_t dependency_idx = dependency - _buffer;
    w_assert1 (_is_active_idx(dependency_idx));
    bf_tree_cb_t &dependency_cb = get_cb(dependency_idx);
    w_assert1(dependency_cb.latch().held_by_me());

    // each page can have only one out-going dependency
    if (cb._dependency_idx != 0) {
        w_assert1 (cb._dependency_shpid != 0); // the OLD dependency pid
        if (cb._dependency_idx == dependency_idx) {
            // okay, it points to the same block

            if (cb._dependency_shpid == dependency_cb._pid_shpid) {
                // fine. it's just update of minimal lsn with max of the two.
                cb._dependency_lsn = dependency_cb._rec_lsn > cb._dependency_lsn ? dependency_cb._rec_lsn : cb._dependency_lsn;
                return true;
            } else {
                // this means now the old dependency is already evicted. so, we can forget about it.
                cb._dependency_idx = 0;
                cb._dependency_shpid = 0;
                cb._dependency_lsn = 0;
            }
        } else {
            // this means we might be requesting more than one dependency...
            // let's check the old dependency is still active
            if  (_check_dependency_still_active(cb)) {
                // the old dependency is still active. we can't make another dependency
                DBGOUT3(<< "WOD failed on " << page->pid << "->" << dependency->pid
                        << " because dependency still active on CB");
                return false;
            }
        }
    }

    // this is the first dependency
    w_assert1(cb._dependency_idx == 0);
    w_assert1(cb._dependency_shpid == 0);
    w_assert1(cb._dependency_lsn == 0);

    // check a cycle of dependency
    if (dependency_cb._dependency_idx != 0) {
        if (_check_dependency_cycle (idx, dependency_idx)) {
            return false;
        }
    }

    //okay, let's register the dependency
    cb._dependency_idx = dependency_idx;
    cb._dependency_shpid = dependency_cb._pid_shpid;
    cb._dependency_lsn = dependency_cb._rec_lsn;
    DBGOUT3(<< "WOD registered: " << page->pid << "->" << dependency->pid);

    return true;
}

bool bf_tree_m::_check_dependency_cycle(bf_idx source, bf_idx start_idx) {
    w_assert1(source != start_idx);
    bf_idx dependency_idx = start_idx;
    bool dependency_needs_unpin = false;
    bool found_cycle = false;
    while (true) {
        if (dependency_idx == source) {
            found_cycle = true;
            break;
        }
        bf_tree_cb_t &dependency_cb = get_cb(dependency_idx);
        w_assert1(dependency_cb.pin_cnt() >= 0);
        bf_idx next_dependency_idx = dependency_cb._dependency_idx;
        if (next_dependency_idx == 0) {
            break;
        }
        bool increased = _increment_pin_cnt_no_assumption (next_dependency_idx);
        if (!increased) {
            // it's already evicted or being evicted. we can ignore it.
            break; // we can stop here.
        } else {
            // move on to next
            bf_tree_cb_t &next_dependency_cb = get_cb(next_dependency_idx);
            bool still_active = _compare_dependency_lsn(dependency_cb, next_dependency_cb);
            // okay, we no longer need the previous. unpin the previous one.
            if (dependency_needs_unpin) {
                _decrement_pin_cnt_assume_positive(dependency_idx);
            }
            dependency_idx = next_dependency_idx;
            dependency_needs_unpin = true;
            if (!still_active) {
                break;
            }
        }
    }
    if (dependency_needs_unpin) {
        _decrement_pin_cnt_assume_positive(dependency_idx);
    }
    return found_cycle;
}

bool bf_tree_m::_compare_dependency_lsn(const bf_tree_cb_t& cb, const bf_tree_cb_t &dependency_cb) const {
    w_assert1(cb.pin_cnt() >= 0);
    w_assert1(cb._dependency_idx != 0);
    w_assert1(cb._dependency_shpid != 0);
    w_assert1(dependency_cb.pin_cnt() >= 0);
    return dependency_cb._used && dependency_cb._dirty // it's still dirty
        && dependency_cb._pid_vol == cb._pid_vol // it's still the vol and
        && dependency_cb._pid_shpid == cb._dependency_shpid // page it was referring..
        && dependency_cb._rec_lsn <= cb._dependency_lsn; // and not flushed after the registration
}
bool bf_tree_m::_check_dependency_still_active(bf_tree_cb_t& cb) {
    w_assert1(cb.pin_cnt() >= 0);
    bf_idx next_idx = cb._dependency_idx;
    if (next_idx == 0) {
        return false;
    }

    w_assert1(cb._dependency_shpid != 0);

    bool still_active;
    {
        bool increased = _increment_pin_cnt_no_assumption (next_idx);
        if (!increased) {
            // it's already evicted or being evicted. we can ignore it.
            still_active = false;
        } else {
            still_active = _compare_dependency_lsn(cb, get_cb(next_idx));
            _decrement_pin_cnt_assume_positive(next_idx);
        }
    }

    if (!still_active) {
        // reset the values to help future inquiry
        cb._dependency_idx = 0;
        cb._dependency_shpid = 0;
        cb._dependency_lsn = 0;
    }
    return still_active;
}
///////////////////////////////////   WRITE-ORDER-DEPENDENCY END ///////////////////////////////////

#ifdef BP_MAINTAIN_PARENT_PTR
void bf_tree_m::switch_parent(generic_page* page, generic_page* new_parent)
{
    if (!is_swizzling_enabled()) {
        return;
    }
    w_assert1(false == get_cb(idx)._in_doubt);
    bf_idx idx = page - _buffer;
    w_assert1(_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);

    w_assert1(_is_active_idx(cb._parent));

    bf_idx new_parent_idx = new_parent - _buffer;
    w_assert1(_is_active_idx(new_parent_idx));
    w_assert1(cb._parent != new_parent_idx);

    // move the pin_cnt from old to new parent
    _decrement_pin_cnt_assume_positive(cb._parent);
    lintel::unsafe::atomic_fetch_add((uint32_t*) &(get_cb(new_parent_idx)._pin_cnt),1);
    cb._parent = new_parent_idx;
}
#endif // BP_MAINTAIN_PARENT_PTR


void bf_tree_m::_convert_to_disk_page(generic_page* page) const {
    DBGOUT3 (<< "converting the page " << page->pid << "... ");

    fixable_page_h p;
    p.fix_nonbufferpool_page(page);
    int max_slot = p.max_child_slot();
    for (int i= -1; i<=max_slot; i++) {
        _convert_to_pageid(p.child_slot_address(i));
    }
}

inline void bf_tree_m::_convert_to_pageid (shpid_t* shpid) const {
    if ((*shpid) & SWIZZLED_PID_BIT) {
        bf_idx idx = (*shpid) ^ SWIZZLED_PID_BIT;
        w_assert1(_is_active_idx(idx));
        bf_tree_cb_t &cb = get_cb(idx);
        DBGOUT3 (<< "_convert_to_pageid(): converted a swizzled pointer bf_idx=" << idx << " to page-id=" << cb._pid_shpid);
        *shpid = cb._pid_shpid;
    }
}

general_recordid_t bf_tree_m::find_page_id_slot(generic_page* page, shpid_t shpid) const {
    w_assert1((shpid & SWIZZLED_PID_BIT) == 0);

    fixable_page_h p;
    p.fix_nonbufferpool_page(page);
    int max_slot = p.max_child_slot();

    //for (int i = -1; i <= max_slot; ++i) {
    for (general_recordid_t i = GeneralRecordIds::FOSTER_CHILD; i <= max_slot; ++i) {
        if (*p.child_slot_address(i) != shpid) {
            continue;
        }
        return i;
    }
    return GeneralRecordIds::INVALID;
}

///////////////////////////////////   SWIZZLE/UNSWIZZLE BEGIN ///////////////////////////////////

void bf_tree_m::swizzle_child(generic_page* parent, general_recordid_t slot)
{
    return swizzle_children(parent, &slot, 1);
}

void bf_tree_m::swizzle_children(generic_page* parent, const general_recordid_t* slots,
                                 uint32_t slots_size) {
    w_assert1(is_swizzling_enabled());
    w_assert1(parent != NULL);
    w_assert1(latch_mode(parent) != LATCH_NL);
    w_assert1(_is_active_idx(parent - _buffer));
    w_assert1(is_swizzled(parent)); // swizzling is transitive.

    fixable_page_h p;
    p.fix_nonbufferpool_page(parent);
    for (uint32_t i = 0; i < slots_size; ++i) {
        general_recordid_t slot = slots[i];
        // To simplify the tree traversal while unswizzling,
        // we never swizzle foster-child pointers.
        w_assert1(slot >= GeneralRecordIds::PID0); // was w_assert1(slot >= -1);
        w_assert1(slot <= p.max_child_slot());

        shpid_t* addr = p.child_slot_address(slot);
        if (((*addr) & SWIZZLED_PID_BIT) == 0) {
            _swizzle_child_pointer(parent, addr);
        }
    }
}

inline void bf_tree_m::_swizzle_child_pointer(generic_page* parent, shpid_t* pointer_addr) {
    shpid_t child_shpid = *pointer_addr;
    //w_assert1((child_shpid & SWIZZLED_PID_BIT) == 0);
    uint64_t key = bf_key (parent->pid.vol().vol, child_shpid);
    bf_idx idx = _hashtable->lookup(key);
    // so far, we don't swizzle a child page if it's not in bufferpool yet.
    if (idx == 0) {
        DBGOUT1(<< "Unexpected! the child page " << child_shpid << " isn't in bufferpool yet. gave up swizzling it");
        // this is still okay. swizzling is best-effort
        return;
    }
    bool concurrent_swizzling = false;
    while (!lintel::unsafe::atomic_compare_exchange_strong(const_cast<bool*>(&(get_cb(idx)._concurrent_swizzling)), &concurrent_swizzling, true))
    { }

    if ((child_shpid & SWIZZLED_PID_BIT) != 0) {
        /* another thread swizzled it for us */
        get_cb(idx)._concurrent_swizzling = false;
        return;
    }

    // To swizzle the child, add a pin on the page.
    // We might fail here in a very unlucky case.  Still, it's fine.
    bool pinned = _increment_pin_cnt_no_assumption (idx);
    if (!pinned) {
        DBGOUT1(<< "Unlucky! the child page " << child_shpid << " has been just evicted. gave up swizzling it");
        get_cb(idx)._concurrent_swizzling = false;
        return;
    }

    // we keep the pin until we unswizzle it.
    *pointer_addr = idx | SWIZZLED_PID_BIT; // overwrite the pointer in parent page.
    get_cb(idx)._swizzled = true;
#ifdef BP_TRACK_SWIZZLED_PTR_CNT
    get_cb(parent)->_swizzled_ptr_cnt_hint++;
#endif
    ++_swizzled_page_count_approximate;
#ifdef BP_MAINTAIN_PARENT_PTR
    w_assert1(!_is_in_swizzled_lru(idx));
    _add_to_swizzled_lru(idx);
    w_assert1(_is_in_swizzled_lru(idx));
#endif // BP_MAINTAIN_PARENT_PTR
    get_cb(idx)._concurrent_swizzling = false;
}

inline bool bf_tree_m::_are_there_many_swizzled_pages() const {
    return _swizzled_page_count_approximate >= (int) (_block_cnt * 2 / 10);
}

void bf_tree_m::_dump_evict_clockhand(const EvictionContext &context) const {
    DBGOUT1(<< "current clockhand depth=" << context.clockhand_current_depth
        << ". _swizzled_page_count_approximate=" << _swizzled_page_count_approximate << " / "
        << _block_cnt << "\n so far evicted " << context.evicted_count << ", "
        <<  "unswizzled " << context.unswizzled_count << " frames. rounds=" << context.rounds);
    for (int i = 0; i < context.clockhand_current_depth; ++i) {
        DBGOUT1(<< "current clockhand pathway[" << i << "]:" << context.clockhand_pathway[i]);
    }
}

void bf_tree_m::_lookup_buf_imprecise(btree_page_h &parent, uint32_t slot,
                                      bf_idx& idx, bool& swizzled) const {
    volid_t vol = parent.vol();
    shpid_t shpid = *parent.child_slot_address(slot);
    if ((shpid & SWIZZLED_PID_BIT) == 0) {
        idx = _hashtable->lookup_imprecise(bf_key(vol, shpid));
        swizzled = false;
        if (idx == 0) {
#if W_DEBUG_LEVEL>=3
            for (bf_idx i = 1; i < _block_cnt; ++i) {
                bf_tree_cb_t &cb = get_cb(i);
                if (cb._used && cb._pid_shpid == shpid && cb._pid_vol == vol) {
                    ERROUT(<<"Dubious cache miss. Is it really because of concurrent updates?"
                        "vol=" << vol << ", shpid=" << shpid << ", idx=" << i);
                }
            }
#endif // W_DEBUG_LEVEL>=3
            return;
        }
        bf_tree_cb_t &cb = get_cb(idx);
        if (!_is_active_idx(idx) || cb._pid_shpid != shpid || cb._pid_vol != vol) {
            ERROUT(<<"Dubious false negative. Is it really because of concurrent updates?"
                "vol=" << vol << ", shpid=" << shpid << ", idx=" << idx);
            idx = 0;
        }
    } else {
        idx = shpid ^ SWIZZLED_PID_BIT;
        swizzled = true;
    }
}

w_rc_t bf_tree_m::evict_blocks(uint32_t& evicted_count, uint32_t& unswizzled_count,
                               bf_tree_m::evict_urgency_t urgency, uint32_t preferred_count) {
    EvictionContext context;
    context.urgency = urgency;
    context.preferred_count = preferred_count;
    ::memset(context.bufidx_pathway, 0, sizeof(bf_idx) * MAX_CLOCKHAND_DEPTH);
    ::memset(context.swizzled_pathway, 0, sizeof(bool) * MAX_CLOCKHAND_DEPTH);
    // copy-from the shared clockhand_pathway with latch
    {
        CRITICAL_SECTION(cs, &_clockhand_copy_lock);
        context.clockhand_current_depth = _clockhand_current_depth;
        ::memcpy(context.clockhand_pathway, _clockhand_pathway,
                 sizeof(uint32_t) * MAX_CLOCKHAND_DEPTH);
    }

    W_DO(_evict_blocks(context));

    // copy-back to the shared clockhand_pathway with latch
    {
        CRITICAL_SECTION(cs, &_clockhand_copy_lock);
        _clockhand_current_depth = context.clockhand_current_depth;
        ::memcpy(_clockhand_pathway, context.clockhand_pathway,
                 sizeof(uint32_t) * MAX_CLOCKHAND_DEPTH);
    }
    evicted_count = context.evicted_count;
    unswizzled_count = context.unswizzled_count;
    return RCOK;
}
w_rc_t bf_tree_m::_evict_blocks(EvictionContext& context) {
    w_assert1(context.traverse_depth == 0);
    while (context.rounds < EVICT_MAX_ROUNDS) {
        _dump_evict_clockhand(context);
        uint32_t old = context.clockhand_pathway[0] % MAX_VOL_COUNT;
        for (uint16_t i = 0; i < MAX_VOL_COUNT; ++i) {
            volid_t vol = (old + i) % MAX_VOL_COUNT;
            if (_volumes[vol] == NULL) {
                continue;
            }
            context.traverse_depth = 1;
            if (i != 0 || context.clockhand_current_depth == 0) {
                // this means now we are moving on to another volume.
                context.clockhand_current_depth = context.traverse_depth; // reset descendants
                context.clockhand_pathway[context.traverse_depth - 1] = vol;
            }
            W_DO(_evict_traverse_volume(context));
            if (context.is_enough()) {
                return RCOK;
            }
        }
        // checked everything. next round.
        context.clockhand_current_depth = 0;
        ++context.rounds;
        if (context.urgency <= EVICT_NORMAL) {
            break;
        }
        // most likely we have too many dirty pages
        // let's try writing out dirty pages
        W_DO(wakeup_cleaners());
    }

    _dump_evict_clockhand(context);
    return RCOK;
}

w_rc_t bf_tree_m::_evict_traverse_volume(EvictionContext &context) {
    uint32_t depth = context.traverse_depth;
    w_assert1(depth == 1);
    w_assert1(context.clockhand_current_depth >= depth);
    w_assert1(context.clockhand_pathway[depth - 1] != 0);
    volid_t vol = context.get_vol();
    uint32_t old = context.clockhand_pathway[depth];
    for (uint32_t i = 0; i < MAX_STORE_COUNT; ++i) {
        snum_t store = (old + i) % MAX_STORE_COUNT;
        if (_volumes[vol] == NULL) {
            // just give up in unlucky case (probably the volume has been just uninstalled)
            return RCOK;
        }
        bf_idx root_idx = _volumes[vol]->_root_pages[store];
        if (root_idx == 0) {
            continue;
        }

        context.traverse_depth = depth + 1;
        if (i != 0 || context.clockhand_current_depth == depth) {
            // this means now we are moving on to another store.
            context.clockhand_current_depth = depth + 1; // reset descendants
            context.clockhand_pathway[depth] = store;
            context.bufidx_pathway[depth] = root_idx;
        }
        W_DO(_evict_traverse_store(context));
        if (context.is_enough()) {
            return RCOK;
        }
    }
    // exhaustively checked this volume. this volume is 'done'
    context.clockhand_current_depth = depth;
    return RCOK;
}

w_rc_t bf_tree_m::_evict_traverse_store(EvictionContext &context) {
    uint32_t depth = context.traverse_depth;
    w_assert1(depth == 2);
    w_assert1(context.clockhand_current_depth >= depth);
    w_assert1(context.clockhand_pathway[depth - 1] != 0);
    w_assert1(context.bufidx_pathway[depth - 1] != 0);
    bf_idx root_idx = context.bufidx_pathway[depth - 1];
    btree_page_h root_p;
    root_p.fix_nonbufferpool_page(&_buffer[root_idx]);
    uint32_t child_count = (uint32_t) root_p.nrecs() + 1;
    if (child_count == 0) {
        return RCOK;
    }

    // root is never evicted/unswizzled, so it's simpler.
    uint32_t old = context.clockhand_pathway[depth];
    for (uint32_t i = 0; i < child_count; ++i) {
        uint32_t slot = (old + i) % child_count;
        context.traverse_depth = depth + 1;
        if (i != 0 || context.clockhand_current_depth == depth) {
            // this means now we are moving on to another child.
            bool swizzled;
            bf_idx idx;
            _lookup_buf_imprecise(root_p, slot, idx, swizzled);
            if (idx == 0 || idx >= _block_cnt) {
                continue;
            }

            context.clockhand_current_depth = depth + 1; // reset descendants
            context.clockhand_pathway[depth] = slot;
            context.bufidx_pathway[depth] = idx;
            context.swizzled_pathway[depth] = swizzled;
        }

        W_DO(_evict_traverse_page (context));
        if (context.is_enough()) {
            return RCOK;
        }
    }

    // exhaustively checked this store. this store is 'done'
    context.clockhand_current_depth = depth;
    return RCOK;
}


bool bf_tree_m::has_swizzled_child(bf_idx node_idx) {
    fixable_page_h node_p;
    node_p.fix_nonbufferpool_page(_buffer + node_idx);
    int max_slot = node_p.max_child_slot();
    // skipping foster pointer...
    for (int32_t j = 0; j <= max_slot; ++j) {
        shpid_t shpid = *node_p.child_slot_address(j);
        if ((shpid & SWIZZLED_PID_BIT) != 0) {
            return true;
        }
    }
    return false;
}

w_rc_t bf_tree_m::_evict_traverse_page(EvictionContext &context) {
    const uint16_t depth = context.traverse_depth;
    w_assert1(depth >= 3);
    w_assert1(depth < MAX_CLOCKHAND_DEPTH);
    w_assert1(context.clockhand_current_depth >= depth);
    bf_idx idx = context.bufidx_pathway[depth - 1];
    bf_tree_cb_t &cb = get_cb(idx);
    if (!cb._used) {
        return RCOK;
    }

    btree_page_h p;
    p.fix_nonbufferpool_page(_buffer + idx);
    if (!p.is_leaf()) {
        uint32_t old = context.clockhand_pathway[depth];
        uint32_t child_count = (uint32_t) p.nrecs() + 1;
        if (child_count > 0) {
            // check children
            for (uint32_t i = 0; i < child_count; ++i) {
                uint32_t slot = (old + i) % child_count;
                bf_idx child_idx;
                context.traverse_depth = depth + 1;
                if (i != 0) {
                    bool swizzled;
                    _lookup_buf_imprecise(p, slot, child_idx, swizzled);
                    if (child_idx == 0) {
                        continue;
                    }

                    context.clockhand_current_depth = depth + 1;
                    context.clockhand_pathway[depth] = slot;
                    context.bufidx_pathway[depth] = child_idx;
                    context.swizzled_pathway[depth] = swizzled;
                }

                W_DO(_evict_traverse_page(context));
                if (context.is_enough()) {
                    return RCOK;
                }
            }
        }
    }

    // exhaustively checked this node's descendants. this node is 'done'
    context.clockhand_current_depth = depth;

    // consider evicting this page itself
    if (!cb._dirty) {
        W_DO(_evict_page(context, p));
    }
    return RCOK;
}

w_rc_t bf_tree_m::register_and_mark(bf_idx& ret,
                  lpid_t page_of_interest,
                  lsn_t first_lsn,             // Initial LSN, the first LSN which makes the page dirty
                  lsn_t last_lsn,              // Last LSN, last write to the page
                  uint32_t& in_doubt_count)
{
    w_rc_t rc = RCOK;
    w_assert1(page_of_interest.vol().vol != 0);
    w_assert1(page_of_interest.page != 0);
    volid_t vid = page_of_interest.vol().vol;
    shpid_t shpid = page_of_interest.page;

    ret = 0;

    // This function is only allowed in the Recovery Log Analysis phase

    // Note we are not holding latch for any of the operations in this function, because
    // Log Analysis phase is running in serial, it is not opened for new transactions so no race

    if (smlevel_0::t_in_analysis != smlevel_0::operating_mode)
        W_FATAL_MSG(fcINTERNAL,
            << "Can register a page in buffer pool only during Log Analysis phase, current phase: "
            << smlevel_0::operating_mode);

    // Do we have this page in buffer pool already?
    uint64_t key = bf_key(vid, shpid);
    bf_idx idx = lookup_in_doubt(key);
    if (0 != idx)
    {
        // If the page exists in buffer pool - it does not mean the in_doubt flag is on
        //   Increment the in_doubt page counter only if the in_doubt flag was off
        //   Make sure in_doubt and used flags are on, update _rec_lsn if
        //   new_lsn is smaller (earlier) than _rec_lsn in cb, _rec_lsn is the LSN
        //   which made the page 'dirty' initially
        // This is in Log Analysis phase, we have page in buffer pool but in_doubt flag off:
        // 1. Root page pre-loaded by device mounting
        // 2. Page was de-allocated but for some reason it is still in the hash table

        if (false == is_in_doubt(idx))
            ++in_doubt_count;
        // Set in_doubt flag, also update the initial (first dirty) and last write lsns
        set_in_doubt(idx, first_lsn, last_lsn);

        DBGOUT5(<<"Page is registered in buffer pool updated rec_lsn, idx: " << idx);
    }
    else
    {
        // If the page does not exist in buffer pool -
        //   Find a free block in buffer pool without evict, return error if the freelist is empty.
        //   Populate the page cb but not loading the actual page (do not load _buffer)
        //   set the in_doubt and used flags in cb to true, update the rec_lsn
        //   Insert into _hashtable so the page cb can be found later
        //   and return the index of the page

        DBGOUT5(<<"Page not registered in buffer pool, start registration process");
        rc = _grab_free_block(idx, false);  // Do not evict
        if (rc.is_error())
            return rc;

        // Now we have an empty page
        // Do not load the physical page from disk:
        //     volume->_volume->read_page(shpid, _buffer[idx]);
        // The actual page will be loaded in REDO phase

        // Initialize control block of the page, mark the in_doubt and used flags,
        // add the rec_lsn which indicates when the page was dirty initially
        bf_tree_cb_t &cb = get_cb(idx);
        cb._pid_vol = vid;
        cb._store_num = page_of_interest.store();
        cb._pid_shpid = shpid;
        cb._dirty = false;
        cb._in_doubt = true;
        cb._recovery_access = false;
        cb._used = true;
        cb._refbit_approximate = BP_INITIAL_REFCOUNT;
        cb._dependency_idx = 0;        // In_doubt page, no dependency
        cb._dependency_shpid = 0;      // In_doubt page, no dependency
        cb._uncommitted_cnt = 0;

        // For a page from disk,  get _rec_lsn from the physical page
        // (cb._rec_lsn = _buffer[idx].lsn.data())
        // But in Log Analysis, we don't load the page, so initialize _rec_lsn from first_lsn
        // which is the current log record LSN from log scan
        cb._rec_lsn = first_lsn.data();
        w_assert3(first_lsn.hi() > 0);

        // Store the 'last write LSN' in _dependency_lsn (overload this field because there is
        // no dependency for in_doubt page)
        cb._dependency_lsn = last_lsn.data();

        // Register the constructed 'key' into hash table so we can find this page cb later
        bool inserted = _hashtable->insert_if_not_exists(key, idx);
        if (false == inserted)
        {
            ERROUT(<<"failed to register a page as in_doubt in hashtable during Log Analysis phase");
            return RC(eINTERNAL);
        }

        // Update the in_doubt page counter
        ++in_doubt_count;

        // Now we are done
        DBGOUT5(<<"Done registration process, idx: " << idx);
    }

    // Return the idx of this page
    ret = idx;

    return rc;
}

w_rc_t bf_tree_m::load_for_redo(bf_idx idx, volid_t vid,
                  shpid_t shpid, bool& past_end)
{
    // Special function for Recovery REDO phase
    // idx is in hash table already
    // but the actual page has not loaded from disk into buffer pool yet

    // Caller of this fumction is responsible for acquire and release EX latch on this page

    w_rc_t rc = RCOK;
    w_assert1(vid != 0);
    w_assert1(shpid != 0);

    past_end = false;

    DBGOUT3(<<"REDO phase: loading page " << vid << "." << shpid
            << " into buffer pool frame " << idx);

    bf_tree_vol_t *volume = _volumes[vid];
    w_assert1(volume != NULL);
    w_assert1(shpid >= volume->_volume->first_data_pageid());

    // Load the physical page from disk
    rc = volume->_volume->read_page(shpid, _buffer[idx], past_end);
    if (true == past_end)
    {
        // During system recovery REDO phase when trying to load a page from disk,
        // the page does not exist on disk.
        // This can happen only if page driven REDO phase and an in_doubt page
        // was never flushed from buffer pool to disk before the system crash.
        // We cannot apply REDO on this page using the last write lsn of the page,
        // because the page in buffer pool has been zero out, notify caller through
        // return flag, while the return code (rc) is a good return code

        past_end = true;
        DBGOUT3(<<"REDO phase: page does not exist on disk: "
            << vid << "." << shpid);
        return rc;
    }
    if (rc.is_error())
    {
        DBGOUT3(<<"bf_tree_m: error while reading page " << shpid
                << " to frame " << idx << ". rc=" << rc);
        return rc;
    }
    else
    {
        // For the loaded page, compare its checksum
        // If inconsistent, return error
        uint32_t checksum = _buffer[idx].calculate_checksum();
        if (checksum != _buffer[idx].checksum)
        {
            ERROUT(<<"bf_tree_m: bad page checksum in page " << shpid
                    << " -- expected " << checksum
                    << " got " << _buffer[idx].checksum);
            return RC (eBADCHECKSUM);
        }

        // Then, page ID must match, otherwise raise error
        if (( shpid != _buffer[idx].pid.page) || (vid != _buffer[idx].pid.vol().vol))
        {
            W_FATAL_MSG(eINTERNAL, <<"inconsistent disk page: "
                << vid << "." << shpid << " was " << _buffer[idx].pid.vol().vol
                << "." << _buffer[idx].pid.page);
        }
    }

    return rc;
}


w_rc_t bf_tree_m::_evict_page(EvictionContext& context, btree_page_h& p) {
    bf_idx idx = context.bufidx_pathway[context.traverse_depth - 1];
    uint32_t slot = context.clockhand_pathway[context.traverse_depth - 1];
    bool swizzled = context.swizzled_pathway[context.traverse_depth - 1];
    bf_idx parent_idx = context.bufidx_pathway[context.traverse_depth - 2];

    bf_tree_cb_t &cb = get_cb(idx);
    DBGOUT1(<<"_evict_page: idx=" << idx << ", slot=" << slot << ", pid=" << p.pid()
        << ", swizzled=" << swizzled << ", hot-ness=" << cb._refbit_approximate
        << ", dirty=" << cb._dirty << " parent_idx=" << parent_idx);

    // do not evict hot pages, dirty pages, in_doubt pages
    if (!cb._used || cb._dirty || cb._in_doubt) {
        return RCOK;
    }
    if (context.urgency >= EVICT_URGENT) {
        // If in hurry, everyone is victim
        cb._refbit_approximate = 0;
    } else if (cb._refbit_approximate > 0) {
        // decrease refbit for exponential to how long time we are repeating this
        uint32_t decrease = (1 << context.rounds);
        if (decrease > cb._refbit_approximate) {
            cb._refbit_approximate = 0; // still victm
        } else {
            cb._refbit_approximate -= decrease;
            return RCOK; // enough hot
        }
    }

    // do we want to unswizzle this?
    if (context.is_unswizzling() && swizzled && cb._swizzled_ptr_cnt_hint == 0
        && p.is_leaf()) { // so far we don't consider unswizzling intermediate nodes.
        // this is just a hint. try conditionally latching the child and do the actual check
        w_rc_t latch_rc = cb.latch().latch_acquire(LATCH_SH, sthread_t::WAIT_IMMEDIATE);
        if (latch_rc.is_error()) {
            DBGOUT2(<<"unswizzle: oops, unlucky. someone is latching this page. skipping this."
                        " rc=" << latch_rc);
        } else {
            if (!has_swizzled_child(idx)) {
                // unswizzle_a_frame will try to conditionally latch a parent while
                // we hold a latch on a child. While this is latching in the reverse order,
                // it is still safe against deadlock as the operation is conditional.
                if (_unswizzle_a_frame (parent_idx, slot)) {
                    ++context.unswizzled_count;
                }
            }
            cb.latch().latch_release();
        }
    }

    // do we want to evict this page?
    if (_try_evict_block(parent_idx, idx)) {
        _add_free_block(idx);
        ++context.evicted_count;
    }
    return RCOK;
}

struct latch_auto_release {
    latch_auto_release (latch_t &latch) : _latch(latch) {}
    ~latch_auto_release () {
        _latch.latch_release();
    }
    latch_t &_latch;
};

bool bf_tree_m::_unswizzle_a_frame(bf_idx parent_idx, uint32_t child_slot) {
    // misc checks. if any check fails, just returns false.
    bf_tree_cb_t &parent_cb = get_cb(parent_idx);
    if (!parent_cb._used) {
        return false;
    }
    if (!parent_cb._swizzled) {
        return false;
    }
    // now, try a conditional latch on parent page.
    w_rc_t latch_rc = parent_cb.latch().latch_acquire(LATCH_EX, sthread_t::WAIT_IMMEDIATE);
    if (latch_rc.is_error()) {
        DBGOUT2(<<"_unswizzle_a_frame: oops, unlucky. someone is latching this page. skipiing this. rc=" << latch_rc);
        return false;
    }
    latch_auto_release auto_rel(parent_cb.latch()); // this automatically releaes the latch.

    fixable_page_h parent;
    w_assert1(false == parent_cb._in_doubt);
    parent.fix_nonbufferpool_page(_buffer + parent_idx);
    if (child_slot >= (uint32_t) parent.max_child_slot()+1) {
        return false;
    }
    shpid_t* shpid_addr = parent.child_slot_address(child_slot);
    shpid_t shpid = *shpid_addr;
    if ((shpid & SWIZZLED_PID_BIT) == 0) {
        return false;
    }
    bf_idx child_idx = shpid ^ SWIZZLED_PID_BIT;
    bf_tree_cb_t &child_cb = get_cb(child_idx);
    // don't unswizzle a frame that is hotter than current threshold temperature
    if (child_cb._refbit_approximate > _swizzle_clockhand_threshold) {
        return false;
    }
    w_assert1(child_cb._used);
    w_assert1(child_cb._swizzled);
    // in some lazy testcases, _buffer[child_idx] aren't initialized. so these checks are disabled.
    // see the above comments on cache miss
    // w_assert1(child_cb._pid_shpid == _buffer[child_idx].pid.page);
    // w_assert1(_buffer[child_idx].btree_level == 1);
    w_assert1(child_cb._pid_vol == parent_cb._pid_vol);
    w_assert1(child_cb.pin_cnt() >= 1); // because it's swizzled
    w_assert1(child_idx == _hashtable->lookup(bf_key (child_cb._pid_vol, child_cb._pid_shpid)));
    child_cb._swizzled = false;
#ifdef BP_TRACK_SWIZZLED_PTR_CNT
    if (parent_cb._swizzled_ptr_cnt_hint > 0) {
        parent_cb._swizzled_ptr_cnt_hint--;
    }
#endif
    // because it was swizzled, the current pin count is >= 1, so we can simply do atomic decrement.
    _decrement_pin_cnt_assume_positive(child_idx);
    --_swizzled_page_count_approximate;

    *shpid_addr = child_cb._pid_shpid;
    w_assert1(((*shpid_addr) & SWIZZLED_PID_BIT) == 0);

    return true;
}

#ifdef BP_MAINTAIN_PARENT_PTR
void bf_tree_m::_unswizzle_with_parent_pointer() {
}
#endif // BP_MAINTAIN_PARENT_PTR

///////////////////////////////////   SWIZZLE/UNSWIZZLE END ///////////////////////////////////

void bf_tree_m::debug_dump(std::ostream &o) const
{
    o << "dumping the bufferpool contents. _block_cnt=" << _block_cnt << "\n";
    o << "  _freelist_len=" << _freelist_len << ", HEAD=" << FREELIST_HEAD << "\n";
#ifdef BP_MAINTAIN_PARENT_PTR
    o << "  _swizzled_lru_len=" << _swizzled_lru_len << ", HEAD=" << SWIZZLED_LRU_HEAD << ", TAIL=" << SWIZZLED_LRU_TAIL << std::endl;
#endif // BP_MAINTAIN_PARENT_PTR

    for (volid_t vid = 1; vid < MAX_VOL_COUNT; ++vid) {
        bf_tree_vol_t* vol = _volumes[vid];
        if (vol != NULL) {
            o << "  volume[" << vid << "] root pages(stnum=bf_idx):";
            for (uint32_t store = 1; store < MAX_STORE_COUNT; ++store) {
                if (vol->_root_pages[store] != 0) {
                    o << ", " << store << "=" << vol->_root_pages[store];
                }
            }
            o << std::endl;
        }
    }
    for (bf_idx idx = 1; idx < _block_cnt && idx < 1000; ++idx) {
        o << "  frame[" << idx << "]:";
        bf_tree_cb_t &cb = get_cb(idx);
        if (cb._used) {
            o << "page-" << cb._pid_vol << "." << cb._pid_shpid;
            if (cb._dirty) {
                o << " (dirty)";
            }
            if (cb._in_doubt) {
                o << " (in_doubt)";
            }
#ifdef BP_MAINTAIN_PARENT_PTR
            o << ", _parent=" << cb._parent;
#endif // BP_MAINTAIN_PARENT_PTR
            o << ", _swizzled=" << cb._swizzled;
            o << ", _pin_cnt=" << cb.pin_cnt();
            o << ", _rec_lsn=" << cb._rec_lsn;
            o << ", _dependency_idx=" << cb._dependency_idx;
            o << ", _dependency_shpid=" << cb._dependency_shpid;
            o << ", _dependency_lsn=" << cb._dependency_lsn;
            o << ", _refbit_approximate=" << cb._refbit_approximate;
#ifdef BP_MAINTAIN_PARENT_PTR
            o << ", _counter_approximate=" << cb._counter_approximate;
            if (_is_in_swizzled_lru(idx)) {
                o << ", swizzled_lru.prev=" << SWIZZLED_LRU_PREV(idx) << ".next=" << SWIZZLED_LRU_NEXT(idx);
            }
#endif // BP_MAINTAIN_PARENT_PTR
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
void bf_tree_m::debug_dump_pointer(ostream& o, shpid_t shpid) const
{
    if (shpid & SWIZZLED_PID_BIT) {
        bf_idx idx = shpid ^ SWIZZLED_PID_BIT;
        o << "swizzled(bf_idx=" << idx;
        o << ", page=" << get_cb(idx)._pid_shpid << ")";
    } else {
        o << "normal(page=" << shpid << ")";
    }
}

shpid_t bf_tree_m::debug_get_original_pageid (shpid_t shpid) const {
    if (is_swizzled_pointer(shpid)) {
        bf_idx idx = shpid ^ SWIZZLED_PID_BIT;
        return get_cb(idx)._pid_shpid;
    } else {
        return shpid;
    }
}

w_rc_t bf_tree_m::set_swizzling_enabled(bool enabled) {
    if (_enable_swizzling == enabled) {
        return RCOK;
    }
    DBGOUT1 (<< "changing the pointer swizzling setting in the bufferpool to " << enabled << "... ");

    // changing this setting affects all buffered pages because
    //   swizzling-off : "_parent" in the control block is not set
    //   swizzling-on : "_parent" in the control block is set

    // first, flush out all dirty pages. we assume there is no concurrent transaction
    // which produces dirty pages from here on. if there is, booomb.
    W_DO(force_all());

    // remember what volumes are loaded.
    typedef vol_t* volptr;
    volptr installed_volumes[MAX_VOL_COUNT];
    for (volid_t i = 1; i < MAX_VOL_COUNT; ++i) {
        if (_volumes[i] == NULL) {
            installed_volumes[i] = NULL;
        } else {
            installed_volumes[i] = _volumes[i]->_volume;
        }
    }

    // clear all properties. could call uninstall_volume for each of them,
    // but nuking them all is faster.
    for (bf_idx i = 0; i < _block_cnt; i++) {
        bf_tree_cb_t &cb = get_cb(i);
        cb.clear();
    }
#ifdef BP_MAINTAIN_PARENT_PTR
    ::memset (_swizzled_lru, 0, sizeof(bf_idx) * _block_cnt * 2);
    _swizzled_lru_len = 0;
#endif // BP_MAINTAIN_PARENT_PTR
    _freelist[0] = 1;
    for (bf_idx i = 1; i < _block_cnt - 1; ++i) {
        _freelist[i] = i + 1;
    }
    _freelist[_block_cnt - 1] = 0;
    _freelist_len = _block_cnt - 1; // -1 because [0] isn't a valid block

    //re-create hashtable
    delete _hashtable;
    int buckets = w_findprime(1024 + (_block_cnt / 4));
    _hashtable = new bf_hashtable(buckets);
    w_assert0(_hashtable != NULL);
    ::memset (_volumes, 0, sizeof(bf_tree_vol_t*) * MAX_VOL_COUNT);
    _dirty_page_count_approximate = 0;

    // finally switch the property
    _enable_swizzling = enabled;

    // then, reload volumes
    for (volid_t i = 1; i < MAX_VOL_COUNT; ++i) {
        if (installed_volumes[i] != NULL) {
            W_DO(install_volume(installed_volumes[i]));
        }
    }

    DBGOUT1 (<< "changing the pointer swizzling setting done. ");
    return RCOK;
}

void bf_tree_m::get_rec_lsn(bf_idx &start, uint32_t &count, lpid_t *pid,
                             lsn_t *rec_lsn, lsn_t *page_lsn, lsn_t &min_rec_lsn,
                             const lsn_t master, const lsn_t current_lsn,
                             lsn_t last_mount_lsn)
{
    // Only used by checkpoint to gather dirty page information
    // Caller is the checkpoint operation which is holding a 'write' mutex',
    // everything in this function MUST BE W_COERCE (not W_DO).

    w_assert1(start > 0 && count > 0);

    bf_idx i = 0;
    // _block_cnt is the number of blocks/pages in buffer pool, while 0 is never used
    for (i = 0; i < count && start < _block_cnt; ++start)
    {
        bf_tree_cb_t &cb = get_cb(start);

        // Acquire traditional read latch for each page, not Q latch
        // because it cannot fail on latch release
        w_rc_t latch_rc = cb.latch().latch_acquire(LATCH_SH, WAIT_FOREVER);
        if (latch_rc.is_error())
        {
            // Unable to the read acquire latch, cannot continue, raise an internal error
            DBGOUT2 (<< "Error when acquiring LATCH_SH for checkpoint buffer pool. cb._pid_shpid = "
                     << cb._pid_shpid << ", rc = " << latch_rc);

            // Called by checkpoint operation which is holding a 'write' mutex on checkpoint
            // To be a good citizen, release the 'write' mutex before raise error
            chkpt_serial_m::write_release();

            W_FATAL_MSG(fcINTERNAL, << "unable to latch a buffer pool page");
            return;
        }

        // The earliest LSN which made this page dirty
        lsn_t lsn(cb._rec_lsn);
        w_assert3(lsn == lsn_t::null || lsn.hi() > 0);

        // If a page is in use and dirty, or is in_doubt (only marked by Log Analysis phase)
        if ((cb._used && cb._dirty) || (cb._in_doubt))
        {
            if (cb._in_doubt)
            {
                // in_doubt is only marked during Log Analysis and the page is not loaded until
                // REDO phase.  The 'in_doubt' flag would be replaced by 'dirty' flag in REDO phase.
                // A checkpoint is taken at the end of Log Analysis phase, therefore
                // in_doubt pages are not loaded into buffer pool yet, cannot get information
                // from page ('_buffer')

                // lpid_t: Store ID (volume number + store number) + page number (4+4+4)
                // Re-construct the lpid using several fields in cb
                vid_t vid(cb._pid_vol);
                lpid_t store_id(vid, cb._store_num, cb._pid_shpid);
                w_assert1(0 != cb._pid_shpid);   // Page number cannot be 0
                pid[i] = store_id;
                // rec_lsn is when the page got dirty initially, since this is an in_doubt page for Log Analysis
                // use the smaller LSN between cb and checkpoint master
                rec_lsn[i] = (lsn.data() < master.data())? lsn : master;
                w_assert1(lsn_t::null!= current_lsn);
                page_lsn[i] = current_lsn;    // Use the current LSN for page LSN
            }
            else
            {
                // Ignore this page if the pin count is -1
                // Checkpoint records dirty pages in buffer pool, we never evict dirty pages so ignoring
                // a page that is being evicted (pin_cnt == -1) is safe.
                if (cb.pin_cnt() == -1)
                {
                    if (cb.latch().held_by_me())
                        cb.latch().latch_release();
                    continue;
                }

                // Actual dirty page
                // Record pid, minimum (earliest) and latest LSN values of the page
                // cb._rec_lsn is the minimum LSN
                // buffer[start].lsn.data() is the Page LSN, which is the last write LSN

                if ((lsn_t::null == lsn) || (0 == lsn.data()))
                {
                    // If we have a dirty page but _rec_lsn (initial dirty) is 0
                    // someone forgot to set it for the dirty page (most likely a
                    // newly allocated page and it has not been formatted yet, therefore 
                    // the page does not exist on disk and does not contain any change yet).
                    // In this case we won't be able to trace the history of the dirty
                    // page during a crash recovery.
////////////////////////////////////////
// TODO(Restart)... 
// Solution #1:
// We could use the last_mount_lsn as the LSN for this page as a defensive 
// approach, so the Recovery REDO would start the log scan from the mount lsn
// but several issues with this approach
//  1. Last mount LSN might be very old, which would cause REDO LSN
//      to go far back in time, wasted effort if using log driven REDO but
//      it should not cause functional impact.
//  2. If system crashes immediatelly after the checkpoint while the page was not
//      formated, the Log Analysis phase will mark this page as 'in_doubt', but if
//      log-driven REDO was used, there is nothing to recovery for this page and
//      causes a REDO count mis-match even there was no error.
//  3. If there was a second device mount just before the checkpoint,
//      this last mount LSN might not be correct, this would cause a functional
//      impact.
//      In general, multiple device mounting/dismounting was not taken care of
//      in the current 'Restart' implementation, see TODO in restart.cpp Log Analysis
//
// Solution #2:
// Simply ignore this allocated but not formatted page, since there is nothing to recover
//
// Use solution #2, the solution is based on the assumption that
// once a page format log record arrived after the page allocation log record, we will have
// a valid LSN for the page, so this is an extreme corner case
////////////////////////////////////////

                    w_assert1(0 != last_mount_lsn.data());
                    // Solution #1: use last mount lsn
                    lsn = last_mount_lsn;
                    
                    // Solution #2: ignore this page
                    if (cb.latch().held_by_me())
                        cb.latch().latch_release();
                    continue;
                }

                // Now we have a page we want to record               
                pid[i] = _buffer[start].pid;
                w_assert1(0 != pid[i].page);   // Page number cannot be 0

                w_assert1(lsn_t::null != lsn);   // Must have a vliad first lsn
                rec_lsn[i] = lsn;
#ifdef USE_ATOMIC_COMMIT // use clsn instead
                // If page was not fetched from disk initially, but recently
                // allocated, then its CLSN is null, until its first update
                // (normally a page format) commits.
                if(_buffer[start].clsn == lsn_t::null) {
                    w_assert1(lsn_t::null != lsn);
                    page_lsn[i] = lsn;
                }
                else {
                    page_lsn[i] = _buffer[start].clsn.data();
                }
#else
                w_assert1(lsn_t::null != _buffer[start].lsn.data());
                page_lsn[i] = _buffer[start].lsn.data();
#endif
            }

            // Update min_rec_lsn if necessary (if lsn != NULL)
            if (min_rec_lsn.data() > lsn.data())
            {
                min_rec_lsn = lsn;
                w_assert3(lsn.hi() > 0);
            }

            // Increment counter
            ++i;
        }

        // Done with this cb, release the latch on it before moving to the next cb
        if (cb.latch().held_by_me())
            cb.latch().latch_release();
    }
    count = i;
}

// for debugging swizzling policy purposes -- todo: remove it when done
#if 0
int bf_tree_m::nframes(int priority, int level, int refbit, bool swizzled, bool print) {
    int n=0;
    for (bf_idx idx = 0; idx <= _block_cnt; idx++) {
        bf_tree_cb_t &cb(get_cb(idx));
        if (cb._replacement_priority == priority) {
            if (_buffer[idx].btree_level >= level) {  // NO LONGER LEGAL ACCESS; REMOVE <<<>>>
                if (cb._refbit_approximate >= refbit) {
                    if (cb._swizzled == swizzled) {
                        n++;
                        if (print) {
                            std::cout << idx << std::endl;
                        }
                    }
                }
            }
        }
    }
    return n;
}
#endif

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

w_rc_t bf_tree_m::_check_read_page(generic_page* parent, bf_idx idx,
                                   volid_t vol, shpid_t shpid,
                                   const bool past_end,    // In: true if page does not exist on disk
                                                           //     this can happen only if fix_direct for REDO
                                   const lsn_t page_emlsn) // In: if != 0, it is the last page update LSN identified
                                                           //     during Log Analysis and stored in cb,
                                                           //     it is used to recover the page if parent page
                                                           //     does not exist
{
    w_assert1(shpid != 0);
    generic_page &page = _buffer[idx];
    uint32_t checksum = page.calculate_checksum();
    
    if (checksum == 0) {
        DBGOUT0(<<"_check_read_page(): empty checksum?? PID=" << shpid);
    }

    if ((true == past_end) && (parent == NULL))
    {
        ERROUT(<<"bf_tree_m: page does not exist on disk and it is from REDO operation, page: " << shpid);
        // Special scenario from Recovery and REDO with multi-page log record
        // loading the 2nd page via fix_direct.
        // Failure on failure case, need to recover from scratch, no parent page pointer (from fix_direct)
        // therefore we cannot use the typical logic in this function
        // The page has already been zero'd out
        // Call recover_single_page() directly using emlsn if we have a valid one from cb:
        //    page: page to recover
        //    emlsn:  the LSN identified during Log Analysis phase
        //    actual_emlsn: true
        //   or
        //    last_lsn:  the last LSN from pre-crash recovery log
        //    actual_emlsn: false because emlsn is not the actual emlsn

        // Force a complete recovery
        _buffer[idx].lsn = lsn_t::null;
        snum_t store;
        if (NULL != parent)
            store = parent->pid.store();
        else
            store = 1;  // No information for store
        _buffer[idx].pid = lpid_t(vol, store, shpid);
        _buffer[idx].tag = t_btree_p;

        btree_page_h p;
        p.fix_nonbufferpool_page(_buffer + idx);    // Page is marked as not buffer pool managed through this call
                                                    // This is important for minimal logging of page rebalance operation
        if (lsn_t::null != page_emlsn)
            return (smlevel_1::recovery->recover_single_page(p, page_emlsn, true /*actual_emlsn*/));
        else
            return (smlevel_1::recovery->recover_single_page(p, smlevel_0::last_lsn, false /*actual_emlsn*/));
    }
    else if (checksum != page.checksum)
    {
        if (NULL != parent)
        {
            // User transaction page loading detected inconsistent checksum
            // on the target page during normal page loading, recovery required
            if(true == past_end)
            {
                DBGOUT3(<<"bf_tree_m: user transaction, non-existing page " << shpid << ", recovery via Single Page Recovery");
            }
            else
            {
                DBGOUT3(<<"bf_tree_m: user transaction, bad page checksum in page " << shpid << ", recovery via Single Page Recovery");
            }
        }
        else if ((NULL == parent) && (lsn_t::null != page_emlsn))
        {
            // Not a user transaction, it is a forced page load due to recovery
            DBGOUT3(<<"bf_tree_m: force load with page emlsn in page " << shpid << ", recovery via Single Page Recovery");
        }
        else
        {
            // No parent and no page_emlsn, it should not happen,  j.i.c.
            DBGOUT3(<<"bf_tree_m: force load but no page emlsn in page " << shpid << ", not able to recover via Single Page Recovery");
        }

        // Checksum didn't agree! this page image is completely
        // corrupted and we have to recover the page from scratch.

        if (parent == NULL)
        {
            // No parent page, caller is not a user transaction
            if (lsn_t::null != page_emlsn)
            {
                DBGOUT3(<<"_check_read_page(): use emlsn gathered from Log Analysis for recovery: " << page_emlsn);
                // We have the LSN identified during Log Analysis,
                // try the Single Page Recovery in this case even the
                // parent page is not available

                // TODO(Restart)... this is assuming store number is not corrupted
                // it might not be a safe assumption since checksum if different
                lpid_t pid = lpid_t(vol, page.pid.store(), shpid);

                // Wipe out the page so we recover from scratch
                ::memset(&_buffer[idx], '\0', sizeof(generic_page));
                // Force a complete recovery
                _buffer[idx].lsn = lsn_t::null;
                _buffer[idx].pid = pid;
                _buffer[idx].tag = t_btree_p;
                btree_page_h p;
                p.fix_nonbufferpool_page(_buffer + idx);  // Page is marked as not buffer pool managed through this call
                                                          // This is important for minimal logging of page rebalance operation
                W_DO(smlevel_1::recovery->recover_single_page(p, page_emlsn, true /*actual_emlsn*/));
            }
            else
            {
                // If parent page is not available (eg, via fix_direct) and no emlsn
                // we can't apply Single-Page-Recovery. Return error and let the caller
                // decide what to do (e.g., re-traverse from root).
                return RC(eNO_PARENT_SPR);
            }
        }
        else
        {
            // Has parent page, caller is a user transaction
            // Check the input parameter page_emlsn
            // Valid page_emlsn: force loading a page while page_emlsn was from Log Analysis, use it to recover
            // Invalid page_emlsn: normal page loading due to user transaction, use emlsn from parent to recover

            DBGOUT3(<<"_check_read_page(): Caller is a user transaction, recovery with known parent page");
            W_DO(_try_recover_page(parent, idx, vol, shpid, true, page_emlsn));
        }
    }

    // Then, page ID must match.
    // There should be no case where checksum agrees but page ID doesn't agree.
    if (page.pid.page != shpid || page.pid.vol().vol != vol) {
        W_FATAL_MSG(eINTERNAL, <<"inconsistent disk page: "
            << vol << "." << shpid << " was " << page.pid.vol().vol
            << "." << page.pid.page);
    }
    // Page is valid, but it might be stale...
    if (parent == NULL) {
        // EMLSN check is possible only when parent pointer is available.
        // In case of fix_direct, we do only checksum validation.
        // fix_direct are used in 1) restart and 2) cursor refix.
        // 1) is fine because we anyway recover the child page during REDO.
        // 2) is fine because of pin_for_refix() (in other words, it never comes here).
        return RCOK;
    }
    general_recordid_t recordid = find_page_id_slot(parent, shpid);
    btree_page_h parent_h;
    parent_h.fix_nonbufferpool_page(parent);
    lsn_t p_emlsn = parent_h.get_emlsn_general(recordid);
    if (lsn_t::null == p_emlsn)
        return RCOK;
    if (p_emlsn < page.lsn)
    {
        // Parent's EMLSN is out of date, e.g. system died before
        // parent was updated on child's previous eviction.
        // We can update it here and have to do more I/O
        // or try to catch it again on eviction and risk being unlucky.
    }
    else if (p_emlsn > page.lsn)
    {
        // Child is stale. Apply Single-Page-Recovery
        ERROUT(<< "Stale Child LSN found! Invoking Single-Page-Recovery.. parent=" << parent->pid
            << ", child pid=" << shpid << ", EMLSN=" << p_emlsn << " LSN=" << page.lsn);
#if W_DEBUG_LEVEL>0
        debug_dump(std::cerr);
#endif // W_DEBUG_LEVEL>0

        DBGOUT3(<<"bf_tree_m::_check_read_page: After recovery, target page emlsn < parent emlsn"
                << ", recover again without page_emlsn (From Log Analysis)");
        W_DO(_try_recover_page(parent, idx, vol, shpid, false /*corrupted*/, lsn_t::null));
    }
    // else child_emlsn == lsn. we are ok.
    return RCOK;
}

w_rc_t bf_tree_m::_try_recover_page(generic_page* parent,     // In: parent page
                                        bf_idx idx,              // In: index of the target page to be recovered
                                        volid_t vol,
                                        shpid_t shpid,
                                        bool corrupted,          // In: true if page was corrupted, need to zero out the page
                                        const lsn_t page_emlsn)  // In: If non-null, it is the page emlsn
                                                                 //     gathered from Log Analysis,
{
    if (corrupted) {
        ::memset(&_buffer[idx], '\0', sizeof(generic_page));
        _buffer[idx].lsn = lsn_t::null;
        _buffer[idx].pid = lpid_t(vol, parent->pid.store(), shpid);
        _buffer[idx].tag = t_btree_p;
    }
    general_recordid_t recordid = find_page_id_slot(parent, shpid);
    btree_page_h parent_h;
    parent_h.fix_nonbufferpool_page(parent);
    lsn_t emlsn = parent_h.get_emlsn_general(recordid);

    if ((lsn_t::null != page_emlsn) && (page_emlsn >= emlsn))
    {
        // If we have a valid page_emlsn from Log Analysis phase, this is a user transaction
        // which triggers a page REDO operation, if the page_emlsn from Log Analysis phase
        // is available, use it to recover the page
        // The page_emlsn might be different from the parent emlsn if page rebalance was
        // involved during the recovery
        w_assert1(page_emlsn >= emlsn);

        DBGOUT1(<<"bf_tree_m::_try_recover_page: Recovery a page with parent node, parent emlsn:"
                << emlsn << ", page_emlsn (from Log Analysis): " << page_emlsn << ", use page_emlsn for recovery");
        emlsn = page_emlsn;
    }
    else if ((lsn_t::null != page_emlsn) && (lsn_t::null == emlsn))
    {
        DBGOUT1(<<"bf_tree_m::_try_recover_page: Recovery a page with parent node, parent emlsn is NULL"
                << ", page_emlsn (from Log Analysis): " << page_emlsn << ", use parent_emlsn which is NULL");
        //        emlsn = page_emlsn;
    }
    else if (lsn_t::null != emlsn)
    {
        DBGOUT0(<<"bf_tree_m::_try_recover_page: Recovery a page with parent node, parent emlsn:"
                << emlsn << ", no page_emlsn (from Log Analysis), use parent for recovery");        
    }
    else
    {
        DBGOUT0(<<"bf_tree_m::_try_recover_page: Recovery a page with parent node, parent emlsn is NULL"
                << ", no page_emlsn (from Log Analysis)");
    }

    if (emlsn == lsn_t::null)
    {
        // Parent page does not have emlsn, and no page_emlsn from Log Analysis
        // Nothing we can do at this point
        DBGOUT0(<<"bf_tree_m::_try_recover_page: Parent page does not have emlsn, no recovery");
        return RCOK; // this can happen when the page has been just created.
    }

    btree_page_h p;
    p.fix_nonbufferpool_page(_buffer + idx);    // Page is marked as not buffer pool managed through this call
                                                // This is important for minimal logging of page rebalance operation

    if (lsn_t::null != page_emlsn)
    {
        // From Restart, use the last write lsn on the page for recovery (not complete recovery)
        return smlevel_1::recovery->recover_single_page(p, emlsn, true, true);
    }
    else
    {
        // Not from Restart
        return smlevel_1::recovery->recover_single_page(p, emlsn);
    }

}
