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
#include "bf.h"
#include "sm_io.h"
#include "vol.h"
#include "alloc_cache.h"
#include "chkpt_serial.h"


#include <boost/static_assert.hpp>
#include <ostream>
#include <limits>

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

    _swizzle_clockhand_current_depth = 0;
    ::memset (_swizzle_clockhand_pathway, 0, sizeof(uint32_t) * MAX_SWIZZLE_CLOCKHAND_DEPTH);
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
    for (size_t i = 0; i < stores.size(); ++i) {
        snum_t store = stores[i];
        shpid_t shpid = stcache->get_root_pid(store);
        w_assert1(shpid > 0);

        bf_idx idx = 0;
        w_rc_t grab_rc = _grab_free_block(idx);
        if (grab_rc.is_error()) {
            ERROUT(<<"failed to grab a free page while mounting a volume: " << grab_rc);
            rc = grab_rc;
            break;
        }

        w_rc_t preload_rc = _preload_root_page(desc, volume, store, shpid, idx);
        if (preload_rc.is_error()) {
            ERROUT(<<"failed to preload a root page " << shpid << " of store " << store << " in volume " << vid << ". to buffer frame " << idx << ". err=" << preload_rc);
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
    bool passed_end;
    W_DO(volume->read_page(shpid, _buffer[idx], passed_end));

    if (_buffer[idx].calculate_checksum() != _buffer[idx].checksum) {
        return RC(eBADCHECKSUM);
    }
    
    bf_tree_cb_t &cb = get_cb(idx);
    cb.clear();
    cb._pid_vol = vid;
    cb._pid_shpid = shpid;
    // as the page is read from disk, at least it's sure that
    // the page is flushed as of the page LSN (otherwise why we can read it!)
    cb._rec_lsn = _buffer[idx].lsn.data();
    cb.pin_cnt_set(1); // root page's pin count is always positive
    cb._swizzled = true;
    cb._in_doubt = false;
    cb._used = true; // turn on _used at last
    bool inserted = _hashtable->insert_if_not_exists(bf_key(vid, shpid), idx); // for some type of caller (e.g., redo) we still need hashtable entry for root
    if (!inserted) {
        ERROUT (<<"failed to insert a root page to hashtable. this must not have happened because there shouldn't be any race. wtf");
        return RC(eINTERNAL);
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
            bool passed_end;
            W_DO(volume->read_page(idx, _buffer[idx], passed_end));
            if (_buffer[idx].calculate_checksum() != _buffer[idx].checksum) {
                return RC(eBADCHECKSUM);
            }
            bf_tree_cb_t &cb = get_cb(idx);
            cb.clear();
            cb._pid_vol = vid;
            cb._pid_shpid = idx;
            cb._rec_lsn = _buffer[idx].lsn.data();
            cb.pin_cnt_set(1);
            cb._in_doubt = false;
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

w_rc_t bf_tree_m::uninstall_volume(volid_t vid) {
    // assuming this thread is the only thread working on this volume,

    // first, clean up all dirty pages
    DBGOUT1(<<"uninstalling volume " << vid << " from buffer pool...");
    bf_tree_vol_t* desc = _volumes[vid];
    if (desc == NULL) {
        DBGOUT0(<<"this volume is already uninstalled: " << vid);
        return RCOK;
    }
    W_DO(_cleaner->force_volume(vid));

    // then, release all pages.
    for (bf_idx idx = 1; idx < _block_cnt; ++idx) {
        bf_tree_cb_t &cb = get_cb(idx);
        if (!cb._used || cb._pid_vol != vid) {
            continue;
        }
#ifdef BP_MAINTAIN_PARENT_PTR
        // if swizzled, remove from the swizzled-page LRU too
        if (_is_in_swizzled_lru(idx)) {
            _remove_from_swizzled_lru(idx);
        }
#endif // BP_MAINTAIN_PARENT_PTR
        _hashtable->remove(bf_key(vid, cb._pid_shpid));
        if (cb._swizzled) {
            --_swizzled_page_count_approximate;
        }
        get_cb(idx).clear();
        _add_free_block(idx);
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
    return _fix_nonswizzled(NULL, page, vol, shpid, mode, conditional, virgin_page);
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
                                   bool virgin_page) {
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

    // note that the hashtable is separated from this bufferpool.
    // we need to make sure the returned block is still there, and retry otherwise.
#if W_DEBUG_LEVEL>0
    int retry_count = 0;
#endif
    while (true) {
#if W_DEBUG_LEVEL>0
        if (++retry_count % 10000 == 0) {
            DBGOUT1(<<"keep trying to fix.. " << shpid << ". current retry count=" << retry_count);
        }
#endif
        bf_idx idx = _hashtable->lookup(key);
        if (idx == 0) {
            // the page is not in the bufferpool. we need to read it from disk
            W_DO(_grab_free_block(idx)); // get a frame that will be the new page
            w_assert1(idx != 0);
            bf_tree_cb_t &cb = get_cb(idx);
            DBGOUT1(<<"unswizzled case: load shpid = " << shpid << " into frame = " << idx);
            // after here, we must either succeed or release the free block
            if (virgin_page) {
                // except a virgin page. then the page is anyway empty
                DBGOUT3(<<"bf_tree_m: adding a virgin page ("<<vol<<"."<<shpid<<")to bufferpool.");
            } else {
                DBGOUT3(<<"bf_tree_m: cache miss. reading page "<<vol<<"." << shpid << " to frame " << idx);
                INC_TSTAT(bf_fix_nonroot_miss_count);
                bool passed_end;
                w_rc_t read_rc = volume->_volume->read_page(shpid, _buffer[idx], passed_end);
                if (read_rc.is_error()) {
                    DBGOUT3(<<"bf_tree_m: error while reading page " << shpid << " to frame " << idx << ". rc=" << read_rc);
                    _add_free_block(idx);
                    return read_rc;
                } else {
                    // for each page retrieved from disk, compare its checksum
                    uint32_t checksum = _buffer[idx].calculate_checksum();
                    if (checksum != _buffer[idx].checksum) {
                        ERROUT(<<"bf_tree_m: bad page checksum in page " << shpid);
                        _add_free_block(idx);
                        return RC (eBADCHECKSUM);
                    }
                    // Then, page ID must match
                    if (!virgin_page && (_buffer[idx].pid.page != shpid
                        || _buffer[idx].pid.vol().vol != vol)) {
                        W_FATAL_MSG(eINTERNAL, <<"inconsistent disk page: "
                            << vol << "." << shpid << " was " << _buffer[idx].pid.vol().vol
                            << "." << _buffer[idx].pid.page);
                    }
                }
            }

            // initialize control block
            // we don't have to atomically pin it because it's not referenced by any other yet

            // latch the page. (not conditional because this thread will be the only thread touching it)
            cb.clear_latch();
            w_rc_t rc_latch = cb.latch().latch_acquire(mode, sthread_t::WAIT_IMMEDIATE);
            w_assert1(!rc_latch.is_error());

            cb.clear_except_latch();
            cb._pid_vol = vol;
            cb._pid_shpid = shpid;
#ifdef BP_MAINTAIN_PARENT_PTR
            bf_idx parent_idx = 0;
            if (is_swizzling_enabled()) {
                parent_idx = parent - _buffer;
                w_assert1 (_is_active_idx(parent_idx));
                cb._parent = parent_idx;
            }
#endif // BP_MAINTAIN_PARENT_PTR
            if (!virgin_page) {
                // if the page is read from disk, at least it's sure that
                // the page is flushed as of the page LSN (otherwise why we can read it!)
                cb._rec_lsn = _buffer[idx].lsn.data();
            } else {
                cb._dirty = true;
                cb._in_doubt = false;
                ++_dirty_page_count_approximate;
            }
            cb._used = true;
            cb._refbit_approximate = BP_INITIAL_REFCOUNT; 
#ifdef BP_MAINTAIN_REPLACEMENT_PRIORITY
            cb._replacement_priority = me()->get_workload_priority();
#endif
            // finally, register the page to the hashtable.
            bool registered = _hashtable->insert_if_not_exists(key, idx);
            if (!registered) {
                // this pid already exists in bufferpool. this means another thread concurrently
                // added the page to bufferpool. unlucky, but can happen.
                DBGOUT1(<<"bf_tree_m: unlucky! another thread already added the page " << shpid << " to the bufferpool. discard my own work on frame " << idx);
                cb.latch().latch_release();
                cb.clear(); // well, it should be enough to clear _used, but this is anyway a rare event. wouldn't hurt to clear all.
                _add_free_block(idx);
                continue;
            }
            
            // okay, all done
#ifdef BP_MAINTAIN_PARENT_PTR
            if (is_swizzling_enabled()) {
		lintel::unsafe::atomic_fetch_add((uint32_t*) &(_control_blocks[parent_idx]._pin_cnt), 1); // we installed a new child of the parent      to this bufferpool. add parent's count
            }
#endif // BP_MAINTAIN_PARENT_PTR
            page = &(_buffer[idx]);

            return RCOK;
        } else {
            // unlike swizzled case, we have to atomically pin it while verifying it's still there.
            if (parent) {
                DBGOUT1(<<"swizzled case: parent = " << parent->pid << ", shpid = " << shpid << " frame=" << idx);
            } else {
                DBGOUT1(<<"swizzled case: parent = NIL"<< ", shpid = " << shpid << " frame=" << idx);
            }
            bf_tree_cb_t &cb = get_cb(idx);
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
                if (rc.is_error()) {
                    DBGOUT2(<<"bf_tree_m: latch_acquire failed in buffer frame " << idx << " rc=" << rc);
                } else {
                    page = &(_buffer[idx]);
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
    return _cleaner->force_all();
}
w_rc_t bf_tree_m::force_until_lsn(lsndata_t lsn) {
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
            W_DO (_get_replacement_block(ret));
            if (ret != 0) {
                return RCOK;
            }
        }
        else
        {
            // Freelist is empty and caller does not want to evict pages (Recovery M1)
            return RC(eBFFULL);
        }
    }
    return RCOK;
}

w_rc_t bf_tree_m::_get_replacement_block(bf_idx& ret) {
#ifdef SIMULATE_MAINMEMORYDB
    if (true) {
        ERROUT (<<"MAINMEMORY-DB. _get_replacement_block() shouldn't be called. wtf");
        return RC(eINTERNAL);
    }
#endif // SIMULATE_MAINMEMORYDB
    DBGOUT3(<<"trying to evict some page...");

    switch (_replacement_policy) {
        case POLICY_CLOCK:
            return _get_replacement_block_clock(ret, false);
        case POLICY_CLOCK_PRIORITY:
            return _get_replacement_block_clock(ret, true);
        case POLICY_RANDOM:
            return _get_replacement_block_random(ret);
    }
    ERROUT (<<"Unknown replacement policy");
    return RC(eINTERNAL);
}

w_rc_t bf_tree_m::_get_replacement_block_clock(bf_idx& ret, bool use_priority) {
    DBGOUT3(<<"trying to evict some page using the CLOCK replacement policy...");
    int blocks_replaced_count = 0;
    uint32_t rounds = 0; // how many times the clock hand looped in this function
    char priority_threshold = 0;

    while (true) {
        bf_idx idx = ++_clock_hand;
        if (idx >= _block_cnt) {
            if (blocks_replaced_count > 0 && rounds > 1) {
                // after a complete cycle we found at least one so we are essentially done
                return RCOK;
            }
            ++rounds;
            DBGOUT1(<<"clock hand looped! rounds=" << rounds);
            _clock_hand = 1;
            idx = 1;
            priority_threshold++;
            if (_swizzled_page_count_approximate >= (int) (_block_cnt * 95 / 100)) {
                _trigger_unswizzling(rounds >= 10);
            } else {
                if (rounds == 2) {
                    // most likely we have too many dirty pages
                    // let's try writing out dirty pages
                    W_DO(wakeup_cleaners());
                } else if (rounds >= 4 && rounds <= 100) {
                    // seems like we are still having troubles to find evictable page.
                    // this must be because of too many pages swizzled and cannot be evicted
                    // so, let's trigger unswizzling
                    _trigger_unswizzling(rounds >= 10); // in "urgent" mode if taking very long
                } else if (rounds > 100) {
                    ERROUT(<<"woooo, couldn't find an evictable page for long time. gave up!");
                    debug_dump(std::cerr);
                    return RC(eFRAMENOTFOUND);
                }
            }
            if (rounds >= 2) {
                g_me()->sleep(100);
                DBGOUT1(<<"woke up. now there should be some page to evict");
            }
        }
        w_assert1(idx > 0);

        bf_tree_cb_t &cb(get_cb(idx));

        // do not evict a page used by a high priority workload
        if (use_priority && (cb._replacement_priority > priority_threshold)) {
            continue;
        }

#ifndef BP_CAN_EVICT_INNER_NODE
        //do not evict interior nodes
        fixable_page_h p(_buffer + idx);
        if (p.has_children()) {
            continue;
        }
#endif

        // do not evict hot page

#if 0 /* aggressive refcount decrement -- for use when we don't cap refcount */
        if (cb._refbit_approximate > 0) {
            const uint32_t refbit_threshold = 3;
            if (cb._refbit_approximate > refbit_threshold) {
                cb._refbit_approximate /= (rounds >= 5 ? 8 : 2);
                //cb._refbit_approximate=0;
            } else {
                cb._refbit_approximate--;
                //cb._refbit_approximate=0;
            }
            continue;
        }
#else 
        if (cb._refbit_approximate > 0) {
            cb._refbit_approximate--;
            continue;
        }
#endif

        if (_try_evict_block(idx) == 0) {
            // return the first block found back to the caller and try to find more 
            // blocks to free
            if (blocks_replaced_count++ > 0) {
                _add_free_block(idx);
            } else {
                ret = idx;
            }
            if (blocks_replaced_count < (1)) {
                continue;
            }
            return RCOK;
        } else {
            // it can happen. we just give up this block
            continue;
        }
    }
    return RCOK;
}

w_rc_t bf_tree_m::_get_replacement_block_random(bf_idx& ret) {
    DBGOUT3(<<"trying to evict some page using the RANDOM replacement policy...");
    while (true) {
        int blocks_replaced_count = 0;
        unsigned tries = 0;

        bf_idx idx = me()->randn(_block_cnt-1) + 1;
        if (++tries < _block_cnt) {
            if (blocks_replaced_count > 0) {
                return RCOK;
            }
            W_DO(wakeup_cleaners());
            g_me()->sleep(100);
            DBGOUT1(<<"woke up. now there should be some page to evict");
            tries = 0;
        }
    
        if (_try_evict_block(idx) == 0) {
            // return the first block found back to the caller and try to find more to free
            if (blocks_replaced_count++ > 0) {
                _add_free_block(idx);
            } else {
                ret = idx;
            }
            if (blocks_replaced_count < 1024) {
                continue;
            }
            return RCOK;
        } else {
            // it can happen. we just give up this block
            continue;
        }
    }
    return RCOK;
}

int bf_tree_m::_try_evict_block(bf_idx idx) {
    bf_tree_cb_t &cb = get_cb(idx);

    // do not consider dirty pages (at this point)
    // we check this again later because we don't take locks as of this.
    // we also avoid grabbing unused block because it has to be grabbed via freelist
    if (cb._dirty || !cb._used || cb._in_doubt) {
        return -1;
    }

    // find a block that has no pinning (or not being evicted by others).
    // this check is approximate as it's without lock.
    // false positives are fine, and we do the real check later
    if (cb.pin_cnt() != 0) {
        return -1;
    }
    
    // if it seems someone latches it, give up.
    if (cb.latch().latch_cnt() != 0) { // again, we check for real later
        return -1;
    }
    
    // okay, let's try evicting this page.
    // first, we have to make sure the page's pin_cnt is exactly 0.
    // we atomically change it to -1.
    int zero = 0;
    if (lintel::unsafe::atomic_compare_exchange_strong(const_cast<int32_t     *>(&cb._pin_cnt), (int32_t * ) &zero , (int32_t) -1))
    {
        // CAS did it job. the current thread has an exclusive access to this block
        w_assert1(cb.pin_cnt() == -1);
        
        // let's do a real check.
        if (cb._dirty || !cb._used || cb._in_doubt) {
            DBGOUT1(<<"very unlucky, this block has just become dirty.");
            // oops, then put this back and give up this block
            cb.pin_cnt_set(0);
            return -1;
        }
        
        // check latches too. just conditionally test it to avoid long waits.
        w_rc_t latch_rc = cb.latch().latch_acquire(LATCH_EX, WAIT_IMMEDIATE);
        if (latch_rc.is_error()) {
            DBGOUT1(<<"very unlucky, someone has just latched this block.");
            cb.pin_cnt_set(0);
            return -1;
        }
        // we can immediately release EX latch because no one will newly take latch as _pin_cnt==-1
        cb.latch().latch_release();
        DBGOUT1(<<"evicting page idx = " << idx << " shpid = " << cb._pid_shpid 
                << " pincnt = " << cb.pin_cnt());
        
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
        return 0; // success
    } 
    // it can happen. we just give up this block
    return -1;
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

    //  don't swizzle foster-child:
    w_assert1( *p.child_slot_address(GeneralRecordIds::FOSTER_CHILD) != shpid );
    //for (int i = -1; i <= max_slot; ++i) {
    for (general_recordid_t i = GeneralRecordIds::PID0; i <= max_slot; ++i) {
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

void bf_tree_m::_dump_swizzle_clockhand() const {
    DBGOUT2(<< "current clockhand depth=" << _swizzle_clockhand_current_depth
        << ". _swizzled_page_count_approximate=" << _swizzled_page_count_approximate << " / " << _block_cnt);
    for (int i = 0; i < _swizzle_clockhand_current_depth; ++i) {
        DBGOUT2(<< "current clockhand pathway[" << i << "]:" << _swizzle_clockhand_pathway[i]);
    }
}

void bf_tree_m::_trigger_unswizzling(bool urgent) {
    if (!is_swizzling_enabled()) {
        return;
    }
    if (!urgent && !_are_there_many_swizzled_pages()) {
        // there seems not many swizzled pages. we don't bother unless it's really urgent
        return;
    }
#ifdef BP_MAINTAIN_PARENT_PTR
    _unswizzle_with_parent_pointer();
    if (true) return;
#endif // BP_MAINTAIN_PARENT_PTR

    if (_swizzle_clockhand_current_depth == 0) {
        _swizzle_clockhand_pathway[0] = 1;
        _swizzle_clockhand_current_depth = 1;
    }
    
#if W_DEBUG_LEVEL>=2
    DBGOUT2(<< "_trigger_unswizzling...");
    _dump_swizzle_clockhand();
#endif // W_DEBUG_LEVEL>=2
    
    uint32_t unswizzled_frames = 0;
    uint32_t old = _swizzle_clockhand_pathway[0];
    for (uint16_t i = 0; i < MAX_VOL_COUNT
            && unswizzled_frames < UNSWIZZLE_BATCH_SIZE
            && (urgent || _are_there_many_swizzled_pages());
        ++i) {
        volid_t vol = (old + i) % MAX_VOL_COUNT;
        if (_volumes[vol] == NULL) {
            continue;
        }
        if (i != 0) {
            // this means now we are moving on to another volume.
            _swizzle_clockhand_current_depth = 1; // reset descendants
            _swizzle_clockhand_pathway[0] = vol;
        }
        
        _unswizzle_traverse_volume(unswizzled_frames, vol);
    }
    if (unswizzled_frames < UNSWIZZLE_BATCH_SIZE && _are_there_many_swizzled_pages()) {
        // checked everything.
        _swizzle_clockhand_current_depth = 0;
    }

#if W_DEBUG_LEVEL>=1
    DBGOUT1(<< "_trigger_unswizzling: unswizzled " << unswizzled_frames << " frames.");
    _dump_swizzle_clockhand();
#endif // W_DEBUG_LEVEL>=1
}

void bf_tree_m::_unswizzle_traverse_volume(uint32_t &unswizzled_frames, volid_t vol) {
    if (_swizzle_clockhand_current_depth <= 1) {
        _swizzle_clockhand_pathway[1] = 1;
        _swizzle_clockhand_current_depth = 2;
    }
    uint32_t old = _swizzle_clockhand_pathway[1];
    if (old >= MAX_STORE_COUNT) {
        return;
    }
    uint32_t remaining = MAX_STORE_COUNT - old;
    for (uint32_t i = 0; i < remaining && unswizzled_frames < UNSWIZZLE_BATCH_SIZE; ++i) {
        snum_t store = old + i;
        if (_volumes[vol] == NULL) {
            return; // just give up in unlucky case (probably the volume has been just uninstalled)
        }
        if (_volumes[vol]->_root_pages[store] == 0) {
            continue;
        }
        if (i != 0) {
            // this means now we are moving on to another store.
            _swizzle_clockhand_current_depth = 2; // reset descendants
            _swizzle_clockhand_pathway[1] = store;
        }
        
        _unswizzle_traverse_store(unswizzled_frames, vol, store);
    }
    if (unswizzled_frames < UNSWIZZLE_BATCH_SIZE) {
        // exhaustively checked this volume. this volume is 'done'
        _swizzle_clockhand_current_depth = 1;
    }
}

void bf_tree_m::_unswizzle_traverse_store(uint32_t &unswizzled_frames, volid_t vol, snum_t store) {
    w_assert1 (_volumes[vol] != NULL);
    if (_volumes[vol]->_root_pages[store] == 0) {
        return; // just give up in unlucky case (probably the store has been just deleted)
    }
    bf_idx parent_idx = _volumes[vol]->_root_pages[store];
    fixable_page_h p;
    p.fix_nonbufferpool_page(&_buffer[parent_idx]);
    if (!p.has_children()) {
        return;
    }
    // collect cold pages first. if need more then repeat for hot pages
    _swizzle_clockhand_threshold = 0;
    _unswizzle_traverse_node (unswizzled_frames, vol, store, parent_idx, 2);
    if (unswizzled_frames < UNSWIZZLE_BATCH_SIZE) {
        _swizzle_clockhand_threshold = (uint32_t) -1; // set to maximum
        _unswizzle_traverse_node (unswizzled_frames, vol, store, parent_idx, 2);
    }
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

w_rc_t bf_tree_m::register_and_mark(bf_idx& ret,
                  lpid_t page_of_interest,
                  lsn_t new_lsn,             // Current log record LSN from log scan
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
        if (false == is_in_doubt(idx))        
            ++in_doubt_count;
        set_in_doubt(idx, new_lsn);

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
        cb._used = true;
        cb._refbit_approximate = BP_INITIAL_REFCOUNT; 

        // For a page from disk,  get _rec_lsn from the physical page 
        // (cb._rec_lsn = _buffer[idx].lsn.data())
        // But in Log Analysis, we don't load the page, so initialize _rec_lsn from new_lsn
        // which is the current log record LSN from log scan
        cb._rec_lsn = new_lsn.data();

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
                  shpid_t shpid, bool& passed_end)
{
    // Special function for Recovery REDO phase
    // idx is in hash table already
    // but the actual page has not loaded from disk into buffer pool yet

    // Caller of this fumction is responsible for acquire and release EX latch on this page
    
    w_rc_t rc = RCOK;
    w_assert1(vid != 0);
    w_assert1(shpid != 0);

    passed_end = false;

    DBGOUT3(<<"REDO phase: loading page " << vid << "." << shpid 
            << " into buffer pool frame " << idx);

    bf_tree_vol_t *volume = _volumes[vid];
    w_assert1(volume != NULL);
    w_assert1(shpid >= volume->_volume->first_data_pageid());

    // Load the physical page from disk
    rc = volume->_volume->read_page(shpid, _buffer[idx], passed_end);
    if (true == passed_end)
    {
        // During REDO phase when trying to load a page from disk, the disk does
        // not exist on disk.
        // We cannot apply REDO because the page in buffer pool has been zero out
        // notify the caller by setting a return flag
       
        passed_end = true;
        DBGOUT3(<<"REDO phase: page does not exist on disk: "
            << vid << "." << shpid);
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
            ERROUT(<<"bf_tree_m: bad page checksum in page " << shpid);
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

void bf_tree_m::_unswizzle_traverse_node(uint32_t &unswizzled_frames,
                                         volid_t vol,
                                         snum_t store,
                                         bf_idx node_idx,
                                         uint16_t cur_clockhand_depth) {
    w_assert1(cur_clockhand_depth < MAX_SWIZZLE_CLOCKHAND_DEPTH);
    if (_swizzle_clockhand_current_depth <= cur_clockhand_depth) {
        _swizzle_clockhand_pathway[cur_clockhand_depth] = 0;
        _swizzle_clockhand_current_depth = cur_clockhand_depth + 1;
    }
    uint32_t old = _swizzle_clockhand_pathway[cur_clockhand_depth];
    bf_tree_cb_t &node_cb = get_cb(node_idx);
    fixable_page_h node_p;
    w_assert1(false == node_cb._in_doubt);
    node_p.fix_nonbufferpool_page(_buffer + node_idx);
    if (old >= (uint32_t) node_p.max_child_slot()+1) {
        return;
    }

    // check children
    uint32_t remaining = node_p.max_child_slot()+1 - old;
    for (uint32_t i = 0; i < remaining && unswizzled_frames < UNSWIZZLE_BATCH_SIZE; ++i) {
        uint32_t slot = old + i;
        if (!node_cb._used || !node_p.has_children()) {
            return;
        }

        shpid_t shpid = *node_p.child_slot_address(slot);
        if ((shpid & SWIZZLED_PID_BIT) == 0) {
            // if this page is not swizzled, none of its descendants is not swizzled either.
            continue;
        }
        if (i != 0) {
            // this means now we are moving on to another child.
            _swizzle_clockhand_pathway[cur_clockhand_depth] = slot;
            _swizzle_clockhand_current_depth = cur_clockhand_depth + 1;
        }

        bf_idx child_idx = shpid ^ SWIZZLED_PID_BIT;
        fixable_page_h node_child;
        node_child.fix_nonbufferpool_page(_buffer + child_idx);
        if (node_child.has_children()) {
            // child is also an intermediate node
            _unswizzle_traverse_node (unswizzled_frames, vol, store, child_idx, cur_clockhand_depth + 1);
            // if the child node is left with no swizzled pointers then try to unswizzle 
            // the parent pointer to the child as well 
            bf_tree_cb_t& child_cb = get_cb(child_idx);
            if (unswizzled_frames < UNSWIZZLE_BATCH_SIZE &&
                child_cb._swizzled_ptr_cnt_hint == 0) 
            {
                // this is just a hint. try conditionally latching the child and do the actual check
                w_rc_t latch_rc = child_cb.latch().latch_acquire(LATCH_SH, sthread_t::WAIT_IMMEDIATE);
                if (latch_rc.is_error()) {
                    DBGOUT2(<<"_unswizzle_traverse_node: oops, unlucky. someone is latching this page. skiping this. rc=" << latch_rc);
                } else {
                    if (!has_swizzled_child(child_idx)) {
                        // unswizzle_a_frame will try to conditionally latch a parent while
                        // we hold a latch on a child. While this is latching in the reverse order,
                        // it is still safe against deadlock as the operation is conditional.
                        bool unswizzled = _unswizzle_a_frame (node_idx, slot);
                        if (unswizzled) {
                            ++unswizzled_frames;
                        }
                    }
                    child_cb.latch().latch_release();
                }
            }
        } else {
            // child is a leaf page. now let's unswizzle it!
            bool unswizzled = _unswizzle_a_frame (node_idx, slot);
            if (unswizzled) {
                ++unswizzled_frames;
            }
        }
    }

    if (unswizzled_frames < UNSWIZZLE_BATCH_SIZE) {
        // exhaustively checked this node's descendants. this node is 'done'
        _swizzle_clockhand_current_depth = cur_clockhand_depth;
    }
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
    o << "dumping the bufferpool contents. _block_cnt=" << _block_cnt << ", _clock_hand=" << _clock_hand << "\n";
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
                             const lsn_t master, const lsn_t current_lsn)
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

        // If a page is in use and dirty, or is in_doubt (only marked by Log Analysis phase)
        if ((cb._used && cb._dirty && lsn != lsn_t::null) || (cb._in_doubt))
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
                    cb.latch().latch_release();                
                    continue;
                }
            
                // Actual dirty page
                // Record pid, minimum (earliest) and latest LSN values of the page
                // cb._rec_lsn is the minimum LSN
                // buffer[start].lsn.data() is the Page LSN, which is the latest LSN
                
                pid[i] = _buffer[start].pid;
                w_assert1(0 != pid[i].page);   // Page number cannot be 0
                w_assert1(lsn_t::null!= lsn);
                rec_lsn[i] = lsn;
                w_assert1(lsn_t::null!= _buffer[start].lsn.data());
                page_lsn[i] = _buffer[start].lsn.data();
            }

            // Update min_rec_lsn if necessary
            if(min_rec_lsn > lsn) 
            {
                min_rec_lsn = lsn;
            }

            // Increment counter
            ++i;
        }

        // Done with this cb, release the latch on it before moving to the next cb
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
