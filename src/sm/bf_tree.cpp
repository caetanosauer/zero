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

#include <boost/static_assert.hpp>
#include <ostream>
#include <limits>

#include "latch.h"
#include "btree_page_h.h"
#include "log.h"
#include "xct.h"
#include <logfunc_gen.h>
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
    W_DO(volume->read_page(shpid, _buffer[idx]));

    // _buffer[idx].checksum == 0 is possible when the root page has been never flushed out.
    // this method is called during volume mount (even before recover), so crash tests like
    // test_restart, test_crash
    if (_buffer[idx].checksum == 0) {
        DBGOUT0(<<"emptycheck sum root page during volume nount. crash happened?"
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
    // as the page is read from disk, at least it's sure that
    // the page is flushed as of the page LSN (otherwise why we can read it!)
    cb._rec_lsn = _buffer[idx].lsn.data();
    cb.pin_cnt_set(1); // root page's pin count is always positive
    cb._swizzled = true;
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
            W_DO(volume->read_page(idx, _buffer[idx]));
            if (_buffer[idx].calculate_checksum() != _buffer[idx].checksum) {
                return RC(eBADCHECKSUM);
            }
            bf_tree_cb_t &cb = get_cb(idx);
            cb.clear();
            cb._pid_vol = vid;
            cb._pid_shpid = idx;
            cb._rec_lsn = _buffer[idx].lsn.data();
            cb.pin_cnt_set(1);
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
                w_rc_t read_rc = volume->_volume->read_page(shpid, _buffer[idx]);
                if (read_rc.is_error()) {
                    DBGOUT3(<<"bf_tree_m: error while reading page " << shpid << " to frame " << idx << ". rc=" << read_rc);
                    _add_free_block(idx);
                    return read_rc;
                    /* TODO: if this is an I/O error, we could try to do SPR on it. */
                } else {
                    // for each page retrieved from disk, check validity, possibly apply SPR.
                    w_rc_t check_rc = _check_read_page(parent, idx, vol, shpid);
                    if (check_rc.is_error()) {
                        _add_free_block(idx);
                        return check_rc;
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

w_rc_t bf_tree_m::_grab_free_block(bf_idx& ret) {
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
        W_DO (_get_replacement_block());
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

    ERROUT(<<"woooo, couldn't find an evictable page for long time. gave up!");
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
    if (cb._dirty || !cb._used || !parent_cb._used) {
        return false;
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
            // oops, then put this back and give up this block
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
    if (cb._dirty || !cb._used) {
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
            DBGOUT1(<<"Unlucky, failed to latch parent block while evicting. skipping this");
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

    // Output SPR log for updating EMLSN in parent.
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
            if (idx == 0) {
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

w_rc_t bf_tree_m::_evict_page(EvictionContext& context, btree_page_h& p) {
    bf_idx idx = context.bufidx_pathway[context.traverse_depth - 1];
    uint32_t slot = context.clockhand_pathway[context.traverse_depth - 1];
    bool swizzled = context.swizzled_pathway[context.traverse_depth - 1];
    bf_idx parent_idx = context.bufidx_pathway[context.traverse_depth - 2];

    bf_tree_cb_t &cb = get_cb(idx);
    DBGOUT1(<<"_evict_page: idx=" << idx << ", slot=" << slot << ", pid=" << p.pid()
        << ", swizzled=" << swizzled << ", hot-ness=" << cb._refbit_approximate
        << ", dirty=" << cb._dirty << " parent_idx=" << parent_idx);

    // do not evict hot pages, dirty pages
    if (!cb._used || cb._dirty) {
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

void bf_tree_m::get_rec_lsn(bf_idx &start, uint32_t &count, lpid_t *pid, lsn_t *rec_lsn, lsn_t &min_rec_lsn)
{
    w_assert1(start > 0 && count > 0);

    bf_idx i;
    for (i = 1; i < count && start < _block_cnt; ++start)  {
        bf_tree_cb_t &cb = get_cb(start);
        lsn_t lsn(cb._rec_lsn);
        if (cb._used && cb._dirty && lsn != lsn_t::null) {
            pid[i] = _buffer[start].pid;
            rec_lsn[i] = lsn;
            if(min_rec_lsn > lsn) {
                min_rec_lsn = lsn;
            }
            ++i;
        }
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
                                   volid_t vol, shpid_t shpid) {
    w_assert0(shpid != 0);
    generic_page &page = _buffer[idx];
    uint32_t checksum = page.calculate_checksum();
    if (checksum == 0) {
        DBGOUT0(<<"_check_read_page(): empty checksum?? PID=" << shpid);
    }
    if (checksum != page.checksum) {
        ERROUT(<<"bf_tree_m: bad page checksum in page " << shpid);
        // Checksum didn't agree! this page image is completely
        // corrupted and we have to recover the page from scratch.

        if (parent == NULL) {
            // If parent page is not available (eg, via fix_direct),
            // we can't apply SPR. Then we return error and let the caller
            // to decide what to do (e.g., re-traverse from root).
            return RC(eNO_PARENT_SPR);
        }
        W_DO(_try_recover_page(parent, idx, vol, shpid, true));
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
    lsn_t emlsn = parent_h.get_emlsn_general(recordid);
    if (emlsn < page.lsn) {
        // Parent's EMLSN is out of date, e.g. system died before
        // parent was updated on child's previous eviction.
        // We can update it here and have to do more I/O
        // or try to catch it again on eviction and risk being unlucky.
    } else if (emlsn > page.lsn) {
        // Child is stale. Apply SPR
        ERROUT(<< "Stale Child LSN found! Invoking SPR.. parent=" << parent->pid
            << ", child pid=" << shpid << ", EMLSN=" << emlsn << " LSN=" << page.lsn);
#if W_DEBUG_LEVEL>0
        debug_dump(std::cerr);
#endif // W_DEBUG_LEVEL>0
        W_DO(_try_recover_page(parent, idx, vol, shpid, false));
    }
    // else child_emlsn == lsn. we are ok.
    return RCOK;
}

w_rc_t bf_tree_m::_try_recover_page(generic_page* parent, bf_idx idx, volid_t vol,
                                    shpid_t shpid, bool corrupted) {
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
    if (emlsn == lsn_t::null) {
        return RCOK; // this can happen when the page has been just created.
    }
    btree_page_h p;
    p.fix_nonbufferpool_page(_buffer + idx);
    return smlevel_0::log->recover_single_page(p, emlsn);
}
