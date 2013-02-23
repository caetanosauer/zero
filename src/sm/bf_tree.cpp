#include "w_defines.h"

#include "bf_hashtable.h"
#include "bf_tree_cb.h"
#include "bf_tree_vol.h"
#include "bf_tree_cleaner.h"
#include "bf_tree.h"
#include "bf_tree_inline.h"

#include "smthread.h"
#include "vid_t.h"
#include "page_s.h"
#include <string.h>
#include "w_findprime.h"
#include <stdlib.h>

#include "sm_int_0.h"
#include "sm_int_1.h"
#include "bf.h"
#include "page.h"
#include "sm_io.h"
#include "vol.h"
#include "alloc_cache.h"

#include <ostream>

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
    bool initially_enable_cleaners,
    bool enable_swizzling) {
    ::memset (this, 0, sizeof(bf_tree_m));

    _block_cnt = block_cnt;
    _enable_swizzling = enable_swizzling;

#ifdef SIMULATE_NO_SWIZZLING
    _enable_swizzling = false;
    enable_swizzling = false;
    DBGOUT0 (<< "THIS MESSAGE MUST NOT APPEAR unless you intended. Completely turned off swizzling in bufferpool.");
#endif // SIMULATE_NO_SWIZZLING

    DBGOUT1 (<< "constructing bufferpool with " << block_cnt << " blocks of " << SM_PAGESIZE << "-bytes pages... enable_swizzling=" << enable_swizzling);
    
    // use posix_memalign to allow unbuffered disk I/O
    void *buf = NULL;
    ::posix_memalign(&buf, SM_PAGESIZE, SM_PAGESIZE * block_cnt);
    if (buf == NULL) {
        ERROUT (<< "failed to reserve " << block_cnt << " blocks of " << SM_PAGESIZE << "-bytes pages. ");
        W_FATAL(smlevel_0::eOUTOFMEMORY);
    }
    _buffer = reinterpret_cast<page_s*>(buf);
    
    // the index 0 is never used. to make sure no one can successfully use it,
    // fill the block-0 with garbages
    ::memset (_buffer, 0x27, sizeof(page_s));
    
    _control_blocks = reinterpret_cast<bf_tree_cb_t*>(new char[sizeof(bf_tree_cb_t) * block_cnt]);
    w_assert0(_control_blocks != NULL);
    ::memset (_control_blocks, 0, sizeof(bf_tree_cb_t) * block_cnt);

#ifdef BP_MAINTAIN_PARNET_PTR
    // swizzled-LRU is initially empty
    _swizzled_lru = new bf_idx[block_cnt * 2];
    w_assert0(_swizzled_lru != NULL);
    ::memset (_swizzled_lru, 0, sizeof(bf_idx) * block_cnt * 2);
    _swizzled_lru_len = 0;
#endif // BP_MAINTAIN_PARNET_PTR

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
        delete[] reinterpret_cast<char*>(_control_blocks);
        _control_blocks = NULL;
    }
#ifdef BP_MAINTAIN_PARNET_PTR
    if (_swizzled_lru != NULL) {
        delete[] _swizzled_lru;
        _swizzled_lru = NULL;
    }
#endif // BP_MAINTAIN_PARNET_PTR
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
    std::vector<snum_t> stores (stcache->get_all_used_store_id());
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
                _control_blocks[idx].clear();
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

    if (_buffer[idx].calculate_checksum() != _buffer[idx].checksum) {
        return RC(smlevel_0::eBADCHECKSUM);
    }
    
    bf_tree_cb_t &cb(_control_blocks[idx]);
    cb.clear();
    cb._pid_vol = vid;
    cb._pid_shpid = shpid;
    // as the page is read from disk, at least it's sure that
    // the page is flushed as of the page LSN (otherwise why we can read it!)
    cb._rec_lsn = _buffer[idx].lsn.data();
    cb._pin_cnt = 1; // root page's pin count is always positive
    cb._swizzled = true;
    cb._used = true; // turn on _used at last
    bool inserted = _hashtable->insert_if_not_exists(bf_key(vid, shpid), idx); // for some type of caller (e.g., redo) we still need hashtable entry for root
    if (!inserted) {
        ERROUT (<<"failed to insert a root page to hashtable. this must not have happened because there shouldn't be any race. wtf");
        return RC(smlevel_0::eINTERNAL);
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
            return RC(smlevel_0::eINTERNAL);
        }
    }
    
    // load all pages. pin them forever
    bf_idx endidx = volume->num_pages();
    for (bf_idx idx = volume->first_data_pageid(); idx < endidx; ++idx) {
        if (volume->is_allocated_page(idx)) {
            W_DO(volume->read_page(idx, _buffer[idx]));
            if (_buffer[idx].calculate_checksum() != _buffer[idx].checksum) {
                return RC(smlevel_0::eBADCHECKSUM);
            }
            bf_tree_cb_t &cb(_control_blocks[idx]);
            cb.clear();
            cb._pid_vol = vid;
            cb._pid_shpid = idx;
            cb._rec_lsn = _buffer[idx].lsn.data();
            cb._pin_cnt = 1;
            cb._used = true;
            cb._swizzled = true;
        }
    }
    // now freelist is inconsistent, but who cares. this is mainmemory-db experiment

    bf_tree_vol_t* desc = new bf_tree_vol_t(volume);
    stnode_cache_t *stcache = volume->get_stnode_cache();
    std::vector<snum_t> stores (stcache->get_all_used_store_id());
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
        bf_tree_cb_t &cb(_control_blocks[idx]);
        if (!cb._used || cb._pid_vol != vid) {
            continue;
        }
#ifdef BP_MAINTAIN_PARNET_PTR
        // if swizzled, remove from the swizzled-page LRU too
        if (_is_in_swizzled_lru(idx)) {
            _remove_from_swizzled_lru(idx);
        }
#endif // BP_MAINTAIN_PARNET_PTR
        _hashtable->remove(bf_key(vid, cb._pid_shpid));
        if (cb._swizzled) {
            --_swizzled_page_count_approximate;
        }
        _control_blocks[idx].clear();
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

w_rc_t bf_tree_m::fix_direct (page_s*& page, volid_t vol, shpid_t shpid, latch_mode_t mode, bool conditional, bool virgin_page) {
    return _fix_nonswizzled(NULL, page, vol, shpid, mode, conditional, virgin_page);
}

w_rc_t bf_tree_m::_fix_nonswizzled_mainmemorydb(page_s* parent, page_s*& page, shpid_t shpid, latch_mode_t mode, bool conditional, bool virgin_page) {
    bf_idx idx = shpid;
    bf_tree_cb_t &cb(_control_blocks[idx]);
#ifdef BP_MAINTAIN_PARNET_PTR
    bf_idx parent_idx = 0;
    if (is_swizzling_enabled()) {
        parent_idx = parent - _buffer;
        w_assert1 (_is_active_idx(parent_idx));
        cb._parent = parent_idx;
    }
#endif // BP_MAINTAIN_PARNET_PTR
    if (virgin_page) {
        cb._rec_lsn = 0;
        cb._dirty = true;
        ++_dirty_page_count_approximate;
    }
    w_rc_t rc = cb._latch.latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER);
    if (rc.is_error()) {
        DBGOUT2(<<"bf_tree_m: latch_acquire failed in buffer frame " << idx << " rc=" << rc);
    } else {
        page = &(_buffer[idx]);
    }
    return rc;
}

w_rc_t bf_tree_m::_fix_nonswizzled(page_s* parent, page_s*& page, volid_t vol, shpid_t shpid, latch_mode_t mode, bool conditional, bool virgin_page) {
    w_assert1(vol != 0);
    w_assert1(shpid != 0);
    w_assert1((shpid & SWIZZLED_PID_BIT) == 0);
    w_assert1(!is_swizzling_enabled() || parent != NULL);
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
            bf_tree_cb_t &cb(_control_blocks[idx]);
            // after here, we must either succeed or release the free block
            if (virgin_page) {
                // except a virgin page. then the page is anyway empty
                DBGOUT3(<<"bf_tree_m: adding a virgin page ("<<vol<<"."<<shpid<<")to bufferpool.");
            } else {
                DBGOUT3(<<"bf_tree_m: cache miss. reading page "<<vol<<"." << shpid << " to frame " << idx);
                w_rc_t read_rc = volume->_volume->read_page(shpid, _buffer[idx]);
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
                        return RC (smlevel_0::eBADCHECKSUM);
                    }
                    // this is actually an error, but some testcases don't bother making real pages, so
                    // we just write out some warning.
                    if (!virgin_page && (_buffer[idx].pid.page != shpid || _buffer[idx].pid.vol().vol != vol)) {
                        ERROUT(<<"WARNING!! bf_tree_m: page id doesn't match! " << vol << "." << shpid << " was " << _buffer[idx].pid.vol().vol << "." << _buffer[idx].pid.page
                            << ". This means an inconsistent disk page unless this message is issued in testcases without real disk pages."
                        );
                    }
                }
            }

            // initialize control block
            // we don't have to atomically pin it because it's not referenced by any other yet
            cb.clear();
            cb._pid_vol = vol;
            cb._pid_shpid = shpid;
#ifdef BP_MAINTAIN_PARNET_PTR
            bf_idx parent_idx = 0;
            if (is_swizzling_enabled()) {
                parent_idx = parent - _buffer;
                w_assert1 (_is_active_idx(parent_idx));
                cb._parent = parent_idx;
            }
#endif // BP_MAINTAIN_PARNET_PTR
            if (!virgin_page) {
                // if the page is read from disk, at least it's sure that
                // the page is flushed as of the page LSN (otherwise why we can read it!)
                cb._rec_lsn = _buffer[idx].lsn.data();
            } else {
                cb._dirty = true;
                ++_dirty_page_count_approximate;
            }
            cb._used = true;

            // latch the page. (not conditional because this thread will be the only thread touching it)
            w_rc_t rc_latch = cb._latch.latch_acquire(mode, sthread_t::WAIT_IMMEDIATE);
            w_assert1(!rc_latch.is_error());
            
            // finally, register the page to the hashtable.
            bool registered = _hashtable->insert_if_not_exists(key, idx);
            if (!registered) {
                // this pid already exists in bufferpool. this means another thread concurrently
                // added the page to bufferpool. unlucky, but can happen.
                DBGOUT1(<<"bf_tree_m: unlucky! another thread already added the page " << shpid << " to the bufferpool. discard my own work on frame " << idx);
                cb._latch.latch_release();
                cb.clear(); // well, it should be enough to clear _used, but this is anyway a rare event. wouldn't hurt to clear all.
                _add_free_block(idx);
                continue;
            }
            
            // okay, all done
#ifdef BP_MAINTAIN_PARNET_PTR
            if (is_swizzling_enabled()) {
		lintel::unsafe::atomic_fetch_add((uint32_t*) &(_control_bl     ocks[parent_idx]._pin_cnt), 1); // we installed a new child of the parent      to this bufferpool. add parent's count
            }
#endif // BP_MAINTAIN_PARNET_PTR
            page = &(_buffer[idx]);

            return RCOK;
        } else {
            // unlike swizzled case, we have to atomically pin it while verifying it's still there.
            bf_tree_cb_t &cb(_control_blocks[idx]);
            int32_t cur_cnt = cb._pin_cnt;
            if (cur_cnt < 0) {
                w_assert1(cur_cnt == -1);
                DBGOUT1(<<"bf_tree_m: very unlucky! buffer frame " << idx << " has been just evicted. retrying..");
                continue;
            }
            int32_t cur_ucnt = cur_cnt;
	    if (lintel::unsafe::atomic_compare_exchange_strong(const_cast<     int32_t*>(&cb._pin_cnt), &cur_ucnt, cur_ucnt + 1))
	    {
                // okay, CAS went through
                ++cb._refbit_approximate;
                w_rc_t rc = cb._latch.latch_acquire(mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER);
                // either successfully or unsuccessfully, we latched the page.
                // we don't need the pin any more.
                // here we can simply use atomic_dec because it must be positive now.
                w_assert1(cb._pin_cnt > 0);
                w_assert1(cb._pid_vol == vol);
                w_assert1(cb._pid_shpid == shpid);
		lintel::unsafe::atomic_fetch_sub((uint32_t*)(&cb._pin_cnt)     ,1);

                if (rc.is_error()) {
                    DBGOUT2(<<"bf_tree_m: latch_acquire failed in buffer frame " << idx << " rc=" << rc);
                } else {
                    page = &(_buffer[idx]);
                }
                return rc;
            } else {
                // another thread is doing something. keep trying.
                DBGOUT1(<<"bf_tree_m: a bit unlucky! buffer frame " << idx << " has contention. cb._pin_cnt=" << cb._pin_cnt <<", expected=" << cur_ucnt);
                continue;
            }
        }
    }
}

bf_idx bf_tree_m::pin_for_refix(const page_s* page) {
    w_assert1(page != NULL);
    w_assert1(latch_mode(page) != LATCH_NL);
    bf_idx idx = page - _buffer;
    w_assert1(_is_active_idx(idx));
#ifdef SIMULATE_MAINMEMORYDB
    if (true) return idx;
#endif // SIMULATE_MAINMEMORYDB
    // this is just atomic increment, not a CAS, because we know
    // the page is latched and eviction thread wouldn't consider this block.
    w_assert1(_control_blocks[idx]._pin_cnt >= 0);
    lintel::unsafe::atomic_fetch_add((uint32_t*) &(_control_blocks[idx]._pin_cnt),1);
    return idx;
}

void bf_tree_m::unpin_for_refix(bf_idx idx) {
    w_assert1(_is_active_idx(idx));
    w_assert1(_control_blocks[idx]._pin_cnt > 0);
#ifdef SIMULATE_MAINMEMORYDB
    if (true) return;
#endif // SIMULATE_MAINMEMORYDB
    //atomic_dec_32((uint32_t*)(&_control_blocks[idx]._pin_cnt));
    lintel::unsafe::atomic_fetch_sub((uint32_t*) &(_control_blocks[idx]._pin_cnt)     ,1);
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

void bf_tree_m::repair_rec_lsn (page_s *page, bool was_dirty, const lsn_t &new_rlsn) {
    if( !smlevel_0::logging_enabled) return;
    
    bf_idx idx = page - _buffer;
    w_assert1 (_is_active_idx(idx));
    
    lsn_t lsn = _buffer[idx].lsn;
    lsn_t rec_lsn (_control_blocks[idx]._rec_lsn);
    if (was_dirty) {
        // never mind!
        w_assert0(rec_lsn <= lsn);
    } else {
        w_assert0(rec_lsn > lsn);
        if(new_rlsn.valid()) {
            w_assert0(new_rlsn <= lsn);
            w_assert2(_control_blocks[idx]._dirty);
            _control_blocks[idx]._rec_lsn = new_rlsn.data();
            INC_TSTAT(restart_repair_rec_lsn);
        } else {
            _control_blocks[idx]._dirty = false;
        }
    }
}

///////////////////////////////////   Dirty Page Cleaner END       ///////////////////////////////////  


///////////////////////////////////   LRU/Freelist BEGIN ///////////////////////////////////  

#ifdef BP_MAINTAIN_PARNET_PTR
void bf_tree_m::_add_to_swizzled_lru(bf_idx idx) {
    w_assert1 (is_swizzling_enabled());
    w_assert1 (_is_active_idx(idx));
    w_assert1 (!_control_blocks[idx]._swizzled);
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
    w_assert1 (_control_blocks[idx]._swizzled);

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
    w_assert1 (_control_blocks[idx]._swizzled);

    _control_blocks[idx]._swizzled = false;
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
#endif // BP_MAINTAIN_PARNET_PTR

w_rc_t bf_tree_m::_grab_free_block(bf_idx& ret) {
#ifdef SIMULATE_MAINMEMORYDB
    if (true) {
        ERROUT (<<"MAINMEMORY-DB. _grab_free_block() shouldn't be called. wtf");
        return RC(smlevel_0::eINTERNAL);
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
                w_assert1 (idx > 0 && idx < _block_cnt);
                w_assert1 (!_control_blocks[idx]._used);
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
        W_DO (_get_replacement_block(ret));
        if (ret != 0) {
            return RCOK;
        }
    }
    return RCOK;
}
w_rc_t bf_tree_m::_get_replacement_block(bf_idx& ret) {
#ifdef SIMULATE_MAINMEMORYDB
    if (true) {
        ERROUT (<<"MAINMEMORY-DB. _get_replacement_block() shouldn't be called. wtf");
        return RC(smlevel_0::eINTERNAL);
    }
#endif // SIMULATE_MAINMEMORYDB
    DBGOUT3(<<"trying to evict some page...");
    uint32_t rounds = 0; // how many times the clock hand looped in this function
    while (true) {
        bf_idx idx = ++_clock_hand;
        if (idx >= _block_cnt) {
            ++rounds;
            DBGOUT1(<<"clock hand looped! rounds=" << rounds);
            _clock_hand = 1;
            idx = 1;
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
                    return RC(smlevel_0::eFRAMENOTFOUND);
                }
            }
            if (rounds >= 2) {
                g_me()->sleep(100);
                DBGOUT1(<<"woke up. now there should be some page to evict");
            }
        }
        w_assert1(idx > 0);

        bf_tree_cb_t &cb(_control_blocks[idx]);
        // do not consider dirty pages (at this point)
        // we check this again later because we don't take locks as of this.
        // we also avoid grabbing unused block because it has to be grabbed via freelist
        if (cb._dirty || !cb._used) {
            continue;
        }

        // do not evict hot page
        if (cb._refbit_approximate > 0) {
            const uint32_t refbit_threshold = 3;
            if (cb._refbit_approximate > refbit_threshold) {
                cb._refbit_approximate /= (rounds >= 5 ? 8 : 2);
            } else {
                cb._refbit_approximate = 0;
            }
            continue;
        }

        // find a block that has no pinning (or not being evicted by others).
        // this check is approximate as it's without lock.
        // false positives are fine, and we do the real check later
        if (cb._pin_cnt != 0) {
            continue;
        }
        
        // if it seems someone latches it, give up.
        if (cb._latch.latch_cnt() != 0) { // again, we check for real later
            continue;
        }
        
        // okay, let's try evicting this page.
        // first, we have to make sure the page's pin_cnt is exactly 0.
        // we atomically change it to -1.
	int zero = 0;
	if (lintel::unsafe::atomic_compare_exchange_strong(const_cast<int32_t     *>(&cb._pin_cnt), (int32_t * ) &zero , (int32_t) -1))
	{
            // CAS did it job. the current thread has an exclusive access to this block
            w_assert1(cb._pin_cnt == -1);
            
            // let's do a real check.
            if (cb._dirty || !cb._used) {
                DBGOUT1(<<"very unlucky, this block has just become dirty.");
                // oops, then put this back and give up this block
                cb._pin_cnt = 0;
                continue;
            }
            
            // check latches too. just conditionally test it to avoid long waits.
            w_rc_t latch_rc = cb._latch.latch_acquire(LATCH_EX, WAIT_IMMEDIATE);
            if (latch_rc.is_error()) {
                DBGOUT1(<<"very unlucky, someone has just latched this block.");
                cb._pin_cnt = 0;
                continue;
            }
            // we can immediately release EX latch because no one will newly take latch as _pin_cnt==-1
            cb._latch.latch_release();
            
            // remove it from hashtable.
            W_IFDEBUG1(bool removed =) _hashtable->remove(bf_key(cb._pid_vol, cb._pid_shpid));
            w_assert1(removed);
#ifdef BP_MAINTAIN_PARNET_PTR
            w_assert1(!_is_in_swizzled_lru(idx));
            if (is_swizzling_enabled()) {
                w_assert1(cb._parent != 0);
                _decrement_pin_cnt_assume_positive(cb._parent);
            }
#endif // BP_MAINTAIN_PARNET_PTR
            ret = idx;
            return RCOK;
        } else {
            // it can happen. we just give up this block
            continue;
        }
    }
    return RCOK;
}

void bf_tree_m::_add_free_block(bf_idx idx) {
    CRITICAL_SECTION(cs, &_freelist_lock);
    ++_freelist_len;
    _freelist[idx] = FREELIST_HEAD;
    FREELIST_HEAD = idx;
#ifdef BP_MAINTAIN_PARNET_PTR
    // if the following fails, you might have forgot to remove it from the LRU before calling this method
    w_assert1(SWIZZLED_LRU_NEXT(idx) == 0);
    w_assert1(SWIZZLED_LRU_PREV(idx) == 0);
#endif // BP_MAINTAIN_PARNET_PTR
}

void bf_tree_m::_delete_block(bf_idx idx) {
    w_assert1(_is_active_idx(idx));
    bf_tree_cb_t &cb(_control_blocks[idx]);
    w_assert1(cb._dirty);
    w_assert1(cb._pin_cnt == 0);
    w_assert1(!cb._latch.is_latched());
    cb._used = false; // clear _used BEFORE _dirty so that eviction thread will ignore this block.
    cb._dirty = false;

    W_IFDEBUG1(bool removed =) _hashtable->remove(bf_key(cb._pid_vol, cb._pid_shpid));
    w_assert1(removed);
#ifdef BP_MAINTAIN_PARNET_PTR
    w_assert1(!_is_in_swizzled_lru(idx));
    if (is_swizzling_enabled()) {
        _decrement_pin_cnt_assume_positive(cb._parent);
    }
#endif // BP_MAINTAIN_PARNET_PTR
    
    // after all, give back this block to the freelist. other threads can see this block from now on
    _add_free_block(idx);
}

///////////////////////////////////   LRU/Freelist END ///////////////////////////////////  

bool bf_tree_m::_increment_pin_cnt_no_assumption(bf_idx idx) {
    w_assert1(idx > 0 && idx < _block_cnt);
    bf_tree_cb_t &cb(_control_blocks[idx]);
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
}

void bf_tree_m::_decrement_pin_cnt_assume_positive(bf_idx idx) {
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb(_control_blocks[idx]);
    w_assert1 (cb._pin_cnt >= 1);
    lintel::unsafe::atomic_fetch_sub((uint32_t*) &(cb._pin_cnt),1);
}

///////////////////////////////////   WRITE-ORDER-DEPENDENCY BEGIN ///////////////////////////////////  
bool bf_tree_m::register_write_order_dependency(const page_s* page, const page_s* dependency) {
    w_assert1(page);
    w_assert1(dependency);
    w_assert1(page->pid != dependency->pid);

    uint32_t idx = page - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb(_control_blocks[idx]);
    w_assert1(cb._latch.held_by_me()); 

    uint32_t dependency_idx = dependency - _buffer;
    w_assert1 (_is_active_idx(dependency_idx));
    bf_tree_cb_t &dependency_cb(_control_blocks[dependency_idx]);
    w_assert1(dependency_cb._latch.held_by_me()); 

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
        bf_tree_cb_t &dependency_cb(_control_blocks[dependency_idx]);
        w_assert1(dependency_cb._pin_cnt >= 0);
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
            bf_tree_cb_t &next_dependency_cb(_control_blocks[next_dependency_idx]);
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
    w_assert1(cb._pin_cnt >= 0);
    w_assert1(cb._dependency_idx != 0);
    w_assert1(cb._dependency_shpid != 0);
    w_assert1(dependency_cb._pin_cnt >= 0);
    return dependency_cb._used && dependency_cb._dirty // it's still dirty
        && dependency_cb._pid_vol == cb._pid_vol // it's still the vol and 
        && dependency_cb._pid_shpid == cb._dependency_shpid // page it was referring..
        && dependency_cb._rec_lsn <= cb._dependency_lsn; // and not flushed after the registration
}
bool bf_tree_m::_check_dependency_still_active(bf_tree_cb_t& cb) {
    w_assert1(cb._pin_cnt >= 0);
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
            still_active = _compare_dependency_lsn(cb, _control_blocks[next_idx]);
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

#ifdef BP_MAINTAIN_PARNET_PTR
void bf_tree_m::switch_parent(page_s* page, page_s* new_parent)
{
    if (!is_swizzling_enabled()) {
        return;
    }
    bf_idx idx = page - _buffer;
    w_assert1(_is_active_idx(idx));
    bf_tree_cb_t &cb(_control_blocks[idx]);

    w_assert1(_is_active_idx(cb._parent));

    bf_idx new_parent_idx = new_parent - _buffer;
    w_assert1(_is_active_idx(new_parent_idx));
    w_assert1(cb._parent != new_parent_idx);
    
    // move the pin_cnt from old to new parent
    _decrement_pin_cnt_assume_positive(cb._parent);
    lintel::unsafe::atomic_fetch_add((uint32_t*) &(_control_blocks[new_par     ent_idx]._pin_cnt),1);
    cb._parent = new_parent_idx;
}
#endif // BP_MAINTAIN_PARNET_PTR


void bf_tree_m::_convert_to_disk_page(page_s* page) const {
    DBGOUT3 (<< "converting the page " << page->pid << "... ");
    
    // if the page is a leaf page, blink is the only pointer
    _convert_to_pageid(&(page->btree_blink));
    w_assert1(page->btree_level >= 1);
    
    //otherwise, we have to check all children
    if (page->btree_level > 1) {
        _convert_to_pageid(&(page->btree_pid0));
        slot_index_t slots = page->nslots;
        // use page_p class just for using tuple_addr().
        page_p p (page);
        for (slot_index_t i = 1; i < slots; ++i) {
            void* addr = p.tuple_addr(i);
            _convert_to_pageid(reinterpret_cast<shpid_t*>(addr));
        }
    }
}

inline void bf_tree_m::_convert_to_pageid (shpid_t* shpid) const {
    if ((*shpid) & SWIZZLED_PID_BIT) {
        bf_idx idx = (*shpid) ^ SWIZZLED_PID_BIT;
        w_assert1(_is_active_idx(idx));
        bf_tree_cb_t &cb(_control_blocks[idx]);
        DBGOUT3 (<< "_convert_to_pageid(): converted a swizzled pointer bf_idx=" << idx << " to page-id=" << cb._pid_shpid);
        *shpid = cb._pid_shpid;
    }
}

slotid_t bf_tree_m::find_page_id_slot(page_s* page, shpid_t shpid) const
{
    w_assert1((shpid & SWIZZLED_PID_BIT) == 0);
    // w_assert1(page->btree_blink != (shpid | SWIZZLED_PID_BIT));
    // if (page->btree_blink == shpid) {
    //     return -1;
    // }
    w_assert1(page->btree_blink != shpid); // don't swizzle foster-child
    if (page->btree_level > 1) {
        if (page->btree_pid0 == shpid) {
            return 0;
        }
        slot_index_t slots = page->nslots;
        page_p p (page);
        for (slot_index_t i = 1; i < slots; ++i) {
            void* addr = p.tuple_addr(i);
            if (*reinterpret_cast<shpid_t*>(addr) == shpid) {
                return i;
            }
        }
    }
    return -2;
}

///////////////////////////////////   SWIZZLE/UNSWIZZLE BEGIN ///////////////////////////////////  

void bf_tree_m::swizzle_child(page_s* parent, slotid_t slot)
{
    return swizzle_children(parent, &slot, 1);
}

void bf_tree_m::swizzle_children(page_s* parent, const slotid_t* slots, uint32_t slots_size)
{
    w_assert1(is_swizzling_enabled());
    w_assert1(parent != NULL);
    w_assert1(latch_mode(parent) != LATCH_NL);
    bf_idx parent_idx = parent - _buffer;
    w_assert1(_is_active_idx(parent_idx));
    w_assert1(is_swizzled(parent)); // swizzling is transitive.

    page_p p (parent);
    for (uint32_t i = 0; i < slots_size; ++i) {
        slotid_t slot = slots[i];
        w_assert1(slot >= 0); // w_assert1(slot >= -1); see below
        w_assert1(slot < parent->nslots);

        // To simplify the tree traversal while unswizzling,
        // we never swizzle foster-child pointers.
        // if (slot == -1) {
        //    if ((parent->btree_blink & SWIZZLED_PID_BIT) == 0) {
        //        _swizzle_child_pointer (parent, &(parent->btree_blink));
        //    }
        //} else
        if (slot == 0) {
            if ((parent->btree_pid0 & SWIZZLED_PID_BIT) == 0) {
                _swizzle_child_pointer (parent, &(parent->btree_pid0));
            }
        } else {
            shpid_t* addr = reinterpret_cast<shpid_t*>(p.tuple_addr(slot));
            if (((*addr) & SWIZZLED_PID_BIT) == 0) {
                _swizzle_child_pointer (parent, addr);
            }
        }
    }
}

inline void bf_tree_m::_swizzle_child_pointer(page_s* parent, shpid_t* pointer_addr)
{
    shpid_t child_shpid = *pointer_addr;
    w_assert1((child_shpid & SWIZZLED_PID_BIT) == 0);
    uint64_t key = bf_key (parent->pid.vol().vol, child_shpid);
    bf_idx idx = _hashtable->lookup(key);
    // so far, we don't swizzle a child page if it's not in bufferpool yet.
    if (idx == 0) {
        DBGOUT1(<< "Unexpected! the child page " << child_shpid << " isn't in bufferpool yet. gave up swizzling it");
        // this is still okay. swizzling is best-effort
        return;
    }
    // to swizzle the child, add a pin on the page.
    // we might fail here in a very unlucky case. still, it's fine.
    bool pinned = _increment_pin_cnt_no_assumption (idx);
    if (!pinned) {
        DBGOUT1(<< "Unlucky! the child page " << child_shpid << " has been just evicted. gave up swizzling it");
        return;
    }
    
    // we keep the pin until we unswizzle it.
    *pointer_addr = idx | SWIZZLED_PID_BIT; // overwrite the pointer in parent page.
    _control_blocks[idx]._swizzled = true;
    ++_swizzled_page_count_approximate;
#ifdef BP_MAINTAIN_PARNET_PTR
    w_assert1(!_is_in_swizzled_lru(idx));
    _add_to_swizzled_lru(idx);
    w_assert1(_is_in_swizzled_lru(idx));
#endif // BP_MAINTAIN_PARNET_PTR
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
#ifdef BP_MAINTAIN_PARNET_PTR
    _unswizzle_with_parent_pointer();
    if (true) return;
#endif // BP_MAINTAIN_PARNET_PTR

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
    if (_buffer[parent_idx].btree_level <= 1) {
        return;
    }
    _unswizzle_traverse_node (unswizzled_frames, vol, store, parent_idx, 2);
}

void bf_tree_m::_unswizzle_traverse_node(
    uint32_t &unswizzled_frames, volid_t vol, snum_t store, bf_idx node_idx,
    uint16_t cur_clockhand_depth) {
    w_assert1(cur_clockhand_depth < MAX_SWIZZLE_CLOCKHAND_DEPTH);
    if (_swizzle_clockhand_current_depth <= cur_clockhand_depth) {
        _swizzle_clockhand_pathway[cur_clockhand_depth] = 0;
        _swizzle_clockhand_current_depth = cur_clockhand_depth + 1;
    }
    uint32_t old = _swizzle_clockhand_pathway[cur_clockhand_depth];
    bf_tree_cb_t &node_cb(_control_blocks[node_idx]);
    if (old >= (uint32_t) _buffer[node_idx].nslots) {
        return;
    }

    // check children
    uint32_t remaining = _buffer[node_idx].nslots - old;
    page_p node_p (_buffer + node_idx);
    for (uint32_t i = 0; i < remaining && unswizzled_frames < UNSWIZZLE_BATCH_SIZE; ++i) {
        uint32_t slot = old + i;
        if (!node_cb._used || _buffer[node_idx].btree_level <= 1) {
            return;
        }

        shpid_t shpid;
        if (slot == 0) {
            shpid = _buffer[node_idx].btree_pid0;
        } else {
            shpid = *reinterpret_cast<shpid_t*>(node_p.tuple_addr(slot));
        }

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
        if (_buffer[node_idx].btree_level >= 3) {
            // child is also an intermediate node
            _unswizzle_traverse_node (unswizzled_frames, vol, store, child_idx, cur_clockhand_depth + 1);
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
    bf_tree_cb_t &parent_cb(_control_blocks[parent_idx]);
    if (!parent_cb._used) {
        return false;
    }
    if (!parent_cb._swizzled) {
        return false;
    }
    
    // now, try a conditional latch on parent page.
    w_rc_t latch_rc = parent_cb._latch.latch_acquire(LATCH_EX, sthread_t::WAIT_IMMEDIATE);
    if (latch_rc.is_error()) {
        DBGOUT2(<<"_unswizzle_a_frame: oops, unlucky. someone is latching this page. skipiing this. rc=" << latch_rc);
        return false;
    }
    latch_auto_release auto_rel(parent_cb._latch); // this automatically releaes the latch.

    if (child_slot >= (uint32_t) _buffer[parent_idx].nslots) {
        return false;
    }
    page_p parent (_buffer + parent_idx);
    shpid_t* shpid_addr;
    if (child_slot == 0) {
        shpid_addr = &(_buffer[parent_idx].btree_pid0);
    } else {
        shpid_addr = reinterpret_cast<shpid_t*>(parent.tuple_addr(child_slot));
    }
    shpid_t shpid = *shpid_addr;
    if ((shpid & SWIZZLED_PID_BIT) == 0) {
        return false;
    }
    bf_idx child_idx = shpid ^ SWIZZLED_PID_BIT;
    bf_tree_cb_t &child_cb(_control_blocks[child_idx]);
    w_assert1(child_cb._used);
    w_assert1(child_cb._swizzled);
    // in some lazy testcases, _buffer[child_idx] aren't initialized. so these checks are disabled.
    // see the above comments on cache miss
    // w_assert1(child_cb._pid_shpid == _buffer[child_idx].pid.page);
    // w_assert1(_buffer[child_idx].btree_level == 1);
    w_assert1(child_cb._pid_vol == parent_cb._pid_vol);
    w_assert1(child_cb._pin_cnt >= 1); // because it's swizzled
    w_assert1(child_idx == _hashtable->lookup(bf_key (child_cb._pid_vol, child_cb._pid_shpid)));
    child_cb._swizzled = false;
    // because it was swizzled, the current pin count is >= 1, so we can simply do atomic decrement.
    _decrement_pin_cnt_assume_positive(child_idx);
    --_swizzled_page_count_approximate;

    *shpid_addr = child_cb._pid_shpid;
    w_assert1(((*shpid_addr) & SWIZZLED_PID_BIT) == 0);
    
    return true;
}

#ifdef BP_MAINTAIN_PARNET_PTR
void bf_tree_m::_unswizzle_with_parent_pointer() {
}
#endif // BP_MAINTAIN_PARNET_PTR

///////////////////////////////////   SWIZZLE/UNSWIZZLE END ///////////////////////////////////  

void bf_tree_m::debug_dump(std::ostream &o) const
{
    o << "dumping the bufferpool contents. _block_cnt=" << _block_cnt << ", _clock_hand=" << _clock_hand << std::endl;
    o << "  _freelist_len=" << _freelist_len << ", HEAD=" << FREELIST_HEAD << std::endl;
#ifdef BP_MAINTAIN_PARNET_PTR
    o << "  _swizzled_lru_len=" << _swizzled_lru_len << ", HEAD=" << SWIZZLED_LRU_HEAD << ", TAIL=" << SWIZZLED_LRU_TAIL << std::endl;
#endif // BP_MAINTAIN_PARNET_PTR
    
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
        bf_tree_cb_t &cb(_control_blocks[idx]);
        if (cb._used) {
            o << "page-" << cb._pid_vol << "." << cb._pid_shpid;
            if (cb._dirty) {
                o << " (dirty)";
            }
#ifdef BP_MAINTAIN_PARNET_PTR
            o << ", _parent=" << cb._parent;
#endif // BP_MAINTAIN_PARNET_PTR
            o << ", _swizzled=" << cb._swizzled;
            o << ", _pin_cnt=" << cb._pin_cnt;
            o << ", _rec_lsn=" << cb._rec_lsn;
            o << ", _dependency_idx=" << cb._dependency_idx;
            o << ", _dependency_shpid=" << cb._dependency_shpid;
            o << ", _dependency_lsn=" << cb._dependency_lsn;
            o << ", _refbit_approximate=" << cb._refbit_approximate;
#ifdef BP_MAINTAIN_PARNET_PTR
            o << ", _counter_approximate=" << cb._counter_approximate;
            if (_is_in_swizzled_lru(idx)) {
                o << ", swizzled_lru.prev=" << SWIZZLED_LRU_PREV(idx) << ".next=" << SWIZZLED_LRU_NEXT(idx);
            }
#endif // BP_MAINTAIN_PARNET_PTR
            o << ", ";
            cb._latch.print(o);
        } else {
            o << "unused (next_free=" << _freelist[idx] << ")";
        }
        o << std::endl;
    }
    if (_block_cnt >= 1000) {
        o << "  ..." << std::endl;
    }
}

void bf_tree_m::debug_dump_page_pointers(std::ostream& o, page_s* page) const
{
    bf_idx idx = page - _buffer;
    w_assert1(idx > 0);
    w_assert1(idx < _block_cnt);
    o << "dumping page:" << page->pid << ", bf_idx=" << idx << std::endl;
    o << "  blink=";
    debug_dump_pointer (o, page->btree_blink);
    o << std::endl;

    if (page->btree_level > 1) {
        slot_index_t slots = page->nslots;
        page_p p (page);
        o << "  ";
        for (slot_index_t i = 0; i < slots; ++i) {
            o << "child[" << i << "]=";
            if (i == 0) {
                debug_dump_pointer(o, page->btree_pid0);
            } else {
                void* addr = p.tuple_addr(i);
                debug_dump_pointer(o, *reinterpret_cast<shpid_t*>(addr));
            }
            o << ", ";
        }
        o << std::endl;
    }
}
void bf_tree_m::debug_dump_pointer(ostream& o, shpid_t shpid) const
{
    if (shpid & SWIZZLED_PID_BIT) {
        bf_idx idx = shpid ^ SWIZZLED_PID_BIT;
        o << "swizzled(bf_idx=" << idx;
        o << ", page=" << _control_blocks[idx]._pid_shpid << ")";
    } else {
        o << "normal(page=" << shpid << ")";
    }
}

shpid_t bf_tree_m::debug_get_original_pageid (shpid_t shpid) const {
    if (is_swizzled_pointer(shpid)) {
        bf_idx idx = shpid ^ SWIZZLED_PID_BIT;
        return _control_blocks[idx]._pid_shpid;
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
    ::memset (_control_blocks, 0, sizeof(bf_tree_cb_t) * _block_cnt);
#ifdef BP_MAINTAIN_PARNET_PTR
    ::memset (_swizzled_lru, 0, sizeof(bf_idx) * _block_cnt * 2);
    _swizzled_lru_len = 0;
#endif // BP_MAINTAIN_PARNET_PTR
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
        bf_tree_cb_t &cb(_control_blocks[start]);
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
