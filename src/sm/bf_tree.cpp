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
#include "xct_logger.h"

// Template definitions
#include "bf_hashtable.cpp"

thread_local unsigned bf_tree_m::_fix_cnt = 0;
thread_local unsigned bf_tree_m::_hit_cnt = 0;
thread_local SprIterator bf_tree_m::_localSprIter;

// lots of help from Wikipedia here!
int64_t w_findprime(int64_t min)
{
    // the first 25 primes
    static char const prime_start[] = {
    // skip 2,3,5 because our mod60 test takes care of them for us
    /*2, 3, 5,*/ 7, 11, 13, 17, 19, 23, 29, 31, 37, 41,
    43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97
    };
    // x%60 isn't on this list, x is divisible by 2, 3 or 5. If it
    // *is* on the list it still might not be prime
    static char const sieve_start[] = {
    // same as the start list, but adds 1,49 and removes 3,5
    1, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 49, 53, 59
    };

    // use the starts to populate our data structures
    std::vector<int64_t> primes(prime_start, prime_start+sizeof(prime_start));
    char sieve[60];
    memset(sieve, 0, sizeof(sieve));

    for(uint64_t i=0; i < sizeof(sieve_start); i++)
    sieve[int64_t(sieve_start[i])] = 1;

    /* We aren't interested in 4000 digit numbers here, so a Sieve of
       Erastothenes will work just fine, especially since we're
       seeding it with the first 25 primes and avoiding the (many)
       numbers that divide by 2,3 or 5.
     */
    for(int64_t x=primes.back()+1; primes.back() < min; x++) {
    if(!sieve[x%60])
        continue; // divides by 2, 3 or 5

    bool prime = true;
    for(int64_t i=0; prime && primes[i]*primes[i] <= x; i++)
        prime = (x%primes[i]) > 0;

    if(prime)
        primes.push_back(x);
    }

    return primes.back();
}

///////////////////////////////////   Initialization and Release BEGIN ///////////////////////////////////

bf_tree_m::bf_tree_m(const sm_options& options)
    : _cleaner(nullptr)
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

    _write_elision = options.get_bool_option("sm_write_elision", false);

    // No-DB options
    _no_db_mode = options.get_bool_option("sm_no_db", false);
    _batch_segment_size = options.get_int_option("sm_batch_segment_size", 1);
    _batch_warmup = _batch_segment_size > 1;

    _log_fetches = options.get_bool_option("sm_log_page_fetches", false);

    _media_failure_pid = 0;

    // Warm-up options
    // Warm-up hit ratio must be an int between 0 and 100
    auto warmup_hr = options.get_int_option("sm_bf_warmup_hit_ratio", 100);
    if (warmup_hr < 0) { _warmup_hit_ratio = 0; }
    else if (warmup_hr > 100) { _warmup_hit_ratio = 1; }
    else { _warmup_hit_ratio = static_cast<double>(warmup_hr) / 100; }
    // CS TODO min_fixes should be proportional to buffer size somehow
    _warmup_min_fixes = options.get_int_option("sm_bf_warmup_min_fixes", 1000000);
    _warmup_done = false;

    _root_pages.fill(0);

    _block_cnt = nbufpages;
    _enable_swizzling = bufferpool_swizzle;
    _maintain_emlsn = options.get_bool_option("sm_bf_maintain_emlsn", false);
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
    // CS: no need to memset control blocks with zero since we loop over them below to
    // initialize (btw, setting pin count to -1 and used to false is enough)
    // ::memset (buf, 0, (sizeof(bf_tree_cb_t) + sizeof(latch_t)) * (((uint64_t)
    //                 nbufpages) + 1LLU));
    _control_blocks = reinterpret_cast<bf_tree_cb_t*>(reinterpret_cast<char
            *>(buf) + sizeof(bf_tree_cb_t));
    w_assert0(_control_blocks != NULL);

    /*
     * CS: comment copied from old code
     * [...] the bufferpool should alternate location of latches and control blocks
     * starting at an odd multiple of 64B as follows: |CB0|L0|L1|CB1|CB2|L2|L3|CB3|...
     * This layout addresses a pathology that we attribute to the hardware spatial prefetcher.
     * The default layout allocates a latch right after a control block so that
     * the control block and latch live in adjacent cache lines (in the same 128B sector).
     * The pathology happens because when we write-access the latch, the processor prefetches
     * the control block in read-exclusive mode even if we late really only read-access the
     * control block. This causes unnecessary coherence traffic. With the new layout, we avoid
     * having a control block and latch in the same 128B sector.
     */
    for (bf_idx i = 0; i < nbufpages; i++) {
        BOOST_STATIC_ASSERT(sizeof(bf_tree_cb_t) < SCHAR_MAX);

        auto& cb = get_cb(i);
        cb._pin_cnt = -1;
        cb._used = false;

        if (i & 0x1) { /* odd */
            cb._latch_offset = -static_cast<int8_t>(sizeof(bf_tree_cb_t)); // place the latch before the control block
        } else { /* even */
            cb._latch_offset = sizeof(bf_tree_cb_t); // place the latch after the control block
        }

        cb.clear_latch();
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

    _cleaner_decoupled = options.get_bool_option("sm_cleaner_decoupled", false);

    _instant_restore = options.get_bool_option("sm_restore_instant", true);

    _evictioner = std::make_shared<page_evictioner_base>(this, options);
    _async_eviction = options.get_bool_option("sm_async_eviction", false);
    if (_async_eviction) { _evictioner->fork(); }
}

void bf_tree_m::shutdown()
{
    // Order in which threads are destroyed is very important!
    if (_background_restorer) {
        _background_restorer->stop();
        _background_restorer = nullptr;
    }

    if(_async_eviction) {
        _evictioner->stop();
        _evictioner = nullptr;
    }

    if (_cleaner) {
        _cleaner->stop();
        _cleaner = nullptr;
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
}

std::shared_ptr<page_cleaner_base> bf_tree_m::get_cleaner()
{
    return _cleaner;
}

///////////////////////////////////   Initialization and Release END ///////////////////////////////////

bf_idx bf_tree_m::lookup_parent(PageID pid) const
{
    bf_idx idx = 0;
    bf_idx_pair p;
    if (_hashtable->lookup(pid, p)) {
        idx = p.second;
    }
    return idx;
}

bf_idx bf_tree_m::lookup(PageID pid) const
{
    bf_idx idx = 0;
    bf_idx_pair p;
    if (_hashtable->lookup(pid, p)) {
        idx = p.first;
    }
    return idx;
}

w_rc_t bf_tree_m::_grab_free_block(bf_idx& ret)
{
    ret = 0;

    using namespace std::chrono;
    auto time1 = steady_clock::now();

    while (true) {
        // once the bufferpool becomes full, getting _freelist_lock everytime will be
        // too costly. so, we check _freelist_len without lock first.
        //   false positive : fine. we do real check with locks in it
        //   false negative : fine. we will eventually get some free block anyways.
        if (_freelist_len > 0) {
            CRITICAL_SECTION(cs, &_freelist_lock);
            if (_freelist_len > 0) { // here, we do the real check
                bf_idx idx = FREELIST_HEAD;
                DBG5(<< "Grabbing idx " << idx);
                w_assert1(_is_valid_idx(idx));
                w_assert1 (!get_cb(idx)._used);
                w_assert1 (get_cb(idx)._pin_cnt < 0);
                ret = idx;

                --_freelist_len;
                if (_freelist_len == 0) {
                    FREELIST_HEAD = 0;
                } else {
                    FREELIST_HEAD = _freelist[idx];
                    w_assert1 (FREELIST_HEAD > 0 && FREELIST_HEAD < _block_cnt);
                }
                DBG5(<< "New head " << FREELIST_HEAD);
                w_assert1(ret != FREELIST_HEAD);
                break;
            }
        } // exit the scope to do the following out of the critical section

        // no free frames -> warmup is done
        set_warmup_done();

        // no more free pages -- invoke eviction
        if (_async_eviction) {
            // this will block until we get a notification that a frame was evicted
            _evictioner->wakeup(true);
        }
        else {
            bool success = false;
            bf_idx victim = 0;
            while (!success) {
                victim = _evictioner->pick_victim();
                w_assert0(victim > 0);
                success = _evictioner->evict_one(victim);
            }
            ret = victim;
            break;
        }
    }

    auto time2 = steady_clock::now();
    ADD_TSTAT(bf_evict_duration, duration_cast<nanoseconds>(time2-time1).count());

    return RCOK;
}

void bf_tree_m::set_warmup_done()
{
    // CS: no CC needed, threads can race on blind updates and visibility not an issue
    if (!_warmup_done) {
        _warmup_done = true;
        _restore_coord = nullptr;
        Logger::log_sys<warmup_done_log>();

        // Start backgroud recovery after warmup, in order to not interfere
        // with on-demand recovery.
        if (smlevel_0::recovery && smlevel_0::recovery->isInstant()) {
            smlevel_0::recovery->wakeup();
        }
    }
}

void bf_tree_m::check_warmup_done()
{
    // if warm-up hit ratio is 100%, we don't even bother
    if (!_warmup_done && _warmup_hit_ratio < 1.0 && _fix_cnt > _warmup_min_fixes) {
        double hit_ratio = static_cast<double>(_hit_cnt)/_fix_cnt;
        if (hit_ratio > _warmup_hit_ratio) {
            set_warmup_done();
        }
    }
}

void bf_tree_m::_add_free_block(bf_idx idx)
{
    CRITICAL_SECTION(cs, &_freelist_lock);
    w_assert1(idx != FREELIST_HEAD);
    w_assert1(!get_cb(idx)._used);
    w_assert1(get_cb(idx)._pin_cnt < 0);
    ++_freelist_len;
    _freelist[idx] = FREELIST_HEAD;
    FREELIST_HEAD = idx;
}

void bf_tree_m::post_init()
{
    if (_no_db_mode && _batch_warmup) {
        constexpr bool virgin_pages = true;
        auto vol_pages = smlevel_0::vol->num_used_pages();
        auto segcount = vol_pages  / _batch_segment_size
            + (vol_pages % _batch_segment_size ? 1 : 0);
        _restore_coord = std::make_shared<RestoreCoord> (_batch_segment_size,
                segcount, SegmentRestorer::bf_restore, virgin_pages);
    }

    if(_cleaner_decoupled) {
        w_assert0(smlevel_0::logArchiver);
        _cleaner = std::make_shared<page_cleaner_decoupled>(ss_m::get_options());
    }
    else{
        _cleaner = std::make_shared<bf_tree_cleaner>(ss_m::get_options());
    }
    _cleaner->fork();
}

void bf_tree_m::recover_if_needed(bf_tree_cb_t& cb, generic_page* page, bool only_if_dirty)
{
    if (!cb._check_recovery || !smlevel_0::recovery) { return; }

    w_assert1(cb.latch().is_mine());
    w_assert1(cb.get_page_lsn() == page->lsn);

    auto pid = cb._pid;
    auto expected_lsn = smlevel_0::recovery->get_dirty_page_emlsn(pid);
    if (!only_if_dirty || (!expected_lsn.is_null() && page->lsn < expected_lsn)) {
        btree_page_h p;
        p.fix_nonbufferpool_page(page);
        constexpr bool use_archive = true;
        // CS TODO: this is required to replay a btree_split correctly
        page->pid = pid;
        _localSprIter.open(pid, page->lsn, expected_lsn, use_archive);
        _localSprIter.apply(p);
        w_assert0(page->lsn >= expected_lsn);
    }

    w_assert0(page->pid == pid);
    w_assert0(cb._pid == pid);
    w_assert0(page->lsn > lsn_t::null);
    cb.set_check_recovery(false);

    if (_log_fetches) {
        Logger::log_sys<fetch_page_log>(pid, page->lsn, page->store);
    }
}

void bf_tree_m::set_media_failure()
{
    w_assert0(smlevel_0::logArchiver);

    auto vol_pages = smlevel_0::vol->num_used_pages();

    constexpr bool virgin_pages = false;
    constexpr bool start_locked = true;
    auto segcount = vol_pages  / _batch_segment_size
        + (vol_pages % _batch_segment_size ? 1 : 0);
    _restore_coord = std::make_shared<RestoreCoord> (_batch_segment_size,
            segcount, SegmentRestorer::bf_restore, virgin_pages,
            _instant_restore, start_locked);

    smlevel_0::vol->open_backup();
    lsn_t backup_lsn = smlevel_0::vol->get_backup_lsn();

    _media_failure_pid = vol_pages;

    // Make sure log is archived until failureLSN
    lsn_t failure_lsn = Logger::log_sys<restore_begin_log>(vol_pages);
    ERROUT(<< "Media failure injected! Waiting for log archiver to reach LSN "
            << failure_lsn);
    stopwatch_t timer;
    smlevel_0::logArchiver->archiveUntilLSN(failure_lsn);
    ERROUT(<< "Failure LSN reached in " << timer.time() << " seconds");

    _restore_coord->set_lsns(backup_lsn, failure_lsn);
    _restore_coord->start();

    _background_restorer = std::make_shared<BgRestorer>
        (_restore_coord, [this] { unset_media_failure(); });
    _background_restorer->fork();
    _background_restorer->wakeup();
}

void bf_tree_m::unset_media_failure()
{
    _media_failure_pid = 0;
    // Background restorer cannot be destroyed here because it is the caller
    // of this method via a callback. For now, well just let it linger as a
    // "zombie" thread
    // _background_restorer = nullptr;
    Logger::log_sys<restore_end_log>();
    smlevel_0::vol->close_backup();
    ERROUT(<< "Restore done!");
    _restore_coord = nullptr;
}

void bf_tree_m::notify_archived_lsn(lsn_t archived_lsn)
{
    if (_cleaner) {
        _cleaner->notify_archived_lsn(archived_lsn);
    }
}

///////////////////////////////////   Page fix/unfix BEGIN         ///////////////////////////////////
// NOTE most of the page fix/unfix functions are in bf_tree_inline.h.
// These functions are here are because called less frequently.

w_rc_t bf_tree_m::fix(generic_page* parent, generic_page*& page,
                                   PageID pid, latch_mode_t mode,
                                   bool conditional, bool virgin_page,
                                   bool only_if_hit, bool do_recovery,
                                   lsn_t emlsn)
{
    stopwatch_t timer;

    if (is_swizzled_pointer(pid)) {
        w_assert1(!virgin_page);
        // Swizzled pointer traversal only valid with latch coupling (i.e.,
        // parent must also have been fixed)
        w_assert1(parent);

        bf_idx idx = pid ^ SWIZZLED_PID_BIT;
        w_assert1(_is_valid_idx(idx));
        bf_tree_cb_t &cb = get_cb(idx);

        W_DO(cb.latch().latch_acquire(mode,
                    conditional ? timeout_t::WAIT_IMMEDIATE : timeout_t::WAIT_FOREVER));

        // CS: Normally, we must always check if cb is still valid after
        // latching, because page might have been evicted while we were waiting
        // for the latch. In the case of following a swizzled pointer, however,
        // that is not necessary because of latch coupling: the thread calling
        // fix here *must* have the parent latched in shared mode. Eviction, on
        // the other hand, will only select a victim if it can acquire an
        // exclusive latch on its parent. Thus, the mere fact that we are
        // following a swizzled pointer already gives us the guarantee that the
        // control block cannot be invalidated.

        w_assert1(cb.is_in_use());
        w_assert1(cb._swizzled);
        w_assert1(cb._pid == _buffer[idx].pid);

        cb.inc_ref_count();
        _evictioner->ref(idx);
        if (mode == LATCH_EX) { cb.inc_ref_count_ex(); }

        page = &(_buffer[idx]);

        INC_TSTAT(bf_fix_cnt);
        INC_TSTAT(bf_hit_cnt);
        _fix_cnt++;
        _hit_cnt++;

        return RCOK;
    }

    // Wait for log replay before attempting to fix anything
    //->  nodb mode only! (for instant restore see below)
    bool nodb_used_restore = false;
    if (_no_db_mode && do_recovery && !virgin_page && _restore_coord && !_warmup_done)
    {
        // copy into local variable to avoid race condition with setting member to null
        timer.reset();
        auto restore = _restore_coord;
        if (restore) {
            restore->fetch(pid);
            nodb_used_restore = true;
        }
        ADD_TSTAT(bf_batch_wait_time, timer.time_us());
    }

    while (true)
    {
        bf_idx_pair p;
        bf_idx idx = 0;
        if (_hashtable->lookup(pid, p)) {
            idx = p.first;
            if (parent && p.second != parent - _buffer) {
                // need to update parent pointer
                p.second = parent - _buffer;
                _hashtable->update(pid, p);
                INC_TSTAT(bf_fix_adjusted_parent);
            }
        }

        // The result of calling this function determines if we'll behave
        // in normal mode or in failure mode below, and it does that in an
        // atomic way, i.e., we can't switch from one mode to another in
        // the middle of a fix. Note that we can operate in normal mode
        // even if a failure occurred and we missed it here, because vol_t
        // still operates normally even during the failure (remember, we
        // just simulate a media failure)
        bool media_failure = is_media_failure(pid);

        if (idx == 0)
        {
            if (only_if_hit) {
                return RC(stINUSE);
            }

            // Wait for instant restore to restore this segment
            if (do_recovery && !virgin_page && media_failure)
            {
                // copy into local variable to avoid race condition with setting member to null
                timer.reset();
                auto restore = _restore_coord;
                if (restore) { restore->fetch(pid); }
                ADD_TSTAT(bf_batch_wait_time, timer.time_us());
            }

            // STEP 1) Grab a free frame to read into
            W_DO(_grab_free_block(idx));
            bf_tree_cb_t &cb = get_cb(idx);

            // STEP 2) Acquire EX latch before hash table insert, to make sure
            // nobody will access this page until we're done
            w_rc_t check_rc = cb.latch().latch_acquire(LATCH_EX,
                    timeout_t::WAIT_IMMEDIATE);
            if (check_rc.is_error())
            {
                _add_free_block(idx);
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

            w_assert1(idx != parent_idx);
            // Read page from disk
            page = &_buffer[idx];

            if (!virgin_page && !_no_db_mode) {
                INC_TSTAT(bf_fix_nonroot_miss_count);

                if (parent && emlsn.is_null() && _maintain_emlsn) {
                    // Get emlsn from parent
                    general_recordid_t recordid = find_page_id_slot(parent, pid);
                    btree_page_h parent_h;
                    parent_h.fix_nonbufferpool_page(parent);
                    emlsn = parent_h.get_emlsn_general(recordid);
                }

                bool from_backup = media_failure && !do_recovery;
                W_DO(_read_page(pid, cb, from_backup));
                cb.init(pid, page->lsn);
                if (from_backup) { cb.pin_for_restore(); }
            }
            else {
                // initialize contents of virgin page
                cb.init(pid, lsn_t::null);
                ::memset(page, 0, sizeof(generic_page));
                page->pid = pid;

                // Only way I could think of to destroy background restorer
                static std::atomic<bool> i_shall_destroy {false};
                if (!is_media_failure() && !_restore_coord && _background_restorer) {
                    bool expected = false;
                    if (i_shall_destroy.compare_exchange_strong(expected, true)) {
                        _background_restorer->join();
                        _background_restorer = nullptr;
                    }
                }
            }

            // When a page is first fetched from storage, we always check
            // if recovery is needed (we might not recover it right now,
            // because do_recovery might be false, i.e., due to bulk fetch
            // with GenericPageIterator or when prefetching pages).
            cb.set_check_recovery(true);

            w_assert1(_is_active_idx(idx));
            w_assert1(cb.latch().is_mine());
            DBG(<< "Fixed page " << pid << " (miss) to frame " << idx);
        }
        else
        {
            INC_TSTAT(bf_hit_cnt);
            _hit_cnt++;

            // Page index is registered in hash table
            bf_tree_cb_t &cb = get_cb(idx);

            // Wait for instant restore to restore this segment
            if (do_recovery && cb.is_pinned_for_restore())
            {
                timer.reset();
                auto restore = _restore_coord;
                if (restore) { restore->fetch(pid); }
                ADD_TSTAT(bf_batch_wait_time, timer.time_us());
            }

            // Grab latch in the mode requested by user (or in EX if we might
            // have to recover this page)
            latch_mode_t temp_mode = cb._check_recovery ? LATCH_EX : mode;
            W_DO(cb.latch().latch_acquire(temp_mode, conditional ?
                        timeout_t::WAIT_IMMEDIATE : timeout_t::WAIT_FOREVER));

            // Checks below must be performed again, because CB state may have changed
            // while we were waiting for the latch
            bool check_recovery_changed = cb._check_recovery && temp_mode == LATCH_SH;
            bool wait_for_restore = cb.is_pinned_for_restore() && do_recovery;
            bool page_was_evicted = !cb.is_in_use() || cb._pid != pid;
            if (page_was_evicted || check_recovery_changed || wait_for_restore)
            {
                cb.latch().latch_release();
                continue;
            }

            w_assert1(_is_active_idx(idx));
            cb.inc_ref_count();
            if (mode == LATCH_EX) {
                cb.inc_ref_count_ex();
            }

            page = &(_buffer[idx]);

            w_assert1(cb.latch().held_by_me());
            w_assert1(!do_recovery || !cb.is_pinned_for_restore());
            w_assert1(!cb._check_recovery || cb.latch().is_mine());
            DBG(<< "Fixed page " << pid << " (hit) to frame " << idx);
        }

        _evictioner->ref(idx);
        INC_TSTAT(bf_fix_cnt);
        _fix_cnt++;

        check_warmup_done();

        auto& cb = get_cb(idx);
        // w_assert1(cb._pid == pid);
        // w_assert1(page->pid == pid);
        // w_assert1(page->lsn == cb.get_page_lsn());

        if (do_recovery) {
            const bool only_if_dirty = !nodb_used_restore;
            if (virgin_page) { cb.set_check_recovery(false); }
            else { recover_if_needed(cb, page, only_if_dirty); }
        }
        w_assert1(cb._pin_cnt >= 0);

        // downgrade latch if necessary
        if (cb.latch().mode() != mode) {
            w_assert1(mode == LATCH_SH && cb.latch().mode() == LATCH_EX);
            cb.latch().downgrade();
        }

        if (!cb._swizzled && _enable_swizzling && parent) {
            if (!is_swizzled(parent)) { return RCOK; }
            w_assert1(!cb._check_recovery);
            w_assert1(!get_cb(parent - _buffer)._check_recovery);

            // Get slot on parent page
            w_assert1(_is_active_idx(parent - _buffer));
            w_assert1(latch_mode(parent) != LATCH_NL);
            fixable_page_h p;
            p.fix_nonbufferpool_page(parent);
            general_recordid_t slot = find_page_id_slot (parent, pid);

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
            w_assert1(_is_active_idx(idx));

#if W_DEBUG_LEVEL > 0
            PageID swizzled_pid = idx | SWIZZLED_PID_BIT;
            general_recordid_t child_slotid = find_page_id_slot(parent, swizzled_pid);
            w_assert1(child_slotid != GeneralRecordIds::INVALID);
#endif
        }

        return RCOK;
    }
}

rc_t bf_tree_m::_read_page(PageID pid, bf_tree_cb_t& cb, bool from_backup)
{
    w_assert1(cb.latch().is_mine());
    auto page = get_page(&cb);

    rc_t rc = RCOK;
    if (from_backup) { smlevel_0::vol->read_backup(pid, 1, page); }
    else { rc = smlevel_0::vol->read_page(pid, page); }
    if (rc.is_error()) {
        _hashtable->remove(pid);
        cb.latch().latch_release();
        _add_free_block(get_idx(&cb));
    }
    return rc;
}

void bf_tree_m::fuzzy_checkpoint(chkpt_t& chkpt) const
{
    if (_no_db_mode) { return; }

    for (size_t i = 1; i < _block_cnt; i++) {
        auto& cb = get_cb(i);
        /*
         * CS: We don't latch or pin because a fuzzy checkpoint doesn't care
         * about false positives (i.e., pages marked dirty that are actually
         * clean).  Thus, if any of the cb variables changes inbetween, the
         * fuzzy checkpoint is still correct, because LSN updates are atomic
         * and monotonically increasing.
         */
        if (cb.is_in_use() && cb.is_dirty()) {
            // There's a small time window after page_lsn is updated for the first
            // time and before rec_lsn is set, where is_dirty() returns true but
            // rec_lsn is still null. In that case, we can use the page_lsn instead,
            // since it is what rec_lsn will be eventually set to.
            auto rec = cb.get_rec_lsn();
            if (rec.is_null()) { rec = cb.get_page_lsn(); }
            chkpt.mark_page_dirty(cb._pid, cb.get_page_lsn(), rec);
        }
    }
}

void bf_tree_m::prefetch_pages(PageID first, unsigned count)
{
    static thread_local std::vector<generic_page*> frames;
    frames.resize(count);

    // First grab enough free frames to read into
    for (unsigned i = 0; i < count; i++) {
        bf_idx idx;
        while (true) {
            W_COERCE(_grab_free_block(idx));
            auto& cb = get_cb(idx);
            w_rc_t check_rc = cb.latch().latch_acquire(LATCH_EX,
                    timeout_t::WAIT_IMMEDIATE);
            if (check_rc.is_error()) { _add_free_block(idx); }
            else { break; }
        }
        frames[i] = get_page(idx);
    }

    bool media_failure = is_media_failure();

    // Then read into them using iovec
    smlevel_0::vol->read_vector(first, count, frames, media_failure);

    // Finally, add the frames to the hash table if not already there and
    // initialize the control blocks
    for (unsigned i = 0; i < count; i++) {
        PageID pid = first + i;
        auto& cb = *get_cb(frames[i]);
        auto idx = get_idx(&cb);

        constexpr bf_idx parent_idx = 0;
        bool registered = _hashtable->insert_if_not_exists(pid,
                bf_idx_pair(idx, parent_idx));

        if (registered) {
            cb.init(pid, frames[i]->lsn);
            // cb.set_check_recovery(true);

            if (media_failure) { cb.pin_for_restore(); }
        }
        else { _add_free_block(idx); }

        cb.latch().latch_release();
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

    bool pinned = get_cb(idx).pin();
    w_assert0(pinned);
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
    // CS Update: yes it can be evicted, in adoption for example, where we
    // don't hold latch on the foster child
    // w_assert0(found);
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
    auto& child_cb = get_cb(child_idx);

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
    Logger::log_p<update_emlsn_log>(&parent, child_slotid, child_emlsn);
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
                timeout_t::WAIT_IMMEDIATE : timeout_t::WAIT_FOREVER));
    w_assert1(cb._pin_cnt > 0);
    // cb.pin();
    DBG(<< "Refix direct of " << idx << " set pin cnt to " << cb._pin_cnt);
    cb.inc_ref_count();
    _evictioner->ref(idx);
    if (mode == LATCH_EX) { cb.inc_ref_count_ex(); }
    page = &(_buffer[idx]);
    return RCOK;
}

w_rc_t bf_tree_m::fix_nonroot(generic_page*& page, generic_page *parent,
                                     PageID pid, latch_mode_t mode, bool conditional,
                                     bool virgin_page, bool only_if_hit, bool do_recovery,
                                     lsn_t emlsn)
{
    INC_TSTAT(bf_fix_nonroot_count);
    return fix(parent, page, pid, mode, conditional, virgin_page, only_if_hit,
            do_recovery, emlsn);
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

        w_assert1(!get_cb(idx)._check_recovery);

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
                    mode, conditional ? timeout_t::WAIT_IMMEDIATE : timeout_t::WAIT_FOREVER));
        page = &(_buffer[idx]);
    }

    w_assert1(_is_valid_idx(idx));
    w_assert1(_is_active_idx(idx));
    w_assert1(get_cb(idx)._used);
    // w_assert1(!get_cb(idx)._check_recovery); // assertion fails with instant restore!
    w_assert1(get_cb(idx)._pin_cnt >= 0);
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
        if (!cb.prepare_for_eviction()) {
            return;
        }
        w_assert0(cb.latch().is_mine());
        bool removed = _hashtable->remove(p->pid);
        w_assert1(removed);

        _add_free_block(idx);
    }
    else {
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

bool bf_tree_m::has_dirty_frames() const
{
    if (_no_db_mode) { return false; }

    for (bf_idx i = 1; i < _block_cnt; i++) {
        auto& cb = get_cb(i);
        if (!cb.pin()) { continue; }
        if (cb.is_dirty() && cb._used) {
            cb.unpin();
            return true;
        }
        cb.unpin();
    }

    return false;
}

void bf_tree_m::set_page_lsn(generic_page* p, lsn_t lsn)
{
    uint32_t idx = p - _buffer;
    p->lsn = lsn;

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
    w_assert1(cb.is_pinned_for_restore() || cb.get_page_lsn() <= lsn);
    cb.set_page_lsn(lsn);
}

lsn_t bf_tree_m::get_page_lsn(generic_page* p)
{
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx).get_page_lsn();
}

void bf_tree_m::set_check_recovery(generic_page* p, bool chk)
{
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx).set_check_recovery(chk);
}

void bf_tree_m::pin_for_restore(generic_page* p)
{
    get_cb(p)->pin_for_restore();
}

void bf_tree_m::unpin_for_restore(generic_page* p)
{
    get_cb(p)->unpin_for_restore();
}

uint32_t bf_tree_m::get_log_volume(generic_page* p)
{
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx).get_log_volume();
}

void bf_tree_m::increment_log_volume(generic_page* p, uint32_t v)
{
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    get_cb(idx).increment_log_volume(v);
}

void bf_tree_m::reset_log_volume(generic_page* p)
{
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    get_cb(idx).set_log_volume(0);
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
