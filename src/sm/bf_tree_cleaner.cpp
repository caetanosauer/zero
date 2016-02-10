/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "bf_tree_cleaner.h"
#include <sys/time.h>
#include "sm_base.h"
#include "bf_tree_cb.h"
#include "bf_tree.h"
#include "w_autodel.h"
#include "generic_page.h"
#include "fixable_page_h.h"  // just for get_cb in bf_tree_inline.h
#include "log.h"
#include "vol.h"
#include <string.h>
#include <vector>
#include <algorithm>
#include <stdlib.h>

#include "sm.h"
#include "xct.h"

bool _dirty_shutdown_happening() {
    return (ss_m::shutting_down && !ss_m::shutdown_clean);
}

const int INITIAL_SORT_BUFFER_SIZE = 64;

bf_tree_cleaner::bf_tree_cleaner(bf_tree_m* bufferpool,
    uint32_t interval_millisec,
    uint32_t write_buffer_pages) :
    _bufferpool(bufferpool),
    _write_buffer_pages (write_buffer_pages),
    _interval_millisec(interval_millisec),
    _sort_buffer (new uint64_t[INITIAL_SORT_BUFFER_SIZE]), _sort_buffer_size(INITIAL_SORT_BUFFER_SIZE),
    _stop_requested(false), _wakeup_requested(false)
{
   _error_happened = false;
   ::pthread_mutex_init(&_interval_mutex, NULL);
   ::pthread_cond_init(&_interval_cond, NULL);

   // use posix_memalign because the write buffer might be used for raw disk I/O
   void *buf = NULL;
   w_assert0(::posix_memalign(&buf, SM_PAGESIZE, SM_PAGESIZE * (_write_buffer_pages + 1))==0); // +1 margin for switching to next batch
   w_assert0(buf != NULL);
   _write_buffer = reinterpret_cast<generic_page*>(buf);

   _write_buffer_indexes = new bf_idx[_write_buffer_pages + 1];
}

bf_tree_cleaner::~bf_tree_cleaner()
{
    ::pthread_mutex_destroy(&_interval_mutex);
    ::pthread_cond_destroy(&_interval_cond);
    delete[] _sort_buffer;

    void *buf = reinterpret_cast<void*>(_write_buffer);
    // note we use free(), not delete[], which corresponds to posix_memalign
    ::free (buf);

    delete[] _write_buffer_indexes;    
}

w_rc_t bf_tree_cleaner::wakeup_cleaner()
{
    int rc_mutex_lock = ::pthread_mutex_lock (&_interval_mutex);
    w_assert1(rc_mutex_lock == 0);

    _wakeup_requested = true;

    int rc_broadcast = ::pthread_cond_broadcast(&_interval_cond);
    w_assert1(rc_broadcast == 0);

    int rc_mutex_unlock = ::pthread_mutex_unlock (&_interval_mutex);
    w_assert1(rc_mutex_unlock == 0);
    return RCOK;
}

w_rc_t bf_tree_cleaner::shutdown()
{
    _stop_requested = true;
    lintel::atomic_thread_fence(lintel::memory_order_release);
    lintel::atomic_thread_fence(lintel::memory_order_consume);
    W_DO(join(sthread_base_t::WAIT_FOREVER));
    return RCOK;
}

const uint32_t FORCE_SLEEP_MS_MIN = 10;
const uint32_t FORCE_SLEEP_MS_MAX = 1000;

w_rc_t bf_tree_cleaner::force_volume()
{
    // CS TODO: force pages of stnode and alloc caches

    while (true) {
        _requested_volume = true;
        lintel::atomic_thread_fence(lintel::memory_order_release);
        W_DO(wakeup_cleaner());
        uint32_t interval = FORCE_SLEEP_MS_MIN;
        while (_requested_volume && !_dirty_shutdown_happening() && !_error_happened) {
            DBGOUT2(<< "waiting in force_volume...");
            usleep(interval * 1000);
            interval *= 2;
            if (interval >= FORCE_SLEEP_MS_MAX) {
                interval = FORCE_SLEEP_MS_MAX;
            }
            lintel::atomic_thread_fence(lintel::memory_order_consume);
        }

        // CS TODO: temporary fix to make sure all pages are cleaned.
        // This is required because pages can be written in an older version
        // when an update happens after the cleaner copy is taken. We need
        // a better implementation for force_all, but eventually the whole
        // cleaner will be rewritten, so this suffices for now.
        bool all_clean = true;
        bf_idx block_cnt = _bufferpool->_block_cnt;
        for (bf_idx idx = 1; idx < block_cnt; ++idx) {
            // no latching is needed -- fuzzy check
            bf_tree_cb_t &cb = _bufferpool->get_cb(idx);
            if (cb._dirty) {
                all_clean = false;
                break;
            }
        }
        if (all_clean) {
            break;
        }
    }

    if (_dirty_shutdown_happening()) {
        DBGOUT1(<< "joining all cleaner threads up to 100ms...");
        W_DO(join(sthread_base_t::WAIT_FOREVER));
    }
    DBGOUT2(<< "done force_volume!");
    return RCOK;
}

bool bf_tree_cleaner::_cond_timedwait (uint64_t timeout_microsec) {
    int rc_mutex_lock = ::pthread_mutex_lock (&_interval_mutex);
    w_assert1(rc_mutex_lock == 0);

    timespec   ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += timeout_microsec * 1000;
    ts.tv_sec += ts.tv_nsec / 1000000000;
    ts.tv_nsec = ts.tv_nsec % 1000000000;

    bool timeouted = false;
    lintel::atomic_thread_fence(lintel::memory_order_consume);
    while (!_wakeup_requested) {
        int rc_wait =
            ::pthread_cond_timedwait(&_interval_cond, &_interval_mutex, &ts);

        if (rc_wait == ETIMEDOUT) {
            DBGOUT2(<<"timeouted");
            timeouted = true;
            break;
        }
        lintel::atomic_thread_fence(lintel::memory_order_consume);
    }

    int rc_mutex_unlock = ::pthread_mutex_unlock (&_interval_mutex);
    w_assert1(rc_mutex_unlock == 0);
    _wakeup_requested = false;
    lintel::atomic_thread_fence(lintel::memory_order_release);
    return timeouted;
}

void bf_tree_cleaner::run()
{
    lintel::atomic_thread_fence(lintel::memory_order_consume);
    while (!_stop_requested && !_error_happened) {
        if (_dirty_shutdown_happening()) {
            ERROUT (<<"the system is going to shutdown dirtily. this cleaner will shutdown without flushing!");
            break;
        }
        w_rc_t rc = _do_work();
        if (rc.is_error()) {
            ERROUT (<<"cleaner thread error:" << rc);
            _error_happened = true;
            break;
        }
        lintel::atomic_thread_fence(lintel::memory_order_consume);
        if (!_stop_requested && !_exists_requested_work() && !_error_happened) {
            _cond_timedwait((uint64_t) _interval_millisec * 1000); //sleep for a bit
            lintel::atomic_thread_fence(lintel::memory_order_consume);
        }
    }
}

bool bf_tree_cleaner::_exists_requested_work()
{
    // the cleaner is requested to flush out all dirty pages for assigned volume. let's do it immediately
    lintel::atomic_thread_fence(lintel::memory_order_consume);
    if (_requested_volume) {
        return true;
    }

    // otherwise let's take a sleep
    return false;
}

w_rc_t bf_tree_cleaner::_clean_volume(const std::vector<bf_idx> &candidates)
{
    if (_dirty_shutdown_happening()) return RCOK;

    // CS: flush log to guarantee WAL property. It's much simpler and more
    // efficient to do it once and with the current LSN as argument, instead
    // of checking the max LSN of each copied page. Taking the current LSN is
    // also safe because all copies were taken at this point, and further updates
    // would affect the page image on the buffer, and not the copied versions.
    W_COERCE(smlevel_0::log->flush(smlevel_0::log->curr_lsn()));

    DBG(<< "Cleaner activated with " << candidates.size() << " candidate frames");
    unsigned cleaned_count = 0;

    // TODO this method should separate dirty pages that have dependency and flush them after others.
    if (_sort_buffer_size < candidates.size()) {
        size_t new_buffer_size = 2 * candidates.size();
        uint64_t* new_sort_buffer = new uint64_t[new_buffer_size];
        if (new_sort_buffer == NULL) {
            return RC(eOUTOFMEMORY);
        }
        delete[] _sort_buffer;
        _sort_buffer = new_sort_buffer;
        _sort_buffer_size = new_buffer_size;
    }

    // again, we don't take latch at this point because this sorting is just for efficiency.
    size_t sort_buf_used = 0;
    for (size_t i = 0; i < candidates.size(); ++i) {
        bf_idx idx = candidates[i];
        bf_tree_cb_t &cb = _bufferpool->get_cb(idx);
        w_assert1(cb._pid_shpid >= smlevel_0::vol->first_data_pageid());
        w_assert1(_bufferpool->_buffer->lsn.valid());
        _sort_buffer[sort_buf_used] = (((uint64_t) cb._pid_shpid) << 32) + ((uint64_t) idx);
        ++sort_buf_used;
    }
    std::sort(_sort_buffer, _sort_buffer + sort_buf_used);

    // now, write them out as sequentially as possible.
    // Note that
    // 1) at this point we need to take SH latch while copying the page and doing the real check
    // 2) there might be duplicates in _sort_buffer. it can be easily detected because it's sorted.
    size_t write_buffer_from = 0;
    size_t write_buffer_cur = 0;
    PageID prev_idx = 0; // to check duplicates
    PageID prev_shpid = 0; // to check if it's contiguous
    const generic_page* const page_buffer = _bufferpool->_buffer;
    int rounds = 0;
    while (true) {
        bool skipped_something = false;
        for (size_t i = 0; i < sort_buf_used; ++i) {
            if (write_buffer_cur == _write_buffer_pages) {
                // now the buffer is full. flush it out and also reset the buffer
                DBGOUT3(<< "Write buffer full. Flushing from " << write_buffer_from
                        << " to " << write_buffer_cur);
                W_DO(_flush_write_buffer (write_buffer_from,
                            write_buffer_cur - write_buffer_from, cleaned_count));
                write_buffer_from = 0;
                write_buffer_cur = 0;
            }
            bf_idx idx = (bf_idx) (_sort_buffer[i] & 0xFFFFFFFF); // extract bf_idx
            w_assert1(idx > 0);
            w_assert1(idx < _bufferpool->_block_cnt);
            if (idx == prev_idx) {
                continue;
            }
            bf_tree_cb_t &cb = _bufferpool->get_cb(idx);
            // Not checking in_doubt flag, because if in_doubt flag is on then cb._used flag must be on
            if (!cb._dirty || !cb._used) {
                continue;
            }
            // just copy the page, and release the latch as soon as possible.
            // also tentatively skip an EX-latched page to avoid being interrepted by
            // a single EX latch for too long time.
            bool tobedeleted = false;
            w_rc_t latch_rc = cb.latch().latch_acquire(LATCH_SH, WAIT_IMMEDIATE);
            if (latch_rc.is_error()) {
                DBGOUT2 (<< "tentatively skipped an EX-latched page. " << i << "=" << page_buffer[idx].pid << ". rc=" << latch_rc);
                skipped_something = true;
                continue;
            }

            // CS: No-steal policy for atomic commit protocol:
            // Only flush pages without uncommitted updates
            bool flush_it = true;
#ifdef USE_ATOMIC_COMMIT
            flush_it = cb._uncommitted_cnt == 0;
#endif

            if (flush_it) {
                fixable_page_h page;
                page.fix_nonbufferpool_page(const_cast<generic_page*>(&page_buffer[idx])); // <<<>>>
                if (page.is_to_be_deleted()) {
                    tobedeleted = true;
                } else {
                    w_assert1(!smlevel_0::clog || cb._uncommitted_cnt == 0);
                    ::memcpy(_write_buffer + write_buffer_cur, page_buffer + idx, sizeof (generic_page));
                    // if the page contains a swizzled pointer, we need to convert the data back to the original pointer.
                    // we need to do this before releasing SH latch because the pointer might be unswizzled by other threads.
                    _bufferpool->_convert_to_disk_page(_write_buffer + write_buffer_cur);// convert swizzled data.
                }
                cb.latch().latch_release();
            }
            else {
                DBGOUT3(<< "ACP: skipped flush of uncommitted page "
                        << cb._pid_shpid);
                cb.latch().latch_release();
                continue;
            }

#ifdef USE_ATOMIC_COMMIT
            // In order for recovery to work properly with ACP,
            // the CLSN field must be used instead of the old PageLSN.
            // Instead of changing restart.cpp in a thousand places,
            // we simply duplicate the fields before writing to disk.
            _write_buffer[write_buffer_cur].lsn =
                _write_buffer[write_buffer_cur].clsn;

#endif

            // then, re-calculate the checksum:
            _write_buffer[write_buffer_cur].checksum
                = _write_buffer[write_buffer_cur].calculate_checksum();
            // also copy the new checksum to make the original (bufferpool) page consistent.
            // we can do this out of latch scope because it's never used in race condition.
            // this is mainly for testcases and such.
            page_buffer[idx].checksum = _write_buffer[write_buffer_cur].checksum;
            // DBGOUT3(<< "Checksum for " << _write_buffer[write_buffer_cur].pid
            //         << " is " << _write_buffer[write_buffer_cur].checksum);


            if (tobedeleted) {
                // as it's deleted, no one should be accessing it now. we can do everything without latch
                DBGOUT2(<< "physically delete a page by buffer pool:" << page_buffer[idx].pid);
                // this operation requires a xct for logging. we create a ssx for this reason.
                sys_xct_section_t sxs(true); // ssx to call free_page
                W_DO (sxs.check_error_on_start());
                W_DO (smlevel_0::vol->deallocate_page(page_buffer[idx].pid));
                W_DO (sxs.end_sys_xct (RCOK));
                // drop the page from bufferpool too
                _bufferpool->_delete_block(idx);
            } else {
                PageID shpid = _write_buffer[write_buffer_cur].pid;
                _write_buffer_indexes[write_buffer_cur] = idx;

                // if next page is not consecutive, flush it out
                if (write_buffer_from < write_buffer_cur && shpid != prev_shpid + 1) {
                    // flush up to _previous_ entry (before incrementing write_buffer_cur)
                    DBGOUT3(<< "Next page not consecutive. Flushing from " <<
                            write_buffer_from << " to " << write_buffer_cur);
                    W_DO(_flush_write_buffer (write_buffer_from,
                                write_buffer_cur - write_buffer_from, cleaned_count));
                    write_buffer_from = write_buffer_cur;
                }
                ++write_buffer_cur;

                prev_idx = idx;
                prev_shpid = shpid;
            }
        }
        if (skipped_something)
        {
            // CS: TODO instead of waiting forever, cleaner should have a
            // "best effort" approach, meaning that it cannot guarantee either:
            // 1) that all pages of a volume are flushed; or
            // 2) that all updates up to a certain LSN are propagated.
            // Instead, the cleaner should simply report back the highest LSN
            // guaranteed to be persistent and whether all requested pages of
            // a volume were flushed or not. If guarantees are required, then
            // the caller should keep invoking the cleaner until its reported
            // result is satisfying.
            //
            // Note that using the naive no-steal policy of the current
            // implementation of the atomic commit protocol, this wait will
            // depend on transaction activity, because pages cannot be cleaned
            // until all transactions modifying it either commit or abort.
            // The caller should also be aware of the new semantics of LSNs,
            // which refers only to committed updates.

            // umm, we really need to make sure all of these are flushed out.
            ++rounds;
            if (rounds > 2) {
                DBGOUT1(<<"some dirty page seems to have a persistent EX latch. waiting... rounds=" << rounds);
                if (rounds > 50) {
                    ERROUT(<<"FATAL! some dirty page keeps EX latch for long time! failed to flush out the bufferpool");
                    return RC(eINTERNAL);
                }
                usleep(rounds > 5 ? 100000 : 20000);
            }
        }
        else {
            break;
        }
    }
    if (write_buffer_cur > write_buffer_from) {
        DBGOUT3(<< "Finished cleaning round. Flushing from " << write_buffer_from
                << " to " << write_buffer_cur);
        W_DO(_flush_write_buffer (write_buffer_from,
                    write_buffer_cur - write_buffer_from, cleaned_count));
        write_buffer_from = 0; // not required, but to make sure
        write_buffer_cur = 0;
    }

    DBG(<< "Cleaner round done. Pages cleaned: " << cleaned_count);

    return RCOK;
}


w_rc_t bf_tree_cleaner::_flush_write_buffer(size_t from, size_t consecutive, unsigned& cleaned_count)
{
    if (_dirty_shutdown_happening()) return RCOK;
    if (consecutive == 0) {
        return RCOK;
    }

    // CS: flushing the log only once for each round (see _clean_volume)
    // we'll compute the highest lsn of the pages, and do one log flush to that lsn.
    // lsndata_t max_page_lsn = lsndata_null;
    // for (size_t i = from; i < from + consecutive; ++i) {
    //     w_assert1(_write_buffer[i].pid.vol() == vol);
    //     if (i > from) {
    //         w_assert1(_write_buffer[i].pid.page == _write_buffer[i - 1].pid.page + 1);
    //     }
    //     if (_write_buffer[i].lsn.data() > max_page_lsn) {
    //         max_page_lsn = _write_buffer[i].lsn.data();
    //     }
    // }
    // // WAL: ensure that the log is durable to this lsn
    // if (max_page_lsn != lsndata_null) {
    //     if (smlevel_0::log == NULL) {
    //         ERROUT (<< "Cleaner encountered null log manager. Probably dirty shutdown?");
    //         return RC(eINTERNAL);
    //     }
    //     W_COERCE( smlevel_0::log->flush(lsn_t(max_page_lsn)) );
    // }

    W_COERCE(smlevel_0::vol->write_many_pages(
                _write_buffer[from].pid, _write_buffer + from,
                consecutive));

    for (size_t i = from; i < from + consecutive; ++i) {
        bf_idx idx = _write_buffer_indexes[i];
        bf_tree_cb_t &cb = _bufferpool->get_cb(idx);

        if (i > from) {
            w_assert1(_write_buffer[i].pid == _write_buffer[i - 1].pid + 1);
        }

        // CS bugfix: we have to latch and compare LSNs before marking clean
        cb.latch().latch_acquire(LATCH_SH, sthread_t::WAIT_FOREVER);
        generic_page& buffered = *smlevel_0::bf->get_page(idx);
        generic_page& copied = _write_buffer[i];

        if (buffered.pid == copied.pid) {
            if (buffered.lsn == copied.lsn) {
                cb._dirty = false;
                cleaned_count++;
            }
            --_bufferpool->_dirty_page_count_approximate;

            // cb._rec_lsn = _write_buffer[i].lsn.data();
            cb._rec_lsn = lsn_t::null.data();
            cb._dependency_idx = 0;
            cb._dependency_lsn = 0;
            cb._dependency_shpid = 0;
        }

        cb.latch().latch_release();
    }

    return RCOK;
}

w_rc_t bf_tree_cleaner::_do_work()
{
    if (_dirty_shutdown_happening()) return RCOK;

    // I'm hoping this doesn't revoke the buffer, leaving the capacity for reuse.
    // but in some STL implementation this might make the capacity zero. even in that case it shouldn't be a big issue..
    _candidates_buffer.clear();

    // if the dirty page's lsn is same or smaller than durable lsn,
    // we can write it out without log flush overheads.
    bf_idx block_cnt = _bufferpool->_block_cnt;

    // do we have lots of dirty pages? (note, these are approximate statistics)
    if (_bufferpool->_dirty_page_count_approximate < 0) {// so, even this can happen.
        _bufferpool->_dirty_page_count_approximate = 0;
    }

    // list up dirty pages
    for (bf_idx idx = 1; idx < block_cnt; ++idx) {
        bf_tree_cb_t &cb = _bufferpool->get_cb(idx);
        // If page is not dirty or not in use, no need to flush
        if (!cb._dirty || !cb._used) {
            continue;
        }

        _candidates_buffer.push_back (idx);
        // DBGOUT3(<< "Picked page for cleaning: idx = " << idx
        //         << " vol = " << cb._pid_vol
        //         << " shpid = " << cb._pid_shpid);

        // also add dependent pages. note that this might cause a duplicate. we deal with duplicates in _clean_volume()
        bf_idx didx = cb._dependency_idx;
        if (didx != 0) {
            bf_tree_cb_t &dcb = _bufferpool->get_cb(didx);
            if (dcb._dirty && dcb._used && dcb._rec_lsn <= cb._dependency_lsn) {
                _candidates_buffer.push_back (didx);
            }
        }
    }
    if (!_candidates_buffer.empty()) {
        W_DO(_clean_volume(_candidates_buffer));
    }

    _requested_volume = false;
    lintel::atomic_thread_fence(lintel::memory_order_release);
    return RCOK;
}