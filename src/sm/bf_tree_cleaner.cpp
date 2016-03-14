/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "bf_tree_cleaner.h"
#include "sm_base.h"
#include "bf_tree_cb.h"
#include "bf_tree.h"
#include "generic_page.h"
#include "fixable_page_h.h"
#include "log_core.h"
#include "vol.h"
#include "alloc_cache.h"
#include "eventlog.h"
#include "sm.h"
#include "xct.h"

#include <vector>

const int INITIAL_SORT_BUFFER_SIZE = 64;

bf_tree_cleaner::bf_tree_cleaner(bf_tree_m* bufferpool, const sm_options& _options)
    :
    page_cleaner_base(bufferpool, _options),
    _sort_buffer (new uint64_t[INITIAL_SORT_BUFFER_SIZE]),
    _sort_buffer_size(INITIAL_SORT_BUFFER_SIZE)
{
}

bf_tree_cleaner::~bf_tree_cleaner()
{
    delete[] _sort_buffer;
}

void bf_tree_cleaner::_clean_volume(const std::vector<bf_idx> &candidates)
{
    // CS: flush log to guarantee WAL property. It's much simpler and more
    // efficient to do it once and with the current LSN as argument, instead
    // of checking the max LSN of each copied page. Taking the current LSN is
    // also safe because all copies were taken at this point, and further updates
    // would affect the page image on the buffer, and not the copied versions.
    W_COERCE(smlevel_0::log->flush_all());

    DBG(<< "Cleaner activated with " << candidates.size() << " candidate frames");

    if (_sort_buffer_size < candidates.size()) {
        size_t new_buffer_size = 2 * candidates.size();
        uint64_t* new_sort_buffer = new uint64_t[new_buffer_size];
        delete[] _sort_buffer;
        _sort_buffer = new_sort_buffer;
        _sort_buffer_size = new_buffer_size;
    }

    // again, we don't take latch at this point because this sorting is just for efficiency.
    // CS TODO: collect pid on list too, to avoid CB lookups here
    size_t sort_buf_used = 0;
    for (size_t i = 0; i < candidates.size(); ++i) {
        bf_idx idx = candidates[i];
        bf_tree_cb_t &cb = _bufferpool->get_cb(idx);
        w_assert1(_bufferpool->_buffer->lsn.valid());
        _sort_buffer[sort_buf_used] = (((uint64_t) cb._pid) << 32) + ((uint64_t) idx);
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
        _clean_lsn = smlevel_0::log->curr_lsn();
        for (size_t i = 0; i < sort_buf_used; ++i) {
            if (write_buffer_cur == _workspace_size) {
                // now the buffer is full. flush it out and also reset the buffer
                DBGOUT3(<< "Write buffer full. Flushing from " << write_buffer_from
                        << " to " << write_buffer_cur);
                flush_workspace (write_buffer_from, write_buffer_cur);

                PageID shpid = _workspace[write_buffer_cur].pid;
                sysevent::log_page_write(shpid, _clean_lsn,
                        write_buffer_cur - write_buffer_from);
                _clean_lsn = smlevel_0::log->curr_lsn();

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
            if (!cb.is_dirty() || !cb._used) {
                continue;
            }
            // just copy the page, and release the latch as soon as possible.
            // also tentatively skip an EX-latched page to avoid being interrepted by
            // a single EX latch for too long time.
            bool tobedeleted = false;
            w_rc_t latch_rc = cb.latch().latch_acquire(LATCH_SH, WAIT_IMMEDIATE);
            if (latch_rc.is_error()) {
                DBGOUT2 (<< "tentatively skipped an EX-latched page. "
                        << i << "=" << page_buffer[idx].pid << ". rc=" << latch_rc);
                skipped_something = true;
                continue;
            }

            fixable_page_h page;
            page.fix_nonbufferpool_page(const_cast<generic_page*>(&page_buffer[idx]));
            if (page.is_to_be_deleted()) {
                tobedeleted = true;
            }
            else {
                // Copy page and update its page_lsn from what's on the cb
                generic_page* pdest = _workspace + write_buffer_cur;
                ::memcpy(pdest, page_buffer + idx, sizeof (generic_page));
                pdest->lsn = cb.get_page_lsn();
                // CS TODO: swizzling!
                // if the page contains a swizzled pointer, we need to convert
                // the data back to the original pointer.  we need to do this
                // before releasing SH latch because the pointer might be
                // unswizzled by other threads.
                _bufferpool->_convert_to_disk_page(pdest);
            }
            cb.latch().latch_release();

            // then, re-calculate the checksum:
            _workspace[write_buffer_cur].checksum
                = _workspace[write_buffer_cur].calculate_checksum();

            if (tobedeleted) {
                // CS TODO: what's up with this stuff???
                // as it's deleted, no one should be accessing it now. we can do everything without latch
                DBGOUT2(<< "physically delete a page by buffer pool:" << page_buffer[idx].pid);
                // this operation requires a xct for logging. we create a ssx for this reason.
                sys_xct_section_t sxs(true); // ssx to call free_page
                W_COERCE (sxs.check_error_on_start());
                W_COERCE (smlevel_0::vol->deallocate_page(page_buffer[idx].pid));
                W_COERCE (sxs.end_sys_xct (RCOK));
                // drop the page from bufferpool too
                _bufferpool->_delete_block(idx);
            } else {
                PageID shpid = _workspace[write_buffer_cur].pid;
                _workspace_cb_indexes[write_buffer_cur] = idx;

                // if next page is not consecutive, flush it out
                if (write_buffer_from < write_buffer_cur && shpid != prev_shpid + 1) {
                    // flush up to _previous_ entry (before incrementing write_buffer_cur)
                    DBGOUT3(<< "Next page not consecutive. Flushing from " <<
                            write_buffer_from << " to " << write_buffer_cur);
                    flush_workspace(write_buffer_from, write_buffer_cur);

                    sysevent::log_page_write(shpid, _clean_lsn,
                            write_buffer_cur - write_buffer_from);
                    _clean_lsn = smlevel_0::log->curr_lsn();

                    write_buffer_from = write_buffer_cur;
                }
                ++write_buffer_cur;

                prev_idx = idx;
                prev_shpid = shpid;
            }
        } // for i in 0 .. sort_buf_used

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
                    throw runtime_error("Some dirty page keeps EX latch for \
                            long time! failed to flush out the bufferpool");
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
        flush_workspace(write_buffer_from, write_buffer_cur);

        PageID shpid = _workspace[write_buffer_cur].pid;
        sysevent::log_page_write(shpid, _clean_lsn,
                write_buffer_cur - write_buffer_from);
        _clean_lsn = smlevel_0::log->curr_lsn();

        write_buffer_from = 0; // not required, but to make sure
        write_buffer_cur = 0;
    }
}

void bf_tree_cleaner::do_work()
{
    _candidates_buffer.clear();

    // if the dirty page's lsn is same or smaller than durable lsn,
    // we can write it out without log flush overheads.
    bf_idx block_cnt = _bufferpool->_block_cnt;

    // list up dirty pages
    for (bf_idx idx = 1; idx < block_cnt; ++idx) {
        bf_tree_cb_t &cb = _bufferpool->get_cb(idx);
        // If page is not dirty or not in use, no need to flush
        if (!cb.is_dirty() || !cb._used) {
            continue;
        }

        _candidates_buffer.push_back (idx);
        // DBGOUT3(<< "Picked page for cleaning: idx = " << idx
        //         << " vol = " << cb._pid_vol
        //         << " shpid = " << cb._pid);
    }
    if (!_candidates_buffer.empty()) {
        _clean_volume(_candidates_buffer);
    }
}
